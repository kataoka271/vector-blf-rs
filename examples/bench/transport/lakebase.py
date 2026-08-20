"""Lakebase (managed Postgres) channel: LISTEN/NOTIFY with Nagle-style batching.

Batching is self-clocking, not timed: LakebaseTx's background writer thread sends
whatever accumulated in its queue the moment the previous round trip completes, so a
batch of one costs no added delay at a low frame rate and a batch grows to roughly
round-trip-time x offered-rate at a high one -- no batch-size or batch-interval to
configure. LakebaseTx only owns the queue/thread that decides *when* a round trip
starts; _LakebaseBatchWriter owns the connection and does the round trip itself (one
INSERT+NOTIFY per batch) -- see each class's docstring.

NOTIFY payloads are hybrid: Postgres caps a notification at 8000 bytes
(NOTIFY_PAYLOAD_LIMIT). A batch that fits is carried inline (build_envelope returns
inline=True), so a receiver needs no follow-up query; a batch that doesn't (a few
1500-byte Ethernet frames is enough) carries only its identity plus an `ids` array
merged in server-side, and the receiver fetches those exact ids via fetch_ids_sql (not a
min/max range -- concurrent writers to the same table make one batch's ids
non-contiguous).

NOTIFY is still fire-and-forget, so a receiver still needs to catch up on connect and
dedupe by row id -- that part is unrelated to batching and unchanged.

`psycopg`/`databricks.sdk` are imported lazily inside `connect()`, not at module level,
so constructing a `LakebaseConfig` (or importing this module for its SQL builders in a
test) never requires either package to be installed.
"""

from __future__ import annotations

import json
import queue
import re
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from bench.frame import FRAME_COLUMNS, Frame, accepts

if TYPE_CHECKING:
    import psycopg
    from bench.bus import Bus

DEFAULT_DATABASE = "databricks_postgres"

# Postgres caps a NOTIFY payload at 8000 bytes. The envelope is built client-side without
# the row ids, which the INSERT statement merges in server-side, so the budget has to
# leave room for one id (up to 20 digits, plus a separator) per frame.
NOTIFY_PAYLOAD_LIMIT = 8000
_ID_BYTES_PER_FRAME = 21
_ID_ARRAY_OVERHEAD = 16

ENVELOPE_VERSION = 1

# Seconds of overlap re-scanned by a catch-up query. A row's BIGSERIAL id is assigned
# when the INSERT runs but only becomes visible once its transaction commits, so ids can
# become visible out of order and `id > watermark` alone can step over a row that
# committed late. Re-scanning by insert time and deduplicating by id closes that window.
CATCHUP_LOOKBACK_SECONDS = 5.0

# Bus table names are interpolated into DDL/DML where a bound parameter is not allowed
# (identifiers can't be bound). Restricting them to bare identifiers keeps that safe.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class LakebaseConfig:
    profile: str | None = None
    endpoint_name: str = ""
    dbname: str = DEFAULT_DATABASE
    # Empty means "use the owning Bus's generated name" -- see bench.bus.Bus.__init__,
    # which resolves this so that e.g. two independently-constructed Lakebase() buses in
    # one TestBench land on two distinct Postgres tables rather than silently colliding.
    table: str = ""


def check_identifier(name: str, *, what: str = "table") -> str:
    """Return `name` unchanged if it is a bare SQL identifier, else raise ValueError."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"{what} name must match {_IDENTIFIER.pattern!r} (bare identifier), got {name!r}")
    return name


def sql(statement: str) -> Any:
    """Mark a dynamically-built statement as safe to execute.

    psycopg's stubs accept only `LiteralString` for a query built from an f-string, so
    that interpolating anything into a statement is a type error by default. Every
    statement built here interpolates nothing but table names `check_identifier` has
    already validated -- all values are bound parameters -- and this is the single place
    that argument is recorded. Returns `Any` rather than `LiteralString`, which needs
    Python 3.11 while this project supports 3.10.
    """
    return statement


def notify_channel(table: str) -> str:
    """Return the LISTEN/NOTIFY channel name carrying `table`'s frames."""
    return check_identifier(table)


def connect(profile: str | None, endpoint_name: str, dbname: str) -> psycopg.Connection:
    """Open a new autocommitting connection with a freshly minted OAuth token.

    Re-resolves the endpoint host and mints a fresh token on every call -- simpler than
    caching the resolved host, and cheap enough for the reconnect-on-failure paths in
    LakebaseTx/LakebaseRx to call directly.
    """
    import psycopg
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=profile or None)

    endpoint = w.postgres.get_endpoint(endpoint_name)
    if endpoint.status is None or endpoint.status.hosts is None:
        raise RuntimeError(f"endpoint {endpoint!r} has no status.hosts yet; is it still provisioning?")

    user = w.current_user.me().user_name
    if not user:
        raise RuntimeError("current_user.me() returned no user_name; cannot resolve a Postgres role")

    assert endpoint.name is not None, "endpoint.name is None; cannot generate database credential"

    token = w.postgres.generate_database_credential(endpoint=endpoint.name).token
    return psycopg.connect(
        host=endpoint.status.hosts.host,
        dbname=dbname,
        user=user,
        password=token,
        sslmode="require",
        autocommit=True,
    )


def _quoted_columns() -> str:
    return ", ".join(f'"{c}"' for c in FRAME_COLUMNS)


def ensure_schema_sql(table: str) -> str:
    """Return idempotent DDL creating `table` as a bus segment.

    Safe to run on every Tx/Rx construction, so the two ends can start in either order.
    """
    check_identifier(table)
    return f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            id            BIGSERIAL PRIMARY KEY,
            run_id        TEXT NOT NULL,
            source_file   TEXT NOT NULL,
            message_type  TEXT NOT NULL,
            timestamp_ns  BIGINT NOT NULL,
            observed_ns   BIGINT NOT NULL,
            channel       INT NOT NULL,
            dir           SMALLINT NOT NULL,
            data          BYTEA NOT NULL,
            can_id        BIGINT,
            is_ext_id     BOOLEAN,
            rtr           BOOLEAN,
            dlc           SMALLINT,
            is_fd         BOOLEAN,
            src_addr      BYTEA,
            dst_addr      BYTEA,
            ether_type    INT,
            vlan_tpid     INT,
            vlan_cos      INT,
            vlan_id       INT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS "idx_{table}_run_id" ON "{table}" (run_id, id);
        CREATE INDEX IF NOT EXISTS "idx_{table}_created" ON "{table}" (created_at);
    """


def ensure_schema(conn: psycopg.Connection, table: str) -> None:
    """Create `table` and its indexes if absent, on an already-open connection.

    Two sessions racing this on a table that exists in neither yet can both pass
    Postgres's existence check before either commits -- CREATE TABLE IF NOT EXISTS
    is not atomic across sessions -- so the loser gets a UniqueViolation on the system
    catalog rather than a clean no-op. That failure means the winner's CREATE TABLE
    already went through, so it is safe to swallow.
    """
    import psycopg

    try:
        conn.execute(ensure_schema_sql(table).encode("utf-8"))
    except psycopg.errors.UniqueViolation:
        pass


def build_envelope(frames: Sequence[Frame]) -> tuple[str, bool]:
    """Serialize `frames` into a NOTIFY payload.

    Returns `(payload, inline)`. When `inline` is True the payload carries the frames
    themselves and the receiver needs no follow-up query; when False it carries only the
    batch's identity and the receiver must fetch the rows itself. Either way the payload
    lacks the `ids` array -- that is added server-side from the inserted rows.

    All frames must share one run_id and source_file; they are hoisted out of the
    per-frame objects to keep the payload under the 8000-byte cap. Raises ValueError for
    an empty batch or a mixed one.
    """
    if not frames:
        raise ValueError("cannot build an envelope for an empty batch")
    run_id = frames[0].run_id
    source_file = frames[0].source_file
    if any(f.run_id != run_id or f.source_file != source_file for f in frames):
        raise ValueError("every frame in one batch must share run_id and source_file")

    envelope = {"v": ENVELOPE_VERSION, "rid": run_id, "sf": source_file}
    ids_budget = _ID_ARRAY_OVERHEAD + _ID_BYTES_PER_FRAME * len(frames)
    inline = dict(envelope, frames=[f.to_json_obj() for f in frames])
    payload = json.dumps(inline, separators=(",", ":"))
    if len(payload.encode("utf-8")) + ids_budget <= NOTIFY_PAYLOAD_LIMIT:
        return payload, True
    return json.dumps(envelope, separators=(",", ":")), False


def parse_envelope(payload: str) -> dict:
    """Parse a NOTIFY payload into `{run_id, source_file, ids, frames}`.

    `ids` are the batch's bus row ids in insertion order. `frames` is a same-length list
    of Frame for an inline payload, and None when the receiver has to fetch those ids
    itself. Raises ValueError on an unsupported envelope version, or when an inline
    payload's frame count does not match its id count.
    """
    obj = json.loads(payload)
    version = obj.get("v")
    if version != ENVELOPE_VERSION:
        raise ValueError(f"unsupported bus envelope version {version!r} (expected {ENVELOPE_VERSION})")
    run_id = obj["rid"]
    source_file = obj["sf"]
    ids = obj["ids"]
    raw = obj.get("frames")
    if raw is not None and len(raw) != len(ids):
        raise ValueError(f"envelope carries {len(raw)} frames but {len(ids)} ids")
    return {
        "run_id": run_id,
        "source_file": source_file,
        "ids": ids,
        "frames": (
            None if raw is None else [Frame.from_json_obj(o, run_id=run_id, source_file=source_file) for o in raw]
        ),
    }


def insert_and_notify_sql(table: str, batch_size: int) -> str:
    """Return the statement that inserts `batch_size` frames and notifies about them.

    Placeholders bind, in order: every frame's FRAME_COLUMNS values row by row, then the
    notification channel, then the envelope JSON from build_envelope. The envelope's
    `ids` array is merged in from the inserted rows, which is why this has to be one
    statement -- doing the INSERT and the NOTIFY separately would need a second round
    trip and put the notification outside the insert's transaction.
    """
    check_identifier(table)
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    row = "(" + ", ".join(["%s"] * len(FRAME_COLUMNS)) + ")"
    values = ", ".join([row] * batch_size)
    # The real ids go on the wire rather than just their min and max: other ECUs insert
    # into the same bus table concurrently, so one statement's sequence values are not
    # necessarily contiguous and a receiver cannot reconstruct them from a range. Ordering
    # by id restores insertion order, which is what pairs ids[i] with frames[i].
    return (
        f'WITH ins AS (INSERT INTO "{table}" ({_quoted_columns()}) VALUES {values} RETURNING id) '
        "SELECT pg_notify(%s, (%s::jsonb || jsonb_build_object('ids', jsonb_agg(id ORDER BY id)))::text) FROM ins"
    )


def insert_params(frames: Sequence[Frame], channel: str, envelope: str) -> list:
    """Return the bound parameters for insert_and_notify_sql, in placeholder order."""
    params: list = []
    for frame in frames:
        row = frame.to_row()
        params.extend(row[c] for c in FRAME_COLUMNS)
    params.append(channel)
    params.append(envelope)
    return params


def catchup_sql(table: str) -> str:
    """Return the query replaying a run's recent frames after a (re)connect.

    Placeholders bind: run_id, watermark id, lookback seconds. Rows come back oldest
    first, with `id` prepended to the frame columns.
    """
    check_identifier(table)
    return (
        f'SELECT id, {_quoted_columns()} FROM "{table}" '
        "WHERE run_id = %s AND (id > %s OR created_at > now() - make_interval(secs => %s)) "
        "ORDER BY id"
    )


def fetch_ids_sql(table: str) -> str:
    """Return the query fetching a batch of frames by their exact bus row ids.

    Placeholders bind: run_id, the id list. Uses `id = ANY(%s)` rather than a min/max
    range -- see insert_and_notify_sql for why a range can't be reconstructed. Rows come
    back oldest first, with `id` prepended to the frame columns.
    """
    check_identifier(table)
    return f'SELECT id, {_quoted_columns()} FROM "{table}" WHERE run_id = %s AND id = ANY(%s) ORDER BY id'


class SeenIds:
    """Bounded set of recently-observed bus row ids, for deduplicating overlapping reads.

    Remembers the most recent `capacity` ids; older ones fall out. That is safe because
    the only source of duplicates is a catch-up window measured in seconds, far inside
    what `capacity` covers at any plausible frame rate.
    """

    def __init__(self, capacity: int = 8192) -> None:
        self._capacity = capacity
        self._seen: dict[int, None] = {}

    def add_if_new(self, row_id: int) -> bool:
        """Record `row_id` and return whether it had not been seen before."""
        if row_id in self._seen:
            return False
        self._seen[row_id] = None
        if len(self._seen) > self._capacity:
            # dicts preserve insertion order, so this evicts the oldest id.
            del self._seen[next(iter(self._seen))]
        return True

    def __len__(self) -> int:
        return len(self._seen)


class _BatchWriter(Protocol):
    """What LakebaseTx needs from its writer -- satisfied structurally by
    _LakebaseBatchWriter, and by any test double a caller injects via LakebaseTx's
    `writer` parameter.
    """

    table: str

    def write_batch(self, frames: list[Frame]) -> None: ...
    def close(self) -> None: ...


class _LakebaseBatchWriter:
    """Owns one Lakebase connection and writes a batch of frames in a single round trip.

    Separated from LakebaseTx so the batching policy (deciding *when* a round trip
    starts) and the round trip itself (what one round trip does) vary independently --
    LakebaseTx wraps an instance of this and only ever calls write_batch()/close().
    """

    def __init__(self, *, config: LakebaseConfig, create_schema: bool = True) -> None:
        self.table = check_identifier(config.table)
        self._config = config
        self._channel = notify_channel(config.table)
        self._conn = connect(config.profile, config.endpoint_name, config.dbname)
        if create_schema:
            ensure_schema(self._conn, config.table)

    def write_batch(self, frames: list[Frame]) -> None:
        envelope, _inline = build_envelope(frames)
        stmt = sql(insert_and_notify_sql(self.table, len(frames)))
        params = insert_params(frames, self._channel, envelope)
        try:
            self._conn.execute(stmt, params)
        except Exception as exc:
            # One reconnect covers the connection drops a long run inevitably sees: an
            # idle timeout, or Lakebase scaling its compute to zero and back.
            print(f"[bench] lakebase tx {self.table}: {exc!r}; reconnecting and retrying once", flush=True)
            self._conn = connect(self._config.profile, self._config.endpoint_name, self._config.dbname)
            self._conn.execute(stmt, params)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


class LakebaseTx:
    """Transmit side of a Lakebase channel: batches self-clock to the round trip.

    send() only queues a frame; a background thread sends whatever has accumulated the
    moment the previous write_batch() call returns, so a batch of one costs no added
    delay at a low frame rate and a batch grows to roughly round-trip-time x
    offered-rate at a high one -- no batch-size or batch-interval to configure. A write
    that fails its one retry (inside _LakebaseBatchWriter.write_batch) is not swallowed:
    the error latches and surfaces from the next send(), flush(), or close(), so a
    dropped frame is never silent.
    """

    def __init__(
        self,
        *,
        config: LakebaseConfig | None = None,
        create_schema: bool = True,
        writer: _BatchWriter | None = None,
    ) -> None:
        if writer is None:
            if config is None:
                raise ValueError("LakebaseTx needs either config or an explicit writer")
            writer = _LakebaseBatchWriter(config=config, create_schema=create_schema)
        self._writer = writer
        self._table = writer.table
        self._pending: list[Frame] = []
        self._in_flight = False
        self._closed = False
        self._error: BaseException | None = None
        self._cv = threading.Condition()
        self._thread = threading.Thread(target=self._run, name=f"lakebase-tx-{self._table}", daemon=True)
        self._thread.start()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError(f"lakebase channel {self._table!r} transmit failed") from self._error

    def send(self, frame: Frame) -> None:
        with self._cv:
            self._raise_if_failed()
            if self._closed:
                raise RuntimeError(f"lakebase channel {self._table!r} is closed")
            self._pending.append(frame)
            self._cv.notify_all()

    def flush(self) -> None:
        with self._cv:
            while (self._pending or self._in_flight) and self._error is None:
                self._cv.wait()
            self._raise_if_failed()

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()
        self._thread.join(timeout=30.0)
        self._writer.close()
        self._raise_if_failed()

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._pending and not self._closed:
                    self._cv.wait()
                if not self._pending:
                    return
                batch, self._pending = self._pending, []
                self._in_flight = True
            try:
                self._writer.write_batch(batch)
            except Exception as exc:
                with self._cv:
                    self._error = exc
                    self._in_flight = False
                    self._cv.notify_all()
                print(f"[bench] lakebase tx {self._table}: giving up after retry ({exc!r})", flush=True)
                return
            with self._cv:
                self._in_flight = False
                self._cv.notify_all()


class LakebaseRx:
    """Receive side of a Lakebase channel: LISTEN on a background thread, with a
    catch-up SELECT on every (re)connect and id-based dedup, since NOTIFY is
    fire-and-forget and can drop or duplicate around a reconnect.
    """

    def __init__(
        self,
        *,
        config: LakebaseConfig,
        run_id: str,
        can_ids: Sequence[int] | None = None,
        message_types: Sequence[str] | None = None,
        create_schema: bool = True,
    ) -> None:
        self._config = config
        self._table = config.table
        self._channel = notify_channel(config.table)
        self._run_id = run_id
        self._can_ids = set(can_ids) if can_ids else None
        self._message_types = set(message_types) if message_types else None
        self._queue: queue.Queue[Frame] = queue.Queue()
        self._seen = SeenIds()
        self._watermark = 0
        self._stop = threading.Event()
        self._conn = None
        if create_schema:
            # Create the table before the listener thread can race an unprovisioned
            # database: the transmitting Ecu may not have started yet.
            conn = connect(config.profile, config.endpoint_name, config.dbname)
            try:
                ensure_schema(conn, config.table)
            finally:
                conn.close()
        self._thread = threading.Thread(target=self._run, name=f"lakebase-rx-{config.table}", daemon=True)
        self._thread.start()

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        try:
            frames.append(self._queue.get(timeout=timeout))
        except queue.Empty:
            return frames
        while True:
            try:
                frames.append(self._queue.get_nowait())
            except queue.Empty:
                return frames

    def close(self) -> None:
        self._stop.set()
        if self._conn is not None:
            # Closing the connection unblocks the notifies() iterator immediately
            # rather than leaving the thread parked until the next notification.
            try:
                self._conn.close()
            except Exception:
                pass
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._conn = connect(self._config.profile, self._config.endpoint_name, self._config.dbname)
                # LISTEN before catching up: a notification arriving during the catch-up
                # query is then queued by the server rather than lost, and any duplicate
                # it causes is absorbed by the id dedup.
                self._conn.execute(sql(f'LISTEN "{self._channel}"'))
                self._catch_up(self._conn)
                for notify in self._conn.notifies():
                    self._on_notify(self._conn, notify.payload)
                    if self._stop.is_set():
                        return
            except Exception as exc:
                if self._stop.is_set():
                    return
                print(f"[bench] lakebase rx {self._table}: {exc!r}; reconnecting", flush=True)
                self._stop.wait(1.0)

    def _catch_up(self, conn) -> None:
        cur = conn.execute(sql(catchup_sql(self._table)), (self._run_id, self._watermark, CATCHUP_LOOKBACK_SECONDS))
        self._emit(cur.fetchall())

    def _on_notify(self, conn, payload: str) -> None:
        envelope = parse_envelope(payload)
        if envelope["run_id"] != self._run_id:
            return
        if envelope["frames"] is not None:
            self._emit_inline(envelope["ids"], envelope["frames"])
            return
        cur = conn.execute(sql(fetch_ids_sql(self._table)), (self._run_id, envelope["ids"]))
        self._emit(cur.fetchall())

    def _emit_inline(self, ids: Sequence[int], frames: Sequence[Frame]) -> None:
        for row_id, frame in zip(ids, frames):
            if self._seen.add_if_new(row_id) and accepts(
                frame, can_ids=self._can_ids, message_types=self._message_types
            ):
                self._queue.put(frame)
        if ids:
            self._watermark = max(self._watermark, max(ids))

    def _emit(self, rows: Sequence[Sequence]) -> None:
        for row in rows:
            row_id = row[0]
            if not self._seen.add_if_new(row_id):
                continue
            frame = Frame.from_row(dict(zip(FRAME_COLUMNS, row[1:])))
            if accepts(frame, can_ids=self._can_ids, message_types=self._message_types):
                self._queue.put(frame)
            self._watermark = max(self._watermark, row_id)


def on_construct(bus: Bus) -> None:
    """Registered as this transport's Bus on_construct hook -- see
    bench.bus.register_transport. Resolves an unset LakebaseConfig.table from the
    owning Bus's generated name, so two independently-constructed Lakebase() buses
    never silently share a table just because neither caller named one.
    """
    assert isinstance(bus.config, LakebaseConfig)
    bus.config.table = bus.config.table or bus.name


def open_tx(bus: Bus) -> LakebaseTx:
    """Registered as this transport's Bus.open_tx() -- see bench.bus.register_transport."""
    assert isinstance(bus.config, LakebaseConfig)
    return LakebaseTx(config=bus.config)


def open_rx(
    bus: Bus,
    *,
    run_id: str,
    can_ids: Sequence[int] | None = None,
    message_types: Sequence[str] | None = None,
    source_file: str | None = None,
    run_epoch_ns: int | None = None,
) -> LakebaseRx:
    """Registered as this transport's Bus.open_rx() -- see bench.bus.register_transport.

    `source_file`/`run_epoch_ns` are unused: a sender already stamped them onto the
    Frame before it was inserted.
    """
    assert isinstance(bus.config, LakebaseConfig)
    return LakebaseRx(config=bus.config, run_id=run_id, can_ids=can_ids, message_types=message_types)
