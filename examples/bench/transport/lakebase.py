"""Lakebase (managed Postgres) connection and wire-format helpers shared by
`bench.channel.LakebaseTx`/`LakebaseRx`.

Deliberately simpler than a Nagle-batched transport (see the plan's "explicitly
deferred" table): `LakebaseTx.send()` does one synchronous `INSERT ... RETURNING id` +
`pg_notify(channel, id::text)` per frame, so the SQL builders here only ever deal with a
single row at a time -- no NOTIFY-payload-size envelope, no inline-vs-id-range branching.
NOTIFY is still fire-and-forget, though, so a receiver still needs to catch up on
connect and dedupe by row id -- that part is not a batching artifact and stays even at
this minimal scope.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import psycopg
from bench.frame import FRAME_COLUMNS, Frame
from databricks.sdk import WorkspaceClient

DEFAULT_DATABASE = "databricks_postgres"

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
    """Create `table` and its indexes if absent, on an already-open connection."""
    conn.execute(ensure_schema_sql(table).encode("utf-8"))


def insert_and_notify_sql(table: str) -> str:
    """Return the statement that inserts one frame and notifies its row id.

    Placeholders bind, in order: the frame's FRAME_COLUMNS values, then the notification
    channel. One statement so the NOTIFY fires inside the INSERT's own transaction.
    """
    check_identifier(table)
    placeholders = ", ".join(["%s"] * len(FRAME_COLUMNS))
    return (
        f'WITH ins AS (INSERT INTO "{table}" ({_quoted_columns()}) VALUES ({placeholders}) RETURNING id) '
        "SELECT pg_notify(%s, id::text) FROM ins"
    )


def insert_params(frame: Frame, channel: str) -> list:
    """Return the bound parameters for insert_and_notify_sql, in placeholder order."""
    row = frame.to_row()
    return [row[c] for c in FRAME_COLUMNS] + [channel]


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


def select_by_id_sql(table: str) -> str:
    """Return the query fetching one frame by its bus row id.

    Placeholders bind: run_id, id.
    """
    check_identifier(table)
    return f'SELECT id, {_quoted_columns()} FROM "{table}" WHERE run_id = %s AND id = %s'


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


def accepts(frame: Frame, *, can_ids: set[int] | None, message_types: set[str] | None) -> bool:
    """Return whether a receiver with these filters wants `frame`.

    A None filter accepts everything. `can_ids` never matches a non-CAN frame.
    """
    if message_types is not None and frame.message_type not in message_types:
        return False
    if can_ids is not None and frame.can_id not in can_ids:
        return False
    return True
