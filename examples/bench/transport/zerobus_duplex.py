"""Bidirectional Zerobus channel: one `catalog.schema.table` an Ecu can both send on and
receive from, so a Zerobus bus segment is a peer of `transport.lakebase` rather than a
send-only leg needing a second bus to hear anything back.

The two directions do not use the same mechanism, because Zerobus Ingest has no subscribe
API (see transport/zerobus.py):

* tx -- Zerobus Ingest, delegated to `transport.zerobus.ZerobusTx`. Fire-and-forget gRPC.
* rx -- repeated catch-up SELECTs against the table those writes land in, over the
  Databricks SQL Connector.

So this is a polling channel with warehouse-sized latency: frames surface seconds after
they were sent, in batches, ordered by `observed_ns` -- not a push channel like Lakebase's
LISTEN/NOTIFY. Everything above the channel is unaffected: `ChannelTx`/`ChannelRx` are
satisfied either way, so switching a bus between here and Lakebase is a constructor
change and nothing else.

The rx side filters on `run_id`, which every bench in one run shares, so two benches
talking to each other need one table per direction -- a shared table hands each bench its
own transmissions straight back (`bench.handle.BusHandle` suppresses only the echo of what
that same handle sent).

`bench.db`/`databricks.sdk` are imported lazily inside `_connect()`, so constructing a
`ZerobusDuplexConfig` never requires the SQL connector to be installed.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any

from bench.frame import FRAME_COLUMNS, Frame, accepts
from pydantic import StringConstraints, ValidationInfo, field_validator

from transport.zerobus import ZerobusConfig, ZerobusTx

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bench.bus import Bus

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

_SELECT_COLUMNS = ", ".join(f"`{name}`" for name in FRAME_COLUMNS)


class ZerobusDuplexConfig(ZerobusConfig):
    """A `ZerobusConfig` plus what the receive side needs: which warehouse to query and
    how hard to poll it. Ingest fields are inherited, so one config drives both directions.

    `warehouse_id` is optional because `databricks.sdk.core.Config` resolves
    DATABRICKS_WAREHOUSE_ID (or the active profile's) on its own; naming it here is for
    when a bench must not inherit whatever the environment happens to point at.

    `poll_interval_s` is what keeps an Ecu's drain loop (which polls every 50 ms by
    default) from issuing a warehouse query per iteration: a poll that arrives before the
    interval has elapsed waits instead of querying.

    `lookback_s` re-scans that much of the already-seen `observed_ns` range on every
    query, since Zerobus rows become visible in ingest order, not in `observed_ns` order
    -- a strict high-water mark alone would skip a late-landing frame. Rows the re-scan
    returns twice are dropped by the dedup set.

    Raises `pydantic.ValidationError` on everything ZerobusConfig rejects, plus a
    non-positive interval.
    """

    warehouse_id: _NonEmpty | None = None
    poll_interval_s: float = 2.0
    lookback_s: float = 10.0

    @field_validator("poll_interval_s", "lookback_s")
    @classmethod
    def _positive(cls, value: float, info: ValidationInfo) -> float:
        if value <= 0:
            raise ValueError(f"{info.field_name} must be > 0, got {value!r}")
        return value

    @field_validator("warehouse_id", mode="before")
    @classmethod
    def _blank_warehouse_means_none(cls, value: Any) -> Any:
        return None if isinstance(value, str) and not value.strip() else value

    @classmethod
    def from_zerobus(cls, config: ZerobusConfig, **overrides: Any) -> ZerobusDuplexConfig:
        """Widen an ingest-only config into a duplex one, so both directions of a link are
        derived from one ConnectionConfig rather than configured twice.
        """
        return cls(**config.model_dump(), **overrides)

    @property
    def qualified_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"


def select_sql(config: ZerobusDuplexConfig) -> str:
    """Return the catch-up SELECT: one run's frames at or after a watermark, oldest first."""
    return (
        f"SELECT {_SELECT_COLUMNS} FROM {config.qualified_table}"
        f" WHERE run_id = ? AND observed_ns >= ? ORDER BY observed_ns"
    )


ConnectFn = Callable[[ZerobusDuplexConfig], Any]


def _connect(config: ZerobusDuplexConfig) -> Any:
    from bench.db import connect
    from databricks.sdk.core import Config

    kwargs: dict[str, Any] = {}
    if config.profile:
        kwargs["profile"] = config.profile
    if config.warehouse_id:
        kwargs["warehouse_id"] = config.warehouse_id
    return connect(Config(**kwargs))


class ZerobusDuplexRx:
    """Receive side of a Zerobus bus: repeated catch-up queries against the table the
    transmit side ingests into, deduped by row content.

    Every query re-reads the last `lookback_s` of `observed_ns` (see
    ZerobusDuplexConfig), so a frame that lands late is still delivered; `_seen` is what
    keeps that overlap from delivering it twice, and is pruned to the same window so it
    cannot grow with the run.
    """

    def __init__(
        self,
        *,
        config: ZerobusDuplexConfig,
        run_id: str,
        can_ids: Sequence[int] | None = None,
        message_types: Sequence[str] | None = None,
        connect_fn: ConnectFn | None = None,
    ) -> None:
        self._config = config
        self._run_id = run_id
        self._can_ids = set(can_ids) if can_ids else None
        self._message_types = set(message_types) if message_types else None
        self._connect_fn = connect_fn or _connect
        self._sql = select_sql(config)
        self._lookback_ns = int(config.lookback_s * 1e9)
        self._watermark_ns = 0
        self._seen: dict[tuple[Any, ...], int] = {}
        self._next_query_at = 0.0
        self._conn: Any = None

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        """Return frames that landed since the last call, waiting up to `timeout`.

        Returns an empty list without querying when the poll interval has not elapsed --
        an Ecu's drain loop polls far faster than a warehouse round trip is worth.
        """
        deadline = time.monotonic() + timeout
        while True:
            wait = self._next_query_at - time.monotonic()
            if wait <= 0:
                self._next_query_at = time.monotonic() + self._config.poll_interval_s
                frames = self._fetch()
                if frames:
                    return frames
                wait = self._config.poll_interval_s
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            time.sleep(min(wait, remaining))

    def close(self) -> None:
        self._close_conn()

    def _connection(self) -> Any:
        if self._conn is None:
            self._conn = self._connect_fn(self._config)
        return self._conn

    def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch(self) -> list[Frame]:
        rows = self._query()
        cutoff = max(0, self._watermark_ns - self._lookback_ns)
        frames: list[Frame] = []
        for row in rows:
            values = tuple(row)
            key = values
            if key in self._seen:
                continue
            record = dict(zip(FRAME_COLUMNS, values))
            observed_ns = int(record.get("observed_ns") or 0)
            self._seen[key] = observed_ns
            self._watermark_ns = max(self._watermark_ns, observed_ns)
            frame = Frame.from_row(record)
            if accepts(frame, can_ids=self._can_ids, message_types=self._message_types):
                frames.append(frame)
        self._prune(cutoff)
        return frames

    def _query(self) -> list[Any]:
        params = [self._run_id, max(0, self._watermark_ns - self._lookback_ns)]
        try:
            return self._execute(params)
        except Exception as exc:
            # One reconnect before giving up on this poll: a warehouse connection can be
            # closed under an idle bench, and dropping the run over that would lose every
            # frame still to land.
            print(f"[bench] zerobus_duplex {self._config.table}: {exc!r}; reconnecting and retrying once", flush=True)
            self._close_conn()
            try:
                return self._execute(params)
            except Exception as retry_exc:
                print(f"[bench] zerobus_duplex {self._config.table}: {retry_exc!r}; giving up on this poll", flush=True)
                self._close_conn()
                return []

    def _execute(self, params: list[Any]) -> list[Any]:
        with self._connection().cursor() as cur:
            cur.execute(self._sql, params)
            return list(cur.fetchall())

    def _prune(self, cutoff: int) -> None:
        for key, observed_ns in list(self._seen.items()):
            if observed_ns < cutoff:
                del self._seen[key]


def open_tx(bus: Bus) -> ZerobusTx:
    """Registered as this transport's Bus.open_tx() -- see bench.bus.register_transport.

    The transmit side is Zerobus Ingest itself, unchanged; only the receive side is this
    module's own. `ZerobusDuplexConfig` is a `ZerobusConfig`, so it needs no translation.
    """
    assert isinstance(bus.config, ZerobusDuplexConfig)
    return ZerobusTx(config=bus.config)


def open_rx(
    bus: Bus,
    *,
    run_id: str,
    can_ids: Sequence[int] | None = None,
    message_types: Sequence[str] | None = None,
    source_file: str | None = None,
    run_epoch_ns: int | None = None,
) -> ZerobusDuplexRx:
    """Registered as this transport's Bus.open_rx() -- see bench.bus.register_transport."""
    assert isinstance(bus.config, ZerobusDuplexConfig)
    return ZerobusDuplexRx(config=bus.config, run_id=run_id, can_ids=can_ids, message_types=message_types)
