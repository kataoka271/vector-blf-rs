"""Databricks SQL Connector query helpers, and the adapter turning a fetched DataFrame
into a paced list[Frame] for GeneratorEcu/ReplayEcu.

Fetches the raw CAN frames to replay in two steps: (1) filter `blf_gold_signals` down to
the (_source_file, channel, timestamp_ns) key set the configured FilterSpec selects, then
(2) join that key set against `blf_silver_can` for the exact original frame bytes.
`blf_gold_signals` only carries decoded signal_name/signal_value, not raw can_id/data, so
sourcing the replay bytes from blf_silver_can replays the bit-exact original frames
instead of re-encoding signal values.
"""

from __future__ import annotations

import dataclasses
import os
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import yaml
from databricks.sdk.core import Config

from bench.frame import CAN, CAN_FD, DIR_RX, Frame
from databricks import sql


@dataclasses.dataclass
class FilterSpec:
    """Selects which blf_gold_signals events feed a replay."""

    source_files: list[str] | None = None
    t_start_s: float | None = None
    t_end_s: float | None = None
    channels: list[int] | None = None
    signal_source: list[str] = dataclasses.field(default_factory=lambda: ["CAN"])
    exclude_dirs: list[str] = dataclasses.field(default_factory=lambda: ["Tx"])
    exclude_source_prefix: str = "testbench/"
    extra_where: str | None = None

    def build_where(self) -> tuple[str, list]:
        """Build a `WHERE`-clause predicate (without the `WHERE` keyword) and its `?`-placeholder parameters, in placeholder order."""
        clauses: list[str] = []
        params: list = []

        if self.source_files:
            clauses.append("_source_file IN (" + ", ".join(["?"] * len(self.source_files)) + ")")
            params += list(self.source_files)

        if self.exclude_source_prefix:
            clauses.append("_source_file NOT LIKE ?")
            params.append(f"{self.exclude_source_prefix}%")

        if self.t_start_s is not None:
            clauses.append("timestamp_s >= ?")
            params.append(self.t_start_s)

        if self.t_end_s is not None:
            clauses.append("timestamp_s <= ?")
            params.append(self.t_end_s)

        if self.channels:
            clauses.append("channel IN (" + ", ".join(["?"] * len(self.channels)) + ")")
            params += list(self.channels)

        if self.signal_source:
            clauses.append("signal_source IN (" + ", ".join(["?"] * len(self.signal_source)) + ")")
            params += list(self.signal_source)

        if self.exclude_dirs:
            clauses.append("dir NOT IN (" + ", ".join(["?"] * len(self.exclude_dirs)) + ")")
            params += list(self.exclude_dirs)

        if self.extra_where:
            clauses.append(f"({self.extra_where})")

        if not clauses:
            return "1 = 1", []
        return " AND ".join(clauses), params


@dataclasses.dataclass
class ReplayConfig:
    """Where to fetch replay frames from, and how to filter them. Distinct from
    `bench.bench.TestBench` (the run orchestrator) -- this only configures one Ecu's
    Databricks fetch.
    """

    catalog: str = dataclasses.field(default_factory=lambda: os.environ.get("BLF_CATALOG", "main"))
    schema: str = dataclasses.field(default_factory=lambda: os.environ.get("BLF_SCHEMA", "blf"))
    filter: FilterSpec = dataclasses.field(default_factory=FilterSpec)

    @property
    def gold_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`blf_gold_signals`"

    @property
    def silver_can_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`blf_silver_can`"

    @classmethod
    def from_yaml(cls, path: str | Path) -> ReplayConfig:
        """Load a ReplayConfig from a YAML file. Missing sections/keys keep their
        dataclass defaults.
        """
        data = yaml.safe_load(Path(path).read_text()) or {}
        filter_data = data.get("filter", {})
        filter_spec = FilterSpec(**filter_data) if filter_data else FilterSpec()
        kwargs = {k: v for k, v in data.items() if k != "filter"}
        return cls(filter=filter_spec, **kwargs)


def connect(dbx_cfg: Config):
    """Open a Databricks SQL Connector connection for `dbx_cfg`'s warehouse.

    Public: used by this module's own query functions and by `bench.assertions.evaluate`
    -- not just an internal helper, so it isn't underscore-prefixed. Raises RuntimeError
    if no SQL warehouse is configured.
    """
    if not dbx_cfg.warehouse_id:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set (or missing from the active .databrickscfg "
            "profile). Set it to a SQL Warehouse id before running."
        )
    return sql.connect(
        server_hostname=dbx_cfg.host,
        http_path=f"/sql/1.0/warehouses/{dbx_cfg.warehouse_id}",
        # sql.connect expects a CredentialsProvider: Callable[[], Callable[[], Dict[str, str]]].
        # Config.authenticate is the inner header factory (calling it returns headers
        # directly), so it must be wrapped, not passed bare -- passing it bare makes the
        # connector call it once too many, then try to call the resulting headers dict as
        # a function.
        credentials_provider=lambda: dbx_cfg.authenticate,
    )


def fetch_replay_frames(cfg: ReplayConfig, dbx_cfg: Config | None = None) -> pd.DataFrame:
    """Return the raw CAN frames matching cfg.filter, ordered by timestamp_ns.

    Columns: timestamp_ns, timestamp_s, channel, can_id, is_ext_id, rtr, dlc, data, dir,
    message_type. message_type ("CAN"/"CAN_FD"/"CAN_FD64") is the pipeline's own
    is-this-FD determination (from the original BLF object type), not something
    re-derived from dlc -- a CAN-FD frame with an 8-byte-or-smaller payload has the same
    raw dlc code as classic CAN, so dlc alone cannot tell them apart.
    """
    dbx_cfg = dbx_cfg or Config()
    where, params = cfg.filter.build_where()
    stmt = (
        f"SELECT s.timestamp_ns, s.timestamp_s, s.channel, s.can_id, s.is_ext_id,"
        f" s.rtr, s.dlc, s.data, s.dir, s.message_type"
        f" FROM {cfg.silver_can_table} s"
        f" JOIN ("
        f"   SELECT DISTINCT _source_file, channel, timestamp_ns"
        f"   FROM {cfg.gold_table} WHERE {where}"
        f" ) g"
        f" ON s._source_file = g._source_file"
        f" AND s.channel = g.channel"
        f" AND s.timestamp_ns = g.timestamp_ns"
        f" ORDER BY s.timestamp_ns"
    )
    print(f"[fetch_replay_frames] stmt={stmt!r} params={params!r}", flush=True)
    with connect(dbx_cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params or None)
            df = cur.fetchall_arrow().to_pandas()
    print(f"[fetch_replay_frames] -> {len(df)} frame(s)", flush=True)
    return df


def _count_source_file_rows(cfg: ReplayConfig, source_file: str, dbx_cfg: Config | None = None) -> int:
    """Return how many blf_gold_signals rows currently exist for source_file."""
    dbx_cfg = dbx_cfg or Config()
    stmt = f"SELECT count(*) FROM {cfg.gold_table} WHERE _source_file = ?"
    with connect(dbx_cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, [source_file])
            return cur.fetchone()[0]


def wait_for_ingestion(
    cfg: ReplayConfig,
    source_file: str,
    expected_count: int,
    *,
    dbx_cfg: Config | None = None,
    timeout_s: float = 60.0,
    poll_s: float = 2.0,
    count_fn: Callable[[ReplayConfig, str, Config | None], int] = _count_source_file_rows,
) -> int:
    """Block until `source_file` has at least `expected_count` rows in blf_gold_signals.

    A captured frame only becomes usable as a filter for a downstream Ecu once
    blf_ingestion has streamed it through blf_bronze -> blf_silver_can_signals ->
    blf_gold_signals, which lags the Zerobus upload by the pipeline's own latency. This
    polls rather than blocking on the upload itself, since Zerobus Ingest acknowledges a
    write once Delta has it, not once blf_ingestion has read it.

    `expected_count <= 0` returns 0 immediately. Raises TimeoutError if `expected_count`
    is not reached within `timeout_s`.
    """
    if expected_count <= 0:
        return 0
    deadline = time.monotonic() + timeout_s
    while True:
        count = count_fn(cfg, source_file, dbx_cfg)
        if count >= expected_count:
            return count
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"source_file={source_file!r}: only {count}/{expected_count} row(s) visible in "
                f"blf_gold_signals after {timeout_s}s"
            )
        time.sleep(poll_s)


def to_frames(df: pd.DataFrame, *, run_id: str, source_file: str, channel: int) -> list[Frame]:
    """Convert fetch_replay_frames' rows into a paced list[Frame].

    `timestamp_ns` on each Frame is set to its relative offset from the first row (so
    consecutive frames' timestamp_ns deltas reproduce the recorded gaps, in nanoseconds,
    for a caller to sleep by) -- this is the replay's relative position, not when it is
    actually sent. `observed_ns` is left at 0 as a placeholder: the caller should
    re-stamp each frame via `frame.forwarded(channel=frame.channel)` right before
    sending, so `observed_ns` reflects the actual wall-clock instant it went out rather
    than the instant this conversion ran.
    """
    if df.empty:
        return []
    t0 = float(df["timestamp_s"].min())
    frames = []
    for row in df.to_dict(orient="records"):
        data = bytes(row["data"])
        is_fd = row["message_type"] != "CAN"
        relative_ns = max(0, round((float(row["timestamp_s"]) - t0) * 1e9))
        frames.append(
            Frame(
                run_id=run_id,
                source_file=source_file,
                message_type=CAN_FD if is_fd else CAN,
                timestamp_ns=relative_ns,
                observed_ns=0,
                channel=channel,
                dir=DIR_RX,
                data=data,
                can_id=int(row["can_id"]),
                is_ext_id=bool(row["is_ext_id"]),
                rtr=bool(row["rtr"]),
                # BLF DLC is a code (e.g. 15 -> 64 bytes for CAN-FD); use the actual byte
                # length for FD frames rather than the raw code.
                dlc=len(data) if is_fd else int(row["dlc"]),
                is_fd=is_fd,
            )
        )
    return frames
