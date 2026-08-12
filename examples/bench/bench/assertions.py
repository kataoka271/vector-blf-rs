"""Declarative expectations for a run, evaluated against the decoded Delta tables.

Without this, "did the run pass?" is a SQL query somebody remembers to write. An
assertion set is evaluated once the pipeline has caught up (see `bench.db.
wait_for_ingestion`) and materialized into `blf_testbench_assertions` -- so a run's
verdict is a row, and a regression across runs is a query over rows rather than a diff
of two ad-hoc queries.

Each kind compiles to one SQL statement returning a single row of
`(actual, passed, detail)`. They are built as text rather than through a DataFrame API so
the same statements run from a notebook, a job, or `evaluate()` below, and so a failing
assertion can be pasted into a SQL editor as-is.

`LATENCY` is the one that justifies logging every bus segment: it joins a frame's two
hops on `(run_id, can_id, timestamp_ns)`, which is exactly why a forwarding Ecu must
preserve `timestamp_ns` (see `bench.frame.Frame.forwarded`).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence

from databricks.sdk.core import Config

from bench.db import ReplayConfig, connect

RANGE = "range"
PERIOD = "period"
COUNT = "count"
PRESENCE = "presence"
LATENCY = "latency"
FORWARDED = "forwarded"
KINDS = (RANGE, PERIOD, COUNT, PRESENCE, LATENCY, FORWARDED)


@dataclasses.dataclass(frozen=True)
class Assertion:
    """One expectation about a run.

    `kind` selects the check; the other fields are the parameters that kind uses:

    * `range` -- `signal_name` stays within [`min_value`, `max_value`].
    * `period` -- consecutive frames of `can_id` on `channel` are `expected_s` apart,
      within `tolerance` (a fraction, so 0.1 is +/-10%). Checks the mean gap.
    * `count` -- the number of `signal_name` samples is within [`min_value`, `max_value`].
    * `presence` -- `can_id` on `channel` appears at least `min_value` times (default 1).
    * `latency` -- the p99 hop latency from `channel` to `to_channel` is at most
      `max_value` seconds.
    * `forwarded` -- every frame of `can_id` seen on `channel` also appears on
      `to_channel`, i.e. the Ecu under test dropped none of them.
    """

    name: str
    kind: str
    signal_name: str | None = None
    can_id: int | None = None
    channel: int | None = None
    to_channel: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    expected_s: float | None = None
    tolerance: float = 0.1

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"assertion {self.name!r}: kind must be one of {KINDS}, got {self.kind!r}")
        required = {
            RANGE: ("signal_name",),
            PERIOD: ("can_id", "channel", "expected_s"),
            COUNT: ("signal_name",),
            PRESENCE: ("can_id", "channel"),
            LATENCY: ("channel", "to_channel", "max_value"),
            FORWARDED: ("can_id", "channel", "to_channel"),
        }[self.kind]
        missing = [field for field in required if getattr(self, field) is None]
        if missing:
            raise ValueError(f"assertion {self.name!r}: kind {self.kind!r} needs {missing}")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_sql(assertion: Assertion, *, source_file: str, gold: str, silver_can: str) -> str:
    """Return the statement evaluating one assertion.

    `gold` and `silver_can` are fully-qualified table names (`ReplayConfig.gold_table`/
    `.silver_can_table`). The statement yields exactly one row of
    `(name, kind, expected, actual, passed)`.
    """
    src = _quote(source_file)
    name, kind = _quote(assertion.name), _quote(assertion.kind)
    head = f"SELECT {name} AS name, {kind} AS kind"

    def required(field: str):
        # Assertion.__post_init__ already rejected a kind missing its parameters, so this
        # only re-states that guarantee where the value is used.
        value = getattr(assertion, field)
        assert value is not None, f"{assertion.kind} assertion reached build_sql without {field}"
        return value

    if assertion.kind == RANGE:
        lo = assertion.min_value if assertion.min_value is not None else float("-inf")
        hi = assertion.max_value if assertion.max_value is not None else float("inf")
        return (
            f"{head}, {_quote(f'[{lo}, {hi}]')} AS expected,"
            " concat('[', coalesce(cast(min(signal_value) AS string), 'n/a'), ', ',"
            " coalesce(cast(max(signal_value) AS string), 'n/a'), ']') AS actual,"
            f" count(*) > 0 AND min(signal_value) >= {lo} AND max(signal_value) <= {hi} AS passed"
            f" FROM {gold} WHERE _source_file = {src} AND signal_name = {_quote(required('signal_name'))}"
        )

    if assertion.kind == COUNT:
        lo = assertion.min_value if assertion.min_value is not None else 0
        hi = assertion.max_value if assertion.max_value is not None else float("inf")
        return (
            f"{head}, {_quote(f'{lo}..{hi} samples')} AS expected,"
            " cast(count(*) AS string) AS actual,"
            f" count(*) >= {lo} AND count(*) <= {hi} AS passed"
            f" FROM {gold} WHERE _source_file = {src} AND signal_name = {_quote(required('signal_name'))}"
        )

    if assertion.kind == PRESENCE:
        minimum = int(assertion.min_value) if assertion.min_value is not None else 1
        return (
            f"{head}, {_quote(f'>= {minimum} frames')} AS expected,"
            " cast(count(*) AS string) AS actual,"
            f" count(*) >= {minimum} AS passed"
            f" FROM {silver_can} WHERE _source_file = {src}"
            f" AND can_id = {assertion.can_id} AND channel = {assertion.channel}"
        )

    if assertion.kind == PERIOD:
        lo = required("expected_s") * (1 - assertion.tolerance)
        hi = required("expected_s") * (1 + assertion.tolerance)
        # Mean of the consecutive gaps rather than every gap individually: one scheduling
        # hiccup should not fail a period check that is otherwise steady.
        return (
            f"WITH gaps AS (SELECT timestamp_s - lag(timestamp_s) OVER (ORDER BY timestamp_s) AS gap"
            f" FROM {silver_can} WHERE _source_file = {src}"
            f" AND can_id = {assertion.can_id} AND channel = {assertion.channel})"
            f" {head}, {_quote(f'{assertion.expected_s}s +/-{assertion.tolerance:.0%}')} AS expected,"
            " coalesce(cast(avg(gap) AS string), 'n/a') AS actual,"
            f" avg(gap) BETWEEN {lo} AND {hi} AS passed FROM gaps WHERE gap IS NOT NULL"
        )

    if assertion.kind == LATENCY:
        return (
            f"WITH hops AS (SELECT unix_micros(b.event_time) - unix_micros(a.event_time) AS us"
            f" FROM {silver_can} a JOIN {silver_can} b"
            "  ON a._source_file = b._source_file AND a.can_id = b.can_id"
            "  AND a.timestamp_ns = b.timestamp_ns"
            f" WHERE a._source_file = {src}"
            f" AND a.channel = {assertion.channel} AND b.channel = {assertion.to_channel})"
            f" {head}, {_quote(f'p99 <= {assertion.max_value}s')} AS expected,"
            " coalesce(cast(percentile(us, 0.99) / 1e6 AS string), 'n/a') AS actual,"
            f" percentile(us, 0.99) / 1e6 <= {assertion.max_value} AS passed FROM hops"
        )

    # FORWARDED
    return (
        f"WITH sent AS (SELECT timestamp_ns FROM {silver_can} WHERE _source_file = {src}"
        f" AND can_id = {assertion.can_id} AND channel = {assertion.channel}),"
        f" got AS (SELECT timestamp_ns FROM {silver_can} WHERE _source_file = {src}"
        f" AND can_id = {assertion.can_id} AND channel = {assertion.to_channel})"
        f" {head}, 'every frame forwarded' AS expected,"
        " concat(cast((SELECT count(*) FROM got) AS string), '/',"
        " cast((SELECT count(*) FROM sent) AS string)) AS actual,"
        " (SELECT count(*) FROM sent) > 0"
        " AND NOT EXISTS (SELECT 1 FROM sent LEFT ANTI JOIN got USING (timestamp_ns)) AS passed"
    )


def build_report_sql(
    assertions: Sequence[Assertion],
    *,
    run_id: str,
    source_file: str,
    gold: str,
    silver_can: str,
) -> str:
    """Return one statement evaluating every assertion, as a UNION ALL with `run_id`.

    The result is the shape of `blf_testbench_assertions`:
    `(run_id, name, kind, expected, actual, passed)`. Raises ValueError for an empty set.
    """
    if not assertions:
        raise ValueError("cannot build a report for an empty assertion set")
    parts = [
        f"SELECT {_quote(run_id)} AS run_id, * FROM ("
        + build_sql(a, source_file=source_file, gold=gold, silver_can=silver_can)
        + ")"
        for a in assertions
    ]
    return "\nUNION ALL\n".join(parts)


def build_write_sql(report_sql: str, *, table: str, run_id: str) -> list[str]:
    """Return the statements that replace `run_id`'s rows in `table` with a fresh report.

    Delete-then-insert rather than a MERGE: a rerun of the same run should leave exactly
    one set of rows, and the assertion set itself may have changed between runs, so
    matching on name would strand rows for assertions that no longer exist.
    """
    return [
        f"CREATE TABLE IF NOT EXISTS {table} ("
        " run_id STRING, name STRING, kind STRING, expected STRING, actual STRING, passed BOOLEAN,"
        " evaluated_at TIMESTAMP)",
        f"DELETE FROM {table} WHERE run_id = {_quote(run_id)}",
        f"INSERT INTO {table} SELECT *, current_timestamp() FROM ({report_sql})",
    ]


def hop_latency_sql(*, source_file: str, silver_can: str, from_channel: int, to_channel: int) -> str:
    """Return the per-message hop latency query.

    Yields `(can_id, frames, p50_us, p99_us)` for frames observed on both channels. This
    is the standalone form of the LATENCY assertion, for exploring a run rather than
    passing or failing it.
    """
    src = _quote(source_file)
    return (
        "SELECT a.can_id,"
        " count(*) AS frames,"
        " percentile(unix_micros(b.event_time) - unix_micros(a.event_time), 0.5) AS p50_us,"
        " percentile(unix_micros(b.event_time) - unix_micros(a.event_time), 0.99) AS p99_us"
        f" FROM {silver_can} a JOIN {silver_can} b"
        " ON a._source_file = b._source_file AND a.can_id = b.can_id"
        " AND a.timestamp_ns = b.timestamp_ns"
        f" WHERE a._source_file = {src} AND a.channel = {from_channel} AND b.channel = {to_channel}"
        " GROUP BY a.can_id ORDER BY a.can_id"
    )


def evaluate(
    cfg: ReplayConfig,
    *,
    run_id: str,
    source_file: str,
    assertions: Sequence[Assertion],
    table: str | None = None,
    dbx_cfg: Config | None = None,
    execute_fn: Callable[[str], None] | None = None,
) -> None:
    """Evaluate `assertions` against `source_file`'s decoded rows and materialize the
    verdicts into `table` (default: `` `cfg.catalog`.`cfg.schema`.`blf_testbench_assertions` ``).

    `execute_fn`, if given, is called with each SQL statement instead of opening a real
    Databricks connection -- injectable for tests, mirroring `db.wait_for_ingestion`'s
    `count_fn`. Otherwise runs the statements over `bench.db`'s SQL connector.
    """
    table = table or f"`{cfg.catalog}`.`{cfg.schema}`.`blf_testbench_assertions`"
    report_sql = build_report_sql(
        assertions, run_id=run_id, source_file=source_file, gold=cfg.gold_table, silver_can=cfg.silver_can_table
    )
    statements = build_write_sql(report_sql, table=table, run_id=run_id)

    if execute_fn is not None:
        for stmt in statements:
            execute_fn(stmt)
        return

    dbx_cfg = dbx_cfg or Config()
    with connect(dbx_cfg) as conn:
        for stmt in statements:
            with conn.cursor() as cur:
                cur.execute(stmt)
