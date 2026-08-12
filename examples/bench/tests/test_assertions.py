"""Tests for the assertion compiler. Statements are checked structurally rather than
executed -- there is no warehouse here -- so these cover the parts that are easy to get
silently wrong: scoping to one run, the join key the latency check depends on, SQL
string quoting, and evaluate()'s statement dispatch via a fake execute_fn.
"""

from __future__ import annotations

import pytest
from bench.assertions import (
    Assertion,
    build_report_sql,
    build_sql,
    build_write_sql,
    evaluate,
    hop_latency_sql,
)
from bench.db import ReplayConfig

GOLD = "`main`.`blf`.`blf_gold_signals`"
SILVER = "`main`.`blf`.`blf_silver_can`"
SOURCE = "testbench/run_001.blf"


def sql(assertion: Assertion) -> str:
    return build_sql(assertion, source_file=SOURCE, gold=GOLD, silver_can=SILVER)


def every_kind() -> list[Assertion]:
    return [
        Assertion(name="sine range", kind="range", signal_name="Sine", min_value=-100, max_value=100),
        Assertion(name="sine count", kind="count", signal_name="Sine", min_value=10),
        Assertion(name="counter present", kind="presence", can_id=0x312, channel=2),
        Assertion(name="tick period", kind="period", can_id=0x310, channel=1, expected_s=1.0),
        Assertion(name="hop latency", kind="latency", channel=1, to_channel=2, max_value=0.1),
        Assertion(name="no drops", kind="forwarded", can_id=0x310, channel=1, to_channel=2),
    ]


@pytest.mark.parametrize("assertion", every_kind(), ids=lambda a: a.kind)
def test_every_kind_is_scoped_to_one_run_and_yields_the_report_columns(assertion):
    stmt = sql(assertion)
    assert f"'{SOURCE}'" in stmt
    for column in ("AS name", "AS kind", "AS expected", "AS actual", "AS passed"):
        assert column in stmt, f"{assertion.kind} is missing {column}"


def test_range_uses_the_gold_table_and_period_uses_silver():
    assert GOLD in sql(every_kind()[0])
    assert SILVER in sql(every_kind()[3])


def test_range_fails_when_no_samples_exist():
    # count(*) > 0 in the predicate: min/max over an empty set are NULL, which would
    # otherwise make a signal that never appeared look like it passed.
    assert "count(*) > 0" in sql(every_kind()[0])


def test_latency_joins_on_the_cross_hop_correlation_key():
    stmt = sql(every_kind()[4])
    assert "a.can_id = b.can_id" in stmt
    assert "a.timestamp_ns = b.timestamp_ns" in stmt
    assert "a.channel = 1" in stmt and "b.channel = 2" in stmt


def test_forwarded_compares_both_channels_of_one_message():
    stmt = sql(every_kind()[5])
    assert "channel = 1" in stmt and "channel = 2" in stmt
    assert "LEFT ANTI JOIN" in stmt


def test_period_tolerance_becomes_a_numeric_window():
    stmt = sql(Assertion(name="p", kind="period", can_id=1, channel=1, expected_s=2.0, tolerance=0.25))
    assert "BETWEEN 1.5 AND 2.5" in stmt


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="kind must be one of"):
        Assertion(name="x", kind="vibes")


def test_missing_parameters_for_a_kind_are_named():
    with pytest.raises(ValueError, match=r"needs \['to_channel', 'max_value'\]"):
        Assertion(name="x", kind="latency", channel=1)


def test_names_with_quotes_are_escaped_not_injected():
    stmt = sql(Assertion(name="it's fine", kind="range", signal_name="a'b"))
    assert "'it''s fine'" in stmt
    assert "'a''b'" in stmt


def test_report_unions_every_assertion_and_tags_the_run():
    stmt = build_report_sql(every_kind(), run_id="run_001", source_file=SOURCE, gold=GOLD, silver_can=SILVER)
    assert stmt.count("UNION ALL") == len(every_kind()) - 1
    assert stmt.count("'run_001' AS run_id") == len(every_kind())


def test_report_rejects_an_empty_set():
    with pytest.raises(ValueError, match="empty assertion set"):
        build_report_sql([], run_id="r", source_file=SOURCE, gold=GOLD, silver_can=SILVER)


def test_write_replaces_only_this_run_s_rows():
    stmts = build_write_sql("SELECT 1", table="`main`.`blf`.`blf_testbench_assertions`", run_id="run_001")
    assert stmts[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert stmts[1] == "DELETE FROM `main`.`blf`.`blf_testbench_assertions` WHERE run_id = 'run_001'"
    assert stmts[2].startswith("INSERT INTO `main`.`blf`.`blf_testbench_assertions`")


def test_hop_latency_query_groups_per_message():
    stmt = hop_latency_sql(source_file=SOURCE, silver_can=SILVER, from_channel=1, to_channel=2)
    assert "GROUP BY a.can_id" in stmt
    assert "p50_us" in stmt and "p99_us" in stmt


def test_evaluate_runs_every_write_statement_through_execute_fn():
    executed = []
    evaluate(
        ReplayConfig(catalog="main", schema="blf"),
        run_id="run_001",
        source_file=SOURCE,
        assertions=[Assertion(name="present", kind="presence", can_id=0x310, channel=1)],
        execute_fn=executed.append,
    )
    assert len(executed) == 3
    assert executed[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert "blf_testbench_assertions" in executed[0]


def test_evaluate_defaults_the_table_from_cfg_catalog_and_schema():
    executed = []
    evaluate(
        ReplayConfig(catalog="main", schema="blf_dev"),
        run_id="run_001",
        source_file=SOURCE,
        assertions=[Assertion(name="present", kind="presence", can_id=0x310, channel=1)],
        execute_fn=executed.append,
    )
    assert "`main`.`blf_dev`.`blf_testbench_assertions`" in executed[0]
