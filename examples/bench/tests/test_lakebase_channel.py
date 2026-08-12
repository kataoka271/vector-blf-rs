"""Offline tests for transport/lakebase.py's SQL builders and helpers -- no Postgres
connection required.
"""

from __future__ import annotations

import pytest
from bench.frame import accepts, make_can_frame
from transport.lakebase import (
    SeenIds,
    catchup_sql,
    check_identifier,
    ensure_schema_sql,
    insert_and_notify_sql,
    insert_params,
    notify_channel,
    select_by_id_sql,
)

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000


def _frame():
    return make_can_frame(
        run_id=RUN_ID,
        source_file="testbench/run_001.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=0x310,
        data=b"\x01\x02",
    )


def test_check_identifier_accepts_bare_names():
    assert check_identifier("bus_frames") == "bus_frames"


def test_check_identifier_rejects_qualified_names():
    with pytest.raises(ValueError, match="bare identifier"):
        check_identifier('schema."bus_frames"')


def test_notify_channel_is_the_table_name():
    assert notify_channel("bus_frames") == "bus_frames"


def test_ensure_schema_sql_declares_every_frame_column():
    ddl = ensure_schema_sql("bus_frames")
    assert '"bus_frames"' in ddl
    for column in ("run_id", "source_file", "message_type", "can_id", "vlan_id"):
        assert column in ddl


def test_insert_and_notify_sql_is_one_statement_with_returning_and_notify():
    stmt = insert_and_notify_sql("bus_frames")
    assert "INSERT INTO" in stmt
    assert "RETURNING id" in stmt
    assert "pg_notify" in stmt


def test_insert_params_orders_frame_columns_then_channel():
    params = insert_params(_frame(), "bus_frames")
    assert params[0] == RUN_ID  # run_id is FRAME_COLUMNS[0]
    assert params[-1] == "bus_frames"  # notify channel argument appended last


def test_catchup_sql_and_select_by_id_sql_reference_the_table():
    assert '"bus_frames"' in catchup_sql("bus_frames")
    assert '"bus_frames"' in select_by_id_sql("bus_frames")


def test_seen_ids_deduplicates_and_evicts_oldest_past_capacity():
    seen = SeenIds(capacity=2)
    assert seen.add_if_new(1) is True
    assert seen.add_if_new(1) is False
    assert seen.add_if_new(2) is True
    assert seen.add_if_new(3) is True  # evicts 1
    assert len(seen) == 2
    assert seen.add_if_new(1) is True  # 1 was evicted, so it counts as new again


def test_accepts_can_id_and_message_type_filters():
    frame = _frame()
    assert accepts(frame, can_ids=None, message_types=None) is True
    assert accepts(frame, can_ids={0x310}, message_types=None) is True
    assert accepts(frame, can_ids={0x999}, message_types=None) is False
    assert accepts(frame, can_ids=None, message_types={"CAN"}) is True
    assert accepts(frame, can_ids=None, message_types={"ETH"}) is False
