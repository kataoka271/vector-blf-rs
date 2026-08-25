"""Offline tests for transport/lakebase.py's SQL builders and NOTIFY envelope -- no
Postgres connection required. The same statements and payloads are exercised against a
real Lakebase endpoint in `online/test_lakebase.py`.
"""

from __future__ import annotations

import json
from typing import cast

import psycopg
import pytest
from bench.frame import FRAME_COLUMNS, make_can_frame, make_eth_frame
from transport.lakebase import (
    _ID_ARRAY_OVERHEAD,
    _ID_BYTES_PER_FRAME,
    NOTIFY_PAYLOAD_LIMIT,
    SeenIds,
    build_envelope,
    catchup_sql,
    check_identifier,
    ensure_schema,
    ensure_schema_sql,
    fetch_ids_sql,
    insert_and_notify_sql,
    insert_params,
    notify_channel,
    parse_envelope,
)

RUN_ID = "run_001"
SOURCE_FILE = "testbench/run_001.blf"
EPOCH_NS = 1_700_000_000_000_000_000


def _frame(can_id: int = 0x310, *, data: bytes = b"\x01\x02"):
    return make_can_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=can_id,
        data=data,
    )


def _eth_frame(payload_len: int = 1500):
    return make_eth_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=2,
        src_addr=bytes.fromhex("001122334455"),
        dst_addr=bytes.fromhex("66778899aabb"),
        ether_type=0x0800,
        data=bytes(payload_len),
    )


def _merge_ids(payload: str, ids: list[int]) -> str:
    """Apply what insert_and_notify_sql's jsonb merge does server-side."""
    return json.dumps({**json.loads(payload), "ids": ids})


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


def test_ensure_schema_swallows_a_concurrent_creation_race():
    """Two sessions racing CREATE TABLE IF NOT EXISTS on a brand-new table (e.g. the
    Tx and Rx of a bus's first two containers starting at once) can both pass
    Postgres's existence check before either commits -- the loser sees a
    UniqueViolation on the system catalog, not a clean no-op. ensure_schema() should
    treat that the same as "the table already exists".
    """

    class _RacingConn:
        def execute(self, _stmt):
            raise psycopg.errors.UniqueViolation("duplicate key value violates unique constraint")

    ensure_schema(cast(psycopg.Connection, _RacingConn()), "bus_frames")  # must not raise


def test_seen_ids_deduplicates_and_evicts_oldest_past_capacity():
    seen = SeenIds(capacity=2)
    assert seen.add_if_new(1) is True
    assert seen.add_if_new(1) is False
    assert seen.add_if_new(2) is True
    assert seen.add_if_new(3) is True  # evicts 1
    assert len(seen) == 2
    assert seen.add_if_new(1) is True  # 1 was evicted, so it counts as new again


# ── batch SQL construction ──────────────────────────────────────────────────


def test_insert_sql_has_one_placeholder_group_per_frame_plus_channel_and_envelope():
    stmt = insert_and_notify_sql("bus_frames", 3)
    assert stmt.count("%s") == 3 * len(FRAME_COLUMNS) + 2
    assert "pg_notify" in stmt
    assert "jsonb_agg(id ORDER BY id)" in stmt


def test_insert_sql_rejects_empty_batch():
    with pytest.raises(ValueError, match="batch_size"):
        insert_and_notify_sql("bus_frames", 0)


def test_insert_params_are_row_major_then_channel_then_envelope():
    frames = [_frame(0x310), _frame(0x311)]
    params = insert_params(frames, "bus_frames", "{}")
    assert len(params) == len(FRAME_COLUMNS) * 2 + 2
    can_id_index = FRAME_COLUMNS.index("can_id")
    assert params[can_id_index] == 0x310
    assert params[len(FRAME_COLUMNS) + can_id_index] == 0x311
    assert params[-2:] == ["bus_frames", "{}"]


def test_catchup_sql_combines_watermark_with_a_time_lookback():
    stmt = catchup_sql("bus_frames")
    # Both halves are required: ids can become visible out of order, so the watermark
    # alone would step over a row whose transaction committed late.
    assert "id > %s" in stmt
    assert "created_at > now() - make_interval(secs => %s)" in stmt
    assert stmt.strip().endswith("ORDER BY id")


def test_fetch_ids_sql_selects_exact_ids():
    assert "id = ANY(%s)" in fetch_ids_sql("bus_frames")


# ── envelope ─────────────────────────────────────────────────────────────────


def test_small_can_batch_is_inline():
    payload, inline = build_envelope([_frame()] * 4)
    assert inline
    assert json.loads(payload)["frames"]


def test_envelope_hoists_identity_out_of_frames():
    obj = json.loads(build_envelope([_frame()])[0])
    assert obj["rid"] == RUN_ID
    assert obj["sf"] == SOURCE_FILE
    assert "rid" not in obj["frames"][0]


def test_large_ethernet_batch_falls_back_to_fetch():
    # A 1500-byte payload is 2000 base64 characters, so a handful of full-size Ethernet
    # frames is past the NOTIFY cap while a single one is comfortably inside it.
    payload, inline = build_envelope([_eth_frame()] * 5)
    assert not inline
    assert "frames" not in json.loads(payload)
    assert len(payload.encode()) < NOTIFY_PAYLOAD_LIMIT


def test_single_ethernet_frame_still_fits_inline():
    _payload, inline = build_envelope([_eth_frame()])
    assert inline


def test_inline_decision_tracks_the_payload_cap():
    # Two invariants, independent of where the exact cut-off lands: every payload the
    # encoder emits fits under the cap once the ids are merged in, and the decision is
    # monotone in batch size (a batch never goes back to inline as it grows).
    seen_fallback = False
    for count in range(1, 8):
        payload, inline = build_envelope([_eth_frame()] * count)
        reserved = _ID_ARRAY_OVERHEAD + _ID_BYTES_PER_FRAME * count
        assert len(payload.encode()) + reserved <= NOTIFY_PAYLOAD_LIMIT
        if inline:
            assert not seen_fallback
        else:
            seen_fallback = True
    assert seen_fallback, "no batch size in range exercised the fetch fallback"


def test_inline_payload_plus_merged_ids_stays_under_the_cap():
    # build_envelope reserves room for the ids the INSERT merges in; the reservation has
    # to actually cover a realistic worst case, or the NOTIFY is rejected at runtime.
    frames = [_frame()] * 60
    payload, inline = build_envelope(frames)
    if inline:
        merged = _merge_ids(payload, [9_223_372_036_854_775_807] * len(frames))
        assert len(merged.encode()) <= NOTIFY_PAYLOAD_LIMIT


def test_envelope_rejects_empty_batch():
    with pytest.raises(ValueError, match="empty batch"):
        build_envelope([])


def test_envelope_rejects_mixed_run_ids():
    other = make_can_frame(
        run_id="run_002",
        source_file="testbench/run_002.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=1,
        data=b"",
    )
    with pytest.raises(ValueError, match="share run_id"):
        build_envelope([_frame(), other])


def test_parse_envelope_round_trips_inline_frames():
    frames = [_frame(0x310), _frame(0x311)]
    payload, _ = build_envelope(frames)
    parsed = parse_envelope(_merge_ids(payload, [11, 12]))
    assert parsed["run_id"] == RUN_ID
    assert parsed["ids"] == [11, 12]
    assert parsed["frames"] == frames


def test_parse_envelope_reports_fetch_needed():
    payload, inline = build_envelope([_eth_frame()] * 5)
    assert not inline
    parsed = parse_envelope(_merge_ids(payload, [1, 2, 3, 4, 5]))
    assert parsed["frames"] is None
    assert parsed["ids"] == [1, 2, 3, 4, 5]


def test_parse_envelope_rejects_unknown_version():
    payload = json.dumps({"v": 99, "rid": RUN_ID, "sf": SOURCE_FILE, "ids": []})
    with pytest.raises(ValueError, match="unsupported bus envelope version"):
        parse_envelope(payload)


def test_parse_envelope_rejects_id_frame_count_mismatch():
    payload, _ = build_envelope([_frame(), _frame()])
    with pytest.raises(ValueError, match="2 frames but 1 ids"):
        parse_envelope(_merge_ids(payload, [7]))
