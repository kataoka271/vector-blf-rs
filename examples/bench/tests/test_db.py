"""Unit tests for bench/db.py's FilterSpec.build_where(), wait_for_ingestion, and
to_frames -- no Databricks connection required (count_fn/fetch_fn are fakes, not real
SQL queries).
"""

from __future__ import annotations

import pandas as pd
import pytest
from bench.db import FilterSpec, ReplayConfig, to_frames, wait_for_ingestion


def test_build_where_defaults():
    sql, params = FilterSpec().build_where()
    assert sql == "_source_file NOT LIKE ? AND signal_source IN (?) AND dir NOT IN (?)"
    assert params == ["testbench/%", "CAN", "Tx"]


def test_build_where_all_fields():
    spec = FilterSpec(
        source_files=["a.blf", "b.blf"],
        t_start_s=1.0,
        t_end_s=2.0,
        channels=[1, 2],
        signal_source=["CAN"],
        exclude_dirs=["Tx"],
        exclude_source_prefix="testbench/",
        extra_where="message_id_str = '0x100'",
    )
    sql, params = spec.build_where()
    assert sql == (
        "_source_file IN (?, ?) AND _source_file NOT LIKE ?"
        " AND timestamp_s >= ? AND timestamp_s <= ?"
        " AND channel IN (?, ?) AND signal_source IN (?) AND dir NOT IN (?)"
        " AND (message_id_str = '0x100')"
    )
    assert params == ["a.blf", "b.blf", "testbench/%", 1.0, 2.0, 1, 2, "CAN", "Tx"]


def test_build_where_empty_spec_is_permissive():
    spec = FilterSpec(signal_source=[], exclude_dirs=[], exclude_source_prefix="")
    sql, params = spec.build_where()
    assert sql == "1 = 1"
    assert params == []


def test_wait_for_ingestion_returns_immediately_when_already_satisfied():
    counts = iter([3])
    got = wait_for_ingestion(
        ReplayConfig(),
        "testbench/r1.blf",
        expected_count=3,
        count_fn=lambda cfg, source_file, dbx_cfg: next(counts),
    )
    assert got == 3


def test_wait_for_ingestion_polls_until_expected_count_is_reached():
    counts = iter([0, 1, 3])
    calls = []

    def count_fn(cfg, source_file, dbx_cfg):
        calls.append(source_file)
        return next(counts)

    got = wait_for_ingestion(ReplayConfig(), "testbench/r1.blf", expected_count=3, poll_s=0, count_fn=count_fn)
    assert got == 3
    assert calls == ["testbench/r1.blf"] * 3


def test_wait_for_ingestion_expected_count_zero_short_circuits():
    def count_fn(cfg, source_file, dbx_cfg):
        raise AssertionError("count_fn must not be called when expected_count <= 0")

    assert wait_for_ingestion(ReplayConfig(), "testbench/r1.blf", expected_count=0, count_fn=count_fn) == 0


def test_wait_for_ingestion_raises_timeout_error_when_count_never_catches_up():
    with pytest.raises(TimeoutError, match=r"1/5"):
        wait_for_ingestion(
            ReplayConfig(),
            "testbench/r1.blf",
            expected_count=5,
            timeout_s=0,
            poll_s=0,
            count_fn=lambda cfg, source_file, dbx_cfg: 1,
        )


def test_to_frames_relative_timestamps():
    df = pd.DataFrame(
        [
            {
                "timestamp_ns": 10_000_000_000,
                "timestamp_s": 10.0,
                "channel": 1,
                "can_id": 0x100,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 2,
                "data": bytes([1, 2]),
                "dir": 1,
                "message_type": "CAN",
            },
            {
                "timestamp_ns": 10_250_000_000,
                "timestamp_s": 10.25,
                "channel": 1,
                "can_id": 0x200,
                "is_ext_id": True,
                "rtr": False,
                "dlc": 1,
                "data": bytes([9]),
                "dir": 1,
                "message_type": "CAN",
            },
        ]
    )
    frames = to_frames(df, run_id="r1", source_file="testbench/r1.blf", channel=3)
    assert [f.timestamp_ns for f in frames] == [0, 250_000_000]
    assert [f.can_id for f in frames] == [0x100, 0x200]
    assert frames[1].is_ext_id is True
    assert frames[0].data == bytes([1, 2])
    assert frames[0].is_fd is False
    assert all(f.channel == 3 for f in frames)
    assert all(f.run_id == "r1" for f in frames)


def test_to_frames_empty():
    assert to_frames(pd.DataFrame({"timestamp_s": []}), run_id="r1", source_file="s", channel=1) == []


def test_to_frames_fd_with_small_payload_uses_message_type_not_dlc():
    """A CAN-FD frame with a <= 8-byte payload has the same raw dlc code as classic CAN,
    so is_fd must come from message_type, not `dlc > 8`.
    """
    df = pd.DataFrame(
        [
            {
                "timestamp_ns": 0,
                "timestamp_s": 0.0,
                "channel": 1,
                "can_id": 0x300,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 4,
                "data": bytes([1, 2, 3, 4]),
                "dir": 1,
                "message_type": "CAN_FD",
            },
        ]
    )
    frames = to_frames(df, run_id="r1", source_file="s", channel=1)
    assert frames[0].is_fd is True
    assert frames[0].message_type == "CAN_FD"
    # For FD frames the byte length is used, not the BLF DLC code.
    assert frames[0].dlc == 4
