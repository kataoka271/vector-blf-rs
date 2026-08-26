"""Offline tests for the unified frame model (bench/frame.py): the two encodings
round-trip, the CAN/Ethernet variants stay disjoint, forwarding preserves the correlation
key, and `accepts()` -- the receive filter every transport shares -- answers the same way
for all of them.
"""

from __future__ import annotations

import json

import pytest
from bench import frame as F

RUN_ID = "run_001"
SOURCE_FILE = F.source_file_for(RUN_ID)
EPOCH_NS = 1_700_000_000_000_000_000


def _can() -> F.Frame:
    return F.make_can_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=0x310,
        data=b"\x25\x2b\x00\x00\x00\x00\x00\x00",
        observed_ns=EPOCH_NS + 1_000_000_000,
    )


def _eth(
    *,
    src_addr: bytes = bytes.fromhex("001122334455"),
    dst_addr: bytes = bytes.fromhex("66778899aabb"),
    vlan_id: int | None = None,
) -> F.Frame:
    return F.make_eth_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=2,
        src_addr=src_addr,
        dst_addr=dst_addr,
        ether_type=0x0800,
        data=bytes(range(64)),
        observed_ns=EPOCH_NS + 2_000_000_000,
        vlan_id=vlan_id,
    )


def test_make_can_frame_derives_relative_timestamp_and_dlc():
    f = _can()
    assert f.message_type == F.CAN
    assert f.timestamp_ns == 1_000_000_000
    assert f.dlc == 8
    assert f.is_fd is False
    assert f.is_can and not f.is_eth


def test_make_can_frame_fd_selects_can_fd_type():
    f = F.make_can_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=0x310,
        data=bytes(24),
        is_fd=True,
    )
    assert f.message_type == F.CAN_FD
    assert f.dlc == 24


def test_timestamp_clamps_at_zero_for_pre_epoch_observation():
    f = F.make_can_frame(
        run_id=RUN_ID,
        source_file=SOURCE_FILE,
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=1,
        data=b"",
        observed_ns=EPOCH_NS - 5,
    )
    assert f.timestamp_ns == 0


def test_make_eth_frame_untagged_is_eth_and_tagged_is_eth_ex():
    assert _eth().message_type == F.ETH
    assert _eth(vlan_id=42).message_type == F.ETH_EX


def test_make_eth_frame_rejects_bad_mac_length():
    with pytest.raises(ValueError, match="src_addr"):
        _eth(src_addr=b"\x00\x11\x22")


def test_frame_rejects_unknown_message_type():
    with pytest.raises(ValueError, match="message_type"):
        F.Frame(
            run_id=RUN_ID,
            source_file=SOURCE_FILE,
            message_type="SOMEIP",
            timestamp_ns=0,
            observed_ns=0,
            channel=1,
        )


@pytest.mark.parametrize("build", [_can, _eth], ids=["can", "eth"])
def test_row_round_trip(build):
    f = build()
    assert F.Frame.from_row(f.to_row()) == f


@pytest.mark.parametrize("build", [_can, _eth], ids=["can", "eth"])
def test_json_round_trip(build):
    f = build()
    obj = json.loads(json.dumps(f.to_json_obj()))
    assert F.Frame.from_json_obj(obj, run_id=RUN_ID, source_file=SOURCE_FILE) == f


def test_row_covers_every_frame_column():
    # The row dict is used directly as both the Postgres parameter set and the protobuf
    # constructor kwargs, so a missing key would silently drop a column.
    assert set(_can().to_row()) == set(F.FRAME_COLUMNS)


def test_from_row_normalizes_memoryview_binaries():
    row = _eth().to_row()
    row["data"] = memoryview(row["data"])
    row["src_addr"] = memoryview(row["src_addr"])
    rebuilt = F.Frame.from_row(row)
    assert rebuilt == _eth()
    assert isinstance(rebuilt.data, bytes)


def test_from_row_ignores_extra_columns():
    row = _can().to_row()
    row["id"] = 12345  # the bus table's BIGSERIAL primary key
    assert F.Frame.from_row(row) == _can()


def test_json_omits_unset_variant_fields_and_hoisted_identity():
    obj = _can().to_json_obj()
    for absent in ("src", "dst", "et", "tpid", "cos", "vid", "run_id", "source_file"):
        assert absent not in obj


def test_json_from_obj_ignores_unknown_keys():
    obj = _can().to_json_obj()
    obj["zz"] = "from a newer producer"
    assert F.Frame.from_json_obj(obj, run_id=RUN_ID, source_file=SOURCE_FILE) == _can()


def test_forwarded_preserves_correlation_key_and_restamps_hop():
    original = _can()
    fwd = original.forwarded(channel=2, observed_ns=original.observed_ns + 50_000_000)
    assert (fwd.run_id, fwd.can_id, fwd.timestamp_ns) == (original.run_id, original.can_id, original.timestamp_ns)
    assert fwd.data == original.data
    assert fwd.channel == 2
    assert fwd.observed_ns - original.observed_ns == 50_000_000


def test_forwarded_defaults_to_now():
    original = _can()
    fwd = original.forwarded(channel=2)
    assert fwd.observed_ns > original.observed_ns


def test_source_file_for_matches_pipeline_convention():
    assert F.source_file_for("abc") == "testbench/abc.blf"


def test_accepts_treats_an_unset_filter_as_accepting_everything():
    assert F.accepts(_can(), can_ids=None, message_types=None) is True
    assert F.accepts(_eth(), can_ids=None, message_types=None) is True


def test_accepts_matches_can_id_and_message_type():
    frame = _can()
    assert F.accepts(frame, can_ids={0x310}, message_types=None) is True
    assert F.accepts(frame, can_ids={0x999}, message_types=None) is False
    assert F.accepts(frame, can_ids=None, message_types={F.CAN}) is True
    assert F.accepts(frame, can_ids=None, message_types={F.ETH}) is False


def test_accepts_never_matches_a_non_can_frame_against_a_can_id_filter():
    # An Ethernet frame's can_id is None, so a receiver filtered to a set of ids must not
    # be handed one -- a filter meant to narrow traffic would otherwise widen it.
    assert F.accepts(_eth(), can_ids={0x310}, message_types=None) is False


def test_accepts_ands_the_two_filters():
    assert F.accepts(_can(), can_ids={0x310}, message_types={F.ETH}) is False
