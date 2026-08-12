"""Tests for CanDeviceTx/CanDeviceRx's Frame<->can.Message conversion, using
python-can's in-process "virtual" interface (not udp_multicast) so this stays
deterministic and network-free. Two virtual buses constructed with the same `channel`
string share state within this process -- that is what connects tx and rx here.
"""

from __future__ import annotations

import pytest
from bench.channel import CanDeviceRx, CanDeviceTx
from bench.frame import make_can_frame, make_eth_frame
from transport.device import CanDeviceConfig

RUN_ID = "run_001"
SOURCE_FILE = "testbench/run_001.blf"
EPOCH_NS = 1_700_000_000_000_000_000


def _virtual_config(channel: str, *, fd: bool | None = None) -> CanDeviceConfig:
    return CanDeviceConfig(interface="virtual", channel=channel, port=None, fd=fd)


def test_can_device_round_trips_a_classic_frame():
    config = _virtual_config("test-can-classic")
    tx = CanDeviceTx(config=config)
    rx = CanDeviceRx(config=config, run_id=RUN_ID, source_file=SOURCE_FILE, channel=3, run_epoch_ns=EPOCH_NS)
    try:
        sent = make_can_frame(
            run_id=RUN_ID,
            source_file=SOURCE_FILE,
            run_epoch_ns=EPOCH_NS,
            channel=1,
            can_id=0x310,
            data=b"\x01\x02\x03\x04",
        )
        tx.send(sent)
        frames = rx.poll(timeout=1.0)
        assert len(frames) == 1
        got = frames[0]
        assert got.can_id == 0x310
        assert got.data == b"\x01\x02\x03\x04"
        assert got.dlc == 4
        assert got.is_fd is False
        assert got.message_type == "CAN"
        # CanDeviceRx stamps its own `channel` (the BLF channel number), not the
        # sender's Frame.channel above -- the two are different namespaces.
        assert got.channel == 3
    finally:
        tx.close()
        rx.close()


def test_can_device_round_trips_an_fd_frame_using_byte_length_as_dlc():
    config = _virtual_config("test-can-fd", fd=True)
    tx = CanDeviceTx(config=config)
    rx = CanDeviceRx(config=config, run_id=RUN_ID, source_file=SOURCE_FILE, channel=1, run_epoch_ns=EPOCH_NS)
    try:
        sent = make_can_frame(
            run_id=RUN_ID,
            source_file=SOURCE_FILE,
            run_epoch_ns=EPOCH_NS,
            channel=1,
            can_id=0x400,
            data=bytes(24),
            is_fd=True,
        )
        tx.send(sent)
        frames = rx.poll(timeout=1.0)
        assert len(frames) == 1
        got = frames[0]
        assert got.is_fd is True
        assert got.message_type == "CAN_FD"
        assert got.dlc == 24  # byte length, not a BLF DLC code
    finally:
        tx.close()
        rx.close()


def test_can_device_tx_rejects_a_non_can_frame():
    config = _virtual_config("test-can-reject")
    tx = CanDeviceTx(config=config)
    try:
        eth = make_eth_frame(
            run_id=RUN_ID,
            source_file=SOURCE_FILE,
            run_epoch_ns=EPOCH_NS,
            channel=1,
            src_addr=bytes(6),
            dst_addr=bytes(6),
            ether_type=0x0800,
            data=b"x",
        )
        with pytest.raises(ValueError, match="message_type"):
            tx.send(eth)
    finally:
        tx.close()
