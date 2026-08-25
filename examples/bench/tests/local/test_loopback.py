"""Tests for the loopback transport itself (transport/loopback.py).

Loopback is the transport every other local test runs on, so its own guarantees are worth
pinning down separately: delivery is synchronous inside send(), a receiver that attaches
late still gets the backlog (the in-process stand-in for the Lakebase channel's catch-up
on connect), and a segment is shared by bus *name* while runs stay isolated by run_id.
"""

from __future__ import annotations

import pytest
from bench.frame import CAN, ETH, make_can_frame, make_eth_frame
from transport import loopback
from transport.loopback import LoopbackRx, LoopbackTx

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000


def _can(can_id: int = 0x310, *, run_id: str = RUN_ID):
    return make_can_frame(
        run_id=run_id,
        source_file=f"testbench/{run_id}.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=can_id,
        data=b"\x01\x02",
    )


def _eth():
    return make_eth_frame(
        run_id=RUN_ID,
        source_file=f"testbench/{RUN_ID}.blf",
        run_epoch_ns=EPOCH_NS,
        channel=2,
        src_addr=bytes(6),
        dst_addr=bytes(6),
        ether_type=0x0800,
        data=b"x",
    )


def test_send_delivers_before_it_returns():
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        tx.send(_can())
        # timeout=0 would still pass if delivery were merely fast; it passes here because
        # send() fanned the frame out inline.
        assert [f.can_id for f in rx.poll(timeout=0)] == [0x310]
    finally:
        tx.close()
        rx.close()


def test_a_receiver_attaching_after_a_send_still_gets_the_frame():
    # The in-process equivalent of the Lakebase channel's catch-up query: an Ecu that
    # starts polling late must not behave differently depending on its transport.
    tx = LoopbackTx(table="a")
    try:
        tx.send(_can())
        rx = LoopbackRx(table="a", run_id=RUN_ID)
        assert len(rx.poll(timeout=0)) == 1
        rx.close()
    finally:
        tx.close()


def test_the_replayed_backlog_is_bounded(monkeypatch):
    monkeypatch.setattr(loopback, "BACKLOG_LIMIT", 3)
    tx = LoopbackTx(table="a")
    try:
        for i in range(10):
            tx.send(_can(0x300 + i))
        rx = LoopbackRx(table="a", run_id=RUN_ID)
        assert [f.can_id for f in rx.poll(timeout=0)] == [0x307, 0x308, 0x309]
        rx.close()
    finally:
        tx.close()


def test_an_already_attached_receiver_is_not_bounded_by_the_backlog(monkeypatch):
    # The limit caps what a late subscriber replays, not what a live one receives.
    monkeypatch.setattr(loopback, "BACKLOG_LIMIT", 3)
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        for i in range(10):
            tx.send(_can(0x300 + i))
        assert len(rx.poll(timeout=0)) == 10
    finally:
        tx.close()
        rx.close()


def test_segments_are_shared_by_bus_name():
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    other = LoopbackRx(table="b", run_id=RUN_ID)
    try:
        tx.send(_can())
        assert len(rx.poll(timeout=0)) == 1
        assert other.poll(timeout=0) == []
    finally:
        tx.close()
        rx.close()
        other.close()


def test_another_runs_frames_are_not_delivered():
    # Two runs can share one segment (a re-run against the same bus name, or a Lakebase
    # table); run_id is what keeps their traffic apart.
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        tx.send(_can(run_id="run_002"))
        tx.send(_can())
        assert [f.run_id for f in rx.poll(timeout=0)] == [RUN_ID]
    finally:
        tx.close()
        rx.close()


def test_receiver_filters_narrow_what_is_queued():
    tx = LoopbackTx(table="a")
    by_id = LoopbackRx(table="a", run_id=RUN_ID, can_ids=[0x310])
    by_type = LoopbackRx(table="a", run_id=RUN_ID, message_types=[ETH])
    try:
        tx.send(_can(0x310))
        tx.send(_can(0x311))
        tx.send(_eth())
        assert [f.can_id for f in by_id.poll(timeout=0)] == [0x310]
        assert [f.message_type for f in by_type.poll(timeout=0)] == [ETH]
    finally:
        tx.close()
        by_id.close()
        by_type.close()


def test_poll_drains_everything_queued_in_one_call():
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        for i in range(5):
            tx.send(_can(0x300 + i))
        assert len(rx.poll(timeout=0)) == 5
        assert rx.poll(timeout=0) == []
    finally:
        tx.close()
        rx.close()


def test_poll_returns_empty_on_timeout_rather_than_raising():
    rx = LoopbackRx(table="a", run_id=RUN_ID)
    try:
        assert rx.poll(timeout=0.01) == []
    finally:
        rx.close()


def test_a_closed_receiver_stops_being_fed():
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        rx.close()
        rx.close()  # close is idempotent
        tx.send(_can())
        assert rx.poll(timeout=0) == []
    finally:
        tx.close()


def test_sending_on_a_closed_transmitter_is_rejected():
    tx = LoopbackTx(table="a")
    tx.close()
    with pytest.raises(RuntimeError, match="closed"):
        tx.send(_can())


def test_reset_drops_every_segment():
    tx = LoopbackTx(table="a")
    tx.send(_can())
    loopback.reset()
    rx = LoopbackRx(table="a", run_id=RUN_ID)
    try:
        # A fresh segment, so nothing to replay -- this is what keeps one test's frames
        # out of the next one (see tests/conftest.py).
        assert rx.poll(timeout=0) == []
    finally:
        rx.close()
        tx.close()


def test_ethernet_frames_survive_the_segment_unchanged():
    tx, rx = LoopbackTx(table="a"), LoopbackRx(table="a", run_id=RUN_ID)
    try:
        sent = _eth()
        tx.send(sent)
        (got,) = rx.poll(timeout=0)
        assert got == sent
        assert got.message_type == ETH and sent.message_type != CAN
    finally:
        tx.close()
        rx.close()
