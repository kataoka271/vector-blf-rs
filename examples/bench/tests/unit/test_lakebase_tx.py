"""Tests for LakebaseTx's self-clocking writer: the batching/flush/failure policy that
sits above _LakebaseBatchWriter's actual round trip. No Postgres connection is involved
-- a FakeWriter stands in for _LakebaseBatchWriter, since LakebaseTx accepts one via its
`writer` parameter.
"""

from __future__ import annotations

import threading

import pytest
from bench.frame import Frame, make_can_frame
from transport.lakebase import LakebaseTx

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000


def _frame():
    return make_can_frame(
        run_id=RUN_ID,
        source_file="testbench/run_001.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=0x310,
        data=b"\x01\x02\x03\x04\x05\x06\x07\x08",
    )


class FakeWriter:
    """A _LakebaseBatchWriter stand-in whose write_batch() blocks until released, so a
    test can observe exactly what accumulated while one round trip was in flight.
    """

    def __init__(self) -> None:
        self.table = "bus_a"
        self.batches: list[int] = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.gate = False
        self.fail = False

    def write_batch(self, frames: list[Frame]) -> None:
        if self.fail:
            raise RuntimeError("simulated write failure")
        if self.gate:
            self.batches.append(len(frames))
            self.entered.set()
            self.release.wait(5.0)
        else:
            self.batches.append(len(frames))

    def close(self) -> None:
        pass


def test_writer_coalesces_frames_offered_during_an_in_flight_round_trip():
    writer = FakeWriter()
    writer.gate = True
    tx = LakebaseTx(writer=writer)
    try:
        tx.send(_frame())
        assert writer.entered.wait(5.0), "writer never started the first round trip"
        # These arrive while the first write is blocked; they must go out together as
        # the next batch, not one round trip each.
        for _ in range(5):
            tx.send(_frame())
        writer.release.set()
        tx.flush()
    finally:
        writer.release.set()
        tx.close()
    assert writer.batches == [1, 5]


def test_flush_returns_once_everything_is_written():
    writer = FakeWriter()
    tx = LakebaseTx(writer=writer)
    try:
        for _ in range(3):
            tx.send(_frame())
        tx.flush()
        assert sum(writer.batches) == 3
    finally:
        tx.close()


def test_write_failure_surfaces_from_send_and_close():
    writer = FakeWriter()
    writer.fail = True
    tx = LakebaseTx(writer=writer)
    tx.send(_frame())
    tx._thread.join(timeout=5.0)
    # The failure must not be swallowed: a silently dropped frame would show up much
    # later as an unexplained gap in the Delta log.
    with pytest.raises(RuntimeError, match="transmit failed"):
        tx.send(_frame())
    with pytest.raises(RuntimeError, match="transmit failed"):
        tx.close()


def test_write_failure_surfaces_from_a_waiting_flush():
    writer = FakeWriter()
    writer.gate = True
    tx = LakebaseTx(writer=writer)
    try:
        tx.send(_frame())
        assert writer.entered.wait(5.0), "writer never started the first round trip"
        # flush() blocks on the in-flight write; the failure has to wake it rather than
        # leave the caller parked until its own timeout.
        writer.fail = True
        tx.send(_frame())
        writer.release.set()
        with pytest.raises(RuntimeError, match="transmit failed"):
            tx.flush()
    finally:
        writer.release.set()
        with pytest.raises(RuntimeError, match="transmit failed"):
            tx.close()


def test_send_after_close_is_rejected():
    writer = FakeWriter()
    tx = LakebaseTx(writer=writer)
    tx.close()
    with pytest.raises(RuntimeError, match="is closed"):
        tx.send(_frame())


def test_constructing_without_a_config_or_a_writer_is_rejected():
    # Both default to None so a test can inject a writer; with neither there is nothing
    # to connect to, and failing here beats a None dereference on the writer thread.
    with pytest.raises(ValueError, match="either config or an explicit writer"):
        LakebaseTx()
