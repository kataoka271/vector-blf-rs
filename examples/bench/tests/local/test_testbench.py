"""Whole-bench runs over loopback buses: a Generator -> Gateway -> Receiver topology
end to end, plus the orchestration TestBench itself owns (one shared run epoch, a thread
per Ecu, stopping a run early).

This is the executable version of the "unify replay_bench + vecu_sdk into one TestBench"
claim -- nothing here talks to Databricks, Lakebase, or Zerobus. `_fake_frames()` mirrors
examples/testing/replay_bench/run_testbench.py's own dry-run fixture rows.
"""

from __future__ import annotations

import threading
import time

import pandas as pd
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import Ecu, GatewayEcu, ReceiverEcu
from bench.replay import GeneratorEcu

RUN_ID = "run_001"


def _fake_frames() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp_ns": 0,
                "timestamp_s": 0.0,
                "channel": 1,
                "can_id": 0x310,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 8,
                "data": b"\x01\x02\x03\x04\x05\x06\x07\x08",
                "dir": 1,
                "message_type": "CAN",
            },
            {
                "timestamp_ns": 10_000_000,
                "timestamp_s": 0.01,
                "channel": 1,
                "can_id": 0x311,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 4,
                "data": b"\xaa\xbb\xcc\xdd",
                "dir": 1,
                "message_type": "CAN",
            },
        ]
    )


def _standalone_ecu(name: str) -> Ecu:
    """An Ecu bound to this run but driven by the test rather than by the bench, for
    observing or injecting traffic from outside the topology under test.
    """
    ecu = Ecu(name)
    ecu._bind(run_id=RUN_ID, run_epoch_ns=time.time_ns())
    return ecu


def test_generator_gateway_receiver_over_loopback():
    bench = TestBench(run_id=RUN_ID)
    bus1 = bench.add_bus(Loopback())
    bus2 = bench.add_bus(Loopback())

    generator = GeneratorEcu(ch=bus1, fetch_fn=_fake_frames)
    gateway = GatewayEcu(ch1=bus1, ch2=bus2)
    receiver = ReceiverEcu(ch=bus2)
    bench.add_ecu(generator)
    bench.add_ecu(gateway)
    bench.add_ecu(receiver)

    bench.run(duration=0.3)

    assert [f.can_id for f in receiver.captured] == [0x310, 0x311]
    assert [f.data for f in receiver.captured] == [
        b"\x01\x02\x03\x04\x05\x06\x07\x08",
        b"\xaa\xbb\xcc\xdd",
    ]
    # The gateway must have re-stamped every frame onto bus2's channel, not bus1's.
    assert all(f.channel == bus2.channel for f in receiver.captured)
    assert all(f.run_id == RUN_ID for f in receiver.captured)


def test_a_bidirectional_gateway_forwards_the_return_path_exactly_once():
    bench = TestBench(run_id=RUN_ID)
    bus1 = bench.add_bus(Loopback())
    bus2 = bench.add_bus(Loopback())

    bench.add_ecu(GatewayEcu(ch1=bus1, ch2=bus2, bidirectional=True))
    upstream = ReceiverEcu(ch=bus1, name="upstream")
    bench.add_ecu(upstream)

    # An external responder, not part of the bench: its reply goes onto bus2 before the
    # run starts and reaches the gateway through the segment's backlog replay.
    responder = _standalone_ecu("responder")
    responder.handle(bus2).send_can(0x400, b"\xff")

    bench.run(duration=0.3)
    responder.close()

    assert [f.can_id for f in upstream.captured] == [0x400]
    assert upstream.captured[0].channel == bus1.channel


def test_a_receiver_uploads_every_captured_frame_to_its_second_bus():
    # ReceiverEcu's upload leg is just another Bus (a Zerobus one in the reference
    # topology), which is what retires the separate Lakebase-to-Zerobus bridge process.
    # A loopback bus stands in for it here.
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    upload = bench.add_bus(Loopback())

    receiver = ReceiverEcu(ch=bus, zerobus=upload)
    bench.add_ecu(receiver)
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=_fake_frames))

    watcher = _standalone_ecu("watcher")
    uploaded = []
    watcher.handle(upload).on()(uploaded.append)
    watcher.handle(upload).subscribe()

    bench.run(duration=0.3)
    watcher.handle(upload).drain(timeout=0.2)
    watcher.close()

    assert [f.can_id for f in uploaded] == [0x310, 0x311]
    # Uploaded as captured: the upload leg re-sends the frame rather than re-stamping it.
    assert [f.channel for f in uploaded] == [bus.channel, bus.channel]


def test_every_ecu_is_bound_to_one_shared_run_epoch():
    # A per-Ecu epoch would make each Ecu's timestamp_ns relative to a different t=0, and
    # the merged log's timeline would be meaningless.
    bench = TestBench(run_id=RUN_ID)
    bench.add_bus(Loopback())
    ecus = [bench.add_ecu(Ecu(f"ecu{i}")) for i in range(3)]

    bench.start(duration=0.05)
    bench._join()

    epochs = {ecu.clock.run_epoch_ns for ecu in ecus if ecu.clock is not None}
    assert len(epochs) == 1
    assert all(ecu.run_id == RUN_ID for ecu in ecus)
    assert all(ecu.source_file == f"testbench/{RUN_ID}.blf" for ecu in ecus)


def test_stop_ends_a_run_that_has_no_duration():
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    receiver = ReceiverEcu(ch=bus)
    bench.add_ecu(receiver)

    bench.start(duration=None)  # would otherwise run forever
    bench.stop(timeout=5.0)

    assert not any(thread.is_alive() for thread in bench._threads)


def test_ecus_run_concurrently_rather_than_one_after_another():
    # Every Ecu blocks in its own poll, so a bench that ran them in sequence would stall
    # on the first one forever. Two handlers meeting at a barrier can only both arrive if
    # they are on different threads.
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    rendezvous = threading.Barrier(2, timeout=3.0)
    met: list[str] = []

    for i in range(2):
        ecu = bench.add_ecu(Ecu(f"ecu{i}"))

        @ecu.handle(bus).on()
        def _meet(_frame, name=ecu.name):
            try:
                rendezvous.wait()
            except threading.BrokenBarrierError:
                return  # the other Ecu never arrived: the run was serialized
            met.append(name)

    _standalone_ecu("trigger").handle(bus).send_can(0x310, b"\x00")
    bench.run(duration=0.5)

    assert sorted(met) == ["ecu0", "ecu1"]
    assert {thread.name for thread in bench._threads} == {"ecu0", "ecu1"}
