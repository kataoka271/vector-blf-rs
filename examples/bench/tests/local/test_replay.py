"""Tests for the fetch-pace-send engine (bench/replay.py), driven with a hand-built
DataFrame through `fetch_fn` instead of a live warehouse connection.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import Ecu, ReceiverEcu
from bench.replay import GeneratorEcu, ReplayEcu

RUN_ID = "run_001"


def _frames(*offsets_s: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp_ns": int(offset * 1e9),
                "timestamp_s": offset,
                "channel": 1,
                "can_id": 0x310 + i,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 2,
                "data": bytes([i, i]),
                "dir": 1,
                "message_type": "CAN",
            }
            for i, offset in enumerate(offsets_s)
        ]
    )


def test_recorded_gaps_are_reproduced_as_send_pacing():
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    receiver = ReceiverEcu(ch=bus)
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=lambda: _frames(0.0, 0.2)))
    bench.add_ecu(receiver)

    started = time.monotonic()
    bench.run(duration=2.0)
    elapsed = time.monotonic() - started

    assert [f.can_id for f in receiver.captured] == [0x310, 0x311]
    # The 200 ms recorded gap has to be slept through, not collapsed.
    assert elapsed >= 0.2


def test_each_frame_is_restamped_with_the_instant_it_actually_went_out():
    # to_frames() leaves observed_ns at 0 as a placeholder; sending it that way would
    # make every hop latency computed downstream meaningless.
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    receiver = ReceiverEcu(ch=bus)
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=lambda: _frames(0.0, 0.05)))
    bench.add_ecu(receiver)

    before = time.time_ns()
    bench.run(duration=2.0)

    assert all(f.observed_ns >= before for f in receiver.captured)
    # timestamp_ns stays the recorded relative position, which is what keeps a replayed
    # frame joinable to the log it came from.
    assert [f.timestamp_ns for f in receiver.captured] == [0, 50_000_000]


def test_an_empty_fetch_sends_nothing_and_still_finishes():
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    receiver = ReceiverEcu(ch=bus)
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=lambda: pd.DataFrame({"timestamp_s": []})))
    bench.add_ecu(receiver)

    bench.run(duration=0.3)

    assert receiver.captured == []


def test_generator_excludes_prior_testbench_uploads_and_replay_includes_them():
    # The one difference between the two roles: a generator must never replay its own (or
    # another run's) captured output, while ReplayEcu exists precisely to re-inject it.
    bus = Loopback()
    generator = GeneratorEcu(ch=bus)
    replay = ReplayEcu(ch=bus)
    assert generator._config.filter.exclude_source_prefix == "testbench/"
    assert replay._config.filter.exclude_source_prefix == ""


def test_run_rejects_on_tick_rather_than_never_firing_it():
    # Accepted for substitutability with Ecu.run(), rejected rather than ignored: pacing
    # here comes from the fetched rows, so a tick callback would silently never fire.
    generator = GeneratorEcu(ch=Loopback(), fetch_fn=lambda: _frames(0.0))
    with pytest.raises(TypeError, match="on_tick"):
        generator.run(duration=0.01, on_tick=lambda _ecu: None)


def test_run_requires_bind_before_running():
    generator = GeneratorEcu(ch=Loopback(), fetch_fn=lambda: _frames(0.0))
    with pytest.raises(RuntimeError, match="never bound"):
        generator.run(duration=0.01)


def test_replay_is_substitutable_for_a_plain_ecu_in_a_bench():
    # TestBench.start() calls ecu.run(duration=..., should_stop=...) uniformly, so this
    # override has to accept the same call an Ecu does.
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback())
    receiver = ReceiverEcu(ch=bus)
    bench.add_ecu(ReplayEcu(ch=bus, fetch_fn=lambda: _frames(0.0)))
    bench.add_ecu(receiver)
    bench.add_ecu(Ecu("bystander"))

    bench.run(duration=0.3)

    assert [f.can_id for f in receiver.captured] == [0x310]
