"""End-to-end smoke test: Generator -> Gateway -> Receiver over loopback buses, with a
hand-built fetch_fn standing in for the Databricks fetch (mirrors
examples/testing/replay_bench/run_testbench.py's own dry-run fixture rows).

This is the executable version of the "unify replay_bench + vecu_sdk into one
TestBench" claim -- nothing here talks to Databricks, Lakebase, or Zerobus.
"""

from __future__ import annotations

import pandas as pd
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import GatewayEcu, ReceiverEcu
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
