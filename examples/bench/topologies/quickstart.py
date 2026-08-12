"""A fully offline topology: Generator -> Gateway -> Receiver over loopback buses, with
a hand-built fetch_fn standing in for the Databricks fetch (same fixture rows as
tests/test_testbench_gateway.py). No Databricks/Lakebase/Zerobus credentials needed --
this is the topology to reach for to sanity-check the wiring/forwarding/capture path, or
as a starting point for a new offline topology.

Run with:
    uv run --group testing python examples/bench/main.py --topology quickstart --duration 2
"""

from __future__ import annotations

import pandas as pd
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import GatewayEcu, ReceiverEcu
from bench.replay import GeneratorEcu
from bench.topology import register_topology


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
                "timestamp_ns": 200_000_000,
                "timestamp_s": 0.2,
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


def build(run_id: str | None = None) -> TestBench:
    bench = TestBench(run_id=run_id)

    bus1 = bench.add_bus(Loopback())
    bus2 = bench.add_bus(Loopback())

    bench.add_ecu(GeneratorEcu(ch=bus1, fetch_fn=_fake_frames))
    bench.add_ecu(GatewayEcu(ch1=bus1, ch2=bus2))
    bench.add_ecu(ReceiverEcu(ch=bus2))

    return bench


register_topology("quickstart", build)
