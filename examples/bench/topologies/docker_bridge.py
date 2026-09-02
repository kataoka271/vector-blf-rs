"""Two-container demo: a producer container and a consumer container that only ever
talk to each other through a real Lakebase table and a real Zerobus stream -- no
loopback bus, no shared process.

    [producer container]                          [consumer container]
    GeneratorEcu --(bus, Lakebase table)-------->  ReceiverEcu --(Zerobus)--> Delta table

Both containers need LAKEBASE_*/ZEROBUS_* env vars and must agree on DOCKER_BUS_TABLE
(default "bench_docker_bus") and `--run-id`; see docker-compose.yml, which passes both
through identically. Each run needs a fresh RUN_ID -- a repeated one replays that id's
entire history on every fresh consumer start.

Run with (from the repo root):
    RUN_ID=$(python -c "import uuid; print(uuid.uuid4())") \\
        docker compose -f examples/bench/docker-compose.yml up --build
"""

from __future__ import annotations

import os

import pandas as pd
from bench.bench import TestBench
from bench.bus import Bus, Lakebase, Zerobus
from bench.ecu import ReceiverEcu
from bench.replay import GeneratorEcu
from bench.topology import register_topology, require_config
from transport.connection import ConnectionConfig

# Any bare identifier is a valid table name, so a producer/consumer pair that disagrees
# on this doesn't error -- it silently talks past each other on two distinct tables.
BUS_TABLE = os.environ.get("DOCKER_BUS_TABLE", "bench_docker_bus")
ZEROBUS_TABLE = os.environ.get("ZEROBUS_TABLE", "blf_testbench_frames")


def _demo_frames(count: int = 10, gap_ns: int = 500_000_000) -> pd.DataFrame:
    """`count` synthetic CAN frames, `gap_ns` apart, standing in for a real replay."""
    rows = []
    for i in range(count):
        t_ns = i * gap_ns
        rows.append(
            {
                "timestamp_ns": t_ns,
                "timestamp_s": t_ns / 1e9,
                "channel": 1,
                "can_id": 0x100 + i,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 8,
                "data": bytes([i] * 8),
                "dir": 1,
                "message_type": "CAN",
            }
        )
    return pd.DataFrame(rows)


def build_producer(config: ConnectionConfig | None = None, run_id: str | None = None, **buses: Bus) -> TestBench:
    """The producer container: one GeneratorEcu writing synthetic frames onto the
    shared Lakebase bus. Needs LAKEBASE_* credentials; no Zerobus config needed.
    """
    bench = TestBench(run_id=run_id, buses=buses)
    bus = bench.bus(
        "bus",
        lambda: Lakebase(require_config(config, "docker-producer").lakebase_config(table=BUS_TABLE), name=BUS_TABLE),
    )
    # _demo_frames rather than a real Databricks fetch, so this demo needs only
    # Lakebase/Zerobus credentials, not a populated blf_gold_signals/blf_silver_can.
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=_demo_frames))
    return bench


def build_consumer(config: ConnectionConfig | None = None, run_id: str | None = None, **buses: Bus) -> TestBench:
    """The consumer container: one ReceiverEcu reading the shared Lakebase bus and
    forwarding everything to Zerobus. Needs both LAKEBASE_* and ZEROBUS_*
    credentials.
    """
    bench = TestBench(run_id=run_id, buses=buses)
    bus = bench.bus(
        "bus",
        lambda: Lakebase(require_config(config, "docker-consumer").lakebase_config(table=BUS_TABLE), name=BUS_TABLE),
    )
    zerobus = bench.bus(
        "zerobus",
        lambda: Zerobus(require_config(config, "docker-consumer").zerobus_config(table=ZEROBUS_TABLE)),
    )
    bench.add_ecu(ReceiverEcu(ch=bus, zerobus=zerobus))
    return bench


register_topology("docker-producer", build_producer)
register_topology("docker-consumer", build_consumer)
