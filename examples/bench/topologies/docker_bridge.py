"""Two-container demo: a producer container and a consumer container that only ever
talk to each other through a real Lakebase table and a real Zerobus stream -- no
loopback bus, no shared process. This is what proves the Lakebase/Zerobus transports
(examples/bench/transport/lakebase.py, .../zerobus.py) actually work across a process
boundary, not just within one in-process TestBench like the `reference`/`quickstart`
topologies do.

    [producer container]                          [consumer container]
    GeneratorEcu --(bus, Lakebase table)-------->  ReceiverEcu --(Zerobus)--> Delta table

`build_producer()` makes up its own frames (`_demo_frames`, below) instead of fetching
from Databricks -- this demo only needs Lakebase + Zerobus credentials, not a populated
blf_gold_signals/blf_silver_can to replay from. Pass a `fetch_fn`/`config` of your own
(or reuse `bench.replay.GeneratorEcu`'s default Databricks fetch) for a real replay.

Both containers must agree on:
- the shared bus's Lakebase table name (`DOCKER_BUS_TABLE` env var, default
  "bench_docker_bus") -- any bare identifier is a valid table name, so a producer/
  consumer pair that disagrees doesn't error, it just silently talks past each other on
  two distinct tables.
- `--run-id` -- `LakebaseRx` only accepts frames whose envelope carries its own run_id.

`LAKEBASE_DATABASE`/`ZEROBUS_CATALOG`/`ZEROBUS_SCHEMA` are required fields on
ConnectionConfig with no library-level default (see transport/connection.py) --
ConnectionConfig.from_environ() raises KeyError if any is unset, rather than silently
landing on some default database/catalog/schema. Set them explicitly; the Zerobus table
itself is a demo-level default here (`ZEROBUS_TABLE`, "blf_testbench_frames") rather than
a ConnectionConfig field, since it's named per-bus via `zerobus_config(table=...)`.

See examples/bench/docker-compose.yml, which passes both through to each service
identically and requires the caller to set `RUN_ID` explicitly (a repeated run_id
replays that id's entire history on every fresh consumer start -- see
transport/lakebase.py's catch-up query -- so each demo run needs a fresh one).

Run with (from the repo root):
    RUN_ID=$(python -c "import uuid; print(uuid.uuid4())") \\
        docker compose -f examples/bench/docker-compose.yml up --build
"""

from __future__ import annotations

import os

import pandas as pd
from bench.bench import TestBench
from bench.bus import Lakebase, Zerobus
from bench.ecu import ReceiverEcu
from bench.replay import GeneratorEcu
from bench.topology import register_topology
from transport.connection import ConnectionConfig

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


def build_producer(config: ConnectionConfig, run_id: str | None = None) -> TestBench:
    """The producer container: one GeneratorEcu writing synthetic frames onto the
    shared Lakebase bus. Needs LAKEBASE_* credentials; no Zerobus config needed.
    """
    bench = TestBench(run_id=run_id)
    bus = bench.add_bus(Lakebase(config.lakebase_config(table=BUS_TABLE), name=BUS_TABLE))
    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=_demo_frames))
    return bench


def build_consumer(config: ConnectionConfig, run_id: str | None = None) -> TestBench:
    """The consumer container: one ReceiverEcu reading the shared Lakebase bus and
    forwarding everything to Zerobus. Needs both LAKEBASE_* and ZEROBUS_*
    credentials.
    """
    bench = TestBench(run_id=run_id)
    bus = bench.add_bus(Lakebase(config.lakebase_config(table=BUS_TABLE), name=BUS_TABLE))
    zerobus = bench.add_bus(Zerobus(config.zerobus_config(table=ZEROBUS_TABLE)))
    bench.add_ecu(ReceiverEcu(ch=bus, zerobus=zerobus))
    return bench


register_topology("docker-producer", build_producer)
register_topology("docker-consumer", build_consumer)
