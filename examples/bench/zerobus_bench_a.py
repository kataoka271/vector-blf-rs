"""Bench A of a two-bench pair whose only link to each other is Zerobus. Standalone:
run it directly, no main.py and no topology registry.

    [bench A -- this script]                             [bench B -- zerobus_bench_b.py]
    GeneratorEcu --(tx bus, Zerobus)--> a2b table --------> ReceiverEcu
    ReceiverEcu  <--(rx bus, SQL poll)-- b2a table <-------- GeneratorEcu

Two buses per bench, one per direction. Both are `ZerobusDuplex` buses (see
transport/zerobus_duplex.py), which send over Zerobus Ingest and receive by polling the
table those writes land in -- so a bench sends on the table it owns and listens on the
one the other bench owns. Received frames arrive in batches seconds behind the send, not
one at a time.

One table per direction, not one shared table: the receive side filters on run_id, which
both benches share, so a single table would hand each bench its own generator's frames
straight back (`bench.handle.BusHandle` suppresses only the echo of what that same handle
sent).

Both benches must agree on `--run-id` and on the two table names, and both tables must
already exist with the transport/record.proto schema. Each bench needs a SQL warehouse
(DATABRICKS_WAREHOUSE_ID, or the active profile's) for its inbound leg, on top of the
ZEROBUS_* settings in transport/connection.py::ConnectionConfig. `--duration` has
to outlast the ingest latency or the receiver captures nothing.

Run with (from the repo root), alongside bench B and with the same RUN_ID:
    RUN_ID=$(python -c "import uuid; print(uuid.uuid4())")
    uv run --group testing python examples/bench/zerobus_bench_a.py --run-id "$RUN_ID" --duration 120
"""

from __future__ import annotations

import argparse
import os

import pandas as pd
from bench.bench import TestBench
from bench.bus import ZerobusDuplex
from bench.ecu import ReceiverEcu
from bench.replay import GeneratorEcu
from transport.connection import ConnectionConfig
from transport.zerobus_duplex import ZerobusDuplexConfig

# A -> B carries this bench's generator output; B -> A carries the other bench's.
TABLE_A2B = os.environ.get("ZEROBUS_TABLE_A2B", "blf_testbench_frames_a2b")
TABLE_B2A = os.environ.get("ZEROBUS_TABLE_B2A", "blf_testbench_frames_b2a")

# 0x2xx from bench A, 0x3xx from bench B, so a captured frame's can_id says which bench
# sent it without cross-referencing anything.
CAN_ID_BASE = 0x200
FRAME_COUNT = int(os.environ.get("ZEROBUS_PAIR_FRAME_COUNT", "20"))
FRAME_GAP_NS = int(os.environ.get("ZEROBUS_PAIR_FRAME_GAP_NS", str(500_000_000)))
POLL_INTERVAL_S = float(os.environ.get("ZEROBUS_PAIR_POLL_INTERVAL_S", "2.0"))


def demo_frames(count: int = FRAME_COUNT, gap_ns: int = FRAME_GAP_NS) -> pd.DataFrame:
    """`count` synthetic CAN frames, `gap_ns` apart, standing in for a real replay.

    Used instead of GeneratorEcu's default Databricks fetch so the pair demonstrates the
    Zerobus link without a populated blf_gold_signals/blf_silver_can.
    """
    rows = []
    for i in range(count):
        t_ns = i * gap_ns
        rows.append(
            {
                "timestamp_ns": t_ns,
                "timestamp_s": t_ns / 1e9,
                "channel": 1,
                "can_id": CAN_ID_BASE + i,
                "is_ext_id": False,
                "rtr": False,
                "dlc": 8,
                "data": bytes([i] * 8),
                "dir": 1,
                "message_type": "CAN",
            }
        )
    return pd.DataFrame(rows)


def _bus_config(config: ConnectionConfig, table: str) -> ZerobusDuplexConfig:
    """One direction's bus, derived from the shared connection settings."""
    return ZerobusDuplexConfig.from_zerobus(
        config.zerobus_config(table=table),
        poll_interval_s=POLL_INTERVAL_S,
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
    )


def build(config: ConnectionConfig, run_id: str) -> TestBench:
    """This bench: the Generator on the bus this bench owns, the Receiver on the bus the
    other bench owns. Swap the two table names to get bench B.
    """
    bench = TestBench(run_id=run_id)

    tx_bus = bench.add_bus(ZerobusDuplex(_bus_config(config, TABLE_A2B), name=f"{TABLE_A2B}_tx"))
    rx_bus = bench.add_bus(ZerobusDuplex(_bus_config(config, TABLE_B2A), name=f"{TABLE_B2A}_rx"))

    bench.add_ecu(GeneratorEcu(ch=tx_bus, fetch_fn=demo_frames))
    bench.add_ecu(ReceiverEcu(ch=rx_bus))

    return bench


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--run-id",
        required=True,
        help="Shared by both benches; the read-back query filters on it, so a mismatch captures nothing.",
    )
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument(
        "--connection-config",
        default=None,
        metavar="PATH",
        help=(
            "YAML file of Zerobus connection settings (see transport/connection.py::"
            "ConnectionConfig.from_yaml). Falls back to ZEROBUS_*/LAKEBASE_* environment"
            " variables when omitted."
        ),
    )
    args = parser.parse_args(argv)

    if args.connection_config:
        config = ConnectionConfig.from_yaml(args.connection_config)
    else:
        try:
            config = ConnectionConfig.from_environ()
        except KeyError as exc:
            parser.error(f"set LAKEBASE_*/ZEROBUS_* environment variables (missing {exc}) or pass --connection-config")

    build(config, args.run_id).run(duration=args.duration)


if __name__ == "__main__":
    main()
