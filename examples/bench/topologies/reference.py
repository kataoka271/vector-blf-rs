"""The reference topology (see examples/testing/README.md for prerequisites):

    Generator --(bus1, Lakebase)--> Gateway --(bus2, Lakebase)--> Receiver --> Zerobus
                                        ^                            ^
    Proxy (external UDP) -------------/         Replay (re-injects prior output) -/

- `generator` replays recorded traffic (blf_gold_signals/blf_silver_can) onto bus1.
- `gateway` forwards everything from bus1 to bus2 -- the ECU under test, in this demo
  topology played by a pass-through rather than an external process.
- `receiver` captures bus2 and uploads every frame to Zerobus (which the blf_ingestion
  pipeline unions into blf_bronze, so results reach blf_gold_signals for comparison).
- `replay` re-injects a prior testbench run's captured output onto bus2, independent of
  whether `gateway` is live.
- `proxy` opens a UDP port and forwards anything it receives onto bus1, for bridging in
  an external (non-Python) process instead of `generator`.

Run with:
    uv run --group testing python examples/bench/main.py --topology reference

Requires a working Databricks/Lakebase/Zerobus setup. `build()` rejects a missing
`config` via `require_config()` (see bench/topology.py); main.py resolves one from
LAKEBASE_*/ZEROBUS_* environment variables by default, or `--connection-config
<path.yaml>`. For a network-free smoke test, see the `quickstart` topology instead.
"""

from __future__ import annotations

from bench.bench import TestBench
from bench.bus import Lakebase, Zerobus
from bench.ecu import GatewayEcu, ProxyEcu, ReceiverEcu
from bench.replay import GeneratorEcu, ReplayEcu
from bench.topology import register_topology, require_config
from transport.connection import ConnectionConfig


def build(config: ConnectionConfig | None = None, run_id: str | None = None) -> TestBench:
    config = require_config(config, "reference")
    bench = TestBench(run_id=run_id)

    bus1 = bench.add_bus(Lakebase(config.lakebase_config(table="bench_reference_bus1")))
    bus2 = bench.add_bus(Lakebase(config.lakebase_config(table="bench_reference_bus2")))
    zerobus = bench.add_bus(Zerobus(config.zerobus_config(table="blf_testbench_frames")))

    generator = GeneratorEcu(ch=bus1)
    gateway = GatewayEcu(ch1=bus1, ch2=bus2)
    receiver = ReceiverEcu(ch=bus2, zerobus=zerobus)
    replay = ReplayEcu(ch=bus2)
    proxy = ProxyEcu(ch=bus1, port=12345)  # opens a UDP port, forwards received frames onto bus1

    bench.add_ecu(generator)
    bench.add_ecu(gateway)
    bench.add_ecu(receiver)
    bench.add_ecu(replay)
    bench.add_ecu(proxy)

    return bench


register_topology("reference", build)
