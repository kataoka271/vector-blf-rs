"""Reference topology for examples/bench: a minimal, unified rewrite of
examples/testing's replay_bench + vecu_sdk systems (see the plan this was built from,
and examples/testing/README.md for the two systems being unified).

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
    uv run --group testing python examples/bench/main.py

Requires a working Databricks/Lakebase/Zerobus setup (see examples/testing/README.md's
prerequisites -- examples/bench does not duplicate that setup documentation). For a
network-free smoke test of just the wiring/forwarding/capture logic, see
examples/bench/tests/test_testbench_gateway.py, which drives the same topology over
loopback buses with a hand-built fetch_fn instead.
"""

from __future__ import annotations

from bench.bench import TestBench
from bench.bus import Lakebase, Zerobus
from bench.ecu import GatewayEcu, ProxyEcu, ReceiverEcu
from bench.replay import GeneratorEcu, ReplayEcu


def main() -> None:
    bench = TestBench()

    bus1 = bench.add_bus(Lakebase())
    bus2 = bench.add_bus(Lakebase())
    zerobus = bench.add_bus(Zerobus())

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

    # Each Ecu also stops on its own should_stop()/KeyboardInterrupt; this duration is
    # just the run's overall wall-clock bound.
    bench.run(duration=60.0)


if __name__ == "__main__":
    main()
