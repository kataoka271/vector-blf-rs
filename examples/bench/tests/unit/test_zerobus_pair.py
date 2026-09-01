"""Offline tests for the standalone bench pair (zerobus_bench_a.py / zerobus_bench_b.py):
that each script builds two buses and two Ecus, and that the two are mirror images -- the
table A's Generator sends on is the one B's Receiver listens to and vice versa, which is
the one thing a swapped table name would silently break (each bench would hear only
itself).

Building a bench opens no connection (open_tx()/open_rx() are lazy), so nothing here
touches Zerobus or a SQL warehouse.
"""

from __future__ import annotations

import zerobus_bench_a
import zerobus_bench_b
from bench.bench import TestBench
from bench.bus import ZEROBUS_DUPLEX
from transport.connection import ConnectionConfig
from transport.zerobus_duplex import ZerobusDuplexConfig

RUN_ID = "run_001"

CONFIG = ConnectionConfig(
    lakebase_endpoint="endpoint",
    lakebase_database="db",
    zerobus_catalog="main",
    zerobus_schema="blf",
    zerobus_workspace_id="1234567890",
    zerobus_region="us-east-2",
    zerobus_service_principal_id="1122334455",
)


def _table(bench: TestBench, ecu_name: str) -> str:
    """The Unity Catalog table behind the one bus `ecu_name` is wired to."""
    (ecu,) = [e for e in bench._ecus if e.name == ecu_name]
    (handle,) = ecu._buses.values()
    assert isinstance(handle.bus.config, ZerobusDuplexConfig)
    return handle.bus.config.table


def test_each_bench_has_two_buses_a_generator_and_a_receiver():
    for module in (zerobus_bench_a, zerobus_bench_b):
        bench = module.build(CONFIG, RUN_ID)

        assert [b.bus_type for b in bench._buses.values()] == [ZEROBUS_DUPLEX, ZEROBUS_DUPLEX]
        assert sorted(e.name for e in bench._ecus) == ["generator", "receiver"]
        # Distinct channels: the two legs are separate bus segments, not one duplicated.
        assert len({b.channel for b in bench._buses.values()}) == 2


def test_the_two_benches_send_where_the_other_one_listens():
    bench_a = zerobus_bench_a.build(CONFIG, RUN_ID)
    bench_b = zerobus_bench_b.build(CONFIG, RUN_ID)

    assert _table(bench_a, "generator") == _table(bench_b, "receiver")
    assert _table(bench_b, "generator") == _table(bench_a, "receiver")
    # One table per direction: a shared one would echo each bench's own output back.
    assert _table(bench_a, "generator") != _table(bench_b, "generator")


def test_a_captured_frame_says_which_bench_sent_it():
    ids_a = set(zerobus_bench_a.demo_frames()["can_id"])
    ids_b = set(zerobus_bench_b.demo_frames()["can_id"])

    assert not ids_a & ids_b
