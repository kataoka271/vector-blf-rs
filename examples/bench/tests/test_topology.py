"""Tests for the topology registry (bench/topology.py): registration, lookup, and
discovery of examples/bench/topologies/*.py.
"""

from __future__ import annotations

import pytest
from bench.bench import TestBench
from bench.topology import discover, get_topology, list_topologies, register_topology


def test_register_and_get_topology():
    def build(run_id: str | None = None) -> TestBench:
        return TestBench(run_id=run_id)

    register_topology("unit-test-topology", build)
    assert get_topology("unit-test-topology") is build
    assert "unit-test-topology" in list_topologies()


def test_get_topology_raises_for_an_unknown_name():
    with pytest.raises(ValueError, match="unknown topology"):
        get_topology("does-not-exist")


def test_discover_registers_the_shipped_topologies():
    discover()
    assert {"reference", "quickstart"} <= set(list_topologies())


def test_quickstart_topology_builds_a_working_bench():
    discover()
    bench = get_topology("quickstart")(run_id="run_001")
    assert bench.run_id == "run_001"
    assert len(bench._buses) == 2
    assert len(bench._ecus) == 3


def test_reference_topology_builds_without_touching_the_network():
    # Building a Bus never opens a connection (open_tx()/open_rx() do that lazily), so
    # this covers the topology's wiring without needing real Lakebase/Zerobus.
    discover()
    bench = get_topology("reference")(run_id="run_001")
    assert bench.run_id == "run_001"
    assert len(bench._buses) == 3
    assert len(bench._ecus) == 5
