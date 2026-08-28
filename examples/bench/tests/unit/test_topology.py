"""Tests for the topology registry (bench/topology.py): registration, lookup, and
discovery of examples/bench/topologies/*.py.
"""

from __future__ import annotations

import pytest
from bench.bench import TestBench
from bench.topology import (
    MissingConfig,
    discover,
    get_topology,
    list_topologies,
    register_topology,
    require_config,
)
from transport.connection import ConnectionConfig


def test_register_and_get_topology():
    def build(config: ConnectionConfig | None = None, run_id: str | None = None) -> TestBench:
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
    config = ConnectionConfig(
        lakebase_database="d",
        zerobus_catalog="c",
        zerobus_schema="s",
        lakebase_endpoint="l",
        zerobus_workspace_id="w",
        zerobus_region="r",
        zerobus_service_principal_id="sp",
    )
    bench = get_topology("quickstart")(config=config, run_id="run_001")
    assert bench.run_id == "run_001"
    assert len(bench._buses) == 2
    assert len(bench._ecus) == 3


def test_quickstart_topology_needs_no_connection_config():
    # quickstart is fully offline, so it never calls require_config() and builds from a
    # None config -- exactly how main.py calls it when LAKEBASE_*/ZEROBUS_* are unset.
    discover()
    bench = get_topology("quickstart")(config=None, run_id="run_001")
    assert bench.run_id == "run_001"


def test_a_credential_needing_topology_rejects_a_missing_config():
    # The mirror of the test above: `config` is optional in BuildFn so that main.py can
    # always call a topology the same way, so a topology that does need credentials says
    # so at runtime instead of in its signature -- see bench/topology.py.
    discover()
    with pytest.raises(MissingConfig, match="reference"):
        get_topology("reference")(config=None, run_id="run_001")


def test_require_config_passes_a_real_config_through():
    config = ConnectionConfig(
        lakebase_database="d",
        zerobus_catalog="c",
        zerobus_schema="s",
        lakebase_endpoint="l",
        zerobus_workspace_id="w",
        zerobus_region="r",
        zerobus_service_principal_id="sp",
    )
    assert require_config(config, "some-topology") is config


def test_reference_topology_builds_without_touching_the_network():
    # Building a Bus never opens a connection (open_tx()/open_rx() do that lazily), so this
    # covers the wiring without real Lakebase/Zerobus -- but ConnectionConfig's fields have
    # no library-level default, so a filled-in config is passed rather than the environment.
    discover()
    config = ConnectionConfig(
        lakebase_database="d",
        zerobus_catalog="c",
        zerobus_schema="s",
        lakebase_endpoint="l",
        zerobus_workspace_id="w",
        zerobus_region="r",
        zerobus_service_principal_id="sp",
    )
    bench = get_topology("reference")(config=config, run_id="run_001")
    assert bench.run_id == "run_001"
    assert len(bench._buses) == 3
    assert len(bench._ecus) == 5
