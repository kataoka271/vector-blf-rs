"""Tests for Bus's transport dispatch and TestBench's bus bookkeeping."""

from __future__ import annotations

import pytest
from bench.bench import TestBench
from bench.bus import Bus, Lakebase, Loopback
from bench.channel import LoopbackRx, LoopbackTx
from transport.lakebase import LakebaseConfig


def test_loopback_bus_opens_loopback_channels():
    bus = Loopback(name="a")
    tx = bus.open_tx()
    rx = bus.open_rx(run_id="run_001")
    assert isinstance(tx, LoopbackTx)
    assert isinstance(rx, LoopbackRx)
    tx.close()
    rx.close()


def test_unknown_bus_type_raises():
    bus = Bus("smoke-signal")
    with pytest.raises(ValueError, match="unknown bus_type"):
        bus.open_tx()


def test_two_lakebase_buses_get_distinct_default_table_names():
    # Both Lakebase() calls take no config, so this only stays correct as long as Bus
    # resolves an unset LakebaseConfig.table from its own generated name -- see
    # transport/lakebase.py's LakebaseConfig.table docstring.
    a = Lakebase()
    b = Lakebase()
    assert isinstance(a.config, LakebaseConfig)
    assert isinstance(b.config, LakebaseConfig)
    assert a.config.table != b.config.table
    assert a.config.table == a.name
    assert b.config.table == b.name


def test_add_bus_auto_assigns_incrementing_channels():
    bench = TestBench()
    bus1 = bench.add_bus(Loopback(name="a"))
    bus2 = bench.add_bus(Loopback(name="b"))
    assert (bus1.channel, bus2.channel) == (1, 2)


def test_add_bus_rejects_a_duplicate_channel():
    bench = TestBench()
    bench.add_bus(Loopback(name="a"), channel=5)
    with pytest.raises(ValueError, match="already assigned"):
        bench.add_bus(Loopback(name="b"), channel=5)


def test_add_bus_respects_an_explicit_channel_then_continues_after_it():
    bench = TestBench()
    bench.add_bus(Loopback(name="a"), channel=5)
    bus2 = bench.add_bus(Loopback(name="b"))
    assert bus2.channel == 6
