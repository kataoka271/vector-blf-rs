"""Tests for TestBench's bus bookkeeping: which BLF channel number each registered bus
ends up with. Channel numbers are what distinguish the two ends of a forwarding ECU in
the merged log, so a collision or a silent renumber would corrupt every downstream
per-hop query.

Running a bench (threads, binding, stopping) is exercised in `local/test_testbench.py`.
"""

from __future__ import annotations

import pytest
from bench.bench import TestBench
from bench.bus import Loopback


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


def test_add_bus_keeps_a_channel_a_bus_already_carries():
    # A bus can be handed a channel before it is registered (a topology pinning one to
    # match a recorded log); add_bus() must not renumber it.
    bench = TestBench()
    bus = Loopback(name="a")
    bus.channel = 7
    assert bench.add_bus(bus).channel == 7
    assert bench.add_bus(Loopback(name="b")).channel == 8


def test_bus_slot_falls_back_to_the_topology_default():
    bench = TestBench()
    default = Loopback(name="default")
    assert bench.bus("bus1", default) is default


def test_bus_slot_takes_the_callers_override_without_building_the_default():
    built = []

    def default():
        built.append(1)
        return Loopback(name="default")

    override = Loopback(name="override")
    bench = TestBench(buses={"bus1": override})
    assert bench.bus("bus1", default) is override
    assert built == []


def test_bus_slot_assigns_channels_like_add_bus():
    bench = TestBench(buses={"bus2": Loopback(name="override")})
    bus1 = bench.bus("bus1", Loopback(name="a"))
    bus2 = bench.bus("bus2", Loopback(name="b"))
    assert (bus1.channel, bus2.channel) == (1, 2)
    assert bus2.name == "override"


def test_start_rejects_an_override_for_an_unknown_slot():
    bench = TestBench(buses={"bus9": Loopback(name="override")})
    bench.bus("bus1", Loopback(name="a"))
    with pytest.raises(ValueError, match="no such bus slot: bus9"):
        bench.start(duration=0.0)


def test_a_run_id_is_generated_when_none_is_given():
    assert TestBench().run_id != TestBench().run_id
    assert TestBench(run_id="run_001").run_id == "run_001"
