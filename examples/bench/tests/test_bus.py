"""Tests for Bus's transport dispatch and TestBench's bus bookkeeping."""

from __future__ import annotations

import pytest
from bench.bench import TestBench
from bench.bus import Bus, Loopback, register_transport
from transport.loopback import LoopbackRx, LoopbackTx


def test_loopback_bus_opens_loopback_channels():
    bus = Loopback(name="a")
    tx = bus.open_tx()
    rx = bus.open_rx(run_id="run_001")
    assert isinstance(tx, LoopbackTx)
    assert isinstance(rx, LoopbackRx)
    tx.close()
    rx.close()


def test_unknown_bus_type_raises_at_construction():
    # Fails fast at Bus(...) rather than waiting until open_tx()/open_rx() is called.
    with pytest.raises(ValueError, match="unknown bus_type"):
        Bus("smoke-signal")


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


def test_register_transport_adds_a_bus_type_without_editing_bus_py():
    # Proves the extension point: a whole new transport needs one register_transport()
    # call, not an edit to Bus.open_tx()/open_rx().
    class _FakeTx:
        def __init__(self, bus: Bus) -> None:
            self.bus = bus

        def send(self, frame) -> None: ...
        def flush(self) -> None: ...
        def close(self) -> None: ...

    class _FakeRx:
        def poll(self, timeout: float = 1.0) -> list:
            return []

        def close(self) -> None: ...

    register_transport("fake", open_tx=_FakeTx, open_rx=lambda bus, **kwargs: _FakeRx())
    bus = Bus("fake", name="f")

    tx = bus.open_tx()
    assert isinstance(tx, _FakeTx)
    assert tx.bus is bus
    assert isinstance(bus.open_rx(run_id="run_001"), _FakeRx)
