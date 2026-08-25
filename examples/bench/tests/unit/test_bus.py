"""Tests for Bus's transport registry: a bus_type string selects a transport, and adding
a transport is one register_transport() call rather than an edit to Bus.

Which concrete channel a bus_type resolves to is exactly what a type checker cannot see
-- `Bus.open_tx()` is annotated as returning the `ChannelTx` protocol, and ty already
proves every registered implementation satisfies it.
"""

from __future__ import annotations

import pytest
from bench.bus import CAN, LAKEBASE, LOOPBACK, ZEROBUS, Bus, Loopback, register_transport
from bench.frame import Frame
from transport.loopback import LoopbackRx, LoopbackTx


def test_the_four_built_in_transports_are_registered():
    for bus_type in (LOOPBACK, LAKEBASE, ZEROBUS, CAN):
        assert Bus(bus_type, name=f"bus-{bus_type}").bus_type == bus_type


def test_loopback_bus_type_resolves_to_the_loopback_channels():
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


def test_connect_opens_both_sides_at_once():
    tx, rx = Loopback(name="a").connect(run_id="run_001")
    try:
        assert isinstance(tx, LoopbackTx)
        assert isinstance(rx, LoopbackRx)
    finally:
        tx.close()
        rx.close()


def test_a_bus_gets_a_unique_generated_name_when_unnamed():
    assert Loopback().name != Loopback().name


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
        def poll(self, timeout: float = 1.0) -> list[Frame]:
            return []

        def close(self) -> None: ...

    register_transport("fake", open_tx=_FakeTx, open_rx=lambda bus, **kwargs: _FakeRx())
    bus = Bus("fake", name="f")

    tx = bus.open_tx()
    assert isinstance(tx, _FakeTx)
    assert tx.bus is bus
    assert isinstance(bus.open_rx(run_id="run_001"), _FakeRx)
