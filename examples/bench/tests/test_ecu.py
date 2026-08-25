"""Tests for the Ecu/BusHandle facade, driven over loopback buses so a whole
producer/consumer/gateway topology runs in one process with no cloud resources.
"""

from __future__ import annotations

import pytest
from bench import frame as F
from bench.bus import Bus, Loopback
from bench.clock import LOGICAL, WALL
from bench.ecu import Ecu

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000


def make_ecu(name: str, *, clock: str = WALL, tick_hz: float = 1.0) -> Ecu:
    ecu = Ecu(name, clock=clock, tick_hz=tick_hz)
    ecu._bind(run_id=RUN_ID, run_epoch_ns=EPOCH_NS)
    return ecu


def make_bus(name: str, channel: int) -> Bus:
    bus = Loopback(name=name)
    bus.channel = channel
    return bus


def test_handle_resolves_to_the_same_object_for_the_same_bus():
    ecu = make_ecu("gateway")
    bus = make_bus("a", 1)
    assert ecu.handle(bus) is ecu.handle(bus)


def test_send_can_stamps_channel_run_and_source_file():
    ecu = make_ecu("generator")
    bus = make_bus("a", 1)
    frame = ecu.handle(bus).send_can(0x310, b"\x01\x02")
    assert frame.channel == 1
    assert frame.run_id == RUN_ID
    assert frame.source_file == F.source_file_for(RUN_ID)
    assert frame.can_id == 0x310


def test_send_eth_produces_an_ethernet_frame():
    ecu = make_ecu("generator")
    bus = make_bus("b", 2)
    frame = ecu.handle(bus).send_eth(
        src_addr=bytes.fromhex("001122334455"),
        dst_addr=bytes.fromhex("66778899aabb"),
        ether_type=0x0800,
        data=b"payload",
    )
    assert frame.message_type == F.ETH
    assert frame.channel == 2


def test_handlers_receive_only_matching_frames():
    bus = make_bus("a", 1)
    producer, consumer = make_ecu("generator"), make_ecu("receiver")
    seen = []

    @consumer.handle(bus).on(can_id=0x310)
    def _handler(frame):
        seen.append(frame.can_id)

    consumer.handle(bus).subscribe()
    producer.handle(bus).send_can(0x311, b"\x00")
    producer.handle(bus).send_can(0x310, b"\x00")
    consumer.handle(bus).drain(timeout=0.1)
    assert seen == [0x310]


def test_message_type_filter_separates_can_from_ethernet():
    bus = make_bus("b", 2)
    producer, consumer = make_ecu("generator"), make_ecu("receiver")
    seen = []

    @consumer.handle(bus).on(message_type=F.ETH)
    def _handler(frame):
        seen.append(frame.message_type)

    consumer.handle(bus).subscribe()
    producer.handle(bus).send_can(0x310, b"\x00")
    producer.handle(bus).send_eth(src_addr=bytes(6), dst_addr=bytes(6), ether_type=0x88B5, data=b"x")
    consumer.handle(bus).drain(timeout=0.1)
    assert seen == [F.ETH]


def test_an_unrestricted_handler_widens_the_receiver_filter():
    bus = make_bus("a", 1)
    producer, consumer = make_ecu("generator"), make_ecu("receiver")
    narrow, wide = [], []

    consumer.handle(bus).on(can_id=0x310)(narrow.append)
    consumer.handle(bus).on()(wide.append)
    consumer.handle(bus).subscribe()

    producer.handle(bus).send_can(0x310, b"\x00")
    producer.handle(bus).send_can(0x999, b"\x00")
    consumer.handle(bus).drain(timeout=0.1)
    # Restricting the channel-level filter to 0x310 would have starved the wide handler.
    assert [f.can_id for f in wide] == [0x310, 0x999]
    assert [f.can_id for f in narrow] == [0x310]


def test_handlers_cannot_be_registered_after_subscribing():
    bus = make_bus("a", 1)
    ecu = make_ecu("receiver")
    ecu.handle(bus).on(can_id=1)(lambda _f: None)
    ecu.handle(bus).subscribe()
    with pytest.raises(RuntimeError, match="before run"):
        ecu.handle(bus).on(can_id=2)(lambda _f: None)


def test_forward_preserves_the_cross_hop_correlation_key():
    bus_a, bus_b = make_bus("a", 1), make_bus("b", 2)
    generator, gateway = make_ecu("generator"), make_ecu("gateway")
    forwarded = []

    @gateway.handle(bus_a).on()
    def _fwd(frame):
        gateway.handle(bus_b).forward(frame)

    sink = make_ecu("receiver")
    sink.handle(bus_b).on()(forwarded.append)
    gateway.handle(bus_a).subscribe()
    sink.handle(bus_b).subscribe()

    original = generator.handle(bus_a).send_can(0x310, b"\xaa\xbb")
    gateway.handle(bus_a).drain(timeout=0.1)
    sink.handle(bus_b).drain(timeout=0.1)

    assert len(forwarded) == 1
    hop = forwarded[0]
    assert (hop.can_id, hop.timestamp_ns, hop.data) == (original.can_id, original.timestamp_ns, original.data)
    assert hop.channel == 2 and original.channel == 1


def test_run_stops_when_should_stop_is_already_true():
    ecu = make_ecu("generator")
    ecu.run(duration=None, should_stop=lambda: True)


def test_run_stops_after_the_duration_and_fires_ticks():
    ecu = make_ecu("generator", clock=LOGICAL, tick_hz=200.0)
    ticks = []

    def _tick(e):
        # run() already raised if the Ecu had never been bound to a run, so by the time
        # a tick fires the clock is always there.
        assert e.clock is not None
        ticks.append(e.clock.timestamp_ns())

    ecu.run(duration=0.15, on_tick=_tick, poll_timeout=0.001)
    assert len(ticks) >= 2
    # Logical timestamps advance by exactly the tick period regardless of scheduling.
    assert ticks[1] - ticks[0] == 5_000_000


def test_on_tick_lets_an_ecu_generate_its_own_signals():
    bus = make_bus("a", 1)
    generator = make_ecu("generator", clock=LOGICAL, tick_hz=100.0)
    sent = []

    def _tick(ecu):
        frame = ecu.handle(bus).send_can(0x310, b"\x2a")
        sent.append(frame.timestamp_ns)

    generator.run(duration=0.05, on_tick=_tick, poll_timeout=0.001)
    assert len(sent) >= 2
    # Exactly tick_index * tick_period, regardless of how long each tick actually took
    # to run -- the property that makes a logically-clocked run reproducible.
    assert sent == [i * 10_000_000 for i in range(len(sent))]


def test_run_propagates_a_handler_failure():
    bus = make_bus("a", 1)
    producer, consumer = make_ecu("generator"), make_ecu("receiver")

    @consumer.handle(bus).on()
    def _boom(_frame):
        raise ValueError("handler exploded")

    producer.handle(bus).send_can(0x310, b"\x00")
    with pytest.raises(ValueError, match="handler exploded"):
        consumer.run(duration=1.0, poll_timeout=0.01)


def test_run_closes_buses_after_it_stops():
    bus = make_bus("a", 1)
    ecu = make_ecu("receiver")
    ecu.handle(bus).on()(lambda _f: None)
    ecu.run(duration=0.02, poll_timeout=0.005)
    assert ecu.handle(bus)._rx is None


def test_run_requires_bind_before_running():
    ecu = Ecu("generator")
    with pytest.raises(RuntimeError, match="never bound"):
        ecu.run(duration=None, should_stop=lambda: True)
