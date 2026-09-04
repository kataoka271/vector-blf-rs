"""Tests for the traffic view: which legs a snapshot finds, what the counters say, and
what `to_mermaid()` renders.

The bench is driven by hand here (handles opened and drained directly) rather than by
`TestBench.run()` -- what a real run does to the numbers is covered in
`local/test_live.py`.
"""

from __future__ import annotations

import dataclasses
import time

from bench import traffic
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import Ecu
from bench.frame import Frame
from bench.handle import BusStats

RUN_ID = "run_001"


def _bench() -> tuple[TestBench, Ecu, Ecu]:
    """A sender and a listener wired to one loopback bus, bound but not thread-driven."""
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback(name="bus1"))
    sender, listener = Ecu("sender"), Ecu("listener")
    bench.add_ecu(sender)
    bench.add_ecu(listener)

    @listener.handle(bus).on()
    def _ignore(frame: Frame) -> None:
        pass

    run_epoch_ns = time.time_ns()
    for ecu in (sender, listener):
        ecu._bind(run_id=RUN_ID, run_epoch_ns=run_epoch_ns)
    sender.handle(bus)
    listener.handle(bus).subscribe()
    return bench, sender, listener


def _link(snap: traffic.Snapshot, ecu: str) -> traffic.Link:
    return next(link for link in snap.links if link.ecu == ecu)


def test_snapshot_counts_a_sent_frame_on_the_sending_leg():
    bench, sender, _listener = _bench()
    sender.handles[0].send_can(0x310, b"\x01\x02\x03\x04")

    link = _link(traffic.snapshot(bench), "sender")
    assert (link.stats.tx_frames, link.stats.tx_bytes) == (1, 4)
    assert (link.stats.rx_frames, link.sends, link.receives) == (0, True, False)


def test_snapshot_counts_a_received_frame_on_the_receiving_leg():
    bench, sender, listener = _bench()
    sender.handles[0].send_can(0x310, b"\xaa\xbb")
    listener.handles[0].drain(0.05)

    link = _link(traffic.snapshot(bench), "listener")
    assert (link.stats.rx_frames, link.stats.rx_bytes) == (1, 2)
    assert (link.stats.tx_frames, link.sends, link.receives) == (0, False, True)


def test_a_handles_own_echo_is_not_counted_as_received():
    # The same suppression drain() already applies: a sender that also listens must not
    # show its own transmissions as received traffic.
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback(name="bus1"))
    ecu = Ecu("both")
    bench.add_ecu(ecu)

    @ecu.handle(bus).on()
    def _ignore(frame: Frame) -> None:
        pass

    ecu._bind(run_id=RUN_ID, run_epoch_ns=time.time_ns())
    ecu.handles[0].subscribe()
    ecu.handles[0].send_can(0x310, b"\x01")
    ecu.handles[0].drain(0.05)

    link = _link(traffic.snapshot(bench), "both")
    assert (link.stats.tx_frames, link.stats.rx_frames) == (1, 0)


def test_since_reports_only_the_traffic_of_the_window():
    bench, sender, _listener = _bench()
    sender.handles[0].send_can(0x310, b"\x01\x02")
    earlier = traffic.snapshot(bench)
    sender.handles[0].send_can(0x311, b"\x03\x04")

    window = traffic.snapshot(bench).since(earlier)
    assert _link(window, "sender").stats.tx_frames == 1


def test_since_counts_a_leg_absent_from_the_earlier_snapshot_from_zero():
    bench, sender, _listener = _bench()
    earlier = traffic.Snapshot(run_id=RUN_ID, window_s=0.0, buses=(), links=())
    sender.handles[0].send_can(0x310, b"\x01")

    window = traffic.snapshot(bench).since(earlier)
    assert _link(window, "sender").stats.tx_frames == 1


def test_stats_since_subtracts_every_counter():
    later = BusStats(tx_frames=10, tx_bytes=80, rx_frames=4, rx_bytes=32)
    earlier = BusStats(tx_frames=3, tx_bytes=24, rx_frames=1, rx_bytes=8)
    assert later.since(earlier) == BusStats(tx_frames=7, tx_bytes=56, rx_frames=3, rx_bytes=24)


def test_mermaid_names_every_ecu_and_bus():
    bench, sender, _listener = _bench()
    sender.handles[0].send_can(0x310, b"\x01")

    rendered = traffic.to_mermaid(traffic.snapshot(bench))
    assert rendered.startswith("flowchart LR")
    for label in ('"sender"', '"listener"', "bus1<br/>loopback ch1"):
        assert label in rendered


def test_mermaid_labels_an_edge_with_its_rate():
    bench, sender, _listener = _bench()
    for _ in range(4):
        sender.handles[0].send_can(0x310, b"\x01\x02\x03\x04")

    # A fixed window, so the rate in the label is not the test machine's wall clock.
    snap = dataclasses.replace(traffic.snapshot(bench), window_s=2.0)
    rendered = traffic.to_mermaid(snap)
    assert "4 fr<br/>2.0 fr/s<br/>8 B/s" in rendered


def test_mermaid_draws_a_silent_leg_as_a_dotted_idle_edge():
    bench, _sender, _listener = _bench()

    rendered = traffic.to_mermaid(traffic.snapshot(bench))
    assert '-.->|"idle"|' in rendered


def test_mermaid_draws_the_busiest_leg_thickest():
    bench, sender, listener = _bench()
    sender.handles[0].send_can(0x310, b"\x01")
    listener.handles[0].drain(0.05)
    for _ in range(2):  # sent after the listener's only drain, so it stays behind at 1
        sender.handles[0].send_can(0x310, b"\x01")

    rendered = traffic.to_mermaid(traffic.snapshot(bench))
    widths = [line.split("stroke-width:")[1] for line in rendered.splitlines() if "linkStyle" in line]
    assert widths == ["7px", "2px"]  # the sender's 3 frames, then the listener's 1


def test_mermaid_labels_a_bus_with_its_topology_slot():
    # A topology's slot name is what a reader recognises; the bus's own name is a
    # generated id unless the topology bothered to pass one.
    bench = TestBench(run_id=RUN_ID)
    bench.bus("bus1", Loopback())

    assert "bus1<br/>loopback ch1" in traffic.to_mermaid(traffic.snapshot(bench))


def test_mermaid_adds_a_title_when_given_one():
    bench, _sender, _listener = _bench()
    assert traffic.to_mermaid(traffic.snapshot(bench), title="quickstart").startswith("---\ntitle: quickstart\n---")


def test_format_summary_fences_the_diagram_for_markdown():
    bench, _sender, _listener = _bench()
    summary = traffic.format_summary(traffic.snapshot(bench))
    assert summary.startswith("```mermaid\n") and summary.endswith("\n```")


def test_to_dict_round_trips_through_json_shaped_values():
    bench, sender, _listener = _bench()
    sender.handles[0].send_can(0x310, b"\x01")

    raw = traffic.snapshot(bench).to_dict()
    assert raw["run_id"] == RUN_ID
    assert {"name": "bus1", "bus_type": "loopback", "channel": 1, "label": "bus1"} in raw["buses"]
    assert next(link for link in raw["links"] if link["ecu"] == "sender")["tx_frames"] == 1
