"""Traffic view: the running bench as a graph of Ecus, bus segments, and how much
traffic flows over each leg.

`snapshot(bench)` reads what `TestBench` already knows -- its buses, its Ecus, and each
`BusHandle`'s counters (`bench.handle.BusStats`) -- into a plain, thread-safe-to-read
value; `to_mermaid()` renders one as a Mermaid `flowchart`, with each edge labelled by
its rate and drawn thicker the busier it is.

Counters are cumulative, so `Snapshot.since(earlier)` is what turns two samples into a
rate over the window between them -- that is what `bench.live` polls to animate the same
diagram while a run is in progress. A snapshot taken once at the end (what `main.py`
prints) needs no `since()`: its window is the whole run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from bench.handle import BusStats

if TYPE_CHECKING:
    from bench.bench import TestBench

# Mermaid edge widths, thinnest (idle) to thickest (the busiest leg in the snapshot).
_STROKE_WIDTHS = (1, 2, 3, 5, 7)


@dataclass(frozen=True)
class BusInfo:
    """One bus segment. `name` is the transport-level key links refer to; `label` is what
    the diagram shows -- the topology's slot name, where the segment has one.
    """

    name: str
    bus_type: str
    channel: int | None
    label: str = ""

    def __post_init__(self) -> None:
        if not self.label:
            object.__setattr__(self, "label", self.name)


@dataclass(frozen=True)
class Link:
    """One Ecu's use of one bus segment, and the traffic it carried.

    `sends`/`receives` are the wiring rather than the traffic: a leg an Ecu subscribed to
    but never heard anything on still belongs in the diagram, drawn idle.
    """

    ecu: str
    bus: str
    sends: bool
    receives: bool
    stats: BusStats


@dataclass(frozen=True)
class Snapshot:
    """The whole bench at one instant: its buses, its Ecu-to-bus legs, and the traffic
    each leg counted over `window_s` seconds.
    """

    run_id: str
    window_s: float
    buses: tuple[BusInfo, ...]
    links: tuple[Link, ...]

    def since(self, earlier: Snapshot) -> Snapshot:
        """Return the traffic counted between `earlier` and this snapshot.

        Legs are matched on (ecu, bus); one absent from `earlier` (an Ecu that had not
        touched that bus yet) counts from zero.
        """
        before = {(link.ecu, link.bus): link.stats for link in earlier.links}
        return Snapshot(
            run_id=self.run_id,
            window_s=max(self.window_s - earlier.window_s, 0.0),
            buses=self.buses,
            links=tuple(
                Link(
                    ecu=link.ecu,
                    bus=link.bus,
                    sends=link.sends,
                    receives=link.receives,
                    stats=link.stats.since(before.get((link.ecu, link.bus), BusStats())),
                )
                for link in self.links
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "window_s": self.window_s,
            "buses": [
                {"name": b.name, "bus_type": b.bus_type, "channel": b.channel, "label": b.label} for b in self.buses
            ],
            "links": [
                {
                    "ecu": link.ecu,
                    "bus": link.bus,
                    "sends": link.sends,
                    "receives": link.receives,
                    "tx_frames": link.stats.tx_frames,
                    "tx_bytes": link.stats.tx_bytes,
                    "rx_frames": link.stats.rx_frames,
                    "rx_bytes": link.stats.rx_bytes,
                }
                for link in self.links
            ],
        }


def snapshot(bench: TestBench) -> Snapshot:
    """Read `bench`'s current wiring and traffic counters into a Snapshot.

    Safe to call from another thread while the run is going: it only reads ints and
    already-registered objects, so the worst a concurrent frame can do is land in the
    next snapshot instead of this one.
    """
    links = [
        Link(
            ecu=ecu.name,
            bus=handle.bus.name,
            sends=handle.sends,
            receives=handle.receives,
            stats=handle.stats,
        )
        for ecu in bench.ecus
        for handle in ecu.handles
    ]
    return Snapshot(
        run_id=bench.run_id,
        window_s=bench.elapsed,
        buses=tuple(
            BusInfo(name=bus.name, bus_type=bus.bus_type, channel=bus.channel, label=bus.label) for bus in bench.buses
        ),
        links=tuple(links),
    )


def to_mermaid(snap: Snapshot, *, title: str | None = None) -> str:
    """Render `snap` as a Mermaid `flowchart LR`.

    Ecus are rounded nodes, bus segments are labelled with their transport and BLF
    channel. Each direction an Ecu actually uses becomes one edge, labelled with the
    frames, frame rate and byte rate over the snapshot's window, and drawn thicker the
    busier it is; a wired-but-silent leg is drawn as a dotted line.
    """
    ecu_ids = _node_ids(dict.fromkeys(link.ecu for link in snap.links), "e")
    bus_ids = _node_ids([bus.name for bus in snap.buses], "b")
    for link in snap.links:  # a bus an Ecu holds but TestBench never registered
        bus_ids.setdefault(link.bus, f"b{len(bus_ids)}")

    lines = ["flowchart LR"]
    if title:
        lines.insert(0, f"---\ntitle: {title}\n---")
    for name, node in ecu_ids.items():
        lines.append(f'    {node}(["{name}"])')
    for bus in snap.buses:
        channel = "" if bus.channel is None else f" ch{bus.channel}"
        lines.append(f'    {bus_ids[bus.name]}[["{bus.label}<br/>{bus.bus_type}{channel}"]]')

    edges = list(_edges(snap))
    busiest = max((edge[3] for edge in edges), default=0)
    styles = []
    for index, (src, dst, label, frames, _bytes) in enumerate(edges):
        arrow = "-->" if frames else "-.->"
        lines.append(
            f'    {ecu_ids.get(src, bus_ids.get(src))} {arrow}|"{label}"| {ecu_ids.get(dst, bus_ids.get(dst))}'
        )
        styles.append(f"    linkStyle {index} stroke-width:{_stroke_width(frames, busiest)}px")
    lines.extend(styles)
    return "\n".join(lines)


def _edges(snap: Snapshot) -> list[tuple[str, str, str, int, int]]:
    """Yield (source, target, label, frames, bytes) for every leg an Ecu uses.

    Transmit and receive are separate edges rather than one bidirectional line: a
    forwarding Ecu's whole point is that the two directions carry different traffic.
    """
    edges = []
    for link in snap.links:
        if link.sends or link.stats.tx_frames:
            edges.append(
                (
                    link.ecu,
                    link.bus,
                    _label(link.stats.tx_frames, link.stats.tx_bytes, snap.window_s),
                    link.stats.tx_frames,
                    link.stats.tx_bytes,
                )
            )
        if link.receives or link.stats.rx_frames:
            edges.append(
                (
                    link.bus,
                    link.ecu,
                    _label(link.stats.rx_frames, link.stats.rx_bytes, snap.window_s),
                    link.stats.rx_frames,
                    link.stats.rx_bytes,
                )
            )
    return edges


def _label(frames: int, byte_count: int, window_s: float) -> str:
    if not frames:
        return "idle"
    if window_s <= 0:
        return f"{frames} fr"
    return f"{frames} fr<br/>{frames / window_s:.1f} fr/s<br/>{_byte_rate(byte_count / window_s)}"


def _byte_rate(bytes_per_s: float) -> str:
    if bytes_per_s >= 1e6:
        return f"{bytes_per_s / 1e6:.2f} MB/s"
    if bytes_per_s >= 1e3:
        return f"{bytes_per_s / 1e3:.1f} kB/s"
    return f"{bytes_per_s:.0f} B/s"


def _stroke_width(frames: int, busiest: int) -> int:
    if not frames or busiest <= 0:
        return _STROKE_WIDTHS[0]
    step = frames * (len(_STROKE_WIDTHS) - 1) // busiest
    return _STROKE_WIDTHS[step]


def _node_ids(names: list[str] | dict[str, Any], prefix: str) -> dict[str, str]:
    """Map each name to a short, Mermaid-safe node id.

    Ids are positional rather than derived from the name: bus and Ecu names are free-form
    and two of them can sanitize to the same identifier.
    """
    return {name: f"{prefix}{index}" for index, name in enumerate(names)}


def format_summary(snap: Snapshot, *, title: str | None = None) -> str:
    """Render `snap` as a fenced Mermaid block, ready to paste into Markdown."""
    return f"```mermaid\n{to_mermaid(snap, title=title)}\n```"
