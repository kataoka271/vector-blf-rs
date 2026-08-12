"""BusHandle: one bus segment as an Ecu sees it.

Transmit and receive sides are opened lazily -- a segment an Ecu only sends on never
subscribes, and one it only listens to never opens a writer. This is also what keeps
Zerobus usable for a send-only ReceiverEcu leg despite its Rx always raising: nothing
ever calls open_rx() on it unless the Ecu itself registers a handler via `.on()`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from bench.clock import WallClock
from bench.frame import DIR_RX, Frame, make_can_frame, make_eth_frame

if TYPE_CHECKING:
    from bench.bus import Bus
    from bench.ecu import Ecu

Handler = Callable[[Frame], None]


class _Subscription:
    def __init__(self, handler: Handler, can_ids: set[int] | None, message_types: set[str] | None) -> None:
        self.handler = handler
        self.can_ids = can_ids
        self.message_types = message_types

    def matches(self, frame: Frame) -> bool:
        if self.can_ids is not None and frame.can_id not in self.can_ids:
            return False
        if self.message_types is not None and frame.message_type not in self.message_types:
            return False
        return True


class BusHandle:
    """One bus segment as this Ecu sees it."""

    def __init__(self, bus: Bus, ecu: Ecu) -> None:
        self.bus = bus
        self._ecu = ecu
        self._tx = None
        self._rx = None
        self._subscriptions: list[_Subscription] = []

    @property
    def channel(self) -> int:
        if self.bus.channel is None:
            raise RuntimeError(f"bus {self.bus.name!r} has no channel assigned; add it via TestBench.add_bus() first")
        return self.bus.channel

    @property
    def _clock(self) -> WallClock:
        if self._ecu.clock is None:
            raise RuntimeError(f"Ecu {self._ecu.name!r} was never bound to a run; add it via TestBench.add_ecu() first")
        return self._ecu.clock

    def on(
        self,
        *,
        can_id: int | None = None,
        can_ids: Sequence[int] | None = None,
        message_type: str | None = None,
        message_types: Sequence[str] | None = None,
    ) -> Callable[[Handler], Handler]:
        """Register a handler for frames on this segment, as a decorator.

        Filters are ANDed and each defaults to accepting everything. Must be called
        before run(); the union of every handler's filters is what the receiver asks
        the channel for, so a handler added later would not be fed.
        """
        ids = _as_set(can_id, can_ids)
        types = _as_set(message_type, message_types)

        def decorator(handler: Handler) -> Handler:
            if self._rx is not None:
                raise RuntimeError(f"bus {self.bus.name!r}: handlers must be registered before run()")
            self._subscriptions.append(_Subscription(handler, ids, types))
            return handler

        return decorator

    def send(self, frame: Frame) -> None:
        """Transmit `frame` as-is."""
        if self._tx is None:
            self._tx = self.bus.open_tx()
        self._tx.send(frame)

    def forward(self, frame: Frame) -> None:
        """Re-stamp `frame` onto this segment and transmit it.

        Keeps timestamp_ns and the payload, so the frame stays joinable to its other hop
        for a per-hop latency query.
        """
        self.send(frame.forwarded(channel=self.channel, observed_ns=self._clock.observed_ns()))

    def send_can(self, can_id: int, data: bytes, *, dir: int = DIR_RX, is_fd: bool = False, **kwargs) -> Frame:
        """Build a CAN frame timestamped by this run's clock, transmit it, and return it."""
        frame = make_can_frame(
            run_id=self._ecu.run_id,
            source_file=self._ecu.source_file,
            run_epoch_ns=self._clock.run_epoch_ns,
            channel=self.channel,
            can_id=can_id,
            data=data,
            dir=dir,
            is_fd=is_fd,
            timestamp_ns=self._clock.timestamp_ns(),
            observed_ns=self._clock.observed_ns(),
            **kwargs,
        )
        self.send(frame)
        return frame

    def send_eth(
        self,
        *,
        src_addr: bytes,
        dst_addr: bytes,
        ether_type: int,
        data: bytes,
        dir: int = DIR_RX,
        **kwargs,
    ) -> Frame:
        """Build an Ethernet frame timestamped by this run's clock, transmit it, and
        return it.
        """
        frame = make_eth_frame(
            run_id=self._ecu.run_id,
            source_file=self._ecu.source_file,
            run_epoch_ns=self._clock.run_epoch_ns,
            channel=self.channel,
            src_addr=src_addr,
            dst_addr=dst_addr,
            ether_type=ether_type,
            data=data,
            dir=dir,
            timestamp_ns=self._clock.timestamp_ns(),
            observed_ns=self._clock.observed_ns(),
            **kwargs,
        )
        self.send(frame)
        return frame

    def subscribe(self) -> None:
        """Open the receive side, filtered to the union of the registered handlers.

        No-op when nothing is subscribed or the receiver is already open. Called by
        Ecu.run(); only call it directly when driving the poll loop yourself.
        """
        if self._rx is not None or not self._subscriptions:
            return
        self._rx = self.bus.open_rx(
            run_id=self._ecu.run_id,
            can_ids=_union(sub.can_ids for sub in self._subscriptions),
            message_types=_union(sub.message_types for sub in self._subscriptions),
            source_file=self._ecu.source_file,
            run_epoch_ns=self._clock.run_epoch_ns,
        )

    def drain(self, timeout: float) -> int:
        """Poll the receiver once and dispatch what it returns. Returns the frame count.

        Returns 0 without blocking when this segment has no subscriptions.
        """
        if self._rx is None:
            return 0
        frames = self._rx.poll(timeout=timeout)
        for frame in frames:
            for sub in self._subscriptions:
                if sub.matches(frame):
                    sub.handler(frame)
        return len(frames)

    def close(self) -> None:
        for side in (self._tx, self._rx):
            if side is not None:
                try:
                    side.close()
                except Exception as exc:  # noqa: BLE001 -- teardown must not mask a run's real failure
                    print(f"[bench] closing bus {self.bus.name}: {exc!r}", flush=True)
        self._tx = self._rx = None


def _as_set(single, plural) -> set | None:
    if single is None and plural is None:
        return None
    values = set() if plural is None else set(plural)
    if single is not None:
        values.add(single)
    return values


def _union(sets) -> list | None:
    """Return the union of the given filters, or None if any of them is unrestricted.

    One unrestricted handler makes the whole receiver unrestricted -- narrowing the
    channel-level filter to the other handlers' ids would starve it.
    """
    out: set = set()
    for values in sets:
        if values is None:
            return None
        out |= values
    return sorted(out) if out else None
