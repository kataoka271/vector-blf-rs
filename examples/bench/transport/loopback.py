"""Loopback channel: in-process, synchronous, deterministic. The default transport for
tests -- no cloud resources, no network, and (unlike every other transport) no optional
dependency to install.

Delivery happens inline inside `send()`, so once it returns every attached receiver
already has the frame. A segment also keeps a bounded backlog, replayed to a receiver as
it attaches, mirroring the catch-up the Lakebase channel performs on connect -- without
it, an Ecu that starts polling after a frame was already sent would behave differently
depending on which transport it used.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Sequence

from bench.frame import Frame, accepts

# Frames retained per segment for late subscribers.
BACKLOG_LIMIT = 10_000

_segments: dict[str, _Segment] = {}
_segments_lock = threading.Lock()


class _Segment:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.receivers: list[LoopbackRx] = []
        self.backlog: list[Frame] = []


def _segment(name: str) -> _Segment:
    with _segments_lock:
        return _segments.setdefault(name, _Segment())


def reset() -> None:
    """Drop every loopback segment and its attached receivers.

    Intended for test isolation; segments are otherwise process-global so that two
    channels constructed independently with the same table name find each other.
    """
    with _segments_lock:
        _segments.clear()


class LoopbackTx:
    """Transmit side of an in-process channel. Delivery is synchronous inside send()."""

    def __init__(self, *, table: str) -> None:
        self._segment = _segment(table)
        self._closed = False

    def send(self, frame: Frame) -> None:
        if self._closed:
            raise RuntimeError("loopback channel tx is closed")
        with self._segment.lock:
            self._segment.backlog.append(frame)
            del self._segment.backlog[:-BACKLOG_LIMIT]
            receivers = list(self._segment.receivers)
        for rx in receivers:
            rx._offer(frame)

    def flush(self) -> None:
        pass  # send() has already delivered

    def close(self) -> None:
        self._closed = True


class LoopbackRx:
    """Receive side of an in-process channel, filtered to this `run_id` and optionally
    `can_ids`/`message_types`.
    """

    def __init__(
        self,
        *,
        table: str,
        run_id: str,
        can_ids: Sequence[int] | None = None,
        message_types: Sequence[str] | None = None,
    ) -> None:
        self._run_id = run_id
        self._can_ids = set(can_ids) if can_ids else None
        self._message_types = set(message_types) if message_types else None
        self._queue: queue.Queue[Frame] = queue.Queue()
        self._segment = _segment(table)
        # Attach and replay under one lock hold, so a concurrent send is either in the
        # backlog we replay or fanned out to us, never both and never neither.
        with self._segment.lock:
            backlog = list(self._segment.backlog)
            self._segment.receivers.append(self)
        for frame in backlog:
            self._offer(frame)

    def _offer(self, frame: Frame) -> None:
        if frame.run_id != self._run_id:
            return
        if accepts(frame, can_ids=self._can_ids, message_types=self._message_types):
            self._queue.put(frame)

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        try:
            frames.append(self._queue.get(timeout=timeout))
        except queue.Empty:
            return frames
        while True:
            try:
                frames.append(self._queue.get_nowait())
            except queue.Empty:
                return frames

    def close(self) -> None:
        with self._segment.lock:
            if self in self._segment.receivers:
                self._segment.receivers.remove(self)
