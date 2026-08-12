"""The run clock: how an ECU decides the `timestamp_ns` it stamps on a frame.

Every frame carries two times. `observed_ns` is always the real wall clock at the hop
that produced it, which is what makes per-hop latency measurable. `timestamp_ns` is the
run-relative time the log is indexed by. WallClock derives it from the shared
`run_epoch_ns` a TestBench hands out when it starts. Across development environments
that leaves NTP skew (typically well under 50 ms) in the merged log -- the same order as
the delivery latency itself, so it is usually fine.

A reproducible, tick-derived LogicalClock (independent of wall-clock jitter) is a
deferred item -- see the plan's "explicitly deferred" table.
"""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Supplies the two timestamps a frame carries."""

    run_epoch_ns: int

    def observed_ns(self) -> int:
        """Return absolute wall-clock nanoseconds for the current instant."""
        ...

    def timestamp_ns(self) -> int:
        """Return the run-relative nanoseconds to index this frame by (never negative)."""
        ...


class WallClock:
    """Run-relative time measured from `run_epoch_ns` with the machine's wall clock."""

    def __init__(self, *, run_epoch_ns: int) -> None:
        self.run_epoch_ns = run_epoch_ns

    def observed_ns(self) -> int:
        return time.time_ns()

    def timestamp_ns(self) -> int:
        return max(0, self.observed_ns() - self.run_epoch_ns)
