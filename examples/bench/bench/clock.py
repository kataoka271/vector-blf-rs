"""The run clock: how an Ecu decides the `timestamp_ns` it stamps on a frame.

Every frame carries two times. `observed_ns` is always the real wall clock at the hop
that produced it. `timestamp_ns` is the run-relative time the log is indexed by, and
that is what these classes decide.

`WallClock` (default) derives it from the shared `run_epoch_ns` a TestBench hands out
when it starts -- leaves typical NTP skew (well under 50ms) in the merged log, usually
fine. `LogicalClock` derives it from the tick index instead, so a replayed scenario
produces byte-identical timestamps regardless of network jitter or machine load; use it
whenever a run's output is compared against a stored baseline or another run.
`observed_ns` stays real on both clocks.

Both clocks also schedule ticks (`tick()`/`next_tick_due_ns()`/`sleep_until_next_tick()`),
which is what `Ecu.run()`'s `on_tick` callback uses to fire at a steady rate.
"""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable

WALL = "wall"
LOGICAL = "logical"


@runtime_checkable
class Clock(Protocol):
    """Supplies the timestamps a frame carries, and the tick schedule `Ecu.run()`'s
    `on_tick` fires against.
    """

    run_epoch_ns: int

    def observed_ns(self) -> int:
        """Return absolute wall-clock nanoseconds for the current instant."""
        ...

    def timestamp_ns(self) -> int:
        """Return the run-relative nanoseconds to index this frame by (never negative)."""
        ...

    def tick(self) -> int:
        """Advance to the next simulation tick and return its 0-based index."""
        ...

    def next_tick_due_ns(self) -> int:
        """Return the wall-clock instant at which the next tick becomes due."""
        ...

    def sleep_until_next_tick(self) -> None:
        """Block until the next tick is due, returning immediately if it already is."""
        ...


class WallClock:
    """Run-relative time measured from `run_epoch_ns` with the machine's wall clock."""

    def __init__(self, *, run_epoch_ns: int, tick_hz: float = 1.0) -> None:
        if tick_hz <= 0:
            raise ValueError(f"tick_hz must be > 0, got {tick_hz}")
        self.run_epoch_ns = run_epoch_ns
        self._tick_period_ns = int(1e9 / tick_hz)
        self._tick = -1

    def observed_ns(self) -> int:
        return time.time_ns()

    def timestamp_ns(self) -> int:
        return max(0, self.observed_ns() - self.run_epoch_ns)

    def tick(self) -> int:
        self._tick += 1
        return self._tick

    def next_tick_due_ns(self) -> int:
        # Ticks are scheduled against the run epoch rather than "now + period" so the
        # rate cannot drift as per-tick work varies.
        return self.run_epoch_ns + (self._tick + 1) * self._tick_period_ns

    def sleep_until_next_tick(self) -> None:
        remaining_s = (self.next_tick_due_ns() - time.time_ns()) / 1e9
        if remaining_s > 0:
            time.sleep(remaining_s)


class LogicalClock:
    """Run-relative time derived from the tick index, independent of the wall clock.

    `timestamp_ns` is exactly `tick * tick_period`, so a scenario replayed on a
    different machine, or under a different load, produces identical timestamps.
    `observed_ns` stays real -- it measures the delivery path, which is a property of
    the deployment and not of the simulation.
    """

    def __init__(self, *, run_epoch_ns: int, tick_hz: float = 1.0) -> None:
        if tick_hz <= 0:
            raise ValueError(f"tick_hz must be > 0, got {tick_hz}")
        self.run_epoch_ns = run_epoch_ns
        self._tick_period_ns = int(1e9 / tick_hz)
        self._tick = -1

    def observed_ns(self) -> int:
        return time.time_ns()

    def timestamp_ns(self) -> int:
        return max(0, self._tick) * self._tick_period_ns

    def tick(self) -> int:
        self._tick += 1
        return self._tick

    def next_tick_due_ns(self) -> int:
        return self.run_epoch_ns + (self._tick + 1) * self._tick_period_ns

    def sleep_until_next_tick(self) -> None:
        remaining_s = (self.next_tick_due_ns() - time.time_ns()) / 1e9
        if remaining_s > 0:
            time.sleep(remaining_s)


def make_clock(kind: str, *, run_epoch_ns: int, tick_hz: float = 1.0) -> Clock:
    """Return the clock named by `kind` (`WALL` or `LOGICAL`).

    Raises ValueError for an unknown kind.
    """
    if kind == WALL:
        return WallClock(run_epoch_ns=run_epoch_ns, tick_hz=tick_hz)
    if kind == LOGICAL:
        return LogicalClock(run_epoch_ns=run_epoch_ns, tick_hz=tick_hz)
    raise ValueError(f"unknown clock {kind!r}")
