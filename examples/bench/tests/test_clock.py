"""Tests for the run clocks, in particular the property that motivates the logical one:
identical timestamps regardless of when the process actually got scheduled.
"""

from __future__ import annotations

import pytest
from bench.clock import LOGICAL, WALL, Clock, LogicalClock, WallClock, make_clock

EPOCH_NS = 1_700_000_000_000_000_000


def test_make_clock_returns_the_kind_named():
    assert isinstance(make_clock(WALL, run_epoch_ns=EPOCH_NS, tick_hz=1.0), WallClock)
    assert isinstance(make_clock(LOGICAL, run_epoch_ns=EPOCH_NS, tick_hz=1.0), LogicalClock)
    with pytest.raises(ValueError, match="unknown clock"):
        make_clock("sundial", run_epoch_ns=EPOCH_NS, tick_hz=1.0)


@pytest.mark.parametrize("kind", [WALL, LOGICAL])
def test_both_clocks_satisfy_the_protocol(kind):
    assert isinstance(make_clock(kind, run_epoch_ns=EPOCH_NS, tick_hz=1.0), Clock)


@pytest.mark.parametrize("kind", [WALL, LOGICAL])
def test_tick_indices_start_at_zero(kind):
    clock = make_clock(kind, run_epoch_ns=EPOCH_NS, tick_hz=10.0)
    assert [clock.tick() for _ in range(3)] == [0, 1, 2]


def test_logical_timestamps_depend_only_on_the_tick(monkeypatch):
    clock = LogicalClock(run_epoch_ns=EPOCH_NS, tick_hz=4.0)
    monkeypatch.setattr("time.time_ns", lambda: EPOCH_NS + 999_999_999)
    clock.tick()
    clock.tick()
    # Tick 0 is t=0, so being on tick 1 at 4 Hz is 250 ms -- whatever the wall clock says
    # the delay between the two actually was.
    assert clock.timestamp_ns() == 250_000_000


def test_logical_timestamps_are_reproducible_across_wildly_different_wall_times(monkeypatch):
    def run(jitter_ns: int) -> list[int]:
        clock = LogicalClock(run_epoch_ns=EPOCH_NS, tick_hz=100.0)
        stamps = []
        for i in range(5):
            monkeypatch.setattr("time.time_ns", lambda i=i: EPOCH_NS + i * jitter_ns)
            clock.tick()
            stamps.append(clock.timestamp_ns())
        return stamps

    assert run(1_000) == run(750_000_000)


def test_wall_timestamps_track_the_wall_clock(monkeypatch):
    clock = WallClock(run_epoch_ns=EPOCH_NS, tick_hz=1.0)
    monkeypatch.setattr("time.time_ns", lambda: EPOCH_NS + 250_000_000)
    assert clock.timestamp_ns() == 250_000_000


def test_wall_timestamps_never_go_negative(monkeypatch):
    # Clock skew between development environments can put an Ecu marginally behind the
    # TestBench's epoch; a negative timestamp_ns would corrupt every downstream window.
    clock = WallClock(run_epoch_ns=EPOCH_NS, tick_hz=1.0)
    monkeypatch.setattr("time.time_ns", lambda: EPOCH_NS - 5_000_000)
    assert clock.timestamp_ns() == 0


@pytest.mark.parametrize("kind", [WALL, LOGICAL])
def test_tick_schedule_is_anchored_to_the_epoch_not_to_now(kind):
    clock = make_clock(kind, run_epoch_ns=EPOCH_NS, tick_hz=2.0)
    assert clock.next_tick_due_ns() == EPOCH_NS
    clock.tick()
    assert clock.next_tick_due_ns() == EPOCH_NS + 500_000_000
    clock.tick()
    # A slow tick does not push the schedule out: the third tick is still due at 1.0 s.
    assert clock.next_tick_due_ns() == EPOCH_NS + 1_000_000_000


@pytest.mark.parametrize("kind", [WALL, LOGICAL])
def test_sleep_returns_immediately_when_the_tick_is_overdue(monkeypatch, kind):
    clock = make_clock(kind, run_epoch_ns=EPOCH_NS, tick_hz=1.0)
    monkeypatch.setattr("time.time_ns", lambda: EPOCH_NS + 10_000_000_000)
    monkeypatch.setattr("time.sleep", lambda _s: pytest.fail("should not sleep when already overdue"))
    clock.sleep_until_next_tick()


@pytest.mark.parametrize("cls", [WallClock, LogicalClock])
def test_clocks_reject_a_non_positive_tick_rate(cls):
    with pytest.raises(ValueError, match="tick_hz must be > 0"):
        cls(run_epoch_ns=EPOCH_NS, tick_hz=0)
