"""ReplayableEcu: the fetch-pace-send engine shared by GeneratorEcu and ReplayEcu.

Both fetch rows from Databricks (blf_gold_signals/blf_silver_can, via bench.db) and
replay them onto one bus with the recorded relative pacing; they differ only in their
default ReplayConfig/FilterSpec (see the two subclasses below). Overrides Ecu.run()
entirely rather than using the base reactive drain loop, since this Ecu's pacing is
data-driven, not event-driven.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable

import pandas as pd

from bench.bus import Bus
from bench.clock import WALL
from bench.db import FilterSpec, ReplayConfig, fetch_replay_frames, to_frames
from bench.ecu import DEFAULT_POLL_TIMEOUT, Ecu
from bench.log import bind_run

FetchFn = Callable[[], pd.DataFrame]

SLEEP_SLICE = 0.05


def _sleep_between_frames(gap_s: float, should_stop: Callable[[], bool]) -> bool:
    """Wait out a recorded inter-frame gap, in slices so a stop lands mid-gap.

    Returns False if the run was stopped during the wait. Sleeping the whole gap in one
    call would make a recording with second-scale gaps take that long to react to stop().

    `duration` deliberately does not cut a gap short: it is checked once per frame, so a
    frame whose gap crosses the deadline is still sent, as it was before slicing.
    """
    end = time.monotonic() + gap_s
    while True:
        if should_stop():
            return False
        remaining = end - time.monotonic()
        if remaining <= 0:
            return True
        time.sleep(min(remaining, SLEEP_SLICE))


def _pass_count(loop: bool | int) -> int | None:
    """Normalize a `loop` argument to a pass count; None means replay forever."""
    if loop is True:
        return None
    if loop is False:
        return 1
    if not isinstance(loop, int) or loop < 1:
        raise ValueError(f"loop must be True, False, or a positive pass count; got {loop!r}")
    return loop


class ReplayableEcu(Ecu):
    """Fetches replay frames once, then sends them onto `ch` paced by their recorded
    relative timing.

    `fetch_fn` is injectable (defaulting to a real Databricks fetch via
    `bench.db.fetch_replay_frames`) so tests can drive this Ecu with a hand-built
    DataFrame instead of a live warehouse connection.

    `loop` controls how many times the fetched frames are replayed: `False` plays them
    once, `True` replays them over and over until the run's `duration` elapses or
    `should_stop()` fires, and a positive int stops after that many passes. So a short
    recording can drive a bench of any length. The rows are fetched once, not re-queried
    per pass.
    """

    def __init__(
        self,
        ch: Bus,
        *,
        config: ReplayConfig | None = None,
        fetch_fn: FetchFn | None = None,
        loop: bool | int = False,
        name: str,
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        super().__init__(name, clock=clock, tick_hz=tick_hz)
        self._ch = self.handle(ch)
        self._config = config or ReplayConfig()
        self._fetch_fn = fetch_fn or (lambda: fetch_replay_frames(self._config))
        self._passes = _pass_count(loop)

    def run(
        self,
        *,
        duration: float | None = None,
        on_tick: Callable[[Ecu], None] | None = None,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,  # unused: this Ecu's pacing is data-driven, not poll-driven
        should_stop: Callable[[], bool] = lambda: False,
    ) -> None:
        if on_tick is not None:
            raise TypeError(f"{type(self).__name__}.run() has no tick loop and does not support on_tick")
        if self.clock is None:
            raise RuntimeError(f"Ecu {self.name!r} was never bound to a run; add it via TestBench.add_ecu() first")
        bind_run(run_id=self.run_id, run_epoch_ns=self.clock.run_epoch_ns, clock=self.clock)
        try:
            df = self._fetch_fn()
            frames = to_frames(df, run_id=self.run_id, source_file=self.source_file, channel=self._ch.channel)
            deadline = None if duration is None else time.monotonic() + duration
            prev_ns: int | None = None
            # Each looped pass shifts timestamp_ns past the one before it, so a replayed
            # frame stays uniquely identifiable by (run_id, can_id, timestamp_ns) rather
            # than colliding with itself one pass earlier.
            # One cycle is the recording's span plus its final gap, so the wrap-around
            # keeps the recorded pacing instead of firing two frames at the same instant.
            cycle_ns = 0
            if frames:
                last_gap_ns = frames[-1].timestamp_ns - frames[-2].timestamp_ns if len(frames) > 1 else 0
                cycle_ns = max(1, frames[-1].timestamp_ns + last_gap_ns)
            offset_ns = 0
            passes_done = 0
            while frames:
                for frame in frames:
                    if should_stop():
                        return
                    if deadline is not None and time.monotonic() >= deadline:
                        return
                    timestamp_ns = frame.timestamp_ns + offset_ns
                    if prev_ns is not None:
                        gap_s = (timestamp_ns - prev_ns) / 1e9
                        if gap_s > 0 and not _sleep_between_frames(gap_s, should_stop):
                            return
                    prev_ns = timestamp_ns
                    # Re-stamp observed_ns to the actual instant this frame goes out --
                    # to_frames() left it at 0 as a placeholder (see its docstring).
                    shifted = dataclasses.replace(frame, timestamp_ns=timestamp_ns)
                    self._ch.send(shifted.forwarded(channel=shifted.channel))
                passes_done += 1
                if self._passes is not None and passes_done >= self._passes:
                    break
                offset_ns += cycle_ns
        finally:
            self.close()


class GeneratorEcu(ReplayableEcu):
    """Replays fetched traffic onto `ch`. Default `ReplayConfig.filter` excludes prior
    testbench uploads (`FilterSpec.exclude_source_prefix`), so it only ever replays real
    recorded logs -- never its own or another run's captured output.
    """

    def __init__(
        self,
        ch: Bus,
        *,
        config: ReplayConfig | None = None,
        fetch_fn: FetchFn | None = None,
        loop: bool | int = False,
        name: str = "generator",
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        super().__init__(ch, config=config, fetch_fn=fetch_fn, loop=loop, name=name, clock=clock, tick_hz=tick_hz)


class ReplayEcu(ReplayableEcu):
    """Same fetch-pace-send engine as GeneratorEcu, but defaults to *including* prior
    testbench uploads (`exclude_source_prefix=""`) -- e.g. to intentionally re-inject a
    previous run's captured output, or to drive downstream traffic for exercising a
    receiver-only leg without a live Gateway.
    """

    def __init__(
        self,
        ch: Bus,
        *,
        config: ReplayConfig | None = None,
        fetch_fn: FetchFn | None = None,
        loop: bool | int = False,
        name: str = "replay",
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        if config is None:
            config = ReplayConfig(filter=FilterSpec(exclude_source_prefix=""))
        super().__init__(ch, config=config, fetch_fn=fetch_fn, loop=loop, name=name, clock=clock, tick_hz=tick_hz)
