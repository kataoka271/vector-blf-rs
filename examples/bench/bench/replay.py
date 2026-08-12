"""ReplayableEcu: the fetch-pace-send engine shared by GeneratorEcu and ReplayEcu.

Both fetch rows from Databricks (blf_gold_signals/blf_silver_can, via bench.db) and
replay them onto one bus with the recorded relative pacing; they differ only in their
default ReplayConfig/FilterSpec (see the two subclasses below). Overrides Ecu.run()
entirely rather than using the base reactive drain loop, since this Ecu's pacing is
data-driven, not event-driven.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pandas as pd

from bench.bench import FilterSpec
from bench.bus import Bus
from bench.clock import WALL
from bench.db import ReplayConfig, fetch_replay_frames, to_frames
from bench.ecu import DEFAULT_POLL_TIMEOUT, Ecu

FetchFn = Callable[[], pd.DataFrame]


class ReplayableEcu(Ecu):
    """Fetches replay frames once, then sends them onto `ch` paced by their recorded
    relative timing.

    `fetch_fn` is injectable (defaulting to a real Databricks fetch via
    `bench.db.fetch_replay_frames`) so tests can drive this Ecu with a hand-built
    DataFrame instead of a live warehouse connection.
    """

    def __init__(
        self,
        ch: Bus,
        *,
        config: ReplayConfig | None = None,
        fetch_fn: FetchFn | None = None,
        name: str,
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        super().__init__(name, clock=clock, tick_hz=tick_hz)
        self._ch = self.handle(ch)
        self._config = config or ReplayConfig()
        self._fetch_fn = fetch_fn or (lambda: fetch_replay_frames(self._config))

    def run(
        self,
        *,
        duration: float | None = None,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,  # unused: this Ecu's pacing is data-driven, not poll-driven
        should_stop: Callable[[], bool] = lambda: False,
    ) -> None:
        if self.clock is None:
            raise RuntimeError(f"Ecu {self.name!r} was never bound to a run; add it via TestBench.add_ecu() first")
        try:
            df = self._fetch_fn()
            frames = to_frames(df, run_id=self.run_id, source_file=self.source_file, channel=self._ch.channel)
            deadline = None if duration is None else time.monotonic() + duration
            prev_ns: int | None = None
            for frame in frames:
                if should_stop():
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    break
                if prev_ns is not None:
                    gap_s = (frame.timestamp_ns - prev_ns) / 1e9
                    if gap_s > 0:
                        time.sleep(gap_s)
                prev_ns = frame.timestamp_ns
                # Re-stamp observed_ns to the actual instant this frame goes out --
                # to_frames() left it at 0 as a placeholder (see its docstring).
                self._ch.send(frame.forwarded(channel=frame.channel))
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
        name: str = "generator",
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        super().__init__(ch, config=config, fetch_fn=fetch_fn, name=name, clock=clock, tick_hz=tick_hz)


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
        name: str = "replay",
        clock: str = WALL,
        tick_hz: float = 1.0,
    ) -> None:
        if config is None:
            config = ReplayConfig(filter=FilterSpec(exclude_source_prefix=""))
        super().__init__(ch, config=config, fetch_fn=fetch_fn, name=name, clock=clock, tick_hz=tick_hz)
