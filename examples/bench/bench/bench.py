"""TestBench: the orchestrator. Owns the buses and Ecus for one run, assigns each bus a
BLF channel number, binds a shared run_epoch_ns into every Ecu when the run starts, and
drives each Ecu's run() loop on its own thread -- one thread per Ecu, in-process, no
barrier.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from bench.bus import Bus

if TYPE_CHECKING:
    from bench.ecu import Ecu

JOIN_SLICE = 0.1


class TestBench:
    """Orchestrates one run: a set of buses (each a transport config) and a set of Ecus
    (each wired to one or more of those buses).
    """

    # The name matches pytest's default test-class pattern, so every test module that
    # imports it draws a "cannot collect test class" warning; this opts it out.
    __test__ = False

    def __init__(self, run_id: str | None = None, buses: Mapping[str, Bus] | None = None) -> None:
        """`buses` replaces the transport a topology picked for one of its bus slots,
        keyed by the slot name that topology passes to `bus()` -- see that method.
        """
        self.run_id = run_id or str(uuid.uuid4())
        self._buses: dict[str, Bus] = {}
        self._ecus: list[Ecu] = []
        self._next_channel = 1
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._overrides = dict(buses or {})
        self._slots: list[str] = []
        self._started_at: float | None = None
        self._finished_at: float | None = None

    @property
    def buses(self) -> list[Bus]:
        """Every registered bus, in registration order."""
        return list(self._buses.values())

    @property
    def ecus(self) -> list[Ecu]:
        """Every registered Ecu, in registration order."""
        return list(self._ecus)

    @property
    def elapsed(self) -> float:
        """Seconds the run has been going, or lasted once it has finished. 0.0 before
        start().
        """
        if self._started_at is None:
            return 0.0
        end = time.monotonic() if self._finished_at is None else self._finished_at
        return end - self._started_at

    def bus(self, slot: str, default: Bus | Callable[[], Bus], *, channel: int | None = None) -> Bus:
        """Register the bus this topology calls `slot`: the caller's override when one
        was passed as `TestBench(buses=...)`, else `default`.

        This is what lets a topology's transports be chosen from outside it --
        `build(bus1=Loopback())` against a topology that would otherwise open a Lakebase
        table -- instead of being fixed in the topology's own code. Give `default` as a
        zero-argument callable whenever building it needs credentials, so an overridden
        slot never constructs (nor demands a config for) the transport it replaced.
        """
        self._slots.append(slot)
        override = self._overrides.get(slot)
        bus = override if override is not None else (default if isinstance(default, Bus) else default())
        bus.slot = slot
        return self.add_bus(bus, channel=channel)

    def add_bus(self, bus: Bus, *, channel: int | None = None) -> Bus:
        """Register `bus` and assign it a BLF channel number.

        `channel` picks the number explicitly; otherwise the next unused one is
        assigned. Raises ValueError if the resulting channel is already taken by
        another registered bus.

        For a bench built in place (a test, a one-off script) where the caller already
        holds the Bus it wants. A registered topology should call `bus()` instead: a bus
        added here carries no slot name, so `TestBench(buses=...)` cannot replace it.
        """
        if channel is not None:
            bus.channel = channel
        if bus.channel is None:
            bus.channel = self._next_channel
        taken = {b.channel for b in self._buses.values()}
        if bus.channel in taken:
            raise ValueError(f"bus channel {bus.channel} is already assigned (bus {bus.name!r})")
        self._next_channel = max(self._next_channel, bus.channel + 1)
        self._buses[bus.name] = bus
        return bus

    def add_ecu(self, ecu: Ecu) -> Ecu:
        """Register `ecu`. Its buses resolve lazily; run context is bound in start()."""
        self._ecus.append(ecu)
        return ecu

    def start(self, duration: float | None = None) -> None:
        """Bind a shared run_epoch_ns into every registered Ecu and start each one on
        its own thread. Non-blocking -- call run() instead to block until they finish,
        or stop() to end the run early.
        """
        # A misspelled override would otherwise run the topology unchanged, silently.
        unknown = sorted(set(self._overrides) - set(self._slots))
        if unknown:
            raise ValueError(f"no such bus slot: {', '.join(unknown)}; this topology declares {self._slots}")
        run_epoch_ns = time.time_ns()
        self._started_at = time.monotonic()
        self._finished_at = None
        for ecu in self._ecus:
            ecu._bind(run_id=self.run_id, run_epoch_ns=run_epoch_ns)
        # No role-aware start ordering: every Ecu's channel catches up on connect
        # (Lakebase) or replays its backlog on attach (loopback), so which thread wins
        # the race to start first does not drop frames.
        self._threads = [
            threading.Thread(
                target=ecu.run,
                kwargs={"duration": duration, "should_stop": self._stop.is_set},
                name=ecu.name,
            )
            for ecu in self._ecus
        ]
        for thread in self._threads:
            thread.start()

    def run(self, duration: float | None = None) -> None:
        """Start the run and block until every Ecu stops.

        Ctrl-C is turned into an ordinary stop() rather than an abort, so every Ecu still
        runs its close() teardown and flushes whatever its transport has buffered.
        """
        self.start(duration)
        try:
            self._join()
        except KeyboardInterrupt:
            print("[bench] interrupted -- stopping every Ecu", flush=True)
            self.stop()
        finally:
            if self._finished_at is None:
                self._finished_at = time.monotonic()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal every Ecu to stop and wait for them to finish."""
        self._stop.set()
        self._join(timeout=timeout)
        self._finished_at = time.monotonic()
        stragglers = [t.name for t in self._threads if t.is_alive()]
        if stragglers:
            # The threads are deliberately not daemons -- an Ecu killed mid-close() loses
            # whatever its transport had buffered -- so say who is holding up the exit.
            print(f"[bench] still running after {timeout}s: {', '.join(stragglers)}", flush=True)

    def _join(self, timeout: float | None = None) -> None:
        # Always a timed join: a bare Thread.join() blocks the main thread in a way that
        # defers KeyboardInterrupt until the thread exits on its own, which for an
        # unbounded run means Ctrl-C never reaches _stop at all.
        deadline = None if timeout is None else time.monotonic() + timeout
        for thread in self._threads:
            while thread.is_alive():
                if deadline is not None and time.monotonic() >= deadline:
                    return
                thread.join(JOIN_SLICE)
