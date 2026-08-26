"""TestBench: the orchestrator. Owns the buses and Ecus for one run, assigns each bus a
BLF channel number, binds a shared run_epoch_ns into every Ecu when the run starts, and
drives each Ecu's run() loop on its own thread -- modeled directly on
examples/testing/test_ecu/vecu/run_vecu_testbench.py's run_loopback(), which is the
same "thread per Ecu, in-process, no barrier" pattern.

`FilterSpec` (the Databricks-fetch filter) used to live in this module too, but it is a
`bench.db`/`bench.replay` concern, not an orchestration one -- see bench/db.py.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import TYPE_CHECKING

from bench.bus import Bus

if TYPE_CHECKING:
    from bench.ecu import Ecu


class TestBench:
    """Orchestrates one run: a set of buses (each a transport config) and a set of Ecus
    (each wired to one or more of those buses).
    """

    # The name matches pytest's default test-class pattern, so every test module that
    # imports it draws a "cannot collect test class" warning; this opts it out.
    __test__ = False

    def __init__(self, run_id: str | None = None) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self._buses: dict[str, Bus] = {}
        self._ecus: list[Ecu] = []
        self._next_channel = 1
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def add_bus(self, bus: Bus, *, channel: int | None = None) -> Bus:
        """Register `bus` and assign it a BLF channel number.

        `channel` picks the number explicitly; otherwise the next unused one is
        assigned. Raises ValueError if the resulting channel is already taken by
        another registered bus.
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
        run_epoch_ns = time.time_ns()
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
        """Start the run and block until every Ecu stops."""
        self.start(duration)
        self._join()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal every Ecu to stop and wait for them to finish."""
        self._stop.set()
        self._join(timeout=timeout)

    def _join(self, timeout: float | None = None) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)
