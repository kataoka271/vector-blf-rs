"""TestBench: the orchestrator. Owns the buses and Ecus for one run, assigns each bus a
BLF channel number, binds a shared run_epoch_ns into every Ecu when the run starts, and
drives each Ecu's run() loop on its own thread -- modeled directly on
examples/testing/test_ecu/vecu/run_vecu_testbench.py's run_loopback(), which is the
same "thread per Ecu, in-process, no barrier" pattern.
"""

from __future__ import annotations

import dataclasses
import threading
import time
import uuid
from typing import TYPE_CHECKING

from bench.bus import Bus

if TYPE_CHECKING:
    from bench.ecu import Ecu


@dataclasses.dataclass
class FilterSpec:
    """Selects which blf_gold_signals events feed a replay."""

    source_files: list[str] | None = None
    t_start_s: float | None = None
    t_end_s: float | None = None
    channels: list[int] | None = None
    signal_source: list[str] = dataclasses.field(default_factory=lambda: ["CAN"])
    exclude_dirs: list[str] = dataclasses.field(default_factory=lambda: ["Tx"])
    exclude_source_prefix: str = "testbench/"
    extra_where: str | None = None

    def build_where(self) -> tuple[str, list]:
        """Build a `WHERE`-clause predicate (without the `WHERE` keyword) and its `?`-placeholder parameters, in placeholder order."""
        clauses: list[str] = []
        params: list = []

        if self.source_files:
            clauses.append("_source_file IN (" + ", ".join(["?"] * len(self.source_files)) + ")")
            params += list(self.source_files)

        if self.exclude_source_prefix:
            clauses.append("_source_file NOT LIKE ?")
            params.append(f"{self.exclude_source_prefix}%")

        if self.t_start_s is not None:
            clauses.append("timestamp_s >= ?")
            params.append(self.t_start_s)

        if self.t_end_s is not None:
            clauses.append("timestamp_s <= ?")
            params.append(self.t_end_s)

        if self.channels:
            clauses.append("channel IN (" + ", ".join(["?"] * len(self.channels)) + ")")
            params += list(self.channels)

        if self.signal_source:
            clauses.append("signal_source IN (" + ", ".join(["?"] * len(self.signal_source)) + ")")
            params += list(self.signal_source)

        if self.exclude_dirs:
            clauses.append("dir NOT IN (" + ", ".join(["?"] * len(self.exclude_dirs)) + ")")
            params += list(self.exclude_dirs)

        if self.extra_where:
            clauses.append(f"({self.extra_where})")

        if not clauses:
            return "1 = 1", []
        return " AND ".join(clauses), params


class TestBench:
    """Orchestrates one run: a set of buses (each a transport config) and a set of Ecus
    (each wired to one or more of those buses).
    """

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
