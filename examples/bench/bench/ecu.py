"""Ecu: what a bench Ecu author writes against, plus three of the five reference roles
(GatewayEcu, ReceiverEcu, ProxyEcu). GeneratorEcu/ReplayEcu live in bench/replay.py --
their run loop is fetch-paced-send rather than reactive-drain, so they do not share
Ecu.run()'s loop the way these three do.

Deliberately simpler than examples/testing/vecu_sdk's Ecu: no manifest, no RunControl,
no barrier or heartbeat. A TestBench wires each Ecu directly to already-constructed Bus
objects at construction time, and TestBench.start() binds run_id/run_epoch_ns into every
registered Ecu at once (`_bind()`) -- see the plan's "explicitly deferred" table for what
this simplifies away.
"""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Callable

from bench.bus import Bus
from bench.clock import WallClock
from bench.frame import Frame, source_file_for
from bench.handle import BusHandle

DEFAULT_POLL_TIMEOUT = 0.05


class Ecu:
    """One Ecu participating in one TestBench run.

    Construct with the Bus objects it talks to (subclasses take them as named args,
    e.g. `GatewayEcu(ch1=bus1, ch2=bus2)`); `run_id`/the clock are not resolved until
    the owning TestBench calls `_bind()` from `start()`.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.run_id: str = ""
        self.source_file: str = ""
        self.clock: WallClock | None = None
        # Keyed by id(bus) rather than bus.name so passing the same Bus object twice
        # (e.g. GatewayEcu(ch1=bus, ch2=bus)) still resolves to one handle.
        self._buses: dict[int, BusHandle] = {}

    def _bind(self, *, run_id: str, run_epoch_ns: int) -> None:
        """Finalize construction once the owning TestBench knows the run's identity.
        Called exactly once, by TestBench.start().
        """
        self.run_id = run_id
        self.source_file = source_file_for(run_id)
        self.clock = WallClock(run_epoch_ns=run_epoch_ns)

    def handle(self, bus: Bus) -> BusHandle:
        """Return this Ecu's BusHandle for `bus`, creating it on first use."""
        key = id(bus)
        if key not in self._buses:
            self._buses[key] = BusHandle(bus, self)
        return self._buses[key]

    def run(
        self,
        *,
        duration: float | None = None,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,
        should_stop: Callable[[], bool] = lambda: False,
    ) -> None:
        """Drive this Ecu until it is time to stop, then tear it down.

        Each iteration drains every subscribed bus and dispatches to the handlers
        registered via `BusHandle.on()`. Stops on `duration` elapsing, `should_stop()`
        returning True, or KeyboardInterrupt.
        """
        if self.clock is None:
            raise RuntimeError(f"Ecu {self.name!r} was never bound to a run; add it via TestBench.add_ecu() first")
        for handle in self._buses.values():
            handle.subscribe()
        deadline = None if duration is None else time.monotonic() + duration
        try:
            while not should_stop():
                if deadline is not None and time.monotonic() >= deadline:
                    break
                for handle in self._buses.values():
                    handle.drain(poll_timeout)
                if not self._buses:
                    # Each drain() already blocks up to poll_timeout waiting for a
                    # frame, so this only matters when there is nothing to poll at all.
                    time.sleep(poll_timeout)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self) -> None:
        for handle in self._buses.values():
            handle.close()


class GatewayEcu(Ecu):
    """Forwards every frame from `ch1` to `ch2` (and, with `bidirectional=True`, the
    other way too). Purely reactive -- no polling loop of its own beyond Ecu.run()'s
    drain, matching the reference Gateway ECU pattern.
    """

    def __init__(self, ch1: Bus, ch2: Bus, *, bidirectional: bool = False, name: str = "gateway") -> None:
        super().__init__(name)
        h1, h2 = self.handle(ch1), self.handle(ch2)

        @h1.on()
        def _forward_1_to_2(frame: Frame) -> None:
            h2.forward(frame)
            print(f"[{self.name}] {ch1.name} -> {ch2.name}: {frame.data.hex()}", flush=True)

        if bidirectional:

            @h2.on()
            def _forward_2_to_1(frame: Frame) -> None:
                h1.forward(frame)
                print(f"[{self.name}] {ch2.name} -> {ch1.name}: {frame.data.hex()}", flush=True)


class ReceiverEcu(Ecu):
    """Captures every frame on `ch` and, if `zerobus` is given, uploads it.

    Zerobus is just another Bus (see bench.bus.Zerobus) rather than a special-cased
    upload parameter, so the upload leg is only a second BusHandle -- this is also what
    retires the separate Lakebase-to-Zerobus bridge process: this one Ecu both captures
    and uploads, in-process.
    """

    def __init__(self, ch: Bus, *, zerobus: Bus | None = None, name: str = "receiver") -> None:
        super().__init__(name)
        self._ch = self.handle(ch)
        self._zerobus = self.handle(zerobus) if zerobus is not None else None
        self.captured: list[Frame] = []

        @self._ch.on()
        def _capture(frame: Frame) -> None:
            self.captured.append(frame)
            label = f"can_id=0x{frame.can_id:X}" if frame.can_id is not None else f"ether_type={frame.ether_type}"
            print(f"[{self.name}] captured {label} data={frame.data.hex()}", flush=True)
            if self._zerobus is not None:
                self._zerobus.send(frame)


class ProxyEcu(Ecu):
    """Opens a UDP port and forwards every received datagram onto `ch`.

    Datagrams are JSON-encoded Frames (`Frame.to_json_obj()`/`from_json_obj()`), the
    same compact encoding the Lakebase channel already uses for NOTIFY payloads -- one
    wire format, not two. One-directional (UDP -> bus only); overrides run() entirely
    since there is a socket to poll instead of a bus to drain.
    """

    def __init__(self, ch: Bus, port: int, *, host: str = "0.0.0.0", name: str = "proxy") -> None:
        super().__init__(name)
        self._ch = self.handle(ch)
        self._host = host
        self._port = port
        self._sock: socket.socket | None = None

    def run(
        self,
        *,
        duration: float | None = None,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,
        should_stop: Callable[[], bool] = lambda: False,
    ) -> None:
        if self.clock is None:
            raise RuntimeError(f"Ecu {self.name!r} was never bound to a run; add it via TestBench.add_ecu() first")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((self._host, self._port))
        self._sock.settimeout(poll_timeout)
        deadline = None if duration is None else time.monotonic() + duration
        try:
            while not should_stop():
                if deadline is not None and time.monotonic() >= deadline:
                    break
                try:
                    data, _addr = self._sock.recvfrom(65535)
                except TimeoutError:
                    continue
                obj = json.loads(data.decode("utf-8"))
                frame = Frame.from_json_obj(obj, run_id=self.run_id, source_file=self.source_file)
                self._ch.forward(frame)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        super().close()
