"""Bus: a bus segment's transport configuration.

The transport (loopback / lakebase / zerobus / zerobus_duplex / can) is a config choice
made once, here -- `bench.handle.BusHandle` and Ecu code downstream only ever see
`ChannelTx`/`ChannelRx`. This is what turns "Lakebase or Zerobus?" into a constructor argument
instead of something baked into an Ecu's code.

Transports are a registry (`register_transport()`), not an if/elif chain: adding one
means writing a `transport/<name>.py` with `open_tx(bus)`/`open_rx(bus, **kwargs)`
functions and calling `register_transport()` once -- see the built-in registrations at
the bottom of this module for the pattern, and any `transport/<name>.py` for what the
functions look like. Nothing here needs editing to add another transport.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass

from transport.device import CanDeviceConfig
from transport.device import open_rx as _can_open_rx
from transport.device import open_tx as _can_open_tx
from transport.lakebase import LakebaseConfig
from transport.lakebase import open_rx as _lakebase_open_rx
from transport.lakebase import open_tx as _lakebase_open_tx
from transport.loopback import open_rx as _loopback_open_rx
from transport.loopback import open_tx as _loopback_open_tx
from transport.zerobus import ZerobusConfig
from transport.zerobus import open_rx as _zerobus_open_rx
from transport.zerobus import open_tx as _zerobus_open_tx
from transport.zerobus_duplex import ZerobusDuplexConfig
from transport.zerobus_duplex import open_rx as _zerobus_duplex_open_rx
from transport.zerobus_duplex import open_tx as _zerobus_duplex_open_tx

from bench.channel import ChannelRx, ChannelTx

LOOPBACK = "loopback"
LAKEBASE = "lakebase"
ZEROBUS = "zerobus"
ZEROBUS_DUPLEX = "zerobus_duplex"
CAN = "can"

BusConfig = LakebaseConfig | ZerobusConfig | ZerobusDuplexConfig | CanDeviceConfig | None


@dataclass(frozen=True)
class _Transport:
    open_tx: Callable[[Bus], ChannelTx]
    open_rx: Callable[..., ChannelRx]


_TRANSPORTS: dict[str, _Transport] = {}


def register_transport(
    bus_type: str,
    *,
    open_tx: Callable[[Bus], ChannelTx],
    open_rx: Callable[..., ChannelRx],
) -> None:
    """Register a transport under `bus_type`, so `Bus(bus_type, ...)` dispatches to it.

    `open_tx(bus) -> ChannelTx` and `open_rx(bus, *, run_id, can_ids=None,
    message_types=None, source_file=None, run_epoch_ns=None) -> ChannelRx` do the actual
    work. Calling this again with an already-registered `bus_type` replaces it.
    """
    _TRANSPORTS[bus_type] = _Transport(open_tx=open_tx, open_rx=open_rx)


def _transport(bus_type: str) -> _Transport:
    try:
        return _TRANSPORTS[bus_type]
    except KeyError:
        raise ValueError(f"unknown bus_type {bus_type!r}; registered: {sorted(_TRANSPORTS)}") from None


class Bus:
    """One bus segment: a name, a transport type, that transport's config, and the BLF
    channel number `TestBench.add_bus()` assigns it.
    """

    def __init__(self, bus_type: str, config: BusConfig = None, *, name: str | None = None) -> None:
        _transport(bus_type)  # validates bus_type now, rather than at open_tx()/open_rx()
        self.name = name or f"bus_{uuid.uuid4().hex[:8]}"
        self.bus_type = bus_type
        self.config = config
        self.channel: int | None = None
        # Set by TestBench.bus(); the name is the transport's key (loopback segments,
        # Lakebase tables) and stays unique, while the slot is what the topology calls
        # this segment and is what a reader recognises.
        self.slot: str | None = None

    @property
    def label(self) -> str:
        """A human-facing name for this segment: its topology slot when it has one."""
        return self.slot or self.name

    def open_tx(self) -> ChannelTx:
        return _transport(self.bus_type).open_tx(self)

    def open_rx(
        self,
        *,
        run_id: str,
        can_ids: list[int] | None = None,
        message_types: list[str] | None = None,
        source_file: str | None = None,
        run_epoch_ns: int | None = None,
    ) -> ChannelRx:
        """Open the receive side. `source_file`/`run_epoch_ns` are only used by the CAN
        channel, which has to stamp them onto every received Frame itself (unlike the
        other transports, where the sender already stamped the Frame before it arrived).
        """
        return _transport(self.bus_type).open_rx(
            self,
            run_id=run_id,
            can_ids=can_ids,
            message_types=message_types,
            source_file=source_file,
            run_epoch_ns=run_epoch_ns,
        )

    def connect(self, *, run_id: str, **rx_kwargs) -> tuple[ChannelTx, ChannelRx]:
        """Open both sides eagerly. Prefer BusHandle (via Ecu.bus()), which opens each
        side lazily on first use instead.
        """
        return self.open_tx(), self.open_rx(run_id=run_id, **rx_kwargs)


register_transport(LOOPBACK, open_tx=_loopback_open_tx, open_rx=_loopback_open_rx)
register_transport(LAKEBASE, open_tx=_lakebase_open_tx, open_rx=_lakebase_open_rx)
register_transport(ZEROBUS, open_tx=_zerobus_open_tx, open_rx=_zerobus_open_rx)
register_transport(ZEROBUS_DUPLEX, open_tx=_zerobus_duplex_open_tx, open_rx=_zerobus_duplex_open_rx)
register_transport(CAN, open_tx=_can_open_tx, open_rx=_can_open_rx)


def Lakebase(config: LakebaseConfig, *, name: str | None = None) -> Bus:
    """A bus segment backed by a Lakebase (managed Postgres) table."""
    return Bus(LAKEBASE, config, name=name)


def Zerobus(config: ZerobusConfig, *, name: str | None = None) -> Bus:
    """A send-only bus segment backed by a Zerobus-ingested Delta table."""
    return Bus(ZEROBUS, config, name=name)


def ZerobusDuplex(config: ZerobusDuplexConfig, *, name: str | None = None) -> Bus:
    """A bus segment an Ecu can both send on and receive from over one Zerobus table:
    Zerobus Ingest out, polled catch-up queries back. Unlike `Zerobus`, needs a SQL
    warehouse.
    """
    return Bus(ZEROBUS_DUPLEX, config, name=name)


def CanDeviceBus(config: CanDeviceConfig | None = None, *, name: str | None = None) -> Bus:
    """A bus segment backed by a python-can device (udp_multicast by default)."""
    return Bus(CAN, config or CanDeviceConfig(), name=name)


def Loopback(*, name: str | None = None) -> Bus:
    """An in-process bus segment. No config -- the default transport for tests."""
    return Bus(LOOPBACK, None, name=name)
