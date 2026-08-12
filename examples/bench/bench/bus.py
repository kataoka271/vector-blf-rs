"""Bus: a bus segment's transport configuration.

The transport (loopback / lakebase / zerobus / can) is a config choice made once, here
-- `bench.handle.BusHandle` and Ecu code downstream only ever see `ChannelTx`/
`ChannelRx`. This is what turns "Lakebase or Zerobus?" into a constructor argument
instead of something baked into an Ecu's code.
"""

from __future__ import annotations

import uuid

from transport.device import CanDeviceConfig
from transport.lakebase import LakebaseConfig
from transport.zerobus import ZerobusConfig

from bench.channel import (
    CanDeviceRx,
    CanDeviceTx,
    ChannelRx,
    ChannelTx,
    LakebaseRx,
    LakebaseTx,
    LoopbackRx,
    LoopbackTx,
    ZerobusRx,
    ZerobusTx,
)

LOOPBACK = "loopback"
LAKEBASE = "lakebase"
ZEROBUS = "zerobus"
CAN = "can"

BusConfig = LakebaseConfig | ZerobusConfig | CanDeviceConfig | None


class Bus:
    """One bus segment: a name, a transport type, that transport's config, and the BLF
    channel number `TestBench.add_bus()` assigns it.
    """

    def __init__(self, bus_type: str, config: BusConfig = None, *, name: str | None = None) -> None:
        self.name = name or f"bus_{uuid.uuid4().hex[:8]}"
        self.bus_type = bus_type
        self.config = config
        self.channel: int | None = None
        if bus_type == LAKEBASE:
            assert isinstance(config, LakebaseConfig)
            # An unset table means "one Postgres table per Bus instance" -- resolving it
            # here means two independently-constructed Lakebase() buses never silently
            # share a table just because neither caller named one.
            config.table = config.table or self.name

    def open_tx(self) -> ChannelTx:
        if self.bus_type == LOOPBACK:
            return LoopbackTx(table=self.name)
        if self.bus_type == LAKEBASE:
            assert isinstance(self.config, LakebaseConfig)
            return LakebaseTx(config=self.config)
        if self.bus_type == ZEROBUS:
            assert isinstance(self.config, ZerobusConfig)
            return ZerobusTx(config=self.config)
        if self.bus_type == CAN:
            assert isinstance(self.config, CanDeviceConfig)
            return CanDeviceTx(config=self.config)
        raise ValueError(f"unknown bus_type {self.bus_type!r}")

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
        if self.bus_type == LOOPBACK:
            return LoopbackRx(table=self.name, run_id=run_id, can_ids=can_ids, message_types=message_types)
        if self.bus_type == LAKEBASE:
            assert isinstance(self.config, LakebaseConfig)
            return LakebaseRx(config=self.config, run_id=run_id, can_ids=can_ids, message_types=message_types)
        if self.bus_type == ZEROBUS:
            return ZerobusRx()
        if self.bus_type == CAN:
            assert isinstance(self.config, CanDeviceConfig)
            if self.channel is None:
                raise RuntimeError(f"bus {self.name!r} has no channel assigned; add it via TestBench.add_bus() first")
            if source_file is None or run_epoch_ns is None:
                raise ValueError("CAN device rx needs source_file and run_epoch_ns")
            return CanDeviceRx(
                config=self.config,
                run_id=run_id,
                source_file=source_file,
                channel=self.channel,
                run_epoch_ns=run_epoch_ns,
            )
        raise ValueError(f"unknown bus_type {self.bus_type!r}")

    def connect(self, *, run_id: str, **rx_kwargs) -> tuple[ChannelTx, ChannelRx]:
        """Open both sides eagerly. Prefer BusHandle (via Ecu.bus()), which opens each
        side lazily on first use instead.
        """
        return self.open_tx(), self.open_rx(run_id=run_id, **rx_kwargs)


def Lakebase(config: LakebaseConfig | None = None, *, name: str | None = None) -> Bus:
    """A bus segment backed by a Lakebase (managed Postgres) table."""
    return Bus(LAKEBASE, config or LakebaseConfig(), name=name)


def Zerobus(config: ZerobusConfig | None = None, *, name: str | None = None) -> Bus:
    """A send-only bus segment backed by a Zerobus-ingested Delta table."""
    return Bus(ZEROBUS, config or ZerobusConfig(), name=name)


def CanDeviceBus(config: CanDeviceConfig | None = None, *, name: str | None = None) -> Bus:
    """A bus segment backed by a python-can device (udp_multicast by default)."""
    return Bus(CAN, config or CanDeviceConfig(), name=name)


def Loopback(*, name: str | None = None) -> Bus:
    """An in-process bus segment. No config -- the default transport for tests."""
    return Bus(LOOPBACK, None, name=name)
