"""CAN device channel: python-can (udp_multicast by default, needs no hardware), for
bridging to an external test-ECU process or real hardware.

`python-can` is imported lazily inside `_open_can_bus()`/`CanDeviceTx.send()`, not at
module level, so constructing a `CanDeviceConfig` never requires it to be installed.
"""

from __future__ import annotations

from dataclasses import dataclass

from bench.frame import CAN, CAN_FD, DIR_RX, Frame


@dataclass
class CanDeviceConfig:
    interface: str = "udp_multicast"
    channel: str = "239.42.0.1"
    port: int | None = 43113
    bitrate: int | None = None
    receive_own_messages: bool = False
    fd: bool | None = None


@dataclass
class EthDeviceConfig:
    interface: str = "udp_multicast"
    channel: str = "239.42.0.1"
    port: int | None = 43113
    bitrate: int | None = None
    receive_own_messages: bool = False
    fd: bool | None = None


def _open_can_bus(config: CanDeviceConfig):
    import can

    kwargs: dict = {
        "interface": config.interface,
        "channel": config.channel,
        "receive_own_messages": config.receive_own_messages,
    }
    if config.bitrate is not None:
        kwargs["bitrate"] = config.bitrate
    if config.port is not None:
        kwargs["port"] = config.port
    if config.fd is not None:
        kwargs["fd"] = config.fd
    return can.interface.Bus(**kwargs)


class CanDeviceTx:
    """Transmit side of a CAN device channel. CAN-only: raises on an Ethernet frame."""

    def __init__(self, *, config: CanDeviceConfig) -> None:
        self._bus = _open_can_bus(config)

    def send(self, frame: Frame) -> None:
        import can

        if not frame.is_can or frame.can_id is None:
            raise ValueError(f"CAN device channel cannot send message_type={frame.message_type!r}")
        # python-can requires dlc == len(data) for non-remote frames; a CAN-FD dlc is a
        # code (e.g. 15 -> 64 bytes) rather than a byte count, so use the byte length
        # instead of the frame's own dlc field when is_fd (see transport can_io history).
        dlc = len(frame.data) if frame.is_fd else (frame.dlc if frame.dlc is not None else len(frame.data))
        self._bus.send(
            can.Message(
                arbitration_id=frame.can_id,
                is_extended_id=bool(frame.is_ext_id),
                is_remote_frame=bool(frame.rtr),
                is_fd=bool(frame.is_fd),
                dlc=dlc,
                data=frame.data,
            )
        )

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._bus.shutdown()


class CanDeviceRx:
    """Receive side of a CAN device channel. Stamps received messages into Frames using
    `channel` as the BLF channel number (distinct from `config.channel`, which is the
    interface's own multicast address / channel name).
    """

    def __init__(
        self,
        *,
        config: CanDeviceConfig,
        run_id: str,
        source_file: str,
        channel: int,
        run_epoch_ns: int,
    ) -> None:
        import can

        self._bus = _open_can_bus(config)
        self._reader = can.BufferedReader()
        self._notifier = can.Notifier(self._bus, [self._reader])
        self._run_id = run_id
        self._source_file = source_file
        self._channel = channel
        self._run_epoch_ns = run_epoch_ns

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        msg = self._reader.get_message(timeout=timeout)
        while msg is not None:
            frames.append(self._to_frame(msg))
            msg = self._reader.get_message(timeout=0)
        return frames

    def _to_frame(self, msg) -> Frame:
        # msg.timestamp is float seconds since the epoch; the udp_multicast backend
        # fills it from the kernel's SO_TIMESTAMPNS control message. Only accurate to
        # ~240ns at present-day epoch magnitudes (float64 resolution), far below CAN
        # frame spacing, so it does not affect ordering.
        observed_ns = int(msg.timestamp * 1e9)
        return Frame(
            run_id=self._run_id,
            source_file=self._source_file,
            message_type=CAN_FD if msg.is_fd else CAN,
            timestamp_ns=max(0, observed_ns - self._run_epoch_ns),
            observed_ns=observed_ns,
            channel=self._channel,
            dir=DIR_RX,
            data=bytes(msg.data),
            can_id=int(msg.arbitration_id),
            is_ext_id=bool(msg.is_extended_id),
            rtr=bool(msg.is_remote_frame),
            dlc=int(msg.dlc),
            is_fd=bool(msg.is_fd),
        )

    def close(self) -> None:
        self._notifier.stop()
        self._reader.stop()
        self._bus.shutdown()
