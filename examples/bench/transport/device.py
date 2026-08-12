from dataclasses import dataclass


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
