from typing import Iterator, List, Optional, Tuple, Union

class Can:
    channel: int
    id: int
    is_ext_id: bool
    dir: int
    rtr: bool
    dlc: int
    data: List[int]
    def __repr__(self) -> str: ...

class CanFd:
    channel: int
    id: int
    is_ext_id: bool
    dir: int
    rtr: bool
    fdf: bool
    brs: bool
    esi: bool
    dlc: int
    data: List[int]
    def __repr__(self) -> str: ...

class CanFd64:
    channel: int
    id: int
    is_ext_id: bool
    dir: int
    rtr: bool
    fdf: bool
    brs: bool
    esi: bool
    dlc: int
    data: List[int]
    def __repr__(self) -> str: ...

class Ethernet:
    channel: int
    dir: int
    src_addr: List[int]
    dst_addr: List[int]
    ether_type: int
    data: List[int]
    def __repr__(self) -> str: ...

class EthernetEx:
    channel: int
    dir: int
    src_addr: List[int]
    dst_addr: List[int]
    ether_type: int
    data: List[int]
    def __repr__(self) -> str: ...

class Mf4Signal:
    group: str
    name: str
    value: float
    unit: str
    def __repr__(self) -> str: ...

class BaseObject:
    timestamp_ns: int
    message: Union[Can, CanFd, CanFd64, Ethernet, EthernetEx, Mf4Signal, None]
    def __repr__(self) -> str: ...

class Reader:
    """Iterator over BaseObjects in a BLF file.

    Parameters
    ----------
    path:
        Path to the BLF file.
    types:
        Optional allowlist of message types to yield. Filtering happens in
        Rust before any Python object is allocated. Valid values (case-
        insensitive): ``"Can"``, ``"CanFd"``, ``"CanFd64"``, ``"Ethernet"``,
        ``"EthernetEx"``, ``"Mf4Signal"``, ``"Other"``. If omitted, all types are yielded.
    """

    def __init__(self, path: str, types: Optional[List[str]] = None) -> None: ...
    def __iter__(self) -> Iterator[BaseObject]: ...
    def __next__(self) -> BaseObject: ...

class CanSignalDb:
    """CAN signal database loaded from a CSV file.

    CSV format (header required)::

        message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset[,pdu_id]
        0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
        0x200,ContainerSig,0,8,Intel,false,1.0,0.0,0x10

    ``message_id`` accepts hex (``0x…``) or decimal.
    ``byte_order`` is ``Intel`` or ``Motorola`` (case-insensitive).
    ``is_signed`` accepts ``true``/``false`` or ``1``/``0``.
    The optional ninth column ``pdu_id`` marks the row as a container-frame signal;
    ``start_bit`` is then relative to the I-PDU payload after demultiplexing.
    """

    def __init__(self, path: str) -> None: ...
    def is_container(self, message_id: int) -> bool:
        """Return ``True`` if *message_id* is configured as a CAN-FD container frame."""
        ...

    def decode(self, message_id: int, data: bytes) -> List[Tuple[str, float]]:
        """Decode all matching signals for *message_id* from *data*.

        Returns a list of ``(signal_name, value)`` tuples.
        Signals whose bit range extends outside *data* are silently skipped.
        """
        ...

    def decode_container(
        self,
        message_id: int,
        data: bytes,
        long_header: bool = False,
    ) -> List[Tuple[str, float]]:
        """Demultiplex a CAN-FD container frame and decode all signals.

        *data* is the raw CAN frame payload. *long_header* selects between the
        4-byte-overhead short header (default) and the 8-byte-overhead long header.
        Returns a list of ``(signal_name, value)`` tuples for all matched I-PDUs.
        """
        ...

class SomeIpSignalDb:
    """SOME/IP signal database loaded from a CSV file.

    CSV format (header required)::

        service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
        0x0064,0x0001,Temperature,0,16,Intel,false,0.01,0.0

    ``service_id`` and ``method_id`` accept hex (``0x…``) or decimal.
    Signals are decoded from the SOME/IP application payload (bytes after the
    16-byte header).
    """

    def __init__(self, path: str) -> None: ...
    def decode(self, service_id: int, method_id: int, payload: bytes) -> List[Tuple[str, float]]:
        """Decode all matching signals for *(service_id, method_id)* from *payload*.

        Returns a list of ``(signal_name, value)`` tuples.
        Signals whose bit range extends outside *payload* are silently skipped.
        """
        ...
