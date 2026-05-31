from typing import Dict, Iterator, List, Optional, Tuple, Union

class Can:
    channel: int
    id: int
    is_ext_id: bool
    dir: int
    rtr: bool
    dlc: int
    data: bytes
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
    data: bytes
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
    data: bytes
    def __repr__(self) -> str: ...

class Ethernet:
    channel: int
    dir: int
    src_addr: bytes
    dst_addr: bytes
    ether_type: int
    data: bytes
    def __repr__(self) -> str: ...

class EthernetEx:
    channel: int
    dir: int
    src_addr: bytes
    dst_addr: bytes
    ether_type: int
    data: bytes
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
    """Iterator over BaseObjects in a BLF or MF4 file.

    Parameters
    ----------
    path:
        Path to the BLF or MF4 file (.blf, .mf4, .mdf).
    types:
        Optional allowlist of message types to yield. Filtering happens in
        Rust before any Python object is allocated. Valid values (case-
        insensitive): ``"Can"``, ``"CanFd"``, ``"CanFd64"``, ``"Ethernet"``,
        ``"EthernetEx"``, ``"Mf4Signal"``, ``"Other"``. If omitted, all types are yielded.
    """

    def __init__(self, path: str, types: Optional[List[str]] = None) -> None: ...
    def __iter__(self) -> Iterator[BaseObject]: ...
    def __next__(self) -> BaseObject: ...
    def read_batch(self, n: int = 50000) -> Optional[Dict[str, List]]:
        """Read up to n records and return a column-oriented dict ready for pd.DataFrame.

        Returns None at EOF. Columns: timestamp_ns, message_type, channel, can_id,
        is_ext_id, dir, rtr, dlc, data, fdf, brs, esi, src_addr, dst_addr,
        ether_type, mf4_group, mf4_name, mf4_value, mf4_unit.
        """
        ...

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
        """Return True if message_id is configured as a CAN-FD container frame."""
        ...

    def decode(self, message_id: int, data: bytes) -> List[Tuple[str, float]]:
        """Decode all matching signals for message_id from data.

        Returns a list of (signal_name, value) tuples.
        Signals whose bit range extends outside data are silently skipped.
        """
        ...

    def decode_container(
        self,
        message_id: int,
        data: bytes,
        long_header: bool = False,
    ) -> List[Tuple[str, float]]:
        """Demultiplex a CAN-FD container frame and decode all signals.

        data is the raw CAN frame payload. long_header selects between the
        4-byte-overhead short header (default) and the 8-byte-overhead long header.
        Returns a list of (signal_name, value) tuples for all matched I-PDUs.
        """
        ...

    def extract_container_pdus(
        self,
        message_id: int,
        data: bytes,
        long_header: bool = False,
    ) -> List[Tuple[int, bytes]]:
        """Demultiplex a CAN-FD container frame into raw (pdu_id, pdu_payload) pairs.

        Returns an empty list if message_id is not a known container frame.
        long_header selects the 8-byte-overhead long header (default: 4-byte short).
        """
        ...

    def decode_pdu(self, can_id: int, pdu_id: int, data: bytes) -> List[Tuple[str, float]]:
        """Decode signals for a single I-PDU previously extracted from a container frame.

        can_id is the parent container CAN ID; pdu_id identifies the I-PDU within it.
        Returns a list of (signal_name, value) tuples.
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
        """Decode all matching signals for (service_id, method_id) from payload.

        Returns a list of (signal_name, value) tuples.
        Signals whose bit range extends outside payload are silently skipped.
        """
        ...

class IsoTpReassembler:
    """Stateful ISO-TP (ISO 15765-2) reassembler for a single sender/receiver conversation.

    Create one instance per (channel, can_id) pair and call push() for
    every CAN frame in timestamp order. Handles standard and extended
    (CAN-FD) Single-Frame and First-Frame formats automatically.
    """

    def __init__(self) -> None: ...
    def push(self, data: bytes) -> Optional[Tuple[str, int, str, Optional[int], Optional[str], bytes]]:
        """Feed one CAN frame payload.

        Returns (uds_type, service_id, service_name, nrc, nrc_name, data)
        when a complete UDS PDU is assembled, or None if more frames are needed.
        uds_type is one of "Request", "PositiveResponse", or "NegativeResponse".
        nrc and nrc_name are None unless the PDU is a "NegativeResponse".
        """
        ...

def parse_someip_udp(ether_type: int, eth_payload: bytes) -> List[Dict[str, Union[str, int, bytes]]]:
    """Parse SOME/IP messages from an Ethernet frame payload.

    Supports IPv4 and IPv6 outer headers, UDP transport, and AUTOSAR Container
    PDU Transport (multiple back-to-back SOME/IP PDUs per UDP datagram).
    SOME/IP-SD (service_id=0xFFFF) frames are silently skipped.

    Returns a list of dicts with keys: src_ip, dst_ip, udp_src_port, udp_dst_port,
    someip_service_id, someip_method_id, someip_length, someip_client_id,
    someip_session_id, someip_protocol_version, someip_interface_version,
    someip_msg_type, someip_return_code, payload.
    """
    ...

def parse_doip_diag(ether_type: int, eth_payload: bytes) -> List[Tuple[int, int, bytes]]:
    """Parse DoIP DiagMessages from an Ethernet frame payload.

    Strips the IP and TCP headers (port 13400), then iterates over back-to-back
    DoIP frames in the TCP segment. Each DiagMessage yields a
    (src_addr, target_addr, uds_payload) tuple where addresses are DoIP
    logical addresses (integers) and uds_payload is the raw UDS bytes.

    Returns an empty list if the frame is not TCP/13400 or contains no DiagMessages.
    """
    ...

def parse_uds(data: bytes) -> Optional[Tuple[str, int, str, Optional[int], Optional[str], bytes]]:
    """Parse a raw UDS payload (ISO 14229-1).

    Returns (uds_type, service_id, service_name, nrc, nrc_name, data)
    or None if the payload is empty or malformed.

    uds_type is one of "Request", "PositiveResponse", or "NegativeResponse".
    nrc and nrc_name are None unless the PDU is a "NegativeResponse".
    """
    ...

def parse_eth_payload_signals(ether_type: int, eth_payload: bytes) -> List[Dict[str, Union[str, float, None]]]:
    """Parse IP/TCP/UDP header fields from an Ethernet frame payload as named signals.

    Returns a list of dicts with keys: signal_name, signal_value, signal_str.
    IPv4 and IPv6 are both supported. Returns an empty list for non-IP frames
    or frames that cannot be parsed.

    Signal names emitted: ip.protocol, ip.ttl (IPv4) / ip.hop_limit (IPv6),
    ip.total_len (IPv4 only), ip.src, ip.dst, tcp.src_port, tcp.dst_port,
    tcp.flags, udp.src_port, udp.dst_port, udp.payload_bytes.
    """
    ...
