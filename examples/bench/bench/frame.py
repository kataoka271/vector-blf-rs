"""The unified frame: one record shape for CAN, CAN-FD and Ethernet traffic, carried by
every channel and logged through every sink.

`message_type` is the discriminator and uses exactly the vocabulary blf_bronze already
speaks ("CAN" / "CAN_FD" / "ETH" / "ETH_EX"), so a frame maps onto a bronze row with no
translation.

Two encodings of the same record:

* `to_row()` / `from_row()` -- a plain dict keyed by FRAME_COLUMNS. This is the Postgres
  column tuple, and also the protobuf constructor kwargs for Zerobus (protobuf's generated
  __init__ skips None values, so unset variant fields simply stay unset).
* `to_json_obj()` / `from_json_obj()` -- the compact form carried inside a Lakebase NOTIFY
  payload or a ProxyEcu UDP datagram. Keys are abbreviated and unset fields omitted.
  `data`/`src_addr`/`dst_addr` are base64, not hex (4/3 expansion instead of 2x).
  `run_id`/`source_file` are not included -- they are constant per run and passed
  separately by the caller.
"""

from __future__ import annotations

import base64
import dataclasses
import time
from typing import Any

CAN = "CAN"
CAN_FD = "CAN_FD"
ETH = "ETH"
ETH_EX = "ETH_EX"

CAN_TYPES = (CAN, CAN_FD)
ETH_TYPES = (ETH, ETH_EX)
MESSAGE_TYPES = CAN_TYPES + ETH_TYPES

# Column order for the Postgres bus tables and the Zerobus protobuf record. Shared by
# both so a frame never needs reshaping between the wire and the log.
FRAME_COLUMNS = (
    "run_id",
    "source_file",
    "message_type",
    "timestamp_ns",
    "observed_ns",
    "channel",
    "dir",
    "data",
    # CAN / CAN-FD
    "can_id",
    "is_ext_id",
    "rtr",
    "dlc",
    "is_fd",
    # Ethernet
    "src_addr",
    "dst_addr",
    "ether_type",
    # Ethernet, 802.1Q / 802.1AD tagged only (message_type == ETH_EX)
    "vlan_tpid",
    "vlan_cos",
    "vlan_id",
)

# Abbreviated JSON keys, mapped to FRAME_COLUMNS names. run_id and source_file are
# absent by design (passed alongside, not per-frame).
_JSON_KEYS = {
    "message_type": "mt",
    "timestamp_ns": "ts",
    "observed_ns": "ob",
    "channel": "ch",
    "dir": "dir",
    "data": "data",
    "can_id": "id",
    "is_ext_id": "ext",
    "rtr": "rtr",
    "dlc": "dlc",
    "is_fd": "fd",
    "src_addr": "src",
    "dst_addr": "dst",
    "ether_type": "et",
    "vlan_tpid": "tpid",
    "vlan_cos": "cos",
    "vlan_id": "vid",
}
_JSON_KEYS_INVERSE = {v: k for k, v in _JSON_KEYS.items()}

# Fields carried as base64 strings in JSON rather than as-is.
_BINARY_FIELDS = ("data", "src_addr", "dst_addr")

# The two fields `forwarded()` re-stamps at each hop, and so the two Frame.hop_key()
# leaves out.
_HOP_VARYING = ("channel", "observed_ns")

DIR_TX = 0
DIR_RX = 1
DIR_TX_RQ = 2


@dataclasses.dataclass(frozen=True)
class Frame:
    """One CAN, CAN-FD or Ethernet frame on a bus segment.

    `timestamp_ns` is run-relative (nanoseconds since the run's t=0), matching
    blf_bronze's log-relative convention. `observed_ns` is absolute epoch nanoseconds and
    is re-stamped at each hop, which is what makes per-hop latency measurable in SQL
    afterwards.

    `channel` identifies the bus segment, not a physical CAN controller: each segment of
    the simulated topology gets its own number so both ends of a forwarding ECU land in
    the same log distinguishable by channel.
    """

    run_id: str
    source_file: str
    message_type: str
    timestamp_ns: int
    observed_ns: int
    channel: int
    dir: int = DIR_RX
    data: bytes = b""
    can_id: int | None = None
    is_ext_id: bool | None = None
    rtr: bool | None = None
    dlc: int | None = None
    is_fd: bool | None = None
    src_addr: bytes | None = None
    dst_addr: bytes | None = None
    ether_type: int | None = None
    vlan_tpid: int | None = None
    vlan_cos: int | None = None
    vlan_id: int | None = None

    def __post_init__(self) -> None:
        if self.message_type not in MESSAGE_TYPES:
            raise ValueError(f"message_type must be one of {MESSAGE_TYPES}, got {self.message_type!r}")

    @property
    def is_can(self) -> bool:
        return self.message_type in CAN_TYPES

    @property
    def is_eth(self) -> bool:
        return self.message_type in ETH_TYPES

    def to_row(self) -> dict[str, Any]:
        """Return the FRAME_COLUMNS-keyed dict used as Postgres parameters and as the
        Zerobus protobuf constructor kwargs.
        """
        return {name: getattr(self, name) for name in FRAME_COLUMNS}

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Frame:
        """Rebuild a Frame from a FRAME_COLUMNS-keyed dict (a Postgres row, a Zerobus
        poll result, ...). Extra keys such as the bus table's `id` are ignored.
        """
        kwargs = {name: row[name] for name in FRAME_COLUMNS if name in row}
        data = kwargs.get("data")
        if data is not None and not isinstance(data, bytes):
            # psycopg hands BYTEA back as memoryview.
            kwargs["data"] = bytes(data)
        for name in ("src_addr", "dst_addr"):
            value = kwargs.get(name)
            if value is not None and not isinstance(value, bytes):
                kwargs[name] = bytes(value)
        return cls(**kwargs)

    def to_json_obj(self) -> dict[str, Any]:
        """Return the compact representation: abbreviated keys, unset fields omitted,
        binary fields base64-encoded. run_id/source_file are excluded (see the module
        docstring).
        """
        obj: dict[str, Any] = {}
        for name, key in _JSON_KEYS.items():
            value = getattr(self, name)
            if value is None:
                continue
            if name in _BINARY_FIELDS:
                value = base64.b64encode(value).decode("ascii")
            obj[key] = value
        return obj

    @classmethod
    def from_json_obj(cls, obj: dict[str, Any], *, run_id: str, source_file: str) -> Frame:
        """Inverse of to_json_obj; `run_id`/`source_file` come from the caller (a batch
        envelope, a run context, ...).
        """
        kwargs: dict[str, Any] = {"run_id": run_id, "source_file": source_file}
        for key, value in obj.items():
            name = _JSON_KEYS_INVERSE.get(key)
            if name is None:
                # Forward compatibility: ignore keys a newer producer added.
                continue
            kwargs[name] = base64.b64decode(value) if name in _BINARY_FIELDS else value
        return cls(**kwargs)

    def forwarded(self, *, channel: int, observed_ns: int | None = None) -> Frame:
        """Return a copy re-stamped for the next hop: new `channel` and a fresh
        `observed_ns`, with `timestamp_ns` and the payload preserved.

        Preserving timestamp_ns is what makes (run_id, can_id, timestamp_ns) a valid
        correlation key across hops for per-hop latency queries, so a forwarding ECU must
        go through this rather than building a new frame.
        """
        return dataclasses.replace(
            self,
            channel=channel,
            observed_ns=time.time_ns() if observed_ns is None else observed_ns,
        )

    def hop_key(self) -> tuple[Any, ...]:
        """Return an identity for this frame that survives forwarding.

        `forwarded()` re-stamps exactly `channel` and `observed_ns`, so every other field
        together identifies the same frame at any hop. This is the (run_id, can_id,
        timestamp_ns) correlation key the module docstring describes, widened to the whole
        record so that two frames a LogicalClock stamped within one tick -- identical
        timestamp_ns, different payloads -- do not collide.

        Used to recognise a frame an Ecu put on a segment itself when it comes back (see
        bench/handle.py).
        """
        return tuple(getattr(self, name) for name in FRAME_COLUMNS if name not in _HOP_VARYING)


def make_can_frame(
    *,
    run_id: str,
    source_file: str,
    run_epoch_ns: int,
    channel: int,
    can_id: int,
    data: bytes,
    is_fd: bool = False,
    is_ext_id: bool = False,
    rtr: bool = False,
    dir: int = DIR_RX,
    dlc: int | None = None,
    timestamp_ns: int | None = None,
    observed_ns: int | None = None,
) -> Frame:
    """Build a CAN or CAN-FD frame.

    `observed_ns` defaults to now; `timestamp_ns` defaults to the run-relative
    `observed_ns - run_epoch_ns`, clamped to >= 0. `dlc` defaults to len(data).
    """
    if observed_ns is None:
        observed_ns = time.time_ns()
    if timestamp_ns is None:
        timestamp_ns = max(0, observed_ns - run_epoch_ns)
    return Frame(
        run_id=run_id,
        source_file=source_file,
        message_type=CAN_FD if is_fd else CAN,
        timestamp_ns=timestamp_ns,
        observed_ns=observed_ns,
        channel=channel,
        dir=dir,
        data=data,
        can_id=can_id,
        is_ext_id=is_ext_id,
        rtr=rtr,
        dlc=len(data) if dlc is None else dlc,
        is_fd=is_fd,
    )


def make_eth_frame(
    *,
    run_id: str,
    source_file: str,
    run_epoch_ns: int,
    channel: int,
    src_addr: bytes,
    dst_addr: bytes,
    ether_type: int,
    data: bytes,
    dir: int = DIR_RX,
    vlan_tpid: int | None = None,
    vlan_cos: int | None = None,
    vlan_id: int | None = None,
    timestamp_ns: int | None = None,
    observed_ns: int | None = None,
) -> Frame:
    """Build a raw Ethernet frame.

    `data` is the payload after the Ethernet header. Supplying any VLAN field selects
    message_type ETH_EX over ETH. Raises ValueError if a MAC address is not 6 bytes.
    """
    for name, mac in (("src_addr", src_addr), ("dst_addr", dst_addr)):
        if len(mac) != 6:
            raise ValueError(f"{name} must be a 6-byte MAC address, got {len(mac)} bytes")
    if observed_ns is None:
        observed_ns = time.time_ns()
    if timestamp_ns is None:
        timestamp_ns = max(0, observed_ns - run_epoch_ns)
    tagged = vlan_tpid is not None or vlan_cos is not None or vlan_id is not None
    return Frame(
        run_id=run_id,
        source_file=source_file,
        message_type=ETH_EX if tagged else ETH,
        timestamp_ns=timestamp_ns,
        observed_ns=observed_ns,
        channel=channel,
        dir=dir,
        data=data,
        src_addr=src_addr,
        dst_addr=dst_addr,
        ether_type=ether_type,
        vlan_tpid=vlan_tpid,
        vlan_cos=vlan_cos,
        vlan_id=vlan_id,
    )


def source_file_for(run_id: str) -> str:
    """Return the synthetic BLF path a run's frames are grouped under.

    Every pipeline treats blf_bronze._source_file as an opaque per-log grouping key, so
    one run partitions exactly like one recorded file.
    """
    return f"testbench/{run_id}.blf"


def accepts(frame: Frame, *, can_ids: set[int] | None, message_types: set[str] | None) -> bool:
    """Return whether a receiver with these filters wants `frame`.

    A None filter accepts everything. `can_ids` never matches a non-CAN frame. Pure
    Frame predicate shared by every channel implementation (loopback, Lakebase, ...),
    so a channel that doesn't otherwise need any of them never has to import another
    transport module just for this filter.
    """
    if message_types is not None and frame.message_type not in message_types:
        return False
    if can_ids is not None and frame.can_id not in can_ids:
        return False
    return True
