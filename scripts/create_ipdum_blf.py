"""
Create a BLF file containing CAN-FD frames with I-PDU Multiplexer (IPduM) payloads.

Two container-frame header variants are written (matching signal.rs ContainerHeader):

  Short  -- 3-byte PDU ID (big-endian) + 1-byte DLC per I-PDU
            DLC encoding follows ISO 11898-1 Table 3 (CAN-FD):
              0-8 -> 0-8 bytes, 9->12, 10->16, 11->20, 12->24, 13->32, 14->48, 15->64

  Long   -- 4-byte PDU ID (big-endian) + 4-byte byte-length per I-PDU

Usage:
    uv run python scripts/create_ipdum_blf.py [output.blf]
"""

import struct
import sys

import can

OUT = sys.argv[1] if len(sys.argv) > 1 else "data/test_ipdum.blf"

# ── DLC helpers ───────────────────────────────────────────────────────────────

_CANFD_DLC_TO_LEN = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]
_LEN_TO_CANFD_DLC = {v: i for i, v in enumerate(_CANFD_DLC_TO_LEN)}


def len_to_dlc(n: int) -> int:
    """Return the smallest CAN-FD DLC that fits exactly n bytes, or raise."""
    if n in _LEN_TO_CANFD_DLC:
        return _LEN_TO_CANFD_DLC[n]
    raise ValueError(f"no CAN-FD DLC for payload length {n}")


# ── Container frame builders ──────────────────────────────────────────────────


def build_short(pdus: list[tuple[int, bytes]]) -> bytes:
    """Build a Short-header container frame payload.

    Each I-PDU is prefixed with:  PDU_ID[2] PDU_ID[1] PDU_ID[0]  DLC[1]
    (3-byte big-endian PDU ID, then 1-byte CAN-FD DLC)
    """
    out = bytearray()
    for pdu_id, payload in pdus:
        dlc = len_to_dlc(len(payload))
        out += struct.pack(">I", pdu_id)[1:]  # 3 high bytes of big-endian u32
        out += bytes([dlc])
        out += payload
    return bytes(out)


def build_long(pdus: list[tuple[int, bytes]]) -> bytes:
    """Build a Long-header container frame payload.

    Each I-PDU is prefixed with: PDU_ID[4]  LENGTH[4]  (both big-endian u32)
    """
    out = bytearray()
    for pdu_id, payload in pdus:
        out += struct.pack(">II", pdu_id, len(payload))
        out += payload
    return bytes(out)


# ── CAN-FD message factory ────────────────────────────────────────────────────


def canfd_msg(
    arbitration_id: int,
    data: bytes,
    timestamp: float,
    channel: int = 1,
    is_rx: bool = True,
) -> can.Message:
    return can.Message(
        arbitration_id=arbitration_id,
        data=data,
        is_fd=True,
        bitrate_switch=True,
        is_extended_id=False,
        channel=channel,
        is_rx=is_rx,
        timestamp=timestamp,
    )


# ── Scenario definitions ──────────────────────────────────────────────────────

# Short-header container frames on CAN ID 0x100
SHORT_FRAMES = [
    # t=0.001 s: two I-PDUs
    (
        0.001,
        0x100,
        build_short(
            [
                (0x000010, bytes([0xAB, 0xCD])),  # PDU 0x10, 2 bytes
                (0x000020, bytes([0xFF])),  # PDU 0x20, 1 byte
            ]
        ),
    ),
    # t=0.002 s: three I-PDUs, varying payload sizes
    (
        0.002,
        0x100,
        build_short(
            [
                (0x000010, bytes([0x01, 0x02, 0x03, 0x04])),  # PDU 0x10, 4 bytes
                (0x000020, bytes([0xAA, 0xBB])),  # PDU 0x20, 2 bytes
                (0x000030, bytes([0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])),  # PDU 0x30, 8 bytes
            ]
        ),
    ),
    # t=0.003 s: DLC=9 (12-byte payload)
    (
        0.003,
        0x100,
        build_short(
            [
                (0x000001, bytes(range(12))),  # PDU 0x01, 12 bytes -> DLC 9
            ]
        ),
    ),
    (
        0.004,
        0x600,
        build_short(
            [
                (0x000001, bytes([0x01, 0x00, 0x02, 0x11, 0xFF])),  # PDU 0x01, 5 bytes -> DLC 5
                (0x000002, bytes([0x01, 0x00, 0x02])),  # PDU 0x02, 3 bytes -> DLC 3
            ]
        ),
    ),
]

# Long-header container frames on CAN ID 0x200
LONG_FRAMES = [
    # t=0.011 s: single I-PDU
    (
        0.011,
        0x200,
        build_long(
            [
                (0x00000010, bytes([0xAB, 0xCD])),
            ]
        ),
    ),
    # t=0.012 s: two I-PDUs with larger payloads
    (
        0.012,
        0x200,
        build_long(
            [
                (0x00000010, bytes(range(8))),
                (0x00000020, bytes([0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE])),
            ]
        ),
    ),
    # t=0.013 s: 16-byte payload (arbitrary length, allowed by long header)
    (
        0.013,
        0x200,
        build_long(
            [
                (0x00000030, bytes(range(16))),
            ]
        ),
    ),
]

# Mixed: one short-header frame followed by a non-container CAN-FD frame
PLAIN_CANFD_FRAMES = [
    # t=0.020 s: plain CAN-FD (not a container frame)
    (0.020, 0x300, bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])),
]

# ── Write BLF ─────────────────────────────────────────────────────────────────

with can.BLFWriter(OUT) as writer:
    for ts, arb_id, payload in SHORT_FRAMES:
        writer(canfd_msg(arb_id, payload, ts, channel=1))

    for ts, arb_id, payload in LONG_FRAMES:
        writer(canfd_msg(arb_id, payload, ts, channel=2))

    for ts, arb_id, payload in PLAIN_CANFD_FRAMES:
        writer(canfd_msg(arb_id, payload, ts, channel=1))

print(f"Wrote {OUT}")
print(f"  {len(SHORT_FRAMES)} short-header container frames on CAN ID 0x100 (ch 1)")
print(f"  {len(LONG_FRAMES)} long-header  container frames on CAN ID 0x200 (ch 2)")
print(f"  {len(PLAIN_CANFD_FRAMES)} plain CAN-FD frame(s) on CAN ID 0x300 (ch 1)")
