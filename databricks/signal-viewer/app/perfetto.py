"""Perfetto native trace encoding (hand-written protobuf wire format)."""

import struct

import pandas as pd

# ---------------------------------------------------------------------------
# Wire-format primitives
# ---------------------------------------------------------------------------


def _pf_varint(v: int) -> bytes:
    out = []
    while True:
        if v < 0x80:
            out.append(v)
            break
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    return bytes(out)


def _pf_field(field: int, wt: int) -> bytes:
    return _pf_varint((field << 3) | wt)


def _pf_u64(field: int, v: int) -> bytes:
    return _pf_field(field, 0) + _pf_varint(v)


def _pf_f64(field: int, v: float) -> bytes:
    return _pf_field(field, 1) + struct.pack("<d", v)


def _pf_bytes(field: int, b: bytes) -> bytes:
    return _pf_field(field, 2) + _pf_varint(len(b)) + b


def _pf_str(field: int, s: str) -> bytes:
    return _pf_bytes(field, s.encode())


def _pf_packet(inner: bytes) -> bytes:
    return _pf_bytes(1, inner)  # Trace.packet = field 1


# ---------------------------------------------------------------------------
# Perfetto proto field numbers
# ---------------------------------------------------------------------------

_PF_PKT_CLOCK_SNAPSHOT = 6
_PF_PKT_TRACK_EVENT = 11
_PF_PKT_TIMESTAMP = 8
_PF_PKT_TIMESTAMP_CLOCK_ID = 58
_PF_PKT_TRUSTED_SEQ_ID = 10
_PF_PKT_TRACK_DESCRIPTOR = 60
_PF_CLK_ID, _PF_CLK_TIMESTAMP, _PF_SNAP_CLOCKS = 1, 2, 1
_PF_TD_UUID, _PF_TD_NAME, _PF_TD_COUNTER = 1, 2, 8
_PF_TE_TYPE, _PF_TE_TRACK_UUID, _PF_TE_DOUBLE = 9, 11, 44
_PF_CLOCK_REALTIME, _PF_CLOCK_BOOTTIME = 1, 6
_PF_TYPE_COUNTER, _PF_SEQ_ID = 4, 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_perfetto_trace(df: pd.DataFrame, selected: list[str]) -> bytes:
    """Convert a signal DataFrame to a Perfetto native trace (.perfetto-trace) binary."""
    selected_set = set(selected)
    key_col = df["signal_source"] + df["channel"].astype(str) + "::" + df["signal_name"]
    sub = df[key_col.isin(selected_set)].sort_values("timestamp_ns")
    if sub.empty:
        return b""

    t_min_ns = int(sub["timestamp_ns"].min())

    # Anchor BOOTTIME=0 to wall clock
    realtime_ns = t_min_ns
    if "event_time" in sub.columns:
        first_ts = sub.loc[sub["timestamp_ns"].idxmin(), "event_time"]
        try:
            ts = pd.Timestamp(first_ts)
            ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
            if isinstance(ts, pd.Timestamp):
                realtime_ns = int(ts.timestamp() * 1e9)
        except Exception:
            pass

    buf = bytearray()

    boot_clk = _pf_u64(_PF_CLK_ID, _PF_CLOCK_BOOTTIME) + _pf_u64(_PF_CLK_TIMESTAMP, 0)
    real_clk = _pf_u64(_PF_CLK_ID, _PF_CLOCK_REALTIME) + _pf_u64(_PF_CLK_TIMESTAMP, realtime_ns)
    snap = _pf_bytes(_PF_SNAP_CLOCKS, boot_clk) + _pf_bytes(_PF_SNAP_CLOCKS, real_clk)
    pkt = _pf_bytes(_PF_PKT_CLOCK_SNAPSHOT, snap) + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
    buf += _pf_packet(pkt)

    track_uuid: dict[str, int] = {}
    for i, key in enumerate(selected, start=1):
        td = _pf_u64(_PF_TD_UUID, i) + _pf_str(_PF_TD_NAME, key) + _pf_bytes(_PF_TD_COUNTER, b"")
        pkt = _pf_bytes(_PF_PKT_TRACK_DESCRIPTOR, td) + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
        buf += _pf_packet(pkt)
        track_uuid[key] = i

    for row in sub.itertuples(index=False):
        key = f"{row.signal_source}{row.channel}::{row.signal_name}"
        uuid = track_uuid.get(key)
        if uuid is None:
            continue
        boot_ns = max(0, int(row.timestamp_ns) - t_min_ns)
        event = (
            _pf_u64(_PF_TE_TYPE, _PF_TYPE_COUNTER)
            + _pf_u64(_PF_TE_TRACK_UUID, uuid)
            + _pf_f64(_PF_TE_DOUBLE, float(row.signal_value))
        )
        pkt = (
            _pf_u64(_PF_PKT_TIMESTAMP, boot_ns)
            + _pf_u64(_PF_PKT_TIMESTAMP_CLOCK_ID, _PF_CLOCK_BOOTTIME)
            + _pf_bytes(_PF_PKT_TRACK_EVENT, event)
            + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
        )
        buf += _pf_packet(pkt)

    return bytes(buf)
