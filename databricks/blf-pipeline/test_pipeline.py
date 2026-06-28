"""Tests for dlt_blf_pipeline — pure-Python helpers and mapInPandas workers.

Rust-API tests (require `uv run maturin develop --features python`):
  parse_eth_payload_signals, parse_someip_udp, parse_uds,
  IsoTpReassembler, parse_doip_diag, pandas UDFs

Integration tests (require `uv run maturin develop --features python`):
  _parse_blf_batch, _decode_signals, _decode_someip_signals
"""

import importlib.util
import struct
from pathlib import Path

import dlt_blf_pipeline as pipeline
import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent.parent.parent / "data"
ASSETS_DIR = Path(__file__).parent.parent.parent / "assets"

_needs_vector_blf = pytest.mark.skipif(
    not importlib.util.find_spec("vector_blf"),
    reason="vector_blf not installed; run: uv run maturin develop --features python",
)

# ── packet-construction helpers ───────────────────────────────────────────────


def _ipv4(protocol: int, src_ip: str, dst_ip: str, payload: bytes) -> bytes:
    src = bytes(int(x) for x in src_ip.split("."))
    dst = bytes(int(x) for x in dst_ip.split("."))
    total_len = 20 + len(payload)
    return struct.pack(">BBHHHBBH4s4s", 0x45, 0, total_len, 0, 0, 64, protocol, 0, src, dst) + payload


def _ipv6(next_header: int, src: bytes, dst: bytes, payload: bytes) -> bytes:
    return struct.pack(">IHBB16s16s", 0x60000000, len(payload), next_header, 64, src, dst) + payload


def _udp(src_port: int, dst_port: int, payload: bytes) -> bytes:
    return struct.pack(">HHHH", src_port, dst_port, 8 + len(payload), 0) + payload


def _tcp(src_port: int, dst_port: int, payload: bytes) -> bytes:
    # data_offset = 0x50 => 20-byte header, flags = SYN
    return struct.pack(">HHIIBBHHH", src_port, dst_port, 0, 0, 0x50, 0x02, 65535, 0, 0) + payload


def _someip(
    service_id: int,
    method_id: int,
    app_payload: bytes = b"",
    msg_type: int = 0x00,
    return_code: int = 0x00,
    client_id: int = 0x0001,
    session_id: int = 0x0001,
    interface_version: int = 0x01,
) -> bytes:
    length = 8 + len(app_payload)
    return (
        struct.pack(
            ">HHIHHBBBB",
            service_id,
            method_id,
            length,
            client_id,
            session_id,
            0x01,
            interface_version,
            msg_type,
            return_code,
        )
        + app_payload
    )


def _doip_diag(src_addr: int, target_addr: int, uds_payload: bytes) -> bytes:
    content = struct.pack(">HH", src_addr, target_addr) + uds_payload
    return bytes([0x02, 0xFD]) + struct.pack(">HI", 0x8001, len(content)) + content


# ── _local_path ───────────────────────────────────────────────────────────────


def test_local_path_dbfs_volume_prefix():
    assert pipeline._local_path("dbfs:/Volumes/cat/schema/raw") == "/Volumes/cat/schema/raw"


def test_local_path_legacy_dbfs():
    assert pipeline._local_path("dbfs:/user/data/file.blf") == "/dbfs/user/data/file.blf"


def test_local_path_already_local():
    assert pipeline._local_path("/Volumes/cat/schema/raw") == "/Volumes/cat/schema/raw"


def test_local_path_plain():
    assert pipeline._local_path("/tmp/file.blf") == "/tmp/file.blf"


# ── parse_eth_payload_signals (vector_blf) ───────────────────────────────────


@_needs_vector_blf
def test_parse_eth_signals_tcp() -> None:
    import vector_blf

    pkt = _ipv4(6, "10.0.0.1", "10.0.0.2", _tcp(8080, 443, b"\x00" * 4))
    sigs = vector_blf.parse_eth_payload_signals(0x0800, pkt)
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    assert by_val["tcp.src_port"] == 8080.0
    assert by_val["tcp.dst_port"] == 443.0
    assert "tcp.flags" in by_val


@_needs_vector_blf
def test_parse_eth_signals_tcp_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_eth_payload_signals(0x0800, b"\x45" * 10) == []


@_needs_vector_blf
def test_parse_eth_signals_udp() -> None:
    import vector_blf

    pkt = _ipv4(17, "192.168.1.1", "10.0.0.2", _udp(5000, 5001, b"\xab" * 12))
    sigs = vector_blf.parse_eth_payload_signals(0x0800, pkt)
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    assert by_val["udp.src_port"] == 5000.0
    assert by_val["udp.dst_port"] == 5001.0
    assert by_val["udp.payload_bytes"] == 12.0


@_needs_vector_blf
def test_parse_eth_signals_udp_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_eth_payload_signals(0x0800, b"\x45" * 10) == []


@_needs_vector_blf
def test_parse_eth_signals_ipv4_fields() -> None:
    import vector_blf

    pkt = _ipv4(17, "192.168.1.1", "10.0.0.2", _udp(1234, 5678, b"\x00" * 4))
    sigs = vector_blf.parse_eth_payload_signals(0x0800, pkt)
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    by_str = {s["signal_name"]: s["signal_str"] for s in sigs}
    assert by_val["ip.protocol"] == 17.0
    assert by_val["ip.ttl"] == 64.0
    assert by_str["ip.src"] == "192.168.1.1"
    assert by_str["ip.dst"] == "10.0.0.2"
    assert by_val["udp.src_port"] == 1234.0
    assert by_val["udp.dst_port"] == 5678.0


@_needs_vector_blf
def test_parse_eth_signals_ipv4_with_tcp() -> None:
    import vector_blf

    pkt = _ipv4(6, "10.0.0.1", "10.0.0.2", _tcp(443, 12345, b"\x00" * 4))
    names = {s["signal_name"] for s in vector_blf.parse_eth_payload_signals(0x0800, pkt)}
    assert "tcp.src_port" in names
    assert "ip.protocol" in names


@_needs_vector_blf
def test_parse_eth_signals_ipv4_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_eth_payload_signals(0x0800, b"\x45" * 10) == []


@_needs_vector_blf
def test_parse_eth_signals_ipv6_with_udp() -> None:
    import vector_blf

    src = b"\x20\x01\x0d\xb8" + b"\x00" * 12
    dst = b"\x20\x01\x0d\xb8" + b"\x00" * 11 + b"\x01"
    pkt = _ipv6(17, src, dst, _udp(9000, 9001, b"\x00" * 8))
    sigs = vector_blf.parse_eth_payload_signals(0x86DD, pkt)
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    assert by_val["ip.protocol"] == 17.0
    assert by_val["ip.hop_limit"] == 64.0
    assert by_val["udp.src_port"] == 9000.0


@_needs_vector_blf
def test_parse_eth_signals_ipv6_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_eth_payload_signals(0x86DD, b"\x00" * 30) == []


@_needs_vector_blf
def test_parse_eth_signals_unknown_ethertype() -> None:
    import vector_blf

    assert vector_blf.parse_eth_payload_signals(0x9999, b"\x00" * 28) == []


# ── parse_someip_udp (vector_blf) ────────────────────────────────────────────


@_needs_vector_blf
def test_parse_someip_udp_valid_ipv4() -> None:
    import vector_blf

    app = b"\x01\x02\x03\x04"
    pkt = _ipv4(17, "192.168.0.1", "224.0.0.1", _udp(30509, 30509, _someip(0x0064, 0x0001, app, msg_type=0x02)))
    rows = vector_blf.parse_someip_udp(0x0800, pkt)
    assert len(rows) == 1
    row = rows[0]
    assert row["someip_service_id"] == 0x0064
    assert row["someip_method_id"] == 0x0001
    assert row["someip_protocol_version"] == 0x01
    assert row["someip_msg_type"] == 0x02
    assert row["payload"] == app
    assert row["src_ip"] == "192.168.0.1"


@_needs_vector_blf
def test_parse_someip_udp_not_udp() -> None:
    import vector_blf

    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(80, 443, b"\x00" * 4))
    assert vector_blf.parse_someip_udp(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_someip_udp_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_someip_udp(0x0800, b"\x00" * 10) == []


@_needs_vector_blf
def test_parse_someip_udp_ipv6_valid() -> None:
    import vector_blf

    body = b"\xbe\xef"
    pkt = _ipv6(17, b"\x00" * 16, b"\xff\x02" + b"\x00" * 14, _udp(30490, 30490, _someip(0x0064, 0x0001, body)))
    rows = vector_blf.parse_someip_udp(0x86DD, pkt)
    assert len(rows) == 1
    assert rows[0]["someip_service_id"] == 0x0064


@_needs_vector_blf
def test_parse_someip_udp_sd_skipped() -> None:
    import vector_blf

    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(30490, 30490, _someip(0xFFFF, 0x8100)))
    assert vector_blf.parse_someip_udp(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_someip_udp_wrong_protocol_version() -> None:
    import vector_blf

    hdr = bytearray(_someip(0x0064, 0x0001))
    hdr[12] = 0x02  # change protocol version from 0x01 to 0x02
    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(30509, 30509, bytes(hdr)))
    assert vector_blf.parse_someip_udp(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_someip_udp_invalid_msg_type() -> None:
    import vector_blf

    hdr = bytearray(_someip(0x0064, 0x0001))
    hdr[14] = 0x03  # invalid message type
    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(30509, 30509, bytes(hdr)))
    assert vector_blf.parse_someip_udp(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_someip_udp_length_below_minimum() -> None:
    import vector_blf

    hdr = bytearray(_someip(0x0064, 0x0001))
    struct.pack_into(">I", hdr, 4, 4)  # length field < 8
    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(30509, 30509, bytes(hdr)))
    assert vector_blf.parse_someip_udp(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_someip_udp_container_two_pdus() -> None:
    import vector_blf

    app = b"\xde\xad"
    udp_body = _someip(0x0064, 0x0001, app) + _someip(0x0064, 0x0002, app)
    pkt = _ipv4(17, "10.0.0.1", "10.0.0.2", _udp(30509, 30509, udp_body))
    rows = vector_blf.parse_someip_udp(0x0800, pkt)
    assert len(rows) == 2
    assert rows[0]["someip_method_id"] == 0x0001
    assert rows[1]["someip_method_id"] == 0x0002


# ── parse_uds (vector_blf) ────────────────────────────────────────────────────


@_needs_vector_blf
def test_parse_uds_request() -> None:
    import vector_blf

    result = vector_blf.parse_uds(bytes([0x22, 0xF1, 0x90]))
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "Request"
    assert service_id == 0x22
    assert service_name == "ReadDataByIdentifier"
    assert nrc is None
    assert bytes(data) == bytes([0xF1, 0x90])


@_needs_vector_blf
def test_parse_uds_positive_response() -> None:
    import vector_blf

    result = vector_blf.parse_uds(bytes([0x62, 0xF1, 0x90, 0xAB, 0xCD]))
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "PositiveResponse"
    assert service_id == 0x22
    assert bytes(data) == bytes([0xF1, 0x90, 0xAB, 0xCD])


@_needs_vector_blf
def test_parse_uds_negative_response() -> None:
    import vector_blf

    result = vector_blf.parse_uds(bytes([0x7F, 0x22, 0x31]))
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "NegativeResponse"
    assert service_id == 0x22
    assert nrc == 0x31
    assert nrc_name == "RequestOutOfRange"


@_needs_vector_blf
def test_parse_uds_negative_response_too_short() -> None:
    import vector_blf

    assert vector_blf.parse_uds(bytes([0x7F, 0x22])) is None


@_needs_vector_blf
def test_parse_uds_empty() -> None:
    import vector_blf

    assert vector_blf.parse_uds(b"") is None


@_needs_vector_blf
def test_parse_uds_unknown_service() -> None:
    import vector_blf

    result = vector_blf.parse_uds(bytes([0xAA, 0x00]))
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "Request"
    assert service_name == "Unknown_0xAA"


# ── IsoTpReassembler (vector_blf) ─────────────────────────────────────────────


@_needs_vector_blf
def test_isotp_reassembler_single_frame() -> None:
    import vector_blf

    result = vector_blf.IsoTpReassembler().push(bytes([0x03, 0x22, 0xF1, 0x90]))
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "Request"
    assert service_id == 0x22
    assert bytes(data) == bytes([0xF1, 0x90])


@_needs_vector_blf
def test_isotp_reassembler_sf_extended_canfd() -> None:
    import vector_blf

    # Extended SF for CAN FD: first byte 0x00, length in second byte
    result = vector_blf.IsoTpReassembler().push(bytes([0x00, 0x03, 0x22, 0xF1, 0x90]))
    assert result is not None
    uds_type, _, _, _, _, data = result
    assert uds_type == "Request"
    assert bytes(data) == bytes([0xF1, 0x90])


@_needs_vector_blf
def test_isotp_reassembler_first_frame_returns_none() -> None:
    import vector_blf

    # FF: total_len=10, initial payload=[0x22, 0xF1, 0x90]
    assert vector_blf.IsoTpReassembler().push(bytes([0x10, 0x0A, 0x22, 0xF1, 0x90])) is None


@_needs_vector_blf
def test_isotp_reassembler_ff_extended_canfd_returns_none() -> None:
    import vector_blf

    # Extended FF for CAN FD: bytes [0x10, 0x00] + 32-bit total_len + initial_data
    data = bytes([0x10, 0x00]) + struct.pack(">I", 1000) + bytes([0x22, 0xF1, 0x90])
    assert vector_blf.IsoTpReassembler().push(data) is None


@_needs_vector_blf
def test_isotp_reassembler_empty() -> None:
    import vector_blf

    assert vector_blf.IsoTpReassembler().push(b"") is None


@_needs_vector_blf
def test_isotp_reassembler_truncated_single_frame() -> None:
    import vector_blf

    # SF claims len=5 but only 2 payload bytes provided
    assert vector_blf.IsoTpReassembler().push(bytes([0x05, 0x22, 0xF1])) is None


@_needs_vector_blf
def test_isotp_reassembler_consecutive_without_first_frame() -> None:
    import vector_blf

    # CF with no preceding FF — should return None
    assert vector_blf.IsoTpReassembler().push(bytes([0x21, 0x22, 0xF1])) is None


@_needs_vector_blf
def test_isotp_reassembler_ff_too_short() -> None:
    import vector_blf

    assert vector_blf.IsoTpReassembler().push(bytes([0x10])) is None


@_needs_vector_blf
def test_isotp_reassembler_multiframe() -> None:
    import vector_blf

    # FF: total_len=9, initial=[0x62, 0xF1, 0x90, 0xAB, 0xCD, 0xEF] (6 bytes)
    ff = bytes([0x10, 0x09, 0x62, 0xF1, 0x90, 0xAB, 0xCD, 0xEF])
    # CF: seq=1, remaining=[0x01, 0x02, 0x03]
    cf = bytes([0x21, 0x01, 0x02, 0x03])
    r = vector_blf.IsoTpReassembler()
    assert r.push(ff) is None
    result = r.push(cf)
    assert result is not None
    uds_type, service_id, service_name, nrc, nrc_name, data = result
    assert uds_type == "PositiveResponse"
    assert service_id == 0x22
    assert bytes(data) == bytes([0xF1, 0x90, 0xAB, 0xCD, 0xEF, 0x01, 0x02, 0x03])


# ── parse_doip_diag (vector_blf) ──────────────────────────────────────────────


@_needs_vector_blf
def test_parse_doip_diag_single_message() -> None:
    import vector_blf

    uds = bytes([0x22, 0xF1, 0x90])
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(12345, 13400, _doip_diag(0x0E80, 0x0010, uds)))
    messages = vector_blf.parse_doip_diag(0x0800, pkt)
    assert len(messages) == 1
    src, tgt, payload = messages[0]
    assert src == 0x0E80
    assert tgt == 0x0010
    assert bytes(payload) == uds


@_needs_vector_blf
def test_parse_doip_diag_two_back_to_back() -> None:
    import vector_blf

    uds1 = bytes([0x22, 0xF1, 0x90])
    uds2 = bytes([0x7F, 0x22, 0x31])
    tcp_body = _doip_diag(0x0E80, 0x0010, uds1) + _doip_diag(0x0010, 0x0E80, uds2)
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(12345, 13400, tcp_body))
    messages = vector_blf.parse_doip_diag(0x0800, pkt)
    assert len(messages) == 2
    assert bytes(messages[0][2]) == uds1
    assert bytes(messages[1][2]) == uds2


@_needs_vector_blf
def test_parse_doip_diag_not_tcp() -> None:
    import vector_blf

    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(1234, 5678, b"\x00" * 4))
    assert vector_blf.parse_doip_diag(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_doip_diag_unknown_ethertype() -> None:
    import vector_blf

    assert vector_blf.parse_doip_diag(0x0806, b"\x00" * 28) == []


@_needs_vector_blf
def test_parse_doip_diag_bad_version_check() -> None:
    import vector_blf

    # DoIP version byte XOR inverse byte != 0xFF => invalid
    bad_doip = bytes([0x01, 0x01]) + b"\x00" * 6
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(12345, 13400, bad_doip))
    assert vector_blf.parse_doip_diag(0x0800, pkt) == []


@_needs_vector_blf
def test_parse_doip_diag_non_diag_payload_type_skipped() -> None:
    import vector_blf

    # payload_type 0x0005 != 0x8001 — not a DiagMessage, should be skipped
    content = struct.pack(">HH", 0x0001, 0x0002) + b"\x22"
    frame = bytes([0x02, 0xFD]) + struct.pack(">HI", 0x0005, len(content)) + content
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(12345, 13400, frame))
    assert vector_blf.parse_doip_diag(0x0800, pkt) == []


# ── _parse_eth_payload pandas UDF ─────────────────────────────────────────────


@_needs_vector_blf
def test_parse_eth_payload_ipv4_udp():
    pkt = _ipv4(17, "192.168.1.100", "10.20.30.40", _udp(1234, 5678, b"\x00" * 8))
    result = pipeline._parse_eth_payload(pd.Series([0x0800]), pd.Series([pkt]))
    sigs = result.iloc[0]
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    by_str = {s["signal_name"]: s["signal_str"] for s in sigs}
    assert by_str["ip.src"] == "192.168.1.100"
    assert by_str["ip.dst"] == "10.20.30.40"
    assert by_val["ip.protocol"] == 17.0
    assert by_val["udp.src_port"] == 1234.0
    assert by_val["udp.payload_bytes"] == 8.0


@_needs_vector_blf
def test_parse_eth_payload_ipv6_udp():
    src = b"\x20\x01\x0d\xb8" + b"\x00" * 12
    dst = b"\xff\x02" + b"\x00" * 14
    pkt = _ipv6(17, src, dst, _udp(9000, 9001, b"\x00" * 4))
    result = pipeline._parse_eth_payload(pd.Series([0x86DD]), pd.Series([pkt]))
    names = {s["signal_name"] for s in result.iloc[0]}
    assert "ip.src" in names
    assert "udp.src_port" in names


@_needs_vector_blf
def test_parse_eth_payload_none_data():
    result = pipeline._parse_eth_payload(pd.Series([0x0800]), pd.Series([None]))
    assert result.iloc[0] == []


@_needs_vector_blf
def test_parse_eth_payload_unknown_ethertype():
    result = pipeline._parse_eth_payload(pd.Series([0x0806]), pd.Series([b"\x00" * 28]))
    assert result.iloc[0] == []


# ── _parse_someip pandas UDF ──────────────────────────────────────────────────


@_needs_vector_blf
def test_parse_someip_valid():
    app = b"\x01\x02\x03\x04"
    pkt = _ipv4(17, "192.168.1.1", "239.0.0.1", _udp(30509, 30509, _someip(0x0064, 0x0001, app, msg_type=0x02)))
    result = pipeline._parse_someip(pd.Series([0x0800]), pd.Series([pkt]))
    rows = result.iloc[0]
    assert len(rows) == 1
    row = rows[0]
    assert row["someip_service_id"] == 0x0064
    assert row["someip_method_id"] == 0x0001
    assert row["someip_msg_type"] == 0x02
    assert row["payload"] == app
    assert row["src_ip"] == "192.168.1.1"


@_needs_vector_blf
def test_parse_someip_container_two_pdus():
    # Two SOME/IP PDUs back-to-back in one UDP payload (AUTOSAR Container PDU Transport)
    app = b"\xde\xad"
    udp_body = _someip(0x0064, 0x0001, app) + _someip(0x0064, 0x0002, app)
    pkt = _ipv4(17, "10.0.0.1", "10.0.0.2", _udp(30509, 30509, udp_body))
    result = pipeline._parse_someip(pd.Series([0x0800]), pd.Series([pkt]))
    rows = result.iloc[0]
    assert len(rows) == 2
    assert rows[0]["someip_method_id"] == 0x0001
    assert rows[1]["someip_method_id"] == 0x0002


@_needs_vector_blf
def test_parse_someip_sd_skipped():
    pkt = _ipv4(17, "0.0.0.0", "255.255.255.255", _udp(30490, 30490, _someip(0xFFFF, 0x8100)))
    result = pipeline._parse_someip(pd.Series([0x0800]), pd.Series([pkt]))
    assert result.iloc[0] == []


@_needs_vector_blf
def test_parse_someip_tcp_not_parsed():
    # SOME/IP is only extracted from UDP; TCP frames yield empty rows
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(30509, 30509, _someip(0x0064, 0x0001)))
    result = pipeline._parse_someip(pd.Series([0x0800]), pd.Series([pkt]))
    assert result.iloc[0] == []


# ── _parse_blf_batch integration tests ───────────────────────────────────────


@_needs_vector_blf
@pytest.mark.parametrize(
    "blf_file,expected_type",
    [
        ("test_logfile.blf", "CAN"),
        ("test_CanFdMessage.blf", "CAN_FD"),
        ("test_CanFdMessage64.blf", "CAN_FD64"),
        ("test_EthernetFrame.blf", "ETH"),
        ("test_EthernetFrameEx.blf", "ETH_EX"),
    ],
)
def test_parse_blf_batch_message_type(blf_file: str, expected_type: str) -> None:
    path = str(DATA_DIR / blf_file)
    batch_df = pd.DataFrame({"_source_file": [path], "_file_mtime": [None], "_file_size_bytes": [None]})
    frames = list(pipeline._parse_blf_batch(iter([batch_df])))
    assert frames, f"no rows parsed from {blf_file}"
    df = pd.concat(frames, ignore_index=True)
    assert expected_type in df["message_type"].values


@_needs_vector_blf
def test_parse_blf_batch_output_schema() -> None:
    path = str(DATA_DIR / "test_logfile.blf")
    batch_df = pd.DataFrame({"_source_file": [path], "_file_mtime": [None], "_file_size_bytes": [None]})
    df = pd.concat(list(pipeline._parse_blf_batch(iter([batch_df]))), ignore_index=True)
    required = {"_source_file", "_ingested_at", "timestamp_ns", "message_type", "channel", "can_id", "data"}
    assert required.issubset(set(df.columns))
    assert (df["_source_file"] == path).all()
    assert (df["timestamp_ns"] > 0).all()


@_needs_vector_blf
def test_parse_blf_batch_missing_file_does_not_raise() -> None:
    batch_df = pd.DataFrame(
        {"_source_file": ["/nonexistent/missing.blf"], "_file_mtime": [None], "_file_size_bytes": [None]}
    )
    frames = list(pipeline._parse_blf_batch(iter([batch_df])))
    assert frames == []


# ── _decode_signals integration tests ────────────────────────────────────────


@_needs_vector_blf
def test_decode_signals_with_csv() -> None:
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    # CAN ID 0x100: EngineSpeed_rpm bits 0-15 Intel, scale 0.125
    # raw bytes [0x80, 0x00, ...] => raw value 0x0080 = 128, physical = 128 * 0.125 = 16.0
    data = bytes([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    result = pipeline._decode_signals(
        pd.Series([0x100]),
        pd.Series([data]),
        pd.Series([csv_path]),
        pd.Series([False]),
    )
    signals = result.iloc[0]
    by_name = {s["signal_name"]: s["signal_value"] for s in signals}
    assert "EngineSpeed_rpm" in by_name
    assert by_name["EngineSpeed_rpm"] == pytest.approx(16.0)


@_needs_vector_blf
def test_decode_signals_skips_container_frame() -> None:
    # CAN ID 0x600 is a container frame; _decode_signals must return empty — container
    # rows are handled by the _extract_container_pdus -> _decode_pdu_signals path.
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    frame = bytes([0x00, 0x00, 0x01, 0x05, 0x10, 0x00, 0x00, 0x00, 0x00])
    result = pipeline._decode_signals(
        pd.Series([0x600]),
        pd.Series([frame]),
        pd.Series([csv_path]),
        pd.Series([False]),
    )
    assert result.iloc[0] == []


@_needs_vector_blf
def test_extract_container_pdus_short_header() -> None:
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    # CAN ID 0x600 is a container frame with two PDUs defined in can_signals.csv:
    #   PDU 0x01: Radar_Distance_m (bits 0-15, Intel, scale 0.01)
    #             Radar_RelSpeed_mps (bits 16-31, Intel, signed, scale 0.01)
    #             Radar_Confidence_pct (bits 32-39, Intel, scale 1.0)
    #   PDU 0x02: CabinTemp_degC (bits 0-7, Intel, signed, scale 0.5, offset -40)
    #             FanSpeed_pct (bits 8-15, Intel, scale 0.4)
    #             AcRequest (bit 16, Intel, scale 1.0)
    #
    # Short header: [pdu_id_24bit_BE, dlc_8bit] + payload  (4-byte overhead per PDU)
    # dlc=5 -> 5 bytes payload; dlc=3 -> 3 bytes payload
    frame = bytes(
        [
            0x00,
            0x00,
            0x01,
            0x05,  # PDU 0x01 header: id=1, dlc=5
            0x10,
            0x00,
            0x00,
            0x00,
            0x00,  # PDU 0x01 payload -> Radar_Distance_m raw=16
            0x00,
            0x00,
            0x02,
            0x03,  # PDU 0x02 header: id=2, dlc=3
            0x14,
            0x00,
            0x00,  # PDU 0x02 payload -> CabinTemp_degC raw=20
        ]
    )
    result = pipeline._extract_container_pdus(
        pd.Series([0x600]),
        pd.Series([frame]),
        pd.Series([csv_path]),
        pd.Series([False]),
    )
    pdus = {p["pdu_id"]: p["pdu_payload"] for p in result.iloc[0]}
    assert set(pdus.keys()) == {1, 2}
    assert pdus[1] == bytes([0x10, 0x00, 0x00, 0x00, 0x00])
    assert pdus[2] == bytes([0x14, 0x00, 0x00])


@_needs_vector_blf
def test_extract_container_pdus_long_header() -> None:
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    # Long header: [pdu_id_32bit_BE, byte_length_32bit_BE] + payload (8-byte overhead per PDU)
    frame = bytes(
        [
            0x00,
            0x00,
            0x00,
            0x01,  # PDU 0x01 id (u32 BE)
            0x00,
            0x00,
            0x00,
            0x05,  # PDU 0x01 byte length = 5
            0x10,
            0x00,
            0x00,
            0x00,
            0x00,  # PDU 0x01 payload -> Radar_Distance_m raw=16
        ]
    )
    result = pipeline._extract_container_pdus(
        pd.Series([0x600]),
        pd.Series([frame]),
        pd.Series([csv_path]),
        pd.Series([True]),
    )
    pdus = {p["pdu_id"]: p["pdu_payload"] for p in result.iloc[0]}
    assert 1 in pdus
    assert pdus[1] == bytes([0x10, 0x00, 0x00, 0x00, 0x00])


@_needs_vector_blf
def test_extract_container_pdus_non_container_returns_empty() -> None:
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    result = pipeline._extract_container_pdus(
        pd.Series([0x100]),  # regular frame, not a container
        pd.Series([bytes([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])]),
        pd.Series([csv_path]),
        pd.Series([False]),
    )
    assert result.iloc[0] == []


@_needs_vector_blf
def test_decode_pdu_signals_short_header_two_pdus() -> None:
    csv_path = str(ASSETS_DIR / "can_signals.csv")
    # PDU 0x01 of container 0x600: Radar_Distance_m raw=16 -> 0.16
    pdu1_payload = bytes([0x10, 0x00, 0x00, 0x00, 0x00])
    result = pipeline._decode_pdu_signals(
        pd.Series([0x600]),
        pd.Series([1]),
        pd.Series([pdu1_payload]),
        pd.Series([csv_path]),
    )
    by_name = {s["signal_name"]: s["signal_value"] for s in result.iloc[0]}
    assert by_name["Radar_Distance_m"] == pytest.approx(0.16)  # 16 * 0.01

    # PDU 0x02 of container 0x600: CabinTemp_degC raw=20 -> -30
    pdu2_payload = bytes([0x14, 0x00, 0x00])
    result2 = pipeline._decode_pdu_signals(
        pd.Series([0x600]),
        pd.Series([2]),
        pd.Series([pdu2_payload]),
        pd.Series([csv_path]),
    )
    by_name2 = {s["signal_name"]: s["signal_value"] for s in result2.iloc[0]}
    assert by_name2["CabinTemp_degC"] == pytest.approx(-30.0)  # 20 * 0.5 - 40


@_needs_vector_blf
def test_decode_signals_empty_path_returns_no_signals() -> None:
    result = pipeline._decode_signals(
        pd.Series([0x100]),
        pd.Series([bytes([0x00] * 8)]),
        pd.Series([""]),
        pd.Series([False]),
    )
    assert result.iloc[0] == []


# ── _decode_someip_signals integration tests ──────────────────────────────────


@_needs_vector_blf
def test_decode_someip_signals_with_csv() -> None:
    csv_path = str(ASSETS_DIR / "someip_signals.csv")
    # service_id=0x0064, method_id=0x0001: MotorSpeed_rpm bits 0-15 Intel, scale 1.0
    # raw [0x10, 0x00, ...] => raw=16, physical=16.0
    app_payload = bytes([0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    result = pipeline._decode_someip_signals(
        pd.Series([0x0064]),
        pd.Series([0x0001]),
        pd.Series([app_payload]),
        pd.Series([csv_path]),
    )
    signals = result.iloc[0]
    by_name = {s["signal_name"]: s["signal_value"] for s in signals}
    assert "MotorSpeed_rpm" in by_name
    assert by_name["MotorSpeed_rpm"] == pytest.approx(16.0)
