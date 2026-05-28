"""Tests for dlt_blf_pipeline — pure-Python helpers and mapInPandas workers.

Pure-function tests (no vector_blf needed):
  _local_path, _eth_parse_*, _someip_*, _uds_parse_payload,
  _isotp_sf_payload, _isotp_ff_info, _doip_*, pandas UDFs

Integration tests (requires `uv run maturin develop --features python`):
  _parse_blf_batch, _parse_blf_uds_batch, _decode_signals, _decode_someip_signals
"""

import importlib.util
import struct
from pathlib import Path

import dlt_blf_pipeline as pipeline
import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent.parent / "data"
ASSETS_DIR = Path(__file__).parent.parent / "assets"

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


# ── _eth_parse_tcp ────────────────────────────────────────────────────────────


def test_eth_parse_tcp_fields():
    data = _tcp(8080, 443, b"\x00" * 4)
    out: list = []
    pipeline._eth_parse_tcp(data, out)
    by_name = {s["signal_name"]: s["signal_value"] for s in out}
    assert by_name["tcp.src_port"] == 8080.0
    assert by_name["tcp.dst_port"] == 443.0
    assert "tcp.flags" in by_name


def test_eth_parse_tcp_too_short():
    out: list = []
    pipeline._eth_parse_tcp(b"\x00" * 10, out)
    assert out == []


# ── _eth_parse_udp ────────────────────────────────────────────────────────────


def test_eth_parse_udp_fields():
    data = _udp(5000, 5001, b"\xab" * 12)
    out: list = []
    pipeline._eth_parse_udp(data, out)
    by_name = {s["signal_name"]: s["signal_value"] for s in out}
    assert by_name["udp.src_port"] == 5000.0
    assert by_name["udp.dst_port"] == 5001.0
    assert by_name["udp.payload_bytes"] == 12.0


def test_eth_parse_udp_too_short():
    out: list = []
    pipeline._eth_parse_udp(b"\x00" * 4, out)
    assert out == []


# ── _eth_parse_ipv4 ───────────────────────────────────────────────────────────


def test_eth_parse_ipv4_with_udp():
    pkt = _ipv4(17, "192.168.1.1", "10.0.0.2", _udp(1234, 5678, b"\x00" * 4))
    out: list = []
    pipeline._eth_parse_ipv4(pkt, out)
    by_name_val = {s["signal_name"]: s["signal_value"] for s in out}
    by_name_str = {s["signal_name"]: s["signal_str"] for s in out}
    assert by_name_val["ip.protocol"] == 17.0
    assert by_name_val["ip.ttl"] == 64.0
    assert by_name_str["ip.src"] == "192.168.1.1"
    assert by_name_str["ip.dst"] == "10.0.0.2"
    assert by_name_val["udp.src_port"] == 1234.0
    assert by_name_val["udp.dst_port"] == 5678.0


def test_eth_parse_ipv4_with_tcp():
    pkt = _ipv4(6, "10.0.0.1", "10.0.0.2", _tcp(443, 12345, b"\x00" * 4))
    out: list = []
    pipeline._eth_parse_ipv4(pkt, out)
    names = {s["signal_name"] for s in out}
    assert "tcp.src_port" in names
    assert "ip.protocol" in names


def test_eth_parse_ipv4_too_short():
    out: list = []
    pipeline._eth_parse_ipv4(b"\x45" * 10, out)
    assert out == []


# ── _eth_parse_ipv6 ───────────────────────────────────────────────────────────


def test_eth_parse_ipv6_with_udp():
    src = b"\x20\x01\x0d\xb8" + b"\x00" * 12
    dst = b"\x20\x01\x0d\xb8" + b"\x00" * 11 + b"\x01"
    pkt = _ipv6(17, src, dst, _udp(9000, 9001, b"\x00" * 8))
    out: list = []
    pipeline._eth_parse_ipv6(pkt, out)
    by_name = {s["signal_name"]: s["signal_value"] for s in out}
    assert by_name["ip.protocol"] == 17.0
    assert by_name["ip.hop_limit"] == 64.0
    assert by_name["udp.src_port"] == 9000.0


def test_eth_parse_ipv6_too_short():
    out: list = []
    pipeline._eth_parse_ipv6(b"\x00" * 30, out)
    assert out == []


# ── _someip_strip_ipv4_udp ────────────────────────────────────────────────────


def test_someip_strip_ipv4_udp_valid():
    body = b"\xde\xad\xbe\xef"
    pkt = _ipv4(17, "192.168.0.1", "224.0.0.1", _udp(30509, 30509, body))
    result = pipeline._someip_strip_ipv4_udp(pkt)
    assert result is not None
    src_ip, dst_ip, src_port, dst_port, payload = result
    assert src_ip == "192.168.0.1"
    assert dst_ip == "224.0.0.1"
    assert src_port == 30509
    assert payload == body


def test_someip_strip_ipv4_udp_not_udp():
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(80, 443, b"\x00" * 4))
    assert pipeline._someip_strip_ipv4_udp(pkt) is None


def test_someip_strip_ipv4_udp_too_short():
    assert pipeline._someip_strip_ipv4_udp(b"\x00" * 10) is None


def test_someip_strip_ipv6_udp_valid():
    body = b"\xbe\xef"
    pkt = _ipv6(17, b"\x00" * 16, b"\xff\x02" + b"\x00" * 14, _udp(30490, 30490, body))
    result = pipeline._someip_strip_ipv6_udp(pkt)
    assert result is not None
    _, _, src_port, dst_port, payload = result
    assert src_port == 30490
    assert payload == body


# ── _someip_parse_header ──────────────────────────────────────────────────────


def test_someip_parse_header_valid():
    app = b"\x01\x02\x03\x04"
    hdr = _someip(0x0064, 0x0001, app_payload=app, msg_type=0x02)
    result = pipeline._someip_parse_header(hdr)
    assert result is not None
    assert result["service_id"] == 0x0064
    assert result["method_id"] == 0x0001
    assert result["protocol_version"] == 0x01
    assert result["msg_type"] == 0x02
    assert result["app_payload"] == app


def test_someip_parse_header_too_short():
    assert pipeline._someip_parse_header(b"\x00" * 10) is None


def test_someip_parse_header_wrong_protocol_version():
    hdr = bytearray(_someip(0x0064, 0x0001))
    hdr[12] = 0x02
    assert pipeline._someip_parse_header(bytes(hdr)) is None


def test_someip_parse_header_sd_service_skipped():
    assert pipeline._someip_parse_header(_someip(0xFFFF, 0x8100)) is None


def test_someip_parse_header_invalid_msg_type():
    hdr = bytearray(_someip(0x0064, 0x0001))
    hdr[14] = 0x03
    assert pipeline._someip_parse_header(bytes(hdr)) is None


def test_someip_parse_header_length_below_minimum():
    hdr = bytearray(_someip(0x0064, 0x0001))
    struct.pack_into(">I", hdr, 4, 4)  # length field < 8
    assert pipeline._someip_parse_header(bytes(hdr)) is None


# ── _uds_parse_payload ────────────────────────────────────────────────────────


def test_uds_parse_request():
    result = pipeline._uds_parse_payload(bytes([0x22, 0xF1, 0x90]))
    assert result is not None
    assert result["uds_type"] == "Request"
    assert result["service_id"] == 0x22
    assert result["service_name"] == "ReadDataByIdentifier"
    assert result["nrc"] is None
    assert result["data"] == bytes([0xF1, 0x90])


def test_uds_parse_positive_response():
    result = pipeline._uds_parse_payload(bytes([0x62, 0xF1, 0x90, 0xAB, 0xCD]))
    assert result is not None
    assert result["uds_type"] == "PositiveResponse"
    assert result["service_id"] == 0x22
    assert result["data"] == bytes([0xF1, 0x90, 0xAB, 0xCD])


def test_uds_parse_negative_response():
    result = pipeline._uds_parse_payload(bytes([0x7F, 0x22, 0x31]))
    assert result is not None
    assert result["uds_type"] == "NegativeResponse"
    assert result["service_id"] == 0x22
    assert result["nrc"] == 0x31
    assert result["nrc_name"] == "RequestOutOfRange"


def test_uds_parse_negative_response_too_short():
    assert pipeline._uds_parse_payload(bytes([0x7F, 0x22])) is None


def test_uds_parse_empty():
    assert pipeline._uds_parse_payload(b"") is None


def test_uds_parse_unknown_service():
    result = pipeline._uds_parse_payload(bytes([0xAA, 0x00]))
    assert result is not None
    assert result["uds_type"] == "Request"
    assert result["service_name"] == "Unknown_0xAA"


# ── _isotp_sf_payload ─────────────────────────────────────────────────────────


def test_isotp_sf_normal():
    assert pipeline._isotp_sf_payload(bytes([0x03, 0x22, 0xF1, 0x90])) == bytes([0x22, 0xF1, 0x90])


def test_isotp_sf_extended_canfd():
    # Extended SF: first byte 0x00, length in second byte
    data = bytes([0x00, 0x03, 0x22, 0xF1, 0x90])
    assert pipeline._isotp_sf_payload(data) == bytes([0x22, 0xF1, 0x90])


def test_isotp_sf_wrong_nibble():
    assert pipeline._isotp_sf_payload(bytes([0x10, 0x03, 0x22])) is None


def test_isotp_sf_empty():
    assert pipeline._isotp_sf_payload(b"") is None


def test_isotp_sf_truncated_payload():
    # SF claims len=5 but only 2 payload bytes provided
    assert pipeline._isotp_sf_payload(bytes([0x05, 0x22, 0xF1])) is None


# ── _isotp_ff_info ────────────────────────────────────────────────────────────


def test_isotp_ff_normal():
    data = bytes([0x10, 0x0A, 0x22, 0xF1, 0x90])
    result = pipeline._isotp_ff_info(data)
    assert result is not None
    total_len, initial = result
    assert total_len == 10
    assert initial == bytes([0x22, 0xF1, 0x90])


def test_isotp_ff_extended_canfd():
    data = bytes([0x10, 0x00]) + struct.pack(">I", 1000) + bytes([0x22, 0xF1, 0x90])
    result = pipeline._isotp_ff_info(data)
    assert result is not None
    total_len, initial = result
    assert total_len == 1000
    assert initial == bytes([0x22, 0xF1, 0x90])


def test_isotp_ff_wrong_nibble():
    assert pipeline._isotp_ff_info(bytes([0x20, 0x10, 0x22])) is None


def test_isotp_ff_too_short():
    assert pipeline._isotp_ff_info(bytes([0x10])) is None


# ── _doip_strip_tcp ───────────────────────────────────────────────────────────


def test_doip_strip_tcp_ipv4():
    body = b"\xde\xad\xbe\xef"
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(12345, pipeline._DOIP_PORT, body))
    result = pipeline._doip_strip_tcp(pipeline._ETHERTYPE_IPV4, pkt)
    assert result is not None
    src_port, dst_port, payload = result
    assert src_port == 12345
    assert dst_port == pipeline._DOIP_PORT
    assert payload == body


def test_doip_strip_tcp_ipv4_not_tcp():
    pkt = _ipv4(17, "1.2.3.4", "5.6.7.8", _udp(1234, 5678, b"\x00" * 4))
    assert pipeline._doip_strip_tcp(pipeline._ETHERTYPE_IPV4, pkt) is None


def test_doip_strip_tcp_unknown_ethertype():
    assert pipeline._doip_strip_tcp(0x0806, b"\x00" * 28) is None


# ── _doip_diag_messages ───────────────────────────────────────────────────────


def test_doip_diag_single_message():
    uds = bytes([0x22, 0xF1, 0x90])
    messages = list(pipeline._doip_diag_messages(_doip_diag(0x0E80, 0x0010, uds)))
    assert len(messages) == 1
    src, tgt, payload = messages[0]
    assert src == 0x0E80
    assert tgt == 0x0010
    assert payload == uds


def test_doip_diag_two_back_to_back():
    uds1 = bytes([0x22, 0xF1, 0x90])
    uds2 = bytes([0x7F, 0x22, 0x31])
    frame = _doip_diag(0x0E80, 0x0010, uds1) + _doip_diag(0x0010, 0x0E80, uds2)
    messages = list(pipeline._doip_diag_messages(frame))
    assert len(messages) == 2
    assert messages[0][2] == uds1
    assert messages[1][2] == uds2


def test_doip_diag_bad_version_check():
    # bytes[0] XOR bytes[1] != 0xFF => invalid
    assert list(pipeline._doip_diag_messages(bytes([0x01, 0x01]) + b"\x00" * 6)) == []


def test_doip_diag_non_diag_payload_type_skipped():
    # payload_type 0x0005 != 0x8001 — not a DiagMessage, should be skipped
    content = struct.pack(">HH", 0x0001, 0x0002) + b"\x22"
    frame = bytes([0x02, 0xFD]) + struct.pack(">HI", 0x0005, len(content)) + content
    assert list(pipeline._doip_diag_messages(frame)) == []


# ── _parse_eth_payload pandas UDF ─────────────────────────────────────────────


def test_parse_eth_payload_ipv4_udp():
    pkt = _ipv4(17, "192.168.1.100", "10.20.30.40", _udp(1234, 5678, b"\x00" * 8))
    result = pipeline._parse_eth_payload(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([pkt]))
    sigs = result.iloc[0]
    by_val = {s["signal_name"]: s["signal_value"] for s in sigs}
    by_str = {s["signal_name"]: s["signal_str"] for s in sigs}
    assert by_str["ip.src"] == "192.168.1.100"
    assert by_str["ip.dst"] == "10.20.30.40"
    assert by_val["ip.protocol"] == 17.0
    assert by_val["udp.src_port"] == 1234.0
    assert by_val["udp.payload_bytes"] == 8.0


def test_parse_eth_payload_ipv6_udp():
    src = b"\x20\x01\x0d\xb8" + b"\x00" * 12
    dst = b"\xff\x02" + b"\x00" * 14
    pkt = _ipv6(17, src, dst, _udp(9000, 9001, b"\x00" * 4))
    result = pipeline._parse_eth_payload(pd.Series([pipeline._ETHERTYPE_IPV6]), pd.Series([pkt]))
    names = {s["signal_name"] for s in result.iloc[0]}
    assert "ip.src" in names
    assert "udp.src_port" in names


def test_parse_eth_payload_none_data():
    result = pipeline._parse_eth_payload(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([None]))
    assert result.iloc[0] == []


def test_parse_eth_payload_unknown_ethertype():
    result = pipeline._parse_eth_payload(pd.Series([0x0806]), pd.Series([b"\x00" * 28]))
    assert result.iloc[0] == []


# ── _parse_someip pandas UDF ──────────────────────────────────────────────────


def test_parse_someip_valid():
    app = b"\x01\x02\x03\x04"
    pkt = _ipv4(17, "192.168.1.1", "239.0.0.1", _udp(30509, 30509, _someip(0x0064, 0x0001, app, msg_type=0x02)))
    result = pipeline._parse_someip(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([pkt]))
    rows = result.iloc[0]
    assert len(rows) == 1
    row = rows[0]
    assert row["someip_service_id"] == 0x0064
    assert row["someip_method_id"] == 0x0001
    assert row["someip_msg_type"] == 0x02
    assert row["payload"] == app
    assert row["src_ip"] == "192.168.1.1"


def test_parse_someip_container_two_pdus():
    # Two SOME/IP PDUs back-to-back in one UDP payload (AUTOSAR Container PDU Transport)
    app = b"\xde\xad"
    udp_body = _someip(0x0064, 0x0001, app) + _someip(0x0064, 0x0002, app)
    pkt = _ipv4(17, "10.0.0.1", "10.0.0.2", _udp(30509, 30509, udp_body))
    result = pipeline._parse_someip(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([pkt]))
    rows = result.iloc[0]
    assert len(rows) == 2
    assert rows[0]["someip_method_id"] == 0x0001
    assert rows[1]["someip_method_id"] == 0x0002


def test_parse_someip_sd_skipped():
    pkt = _ipv4(17, "0.0.0.0", "255.255.255.255", _udp(30490, 30490, _someip(0xFFFF, 0x8100)))
    result = pipeline._parse_someip(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([pkt]))
    assert result.iloc[0] == []


def test_parse_someip_tcp_not_parsed():
    # SOME/IP is only extracted from UDP; TCP frames yield empty rows
    pkt = _ipv4(6, "1.2.3.4", "5.6.7.8", _tcp(30509, 30509, _someip(0x0064, 0x0001)))
    result = pipeline._parse_someip(pd.Series([pipeline._ETHERTYPE_IPV4]), pd.Series([pkt]))
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


# ── _parse_blf_uds_batch integration tests ────────────────────────────────────


@_needs_vector_blf
def test_parse_blf_uds_batch_runs_without_error() -> None:
    path = str(DATA_DIR / "test_logfile.blf")
    batch_df = pd.DataFrame({"_source_file": [path], "_file_mtime": [None]})
    frames = list(pipeline._parse_blf_uds_batch(iter([batch_df])))
    # test_logfile.blf contains raw CAN — ISO-TP reassembly may yield 0 UDS PDUs,
    # but the worker must complete without raising.
    if frames:
        df = pd.concat(frames, ignore_index=True)
        required = {"_source_file", "_ingested_at", "timestamp_ns", "transport", "uds_type", "service_id"}
        assert required.issubset(set(df.columns))
        assert df["transport"].isin(["CAN", "DOIP"]).all()


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
