"""
Databricks Delta Live Tables pipeline — Vector BLF ingestion
=============================================================

Reads *.blf files from a Unity Catalog Volume using the vector_blf Rust
extension (PyO3/maturin), and writes structured Delta tables.

Layer layout
------------
  blf_bronze                 streaming table — one row per log object, all message types
  blf_silver_can             streaming table — CAN / CAN-FD / CAN-FD64 messages
  blf_silver_eth             streaming table — Ethernet / EthernetEx messages
  blf_silver_can_signals     streaming table — decoded physical signal values (long format)
  blf_silver_eth_signals     streaming table — IP/TCP/UDP protocol fields as signals (long format)
  blf_silver_someip_signals  streaming table — SOME/IP application signals (long format)
  blf_gold_signals           streaming table — CAN + ETH + SOME/IP signals merged into one schema

Setup
-----
1. Build a manylinux wheel on a Linux host (or use GitHub Actions):

       maturin build --release --features python --manylinux auto
       # → target/wheels/vector_blf-0.1.0-cp3XX-cp3XX-manylinux_2_17_x86_64.whl

2. Upload to a Volume accessible from the cluster:

       databricks fs cp target/wheels/vector_blf-*.whl \
           /Volumes/<catalog>/<schema>/<vol>/wheels/

3. In the DLT pipeline settings → Libraries, add:
       /Volumes/<catalog>/<schema>/<vol>/wheels/vector_blf-*.whl

4. Set pipeline parameters (Edit → Advanced → Parameters):
       blf.source_path         /Volumes/mycat/myschema/blf_raw
       blf.target_catalog      mycat            (optional, default: main)
       blf.target_schema       automotive       (optional, default: blf)
       blf.signals_path        /Volumes/mycat/myschema/can_signals.csv        (optional)
       blf.someip_signals_path /Volumes/mycat/myschema/someip_signals.csv (optional)

   CAN signal CSV format (header required):
       message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
       0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
       0x200,BrakeForce,7,12,Motorola,true,0.1,-100.0

   message_id accepts hex (0x…) or decimal.  byte_order is Intel or Motorola
   (case-insensitive).  is_signed accepts true/false or 1/0.
   If blf.signals_path is not set, blf_silver_can_signals will be empty.

   SOME/IP signal CSV format (header required):
       service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
       0x0064,0x0001,MotorSpeed_rpm,0,16,Intel,false,1.0,0.0
       0x0064,0x0001,MotorTorque_Nm,16,16,Intel,true,0.1,0.0

   service_id and method_id accept hex (0x…) or decimal.
   Bit extraction uses the same Intel/Motorola logic as CAN signals, applied
   to the SOME/IP application payload (bytes after the 16-byte SOME/IP header).
   SOME/IP messages are detected via UDP; SOME/IP-SD (service_id=0xFFFF) is skipped.
   If blf.someip_signals_path is not set, blf_silver_someip_signals will be empty.

The pipeline is continuous-streaming: Auto Loader tracks which files have
been processed, so only new *.blf files are ingested on each run.
"""

import dlt
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── pipeline parameters ───────────────────────────────────────────────────────

SOURCE_PATH = spark.conf.get("blf.source_path")
TARGET_CATALOG = spark.conf.get("blf.target_catalog", "main")
TARGET_SCHEMA = spark.conf.get("blf.target_schema", "blf")
SIGNALS_PATH = spark.conf.get("blf.signals_path", "")
SOMEIP_SIGNALS_PATH = spark.conf.get("blf.someip_signals_path", "")

# ── protocol constants ────────────────────────────────────────────────────────

_ETHERTYPE_IPV4: int = 0x0800
_ETHERTYPE_IPV6: int = 0x86DD
_IPPROTO_TCP: int = 6
_IPPROTO_UDP: int = 17
_SOMEIP_PROTOCOL_VERSION: int = 0x01
_SOMEIP_SD_SERVICE_ID: int = 0xFFFF   # Service Discovery — always skip

# ── record schema (struct returned by the parsing UDF) ────────────────────────

# One element per log object.  Fields not relevant to a message type are null.
_RECORD_SCHEMA = StructType(
    [
        StructField("timestamp_ns", LongType(), nullable=False),
        StructField("message_type", StringType(), nullable=False),
        # CAN / CAN-FD / CAN-FD64
        StructField("channel", IntegerType()),
        StructField("can_id", LongType()),
        StructField("is_ext_id", BooleanType()),
        StructField("dir", ByteType()),       # 0=Tx  1=Rx  2=TxRq
        StructField("rtr", BooleanType()),
        StructField("dlc", ByteType()),
        StructField("data", BinaryType()),
        # CAN-FD extras
        StructField("fdf", BooleanType()),
        StructField("brs", BooleanType()),
        StructField("esi", BooleanType()),
        # Ethernet extras
        StructField("src_addr", BinaryType()),
        StructField("dst_addr", BinaryType()),
        StructField("ether_type", IntegerType()),
    ]
)

# ── signal output schemas ────────────────────────────────────────────────────

_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType([
        StructField("signal_name", StringType(), nullable=False),
        StructField("signal_value", DoubleType(), nullable=False),
    ])
)

_ETH_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType([
        StructField("signal_name", StringType(), nullable=False),
        StructField("signal_value", DoubleType()),   # numeric fields (ports, TTL, …)
        StructField("signal_str", StringType()),      # address strings (IPs)
    ])
)

_SOMEIP_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType([
        StructField("src_ip",            StringType(),  True),
        StructField("dst_ip",            StringType(),  True),
        StructField("udp_src_port",      IntegerType(), True),
        StructField("udp_dst_port",      IntegerType(), True),
        StructField("someip_service_id", IntegerType(), False),
        StructField("someip_method_id",  IntegerType(), False),
        StructField("someip_msg_type",   IntegerType(), False),
        StructField("signal_name",       StringType(),  False),
        StructField("signal_value",      DoubleType(),  False),
    ])
)

# ── path helper ───────────────────────────────────────────────────────────────

def _local_path(spark_path: str) -> str:
    """Convert a Spark/DBFS URI to a local filesystem path.

    Unity Catalog Volumes are mounted at /Volumes/... without the dbfs: prefix.
    Legacy DBFS paths are mounted at /dbfs/....
    """
    if spark_path.startswith("dbfs:/Volumes/"):
        return spark_path[len("dbfs:"):]          # /Volumes/...
    if spark_path.startswith("dbfs:/"):
        return "/dbfs/" + spark_path[len("dbfs:/"):]
    return spark_path                             # already a local path

# ── parsing UDF ───────────────────────────────────────────────────────────────

from pyspark.sql.functions import pandas_udf  # noqa: E402


@pandas_udf(ArrayType(_RECORD_SCHEMA))
def _parse_blf_file(paths: pd.Series) -> pd.Series:
    """Parse one BLF file per row; return a list of record dicts per file.

    Runs inside Spark executors — each executor processes a batch of file paths.
    The vector_blf wheel must be installed on the cluster (see module docstring).
    Memory note: all records for a single file are collected before yielding.
    For extremely large files (> a few hundred MB) consider splitting upstream.
    """
    import vector_blf  # noqa: PLC0415  (imported here so it's on the executor)

    result: list[list[dict]] = []

    for spark_path in paths:
        local = _local_path(spark_path)
        records: list[dict] = []
        try:
            for obj in vector_blf.Reader(
                local,
                types=["Can", "CanFd", "CanFd64", "Ethernet", "EthernetEx"],
            ):
                msg = obj.message
                rec: dict = {
                    "timestamp_ns": obj.timestamp_ns,
                    "message_type": None,
                    "channel": None,
                    "can_id": None,
                    "is_ext_id": None,
                    "dir": None,
                    "rtr": None,
                    "dlc": None,
                    "data": None,
                    "fdf": None,
                    "brs": None,
                    "esi": None,
                    "src_addr": None,
                    "dst_addr": None,
                    "ether_type": None,
                }

                if isinstance(msg, vector_blf.Can):
                    rec.update(
                        message_type="CAN",
                        channel=msg.channel,
                        can_id=msg.id,
                        is_ext_id=msg.is_ext_id,
                        dir=msg.dir,
                        rtr=msg.rtr,
                        dlc=msg.dlc,
                        data=bytes(msg.data),
                    )
                elif isinstance(msg, vector_blf.CanFd):
                    rec.update(
                        message_type="CAN_FD",
                        channel=msg.channel,
                        can_id=msg.id,
                        is_ext_id=msg.is_ext_id,
                        dir=msg.dir,
                        rtr=msg.rtr,
                        dlc=msg.dlc,
                        data=bytes(msg.data),
                        fdf=msg.fdf,
                        brs=msg.brs,
                        esi=msg.esi,
                    )
                elif isinstance(msg, vector_blf.CanFd64):
                    rec.update(
                        message_type="CAN_FD64",
                        channel=int(msg.channel),
                        can_id=msg.id,
                        is_ext_id=msg.is_ext_id,
                        dir=msg.dir,
                        rtr=msg.rtr,
                        dlc=msg.dlc,
                        data=bytes(msg.data),
                        fdf=msg.fdf,
                        brs=msg.brs,
                        esi=msg.esi,
                    )
                elif isinstance(msg, vector_blf.Ethernet):
                    rec.update(
                        message_type="ETH",
                        channel=msg.channel,
                        dir=msg.dir,
                        src_addr=bytes(msg.src_addr),
                        dst_addr=bytes(msg.dst_addr),
                        ether_type=msg.ether_type,
                        data=bytes(msg.data),
                    )
                elif isinstance(msg, vector_blf.EthernetEx):
                    rec.update(
                        message_type="ETH_EX",
                        channel=msg.channel,
                        dir=msg.dir,
                        src_addr=bytes(msg.src_addr),
                        dst_addr=bytes(msg.dst_addr),
                        ether_type=msg.ether_type,
                        data=bytes(msg.data),
                    )
                else:
                    continue  # filtered by types= above; should not reach here

                records.append(rec)

        except Exception as exc:  # noqa: BLE001
            # Log parse errors to the executor log without failing the pipeline.
            print(f"[blf_pipeline] failed to parse {spark_path!r}: {exc}")

        result.append(records)

    return pd.Series(result)


# ── bronze layer ──────────────────────────────────────────────────────────────

@dlt.table(
    name="blf_bronze",
    comment="Raw log objects from BLF files — one row per message.",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    partition_cols=["message_type"],
)
def blf_bronze():
    """Stream new BLF files via Auto Loader; expand each file into rows."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.blf")
        # Auto Loader tracks processed files in a _checkpoint directory.
        # Set to a stable Volume path so state survives pipeline restarts.
        .option(
            "cloudFiles.schemaLocation",
            f"{SOURCE_PATH}/_autoloader_schema",
        )
        .load(SOURCE_PATH)
        # Keep only path + metadata columns; avoid pulling file content into Spark.
        .select(
            F.col("path").alias("_source_file"),
            F.col("modificationTime").alias("_file_mtime"),
            F.col("length").alias("_file_size_bytes"),
        )
        .withColumn("_records", _parse_blf_file(F.col("_source_file")))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            "_source_file",
            "_file_mtime",
            "_file_size_bytes",
            "_ingested_at",
            F.explode("_records").alias("_rec"),
        )
        .select(
            "_source_file",
            "_file_mtime",
            "_file_size_bytes",
            "_ingested_at",
            "_rec.*",   # flatten all record fields to top-level columns
        )
    )


# ── silver layer — CAN ────────────────────────────────────────────────────────

_DIR_LABEL = F.when(F.col("dir") == 0, "Tx").when(F.col("dir") == 1, "Rx").otherwise("TxRq")


@dlt.table(
    name="blf_silver_can",
    comment="CAN, CAN-FD, and CAN-FD64 messages with decoded helper columns.",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_can():
    return (
        dlt.read_stream("blf_bronze")
        .filter(F.col("message_type").isin("CAN", "CAN_FD", "CAN_FD64"))
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            (F.col("timestamp_ns") / 1e9).alias("timestamp_s"),
            "channel",
            F.format_string("0x%08X", F.col("can_id")).alias("can_id_hex"),
            "can_id",
            "is_ext_id",
            _DIR_LABEL.alias("dir"),
            "rtr",
            "dlc",
            F.hex("data").alias("data_hex"),
            "data",
            # CAN-FD flags (null for classic CAN)
            "fdf",
            "brs",
            "esi",
        )
    )


# ── silver layer — Ethernet ───────────────────────────────────────────────────

def _mac_str(col_name: str) -> F.Column:
    """Format a 6-byte binary column as XX:XX:XX:XX:XX:XX."""
    b = F.col(col_name)
    return F.concat_ws(
        ":",
        *[
            F.upper(F.lpad(F.hex(F.substring(b, i, 1)), 2, "0"))
            for i in range(1, 7)
        ],
    )


# ── silver layer — decoded CAN signals ───────────────────────────────────────

# Pure-Python port of signal.rs bit-extraction logic (Intel / Motorola byte order).
# Runs on Spark executors inside the pandas UDF below.

def _extract_intel(data: bytes, start_bit: int, bit_length: int):
    raw = 0
    for i in range(bit_length):
        pos = start_bit + i
        byte_idx = pos >> 3
        if byte_idx >= len(data):
            return None
        raw |= ((data[byte_idx] >> (pos & 7)) & 1) << i
    return raw


def _extract_motorola(data: bytes, start_bit: int, bit_length: int):
    raw = 0
    pos = start_bit
    for i in range(bit_length):
        byte_idx = pos >> 3
        if byte_idx >= len(data):
            return None
        raw |= ((data[byte_idx] >> (pos & 7)) & 1) << (bit_length - 1 - i)
        pos = pos + 15 if pos % 8 == 0 else pos - 1
    return raw


def _sign_extend(value: int, bit_length: int) -> int:
    if bit_length == 0 or bit_length >= 64:
        return value
    sign_bit = 1 << (bit_length - 1)
    if value & sign_bit:
        return value | (~((1 << bit_length) - 1))
    return value


# ── signal CSV helpers ────────────────────────────────────────────────────────

def _parse_id_field(raw: str) -> int:
    """Parse a hex (0x…) or decimal integer string."""
    s = raw.strip()
    return int(s, 16) if s.lower().startswith("0x") else int(s)


_SIGNAL_COMMON_FIELDS = ("signal_name", "start_bit", "bit_length",
                         "byte_order", "is_signed", "scale", "offset")


def _coerce_signal_field(field: str, raw: str):
    raw = raw.strip()
    if field == "signal_name":  return raw
    if field == "start_bit":    return int(raw)
    if field == "bit_length":   return int(raw)
    if field == "byte_order":   return raw.lower()
    if field == "is_signed":    return raw.lower() in ("true", "1")
    if field == "scale":        return float(raw)
    if field == "offset":       return float(raw)


# Per-executor cache: csv_path → list[signal_def_dict]
_SIGNAL_DB_CACHE: dict = {}


def _load_signal_db(csv_path: str) -> list:
    if csv_path in _SIGNAL_DB_CACHE:
        return _SIGNAL_DB_CACHE[csv_path]
    import csv as _csv
    signals = []
    if csv_path:
        try:
            with open(csv_path, newline="") as fh:
                for row in _csv.DictReader(fh):
                    signals.append({
                        "message_id": _parse_id_field(row["message_id"]),
                        **{f: _coerce_signal_field(f, row[f]) for f in _SIGNAL_COMMON_FIELDS},
                    })
        except Exception as exc:
            print(f"[blf_pipeline] failed to load signals CSV {csv_path!r}: {exc}")
    _SIGNAL_DB_CACHE[csv_path] = signals
    return signals


@pandas_udf(_SIGNAL_RESULT_SCHEMA)
def _decode_signals(
    can_ids: pd.Series,
    data_col: pd.Series,
    paths: pd.Series,
) -> pd.Series:
    """Decode all matching CAN signals for each (can_id, data) row.

    `paths` carries the signal CSV path as a per-row literal so that the
    value is available on executors without relying on driver-side state.
    """
    result = []
    # All rows in this micro-batch share the same CSV path (driver-side F.lit literal).
    csv_path = paths.iloc[0] if len(paths) else ""
    signal_db = _load_signal_db(csv_path)

    for can_id, data in zip(can_ids, data_col):
        decoded = []
        if data is not None:
            raw_bytes = bytes(data)
            for sig in signal_db:
                if sig["message_id"] != can_id:
                    continue
                if sig["byte_order"] == "intel":
                    raw = _extract_intel(raw_bytes, sig["start_bit"], sig["bit_length"])
                else:
                    raw = _extract_motorola(raw_bytes, sig["start_bit"], sig["bit_length"])
                if raw is None:
                    continue
                numeric = _sign_extend(raw, sig["bit_length"]) if sig["is_signed"] else raw
                value = numeric * sig["scale"] + sig["offset"]
                decoded.append({"signal_name": sig["signal_name"], "signal_value": value})
        result.append(decoded)

    return pd.Series(result)


@dlt.table(
    name="blf_silver_can_signals",
    comment=(
        "Physical signal values decoded from CAN / CAN-FD / CAN-FD64 messages "
        "using the signal definition CSV at blf.signals_path. "
        "Long format: one row per (message, signal)."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_can_signals():
    return (
        dlt.read_stream("blf_silver_can")
        .withColumn(
            "_signals",
            _decode_signals(
                F.col("can_id"),
                F.col("data"),
                F.lit(SIGNALS_PATH),
            ),
        )
        .filter(F.size("_signals") > 0)
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "can_id",
            "can_id_hex",
            "dir",
            F.explode("_signals").alias("_sig"),
        )
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "can_id",
            "can_id_hex",
            "dir",
            F.col("_sig.signal_name"),
            F.col("_sig.signal_value"),
        )
    )


@dlt.table(
    name="blf_silver_eth",
    comment="Ethernet and EthernetEx messages with formatted MAC addresses.",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_eth():
    return (
        dlt.read_stream("blf_bronze")
        .filter(F.col("message_type").isin("ETH", "ETH_EX"))
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            (F.col("timestamp_ns") / 1e9).alias("timestamp_s"),
            "channel",
            _DIR_LABEL.alias("dir"),
            _mac_str("src_addr").alias("src_mac"),
            _mac_str("dst_addr").alias("dst_mac"),
            F.format_string("0x%04X", F.col("ether_type")).alias("ether_type_hex"),
            "ether_type",
            F.length("data").alias("payload_bytes"),
            "data",
        )
    )


# ── silver layer — parsed Ethernet payload signals ────────────────────────────

def _eth_parse_tcp(data: bytes, out: list) -> None:
    if len(data) < 20:
        return
    out.extend([
        {"signal_name": "tcp.src_port",  "signal_value": float((data[0] << 8) | data[1]), "signal_str": None},
        {"signal_name": "tcp.dst_port",  "signal_value": float((data[2] << 8) | data[3]), "signal_str": None},
        {"signal_name": "tcp.flags",     "signal_value": float(data[13] & 0x3F),           "signal_str": None},
    ])


def _eth_parse_udp(data: bytes, out: list) -> None:
    if len(data) < 8:
        return
    out.extend([
        {"signal_name": "udp.src_port",      "signal_value": float((data[0] << 8) | data[1]), "signal_str": None},
        {"signal_name": "udp.dst_port",      "signal_value": float((data[2] << 8) | data[3]), "signal_str": None},
        {"signal_name": "udp.payload_bytes", "signal_value": float(((data[4] << 8) | data[5]) - 8), "signal_str": None},
    ])


def _eth_parse_ipv4(data: bytes, out: list) -> None:
    if len(data) < 20:
        return
    ihl = (data[0] & 0x0F) * 4
    protocol = data[9]
    out.extend([
        {"signal_name": "ip.protocol",    "signal_value": float(protocol),                              "signal_str": None},
        {"signal_name": "ip.ttl",         "signal_value": float(data[8]),                               "signal_str": None},
        {"signal_name": "ip.total_len",   "signal_value": float((data[2] << 8) | data[3]),              "signal_str": None},
        {"signal_name": "ip.src",         "signal_value": None, "signal_str": ".".join(str(b) for b in data[12:16])},
        {"signal_name": "ip.dst",         "signal_value": None, "signal_str": ".".join(str(b) for b in data[16:20])},
    ])
    transport = data[ihl:] if ihl <= len(data) else b""
    if protocol == _IPPROTO_TCP:
        _eth_parse_tcp(transport, out)
    elif protocol == _IPPROTO_UDP:
        _eth_parse_udp(transport, out)


def _eth_parse_ipv6(data: bytes, out: list) -> None:
    if len(data) < 40:
        return
    next_header = data[6]
    out.extend([
        {"signal_name": "ip.protocol",   "signal_value": float(next_header), "signal_str": None},
        {"signal_name": "ip.hop_limit",  "signal_value": float(data[7]),     "signal_str": None},
        {
            "signal_name": "ip.src", "signal_value": None,
            "signal_str": ":".join(f"{(data[8  + i*2] << 8 | data[9  + i*2]):04x}" for i in range(8)),
        },
        {
            "signal_name": "ip.dst", "signal_value": None,
            "signal_str": ":".join(f"{(data[24 + i*2] << 8 | data[25 + i*2]):04x}" for i in range(8)),
        },
    ])
    transport = data[40:]
    if next_header == _IPPROTO_TCP:
        _eth_parse_tcp(transport, out)
    elif next_header == _IPPROTO_UDP:
        _eth_parse_udp(transport, out)


@pandas_udf(_ETH_SIGNAL_RESULT_SCHEMA)
def _parse_eth_payload(ether_types: pd.Series, data_col: pd.Series) -> pd.Series:
    """Extract IP/TCP/UDP header fields from the Ethernet payload as named signals."""
    result = []
    for ether_type, data in zip(ether_types, data_col):
        signals: list = []
        if data is not None:
            raw = bytes(data)
            if ether_type == _ETHERTYPE_IPV4:
                _eth_parse_ipv4(raw, signals)
            elif ether_type == _ETHERTYPE_IPV6:
                _eth_parse_ipv6(raw, signals)
        result.append(signals)
    return pd.Series(result)


@dlt.table(
    name="blf_silver_eth_signals",
    comment=(
        "Protocol-layer fields parsed from Ethernet payload data. "
        "Long format: one row per (message, signal). "
        "Covers IPv4/IPv6 headers (ip.src, ip.dst, ip.protocol, ip.ttl) "
        "and TCP/UDP transport (src_port, dst_port, tcp.flags, udp.payload_bytes). "
        "Numeric fields in signal_value; address strings in signal_str."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_eth_signals():
    return (
        dlt.read_stream("blf_silver_eth")
        .withColumn("_signals", _parse_eth_payload(F.col("ether_type"), F.col("data")))
        .filter(F.size("_signals") > 0)
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "dir",
            "src_mac",
            "dst_mac",
            "ether_type",
            "ether_type_hex",
            F.explode("_signals").alias("_sig"),
        )
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "dir",
            "src_mac",
            "dst_mac",
            "ether_type",
            "ether_type_hex",
            F.col("_sig.signal_name"),
            F.col("_sig.signal_value"),
            F.col("_sig.signal_str"),
        )
    )


# ── silver layer — SOME/IP application signals ───────────────────────────────
#
# SOME/IP header layout (16 bytes total):
#   Offset  Size  Field
#   0       2     Service ID
#   2       2     Method ID
#   4       4     Length  (bytes from offset 8 to end of message)
#   8       2     Client ID
#   10      2     Session ID
#   12      1     SOME/IP Protocol Version  (must be 0x01)
#   13      1     Interface Version
#   14      1     Message Type  (0x00=REQUEST 0x01=REQUEST_NO_RETURN 0x02=NOTIFICATION
#                                0x80=RESPONSE 0x81=ERROR)
#   15      1     Return Code
#   16+     …     Application payload  ← signal bits are extracted from here

_SOMEIP_VALID_MSG_TYPES = frozenset({0x00, 0x01, 0x02, 0x80, 0x81})

# Per-executor cache: csv_path → dict[(service_id, method_id)] → [signal_def]
_SOMEIP_SIGNAL_DB_CACHE: dict = {}


def _load_someip_signal_db(csv_path: str) -> dict:
    """Load SOME/IP signal CSV; returns dict keyed by (service_id, method_id)."""
    if csv_path in _SOMEIP_SIGNAL_DB_CACHE:
        return _SOMEIP_SIGNAL_DB_CACHE[csv_path]
    import csv as _csv
    db: dict = {}
    if csv_path:
        try:
            with open(csv_path, newline="") as fh:
                for row in _csv.DictReader(fh):
                    key = (_parse_id_field(row["service_id"]),
                           _parse_id_field(row["method_id"]))
                    db.setdefault(key, []).append(
                        {f: _coerce_signal_field(f, row[f]) for f in _SIGNAL_COMMON_FIELDS}
                    )
        except Exception as exc:
            print(f"[blf_pipeline] failed to load SOME/IP signals CSV {csv_path!r}: {exc}")
    _SOMEIP_SIGNAL_DB_CACHE[csv_path] = db
    return db


def _someip_strip_ipv4_udp(data: bytes):
    """Strip IPv4 + UDP headers; return (src_ip, dst_ip, src_port, dst_port, udp_payload) or None."""
    if len(data) < 20 or data[9] != _IPPROTO_UDP:  # not UDP
        return None
    ihl = (data[0] & 0x0F) * 4
    src_ip = ".".join(str(b) for b in data[12:16])
    dst_ip = ".".join(str(b) for b in data[16:20])
    udp = data[ihl:]
    if len(udp) < 8:
        return None
    return (
        src_ip, dst_ip,
        (udp[0] << 8) | udp[1],
        (udp[2] << 8) | udp[3],
        udp[8:],
    )


def _someip_strip_ipv6_udp(data: bytes):
    """Strip IPv6 + UDP headers; return (src_ip, dst_ip, src_port, dst_port, udp_payload) or None.

    Handles fixed IPv6 header only (no extension headers).
    """
    if len(data) < 40 or data[6] != _IPPROTO_UDP:  # next header not UDP
        return None
    src_ip = ":".join(f"{(data[8  + i*2] << 8 | data[9  + i*2]):04x}" for i in range(8))
    dst_ip = ":".join(f"{(data[24 + i*2] << 8 | data[25 + i*2]):04x}" for i in range(8))
    udp = data[40:]
    if len(udp) < 8:
        return None
    return (
        src_ip, dst_ip,
        (udp[0] << 8) | udp[1],
        (udp[2] << 8) | udp[3],
        udp[8:],
    )


def _someip_parse_header(payload: bytes):
    """Validate SOME/IP header; return (service_id, method_id, msg_type, app_payload) or None."""
    if len(payload) < 16:
        return None
    if payload[12] != _SOMEIP_PROTOCOL_VERSION:  # SOME/IP protocol version
        return None
    msg_type = payload[14]
    if msg_type not in _SOMEIP_VALID_MSG_TYPES:
        return None
    service_id = (payload[0] << 8) | payload[1]
    if service_id == _SOMEIP_SD_SERVICE_ID:       # skip SOME/IP-SD
        return None
    method_id = (payload[2] << 8) | payload[3]
    length    = int.from_bytes(payload[4:8], "big")
    if length < 8 or 8 + length > len(payload):  # length field sanity check
        return None
    return service_id, method_id, msg_type, payload[16 : 8 + length]


@pandas_udf(_SOMEIP_SIGNAL_RESULT_SCHEMA)
def _decode_someip_signals(
    ether_types: pd.Series,
    data_col: pd.Series,
    paths: pd.Series,
) -> pd.Series:
    """Decode SOME/IP application signals from Ethernet UDP payloads.

    Detection heuristic: UDP payload whose 13th byte equals 0x01 (SOME/IP protocol
    version) and whose message-type byte is a known value.  SOME/IP-SD is skipped.
    """
    # All rows in this micro-batch share the same CSV path (driver-side F.lit literal).
    csv_path = paths.iloc[0] if len(paths) else ""
    signal_db = _load_someip_signal_db(csv_path)
    result = []

    for ether_type, data in zip(ether_types, data_col):
        signals: list = []
        if data is not None:
            raw = bytes(data)
            udp_info = None
            if ether_type == _ETHERTYPE_IPV4:
                udp_info = _someip_strip_ipv4_udp(raw)
            elif ether_type == _ETHERTYPE_IPV6:
                udp_info = _someip_strip_ipv6_udp(raw)

            if udp_info is not None:
                src_ip, dst_ip, src_port, dst_port, udp_payload = udp_info
                parsed = _someip_parse_header(udp_payload)
                if parsed is not None:
                    service_id, method_id, msg_type, app_payload = parsed
                    for sig in signal_db.get((service_id, method_id), []):
                        if sig["byte_order"] == "intel":
                            raw_val = _extract_intel(app_payload, sig["start_bit"], sig["bit_length"])
                        else:
                            raw_val = _extract_motorola(app_payload, sig["start_bit"], sig["bit_length"])
                        if raw_val is None:
                            continue
                        numeric = _sign_extend(raw_val, sig["bit_length"]) if sig["is_signed"] else raw_val
                        signals.append({
                            "src_ip":            src_ip,
                            "dst_ip":            dst_ip,
                            "udp_src_port":      src_port,
                            "udp_dst_port":      dst_port,
                            "someip_service_id": service_id,
                            "someip_method_id":  method_id,
                            "someip_msg_type":   msg_type,
                            "signal_name":       sig["signal_name"],
                            "signal_value":      numeric * sig["scale"] + sig["offset"],
                        })
        result.append(signals)

    return pd.Series(result)


@dlt.table(
    name="blf_silver_someip_signals",
    comment=(
        "SOME/IP application signals decoded from Ethernet UDP payloads. "
        "Long format: one row per (message, signal). "
        "Requires blf.someip_signals_path to be set; table is empty otherwise. "
        "Signal definitions are keyed by (service_id, method_id). "
        "Bit extraction uses the same Intel/Motorola logic as CAN signals."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_someip_signals():
    return (
        dlt.read_stream("blf_silver_eth")
        .withColumn(
            "_signals",
            _decode_someip_signals(
                F.col("ether_type"),
                F.col("data"),
                F.lit(SOMEIP_SIGNALS_PATH),
            ),
        )
        .filter(F.size("_signals") > 0)
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "dir",
            "src_mac",
            "dst_mac",
            F.explode("_signals").alias("_sig"),
        )
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            "dir",
            "src_mac",
            "dst_mac",
            F.col("_sig.src_ip"),
            F.col("_sig.dst_ip"),
            F.col("_sig.udp_src_port"),
            F.col("_sig.udp_dst_port"),
            F.col("_sig.someip_service_id"),
            F.format_string("0x%04X", F.col("_sig.someip_service_id")).alias("someip_service_id_hex"),
            F.col("_sig.someip_method_id"),
            F.format_string("0x%04X", F.col("_sig.someip_method_id")).alias("someip_method_id_hex"),
            F.col("_sig.someip_msg_type"),
            F.col("_sig.signal_name"),
            F.col("_sig.signal_value"),
        )
    )


# ── gold layer — merged CAN + Ethernet + SOME/IP signals ─────────────────────

@dlt.table(
    name="blf_gold_signals",
    comment=(
        "Unified signal table merging CAN decoded signals, Ethernet protocol fields, "
        "and SOME/IP application signals. "
        "One row per (message, signal). "
        "signal_source: 'CAN', 'ETH', or 'SOMEIP'. "
        "message_id_str: can_id_hex | ether_type_hex | service_id_hex/method_id_hex. "
        "signal_value for numeric signals; signal_str for address strings (ETH only). "
        "WARNING: signal_name is NOT unique across sources — always filter by "
        "signal_source when querying a specific signal by name."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["signal_source"],
)
def blf_gold_signals():
    _null_str = F.lit(None).cast(StringType())
    can = (
        dlt.read_stream("blf_silver_can_signals")
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            F.lit("CAN").alias("signal_source"),
            F.col("can_id_hex").alias("message_id_str"),
            "dir",
            "signal_name",
            "signal_value",
            _null_str.alias("signal_str"),
        )
    )
    eth = (
        dlt.read_stream("blf_silver_eth_signals")
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            F.lit("ETH").alias("signal_source"),
            F.col("ether_type_hex").alias("message_id_str"),
            "dir",
            "signal_name",
            "signal_value",
            "signal_str",
        )
    )
    someip = (
        dlt.read_stream("blf_silver_someip_signals")
        .select(
            "_source_file",
            "_ingested_at",
            "message_type",
            "timestamp_ns",
            "timestamp_s",
            "channel",
            F.lit("SOMEIP").alias("signal_source"),
            F.concat_ws(
                "/", "someip_service_id_hex", "someip_method_id_hex"
            ).alias("message_id_str"),
            "dir",
            "signal_name",
            "signal_value",
            _null_str.alias("signal_str"),
        )
    )
    # signal_name is not globally unique; consumers must include signal_source in WHERE.
    return can.union(eth).union(someip)
