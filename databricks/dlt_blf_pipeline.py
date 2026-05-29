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
  blf_silver_someip          streaming table — SOME/IP messages parsed from Ethernet UDP frames
  blf_silver_can_signals     streaming table — decoded physical signal values (long format)
  blf_silver_eth_signals     streaming table — IP/TCP/UDP protocol fields as signals (long format)
  blf_silver_someip_signals  streaming table — SOME/IP application signals (long format)
  blf_silver_mf4_signals     streaming table — MF4 scalar signal values (long format)
  blf_silver_diag            streaming table — UDS messages merged from CAN ISO-TP and DoIP
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
       blf.container_long_header false                (optional, default: false)

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
    TimestampType,
)

# ── pipeline parameters ───────────────────────────────────────────────────────

SOURCE_PATH = spark.conf.get("blf.source_path")
TARGET_CATALOG = spark.conf.get("blf.target_catalog", "main")
TARGET_SCHEMA = spark.conf.get("blf.target_schema", "blf")
SIGNALS_PATH = spark.conf.get("blf.signals_path", "")
SOMEIP_SIGNALS_PATH = spark.conf.get("blf.someip_signals_path", "")
CONTAINER_LONG_HEADER: bool = spark.conf.get("blf.container_long_header", "false").lower() == "true"

# ── protocol constants ────────────────────────────────────────────────────────

_ETHERTYPE_IPV4: int = 0x0800
_ETHERTYPE_IPV6: int = 0x86DD
_IPPROTO_TCP: int = 6
_IPPROTO_UDP: int = 17
_SOMEIP_PROTOCOL_VERSION: int = 0x01
_SOMEIP_SD_SERVICE_ID: int = 0xFFFF  # Service Discovery — always skip
_DOIP_PORT: int = 13400
_DOIP_PAYLOAD_TYPE_DIAG: int = 0x8001  # DoIP DiagMessage — carries UDS payload

# ── bronze output schema (flat rows emitted by _parse_blf_batch) ──────────────

_BRONZE_OUTPUT_SCHEMA = StructType(
    [
        StructField("_source_file", StringType(), nullable=False),
        StructField("_file_mtime", TimestampType()),
        StructField("_file_size_bytes", LongType()),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("timestamp_ns", LongType(), nullable=False),
        StructField("message_type", StringType(), nullable=False),
        # CAN / CAN-FD / CAN-FD64
        StructField("channel", IntegerType()),
        StructField("can_id", LongType()),
        StructField("is_ext_id", BooleanType()),
        StructField("dir", ByteType()),  # 0=Tx  1=Rx  2=TxRq
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
        # MF4 scalar signal extras
        StructField("mf4_group", StringType()),
        StructField("mf4_name", StringType()),
        StructField("mf4_value", DoubleType()),
        StructField("mf4_unit", StringType()),
    ]
)

# Rows buffered per executor before yielding a DataFrame; limits peak heap per file.
_PARSE_BATCH_SIZE = 5_000

# ── signal output schemas ────────────────────────────────────────────────────

_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("signal_name", StringType(), nullable=False),
            StructField("signal_value", DoubleType(), nullable=False),
        ]
    )
)

_ETH_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("signal_name", StringType(), nullable=False),
            StructField("signal_value", DoubleType()),  # numeric fields (ports, TTL, …)
            StructField("signal_str", StringType()),  # address strings (IPs)
        ]
    )
)

_SOMEIP_ROW_SCHEMA = ArrayType(
    StructType(
        [
            StructField("src_ip", StringType(), True),
            StructField("dst_ip", StringType(), True),
            StructField("udp_src_port", IntegerType(), True),
            StructField("udp_dst_port", IntegerType(), True),
            StructField("someip_service_id", IntegerType(), False),
            StructField("someip_method_id", IntegerType(), False),
            StructField("someip_length", IntegerType(), False),
            StructField("someip_client_id", IntegerType(), False),
            StructField("someip_session_id", IntegerType(), False),
            StructField("someip_protocol_version", IntegerType(), False),
            StructField("someip_interface_version", IntegerType(), False),
            StructField("someip_msg_type", IntegerType(), False),
            StructField("someip_return_code", IntegerType(), False),
            StructField("payload", BinaryType(), False),
        ]
    )
)

# ── path helper ───────────────────────────────────────────────────────────────


def _local_path(spark_path: str) -> str:
    """Convert a Spark/DBFS URI to a local filesystem path.

    Unity Catalog Volumes are mounted at /Volumes/... without the dbfs: prefix.
    Legacy DBFS paths are mounted at /dbfs/....
    """
    if spark_path.startswith("dbfs:/Volumes/"):
        return spark_path[len("dbfs:") :]  # /Volumes/...
    if spark_path.startswith("dbfs:/"):
        return "/dbfs/" + spark_path[len("dbfs:/") :]
    return spark_path  # already a local path


# ── parsing worker (mapInPandas) ──────────────────────────────────────────────

from pyspark.sql.functions import pandas_udf  # noqa: E402  (still used by signal UDFs)


def _parse_blf_batch(iterator):
    """mapInPandas worker: parse BLF files and yield record rows iteratively.

    Each input DataFrame batch contains one or more file-path rows.  Records are
    emitted in chunks of _PARSE_BATCH_SIZE, so peak heap is proportional to that
    constant rather than to the file size.
    """
    from datetime import datetime, timezone

    import vector_blf  # noqa: PLC0415

    for batch_df in iterator:
        rows: list[dict] = []
        now = datetime.now(timezone.utc)

        for _, row in batch_df.iterrows():
            spark_path = str(row["_source_file"])
            file_mtime = row.get("_file_mtime")
            file_size = row.get("_file_size_bytes")
            local = _local_path(spark_path)
            try:
                for obj in vector_blf.Reader(
                    local,
                    types=["Can", "CanFd", "CanFd64", "Ethernet", "EthernetEx", "Mf4Signal"],
                ):
                    msg = obj.message
                    rec: dict = {
                        "_source_file": spark_path,
                        "_file_mtime": file_mtime,
                        "_file_size_bytes": file_size,
                        "_ingested_at": now,
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
                        "mf4_group": None,
                        "mf4_name": None,
                        "mf4_value": None,
                        "mf4_unit": None,
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
                    elif isinstance(msg, vector_blf.Mf4Signal):
                        rec.update(
                            message_type="MF4_SIGNAL",
                            mf4_group=msg.group,
                            mf4_name=msg.name,
                            mf4_value=msg.value,
                            mf4_unit=msg.unit,
                        )
                    else:
                        continue

                    rows.append(rec)
                    if len(rows) >= _PARSE_BATCH_SIZE:
                        yield pd.DataFrame(rows)
                        rows = []

            except Exception as exc:  # noqa: BLE001
                print(f"[blf_pipeline] failed to parse {spark_path!r}: {exc}")

        if rows:
            yield pd.DataFrame(rows)


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
        .option("pathGlobFilter", "*.{blf,mf4,mdf}")
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
        # mapInPandas yields rows as they are parsed — no full-file buffering.
        .mapInPandas(_parse_blf_batch, schema=_BRONZE_OUTPUT_SCHEMA)
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
        *[F.upper(F.lpad(F.hex(F.substring(b, i, 1)), 2, "0")) for i in range(1, 7)],
    )


# ── silver layer — decoded CAN signals ───────────────────────────────────────

# Per-executor cache: csv_path → vector_blf.CanSignalDb | None
_SIGNAL_DB_CACHE: dict = {}


def _load_signal_db(csv_path: str):
    if csv_path in _SIGNAL_DB_CACHE:
        return _SIGNAL_DB_CACHE[csv_path]
    import vector_blf  # noqa: PLC0415

    db = None
    if csv_path:
        db = vector_blf.CanSignalDb(csv_path)
    _SIGNAL_DB_CACHE[csv_path] = db
    return db


@pandas_udf(_SIGNAL_RESULT_SCHEMA)
def _decode_signals(
    can_ids: pd.Series,
    data_col: pd.Series,
    paths: pd.Series,
    long_headers: pd.Series,
) -> pd.Series:
    """Decode all matching CAN signals for each (can_id, data) row.

    `paths` and `long_headers` carry per-batch constants via F.lit so the
    values are available on executors without relying on driver-side state.
    Container-frame CAN IDs (those with a pdu_id column in the CSV) are
    demultiplexed via decode_container; regular frames use decode.
    """
    csv_path = paths.iloc[0] if len(paths) else ""
    use_long = bool(long_headers.iloc[0]) if len(long_headers) else False
    db = _load_signal_db(csv_path)

    result = []
    for can_id, data in zip(can_ids, data_col):
        decoded = []
        if data is not None and db is not None:
            mid = int(can_id)
            if db.is_container(mid):
                pairs = db.decode_container(mid, bytes(data), use_long)
            else:
                pairs = db.decode(mid, bytes(data))
            decoded = [{"signal_name": name, "signal_value": value} for name, value in pairs]
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
                F.lit(CONTAINER_LONG_HEADER),
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
    out.extend(
        [
            {
                "signal_name": "tcp.src_port",
                "signal_value": float((data[0] << 8) | data[1]),
                "signal_str": None,
            },
            {
                "signal_name": "tcp.dst_port",
                "signal_value": float((data[2] << 8) | data[3]),
                "signal_str": None,
            },
            {
                "signal_name": "tcp.flags",
                "signal_value": float(data[13] & 0x3F),
                "signal_str": None,
            },
        ]
    )


def _eth_parse_udp(data: bytes, out: list) -> None:
    if len(data) < 8:
        return
    out.extend(
        [
            {
                "signal_name": "udp.src_port",
                "signal_value": float((data[0] << 8) | data[1]),
                "signal_str": None,
            },
            {
                "signal_name": "udp.dst_port",
                "signal_value": float((data[2] << 8) | data[3]),
                "signal_str": None,
            },
            {
                "signal_name": "udp.payload_bytes",
                "signal_value": float(((data[4] << 8) | data[5]) - 8),
                "signal_str": None,
            },
        ]
    )


def _eth_parse_ipv4(data: bytes, out: list) -> None:
    if len(data) < 20:
        return
    ihl = (data[0] & 0x0F) * 4
    protocol = data[9]
    out.extend(
        [
            {
                "signal_name": "ip.protocol",
                "signal_value": float(protocol),
                "signal_str": None,
            },
            {
                "signal_name": "ip.ttl",
                "signal_value": float(data[8]),
                "signal_str": None,
            },
            {
                "signal_name": "ip.total_len",
                "signal_value": float((data[2] << 8) | data[3]),
                "signal_str": None,
            },
            {
                "signal_name": "ip.src",
                "signal_value": None,
                "signal_str": ".".join(str(b) for b in data[12:16]),
            },
            {
                "signal_name": "ip.dst",
                "signal_value": None,
                "signal_str": ".".join(str(b) for b in data[16:20]),
            },
        ]
    )
    transport = data[ihl:] if ihl <= len(data) else b""
    if protocol == _IPPROTO_TCP:
        _eth_parse_tcp(transport, out)
    elif protocol == _IPPROTO_UDP:
        _eth_parse_udp(transport, out)


def _eth_parse_ipv6(data: bytes, out: list) -> None:
    if len(data) < 40:
        return
    next_header = data[6]
    out.extend(
        [
            {
                "signal_name": "ip.protocol",
                "signal_value": float(next_header),
                "signal_str": None,
            },
            {
                "signal_name": "ip.hop_limit",
                "signal_value": float(data[7]),
                "signal_str": None,
            },
            {
                "signal_name": "ip.src",
                "signal_value": None,
                "signal_str": ":".join(f"{(data[8 + i * 2] << 8 | data[9 + i * 2]):04x}" for i in range(8)),
            },
            {
                "signal_name": "ip.dst",
                "signal_value": None,
                "signal_str": ":".join(f"{(data[24 + i * 2] << 8 | data[25 + i * 2]):04x}" for i in range(8)),
            },
        ]
    )
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


# ── silver layer — MF4 scalar signals ────────────────────────────────────────


@dlt.table(
    name="blf_silver_mf4_signals",
    comment=(
        "MF4 scalar signal values from channel groups read from .mf4 / .mdf files. "
        "Long format: one row per sample. "
        "mf4_group: acquisition group / channel group name. "
        "mf4_name: channel name. mf4_value: physical value. mf4_unit: unit string."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def blf_silver_mf4_signals():
    return (
        dlt.read_stream("blf_bronze")
        .filter(F.col("message_type") == "MF4_SIGNAL")
        .select(
            "_source_file",
            "_ingested_at",
            "timestamp_ns",
            (F.col("timestamp_ns") / 1e9).alias("timestamp_s"),
            F.col("mf4_group").alias("group_name"),
            F.col("mf4_name").alias("channel_name"),
            F.col("mf4_value").alias("value"),
            F.col("mf4_unit").alias("unit"),
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

# Per-executor cache: csv_path → vector_blf.SomeIpSignalDb | None
_SOMEIP_SIGNAL_DB_CACHE: dict = {}


def _load_someip_signal_db(csv_path: str):
    """Load SOME/IP signal CSV; returns a vector_blf.SomeIpSignalDb or None."""
    if csv_path in _SOMEIP_SIGNAL_DB_CACHE:
        return _SOMEIP_SIGNAL_DB_CACHE[csv_path]
    import vector_blf  # noqa: PLC0415

    db = None
    if csv_path:
        db = vector_blf.SomeIpSignalDb(csv_path)
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
        src_ip,
        dst_ip,
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
    src_ip = ":".join(f"{(data[8 + i * 2] << 8 | data[9 + i * 2]):04x}" for i in range(8))
    dst_ip = ":".join(f"{(data[24 + i * 2] << 8 | data[25 + i * 2]):04x}" for i in range(8))
    udp = data[40:]
    if len(udp) < 8:
        return None
    return (
        src_ip,
        dst_ip,
        (udp[0] << 8) | udp[1],
        (udp[2] << 8) | udp[3],
        udp[8:],
    )


def _someip_parse_header(payload: bytes):
    """Validate SOME/IP header; return a dict with all header fields or None."""
    if len(payload) < 16:
        return None
    if payload[12] != _SOMEIP_PROTOCOL_VERSION:
        return None
    msg_type = payload[14]
    if msg_type not in _SOMEIP_VALID_MSG_TYPES:
        return None
    service_id = (payload[0] << 8) | payload[1]
    if service_id == _SOMEIP_SD_SERVICE_ID:  # skip SOME/IP-SD
        return None
    method_id = (payload[2] << 8) | payload[3]
    length = int.from_bytes(payload[4:8], "big")
    if length < 8 or 8 + length > len(payload):
        return None
    return {
        "service_id": service_id,
        "method_id": method_id,
        "length": length,
        "client_id": (payload[8] << 8) | payload[9],
        "session_id": (payload[10] << 8) | payload[11],
        "protocol_version": payload[12],
        "interface_version": payload[13],
        "msg_type": msg_type,
        "return_code": payload[15],
        "app_payload": payload[16 : 8 + length],
    }


@pandas_udf(_SOMEIP_ROW_SCHEMA)
def _parse_someip(ether_types: pd.Series, data_col: pd.Series) -> pd.Series:
    """Parse SOME/IP messages from Ethernet UDP payloads.

    Supports AUTOSAR Container PDU Transport: loops over the UDP payload and
    parses all back-to-back SOME/IP PDUs, advancing by 8 + length bytes after
    each valid message.  Stops on the first invalid/unrecognised header.
    """
    result = []
    for ether_type, data in zip(ether_types, data_col):
        rows: list = []
        if data is not None:
            raw = bytes(data)
            udp_info = None
            if ether_type == _ETHERTYPE_IPV4:
                udp_info = _someip_strip_ipv4_udp(raw)
            elif ether_type == _ETHERTYPE_IPV6:
                udp_info = _someip_strip_ipv6_udp(raw)
            if udp_info is not None:
                src_ip, dst_ip, src_port, dst_port, udp_payload = udp_info
                pos = 0
                while pos < len(udp_payload):
                    hdr = _someip_parse_header(udp_payload[pos:])
                    if hdr is None:
                        break
                    rows.append(
                        {
                            "src_ip": src_ip,
                            "dst_ip": dst_ip,
                            "udp_src_port": src_port,
                            "udp_dst_port": dst_port,
                            "someip_service_id": hdr["service_id"],
                            "someip_method_id": hdr["method_id"],
                            "someip_length": hdr["length"],
                            "someip_client_id": hdr["client_id"],
                            "someip_session_id": hdr["session_id"],
                            "someip_protocol_version": hdr["protocol_version"],
                            "someip_interface_version": hdr["interface_version"],
                            "someip_msg_type": hdr["msg_type"],
                            "someip_return_code": hdr["return_code"],
                            "payload": hdr["app_payload"],
                        }
                    )
                    pos += 8 + hdr["length"]
        result.append(rows)
    return pd.Series(result)


@dlt.table(
    name="blf_silver_someip",
    comment=(
        "SOME/IP messages parsed from Ethernet UDP frames. "
        "One row per SOME/IP message (header validated; SOME/IP-SD excluded). "
        "All 16-byte header fields are exposed as columns; app payload in payload/payload_hex."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_someip():
    return (
        dlt.read_stream("blf_silver_eth")
        .withColumn("_someip", _parse_someip(F.col("ether_type"), F.col("data")))
        .filter(F.size("_someip") > 0)
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
            F.explode("_someip").alias("_s"),
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
            F.col("_s.src_ip"),
            F.col("_s.dst_ip"),
            F.col("_s.udp_src_port"),
            F.col("_s.udp_dst_port"),
            F.col("_s.someip_service_id"),
            F.format_string("0x%04X", F.col("_s.someip_service_id")).alias("someip_service_id_hex"),
            F.col("_s.someip_method_id"),
            F.format_string("0x%04X", F.col("_s.someip_method_id")).alias("someip_method_id_hex"),
            F.col("_s.someip_length"),
            F.col("_s.someip_client_id"),
            F.col("_s.someip_session_id"),
            F.col("_s.someip_protocol_version"),
            F.col("_s.someip_interface_version"),
            F.col("_s.someip_msg_type"),
            F.col("_s.someip_return_code"),
            F.hex(F.col("_s.payload")).alias("payload_hex"),
            F.col("_s.payload"),
        )
    )


# ── silver layer — SOME/IP application signals ───────────────────────────────


@pandas_udf(_SIGNAL_RESULT_SCHEMA)
def _decode_someip_signals(
    service_ids: pd.Series,
    method_ids: pd.Series,
    payloads: pd.Series,
    paths: pd.Series,
) -> pd.Series:
    """Decode SOME/IP application signals from already-parsed SOME/IP payloads."""
    csv_path = paths.iloc[0] if len(paths) else ""
    db = _load_someip_signal_db(csv_path)
    result = []
    for service_id, method_id, payload in zip(service_ids, method_ids, payloads):
        decoded = []
        if payload is not None and db is not None:
            decoded = [
                {"signal_name": name, "signal_value": value}
                for name, value in db.decode(int(service_id), int(method_id), bytes(payload))
            ]
        result.append(decoded)
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
        dlt.read_stream("blf_silver_someip")
        .withColumn(
            "_signals",
            _decode_someip_signals(
                F.col("someip_service_id"),
                F.col("someip_method_id"),
                F.col("payload"),
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
            "src_ip",
            "dst_ip",
            "udp_src_port",
            "udp_dst_port",
            "someip_service_id",
            "someip_service_id_hex",
            "someip_method_id",
            "someip_method_id_hex",
            "someip_msg_type",
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
            "src_ip",
            "dst_ip",
            "udp_src_port",
            "udp_dst_port",
            "someip_service_id",
            "someip_service_id_hex",
            "someip_method_id",
            "someip_method_id_hex",
            "someip_msg_type",
            F.col("_sig.signal_name"),
            F.col("_sig.signal_value"),
        )
    )


# ── silver layer — UDS diagnostics (CAN ISO-TP + DoIP) ───────────────────────
#
# UDS (ISO 14229) messages are extracted from two transport layers:
#
#   • CAN  — ISO-TP (ISO 15765-2) reassembly over CAN / CAN-FD / CAN-FD64
#            frames.  Single-frame and multi-frame (FF + CF*) PDUs are handled.
#            Reassembly is per (channel, can_id) conversation within each file.
#            Sequence errors cause the in-progress PDU to be discarded.
#
#   • DOIP — DoIP (ISO 13400-2) DiagMessage (payload_type=0x8001) payloads
#            carried over TCP port 13400.  Multiple DoIP frames per TCP
#            segment are supported; fragmented TCP segments are not reassembled.
#
# UDS frame classification:
#   Request          — first byte 0x10-0x3E (no 0x40 bit set)
#   PositiveResponse — first byte = requested SID | 0x40
#   NegativeResponse — first byte 0x7F, second byte = requested SID, third = NRC

_DIAG_RAW_SCHEMA = StructType(
    [
        StructField("_source_file", StringType(), nullable=False),
        StructField("_file_mtime", TimestampType()),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("timestamp_ns", LongType(), nullable=False),
        StructField("channel", IntegerType()),
        StructField("can_id", LongType()),  # null for DOIP rows
        StructField("dir", ByteType()),
        StructField("transport", StringType(), nullable=False),  # "CAN" | "DOIP"
        StructField("doip_src_addr", IntegerType()),  # null for CAN rows
        StructField("doip_target_addr", IntegerType()),  # null for CAN rows
        StructField("uds_type", StringType(), nullable=False),
        StructField("service_id", IntegerType(), nullable=False),
        StructField("service_name", StringType()),
        StructField("nrc", IntegerType()),
        StructField("nrc_name", StringType()),
        StructField("data", BinaryType()),
    ]
)

_UDS_BATCH_SIZE = 1_000  # rows buffered before yielding from _parse_blf_uds_batch

_UDS_SERVICE_NAMES: dict = {
    0x10: "DiagnosticSessionControl",
    0x11: "EcuReset",
    0x14: "ClearDiagnosticInformation",
    0x19: "ReadDtcInformation",
    0x22: "ReadDataByIdentifier",
    0x23: "ReadMemoryByAddress",
    0x24: "ReadScalingDataByIdentifier",
    0x27: "SecurityAccess",
    0x28: "CommunicationControl",
    0x29: "Authentication",
    0x2A: "ReadDataByPeriodicIdentifier",
    0x2C: "DynamicallyDefineDataIdentifier",
    0x2E: "WriteDataByIdentifier",
    0x2F: "InputOutputControlByIdentifier",
    0x31: "RoutineControl",
    0x34: "RequestDownload",
    0x35: "RequestUpload",
    0x36: "TransferData",
    0x37: "RequestTransferExit",
    0x38: "RequestFileTransfer",
    0x3D: "WriteMemoryByAddress",
    0x3E: "TesterPresent",
    0x83: "AccessTimingParameter",
    0x84: "SecuredDataTransmission",
    0x85: "ControlDtcSetting",
    0x86: "ResponseOnEvent",
    0x87: "LinkControl",
}

_UDS_NRC_NAMES: dict = {
    0x10: "GeneralReject",
    0x11: "ServiceNotSupported",
    0x12: "SubFunctionNotSupported",
    0x13: "IncorrectMessageLengthOrInvalidFormat",
    0x14: "ResponseTooLong",
    0x21: "BusyRepeatRequest",
    0x22: "ConditionsNotCorrect",
    0x24: "RequestSequenceError",
    0x25: "NoResponseFromSubnetComponent",
    0x26: "FailurePreventsExecutionOfRequestedAction",
    0x31: "RequestOutOfRange",
    0x33: "SecurityAccessDenied",
    0x35: "InvalidKey",
    0x36: "ExceededNumberOfAttempts",
    0x37: "RequiredTimeDelayNotExpired",
    0x70: "UploadDownloadNotAccepted",
    0x71: "TransferDataSuspended",
    0x72: "GeneralProgrammingFailure",
    0x73: "WrongBlockSequenceCounter",
    0x78: "RequestCorrectlyReceivedResponsePending",
    0x7E: "SubFunctionNotSupportedInActiveSession",
    0x7F: "ServiceNotSupportedInActiveSession",
}


def _uds_parse_payload(payload: bytes) -> dict | None:
    """Parse a raw UDS payload into a record dict, or None if it is invalid."""
    if not payload:
        return None
    sid = payload[0]
    if sid == 0x7F:  # NegativeResponse
        if len(payload) < 3:
            return None
        req_sid = payload[1]
        nrc = payload[2]
        return {
            "uds_type": "NegativeResponse",
            "service_id": req_sid,
            "service_name": _UDS_SERVICE_NAMES.get(req_sid, f"Unknown_0x{req_sid:02X}"),
            "nrc": nrc,
            "nrc_name": _UDS_NRC_NAMES.get(nrc, f"Unknown_0x{nrc:02X}"),
            "data": bytes(payload[3:]),
        }
    elif sid & 0x40:  # PositiveResponse
        service_id = sid & ~0x40
        return {
            "uds_type": "PositiveResponse",
            "service_id": service_id,
            "service_name": _UDS_SERVICE_NAMES.get(service_id, f"Unknown_0x{service_id:02X}"),
            "nrc": None,
            "nrc_name": None,
            "data": bytes(payload[1:]),
        }
    else:  # Request
        return {
            "uds_type": "Request",
            "service_id": sid,
            "service_name": _UDS_SERVICE_NAMES.get(sid, f"Unknown_0x{sid:02X}"),
            "nrc": None,
            "nrc_name": None,
            "data": bytes(payload[1:]),
        }


def _isotp_sf_payload(data: bytes) -> bytes | None:
    """Return the UDS payload bytes from an ISO-TP Single Frame, or None."""
    if not data or (data[0] >> 4) != 0:
        return None
    sf_len = data[0] & 0x0F
    if sf_len == 0:  # Extended SF (CAN-FD): length in second byte
        if len(data) < 2:
            return None
        sf_len = data[1]
        return bytes(data[2 : 2 + sf_len]) if len(data) >= 2 + sf_len else None
    return bytes(data[1 : 1 + sf_len]) if len(data) >= 1 + sf_len else None


def _isotp_ff_info(data: bytes) -> tuple | None:
    """Return (total_length, initial_payload) from an ISO-TP First Frame, or None."""
    if len(data) < 2 or (data[0] >> 4) != 1:
        return None
    total_len = ((data[0] & 0x0F) << 8) | data[1]
    if total_len == 0:  # Extended FF (CAN-FD): 32-bit length at bytes 2-5
        if len(data) < 6:
            return None
        total_len = int.from_bytes(data[2:6], "big")
        return total_len, bytes(data[6:])
    return total_len, bytes(data[2:])


def _parse_uds_from_can(local_path: str, source_file: str):
    """Generator: read CAN/CAN-FD frames, reassemble ISO-TP PDUs, yield UDS record dicts."""
    import vector_blf  # noqa: PLC0415

    conversations: dict = {}  # (channel, can_id) -> reassembly state dict

    try:
        for obj in vector_blf.Reader(local_path, types=["Can", "CanFd", "CanFd64"]):
            msg = obj.message
            assert isinstance(msg, (vector_blf.Can, vector_blf.CanFd, vector_blf.CanFd64))
            data = bytes(msg.data)
            if not data:
                continue
            key = (int(msg.channel), int(msg.id))
            nibble = (data[0] >> 4) & 0xF

            if nibble == 0:  # Single Frame
                payload = _isotp_sf_payload(data)
                if payload:
                    rec = _uds_parse_payload(payload)
                    if rec:
                        yield dict(
                            timestamp_ns=obj.timestamp_ns,
                            channel=key[0],
                            can_id=key[1],
                            dir=msg.dir,
                            transport="CAN",
                            doip_src_addr=None,
                            doip_target_addr=None,
                            _source_file=source_file,
                            **rec,
                        )
                conversations.pop(key, None)

            elif nibble == 1:  # First Frame — begin reassembly
                ff = _isotp_ff_info(data)
                if ff:
                    total_len, initial = ff
                    conversations[key] = {
                        "total_len": total_len,
                        "buf": bytearray(initial),
                        "next_sn": 1,
                        "ts": obj.timestamp_ns,
                        "dir": msg.dir,
                    }

            elif nibble == 2:  # Consecutive Frame
                state = conversations.get(key)
                if state is not None:
                    sn = data[0] & 0x0F
                    if sn == state["next_sn"] % 16:
                        state["buf"].extend(data[1:])
                        state["next_sn"] += 1
                        if len(state["buf"]) >= state["total_len"]:
                            payload = bytes(state["buf"][: state["total_len"]])
                            rec = _uds_parse_payload(payload)
                            if rec:
                                yield dict(
                                    timestamp_ns=state["ts"],
                                    channel=key[0],
                                    can_id=key[1],
                                    dir=state["dir"],
                                    transport="CAN",
                                    doip_src_addr=None,
                                    doip_target_addr=None,
                                    _source_file=source_file,
                                    **rec,
                                )
                            del conversations[key]
                    else:
                        del conversations[key]  # sequence error — discard PDU

            # nibble == 3: Flow Control — no action needed

    except Exception as exc:  # noqa: BLE001
        print(f"[blf_pipeline] CAN/UDS parse failed for {source_file!r}: {exc}")


def _doip_strip_tcp(ether_type: int, data: bytes) -> tuple | None:
    """Strip IP + TCP headers; return (src_port, dst_port, tcp_payload) or None."""
    if ether_type == _ETHERTYPE_IPV4:
        if len(data) < 20 or data[9] != _IPPROTO_TCP:
            return None
        ihl = (data[0] & 0x0F) * 4
        tcp = data[ihl:]
    elif ether_type == _ETHERTYPE_IPV6:
        if len(data) < 40 or data[6] != _IPPROTO_TCP:
            return None
        tcp = data[40:]
    else:
        return None
    if len(tcp) < 20:
        return None
    tcp_hdr_len = ((tcp[12] >> 4) & 0xF) * 4
    return (tcp[0] << 8) | tcp[1], (tcp[2] << 8) | tcp[3], tcp[tcp_hdr_len:]


def _doip_diag_messages(tcp_payload: bytes):
    """Yield (src_addr, target_addr, uds_payload) for each DoIP DiagMessage in *tcp_payload*.

    Handles multiple back-to-back DoIP frames within a single TCP segment.
    """
    pos = 0
    while pos + 8 <= len(tcp_payload):
        if (tcp_payload[pos] ^ tcp_payload[pos + 1]) != 0xFF:  # version/inverse check
            break
        ptype = (tcp_payload[pos + 2] << 8) | tcp_payload[pos + 3]
        plen = int.from_bytes(tcp_payload[pos + 4 : pos + 8], "big")
        if pos + 8 + plen > len(tcp_payload):
            break
        if ptype == _DOIP_PAYLOAD_TYPE_DIAG and plen >= 4:
            pl = tcp_payload[pos + 8 : pos + 8 + plen]
            yield (pl[0] << 8) | pl[1], (pl[2] << 8) | pl[3], pl[4:]
        pos += 8 + plen


def _parse_uds_from_doip(local_path: str, source_file: str):
    """Generator: read Ethernet frames, parse DoIP DiagMessages over TCP 13400, yield UDS record dicts."""
    import vector_blf  # noqa: PLC0415

    try:
        for obj in vector_blf.Reader(local_path, types=["Ethernet", "EthernetEx"]):
            msg = obj.message
            assert isinstance(msg, (vector_blf.Ethernet, vector_blf.EthernetEx))
            tcp = _doip_strip_tcp(msg.ether_type, bytes(msg.data))
            if tcp is None:
                continue
            src_port, dst_port, tcp_payload = tcp
            if src_port != _DOIP_PORT and dst_port != _DOIP_PORT:
                continue
            for src_addr, tgt_addr, uds_payload in _doip_diag_messages(tcp_payload):
                rec = _uds_parse_payload(uds_payload)
                if rec:
                    yield dict(
                        timestamp_ns=obj.timestamp_ns,
                        channel=int(msg.channel),
                        can_id=None,
                        dir=msg.dir,
                        transport="DOIP",
                        doip_src_addr=src_addr,
                        doip_target_addr=tgt_addr,
                        _source_file=source_file,
                        **rec,
                    )

    except Exception as exc:  # noqa: BLE001
        print(f"[blf_pipeline] DoIP/UDS parse failed for {source_file!r}: {exc}")


def _parse_blf_uds_batch(iterator):
    """mapInPandas worker: parse UDS diagnostic messages from BLF files.

    Processes both transports via generators, merges sorted by timestamp, and
    yields in chunks of _UDS_BATCH_SIZE to avoid large intermediate allocations.
    ISO-TP reassembly state is local to each file invocation.
    """
    import itertools
    from datetime import datetime, timezone

    for batch_df in iterator:
        now = datetime.now(timezone.utc)
        for _, row in batch_df.iterrows():
            spark_path = str(row["_source_file"])
            file_mtime = row.get("_file_mtime")
            local = _local_path(spark_path)
            records = sorted(
                itertools.chain(
                    _parse_uds_from_can(local, spark_path),
                    _parse_uds_from_doip(local, spark_path),
                ),
                key=lambda r: r["timestamp_ns"],
            )
            rows: list[dict] = []
            for r in records:
                rows.append(
                    {
                        "_source_file": spark_path,
                        "_file_mtime": file_mtime,
                        "_ingested_at": now,
                        "timestamp_ns": r["timestamp_ns"],
                        "channel": r.get("channel"),
                        "can_id": r.get("can_id"),
                        "dir": r.get("dir"),
                        "transport": r["transport"],
                        "doip_src_addr": r.get("doip_src_addr"),
                        "doip_target_addr": r.get("doip_target_addr"),
                        "uds_type": r["uds_type"],
                        "service_id": r["service_id"],
                        "service_name": r.get("service_name"),
                        "nrc": r.get("nrc"),
                        "nrc_name": r.get("nrc_name"),
                        "data": r.get("data", b""),
                    }
                )
                if len(rows) >= _UDS_BATCH_SIZE:
                    yield pd.DataFrame(rows)
                    rows = []
            if rows:
                yield pd.DataFrame(rows)


@dlt.table(
    name="blf_silver_diag",
    comment=(
        "UDS (ISO 14229) diagnostic messages merged from two transport layers. "
        "transport='CAN': reassembled from ISO-TP (ISO 15765-2) over CAN / CAN-FD / CAN-FD64. "
        "transport='DOIP': extracted from DoIP DiagMessage (payload_type=0x8001) over TCP 13400. "
        "uds_type: 'Request', 'PositiveResponse', or 'NegativeResponse'. "
        "can_id / can_id_hex are null for DOIP rows. "
        "doip_src_addr / doip_target_addr (DoIP logical addresses) are null for CAN rows. "
        "One row per complete UDS PDU."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["transport", "uds_type"],
)
def blf_silver_diag():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.{blf,mf4,mdf}")
        .option(
            "cloudFiles.schemaLocation",
            f"{SOURCE_PATH}/_autoloader_schema_diag",
        )
        .load(SOURCE_PATH)
        .select(
            F.col("path").alias("_source_file"),
            F.col("modificationTime").alias("_file_mtime"),
        )
        # mapInPandas yields rows as they are parsed — no full-file buffering.
        .mapInPandas(_parse_blf_uds_batch, schema=_DIAG_RAW_SCHEMA)
        .select(
            "_source_file",
            "_ingested_at",
            "timestamp_ns",
            (F.col("timestamp_ns") / 1e9).alias("timestamp_s"),
            "transport",
            "channel",
            "can_id",
            F.when(F.col("can_id").isNotNull(), F.format_string("0x%08X", F.col("can_id"))).alias("can_id_hex"),
            "doip_src_addr",
            "doip_target_addr",
            _DIR_LABEL.alias("dir"),
            "uds_type",
            "service_id",
            F.format_string("0x%02X", F.col("service_id")).alias("service_id_hex"),
            "service_name",
            "nrc",
            F.when(F.col("nrc").isNotNull(), F.format_string("0x%02X", F.col("nrc"))).alias("nrc_hex"),
            "nrc_name",
            F.hex("data").alias("data_hex"),
            "data",
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
    can = dlt.read_stream("blf_silver_can_signals").select(
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
    eth = dlt.read_stream("blf_silver_eth_signals").select(
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
    someip = dlt.read_stream("blf_silver_someip_signals").select(
        "_source_file",
        "_ingested_at",
        "message_type",
        "timestamp_ns",
        "timestamp_s",
        "channel",
        F.lit("SOMEIP").alias("signal_source"),
        F.concat_ws("/", "someip_service_id_hex", "someip_method_id_hex").alias("message_id_str"),
        "dir",
        "signal_name",
        "signal_value",
        _null_str.alias("signal_str"),
    )
    # signal_name is not globally unique; consumers must include signal_source in WHERE.
    return can.union(eth).union(someip)
