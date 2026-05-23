"""
Databricks Delta Live Tables pipeline — Vector BLF ingestion
=============================================================

Reads *.blf files from a Unity Catalog Volume using the vector_blf Rust
extension (PyO3/maturin), and writes structured Delta tables.

Layer layout
------------
  blf_bronze             streaming table — one row per log object, all message types
  blf_silver_can         streaming table — CAN / CAN-FD / CAN-FD64 messages
  blf_silver_eth         streaming table — Ethernet / EthernetEx messages
  blf_silver_can_signals streaming table — decoded physical signal values (long format)

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
       blf.source_path    /Volumes/mycat/myschema/blf_raw
       blf.target_catalog mycat            (optional, default: main)
       blf.target_schema  automotive       (optional, default: blf)
       blf.signals_path   /Volumes/mycat/myschema/signals.csv  (optional)

   Signal CSV format (header required):
       message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
       0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
       0x200,BrakeForce,7,12,Motorola,true,0.1,-100.0

   message_id accepts hex (0x…) or decimal.  byte_order is Intel or Motorola
   (case-insensitive).  is_signed accepts true/false or 1/0.
   If blf.signals_path is not set, blf_silver_can_signals will be empty.

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
            F.explode("_records").alias("_r"),
        )
        .select(
            "_source_file",
            "_file_mtime",
            "_file_size_bytes",
            "_ingested_at",
            "_r.*",   # flatten all record fields to top-level columns
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
            F.format_string("0x%X", F.col("can_id")).alias("can_id_hex"),
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

_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType([
        StructField("signal_name", StringType(), nullable=False),
        StructField("signal_value", DoubleType(), nullable=False),
    ])
)

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
                    mid_s = row["message_id"].strip()
                    mid = int(mid_s, 16) if mid_s.lower().startswith("0x") else int(mid_s)
                    signals.append({
                        "message_id": mid,
                        "signal_name": row["signal_name"].strip(),
                        "start_bit": int(row["start_bit"]),
                        "bit_length": int(row["bit_length"]),
                        "byte_order": row["byte_order"].strip().lower(),
                        "is_signed": row["is_signed"].strip().lower() in ("true", "1"),
                        "scale": float(row["scale"]),
                        "offset": float(row["offset"]),
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
            F.explode("_signals").alias("_s"),
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
            F.col("_s.signal_name"),
            F.col("_s.signal_value"),
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
