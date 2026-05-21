"""
Databricks Delta Live Tables pipeline — Vector BLF ingestion
=============================================================

Reads *.blf files from a Unity Catalog Volume using the vector_blf Rust
extension (PyO3/maturin), and writes structured Delta tables.

Layer layout
------------
  blf_bronze        streaming table — one row per log object, all message types
  blf_silver_can    streaming table — CAN / CAN-FD / CAN-FD64 messages
  blf_silver_eth    streaming table — Ethernet / EthernetEx messages

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
       blf.source_path   /Volumes/mycat/myschema/blf_raw
       blf.target_catalog  mycat            (optional, default: main)
       blf.target_schema   automotive       (optional, default: blf)

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
