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
  blf_silver_can_container_pdus streaming table — raw I-PDU payloads demuxed from container CAN-FD frames
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
       # -> target/wheels/vector_blf-0.1.0-cp3XX-cp3XX-manylinux_2_17_x86_64.whl

2. Upload to a Volume accessible from the cluster:

       databricks fs cp target/wheels/vector_blf-*.whl \
           /Volumes/<catalog>/<schema>/<vol>/wheels/

3. In the DLT pipeline settings -> Libraries, add:
       /Volumes/<catalog>/<schema>/<vol>/wheels/vector_blf-*.whl

4. Set pipeline parameters (Edit -> Advanced -> Parameters):
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

   message_id accepts hex (0x...) or decimal.  byte_order is Intel or Motorola
   (case-insensitive).  is_signed accepts true/false or 1/0.
   If blf.signals_path is not set, blf_silver_can_container_pdus and
   blf_silver_can_signals will both be empty.

   SOME/IP signal CSV format (header required):
       service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
       0x0064,0x0001,MotorSpeed_rpm,0,16,Intel,false,1.0,0.0
       0x0064,0x0001,MotorTorque_Nm,16,16,Intel,true,0.1,0.0

   service_id and method_id accept hex (0x...) or decimal.
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
from pyspark.sql.functions import pandas_udf
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
        # VLAN 802.1Q / 802.1AD tag (null when not tagged)
        StructField("vlan_tpid", IntegerType()),
        StructField("vlan_cos", IntegerType()),
        StructField("vlan_id", IntegerType()),
        # MF4 scalar signal extras
        StructField("mf4_group", StringType()),
        StructField("mf4_name", StringType()),
        StructField("mf4_value", DoubleType()),
        StructField("mf4_unit", StringType()),
        # UDS diagnostic extras (null for non-UDS rows)
        StructField("transport", StringType()),  # "CAN" | "DOIP"
        StructField("doip_src_addr", IntegerType()),
        StructField("doip_target_addr", IntegerType()),
        StructField("uds_type", StringType()),  # "Request" | "PositiveResponse" | "NegativeResponse"
        StructField("service_id", IntegerType()),
        StructField("service_name", StringType()),
        StructField("nrc", IntegerType()),
        StructField("nrc_name", StringType()),
    ]
)

# Records buffered per executor before yielding a DataFrame; limits peak heap per file.
_PARSE_BATCH_SIZE = 50_000

# ── signal output schemas ────────────────────────────────────────────────────

_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("signal_name", StringType(), nullable=False),
            StructField("signal_value", DoubleType(), nullable=False),
        ]
    )
)

_PDU_RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("pdu_id", LongType(), nullable=False),
            StructField("pdu_payload", BinaryType(), nullable=False),
        ]
    )
)

_ETH_SIGNAL_RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("signal_name", StringType(), nullable=False),
            StructField("signal_value", DoubleType()),  # numeric fields (ports, TTL, ...)
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


def _parse_blf_batch(iterator):
    """mapInPandas worker: parse BLF files and yield record rows iteratively.

    Each input DataFrame batch contains one or more file-path rows.  Records are
    emitted in chunks of _PARSE_BATCH_SIZE, so peak heap is proportional to that
    constant rather than to the file size.

    Uses read_batch() for columnar output (avoids per-object Python class
    allocations) and performs ISO-TP reassembly and DoIP extraction inline so
    UDS diagnostic rows are emitted in the same single-pass worker — no second
    file read for blf_silver_diag.
    """
    from datetime import datetime, timezone

    import vector_blf

    for batch_df in iterator:
        now = datetime.now(timezone.utc)

        for _, row in batch_df.iterrows():
            spark_path = str(row["_source_file"])
            file_mtime = row.get("_file_mtime")
            file_size = row.get("_file_size_bytes")
            local = _local_path(spark_path)

            # per-file ISO-TP state: (channel, can_id) -> reassembler
            isotp_reassemblers: dict = {}
            isotp_ff_ts: dict = {}  # (channel, can_id) -> First Frame timestamp_ns
            uds_rows: list = []

            try:
                reader = vector_blf.Reader(
                    local,
                    types=["Can", "CanFd", "CanFd64", "Ethernet", "EthernetEx", "Mf4Signal"],
                )
                while True:
                    batch = reader.read_batch(_PARSE_BATCH_SIZE)
                    if batch is None:
                        break

                    n = len(batch["timestamp_ns"])
                    null_n = [None] * n

                    # attach metadata and null-out diag columns
                    batch["_source_file"] = [spark_path] * n
                    batch["_file_mtime"] = [file_mtime] * n
                    batch["_file_size_bytes"] = [file_size] * n
                    batch["_ingested_at"] = [now] * n
                    batch["transport"] = null_n
                    batch["doip_src_addr"] = null_n
                    batch["doip_target_addr"] = null_n
                    batch["uds_type"] = null_n
                    batch["service_id"] = null_n
                    batch["service_name"] = null_n
                    batch["nrc"] = null_n
                    batch["nrc_name"] = null_n

                    yield pd.DataFrame(batch)

                    # inline UDS extraction — iterate over the batch columns
                    msg_types = batch["message_type"]
                    ts_col = batch["timestamp_ns"]
                    channel_col = batch["channel"]
                    can_id_col = batch["can_id"]
                    dir_col = batch["dir"]
                    data_col = batch["data"]
                    ether_type_col = batch["ether_type"]

                    for i in range(n):
                        mt = msg_types[i]
                        ts = ts_col[i]

                        if mt in ("CAN", "CAN_FD", "CAN_FD64"):
                            data = data_col[i]
                            if not data:
                                continue
                            data_bytes = bytes(data)
                            key = (channel_col[i], can_id_col[i])

                            nibble = (data_bytes[0] >> 4) & 0xF
                            if nibble == 1:  # First Frame -> record its timestamp
                                isotp_ff_ts[key] = ts

                            if key not in isotp_reassemblers:
                                isotp_reassemblers[key] = vector_blf.IsoTpReassembler()
                            result = isotp_reassemblers[key].push(data_bytes)
                            if result is None:
                                continue

                            uds_type, svc_id, svc_name, nrc, nrc_name, uds_data = result
                            uds_ts = ts if nibble == 0 else isotp_ff_ts.get(key, ts)
                            uds_rows.append(
                                {
                                    "_source_file": spark_path,
                                    "_file_mtime": file_mtime,
                                    "_file_size_bytes": None,
                                    "_ingested_at": now,
                                    "timestamp_ns": uds_ts,
                                    "message_type": "UDS_CAN",
                                    "channel": channel_col[i],
                                    "can_id": can_id_col[i],
                                    "is_ext_id": None,
                                    "dir": dir_col[i],
                                    "rtr": None,
                                    "dlc": None,
                                    "data": bytes(uds_data),
                                    "fdf": None,
                                    "brs": None,
                                    "esi": None,
                                    "src_addr": None,
                                    "dst_addr": None,
                                    "ether_type": None,
                                    "vlan_tpid": None,
                                    "vlan_cos": None,
                                    "vlan_id": None,
                                    "mf4_group": None,
                                    "mf4_name": None,
                                    "mf4_value": None,
                                    "mf4_unit": None,
                                    "transport": "CAN",
                                    "doip_src_addr": None,
                                    "doip_target_addr": None,
                                    "uds_type": uds_type,
                                    "service_id": svc_id,
                                    "service_name": svc_name,
                                    "nrc": nrc,
                                    "nrc_name": nrc_name,
                                }
                            )

                        elif mt in ("ETH", "ETH_EX"):
                            data = data_col[i]
                            et = ether_type_col[i]
                            if data is None or et is None:
                                continue
                            for src_addr, target_addr, uds_bytes in vector_blf.parse_doip_diag(et, bytes(data)):
                                result = vector_blf.parse_uds(bytes(uds_bytes))
                                if result is None:
                                    continue
                                uds_type, svc_id, svc_name, nrc, nrc_name, uds_data = result
                                uds_rows.append(
                                    {
                                        "_source_file": spark_path,
                                        "_file_mtime": file_mtime,
                                        "_file_size_bytes": None,
                                        "_ingested_at": now,
                                        "timestamp_ns": ts,
                                        "message_type": "UDS_DOIP",
                                        "channel": channel_col[i],
                                        "can_id": None,
                                        "is_ext_id": None,
                                        "dir": dir_col[i],
                                        "rtr": None,
                                        "dlc": None,
                                        "data": bytes(uds_data),
                                        "fdf": None,
                                        "brs": None,
                                        "esi": None,
                                        "src_addr": None,
                                        "dst_addr": None,
                                        "ether_type": None,
                                        "vlan_tpid": None,
                                        "vlan_cos": None,
                                        "vlan_id": None,
                                        "mf4_group": None,
                                        "mf4_name": None,
                                        "mf4_value": None,
                                        "mf4_unit": None,
                                        "transport": "DOIP",
                                        "doip_src_addr": src_addr,
                                        "doip_target_addr": target_addr,
                                        "uds_type": uds_type,
                                        "service_id": svc_id,
                                        "service_name": svc_name,
                                        "nrc": nrc,
                                        "nrc_name": nrc_name,
                                    }
                                )

                        if len(uds_rows) >= _PARSE_BATCH_SIZE:
                            yield pd.DataFrame(uds_rows)
                            uds_rows = []

                if uds_rows:
                    yield pd.DataFrame(uds_rows)

            except Exception as exc:
                print(f"[blf_pipeline] failed to parse {spark_path!r}: {exc}")


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

# Per-executor cache: csv_path -> vector_blf.CanSignalDb | None
_SIGNAL_DB_CACHE: dict = {}


def _load_signal_db(csv_path: str):
    if csv_path in _SIGNAL_DB_CACHE:
        return _SIGNAL_DB_CACHE[csv_path]
    import vector_blf

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
    """Decode signals for non-container CAN frames.

    Container-frame CAN IDs are skipped here; they are handled by the
    blf_silver_can_container_pdus -> _decode_pdu_signals path.
    """
    csv_path = paths.iloc[0] if len(paths) else ""
    db = _load_signal_db(csv_path)

    result = []
    for can_id, data in zip(can_ids, data_col):
        decoded = []
        if data is not None and db is not None:
            mid = int(can_id)
            if not db.is_container(mid):
                decoded = [{"signal_name": name, "signal_value": value} for name, value in db.decode(mid, bytes(data))]
        result.append(decoded)

    return pd.Series(result)


@pandas_udf(_PDU_RESULT_SCHEMA)
def _extract_container_pdus(
    can_ids: pd.Series,
    data_col: pd.Series,
    paths: pd.Series,
    long_headers: pd.Series,
) -> pd.Series:
    """Demultiplex container-frame CAN IDs into raw (pdu_id, pdu_payload) pairs.

    Non-container rows return an empty array.
    """
    csv_path = paths.iloc[0] if len(paths) else ""
    use_long = bool(long_headers.iloc[0]) if len(long_headers) else False
    db = _load_signal_db(csv_path)

    result = []
    for can_id, data in zip(can_ids, data_col):
        pdus = []
        if data is not None and db is not None:
            mid = int(can_id)
            if db.is_container(mid):
                pdus = [
                    {"pdu_id": pdu_id, "pdu_payload": bytes(payload)}
                    for pdu_id, payload in db.extract_container_pdus(mid, bytes(data), use_long)
                ]
        result.append(pdus)

    return pd.Series(result)


@pandas_udf(_SIGNAL_RESULT_SCHEMA)
def _decode_pdu_signals(
    can_ids: pd.Series,
    pdu_ids: pd.Series,
    payloads: pd.Series,
    paths: pd.Series,
) -> pd.Series:
    """Decode signals from a single already-demuxed I-PDU row."""
    csv_path = paths.iloc[0] if len(paths) else ""
    db = _load_signal_db(csv_path)

    result = []
    for can_id, pdu_id, payload in zip(can_ids, pdu_ids, payloads):
        decoded = []
        if payload is not None and db is not None:
            decoded = [
                {"signal_name": name, "signal_value": value}
                for name, value in db.decode_pdu(int(can_id), int(pdu_id), bytes(payload))
            ]
        result.append(decoded)

    return pd.Series(result)


@dlt.table(
    name="blf_silver_can_container_pdus",
    comment=(
        "Raw I-PDU payloads demultiplexed from AUTOSAR Container PDU CAN-FD frames. "
        "One row per I-PDU; pdu_payload holds the raw bytes before signal decoding. "
        "Empty when blf.signals_path is not set."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_can_container_pdus():
    return (
        dlt.read_stream("blf_silver_can")
        .withColumn(
            "_pdus",
            _extract_container_pdus(
                F.col("can_id"),
                F.col("data"),
                F.lit(SIGNALS_PATH),
                F.lit(CONTAINER_LONG_HEADER),
            ),
        )
        .filter(F.size("_pdus") > 0)
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
            F.explode("_pdus").alias("_pdu"),
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
            F.col("_pdu.pdu_id").alias("pdu_id"),
            F.col("_pdu.pdu_payload").alias("pdu_payload"),
        )
    )


@dlt.table(
    name="blf_silver_can_signals",
    comment=(
        "Physical signal values decoded from CAN / CAN-FD / CAN-FD64 messages "
        "using the signal definition CSV at blf.signals_path. "
        "Long format: one row per (message, signal). "
        "Non-container frames are decoded directly from blf_silver_can; "
        "container I-PDUs are decoded from blf_silver_can_container_pdus."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["message_type"],
)
def blf_silver_can_signals():
    _sig_cols = [
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
    ]

    non_container = (
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
        .select(*_sig_cols)
    )

    container = (
        dlt.read_stream("blf_silver_can_container_pdus")
        .withColumn(
            "_signals",
            _decode_pdu_signals(
                F.col("can_id"),
                F.col("pdu_id"),
                F.col("pdu_payload"),
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
        .select(*_sig_cols)
    )

    return non_container.union(container)


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
            "vlan_tpid",
            "vlan_cos",
            "vlan_id",
            F.length("data").alias("payload_bytes"),
            "data",
        )
    )


# ── silver layer — parsed Ethernet payload signals ────────────────────────────


@pandas_udf(_ETH_SIGNAL_RESULT_SCHEMA)
def _parse_eth_payload(ether_types: pd.Series, data_col: pd.Series) -> pd.Series:
    """Extract IP/TCP/UDP header fields from the Ethernet payload as named signals."""
    import vector_blf

    result = []
    for ether_type, data in zip(ether_types, data_col):
        if data is not None:
            signals = vector_blf.parse_eth_payload_signals(int(ether_type), bytes(data))
        else:
            signals = []
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
#   16+     ...   Application payload  <- signal bits are extracted from here


# Per-executor cache: csv_path -> vector_blf.SomeIpSignalDb | None
_SOMEIP_SIGNAL_DB_CACHE: dict = {}


def _load_someip_signal_db(csv_path: str):
    """Load SOME/IP signal CSV; returns a vector_blf.SomeIpSignalDb or None."""
    if csv_path in _SOMEIP_SIGNAL_DB_CACHE:
        return _SOMEIP_SIGNAL_DB_CACHE[csv_path]
    import vector_blf

    db = None
    if csv_path:
        db = vector_blf.SomeIpSignalDb(csv_path)
    _SOMEIP_SIGNAL_DB_CACHE[csv_path] = db
    return db


@pandas_udf(_SOMEIP_ROW_SCHEMA)
def _parse_someip(ether_types: pd.Series, data_col: pd.Series) -> pd.Series:
    """Parse SOME/IP messages from Ethernet UDP payloads using the Rust parser.

    Supports AUTOSAR Container PDU Transport: loops over the UDP payload and
    parses all back-to-back SOME/IP PDUs.  IPv4 and IPv6 outer headers are
    both handled in Rust.
    """
    import vector_blf

    result = []
    for ether_type, data in zip(ether_types, data_col):
        if data is not None:
            rows = vector_blf.parse_someip_udp(int(ether_type), bytes(data))
        else:
            rows = []
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
# UDS rows are emitted by the bronze worker inline with the network rows.
# message_type="UDS_CAN" rows come from ISO-TP reassembly over CAN frames.
# message_type="UDS_DOIP" rows come from DoIP DiagMessage frames over TCP 13400.
#
# Both transports are handled by the Rust extension:
#   - IsoTpReassembler: stateful per (channel, can_id) conversation, CAN-FD extended frames
#   - parse_doip_diag:  stateless, strips IP+TCP headers, iterates DoIP frames


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
        dlt.read_stream("blf_bronze")
        .filter(F.col("message_type").isin("UDS_CAN", "UDS_DOIP"))
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
