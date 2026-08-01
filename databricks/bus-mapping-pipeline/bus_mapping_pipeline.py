"""
Databricks Delta Live Tables pipeline -- CAN channel <-> bus name mapping
==========================================================================

Infers which physical bus (Powertrain, Body, Chassis, ...) each logged CAN
channel corresponds to, by comparing the message IDs actually observed on
each channel against the message IDs a bus is expected to carry given its
connected ECUs. Formerly a standalone script (databricks/infer-bus-mapping/);
this is the same set-similarity-matching algorithm wired into the pipeline
graph so it runs against every ingested log instead of one CSV at a time.

Separate from blf_ingestion (dlt_blf_pipeline.py): this pipeline reads
blf_silver_can as a plain batch Unity Catalog table and doesn't need the
vector_blf wheel. It's triggered rather than continuous because the
channel<->bus assignment is a per-file computation (recomputing it needs
every channel's observed set within one file at once) -- the same reason
blf_scenes is triggered.

The assignment is solved separately per _source_file, not once across the
whole corpus: channel wiring is consistent within a single log but is not
guaranteed to be consistent across logs (different vehicles, different
harness revisions, a channel renumbered between recording sessions), so
pooling every file's observations into one channel-level set would blur
together traffic that may not actually share a wire.

Layer layout
------------
  blf_bus_mapping_bus_native      one row per (bus_name, message_id) -- the expected set S_B
  blf_bus_mapping_channel_observed one row per (_source_file, channel, message_id) -- the observed set O_C, with count
  blf_bus_mapping_scores          one row per (_source_file, channel, bus_name) -- recall, precision, jaccard, F-beta score
  blf_bus_mapping                 one row per (_source_file, channel), its best-matching bus
  blf_gold_signals_bus            blf_gold_signals left-joined with blf_bus_mapping on (_source_file, channel)

Setup
-----
Set pipeline parameters (Edit -> Advanced -> Parameters):
    blf.target_catalog              main                                           (optional)
    blf.target_schema                blf                                           (optional)
    blf.bus_mapping_ecu_bus_path     /Volumes/mycat/myschema/signals/ecu_bus.csv
    blf.bus_mapping_message_sender_path /Volumes/mycat/myschema/signals/message_sender.csv

Every other blf.bus_mapping_* parameter has a default; see the constants below.

Reference CSV formats (header required)
----------------------------------------
ecu_bus.csv          columns: ecu, bus_name
                      A gateway ECU that bridges multiple buses gets one row per bus.

message_sender.csv   columns: message_id, sender_ecu
                      message_id accepts "0x100", "100" (hex) or a plain decimal integer.
                      Use the message's true originating ECU, not a relay/gateway, so a
                      relayed message doesn't pollute its own bus's native set.

This pipeline is triggered: run it manually (`databricks bundle run
blf_bus_mapping`) or on a schedule after blf_ingestion has caught up.
"""

from __future__ import annotations

import dlt
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── pipeline parameters ───────────────────────────────────────────────────────

TARGET_CATALOG = spark.conf.get("blf.target_catalog", "main")
TARGET_SCHEMA = spark.conf.get("blf.target_schema", "blf")
SOURCE_TABLE = spark.conf.get("blf.bus_mapping_source_table", "") or f"{TARGET_CATALOG}.{TARGET_SCHEMA}.blf_silver_can"
GOLD_SOURCE_TABLE = (
    spark.conf.get("blf.bus_mapping_gold_source_table", "") or f"{TARGET_CATALOG}.{TARGET_SCHEMA}.blf_gold_signals"
)

ECU_BUS_PATH = spark.conf.get("blf.bus_mapping_ecu_bus_path", "")
ECU_BUS_TABLE = spark.conf.get("blf.bus_mapping_ecu_bus_table", "")
MESSAGE_SENDER_PATH = spark.conf.get("blf.bus_mapping_message_sender_path", "")
MESSAGE_SENDER_TABLE = spark.conf.get("blf.bus_mapping_message_sender_table", "")

# beta > 1 weights recall over precision, which is the right bias here: a
# relayed (gateway) message inflates a channel's observed set and drags
# precision down, but it doesn't change what that channel's native traffic is.
BETA = float(spark.conf.get("blf.bus_mapping_beta", "2.0"))
MIN_SCORE = float(spark.conf.get("blf.bus_mapping_min_score", "0.3"))

# ── schemas ───────────────────────────────────────────────────────────────────

_BUS_NATIVE_SCHEMA = StructType(
    [
        StructField("bus_name", StringType(), nullable=False),
        StructField("message_id", LongType(), nullable=False),
    ]
)

_ASSIGN_SCHEMA = StructType(
    [
        StructField("_source_file", StringType(), nullable=False),
        StructField("channel", IntegerType(), nullable=False),
        StructField("matched_bus", StringType(), nullable=True),
        StructField("score", DoubleType(), nullable=False),
        StructField("recall", DoubleType(), nullable=False),
        StructField("precision", DoubleType(), nullable=False),
        StructField("jaccard", DoubleType(), nullable=False),
        StructField("note", StringType(), nullable=False),
    ]
)

# ── reference data loading (pure Python, external sources) ───────────────────


def _parse_message_id(raw: str) -> int | None:
    """Parse a message id written as '0x100', '100' (hex) or a decimal integer."""
    s = (raw or "").strip()
    if not s:
        return None
    try:
        if s.lower().startswith("0x"):
            return int(s, 16)
        try:
            return int(s, 10)
        except ValueError:
            return int(s, 16)
    except ValueError:
        return None


def _load_rows(path: str, table: str) -> list[dict]:
    """Read a reference CSV/table into plain dicts.

    Returns an empty list when neither source is configured or it can't be
    read, which degrades the mapping to "no candidates" rather than failing
    the pipeline -- the same tolerant convention blf_scenes uses for its rule
    table. Safe to call at module scope: both sources are external to this
    pipeline, so they are already fully materialized before the pipeline
    graph is resolved.
    """
    try:
        if table:
            frame = spark.table(table)
        elif path:
            frame = spark.read.option("header", True).csv(path)
        else:
            return []
        return [row.asDict() for row in frame.collect()]
    except Exception as exc:  # noqa: BLE001 -- any read failure means "no candidates"
        print(f"[bus_mapping_pipeline] could not load {path or table}: {exc}", flush=True)
        return []


def _build_bus_native_rows() -> list[tuple[str, int]]:
    """bus_name -> set(message_id), flattened to one row per pair.

    A bus's native set is the union of every message its connected ECUs send,
    found by joining ecu_bus (ecu -> bus_name) with message_sender
    (message_id -> sender_ecu) on the ECU name.
    """
    bus_to_ecus: dict[str, set[str]] = {}
    for row in _load_rows(ECU_BUS_PATH, ECU_BUS_TABLE):
        ecu = str(row.get("ecu") or "").strip()
        bus = str(row.get("bus_name") or "").strip()
        if ecu and bus:
            bus_to_ecus.setdefault(bus, set()).add(ecu)

    ecu_to_msgs: dict[str, set[int]] = {}
    for row in _load_rows(MESSAGE_SENDER_PATH, MESSAGE_SENDER_TABLE):
        mid = _parse_message_id(str(row.get("message_id") or ""))
        ecu = str(row.get("sender_ecu") or "").strip()
        if mid is not None and ecu:
            ecu_to_msgs.setdefault(ecu, set()).add(mid)

    pairs: set[tuple[str, int]] = set()
    for bus, ecus in bus_to_ecus.items():
        for ecu in ecus:
            for mid in ecu_to_msgs.get(ecu, ()):
                pairs.add((bus, mid))
    return sorted(pairs)


_BUS_NATIVE_ROWS = _build_bus_native_rows()
print(
    f"[bus_mapping_pipeline] loaded {len(_BUS_NATIVE_ROWS)} (bus, message_id) pairs "
    f"across {len({b for b, _ in _BUS_NATIVE_ROWS})} buses",
    flush=True,
)

# ── assignment maths (pure Python) ────────────────────────────────────────────


def _solve_assignment(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas worker: Hungarian-match channels to buses on the score matrix.

    Called once per _source_file (the groupBy key), so it receives every
    (channel, bus_name) score row for that one file as a single group --
    required since the assignment is one-to-one across the whole group, not
    per-row, but scoped to a file because channel wiring is not guaranteed to
    be consistent across files.
    """
    columns = [f.name for f in _ASSIGN_SCHEMA.fields]
    if pdf.empty:
        return pd.DataFrame(columns=columns)

    from scipy.optimize import linear_sum_assignment

    source_file = str(pdf["_source_file"].iloc[0])
    channels = sorted(pdf["channel"].unique().tolist())
    buses = sorted(pdf["bus_name"].unique().tolist())
    matrix = (
        pdf.pivot(index="channel", columns="bus_name", values="score")
        .reindex(index=channels, columns=buses)
        .fillna(0.0)
        .to_numpy()
    )
    detail = pdf.set_index(["channel", "bus_name"])

    row_ind, col_ind = linear_sum_assignment(-matrix)

    rows = []
    assigned = set()
    for r, c in zip(row_ind, col_ind):
        ch, bus = channels[r], buses[c]
        d = detail.loc[(ch, bus)]
        score = float(d["score"])
        note = "" if score >= MIN_SCORE else "score below threshold, needs review"
        rows.append(
            (source_file, int(ch), bus, score, float(d["recall"]), float(d["precision"]), float(d["jaccard"]), note)
        )
        assigned.add(ch)

    for ch in channels:
        if ch not in assigned:
            rows.append((source_file, int(ch), None, 0.0, 0.0, 0.0, 0.0, "no bus candidate available"))

    return pd.DataFrame(rows, columns=columns)


# ── reference tables ──────────────────────────────────────────────────────────


@dlt.table(
    name="blf_bus_mapping_bus_native",
    comment=(
        "One row per (bus_name, message_id): the expected native set S_B for each bus, "
        "derived from ecu_bus.csv joined with message_sender.csv on ECU name."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def blf_bus_mapping_bus_native():
    return spark.createDataFrame(_BUS_NATIVE_ROWS, schema=_BUS_NATIVE_SCHEMA)


@dlt.table(
    name="blf_bus_mapping_channel_observed",
    comment=(
        "One row per (_source_file, channel, message_id) actually observed in blf_silver_can, "
        "with an occurrence count. This is the observed set O_C, kept per source file rather "
        "than pooled across the corpus -- channel wiring is consistent within one log but not "
        "guaranteed to be across logs (different vehicles, harness revisions, renumbering)."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file", "channel"],
)
def blf_bus_mapping_channel_observed():
    return (
        spark.read.table(SOURCE_TABLE)
        .groupBy("_source_file", "channel", F.col("can_id").alias("message_id"))
        .agg(F.count(F.lit(1)).alias("msg_count"))
    )


@dlt.table(
    name="blf_bus_mapping_scores",
    comment=(
        "One row per (_source_file, channel, bus_name): recall = |O_C n S_B| / |S_B|, "
        "precision = |O_C n S_B| / |O_C|, jaccard = |O_C n S_B| / |O_C u S_B|, and score is "
        "the F-beta blend of recall and precision (blf.bus_mapping_beta, default 2.0 -- "
        "recall-weighted, since gateway relaying depresses precision without changing "
        "what a channel's native traffic is). O_C is per (_source_file, channel)."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file", "channel"],
)
def blf_bus_mapping_scores():
    observed = (
        dlt.read("blf_bus_mapping_channel_observed")
        .groupBy("_source_file", "channel")
        .agg(F.collect_set("message_id").alias("observed_ids"))
    )
    expected = (
        dlt.read("blf_bus_mapping_bus_native")
        .groupBy("bus_name")
        .agg(F.collect_set("message_id").alias("expected_ids"))
    )

    inter_size = F.size(F.array_intersect(F.col("observed_ids"), F.col("expected_ids")))
    union_size = F.size(F.array_union(F.col("observed_ids"), F.col("expected_ids")))
    recall = F.coalesce(F.try_divide(inter_size, F.size(F.col("expected_ids"))), F.lit(0.0))
    precision = F.coalesce(F.try_divide(inter_size, F.size(F.col("observed_ids"))), F.lit(0.0))
    jaccard = F.coalesce(F.try_divide(inter_size, union_size), F.lit(0.0))
    beta2 = F.lit(BETA * BETA)
    f_beta = F.coalesce(
        F.try_divide(
            (F.lit(1.0) + beta2) * precision * recall,
            beta2 * precision + recall,
        ),
        F.lit(0.0),
    )

    # Each _source_file's channels are cross-joined against the (small, global)
    # bus candidate set independently -- this is a per-file cross join, not a
    # corpus-wide one, since observed_ids already carries _source_file.
    return (
        observed.crossJoin(F.broadcast(expected))
        .withColumn("recall", recall)
        .withColumn("precision", precision)
        .withColumn("jaccard", jaccard)
        .withColumn("score", f_beta)
        .select("_source_file", "channel", "bus_name", "score", "recall", "precision", "jaccard")
    )


# ── user-facing mapping table ─────────────────────────────────────────────────


@dlt.table(
    name="blf_bus_mapping",
    comment=(
        "One row per (_source_file, channel): its best-matching bus name, found by solving "
        "the channel<->bus assignment as a maximum-weight one-to-one matching (Hungarian "
        "algorithm) over that file's rows of blf_bus_mapping_scores. Solved independently per "
        "_source_file -- channel wiring is consistent within one log but not guaranteed to be "
        "across logs. note flags a channel whose best score is below blf.bus_mapping_min_score "
        "(default 0.3, needs a human look) or one left unmatched because the file has more "
        "channels than candidate buses."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file"],
)
def blf_bus_mapping():
    return (
        dlt.read("blf_bus_mapping_scores")
        .groupBy("_source_file")
        .applyInPandas(_solve_assignment, schema=_ASSIGN_SCHEMA)
    )


# ── blf_gold_signals enriched with the inferred bus name ─────────────────────


@dlt.table(
    name="blf_gold_signals_bus",
    comment=(
        "blf_gold_signals (from blf_ingestion, read as an external batch table) left-joined "
        "with blf_bus_mapping on (_source_file, channel) -- the assignment is solved per file, "
        "not per channel alone, since channel wiring is not guaranteed to be consistent across "
        "logs. bus_name is the inferred physical bus (NULL for a (file, channel) blf_bus_mapping "
        "never assigned); bus_match_score/bus_match_note carry the match quality so a "
        "low-confidence join is visible rather than silently trusted. Kept as its own table "
        "rather than added to blf_gold_signals itself: blf_bus_mapping is built from "
        "blf_silver_can, so joining bus_name back into blf_silver_can or blf_gold_signals would "
        "make blf_ingestion depend on this pipeline's output while this pipeline depends on "
        "blf_ingestion's -- a cross-pipeline cycle. Reading blf_gold_signals here, in the "
        "pipeline that already depends on blf_ingestion, keeps the dependency one-directional."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["signal_source", "channel", "signal_name", "timestamp_s"],
)
def blf_gold_signals_bus():
    mapping = dlt.read("blf_bus_mapping").select(
        "_source_file",
        "channel",
        F.col("matched_bus").alias("bus_name"),
        F.col("score").alias("bus_match_score"),
        F.col("note").alias("bus_match_note"),
    )
    return spark.read.table(GOLD_SOURCE_TABLE).join(F.broadcast(mapping), ["_source_file", "channel"], "left")
