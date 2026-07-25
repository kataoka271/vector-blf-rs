"""
Databricks Delta Live Tables pipeline -- BLF scene analysis
============================================================

Cuts blf_gold_signals into time sections ("scenes"), describes each scene with a
per-signal feature vector, clusters the scenes, labels each one with the driver
actions a rule table says took place ("pressed the brake", "pushed the A/C
switch"), and scores how unusual it is.

Separate from blf_ingestion (dlt_blf_pipeline.py): this pipeline reads
blf_gold_signals as a plain batch Unity Catalog table and doesn't need the
vector_blf wheel. It's triggered rather than continuous because clustering and
the anomaly baseline are whole-corpus computations that have to recompute
together.

Layer layout
------------
  blf_scene_signal_stats        robust per-(file, signal) scale + time span
  blf_scene_boundaries          one row per scene: the segmentation itself
  blf_scene_signal_features     long format -- one row per (scene, signal)
  blf_scene_feature_index       one row -- the globally aligned feature key list
  blf_scene_vectors             one row per scene -- dense feature vector
  blf_scene_cluster_assignments per-scene cluster id + centroid distance
  blf_scene_feature_z           long format -- robust z per (scene, feature)
  blf_scene_labels              rule-derived action labels per scene
  blf_scene_clusters            per-cluster profile (k rows)
  blf_scenes                    the user-facing scene table

Setup
-----
Set pipeline parameters (Edit -> Advanced -> Parameters):
    blf.target_catalog            main                                    (optional)
    blf.target_schema             blf                                     (optional)
    blf.scene_rules_path          /Volumes/mycat/myschema/signals/scene_rules.csv
    blf.semantic_model_endpoint   databricks-claude-3-7-sonnet            (optional)

Every other blf.scene_* parameter has a default; see the constants below.

Rule CSV format (header required):
    rule_id,condition_index,label,priority,signal_source,channel,signal_name,
    feature,op,value_a,value_b,negate,description

One row per condition. A rule fires when *all* of its conditions match, so a
disjunction is expressed as two rule_ids sharing a label. An empty channel
matches any channel. Lower priority wins when several rules fire on one scene.
Supported ops: gt, ge, lt, le, eq, between, abs_gt, abs_ge, abs_lt, abs_le.

This pipeline is triggered: run it manually (`databricks bundle run
blf_scenes`) or on a schedule after blf_ingestion has caught up.
"""

from __future__ import annotations

import math

import dlt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
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
SOURCE_TABLE = spark.conf.get("blf.scene_source_table", "") or f"{TARGET_CATALOG}.{TARGET_SCHEMA}.blf_gold_signals"

# ETH signals are per-packet protocol fields (ip.ttl, src_port, ...). Including
# them would swamp both the change-point detector and the feature vector with
# thousands of keys that say nothing about what the vehicle was doing.
SIGNAL_SOURCES = [s.strip() for s in spark.conf.get("blf.scene_signal_sources", "CAN,SOMEIP").split(",") if s.strip()]

WINDOW_SECONDS = float(spark.conf.get("blf.scene_window_seconds", "10.0"))
MIN_SEGMENT_SECONDS = float(spark.conf.get("blf.scene_min_segment_seconds", "2.0"))
CHANGEPOINT_K = float(spark.conf.get("blf.scene_changepoint_k", "4.0"))
CHANGEPOINT_QUANTUM_S = float(spark.conf.get("blf.scene_changepoint_quantum_s", "0.05"))
MAX_SEGMENTS_PER_FILE = int(spark.conf.get("blf.scene_max_segments_per_file", "500"))

_DEFAULT_CHANGEPOINT_SIGNALS = (
    "VehicleSpeed_kmh,BrakePressure_bar,AccelPedal_pct,SteeringAngle_deg,ignition_status,AcRequest,FanSpeed_pct"
)
CHANGEPOINT_SIGNALS = [
    s.strip()
    for s in spark.conf.get("blf.scene_changepoint_signals", _DEFAULT_CHANGEPOINT_SIGNALS).split(",")
    if s.strip()
]

MAX_FEATURES = int(spark.conf.get("blf.scene_max_features", "256"))
SCENE_K = int(spark.conf.get("blf.scene_k", "8"))
SCENE_K_MIN = int(spark.conf.get("blf.scene_k_min", "3"))
SCENE_K_MAX = int(spark.conf.get("blf.scene_k_max", "12"))
SCENE_SEED = int(spark.conf.get("blf.scene_seed", "42"))

BASELINE_SCOPE = spark.conf.get("blf.scene_baseline_scope", "global").strip().lower()
ANOMALY_SCALE = float(spark.conf.get("blf.scene_anomaly_scale", "3.0"))
Z_CLIP = float(spark.conf.get("blf.scene_z_clip", "10.0"))

RULES_PATH = spark.conf.get("blf.scene_rules_path", "")
RULES_TABLE = spark.conf.get("blf.scene_rules_table", "")
DEFAULT_LABEL = spark.conf.get("blf.scene_default_label", "idle")

# Reuses blf.semantic_model_endpoint rather than introducing a second endpoint
# variable -- a workspace that has one foundation-model endpoint wants both the
# signal-doc summaries and the scene summaries to go to it.
SEMANTIC_MODEL_ENDPOINT = spark.conf.get("blf.semantic_model_endpoint", "")
SUMMARY_MIN_SCORE = float(spark.conf.get("blf.scene_summary_min_score", "60"))
SUMMARY_MAX_ROWS = int(spark.conf.get("blf.scene_summary_max_rows", "2000"))


def parse_weights(text: str, n: int = 3) -> tuple[float, ...]:
    """Parse a comma-separated weight list and normalize it to sum to 1.

    Raises ValueError if the list is not exactly n entries, is not numeric, or
    sums to zero or less.
    """
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) != n:
        raise ValueError(f"expected {n} comma-separated weights, got {len(parts)}: {text!r}")
    values = [float(p) for p in parts]
    total = sum(values)
    if total <= 0:
        raise ValueError(f"weights must sum to a positive number, got {total}: {text!r}")
    return tuple(v / total for v in values)


ANOMALY_WEIGHTS = parse_weights(spark.conf.get("blf.scene_anomaly_weights", "0.45,0.40,0.15"))

# With min > window the greedy merge deletes most grid boundaries and the fixed
# grid silently stops being a grid, so clamp rather than produce quiet nonsense.
if MIN_SEGMENT_SECONDS > WINDOW_SECONDS:
    print(
        f"[scene_pipeline] blf.scene_min_segment_seconds ({MIN_SEGMENT_SECONDS}) exceeds "
        f"blf.scene_window_seconds ({WINDOW_SECONDS}); clamping to the window",
        flush=True,
    )
    MIN_SEGMENT_SECONDS = WINDOW_SECONDS

# ── feature sets ──────────────────────────────────────────────────────────────

# Fed to the clustering. Every entry is duration-invariant (a rate or a level),
# so k-means learns "busy scene" rather than "long scene".
_CLUSTER_FEATURES = [
    "mean_v",
    "std_v",
    "delta_v",
    "range_v",
    "slope_per_s",
    "duty_cycle",
    "transition_rate_hz",
]

# Additionally visible to the rule engine, where raw counts and endpoints are
# what a human actually wants to threshold on.
_RULE_FEATURES = _CLUSTER_FEATURES + [
    "min_v",
    "max_v",
    "first_v",
    "last_v",
    "n_transitions",
    "n_samples",
    "sample_rate_hz",
]

# ── schemas ───────────────────────────────────────────────────────────────────

_SEGMENTS_SCHEMA = StructType(
    [
        StructField("_source_file", StringType(), nullable=False),
        StructField("segment_index", IntegerType(), nullable=False),
        StructField("seg_start_s", DoubleType(), nullable=False),
        StructField("seg_end_s", DoubleType(), nullable=False),
        StructField("duration_s", DoubleType(), nullable=False),
        StructField("from_changepoint", BooleanType(), nullable=False),
        StructField("is_last", BooleanType(), nullable=False),
    ]
)

_ASSIGN_SCHEMA = StructType(
    [
        StructField("scene_id", StringType(), nullable=False),
        StructField("cluster_id", IntegerType(), nullable=False),
        StructField("centroid_distance", DoubleType(), nullable=False),
        StructField("cluster_size", IntegerType(), nullable=False),
        StructField("cluster_share", DoubleType(), nullable=False),
        StructField("k_used", IntegerType(), nullable=False),
    ]
)

_RULE_CONDITIONS_SCHEMA = StructType(
    [
        StructField("rule_id", StringType(), nullable=False),
        StructField("condition_index", IntegerType(), nullable=False),
        StructField("signal_source", StringType(), nullable=False),
        StructField("channel_key", StringType(), nullable=False),
        StructField("signal_name", StringType(), nullable=False),
        StructField("feature", StringType(), nullable=False),
        StructField("transform", StringType(), nullable=False),
        StructField("lo", DoubleType(), nullable=False),
        StructField("hi", DoubleType(), nullable=False),
        StructField("negate", BooleanType(), nullable=False),
    ]
)

_RULE_META_SCHEMA = StructType(
    [
        StructField("rule_id", StringType(), nullable=False),
        StructField("label", StringType(), nullable=False),
        StructField("priority", IntegerType(), nullable=False),
        StructField("n_conditions", IntegerType(), nullable=False),
    ]
)

# ── segmentation maths (pure Python) ──────────────────────────────────────────


def build_segments(
    boundaries: np.ndarray,
    strengths: np.ndarray,
    t_start: float,
    t_end: float,
    window_seconds: float,
    min_segment_seconds: float,
    max_segments: int,
) -> list[tuple[float, float, bool]]:
    """Merge a fixed tumbling grid with change-point boundaries into segments.

    boundaries and strengths are parallel arrays of candidate change-point times
    (in the same units as t_start/t_end) and their magnitudes; neither needs to
    be sorted.

    Returns a list of (seg_start, seg_end, from_changepoint) tuples that tile
    [t_start, t_end] with no gaps or overlaps. No segment is shorter than
    min_segment_seconds unless the whole span is, in which case a single
    degenerate segment is returned.
    """
    span = t_end - t_start
    if span <= 0:
        return [(t_start, t_end, False)]

    n_cells = max(1, int(math.ceil(span / window_seconds)))
    grid = t_start + window_seconds * np.arange(1, n_cells, dtype=float)

    cand_t = np.concatenate([grid, np.asarray(boundaries, dtype=float)])
    # Grid boundaries are pinned at infinite strength so the max_segments cap
    # drops change points first and always degrades toward the plain grid.
    cand_s = np.concatenate([np.full(grid.shape, np.inf), np.asarray(strengths, dtype=float)])
    is_cp = np.concatenate([np.zeros(grid.shape, dtype=bool), np.ones(np.shape(boundaries), dtype=bool)])

    inside = (cand_t > t_start) & (cand_t < t_end) & np.isfinite(cand_t)
    cand_t, cand_s, is_cp = cand_t[inside], cand_s[inside], is_cp[inside]

    if max_segments > 0 and cand_t.size > max_segments:
        keep = np.argpartition(-np.nan_to_num(cand_s, nan=-np.inf), max_segments - 1)[:max_segments]
        cand_t, is_cp = cand_t[keep], is_cp[keep]

    order = np.argsort(cand_t, kind="stable")
    cand_t, is_cp = cand_t[order], is_cp[order]

    # Greedy forward merge: a boundary is kept only if it is far enough from the
    # last *kept* one. That backward dependence is why this cannot be a window
    # function and has to see the whole per-file list at once.
    kept_t: list[float] = [t_start]
    kept_cp: list[bool] = [False]
    for t, cp in zip(cand_t.tolist(), is_cp.tolist()):
        if t - kept_t[-1] >= min_segment_seconds:
            kept_t.append(float(t))
            kept_cp.append(bool(cp))

    # A trailing runt is merged backwards rather than emitted as a short segment.
    while len(kept_t) > 1 and t_end - kept_t[-1] < min_segment_seconds:
        kept_t.pop()
        kept_cp.pop()

    edges = kept_t + [t_end]
    return [(edges[i], edges[i + 1], kept_cp[i]) for i in range(len(edges) - 1)]


def _segments_for_file(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas worker: turn one file's candidate boundaries into segments."""
    source_file = str(pdf["_source_file"].iloc[0])
    t_start = float(pdf["t_min_s"].iloc[0])
    t_end = float(pdf["t_max_s"].iloc[0])

    present = pdf["boundary_s"].notna()
    segments = build_segments(
        pdf.loc[present, "boundary_s"].to_numpy(dtype=float),
        pdf.loc[present, "strength"].to_numpy(dtype=float),
        t_start,
        t_end,
        WINDOW_SECONDS,
        MIN_SEGMENT_SECONDS,
        MAX_SEGMENTS_PER_FILE,
    )
    last = len(segments) - 1
    return pd.DataFrame(
        [
            (source_file, i, start, end, end - start, from_cp, i == last)
            for i, (start, end, from_cp) in enumerate(segments)
        ],
        columns=[f.name for f in _SEGMENTS_SCHEMA.fields],
    )


# ── clustering maths (pure Python) ────────────────────────────────────────────


def robust_scale(x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Center on the median and scale by 1.4826 * MAD.

    Returns (scaled, median, scale). Columns with zero MAD get a scale of 1.0 so
    a constant feature contributes nothing instead of dividing by zero.
    """
    med = np.nanmedian(x, axis=0)
    mad = np.nanmedian(np.abs(x - med), axis=0)
    # 1.4826 rescales the MAD to a sigma equivalent under normality. Plain
    # z-scoring is unusable here: the features are heavy-tailed, so one outlier
    # scene would otherwise dominate the whole geometry.
    scale = np.where(mad > 0, 1.4826 * mad, 1.0)
    return (x - med) / scale, med, scale


def kmeans_fit(
    x: np.ndarray,
    k: int,
    seed: int,
    n_iter: int = 100,
    tol: float = 1e-6,
) -> tuple[np.ndarray, np.ndarray, float]:
    """Run k-means++ seeding followed by Lloyd's algorithm.

    Returns (centroids, labels, inertia). Cluster numbering follows the seeding
    order; use canonical_order to make it reproducible.
    """
    n = x.shape[0]
    k = max(1, min(k, n))
    rng = np.random.default_rng(seed)

    centers = x[rng.integers(n)][None, :].copy()
    for _ in range(1, k):
        d2 = ((x[:, None, :] - centers[None, :, :]) ** 2).sum(-1).min(1)
        total = float(d2.sum())
        probs = np.full(n, 1.0 / n) if total <= 0 else d2 / total
        centers = np.vstack([centers, x[rng.choice(n, p=probs)]])

    labels = np.zeros(n, dtype=np.int64)
    inertia = float("inf")
    previous = float("inf")
    for _ in range(n_iter):
        d2 = ((x[:, None, :] - centers[None, :, :]) ** 2).sum(-1)
        labels = d2.argmin(1)
        inertia = float(d2[np.arange(n), labels].sum())
        for j in range(k):
            members = x[labels == j]
            if members.size:
                centers[j] = members.mean(0)
            else:
                # Re-seed an emptied cluster on the worst-fit point. Leaving the
                # centroid untouched would let a NaN mean poison every later
                # distance computation.
                centers[j] = x[d2[np.arange(n), labels].argmax()]
        if previous - inertia <= tol * max(previous, 1.0):
            break
        previous = inertia

    return centers, labels, inertia


def canonical_order(centers: np.ndarray, counts: np.ndarray) -> np.ndarray:
    """Return the permutation that makes cluster ids reproducible.

    Sorting by descending size then centroid coordinates is a total order on the
    fitted model, so an identical fit always yields identical ids even though
    Lloyd's iteration numbers clusters by seeding order.
    """
    keys = [tuple(np.round(c, 9).tolist()) for c in centers]
    return np.array(
        sorted(range(len(centers)), key=lambda j: (-int(counts[j]), keys[j])),
        dtype=np.int64,
    )


def mean_silhouette(distances: np.ndarray, labels: np.ndarray, k: int) -> float:
    """Mean silhouette coefficient given a precomputed pairwise distance matrix."""
    n = labels.shape[0]
    if k < 2 or n <= k:
        return -1.0
    scores = np.zeros(n, dtype=float)
    for i in range(n):
        own = labels == labels[i]
        own[i] = False
        if not own.any():
            continue
        a = distances[i, own].mean()
        b = np.inf
        for j in range(k):
            if j == labels[i]:
                continue
            other = labels == j
            if other.any():
                b = min(b, distances[i, other].mean())
        if np.isfinite(b):
            scores[i] = (b - a) / max(a, b, 1e-12)
    return float(scores.mean())


def choose_k(x: np.ndarray, k_min: int, k_max: int, seed: int, sample: int = 5000) -> int:
    """Pick k by mean silhouette on a subsample; ties break toward the smaller k."""
    rng = np.random.default_rng(seed)
    # Silhouette is O(n^2) in both time and memory, so it never sees the full set.
    idx = rng.choice(x.shape[0], min(sample, x.shape[0]), replace=False)
    sub = x[idx]
    distances = np.linalg.norm(sub[:, None, :] - sub[None, :, :], axis=-1)

    best_k, best_score = k_min, -np.inf
    for k in range(k_min, min(k_max, sub.shape[0] - 1) + 1):
        _, labels, _ = kmeans_fit(sub, k, seed)
        score = mean_silhouette(distances, labels, k)
        if score > best_score + 1e-9:
            best_k, best_score = k, score
    return best_k


def cluster_scenes(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas worker: fit k-means over every scene and assign each one.

    Receives all scenes as a single group, so the fit sees the whole corpus.
    """
    x = np.vstack(pdf["feature_vector"].to_numpy())
    x = np.nan_to_num(x.astype(float), nan=0.0, posinf=0.0, neginf=0.0)
    scaled, _, _ = robust_scale(x)
    scaled = np.nan_to_num(scaled, nan=0.0, posinf=0.0, neginf=0.0)

    k = SCENE_K if SCENE_K > 0 else choose_k(scaled, SCENE_K_MIN, SCENE_K_MAX, SCENE_SEED)
    k = max(1, min(k, scaled.shape[0]))
    centers, labels, _ = kmeans_fit(scaled, k, SCENE_SEED)

    counts = np.bincount(labels, minlength=k)
    permutation = canonical_order(centers, counts)
    remap = np.empty(k, dtype=np.int64)
    remap[permutation] = np.arange(k)
    labels = remap[labels]
    centers = centers[permutation]
    counts = counts[permutation]

    # pyspark.ml's KMeansModel does not expose this at all; here it falls
    # straight out of the fit we already did.
    distance = np.linalg.norm(scaled - centers[labels], axis=1)

    return pd.DataFrame(
        {
            "scene_id": pdf["scene_id"].to_numpy(),
            "cluster_id": labels.astype(np.int32),
            "centroid_distance": distance.astype(float),
            "cluster_size": counts[labels].astype(np.int32),
            "cluster_share": (counts[labels] / len(labels)).astype(float),
            "k_used": np.full(len(labels), k, dtype=np.int32),
        }
    )


# ── anomaly maths (pure Python) ───────────────────────────────────────────────


def anomaly_score(
    distance_z: np.ndarray,
    z_agg: np.ndarray,
    rarity_bits: np.ndarray,
    weights: tuple[float, ...],
    scale: float,
) -> np.ndarray:
    """Blend cluster distance, robust z magnitude and cluster rarity into 0-100.

    Each term is clamped at zero so that being *more* typical than the median
    never subtracts from the score. The exponential saturation keeps the result
    bounded and monotone without forcing a fixed fraction of scenes to look
    anomalous the way a percentile rank would.
    """
    w_d, w_z, w_r = weights
    u = w_d * np.maximum(distance_z, 0.0) + w_z * np.maximum(z_agg, 0.0) + w_r * np.maximum(rarity_bits, 0.0)
    return 100.0 * (1.0 - np.exp(-np.maximum(u, 0.0) / float(scale)))


@pandas_udf(DoubleType())
def _anomaly_score_udf(distance_z: pd.Series, z_agg: pd.Series, rarity_bits: pd.Series) -> pd.Series:
    return pd.Series(
        anomaly_score(
            distance_z.fillna(0.0).to_numpy(dtype=float),
            z_agg.fillna(0.0).to_numpy(dtype=float),
            rarity_bits.fillna(0.0).to_numpy(dtype=float),
            ANOMALY_WEIGHTS,
            ANOMALY_SCALE,
        )
    )


# ── rule compilation (pure Python) ────────────────────────────────────────────

# Every operator becomes a closed interval over an optional transform, so Spark
# evaluates one BETWEEN instead of one branch per rule. math.nextafter gives
# exact strict-inequality semantics on doubles.
_OPS = {
    "gt": lambda a, b: ("identity", math.nextafter(a, math.inf), math.inf),
    "ge": lambda a, b: ("identity", a, math.inf),
    "lt": lambda a, b: ("identity", -math.inf, math.nextafter(a, -math.inf)),
    "le": lambda a, b: ("identity", -math.inf, a),
    "eq": lambda a, b: ("identity", a, a),
    "between": lambda a, b: ("identity", a, b),
    "abs_gt": lambda a, b: ("abs", math.nextafter(a, math.inf), math.inf),
    "abs_ge": lambda a, b: ("abs", a, math.inf),
    "abs_lt": lambda a, b: ("abs", -math.inf, math.nextafter(a, -math.inf)),
    "abs_le": lambda a, b: ("abs", -math.inf, a),
}

_TRUTHY = {"1", "true", "t", "yes", "y"}


def normalize_rules(rows: list[dict]) -> tuple[list[tuple], list[tuple]]:
    """Compile raw rule rows into interval conditions plus per-rule metadata.

    Returns (conditions, meta) as lists of tuples matching
    _RULE_CONDITIONS_SCHEMA and _RULE_META_SCHEMA respectively.

    Raises ValueError if an op is unknown, a feature is not one of
    _RULE_FEATURES, `between` is missing its second bound, or a rule's rows
    disagree about label or priority.
    """
    conditions: list[tuple] = []
    labels: dict[str, str] = {}
    priorities: dict[str, int] = {}
    counts: dict[str, int] = {}

    for row in rows:
        rule_id = str(row["rule_id"]).strip()
        if not rule_id:
            continue
        label = str(row["label"]).strip()
        priority = int(float(row["priority"]))
        op = str(row["op"]).strip().lower()
        feature = str(row["feature"]).strip()

        if op not in _OPS:
            raise ValueError(f"rule {rule_id!r}: unknown op {op!r}; expected one of {sorted(_OPS)}")
        if feature not in _RULE_FEATURES:
            raise ValueError(f"rule {rule_id!r}: unknown feature {feature!r}; expected one of {_RULE_FEATURES}")
        if rule_id in labels and labels[rule_id] != label:
            raise ValueError(f"rule {rule_id!r}: conflicting labels {labels[rule_id]!r} and {label!r}")
        if rule_id in priorities and priorities[rule_id] != priority:
            raise ValueError(f"rule {rule_id!r}: conflicting priorities {priorities[rule_id]} and {priority}")

        value_a = float(row["value_a"])
        raw_b = row.get("value_b")
        value_b = float(raw_b) if raw_b not in (None, "") else None
        if op == "between" and value_b is None:
            raise ValueError(f"rule {rule_id!r}: op 'between' needs value_b")

        transform, lo, hi = _OPS[op](value_a, value_b)
        raw_channel = row.get("channel")
        conditions.append(
            (
                rule_id,
                int(float(row["condition_index"])),
                str(row["signal_source"]).strip(),
                "*" if raw_channel in (None, "") else str(raw_channel).strip(),
                str(row["signal_name"]).strip(),
                feature,
                transform,
                lo,
                hi,
                str(row.get("negate") or "").strip().lower() in _TRUTHY,
            )
        )
        labels[rule_id] = label
        priorities[rule_id] = priority
        counts[rule_id] = counts.get(rule_id, 0) + 1

    meta = [(rule_id, labels[rule_id], priorities[rule_id], counts[rule_id]) for rule_id in sorted(counts)]
    return conditions, meta


def _load_rule_rows() -> list[dict]:
    """Read the rule table or CSV into plain dicts.

    Returns an empty list when neither is configured or the source is missing,
    which disables labeling rather than failing the pipeline. Safe to call at
    module scope: the source is external to this pipeline, so it is fully
    materialized before the pipeline graph is resolved.
    """
    try:
        if RULES_TABLE:
            frame = spark.table(RULES_TABLE)
        elif RULES_PATH:
            frame = spark.read.option("header", True).csv(RULES_PATH)
        else:
            return []
        return [row.asDict() for row in frame.collect()]
    except Exception as exc:  # noqa: BLE001 -- any read failure means "no rules"
        print(f"[scene_pipeline] could not load scene rules: {exc}", flush=True)
        return []


try:
    RULE_CONDITIONS, RULE_META = normalize_rules(_load_rule_rows())
except ValueError as exc:
    # A malformed rule file is an operator error worth surfacing loudly, but it
    # should not take the whole pipeline down -- the scenes are still useful
    # unlabeled.
    print(f"[scene_pipeline] invalid scene rules, labeling disabled: {exc}", flush=True)
    RULE_CONDITIONS, RULE_META = [], []

print(f"[scene_pipeline] loaded {len(RULE_META)} rules ({len(RULE_CONDITIONS)} conditions)", flush=True)

# ── shared column expressions ─────────────────────────────────────────────────

# One null-safe string key per (source, channel, name). Collapsing the triple
# removes three-column joins everywhere downstream and sidesteps the fact that a
# NULL channel never equals itself in a join predicate.
_SIGNAL_KEY = F.concat_ws(
    "|",
    F.col("signal_source"),
    F.coalesce(F.col("channel").cast("string"), F.lit("-")),
    F.col("signal_name"),
)

_SCENE_ID = F.concat_ws("#", F.col("_source_file"), F.col("segment_index"))

_W_SIGNAL = Window.partitionBy("_source_file", "signal_key").orderBy("timestamp_ns")

_BASELINE_KEYS = ["feature_key"] if BASELINE_SCOPE == "global" else ["_source_file", "feature_key"]


def _gold_in_scope():
    """Batch-read blf_gold_signals, restricted to the configured signal sources."""
    frame = spark.read.table(SOURCE_TABLE)
    if SIGNAL_SOURCES:
        # blf_gold_signals is clustered on signal_source first, so this prunes
        # files at the Delta level rather than filtering after the scan.
        frame = frame.filter(F.col("signal_source").isin(SIGNAL_SOURCES))
    return frame.withColumn("signal_key", _SIGNAL_KEY)


def _melt(frame, features: list[str]):
    """Reshape wide per-signal features into (scene_id, feature_key, feature_value)."""
    pairs = ", ".join(f"'{name}', CAST({name} AS DOUBLE)" for name in features)
    return frame.select(
        "_source_file",
        "scene_id",
        "signal_key",
        "signal_source",
        "channel",
        "signal_name",
        F.expr(f"stack({len(features)}, {pairs}) as (feature_name, feature_value)"),
    ).withColumn("feature_key", F.concat_ws(".", F.col("signal_key"), F.col("feature_name")))


def _assign_segments(frame):
    """Attach scene columns to per-sample rows via a grid-bucket equi-join."""
    grid_start = F.floor(F.col("seg_start_s") / F.lit(WINDOW_SECONDS)).cast("int")
    # The interval is half-open, so the final segment has to include the cell its
    # endpoint falls in -- otherwise the file's last sample, which sits exactly
    # on seg_end_s, lands in a grid cell no segment claims and is dropped.
    grid_end = F.floor(
        (F.col("seg_end_s") - F.when(F.col("is_last"), F.lit(0.0)).otherwise(F.lit(1e-9))) / F.lit(WINDOW_SECONDS)
    ).cast("int")

    segments = (
        dlt.read("blf_scene_boundaries")
        .withColumn("grid_idx", F.explode(F.sequence(grid_start, grid_end)))
        .select("_source_file", "grid_idx", "scene_id", "segment_index", "seg_start_s", "seg_end_s", "is_last")
    )

    within = (F.col("timestamp_s") >= F.col("seg_start_s")) & (
        F.when(F.col("is_last"), F.col("timestamp_s") <= F.col("seg_end_s")).otherwise(
            F.col("timestamp_s") < F.col("seg_end_s")
        )
    )
    return (
        frame.withColumn("grid_idx", F.floor(F.col("timestamp_s") / F.lit(WINDOW_SECONDS)).cast("int"))
        # Broadcasting the exploded segment side keeps the gold table unshuffled;
        # the segment side is roughly one row per grid cell, a few MB at most.
        .join(F.broadcast(segments), ["_source_file", "grid_idx"], "inner")
        .filter(within)
        .drop("grid_idx", "is_last")
    )


# ── segmentation ──────────────────────────────────────────────────────────────


@dlt.table(
    name="blf_scene_signal_stats",
    comment=(
        "Robust scale and time span for every (source file, signal) pair in scope. "
        "robust_sigma is the IQR divided by 1.349, a one-pass stand-in for the standard "
        "deviation that outliers cannot inflate; it sets the change-point threshold. "
        "signal_key is 'signal_source|channel|signal_name'."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def blf_scene_signal_stats():
    return (
        _gold_in_scope()
        .groupBy("_source_file", "signal_key", "signal_source", "channel", "signal_name")
        .agg(
            F.expr("percentile_approx(signal_value, array(0.25, 0.5, 0.75))").alias("_quartiles"),
            F.count(F.lit(1)).alias("n_samples"),
            F.min("timestamp_s").alias("t_min_s"),
            F.max("timestamp_s").alias("t_max_s"),
        )
        .withColumn("median_value", F.col("_quartiles")[1])
        .withColumn(
            "robust_sigma",
            F.greatest((F.col("_quartiles")[2] - F.col("_quartiles")[0]) / F.lit(1.349), F.lit(1e-9)),
        )
        .drop("_quartiles")
    )


def _candidate_changepoints():
    """Per-file timestamps where a watched signal changed enough to split a scene."""
    scoped = _gold_in_scope()
    if CHANGEPOINT_SIGNALS:
        scoped = scoped.filter(F.col("signal_name").isin(CHANGEPOINT_SIGNALS))

    lagged = (
        scoped
        # signal_str carries the enum category when one is defined, so enum and
        # numeric signals share a single definition of "changed".
        .withColumn("state", F.coalesce(F.col("signal_str"), F.col("signal_value").cast("string")))
        .withColumn("prev_value", F.lag("signal_value").over(_W_SIGNAL))
        .withColumn("prev_state", F.lag("state").over(_W_SIGNAL))
        .join(
            F.broadcast(dlt.read("blf_scene_signal_stats").select("_source_file", "signal_key", "robust_sigma")),
            ["_source_file", "signal_key"],
            "left",
        )
    )
    jump = F.abs(F.col("signal_value") - F.col("prev_value")) / F.col("robust_sigma")
    changed_state = (
        F.col("signal_str").isNotNull() & F.col("prev_state").isNotNull() & (F.col("state") != F.col("prev_state"))
    )

    return (
        lagged.filter(changed_state | (jump > F.lit(CHANGEPOINT_K)))
        .select(
            "_source_file",
            # Quantizing collapses near-simultaneous change points across
            # different signals into one candidate boundary.
            (F.round(F.col("timestamp_s") / F.lit(CHANGEPOINT_QUANTUM_S)) * F.lit(CHANGEPOINT_QUANTUM_S)).alias(
                "boundary_s"
            ),
            F.coalesce(jump, F.lit(float("inf"))).alias("strength"),
        )
        .groupBy("_source_file", "boundary_s")
        .agg(F.max("strength").alias("strength"))
    )


@dlt.table(
    name="blf_scene_boundaries",
    comment=(
        "One row per scene: the segmentation of each source file into time sections. "
        "Boundaries are a fixed tumbling grid of blf.scene_window_seconds merged with "
        "detected change points, with anything closer than blf.scene_min_segment_seconds "
        "to the previous kept boundary dropped. Segments tile the file with no gaps or "
        "overlaps; seg_end_s is exclusive except on the last segment of a file."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file", "seg_start_s"],
)
def blf_scene_boundaries():
    file_span = (
        dlt.read("blf_scene_signal_stats")
        .groupBy("_source_file")
        .agg(F.min("t_min_s").alias("t_min_s"), F.max("t_max_s").alias("t_max_s"))
    )
    return (
        _candidate_changepoints()
        # Right join so a file with no change points at all still gets the grid.
        .join(F.broadcast(file_span), "_source_file", "right")
        .groupBy("_source_file")
        .applyInPandas(_segments_for_file, schema=_SEGMENTS_SCHEMA)
        .withColumn("scene_id", _SCENE_ID)
    )


# ── per-scene, per-signal features ────────────────────────────────────────────


@dlt.table(
    name="blf_scene_signal_features",
    comment=(
        "One row per (scene, signal) with the aggregates describing that signal's "
        "behaviour during the scene. Rates (sample_rate_hz, transition_rate_hz) are "
        "duration-invariant and feed the clustering; raw counts and endpoints "
        "(n_transitions, first_v, last_v) are what the rule engine thresholds on. "
        "This is the drill-down table behind blf_scenes."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file", "segment_index", "signal_name"],
)
def blf_scene_signal_features():
    enriched = (
        _gold_in_scope()
        .withColumn("state", F.coalesce(F.col("signal_str"), F.col("signal_value").cast("string")))
        .withColumn("prev_state", F.lag("state").over(_W_SIGNAL))
        .withColumn("next_ts_s", F.lead("timestamp_s").over(_W_SIGNAL))
    )

    with_scene = _assign_segments(enriched).withColumn(
        # Clipping each sample's dwell time at the segment end stops a gap that
        # spans a boundary from being counted in both scenes.
        "dt_s",
        F.greatest(
            F.least(F.coalesce(F.col("next_ts_s"), F.col("seg_end_s")), F.col("seg_end_s")) - F.col("timestamp_s"),
            F.lit(0.0),
        ),
    )

    aggregated = with_scene.groupBy(
        "_source_file",
        "scene_id",
        "segment_index",
        "seg_start_s",
        "seg_end_s",
        "signal_key",
        "signal_source",
        "channel",
        "signal_name",
    ).agg(
        F.count(F.lit(1)).alias("n_samples"),
        F.avg("signal_value").alias("mean_v"),
        F.stddev_samp("signal_value").alias("std_v"),
        F.min("signal_value").alias("min_v"),
        F.max("signal_value").alias("max_v"),
        # min_by/max_by give an ordered first/last in a single pass. F.first and
        # F.last would depend on partition arrival order and change silently
        # between runs.
        F.expr("min_by(signal_value, timestamp_ns)").alias("first_v"),
        F.expr("max_by(signal_value, timestamp_ns)").alias("last_v"),
        F.expr("max_by(signal_str, timestamp_ns)").alias("last_str"),
        F.min("event_time").alias("event_time_start"),
        # regr_slope is the built-in OLS slope aggregate: one pass, NULL-safe,
        # and duration-normalized by construction.
        F.expr("regr_slope(signal_value, timestamp_s)").alias("slope_raw"),
        F.sum(F.when(F.col("prev_state").isNotNull() & (F.col("state") != F.col("prev_state")), 1).otherwise(0)).alias(
            "n_transitions"
        ),
        F.sum(F.when(F.col("signal_value") != 0, F.col("dt_s")).otherwise(F.lit(0.0))).alias("_on_time_s"),
        F.sum("dt_s").alias("_cover_s"),
    )

    duration = F.col("seg_end_s") - F.col("seg_start_s")
    return (
        aggregated.withColumn("duration_s", duration)
        .withColumn("delta_v", F.col("last_v") - F.col("first_v"))
        .withColumn("range_v", F.col("max_v") - F.col("min_v"))
        .withColumn("slope_per_s", F.coalesce(F.col("slope_raw"), F.lit(0.0)))
        .withColumn("duty_cycle", F.coalesce(F.try_divide(F.col("_on_time_s"), F.col("_cover_s")), F.lit(0.0)))
        .withColumn("sample_rate_hz", F.try_divide(F.col("n_samples"), duration))
        .withColumn("transition_rate_hz", F.try_divide(F.col("n_transitions"), duration))
        .drop("slope_raw", "_on_time_s", "_cover_s")
    )


# ── dense feature vectors ─────────────────────────────────────────────────────


@dlt.table(
    name="blf_scene_feature_index",
    comment=(
        "Single row holding the globally aligned feature key list and per-key median. "
        "Cross-joined into blf_scene_vectors so every scene's vector has the same "
        "length and ordering regardless of which signals its source file carries. "
        "Keys are ranked by how many scenes contain them and truncated to "
        "blf.scene_max_features, with the key name as a tie-break so the selected set "
        "does not churn between refreshes."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def blf_scene_feature_index():
    long = _melt(dlt.read("blf_scene_signal_features"), _CLUSTER_FEATURES).filter(F.col("feature_value").isNotNull())
    per_key = long.groupBy("feature_key").agg(
        F.countDistinct("scene_id").alias("n_scenes"),
        F.expr("percentile_approx(feature_value, 0.5)").alias("median_value"),
    )
    total = long.select(F.countDistinct("scene_id").alias("total_scenes"))

    ranked = (
        per_key.crossJoin(F.broadcast(total))
        .withColumn("coverage", F.col("n_scenes") / F.col("total_scenes"))
        .withColumn(
            "rank",
            F.row_number().over(Window.orderBy(F.col("coverage").desc(), F.col("feature_key").asc())),
        )
        .filter(F.col("rank") <= F.lit(MAX_FEATURES))
    )
    return ranked.agg(
        F.array_sort(F.collect_list("feature_key")).alias("feature_keys"),
        F.map_from_entries(F.collect_list(F.struct("feature_key", "median_value"))).alias("median_map"),
        F.count(F.lit(1)).cast("int").alias("n_features"),
    )


@dlt.table(
    name="blf_scene_vectors",
    comment=(
        "One dense feature vector per scene, aligned to blf_scene_feature_index. "
        "Stored as array<double> because Delta cannot hold a VectorUDT column. "
        "Features a scene does not carry are filled with the global median, so a "
        "missing signal reads as typical rather than extreme for clustering purposes; "
        "the anomaly path in blf_scenes deliberately does not impute."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file"],
)
def blf_scene_vectors():
    index = F.broadcast(dlt.read("blf_scene_feature_index"))
    per_scene = (
        _melt(dlt.read("blf_scene_signal_features"), _CLUSTER_FEATURES)
        .filter(F.col("feature_value").isNotNull())
        .groupBy("_source_file", "scene_id")
        .agg(F.map_from_entries(F.collect_list(F.struct("feature_key", "feature_value"))).alias("feature_map"))
    )
    return (
        per_scene.crossJoin(index)
        .withColumn(
            # transform() over the catalog's key array, looking each key up in
            # this scene's own map. A pivot would need the column list at plan
            # time, which means collecting an in-pipeline table on the driver --
            # and that reads the previous run's data, silently.
            "feature_vector",
            F.transform(
                F.col("feature_keys"),
                lambda key: F.coalesce(
                    F.element_at(F.col("feature_map"), key),
                    F.element_at(F.col("median_map"), key),
                    F.lit(0.0),
                ),
            ),
        )
        .select("_source_file", "scene_id", "feature_keys", "feature_vector")
    )


@dlt.table(
    name="blf_scene_cluster_assignments",
    comment=(
        "Per-scene k-means assignment with the distance to its cluster centroid. "
        "Fitted in a single applyInPandas group so the model sees every scene at once. "
        "cluster_id is canonicalized by descending cluster size, so an identical fit "
        "always produces identical ids -- but it is NOT a durable key across a refresh "
        "whose input data changed. Anchor on the labels and the cluster profile instead."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["cluster_id"],
)
def blf_scene_cluster_assignments():
    return (
        dlt.read("blf_scene_vectors")
        .select("scene_id", "feature_vector")
        .groupBy(F.lit(0))
        .applyInPandas(cluster_scenes, schema=_ASSIGN_SCHEMA)
    )


# ── robust z-scores ───────────────────────────────────────────────────────────


@dlt.table(
    name="blf_scene_feature_z",
    comment=(
        "Robust z-score per (scene, feature): (value - median) / (1.4826 * MAD), "
        "clipped at blf.scene_z_clip. The baseline population is controlled by "
        "blf.scene_baseline_scope -- 'global' (default) compares every scene against "
        "the whole corpus, 'file' against its own source file. This is the evidence "
        "behind the top_features column on blf_scenes."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["scene_id"],
)
def blf_scene_feature_z():
    long = _melt(dlt.read("blf_scene_signal_features"), _CLUSTER_FEATURES).filter(F.col("feature_value").isNotNull())
    median = long.groupBy(*_BASELINE_KEYS).agg(F.expr("percentile_approx(feature_value, 0.5)").alias("median_value"))
    # MAD needs the median first, hence the second pass. Both aggregates are over
    # a table with at most n_scenes * n_features rows, so this stays cheap.
    deviation = (
        long.join(F.broadcast(median), _BASELINE_KEYS)
        .withColumn("abs_dev", F.abs(F.col("feature_value") - F.col("median_value")))
        .groupBy(*_BASELINE_KEYS)
        .agg(F.expr("percentile_approx(abs_dev, 0.5)").alias("mad"))
    )
    return (
        long.join(F.broadcast(median), _BASELINE_KEYS)
        .join(F.broadcast(deviation), _BASELINE_KEYS)
        .withColumn(
            "z",
            (F.col("feature_value") - F.col("median_value"))
            # The floor stops a constant feature (MAD = 0) producing infinite z.
            / F.greatest(F.lit(1.4826) * F.col("mad"), F.lit(1e-9)),
        )
        .withColumn("abs_z", F.least(F.abs(F.col("z")), F.lit(Z_CLIP)))
        .select("_source_file", "scene_id", "feature_key", "feature_value", "median_value", "mad", "z", "abs_z")
    )


# ── rule-based action labels ──────────────────────────────────────────────────


@dlt.table(
    name="blf_scene_labels",
    comment=(
        "Action labels attached to each scene by the rule table (blf.scene_rules_path "
        "or blf.scene_rules_table). A rule fires when every one of its conditions "
        "matches; primary_action_label is the lowest-priority firing rule. Scenes that "
        "match nothing are absent here and fall back to blf.scene_default_label in "
        "blf_scenes. With a wildcard channel, two conditions of one rule may match on "
        "different channels -- that is intended scene-level semantics."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file"],
)
def blf_scene_labels():
    if not RULE_META:
        return (
            dlt.read("blf_scene_boundaries")
            .select("_source_file", "scene_id")
            .withColumn("action_labels", F.array().cast(ArrayType(StringType())))
            .withColumn("matched_rule_ids", F.array().cast(ArrayType(StringType())))
            .withColumn("primary_action_label", F.lit(DEFAULT_LABEL))
        )

    conditions = F.broadcast(spark.createDataFrame(RULE_CONDITIONS, schema=_RULE_CONDITIONS_SCHEMA))
    meta = F.broadcast(spark.createDataFrame(RULE_META, schema=_RULE_META_SCHEMA))

    features = _melt(dlt.read("blf_scene_signal_features"), _RULE_FEATURES)
    joined = features.join(conditions, ["signal_source", "signal_name", "feature"], "inner").filter(
        (F.col("channel_key") == F.lit("*")) | (F.col("channel_key") == F.col("channel").cast("string"))
    )

    # The only branch in the whole rule engine: every operator was already
    # compiled to an interval, so adding a rule never adds a when().
    value = F.when(F.col("transform") == F.lit("abs"), F.abs(F.col("feature_value"))).otherwise(F.col("feature_value"))
    in_range = (value >= F.col("lo")) & (value <= F.col("hi")) & F.col("feature_value").isNotNull()
    # XOR against negate, coalesced so a NULL feature is a non-match, never NULL.
    matches = F.col("negate") != F.coalesce(in_range, F.lit(False))

    fired = (
        joined.filter(matches)
        .groupBy("_source_file", "scene_id", "rule_id")
        .agg(F.countDistinct("condition_index").alias("n_matched"))
        .join(meta, "rule_id")
        .filter(F.col("n_matched") == F.col("n_conditions"))
    )

    return fired.groupBy("_source_file", "scene_id").agg(
        F.array_sort(F.collect_set("label")).alias("action_labels"),
        F.array_sort(F.collect_set("rule_id")).alias("matched_rule_ids"),
        # min_by over a struct rather than over priority alone, so ties between
        # equal-priority rules resolve to the same label on every run.
        F.expr("min_by(label, struct(priority, rule_id))").alias("primary_action_label"),
    )


# ── user-facing scene table ───────────────────────────────────────────────────


def _headline(signal_source: str, signal_name: str, column: str, alias: str):
    """Aggregate one well-known signal's feature into a flat scene column."""
    return F.max(
        F.when(
            (F.col("signal_source") == F.lit(signal_source)) & (F.col("signal_name") == F.lit(signal_name)),
            F.col(column),
        )
    ).alias(alias)


def _with_summary(frame):
    """Add scene_summary, generated by ai_query when an endpoint is configured."""
    if not SEMANTIC_MODEL_ENDPOINT:
        return frame.withColumn("scene_summary", F.lit(None).cast(StringType()))

    endpoint_sql = SEMANTIC_MODEL_ENDPOINT.replace("'", "''")
    prompt = (
        "You are summarizing one time segment of an automotive CAN log for an engineer. "
        "Write 1-2 plain sentences describing what happened. Do not invent signals. Facts: "
    )
    facts = F.concat_ws(
        " | ",
        F.concat(F.lit("duration="), F.round("duration_s", 2), F.lit("s")),
        F.concat(F.lit("actions="), F.concat_ws(", ", F.col("action_labels"))),
        F.concat(F.lit("cluster="), F.col("cluster_id"), F.lit(" anomaly="), F.round("anomaly_score", 0)),
        F.concat(
            F.lit("outliers="),
            F.concat_ws(
                "; ",
                F.transform(
                    F.col("top_features"),
                    lambda item: F.concat(item["feature"], F.lit("="), F.round(item["z"], 1), F.lit("z")),
                ),
            ),
        ),
    )
    # Gating by filtering a separate frame and joining back, rather than wrapping
    # ai_query in a CASE, makes the number of rows sent to the endpoint a
    # guarantee instead of an optimizer decision. blf_scenes is a materialized
    # view, so every refresh re-runs every call it makes.
    summaries = (
        frame.filter(F.col("anomaly_score") >= F.lit(SUMMARY_MIN_SCORE))
        .withColumn("_facts", facts)
        .limit(SUMMARY_MAX_ROWS)
        .select(
            "scene_id",
            F.expr(f"ai_query('{endpoint_sql}', concat('{prompt}', _facts))").alias("scene_summary"),
        )
    )
    return frame.join(summaries, "scene_id", "left")


@dlt.table(
    name="blf_scenes",
    comment=(
        "One row per scene: a time section of a BLF log, with the driver actions a rule "
        "matched, the behaviour cluster it belongs to, and how anomalous it is. "
        "anomaly_score is 0-100, blending the scene's distance from its cluster centroid, "
        "the magnitude of its robust feature z-scores, and how rare its cluster is; "
        "top_features lists the signals driving that score, with signed z. "
        "n_features_present says how many features the score was computed from -- a scene "
        "scored from four features is not comparable to one scored from two hundred. "
        "scene_summary is populated only when blf.semantic_model_endpoint is set."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["_source_file", "segment_index"],
)
def blf_scenes():
    features = dlt.read("blf_scene_signal_features")

    per_scene = features.groupBy("_source_file", "scene_id", "segment_index").agg(
        F.min("seg_start_s").alias("seg_start_s"),
        F.max("seg_end_s").alias("seg_end_s"),
        F.min("event_time_start").alias("event_time_start"),
        F.countDistinct("signal_key").alias("n_signals"),
        F.sum("n_samples").alias("n_samples"),
        _headline("CAN", "VehicleSpeed_kmh", "mean_v", "speed_mean_kmh"),
        _headline("CAN", "VehicleSpeed_kmh", "max_v", "speed_max_kmh"),
        _headline("CAN", "BrakePressure_bar", "max_v", "brake_max_bar"),
        _headline("CAN", "AccelPedal_pct", "max_v", "accel_max_pct"),
        _headline("CAN", "EngineSpeed_rpm", "mean_v", "engine_mean_rpm"),
        _headline("CAN", "SteeringAngle_deg", "range_v", "steering_range_deg"),
    )

    z_per_scene = (
        dlt.read("blf_scene_feature_z")
        .groupBy("scene_id")
        .agg(
            F.sqrt(F.avg(F.pow(F.col("abs_z"), F.lit(2.0)))).alias("z_agg"),
            F.max("abs_z").alias("z_max"),
            F.count(F.lit(1)).cast("int").alias("n_features_present"),
            F.slice(
                # sort_array on a struct orders by its first field, so abs_z
                # leads the struct purely to drive the ordering.
                F.sort_array(
                    F.collect_list(F.struct(F.col("abs_z").alias("_abs_z"), "feature_key", "z")),
                    asc=False,
                ),
                1,
                5,
            ).alias("_top"),
        )
        .withColumn(
            "top_features",
            F.transform(
                F.col("_top"),
                lambda item: F.struct(item["feature_key"].alias("feature"), item["z"].alias("z")),
            ),
        )
        .drop("_top")
    )

    assignments = dlt.read("blf_scene_cluster_assignments")
    distance_stats = assignments.groupBy("cluster_id").agg(
        F.expr("percentile_approx(centroid_distance, 0.5)").alias("d_median")
    )
    distance_mad = (
        assignments.join(F.broadcast(distance_stats), "cluster_id")
        .withColumn("abs_dev", F.abs(F.col("centroid_distance") - F.col("d_median")))
        .groupBy("cluster_id")
        .agg(F.expr("percentile_approx(abs_dev, 0.5)").alias("d_mad"))
    )

    scored = (
        per_scene.join(z_per_scene, "scene_id", "left")
        .join(assignments, "scene_id", "left")
        .join(F.broadcast(distance_stats), "cluster_id", "left")
        .join(F.broadcast(distance_mad), "cluster_id", "left")
        .join(dlt.read("blf_scene_labels").drop("_source_file"), "scene_id", "left")
        .withColumn("duration_s", F.col("seg_end_s") - F.col("seg_start_s"))
        .withColumn(
            # Normalizing the distance within its own cluster stops a naturally
            # diffuse cluster from reading as uniformly anomalous.
            "distance_z",
            (F.col("centroid_distance") - F.col("d_median")) / F.greatest(F.lit(1.4826) * F.col("d_mad"), F.lit(1e-9)),
        )
        # Bits of surprise: a cluster holding 1% of scenes scores 6.6.
        .withColumn("rarity_bits", -F.log2(F.greatest(F.col("cluster_share"), F.lit(1e-9))))
        .withColumn(
            "anomaly_score",
            _anomaly_score_udf(F.col("distance_z"), F.col("z_agg"), F.col("rarity_bits")),
        )
        .withColumn("primary_action_label", F.coalesce(F.col("primary_action_label"), F.lit(DEFAULT_LABEL)))
        .withColumn("action_labels", F.coalesce(F.col("action_labels"), F.array().cast(ArrayType(StringType()))))
        .withColumn("matched_rule_ids", F.coalesce(F.col("matched_rule_ids"), F.array().cast(ArrayType(StringType()))))
    )

    return _with_summary(scored).select(
        "_source_file",
        "scene_id",
        "segment_index",
        "seg_start_s",
        "seg_end_s",
        "duration_s",
        "event_time_start",
        "n_signals",
        F.col("n_samples").cast(LongType()).alias("n_samples"),
        "primary_action_label",
        "action_labels",
        "matched_rule_ids",
        "cluster_id",
        "cluster_size",
        "cluster_share",
        "k_used",
        "centroid_distance",
        "distance_z",
        "z_agg",
        "z_max",
        "n_features_present",
        "rarity_bits",
        "anomaly_score",
        "top_features",
        "scene_summary",
        "speed_mean_kmh",
        "speed_max_kmh",
        "brake_max_bar",
        "accel_max_pct",
        "engine_mean_rpm",
        "steering_range_deg",
    )


@dlt.table(
    name="blf_scene_clusters",
    comment=(
        "One row per behaviour cluster: how many scenes it holds, how tight it is, and "
        "which action label dominates it. Read this to name a cluster; cluster_name is "
        "generated by ai_query when blf.semantic_model_endpoint is set, at one call per "
        "cluster."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def blf_scene_clusters():
    profile = (
        dlt.read("blf_scenes")
        .groupBy("cluster_id")
        .agg(
            F.count(F.lit(1)).cast("int").alias("n_scenes"),
            # cluster_share and k_used are constant within a cluster, so max just
            # picks that constant -- F.first would be order-nondeterministic for
            # no benefit.
            F.max("cluster_share").alias("cluster_share"),
            F.max("k_used").alias("k_used"),
            F.expr("percentile_approx(centroid_distance, 0.5)").alias("median_distance"),
            F.avg("duration_s").alias("mean_duration_s"),
            F.avg("anomaly_score").alias("mean_anomaly_score"),
            F.avg("speed_mean_kmh").alias("mean_speed_kmh"),
            F.array_sort(F.collect_set("primary_action_label")).alias("action_labels"),
        )
    )
    if not SEMANTIC_MODEL_ENDPOINT:
        return profile.withColumn("cluster_name", F.lit(None).cast(StringType()))

    endpoint_sql = SEMANTIC_MODEL_ENDPOINT.replace("'", "''")
    prompt = (
        "Name this cluster of automotive drive-log segments in at most five words, "
        "as a noun phrase. Reply with the name only. Cluster: "
    )
    facts = F.concat_ws(
        " | ",
        F.concat(F.lit("labels="), F.concat_ws(", ", F.col("action_labels"))),
        F.concat(F.lit("mean_speed_kmh="), F.round("mean_speed_kmh", 1)),
        F.concat(F.lit("mean_duration_s="), F.round("mean_duration_s", 1)),
    )
    return (
        profile.withColumn("_facts", facts)
        .select(
            "*",
            F.expr(f"ai_query('{endpoint_sql}', concat('{prompt}', _facts))").alias("cluster_name"),
        )
        .drop("_facts")
    )
