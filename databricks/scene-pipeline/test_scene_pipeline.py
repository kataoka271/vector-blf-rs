"""Unit tests for the pure-Python helpers in scene_pipeline.

The Spark and DLT surfaces are stubbed by conftest.py, so only the segmentation,
rule-compilation, clustering and scoring maths are exercised here.
"""

import math

import numpy as np
import pandas as pd
import pytest
import scene_pipeline as sp

# ── build_segments tests ──────────────────────────────────────────────────────

_NO_CP = np.array([], dtype=float)


def _segments(**kwargs):
    params = {
        "boundaries": _NO_CP,
        "strengths": _NO_CP,
        "t_start": 0.0,
        "t_end": 100.0,
        "window_seconds": 10.0,
        "min_segment_seconds": 2.0,
        "max_segments": 500,
    }
    params.update(kwargs)
    return sp.build_segments(**params)


def test_build_segments_plain_grid() -> None:
    segments = _segments()
    assert len(segments) == 10
    assert all(math.isclose(end - start, 10.0) for start, end, _ in segments)
    assert not any(from_cp for _, _, from_cp in segments)


def test_build_segments_tiles_without_gaps_or_overlaps() -> None:
    segments = _segments(boundaries=np.array([3.7, 41.2, 88.9]), strengths=np.array([5.0, 9.0, 4.0]))
    assert segments[0][0] == 0.0
    assert segments[-1][1] == 100.0
    for (_, prev_end, _), (next_start, _, _) in zip(segments, segments[1:]):
        assert prev_end == next_start


def test_build_segments_marks_changepoint_origin() -> None:
    segments = _segments(t_end=20.0, boundaries=np.array([5.0]), strengths=np.array([7.0]))
    assert segments == [(0.0, 5.0, False), (5.0, 10.0, True), (10.0, 20.0, False)]


def test_build_segments_drops_boundaries_closer_than_minimum() -> None:
    # 10.5 sits 0.5 s after the grid boundary at 10.0, below the 2 s floor.
    segments = _segments(t_end=30.0, boundaries=np.array([10.5]), strengths=np.array([6.0]))
    starts = [start for start, _, _ in segments]
    assert 10.0 in starts
    assert 10.5 not in starts


def test_build_segments_never_emits_a_short_segment() -> None:
    noisy = np.linspace(0.1, 99.9, 400)
    segments = _segments(boundaries=noisy, strengths=np.full(noisy.shape, 3.0))
    assert all(end - start >= 2.0 for start, end, _ in segments)


def test_build_segments_merges_trailing_runt_backwards() -> None:
    segments = _segments(t_end=100.5)
    assert segments[-1] == (90.0, 100.5, False)


def test_build_segments_cap_degrades_toward_the_grid() -> None:
    noisy = np.linspace(0.5, 99.5, 300)
    segments = _segments(boundaries=noisy, strengths=np.full(noisy.shape, 100.0), max_segments=5)
    assert len(segments) <= 6
    assert segments[0][0] == 0.0
    assert segments[-1][1] == 100.0


def test_build_segments_handles_zero_length_file() -> None:
    assert _segments(t_start=7.0, t_end=7.0) == [(7.0, 7.0, False)]


def test_build_segments_handles_file_shorter_than_minimum() -> None:
    segments = _segments(t_end=1.0)
    assert segments == [(0.0, 1.0, False)]


def test_segments_for_file_shapes_the_declared_schema() -> None:
    pdf = pd.DataFrame(
        {
            "_source_file": ["a.blf"] * 2,
            "t_min_s": [0.0] * 2,
            "t_max_s": [40.0] * 2,
            "boundary_s": [5.0, np.nan],
            "strength": [8.0, np.nan],
        }
    )
    out = sp._segments_for_file(pdf)
    assert list(out.columns) == [f.name for f in sp._SEGMENTS_SCHEMA.fields]
    assert out["_source_file"].unique().tolist() == ["a.blf"]
    assert out["segment_index"].tolist() == list(range(len(out)))
    assert out["is_last"].tolist() == [False] * (len(out) - 1) + [True]
    assert out["seg_end_s"].iloc[-1] == 40.0


# ── parse_weights tests ───────────────────────────────────────────────────────


def test_parse_weights_normalizes_to_one() -> None:
    weights = sp.parse_weights("1,1,2")
    assert math.isclose(sum(weights), 1.0)
    assert weights == (0.25, 0.25, 0.5)


def test_parse_weights_rejects_wrong_arity() -> None:
    with pytest.raises(ValueError, match="expected 3"):
        sp.parse_weights("0.5,0.5")


def test_parse_weights_rejects_non_positive_total() -> None:
    with pytest.raises(ValueError, match="positive"):
        sp.parse_weights("0,0,0")


# ── normalize_rules tests ─────────────────────────────────────────────────────


def _rule_row(**overrides) -> dict:
    row = {
        "rule_id": "brake",
        "condition_index": "0",
        "label": "pressed the brake",
        "priority": "10",
        "signal_source": "CAN",
        "channel": "",
        "signal_name": "BrakePressure_bar",
        "feature": "max_v",
        "op": "gt",
        "value_a": "5",
        "value_b": "",
        "negate": "false",
        "description": "",
    }
    row.update(overrides)
    return row


def _condition_fields(condition):
    _, _, _, channel_key, _, _, transform, lo, hi, negate = condition
    return channel_key, transform, lo, hi, negate


def test_normalize_rules_compiles_metadata() -> None:
    conditions, meta = sp.normalize_rules([_rule_row(), _rule_row(condition_index="1", feature="delta_v")])
    assert len(conditions) == 2
    assert meta == [("brake", "pressed the brake", 10, 2)]


def test_normalize_rules_maps_gt_to_a_strict_lower_bound() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(op="gt", value_a="5")])
    _, transform, lo, hi, _negate = _condition_fields(condition)
    assert transform == "identity"
    assert lo > 5.0
    assert lo == math.nextafter(5.0, math.inf)
    assert hi == math.inf


def test_normalize_rules_maps_ge_to_an_inclusive_bound() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(op="ge", value_a="5")])
    assert _condition_fields(condition)[2] == 5.0


def test_normalize_rules_maps_lt_to_a_strict_upper_bound() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(op="lt", value_a="5")])
    _, _, lo, hi, _ = _condition_fields(condition)
    assert lo == -math.inf
    assert hi < 5.0


def test_normalize_rules_maps_between_to_a_closed_interval() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(op="between", feature="mean_v", value_a="500", value_b="1000")])
    _, _, lo, hi, _ = _condition_fields(condition)
    assert (lo, hi) == (500.0, 1000.0)


def test_normalize_rules_marks_absolute_operators() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(op="abs_gt", value_a="2.5")])
    assert _condition_fields(condition)[1] == "abs"


def test_normalize_rules_defaults_blank_channel_to_a_wildcard() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(channel="")])
    assert _condition_fields(condition)[0] == "*"
    (condition,), _ = sp.normalize_rules([_rule_row(channel="1")])
    assert _condition_fields(condition)[0] == "1"


def test_normalize_rules_parses_negate() -> None:
    (condition,), _ = sp.normalize_rules([_rule_row(negate="true")])
    assert _condition_fields(condition)[4] is True
    (condition,), _ = sp.normalize_rules([_rule_row(negate="")])
    assert _condition_fields(condition)[4] is False


def test_normalize_rules_rejects_unknown_op() -> None:
    with pytest.raises(ValueError, match="unknown op"):
        sp.normalize_rules([_rule_row(op="approximately")])


def test_normalize_rules_rejects_unknown_feature() -> None:
    with pytest.raises(ValueError, match="unknown feature"):
        sp.normalize_rules([_rule_row(feature="vibe")])


def test_normalize_rules_rejects_between_without_upper_bound() -> None:
    with pytest.raises(ValueError, match="needs value_b"):
        sp.normalize_rules([_rule_row(op="between")])


def test_normalize_rules_rejects_conflicting_labels() -> None:
    with pytest.raises(ValueError, match="conflicting labels"):
        sp.normalize_rules([_rule_row(), _rule_row(condition_index="1", label="something else")])


def test_normalize_rules_rejects_conflicting_priorities() -> None:
    with pytest.raises(ValueError, match="conflicting priorities"):
        sp.normalize_rules([_rule_row(), _rule_row(condition_index="1", priority="99")])


def test_normalize_rules_skips_blank_rule_ids() -> None:
    conditions, meta = sp.normalize_rules([_rule_row(rule_id="  ")])
    assert conditions == []
    assert meta == []


def test_shipped_rules_csv_compiles() -> None:
    """The rules shipped in assets/ must survive normalize_rules unchanged."""
    from pathlib import Path

    csv_path = Path(__file__).resolve().parents[2] / "assets" / "scene_rules.csv"
    rows = pd.read_csv(csv_path, dtype=str, keep_default_na=False).to_dict("records")
    conditions, meta = sp.normalize_rules(rows)
    assert len(meta) > 0
    assert len(conditions) == len(rows)
    # Multi-condition rules must declare their conditions contiguously from 0.
    by_rule: dict[str, list[int]] = {}
    for condition in conditions:
        by_rule.setdefault(condition[0], []).append(condition[1])
    for rule_id, indexes in by_rule.items():
        assert sorted(indexes) == list(range(len(indexes))), rule_id


# ── robust_scale tests ────────────────────────────────────────────────────────


def test_robust_scale_centers_on_the_median() -> None:
    x = np.array([[1.0], [2.0], [3.0], [4.0], [5.0]])
    scaled, median, _ = sp.robust_scale(x)
    assert median[0] == 3.0
    assert scaled[2, 0] == 0.0


def test_robust_scale_survives_a_constant_column() -> None:
    x = np.array([[1.0, 7.0], [2.0, 7.0], [3.0, 7.0]])
    scaled, _, scale = sp.robust_scale(x)
    assert scale[1] == 1.0
    assert np.isfinite(scaled).all()


def test_robust_scale_resists_a_single_outlier() -> None:
    x = np.array([[1.0], [2.0], [3.0], [4.0], [1000.0]])
    scaled, _, _ = sp.robust_scale(x)
    # The bulk stays within a few units even though the outlier is 1000.
    assert abs(scaled[0, 0]) < 5.0


# ── kmeans tests ──────────────────────────────────────────────────────────────


def _three_blobs(seed: int = 0, per_blob: int = 20) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    centers = np.array([[0.0, 0.0], [30.0, 0.0], [0.0, 30.0]])
    points = np.vstack([c + rng.normal(0, 0.4, size=(per_blob, 2)) for c in centers])
    truth = np.repeat(np.arange(3), per_blob)
    return points, truth


def test_kmeans_fit_recovers_separated_blobs() -> None:
    points, truth = _three_blobs()
    _, labels, _ = sp.kmeans_fit(points, 3, seed=1)
    for blob in range(3):
        assigned = labels[truth == blob]
        assert len(set(assigned.tolist())) == 1
    assert len(set(labels.tolist())) == 3


def test_kmeans_fit_is_deterministic_for_a_given_seed() -> None:
    points, _ = _three_blobs()
    first = sp.kmeans_fit(points, 3, seed=7)[1]
    second = sp.kmeans_fit(points, 3, seed=7)[1]
    assert first.tolist() == second.tolist()


def test_kmeans_fit_clamps_k_to_the_sample_count() -> None:
    points = np.array([[0.0], [1.0]])
    centers, labels, _ = sp.kmeans_fit(points, 10, seed=0)
    assert centers.shape[0] == 2
    assert labels.shape[0] == 2


def test_kmeans_fit_produces_no_nan_centroids() -> None:
    # Duplicated points force clusters to empty out during Lloyd's iteration.
    points = np.repeat(np.array([[0.0, 0.0], [1.0, 1.0]]), 10, axis=0)
    centers, _, _ = sp.kmeans_fit(points, 6, seed=3)
    assert np.isfinite(centers).all()


def test_canonical_order_sorts_by_descending_size() -> None:
    centers = np.array([[0.0], [1.0], [2.0]])
    counts = np.array([1, 5, 3])
    assert sp.canonical_order(centers, counts).tolist() == [1, 2, 0]


def test_canonical_order_is_invariant_to_cluster_numbering() -> None:
    centers = np.array([[0.0], [1.0], [2.0]])
    counts = np.array([4, 9, 2])
    permutation = np.array([2, 0, 1])
    reordered = sp.canonical_order(centers[permutation], counts[permutation])
    # Both orderings must name the same centroid as the largest cluster.
    assert centers[sp.canonical_order(centers, counts)[0]] == centers[permutation][reordered[0]]


def test_mean_silhouette_prefers_the_true_cluster_count() -> None:
    points, truth = _three_blobs()
    distances = np.linalg.norm(points[:, None, :] - points[None, :, :], axis=-1)
    good = sp.mean_silhouette(distances, truth, 3)
    merged = sp.mean_silhouette(distances, np.where(truth == 2, 1, truth), 2)
    assert good > merged


def test_choose_k_finds_three_blobs() -> None:
    points, _ = _three_blobs(per_blob=30)
    assert sp.choose_k(points, 2, 6, seed=5) == 3


def test_cluster_scenes_returns_the_declared_schema() -> None:
    points, _ = _three_blobs(per_blob=15)
    pdf = pd.DataFrame(
        {
            "scene_id": [f"f.blf#{i}" for i in range(len(points))],
            "feature_vector": list(points),
        }
    )
    out = sp.cluster_scenes(pdf)
    assert list(out.columns) == [f.name for f in sp._ASSIGN_SCHEMA.fields]
    assert len(out) == len(pdf)
    assert (out["centroid_distance"] >= 0).all()
    assert math.isclose(out.groupby("cluster_id")["cluster_share"].first().sum(), 1.0, rel_tol=1e-9)
    assert out["cluster_size"].sum() == sum(
        out.groupby("cluster_id")["cluster_size"].first() * out.groupby("cluster_id").size()
    )


def test_cluster_scenes_tolerates_non_finite_features() -> None:
    pdf = pd.DataFrame(
        {
            "scene_id": [f"f.blf#{i}" for i in range(12)],
            "feature_vector": [np.array([float(i), np.nan, np.inf]) for i in range(12)],
        }
    )
    out = sp.cluster_scenes(pdf)
    assert np.isfinite(out["centroid_distance"]).all()


# ── anomaly_score tests ───────────────────────────────────────────────────────


def _score(distance_z=0.0, z_agg=0.0, rarity=0.0) -> float:
    return float(
        sp.anomaly_score(np.array([distance_z]), np.array([z_agg]), np.array([rarity]), (0.45, 0.40, 0.15), 3.0)[0]
    )


def test_anomaly_score_is_zero_for_a_typical_scene() -> None:
    assert _score() == 0.0


def test_anomaly_score_is_bounded() -> None:
    # Saturates at exactly 100 once the exponential underflows, never past it.
    assert _score(distance_z=1e6, z_agg=1e6, rarity=1e6) == 100.0
    assert 0.0 < _score(distance_z=6.0, z_agg=4.0, rarity=3.0) < 100.0


def test_anomaly_score_is_monotone_in_each_term() -> None:
    assert _score(distance_z=1.0) < _score(distance_z=5.0)
    assert _score(z_agg=1.0) < _score(z_agg=5.0)
    assert _score(rarity=1.0) < _score(rarity=5.0)


def test_anomaly_score_ignores_better_than_typical_terms() -> None:
    # A scene closer to its centroid than the median must not score below zero.
    assert _score(distance_z=-10.0) == 0.0
    assert _score(distance_z=-10.0, z_agg=3.0) == _score(z_agg=3.0)


def test_anomaly_score_weights_are_respected() -> None:
    distance_only = sp.anomaly_score(np.array([4.0]), np.array([0.0]), np.array([0.0]), (1.0, 0.0, 0.0), 3.0)[0]
    z_only = sp.anomaly_score(np.array([0.0]), np.array([4.0]), np.array([0.0]), (0.0, 1.0, 0.0), 3.0)[0]
    assert math.isclose(distance_only, z_only)
