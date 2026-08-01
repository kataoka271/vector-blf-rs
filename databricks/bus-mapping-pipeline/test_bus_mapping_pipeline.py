"""Unit tests for the pure-Python helpers in bus_mapping_pipeline.

The Spark and DLT surfaces are stubbed by conftest.py, so only the id parsing,
reference-data joining, and Hungarian-assignment maths are exercised here.
"""

from pathlib import Path

import bus_mapping_pipeline as bmp
import pandas as pd
import pytest

# ── _parse_message_id tests ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0x100", 0x100),
        ("0X1a", 0x1A),
        ("100", 100),
        ("1A", 0x1A),  # not valid decimal -> falls back to hex
        ("  0x200  ", 0x200),
        ("", None),
        ("   ", None),
        (None, None),
        ("zz", None),
    ],
)
def test_parse_message_id(raw, expected) -> None:
    assert bmp._parse_message_id(raw) == expected


# ── _load_id_to_ecu tests ─────────────────────────────────────────────────────


def test_load_id_to_ecu_parses_hex_and_decimal(monkeypatch) -> None:
    monkeypatch.setattr(
        bmp,
        "_load_rows",
        lambda path, table: [
            {"message_id": "0x100", "sender_ecu": "ECU_A"},
            {"message_id": "257", "sender_ecu": "ECU_A"},
            {"message_id": "0x200", "sender_ecu": "ECU_B"},
        ],
    )
    result = bmp._load_id_to_ecu("path", "table", "message_id")
    assert result == {"ECU_A": {0x100, 257}, "ECU_B": {0x200}}


def test_load_id_to_ecu_skips_blank_ecu_or_unparseable_id(monkeypatch) -> None:
    monkeypatch.setattr(
        bmp,
        "_load_rows",
        lambda path, table: [
            {"message_id": "0x100", "sender_ecu": ""},
            {"message_id": "not-a-number", "sender_ecu": "ECU_A"},
            {"message_id": "0x200", "sender_ecu": "ECU_B"},
        ],
    )
    result = bmp._load_id_to_ecu("path", "table", "message_id")
    assert result == {"ECU_B": {0x200}}


def test_load_id_to_ecu_returns_empty_for_no_rows(monkeypatch) -> None:
    monkeypatch.setattr(bmp, "_load_rows", lambda path, table: [])
    assert bmp._load_id_to_ecu("path", "table", "service_id") == {}


# ── _build_bus_native_rows tests ──────────────────────────────────────────────


def test_build_bus_native_rows_fans_out_a_gateway_and_keeps_protocols_separate(monkeypatch) -> None:
    ecu_bus_rows = [
        {"ecu": "ECU_ENGINE", "bus_name": "Powertrain"},
        {"ecu": "ECU_GATEWAY", "bus_name": "Powertrain"},
        {"ecu": "ECU_GATEWAY", "bus_name": "Body"},
    ]
    message_sender_rows = [{"message_id": "0x100", "sender_ecu": "ECU_ENGINE"}]
    # Same numeric value as the CAN id above -- must not collide across protocols.
    someip_rows = [{"service_id": "0x100", "sender_ecu": "ECU_GATEWAY"}]

    calls = iter([ecu_bus_rows, message_sender_rows, someip_rows])
    monkeypatch.setattr(bmp, "_load_rows", lambda path, table: next(calls))

    pairs = set(bmp._build_bus_native_rows())

    assert pairs == {
        ("CAN", "Powertrain", 0x100),
        ("SOMEIP", "Powertrain", 0x100),
        ("SOMEIP", "Body", 0x100),
    }


def test_build_bus_native_rows_ecu_with_no_traffic_contributes_nothing(monkeypatch) -> None:
    ecu_bus_rows = [{"ecu": "ECU_QUIET", "bus_name": "Body"}]
    calls = iter([ecu_bus_rows, [], []])
    monkeypatch.setattr(bmp, "_load_rows", lambda path, table: next(calls))
    assert bmp._build_bus_native_rows() == []


def test_shipped_reference_csvs_produce_the_expected_bus_native_mapping(monkeypatch) -> None:
    """The demo CSVs shipped in assets/ must survive the join unchanged."""
    root = Path(__file__).resolve().parents[2] / "assets"
    ecu_bus_rows = pd.read_csv(root / "ecu_bus.csv", dtype=str, keep_default_na=False).to_dict("records")
    message_sender_rows = pd.read_csv(root / "message_sender.csv", dtype=str, keep_default_na=False).to_dict("records")
    someip_rows = pd.read_csv(root / "someip_service_sender.csv", dtype=str, keep_default_na=False).to_dict("records")

    calls = iter([ecu_bus_rows, message_sender_rows, someip_rows])
    monkeypatch.setattr(bmp, "_load_rows", lambda path, table: next(calls))

    pairs = set(bmp._build_bus_native_rows())
    can_pairs = {p for p in pairs if p[0] == "CAN"}
    someip_pairs = {p for p in pairs if p[0] == "SOMEIP"}

    assert can_pairs == {
        ("CAN", "Powertrain", 0x100),
        ("CAN", "Powertrain", 0x101),
        ("CAN", "Powertrain", 0x110),
        ("CAN", "Body", 0x200),
        ("CAN", "Body", 0x201),
        ("CAN", "Chassis", 0x300),
        ("CAN", "Chassis", 0x301),
    }
    # ECU_GATEWAY (Powertrain/Body/Chassis) provides 0x1234 -- fans out to all
    # three buses; ECU_BCM (Body only) and ECU_ABS (Chassis only) do not.
    assert someip_pairs == {
        ("SOMEIP", "Powertrain", 0x1234),
        ("SOMEIP", "Body", 0x1234),
        ("SOMEIP", "Body", 0x1235),
        ("SOMEIP", "Chassis", 0x1234),
        ("SOMEIP", "Chassis", 0x2001),
    }


# ── _solve_assignment tests ───────────────────────────────────────────────────


def _score_row(source_file, signal_source, channel, bus_name, score, recall=None, precision=None, jaccard=None):
    return {
        "_source_file": source_file,
        "signal_source": signal_source,
        "channel": channel,
        "bus_name": bus_name,
        "score": score,
        "recall": score if recall is None else recall,
        "precision": score if precision is None else precision,
        "jaccard": score if jaccard is None else jaccard,
    }


_SCORE_COLUMNS = ["_source_file", "signal_source", "channel", "bus_name", "score", "recall", "precision", "jaccard"]


def test_solve_assignment_returns_the_declared_schema_for_an_empty_group() -> None:
    out = bmp._solve_assignment(pd.DataFrame(columns=_SCORE_COLUMNS))
    assert list(out.columns) == [f.name for f in bmp._ASSIGN_SCHEMA.fields]
    assert len(out) == 0


def test_solve_assignment_matches_a_clean_two_by_two(monkeypatch) -> None:
    monkeypatch.setattr(bmp, "MIN_SCORE", 0.3)
    pdf = pd.DataFrame(
        [
            _score_row("f.blf", "CAN", 0, "Powertrain", 1.0),
            _score_row("f.blf", "CAN", 0, "Body", 0.0),
            _score_row("f.blf", "CAN", 1, "Powertrain", 0.0),
            _score_row("f.blf", "CAN", 1, "Body", 1.0),
        ]
    )
    out = bmp._solve_assignment(pdf)

    assert list(out.columns) == [f.name for f in bmp._ASSIGN_SCHEMA.fields]
    matched = dict(zip(out["channel"], out["matched_bus"]))
    assert matched == {0: "Powertrain", 1: "Body"}
    assert (out["note"] == "").all()
    assert out["_source_file"].unique().tolist() == ["f.blf"]
    assert out["signal_source"].unique().tolist() == ["CAN"]


def test_solve_assignment_finds_the_global_optimum_not_a_greedy_match() -> None:
    # Row-wise argmax would send both channels to bus A (a conflict): channel 0
    # wants A (0.9 > 0.8) and channel 1 wants A (1.0 > 0.1). The optimal
    # one-to-one assignment instead swaps channel 0 onto B for a higher total
    # score (0.8 + 1.0 = 1.8 vs. 0.9 + 0.1 if channel 1 were bumped to B).
    pdf = pd.DataFrame(
        [
            _score_row("f.blf", "CAN", 0, "A", 0.9),
            _score_row("f.blf", "CAN", 0, "B", 0.8),
            _score_row("f.blf", "CAN", 1, "A", 1.0),
            _score_row("f.blf", "CAN", 1, "B", 0.1),
        ]
    )
    out = bmp._solve_assignment(pdf)
    matched = dict(zip(out["channel"], out["matched_bus"]))
    assert matched == {0: "B", 1: "A"}


def test_solve_assignment_flags_a_low_score_for_review(monkeypatch) -> None:
    monkeypatch.setattr(bmp, "MIN_SCORE", 0.5)
    pdf = pd.DataFrame([_score_row("f.blf", "CAN", 0, "Powertrain", 0.2)])
    out = bmp._solve_assignment(pdf)
    row = out.iloc[0]
    assert row["matched_bus"] == "Powertrain"
    assert row["note"] == "score below threshold, needs review"


def test_solve_assignment_accepts_a_score_at_the_threshold(monkeypatch) -> None:
    monkeypatch.setattr(bmp, "MIN_SCORE", 0.5)
    pdf = pd.DataFrame([_score_row("f.blf", "CAN", 0, "Powertrain", 0.5)])
    out = bmp._solve_assignment(pdf)
    assert out.iloc[0]["note"] == ""


def test_solve_assignment_flags_an_unmatched_channel_when_buses_run_out(monkeypatch) -> None:
    monkeypatch.setattr(bmp, "MIN_SCORE", 0.3)
    pdf = pd.DataFrame(
        [
            _score_row("f.blf", "CAN", 0, "Powertrain", 0.9),
            _score_row("f.blf", "CAN", 1, "Powertrain", 0.4),
        ]
    )
    out = bmp._solve_assignment(pdf)
    by_channel = out.set_index("channel")

    assert by_channel.loc[0, "matched_bus"] == "Powertrain"
    assert pd.isna(by_channel.loc[1, "matched_bus"])
    assert by_channel.loc[1, "note"] == "no bus candidate available"
    assert by_channel.loc[1, "score"] == 0.0


def test_solve_assignment_is_scoped_to_its_own_group() -> None:
    """signal_source/_source_file are read from the group, not recomputed per row."""
    pdf = pd.DataFrame(
        [
            _score_row("drive002.blf", "SOMEIP", 3, "Backbone", 0.7),
            _score_row("drive002.blf", "SOMEIP", 4, "Backbone", 0.1),
        ]
    )
    out = bmp._solve_assignment(pdf)
    assert out["_source_file"].unique().tolist() == ["drive002.blf"]
    assert out["signal_source"].unique().tolist() == ["SOMEIP"]
