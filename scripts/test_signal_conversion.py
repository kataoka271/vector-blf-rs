"""Tests for signal_conversion.py."""

import pandas as pd
import pytest
from signal_conversion import (
    CAN_FIELDNAMES,
    CAN_FIELDNAMES_CONTAINER,
    ENUM_FIELDNAMES,
    SOMEIP_FIELDNAMES,
    convert_dataframe,
    decrypt_excel_or_passthrough,
    extract_enum_rows,
    is_empty,
    normalize_byte_order,
    normalize_float,
    normalize_id,
    normalize_int,
    normalize_is_signed,
    normalize_nullable_float,
    normalize_nullable_int,
    parse_flexible_int,
    resolve_col,
    resolve_column_map,
    uc_volume_to_local,
)

# ---------------------------------------------------------------------------
# is_empty
# ---------------------------------------------------------------------------


class TestIsEmpty:
    def test_none(self):
        assert is_empty(None)

    def test_nan(self):
        assert is_empty(float("nan"))

    def test_empty_string(self):
        assert is_empty("")

    def test_whitespace_string(self):
        assert is_empty("   ")

    @pytest.mark.parametrize("sentinel", ["-", "not available", "n/a", "na", "none", "null"])
    def test_sentinels(self, sentinel):
        assert is_empty(sentinel)

    @pytest.mark.parametrize("sentinel", ["-", "NOT AVAILABLE", "N/A", "NA", "NONE", "NULL"])
    def test_sentinels_case_insensitive(self, sentinel):
        assert is_empty(sentinel)

    @pytest.mark.parametrize("value", [0, 1, 0.0, 1.5, "hello", "0x1A", False, True])
    def test_non_empty(self, value):
        assert not is_empty(value)


# ---------------------------------------------------------------------------
# parse_flexible_int
# ---------------------------------------------------------------------------


class TestParseFlexibleInt:
    def test_decimal(self):
        assert parse_flexible_int("42") == 42

    def test_negative_decimal(self):
        assert parse_flexible_int("-5") == -5

    def test_hex_0x(self):
        assert parse_flexible_int("0x1A") == 26

    def test_hex_0x_lower(self):
        assert parse_flexible_int("0xff") == 255

    def test_binary_0b(self):
        assert parse_flexible_int("0b1010") == 10

    def test_trailing_h_lower(self):
        assert parse_flexible_int("1Ah") == 26

    def test_trailing_H_upper(self):
        assert parse_flexible_int("FFH") == 255

    def test_int_passthrough(self):
        assert parse_flexible_int(99) == 99

    def test_float_passthrough(self):
        assert parse_flexible_int(3.7) == 3

    def test_invalid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            parse_flexible_int("xyz")


# ---------------------------------------------------------------------------
# normalize_byte_order
# ---------------------------------------------------------------------------


class TestNormalizeByteOrder:
    @pytest.mark.parametrize("raw", ["intel", "Intel", "little", "little_endian", "little-endian", "le", "LE", "l"])
    def test_intel_strings(self, raw):
        assert normalize_byte_order(raw) == "Intel"

    @pytest.mark.parametrize("raw", ["motorola", "Motorola", "big", "big_endian", "big-endian", "be", "BE", "m", "b"])
    def test_motorola_strings(self, raw):
        assert normalize_byte_order(raw) == "Motorola"

    def test_int_zero_is_intel(self):
        assert normalize_byte_order(0) == "Intel"

    def test_int_one_is_motorola(self):
        assert normalize_byte_order(1) == "Motorola"

    def test_float_zero_is_intel(self):
        assert normalize_byte_order(0.0) == "Intel"

    def test_invalid_int_raises(self):
        with pytest.raises(ValueError):
            normalize_byte_order(2)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            normalize_byte_order("unknown")


# ---------------------------------------------------------------------------
# normalize_is_signed
# ---------------------------------------------------------------------------


class TestNormalizeIsSigned:
    @pytest.mark.parametrize("raw", ["true", "True", "yes", "1", "signed", "t", "y"])
    def test_true_strings(self, raw):
        assert normalize_is_signed(raw) == "true"

    @pytest.mark.parametrize("raw", ["false", "False", "no", "0", "unsigned", "f", "n"])
    def test_false_strings(self, raw):
        assert normalize_is_signed(raw) == "false"

    def test_bool_true(self):
        assert normalize_is_signed(True) == "true"

    def test_bool_false(self):
        assert normalize_is_signed(False) == "false"

    def test_int_nonzero(self):
        assert normalize_is_signed(1) == "true"

    def test_int_zero(self):
        assert normalize_is_signed(0) == "false"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            normalize_is_signed("maybe")


# ---------------------------------------------------------------------------
# normalize_id
# ---------------------------------------------------------------------------


class TestNormalizeId:
    def test_decimal(self):
        assert normalize_id(256) == "0x100"

    def test_hex_string(self):
        assert normalize_id("0x1A") == "0x1A"

    def test_trailing_h(self):
        assert normalize_id("1Ah") == "0x1A"

    def test_zero(self):
        assert normalize_id(0) == "0x0"


# ---------------------------------------------------------------------------
# normalize_int / normalize_float
# ---------------------------------------------------------------------------


class TestNormalizeInt:
    def test_decimal_string(self):
        assert normalize_int("7", "start_byte") == 7

    def test_hex_string(self):
        assert normalize_int("0xFF", "bit_length") == 255

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="invalid integer"):
            normalize_int("abc", "start_bit")


class TestNormalizeFloat:
    def test_int(self):
        assert normalize_float(2, "scale") == 2.0

    def test_float(self):
        assert normalize_float(0.5, "scale") == 0.5

    def test_string(self):
        assert normalize_float("3.14", "scale") == pytest.approx(3.14)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="invalid float"):
            normalize_float("abc", "scale")


# ---------------------------------------------------------------------------
# normalize_nullable_int / normalize_nullable_float
# ---------------------------------------------------------------------------


class TestNormalizeNullableInt:
    def test_null_returns_none(self):
        assert normalize_nullable_int(None, "start_byte") is None

    def test_na_sentinel_returns_none(self):
        assert normalize_nullable_int("-", "start_byte") is None

    def test_valid(self):
        assert normalize_nullable_int("10", "start_byte") == 10

    def test_invalid_returns_none(self):
        assert normalize_nullable_int("xyz", "start_byte") is None


class TestNormalizeNullableFloat:
    def test_null_returns_none(self):
        assert normalize_nullable_float(None, "scale") is None

    def test_na_sentinel_returns_none(self):
        assert normalize_nullable_float("n/a", "scale") is None

    def test_valid_float(self):
        assert normalize_nullable_float(2.5, "scale") == 2.5

    def test_valid_string(self):
        assert normalize_nullable_float("0.1", "scale") == pytest.approx(0.1)

    def test_invalid_returns_none(self):
        assert normalize_nullable_float("xyz", "scale") is None


# ---------------------------------------------------------------------------
# resolve_col / resolve_column_map
# ---------------------------------------------------------------------------


class TestResolveCol:
    COLS = ["message_id", "signal_name", "start_byte"]

    def test_by_name(self):
        assert resolve_col("signal_name", self.COLS) == 1

    def test_by_int(self):
        assert resolve_col(2, self.COLS) == 2

    def test_by_string_int(self):
        assert resolve_col("0", self.COLS) == 0

    def test_name_not_found_raises(self):
        with pytest.raises(SystemExit):
            resolve_col("missing", self.COLS)

    def test_index_out_of_range_raises(self):
        with pytest.raises(SystemExit):
            resolve_col(10, self.COLS)


class TestResolveColumnMap:
    def test_mixed_spec(self):
        cols = ["message_id", "signal_name", "start_byte"]
        mapping = {"message_id": "message_id", "signal_name": 1}
        result = resolve_column_map(mapping, cols)
        assert result == {"message_id": 0, "signal_name": 1}


# ---------------------------------------------------------------------------
# extract_enum_rows
# ---------------------------------------------------------------------------


class TestExtractEnumRows:
    def test_empty_coding(self):
        assert extract_enum_rows("sig", None, "Idle", 1, []) == []

    def test_empty_meaning(self):
        assert extract_enum_rows("sig", "0", None, 1, []) == []

    def test_single_pair(self):
        rows = extract_enum_rows("sig", "0", "Idle", 1, [])
        assert rows == [{"signal_name": "sig", "raw_value": "0", "category": "Idle"}]

    def test_multiline_pairs(self):
        coding = "0\n1\n2"
        meaning = "Off\nOn\nError"
        rows = extract_enum_rows("sig", coding, meaning, 1, [])
        assert rows == [
            {"signal_name": "sig", "raw_value": "0", "category": "Off"},
            {"signal_name": "sig", "raw_value": "1", "category": "On"},
            {"signal_name": "sig", "raw_value": "2", "category": "Error"},
        ]

    def test_hex_coding(self):
        rows = extract_enum_rows("sig", "0x1A", "Active", 1, [])
        assert rows == [{"signal_name": "sig", "raw_value": "26", "category": "Active"}]

    def test_mismatch_truncates_and_warns(self):
        errors: list[str] = []
        rows = extract_enum_rows("sig", "0\n1\n2", "Off\nOn", 5, errors)
        assert len(rows) == 2
        assert any("mismatch" in e for e in errors)

    def test_invalid_coding_token_skipped(self):
        errors: list[str] = []
        rows = extract_enum_rows("sig", "0\nXXX\n2", "Off\nBad\nError", 1, errors)
        assert len(rows) == 2
        assert any("unparseable" in e for e in errors)


# ---------------------------------------------------------------------------
# uc_volume_to_local
# ---------------------------------------------------------------------------


class TestUcVolumeToLocal:
    def test_dbfs_volumes(self):
        assert uc_volume_to_local("dbfs:/Volumes/main/dev/raw/") == "/Volumes/main/dev/raw/"

    def test_dbfs_plain(self):
        assert uc_volume_to_local("dbfs:/user/hive/warehouse/") == "/dbfs/user/hive/warehouse/"

    def test_plain_path_unchanged(self):
        assert uc_volume_to_local("/local/path") == "/local/path"


# ---------------------------------------------------------------------------
# convert_dataframe (CAN and SOME/IP)
# ---------------------------------------------------------------------------


def _can_df():
    """Minimal valid CAN DataFrame (column order matches col map below)."""
    return pd.DataFrame(
        {
            "message_id": ["0x100", "0x200"],
            "signal_name": ["speed", "rpm"],
            "start_byte": [0, 2],
            "start_bit": [0, 0],
            "bit_length": [8, 16],
            "byte_order": ["Intel", "Motorola"],
            "is_signed": ["false", "true"],
            "scale": [0.5, 1.0],
            "offset": [0.0, 0.0],
        }
    )


_CAN_COL = {
    "message_id": 0,
    "signal_name": 1,
    "start_byte": 2,
    "start_bit": 3,
    "bit_length": 4,
    "byte_order": 5,
    "is_signed": 6,
    "scale": 7,
    "offset": 8,
}


class TestConvertDataframeCan:
    def test_happy_path(self):
        errors: list[str] = []
        signals, enums = convert_dataframe(_can_df(), _CAN_COL, "can", errors)
        assert len(signals) == 2
        assert enums == []
        assert errors == []

    def test_signal_fields_present(self):
        errors: list[str] = []
        signals, _ = convert_dataframe(_can_df(), _CAN_COL, "can", errors)
        for row in signals:
            for field in CAN_FIELDNAMES:
                assert field in row

    def test_blank_row_skipped(self):
        df = _can_df()
        blank = pd.DataFrame(
            [
                {
                    "message_id": None,
                    "signal_name": None,
                    "start_byte": 0,
                    "start_bit": 0,
                    "bit_length": 8,
                    "byte_order": "Intel",
                    "is_signed": "false",
                    "scale": 1.0,
                    "offset": 0.0,
                }
            ]
        )
        df = pd.concat([df, blank], ignore_index=True)
        errors: list[str] = []
        signals, _ = convert_dataframe(df, _CAN_COL, "can", errors)
        assert len(signals) == 2

    def test_invalid_byte_order_produces_error(self):
        df = _can_df()
        df.loc[0, "byte_order"] = "UNKNOWN"
        errors: list[str] = []
        signals, _ = convert_dataframe(df, _CAN_COL, "can", errors)
        assert len(signals) == 1
        assert any("byte_order" in e or "unrecognized" in e for e in errors)

    def test_scale_defaults_to_1(self):
        df = _can_df()
        df.loc[0, "scale"] = None
        errors: list[str] = []
        signals, _ = convert_dataframe(df, _CAN_COL, "can", errors)
        matched = [r for r in signals if r["signal_name"] == "speed"]
        assert matched and matched[0]["scale"] == "1.0"

    def test_offset_defaults_to_0(self):
        df = _can_df()
        df.loc[0, "offset"] = None
        errors: list[str] = []
        signals, _ = convert_dataframe(df, _CAN_COL, "can", errors)
        matched = [r for r in signals if r["signal_name"] == "speed"]
        assert matched and matched[0]["offset"] == "0.0"

    def test_enum_rows_extracted(self):
        df = _can_df()
        df["coding"] = ["0\n1", None]
        df["meaning"] = ["Off\nOn", None]
        col = {**_CAN_COL, "coding": 9, "meaning": 10}
        errors: list[str] = []
        signals, enums = convert_dataframe(df, col, "can", errors)
        assert len(enums) == 2
        assert enums[0]["signal_name"] == "speed"


def _someip_df():
    return pd.DataFrame(
        {
            "service_id": ["0x10", "0x10"],
            "method_id": ["0x01", "0x02"],
            "signal_name": ["temp", "pressure"],
            "start_byte": [0, 2],
            "start_bit": [0, 0],
            "bit_length": [16, 8],
            "byte_order": ["Intel", "Intel"],
            "is_signed": ["true", "false"],
            "scale": [0.1, 1.0],
            "offset": [0.0, 0.0],
        }
    )


_SOMEIP_COL = {
    "service_id": 0,
    "method_id": 1,
    "signal_name": 2,
    "start_byte": 3,
    "start_bit": 4,
    "bit_length": 5,
    "byte_order": 6,
    "is_signed": 7,
    "scale": 8,
    "offset": 9,
}


class TestConvertDataframeSomeip:
    def test_happy_path(self):
        errors: list[str] = []
        signals, enums = convert_dataframe(_someip_df(), _SOMEIP_COL, "someip", errors)
        assert len(signals) == 2
        assert enums == []
        assert errors == []

    def test_signal_fields_present(self):
        errors: list[str] = []
        signals, _ = convert_dataframe(_someip_df(), _SOMEIP_COL, "someip", errors)
        for row in signals:
            for field in SOMEIP_FIELDNAMES:
                assert field in row

    def test_invalid_message_id_produces_error(self):
        df = _someip_df()
        df.loc[0, "service_id"] = "ZZZZ"
        errors: list[str] = []
        signals, _ = convert_dataframe(df, _SOMEIP_COL, "someip", errors)
        assert len(signals) == 1
        assert errors


# ---------------------------------------------------------------------------
# Field name list sanity checks
# ---------------------------------------------------------------------------


def test_can_fieldnames_subset_of_container():
    assert all(f in CAN_FIELDNAMES_CONTAINER for f in CAN_FIELDNAMES)
    assert "pdu_id" in CAN_FIELDNAMES_CONTAINER


def test_enum_fieldnames():
    assert ENUM_FIELDNAMES == ["signal_name", "raw_value", "category"]


# ---------------------------------------------------------------------------
# decrypt_excel_or_passthrough (no password path only)
# ---------------------------------------------------------------------------


def test_decrypt_no_password_returns_path():
    assert decrypt_excel_or_passthrough("/some/file.xlsx", None) == "/some/file.xlsx"
    assert decrypt_excel_or_passthrough("/some/file.xlsx", "") == "/some/file.xlsx"
