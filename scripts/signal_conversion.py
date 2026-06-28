"""
signal_conversion.py -- Shared Excel-to-signal-CSV conversion helpers.

Used by:
  scripts/excel_to_signals.py          (local CLI tool)
  databricks/signal-importer/import_signals_task.py  (Databricks job task)
"""

from __future__ import annotations

import io
import re
from typing import IO, Callable, Optional, TypeVar, Union

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

# ---------------------------------------------------------------------------
# CSV field name lists (authoritative schema)
# ---------------------------------------------------------------------------

CAN_FIELDNAMES = [
    "message_id",
    "signal_name",
    "start_byte",
    "start_bit",
    "bit_length",
    "byte_order",
    "is_signed",
    "scale",
    "offset",
]
CAN_FIELDNAMES_CONTAINER = CAN_FIELDNAMES + ["pdu_id"]

SOMEIP_FIELDNAMES = [
    "service_id",
    "method_id",
    "signal_name",
    "start_byte",
    "start_bit",
    "bit_length",
    "byte_order",
    "is_signed",
    "scale",
    "offset",
]

ENUM_FIELDNAMES = ["signal_name", "raw_value", "category"]

# ---------------------------------------------------------------------------
# Null / NA detection
# ---------------------------------------------------------------------------

_NA_SENTINELS = frozenset({"-", "not available", "n/a", "na", "none", "null"})


def is_empty(v: object) -> bool:
    """True for None, NaN, empty string, or known NA placeholder tokens."""
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return str(v).strip().lower() in _NA_SENTINELS | {""}


# ---------------------------------------------------------------------------
# Multi-base integer parsing
# ---------------------------------------------------------------------------

_TRAILING_H_RE = re.compile(r"^([0-9A-Fa-f]+)[hH]$")


def parse_flexible_int(raw: object) -> int:
    """Parse decimal, 0x hex, 0b binary, or <hex_digits>h notation.

    Raises ValueError on failure.
    """
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, int):
        return raw
    s = str(raw).strip()
    m = _TRAILING_H_RE.match(s)
    if m:
        return int(m.group(1), 16)
    sl = s.lower()
    if sl.startswith("0x"):
        return int(s, 16)
    if sl.startswith("0b"):
        return int(s, 2)
    return int(s, 10)


# ---------------------------------------------------------------------------
# Normalizers (raise ValueError on bad non-null input)
# ---------------------------------------------------------------------------

_INTEL_TOKENS = {"intel", "little", "little_endian", "little-endian", "le", "l"}
_MOTOROLA_TOKENS = {"motorola", "big", "big_endian", "big-endian", "be", "m", "b"}
_TRUE_TOKENS = {"true", "yes", "1", "signed", "t", "y"}
_FALSE_TOKENS = {"false", "no", "0", "unsigned", "f", "n"}


def normalize_byte_order(raw: object) -> str:
    """Normalize a byte_order value to "Intel" or "Motorola"."""
    if isinstance(raw, (int, float)):
        if int(raw) == 0:
            return "Intel"
        if int(raw) == 1:
            return "Motorola"
        raise ValueError(f"unrecognized byte_order value: {raw!r}")
    s = str(raw).strip().lower()
    if s in _INTEL_TOKENS:
        return "Intel"
    if s in _MOTOROLA_TOKENS:
        return "Motorola"
    raise ValueError(f"unrecognized byte_order value: {raw!r}")


def normalize_is_signed(raw: object) -> str:
    """Normalize a boolean or string to "true" or "false"."""
    if isinstance(raw, bool):
        return "true" if raw else "false"
    if isinstance(raw, (int, float)):
        return "true" if int(raw) != 0 else "false"
    s = str(raw).strip().lower()
    if s in _TRUE_TOKENS:
        return "true"
    if s in _FALSE_TOKENS:
        return "false"
    raise ValueError(f"unrecognized is_signed value: {raw!r}")


def normalize_id(raw: object) -> str:
    """Parse an integer ID (decimal, 0x hex, 0b binary, or NNh) and return as 0x<HEX>."""
    return f"0x{parse_flexible_int(raw):X}"


def normalize_int(raw: object, field: str) -> int:
    """Parse an integer from a string or numeric value, raising ValueError on failure."""
    try:
        return parse_flexible_int(raw)
    except (ValueError, TypeError):
        raise ValueError(f"invalid integer for {field}: {raw!r}")


def normalize_float(raw: object, field: str) -> float:
    """Parse a float from a string or numeric value, raising ValueError on failure."""
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(str(raw).strip())
    except ValueError:
        raise ValueError(f"invalid float for {field}: {raw!r}")


# ---------------------------------------------------------------------------
# Nullable variants (return None for null / unparseable values)
# ---------------------------------------------------------------------------


def normalize_nullable_int(raw: object, field: str) -> Optional[int]:
    if is_empty(raw):
        return None
    try:
        return parse_flexible_int(raw)
    except (ValueError, TypeError):
        return None


def normalize_nullable_float(raw: object, field: str) -> Optional[float]:
    if is_empty(raw):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(str(raw).strip())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Vectorized column helpers (private)
# ---------------------------------------------------------------------------


def _is_empty_series(s: pd.Series) -> pd.Series:
    """Vectorized is_empty check across an entire column."""
    null_mask = s.isna()
    sentinel_mask = s.fillna("").astype(str).str.strip().str.lower().isin(_NA_SENTINELS | {""})
    return null_mask | sentinel_mask


T = TypeVar("T")


def _safe_call(fn, v: T) -> tuple[Optional[T], Optional[str]]:
    """Call fn(v), returning (result, None) on success or (None, error_str) on failure."""
    try:
        return fn(v), None
    except (ValueError, TypeError) as exc:
        return None, str(exc)


def _norm_col(fn: Callable[[object], object], series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Normalize a Series in one pass, returning (value_series, error_series)."""
    pairs = [_safe_call(fn, v) for v in series]
    return (
        pd.Series([p[0] for p in pairs], index=series.index),
        pd.Series([p[1] for p in pairs], index=series.index, dtype=object),
    )


def _nones(len: int) -> pd.Series:
    """Return a Series of None values with the given shape."""
    return pd.Series([None] * len, index=range(len), dtype=object)


_NORMALIZERS: dict[str, Callable[[pd.Series], tuple[pd.Series, pd.Series]]] = {
    "message_id": lambda s: _norm_col(normalize_id, s),
    "service_id": lambda s: _norm_col(normalize_id, s),
    "method_id": lambda s: _norm_col(normalize_id, s),
    "pdu_id": lambda s: _norm_col(lambda v: normalize_id(v) if not is_empty(v) else None, s),
    "signal_name": lambda s: (s.astype(str).str.strip().where(~_is_empty_series(s)), _nones(len(s))),
    "start_byte": lambda s: _norm_col(lambda v: normalize_nullable_int(v, "start_byte"), s),
    "start_bit": lambda s: _norm_col(lambda v: normalize_nullable_int(v, "start_bit"), s),
    "bit_length": lambda s: _norm_col(lambda v: normalize_nullable_int(v, "bit_length"), s),
    "byte_order": lambda s: _norm_col(normalize_byte_order, s),
    "is_signed": lambda s: _norm_col(normalize_is_signed, s),
    "scale": lambda s: (_norm_col(lambda v: normalize_nullable_float(v, "scale"), s)[0].fillna(1.0), _nones(len(s))),
    "offset": lambda s: (_norm_col(lambda v: normalize_nullable_float(v, "offset"), s)[0].fillna(0.0), _nones(len(s))),
}


# ---------------------------------------------------------------------------
# Pandera schemas for normalized signal DataFrames
# ---------------------------------------------------------------------------

_HEX_RE = r"^0x[0-9A-Fa-f]+$"


def _hex_check(s: pd.Series) -> pd.Series:
    """True for hex ID strings; True for nulls (null-check is handled separately)."""
    return s.str.match(_HEX_RE).fillna(True)


_COMMON_SIGNAL_COLUMNS = {
    "signal_name": pa.Column(str, nullable=False),
    "start_byte": pa.Column(int, pa.Check.ge(0), nullable=False, coerce=True),
    "start_bit": pa.Column(int, pa.Check.ge(0), nullable=False, coerce=True),
    "bit_length": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
    "byte_order": pa.Column(str, pa.Check.isin(["Intel", "Motorola"]), nullable=False),
    "is_signed": pa.Column(str, pa.Check.isin(["true", "false"]), nullable=False),
    "scale": pa.Column(float, nullable=False, coerce=True),
    "offset": pa.Column(float, nullable=False, coerce=True),
}

CAN_SIGNAL_SCHEMA = pa.DataFrameSchema(
    {
        "message_id": pa.Column(str, pa.Check(_hex_check, error="must be 0x<HEX>"), nullable=False),
        "pdu_id": pa.Column(str, pa.Check(_hex_check, error="must be 0x<HEX>"), nullable=True, required=False),
        **_COMMON_SIGNAL_COLUMNS,
    },
    coerce=True,
)

SOMEIP_SIGNAL_SCHEMA = pa.DataFrameSchema(
    {
        "service_id": pa.Column(str, pa.Check(_hex_check, error="must be 0x<HEX>"), nullable=False),
        "method_id": pa.Column(str, pa.Check(_hex_check, error="must be 0x<HEX>"), nullable=False),
        **_COMMON_SIGNAL_COLUMNS,
    },
    coerce=True,
)


# ---------------------------------------------------------------------------
# Typed DataFrame → string dict converter
# ---------------------------------------------------------------------------


def _df_to_signal_dicts(df: pd.DataFrame) -> list[dict[str, str]]:
    """Convert a pandera-validated signal DataFrame to list of string dicts for CSV."""
    out = df.copy()
    for c in ("start_byte", "start_bit", "bit_length"):
        out[c] = out[c].astype(int).astype(str)
    out["scale"] = out["scale"].astype(str)
    out["offset"] = out["offset"].astype(str)

    pdu_col: Optional[pd.Series] = None
    if "pdu_id" in out.columns:
        pdu_col = out.pop("pdu_id")

    records: list[dict[str, str]] = out.to_dict("records")  # type: ignore[assignment]

    if pdu_col is not None:
        for rec, pdu_val in zip(records, pdu_col):
            if pd.notna(pdu_val):
                rec["pdu_id"] = str(pdu_val)

    return records


# ---------------------------------------------------------------------------
# Column resolver
# ---------------------------------------------------------------------------


def resolve_col(spec: Union[str, int], columns: list[str]) -> int:
    """Resolve a column spec (0-based int or header name) to a positional index."""
    if isinstance(spec, int):
        if spec < 0 or spec >= len(columns):
            raise SystemExit(f"Column index {spec} out of range (sheet has {len(columns)} columns)")
        return spec
    try:
        idx = int(spec)
        if idx < 0 or idx >= len(columns):
            raise SystemExit(f"Column index {idx} out of range (sheet has {len(columns)} columns)")
        return idx
    except (ValueError, TypeError):
        pass
    try:
        return columns.index(str(spec))
    except ValueError:
        raise SystemExit(f"Column {spec!r} not found. Available headers: {columns}")


def resolve_column_map(
    mapping: dict[str, Union[str, int]],
    columns: list[str],
) -> dict[str, int]:
    """Convert a COLUMN_MAP dict (field -> header_or_index) to field -> positional index."""
    return {field: resolve_col(spec, columns) for field, spec in mapping.items()}


# ---------------------------------------------------------------------------
# Full column-wise conversion
# ---------------------------------------------------------------------------


def convert_dataframe(
    df: pd.DataFrame,
    col: dict[str, int],
    mode: str,
    errors: list[str],
    header_row: int = 0,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Convert a DataFrame to (signal_rows, enum_rows) using column-wise processing.

    mode: "can" | "someip"
    col:  field -> positional column index (from resolve_column_map)
    """
    row_offset = header_row + 2
    primary_key = "message_id" if mode == "can" else "service_id"
    required = [primary_key, "signal_name", "start_byte", "start_bit", "bit_length", "byte_order", "is_signed"]
    if mode == "someip":
        required.append("method_id")

    # ------------------------------------------------------------------
    # Step 1: select & rename columns
    # ------------------------------------------------------------------
    available = {k: v for k, v in col.items() if v is not None}
    rename_map = {v: k for k, v in available.items()}
    selected = df.iloc[:, sorted(rename_map)].rename(columns=rename_map).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Step 2: normalize each column (Result: value Series + error Series)
    # ------------------------------------------------------------------
    blank_mask = _is_empty_series(selected[primary_key]) | _is_empty_series(selected["signal_name"])
    normalized = selected.copy()
    _error: pd.Series = pd.Series([None] * len(selected), index=selected.index, dtype=object)

    for field, normalizer in _NORMALIZERS.items():
        if field not in selected.columns:
            continue
        val_s, err_s = normalizer(selected[field])
        normalized[field] = val_s
        # Suppress errors for blank rows; coalesce (first error per row wins)
        _error = _error.where(_error.notna(), err_s.where(~blank_mask))

    normalized["_error"] = _error

    # ------------------------------------------------------------------
    # Step 3: collect errors before dropping invalid rows
    # ------------------------------------------------------------------
    bad_rows = normalized.loc[normalized["_error"].notna(), "_error"]
    errors.extend(f"row {row_offset + idx}: {msg}" for idx, msg in bad_rows.items())

    # ------------------------------------------------------------------
    # Step 4: drop invalid rows
    # ------------------------------------------------------------------
    valid = normalized.drop(columns=["_error"]).dropna(subset=required)

    # ------------------------------------------------------------------
    # Validate with pandera (lazy=True collects all failures at once)
    # ------------------------------------------------------------------
    schema = CAN_SIGNAL_SCHEMA if mode == "can" else SOMEIP_SIGNAL_SCHEMA
    schema_cols = [c for c in schema.columns if c in valid.columns]
    valid_typed = valid[schema_cols].copy()

    failed: set[int] = set()
    try:
        validated_df = schema.validate(valid_typed, lazy=True)
    except SchemaErrors as exc:
        for _, fc in exc.failure_cases.iterrows():
            orig_pos = fc["index"]
            if orig_pos is not None and pd.notna(orig_pos):
                lineno = row_offset + int(orig_pos)
                failed.add(int(orig_pos))
            else:
                lineno = "?"
            col_name = str(fc["column"]) if pd.notna(fc["column"]) else "?"
            errors.append(f"row {lineno}: column {col_name!r} failed {fc['check']} (value: {fc['failure_case']!r})")
        validated_df = valid_typed.drop(index=list(failed))

    signal_rows = _df_to_signal_dicts(validated_df)
    final_valid_pos = list(validated_df.index)

    # ------------------------------------------------------------------
    # Enum rows (coding/meaning columns — after pandera filtering)
    # ------------------------------------------------------------------
    enum_rows: list[dict[str, str]] = []
    if col.get("coding") is not None and col.get("meaning") is not None:
        for i in final_valid_pos:
            enum_rows.extend(
                extract_enum_rows(
                    selected.at[i, "signal_name"],
                    selected.at[i, "coding"],
                    selected.at[i, "meaning"],
                    row_offset + i,
                    errors,
                )
            )

    return signal_rows, enum_rows


# ---------------------------------------------------------------------------
# Enum extraction from Coding / Meaning cell pair
# ---------------------------------------------------------------------------


def extract_enum_rows(
    signal_name: str,
    coding_raw: object,
    meaning_raw: object,
    lineno: int,
    errors: list[str],
) -> list[dict[str, str]]:
    """Parse a Coding/Meaning cell pair into zero or more enum mapping rows.

    Each Excel cell contains newline-separated tokens; the i-th coding token
    corresponds to the i-th meaning token.
    """
    if is_empty(coding_raw) or is_empty(meaning_raw):
        return []
    coding_lines = [s.strip() for s in str(coding_raw).splitlines() if s.strip()]
    meaning_lines = [s.strip() for s in str(meaning_raw).splitlines() if s.strip()]
    if len(coding_lines) != len(meaning_lines):
        n = min(len(coding_lines), len(meaning_lines))
        errors.append(
            f"row {lineno}: coding/meaning line count mismatch "
            f"({len(coding_lines)} vs {len(meaning_lines)}) for signal {signal_name!r}; "
            f"using first {n} pair(s)"
        )
        coding_lines = coding_lines[:n]
        meaning_lines = meaning_lines[:n]
    rows: list[dict[str, str]] = []
    for raw_token, label in zip(coding_lines, meaning_lines):
        try:
            raw_int = parse_flexible_int(raw_token)
            rows.append(
                {
                    "signal_name": signal_name,
                    "raw_value": str(raw_int),
                    "category": label,
                }
            )
        except (ValueError, TypeError):
            errors.append(f"row {lineno}: unparseable coding value {raw_token!r} for signal {signal_name!r} — skipped")
    return rows


# ---------------------------------------------------------------------------
# Password decryption
# ---------------------------------------------------------------------------


def decrypt_excel_or_passthrough(
    path: str,
    password: Optional[str],
) -> Union[str, IO[bytes]]:
    """Return path unchanged when no password; otherwise decrypt to BytesIO."""
    if not password:
        return path
    try:
        import msoffcrypto  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "msoffcrypto-tool is required for password-protected Excel files. "
            "Install with: pip install msoffcrypto-tool"
        )
    with open(path, "rb") as f:
        office = msoffcrypto.OfficeFile(f)
        office.load_key(password=password)
        buf = io.BytesIO()
        office.decrypt(buf)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Path helper (mirrors _local_path in dlt_blf_pipeline.py)
# ---------------------------------------------------------------------------


def uc_volume_to_local(spark_path: str) -> str:
    """Convert a Spark/DBFS URI to a local filesystem path."""
    if spark_path.startswith("dbfs:/Volumes/"):
        return spark_path[len("dbfs:") :]
    if spark_path.startswith("dbfs:/"):
        return "/dbfs/" + spark_path[len("dbfs:/") :]
    return spark_path
