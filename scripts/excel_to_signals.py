#!/usr/bin/env python3
"""
excel_to_signals.py -- Convert an Excel sheet to a signal-DB CSV

Reads one sheet from an Excel workbook and maps its columns to the fields
required by vector-blf-rs CAN or SOME/IP signal CSVs.

Each --<field> flag accepts either a 0-based integer column index or the
exact header string from the first row of the Excel sheet.

CAN output (default):
    message_id,signal_name,start_byte,start_bit,bit_length,
    byte_order,is_signed,scale,offset[,pdu_id]

SOME/IP output (--mode someip):
    service_id,method_id,signal_name,start_byte,start_bit,
    bit_length,byte_order,is_signed,scale,offset

Usage (CAN):
    uv run python scripts/excel_to_signals.py signals.xlsx \\
        --message-id 0 --signal-name 1 \\
        --start-byte 2 --start-bit 3 --bit-length 4 \\
        --byte-order 5 --is-signed 6 --scale 7 --offset 8 \\
        -o can_signals.csv

Usage (SOME/IP):
    uv run python scripts/excel_to_signals.py signals.xlsx --mode someip \\
        --service-id 0 --method-id 1 --signal-name 2 \\
        --start-byte 3 --start-bit 4 --bit-length 5 \\
        --byte-order 6 --is-signed 7 --scale 8 --offset 9 \\
        -o someip_signals.csv

Byte-order normalization (case-insensitive):
  "intel", "little", "little_endian", "le", "l" -> Intel
  "motorola", "big", "big_endian", "be", "m", "b" -> Motorola
  numeric: 0 -> Intel, 1 -> Motorola

is_signed normalization (case-insensitive):
  "true", "yes", "1", "signed", "t", "y" -> true
  "false", "no", "0", "unsigned", "f", "n" -> false
"""

import argparse
import csv
import sys
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Value normalization helpers
# ---------------------------------------------------------------------------

_INTEL_TOKENS = {"intel", "little", "little_endian", "little-endian", "le", "l"}
_MOTOROLA_TOKENS = {"motorola", "big", "big_endian", "big-endian", "be", "m", "b"}
_TRUE_TOKENS = {"true", "yes", "1", "signed", "t", "y"}
_FALSE_TOKENS = {"false", "no", "0", "unsigned", "f", "n"}


def normalize_byte_order(raw: object) -> str:
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
    """Parse an integer ID (decimal or hex string) and return it as 0x<HEX>."""
    if isinstance(raw, float):
        raw = int(raw)
    if isinstance(raw, int):
        return f"0x{raw:X}"
    s = str(raw).strip()
    if s.lower().startswith("0x"):
        return f"0x{int(s, 16):X}"
    return f"0x{int(s, 0):X}"


def normalize_int(raw: object, field: str) -> int:
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, int):
        return raw
    try:
        return int(str(raw).strip(), 0)
    except ValueError:
        raise ValueError(f"invalid integer for {field}: {raw!r}")


def normalize_float(raw: object, field: str) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(str(raw).strip())
    except ValueError:
        raise ValueError(f"invalid float for {field}: {raw!r}")


# ---------------------------------------------------------------------------
# Column resolver
# ---------------------------------------------------------------------------


def resolve_col(spec: str, columns: list[str]) -> int:
    """Resolve a column spec (0-based int or header name) to a positional index."""
    try:
        idx = int(spec)
        if idx < 0 or idx >= len(columns):
            raise SystemExit(f"Column index {idx} out of range (sheet has {len(columns)} columns)")
        return idx
    except ValueError:
        pass
    try:
        return columns.index(spec)
    except ValueError:
        raise SystemExit(f"Column {spec!r} not found. Available headers: {columns}")


# ---------------------------------------------------------------------------
# Row extraction
# ---------------------------------------------------------------------------


def get_cell(row: pd.Series, idx: int) -> object:
    return row.iloc[idx]


def is_empty(v: object) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return str(v).strip() == ""


# ---------------------------------------------------------------------------
# CAN row builder
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


def build_can_row(
    row: pd.Series,
    col: dict[str, int],
    lineno: int,
    errors: list[str],
) -> Optional[dict[str, str]]:
    def get(field: str) -> object:
        idx = col.get(field)
        return get_cell(row, idx) if idx is not None else None

    message_id_raw = get("message_id")
    signal_name_raw = get("signal_name")
    if is_empty(message_id_raw) or is_empty(signal_name_raw):
        return None  # skip blank rows silently

    try:
        result: dict[str, str] = {
            "message_id": normalize_id(message_id_raw),
            "signal_name": str(signal_name_raw).strip(),
            "start_byte": str(normalize_int(get("start_byte"), "start_byte")),
            "start_bit": str(normalize_int(get("start_bit"), "start_bit")),
            "bit_length": str(normalize_int(get("bit_length"), "bit_length")),
            "byte_order": normalize_byte_order(get("byte_order")),
            "is_signed": normalize_is_signed(get("is_signed")),
            "scale": str(normalize_float(get("scale"), "scale")),
            "offset": str(normalize_float(get("offset"), "offset")),
        }
        pdu_id_raw = get("pdu_id")
        if not is_empty(pdu_id_raw):
            result["pdu_id"] = normalize_id(pdu_id_raw)
        return result
    except (ValueError, TypeError) as exc:
        errors.append(f"row {lineno}: {exc}")
        return None


# ---------------------------------------------------------------------------
# SOME/IP row builder
# ---------------------------------------------------------------------------

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


def build_someip_row(
    row: pd.Series,
    col: dict[str, int],
    lineno: int,
    errors: list[str],
) -> Optional[dict[str, str]]:
    def get(field: str) -> object:
        idx = col.get(field)
        return get_cell(row, idx) if idx is not None else None

    service_id_raw = get("service_id")
    signal_name_raw = get("signal_name")
    if is_empty(service_id_raw) or is_empty(signal_name_raw):
        return None

    try:
        return {
            "service_id": normalize_id(service_id_raw),
            "method_id": normalize_id(get("method_id")),
            "signal_name": str(signal_name_raw).strip(),
            "start_byte": str(normalize_int(get("start_byte"), "start_byte")),
            "start_bit": str(normalize_int(get("start_bit"), "start_bit")),
            "bit_length": str(normalize_int(get("bit_length"), "bit_length")),
            "byte_order": normalize_byte_order(get("byte_order")),
            "is_signed": normalize_is_signed(get("is_signed")),
            "scale": str(normalize_float(get("scale"), "scale")),
            "offset": str(normalize_float(get("offset"), "offset")),
        }
    except (ValueError, TypeError) as exc:
        errors.append(f"row {lineno}: {exc}")
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert an Excel sheet to a vector-blf-rs signal-DB CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input", metavar="XLSX", help="Input Excel file")
    parser.add_argument("-o", "--output", metavar="CSV", help="Output CSV file (default: stdout)")
    parser.add_argument(
        "--mode", choices=["can", "someip"], default="can", help="Output format: CAN (default) or SOME/IP"
    )
    parser.add_argument("--sheet", default=0, metavar="SHEET", help="Sheet name or 0-based index (default: 0)")
    parser.add_argument(
        "--header-row",
        type=int,
        default=0,
        metavar="N",
        help="0-based row index that contains column headers (default: 0)",
    )

    # CAN fields
    can_group = parser.add_argument_group("CAN column mapping")
    can_group.add_argument("--message-id", metavar="COL")
    can_group.add_argument("--pdu-id", metavar="COL", help="Optional: marks rows as container-frame signals")

    # SOME/IP fields
    someip_group = parser.add_argument_group("SOME/IP column mapping")
    someip_group.add_argument("--service-id", metavar="COL")
    someip_group.add_argument("--method-id", metavar="COL")

    # Shared fields
    shared_group = parser.add_argument_group("shared column mapping (required)")
    shared_group.add_argument("--signal-name", metavar="COL", required=True)
    shared_group.add_argument("--start-byte", metavar="COL", required=True)
    shared_group.add_argument("--start-bit", metavar="COL", required=True)
    shared_group.add_argument("--bit-length", metavar="COL", required=True)
    shared_group.add_argument("--byte-order", metavar="COL", required=True)
    shared_group.add_argument("--is-signed", metavar="COL", required=True)
    shared_group.add_argument("--scale", metavar="COL", required=True)
    shared_group.add_argument("--offset", metavar="COL", required=True)

    args = parser.parse_args()

    # Resolve sheet identifier
    sheet = args.sheet
    try:
        sheet = int(sheet)
    except (ValueError, TypeError):
        pass

    try:
        df = pd.read_excel(args.input, sheet_name=sheet, header=args.header_row, dtype=object)
    except Exception as exc:
        print(f"Error reading {args.input}: {exc}", file=sys.stderr)
        return 1

    columns: list[str] = list(df.columns.astype(str))

    # Build field -> column index map
    col: dict[str, int] = {}
    col["signal_name"] = resolve_col(args.signal_name, columns)
    col["start_byte"] = resolve_col(args.start_byte, columns)
    col["start_bit"] = resolve_col(args.start_bit, columns)
    col["bit_length"] = resolve_col(args.bit_length, columns)
    col["byte_order"] = resolve_col(args.byte_order, columns)
    col["is_signed"] = resolve_col(args.is_signed, columns)
    col["scale"] = resolve_col(args.scale, columns)
    col["offset"] = resolve_col(args.offset, columns)

    if args.mode == "can":
        if not args.message_id:
            parser.error("--message-id is required for CAN mode")
        col["message_id"] = resolve_col(args.message_id, columns)
        if args.pdu_id:
            col["pdu_id"] = resolve_col(args.pdu_id, columns)
    else:
        if not args.service_id:
            parser.error("--service-id is required for SOME/IP mode")
        if not args.method_id:
            parser.error("--method-id is required for SOME/IP mode")
        col["service_id"] = resolve_col(args.service_id, columns)
        col["method_id"] = resolve_col(args.method_id, columns)

    # Process rows
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    # pandas uses 0-based index starting after the header; add 2 for 1-based Excel rows
    row_offset = args.header_row + 2

    for i, (_, row) in enumerate(df.iterrows()):
        lineno = i + row_offset
        if args.mode == "can":
            result = build_can_row(row, col, lineno, errors)
        else:
            result = build_someip_row(row, col, lineno, errors)
        if result is not None:
            rows.append(result)

    if errors:
        for e in errors:
            print(f"warning: {e}", file=sys.stderr)

    if not rows:
        print("No signal rows produced.", file=sys.stderr)
        return 1

    # Choose fieldnames
    if args.mode == "can":
        has_pdu = any("pdu_id" in r for r in rows)
        fieldnames = CAN_FIELDNAMES_CONTAINER if has_pdu else CAN_FIELDNAMES
    else:
        fieldnames = SOMEIP_FIELDNAMES

    if args.output:
        f = open(args.output, "w", newline="", encoding="utf-8")
    else:
        import io

        f = io.TextIOWrapper(sys.stdout.buffer, newline="")  # type: ignore[assignment]

    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    if args.output:
        f.close()
        print(f"Wrote {len(rows)} signal(s) to {args.output}", file=sys.stderr)

    if errors:
        print(f"{len(errors)} row(s) skipped due to errors.", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
