#!/usr/bin/env python3
"""
excel_to_signals.py -- Convert an Excel sheet to signal-DB CSV(s)

Reads one sheet from an Excel workbook and maps its columns to the fields
required by vector-blf-rs CAN or SOME/IP signal CSVs. An optional Coding/Meaning
column pair generates an enum_values.csv sidecar for categorical signals.

Column mapping can be specified three ways (applied in order, later wins):
  1. --mapping-file <file.py>  Python file defining a COLUMN_MAP dict
  2. --map KEY=COL             one-off overrides (repeatable)
  3. --<field> COL             per-field flags (backward-compatible)

COL is either a 0-based column index or the exact header string.

CAN output (default):
    message_id,signal_name,start_byte,start_bit,bit_length,
    byte_order,is_signed,scale,offset[,pdu_id]

SOME/IP output (--mode someip):
    service_id,method_id,signal_name,start_byte,start_bit,
    bit_length,byte_order,is_signed,scale,offset

Enum sidecar (when --coding-col and --meaning-col are set):
    signal_name,raw_value,category

Numeric field cleansing:
  "-", blank, "Not Available", "N/A", "None" -> NA (row skipped / default used)
  scale / offset: NA -> defaults 1.0 / 0.0

Numeric base formats (start_byte, start_bit, bit_length, IDs, coding values):
  0x1A  -> hex    0b101 -> binary    1Ah / FFh -> hex    plain digits -> decimal
  Unparseable -> NA

Byte-order normalization (case-insensitive):
  "intel", "little", "le", "l" -> Intel
  "motorola", "big", "be", "m", "b" -> Motorola
  numeric: 0 -> Intel, 1 -> Motorola

is_signed normalization (case-insensitive):
  "true", "yes", "1", "signed", "t", "y" -> true
  "false", "no", "0", "unsigned", "f", "n" -> false

Example -- CAN with mapping file:
    uv run python databricks/signal-importer/excel_to_signals.py signals.xlsx \\
        --mapping-file my_mapping.py \\
        --coding-col "Coding" --meaning-col "Meaning" \\
        --enum-output enum_values.csv -o can_signals.csv

Example -- CAN with per-field flags (backward-compatible):
    uv run python databricks/signal-importer/excel_to_signals.py signals.xlsx \\
        --message-id 0 --signal-name 1 \\
        --start-byte 2 --start-bit 3 --bit-length 4 \\
        --byte-order 5 --is-signed 6 --scale 7 --offset 8 \\
        -o can_signals.csv

Example -- encrypted Excel:
    uv run python databricks/signal-importer/excel_to_signals.py signals.xlsx \\
        --password "s3cr3t" --mapping-file my_mapping.py -o can_signals.csv

Example mapping file (my_mapping.py):
    COLUMN_MAP = {
        "message_id":  "CAN ID",
        "signal_name": "Signal Name",
        "start_byte":  "Start Byte",
        "start_bit":   "Start Bit",
        "bit_length":  "Bit Length",
        "byte_order":  "Byte Order",
        "is_signed":   "Signed",
        "scale":       "Factor",
        "offset":      "Offset",
        "coding":      "Coding",   # newline-separated raw enum values
        "meaning":     "Meaning",  # newline-separated enum labels (1:1 with coding)
        # "pdu_id":   "PDU ID",    # uncomment for container-frame signals
    }
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
from typing import Union

from signal_conversion import (
    CAN_FIELDNAMES,
    CAN_FIELDNAMES_CONTAINER,
    ENUM_FIELDNAMES,
    SOMEIP_FIELDNAMES,
    convert_excel,
    load_excel,
)

# ---------------------------------------------------------------------------
# Mapping file loader
# ---------------------------------------------------------------------------


def _load_mapping_file(path: str) -> dict[str, Union[str, int]]:
    """Load COLUMN_MAP dict from a plain Python file."""
    spec = importlib.util.spec_from_file_location("_signal_mapping", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Cannot load mapping file {path!r}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    mapping = getattr(mod, "COLUMN_MAP", None)
    if not isinstance(mapping, dict):
        raise SystemExit(f"--mapping-file {path!r}: must define a dict named COLUMN_MAP")
    return mapping


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_REQUIRED_SHARED = [
    "signal_name",
    "start_byte",
    "start_bit",
    "bit_length",
    "byte_order",
    "is_signed",
    "scale",
    "offset",
]
_REQUIRED_CAN = ["message_id"]
_REQUIRED_SOMEIP = ["service_id", "method_id"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert an Excel sheet to vector-blf-rs signal-DB CSV(s)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input", metavar="XLSX", help="Input Excel file")
    parser.add_argument("-o", "--output", metavar="CSV", help="Output signal CSV (default: stdout)")
    parser.add_argument("--mode", choices=["can", "someip"], default="can", help="Output format (default: can)")
    parser.add_argument("--sheet", default=0, metavar="SHEET", help="Sheet name or 0-based index (default: 0)")
    parser.add_argument(
        "--header-row",
        type=int,
        default=0,
        metavar="N",
        help="0-based row index that contains column headers (default: 0)",
    )
    parser.add_argument("--password", metavar="PWD", help="Password for encrypted Excel file")

    # File-based mapping
    map_group = parser.add_argument_group("column mapping (file-based)")
    map_group.add_argument(
        "--mapping-file",
        metavar="PY",
        help="Python file defining COLUMN_MAP dict (field -> column header or 0-based index)",
    )
    map_group.add_argument(
        "--map",
        metavar="KEY=COL",
        action="append",
        default=[],
        help="Override or add one column mapping, e.g. --map message_id=0 (repeatable)",
    )

    # Enum extraction
    enum_group = parser.add_argument_group("enum extraction")
    enum_group.add_argument("--coding-col", metavar="COL", help="Column with newline-separated raw enum values")
    enum_group.add_argument(
        "--meaning-col",
        metavar="COL",
        help="Column with newline-separated enum category labels (1:1 with coding)",
    )
    enum_group.add_argument("--enum-output", metavar="PATH", help="Write enum_values.csv sidecar to this path")

    # CAN per-field flags
    can_group = parser.add_argument_group("CAN column mapping (per-flag)")
    can_group.add_argument("--message-id", metavar="COL")
    can_group.add_argument("--pdu-id", metavar="COL", help="Optional: marks rows as container-frame signals")

    # SOME/IP per-field flags
    someip_group = parser.add_argument_group("SOME/IP column mapping (per-flag)")
    someip_group.add_argument("--service-id", metavar="COL")
    someip_group.add_argument("--method-id", metavar="COL")

    # Shared per-field flags
    shared_group = parser.add_argument_group("shared column mapping (per-flag)")
    shared_group.add_argument("--signal-name", metavar="COL")
    shared_group.add_argument("--start-byte", metavar="COL")
    shared_group.add_argument("--start-bit", metavar="COL")
    shared_group.add_argument("--bit-length", metavar="COL")
    shared_group.add_argument("--byte-order", metavar="COL")
    shared_group.add_argument("--is-signed", metavar="COL")
    shared_group.add_argument("--scale", metavar="COL")
    shared_group.add_argument("--offset", metavar="COL")

    args = parser.parse_args()

    # Load and optionally decrypt Excel
    try:
        df = load_excel(args.input, args.password, args.sheet, args.header_row)
    except ImportError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error reading {args.input}: {exc}", file=sys.stderr)
        return 1

    # Build raw_mapping: mapping file -> --map overrides -> per-field flags
    raw_mapping: dict[str, Union[str, int]] = {}

    if args.mapping_file:
        try:
            raw_mapping = _load_mapping_file(args.mapping_file)
        except SystemExit:
            raise
        except Exception as exc:
            print(f"Error loading --mapping-file: {exc}", file=sys.stderr)
            return 1

    for entry in args.map:
        if "=" not in entry:
            print(f"Invalid --map entry {entry!r}: expected KEY=COL", file=sys.stderr)
            return 1
        key, _, val = entry.partition("=")
        raw_mapping[key.strip()] = val.strip()

    # Per-field flags override everything else
    flag_specs: list[tuple[str, object]] = [
        ("signal_name", args.signal_name),
        ("start_byte", args.start_byte),
        ("start_bit", args.start_bit),
        ("bit_length", args.bit_length),
        ("byte_order", args.byte_order),
        ("is_signed", args.is_signed),
        ("scale", args.scale),
        ("offset", args.offset),
        ("coding", args.coding_col),
        ("meaning", args.meaning_col),
    ]
    if args.mode == "can":
        flag_specs += [("message_id", args.message_id), ("pdu_id", args.pdu_id)]
    else:
        flag_specs += [("service_id", args.service_id), ("method_id", args.method_id)]

    for field, flag_val in flag_specs:
        if flag_val is not None:
            raw_mapping[field] = str(flag_val)

    # Validate required fields
    required = _REQUIRED_SHARED + (_REQUIRED_CAN if args.mode == "can" else _REQUIRED_SOMEIP)
    missing = [f for f in required if f not in raw_mapping]
    if missing:
        if args.mapping_file:
            parser.error(
                f"fields missing from mapping: {', '.join(missing)}. "
                f"Add to --mapping-file or use --map / per-field flags."
            )
        else:
            # Legacy: give flag-style error messages
            for field in missing:
                flag = "--" + field.replace("_", "-")
                parser.error(f"{flag} is required (or use --mapping-file)")

    # Resolve column mapping and convert rows
    try:
        signal_rows, enum_rows, errors = convert_excel(df, raw_mapping, args.mode, args.header_row)
    except SystemExit:
        raise

    for e in errors:
        print(f"warning: {e}", file=sys.stderr)

    if not signal_rows:
        print("No signal rows produced.", file=sys.stderr)
        return 1

    # Determine signal CSV fieldnames
    if args.mode == "can":
        has_pdu = any("pdu_id" in r for r in signal_rows)
        fieldnames = CAN_FIELDNAMES_CONTAINER if has_pdu else CAN_FIELDNAMES
    else:
        fieldnames = SOMEIP_FIELDNAMES

    # Write signal CSV
    if args.output:
        sig_file = open(args.output, "w", newline="", encoding="utf-8")
    else:
        import io as _io

        sig_file = _io.TextIOWrapper(sys.stdout.buffer, newline="")  # type: ignore[assignment]

    writer = csv.DictWriter(sig_file, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(signal_rows)

    if args.output:
        sig_file.close()
        print(f"Wrote {len(signal_rows)} signal(s) to {args.output}", file=sys.stderr)

    # Write enum sidecar CSV
    if enum_rows:
        if args.enum_output:
            with open(args.enum_output, "w", newline="", encoding="utf-8") as ef:
                ew = csv.DictWriter(ef, fieldnames=ENUM_FIELDNAMES, extrasaction="ignore")
                ew.writeheader()
                ew.writerows(enum_rows)
            print(f"Wrote {len(enum_rows)} enum row(s) to {args.enum_output}", file=sys.stderr)
        else:
            print(
                f"warning: {len(enum_rows)} enum row(s) found but --enum-output not set — skipped",
                file=sys.stderr,
            )

    if errors:
        print(f"{len(errors)} row(s) had warnings.", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
