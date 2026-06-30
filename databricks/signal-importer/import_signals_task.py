"""
import_signals_task.py -- Databricks spark_python_task

Reads a (possibly password-protected) Excel file from a Unity Catalog Volume,
converts CAN or SOME/IP signal definitions and enum values, then writes them
to Unity Catalog Delta tables.

Depends on signal_conversion.py (uploaded alongside this file as a library
in the DAB job YAML).

Parameters (passed as sys.argv by the DAB job YAML):
  --excel-path     UC Volume path to the Excel file
  --sheet          Sheet name or 0-based index (default: 0)
  --header-row     0-based header row index (default: 0)
  --mode           "can" or "someip" (default: can)
  --mapping-json   JSON of COLUMN_MAP dict (field -> header or 0-based index)
  --secret-scope   Databricks secret scope holding the password (default: blf)
  --secret-key     Secret key for the Excel password (default: excel_password)
  --target-catalog Unity Catalog catalog name (default: main)
  --target-schema  Unity Catalog schema name (default: blf_dev)
  --can-table      Override output table for CAN signals
  --someip-table   Override output table for SOME/IP signals
  --enum-table     Override output table for enum values (empty = skip)

Tables written:
  <catalog>.<schema>.can_signals      (mode=can)
  <catalog>.<schema>.someip_signals   (mode=someip)
  <catalog>.<schema>.enum_signals     (when enum rows exist and --enum-table is set
                                       or when both coding + meaning are mapped)

Column mapping example (--mapping-json):
  '{"message_id":"CAN ID","signal_name":"Signal","start_byte":5,...}'
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Union

# signal_conversion.py lives in scripts/ two levels above this file in the workspace.
# DABs syncs the entire bundle, so the relative path is stable across targets.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))

import pandas as pd
from signal_conversion import (
    convert_dataframe,
    decrypt_excel_or_passthrough,
    resolve_column_map,
    uc_volume_to_local,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Excel signal definitions to Delta tables")
    parser.add_argument("--excel-path", required=True, help="UC Volume path to Excel file")
    parser.add_argument("--sheet", default="0")
    parser.add_argument("--header-row", type=int, default=0)
    parser.add_argument("--mode", choices=["can", "someip"], default="can")
    parser.add_argument("--mapping-json", default="{}", help="JSON COLUMN_MAP dict")
    parser.add_argument("--secret-scope", default="blf")
    parser.add_argument("--secret-key", default="excel_password")
    parser.add_argument("--target-catalog", default="main")
    parser.add_argument("--target-schema", default="blf_dev")
    parser.add_argument("--can-table", default="")
    parser.add_argument("--someip-table", default="")
    parser.add_argument("--enum-table", default="")
    return parser.parse_args()


def _resolve_table(explicit: str, catalog: str, schema: str, suffix: str) -> str:
    return explicit if explicit else f"{catalog}.{schema}.{suffix}"


def _write_table(rows: list[dict], table: str) -> None:
    """Write a list of dicts to a Delta table via spark.createDataFrame."""
    df = pd.DataFrame(rows)
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)  # noqa: F821


def main() -> int:
    args = _parse_args()

    # Resolve sheet identifier
    sheet: Union[str, int] = args.sheet
    try:
        sheet = int(sheet)
    except (ValueError, TypeError):
        pass

    # Retrieve password from Databricks secret scope (empty scope/key = no encryption)
    password = ""
    if args.secret_scope and args.secret_key:
        try:
            password = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)  # noqa: F821
        except Exception as exc:
            print(f"[import_signals] warning: could not retrieve secret — {exc}", flush=True)

    # Load and optionally decrypt Excel
    local = uc_volume_to_local(args.excel_path)
    try:
        source = decrypt_excel_or_passthrough(local, password)
        df = pd.read_excel(source, sheet_name=sheet, header=args.header_row, dtype=object)
    except ImportError as exc:
        print(f"[import_signals] error: {exc}", flush=True)
        return 1
    except Exception as exc:
        print(f"[import_signals] error reading Excel: {exc}", flush=True)
        return 1

    print(f"[import_signals] loaded {len(df)} row(s) from {args.excel_path!r}", flush=True)

    # Build column map
    try:
        raw_mapping: dict[str, Union[str, int]] = json.loads(args.mapping_json)
    except json.JSONDecodeError as exc:
        print(f"[import_signals] error: invalid --mapping-json: {exc}", flush=True)
        return 1

    columns: list[str] = list(df.columns.astype(str))
    try:
        col = resolve_column_map(raw_mapping, columns)
    except SystemExit as exc:
        print(f"[import_signals] error: {exc}", flush=True)
        return 1

    # Convert rows
    errors: list[str] = []
    signal_rows, enum_rows = convert_dataframe(df, col, args.mode, errors, args.header_row)

    for e in errors:
        print(f"[import_signals] warning: {e}", flush=True)

    if not signal_rows:
        print("[import_signals] no signal rows produced — aborting", flush=True)
        return 1

    # Determine target tables
    catalog, schema = args.target_catalog, args.target_schema
    can_table = _resolve_table(args.can_table, catalog, schema, "can_signals")
    someip_table = _resolve_table(args.someip_table, catalog, schema, "someip_signals")
    enum_table = args.enum_table or (f"{catalog}.{schema}.enum_signals" if enum_rows else "")

    # Write signal table
    sig_table = can_table if args.mode == "can" else someip_table
    _write_table(signal_rows, sig_table)
    print(f"[import_signals] wrote {len(signal_rows)} signal row(s) to {sig_table!r}", flush=True)

    # Write enum table
    if enum_rows and enum_table:
        _write_table(enum_rows, enum_table)
        print(f"[import_signals] wrote {len(enum_rows)} enum row(s) to {enum_table!r}", flush=True)
    elif enum_rows:
        print(f"[import_signals] {len(enum_rows)} enum row(s) found but --enum-table not set — skipped", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
