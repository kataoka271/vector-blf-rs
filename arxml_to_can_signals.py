#!/usr/bin/env python3
"""
arxml_to_can_signals.py — Convert AUTOSAR ARXML to can_signals.csv

Extracts CAN signal definitions from one or more AUTOSAR ARXML files and
writes them in the can_signals.csv format expected by vector-blf-rs.

Usage:
    uv run python arxml_to_can_signals.py input.arxml [-o output.csv]
    uv run python arxml_to_can_signals.py a.arxml b.arxml -o signals.csv

Output columns:
    message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
"""

import argparse
import csv
import sys
from typing import Optional

import autosar_data


# ---------------------------------------------------------------------------
# Safe navigation helpers
# ---------------------------------------------------------------------------


def child_text(elem, tag: str) -> Optional[str]:
    c = elem.get_sub_element(tag)
    if c is None:
        return None
    d = c.character_data
    return str(d) if d is not None else None


def child_int(elem, tag: str) -> Optional[int]:
    c = elem.get_sub_element(tag)
    if c is None:
        return None
    d = c.character_data
    try:
        return int(d)
    except (TypeError, ValueError):
        return None


def follow_ref(elem, tag: str) -> Optional[object]:
    ref = elem.get_sub_element(tag)
    if ref is None:
        return None
    try:
        return ref.reference_target()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# COMPU-METHOD → (scale, offset)
# ---------------------------------------------------------------------------


def _collect_v_values(elem) -> list[float]:
    values: list[float] = []
    for child in elem.sub_elements():
        if child.element_name == "V":
            try:
                values.append(float(str(child.character_data)))
            except (TypeError, ValueError):
                pass
    return values


def parse_compu_method(compu_method) -> tuple[float, float]:
    """Return (scale, offset) from a LINEAR or RATIONAL COMPU-METHOD."""
    try:
        internal = compu_method.get_sub_element("COMPU-INTERNAL-TO-PHYS")
        if internal is None:
            return 1.0, 0.0
        scales = internal.get_sub_element("COMPU-SCALES")
        if scales is None:
            return 1.0, 0.0
        scale_elem = scales.get_sub_element("COMPU-SCALE")
        if scale_elem is None:
            return 1.0, 0.0

        rational = scale_elem.get_sub_element("COMPU-RATIONAL-COEFFS")
        if rational is None:
            # Some schemas put COMPU-NUMERATOR directly under COMPU-SCALE
            num_elem = scale_elem.get_sub_element("COMPU-NUMERATOR")
            if num_elem is not None:
                vs = _collect_v_values(num_elem)
                if len(vs) >= 2:
                    return vs[1], vs[0]
                if len(vs) == 1:
                    return 1.0, vs[0]
            return 1.0, 0.0

        num_elem = rational.get_sub_element("COMPU-NUMERATOR")
        den_elem = rational.get_sub_element("COMPU-DENOMINATOR")

        offset, scale = 0.0, 1.0
        if num_elem is not None:
            vs = _collect_v_values(num_elem)
            if len(vs) >= 2:
                offset, scale = vs[0], vs[1]
            elif len(vs) == 1:
                offset = vs[0]

        divisor = 1.0
        if den_elem is not None:
            vs = _collect_v_values(den_elem)
            if vs and vs[0] != 0.0:
                divisor = vs[0]

        return scale / divisor, offset / divisor

    except Exception:
        return 1.0, 0.0


# ---------------------------------------------------------------------------
# I-SIGNAL props: signedness and COMPU-METHOD
# ---------------------------------------------------------------------------

# AUTOSAR nests SW-DATA-DEF-PROPS under several possible wrapper elements.
_PROPS_PATH = [
    ["SW-DATA-DEF-PROPS", "SW-DATA-DEF-PROPS-VARIANTS", "SW-DATA-DEF-PROPS-CONDITIONAL"],
    ["SW-DATA-DEF-PROPS-VARIANTS", "SW-DATA-DEF-PROPS-CONDITIONAL"],
    ["SW-DATA-DEF-PROPS-CONDITIONAL"],
]


def _find_props_conditional(container):
    for path in _PROPS_PATH:
        elem = container
        for tag in path:
            if elem is None:
                break
            elem = elem.get_sub_element(tag)
        if elem is not None:
            return elem
    return None


def _get_sw_props(isignal):
    """Return the SW-DATA-DEF-PROPS-CONDITIONAL element for an I-SIGNAL."""
    for wrapper_tag in ("I-SIGNAL-PROPS", "NETWORK-REPRESENTATION-PROPS"):
        wrapper = isignal.get_sub_element(wrapper_tag)
        if wrapper is not None:
            cond = _find_props_conditional(wrapper)
            if cond is not None:
                return cond
    return None


def get_is_signed(isignal) -> bool:
    cond = _get_sw_props(isignal)
    if cond is None:
        return False
    base_type = follow_ref(cond, "BASE-TYPE-REF")
    if base_type is None:
        return False
    encoding = child_text(base_type, "BASE-TYPE-ENCODING")
    return encoding is not None and encoding.upper() == "2S-COMPLEMENT"


def get_compu_method(isignal):
    cond = _get_sw_props(isignal)
    if cond is None:
        return None
    return follow_ref(cond, "COMPU-METHOD-REF")


# ---------------------------------------------------------------------------
# Main extraction logic
# ---------------------------------------------------------------------------


def extract_signals(model) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()

    for elem in model.elements_dfs():
        if elem.element_name != "CAN-FRAME-TRIGGERING":
            continue

        can_id = child_int(elem, "IDENTIFIER")
        if can_id is None:
            continue

        frame = follow_ref(elem, "FRAME-REF")
        if frame is None:
            continue

        pdu_frame_mappings = frame.get_sub_element("PDU-TO-FRAME-MAPPINGS")
        if pdu_frame_mappings is None:
            continue

        for pdu_mapping in pdu_frame_mappings.sub_elements():
            if pdu_mapping.element_name != "PDU-TO-FRAME-MAPPING":
                continue

            pdu_byte_offset = child_int(pdu_mapping, "START-POSITION") or 0

            pdu = follow_ref(pdu_mapping, "PDU-REF")
            if pdu is None or pdu.element_name != "I-SIGNAL-I-PDU":
                continue

            # Try both AUTOSAR 3.x and 4.x element names for the signal mapping list
            sig_mappings_container = pdu.get_sub_element(
                "I-SIGNAL-TO-PDU-MAPPINGS"
            ) or pdu.get_sub_element("I-SIGNAL-TO-I-PDU-MAPPINGS")
            if sig_mappings_container is None:
                continue

            for sig_mapping in sig_mappings_container.sub_elements():
                if "I-SIGNAL-TO" not in sig_mapping.element_name:
                    continue

                sig_start_in_pdu = child_int(sig_mapping, "START-POSITION")
                if sig_start_in_pdu is None:
                    continue

                packing = child_text(sig_mapping, "PACKING-BYTE-ORDER") or ""
                # MOST-SIGNIFICANT-BYTE-FIRST → Motorola (big-endian)
                # MOST-SIGNIFICANT-BYTE-LAST  → Intel   (little-endian)
                byte_order = "Motorola" if "FIRST" in packing.upper() else "Intel"

                isignal = follow_ref(sig_mapping, "I-SIGNAL-REF")
                if isignal is None:
                    continue

                signal_name = isignal.item_name
                if not signal_name:
                    continue

                bit_length = child_int(isignal, "LENGTH")
                if bit_length is None:
                    continue

                # Bit position within the CAN frame
                start_bit = pdu_byte_offset * 8 + sig_start_in_pdu

                # Physical value encoding
                compu_method = get_compu_method(isignal)
                scale, offset = parse_compu_method(compu_method) if compu_method is not None else (1.0, 0.0)

                is_signed = get_is_signed(isignal)

                key = (can_id, signal_name)
                if key in seen:
                    continue
                seen.add(key)

                rows.append(
                    {
                        "message_id": f"0x{can_id:X}",
                        "signal_name": signal_name,
                        "start_bit": start_bit,
                        "bit_length": bit_length,
                        "byte_order": byte_order,
                        "is_signed": str(is_signed).lower(),
                        "scale": scale,
                        "offset": offset,
                    }
                )

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

FIELDNAMES = ["message_id", "signal_name", "start_bit", "bit_length", "byte_order", "is_signed", "scale", "offset"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert AUTOSAR ARXML to can_signals.csv for vector-blf-rs"
    )
    parser.add_argument("inputs", nargs="+", metavar="ARXML", help="Input ARXML file(s)")
    parser.add_argument("-o", "--output", metavar="CSV", help="Output CSV file (default: stdout)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print ARXML load warnings")
    args = parser.parse_args()

    model = autosar_data.AutosarModel()
    for path in args.inputs:
        try:
            _, warnings = model.load_file(path)
        except Exception as exc:
            print(f"Error loading {path}: {exc}", file=sys.stderr)
            return 1
        if args.verbose:
            for w in warnings:
                print(f"  warning: {w}", file=sys.stderr)

    rows = extract_signals(model)

    if not rows:
        print("No CAN signals found in the provided ARXML file(s).", file=sys.stderr)
        return 1

    if args.output:
        f = open(args.output, "w", newline="", encoding="utf-8")
    else:
        f = sys.stdout

    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)

    if args.output:
        f.close()
        print(f"Wrote {len(rows)} signal(s) to {args.output}", file=sys.stderr)
    else:
        print(f"# {len(rows)} signal(s) extracted", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
