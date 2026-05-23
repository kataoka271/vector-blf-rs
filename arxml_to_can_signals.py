#!/usr/bin/env python3
"""
arxml_to_can_signals.py — Convert AUTOSAR ARXML to can_signals.csv

Extracts CAN signal definitions from one or more AUTOSAR ARXML files and
writes them in the can_signals.csv format expected by vector-blf-rs.

Regular frames:
    message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset

Container frames (CAN-FD Container I-PDU):
    message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id

Usage:
    uv run python arxml_to_can_signals.py input.arxml [-o output.csv]
    uv run python arxml_to_can_signals.py a.arxml b.arxml -o signals.csv
"""

import argparse
import csv
import sys
from typing import Optional

import autosar_data
from autosar_data.abstraction.communication import (
    ContainerIPdu,
    ContainerIPduHeaderType,
    ISignalIPdu,
)


# ---------------------------------------------------------------------------
# Safe navigation helpers (raw element tree)
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
# Signal extraction from a single I-SIGNAL-I-PDU element
# ---------------------------------------------------------------------------


def _signals_from_isignal_ipdu(
    pdu_elem,
    can_id: int,
    pdu_byte_offset: int,
    seen: set,
    pdu_id: Optional[int] = None,
) -> list[dict]:
    """Extract signal rows from one I-SIGNAL-I-PDU element.

    pdu_byte_offset: byte offset of this PDU within the CAN frame (0 for container I-PDUs,
                     since start_bit is relative to the I-PDU payload after demux).
    pdu_id:         when set, the row gets a ninth 'pdu_id' column (container frame signal).
    """
    rows: list[dict] = []

    sig_mappings_container = pdu_elem.get_sub_element(
        "I-SIGNAL-TO-PDU-MAPPINGS"
    ) or pdu_elem.get_sub_element("I-SIGNAL-TO-I-PDU-MAPPINGS")
    if sig_mappings_container is None:
        return rows

    for sig_mapping in sig_mappings_container.sub_elements():
        if "I-SIGNAL-TO" not in sig_mapping.element_name:
            continue

        sig_start_in_pdu = child_int(sig_mapping, "START-POSITION")
        if sig_start_in_pdu is None:
            continue

        packing = child_text(sig_mapping, "PACKING-BYTE-ORDER") or ""
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

        start_bit = pdu_byte_offset * 8 + sig_start_in_pdu

        compu_method = get_compu_method(isignal)
        scale, offset = parse_compu_method(compu_method) if compu_method is not None else (1.0, 0.0)
        is_signed = get_is_signed(isignal)

        key = (can_id, pdu_id, signal_name)
        if key in seen:
            continue
        seen.add(key)

        row: dict = {
            "message_id": f"0x{can_id:X}",
            "signal_name": signal_name,
            "start_bit": start_bit,
            "bit_length": bit_length,
            "byte_order": byte_order,
            "is_signed": str(is_signed).lower(),
            "scale": scale,
            "offset": offset,
        }
        if pdu_id is not None:
            row["pdu_id"] = f"0x{pdu_id:X}"

        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Container I-PDU handling (uses autosar_data.abstraction)
# ---------------------------------------------------------------------------


def _signals_from_container_ipdu(
    pdu_elem,
    can_id: int,
    seen: set,
    verbose: bool = False,
) -> list[dict]:
    """Demux a CONTAINER-I-PDU and extract signals from each contained I-SIGNAL-I-PDU."""
    rows: list[dict] = []
    try:
        container = ContainerIPdu(pdu_elem)
        header_type = container.header_type
        if header_type == ContainerIPduHeaderType.NoHeader:
            if verbose:
                print(f"  skipping NoHeader container at CAN ID 0x{can_id:X}", file=sys.stderr)
            return rows

        for pt in container.contained_ipdu_triggerings():
            inner = pt.pdu
            if inner is None:
                continue

            inner_elem = inner.element
            if inner_elem.element_name != "I-SIGNAL-I-PDU":
                continue

            # Get the PDU ID from ContainedIPduProps on the inner I-SIGNAL-I-PDU
            try:
                inner_isignal_ipdu = ISignalIPdu(inner_elem)
                props = inner_isignal_ipdu.contained_ipdu_props
            except Exception:
                props = None

            pdu_id: Optional[int] = None
            if props is not None:
                if header_type == ContainerIPduHeaderType.ShortHeader:
                    pdu_id = props.header_id_short
                elif header_type == ContainerIPduHeaderType.LongHeader:
                    pdu_id = props.header_id_long

            if pdu_id is None:
                if verbose:
                    name = inner_elem.item_name or "?"
                    print(
                        f"  no header_id for contained PDU '{name}' in container 0x{can_id:X}",
                        file=sys.stderr,
                    )
                continue

            # start_bit is relative to the I-PDU payload (pdu_byte_offset = 0)
            rows.extend(_signals_from_isignal_ipdu(inner_elem, can_id, 0, seen, pdu_id=pdu_id))

    except Exception as exc:
        if verbose:
            print(f"  error processing container at CAN ID 0x{can_id:X}: {exc}", file=sys.stderr)

    return rows


# ---------------------------------------------------------------------------
# Main extraction loop
# ---------------------------------------------------------------------------


def extract_signals(model, verbose: bool = False) -> list[dict]:
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

            # START-POSITION is in bits; PDUs are byte-aligned
            start_pos_bits = child_int(pdu_mapping, "START-POSITION") or 0
            pdu_byte_offset = start_pos_bits // 8

            pdu = follow_ref(pdu_mapping, "PDU-REF")
            if pdu is None:
                continue

            if pdu.element_name == "I-SIGNAL-I-PDU":
                rows.extend(_signals_from_isignal_ipdu(pdu, can_id, pdu_byte_offset, seen))
            elif pdu.element_name == "CONTAINER-I-PDU":
                rows.extend(_signals_from_container_ipdu(pdu, can_id, seen, verbose=verbose))

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

FIELDNAMES_BASE = ["message_id", "signal_name", "start_bit", "bit_length", "byte_order", "is_signed", "scale", "offset"]
FIELDNAMES_CONTAINER = FIELDNAMES_BASE + ["pdu_id"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert AUTOSAR ARXML to can_signals.csv for vector-blf-rs"
    )
    parser.add_argument("inputs", nargs="+", metavar="ARXML", help="Input ARXML file(s)")
    parser.add_argument("-o", "--output", metavar="CSV", help="Output CSV file (default: stdout)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print ARXML load warnings and container debug info")
    args = parser.parse_args()

    model = autosar_data.AutosarModel()
    for path in args.inputs:
        try:
            _, warnings = model.load_file(path)
        except Exception as exc:
            print(f"Error loading {path}: {exc}", file=sys.stderr)
            return 1
        if args.verbose and warnings:
            for w in warnings:
                print(f"  warning: {w}", file=sys.stderr)

    rows = extract_signals(model, verbose=args.verbose)

    if not rows:
        print("No CAN signals found in the provided ARXML file(s).", file=sys.stderr)
        return 1

    # Use the wider fieldnames if any row has pdu_id
    has_container = any("pdu_id" in r for r in rows)
    fieldnames = FIELDNAMES_CONTAINER if has_container else FIELDNAMES_BASE

    if args.output:
        f = open(args.output, "w", newline="", encoding="utf-8")
    else:
        f = sys.stdout

    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
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
