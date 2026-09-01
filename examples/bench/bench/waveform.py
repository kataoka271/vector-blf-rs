"""Synthetic waveform generation: a `fetch_fn` for GeneratorEcu/ReplayEcu that
synthesizes sine/square/triangle CAN traffic instead of fetching recorded frames from
Databricks.

Each `WaveformSpec` is one signal packed into the frame payload the way
`assets/can_signals.csv` describes it (start_byte/bit_length/byte_order/is_signed/scale/
offset), so traffic generated here decodes back into physical values through the normal
signal pipeline. Several specs sharing one frame are packed side by side into disjoint
byte ranges.

The generated rows carry the same columns as `bench.db.fetch_replay_frames`, so
`bench.db.to_frames` consumes them unchanged:

    bench.add_ecu(GeneratorEcu(ch=bus, fetch_fn=waveform_fetch_fn(WaveformSpec()), loop=True))

The last sample stops one sample period short of `duration_s`, so replaying with
`loop=True` continues the waveform seamlessly across passes.
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Callable, Sequence
from typing import Any

import pandas as pd

SINE = "sine"
SQUARE = "square"
TRIANGLE = "triangle"
WAVEFORMS = (SINE, SQUARE, TRIANGLE)

INTEL = "Intel"
MOTOROLA = "Motorola"


def _shape(waveform: str, u: float, duty: float) -> float:
    """One cycle of `waveform` at phase `u` in [0, 1), normalized to [-1, 1] and starting
    at 0 for sine and triangle alike.
    """
    if waveform == SINE:
        return math.sin(2.0 * math.pi * u)
    if waveform == SQUARE:
        return 1.0 if u < duty else -1.0
    if waveform == TRIANGLE:
        if u < 0.25:
            return 4.0 * u
        if u < 0.75:
            return 2.0 - 4.0 * u
        return 4.0 * u - 4.0
    raise ValueError(f"unknown waveform {waveform!r}; expected one of {WAVEFORMS}")


@dataclasses.dataclass(frozen=True)
class WaveformSpec:
    """One periodic signal, and where it sits in the frame payload.

    `amplitude`/`dc_offset` are in physical units (what a decoder yields); `scale`/
    `offset` are the CAN signal's raw-to-physical conversion, matching the signal CSV
    columns of the same name. `phase` is in cycles, not radians; `duty` applies to
    `SQUARE` only.

    Only byte-aligned fields are supported, so `bit_length` must be a multiple of 8.
    """

    waveform: str = SINE
    start_byte: int = 0
    bit_length: int = 16
    byte_order: str = INTEL
    is_signed: bool = False
    scale: float = 1.0
    offset: float = 0.0
    freq_hz: float = 1.0
    amplitude: float = 1.0
    dc_offset: float = 0.0
    phase: float = 0.0
    duty: float = 0.5

    def __post_init__(self) -> None:
        if self.waveform not in WAVEFORMS:
            raise ValueError(f"unknown waveform {self.waveform!r}; expected one of {WAVEFORMS}")
        if self.byte_order not in (INTEL, MOTOROLA):
            raise ValueError(f"unknown byte_order {self.byte_order!r}; expected {INTEL!r} or {MOTOROLA!r}")
        if self.bit_length <= 0 or self.bit_length % 8:
            raise ValueError(f"bit_length must be a positive multiple of 8, got {self.bit_length}")
        if self.start_byte < 0:
            raise ValueError(f"start_byte must be >= 0, got {self.start_byte}")
        if self.scale == 0.0:
            raise ValueError("scale must be non-zero")

    @property
    def byte_width(self) -> int:
        return self.bit_length // 8

    @property
    def end_byte(self) -> int:
        """Exclusive end of this signal's byte range."""
        return self.start_byte + self.byte_width

    def value_at(self, t: float) -> float:
        """Physical value of the signal at `t` seconds."""
        u = (t * self.freq_hz + self.phase) % 1.0
        return self.dc_offset + self.amplitude * _shape(self.waveform, u, self.duty)

    def raw_at(self, t: float) -> int:
        """Raw (encoded) value at `t`, clamped to what `bit_length` can represent."""
        raw = round((self.value_at(t) - self.offset) / self.scale)
        if self.is_signed:
            lo, hi = -(1 << (self.bit_length - 1)), (1 << (self.bit_length - 1)) - 1
        else:
            lo, hi = 0, (1 << self.bit_length) - 1
        return max(lo, min(hi, raw))

    def pack_into(self, buf: bytearray, t: float) -> None:
        """Write this signal's value at `t` into its byte range of `buf`."""
        endian = "little" if self.byte_order == INTEL else "big"
        buf[self.start_byte : self.end_byte] = self.raw_at(t).to_bytes(self.byte_width, endian, signed=self.is_signed)


def waveform_frames(
    specs: WaveformSpec | Sequence[WaveformSpec],
    *,
    can_id: int = 0x300,
    channel: int = 1,
    sample_hz: float = 50.0,
    duration_s: float = 1.0,
    dlc: int | None = None,
    is_ext_id: bool = False,
    message_type: str = "CAN",
) -> pd.DataFrame:
    """Synthesize `duration_s` of `sample_hz` frames carrying `specs`, in the column
    shape `bench.db.to_frames` expects.

    `dlc` defaults to the smallest payload holding every spec. Raises ValueError if two
    specs overlap, if a spec does not fit in `dlc`, or if `sample_hz`/`duration_s` are
    not positive.
    """
    if isinstance(specs, WaveformSpec):
        specs = [specs]
    specs = list(specs)
    if not specs:
        raise ValueError("waveform_frames() needs at least one WaveformSpec")
    if sample_hz <= 0:
        raise ValueError(f"sample_hz must be > 0, got {sample_hz}")
    if duration_s <= 0:
        raise ValueError(f"duration_s must be > 0, got {duration_s}")

    claimed: dict[int, WaveformSpec] = {}
    for spec in specs:
        for byte in range(spec.start_byte, spec.end_byte):
            if byte in claimed:
                raise ValueError(f"specs overlap at byte {byte}: {claimed[byte]} and {spec}")
            claimed[byte] = spec

    width = max(spec.end_byte for spec in specs)
    if dlc is None:
        dlc = width
    elif dlc < width:
        raise ValueError(f"dlc={dlc} is too small for specs spanning {width} byte(s)")

    period_s = 1.0 / sample_hz
    rows: list[dict[str, Any]] = []
    for i in range(max(1, round(duration_s * sample_hz))):
        t = i * period_s
        buf = bytearray(dlc)
        for spec in specs:
            spec.pack_into(buf, t)
        rows.append(
            {
                "timestamp_ns": round(t * 1e9),
                "timestamp_s": t,
                "channel": channel,
                "can_id": can_id,
                "is_ext_id": is_ext_id,
                "rtr": False,
                "dlc": dlc,
                "data": bytes(buf),
                "dir": 1,
                "message_type": message_type,
            }
        )
    return pd.DataFrame(rows)


def waveform_fetch_fn(
    specs: WaveformSpec | Sequence[WaveformSpec],
    **kwargs: Any,
) -> Callable[[], pd.DataFrame]:
    """Return a zero-argument fetch_fn calling `waveform_frames(specs, **kwargs)`.

    Synthesizes on each call rather than caching, so a re-fetching caller stays correct;
    GeneratorEcu/ReplayEcu fetch once per run either way.
    """
    return lambda: waveform_frames(specs, **kwargs)
