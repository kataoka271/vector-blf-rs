"""Offline tests for the synthetic waveform fetch_fn (bench/waveform.py): the three
shapes have the right levels, packing round-trips through the signal CSV convention, the
row shape matches what bench.db.to_frames consumes, and the sample grid is loop-seamless.
"""

from __future__ import annotations

import math

import pytest
from bench.db import to_frames
from bench.waveform import SINE, SQUARE, TRIANGLE, WaveformSpec, waveform_fetch_fn, waveform_frames

RUN_ID = "run_001"


def test_sine_hits_its_quarter_cycle_levels():
    spec = WaveformSpec(waveform=SINE, freq_hz=1.0, amplitude=2.0, dc_offset=1.0)
    assert spec.value_at(0.0) == pytest.approx(1.0)
    assert spec.value_at(0.25) == pytest.approx(3.0)
    assert spec.value_at(0.75) == pytest.approx(-1.0)


def test_square_switches_at_the_duty_point():
    spec = WaveformSpec(waveform=SQUARE, freq_hz=1.0, duty=0.25)
    assert spec.value_at(0.0) == pytest.approx(1.0)
    assert spec.value_at(0.2) == pytest.approx(1.0)
    assert spec.value_at(0.3) == pytest.approx(-1.0)


def test_triangle_is_piecewise_linear_between_its_peaks():
    spec = WaveformSpec(waveform=TRIANGLE, freq_hz=1.0)
    assert spec.value_at(0.0) == pytest.approx(0.0)
    assert spec.value_at(0.125) == pytest.approx(0.5)
    assert spec.value_at(0.25) == pytest.approx(1.0)
    assert spec.value_at(0.75) == pytest.approx(-1.0)


def test_phase_shifts_by_whole_cycles():
    a = WaveformSpec(waveform=SINE, freq_hz=2.0)
    b = WaveformSpec(waveform=SINE, freq_hz=2.0, phase=0.25)
    assert b.value_at(0.0) == pytest.approx(a.value_at(0.125))


def test_raw_encoding_follows_scale_and_offset():
    spec = WaveformSpec(waveform=SQUARE, scale=0.5, offset=-40.0, amplitude=10.0)
    assert spec.raw_at(0.0) == round((10.0 + 40.0) / 0.5)


def test_raw_is_clamped_to_the_field_width():
    spec = WaveformSpec(waveform=SQUARE, bit_length=8, amplitude=1000.0)
    assert spec.raw_at(0.0) == 255
    assert WaveformSpec(waveform=SQUARE, bit_length=8, is_signed=True, amplitude=1000.0).raw_at(0.0) == 127


def test_intel_and_motorola_pack_the_same_value_in_opposite_orders():
    intel = WaveformSpec(waveform=SQUARE, bit_length=16, amplitude=0x1234)
    motorola = WaveformSpec(waveform=SQUARE, bit_length=16, amplitude=0x1234, byte_order="Motorola")
    assert waveform_frames(intel, duration_s=0.02, sample_hz=50.0)["data"][0] == b"\x34\x12"
    assert waveform_frames(motorola, duration_s=0.02, sample_hz=50.0)["data"][0] == b"\x12\x34"


def test_specs_are_packed_side_by_side_into_one_frame():
    df = waveform_frames(
        [
            WaveformSpec(waveform=SQUARE, start_byte=0, bit_length=8, amplitude=1.0),
            WaveformSpec(waveform=SQUARE, start_byte=2, bit_length=8, amplitude=2.0),
        ],
        duration_s=0.02,
        sample_hz=50.0,
    )
    assert df["dlc"][0] == 3
    assert df["data"][0] == b"\x01\x00\x02"


def test_overlapping_specs_are_rejected():
    with pytest.raises(ValueError, match="overlap"):
        waveform_frames([WaveformSpec(bit_length=16), WaveformSpec(start_byte=1, bit_length=16)])


def test_dlc_smaller_than_the_specs_is_rejected():
    with pytest.raises(ValueError, match="too small"):
        waveform_frames(WaveformSpec(bit_length=16), dlc=1)


def test_sample_grid_stops_one_period_short_so_looping_is_seamless():
    df = waveform_frames(WaveformSpec(), sample_hz=10.0, duration_s=1.0)
    assert len(df) == 10
    assert df["timestamp_s"].iloc[-1] == pytest.approx(0.9)


def test_rows_convert_through_to_frames():
    df = waveform_fetch_fn(WaveformSpec(), can_id=0x321, sample_hz=10.0, duration_s=0.5)()
    frames = to_frames(df, run_id=RUN_ID, source_file="testbench/run_001.blf", channel=2)
    assert len(frames) == 5
    assert {f.can_id for f in frames} == {0x321}
    assert {f.channel for f in frames} == {2}
    assert [f.timestamp_ns for f in frames] == [0, 100_000_000, 200_000_000, 300_000_000, 400_000_000]
    values = [int.from_bytes(f.data, "little") for f in frames]
    assert values[1] == round(math.sin(2.0 * math.pi * 0.1))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"waveform": "sawtooth"},
        {"byte_order": "Middle"},
        {"bit_length": 12},
        {"start_byte": -1},
        {"scale": 0.0},
    ],
)
def test_invalid_specs_are_rejected(kwargs):
    with pytest.raises(ValueError):
        WaveformSpec(**kwargs)
