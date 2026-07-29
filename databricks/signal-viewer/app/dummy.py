"""Local-dev dummy data and query stub."""

import math
import os

import pandas as pd

from .config import DEV_SAMPLE_VIDEO

_DUMMY_N = 500
_DUMMY_DURATION = 300.0
_DUMMY_T = [i * _DUMMY_DURATION / (_DUMMY_N - 1) for i in range(_DUMMY_N)]
_DUMMY_TS_NS = [int(t * 1e9) for t in _DUMMY_T]
_DUMMY_T0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")

_DUMMY_FILES = [
    "dbfs:/Volumes/main/blf_dev/raw/test_logfile.blf",
    "dbfs:/Volumes/main/blf_dev/raw/bench_large.blf",
]

_DUMMY_CATALOG = [
    ("CAN", 1, "EngineSpeed_rpm"),
    ("CAN", 1, "VehicleSpeed_kph"),
    ("CAN", 1, "BatteryVoltage_V"),
    ("CAN", 1, "SteeringAngle_deg"),
    ("SOMEIP", 0, "TemperatureSensor_C"),
    ("SOMEIP", 0, "AmbientLight_lux"),
    ("CAN", 1, "GPS_Latitude"),
    ("CAN", 1, "GPS_Longitude"),
]

# Set BLF_DUMMY_SIGNAL_COUNT to append N synthetic signals spread across several
# sources/channels, to exercise UI caps (500-row browse cache, 100-row display
# truncation, unbounded channel-filter list, 20-tag overflow) without a real
# Databricks deployment. E.g.: BLF_DUMMY_SIGNAL_COUNT=800 uv run python app.py
_DUMMY_SYNTHETIC_SOURCES_CHANNELS = [("CAN", 0), ("CAN", 1), ("CAN", 2), ("SOMEIP", 0), ("SOMEIP", 1)]
_DUMMY_SIGNAL_COUNT = int(os.environ.get("BLF_DUMMY_SIGNAL_COUNT", "0"))
_DUMMY_CATALOG = _DUMMY_CATALOG + [
    (*_DUMMY_SYNTHETIC_SOURCES_CHANNELS[i % len(_DUMMY_SYNTHETIC_SOURCES_CHANNELS)], f"SynthSignal_{i:04d}")
    for i in range(_DUMMY_SIGNAL_COUNT)
]


def _dummy_values(src: str, channel: int, name: str) -> list[float]:
    if name == "GPS_Latitude":
        return [35.6895 + 0.01 * math.sin(2 * math.pi * t / _DUMMY_DURATION) for t in _DUMMY_T]
    if name == "GPS_Longitude":
        return [139.6917 + 0.01 * math.cos(2 * math.pi * t / _DUMMY_DURATION) for t in _DUMMY_T]
    seed = hash(f"{src}{channel}::{name}") & 0xFFFF
    freq = 0.05 + (seed % 20) * 0.01
    amp = 10 + (seed % 90)
    offset = (seed % 100) - 50
    return [offset + amp * math.sin(2 * math.pi * freq * t + seed * 0.001) for t in _DUMMY_T]


def _dummy_query(stmt: str, params=None) -> pd.DataFrame:
    print(f"[_dummy_query] stmt={stmt!r} params={params!r}", flush=True)
    if "_video_path" in stmt:
        # No video Volume in local dev. If BLF_DEV_SAMPLE_VIDEO points at a local file,
        # report it as a match for any file so the sync UI can be exercised; otherwise
        # report "no matching video".
        if DEV_SAMPLE_VIDEO:
            return pd.DataFrame({"_video_path": [DEV_SAMPLE_VIDEO], "_video_mtime": [pd.Timestamp.now(tz="UTC")]})
        return pd.DataFrame({"_video_path": [], "_video_mtime": []})
    if "_source_file" in stmt and "signal_name" not in stmt:
        return pd.DataFrame({"_source_file": _DUMMY_FILES})
    if "GROUP BY _source_file" in stmt:
        # Per-file time ranges anchoring each video segment. Dummy samples carry no file
        # dimension, so every selected file reports the same span -- enough for the
        # playlist to be built with the right number of segments in the right order.
        files = list(params)[stmt.count("(?, ?, ?)") * 3 :] if params else list(_DUMMY_FILES)
        return pd.DataFrame(
            {
                "source_file": files,
                "t_min": [0.0] * len(files),
                "t_max": [_DUMMY_DURATION] * len(files),
                "t0": [_DUMMY_T0] * len(files),
            }
        )
    if "blf_signal_catalog" in stmt or "DISTINCT" in stmt:
        rows = [{"signal_name": n, "signal_source": s, "channel": c} for s, c, n in _DUMMY_CATALOG]
        return pd.DataFrame(rows).sort_values(["signal_source", "channel", "signal_name"]).reset_index(drop=True)
    if "t_min" in stmt:
        return pd.DataFrame({"t_min": [0.0], "t_max": [_DUMMY_DURATION], "t0": [_DUMMY_T0]})
    if "PARTITION BY signal_source, channel, signal_name" in stmt:
        # params = [src, ch, name] * N + optional [file, ...] + optional [t_lo, t_hi].
        # Only the leading triples pick which signals to synthesize, and N comes from the
        # SQL rather than len(params) // 3: the trailing file params are not a multiple
        # of 3, so slicing by length alone reads a filename where a channel is expected.
        # The file filter itself is ignored -- dummy signals exist in every file.
        n_triples = stmt.count("signal_source = ? AND channel = ? AND signal_name = ?")
        if params and n_triples:
            p = list(params)[: n_triples * 3]
            requested = {(str(p[i * 3]), int(p[i * 3 + 1]), str(p[i * 3 + 2])) for i in range(n_triples)}
            catalog = [(s, c, n) for s, c, n in _DUMMY_CATALOG if (s, c, n) in requested]
        else:
            catalog = _DUMMY_CATALOG
        rows = []
        for src, ch, name in catalog:
            vals = _dummy_values(src, ch, name)
            for t, ts_ns, v in zip(_DUMMY_T, _DUMMY_TS_NS, vals):
                rows.append(
                    {
                        "signal_source": src,
                        "channel": ch,
                        "signal_name": name,
                        "event_time": _DUMMY_T0 + pd.Timedelta(seconds=t),
                        "timestamp_s": t,
                        "timestamp_ns": ts_ns,
                        "signal_value": v,
                        "signal_str": None,
                    }
                )
        return pd.DataFrame(rows)
    # Single-signal fallback (ORDER BY timestamp_ns, no PARTITION BY)
    _fb = params or ["CAN", 1, "GPS_Latitude"]
    vals = _dummy_values(str(_fb[0]), int(_fb[1]), str(_fb[2]))
    return pd.DataFrame({"timestamp_ns": _DUMMY_TS_NS, "signal_value": vals, "signal_str": [None] * len(_DUMMY_TS_NS)})
