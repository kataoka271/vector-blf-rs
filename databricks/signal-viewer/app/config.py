"""App-wide configuration and key-parsing utilities."""

import os
import re

import pandas as pd

_LOCAL_DEV = not os.getenv("DATABRICKS_WAREHOUSE_ID")

USE_USER_TOKEN = True  # Set to False to use Service Principal credentials instead of user token
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
_GOLD_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"
_CATALOG_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_signal_catalog`"
_CATALOG_BY_FILE_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_signal_catalog_by_file`"
_TIME_RANGE_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_time_range`"
_SOURCE_FILES_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_source_files`"
_VIDEO_FILES_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_video_files`"

_KEY_RE = re.compile(r"^([A-Za-z]+)(\d+)::(.+)$")

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

# Optional: when set, the GPS map uses real Mapbox styles (mapbox.com token);
# when unset, it falls back to the tokenless carto-darkmatter style.
MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN", "")

# Local-dev only: path to a local video file to serve as a stand-in for a Volume-hosted
# video, so the sync UI can be exercised without a real Databricks deployment.
DEV_SAMPLE_VIDEO = os.environ.get("BLF_DEV_SAMPLE_VIDEO", "")

# Unity Catalog Volume directories the /upload screen writes new files into (see
# app/upload.py). BLF_RAW_PATH matches databricks.yml's blf_source_path, so an upload
# lands where blf_ingestion's Auto Loader is already watching; BLF_VIDEO_UPLOAD_PATH
# matches video_path, so an uploaded video is matched to a BLF file by filename stem
# the same way blf_video_files does. Empty disables that uploader (shown as
# "not configured" rather than failing silently).
BLF_RAW_PATH = os.environ.get("BLF_RAW_PATH", "")
BLF_VIDEO_UPLOAD_PATH = os.environ.get("BLF_VIDEO_UPLOAD_PATH", "")

# Databricks SDK config (None in local dev)
cfg = None
if not _LOCAL_DEV:
    from databricks.sdk.core import Config

    cfg = Config()
else:
    print("[LOCAL DEV] No DATABRICKS_WAREHOUSE_ID -- serving dummy data.", flush=True)


def _parse_key(key: str) -> tuple[str, int, str]:
    """Parse '{signal_source}{channel}::{signal_name}' into (source, channel, name)."""
    m = _KEY_RE.match(key)
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    if "::" not in key:
        print(f"[_parse_key] key {key!r} has no '::' separator", flush=True)
        return key, 0, ""
    prefix, name = key.split("::", 1)
    print(f"[_parse_key] key {key!r} has no numeric channel; defaulting to 0", flush=True)
    return prefix, 0, name


def _to_utc_naive(ts: pd.Timestamp) -> pd.Timestamp:
    """Normalize a Timestamp to the tz-naive UTC instant it represents.

    A `t0` sourced from a SQL timestamp column may carry tzinfo (e.g. a
    non-UTC session timezone), while chart x-values and other derived
    timestamps are tz-naive UTC (figures._plot_x converts tz-aware trace
    data to UTC before dropping tz for Plotly). Comparing/subtracting a
    non-UTC-offset t0 against those without this conversion silently
    shifts results by the offset instead of just failing loudly.
    """
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC").tz_localize(None)
    return ts
