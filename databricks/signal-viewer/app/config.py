"""App-wide configuration and key-parsing utilities."""

import os
import re

_LOCAL_DEV = not os.getenv("DATABRICKS_WAREHOUSE_ID")

USE_USER_TOKEN = True  # Set to False to use Service Principal credentials instead of user token
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
_GOLD_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"

_KEY_RE = re.compile(r"^([A-Za-z]+)(\d+)::(.+)$")

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

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
