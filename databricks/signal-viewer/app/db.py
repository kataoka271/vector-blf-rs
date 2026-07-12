"""Database query helpers and DataFrame serialization."""

import base64
import datetime
import json
import traceback

import flask
import pandas as pd
import pyarrow as pa

from databricks import sql

from .config import (
    _CATALOG_TABLE,
    _GOLD_TABLE,
    _LOCAL_DEV,
    _SOURCE_FILES_TABLE,
    _TIME_RANGE_TABLE,
    _VIDEO_FILES_TABLE,
    USE_USER_TOKEN,
    cfg,
)
from .dummy import _dummy_query


def _log_token_info(token: str) -> None:
    """Log non-sensitive metadata from the X-Forwarded-Access-Token."""
    parts = token.split(".")
    if len(parts) == 3:
        # JWT: decode the payload (no signature verification needed for logging)
        try:
            padding = 4 - len(parts[1]) % 4
            payload_bytes = base64.urlsafe_b64decode(parts[1] + "=" * padding)
            claims = json.loads(payload_bytes)
            sub = claims.get("sub", "")
            iss = claims.get("iss", "")
            scope = claims.get("scope", "")
            exp_raw = claims.get("exp")
            exp_str = (
                datetime.datetime.fromtimestamp(exp_raw, tz=datetime.timezone.utc).isoformat()
                if isinstance(exp_raw, (int, float))
                else str(exp_raw)
            )
            print(
                f"[token] type=JWT sub={sub!r} iss={iss!r} scope={scope!r} exp={exp_str}",
                flush=True,
            )
        except Exception as exc:
            print(f"[token] type=JWT (payload decode failed: {exc})", flush=True)
    else:
        # Non-JWT token: log only a short prefix
        prefix = token[:8] + "..." if len(token) > 8 else token
        print(f"[token] type=opaque prefix={prefix!r} len={len(token)}", flush=True)


def _run_query(stmt: str, params: list | dict | None, user_token: str | None = None) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    assert cfg is not None, "Databricks config is not initialized."
    print(f"[_run_query] stmt={stmt!r} params={params!r}", flush=True)
    connect_kwargs = {"access_token": user_token} if user_token else {"credentials_provider": cfg.authenticate}
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        **connect_kwargs,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params)
            df = cur.fetchall_arrow().to_pandas()
            print(f"[_run_query] -> {len(df)} row(s)", flush=True)
            return df


def _query(stmt: str, params=None) -> pd.DataFrame:
    """Run a parameterised SQL query using the request's user token or SP credentials."""
    if _LOCAL_DEV:
        return _dummy_query(stmt, params)
    user_token = flask.request.headers.get("X-Forwarded-Access-Token")
    if not user_token:
        raise RuntimeError("Missing X-Forwarded-Access-Token header.")
    _log_token_info(user_token)
    return _run_query(stmt, params, user_token=user_token if USE_USER_TOKEN else None)


def _df_to_store(df: pd.DataFrame) -> str:
    """Serialize a DataFrame to a base64-encoded Arrow IPC stream for dcc.Store."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")


def _store_to_df(data: str) -> pd.DataFrame:
    """Deserialize a base64-encoded Arrow IPC stream produced by _df_to_store."""
    return pa.ipc.open_stream(base64.b64decode(data)).read_pandas()


def _fetch_filenames() -> list[str] | None:
    try:
        df = _query(f"SELECT _source_file FROM {_SOURCE_FILES_TABLE} ORDER BY _source_file LIMIT 200")
        print(f"[_fetch_filenames] fetched {len(df)} file(s)", flush=True)
        return df["_source_file"].tolist()
    except Exception as exc:
        print(f"[_fetch_filenames] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_all_signals(filenames: list[str] | None = None, limit: int = 500) -> list[dict] | None:
    try:
        if filenames:
            # File-filtered: query gold table (catalog table has no _source_file column)
            placeholders = ", ".join(["?"] * len(filenames))
            stmt = (
                f"SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}"
                f" WHERE _source_file IN ({placeholders})"
                f" ORDER BY signal_source, channel, signal_name LIMIT {limit}"
            )
            params: list | None = list(filenames)
        else:
            # Unfiltered: query the pre-aggregated catalog table (fast, no gold table scan)
            stmt = (
                f"SELECT signal_name, signal_source, channel FROM {_CATALOG_TABLE}"
                f" ORDER BY signal_source, channel, signal_name LIMIT {limit}"
            )
            params = None
        df = _query(stmt, params)
        print(f"[_fetch_all_signals] fetched {len(df)} signal(s)", flush=True)
        return df.to_dict("records")
    except Exception as exc:
        print(f"[_fetch_all_signals] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_all_channels(filenames: list[str] | None = None) -> list[dict] | None:
    """Fetch the distinct (signal_source, channel) pairs, uncapped.

    Kept separate from _fetch_all_signals so the channel-filter checklist isn't
    limited to whatever channels happen to fall within the 500-row signal browse cache.
    """
    try:
        if filenames:
            placeholders = ", ".join(["?"] * len(filenames))
            stmt = (
                f"SELECT DISTINCT signal_source, channel FROM {_GOLD_TABLE}"
                f" WHERE _source_file IN ({placeholders})"
                f" ORDER BY signal_source, channel"
            )
            params: list | None = list(filenames)
        else:
            stmt = f"SELECT DISTINCT signal_source, channel FROM {_CATALOG_TABLE} ORDER BY signal_source, channel"
            params = None
        df = _query(stmt, params)
        print(f"[_fetch_all_channels] fetched {len(df)} channel(s)", flush=True)
        return df.to_dict("records")
    except Exception as exc:
        print(f"[_fetch_all_channels] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_signals_by_search(
    keyword: str, filenames: list[str] | None = None, limit: int = 500
) -> tuple[list[dict], bool]:
    """Search signals matching keyword server-side (not limited to the cached browse window)."""
    try:
        like = f"%{keyword}%"
        if filenames:
            placeholders = ", ".join(["?"] * len(filenames))
            stmt = (
                f"SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}"
                f" WHERE _source_file IN ({placeholders}) AND LOWER(signal_name) LIKE ?"
                f" ORDER BY signal_source, channel, signal_name LIMIT {limit + 1}"
            )
            params: list = [*filenames, like]
        else:
            stmt = (
                f"SELECT signal_name, signal_source, channel FROM {_CATALOG_TABLE}"
                f" WHERE LOWER(signal_name) LIKE ? ORDER BY signal_source, channel, signal_name LIMIT {limit + 1}"
            )
            params = [like]
        df = _query(stmt, params)
        rows = df.to_dict("records")
        truncated = len(rows) > limit
        print(
            f"[_fetch_signals_by_search] keyword={keyword!r} -> {len(rows)} row(s), truncated={truncated}", flush=True
        )
        return rows[:limit], truncated
    except Exception as exc:
        print(f"[_fetch_signals_by_search] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return [], False


def _fetch_global_time_range() -> dict | None:
    try:
        df = _query(f"SELECT t_min, t_max, t0 FROM {_TIME_RANGE_TABLE}")
        t0_raw = df["t0"].iloc[0] if "t0" in df.columns else None
        if t0_raw is not None and pd.isna(t0_raw):
            t0_raw = None
        t0_ts = pd.Timestamp(t0_raw) if t0_raw is not None else None
        return {
            "min": float(df["t_min"].iloc[0]),
            "max": float(df["t_max"].iloc[0]),
            "t0": t0_ts.isoformat() if isinstance(t0_ts, pd.Timestamp) else None,
        }
    except Exception as exc:
        print(f"[_fetch_global_time_range] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_video_for_file(filename: str) -> dict | None:
    """Look up the video matching a BLF source file by filename stem (e.g. drive001.blf <-> drive001.mp4)."""
    try:
        stmt = (
            f"SELECT _video_path, _video_mtime FROM {_VIDEO_FILES_TABLE}"
            r" WHERE _video_stem = regexp_extract(?, '([^/]+)\.[^./]+$', 1) LIMIT 1"
        )
        df = _query(stmt, [filename])
        if df.empty:
            return None
        mtime_raw = df["_video_mtime"].iloc[0]
        mtime_ts = pd.Timestamp(mtime_raw) if pd.notna(mtime_raw) else None
        return {
            "video_path": df["_video_path"].iloc[0],
            "mtime": mtime_ts.isoformat() if isinstance(mtime_ts, pd.Timestamp) else None,
        }
    except Exception as exc:
        print(f"[_fetch_video_for_file] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None
