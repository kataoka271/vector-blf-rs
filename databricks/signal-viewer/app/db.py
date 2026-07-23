"""Database query helpers."""

import base64
import datetime
import json
import traceback

import dash
import flask
import pandas as pd

from databricks import sql

from .config import (
    _CATALOG_TABLE,
    _GOLD_TABLE,
    _LOCAL_DEV,
    _SOURCE_FILES_TABLE,
    _TIME_RANGE_TABLE,
    _VIDEO_FILES_TABLE,
    USE_USER_TOKEN,
    _parse_key,
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


def _fetch_filenames() -> list[str] | None:
    try:
        df = _query(f"SELECT _source_file FROM {_SOURCE_FILES_TABLE} ORDER BY _source_file LIMIT 200")
        print(f"[_fetch_filenames] fetched {len(df)} file(s)", flush=True)
        return df["_source_file"].tolist()
    except Exception as exc:
        print(f"[_fetch_filenames] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_all_signals(filenames: list[str] | None = None, limit: int | None = 500) -> list[dict] | None:
    """Fetch known signals, ordered by (signal_source, channel, signal_name).

    `limit` caps rows per signal_source (not the total row count), via a
    ROW_NUMBER() window partitioned by signal_source. A plain `ORDER BY
    signal_source ... LIMIT n` would let an alphabetically-earlier source
    with many rows (e.g. CAN) crowd out a later one (e.g. SOMEIP) entirely
    once the true row count exceeds `limit`.
    `limit=None` fetches the full catalog uncapped.
    """
    try:
        per_source_limit = max(1, limit // 3) if limit is not None else None
        if filenames:
            # File-filtered: query gold table (catalog table has no _source_file column)
            placeholders = ", ".join(["?"] * len(filenames))
            if per_source_limit is not None:
                stmt = (
                    f"SELECT signal_name, signal_source, channel FROM ("
                    f"  SELECT signal_name, signal_source, channel,"
                    f"    ROW_NUMBER() OVER (PARTITION BY signal_source ORDER BY channel, signal_name) AS rn"
                    f"  FROM (SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}"
                    f"        WHERE _source_file IN ({placeholders}))"
                    f") WHERE rn <= {per_source_limit}"
                    f" ORDER BY signal_source, channel, signal_name"
                )
            else:
                stmt = (
                    f"SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}"
                    f" WHERE _source_file IN ({placeholders})"
                    f" ORDER BY signal_source, channel, signal_name"
                )
            params: list | None = list(filenames)
        else:
            # Unfiltered: query the pre-aggregated catalog table (fast, no gold table scan)
            if per_source_limit is not None:
                stmt = (
                    f"SELECT signal_name, signal_source, channel FROM ("
                    f"  SELECT signal_name, signal_source, channel,"
                    f"    ROW_NUMBER() OVER (PARTITION BY signal_source ORDER BY channel, signal_name) AS rn"
                    f"  FROM {_CATALOG_TABLE}"
                    f") WHERE rn <= {per_source_limit}"
                    f" ORDER BY signal_source, channel, signal_name"
                )
            else:
                stmt = (
                    f"SELECT signal_name, signal_source, channel FROM {_CATALOG_TABLE}"
                    f" ORDER BY signal_source, channel, signal_name"
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
    """Search signals matching keyword server-side (not limited to the cached browse window).

    Caps matches per signal_source (not the total), same rationale as
    _fetch_all_signals: a keyword matching many CAN/ETH signal names must not
    starve SOMEIP matches out of the result set via `ORDER BY signal_source
    ... LIMIT n`.
    """
    try:
        words = keyword.split()
        words_placeholders = ", ".join(["?"] * len(words))
        match_clause = f"forall(array({words_placeholders}), w -> signal_name ILIKE concat('%', w, '%'))"
        per_source_limit = max(1, limit // 3)
        fetch_cap = per_source_limit + 1  # one extra row per source, to detect truncation
        if filenames:
            placeholders = ", ".join(["?"] * len(filenames))
            stmt = (
                f"SELECT signal_name, signal_source, channel FROM ("
                f"  SELECT signal_name, signal_source, channel,"
                f"    ROW_NUMBER() OVER (PARTITION BY signal_source ORDER BY channel, signal_name) AS rn"
                f"  FROM (SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}"
                f"        WHERE _source_file IN ({placeholders}) AND {match_clause})"
                f") WHERE rn <= {fetch_cap}"
                f" ORDER BY signal_source, channel, signal_name"
            )
            params: list = [*filenames, *words]
        else:
            stmt = (
                f"SELECT signal_name, signal_source, channel FROM ("
                f"  SELECT signal_name, signal_source, channel,"
                f"    ROW_NUMBER() OVER (PARTITION BY signal_source ORDER BY channel, signal_name) AS rn"
                f"  FROM {_CATALOG_TABLE} WHERE {match_clause}"
                f") WHERE rn <= {fetch_cap}"
                f" ORDER BY signal_source, channel, signal_name"
            )
            params = words
        df = _query(stmt, params)
        truncated = not df.empty and bool((df["signal_source"].value_counts() > per_source_limit).any())
        if truncated:
            df = df.groupby("signal_source", group_keys=False).head(per_source_limit)
        rows = df.to_dict("records")
        print(
            f"[_fetch_signals_by_search] keyword={keyword!r} -> {len(rows)} row(s), truncated={truncated}", flush=True
        )
        return rows, truncated
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


def fetch_signal_data(
    sources, selected, max_pts, time_range, time_range_store, lat_key, lon_key, filenames
) -> tuple[pd.DataFrame | None, dict | object, str]:
    """Run the two-query fetch (unfiltered time-range + downsampled main query).

    Returns (df, new_time_range, message). df is None only on the validation
    guard paths (no source/no signal selected) and on a main-query error --
    callers rely on this None-vs-not-None distinction to mean "a successful
    query happened, even if it returned zero rows".
    """
    if not sources:
        return None, dash.no_update, "No source selected."

    keys = set(selected or [])
    keys.update(k for k in (lat_key, lon_key) if k)
    if not keys:
        return None, dash.no_update, "No signal selected."

    key_triples = [_parse_key(key) for key in keys]
    pair_filter = "(signal_source, channel, signal_name) IN (" + ", ".join(["(?, ?, ?)"] * len(key_triples)) + ")"
    pair_params = [part for triple in key_triples for part in triple]

    file_filter = ""
    file_params: list = []
    if filenames:
        file_filter = " AND _source_file IN (" + ", ".join(["?"] * len(filenames)) + ")"
        file_params = list(filenames)

    time_filter = ""
    time_params: list = []
    if time_range_store is not None and time_range is not None:
        t_lo, t_hi = float(time_range[0]), float(time_range[1])
        time_filter = " AND timestamp_s BETWEEN ? AND ?"
        time_params = [t_lo, t_hi]

    new_time_range: dict | object = dash.no_update
    range_stmt = (
        f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max, MIN(event_time) AS t0 "
        f"FROM {_GOLD_TABLE} WHERE {pair_filter}{file_filter}"
    )
    try:
        range_df = _query(range_stmt, list(pair_params) + file_params)
        t0_raw = range_df["t0"].iloc[0] if "t0" in range_df.columns else None
        if t0_raw is not None and pd.isna(t0_raw):
            t0_raw = None
        t0_ts = pd.Timestamp(t0_raw) if t0_raw is not None else None
        t0_iso = t0_ts.isoformat() if isinstance(t0_ts, pd.Timestamp) else None
        new_time_range = {
            "min": float(range_df["t_min"].iloc[0]),
            "max": float(range_df["t_max"].iloc[0]),
            "t0": t0_iso,
        }
    except Exception as exc:
        print(f"[fetch_signal_data] time-range query error: {exc}", flush=True)

    stmt = (
        f"WITH bucketed AS ("
        f"  SELECT signal_source, channel, signal_name, event_time, timestamp_s, timestamp_ns, signal_value, signal_str,"
        f"    NTILE({int(max_pts)}) OVER ("
        f"      PARTITION BY signal_source, channel, signal_name ORDER BY timestamp_ns"
        f"    ) AS bucket"
        f"  FROM {_GOLD_TABLE} WHERE {pair_filter}{file_filter}{time_filter}"
        f"), agg AS ("
        f"  SELECT signal_source, channel, signal_name, bucket,"
        f"    MIN_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value, signal_str), signal_value) AS lo,"
        f"    MAX_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value, signal_str), signal_value) AS hi"
        f"  FROM bucketed GROUP BY signal_source, channel, signal_name, bucket"
        f") SELECT * FROM ("
        f"  SELECT signal_source, channel, signal_name, lo.event_time AS event_time, lo.timestamp_s AS timestamp_s,"
        f"    lo.timestamp_ns AS timestamp_ns, lo.signal_value AS signal_value, lo.signal_str AS signal_str FROM agg"
        f"  UNION ALL"
        f"  SELECT signal_source, channel, signal_name, hi.event_time, hi.timestamp_s, hi.timestamp_ns, hi.signal_value, hi.signal_str FROM agg"
        f") ORDER BY signal_source, channel, signal_name, event_time, timestamp_ns"
    )
    try:
        df = _query(stmt, pair_params + file_params + time_params)
    except Exception as exc:
        msg = f"Query error: {exc}"
        print(f"[fetch_signal_data] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None, new_time_range, msg

    signal_count = df["signal_name"].nunique() if not df.empty else 0
    print(f"[fetch_signal_data] {len(df):,} rows, {signal_count} signal(s)", flush=True)
    return df, new_time_range, f"Fetched {len(df):,} pts, {signal_count} signal(s)."


def _fetch_video_for_file(filename: str) -> dict | None:
    """Look up the video matching a BLF source file by filename stem (e.g. drive001.blf <-> drive001.mp4)."""
    try:
        stmt = (
            f"SELECT _video_path, _video_mtime FROM {_VIDEO_FILES_TABLE}"
            r" WHERE _video_stem = regexp_extract(?, '([^/]+)[.][^./]+$', 1) LIMIT 1"
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
