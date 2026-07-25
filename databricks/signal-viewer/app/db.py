"""Database query helpers."""

import base64
import concurrent.futures
import datetime
import json
import traceback
from typing import Any, Callable

import dash
import flask
import pandas as pd

from databricks import sql

from .config import (
    _CATALOG_BY_FILE_TABLE,
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


def _resolve_user_token() -> str | None:
    """Read the X-Forwarded-Access-Token header from the current request context.

    Must be called from the thread handling the Flask request -- `flask.request`
    is only valid there. Callers that fan out queries to worker threads (see
    `_run_parallel`) must resolve the token once up front and pass it through
    explicitly, rather than letting `_query` re-read `flask.request` from a
    worker thread.
    """
    if _LOCAL_DEV:
        return None
    user_token = flask.request.headers.get("X-Forwarded-Access-Token")
    if not user_token:
        raise RuntimeError("Missing X-Forwarded-Access-Token header.")
    _log_token_info(user_token)
    return user_token


def _query(stmt: str, params=None, *, user_token: str | None = None) -> pd.DataFrame:
    """Run a parameterised SQL query using the request's user token or SP credentials.

    `user_token`, if not given, is resolved from the current request context.
    Pass it explicitly when calling from a worker thread (see `_run_parallel`).
    """
    if _LOCAL_DEV:
        return _dummy_query(stmt, params)
    if user_token is None:
        user_token = _resolve_user_token()
    return _run_query(stmt, params, user_token=user_token if USE_USER_TOKEN else None)


def _run_parallel(calls: list[Callable[[], Any]]) -> list[Any]:
    """Run independent no-arg fetch callables concurrently, preserving call order.

    Each callable is expected to open its own SQL connection (see `_run_query`),
    so this trades N sequential connect+round-trip waits for the slowest single
    one. Callers must resolve any request-scoped state (e.g. the user token via
    `_resolve_user_token`) before building these callables, since worker threads
    cannot access `flask.request`.
    """
    if len(calls) <= 1:
        return [c() for c in calls]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as ex:
        futures = [ex.submit(c) for c in calls]
        return [f.result() for f in futures]


def _fetch_filenames(user_token: str | None = None) -> list[str] | None:
    try:
        df = _query(
            f"SELECT _source_file FROM {_SOURCE_FILES_TABLE} ORDER BY _source_file LIMIT 200",
            user_token=user_token,
        )
        print(f"[_fetch_filenames] fetched {len(df)} file(s)", flush=True)
        return df["_source_file"].tolist()
    except Exception as exc:
        print(f"[_fetch_filenames] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


_CATALOG_COLS = "signal_name, signal_source, channel"


def _catalog_query(
    filenames: list[str] | None,
    *,
    match_clause: str = "",
    match_params: list | None = None,
    per_source_limit: int | None = None,
) -> tuple[str, list | None]:
    """Build a signal-catalog browse query, ordered by (signal_source, channel, signal_name).

    Selects (signal_name, signal_source, channel) from the pre-aggregated catalog
    table, or -- when `filenames` is given -- from the per-file catalog restricted
    to those files and de-duplicated.

    `match_clause` is an optional SQL predicate spliced in after WHERE, with
    `match_params` supplying its parameters. `per_source_limit` caps how many rows
    each signal_source may contribute; `None` leaves the result uncapped.

    Returns (statement, params). `params` is in placeholder order -- filenames
    first, then `match_params` -- or None if the statement takes no parameters.
    """
    params: list = []
    if filenames:
        # The per-file catalog is clustered on _source_file, so this stays cheap
        # even though it holds one row per (file, signal). DISTINCT needs its own
        # subquery: QUALIFY numbers rows before de-duplication.
        placeholders = ", ".join(["?"] * len(filenames))
        source = (
            f"(SELECT DISTINCT {_CATALOG_COLS} FROM {_CATALOG_BY_FILE_TABLE} WHERE _source_file IN ({placeholders}))"
        )
        params += list(filenames)
    else:
        source = _CATALOG_TABLE
    stmt = f"SELECT {_CATALOG_COLS} FROM {source}"
    if match_clause:
        stmt += f" WHERE {match_clause}"
        params += match_params or []
    if per_source_limit is not None:
        # Capped per source rather than in total: `ORDER BY signal_source ... LIMIT n`
        # would let an alphabetically-earlier source with many rows (e.g. CAN) crowd
        # out a later one (e.g. SOMEIP) entirely once the row count exceeds the limit.
        stmt += (
            f" QUALIFY ROW_NUMBER() OVER (PARTITION BY signal_source ORDER BY channel, signal_name)"
            f" <= {per_source_limit}"
        )
    return stmt + " ORDER BY signal_source, channel, signal_name", params or None


def _fetch_all_signals(
    filenames: list[str] | None = None, limit: int | None = 500, user_token: str | None = None
) -> list[dict] | None:
    """Fetch known signals, ordered by (signal_source, channel, signal_name).

    Returns at most `limit` // 3 signals per signal_source (see _catalog_query);
    `limit=None` fetches the full catalog uncapped.
    """
    try:
        stmt, params = _catalog_query(filenames, per_source_limit=max(1, limit // 3) if limit is not None else None)
        df = _query(stmt, params, user_token=user_token)
        print(f"[_fetch_all_signals] fetched {len(df)} signal(s)", flush=True)
        return df.to_dict("records")
    except Exception as exc:
        print(f"[_fetch_all_signals] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_all_channels(filenames: list[str] | None = None, user_token: str | None = None) -> list[dict] | None:
    """Fetch the distinct (signal_source, channel) pairs, uncapped.

    Kept separate from _fetch_all_signals so the channel-filter checklist isn't
    limited to whatever channels happen to fall within the 500-row signal browse cache.
    """
    try:
        if filenames:
            placeholders = ", ".join(["?"] * len(filenames))
            stmt = (
                f"SELECT DISTINCT signal_source, channel FROM {_CATALOG_BY_FILE_TABLE}"
                f" WHERE _source_file IN ({placeholders})"
                f" ORDER BY signal_source, channel"
            )
            params: list | None = list(filenames)
        else:
            stmt = f"SELECT DISTINCT signal_source, channel FROM {_CATALOG_TABLE} ORDER BY signal_source, channel"
            params = None
        df = _query(stmt, params, user_token=user_token)
        print(f"[_fetch_all_channels] fetched {len(df)} channel(s)", flush=True)
        return df.to_dict("records")
    except Exception as exc:
        print(f"[_fetch_all_channels] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_signals_by_search(
    keyword: str, filenames: list[str] | None = None, limit: int = 500
) -> tuple[list[dict], bool]:
    """Search signals matching keyword server-side (not limited to the cached browse window).

    A signal matches when its name contains every whitespace-separated word of
    `keyword`, case-insensitively. Returns (rows, truncated): rows carries at most
    `limit` // 3 matches per signal_source (see _catalog_query), and `truncated` is
    True when that cap dropped matches.
    """
    try:
        words = keyword.split()
        per_source_limit = max(1, limit // 3)
        # A chain of `ILIKE ?` rather than forall(array(...), w -> signal_name ILIKE
        # concat('%', w, '%')): a bound parameter marker makes each pattern foldable,
        # so Catalyst rewrites it to a vectorized Contains() and pushes it into the
        # scan, whereas the lambda's pattern is per-row and compiles a regex.
        stmt, params = _catalog_query(
            filenames,
            match_clause=" AND ".join(["signal_name ILIKE ?"] * len(words)),
            match_params=[f"%{w}%" for w in words],
            per_source_limit=per_source_limit + 1,  # one extra row per source, to detect truncation
        )
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


# Common name fragments for GPS position signals, used to pre-seed the Lat/Lon
# pickers with likely candidates regardless of where they'd otherwise fall in
# the (per-source-capped) browse cache -- see _fetch_all_signals.
# "lat"/"lon" already subsume "latitude"/"longitude" as substrings.
_LATLON_KEYWORDS = ["lat", "lon", "gps"]


def _fetch_latlon_candidates(
    filenames: list[str] | None = None, limit: int = 100, user_token: str | None = None
) -> list[dict] | None:
    """Fetch signals matching common GPS lat/lon keywords, capped per signal_source.

    A signal matches when its name contains any of `_LATLON_KEYWORDS`,
    case-insensitively. At most `limit` // 3 rows per signal_source (see
    _catalog_query).
    """
    try:
        # `ILIKE ?` terms for the same reason as _fetch_signals_by_search: each bound
        # pattern folds into a vectorized Contains(), where a single RLIKE alternation
        # would stay a regex. Parenthesised so the OR cannot bind past this clause.
        stmt, params = _catalog_query(
            filenames,
            match_clause="(" + " OR ".join(["signal_name ILIKE ?"] * len(_LATLON_KEYWORDS)) + ")",
            match_params=[f"%{kw}%" for kw in _LATLON_KEYWORDS],
            per_source_limit=max(1, limit // 3),
        )
        df = _query(stmt, params, user_token=user_token)
        rows = df.to_dict("records")
        print(f"[_fetch_latlon_candidates] -> {len(rows)} row(s)", flush=True)
        return rows
    except Exception as exc:
        print(f"[_fetch_latlon_candidates] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_global_time_range(user_token: str | None = None) -> dict | None:
    try:
        df = _query(f"SELECT t_min, t_max, t0 FROM {_TIME_RANGE_TABLE}", user_token=user_token)
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
    # An OR of equality conjuncts, not `(signal_source, channel, signal_name) IN
    # ((?, ?, ?), ...)`: Photon has no columnar path for a struct-typed IN, and this
    # filter sits directly on the gold-table scan, so the fallback drags the whole
    # window/aggregate pipeline above it onto row-based Spark. Plain =/AND/OR stays
    # vectorized and reaches the scan as data filters, so Delta can skip files.
    key_filter = "(" + " OR ".join(["(signal_source = ? AND channel = ? AND signal_name = ?)"] * len(key_triples)) + ")"
    key_params = [part for triple in key_triples for part in triple]

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

    where = f"{key_filter}{file_filter}"
    base_params = key_params + file_params

    new_time_range: dict | object = dash.no_update
    range_stmt = (
        f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max, MIN(event_time) AS t0 "
        f"FROM {_GOLD_TABLE} WHERE {where}"
    )

    # Downsample: NTILE splits each signal into max_pts equal-count buckets, and each
    # bucket contributes its lowest and highest sample so extremes survive. Carrying
    # the sample as one struct keeps the column list in a single place, and inline()
    # unpacks the (lo, hi) pair in one pass -- a UNION ALL of two SELECTs over `agg`
    # makes the scan/window/aggregate pipeline a candidate for being run twice.
    sample_cols = "event_time, timestamp_s, timestamp_ns, signal_value, signal_str"
    stmt = (
        f"WITH bucketed AS ("
        f"  SELECT signal_source, channel, signal_name, struct({sample_cols}) AS sample,"
        f"    NTILE({int(max_pts)}) OVER ("
        f"      PARTITION BY signal_source, channel, signal_name ORDER BY timestamp_ns"
        f"    ) AS bucket"
        f"  FROM {_GOLD_TABLE} WHERE {where}{time_filter}"
        f"), agg AS ("
        f"  SELECT signal_source, channel, signal_name,"
        f"    MIN_BY(sample, sample.signal_value) AS lo, MAX_BY(sample, sample.signal_value) AS hi"
        f"  FROM bucketed GROUP BY signal_source, channel, signal_name, bucket"
        f") SELECT signal_source, channel, signal_name, {sample_cols}"
        f" FROM agg LATERAL VIEW inline(array(lo, hi)) AS {sample_cols}"
        f" ORDER BY signal_source, channel, signal_name, event_time, timestamp_ns"
    )

    # Range and main queries are independent (the range query is deliberately
    # unfiltered by time_filter, to keep the slider bounds stable) -- run them
    # concurrently rather than paying two sequential connect+round-trip waits.
    user_token = _resolve_user_token()

    def _run_range() -> pd.DataFrame | Exception:
        try:
            return _query(range_stmt, base_params, user_token=user_token)
        except Exception as exc:
            return exc

    def _run_main() -> pd.DataFrame | Exception:
        try:
            return _query(stmt, base_params + time_params, user_token=user_token)
        except Exception as exc:
            return exc

    range_result, main_result = _run_parallel([_run_range, _run_main])

    if isinstance(range_result, Exception):
        print(f"[fetch_signal_data] time-range query error: {range_result}", flush=True)
    else:
        range_df = range_result
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

    if isinstance(main_result, Exception):
        msg = f"Query error: {main_result}"
        tb = "".join(traceback.format_exception(type(main_result), main_result, main_result.__traceback__))
        print(f"[fetch_signal_data] ERROR: {main_result}\n{tb}", flush=True)
        return None, new_time_range, msg
    df = main_result

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
