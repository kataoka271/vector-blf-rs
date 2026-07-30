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
    _to_utc_naive,
    cfg,
)
from .dummy import _dummy_query


def _log_token_info(token: str) -> None:
    """Log non-sensitive metadata from the X-Forwarded-Access-Token.

    Logs the subject, issuer, scope and expiry for a JWT, or just a short prefix
    and the length for an opaque token. Never logs the token itself.
    """
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
    """Execute a SQL query on the configured warehouse and return the rows as a DataFrame.

    `params` binds the statement's `?` markers, in placeholder order. Authenticates
    as the end user when `user_token` is given, otherwise with the app's own
    (service principal) credentials. Opens and closes a connection per call.
    """
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
    """Return the caller's X-Forwarded-Access-Token, or None in local dev.

    Must be called from the thread handling the Flask request. Raises RuntimeError
    when the header is missing.
    """
    if _LOCAL_DEV:
        return None
    # `flask.request` is only valid on the request thread, which is why callers that
    # fan out to workers (see _run_parallel) resolve the token here once and pass it
    # through explicitly instead of letting `_query` re-read the header downstream.
    user_token = flask.request.headers.get("X-Forwarded-Access-Token")
    if not user_token:
        raise RuntimeError("Missing X-Forwarded-Access-Token header.")
    _log_token_info(user_token)
    return user_token


def _query(stmt: str, params=None, *, user_token: str | None = None) -> pd.DataFrame:
    """Run a parameterised SQL query using the request's user token or SP credentials.

    `user_token`, if not given, is resolved from the current request context, so it
    must be passed explicitly when calling from a worker thread (see `_run_parallel`).
    In local dev this serves dummy data instead of reaching a warehouse.
    """
    if _LOCAL_DEV:
        return _dummy_query(stmt, params)
    if user_token is None:
        user_token = _resolve_user_token()
    return _run_query(stmt, params, user_token=user_token if USE_USER_TOKEN else None)


def _run_parallel(calls: list[Callable[[], Any]]) -> list[Any]:
    """Run independent no-arg fetch callables concurrently, returning results in call order.

    Callers must resolve any request-scoped state (e.g. the user token via
    `_resolve_user_token`) before building the callables; worker threads cannot
    access `flask.request`. Exceptions propagate from the first failing callable,
    so callables that need per-call error handling must catch and return it.
    """
    if len(calls) <= 1:
        return [c() for c in calls]
    # Each callable opens its own SQL connection (see _run_query), so this trades N
    # sequential connect+round-trip waits for the slowest single one.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as ex:
        futures = [ex.submit(c) for c in calls]
        return [f.result() for f in futures]


def _fetch_filenames(user_token: str | None = None) -> list[str] | None:
    """Fetch up to 200 ingested BLF source-file paths, sorted by path.

    Returns None if the query fails.
    """
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

# The picker labels a SOMEIP signal as ETH (see figures._display_src), so search has to
# match the label on screen as well as the stored value -- otherwise typing what you can
# read finds nothing. The client-side filter in callbacks.py checks both spellings too;
# the two must agree, or rows this query returns get dropped again in the browser.
_DISPLAY_SOURCE_SQL = "IF(signal_source = 'SOMEIP', 'ETH', signal_source)"


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
    """Fetch the distinct (signal_source, channel) pairs, ordered and uncapped.

    Restricted to `filenames` when given. Returns None if the query fails.
    """
    # Kept separate from _fetch_all_signals so the channel-filter checklist isn't
    # limited to the channels that happen to fall inside that function's
    # per-source-capped browse cache.
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

    A signal matches when every whitespace-separated word of `keyword` is contained in
    its name, its source, its channel number, or its source+channel label (e.g. "CAN3"
    or "ETH0", matching what's shown on screen), case-insensitively -- so both "can 1"
    and "can1" find the signals on CAN channel 1. Returns (rows, truncated): rows
    carries at most `limit` // 3 matches per signal_source (see _catalog_query), and
    `truncated` is True when that cap dropped matches.
    """
    try:
        words = keyword.split()
        per_source_limit = max(1, limit // 3)
        # A chain of `ILIKE ?` rather than forall(array(...), w -> signal_name ILIKE
        # concat('%', w, '%')): a bound parameter marker makes each pattern foldable,
        # so Catalyst rewrites it to a vectorized Contains() and pushes it into the
        # scan, whereas the lambda's pattern is per-row and compiles a regex.
        word_clause = (
            f"(signal_name ILIKE ? OR signal_source ILIKE ? OR {_DISPLAY_SOURCE_SQL} ILIKE ?"
            f" OR CAST(channel AS STRING) LIKE ?"
            f" OR CONCAT(signal_source, CAST(channel AS STRING)) ILIKE ?"
            f" OR CONCAT({_DISPLAY_SOURCE_SQL}, CAST(channel AS STRING)) ILIKE ?)"
        )
        stmt, params = _catalog_query(
            filenames,
            match_clause=" AND ".join([word_clause] * len(words)),
            match_params=[f"%{w}%" for w in words for _ in range(6)],
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


# An OR of equality conjuncts, not `(signal_source, channel, signal_name) IN ((?, ?, ?),
# ...)`: Photon has no columnar path for a struct-typed IN, and this filter sits directly
# on the gold-table scan, so the fallback drags the whole window/aggregate pipeline above
# it onto row-based Spark. Plain =/AND/OR stays vectorized and reaches the scan as data
# filters, so Delta can skip files.
_KEY_PRED = "(signal_source = ? AND channel = ? AND signal_name = ?)"


def _key_pred_block(keys: list[str]) -> tuple[str, list]:
    """Build an OR of _KEY_PRED over `keys`, with its parameters in placeholder order."""
    return "(" + " OR ".join([_KEY_PRED] * len(keys)) + ")", [part for k in keys for part in _parse_key(k)]


def _file_pred_clause(filenames: list[str] | None) -> tuple[str, list]:
    """Build the ` AND _source_file IN (...)` suffix for `filenames`, or ("", []) for no filter."""
    if not filenames:
        return "", []
    return " AND _source_file IN (" + ", ".join(["?"] * len(filenames)) + ")", list(filenames)


def _fetch_signal_file_scopes(
    keys: list[str], filenames: list[str] | None, user_token: str | None = None
) -> tuple[dict[str, list[str]], list[str]]:
    """Split `keys` by whether they occur in the currently selected `filenames`.

    Returns (out_of_scope, unknown). `out_of_scope` maps a key that the catalog knows
    but that appears in none of `filenames` to every file that does contain it, sorted.
    `unknown` lists keys the catalog has no row for at all. A key found in at least one
    selected file appears in neither -- callers need no entry for the ordinary case.

    An empty `filenames` means "no file filter is in effect", so nothing can be out of
    scope and no query is issued. A failed query degrades the same way, reporting
    everything as in scope rather than blocking the caller.
    """
    if not keys or not filenames:
        return {}, []
    try:
        key_block, key_params = _key_pred_block(keys)
        stmt = (
            f"SELECT DISTINCT signal_source, channel, signal_name, _source_file"
            f" FROM {_CATALOG_BY_FILE_TABLE} WHERE {key_block}"
        )
        df = _query(stmt, key_params, user_token=user_token)
        files_by_key: dict[str, set[str]] = {}
        for row in df.to_dict("records"):
            key = f"{row['signal_source']}{row['channel']}::{row['signal_name']}"
            files_by_key.setdefault(key, set()).add(row["_source_file"])
        selected = set(filenames)
        out_of_scope: dict[str, list[str]] = {}
        unknown: list[str] = []
        for key in keys:
            found = files_by_key.get(key)
            if not found:
                unknown.append(key)
            elif not (found & selected):
                out_of_scope[key] = sorted(found)
        print(
            f"[_fetch_signal_file_scopes] {len(keys)} key(s) -> "
            f"{len(out_of_scope)} out of scope, {len(unknown)} unknown",
            flush=True,
        )
        return out_of_scope, unknown
    except Exception as exc:
        print(f"[_fetch_signal_file_scopes] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return {}, []


def _time_bounds(df: pd.DataFrame) -> tuple[float, float] | None:
    """Read (t_min, t_max) out of a single-row bounds query, or None when there are none.

    A MIN()/MAX() over zero matching rows still returns one row, holding NULL --
    which arrives here as NaN. Passing that on would put NaN in time-range-store,
    where Dash serializes it to JSON null and every later `t_max - t_min` raises
    TypeError, so callers get None and keep the bounds they already had.
    """
    if df.empty:
        return None
    t_min_raw = df["t_min"].iloc[0]
    t_max_raw = df["t_max"].iloc[0]
    if pd.isna(t_min_raw) or pd.isna(t_max_raw):
        return None
    return float(t_min_raw), float(t_max_raw)


def _fetch_global_time_range(user_token: str | None = None) -> dict | None:
    """Fetch the timestamp bounds covering every ingested signal.

    Returns {"min": float, "max": float, "t0": ISO-8601 str | None}, where t0 is the
    wall-clock time of the first sample, or None if the query fails.
    """
    try:
        df = _query(f"SELECT t_min, t_max, t0 FROM {_TIME_RANGE_TABLE}", user_token=user_token)
        bounds = _time_bounds(df)
        if bounds is None:
            print("[_fetch_global_time_range] table holds no usable bounds", flush=True)
            return None
        t_min, t_max = bounds
        t0_raw = df["t0"].iloc[0] if "t0" in df.columns else None
        if t0_raw is not None and pd.isna(t0_raw):
            t0_raw = None
        t0_ts = pd.Timestamp(t0_raw) if t0_raw is not None else None
        return {
            "min": t_min,
            "max": t_max,
            # Normalized to tz-naive UTC to match figures._plot_x's chart x-values --
            # otherwise the browser's new Date() parses this offset-bearing string as
            # UTC but the tz-stripped chart x-values as local time, and video-sync.js's
            # click-to-seek math comes out off by the browser's UTC offset.
            "t0": _to_utc_naive(t0_ts).isoformat() if isinstance(t0_ts, pd.Timestamp) else None,
        }
    except Exception as exc:
        print(f"[_fetch_global_time_range] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def fetch_signal_data(
    sources, selected, max_pts, time_range, time_range_store, lat_key, lon_key, filenames, extra_scope=None
) -> tuple[pd.DataFrame | None, dict | object, str, bool]:
    """Run the two-query fetch (unfiltered time-range + downsampled main query).

    Fetches the signals named by `selected` plus `lat_key`/`lon_key` (keys in
    `_parse_key` form), restricted to `filenames` when given, and to `time_range`
    ([lo, hi] seconds) when `time_range_store` says a range is in effect. `sources`
    only gates the fetch: an empty value returns early. Each signal is reduced to at
    most 2 * `max_pts` points.

    `extra_scope` maps a key to the files it should be read from instead of
    `filenames`; those keys are fetched from their own files and every other key
    still honours the sidebar's file selection.

    Returns (df, new_time_range, message, widened). df is None only on the
    validation guard paths (no source/no signal selected) and on a main-query
    error -- callers rely on this None-vs-not-None distinction to mean "a
    successful query happened, even if it returned zero rows". `new_time_range`
    is `dash.no_update` when the bounds could not be refreshed. `widened` is True
    when a requested key was missing under the active `time_range` filter and
    got pulled in by dropping that filter -- callers should push the time-range
    slider out to `new_time_range` so it isn't left showing a window narrower
    than what's now plotted.
    """
    if not sources:
        return None, dash.no_update, "No source selected.", False

    keys = set(selected or [])
    keys.update(k for k in (lat_key, lon_key) if k)
    if not keys:
        return None, dash.no_update, "No signal selected.", False

    # Genie can name a signal that no selected file contains (see
    # _fetch_signal_file_scopes). Giving those keys their own file scope lets them plot
    # alongside the rest instead of silently contributing zero rows, without widening
    # the file filter that the other keys -- and the sidebar -- are working under.
    per_key_scope = {k: v for k, v in (extra_scope or {}).items() if k in keys and v}
    scoped_keys = sorted(keys - set(per_key_scope))

    blocks: list[str] = []
    base_params: list = []
    if scoped_keys:
        key_block, key_params = _key_pred_block(scoped_keys)
        file_clause, file_params = _file_pred_clause(filenames)
        blocks.append(f"({key_block}{file_clause})")
        base_params += key_params + file_params
    for key in sorted(per_key_scope):
        file_clause, file_params = _file_pred_clause(per_key_scope[key])
        blocks.append(f"({_KEY_PRED}{file_clause})")
        base_params += list(_parse_key(key)) + file_params
    where = "(" + " OR ".join(blocks) + ")"

    time_filter = ""
    time_params: list = []
    if time_range_store is not None and time_range is not None:
        t_lo, t_hi = float(time_range[0]), float(time_range[1])
        time_filter = " AND timestamp_s BETWEEN ? AND ?"
        time_params = [t_lo, t_hi]

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
        bounds = _time_bounds(range_df)
        if bounds is None:
            # No row matched the key/file filter at all, so there are no bounds to
            # move the slider to -- leave it on the range it already shows.
            print("[fetch_signal_data] time-range query matched no rows; keeping current bounds", flush=True)
        else:
            t0_raw = range_df["t0"].iloc[0] if "t0" in range_df.columns else None
            if t0_raw is not None and pd.isna(t0_raw):
                t0_raw = None
            t0_ts = pd.Timestamp(t0_raw) if t0_raw is not None else None
            # See _fetch_global_time_range for why this must match figures._plot_x's
            # tz-naive-UTC chart x-values rather than keeping the SQL session's tzinfo.
            t0_iso = _to_utc_naive(t0_ts).isoformat() if isinstance(t0_ts, pd.Timestamp) else None
            new_time_range = {"min": bounds[0], "max": bounds[1], "t0": t0_iso}

    if isinstance(main_result, Exception):
        msg = f"Query error: {main_result}"
        tb = "".join(traceback.format_exception(type(main_result), main_result, main_result.__traceback__))
        print(f"[fetch_signal_data] ERROR: {main_result}\n{tb}", flush=True)
        return None, new_time_range, msg, False
    df = main_result

    # A signal added to `selected` this call can have all of its data outside the
    # still-active time window from a previous, narrower selection -- e.g. the window
    # was fit to CAN1/CAN2/ETH1 (~0.8-48.8s) and the newly added ETH4 signal only has
    # samples at ~100-110s. That signal would then always come back as zero rows, no
    # matter how many times Plot is clicked, with nothing telling the user why.
    #
    # Only retry when a *requested* key is entirely absent from `df` -- not merely
    # whenever the unfiltered bounds exceed the window -- so a plain re-plot of an
    # unchanged selection under a manually narrowed window (fewer rows per signal,
    # but every signal still present) is left alone rather than silently un-zoomed.
    widened = False
    if time_filter:
        present = set(zip(df["signal_source"], df["channel"], df["signal_name"])) if not df.empty else set()
        requested = {_parse_key(k) for k in keys}
        if requested - present:
            try:
                wide_df = _query(stmt.replace(time_filter, "", 1), base_params, user_token=user_token)
            except Exception as exc:
                tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                print(f"[fetch_signal_data] ERROR widening time filter: {exc}\n{tb}", flush=True)
            else:
                wide_present = (
                    set(zip(wide_df["signal_source"], wide_df["channel"], wide_df["signal_name"]))
                    if not wide_df.empty
                    else set()
                )
                # Only actually widen if it recovered a key the filtered query
                # missed -- otherwise the key has no data anywhere and the narrow
                # result already reflects that correctly.
                if wide_present - present:
                    print(
                        f"[fetch_signal_data] widening time filter: {requested - present} "
                        f"missing under current window, found under full range",
                        flush=True,
                    )
                    df = wide_df
                    widened = True

    signal_count = df["signal_name"].nunique() if not df.empty else 0
    print(f"[fetch_signal_data] {len(df):,} rows, {signal_count} signal(s)", flush=True)
    return df, new_time_range, f"Fetched {len(df):,} pts, {signal_count} signal(s).", widened


def _fetch_video_for_file(filename: str) -> dict | None:
    """Look up the video matching a BLF source file by filename stem (e.g. drive001.blf <-> drive001.mp4).

    Returns {"video_path": str, "mtime": ISO-8601 str | None}, or None when no video
    matches or the query fails.
    """
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


def _fetch_per_file_time_ranges(filenames: list[str], key_triples: list[tuple] | None = None) -> dict[str, dict]:
    """MIN/MAX timestamp_s and MIN event_time per _source_file, optionally restricted to
    the given signal keys. Used to anchor each file's video segment when building a
    playback playlist (see update_video_panel): callers pass key_triples first to anchor
    to the selected signal(s)' own span, then re-query with key_triples=None for any file
    that comes back empty, falling back to that file's overall span (across every signal)
    so its video can still be aligned and played by time alone even when it holds none of
    the currently selected signals."""
    try:
        where = f"_source_file IN ({', '.join(['?'] * len(filenames))})"
        params: list = list(filenames)
        if key_triples:
            pair_filter = (
                "(signal_source, channel, signal_name) IN (" + ", ".join(["(?, ?, ?)"] * len(key_triples)) + ")"
            )
            pair_params = [part for triple in key_triples for part in triple]
            where = f"{pair_filter} AND {where}"
            params = pair_params + params
        stmt = (
            f"SELECT _source_file AS source_file, MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max,"
            f" MIN(event_time) AS t0 FROM {_GOLD_TABLE}"
            f" WHERE {where} GROUP BY _source_file"
        )
        df = _query(stmt, params)
        out = {}
        for _, row in df.iterrows():
            t0_raw = row["t0"]
            t0_ts = pd.Timestamp(t0_raw) if pd.notna(t0_raw) else None
            out[row["source_file"]] = {
                "t_min": float(row["t_min"]),
                "t_max": float(row["t_max"]),
                # See _fetch_global_time_range for why this must match figures._plot_x's
                # tz-naive-UTC chart x-values rather than keeping the SQL session's tzinfo.
                "t0": _to_utc_naive(t0_ts).isoformat() if isinstance(t0_ts, pd.Timestamp) else None,
            }
        return out
    except Exception as exc:
        print(f"[_fetch_per_file_time_ranges] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return {}
