"""In-process, per-session server-side cache for fetched signal DataFrames.

Keeps the large fetched DataFrame out of the browser (dcc.Store) entirely.
One entry per session id, overwritten (not accumulated) on every new fetch,
with LRU eviction as a safety net for many concurrent tabs/users. Assumes a
single-process deployment (see app.yaml: `python app.py`, no gunicorn/worker
config) -- if that ever changes to multiple replicas without sticky sessions,
a redraw could miss the cache on a different worker; callers must treat a
miss as "no data yet", not as an error.
"""

import collections
import threading

import pandas as pd

_MAX_SESSIONS = 50

_lock = threading.Lock()
_cache: "collections.OrderedDict[str, pd.DataFrame]" = collections.OrderedDict()


def put_df(session_id: str, df: pd.DataFrame) -> None:
    """Store/overwrite the DataFrame for a session id, evicting the LRU entry past capacity."""
    with _lock:
        _cache[session_id] = df
        _cache.move_to_end(session_id)
        while len(_cache) > _MAX_SESSIONS:
            _cache.popitem(last=False)


def get_df(session_id: str | None) -> "pd.DataFrame | None":
    """Look up the cached DataFrame for a session id, or None on miss/expiry."""
    if not session_id:
        return None
    with _lock:
        df = _cache.get(session_id)
        if df is not None:
            _cache.move_to_end(session_id)
        return df
