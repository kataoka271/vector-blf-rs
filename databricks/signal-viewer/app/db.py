"""Database query helpers and DataFrame serialization."""

import base64
import traceback

import flask
import pandas as pd
import pyarrow as pa
from databricks import sql

from .config import USE_USER_TOKEN, _GOLD_TABLE, _LOCAL_DEV, cfg
from .dummy import _dummy_query


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
        df = _query(f"SELECT DISTINCT _source_file FROM {_GOLD_TABLE} ORDER BY _source_file")
        print(f"[_fetch_filenames] fetched {len(df)} file(s)", flush=True)
        return df["_source_file"].tolist()
    except Exception as exc:
        print(f"[_fetch_filenames] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_all_signals(filenames: list[str] | None = None) -> list[dict] | None:
    try:
        where = ""
        params: list | None = None
        if filenames:
            placeholders = ", ".join(["?"] * len(filenames))
            where = f" WHERE _source_file IN ({placeholders})"
            params = list(filenames)
        df = _query(
            f"SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE}{where} ORDER BY signal_source, channel, signal_name",
            params,
        )
        print(f"[_fetch_all_signals] fetched {len(df)} signal(s)", flush=True)
        return df.to_dict("records")
    except Exception as exc:
        print(f"[_fetch_all_signals] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None


def _fetch_global_time_range() -> dict | None:
    try:
        df = _query(
            f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max, MIN(event_time) AS t0 FROM {_GOLD_TABLE}"
        )
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
