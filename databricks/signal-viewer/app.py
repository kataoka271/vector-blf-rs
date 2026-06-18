"""Signal Viewer — Plotly Dash visualization app for Databricks Apps."""

import base64
import concurrent.futures
import math
import os
import re
import struct
import time as _time
import traceback
import uuid as _uuid
from typing import Literal

import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import flask
import pandas as pd
import plotly.graph_objects as go
import pyarrow as pa
from dash import Input, Output, State, callback, dcc, html
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from databricks import sql

_LOCAL_DEV = not os.getenv("DATABRICKS_WAREHOUSE_ID")

# Config
USE_USER_TOKEN = True  # Set to False to use Service Principal credentials instead of user token
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
_GOLD_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"

_KEY_RE = re.compile(r"^([A-Za-z]+)(\d+)::(.+)$")


def _parse_key(key: str) -> tuple[str, int, str]:
    """Parse '{signal_source}{channel}::{signal_name}' into (source, channel, name)."""
    m = _KEY_RE.match(key)
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    prefix, name = key.split("::", 1)
    return prefix, 0, name


# Genie Space config
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
_genie_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="genie")
# Maps request_id -> (Future, created_at_epoch)
_genie_futures: dict[str, tuple[concurrent.futures.Future, float]] = {}

# Databricks config (skipped in local dev mode)
cfg = None if _LOCAL_DEV else Config()

if _LOCAL_DEV:
    print("[LOCAL DEV] No DATABRICKS_WAREHOUSE_ID -- serving dummy data.", flush=True)

    _DUMMY_N = 500
    _DUMMY_DURATION = 300.0
    _DUMMY_T = [i * _DUMMY_DURATION / (_DUMMY_N - 1) for i in range(_DUMMY_N)]
    _DUMMY_TS_NS = [int(t * 1e9) for t in _DUMMY_T]
    _DUMMY_T0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")

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
        if "DISTINCT" in stmt:
            rows = [{"signal_name": n, "signal_source": s, "channel": c} for s, c, n in _DUMMY_CATALOG]
            return pd.DataFrame(rows).sort_values(["signal_source", "channel", "signal_name"]).reset_index(drop=True)
        if "t_min" in stmt:
            return pd.DataFrame({"t_min": [0.0], "t_max": [_DUMMY_DURATION], "t0": [_DUMMY_T0]})
        if "PARTITION BY signal_source, channel, signal_name" in stmt:
            # params = [src, ch, name, src, ch, name, ...] + optional [t_lo, t_hi]
            if params:
                p = list(params)
                n_triples = len(p) // 3
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
                        }
                    )
            return pd.DataFrame(rows)
        # Single-signal fallback (ORDER BY timestamp_ns, no PARTITION BY)
        _fb = params or ["CAN", 1, "GPS_Latitude"]
        vals = _dummy_values(str(_fb[0]), int(_fb[1]), str(_fb[2]))
        return pd.DataFrame({"timestamp_ns": _DUMMY_TS_NS, "signal_value": vals})


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


# ---------------------------------------------------------------------------
# Perfetto native trace encoding (hand-written protobuf wire format)
# ---------------------------------------------------------------------------


def _pf_varint(v: int) -> bytes:
    out = []
    while True:
        if v < 0x80:
            out.append(v)
            break
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    return bytes(out)


def _pf_field(field: int, wt: int) -> bytes:
    return _pf_varint((field << 3) | wt)


def _pf_u64(field: int, v: int) -> bytes:
    return _pf_field(field, 0) + _pf_varint(v)


def _pf_f64(field: int, v: float) -> bytes:
    return _pf_field(field, 1) + struct.pack("<d", v)


def _pf_bytes(field: int, b: bytes) -> bytes:
    return _pf_field(field, 2) + _pf_varint(len(b)) + b


def _pf_str(field: int, s: str) -> bytes:
    return _pf_bytes(field, s.encode())


def _pf_packet(inner: bytes) -> bytes:
    return _pf_bytes(1, inner)  # Trace.packet = field 1


# Perfetto proto field numbers
_PF_PKT_CLOCK_SNAPSHOT = 6
_PF_PKT_TRACK_EVENT = 11
_PF_PKT_TIMESTAMP = 8
_PF_PKT_TIMESTAMP_CLOCK_ID = 58
_PF_PKT_TRUSTED_SEQ_ID = 10
_PF_PKT_TRACK_DESCRIPTOR = 60
_PF_CLK_ID, _PF_CLK_TIMESTAMP, _PF_SNAP_CLOCKS = 1, 2, 1
_PF_TD_UUID, _PF_TD_NAME, _PF_TD_COUNTER = 1, 2, 8
_PF_TE_TYPE, _PF_TE_TRACK_UUID, _PF_TE_DOUBLE = 9, 11, 44
_PF_CLOCK_REALTIME, _PF_CLOCK_BOOTTIME = 1, 6
_PF_TYPE_COUNTER, _PF_SEQ_ID = 4, 1


def build_perfetto_trace(df: pd.DataFrame, selected: list[str]) -> bytes:
    """Convert a signal DataFrame to a Perfetto native trace (.perfetto-trace) binary."""
    selected_set = set(selected)
    key_col = df["signal_source"] + df["channel"].astype(str) + "::" + df["signal_name"]
    sub = df[key_col.isin(selected_set)].sort_values("timestamp_ns")
    if sub.empty:
        return b""

    t_min_ns = int(sub["timestamp_ns"].min())

    # Anchor BOOTTIME=0 to wall clock
    realtime_ns = t_min_ns
    if "event_time" in sub.columns:
        first_ts = sub.loc[sub["timestamp_ns"].idxmin(), "event_time"]
        try:
            ts = pd.Timestamp(first_ts)
            ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
            if isinstance(ts, pd.Timestamp):
                realtime_ns = int(ts.timestamp() * 1e9)
        except Exception:
            pass

    buf = bytearray()

    boot_clk = _pf_u64(_PF_CLK_ID, _PF_CLOCK_BOOTTIME) + _pf_u64(_PF_CLK_TIMESTAMP, 0)
    real_clk = _pf_u64(_PF_CLK_ID, _PF_CLOCK_REALTIME) + _pf_u64(_PF_CLK_TIMESTAMP, realtime_ns)
    snap = _pf_bytes(_PF_SNAP_CLOCKS, boot_clk) + _pf_bytes(_PF_SNAP_CLOCKS, real_clk)
    pkt = _pf_bytes(_PF_PKT_CLOCK_SNAPSHOT, snap) + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
    buf += _pf_packet(pkt)

    track_uuid: dict[str, int] = {}
    for i, key in enumerate(selected, start=1):
        td = _pf_u64(_PF_TD_UUID, i) + _pf_str(_PF_TD_NAME, key) + _pf_bytes(_PF_TD_COUNTER, b"")
        pkt = _pf_bytes(_PF_PKT_TRACK_DESCRIPTOR, td) + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
        buf += _pf_packet(pkt)
        track_uuid[key] = i

    for row in sub.itertuples(index=False):
        key = f"{row.signal_source}{row.channel}::{row.signal_name}"
        uuid = track_uuid.get(key)
        if uuid is None:
            continue
        boot_ns = max(0, int(row.timestamp_ns) - t_min_ns)
        event = (
            _pf_u64(_PF_TE_TYPE, _PF_TYPE_COUNTER)
            + _pf_u64(_PF_TE_TRACK_UUID, uuid)
            + _pf_f64(_PF_TE_DOUBLE, float(row.signal_value))
        )
        pkt = (
            _pf_u64(_PF_PKT_TIMESTAMP, boot_ns)
            + _pf_u64(_PF_PKT_TIMESTAMP_CLOCK_ID, _PF_CLOCK_BOOTTIME)
            + _pf_bytes(_PF_PKT_TRACK_EVENT, event)
            + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
        )
        buf += _pf_packet(pkt)

    return bytes(buf)


# Layout helpers

_BG = "#13111a"
_PANEL = "#1c1a27"
_BORDER = "#2a2838"
_ACCENT = "#7ecfec"
_TEXT = "#ccc"

_AXIS_BOX = {"showline": True, "mirror": True, "linecolor": _BORDER, "linewidth": 1}


def _empty_fig(msg="") -> go.Figure:
    ann = (
        [
            {
                "text": msg,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 16, "color": "#444"},
            }
        ]
        if msg
        else []
    )
    fig = go.Figure(
        layout=go.Layout(
            template="plotly_dark",
            paper_bgcolor=_BG,
            plot_bgcolor=_BG,
            height=600,
            annotations=ann,
        )
    )
    fig.update_xaxes(**_AXIS_BOX)
    fig.update_yaxes(**_AXIS_BOX)
    return fig


_Traces = list[tuple[str, int, str, pd.Series, pd.Series]]

_VAL_WIDTH = 12  # fixed character width for right-aligned signal values in hover tooltip


def _hover_customdata(y: pd.Series) -> "list[list[str]]":
    """Return customdata for right-aligned signal value column (monospace, &nbsp; padded)."""
    return [[f"{v:{_VAL_WIDTH}.4g}".replace(" ", "&nbsp;")] for v in y]


def _overlay_fig(traces: _Traces, height: int = 600) -> go.Figure:
    fig = go.Figure()
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _ in traces), default=0)
    for src, channel, name, x, y in traces:
        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        fig.add_trace(
            go.Scattergl(
                x=x,
                y=y,
                mode="lines+markers",
                line=dict(shape="hv"),
                name=label,
                showlegend=False,
                customdata=_hover_customdata(y),
                hovertemplate=f"{label}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=height,
        hovermode="x unified",
        xaxis_title="Time",
        hoverlabel={"font_family": "monospace"},
        margin={"l": 60, "r": 20, "t": 50, "b": 60},
    )
    fig.update_xaxes(
        **_AXIS_BOX,
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikedash="dot",
        spikecolor="#888",
        spikethickness=1,
    )
    fig.update_yaxes(**_AXIS_BOX)
    return fig


_XaxisMode = Literal["shared", "synced", "free"]


# Intentionally avoids make_subplots: hoversubplots="axis" does not propagate
# the cursor across subplots created by make_subplots (Plotly bug, see
# https://community.plotly.com/t/hoversubplots-axis-not-working-with-make-subplots/84239).
# Instead, each trace gets its own y-axis with a computed domain sharing one x-axis.
def _stacked_fig(traces: _Traces, height: int = 600, xaxis_mode: _XaxisMode = "shared") -> go.Figure:
    n = len(traces)
    spacing = max(0.03, 0.20 / n) if xaxis_mode in ("synced", "free") else max(0.02, 0.20 / n)
    h = (1.0 - spacing * max(n - 1, 0)) / n
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _ in traces), default=0)

    fig = go.Figure()
    axes_kw: dict = {}
    annotations = []

    for i, (src, channel, name, x, y) in enumerate(traces):
        bottom = max(0.0, 1.0 - (i + 1) * h - i * spacing)
        top = min(1.0, 1.0 - i * (h + spacing))
        yref = "y" if i == 0 else f"y{i + 1}"
        ykey = "yaxis" if i == 0 else f"yaxis{i + 1}"

        if xaxis_mode in ("synced", "free"):
            xref = "x" if i == 0 else f"x{i + 1}"
            xkey = "xaxis" if i == 0 else f"xaxis{i + 1}"
            axes_kw[xkey] = {
                **_AXIS_BOX,
                "anchor": yref,
                "showspikes": True,
                "spikemode": "across",
                "spikesnap": "cursor",
                "spikedash": "dot",
                "spikecolor": "#888",
                "spikethickness": 1,
                **({"matches": "x"} if xaxis_mode == "synced" and i > 0 else {}),
                **({"title": "Time"} if i == n - 1 else {}),
            }
        else:
            xref = "x"

        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        fig.add_trace(
            go.Scattergl(
                x=x,
                y=y,
                mode="lines+markers",
                line=dict(shape="hv"),
                name=label,
                xaxis=xref,
                yaxis=yref,
                showlegend=False,
                customdata=_hover_customdata(y),
                hovertemplate=f"{label}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
        axes_kw[ykey] = {"domain": [bottom, top], **_AXIS_BOX}
        annotations.append(
            {
                "text": f"{src}{channel}::{name}",
                "xref": "paper",
                "yref": "paper",
                "x": 0,
                "y": top,
                "xanchor": "left",
                "yanchor": "bottom",
                "showarrow": False,
                "font": {"size": 14, "color": _TEXT},
                "bgcolor": _PANEL,
                "borderpad": 4,
            }
        )

    if xaxis_mode == "shared":
        last_y_ref = "y" if n == 1 else f"y{n}"
        axes_kw["xaxis"] = {
            **_AXIS_BOX,
            "anchor": last_y_ref,
            "title": "Time",
            "showspikes": True,
            "spikemode": "across",
            "spikesnap": "cursor",
            "spikedash": "dot",
            "spikecolor": "#888",
            "spikethickness": 1,
        }

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=max(height, 300 * n),
        hovermode="x unified",
        hoversubplots="axis",
        hoverlabel={"font_family": "monospace"},
        annotations=annotations,
        margin={"l": 60, "r": 20, "t": 60, "b": 60},
        **axes_kw,
    )
    return fig


def _pivot_table(traces: _Traces) -> tuple[list[dict], list[dict]]:
    """Return (row_data, col_defs) for the AgGrid pivot view."""
    parts = [traces[0][3].reset_index(drop=True).astype(str).rename("time")]
    for src, channel, name, _, y in traces:
        parts.append(y.reset_index(drop=True).rename(f"{src}{channel}::{name}"))
    pivot = pd.concat(parts, axis=1)

    signal_cols = [c for c in pivot.columns if c != "time"]
    col_defs = [{"field": "time", "headerName": "Time", "pinned": "left", "filter": True, "minWidth": 160}] + [
        {"field": c, "headerName": c, "type": "numericColumn", "filter": True, "minWidth": 140} for c in signal_cols
    ]
    return pivot.to_dict("records"), col_defs


def _map_fig(lat: pd.Series, lon: pd.Series) -> go.Figure:
    center_lat = float(lat.mean())
    center_lon = float(lon.mean())
    fig = go.Figure(
        go.Scattermapbox(
            lat=lat,
            lon=lon,
            mode="lines+markers",
            marker={"size": 4, "color": _ACCENT},
            line={"width": 1, "color": _ACCENT},
        )
    )
    fig.update_layout(
        paper_bgcolor=_BG,
        mapbox={
            "style": "open-street-map",
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": 10,
        },
        height=500,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
    )
    return fig


# App


def _fmt_s(seconds: float) -> str:
    """Format elapsed seconds as HH:MM:SS."""
    s = int(abs(seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def _section(label: str, *children, **kwargs) -> html.Div:
    return html.Div(
        [dbc.Label(label, size="sm", className="fw-semibold text-secondary mb-0 d-block"), *children], **kwargs
    )


def _genie_panel() -> dbc.Col:
    return dbc.Col(
        width="auto",
        style={
            "width": "340px",
            "backgroundColor": _PANEL,
            "borderLeft": f"1px solid {_BORDER}",
            "display": "flex",
            "flexDirection": "column",
            "height": "100vh",
        },
        children=dbc.Card(
            className="h-100 border-0 rounded-0",
            style={"backgroundColor": _PANEL},
            children=[
                dbc.CardHeader(
                    html.Div(
                        [
                            html.Span("Genie AI", style={"fontWeight": "bold", "color": _ACCENT, "fontSize": "14px"}),
                            dbc.Button(
                                "New",
                                id="genie-new-conv-btn",
                                size="sm",
                                color="secondary",
                                outline=True,
                                style={"fontSize": "11px", "padding": "2px 8px"},
                            ),
                        ],
                        className="d-flex justify-content-between align-items-center",
                    ),
                    style={"backgroundColor": _PANEL, "borderBottom": f"1px solid {_BORDER}", "padding": "8px 12px"},
                ),
                dbc.CardBody(
                    style={
                        "padding": "8px",
                        "display": "flex",
                        "flexDirection": "column",
                        "gap": "8px",
                        "overflowY": "hidden",
                    },
                    children=[
                        html.Div(
                            id="genie-chat-log",
                            className="genie-chat-log",
                            style={
                                "flex": "1",
                                "overflowY": "auto",
                                "display": "flex",
                                "flexDirection": "column",
                                "gap": "6px",
                                "minHeight": "0",
                            },
                        ),
                        dbc.Textarea(
                            id="genie-input",
                            placeholder="シグナル名や現象を自然言語で質問...",
                            style={
                                "fontSize": "12px",
                                "backgroundColor": _BG,
                                "color": _TEXT,
                                "border": f"1px solid {_BORDER}",
                                "resize": "none",
                            },
                            rows=3,
                        ),
                        dbc.Button("Ask", id="genie-ask-btn", color="info", size="sm", className="w-100"),
                        dcc.Interval(id="genie-poll-interval", interval=600, n_intervals=0, disabled=True),
                    ],
                ),
                dbc.CardFooter(
                    html.Div(
                        id="genie-preview-box",
                        style={"display": "none"},
                        children=[
                            html.Span(
                                id="genie-preview-summary",
                                style={"fontSize": "11px", "color": _TEXT, "display": "block", "marginBottom": "6px"},
                            ),
                            dbc.Button(
                                "Apply & Plot", id="genie-apply-btn", color="success", size="sm", className="w-100"
                            ),
                        ],
                    ),
                    style={"backgroundColor": _PANEL, "borderTop": f"1px solid {_BORDER}", "padding": "8px 12px"},
                ),
            ],
        ),
    )


app = dash.Dash(__name__, title="Signal Viewer", external_stylesheets=[dbc.themes.DARKLY])

app.layout = dbc.Container(
    fluid=True,
    className="p-0",
    style={"height": "100vh", "backgroundColor": _BG, "fontFamily": "Inter, system-ui, sans-serif"},
    children=[
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="genie-conversation-store"),
        dcc.Store(id="genie-request-store"),
        dcc.Store(id="genie-preview-store"),
        dcc.Store(id="genie-insight-store"),
        dbc.Toast(
            id="replot-toast",
            header="Re-plot needed",
            icon="warning",
            is_open=False,
            dismissable=True,
            style={"position": "fixed", "bottom": "20px", "right": "20px", "width": "300px", "zIndex": 9999},
        ),
        dbc.Row(
            className="h-100 flex-nowrap g-0",
            children=[
                # Sidebar
                dbc.Col(
                    id="sidebar",
                    width="auto",
                    className="d-flex flex-column overflow-auto",
                    style={
                        "width": "270px",
                        "backgroundColor": _PANEL,
                        "padding": "12px 12px",
                        "gap": "10px",
                        "color": _TEXT,
                        "borderRight": f"1px solid {_BORDER}",
                    },
                    children=[
                        html.H3("Signal Viewer", style={"margin": "0 0 3px", "color": _ACCENT, "fontSize": "16px"}),
                        html.Div(f"{CATALOG}.{SCHEMA}.blf_gold_signals", style={"fontSize": "11px", "color": "#888"}),
                        html.Hr(style={"borderColor": _BORDER, "margin": "0"}),
                        _section(
                            "Source",
                            dbc.Checklist(
                                id="source-filter",
                                options=[
                                    {"label": " CAN", "value": "CAN"},
                                    {"label": " ETH", "value": "ETH"},
                                    {"label": " SOMEIP", "value": "SOMEIP"},
                                ],
                                value=["CAN", "SOMEIP"],
                            ),
                        ),
                        _section(
                            "Channel",
                            dbc.Checklist(
                                id="channel-filter",
                                options=[],
                                value=[],
                                labelStyle={"whiteSpace": "nowrap"},
                                style={
                                    "fontSize": "11px",
                                    "maxHeight": "80px",
                                    "overflowY": "auto",
                                },
                            ),
                        ),
                        dbc.Input(
                            id="signal-search",
                            type="search",
                            placeholder="Filter signals...",
                            size="sm",
                            style={"fontSize": "12px"},
                        ),
                        html.Div(
                            [
                                dbc.Label("Signals", size="sm", className="fw-semibold text-secondary mb-0 me-2"),
                                dbc.ButtonGroup(
                                    [
                                        dbc.Button(
                                            "All",
                                            id="select-all-btn",
                                            size="sm",
                                            color="secondary",
                                            outline=True,
                                            className="py-0",
                                            style={"fontSize": "11px"},
                                        ),
                                        dbc.Button(
                                            "None",
                                            id="clear-all-btn",
                                            size="sm",
                                            color="secondary",
                                            outline=True,
                                            className="py-0",
                                            style={"fontSize": "11px"},
                                        ),
                                    ],
                                    size="sm",
                                ),
                                dbc.Button(
                                    "↻",
                                    id="refresh-signals-btn",
                                    size="sm",
                                    color="secondary",
                                    outline=True,
                                    className="py-0 ms-1",
                                    style={"fontSize": "11px"},
                                    title="Refresh signal list from warehouse",
                                ),
                            ],
                            className="d-flex align-items-center mb-1",
                        ),
                        dcc.Store(id="signal-data-cache"),
                        dcc.Store(id="time-range-store"),
                        dcc.Download(id="dl-perfetto"),
                        dcc.Loading(
                            type="dot",
                            color=_ACCENT,
                            target_components={"all-signals-cache": "data"},
                            children=[
                                dcc.Store(id="all-signals-cache"),
                                dbc.Checklist(
                                    id="signal-select",
                                    options=[],
                                    value=[],
                                    labelStyle={"whiteSpace": "nowrap"},
                                    style={
                                        "fontSize": "11px",
                                        "maxHeight": "260px",
                                        "overflowY": "auto",
                                        "overflowX": "auto",
                                        "border": f"1px solid {_BORDER}",
                                        "borderRadius": "4px",
                                        "padding": "6px 8px",
                                    },
                                ),
                            ],
                        ),
                        _section(
                            "Layout",
                            dbc.RadioItems(
                                id="layout-mode",
                                options=[
                                    {"label": "Overlay", "value": "overlay"},
                                    {"label": "Stacked", "value": "stacked"},
                                ],
                                value="stacked",
                                className="btn-group d-flex",
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-secondary btn-sm text-center flex-fill",
                                labelCheckedClassName="active",
                            ),
                            className="radio-group",
                        ),
                        _section(
                            "X axis (stacked)",
                            dbc.RadioItems(
                                id="xaxis-mode",
                                options=[
                                    {"label": "Shared", "value": "shared"},
                                    {"label": "Synced", "value": "synced"},
                                    {"label": "Free", "value": "free"},
                                ],
                                value="shared",
                                className="btn-group d-flex",
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-secondary btn-sm text-center flex-fill",
                                labelCheckedClassName="active",
                            ),
                            className="radio-group",
                        ),
                        _section(
                            "Chart height (px)",
                            dcc.Slider(
                                id="chart-height",
                                min=300,
                                max=2000,
                                step=100,
                                value=600,
                                marks={300: "300", 600: "600", 1200: "1.2k", 2000: "2k"},
                                tooltip={"placement": "bottom", "always_visible": False},
                            ),
                        ),
                        _section(
                            "Buckets per signal",
                            dcc.Slider(
                                id="max-pts",
                                min=1_000,
                                max=50_000,
                                step=1_000,
                                value=10_000,
                                marks={1_000: "1k", 10_000: "10k", 50_000: "50k"},
                                tooltip={"placement": "bottom", "always_visible": False},
                            ),
                        ),
                        _section(
                            "Time range",
                            dcc.RangeSlider(
                                id="time-range-slider",
                                min=0,
                                max=1,
                                step=1,
                                value=[0, 1],
                                marks={},
                                tooltip={"placement": "bottom", "always_visible": False},
                                disabled=True,
                            ),
                            html.Div(
                                id="time-range-label",
                                style={"fontSize": "11px", "color": "#888", "textAlign": "center", "marginTop": "4px"},
                            ),
                        ),
                        _section(
                            "Map",
                            html.Div(
                                [
                                    dbc.Label(
                                        "Lat",
                                        size="sm",
                                        className="text-secondary mb-0",
                                        style={"minWidth": "28px", "flex": "0"},
                                    ),
                                    dcc.Dropdown(
                                        id="lat-signal",
                                        options=[],
                                        placeholder="latitude signal...",
                                        clearable=True,
                                        style={"fontSize": "12px", "flex": "1"},
                                    ),
                                ],
                                className="d-flex align-items-center gap-2 mb-1",
                            ),
                            html.Div(
                                [
                                    dbc.Label(
                                        "Lon",
                                        size="sm",
                                        className="text-secondary mb-0",
                                        style={"minWidth": "28px", "flex": "0"},
                                    ),
                                    dcc.Dropdown(
                                        id="lon-signal",
                                        options=[],
                                        placeholder="longitude signal...",
                                        clearable=True,
                                        style={"fontSize": "12px", "flex": "1"},
                                    ),
                                ],
                                className="d-flex align-items-center gap-2",
                            ),
                        ),
                        dbc.Button(
                            "Ask Genie",
                            id="genie-toggle-btn",
                            n_clicks=0,
                            color="secondary",
                            outline=True,
                            size="sm",
                            className="w-100",
                            style={"display": "block" if GENIE_SPACE_ID else "none"},
                        ),
                        dbc.Button("Plot", id="plot-btn", n_clicks=0, color="info", className="w-100 fw-bold"),
                        dbc.Button(
                            "Download Perfetto",
                            id="download-perfetto-btn",
                            n_clicks=0,
                            color="secondary",
                            outline=True,
                            size="sm",
                            className="w-100",
                        ),
                        html.Div(id="avail-msg", style={"fontSize": "12px", "color": "#ccc", "minHeight": "16px"}),
                        html.Div(
                            id="plot-msg",
                            style={"fontSize": "12px", "color": "#ccc", "wordBreak": "break-word", "minHeight": "16px"},
                        ),
                    ],
                ),
                # Sidebar toggle strip
                dbc.Col(
                    width="auto",
                    children=dbc.Button(
                        "<",
                        id="sidebar-toggle",
                        size="sm",
                        color="secondary",
                        outline=True,
                        style={"fontSize": "12px", "padding": "4px 5px", "lineHeight": "1"},
                        title="Collapse sidebar",
                    ),
                    style={
                        "display": "flex",
                        "alignItems": "flex-start",
                        "padding": "8px 2px",
                        "backgroundColor": _PANEL,
                        "borderRight": f"1px solid {_BORDER}",
                    },
                ),
                # Chart + table area
                dbc.Col(
                    className="d-flex flex-column",
                    style={"height": "100vh", "overflowY": "auto"},
                    children=[
                        html.Div(
                            id="genie-insight-banner",
                            style={"display": "none"},
                        ),
                        dcc.Loading(
                            type="circle",
                            color=_ACCENT,
                            target_components={"signal-data-cache": "data", "chart": "figure"},
                            children=dcc.Graph(
                                id="chart",
                                config={"displayModeBar": True, "scrollZoom": True},
                                figure=_empty_fig("Select signals and click Plot"),
                            ),
                        ),
                        html.Div(
                            id="map-section",
                            style={"display": "none"},
                            children=dcc.Loading(
                                type="circle",
                                color=_ACCENT,
                                children=dcc.Graph(
                                    id="map-chart",
                                    config={"displayModeBar": True, "scrollZoom": True},
                                    style={"padding": "0 16px 16px"},
                                ),
                            ),
                        ),
                        dag.AgGrid(
                            id="grid",
                            className="ag-theme-alpine-dark",
                            style={
                                "height": "500px",
                                "padding": "0 16px",
                                "flexShrink": "0",
                                "--ag-background-color": _BG,
                                "--ag-odd-row-background-color": _PANEL,
                                "--ag-border-color": _BORDER,
                                "--ag-header-background-color": _PANEL,
                                "--ag-foreground-color": _TEXT,
                                "--ag-header-foreground-color": _TEXT,
                            },
                            columnDefs=[
                                {"field": "signal", "headerName": "Signal", "filter": True, "minWidth": 180},
                                {"field": "time", "headerName": "Time", "filter": True, "minWidth": 160},
                                {
                                    "field": "value",
                                    "headerName": "Value",
                                    "filter": True,
                                    "type": "numericColumn",
                                    "minWidth": 100,
                                },
                            ],
                            rowData=[],
                            defaultColDef={"resizable": True, "sortable": True, "flex": 1},
                            dashGridOptions={
                                "animateRows": False,
                                "pagination": True,
                                "paginationPageSize": 100,
                                "paginationPageSizeSelector": [50, 100, 500],
                            },
                        ),
                    ],
                ),
                # Genie panel (right side, collapsible)
                dbc.Collapse(
                    id="genie-panel-collapse",
                    is_open=False,
                    dimension="width",
                    children=_genie_panel(),
                ),
            ],
        ),
    ],
)


# ---------------------------------------------------------------------------
# Genie helpers
# ---------------------------------------------------------------------------

_RE_SECONDS = re.compile(
    r"(?:between\s+)?(\d+(?:\.\d+)?)\s*s(?:ec(?:ond)?s?)?\s+(?:to|and|-|–)\s+(\d+(?:\.\d+)?)\s*s",
    re.IGNORECASE,
)
_RE_CLOCK = re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s*(?:to|-|–)\s*(\d{1,2}:\d{2}(?::\d{2})?)")
_RE_LAST = re.compile(r"last\s+(\d+(?:\.\d+)?)\s*(second|sec|minute|min)s?", re.IGNORECASE)


def _genie_query(space_id: str, content: str, conversation_id: str | None, user_token: str) -> dict:
    """Run a Genie Space query in an executor thread. Returns result dict."""
    if _LOCAL_DEV:
        _time.sleep(1.5)
        mock_text = (
            f"Based on your query '{content}', I identified signals CAN1::EngineSpeed_rpm "
            "and CAN1::VehicleSpeed_kph between 50s and 150s."
        )
        return {
            "status": "done",
            "text": mock_text,
            "sql_rows": [],
            "conversation_id": conversation_id or "mock-conv-001",
        }

    assert cfg is not None
    try:
        w = WorkspaceClient(host=cfg.host, token=user_token)
        if conversation_id:
            msg = w.genie.create_message_and_wait(space_id, conversation_id, content=content)
        else:
            result = w.genie.start_conversation_and_wait(space_id, content=content)
            msg = result
            conversation_id = str(msg.conversation_id)

        text = ""
        sql_rows: list[dict] = []
        if hasattr(msg, "attachments") and msg.attachments:
            for att in msg.attachments:
                if hasattr(att, "text") and att.text:
                    text += att.text.content or ""
                if hasattr(att, "query") and att.query:
                    try:
                        result_set = w.genie.get_message_query_result_by_attachment(
                            space_id, str(msg.conversation_id), str(msg.id), str(att.id)
                        )
                        if result_set and hasattr(result_set, "statement_response"):
                            sr = result_set.statement_response
                            if sr and sr.result and sr.manifest:
                                cols = [c.name for c in sr.manifest.schema.columns]
                                sql_rows = [dict(zip(cols, row)) for row in (sr.result.data_array or [])]
                    except Exception:
                        pass
        return {"status": "done", "text": text, "sql_rows": sql_rows, "conversation_id": str(msg.conversation_id)}
    except Exception as exc:
        return {"status": "error", "text": str(exc), "sql_rows": [], "conversation_id": conversation_id or ""}


def _extract_signals(text: str, sql_rows: list[dict], all_signals: list[dict]) -> list[str]:
    """Extract signal keys matching known signals from Genie response."""
    all_keys = {f"{r['signal_source']}{r['channel']}::{r['signal_name']}": r for r in all_signals}
    all_names = {
        r["signal_name"].lower(): f"{r['signal_source']}{r['channel']}::{r['signal_name']}" for r in all_signals
    }

    matched: list[str] = []

    # Priority 1: SQL result rows with signal_name column
    if sql_rows:
        for row in sql_rows:
            sn = row.get("signal_name", "")
            src = str(row.get("signal_source", ""))
            ch = str(row.get("channel", ""))
            full_key = f"{src}{ch}::{sn}"
            if full_key in all_keys:
                matched.append(full_key)
            elif sn.lower() in all_names:
                matched.append(all_names[sn.lower()])
        if matched:
            return list(dict.fromkeys(matched))

    # Priority 2: Exact key in text
    for key in all_keys:
        if key in text:
            matched.append(key)
    if matched:
        return list(dict.fromkeys(matched))

    # Priority 3: Case-insensitive signal_name substring
    text_lower = text.lower()
    for name_lower, key in all_names.items():
        if name_lower in text_lower:
            matched.append(key)
    return list(dict.fromkeys(matched))


def _extract_time_range(text: str, sql_rows: list[dict], time_store: dict | None) -> tuple[float, float] | None:
    """Extract time range as (t_lo, t_hi) in timestamp_s units."""
    if not time_store:
        return None
    t_min = float(time_store.get("min", 0))
    t_max = float(time_store.get("max", 0))

    # Priority 1: SQL rows with timestamp columns
    if sql_rows and sql_rows[0]:
        ts_candidates = ["min_timestamp_s", "timestamp_s", "t_min"]
        te_candidates = ["max_timestamp_s", "timestamp_s", "t_max"]
        ts_col = next((c for c in ts_candidates if c in sql_rows[0]), None)
        te_col = next((c for c in te_candidates if c in sql_rows[0]), None)
        if ts_col and te_col:
            try:
                lo = min(float(r[ts_col]) for r in sql_rows if r.get(ts_col) is not None)
                hi = max(float(r[te_col]) for r in sql_rows if r.get(te_col) is not None)
                if lo < hi and t_min <= lo and hi <= t_max + 1:
                    return (lo, hi)
            except (ValueError, TypeError):
                pass

    # Priority 2: "last N seconds/minutes"
    m = _RE_LAST.search(text)
    if m:
        n = float(m.group(1))
        factor = 60.0 if "min" in m.group(2).lower() else 1.0
        duration = n * factor
        return (max(t_min, t_max - duration), t_max)

    # Priority 3: "50s to 100s"
    m = _RE_SECONDS.search(text)
    if m:
        lo = t_min + float(m.group(1))
        hi = t_min + float(m.group(2))
        return (max(t_min, lo), min(t_max, hi))

    # Priority 4: "10:05 to 10:35" clock time
    m = _RE_CLOCK.search(text)
    if m and time_store.get("t0"):
        try:
            t0 = pd.Timestamp(time_store["t0"])

            def _hms_to_s(hms: str) -> float:
                parts = hms.split(":")
                h, mn = int(parts[0]), int(parts[1])
                s = int(parts[2]) if len(parts) > 2 else 0
                return h * 3600 + mn * 60 + s

            t0_abs = t0.hour * 3600 + t0.minute * 60 + t0.second
            lo = t_min + (_hms_to_s(m.group(1)) - t0_abs)
            hi = t_min + (_hms_to_s(m.group(2)) - t0_abs)
            return (max(t_min, lo), min(t_max, hi))
        except Exception:
            pass

    return None


# Callbacks


def _fetch_all_signals() -> list[dict] | None:
    try:
        df = _query(
            f"SELECT DISTINCT signal_name, signal_source, channel FROM {_GOLD_TABLE} ORDER BY signal_source, channel, signal_name"
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


@callback(
    Output("all-signals-cache", "data"),
    Output("time-range-store", "data"),
    Input("url", "pathname"),
)
def prefetch_signals(_):
    return _fetch_all_signals(), _fetch_global_time_range()


@callback(
    Output("all-signals-cache", "data", allow_duplicate=True),
    Output("time-range-store", "data", allow_duplicate=True),
    Input("refresh-signals-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_signal_cache(_):
    return _fetch_all_signals(), _fetch_global_time_range()


app.clientside_callback(
    """
    function(sources, cache) {
        if (!cache || !sources || sources.length === 0) return [[], []];
        var sourceSet = new Set(sources);
        var bySource = cache.filter(function(r) { return sourceSet.has(r.signal_source); });
        var seen = {};
        bySource.forEach(function(r) { seen[r.signal_source + r.channel] = true; });
        function chLabel(k) { return k.startsWith('SOMEIP') ? 'ETH' + k.slice(6) : k; }
        var opts = Object.keys(seen).sort().map(function(k) { return { label: chLabel(k), value: k }; });
        return [opts, opts.map(function(o) { return o.value; })];
    }
    """,
    Output("channel-filter", "options"),
    Output("channel-filter", "value"),
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
)


app.clientside_callback(
    """
    function(sources, cache, search, channels, currentValue) {
        var no_update = window.dash_clientside.no_update;

        if (cache === null || cache === undefined) {
            return [no_update, no_update, "Loading signals...", no_update, no_update];
        }
        if (!sources || sources.length === 0) {
            return [[], [], "No source selected.", [], []];
        }

        var ctx = window.dash_clientside.callback_context;
        var triggeredId = (ctx.triggered && ctx.triggered.length > 0)
            ? ctx.triggered[0].prop_id.split(".")[0]
            : null;
        var searchTriggered = triggeredId === "signal-search";

        var sourceSet = new Set(sources);

        function toOpt(r) {
            var key = r.signal_source + r.channel + "::" + r.signal_name;
            var src = r.signal_source === 'SOMEIP' ? 'ETH' : r.signal_source;
            var label = src + r.channel + "::" + r.signal_name;
            return { label: label, value: key };
        }

        var bySource = cache.filter(function(r) { return sourceSet.has(r.signal_source); });

        var channelSet = (channels && channels.length > 0) ? new Set(channels) : null;
        var byChannel = channelSet
            ? bySource.filter(function(r) { return channelSet.has(r.signal_source + r.channel); })
            : bySource;

        var allOpts = byChannel.map(toOpt);

        var filtered = byChannel;
        if (search) {
            var kw = search.toLowerCase();
            filtered = byChannel.filter(function(r) {
                return r.signal_name.toLowerCase().indexOf(kw) !== -1 ||
                       r.signal_source.toLowerCase().indexOf(kw) !== -1;
            });
        }

        if (filtered.length === 0) {
            var msg = search ? "No signals match." : "No signals found. Has the pipeline run?";
            var valueOut = searchTriggered ? no_update : [];
            return [[], valueOut, msg, [], []];
        }

        var opts = filtered.map(toOpt);
        var suffix = (search && opts.length < allOpts.length) ? " (" + opts.length + " shown)" : "";
        var statusMsg = allOpts.length + " signal(s) available." + suffix;

        if (searchTriggered) {
            return [opts, no_update, statusMsg, allOpts, allOpts];
        }

        var allValid = new Set(allOpts.map(function(o) { return o.value; }));
        var newValue = (currentValue || []).filter(function(v) { return allValid.has(v); });
        return [opts, newValue, statusMsg, allOpts, allOpts];
    }
    """,
    Output("signal-select", "options"),
    Output("signal-select", "value"),
    Output("avail-msg", "children"),
    Output("lat-signal", "options"),
    Output("lon-signal", "options"),
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
    Input("signal-search", "value"),
    Input("channel-filter", "value"),
    State("signal-select", "value"),
)


app.clientside_callback(
    "function(n) { return true; }",
    Output("plot-btn", "disabled", allow_duplicate=True),
    Input("plot-btn", "n_clicks"),
    prevent_initial_call=True,
)


@callback(
    Output("signal-select", "value", allow_duplicate=True),
    Input("select-all-btn", "n_clicks"),
    Input("clear-all-btn", "n_clicks"),
    State("signal-select", "options"),
    prevent_initial_call=True,
)
def toggle_all(select_clicks, clear_clicks, options):
    triggered = dash.ctx.triggered_id
    if triggered == "select-all-btn":
        return [o["value"] for o in options]
    return []


@callback(
    Output("signal-data-cache", "data"),
    Output("time-range-store", "data", allow_duplicate=True),
    Output("plot-msg", "children"),
    Input("plot-btn", "n_clicks"),
    State("source-filter", "value"),
    State("signal-select", "value"),
    State("max-pts", "value"),
    State("time-range-slider", "value"),
    State("time-range-store", "data"),
    State("lat-signal", "value"),
    State("lon-signal", "value"),
    prevent_initial_call=True,
)
def fetch_data(_, sources, selected, max_pts, time_range, time_range_store, lat_key, lon_key):
    if not sources:
        return None, dash.no_update, "No source selected."

    keys = set(selected or [])
    keys.update(k for k in (lat_key, lon_key) if k)
    if not keys:
        return None, dash.no_update, "No signal selected."

    key_triples = [_parse_key(key) for key in keys]
    pair_filter = "(signal_source, channel, signal_name) IN (" + ", ".join(["(?, ?, ?)"] * len(key_triples)) + ")"
    pair_params = [part for triple in key_triples for part in triple]

    time_filter = ""
    time_params: list = []
    if time_range_store is not None and time_range is not None:
        t_lo, t_hi = float(time_range[0]), float(time_range[1])
        time_filter = " AND timestamp_s BETWEEN ? AND ?"
        time_params = [t_lo, t_hi]

    new_time_range: dict | object = dash.no_update
    range_stmt = (
        f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max, MIN(event_time) AS t0 "
        f"FROM {_GOLD_TABLE} WHERE {pair_filter}"
    )
    try:
        range_df = _query(range_stmt, list(pair_params))
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
        print(f"[fetch_data] time-range query error: {exc}", flush=True)

    stmt = (
        f"WITH bucketed AS ("
        f"  SELECT signal_source, channel, signal_name, event_time, timestamp_s, timestamp_ns, signal_value,"
        f"    NTILE({int(max_pts)}) OVER ("
        f"      PARTITION BY signal_source, channel, signal_name ORDER BY timestamp_ns"
        f"    ) AS bucket"
        f"  FROM {_GOLD_TABLE} WHERE {pair_filter}{time_filter}"
        f"), agg AS ("
        f"  SELECT signal_source, channel, signal_name, bucket,"
        f"    MIN_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value), signal_value) AS lo,"
        f"    MAX_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value), signal_value) AS hi"
        f"  FROM bucketed GROUP BY signal_source, channel, signal_name, bucket"
        f") SELECT * FROM ("
        f"  SELECT signal_source, channel, signal_name, lo.event_time AS event_time, lo.timestamp_s AS timestamp_s,"
        f"    lo.timestamp_ns AS timestamp_ns, lo.signal_value AS signal_value FROM agg"
        f"  UNION ALL"
        f"  SELECT signal_source, channel, signal_name, hi.event_time, hi.timestamp_s, hi.timestamp_ns, hi.signal_value FROM agg"
        f") ORDER BY signal_source, channel, signal_name, event_time, timestamp_ns"
    )
    try:
        df = _query(stmt, pair_params + time_params)
    except Exception as exc:
        msg = f"Query error: {exc}"
        print(f"[fetch_data] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None, new_time_range, msg

    signal_count = df["signal_name"].nunique() if not df.empty else 0
    print(f"[fetch_data] {len(df):,} rows, {signal_count} signal(s)", flush=True)
    return (
        _df_to_store(df),
        new_time_range,
        f"Fetched {len(df):,} pts, {signal_count} signal(s).",
    )


@callback(
    Output("chart", "figure"),
    Output("plot-msg", "children", allow_duplicate=True),
    Output("grid", "rowData"),
    Output("grid", "columnDefs"),
    Output("map-chart", "figure"),
    Output("map-section", "style"),
    Output("plot-btn", "disabled", allow_duplicate=True),
    Input("signal-data-cache", "data"),
    Input("signal-select", "value"),
    Input("layout-mode", "value"),
    Input("chart-height", "value"),
    Input("xaxis-mode", "value"),
    State("lat-signal", "value"),
    State("lon-signal", "value"),
    prevent_initial_call=True,
)
def render_chart(cache_data, selected, layout, chart_height, xaxis_mode: _XaxisMode, lat_key, lon_key):
    map_empty = go.Figure()
    map_hidden = {"display": "none"}

    btn_disabled = False if dash.ctx.triggered_id == "signal-data-cache" else dash.no_update

    if cache_data is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, map_empty, map_hidden, btn_disabled

    df_all = _store_to_df(cache_data)

    chart_fig: object = dash.no_update
    chart_msg = ""
    row_data: object = dash.no_update
    col_defs: object = dash.no_update

    if selected:
        traces: _Traces = []
        for key in selected:
            src, channel, name = _parse_key(key)
            sub = df_all[
                (df_all["signal_source"] == src) & (df_all["channel"] == channel) & (df_all["signal_name"] == name)
            ]
            if sub.empty:
                continue
            x = (
                sub["event_time"]
                if "event_time" in df_all.columns and sub["event_time"].notna().any()
                else sub["timestamp_s"]
            )
            traces.append((src, channel, name, x, sub["signal_value"]))

        if traces:
            h = int(chart_height or 600)
            chart_fig = (
                _overlay_fig(traces, h) if layout == "overlay" else _stacked_fig(traces, h, xaxis_mode or "shared")
            )
            row_data, col_defs = _pivot_table(traces)
            total = sum(len(x) for _, _, _, x, _ in traces)
            chart_msg = f"{total:,} pts across {len(traces)} signal(s)."
        else:
            chart_msg = "No data for selected signals."
            chart_fig = _empty_fig(chart_msg)
            row_data = []

    map_fig = map_empty
    map_style = map_hidden
    if lat_key and lon_key and "timestamp_ns" in df_all.columns:
        lat_src, lat_channel, lat_name = _parse_key(lat_key)
        lon_src, lon_channel, lon_name = _parse_key(lon_key)
        lat_df = (
            df_all[
                (df_all["signal_source"] == lat_src)
                & (df_all["channel"] == lat_channel)
                & (df_all["signal_name"] == lat_name)
            ][["timestamp_ns", "signal_value"]]
            .rename(columns={"signal_value": "lat"})
            .sort_values("timestamp_ns")
        )
        lon_df = (
            df_all[
                (df_all["signal_source"] == lon_src)
                & (df_all["channel"] == lon_channel)
                & (df_all["signal_name"] == lon_name)
            ][["timestamp_ns", "signal_value"]]
            .rename(columns={"signal_value": "lon"})
            .sort_values("timestamp_ns")
        )
        if not lat_df.empty and not lon_df.empty:
            merged = pd.merge_asof(lat_df, lon_df, on="timestamp_ns", direction="nearest").dropna(subset=["lat", "lon"])
            if not merged.empty:
                map_fig = _map_fig(merged["lat"], merged["lon"])
                map_style = {}
                pts = len(merged)
                chart_msg = chart_msg + f" Map: {pts:,} GPS pts." if chart_msg else f"Map: {pts:,} GPS pts."

    return chart_fig, chart_msg, row_data, col_defs, map_fig, map_style, btn_disabled


@callback(
    Output("time-range-slider", "min"),
    Output("time-range-slider", "max"),
    Output("time-range-slider", "step"),
    Output("time-range-slider", "marks"),
    Output("time-range-slider", "value"),
    Output("time-range-slider", "disabled"),
    Output("time-range-label", "children", allow_duplicate=True),
    Input("time-range-store", "data"),
    State("time-range-slider", "value"),
    State("time-range-slider", "disabled"),
    prevent_initial_call="initial_duplicate",
)
def update_time_slider(store, current_value, is_disabled):
    if store is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""
    t_min, t_max = store["min"], store["max"]
    duration = max(t_max - t_min, 1.0)
    step = max(0.1, duration / 1000)

    t0_raw = pd.Timestamp(store["t0"]) if store.get("t0") else None
    t0 = t0_raw if isinstance(t0_raw, pd.Timestamp) else None

    def _abs_ts(offset_s: float) -> str:
        if t0 is None:
            return _fmt_s(offset_s - t_min)
        ts = t0 + pd.Timedelta(seconds=offset_s - t_min)
        assert isinstance(ts, pd.Timestamp)
        return ts.strftime("%H:%M:%S")

    marks = {}
    for i in range(6):
        t = t_min + i * duration / 5
        marks[round(t, 3)] = _abs_ts(t)

    # Preserve user selection when within bounds; reset to full range otherwise.
    if not is_disabled and current_value is not None:
        low = max(t_min, float(current_value[0]))
        high = min(t_max, float(current_value[1]))
        if low >= high:
            low, high = t_min, t_max
    else:
        low, high = t_min, t_max

    if t0 is not None:
        lo_dt = t0 + pd.Timedelta(seconds=low - t_min)
        hi_dt = t0 + pd.Timedelta(seconds=high - t_min)
        assert isinstance(lo_dt, pd.Timestamp) and isinstance(hi_dt, pd.Timestamp)
        low_ts = lo_dt.strftime("%H:%M:%S")
        high_ts = hi_dt.strftime("%H:%M:%S")
        label = f"{low_ts} – {high_ts}  (duration {_fmt_s(high - low)})"
    else:
        label = f"{_fmt_s(low - t_min)} – {_fmt_s(high - t_min)}  (total {_fmt_s(duration)})"
    return t_min, t_max, step, marks, [low, high], False, label


app.clientside_callback(
    """
    function(value, store) {
        if (!value || !store) return window.dash_clientside.no_update;
        var lo = value[0], hi = value[1];
        var tMin = store.min;
        var t0 = store.t0 ? new Date(store.t0) : null;

        function fmtHms(totalSec) {
            var s = Math.floor(Math.abs(totalSec));
            return [Math.floor(s / 3600), Math.floor((s % 3600) / 60), s % 60]
                .map(function(v) { return v.toString().padStart(2, '0'); }).join(':');
        }
        function fmtTime(offsetS) {
            if (t0) {
                var dt = new Date(t0.getTime() + (offsetS - tMin) * 1000);
                return [dt.getUTCHours(), dt.getUTCMinutes(), dt.getUTCSeconds()]
                    .map(function(v) { return v.toString().padStart(2, '0'); }).join(':');
            }
            return fmtHms(offsetS - tMin);
        }

        var loStr = fmtTime(lo), hiStr = fmtTime(hi), durStr = fmtHms(hi - lo);
        if (t0) {
            return loStr + ' – ' + hiStr + '  (duration ' + durStr + ')';
        }
        return loStr + ' – ' + hiStr + '  (total ' + fmtHms(store.max - store.min) + ')';
    }
    """,
    Output("time-range-label", "children"),
    Input("time-range-slider", "value"),
    State("time-range-store", "data"),
)


@callback(
    Output("replot-toast", "children"),
    Output("replot-toast", "is_open"),
    Input("signal-select", "value"),
    Input("signal-data-cache", "data"),
)
def update_replot_notice(selected, cache_data):
    if not cache_data or not selected:
        return "", False
    df = _store_to_df(cache_data)
    cached_keys = set(df["signal_source"] + df["channel"].astype(str) + "::" + df["signal_name"])
    new_count = sum(1 for s in selected if s not in cached_keys)
    if new_count == 0:
        return "", False
    return f"{new_count} new signal(s) not yet fetched. Click Plot to update.", True


@callback(
    Output("dl-perfetto", "data"),
    Input("download-perfetto-btn", "n_clicks"),
    State("signal-data-cache", "data"),
    State("signal-select", "value"),
    prevent_initial_call=True,
)
def download_perfetto(_, cache_data, selected):
    if not cache_data or not selected:
        return dash.no_update
    df = _store_to_df(cache_data)
    trace_bytes = build_perfetto_trace(df, selected)
    if not trace_bytes:
        return dash.no_update
    return dcc.send_bytes(trace_bytes, "signals.perfetto-trace")


@callback(
    Output("sidebar", "style"),
    Output("sidebar-toggle", "children"),
    Output("sidebar-toggle", "title"),
    Input("sidebar-toggle", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_sidebar(n_clicks):
    collapsed = (n_clicks or 0) % 2 == 1
    if collapsed:
        return (
            {
                "width": "0",
                "minWidth": "0",
                "overflow": "hidden",
                "padding": "0",
            },
            ">",
            "Expand sidebar",
        )
    return (
        {
            "width": "270px",
            "backgroundColor": _PANEL,
            "padding": "12px 12px",
            "gap": "10px",
            "color": _TEXT,
            "borderRight": f"1px solid {_BORDER}",
        },
        "<",
        "Collapse sidebar",
    )


# ---------------------------------------------------------------------------
# Genie callbacks
# ---------------------------------------------------------------------------


@callback(
    Output("genie-panel-collapse", "is_open"),
    Output("genie-toggle-btn", "children"),
    Input("genie-toggle-btn", "n_clicks"),
    State("genie-panel-collapse", "is_open"),
    prevent_initial_call=True,
)
def toggle_genie_panel(n, is_open):
    new_state = not (is_open or False)
    label = "Close Genie" if new_state else "Ask Genie"
    return new_state, label


@callback(
    Output("genie-conversation-store", "data", allow_duplicate=True),
    Output("genie-chat-log", "children", allow_duplicate=True),
    Input("genie-new-conv-btn", "n_clicks"),
    prevent_initial_call=True,
)
def new_genie_conversation(_):
    return None, []


@callback(
    Output("genie-request-store", "data"),
    Output("genie-poll-interval", "disabled"),
    Output("genie-chat-log", "children"),
    Output("genie-ask-btn", "disabled"),
    Input("genie-ask-btn", "n_clicks"),
    State("genie-input", "value"),
    State("genie-conversation-store", "data"),
    State("genie-chat-log", "children"),
    prevent_initial_call=True,
)
def submit_genie_query(n_clicks, question, conv_store, chat_log):
    if not question or not question.strip():
        return dash.no_update, True, dash.no_update, False
    if not GENIE_SPACE_ID and not _LOCAL_DEV:
        error_bubble = html.Div(
            "GENIE_SPACE_ID is not configured.",
            className="genie-bubble genie-ai",
        )
        return dash.no_update, True, (chat_log or []) + [error_bubble], False

    user_token = flask.request.headers.get("X-Forwarded-Access-Token", "") if not _LOCAL_DEV else ""
    conv_id = (conv_store or {}).get("conversation_id")
    request_id = str(_uuid.uuid4())
    future = _genie_executor.submit(_genie_query, GENIE_SPACE_ID or "mock", question.strip(), conv_id, user_token)
    _genie_futures[request_id] = (future, _time.time())

    user_bubble = html.Div(question.strip(), className="genie-bubble genie-user")
    thinking_bubble = html.Div("Thinking...", id="genie-thinking-bubble", className="genie-bubble genie-ai")
    new_log = (chat_log or []) + [user_bubble, thinking_bubble]
    return {"request_id": request_id, "status": "pending"}, False, new_log, True


@callback(
    Output("genie-request-store", "data", allow_duplicate=True),
    Output("genie-poll-interval", "disabled", allow_duplicate=True),
    Output("genie-conversation-store", "data"),
    Output("genie-preview-store", "data"),
    Output("genie-chat-log", "children", allow_duplicate=True),
    Output("genie-ask-btn", "disabled", allow_duplicate=True),
    Input("genie-poll-interval", "n_intervals"),
    State("genie-request-store", "data"),
    State("genie-conversation-store", "data"),
    State("genie-chat-log", "children"),
    State("all-signals-cache", "data"),
    State("time-range-store", "data"),
    prevent_initial_call=True,
)
def poll_genie_result(n_intervals, req_store, conv_store, chat_log, all_signals_raw, time_store):
    if not req_store or req_store.get("status") != "pending":
        return dash.no_update, True, dash.no_update, dash.no_update, dash.no_update, False

    request_id = req_store["request_id"]
    entry = _genie_futures.get(request_id)
    if entry is None:
        return {"request_id": request_id, "status": "done"}, True, dash.no_update, dash.no_update, dash.no_update, False

    future, created_at = entry
    # Expire after 120 s
    if not future.done() and _time.time() - created_at < 120:
        return dash.no_update, False, dash.no_update, dash.no_update, dash.no_update, True

    if not future.done():
        future.cancel()
        del _genie_futures[request_id]
        timeout_bubble = html.Div("Request timed out.", className="genie-bubble genie-ai")
        log = [b for b in (chat_log or []) if getattr(b, "id", None) != "genie-thinking-bubble"]
        return (
            {"request_id": request_id, "status": "done"},
            True,
            dash.no_update,
            dash.no_update,
            log + [timeout_bubble],
            False,
        )

    result = future.result()
    del _genie_futures[request_id]

    new_conv = {"space_id": GENIE_SPACE_ID, "conversation_id": result["conversation_id"]}

    log = [b for b in (chat_log or []) if getattr(b, "id", None) != "genie-thinking-bubble"]
    response_text = result["text"] or "(no text response)"
    ai_bubble = html.Div(response_text, className="genie-bubble genie-ai")
    log = log + [ai_bubble]

    preview = None
    if result["status"] == "done":
        all_signals = all_signals_raw if isinstance(all_signals_raw, list) else []
        signals = _extract_signals(response_text, result["sql_rows"], all_signals)
        time_range = _extract_time_range(response_text, result["sql_rows"], time_store)
        if signals or time_range:
            preview = {
                "signals": signals,
                "t_lo": time_range[0] if time_range else None,
                "t_hi": time_range[1] if time_range else None,
                "explanation": response_text,
            }

    return (
        {"request_id": request_id, "status": "done"},
        True,
        new_conv,
        preview,
        log,
        False,
    )


app.clientside_callback(
    """
    function(preview) {
        if (!preview || (!preview.signals.length && preview.t_lo === null)) {
            return [{display: "none"}, ""];
        }
        var parts = [];
        if (preview.signals && preview.signals.length) {
            var shown = preview.signals.slice(0, 3).join(", ");
            var extra = preview.signals.length > 3 ? " +" + (preview.signals.length - 3) + " more" : "";
            parts.push(preview.signals.length + " signal(s): " + shown + extra);
        }
        if (preview.t_lo !== null && preview.t_lo !== undefined) {
            parts.push("Time: " + preview.t_lo.toFixed(1) + "s – " + preview.t_hi.toFixed(1) + "s");
        }
        return [{display: "block"}, parts.join(" | ")];
    }
    """,
    Output("genie-preview-box", "style"),
    Output("genie-preview-summary", "children"),
    Input("genie-preview-store", "data"),
)


app.clientside_callback(
    """
    function(insight) {
        if (!insight) {
            return {display: "none"};
        }
        return {
            display: "block",
            padding: "8px 16px",
            backgroundColor: "#1c3a4a",
            color: "#7ecfec",
            fontSize: "12px",
            borderBottom: "1px solid #2a5060",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word"
        };
    }
    """,
    Output("genie-insight-banner", "style"),
    Input("genie-insight-store", "data"),
)


app.clientside_callback(
    """
    function(insight) {
        if (!insight) return "";
        return "Genie: " + insight;
    }
    """,
    Output("genie-insight-banner", "children"),
    Input("genie-insight-store", "data"),
)


@callback(
    Output("signal-select", "value", allow_duplicate=True),
    Output("time-range-slider", "value", allow_duplicate=True),
    Output("plot-btn", "n_clicks", allow_duplicate=True),
    Output("genie-insight-store", "data"),
    Input("genie-apply-btn", "n_clicks"),
    State("genie-preview-store", "data"),
    State("signal-select", "value"),
    State("time-range-slider", "value"),
    State("plot-btn", "n_clicks"),
    prevent_initial_call=True,
)
def apply_genie_preview(n_clicks, preview, current_signals, current_range, plot_n):
    if not preview:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    new_signals = preview["signals"] if preview.get("signals") else current_signals
    new_range = [preview["t_lo"], preview["t_hi"]] if preview.get("t_lo") is not None else current_range
    return new_signals, new_range, (plot_n or 0) + 1, preview.get("explanation")


if __name__ == "__main__":
    app.run(debug=True)
