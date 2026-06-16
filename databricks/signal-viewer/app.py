"""Signal Viewer — Plotly Dash visualization app for Databricks Apps."""

import io
import math
import os
import struct
import traceback
from typing import Literal

import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import flask
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html
from databricks.sdk.core import Config

from databricks import sql

_LOCAL_DEV = not os.getenv("DATABRICKS_WAREHOUSE_ID")

# Config
USE_USER_TOKEN = True  # Set to False to use Service Principal credentials instead of user token
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
_GOLD_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"


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
        ("CAN", "EngineSpeed_rpm"),
        ("CAN", "VehicleSpeed_kph"),
        ("CAN", "BatteryVoltage_V"),
        ("CAN", "SteeringAngle_deg"),
        ("SOMEIP", "TemperatureSensor_C"),
        ("SOMEIP", "AmbientLight_lux"),
        ("CAN", "GPS_Latitude"),
        ("CAN", "GPS_Longitude"),
    ]

    def _dummy_values(src: str, name: str) -> list[float]:
        if name == "GPS_Latitude":
            return [35.6895 + 0.01 * math.sin(2 * math.pi * t / _DUMMY_DURATION) for t in _DUMMY_T]
        if name == "GPS_Longitude":
            return [139.6917 + 0.01 * math.cos(2 * math.pi * t / _DUMMY_DURATION) for t in _DUMMY_T]
        seed = hash(f"{src}::{name}") & 0xFFFF
        freq = 0.05 + (seed % 20) * 0.01
        amp = 10 + (seed % 90)
        offset = (seed % 100) - 50
        return [offset + amp * math.sin(2 * math.pi * freq * t + seed * 0.001) for t in _DUMMY_T]

    def _dummy_query(stmt: str, params=None) -> pd.DataFrame:
        print(f"[_dummy_query] stmt={stmt!r} params={params!r}", flush=True)
        if "DISTINCT" in stmt:
            rows = [{"signal_name": n, "signal_source": s} for s, n in _DUMMY_CATALOG]
            return pd.DataFrame(rows).sort_values(["signal_source", "signal_name"]).reset_index(drop=True)
        if "t_min" in stmt:
            return pd.DataFrame({"t_min": [0.0], "t_max": [_DUMMY_DURATION], "t0": [_DUMMY_T0]})
        if "PARTITION BY signal_source, signal_name" in stmt:
            rows = []
            for src, name in _DUMMY_CATALOG:
                vals = _dummy_values(src, name)
                for t, ts_ns, v in zip(_DUMMY_T, _DUMMY_TS_NS, vals):
                    rows.append(
                        {
                            "signal_source": src,
                            "signal_name": name,
                            "event_time": _DUMMY_T0 + pd.Timedelta(seconds=t),
                            "timestamp_s": t,
                            "timestamp_ns": ts_ns,
                            "signal_value": v,
                        }
                    )
            return pd.DataFrame(rows)
        # Single-signal GPS query (ORDER BY timestamp_ns, no PARTITION BY)
        src = (params or ["CAN", "GPS_Latitude"])[0]
        name = (params or ["CAN", "GPS_Latitude"])[1]
        vals = _dummy_values(src, name)
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
    key_col = df["signal_source"] + "::" + df["signal_name"]
    sub = df[key_col.isin(selected_set)].sort_values("timestamp_ns")
    if sub.empty:
        return b""

    t_min_ns = int(sub["timestamp_ns"].min())

    # Anchor BOOTTIME=0 to wall clock
    realtime_ns = t_min_ns
    if "event_time" in sub.columns:
        first_ts = sub.loc[sub["timestamp_ns"].idxmin(), "event_time"]
        try:
            ts = pd.Timestamp(first_ts, tz="UTC")
            if not pd.isna(ts):
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
        src, name = key.split("::", 1)
        td = _pf_u64(_PF_TD_UUID, i) + _pf_str(_PF_TD_NAME, f"[{src}] {name}") + _pf_bytes(_PF_TD_COUNTER, b"")
        pkt = _pf_bytes(_PF_PKT_TRACK_DESCRIPTOR, td) + _pf_u64(_PF_PKT_TRUSTED_SEQ_ID, _PF_SEQ_ID)
        buf += _pf_packet(pkt)
        track_uuid[key] = i

    for row in sub.itertuples(index=False):
        key = f"{row.signal_source}::{row.signal_name}"
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


_Traces = list[tuple[str, str, pd.Series, pd.Series]]


def _overlay_fig(traces: _Traces, height: int = 600) -> go.Figure:
    fig = go.Figure()
    for src, name, x, y in traces:
        fig.add_trace(go.Scattergl(x=x, y=y, mode="lines", name=f"[{src}] {name}"))
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=height,
        hovermode="x unified",
        xaxis_title="Time",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
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

    fig = go.Figure()
    axes_kw: dict = {}
    annotations = []

    for i, (src, name, x, y) in enumerate(traces):
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

        fig.add_trace(
            go.Scattergl(x=x, y=y, mode="lines", name=f"[{src}] {name}", xaxis=xref, yaxis=yref, showlegend=False)
        )
        axes_kw[ykey] = {"domain": [bottom, top], **_AXIS_BOX}
        annotations.append(
            {
                "text": f"[{src}] {name}",
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
        annotations=annotations,
        margin={"l": 60, "r": 20, "t": 60, "b": 60},
        **axes_kw,
    )
    return fig


def _pivot_table(traces: _Traces) -> tuple[list[dict], list[dict]]:
    """Return (row_data, col_defs) for the AgGrid pivot view."""
    parts = [traces[0][2].reset_index(drop=True).astype(str).rename("time")]
    for src, name, _, y in traces:
        parts.append(y.reset_index(drop=True).rename(f"[{src}] {name}"))
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


app = dash.Dash(__name__, title="Signal Viewer", external_stylesheets=[dbc.themes.DARKLY])

app.layout = dbc.Container(
    fluid=True,
    className="p-0",
    style={"height": "100vh", "backgroundColor": _BG, "fontFamily": "Inter, system-ui, sans-serif"},
    children=[
        dcc.Location(id="url", refresh=False),
        dbc.Row(
            className="h-100 flex-nowrap g-0",
            children=[
                # Sidebar
                dbc.Col(
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
                        dbc.Input(
                            id="signal-search",
                            type="search",
                            placeholder="Filter signals...",
                            debounce=True,
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
                        dcc.Loading(
                            type="dot",
                            color=_ACCENT,
                            children=[
                                dcc.Store(id="all-signals-cache"),
                                dcc.Store(id="time-range-store"),
                                dcc.Store(id="signal-data-cache"),
                                dcc.Download(id="dl-perfetto"),
                                dbc.Checklist(
                                    id="signal-select",
                                    options=[],
                                    value=[],
                                    style={
                                        "fontSize": "13px",
                                        "maxHeight": "260px",
                                        "overflowY": "auto",
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
                # Chart + table area
                dbc.Col(
                    className="d-flex flex-column",
                    style={"height": "100vh", "overflowY": "auto"},
                    children=[
                        dcc.Loading(
                            type="circle",
                            color=_ACCENT,
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
            ],
        ),
    ],
)


# Callbacks


def _fetch_all_signals() -> list[dict] | None:
    try:
        df = _query(
            f"SELECT DISTINCT signal_name, signal_source FROM {_GOLD_TABLE} ORDER BY signal_source, signal_name"
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
        return {
            "min": float(df["t_min"].iloc[0]),
            "max": float(df["t_max"].iloc[0]),
            "t0": pd.Timestamp(t0_raw).isoformat() if t0_raw is not None else None,
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


@callback(
    Output("signal-select", "options"),
    Output("signal-select", "value"),
    Output("avail-msg", "children"),
    Output("lat-signal", "options"),
    Output("lon-signal", "options"),
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
    Input("signal-search", "value"),
    State("signal-select", "value"),
)
def refresh_signals(sources, cache, search, current_value):
    print(f"[refresh_signals] sources={sources} cache={'hit' if cache is not None else 'miss'}", flush=True)
    if cache is None:
        return dash.no_update, dash.no_update, "Loading signals...", dash.no_update, dash.no_update
    if not sources:
        return [], [], "No source selected.", [], []
    source_set = set(sources)
    filtered = [r for r in cache if r["signal_source"] in source_set]
    if search:
        kw = search.lower()
        filtered = [r for r in filtered if kw in r["signal_name"].lower() or kw in r["signal_source"].lower()]
    if not filtered:
        msg = "No signals match." if search else "No signals found. Has the pipeline run?"
        return [], [], msg, [], []
    opts = [
        {
            "label": f"[{r['signal_source']}] {r['signal_name']}",
            "value": f"{r['signal_source']}::{r['signal_name']}",
        }
        for r in filtered
    ]
    all_opts = [
        {
            "label": f"[{r['signal_source']}] {r['signal_name']}",
            "value": f"{r['signal_source']}::{r['signal_name']}",
        }
        for r in cache
        if r["signal_source"] in source_set
    ]
    all_valid = {o["value"] for o in all_opts}
    new_value = [v for v in (current_value or []) if v in all_valid]
    print(f"[refresh_signals] returning {len(opts)} opts (from cache)", flush=True)
    suffix = f" ({len(opts)} shown)" if search and len(opts) < len(all_opts) else ""
    return opts, new_value, f"{len(all_opts)} signal(s) available.{suffix}", all_opts, all_opts


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
    State("max-pts", "value"),
    State("time-range-slider", "value"),
    State("time-range-store", "data"),
    prevent_initial_call=True,
)
def fetch_data(_, sources, max_pts, time_range, time_range_store):
    if not sources:
        return None, dash.no_update, "No source selected."

    source_placeholders = ", ".join(["?"] * len(sources))
    time_filter = ""
    time_params: list = []
    if time_range_store is not None and time_range is not None:
        t_lo, t_hi = float(time_range[0]), float(time_range[1])
        time_filter = " AND timestamp_s BETWEEN ? AND ?"
        time_params = [t_lo, t_hi]

    new_time_range: dict | object = dash.no_update
    range_stmt = f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max, MIN(event_time) AS t0 FROM {_GOLD_TABLE} WHERE signal_source IN ({source_placeholders})"
    try:
        range_df = _query(range_stmt, list(sources))
        t0_raw = range_df["t0"].iloc[0] if "t0" in range_df.columns else None
        if t0_raw is not None and pd.isna(t0_raw):
            t0_raw = None
        t0_iso = pd.Timestamp(t0_raw).isoformat() if t0_raw is not None else None
        new_time_range = {
            "min": float(range_df["t_min"].iloc[0]),
            "max": float(range_df["t_max"].iloc[0]),
            "t0": t0_iso,
        }
    except Exception as exc:
        print(f"[fetch_data] time-range query error: {exc}", flush=True)

    stmt = (
        f"WITH bucketed AS ("
        f"  SELECT signal_source, signal_name, event_time, timestamp_s, timestamp_ns, signal_value,"
        f"    NTILE({int(max_pts)}) OVER ("
        f"      PARTITION BY signal_source, signal_name ORDER BY timestamp_ns"
        f"    ) AS bucket"
        f"  FROM {_GOLD_TABLE} WHERE signal_source IN ({source_placeholders}){time_filter}"
        f"), agg AS ("
        f"  SELECT signal_source, signal_name, bucket,"
        f"    MIN_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value), signal_value) AS lo,"
        f"    MAX_BY(struct(event_time, timestamp_s, timestamp_ns, signal_value), signal_value) AS hi"
        f"  FROM bucketed GROUP BY signal_source, signal_name, bucket"
        f") SELECT * FROM ("
        f"  SELECT signal_source, signal_name, lo.event_time AS event_time, lo.timestamp_s AS timestamp_s,"
        f"    lo.timestamp_ns AS timestamp_ns, lo.signal_value AS signal_value FROM agg"
        f"  UNION ALL"
        f"  SELECT signal_source, signal_name, hi.event_time, hi.timestamp_s, hi.timestamp_ns, hi.signal_value FROM agg"
        f") ORDER BY signal_source, signal_name, timestamp_ns"
    )
    try:
        df = _query(stmt, list(sources) + time_params)
    except Exception as exc:
        msg = f"Query error: {exc}"
        print(f"[fetch_data] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return None, new_time_range, msg

    signal_count = df["signal_name"].nunique() if not df.empty else 0
    print(f"[fetch_data] {len(df):,} rows, {signal_count} signal(s)", flush=True)
    return (
        df.to_json(orient="records", date_format="iso"),
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

    if cache_data is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, map_empty, map_hidden

    df_all = pd.read_json(io.StringIO(cache_data), orient="records")
    if "event_time" in df_all.columns:
        df_all["event_time"] = pd.to_datetime(df_all["event_time"], utc=True, errors="coerce")

    chart_fig: object = dash.no_update
    chart_msg = ""
    row_data: object = dash.no_update
    col_defs: object = dash.no_update

    if selected:
        traces: _Traces = []
        for key in selected:
            src, name = key.split("::", 1)
            sub = df_all[(df_all["signal_source"] == src) & (df_all["signal_name"] == name)]
            if sub.empty:
                continue
            x = (
                sub["event_time"]
                if "event_time" in df_all.columns and sub["event_time"].notna().any()
                else sub["timestamp_s"]
            )
            traces.append((src, name, x, sub["signal_value"]))

        if traces:
            h = int(chart_height or 600)
            chart_fig = (
                _overlay_fig(traces, h) if layout == "overlay" else _stacked_fig(traces, h, xaxis_mode or "shared")
            )
            row_data, col_defs = _pivot_table(traces)
            total = sum(len(x) for _, _, x, _ in traces)
            chart_msg = f"{total:,} pts across {len(traces)} signal(s)."
        else:
            chart_msg = "No data for selected signals."
            chart_fig = _empty_fig(chart_msg)
            row_data = []

    map_fig = map_empty
    map_style = map_hidden
    if lat_key and lon_key and "timestamp_ns" in df_all.columns:
        lat_src, lat_name = lat_key.split("::", 1)
        lon_src, lon_name = lon_key.split("::", 1)
        lat_df = (
            df_all[(df_all["signal_source"] == lat_src) & (df_all["signal_name"] == lat_name)][
                ["timestamp_ns", "signal_value"]
            ]
            .rename(columns={"signal_value": "lat"})
            .sort_values("timestamp_ns")
        )
        lon_df = (
            df_all[(df_all["signal_source"] == lon_src) & (df_all["signal_name"] == lon_name)][
                ["timestamp_ns", "signal_value"]
            ]
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

    return chart_fig, chart_msg, row_data, col_defs, map_fig, map_style


@callback(
    Output("time-range-slider", "min"),
    Output("time-range-slider", "max"),
    Output("time-range-slider", "step"),
    Output("time-range-slider", "marks"),
    Output("time-range-slider", "value"),
    Output("time-range-slider", "disabled"),
    Output("time-range-label", "children"),
    Input("time-range-store", "data"),
    State("time-range-slider", "value"),
    State("time-range-slider", "disabled"),
)
def update_time_slider(store, current_value, is_disabled):
    if store is None:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""
    t_min, t_max = store["min"], store["max"]
    duration = max(t_max - t_min, 1.0)
    step = max(0.1, duration / 1000)

    t0_raw = pd.Timestamp(store["t0"]) if store.get("t0") else None
    t0 = None if (t0_raw is None or pd.isna(t0_raw)) else t0_raw

    def _abs_ts(offset_s: float) -> str:
        if t0 is None:
            return _fmt_s(offset_s - t_min)
        ts = t0 + pd.Timedelta(seconds=offset_s - t_min)
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
        low_ts = (t0 + pd.Timedelta(seconds=low - t_min)).strftime("%H:%M:%S")
        high_ts = (t0 + pd.Timedelta(seconds=high - t_min)).strftime("%H:%M:%S")
        label = f"{low_ts} – {high_ts}  (duration {_fmt_s(high - low)})"
    else:
        label = f"{_fmt_s(low - t_min)} – {_fmt_s(high - t_min)}  (total {_fmt_s(duration)})"
    return t_min, t_max, step, marks, [low, high], False, label


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
    df = pd.read_json(io.StringIO(cache_data), orient="records")
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    trace_bytes = build_perfetto_trace(df, selected)
    if not trace_bytes:
        return dash.no_update
    return dcc.send_bytes(trace_bytes, "signals.perfetto-trace")


if __name__ == "__main__":
    app.run(debug=True)
