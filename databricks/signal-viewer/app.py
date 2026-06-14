"""Signal Viewer — Plotly Dash visualization app for Databricks Apps."""

import math
import os
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
        if "DISTINCT" in stmt:
            rows = [{"signal_name": n, "signal_source": s} for s, n in _DUMMY_CATALOG]
            return pd.DataFrame(rows).sort_values(["signal_source", "signal_name"]).reset_index(drop=True)
        if "t_min" in stmt:
            return pd.DataFrame({"t_min": [0.0], "t_max": [_DUMMY_DURATION]})
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
    connect_kwargs = {"access_token": user_token} if user_token else {"credentials_provider": cfg.authenticate}
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        **connect_kwargs,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, params)
            return cur.fetchall_arrow().to_pandas()


def _query(stmt: str, params=None) -> pd.DataFrame:
    """Run a parameterised SQL query using the request's user token or SP credentials."""
    if _LOCAL_DEV:
        return _dummy_query(stmt, params)
    user_token = flask.request.headers.get("X-Forwarded-Access-Token")
    if not user_token:
        raise RuntimeError("Missing X-Forwarded-Access-Token header.")
    return _run_query(stmt, params, user_token=user_token if USE_USER_TOKEN else None)


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
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}"))
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
            go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}", xaxis=xref, yaxis=yref, showlegend=False)
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
                            "Max points / signal",
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


@callback(Output("all-signals-cache", "data"), Input("url", "pathname"))
def prefetch_signals(_):
    return _fetch_all_signals()


@callback(
    Output("all-signals-cache", "data", allow_duplicate=True),
    Input("refresh-signals-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_signal_cache(_):
    return _fetch_all_signals()


@callback(
    Output("signal-select", "options"),
    Output("signal-select", "value"),
    Output("avail-msg", "children"),
    Output("lat-signal", "options"),
    Output("lon-signal", "options"),
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
)
def refresh_signals(sources, cache):
    print(f"[refresh_signals] sources={sources} cache={'hit' if cache is not None else 'miss'}", flush=True)
    if cache is None:
        return dash.no_update, dash.no_update, "Loading signals...", dash.no_update, dash.no_update
    if not sources:
        return [], [], "No source selected.", [], []
    source_set = set(sources)
    filtered = [r for r in cache if r["signal_source"] in source_set]
    if not filtered:
        return [], [], "No signals found. Has the pipeline run?", [], []
    opts = [
        {
            "label": f"[{r['signal_source']}] {r['signal_name']}",
            "value": f"{r['signal_source']}::{r['signal_name']}",
        }
        for r in filtered
    ]
    print(f"[refresh_signals] returning {len(opts)} opts (from cache)", flush=True)
    return opts, [], f"{len(opts)} signal(s) available.", opts, opts


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
    Output("chart", "figure"),
    Output("plot-msg", "children"),
    Output("grid", "rowData"),
    Output("grid", "columnDefs"),
    Output("map-chart", "figure"),
    Output("map-section", "style"),
    Output("time-range-store", "data"),
    Input("plot-btn", "n_clicks"),
    Input("layout-mode", "value"),
    Input("chart-height", "value"),
    State("signal-select", "value"),
    State("max-pts", "value"),
    State("lat-signal", "value"),
    State("lon-signal", "value"),
    State("time-range-slider", "value"),
    State("time-range-store", "data"),
    State("xaxis-mode", "value"),
    prevent_initial_call=True,
)
def render_chart(
    _, layout, chart_height, selected, max_pts, lat_key, lon_key, time_range, time_range_store, xaxis_mode: _XaxisMode
):
    map_empty = go.Figure()
    map_hidden = {"display": "none"}

    if not selected and not (lat_key and lon_key):
        return (
            dash.no_update,
            "Select at least one signal.",
            dash.no_update,
            dash.no_update,
            map_empty,
            map_hidden,
            dash.no_update,
        )

    chart_fig = dash.no_update
    chart_msg = ""
    row_data = dash.no_update
    col_defs = dash.no_update
    new_time_range = dash.no_update

    if selected:
        where_clauses = " OR ".join(["(signal_source = ? AND signal_name = ?)"] * len(selected))
        flat_params = [val for key in selected for val in key.split("::", 1)]

        # Apply time filter when slider has been initialised (store is populated).
        time_filter = ""
        time_params: list = []
        if time_range_store is not None and time_range is not None:
            t_lo, t_hi = float(time_range[0]), float(time_range[1])
            time_filter = " AND timestamp_s BETWEEN ? AND ?"
            time_params = [t_lo, t_hi]

        # Fetch full time extent for selected signals to keep slider bounds accurate.
        range_stmt = (
            f"SELECT MIN(timestamp_s) AS t_min, MAX(timestamp_s) AS t_max FROM {_GOLD_TABLE} WHERE {where_clauses}"
        )
        try:
            range_df = _query(range_stmt, flat_params)
            new_time_range = {
                "min": float(range_df["t_min"].iloc[0]),
                "max": float(range_df["t_max"].iloc[0]),
            }
        except Exception as exc:
            print(f"[render_chart] time-range query error: {exc}", flush=True)

        stmt = (
            f"WITH ranked AS ("
            f"SELECT signal_source, signal_name, event_time, timestamp_s, signal_value,"
            f" ROW_NUMBER() OVER (PARTITION BY signal_source, signal_name ORDER BY timestamp_ns) AS rn"
            f" FROM {_GOLD_TABLE} WHERE {where_clauses}{time_filter}"
            f") SELECT signal_source, signal_name, event_time, timestamp_s, signal_value"
            f" FROM ranked WHERE rn <= {int(max_pts)}"
        )
        flat_params = flat_params + time_params
        try:
            df_all = _query(stmt, flat_params)
        except Exception as exc:
            chart_msg = f"Query error: {exc}"
            print(f"[render_chart] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
            return _empty_fig(chart_msg), chart_msg, [], dash.no_update, map_empty, map_hidden, new_time_range

        traces: _Traces = []
        for key in selected:
            src, name = key.split("::", 1)
            sub = df_all[(df_all["signal_source"] == src) & (df_all["signal_name"] == name)]
            if sub.empty:
                continue
            # Prefer absolute event_time; fall back to relative timestamp_s.
            x = sub["event_time"] if sub["event_time"].notna().any() else sub["timestamp_s"]
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
            chart_msg = "No data returned."
            chart_fig = _empty_fig(chart_msg)
            row_data = []

    if lat_key and lon_key:
        gps_stmt = (
            f"WITH ranked AS ("
            f"SELECT timestamp_ns, signal_value,"
            f" ROW_NUMBER() OVER (ORDER BY timestamp_ns) AS rn"
            f" FROM {_GOLD_TABLE} WHERE signal_source = ? AND signal_name = ?"
            f") SELECT timestamp_ns, signal_value FROM ranked WHERE rn <= {int(max_pts)}"
        )
        try:
            lat_src, lat_name = lat_key.split("::", 1)
            lon_src, lon_name = lon_key.split("::", 1)
            lat_df = (
                _query(gps_stmt, [lat_src, lat_name])
                .rename(columns={"signal_value": "lat"})
                .sort_values("timestamp_ns")
            )
            lon_df = (
                _query(gps_stmt, [lon_src, lon_name])
                .rename(columns={"signal_value": "lon"})
                .sort_values("timestamp_ns")
            )
            merged = pd.merge_asof(lat_df, lon_df, on="timestamp_ns", direction="nearest").dropna(subset=["lat", "lon"])
        except Exception as exc:
            print(f"[render_chart] GPS fetch error: {exc}\n{traceback.format_exc()}", flush=True)
            merged = pd.DataFrame()

        if not merged.empty:
            map_fig = _map_fig(merged["lat"], merged["lon"])
            map_style: dict = {}
            pts = len(merged)
            chart_msg = chart_msg + f" Map: {pts:,} GPS pts." if chart_msg else f"Map: {pts:,} GPS pts."
        else:
            map_fig = map_empty
            map_style = map_hidden
    else:
        map_fig = map_empty
        map_style = map_hidden

    return chart_fig, chart_msg, row_data, col_defs, map_fig, map_style, new_time_range


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

    marks = {}
    for i in range(6):
        t = t_min + i * duration / 5
        marks[round(t, 3)] = _fmt_s(i * duration / 5)

    # Preserve user selection when within bounds; reset to full range otherwise.
    if not is_disabled and current_value is not None:
        low = max(t_min, float(current_value[0]))
        high = min(t_max, float(current_value[1]))
        if low >= high:
            low, high = t_min, t_max
    else:
        low, high = t_min, t_max

    label = f"{_fmt_s(low - t_min)} – {_fmt_s(high - t_min)}  (total {_fmt_s(duration)})"
    return t_min, t_max, step, marks, [low, high], False, label


if __name__ == "__main__":
    app.run(debug=True)
