"""Signal Viewer — Plotly Dash visualization app for Databricks Apps."""

import os
import traceback

import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import flask
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html
from databricks.sdk.core import Config

from databricks import sql

# Ensure environment variable is set correctly
assert os.getenv("DATABRICKS_WAREHOUSE_ID"), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Config
USE_USER_TOKEN = True  # Set to False to use Service Principal credentials instead of user token
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
_GOLD_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"


# Databricks config
cfg = Config()


def _run_query(stmt: str, params: list | dict | None, user_token: str | None = None) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    connect_kwargs = {"access_token": user_token} if user_token else {"credentials_provider": lambda: cfg.authenticate}
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


# Intentionally avoids make_subplots: hoversubplots="axis" does not propagate
# the cursor across subplots created by make_subplots (Plotly bug, see
# https://community.plotly.com/t/hoversubplots-axis-not-working-with-make-subplots/84239).
# Instead, each trace gets its own y-axis with a computed domain sharing one x-axis.
def _stacked_fig(traces: _Traces, height: int = 600) -> go.Figure:
    n = len(traces)
    spacing = max(0.07, 0.20 / n)
    h = (1.0 - spacing * max(n - 1, 0)) / n

    fig = go.Figure()
    axes_kw: dict = {}
    annotations = []

    for i, (src, name, x, y) in enumerate(traces):
        bottom = max(0.0, 1.0 - (i + 1) * h - i * spacing)
        top = min(1.0, 1.0 - i * (h + spacing))
        yref = "y" if i == 0 else f"y{i + 1}"
        ykey = "yaxis" if i == 0 else f"yaxis{i + 1}"

        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}", yaxis=yref, showlegend=False))
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

    last_y_ref = "y" if n == 1 else f"y{n}"

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=max(height, 300 * n),
        hovermode="x unified",
        hoversubplots="axis",
        annotations=annotations,
        margin={"l": 60, "r": 20, "t": 30, "b": 60},
        xaxis={
            **_AXIS_BOX,
            "anchor": last_y_ref,
            "title": "Time",
            "showspikes": True,
            "spikemode": "across",
            "spikesnap": "cursor",
            "spikedash": "dot",
            "spikecolor": "#888",
            "spikethickness": 1,
        },
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


# App


def _section(label: str, *children) -> html.Div:
    return html.Div([dbc.Label(label, size="sm", className="fw-semibold text-secondary mb-1"), *children])


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
                        "width": "290px",
                        "backgroundColor": _PANEL,
                        "padding": "20px 16px",
                        "gap": "16px",
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
                                    {"label": " Overlay", "value": "overlay"},
                                    {"label": " Stacked", "value": "stacked"},
                                ],
                                value="stacked",
                                inline=True,
                            ),
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
                            dag.AgGrid(
                                id="grid",
                                className="ag-theme-alpine-dark",
                                style={
                                    "height": "500px",
                                    "--ag-background-color": _BG,
                                    "--ag-odd-row-background-color": _PANEL,
                                    "--ag-border-color": _BORDER,
                                    "--ag-header-background-color": _PANEL,
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
                            style={"padding": "0 16px", "flexShrink": "0"},
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
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
)
def refresh_signals(sources, cache):
    print(f"[refresh_signals] sources={sources} cache={'hit' if cache is not None else 'miss'}", flush=True)
    if cache is None:
        return dash.no_update, dash.no_update, "Loading signals..."
    if not sources:
        return [], [], "No source selected."
    source_set = set(sources)
    filtered = [r for r in cache if r["signal_source"] in source_set]
    if not filtered:
        return [], [], "No signals found. Has the pipeline run?"
    opts = [
        {
            "label": f"[{r['signal_source']}] {r['signal_name']}",
            "value": f"{r['signal_source']}::{r['signal_name']}",
        }
        for r in filtered
    ]
    print(f"[refresh_signals] returning {len(opts)} opts (from cache)", flush=True)
    return opts, [], f"{len(opts)} signal(s) available."


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
    Input("plot-btn", "n_clicks"),
    Input("layout-mode", "value"),
    Input("chart-height", "value"),
    State("signal-select", "value"),
    State("max-pts", "value"),
    prevent_initial_call=True,
)
def render_chart(_, layout, chart_height, selected, max_pts):
    if not selected:
        return dash.no_update, "Select at least one signal.", dash.no_update, dash.no_update

    where_clauses = " OR ".join(["(signal_source = ? AND signal_name = ?)"] * len(selected))
    flat_params = [val for key in selected for val in key.split("::", 1)]
    stmt = (
        f"WITH ranked AS ("
        f"SELECT signal_source, signal_name, event_time, timestamp_s, signal_value,"
        f" ROW_NUMBER() OVER (PARTITION BY signal_source, signal_name ORDER BY timestamp_ns) AS rn"
        f" FROM {_GOLD_TABLE} WHERE {where_clauses}"
        f") SELECT signal_source, signal_name, event_time, timestamp_s, signal_value"
        f" FROM ranked WHERE rn <= {int(max_pts)}"
    )
    try:
        df_all = _query(stmt, flat_params)
    except Exception as exc:
        msg = f"Query error: {exc}"
        print(f"[render_chart] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return _empty_fig(msg), msg, [], dash.no_update

    traces: _Traces = []
    for key in selected:
        src, name = key.split("::", 1)
        sub = df_all[(df_all["signal_source"] == src) & (df_all["signal_name"] == name)]
        if sub.empty:
            continue
        # Prefer absolute event_time; fall back to relative timestamp_s.
        x = sub["event_time"] if sub["event_time"].notna().any() else sub["timestamp_s"]
        traces.append((src, name, x, sub["signal_value"]))

    if not traces:
        msg = "No data returned."
        return _empty_fig(msg), msg, [], dash.no_update

    h = int(chart_height or 600)
    fig = _overlay_fig(traces, h) if layout == "overlay" else _stacked_fig(traces, h)
    row_data, col_defs = _pivot_table(traces)

    total = sum(len(x) for _, _, x, _ in traces)
    msg = f"{total:,} pts across {len(traces)} signal(s)."
    return fig, msg, row_data, col_defs


if __name__ == "__main__":
    app.run(debug=True)
