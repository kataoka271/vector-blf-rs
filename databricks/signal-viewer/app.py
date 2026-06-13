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
from plotly.subplots import make_subplots

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


def _overlay_fig(traces: _Traces) -> go.Figure:
    fig = go.Figure()
    for src, name, x, y in traces:
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}"))
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=600,
        xaxis_title="Time",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        margin={"l": 60, "r": 20, "t": 50, "b": 60},
    )
    fig.update_xaxes(**_AXIS_BOX)
    fig.update_yaxes(**_AXIS_BOX)
    return fig


def _stacked_fig(traces: _Traces) -> go.Figure:
    n = len(traces)
    fig = make_subplots(
        rows=n,
        cols=1,
        shared_xaxes=True,
        subplot_titles=[f"[{s}] {nm}" for s, nm, _, _ in traces],
        vertical_spacing=max(0.02, 0.10 / max(n, 1)),
    )
    for i, (src, name, x, y) in enumerate(traces, 1):
        fig.add_trace(
            go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}", showlegend=False),
            row=i,
            col=1,
        )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=max(600, 300 * n),
        margin={"l": 60, "r": 20, "t": 30, "b": 60},
    )
    fig.update_xaxes(title_text="Time", row=n, col=1)
    fig.update_xaxes(**_AXIS_BOX)
    fig.update_yaxes(**_AXIS_BOX)
    fig.update_annotations(xanchor="left", x=0, align="left", bgcolor=_PANEL, borderpad=4)
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
                            ],
                            className="d-flex align-items-center mb-1",
                        ),
                        dcc.Loading(
                            type="dot",
                            color=_ACCENT,
                            children=dbc.Checklist(
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
                    className="d-flex flex-column overflow-hidden",
                    style={"height": "100vh"},
                    children=[
                        dcc.Loading(
                            type="circle",
                            color=_ACCENT,
                            style={"flex": "1", "minHeight": "0", "overflow": "hidden"},
                            children=dcc.Graph(
                                id="chart",
                                style={"height": "100%"},
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


@callback(
    Output("signal-select", "options"),
    Output("signal-select", "value"),
    Output("avail-msg", "children"),
    Input("source-filter", "value"),
)
def refresh_signals(sources):
    print(f"[refresh_signals] sources={sources}", flush=True)
    if not sources:
        return [], [], "No source selected."
    try:
        ph = ",".join(["?"] * len(sources))
        df = _query(
            f"SELECT DISTINCT signal_name, signal_source "
            f"FROM {_GOLD_TABLE} "
            f"WHERE signal_source IN ({ph}) "
            f"ORDER BY signal_source, signal_name",
            list(sources),
        )
        print(f"[refresh_signals] df.shape={df.shape} cols={list(df.columns)}", flush=True)
        if df.empty:
            return [], [], "No signals found. Has the pipeline run?"
        opts = [
            {
                "label": f"[{r['signal_source']}] {r['signal_name']}",
                "value": f"{r['signal_source']}::{r['signal_name']}",
            }
            for _, r in df.iterrows()
        ]
        print(f"[refresh_signals] returning {len(opts)} opts", flush=True)
        return opts, [], f"{len(opts)} signal(s) available."
    except Exception as exc:
        print(f"[refresh_signals] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return [], [], f"Error: {exc}"


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
    State("signal-select", "value"),
    State("layout-mode", "value"),
    State("max-pts", "value"),
    prevent_initial_call=True,
)
def render_chart(_, selected, layout, max_pts):
    if not selected:
        return dash.no_update, "Select at least one signal.", dash.no_update, dash.no_update

    traces: _Traces = []
    errors: list[str] = []
    for key in selected:
        src, name = key.split("::", 1)
        try:
            df = _query(
                f"SELECT event_time, timestamp_s, signal_value "
                f"FROM {_GOLD_TABLE} "
                f"WHERE signal_source = ? AND signal_name = ? "
                f"ORDER BY timestamp_ns "
                f"LIMIT {int(max_pts)}",
                [src, name],
            )
            if df.empty:
                continue
            # Prefer absolute event_time; fall back to relative timestamp_s.
            x = df["event_time"] if df["event_time"].notna().any() else df["timestamp_s"]
            traces.append((src, name, x, df["signal_value"]))
        except Exception as exc:
            errors.append(f"[{src}] {name}: {exc}")

    if not traces:
        msg = "No data returned."
        if errors:
            msg += " " + "; ".join(errors)
        return _empty_fig(msg), msg, [], dash.no_update

    fig = _overlay_fig(traces) if layout == "overlay" else _stacked_fig(traces)
    row_data, col_defs = _pivot_table(traces)

    total = sum(len(x) for _, _, x, _ in traces)
    msg = f"{total:,} pts across {len(traces)} signal(s)."
    if errors:
        msg += " Errors: " + "; ".join(errors)
    return fig, msg, row_data, col_defs


if __name__ == "__main__":
    app.run(debug=True)
