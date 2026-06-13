"""BLF Signal Viewer — Plotly Dash visualization app for Databricks Apps."""

import os

import dash
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html
from plotly.subplots import make_subplots

# ── config ────────────────────────────────────────────────────────────────────

WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
CATALOG = os.environ.get("BLF_CATALOG", "main")
SCHEMA = os.environ.get("BLF_SCHEMA", "blf")
PORT = int(os.environ.get("DATABRICKS_APP_PORT", 8000))
_GOLD = f"`{CATALOG}`.`{SCHEMA}`.`blf_gold_signals`"

# ── database (SDK statement execution — no sql connector needed) ──────────────

_w = None


def _workspace():
    global _w
    if _w is None:
        from databricks.sdk import WorkspaceClient

        _w = WorkspaceClient()
    return _w


def _query(stmt: str, params=None) -> pd.DataFrame:
    """Run a SQL statement via the SDK Statement Execution API.

    %s placeholders in stmt are replaced with named :p0, :p1, ...
    parameters so that the warehouse handles quoting safely.
    """
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    sdk_params = None
    if params:
        sdk_params = []
        for i, p in enumerate(params):
            stmt = stmt.replace("%s", f":p{i}", 1)
            sdk_params.append(StatementParameterListItem(name=f"p{i}", value=str(p), type="STRING"))

    w = _workspace()
    result = w.statement_execution.execute_statement(
        statement=stmt,
        warehouse_id=WAREHOUSE_ID,
        parameters=sdk_params,
        wait_timeout="50s",
    )

    from databricks.sdk.service.sql import StatementState

    if result.status.state != StatementState.SUCCEEDED:
        err = result.status.error
        msg = f"{err.error_code}: {err.message}" if err else "Query failed"
        raise Exception(msg)

    if not result.manifest or not result.result:
        return pd.DataFrame()

    cols = [c.name for c in result.manifest.schema.columns]
    rows = result.result.data_array or []

    # Fetch additional chunks if the result was paginated.
    chunk_index = result.result.next_chunk_index
    while chunk_index is not None:
        chunk = w.statement_execution.get_statement_result_chunk_n(result.statement_id, chunk_index)
        if chunk.data_array:
            rows = rows + chunk.data_array
        chunk_index = chunk.next_chunk_index

    return pd.DataFrame([[v for v in row] for row in rows], columns=cols)


# ── layout helpers ────────────────────────────────────────────────────────────

_BG = "#13111a"
_PANEL = "#1c1a27"
_BORDER = "#2a2838"
_ACCENT = "#7ecfec"
_TEXT = "#ccc"


def _label(text):
    return html.Div(
        text,
        style={"fontSize": "12px", "fontWeight": "600", "color": "#aaa", "marginBottom": "4px"},
    )


def _field(label, component):
    return html.Div([_label(label), component])


def _empty_fig(msg=""):
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
    return go.Figure(
        layout=go.Layout(
            template="plotly_dark",
            paper_bgcolor=_BG,
            plot_bgcolor=_BG,
            annotations=ann,
        )
    )


# ── app ───────────────────────────────────────────────────────────────────────

app = dash.Dash(__name__, title="BLF Signal Viewer")
server = app.server

app.layout = html.Div(
    style={
        "display": "flex",
        "height": "100vh",
        "backgroundColor": _BG,
        "fontFamily": "Inter, system-ui, sans-serif",
    },
    children=[
        # ── Sidebar ───────────────────────────────────────────────────────────
        html.Div(
            style={
                "width": "290px",
                "flexShrink": "0",
                "backgroundColor": _PANEL,
                "padding": "20px 16px",
                "display": "flex",
                "flexDirection": "column",
                "gap": "16px",
                "overflowY": "auto",
                "color": _TEXT,
                "borderRight": f"1px solid {_BORDER}",
            },
            children=[
                html.Div(
                    [
                        html.H3(
                            "BLF Signal Viewer",
                            style={"margin": "0 0 3px", "color": _ACCENT, "fontSize": "16px"},
                        ),
                        html.Div(
                            f"{CATALOG}.{SCHEMA}.blf_gold_signals",
                            style={"fontSize": "11px", "color": "#888"},
                        ),
                    ]
                ),
                html.Hr(style={"borderColor": _BORDER, "margin": "0"}),
                _field(
                    "Source",
                    dcc.Checklist(
                        id="source-filter",
                        options=[
                            {"label": " CAN", "value": "CAN"},
                            {"label": " ETH", "value": "ETH"},
                            {"label": " SOMEIP", "value": "SOMEIP"},
                        ],
                        value=["CAN", "SOMEIP"],
                        inputStyle={"marginRight": "6px"},
                        labelStyle={"display": "block", "marginBottom": "4px"},
                    ),
                ),
                _field(
                    "Signals",
                    dcc.Dropdown(
                        id="signal-select",
                        multi=True,
                        placeholder="Select signals...",
                        style={"fontSize": "13px"},
                    ),
                ),
                _field(
                    "Layout",
                    dcc.RadioItems(
                        id="layout-mode",
                        options=[
                            {"label": " Overlay", "value": "overlay"},
                            {"label": " Stacked", "value": "stacked"},
                        ],
                        value="stacked",
                        inputStyle={"marginRight": "6px"},
                        labelStyle={"display": "inline-block", "marginRight": "14px"},
                    ),
                ),
                _field(
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
                html.Button(
                    "Plot",
                    id="plot-btn",
                    n_clicks=0,
                    style={
                        "padding": "10px 0",
                        "width": "100%",
                        "background": _ACCENT,
                        "color": "#000",
                        "border": "none",
                        "borderRadius": "6px",
                        "cursor": "pointer",
                        "fontWeight": "700",
                        "fontSize": "14px",
                    },
                ),
                # Status messages — white text so they're always readable.
                html.Div(id="avail-msg", style={"fontSize": "12px", "color": "#ccc", "minHeight": "16px"}),
                html.Div(
                    id="plot-msg",
                    style={"fontSize": "12px", "color": "#ccc", "wordBreak": "break-word", "minHeight": "16px"},
                ),
            ],
        ),
        # ── Chart area ────────────────────────────────────────────────────────
        html.Div(
            style={"flex": "1", "minWidth": "0", "overflow": "hidden"},
            children=[
                dcc.Loading(
                    type="circle",
                    color=_ACCENT,
                    children=dcc.Graph(
                        id="chart",
                        style={"height": "100%"},
                        config={"displayModeBar": True, "scrollZoom": True},
                        figure=_empty_fig("Select signals and click Plot"),
                    ),
                ),
            ],
        ),
    ],
)


# ── callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("signal-select", "options"),
    Output("avail-msg", "children"),
    Input("source-filter", "value"),
)
def refresh_signals(sources):
    print(f"[refresh_signals] sources={sources}", flush=True)
    if not sources:
        return [], "No source selected."
    try:
        ph = ",".join(["%s"] * len(sources))
        df = _query(
            f"SELECT DISTINCT signal_name, signal_source "
            f"FROM {_GOLD} "
            f"WHERE signal_source IN ({ph}) "
            f"ORDER BY signal_source, signal_name",
            list(sources),
        )
        print(f"[refresh_signals] df.shape={df.shape} cols={list(df.columns)}", flush=True)
        if df.empty:
            return [], "No signals found. Has the pipeline run?"
        opts = [
            {
                "label": f"[{r['signal_source']}] {r['signal_name']}",
                "value": f"{r['signal_source']}::{r['signal_name']}",
            }
            for _, r in df.iterrows()
        ]
        print(f"[refresh_signals] returning {len(opts)} opts", flush=True)
        return opts, f"{len(opts)} signal(s) available."
    except Exception as exc:
        import traceback

        print(f"[refresh_signals] ERROR: {exc}\n{traceback.format_exc()}", flush=True)
        return [], f"Error: {exc}"


@callback(
    Output("chart", "figure"),
    Output("plot-msg", "children"),
    Input("plot-btn", "n_clicks"),
    State("signal-select", "value"),
    State("layout-mode", "value"),
    State("max-pts", "value"),
    prevent_initial_call=True,
)
def render_chart(_, selected, layout, max_pts):
    if not selected:
        return dash.no_update, "Select at least one signal."

    traces, errors = [], []
    for key in selected:
        src, name = key.split("::", 1)
        try:
            df = _query(
                f"SELECT event_time, timestamp_s, signal_value "
                f"FROM {_GOLD} "
                f"WHERE signal_source = %s AND signal_name = %s "
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
        return _empty_fig(msg), msg

    if layout == "overlay":
        fig = go.Figure()
        for src, name, x, y in traces:
            fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=f"[{src}] {name}"))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=_BG,
            plot_bgcolor=_BG,
            xaxis_title="Time",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            margin={"l": 60, "r": 20, "t": 50, "b": 60},
        )
    else:
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
            height=max(500, 220 * n),
            margin={"l": 60, "r": 20, "t": 30, "b": 60},
        )
        fig.update_xaxes(title_text="Time", row=n, col=1)

    total = sum(len(x) for _, _, x, _ in traces)
    msg = f"{total:,} pts across {len(traces)} signal(s)."
    if errors:
        msg += " Errors: " + "; ".join(errors)
    return fig, msg


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
