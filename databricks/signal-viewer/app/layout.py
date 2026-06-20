"""Dash layout definition — sets app.layout on import."""

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from ._dash import app
from .config import CATALOG, GENIE_SPACE_ID, SCHEMA
from .figures import _ACCENT, _BG, _BORDER, _PANEL, _TEXT, _empty_fig

# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def _section(label: str, *children, **kwargs) -> html.Div:
    return html.Div(
        [dbc.Label(label, size="sm", className="fw-semibold text-secondary mb-0 d-block"), *children], **kwargs
    )


def _genie_panel() -> dbc.Offcanvas:
    return dbc.Offcanvas(
        id="genie-panel-collapse",
        title=html.Div(
            [
                html.Span("Genie AI", style={"fontWeight": "bold", "color": _ACCENT, "fontSize": "14px", "flex": "1"}),
                dbc.Button(
                    "New",
                    id="genie-new-conv-btn",
                    size="sm",
                    color="secondary",
                    outline=True,
                    style={"fontSize": "11px", "padding": "2px 8px"},
                ),
                dbc.Button(
                    "×",
                    id="genie-close-btn",
                    size="sm",
                    color="danger",
                    outline=True,
                    style={"fontSize": "16px", "padding": "2px 7px", "marginLeft": "4px", "lineHeight": "1"},
                ),
            ],
            style={"display": "flex", "alignItems": "center"},
        ),
        is_open=False,
        placement="end",
        backdrop=False,
        close_button=False,
        style={"width": "340px", "backgroundColor": _PANEL, "color": _TEXT},
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
            html.Div(
                id="genie-preview-box",
                style={"display": "none"},
                children=[
                    html.Span(
                        id="genie-preview-summary",
                        style={"fontSize": "11px", "color": _TEXT, "display": "block", "marginBottom": "6px"},
                    ),
                    dbc.Button("Apply & Plot", id="genie-apply-btn", color="success", size="sm", className="w-100"),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

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
                            style={
                                "fontSize": "12px",
                                "color": "#ccc",
                                "wordBreak": "break-word",
                                "minHeight": "16px",
                            },
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
            ],
        ),
        _genie_panel(),
    ],
)
