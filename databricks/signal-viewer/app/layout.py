"""Dash layout definition — sets app.layout on import."""

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from ._dash import app
from .config import CATALOG, GENIE_SPACE_ID, SCHEMA
from .figures import _ACCENT, _BG, _BORDER, _PANEL, _SIDEBAR_CONTENT_STYLE, _SIDEBAR_STYLE, _TEXT, _empty_fig

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
            html.Div(
                id="genie-history-panel",
                style={"display": "none", "flexShrink": "0"},
                children=[
                    html.Hr(style={"borderColor": _BORDER, "margin": "0"}),
                    html.Div(
                        [
                            html.Span(
                                "History",
                                style={"fontSize": "12px", "color": "#888", "flex": "1"},
                            ),
                            html.Span(
                                id="genie-history-count",
                                style={"fontSize": "11px", "color": "#666"},
                            ),
                            html.Span(
                                id="genie-history-arrow",
                                children="▼",
                                style={"fontSize": "10px", "color": "#666"},
                            ),
                        ],
                        id="genie-history-header",
                        n_clicks=0,
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "cursor": "pointer",
                            "padding": "4px 0",
                            "userSelect": "none",
                            "gap": "6px",
                        },
                    ),
                    dbc.Collapse(
                        id="genie-history-collapse",
                        is_open=False,
                        children=dbc.Accordion(
                            id="genie-history-accordion",
                            start_collapsed=True,
                            flush=True,
                            always_open=False,
                            children=[],
                            className="genie-history-accordion",
                        ),
                    ),
                ],
            ),
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
            dbc.Textarea(
                id="genie-input",
                placeholder="Ask about signals or anomalies...",
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
        dcc.Store(id="genie-anomaly-markers-store"),
        dcc.Store(id="genie-flagged-signals-store"),
        dcc.Store(id="genie-history-store"),
        dcc.Store(id="all-channels-cache"),
        dcc.Store(id="signal-data-cache"),
        dcc.Store(id="time-range-store"),
        dcc.Store(id="filenames-cache"),
        dcc.Store(id="filename-pending-store"),
        dcc.Store(id="signal-search-cache"),
        dcc.Interval(id="filename-debounce-interval", interval=600, n_intervals=0, disabled=True),
        dcc.Download(id="dl-perfetto"),
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
                    className="d-flex flex-column",
                    style=_SIDEBAR_STYLE,
                    children=[
                        html.Div(
                            id="sidebar-content",
                            className="d-flex flex-column",
                            style=_SIDEBAR_CONTENT_STYLE,
                            children=[
                                html.Div(
                                    className="d-flex flex-column",
                                    style={"flex": "1", "overflowY": "auto", "gap": "10px", "paddingBottom": "6px"},
                                    children=[
                                        html.Div(
                                            [
                                                html.H3(
                                                    "Signal Viewer",
                                                    style={
                                                        "margin": "0",
                                                        "color": _ACCENT,
                                                        "fontSize": "16px",
                                                        "flex": "1",
                                                    },
                                                ),
                                                dbc.Button(
                                                    "<",
                                                    id="sidebar-toggle",
                                                    size="sm",
                                                    color="secondary",
                                                    outline=True,
                                                    style={"fontSize": "12px", "padding": "4px 5px", "lineHeight": "1"},
                                                    title="Collapse sidebar",
                                                ),
                                            ],
                                            style={"display": "flex", "alignItems": "center"},
                                        ),
                                        html.Div(
                                            f"{CATALOG}.{SCHEMA}.blf_gold_signals",
                                            style={"fontSize": "11px", "color": "#888"},
                                        ),
                                        html.Hr(style={"borderColor": _BORDER, "margin": "0"}),
                                        _section(
                                            "File",
                                            dcc.Dropdown(
                                                id="filename-filter",
                                                options=[],
                                                value=[],
                                                multi=True,
                                                placeholder="All files...",
                                                clearable=True,
                                                style={"fontSize": "11px"},
                                            ),
                                        ),
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
                                            debounce=True,
                                            style={"fontSize": "12px"},
                                        ),
                                        html.Div(
                                            [
                                                dbc.Label(
                                                    "Signals",
                                                    size="sm",
                                                    className="fw-semibold text-secondary mb-0 me-2",
                                                ),
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
                                            "Y scale (overlay)",
                                            dbc.RadioItems(
                                                id="overlay-mode",
                                                options=[
                                                    {"label": "Normalized", "value": "normalized"},
                                                    {"label": "Nominal", "value": "nominal"},
                                                ],
                                                value="normalized",
                                                className="btn-group d-flex",
                                                inputClassName="btn-check",
                                                labelClassName="btn btn-outline-secondary btn-sm text-center flex-fill",
                                                labelCheckedClassName="active",
                                            ),
                                            id="overlay-section",
                                            className="radio-group",
                                            style={"display": "none"},
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
                                            id="xaxis-section",
                                            className="radio-group",
                                        ),
                                        _section(
                                            "Chart height (px)",
                                            dcc.Slider(
                                                id="chart-height",
                                                min=100,
                                                max=800,
                                                step=100,
                                                value=200,
                                                marks={100: "100", 200: "200", 400: "400", 800: "800"},
                                                tooltip={"placement": "bottom", "always_visible": True},
                                            ),
                                        ),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        dbc.Label(
                                                            "Buckets per signal",
                                                            size="sm",
                                                            className="fw-semibold text-secondary mb-0",
                                                        ),
                                                        html.Span(
                                                            "?",
                                                            id="buckets-help-icon",
                                                            style={
                                                                "cursor": "pointer",
                                                                "fontSize": "10px",
                                                                "color": "#888",
                                                                "border": "1px solid #888",
                                                                "borderRadius": "50%",
                                                                "width": "14px",
                                                                "height": "14px",
                                                                "display": "inline-flex",
                                                                "alignItems": "center",
                                                                "justifyContent": "center",
                                                                "marginLeft": "5px",
                                                                "verticalAlign": "middle",
                                                                "lineHeight": "1",
                                                                "flexShrink": "0",
                                                            },
                                                        ),
                                                        dbc.Tooltip(
                                                            "Downsampling resolution. Data points are divided into N buckets; "
                                                            "the min and max value within each bucket are plotted, preserving spikes. "
                                                            "Higher values show more detail but increase query time.",
                                                            target="buckets-help-icon",
                                                            placement="right",
                                                        ),
                                                    ],
                                                    className="d-flex align-items-center mb-0",
                                                ),
                                                dcc.Slider(
                                                    id="max-pts",
                                                    min=1_000,
                                                    max=50_000,
                                                    step=1_000,
                                                    value=10_000,
                                                    marks={1_000: "1k", 10_000: "10k", 50_000: "50k"},
                                                    tooltip={"placement": "bottom", "always_visible": False},
                                                ),
                                            ]
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
                                                tooltip={
                                                    "placement": "bottom",
                                                    "always_visible": False,
                                                    "transform": "_fmtSliderTime",
                                                },
                                                disabled=True,
                                            ),
                                            html.Div(
                                                id="time-range-label",
                                                style={
                                                    "fontSize": "11px",
                                                    "color": "#888",
                                                    "textAlign": "center",
                                                    "marginTop": "4px",
                                                },
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
                                            "Download Perfetto",
                                            id="download-perfetto-btn",
                                            n_clicks=0,
                                            color="secondary",
                                            outline=True,
                                            size="sm",
                                            className="w-100",
                                            disabled=True,
                                        ),
                                    ],
                                ),
                                html.Div(
                                    className="d-flex flex-column",
                                    style={
                                        "gap": "6px",
                                        "paddingTop": "10px",
                                        "borderTop": f"1px solid {_BORDER}",
                                    },
                                    children=[
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
                                        dbc.Button(
                                            "Plot", id="plot-btn", n_clicks=0, color="info", className="w-100 fw-bold"
                                        ),
                                        html.Div(
                                            id="avail-msg",
                                            style={"fontSize": "12px", "color": "#ccc", "minHeight": "16px"},
                                        ),
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
                            ],
                        ),
                    ],
                ),
                # Sidebar expand strip (visible only when sidebar is collapsed)
                dbc.Col(
                    id="sidebar-expand-strip",
                    width="auto",
                    children=dbc.Button(
                        ">",
                        id="sidebar-expand-btn",
                        size="sm",
                        color="secondary",
                        outline=True,
                        style={"fontSize": "12px", "padding": "4px 5px", "lineHeight": "1"},
                        title="Expand sidebar",
                    ),
                    style={
                        "display": "none",
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
                            children=[
                                html.Span(
                                    id="genie-insight-text",
                                    style={"flex": "1", "whiteSpace": "pre-wrap", "wordBreak": "break-word"},
                                ),
                                html.Button(
                                    "×",
                                    id="genie-insight-close-btn",
                                    n_clicks=0,
                                    style={
                                        "background": "none",
                                        "border": "none",
                                        "color": "#7ecfec",
                                        "cursor": "pointer",
                                        "fontSize": "16px",
                                        "lineHeight": "1",
                                        "padding": "0 4px",
                                        "flexShrink": "0",
                                        "alignSelf": "flex-start",
                                    },
                                ),
                            ],
                        ),
                        html.Div(
                            id="signal-tags",
                            style={
                                "display": "flex",
                                "flexWrap": "wrap",
                                "gap": "6px",
                                "padding": "6px 16px",
                                "flexShrink": "0",
                            },
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
