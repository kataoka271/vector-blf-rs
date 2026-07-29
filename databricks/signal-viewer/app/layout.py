"""Dash layout definition — sets app.layout on import.

The tree is assembled bottom-up from small builders so no single expression
nests more than a couple of levels deep:

    app.layout
    +- _stores()                 hidden state: dcc.Store / Interval / sinks
    +- _sidebar()                controls column
    |  +- _sidebar_header()
    |  +- _source_filters()      file tree / source / channel
    |  +- _signal_picker()       search box + checklist
    |  +- _display_options()     layout, scales, sliders, map signals
    |  +- _sidebar_footer()      Genie / Plot buttons + status text
    +- _sidebar_expand_strip()
    +- _main_area()              chart, floating panels, data grid
    +- _genie_panel()            offcanvas
"""

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from ._dash import app
from .config import CATALOG, GENIE_SPACE_ID, SCHEMA
from .figures import _ACCENT, _BG, _BORDER, _PANEL, _SIDEBAR_CONTENT_STYLE, _SIDEBAR_STYLE, _TEXT, _WARN, _empty_fig

# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

_SEGMENTED_RADIO = {
    "className": "btn-group d-flex",
    "inputClassName": "btn-check",
    "labelClassName": "btn btn-outline-secondary btn-sm text-center flex-fill",
    "labelCheckedClassName": "active",
}

_SMALL_OUTLINE_BTN = {"size": "sm", "color": "secondary", "outline": True}


def _section(label: str, *children, **kwargs) -> html.Div:
    return html.Div(
        [dbc.Label(label, size="sm", className="fw-semibold text-secondary mb-0 d-block"), *children], **kwargs
    )


def _signal_dropdown(label: str, dropdown_id: str, placeholder: str, className: str) -> html.Div:
    """One labelled dropdown row used by the Map lat/lon selectors."""
    return html.Div(
        className=className,
        children=[
            dbc.Label(label, size="sm", className="text-secondary mb-0", style={"minWidth": "28px", "flex": "0"}),
            dcc.Dropdown(
                id=dropdown_id,
                options=[],
                placeholder=placeholder,
                clearable=True,
                style={"fontSize": "12px", "flex": "1"},
            ),
        ],
    )


def _float_panel(kind: str, title: str, *body_children) -> html.Div:
    """Dockable/floating panel shell shared by the map and video sections.

    Ids follow the `<kind>-section` / `<kind>-float-*` convention that
    float-panel.js and the callbacks key off.
    """
    return html.Div(
        id=f"{kind}-section",
        className=f"float-panel {kind}-panel docked",
        style={"display": "none"},  # Dash/Python only ever touches "display" here
        children=[
            html.Div(
                id=f"{kind}-float-header",
                className="float-header",
                children=[
                    html.Span(title, className="float-title"),
                    html.Span(
                        "⇱",
                        id=f"{kind}-float-dock-btn",
                        className="float-dock-btn",
                        title="Switch to floating mode",
                    ),
                    html.Span("-", id=f"{kind}-float-minimize-btn", className="float-minimize-btn"),
                ],
            ),
            html.Div(id=f"{kind}-float-body", className="float-body", children=list(body_children)),
        ],
    )


# ---------------------------------------------------------------------------
# Hidden state: stores, intervals, clientside sinks
# ---------------------------------------------------------------------------


def _stores() -> list:
    return [
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="genie-conversation-store"),
        dcc.Store(id="genie-request-store"),
        dcc.Store(id="genie-preview-store"),
        dcc.Store(id="genie-insight-store"),
        dcc.Store(id="genie-anomaly-markers-store"),
        dcc.Store(id="genie-flagged-signals-store"),
        dcc.Store(id="genie-history-store"),
        dcc.Store(id="all-channels-cache"),
        dcc.Store(id="latlon-candidates-cache"),
        dcc.Store(id="signal-data-cache"),
        dcc.Store(id="session-id-store"),  # opaque per-tab id keying the server-side fetched-data cache
        dcc.Store(id="time-range-store"),
        dcc.Store(id="filenames-cache"),
        dcc.Store(id="filename-pending-store"),
        # {segments: [{file, src, t_min, t_max, t0}, ...]}, one segment per selected file
        # that has a matching video, ordered by start time -- see update_video_panel and
        # window._videoSync.playlist in video-sync.js for continuous multi-file playback.
        dcc.Store(id="video-playlist-store"),
        # {t: [...], lat: [...], lon: [...]} GPS track for the video-synced map marker;
        # t entries match the chart's x-axis domain (event_time ISO strings, or
        # numeric timestamp_s) so they're directly comparable to the cursor xValue
        # computed in video-sync.js.
        dcc.Store(id="gps-track-store"),
        html.Div(id="video-cursor-sink", style={"display": "none"}),
        html.Div(id="video-listener-sink", style={"display": "none"}),
        html.Div(id="video-playlist-sink", style={"display": "none"}),
        html.Div(id="color-mode-sink", style={"display": "none"}),
        # max_intervals=1 caps this at exactly one fire per debounce cycle -- without
        # it, a slow SQL round-trip (filter_signals_by_file re-disabling this after
        # the query returns) leaves the 600ms tick free to fire again in the
        # meantime, re-querying blf_gold_signals/blf_signal_catalog repeatedly until
        # the response finally lands. The clientside callback below resets
        # n_intervals to 0 on every filename-filter change so it can fire again.
        dcc.Interval(id="filename-debounce-interval", interval=600, n_intervals=0, max_intervals=1, disabled=True),
        dcc.Interval(id="video-cursor-interval", interval=100, n_intervals=0, disabled=True),
        dcc.Download(id="dl-perfetto"),
        dbc.Toast(
            id="replot-toast",
            header="Re-plot needed",
            icon="warning",
            is_open=False,
            dismissable=True,
            style={"position": "fixed", "bottom": "20px", "right": "20px", "width": "300px", "zIndex": 9999},
        ),
    ]


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------


def _color_mode_toggle() -> html.Span:
    return html.Span(
        className="d-flex align-items-center me-2",
        children=[
            html.I(className="bi bi-sun-fill text-muted", style={"fontSize": "12px"}),
            dbc.Switch(
                id="color-mode-switch",
                value=True,
                persistence=True,
                className="d-inline-block mx-1 mb-0",
                # Bootstrap's .form-switch reserves padding-left: 2.5em for a
                # label; with no label that leaves 0.5em of dead space
                # trailing the switch, making the icon gaps asymmetric.
                # Tighten both the reserved space and the input's offsetting
                # negative margin to the switch's actual width (2em).
                style={"paddingLeft": "2em"},
                input_style={"marginLeft": "-2em"},
            ),
            html.I(className="bi bi-moon-stars-fill text-muted", style={"fontSize": "12px"}),
        ],
    )


def _sidebar_header() -> list:
    title_row = html.Div(
        style={"display": "flex", "alignItems": "center"},
        children=[
            html.H3("Signal Viewer", style={"margin": "0", "color": _ACCENT, "fontSize": "16px", "flex": "1"}),
            _color_mode_toggle(),
            dbc.Button(
                "<",
                id="sidebar-toggle",
                **_SMALL_OUTLINE_BTN,
                style={"fontSize": "12px", "padding": "4px 5px", "lineHeight": "1"},
                title="Collapse sidebar",
            ),
        ],
    )
    table_name = html.Div(
        f"{CATALOG}.{SCHEMA}.blf_gold_signals",
        className="text-muted",
        style={"fontSize": "11px"},
    )
    return [title_row, table_name, html.Hr(style={"borderColor": _BORDER, "margin": "0"})]


def _source_filters() -> list:
    return [
        # filename-filter is the single source of truth read by every other
        # callback (fetch_and_render, update_video_panel, etc.); the file-tree
        # below is purely a UI on top of it -- see sync_tree_selection in
        # callbacks.py, which folds every folder's checked files into this
        # dropdown's value. Kept mounted-but-hidden rather than removed so
        # none of that existing wiring has to change.
        dcc.Dropdown(id="filename-filter", options=[], value=[], multi=True, style={"display": "none"}),
        _section(
            "Files",
            html.Div(id="file-tree", style={"maxHeight": "220px", "overflowY": "auto", "fontSize": "11px"}),
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
                style={"fontSize": "11px", "maxHeight": "80px", "overflowY": "auto"},
            ),
        ),
    ]


def _signal_picker() -> list:
    search_box = dbc.Input(
        id="signal-search",
        type="search",
        placeholder="Filter signals...",
        size="sm",
        debounce=True,
        style={"fontSize": "12px"},
    )
    header_row = html.Div(
        className="d-flex align-items-center mb-1",
        children=[
            dbc.Label("Signals", size="sm", className="fw-semibold text-secondary mb-0 me-2"),
            dbc.ButtonGroup(
                size="sm",
                children=[
                    dbc.Button(
                        "All", id="select-all-btn", **_SMALL_OUTLINE_BTN, className="py-0", style={"fontSize": "11px"}
                    ),
                    dbc.Button(
                        "None", id="clear-all-btn", **_SMALL_OUTLINE_BTN, className="py-0", style={"fontSize": "11px"}
                    ),
                ],
            ),
            dbc.Button(
                "↻",
                id="refresh-signals-btn",
                **_SMALL_OUTLINE_BTN,
                className="py-0 ms-1",
                style={"fontSize": "11px"},
                title="Refresh signal list from warehouse",
            ),
        ],
    )
    checklist_box = html.Div(
        style={
            "maxHeight": "260px",
            "overflowY": "auto",
            "overflowX": "auto",
            "border": f"1px solid {_BORDER}",
            "borderRadius": "4px",
            "padding": "6px 8px",
        },
        children=[
            dbc.Checklist(
                id="signal-select",
                options=[],
                value=[],
                labelStyle={"whiteSpace": "nowrap"},
                style={"fontSize": "11px"},
            ),
            html.Div(
                id="signal-select-empty",
                children="",
                style={"display": "none", "fontSize": "11px", "color": _WARN, "padding": "2px 0"},
            ),
        ],
    )
    return [
        search_box,
        header_row,
        dcc.Loading(
            type="dot",
            color=_ACCENT,
            target_components={
                "all-signals-cache": "data",
                "signal-search-cache": "data",
                "signal-search-loading": "data",
            },
            children=[
                dcc.Store(id="all-signals-cache"),
                dcc.Store(id="signal-search-cache"),
                dcc.Store(id="signal-search-loading"),
                checklist_box,
            ],
        ),
    ]


def _bucket_slider() -> html.Div:
    label_row = html.Div(
        className="d-flex align-items-center mb-0",
        children=[
            dbc.Label("Buckets per signal", size="sm", className="fw-semibold text-secondary mb-0"),
            dbc.Badge(
                "?",
                id="buckets-help-icon",
                color="secondary",
                pill=True,
                className="ms-1",
                style={"cursor": "pointer", "fontSize": "10px"},
            ),
            dbc.Tooltip(
                "Downsampling resolution. Data points are divided into N buckets; "
                "the min and max value within each bucket are plotted, preserving spikes. "
                "Higher values show more detail but increase query time.",
                target="buckets-help-icon",
                placement="right",
            ),
        ],
    )
    return html.Div(
        [
            label_row,
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
    )


def _display_options() -> list:
    return [
        _section(
            "Layout",
            dbc.RadioItems(
                id="layout-mode",
                options=[
                    {"label": "Overlay", "value": "overlay"},
                    {"label": "Stacked", "value": "stacked"},
                ],
                value="stacked",
                **_SEGMENTED_RADIO,
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
                **_SEGMENTED_RADIO,
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
                **_SEGMENTED_RADIO,
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
        _bucket_slider(),
        _section(
            "Time range",
            dcc.RangeSlider(
                id="time-range-slider",
                min=0,
                max=1,
                step=1,
                value=[0, 1],
                marks={},
                tooltip={"placement": "bottom", "always_visible": False, "transform": "_fmtSliderTime"},
                disabled=True,
            ),
            html.Div(
                id="time-range-label",
                className="text-muted",
                style={"fontSize": "11px", "textAlign": "center", "marginTop": "4px"},
            ),
        ),
        _section(
            "Map",
            _signal_dropdown("Lat", "lat-signal", "latitude signal...", "d-flex align-items-center gap-2 mb-1"),
            _signal_dropdown("Lon", "lon-signal", "longitude signal...", "d-flex align-items-center gap-2"),
        ),
        dbc.Button(
            "Download Perfetto",
            id="download-perfetto-btn",
            n_clicks=0,
            **_SMALL_OUTLINE_BTN,
            className="w-100",
            disabled=True,
        ),
    ]


def _sidebar_footer() -> html.Div:
    return html.Div(
        className="d-flex flex-column",
        style={"gap": "6px", "paddingTop": "10px", "borderTop": f"1px solid {_BORDER}"},
        children=[
            dbc.Button(
                "Ask Genie",
                id="genie-toggle-btn",
                n_clicks=0,
                **_SMALL_OUTLINE_BTN,
                className="w-100",
                style={"display": "block" if GENIE_SPACE_ID else "none"},
            ),
            dbc.Button("Plot", id="plot-btn", n_clicks=0, color="info", className="w-100 fw-bold"),
            html.Div(id="avail-msg", style={"fontSize": "12px", "color": _TEXT, "minHeight": "16px"}),
            html.Div(
                id="plot-msg",
                style={"fontSize": "12px", "color": _TEXT, "wordBreak": "break-word", "minHeight": "16px"},
            ),
        ],
    )


def _sidebar() -> dbc.Col:
    scroll_area = html.Div(
        className="d-flex flex-column",
        style={"flex": "1", "overflowY": "auto", "gap": "10px", "paddingBottom": "6px"},
        children=[
            *_sidebar_header(),
            *_source_filters(),
            *_signal_picker(),
            *_display_options(),
        ],
    )
    return dbc.Col(
        id="sidebar",
        width="auto",
        className="d-flex flex-column",
        style=_SIDEBAR_STYLE,
        children=html.Div(
            id="sidebar-content",
            className="d-flex flex-column",
            style=_SIDEBAR_CONTENT_STYLE,
            children=[scroll_area, _sidebar_footer()],
        ),
    )


def _sidebar_expand_strip() -> dbc.Col:
    """Thin strip shown in place of the sidebar while it is collapsed."""
    return dbc.Col(
        id="sidebar-expand-strip",
        width="auto",
        style={
            "display": "none",
            "alignItems": "flex-start",
            "padding": "8px 2px",
            "backgroundColor": _PANEL,
            "borderRight": f"1px solid {_BORDER}",
        },
        children=dbc.Button(
            ">",
            id="sidebar-expand-btn",
            **_SMALL_OUTLINE_BTN,
            style={"fontSize": "12px", "padding": "4px 5px", "lineHeight": "1"},
            title="Expand sidebar",
        ),
    )


# ---------------------------------------------------------------------------
# Main area: insight banner, chart, floating panels, data grid
# ---------------------------------------------------------------------------


def _insight_banner() -> dbc.Alert:
    return dbc.Alert(
        html.Span(id="genie-insight-text", style={"whiteSpace": "pre-wrap", "wordBreak": "break-word"}),
        id="genie-insight-banner",
        is_open=False,
        dismissable=True,
        color="info",
        className="mb-0 rounded-0 py-2",
    )


def _chart() -> dcc.Loading:
    return dcc.Loading(
        type="circle",
        color=_ACCENT,
        # signal-data-cache intentionally excluded: it's now a tiny
        # sentinel, not the render-completion signal. chart.figure is
        # already an auto-tracked descendant output of this wrapper.
        # target_components={"chart": "figure"},
        children=dcc.Graph(
            id="chart",
            config={"displayModeBar": True, "scrollZoom": True},
            figure=_empty_fig("Select signals and click Plot"),
        ),
    )


def _map_panel() -> html.Div:
    return _float_panel(
        "map",
        "Map",
        dcc.Loading(
            type="circle",
            color=_ACCENT,
            children=dcc.Graph(id="map-chart", config={"displayModeBar": True, "scrollZoom": True}),
        ),
    )


def _video_panel() -> html.Div:
    offset_row = html.Div(
        className="d-flex align-items-center gap-2 mt-2",
        children=[
            dbc.Label("Offset (s)", size="sm", className="text-secondary mb-0", style={"minWidth": "70px"}),
            dbc.Input(
                id="video-offset-input",
                type="number",
                value=0,
                step=0.1,
                style={"width": "100px", "fontSize": "12px"},
            ),
            html.Span(
                "Adjust if video and log clocks are out of sync.",
                className="text-muted",
                style={"fontSize": "11px", "marginLeft": "8px"},
            ),
        ],
    )
    # Two stacked players, only one visible at a time: while one plays, the other
    # preloads the next segment so an end-of-file switch is a visibility swap
    # instead of a cold fetch. window._videoSync owns which is which -- see
    # show()/preload() in video-sync.js.
    players = [
        html.Video(
            id=f"video-player-{suffix}",
            controls=True,
            muted=True,
            preload="auto",
            className="video-float-video" + ("" if suffix == "a" else " video-idle"),
        )
        for suffix in ("a", "b")
    ]
    return _float_panel("video", "Video", html.Div(children=players), offset_row)


def _data_grid() -> dag.AgGrid:
    return dag.AgGrid(
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
            {"field": "value", "headerName": "Value", "filter": True, "type": "numericColumn", "minWidth": 100},
        ],
        rowData=[],
        defaultColDef={"resizable": True, "sortable": True, "flex": 1},
        dashGridOptions={
            "animateRows": False,
            "pagination": True,
            "paginationPageSize": 100,
            "paginationPageSizeSelector": [50, 100, 500],
        },
    )


def _main_area() -> dbc.Col:
    signal_tags = html.Div(
        id="signal-tags",
        style={"display": "flex", "flexWrap": "wrap", "gap": "6px", "padding": "6px 16px", "flexShrink": "0"},
    )
    return dbc.Col(
        className="d-flex flex-column",
        style={"height": "100vh", "overflowY": "auto"},
        children=[
            _insight_banner(),
            signal_tags,
            _chart(),
            _map_panel(),
            _video_panel(),
            _data_grid(),
        ],
    )


# ---------------------------------------------------------------------------
# Genie offcanvas
# ---------------------------------------------------------------------------


def _genie_title() -> html.Div:
    return html.Div(
        style={"display": "flex", "alignItems": "center"},
        children=[
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
    )


def _genie_history() -> html.Div:
    header = html.Div(
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
        children=[
            html.Span("History", className="text-muted", style={"fontSize": "12px", "flex": "1"}),
            html.Span(id="genie-history-count", className="text-muted", style={"fontSize": "11px"}),
            html.Span(id="genie-history-arrow", children="▼", className="text-muted", style={"fontSize": "10px"}),
        ],
    )
    return html.Div(
        id="genie-history-panel",
        style={"display": "none", "flexShrink": "0"},
        children=[
            html.Hr(style={"borderColor": _BORDER, "margin": "0"}),
            header,
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
    )


def _genie_preview() -> html.Div:
    return html.Div(
        id="genie-preview-box",
        style={"display": "none"},
        children=[
            html.Span(
                id="genie-preview-summary",
                style={"fontSize": "11px", "color": _TEXT, "display": "block", "marginBottom": "6px"},
            ),
            dbc.Button("Apply & Plot", id="genie-apply-btn", color="success", size="sm", className="w-100"),
        ],
    )


def _genie_panel() -> dbc.Offcanvas:
    chat_log = html.Div(
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
    )
    return dbc.Offcanvas(
        id="genie-panel-collapse",
        title=_genie_title(),
        is_open=False,
        placement="end",
        backdrop=False,
        # scrollable=True also disables react-bootstrap's Modal enforceFocus (it computes
        # enforceFocus && !scroll internally), which otherwise yanks focus back into the
        # offcanvas the instant filename-filter/signal-select are clicked.
        scrollable=True,
        close_button=False,
        style={"width": "340px", "backgroundColor": _PANEL, "color": _TEXT},
        children=[
            chat_log,
            _genie_history(),
            _genie_preview(),
            dbc.Textarea(
                id="genie-input",
                placeholder="Ask about signals or anomalies... (Ctrl+Enter to send)",
                style={"fontSize": "12px", "resize": "none"},
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
        *_stores(),
        dbc.Row(
            className="h-100 flex-nowrap g-0",
            children=[_sidebar(), _sidebar_expand_strip(), _main_area()],
        ),
        _genie_panel(),
    ],
)
