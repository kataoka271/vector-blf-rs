"""All Dash callbacks (server-side and clientside)."""

import time as _time
import uuid as _uuid
from urllib.parse import quote

import dash
import dash_bootstrap_components as dbc
import flask
import pandas as pd
import plotly.graph_objects as go
from dash import ALL, Input, Output, State, callback, dcc, html

from . import cache
from ._dash import app
from .config import _LOCAL_DEV, GENIE_SPACE_ID, _parse_key
from .db import (
    _fetch_all_channels,
    _fetch_all_signals,
    _fetch_filenames,
    _fetch_global_time_range,
    _fetch_signals_by_search,
    _fetch_video_for_file,
    _log_token_info,
    fetch_signal_data,
)
from .figures import (
    _BORDER,
    _PANEL,
    _SIDEBAR_CONTENT_STYLE,
    _SIDEBAR_STYLE,
    _TEXT,
    _XaxisMode,
    render_chart_and_grid,
)
from .genie import (
    _genie_executor,
    _genie_futures,
    _genie_query,
    build_context_prefix,
    interpret_genie_response,
)
from .perfetto import build_perfetto_trace

_MAX_VISIBLE_TAGS = 20  # cap pattern-matched button registrations in signal-tags


def _fmt_s(seconds: float) -> str:
    """Format elapsed seconds as HH:MM:SS.mmm."""
    total_ms = round(abs(seconds) * 1000)
    ms = total_ms % 1000
    s = total_ms // 1000
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}.{ms:03d}"


# ---------------------------------------------------------------------------
# Signal list
# ---------------------------------------------------------------------------


@callback(
    Output("all-signals-cache", "data"),
    Output("all-channels-cache", "data"),
    Output("time-range-store", "data"),
    Output("filenames-cache", "data"),
    Output("session-id-store", "data"),
    Input("url", "pathname"),
)
def prefetch_signals(_):
    return (
        _fetch_all_signals(),
        _fetch_all_channels(),
        _fetch_global_time_range(),
        _fetch_filenames(),
        _uuid.uuid4().hex,
    )


@callback(
    Output("all-signals-cache", "data", allow_duplicate=True),
    Output("all-channels-cache", "data", allow_duplicate=True),
    Output("time-range-store", "data", allow_duplicate=True),
    Output("filenames-cache", "data", allow_duplicate=True),
    Output("filename-debounce-interval", "disabled", allow_duplicate=True),
    Input("refresh-signals-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_signal_cache(_):
    return _fetch_all_signals(), _fetch_all_channels(), _fetch_global_time_range(), _fetch_filenames(), True


app.clientside_callback(
    """
    function(filenames) {
        if (!filenames || filenames.length === 0) return [];
        return filenames.map(function(f) {
            var parts = f.split('/');
            return { label: parts[parts.length - 1], value: f };
        });
    }
    """,
    Output("filename-filter", "options"),
    Input("filenames-cache", "data"),
)


app.clientside_callback(
    "function(v) { return [v, false]; }",
    Output("filename-pending-store", "data"),
    Output("filename-debounce-interval", "disabled"),
    Input("filename-filter", "value"),
    prevent_initial_call=True,
)


@callback(
    Output("all-signals-cache", "data", allow_duplicate=True),
    Output("all-channels-cache", "data", allow_duplicate=True),
    Output("filename-debounce-interval", "disabled", allow_duplicate=True),
    Input("filename-debounce-interval", "n_intervals"),
    State("filename-pending-store", "data"),
    prevent_initial_call=True,
)
def filter_signals_by_file(_, filenames):
    scope = filenames or None
    return _fetch_all_signals(scope), _fetch_all_channels(scope), True


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
    Input("all-channels-cache", "data"),
)


@callback(
    Output("signal-search-cache", "data"),
    Input("signal-search", "value"),
    Input("all-signals-cache", "data"),
    State("filename-pending-store", "data"),
    State("signal-search-cache", "data"),
    prevent_initial_call=True,
)
def search_signals_server(search_value, _all_signals, filenames, prev_cache):
    """Search signals server-side so results aren't limited to the ~500-row browse cache.

    Reuses the previous result set when the new keyword only narrows an
    untruncated prior search in the same file scope, to avoid re-querying on
    every keystroke.
    """
    kw = (search_value or "").strip().lower()
    if not kw:
        return None

    scope = list(filenames) if filenames else None
    triggered_id = dash.ctx.triggered_id
    if (
        triggered_id == "signal-search"
        and prev_cache
        and prev_cache.get("scope") == scope
        and not prev_cache.get("truncated")
        and kw.startswith(prev_cache.get("keyword", "\0"))
    ):
        return dash.no_update

    rows, truncated = _fetch_signals_by_search(kw, scope)
    return {"keyword": kw, "scope": scope, "truncated": truncated, "rows": rows}


# Rebuilds the signal picker (and lat/lon pickers) entirely client-side so
# filtering/typing doesn't round-trip to the server.
# Inputs: source-filter (selected sources), all-signals-cache (browse cache,
#   capped ~500 rows), signal-search (keyword), channel-filter (selected
#   channels), signal-search-cache (server-side search results, may cover
#   more rows than the browse cache), signal-select.value (State, current
#   selection to preserve/prune).
# Outputs: signal-select.options/.value, avail-msg.children (status text),
#   lat-signal.options, lon-signal.options.
app.clientside_callback(
    """
    function(sources, cache, search, channels, searchCache, currentValue) {
        var no_update = window.dash_clientside.no_update;
        var NORMAL_STYLE = {fontSize: "12px", color: "#ccc", minHeight: "16px"};
        var WARN_STYLE = {fontSize: "12px", color: "#ff4444", minHeight: "16px", fontWeight: "600"};
        var EMPTY_HIDDEN = {display: "none", fontSize: "11px", color: "#ff4444", padding: "2px 0"};
        var EMPTY_SHOWN = {display: "block", fontSize: "11px", color: "#ff4444", padding: "2px 0"};

        if (cache === null || cache === undefined) {
            return [no_update, no_update, "Loading signals...", NORMAL_STYLE, no_update, no_update, "", EMPTY_HIDDEN];
        }
        if (!sources || sources.length === 0) {
            return [[], [], "No source selected.", NORMAL_STYLE, [], [], "", EMPTY_HIDDEN];
        }

        var ctx = window.dash_clientside.callback_context;
        var triggeredId = (ctx.triggered && ctx.triggered.length > 0)
            ? ctx.triggered[0].prop_id.split(".")[0]
            : null;
        var searchTriggered = triggeredId === "signal-search" || triggeredId === "signal-search-cache";

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

        // allOpts (and therefore lat/lon pickers + the "N available" baseline) is always
        // derived from the browse cache scoped by source/channel only, never by search.
        var allOpts = byChannel.map(toOpt);

        var kw = search ? search.trim().toLowerCase() : "";

        // Prefer full server search results once they arrive (scoped by source/channel);
        // until then fall back to filtering the (possibly incomplete) browse cache so
        // typing still gives instant feedback.
        var usingServerRows = false;
        var searchPool = byChannel;
        if (kw && searchCache && searchCache.rows && searchCache.keyword !== undefined
            && kw.indexOf(searchCache.keyword) === 0) {
            var poolBySource = searchCache.rows.filter(function(r) { return sourceSet.has(r.signal_source); });
            searchPool = channelSet
                ? poolBySource.filter(function(r) { return channelSet.has(r.signal_source + r.channel); })
                : poolBySource;
            usingServerRows = true;
        }

        var filtered = byChannel;
        if (kw) {
            filtered = searchPool.filter(function(r) {
                return r.signal_name.toLowerCase().indexOf(kw) !== -1 ||
                       r.signal_source.toLowerCase().indexOf(kw) !== -1;
            });
        }

        if (filtered.length === 0) {
            var msg;
            if (search) {
                msg = "No signals match.";
            } else if (cache.length === 0) {
                msg = "No signals found. Has the pipeline run?";
            } else {
                msg = "No signals match the current filters.";
            }
            var valueOut = searchTriggered ? no_update : [];
            return [[], valueOut, msg, WARN_STYLE, [], [], msg, EMPTY_SHOWN];
        }

        var opts = filtered.map(toOpt);
        var MAX_SHOWN = 100;
        var truncated = opts.length > MAX_SHOWN;
        var visibleOpts = truncated ? opts.slice(0, MAX_SHOWN) : opts;
        var matchCountLabel = String(opts.length) + (usingServerRows && searchCache.truncated ? "+" : "");
        var suffix = "";
        if (search) {
            suffix = truncated
                ? " (" + matchCountLabel + " match, showing first " + MAX_SHOWN + ")"
                : " (" + matchCountLabel + " match)";
        } else if (truncated) {
            suffix = " (showing first " + MAX_SHOWN + ", filter to narrow)";
        }
        var statusMsg = allOpts.length + " signal(s) available." + suffix;

        var LAT_LON_MAX = 200;
        var latLonOpts = allOpts.length > LAT_LON_MAX ? allOpts.slice(0, LAT_LON_MAX) : allOpts;

        if (searchTriggered) {
            return [visibleOpts, no_update, statusMsg, NORMAL_STYLE, latLonOpts, latLonOpts, "", EMPTY_HIDDEN];
        }

        var allValid = new Set(allOpts.map(function(o) { return o.value; }));
        var newValue = (currentValue || []).filter(function(v) { return allValid.has(v); });
        return [visibleOpts, newValue, statusMsg, NORMAL_STYLE, latLonOpts, latLonOpts, "", EMPTY_HIDDEN];
    }
    """,
    Output("signal-select", "options"),
    Output("signal-select", "value"),
    Output("avail-msg", "children"),
    Output("avail-msg", "style"),
    Output("lat-signal", "options"),
    Output("lon-signal", "options"),
    Output("signal-select-empty", "children"),
    Output("signal-select-empty", "style"),
    Input("source-filter", "value"),
    Input("all-signals-cache", "data"),
    Input("signal-search", "value"),
    Input("channel-filter", "value"),
    Input("signal-search-cache", "data"),
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
    Output("signal-tags", "children"),
    Input("signal-select", "value"),
    State("genie-flagged-signals-store", "data"),
)
def render_signal_tags(selected, anomalous_signals):
    if not selected:
        return []
    anomalous_set = set(anomalous_signals or [])
    tags = []
    for key in selected[:_MAX_VISIBLE_TAGS]:
        src, channel, name = _parse_key(key)
        display_src = "ETH" if src == "SOMEIP" else src
        label = f"{display_src}{channel}::{name}"
        is_anomalous = key in anomalous_set
        tags.append(
            html.Span(
                [
                    label,
                    html.Button(
                        "×",
                        id={"type": "remove-signal-btn", "index": key},
                        n_clicks=0,
                        className="signal-tag-close",
                        style={
                            "background": "none",
                            "border": "none",
                            "color": "#ff8888" if is_anomalous else "#888",
                            "cursor": "pointer",
                            "fontSize": "14px",
                            "lineHeight": "1",
                            "padding": "0 2px",
                            "marginLeft": "2px",
                        },
                    ),
                ],
                style={
                    "display": "inline-flex",
                    "alignItems": "center",
                    "gap": "2px",
                    "backgroundColor": _PANEL,
                    "border": "1px solid #ff4444" if is_anomalous else f"1px solid {_BORDER}",
                    "borderRadius": "12px",
                    "padding": "2px 8px 2px 10px",
                    "fontSize": "11px",
                    "color": "#ff8888" if is_anomalous else _TEXT,
                    "whiteSpace": "nowrap",
                },
            )
        )
    if len(selected) > _MAX_VISIBLE_TAGS:
        tags.append(
            html.Span(
                f"+{len(selected) - _MAX_VISIBLE_TAGS}",
                style={
                    "display": "inline-flex",
                    "alignItems": "center",
                    "backgroundColor": _PANEL,
                    "border": f"1px solid {_BORDER}",
                    "borderRadius": "12px",
                    "padding": "2px 10px",
                    "fontSize": "11px",
                    "color": "#888",
                    "whiteSpace": "nowrap",
                },
            )
        )
    return tags


@callback(
    Output("signal-select", "value", allow_duplicate=True),
    Input({"type": "remove-signal-btn", "index": ALL}, "n_clicks"),
    State("signal-select", "value"),
    prevent_initial_call=True,
)
def remove_signal(n_clicks_list, selected):
    if not any(n_clicks_list):
        return dash.no_update
    triggered = dash.ctx.triggered_id
    if not isinstance(triggered, dict):
        return dash.no_update
    key_to_remove = triggered["index"]
    return [s for s in (selected or []) if s != key_to_remove]


# ---------------------------------------------------------------------------
# Data fetch & chart render
# ---------------------------------------------------------------------------


@callback(
    Output("signal-data-cache", "data"),
    Output("time-range-store", "data", allow_duplicate=True),
    Output("plot-msg", "children"),
    Output("chart", "figure", allow_duplicate=True),
    Output("grid", "rowData", allow_duplicate=True),
    Output("grid", "columnDefs", allow_duplicate=True),
    Output("map-chart", "figure", allow_duplicate=True),
    Output("map-section", "style", allow_duplicate=True),
    Output("plot-btn", "disabled", allow_duplicate=True),
    Output("session-id-store", "data", allow_duplicate=True),
    Input("plot-btn", "n_clicks"),
    State("source-filter", "value"),
    State("signal-select", "value"),
    State("max-pts", "value"),
    State("time-range-slider", "value"),
    State("time-range-store", "data"),
    State("lat-signal", "value"),
    State("lon-signal", "value"),
    State("filename-filter", "value"),
    State("layout-mode", "value"),
    State("chart-height", "value"),
    State("xaxis-mode", "value"),
    State("overlay-mode", "value"),
    State("genie-anomaly-markers-store", "data"),
    State("session-id-store", "data"),
    prevent_initial_call=True,
)
def fetch_and_render(
    _,
    sources,
    selected,
    max_pts,
    time_range,
    time_range_store,
    lat_key,
    lon_key,
    filenames,
    layout,
    chart_height,
    xaxis_mode: _XaxisMode,
    overlay_mode,
    anomalies_raw,
    session_id,
):
    # Self-heal: prefetch_signals should have already set this at page load; fall back
    # if a click somehow raced ahead of it, so later redraws still have a valid key.
    session_id = session_id or _uuid.uuid4().hex
    map_empty = go.Figure()
    map_hidden = {"display": "none"}

    df, new_time_range, msg = fetch_signal_data(
        sources, selected, max_pts, time_range, time_range_store, lat_key, lon_key, filenames
    )

    if df is None:
        return (
            None,
            new_time_range,
            msg,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            map_empty,
            map_hidden,
            False,
            session_id,
        )

    cache.put_df(session_id, df)
    sentinel = {"session_id": session_id, "n_rows": len(df)}
    # Mirrors what render_chart used to read via State("time-range-store", "data"):
    # the freshly computed range when the range query succeeded, else whatever was
    # already in the store (time_range_store, unchanged since new_time_range is no_update).
    time_store = new_time_range if isinstance(new_time_range, dict) else time_range_store

    chart_fig, chart_msg, row_data, col_defs, map_fig, map_style = render_chart_and_grid(
        df, selected, layout, chart_height, xaxis_mode, overlay_mode, lat_key, lon_key, anomalies_raw, time_store
    )

    return (
        sentinel,
        new_time_range,
        chart_msg,
        chart_fig,
        row_data,
        col_defs,
        map_fig,
        map_style,
        False,
        session_id,
    )


@callback(
    Output("chart", "figure"),
    Output("plot-msg", "children", allow_duplicate=True),
    Output("grid", "rowData"),
    Output("grid", "columnDefs"),
    Output("map-chart", "figure"),
    Output("map-section", "style"),
    Input("signal-select", "value"),
    Input("layout-mode", "value"),
    Input("chart-height", "value"),
    Input("xaxis-mode", "value"),
    Input("overlay-mode", "value"),
    State("lat-signal", "value"),
    State("lon-signal", "value"),
    State("genie-anomaly-markers-store", "data"),
    State("time-range-store", "data"),
    State("session-id-store", "data"),
    prevent_initial_call=True,
)
def redraw_chart(
    selected,
    layout,
    chart_height,
    xaxis_mode: _XaxisMode,
    overlay_mode,
    lat_key,
    lon_key,
    anomalies_raw,
    time_store,
    session_id,
):
    df_all = cache.get_df(session_id)
    if df_all is None:
        # Cause-neutral: a miss here means either "nothing fetched yet this session"
        # (e.g. toggling layout-mode before ever clicking Plot) or "the server-side
        # cache entry is gone" (restart/eviction) -- these are indistinguishable, so
        # avoid implying anything specific "expired".
        msg = "No data yet -- click Plot." if selected else dash.no_update
        return dash.no_update, msg, [], dash.no_update, go.Figure(), {"display": "none"}
    return render_chart_and_grid(
        df_all, selected, layout, chart_height, xaxis_mode, overlay_mode, lat_key, lon_key, anomalies_raw, time_store
    )


# ---------------------------------------------------------------------------
# Time-range slider
# ---------------------------------------------------------------------------


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
        low_ts = lo_dt.strftime("%H:%M:%S.") + f"{lo_dt.microsecond // 1000:03d}"
        high_ts = hi_dt.strftime("%H:%M:%S.") + f"{hi_dt.microsecond // 1000:03d}"
        label = f"{low_ts} - {high_ts}  (duration {_fmt_s(high - low)})"
    else:
        label = f"{_fmt_s(low - t_min)} - {_fmt_s(high - t_min)}  (total {_fmt_s(duration)})"
    return t_min, t_max, step, marks, [low, high], False, label


app.clientside_callback(
    """
    function(value, store) {
        if (!value || !store) return window.dash_clientside.no_update;
        var lo = value[0], hi = value[1];
        var tMin = store.min;
        var t0 = store.t0 ? new Date(store.t0) : null;

        function fmtHms(totalSec) {
            var totalMs = Math.round(Math.abs(totalSec) * 1000);
            var ms = totalMs % 1000;
            var s = Math.floor(totalMs / 1000);
            return [Math.floor(s / 3600), Math.floor((s % 3600) / 60), s % 60]
                .map(function(v) { return v.toString().padStart(2, '0'); }).join(':')
                + '.' + ms.toString().padStart(3, '0');
        }
        function fmtTime(offsetS) {
            if (t0) {
                var dt = new Date(t0.getTime() + (offsetS - tMin) * 1000);
                return [dt.getUTCHours(), dt.getUTCMinutes(), dt.getUTCSeconds()]
                    .map(function(v) { return v.toString().padStart(2, '0'); }).join(':')
                    + '.' + dt.getUTCMilliseconds().toString().padStart(3, '0');
            }
            return fmtHms(offsetS - tMin);
        }

        var loStr = fmtTime(lo), hiStr = fmtTime(hi), durStr = fmtHms(hi - lo);
        if (t0) {
            return loStr + ' - ' + hiStr + '  (duration ' + durStr + ')';
        }
        return loStr + ' - ' + hiStr + '  (total ' + fmtHms(store.max - store.min) + ')';
    }
    """,
    Output("time-range-label", "children"),
    Input("time-range-slider", "value"),
    State("time-range-store", "data"),
)

app.clientside_callback(
    """
    function(store) {
        if (!store) return window.dash_clientside.no_update;
        window._timeRangeStore = { tMin: store.min, t0: store.t0 || null };
        return window.dash_clientside.no_update;
    }
    """,
    Output("time-range-store", "data", allow_duplicate=True),
    Input("time-range-store", "data"),
    prevent_initial_call=True,
)

# ---------------------------------------------------------------------------
# Video playback sync
# ---------------------------------------------------------------------------


@callback(
    Output("video-player", "src"),
    Output("video-section", "style"),
    Output("video-cursor-interval", "disabled"),
    Output("video-meta-store", "data"),
    Input("signal-data-cache", "data"),
    State("filename-filter", "value"),
    State("time-range-store", "data"),
    prevent_initial_call=True,
)
def update_video_panel(cache_data, filenames, time_store):
    hidden = {"display": "none"}
    if cache_data is None or not filenames or len(filenames) != 1 or time_store is None:
        return dash.no_update, hidden, True, None

    info = _fetch_video_for_file(filenames[0])
    if info is None:
        return dash.no_update, hidden, True, None

    src = f"/video-proxy?file={quote(filenames[0])}"
    meta = {"t_min": time_store["min"], "t0": time_store.get("t0")}
    return src, {}, False, meta


@callback(
    Output("video-seek-store", "data"),
    Input("chart", "clickData"),
    State("video-meta-store", "data"),
    State("video-offset-input", "value"),
    prevent_initial_call=True,
)
def seek_video_from_chart_click(click_data, meta, offset):
    if not click_data or not meta:
        return dash.no_update
    x = click_data["points"][0].get("x")
    if x is None:
        return dash.no_update
    offset = float(offset or 0)
    if meta.get("t0"):
        video_seconds = (pd.Timestamp(x) - pd.Timestamp(meta["t0"])).total_seconds() - offset
    else:
        video_seconds = (float(x) - meta["t_min"]) - offset
    return {"seconds": max(0.0, video_seconds)}


app.clientside_callback(
    """
    function(seek) {
        if (!seek) return window.dash_clientside.no_update;
        var videoEl = document.getElementById('video-player');
        if (videoEl) { videoEl.currentTime = Math.max(0, seek.seconds); }
        return window.dash_clientside.no_update;
    }
    """,
    Output("video-cursor-sink", "children", allow_duplicate=True),
    Input("video-seek-store", "data"),
    prevent_initial_call=True,
)

app.clientside_callback(
    """
    function(_n, meta, offset) {
        if (!meta) return window.dash_clientside.no_update;
        var videoEl = document.getElementById('video-player');
        if (!videoEl) return window.dash_clientside.no_update;
        var t = videoEl.currentTime + (parseFloat(offset) || 0);
        var xValue = meta.t0 ? new Date(new Date(meta.t0).getTime() + t * 1000).toISOString() : meta.t_min + t;
        window._videoSync.setCursor('chart', xValue);
        return window.dash_clientside.no_update;
    }
    """,
    Output("video-cursor-sink", "children", allow_duplicate=True),
    Input("video-cursor-interval", "n_intervals"),
    State("video-meta-store", "data"),
    State("video-offset-input", "value"),
    prevent_initial_call=True,
)

# ---------------------------------------------------------------------------
# Misc UI
# ---------------------------------------------------------------------------


@callback(
    Output("replot-toast", "children"),
    Output("replot-toast", "is_open"),
    Input("signal-select", "value"),
    Input("signal-data-cache", "data"),
    State("session-id-store", "data"),
)
def update_replot_notice(selected, cache_data, session_id):
    if not cache_data or not selected:
        return "", False
    df = cache.get_df(session_id)
    if df is None:
        return "", False
    cached_keys = set(df["signal_source"] + df["channel"].astype(str) + "::" + df["signal_name"])
    new_count = sum(1 for s in selected if s not in cached_keys)
    if new_count == 0:
        return "", False
    return f"{new_count} new signal(s) not yet fetched. Click Plot to update.", True


app.clientside_callback(
    "function(cache, selected) { return !cache || !selected || selected.length === 0; }",
    Output("download-perfetto-btn", "disabled"),
    Input("signal-data-cache", "data"),
    Input("signal-select", "value"),
)


@callback(
    Output("dl-perfetto", "data"),
    Input("download-perfetto-btn", "n_clicks"),
    State("signal-data-cache", "data"),
    State("signal-select", "value"),
    State("session-id-store", "data"),
    prevent_initial_call=True,
)
def download_perfetto(_, cache_data, selected, session_id):
    if not cache_data or not selected:
        return dash.no_update
    df = cache.get_df(session_id)
    if df is None:
        return dash.no_update
    trace_bytes = build_perfetto_trace(df, selected)
    if not trace_bytes:
        return dash.no_update
    return dcc.send_bytes(trace_bytes, "signals.perfetto-trace")


@callback(
    Output("sidebar", "style"),
    Output("sidebar-content", "style"),
    Output("sidebar-expand-strip", "style"),
    Input("sidebar-toggle", "n_clicks"),
    Input("sidebar-expand-btn", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_sidebar(collapse_clicks, expand_clicks):
    total = (collapse_clicks or 0) + (expand_clicks or 0)
    collapsed = total % 2 == 1
    if collapsed:
        return (
            {"width": "0", "minWidth": "0", "overflow": "hidden", "padding": "0"},
            {"display": "none"},
            {
                "display": "flex",
                "alignItems": "flex-start",
                "padding": "8px 2px",
                "backgroundColor": _PANEL,
                "borderRight": f"1px solid {_BORDER}",
            },
        )
    return (
        _SIDEBAR_STYLE,
        _SIDEBAR_CONTENT_STYLE,
        {"display": "none"},
    )


app.clientside_callback(
    "function(layout) { return layout === 'overlay' ? {display: 'none'} : {}; }",
    Output("xaxis-section", "style"),
    Input("layout-mode", "value"),
)

app.clientside_callback(
    "function(layout) { return layout === 'overlay' ? {} : {display: 'none'}; }",
    Output("overlay-section", "style"),
    Input("layout-mode", "value"),
)


# ---------------------------------------------------------------------------
# Genie callbacks
# ---------------------------------------------------------------------------


def _get_component_id(b: object) -> object:
    """Return the id of a Dash component whether it is a Python object or a serialized dict."""
    if isinstance(b, dict):
        return b.get("props", {}).get("id")
    return getattr(b, "id", None)


@callback(
    Output("genie-panel-collapse", "is_open"),
    Output("genie-toggle-btn", "children"),
    Input("genie-toggle-btn", "n_clicks"),
    Input("genie-close-btn", "n_clicks"),
    State("genie-panel-collapse", "is_open"),
    prevent_initial_call=True,
)
def toggle_genie_panel(n, _close, is_open):
    if dash.ctx.triggered_id == "genie-close-btn":
        return False, "Ask Genie"
    new_state = not (is_open or False)
    label = "Close Genie" if new_state else "Ask Genie"
    return new_state, label


@callback(
    Output("genie-conversation-store", "data", allow_duplicate=True),
    Output("genie-chat-log", "children", allow_duplicate=True),
    Output("genie-preview-store", "data", allow_duplicate=True),
    Output("genie-anomaly-markers-store", "data", allow_duplicate=True),
    Output("genie-flagged-signals-store", "data", allow_duplicate=True),
    Output("genie-input", "value"),
    Input("genie-new-conv-btn", "n_clicks"),
    prevent_initial_call=True,
)
def new_genie_conversation(_):
    return None, [], None, None, None, ""


@callback(
    Output("genie-request-store", "data"),
    Output("genie-poll-interval", "disabled"),
    Output("genie-chat-log", "children"),
    Output("genie-ask-btn", "disabled"),
    Output("genie-input", "value", allow_duplicate=True),
    Input("genie-ask-btn", "n_clicks"),
    State("genie-input", "value"),
    State("genie-conversation-store", "data"),
    State("genie-chat-log", "children"),
    State("filename-filter", "value"),
    State("source-filter", "value"),
    State("channel-filter", "value"),
    prevent_initial_call=True,
)
def submit_genie_query(n_clicks, question, conv_store, chat_log, filenames, sources, channels):
    if not question or not question.strip():
        return dash.no_update, True, dash.no_update, False, dash.no_update
    if not GENIE_SPACE_ID and not _LOCAL_DEV:
        error_bubble = html.Div(
            "GENIE_SPACE_ID is not configured.",
            className="genie-bubble genie-ai",
        )
        return dash.no_update, True, (chat_log or []) + [error_bubble], False, dash.no_update

    user_token = flask.request.headers.get("X-Forwarded-Access-Token", "") if not _LOCAL_DEV else ""
    if user_token:
        _log_token_info(user_token)
    conv_id = (conv_store or {}).get("conversation_id")
    request_id = str(_uuid.uuid4())
    context = build_context_prefix(filenames, sources, channels)
    content = f"{context}\n\n{question.strip()}" if context else question.strip()
    future = _genie_executor.submit(_genie_query, GENIE_SPACE_ID or "mock", content, conv_id, user_token)
    _genie_futures[request_id] = (future, _time.time())

    user_bubble = html.Div(question.strip(), className="genie-bubble genie-user")
    thinking_bubble = html.Div("Thinking...", id="genie-thinking-bubble", className="genie-bubble genie-ai")
    new_log = (chat_log or []) + [user_bubble, thinking_bubble]
    return {"request_id": request_id, "status": "pending", "question": question.strip()}, False, new_log, True, ""


@callback(
    Output("genie-request-store", "data", allow_duplicate=True),
    Output("genie-poll-interval", "disabled", allow_duplicate=True),
    Output("genie-conversation-store", "data"),
    Output("genie-preview-store", "data"),
    Output("genie-chat-log", "children", allow_duplicate=True),
    Output("genie-ask-btn", "disabled", allow_duplicate=True),
    Output("genie-history-store", "data", allow_duplicate=True),
    Input("genie-poll-interval", "n_intervals"),
    State("genie-request-store", "data"),
    State("genie-conversation-store", "data"),
    State("genie-chat-log", "children"),
    State("all-signals-cache", "data"),
    State("time-range-store", "data"),
    State("genie-history-store", "data"),
    prevent_initial_call=True,
)
def poll_genie_result(n_intervals, req_store, conv_store, chat_log, all_signals_raw, time_store, history_raw):
    print(
        f"[poll_genie_result] called n_intervals={n_intervals} status={req_store.get('status') if req_store else None}",
        flush=True,
    )
    if not req_store or req_store.get("status") != "pending":
        return dash.no_update, True, dash.no_update, dash.no_update, dash.no_update, False, dash.no_update

    request_id = req_store["request_id"]
    entry = _genie_futures.get(request_id)
    if entry is None:
        return (
            {"request_id": request_id, "status": "done"},
            True,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            False,
            dash.no_update,
        )

    future, created_at = entry
    # Expire after 120 s
    if not future.done() and _time.time() - created_at < 120:
        return dash.no_update, False, dash.no_update, dash.no_update, dash.no_update, True, dash.no_update

    if not future.done():
        future.cancel()
        del _genie_futures[request_id]
        timeout_bubble = html.Div("Request timed out.", className="genie-bubble genie-ai")
        log = [b for b in (chat_log or []) if _get_component_id(b) != "genie-thinking-bubble"]
        return (
            {"request_id": request_id, "status": "done"},
            True,
            dash.no_update,
            dash.no_update,
            log + [timeout_bubble],
            False,
            dash.no_update,
        )

    result = future.result()
    del _genie_futures[request_id]

    new_conv = {"space_id": GENIE_SPACE_ID, "conversation_id": result["conversation_id"]}

    log = [b for b in (chat_log or []) if _get_component_id(b) != "genie-thinking-bubble"]
    response_text = result["text"] or "(no text response)"
    ai_bubble = html.Div(response_text, className="genie-bubble genie-ai")
    log = log + [ai_bubble]

    all_signals = all_signals_raw if isinstance(all_signals_raw, list) else []
    preview_obj = interpret_genie_response(result, all_signals, time_store)
    preview = preview_obj.to_dict() if preview_obj is not None else None

    question = req_store.get("question", "")
    history = list(history_raw or [])
    history.append({"question": question, "answer": response_text})
    history = history[-20:]

    return (
        {"request_id": request_id, "status": "done"},
        True,
        new_conv,
        preview,
        log,
        False,
        history,
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
            parts.push("Time: " + preview.t_lo.toFixed(1) + "s - " + preview.t_hi.toFixed(1) + "s");
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
            display: "flex",
            alignItems: "flex-start",
            gap: "8px",
            padding: "8px 16px",
            backgroundColor: "#1c3a4a",
            color: "#7ecfec",
            fontSize: "12px",
            borderBottom: "1px solid #2a5060",
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
    Output("genie-insight-text", "children"),
    Input("genie-insight-store", "data"),
)


app.clientside_callback(
    "function(n) { return null; }",
    Output("genie-insight-store", "data", allow_duplicate=True),
    Input("genie-insight-close-btn", "n_clicks"),
    prevent_initial_call=True,
)


@callback(
    Output("signal-select", "value", allow_duplicate=True),
    Output("time-range-slider", "value", allow_duplicate=True),
    Output("plot-btn", "n_clicks", allow_duplicate=True),
    Output("genie-insight-store", "data"),
    Output("genie-preview-store", "data", allow_duplicate=True),
    Output("genie-anomaly-markers-store", "data"),
    Output("genie-flagged-signals-store", "data"),
    Input("genie-apply-btn", "n_clicks"),
    State("genie-preview-store", "data"),
    State("signal-select", "value"),
    State("time-range-slider", "value"),
    State("plot-btn", "n_clicks"),
    prevent_initial_call=True,
)
def apply_genie_preview(n_clicks, preview, current_signals, current_range, plot_n):
    if not preview:
        return (
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )
    new_signals = preview["signals"] if preview.get("signals") else current_signals
    new_range = [preview["t_lo"], preview["t_hi"]] if preview.get("t_lo") is not None else current_range
    return (
        new_signals,
        new_range,
        (plot_n or 0) + 1,
        preview.get("explanation"),
        None,
        preview.get("anomalies"),
        preview.get("anomalous_signals"),
    )


@callback(
    Output("genie-history-accordion", "children"),
    Output("genie-history-panel", "style"),
    Output("genie-history-count", "children"),
    Input("genie-history-store", "data"),
)
def render_genie_history(history):
    if not history:
        return [], {"display": "none", "flexShrink": "0"}, ""
    items = []
    for i, entry in enumerate(reversed(history)):
        q = entry.get("question", "")
        a = entry.get("answer", "")
        q_short = q[:50] + "..." if len(q) > 50 else q
        items.append(
            dbc.AccordionItem(
                html.Div(
                    a,
                    style={"fontSize": "11px", "color": _TEXT, "whiteSpace": "pre-wrap", "wordBreak": "break-word"},
                ),
                title=html.Span(q_short, style={"fontSize": "11px", "color": "#aaa"}),
                item_id=f"h{len(history) - 1 - i}",
            )
        )
    return items, {"flexShrink": "0"}, f"({len(history)})"


app.clientside_callback(
    "function(n, is_open) { return n ? !is_open : is_open; }",
    Output("genie-history-collapse", "is_open"),
    Input("genie-history-header", "n_clicks"),
    State("genie-history-collapse", "is_open"),
    prevent_initial_call=True,
)


app.clientside_callback(
    "function(is_open) { return is_open ? '▲' : '▼'; }",
    Output("genie-history-arrow", "children"),
    Input("genie-history-collapse", "is_open"),
)
