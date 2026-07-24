"""Plotly figure builders and shared theme constants."""

import html as _html
from typing import Literal, cast

import dash
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas.core.groupby import DataFrameGroupBy

from .config import _parse_key, _to_utc_naive

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

# DOM-facing styling (layout.py, AgGrid): reference the active Bootstrap
# color-mode's CSS variables directly so the sidebar/panel chrome tracks the
# dark/light toggle (see color-mode-switch) without any Python-side
# branching -- swapping the <link> stylesheet in _dash.py's clientside
# callback is enough.
_BG = "var(--bs-body-bg)"
_PANEL = "var(--bs-secondary-bg)"
_BORDER = "var(--bs-border-color)"
_ACCENT = "var(--bs-info)"
_TEXT = "var(--bs-body-color)"
_WARN = "var(--bs-danger)"

# Plotly bakes real color values into the figure JSON -- it can't read CSS
# variables -- so figures mirror the dbc.themes.DARKLY / FLATLY computed
# palette directly instead. The two are bootswatch's "flatly family"
# (Darkly is documented upstream as "Flatly in night mode"), so info/danger
# are identical between them and don't need a light variant.
_PLOTLY_ACCENT = "#3498db"  # --bs-info (both themes)
_PLOTLY_WARN = "#e74c3c"  # --bs-danger (both themes)
_PLOTLY_DARK = {
    "bg": "#222222",  # --bs-body-bg
    "panel": "#303030",  # --bs-secondary-bg
    "border": "#444444",  # --bs-border-color
    "text": "#dee2e6",  # --bs-body-color
    "muted": "#888888",  # --bs-gray
}
_PLOTLY_LIGHT = {
    "bg": "#ffffff",  # --bs-body-bg
    "panel": "#ecf0f1",  # --bs-secondary-bg
    "border": "#dee2e6",  # --bs-border-color
    "text": "#212529",  # --bs-body-color
    "muted": "#95a5a6",  # --bs-gray
}


def _plotly_palette(dark: bool) -> dict:
    p = dict(_PLOTLY_DARK if dark else _PLOTLY_LIGHT)
    p["accent"] = _PLOTLY_ACCENT
    p["warn"] = _PLOTLY_WARN
    p["template"] = "plotly_dark" if dark else "plotly_white"
    return p


def _axis_box(palette: dict) -> dict:
    return {"showline": True, "mirror": True, "linecolor": palette["border"], "linewidth": 1}


_SIDEBAR_STYLE: dict = {
    "width": "270px",
    "backgroundColor": _PANEL,
    "padding": "12px 12px",
    "gap": "0",
    "color": _TEXT,
    "borderRight": f"1px solid {_BORDER}",
}
_SIDEBAR_CONTENT_STYLE: dict = {"flex": "1", "overflow": "hidden"}

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

_Traces = list[tuple[str, int, str, pd.Series, pd.Series, "pd.Series | None"]]
_Anomalies = list[dict]  # list of {"x": float | str, "label": str}
_XaxisMode = Literal["shared", "synced", "free"]

_VAL_WIDTH = 12  # fixed character width for right-aligned signal values in hover tooltip
_GRID_MAX_ROWS = 50_000  # cap AgGrid rowData; AgGrid paginates client-side, so this only guards against pathological payload sizes (e.g. max_pts=50k x lo/hi)
_MAX_STACKED_TRACES = 20  # cap to prevent stacked figure height explosion


def _scale_traces(traces: _Traces) -> _Traces:
    """Scale each numeric trace to [-1, 1]: y' = (y - mid) / half_range."""
    result = []
    for src, channel, name, x, y, y_str in traces:
        if y_str is not None:
            result.append((src, channel, name, x, y, y_str))
            continue
        y_min = float(y.min())
        y_max = float(y.max())
        if y_max == y_min:
            result.append((src, channel, name, x, y, y_str))
            continue
        mid = (y_max + y_min) / 2.0
        half_range = (y_max - y_min) / 2.0
        result.append((src, channel, name, x, (y - mid) / half_range, y_str))
    return result


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _empty_fig(msg="", dark: bool = True) -> go.Figure:
    palette = _plotly_palette(dark)
    ann = (
        [
            {
                "text": msg,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 16, "color": palette["border"]},
            }
        ]
        if msg
        else []
    )
    fig = go.Figure(
        layout=go.Layout(
            template=palette["template"],
            paper_bgcolor=palette["bg"],
            plot_bgcolor=palette["bg"],
            height=600,
            annotations=ann,
        )
    )
    axis_box = _axis_box(palette)
    fig.update_xaxes(**axis_box)
    fig.update_yaxes(**axis_box)
    return fig


def _hover_customdata(y: pd.Series, y_str: "pd.Series | None" = None) -> "np.ndarray":
    """Return customdata for hover tooltip: category string when available, formatted float otherwise.

    Returns a 2-D fixed-width numpy string array (n, 1) rather than a nested
    Python list. Plotly's property validator deep-copies and re-validates
    every element of a nested list (or an object-dtype numpy array -- numpy's
    own __deepcopy__ falls back to per-element copy.deepcopy for dtype=object)
    one at a time, which is catastrophically slow at tens of thousands of
    points; a native `<U*` array takes numpy's fast bulk-memory-copy path.
    """
    formatted = y.map(lambda v: f"{v:{_VAL_WIDTH}.4g}".replace(" ", "&nbsp;")).to_numpy(dtype=object).astype(str)
    if y_str is not None:
        str_vals = y_str.to_numpy(dtype=object)
        is_na = y_str.isna().to_numpy()
        combined = np.where(is_na, formatted, str_vals.astype(str)).astype(str)
        return combined.reshape(-1, 1)
    return formatted.reshape(-1, 1)


def _plot_x(x: pd.Series) -> "pd.Series | np.ndarray":
    """Strip tz info for a tz-aware datetime x-series before handing it to Plotly.

    `Series.to_numpy()` on a tz-aware datetime64 series produces a dtype=object
    array of Timestamp objects (not a native datetime64 array); Plotly's
    property validator then deep-copies it element by element -- the same
    catastrophic cost as the nested-list customdata case above. A plain
    datetime64[ns] array (tz dropped) avoids that path and renders identically.
    """
    if isinstance(x.dtype, pd.DatetimeTZDtype):
        return x.to_numpy(dtype="datetime64[ns]")
    return x


def _overlay_fig(
    traces: _Traces,
    height: int = 600,
    min_height: int = 400,
    anomalies: "_Anomalies | None" = None,
    original_traces: "_Traces | None" = None,
    dark: bool = True,
) -> go.Figure:
    palette = _plotly_palette(dark)
    axis_box = _axis_box(palette)
    fig = go.Figure()
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _, _ in traces), default=0)
    for i, (src, channel, name, x, y, y_str) in enumerate(traces):
        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        y_plot = y_str if y_str is not None else y
        y_hover, y_hover_str = (original_traces[i][4], original_traces[i][5]) if original_traces else (y, y_str)
        fig.add_trace(
            go.Scattergl(
                x=_plot_x(x),
                y=y_plot,
                mode="lines+markers",
                line=dict(shape="hv"),
                name=label,
                showlegend=False,
                customdata=_hover_customdata(y_hover, y_hover_str),
                hovertemplate=f"{_html.escape(label)}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
    if anomalies:
        for anom in anomalies:
            fig.add_vline(
                x=anom["x"],
                line_color=palette["warn"],
                line_dash="dash",
                line_width=1.5,
                annotation_text=anom["label"],
                annotation_font_color=palette["warn"],
                annotation_font_size=10,
                annotation_position="top right",
            )
    fig.update_layout(
        template=palette["template"],
        paper_bgcolor=palette["bg"],
        plot_bgcolor=palette["bg"],
        height=max(height, min_height),
        hovermode="x unified",
        xaxis_title="Time",
        hoverlabel={"font_family": "monospace"},
        margin={"l": 60, "r": 20, "t": 50, "b": 60},
    )
    fig.update_xaxes(
        **axis_box,
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikedash="dot",
        spikecolor=palette["muted"],
        spikethickness=1,
    )
    fig.update_yaxes(**axis_box)
    return fig


# Intentionally avoids make_subplots: hoversubplots="axis" does not propagate
# the cursor across subplots created by make_subplots (Plotly bug, see
# https://community.plotly.com/t/hoversubplots-axis-not-working-with-make-subplots/84239).
# Instead, each trace gets its own y-axis with a computed domain sharing one x-axis.
def _stacked_fig(
    traces: _Traces,
    height: int = 200,
    xaxis_mode: _XaxisMode = "shared",
    min_height: int = 100,
    anomalies: "_Anomalies | None" = None,
    dark: bool = True,
) -> go.Figure:
    palette = _plotly_palette(dark)
    axis_box = _axis_box(palette)
    n = len(traces)
    spacing = max(0.03, 0.20 / n) if xaxis_mode in ("synced", "free") else max(0.02, 0.20 / n)
    h = (1.0 - spacing * max(n - 1, 0)) / n
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _, _ in traces), default=0)

    fig = go.Figure()
    axes_kw: dict = {}
    annotations = []

    for i, (src, channel, name, x, y, y_str) in enumerate(traces):
        bottom = max(0.0, 1.0 - (i + 1) * h - i * spacing)
        top = min(1.0, 1.0 - i * (h + spacing))
        yref = "y" if i == 0 else f"y{i + 1}"
        ykey = "yaxis" if i == 0 else f"yaxis{i + 1}"

        if xaxis_mode in ("synced", "free"):
            xref = "x" if i == 0 else f"x{i + 1}"
            xkey = "xaxis" if i == 0 else f"xaxis{i + 1}"
            axes_kw[xkey] = {
                **axis_box,
                "anchor": yref,
                "showspikes": True,
                "spikemode": "across",
                "spikesnap": "cursor",
                "spikedash": "dot",
                "spikecolor": palette["muted"],
                "spikethickness": 1,
                **({"matches": "x"} if xaxis_mode == "synced" and i > 0 else {}),
                **({"title": "Time"} if i == n - 1 else {}),
            }
        else:
            xref = "x"

        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        y_plot = y_str if y_str is not None else y
        fig.add_trace(
            go.Scattergl(
                x=_plot_x(x),
                y=y_plot,
                mode="lines+markers",
                line=dict(shape="hv"),
                name=label,
                xaxis=xref,
                yaxis=yref,
                showlegend=False,
                customdata=_hover_customdata(y, y_str),
                hovertemplate=f"{_html.escape(label)}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
        axes_kw[ykey] = {"domain": [bottom, top], **axis_box}
        annotations.append(
            {
                "text": _html.escape(f"{src}{channel}::{name}"),
                "xref": "paper",
                "yref": "paper",
                "x": 0,
                "y": top,
                "xanchor": "left",
                "yanchor": "bottom",
                "showarrow": False,
                "font": {"size": 14, "color": palette["text"]},
                "bgcolor": palette["panel"],
                "borderpad": 4,
            }
        )

    if xaxis_mode == "shared":
        last_y_ref = "y" if n == 1 else f"y{n}"
        axes_kw["xaxis"] = {
            **axis_box,
            "anchor": last_y_ref,
            "title": "Time",
            "showspikes": True,
            "spikemode": "across",
            "spikesnap": "cursor",
            "spikedash": "dot",
            "spikecolor": palette["muted"],
            "spikethickness": 1,
        }

    if anomalies:
        for anom in anomalies:
            fig.add_vline(
                x=anom["x"],
                line_color=palette["warn"],
                line_dash="dash",
                line_width=1.5,
                annotation_text=anom["label"],
                annotation_font_color=palette["warn"],
                annotation_font_size=10,
                annotation_position="top right",
            )
    fig.update_layout(
        template=palette["template"],
        paper_bgcolor=palette["bg"],
        plot_bgcolor=palette["bg"],
        height=max(height, min_height) * n,
        hovermode="x unified",
        hoversubplots="axis",
        hoverlabel={"font_family": "monospace"},
        annotations=annotations,
        margin={"l": 60, "r": 20, "t": 60, "b": 60},
        **axes_kw,
    )
    return fig


def _group_by_key(df_all: pd.DataFrame) -> "DataFrameGroupBy | None":
    """Group df_all by (signal_source, channel, signal_name) once.

    Looking up each selected signal via boolean-masking df_all is O(n_signals
    * n_rows); grouping once up front makes each lookup an O(1) get_group.
    """
    if df_all.empty:
        return None
    return df_all.groupby(["signal_source", "channel", "signal_name"], sort=False)


def _lookup_key(groups: "DataFrameGroupBy | None", key: str) -> "pd.DataFrame | None":
    if groups is None:
        return None
    src, channel, name = _parse_key(key)
    try:
        return cast(pd.DataFrame, groups.get_group((src, channel, name)))
    except KeyError:
        return None


def _extract_traces(df_all: pd.DataFrame, selected, groups: "DataFrameGroupBy | None") -> _Traces:
    traces: _Traces = []
    for key in selected:
        sub = _lookup_key(groups, key)
        if sub is None or sub.empty:
            continue
        src, channel, name = _parse_key(key)
        x = (
            sub["event_time"]
            if "event_time" in df_all.columns and sub["event_time"].notna().any()
            else sub["timestamp_s"]
        )
        y_str = sub["signal_str"] if "signal_str" in df_all.columns and sub["signal_str"].notna().any() else None
        traces.append((src, channel, name, x, sub["signal_value"], y_str))
    return traces


def _pivot_table(traces: _Traces) -> tuple[list[dict], list[dict]]:
    """Return (row_data, col_defs) for the AgGrid pivot view."""
    parts = [traces[0][3].reset_index(drop=True).astype(str).rename("time")]
    categorical_cols: set[str] = set()
    for src, channel, name, _, y, y_str in traces:
        display_src = "ETH" if src == "SOMEIP" else src
        col_name = f"{display_src}{channel}::{name}"
        col = (y_str if y_str is not None else y).reset_index(drop=True)
        parts.append(col.rename(col_name))
        if y_str is not None:
            categorical_cols.add(col_name)
    pivot = pd.concat(parts, axis=1).head(_GRID_MAX_ROWS)

    signal_cols = [c for c in pivot.columns if c != "time"]
    col_defs = [{"field": "time", "headerName": "Time", "pinned": "left", "filter": True, "minWidth": 160}] + [
        {"field": c, "headerName": c, "filter": True, "minWidth": 140}
        if c in categorical_cols
        else {"field": c, "headerName": c, "type": "numericColumn", "filter": True, "minWidth": 140}
        for c in signal_cols
    ]
    return pivot.to_dict("records"), col_defs


def build_anomaly_vlines(
    anomalies_raw: "_Anomalies | None",
    traces: _Traces,
    time_store: dict | None,
) -> "_Anomalies | None":
    """Convert anomaly timestamp_s values to the x-axis unit used by the figure traces."""
    if not anomalies_raw or not traces:
        return None
    first_x = traces[0][3]
    use_datetime = pd.api.types.is_datetime64_any_dtype(first_x)
    t0_iso = (time_store or {}).get("t0")
    t_min = float((time_store or {}).get("min", 0))
    t0_raw = pd.Timestamp(t0_iso) if t0_iso else None
    t0 = _to_utc_naive(t0_raw) if isinstance(t0_raw, pd.Timestamp) else None
    result: _Anomalies = []
    for anom in anomalies_raw:
        try:
            ts = float(anom["timestamp_s"])
        except (ValueError, TypeError):
            continue
        if use_datetime and t0 is not None:
            t1 = t0 + pd.Timedelta(seconds=ts - t_min)
            assert isinstance(t1, pd.Timestamp)
            x_val = t1.isoformat()
        else:
            x_val = ts
        result.append({"x": x_val, "label": anom.get("label", "Anomaly")})
    return result or None


def _normalize_lat_lon(lat: pd.Series, lon: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Wrap lat into [-90, 90] and lon into [-180, 180)."""
    lon = (lon + 180) % 360 - 180
    lat = (lat + 90) % 360 - 90
    lat = lat.mask(lat > 90, 180 - lat)
    return lat, lon


def _map_fig(lat: pd.Series, lon: pd.Series, dark: bool = True) -> go.Figure:
    """Build the GPS track map. lat/lon must already be normalized (see _normalize_lat_lon).

    Trace 0 is the full track (line+markers); trace 1 is a single-point marker
    tracking the video's current position, updated client-side via
    Plotly.restyle in video-sync.js (see gps-track-store). It's kept as a
    fixed-index trace here -- rather than added/removed dynamically -- so the
    restyle target index never shifts.
    """
    palette = _plotly_palette(dark)
    center_lat = float(lat.mean())
    center_lon = float(lon.mean())
    fig = go.Figure(
        [
            go.Scattermapbox(
                lat=lat,
                lon=lon,
                mode="lines+markers",
                marker={"size": 4, "color": palette["accent"]},
                line={"width": 1, "color": palette["accent"]},
                hoverinfo="skip",
            ),
            go.Scattermapbox(
                lat=[lat.iloc[0]],
                lon=[lon.iloc[0]],
                mode="markers",
                marker={"size": 14, "color": "#ff3b30"},
                hoverinfo="skip",
            ),
        ]
    )
    fig.update_layout(
        paper_bgcolor=palette["bg"],
        showlegend=False,
        mapbox={
            "style": "carto-darkmatter",
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": 10,
        },
        height=500,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
    )
    return fig


def render_chart_and_grid(
    df_all: pd.DataFrame,
    selected,
    layout,
    chart_height,
    xaxis_mode: _XaxisMode,
    overlay_mode,
    lat_key,
    lon_key,
    anomalies_raw,
    time_store,
    chart_only: bool = False,
    dark: bool = True,
) -> tuple[object, object, object, object, object, object, object]:
    """Build the chart figure, grid rows/columns, and map from an already-fetched DataFrame.

    Pure function of df_all -- no I/O, no dcc.Store (de)serialization. Callers
    own dash.ctx.triggered_id logic and any signal-data-cache/plot-btn.disabled
    writes. Returns (chart_fig, chart_msg, row_data, col_defs, map_fig, map_style,
    gps_track).

    gps_track is {"t": [...], "lat": [...], "lon": [...]} for the video-synced
    map marker (see video-sync.js), or None when there's no GPS track to show.
    Its "t" values are in the same domain as the chart's x-axis (event_time ISO
    strings, or numeric timestamp_s) so video-sync.js can compare them directly
    against the cursor xValue it already computes from video.currentTime.

    chart_only=True skips rebuilding the grid pivot table and the GPS map --
    both are unchanged when the only inputs that fired are chart display
    options (height/xaxis-mode/overlay-mode) that don't affect them. This
    avoids re-filtering df_all and re-serializing the (potentially large)
    AgGrid rowData on every such tweak.
    """
    map_empty = go.Figure()
    map_hidden = {"display": "none"}

    chart_fig: object = dash.no_update
    chart_msg = ""
    row_data: list = []
    col_defs: object = dash.no_update
    map_fig = map_empty
    map_style = map_hidden
    gps_track: object = None

    groups = _group_by_key(df_all) if (selected or (lat_key and lon_key)) else None

    if selected:
        traces = _extract_traces(df_all, selected, groups)

        if traces:
            h = int(chart_height or 600)
            truncation_note = ""
            if layout == "stacked" and len(traces) > _MAX_STACKED_TRACES:
                truncation_note = (
                    f" (stacked: first {_MAX_STACKED_TRACES} of {len(traces)} signals; use Overlay to see all)"
                )
                traces = traces[:_MAX_STACKED_TRACES]
            vlines = build_anomaly_vlines(anomalies_raw, traces, time_store)
            normalize = layout == "overlay" and overlay_mode != "nominal"
            chart_fig = (
                _overlay_fig(
                    _scale_traces(traces) if normalize else traces,
                    h,
                    anomalies=vlines,
                    original_traces=traces if normalize else None,
                    dark=dark,
                )
                if layout == "overlay"
                else _stacked_fig(traces, h, xaxis_mode or "shared", anomalies=vlines, dark=dark)
            )
            if not chart_only:
                row_data, col_defs = _pivot_table(traces)
                total = sum(len(x) for _, _, _, x, _, _ in traces)
                chart_msg = f"{total:,} pts across {len(traces)} signal(s).{truncation_note}"
        else:
            chart_msg = "No data for selected signals."
            chart_fig = _empty_fig(chart_msg, dark=dark)
            row_data = []
    elif not chart_only:
        # All signals removed (e.g. via signal-tags) -- reset the chart instead of
        # leaving the last-plotted figure on screen with no message/grid to match.
        chart_fig = _empty_fig("Select signals and click Plot", dark=dark)

    if not chart_only and lat_key and lon_key and "timestamp_ns" in df_all.columns:
        lat_sub = _lookup_key(groups, lat_key)
        lon_sub = _lookup_key(groups, lon_key)
        if lat_sub is not None and lon_sub is not None:
            # event_time/timestamp_s ride along on lat_df only (not lon_df) to avoid
            # merge_asof _x/_y suffixing -- lat/lon are already aligned to the same
            # instant via the timestamp_ns asof-join, so either row's time works.
            lat_cols = ["timestamp_ns", "signal_value"]
            if "event_time" in df_all.columns:
                lat_cols.append("event_time")
            if "timestamp_s" in df_all.columns:
                lat_cols.append("timestamp_s")
            lat_df = lat_sub[lat_cols].rename(columns={"signal_value": "lat"}).sort_values("timestamp_ns")
            lon_df = (
                lon_sub[["timestamp_ns", "signal_value"]]
                .rename(columns={"signal_value": "lon"})
                .sort_values("timestamp_ns")
            )
            merged = pd.merge_asof(lat_df, lon_df, on="timestamp_ns", direction="nearest").dropna(subset=["lat", "lon"])
            if not merged.empty:
                lat_norm, lon_norm = _normalize_lat_lon(merged["lat"], merged["lon"])
                map_fig = _map_fig(lat_norm, lon_norm, dark=dark)
                map_style = {}
                pts = len(merged)
                chart_msg = chart_msg + f" Map: {pts:,} GPS pts." if chart_msg else f"Map: {pts:,} GPS pts."
                use_event_time = "event_time" in merged.columns and merged["event_time"].notna().any()
                t_values = (
                    [ts.isoformat() if pd.notna(ts) else None for ts in merged["event_time"]]
                    if use_event_time
                    else merged["timestamp_s"].tolist()
                )
                gps_track = {"t": t_values, "lat": lat_norm.tolist(), "lon": lon_norm.tolist()}

    if chart_only:
        return (
            chart_fig,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )
    return chart_fig, chart_msg, row_data, col_defs, map_fig, map_style, gps_track
