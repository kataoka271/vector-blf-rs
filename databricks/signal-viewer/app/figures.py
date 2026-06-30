"""Plotly figure builders and shared theme constants."""

import html as _html
from typing import Literal

import pandas as pd
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

_BG = "#13111a"
_PANEL = "#1c1a27"
_BORDER = "#2a2838"
_ACCENT = "#7ecfec"
_TEXT = "#ccc"

_AXIS_BOX = {"showline": True, "mirror": True, "linecolor": _BORDER, "linewidth": 1}

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


def _hover_customdata(y: pd.Series, y_str: "pd.Series | None" = None) -> "list[list[str]]":
    """Return customdata for hover tooltip: category string when available, formatted float otherwise."""
    if y_str is not None:
        return [[str(s) if pd.notna(s) else f"{v:{_VAL_WIDTH}.4g}".replace(" ", "&nbsp;")] for v, s in zip(y, y_str)]
    return [[f"{v:{_VAL_WIDTH}.4g}".replace(" ", "&nbsp;")] for v in y]


def _overlay_fig(
    traces: _Traces,
    height: int = 600,
    min_height: int = 400,
    anomalies: "_Anomalies | None" = None,
    original_traces: "_Traces | None" = None,
) -> go.Figure:
    fig = go.Figure()
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _, _ in traces), default=0)
    for i, (src, channel, name, x, y, y_str) in enumerate(traces):
        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        y_plot = y_str if y_str is not None else y
        y_hover, y_hover_str = (original_traces[i][4], original_traces[i][5]) if original_traces else (y, y_str)
        fig.add_trace(
            go.Scattergl(
                x=x,
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
                line_color="#ff4444",
                line_dash="dash",
                line_width=1.5,
                annotation_text=anom["label"],
                annotation_font_color="#ff4444",
                annotation_font_size=10,
                annotation_position="top right",
            )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=max(height, min_height),
        hovermode="x unified",
        xaxis_title="Time",
        hoverlabel={"font_family": "monospace"},
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
def _stacked_fig(
    traces: _Traces,
    height: int = 200,
    xaxis_mode: _XaxisMode = "shared",
    min_height: int = 100,
    anomalies: "_Anomalies | None" = None,
) -> go.Figure:
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

        label = f"{src}{channel}::{name}"
        pad = "&nbsp;" * (max_label - len(label))
        y_plot = y_str if y_str is not None else y
        fig.add_trace(
            go.Scattergl(
                x=x,
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
        axes_kw[ykey] = {"domain": [bottom, top], **_AXIS_BOX}
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

    if anomalies:
        for anom in anomalies:
            fig.add_vline(
                x=anom["x"],
                line_color="#ff4444",
                line_dash="dash",
                line_width=1.5,
                annotation_text=anom["label"],
                annotation_font_color="#ff4444",
                annotation_font_size=10,
                annotation_position="top right",
            )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=max(height, min_height) * n,
        hovermode="x unified",
        hoversubplots="axis",
        hoverlabel={"font_family": "monospace"},
        annotations=annotations,
        margin={"l": 60, "r": 20, "t": 60, "b": 60},
        **axes_kw,
    )
    return fig


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
    pivot = pd.concat(parts, axis=1)

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
    t0 = pd.Timestamp(t0_iso) if t0_iso else None
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
