"""Plotly figure builders and shared theme constants."""

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

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

_Traces = list[tuple[str, int, str, pd.Series, pd.Series, "pd.Series | None"]]
_XaxisMode = Literal["shared", "synced", "free"]

_VAL_WIDTH = 12  # fixed character width for right-aligned signal values in hover tooltip

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


def _overlay_fig(traces: _Traces, height: int = 600) -> go.Figure:
    fig = go.Figure()
    max_label = max((len(f"{src}{channel}::{name}") for src, channel, name, _, _, _ in traces), default=0)
    for src, channel, name, x, y, y_str in traces:
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
                showlegend=False,
                customdata=_hover_customdata(y, y_str),
                hovertemplate=f"{label}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        height=height,
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
def _stacked_fig(traces: _Traces, height: int = 600, xaxis_mode: _XaxisMode = "shared") -> go.Figure:
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
                hovertemplate=f"{label}{pad} : %{{customdata[0]}}<extra></extra>",
            )
        )
        axes_kw[ykey] = {"domain": [bottom, top], **_AXIS_BOX}
        annotations.append(
            {
                "text": f"{src}{channel}::{name}",
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
        col_name = f"{src}{channel}::{name}"
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
