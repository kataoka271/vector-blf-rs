"""Dash application instance — imported by layout and callbacks to avoid circular imports."""

import os

import dash
import dash_bootstrap_components as dbc

# assets/ lives next to app.py (one level above this package)
_ASSETS = os.path.join(os.path.dirname(__file__), "..", "assets")

app = dash.Dash(
    __name__,
    title="Signal Viewer",
    external_stylesheets=[dbc.themes.DARKLY],
    assets_folder=_ASSETS,
)
