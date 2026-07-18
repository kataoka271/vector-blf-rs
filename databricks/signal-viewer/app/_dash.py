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

# Bootstrap 5.3's "subtle"/"-emphasis" color tokens (used by dbc.Alert, dbc.Badge,
# the history accordion, etc.) default to their light-mode values unless the
# dark color mode is explicitly selected -- Darkly's own base palette is dark
# regardless, but these newer tokens need the attribute set on <html> to match.
app.index_string = """<!DOCTYPE html>
<html data-bs-theme="dark">
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>"""
