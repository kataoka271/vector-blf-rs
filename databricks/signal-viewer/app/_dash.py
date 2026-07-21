"""Dash application instance — imported by layout and callbacks to avoid circular imports."""

import os

import dash
import dash_bootstrap_components as dbc

# assets/ lives next to app.py (one level above this package)
_ASSETS = os.path.join(os.path.dirname(__file__), "..", "assets")

app = dash.Dash(
    __name__,
    title="Signal Viewer",
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
    assets_folder=_ASSETS,
    # Without this, Dash rewrites document.title to "Updating..." while any
    # callback is in flight -- including the clientside one that fires every
    # 100ms from video-cursor-interval, which flickers the tab title during
    # video playback.
    update_title=None,
)

# Bootstrap 5.3's "subtle"/"-emphasis" color tokens (used by dbc.Alert, dbc.Badge,
# the history accordion, etc.) default to their light-mode values unless the
# dark color mode is explicitly selected -- Darkly's own base palette is dark
# regardless, but these newer tokens need the attribute set on <html> to match.
#
# Darkly and Flatly are bootswatch's matched dark/light pair (Darkly is
# documented upstream as "Flatly in night mode"), so the light-mode
# counterpart is loaded here too -- disabled by default -- and the
# color-mode-switch clientside callback (callbacks.py) flips which <link>
# is disabled and the data-bs-theme attribute together, in lockstep. A
# Bootswatch skin can't be light/dark-toggled via data-bs-theme alone (each
# skin bakes one fixed palette), so this swaps the whole stylesheet instead.
app.index_string = f"""<!DOCTYPE html>
<html data-bs-theme="dark">
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <link id="theme-flatly" rel="stylesheet" href="{dbc.themes.FLATLY}" disabled>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>"""
