"""Signal Viewer -- Plotly Dash visualization app for Databricks Apps."""

from app import app  # noqa: F401 -- triggers layout/callbacks registration, exposes Dash instance
from app.config import _LOCAL_DEV

if __name__ == "__main__":
    # debug=True enables Werkzeug's debug reloader and Dash's dev-tools hot-reload
    # poller (a client-side `/_reload-hash` poll every few seconds that forces a
    # full page reload on any perceived change). Databricks Apps runs this same
    # `python app.py` entrypoint in production (see app.yaml), so leaving debug
    # unconditionally on meant every deployed session was also polling for
    # reloads -- any spurious reload (e.g. a reloader restart) yanks focus out
    # from under whatever the user was typing into (filename-filter,
    # signal-search), which is more noticeable during long-lived interactions
    # like video playback or an in-flight Genie query.
    app.run(debug=_LOCAL_DEV)
