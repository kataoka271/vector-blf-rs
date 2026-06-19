"""Signal Viewer -- Plotly Dash visualization app for Databricks Apps."""

from app import app  # noqa: F401 -- triggers layout/callbacks registration, exposes Dash instance

if __name__ == "__main__":
    app.run(debug=True)
