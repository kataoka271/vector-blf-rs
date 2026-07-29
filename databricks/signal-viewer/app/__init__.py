from . import (
    callbacks,  # noqa: F401 -- registers all callbacks
    layout,  # noqa: F401 -- sets app.layout
    upload,  # noqa: F401 -- registers the /upload-proxy/<kind> Flask route
    video,  # noqa: F401 -- registers the /video-proxy Flask route
)
from ._dash import app  # noqa: F401 -- re-export Dash instance
