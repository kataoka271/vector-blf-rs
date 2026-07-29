"""Flask routes that relay uploaded BLF/video files into their Unity Catalog Volumes.

An upload is streamed straight through to the Files API, not staged on local disk or
routed through a Dash callback: an app instance has only a small ephemeral filesystem,
and a Dash callback payload would base64-encode the whole file into a single request --
the same reasons video.py proxies playback directly rather than caching or buffering a
copy (see that module's docstring).
"""

import traceback
from urllib.parse import quote

import flask
import requests

from ._dash import app
from .config import _LOCAL_DEV, BLF_RAW_PATH, BLF_VIDEO_UPLOAD_PATH, cfg

_TIMEOUT = (10, 300)  # (connect, read) seconds -- large video files can take a while to PUT

# kind -> (destination volume dir, allowed filename suffixes). Matches the file types
# blf_ingestion (*.blf) and blf_video_files (*.mp4/*.webm/*.mov) actually watch for.
_DESTINATIONS = {
    "blf": (BLF_RAW_PATH, (".blf",)),
    "video": (BLF_VIDEO_UPLOAD_PATH, (".mp4", ".webm", ".mov")),
}


def _put_to_volume(volume_dir: str, filename: str, body) -> None:
    """Stream `body` to `{volume_dir}/{filename}` via the Files API, overwriting any existing file.

    Raises requests.RequestException on transport failure.
    """
    assert cfg is not None, "Databricks config is not initialized."
    dest_path = f"{volume_dir.rstrip('/')}/{filename}"
    # Uses the app's own service principal, same as video.py's playback proxy -- see
    # that module's comment on why the viewer's OBO token isn't used here instead.
    headers = dict(cfg.authenticate())
    headers["Content-Type"] = "application/octet-stream"
    url = f"{cfg.host}/api/2.0/fs/files{quote(dest_path)}?overwrite=true"
    resp = requests.put(url, headers=headers, data=body, timeout=_TIMEOUT)
    resp.raise_for_status()


@app.server.route("/upload-proxy/<kind>", methods=["POST"])
def upload_proxy(kind):
    dest = _DESTINATIONS.get(kind)
    if dest is None:
        return "Unknown upload kind.", 400
    volume_dir, allowed_suffixes = dest

    filename = flask.request.headers.get("X-Filename", "").strip()
    if not filename or "/" in filename or "\\" in filename:
        return "Missing or invalid X-Filename header.", 400
    if not filename.lower().endswith(allowed_suffixes):
        return f"Filename must end with one of {allowed_suffixes}.", 400
    if not volume_dir:
        return f"No destination volume configured for {kind!r} uploads.", 500

    if _LOCAL_DEV:
        # No Volume to write to -- report success without persisting anything, so the
        # upload UI can be exercised without a real Databricks deployment.
        print(f"[LOCAL DEV] upload_proxy: pretending to store {filename!r} ({kind})", flush=True)
        return "", 204

    try:
        # flask.request.stream gives the raw (unbuffered) request body as long as
        # nothing upstream of this has already called .get_data()/.form -- the fetch()
        # caller sends the file as an octet-stream body, so Flask never parses it as
        # form data and this stays a straight relay.
        _put_to_volume(volume_dir, filename, flask.request.stream)
    except Exception as exc:
        print(
            f"[upload_proxy] ERROR uploading {filename!r} to {volume_dir!r}: {exc}\n{traceback.format_exc()}",
            flush=True,
        )
        return "Failed to upload file.", 502
    return "", 204
