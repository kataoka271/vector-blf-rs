"""Flask route that proxies video files from a Unity Catalog Volume for HTML5 <video> playback.

Downloads each video once into an ephemeral local cache, then serves it via
flask.send_file(conditional=True), which handles HTTP Range/206 requests (needed
for <video> seeking) automatically -- no manual Range-header parsing.
"""

import hashlib
import os
import tempfile
import traceback

import flask

from ._dash import app
from .config import _LOCAL_DEV, cfg
from .db import _fetch_video_for_file

_CACHE_DIR = os.path.join(tempfile.gettempdir(), "blf-video-cache")


def _cache_path(video_path: str, mtime: str | None) -> str:
    key = hashlib.sha256(f"{video_path}|{mtime}".encode()).hexdigest()
    ext = os.path.splitext(video_path)[1] or ".mp4"
    return os.path.join(_CACHE_DIR, key + ext)


def _download_to_cache(video_path: str, local_path: str) -> None:
    from databricks.sdk import WorkspaceClient

    # Uses the app's own service principal, not the viewer's OBO token: the SP has a
    # scoped READ_VOLUME grant on exactly this volume (signal_viewer.app.yml), whereas
    # granting the OBO token enough scope for Files API volume downloads currently
    # requires the broad "all-apis" user_api_scope.
    assert cfg is not None, "Databricks config is not initialized."
    w = WorkspaceClient(config=cfg)

    os.makedirs(_CACHE_DIR, exist_ok=True)
    tmp_path = local_path + ".part"
    contents = w.files.download(video_path).contents
    assert contents is not None, "Files API download returned no content stream."
    with open(tmp_path, "wb") as f:
        while chunk := contents.read(1024 * 1024):
            f.write(chunk)
    os.replace(tmp_path, local_path)


@app.server.route("/video-proxy")
def video_proxy():
    filename = flask.request.args.get("file")
    if not filename:
        return "Missing 'file' query parameter.", 400

    info = _fetch_video_for_file(filename)
    if info is None:
        return "No video found for this file.", 404

    if _LOCAL_DEV:
        # BLF_DEV_SAMPLE_VIDEO already points at a local file -- no Volume to fetch from.
        return flask.send_file(info["video_path"], conditional=True)

    local_path = _cache_path(info["video_path"], info["mtime"])
    if not os.path.exists(local_path):
        try:
            _download_to_cache(info["video_path"], local_path)
        except Exception as exc:
            print(
                f"[video_proxy] ERROR downloading {info['video_path']!r}: {exc}\n{traceback.format_exc()}", flush=True
            )
            return "Failed to fetch video.", 502

    return flask.send_file(local_path, conditional=True)
