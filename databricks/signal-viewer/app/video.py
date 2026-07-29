"""Flask route that streams video files from a Unity Catalog Volume for HTML5 <video> playback.

Byte ranges are proxied straight through to the Files API, which documents support
for the Range header, so playback starts after the first few hundred KB and a seek
fetches only the range it needs. Nothing is staged on local disk: an app instance
has only a small ephemeral filesystem, and dashcam footage is large enough that
caching whole files there is what made cold playback slow in the first place.
"""

import mimetypes
import traceback
from urllib.parse import quote

import flask
import requests

from ._dash import app
from .config import _LOCAL_DEV, cfg
from .db import _fetch_video_for_file

# Relayed verbatim from the upstream response. Without Content-Range/Accept-Ranges
# a <video> treats the stream as non-seekable and disables scrubbing entirely.
_RELAYED_HEADERS = ("Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag")

_CHUNK_BYTES = 256 * 1024
_TIMEOUT = (10, 60)  # (connect, read) seconds


def _stream_from_volume(video_path: str, range_header: str | None) -> flask.Response:
    """Proxy one (possibly partial) read of `video_path` to the caller.

    Returns a streaming flask.Response mirroring the Files API status (200 or 206)
    and range headers. Raises requests.RequestException on transport failure.
    """
    # Uses the app's own service principal, not the viewer's OBO token: the SP has a
    # scoped READ_VOLUME grant on exactly this volume (signal_viewer.app.yml), whereas
    # granting the OBO token enough scope for Files API volume reads currently
    # requires the broad "all-apis" user_api_scope.
    assert cfg is not None, "Databricks config is not initialized."
    if video_path.startswith("dbfs:"):
        video_path = video_path[5:]

    headers = dict(cfg.authenticate())  # refreshes the OAuth token when needed
    headers["Accept"] = "application/octet-stream"
    if range_header:
        headers["Range"] = range_header

    # The SDK's files.download() wraps this same endpoint but exposes no way to pass
    # a Range header, and drops content-range from the response, so call it directly.
    url = f"{cfg.host}/api/2.0/fs/files{quote(video_path)}"
    upstream = requests.get(url, headers=headers, stream=True, timeout=_TIMEOUT)
    upstream.raise_for_status()

    def relay():
        try:
            yield from upstream.iter_content(_CHUNK_BYTES)
        finally:
            upstream.close()

    # Files API answers with application/octet-stream, which no browser will decode
    # as media -- derive the real type from the extension instead.
    content_type = mimetypes.guess_type(video_path)[0] or "video/mp4"
    out = {k: upstream.headers[k] for k in _RELAYED_HEADERS if k in upstream.headers}
    out.setdefault("Accept-Ranges", "bytes")
    return flask.Response(relay(), status=upstream.status_code, headers=out, content_type=content_type)


@app.server.route("/video-proxy")
def video_proxy():
    filename = flask.request.args.get("file")
    if not filename:
        return "Missing 'file' query parameter.", 400

    info = _fetch_video_for_file(filename)
    if info is None:
        return "No video found for this file.", 404

    if _LOCAL_DEV:
        # BLF_DEV_SAMPLE_VIDEO already points at a local file -- no Volume to fetch
        # from. send_file(conditional=True) serves Range/206 from disk by itself.
        return flask.send_file(info["video_path"], conditional=True)

    try:
        return _stream_from_volume(info["video_path"], flask.request.headers.get("Range"))
    except Exception as exc:
        print(f"[video_proxy] ERROR streaming {info['video_path']!r}: {exc}\n{traceback.format_exc()}", flush=True)
        return "Failed to fetch video.", 502
