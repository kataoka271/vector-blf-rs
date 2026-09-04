"""Live traffic view: the same diagram `bench.traffic` renders, served over HTTP and
re-drawn while the run is in progress.

A sampler thread takes a `traffic.snapshot()` every `interval` seconds and keeps the
difference from the previous one, so the rates shown are what flowed during the last
window rather than the run's average. The HTTP side is stdlib only (`http.server`) and
serves two things: `/` (a page that polls) and `/traffic.json` (the latest sample). No
framework, no dependency -- `examples/bench` stays runnable with nothing installed
beyond what a topology's own transports need.

Mermaid itself is loaded from a CDN by the page. Offline, the page falls back to showing
the diagram source as text, which stays perfectly readable -- the numbers are in it.

    live = LiveTraffic(bench, port=8765)
    live.start()
    bench.run(duration=60)
    live.stop()
"""

from __future__ import annotations

import json
import threading
import time
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

from bench import traffic

if TYPE_CHECKING:
    from bench.bench import TestBench

DEFAULT_PORT = 8765
DEFAULT_INTERVAL = 1.0


class LiveTraffic:
    """Serves a self-refreshing traffic diagram for `bench` on `host:port`."""

    def __init__(
        self,
        bench: TestBench,
        *,
        port: int = DEFAULT_PORT,
        host: str = "127.0.0.1",
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        self._bench = bench
        self._host = host
        self._port = port
        self._interval = interval
        self._stop = threading.Event()
        self._server: ThreadingHTTPServer | None = None
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        first = traffic.snapshot(bench)
        self._payload: dict[str, Any] = _payload(first, first, interval)

    @property
    def port(self) -> int:
        """The bound port -- the real one once start() has resolved a `port=0`."""
        return self._port

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}/"

    def start(self) -> None:
        """Bind the port and start sampling. Non-blocking."""
        handler = partial(_Handler, self)
        self._server = ThreadingHTTPServer((self._host, self._port), handler)
        self._server.daemon_threads = True
        self._port = int(self._server.server_address[1])  # resolves port=0 to the one bound
        self._threads = [
            threading.Thread(target=self._server.serve_forever, name="live-http", daemon=True),
            threading.Thread(target=self._sample_forever, name="live-sampler", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        """Stop sampling and close the port. Idempotent."""
        self._stop.set()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads = []

    def payload(self) -> dict[str, Any]:
        """The most recent sample, as the JSON object `/traffic.json` serves."""
        with self._lock:
            return self._payload

    def sample(self) -> dict[str, Any]:
        """Take one sample now: the traffic since the previous sample, rendered."""
        previous = self.payload()
        current = traffic.snapshot(self._bench)
        window = current.since(_snapshot_of(previous))
        # A zero-length window (two samples inside one clock tick) would divide by zero
        # dressed up as an infinite rate; show the cumulative view until time has moved.
        shown = window if window.window_s > 0 else current
        with self._lock:
            self._payload = _payload(shown, current, self._interval)
        return self.payload()

    def _sample_forever(self) -> None:
        while not self._stop.wait(self._interval):
            self.sample()


def _payload(shown: traffic.Snapshot, cumulative: traffic.Snapshot, interval: float) -> dict[str, Any]:
    """Build the served object: `shown` is what the page displays (usually one window),
    `cumulative` is carried privately so the next window is measured against raw
    counters rather than against the previous window.
    """
    return {
        **shown.to_dict(),
        "mermaid": traffic.to_mermaid(shown),
        "generated_at": time.time(),
        "interval_s": interval,
        "_snapshot": cumulative.to_dict(),
    }


def _snapshot_of(payload: dict[str, Any]) -> traffic.Snapshot:
    """Rebuild the cumulative snapshot a payload was derived from.

    The payload carries it alongside the (possibly windowed) numbers it displays, so
    every window is measured against cumulative counters rather than against the
    previous window.
    """
    raw = payload["_snapshot"]
    return traffic.Snapshot(
        run_id=raw["run_id"],
        window_s=raw["window_s"],
        buses=tuple(traffic.BusInfo(**bus) for bus in raw["buses"]),
        links=tuple(
            traffic.Link(
                ecu=link["ecu"],
                bus=link["bus"],
                sends=link["sends"],
                receives=link["receives"],
                stats=traffic.BusStats(
                    tx_frames=link["tx_frames"],
                    tx_bytes=link["tx_bytes"],
                    rx_frames=link["rx_frames"],
                    rx_bytes=link["rx_bytes"],
                ),
            )
            for link in raw["links"]
        ),
    )


class _Handler(BaseHTTPRequestHandler):
    def __init__(self, live: LiveTraffic, *args, **kwargs) -> None:
        self._live = live
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's own naming
        if self.path.startswith("/traffic.json"):
            payload = {k: v for k, v in self._live.payload().items() if not k.startswith("_")}
            self._respond(json.dumps(payload).encode("utf-8"), "application/json")
        elif self.path in ("/", "/index.html"):
            self._respond(PAGE.encode("utf-8"), "text/html; charset=utf-8")
        else:
            self.send_error(404)

    def _respond(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        """Silence the per-request stderr line -- a 1 Hz poll would bury the run's own
        output.
        """


PAGE = """<!doctype html>
<meta charset="utf-8">
<title>bench traffic</title>
<style>
  :root { color-scheme: light dark; }
  body { margin: 0; font: 14px system-ui, sans-serif; }
  header { display: flex; gap: 1.5rem; align-items: baseline;
           padding: .6rem 1rem; border-bottom: 1px solid #8884; }
  h1 { font-size: 1rem; margin: 0; }
  .meta { opacity: .7; font-variant-numeric: tabular-nums; }
  #diagram { padding: 1rem; overflow-x: auto; }
  pre { white-space: pre-wrap; }
</style>
<header>
  <h1>bench traffic</h1>
  <span class="meta" id="run"></span>
  <span class="meta" id="window"></span>
</header>
<div id="diagram"><pre id="source">waiting for the first sample...</pre></div>
<script type="module">
  let mermaid = null;
  try {
    mermaid = (await import("https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs")).default;
    mermaid.initialize({ startOnLoad: false, theme: matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "default" });
  } catch (err) {
    // Offline: the diagram source below is the fallback, and carries the same numbers.
  }

  let seq = 0;
  let interval = null;
  async function poll() {
    let data;
    try {
      data = await (await fetch("traffic.json", { cache: "no-store" })).json();
    } catch (err) {
      document.getElementById("window").textContent = "disconnected -- run finished?";
      return;
    }
    interval = data.interval_s;
    document.getElementById("run").textContent = "run " + data.run_id;
    document.getElementById("window").textContent = "last " + data.window_s.toFixed(1) + " s";
    const target = document.getElementById("diagram");
    if (mermaid) {
      const { svg } = await mermaid.render("g" + seq++, data.mermaid);
      target.innerHTML = svg;
    } else {
      target.innerHTML = "<pre></pre>";
      target.firstChild.textContent = data.mermaid;
    }
  }

  await poll();
  setInterval(poll, (interval ?? 1) * 1000);
</script>
"""
