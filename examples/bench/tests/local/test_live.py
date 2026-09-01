"""The live traffic server: a real socket, a real HTTP round trip, and the windowing
that turns cumulative counters into a per-interval rate.

Binds an ephemeral port on the loopback interface; nothing leaves this machine.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

import pytest
from bench.bench import TestBench
from bench.bus import Loopback
from bench.ecu import Ecu
from bench.live import LiveTraffic

RUN_ID = "run_live"


@pytest.fixture
def bench_and_sender():
    """A running bench with one Ecu the test transmits through itself.

    Really started, not just built: rates are measured against `TestBench.elapsed`, which
    only advances once a run is under way.
    """
    bench = TestBench(run_id=RUN_ID)
    bus = bench.add_bus(Loopback(name="bus1"))
    sender = Ecu("sender")
    bench.add_ecu(sender)
    handle = sender.handle(bus)
    bench.start(duration=10.0)
    yield bench, handle
    bench.stop()


@pytest.fixture
def live(bench_and_sender):
    bench, _handle = bench_and_sender
    # A long interval keeps the sampler thread out of the way: every test here takes its
    # samples by calling sample() itself, so windows are the test's, not the thread's.
    server = LiveTraffic(bench, port=0, interval=60.0)
    server.start()
    yield server
    server.stop()


def _get(url: str) -> tuple[str, str]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8"), response.headers["Content-Type"]


def test_the_index_page_is_served(live):
    body, content_type = _get(live.url)
    assert content_type.startswith("text/html")
    assert "traffic.json" in body


def test_traffic_json_carries_the_current_diagram(live, bench_and_sender):
    _bench, handle = bench_and_sender
    handle.send_can(0x310, b"\x01\x02\x03\x04")
    live.sample()

    body, content_type = _get(live.url + "traffic.json")
    payload = json.loads(body)
    assert content_type == "application/json"
    assert payload["run_id"] == RUN_ID
    assert payload["mermaid"].startswith("flowchart LR")
    assert next(link for link in payload["links"] if link["ecu"] == "sender")["tx_frames"] == 1


def test_a_sample_reports_only_the_traffic_of_its_own_window(live, bench_and_sender):
    _bench, handle = bench_and_sender
    handle.send_can(0x310, b"\x01")
    live.sample()
    handle.send_can(0x311, b"\x02")
    time.sleep(0.05)  # the run clock has coarse resolution; a zero-length window is cumulative
    payload = live.sample()

    assert next(link for link in payload["links"] if link["ecu"] == "sender")["tx_frames"] == 1


def test_an_unknown_path_is_a_404(live):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(live.url + "nope")
    assert exc.value.code == 404


def test_stop_releases_the_port(live):
    live.stop()
    with pytest.raises(OSError):
        _get(live.url)
