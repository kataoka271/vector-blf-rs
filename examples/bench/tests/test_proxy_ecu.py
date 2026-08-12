"""Tests for ProxyEcu: a UDP datagram carrying a JSON-encoded Frame gets forwarded onto
its bus. port=0 asks the OS for a free port so this test never collides with anything
else listening locally; the test reads the actual bound port back off the socket before
sending to it.
"""

from __future__ import annotations

import json
import socket
import threading
import time

from bench.bus import Loopback
from bench.ecu import Ecu, ProxyEcu
from bench.frame import make_can_frame

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000


def test_proxy_ecu_forwards_a_udp_datagram_onto_its_bus():
    bus = Loopback(name="a")
    bus.channel = 1

    proxy = ProxyEcu(ch=bus, port=0, host="127.0.0.1")
    proxy._bind(run_id=RUN_ID, run_epoch_ns=EPOCH_NS)

    sink = Ecu("sink")
    sink._bind(run_id=RUN_ID, run_epoch_ns=EPOCH_NS)
    received = []
    sink.handle(bus).on()(received.append)
    sink.handle(bus).subscribe()

    thread = threading.Thread(target=proxy.run, kwargs={"duration": 1.0, "poll_timeout": 0.05})
    thread.start()
    try:
        deadline = time.monotonic() + 1.0
        while proxy._sock is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert proxy._sock is not None
        port = proxy._sock.getsockname()[1]

        frame = make_can_frame(
            run_id=RUN_ID,
            source_file="ignored",  # ProxyEcu re-stamps run_id/source_file on receipt
            run_epoch_ns=EPOCH_NS,
            channel=99,  # ProxyEcu re-stamps channel too -- see the assertion below
            can_id=0x310,
            data=b"\x01\x02",
        )
        payload = json.dumps(frame.to_json_obj()).encode("utf-8")
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            client.sendto(payload, ("127.0.0.1", port))
        finally:
            client.close()

        deadline = time.monotonic() + 1.0
        while not received and time.monotonic() < deadline:
            sink.handle(bus).drain(timeout=0.05)
    finally:
        thread.join(timeout=2.0)
        sink.close()

    assert len(received) == 1
    got = received[0]
    assert got.can_id == 0x310
    assert got.data == b"\x01\x02"
    assert got.channel == bus.channel
    assert got.run_id == RUN_ID
