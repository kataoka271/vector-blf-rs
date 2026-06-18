"""
Benchmark: parse_eth_payload_signals across different EtherType / protocol paths.

Usage:
    uv run python scripts/bench_eth_signals.py [iterations]

Measures per-call cost of each code path so callers know the hot paths and
where the new ARP / IGMP branches add overhead versus plain IP/TCP frames.
"""

import struct
import sys
import time

import vector_blf

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
RUNS = 5


# ── synthetic payloads ────────────────────────────────────────────────────────


def _ipv4_header(protocol: int, src=(192, 168, 1, 1), dst=(10, 0, 0, 1), payload_len=0):
    total = 20 + payload_len
    return struct.pack(
        "!BBHHHBBH4s4s",
        0x45,
        0x00,  # version/ihl, dscp/ecn
        total,  # total length
        0x0001,  # id
        0x0000,  # flags/frag
        64,
        protocol,  # ttl, protocol
        0x0000,  # checksum (not validated)
        bytes(src),
        bytes(dst),
    )


# IPv4 / TCP  (SYN, no data)
_TCP = struct.pack(
    "!HHIIBBHHH",
    1234,
    80,  # src/dst port
    0,  # seq
    0,  # ack
    (5 << 4),
    0x02,  # data offset=5, flags=SYN
    65535,  # window
    0,
    0,  # checksum, urgent
)
PAYLOAD_IPV4_TCP = _ipv4_header(6, payload_len=len(_TCP)) + _TCP

# IPv4 / UDP  (8-byte header + 4-byte payload)
_UDP_DATA = b"\x00" * 4
_UDP = struct.pack("!HHHH", 5000, 6000, 8 + len(_UDP_DATA), 0) + _UDP_DATA
PAYLOAD_IPV4_UDP = _ipv4_header(17, payload_len=len(_UDP)) + _UDP

# IPv4 / IGMP  (V2 membership report, 8-byte body)
_IGMP = struct.pack("!BBH4s", 0x16, 0, 0, bytes([239, 1, 1, 1]))
PAYLOAD_IPV4_IGMP = _ipv4_header(2, payload_len=len(_IGMP)) + _IGMP

# ARP  (EtherType 0x0806, request, 28 bytes)
PAYLOAD_ARP = struct.pack(
    "!HHBBH6s4s6s4s",
    1,
    0x0800,
    6,
    4,  # htype, ptype, hlen, plen
    1,  # op = request
    bytes([0x00, 0x11, 0x22, 0x33, 0x44, 0x55]),  # sender MAC
    bytes([192, 168, 1, 100]),  # sender IP
    bytes([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),  # target MAC (broadcast)
    bytes([192, 168, 1, 1]),  # target IP
)

# Unrecognised EtherType (early return before any parsing)
PAYLOAD_UNKNOWN = b"\x00" * 32


# ── benchmark harness ─────────────────────────────────────────────────────────


def bench(label: str, ether_type: int, payload: bytes):
    fn = vector_blf.parse_eth_payload_signals
    # warm-up
    for _ in range(1000):
        fn(ether_type, payload)

    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        for _ in range(N):
            fn(ether_type, payload)
        times.append(time.perf_counter() - t0)

    best = min(times)
    calls_per_sec = N / best
    ns_per_call = best / N * 1e9
    print(f"  {label:<28}  {calls_per_sec:>10,.0f} calls/s  {ns_per_call:>7.1f} ns/call")


print(f"parse_eth_payload_signals  --  {N:,} iterations, best of {RUNS} runs\n")
bench("IPv4/TCP", 0x0800, PAYLOAD_IPV4_TCP)
bench("IPv4/UDP", 0x0800, PAYLOAD_IPV4_UDP)
bench("IPv4/IGMP", 0x0800, PAYLOAD_IPV4_IGMP)
bench("ARP", 0x0806, PAYLOAD_ARP)
bench("unknown EtherType", 0xFFFF, PAYLOAD_UNKNOWN)
