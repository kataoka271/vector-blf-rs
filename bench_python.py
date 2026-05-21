"""
Benchmark: python-can BLFReader vs vector_blf Reader

Usage:
    uv run python bench_python.py [path/to/file.blf]
"""

import sys
import time
import os

BLF_FILE = sys.argv[1] if len(sys.argv) > 1 else "data/bench_large.blf"
file_size_mb = os.path.getsize(BLF_FILE) / 1024 / 1024
RUNS = 3


def bench(label, fn):
    # warm-up
    fn()
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        count = fn()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"  {label:<22} {count:>9,} msgs  {best:.3f}s  {count/best:>12,.0f} msg/s  {file_size_mb/best:>6.1f} MB/s")
    return best


# ── python-can ───────────────────────────────────────────────────────────────

def run_python_can():
    import can
    count = 0
    with can.BLFReader(BLF_FILE) as reader:
        for _ in reader:
            count += 1
    return count


# ── vector_blf ───────────────────────────────────────────────────────────────

def run_vector_blf():
    import vector_blf
    count = 0
    for _ in vector_blf.Reader(BLF_FILE):
        count += 1
    return count


def run_vector_blf_can_only():
    import vector_blf
    count = 0
    for obj in vector_blf.Reader(BLF_FILE):
        if isinstance(obj.message, (vector_blf.Can, vector_blf.CanFd, vector_blf.CanFd64)):
            count += 1
    return count


# ── main ─────────────────────────────────────────────────────────────────────

print(f"File : {BLF_FILE}  ({file_size_mb:.1f} MB)")
print(f"Runs : {RUNS} (best of {RUNS} reported)\n")

t_can = bench("python-can", run_python_can)
t_vblf = bench("vector_blf (all)", run_vector_blf)
t_vblf_can = bench("vector_blf (CAN only)", run_vector_blf_can_only)

print(f"\n  Speedup (all msgs) : {t_can/t_vblf:.1f}x")
