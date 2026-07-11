"""
Benchmark: CanSignalDb.decode() / SomeIpSignalDb.decode() throughput as the
number of signal definitions per message ID grows.

CanSignalDb/SomeIpSignalDb cache one interned Python str per distinct signal
name at construction time (see CanSignalDb::interned_name in src/python.rs),
so a repeated decode of the same signal reuses the same str object instead of
allocating a fresh PyString on every call. This keeps decode() throughput
roughly flat as signals-per-ID grows, rather than degrading with the extra
per-signal Python object churn.

An earlier version of this script compared decode() against a batched
decode_batch() call that moved the whole pandas_udf row loop into Rust. That
approach was measured to be *slower* than the plain per-row loop -- PyO3's
conversion of a nested Vec<Vec<(Py<PyString>, f64, Option<String>)>> back into
Python is more expensive than doing the equivalent number of smaller
conversions across many calls -- so the pipeline UDFs in
databricks/blf-pipeline/dlt_blf_pipeline.py were kept as a per-row Python
for-loop; only the string-interning change was kept.

Usage:
    uv run python scripts/bench_signal_decode.py [rows] [signals_per_id]
"""

import sys
import tempfile
import time

import vector_blf

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
SIGNALS_PER_ID = int(sys.argv[2]) if len(sys.argv) > 2 else 200
RUNS = 5

CAN_ID = 0x100
SERVICE_ID = 0x64
METHOD_ID = 0x01


def _make_can_csv(n_signals: int) -> str:
    # n_signals 2-bit Intel signals packed back-to-back into a CAN-FD payload.
    lines = ["message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset"]
    bit = 0
    for i in range(n_signals):
        start_byte, start_bit = divmod(bit, 8)
        lines.append(f"0x{CAN_ID:X},Sig{i},{start_byte},{start_bit},2,Intel,false,1.0,0.0")
        bit += 2
    return "\n".join(lines) + "\n"


def _make_someip_csv(n_signals: int) -> str:
    lines = ["service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset"]
    bit = 0
    for i in range(n_signals):
        start_byte, start_bit = divmod(bit, 8)
        lines.append(f"0x{SERVICE_ID:04X},0x{METHOD_ID:04X},Sig{i},{start_byte},{start_bit},2,Intel,false,1.0,0.0")
        bit += 2
    return "\n".join(lines) + "\n"


def _write_csv(content: str) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    return f.name


def bench_can(n_signals: int, rows: int) -> None:
    payload_bytes = (n_signals * 2 + 7) // 8
    data = bytes((i * 37) % 256 for i in range(max(payload_bytes, 8)))

    csv_path = _write_csv(_make_can_csv(n_signals))
    db = vector_blf.CanSignalDb(csv_path)

    # warm-up
    for _ in range(100):
        db.decode(CAN_ID, data)

    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        for _ in range(rows):
            [
                {"signal_name": name, "signal_value": value, "signal_str": cat}
                for name, value, cat in db.decode(CAN_ID, data)
            ]
        times.append(time.perf_counter() - t0)

    best = min(times)
    print(f"CanSignalDb.decode()     -- {rows:,} rows x {n_signals} signals/ID")
    print(f"  {rows / best:>12,.0f} rows/s   {best * 1000 / rows:>7.3f} ms/1k rows")


def bench_someip(n_signals: int, rows: int) -> None:
    payload_bytes = (n_signals * 2 + 7) // 8
    payload = bytes((i * 37) % 256 for i in range(max(payload_bytes, 8)))

    csv_path = _write_csv(_make_someip_csv(n_signals))
    db = vector_blf.SomeIpSignalDb(csv_path)

    # warm-up
    for _ in range(100):
        db.decode(SERVICE_ID, METHOD_ID, payload)

    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        for _ in range(rows):
            [
                {"signal_name": name, "signal_value": value, "signal_str": cat}
                for name, value, cat in db.decode(SERVICE_ID, METHOD_ID, payload)
            ]
        times.append(time.perf_counter() - t0)

    best = min(times)
    print(f"SomeIpSignalDb.decode()  -- {rows:,} rows x {n_signals} signals/ID")
    print(f"  {rows / best:>12,.0f} rows/s   {best * 1000 / rows:>7.3f} ms/1k rows")


print(f"decode() throughput  --  {ROWS:,} rows, {SIGNALS_PER_ID} signals/ID, best of {RUNS} runs\n")
bench_can(SIGNALS_PER_ID, ROWS)
print()
bench_someip(SIGNALS_PER_ID, ROWS)
