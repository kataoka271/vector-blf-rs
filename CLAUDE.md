# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                          # debug build
cargo build --release                # release build
cargo check                          # fast type-check without linking
cargo test                           # run all tests
cargo test isotp::                   # run tests in a specific module
cargo fmt                            # format code
cargo clippy                         # lint
cargo run -- <input.blf> [options]   # run the CLI
```

### Python bindings (PyO3 + maturin)

```bash
uv run maturin develop --features python   # build & install into the uv venv (dev/editable)
uv run python <script.py>                  # run a script against the installed bindings
```

`maturin` is a dev dependency in `pyproject.toml` and is available via `uv run`.
The `python` Cargo feature gates all PyO3 code; omit it for pure-Rust builds.

## Project Overview

A Rust library and CLI for reading/writing Vector's BLF (Binary Log File) format — the standard for logging automotive network traffic (CAN, CAN-FD, Ethernet, diagnostic protocols).

## Architecture

### Module Layout (`src/blf/`)

| Module | Responsibility |
|--------|---------------|
| `blf.rs` | Core BLF I/O: `Reader<R>` (iterator), `Writer<W>` (builder), `scan_containers()`, `parse_at()` |
| `object.rs` | BLF structural types: `FileHeader` (144-byte header, "LOGG" magic), `BaseObjectHeader` |
| `objtype.rs` | `ObjType` enum with 100+ variants; controls padding alignment |
| `encoder.rs` | `Encoder`/`Decoder` traits over `Read`/`Write`; `Timestamp` (Nanosecond or Microsecond) |
| `error.rs` | `ParseError` enum (Io, Eof, UnexpectedObjType, ZlibError, InvalidData); `ParseResult<T>` |
| `message.rs` | `Message` enum: `Can`, `CanFd`, `CanFd64`, `Ethernet`, `EthernetEx`, etc. |
| `signal.rs` | CAN signal decoding: `Signal` (start_bit, bit_length, scale, offset), `SignalDb`, Intel/Motorola byte order |
| `ip.rs` | IPv4/IPv6 packet parsing |
| `transport.rs` | TCP/UDP parsing |
| `diag/uds.rs` | UDS (ISO 14229) service parsing — 22 services, NRC codes |
| `diag/isotp.rs` | ISO-TP (ISO 15765-2) frame types + `Reassembler`; handles CAN-FD extended frames |
| `diag/doip.rs` | DoIP (ISO 13400-2) tunneling |
| `diag/someip.rs` | SOME/IP (AUTOSAR) middleware |

### Key Data Flow

1. **File scan** — `scan_containers()` indexes `LogContainer` offsets without decompression (fast, enables parallelism).
2. **Parallel parse** — `parse_at()` decompresses and parses individual containers; containers are distributed across threads in `main.rs`.
3. **Boundary check** — validates timestamp monotonicity between thread-assigned chunks.
4. **Message dispatch** — each `BaseObject` holds a `timestamp` and a `Message` variant; callers match on the variant.

### BLF Format Details

- File magic: `"LOGG"` (4 bytes); `FileHeader` is 144 bytes (72 bytes valid).
- `BaseObjectHeader`: 16-byte base + 16-byte extension = 32 bytes total.
- Objects are padded to 4-byte boundaries when `ObjType::is_padding_needed()`.
- `LogContainer` payloads are zlib-deflated (`compression_method = 2`).
- `Timestamp` variants are unified via `ts_ns()` helper.

### CLI (`src/main.rs`)

```
vector-blf-rs <input.blf> [output.blf [repeat] | output.csv [signals.csv]] [--threads N]
```

- BLF→BLF: copy with optional repetition count
- BLF→CSV: raw message export (timestamp_ns, channel, id, ext_id, dir, dlc, data)
- BLF→CSV+signals: decode signals using a DBC-like CSV signal database

### Python Module (`src/python.rs`)

PyO3 bindings exposed as the `vector_blf` Python extension module:

| Python class | Rust source |
|---|---|
| `Reader(path, types=None)` | wraps `blf::Reader<BufReader<File>>`, implements `__iter__`/`__next__`; `types` is an optional list of message type strings to filter in Rust before any Python object is allocated (valid: `"Can"`, `"CanFd"`, `"CanFd64"`, `"Ethernet"`, `"EthernetEx"`, `"Other"`) |
| `BaseObject` | `.timestamp_ns: int`, `.message: Can \| CanFd \| CanFd64 \| Ethernet \| EthernetEx \| None` |
| `Can` | channel, id, is_ext_id, dir, rtr, dlc, data |
| `CanFd` | + fdf, brs, esi |
| `CanFd64` | same as CanFd but channel is u8 |
| `Ethernet` / `EthernetEx` | channel, dir, src_addr, dst_addr, ether_type, data |

`dir` is exposed as a raw `u8` (0=Tx, 1=Rx, 2=TxRq). `Message::Other` variants map to `None`.

### Databricks Integration (`databricks/`)

| File | Purpose |
|------|---------|
| `databricks.yml` | Databricks Asset Bundle (DAB) config — defines the `vector_blf` wheel artifact, pipeline variables, and `dev`/`prod` targets |
| `databricks/dlt_blf_pipeline.py` | Delta Live Tables pipeline: `blf_bronze` (all messages) → `blf_silver_can`, `blf_silver_eth`, `blf_silver_can_signals` (streaming tables via Auto Loader) |
| `databricks/build_wheel.sh` | Builds a manylinux `aarch64` wheel inside Docker using `maturin` + `cargo-zigbuild`; output goes to `dist/` |
| `assets/signals.csv` | Demo signal definitions CSV; upload to the `spec` volume before running the pipeline |

**DAB workflow:**

```bash
databricks bundle validate            # check YAML
databricks bundle deploy              # build wheel, upload, deploy (dev target)
databricks bundle deploy -t prod      # deploy to production
databricks bundle run blf_ingestion   # trigger a pipeline run
databricks bundle destroy             # tear down all managed resources

# Upload signal definitions to the spec volume (dev)
databricks fs cp assets/signals.csv       dbfs:/Volumes/main/blf_dev/spec/signals.csv       --overwrite
databricks fs cp assets/someip_signals.csv dbfs:/Volumes/main/blf_dev/spec/someip_signals.csv --overwrite
```

The pipeline reads `*.blf` files from a Unity Catalog Volume (set via `blf.source_path`), parses them with the `vector_blf` wheel using a pandas UDF, and writes partitioned Delta tables. Signal definitions live in the `spec` volume (`blf.signals_path` for CAN, `blf.someip_signals_path` for SOME/IP); when present, physical values are decoded into `blf_silver_can_signals` and `blf_silver_someip_signals` respectively.

**Volume layout (dev):**

| Volume | Path | Contents |
|--------|------|----------|
| `main.blf_dev.raw` | `/Volumes/main/blf_dev/raw/` | Source `*.blf` files (Auto Loader input) |
| `main.blf_dev.spec` | `/Volumes/main/blf_dev/spec/signals.csv` | CAN signal definitions CSV |
| `main.blf_dev.spec` | `/Volumes/main/blf_dev/spec/someip_signals.csv` | SOME/IP signal definitions CSV |

**Cross-compilation note:** Databricks Serverless runs Linux ARM64 (`aarch64`). `build_wheel.sh` uses Docker + zig to cross-compile from Windows/macOS without a Linux machine.

### Test Fixtures

Integration tests live in `tests/`; BLF test files live in `data/`. The `SharedCursor` helper in `tests/blf_rw.rs` enables in-memory roundtrip tests.

## Performance Testing

There is no criterion benchmark suite. Use the CLI directly against a generated large file, or `bench_python.py` to compare against python-can.

**1. Generate a large BLF file** (repeat a small fixture N times):

```bash
cargo build --release
./target/release/vector-blf-rs data/test_logfile.blf data/bench_large.blf 200000
# → ~33 MB, 3.4M objects
```

**2. Benchmark parse throughput** (the CLI prints timing for each phase):

```bash
# single-threaded
./target/release/vector-blf-rs data/bench_large.blf

# multi-threaded
./target/release/vector-blf-rs data/bench_large.blf --threads 4
```

Output shows scan time (I/O-bound, benefits from BufReader) and parse time (zlib-bound, scales with `--threads`).

**3. Benchmark CSV export** (exercises hex-encoding path):

```bash
./target/release/vector-blf-rs data/bench_large.blf out.csv
```

**Key numbers on a 33 MB / 58K-container file (for regression detection):**

| Phase | Expected |
|-------|----------|
| Scan (1 thread) | ~0.25 s |
| Parse (1 thread) | ~2.3 s |
| Parse (4 threads) | ~1.2 s |
| CSV export | ~0.18 s |

> `data/bench_large.blf` is gitignored. Regenerate it with the command above before benchmarking.

**4. Python benchmark** (vector_blf vs python-can, with/without Rust-side type filter):

```bash
uv run python bench_python.py data/bench_large.blf
```
