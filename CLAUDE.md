# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commit Messages

- Use only ASCII characters. Do not use arrows (`→`, `–`, `—`), emoji, or other non-ASCII symbols.

## Code Style

- Prefer Rust 2018 module style: use `foo.rs` alongside a `foo/` directory for submodules, not `foo/mod.rs`.
- Update `vector_blf.pyi` after editing `src/python.rs`.
- Do not use `#[allow(...)]` directives without any permissions.

### After editing Rust files

```bash
cargo fmt                     # formatter
cargo clippy --all-targets    # linter
cargo check                   # type-check
```

### After editing Python files

```bash
uv run ruff format <file>                    # formatter
uv run ruff check <file>                     # linter
uv run ruff check --select I --fix <file>    # import organizer
uv run ty check                              # type checker
```

## Commands

```bash
cargo build                          # debug build
cargo build --release                # release build
cargo check                          # fast type-check without linking
cargo test                           # run all tests
cargo test isotp::                   # run tests in a specific module
cargo fmt                            # format code
cargo clippy --all-targets           # lint
cargo run -- parse <input.blf> [options]   # run the CLI
```

**`python` should be run via `uv run`**
```bash
uv run python <script.py>
```

### Python bindings (PyO3 + maturin)

```bash
uv run maturin develop --features python   # build & install into the uv venv (dev/editable)
uv run python <script.py>                  # run a script against the installed bindings
```

`maturin` is a dev dependency in `pyproject.toml` and is available via `uv run`.
The `python` Cargo feature gates all PyO3 code; omit it for pure-Rust builds.

**Gotcha: `uv` overwrites `maturin develop` on every `uv run` invocation.**
`uv` caches the built wheel keyed on the source directory mtime, which does not
change when only file *contents* are modified (no files added/removed). As a
result, every subsequent `uv run <cmd>` silently restores the old cached binary,
undoing the `maturin develop` build.

After editing Rust source files, use this instead of `maturin develop`:

```bash
uv sync --reinstall-package vector-blf    # force uv to rebuild from source and update its cache
```

This rebuilds through uv's own pipeline so the new binary is cached correctly
and will not be reverted by future `uv run` calls.

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
| `signal.rs` | CAN signal decoding: `Signal` (start_bit, bit_length, scale, offset), `CanSignalDb` (CSV: start_byte, start_bit, pdu_id), `SomeIpSignalDb`, `ChannelDb`, Intel/Motorola byte order |
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
vector-blf-rs parse <input.blf|.mf4|.mdf> [output.blf|.csv|.mf4|.mdf] [--can-signals FILE] [--someip-signals FILE] [--channels FILE] [--repeat N] [--threads N] [--pdu-list] [-q]
vector-blf-rs convert <input.dbc|.arxml> <output.csv> [--overlay <overlay.csv>]
vector-blf-rs check <signals.csv>
```

- `parse`: BLF/MF4/MDF input; BLF/MF4/MDF/CSV output; `--channels` maps channel numbers to names; `--pdu-list` prints per-PDU occurrence summary; `-q` suppresses table output
- `convert`: DBC/ARXML → signal CSV, with optional overlay merge
- `check`: validate a CAN or SOME/IP signal CSV

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
| `CanSignalDb(path)` | loads a CAN signal CSV; `.decode(message_id: int, data: bytes) -> list[tuple[str, float]]` |
| `SomeIpSignalDb(path)` | loads a SOME/IP signal CSV; `.decode(service_id: int, method_id: int, payload: bytes) -> list[tuple[str, float]]` |

`dir` is exposed as a raw `u8` (0=Tx, 1=Rx, 2=TxRq). `Message::Other` variants map to `None`.
`CanSignalDb` and `SomeIpSignalDb` use the same Intel/Motorola bit-extraction logic as `signal.rs`; the DLT pipeline delegates to these rather than reimplementing in Python.

### Databricks Integration (`databricks/`)

| File | Purpose |
|------|---------|
| `databricks.yml` | Databricks Asset Bundle (DAB) config — defines the `vector_blf` wheel artifact, pipeline variables, and `dev`/`prod` targets |
| `databricks/blf-pipeline/dlt_blf_pipeline.py` | Delta Live Tables pipeline (`blf_ingestion`, continuous streaming): `blf_bronze` → `blf_silver_can` / `blf_silver_eth` / `blf_silver_mf4_signals` / `blf_silver_diag` → signal tables (`blf_silver_can_signals`, `blf_silver_can_container_pdus`, `blf_silver_eth_signals`, `blf_silver_someip`, `blf_silver_someip_signals`) → `blf_gold_signals` |
| `databricks/signal-docs-pipeline/signal_docs_pipeline.py` | Separate, triggered DLT pipeline (`signal_docs`): ingests PDF/DOCX/PPTX/XLSX signal documentation from a Volume, extracting per-page/slide/sheet/document text into `blf_signal_doc_sections` (optionally enriched with an `ai_query()`-derived `semantic_summary` column) for Genie Space grounding. Shares no lineage with `blf_gold_signals`; kept separate so it doesn't inherit the main pipeline's `continuous: true` in prod |
| `databricks/signal-viewer/app.py` | Plotly Dash visualization app (Databricks App); reads from `blf_gold_signals`; features signal checklist, time-range slider, GPS map, AgGrid data table |
| `databricks/blf-pipeline/build_wheel.sh` | Builds a manylinux `aarch64` wheel inside Docker using `maturin` + `cargo-zigbuild`; output goes to `dist/` |
| `assets/can_signals.csv` | Demo signal definitions CSV; upload to the `signals` volume before running the pipeline |

**DAB workflow:**

```bash
databricks bundle validate            # check YAML
databricks bundle deploy              # build wheel, upload, deploy (dev target)
databricks bundle deploy -t prod      # deploy to production
databricks bundle run blf_ingestion   # trigger a BLF ingestion pipeline run
databricks bundle run signal_docs     # trigger a signal-doc ingestion pipeline run (after uploading new docs)
databricks bundle destroy             # tear down all managed resources

# Upload signal definitions to the signals volume (dev)
databricks fs cp assets/can_signals.csv       dbfs:/Volumes/main/blf_dev/signals/can_signals.csv       --overwrite
databricks fs cp assets/someip_signals.csv dbfs:/Volumes/main/blf_dev/signals/someip_signals.csv --overwrite
```

The `blf_ingestion` pipeline reads `*.blf` files from a Unity Catalog Volume (set via `blf.source_path`), parses them with the `vector_blf` wheel using a pandas UDF, and writes partitioned Delta tables. Signal definitions live in the `signals` volume (`blf.signals_path` for CAN, `blf.someip_signals_path` for SOME/IP); when present, physical values are decoded into `blf_silver_can_signals` and `blf_silver_someip_signals` respectively. The separate `signal_docs` pipeline (triggered, not continuous) reads `blf.signal_docs_path` independently and has no dependency on `blf_ingestion`.

**Volume layout (dev):**

| Volume | Path | Contents |
|--------|------|----------|
| `main.blf_dev.raw` | `/Volumes/main/blf_dev/raw/` | Source `*.blf` files (Auto Loader input) |
| `main.blf_dev.signals` | `/Volumes/main/blf_dev/signals/can_signals.csv` | CAN signal definitions CSV |
| `main.blf_dev.signals` | `/Volumes/main/blf_dev/signals/someip_signals.csv` | SOME/IP signal definitions CSV |
| `main.blf_dev.signals` | `/Volumes/main/blf_dev/signals/docs/*.{pdf,docx,pptx,xlsx}` | Signal documentation files (Auto Loader input for `blf_signal_doc_sections`) |

**Cross-compilation note:** Databricks Serverless runs Linux ARM64 (`aarch64`). `build_wheel.sh` uses Docker + zig to cross-compile from Windows/macOS without a Linux machine.

### Test Fixtures

Integration tests live in `tests/`; BLF test files live in `data/`. The `SharedCursor` helper in `tests/blf_rw.rs` enables in-memory roundtrip tests.

## Performance Testing

There is no criterion benchmark suite. Use the CLI directly against a generated large file, or `scripts/bench_python.py` to compare against python-can.

**1. Generate a large BLF file** (repeat a small fixture N times):

```bash
cargo build --release
./target/release/vector-blf-rs parse data/test_logfile.blf data/bench_large.blf --repeat 200000
# → ~33 MB, 3.4M objects
```

**2. Benchmark parse throughput** (use `--quiet` to suppress table output and get clean timing lines):

```bash
# single-threaded
./target/release/vector-blf-rs parse data/bench_large.blf --quiet

# multi-threaded
./target/release/vector-blf-rs parse data/bench_large.blf --threads 4 --quiet
```

Output shows scan time (I/O-bound, benefits from BufReader) and parse time (zlib-bound, scales with `--threads`).

**3. Benchmark CSV export** (exercises hex-encoding path):

```bash
./target/release/vector-blf-rs parse data/bench_large.blf out.csv
```

**Key numbers on a 32 MB / 58K-container file (for regression detection):**

| Phase | Expected |
|-------|----------|
| Scan (1 thread) | ~0.28 s |
| Parse (1 thread) | ~2.25 s |
| Parse (4 threads) | ~1.3 s |
| CSV export | ~0.18 s |

> `data/bench_large.blf` is gitignored. Regenerate it with the command above before benchmarking.

**4. Python benchmark** (vector_blf vs python-can, with/without Rust-side type filter):

```bash
uv run python scripts/bench_python.py data/bench_large.blf
```
