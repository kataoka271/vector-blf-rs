# vector-blf-rs

A Rust library and CLI for reading/writing Vector's **BLF (Binary Log File)** format — the standard for logging automotive network traffic (CAN, CAN-FD, Ethernet, and diagnostic protocols).

Also ships **Python bindings** via PyO3 + maturin as the `vector_blf` package.

---

## Features

- Parse BLF files: CAN, CAN-FD, CAN-FD64, Ethernet, EthernetEx
- Parallel decompression of log containers (`--threads N`)
- BLF → BLF copy with optional repetition
- BLF/MF4 → CSV export (raw messages or with signal decoding)
- BLF/MF4 → MF4 export for CAN, CAN-FD, Ethernet, and scalar signals
- CAN signal decoding via a DBC-like CSV signal database
- ISO-TP reassembly, UDS, DoIP, SOME/IP parsing
- Python bindings exposing a simple iterator API

---

## CLI

```
vector-blf-rs parse <input.blf|.mf4|.mdf> [output.blf|.csv|.mf4|.mdf] [--can-signals FILE] [--someip-signals FILE] [--repeat N] [--threads N]
vector-blf-rs convert <input.dbc|.arxml> <output.csv> [--overlay FILE]
vector-blf-rs check <signals.csv>
```

```bash
# Parse and print summary
vector-blf-rs parse data/test_logfile.blf

# Copy a BLF file (repeat 200000x to generate a large benchmark file)
vector-blf-rs parse data/test_logfile.blf data/bench_large.blf --repeat 200000

# Export to CSV
vector-blf-rs parse data/test_logfile.blf out.csv

# Export to MF4
vector-blf-rs parse data/test_logfile.blf out.mf4

# Export to CSV with CAN signal decoding
vector-blf-rs parse data/test_logfile.blf out.csv --can-signals assets/can_signals.csv

# Export to CSV with CAN + SOME/IP signal decoding (multi-threaded)
vector-blf-rs parse data/test_logfile.blf out.csv \
  --can-signals assets/can_signals.csv \
  --someip-signals assets/someip_signals.csv \
  --threads 4

# Convert a DBC file to signal CSV
vector-blf-rs convert network.dbc signals.csv

# Convert an ARXML file with an overlay CSV
vector-blf-rs convert network.arxml signals.csv --overlay overlay.csv

# Validate a signal CSV
vector-blf-rs check assets/can_signals.csv
```

---

## Python bindings

### Installation

```bash
# Build and install into the local uv environment (editable)
uv run maturin develop --features python

# Or build a wheel
uvx maturin build --features python --release
```

### Usage

```python
import vector_blf

for obj in vector_blf.Reader("path/to/file.blf"):
    print(obj.timestamp_ns)          # nanoseconds since file epoch
    if isinstance(obj.message, vector_blf.Can):
        msg = obj.message
        print(f"CAN  ch={msg.channel} id=0x{msg.id:X} data={msg.data.hex()}")
    elif isinstance(obj.message, vector_blf.CanFd):
        msg = obj.message
        print(f"CANFD ch={msg.channel} id=0x{msg.id:X} brs={msg.brs}")
    elif isinstance(obj.message, vector_blf.Ethernet):
        msg = obj.message
        print(f"ETH  ch={msg.channel} ether_type=0x{msg.ether_type:04X}")
```

### Python API

| Class / Function | Fields / Signature |
|---|---|
| `Reader(path, types=None)` | Iterator of `BaseObject` for `.blf`, `.mf4`, `.mdf` files; `types` filters in Rust (e.g. `["Can", "CanFd", "Mf4Signal"]`) |
| `Reader.read_batch(n=50000)` | Returns a column-oriented `dict` ready for `pd.DataFrame`, or `None` at EOF |
| `BaseObject` | `timestamp_ns: int`, `message: Can \| CanFd \| CanFd64 \| Ethernet \| EthernetEx \| Mf4Signal \| None` |
| `Can` | `channel`, `id`, `is_ext_id`, `dir`, `rtr`, `dlc`, `data` |
| `CanFd` | + `fdf`, `brs`, `esi` |
| `CanFd64` | same as `CanFd` but `channel` is `int` (u8) |
| `Ethernet` / `EthernetEx` | `channel`, `dir`, `src_addr`, `dst_addr`, `ether_type`, `data` |
| `Mf4Signal` | `group`, `name`, `value`, `unit` |
| `CanSignalDb(path)` | `.decode(message_id, data)`, `.is_container(message_id)`, `.decode_container(message_id, data, long_header=False)`, `.extract_container_pdus(message_id, data, long_header=False)`, `.decode_pdu(can_id, pdu_id, data)` |
| `SomeIpSignalDb(path)` | `.decode(service_id, method_id, payload)` |
| `IsoTpReassembler()` | `.push(data)` — stateful ISO-TP reassembler; returns `(uds_type, service_id, service_name, nrc, nrc_name, data)` or `None` |
| `parse_uds(data)` | Parse a raw UDS payload; returns `(uds_type, service_id, service_name, nrc, nrc_name, data)` or `None` |
| `parse_someip_udp(ether_type, eth_payload)` | Parse SOME/IP messages from an Ethernet frame payload (IPv4/IPv6, UDP, container PDUs); returns list of dicts |
| `parse_doip_diag(ether_type, eth_payload)` | Parse DoIP DiagMessages; returns `[(src_addr, target_addr, uds_payload)]` |
| `parse_eth_payload_signals(ether_type, eth_payload)` | Extract IP/TCP/UDP header fields as named signals; returns list of dicts |

`dir` is a raw `int` (0 = Tx, 1 = Rx, 2 = TxRq). `Message::Other` variants (unsupported object types) appear as `None`.

---

## Databricks DLT pipeline

`databricks/dlt_blf_pipeline.py` is a Delta Live Tables pipeline that streams
`*.blf` files from a Unity Catalog Volume into structured Delta tables.

```
Auto Loader (*.blf → file paths)
         ↓  vector_blf Rust UDF
blf_bronze              — all message types, one row per log object
         ↓ filter
blf_silver_can          — CAN / CAN-FD / CAN-FD64 (+ data_hex, timestamp_s, dir string)
blf_silver_eth          — Ethernet / EthernetEx   (+ formatted MACs, ether_type_hex)
blf_silver_can_signals  — decoded physical signal values (long format, one row per signal)
```

Auto Loader tracks which files have been processed, so only new BLF files are
ingested on each run (exactly-once, incremental).

Signal definitions are loaded from a CSV at `blf.signals_path` (see Pipeline parameters). `assets/can_signals.csv` in this repo contains a demo set covering engine, vehicle dynamics, battery, and ambient signals.

### Deploy with Databricks Asset Bundles

`databricks.yml` at the repo root automates the full deploy workflow:

```bash
# One-time setup
databricks configure           # set workspace host + token

# Build the Linux wheel, upload, and create/update the DLT pipeline
databricks bundle deploy       # → dev target (default)
databricks bundle deploy -t prod

# Trigger a pipeline run
databricks bundle run blf_ingestion

# Tear down managed resources
databricks bundle destroy -t prod
```

`bundle deploy` runs `maturin build --release --features python --manylinux auto`
locally (requires Docker Desktop on Windows/macOS), uploads the resulting wheel
as a cluster library, and deploys the pipeline via the Databricks REST API.

**Without Docker** — use Zig-based cross-compilation instead:

```bash
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu
maturin build --release --features python --target x86_64-unknown-linux-gnu --zig
```

Then update the `artifacts.vector_blf.build` line in `databricks.yml` to match.

### Pipeline parameters

| Parameter | Default | Description |
|---|---|---|
| `blf.source_path` | — | Volume path containing `*.blf` files (required) |
| `blf.target_catalog` | `main` | Unity Catalog output catalog |
| `blf.target_schema` | `blf` | Unity Catalog output schema |
| `blf.signals_path` | `""` | Volume path to signal definitions CSV; if empty, `blf_silver_can_signals` is empty |

Signal CSV format (header required):

```
message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
0x64,ambient_temp,0,8,Intel,true,1.0,-40.0
```

`message_id` accepts hex (`0x…`) or decimal. `byte_order` is `Intel` or `Motorola`. Upload to the `signals` volume before running:

```bash
databricks fs cp assets/can_signals.csv dbfs:/Volumes/main/blf_dev/signals/can_signals.csv --overwrite
```

---

## Benchmark: CLI parse throughput

Measured on a 32 MB BLF file (~3.4 M objects, 58,824 containers). Run with `--quiet` to suppress table output.

| Phase | 1 thread | 4 threads |
|---|---:|---:|
| Scan | 0.28 s | 0.26 s |
| Parse | 2.25 s | 1.29 s |

```bash
vector-blf-rs parse data/bench_large.blf --quiet
vector-blf-rs parse data/bench_large.blf --threads 4 --quiet
```

---

## Benchmark: vector_blf vs python-can

Measured on a 33 MB BLF file (~3.4 M objects, generated by repeating a fixture 200,000×). Best of 3 runs.

| Library | Messages read | Time | Throughput | MB/s |
|---|---:|---:|---:|---:|
| python-can 4.6 | 400,000 | 5.255 s | 76,123 msg/s | 6.3 |
| **vector_blf (all)** | **3,400,000** | **1.134 s** | **2,997,424 msg/s** | **29.1** |
| vector_blf (CAN only) | 200,000 | 1.618 s | 123,615 msg/s | 20.4 |

**~39× higher throughput** in raw msg/s; **~4.6× faster wall-clock** time even though `vector_blf` processes 8.5× more objects per run.

### Notes on the comparison

- **python-can `BLFReader`** surfaces only CAN/CAN-FD messages; non-CAN objects (Ethernet, etc.) are silently skipped, hence the lower message count.
- **`vector_blf` (all)** decodes every object type including Ethernet, so the 3.4 M figure is the true object count in the file.
- **`vector_blf` (CAN only)** filters to `Can | CanFd | CanFd64` after full decode, showing the cost of Python-side `isinstance` dispatch at scale.
- The speedup is primarily from Rust's zero-copy zlib decompression path vs. python-can's pure-Python parsing loop.

Reproduce with:

```bash
# Generate bench file (~33 MB)
cargo build --release
./target/release/vector-blf-rs parse data/test_logfile.blf data/bench_large.blf --repeat 200000

# Run CLI benchmark
./target/release/vector-blf-rs parse data/bench_large.blf --quiet
./target/release/vector-blf-rs parse data/bench_large.blf --threads 4 --quiet

# Run Python benchmark
uv run python scripts/bench_python.py data/bench_large.blf
```

---

## Development

```bash
cargo build                          # debug build
cargo test                           # run all tests
cargo clippy && cargo fmt            # lint + format

uv run maturin develop --features python   # rebuild Python extension
uv run python bench_python.py             # run benchmark
```
