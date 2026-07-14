# vector-blf-rs

A Rust library and CLI for reading/writing Vector's **BLF (Binary Log File)** format — the standard for logging automotive network traffic (CAN, CAN-FD, Ethernet, and diagnostic protocols).

Also ships **Python bindings** via PyO3 + maturin as the `vector_blf` package.

---

## Features

- Parse BLF/MF4/MDF files: CAN, CAN-FD, CAN-FD64, Ethernet, EthernetEx, MF4 scalar signals
- Support for BLF files without LogContainer wrappers (direct mode)
- Parallel decompression of log containers (`--threads N`)
- BLF → BLF copy with optional repetition
- BLF/MF4 → CSV export (raw messages or with signal decoding; includes `absolute_timestamp` column)
- BLF/MF4 → MF4 export for CAN, CAN-FD, Ethernet, and scalar signals
- BLF/MF4 → Perfetto native trace export (`.perfetto-trace`) — signals as counter tracks, viewable in [ui.perfetto.dev](https://ui.perfetto.dev)
- CAN signal decoding via a DBC-like CSV signal database (`--can-signals`)
- Enum/categorical value decoding via a CSV mapping raw values to category strings (`--enum-signals`)
- I-PDU container frame demultiplexing (CAN-FD IPduM)
- Channel number → name mapping via a CSV (`--channels`)
- Per-PDU occurrence summary (`--pdu-list`)
- ISO-TP reassembly, UDS, DoIP, SOME/IP parsing
- `check` auto-detects CAN, SOME/IP, or enum signal CSVs from their header row
- Python bindings exposing a simple iterator API, a column-oriented batch reader, and in-memory `from_rows` signal-DB constructors (bypassing CSV)

---

## CLI

```
vector-blf-rs parse <input.blf|.mf4|.mdf> [output.blf|.csv|.mf4|.mdf|.perfetto-trace] [--can-signals FILE] [--someip-signals FILE] [--enum-signals FILE] [--channels FILE] [--repeat N] [--threads N] [--pdu-list] [-q]
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

# Export to CSV with channel name mapping
vector-blf-rs parse data/test_logfile.blf out.csv --channels assets/channels.csv

# Export to CSV with CAN + SOME/IP signal decoding (multi-threaded)
vector-blf-rs parse data/test_logfile.blf out.csv \
  --can-signals assets/can_signals.csv \
  --someip-signals assets/someip_signals.csv \
  --threads 4

# Print per-PDU occurrence summary (requires --can-signals)
vector-blf-rs parse data/test_logfile.blf --can-signals assets/can_signals.csv --pdu-list

# Export to CSV with enum/categorical value decoding
vector-blf-rs parse data/test_logfile.blf out.csv \
  --can-signals assets/can_signals.csv \
  --enum-signals assets/enum_signals.csv

# Export to a Perfetto native trace (signals as counter tracks; view at ui.perfetto.dev)
vector-blf-rs parse data/test_logfile.blf out.perfetto-trace --can-signals assets/can_signals.csv

# Convert a DBC file to signal CSV
vector-blf-rs convert network.dbc signals.csv

# Convert an ARXML file with an overlay CSV
vector-blf-rs convert network.arxml signals.csv --overlay overlay.csv

# Validate a signal CSV (auto-detects CAN, SOME/IP, or enum format from the header)
vector-blf-rs check assets/can_signals.csv
```

### Channel name mapping CSV

Map channel numbers to human-readable names in CSV output (header required):

```
type,channel,name
CAN,1,CAN_HS
CAN,2,CAN_LS
Ethernet,1,ETH_BACKBONE
```

`type` is case-insensitive: `CAN` or `Ethernet`. `channel` is a decimal integer.

### Enum/categorical value CSV

Map a signal's raw decoded value to a human-readable category string (header required):

```
signal_name,raw_value,category
GearPosition,0,Neutral
GearPosition,1,First
IgnitionStatus,0,Off
IgnitionStatus,1,On
```

`raw_value` is the raw bit-extracted integer **before** scale/offset is applied; accepts hex (`0x...`) or decimal. Pass with `--enum-signals` alongside `--can-signals`/`--someip-signals`.

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
| `Reader.start_time_ns()` | Recording start time as nanoseconds since the Unix epoch (BLF file header / MF4 HD block); `0` if absent |
| `Reader.read_batch(n=50000)` | Returns a column-oriented `dict` ready for `pd.DataFrame`, or `None` at EOF |
| `BaseObject` | `timestamp_ns: int`, `message: Can \| CanFd \| CanFd64 \| Ethernet \| EthernetEx \| Mf4Signal \| None` |
| `Can` | `channel`, `id`, `is_ext_id`, `dir`, `rtr`, `dlc`, `data` |
| `CanFd` | + `fdf`, `brs`, `esi` |
| `CanFd64` | same as `CanFd` but `channel` is `int` (u8) |
| `Ethernet` / `EthernetEx` | `channel`, `dir`, `src_addr`, `dst_addr`, `ether_type`, `data` |
| `Mf4Signal` | `group`, `name`, `value`, `unit` |
| `CanSignalDb(path, enum_path=None)` | `.decode(message_id, data)`, `.is_container(message_id)`, `.decode_container(message_id, data, long_header=False)`, `.extract_container_pdus(message_id, data, long_header=False)`, `.decode_pdu(can_id, pdu_id, data)` |
| `CanSignalDb.from_rows(rows, enum_rows=None)` | Static constructor from in-memory `(message_id, signal_name, start_byte, start_bit, bit_length, byte_order, is_signed, scale, offset, pdu_id)` tuples, bypassing CSV |
| `SomeIpSignalDb(path, enum_path=None)` | `.decode(service_id, method_id, payload)` |
| `SomeIpSignalDb.from_rows(rows, enum_rows=None)` | Static constructor from in-memory `(service_id, method_id, signal_name, start_byte, start_bit, bit_length, byte_order, is_signed, scale, offset)` tuples, bypassing CSV |
| `ChannelDb(path)` | `.name(type, channel)` — maps `(type, channel_number)` to a channel name, or `None` |
| `IsoTpReassembler()` | `.push(data)` — stateful ISO-TP reassembler; returns `(uds_type, service_id, service_name, nrc, nrc_name, data)` or `None` |
| `parse_uds(data)` | Parse a raw UDS payload; returns `(uds_type, service_id, service_name, nrc, nrc_name, data)` or `None` |
| `parse_someip_udp(ether_type, eth_payload)` | Parse SOME/IP messages from an Ethernet frame payload (IPv4/IPv6, UDP, container PDUs); returns list of dicts |
| `parse_doip_diag(ether_type, eth_payload)` | Parse DoIP DiagMessages; returns `[(src_addr, target_addr, uds_payload)]` |
| `parse_eth_payload_signals(ether_type, eth_payload)` | Extract IP/TCP/UDP (and ARP/IGMP) header fields as named signals; returns list of dicts |

`dir` is a raw `int` (0 = Tx, 1 = Rx, 2 = TxRq). `Message::Other` variants (unsupported object types) appear as `None`. `CanSignalDb.decode` / `SomeIpSignalDb.decode` return `(signal_name, value, category)` tuples, where `category` is a string when an enum mapping was supplied (via `enum_path` / `enum_rows`) and matches the raw value, otherwise `None`.

---

## Databricks DLT pipelines

Two independent Delta Live Tables pipelines are deployed by the bundle. They
share no lineage and run on separate schedules.

### `blf_ingestion` (continuous)

`databricks/blf-pipeline/dlt_blf_pipeline.py` streams `*.blf` files from a
Unity Catalog Volume into structured Delta tables.

```
Auto Loader (*.blf -> file paths)
         |  vector_blf Rust UDF
         v
blf_bronze                  -- all message types, one row per log object
         |
         +---> blf_silver_can              -- CAN / CAN-FD / CAN-FD64
         |          |
         |          +---> blf_silver_can_container_pdus  -- I-PDU payloads (IPduM)
         |          +---> blf_silver_can_signals         -- decoded signal values (long)
         |
         +---> blf_silver_eth              -- Ethernet / EthernetEx (+ VLAN tags)
         |          |
         |          +---> blf_silver_eth_signals         -- IP/TCP/UDP header fields (long)
         |          +---> blf_silver_someip              -- SOME/IP messages (UDP)
         |                     |
         |                     +---> blf_silver_someip_signals  -- SOME/IP signals (long)
         |
         +---> blf_silver_mf4_signals      -- MF4 scalar signals (long)
         +---> blf_silver_diag             -- UDS messages (CAN ISO-TP + DoIP merged)
         |
         v
blf_gold_signals            -- CAN + ETH + SOME/IP signals unified schema
```

Auto Loader tracks which files have been processed, so only new BLF files
are ingested on each run (exactly-once, incremental). In `prod` this
pipeline runs `continuous: true` (auto-restarting).

Signal definitions are loaded from CSVs (see Pipeline parameters). `assets/can_signals.csv` and `assets/someip_signals.csv` in this repo contain demo signal sets.

### `signal_docs` (triggered)

`databricks/signal-docs-pipeline/signal_docs_pipeline.py` extracts text from
signal documentation files. It's a separate pipeline resource — not a table
inside `blf_ingestion` — because documentation uploads are infrequent and
don't need `blf_ingestion`'s continuous streaming compute:

```
Auto Loader (*.pdf/*.docx/*.pptx/*.xlsx -> file paths)
         |  pypdf / python-docx / python-pptx / openpyxl
         v
blf_signal_doc_sections     -- extracted text sections, one per page/slide/sheet/document
                                (+ optional ai_query() semantic_summary column)
```

This pipeline is always `continuous: false` (both `dev` and `prod`) — run it
manually after uploading new documentation:

```bash
databricks bundle run signal_docs
```

### Deploy with Databricks Asset Bundles

`databricks.yml` at the repo root automates the full deploy workflow:

```bash
# One-time setup
databricks configure           # set workspace host + token

# Build the Linux wheel, upload, and create/update both DLT pipelines
databricks bundle deploy       # → dev target (default)
databricks bundle deploy -t prod

# Trigger a pipeline run
databricks bundle run blf_ingestion   # continuous in prod; this just kicks off an update
databricks bundle run signal_docs     # always triggered -- run after uploading new docs

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

### Pipeline parameters (`blf_ingestion`)

| Parameter | Default | Description |
|---|---|---|
| `blf.source_path` | — | Volume path containing `*.blf` files (required) |
| `blf.target_catalog` | `main` | Unity Catalog output catalog |
| `blf.target_schema` | `blf` | Unity Catalog output schema |
| `blf.signals_path` | `""` | Volume path to CAN signal CSV; if empty, `blf_silver_can_signals` and `blf_silver_can_container_pdus` are empty |
| `blf.someip_signals_path` | `""` | Volume path to SOME/IP signal CSV; if empty, `blf_silver_someip_signals` is empty |
| `blf.enum_signals_path` | `""` | Volume path to enum value mapping CSV (optional); decoded signals carry a `signal_str` category column when set |
| `blf.signals_table` | `""` | Unity Catalog table (`catalog.schema.table`) with CAN signal definitions; overrides `blf.signals_path` when set |
| `blf.someip_signals_table` | `""` | Unity Catalog table with SOME/IP signal definitions; overrides `blf.someip_signals_path` when set |
| `blf.enum_signals_table` | `""` | Unity Catalog table with enum value mappings; overrides `blf.enum_signals_path` when set |
| `blf.container_long_header` | `false` | Set to `true` for IPduM container frames with a long (32-bit) PDU header |

Table-sourced signal/enum definitions are collected on the driver at pipeline startup and shipped to executors via a Spark broadcast variable — no CSV staging round-trip. Set the bundle variables via `databricks bundle deploy --var="signals_table=main.blf_dev.can_signals"` (etc.).

CAN signal CSV format (header required):

```
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset[,pdu_id]
0x100,EngineSpeed_rpm,0,0,16,Intel,false,0.125,0.0
0x200,AmbientTemp,0,0,8,Intel,true,1.0,-40.0
```

`message_id` accepts hex (`0x...`) or decimal. `byte_order` is `Intel` or `Motorola` (case-insensitive). `start_byte` and `start_bit` locate the signal within the payload; `pdu_id` is optional (for I-PDU container demuxing).

SOME/IP signal CSV format (header required):

```
service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x0064,0x0001,MotorSpeed_rpm,0,0,16,Intel,false,1.0,0.0
0x0064,0x0001,MotorTorque_Nm,2,0,16,Intel,true,0.1,0.0
```

`service_id` and `method_id` accept hex or decimal. Bit extraction uses the same Intel/Motorola logic as CAN signals, applied to the SOME/IP application payload. SOME/IP-SD (`service_id=0xFFFF`) is skipped automatically.

Upload to the `signals` volume before running:

```bash
databricks fs cp assets/can_signals.csv dbfs:/Volumes/main/blf_dev/signals/can_signals.csv --overwrite
databricks fs cp assets/someip_signals.csv dbfs:/Volumes/main/blf_dev/signals/someip_signals.csv --overwrite
```

### Signal documentation (PDF / Word / PowerPoint / Excel)

Real-world signal documentation (OEM CAN matrix / interface control documents, SOME/IP service specs, glossaries) usually isn't a hand-curated CSV — it's a PDF, Word doc, slide deck, or spreadsheet. This is handled by the separate `signal_docs` pipeline (`databricks/signal-docs-pipeline/signal_docs_pipeline.py` — see [above](#databricks-dlt-pipelines)), with its own parameters:

| Parameter | Default | Description |
|---|---|---|
| `blf.signal_docs_path` | `/Volumes/.../signals/docs` | Volume directory of PDF/DOCX/PPTX/XLSX signal documentation files; extracted into `blf_signal_doc_sections` |
| `blf.semantic_model_endpoint` | `""` | Model Serving endpoint name; when set, adds an `ai_query()`-derived `semantic_summary` column to `blf_signal_doc_sections` (optional) |

`blf.signal_docs_path` points at a Volume directory that Auto Loader watches for `*.pdf`, `*.docx`, `*.pptx`, and `*.xlsx` files; each is broken into `blf_signal_doc_sections` rows:

```
_source_file, _file_mtime, _file_size_bytes, _ingested_at,
doc_type, section_number, section_label, text, semantic_summary
```

One row per PDF page, PPTX slide, or XLSX sheet; DOCX has no fixed page boundary in its file format, so it's one row for the whole document. Genie Space (see below) can search `text` directly (e.g. `WHERE text LIKE '%term%'`) to ground questions about signal meaning that aren't obvious from `signal_name` alone.

Upload documentation files the same way as signal CSVs:

```bash
databricks fs cp your_can_matrix.pdf dbfs:/Volumes/main/blf_dev/signals/docs/your_can_matrix.pdf --overwrite
databricks bundle run signal_docs   # triggered pipeline -- doesn't run automatically
```

**This is not the same as the `signal_importer` job** (`databricks/signal-importer/`), which converts a *structured* Excel sheet (fixed columns: `message_id, signal_name, start_byte, ...`) into the `can_signals`/`someip_signals` decode-parameter tables above. This pipeline path is for free-text *documentation*, not decode parameters.

**Optional: AI semantic summaries.** Setting `blf.semantic_model_endpoint` to a Databricks Model Serving endpoint name adds a `semantic_summary` column, computed via the `ai_query()` SQL function from each section's extracted text (1-3 sentence summary calling out signal names/units it recognizes). It's off by default — `semantic_summary` stays `NULL` and no model calls are made until the endpoint is set. The `signal_docs` pipeline's run-as identity needs `CAN_QUERY` permission on that endpoint, and every extracted section triggers one model call per pipeline run, so enabling it adds latency and inference cost proportional to the number of pages/slides/sheets ingested.

---

## Signal Viewer (Databricks App)

`databricks/signal-viewer/` is a [Plotly Dash](https://dash.plotly.com/) web application deployable as a Databricks App. It reads from the `blf_gold_signals` Delta table produced by the `blf_ingestion` pipeline and provides interactive signal visualization.

**Features:**

- Signal checklist with keyword filter and All/None buttons
- Time-range slider to pre-filter data before plotting
- Multi-signal overlay chart with configurable height and x-axis mode (time / timestamp ns)
- GPS map view — automatically shown when signals named `GPS_Latitude` / `GPS_Longitude` are selected
- AgGrid pivot data table for tabular inspection
- Local dev mode with dummy data when `DATABRICKS_WAREHOUSE_ID` is unset

**Deploy:**

```bash
# Deploy as a Databricks App (DAB)
databricks bundle deploy        # deploys pipeline + signal-viewer app

# Run locally (no Databricks warehouse needed)
cd databricks/signal-viewer
uv run python app.py
```

The app reads `BLF_CATALOG` and `BLF_SCHEMA` environment variables (defaults: `main`, `blf_dev`). SQL queries are forwarded with the logged-in user's token via `manifest.yaml` `user_api_scopes`.

### Genie Space setup

The "Ask Genie" panel lets users query signals in natural language via a Databricks AI/BI Genie Space.

**1. Set the Genie Space ID in `app.yaml`:**

```yaml
env:
  - name: GENIE_SPACE_ID
    value: "<your-genie-space-id>"
```

**2. Grant the app service principal access to the Genie Space.**

The app calls the Genie API as the app's service principal (M2M OAuth). The SP must have at least `CAN_RUN` permission on the Genie Space.

Find the SP's `application_id`:

```bash
databricks apps get signal-viewer
# look for "service_principal_id" or "application_id" in the output
```

Grant permission via CLI:

```bash
databricks permissions update genie <GENIE_SPACE_ID> \
  --json '{
    "access_control_list": [{
      "service_principal_name": "<application_id>",
      "permission_level": "CAN_RUN"
    }]
  }'
```

Verify:

```bash
databricks permissions get genie <GENIE_SPACE_ID>
```

**3. Redeploy the app** after updating `app.yaml`.

**4. Add `blf_signal_doc_sections` to the Genie Space.** Genie Space table/instruction curation isn't part of DAB IaC — there's no bundle resource for it — so this step is manual, in the Databricks UI: open the Genie Space, add `blf_signal_doc_sections` to its table list, and add an instruction along the lines of "When a question mentions a domain term not obviously matching a signal_name, search blf_signal_doc_sections.text (and semantic_summary, if populated) for that term to find the right signal." This gives Genie a documentation table to ground natural-language questions on, alongside `blf_gold_signals`/`blf_signal_catalog`.

**5. Teach Genie to use the app's selected-filter context.** `build_context_prefix()` in `databricks/signal-viewer/app/genie.py` prepends a summary of the sidebar's current File/Source/Channel selection to every question sent to Genie, in this form:

```
Currently viewing file(s) (_source_file column) <value>; source(s) (signal_source column) <value>; channel(s) (signal_source+channel key) <value>.
```

Without an instruction telling Genie how to read this, it won't connect a question like "tell me about this log file" to the selected file. Add an instruction to the Genie Space along these lines (also manual, in the Databricks UI — same reason as step 4):

```
Questions from the Signal Viewer app may start with a line like:

  "Currently viewing file(s) (_source_file column) <value>; source(s) (signal_source
  column) <value>; channel(s) (signal_source+channel key) <value>."

This describes the filters currently selected in the app's sidebar. When present,
apply it as a WHERE-clause condition on blf_gold_signals, and treat referring
phrases in the question ("this file", "this log", "these signals") as pointing to
it. If the line is absent, no filter is selected in the app -- ask which file(s) to
use, or default to querying all files.

Field mapping:
- "file(s) (_source_file column) <value>": exact match (=/IN) against _source_file.
  Values are full Volume paths, not basenames -- do not use LIKE with just a
  filename.
- "source(s) (signal_source column) <value>": IN match against signal_source
  ('CAN', 'ETH', or 'SOMEIP').
- "channel(s) (signal_source+channel key) <value>": each value concatenates
  signal_source and a channel number, e.g. "CAN1" means signal_source='CAN' AND
  channel=1, "SOMEIP2" means signal_source='SOMEIP' AND channel=2. ETH and SOMEIP
  are distinct signal_source values -- do not conflate them.

Also remember: signal_name is not unique across signal_source, so always include
signal_source when filtering by signal_name.
```

After saving the instruction, verify it in the app: select a file in the sidebar, open the "Ask Genie" panel, and ask "tell me about this log file" — the generated SQL (visible by expanding Genie's response) should include a `_source_file` filter.

---

## Scripts

| Script | Description |
|---|---|
| `databricks/signal-importer/excel_to_signals.py` | Convert an Excel workbook sheet to a CAN or SOME/IP signal CSV |
| `scripts/arxml_to_can_signals.py` | Convert ARXML to signal CSV (alternative to `vector-blf-rs convert`) |
| `scripts/bench_python.py` | Python benchmark comparing `vector_blf` against `python-can` |
| `scripts/create_ipdum_blf.py` | Generate test BLF files with IPduM container frames |
| `scripts/bench_eth_signals.py` | Benchmark Ethernet signal extraction throughput |

```bash
# Convert an Excel signal sheet to CAN signal CSV
uv run python databricks/signal-importer/excel_to_signals.py signals.xlsx out.csv \
  --message-id 0 --signal-name 1 --start-byte 2 --start-bit 3 --bit-length 4

# Convert to SOME/IP signal CSV
uv run python databricks/signal-importer/excel_to_signals.py signals.xlsx out.csv --mode someip \
  --service-id 0 --method-id 1 --signal-name 2
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
cargo build                                # debug build
cargo test                                 # run all tests
cargo clippy --all-targets && cargo fmt    # lint + format

uv run maturin develop --features python   # rebuild Python extension
uv run python bench_python.py              # run benchmark
```
