pub mod blf;
pub mod mf4;
mod table;

use blf::{
    check_can_csv, check_someip_csv, BaseObject, CanSignalDb, ChannelDb, ChannelType,
    ContainerHeader, Dir, Ip, Message, ParseError, SomeIp, SomeIpSignalDb, Timestamp, Transport,
    Writer,
};
use clap::{Parser, Subcommand};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type SomeIpDecoded<'a> = Option<(u16, u16, Vec<(&'a str, f64)>)>;

#[derive(Parser)]
#[command(
    name = "vector-blf-rs",
    about = "Read and convert Vector BLF log files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check a CAN or SOME/IP signal CSV for formatting errors
    Check {
        /// Path to the signal CSV file
        path: PathBuf,
    },
    /// Convert a DBC or ARXML network description to signal CSV
    Convert {
        /// Input file (.dbc or .arxml)
        input: PathBuf,
        /// Output CSV file
        output: PathBuf,
        /// Overlay CSV merged on top of the converted signals
        #[arg(long, value_name = "FILE")]
        overlay: Option<PathBuf>,
    },
    /// Parse a BLF, MF4, or MDF file; optionally export to CSV, BLF, MF4, or MDF
    Parse {
        /// Input file (.blf, .mf4, or .mdf)
        input: PathBuf,
        /// Output file (.blf, .csv, .mf4, or .mdf); omit to benchmark parse only
        output: Option<PathBuf>,
        /// CAN signal definitions CSV (used when output is .csv)
        #[arg(long, value_name = "FILE")]
        can_signals: Option<PathBuf>,
        /// SOME/IP signal definitions CSV
        #[arg(long, value_name = "FILE")]
        someip_signals: Option<PathBuf>,
        /// Channel number to channel name mapping CSV
        #[arg(long, value_name = "FILE")]
        channels: Option<PathBuf>,
        /// Repeat BLF input N times when writing BLF, MF4, or MDF output
        #[arg(long, default_value = "1", value_name = "N")]
        repeat: u32,
        /// Number of parser threads
        #[arg(long, default_value = "1", value_name = "N")]
        threads: usize,
        /// Suppress table output to stdout
        #[arg(long, short)]
        quiet: bool,
        /// Print a per-PDU occurrence summary (requires --can-signals)
        #[arg(long)]
        pdu_list: bool,
    },
}

fn ts_ns(ts: Timestamp) -> Option<u64> {
    match ts {
        Timestamp::Nanosecond(n) => Some(n),
        Timestamp::Microsecond(u) => u.checked_mul(1000),
    }
}

fn dir_str(d: &Dir) -> &'static str {
    match d {
        Dir::Tx => "Tx",
        Dir::Rx => "Rx",
        Dir::TxRq => "TxRq",
        Dir::Unknown(_) => "?",
    }
}

fn hex_str(data: &[u8]) -> String {
    let s: String = data.iter().take(16).map(|b| format!("{b:02X}")).collect();
    if data.len() > 16 {
        format!("{s}...")
    } else {
        s
    }
}

fn mac_str(addr: &[u8; 6]) -> String {
    format!(
        "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
        addr[0], addr[1], addr[2], addr[3], addr[4], addr[5]
    )
}

fn ch_name(db: Option<&ChannelDb>, ty: ChannelType, ch: u32) -> String {
    db.and_then(|d| d.name(ty, ch))
        .map(String::from)
        .unwrap_or_else(|| ch.to_string())
}

fn write_csv_with_signals(
    w: &mut BufWriter<File>,
    objects: impl Iterator<Item = impl Borrow<BaseObject>>,
    signals: Option<&Path>,
    someip_signals: Option<&Path>,
    start_ns: u64,
    channel_db: Option<&ChannelDb>,
) -> Result<usize> {
    if let Some(sp) = signals {
        let db = CanSignalDb::from_csv(BufReader::new(File::open(sp)?))?;
        let someip_db = someip_signals
            .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
            .transpose()?;
        Ok(blf::csv::write_csv_signals(
            w,
            objects,
            &db,
            someip_db.as_ref(),
            start_ns,
            channel_db,
        )?)
    } else {
        Ok(blf::csv::write_csv_raw(w, objects, start_ns, channel_db)?)
    }
}

fn abs_ts_str(start_ns: u64, relative_ns: u64) -> String {
    use chrono::{TimeZone, Utc};
    let total = start_ns.saturating_add(relative_ns);
    let secs = (total / 1_000_000_000) as i64;
    let nanos = (total % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
        .unwrap_or_default()
}

fn print_table<T: Borrow<BaseObject>>(
    objects: impl IntoIterator<Item = T>,
    start_ns: u64,
    channel_db: Option<&ChannelDb>,
) {
    let mut tbl = table::Table::new(&[
        ("timestamp_ns", false),
        ("absolute_timestamp", false),
        ("type", false),
        ("ch", true),
        ("dir", false),
        ("src_mac", false),
        ("dst_mac", false),
        ("ether_type", false),
        ("vlan_vid", true),
        ("id", false),
        ("ext_id", false),
        ("dlc", true),
        ("data", false),
    ]);

    for item in objects {
        let obj = item.borrow();
        let Some(ns) = ts_ns(obj.timestamp) else {
            continue;
        };
        let abs = abs_ts_str(start_ns, ns);
        let ns_str = ns.to_string();
        let row = match &obj.message {
            Message::Can(m) => vec![
                ns_str,
                abs,
                "CAN".into(),
                ch_name(channel_db, ChannelType::Can, m.channel as u32),
                dir_str(&m.dir).into(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("0x{:X}", m.id),
                m.is_ext_id.to_string(),
                m.dlc.to_string(),
                hex_str(&m.data),
            ],
            Message::CanFd(m) => vec![
                ns_str,
                abs,
                "CAN-FD".into(),
                ch_name(channel_db, ChannelType::Can, m.channel as u32),
                dir_str(&m.dir).into(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("0x{:X}", m.id),
                m.is_ext_id.to_string(),
                m.dlc.to_string(),
                hex_str(&m.data),
            ],
            Message::CanFd64(m) => vec![
                ns_str,
                abs,
                "CAN-FD64".into(),
                ch_name(channel_db, ChannelType::Can, m.channel as u32),
                dir_str(&m.dir).into(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("0x{:X}", m.id),
                m.is_ext_id.to_string(),
                m.dlc.to_string(),
                hex_str(&m.data),
            ],
            Message::Ethernet(m) => vec![
                ns_str,
                abs,
                "Ethernet".into(),
                ch_name(channel_db, ChannelType::Ethernet, m.channel as u32),
                dir_str(&m.dir).into(),
                mac_str(&m.src_addr),
                mac_str(&m.dst_addr),
                format!("0x{:04X}", m.ether_type),
                m.vlan
                    .as_ref()
                    .map(|v| v.vid.to_string())
                    .unwrap_or_default(),
                String::new(),
                String::new(),
                String::new(),
                hex_str(&m.data),
            ],
            Message::EthernetEx(m) => vec![
                ns_str,
                abs,
                "EthernetEx".into(),
                ch_name(channel_db, ChannelType::Ethernet, m.channel as u32),
                dir_str(&m.dir).into(),
                mac_str(&m.src_addr),
                mac_str(&m.dst_addr),
                format!("0x{:04X}", m.ether_type),
                m.vlan
                    .as_ref()
                    .map(|v| v.vid.to_string())
                    .unwrap_or_default(),
                String::new(),
                String::new(),
                String::new(),
                hex_str(&m.data),
            ],
            Message::Mf4Signal(m) => vec![
                ns_str,
                abs,
                "Mf4Signal".into(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("{} {}", m.value, m.unit),
            ],
            Message::Other(ot, _) => vec![
                ns_str,
                abs,
                "Other".into(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("{ot:?}"),
            ],
        };
        tbl.push(row);
    }
    tbl.print();
}

fn try_someip_decode<'a>(
    db: &'a SomeIpSignalDb,
    ether_type: u16,
    data: &[u8],
) -> SomeIpDecoded<'a> {
    let ip = Ip::parse(ether_type, data).ok()?;
    let transport = match &ip {
        Ip::V4(v4) => v4.parse_transport().ok()?,
        Ip::V6(v6) => v6.parse_transport().ok()?,
    };
    let someip_data: &[u8] = match &transport {
        Transport::Udp(u) => &u.data,
        Transport::Tcp(t) => &t.data,
    };
    let someip = SomeIp::parse(someip_data).ok()?;
    let vals = db.extract(someip.service_id, someip.method_id, &someip.payload);
    if vals.is_empty() {
        None
    } else {
        Some((someip.service_id, someip.method_id, vals))
    }
}

fn print_signal_table<T: Borrow<BaseObject>>(
    objects: impl IntoIterator<Item = T>,
    can_db: Option<&CanSignalDb>,
    someip_db: Option<&SomeIpSignalDb>,
    start_ns: u64,
    channel_db: Option<&ChannelDb>,
) {
    let mut tbl = table::Table::new(&[
        ("timestamp_ns", false),
        ("absolute_timestamp", false),
        ("type", false),
        ("ch", true),
        ("id", false),
        ("signal", false),
        ("value", true),
    ]);

    for item in objects {
        let obj = item.borrow();
        let Some(ns) = ts_ns(obj.timestamp) else {
            continue;
        };
        let abs = abs_ts_str(start_ns, ns);
        let ns_str = ns.to_string();

        match &obj.message {
            Message::Can(m) => {
                if let Some(db) = can_db {
                    let vals = if db.is_container(m.id) {
                        db.extract_container(m.id, &m.data, ContainerHeader::Short)
                    } else {
                        db.extract(m.id, &m.data)
                    };
                    let id = format!("0x{:X}", m.id);
                    let ch = ch_name(channel_db, ChannelType::Can, m.channel as u32);
                    for (name, value) in vals {
                        tbl.push(vec![
                            ns_str.clone(),
                            abs.clone(),
                            "CAN".into(),
                            ch.clone(),
                            id.clone(),
                            name.to_string(),
                            value.to_string(),
                        ]);
                    }
                }
            }
            Message::CanFd(m) => {
                if let Some(db) = can_db {
                    let vals = if db.is_container(m.id) {
                        db.extract_container(m.id, &m.data, ContainerHeader::Short)
                    } else {
                        db.extract(m.id, &m.data)
                    };
                    let id = format!("0x{:X}", m.id);
                    let ch = ch_name(channel_db, ChannelType::Can, m.channel as u32);
                    for (name, value) in vals {
                        tbl.push(vec![
                            ns_str.clone(),
                            abs.clone(),
                            "CAN-FD".into(),
                            ch.clone(),
                            id.clone(),
                            name.to_string(),
                            value.to_string(),
                        ]);
                    }
                }
            }
            Message::CanFd64(m) => {
                if let Some(db) = can_db {
                    let vals = if db.is_container(m.id) {
                        db.extract_container(m.id, &m.data, ContainerHeader::Short)
                    } else {
                        db.extract(m.id, &m.data)
                    };
                    let id = format!("0x{:X}", m.id);
                    let ch = ch_name(channel_db, ChannelType::Can, m.channel as u32);
                    for (name, value) in vals {
                        tbl.push(vec![
                            ns_str.clone(),
                            abs.clone(),
                            "CAN-FD64".into(),
                            ch.clone(),
                            id.clone(),
                            name.to_string(),
                            value.to_string(),
                        ]);
                    }
                }
            }
            Message::Ethernet(m) => {
                if let Some(db) = someip_db {
                    if let Some((svc, mth, vals)) = try_someip_decode(db, m.ether_type, &m.data) {
                        let id = format!("0x{:04X}{:04X}", svc, mth);
                        let ch = ch_name(channel_db, ChannelType::Ethernet, m.channel as u32);
                        for (name, value) in vals {
                            tbl.push(vec![
                                ns_str.clone(),
                                abs.clone(),
                                "Ethernet".into(),
                                ch.clone(),
                                id.clone(),
                                name.to_string(),
                                value.to_string(),
                            ]);
                        }
                    }
                }
            }
            Message::EthernetEx(m) => {
                if let Some(db) = someip_db {
                    if let Some((svc, mth, vals)) = try_someip_decode(db, m.ether_type, &m.data) {
                        let id = format!("0x{:04X}{:04X}", svc, mth);
                        let ch = ch_name(channel_db, ChannelType::Ethernet, m.channel as u32);
                        for (name, value) in vals {
                            tbl.push(vec![
                                ns_str.clone(),
                                abs.clone(),
                                "EthernetEx".into(),
                                ch.clone(),
                                id.clone(),
                                name.to_string(),
                                value.to_string(),
                            ]);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    tbl.print();
}

fn collect_pdu_counts<T: Borrow<BaseObject>>(
    objects: impl IntoIterator<Item = T>,
    can_db: &CanSignalDb,
) -> HashMap<(u32, u32), usize> {
    let mut counts: HashMap<(u32, u32), usize> = HashMap::new();
    for item in objects {
        let obj = item.borrow();
        let (can_id, data): (u32, &[u8]) = match &obj.message {
            Message::Can(m) if can_db.is_container(m.id) => (m.id, &m.data),
            Message::CanFd(m) if can_db.is_container(m.id) => (m.id, &m.data),
            Message::CanFd64(m) if can_db.is_container(m.id) => (m.id, &m.data),
            _ => continue,
        };
        for (pdu_id, _) in blf::demux_container(data, blf::ContainerHeader::Short) {
            *counts.entry((can_id, pdu_id)).or_insert(0) += 1;
        }
    }
    counts
}

fn print_pdu_table(counts: &HashMap<(u32, u32), usize>, can_db: &CanSignalDb) {
    let mut tbl = table::Table::new(&[
        ("can_id", false),
        ("pdu_id", false),
        ("count", true),
        ("signals", false),
    ]);
    let mut entries: Vec<_> = counts.iter().collect();
    entries.sort_by_key(|((can_id, pdu_id), _)| (*can_id, *pdu_id));
    for ((can_id, pdu_id), count) in entries {
        let sig_names: Vec<&str> = can_db
            .container_signals(*can_id, *pdu_id)
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        tbl.push(vec![
            format!("0x{:X}", can_id),
            format!("0x{:X}", pdu_id),
            count.to_string(),
            sig_names.join(", "),
        ]);
    }
    tbl.print();
}

fn check_boundaries(chunks: &[Vec<BaseObject>]) {
    for i in 0..chunks.len().saturating_sub(1) {
        if let (Some(a), Some(b)) = (chunks[i].last(), chunks[i + 1].first()) {
            let (Some(a_ns), Some(b_ns)) = (ts_ns(a.timestamp), ts_ns(b.timestamp)) else {
                continue;
            };
            if b_ns < a_ns {
                eprintln!(
                    "WARNING: timestamp not monotone at boundary {}/{}: {}ns > {}ns (delta={}ns)",
                    i,
                    i + 1,
                    a_ns,
                    b_ns,
                    a_ns - b_ns
                );
            }
        }
    }
}

fn cmd_check(path: PathBuf) -> Result<()> {
    let csv_type = {
        let f = File::open(&path)?;
        let mut detected = "";
        for line in BufReader::new(f).lines() {
            let line = line?;
            let trimmed = line.trim().to_string();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                if trimmed.starts_with("message_id") {
                    detected = "can";
                } else if trimmed.starts_with("service_id") {
                    detected = "someip";
                } else {
                    eprintln!(
                        "error: cannot detect CSV type from header {:?}; \
                         expected header starting with 'message_id' (CAN) or 'service_id' (SOME/IP)",
                        trimmed
                    );
                    std::process::exit(1);
                }
                break;
            }
        }
        detected
    };

    let errors = if csv_type == "can" {
        check_can_csv(BufReader::new(File::open(&path)?))
    } else {
        check_someip_csv(BufReader::new(File::open(&path)?))
    };

    let display = path.display();
    if errors.is_empty() {
        println!("{display}: OK");
    } else {
        for (line, msg) in &errors {
            eprintln!("{display}:{line}: {msg}");
        }
        eprintln!("{} error(s) found", errors.len());
        std::process::exit(1);
    }
    Ok(())
}

fn cmd_convert(input: PathBuf, output: PathBuf, overlay: Option<PathBuf>) -> Result<()> {
    let input_str = input.to_str().unwrap_or("");
    let t = time::Instant::now();
    let mut db = if input_str.ends_with(".dbc") {
        blf::convert::dbc::can_db_from_dbc(BufReader::new(File::open(&input)?))?
    } else if input_str.ends_with(".arxml") || input_str.ends_with(".xml") {
        blf::convert::arxml::can_db_from_arxml(BufReader::new(File::open(&input)?))?
    } else {
        eprintln!(
            "error: unrecognised format for {:?} (expected .dbc or .arxml)",
            input
        );
        std::process::exit(1);
    };
    if let Some(overlay) = overlay {
        let ov = CanSignalDb::from_csv(BufReader::new(File::open(&overlay)?))?;
        db.merge(ov);
    }
    let mut w = BufWriter::new(File::create(&output)?);
    db.write_csv(&mut w)?;
    println!(
        "wrote {} signal(s) to {} in {:.3}s",
        db.len(),
        output.display(),
        t.elapsed().as_secs_f32()
    );
    Ok(())
}

fn ext(p: &Path) -> &str {
    p.extension().and_then(|e| e.to_str()).unwrap_or("")
}

struct ParseOptions {
    signals: Option<PathBuf>,
    someip_signals_path: Option<PathBuf>,
    channels_path: Option<PathBuf>,
    quiet: bool,
    pdu_list: bool,
}

fn cmd_parse(
    input: PathBuf,
    output: Option<PathBuf>,
    repeat: u32,
    n_threads: usize,
    opts: ParseOptions,
) -> Result<()> {
    if matches!(ext(&input), "mf4" | "mdf") {
        return cmd_parse_mf4(input, output, opts);
    }
    let channel_db: Option<ChannelDb> = opts
        .channels_path
        .as_ref()
        .map(|p| ChannelDb::from_csv(BufReader::new(File::open(p)?)))
        .transpose()?;

    let is_csv = output.as_ref().map(|p| ext(p) == "csv").unwrap_or(false);
    let output_is_mf4 = output
        .as_ref()
        .map(|p| matches!(ext(p), "mf4" | "mdf"))
        .unwrap_or(false);

    // Single-threaded CSV from BLF: stream one object at a time — O(1-container) peak memory.
    if n_threads == 1 && is_csv && opts.quiet {
        return cmd_parse_csv_stream(
            &input,
            output.as_ref().unwrap(),
            opts.signals,
            opts.someip_signals_path,
            channel_db.as_ref(),
        );
    }

    // Scan phase: index LogContainer offsets without decompression.
    let t = time::Instant::now();
    let (file_header, maybe_offsets) =
        blf::scan_containers(&mut BufReader::new(File::open(&input)?))?;
    let start_ns = ts_ns(file_header.start_timestamp).unwrap_or(0);

    let chunk_results = if let Some(offsets) = maybe_offsets {
        println!(
            "found {} containers in {:.3}s",
            offsets.len(),
            t.elapsed().as_secs_f32()
        );
        let chunks = parse_parallel(&input, &offsets, n_threads)?;
        check_boundaries(&chunks);
        chunks
    } else {
        println!(
            "no containers (direct mode) in {:.3}s",
            t.elapsed().as_secs_f32()
        );
        let objects: Vec<BaseObject> = blf::Reader::new(BufReader::new(File::open(&input)?))?
            .filter_map(|r| r.ok())
            .collect();
        println!(
            "parsed {} objects across 1 chunks in {:.3}s",
            objects.len(),
            t.elapsed().as_secs_f32()
        );
        vec![objects]
    };

    if let Some(ref out) = output {
        let t = time::Instant::now();
        if is_csv {
            let mut w = BufWriter::new(File::create(out)?);
            let count = write_csv_with_signals(
                &mut w,
                chunk_results.iter().flatten(),
                opts.signals.as_deref(),
                opts.someip_signals_path.as_deref(),
                start_ns,
                channel_db.as_ref(),
            )?;
            println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
        } else if output_is_mf4 {
            let mf4_start_ns = chunk_results
                .iter()
                .flatten()
                .next()
                .and_then(|o| ts_ns(o.timestamp))
                .unwrap_or(0);
            let count =
                write_mf4_output(out, chunk_results.iter().flatten(), repeat, mf4_start_ns)?;
            println!(
                "wrote {} objects in {:.3}s",
                count,
                t.elapsed().as_secs_f32()
            );
        } else {
            let count = write_blf_output(out, &chunk_results, repeat)?;
            println!(
                "wrote {} objects in {:.3}s",
                count,
                t.elapsed().as_secs_f32()
            );
        }
    }
    if !opts.quiet {
        let can_db = opts
            .signals
            .as_ref()
            .map(|p| CanSignalDb::from_csv(BufReader::new(File::open(p)?)))
            .transpose()?;
        let someip_db = opts
            .someip_signals_path
            .as_ref()
            .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
            .transpose()?;
        if opts.pdu_list {
            match can_db.as_ref() {
                Some(db) => {
                    let counts = collect_pdu_counts(chunk_results.iter().flatten(), db);
                    print_pdu_table(&counts, db);
                }
                None => {
                    eprintln!("error: --pdu-list requires --can-signals");
                    std::process::exit(1);
                }
            }
        } else if can_db.is_some() || someip_db.is_some() {
            print_signal_table(
                chunk_results.iter().flatten(),
                can_db.as_ref(),
                someip_db.as_ref(),
                start_ns,
                channel_db.as_ref(),
            );
        } else {
            print_table(
                chunk_results.iter().flatten(),
                start_ns,
                channel_db.as_ref(),
            );
        }
    }
    Ok(())
}

/// Parse an MF4 input file; output to CSV, BLF, or another MF4.
fn cmd_parse_mf4(input: PathBuf, output: Option<PathBuf>, opts: ParseOptions) -> Result<()> {
    let t = time::Instant::now();
    let reader = mf4::Reader::new(BufReader::new(File::open(&input)?))?;
    let start_time_ns = reader.start_time_ns;
    let channel_db: Option<ChannelDb> = opts
        .channels_path
        .as_ref()
        .map(|p| ChannelDb::from_csv(BufReader::new(File::open(p)?)))
        .transpose()?;

    if !opts.quiet {
        let objects: Vec<BaseObject> = reader.filter_map(|r| r.ok()).collect();
        println!(
            "parsed {} objects in {:.3}s",
            objects.len(),
            t.elapsed().as_secs_f32()
        );
        let can_db = opts
            .signals
            .as_ref()
            .map(|p| CanSignalDb::from_csv(BufReader::new(File::open(p)?)))
            .transpose()?;
        let someip_db = opts
            .someip_signals_path
            .as_ref()
            .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
            .transpose()?;
        if opts.pdu_list {
            match can_db.as_ref() {
                Some(db) => {
                    let counts = collect_pdu_counts(objects.iter(), db);
                    print_pdu_table(&counts, db);
                }
                None => {
                    eprintln!("error: --pdu-list requires --can-signals");
                    std::process::exit(1);
                }
            }
        } else if can_db.is_some() || someip_db.is_some() {
            print_signal_table(
                objects.iter(),
                can_db.as_ref(),
                someip_db.as_ref(),
                start_time_ns,
                channel_db.as_ref(),
            );
        } else {
            print_table(objects.iter(), start_time_ns, channel_db.as_ref());
        }
        if let Some(ref out) = output {
            let t2 = time::Instant::now();
            let output_ext = ext(out);
            if output_ext == "csv" {
                let mut w = BufWriter::new(File::create(out)?);
                let count = write_csv_with_signals(
                    &mut w,
                    objects.iter(),
                    opts.signals.as_deref(),
                    opts.someip_signals_path.as_deref(),
                    start_time_ns,
                    channel_db.as_ref(),
                )?;
                println!("wrote {} rows in {:.3}s", count, t2.elapsed().as_secs_f32());
            } else if matches!(output_ext, "mf4" | "mdf") {
                let count = write_mf4_output(out, objects.iter(), 1, start_time_ns)?;
                println!(
                    "wrote {} objects in {:.3}s",
                    count,
                    t2.elapsed().as_secs_f32()
                );
            } else {
                let mut writer = Writer::new(BufWriter::new(File::create(out)?))?;
                for obj in &objects {
                    writer.write_base_object(obj)?;
                }
                writer.finish()?;
                println!(
                    "wrote {} objects in {:.3}s",
                    objects.len(),
                    t2.elapsed().as_secs_f32()
                );
            }
        }
        return Ok(());
    }

    let Some(ref out) = output else {
        // Benchmark: just count objects.
        let count = reader.filter_map(|r| r.ok()).count();
        println!(
            "parsed {} objects in {:.3}s",
            count,
            t.elapsed().as_secs_f32()
        );
        return Ok(());
    };

    if ext(out) == "csv" {
        let mut w = BufWriter::new(File::create(out)?);
        let count = write_csv_with_signals(
            &mut w,
            reader.filter_map(|r| r.ok()),
            opts.signals.as_deref(),
            opts.someip_signals_path.as_deref(),
            start_time_ns,
            channel_db.as_ref(),
        )?;
        println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
    } else if matches!(ext(out), "mf4" | "mdf") {
        let count = write_mf4_output(out, reader.filter_map(|r| r.ok()), 1, start_time_ns)?;
        println!(
            "wrote {} objects in {:.3}s",
            count,
            t.elapsed().as_secs_f32()
        );
    } else {
        // MF4 → BLF
        let mut writer = Writer::new(BufWriter::new(File::create(out)?))?;
        let mut count = 0usize;
        for obj in reader.filter_map(|r| r.ok()) {
            writer.write_base_object(&obj)?;
            count += 1;
        }
        writer.finish()?;
        println!(
            "wrote {} objects in {:.3}s",
            count,
            t.elapsed().as_secs_f32()
        );
    }
    Ok(())
}

fn write_mf4_output(
    output: &Path,
    objects: impl Iterator<Item = impl std::borrow::Borrow<BaseObject>>,
    repeat: u32,
    start_time_ns: u64,
) -> Result<usize> {
    let mut writer = mf4::Writer::new(BufWriter::new(File::create(output)?), start_time_ns)?;
    let objects: Vec<_> = objects.collect();
    let mut count = 0usize;
    for _ in 0..repeat {
        for obj in &objects {
            writer.write_base_object(obj.borrow())?;
            count += 1;
        }
    }
    writer.finish()?;
    Ok(count)
}

fn cmd_parse_csv_stream(
    input: &Path,
    output: &Path,
    signals: Option<PathBuf>,
    someip_signals_path: Option<PathBuf>,
    channel_db: Option<&ChannelDb>,
) -> Result<()> {
    let t = time::Instant::now();
    let mut w = BufWriter::new(File::create(output)?);
    let reader = blf::Reader::new(BufReader::new(File::open(input)?))?;
    let start_ns = ts_ns(reader.header.start_timestamp).unwrap_or(0);
    let count = write_csv_with_signals(
        &mut w,
        reader.filter_map(|r| r.ok()),
        signals.as_deref(),
        someip_signals_path.as_deref(),
        start_ns,
        channel_db,
    )?;
    println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
    Ok(())
}

fn parse_parallel(input: &Path, offsets: &[u64], n_threads: usize) -> Result<Vec<Vec<BaseObject>>> {
    let effective_threads = n_threads.min(offsets.len().max(1));
    let chunk_size = offsets.len().div_ceil(effective_threads);

    let t = time::Instant::now();
    let handles: Vec<_> = offsets
        .chunks(chunk_size)
        .map(|chunk| {
            let path = input.to_path_buf();
            let chunk = chunk.to_vec();
            std::thread::spawn(
                move || -> std::result::Result<Vec<BaseObject>, ParseError> {
                    let mut f = File::open(&path)?;
                    blf::parse_at(&mut f, &chunk)
                },
            )
        })
        .collect();

    let mut chunk_results: Vec<Vec<BaseObject>> = Vec::new();
    for h in handles {
        chunk_results.push(h.join().expect("parser thread panicked")?);
    }
    let total: usize = chunk_results.iter().map(|c| c.len()).sum();
    println!(
        "parsed {} objects across {} chunks in {:.3}s",
        total,
        chunk_results.len(),
        t.elapsed().as_secs_f32()
    );
    Ok(chunk_results)
}

fn write_blf_output(output: &Path, chunks: &[Vec<BaseObject>], repeat: u32) -> Result<usize> {
    let mut writer = Writer::new(BufWriter::new(File::create(output)?))?;
    let mut count = 0usize;
    for _ in 0..repeat {
        for obj in chunks.iter().flatten() {
            writer.write_base_object(obj)?;
            count += 1;
        }
    }
    writer.finish()?;
    Ok(count)
}

fn main() -> Result<()> {
    env_logger::init();
    match Cli::parse().command {
        Command::Check { path } => cmd_check(path),
        Command::Convert {
            input,
            output,
            overlay,
        } => cmd_convert(input, output, overlay),
        Command::Parse {
            input,
            output,
            can_signals: signals,
            repeat,
            threads,
            someip_signals,
            channels,
            quiet,
            pdu_list,
        } => cmd_parse(
            input,
            output,
            repeat,
            threads,
            ParseOptions {
                signals,
                someip_signals_path: someip_signals,
                channels_path: channels,
                quiet,
                pdu_list,
            },
        ),
    }
}
