pub mod blf;

use blf::{
    check_can_csv, check_someip_csv, BaseObject, CanSignalDb, ParseError, SomeIpSignalDb,
    Timestamp, Writer,
};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::time;

fn ts_ns(ts: Timestamp) -> u64 {
    match ts {
        Timestamp::Nanosecond(n) => n,
        Timestamp::Microsecond(u) => u * 1000,
    }
}

fn check_boundaries(chunks: &[Vec<BaseObject>]) {
    for i in 0..chunks.len().saturating_sub(1) {
        if let (Some(a), Some(b)) = (chunks[i].last(), chunks[i + 1].first()) {
            let a_ns = ts_ns(a.timestamp);
            let b_ns = ts_ns(b.timestamp);
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    env_logger::init();

    // Parse --threads N, --someip-signals <file>, and collect remaining positional args.
    let mut n_threads: usize = 1;
    let mut someip_signals_path: Option<String> = None;
    let mut overlay_path: Option<String> = None;
    let mut positional: Vec<String> = Vec::new();
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        if arg == "--threads" {
            n_threads = iter.next().and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
        } else if arg == "--someip-signals" {
            someip_signals_path = iter.next().cloned();
        } else if arg == "--overlay" {
            overlay_path = iter.next().cloned();
        } else {
            positional.push(arg.clone());
        }
    }

    // ── check subcommand ─────────────────────────────────────────────────────
    if positional.first().map(String::as_str) == Some("check") {
        let path = positional
            .get(1)
            .expect("usage: vector-blf-rs check <signals.csv>");

        // Detect CSV type from the first non-blank, non-comment line (the header).
        let csv_type = {
            let f = File::open(path)?;
            let mut detected = "";
            for line in std::io::BufReader::new(f).lines() {
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
            check_can_csv(BufReader::new(File::open(path)?))
        } else {
            check_someip_csv(BufReader::new(File::open(path)?))
        };

        if errors.is_empty() {
            println!("{path}: OK");
        } else {
            for (line, msg) in &errors {
                eprintln!("{path}:{line}: {msg}");
            }
            eprintln!("{} error(s) found", errors.len());
            std::process::exit(1);
        }
        return Ok(());
    }

    // ── convert subcommand ────────────────────────────────────────────────────
    if positional.first().map(String::as_str) == Some("convert") {
        let input = positional.get(1).expect(
            "usage: vector-blf-rs convert <input.dbc|.arxml> <output.csv> [--overlay <overlay.csv>]",
        );
        let output = positional.get(2).expect(
            "usage: vector-blf-rs convert <input.dbc|.arxml> <output.csv> [--overlay <overlay.csv>]",
        );
        let t = time::Instant::now();
        let mut db = if input.ends_with(".dbc") {
            blf::convert::dbc::can_db_from_dbc(BufReader::new(File::open(input)?))?
        } else if input.ends_with(".arxml") || input.ends_with(".xml") {
            blf::convert::arxml::can_db_from_arxml(BufReader::new(File::open(input)?))?
        } else {
            eprintln!("error: unrecognised format for {input:?} (expected .dbc or .arxml)");
            std::process::exit(1);
        };
        if let Some(ref overlay) = overlay_path {
            let ov = CanSignalDb::from_csv(BufReader::new(File::open(overlay)?))?;
            db.merge(ov);
        }
        let mut w = BufWriter::new(File::create(output)?);
        db.write_csv(&mut w)?;
        println!(
            "wrote {} signal(s) to {output:?} in {:.3}s",
            db.len(),
            t.elapsed().as_secs_f32()
        );
        return Ok(());
    }

    let input = positional.first().expect(
        "usage:\n  vector-blf-rs <input.blf> [output.blf [repeat] | output.csv [signals.csv]] [--threads N] [--someip-signals <file>]\n  vector-blf-rs convert <input.dbc|.arxml> <output.csv> [--overlay <overlay.csv>]\n  vector-blf-rs check <signals.csv>",
    );

    let output = positional.get(1);
    let is_csv = output.map(|s| s.ends_with(".csv")).unwrap_or(false);

    // Single-threaded CSV: stream one object at a time — O(1-container) peak memory.
    if n_threads == 1 && is_csv {
        let t = time::Instant::now();
        let output = output.unwrap();
        let mut w = BufWriter::new(File::create(output)?);
        let reader = blf::Reader::new(BufReader::new(File::open(input)?))?;
        let count = if let Some(signals_path) = positional.get(2) {
            let db = CanSignalDb::from_csv(BufReader::new(File::open(signals_path)?))?;
            let someip_db = someip_signals_path
                .as_deref()
                .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
                .transpose()?;
            blf::csv::write_csv_signals(
                &mut w,
                reader.filter_map(|r| r.ok()),
                &db,
                someip_db.as_ref(),
            )?
        } else {
            blf::csv::write_csv_raw(&mut w, reader.filter_map(|r| r.ok()))?
        };
        println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
        return Ok(());
    }

    // Buffered path: scan → parallel parse → output.
    // Used for multi-threaded parsing, BLF→BLF copy, or benchmarking (no output).

    // Scan phase: index LogContainer offsets without decompression.
    let t = time::Instant::now();
    let offsets = {
        let f = File::open(input)?;
        blf::scan_containers(&mut BufReader::new(f))?
    };
    println!(
        "found {} containers in {:.3}s",
        offsets.len(),
        t.elapsed().as_secs_f32()
    );

    // Parallel parse phase: each thread gets its own file handle and a chunk of offsets.
    let effective_threads = n_threads.min(offsets.len().max(1));
    let chunk_size = offsets.len().div_ceil(effective_threads);

    let t = time::Instant::now();
    let handles: Vec<_> = offsets
        .chunks(chunk_size)
        .map(|chunk| {
            let path = input.clone();
            let chunk = chunk.to_vec();
            std::thread::spawn(move || -> Result<Vec<BaseObject>, ParseError> {
                let mut f = File::open(&path)?;
                blf::parse_at(&mut f, &chunk)
            })
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

    // Boundary check: warn if timestamps are out of order at chunk seams.
    check_boundaries(&chunk_results);

    if let Some(output) = output {
        let t = time::Instant::now();
        if is_csv {
            let mut w = BufWriter::new(File::create(output)?);
            // Pass chunk_results directly — no flatten().collect() needed.
            let count = if let Some(signals_path) = positional.get(2) {
                let db = CanSignalDb::from_csv(BufReader::new(File::open(signals_path)?))?;
                let someip_db = someip_signals_path
                    .as_deref()
                    .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
                    .transpose()?;
                blf::csv::write_csv_signals(
                    &mut w,
                    chunk_results.iter().flatten(),
                    &db,
                    someip_db.as_ref(),
                )?
            } else {
                blf::csv::write_csv_raw(&mut w, chunk_results.iter().flatten())?
            };
            println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
        } else {
            let repeat: u32 = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
            let mut writer = Writer::new(BufWriter::new(File::create(output)?))?;
            let mut count = 0usize;
            for _ in 0..repeat {
                for obj in chunk_results.iter().flatten() {
                    writer.write_base_object(obj)?;
                    count += 1;
                }
            }
            writer.finish()?;
            println!(
                "wrote {} objects in {:.3}s",
                count,
                t.elapsed().as_secs_f32()
            );
        }
    }

    Ok(())
}
