pub mod blf;

use blf::{
    check_can_csv, check_someip_csv, BaseObject, CanSignalDb, ParseError, SomeIpSignalDb,
    Timestamp, Writer,
};
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::PathBuf;
use std::time;

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
    /// Parse a BLF file; optionally export to CSV or another BLF
    Parse {
        /// Input BLF file
        input: PathBuf,
        /// Output file (.blf or .csv); omit to benchmark parse only
        output: Option<PathBuf>,
        /// CAN signal definitions CSV (used when output is .csv)
        signals: Option<PathBuf>,
        /// Repeat input N times into BLF output
        #[arg(long, default_value = "1", value_name = "N")]
        repeat: u32,
        /// Number of parser threads
        #[arg(long, default_value = "1", value_name = "N")]
        threads: usize,
        /// SOME/IP signal definitions CSV
        #[arg(long, value_name = "FILE")]
        someip_signals: Option<PathBuf>,
    },
}

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
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Command::Check { path } => {
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
        }

        Command::Convert {
            input,
            output,
            overlay,
        } => {
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
        }

        Command::Parse {
            input,
            output,
            signals,
            repeat,
            threads: n_threads,
            someip_signals: someip_signals_path,
        } => {
            let is_csv = output
                .as_ref()
                .map(|p| p.extension().and_then(|e| e.to_str()) == Some("csv"))
                .unwrap_or(false);

            // Single-threaded CSV: stream one object at a time — O(1-container) peak memory.
            if n_threads == 1 && is_csv {
                let t = time::Instant::now();
                let out = output.as_ref().unwrap();
                let mut w = BufWriter::new(File::create(out)?);
                let reader = blf::Reader::new(BufReader::new(File::open(&input)?))?;
                let count = if let Some(ref signals_path) = signals {
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

            // Scan phase: index LogContainer offsets without decompression.
            let t = time::Instant::now();
            let offsets = {
                let f = File::open(&input)?;
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

            check_boundaries(&chunk_results);

            if let Some(ref output) = output {
                let t = time::Instant::now();
                if is_csv {
                    let mut w = BufWriter::new(File::create(output)?);
                    let count = if let Some(ref signals_path) = signals {
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
        }
    }

    Ok(())
}
