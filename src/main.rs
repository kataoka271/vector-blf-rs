pub mod blf;

use blf::{BaseObject, Message, ParseError, SignalDb, Timestamp, Writer};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::time;

fn ts_ns(ts: Timestamp) -> u64 {
    match ts {
        Timestamp::Nanosecond(n) => n,
        Timestamp::Microsecond(u) => u * 1000,
    }
}

fn write_csv_raw<W: Write>(
    w: &mut W,
    objects: &[BaseObject],
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(w, "timestamp_ns,channel,id,ext_id,dir,dlc,data")?;
    let mut count = 0usize;
    for obj in objects {
        let ns = ts_ns(obj.timestamp);
        match &obj.message {
            Message::Can(m) => {
                let hex: String = m.data.iter().map(|b| format!("{:02X}", b)).collect();
                writeln!(w, "{},{},0x{:X},{},{:?},{},{}", ns, m.channel, m.id, m.is_ext_id, m.dir, m.dlc, hex)?;
                count += 1;
            }
            Message::CanFd(m) => {
                let hex: String = m.data.iter().map(|b| format!("{:02X}", b)).collect();
                writeln!(w, "{},{},0x{:X},{},{:?},{},{}", ns, m.channel, m.id, m.is_ext_id, m.dir, m.dlc, hex)?;
                count += 1;
            }
            Message::CanFd64(m) => {
                let hex: String = m.data.iter().map(|b| format!("{:02X}", b)).collect();
                writeln!(w, "{},{},0x{:X},{},{:?},{},{}", ns, m.channel, m.id, m.is_ext_id, m.dir, m.dlc, hex)?;
                count += 1;
            }
            _ => {}
        }
    }
    Ok(count)
}

fn write_csv_signals<W: Write>(
    w: &mut W,
    objects: &[BaseObject],
    db: &SignalDb,
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(w, "timestamp_ns,channel,message_id,signal_name,value")?;
    let mut count = 0usize;
    for obj in objects {
        let ns = ts_ns(obj.timestamp);
        let (channel, id, data) = match &obj.message {
            Message::Can(m) => (m.channel as u32, m.id, m.data.as_slice()),
            Message::CanFd(m) => (m.channel as u32, m.id, m.data.as_slice()),
            Message::CanFd64(m) => (m.channel as u32, m.id, m.data.as_slice()),
            _ => continue,
        };
        for (name, value) in db.extract(id, data) {
            writeln!(w, "{},{},0x{:X},{},{}", ns, channel, id, name, value)?;
            count += 1;
        }
    }
    Ok(count)
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

    // Parse --threads N and collect remaining positional args.
    let mut n_threads: usize = 1;
    let mut positional: Vec<String> = Vec::new();
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        if arg == "--threads" {
            n_threads = iter.next().and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
        } else {
            positional.push(arg.clone());
        }
    }

    let input = positional.get(0).expect(
        "usage: vector-blf-rs <input.blf> [output.blf [repeat] | output.csv [signals.csv]] [--threads N]",
    );

    // Scan phase: index LogContainer offsets without decompression.
    let t = time::Instant::now();
    let offsets = {
        let mut f = File::open(input)?;
        blf::scan_containers(&mut f)?
    };
    println!(
        "found {} containers in {:.3}s",
        offsets.len(),
        t.elapsed().as_secs_f32()
    );

    // Parallel parse phase: each thread gets its own file handle and a chunk of offsets.
    let effective_threads = n_threads.min(offsets.len().max(1));
    let chunk_size = (offsets.len() + effective_threads - 1) / effective_threads;

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

    // Merge in original file order.
    let objects: Vec<BaseObject> = chunk_results.into_iter().flatten().collect();

    if let Some(output) = positional.get(1) {
        let t = time::Instant::now();
        if output.ends_with(".csv") {
            let mut w = BufWriter::new(File::create(output)?);
            let count = if let Some(signals_path) = positional.get(2) {
                let db = SignalDb::from_csv(BufReader::new(File::open(signals_path)?))?;
                write_csv_signals(&mut w, &objects, &db)?
            } else {
                write_csv_raw(&mut w, &objects)?
            };
            println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
        } else {
            let repeat: u32 = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
            let mut writer = Writer::new(BufWriter::new(File::create(output)?))?;
            let mut count = 0usize;
            for _ in 0..repeat {
                for obj in &objects {
                    writer.write_base_object(obj)?;
                    count += 1;
                }
            }
            writer.finish()?;
            println!("wrote {} objects in {:.3}s", count, t.elapsed().as_secs_f32());
        }
    }

    Ok(())
}
