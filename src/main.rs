pub mod blf;

use blf::{BaseObject, Message, ParseError, Reader, SignalDb, Timestamp, Writer};
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
    objects: &[Result<BaseObject, ParseError>],
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(w, "timestamp_ns,channel,id,ext_id,dir,dlc,data")?;
    let mut count = 0usize;
    for obj in objects.iter().flatten() {
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
    objects: &[Result<BaseObject, ParseError>],
    db: &SignalDb,
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(w, "timestamp_ns,channel,message_id,signal_name,value")?;
    let mut count = 0usize;
    for obj in objects.iter().flatten() {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    env_logger::init();

    let input = args.get(1).expect(
        "usage: vector-blf-rs <input.blf> [output.blf [repeat] | output.csv [signals.csv]]",
    );
    let t = time::Instant::now();
    let reader = Reader::new(BufReader::new(File::open(input)?))?;
    println!("header: {:?}", reader.header);

    let mut objects: Vec<Result<BaseObject, ParseError>> = Vec::new();
    for obj in reader {
        objects.push(obj);
    }
    println!(
        "read {} objects in {:.3}s",
        objects.len(),
        t.elapsed().as_secs_f32()
    );

    if let Some(output) = args.get(2) {
        let t = time::Instant::now();
        if output.ends_with(".csv") {
            let mut w = BufWriter::new(File::create(output)?);
            let count = if let Some(signals_path) = args.get(3) {
                let db = SignalDb::from_csv(BufReader::new(File::open(signals_path)?))?;
                write_csv_signals(&mut w, &objects, &db)?
            } else {
                write_csv_raw(&mut w, &objects)?
            };
            println!("wrote {} rows in {:.3}s", count, t.elapsed().as_secs_f32());
        } else {
            let repeat: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
            let mut writer = Writer::new(BufWriter::new(File::create(output)?))?;
            let mut count = 0usize;
            for _ in 0..repeat {
                for obj in objects.iter().flatten() {
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
