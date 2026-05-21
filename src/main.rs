pub mod blf;

use blf::{BaseObject, Ip, Message, ParseError, SignalDb, SomeIpSignalDb, Timestamp, Transport, Writer};
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
    writeln!(w, "timestamp_ns,type,channel,dir,src_mac,dst_mac,ether_type,vlan_vid,id,ext_id,dlc,data")?;
    let mut count = 0usize;
    for obj in objects {
        let ns = ts_ns(obj.timestamp);
        match &obj.message {
            Message::Can(m) => {
                write!(w, "{},CAN,{},{:?},,,,0x{:X},{},{},", ns, m.channel, m.dir, m.id, m.is_ext_id, m.dlc)?;
                for b in &m.data { write!(w, "{:02X}", b)?; }
                writeln!(w)?;
                count += 1;
            }
            Message::CanFd(m) => {
                write!(w, "{},CAN-FD,{},{:?},,,,0x{:X},{},{},", ns, m.channel, m.dir, m.id, m.is_ext_id, m.dlc)?;
                for b in &m.data { write!(w, "{:02X}", b)?; }
                writeln!(w)?;
                count += 1;
            }
            Message::CanFd64(m) => {
                write!(w, "{},CAN-FD64,{},{:?},,,,0x{:X},{},{},", ns, m.channel, m.dir, m.id, m.is_ext_id, m.dlc)?;
                for b in &m.data { write!(w, "{:02X}", b)?; }
                writeln!(w)?;
                count += 1;
            }
            Message::Ethernet(m) => {
                let a = &m.src_addr;
                let b = &m.dst_addr;
                let vlan_vid = m.vlan.as_ref().map(|v| v.vid.to_string()).unwrap_or_default();
                write!(w, "{},Ethernet,{},{:?},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},0x{:04X},{},,,,",
                    ns, m.channel, m.dir,
                    a[0], a[1], a[2], a[3], a[4], a[5],
                    b[0], b[1], b[2], b[3], b[4], b[5],
                    m.ether_type, vlan_vid)?;
                for byte in &m.data { write!(w, "{:02X}", byte)?; }
                writeln!(w)?;
                count += 1;
            }
            Message::EthernetEx(m) => {
                let a = &m.src_addr;
                let b = &m.dst_addr;
                let vlan_vid = m.vlan.as_ref().map(|v| v.vid.to_string()).unwrap_or_default();
                write!(w, "{},EthernetEx,{},{:?},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},0x{:04X},{},,,,",
                    ns, m.channel, m.dir,
                    a[0], a[1], a[2], a[3], a[4], a[5],
                    b[0], b[1], b[2], b[3], b[4], b[5],
                    m.ether_type, vlan_vid)?;
                for byte in &m.data { write!(w, "{:02X}", byte)?; }
                writeln!(w)?;
                count += 1;
            }
            _ => {}
        }
    }
    Ok(count)
}

fn try_someip_signals<'a>(
    ether_type: u16,
    data: &[u8],
    db: &'a SomeIpSignalDb,
) -> Option<(u16, u16, Vec<(&'a str, f64)>)> {
    let ip = Ip::parse(ether_type, data).ok()?;
    let transport = match &ip {
        Ip::V4(v4) => v4.parse_transport().ok()?,
        Ip::V6(v6) => v6.parse_transport().ok()?,
    };
    let someip = match &transport {
        Transport::Udp(udp) => udp.parse_someip().ok()?,
        Transport::Tcp(tcp) => tcp.parse_someip().ok()?,
    };
    let vals = db.extract(someip.service_id, someip.method_id, &someip.payload);
    if vals.is_empty() { None } else { Some((someip.service_id, someip.method_id, vals)) }
}

fn write_csv_signals<W: Write>(
    w: &mut W,
    objects: &[BaseObject],
    db: &SignalDb,
    someip_db: Option<&SomeIpSignalDb>,
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(w, "timestamp_ns,channel,message_id,signal_name,value")?;
    let mut count = 0usize;
    for obj in objects {
        let ns = ts_ns(obj.timestamp);
        match &obj.message {
            Message::Can(m) => {
                for (name, value) in db.extract(m.id, &m.data) {
                    writeln!(w, "{},{},0x{:X},{},{}", ns, m.channel, m.id, name, value)?;
                    count += 1;
                }
            }
            Message::CanFd(m) => {
                for (name, value) in db.extract(m.id, &m.data) {
                    writeln!(w, "{},{},0x{:X},{},{}", ns, m.channel, m.id, name, value)?;
                    count += 1;
                }
            }
            Message::CanFd64(m) => {
                for (name, value) in db.extract(m.id, &m.data) {
                    writeln!(w, "{},{},0x{:X},{},{}", ns, m.channel as u32, m.id, name, value)?;
                    count += 1;
                }
            }
            Message::Ethernet(m) => {
                if let Some(sdb) = someip_db {
                    if let Some((svc, mth, vals)) = try_someip_signals(m.ether_type, &m.data, sdb) {
                        let msg_id = ((svc as u32) << 16) | (mth as u32);
                        for (name, value) in vals {
                            writeln!(w, "{},{},0x{:08X},{},{}", ns, m.channel, msg_id, name, value)?;
                            count += 1;
                        }
                    }
                }
            }
            Message::EthernetEx(m) => {
                if let Some(sdb) = someip_db {
                    if let Some((svc, mth, vals)) = try_someip_signals(m.ether_type, &m.data, sdb) {
                        let msg_id = ((svc as u32) << 16) | (mth as u32);
                        for (name, value) in vals {
                            writeln!(w, "{},{},0x{:08X},{},{}", ns, m.channel, msg_id, name, value)?;
                            count += 1;
                        }
                    }
                }
            }
            _ => {}
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

    // Parse --threads N, --someip-signals <file>, and collect remaining positional args.
    let mut n_threads: usize = 1;
    let mut someip_signals_path: Option<String> = None;
    let mut positional: Vec<String> = Vec::new();
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        if arg == "--threads" {
            n_threads = iter.next().and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
        } else if arg == "--someip-signals" {
            someip_signals_path = iter.next().cloned();
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
                let someip_db = someip_signals_path
                    .as_deref()
                    .map(|p| SomeIpSignalDb::from_csv(BufReader::new(File::open(p)?)))
                    .transpose()?;
                write_csv_signals(&mut w, &objects, &db, someip_db.as_ref())?
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
