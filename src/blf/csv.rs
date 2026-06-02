use super::diag::someip::SomeIp;
use super::eth::ip::Ip;
use super::eth::transport::Transport;
use super::message::Message;
use super::signal::{CanSignalDb, ContainerHeader, SomeIpSignalDb};
use super::BaseObject;
use super::Timestamp;
use chrono::{TimeZone, Utc};
use std::borrow::Borrow;
use std::io::Write;

fn ts_ns(ts: Timestamp) -> Option<u64> {
    match ts {
        Timestamp::Nanosecond(n) => Some(n),
        Timestamp::Microsecond(u) => u.checked_mul(1000),
    }
}

fn abs_ts_str(start_ns: u64, relative_ns: u64) -> String {
    let total = start_ns.saturating_add(relative_ns);
    let secs = (total / 1_000_000_000) as i64;
    let nanos = (total % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
        .unwrap_or_default()
}

type SomeIpDecoded<'a> = Option<(u16, u16, Vec<(&'a str, f64)>)>;

fn try_someip_signals<'a>(
    ether_type: u16,
    data: &[u8],
    db: &'a SomeIpSignalDb,
) -> SomeIpDecoded<'a> {
    let ip = Ip::parse(ether_type, data).ok()?;
    let transport = match &ip {
        Ip::V4(v4) => v4.parse_transport().ok()?,
        Ip::V6(v6) => v6.parse_transport().ok()?,
    };
    let someip = match &transport {
        Transport::Udp(udp) => SomeIp::parse(udp.data.as_slice()).ok()?,
        Transport::Tcp(tcp) => SomeIp::parse(tcp.data.as_slice()).ok()?,
    };
    let vals = db.extract(someip.service_id, someip.method_id, &someip.payload);
    if vals.is_empty() {
        None
    } else {
        Some((someip.service_id, someip.method_id, vals))
    }
}

pub fn write_csv_raw<W: Write, T: Borrow<BaseObject>>(
    w: &mut W,
    objects: impl IntoIterator<Item = T>,
    start_ns: u64,
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(
        w,
        "timestamp_ns,absolute_timestamp,type,channel,dir,src_mac,dst_mac,ether_type,vlan_vid,id,ext_id,dlc,data"
    )?;
    let mut count = 0usize;
    for item in objects {
        let obj = item.borrow();
        let Some(ns) = ts_ns(obj.timestamp) else {
            eprintln!(
                "WARNING: skipping object with overflowing timestamp {:?}",
                obj.timestamp
            );
            continue;
        };
        let abs = abs_ts_str(start_ns, ns);
        match &obj.message {
            Message::Can(m) => {
                write!(
                    w,
                    "{},{},CAN,{},{:?},,,,0x{:X},{},{},",
                    ns, abs, m.channel, m.dir, m.id, m.is_ext_id, m.dlc
                )?;
                for b in &m.data {
                    write!(w, "{:02X}", b)?;
                }
                writeln!(w)?;
                count += 1;
            }
            Message::CanFd(m) => {
                write!(
                    w,
                    "{},{},CAN-FD,{},{:?},,,,0x{:X},{},{},",
                    ns, abs, m.channel, m.dir, m.id, m.is_ext_id, m.dlc
                )?;
                for b in &m.data {
                    write!(w, "{:02X}", b)?;
                }
                writeln!(w)?;
                count += 1;
            }
            Message::CanFd64(m) => {
                write!(
                    w,
                    "{},{},CAN-FD64,{},{:?},,,,0x{:X},{},{},",
                    ns, abs, m.channel, m.dir, m.id, m.is_ext_id, m.dlc
                )?;
                for b in &m.data {
                    write!(w, "{:02X}", b)?;
                }
                writeln!(w)?;
                count += 1;
            }
            Message::Ethernet(m) => {
                let a = &m.src_addr;
                let b = &m.dst_addr;
                let vlan_vid = m
                    .vlan
                    .as_ref()
                    .map(|v| v.vid.to_string())
                    .unwrap_or_default();
                write!(w, "{},{},Ethernet,{},{:?},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},0x{:04X},{},,,,",
                    ns, abs, m.channel, m.dir,
                    a[0], a[1], a[2], a[3], a[4], a[5],
                    b[0], b[1], b[2], b[3], b[4], b[5],
                    m.ether_type, vlan_vid)?;
                for byte in &m.data {
                    write!(w, "{:02X}", byte)?;
                }
                writeln!(w)?;
                count += 1;
            }
            Message::EthernetEx(m) => {
                let a = &m.src_addr;
                let b = &m.dst_addr;
                let vlan_vid = m
                    .vlan
                    .as_ref()
                    .map(|v| v.vid.to_string())
                    .unwrap_or_default();
                write!(w, "{},{},EthernetEx,{},{:?},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X},0x{:04X},{},,,,",
                    ns, abs, m.channel, m.dir,
                    a[0], a[1], a[2], a[3], a[4], a[5],
                    b[0], b[1], b[2], b[3], b[4], b[5],
                    m.ether_type, vlan_vid)?;
                for byte in &m.data {
                    write!(w, "{:02X}", byte)?;
                }
                writeln!(w)?;
                count += 1;
            }
            Message::Mf4Signal(m) => {
                writeln!(w, "{},{},Mf4Signal,,,,,,,,{},{}", ns, abs, m.value, m.unit)?;
                count += 1;
            }
            _ => {}
        }
    }
    Ok(count)
}

fn can_extract<'a>(db: &'a CanSignalDb, can_id: u32, data: &[u8]) -> Vec<(&'a str, f64)> {
    if db.is_container(can_id) {
        db.extract_container(can_id, data, ContainerHeader::Short)
    } else {
        db.extract(can_id, data)
    }
}

pub fn write_csv_signals<W: Write, T: Borrow<BaseObject>>(
    w: &mut W,
    objects: impl IntoIterator<Item = T>,
    db: &CanSignalDb,
    someip_db: Option<&SomeIpSignalDb>,
    start_ns: u64,
) -> Result<usize, Box<dyn std::error::Error>> {
    writeln!(
        w,
        "timestamp_ns,absolute_timestamp,channel,message_id,signal_name,value"
    )?;
    let mut count = 0usize;
    for item in objects {
        let obj = item.borrow();
        let Some(ns) = ts_ns(obj.timestamp) else {
            eprintln!(
                "WARNING: skipping object with overflowing timestamp {:?}",
                obj.timestamp
            );
            continue;
        };
        let abs = abs_ts_str(start_ns, ns);
        match &obj.message {
            Message::Can(m) => {
                let vals = can_extract(db, m.id, &m.data);
                for (name, value) in vals {
                    writeln!(
                        w,
                        "{},{},{},0x{:X},{},{}",
                        ns, abs, m.channel, m.id, name, value
                    )?;
                    count += 1;
                }
            }
            Message::CanFd(m) => {
                let vals = can_extract(db, m.id, &m.data);
                for (name, value) in vals {
                    writeln!(
                        w,
                        "{},{},{},0x{:X},{},{}",
                        ns, abs, m.channel, m.id, name, value
                    )?;
                    count += 1;
                }
            }
            Message::CanFd64(m) => {
                let vals = can_extract(db, m.id, &m.data);
                for (name, value) in vals {
                    writeln!(
                        w,
                        "{},{},{},0x{:X},{},{}",
                        ns, abs, m.channel as u32, m.id, name, value
                    )?;
                    count += 1;
                }
            }
            Message::Ethernet(m) => {
                if let Some(sdb) = someip_db {
                    if let Some((svc, mth, vals)) = try_someip_signals(m.ether_type, &m.data, sdb) {
                        let msg_id = ((svc as u32) << 16) | (mth as u32);
                        for (name, value) in vals {
                            writeln!(
                                w,
                                "{},{},{},0x{:08X},{},{}",
                                ns, abs, m.channel, msg_id, name, value
                            )?;
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
                            writeln!(
                                w,
                                "{},{},{},0x{:08X},{},{}",
                                ns, abs, m.channel, msg_id, name, value
                            )?;
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
