use std::cell::RefCell;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::rc::Rc;
use vector_blf::blf::{BaseObject, Can, CanFd, Dir, Ethernet, Message, Mf4Signal, Timestamp};
use vector_blf::mf4;

// ── shared in-memory buffer ───────────────────────────────────────────────────

#[derive(Clone)]
struct SharedCursor(Rc<RefCell<Cursor<Vec<u8>>>>);

impl SharedCursor {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(Cursor::new(Vec::new()))))
    }
    fn rewind(&self) {
        self.0.borrow_mut().set_position(0);
    }
    fn len(&self) -> usize {
        self.0.borrow().get_ref().len()
    }
}

impl Write for SharedCursor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.borrow_mut().flush()
    }
}
impl Seek for SharedCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.borrow_mut().seek(pos)
    }
}
impl Read for SharedCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

const START_NS: u64 = 1_700_000_000_000_000_000; // 2023-11-14 ~22:13 UTC

fn can_obj(channel: u16, id: u32, data: &[u8], ts_ns: u64) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::Can(Can {
            channel,
            id,
            is_ext_id: false,
            dir: Dir::Rx,
            rtr: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn canfd_obj(channel: u16, id: u32, data: &[u8], ts_ns: u64) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::CanFd(CanFd {
            channel,
            id,
            is_ext_id: false,
            dir: Dir::Tx,
            rtr: false,
            fdf: true,
            brs: true,
            esi: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn sig_obj(group: &str, name: &str, value: f64, unit: &str, ts_ns: u64) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::Mf4Signal(Mf4Signal {
            group: group.to_string(),
            name: name.to_string(),
            value,
            unit: unit.to_string(),
        }),
    }
}

fn eth_obj(ts_ns: u64) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::Ethernet(Ethernet {
            channel: 1,
            dir: Dir::Rx,
            src_addr: [0x11, 0x22, 0x33, 0x44, 0x55, 0x66],
            dst_addr: [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            vlan: None,
            ether_type: 0x0800,
            data: vec![0x45, 0x00, 0x00, 0x14],
        }),
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[test]
fn test_mf4_can_roundtrip() {
    let buf = SharedCursor::new();
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.write_base_object(&can_obj(1, 0x100, &[0x01, 0x02, 0x03, 0x04], START_NS))
            .unwrap();
        w.write_base_object(&can_obj(2, 0x200, &[0xFF], START_NS + 1_000_000))
            .unwrap();
        w.write_base_object(&can_obj(1, 0x300, &[0xAA, 0xBB], START_NS + 2_000_000))
            .unwrap();
        w.finish().unwrap();
    }
    assert!(buf.len() > 64, "file should have content");
    buf.rewind();

    let mut r = mf4::Reader::new(buf.clone()).unwrap();
    assert_eq!(r.start_time_ns, START_NS);

    let obj1 = r.next().unwrap().unwrap();
    assert!(
        matches!(&obj1.message, Message::Can(c) if c.id == 0x100 && c.data == vec![0x01,0x02,0x03,0x04])
    );

    let obj2 = r.next().unwrap().unwrap();
    assert!(matches!(&obj2.message, Message::Can(c) if c.id == 0x200 && c.data == vec![0xFF]));

    let obj3 = r.next().unwrap().unwrap();
    assert!(matches!(&obj3.message, Message::Can(c) if c.id == 0x300));

    assert!(r.next().is_none(), "no more objects expected");
}

#[test]
fn test_mf4_canfd_roundtrip() {
    let buf = SharedCursor::new();
    let data = vec![0u8; 64];
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.write_base_object(&canfd_obj(1, 0x7FF, &data, START_NS))
            .unwrap();
        w.finish().unwrap();
    }
    buf.rewind();

    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(objs.len(), 1);
    match &objs[0].message {
        Message::CanFd(m) => {
            assert_eq!(m.id, 0x7FF);
            assert_eq!(m.data.len(), 64);
            assert!(m.fdf);
            assert!(m.brs);
        }
        other => panic!("expected CanFd, got {:?}", other),
    }
}

#[test]
fn test_mf4_signal_roundtrip() {
    let buf = SharedCursor::new();
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.write_base_object(&sig_obj(
            "Powertrain",
            "EngineSpeed",
            1234.5,
            "rpm",
            START_NS,
        ))
        .unwrap();
        w.write_base_object(&sig_obj(
            "Powertrain",
            "EngineSpeed",
            1300.0,
            "rpm",
            START_NS + 10_000_000,
        ))
        .unwrap();
        w.write_base_object(&sig_obj(
            "Powertrain",
            "Torque",
            42.0,
            "Nm",
            START_NS + 5_000_000,
        ))
        .unwrap();
        w.finish().unwrap();
    }
    buf.rewind();

    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    // 2 channels × their respective sample counts
    assert_eq!(objs.len(), 3, "3 signal samples expected");
    let mut found_speed = false;
    let mut found_torque = false;
    for obj in &objs {
        match &obj.message {
            Message::Mf4Signal(s) => {
                assert_eq!(s.group, "Powertrain");
                if s.name == "EngineSpeed" {
                    found_speed = true;
                    assert!(s.value == 1234.5 || s.value == 1300.0);
                    assert_eq!(s.unit, "rpm");
                } else if s.name == "Torque" {
                    found_torque = true;
                    assert!((s.value - 42.0).abs() < 1e-9);
                    assert_eq!(s.unit, "Nm");
                }
            }
            other => panic!("expected Mf4Signal, got {:?}", other),
        }
    }
    assert!(found_speed, "EngineSpeed signal not found");
    assert!(found_torque, "Torque signal not found");
}

#[test]
fn test_mf4_ethernet_roundtrip() {
    let buf = SharedCursor::new();
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.write_base_object(&eth_obj(START_NS)).unwrap();
        w.finish().unwrap();
    }
    buf.rewind();

    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(objs.len(), 1);
    match &objs[0].message {
        Message::Ethernet(m) => {
            assert_eq!(m.ether_type, 0x0800);
            assert_eq!(m.src_addr, [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        }
        other => panic!("expected Ethernet, got {:?}", other),
    }
}

#[test]
fn test_mf4_mixed_types() {
    let buf = SharedCursor::new();
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.write_base_object(&can_obj(1, 0x100, &[1, 2], START_NS))
            .unwrap();
        w.write_base_object(&sig_obj("Test", "Voltage", 12.3, "V", START_NS + 1_000_000))
            .unwrap();
        w.write_base_object(&canfd_obj(1, 0x200, &[0; 8], START_NS + 2_000_000))
            .unwrap();
        w.finish().unwrap();
    }
    buf.rewind();

    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    // The writer groups by message type (CAN/CAN-FD together, then scalar signals),
    // so check presence rather than order.
    assert_eq!(objs.len(), 3);
    let can_count = objs
        .iter()
        .filter(|o| matches!(o.message, Message::Can(_)))
        .count();
    let canfd_count = objs
        .iter()
        .filter(|o| matches!(o.message, Message::CanFd(_)))
        .count();
    let sig_count = objs
        .iter()
        .filter(|o| matches!(o.message, Message::Mf4Signal(_)))
        .count();
    assert_eq!(can_count, 1);
    assert_eq!(canfd_count, 1);
    assert_eq!(sig_count, 1);
}

#[test]
fn test_mf4_empty_file() {
    // A writer with no objects should still produce a valid MF4 file.
    let buf = SharedCursor::new();
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        w.finish().unwrap();
    }
    assert!(buf.len() >= 64 + 104, "must have at least ID + HD blocks");
    // Reading it back should yield no objects, not an error.
    buf.rewind();
    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert!(objs.is_empty());
}

#[test]
fn test_mf4_can_timestamp_ordering() {
    let buf = SharedCursor::new();
    let timestamps: Vec<u64> = vec![START_NS, START_NS + 500_000_000, START_NS + 1_000_000_000];
    {
        let mut w = mf4::Writer::new(buf.clone(), START_NS).unwrap();
        for (i, &ts) in timestamps.iter().enumerate() {
            w.write_base_object(&can_obj(1, i as u32, &[i as u8], ts))
                .unwrap();
        }
        w.finish().unwrap();
    }
    buf.rewind();

    let objs: Vec<_> = mf4::Reader::new(buf)
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(objs.len(), 3);
    let ts: Vec<u64> = objs
        .iter()
        .map(|o| match o.timestamp {
            Timestamp::Nanosecond(n) => n,
            Timestamp::Microsecond(u) => u * 1000,
        })
        .collect();
    // Timestamps should be in non-decreasing order and reasonably close to input values.
    assert!(ts[0] <= ts[1] && ts[1] <= ts[2]);
    // Each reconstructed timestamp should be within 1ms of the original.
    for (orig, got) in timestamps.iter().zip(ts.iter()) {
        let delta = orig.abs_diff(*got);
        assert!(delta < 1_000_000, "timestamp drift too large: {} ns", delta);
    }
}

#[test]
fn test_mf4_invalid_magic() {
    // A buffer with wrong magic should return InvalidMf4Magic.
    let mut buf = vec![0u8; 64];
    buf[0..8].copy_from_slice(b"NOTMDF  ");
    use vector_blf::blf::ParseError;
    let cursor = Cursor::new(buf);
    match mf4::Reader::new(cursor) {
        Err(ParseError::InvalidMf4Magic) => {}
        Err(e) => panic!("expected InvalidMf4Magic, got Err({e})"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}
