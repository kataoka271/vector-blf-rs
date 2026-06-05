use std::cell::RefCell;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::rc::Rc;
use vector_blf::blf::{
    parse_at, scan_containers, BaseObject, Can, CanFd, CanFd64, Dir, Message, ParseError, Reader,
    Timestamp, Writer,
};

/// Shareable in-memory buffer implementing both Write+Seek and Read+Seek so
/// the same underlying Cursor can be used for a Writer then a Reader.
#[derive(Clone)]
struct SharedCursor(Rc<RefCell<Cursor<Vec<u8>>>>);

impl SharedCursor {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(Cursor::new(Vec::new()))))
    }

    fn rewind(&self) {
        self.0.borrow_mut().set_position(0);
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

fn can_obj(id: u32, data: &[u8]) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(1_000_000),
        message: Message::Can(Can {
            channel: 1,
            id,
            is_ext_id: false,
            dir: Dir::Rx,
            rtr: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn canfd_obj(id: u32, data: &[u8]) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(2_000_000),
        message: Message::CanFd(CanFd {
            channel: 2,
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

fn canfd64_obj(id: u32, data: &[u8]) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(3_000_000),
        message: Message::CanFd64(CanFd64 {
            channel: 3,
            id,
            is_ext_id: true,
            dir: Dir::Rx,
            rtr: false,
            fdf: true,
            brs: true,
            esi: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn write_then_read(objects: Vec<BaseObject>) -> Vec<BaseObject> {
    let sc = SharedCursor::new();
    let sc_reader = sc.clone();

    let mut writer = Writer::new(sc).unwrap();
    for obj in &objects {
        writer.write_base_object(obj).unwrap();
    }
    writer.finish().unwrap();

    sc_reader.rewind();
    let reader = Reader::new(sc_reader).unwrap();
    reader.map(|r| r.unwrap()).collect()
}

// ── basic roundtrip ───────────────────────────────────────────────────────────

#[test]
fn roundtrip_single_can_frame() {
    let objs = vec![can_obj(0x123, &[0x01, 0x02, 0x03])];
    let got = write_then_read(objs);

    assert_eq!(got.len(), 1);
    match &got[0].message {
        Message::Can(can) => {
            assert_eq!(can.id, 0x123);
            assert_eq!(can.channel, 1);
            assert_eq!(can.dir, Dir::Rx);
            assert_eq!(can.dlc, 3);
            assert_eq!(&can.data[..3], &[0x01, 0x02, 0x03]);
        }
        _ => panic!("expected Can"),
    }
}

#[test]
fn roundtrip_multiple_can_frames() {
    let objs = vec![
        can_obj(0x100, &[0xAA]),
        can_obj(0x200, &[0xBB, 0xCC]),
        can_obj(0x300, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
    ];
    let got = write_then_read(objs);
    assert_eq!(got.len(), 3);

    let ids: Vec<u32> = got
        .iter()
        .map(|o| match &o.message {
            Message::Can(c) => c.id,
            _ => panic!("expected Can"),
        })
        .collect();
    assert_eq!(ids, vec![0x100, 0x200, 0x300]);
}

#[test]
fn roundtrip_canfd_frame() {
    let data: Vec<u8> = (0..12).collect();
    let objs = vec![canfd_obj(0x456, &data)];
    let got = write_then_read(objs);

    assert_eq!(got.len(), 1);
    match &got[0].message {
        Message::CanFd(fd) => {
            assert_eq!(fd.id, 0x456);
            assert_eq!(fd.channel, 2);
            assert_eq!(fd.dir, Dir::Tx);
            assert!(fd.fdf);
            assert!(fd.brs);
            assert_eq!(fd.data, data);
        }
        _ => panic!("expected CanFd"),
    }
}

#[test]
fn roundtrip_canfd64_frame() {
    let data: Vec<u8> = (0u8..64).collect();
    let objs = vec![canfd64_obj(0x789, &data)];
    let got = write_then_read(objs);

    assert_eq!(got.len(), 1);
    match &got[0].message {
        Message::CanFd64(fd) => {
            assert_eq!(fd.id, 0x789);
            assert_eq!(fd.channel, 3);
            assert!(fd.is_ext_id);
            assert_eq!(fd.data, data);
        }
        _ => panic!("expected CanFd64"),
    }
}

#[test]
fn roundtrip_mixed_frame_types() {
    let objs = vec![
        can_obj(0x111, &[0x01]),
        canfd_obj(0x222, &[0x02, 0x03]),
        can_obj(0x333, &[0x04, 0x05, 0x06]),
    ];
    let got = write_then_read(objs);
    assert_eq!(got.len(), 3);

    assert!(matches!(&got[0].message, Message::Can(c) if c.id == 0x111));
    assert!(matches!(&got[1].message, Message::CanFd(fd) if fd.id == 0x222));
    assert!(matches!(&got[2].message, Message::Can(c) if c.id == 0x333));
}

#[test]
fn roundtrip_object_count_tracked() {
    let sc = SharedCursor::new();
    let sc_reader = sc.clone();

    let mut writer = Writer::new(sc).unwrap();
    for i in 0..5 {
        writer.write_base_object(&can_obj(i, &[i as u8])).unwrap();
    }
    writer.finish().unwrap();

    sc_reader.rewind();
    let reader = Reader::new(sc_reader).unwrap();
    assert_eq!(reader.header.object_count, 5);
}

#[test]
fn roundtrip_empty_file() {
    let sc = SharedCursor::new();
    let sc_reader = sc.clone();

    let mut writer = Writer::new(sc).unwrap();
    writer.finish().unwrap();

    sc_reader.rewind();
    let reader = Reader::new(sc_reader).unwrap();
    let objects: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert!(objects.is_empty());
}

#[test]
fn roundtrip_can_ext_id() {
    let objs = vec![BaseObject {
        timestamp: Timestamp::Nanosecond(0),
        message: Message::Can(Can {
            channel: 1,
            id: 0x1FFFFFFF,
            is_ext_id: true,
            dir: Dir::Tx,
            rtr: false,
            dlc: 1,
            data: vec![0xFF],
        }),
    }];
    let got = write_then_read(objs);
    match &got[0].message {
        Message::Can(can) => {
            assert!(can.is_ext_id);
            assert_eq!(can.id, 0x1FFFFFFF);
        }
        _ => panic!("expected Can"),
    }
}

#[test]
fn roundtrip_many_frames_spans_multiple_containers() {
    // Write enough frames that the internal 4096-byte buffer flushes at least once,
    // exercising the multi-container path in the Writer/Reader.
    let mut objs = Vec::new();
    for i in 0u32..200 {
        objs.push(can_obj(i & 0x7FF, &[i as u8, (i >> 8) as u8, 0x00]));
    }
    let got = write_then_read(objs);
    assert_eq!(got.len(), 200);
    for (i, obj) in got.iter().enumerate() {
        match &obj.message {
            Message::Can(can) => assert_eq!(can.id, (i as u32) & 0x7FF),
            _ => panic!("expected Can at index {i}"),
        }
    }
}

// ── direct-mode BLF (no LogContainers) ───────────────────────────────────────

#[test]
fn reader_direct_mode_no_containers() {
    let f = std::fs::File::open("data/technica/errors/FileWithoutLogContainers.blf").unwrap();
    let objects: Vec<_> = Reader::new(std::io::BufReader::new(f))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(!objects.is_empty(), "expected at least one object");
}

#[test]
fn scan_containers_direct_mode_returns_none() {
    let mut f = std::io::BufReader::new(
        std::fs::File::open("data/technica/errors/FileWithoutLogContainers.blf").unwrap(),
    );
    let (_header, offsets) = scan_containers(&mut f).unwrap();
    assert!(
        offsets.is_none(),
        "expected None offsets for direct-mode BLF"
    );
}

// ── truncated object detection ───────────────────────────────────────────────

#[test]
fn parse_at_errors_on_truncated_object() {
    let path = "data/technica/errors/FileWithTruncatedCanMessage.blf";
    let mut f = std::io::BufReader::new(std::fs::File::open(path).unwrap());
    let (_header, offsets) = scan_containers(&mut f).unwrap();
    let offsets = offsets.unwrap();
    let result = parse_at(&mut std::fs::File::open(path).unwrap(), &offsets);
    assert!(
        matches!(result, Err(ParseError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof),
        "expected UnexpectedEof, got {result:?}"
    );
}

#[test]
fn reader_errors_on_truncated_object() {
    let path = "data/technica/errors/FileWithTruncatedCanMessage.blf";
    let f = std::io::BufReader::new(std::fs::File::open(path).unwrap());
    let results: Vec<_> = Reader::new(f).unwrap().collect();
    let has_error = results.iter().any(
        |r| matches!(r, Err(ParseError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof),
    );
    assert!(
        has_error,
        "expected an UnexpectedEof error, got {results:?}"
    );
}

// ── cross-container object stitching ─────────────────────────────────────────

/// Constructs a BLF file in memory where two CAN objects are stored across
/// two uncompressed LogContainers, with the split falling mid-object.
/// Verifies that parse_at stitches the pieces and returns both objects.
#[test]
fn parse_at_stitches_object_spanning_containers() {
    use std::io::Cursor;

    // Serialize a single CAN BaseObject to 48 bytes:
    //   BaseObjectHeader (16) + ObjectHeaderV1 (16) + CAN message (16)
    let make_can_bytes = |ts_ns: u64, id: u32| -> [u8; 48] {
        let mut b = [0u8; 48];
        // BaseObjectHeader
        b[0..4].copy_from_slice(b"LOBJ");
        b[4..6].copy_from_slice(&16u16.to_le_bytes()); // header_size
        b[6..8].copy_from_slice(&1u16.to_le_bytes()); // version
        b[8..12].copy_from_slice(&48u32.to_le_bytes()); // obj_size
        b[12..16].copy_from_slice(&1u32.to_le_bytes()); // obj_type = CanMessage
                                                        // ObjectHeaderV1 (flags=0 → Nanosecond)
        b[16..20].copy_from_slice(&0u32.to_le_bytes()); // flags
        b[20..22].copy_from_slice(&0u16.to_le_bytes()); // client
        b[22..24].copy_from_slice(&0u16.to_le_bytes()); // obj_version
        b[24..32].copy_from_slice(&ts_ns.to_le_bytes()); // timestamp
                                                         // CAN message: channel=1, flags=1 (Dir::Rx), dlc=3, id, data[8]
        b[32..34].copy_from_slice(&1u16.to_le_bytes()); // channel
        b[34] = 1; // flags = Dir::Rx
        b[35] = 3; // dlc
        b[36..40].copy_from_slice(&id.to_le_bytes()); // can_id
                                                      // data[8] left as zeros
        b
    };

    let can1 = make_can_bytes(1_000_000, 0x123);
    let can2 = make_can_bytes(2_000_000, 0x456);

    // Split can2 at byte 20: 16 bytes of BaseObjectHeader + 4 bytes into ObjectHeaderV1.
    let split = 20usize;

    // Container 1 payload: complete can1 + first `split` bytes of can2 = 68 bytes
    let mut payload1: Vec<u8> = Vec::new();
    payload1.extend_from_slice(&can1);
    payload1.extend_from_slice(&can2[..split]);

    // Container 2 payload: remaining bytes of can2 = 28 bytes
    let payload2 = &can2[split..];

    let make_lc = |payload: &[u8]| -> Vec<u8> {
        let data_len = payload.len() as u32;
        let obj_size = 32u32 + data_len;
        let mut lc: Vec<u8> = Vec::new();
        lc.extend_from_slice(b"LOBJ");
        lc.extend_from_slice(&16u16.to_le_bytes()); // header_size
        lc.extend_from_slice(&1u16.to_le_bytes()); // version
        lc.extend_from_slice(&obj_size.to_le_bytes());
        lc.extend_from_slice(&10u32.to_le_bytes()); // obj_type = LogContainer
                                                    // LogContainerHeader
        lc.extend_from_slice(&0u16.to_le_bytes()); // compression_method = 0 (none)
        lc.extend_from_slice(&[0u8; 6]); // padding
        lc.extend_from_slice(&data_len.to_le_bytes()); // uncompressed_size
        lc.extend_from_slice(&[0u8; 4]); // padding
        lc.extend_from_slice(payload);
        let pad = obj_size as usize % 4;
        if pad > 0 {
            lc.extend_from_slice(&[0u8; 4][..pad]);
        }
        lc
    };

    let lc1 = make_lc(&payload1);
    let lc2 = make_lc(payload2);

    // Assemble the full BLF: FileHeader (144 bytes) + LC1 + LC2
    let mut blf: Vec<u8> = Vec::new();
    blf.extend_from_slice(b"LOGG");
    blf.extend_from_slice(&144u32.to_le_bytes()); // header_size
    blf.extend_from_slice(&[0u8; 8]); // version bytes
    blf.extend_from_slice(&0u64.to_le_bytes()); // file_size
    blf.extend_from_slice(&0u64.to_le_bytes()); // uncompressed_size
    blf.extend_from_slice(&2u32.to_le_bytes()); // object_count
    blf.extend_from_slice(&0u32.to_le_bytes()); // count_read
    blf.extend_from_slice(&[0u8; 16]); // start_timestamp (zeros → Nanosecond(0))
    blf.extend_from_slice(&[0u8; 16]); // stop_timestamp
    blf.extend_from_slice(&[0u8; 72]); // padding to 144 bytes
    blf.extend_from_slice(&lc1);
    blf.extend_from_slice(&lc2);

    let mut cursor = Cursor::new(blf);
    let (_header, offsets) = scan_containers(&mut cursor).unwrap();
    let offsets = offsets.expect("expected LogContainer-based BLF");
    assert_eq!(offsets.len(), 2, "expected 2 LogContainers");

    let result = parse_at(&mut cursor, &offsets);
    assert!(
        result.is_ok(),
        "expected Ok for spanning object, got {result:?}"
    );
    let objects = result.unwrap();
    assert_eq!(objects.len(), 2, "expected 2 parsed objects");

    match &objects[0].message {
        Message::Can(can) => assert_eq!(can.id, 0x123),
        _ => panic!("expected Can at index 0"),
    }
    match &objects[1].message {
        Message::Can(can) => assert_eq!(can.id, 0x456),
        _ => panic!("expected Can at index 1"),
    }
}

// ── FileHeader metadata ───────────────────────────────────────────────────────

#[test]
fn file_header_file_size_nonzero_after_finish() {
    let sc = SharedCursor::new();
    let sc_reader = sc.clone();

    let mut writer = Writer::new(sc).unwrap();
    writer.write_base_object(&can_obj(0x1, &[0x01])).unwrap();
    writer.finish().unwrap();

    sc_reader.rewind();
    let reader = Reader::new(sc_reader).unwrap();
    assert!(reader.header.file_size > 0);
}
