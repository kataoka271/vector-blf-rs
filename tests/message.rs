use std::io::Cursor;
use vector_blf_rs::blf::{Can, CanFd, CanFd64, Dir, Message, ObjType};

fn encode_message(msg: &Message) -> Vec<u8> {
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();
    buf
}

fn decode_message(buf: &[u8], obj_type: ObjType) -> Message {
    Message::decode(Cursor::new(buf), obj_type, buf.len() as u32).unwrap()
}

// ── Dir ───────────────────────────────────────────────────────────────────────

#[test]
fn dir_from_u8_known_values() {
    assert_eq!(Dir::from_u8(0), Dir::Tx);
    assert_eq!(Dir::from_u8(1), Dir::Rx);
    assert_eq!(Dir::from_u8(2), Dir::TxRq);
}

#[test]
fn dir_unknown_roundtrip() {
    let d = Dir::from_u8(0xFF);
    assert!(matches!(d, Dir::Unknown(0xFF)));
    assert_eq!(d.to_u8(), 0xFF);
}

#[test]
fn dir_to_u8_roundtrip() {
    for &(d, v) in &[(Dir::Tx, 0u8), (Dir::Rx, 1), (Dir::TxRq, 2)] {
        assert_eq!(Dir::from_u8(v), d);
        assert_eq!(d.to_u8(), v);
    }
}

// ── CAN ───────────────────────────────────────────────────────────────────────

fn make_can(id: u32, is_ext: bool, dir: Dir, rtr: bool, data: &[u8]) -> Message {
    Message::Can(Can {
        channel: 1,
        id,
        is_ext_id: is_ext,
        dir,
        rtr,
        dlc: data.len() as u8,
        data: data.to_vec(),
    })
}

#[test]
fn can_encode_decode_roundtrip_std_id() {
    let msg = make_can(0x123, false, Dir::Rx, false, &[0x01, 0x02, 0x03]);
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanMessage);
    match decoded {
        Message::Can(can) => {
            assert_eq!(can.channel, 1);
            assert_eq!(can.id, 0x123);
            assert!(!can.is_ext_id);
            assert_eq!(can.dir, Dir::Rx);
            assert!(!can.rtr);
            assert_eq!(can.dlc, 3);
            assert_eq!(&can.data[..3], &[0x01, 0x02, 0x03]);
        }
        _ => panic!("expected Can"),
    }
}

#[test]
fn can_encode_decode_roundtrip_ext_id() {
    let msg = make_can(0x1FFFFFFF, true, Dir::Tx, false, &[0xAA, 0xBB]);
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanMessage);
    match decoded {
        Message::Can(can) => {
            assert_eq!(can.id, 0x1FFFFFFF);
            assert!(can.is_ext_id);
            assert_eq!(can.dir, Dir::Tx);
        }
        _ => panic!("expected Can"),
    }
}

#[test]
fn can_encode_decode_rtr_flag() {
    let msg = make_can(0x200, false, Dir::Rx, true, &[]);
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanMessage);
    match decoded {
        Message::Can(can) => assert!(can.rtr),
        _ => panic!("expected Can"),
    }
}

#[test]
fn can_parse_isotp_single_frame() {
    let can = Can {
        channel: 1,
        id: 0x7E0,
        is_ext_id: false,
        dir: Dir::Tx,
        rtr: false,
        dlc: 8,
        data: vec![0x02, 0x3E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    };
    let frame = can.parse_isotp().unwrap();
    match frame {
        vector_blf_rs::blf::IsoTpFrame::SingleFrame { data } => {
            assert_eq!(data, vec![0x3E, 0x00]);
        }
        _ => panic!("expected SingleFrame"),
    }
}

// ── CAN-FD ────────────────────────────────────────────────────────────────────

fn make_canfd(id: u32, dir: Dir, fdf: bool, brs: bool, data: &[u8]) -> Message {
    Message::CanFd(CanFd {
        channel: 2,
        id,
        is_ext_id: false,
        dir,
        rtr: false,
        fdf,
        brs,
        esi: false,
        dlc: data.len() as u8,
        data: data.to_vec(),
    })
}

#[test]
fn canfd_encode_decode_roundtrip() {
    let data: Vec<u8> = (0..12).collect();
    let msg = make_canfd(0x456, Dir::Rx, true, true, &data);
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanFdMessage);
    match decoded {
        Message::CanFd(fd) => {
            assert_eq!(fd.channel, 2);
            assert_eq!(fd.id, 0x456);
            assert_eq!(fd.dir, Dir::Rx);
            assert!(fd.fdf);
            assert!(fd.brs);
            assert!(!fd.esi);
            assert_eq!(fd.data, data);
        }
        _ => panic!("expected CanFd"),
    }
}

#[test]
fn canfd_esi_flag_roundtrip() {
    let msg = Message::CanFd(CanFd {
        channel: 1,
        id: 0x100,
        is_ext_id: false,
        dir: Dir::Tx,
        rtr: false,
        fdf: true,
        brs: false,
        esi: true,
        dlc: 2,
        data: vec![0xCA, 0xFE],
    });
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanFdMessage);
    match decoded {
        Message::CanFd(fd) => assert!(fd.esi),
        _ => panic!("expected CanFd"),
    }
}

#[test]
fn canfd_parse_isotp_first_frame() {
    let fd = CanFd {
        channel: 1,
        id: 0x7E0,
        is_ext_id: false,
        dir: Dir::Tx,
        rtr: false,
        fdf: true,
        brs: true,
        esi: false,
        dlc: 8,
        data: vec![0x10, 0x0A, 0x22, 0xF1, 0x90, 0x01, 0x02, 0x03],
    };
    let frame = fd.parse_isotp().unwrap();
    match frame {
        vector_blf_rs::blf::IsoTpFrame::FirstFrame { total_length, .. } => {
            assert_eq!(total_length, 10);
        }
        _ => panic!("expected FirstFrame"),
    }
}

// ── CAN-FD64 ──────────────────────────────────────────────────────────────────

fn make_canfd64(id: u32, dir: Dir, data: &[u8]) -> Message {
    Message::CanFd64(CanFd64 {
        channel: 3,
        id,
        is_ext_id: false,
        dir,
        rtr: false,
        fdf: true,
        brs: true,
        esi: false,
        dlc: data.len() as u8,
        data: data.to_vec(),
    })
}

#[test]
fn canfd64_encode_decode_roundtrip() {
    let data: Vec<u8> = (0u8..64).collect();
    let msg = make_canfd64(0x789, Dir::Tx, &data);
    let encoded = encode_message(&msg);
    let decoded = decode_message(&encoded, ObjType::CanFdMessage64);
    match decoded {
        Message::CanFd64(fd) => {
            assert_eq!(fd.channel, 3);
            assert_eq!(fd.id, 0x789);
            assert_eq!(fd.dir, Dir::Tx);
            assert!(fd.fdf);
            assert!(fd.brs);
            assert_eq!(fd.data, data);
        }
        _ => panic!("expected CanFd64"),
    }
}

#[test]
fn canfd64_parse_isotp_extended_single_frame() {
    let fd64 = CanFd64 {
        channel: 1,
        id: 0x7E0,
        is_ext_id: false,
        dir: Dir::Tx,
        rtr: false,
        fdf: true,
        brs: true,
        esi: false,
        dlc: 10,
        data: {
            let mut v = vec![0x00, 0x08]; // extended SF, length=8
            v.extend_from_slice(&[0x22, 0xF1, 0x90, 0x01, 0x02, 0x03, 0x04, 0x05]);
            v
        },
    };
    let frame = fd64.parse_isotp().unwrap();
    match frame {
        vector_blf_rs::blf::IsoTpFrame::SingleFrame { data } => {
            assert_eq!(data.len(), 8);
            assert_eq!(data[0], 0x22);
        }
        _ => panic!("expected SingleFrame"),
    }
}

// ── Message::obj_type ─────────────────────────────────────────────────────────

#[test]
fn message_obj_type_matches_variant() {
    assert_eq!(make_can(0, false, Dir::Tx, false, &[]).obj_type(), ObjType::CanMessage);
    assert_eq!(make_canfd(0, Dir::Tx, false, false, &[]).obj_type(), ObjType::CanFdMessage);
    assert_eq!(make_canfd64(0, Dir::Tx, &[]).obj_type(), ObjType::CanFdMessage64);
}
