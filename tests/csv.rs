use vector_blf::blf::csv::{write_csv_raw, write_csv_signals};
use vector_blf::blf::{
    BaseObject, Can, CanFd, CanFd64, CanSignalDb, Dir, Ethernet, EthernetEx, Message, Timestamp,
    Vlan,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_can(
    ts_ns: u64,
    channel: u16,
    id: u32,
    is_ext_id: bool,
    dir: Dir,
    data: &[u8],
) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::Can(Can {
            channel,
            id,
            is_ext_id,
            dir,
            rtr: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn make_canfd(ts_ns: u64, channel: u16, id: u32, data: &[u8]) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::CanFd(CanFd {
            channel,
            id,
            is_ext_id: false,
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

fn make_canfd64(ts_ns: u64, channel: u8, id: u32, data: &[u8]) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::CanFd64(CanFd64 {
            channel,
            id,
            is_ext_id: false,
            dir: Dir::Tx,
            rtr: false,
            fdf: true,
            brs: false,
            esi: false,
            dlc: data.len() as u8,
            data: data.to_vec(),
        }),
    }
}

fn make_ethernet(
    ts_ns: u64,
    channel: u16,
    dir: Dir,
    src: [u8; 6],
    dst: [u8; 6],
    ether_type: u16,
    vlan: Option<Vlan>,
    data: &[u8],
) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::Ethernet(Ethernet {
            channel,
            dir,
            src_addr: src,
            dst_addr: dst,
            vlan,
            ether_type,
            data: data.to_vec(),
        }),
    }
}

fn make_ethernet_ex(
    ts_ns: u64,
    channel: u16,
    dir: Dir,
    src: [u8; 6],
    dst: [u8; 6],
    ether_type: u16,
    vlan: Option<Vlan>,
    data: &[u8],
) -> BaseObject {
    BaseObject {
        timestamp: Timestamp::Nanosecond(ts_ns),
        message: Message::EthernetEx(EthernetEx {
            channel,
            dir,
            src_addr: src,
            dst_addr: dst,
            vlan,
            ether_type,
            data: data.to_vec(),
        }),
    }
}

fn csv_rows(out: &[u8]) -> Vec<String> {
    String::from_utf8(out.to_vec())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

// ── write_csv_raw ─────────────────────────────────────────────────────────────

#[test]
fn csv_raw_header_only() {
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, std::iter::empty::<BaseObject>()).unwrap();
    assert_eq!(count, 0);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        "timestamp_ns,type,channel,dir,src_mac,dst_mac,ether_type,vlan_vid,id,ext_id,dlc,data"
    );
}

#[test]
fn csv_raw_can_frame() {
    let obj = make_can(1_000_000, 1, 0x123, false, Dir::Rx, &[0x01, 0x02, 0x03]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj]).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1], "1000000,CAN,1,Rx,,,,0x123,false,3,010203");
}

#[test]
fn csv_raw_can_ext_id() {
    let obj = make_can(0, 2, 0x1FFFFFFF, true, Dir::Tx, &[0xFF]);
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj]).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "0,CAN,2,Tx,,,,0x1FFFFFFF,true,1,FF");
}

#[test]
fn csv_raw_canfd_frame() {
    let obj = make_canfd(2_000_000, 3, 0x456, &[0xAA, 0xBB]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj]).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "2000000,CAN-FD,3,Rx,,,,0x456,false,2,AABB");
}

#[test]
fn csv_raw_canfd64_frame() {
    let obj = make_canfd64(3_000_000, 4, 0x789, &[0xDE, 0xAD]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj]).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "3000000,CAN-FD64,4,Tx,,,,0x789,false,2,DEAD");
}

#[test]
fn csv_raw_ethernet_no_vlan() {
    let src = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66];
    let dst = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
    let obj = make_ethernet(5_000_000, 1, Dir::Tx, src, dst, 0x0800, None, &[0xDE, 0xAD]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj]).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "5000000,Ethernet,1,Tx,11:22:33:44:55:66,AA:BB:CC:DD:EE:FF,0x0800,,,,,DEAD"
    );
}

#[test]
fn csv_raw_ethernet_with_vlan() {
    let src = [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06];
    let dst = [0x07u8, 0x08, 0x09, 0x0A, 0x0B, 0x0C];
    let vlan = Vlan {
        tpid: 0x8100,
        pri: 0,
        cfi: 0,
        vid: 100,
    };
    let obj = make_ethernet(
        6_000_000,
        2,
        Dir::Rx,
        src,
        dst,
        0x0800,
        Some(vlan),
        &[0xBE, 0xEF],
    );
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj]).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "6000000,Ethernet,2,Rx,01:02:03:04:05:06,07:08:09:0A:0B:0C,0x0800,100,,,,BEEF"
    );
}

#[test]
fn csv_raw_ethernet_ex() {
    let src = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66];
    let dst = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
    let obj = make_ethernet_ex(7_000_000, 3, Dir::Rx, src, dst, 0x86DD, None, &[0x60]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj]).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "7000000,EthernetEx,3,Rx,11:22:33:44:55:66,AA:BB:CC:DD:EE:FF,0x86DD,,,,,60"
    );
}

#[test]
fn csv_raw_microsecond_timestamp() {
    // Timestamp::Microsecond(5) → 5000 ns in the output
    let obj = BaseObject {
        timestamp: Timestamp::Microsecond(5),
        message: Message::Can(Can {
            channel: 1,
            id: 0x10,
            is_ext_id: false,
            dir: Dir::Rx,
            rtr: false,
            dlc: 1,
            data: vec![0x00],
        }),
    };
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj]).unwrap();
    let rows = csv_rows(&out);
    assert!(
        rows[1].starts_with("5000,"),
        "expected ts 5000, got: {}",
        rows[1]
    );
}

#[test]
fn csv_raw_other_message_skipped() {
    let objs = vec![
        make_can(1_000, 1, 0x100, false, Dir::Rx, &[0x01]),
        BaseObject {
            timestamp: Timestamp::Nanosecond(2_000),
            message: Message::Other(vector_blf::blf::ObjType::AppText, vec![0x00]),
        },
        make_can(3_000, 1, 0x200, false, Dir::Rx, &[0x02]),
    ];
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, objs).unwrap();
    assert_eq!(count, 2, "Other messages should not increment count");
    assert_eq!(csv_rows(&out).len(), 3); // header + 2 CAN rows
}

#[test]
fn csv_raw_count_matches_written() {
    let objs = vec![
        make_can(1_000, 1, 0x100, false, Dir::Rx, &[0x01]),
        make_canfd(2_000, 2, 0x200, &[0x02, 0x03]),
        make_canfd64(3_000, 3, 0x300, &[0x04]),
    ];
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, objs).unwrap();
    assert_eq!(count, 3);
    assert_eq!(csv_rows(&out).len(), 4); // header + 3 data rows
}

#[test]
fn csv_raw_empty_data_field() {
    let obj = make_can(0, 1, 0x10, false, Dir::Tx, &[]);
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj]).unwrap();
    let rows = csv_rows(&out);
    // dlc=0, data hex is empty → trailing comma with nothing after it
    assert_eq!(rows[1], "0,CAN,1,Tx,,,,0x10,false,0,");
}

// ── write_csv_signals ─────────────────────────────────────────────────────────

fn engine_signal_db() -> CanSignalDb {
    let csv = "\
message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
0x100,Throttle,16,8,Intel,false,0.4,0.0
";
    CanSignalDb::from_csv(csv.as_bytes()).unwrap()
}

#[test]
fn csv_signals_header_only() {
    let db = engine_signal_db();
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, std::iter::empty::<BaseObject>(), &db, None).unwrap();
    assert_eq!(count, 0);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], "timestamp_ns,channel,message_id,signal_name,value");
}

#[test]
fn csv_signals_can_match() {
    let db = engine_signal_db();
    // EngineSpeed: raw=0x0100=256 → 256*0.25=64.0
    // Throttle: raw=0x32=50 → 50*0.4=20.0
    let data = [0x00u8, 0x01, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00];
    let obj = make_can(1_000_000, 1, 0x100, false, Dir::Rx, &data);
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, [obj], &db, None).unwrap();
    assert_eq!(count, 2);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 3);
    let has_speed = rows[1..]
        .iter()
        .any(|r| r.contains("EngineSpeed") && r.contains("64"));
    let has_throttle = rows[1..]
        .iter()
        .any(|r| r.contains("Throttle") && r.contains("20"));
    assert!(has_speed, "EngineSpeed row missing: {:?}", &rows[1..]);
    assert!(has_throttle, "Throttle row missing: {:?}", &rows[1..]);
}

#[test]
fn csv_signals_can_no_match() {
    let db = engine_signal_db();
    let obj = make_can(1_000_000, 1, 0x999, false, Dir::Rx, &[0x01, 0x02]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, [obj], &db, None).unwrap();
    assert_eq!(count, 0);
    assert_eq!(csv_rows(&out).len(), 1); // header only
}

#[test]
fn csv_signals_row_format() {
    let db = {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x200,Voltage,0,8,Intel,false,1.0,0.0\n";
        CanSignalDb::from_csv(csv.as_bytes()).unwrap()
    };
    // raw=42 → 42*1.0+0.0 = 42
    let obj = make_can(500_000, 3, 0x200, false, Dir::Rx, &[42]);
    let mut out = Vec::<u8>::new();
    write_csv_signals(&mut out, [obj], &db, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "500000,3,0x200,Voltage,42");
}

#[test]
fn csv_signals_canfd_match() {
    let db = engine_signal_db();
    // EngineSpeed raw=0x0200=512 → 512*0.25=128
    let mut data = [0u8; 8];
    data[0] = 0x00;
    data[1] = 0x02;
    let obj = make_canfd(2_000_000, 1, 0x100, &data);
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, [obj], &db, None).unwrap();
    assert!(count > 0);
    let rows = csv_rows(&out);
    assert!(
        rows[1..]
            .iter()
            .any(|r| r.contains("EngineSpeed") && r.contains("128")),
        "expected EngineSpeed=128: {:?}",
        &rows[1..]
    );
}

#[test]
fn csv_signals_canfd64_match() {
    let db = engine_signal_db();
    // EngineSpeed raw=0x0400=1024 → 1024*0.25=256
    let mut data = [0u8; 8];
    data[0] = 0x00;
    data[1] = 0x04;
    let obj = make_canfd64(3_000_000, 1, 0x100, &data);
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, [obj], &db, None).unwrap();
    assert!(count > 0);
    let rows = csv_rows(&out);
    assert!(
        rows[1..]
            .iter()
            .any(|r| r.contains("EngineSpeed") && r.contains("256")),
        "expected EngineSpeed=256: {:?}",
        &rows[1..]
    );
}

#[test]
fn csv_signals_canfd64_channel_cast_to_u32() {
    // CanFd64.channel is u8; verify it's emitted without loss
    let db = {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x300,Sig,0,8,Intel,false,1.0,0.0\n";
        CanSignalDb::from_csv(csv.as_bytes()).unwrap()
    };
    let obj = make_canfd64(0, 255, 0x300, &[1]);
    let mut out = Vec::<u8>::new();
    write_csv_signals(&mut out, [obj], &db, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "0,255,0x300,Sig,1");
}

#[test]
fn csv_signals_mixed_ids_only_matching_emitted() {
    let db = engine_signal_db(); // only 0x100 defined
    let objs = vec![
        make_can(
            1_000,
            1,
            0x100,
            false,
            Dir::Rx,
            &[0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        make_can(2_000, 1, 0x999, false, Dir::Rx, &[0xFF]),
        make_can(
            3_000,
            1,
            0x100,
            false,
            Dir::Rx,
            &[0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
    ];
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, objs, &db, None).unwrap();
    // Each 0x100 frame has 2 signals (EngineSpeed + Throttle); 0x999 has none
    assert_eq!(count, 4);
}
