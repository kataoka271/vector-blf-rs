use vector_blf::blf::csv::{write_csv_raw, write_csv_signals};
use vector_blf::blf::{
    check_can_csv, check_someip_csv, BaseObject, Can, CanFd, CanFd64, CanSignalDb, Dir, Ethernet,
    EthernetEx, Message, Timestamp, Vlan,
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
    let count = write_csv_raw(&mut out, std::iter::empty::<BaseObject>(), 0, None).unwrap();
    assert_eq!(count, 0);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        "timestamp_ns,absolute_timestamp,type,channel,dir,src_mac,dst_mac,ether_type,vlan_vid,id,ext_id,dlc,data"
    );
}

#[test]
fn csv_raw_can_frame() {
    let obj = make_can(1_000_000, 1, 0x123, false, Dir::Rx, &[0x01, 0x02, 0x03]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj], 0, None).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1],
        "1000000,1970-01-01T00:00:00.001000000Z,CAN,1,Rx,,,,0x123,false,3,010203"
    );
}

#[test]
fn csv_raw_can_ext_id() {
    let obj = make_can(0, 2, 0x1FFFFFFF, true, Dir::Tx, &[0xFF]);
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj], 0, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "0,1970-01-01T00:00:00.000000000Z,CAN,2,Tx,,,,0x1FFFFFFF,true,1,FF"
    );
}

#[test]
fn csv_raw_canfd_frame() {
    let obj = make_canfd(2_000_000, 3, 0x456, &[0xAA, 0xBB]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj], 0, None).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "2000000,1970-01-01T00:00:00.002000000Z,CAN-FD,3,Rx,,,,0x456,false,2,AABB"
    );
}

#[test]
fn csv_raw_canfd64_frame() {
    let obj = make_canfd64(3_000_000, 4, 0x789, &[0xDE, 0xAD]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj], 0, None).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "3000000,1970-01-01T00:00:00.003000000Z,CAN-FD64,4,Tx,,,,0x789,false,2,DEAD"
    );
}

#[test]
fn csv_raw_ethernet_no_vlan() {
    let src = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66];
    let dst = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
    let obj = make_ethernet(5_000_000, 1, Dir::Tx, src, dst, 0x0800, None, &[0xDE, 0xAD]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj], 0, None).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "5000000,1970-01-01T00:00:00.005000000Z,Ethernet,1,Tx,11:22:33:44:55:66,AA:BB:CC:DD:EE:FF,0x0800,,,,,DEAD"
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
    write_csv_raw(&mut out, [obj], 0, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "6000000,1970-01-01T00:00:00.006000000Z,Ethernet,2,Rx,01:02:03:04:05:06,07:08:09:0A:0B:0C,0x0800,100,,,,BEEF"
    );
}

#[test]
fn csv_raw_ethernet_ex() {
    let src = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66];
    let dst = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
    let obj = make_ethernet_ex(7_000_000, 3, Dir::Rx, src, dst, 0x86DD, None, &[0x60]);
    let mut out = Vec::<u8>::new();
    let count = write_csv_raw(&mut out, [obj], 0, None).unwrap();
    assert_eq!(count, 1);
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "7000000,1970-01-01T00:00:00.007000000Z,EthernetEx,3,Rx,11:22:33:44:55:66,AA:BB:CC:DD:EE:FF,0x86DD,,,,,60"
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
    write_csv_raw(&mut out, [obj], 0, None).unwrap();
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
    let count = write_csv_raw(&mut out, objs, 0, None).unwrap();
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
    let count = write_csv_raw(&mut out, objs, 0, None).unwrap();
    assert_eq!(count, 3);
    assert_eq!(csv_rows(&out).len(), 4); // header + 3 data rows
}

#[test]
fn csv_raw_empty_data_field() {
    let obj = make_can(0, 1, 0x10, false, Dir::Tx, &[]);
    let mut out = Vec::<u8>::new();
    write_csv_raw(&mut out, [obj], 0, None).unwrap();
    let rows = csv_rows(&out);
    // dlc=0, data hex is empty → trailing comma with nothing after it
    assert_eq!(
        rows[1],
        "0,1970-01-01T00:00:00.000000000Z,CAN,1,Tx,,,,0x10,false,0,"
    );
}

// ── write_csv_signals ─────────────────────────────────────────────────────────

fn engine_signal_db() -> CanSignalDb {
    let csv = "\
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x100,EngineSpeed,0,0,16,Intel,false,0.25,0.0
0x100,Throttle,2,0,8,Intel,false,0.4,0.0
";
    CanSignalDb::from_csv(csv.as_bytes()).unwrap()
}

#[test]
fn csv_signals_header_only() {
    let db = engine_signal_db();
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(
        &mut out,
        std::iter::empty::<BaseObject>(),
        &db,
        None,
        0,
        None,
    )
    .unwrap();
    assert_eq!(count, 0);
    let rows = csv_rows(&out);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        "timestamp_ns,absolute_timestamp,channel,message_id,signal_name,value"
    );
}

#[test]
fn csv_signals_can_match() {
    let db = engine_signal_db();
    // EngineSpeed: raw=0x0100=256 → 256*0.25=64.0
    // Throttle: raw=0x32=50 → 50*0.4=20.0
    let data = [0x00u8, 0x01, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00];
    let obj = make_can(1_000_000, 1, 0x100, false, Dir::Rx, &data);
    let mut out = Vec::<u8>::new();
    let count = write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
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
    let count = write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
    assert_eq!(count, 0);
    assert_eq!(csv_rows(&out).len(), 1); // header only
}

#[test]
fn csv_signals_row_format() {
    let db = {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x200,Voltage,0,0,8,Intel,false,1.0,0.0\n";
        CanSignalDb::from_csv(csv.as_bytes()).unwrap()
    };
    // raw=42 → 42*1.0+0.0 = 42
    let obj = make_can(500_000, 3, 0x200, false, Dir::Rx, &[42]);
    let mut out = Vec::<u8>::new();
    write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(
        rows[1],
        "500000,1970-01-01T00:00:00.000500000Z,3,0x200,Voltage,42"
    );
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
    let count = write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
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
    let count = write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
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
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x300,Sig,0,0,8,Intel,false,1.0,0.0\n";
        CanSignalDb::from_csv(csv.as_bytes()).unwrap()
    };
    let obj = make_canfd64(0, 255, 0x300, &[1]);
    let mut out = Vec::<u8>::new();
    write_csv_signals(&mut out, [obj], &db, None, 0, None).unwrap();
    let rows = csv_rows(&out);
    assert_eq!(rows[1], "0,1970-01-01T00:00:00.000000000Z,255,0x300,Sig,1");
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
    let count = write_csv_signals(&mut out, objs, &db, None, 0, None).unwrap();
    // Each 0x100 frame has 2 signals (EngineSpeed + Throttle); 0x999 has none
    assert_eq!(count, 4);
}

// ── check_can_csv ─────────────────────────────────────────────────────────────

#[test]
fn check_can_csv_valid() {
    let csv = "\
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x100,EngineSpeed,0,0,16,Intel,false,0.25,0.0
0x200,BrakeForce,0,7,12,Motorola,true,0.1,-100.0
";
    assert!(check_can_csv(csv.as_bytes()).is_empty());
}

#[test]
fn check_can_csv_too_few_columns() {
    let csv = "message_id,signal_name,start_bit\n0x100,Speed,0\n";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 2);
    assert!(errors[0].1.contains("expected at least 9 columns"));
}

#[test]
fn check_can_csv_bad_message_id() {
    let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\nnot_a_number,Speed,0,0,16,Intel,false,1.0,0.0\n";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("invalid message_id"));
}

#[test]
fn check_can_csv_bad_byte_order() {
    let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n0x100,Speed,0,0,16,BigEndian,false,1.0,0.0\n";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("invalid byte_order"));
}

#[test]
fn check_can_csv_bad_is_signed() {
    let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n0x100,Speed,0,0,16,Intel,yes,1.0,0.0\n";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("invalid is_signed"));
}

#[test]
fn check_can_csv_bad_scale_and_offset() {
    let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n0x100,Speed,0,0,16,Intel,false,abc,xyz\n";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|(_, m)| m.contains("invalid scale")));
    assert!(errors.iter().any(|(_, m)| m.contains("invalid offset")));
}

#[test]
fn check_can_csv_collects_multiple_rows() {
    let csv = "\
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
bad_id,Speed,0,0,16,Intel,false,1.0,0.0
0x200,Throttle,0,7,12,Motorola,true,0.1,-100.0
another_bad,Brake,0,0,8,Intel,false,1.0,0.0
";
    let errors = check_can_csv(csv.as_bytes());
    assert_eq!(errors.len(), 2, "should collect errors from all bad rows");
    assert_eq!(errors[0].0, 2);
    assert_eq!(errors[1].0, 4);
}

#[test]
fn check_can_csv_skips_comments_and_blanks() {
    let csv = "\
# comment line
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset

0x100,Speed,0,0,16,Intel,false,1.0,0.0
";
    assert!(check_can_csv(csv.as_bytes()).is_empty());
}

// ── check_someip_csv ──────────────────────────────────────────────────────────

#[test]
fn check_someip_csv_valid() {
    let csv = "\
service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0
";
    assert!(check_someip_csv(csv.as_bytes()).is_empty());
}

#[test]
fn check_someip_csv_too_few_columns() {
    let csv = "service_id,method_id\n0x64,0x01\n";
    let errors = check_someip_csv(csv.as_bytes());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("expected at least 10 columns"));
}

#[test]
fn check_someip_csv_bad_service_and_method_id() {
    let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\nnope,nope,Temp,0,0,16,Intel,false,1.0,0.0\n";
    let errors = check_someip_csv(csv.as_bytes());
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|(_, m)| m.contains("invalid service_id")));
    assert!(errors.iter().any(|(_, m)| m.contains("invalid method_id")));
}

#[test]
fn check_someip_csv_collects_multiple_rows() {
    let csv = "\
service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
bad,0x01,Temp,0,0,16,Intel,false,1.0,0.0
0x64,0x01,Temp,0,0,16,Intel,false,1.0,0.0
bad,0x01,Pressure,0,0,8,Intel,false,1.0,0.0
";
    let errors = check_someip_csv(csv.as_bytes());
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].0, 2);
    assert_eq!(errors[1].0, 4);
}
