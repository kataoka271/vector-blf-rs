use std::fs::File;
use std::io::BufWriter;

use vector_blf::blf::{
    BaseObject, Can, CanFd, CanFd64, Dir, Ethernet, EthernetEx, Message, Timestamp, Writer,
};

const MESSAGE_COUNT: usize = 500;
const CHANNELS: [u16; 4] = [1, 2, 3, 4];
// Fixed message IDs, matching data/test_can_signals.csv so --can-signals decodes them.
const CAN_IDS: [u32; 3] = [0x100, 0x200, 0x300];
const CANFD_ID: u32 = 0x400;
const CANFD64_ID: u32 = 0x500;
const ETHER_TYPES: [u16; 3] = [0x0800, 0x86dd, 0x0806];

fn dir_for(i: usize) -> Dir {
    match i % 3 {
        0 => Dir::Rx,
        1 => Dir::Tx,
        _ => Dir::TxRq,
    }
}

fn mac_for(seed: u8) -> [u8; 6] {
    [
        seed,
        seed.wrapping_add(1),
        seed.wrapping_add(2),
        seed.wrapping_add(3),
        seed.wrapping_add(4),
        seed.wrapping_add(5),
    ]
}

fn main() {
    let path = "data/test_mixed.blf";
    let file = File::create(path).expect("create output file");
    let mut writer = Writer::new(BufWriter::new(file)).expect("init writer");

    let mut written = 0usize;
    let mut ts_ns: u64 = 1_000_000;

    for i in 0..MESSAGE_COUNT {
        let channel = CHANNELS[i % CHANNELS.len()];
        let dir = dir_for(i);

        let message = match i % 5 {
            0 | 1 => {
                // Fixed 8-byte DLC so every signal in data/test_can_signals.csv decodes.
                let id = CAN_IDS[i % CAN_IDS.len()];
                Message::Can(Can {
                    channel,
                    id,
                    is_ext_id: i % 4 == 0,
                    dir,
                    rtr: false,
                    dlc: 8,
                    data: (0..8u32).map(|b| (b + i as u32) as u8).collect(),
                })
            }
            2 => {
                let len = 8 + (i % 57);
                Message::CanFd(CanFd {
                    channel,
                    id: CANFD_ID,
                    is_ext_id: i % 3 == 0,
                    dir,
                    rtr: false,
                    fdf: true,
                    brs: i % 2 == 0,
                    esi: false,
                    dlc: len as u8,
                    data: (0..len).map(|b| (b + i) as u8).collect(),
                })
            }
            3 => {
                let len = 8 + (i % 57);
                Message::CanFd64(CanFd64 {
                    channel: channel as u8,
                    id: CANFD64_ID,
                    is_ext_id: i % 2 == 0,
                    dir,
                    rtr: false,
                    fdf: true,
                    brs: i % 2 == 0,
                    esi: false,
                    dlc: len as u8,
                    data: (0..len).map(|b| (b + i) as u8).collect(),
                })
            }
            _ => {
                let ether_type = ETHER_TYPES[i % ETHER_TYPES.len()];
                let len = 46 + (i % 20);
                let data = (0..len).map(|b| (b + i) as u8).collect();
                if i % 2 == 0 {
                    Message::Ethernet(Ethernet {
                        channel,
                        dir,
                        src_addr: mac_for((i % 250) as u8),
                        dst_addr: mac_for((250 - (i % 250)) as u8),
                        vlan: None,
                        ether_type,
                        data,
                    })
                } else {
                    Message::EthernetEx(EthernetEx {
                        channel,
                        dir,
                        src_addr: mac_for((i % 250) as u8),
                        dst_addr: mac_for((250 - (i % 250)) as u8),
                        vlan: None,
                        ether_type,
                        data,
                    })
                }
            }
        };

        let obj = BaseObject {
            timestamp: Timestamp::Nanosecond(ts_ns),
            message,
        };
        writer.write_base_object(&obj).expect("write object");
        written += 1;
        ts_ns += 100_000_000;
    }

    writer.finish().expect("finish writer");

    println!("wrote {written} objects to {path}");
}
