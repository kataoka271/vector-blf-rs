use std::fs::File;
use std::io::{BufWriter, Write as _};

use vector_blf::blf::{BaseObject, CanFd64, Dir, Message, Timestamp, Writer};

#[path = "util/waveform.rs"]
mod waveform;

// 100 message IDs x 50 signals/message (byte-aligned, 8-bit each) = 5000 signals.
const NUM_IDS: u32 = 100;
const SIGNALS_PER_ID: u32 = 50;
const BASE_ID: u32 = 0x600;
// Enough samples to make the per-signal waveforms (sine, square, ...) visible.
const SAMPLES_PER_ID: usize = 100;
const CHANNELS: [u8; 4] = [1, 2, 3, 4];

fn write_signal_csv(path: &str) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(
        w,
        "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id"
    )?;
    for idx in 0..NUM_IDS {
        let id = BASE_ID + idx;
        for sig in 0..SIGNALS_PER_ID {
            writeln!(w, "0x{id:X},Sig_{idx}_{sig},{sig},0,8,Intel,false,1.0,0.0,")?;
        }
    }
    Ok(())
}

fn write_blf(path: &str) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut writer = Writer::new(BufWriter::new(file)).expect("init writer");

    let mut ts_ns: u64 = 1_000_000;
    let mut written = 0usize;
    for idx in 0..NUM_IDS {
        let id = BASE_ID + idx;
        for sample in 0..SAMPLES_PER_ID {
            let channel = CHANNELS[(idx as usize + sample) % CHANNELS.len()];
            // SIGNALS_PER_ID bytes carry the defined signals; pad to a valid CAN-FD length.
            // Each signal follows its own waveform (shape cycles by signal index).
            let mut data: Vec<u8> = (0..SIGNALS_PER_ID)
                .map(|sig| waveform::sample(sig, sample as u32, idx * SIGNALS_PER_ID + sig))
                .collect();
            data.resize(64, 0);
            let obj = BaseObject {
                timestamp: Timestamp::Nanosecond(ts_ns),
                message: Message::CanFd64(CanFd64 {
                    channel,
                    id,
                    is_ext_id: id > 0x7ff,
                    dir: Dir::Rx,
                    rtr: false,
                    fdf: true,
                    brs: true,
                    esi: false,
                    dlc: data.len() as u8,
                    data,
                }),
            };
            writer.write_base_object(&obj).expect("write object");
            written += 1;
            ts_ns += 100_000_000;
        }
    }
    writer.finish().expect("finish writer");
    println!("wrote {written} CAN-FD64 objects to {path}");
    Ok(())
}

fn main() {
    let csv_path = "data/test_signals_5000.csv";
    write_signal_csv(csv_path).expect("write signal csv");
    println!(
        "wrote {} signal definitions to {csv_path}",
        NUM_IDS * SIGNALS_PER_ID
    );

    write_blf("data/test_signals_5000.blf").expect("write blf");
}
