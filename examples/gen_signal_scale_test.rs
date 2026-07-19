use std::fs::File;
use std::io::{BufWriter, Write as _};

use vector_blf::blf::{CanFd64, Dir, Message};

#[path = "util/anomaly.rs"]
mod anomaly;
#[path = "util/test_writer.rs"]
mod test_writer;
#[path = "util/waveform.rs"]
mod waveform;

use test_writer::TestBlfWriter;

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

fn write_blf(path: &str) {
    let mut writer = TestBlfWriter::create(path);
    for idx in 0..NUM_IDS {
        let id = BASE_ID + idx;
        for sample in 0..SAMPLES_PER_ID {
            let channel = CHANNELS[(idx as usize + sample) % CHANNELS.len()];
            // SIGNALS_PER_ID bytes carry the defined signals, each following its
            // own waveform; pad to a valid CAN-FD length.
            let mut data = waveform::payload(SIGNALS_PER_ID, sample as u32, idx);
            anomaly::apply(
                &mut data,
                idx * SIGNALS_PER_ID,
                sample as u32,
                SAMPLES_PER_ID as u32,
            );
            data.resize(64, 0);
            writer.push(Message::CanFd64(CanFd64 {
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
            }));
        }
    }
    writer.finish("CAN-FD64");
}

fn main() {
    let csv_path = "data/test_signals_5000.csv";
    write_signal_csv(csv_path).expect("write signal csv");
    println!(
        "wrote {} signal definitions to {csv_path}",
        NUM_IDS * SIGNALS_PER_ID
    );

    write_blf("data/test_signals_5000.blf");

    let anomaly_csv = "data/test_signals_5000_anomalies.csv";
    let rows = anomaly::write_ground_truth(
        anomaly_csv,
        NUM_IDS,
        SIGNALS_PER_ID,
        SAMPLES_PER_ID as u32,
        test_writer::ts_at,
    )
    .expect("write anomaly ground truth");
    println!("wrote {rows} anomaly ground-truth rows to {anomaly_csv}");
}
