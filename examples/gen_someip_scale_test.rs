use std::fs::File;
use std::io::{BufWriter, Write as _};

use vector_blf::blf::{Dir, Ethernet, Message};

#[path = "util/anomaly.rs"]
mod anomaly;
#[path = "util/test_writer.rs"]
mod test_writer;
#[path = "util/waveform.rs"]
mod waveform;

use test_writer::TestBlfWriter;

// 100 (service_id, method_id) pairs x 50 signals/message (byte-aligned, 8-bit each) = 5000 signals.
const NUM_SERVICES: u32 = 100;
const SIGNALS_PER_SERVICE: u32 = 50;
const BASE_SERVICE_ID: u32 = 0x1000;
const METHOD_ID: u16 = 0x0001;
// Enough samples to make the per-signal waveforms (sine, square, ...) visible.
const SAMPLES_PER_SERVICE: usize = 100;
const CHANNELS: [u16; 4] = [1, 2, 3, 4];

fn write_signal_csv(path: &str) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(
        w,
        "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset"
    )?;
    for idx in 0..NUM_SERVICES {
        let service_id = BASE_SERVICE_ID + idx;
        for sig in 0..SIGNALS_PER_SERVICE {
            writeln!(
                w,
                "0x{service_id:04X},0x{METHOD_ID:04X},Sig_{idx}_{sig},{sig},0,8,Intel,false,1.0,0.0"
            )?;
        }
    }
    Ok(())
}

/// Wraps a SOME/IP payload in a 16-byte SOME/IP header.
fn someip_frame(service_id: u16, method_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(16 + payload.len());
    f.extend_from_slice(&service_id.to_be_bytes());
    f.extend_from_slice(&method_id.to_be_bytes());
    let length = 8u32 + payload.len() as u32; // remaining bytes after this field
    f.extend_from_slice(&length.to_be_bytes());
    f.extend_from_slice(&0x0000u16.to_be_bytes()); // client_id
    f.extend_from_slice(&0x0000u16.to_be_bytes()); // session_id
    f.push(0x01); // protocol_version
    f.push(0x01); // interface_version
    f.push(0x02); // message_type = Notification
    f.push(0x00); // return_code = Ok
    f.extend_from_slice(payload);
    f
}

/// Wraps a UDP payload in an 8-byte UDP header.
fn udp_frame(src_port: u16, dst_port: u16, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(8 + payload.len());
    f.extend_from_slice(&src_port.to_be_bytes());
    f.extend_from_slice(&dst_port.to_be_bytes());
    let length = 8u16 + payload.len() as u16;
    f.extend_from_slice(&length.to_be_bytes());
    f.extend_from_slice(&0x0000u16.to_be_bytes()); // checksum (unchecked by the parser)
    f.extend_from_slice(payload);
    f
}

/// Wraps a UDP datagram in a minimal (20-byte, no-options) IPv4 header.
fn ipv4_frame(id: u16, src: [u8; 4], dst: [u8; 4], udp_payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(20 + udp_payload.len());
    f.push(0x45); // version=4, ihl=5
    f.push(0x00); // dscp/ecn
    let total_length = 20u16 + udp_payload.len() as u16;
    f.extend_from_slice(&total_length.to_be_bytes());
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(&0x4000u16.to_be_bytes()); // flags=don't fragment
    f.push(64); // ttl
    f.push(17); // protocol = UDP
    f.extend_from_slice(&0x0000u16.to_be_bytes()); // checksum (unchecked by the parser)
    f.extend_from_slice(&src);
    f.extend_from_slice(&dst);
    f.extend_from_slice(udp_payload);
    f
}

fn write_blf(path: &str) {
    let mut writer = TestBlfWriter::create(path);
    for idx in 0..NUM_SERVICES {
        let service_id = (BASE_SERVICE_ID + idx) as u16;
        for sample in 0..SAMPLES_PER_SERVICE {
            let channel = CHANNELS[(idx as usize + sample) % CHANNELS.len()];
            // SIGNALS_PER_SERVICE byte-wide signals, each following its own waveform.
            let mut payload = waveform::payload(SIGNALS_PER_SERVICE, sample as u32, idx);
            anomaly::apply(
                &mut payload,
                idx * SIGNALS_PER_SERVICE,
                sample as u32,
                SAMPLES_PER_SERVICE as u32,
            );
            let someip = someip_frame(service_id, METHOD_ID, &payload);
            let udp = udp_frame(30509, 30509, &someip);
            let ip = ipv4_frame(
                idx as u16,
                [10, 0, 0, (idx % 256) as u8],
                [10, 0, 1, (idx % 256) as u8],
                &udp,
            );

            writer.push(Message::Ethernet(Ethernet {
                channel,
                dir: Dir::Rx,
                src_addr: [0x02, 0x00, 0x00, 0x00, 0x00, (idx % 256) as u8],
                dst_addr: [0x02, 0x00, 0x00, 0x00, 0x01, (idx % 256) as u8],
                vlan: None,
                ether_type: 0x0800,
                data: ip,
            }));
        }
    }
    writer.finish("Ethernet/SOME-IP");
}

fn main() {
    let csv_path = "data/test_someip_signals_5000.csv";
    write_signal_csv(csv_path).expect("write signal csv");
    println!(
        "wrote {} signal definitions to {csv_path}",
        NUM_SERVICES * SIGNALS_PER_SERVICE
    );

    write_blf("data/test_someip_signals_5000.blf");

    let anomaly_csv = "data/test_someip_signals_5000_anomalies.csv";
    let rows = anomaly::write_ground_truth(
        anomaly_csv,
        NUM_SERVICES,
        SIGNALS_PER_SERVICE,
        SAMPLES_PER_SERVICE as u32,
        test_writer::ts_at,
    )
    .expect("write anomaly ground truth");
    println!("wrote {rows} anomaly ground-truth rows to {anomaly_csv}");
}
