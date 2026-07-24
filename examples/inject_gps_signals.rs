//! Injects a synthetic GPS track (CAN ID 0x700, channel 1) into
//! data/test_logfile.blf, decodable via assets/can_signals.csv as
//! GPS_Latitude/GPS_Longitude. Preserves every existing object in the file
//! and the original file header's start_timestamp. Backs up the original
//! file to data/test_logfile.blf.bak before replacing it.

use std::f64::consts::PI;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};

use vector_blf::blf::{BaseObject, Can, Dir, Message, Reader, Timestamp, Writer};

const TARGET: &str = "data/test_logfile.blf";
const GPS_CAN_ID: u32 = 0x700;
const GPS_CHANNEL: u16 = 1;
const POINT_COUNT: usize = 50;
// A small loop around Tokyo Station, matching the coordinates used by the
// Signal Viewer dummy-data GPS candidates (databricks/signal-viewer/app/dummy.py).
const LAT_CENTER: f64 = 35.6895;
const LON_CENTER: f64 = 139.6917;
const RADIUS_DEG: f64 = 0.01;
// int32 raw * 1e-7 degrees, the common NMEA/OBD GPS fixed-point convention.
const SCALE: f64 = 0.0000001;

fn ts_ns(ts: Timestamp) -> u64 {
    match ts {
        Timestamp::Nanosecond(ns) => ns,
        Timestamp::Microsecond(us) => us * 1000,
    }
}

fn encode_gps(lat: f64, lon: f64) -> Vec<u8> {
    let raw_lat = (lat / SCALE).round() as i32;
    let raw_lon = (lon / SCALE).round() as i32;
    let mut data = vec![0u8; 8];
    data[0..4].copy_from_slice(&raw_lat.to_le_bytes());
    data[4..8].copy_from_slice(&raw_lon.to_le_bytes());
    data
}

fn main() {
    let file = File::open(TARGET).expect("open source BLF");
    let reader = Reader::new(BufReader::new(file)).expect("init reader");
    let start_timestamp = reader.header.start_timestamp;

    let mut objects: Vec<BaseObject> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("parse existing objects");
    let original_count = objects.len();

    let min_ns = objects
        .iter()
        .map(|o| ts_ns(o.timestamp))
        .min()
        .unwrap_or(0);
    let max_ns = objects
        .iter()
        .map(|o| ts_ns(o.timestamp))
        .max()
        .unwrap_or(min_ns + 1_000_000_000);

    for i in 0..POINT_COUNT {
        let frac = i as f64 / (POINT_COUNT - 1) as f64;
        let ts = min_ns + ((max_ns - min_ns) as f64 * frac).round() as u64;
        let angle = 2.0 * PI * frac;
        let lat = LAT_CENTER + RADIUS_DEG * angle.sin();
        let lon = LON_CENTER + RADIUS_DEG * angle.cos();
        objects.push(BaseObject {
            timestamp: Timestamp::Nanosecond(ts),
            message: Message::Can(Can {
                channel: GPS_CHANNEL,
                id: GPS_CAN_ID,
                is_ext_id: false,
                dir: Dir::Rx,
                rtr: false,
                dlc: 8,
                data: encode_gps(lat, lon),
            }),
        });
    }

    // BLF objects must be in non-decreasing timestamp order.
    objects.sort_by_key(|o| ts_ns(o.timestamp));

    let tmp_path = format!("{TARGET}.tmp");
    {
        let out_file = File::create(&tmp_path).expect("create temp output");
        let mut writer = Writer::new(BufWriter::new(out_file)).expect("init writer");
        writer.header.start_timestamp = start_timestamp;
        writer.header.stop_timestamp = start_timestamp;
        for obj in &objects {
            writer.write_base_object(obj).expect("write object");
        }
        writer.finish().expect("finish writer");
    }

    let backup_path = format!("{TARGET}.bak");
    fs::rename(TARGET, &backup_path).expect("back up original file");
    fs::rename(&tmp_path, TARGET).expect("install rewritten file");

    println!(
        "injected {POINT_COUNT} GPS points (CAN ID {GPS_CAN_ID:#x}, channel {GPS_CHANNEL}) into {TARGET} \
         ({original_count} -> {} objects); original backed up to {backup_path}",
        original_count + POINT_COUNT
    );
}
