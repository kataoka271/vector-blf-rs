use std::collections::HashMap;
use std::io::{self, Write};

// ---------------------------------------------------------------------------
// Minimal protobuf encoding (wire types only; no generated code required)
// ---------------------------------------------------------------------------

const VARINT: u8 = 0;
const I64: u8 = 1;
const LEN: u8 = 2;

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            return;
        }
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
}

fn tag(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    varint(buf, ((field as u64) << 3) | wire_type as u64);
}

fn pb_u32(buf: &mut Vec<u8>, field: u32, v: u32) {
    tag(buf, field, VARINT);
    varint(buf, v as u64);
}

fn pb_u64(buf: &mut Vec<u8>, field: u32, v: u64) {
    tag(buf, field, VARINT);
    varint(buf, v);
}

fn pb_f64(buf: &mut Vec<u8>, field: u32, v: f64) {
    tag(buf, field, I64);
    buf.extend_from_slice(&v.to_le_bytes());
}

fn pb_str(buf: &mut Vec<u8>, field: u32, s: &str) {
    tag(buf, field, LEN);
    varint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn pb_msg(buf: &mut Vec<u8>, field: u32, msg: &[u8]) {
    tag(buf, field, LEN);
    varint(buf, msg.len() as u64);
    buf.extend_from_slice(msg);
}

// ---------------------------------------------------------------------------
// Perfetto proto field numbers (perfetto/protos/perfetto/trace/*)
// ---------------------------------------------------------------------------

// TracePacket
const PKT_CLOCK_SNAPSHOT: u32 = 6;
const PKT_TRACK_EVENT: u32 = 11;
const PKT_TIMESTAMP: u32 = 8;
const PKT_TIMESTAMP_CLOCK_ID: u32 = 58;
const PKT_TRUSTED_SEQ_ID: u32 = 10;
const PKT_TRACK_DESCRIPTOR: u32 = 60;

// ClockSnapshot.Clock
const CLK_ID: u32 = 1;
const CLK_TIMESTAMP: u32 = 2;
// ClockSnapshot
const SNAP_CLOCKS: u32 = 1;

// TrackDescriptor
const TD_UUID: u32 = 1;
const TD_NAME: u32 = 2;
const TD_COUNTER: u32 = 8;

// CounterDescriptor
const CD_UNIT_NAME: u32 = 4;

// TrackEvent
const TE_TYPE: u32 = 9;
const TE_TRACK_UUID: u32 = 11;
const TE_DOUBLE_COUNTER_VALUE: u32 = 44;

// Builtin clock IDs (perfetto/protos/perfetto/common/builtin_clock.proto)
const CLOCK_REALTIME: u32 = 1;
const CLOCK_BOOTTIME: u32 = 6;

// TYPE_COUNTER = 4
const TYPE_COUNTER: u32 = 4;

// All packets in this writer share one sequence.
const SEQ_ID: u32 = 1;

// ---------------------------------------------------------------------------
// PerfettoWriter
// ---------------------------------------------------------------------------

/// Writes a Perfetto native trace (.perfetto-trace) using counter tracks.
///
/// The file is a sequence of length-delimited `TracePacket` proto messages
/// wrapped in a `Trace` message (field 1). No external protobuf library is
/// needed; the encoding is hand-written using the wire format.
pub struct PerfettoWriter<W: Write> {
    writer: W,
    next_uuid: u64,
    track_map: HashMap<String, u64>,
}

impl<W: Write> PerfettoWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            next_uuid: 1,
            track_map: HashMap::new(),
        }
    }

    // Wrap `packet` in a Trace.packet field and flush to the underlying writer.
    fn emit(&mut self, packet: &[u8]) -> io::Result<()> {
        let mut buf = Vec::with_capacity(packet.len() + 10);
        pb_msg(&mut buf, 1, packet); // Trace.packet = field 1
        self.writer.write_all(&buf)
    }

    /// Emit a `ClockSnapshot` that anchors trace time (BOOTTIME offset 0) to
    /// the given wall-clock nanoseconds, so the Perfetto UI can display
    /// absolute timestamps.
    pub fn write_clock_snapshot(&mut self, realtime_ns: u64) -> io::Result<()> {
        let mut boot_clk = Vec::new();
        pb_u32(&mut boot_clk, CLK_ID, CLOCK_BOOTTIME);
        pb_u64(&mut boot_clk, CLK_TIMESTAMP, 0);

        let mut real_clk = Vec::new();
        pb_u32(&mut real_clk, CLK_ID, CLOCK_REALTIME);
        pb_u64(&mut real_clk, CLK_TIMESTAMP, realtime_ns);

        let mut snap = Vec::new();
        pb_msg(&mut snap, SNAP_CLOCKS, &boot_clk);
        pb_msg(&mut snap, SNAP_CLOCKS, &real_clk);

        let mut pkt = Vec::new();
        pb_msg(&mut pkt, PKT_CLOCK_SNAPSHOT, &snap);
        pb_u32(&mut pkt, PKT_TRUSTED_SEQ_ID, SEQ_ID);
        self.emit(&pkt)
    }

    fn alloc_uuid(&mut self) -> u64 {
        let u = self.next_uuid;
        self.next_uuid += 1;
        u
    }

    fn define_counter_track(&mut self, uuid: u64, name: &str, unit: &str) -> io::Result<()> {
        let mut counter_desc = Vec::new();
        if !unit.is_empty() {
            pb_str(&mut counter_desc, CD_UNIT_NAME, unit);
        }

        let mut td = Vec::new();
        pb_u64(&mut td, TD_UUID, uuid);
        pb_str(&mut td, TD_NAME, name);
        pb_msg(&mut td, TD_COUNTER, &counter_desc);

        let mut pkt = Vec::new();
        pb_msg(&mut pkt, PKT_TRACK_DESCRIPTOR, &td);
        pb_u32(&mut pkt, PKT_TRUSTED_SEQ_ID, SEQ_ID);
        self.emit(&pkt)
    }

    /// Return the UUID for a named counter track, creating it on first use.
    pub fn get_or_create_track(&mut self, name: &str, unit: &str) -> io::Result<u64> {
        if let Some(&uuid) = self.track_map.get(name) {
            return Ok(uuid);
        }
        let uuid = self.alloc_uuid();
        self.define_counter_track(uuid, name, unit)?;
        self.track_map.insert(name.to_owned(), uuid);
        Ok(uuid)
    }

    /// Emit a `TYPE_COUNTER` track event with a double value.
    ///
    /// `timestamp_ns` is nanoseconds since the start of the trace (BOOTTIME).
    pub fn write_counter(
        &mut self,
        timestamp_ns: u64,
        track_uuid: u64,
        value: f64,
    ) -> io::Result<()> {
        let mut event = Vec::new();
        pb_u32(&mut event, TE_TYPE, TYPE_COUNTER);
        pb_u64(&mut event, TE_TRACK_UUID, track_uuid);
        pb_f64(&mut event, TE_DOUBLE_COUNTER_VALUE, value);

        let mut pkt = Vec::new();
        pb_u64(&mut pkt, PKT_TIMESTAMP, timestamp_ns);
        pb_u32(&mut pkt, PKT_TIMESTAMP_CLOCK_ID, CLOCK_BOOTTIME);
        pb_msg(&mut pkt, PKT_TRACK_EVENT, &event);
        pb_u32(&mut pkt, PKT_TRUSTED_SEQ_ID, SEQ_ID);
        self.emit(&pkt)
    }
}
