use super::error::{ParseError, ParseResult};
use std::collections::HashMap;
use std::io::BufRead;

/// Bit ordering convention for signal extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteOrder {
    /// Intel / little-endian: start_bit is the LSB position.
    /// Bit numbering: bit 0 = LSB of byte 0, bit 7 = MSB of byte 0, bit 8 = LSB of byte 1, …
    Intel,
    /// Motorola / big-endian (DBC convention): start_bit is the MSB position.
    /// Same bit numbering as Intel, but traversal wraps across byte boundaries MSB-first.
    Motorola,
}

/// A CAN signal definition. Call `decode()` with the raw CAN data bytes.
#[derive(Debug, Clone)]
pub struct Signal {
    /// Start bit position (LSB for Intel, MSB for Motorola).
    pub start_bit: u32,
    /// Number of bits.
    pub bit_length: u32,
    pub byte_order: ByteOrder,
    pub is_signed: bool,
    /// Physical = raw * scale + offset
    pub scale: f64,
    pub offset: f64,
}

impl Signal {
    /// Decode the physical value from `data`. Returns `None` if the signal
    /// extends outside `data`.
    pub fn decode(&self, data: &[u8]) -> Option<f64> {
        let raw = self.decode_raw(data)?;
        let numeric = if self.is_signed {
            sign_extend(raw, self.bit_length) as f64
        } else {
            raw as f64
        };
        Some(numeric * self.scale + self.offset)
    }

    /// Decode the raw unsigned integer value before scale/offset are applied.
    pub fn decode_raw(&self, data: &[u8]) -> Option<u64> {
        match self.byte_order {
            ByteOrder::Intel => extract_intel(data, self.start_bit, self.bit_length),
            ByteOrder::Motorola => extract_motorola(data, self.start_bit, self.bit_length),
        }
    }
}

/// Intel (little-endian): start_bit is LSB; bits increase toward MSB.
fn extract_intel(data: &[u8], start_bit: u32, bit_length: u32) -> Option<u64> {
    let mut raw = 0u64;
    for i in 0..bit_length {
        let pos = start_bit + i;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        if byte_idx >= data.len() {
            return None;
        }
        raw |= ((data[byte_idx] >> bit_idx) as u64 & 1) << i;
    }
    Some(raw)
}

/// Motorola (big-endian, DBC MSB convention): start_bit is the MSB.
/// Within-byte bit numbering: 0 = LSB, 7 = MSB.
/// At byte boundaries, traversal jumps to the MSB of the next byte (+15).
fn extract_motorola(data: &[u8], start_bit: u32, bit_length: u32) -> Option<u64> {
    let mut raw = 0u64;
    let mut pos = start_bit;
    for i in 0..bit_length {
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        if byte_idx >= data.len() {
            return None;
        }
        let bit_val = (data[byte_idx] >> bit_idx) as u64 & 1;
        // MSB goes into the highest output bit
        raw |= bit_val << (bit_length - 1 - i);
        pos = if pos.is_multiple_of(8) {
            pos + 15
        } else {
            pos - 1
        };
    }
    Some(raw)
}

fn sign_extend(value: u64, bit_length: u32) -> i64 {
    if bit_length == 0 || bit_length >= 64 {
        return value as i64;
    }
    let sign_bit = 1u64 << (bit_length - 1);
    if value & sign_bit != 0 {
        (value | !((1u64 << bit_length) - 1)) as i64
    } else {
        value as i64
    }
}

/// CAN-FD container frame header format. Both variants are big-endian (network byte order).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerHeader {
    /// 2-byte PDU ID + 2-byte length (4-byte overhead per I-PDU). Most common.
    #[default]
    Short,
    /// 4-byte PDU ID + 4-byte length (8-byte overhead per I-PDU).
    Long,
}

/// Demultiplex a CAN-FD container frame payload into `(pdu_id, i_pdu_payload)` pairs.
///
/// Stops at the first header that would extend past the end of `data`.
pub fn demux_container(data: &[u8], header: ContainerHeader) -> Vec<(u32, &[u8])> {
    let mut result = Vec::new();
    let mut pos = 0usize;
    loop {
        let (pdu_id, length) = match header {
            ContainerHeader::Short => {
                if pos + 4 > data.len() {
                    break;
                }
                let id = u16::from_be_bytes([data[pos], data[pos + 1]]) as u32;
                let len = u16::from_be_bytes([data[pos + 2], data[pos + 3]]) as usize;
                pos += 4;
                (id, len)
            }
            ContainerHeader::Long => {
                if pos + 8 > data.len() {
                    break;
                }
                let id =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                let len = u32::from_be_bytes([
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]) as usize;
                pos += 8;
                (id, len)
            }
        };
        if pos + length > data.len() {
            break;
        }
        result.push((pdu_id, &data[pos..pos + length]));
        pos += length;
    }
    result
}

/// A named signal bound to a specific CAN message ID.
#[derive(Debug, Clone)]
pub struct SignalDef {
    pub name: String,
    pub message_id: u32,
    pub signal: Signal,
}

impl SignalDef {
    pub fn decode(&self, data: &[u8]) -> Option<f64> {
        self.signal.decode(data)
    }
}

/// A collection of signal definitions keyed by CAN message ID, loadable from CSV.
///
/// CSV format (header required):
/// ```text
/// message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset[,pdu_id]
/// 0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
/// 0x200,BrakeForce,7,12,Motorola,true,0.1,-100.0
/// 0x300,ContainerSig,0,8,Intel,false,1.0,0.0,0x10
/// ```
/// `message_id` accepts hex (`0x…`) or decimal. `byte_order` is `Intel` or `Motorola`
/// (case-insensitive). `is_signed` accepts `true`/`false` or `1`/`0`.
/// The optional ninth column `pdu_id` marks the row as a container-frame signal;
/// `start_bit` is then the bit offset within the I-PDU payload after demultiplexing.
#[derive(Debug, Default)]
pub struct CanSignalDb {
    /// Regular-frame signals keyed by CAN ID.
    frames: HashMap<u32, Vec<SignalDef>>,
    /// Container-frame signals keyed by CAN ID → PDU ID → signals.
    containers: HashMap<u32, HashMap<u32, Vec<SignalDef>>>,
}

impl CanSignalDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_csv<R: std::io::Read>(reader: R) -> ParseResult<Self> {
        let mut db = Self::new();
        for line in std::io::BufReader::new(reader).lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("message_id") {
                continue;
            }
            let p: Vec<&str> = line.splitn(9, ',').map(str::trim).collect();
            if p.len() < 8 {
                return Err(ParseError::InvalidData);
            }
            let message_id = parse_u32(p[0])?;
            let name = p[1].to_string();
            let start_bit = parse_u32(p[2])?;
            let bit_length = parse_u32(p[3])?;
            let byte_order = match p[4].to_lowercase().as_str() {
                "intel" => ByteOrder::Intel,
                "motorola" => ByteOrder::Motorola,
                _ => return Err(ParseError::InvalidData),
            };
            let is_signed = match p[5].to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(ParseError::InvalidData),
            };
            let scale = p[6].parse::<f64>().map_err(|_| ParseError::InvalidData)?;
            let offset = p[7].parse::<f64>().map_err(|_| ParseError::InvalidData)?;
            let def = SignalDef {
                name,
                message_id,
                signal: Signal {
                    start_bit,
                    bit_length,
                    byte_order,
                    is_signed,
                    scale,
                    offset,
                },
            };
            if p.len() >= 9 && !p[8].is_empty() {
                let pdu_id = parse_u32(p[8])?;
                db.insert_container(def, pdu_id);
            } else {
                db.insert(def);
            }
        }
        Ok(db)
    }

    /// Insert a regular-frame signal definition.
    pub fn insert(&mut self, def: SignalDef) {
        self.frames.entry(def.message_id).or_default().push(def);
    }

    /// Insert a container-frame signal definition bound to `pdu_id` within `can_id`.
    pub fn insert_container(&mut self, def: SignalDef, pdu_id: u32) {
        self.containers
            .entry(def.message_id)
            .or_default()
            .entry(pdu_id)
            .or_default()
            .push(def);
    }

    /// Returns `true` if `can_id` is configured as a container frame.
    pub fn is_container(&self, can_id: u32) -> bool {
        self.containers.contains_key(&can_id)
    }

    /// All regular-frame signal definitions for the given CAN ID.
    pub fn signals(&self, message_id: u32) -> &[SignalDef] {
        self.frames
            .get(&message_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Decode all regular-frame signals for `message_id` from `data`.
    /// Signals that extend outside `data` are silently skipped.
    pub fn extract(&self, message_id: u32, data: &[u8]) -> Vec<(&str, f64)> {
        self.signals(message_id)
            .iter()
            .filter_map(|def| def.signal.decode(data).map(|v| (def.name.as_str(), v)))
            .collect()
    }

    /// Demultiplex a container frame and decode all signals from the contained I-PDUs.
    ///
    /// I-PDUs whose PDU ID is not in the database are silently skipped.
    /// Signal bit positions are relative to each I-PDU payload (after stripping the header).
    pub fn extract_container<'a>(
        &'a self,
        can_id: u32,
        data: &[u8],
        header: ContainerHeader,
    ) -> Vec<(&'a str, f64)> {
        let Some(pdu_map) = self.containers.get(&can_id) else {
            return vec![];
        };
        let mut result = Vec::new();
        for (pdu_id, payload) in demux_container(data, header) {
            if let Some(defs) = pdu_map.get(&pdu_id) {
                for def in defs {
                    if let Some(v) = def.signal.decode(payload) {
                        result.push((def.name.as_str(), v));
                    }
                }
            }
        }
        result
    }
}

fn parse_u32(s: &str) -> ParseResult<u32> {
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"));
    match hex {
        Some(h) => u32::from_str_radix(h, 16).map_err(|_| ParseError::InvalidData),
        None => s.parse::<u32>().map_err(|_| ParseError::InvalidData),
    }
}

fn parse_u16(s: &str) -> ParseResult<u16> {
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"));
    match hex {
        Some(h) => u16::from_str_radix(h, 16).map_err(|_| ParseError::InvalidData),
        None => s.parse::<u16>().map_err(|_| ParseError::InvalidData),
    }
}

/// A named signal bound to a specific SOME/IP (service_id, method_id) pair.
#[derive(Debug, Clone)]
pub struct SomeIpSignalDef {
    pub name: String,
    pub service_id: u16,
    pub method_id: u16,
    pub signal: Signal,
}

impl SomeIpSignalDef {
    pub fn decode(&self, data: &[u8]) -> Option<f64> {
        self.signal.decode(data)
    }
}

/// Signal definitions for SOME/IP messages, keyed by (service_id, method_id).
///
/// CSV format (header required):
/// ```text
/// service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
/// 0x0064,0x0001,Temperature,0,16,Intel,false,0.01,0.0
/// ```
/// `service_id` and `method_id` accept hex (`0x…`) or decimal.
/// Signals are decoded from the SOME/IP payload (bytes after the 16-byte header).
#[derive(Debug, Default)]
pub struct SomeIpSignalDb {
    map: HashMap<(u16, u16), Vec<SomeIpSignalDef>>,
}

impl SomeIpSignalDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_csv<R: std::io::Read>(reader: R) -> ParseResult<Self> {
        let mut db = Self::new();
        for line in std::io::BufReader::new(reader).lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("service_id") {
                continue;
            }
            let p: Vec<&str> = line.splitn(9, ',').map(str::trim).collect();
            if p.len() < 9 {
                return Err(ParseError::InvalidData);
            }
            let service_id = parse_u16(p[0])?;
            let method_id = parse_u16(p[1])?;
            let name = p[2].to_string();
            let start_bit = parse_u32(p[3])?;
            let bit_length = parse_u32(p[4])?;
            let byte_order = match p[5].to_lowercase().as_str() {
                "intel" => ByteOrder::Intel,
                "motorola" => ByteOrder::Motorola,
                _ => return Err(ParseError::InvalidData),
            };
            let is_signed = match p[6].to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(ParseError::InvalidData),
            };
            let scale = p[7].parse::<f64>().map_err(|_| ParseError::InvalidData)?;
            let offset = p[8].parse::<f64>().map_err(|_| ParseError::InvalidData)?;
            db.insert(SomeIpSignalDef {
                name,
                service_id,
                method_id,
                signal: Signal {
                    start_bit,
                    bit_length,
                    byte_order,
                    is_signed,
                    scale,
                    offset,
                },
            });
        }
        Ok(db)
    }

    pub fn insert(&mut self, def: SomeIpSignalDef) {
        self.map
            .entry((def.service_id, def.method_id))
            .or_default()
            .push(def);
    }

    /// All signal definitions for the given (service_id, method_id) pair.
    pub fn signals(&self, service_id: u16, method_id: u16) -> &[SomeIpSignalDef] {
        self.map
            .get(&(service_id, method_id))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Decode all signals for `(service_id, method_id)` from `payload`.
    pub fn extract(&self, service_id: u16, method_id: u16, payload: &[u8]) -> Vec<(&str, f64)> {
        self.signals(service_id, method_id)
            .iter()
            .filter_map(|def| def.signal.decode(payload).map(|v| (def.name.as_str(), v)))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(start_bit: u32, bit_length: u32, byte_order: ByteOrder, is_signed: bool) -> Signal {
        Signal {
            start_bit,
            bit_length,
            byte_order,
            is_signed,
            scale: 1.0,
            offset: 0.0,
        }
    }

    #[test]
    fn intel_full_byte() {
        // 8-bit Intel signal starting at bit 0 → entire first byte
        let s = sig(0, 8, ByteOrder::Intel, false);
        assert_eq!(s.decode_raw(&[0xAB, 0x00]), Some(0xAB));
    }

    #[test]
    fn intel_cross_byte() {
        // 12-bit Intel signal at bit 4: bits 4-15
        let s = sig(4, 12, ByteOrder::Intel, false);
        // data = [0xF0, 0x0A] → bits 4-7 = 0xF, bits 8-15 = 0x0A → raw = 0x0AF
        assert_eq!(s.decode_raw(&[0xF0, 0x0A]), Some(0x0AF));
    }

    #[test]
    fn motorola_full_byte() {
        // 8-bit Motorola signal, MSB at bit 7 → entire first byte
        let s = sig(7, 8, ByteOrder::Motorola, false);
        assert_eq!(s.decode_raw(&[0xAB]), Some(0xAB));
    }

    #[test]
    fn motorola_two_bytes() {
        // 16-bit Motorola signal, MSB at bit 15 → byte 1 (high) + byte 2 (low)
        let s = sig(15, 16, ByteOrder::Motorola, false);
        assert_eq!(s.decode_raw(&[0x00, 0x12, 0x34]), Some(0x1234));
    }

    #[test]
    fn signed_negative() {
        // 8-bit signed Intel, value 0xFF = -1
        let s = sig(0, 8, ByteOrder::Intel, true);
        assert_eq!(s.decode(&[0xFF]), Some(-1.0));
    }

    #[test]
    fn scale_offset() {
        let s = Signal {
            start_bit: 0,
            bit_length: 8,
            byte_order: ByteOrder::Intel,
            is_signed: false,
            scale: 0.5,
            offset: -40.0,
        };
        // raw=100 → 100*0.5 - 40 = 10.0
        assert_eq!(s.decode(&[100]), Some(10.0));
    }

    #[test]
    fn csv_round_trip() {
        let csv = "\
message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
0x100,Throttle,16,8,Intel,false,0.4,0.0
0x200,BrakeForce,7,12,Motorola,true,0.1,-100.0
";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x100).len(), 2);
        assert_eq!(db.signals(0x200).len(), 1);
        assert_eq!(db.signals(0x300).len(), 0);

        // EngineSpeed: raw=0x0100 → 256 * 0.25 = 64.0
        let data = [0x00u8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let vals = db.extract(0x100, &data);
        let speed = vals
            .iter()
            .find(|(n, _)| *n == "EngineSpeed")
            .map(|(_, v)| *v);
        assert_eq!(speed, Some(64.0));
    }

    #[test]
    fn csv_decimal_id() {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   256,Sig,0,8,Intel,false,1.0,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(256).len(), 1); // 256 == 0x100
    }

    #[test]
    fn csv_skips_comments_and_blanks() {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   # this is a comment\n\
                   \n\
                   0x10,Voltage,0,8,Intel,false,0.1,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x10).len(), 1);
    }

    #[test]
    fn someip_csv_round_trip() {
        let csv = "\
service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
0x0064,0x0001,Temperature,0,16,Intel,false,0.01,0.0
0x0064,0x0001,Pressure,16,8,Intel,false,1.0,0.0
0x0064,0x0002,Speed,0,16,Motorola,false,0.5,0.0
";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x0064, 0x0001).len(), 2);
        assert_eq!(db.signals(0x0064, 0x0002).len(), 1);
        assert_eq!(db.signals(0x0064, 0x0003).len(), 0);

        // Temperature: raw=0x0190 (400) → 400 * 0.01 = 4.0
        // Pressure: raw=0x32 (50) → 50 * 1.0 = 50.0
        let payload = [0x90u8, 0x01, 0x32];
        let vals = db.extract(0x0064, 0x0001, &payload);
        let temp = vals
            .iter()
            .find(|(n, _)| *n == "Temperature")
            .map(|(_, v)| *v);
        let pres = vals.iter().find(|(n, _)| *n == "Pressure").map(|(_, v)| *v);
        assert_eq!(temp, Some(4.0));
        assert_eq!(pres, Some(50.0));
    }

    #[test]
    fn someip_csv_decimal_ids() {
        let csv = "service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   100,1,Sig,0,8,Intel,false,1.0,0.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(100, 1).len(), 1); // 100 == 0x64, 1 == 0x01
    }

    #[test]
    fn someip_csv_skips_header_and_blanks() {
        let csv = "service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   # comment\n\
                   \n\
                   0x0001,0x0002,Sig,0,8,Intel,false,1.0,0.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x0001, 0x0002).len(), 1);
    }

    #[test]
    fn someip_csv_signed_scale_offset() {
        let csv = "service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x0010,0x0003,Temp,0,8,Intel,true,0.5,-40.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        // raw=0xFF = -1 signed → -1 * 0.5 - 40.0 = -40.5
        let vals = db.extract(0x0010, 0x0003, &[0xFF]);
        assert_eq!(vals.first().map(|(_, v)| *v), Some(-40.5));
    }

    // ── container frame tests ─────────────────────────────────────────────────

    #[test]
    fn demux_short_header() {
        // Two I-PDUs: PDU 0x0010 with 2 bytes, PDU 0x0020 with 1 byte
        // [0x00, 0x10, 0x00, 0x02, 0xAB, 0xCD, 0x00, 0x20, 0x00, 0x01, 0xFF]
        let data = [
            0x00u8, 0x10, 0x00, 0x02, 0xAB, 0xCD, 0x00, 0x20, 0x00, 0x01, 0xFF,
        ];
        let pdus = demux_container(&data, ContainerHeader::Short);
        assert_eq!(pdus.len(), 2);
        assert_eq!(pdus[0], (0x0010, [0xAB, 0xCD].as_slice()));
        assert_eq!(pdus[1], (0x0020, [0xFF].as_slice()));
    }

    #[test]
    fn demux_long_header() {
        // PDU 0x00000010 with 2 bytes: [0x00,0x00,0x00,0x10, 0x00,0x00,0x00,0x02, 0xAB, 0xCD]
        let data = [0x00u8, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0xAB, 0xCD];
        let pdus = demux_container(&data, ContainerHeader::Long);
        assert_eq!(pdus.len(), 1);
        assert_eq!(pdus[0], (0x10u32, [0xAB, 0xCD].as_slice()));
    }

    #[test]
    fn demux_truncated_stops_early() {
        // Header says length=5 but only 2 bytes available after header
        let data = [0x00u8, 0x01, 0x00, 0x05, 0xAA, 0xBB];
        let pdus = demux_container(&data, ContainerHeader::Short);
        assert!(pdus.is_empty());
    }

    #[test]
    fn container_csv_round_trip() {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x200,Sig1,0,8,Intel,false,1.0,0.0,0x10\n\
                   0x200,Sig2,8,8,Intel,false,2.0,0.0,0x10\n\
                   0x200,Sig3,0,8,Intel,false,0.5,0.0,0x20\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert!(db.is_container(0x200));
        assert!(!db.is_container(0x100));

        // Build a container frame: PDU 0x10 → [0x05, 0x0A], PDU 0x20 → [0x08]
        let frame = [
            0x00u8, 0x10, 0x00, 0x02, 0x05, 0x0A, // PDU 0x10: Sig1=5, Sig2=10
            0x00, 0x20, 0x00, 0x01, 0x08, // PDU 0x20: Sig3=8*0.5=4.0
        ];
        let vals = db.extract_container(0x200, &frame, ContainerHeader::Short);
        let get = |name: &str| vals.iter().find(|(n, _)| *n == name).map(|(_, v)| *v);
        assert_eq!(get("Sig1"), Some(5.0));
        assert_eq!(get("Sig2"), Some(20.0));
        assert_eq!(get("Sig3"), Some(4.0));
    }

    #[test]
    fn container_pdu_order_independent() {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x600,Radar_Distance_m,0,16,Intel,false,0.01,0.0,0x01\n\
                   0x600,CabinTemp_degC,0,8,Intel,true,0.5,-40.0,0x02\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();

        // PDU 0x01 first, then PDU 0x02
        let frame_ab = [
            0x00u8, 0x01, 0x00, 0x02, 0x40, 0x1F, // PDU 0x01: raw=0x1F40=8000 → 80.0 m
            0x00, 0x02, 0x00, 0x01, 0x64, // PDU 0x02: raw=0x64=100 → 100*0.5-40=10.0 °C
        ];
        // PDU 0x02 first, then PDU 0x01
        let frame_ba = [
            0x00u8, 0x02, 0x00, 0x01, 0x64, // PDU 0x02
            0x00, 0x01, 0x00, 0x02, 0x40, 0x1F, // PDU 0x01
        ];

        let vals_ab = db.extract_container(0x600, &frame_ab, ContainerHeader::Short);
        let vals_ba = db.extract_container(0x600, &frame_ba, ContainerHeader::Short);

        let get = |vals: &[(&str, f64)], name: &str| {
            vals.iter().find(|(n, _)| *n == name).map(|(_, v)| *v)
        };
        assert_eq!(get(&vals_ab, "Radar_Distance_m"), Some(80.0));
        assert_eq!(get(&vals_ab, "CabinTemp_degC"), Some(10.0));
        assert_eq!(
            get(&vals_ba, "Radar_Distance_m"),
            get(&vals_ab, "Radar_Distance_m")
        );
        assert_eq!(
            get(&vals_ba, "CabinTemp_degC"),
            get(&vals_ab, "CabinTemp_degC")
        );
    }

    #[test]
    fn container_and_regular_signals_coexist() {
        let csv = "message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x100,Direct,0,8,Intel,false,1.0,0.0\n\
                   0x200,Contained,0,8,Intel,false,1.0,0.0,0x01\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert!(!db.is_container(0x100));
        assert!(db.is_container(0x200));
        assert_eq!(db.extract(0x100, &[0x42]).len(), 1);
        let frame = [0x00u8, 0x01, 0x00, 0x01, 0x07];
        assert_eq!(
            db.extract_container(0x200, &frame, ContainerHeader::Short)
                .len(),
            1
        );
    }
}
