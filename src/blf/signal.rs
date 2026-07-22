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
    /// 24-bit PDU ID + 8-bit DLC (4-byte overhead per I-PDU). Standard CAN-FD container format.
    #[default]
    Short,
    /// 4-byte PDU ID + 4-byte length (8-byte overhead per I-PDU).
    Long,
}

/// CAN-FD DLC → payload byte length (ISO 11898-1 Table 3).
fn canfd_dlc_to_len(dlc: u8) -> usize {
    match dlc {
        0..=8 => dlc as usize,
        9 => 12,
        10 => 16,
        11 => 20,
        12 => 24,
        13 => 32,
        14 => 48,
        15 => 64,
        _ => dlc as usize,
    }
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
                let id =
                    (data[pos] as u32) << 16 | (data[pos + 1] as u32) << 8 | (data[pos + 2] as u32);
                let len = canfd_dlc_to_len(data[pos + 3]);
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
/// message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset[,pdu_id]
/// 0x100,EngineSpeed,0,0,16,Intel,false,0.25,0.0
/// 0x200,BrakeForce,0,7,12,Motorola,true,0.1,-100.0
/// 0x300,ContainerSig,0,0,8,Intel,false,1.0,0.0,0x10
/// ```
/// `message_id` accepts hex (`0x…`) or decimal. `byte_order` is `Intel` or `Motorola`
/// (case-insensitive). `is_signed` accepts `true`/`false` or `1`/`0`.
/// The optional tenth column `pdu_id` marks the row as a container-frame signal;
/// `start_byte` and `start_bit` are then relative to the I-PDU payload after demultiplexing.
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
        for (i, line) in std::io::BufReader::new(reader).lines().enumerate() {
            let lineno = i + 1;
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("message_id") {
                continue;
            }
            let p: Vec<&str> = line.split(',').map(str::trim).collect();
            if p.len() < 9 {
                return Err(ParseError::Csv {
                    line: lineno,
                    message: format!("expected at least 9 columns, got {}", p.len()),
                });
            }
            let message_id = parse_u32(p[0]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("message_id", p[0]),
            })?;
            let name = p[1].to_string();
            let start_byte = parse_u32(p[2]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("start_byte", p[2]),
            })?;
            let start_bit_in_byte = parse_u32(p[3]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("start_bit", p[3]),
            })?;
            let bit_length = parse_u32(p[4]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("bit_length", p[4]),
            })?;
            let byte_order = match p[5].to_lowercase().as_str() {
                "intel" => ByteOrder::Intel,
                "motorola" => ByteOrder::Motorola,
                _ => {
                    return Err(ParseError::Csv {
                        line: lineno,
                        message: format!(
                            "invalid byte_order: {:?}, expected Intel or Motorola",
                            p[5]
                        ),
                    })
                }
            };
            let is_signed = match p[6].to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(ParseError::Csv {
                        line: lineno,
                        message: format!("invalid is_signed: {:?}, expected true/false/1/0", p[6]),
                    })
                }
            };
            let scale = if p[7].is_empty() {
                1.0
            } else {
                p[7].parse::<f64>().map_err(|_| ParseError::Csv {
                    line: lineno,
                    message: format!("invalid scale: {:?}", p[7]),
                })?
            };
            let offset = if p[8].is_empty() {
                0.0
            } else {
                p[8].parse::<f64>().map_err(|_| ParseError::Csv {
                    line: lineno,
                    message: format!("invalid offset: {:?}", p[8]),
                })?
            };
            let start_bit = start_byte * 8 + start_bit_in_byte;
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
            if p.len() >= 10 && !p[9].is_empty() {
                let pdu_id = parse_u32(p[9]).map_err(|_| ParseError::Csv {
                    line: lineno,
                    message: invalid_int_message("pdu_id", p[9]),
                })?;
                db.insert_container(def, pdu_id);
            } else {
                db.insert(def);
            }
        }
        Ok(db)
    }

    /// Total number of signal definitions across all frames and container PDUs.
    pub fn len(&self) -> usize {
        let regular: usize = self.frames.values().map(Vec::len).sum();
        let container: usize = self
            .containers
            .values()
            .flat_map(|m| m.values())
            .map(Vec::len)
            .sum();
        regular + container
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All signal names across regular and container frames. Not deduplicated.
    pub fn signal_names(&self) -> impl Iterator<Item = &str> {
        self.frames
            .values()
            .flat_map(|defs| defs.iter().map(|d| d.name.as_str()))
            .chain(
                self.containers
                    .values()
                    .flat_map(|m| m.values())
                    .flat_map(|defs| defs.iter().map(|d| d.name.as_str())),
            )
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

    /// All CAN IDs configured as container frames, sorted ascending.
    ///
    /// Lets callers push down an `is_container` filter (e.g. a Spark
    /// `isin(...)` predicate) before invoking a per-row decode/demux path,
    /// instead of running that path over every CAN ID and discarding
    /// non-container rows afterward.
    pub fn container_can_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.containers.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Signal definitions for a specific container (can_id, pdu_id).
    pub fn container_signals(&self, can_id: u32, pdu_id: u32) -> &[SignalDef] {
        self.containers
            .get(&can_id)
            .and_then(|m| m.get(&pdu_id))
            .map(Vec::as_slice)
            .unwrap_or(&[])
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
    /// If `enums` is provided, the third element of each tuple is the category string for the
    /// raw value, or `None` when no mapping exists.
    pub fn extract<'a>(
        &'a self,
        message_id: u32,
        data: &[u8],
        enums: Option<&EnumValueMap>,
    ) -> Vec<(&'a str, f64, Option<String>)> {
        self.signals(message_id)
            .iter()
            .filter_map(|def| {
                let raw = def.signal.decode_raw(data)?;
                let v = def.signal.decode(data)?;
                let cat = enums
                    .and_then(|m| m.get(def.name.as_str()))
                    .and_then(|inner| inner.get(&raw))
                    .cloned();
                Some((def.name.as_str(), v, cat))
            })
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
        enums: Option<&EnumValueMap>,
    ) -> Vec<(&'a str, f64, Option<String>)> {
        let Some(pdu_map) = self.containers.get(&can_id) else {
            return vec![];
        };
        let mut result = Vec::new();
        for (pdu_id, payload) in demux_container(data, header) {
            if let Some(defs) = pdu_map.get(&pdu_id) {
                for def in defs {
                    if let Some(raw) = def.signal.decode_raw(payload) {
                        if let Some(v) = def.signal.decode(payload) {
                            let cat = enums
                                .and_then(|m| m.get(def.name.as_str()))
                                .and_then(|inner| inner.get(&raw))
                                .cloned();
                            result.push((def.name.as_str(), v, cat));
                        }
                    }
                }
            }
        }
        result
    }

    /// Demultiplex a container frame into raw `(pdu_id, pdu_payload)` pairs without
    /// decoding signals.  Returns an empty vec if `can_id` is not a known container.
    pub fn demux_pdus(
        &self,
        can_id: u32,
        data: &[u8],
        header: ContainerHeader,
    ) -> Vec<(u32, Vec<u8>)> {
        if !self.is_container(can_id) {
            return vec![];
        }
        demux_container(data, header)
            .into_iter()
            .map(|(id, payload)| (id, payload.to_vec()))
            .collect()
    }

    /// Decode signals for a single already-demuxed I-PDU identified by `(can_id, pdu_id)`.
    ///
    /// Bit positions in `data` are relative to the I-PDU payload start.
    pub fn extract_pdu<'a>(
        &'a self,
        can_id: u32,
        pdu_id: u32,
        data: &[u8],
        enums: Option<&EnumValueMap>,
    ) -> Vec<(&'a str, f64, Option<String>)> {
        let Some(pdu_map) = self.containers.get(&can_id) else {
            return vec![];
        };
        let Some(defs) = pdu_map.get(&pdu_id) else {
            return vec![];
        };
        defs.iter()
            .filter_map(|def| {
                let raw = def.signal.decode_raw(data)?;
                let v = def.signal.decode(data)?;
                let cat = enums
                    .and_then(|m| m.get(def.name.as_str()))
                    .and_then(|inner| inner.get(&raw))
                    .cloned();
                Some((def.name.as_str(), v, cat))
            })
            .collect()
    }

    /// Write all signal definitions to `w` in the canonical CSV format.
    ///
    /// Regular-frame rows have an empty `pdu_id` column; container-frame rows
    /// include the PDU ID. Rows are sorted by message ID then signal name.
    pub fn write_csv<W: std::io::Write>(
        &self,
        w: &mut W,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bo = |b: ByteOrder| {
            if b == ByteOrder::Intel {
                "Intel"
            } else {
                "Motorola"
            }
        };
        writeln!(
            w,
            "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id"
        )?;
        let mut msg_ids: Vec<u32> = self.frames.keys().copied().collect();
        msg_ids.sort_unstable();
        for id in msg_ids {
            let mut defs = self.frames[&id].iter().collect::<Vec<_>>();
            defs.sort_by(|a, b| a.name.cmp(&b.name));
            for d in defs {
                writeln!(
                    w,
                    "0x{:X},{},{},{},{},{},{},{},{}",
                    id,
                    d.name,
                    d.signal.start_bit / 8,
                    d.signal.start_bit % 8,
                    d.signal.bit_length,
                    bo(d.signal.byte_order),
                    d.signal.is_signed,
                    d.signal.scale,
                    d.signal.offset,
                )?;
            }
        }
        let mut msg_ids: Vec<u32> = self.containers.keys().copied().collect();
        msg_ids.sort_unstable();
        for id in msg_ids {
            let mut pdu_ids: Vec<u32> = self.containers[&id].keys().copied().collect();
            pdu_ids.sort_unstable();
            for pdu in pdu_ids {
                let mut defs = self.containers[&id][&pdu].iter().collect::<Vec<_>>();
                defs.sort_by(|a, b| a.name.cmp(&b.name));
                for d in defs {
                    writeln!(
                        w,
                        "0x{:X},{},{},{},{},{},{},{},{},0x{:X}",
                        id,
                        d.name,
                        d.signal.start_bit / 8,
                        d.signal.start_bit % 8,
                        d.signal.bit_length,
                        bo(d.signal.byte_order),
                        d.signal.is_signed,
                        d.signal.scale,
                        d.signal.offset,
                        pdu,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Merge `overlay` into `self`; overlay wins on `(message_id, signal_name)` conflicts.
    pub fn merge(&mut self, overlay: CanSignalDb) {
        for (id, defs) in overlay.frames {
            let base = self.frames.entry(id).or_default();
            for d in defs {
                base.retain(|x| x.name != d.name);
                base.push(d);
            }
        }
        for (id, pdu_map) in overlay.containers {
            for (pdu, defs) in pdu_map {
                let base = self
                    .containers
                    .entry(id)
                    .or_default()
                    .entry(pdu)
                    .or_default();
                for d in defs {
                    base.retain(|x| x.name != d.name);
                    base.push(d);
                }
            }
        }
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

/// Build an "invalid {field}" CSV error message, adding a hint when the value
/// looks like a decimal (e.g. "12.0"), a common artifact of spreadsheet exports
/// that formatted an integer column as a number.
fn invalid_int_message(field: &str, value: &str) -> String {
    if value.contains('.') {
        format!(
            "invalid {field}: {value:?} (must be a whole number, not a decimal; \
             if this came from a spreadsheet, format the column as integer or text)"
        )
    } else {
        format!("invalid {field}: {value:?}")
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
/// service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
/// 0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0
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
        for (i, line) in std::io::BufReader::new(reader).lines().enumerate() {
            let lineno = i + 1;
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("service_id") {
                continue;
            }
            let p: Vec<&str> = line.split(',').map(str::trim).collect();
            if p.len() < 10 {
                return Err(ParseError::Csv {
                    line: lineno,
                    message: format!("expected at least 10 columns, got {}", p.len()),
                });
            }
            let service_id = parse_u16(p[0]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("service_id", p[0]),
            })?;
            let method_id = parse_u16(p[1]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("method_id", p[1]),
            })?;
            let name = p[2].to_string();
            let start_byte = parse_u32(p[3]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("start_byte", p[3]),
            })?;
            let start_bit_in_byte = parse_u32(p[4]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("start_bit", p[4]),
            })?;
            let bit_length = parse_u32(p[5]).map_err(|_| ParseError::Csv {
                line: lineno,
                message: invalid_int_message("bit_length", p[5]),
            })?;
            let byte_order = match p[6].to_lowercase().as_str() {
                "intel" => ByteOrder::Intel,
                "motorola" => ByteOrder::Motorola,
                _ => {
                    return Err(ParseError::Csv {
                        line: lineno,
                        message: format!(
                            "invalid byte_order: {:?}, expected Intel or Motorola",
                            p[6]
                        ),
                    })
                }
            };
            let is_signed = match p[7].to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(ParseError::Csv {
                        line: lineno,
                        message: format!("invalid is_signed: {:?}, expected true/false/1/0", p[7]),
                    })
                }
            };
            let scale = if p[8].is_empty() {
                1.0
            } else {
                p[8].parse::<f64>().map_err(|_| ParseError::Csv {
                    line: lineno,
                    message: format!("invalid scale: {:?}", p[8]),
                })?
            };
            let offset = if p[9].is_empty() {
                0.0
            } else {
                p[9].parse::<f64>().map_err(|_| ParseError::Csv {
                    line: lineno,
                    message: format!("invalid offset: {:?}", p[9]),
                })?
            };
            let start_bit = start_byte * 8 + start_bit_in_byte;
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
    pub fn extract<'a>(
        &'a self,
        service_id: u16,
        method_id: u16,
        payload: &[u8],
        enums: Option<&EnumValueMap>,
    ) -> Vec<(&'a str, f64, Option<String>)> {
        self.signals(service_id, method_id)
            .iter()
            .filter_map(|def| {
                let raw = def.signal.decode_raw(payload)?;
                let v = def.signal.decode(payload)?;
                let cat = enums
                    .and_then(|m| m.get(def.name.as_str()))
                    .and_then(|inner| inner.get(&raw))
                    .cloned();
                Some((def.name.as_str(), v, cat))
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// All signal names across every (service_id, method_id) pair. Not deduplicated.
    pub fn signal_names(&self) -> impl Iterator<Item = &str> {
        self.map
            .values()
            .flat_map(|defs| defs.iter().map(|d| d.name.as_str()))
    }

    /// Write all signal definitions to `w` in the canonical CSV format.
    /// Rows are sorted by (service_id, method_id, signal_name).
    pub fn write_csv<W: std::io::Write>(
        &self,
        w: &mut W,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bo = |b: ByteOrder| {
            if b == ByteOrder::Intel {
                "Intel"
            } else {
                "Motorola"
            }
        };
        writeln!(
            w,
            "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset"
        )?;
        let mut keys: Vec<(u16, u16)> = self.map.keys().copied().collect();
        keys.sort_unstable();
        for (svc, mth) in keys {
            let mut defs = self.map[&(svc, mth)].iter().collect::<Vec<_>>();
            defs.sort_by(|a, b| a.name.cmp(&b.name));
            for d in defs {
                writeln!(
                    w,
                    "0x{:04X},0x{:04X},{},{},{},{},{},{},{},{}",
                    svc,
                    mth,
                    d.name,
                    d.signal.start_bit / 8,
                    d.signal.start_bit % 8,
                    d.signal.bit_length,
                    bo(d.signal.byte_order),
                    d.signal.is_signed,
                    d.signal.scale,
                    d.signal.offset,
                )?;
            }
        }
        Ok(())
    }

    /// Merge `overlay` into `self`; overlay wins on `(service_id, method_id, signal_name)` conflicts.
    pub fn merge(&mut self, overlay: SomeIpSignalDb) {
        for ((svc, mth), defs) in overlay.map {
            let base = self.map.entry((svc, mth)).or_default();
            for d in defs {
                base.retain(|x| x.name != d.name);
                base.push(d);
            }
        }
    }
}

/// Validate every data row of a CAN signal CSV and return all errors as `(line, message)` pairs.
///
/// Skips blank lines, comment lines (`#`), and the header row (`message_id,...`).
/// Unlike `CanSignalDb::from_csv`, this function continues past the first error to collect
/// all problems in a single pass.
pub fn check_can_csv<R: std::io::Read>(reader: R) -> Vec<(usize, String)> {
    let mut errors = Vec::new();
    let mut lineno = 0usize;
    // (message_id) -> (is_container, first_lineno) — detect mixed regular/container rows
    let mut first_seen: HashMap<u32, (bool, usize)> = HashMap::new();
    for line in std::io::BufReader::new(reader).lines() {
        lineno += 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push((lineno, format!("I/O error: {e}")));
                break;
            }
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("message_id") {
            continue;
        }
        let p: Vec<&str> = line.split(',').map(str::trim).collect();
        if p.len() < 9 {
            errors.push((
                lineno,
                format!("expected at least 9 columns, got {}", p.len()),
            ));
            continue;
        }
        if parse_u32(p[0]).is_err() {
            errors.push((lineno, invalid_int_message("message_id", p[0])));
        }
        if parse_u32(p[2]).is_err() {
            errors.push((lineno, invalid_int_message("start_byte", p[2])));
        }
        if parse_u32(p[3]).is_err() {
            errors.push((lineno, invalid_int_message("start_bit", p[3])));
        }
        if parse_u32(p[4]).is_err() {
            errors.push((lineno, invalid_int_message("bit_length", p[4])));
        }
        if !matches!(p[5].to_lowercase().as_str(), "intel" | "motorola") {
            errors.push((
                lineno,
                format!("invalid byte_order: {:?}, expected Intel or Motorola", p[5]),
            ));
        }
        if !matches!(p[6].to_lowercase().as_str(), "true" | "false" | "1" | "0") {
            errors.push((
                lineno,
                format!("invalid is_signed: {:?}, expected true/false/1/0", p[6]),
            ));
        }
        if !p[7].is_empty() && p[7].parse::<f64>().is_err() {
            errors.push((lineno, format!("invalid scale: {:?}", p[7])));
        }
        if !p[8].is_empty() && p[8].parse::<f64>().is_err() {
            errors.push((lineno, format!("invalid offset: {:?}", p[8])));
        }
        if p.len() >= 10 && !p[9].is_empty() && parse_u32(p[9]).is_err() {
            errors.push((lineno, invalid_int_message("pdu_id", p[9])));
        }
        if let Ok(mid) = parse_u32(p[0]) {
            let is_container = p.len() >= 10 && !p[9].is_empty();
            match first_seen.get(&mid) {
                Some(&(prev, prev_line)) if prev != is_container => {
                    let (regular_line, container_line) = if prev {
                        (lineno, prev_line)
                    } else {
                        (prev_line, lineno)
                    };
                    errors.push((lineno, format!(
                        "message_id 0x{mid:X} mixes regular-frame (line {regular_line}) and container-frame (line {container_line}) rows; regular signals will be silently ignored"
                    )));
                }
                None => {
                    first_seen.insert(mid, (is_container, lineno));
                }
                _ => {}
            }
        }
    }
    errors
}

/// Validate every data row of a SOME/IP signal CSV and return all errors as `(line, message)` pairs.
///
/// Skips blank lines, comment lines (`#`), and the header row (`service_id,...`).
/// Unlike `SomeIpSignalDb::from_csv`, this function continues past the first error to collect
/// all problems in a single pass.
pub fn check_someip_csv<R: std::io::Read>(reader: R) -> Vec<(usize, String)> {
    let mut errors = Vec::new();
    let mut lineno = 0usize;
    for line in std::io::BufReader::new(reader).lines() {
        lineno += 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push((lineno, format!("I/O error: {e}")));
                break;
            }
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("service_id") {
            continue;
        }
        let p: Vec<&str> = line.split(',').map(str::trim).collect();
        if p.len() < 10 {
            errors.push((
                lineno,
                format!("expected at least 10 columns, got {}", p.len()),
            ));
            continue;
        }
        if parse_u16(p[0]).is_err() {
            errors.push((lineno, invalid_int_message("service_id", p[0])));
        }
        if parse_u16(p[1]).is_err() {
            errors.push((lineno, invalid_int_message("method_id", p[1])));
        }
        if parse_u32(p[3]).is_err() {
            errors.push((lineno, invalid_int_message("start_byte", p[3])));
        }
        if parse_u32(p[4]).is_err() {
            errors.push((lineno, invalid_int_message("start_bit", p[4])));
        }
        if parse_u32(p[5]).is_err() {
            errors.push((lineno, invalid_int_message("bit_length", p[5])));
        }
        if !matches!(p[6].to_lowercase().as_str(), "intel" | "motorola") {
            errors.push((
                lineno,
                format!("invalid byte_order: {:?}, expected Intel or Motorola", p[6]),
            ));
        }
        if !matches!(p[7].to_lowercase().as_str(), "true" | "false" | "1" | "0") {
            errors.push((
                lineno,
                format!("invalid is_signed: {:?}, expected true/false/1/0", p[7]),
            ));
        }
        if !p[8].is_empty() && p[8].parse::<f64>().is_err() {
            errors.push((lineno, format!("invalid scale: {:?}", p[8])));
        }
        if !p[9].is_empty() && p[9].parse::<f64>().is_err() {
            errors.push((lineno, format!("invalid offset: {:?}", p[9])));
        }
    }
    errors
}

/// Maps `signal_name → (raw_value → category_string)`, loaded from an enum CSV.
///
/// CSV format (header required):
/// ```text
/// signal_name,raw_value,category
/// GearPosition,0,Neutral
/// GearPosition,1,First
/// IgnitionStatus,0,Off
/// IgnitionStatus,1,On
/// ```
/// `raw_value` is the raw bit-extracted integer **before** scale/offset is applied.
/// Accepts hex (`0x…`) or decimal.
pub type EnumValueMap = HashMap<String, HashMap<u64, String>>;

/// Load an `EnumValueMap` from a CSV reader.
pub fn enum_value_map_from_csv<R: std::io::Read>(reader: R) -> ParseResult<EnumValueMap> {
    let mut map: EnumValueMap = HashMap::new();
    for (i, line) in std::io::BufReader::new(reader).lines().enumerate() {
        let lineno = i + 1;
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("signal_name") {
            continue;
        }
        let p: Vec<&str> = line.split(',').map(str::trim).collect();
        if p.len() < 3 {
            return Err(ParseError::Csv {
                line: lineno,
                message: format!("expected at least 3 columns, got {}", p.len()),
            });
        }
        let name = p[0].to_string();
        let raw = parse_u32(p[1]).map_err(|_| ParseError::Csv {
            line: lineno,
            message: invalid_int_message("raw_value", p[1]),
        })? as u64;
        let category = p[2].to_string();
        map.entry(name).or_default().insert(raw, category);
    }
    Ok(map)
}

/// Validate every data row of an enum CSV and return all errors as `(line, message)` pairs.
///
/// Skips blank lines, comment lines (`#`), and the header row (`signal_name,...`).
pub fn check_enum_csv<R: std::io::Read>(reader: R) -> Vec<(usize, String)> {
    let mut errors = Vec::new();
    let mut lineno = 0usize;
    for line in std::io::BufReader::new(reader).lines() {
        lineno += 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push((lineno, format!("I/O error: {e}")));
                break;
            }
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("signal_name") {
            continue;
        }
        let p: Vec<&str> = line.split(',').map(str::trim).collect();
        if p.len() < 3 {
            errors.push((
                lineno,
                format!("expected at least 3 columns, got {}", p.len()),
            ));
            continue;
        }
        if parse_u32(p[1]).is_err() {
            errors.push((lineno, invalid_int_message("raw_value", p[1])));
        }
        if p[2].is_empty() {
            errors.push((lineno, "category must not be empty".to_string()));
        }
    }
    errors
}

/// Protocol family used as the key dimension in `ChannelDb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelType {
    Can,
    Ethernet,
}

/// Maps `(ChannelType, channel_number)` to a human-readable channel name.
///
/// CSV format (header required):
/// ```text
/// type,channel,name
/// CAN,1,CAN_HS
/// CAN,2,CAN_LS
/// Ethernet,1,ETH_BACKBONE
/// ```
/// `type` is case-insensitive: `CAN` or `Ethernet`.
/// `channel` is a decimal integer.
/// Duplicate `(type, channel)` pairs are an error.
#[derive(Debug, Default)]
pub struct ChannelDb {
    names: HashMap<(ChannelType, u32), String>,
}

impl ChannelDb {
    pub fn from_csv<R: std::io::Read>(reader: R) -> ParseResult<Self> {
        let mut db = Self::default();
        for (i, line) in std::io::BufReader::new(reader).lines().enumerate() {
            let lineno = i + 1;
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("type") {
                continue;
            }
            let p: Vec<&str> = line.split(',').map(str::trim).collect();
            if p.len() < 3 {
                return Err(ParseError::Csv {
                    line: lineno,
                    message: format!("expected at least 3 columns, got {}", p.len()),
                });
            }
            let ty = match p[0].to_lowercase().as_str() {
                "can" => ChannelType::Can,
                "ethernet" => ChannelType::Ethernet,
                _ => {
                    return Err(ParseError::Csv {
                        line: lineno,
                        message: format!("invalid type: {:?}, expected CAN or Ethernet", p[0]),
                    })
                }
            };
            let channel = p[1].parse::<u32>().map_err(|_| ParseError::Csv {
                line: lineno,
                message: format!("invalid channel: {:?}", p[1]),
            })?;
            let name = p[2].to_string();
            if db.names.contains_key(&(ty, channel)) {
                return Err(ParseError::Csv {
                    line: lineno,
                    message: format!("duplicate entry for ({:?}, {})", ty, channel),
                });
            }
            db.names.insert((ty, channel), name);
        }
        Ok(db)
    }

    pub fn name(&self, ty: ChannelType, channel: u32) -> Option<&str> {
        self.names.get(&(ty, channel)).map(String::as_str)
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
message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x100,EngineSpeed,0,0,16,Intel,false,0.25,0.0
0x100,Throttle,2,0,8,Intel,false,0.4,0.0
0x200,BrakeForce,0,7,12,Motorola,true,0.1,-100.0
";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x100).len(), 2);
        assert_eq!(db.signals(0x200).len(), 1);
        assert_eq!(db.signals(0x300).len(), 0);

        // EngineSpeed: raw=0x0100 → 256 * 0.25 = 64.0
        let data = [0x00u8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let vals = db.extract(0x100, &data, None);
        let speed = vals
            .iter()
            .find(|(n, _, _)| *n == "EngineSpeed")
            .map(|(_, v, _)| *v);
        assert_eq!(speed, Some(64.0));
    }

    #[test]
    fn csv_decimal_id() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   256,Sig,0,0,8,Intel,false,1.0,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(256).len(), 1); // 256 == 0x100
    }

    #[test]
    fn csv_start_bit_decimal_point_hint() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,Sig,0,0.0,8,Intel,false,1.0,0.0\n";
        let err = CanSignalDb::from_csv(csv.as_bytes()).unwrap_err();
        let ParseError::Csv { message, .. } = err else {
            panic!("expected ParseError::Csv");
        };
        assert!(message.contains("start_bit"), "{message}");
        assert!(message.contains("whole number"), "{message}");
    }

    #[test]
    fn csv_skips_comments_and_blanks() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   # this is a comment\n\
                   \n\
                   0x10,Voltage,0,0,8,Intel,false,0.1,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x10).len(), 1);
    }

    #[test]
    fn someip_csv_round_trip() {
        let csv = "\
service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0
0x0064,0x0001,Pressure,2,0,8,Intel,false,1.0,0.0
0x0064,0x0002,Speed,0,0,16,Motorola,false,0.5,0.0
";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x0064, 0x0001).len(), 2);
        assert_eq!(db.signals(0x0064, 0x0002).len(), 1);
        assert_eq!(db.signals(0x0064, 0x0003).len(), 0);

        // Temperature: raw=0x0190 (400) → 400 * 0.01 = 4.0
        // Pressure: raw=0x32 (50) → 50 * 1.0 = 50.0
        let payload = [0x90u8, 0x01, 0x32];
        let vals = db.extract(0x0064, 0x0001, &payload, None);
        let temp = vals
            .iter()
            .find(|(n, _, _)| *n == "Temperature")
            .map(|(_, v, _)| *v);
        let pres = vals
            .iter()
            .find(|(n, _, _)| *n == "Pressure")
            .map(|(_, v, _)| *v);
        assert_eq!(temp, Some(4.0));
        assert_eq!(pres, Some(50.0));
    }

    #[test]
    fn someip_csv_decimal_ids() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   100,1,Sig,0,0,8,Intel,false,1.0,0.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(100, 1).len(), 1); // 100 == 0x64, 1 == 0x01
    }

    #[test]
    fn csv_trailing_comma_regular() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,Speed,0,0,16,Intel,false,0.25,0.0,\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x100).len(), 1);
    }

    #[test]
    fn csv_trailing_comma_container() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x200,Sig,0,0,8,Intel,false,1.0,0.0,0x10,\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert!(db.is_container(0x200));
    }

    #[test]
    fn csv_default_scale_offset() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,Speed,0,0,8,Intel,false,,\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        // scale=1.0, offset=0.0: raw=42 → 42.0
        let vals = db.extract(0x100, &[42], None);
        assert_eq!(vals.first().map(|(_, v, _)| *v), Some(42.0));
    }

    #[test]
    fn someip_csv_trailing_comma() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0,\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x0064, 0x0001).len(), 1);
    }

    #[test]
    fn someip_csv_default_scale_offset() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x0010,0x0001,Sig,0,0,8,Intel,false,,\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        // scale=1.0, offset=0.0: raw=7 → 7.0
        let vals = db.extract(0x0010, 0x0001, &[7], None);
        assert_eq!(vals.first().map(|(_, v, _)| *v), Some(7.0));
    }

    #[test]
    fn check_csv_mixed_regular_and_container() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x100,Direct,0,0,8,Intel,false,1.0,0.0\n\
                   0x100,Contained,0,0,8,Intel,false,1.0,0.0,0x01\n";
        let errs = check_can_csv(csv.as_bytes());
        assert_eq!(errs.len(), 1);
        assert!(errs[0].1.contains("0x100"));
        assert!(errs[0].1.contains("regular"));
        assert!(errs[0].1.contains("container"));
    }

    #[test]
    fn check_csv_pure_regular_no_error() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,Sig1,0,0,8,Intel,false,1.0,0.0\n\
                   0x100,Sig2,1,0,8,Intel,false,1.0,0.0\n";
        assert!(check_can_csv(csv.as_bytes()).is_empty());
    }

    #[test]
    fn check_csv_pure_container_no_error() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x200,Sig1,0,0,8,Intel,false,1.0,0.0,0x01\n\
                   0x200,Sig2,0,0,8,Intel,false,1.0,0.0,0x02\n";
        assert!(check_can_csv(csv.as_bytes()).is_empty());
    }

    #[test]
    fn someip_csv_skips_header_and_blanks() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   # comment\n\
                   \n\
                   0x0001,0x0002,Sig,0,0,8,Intel,false,1.0,0.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.signals(0x0001, 0x0002).len(), 1);
    }

    #[test]
    fn someip_csv_signed_scale_offset() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x0010,0x0003,Temp,0,0,8,Intel,true,0.5,-40.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        // raw=0xFF = -1 signed → -1 * 0.5 - 40.0 = -40.5
        let vals = db.extract(0x0010, 0x0003, &[0xFF], None);
        assert_eq!(vals.first().map(|(_, v, _)| *v), Some(-40.5));
    }

    // ── container frame tests ─────────────────────────────────────────────────

    #[test]
    fn demux_short_header() {
        // Two I-PDUs: PDU 0x000010 (2 bytes), PDU 0x000020 (1 byte)
        // Each header: 3-byte PDU ID (big-endian) + 1-byte DLC
        let data = [
            0x00u8, 0x00, 0x10, 0x02, 0xAB, 0xCD, // PDU 0x000010, dlc=2
            0x00, 0x00, 0x20, 0x01, 0xFF, // PDU 0x000020, dlc=1
        ];
        let pdus = demux_container(&data, ContainerHeader::Short);
        assert_eq!(pdus.len(), 2);
        assert_eq!(pdus[0], (0x000010, [0xAB, 0xCD].as_slice()));
        assert_eq!(pdus[1], (0x000020, [0xFF].as_slice()));
    }

    #[test]
    fn demux_short_header_dlc_extended() {
        // DLC=9 maps to 12 payload bytes (ISO 11898-1 Table 3)
        let mut data = vec![0x00u8, 0x00, 0x01, 0x09]; // PDU 0x000001, dlc=9
        data.extend_from_slice(&[
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC,
        ]);
        let pdus = demux_container(&data, ContainerHeader::Short);
        assert_eq!(pdus.len(), 1);
        assert_eq!(pdus[0].0, 0x000001);
        assert_eq!(pdus[0].1.len(), 12);
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
        // Header says dlc=5 but only 2 bytes available after the 4-byte header
        let data = [0x00u8, 0x00, 0x01, 0x05, 0xAA, 0xBB];
        let pdus = demux_container(&data, ContainerHeader::Short);
        assert!(pdus.is_empty());
    }

    #[test]
    fn container_csv_round_trip() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x200,Sig1,0,0,8,Intel,false,1.0,0.0,0x10\n\
                   0x200,Sig2,1,0,8,Intel,false,2.0,0.0,0x10\n\
                   0x200,Sig3,0,0,8,Intel,false,0.5,0.0,0x20\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert!(db.is_container(0x200));
        assert!(!db.is_container(0x100));

        // Build a container frame: PDU 0x10 → [0x05, 0x0A], PDU 0x20 → [0x08]
        let frame = [
            0x00u8, 0x00, 0x10, 0x02, 0x05, 0x0A, // PDU 0x10: Sig1=5, Sig2=10
            0x00, 0x00, 0x20, 0x01, 0x08, // PDU 0x20: Sig3=8*0.5=4.0
        ];
        let vals = db.extract_container(0x200, &frame, ContainerHeader::Short, None);
        let get = |name: &str| vals.iter().find(|(n, _, _)| *n == name).map(|(_, v, _)| *v);
        assert_eq!(get("Sig1"), Some(5.0));
        assert_eq!(get("Sig2"), Some(20.0));
        assert_eq!(get("Sig3"), Some(4.0));
    }

    #[test]
    fn container_pdu_order_independent() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x600,Radar_Distance_m,0,0,16,Intel,false,0.01,0.0,0x01\n\
                   0x600,CabinTemp_degC,0,0,8,Intel,true,0.5,-40.0,0x02\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();

        // PDU 0x01 first, then PDU 0x02
        let frame_ab = [
            0x00u8, 0x00, 0x01, 0x02, 0x40, 0x1F, // PDU 0x01: raw=0x1F40=8000 → 80.0 m
            0x00, 0x00, 0x02, 0x01, 0x64, // PDU 0x02: raw=0x64=100 → 100*0.5-40=10.0 °C
        ];
        // PDU 0x02 first, then PDU 0x01
        let frame_ba = [
            0x00u8, 0x00, 0x02, 0x01, 0x64, // PDU 0x02
            0x00, 0x00, 0x01, 0x02, 0x40, 0x1F, // PDU 0x01
        ];

        let vals_ab = db.extract_container(0x600, &frame_ab, ContainerHeader::Short, None);
        let vals_ba = db.extract_container(0x600, &frame_ba, ContainerHeader::Short, None);

        let get = |vals: &[(&str, f64, Option<String>)], name: &str| {
            vals.iter().find(|(n, _, _)| *n == name).map(|(_, v, _)| *v)
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
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x100,Direct,0,0,8,Intel,false,1.0,0.0\n\
                   0x200,Contained,0,0,8,Intel,false,1.0,0.0,0x01\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert!(!db.is_container(0x100));
        assert!(db.is_container(0x200));
        assert_eq!(db.extract(0x100, &[0x42], None).len(), 1);
        let frame = [0x00u8, 0x00, 0x01, 0x01, 0x07];
        assert_eq!(
            db.extract_container(0x200, &frame, ContainerHeader::Short, None)
                .len(),
            1
        );
    }

    #[test]
    fn container_can_ids_lists_only_container_frames_sorted() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x100,Direct,0,0,8,Intel,false,1.0,0.0\n\
                   0x300,Contained,0,0,8,Intel,false,1.0,0.0,0x01\n\
                   0x200,Contained,0,0,8,Intel,false,1.0,0.0,0x02\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        assert_eq!(db.container_can_ids(), vec![0x200, 0x300]);
    }

    #[test]
    fn enum_csv_round_trip() {
        let csv = "\
signal_name,raw_value,category
GearPosition,0,Neutral
GearPosition,1,First
GearPosition,2,Second
IgnitionStatus,0,Off
IgnitionStatus,0x01,On
";
        let map = enum_value_map_from_csv(csv.as_bytes()).unwrap();
        assert_eq!(map["GearPosition"][&0], "Neutral");
        assert_eq!(map["GearPosition"][&1], "First");
        assert_eq!(map["GearPosition"][&2], "Second");
        assert_eq!(map["IgnitionStatus"][&0], "Off");
        assert_eq!(map["IgnitionStatus"][&1], "On");
    }

    #[test]
    fn extract_with_enum() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,GearPosition,0,0,8,Intel,false,1.0,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();

        let enum_csv =
            "signal_name,raw_value,category\nGearPosition,0,Neutral\nGearPosition,1,First\n";
        let enums = enum_value_map_from_csv(enum_csv.as_bytes()).unwrap();

        let vals = db.extract(0x100, &[1], Some(&enums));
        let (_, v, cat) = vals.first().unwrap();
        assert_eq!(*v, 1.0);
        assert_eq!(cat.as_deref(), Some("First"));

        let vals_no_match = db.extract(0x100, &[5], Some(&enums));
        assert_eq!(vals_no_match.first().unwrap().2, None);
    }

    #[test]
    fn extract_without_enum() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x100,GearPosition,0,0,8,Intel,false,1.0,0.0\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        let vals = db.extract(0x100, &[1], None);
        assert_eq!(vals.first().unwrap().2, None);
    }

    #[test]
    fn can_signal_names_covers_regular_and_container() {
        let csv = "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id\n\
                   0x100,Direct,0,0,8,Intel,false,1.0,0.0\n\
                   0x200,Contained,0,0,8,Intel,false,1.0,0.0,0x01\n";
        let db = CanSignalDb::from_csv(csv.as_bytes()).unwrap();
        let mut names: Vec<&str> = db.signal_names().collect();
        names.sort_unstable();
        assert_eq!(names, vec!["Contained", "Direct"]);
    }

    #[test]
    fn someip_signal_names() {
        let csv = "service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset\n\
                   0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0\n\
                   0x0064,0x0001,Pressure,2,0,8,Intel,false,1.0,0.0\n";
        let db = SomeIpSignalDb::from_csv(csv.as_bytes()).unwrap();
        let mut names: Vec<&str> = db.signal_names().collect();
        names.sort_unstable();
        assert_eq!(names, vec!["Pressure", "Temperature"]);
    }

    #[test]
    fn check_enum_csv_errors() {
        let csv = "signal_name,raw_value,category\nGearPosition,notanumber,First\nOther,1,\n";
        let errs = check_enum_csv(csv.as_bytes());
        assert_eq!(errs.len(), 2);
        assert!(errs[0].1.contains("raw_value"));
        assert!(errs[1].1.contains("category"));
    }
}
