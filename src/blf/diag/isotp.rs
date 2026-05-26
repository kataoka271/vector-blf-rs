use super::super::error::{ParseError, ParseResult};
use super::uds::Uds;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStatus {
    ContinueToSend,
    Wait,
    Overflow,
    Unknown(u8),
}

impl FlowStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::ContinueToSend,
            1 => Self::Wait,
            2 => Self::Overflow,
            v => Self::Unknown(v),
        }
    }
}

/// ISO 15765-2 transport layer frame.
///
/// CAN-FD extended formats are handled automatically:
/// - Extended SF: first byte = 0x00, second byte = length (used when length > 7)
/// - Extended FF: first two bytes = 0x10 0x00, next four bytes = 32-bit length
#[derive(Debug)]
pub enum IsoTpFrame {
    SingleFrame {
        data: Vec<u8>,
    },
    FirstFrame {
        total_length: u32,
        data: Vec<u8>,
    },
    ConsecutiveFrame {
        sequence_number: u8,
        data: Vec<u8>,
    },
    FlowControl {
        flow_status: FlowStatus,
        block_size: u8,
        min_separation_time: u8,
    },
}

impl IsoTpFrame {
    /// Parse a UDS message from a SingleFrame. Returns InvalidData for all
    /// other frame types — multi-frame payloads require a Reassembler.
    pub fn parse_uds(&self) -> ParseResult<Uds> {
        match self {
            IsoTpFrame::SingleFrame { data } => Uds::parse(data),
            _ => Err(ParseError::InvalidData),
        }
    }

    pub fn parse(data: &[u8]) -> ParseResult<Self> {
        if data.is_empty() {
            return Err(ParseError::InvalidData);
        }
        match (data[0] & 0xF0) >> 4 {
            0x0 => Self::parse_sf(data),
            0x1 => Self::parse_ff(data),
            0x2 => Self::parse_cf(data),
            0x3 => Self::parse_fc(data),
            _ => Err(ParseError::InvalidData),
        }
    }

    fn parse_sf(data: &[u8]) -> ParseResult<Self> {
        let len = data[0] & 0x0F;
        // Extended SF for CAN-FD: length nibble == 0, actual length in next byte
        let (payload_offset, payload_len) = if len == 0 {
            if data.len() < 2 {
                return Err(ParseError::InvalidData);
            }
            (2, data[1] as usize)
        } else {
            (1, len as usize)
        };
        if data.len() < payload_offset + payload_len {
            return Err(ParseError::InvalidData);
        }
        Ok(IsoTpFrame::SingleFrame {
            data: data[payload_offset..payload_offset + payload_len].to_vec(),
        })
    }

    fn parse_ff(data: &[u8]) -> ParseResult<Self> {
        if data.len() < 2 {
            return Err(ParseError::InvalidData);
        }
        let len12 = (((data[0] & 0x0F) as u32) << 8) | data[1] as u32;
        // Extended FF for CAN-FD: 12-bit length == 0, actual length in next 4 bytes
        let (payload_offset, total_length) = if len12 == 0 {
            if data.len() < 6 {
                return Err(ParseError::InvalidData);
            }
            let tl = ((data[2] as u32) << 24)
                | ((data[3] as u32) << 16)
                | ((data[4] as u32) << 8)
                | data[5] as u32;
            (6, tl)
        } else {
            (2, len12)
        };
        Ok(IsoTpFrame::FirstFrame {
            total_length,
            data: data[payload_offset..].to_vec(),
        })
    }

    fn parse_cf(data: &[u8]) -> ParseResult<Self> {
        Ok(IsoTpFrame::ConsecutiveFrame {
            sequence_number: data[0] & 0x0F,
            data: data[1..].to_vec(),
        })
    }

    fn parse_fc(data: &[u8]) -> ParseResult<Self> {
        if data.len() < 3 {
            return Err(ParseError::InvalidData);
        }
        Ok(IsoTpFrame::FlowControl {
            flow_status: FlowStatus::from_u8(data[0] & 0x0F),
            block_size: data[1],
            min_separation_time: data[2],
        })
    }
}

/// Stateful ISO-TP reassembler for a single sender/receiver pair.
///
/// Feed frames in order with `push()`; when it returns `Some(uds)` the
/// multi-frame message is complete and the reassembler resets automatically.
#[derive(Debug, Default)]
pub struct Reassembler {
    total_length: u32,
    next_sn: u8,
    buf: Vec<u8>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the next ISO-TP frame. Returns a complete UDS payload when the
    /// last ConsecutiveFrame arrives, or `None` if more frames are needed.
    /// Returns `Err` if a frame is out of sequence or the data is malformed.
    pub fn push(&mut self, frame: &IsoTpFrame) -> ParseResult<Option<Uds>> {
        match frame {
            IsoTpFrame::SingleFrame { data } => {
                self.reset();
                Ok(Some(Uds::parse(data)?))
            }
            IsoTpFrame::FirstFrame { total_length, data } => {
                self.reset();
                self.total_length = *total_length;
                self.next_sn = 1;
                self.buf = Vec::with_capacity(*total_length as usize);
                self.buf.extend_from_slice(data);
                Ok(None)
            }
            IsoTpFrame::ConsecutiveFrame {
                sequence_number,
                data,
            } => {
                if self.total_length == 0 {
                    return Err(ParseError::InvalidData);
                }
                if *sequence_number != self.next_sn {
                    self.reset();
                    return Err(ParseError::InvalidData);
                }
                self.next_sn = (self.next_sn + 1) & 0x0F;
                let remaining = self.total_length as usize - self.buf.len();
                self.buf
                    .extend_from_slice(&data[..remaining.min(data.len())]);
                if self.buf.len() >= self.total_length as usize {
                    let payload = std::mem::take(&mut self.buf);
                    self.reset();
                    Ok(Some(Uds::parse(&payload)?))
                } else {
                    Ok(None)
                }
            }
            IsoTpFrame::FlowControl { .. } => Ok(None),
        }
    }

    fn reset(&mut self) {
        self.total_length = 0;
        self.next_sn = 0;
        self.buf.clear();
    }
}
