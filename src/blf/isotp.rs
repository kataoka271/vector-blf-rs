use super::error::{ParseError, ParseResult};

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
