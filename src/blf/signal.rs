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
        pos = if pos % 8 == 0 { pos + 15 } else { pos - 1 };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(start_bit: u32, bit_length: u32, byte_order: ByteOrder, is_signed: bool) -> Signal {
        Signal { start_bit, bit_length, byte_order, is_signed, scale: 1.0, offset: 0.0 }
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
            start_bit: 0, bit_length: 8, byte_order: ByteOrder::Intel,
            is_signed: false, scale: 0.5, offset: -40.0,
        };
        // raw=100 → 100*0.5 - 40 = 10.0
        assert_eq!(s.decode(&[100]), Some(10.0));
    }
}
