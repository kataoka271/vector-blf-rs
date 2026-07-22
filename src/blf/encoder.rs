use super::error::ParseResult;
use chrono::{DateTime, Datelike, LocalResult, TimeZone, Timelike, Utc};
use std::io::{Read, Write};

pub trait Encoder<W: Write> {
    fn encode(&self, w: W) -> ParseResult<()>;
}

pub trait Decoder<R: Read> {
    type Item;
    fn decode(r: R) -> ParseResult<Self::Item>;
}

impl<W: Write> Encoder<W> for u8 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        Ok(w.write_all(&[*self])?)
    }
}

impl<W: Write> Encoder<W> for u16 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        Ok(w.write_all(&[(self & 0xFF) as u8, ((self >> 8) & 0xFF) as u8])?)
    }
}

impl<W: Write> Encoder<W> for u32 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        Ok(w.write_all(&[
            (self & 0xFF) as u8,
            ((self >> 8) & 0xFF) as u8,
            ((self >> 16) & 0xFF) as u8,
            ((self >> 24) & 0xFF) as u8,
        ])?)
    }
}

impl<W: Write> Encoder<W> for u64 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        Ok(w.write_all(&[
            (self & 0xFF) as u8,
            ((self >> 8) & 0xFF) as u8,
            ((self >> 16) & 0xFF) as u8,
            ((self >> 24) & 0xFF) as u8,
            ((self >> 32) & 0xFF) as u8,
            ((self >> 40) & 0xFF) as u8,
            ((self >> 48) & 0xFF) as u8,
            ((self >> 56) & 0xFF) as u8,
        ])?)
    }
}

impl<R: Read> Decoder<R> for u8 {
    type Item = u8;
    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        Ok(b[0])
    }
}

impl<R: Read> Decoder<R> for u16 {
    type Item = u16;
    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut b = [0u8; 2];
        r.read_exact(&mut b)?;
        Ok(b[0] as u16 | (b[1] as u16) << 8)
    }
}

impl<R: Read> Decoder<R> for u32 {
    type Item = u32;
    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut b = [0u8; 4];
        r.read_exact(&mut b)?;
        Ok(b[0] as u32 | (b[1] as u32) << 8 | (b[2] as u32) << 16 | (b[3] as u32) << 24)
    }
}

impl<R: Read> Decoder<R> for u64 {
    type Item = u64;
    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut b = [0u8; 8];
        r.read_exact(&mut b)?;
        Ok(b[0] as u64
            | (b[1] as u64) << 8
            | (b[2] as u64) << 16
            | (b[3] as u64) << 24
            | (b[4] as u64) << 32
            | (b[5] as u64) << 40
            | (b[6] as u64) << 48
            | (b[7] as u64) << 56)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Timestamp {
    Nanosecond(u64),
    Microsecond(u64),
}

impl Timestamp {
    fn to_datetime(self) -> DateTime<Utc> {
        match self {
            Timestamp::Nanosecond(ns) => Utc.timestamp_nanos(ns as i64),
            Timestamp::Microsecond(us) => Utc.timestamp_micros(us as i64).unwrap(),
        }
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_datetime().format("%Y-%m-%d %H:%M:%S"))
    }
}

impl<W: Write> Encoder<W> for Timestamp {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let tm = self.to_datetime();
        (tm.year() as u16).encode(&mut w)?;
        (tm.month() as u16).encode(&mut w)?;
        (tm.weekday().num_days_from_monday() as u16).encode(&mut w)?;
        (tm.day() as u16).encode(&mut w)?;
        (tm.hour() as u16).encode(&mut w)?;
        (tm.minute() as u16).encode(&mut w)?;
        (tm.second() as u16).encode(&mut w)?;
        ((tm.nanosecond() / 1_000_000) as u16).encode(&mut w)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for Timestamp {
    type Item = Timestamp;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut tm = [0u16; 8];
        for v in &mut tm {
            *v = u16::decode(&mut r)?;
        }
        let year = tm[0] as i32;
        let month = tm[1] as u32;
        let _weekday = tm[2];
        let day = tm[3] as u32;
        let hour = tm[4] as u32;
        let min = tm[5] as u32;
        let sec = tm[6] as u32;
        let msec = tm[7] as u32;
        match Utc.with_ymd_and_hms(year, month, day, hour, min, sec) {
            LocalResult::None => Ok(Timestamp::Nanosecond(0)),
            LocalResult::Single(dt) => {
                let ns = dt.timestamp() * 1_000_000_000 + msec as i64 * 1_000_000;
                Ok(Timestamp::Nanosecond(ns as u64))
            }
            LocalResult::Ambiguous(_, _) => Ok(Timestamp::Nanosecond(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_systemtime_roundtrip_preserves_milliseconds() {
        let ns = 1_700_000_000_000_000_000u64 + 123_000_000; // +123 ms
        let mut buf = Vec::new();
        Timestamp::Nanosecond(ns).encode(&mut buf).unwrap();
        let decoded = Timestamp::decode(&buf[..]).unwrap();
        match decoded {
            Timestamp::Nanosecond(decoded_ns) => assert_eq!(decoded_ns, ns),
            other => panic!("expected Nanosecond, got {other:?}"),
        }
    }
}
