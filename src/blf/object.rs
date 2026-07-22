use super::encoder::{Decoder, Encoder, Timestamp};
use super::error::{ParseError, ParseResult};
use super::objtype::ObjType;
use std::io::{Read, Write};

const LOGG: &[u8] = b"LOGG";
const LOBJ: &[u8] = b"LOBJ";
const FILE_HEADER_SIZE: u32 = 144;
const VALID_FILE_HEADER_SIZE: u32 = 72;
const TIME_TEN_MICS: u32 = 1;

#[derive(Debug)]
pub struct FileHeader {
    pub start_timestamp: Timestamp,
    pub stop_timestamp: Timestamp,
    pub file_size: u64,
    pub uncompressed_size: u64,
    pub object_count: u32,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            start_timestamp: Timestamp::Nanosecond(0),
            stop_timestamp: Timestamp::Nanosecond(0),
            file_size: FILE_HEADER_SIZE as u64,
            uncompressed_size: FILE_HEADER_SIZE as u64,
            object_count: 0,
        }
    }
}

impl<W: Write> Encoder<W> for FileHeader {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        w.write_all(LOGG)?;
        FILE_HEADER_SIZE.encode(&mut w)?;
        0u8.encode(&mut w)?; // app ID
        0u8.encode(&mut w)?; // app major
        0u8.encode(&mut w)?; // app minor
        0u8.encode(&mut w)?; // app build
        0u8.encode(&mut w)?; // log major
        0u8.encode(&mut w)?; // log minor
        0u8.encode(&mut w)?; // log build
        0u8.encode(&mut w)?; // log patch
        self.file_size.encode(&mut w)?;
        self.uncompressed_size.encode(&mut w)?;
        self.object_count.encode(&mut w)?;
        0u32.encode(&mut w)?; // count of objects read
        self.start_timestamp.encode(&mut w)?;
        self.stop_timestamp.encode(&mut w)?;
        w.write_all(&[0; (FILE_HEADER_SIZE - VALID_FILE_HEADER_SIZE) as usize])?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for FileHeader {
    type Item = FileHeader;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut tag = [0u8; 4];
        r.read_exact(&mut tag)?;
        if tag != LOGG {
            return Err(ParseError::Logg);
        }
        let header_size = u32::decode(&mut r)?;
        u8::decode(&mut r)?; // app ID
        u8::decode(&mut r)?; // app major
        u8::decode(&mut r)?; // app minor
        u8::decode(&mut r)?; // app build
        u8::decode(&mut r)?; // log major
        u8::decode(&mut r)?; // log minor
        u8::decode(&mut r)?; // log build
        u8::decode(&mut r)?; // log patch
        let file_size = u64::decode(&mut r)?;
        let uncompressed_size = u64::decode(&mut r)?;
        let object_count = u32::decode(&mut r)?;
        u32::decode(&mut r)?; // count of objects read
        let start_timestamp = Timestamp::decode(&mut r)?;
        let stop_timestamp = Timestamp::decode(&mut r)?;
        r.read_exact(&mut vec![
            0;
            (header_size - VALID_FILE_HEADER_SIZE) as usize
        ])?;
        Ok(FileHeader {
            start_timestamp,
            stop_timestamp,
            file_size,
            uncompressed_size,
            object_count,
        })
    }
}

#[derive(Debug)]
pub struct BaseObjectHeader {
    pub header_size: u16,
    pub version: u16,
    pub obj_size: u32,
    pub obj_type: ObjType,
}

impl<W: Write> Encoder<W> for BaseObjectHeader {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        w.write_all(LOBJ)?;
        self.header_size.encode(&mut w)?;
        self.version.encode(&mut w)?;
        self.obj_size.encode(&mut w)?;
        self.obj_type.to_u32().encode(&mut w)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for BaseObjectHeader {
    type Item = BaseObjectHeader;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut b = [0u8; 4];
        if r.read_exact(&mut b).is_err() {
            return Err(ParseError::Eof);
        }
        if b != LOBJ {
            return Err(ParseError::Lobj);
        }
        let header_size = u16::decode(&mut r)?;
        let version = u16::decode(&mut r)?;
        let obj_size = u32::decode(&mut r)?;
        let obj_type = ObjType::from_u32(u32::decode(&mut r)?);
        Ok(BaseObjectHeader {
            header_size,
            version,
            obj_size,
            obj_type,
        })
    }
}

#[derive(Debug)]
pub struct LogContainerHeader {
    pub compression_method: u16,
    pub uncompressed_size: u32,
}

impl<W: Write> Encoder<W> for LogContainerHeader {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        self.compression_method.encode(&mut w)?;
        w.write_all(&[0u8; 6])?;
        self.uncompressed_size.encode(&mut w)?;
        w.write_all(&[0u8; 4])?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for LogContainerHeader {
    type Item = LogContainerHeader;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let compression_method = u16::decode(&mut r)?;
        r.read_exact(&mut [0u8; 6])?;
        let uncompressed_size = u32::decode(&mut r)?;
        r.read_exact(&mut [0u8; 4])?;
        Ok(LogContainerHeader {
            compression_method,
            uncompressed_size,
        })
    }
}

#[derive(Debug)]
pub struct ObjectHeaderV1 {
    pub timestamp: Timestamp,
}

impl<W: Write> Encoder<W> for ObjectHeaderV1 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        match self.timestamp {
            Timestamp::Nanosecond(ns) => {
                0u32.encode(&mut w)?; // flags
                0u16.encode(&mut w)?; // client index
                0u16.encode(&mut w)?; // object version
                ns.encode(&mut w)?;
            }
            Timestamp::Microsecond(us) => {
                TIME_TEN_MICS.encode(&mut w)?; // flags
                0u16.encode(&mut w)?;
                0u16.encode(&mut w)?;
                (us / 10).encode(&mut w)?;
            }
        }
        Ok(())
    }
}

impl<R: Read> Decoder<R> for ObjectHeaderV1 {
    type Item = ObjectHeaderV1;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let flags = u32::decode(&mut r)?;
        u16::decode(&mut r)?; // client index
        u16::decode(&mut r)?; // object version
        let timestamp = u64::decode(&mut r)?;
        let timestamp = if flags == TIME_TEN_MICS {
            Timestamp::Microsecond(timestamp * 10)
        } else {
            Timestamp::Nanosecond(timestamp)
        };
        Ok(ObjectHeaderV1 { timestamp })
    }
}

#[derive(Debug)]
pub struct ObjectHeaderV2 {
    pub timestamp: Timestamp,
}

impl<W: Write> Encoder<W> for ObjectHeaderV2 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        match self.timestamp {
            Timestamp::Nanosecond(ns) => {
                0u32.encode(&mut w)?; // flags
                0u8.encode(&mut w)?;
                0u8.encode(&mut w)?;
                0u16.encode(&mut w)?; // object version
                ns.encode(&mut w)?;
                0u64.encode(&mut w)?; // original timestamp
            }
            Timestamp::Microsecond(us) => {
                TIME_TEN_MICS.encode(&mut w)?;
                0u8.encode(&mut w)?;
                0u8.encode(&mut w)?;
                0u16.encode(&mut w)?;
                (us / 10).encode(&mut w)?;
                0u64.encode(&mut w)?;
            }
        }
        Ok(())
    }
}

impl<R: Read> Decoder<R> for ObjectHeaderV2 {
    type Item = ObjectHeaderV2;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let flags = u32::decode(&mut r)?;
        u8::decode(&mut r)?; // timestamp status
        u8::decode(&mut r)?;
        u16::decode(&mut r)?; // object version
        let timestamp = u64::decode(&mut r)?;
        u64::decode(&mut r)?; // original timestamp
        let timestamp = if flags == TIME_TEN_MICS {
            Timestamp::Microsecond(timestamp * 10)
        } else {
            Timestamp::Nanosecond(timestamp)
        };
        Ok(ObjectHeaderV2 { timestamp })
    }
}
