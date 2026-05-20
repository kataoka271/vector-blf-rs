mod encoder;
mod error;
pub mod diag;
pub mod ip;
pub mod message;
mod object;
mod objtype;
pub mod signal;
pub mod transport;

use encoder::{Decoder, Encoder};
use error::ParseResult;
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use object::{BaseObjectHeader, LogContainerHeader, ObjectHeaderV1, ObjectHeaderV2};
use std::io::{Read, Seek, Write};

pub use diag::doip::{DiagMessage, DoIp, PayloadType};
pub use diag::isotp::{FlowStatus, IsoTpFrame, Reassembler};
pub use diag::someip::{MessageType, ReturnCode, SomeIp};
pub use diag::uds::{Nrc, ServiceId, Uds};
pub use encoder::Timestamp;
pub use error::ParseError;
pub use ip::{Ip, IpProtocol, Ipv4, Ipv6};
pub use message::{Can, CanFd, CanFd64, Dir, Ethernet, EthernetEx, Message, Vlan};
pub use object::FileHeader;
pub use objtype::ObjType;
pub use signal::{ByteOrder, Signal, SignalDb, SignalDef};
pub use transport::{Tcp, TcpFlags, Transport, Udp};

#[derive(Debug)]
pub struct BaseObject {
    pub timestamp: Timestamp,
    pub message: Message,
}

const OBJECT_HEADER_SIZE: u32 = 32;
const BASE_OBJECT_HEADER_SIZE: u32 = 16;
const ZLIB_DEFLATE: u16 = 2;
const BUF_SIZE: usize = 4096;

// Reads one LogContainer from `r` at its current position, appending
// the decompressed payload into `out`. `tmp` is a reusable scratch buffer.
fn decompress_container<R: Read>(
    r: &mut R,
    out: &mut Vec<u8>,
    tmp: &mut Vec<u8>,
) -> ParseResult<()> {
    let base_header = BaseObjectHeader::decode(&mut *r)?;
    if base_header.obj_type != ObjType::LogContainer {
        return Err(ParseError::UnexpectedObjType(base_header.obj_type));
    }
    let lc_header = LogContainerHeader::decode(&mut *r)?;
    let data_size = (base_header.obj_size - OBJECT_HEADER_SIZE) as usize;
    if tmp.len() < data_size {
        tmp.resize(data_size, 0);
    }
    r.read_exact(&mut tmp[..data_size])?;
    let padding_size = data_size % 4;
    if padding_size > 0 {
        let mut padding = [0u8; 4];
        r.read_exact(&mut padding[..padding_size])?;
    }
    if lc_header.compression_method == ZLIB_DEFLATE {
        let expected = lc_header.uncompressed_size as usize;
        out.reserve(expected);
        let before = out.len();
        let mut z = ZlibDecoder::new(&tmp[..data_size]);
        z.read_to_end(out).map_err(|_| ParseError::ZlibError)?;
        if out.len() - before != expected {
            return Err(ParseError::UncompressedSizeMismatch);
        }
    } else {
        out.write_all(&tmp[..data_size])?;
    }
    Ok(())
}

// Parses as many complete BaseObjects as possible from `buf`, appending
// them to `out`. Advances a cursor rather than draining on every object to
// avoid the O(n²) byte-shifting cost; drains once at the end.
fn parse_objects_from_buf(
    buf: &mut Vec<u8>,
    tmp: &mut Vec<u8>,
    out: &mut Vec<BaseObject>,
) -> ParseResult<()> {
    let mut cursor = 0usize;
    loop {
        let mut r = &buf[cursor..];
        let before = r.len();
        let base_header = match BaseObjectHeader::decode(&mut r) {
            Err(_) => break,
            Ok(h) => h,
        };
        let data_size = (base_header.obj_size - BASE_OBJECT_HEADER_SIZE) as usize;
        if tmp.len() < data_size {
            tmp.resize(data_size, 0);
        }
        if r.read_exact(&mut tmp[..data_size]).is_err() {
            break;
        }
        let padding_size = base_header.obj_size as usize % 4;
        if padding_size > 0 && base_header.obj_type.is_padding_needed() {
            let mut padding = [0u8; 4];
            if r.read_exact(&mut padding[..padding_size]).is_err() {
                break;
            }
        }
        cursor += before - r.len();
        if !r.is_empty() {
            log::info!("log container data remaining: {} bytes", r.len());
        }
        let mut payload = &tmp[..data_size];
        let timestamp = if base_header.version == 1 {
            ObjectHeaderV1::decode(&mut payload)?.timestamp
        } else {
            ObjectHeaderV2::decode(&mut payload)?.timestamp
        };
        let msg_size = base_header.obj_size - OBJECT_HEADER_SIZE;
        let message = Message::decode(&mut payload, base_header.obj_type, msg_size)?;
        if !payload.is_empty() {
            log::warn!("base object data remaining: {} bytes", payload.len());
        }
        out.push(BaseObject { timestamp, message });
    }
    buf.drain(0..cursor);
    Ok(())
}

/// Returns the byte offset of every LogContainer in the file.
/// Reads only the 16-byte `BaseObjectHeader` of each container (no decompression).
pub fn scan_containers<R: Read + Seek>(r: &mut R) -> ParseResult<Vec<u64>> {
    FileHeader::decode(&mut *r)?;
    let mut offsets = Vec::new();
    loop {
        let pos = r.stream_position()?;
        let base_header = match BaseObjectHeader::decode(&mut *r) {
            Err(ParseError::Eof) => break,
            Err(ParseError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
            Ok(h) => h,
        };
        if base_header.obj_type != ObjType::LogContainer {
            return Err(ParseError::UnexpectedObjType(base_header.obj_type));
        }
        offsets.push(pos);
        // Skip LogContainerHeader (16) + data + padding, then continue to next container.
        let data_size = (base_header.obj_size - OBJECT_HEADER_SIZE) as i64;
        let padding = data_size % 4;
        r.seek(std::io::SeekFrom::Current(16 + data_size + padding))?;
    }
    Ok(offsets)
}

/// Seeks to each offset in `offsets`, decompresses the LogContainer there,
/// and parses every BaseObject from it. Returns all objects in file order.
pub fn parse_at<R: Read + Seek>(r: &mut R, offsets: &[u64]) -> ParseResult<Vec<BaseObject>> {
    let mut buf = Vec::new();
    let mut tmp = Vec::new();
    let mut out = Vec::new();
    for &offset in offsets {
        r.seek(std::io::SeekFrom::Start(offset))?;
        buf.clear();
        decompress_container(&mut *r, &mut buf, &mut tmp)?;
        parse_objects_from_buf(&mut buf, &mut tmp, &mut out)?;
    }
    Ok(out)
}

pub struct Reader<R: Read + Seek> {
    pub header: FileHeader,
    r: R,
    buf: Vec<u8>,
    tmp: Vec<u8>,
    cursor: usize,
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(mut r: R) -> ParseResult<Self> {
        let header = FileHeader::decode(&mut r)?;
        let mut reader = Self {
            header,
            r,
            buf: Vec::new(),
            tmp: Vec::new(),
            cursor: 0,
        };
        reader.load_next_container()?;
        Ok(reader)
    }

    fn load_next_container(&mut self) -> ParseResult<()> {
        // Discard all cleanly-consumed bytes in one shot, then append the next container.
        self.buf.drain(0..self.cursor);
        self.cursor = 0;
        decompress_container(&mut self.r, &mut self.buf, &mut self.tmp)
    }

    pub fn read_base_object(&mut self) -> ParseResult<BaseObject> {
        let mut r = &self.buf[self.cursor..];
        let before = r.len();
        let base_header = match BaseObjectHeader::decode(&mut r) {
            Err(_) => return self.retry_read_base_object(),
            Ok(h) => h,
        };
        let data_size = (base_header.obj_size - BASE_OBJECT_HEADER_SIZE) as usize;
        if self.tmp.len() < data_size {
            self.tmp.resize(data_size, 0);
        }
        if r.read_exact(&mut self.tmp[..data_size]).is_err() {
            return self.retry_read_base_object();
        }
        let padding_size = base_header.obj_size as usize % 4;
        if padding_size > 0 && base_header.obj_type.is_padding_needed() {
            let mut padding = [0u8; 4];
            if r.read_exact(&mut padding[..padding_size]).is_err() {
                return self.retry_read_base_object();
            }
        }
        self.cursor += before - r.len();
        if !r.is_empty() {
            log::info!("log container data remaining: {} bytes", r.len());
        }
        let mut payload = &self.tmp[..data_size];
        let timestamp = if base_header.version == 1 {
            ObjectHeaderV1::decode(&mut payload)?.timestamp
        } else {
            ObjectHeaderV2::decode(&mut payload)?.timestamp
        };
        let msg_size = base_header.obj_size - OBJECT_HEADER_SIZE;
        let message = Message::decode(&mut payload, base_header.obj_type, msg_size)?;
        if !payload.is_empty() {
            log::warn!("base object data remaining: {} bytes", payload.len());
        }
        Ok(BaseObject { timestamp, message })
    }

    fn retry_read_base_object(&mut self) -> ParseResult<BaseObject> {
        match self.load_next_container() {
            Err(ParseError::Eof) => Err(ParseError::Eof),
            Err(ParseError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(ParseError::Eof)
            }
            Err(e) => Err(e),
            Ok(_) => self.read_base_object(),
        }
    }
}

impl<R: Read + Seek> Iterator for Reader<R> {
    type Item = ParseResult<BaseObject>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_base_object() {
            Err(ParseError::Eof) => None,
            result => Some(result),
        }
    }
}

pub struct Writer<W: Write + Seek> {
    pub header: FileHeader,
    w: W,
    buf: Vec<u8>,
    tmp: Vec<u8>,
}

impl<W: Write + Seek> Writer<W> {
    pub fn new(mut w: W) -> ParseResult<Self> {
        let header = FileHeader::default();
        header.encode(&mut w)?;
        Ok(Self {
            header,
            w,
            buf: Vec::new(),
            tmp: Vec::new(),
        })
    }

    pub fn write_base_object(&mut self, obj: &BaseObject) -> ParseResult<()> {
        self.tmp.clear();
        obj.message.encode(&mut self.tmp)?;
        let base_header = BaseObjectHeader {
            header_size: BASE_OBJECT_HEADER_SIZE as u16,
            version: 1,
            obj_size: self.tmp.len() as u32 + OBJECT_HEADER_SIZE,
            obj_type: obj.message.obj_type(),
        };
        let obj_header = ObjectHeaderV1 {
            timestamp: obj.timestamp,
        };
        // Record the position before this object so we can flush up to here,
        // ensuring every container boundary falls on an object boundary.
        let obj_start = self.buf.len();
        base_header.encode(&mut self.buf)?;
        obj_header.encode(&mut self.buf)?;
        self.buf.write_all(&self.tmp)?;
        let padding_size = base_header.obj_size as usize % 4;
        if padding_size != 0 && base_header.obj_type.is_padding_needed() {
            self.buf.write_all(&[0u8; 4][..padding_size])?;
        }
        self.header.object_count += 1;
        if self.buf.len() >= BUF_SIZE {
            // Flush everything before the current object (obj_start bytes).
            // If obj_start == 0 the object alone exceeds BUF_SIZE; flush it whole.
            let flush_end = if obj_start > 0 { obj_start } else { self.buf.len() };
            self.header.uncompressed_size += flush_end as u64;
            let mut flush_data = std::mem::take(&mut self.buf);
            self.buf = flush_data.split_off(flush_end);
            self.write_log_container(&flush_data)?;
        }
        Ok(())
    }

    fn write_log_container(&mut self, data: &[u8]) -> ParseResult<()> {
        self.tmp.clear();
        let mut z = ZlibEncoder::new(&mut self.tmp, Compression::default());
        z.write_all(data).map_err(|_| ParseError::ZlibError)?;
        let compressed = z.finish()?;
        let base_header = BaseObjectHeader {
            header_size: BASE_OBJECT_HEADER_SIZE as u16,
            version: 1,
            obj_size: compressed.len() as u32 + OBJECT_HEADER_SIZE,
            obj_type: ObjType::LogContainer,
        };
        let lc_header = LogContainerHeader {
            compression_method: ZLIB_DEFLATE,
            uncompressed_size: data.len() as u32,
        };
        base_header.encode(&mut self.w)?;
        lc_header.encode(&mut self.w)?;
        self.w.write_all(compressed)?;
        let padding_size = base_header.obj_size as usize % 4;
        if padding_size > 0 {
            self.w.write_all(&[0u8; 4][..padding_size])?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ParseResult<()> {
        self.header.uncompressed_size += self.buf.len() as u64;
        let remaining = std::mem::take(&mut self.buf);
        self.write_log_container(&remaining)?;
        let pos = self.w.stream_position()?;
        self.header.file_size = pos;
        self.w.seek(std::io::SeekFrom::Start(0))?;
        self.header.encode(&mut self.w)?;
        self.w.seek(std::io::SeekFrom::Start(pos))?;
        Ok(())
    }

    pub fn stream_position(&mut self) -> ParseResult<u64> {
        Ok(self.w.stream_position()?)
    }
}
