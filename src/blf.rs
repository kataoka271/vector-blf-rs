mod encoder;
mod error;
pub mod doip;
pub mod ip;
pub mod isotp;
pub mod message;
mod object;
mod objtype;
pub mod raw;
pub mod someip;
pub mod transport;
pub mod uds;

use encoder::{Decoder, Encoder};
use error::ParseResult;
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use object::{BaseObjectHeader, LogContainerHeader, ObjectHeaderV1, ObjectHeaderV2};
use std::io::{Read, Seek, Write};

pub use doip::{DiagMessage, DoIp, PayloadType};
pub use encoder::Timestamp;
pub use error::ParseError;
pub use ip::{Ip, IpProtocol, Ipv4, Ipv6};
pub use isotp::{FlowStatus, IsoTpFrame};
pub use someip::{MessageType, ReturnCode, SomeIp};
pub use uds::{Nrc, ServiceId, Uds};
pub use transport::{Tcp, TcpFlags, Transport, Udp};
pub use message::{Can, CanFd, CanFd64, Dir, Ethernet, EthernetEx, Message, Vlan};
pub use object::FileHeader;
pub use objtype::ObjType;

#[derive(Debug)]
pub struct BaseObject {
    pub timestamp: Timestamp,
    pub message: Message,
}

const OBJECT_HEADER_SIZE: u32 = 32;
const BASE_OBJECT_HEADER_SIZE: u32 = 16;
const ZLIB_DEFLATE: u16 = 2;
const BUF_SIZE: usize = 4096;

pub struct Reader<R: Read + Seek> {
    pub header: FileHeader,
    r: R,
    buf: Vec<u8>,
    tmp: Vec<u8>,
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(mut r: R) -> ParseResult<Self> {
        let header = FileHeader::decode(&mut r)?;
        let mut reader = Self {
            header,
            r,
            buf: Vec::new(),
            tmp: Vec::new(),
        };
        reader.read_log_container()?;
        Ok(reader)
    }

    fn read_log_container(&mut self) -> ParseResult<()> {
        let base_header = BaseObjectHeader::decode(&mut self.r)?;
        if base_header.obj_type != ObjType::LogContainer {
            return Err(ParseError::UnexpectedObjType(base_header.obj_type));
        }
        let lc_header = LogContainerHeader::decode(&mut self.r)?;
        let data_size = (base_header.obj_size - OBJECT_HEADER_SIZE) as usize;
        if self.tmp.len() < data_size {
            self.tmp.resize(data_size, 0);
        }
        self.r.read_exact(&mut self.tmp[..data_size])?;
        let padding_size = data_size % 4;
        if padding_size > 0 {
            let mut padding = [0u8; 4];
            self.r.read_exact(&mut padding[..padding_size])?;
        }
        if lc_header.compression_method == ZLIB_DEFLATE {
            let mut z = ZlibDecoder::new(&self.tmp[..data_size]);
            let mut chunk = [0u8; BUF_SIZE];
            let mut uncompressed = 0usize;
            loop {
                let n = z.read(&mut chunk).map_err(|_| ParseError::ZlibError)?;
                if n == 0 {
                    break;
                }
                uncompressed += n;
                self.buf.write_all(&chunk[..n])?;
            }
            if lc_header.uncompressed_size as usize != uncompressed {
                return Err(ParseError::UncompressedSizeMismatch);
            }
        } else {
            self.buf.write_all(&self.tmp[..data_size])?;
        }
        Ok(())
    }

    pub fn read_base_object(&mut self) -> ParseResult<BaseObject> {
        let mut r = &self.buf[..];
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
        if !r.is_empty() {
            log::info!("log container data remaining: {} bytes", r.len());
        }
        self.buf.drain(0..self.buf.len() - r.len());
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
        match self.read_log_container() {
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
        let obj_header = ObjectHeaderV2 {
            timestamp: obj.timestamp,
        };
        base_header.encode(&mut self.buf)?;
        obj_header.encode(&mut self.buf)?;
        self.buf.write_all(&self.tmp)?;
        let padding_size = base_header.obj_size as usize % 4;
        if padding_size != 0 && base_header.obj_type.is_padding_needed() {
            self.buf.write_all(&[0u8; 4][..padding_size])?;
        }
        self.header.object_count += 1;
        if self.buf.len() >= BUF_SIZE {
            self.header.uncompressed_size += BUF_SIZE as u64;
            let data: Vec<u8> = self.buf.drain(..BUF_SIZE).collect();
            self.write_log_container(&data)?;
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
        let remaining: Vec<u8> = self.buf.drain(..).collect();
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
