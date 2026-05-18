use super::error::{ParseError, ParseResult};
use super::ip::IpProtocol;
use std::io::Read;

#[derive(Debug)]
pub struct TcpFlags {
    pub ns: bool,
    pub cwr: bool,
    pub ece: bool,
    pub urg: bool,
    pub ack: bool,
    pub psh: bool,
    pub rst: bool,
    pub syn: bool,
    pub fin: bool,
}

#[derive(Debug)]
pub struct Tcp {
    pub src_port: u16,
    pub dst_port: u16,
    pub seq: u32,
    pub ack: u32,
    pub flags: TcpFlags,
    pub window_size: u16,
    pub urgent_ptr: u16,
    pub data: Vec<u8>,
}

impl Tcp {
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let src_port = read_u16_be(&mut r)?;
        let dst_port = read_u16_be(&mut r)?;
        let seq = read_u32_be(&mut r)?;
        let ack = read_u32_be(&mut r)?;
        let data_offset_flags = read_u16_be(&mut r)?;
        let data_offset = ((data_offset_flags >> 12) as usize) * 4;
        if data_offset < 20 {
            return Err(ParseError::InvalidData);
        }
        let f = data_offset_flags & 0x1FF;
        let flags = TcpFlags {
            ns:  f & 0x100 != 0,
            cwr: f & 0x080 != 0,
            ece: f & 0x040 != 0,
            urg: f & 0x020 != 0,
            ack: f & 0x010 != 0,
            psh: f & 0x008 != 0,
            rst: f & 0x004 != 0,
            syn: f & 0x002 != 0,
            fin: f & 0x001 != 0,
        };
        let window_size = read_u16_be(&mut r)?;
        read_u16_be(&mut r)?; // checksum
        let urgent_ptr = read_u16_be(&mut r)?;
        if data_offset > 20 {
            let mut options = vec![0u8; data_offset - 20];
            r.read_exact(&mut options)?;
        }
        let mut data = Vec::new();
        r.read_to_end(&mut data)?;
        Ok(Tcp { src_port, dst_port, seq, ack, flags, window_size, urgent_ptr, data })
    }
}

#[derive(Debug)]
pub struct Udp {
    pub src_port: u16,
    pub dst_port: u16,
    pub data: Vec<u8>,
}

impl Udp {
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let src_port = read_u16_be(&mut r)?;
        let dst_port = read_u16_be(&mut r)?;
        let length = read_u16_be(&mut r)?;
        read_u16_be(&mut r)?; // checksum
        let data_len = (length as usize).saturating_sub(8);
        let mut data = vec![0u8; data_len];
        r.read_exact(&mut data)?;
        Ok(Udp { src_port, dst_port, data })
    }
}

#[derive(Debug)]
pub enum Transport {
    Tcp(Tcp),
    Udp(Udp),
}

impl Transport {
    pub fn parse(protocol: IpProtocol, data: &[u8]) -> ParseResult<Self> {
        match protocol {
            IpProtocol::Tcp => Ok(Transport::Tcp(Tcp::parse(data)?)),
            IpProtocol::Udp => Ok(Transport::Udp(Udp::parse(data)?)),
            _ => Err(ParseError::InvalidData),
        }
    }
}

fn read_u16_be<R: Read>(r: &mut R) -> ParseResult<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(((b[0] as u16) << 8) | b[1] as u16)
}

fn read_u32_be<R: Read>(r: &mut R) -> ParseResult<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(((b[0] as u32) << 24) | ((b[1] as u32) << 16) | ((b[2] as u32) << 8) | b[3] as u32)
}
