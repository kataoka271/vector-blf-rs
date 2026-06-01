use super::error::{ParseError, ParseResult};
use super::read_util::{read_u16_be, read_u8};
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArpOp {
    Request,
    Reply,
    Other(u16),
}

impl ArpOp {
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::Request,
            2 => Self::Reply,
            v => Self::Other(v),
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            Self::Request => 1,
            Self::Reply => 2,
            Self::Other(v) => v,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Request => "Request",
            Self::Reply => "Reply",
            Self::Other(_) => "Other",
        }
    }
}

#[derive(Debug)]
pub struct Arp {
    pub operation: ArpOp,
    pub sender_mac: [u8; 6],
    pub sender_ip: [u8; 4],
    pub target_mac: [u8; 6],
    pub target_ip: [u8; 4],
}

impl Arp {
    /// Parse an ARP packet for IPv4-over-Ethernet (htype=1, ptype=0x0800, hlen=6, plen=4).
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        if read_u16_be(&mut r)? != 1 {
            return Err(ParseError::InvalidData); // htype must be Ethernet
        }
        if read_u16_be(&mut r)? != 0x0800 {
            return Err(ParseError::InvalidData); // ptype must be IPv4
        }
        if read_u8(&mut r)? != 6 {
            return Err(ParseError::InvalidData); // hlen must be 6
        }
        if read_u8(&mut r)? != 4 {
            return Err(ParseError::InvalidData); // plen must be 4
        }
        let operation = ArpOp::from_u16(read_u16_be(&mut r)?);
        let mut sender_mac = [0u8; 6];
        r.read_exact(&mut sender_mac)?;
        let mut sender_ip = [0u8; 4];
        r.read_exact(&mut sender_ip)?;
        let mut target_mac = [0u8; 6];
        r.read_exact(&mut target_mac)?;
        let mut target_ip = [0u8; 4];
        r.read_exact(&mut target_ip)?;
        Ok(Arp {
            operation,
            sender_mac,
            sender_ip,
            target_mac,
            target_ip,
        })
    }
}
