use super::super::error::{ParseError, ParseResult};
use super::super::read_util::{read_u16_be, read_u8};
use super::transport::Transport;
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpProtocol {
    Icmp,
    Igmp,
    Tcp,
    Udp,
    IcmpV6,
    Other(u8),
}

impl IpProtocol {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Icmp,
            2 => Self::Igmp,
            6 => Self::Tcp,
            17 => Self::Udp,
            58 => Self::IcmpV6,
            v => Self::Other(v),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::Icmp => 1,
            Self::Igmp => 2,
            Self::Tcp => 6,
            Self::Udp => 17,
            Self::IcmpV6 => 58,
            Self::Other(v) => v,
        }
    }
}

#[derive(Debug)]
pub struct Ipv4 {
    pub dscp: u8,
    pub ecn: u8,
    pub total_length: u16,
    pub id: u16,
    pub dont_fragment: bool,
    pub more_fragments: bool,
    pub fragment_offset: u16,
    pub ttl: u8,
    pub protocol: IpProtocol,
    pub src_addr: [u8; 4],
    pub dst_addr: [u8; 4],
    pub data: Vec<u8>,
}

impl Ipv4 {
    pub fn parse_transport(&self) -> ParseResult<Transport> {
        Transport::parse(self.protocol, &self.data)
    }

    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let version_ihl = read_u8(&mut r)?;
        if version_ihl >> 4 != 4 {
            return Err(ParseError::InvalidData);
        }
        let ihl = ((version_ihl & 0xF) as usize) * 4;
        if ihl < 20 {
            return Err(ParseError::InvalidData);
        }
        let dscp_ecn = read_u8(&mut r)?;
        let total_length = read_u16_be(&mut r)?;
        let id = read_u16_be(&mut r)?;
        let flags_frag = read_u16_be(&mut r)?;
        let dont_fragment = flags_frag & 0x4000 != 0;
        let more_fragments = flags_frag & 0x2000 != 0;
        let fragment_offset = flags_frag & 0x1FFF;
        let ttl = read_u8(&mut r)?;
        let protocol = IpProtocol::from_u8(read_u8(&mut r)?);
        read_u16_be(&mut r)?; // checksum
        let mut src_addr = [0u8; 4];
        r.read_exact(&mut src_addr)?;
        let mut dst_addr = [0u8; 4];
        r.read_exact(&mut dst_addr)?;
        if ihl > 20 {
            let mut options = vec![0u8; ihl - 20];
            r.read_exact(&mut options)?;
        }
        let data_len = (total_length as usize).saturating_sub(ihl);
        let mut data = vec![0u8; data_len];
        r.read_exact(&mut data)?;
        Ok(Ipv4 {
            dscp: dscp_ecn >> 2,
            ecn: dscp_ecn & 0x3,
            total_length,
            id,
            dont_fragment,
            more_fragments,
            fragment_offset,
            ttl,
            protocol,
            src_addr,
            dst_addr,
            data,
        })
    }
}

#[derive(Debug)]
pub struct Ipv6 {
    pub traffic_class: u8,
    pub flow_label: u32,
    pub next_header: IpProtocol,
    pub hop_limit: u8,
    pub src_addr: [u8; 16],
    pub dst_addr: [u8; 16],
    pub data: Vec<u8>,
}

impl Ipv6 {
    pub fn parse_transport(&self) -> ParseResult<Transport> {
        Transport::parse(self.next_header, &self.data)
    }

    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let mut word = [0u8; 4];
        r.read_exact(&mut word)?;
        if word[0] >> 4 != 6 {
            return Err(ParseError::InvalidData);
        }
        let traffic_class = ((word[0] & 0xF) << 4) | (word[1] >> 4);
        let flow_label = ((word[1] as u32 & 0xF) << 16) | ((word[2] as u32) << 8) | word[3] as u32;
        let payload_length = read_u16_be(&mut r)?;
        let next_header = IpProtocol::from_u8(read_u8(&mut r)?);
        let hop_limit = read_u8(&mut r)?;
        let mut src_addr = [0u8; 16];
        r.read_exact(&mut src_addr)?;
        let mut dst_addr = [0u8; 16];
        r.read_exact(&mut dst_addr)?;
        let mut data = vec![0u8; payload_length as usize];
        r.read_exact(&mut data)?;
        Ok(Ipv6 {
            traffic_class,
            flow_label,
            next_header,
            hop_limit,
            src_addr,
            dst_addr,
            data,
        })
    }
}

#[derive(Debug)]
pub enum Ip {
    V4(Ipv4),
    V6(Ipv6),
}

impl Ip {
    pub fn parse(ether_type: u16, data: &[u8]) -> ParseResult<Self> {
        match ether_type {
            0x0800 => Ok(Ip::V4(Ipv4::parse(data)?)),
            0x86DD => Ok(Ip::V6(Ipv6::parse(data)?)),
            _ => Err(ParseError::InvalidData),
        }
    }
}
