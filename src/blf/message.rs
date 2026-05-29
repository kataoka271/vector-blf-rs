use super::diag::isotp::IsoTpFrame;
use super::encoder::{Decoder, Encoder};
use super::error::ParseResult;
use super::ip::Ip;
use super::objtype::ObjType;
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Tx,
    Rx,
    TxRq,
    Unknown(u8),
}

impl Dir {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Dir::Tx,
            1 => Dir::Rx,
            2 => Dir::TxRq,
            v => Dir::Unknown(v),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Dir::Tx => 0,
            Dir::Rx => 1,
            Dir::TxRq => 2,
            Dir::Unknown(v) => v,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Vlan {
    pub tpid: u16,
    pub pri: u16,
    pub cfi: u16,
    pub vid: u16,
}

#[derive(Debug)]
pub struct Can {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: Dir,
    pub rtr: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

impl Can {
    pub fn parse_isotp(&self) -> ParseResult<IsoTpFrame> {
        IsoTpFrame::parse(&self.data)
    }
}

#[derive(Debug)]
pub struct CanFd {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: Dir,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

impl CanFd {
    pub fn parse_isotp(&self) -> ParseResult<IsoTpFrame> {
        IsoTpFrame::parse(&self.data)
    }
}

#[derive(Debug)]
pub struct CanFd64 {
    pub channel: u8,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: Dir,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

impl CanFd64 {
    pub fn parse_isotp(&self) -> ParseResult<IsoTpFrame> {
        IsoTpFrame::parse(&self.data)
    }
}

#[derive(Debug)]
pub struct Ethernet {
    pub channel: u16,
    pub dir: Dir,
    pub src_addr: [u8; 6],
    pub dst_addr: [u8; 6],
    pub vlan: Option<Vlan>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

impl Ethernet {
    pub fn parse_ip(&self) -> ParseResult<Ip> {
        Ip::parse(self.ether_type, &self.data)
    }
}

#[derive(Debug)]
pub struct EthernetEx {
    pub channel: u16,
    pub dir: Dir,
    pub src_addr: [u8; 6],
    pub dst_addr: [u8; 6],
    pub vlan: Option<Vlan>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

impl EthernetEx {
    pub fn parse_ip(&self) -> ParseResult<Ip> {
        Ip::parse(self.ether_type, &self.data)
    }
}

#[derive(Debug)]
pub struct Mf4Signal {
    pub group: String,
    pub name: String,
    pub value: f64,
    pub unit: String,
}

#[derive(Debug)]
pub enum Message {
    Can(Can),
    CanFd(CanFd),
    CanFd64(CanFd64),
    Ethernet(Ethernet),
    EthernetEx(EthernetEx),
    Mf4Signal(Mf4Signal),
    Other(ObjType, Vec<u8>),
}

impl Message {
    pub fn decode<R: Read>(mut r: R, obj_type: ObjType, data_size: u32) -> ParseResult<Self> {
        match obj_type {
            ObjType::CanMessage | ObjType::CanMessage2 => Ok(Message::Can(Can::decode(&mut r)?)),
            ObjType::CanFdMessage => Ok(Message::CanFd(CanFd::decode(&mut r)?)),
            ObjType::CanFdMessage64 => Ok(Message::CanFd64(CanFd64::decode(&mut r)?)),
            ObjType::EthernetFrame => Ok(Message::Ethernet(Ethernet::decode(&mut r)?)),
            ObjType::EthernetFrameEx => Ok(Message::EthernetEx(EthernetEx::decode(&mut r)?)),
            _ => {
                let mut obj_data = vec![0u8; data_size as usize];
                r.read_exact(&mut obj_data)?;
                Ok(Message::Other(obj_type, obj_data))
            }
        }
    }

    pub fn encode<W: Write>(&self, w: W) -> ParseResult<()> {
        match self {
            Message::Can(m) => m.encode(w),
            Message::CanFd(m) => m.encode(w),
            Message::CanFd64(m) => m.encode(w),
            Message::Ethernet(m) => m.encode(w),
            Message::EthernetEx(m) => m.encode(w),
            // Mf4Signal has no BLF binary representation; callers must filter before
            // passing to a BLF Writer (see blf::Writer::write_base_object).
            Message::Mf4Signal(_) => Ok(()),
            Message::Other(_, obj_data) => {
                let mut w = w;
                Ok(w.write_all(obj_data)?)
            }
        }
    }

    pub fn obj_type(&self) -> ObjType {
        match self {
            Message::Can(_) => ObjType::CanMessage,
            Message::CanFd(_) => ObjType::CanFdMessage,
            Message::CanFd64(_) => ObjType::CanFdMessage64,
            Message::Ethernet(_) => ObjType::EthernetFrame,
            Message::EthernetEx(_) => ObjType::EthernetFrameEx,
            Message::Mf4Signal(_) => ObjType::Other(0),
            Message::Other(obj_type, _) => *obj_type,
        }
    }
}
