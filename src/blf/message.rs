use super::encoder::{Decoder, Encoder};
use super::error::ParseResult;
use super::ip::Ip;
use super::isotp::IsoTpFrame;
use super::objtype::ObjType;
use super::raw;
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

impl From<raw::Vlan> for Vlan {
    fn from(v: raw::Vlan) -> Self {
        Vlan {
            tpid: v.tpid,
            pri: v.pri,
            cfi: v.cfi,
            vid: v.vid,
        }
    }
}

impl From<Vlan> for raw::Vlan {
    fn from(v: Vlan) -> Self {
        raw::Vlan {
            tpid: v.tpid,
            pri: v.pri,
            cfi: v.cfi,
            vid: v.vid,
        }
    }
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
pub enum Message {
    Can(Can),
    CanFd(CanFd),
    CanFd64(CanFd64),
    Ethernet(Ethernet),
    EthernetEx(EthernetEx),
    Other(ObjType, Vec<u8>),
}

impl Message {
    pub fn decode<R: Read>(mut r: R, obj_type: ObjType, data_size: u32) -> ParseResult<Self> {
        match obj_type {
            ObjType::CanMessage | ObjType::CanMessage2 => {
                let m = raw::Can::decode(&mut r)?;
                Ok(Message::Can(Can {
                    channel: m.channel,
                    id: m.id,
                    is_ext_id: m.is_ext_id,
                    dir: Dir::from_u8(m.dir),
                    rtr: m.rtr,
                    dlc: m.dlc,
                    data: m.data.to_vec(),
                }))
            }
            ObjType::CanFdMessage => {
                let m = raw::CanFd::decode(&mut r)?;
                Ok(Message::CanFd(CanFd {
                    channel: m.channel,
                    id: m.id,
                    is_ext_id: m.is_ext_id,
                    dir: Dir::from_u8(m.dir),
                    rtr: m.rtr,
                    fdf: m.fdf,
                    brs: m.brs,
                    esi: m.esi,
                    dlc: m.dlc,
                    data: m.data,
                }))
            }
            ObjType::CanFdMessage64 => {
                let m = raw::CanFd64::decode(&mut r)?;
                Ok(Message::CanFd64(CanFd64 {
                    channel: m.channel,
                    id: m.id,
                    is_ext_id: m.is_ext_id,
                    dir: Dir::from_u8(m.dir),
                    rtr: m.rtr,
                    fdf: m.fdf,
                    brs: m.brs,
                    esi: m.esi,
                    dlc: m.dlc,
                    data: m.data,
                }))
            }
            ObjType::EthernetFrame => {
                let m = raw::Ethernet::decode(&mut r)?;
                Ok(Message::Ethernet(Ethernet {
                    channel: m.channel,
                    dir: Dir::from_u8(m.dir as u8),
                    src_addr: m.src_addr,
                    dst_addr: m.dst_addr,
                    vlan: m.vlan.map(Into::into),
                    ether_type: m.ether_type,
                    data: m.data,
                }))
            }
            ObjType::EthernetFrameEx => {
                let m = raw::EthernetEx::decode(&mut r)?;
                Ok(Message::EthernetEx(EthernetEx {
                    channel: m.channel,
                    dir: Dir::from_u8(m.dir as u8),
                    src_addr: m.src_addr,
                    dst_addr: m.dst_addr,
                    vlan: m.vlan.map(Into::into),
                    ether_type: m.ether_type,
                    data: m.data,
                }))
            }
            _ => {
                let m = raw::Other::decode(&mut r, obj_type, data_size)?;
                Ok(Message::Other(m.obj_type, m.obj_data))
            }
        }
    }

    pub fn encode<W: Write>(&self, w: W) -> ParseResult<()> {
        match self {
            Message::Can(m) => {
                let mut data = [0u8; 8];
                let len = m.data.len().min(8);
                data[..len].copy_from_slice(&m.data[..len]);
                raw::Can {
                    channel: m.channel,
                    id: m.id,
                    is_ext_id: m.is_ext_id,
                    dir: m.dir.to_u8(),
                    rtr: m.rtr,
                    dlc: m.dlc,
                    data,
                }
                .encode(w)
            }
            Message::CanFd(m) => raw::CanFd {
                channel: m.channel,
                id: m.id,
                is_ext_id: m.is_ext_id,
                dir: m.dir.to_u8(),
                rtr: m.rtr,
                fdf: m.fdf,
                brs: m.brs,
                esi: m.esi,
                dlc: m.dlc,
                data: m.data.clone(),
            }
            .encode(w),
            Message::CanFd64(m) => raw::CanFd64 {
                channel: m.channel,
                id: m.id,
                is_ext_id: m.is_ext_id,
                dir: m.dir.to_u8(),
                rtr: m.rtr,
                fdf: m.fdf,
                brs: m.brs,
                esi: m.esi,
                dlc: m.dlc,
                data: m.data.clone(),
            }
            .encode(w),
            Message::Ethernet(m) => raw::Ethernet {
                channel: m.channel,
                dir: m.dir.to_u8() as u16,
                src_addr: m.src_addr,
                dst_addr: m.dst_addr,
                vlan: m.vlan.clone().map(Into::into),
                ether_type: m.ether_type,
                data: m.data.clone(),
            }
            .encode(w),
            Message::EthernetEx(m) => raw::EthernetEx {
                channel: m.channel,
                dir: m.dir.to_u8() as u16,
                src_addr: m.src_addr,
                dst_addr: m.dst_addr,
                vlan: m.vlan.clone().map(Into::into),
                ether_type: m.ether_type,
                data: m.data.clone(),
            }
            .encode(w),
            Message::Other(obj_type, obj_data) => raw::Other {
                obj_type: *obj_type,
                obj_data: obj_data.clone(),
            }
            .encode(w),
        }
    }

    pub fn obj_type(&self) -> ObjType {
        match self {
            Message::Can(_) => ObjType::CanMessage,
            Message::CanFd(_) => ObjType::CanFdMessage,
            Message::CanFd64(_) => ObjType::CanFdMessage64,
            Message::Ethernet(_) => ObjType::EthernetFrame,
            Message::EthernetEx(_) => ObjType::EthernetFrameEx,
            Message::Other(obj_type, _) => *obj_type,
        }
    }
}
