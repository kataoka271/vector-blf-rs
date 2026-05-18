use super::encoder::{Decoder, Encoder};
use super::error::ParseResult;
use super::objtype::ObjType;
use std::io::{Read, Write};

#[derive(Debug, PartialEq, Eq)]
pub struct Can {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub dlc: u8,
    pub data: [u8; 8],
}

impl<W: Write> Encoder<W> for Can {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let can_id = (self.id & 0x1FFF_FFFF) | if self.is_ext_id { 0x8000_0000 } else { 0 };
        let flags = (self.dir & 0x3) | if self.rtr { 0x80 } else { 0 };
        self.channel.encode(&mut w)?;
        flags.encode(&mut w)?;
        self.dlc.encode(&mut w)?;
        can_id.encode(&mut w)?;
        w.write_all(&self.data)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for Can {
    type Item = Can;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let channel = u16::decode(&mut r)?;
        let flags = u8::decode(&mut r)?;
        let dlc = u8::decode(&mut r)?;
        let can_id = u32::decode(&mut r)?;
        let is_ext_id = can_id & 0x8000_0000;
        let can_id = can_id & 0x1FFF_FFFF;
        let mut data = [0u8; 8];
        r.read_exact(&mut data)?;
        Ok(Can {
            channel,
            id: can_id,
            is_ext_id: is_ext_id > 0,
            dir: flags & 0x3,
            rtr: flags & 0x80 > 0,
            dlc,
            data,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CanFd {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

impl<W: Write> Encoder<W> for CanFd {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let can_id = (self.id & 0x1FFF_FFFF) | if self.is_ext_id { 0x8000_0000 } else { 0 };
        let flags = (self.dir & 0x3) | if self.rtr { 0x80 } else { 0 };
        let fd_flags = (if self.fdf { 0x1u8 } else { 0 })
            | (if self.brs { 0x2 } else { 0 })
            | (if self.esi { 0x4 } else { 0 });
        let valid_data_bytes = self.data.len() as u8;
        self.channel.encode(&mut w)?;
        flags.encode(&mut w)?;
        self.dlc.encode(&mut w)?;
        can_id.encode(&mut w)?;
        0u32.encode(&mut w)?; // frame length
        0u8.encode(&mut w)?; // bit count
        fd_flags.encode(&mut w)?;
        valid_data_bytes.encode(&mut w)?;
        w.write_all(b"\x00\x00\x00\x00\x00")?;
        w.write_all(&self.data)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for CanFd {
    type Item = CanFd;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let channel = u16::decode(&mut r)?;
        let flags = u8::decode(&mut r)?;
        let dlc = u8::decode(&mut r)?;
        let can_id = u32::decode(&mut r)?;
        let is_ext_id = can_id & 0x8000_0000;
        let can_id = can_id & 0x1FFF_FFFF;
        u32::decode(&mut r)?; // frame length
        u8::decode(&mut r)?; // bit count
        let fd_flags = u8::decode(&mut r)?;
        let valid_data_bytes = u8::decode(&mut r)?;
        r.read_exact(&mut [0; 5])?;
        let mut data = vec![0u8; valid_data_bytes as usize];
        r.read_exact(&mut data)?;
        Ok(CanFd {
            channel,
            id: can_id,
            is_ext_id: is_ext_id > 0,
            dir: flags & 0x3,
            rtr: flags & 0x80 > 0,
            fdf: fd_flags & 0x1 > 0,
            brs: fd_flags & 0x2 > 0,
            esi: fd_flags & 0x4 > 0,
            dlc,
            data,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CanFd64 {
    pub channel: u8,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

impl<W: Write> Encoder<W> for CanFd64 {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let can_id = (self.id & 0x1FFF_FFFF) | if self.is_ext_id { 0x8000_0000 } else { 0 };
        let data_length = self.data.len() as u8;
        let flags = ((self.dir as u32 & 0x3) << 6)
            | if self.rtr { 0x0010 } else { 0 }
            | if self.fdf { 0x1000 } else { 0 }
            | if self.brs { 0x2000 } else { 0 }
            | if self.esi { 0x4000 } else { 0 };
        self.channel.encode(&mut w)?;
        self.dlc.encode(&mut w)?;
        data_length.encode(&mut w)?; // valid payload length
        0u8.encode(&mut w)?; // tx count
        can_id.encode(&mut w)?;
        0u32.encode(&mut w)?; // frame length
        flags.encode(&mut w)?;
        0u32.encode(&mut w)?; // bit rate arbitration phase
        0u32.encode(&mut w)?; // bit rate data phase
        0u32.encode(&mut w)?; // time offset BRS
        0u32.encode(&mut w)?; // time offset CRC delimiter
        0u16.encode(&mut w)?; // bit count
        0u8.encode(&mut w)?; // direction
        0u8.encode(&mut w)?; // ext data offset
        0u32.encode(&mut w)?; // CRC
        w.write_all(&self.data)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for CanFd64 {
    type Item = CanFd64;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let channel = u8::decode(&mut r)?;
        let dlc = u8::decode(&mut r)?;
        let data_length = u8::decode(&mut r)?;
        u8::decode(&mut r)?; // tx count
        let can_id = u32::decode(&mut r)?;
        let is_ext_id = can_id & 0x8000_0000;
        let can_id = can_id & 0x1FFF_FFFF;
        u32::decode(&mut r)?; // frame length
        let flags = u32::decode(&mut r)?;
        u32::decode(&mut r)?; // bit rate arbitration phase
        u32::decode(&mut r)?; // bit rate data phase
        u32::decode(&mut r)?; // time offset BRS
        u32::decode(&mut r)?; // time offset CRC delimiter
        u16::decode(&mut r)?; // bit count
        u8::decode(&mut r)?; // direction
        u8::decode(&mut r)?; // ext data offset
        u32::decode(&mut r)?; // CRC
        let mut data = vec![0u8; data_length as usize];
        r.read_exact(&mut data)?;
        Ok(CanFd64 {
            channel,
            id: can_id,
            is_ext_id: is_ext_id > 0,
            dir: ((flags >> 6) & 0x3) as u8,
            rtr: flags & 0x0010 > 0,
            fdf: flags & 0x1000 > 0,
            brs: flags & 0x2000 > 0,
            esi: flags & 0x4000 > 0,
            dlc,
            data,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Vlan {
    pub tpid: u16,
    pub pri: u16,
    pub cfi: u16,
    pub vid: u16,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Ethernet {
    pub channel: u16,
    pub dir: u16,
    pub src_addr: [u8; 6],
    pub dst_addr: [u8; 6],
    pub vlan: Option<Vlan>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

impl<W: Write> Encoder<W> for Ethernet {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let flags = self.dir & 0x3;
        w.write_all(&self.src_addr)?;
        self.channel.encode(&mut w)?;
        w.write_all(&self.dst_addr)?;
        flags.encode(&mut w)?;
        self.ether_type.encode(&mut w)?;
        match &self.vlan {
            Some(vlan) => {
                let tci = (vlan.pri & 0x7) << 13 | (vlan.cfi & 0x1) << 12 | (vlan.vid & 0xFFF);
                vlan.tpid.encode(&mut w)?;
                tci.encode(&mut w)?;
            }
            None => {
                w.write_all(b"\x00\x00\x00\x00")?;
            }
        }
        (self.data.len() as u16).encode(&mut w)?;
        w.write_all(b"\x00\x00\x00\x00\x00\x00\x00\x00")?;
        w.write_all(&self.data)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for Ethernet {
    type Item = Ethernet;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        let mut src_addr = [0u8; 6];
        r.read_exact(&mut src_addr)?;
        let channel = u16::decode(&mut r)?;
        let mut dst_addr = [0u8; 6];
        r.read_exact(&mut dst_addr)?;
        let flags = u16::decode(&mut r)?;
        let ether_type = u16::decode(&mut r)?;
        let tpid = u16::decode(&mut r)?;
        let tci = u16::decode(&mut r)?;
        let vlan = if tpid != 0 {
            Some(Vlan {
                tpid,
                pri: (tci >> 13) & 0x7,
                cfi: (tci >> 12) & 0x1,
                vid: tci & 0xFFF,
            })
        } else {
            None
        };
        let payload_length = u16::decode(&mut r)?;
        r.read_exact(&mut [0u8; 8])?;
        let mut data = vec![0u8; payload_length as usize];
        r.read_exact(&mut data)?;
        Ok(Ethernet {
            channel,
            dir: flags & 0x3,
            src_addr,
            dst_addr,
            vlan,
            ether_type,
            data,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct EthernetEx {
    pub channel: u16,
    pub dir: u16,
    pub src_addr: [u8; 6],
    pub dst_addr: [u8; 6],
    pub vlan: Option<Vlan>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

impl<W: Write> Encoder<W> for EthernetEx {
    fn encode(&self, mut w: W) -> ParseResult<()> {
        let mut frame: Vec<u8> = Vec::new();
        frame.write_all(&self.dst_addr)?;
        frame.write_all(&self.src_addr)?;
        if let Some(vlan) = &self.vlan {
            let tci = ((vlan.pri & 0x7) << 13) | ((vlan.cfi & 0x1) << 12) | (vlan.vid & 0xFFF);
            frame.write_all(&[
                ((vlan.tpid >> 8) & 0xFF) as u8,
                (vlan.tpid & 0xFF) as u8,
                ((tci >> 8) & 0xFF) as u8,
                (tci & 0xFF) as u8,
            ])?;
        }
        frame.write_all(&[
            ((self.ether_type >> 8) & 0xFF) as u8,
            (self.ether_type & 0xFF) as u8,
        ])?;
        let frame_length = frame.len() as u16;
        frame.write_all(&self.data)?;
        0u16.encode(&mut w)?; // struct length
        0u16.encode(&mut w)?; // flags
        self.channel.encode(&mut w)?;
        0u16.encode(&mut w)?; // hardware channel
        0u64.encode(&mut w)?; // frame duration
        0u32.encode(&mut w)?; // frame checksum
        self.dir.encode(&mut w)?;
        frame_length.encode(&mut w)?;
        0u32.encode(&mut w)?; // frame handle
        0u32.encode(&mut w)?; // reserved
        w.write_all(&frame)?;
        Ok(())
    }
}

impl<R: Read> Decoder<R> for EthernetEx {
    type Item = EthernetEx;

    fn decode(mut r: R) -> ParseResult<Self::Item> {
        u16::decode(&mut r)?; // struct length
        u16::decode(&mut r)?; // flags
        let channel = u16::decode(&mut r)?;
        u16::decode(&mut r)?; // hardware channel
        u64::decode(&mut r)?; // frame duration
        u32::decode(&mut r)?; // frame checksum
        let dir = u16::decode(&mut r)?;
        let frame_length = u16::decode(&mut r)?;
        u32::decode(&mut r)?; // frame handle
        u32::decode(&mut r)?; // reserved
        let mut frame = vec![0u8; frame_length as usize];
        r.read_exact(&mut frame)?;
        let mut p = &frame[..];
        let mut dst_addr = [0u8; 6];
        p.read_exact(&mut dst_addr)?;
        let mut src_addr = [0u8; 6];
        p.read_exact(&mut src_addr)?;
        let ether_type = ((p[0] as u16) << 8) | (p[1] as u16);
        let vlan = if ether_type == 0x8100 || ether_type == 0x8800 || ether_type == 0x9100 {
            let tci = ((p[2] as u16) << 8) | (p[3] as u16);
            let vlan = Vlan {
                tpid: ether_type,
                pri: (tci >> 13) & 0x7,
                cfi: (tci >> 12) & 0x1,
                vid: tci & 0xFFF,
            };
            p = &p[4..];
            Some(vlan)
        } else {
            None
        };
        let ether_type = ((p[0] as u16) << 8) | (p[1] as u16);
        let data = p[2..].to_vec();
        Ok(EthernetEx {
            channel,
            dir,
            src_addr,
            dst_addr,
            vlan,
            ether_type,
            data,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Other {
    pub obj_type: ObjType,
    pub obj_data: Vec<u8>,
}

impl Other {
    pub fn decode<R: Read>(mut r: R, obj_type: ObjType, data_size: u32) -> ParseResult<Self> {
        let mut obj_data = vec![0u8; data_size as usize];
        r.read_exact(&mut obj_data)?;
        Ok(Other { obj_type, obj_data })
    }

    pub fn encode<W: Write>(&self, mut w: W) -> ParseResult<()> {
        Ok(w.write_all(&self.obj_data)?)
    }
}
