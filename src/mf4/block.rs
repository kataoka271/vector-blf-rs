// MDF 4.10 block I/O — minimal subset needed for CAN bus logging and scalar signals.
//
// Block layout common to all non-ID blocks:
//   0-3   id (e.g. b"##HD")
//   4-7   reserved
//   8-15  length  (total bytes including this header)
//   16-23 link_count
//   24..  links (link_count * 8 bytes, each a u64 file offset or 0 = null)
//   ...   data fields
#![allow(dead_code)]
use crate::blf::{ParseError, ParseResult};
use std::io::{Read, Seek, SeekFrom, Write};

// ── low-level primitives ──────────────────────────────────────────────────────

pub fn read_u8<R: Read>(r: &mut R) -> ParseResult<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

pub fn read_u16_le<R: Read>(r: &mut R) -> ParseResult<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}

pub fn read_u32_le<R: Read>(r: &mut R) -> ParseResult<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

pub fn read_u64_le<R: Read>(r: &mut R) -> ParseResult<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

pub fn read_i16_le<R: Read>(r: &mut R) -> ParseResult<i16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(i16::from_le_bytes(b))
}

pub fn read_f64_le<R: Read>(r: &mut R) -> ParseResult<f64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_le_bytes(b))
}

pub fn write_u8<W: Write>(w: &mut W, v: u8) -> ParseResult<()> {
    Ok(w.write_all(&[v])?)
}

pub fn write_u16_le<W: Write>(w: &mut W, v: u16) -> ParseResult<()> {
    Ok(w.write_all(&v.to_le_bytes())?)
}

pub fn write_u32_le<W: Write>(w: &mut W, v: u32) -> ParseResult<()> {
    Ok(w.write_all(&v.to_le_bytes())?)
}

pub fn write_u64_le<W: Write>(w: &mut W, v: u64) -> ParseResult<()> {
    Ok(w.write_all(&v.to_le_bytes())?)
}

pub fn write_i16_le<W: Write>(w: &mut W, v: i16) -> ParseResult<()> {
    Ok(w.write_all(&v.to_le_bytes())?)
}

pub fn write_f64_le<W: Write>(w: &mut W, v: f64) -> ParseResult<()> {
    Ok(w.write_all(&v.to_le_bytes())?)
}

// ── block header ──────────────────────────────────────────────────────────────

/// Common header for all blocks except the ID block.
pub struct BlockHeader {
    pub id: [u8; 4],
    pub length: u64,
    pub link_count: u64,
}

impl BlockHeader {
    pub fn read<R: Read>(r: &mut R) -> ParseResult<Self> {
        let mut id = [0u8; 4];
        r.read_exact(&mut id)?;
        let mut reserved = [0u8; 4];
        r.read_exact(&mut reserved)?;
        let length = read_u64_le(r)?;
        let link_count = read_u64_le(r)?;
        Ok(Self {
            id,
            length,
            link_count,
        })
    }

    pub fn write<W: Write>(
        w: &mut W,
        id: &[u8; 4],
        length: u64,
        link_count: u64,
    ) -> ParseResult<()> {
        w.write_all(id)?;
        w.write_all(&[0u8; 4])?;
        write_u64_le(w, length)?;
        write_u64_le(w, link_count)?;
        Ok(())
    }

    /// Total size of the header itself (id + reserved + length field + link_count field).
    pub const SIZE: u64 = 24;
}

pub fn read_link<R: Read>(r: &mut R) -> ParseResult<u64> {
    read_u64_le(r)
}

pub fn write_link<W: Write>(w: &mut W, offset: u64) -> ParseResult<()> {
    write_u64_le(w, offset)
}

// ── ID block (64 bytes at offset 0) ──────────────────────────────────────────

pub const MDF4_FILE_ID: &[u8; 8] = b"MDF     ";

pub struct IdBlock {
    pub version: u16,
}

impl IdBlock {
    pub fn read<R: Read>(r: &mut R) -> ParseResult<Self> {
        let mut file_id = [0u8; 8];
        r.read_exact(&mut file_id)?;
        if &file_id != MDF4_FILE_ID {
            return Err(ParseError::InvalidMf4Magic);
        }
        // vers_id "4.xx    " — read but don't validate strictly
        let mut _vers_id = [0u8; 8];
        r.read_exact(&mut _vers_id)?;
        // prog_id, reserved
        let mut _skip = [0u8; 12];
        r.read_exact(&mut _skip)?;
        let version = read_u16_le(r)?;
        // remainder of 64-byte block: 64 - 8 - 8 - 12 - 2 = 34 bytes
        let mut _tail = [0u8; 34];
        r.read_exact(&mut _tail)?;
        Ok(Self { version })
    }

    pub fn write<W: Write>(w: &mut W) -> ParseResult<()> {
        w.write_all(MDF4_FILE_ID)?;
        w.write_all(b"4.10    ")?;
        // prog_id — 8 bytes
        w.write_all(b"vector-b")?;
        // reserved — 4 bytes
        w.write_all(&[0u8; 4])?;
        // version number 410 (u16 LE)
        write_u16_le(w, 410)?;
        // unfin_flags(u8), custom_unfin_flags lower(u8): 2 bytes
        w.write_all(&[0u8; 2])?;
        // padding to 64 bytes total: 64 - 8 - 8 - 8 - 4 - 2 - 2 = 32 remaining
        w.write_all(&[0u8; 32])?;
        Ok(())
    }
}

// ── HD block (Header, 104 bytes at offset 64) ─────────────────────────────────

pub const HD_LINK_COUNT: u64 = 6;
pub const HD_BLOCK_LENGTH: u64 = BlockHeader::SIZE + HD_LINK_COUNT * 8 + 32;

pub struct HdBlock {
    pub first_dg: u64,
    pub start_time_ns: u64,
}

impl HdBlock {
    pub fn read<R: Read + Seek>(r: &mut R) -> ParseResult<Self> {
        let header = BlockHeader::read(r)?;
        if &header.id != b"##HD" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##HD",
                got: header.id,
            });
        }
        let first_dg = read_link(r)?;
        // first_fh, first_ch, first_at, first_ev, md_comment
        for _ in 0..5 {
            read_link(r)?;
        }
        let start_time_ns = read_u64_le(r)?;
        // skip remainder of data fields
        let data_read = 8u64;
        let total_data = header.length - BlockHeader::SIZE - header.link_count * 8;
        if total_data > data_read {
            r.seek(SeekFrom::Current((total_data - data_read) as i64))?;
        }
        Ok(Self {
            first_dg,
            start_time_ns,
        })
    }

    /// Write the HD block. `first_dg` is a placeholder; call `patch_first_dg` later.
    pub fn write<W: Write + Seek>(w: &mut W, start_time_ns: u64) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        BlockHeader::write(w, b"##HD", HD_BLOCK_LENGTH, HD_LINK_COUNT)?;
        // Links: first_dg (placeholder 0), first_fh=0, first_ch=0, first_at=0, first_ev=0, md_comment=0
        for _ in 0..6 {
            write_link(w, 0)?;
        }
        // Data (32 bytes)
        write_u64_le(w, start_time_ns)?;
        write_i16_le(w, 0)?; // tz_offset_min
        write_i16_le(w, 0)?; // dst_offset_min
        w.write_all(&[0u8; 4])?; // time_flags, time_class, flags, reserved
        write_f64_le(w, 0.0)?; // start_angle_rad
        write_f64_le(w, 0.0)?; // start_distance_m
        Ok(pos)
    }

    /// Back-patch `first_dg` link at the offset returned by `write`.
    pub fn patch_first_dg<W: Write + Seek>(
        w: &mut W,
        hd_pos: u64,
        first_dg: u64,
    ) -> ParseResult<()> {
        // first_dg is the first link, at offset hd_pos + BlockHeader::SIZE
        w.seek(SeekFrom::Start(hd_pos + BlockHeader::SIZE))?;
        write_link(w, first_dg)?;
        Ok(())
    }
}

// ── TX block (text) ───────────────────────────────────────────────────────────

/// Write a text block and return its file offset. Text is stored as UTF-8 + null terminator.
pub fn write_tx<W: Write + Seek>(w: &mut W, text: &str) -> ParseResult<u64> {
    let pos = w.stream_position()?;
    let text_bytes = text.as_bytes();
    let data_len = text_bytes.len() as u64 + 1; // +1 for null terminator
    let total_len = BlockHeader::SIZE + data_len;
    BlockHeader::write(w, b"##TX", total_len, 0)?;
    w.write_all(text_bytes)?;
    w.write_all(&[0u8])?;
    Ok(pos)
}

/// Read a text block at the given offset; returns the string (without null terminator).
pub fn read_tx<R: Read + Seek>(r: &mut R, offset: u64) -> ParseResult<String> {
    if offset == 0 {
        return Ok(String::new());
    }
    r.seek(SeekFrom::Start(offset))?;
    let header = BlockHeader::read(r)?;
    if &header.id != b"##TX" {
        return Err(ParseError::InvalidMf4Block {
            expected: "##TX",
            got: header.id,
        });
    }
    let data_len = (header.length - BlockHeader::SIZE) as usize;
    let mut data = vec![0u8; data_len];
    r.read_exact(&mut data)?;
    // strip null terminator(s)
    while data.last() == Some(&0) {
        data.pop();
    }
    Ok(String::from_utf8_lossy(&data).into_owned())
}

// ── SI block (source information) ────────────────────────────────────────────

pub const SI_LINK_COUNT: u64 = 3;
pub const SI_BLOCK_LENGTH: u64 = BlockHeader::SIZE + SI_LINK_COUNT * 8 + 4;

pub const BUS_TYPE_NONE: u8 = 0;
pub const BUS_TYPE_CAN: u8 = 2;
pub const BUS_TYPE_ETHERNET: u8 = 7;

pub struct SiBlock {
    pub src_type: u8,
    pub bus_type: u8,
}

impl SiBlock {
    pub fn read<R: Read + Seek>(r: &mut R, offset: u64) -> ParseResult<Self> {
        if offset == 0 {
            return Ok(Self {
                src_type: 0,
                bus_type: BUS_TYPE_NONE,
            });
        }
        r.seek(SeekFrom::Start(offset))?;
        let header = BlockHeader::read(r)?;
        if &header.id != b"##SI" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##SI",
                got: header.id,
            });
        }
        for _ in 0..header.link_count {
            read_link(r)?;
        }
        let src_type = read_u8(r)?;
        let bus_type = read_u8(r)?;
        // skip remainder
        let data_read = 2u64;
        let total_data = header.length - BlockHeader::SIZE - header.link_count * 8;
        if total_data > data_read {
            r.seek(SeekFrom::Current((total_data - data_read) as i64))?;
        }
        Ok(Self { src_type, bus_type })
    }

    /// Write an SI block and return its file offset.
    pub fn write<W: Write + Seek>(
        w: &mut W,
        bus_type: u8,
        name: u64,
        path: u64,
    ) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        BlockHeader::write(w, b"##SI", SI_BLOCK_LENGTH, SI_LINK_COUNT)?;
        write_link(w, name)?;
        write_link(w, path)?;
        write_link(w, 0)?; // md_comment
        write_u8(w, 2)?; // type = BUS
        write_u8(w, bus_type)?;
        w.write_all(&[0u8; 2])?; // flags, reserved
        Ok(pos)
    }
}

// ── CC block (channel conversion) ────────────────────────────────────────────

pub const CC_TYPE_IDENTITY: u8 = 0;
pub const CC_TYPE_LINEAR: u8 = 1;

pub struct CcBlock {
    pub conv_type: u8,
    pub a: f64,
    pub b: f64,
}

impl CcBlock {
    pub fn apply(&self, raw: f64) -> f64 {
        match self.conv_type {
            CC_TYPE_LINEAR => self.a + self.b * raw,
            _ => raw,
        }
    }

    pub fn read<R: Read + Seek>(r: &mut R, offset: u64) -> ParseResult<Self> {
        if offset == 0 {
            return Ok(Self {
                conv_type: CC_TYPE_IDENTITY,
                a: 0.0,
                b: 1.0,
            });
        }
        r.seek(SeekFrom::Start(offset))?;
        let header = BlockHeader::read(r)?;
        if &header.id != b"##CC" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##CC",
                got: header.id,
            });
        }
        // skip links
        for _ in 0..header.link_count {
            read_link(r)?;
        }
        let conv_type = read_u8(r)?;
        let _precision = read_u8(r)?;
        let _flags = read_u16_le(r)?;
        let _ref_count = read_u16_le(r)?;
        let val_count = read_u16_le(r)?;
        let _phys_range_min = read_f64_le(r)?;
        let _phys_range_max = read_f64_le(r)?;
        let (a, b) = if conv_type == CC_TYPE_LINEAR && val_count >= 2 {
            let a = read_f64_le(r)?;
            let b = read_f64_le(r)?;
            (a, b)
        } else {
            (0.0, 1.0)
        };
        Ok(Self { conv_type, a, b })
    }

    /// Write an identity CC block and return its offset.
    pub fn write_identity<W: Write + Seek>(w: &mut W) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        let link_count: u64 = 4;
        let data_len: u64 = 1 + 1 + 2 + 2 + 2 + 8 + 8; // 24 bytes
        let total = BlockHeader::SIZE + link_count * 8 + data_len;
        BlockHeader::write(w, b"##CC", total, link_count)?;
        for _ in 0..4 {
            write_link(w, 0)?;
        }
        write_u8(w, CC_TYPE_IDENTITY)?; // type
        write_u8(w, 0)?; // precision
        write_u16_le(w, 0)?; // flags
        write_u16_le(w, 0)?; // ref_count
        write_u16_le(w, 0)?; // val_count
        write_f64_le(w, 0.0)?; // phys_range_min
        write_f64_le(w, 0.0)?; // phys_range_max
        Ok(pos)
    }
}

// ── CN block (channel) ────────────────────────────────────────────────────────

pub const CN_LINK_COUNT: u64 = 8;
pub const CN_DATA_LEN: u64 = 1 + 1 + 1 + 1 + 4 + 4 + 4 + 4 + 1 + 1 + 2 + 8 + 8 + 8 + 8 + 8 + 8;
// = 72 bytes
pub const CN_BLOCK_LENGTH: u64 = BlockHeader::SIZE + CN_LINK_COUNT * 8 + CN_DATA_LEN;

// channel_type values
pub const CN_TYPE_DATA: u8 = 0;
pub const CN_TYPE_MASTER: u8 = 2;

// sync_type values
pub const CN_SYNC_NONE: u8 = 0;
pub const CN_SYNC_TIME: u8 = 1;

// data_type values
pub const DT_UINT_LE: u8 = 0;
pub const DT_UINT_BE: u8 = 1;
pub const DT_INT_LE: u8 = 2;
pub const DT_INT_BE: u8 = 3;
pub const DT_FLOAT_LE: u8 = 4;
pub const DT_FLOAT_BE: u8 = 5;
pub const DT_BYTE_ARRAY: u8 = 14;

pub struct CnBlock {
    pub next_cn: u64,
    pub name_offset: u64,
    pub unit_offset: u64,
    pub cc_offset: u64,
    pub si_offset: u64,
    pub channel_type: u8,
    pub sync_type: u8,
    pub data_type: u8,
    pub bit_offset: u8,
    pub byte_offset: u32,
    pub bit_count: u32,
}

impl CnBlock {
    pub fn read<R: Read + Seek>(r: &mut R) -> ParseResult<Self> {
        let header = BlockHeader::read(r)?;
        if &header.id != b"##CN" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##CN",
                got: header.id,
            });
        }
        // Links order: next_cn, composition, tx_name, si_source, cc_value, cn_data, md_comment, unit
        let next_cn = read_link(r)?;
        let _composition = read_link(r)?;
        let name_offset = read_link(r)?;
        let si_offset = read_link(r)?;
        let cc_offset = read_link(r)?;
        let _data_link = read_link(r)?;
        let _md_comment = read_link(r)?;
        let unit_offset = read_link(r)?;
        // skip any extra links beyond the 8 we expect
        for _ in 8..header.link_count {
            read_link(r)?;
        }
        // Data fields
        let channel_type = read_u8(r)?;
        let sync_type = read_u8(r)?;
        let data_type = read_u8(r)?;
        let bit_offset = read_u8(r)?;
        let byte_offset = read_u32_le(r)?;
        let bit_count = read_u32_le(r)?;
        // Skip remaining data: flags(4) + inval_bit_pos(4) + precision(1) + reserved(1) +
        //                       val_range_n(2) + val_range_min(8) + val_range_max(8) +
        //                       limit_min(8) + limit_max(8) + limit_ext_min(8) + limit_ext_max(8)
        //                     = 60 bytes
        let consumed_data = 1u64 + 1 + 1 + 1 + 4 + 4;
        let total_data = header.length - BlockHeader::SIZE - header.link_count * 8;
        if total_data > consumed_data {
            r.seek(SeekFrom::Current((total_data - consumed_data) as i64))?;
        }
        Ok(Self {
            next_cn,
            name_offset,
            unit_offset,
            cc_offset,
            si_offset,
            channel_type,
            sync_type,
            data_type,
            bit_offset,
            byte_offset,
            bit_count,
        })
    }

    /// Write a CN block and return its file offset.
    pub fn write<W: Write + Seek>(
        w: &mut W,
        next_cn: u64,
        name_tx: u64,
        unit_tx: u64,
        cc_offset: u64,
        si_offset: u64,
        channel_type: u8,
        sync_type: u8,
        data_type: u8,
        byte_offset: u32,
        bit_count: u32,
    ) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        BlockHeader::write(w, b"##CN", CN_BLOCK_LENGTH, CN_LINK_COUNT)?;
        write_link(w, next_cn)?; // cn_cn_next
        write_link(w, 0)?; // cn_composition
        write_link(w, name_tx)?; // cn_tx_name
        write_link(w, si_offset)?; // cn_si_source
        write_link(w, cc_offset)?; // cn_cc_value
        write_link(w, 0)?; // cn_data (for variable-length)
        write_link(w, 0)?; // cn_md_comment
        write_link(w, unit_tx)?; // cn_unit (TX block)
                                 // Data
        write_u8(w, channel_type)?;
        write_u8(w, sync_type)?;
        write_u8(w, data_type)?;
        write_u8(w, 0)?; // bit_offset
        write_u32_le(w, byte_offset)?;
        write_u32_le(w, bit_count)?;
        write_u32_le(w, 0)?; // flags
        write_u32_le(w, 0)?; // inval_bit_pos
        write_u8(w, 0)?; // precision
        write_u8(w, 0)?; // reserved
        write_u16_le(w, 0)?; // val_range_n
        write_f64_le(w, 0.0)?; // val_range_min
        write_f64_le(w, 0.0)?; // val_range_max
        write_f64_le(w, 0.0)?; // limit_min
        write_f64_le(w, 0.0)?; // limit_max
        write_f64_le(w, 0.0)?; // limit_ext_min
        write_f64_le(w, 0.0)?; // limit_ext_max
        Ok(pos)
    }
}

// ── CG block (channel group) ──────────────────────────────────────────────────

pub const CG_LINK_COUNT: u64 = 6;
pub const CG_DATA_LEN: u64 = 8 + 8 + 2 + 2 + 4 + 4 + 4; // 32 bytes
pub const CG_BLOCK_LENGTH: u64 = BlockHeader::SIZE + CG_LINK_COUNT * 8 + CG_DATA_LEN;

pub struct CgBlock {
    pub next_cg: u64,
    pub first_cn: u64,
    pub acq_name: u64,
    pub acq_source: u64,
    pub cycle_count: u64,
    pub data_bytes: u32,
}

impl CgBlock {
    pub fn read<R: Read + Seek>(r: &mut R) -> ParseResult<Self> {
        let header = BlockHeader::read(r)?;
        if &header.id != b"##CG" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##CG",
                got: header.id,
            });
        }
        let next_cg = read_link(r)?;
        let first_cn = read_link(r)?;
        let acq_name = read_link(r)?;
        let acq_source = read_link(r)?;
        let _first_sr = read_link(r)?;
        let _md_comment = read_link(r)?;
        for _ in 6..header.link_count {
            read_link(r)?;
        }
        let _record_id = read_u64_le(r)?;
        let cycle_count = read_u64_le(r)?;
        let _flags = read_u16_le(r)?;
        let _path_sep = read_u16_le(r)?;
        let _reserved = read_u32_le(r)?;
        let data_bytes = read_u32_le(r)?;
        // skip remainder
        let consumed = 8u64 + 8 + 2 + 2 + 4 + 4;
        let total_data = header.length - BlockHeader::SIZE - header.link_count * 8;
        if total_data > consumed {
            r.seek(SeekFrom::Current((total_data - consumed) as i64))?;
        }
        Ok(Self {
            next_cg,
            first_cn,
            acq_name,
            acq_source,
            cycle_count,
            data_bytes,
        })
    }

    pub fn write<W: Write + Seek>(
        w: &mut W,
        next_cg: u64,
        first_cn: u64,
        acq_name_tx: u64,
        acq_source_si: u64,
        cycle_count: u64,
        data_bytes: u32,
    ) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        BlockHeader::write(w, b"##CG", CG_BLOCK_LENGTH, CG_LINK_COUNT)?;
        write_link(w, next_cg)?;
        write_link(w, first_cn)?;
        write_link(w, acq_name_tx)?;
        write_link(w, acq_source_si)?;
        write_link(w, 0)?; // first_sr
        write_link(w, 0)?; // md_comment
        write_u64_le(w, 0)?; // record_id
        write_u64_le(w, cycle_count)?;
        write_u16_le(w, 0)?; // flags
        write_u16_le(w, 0)?; // path_separator
        write_u32_le(w, 0)?; // reserved
        write_u32_le(w, data_bytes)?;
        write_u32_le(w, 0)?; // inval_bytes
        Ok(pos)
    }
}

// ── DG block (data group) ─────────────────────────────────────────────────────

pub const DG_LINK_COUNT: u64 = 4;
pub const DG_DATA_LEN: u64 = 8; // rec_id_size(1) + reserved(7)
pub const DG_BLOCK_LENGTH: u64 = BlockHeader::SIZE + DG_LINK_COUNT * 8 + DG_DATA_LEN;

pub struct DgBlock {
    pub next_dg: u64,
    pub first_cg: u64,
    pub data_block: u64,
}

impl DgBlock {
    pub fn read<R: Read + Seek>(r: &mut R) -> ParseResult<Self> {
        let header = BlockHeader::read(r)?;
        if &header.id != b"##DG" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##DG",
                got: header.id,
            });
        }
        let next_dg = read_link(r)?;
        let first_cg = read_link(r)?;
        let data_block = read_link(r)?;
        let _md_comment = read_link(r)?;
        for _ in 4..header.link_count {
            read_link(r)?;
        }
        // skip data
        let total_data = header.length - BlockHeader::SIZE - header.link_count * 8;
        if total_data > 0 {
            r.seek(SeekFrom::Current(total_data as i64))?;
        }
        Ok(Self {
            next_dg,
            first_cg,
            data_block,
        })
    }

    pub fn write<W: Write + Seek>(
        w: &mut W,
        next_dg: u64,
        first_cg: u64,
        data_block: u64,
    ) -> ParseResult<u64> {
        let pos = w.stream_position()?;
        BlockHeader::write(w, b"##DG", DG_BLOCK_LENGTH, DG_LINK_COUNT)?;
        write_link(w, next_dg)?;
        write_link(w, first_cg)?;
        write_link(w, data_block)?;
        write_link(w, 0)?; // md_comment
        write_u8(w, 0)?; // rec_id_size
        w.write_all(&[0u8; 7])?; // reserved
        Ok(pos)
    }
}

// ── DT block (data, uncompressed) ────────────────────────────────────────────

/// Write a DT block containing `data` and return its file offset.
pub fn write_dt<W: Write + Seek>(w: &mut W, data: &[u8]) -> ParseResult<u64> {
    let pos = w.stream_position()?;
    let total = BlockHeader::SIZE + data.len() as u64;
    BlockHeader::write(w, b"##DT", total, 0)?;
    w.write_all(data)?;
    Ok(pos)
}

// ── DZ block (data, zlib-compressed) ─────────────────────────────────────────

pub const DZ_ZIP_DEFLATE: u8 = 0;

pub struct DzBlock {
    pub orig_block_type: [u8; 2],
    pub zip_type: u8,
    pub orig_data_length: u64,
    pub data_length: u64,
    // file position of compressed data (just after the DZ header)
    pub data_offset: u64,
}

impl DzBlock {
    pub fn read<R: Read + Seek>(r: &mut R) -> ParseResult<Self> {
        let header = BlockHeader::read(r)?;
        if &header.id != b"##DZ" {
            return Err(ParseError::InvalidMf4Block {
                expected: "##DZ",
                got: header.id,
            });
        }
        for _ in 0..header.link_count {
            read_link(r)?;
        }
        let mut orig_block_type = [0u8; 2];
        r.read_exact(&mut orig_block_type)?;
        let zip_type = read_u8(r)?;
        let _reserved = read_u8(r)?;
        let _zip_param = read_u32_le(r)?;
        let orig_data_length = read_u64_le(r)?;
        let data_length = read_u64_le(r)?;
        let data_offset = r.stream_position()?;
        Ok(Self {
            orig_block_type,
            zip_type,
            orig_data_length,
            data_length,
            data_offset,
        })
    }

    /// Decompress the data into `out`. Requires `zip_type == DZ_ZIP_DEFLATE`.
    pub fn decompress<R: Read + Seek>(&self, r: &mut R, out: &mut Vec<u8>) -> ParseResult<()> {
        if self.zip_type != DZ_ZIP_DEFLATE {
            return Err(ParseError::UnsupportedMf4Feature(format!(
                "DZ zip_type {} (only deflate=0 supported)",
                self.zip_type
            )));
        }
        r.seek(SeekFrom::Start(self.data_offset))?;
        let mut compressed = vec![0u8; self.data_length as usize];
        r.read_exact(&mut compressed)?;
        use flate2::read::ZlibDecoder;
        let mut decoder = ZlibDecoder::new(&compressed[..]);
        out.reserve(self.orig_data_length as usize);
        std::io::Read::read_to_end(&mut decoder, out).map_err(|_| ParseError::ZlibError)?;
        Ok(())
    }
}

// ── data source (DT or DZ) ────────────────────────────────────────────────────

pub enum DataBlockKind {
    Dt { offset: u64, data_length: u64 },
    Dz(DzBlock),
}

impl DataBlockKind {
    pub fn read_at<R: Read + Seek>(r: &mut R, offset: u64) -> ParseResult<Self> {
        if offset == 0 {
            return Err(ParseError::InvalidData);
        }
        r.seek(SeekFrom::Start(offset))?;
        let mut id = [0u8; 4];
        r.read_exact(&mut id)?;
        r.seek(SeekFrom::Start(offset))?;
        match &id {
            b"##DT" | b"##DV" | b"##RD" | b"##DI" => {
                let header = BlockHeader::read(r)?;
                let data_offset = r.stream_position()?;
                Ok(DataBlockKind::Dt {
                    offset: data_offset,
                    data_length: header.length - BlockHeader::SIZE,
                })
            }
            b"##DZ" => Ok(DataBlockKind::Dz(DzBlock::read(r)?)),
            _ => Err(ParseError::InvalidMf4Block {
                expected: "##DT or ##DZ",
                got: id,
            }),
        }
    }

    /// Number of data bytes (after decompression if DZ).
    pub fn orig_data_length(&self) -> u64 {
        match self {
            DataBlockKind::Dt { data_length, .. } => *data_length,
            DataBlockKind::Dz(dz) => dz.orig_data_length,
        }
    }
}

// ── value extraction from a raw record buffer ─────────────────────────────────

/// Extract a raw numeric value from a record byte slice.
pub fn extract_raw(
    record: &[u8],
    data_type: u8,
    byte_offset: u32,
    bit_count: u32,
    bit_offset: u8,
) -> f64 {
    let start = byte_offset as usize;
    let byte_count = (bit_count + bit_offset as u32).div_ceil(8) as usize;
    if start + byte_count > record.len() {
        return 0.0;
    }
    match data_type {
        DT_UINT_LE | DT_INT_LE => {
            let mut val = 0u64;
            for i in 0..byte_count.min(8) {
                val |= (record[start + i] as u64) << (i * 8);
            }
            val >>= bit_offset;
            let mask = if bit_count >= 64 {
                u64::MAX
            } else {
                (1u64 << bit_count).wrapping_sub(1)
            };
            if data_type == DT_INT_LE {
                // sign-extend
                let raw = val & mask;
                let sign_bit = 1u64 << (bit_count - 1);
                if raw & sign_bit != 0 {
                    return ((raw | !mask) as i64) as f64;
                }
                raw as f64
            } else {
                (val & mask) as f64
            }
        }
        DT_UINT_BE | DT_INT_BE => {
            let mut val = 0u64;
            for i in 0..byte_count.min(8) {
                val = (val << 8) | record[start + i] as u64;
            }
            val >>= bit_offset;
            let mask = if bit_count >= 64 {
                u64::MAX
            } else {
                (1u64 << bit_count).wrapping_sub(1)
            };
            (val & mask) as f64
        }
        DT_FLOAT_LE => {
            if bit_count == 32 && start + 4 <= record.len() {
                let b: [u8; 4] = record[start..start + 4].try_into().unwrap();
                f32::from_le_bytes(b) as f64
            } else if bit_count == 64 && start + 8 <= record.len() {
                let b: [u8; 8] = record[start..start + 8].try_into().unwrap();
                f64::from_le_bytes(b)
            } else {
                0.0
            }
        }
        DT_FLOAT_BE => {
            if bit_count == 32 && start + 4 <= record.len() {
                let b: [u8; 4] = record[start..start + 4].try_into().unwrap();
                f32::from_be_bytes(b) as f64
            } else if bit_count == 64 && start + 8 <= record.len() {
                let b: [u8; 8] = record[start..start + 8].try_into().unwrap();
                f64::from_be_bytes(b)
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

/// Extract raw bytes for a byte-array channel (data_type=14).
pub fn extract_bytes(record: &[u8], byte_offset: u32, byte_count: u32) -> &[u8] {
    let start = byte_offset as usize;
    let end = (start + byte_count as usize).min(record.len());
    &record[start..end]
}
