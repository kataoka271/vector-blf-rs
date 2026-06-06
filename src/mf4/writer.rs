/// MF4 Writer: buffers incoming BaseObjects and writes a valid MDF 4.10 file on finish().
///
/// Record layouts (all little-endian):
///
/// CAN/CAN-FD channel group (record_size = 84 bytes):
///   0: time_s (f64)      — seconds since start_time_ns
///   8: can_id (u32)
///  12: dlc (u8)
///  13: dir (u8)
///  14: channel (u16)
///  16: is_fd (u8)
///  17: brs (u8)
///  18: esi (u8)
///  19: data_len (u8)     — actual data bytes present (0..=64)
///  20: data (64 bytes)   — zero-padded
///
/// Scalar channel group (record_size = 16 bytes):
///   0: time_s (f64)
///   8: value (f64)
///
/// Ethernet channel group (record_size = ETH_RECORD_SIZE bytes):
///   0: time_s (f64)
///   8: channel (u16)
///  10: dir (u8)
///  11: pad (u8)
///  12: ether_type (u16)
///  14: data_len (u16)
///  16: src_addr (6 bytes)
///  22: dst_addr (6 bytes)
///  28: data (MAX_ETH_DATA bytes, zero-padded)
use super::block::{
    self, CcBlock, CgBlock, CnBlock, CnChannelSpec, DgBlock, HdBlock, IdBlock, SiBlock,
    BUS_TYPE_CAN, BUS_TYPE_ETHERNET, CN_SYNC_NONE, CN_SYNC_TIME, CN_TYPE_DATA, CN_TYPE_MASTER,
    DT_BYTE_ARRAY, DT_FLOAT_LE, DT_UINT_LE,
};
use crate::blf::message::Message;
use crate::blf::BaseObject;
use crate::blf::{ParseResult, Timestamp};
use std::collections::BTreeMap;
use std::io::{Seek, Write};

// ── record layout constants ───────────────────────────────────────────────────

const CAN_RECORD_SIZE: u32 = 84;
const CAN_TIME_OFFSET: u32 = 0;
const CAN_ID_OFFSET: u32 = 8;
const CAN_DLC_OFFSET: u32 = 12;
const CAN_DIR_OFFSET: u32 = 13;
const CAN_CHANNEL_OFFSET: u32 = 14;
const CAN_IS_FD_OFFSET: u32 = 16;
const CAN_BRS_OFFSET: u32 = 17;
const CAN_ESI_OFFSET: u32 = 18;
const CAN_DATA_LEN_OFFSET: u32 = 19;
const CAN_DATA_OFFSET: u32 = 20;

const SCALAR_RECORD_SIZE: u32 = 16;
const SCALAR_TIME_OFFSET: u32 = 0;
const SCALAR_VALUE_OFFSET: u32 = 8;

const MAX_ETH_DATA: u32 = 1514;
const ETH_RECORD_SIZE: u32 = 28 + MAX_ETH_DATA;
const ETH_TIME_OFFSET: u32 = 0;
const ETH_CHANNEL_OFFSET: u32 = 8;
const ETH_DIR_OFFSET: u32 = 10;
const ETH_ETHER_TYPE_OFFSET: u32 = 12;
const ETH_DATA_LEN_OFFSET: u32 = 14;
const ETH_SRC_OFFSET: u32 = 16;
const ETH_DST_OFFSET: u32 = 22;
const ETH_DATA_OFFSET: u32 = 28;

// ── writer ────────────────────────────────────────────────────────────────────

pub struct Writer<W: Write + Seek> {
    w: W,
    start_time_ns: u64,
    hd_pos: u64,
    can_records: Vec<u8>,
    eth_records: Vec<u8>,
    // group_name → channel_name → (unit, records_buf)
    scalar_groups: BTreeMap<String, BTreeMap<String, (String, Vec<u8>)>>,
}

impl<W: Write + Seek> Writer<W> {
    pub fn new(mut w: W, start_time_ns: u64) -> ParseResult<Self> {
        IdBlock::write(&mut w)?;
        let hd_pos = HdBlock::write(&mut w, start_time_ns)?;
        Ok(Self {
            w,
            start_time_ns,
            hd_pos,
            can_records: Vec::new(),
            eth_records: Vec::new(),
            scalar_groups: BTreeMap::new(),
        })
    }

    pub fn write_base_object(&mut self, obj: &BaseObject) -> ParseResult<()> {
        let time_s = self.to_time_s(obj.timestamp);
        match &obj.message {
            Message::Can(m) => {
                let mut rec = [0u8; CAN_RECORD_SIZE as usize];
                write_f64_at(&mut rec, CAN_TIME_OFFSET, time_s);
                write_u32_at(
                    &mut rec,
                    CAN_ID_OFFSET,
                    m.id | if m.is_ext_id { 0x8000_0000 } else { 0 },
                );
                rec[CAN_DLC_OFFSET as usize] = m.dlc;
                rec[CAN_DIR_OFFSET as usize] = m.dir.to_u8();
                write_u16_at(&mut rec, CAN_CHANNEL_OFFSET, m.channel);
                let dlen = m.data.len().min(64) as u8;
                rec[CAN_DATA_LEN_OFFSET as usize] = dlen;
                rec[CAN_DATA_OFFSET as usize..CAN_DATA_OFFSET as usize + dlen as usize]
                    .copy_from_slice(&m.data[..dlen as usize]);
                self.can_records.extend_from_slice(&rec);
            }
            Message::CanFd(m) => {
                let mut rec = [0u8; CAN_RECORD_SIZE as usize];
                write_f64_at(&mut rec, CAN_TIME_OFFSET, time_s);
                write_u32_at(
                    &mut rec,
                    CAN_ID_OFFSET,
                    m.id | if m.is_ext_id { 0x8000_0000 } else { 0 },
                );
                rec[CAN_DLC_OFFSET as usize] = m.dlc;
                rec[CAN_DIR_OFFSET as usize] = m.dir.to_u8();
                write_u16_at(&mut rec, CAN_CHANNEL_OFFSET, m.channel);
                rec[CAN_IS_FD_OFFSET as usize] = 1;
                rec[CAN_BRS_OFFSET as usize] = m.brs as u8;
                rec[CAN_ESI_OFFSET as usize] = m.esi as u8;
                let dlen = m.data.len().min(64) as u8;
                rec[CAN_DATA_LEN_OFFSET as usize] = dlen;
                rec[CAN_DATA_OFFSET as usize..CAN_DATA_OFFSET as usize + dlen as usize]
                    .copy_from_slice(&m.data[..dlen as usize]);
                self.can_records.extend_from_slice(&rec);
            }
            Message::CanFd64(m) => {
                let mut rec = [0u8; CAN_RECORD_SIZE as usize];
                write_f64_at(&mut rec, CAN_TIME_OFFSET, time_s);
                write_u32_at(
                    &mut rec,
                    CAN_ID_OFFSET,
                    m.id | if m.is_ext_id { 0x8000_0000 } else { 0 },
                );
                rec[CAN_DLC_OFFSET as usize] = m.dlc;
                rec[CAN_DIR_OFFSET as usize] = m.dir.to_u8();
                write_u16_at(&mut rec, CAN_CHANNEL_OFFSET, m.channel as u16);
                rec[CAN_IS_FD_OFFSET as usize] = 1;
                rec[CAN_BRS_OFFSET as usize] = m.brs as u8;
                rec[CAN_ESI_OFFSET as usize] = m.esi as u8;
                let dlen = m.data.len().min(64) as u8;
                rec[CAN_DATA_LEN_OFFSET as usize] = dlen;
                rec[CAN_DATA_OFFSET as usize..CAN_DATA_OFFSET as usize + dlen as usize]
                    .copy_from_slice(&m.data[..dlen as usize]);
                self.can_records.extend_from_slice(&rec);
            }
            Message::Ethernet(m) => {
                let mut rec = vec![0u8; ETH_RECORD_SIZE as usize];
                write_f64_at(&mut rec, ETH_TIME_OFFSET, time_s);
                write_u16_at(&mut rec, ETH_CHANNEL_OFFSET, m.channel);
                rec[ETH_DIR_OFFSET as usize] = m.dir.to_u8();
                write_u16_at(&mut rec, ETH_ETHER_TYPE_OFFSET, m.ether_type);
                let dlen = m.data.len().min(MAX_ETH_DATA as usize) as u16;
                write_u16_at(&mut rec, ETH_DATA_LEN_OFFSET, dlen);
                rec[ETH_SRC_OFFSET as usize..ETH_SRC_OFFSET as usize + 6]
                    .copy_from_slice(&m.src_addr);
                rec[ETH_DST_OFFSET as usize..ETH_DST_OFFSET as usize + 6]
                    .copy_from_slice(&m.dst_addr);
                rec[ETH_DATA_OFFSET as usize..ETH_DATA_OFFSET as usize + dlen as usize]
                    .copy_from_slice(&m.data[..dlen as usize]);
                self.eth_records.extend_from_slice(&rec);
            }
            Message::EthernetEx(m) => {
                let mut rec = vec![0u8; ETH_RECORD_SIZE as usize];
                write_f64_at(&mut rec, ETH_TIME_OFFSET, time_s);
                write_u16_at(&mut rec, ETH_CHANNEL_OFFSET, m.channel);
                rec[ETH_DIR_OFFSET as usize] = m.dir.to_u8();
                write_u16_at(&mut rec, ETH_ETHER_TYPE_OFFSET, m.ether_type);
                let dlen = m.data.len().min(MAX_ETH_DATA as usize) as u16;
                write_u16_at(&mut rec, ETH_DATA_LEN_OFFSET, dlen);
                rec[ETH_SRC_OFFSET as usize..ETH_SRC_OFFSET as usize + 6]
                    .copy_from_slice(&m.src_addr);
                rec[ETH_DST_OFFSET as usize..ETH_DST_OFFSET as usize + 6]
                    .copy_from_slice(&m.dst_addr);
                rec[ETH_DATA_OFFSET as usize..ETH_DATA_OFFSET as usize + dlen as usize]
                    .copy_from_slice(&m.data[..dlen as usize]);
                self.eth_records.extend_from_slice(&rec);
            }
            Message::Mf4Signal(sig) => {
                let group = self.scalar_groups.entry(sig.group.clone()).or_default();
                let ch = group
                    .entry(sig.name.clone())
                    .or_insert_with(|| (sig.unit.clone(), Vec::new()));
                let mut rec = [0u8; SCALAR_RECORD_SIZE as usize];
                write_f64_at(&mut rec, SCALAR_TIME_OFFSET, time_s);
                write_f64_at(&mut rec, SCALAR_VALUE_OFFSET, sig.value);
                ch.1.extend_from_slice(&rec);
            }
            Message::Other(_, _) => {}
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ParseResult<()> {
        let mut dg_offsets: Vec<u64> = Vec::new();

        if !self.can_records.is_empty() {
            let can_data = std::mem::take(&mut self.can_records);
            dg_offsets.push(self.write_can_dg(&can_data)?);
        }

        if !self.eth_records.is_empty() {
            let eth_data = std::mem::take(&mut self.eth_records);
            dg_offsets.push(self.write_eth_dg(&eth_data)?);
        }

        let scalar_groups = std::mem::take(&mut self.scalar_groups);
        for (group_name, channels) in &scalar_groups {
            for (ch_name, (unit, records)) in channels {
                if !records.is_empty() {
                    dg_offsets.push(self.write_scalar_dg(group_name, ch_name, unit, records)?);
                }
            }
        }

        // Link DG blocks: patch next_dg in each DG to point to the one that follows it.
        for i in 0..dg_offsets.len().saturating_sub(1) {
            let next = dg_offsets[i + 1];
            // next_dg is the first link in DG, at dg_pos + BlockHeader::SIZE.
            self.w.seek(std::io::SeekFrom::Start(
                dg_offsets[i] + block::BlockHeader::SIZE,
            ))?;
            block::write_link(&mut self.w, next)?;
        }

        let first_dg = dg_offsets.first().copied().unwrap_or(0);
        HdBlock::patch_first_dg(&mut self.w, self.hd_pos, first_dg)?;
        self.w.seek(std::io::SeekFrom::End(0))?;
        Ok(())
    }

    // ── per-group DG writers ───────────────────────────────────────────────────

    fn write_can_dg(&mut self, data: &[u8]) -> ParseResult<u64> {
        let cycle_count = (data.len() / CAN_RECORD_SIZE as usize) as u64;

        let tx_can = block::write_tx(&mut self.w, "CAN")?;
        let tx_t = block::write_tx(&mut self.w, "t")?;
        let tx_s = block::write_tx(&mut self.w, "s")?;
        let tx_id = block::write_tx(&mut self.w, "CAN_DataFrame.ID")?;
        let tx_dlc = block::write_tx(&mut self.w, "CAN_DataFrame.DLC")?;
        let tx_dir = block::write_tx(&mut self.w, "CAN_DataFrame.Dir")?;
        let tx_ch = block::write_tx(&mut self.w, "CAN_DataFrame.BusChannel")?;
        let tx_edl = block::write_tx(&mut self.w, "CAN_DataFrame.EDL")?;
        let tx_brs = block::write_tx(&mut self.w, "CAN_DataFrame.BRS")?;
        let tx_esi = block::write_tx(&mut self.w, "CAN_DataFrame.ESI")?;
        let tx_dlen = block::write_tx(&mut self.w, "CAN_DataFrame.DataLength")?;
        let tx_data = block::write_tx(&mut self.w, "CAN_DataFrame.DataBytes")?;
        let tx_raw = block::write_tx(&mut self.w, "")?;
        let tx_bytes = block::write_tx(&mut self.w, "Bytes")?;

        let si_name = block::write_tx(&mut self.w, "CAN_Bus")?;
        let si = SiBlock::write(&mut self.w, BUS_TYPE_CAN, si_name, 0)?;
        let cc = CcBlock::write_identity(&mut self.w)?;

        // CN chain built in reverse (each cn's next_cn is the previously written cn).
        let cn_data = CnBlock::write(
            &mut self.w,
            0,
            tx_data,
            tx_bytes,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_BYTE_ARRAY,
                byte_offset: CAN_DATA_OFFSET,
                bit_count: 64 * 8,
            },
        )?;
        let cn_dlen = CnBlock::write(
            &mut self.w,
            cn_data,
            tx_dlen,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_DATA_LEN_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_esi = CnBlock::write(
            &mut self.w,
            cn_dlen,
            tx_esi,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_ESI_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_brs = CnBlock::write(
            &mut self.w,
            cn_esi,
            tx_brs,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_BRS_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_edl = CnBlock::write(
            &mut self.w,
            cn_brs,
            tx_edl,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_IS_FD_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_ch = CnBlock::write(
            &mut self.w,
            cn_edl,
            tx_ch,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_CHANNEL_OFFSET,
                bit_count: 16,
            },
        )?;
        let cn_dir = CnBlock::write(
            &mut self.w,
            cn_ch,
            tx_dir,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_DIR_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_dlc = CnBlock::write(
            &mut self.w,
            cn_dir,
            tx_dlc,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_DLC_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_id = CnBlock::write(
            &mut self.w,
            cn_dlc,
            tx_id,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: CAN_ID_OFFSET,
                bit_count: 32,
            },
        )?;
        let cn_time = CnBlock::write(
            &mut self.w,
            cn_id,
            tx_t,
            tx_s,
            cc,
            si,
            CnChannelSpec {
                channel_type: CN_TYPE_MASTER,
                sync_type: CN_SYNC_TIME,
                data_type: DT_FLOAT_LE,
                byte_offset: CAN_TIME_OFFSET,
                bit_count: 64,
            },
        )?;

        let dt = block::write_dt(&mut self.w, data)?;
        let cg = CgBlock::write(
            &mut self.w,
            0,
            cn_time,
            tx_can,
            si,
            cycle_count,
            CAN_RECORD_SIZE,
        )?;
        DgBlock::write(&mut self.w, 0, cg, dt)
    }

    fn write_eth_dg(&mut self, data: &[u8]) -> ParseResult<u64> {
        let cycle_count = (data.len() / ETH_RECORD_SIZE as usize) as u64;

        let tx_eth = block::write_tx(&mut self.w, "Ethernet")?;
        let tx_t = block::write_tx(&mut self.w, "t")?;
        let tx_s = block::write_tx(&mut self.w, "s")?;
        let tx_ch = block::write_tx(&mut self.w, "Ethernet.BusChannel")?;
        let tx_dir = block::write_tx(&mut self.w, "Ethernet.Dir")?;
        let tx_et = block::write_tx(&mut self.w, "Ethernet.EtherType")?;
        let tx_dlen = block::write_tx(&mut self.w, "Ethernet.DataLength")?;
        let tx_src = block::write_tx(&mut self.w, "Ethernet.Source")?;
        let tx_dst = block::write_tx(&mut self.w, "Ethernet.Destination")?;
        let tx_data = block::write_tx(&mut self.w, "Ethernet.DataBytes")?;
        let tx_raw = block::write_tx(&mut self.w, "")?;
        let tx_bytes = block::write_tx(&mut self.w, "Bytes")?;

        let si_name = block::write_tx(&mut self.w, "Ethernet_Bus")?;
        let si = SiBlock::write(&mut self.w, BUS_TYPE_ETHERNET, si_name, 0)?;
        let cc = CcBlock::write_identity(&mut self.w)?;

        let cn_data = CnBlock::write(
            &mut self.w,
            0,
            tx_data,
            tx_bytes,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_BYTE_ARRAY,
                byte_offset: ETH_DATA_OFFSET,
                bit_count: MAX_ETH_DATA * 8,
            },
        )?;
        let cn_dst = CnBlock::write(
            &mut self.w,
            cn_data,
            tx_dst,
            tx_bytes,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_BYTE_ARRAY,
                byte_offset: ETH_DST_OFFSET,
                bit_count: 6 * 8,
            },
        )?;
        let cn_src = CnBlock::write(
            &mut self.w,
            cn_dst,
            tx_src,
            tx_bytes,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_BYTE_ARRAY,
                byte_offset: ETH_SRC_OFFSET,
                bit_count: 6 * 8,
            },
        )?;
        let cn_dlen = CnBlock::write(
            &mut self.w,
            cn_src,
            tx_dlen,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: ETH_DATA_LEN_OFFSET,
                bit_count: 16,
            },
        )?;
        let cn_et = CnBlock::write(
            &mut self.w,
            cn_dlen,
            tx_et,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: ETH_ETHER_TYPE_OFFSET,
                bit_count: 16,
            },
        )?;
        let cn_dir = CnBlock::write(
            &mut self.w,
            cn_et,
            tx_dir,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: ETH_DIR_OFFSET,
                bit_count: 8,
            },
        )?;
        let cn_ch = CnBlock::write(
            &mut self.w,
            cn_dir,
            tx_ch,
            tx_raw,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_UINT_LE,
                byte_offset: ETH_CHANNEL_OFFSET,
                bit_count: 16,
            },
        )?;
        let cn_time = CnBlock::write(
            &mut self.w,
            cn_ch,
            tx_t,
            tx_s,
            cc,
            si,
            CnChannelSpec {
                channel_type: CN_TYPE_MASTER,
                sync_type: CN_SYNC_TIME,
                data_type: DT_FLOAT_LE,
                byte_offset: ETH_TIME_OFFSET,
                bit_count: 64,
            },
        )?;

        let dt = block::write_dt(&mut self.w, data)?;
        let cg = CgBlock::write(
            &mut self.w,
            0,
            cn_time,
            tx_eth,
            si,
            cycle_count,
            ETH_RECORD_SIZE,
        )?;
        DgBlock::write(&mut self.w, 0, cg, dt)
    }

    fn write_scalar_dg(
        &mut self,
        group_name: &str,
        ch_name: &str,
        unit: &str,
        records: &[u8],
    ) -> ParseResult<u64> {
        let cycle_count = (records.len() / SCALAR_RECORD_SIZE as usize) as u64;

        let tx_group = block::write_tx(&mut self.w, group_name)?;
        let tx_t = block::write_tx(&mut self.w, "t")?;
        let tx_s = block::write_tx(&mut self.w, "s")?;
        let tx_name = block::write_tx(&mut self.w, ch_name)?;
        let tx_unit = block::write_tx(&mut self.w, unit)?;
        let cc = CcBlock::write_identity(&mut self.w)?;

        let cn_val = CnBlock::write(
            &mut self.w,
            0,
            tx_name,
            tx_unit,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_DATA,
                sync_type: CN_SYNC_NONE,
                data_type: DT_FLOAT_LE,
                byte_offset: SCALAR_VALUE_OFFSET,
                bit_count: 64,
            },
        )?;
        let cn_time = CnBlock::write(
            &mut self.w,
            cn_val,
            tx_t,
            tx_s,
            cc,
            0,
            CnChannelSpec {
                channel_type: CN_TYPE_MASTER,
                sync_type: CN_SYNC_TIME,
                data_type: DT_FLOAT_LE,
                byte_offset: SCALAR_TIME_OFFSET,
                bit_count: 64,
            },
        )?;

        let dt = block::write_dt(&mut self.w, records)?;
        let cg = CgBlock::write(
            &mut self.w,
            0,
            cn_time,
            tx_group,
            0,
            cycle_count,
            SCALAR_RECORD_SIZE,
        )?;
        DgBlock::write(&mut self.w, 0, cg, dt)
    }

    fn to_time_s(&self, ts: Timestamp) -> f64 {
        let ns = match ts {
            Timestamp::Nanosecond(n) => n,
            Timestamp::Microsecond(u) => u * 1_000,
        };
        ns.saturating_sub(self.start_time_ns) as f64 / 1_000_000_000.0
    }
}

// ── byte helpers ──────────────────────────────────────────────────────────────

fn write_f64_at(buf: &mut [u8], offset: u32, v: f64) {
    let s = offset as usize;
    buf[s..s + 8].copy_from_slice(&v.to_le_bytes());
}

fn write_u32_at(buf: &mut [u8], offset: u32, v: u32) {
    let s = offset as usize;
    buf[s..s + 4].copy_from_slice(&v.to_le_bytes());
}

fn write_u16_at(buf: &mut [u8], offset: u32, v: u16) {
    let s = offset as usize;
    buf[s..s + 2].copy_from_slice(&v.to_le_bytes());
}
