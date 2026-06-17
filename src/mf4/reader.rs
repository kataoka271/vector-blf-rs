use super::block::{
    self, CcBlock, CgBlock, CnBlock, DataBlockKind, DgBlock, HdBlock, IdBlock, SiBlock,
    BUS_TYPE_CAN, BUS_TYPE_ETHERNET, CN_TYPE_MASTER, DT_BYTE_ARRAY, DT_FLOAT_LE, DT_UINT_LE,
};
use crate::blf::message::{Can, CanFd, Dir, Ethernet, Message, Mf4Signal, Vlan};
use crate::blf::BaseObject;
use crate::blf::{ParseError, ParseResult, Timestamp};
use std::io::{Read, Seek, SeekFrom};

// ── channel/group metadata ────────────────────────────────────────────────────

struct ChannelInfo {
    name: String,
    unit: String,
    channel_type: u8,
    data_type: u8,
    bit_offset: u8,
    byte_offset: u32,
    bit_count: u32,
    conv_a: f64,
    conv_b: f64,
    is_identity_conv: bool,
}

impl ChannelInfo {
    fn extract_f64(&self, record: &[u8]) -> f64 {
        let raw = block::extract_raw(
            record,
            self.data_type,
            self.byte_offset,
            self.bit_count,
            self.bit_offset,
        );
        if self.is_identity_conv {
            raw
        } else {
            self.conv_a + self.conv_b * raw
        }
    }

    fn extract_bytes<'a>(&self, record: &'a [u8]) -> &'a [u8] {
        block::extract_bytes(record, self.byte_offset, self.bit_count / 8)
    }
}

struct CanLayout {
    time_idx: usize,
    id_idx: usize,
    dlc_idx: usize,
    data_bytes_idx: usize,
    data_len_idx: Option<usize>,
    dir_idx: Option<usize>,
    is_fd_idx: Option<usize>,
    brs_idx: Option<usize>,
    esi_idx: Option<usize>,
    channel_idx: Option<usize>,
}

struct EthLayout {
    time_idx: usize,
    channel_idx: Option<usize>,
    dir_idx: Option<usize>,
    ether_type_idx: Option<usize>,
    src_mac_idx: Option<usize>,
    dst_mac_idx: Option<usize>,
    data_idx: Option<usize>,
    data_len_idx: Option<usize>,
}

enum GroupKind {
    Can(CanLayout),
    Ethernet(EthLayout),
    Scalar,
}

struct GroupInfo {
    name: String,
    channels: Vec<ChannelInfo>,
    record_size: u32,
    cycle_count: u64,
    data: DataBlockKind,
    kind: GroupKind,
    // for Scalar groups: which channels carry actual data (not master)
    data_channel_indices: Vec<usize>,
}

// ── reader ────────────────────────────────────────────────────────────────────

pub struct Reader<R: Read + Seek> {
    r: R,
    pub start_time_ns: u64,
    groups: Vec<GroupInfo>,
    // iterator state
    group_idx: usize,
    record_idx: u64,
    scalar_ch_idx: usize,
    // decompressed buffer for DZ blocks
    dz_buf: Vec<u8>,
    dz_for_group: Option<usize>,
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(mut r: R) -> ParseResult<Self> {
        IdBlock::read(&mut r)?;
        let hd = HdBlock::read(&mut r)?;
        let start_time_ns = hd.start_time_ns;
        let groups = Self::build_index(&mut r, hd.first_dg)?;
        Ok(Self {
            r,
            start_time_ns,
            groups,
            group_idx: 0,
            record_idx: 0,
            scalar_ch_idx: 0,
            dz_buf: Vec::new(),
            dz_for_group: None,
        })
    }

    fn build_index(r: &mut R, first_dg: u64) -> ParseResult<Vec<GroupInfo>> {
        let mut groups = Vec::new();
        let mut dg_offset = first_dg;
        while dg_offset != 0 {
            r.seek(SeekFrom::Start(dg_offset))?;
            let dg = DgBlock::read(r)?;
            let next_dg = dg.next_dg;
            let mut cg_offset = dg.first_cg;
            while cg_offset != 0 {
                r.seek(SeekFrom::Start(cg_offset))?;
                let cg = CgBlock::read(r)?;
                let next_cg = cg.next_cg;
                let group_name = block::read_tx(r, cg.acq_name).unwrap_or_default();
                let si = SiBlock::read(r, cg.acq_source)?;

                // Read all channels in this group.
                let mut channels = Vec::new();
                let mut cn_offset = cg.first_cn;
                while cn_offset != 0 {
                    r.seek(SeekFrom::Start(cn_offset))?;
                    let cn = CnBlock::read(r)?;
                    let name = block::read_tx(r, cn.name_offset).unwrap_or_default();
                    let unit = block::read_tx(r, cn.unit_offset).unwrap_or_default();
                    let cc = CcBlock::read(r, cn.cc_offset)?;
                    channels.push(ChannelInfo {
                        name,
                        unit,
                        channel_type: cn.channel_type,
                        data_type: cn.data_type,
                        bit_offset: cn.bit_offset,
                        byte_offset: cn.byte_offset,
                        bit_count: cn.bit_count,
                        conv_a: cc.a,
                        conv_b: cc.b,
                        is_identity_conv: cc.conv_type == block::CC_TYPE_IDENTITY,
                    });
                    cn_offset = cn.next_cn;
                }

                // Load the data block metadata (no decompression yet).
                let data = if dg.data_block != 0 && cg.cycle_count > 0 {
                    match DataBlockKind::read_at(r, dg.data_block) {
                        Ok(d) => d,
                        Err(_) => {
                            cg_offset = next_cg;
                            continue;
                        }
                    }
                } else {
                    cg_offset = next_cg;
                    continue;
                };

                let kind = Self::classify_group(si.bus_type, &channels);
                let data_channel_indices: Vec<usize> = channels
                    .iter()
                    .enumerate()
                    .filter(|(_, ch)| ch.channel_type != CN_TYPE_MASTER)
                    .map(|(i, _)| i)
                    .collect();

                groups.push(GroupInfo {
                    name: group_name,
                    channels,
                    record_size: cg.data_bytes,
                    cycle_count: cg.cycle_count,
                    data,
                    kind,
                    data_channel_indices,
                });
                cg_offset = next_cg;
            }
            dg_offset = next_dg;
        }
        Ok(groups)
    }

    fn classify_group(bus_type: u8, channels: &[ChannelInfo]) -> GroupKind {
        if bus_type == BUS_TYPE_CAN {
            if let Some(layout) = Self::try_can_layout(channels) {
                return GroupKind::Can(layout);
            }
        }
        if bus_type == BUS_TYPE_ETHERNET {
            if let Some(layout) = Self::try_eth_layout(channels) {
                return GroupKind::Ethernet(layout);
            }
        }
        // Fall back to scalar: any group with a master time channel and data channels.
        GroupKind::Scalar
    }

    fn find_ch(channels: &[ChannelInfo], needle: &str) -> Option<usize> {
        channels
            .iter()
            .position(|ch| ch.name.to_uppercase().contains(needle))
    }

    fn find_master(channels: &[ChannelInfo]) -> Option<usize> {
        channels
            .iter()
            .position(|ch| ch.channel_type == CN_TYPE_MASTER)
    }

    fn try_can_layout(channels: &[ChannelInfo]) -> Option<CanLayout> {
        let time_idx = Self::find_master(channels)?;
        let id_idx =
            Self::find_ch(channels, "ID").or_else(|| Self::find_ch(channels, "IDENTIFIER"))?;
        let dlc_idx = Self::find_ch(channels, "DLC")?;
        let data_bytes_idx =
            Self::find_ch(channels, "DATABYTES").or_else(|| Self::find_ch(channels, "DATA"))?;
        Some(CanLayout {
            time_idx,
            id_idx,
            dlc_idx,
            data_bytes_idx,
            data_len_idx: Self::find_ch(channels, "DATALENGTH"),
            dir_idx: Self::find_ch(channels, "DIR"),
            is_fd_idx: Self::find_ch(channels, "EDL").or_else(|| Self::find_ch(channels, "ISFD")),
            brs_idx: Self::find_ch(channels, "BRS"),
            esi_idx: Self::find_ch(channels, "ESI"),
            channel_idx: Self::find_ch(channels, "BUSCHANNEL")
                .or_else(|| Self::find_ch(channels, "CHANNEL")),
        })
    }

    fn try_eth_layout(channels: &[ChannelInfo]) -> Option<EthLayout> {
        let time_idx = Self::find_master(channels)?;
        Some(EthLayout {
            time_idx,
            channel_idx: Self::find_ch(channels, "CHANNEL"),
            dir_idx: Self::find_ch(channels, "DIR"),
            ether_type_idx: Self::find_ch(channels, "ETHERTYPE"),
            src_mac_idx: Self::find_ch(channels, "SRC")
                .or_else(|| Self::find_ch(channels, "SOURCE")),
            dst_mac_idx: Self::find_ch(channels, "DST").or_else(|| Self::find_ch(channels, "DEST")),
            data_idx: Self::find_ch(channels, "DATABYTES")
                .or_else(|| Self::find_ch(channels, "DATA")),
            data_len_idx: Self::find_ch(channels, "DATALENGTH"),
        })
    }

    // ── record access ──────────────────────────────────────────────────────────

    fn ensure_dz_decompressed(&mut self, group_idx: usize) -> ParseResult<()> {
        if self.dz_for_group == Some(group_idx) {
            return Ok(());
        }
        self.dz_buf.clear();
        if let DataBlockKind::Dz(ref dz) = self.groups[group_idx].data {
            dz.decompress(&mut self.r, &mut self.dz_buf)?;
            self.dz_for_group = Some(group_idx);
        }
        Ok(())
    }

    fn read_record(&mut self, group_idx: usize, record_idx: u64) -> ParseResult<Vec<u8>> {
        let group = &self.groups[group_idx];
        let rec_size = group.record_size as usize;
        let rec_offset = record_idx as usize * rec_size;
        match &group.data {
            DataBlockKind::Dt {
                offset,
                data_length,
            } => {
                if rec_offset + rec_size > *data_length as usize {
                    return Err(ParseError::Eof);
                }
                let abs = offset + rec_offset as u64;
                self.r.seek(SeekFrom::Start(abs))?;
                let mut buf = vec![0u8; rec_size];
                self.r.read_exact(&mut buf)?;
                Ok(buf)
            }
            DataBlockKind::Dz(_) => {
                self.ensure_dz_decompressed(group_idx)?;
                let end = rec_offset + rec_size;
                if end > self.dz_buf.len() {
                    return Err(ParseError::Eof);
                }
                Ok(self.dz_buf[rec_offset..end].to_vec())
            }
        }
    }

    // ── emit helpers ───────────────────────────────────────────────────────────

    fn timestamp_ns(&self, time_s: f64) -> u64 {
        self.start_time_ns
            + (time_s * 1_000_000_000.0) as u64
    }

    fn emit_can(&self, record: &[u8], layout: &CanLayout) -> BaseObject {
        let ch = &self.groups[self.group_idx];
        let time_s = ch.channels[layout.time_idx].extract_f64(record);
        let ts = self.timestamp_ns(time_s);
        let can_id = ch.channels[layout.id_idx].extract_f64(record) as u32;
        let dlc = ch.channels[layout.dlc_idx].extract_f64(record) as u8;

        // Actual data byte count: prefer DataLength channel, else infer from DLC.
        let data_len = if let Some(idx) = layout.data_len_idx {
            ch.channels[idx].extract_f64(record) as usize
        } else {
            dlc_to_len(dlc)
        };

        let raw_data_ch = &ch.channels[layout.data_bytes_idx];
        let raw_data = if raw_data_ch.data_type == DT_BYTE_ARRAY {
            raw_data_ch.extract_bytes(record)
        } else {
            // Fallback: treat as byte_count bytes starting at byte_offset.
            let end = (raw_data_ch.byte_offset as usize + data_len.min(64)).min(record.len());
            &record[raw_data_ch.byte_offset as usize..end]
        };
        let data = raw_data[..data_len.min(raw_data.len())].to_vec();

        let dir = layout
            .dir_idx
            .map(|i| Dir::from_u8(ch.channels[i].extract_f64(record) as u8))
            .unwrap_or(Dir::Unknown(0));
        let channel = layout
            .channel_idx
            .map(|i| ch.channels[i].extract_f64(record) as u16)
            .unwrap_or(1);
        let is_ext_id = (can_id & 0x8000_0000) != 0;
        let can_id_clean = can_id & 0x1FFF_FFFF;

        let is_fd = layout
            .is_fd_idx
            .map(|i| ch.channels[i].extract_f64(record) as u8 != 0)
            .unwrap_or(false);

        let message = if is_fd {
            let brs = layout
                .brs_idx
                .map(|i| ch.channels[i].extract_f64(record) as u8 != 0)
                .unwrap_or(false);
            let esi = layout
                .esi_idx
                .map(|i| ch.channels[i].extract_f64(record) as u8 != 0)
                .unwrap_or(false);
            Message::CanFd(CanFd {
                channel,
                id: can_id_clean,
                is_ext_id,
                dir,
                rtr: false,
                fdf: true,
                brs,
                esi,
                dlc,
                data,
            })
        } else {
            Message::Can(Can {
                channel,
                id: can_id_clean,
                is_ext_id,
                dir,
                rtr: false,
                dlc,
                data,
            })
        };
        BaseObject {
            timestamp: Timestamp::Nanosecond(ts),
            message,
        }
    }

    fn emit_eth(&self, record: &[u8], layout: &EthLayout) -> BaseObject {
        let ch = &self.groups[self.group_idx];
        let time_s = ch.channels[layout.time_idx].extract_f64(record);
        let ts = self.timestamp_ns(time_s);
        let channel = layout
            .channel_idx
            .map(|i| ch.channels[i].extract_f64(record) as u16)
            .unwrap_or(0);
        let dir = layout
            .dir_idx
            .map(|i| Dir::from_u8(ch.channels[i].extract_f64(record) as u8))
            .unwrap_or(Dir::Unknown(0));
        let ether_type = layout
            .ether_type_idx
            .map(|i| ch.channels[i].extract_f64(record) as u16)
            .unwrap_or(0);
        let src_addr = layout
            .src_mac_idx
            .map(|i| {
                let raw = ch.channels[i].extract_bytes(record);
                let mut a = [0u8; 6];
                a[..raw.len().min(6)].copy_from_slice(&raw[..raw.len().min(6)]);
                a
            })
            .unwrap_or([0u8; 6]);
        let dst_addr = layout
            .dst_mac_idx
            .map(|i| {
                let raw = ch.channels[i].extract_bytes(record);
                let mut a = [0u8; 6];
                a[..raw.len().min(6)].copy_from_slice(&raw[..raw.len().min(6)]);
                a
            })
            .unwrap_or([0u8; 6]);
        let data_len = layout
            .data_len_idx
            .map(|i| ch.channels[i].extract_f64(record) as usize)
            .unwrap_or(0);
        let data = if let Some(idx) = layout.data_idx {
            let raw = ch.channels[idx].extract_bytes(record);
            raw[..data_len.min(raw.len())].to_vec()
        } else {
            vec![]
        };
        BaseObject {
            timestamp: Timestamp::Nanosecond(ts),
            message: Message::Ethernet(Ethernet {
                channel,
                dir,
                src_addr,
                dst_addr,
                vlan: Option::<Vlan>::None,
                ether_type,
                data,
            }),
        }
    }

    fn emit_scalar(&self, record: &[u8], ch_idx: usize) -> BaseObject {
        let group = &self.groups[self.group_idx];
        // Find master channel for timestamp.
        let time_s = group
            .channels
            .iter()
            .find(|ch| ch.channel_type == CN_TYPE_MASTER)
            .map(|ch| ch.extract_f64(record))
            .unwrap_or(0.0);
        let ts = self.timestamp_ns(time_s);
        let ch = &group.channels[ch_idx];
        let value = ch.extract_f64(record);
        BaseObject {
            timestamp: Timestamp::Nanosecond(ts),
            message: Message::Mf4Signal(Mf4Signal {
                group: group.name.clone(),
                name: ch.name.clone(),
                value,
                unit: ch.unit.clone(),
            }),
        }
    }

    // ── Iterator implementation ────────────────────────────────────────────────

    fn next_object(&mut self) -> Option<ParseResult<BaseObject>> {
        loop {
            if self.group_idx >= self.groups.len() {
                return None;
            }
            let group = &self.groups[self.group_idx];
            if self.record_idx >= group.cycle_count {
                self.group_idx += 1;
                self.record_idx = 0;
                self.scalar_ch_idx = 0;
                continue;
            }

            let record = match self.read_record(self.group_idx, self.record_idx) {
                Ok(r) => r,
                Err(ParseError::Eof) => {
                    self.group_idx += 1;
                    self.record_idx = 0;
                    self.scalar_ch_idx = 0;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            };

            // We need to borrow group again after read_record (which borrows self mutably).
            let group = &self.groups[self.group_idx];

            match &group.kind {
                GroupKind::Can(_) => {
                    let GroupKind::Can(layout) = &self.groups[self.group_idx].kind else {
                        unreachable!()
                    };
                    let obj = self.emit_can(&record, layout);
                    self.record_idx += 1;
                    return Some(Ok(obj));
                }
                GroupKind::Ethernet(_) => {
                    let GroupKind::Ethernet(layout) = &self.groups[self.group_idx].kind else {
                        unreachable!()
                    };
                    let obj = self.emit_eth(&record, layout);
                    self.record_idx += 1;
                    return Some(Ok(obj));
                }
                GroupKind::Scalar => {
                    let data_ch_indices = group.data_channel_indices.clone();
                    if data_ch_indices.is_empty() {
                        self.record_idx += 1;
                        self.scalar_ch_idx = 0;
                        continue;
                    }
                    if self.scalar_ch_idx >= data_ch_indices.len() {
                        self.record_idx += 1;
                        self.scalar_ch_idx = 0;
                        continue;
                    }
                    let ch_idx = data_ch_indices[self.scalar_ch_idx];
                    let obj = self.emit_scalar(&record, ch_idx);
                    self.scalar_ch_idx += 1;
                    if self.scalar_ch_idx >= data_ch_indices.len() {
                        self.record_idx += 1;
                        self.scalar_ch_idx = 0;
                    }
                    return Some(Ok(obj));
                }
            }
        }
    }
}

impl<R: Read + Seek> Iterator for Reader<R> {
    type Item = ParseResult<BaseObject>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_object()
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn dlc_to_len(dlc: u8) -> usize {
    match dlc {
        0..=8 => dlc as usize,
        9 => 12,
        10 => 16,
        11 => 20,
        12 => 24,
        13 => 32,
        14 => 48,
        _ => 64,
    }
}

// Silence unused warnings for DT_UINT_LE/DT_FLOAT_LE brought in but not used directly.
const _: u8 = DT_UINT_LE;
const _: u8 = DT_FLOAT_LE;
