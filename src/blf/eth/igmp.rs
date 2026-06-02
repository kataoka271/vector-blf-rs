use super::super::error::ParseResult;
use super::super::read_util::{read_u16_be, read_u8};
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgmpType {
    MembershipQuery,    // 0x11
    V1MembershipReport, // 0x12
    V2MembershipReport, // 0x16
    LeaveGroup,         // 0x17
    V3MembershipReport, // 0x22
    Other(u8),
}

impl IgmpType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x11 => Self::MembershipQuery,
            0x12 => Self::V1MembershipReport,
            0x16 => Self::V2MembershipReport,
            0x17 => Self::LeaveGroup,
            0x22 => Self::V3MembershipReport,
            v => Self::Other(v),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::MembershipQuery => 0x11,
            Self::V1MembershipReport => 0x12,
            Self::V2MembershipReport => 0x16,
            Self::LeaveGroup => 0x17,
            Self::V3MembershipReport => 0x22,
            Self::Other(v) => v,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::MembershipQuery => "MembershipQuery",
            Self::V1MembershipReport => "V1MembershipReport",
            Self::V2MembershipReport => "V2MembershipReport",
            Self::LeaveGroup => "LeaveGroup",
            Self::V3MembershipReport => "V3MembershipReport",
            Self::Other(_) => "Other",
        }
    }
}

/// IGMPv2 message (RFC 2236).  Also covers the common 8-byte prefix of IGMPv3.
#[derive(Debug)]
pub struct Igmp {
    pub igmp_type: IgmpType,
    /// Max response time in tenths of a second (IGMPv2 queries); zero for reports.
    pub max_resp_time: u8,
    /// Multicast group address (all-zeros for general queries).
    pub group_addr: [u8; 4],
}

impl Igmp {
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let igmp_type = IgmpType::from_u8(read_u8(&mut r)?);
        let max_resp_time = read_u8(&mut r)?;
        read_u16_be(&mut r)?; // checksum
        let mut group_addr = [0u8; 4];
        r.read_exact(&mut group_addr)?;
        Ok(Igmp {
            igmp_type,
            max_resp_time,
            group_addr,
        })
    }
}
