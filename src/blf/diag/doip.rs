use super::super::error::{ParseError, ParseResult};
use super::super::read_util::{read_u16_be, read_u32_be, read_u8};
use super::uds::Uds;
use std::io::Read;

/// DoIP standard port (ISO 13400-2).
pub const DOIP_PORT: u16 = 13400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadType {
    GenericHeaderNegAck,
    VehicleIdentRequest,
    VehicleIdentRequestWithEid,
    VehicleIdentRequestWithVin,
    VehicleAnnouncementResponse,
    RoutingActivationRequest,
    RoutingActivationResponse,
    AliveCheckRequest,
    AliveCheckResponse,
    EntityStatusRequest,
    EntityStatusResponse,
    PowerModeInfoRequest,
    PowerModeInfoResponse,
    DiagMessage,
    DiagMessagePosAck,
    DiagMessageNegAck,
    Unknown(u16),
}

impl PayloadType {
    pub fn from_u16(v: u16) -> Self {
        match v {
            0x0000 => Self::GenericHeaderNegAck,
            0x0001 => Self::VehicleIdentRequest,
            0x0002 => Self::VehicleIdentRequestWithEid,
            0x0003 => Self::VehicleIdentRequestWithVin,
            0x0004 => Self::VehicleAnnouncementResponse,
            0x0005 => Self::RoutingActivationRequest,
            0x0006 => Self::RoutingActivationResponse,
            0x0007 => Self::AliveCheckRequest,
            0x0008 => Self::AliveCheckResponse,
            0x4001 => Self::EntityStatusRequest,
            0x4002 => Self::EntityStatusResponse,
            0x4003 => Self::PowerModeInfoRequest,
            0x4004 => Self::PowerModeInfoResponse,
            0x8001 => Self::DiagMessage,
            0x8002 => Self::DiagMessagePosAck,
            0x8003 => Self::DiagMessageNegAck,
            v => Self::Unknown(v),
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            Self::GenericHeaderNegAck => 0x0000,
            Self::VehicleIdentRequest => 0x0001,
            Self::VehicleIdentRequestWithEid => 0x0002,
            Self::VehicleIdentRequestWithVin => 0x0003,
            Self::VehicleAnnouncementResponse => 0x0004,
            Self::RoutingActivationRequest => 0x0005,
            Self::RoutingActivationResponse => 0x0006,
            Self::AliveCheckRequest => 0x0007,
            Self::AliveCheckResponse => 0x0008,
            Self::EntityStatusRequest => 0x4001,
            Self::EntityStatusResponse => 0x4002,
            Self::PowerModeInfoRequest => 0x4003,
            Self::PowerModeInfoResponse => 0x4004,
            Self::DiagMessage => 0x8001,
            Self::DiagMessagePosAck => 0x8002,
            Self::DiagMessageNegAck => 0x8003,
            Self::Unknown(v) => v,
        }
    }
}

/// DoIP message (ISO 13400-2). Header is 8 bytes.
#[derive(Debug)]
pub struct DoIp {
    pub protocol_version: u8,
    pub payload_type: PayloadType,
    pub payload: Vec<u8>,
}

impl DoIp {
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let protocol_version = read_u8(&mut r)?;
        let inv_version = read_u8(&mut r)?;
        // inverse version check per ISO 13400-2 section 5.3.2
        if protocol_version ^ inv_version != 0xFF {
            return Err(ParseError::InvalidData);
        }
        let payload_type = PayloadType::from_u16(read_u16_be(&mut r)?);
        let payload_length = read_u32_be(&mut r)?;
        let mut payload = vec![0u8; payload_length as usize];
        r.read_exact(&mut payload)?;
        Ok(DoIp {
            protocol_version,
            payload_type,
            payload,
        })
    }

    /// Parse payload as a diagnostic message (payload type 0x8001).
    pub fn parse_diag_message(&self) -> ParseResult<DiagMessage> {
        if self.payload_type != PayloadType::DiagMessage {
            return Err(ParseError::InvalidData);
        }
        if self.payload.len() < 4 {
            return Err(ParseError::InvalidData);
        }
        let src_addr = ((self.payload[0] as u16) << 8) | self.payload[1] as u16;
        let target_addr = ((self.payload[2] as u16) << 8) | self.payload[3] as u16;
        let data = self.payload[4..].to_vec();
        Ok(DiagMessage {
            src_addr,
            target_addr,
            data,
        })
    }
}

/// Diagnostic message payload — carries a UDS request/response.
#[derive(Debug)]
pub struct DiagMessage {
    pub src_addr: u16,
    pub target_addr: u16,
    pub data: Vec<u8>,
}

impl DiagMessage {
    pub fn parse_uds(&self) -> ParseResult<Uds> {
        Uds::parse(&self.data)
    }
}
