use super::super::error::{ParseError, ParseResult};
use super::super::read_util::{read_u16_be, read_u32_be, read_u8};
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Request,
    RequestNoReturn,
    Notification,
    RequestAck,
    RequestNoReturnAck,
    NotificationAck,
    Response,
    Error,
    ResponseAck,
    ErrorAck,
    Unknown(u8),
}

impl MessageType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Request,
            0x01 => Self::RequestNoReturn,
            0x02 => Self::Notification,
            0x40 => Self::RequestAck,
            0x41 => Self::RequestNoReturnAck,
            0x42 => Self::NotificationAck,
            0x80 => Self::Response,
            0x81 => Self::Error,
            0xC0 => Self::ResponseAck,
            0xC1 => Self::ErrorAck,
            v => Self::Unknown(v),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::Request => 0x00,
            Self::RequestNoReturn => 0x01,
            Self::Notification => 0x02,
            Self::RequestAck => 0x40,
            Self::RequestNoReturnAck => 0x41,
            Self::NotificationAck => 0x42,
            Self::Response => 0x80,
            Self::Error => 0x81,
            Self::ResponseAck => 0xC0,
            Self::ErrorAck => 0xC1,
            Self::Unknown(v) => v,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnCode {
    Ok,
    NotOk,
    UnknownService,
    UnknownMethod,
    NotReady,
    NotReachable,
    Timeout,
    WrongProtocolVersion,
    WrongInterfaceVersion,
    MalformedMessage,
    WrongMessageType,
    Other(u8),
}

impl ReturnCode {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Ok => 0x00,
            Self::NotOk => 0x01,
            Self::UnknownService => 0x02,
            Self::UnknownMethod => 0x03,
            Self::NotReady => 0x04,
            Self::NotReachable => 0x05,
            Self::Timeout => 0x06,
            Self::WrongProtocolVersion => 0x07,
            Self::WrongInterfaceVersion => 0x08,
            Self::MalformedMessage => 0x09,
            Self::WrongMessageType => 0x0A,
            Self::Other(v) => v,
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Ok,
            0x01 => Self::NotOk,
            0x02 => Self::UnknownService,
            0x03 => Self::UnknownMethod,
            0x04 => Self::NotReady,
            0x05 => Self::NotReachable,
            0x06 => Self::Timeout,
            0x07 => Self::WrongProtocolVersion,
            0x08 => Self::WrongInterfaceVersion,
            0x09 => Self::MalformedMessage,
            0x0A => Self::WrongMessageType,
            v => Self::Other(v),
        }
    }
}

/// SOME/IP message (AUTOSAR PRS_SOMEIP_00052).
/// Header is always 16 bytes; payload follows.
#[derive(Debug)]
pub struct SomeIp {
    pub service_id: u16,
    pub method_id: u16,
    pub client_id: u16,
    pub session_id: u16,
    pub protocol_version: u8,
    pub interface_version: u8,
    pub message_type: MessageType,
    pub return_code: ReturnCode,
    pub payload: Vec<u8>,
}

impl SomeIp {
    pub fn parse<R: Read>(mut r: R) -> ParseResult<Self> {
        let service_id = read_u16_be(&mut r)?;
        let method_id = read_u16_be(&mut r)?;
        let length = read_u32_be(&mut r)?; // remaining bytes after this field
        if length < 8 {
            return Err(ParseError::InvalidData);
        }
        let client_id = read_u16_be(&mut r)?;
        let session_id = read_u16_be(&mut r)?;
        let protocol_version = read_u8(&mut r)?;
        let interface_version = read_u8(&mut r)?;
        let message_type = MessageType::from_u8(read_u8(&mut r)?);
        let return_code = ReturnCode::from_u8(read_u8(&mut r)?);
        let payload_len = (length - 8) as usize;
        let mut payload = vec![0u8; payload_len];
        r.read_exact(&mut payload)?;
        Ok(SomeIp {
            service_id,
            method_id,
            client_id,
            session_id,
            protocol_version,
            interface_version,
            message_type,
            return_code,
            payload,
        })
    }

    /// True for SOME/IP-SD messages (service 0xFFFF, method 0x8100).
    pub fn is_sd(&self) -> bool {
        self.service_id == 0xFFFF && self.method_id == 0x8100
    }
}
