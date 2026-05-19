use super::super::error::{ParseError, ParseResult};

const POSITIVE_RESPONSE_MASK: u8 = 0x40;
const NEGATIVE_RESPONSE_SID: u8 = 0x7F;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceId {
    DiagnosticSessionControl,
    EcuReset,
    ClearDiagnosticInformation,
    ReadDtcInformation,
    ReadDataByIdentifier,
    ReadMemoryByAddress,
    ReadScalingDataByIdentifier,
    SecurityAccess,
    CommunicationControl,
    Authentication,
    ReadDataByPeriodicIdentifier,
    DynamicallyDefineDataIdentifier,
    WriteDataByIdentifier,
    InputOutputControlByIdentifier,
    RoutineControl,
    RequestDownload,
    RequestUpload,
    TransferData,
    RequestTransferExit,
    RequestFileTransfer,
    WriteMemoryByAddress,
    TesterPresent,
    AccessTimingParameter,
    SecuredDataTransmission,
    ControlDtcSetting,
    ResponseOnEvent,
    LinkControl,
    Other(u8),
}

impl ServiceId {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x10 => Self::DiagnosticSessionControl,
            0x11 => Self::EcuReset,
            0x14 => Self::ClearDiagnosticInformation,
            0x19 => Self::ReadDtcInformation,
            0x22 => Self::ReadDataByIdentifier,
            0x23 => Self::ReadMemoryByAddress,
            0x24 => Self::ReadScalingDataByIdentifier,
            0x27 => Self::SecurityAccess,
            0x28 => Self::CommunicationControl,
            0x29 => Self::Authentication,
            0x2A => Self::ReadDataByPeriodicIdentifier,
            0x2C => Self::DynamicallyDefineDataIdentifier,
            0x2E => Self::WriteDataByIdentifier,
            0x2F => Self::InputOutputControlByIdentifier,
            0x31 => Self::RoutineControl,
            0x34 => Self::RequestDownload,
            0x35 => Self::RequestUpload,
            0x36 => Self::TransferData,
            0x37 => Self::RequestTransferExit,
            0x38 => Self::RequestFileTransfer,
            0x3D => Self::WriteMemoryByAddress,
            0x3E => Self::TesterPresent,
            0x83 => Self::AccessTimingParameter,
            0x84 => Self::SecuredDataTransmission,
            0x85 => Self::ControlDtcSetting,
            0x86 => Self::ResponseOnEvent,
            0x87 => Self::LinkControl,
            v => Self::Other(v),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::DiagnosticSessionControl => 0x10,
            Self::EcuReset => 0x11,
            Self::ClearDiagnosticInformation => 0x14,
            Self::ReadDtcInformation => 0x19,
            Self::ReadDataByIdentifier => 0x22,
            Self::ReadMemoryByAddress => 0x23,
            Self::ReadScalingDataByIdentifier => 0x24,
            Self::SecurityAccess => 0x27,
            Self::CommunicationControl => 0x28,
            Self::Authentication => 0x29,
            Self::ReadDataByPeriodicIdentifier => 0x2A,
            Self::DynamicallyDefineDataIdentifier => 0x2C,
            Self::WriteDataByIdentifier => 0x2E,
            Self::InputOutputControlByIdentifier => 0x2F,
            Self::RoutineControl => 0x31,
            Self::RequestDownload => 0x34,
            Self::RequestUpload => 0x35,
            Self::TransferData => 0x36,
            Self::RequestTransferExit => 0x37,
            Self::RequestFileTransfer => 0x38,
            Self::WriteMemoryByAddress => 0x3D,
            Self::TesterPresent => 0x3E,
            Self::AccessTimingParameter => 0x83,
            Self::SecuredDataTransmission => 0x84,
            Self::ControlDtcSetting => 0x85,
            Self::ResponseOnEvent => 0x86,
            Self::LinkControl => 0x87,
            Self::Other(v) => v,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nrc {
    GeneralReject,
    ServiceNotSupported,
    SubFunctionNotSupported,
    IncorrectMessageLengthOrInvalidFormat,
    ResponseTooLong,
    BusyRepeatRequest,
    ConditionsNotCorrect,
    RequestSequenceError,
    NoResponseFromSubnetComponent,
    FailurePreventsExecution,
    RequestOutOfRange,
    SecurityAccessDenied,
    InvalidKey,
    ExceededNumberOfAttempts,
    RequiredTimeDelayNotExpired,
    UploadDownloadNotAccepted,
    TransferDataSuspended,
    GeneralProgrammingFailure,
    WrongBlockSequenceCounter,
    ResponsePending,
    SubFunctionNotSupportedInActiveSession,
    ServiceNotSupportedInActiveSession,
    Other(u8),
}

impl Nrc {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x10 => Self::GeneralReject,
            0x11 => Self::ServiceNotSupported,
            0x12 => Self::SubFunctionNotSupported,
            0x13 => Self::IncorrectMessageLengthOrInvalidFormat,
            0x14 => Self::ResponseTooLong,
            0x21 => Self::BusyRepeatRequest,
            0x22 => Self::ConditionsNotCorrect,
            0x24 => Self::RequestSequenceError,
            0x25 => Self::NoResponseFromSubnetComponent,
            0x26 => Self::FailurePreventsExecution,
            0x31 => Self::RequestOutOfRange,
            0x33 => Self::SecurityAccessDenied,
            0x35 => Self::InvalidKey,
            0x36 => Self::ExceededNumberOfAttempts,
            0x37 => Self::RequiredTimeDelayNotExpired,
            0x70 => Self::UploadDownloadNotAccepted,
            0x71 => Self::TransferDataSuspended,
            0x72 => Self::GeneralProgrammingFailure,
            0x73 => Self::WrongBlockSequenceCounter,
            0x78 => Self::ResponsePending,
            0x7E => Self::SubFunctionNotSupportedInActiveSession,
            0x7F => Self::ServiceNotSupportedInActiveSession,
            v => Self::Other(v),
        }
    }
}

/// A parsed UDS message (ISO 14229-1).
#[derive(Debug)]
pub enum Uds {
    Request {
        service: ServiceId,
        data: Vec<u8>,
    },
    PositiveResponse {
        service: ServiceId,
        data: Vec<u8>,
    },
    NegativeResponse {
        service: ServiceId,
        nrc: Nrc,
    },
}

impl Uds {
    pub fn parse(data: &[u8]) -> ParseResult<Self> {
        if data.is_empty() {
            return Err(ParseError::InvalidData);
        }
        let sid = data[0];
        if sid == NEGATIVE_RESPONSE_SID {
            if data.len() < 3 {
                return Err(ParseError::InvalidData);
            }
            return Ok(Uds::NegativeResponse {
                service: ServiceId::from_u8(data[1]),
                nrc: Nrc::from_u8(data[2]),
            });
        }
        if sid & POSITIVE_RESPONSE_MASK != 0 {
            return Ok(Uds::PositiveResponse {
                service: ServiceId::from_u8(sid & !POSITIVE_RESPONSE_MASK),
                data: data[1..].to_vec(),
            });
        }
        Ok(Uds::Request {
            service: ServiceId::from_u8(sid),
            data: data[1..].to_vec(),
        })
    }

    pub fn service(&self) -> ServiceId {
        match self {
            Uds::Request { service, .. } => *service,
            Uds::PositiveResponse { service, .. } => *service,
            Uds::NegativeResponse { service, .. } => *service,
        }
    }

    pub fn is_request(&self) -> bool {
        matches!(self, Uds::Request { .. })
    }

    pub fn is_positive_response(&self) -> bool {
        matches!(self, Uds::PositiveResponse { .. })
    }

    pub fn is_negative_response(&self) -> bool {
        matches!(self, Uds::NegativeResponse { .. })
    }
}
