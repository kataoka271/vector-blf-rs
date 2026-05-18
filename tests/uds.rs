use vector_blf_rs::blf::{Nrc, ParseError, ServiceId, Uds};

#[test]
fn parse_request() {
    let data = [0x10, 0x01]; // DiagnosticSessionControl, defaultSession
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::DiagnosticSessionControl);
    match uds {
        Uds::Request { service, data } => {
            assert_eq!(service, ServiceId::DiagnosticSessionControl);
            assert_eq!(data, vec![0x01]);
        }
        _ => panic!("expected Request"),
    }
}

#[test]
fn parse_positive_response() {
    let data = [0x50, 0x01]; // 0x10 | 0x40 = 0x50
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_positive_response());
    assert_eq!(uds.service(), ServiceId::DiagnosticSessionControl);
    match uds {
        Uds::PositiveResponse { service, data } => {
            assert_eq!(service, ServiceId::DiagnosticSessionControl);
            assert_eq!(data, vec![0x01]);
        }
        _ => panic!("expected PositiveResponse"),
    }
}

#[test]
fn parse_negative_response() {
    let data = [0x7F, 0x22, 0x31]; // NRC: RequestOutOfRange for ReadDataByIdentifier
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_negative_response());
    assert_eq!(uds.service(), ServiceId::ReadDataByIdentifier);
    match uds {
        Uds::NegativeResponse { service, nrc } => {
            assert_eq!(service, ServiceId::ReadDataByIdentifier);
            assert_eq!(nrc, Nrc::RequestOutOfRange);
        }
        _ => panic!("expected NegativeResponse"),
    }
}

#[test]
fn parse_read_data_by_identifier_request() {
    let data = [0x22, 0xF1, 0x90]; // RDBI, DID 0xF190
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::ReadDataByIdentifier);
}

#[test]
fn parse_ecu_reset_request() {
    let data = [0x11, 0x01]; // EcuReset, hardReset
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::EcuReset);
}

#[test]
fn parse_tester_present_request() {
    let data = [0x3E, 0x00]; // TesterPresent, zeroSubFunction
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::TesterPresent);
}

#[test]
fn parse_security_access_request() {
    let data = [0x27, 0x01]; // SecurityAccess, requestSeed
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::SecurityAccess);
}

#[test]
fn parse_transfer_data_request() {
    let data = [0x36, 0x01, 0xDE, 0xAD, 0xBE, 0xEF];
    let uds = Uds::parse(&data).unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::TransferData);
    match uds {
        Uds::Request { data, .. } => assert_eq!(data, vec![0x01, 0xDE, 0xAD, 0xBE, 0xEF]),
        _ => panic!(),
    }
}

#[test]
fn parse_empty_returns_error() {
    assert!(matches!(Uds::parse(&[]), Err(ParseError::InvalidData)));
}

#[test]
fn parse_negative_response_too_short_returns_error() {
    assert!(matches!(Uds::parse(&[0x7F, 0x22]), Err(ParseError::InvalidData)));
}

#[test]
fn service_id_roundtrip() {
    let services = [
        ServiceId::DiagnosticSessionControl,
        ServiceId::EcuReset,
        ServiceId::ReadDataByIdentifier,
        ServiceId::WriteDataByIdentifier,
        ServiceId::SecurityAccess,
        ServiceId::TesterPresent,
        ServiceId::RoutineControl,
        ServiceId::RequestDownload,
        ServiceId::TransferData,
        ServiceId::RequestTransferExit,
    ];
    for svc in services {
        assert_eq!(ServiceId::from_u8(svc.to_u8()), svc);
    }
}

#[test]
fn service_id_other_roundtrip() {
    let other = ServiceId::Other(0xFF);
    assert_eq!(other.to_u8(), 0xFF);
    assert!(matches!(ServiceId::from_u8(0xFF), ServiceId::Other(0xFF)));
}

#[test]
fn nrc_known_values() {
    assert_eq!(Nrc::from_u8(0x10), Nrc::GeneralReject);
    assert_eq!(Nrc::from_u8(0x11), Nrc::ServiceNotSupported);
    assert_eq!(Nrc::from_u8(0x22), Nrc::ConditionsNotCorrect);
    assert_eq!(Nrc::from_u8(0x31), Nrc::RequestOutOfRange);
    assert_eq!(Nrc::from_u8(0x33), Nrc::SecurityAccessDenied);
    assert_eq!(Nrc::from_u8(0x78), Nrc::ResponsePending);
    assert_eq!(Nrc::from_u8(0x7E), Nrc::SubFunctionNotSupportedInActiveSession);
    assert_eq!(Nrc::from_u8(0x7F), Nrc::ServiceNotSupportedInActiveSession);
}

#[test]
fn nrc_unknown_value() {
    assert!(matches!(Nrc::from_u8(0xAB), Nrc::Other(0xAB)));
}
