use vector_blf::blf::{FlowStatus, IsoTpFrame, ParseError, Reassembler, ServiceId};

// ── SingleFrame ────────────────────────────────────────────────────────────────

#[test]
fn parse_single_frame_normal() {
    // SF with length 3, payload = [0x01, 0x02, 0x03]
    let raw = [0x03, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::SingleFrame { data } => assert_eq!(data, vec![0x01, 0x02, 0x03]),
        _ => panic!("expected SingleFrame"),
    }
}

#[test]
fn parse_single_frame_exact_length() {
    // SF with length 7 (max classic CAN)
    let raw = [0x07, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::SingleFrame { data } => {
            assert_eq!(data.len(), 7);
            assert_eq!(data, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
        }
        _ => panic!("expected SingleFrame"),
    }
}

#[test]
fn parse_single_frame_extended_canfd() {
    // Extended SF for CAN-FD: first byte = 0x00, second byte = actual length
    let mut raw = vec![0x00, 0x08];
    raw.extend_from_slice(&[0xAA; 8]);
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::SingleFrame { data } => {
            assert_eq!(data.len(), 8);
            assert!(data.iter().all(|&b| b == 0xAA));
        }
        _ => panic!("expected SingleFrame"),
    }
}

#[test]
fn parse_single_frame_empty_returns_error() {
    assert!(matches!(
        IsoTpFrame::parse(&[]),
        Err(ParseError::InvalidData)
    ));
}

#[test]
fn parse_single_frame_truncated_returns_error() {
    // Length says 5 but only 3 bytes of payload
    let raw = [0x05, 0x01, 0x02, 0x03];
    assert!(matches!(
        IsoTpFrame::parse(&raw),
        Err(ParseError::InvalidData)
    ));
}

#[test]
fn parse_single_frame_extended_too_short_returns_error() {
    // Extended SF (0x00) but only 1 byte total
    let raw = [0x00];
    assert!(matches!(
        IsoTpFrame::parse(&raw),
        Err(ParseError::InvalidData)
    ));
}

// ── FirstFrame ─────────────────────────────────────────────────────────────────

#[test]
fn parse_first_frame_normal() {
    // FF: total_length = 0x00A = 10, first payload bytes
    let raw = [0x10, 0x0A, 0x22, 0xF1, 0x90, 0x01, 0x02, 0x03];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FirstFrame { total_length, data } => {
            assert_eq!(total_length, 10);
            assert_eq!(data, vec![0x22, 0xF1, 0x90, 0x01, 0x02, 0x03]);
        }
        _ => panic!("expected FirstFrame"),
    }
}

#[test]
fn parse_first_frame_extended_canfd() {
    // Extended FF: 12-bit length == 0, actual length in next 4 bytes
    // [0x10, 0x00, 0x00, 0x00, 0x01, 0x00, payload...]
    let mut raw = vec![0x10, 0x00, 0x00, 0x00, 0x01, 0x00];
    raw.extend_from_slice(&[0xBB; 12]);
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FirstFrame { total_length, data } => {
            assert_eq!(total_length, 256);
            assert_eq!(data.len(), 12);
        }
        _ => panic!("expected FirstFrame"),
    }
}

#[test]
fn parse_first_frame_too_short_returns_error() {
    let raw = [0x10]; // Only 1 byte
    assert!(matches!(
        IsoTpFrame::parse(&raw),
        Err(ParseError::InvalidData)
    ));
}

#[test]
fn parse_first_frame_extended_too_short_returns_error() {
    let raw = [0x10, 0x00, 0x00, 0x00]; // Extended but only 4 bytes total
    assert!(matches!(
        IsoTpFrame::parse(&raw),
        Err(ParseError::InvalidData)
    ));
}

// ── ConsecutiveFrame ──────────────────────────────────────────────────────────

#[test]
fn parse_consecutive_frame() {
    let raw = [0x21, 0x04, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::ConsecutiveFrame {
            sequence_number,
            data,
        } => {
            assert_eq!(sequence_number, 1);
            assert_eq!(data[0], 0x04);
            assert_eq!(data[1], 0x05);
        }
        _ => panic!("expected ConsecutiveFrame"),
    }
}

#[test]
fn parse_consecutive_frame_sn_wrap() {
    // SN nibble wraps 0–F
    let raw = [0x2F, 0xAA];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::ConsecutiveFrame {
            sequence_number, ..
        } => assert_eq!(sequence_number, 0x0F),
        _ => panic!("expected ConsecutiveFrame"),
    }
}

// ── FlowControl ───────────────────────────────────────────────────────────────

#[test]
fn parse_flow_control_cts() {
    let raw = [0x30, 0x00, 0x00]; // CTS, BS=0, ST=0
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FlowControl {
            flow_status,
            block_size,
            min_separation_time,
        } => {
            assert_eq!(flow_status, FlowStatus::ContinueToSend);
            assert_eq!(block_size, 0);
            assert_eq!(min_separation_time, 0);
        }
        _ => panic!("expected FlowControl"),
    }
}

#[test]
fn parse_flow_control_wait() {
    let raw = [0x31, 0x00, 0x00];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FlowControl { flow_status, .. } => {
            assert_eq!(flow_status, FlowStatus::Wait);
        }
        _ => panic!("expected FlowControl"),
    }
}

#[test]
fn parse_flow_control_overflow() {
    let raw = [0x32, 0x00, 0x00];
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FlowControl { flow_status, .. } => {
            assert_eq!(flow_status, FlowStatus::Overflow);
        }
        _ => panic!("expected FlowControl"),
    }
}

#[test]
fn parse_flow_control_with_params() {
    let raw = [0x30, 0x0A, 0x19]; // CTS, BS=10, ST=25ms
    match IsoTpFrame::parse(&raw).unwrap() {
        IsoTpFrame::FlowControl {
            block_size,
            min_separation_time,
            ..
        } => {
            assert_eq!(block_size, 10);
            assert_eq!(min_separation_time, 25);
        }
        _ => panic!("expected FlowControl"),
    }
}

#[test]
fn parse_flow_control_too_short_returns_error() {
    let raw = [0x30, 0x00]; // Missing ST byte
    assert!(matches!(
        IsoTpFrame::parse(&raw),
        Err(ParseError::InvalidData)
    ));
}

// ── IsoTpFrame::parse_uds ─────────────────────────────────────────────────────

#[test]
fn single_frame_parse_uds() {
    // SF with UDS TesterPresent request [0x3E, 0x00]
    let raw = [0x02, 0x3E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let frame = IsoTpFrame::parse(&raw).unwrap();
    let uds = frame.parse_uds().unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::TesterPresent);
}

#[test]
fn non_single_frame_parse_uds_returns_error() {
    let ff_frame = IsoTpFrame::parse(&[0x10, 0x08, 0x22, 0xF1, 0x90, 0x01, 0x02, 0x03]).unwrap();
    assert!(matches!(ff_frame.parse_uds(), Err(ParseError::InvalidData)));

    let cf_frame = IsoTpFrame::parse(&[0x21, 0x04, 0x05]).unwrap();
    assert!(matches!(cf_frame.parse_uds(), Err(ParseError::InvalidData)));

    let fc_frame = IsoTpFrame::parse(&[0x30, 0x00, 0x00]).unwrap();
    assert!(matches!(fc_frame.parse_uds(), Err(ParseError::InvalidData)));
}

// ── Reassembler ───────────────────────────────────────────────────────────────

#[test]
fn reassembler_single_frame() {
    let mut r = Reassembler::new();
    let sf = IsoTpFrame::parse(&[0x02, 0x3E, 0x00]).unwrap();
    let result = r.push(&sf).unwrap();
    assert!(result.is_some());
    let uds = result.unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::TesterPresent);
}

#[test]
fn reassembler_multiframe_two_segments() {
    // 8-byte UDS payload: [0x22, 0xF1, 0x90, 0x01, 0x02, 0x03, 0x04, 0x05]
    // FF carries first 6 bytes
    let ff = IsoTpFrame::parse(&[0x10, 0x08, 0x22, 0xF1, 0x90, 0x01, 0x02, 0x03]).unwrap();
    // CF SN=1 carries last 2 bytes (padded to 8)
    let cf = IsoTpFrame::parse(&[0x21, 0x04, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap();

    let mut r = Reassembler::new();
    assert!(r.push(&ff).unwrap().is_none());
    let result = r.push(&cf).unwrap();
    assert!(result.is_some());

    let uds = result.unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::ReadDataByIdentifier);
}

#[test]
fn reassembler_multiframe_three_segments() {
    // 20-byte payload: [0x22] + [0xF1, 0x90] + 17 data bytes
    // FF carries first 6 bytes
    let ff_payload: Vec<u8> = {
        let mut v = vec![0x10, 0x14]; // total_length = 20
        v.extend_from_slice(&[0x22, 0xF1, 0x90, 0x01, 0x02, 0x03]); // 6 bytes
        v
    };
    // CF1 carries next 7 bytes
    let cf1_payload: Vec<u8> = {
        let mut v = vec![0x21];
        v.extend_from_slice(&[0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]);
        v
    };
    // CF2 carries final 7 bytes
    let cf2_payload: Vec<u8> = {
        let mut v = vec![0x22];
        v.extend_from_slice(&[0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11]);
        v
    };

    let ff = IsoTpFrame::parse(&ff_payload).unwrap();
    let cf1 = IsoTpFrame::parse(&cf1_payload).unwrap();
    let cf2 = IsoTpFrame::parse(&cf2_payload).unwrap();

    let mut r = Reassembler::new();
    assert!(r.push(&ff).unwrap().is_none());
    assert!(r.push(&cf1).unwrap().is_none());
    let result = r.push(&cf2).unwrap();
    assert!(result.is_some());
    let uds = result.unwrap();
    assert!(uds.is_request());
    assert_eq!(uds.service(), ServiceId::ReadDataByIdentifier);
}

#[test]
fn reassembler_flow_control_is_ignored() {
    let fc = IsoTpFrame::parse(&[0x30, 0x00, 0x00]).unwrap();
    let mut r = Reassembler::new();
    assert!(r.push(&fc).unwrap().is_none());
}

#[test]
fn reassembler_consecutive_without_first_returns_error() {
    let cf = IsoTpFrame::parse(&[0x21, 0x01, 0x02]).unwrap();
    let mut r = Reassembler::new();
    assert!(matches!(r.push(&cf), Err(ParseError::InvalidData)));
}

#[test]
fn reassembler_out_of_sequence_returns_error() {
    let ff = IsoTpFrame::parse(&[0x10, 0x08, 0x22, 0xF1, 0x90, 0x01, 0x02, 0x03]).unwrap();
    let cf_wrong_sn = IsoTpFrame::parse(&[0x22, 0x04, 0x05]).unwrap(); // SN=2 but expected 1

    let mut r = Reassembler::new();
    r.push(&ff).unwrap();
    assert!(matches!(r.push(&cf_wrong_sn), Err(ParseError::InvalidData)));
}

#[test]
fn reassembler_resets_after_complete() {
    let mut r = Reassembler::new();

    // First complete message
    let sf = IsoTpFrame::parse(&[0x02, 0x3E, 0x00]).unwrap();
    r.push(&sf).unwrap().unwrap();

    // Second complete message — reassembler should be clean
    let sf2 = IsoTpFrame::parse(&[0x02, 0x11, 0x01]).unwrap();
    let result = r.push(&sf2).unwrap();
    assert!(result.is_some());
    let uds = result.unwrap();
    assert_eq!(uds.service(), ServiceId::EcuReset);
}
