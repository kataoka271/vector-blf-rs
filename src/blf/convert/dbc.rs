use crate::blf::error::ParseResult;
use crate::blf::signal::{ByteOrder, CanSignalDb, Signal, SignalDef};
use std::io::BufRead;

/// Parse a Vector DBC file into a [`CanSignalDb`].
///
/// - Extended-frame IDs (bit 31 set) are stripped to the 29-bit value.
/// - Multiplexed signals are included; the mux indicator (`M`/`m<id>`) is ignored.
/// - Lines that cannot be parsed are silently skipped.
pub fn can_db_from_dbc<R: std::io::Read>(reader: R) -> ParseResult<CanSignalDb> {
    let mut db = CanSignalDb::new();
    let mut current_id: Option<u32> = None;

    for line in std::io::BufReader::new(reader).lines() {
        let line = line?;
        let line = line.trim();

        if let Some(rest) = line.strip_prefix("BO_ ") {
            // BO_ <id> <name>: <dlc> <tx>
            // Extended IDs have bit 31 set; mask it off to get the 29-bit CAN ID.
            let id_str = rest.split_whitespace().next().unwrap_or("");
            current_id = id_str.parse::<u32>().ok().map(|id| id & 0x1FFF_FFFF);
        } else if let Some(rest) = line.strip_prefix("SG_ ") {
            if let Some(id) = current_id {
                if let Some(def) = parse_signal_line(rest, id) {
                    db.insert(def);
                }
            }
        } else if !line.is_empty()
            && !line.starts_with(' ')
            && !line.starts_with('\t')
            && !line.starts_with("BO_")
        {
            current_id = None;
        }
    }

    Ok(db)
}

/// Parse a single `SG_` line (without the `SG_ ` prefix) and return a `SignalDef`.
///
/// DBC signal format:
/// ```text
/// <name> [M|m<id>|m<id>M] : <start>|<len>@<order><sign> (<scale>,<offset>) [<min>|<max>] "<unit>" <rx>
/// ```
/// `@1` = Intel (little-endian), `@0` = Motorola (big-endian), `+` = unsigned, `-` = signed.
fn parse_signal_line(rest: &str, message_id: u32) -> Option<SignalDef> {
    // Everything up to " : " is name + optional mux indicator.
    let colon = rest.find(" : ")?;
    let name = rest[..colon].split_whitespace().next()?.to_string();
    let rest = rest[colon + 3..].trim();

    // First whitespace-separated token is the bit-layout descriptor.
    let (bit_token, rest) = rest.split_once(' ')?;
    let at = bit_token.find('@')?;
    let pipe = bit_token[..at].find('|')?;
    let start_bit = bit_token[..pipe].parse::<u32>().ok()?;
    let bit_length = bit_token[pipe + 1..at].parse::<u32>().ok()?;
    let order_sign = &bit_token[at + 1..];
    let byte_order = match order_sign.chars().next()? {
        '1' => ByteOrder::Intel,
        '0' => ByteOrder::Motorola,
        _ => return None,
    };
    let is_signed = order_sign.ends_with('-');

    // Scale and offset are in parentheses: (<scale>,<offset>)
    let rest = rest.trim();
    let paren_open = rest.find('(')?;
    let paren_close = rest.find(')')?;
    let inner = &rest[paren_open + 1..paren_close];
    let (scale_str, offset_str) = inner.split_once(',')?;
    let scale = scale_str.trim().parse::<f64>().ok()?;
    let offset = offset_str.trim().parse::<f64>().ok()?;

    Some(SignalDef {
        name,
        message_id,
        signal: Signal {
            start_bit,
            bit_length,
            byte_order,
            is_signed,
            scale,
            offset,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_DBC: &str = r#"
VERSION ""

NS_ :

BS_:

BU_: ECU1 ECU2

BO_ 256 EngineControl: 8 ECU1
 SG_ EngineSpeed : 0|16@1+ (0.25,0) [0|16383.75] "rpm" ECU2
 SG_ ThrottlePos : 16|8@1+ (0.4,0) [0|100] "%" ECU2
 SG_ BrakeForce : 24|12@0- (0.1,-100) [-100|300] "N" ECU2

BO_ 1024 BrakeSystem: 8 ECU1
 SG_ BrakePressure : 0|16@1+ (0.01,0) [0|655.35] "bar" ECU2

BO_ 2147484160 ExtFrame: 8 ECU1
 SG_ ExtSig : 0|8@1+ (1,0) [0|255] "" ECU2
"#;

    #[test]
    fn parse_can_signals() {
        let db = can_db_from_dbc(SAMPLE_DBC.as_bytes()).unwrap();
        assert_eq!(db.signals(256).len(), 3);
        assert_eq!(db.signals(1024).len(), 1);

        let speed = db
            .signals(256)
            .iter()
            .find(|s| s.name == "EngineSpeed")
            .unwrap();
        assert_eq!(speed.signal.start_bit, 0);
        assert_eq!(speed.signal.bit_length, 16);
        assert_eq!(speed.signal.byte_order, ByteOrder::Intel);
        assert!(!speed.signal.is_signed);
        assert_eq!(speed.signal.scale, 0.25);
        assert_eq!(speed.signal.offset, 0.0);

        let brake = db
            .signals(256)
            .iter()
            .find(|s| s.name == "BrakeForce")
            .unwrap();
        assert_eq!(brake.signal.byte_order, ByteOrder::Motorola);
        assert!(brake.signal.is_signed);
        assert_eq!(brake.signal.offset, -100.0);
    }

    #[test]
    fn extended_id_stripped() {
        // 2147484160 = 0x80000200 → stripped to 0x200 = 512
        let db = can_db_from_dbc(SAMPLE_DBC.as_bytes()).unwrap();
        assert_eq!(db.signals(512).len(), 1); // only ExtFrame maps here
    }

    #[test]
    fn multiplexed_signal_included() {
        let dbc = "BO_ 100 Mux: 8 ECU\n SG_ MuxSig M : 0|4@1+ (1,0) [0|15] \"\" ECU\n SG_ SigA m0 : 4|8@1+ (1,0) [0|255] \"\" ECU\n";
        let db = can_db_from_dbc(dbc.as_bytes()).unwrap();
        assert_eq!(db.signals(100).len(), 2);
    }
}
