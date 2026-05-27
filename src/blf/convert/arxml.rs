use crate::blf::error::{ParseError, ParseResult};
use crate::blf::signal::{ByteOrder, CanSignalDb, Signal, SignalDef};
use quick_xml::{events::Event, Reader};
use std::collections::HashMap;
use std::io::BufRead;

// ── intermediate data structures ──────────────────────────────────────────────

#[derive(Default)]
struct ISignalData {
    name: String,
    bit_length: u32,
    compu_ref: String,
    is_signed: bool,
}

struct MappingData {
    signal_ref: String,
    start_bit: u32,
    byte_order: ByteOrder,
}

impl MappingData {
    fn new() -> Self {
        Self {
            signal_ref: String::new(),
            start_bit: 0,
            byte_order: ByteOrder::Intel, // AUTOSAR default: MOST-SIGNIFICANT-BYTE-LAST
        }
    }
}

#[derive(Default)]
struct IPduData {
    name: String,
    mappings: Vec<MappingData>,
}

#[derive(Default)]
struct FrameData {
    name: String,
    pdu_refs: Vec<String>,
}

#[derive(Default)]
struct TriggeringData {
    frame_ref: String,
    can_id: u32,
}

#[derive(Default)]
struct CompuMethodData {
    name: String,
    scale: f64,
    offset: f64,
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Extract the last `/`-delimited segment from an ARXML cross-reference path.
fn last_seg(arxml_ref: &str) -> &str {
    arxml_ref.rsplit('/').next().unwrap_or(arxml_ref).trim()
}

// ── streaming parser ──────────────────────────────────────────────────────────

/// Parse an AUTOSAR 4.x ARXML file (streaming) into a [`CanSignalDb`].
///
/// Handles the standard CAN signal resolution chain:
/// ```text
/// CAN-FRAME-TRIGGERING → FRAME → I-SIGNAL-I-PDU → I-SIGNAL → COMPU-METHOD
/// ```
///
/// Cross-references are matched by the short-name (last `/`-delimited segment of
/// the ARXML path). If two elements share a short-name, the last one encountered
/// wins — use the overlay CSV to resolve such ambiguities.
///
/// Unresolved references produce a warning on stderr and are skipped; the returned
/// `CanSignalDb` contains all signals that could be fully resolved.
///
/// Signedness defaults to `false` (unsigned). Use the overlay CSV to set
/// `is_signed = true` for signals whose data type is not extractable.
pub fn can_db_from_arxml<R: BufRead>(reader: R) -> ParseResult<CanSignalDb> {
    let mut xml = Reader::from_reader(reader);

    // Collected elements
    let mut isignals: HashMap<String, ISignalData> = HashMap::new();
    let mut ipdus: HashMap<String, IPduData> = HashMap::new();
    let mut frames: HashMap<String, FrameData> = HashMap::new();
    let mut triggerings: Vec<TriggeringData> = Vec::new();
    let mut compu_methods: HashMap<String, CompuMethodData> = HashMap::new();

    // Currently building
    let mut cur_isignal: Option<ISignalData> = None;
    let mut cur_ipdu: Option<IPduData> = None;
    let mut cur_frame: Option<FrameData> = None;
    let mut cur_triggering: Option<TriggeringData> = None;
    let mut cur_compu: Option<CompuMethodData> = None;
    let mut cur_mapping: Option<MappingData> = None;

    // COMPU-NUMERATOR coefficient accumulation
    let mut compu_num_vals: Vec<f64> = Vec::new();
    let mut in_compu_num = false;

    // Current element path (local names only, for parent-context lookups)
    let mut path: Vec<String> = Vec::new();
    // Text content of the element currently being read
    let mut text = String::new();

    let mut buf = Vec::new();

    loop {
        buf.clear();
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = tag_str(e.name().local_name().as_ref());
                text.clear();

                match tag.as_str() {
                    "I-SIGNAL" => cur_isignal = Some(ISignalData::default()),
                    "I-SIGNAL-I-PDU" => cur_ipdu = Some(IPduData::default()),
                    // Both generic FRAME and CAN-specific CAN-FRAME are used in practice.
                    "CAN-FRAME" | "FRAME" => cur_frame = Some(FrameData::default()),
                    "CAN-FRAME-TRIGGERING" => cur_triggering = Some(TriggeringData::default()),
                    "COMPU-METHOD" => cur_compu = Some(CompuMethodData::default()),
                    "I-SIGNAL-TO-I-PDU-MAPPING" => cur_mapping = Some(MappingData::new()),
                    "COMPU-NUMERATOR" => {
                        in_compu_num = true;
                        compu_num_vals.clear();
                    }
                    _ => {}
                }

                path.push(tag);
            }

            Ok(Event::End(e)) => {
                let tag = tag_str(e.name().local_name().as_ref());
                path.pop();
                let txt = text.trim().to_owned();
                // After pop, path.last() is the parent of the element that just closed.
                let parent = path.last().map(String::as_str).unwrap_or("");

                match tag.as_str() {
                    // ── finalise top-level containers ────────────────────────
                    "I-SIGNAL" => {
                        if let Some(s) = cur_isignal.take() {
                            if !s.name.is_empty() {
                                isignals.insert(s.name.clone(), s);
                            }
                        }
                    }
                    "I-SIGNAL-I-PDU" => {
                        if let Some(p) = cur_ipdu.take() {
                            if !p.name.is_empty() {
                                ipdus.insert(p.name.clone(), p);
                            }
                        }
                    }
                    "CAN-FRAME" | "FRAME" => {
                        if let Some(f) = cur_frame.take() {
                            if !f.name.is_empty() {
                                frames.insert(f.name.clone(), f);
                            }
                        }
                    }
                    "CAN-FRAME-TRIGGERING" => {
                        if let Some(t) = cur_triggering.take() {
                            if !t.frame_ref.is_empty() {
                                triggerings.push(t);
                            }
                        }
                    }
                    "COMPU-METHOD" => {
                        if let Some(cm) = cur_compu.take() {
                            if !cm.name.is_empty() {
                                compu_methods.insert(cm.name.clone(), cm);
                            }
                        }
                    }
                    "I-SIGNAL-TO-I-PDU-MAPPING" => {
                        if let Some(m) = cur_mapping.take() {
                            if !m.signal_ref.is_empty() {
                                if let Some(ref mut pdu) = cur_ipdu {
                                    pdu.mappings.push(m);
                                }
                            }
                        }
                    }

                    // ── COMPU-RATIONAL-COEFFS numerator ──────────────────────
                    // AUTOSAR: phys = (a0 + a1*raw) / (b0)
                    // Linear CAN: numerator = [offset, scale], denominator ≈ 1.
                    "COMPU-NUMERATOR" => {
                        in_compu_num = false;
                        if let Some(ref mut cm) = cur_compu {
                            match compu_num_vals.len() {
                                0 => {}
                                1 => {
                                    cm.scale = compu_num_vals[0];
                                    cm.offset = 0.0;
                                }
                                _ => {
                                    cm.offset = compu_num_vals[0];
                                    cm.scale = compu_num_vals[1];
                                }
                            }
                        }
                    }

                    // ── leaf elements — route by parent element ───────────────
                    "SHORT-NAME" => match parent {
                        "I-SIGNAL" => {
                            if let Some(ref mut s) = cur_isignal {
                                s.name = txt;
                            }
                        }
                        "I-SIGNAL-I-PDU" => {
                            if let Some(ref mut p) = cur_ipdu {
                                p.name = txt;
                            }
                        }
                        "CAN-FRAME" | "FRAME" => {
                            if let Some(ref mut f) = cur_frame {
                                f.name = txt;
                            }
                        }
                        "COMPU-METHOD" => {
                            if let Some(ref mut cm) = cur_compu {
                                cm.name = txt;
                            }
                        }
                        _ => {}
                    },

                    // LENGTH appears in both I-SIGNAL (bit count) and CAN-FRAME
                    // (byte count). Guard with cur_isignal + absence of cur_mapping.
                    "LENGTH" if cur_isignal.is_some() && cur_mapping.is_none() => {
                        if let (Some(ref mut s), Ok(n)) = (cur_isignal.as_mut(), txt.parse::<u32>())
                        {
                            s.bit_length = n;
                        }
                    }

                    "START-POSITION" if cur_mapping.is_some() => {
                        if let (Some(ref mut m), Ok(n)) = (cur_mapping.as_mut(), txt.parse::<u32>())
                        {
                            m.start_bit = n;
                        }
                    }

                    "PACKING-BYTE-ORDER" if cur_mapping.is_some() => {
                        if let Some(ref mut m) = cur_mapping {
                            m.byte_order = if txt == "MOST-SIGNIFICANT-BYTE-LAST" {
                                ByteOrder::Intel
                            } else {
                                ByteOrder::Motorola // MOST-SIGNIFICANT-BYTE-FIRST
                            };
                        }
                    }

                    "IDENTIFIER" if cur_triggering.is_some() => {
                        if let (Some(ref mut t), Ok(id)) =
                            (cur_triggering.as_mut(), txt.parse::<u32>())
                        {
                            t.can_id = id;
                        }
                    }

                    "I-SIGNAL-REF" if cur_mapping.is_some() => {
                        if let Some(ref mut m) = cur_mapping {
                            m.signal_ref = last_seg(&txt).to_owned();
                        }
                    }

                    "FRAME-REF" if cur_triggering.is_some() => {
                        if let Some(ref mut t) = cur_triggering {
                            t.frame_ref = last_seg(&txt).to_owned();
                        }
                    }

                    "PDU-REF" if cur_frame.is_some() => {
                        if let Some(ref mut f) = cur_frame {
                            f.pdu_refs.push(last_seg(&txt).to_owned());
                        }
                    }

                    "COMPU-METHOD-REF" if cur_isignal.is_some() => {
                        if let Some(ref mut s) = cur_isignal {
                            s.compu_ref = last_seg(&txt).to_owned();
                        }
                    }

                    // V elements appear as coefficient values inside COMPU-NUMERATOR.
                    "V" if in_compu_num => {
                        if let Ok(v) = txt.parse::<f64>() {
                            compu_num_vals.push(v);
                        }
                    }

                    _ => {}
                }
            }

            Ok(Event::Text(e)) => {
                if let Ok(s) = e.unescape() {
                    text.push_str(&s);
                }
            }

            Ok(Event::Eof) => break,

            Err(e) => {
                return Err(ParseError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                )));
            }

            _ => {}
        }
    }

    // ── resolve cross-references ──────────────────────────────────────────────

    let mut db = CanSignalDb::new();
    let mut unresolved = 0usize;

    for trig in &triggerings {
        let Some(frame) = frames.get(&trig.frame_ref) else {
            eprintln!("[arxml] unresolved FRAME-REF: {}", trig.frame_ref);
            unresolved += 1;
            continue;
        };
        for pdu_ref in &frame.pdu_refs {
            let Some(pdu) = ipdus.get(pdu_ref) else {
                eprintln!("[arxml] unresolved PDU-REF: {pdu_ref}");
                unresolved += 1;
                continue;
            };
            for mapping in &pdu.mappings {
                let Some(signal) = isignals.get(&mapping.signal_ref) else {
                    eprintln!("[arxml] unresolved I-SIGNAL-REF: {}", mapping.signal_ref);
                    unresolved += 1;
                    continue;
                };
                let (scale, offset) = if signal.compu_ref.is_empty() {
                    (1.0, 0.0)
                } else {
                    match compu_methods.get(&signal.compu_ref) {
                        Some(cm) => (cm.scale, cm.offset),
                        None => {
                            eprintln!("[arxml] unresolved COMPU-METHOD-REF: {}", signal.compu_ref);
                            unresolved += 1;
                            (1.0, 0.0)
                        }
                    }
                };
                db.insert(SignalDef {
                    name: signal.name.clone(),
                    message_id: trig.can_id,
                    signal: Signal {
                        start_bit: mapping.start_bit,
                        bit_length: signal.bit_length,
                        byte_order: mapping.byte_order,
                        is_signed: signal.is_signed,
                        scale,
                        offset,
                    },
                });
            }
        }
    }

    if unresolved > 0 {
        eprintln!(
            "[arxml] {unresolved} unresolved reference(s) — \
             use --overlay to supply missing or corrected definitions"
        );
    }

    Ok(db)
}

fn tag_str(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes).unwrap_or("").to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal AUTOSAR 4.x ARXML with one CAN frame, one PDU, one signal, one COMPU-METHOD.
    const SAMPLE_ARXML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>Signals</SHORT-NAME>
      <ELEMENTS>
        <I-SIGNAL>
          <SHORT-NAME>EngineSpeed_ISignal</SHORT-NAME>
          <LENGTH>16</LENGTH>
          <NETWORK-REPRESENTATION-PROPS>
            <SW-DATA-DEF-PROPS-VARIANTS>
              <SW-DATA-DEF-PROPS-CONDITIONAL>
                <COMPU-METHOD-REF>/CompuMethods/EngineSpeed_Compu</COMPU-METHOD-REF>
              </SW-DATA-DEF-PROPS-CONDITIONAL>
            </SW-DATA-DEF-PROPS-VARIANTS>
          </NETWORK-REPRESENTATION-PROPS>
        </I-SIGNAL>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Pdus</SHORT-NAME>
      <ELEMENTS>
        <I-SIGNAL-I-PDU>
          <SHORT-NAME>EngineControl_Pdu</SHORT-NAME>
          <I-SIGNAL-TO-PDU-MAPPINGS>
            <I-SIGNAL-TO-I-PDU-MAPPING>
              <SHORT-NAME>EngineSpeed_Map</SHORT-NAME>
              <I-SIGNAL-REF>/Signals/EngineSpeed_ISignal</I-SIGNAL-REF>
              <PACKING-BYTE-ORDER>MOST-SIGNIFICANT-BYTE-LAST</PACKING-BYTE-ORDER>
              <START-POSITION>0</START-POSITION>
            </I-SIGNAL-TO-I-PDU-MAPPING>
          </I-SIGNAL-TO-PDU-MAPPINGS>
        </I-SIGNAL-I-PDU>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Frames</SHORT-NAME>
      <ELEMENTS>
        <FRAME>
          <SHORT-NAME>EngineControl_Frame</SHORT-NAME>
          <FRAME-LENGTH>8</FRAME-LENGTH>
          <PDU-TO-FRAME-MAPPINGS>
            <PDU-TO-FRAME-MAPPING>
              <SHORT-NAME>EngineControl_PduMap</SHORT-NAME>
              <PDU-REF>/Pdus/EngineControl_Pdu</PDU-REF>
            </PDU-TO-FRAME-MAPPING>
          </PDU-TO-FRAME-MAPPINGS>
        </FRAME>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Clusters</SHORT-NAME>
      <ELEMENTS>
        <CAN-CLUSTER>
          <SHORT-NAME>PowertrainBus</SHORT-NAME>
          <CAN-CLUSTER-VARIANTS>
            <CAN-CLUSTER-CONDITIONAL>
              <PHYSICAL-CHANNELS>
                <CAN-PHYSICAL-CHANNEL>
                  <SHORT-NAME>PowertrainChannel</SHORT-NAME>
                  <FRAME-TRIGGERINGS>
                    <CAN-FRAME-TRIGGERING>
                      <SHORT-NAME>EngineControl_Trig</SHORT-NAME>
                      <FRAME-REF>/Frames/EngineControl_Frame</FRAME-REF>
                      <IDENTIFIER>256</IDENTIFIER>
                    </CAN-FRAME-TRIGGERING>
                  </FRAME-TRIGGERINGS>
                </CAN-PHYSICAL-CHANNEL>
              </PHYSICAL-CHANNELS>
            </CAN-CLUSTER-CONDITIONAL>
          </CAN-CLUSTER-VARIANTS>
        </CAN-CLUSTER>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>CompuMethods</SHORT-NAME>
      <ELEMENTS>
        <COMPU-METHOD>
          <SHORT-NAME>EngineSpeed_Compu</SHORT-NAME>
          <COMPU-INTERNAL-TO-PHYS>
            <COMPU-SCALES>
              <COMPU-SCALE>
                <COMPU-RATIONAL-COEFFS>
                  <COMPU-NUMERATOR>
                    <V>0.0</V>
                    <V>0.25</V>
                  </COMPU-NUMERATOR>
                  <COMPU-DENOMINATOR>
                    <V>1.0</V>
                  </COMPU-DENOMINATOR>
                </COMPU-RATIONAL-COEFFS>
              </COMPU-SCALE>
            </COMPU-SCALES>
          </COMPU-INTERNAL-TO-PHYS>
        </COMPU-METHOD>
      </ELEMENTS>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>"#;

    #[test]
    fn parse_can_signal_chain() {
        let db = can_db_from_arxml(SAMPLE_ARXML.as_bytes()).unwrap();
        let sigs = db.signals(256);
        assert_eq!(sigs.len(), 1);
        let s = &sigs[0];
        assert_eq!(s.name, "EngineSpeed_ISignal");
        assert_eq!(s.signal.start_bit, 0);
        assert_eq!(s.signal.bit_length, 16);
        assert_eq!(s.signal.byte_order, ByteOrder::Intel);
        assert!(!s.signal.is_signed);
        assert_eq!(s.signal.scale, 0.25);
        assert_eq!(s.signal.offset, 0.0);
    }
}
