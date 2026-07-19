use std::fs::File;
use std::io::{BufWriter, Write as _};

/// Number of samples a dropout keeps the signal stuck at zero.
const DROPOUT_LEN: u32 = 10;
/// Saturating level shift added by a step anomaly.
const STEP_OFFSET: u8 = 80;

/// Anomaly kinds injected into generated signals so analytics answers
/// (e.g. from a Genie Space) can be checked against known ground truth.
#[derive(Clone, Copy)]
pub enum Anomaly {
    /// Single-sample outlier pinned to 255.
    Spike,
    /// Level shift of +STEP_OFFSET from the start sample onward.
    Step,
    /// Signal stuck at 0 for DROPOUT_LEN samples.
    Dropout,
}

impl Anomaly {
    fn name(self) -> &'static str {
        match self {
            Anomaly::Spike => "spike",
            Anomaly::Step => "step",
            Anomaly::Dropout => "dropout",
        }
    }

    fn end_sample(self, start: u32, samples: u32) -> u32 {
        match self {
            Anomaly::Spike => start,
            Anomaly::Step => samples - 1,
            Anomaly::Dropout => (start + DROPOUT_LEN - 1).min(samples - 1),
        }
    }

    fn value_at(self, clean: u8, start: u32, t: u32) -> u8 {
        match self {
            Anomaly::Spike if t == start => 255,
            Anomaly::Step if t >= start => clean.saturating_add(STEP_OFFSET),
            Anomaly::Dropout if t >= start && t < start + DROPOUT_LEN => 0,
            _ => clean,
        }
    }
}

/// Deterministic anomaly assignment: roughly one signal in ten gets a single
/// anomaly, starting away from the edges so it stands out from the waveform.
fn anomaly_for(seed: u32, samples: u32) -> Option<(Anomaly, u32)> {
    let h = mix(seed);
    let kind = match h % 30 {
        0 => Anomaly::Spike,
        1 => Anomaly::Step,
        2 => Anomaly::Dropout,
        _ => return None,
    };
    let start = samples / 5 + (h / 30) % (samples * 3 / 5);
    Some((kind, start))
}

/// Overlays the assigned anomalies onto a payload of byte-wide signals whose
/// waveform seeds are `base_seed .. base_seed + payload.len()`, at time `t`.
pub fn apply(payload: &mut [u8], base_seed: u32, t: u32, samples: u32) {
    for (i, value) in payload.iter_mut().enumerate() {
        if let Some((kind, start)) = anomaly_for(base_seed + i as u32, samples) {
            *value = kind.value_at(*value, start, t);
        }
    }
}

/// Writes the ground-truth CSV for `groups * signals_per_group` signals named
/// `Sig_{group}_{sig}`; `ts_of` maps a written-object index to its timestamp.
///
/// Returns the number of anomaly rows written.
pub fn write_ground_truth(
    path: &str,
    groups: u32,
    signals_per_group: u32,
    samples: u32,
    ts_of: impl Fn(u32) -> u64,
) -> std::io::Result<usize> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(
        w,
        "signal_name,anomaly_type,start_sample,end_sample,start_timestamp_ns,end_timestamp_ns"
    )?;
    let mut rows = 0usize;
    for group in 0..groups {
        for sig in 0..signals_per_group {
            let seed = group * signals_per_group + sig;
            if let Some((kind, start)) = anomaly_for(seed, samples) {
                let end = kind.end_sample(start, samples);
                writeln!(
                    w,
                    "Sig_{group}_{sig},{},{start},{end},{},{}",
                    kind.name(),
                    ts_of(group * samples + start),
                    ts_of(group * samples + end),
                )?;
                rows += 1;
            }
        }
    }
    Ok(rows)
}

fn mix(seed: u32) -> u32 {
    let mut x = seed.wrapping_add(0x9E37_79B9);
    x ^= x >> 16;
    x = x.wrapping_mul(0x85EB_CA6B);
    x ^= x >> 13;
    x
}
