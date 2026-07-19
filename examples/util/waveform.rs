use std::f64::consts::PI;

const PERIOD: u32 = 20;

/// Deterministic 8-bit test waveform sampled at time step `t`.
///
/// The shape is selected by `kind % 5` (sine, square, sawtooth, triangle,
/// random walk); `seed` shifts the phase and decorrelates the random walk so
/// signals sharing a shape still differ.
pub fn sample(kind: u32, t: u32, seed: u32) -> u8 {
    match kind % 5 {
        0 => sine(t, seed),
        1 => square(t, seed),
        2 => sawtooth(t, seed),
        3 => triangle(t, seed),
        _ => random_walk(t, seed),
    }
}

fn sine(t: u32, seed: u32) -> u8 {
    let phase = f64::from(seed % PERIOD) / f64::from(PERIOD);
    let x = 2.0 * PI * (f64::from(t) / f64::from(PERIOD) + phase);
    ((x.sin() * 0.5 + 0.5) * 255.0).round() as u8
}

fn square(t: u32, seed: u32) -> u8 {
    if ((t + seed) / (PERIOD / 2)).is_multiple_of(2) {
        255
    } else {
        0
    }
}

fn sawtooth(t: u32, seed: u32) -> u8 {
    (((t + seed) % PERIOD) * 255 / (PERIOD - 1)) as u8
}

fn triangle(t: u32, seed: u32) -> u8 {
    let half = PERIOD / 2;
    let p = (t + seed) % PERIOD;
    let v = if p <= half { p } else { PERIOD - p };
    (v * 255 / half) as u8
}

fn random_walk(t: u32, seed: u32) -> u8 {
    let mut rng = seed.wrapping_mul(2_654_435_761).wrapping_add(1);
    let mut level: i32 = 128;
    for _ in 0..=t {
        rng = rng.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let step = ((rng >> 16) % 31) as i32 - 15;
        level = (level + step).clamp(0, 255);
    }
    level as u8
}
