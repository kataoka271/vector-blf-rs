pub mod blf;

use blf::{BaseObject, ParseError, Reader, Writer};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::time;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    env_logger::init();

    let input = args
        .get(1)
        .expect("usage: vector-blf-rs <input.blf> [output.blf] [repeat]");
    let t = time::Instant::now();
    let reader = Reader::new(BufReader::new(File::open(input)?))?;
    println!("header: {:?}", reader.header);

    let mut objects: Vec<Result<BaseObject, ParseError>> = Vec::new();
    for obj in reader {
        objects.push(obj);
    }
    println!(
        "read {} objects in {:.3}s",
        objects.len(),
        t.elapsed().as_secs_f32()
    );

    if let Some(output) = args.get(2) {
        let repeat: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
        let t = time::Instant::now();
        let mut writer = Writer::new(BufWriter::new(File::create(output)?))?;
        let mut count = 0usize;
        for _ in 0..repeat {
            for obj in objects.iter().flatten() {
                writer.write_base_object(obj)?;
                count += 1;
            }
        }
        writer.finish()?;
        println!(
            "wrote {} objects in {:.3}s",
            count,
            t.elapsed().as_secs_f32()
        );
    }

    Ok(())
}
