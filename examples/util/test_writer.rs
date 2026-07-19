use std::fs::File;
use std::io::BufWriter;

use vector_blf::blf::{BaseObject, Message, Timestamp, Writer};

/// Timestamp of the first test object (1 ms).
const TS_BASE_NS: u64 = 1_000_000;
/// Nanosecond gap between consecutive test objects (100 ms).
const TS_STEP_NS: u64 = 100_000_000;

/// Timestamp assigned to the `index`-th written object.
pub fn ts_at(index: u32) -> u64 {
    TS_BASE_NS + TS_STEP_NS * u64::from(index)
}

/// BLF writer for test generators: assigns evenly spaced nanosecond
/// timestamps and reports the object count on finish.
pub struct TestBlfWriter {
    writer: Writer<BufWriter<File>>,
    path: String,
    written: usize,
}

impl TestBlfWriter {
    pub fn create(path: &str) -> Self {
        let file = File::create(path).expect("create output file");
        Self {
            writer: Writer::new(BufWriter::new(file)).expect("init writer"),
            path: path.to_string(),
            written: 0,
        }
    }

    pub fn push(&mut self, message: Message) {
        let obj = BaseObject {
            timestamp: Timestamp::Nanosecond(ts_at(self.written as u32)),
            message,
        };
        self.writer.write_base_object(&obj).expect("write object");
        self.written += 1;
    }

    /// Finalizes the file and prints how many `what` objects were written.
    pub fn finish(mut self, what: &str) {
        self.writer.finish().expect("finish writer");
        println!("wrote {} {what} objects to {}", self.written, self.path);
    }
}
