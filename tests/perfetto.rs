#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufWriter, Write};
    use vector_blf::blf::{BaseObject, Can, Dir, Message, Timestamp, Writer};

    #[test]
    fn perfetto_output_roundtrip() {
        // 1. Generate a small BLF file on disk.
        let blf_path = "/tmp/test_perfetto_e2e.blf";
        {
            let file = fs::File::create(blf_path).unwrap();
            let mut writer = Writer::new(BufWriter::new(file)).unwrap();
            for i in 0u64..5 {
                let obj = BaseObject {
                    timestamp: Timestamp::Nanosecond(i * 1_000_000_000),
                    message: Message::Can(Can {
                        channel: 1,
                        id: 0x100,
                        is_ext_id: false,
                        dir: Dir::Rx,
                        rtr: false,
                        dlc: 2,
                        data: vec![(i as u8).wrapping_mul(10), 0],
                    }),
                };
                writer.write_base_object(&obj).unwrap();
            }
            writer.finish().unwrap();
        }

        // 2. Write a minimal CAN signal CSV.
        let sig_path = "/tmp/test_perfetto_e2e_signals.csv";
        {
            let mut csv = fs::File::create(sig_path).unwrap();
            writeln!(
                csv,
                "message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset,pdu_id"
            )
            .unwrap();
            writeln!(csv, "0x100,throttle,0,0,8,Intel,false,0.39216,0.0,").unwrap();
        }

        // 3. Convert BLF -> .perfetto-trace via CLI.
        let output_path = "/tmp/test_perfetto_e2e.perfetto-trace";
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_vector-blf-rs"))
            .args([
                "parse",
                blf_path,
                output_path,
                "--can-signals",
                sig_path,
                "-q",
            ])
            .status()
            .unwrap();
        assert!(status.success(), "CLI failed: {status:?}");

        // 4. Validate: non-empty and starts with a valid Trace.packet field tag.
        // Trace { repeated TracePacket packet = 1; }
        // Tag for field 1 with wire type 2 (LEN) = (1 << 3) | 2 = 0x0A
        let out = fs::read(output_path).unwrap();
        assert!(!out.is_empty(), "Perfetto output is empty");
        assert_eq!(
            out[0], 0x0A,
            "Expected Trace.packet field tag 0x0A at byte 0"
        );
    }
}
