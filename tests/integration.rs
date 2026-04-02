use std::fs::{self, OpenOptions};
use std::io::Write;

use dotflow::block::{build_filter, BlockHeader};
use dotflow::event::EventRecord;
use dotflow::format;
use dotflow::recovery;
use dotflow::{FlowReader, FlowWriter};

fn temp_path(name: &str) -> String {
    format!("test_{name}.flow")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

fn ev(event_type_id: u16, event_time: u64, payload: &[u8]) -> EventRecord {
    EventRecord::new(event_type_id, event_time, payload.to_vec())
}

// ── Event record encode/decode round-trip ──

#[test]
fn event_record_roundtrip() {
    let mut event = EventRecord::new(7, 1_000_000_000, b"hello world".to_vec());
    event.sequence = 42;
    event.valid_time = 2_000_000_000;
    event.recorded_time = 3_000_000_000;

    let mut buf = Vec::new();
    event.encode(&mut buf);

    let (decoded, consumed) = EventRecord::decode(&buf).unwrap();
    assert_eq!(consumed, buf.len());
    assert_eq!(decoded, event);
}

#[test]
fn event_record_with_optional_ids() {
    let mut event = EventRecord::new(1, 1_000_000_000, b"data".to_vec());
    event.correlation_id = Some([1u8; 16]);
    event.causation_id = Some([2u8; 16]);
    event.metadata = vec![
        ("key1".to_string(), "val1".to_string()),
        ("key2".to_string(), "val2".to_string()),
    ];

    let mut buf = Vec::new();
    event.encode(&mut buf);

    let (decoded, consumed) = EventRecord::decode(&buf).unwrap();
    assert_eq!(consumed, buf.len());
    assert_eq!(decoded.correlation_id, Some([1u8; 16]));
    assert_eq!(decoded.causation_id, Some([2u8; 16]));
    assert_eq!(decoded.metadata.len(), 2);
    assert_eq!(decoded.metadata[0], ("key1".to_string(), "val1".to_string()));
}

#[test]
fn event_record_empty_payload() {
    let event = EventRecord::new(0, 0, vec![]);

    let mut buf = Vec::new();
    event.encode(&mut buf);
    assert_eq!(buf.len(), format::EVENT_RECORD_MIN_SIZE);

    let (decoded, consumed) = EventRecord::decode(&buf).unwrap();
    assert_eq!(consumed, format::EVENT_RECORD_MIN_SIZE);
    assert_eq!(decoded.event_type_id, 0);
    assert_eq!(decoded.payload, &[] as &[u8]);
}

#[test]
fn event_record_bad_checksum() {
    let event = EventRecord::new(0, 100, b"data".to_vec());

    let mut buf = Vec::new();
    event.encode(&mut buf);

    let last = buf.len() - 1;
    buf[last] ^= 0xFF;

    let result = EventRecord::decode(&buf);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "event record checksum mismatch");
}

// ── Block header bitset ──

#[test]
fn block_header_bitset_operations() {
    let mut hdr = BlockHeader::new();
    assert!(!hdr.contains_event_type(0));
    assert!(!hdr.contains_event_type(5));

    hdr.set_event_type(0);
    hdr.set_event_type(5);
    hdr.set_event_type(255);

    assert!(hdr.contains_event_type(0));
    assert!(hdr.contains_event_type(5));
    assert!(hdr.contains_event_type(255));
    assert!(!hdr.contains_event_type(1));
    assert!(!hdr.contains_event_type(254));
}

#[test]
fn block_header_filter_matching() {
    let mut hdr = BlockHeader::new();
    hdr.set_event_type(2);
    hdr.set_event_type(10);

    let filter_match = build_filter(&[2]);
    assert!(hdr.matches_filter(&filter_match));

    let filter_no_match = build_filter(&[3, 4, 5]);
    assert!(!hdr.matches_filter(&filter_no_match));

    let filter_partial = build_filter(&[1, 10, 20]);
    assert!(hdr.matches_filter(&filter_partial));
}

#[test]
fn block_header_encode_decode_roundtrip() {
    let mut hdr = BlockHeader::new();
    hdr.start_sequence = 100;
    hdr.end_sequence = 199;
    hdr.event_count = 100;
    hdr.data_size = 5000;
    hdr.set_event_type(0);
    hdr.set_event_type(3);
    hdr.checksum = 0xDEADBEEF;

    let encoded = hdr.encode();
    assert_eq!(encoded.len(), format::BLOCK_HEADER_SIZE);

    let decoded = BlockHeader::decode(&encoded);
    assert_eq!(decoded, hdr);
}

// ── Write and read round-trip ──

#[test]
fn write_read_roundtrip() {
    let path = temp_path("roundtrip");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 3, 1000).unwrap();
        writer.append(ev(0, 100, b"event-a")).unwrap();
        writer.append(ev(1, 200, b"event-b")).unwrap();
        writer.append(ev(2, 300, b"event-c")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[0].event_type_id, 0);
    assert_eq!(events[0].event_time, 100);
    assert_eq!(events[0].payload, b"event-a");
    assert_eq!(events[1].sequence, 1);
    assert_eq!(events[1].event_type_id, 1);
    assert_eq!(events[2].sequence, 2);
    assert_eq!(events[2].event_type_id, 2);

    cleanup(&path);
}

#[test]
fn write_multiple_blocks() {
    let path = temp_path("multiblock");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 2, 3).unwrap();
        for i in 0..10u64 {
            writer
                .append(ev((i % 2) as u16, i * 100, format!("evt-{i}").as_bytes()))
                .unwrap();
        }
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 10);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.sequence, i as u64);
        assert_eq!(event.payload, format!("evt-{i}").as_bytes());
    }

    cleanup(&path);
}

// ── Filtered reads ──

#[test]
fn filtered_read_single_type() {
    let path = temp_path("filter_single");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 3, 1000).unwrap();
        writer.append(ev(0, 100, b"created-alice")).unwrap();
        writer.append(ev(1, 200, b"updated-alice")).unwrap();
        writer.append(ev(0, 300, b"created-bob")).unwrap();
        writer.append(ev(2, 400, b"deleted-alice")).unwrap();
        writer.append(ev(0, 500, b"created-charlie")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap().with_filter(&[0]);
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].payload, b"created-alice");
    assert_eq!(events[1].payload, b"created-bob");
    assert_eq!(events[2].payload, b"created-charlie");

    cleanup(&path);
}

#[test]
fn filtered_read_multiple_types() {
    let path = temp_path("filter_multi");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 3, 1000).unwrap();
        writer.append(ev(0, 100, b"a")).unwrap();
        writer.append(ev(1, 200, b"b")).unwrap();
        writer.append(ev(2, 300, b"c")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap().with_filter(&[0, 2]);
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type_id, 0);
    assert_eq!(events[1].event_type_id, 2);

    cleanup(&path);
}

#[test]
fn filtered_read_skips_irrelevant_blocks() {
    let path = temp_path("filter_skip");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 3, 2).unwrap();
        writer.append(ev(0, 100, b"a0")).unwrap();
        writer.append(ev(0, 200, b"a1")).unwrap();
        writer.append(ev(1, 300, b"b0")).unwrap();
        writer.append(ev(1, 400, b"b1")).unwrap();
        writer.append(ev(2, 500, b"c0")).unwrap();
        writer.append(ev(0, 600, b"a2")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap().with_filter(&[2]);
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, b"c0");
    assert_eq!(events[0].sequence, 4);

    cleanup(&path);
}

// ── Recovery ──

#[test]
fn recovery_valid_file() {
    let path = temp_path("recovery_valid");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 2, 5).unwrap();
        for i in 0..10u64 {
            writer.append(ev(i as u16 % 2, i * 100, &[i as u8; 10])).unwrap();
        }
        writer.flush().unwrap();
    }

    let prefix = recovery::validate_file(&path).unwrap();
    assert_eq!(prefix.block_count, 2);
    assert_eq!(prefix.event_count, 10);
    assert_eq!(prefix.next_sequence, 10);

    cleanup(&path);
}

#[test]
fn recovery_truncated_block_data() {
    let path = temp_path("recovery_trunc_data");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 2, 5).unwrap();
        for i in 0..10u64 {
            writer.append(ev(i as u16 % 2, i * 100, &[i as u8; 10])).unwrap();
        }
        writer.flush().unwrap();
    }

    let file_len = fs::metadata(&path).unwrap().len();

    {
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(file_len - 10).unwrap();
    }

    let prefix = recovery::validate_file(&path).unwrap();
    assert_eq!(prefix.block_count, 1);
    assert_eq!(prefix.event_count, 5);
    assert_eq!(prefix.next_sequence, 5);

    cleanup(&path);
}

#[test]
fn recovery_corrupted_block_checksum() {
    let path = temp_path("recovery_corrupt_crc");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 1, 3).unwrap();
        for i in 0..6u64 {
            writer.append(ev(0, i * 100, &[i as u8; 5])).unwrap();
        }
        writer.flush().unwrap();
    }

    let prefix_before = recovery::validate_file(&path).unwrap();
    assert_eq!(prefix_before.block_count, 2);

    {
        let mut data = fs::read(&path).unwrap();
        let corrupt_offset = prefix_before.valid_end as usize - 5;
        data[corrupt_offset] ^= 0xFF;
        fs::write(&path, &data).unwrap();
    }

    let prefix_after = recovery::validate_file(&path).unwrap();
    assert_eq!(prefix_after.block_count, 1);
    assert_eq!(prefix_after.event_count, 3);

    cleanup(&path);
}

#[test]
fn recovery_empty_file_after_header() {
    let path = temp_path("recovery_empty");
    cleanup(&path);

    {
        let _writer = FlowWriter::create(&path, 5, 100).unwrap();
    }

    let prefix = recovery::validate_file(&path).unwrap();
    assert_eq!(prefix.block_count, 0);
    assert_eq!(prefix.event_count, 0);
    assert_eq!(prefix.next_sequence, 0);
    assert_eq!(prefix.valid_end, format::FILE_HEADER_SIZE as u64);

    cleanup(&path);
}

// ── Reopen and append ──

#[test]
fn reopen_and_append() {
    let path = temp_path("reopen");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 2, 100).unwrap();
        writer.append(ev(0, 100, b"first")).unwrap();
        writer.append(ev(1, 200, b"second")).unwrap();
        writer.flush().unwrap();
    }

    {
        let mut writer = FlowWriter::open(&path, 100).unwrap();
        writer.append(ev(0, 300, b"third")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[1].sequence, 1);
    assert_eq!(events[2].sequence, 2);
    assert_eq!(events[2].payload, b"third");

    cleanup(&path);
}

#[test]
fn reopen_after_crash_recovery() {
    let path = temp_path("reopen_crash");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 1, 3).unwrap();
        for i in 0..6u64 {
            writer.append(ev(0, i * 100, format!("e{i}").as_bytes())).unwrap();
        }
        writer.flush().unwrap();
    }

    {
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00]).unwrap();
    }

    {
        let mut writer = FlowWriter::open(&path, 3).unwrap();
        writer.append(ev(0, 700, b"after-crash")).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 7);
    assert_eq!(events[6].sequence, 6);
    assert_eq!(events[6].payload, b"after-crash");

    cleanup(&path);
}

// ── File header validation ──

#[test]
fn bad_magic_bytes() {
    let path = temp_path("bad_magic");
    cleanup(&path);

    fs::write(&path, b"NOPE0000000000000").unwrap();

    let result = FlowReader::open(&path);
    assert!(result.is_err());

    cleanup(&path);
}

// ── Auto-flush on block_size ──

#[test]
fn auto_flush_on_block_size() {
    let path = temp_path("autoflush");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 1, 2).unwrap();
        writer.append(ev(0, 100, b"one")).unwrap();
        writer.append(ev(0, 200, b"two")).unwrap();
    }

    let prefix = recovery::validate_file(&path).unwrap();
    assert!(prefix.block_count >= 1);
    assert!(prefix.event_count >= 2);

    cleanup(&path);
}

// ── Large payload ──

#[test]
fn large_payload() {
    let path = temp_path("large_payload");
    cleanup(&path);

    let big_payload = vec![0xAB; 1_000_000];

    {
        let mut writer = FlowWriter::create(&path, 1, 100).unwrap();
        writer.append(ev(0, 100, &big_payload)).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, big_payload);

    cleanup(&path);
}

// ── Metadata round-trip ──

#[test]
fn metadata_roundtrip() {
    let path = temp_path("metadata");
    cleanup(&path);

    {
        let mut writer = FlowWriter::create(&path, 1, 100).unwrap();
        let mut event = EventRecord::new(0, 1_000_000, b"payload".to_vec());
        event.metadata = vec![
            ("ttl".to_string(), "3600".to_string()),
            ("notify".to_string(), "true".to_string()),
        ];
        writer.append(event).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].metadata.len(), 2);
    assert_eq!(events[0].metadata[0], ("ttl".to_string(), "3600".to_string()));
    assert_eq!(events[0].metadata[1], ("notify".to_string(), "true".to_string()));

    cleanup(&path);
}

// ── Correlation and causation IDs ──

#[test]
fn correlation_causation_roundtrip() {
    let path = temp_path("correlation");
    cleanup(&path);

    let corr = [0xAA; 16];
    let caus = [0xBB; 16];

    {
        let mut writer = FlowWriter::create(&path, 1, 100).unwrap();
        let mut event = EventRecord::new(0, 500, b"linked".to_vec());
        event.correlation_id = Some(corr);
        event.causation_id = Some(caus);
        writer.append(event).unwrap();
        writer.flush().unwrap();
    }

    let reader = FlowReader::open(&path).unwrap();
    let events: Vec<EventRecord> = reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].correlation_id, Some(corr));
    assert_eq!(events[0].causation_id, Some(caus));

    cleanup(&path);
}
