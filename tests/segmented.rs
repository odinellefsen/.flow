use std::fs;

use dotflow::event::EventRecord;
use dotflow::{Cursor, FlowStore, SegmentedReader, SegmentedWriter};

const HOUR_MS: u64 = 3_600_000;
const HOUR_NS: u64 = 3_600_000_000_000;
const BASE_NS: u64 = 1_743_292_800_000_000_000; // 2025-03-30 00:00 UTC

fn test_dir(name: &str) -> String {
    format!("test_{name}.flowtype")
}

fn cleanup(dir: &str) {
    let _ = fs::remove_dir_all(dir);
}

fn ts(hour: u64, offset_within_hour: u64) -> u64 {
    BASE_NS + hour * HOUR_NS + offset_within_hour * 1_000_000_000
}

fn ev(event_type_id: u16, event_time: u64, payload: &str) -> EventRecord {
    EventRecord::new(event_type_id, event_time, payload.as_bytes().to_vec())
}

// -- Segment rotation --

#[test]
fn single_segment_write_read() {
    let dir = test_dir("single_seg");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 2, HOUR_MS, 100).unwrap();
        for i in 0u64..10 {
            w.append(ev((i % 2) as u16, ts(0, i), &format!("e{i}")))
                .unwrap();
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 1);
    assert_eq!(store.manifest.segments[0].event_count, 10);

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(events.len(), 10);
    for (i, (ev, _)) in events.iter().enumerate() {
        assert_eq!(ev.sequence, i as u64);
    }

    cleanup(&dir);
}

#[test]
fn multiple_segments_rotation() {
    let dir = test_dir("multi_seg");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 2, HOUR_MS, 100).unwrap();
        for hour in 0..3u64 {
            for j in 0..5u64 {
                w.append(ev(0, ts(hour, j), &format!("h{hour}e{j}")))
                    .unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 3);

    assert_eq!(store.manifest.segments[0].start_sequence, 0);
    assert_eq!(store.manifest.segments[0].end_sequence, 4);
    assert_eq!(store.manifest.segments[1].start_sequence, 5);
    assert_eq!(store.manifest.segments[1].end_sequence, 9);
    assert_eq!(store.manifest.segments[2].start_sequence, 10);
    assert_eq!(store.manifest.segments[2].end_sequence, 14);

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(events.len(), 15);
    for (i, (ev, _)) in events.iter().enumerate() {
        assert_eq!(ev.sequence, i as u64);
    }

    cleanup(&dir);
}

#[test]
fn segment_files_match_manifest() {
    let dir = test_dir("seg_files");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for hour in 0..3u64 {
            for j in 0..20u64 {
                w.append(ev(0, ts(hour, j), "x")).unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    for seg in store.segments() {
        let path = store.dir().join(&seg.file);
        assert!(path.exists(), "segment file {} missing", seg.file);
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), seg.byte_size);
    }

    cleanup(&dir);
}

// -- Cross-segment reads --

#[test]
fn cross_segment_read_preserves_order() {
    let dir = test_dir("cross_order");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 3, HOUR_MS, 50).unwrap();
        for i in 0..100u64 {
            let hour = i / 25;
            let offset = i % 25;
            w.append(ev((i % 3) as u16, ts(hour, offset), &format!("{i}")))
                .unwrap();
        }
        w.flush().unwrap();
    }

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 100);
    let mut prev_seq = None;
    for (ev, _) in &events {
        if let Some(prev) = prev_seq {
            assert!(ev.sequence > prev, "sequence not monotonic");
        }
        prev_seq = Some(ev.sequence);
    }

    cleanup(&dir);
}

// -- Filtered reads across segments --

#[test]
fn filtered_read_across_segments() {
    let dir = test_dir("filter_seg");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 3, HOUR_MS, 50).unwrap();
        for i in 0..60u64 {
            let hour = i / 20;
            let offset = i % 20;
            w.append(ev((i % 3) as u16, ts(hour, offset), &format!("{i}")))
                .unwrap();
        }
        w.flush().unwrap();
    }

    let reader = SegmentedReader::open(&dir).unwrap().with_filter(&[1]);
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 20);
    for (ev, _) in &events {
        assert_eq!(ev.event_type_id, 1);
    }

    cleanup(&dir);
}

// -- Cursor resume --

#[test]
fn cursor_resume_mid_segment() {
    let dir = test_dir("cursor_mid");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for i in 0..50u64 {
            w.append(ev(0, ts(0, i), &format!("{i}"))).unwrap();
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let bucket = store.manifest.segments[0].bucket_start_ms;

    let cursor = Cursor {
        segment_bucket_ms: bucket,
        sequence: 25,
    };

    let reader = SegmentedReader::open_from(&dir, cursor).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 24); // sequences 26-49
    assert_eq!(events[0].0.sequence, 26);
    assert_eq!(events[events.len() - 1].0.sequence, 49);

    cleanup(&dir);
}

#[test]
fn cursor_resume_across_segments() {
    let dir = test_dir("cursor_cross");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for hour in 0..3u64 {
            for j in 0..20u64 {
                w.append(ev(0, ts(hour, j), &format!("h{hour}e{j}")))
                    .unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let seg1_bucket = store.manifest.segments[0].bucket_start_ms;

    let cursor = Cursor {
        segment_bucket_ms: seg1_bucket,
        sequence: 19,
    };

    let reader = SegmentedReader::open_from(&dir, cursor).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 40);
    assert_eq!(events[0].0.sequence, 20);
    assert_eq!(events[events.len() - 1].0.sequence, 59);

    cleanup(&dir);
}

#[test]
fn cursor_resume_with_filter() {
    let dir = test_dir("cursor_filter");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 3, HOUR_MS, 50).unwrap();
        for i in 0..60u64 {
            let hour = i / 20;
            let offset = i % 20;
            w.append(ev((i % 3) as u16, ts(hour, offset), &format!("{i}")))
                .unwrap();
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let seg1_bucket = store.manifest.segments[1].bucket_start_ms;

    let cursor = Cursor {
        segment_bucket_ms: seg1_bucket,
        sequence: 30,
    };

    let reader = SegmentedReader::open_from(&dir, cursor)
        .unwrap()
        .with_filter(&[0]);
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    for (ev, _) in &events {
        assert_eq!(ev.event_type_id, 0);
        assert!(ev.sequence > 30);
    }
    assert!(!events.is_empty());

    cleanup(&dir);
}

// -- Manifest persistence --

#[test]
fn manifest_survives_reopen() {
    let dir = test_dir("manifest_reopen");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "persist.v0", 2, HOUR_MS, 50).unwrap();
        for hour in 0..2u64 {
            for j in 0..10u64 {
                w.append(ev(j as u16 % 2, ts(hour, j), "x")).unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.flow_type, "persist.v0");
    assert_eq!(store.manifest.event_type_count, 2);
    assert_eq!(store.manifest.bucket_duration_ms, HOUR_MS);
    assert_eq!(store.manifest.segments.len(), 2);
    assert_eq!(store.next_sequence(), 20);

    cleanup(&dir);
}

#[test]
fn reopen_and_append() {
    let dir = test_dir("reopen_append");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for i in 0..10u64 {
            w.append(ev(0, ts(0, i), &format!("first-{i}"))).unwrap();
        }
        w.flush().unwrap();
    }

    {
        let mut w = SegmentedWriter::open(&dir, 50).unwrap();
        for i in 0..5u64 {
            w.append(ev(0, ts(0, 10 + i), &format!("second-{i}")))
                .unwrap();
        }
        for i in 0..5u64 {
            w.append(ev(0, ts(1, i), &format!("third-{i}"))).unwrap();
        }
        w.flush().unwrap();
    }

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(events.len(), 20);
    assert_eq!(events[0].0.sequence, 0);
    assert_eq!(events[19].0.sequence, 19);

    let store = FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 2);

    cleanup(&dir);
}

// -- Time-based seeking --

#[test]
fn segment_lookup_by_time() {
    let dir = test_dir("time_seek");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for hour in 0..4u64 {
            for j in 0..10u64 {
                w.append(ev(0, ts(hour, j), "x")).unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();

    let base_ms = BASE_NS / 1_000_000;
    let target_ms = base_ms + 2 * HOUR_MS + 30 * 60_000; // 2.5 hours in
    let seg = store.segment_for_time_ms(target_ms).unwrap();
    assert_eq!(seg.start_sequence, 20);
    assert_eq!(seg.end_sequence, 29);

    let seg = store.segment_for_time_ms(base_ms).unwrap();
    assert_eq!(seg.start_sequence, 0);

    cleanup(&dir);
}

// -- Cursor values are correct --

#[test]
fn cursor_values_track_position() {
    let dir = test_dir("cursor_vals");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for hour in 0..2u64 {
            for j in 0..5u64 {
                w.append(ev(0, ts(hour, j), "x")).unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let seg0_bucket = store.manifest.segments[0].bucket_start_ms;
    let seg1_bucket = store.manifest.segments[1].bucket_start_ms;

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    for (_, cursor) in &events[..5] {
        assert_eq!(cursor.segment_bucket_ms, seg0_bucket);
    }
    for (_, cursor) in &events[5..] {
        assert_eq!(cursor.segment_bucket_ms, seg1_bucket);
    }

    for (i, (ev, cursor)) in events.iter().enumerate() {
        assert_eq!(ev.sequence, i as u64);
        assert_eq!(cursor.sequence, i as u64);
    }

    cleanup(&dir);
}

// -- Empty store --

#[test]
fn empty_store_read() {
    let dir = test_dir("empty_store");
    cleanup(&dir);

    {
        let _w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
    }

    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(events.len(), 0);

    cleanup(&dir);
}

// -- Event type registry --

#[test]
fn event_type_registry_persists() {
    let dir = test_dir("et_registry");
    cleanup(&dir);

    {
        let _w = SegmentedWriter::create(&dir, "food-item.v0", 3, HOUR_MS, 50).unwrap();
    }

    let mut store = FlowStore::open(&dir).unwrap();
    store.register_event_type(0, "food-item.created.v0");
    store.register_event_type(1, "food-item.updated.v0");
    store.register_event_type(2, "food-item.deleted.v0");
    store.save_manifest().unwrap();

    let store2 = FlowStore::open(&dir).unwrap();
    assert_eq!(store2.event_type_name(0), Some("food-item.created.v0"));
    assert_eq!(store2.event_type_name(1), Some("food-item.updated.v0"));
    assert_eq!(store2.event_type_name(2), Some("food-item.deleted.v0"));
    assert_eq!(store2.event_type_name(99), None);

    cleanup(&dir);
}

// ── Schema-aware encode/decode integration ────────────────────────────────────

#[test]
fn schema_locked_on_first_event_and_persisted() {
    let dir = test_dir("schema_lock_persist");

    let payload = br#"{"id":"abc-123","name":"Burger","price":9.99}"#;
    {
        let mut w = SegmentedWriter::create(&dir, "food-item", 1, HOUR_MS, 50).unwrap();
        let mut event = EventRecord::new(0, 0, payload.to_vec());
        event.event_id = [1u8; 16];
        w.append(event).unwrap();
    }

    // Schema must be locked in the manifest after the write
    let store = FlowStore::open(&dir).unwrap();
    let schema = store.get_schema(0).expect("schema must be locked");
    let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    // All three keys must appear (order is alphabetical from serde_json::Map)
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"price"));

    cleanup(&dir);
}

#[test]
fn schema_encoded_payload_roundtrips_through_reader() {
    let dir = test_dir("schema_roundtrip");

    let payloads: Vec<&[u8]> = vec![
        br#"{"id":"aaa","name":"Burger","price":9.99}"#,
        br#"{"id":"bbb","name":"Fries","price":3.49}"#,
        br#"{"id":"ccc","name":"Shake","price":4.99}"#,
    ];

    {
        let mut w = SegmentedWriter::create(&dir, "food-item", 1, HOUR_MS, 50).unwrap();
        for (i, &p) in payloads.iter().enumerate() {
            let mut event = EventRecord::new(0, i as u64 * 1_000_000, p.to_vec());
            event.event_id = [i as u8 + 1; 16];
            w.append(event).unwrap();
        }
    }

    // All events must decode back to original JSON
    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<_> = reader.into_iter().map(|r| r.unwrap().0).collect();

    assert_eq!(events.len(), 3);
    for (event, &original_payload) in events.iter().zip(payloads.iter()) {
        let original: serde_json::Value = serde_json::from_slice(original_payload).unwrap();
        let recovered: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(original, recovered, "payload mismatch for event {}", event.sequence);
        assert_eq!(event.payload_codec, 0, "payload_codec should be reset to raw after decode");
    }

    cleanup(&dir);
}

#[test]
fn schema_encoded_payload_is_smaller_on_disk() {
    let dir_raw = test_dir("schema_size_raw");
    let dir_schema = test_dir("schema_size_schema");

    let payload = br#"{"id":"a4a41742-16f4-4a7e-9cb5-e0048d207a2c","userId":"user_30eqFgTWfuBysprgdMgwvjEezH2","name":"Burger","categoryHierarchy":["Fast Food","American","Burgers"]}"#;

    let n = 200u64;

    // Write raw (no schema): use FlowStore with no auto-locking by writing
    // raw bytes directly via low-level writer
    {
        use dotflow::FlowWriter;
        FlowStore::create(&dir_raw, "food-item", 1, HOUR_MS).unwrap();
        let path = format!("{dir_raw}/seg_0.flow");
        let mut w = FlowWriter::create(&path, 0, 1000).unwrap();
        for i in 0..n {
            let mut event = EventRecord::new(0, i * 1_000_000, payload.to_vec());
            event.event_id = [i as u8; 16];
            w.append(event).unwrap();
        }
    }

    // Write schema-encoded via SegmentedWriter
    {
        let mut w = SegmentedWriter::create(&dir_schema, "food-item", 1, HOUR_MS, 50).unwrap();
        for i in 0..n {
            let mut event = EventRecord::new(0, i * 1_000_000, payload.to_vec());
            event.event_id = [i as u8; 16];
            w.append(event).unwrap();
        }
    }

    let raw_size: u64 = fs::read_dir(&dir_raw)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .map(|e| e.metadata().unwrap().len())
        .sum();

    let schema_size: u64 = fs::read_dir(&dir_schema)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .map(|e| e.metadata().unwrap().len())
        .sum();

    assert!(
        schema_size < raw_size,
        "schema-encoded ({schema_size} bytes) should be smaller than raw ({raw_size} bytes)"
    );

    cleanup(&dir_raw);
    cleanup(&dir_schema);
}

#[test]
fn schema_validation_rejects_extra_fields() {
    let dir = test_dir("schema_validation");

    let first_payload = br#"{"id":"abc","name":"Burger"}"#;
    let bad_payload = br#"{"id":"def","name":"Fries","extra":"surprise"}"#;

    let mut w = SegmentedWriter::create(&dir, "food-item", 1, HOUR_MS, 50).unwrap();

    // First event locks schema {id, name}
    let mut e1 = EventRecord::new(0, 0, first_payload.to_vec());
    e1.event_id = [1u8; 16];
    w.append(e1).unwrap();

    // Second event has an extra field -- must be rejected
    let mut e2 = EventRecord::new(0, 1_000_000, bad_payload.to_vec());
    e2.event_id = [2u8; 16];
    let result = w.append(e2);
    assert!(result.is_err(), "extra field should be rejected after schema is locked");

    cleanup(&dir);
}

#[test]
fn schema_allows_null_for_locked_fields() {
    let dir = test_dir("schema_null_field");

    let first_payload = br#"{"id":"abc","note":"hello"}"#;
    let null_payload = br#"{"id":"def","note":null}"#;

    let mut w = SegmentedWriter::create(&dir, "food-item", 1, HOUR_MS, 50).unwrap();

    let mut e1 = EventRecord::new(0, 0, first_payload.to_vec());
    e1.event_id = [1u8; 16];
    w.append(e1).unwrap();

    // Null is allowed for any field
    let mut e2 = EventRecord::new(0, 1_000_000, null_payload.to_vec());
    e2.event_id = [2u8; 16];
    w.append(e2).unwrap();

    drop(w);

    // Read back and verify null is preserved
    let reader = SegmentedReader::open(&dir).unwrap();
    let events: Vec<_> = reader.into_iter().map(|r| r.unwrap().0).collect();
    assert_eq!(events.len(), 2);

    let recovered: serde_json::Value = serde_json::from_slice(&events[1].payload).unwrap();
    assert!(recovered["note"].is_null());

    cleanup(&dir);
}

#[test]
fn schema_not_locked_for_non_json_payload() {
    let dir = test_dir("schema_non_json");

    let mut w = SegmentedWriter::create(&dir, "binary-data", 1, HOUR_MS, 50).unwrap();

    // Non-JSON payload -- should be stored raw without locking
    let mut e = EventRecord::new(0, 0, b"\x00\x01\x02\x03binary".to_vec());
    e.event_id = [1u8; 16];
    w.append(e).unwrap();

    drop(w);

    let store = FlowStore::open(&dir).unwrap();
    assert!(store.get_schema(0).is_none(), "non-JSON payload must not lock a schema");

    cleanup(&dir);
}
