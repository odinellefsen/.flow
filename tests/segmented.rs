use std::fs;

use dotflow::{Cursor, EventRecord, FlowStore, SegmentedReader, SegmentedWriter};

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

// -- Segment rotation --

#[test]
fn single_segment_write_read() {
    let dir = test_dir("single_seg");
    cleanup(&dir);

    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 2, HOUR_MS, 100).unwrap();
        for i in 0u64..10 {
            w.append((i % 2) as u16, ts(0, i), format!("e{i}").into_bytes())
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
        // Write events across 3 hourly buckets
        for hour in 0..3u64 {
            for j in 0..5u64 {
                w.append(0, ts(hour, j), format!("h{hour}e{j}").into_bytes())
                    .unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 3);

    // Sequences should be globally monotonic
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
                w.append(0, ts(hour, j), vec![0u8; 10]).unwrap();
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
            w.append((i % 3) as u16, ts(hour, offset), format!("{i}").into_bytes())
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
            w.append((i % 3) as u16, ts(hour, offset), format!("{i}").into_bytes())
                .unwrap();
        }
        w.flush().unwrap();
    }

    // Filter for type 1 only
    let reader = SegmentedReader::open(&dir).unwrap().with_filter(&[1]);
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    // Type 1 events: i % 3 == 1 -> indices 1,4,7,10,13,16,19,22,... -> 20 total
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
            w.append(0, ts(0, i), format!("{i}").into_bytes()).unwrap();
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let bucket = store.manifest.segments[0].bucket_start_ms;

    // Resume after sequence 25
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
                w.append(0, ts(hour, j), format!("h{hour}e{j}").into_bytes())
                    .unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let seg1_bucket = store.manifest.segments[0].bucket_start_ms;

    // Resume from end of first segment (seq 19), should get all of seg 1 and 2
    let cursor = Cursor {
        segment_bucket_ms: seg1_bucket,
        sequence: 19,
    };

    let reader = SegmentedReader::open_from(&dir, cursor).unwrap();
    let events: Vec<(EventRecord, Cursor)> =
        reader.into_iter().map(|r| r.unwrap()).collect();

    // Segments 1 and 2 have sequences 20-39 and 40-59
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
            w.append((i % 3) as u16, ts(hour, offset), format!("{i}").into_bytes())
                .unwrap();
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();
    let seg1_bucket = store.manifest.segments[1].bucket_start_ms;

    // Resume from seg 1, seq 30, filter for type 0
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
                w.append(j as u16 % 2, ts(hour, j), vec![1u8; 5]).unwrap();
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
            w.append(0, ts(0, i), format!("first-{i}").into_bytes())
                .unwrap();
        }
        w.flush().unwrap();
    }

    {
        let mut w = SegmentedWriter::open(&dir, 50).unwrap();
        // Append to the same segment
        for i in 0..5u64 {
            w.append(0, ts(0, 10 + i), format!("second-{i}").into_bytes())
                .unwrap();
        }
        // And to a new segment
        for i in 0..5u64 {
            w.append(0, ts(1, i), format!("third-{i}").into_bytes())
                .unwrap();
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
                w.append(0, ts(hour, j), vec![0u8; 5]).unwrap();
            }
        }
        w.flush().unwrap();
    }

    let store = FlowStore::open(&dir).unwrap();

    // Lookup by time in the middle of hour 2
    let base_ms = BASE_NS / 1_000_000;
    let target_ms = base_ms + 2 * HOUR_MS + 30 * 60_000; // 2.5 hours in
    let seg = store.segment_for_time_ms(target_ms).unwrap();
    assert_eq!(seg.start_sequence, 20);
    assert_eq!(seg.end_sequence, 29);

    // Lookup at the exact start of hour 0
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
                w.append(0, ts(hour, j), vec![0u8; 5]).unwrap();
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

    // First 5 events should have seg0 bucket
    for (_, cursor) in &events[..5] {
        assert_eq!(cursor.segment_bucket_ms, seg0_bucket);
    }
    // Last 5 events should have seg1 bucket
    for (_, cursor) in &events[5..] {
        assert_eq!(cursor.segment_bucket_ms, seg1_bucket);
    }

    // Sequence values match
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
