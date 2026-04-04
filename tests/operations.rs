use std::fs;

use dotflow::operations::{
    compact_flow_type, delete_flow_type, purge_event_type, truncate_event_type,
    truncate_flow_type, CompactOptions,
};
use dotflow::event::EventRecord;
use dotflow::{SegmentedReader, SegmentedWriter};

const HOUR_MS: u64 = 3_600_000;
const HOUR_NS: u64 = 3_600_000_000_000;
const BASE_NS: u64 = 1_743_292_800_000_000_000;

fn ts(hour: u64, offset: u64) -> u64 {
    BASE_NS + hour * HOUR_NS + offset * 1_000_000_000
}

fn test_dir(name: &str) -> String {
    format!("test_ops_{name}.flowtype")
}

fn cleanup(dir: &str) {
    let _ = fs::remove_dir_all(dir);
}

fn write_events(dir: &str, event_count: u64, hours: u64) {
    let mut w = SegmentedWriter::create(dir, "test.v0", 3, HOUR_MS, 50).unwrap();
    for hour in 0..hours {
        for j in 0..event_count / hours {
            let event_type = (j % 3) as u16;
            let payload = format!("h{hour}e{j}t{event_type}").into_bytes();
            w.append(EventRecord::new(event_type, ts(hour, j), payload)).unwrap();
        }
    }
    w.flush().unwrap();
}

fn read_all(dir: &str) -> Vec<EventRecord> {
    let reader = SegmentedReader::open(dir).unwrap();
    reader.into_iter().map(|r| r.unwrap().0).collect()
}

// ── delete_flow_type ──

#[test]
fn delete_removes_directory() {
    let dir = test_dir("delete");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    assert!(fs::metadata(&dir).is_ok());
    delete_flow_type(&dir).unwrap();
    assert!(fs::metadata(&dir).is_err(), "directory should be gone");
}

#[test]
fn delete_nonexistent_errors() {
    let result = delete_flow_type("definitely_does_not_exist.flowtype");
    assert!(result.is_err());
}

// ── truncate_flow_type ──

#[test]
fn truncate_clears_all_events() {
    let dir = test_dir("truncate_all");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    assert_eq!(read_all(&dir).len(), 30);

    truncate_flow_type(&dir).unwrap();

    assert_eq!(read_all(&dir).len(), 0);

    // Directory and manifest still exist
    assert!(fs::metadata(&dir).is_ok());
    let store = dotflow::FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 0);
    assert_eq!(store.manifest.flow_type, "test.v0");

    cleanup(&dir);
}

#[test]
fn truncate_removes_segment_files() {
    let dir = test_dir("truncate_files");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let store = dotflow::FlowStore::open(&dir).unwrap();
    assert_eq!(store.manifest.segments.len(), 3);

    truncate_flow_type(&dir).unwrap();

    // Check no .flow files remain
    let remaining: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .collect();
    assert!(remaining.is_empty(), "no .flow files should remain");

    cleanup(&dir);
}

// ── purge_event_type ──

#[test]
fn purge_removes_all_events_of_type() {
    let dir = test_dir("purge_basic");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let before = read_all(&dir);
    let type1_count = before.iter().filter(|e| e.event_type_id == 1).count();
    assert!(type1_count > 0);

    let stats = purge_event_type(&dir, 1).unwrap();
    assert_eq!(stats.events_removed, type1_count as u64);

    let after = read_all(&dir);
    assert_eq!(after.len(), before.len() - type1_count);
    assert!(after.iter().all(|e| e.event_type_id != 1));

    cleanup(&dir);
}

#[test]
fn purge_preserves_other_event_types() {
    let dir = test_dir("purge_preserve");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let before_type0: Vec<_> = read_all(&dir).into_iter().filter(|e| e.event_type_id == 0).collect();
    let before_type2: Vec<_> = read_all(&dir).into_iter().filter(|e| e.event_type_id == 2).collect();

    purge_event_type(&dir, 1).unwrap();

    let after_type0: Vec<_> = read_all(&dir).into_iter().filter(|e| e.event_type_id == 0).collect();
    let after_type2: Vec<_> = read_all(&dir).into_iter().filter(|e| e.event_type_id == 2).collect();

    assert_eq!(before_type0.len(), after_type0.len());
    assert_eq!(before_type2.len(), after_type2.len());

    cleanup(&dir);
}

#[test]
fn purge_preserves_global_order() {
    let dir = test_dir("purge_order");
    cleanup(&dir);
    write_events(&dir, 30, 3);
    purge_event_type(&dir, 1).unwrap();

    let events = read_all(&dir);
    let mut prev = None;
    for ev in &events {
        if let Some(p) = prev {
            assert!(ev.sequence > p, "order not preserved after purge");
        }
        prev = Some(ev.sequence);
    }

    cleanup(&dir);
}

#[test]
fn purge_updates_manifest() {
    let dir = test_dir("purge_manifest");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let before_total: u64 = dotflow::FlowStore::open(&dir)
        .unwrap()
        .manifest
        .segments
        .iter()
        .map(|s| s.event_count)
        .sum();

    purge_event_type(&dir, 1).unwrap();

    let store = dotflow::FlowStore::open(&dir).unwrap();
    let after_total: u64 = store.manifest.segments.iter().map(|s| s.event_count).sum();

    assert!(after_total < before_total);

    // Manifest byte sizes should be updated
    for seg in store.segments() {
        let actual_size = fs::metadata(store.dir().join(&seg.file)).unwrap().len();
        assert_eq!(actual_size, seg.byte_size);
    }

    cleanup(&dir);
}

#[test]
fn purge_nonexistent_type_is_unchanged() {
    let dir = test_dir("purge_noop");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let before = read_all(&dir);
    let stats = purge_event_type(&dir, 99).unwrap(); // type 99 doesn't exist

    assert_eq!(stats.events_removed, 0);
    assert_eq!(stats.segments_rewritten, 0);

    let after = read_all(&dir);
    assert_eq!(before.len(), after.len());

    cleanup(&dir);
}

#[test]
fn purge_can_empty_a_segment() {
    let dir = test_dir("purge_empty_seg");
    cleanup(&dir);

    // Write one segment with only type 0 events
    {
        let mut w = SegmentedWriter::create(&dir, "test.v0", 1, HOUR_MS, 50).unwrap();
        for j in 0..10u64 {
            w.append(EventRecord::new(0, ts(0, j), b"data".to_vec())).unwrap();
        }
        w.flush().unwrap();
    }

    purge_event_type(&dir, 0).unwrap();

    let store = dotflow::FlowStore::open(&dir).unwrap();
    // Segment should be removed from manifest since it's now empty
    assert_eq!(store.manifest.segments.len(), 0);

    cleanup(&dir);
}

// ── truncate_event_type ──

#[test]
fn truncate_event_type_removes_after_sequence() {
    let dir = test_dir("trunc_type");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let all = read_all(&dir);
    let cutoff_seq = all[all.len() / 2].sequence; // halfway through

    let stats = truncate_event_type(&dir, 1, cutoff_seq).unwrap();

    let after = read_all(&dir);

    // All type 1 events at or after cutoff should be gone
    for ev in &after {
        if ev.event_type_id == 1 {
            assert!(
                ev.sequence < cutoff_seq,
                "type 1 event at seq {} should have been removed (cutoff {})",
                ev.sequence,
                cutoff_seq
            );
        }
    }

    // Type 1 events before cutoff should be kept
    let before_cutoff_type1 = all
        .iter()
        .filter(|e| e.event_type_id == 1 && e.sequence < cutoff_seq)
        .count();
    let after_type1 = after.iter().filter(|e| e.event_type_id == 1).count();
    assert_eq!(before_cutoff_type1, after_type1);

    assert!(stats.events_removed > 0);

    cleanup(&dir);
}

#[test]
fn truncate_event_type_keeps_other_types_intact() {
    let dir = test_dir("trunc_type_others");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    let before = read_all(&dir);
    let type0_before: Vec<_> = before.iter().filter(|e| e.event_type_id == 0).cloned().collect();
    let type2_before: Vec<_> = before.iter().filter(|e| e.event_type_id == 2).cloned().collect();

    let cutoff = before[5].sequence;
    truncate_event_type(&dir, 1, cutoff).unwrap();

    let after = read_all(&dir);
    let type0_after: Vec<_> = after.iter().filter(|e| e.event_type_id == 0).cloned().collect();
    let type2_after: Vec<_> = after.iter().filter(|e| e.event_type_id == 2).cloned().collect();

    assert_eq!(type0_before, type0_after);
    assert_eq!(type2_before, type2_after);

    cleanup(&dir);
}

#[test]
fn truncate_event_type_at_zero_removes_all() {
    let dir = test_dir("trunc_type_zero");
    cleanup(&dir);
    write_events(&dir, 30, 3);

    truncate_event_type(&dir, 1, 0).unwrap();

    let after = read_all(&dir);
    assert!(after.iter().all(|e| e.event_type_id != 1));

    cleanup(&dir);
}

// ── Schema + operations compatibility ─────────────────────────────────────────

fn write_schema_events(dir: &str, n: u64) {
    let mut w = SegmentedWriter::create(dir, "food-item", 2, HOUR_MS, 50).unwrap();
    for i in 0..n {
        let et = (i % 2) as u16;
        let payload = if et == 0 {
            format!(r#"{{"id":"{i:036}","name":"item-{i}","price":{:.2}}}"#, i as f64 * 0.5 + 1.0)
        } else {
            format!(r#"{{"id":"{i:036}","active":true}}"#)
        };
        let mut ev = EventRecord::new(et, ts(i / (n / 4), i % (n / 4) * 1_000_000_000), payload.into_bytes());
        ev.event_id = [i as u8; 16];
        w.append(ev).unwrap();
    }
}

#[test]
fn purge_schema_encoded_event_type_preserves_others() {
    let dir = test_dir("schema_purge");
    cleanup(&dir);
    write_schema_events(&dir, 40);

    // Record type-1 payloads before purge
    let before: Vec<_> = read_all(&dir)
        .into_iter()
        .filter(|e| e.event_type_id == 1)
        .collect();
    assert!(!before.is_empty());

    purge_event_type(&dir, 0).unwrap();

    let after = read_all(&dir);
    // Type 0 must be gone
    assert!(after.iter().all(|e| e.event_type_id != 0));
    // Type 1 must survive and payload must still be valid JSON
    let type1_after: Vec<_> = after.into_iter().filter(|e| e.event_type_id == 1).collect();
    assert_eq!(before.len(), type1_after.len());
    for ev in &type1_after {
        let val: serde_json::Value = serde_json::from_slice(&ev.payload)
            .expect("payload must be valid JSON after purge");
        assert!(val.get("id").is_some(), "id field must be present");
    }

    cleanup(&dir);
}

#[test]
fn compact_schema_encoded_achieves_better_compression() {
    let dir = test_dir("schema_compact");
    cleanup(&dir);

    // Write 200 events with realistic JSON payloads
    {
        let mut w = SegmentedWriter::create(&dir, "food-item", 1, HOUR_MS, 50).unwrap();
        for i in 0u64..200 {
            let payload = format!(
                r#"{{"id":"{i:036}","userId":"user_{i:020}","name":"Item {i}","price":{:.2},"available":true}}"#,
                i as f64 * 0.5 + 1.0
            );
            let mut ev = EventRecord::new(0, ts(0, i * 60_000_000), payload.into_bytes());
            ev.event_id = [i as u8; 16];
            w.append(ev).unwrap();
        }
    }

    // Measure size before compact
    let before_size: u64 = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .map(|e| e.metadata().unwrap().len())
        .sum();

    // Compact with dictionary training
    let stats = compact_flow_type(&dir, CompactOptions { train_dicts: true, level: 3, dict_size: 32_768 })
        .unwrap();

    let after_size: u64 = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .map(|e| e.metadata().unwrap().len())
        .sum();

    assert!(
        after_size < before_size,
        "compact should reduce size: before={before_size}, after={after_size}"
    );
    assert!(stats.blocks_compressed > 0, "at least some blocks should be compressed");

    // Read back -- all events must still decode correctly
    let events = read_all(&dir);
    assert_eq!(events.len(), 200);
    for ev in &events {
        let val: serde_json::Value = serde_json::from_slice(&ev.payload)
            .expect("payload must be valid JSON after compact");
        assert!(val.get("id").is_some());
        assert!(val.get("price").is_some());
    }

    cleanup(&dir);
}
