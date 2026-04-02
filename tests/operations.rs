use std::fs;

use dotflow::operations::{
    delete_flow_type, purge_event_type, truncate_event_type, truncate_flow_type,
};
use dotflow::{EventRecord, SegmentedReader, SegmentedWriter};

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
            w.append(event_type, ts(hour, j), format!("h{hour}e{j}t{event_type}").into_bytes())
                .unwrap();
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
            w.append(0, ts(0, j), b"data".to_vec()).unwrap();
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
