use std::io;
use std::time::Instant;

use dotflow::event::EventRecord;
use dotflow::SegmentedWriter;

const DIR: &str = "benchmark.flowtype";
const FLOW_TYPE: &str = "user.v0";
const EVENT_COUNT: u64 = 100_000;
const BLOCK_SIZE: usize = 1_000;
const EVENT_TYPE_COUNT: u16 = 3;
const BUCKET_DURATION_MS: u64 = 3_600_000; // 1 hour

const USER_CREATED: u16 = 0;
const EMAIL_UPDATED: u16 = 1;
const USER_DELETED: u16 = 2;

fn main() -> io::Result<()> {
    let _ = std::fs::remove_dir_all(DIR);

    println!("Generating {EVENT_COUNT} events into {DIR}/");
    println!("  Flow type: {FLOW_TYPE}");
    println!("  Bucket duration: 1 hour");
    println!("  Block size: {BLOCK_SIZE} events");
    let start = Instant::now();

    let mut writer =
        SegmentedWriter::create(DIR, FLOW_TYPE, EVENT_TYPE_COUNT, BUCKET_DURATION_MS, BLOCK_SIZE)?;

    let names = [
        "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "hank",
    ];

    // Simulate events spread over 4 hours (so we get multiple segments)
    let base_ts_ns: u64 = 1_743_292_800_000_000_000; // 2025-03-30 00:00:00 UTC in nanos
    let hour_ns: u64 = 3_600_000_000_000;
    let interval_ns: u64 = (4 * hour_ns) / EVENT_COUNT;

    for i in 0..EVENT_COUNT {
        let event_type = match i % 10 {
            0..=5 => USER_CREATED,
            6..=8 => EMAIL_UPDATED,
            _ => USER_DELETED,
        };

        let name = names[(i as usize) % names.len()];
        let timestamp = base_ts_ns + i * interval_ns;

        let payload = match event_type {
            USER_CREATED => format!(r#"{{"user":"{}","id":{}}}"#, name, i),
            EMAIL_UPDATED => {
                format!(r#"{{"user":"{}","email":"{}@example.com"}}"#, name, name)
            }
            USER_DELETED => format!(r#"{{"user":"{}","reason":"cleanup"}}"#, name),
            _ => unreachable!(),
        };

        writer.append(EventRecord::new(event_type, timestamp, payload.into_bytes()))?;
    }

    writer.flush()?;

    let store = writer.store();
    let segment_count = store.manifest.segments.len();
    let total_bytes: u64 = store.manifest.segments.iter().map(|s| s.byte_size).sum();

    drop(writer);

    let elapsed = start.elapsed();
    let events_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("\nDone in {:.2?}", elapsed);
    println!("{EVENT_COUNT} events, {events_per_sec:.0} events/sec");
    println!("{segment_count} segments");
    println!(
        "Total size: {:.2} MB ({total_bytes} bytes)",
        total_bytes as f64 / (1024.0 * 1024.0)
    );

    // Show segment breakdown
    let store = dotflow::FlowStore::open(DIR)?;
    for seg in store.segments() {
        println!(
            "  {} | seq {}-{} | {} events | {:.2} KB",
            seg.file,
            seg.start_sequence,
            seg.end_sequence,
            seg.event_count,
            seg.byte_size as f64 / 1024.0
        );
    }

    Ok(())
}
