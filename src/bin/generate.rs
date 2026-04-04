use std::io;
use std::time::Instant;

use dotflow::event::EventRecord;
use dotflow::SegmentedWriter;

const DIR: &str = "benchmark.flowtype";
const FLOW_TYPE: &str = "food-item";
const EVENT_COUNT: u64 = 100_000;
const BLOCK_SIZE: usize = 1_000;
const EVENT_TYPE_COUNT: u16 = 3;
const BUCKET_DURATION_MS: u64 = 3_600_000; // 1 hour

// Mirrors real Flowcore event type names (Event Types carry the version, not the Flow Type)
const FOOD_ITEM_CREATED: u16 = 0;
const FOOD_ITEM_UPDATED: u16 = 1;
const FOOD_ITEM_DELETED: u16 = 2;

// Realistic category data representative of a food delivery platform
const CATEGORIES: &[&str] = &[
    "Fast Food", "American", "Italian", "Japanese", "Chinese", "Mexican",
    "Indian", "Thai", "Mediterranean", "Vegan",
];

const NAMES: &[&str] = &[
    "Classic Burger", "Margherita Pizza", "Sushi Roll", "Pad Thai",
    "Chicken Tikka", "Beef Burrito", "Caesar Salad", "Chocolate Brownie",
    "Vegan Bowl", "Fish & Chips",
];

const USER_IDS: &[&str] = &[
    "user_30eqFgTWfuBysprgdMgwvjEezH2",
    "user_7bNkQrVxWzAoTtpsMmEjLlKiPhGn",
    "user_4xYfRsSuDoImPqHcNvBgJwZlEkCt",
    "user_9mLoKpDcAsEtFiQhUwBxNjGrVyZo",
    "user_2nWqXpYvLiKtMsOgDhCzBrAuEfJn",
];

fn uuid(i: u64) -> String {
    // Deterministic UUID-looking string derived from index
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (i & 0xFFFF_FFFF) as u32,
        ((i >> 32) & 0xFFFF) as u16,
        0x1234u16,
        0x9000u16 | ((i >> 48) & 0x3FFF) as u16,
        i & 0xFFFF_FFFF_FFFF,
    )
}

fn created_payload(i: u64) -> String {
    let name = NAMES[(i as usize) % NAMES.len()];
    let user_id = USER_IDS[(i as usize) % USER_IDS.len()];
    let cat1 = CATEGORIES[(i as usize) % CATEGORIES.len()];
    let cat2 = CATEGORIES[(i as usize + 1) % CATEGORIES.len()];
    let price = 3.99 + (i % 20) as f64 * 0.5;
    let available = i % 7 != 0;

    format!(
        r#"{{"id":"{id}","userId":"{user_id}","name":"{name}","categoryHierarchy":["{cat1}","{cat2}"],"price":{price:.2},"available":{available}}}"#,
        id = uuid(i),
    )
}

fn updated_payload(i: u64) -> String {
    let user_id = USER_IDS[(i as usize) % USER_IDS.len()];
    let price = 4.99 + (i % 15) as f64 * 0.5;
    let available = i % 5 != 0;

    format!(
        r#"{{"id":"{id}","userId":"{user_id}","price":{price:.2},"available":{available}}}"#,
        id = uuid(i),
    )
}

fn deleted_payload(i: u64) -> String {
    let user_id = USER_IDS[(i as usize) % USER_IDS.len()];
    let reasons = ["discontinued", "duplicate", "cleanup", "expired"];
    let reason = reasons[(i as usize) % reasons.len()];

    format!(
        r#"{{"id":"{id}","userId":"{user_id}","reason":"{reason}"}}"#,
        id = uuid(i),
    )
}

fn main() -> io::Result<()> {
    let _ = std::fs::remove_dir_all(DIR);

    println!("Generating {EVENT_COUNT} events into {DIR}/");
    println!("  Flow type:  {FLOW_TYPE}");
    println!("  Bucket duration: 1 hour");
    println!("  Block size: {BLOCK_SIZE} events");
    println!("  Event types: food-item.created.v0, food-item.updated.v0, food-item.deleted.v0");
    let start = Instant::now();

    let mut writer =
        SegmentedWriter::create(DIR, FLOW_TYPE, EVENT_TYPE_COUNT, BUCKET_DURATION_MS, BLOCK_SIZE)?;

    // Register human-readable event type names in the manifest
    {
        let store = writer.store_mut();
        store.register_event_type(FOOD_ITEM_CREATED, "food-item.created.v0");
        store.register_event_type(FOOD_ITEM_UPDATED, "food-item.updated.v0");
        store.register_event_type(FOOD_ITEM_DELETED, "food-item.deleted.v0");
        store.save_manifest()?;
    }

    // Spread events over 4 hours so we get multiple segments
    let base_ts_ns: u64 = 1_743_292_800_000_000_000; // 2025-03-30 00:00:00 UTC
    let hour_ns: u64 = 3_600_000_000_000;
    let interval_ns: u64 = (4 * hour_ns) / EVENT_COUNT;

    for i in 0..EVENT_COUNT {
        let event_type = match i % 10 {
            0..=5 => FOOD_ITEM_CREATED,
            6..=8 => FOOD_ITEM_UPDATED,
            _ => FOOD_ITEM_DELETED,
        };
        let timestamp = base_ts_ns + i * interval_ns;

        let payload = match event_type {
            FOOD_ITEM_CREATED => created_payload(i),
            FOOD_ITEM_UPDATED => updated_payload(i),
            _ => deleted_payload(i),
        };

        let mut event = EventRecord::new(event_type, timestamp, payload.into_bytes());
        event.event_id = uuid(i).as_bytes()[..16].try_into().unwrap_or([0u8; 16]);
        writer.append(event)?;
    }

    writer.flush()?;

    let store = writer.store();
    let segment_count = store.manifest.segments.len();
    let total_bytes: u64 = store.manifest.segments.iter().map(|s| s.byte_size).sum();
    let schema_count = store.manifest.event_types.iter().filter(|e| e.schema.is_some()).count();

    drop(writer);

    let elapsed = start.elapsed();
    let events_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("\nDone in {:.2?}", elapsed);
    println!("{EVENT_COUNT} events, {events_per_sec:.0} events/sec");
    println!("{segment_count} segments, {schema_count} event types with locked schema");
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

    // Show locked schemas
    println!("\nLocked schemas:");
    for et in &store.manifest.event_types {
        if let Some(schema) = &et.schema {
            let fields: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            println!("  [{}] {} → {}", et.id, et.name, fields.join(", "));
        }
    }

    Ok(())
}
