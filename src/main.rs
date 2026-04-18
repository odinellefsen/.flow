use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use dotflow::event::EventRecord;
use dotflow::recovery;
use dotflow::{FlowReader, FlowWriter};

const PATH: &str = "demo.flow";

const EVENT_USER_CREATED: u16 = 0;
const EVENT_EMAIL_UPDATED: u16 = 1;
const EVENT_USER_DELETED: u16 = 2;

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn main() -> io::Result<()> {
    println!("=== Writing events ===");
    let mut writer = FlowWriter::create(PATH, 3, 4)?;

    let s0 = writer.append(EventRecord::new(
        EVENT_USER_CREATED,
        now_nanos(),
        b"alice".to_vec(),
    ))?;
    writer.append(EventRecord::new(
        EVENT_EMAIL_UPDATED,
        now_nanos(),
        b"alice@new.com".to_vec(),
    ))?;
    writer.append(EventRecord::new(
        EVENT_USER_CREATED,
        now_nanos(),
        b"bob".to_vec(),
    ))?;
    writer.append(EventRecord::new(
        EVENT_USER_DELETED,
        now_nanos(),
        b"alice".to_vec(),
    ))?;
    // block_size=4, so the above 4 events auto-flush as block 1

    writer.append(EventRecord::new(
        EVENT_USER_CREATED,
        now_nanos(),
        b"charlie".to_vec(),
    ))?;
    let s5 = writer.append(EventRecord::new(
        EVENT_EMAIL_UPDATED,
        now_nanos(),
        b"bob@new.com".to_vec(),
    ))?;
    writer.flush()?; // explicit flush for the remaining 2 events as block 2

    println!("Wrote 6 events (seq {s0}..{s5})");

    drop(writer);

    // --- Read all ---
    println!("\n=== Reading all events ===");
    let reader = FlowReader::open(PATH)?;
    for result in reader.into_iter() {
        let event = result?;
        let payload = String::from_utf8_lossy(&event.payload);
        println!(
            "  seq={} type={} event_time={} payload={:?}",
            event.sequence, event.event_type_id, event.event_time, payload
        );
    }

    // --- Filtered read (only user.created events) ---
    println!("\n=== Filtered read: only EVENT_USER_CREATED (type 0) ===");
    let reader = FlowReader::open(PATH)?.with_filter(&[EVENT_USER_CREATED]);
    for result in reader.into_iter() {
        let event = result?;
        let payload = String::from_utf8_lossy(&event.payload);
        println!(
            "  seq={} type={} payload={:?}",
            event.sequence, event.event_type_id, payload
        );
    }

    // --- Recovery info ---
    println!("\n=== Recovery validation ===");
    let prefix = recovery::validate_file(PATH)?;
    println!("  Valid blocks: {}", prefix.block_count);
    println!("  Valid events: {}", prefix.event_count);
    println!("  Next sequence: {}", prefix.next_sequence);
    println!("  Valid end byte: {}", prefix.valid_end);

    // --- Reopen and append ---
    println!("\n=== Reopen and append ===");
    let mut writer = FlowWriter::open(PATH, 4)?;
    let mut new_event = EventRecord::new(
        EVENT_EMAIL_UPDATED,
        now_nanos(),
        b"charlie@new.com".to_vec(),
    );
    new_event.metadata = vec![
        ("producer/name".to_string(), "demo".to_string()),
        ("notify-on/stored-event".to_string(), "true".to_string()),
    ];
    let s6 = writer.append(new_event)?;
    writer.flush()?;
    println!("Appended 1 more event (seq {s6})");
    drop(writer);

    println!("\n=== Reading all events after reopen ===");
    let reader = FlowReader::open(PATH)?;
    let mut count = 0;
    for result in reader.into_iter() {
        let event = result?;
        let payload = String::from_utf8_lossy(&event.payload);
        println!(
            "  seq={} type={} meta={:?} payload={:?}",
            event.sequence, event.event_type_id, event.metadata, payload
        );
        count += 1;
    }
    println!("Total events: {count}");

    Ok(())
}
