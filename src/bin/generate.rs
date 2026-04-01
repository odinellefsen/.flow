use std::io;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use dotflow::FlowWriter;

const PATH: &str = "benchmark.flow";
const EVENT_COUNT: u64 = 100_000;
const BLOCK_SIZE: usize = 1_000;
const EVENT_TYPE_COUNT: u16 = 3;

const USER_CREATED: u16 = 0;
const EMAIL_UPDATED: u16 = 1;
const USER_DELETED: u16 = 2;

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn main() -> io::Result<()> {
    println!("Generating {EVENT_COUNT} events into {PATH}...");
    let start = Instant::now();

    let mut writer = FlowWriter::create(PATH, EVENT_TYPE_COUNT, BLOCK_SIZE)?;

    let names = ["alice", "bob", "charlie", "diana", "eve", "frank", "grace", "hank"];

    for i in 0..EVENT_COUNT {
        let event_type = match i % 10 {
            0..=5 => USER_CREATED,
            6..=8 => EMAIL_UPDATED,
            _ => USER_DELETED,
        };

        let name = names[(i as usize) % names.len()];
        let payload = match event_type {
            USER_CREATED => format!(r#"{{"user":"{}","id":{}}}"#, name, i),
            EMAIL_UPDATED => format!(r#"{{"user":"{}","email":"{}@example.com"}}"#, name, name),
            USER_DELETED => format!(r#"{{"user":"{}","reason":"cleanup"}}"#, name),
            _ => unreachable!(),
        };

        writer.append(event_type, now_nanos(), payload.into_bytes())?;
    }

    writer.flush()?;
    drop(writer);

    let elapsed = start.elapsed();
    let events_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("Done in {:.2?}", elapsed);
    println!(
        "{EVENT_COUNT} events, {:.0} events/sec",
        events_per_sec
    );

    let file_size = std::fs::metadata(PATH)?.len();
    println!(
        "File size: {:.2} MB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0),
        file_size
    );

    Ok(())
}
