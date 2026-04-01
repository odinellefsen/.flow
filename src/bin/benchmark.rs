use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

use dotflow::{FlowReader, FlowWriter};

const EVENT_COUNT: u64 = 100_000;
const BLOCK_SIZE: usize = 1_000;
const FLOW_PATH: &str = "bench.flow";
const JSON_PATH: &str = "bench.jsonl";

fn main() -> io::Result<()> {
    let names = [
        "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "hank",
    ];

    println!("=== Benchmark: .flow binary vs .jsonl ===");
    println!("{EVENT_COUNT} events, 3 event types\n");

    // ── Write: .flow ──
    let start = Instant::now();
    {
        let mut writer = FlowWriter::create(FLOW_PATH, 3, BLOCK_SIZE)?;
        for i in 0..EVENT_COUNT {
            let event_type = (i % 3) as u16;
            let name = names[(i as usize) % names.len()];
            let payload = format!(r#"{{"user":"{}","id":{},"action":"test"}}"#, name, i);
            writer.append(event_type, i * 1_000_000, payload.into_bytes())?;
        }
        writer.flush()?;
    }
    let flow_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let flow_size = fs::metadata(FLOW_PATH)?.len();

    // ── Write: JSONL ──
    let start = Instant::now();
    {
        let file = File::create(JSON_PATH)?;
        let mut writer = BufWriter::new(file);
        for i in 0..EVENT_COUNT {
            let event_type = (i % 3) as u16;
            let name = names[(i as usize) % names.len()];
            writeln!(
                writer,
                r#"{{"sequence":{},"event_type_id":{},"timestamp":{},"payload":{{"user":"{}","id":{},"action":"test"}}}}"#,
                i, event_type, i * 1_000_000, name, i
            )?;
        }
        writer.flush()?;
        writer.get_ref().sync_data()?;
    }
    let json_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let json_size = fs::metadata(JSON_PATH)?.len();

    println!("WRITE");
    println!(
        "  .flow:  {:>7.1} ms  ({:.2} MB)",
        flow_write_ms,
        flow_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  .jsonl: {:>7.1} ms  ({:.2} MB)",
        json_write_ms,
        json_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  .flow is {:.1}x smaller",
        json_size as f64 / flow_size as f64
    );
    println!();

    // ── Read: .flow (full scan, decode every event) ──
    let start = Instant::now();
    let mut flow_read_count: u64 = 0;
    {
        let reader = FlowReader::open(FLOW_PATH)?;
        for result in reader.into_iter() {
            let event = result?;
            std::hint::black_box(&event.payload);
            std::hint::black_box(event.sequence);
            std::hint::black_box(event.event_type_id);
            flow_read_count += 1;
        }
    }
    let flow_read_ms = start.elapsed().as_secs_f64() * 1000.0;

    // ── Read: .flow filtered (only type 0) ──
    let start = Instant::now();
    let mut flow_filtered_count: u64 = 0;
    {
        let reader = FlowReader::open(FLOW_PATH)?.with_filter(&[0]);
        for result in reader.into_iter() {
            let event = result?;
            std::hint::black_box(&event.payload);
            flow_filtered_count += 1;
        }
    }
    let flow_filtered_ms = start.elapsed().as_secs_f64() * 1000.0;

    // ── Read: JSONL (full scan, parse every line) ──
    let start = Instant::now();
    let mut json_read_count: u64 = 0;
    {
        let file = File::open(JSON_PATH)?;
        let reader = BufReader::with_capacity(256 * 1024, file);
        for line in reader.lines() {
            let line = line?;
            let v: serde_json::Value = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            std::hint::black_box(&v["payload"]);
            std::hint::black_box(&v["sequence"]);
            std::hint::black_box(&v["event_type_id"]);
            json_read_count += 1;
        }
    }
    let json_read_ms = start.elapsed().as_secs_f64() * 1000.0;

    // ── Read: JSONL filtered (only type 0, must parse every line to check) ──
    let start = Instant::now();
    let mut json_filtered_count: u64 = 0;
    {
        let file = File::open(JSON_PATH)?;
        let reader = BufReader::with_capacity(256 * 1024, file);
        for line in reader.lines() {
            let line = line?;
            let v: serde_json::Value = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if v["event_type_id"].as_u64() == Some(0) {
                std::hint::black_box(&v["payload"]);
                json_filtered_count += 1;
            }
        }
    }
    let json_filtered_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("READ (full scan, all events)");
    println!(
        "  .flow:  {:>7.1} ms  ({flow_read_count} events, {:.0} events/sec)",
        flow_read_ms,
        flow_read_count as f64 / (flow_read_ms / 1000.0)
    );
    println!(
        "  .jsonl: {:>7.1} ms  ({json_read_count} events, {:.0} events/sec)",
        json_read_ms,
        json_read_count as f64 / (json_read_ms / 1000.0)
    );
    println!("  .flow is {:.1}x faster", json_read_ms / flow_read_ms);
    println!();

    println!("READ (filtered, only type 0)");
    println!(
        "  .flow:  {:>7.1} ms  ({flow_filtered_count} events matched)",
        flow_filtered_ms,
    );
    println!(
        "  .jsonl: {:>7.1} ms  ({json_filtered_count} events matched)",
        json_filtered_ms,
    );
    println!(
        "  .flow is {:.1}x faster",
        json_filtered_ms / flow_filtered_ms
    );

    let _ = fs::remove_file(FLOW_PATH);
    let _ = fs::remove_file(JSON_PATH);

    Ok(())
}
