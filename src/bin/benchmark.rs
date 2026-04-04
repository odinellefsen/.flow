use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

use dotflow::event::EventRecord;
use dotflow::{FlowReader, FlowStore, FlowWriter, SegmentedReader, SegmentedWriter};

const EVENT_COUNT: u64 = 100_000;
const BLOCK_SIZE: usize = 1_000;

// Low-level file paths for raw-binary and JSONL comparisons
const FLOW_RAW_PATH: &str = "bench_raw.flow";
const JSON_PATH: &str = "bench.jsonl";

// SegmentedWriter dirs for schema-encoded comparison
const DIR_SCHEMA: &str = "bench_schema.flowtype";

const FOOD_ITEM_CREATED: u16 = 0;
const FOOD_ITEM_UPDATED: u16 = 1;

fn uuid(i: u64) -> String {
    format!(
        "{:08x}-{:04x}-1234-9000-{:012x}",
        (i & 0xFFFF_FFFF) as u32,
        ((i >> 32) & 0xFFFF) as u16,
        i & 0xFFFF_FFFF_FFFF,
    )
}

fn created_payload(i: u64) -> String {
    let names = ["Classic Burger", "Margherita Pizza", "Sushi Roll", "Pad Thai", "Chicken Tikka"];
    let cats = ["Fast Food", "Italian", "Japanese", "Thai", "Indian"];
    let user_ids = [
        "user_30eqFgTWfuBysprgdMgwvjEezH2",
        "user_7bNkQrVxWzAoTtpsMmEjLlKiPhGn",
    ];
    let name = names[(i as usize) % names.len()];
    let cat = cats[(i as usize) % cats.len()];
    let user_id = user_ids[(i as usize) % user_ids.len()];
    let price = 3.99 + (i % 20) as f64 * 0.5;
    format!(
        r#"{{"id":"{id}","userId":"{user_id}","name":"{name}","categoryHierarchy":["{cat}","Burgers"],"price":{price:.2},"available":true}}"#,
        id = uuid(i)
    )
}

fn updated_payload(i: u64) -> String {
    let user_ids = [
        "user_30eqFgTWfuBysprgdMgwvjEezH2",
        "user_7bNkQrVxWzAoTtpsMmEjLlKiPhGn",
    ];
    let user_id = user_ids[(i as usize) % user_ids.len()];
    let price = 4.99 + (i % 15) as f64 * 0.5;
    format!(
        r#"{{"id":"{id}","userId":"{user_id}","price":{price:.2},"available":false}}"#,
        id = uuid(i)
    )
}

fn base_ts(i: u64) -> u64 {
    1_743_292_800_000_000_000u64 + i * (4 * 3_600_000_000_000u64 / EVENT_COUNT)
}

fn event_type(i: u64) -> u16 {
    if i % 10 < 7 { FOOD_ITEM_CREATED } else { FOOD_ITEM_UPDATED }
}

fn payload_for(i: u64) -> String {
    if event_type(i) == FOOD_ITEM_CREATED { created_payload(i) } else { updated_payload(i) }
}

fn section(title: &str) {
    println!("\n── {title} ──");
}

fn main() -> io::Result<()> {
    println!("=== Benchmark: raw .flow vs schema-encoded .flow vs .jsonl ===");
    println!("{EVENT_COUNT} events, 2 event types (food-item.created, food-item.updated)\n");

    // ── Write: raw .flow (no schema encoding) ──────────────────────────────
    section("WRITE");
    let start = Instant::now();
    {
        let mut w = FlowWriter::create(FLOW_RAW_PATH, 2, BLOCK_SIZE)?;
        for i in 0..EVENT_COUNT {
            let payload = payload_for(i);
            w.append(EventRecord::new(event_type(i), base_ts(i), payload.into_bytes()))?;
        }
        w.flush()?;
    }
    let raw_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let raw_size = fs::metadata(FLOW_RAW_PATH)?.len();

    // ── Write: schema-encoded .flow ────────────────────────────────────────
    let _ = fs::remove_dir_all(DIR_SCHEMA);
    let start = Instant::now();
    {
        let mut w =
            SegmentedWriter::create(DIR_SCHEMA, "food-item", 2, 3_600_000, BLOCK_SIZE)?;
        for i in 0..EVENT_COUNT {
            let payload = payload_for(i);
            w.append(EventRecord::new(event_type(i), base_ts(i), payload.into_bytes()))?;
        }
        w.flush()?;
    }
    let schema_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let schema_size: u64 = fs::read_dir(DIR_SCHEMA)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "flow").unwrap_or(false))
        .map(|e| e.metadata().unwrap().len())
        .sum();

    // ── Write: JSONL ───────────────────────────────────────────────────────
    let start = Instant::now();
    {
        let f = File::create(JSON_PATH)?;
        let mut bw = BufWriter::new(f);
        for i in 0..EVENT_COUNT {
            let payload = payload_for(i);
            writeln!(
                bw,
                r#"{{"seq":{i},"type":{},"ts":{},"payload":{payload}}}"#,
                event_type(i),
                base_ts(i)
            )?;
        }
        bw.flush()?;
    }
    let json_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let json_size = fs::metadata(JSON_PATH)?.len();

    let mb = |b: u64| b as f64 / (1024.0 * 1024.0);

    println!(
        "  raw .flow:    {:>7.1} ms  {:.2} MB",
        raw_write_ms,
        mb(raw_size)
    );
    println!(
        "  schema .flow: {:>7.1} ms  {:.2} MB  ({:.1}x smaller than raw .flow)",
        schema_write_ms,
        mb(schema_size),
        raw_size as f64 / schema_size as f64
    );
    println!(
        "  .jsonl:       {:>7.1} ms  {:.2} MB  ({:.1}x larger than raw .flow)",
        json_write_ms,
        mb(json_size),
        json_size as f64 / raw_size as f64
    );

    // ── Show locked schemas ────────────────────────────────────────────────
    section("LOCKED SCHEMAS");
    let store = FlowStore::open(DIR_SCHEMA)?;
    for et in &store.manifest.event_types {
        if let Some(schema) = &et.schema {
            let fields: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            println!("  type {} → [{}]", et.id, fields.join(", "));
        }
    }
    let schema_field_count: usize = store
        .manifest
        .event_types
        .iter()
        .filter_map(|e| e.schema.as_ref())
        .map(|s| s.fields.len())
        .sum();
    println!(
        "  {schema_field_count} total fields stored once in manifest (not per event)"
    );

    // ── Read: full scan ───────────────────────────────────────────────────
    section("READ (full scan, all events)");

    let start = Instant::now();
    let mut raw_count = 0u64;
    {
        let reader = FlowReader::open(FLOW_RAW_PATH)?;
        for r in reader.into_iter() {
            let ev = r?;
            std::hint::black_box(&ev.payload);
            raw_count += 1;
        }
    }
    let raw_read_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    let mut schema_count = 0u64;
    {
        let reader = SegmentedReader::open(DIR_SCHEMA)?;
        for r in reader.into_iter() {
            let (ev, _) = r?;
            std::hint::black_box(&ev.payload);
            schema_count += 1;
        }
    }
    let schema_read_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    let mut json_count = 0u64;
    {
        let f = File::open(JSON_PATH)?;
        let reader = BufReader::with_capacity(256 * 1024, f);
        for line in reader.lines() {
            let v: serde_json::Value = serde_json::from_str(&line?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            std::hint::black_box(&v["payload"]);
            json_count += 1;
        }
    }
    let json_read_ms = start.elapsed().as_secs_f64() * 1000.0;

    let eps = |count: u64, ms: f64| count as f64 / (ms / 1000.0);
    println!(
        "  raw .flow:    {:>7.1} ms  {raw_count} events  ({:.0} ev/s)",
        raw_read_ms,
        eps(raw_count, raw_read_ms)
    );
    println!(
        "  schema .flow: {:>7.1} ms  {schema_count} events  ({:.0} ev/s)",
        schema_read_ms,
        eps(schema_count, schema_read_ms)
    );
    println!(
        "  .jsonl:       {:>7.1} ms  {json_count} events  ({:.0} ev/s)",
        json_read_ms,
        eps(json_count, json_read_ms)
    );
    println!(
        "  raw .flow is {:.1}x faster than .jsonl",
        json_read_ms / raw_read_ms
    );

    // ── Read: filtered ───────────────────────────────────────────────────
    section("READ (filtered, only food-item.created)");

    let start = Instant::now();
    let mut raw_filtered = 0u64;
    {
        let reader = FlowReader::open(FLOW_RAW_PATH)?.with_filter(&[FOOD_ITEM_CREATED]);
        for r in reader.into_iter() {
            let ev = r?;
            std::hint::black_box(&ev.payload);
            raw_filtered += 1;
        }
    }
    let raw_filt_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    let mut schema_filtered = 0u64;
    {
        let reader =
            SegmentedReader::open(DIR_SCHEMA)?.with_filter(&[FOOD_ITEM_CREATED]);
        for r in reader.into_iter() {
            let (ev, _) = r?;
            std::hint::black_box(&ev.payload);
            schema_filtered += 1;
        }
    }
    let schema_filt_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    let mut json_filtered = 0u64;
    {
        let f = File::open(JSON_PATH)?;
        let reader = BufReader::with_capacity(256 * 1024, f);
        for line in reader.lines() {
            let v: serde_json::Value = serde_json::from_str(&line?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if v["type"].as_u64() == Some(FOOD_ITEM_CREATED as u64) {
                std::hint::black_box(&v["payload"]);
                json_filtered += 1;
            }
        }
    }
    let json_filt_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!(
        "  raw .flow:    {:>7.1} ms  {raw_filtered} events matched",
        raw_filt_ms
    );
    println!(
        "  schema .flow: {:>7.1} ms  {schema_filtered} events matched",
        schema_filt_ms
    );
    println!(
        "  .jsonl:       {:>7.1} ms  {json_filtered} events matched",
        json_filt_ms
    );

    // ── Summary ──────────────────────────────────────────────────────────
    section("SUMMARY");
    println!(
        "  Storage: schema .flow is {:.1}x smaller than raw .flow, {:.1}x smaller than .jsonl",
        raw_size as f64 / schema_size as f64,
        json_size as f64 / schema_size as f64,
    );
    println!(
        "  Read speed: schema .flow is {:.1}x faster than .jsonl (full), {:.1}x (filtered)",
        json_read_ms / schema_read_ms,
        json_filt_ms / schema_filt_ms,
    );

    println!();
    println!("  Note: run `compact <dir>` to add Zstd on top of schema encoding.");
    println!("  Schema encoding removes repeated field names (~20-30% savings alone);");
    println!("  Zstd dictionary compression on schema-encoded data yields 5-15x total");
    println!("  vs raw JSON because the value-only patterns are highly compressible.");

    let _ = fs::remove_file(FLOW_RAW_PATH);
    let _ = fs::remove_file(JSON_PATH);
    let _ = fs::remove_dir_all(DIR_SCHEMA);

    Ok(())
}
