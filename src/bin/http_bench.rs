/// HTTP delivery benchmark.
///
/// Spins up a local axum server, writes N events to a temp .flow store,
/// then replays them via HTTP POST in three modes:
///   1. One event per request
///   2. Batched (SMALL_BATCH events per request)
///   3. Batched (LARGE_BATCH events per request)
///
/// Run with: cargo run --bin http_bench --release
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::post;
use axum::{body::Bytes, Router};
use dotflow::event::EventRecord;
use dotflow::{SegmentedReader, SegmentedWriter};
use tokio::net::TcpListener;

const DIR: &str = "http_bench.flowtype";
const EVENT_COUNT: u64 = 10_000;
const BLOCK_SIZE: usize = 500;
const BUCKET_MS: u64 = 3_600_000;
const PORT: u16 = 13700;
const SMALL_BATCH: usize = 50;
const LARGE_BATCH: usize = 500;

// ── Payload generators (same as generate.rs) ──────────────────────────────────

fn uuid(i: u64) -> String {
    format!(
        "{:08x}-{:04x}-1234-9000-{:012x}",
        (i & 0xFFFF_FFFF) as u32,
        ((i >> 32) & 0xFFFF) as u16,
        i & 0xFFFF_FFFF_FFFF,
    )
}

fn payload(i: u64) -> String {
    let names = ["Burger", "Pizza", "Sushi", "Tacos", "Ramen"];
    let users = ["user_30eqFgTWfuBysprgdMgwvjEezH2", "user_7bNkQrVxWzAoTtpsMmEjLlKiPhGn"];
    let name = names[(i as usize) % names.len()];
    let user = users[(i as usize) % users.len()];
    let price = 3.99 + (i % 20) as f64 * 0.5;
    format!(
        r#"{{"id":"{id}","userId":"{user}","name":"{name}","price":{price:.2},"available":true}}"#,
        id = uuid(i)
    )
}

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct ServerState {
    events_received: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
}

async fn handle_event(State(s): State<ServerState>, body: Bytes) -> &'static str {
    s.events_received.fetch_add(1, Ordering::Relaxed);
    s.bytes_received.fetch_add(body.len() as u64, Ordering::Relaxed);
    "ok"
}

async fn handle_batch(State(s): State<ServerState>, body: Bytes) -> &'static str {
    // Body is a JSON array -- count elements to tally events
    let count = body.iter().filter(|&&b| b == b'{').count() as u64;
    s.events_received.fetch_add(count, Ordering::Relaxed);
    s.bytes_received.fetch_add(body.len() as u64, Ordering::Relaxed);
    "ok"
}

async fn start_server(state: ServerState) {
    let app = Router::new()
        .route("/event", post(handle_event))
        .route("/batch", post(handle_batch))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ── Benchmark runs ────────────────────────────────────────────────────────────

fn load_events() -> Vec<Vec<u8>> {
    let reader = SegmentedReader::open(DIR).expect("run generate first or let http_bench generate");
    reader
        .into_iter()
        .map(|r| {
            let (ev, _) = r.unwrap();
            // Reconstruct the full Flowcore JSON envelope the server would receive
            serde_json::to_vec(&serde_json::json!({
                "eventId": uuid(ev.sequence),
                "flowType": "food-item",
                "eventType": "food-item.created.v0",
                "eventTime": ev.event_time,
                "payload": serde_json::from_slice::<serde_json::Value>(&ev.payload).unwrap_or(serde_json::Value::Null),
            }))
            .unwrap()
        })
        .collect()
}

async fn bench_single(client: &reqwest::Client, events: &[Vec<u8>]) -> (Duration, u64) {
    let url = format!("http://127.0.0.1:{PORT}/event");
    let start = Instant::now();
    for body in events {
        client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
    }
    (start.elapsed(), events.len() as u64)
}

async fn bench_batched(
    client: &reqwest::Client,
    events: &[Vec<u8>],
    batch_size: usize,
) -> (Duration, u64) {
    let url = format!("http://127.0.0.1:{PORT}/batch");
    let start = Instant::now();
    let mut total = 0u64;

    for chunk in events.chunks(batch_size) {
        // Build JSON array: [event, event, ...]
        let mut body = Vec::with_capacity(chunk.iter().map(|e| e.len() + 1).sum::<usize>() + 2);
        body.push(b'[');
        for (i, ev) in chunk.iter().enumerate() {
            if i > 0 {
                body.push(b',');
            }
            body.extend_from_slice(ev);
        }
        body.push(b']');

        client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();

        total += chunk.len() as u64;
    }

    (start.elapsed(), total)
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Generate test data
    let _ = std::fs::remove_dir_all(DIR);
    eprint!("Generating {EVENT_COUNT} events... ");
    {
        let mut w = SegmentedWriter::create(DIR, "food-item", 1, BUCKET_MS, BLOCK_SIZE).unwrap();
        for i in 0..EVENT_COUNT {
            let ts = 1_743_292_800_000_000_000u64 + i * 144_000_000_000;
            let mut ev = EventRecord::new(0, ts, payload(i).into_bytes());
            ev.event_id = uuid(i).as_bytes()[..16].try_into().unwrap_or([0u8; 16]);
            w.append(ev).unwrap();
        }
    }
    eprintln!("done.");

    // Start server
    let state = ServerState {
        events_received: Arc::new(AtomicU64::new(0)),
        bytes_received: Arc::new(AtomicU64::new(0)),
    };
    let state_clone = state.clone();
    tokio::spawn(async move { start_server(state_clone).await });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Load events into memory (simulate read from disk already done)
    let events = load_events();
    let total_payload_bytes: usize = events.iter().map(|e| e.len()).sum();
    println!(
        "\n{} events loaded, avg payload {:.0} bytes",
        events.len(),
        total_payload_bytes as f64 / events.len() as f64
    );

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .tcp_nodelay(true)
        .build()
        .unwrap();

    println!("\n=== HTTP POST benchmark (localhost:{PORT}) ===\n");

    let section = |label: &str| println!("── {label} ──");

    // 1. Single event per request
    section("1 event per request");
    state.events_received.store(0, Ordering::Relaxed);
    let (dur, count) = bench_single(&client, &events).await;
    print_stats(count, dur, total_payload_bytes as u64);

    // 2. Small batch
    section(&format!("{SMALL_BATCH} events per request"));
    state.events_received.store(0, Ordering::Relaxed);
    let (dur, count) = bench_batched(&client, &events, SMALL_BATCH).await;
    print_stats(count, dur, total_payload_bytes as u64);

    // 3. Large batch
    section(&format!("{LARGE_BATCH} events per request"));
    state.events_received.store(0, Ordering::Relaxed);
    let (dur, count) = bench_batched(&client, &events, LARGE_BATCH).await;
    print_stats(count, dur, total_payload_bytes as u64);

    println!("\nNote: server is localhost -- real network latency would dominate single-event mode.");

    let _ = std::fs::remove_dir_all(DIR);
}

fn print_stats(count: u64, dur: Duration, total_bytes: u64) {
    let secs = dur.as_secs_f64();
    let ev_per_sec = count as f64 / secs;
    let mb_per_sec = total_bytes as f64 / secs / (1024.0 * 1024.0);
    let ms = dur.as_millis();
    println!(
        "  {count} events in {ms} ms  |  {ev_per_sec:.0} events/sec  |  {mb_per_sec:.1} MB/s"
    );
}
