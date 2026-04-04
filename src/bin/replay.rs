use std::io::{self, Write};
use std::time::Instant;

use dotflow::{Cursor, FlowStore, SegmentedReader};

const DIR: &str = "benchmark.flowtype";

fn type_name<'a>(store: &'a FlowStore, id: u16) -> &'a str {
    store.event_type_name(id).unwrap_or("unknown")
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let store = FlowStore::open(DIR)?;

    let reader = if let Some(ref cursor) = args.from_cursor {
        eprintln!(
            "Resuming from cursor: segment={}, sequence={}",
            cursor.segment_bucket_ms, cursor.sequence
        );
        SegmentedReader::open_from(DIR, cursor.clone())?
    } else {
        SegmentedReader::open(DIR)?
    };

    let reader = if let Some(ref ids) = args.filter {
        let names: Vec<&str> = ids.iter().map(|id| type_name(&store, *id)).collect();
        eprintln!("Filter active: types {:?} ({:?})", ids, names);
        reader.with_filter(ids)
    } else {
        eprintln!("No filter -- replaying all event types");
        reader
    };

    // Print locked schemas so the user knows what they'll see
    let locked_count = store.manifest.event_types.iter().filter(|e| e.schema.is_some()).count();
    if locked_count > 0 {
        eprintln!("{locked_count} event types with locked schema (payloads decoded to JSON transparently)");
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let start = Instant::now();
    let mut count: u64 = 0;
    let mut last_cursor: Option<Cursor> = None;
    let mut last_report = Instant::now();

    for result in reader.into_iter() {
        let (event, cursor) = result?;
        let payload = String::from_utf8_lossy(&event.payload);
        let type_label = store
            .event_type_name(event.event_type_id)
            .unwrap_or("unknown");

        writeln!(
            out,
            "[seq={:>7}] {:<28} {}",
            event.sequence,
            type_label,
            payload,
        )?;

        last_cursor = Some(cursor);
        count += 1;

        if last_report.elapsed().as_millis() >= 500 {
            let rate = count as f64 / start.elapsed().as_secs_f64();
            eprint!("\r\x1b[K{count} events replayed ({rate:.0} events/sec)");
            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        count as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    eprintln!(
        "\r\x1b[KReplay complete: {count} events in {elapsed:.2?} ({rate:.0} events/sec)"
    );

    if let Some(cursor) = last_cursor {
        eprintln!(
            "Last cursor: --from {}:{}",
            cursor.segment_bucket_ms, cursor.sequence
        );
        eprintln!("(use this to resume from where you left off)");
    }

    Ok(())
}

struct Args {
    filter: Option<Vec<u16>>,
    from_cursor: Option<Cursor>,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut filter = None;
        let mut from_cursor = None;
        let mut i = 1;

        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => {
                    eprintln!("Usage: replay [--filter <type_id,...>] [--from <bucket_ms>:<sequence>]");
                    eprintln!();
                    eprintln!("Event types: 0=user.created, 1=email.updated, 2=user.deleted");
                    eprintln!();
                    eprintln!("Examples:");
                    eprintln!("  replay                              Replay all events");
                    eprintln!("  replay --filter 0                   Only user.created events");
                    eprintln!("  replay --filter 0,2                 user.created + user.deleted");
                    eprintln!(
                        "  replay --from 1743292800000:5000     Resume from segment:sequence"
                    );
                    std::process::exit(0);
                }
                "--filter" => {
                    if i + 1 < args.len() {
                        i += 1;
                        let ids: Vec<u16> = args[i]
                            .split(',')
                            .filter_map(|s| s.trim().parse().ok())
                            .collect();
                        if !ids.is_empty() {
                            filter = Some(ids);
                        }
                    }
                }
                "--from" => {
                    if i + 1 < args.len() {
                        i += 1;
                        let parts: Vec<&str> = args[i].splitn(2, ':').collect();
                        if parts.len() == 2 {
                            if let (Ok(bucket), Ok(seq)) =
                                (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                            {
                                from_cursor = Some(Cursor {
                                    segment_bucket_ms: bucket,
                                    sequence: seq,
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Self {
            filter,
            from_cursor,
        }
    }
}
