use std::io::{self, Write};
use std::time::Instant;

use dotflow::{Cursor, SegmentedReader};

const DIR: &str = "benchmark.flowtype";

const TYPE_NAMES: &[&str] = &["user.created", "email.updated", "user.deleted"];

fn type_name(id: u16) -> &'static str {
    TYPE_NAMES.get(id as usize).copied().unwrap_or("unknown")
}

fn main() -> io::Result<()> {
    let args = Args::parse();

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
        let names: Vec<&str> = ids.iter().map(|id| type_name(*id)).collect();
        eprintln!("Filter active: types {:?} ({:?})", ids, names);
        reader.with_filter(ids)
    } else {
        eprintln!("No filter -- replaying all event types");
        reader
    };

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let start = Instant::now();
    let mut count: u64 = 0;
    let mut last_cursor: Option<Cursor> = None;
    let mut last_report = Instant::now();

    for result in reader.into_iter() {
        let (event, cursor) = result?;
        let payload = String::from_utf8_lossy(&event.payload);

        writeln!(
            out,
            "[seq={:>7}] {:<15} {}",
            event.sequence,
            type_name(event.event_type_id),
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
