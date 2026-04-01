use std::io::{self, Write};
use std::time::Instant;

use dotflow::FlowReader;

const PATH: &str = "benchmark.flow";

const TYPE_NAMES: &[&str] = &["user.created", "email.updated", "user.deleted"];

fn type_name(id: u16) -> &'static str {
    TYPE_NAMES
        .get(id as usize)
        .copied()
        .unwrap_or("unknown")
}

fn main() -> io::Result<()> {
    let filter: Option<Vec<u16>> = parse_filter();

    let reader = FlowReader::open(PATH)?;
    let reader = if let Some(ref ids) = filter {
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
    let mut last_report = Instant::now();

    for result in reader.into_iter() {
        let event = result?;
        let payload = String::from_utf8_lossy(&event.payload);

        writeln!(
            out,
            "[seq={:>7}] {:<15} {}",
            event.sequence,
            type_name(event.event_type_id),
            payload,
        )?;

        count += 1;

        if last_report.elapsed().as_millis() >= 500 {
            let rate = count as f64 / start.elapsed().as_secs_f64();
            eprint!("\r\x1b[K{count} events replayed ({rate:.0} events/sec)");
            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    eprintln!("\r\x1b[KReplay complete: {count} events in {elapsed:.2?} ({rate:.0} events/sec)");

    Ok(())
}

fn parse_filter() -> Option<Vec<u16>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        return None;
    }

    if args[1] == "--help" || args[1] == "-h" {
        eprintln!("Usage: replay [--filter <type_id,...>]");
        eprintln!();
        eprintln!("Event types: 0=user.created, 1=email.updated, 2=user.deleted");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  replay                  Replay all events");
        eprintln!("  replay --filter 0       Only user.created events");
        eprintln!("  replay --filter 0,2     Only user.created and user.deleted");
        std::process::exit(0);
    }

    if args[1] == "--filter" && args.len() >= 3 {
        let ids: Vec<u16> = args[2]
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if ids.is_empty() {
            return None;
        }
        return Some(ids);
    }

    None
}
