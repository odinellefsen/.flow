use std::io;
use std::time::Instant;

use dotflow::operations::{compact_flow_type, CompactOptions};

fn main() -> io::Result<()> {
    let args = Args::parse();

    if args.dir.is_empty() {
        eprintln!("Usage: compact <dir> [--level N] [--no-train]");
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  <dir>          Path to the .flowtype directory");
        eprintln!("  --level N      Zstd compression level 1-22 (default: 15)");
        eprintln!("  --no-train     Skip dictionary training, use existing dicts only");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  compact benchmark.flowtype");
        eprintln!("  compact benchmark.flowtype --level 3    # fast compression");
        eprintln!("  compact benchmark.flowtype --level 19   # max compression");
        eprintln!("  compact benchmark.flowtype --no-train   # use existing dicts");
        std::process::exit(1);
    }

    let options = CompactOptions {
        level: args.level,
        train_dicts: args.train_dicts,
        ..Default::default()
    };

    eprintln!("Compacting: {}", args.dir);
    eprintln!(
        "  Level: {}  |  Train dicts: {}",
        options.level, options.train_dicts
    );
    eprintln!();

    let start = Instant::now();
    let stats = compact_flow_type(&args.dir, options)?;
    let elapsed = start.elapsed();

    let ratio = if stats.bytes_after > 0 {
        stats.bytes_before as f64 / stats.bytes_after as f64
    } else {
        1.0
    };
    let saving_pct = if stats.bytes_before > 0 {
        100.0 * (1.0 - stats.bytes_after as f64 / stats.bytes_before as f64)
    } else {
        0.0
    };

    eprintln!("Done in {elapsed:.2?}");
    eprintln!();
    eprintln!("Dictionaries trained: {:?}", stats.dicts_trained);
    eprintln!(
        "Blocks compressed:    {}  (already compressed: {})",
        stats.blocks_compressed, stats.blocks_already_compressed
    );
    eprintln!("Segments rewritten:   {}", stats.segments_rewritten);
    eprintln!(
        "Size before:          {:.2} MB",
        stats.bytes_before as f64 / (1024.0 * 1024.0)
    );
    eprintln!(
        "Size after:           {:.2} MB",
        stats.bytes_after as f64 / (1024.0 * 1024.0)
    );
    eprintln!("Compression ratio:    {ratio:.1}x  ({saving_pct:.1}% smaller)");

    Ok(())
}

struct Args {
    dir: String,
    level: i32,
    train_dicts: bool,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut dir = String::new();
        let mut level = 15i32;
        let mut train_dicts = true;
        let mut i = 1;

        while i < args.len() {
            match args[i].as_str() {
                "--level" => {
                    if i + 1 < args.len() {
                        i += 1;
                        level = args[i].parse().unwrap_or(15).clamp(1, 22);
                    }
                }
                "--no-train" => {
                    train_dicts = false;
                }
                "--help" | "-h" => {
                    eprintln!("Usage: compact <dir> [--level N] [--no-train]");
                    std::process::exit(0);
                }
                arg if !arg.starts_with('-') => {
                    dir = arg.to_string();
                }
                _ => {}
            }
            i += 1;
        }

        Self {
            dir,
            level,
            train_dicts,
        }
    }
}
