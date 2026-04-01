# Testing dotflow

## What this is

dotflow is an append-only event log storage system. Think of it like a Kafka
partition's `.log` file, but designed for Flowcore's event model.

A **Flow Type** (e.g. `user.v0`) is a single ordered history containing
multiple **Event Types** (e.g. `user.created`, `email.updated`,
`user.deleted`). All event types are interleaved in one log in the exact order
they were written.

The log is split into **time-bucketed segment files** (one per hour by default),
stored in a directory:

```
user.v0.flowtype/
  manifest.json          <- JSON index of all segments
  1743292800000.flow      <- binary segment: hour 0
  1743296400000.flow      <- binary segment: hour 1
  ...
```

Each `.flow` segment is a binary file containing **blocks** of events. Each
block has a header with a bitset that says which event types are inside, so
readers can skip entire blocks they don't care about.

## Prerequisites

```bash
# Make sure you have Rust installed
rustc --version

# Build everything in release mode
cargo build --release
```

## Step 1: Generate 100,000 events

```bash
cargo run --release --bin generate
```

This creates a `benchmark.flowtype/` directory with 4 segment files (one per
hour, simulating events spread over 4 hours). You'll see output like:

```
Generating 100000 events into benchmark.flowtype/
  Flow type: user.v0
  Bucket duration: 1 hour
  Block size: 1000 events

Done in ~500ms
100000 events, ~190000 events/sec
4 segments
Total size: 5.57 MB
  1743292800000.flow | seq 0-24999     | 25000 events
  1743296400000.flow | seq 25000-49999 | 25000 events
  1743300000000.flow | seq 50000-74999 | 25000 events
  1743303600000.flow | seq 75000-99999 | 25000 events
```

## Step 2: Look at the manifest

```bash
cat benchmark.flowtype/manifest.json
```

This is the JSON index that any language can read to understand the log
structure without touching the binary files.

## Step 3: Replay all events

```bash
cargo run --release --bin replay
```

Events stream to your terminal one by one as they're decoded. You'll see
something like:

```
[seq=      0] user.created    {"user":"alice","id":0}
[seq=      1] user.created    {"user":"bob","id":1}
...
[seq=  99999] user.deleted    {"user":"hank","reason":"cleanup"}

Replay complete: 100000 events in ~800ms (~125000 events/sec)
Last cursor: --from 1743303600000:99999
```

The ~800ms includes printing to your terminal. To see the actual I/O speed,
redirect stdout:

```bash
cargo run --release --bin replay > /dev/null
```

This should finish in ~85ms (~1.2M events/sec) -- the terminal is the
bottleneck, not the file reading.

## Step 4: Filtered replay

Only replay `user.created` events (type 0):

```bash
cargo run --release --bin replay -- --filter 0
```

This uses the block-level bitset to skip entire blocks that contain no
`user.created` events. You'll get 60,000 events (60% of 100k).

Try other filters:

```bash
# Only user.deleted events (type 2) -- 10% of events
cargo run --release --bin replay -- --filter 2

# user.created + user.deleted, skip email updates
cargo run --release --bin replay -- --filter 0,2
```

## Step 5: Cursor resume (crash recovery)

After a replay, the tool prints a cursor like:

```
Last cursor: --from 1743303600000:99999
```

This cursor means "I last processed sequence 99999 in the segment starting at
timestamp 1743303600000." Use it to resume from exactly where you left off:

```bash
# Resume from the middle of the log
cargo run --release --bin replay -- --from 1743296400000:30000
```

This skips all events up to and including sequence 30000, then continues from
30001. It uses the manifest to jump directly to the right segment file, and
uses block headers to skip past blocks that are entirely before the cursor.

Try combining cursor + filter:

```bash
cargo run --release --bin replay -- --from 1743296400000:30000 --filter 0
```

## Step 6: Run the test suite

```bash
cargo test
```

This runs 33 tests covering:
- Event record encoding/decoding with CRC32C checksums
- Block header bitset operations
- Single-file write/read round-trips
- Filtered reads with block skipping
- Crash recovery (truncated files, corrupted checksums)
- Segment rotation across time buckets
- Cross-segment reads preserving global order
- Cursor resume (mid-segment and cross-segment)
- Manifest persistence across reopen
- Time-based segment lookup

## Step 7: Inspect the binary format

The format is documented in `FORMAT.md`. To see what a segment file looks like
at the byte level:

```bash
# First 16 bytes are the file header
xxd benchmark.flowtype/1743292800000.flow | head -1
# You should see: 464c 4f57 = "FLOW" magic bytes

# Block headers start at byte 16, each is 64 bytes
xxd -s 16 -l 64 benchmark.flowtype/1743292800000.flow
```

## Help

```bash
cargo run --release --bin replay -- --help
```
