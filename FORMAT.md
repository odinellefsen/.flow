# .flow Format Specification v1

This document specifies the binary format for `.flow` segment files, the
directory structure for segmented Flow Type logs, and the JSON manifest schema.
The format is language-agnostic: any language that can read binary files and
parse JSON can implement a reader or writer.

All multi-byte integers are **little-endian**. All checksums use **CRC32C**
(Castagnoli), which is hardware-accelerated on modern CPUs via SSE 4.2 (x86) and
CRC32 instructions (ARM).

## Directory Structure

A Flow Type log is a directory containing segment files and a manifest:

```
<flow_type>.flowtype/
  manifest.json
  <bucket_start_ms>.flow
  <bucket_start_ms>.flow
  ...
```

Segment filenames are the bucket start time in epoch milliseconds (UTC). They
are naturally sortable in chronological order.

## Manifest (`manifest.json`)

```json
{
  "flow_type": "user.v0",
  "version": 1,
  "event_type_count": 3,
  "bucket_duration_ms": 3600000,
  "segments": [
    {
      "file": "1743292800000.flow",
      "bucket_start_ms": 1743292800000,
      "start_sequence": 0,
      "end_sequence": 24999,
      "event_count": 25000,
      "byte_size": 1456192
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `flow_type` | string | Logical name of the Flow Type (e.g. `user.v0`) |
| `version` | integer | Manifest schema version (currently `1`) |
| `event_type_count` | integer | Number of distinct event types (max 256) |
| `bucket_duration_ms` | integer | Time bucket duration in milliseconds |
| `segments` | array | Ordered list of segment entries |
| `segments[].file` | string | Segment filename |
| `segments[].bucket_start_ms` | integer | Bucket start time (epoch ms, UTC) |
| `segments[].start_sequence` | integer | First sequence number in segment |
| `segments[].end_sequence` | integer | Last sequence number in segment |
| `segments[].event_count` | integer | Total events in segment |
| `segments[].byte_size` | integer | Segment file size in bytes |

Segments are sorted by `bucket_start_ms` ascending. Sequence numbers are
globally monotonic across all segments (not per-segment).

## Segment File Format (`.flow`)

A segment file is a sequential binary stream:

```
[File Header][Block 1 Header][Block 1 Data][Block 2 Header][Block 2 Data]...
```

### File Header

Fixed size: **16 bytes**

| Offset | Size | Type | Description |
|--------|------|------|-------------|
| 0 | 4 | `[u8; 4]` | Magic bytes: `FLOW` (ASCII `0x464C4F57`) |
| 4 | 2 | `u16 LE` | Format version (`1`) |
| 6 | 2 | `u16 LE` | Event type count (max 256) |
| 8 | 8 | `[u8; 8]` | Reserved (zero-filled) |

A reader must validate the magic bytes and version before proceeding.

### Block Header

Fixed size: **64 bytes**

| Offset | Size | Type | Description |
|--------|------|------|-------------|
| 0 | 8 | `u64 LE` | Start sequence (first event in block) |
| 8 | 8 | `u64 LE` | End sequence (last event in block, inclusive) |
| 16 | 4 | `u32 LE` | Event count |
| 20 | 4 | `u32 LE` | Data size (total bytes of event records) |
| 24 | 32 | `[u8; 32]` | Event type bitset (256 bits) |
| 56 | 4 | `u32 LE` | CRC32C checksum of the block data |
| 60 | 4 | `[u8; 4]` | Reserved (zero-filled) |

The **event type bitset** has one bit per event type ID. Bit `n` is set if the
block contains at least one event with `event_type_id = n`. The bit for ID `n`
is at byte `n / 8`, bit position `n % 8` (LSB first).

The **checksum** covers only the block data (all event records), not the block
header itself.

### Event Record

Variable size, minimum **26 bytes** (with empty payload).

| Offset | Size | Type | Description |
|--------|------|------|-------------|
| 0 | 8 | `u64 LE` | Sequence number (monotonic within Flow Type) |
| 8 | 2 | `u16 LE` | Event type ID |
| 10 | 8 | `u64 LE` | Timestamp (nanoseconds since Unix epoch) |
| 18 | 4 | `u32 LE` | Payload length in bytes |
| 22 | N | `[u8; N]` | Payload (raw bytes, opaque) |
| 22+N | 4 | `u32 LE` | CRC32C checksum of bytes 0 through 22+N-1 |

The checksum covers everything in the record except the checksum field itself
(i.e., from the sequence number through the end of the payload).

## Reading

### Full Replay

1. Parse `manifest.json` to get the ordered list of segments
2. For each segment file, in order:
   a. Read and validate the 16-byte file header
   b. Read 64-byte block headers sequentially
   c. Read block data (`data_size` bytes after each header)
   d. Decode event records from block data
3. Events are yielded in global sequence order

### Filtered Read

To read only specific event types:

1. Build a 256-bit filter bitset for the desired event type IDs
2. For each block header, AND the block's `event_type_bitset` with the filter
3. If the result is all zeros, skip the block (seek forward by `data_size`)
4. Otherwise, read the block and decode events, yielding only those whose
   `event_type_id` matches the filter

### Cursor Resume

A cursor is a pair: `(segment_bucket_ms, last_processed_sequence)`.

1. Find the segment matching `segment_bucket_ms` in the manifest
2. Open that segment file
3. For each block: if `end_sequence <= last_processed_sequence`, skip it entirely
4. Within a block containing the target sequence, skip events where
   `sequence <= last_processed_sequence`
5. Continue reading forward across remaining segments

## Writing

### Appending Events

1. Assign a monotonically increasing sequence number to each event
2. Buffer events in memory
3. When the buffer reaches the configured block size (or on explicit flush):
   a. Encode all buffered events into a contiguous byte buffer
   b. Build the block header: set sequence range, event count, data size,
      populate the event type bitset, compute CRC32C of the data
   c. Write the 64-byte block header
   d. Write the event data
   e. Call `fsync` / `fdatasync`
4. Only acknowledge events as durable after `fsync` returns

### Segment Rotation

When an event's timestamp falls into a new time bucket:

1. Flush and close the current segment
2. Update the manifest with the completed segment's metadata
3. Create a new segment file with a fresh file header
4. Continue appending to the new segment

Sequence numbers are globally monotonic across segments.

## Recovery

On startup, the writer validates the latest segment:

1. Read and validate the file header
2. For each block: read header, read data, verify CRC32C
3. On first invalid block (bad checksum, truncated data, or partial header):
   stop
4. Truncate the file to the end of the last valid block
5. Resume writing from `last_valid_end_sequence + 1`

This guarantees the log always contains a valid prefix. Partial writes from
crashes are discarded.

## CRC32C

All checksums use CRC32C (polynomial `0x1EDC6F41`). This is the same algorithm
used by iSCSI, Btrfs, and ext4. It is supported by hardware on:

- x86/x64: SSE 4.2 `CRC32` instruction
- ARM: ARMv8 CRC32 instruction

Software fallback implementations are widely available in all languages.
