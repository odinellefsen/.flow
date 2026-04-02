use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::block::BlockHeader;
use crate::event::EventRecord;
use crate::format::{self, BLOCK_HEADER_SIZE, FILE_HEADER_SIZE};
use crate::store::{FlowStore, SegmentInfo};

#[derive(Debug)]
pub struct PurgeStats {
    pub segments_rewritten: u64,
    pub segments_unchanged: u64,
    pub events_removed: u64,
    pub events_kept: u64,
    pub bytes_saved: u64,
}

/// Delete the entire flow type directory and all its segment files.
pub fn delete_flow_type(dir: &str) -> io::Result<()> {
    fs::remove_dir_all(dir)?;
    Ok(())
}

/// Remove all events from a flow type, resetting it to an empty log.
/// The directory and manifest are preserved with their configuration intact.
pub fn truncate_flow_type(dir: &str) -> io::Result<()> {
    let mut store = FlowStore::open(dir)?;

    for seg in store.manifest.segments.drain(..).collect::<Vec<_>>() {
        let path = store.dir().join(&seg.file);
        let _ = fs::remove_file(path);
    }

    store.save_manifest()?;
    Ok(())
}

/// Rewrite all segment files removing every event with the given event_type_id.
/// Blocks that contain none of that type are byte-copied verbatim (no decode).
/// Blocks that do contain it are decoded, filtered, and re-encoded.
/// Empty segments after filtering are removed from the manifest.
pub fn purge_event_type(dir: &str, event_type_id: u16) -> io::Result<PurgeStats> {
    let mut store = FlowStore::open(dir)?;
    let mut stats = PurgeStats {
        segments_rewritten: 0,
        segments_unchanged: 0,
        events_removed: 0,
        events_kept: 0,
        bytes_saved: 0,
    };

    let segments: Vec<SegmentInfo> = store.manifest.segments.clone();
    let mut updated_segments: Vec<SegmentInfo> = Vec::new();

    for seg in segments {
        let path = store.dir().join(&seg.file);
        let result = rewrite_segment_without_type(
            &path,
            event_type_id,
            None,
            &mut stats,
        )?;

        match result {
            SegmentResult::Unchanged => {
                stats.segments_unchanged += 1;
                updated_segments.push(seg);
            }
            SegmentResult::Rewritten(new_info) => {
                stats.segments_rewritten += 1;
                if new_info.event_count > 0 {
                    updated_segments.push(new_info);
                } else {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }

    store.manifest.segments = updated_segments;
    store.save_manifest()?;

    Ok(stats)
}

/// Rewrite segment files removing events of the given event_type_id
/// that occur at or after `after_sequence`. Events before `after_sequence`
/// are kept even if they match the type.
pub fn truncate_event_type(
    dir: &str,
    event_type_id: u16,
    after_sequence: u64,
) -> io::Result<PurgeStats> {
    let mut store = FlowStore::open(dir)?;
    let mut stats = PurgeStats {
        segments_rewritten: 0,
        segments_unchanged: 0,
        events_removed: 0,
        events_kept: 0,
        bytes_saved: 0,
    };

    let segments: Vec<SegmentInfo> = store.manifest.segments.clone();
    let mut updated_segments: Vec<SegmentInfo> = Vec::new();

    for seg in segments {
        let path = store.dir().join(&seg.file);

        // Segments that end entirely before `after_sequence` need no changes
        if seg.end_sequence < after_sequence {
            stats.segments_unchanged += 1;
            updated_segments.push(seg);
            continue;
        }

        let result = rewrite_segment_without_type(
            &path,
            event_type_id,
            Some(after_sequence),
            &mut stats,
        )?;

        match result {
            SegmentResult::Unchanged => {
                stats.segments_unchanged += 1;
                updated_segments.push(seg);
            }
            SegmentResult::Rewritten(new_info) => {
                stats.segments_rewritten += 1;
                if new_info.event_count > 0 {
                    updated_segments.push(new_info);
                } else {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }

    store.manifest.segments = updated_segments;
    store.save_manifest()?;

    Ok(stats)
}

enum SegmentResult {
    Unchanged,
    Rewritten(SegmentInfo),
}

/// Core rewrite logic. Reads a segment block by block:
/// - Blocks not containing `event_type_id` are byte-copied verbatim.
/// - Blocks containing it are decoded, filtered, and re-encoded.
///
/// If `remove_from_sequence` is Some(N), only events of the type at
/// sequence >= N are removed; earlier ones are kept.
fn rewrite_segment_without_type(
    path: &PathBuf,
    event_type_id: u16,
    remove_from_sequence: Option<u64>,
    stats: &mut PurgeStats,
) -> io::Result<SegmentResult> {
    let original_size = fs::metadata(path)?.len();

    let mut input = BufReader::with_capacity(256 * 1024, File::open(path)?);

    // Read and validate file header
    let mut header_buf = [0u8; FILE_HEADER_SIZE];
    input.read_exact(&mut header_buf)?;
    let _file_header = format::decode_file_header(&header_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Write to a temp file alongside the original
    let tmp_path = path.with_extension("flow.tmp");
    let mut output = BufWriter::with_capacity(256 * 1024, File::create(&tmp_path)?);
    output.write_all(&header_buf)?;

    let mut any_block_changed = false;
    let mut total_events_kept: u64 = 0;
    let mut total_events_removed: u64 = 0;
    let mut first_sequence: Option<u64> = None;
    let mut last_sequence: u64 = 0;

    loop {
        let mut hdr_buf = [0u8; BLOCK_HEADER_SIZE];
        match input.read_exact(&mut hdr_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(e);
            }
        }

        let block_header = BlockHeader::decode(&hdr_buf);

        // Read block data
        let mut block_data = vec![0u8; block_header.data_size as usize];
        input.read_exact(&mut block_data)?;

        // Check if this block contains the target event type at all
        if !block_header.contains_event_type(event_type_id) {
            // Block is unaffected -- byte-copy verbatim
            output.write_all(&hdr_buf)?;
            output.write_all(&block_data)?;
            total_events_kept += block_header.event_count as u64;
            if first_sequence.is_none() {
                first_sequence = Some(block_header.start_sequence);
            }
            last_sequence = block_header.end_sequence;
            continue;
        }

        // Block contains the type -- check if any events within it actually
        // need removing (considering the remove_from_sequence cutoff)
        let needs_filtering = match remove_from_sequence {
            None => true,
            Some(cutoff) => block_header.end_sequence >= cutoff,
        };

        if !needs_filtering {
            // All events in this block are before the cutoff -- copy verbatim
            output.write_all(&hdr_buf)?;
            output.write_all(&block_data)?;
            total_events_kept += block_header.event_count as u64;
            if first_sequence.is_none() {
                first_sequence = Some(block_header.start_sequence);
            }
            last_sequence = block_header.end_sequence;
            continue;
        }

        // Decode events and filter
        any_block_changed = true;
        let mut kept: Vec<EventRecord> = Vec::new();
        let mut removed: u64 = 0;
        let mut offset = 0;

        for _ in 0..block_header.event_count {
            match EventRecord::decode(&block_data[offset..]) {
                Ok((record, consumed)) => {
                    offset += consumed;
                    let should_remove = record.event_type_id == event_type_id
                        && remove_from_sequence
                            .map(|cutoff| record.sequence >= cutoff)
                            .unwrap_or(true);

                    if should_remove {
                        removed += 1;
                    } else {
                        kept.push(record);
                    }
                }
                Err(e) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                }
            }
        }

        total_events_removed += removed;

        if kept.is_empty() {
            continue; // drop block entirely
        }

        // Re-encode the filtered block
        let mut new_block_data: Vec<u8> = Vec::new();
        let mut new_header = BlockHeader::new();
        new_header.start_sequence = kept[0].sequence;
        new_header.end_sequence = kept[kept.len() - 1].sequence;
        new_header.event_count = kept.len() as u32;

        for event in &kept {
            new_header.set_event_type(event.event_type_id);
            event.encode(&mut new_block_data);
        }

        new_header.data_size = new_block_data.len() as u32;
        new_header.checksum = format::crc32c(&new_block_data);

        output.write_all(&new_header.encode())?;
        output.write_all(&new_block_data)?;

        total_events_kept += kept.len() as u64;
        if first_sequence.is_none() {
            first_sequence = Some(new_header.start_sequence);
        }
        last_sequence = new_header.end_sequence;
    }

    output.flush()?;
    drop(output);

    if !any_block_changed {
        // Nothing changed -- discard temp file, report unchanged
        let _ = fs::remove_file(&tmp_path);
        stats.events_kept += total_events_kept;
        return Ok(SegmentResult::Unchanged);
    }

    // Atomic replace
    let new_size = fs::metadata(&tmp_path)?.len();
    fs::rename(&tmp_path, path)?;

    stats.events_removed += total_events_removed;
    stats.events_kept += total_events_kept;
    stats.bytes_saved += original_size.saturating_sub(new_size);

    let new_info = SegmentInfo {
        file: path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        bucket_start_ms: path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .parse()
            .unwrap_or(0),
        start_sequence: first_sequence.unwrap_or(0),
        end_sequence: last_sequence,
        event_count: total_events_kept,
        byte_size: new_size,
    };

    Ok(SegmentResult::Rewritten(new_info))
}
