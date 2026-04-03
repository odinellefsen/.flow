use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::block::BlockHeader;
use crate::compression::{
    self, CODEC_NONE, CODEC_ZSTD, CODEC_ZSTD_DICT, DEFAULT_COMPRESS_LEVEL, DEFAULT_DICT_SIZE,
    MIN_DICT_SAMPLES, NO_DICT_ID,
};
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

#[derive(Debug)]
pub struct CompactStats {
    pub blocks_compressed: u64,
    pub blocks_already_compressed: u64,
    pub segments_rewritten: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub dicts_trained: Vec<u16>,
}

#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// Zstd compression level. Range 1 (fast) – 22 (maximum ratio). Default: 15.
    pub level: i32,
    /// Train new per-event-type dictionaries from the existing data before compressing.
    pub train_dicts: bool,
    /// Dictionary size in bytes. Default: 112 640 (110 KB, Zstd recommended max).
    pub dict_size: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            level: DEFAULT_COMPRESS_LEVEL,
            train_dicts: true,
            dict_size: DEFAULT_DICT_SIZE,
        }
    }
}

// ── Public operations ─────────────────────────────────────────────────────────

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
    let dicts = compression::load_dictionaries(store.dir())?;

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
        let result = rewrite_segment_without_type(&path, event_type_id, None, &dicts, &mut stats)?;

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
/// that occur at or after `after_sequence`.
pub fn truncate_event_type(
    dir: &str,
    event_type_id: u16,
    after_sequence: u64,
) -> io::Result<PurgeStats> {
    let mut store = FlowStore::open(dir)?;
    let dicts = compression::load_dictionaries(store.dir())?;

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

        if seg.end_sequence < after_sequence {
            stats.segments_unchanged += 1;
            updated_segments.push(seg);
            continue;
        }

        let result = rewrite_segment_without_type(
            &path,
            event_type_id,
            Some(after_sequence),
            &dicts,
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

/// Compress all uncompressed blocks in all segments.
///
/// If `options.train_dicts` is true, a Zstd dictionary is trained per event type
/// from its existing payloads and saved as `dict_typeN.zst` in the flow type
/// directory. Blocks that contain exactly one event type use that type's dictionary
/// (`CODEC_ZSTD_DICT`); mixed-type blocks use plain Zstd (`CODEC_ZSTD`).
///
/// Already-compressed blocks are preserved as-is.
pub fn compact_flow_type(dir: &str, options: CompactOptions) -> io::Result<CompactStats> {
    let mut store = FlowStore::open(dir)?;
    let mut stats = CompactStats {
        blocks_compressed: 0,
        blocks_already_compressed: 0,
        segments_rewritten: 0,
        bytes_before: 0,
        bytes_after: 0,
        dicts_trained: Vec::new(),
    };

    // ── 1. Train dictionaries ─────────────────────────────────────────────────
    let mut dicts: HashMap<u16, Vec<u8>> = compression::load_dictionaries(store.dir())?;

    if options.train_dicts {
        let new_dicts = train_all_dicts(&store, &options)?;
        for (id, dict_bytes) in new_dicts {
            let path = store.dict_path(id);
            fs::write(&path, &dict_bytes)?;
            stats.dicts_trained.push(id);
            dicts.insert(id, dict_bytes);
        }
    }

    // ── 2. Compress each segment ──────────────────────────────────────────────
    let segments: Vec<SegmentInfo> = store.manifest.segments.clone();
    let mut updated_segments: Vec<SegmentInfo> = Vec::new();

    for seg in segments {
        let path = store.dir().join(&seg.file);
        let size_before = fs::metadata(&path)?.len();
        stats.bytes_before += size_before;

        let result = compress_segment(&path, &seg, &dicts, &options, &mut stats)?;

        match result {
            None => {
                // All blocks were already compressed
                stats.bytes_after += size_before;
                updated_segments.push(seg);
            }
            Some(new_info) => {
                stats.segments_rewritten += 1;
                stats.bytes_after += new_info.byte_size;
                updated_segments.push(new_info);
            }
        }
    }

    store.manifest.segments = updated_segments;
    store.save_manifest()?;

    Ok(stats)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

enum SegmentResult {
    Unchanged,
    Rewritten(SegmentInfo),
}

/// Rewrite a segment, removing events of the given type.
/// Unaffected blocks are byte-copied verbatim (preserving their codec).
/// Affected blocks are decoded (with decompression if needed), filtered,
/// and re-encoded as uncompressed (run compact afterward to recompress).
fn rewrite_segment_without_type(
    path: &PathBuf,
    event_type_id: u16,
    remove_from_sequence: Option<u64>,
    dicts: &HashMap<u16, Vec<u8>>,
    stats: &mut PurgeStats,
) -> io::Result<SegmentResult> {
    let original_size = fs::metadata(path)?.len();

    let mut input = BufReader::with_capacity(256 * 1024, File::open(path)?);

    let mut header_buf = [0u8; FILE_HEADER_SIZE];
    input.read_exact(&mut header_buf)?;
    let _file_header = format::decode_file_header(&header_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

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

        // Read raw on-disk block bytes
        let mut raw_block = vec![0u8; block_header.data_size as usize];
        input.read_exact(&mut raw_block)?;

        if !block_header.contains_event_type(event_type_id) {
            // Block is unaffected -- byte-copy verbatim (codec preserved)
            output.write_all(&hdr_buf)?;
            output.write_all(&raw_block)?;
            total_events_kept += block_header.event_count as u64;
            if first_sequence.is_none() {
                first_sequence = Some(block_header.start_sequence);
            }
            last_sequence = block_header.end_sequence;
            continue;
        }

        let needs_filtering = match remove_from_sequence {
            None => true,
            Some(cutoff) => block_header.end_sequence >= cutoff,
        };

        if !needs_filtering {
            output.write_all(&hdr_buf)?;
            output.write_all(&raw_block)?;
            total_events_kept += block_header.event_count as u64;
            if first_sequence.is_none() {
                first_sequence = Some(block_header.start_sequence);
            }
            last_sequence = block_header.end_sequence;
            continue;
        }

        // Need to decode -- decompress first if the block is compressed
        any_block_changed = true;
        let block_data = decompress_raw(&block_header, &raw_block, dicts)?;

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
            continue;
        }

        // Re-encode filtered block as uncompressed (compact can recompress later)
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
        let _ = fs::remove_file(&tmp_path);
        stats.events_kept += total_events_kept;
        return Ok(SegmentResult::Unchanged);
    }

    let new_size = fs::metadata(&tmp_path)?.len();
    fs::rename(&tmp_path, path)?;

    stats.events_removed += total_events_removed;
    stats.events_kept += total_events_kept;
    stats.bytes_saved += original_size.saturating_sub(new_size);

    let new_info = SegmentInfo {
        file: path.file_name().unwrap().to_string_lossy().to_string(),
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

/// Compress a single segment, rewriting it in place.
/// Returns None if all blocks were already compressed.
fn compress_segment(
    path: &PathBuf,
    seg: &SegmentInfo,
    dicts: &HashMap<u16, Vec<u8>>,
    options: &CompactOptions,
    stats: &mut CompactStats,
) -> io::Result<Option<SegmentInfo>> {
    let mut input = BufReader::with_capacity(256 * 1024, File::open(path)?);

    let mut header_buf = [0u8; FILE_HEADER_SIZE];
    input.read_exact(&mut header_buf)?;

    let tmp_path = path.with_extension("flow.tmp");
    let mut output = BufWriter::with_capacity(256 * 1024, File::create(&tmp_path)?);
    output.write_all(&header_buf)?;

    let mut any_block_changed = false;

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

        let mut block_header = BlockHeader::decode(&hdr_buf);
        let mut raw_block = vec![0u8; block_header.data_size as usize];
        input.read_exact(&mut raw_block)?;

        if block_header.codec != CODEC_NONE {
            // Already compressed -- preserve verbatim
            output.write_all(&block_header.encode())?;
            output.write_all(&raw_block)?;
            stats.blocks_already_compressed += 1;
            continue;
        }

        // Compress this block
        any_block_changed = true;

        let (compressed, codec, dict_id) =
            if let Some(type_id) = block_header.single_event_type() {
                if let Some(dict) = dicts.get(&type_id) {
                    let c = compression::compress_with_dict(&raw_block, dict, options.level)?;
                    (c, CODEC_ZSTD_DICT, type_id)
                } else {
                    let c = compression::compress(&raw_block, options.level)?;
                    (c, CODEC_ZSTD, NO_DICT_ID)
                }
            } else {
                let c = compression::compress(&raw_block, options.level)?;
                (c, CODEC_ZSTD, NO_DICT_ID)
            };

        block_header.checksum = format::crc32c(&compressed);
        block_header.data_size = compressed.len() as u32;
        block_header.codec = codec;
        block_header.dict_id = dict_id;

        output.write_all(&block_header.encode())?;
        output.write_all(&compressed)?;

        stats.blocks_compressed += 1;
    }

    output.flush()?;
    drop(output);

    if !any_block_changed {
        let _ = fs::remove_file(&tmp_path);
        return Ok(None);
    }

    let new_size = fs::metadata(&tmp_path)?.len();
    fs::rename(&tmp_path, path)?;

    Ok(Some(SegmentInfo {
        file: seg.file.clone(),
        bucket_start_ms: seg.bucket_start_ms,
        start_sequence: seg.start_sequence,
        end_sequence: seg.end_sequence,
        event_count: seg.event_count,
        byte_size: new_size,
    }))
}

/// Train a dictionary for every event type that has enough samples.
fn train_all_dicts(
    store: &FlowStore,
    options: &CompactOptions,
) -> io::Result<HashMap<u16, Vec<u8>>> {
    // Collect payload samples per event type (max 5000 per type to bound memory)
    let mut samples: HashMap<u16, Vec<Vec<u8>>> = HashMap::new();
    const MAX_SAMPLES_PER_TYPE: usize = 5_000;

    for seg in store.segments() {
        let path = store.dir().join(&seg.file);
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(&path)?);

        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;

        loop {
            let mut hdr_buf = [0u8; BLOCK_HEADER_SIZE];
            match reader.read_exact(&mut hdr_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let block_header = BlockHeader::decode(&hdr_buf);

            // Skip blocks only when every known type is already saturated.
            // We must have seen at least one type first (avoids vacuous true on empty map).
            let all_saturated = !samples.is_empty()
                && samples.values().all(|s| s.len() >= MAX_SAMPLES_PER_TYPE);

            if all_saturated {
                use std::io::Seek;
                reader.seek(std::io::SeekFrom::Current(block_header.data_size as i64))?;
                continue;
            }

            // We need a BufReader here but read_and_decompress takes BufReader<File>
            // -- inline the read to avoid type issues
            let mut raw = vec![0u8; block_header.data_size as usize];
            reader.read_exact(&mut raw)?;

            let block_data = decompress_raw(&block_header, &raw, &HashMap::new())?;

            let mut offset = 0;
            for _ in 0..block_header.event_count {
                match EventRecord::decode(&block_data[offset..]) {
                    Ok((record, consumed)) => {
                        offset += consumed;
                        let type_samples =
                            samples.entry(record.event_type_id).or_default();
                        if type_samples.len() < MAX_SAMPLES_PER_TYPE {
                            type_samples.push(record.payload);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    let mut result = HashMap::new();
    for (type_id, type_samples) in samples {
        if type_samples.len() < MIN_DICT_SAMPLES {
            continue;
        }
        match compression::train_dictionary(&type_samples, options.dict_size) {
            Ok(dict_bytes) => {
                result.insert(type_id, dict_bytes);
            }
            Err(_) => {
                // Not enough data or training failed -- skip silently
            }
        }
    }

    Ok(result)
}

/// Decompress raw on-disk block bytes if needed, using the codec from the header.
fn decompress_raw(
    header: &BlockHeader,
    raw: &[u8],
    dicts: &HashMap<u16, Vec<u8>>,
) -> io::Result<Vec<u8>> {
    match header.codec {
        CODEC_NONE => Ok(raw.to_vec()),
        CODEC_ZSTD => compression::decompress(raw),
        CODEC_ZSTD_DICT => {
            if let Some(dict) = dicts.get(&header.dict_id) {
                compression::decompress_with_dict(raw, dict)
            } else {
                compression::decompress(raw)
            }
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown codec {other}"),
        )),
    }
}
