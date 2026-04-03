use std::collections::HashMap;
use std::io;
use std::path::Path;

pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;
/// Zstd with a per-event-type trained dictionary stored in the flow type directory.
pub const CODEC_ZSTD_DICT: u8 = 2;

/// Sentinel dict_id used when no dictionary applies.
pub const NO_DICT_ID: u16 = 0xFFFF;

/// Default Zstd compression level for cold/archival segments (high ratio, slow).
pub const DEFAULT_COMPRESS_LEVEL: i32 = 15;

/// Default dictionary size. 112 KB is the Zstd-recommended maximum for effective training.
pub const DEFAULT_DICT_SIZE: usize = 112_640;

/// Minimum number of payload samples required to train a dictionary.
/// Below this, plain Zstd is used instead.
pub const MIN_DICT_SAMPLES: usize = 100;

// ── Dictionary file naming ───────────────────────────────────────────────────

pub fn dict_filename(event_type_id: u16) -> String {
    format!("dict_type{event_type_id}.zst")
}

/// Parse an event type ID from a dictionary filename, or return None.
pub fn parse_dict_filename(name: &str) -> Option<u16> {
    let stem = name.strip_prefix("dict_type")?.strip_suffix(".zst")?;
    stem.parse().ok()
}

// ── Compress / decompress ────────────────────────────────────────────────────

pub fn compress(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    zstd::bulk::compress(data, level)
}

pub fn compress_with_dict(data: &[u8], dict: &[u8], level: i32) -> io::Result<Vec<u8>> {
    let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    compressor
        .compress(data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/// Decompress a Zstd-compressed block (no dictionary).
pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::decode_all(data)
}

/// Decompress a Zstd-compressed block using a specific dictionary.
pub fn decompress_with_dict(data: &[u8], dict: &[u8]) -> io::Result<Vec<u8>> {
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    // 256 MB ceiling -- larger than any sane block but avoids zip-bomb attacks
    decompressor
        .decompress(data, 256 * 1024 * 1024)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

// ── Dictionary training ──────────────────────────────────────────────────────

/// Train a Zstd dictionary from a set of payload samples.
/// Returns Err if there are fewer than `MIN_DICT_SAMPLES` samples.
pub fn train_dictionary(samples: &[Vec<u8>], dict_size: usize) -> io::Result<Vec<u8>> {
    if samples.len() < MIN_DICT_SAMPLES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "need at least {} samples to train a dictionary (got {})",
                MIN_DICT_SAMPLES,
                samples.len()
            ),
        ));
    }
    let refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();
    zstd::dict::from_samples(&refs, dict_size)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

// ── Load dictionaries from directory ────────────────────────────────────────

/// Load all `dict_typeN.zst` files from `dir` into a map of `type_id → bytes`.
/// Missing or unreadable files are silently skipped.
pub fn load_dictionaries(dir: &Path) -> io::Result<HashMap<u16, Vec<u8>>> {
    let mut dicts = HashMap::new();

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(dicts),
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(id) = parse_dict_filename(&name_str) {
            if let Ok(data) = std::fs::read(entry.path()) {
                dicts.insert(id, data);
            }
        }
    }

    Ok(dicts)
}
