pub const MAGIC: &[u8; 4] = b"FLOW";
pub const VERSION: u16 = 1;

pub const FILE_HEADER_SIZE: usize = 16;
pub const BLOCK_HEADER_SIZE: usize = 64;
pub const BITSET_SIZE: usize = 32;

/// Minimum event record size (no optional IDs, no metadata, empty payload):
/// seq(8) + event_id(16) + type(2) + event_time(8) + valid_time(8) +
/// recorded_time(8) + flags(1) + metadata_count(2) + payload_len(4) + crc(4) = 61
pub const EVENT_RECORD_MIN_SIZE: usize = 61;

pub const FLAG_HAS_CORRELATION_ID: u8 = 1 << 0;
pub const FLAG_HAS_CAUSATION_ID: u8 = 1 << 1;

pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

pub fn encode_file_header(event_type_count: u16) -> [u8; FILE_HEADER_SIZE] {
    let mut buf = [0u8; FILE_HEADER_SIZE];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4..6].copy_from_slice(&VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&event_type_count.to_le_bytes());
    buf
}

pub struct FileHeader {
    pub version: u16,
    pub event_type_count: u16,
}

pub fn decode_file_header(buf: &[u8; FILE_HEADER_SIZE]) -> Result<FileHeader, &'static str> {
    if &buf[0..4] != MAGIC {
        return Err("invalid magic bytes");
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != VERSION {
        return Err("unsupported version");
    }
    let event_type_count = u16::from_le_bytes([buf[6], buf[7]]);
    Ok(FileHeader {
        version,
        event_type_count,
    })
}
