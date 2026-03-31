pub const MAGIC: &[u8; 4] = b"FLOW";
pub const VERSION: u16 = 1;

pub const FILE_HEADER_SIZE: usize = 16;
pub const BLOCK_HEADER_SIZE: usize = 64;
pub const BITSET_SIZE: usize = 32;

/// Minimum event record size: u64 seq(8) + u16 type(2) + u64 ts(8) + u32 len(4) + u32 crc(4) = 26 bytes
pub const EVENT_RECORD_MIN_SIZE: usize = 26;

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
