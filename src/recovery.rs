use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::block::BlockHeader;
use crate::format::{self, BLOCK_HEADER_SIZE, FILE_HEADER_SIZE};

#[derive(Debug)]
pub struct ValidPrefix {
    /// Byte offset where the valid data ends (truncate to this).
    pub valid_end: u64,
    /// The sequence number to assign to the next appended event.
    pub next_sequence: u64,
    /// Number of valid blocks found.
    pub block_count: u64,
    /// Total number of valid events across all blocks.
    pub event_count: u64,
    /// Event type count from the file header.
    pub event_type_count: u16,
}

/// Scan the file from beginning, validate the file header and every block.
/// Returns information about the valid prefix.
pub fn validate_file(path: &str) -> io::Result<ValidPrefix> {
    let mut file = File::open(path)?;

    let mut header_buf = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let file_header = format::decode_file_header(&header_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut valid_end = FILE_HEADER_SIZE as u64;
    let mut next_sequence: u64 = 0;
    let mut block_count: u64 = 0;
    let mut event_count: u64 = 0;

    loop {
        let mut hdr_buf = [0u8; BLOCK_HEADER_SIZE];
        match file.read_exact(&mut hdr_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let header = BlockHeader::decode(&hdr_buf);

        if header.data_size == 0 && header.event_count == 0 {
            break;
        }

        let mut data = vec![0u8; header.data_size as usize];
        match file.read_exact(&mut data) {
            Ok(()) => {}
            Err(_) => break, // partial block data
        }

        if !header.validate_data(&data) {
            break;
        }

        valid_end = file.seek(SeekFrom::Current(0))?;
        next_sequence = header.end_sequence + 1;
        block_count += 1;
        event_count += header.event_count as u64;
    }

    Ok(ValidPrefix {
        valid_end,
        next_sequence,
        block_count,
        event_count,
        event_type_count: file_header.event_type_count,
    })
}
