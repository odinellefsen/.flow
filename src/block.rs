use crate::compression::{CODEC_NONE, NO_DICT_ID};
use crate::format::{self, BITSET_SIZE, BLOCK_HEADER_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub event_count: u32,
    /// On-disk byte count of the block data (compressed size when codec != CODEC_NONE).
    pub data_size: u32,
    pub event_type_bitset: [u8; BITSET_SIZE],
    /// CRC32C of the on-disk block data bytes (i.e. the compressed bytes when compressed).
    pub checksum: u32,
    /// Compression codec. One of CODEC_NONE / CODEC_ZSTD / CODEC_ZSTD_DICT.
    pub codec: u8,
    /// Event type ID whose trained dictionary was used (NO_DICT_ID when codec != CODEC_ZSTD_DICT).
    pub dict_id: u16,
}

impl BlockHeader {
    pub fn new() -> Self {
        Self {
            start_sequence: 0,
            end_sequence: 0,
            event_count: 0,
            data_size: 0,
            event_type_bitset: [0u8; BITSET_SIZE],
            checksum: 0,
            codec: CODEC_NONE,
            dict_id: NO_DICT_ID,
        }
    }

    pub fn set_event_type(&mut self, id: u16) {
        let byte_idx = (id / 8) as usize;
        let bit_idx = id % 8;
        if byte_idx < BITSET_SIZE {
            self.event_type_bitset[byte_idx] |= 1 << bit_idx;
        }
    }

    pub fn contains_event_type(&self, id: u16) -> bool {
        let byte_idx = (id / 8) as usize;
        let bit_idx = id % 8;
        if byte_idx >= BITSET_SIZE {
            return false;
        }
        (self.event_type_bitset[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Returns true if any bit in the filter overlaps with this header's bitset.
    pub fn matches_filter(&self, filter: &[u8; BITSET_SIZE]) -> bool {
        for i in 0..BITSET_SIZE {
            if self.event_type_bitset[i] & filter[i] != 0 {
                return true;
            }
        }
        false
    }

    /// If this block contains exactly one event type, return its ID.
    /// Returns None if the block is empty or contains multiple types.
    pub fn single_event_type(&self) -> Option<u16> {
        let mut found: Option<u16> = None;
        for byte_idx in 0..BITSET_SIZE {
            if self.event_type_bitset[byte_idx] == 0 {
                continue;
            }
            for bit_idx in 0..8u16 {
                if self.event_type_bitset[byte_idx] & (1 << bit_idx) != 0 {
                    let id = (byte_idx as u16) * 8 + bit_idx;
                    if found.is_some() {
                        return None; // multiple types
                    }
                    found = Some(id);
                }
            }
        }
        found
    }

    /// Block header wire layout (64 bytes):
    ///
    ///  0.. 8  start_sequence  u64 LE
    ///  8..16  end_sequence    u64 LE
    /// 16..20  event_count     u32 LE
    /// 20..24  data_size       u32 LE  (compressed on-disk size)
    /// 24..56  event_type_bitset [u8; 32]
    /// 56..60  checksum        u32 LE  (CRC32C of on-disk block bytes)
    /// 60      codec           u8
    /// 61..63  dict_id         u16 LE
    /// 63      reserved        u8
    pub fn encode(&self) -> [u8; BLOCK_HEADER_SIZE] {
        let mut buf = [0u8; BLOCK_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.start_sequence.to_le_bytes());
        buf[8..16].copy_from_slice(&self.end_sequence.to_le_bytes());
        buf[16..20].copy_from_slice(&self.event_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self.data_size.to_le_bytes());
        buf[24..56].copy_from_slice(&self.event_type_bitset);
        buf[56..60].copy_from_slice(&self.checksum.to_le_bytes());
        buf[60] = self.codec;
        buf[61..63].copy_from_slice(&self.dict_id.to_le_bytes());
        // byte 63: reserved
        buf
    }

    pub fn decode(buf: &[u8; BLOCK_HEADER_SIZE]) -> Self {
        let start_sequence = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let end_sequence = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let event_count = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let data_size = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        let mut event_type_bitset = [0u8; BITSET_SIZE];
        event_type_bitset.copy_from_slice(&buf[24..56]);
        let checksum = u32::from_le_bytes(buf[56..60].try_into().unwrap());
        let codec = buf[60];
        let dict_id = u16::from_le_bytes(buf[61..63].try_into().unwrap());

        Self {
            start_sequence,
            end_sequence,
            event_count,
            data_size,
            event_type_bitset,
            checksum,
            codec,
            dict_id,
        }
    }

    /// Validate the block data against the stored checksum.
    pub fn validate_data(&self, data: &[u8]) -> bool {
        format::crc32c(data) == self.checksum
    }
}

/// Build a filter bitset from a slice of event type IDs.
pub fn build_filter(event_type_ids: &[u16]) -> [u8; BITSET_SIZE] {
    let mut filter = [0u8; BITSET_SIZE];
    for &id in event_type_ids {
        let byte_idx = (id / 8) as usize;
        let bit_idx = id % 8;
        if byte_idx < BITSET_SIZE {
            filter[byte_idx] |= 1 << bit_idx;
        }
    }
    filter
}
