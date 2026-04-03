use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::block::{self, BlockHeader};
use crate::compression::{self, CODEC_NONE, CODEC_ZSTD, CODEC_ZSTD_DICT};
use crate::event::EventRecord;
use crate::format::{self, BLOCK_HEADER_SIZE, FILE_HEADER_SIZE};

const READ_BUF_SIZE: usize = 256 * 1024;

pub struct FlowReader {
    reader: BufReader<File>,
    filter: Option<[u8; format::BITSET_SIZE]>,
    event_type_count: u16,
    /// Per-type-id Zstd dictionaries, used when a block has codec=CODEC_ZSTD_DICT.
    dictionaries: HashMap<u16, Vec<u8>>,
}

impl FlowReader {
    pub fn open(path: &str) -> io::Result<Self> {
        let mut file = File::open(path)?;

        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = format::decode_file_header(&header_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            reader: BufReader::with_capacity(READ_BUF_SIZE, file),
            filter: None,
            event_type_count: header.event_type_count,
            dictionaries: HashMap::new(),
        })
    }

    /// Load all `dict_typeN.zst` files from `dir` so this reader can decode
    /// compressed blocks that were written with per-type dictionaries.
    pub fn with_dict_dir(mut self, dir: &str) -> io::Result<Self> {
        self.dictionaries = compression::load_dictionaries(Path::new(dir))?;
        Ok(self)
    }

    /// Supply pre-loaded dictionaries (e.g. from a SegmentedReader that has
    /// already loaded them once for all segments).
    pub fn with_dictionaries(mut self, dicts: HashMap<u16, Vec<u8>>) -> Self {
        self.dictionaries = dicts;
        self
    }

    pub fn with_filter(mut self, event_type_ids: &[u16]) -> Self {
        self.filter = Some(block::build_filter(event_type_ids));
        self
    }

    pub fn event_type_count(&self) -> u16 {
        self.event_type_count
    }

    pub fn into_iter(self) -> FlowIterator {
        FlowIterator {
            reader: self.reader,
            filter: self.filter,
            dictionaries: self.dictionaries,
            finished: false,
            block_data: Vec::new(),
            block_offset: 0,
            block_remaining: 0,
        }
    }
}

pub struct FlowIterator {
    reader: BufReader<File>,
    filter: Option<[u8; format::BITSET_SIZE]>,
    dictionaries: HashMap<u16, Vec<u8>>,
    finished: bool,
    /// Decompressed (or raw) block data currently being iterated.
    block_data: Vec<u8>,
    block_offset: usize,
    block_remaining: u32,
}

impl FlowIterator {
    /// Skip forward past all events with sequence <= `last_sequence`.
    /// Uses block headers to jump over entire blocks where possible.
    pub fn skip_to_after(&mut self, last_sequence: u64) -> io::Result<()> {
        loop {
            if self.finished {
                return Ok(());
            }

            if self.block_remaining == 0 {
                let mut hdr_buf = [0u8; BLOCK_HEADER_SIZE];
                match self.reader.read_exact(&mut hdr_buf) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        self.finished = true;
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }

                let header = BlockHeader::decode(&hdr_buf);

                if header.end_sequence <= last_sequence {
                    // Entire block is before the cursor -- seek past it without reading
                    self.reader
                        .seek(SeekFrom::Current(header.data_size as i64))?;
                    continue;
                }

                self.block_data =
                    read_and_decompress(&mut self.reader, &header, &self.dictionaries)?;
                self.block_offset = 0;
                self.block_remaining = header.event_count;
            }

            while self.block_remaining > 0 {
                let data = &self.block_data[self.block_offset..];
                match EventRecord::decode(data) {
                    Ok((record, consumed)) => {
                        if record.sequence <= last_sequence {
                            self.block_offset += consumed;
                            self.block_remaining -= 1;
                        } else {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                    }
                }
            }
        }
    }

    fn load_next_block(&mut self) -> io::Result<Option<BlockHeader>> {
        let mut hdr_buf = [0u8; BLOCK_HEADER_SIZE];
        match self.reader.read_exact(&mut hdr_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.finished = true;
                return Ok(None);
            }
            Err(e) => return Err(e),
        }

        let header = BlockHeader::decode(&hdr_buf);

        if let Some(ref filter) = self.filter {
            if !header.matches_filter(filter) {
                self.reader
                    .seek(SeekFrom::Current(header.data_size as i64))?;
                self.block_remaining = 0;
                self.block_offset = 0;
                self.block_data.clear();
                return self.load_next_block();
            }
        }

        self.block_data =
            read_and_decompress(&mut self.reader, &header, &self.dictionaries)?;
        self.block_offset = 0;
        self.block_remaining = header.event_count;

        Ok(Some(header))
    }
}

impl Iterator for FlowIterator {
    type Item = io::Result<EventRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }

            if self.block_remaining == 0 {
                match self.load_next_block() {
                    Ok(Some(_)) => continue,
                    Ok(None) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let data = &self.block_data[self.block_offset..];
            match EventRecord::decode(data) {
                Ok((record, consumed)) => {
                    self.block_offset += consumed;
                    self.block_remaining -= 1;

                    if let Some(ref filter) = self.filter {
                        let byte_idx = (record.event_type_id / 8) as usize;
                        let bit_idx = record.event_type_id % 8;
                        if byte_idx < format::BITSET_SIZE
                            && (filter[byte_idx] & (1 << bit_idx)) != 0
                        {
                            return Some(Ok(record));
                        }
                        continue;
                    }

                    return Some(Ok(record));
                }
                Err(e) => {
                    return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e)));
                }
            }
        }
    }
}

// ── Internal helper ───────────────────────────────────────────────────────────

/// Read raw block bytes from `reader`, then decompress according to `header.codec`.
/// Returns the event-record bytes ready for `EventRecord::decode`.
pub(crate) fn read_and_decompress(
    reader: &mut BufReader<File>,
    header: &BlockHeader,
    dicts: &HashMap<u16, Vec<u8>>,
) -> io::Result<Vec<u8>> {
    let mut raw = vec![0u8; header.data_size as usize];
    reader.read_exact(&mut raw)?;

    match header.codec {
        CODEC_NONE => Ok(raw),
        CODEC_ZSTD => compression::decompress(&raw),
        CODEC_ZSTD_DICT => {
            if let Some(dict) = dicts.get(&header.dict_id) {
                compression::decompress_with_dict(&raw, dict)
            } else {
                // Dictionary missing -- fall back to plain Zstd decompress.
                // This handles the case where dictionaries haven't been loaded.
                compression::decompress(&raw)
            }
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown block codec: {other}"),
        )),
    }
}
