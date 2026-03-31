use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};

use crate::block::{self, BlockHeader};
use crate::event::EventRecord;
use crate::format::{self, BLOCK_HEADER_SIZE, FILE_HEADER_SIZE};

const READ_BUF_SIZE: usize = 256 * 1024;

pub struct FlowReader {
    reader: BufReader<File>,
    filter: Option<[u8; format::BITSET_SIZE]>,
    event_type_count: u16,
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
        })
    }

    /// Set a filter so only blocks containing at least one of the given event type IDs are scanned.
    /// Within matching blocks, only events with matching IDs are yielded.
    pub fn with_filter(mut self, event_type_ids: &[u16]) -> Self {
        self.filter = Some(block::build_filter(event_type_ids));
        self
    }

    pub fn event_type_count(&self) -> u16 {
        self.event_type_count
    }

    /// Iterate over all matching events in the file.
    pub fn into_iter(self) -> FlowIterator {
        FlowIterator {
            reader: self.reader,
            filter: self.filter,
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
    finished: bool,
    block_data: Vec<u8>,
    block_offset: usize,
    block_remaining: u32,
}

impl FlowIterator {
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

        self.block_data.resize(header.data_size as usize, 0);
        self.reader.read_exact(&mut self.block_data)?;
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
