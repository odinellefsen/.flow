use std::fs::{File, OpenOptions};
use std::io::{self, Write};

use crate::block::BlockHeader;
use crate::event::EventRecord;
use crate::format;
use crate::recovery;

pub struct FlowWriter {
    file: File,
    buffer: Vec<EventRecord>,
    next_sequence: u64,
    block_size: usize,
    #[allow(dead_code)]
    event_type_count: u16,
}

impl FlowWriter {
    pub fn create(path: &str, event_type_count: u16, block_size: usize) -> io::Result<Self> {
        Self::create_at_sequence(path, event_type_count, block_size, 0)
    }

    pub fn create_at_sequence(
        path: &str,
        event_type_count: u16,
        block_size: usize,
        start_sequence: u64,
    ) -> io::Result<Self> {
        let mut file = File::create(path)?;
        let header = format::encode_file_header(event_type_count);
        file.write_all(&header)?;

        Ok(Self {
            file,
            buffer: Vec::with_capacity(block_size),
            next_sequence: start_sequence,
            block_size,
            event_type_count,
        })
    }

    pub fn open(path: &str, block_size: usize) -> io::Result<Self> {
        let valid = recovery::validate_file(path)?;

        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(valid.valid_end)?;

        let mut writer = Self {
            file,
            buffer: Vec::with_capacity(block_size),
            next_sequence: valid.next_sequence,
            block_size,
            event_type_count: valid.event_type_count,
        };

        writer
            .file
            .seek(io::SeekFrom::End(0))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(writer)
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Append an event. The sequence field is overwritten with the next
    /// monotonic sequence number; all other fields are stored as-is.
    pub fn append(&mut self, mut event: EventRecord) -> io::Result<u64> {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        event.sequence = seq;

        self.buffer.push(event);

        if self.buffer.len() >= self.block_size {
            self.flush()?;
        }

        Ok(seq)
    }

    /// Flush buffered events as a single block to the OS page cache.
    /// No-op if the buffer is empty.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let mut data_buf: Vec<u8> = Vec::new();
        let mut header = BlockHeader::new();
        header.start_sequence = self.buffer[0].sequence;
        header.end_sequence = self.buffer[self.buffer.len() - 1].sequence;
        header.event_count = self.buffer.len() as u32;

        for event in &self.buffer {
            header.set_event_type(event.event_type_id);
            event.encode(&mut data_buf);
        }

        header.data_size = data_buf.len() as u32;
        header.checksum = format::crc32c(&data_buf);

        self.file.write_all(&header.encode())?;
        self.file.write_all(&data_buf)?;

        self.buffer.clear();

        Ok(())
    }
}

use std::io::Seek;

impl Drop for FlowWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
