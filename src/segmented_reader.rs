use std::io;

use crate::event::EventRecord;
use crate::reader::FlowReader;
use crate::store::{Cursor, FlowStore, SegmentInfo};

pub struct SegmentedReader {
    store: FlowStore,
    filter: Option<Vec<u16>>,
    start_cursor: Option<Cursor>,
}

impl SegmentedReader {
    pub fn open(dir: &str) -> io::Result<Self> {
        let store = FlowStore::open(dir)?;
        Ok(Self {
            store,
            filter: None,
            start_cursor: None,
        })
    }

    pub fn open_from(dir: &str, cursor: Cursor) -> io::Result<Self> {
        let store = FlowStore::open(dir)?;
        Ok(Self {
            store,
            filter: None,
            start_cursor: Some(cursor),
        })
    }

    pub fn with_filter(mut self, event_type_ids: &[u16]) -> Self {
        self.filter = Some(event_type_ids.to_vec());
        self
    }

    pub fn into_iter(self) -> SegmentedIterator {
        let segments: Vec<SegmentInfo> = if let Some(ref cursor) = self.start_cursor {
            self.store
                .segments_from(cursor)
                .into_iter()
                .cloned()
                .collect()
        } else {
            self.store.segments().to_vec()
        };

        SegmentedIterator {
            dir: self.store.dir().to_path_buf(),
            segments,
            segment_index: 0,
            current_iter: None,
            filter_ids: self.filter,
            start_cursor: self.start_cursor,
            cursor_applied: false,
            last_bucket_ms: 0,
            last_sequence: 0,
        }
    }
}

pub struct SegmentedIterator {
    dir: std::path::PathBuf,
    segments: Vec<SegmentInfo>,
    segment_index: usize,
    current_iter: Option<crate::reader::FlowIterator>,
    filter_ids: Option<Vec<u16>>,
    start_cursor: Option<Cursor>,
    cursor_applied: bool,
    last_bucket_ms: u64,
    last_sequence: u64,
}

impl SegmentedIterator {
    fn open_next_segment(&mut self) -> io::Result<bool> {
        if self.segment_index >= self.segments.len() {
            return Ok(false);
        }

        let seg = &self.segments[self.segment_index];
        let path = self.dir.join(&seg.file);
        let path_str = path.to_string_lossy().to_string();

        let reader = FlowReader::open(&path_str)?;
        let reader = if let Some(ref ids) = self.filter_ids {
            reader.with_filter(ids)
        } else {
            reader
        };

        let mut iter = reader.into_iter();

        if !self.cursor_applied {
            if let Some(ref cursor) = self.start_cursor {
                if seg.bucket_start_ms == cursor.segment_bucket_ms {
                    iter.skip_to_after(cursor.sequence)?;
                }
            }
            self.cursor_applied = true;
        }

        self.last_bucket_ms = seg.bucket_start_ms;
        self.current_iter = Some(iter);
        self.segment_index += 1;

        Ok(true)
    }

    /// Get a cursor representing the current read position.
    pub fn cursor(&self) -> Cursor {
        Cursor {
            segment_bucket_ms: self.last_bucket_ms,
            sequence: self.last_sequence,
        }
    }
}

impl Iterator for SegmentedIterator {
    type Item = io::Result<(EventRecord, Cursor)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.current_iter {
                match iter.next() {
                    Some(Ok(record)) => {
                        self.last_sequence = record.sequence;
                        let cursor = Cursor {
                            segment_bucket_ms: self.last_bucket_ms,
                            sequence: record.sequence,
                        };
                        return Some(Ok((record, cursor)));
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        self.current_iter = None;
                    }
                }
            }

            match self.open_next_segment() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
