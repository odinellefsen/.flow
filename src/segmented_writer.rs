use std::fs;
use std::io;

use crate::recovery;
use crate::store::{FlowStore, SegmentInfo};
use crate::writer::FlowWriter;

pub struct SegmentedWriter {
    store: FlowStore,
    current_writer: Option<FlowWriter>,
    current_bucket_ms: u64,
    next_sequence: u64,
    block_size: usize,
    segment_start_sequence: u64,
    segment_event_count: u64,
}

impl SegmentedWriter {
    pub fn create(
        dir: &str,
        flow_type: &str,
        event_type_count: u16,
        bucket_duration_ms: u64,
        block_size: usize,
    ) -> io::Result<Self> {
        let store = FlowStore::create(dir, flow_type, event_type_count, bucket_duration_ms)?;

        Ok(Self {
            store,
            current_writer: None,
            current_bucket_ms: 0,
            next_sequence: 0,
            block_size,
            segment_start_sequence: 0,
            segment_event_count: 0,
        })
    }

    pub fn open(dir: &str, block_size: usize) -> io::Result<Self> {
        let store = FlowStore::open(dir)?;
        let mut next_sequence = store.next_sequence();

        let (current_writer, current_bucket_ms, segment_start_sequence, segment_event_count) =
            if let Some(latest) = store.latest_segment() {
                let path = store.segment_path(latest.bucket_start_ms);
                let path_str = path.to_string_lossy().to_string();

                let valid = recovery::validate_file(&path_str)?;
                next_sequence = valid.next_sequence;

                let writer = FlowWriter::open(&path_str, block_size)?;

                (
                    Some(writer),
                    latest.bucket_start_ms,
                    latest.start_sequence,
                    valid.event_count,
                )
            } else {
                (None, 0, 0, 0)
            };

        Ok(Self {
            store,
            current_writer,
            current_bucket_ms,
            next_sequence,
            block_size,
            segment_start_sequence,
            segment_event_count,
        })
    }

    pub fn append(
        &mut self,
        event_type_id: u16,
        timestamp: u64,
        payload: Vec<u8>,
    ) -> io::Result<u64> {
        let bucket = self.store.bucket_for_timestamp(timestamp);

        if self.current_writer.is_none() || bucket != self.current_bucket_ms {
            self.rotate_to_bucket(bucket)?;
        }

        let seq = self.next_sequence;
        self.next_sequence += 1;
        self.segment_event_count += 1;

        self.current_writer
            .as_mut()
            .unwrap()
            .append(event_type_id, timestamp, payload)?;

        Ok(seq)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.current_writer {
            writer.flush()?;
        }
        self.update_current_segment_info()?;
        self.store.save_manifest()?;
        Ok(())
    }

    pub fn store(&self) -> &FlowStore {
        &self.store
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    fn rotate_to_bucket(&mut self, bucket_ms: u64) -> io::Result<()> {
        self.close_current_segment()?;

        let path = self.store.segment_path(bucket_ms);
        let path_str = path.to_string_lossy().to_string();

        let writer = FlowWriter::create_at_sequence(
            &path_str,
            self.store.manifest.event_type_count,
            self.block_size,
            self.next_sequence,
        )?;

        self.current_writer = Some(writer);
        self.current_bucket_ms = bucket_ms;
        self.segment_start_sequence = self.next_sequence;
        self.segment_event_count = 0;

        Ok(())
    }

    fn close_current_segment(&mut self) -> io::Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush()?;
        }
        self.update_current_segment_info()?;
        self.store.save_manifest()?;
        Ok(())
    }

    fn update_current_segment_info(&mut self) -> io::Result<()> {
        if self.segment_event_count == 0 {
            return Ok(());
        }

        let path = self.store.segment_path(self.current_bucket_ms);
        let byte_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let end_sequence = if self.next_sequence > 0 {
            self.next_sequence - 1
        } else {
            0
        };

        self.store.upsert_segment(SegmentInfo {
            file: FlowStore::segment_filename(self.current_bucket_ms),
            bucket_start_ms: self.current_bucket_ms,
            start_sequence: self.segment_start_sequence,
            end_sequence,
            event_count: self.segment_event_count,
            byte_size,
        });

        Ok(())
    }
}

impl Drop for SegmentedWriter {
    fn drop(&mut self) {
        let _ = self.close_current_segment();
    }
}
