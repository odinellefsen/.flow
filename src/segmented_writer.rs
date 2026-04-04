use std::fs;
use std::io;

use crate::event::EventRecord;
use crate::format::PAYLOAD_CODEC_SCHEMA;
use crate::recovery;
use crate::schema::PayloadSchema;
use crate::store::{FlowStore, SegmentInfo};
use crate::writer::FlowWriter;

pub struct SegmentedWriter {
    store: FlowStore,
    current_writer: Option<FlowWriter>,
    current_bucket_ms: u64,
    /// Last known next_sequence when there is no active writer.
    last_sequence: u64,
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
            last_sequence: 0,
            block_size,
            segment_start_sequence: 0,
            segment_event_count: 0,
        })
    }

    pub fn open(dir: &str, block_size: usize) -> io::Result<Self> {
        let store = FlowStore::open(dir)?;

        let (current_writer, current_bucket_ms, last_sequence, segment_start_sequence, segment_event_count) =
            if let Some(latest) = store.latest_segment() {
                let path = store.segment_path(latest.bucket_start_ms);
                let path_str = path.to_string_lossy().to_string();

                let valid = recovery::validate_file(&path_str)?;
                let writer = FlowWriter::open(&path_str, block_size)?;
                let next_seq = writer.next_sequence();

                (
                    Some(writer),
                    latest.bucket_start_ms,
                    next_seq,
                    latest.start_sequence,
                    valid.event_count,
                )
            } else {
                (None, 0, 0, 0, 0)
            };

        Ok(Self {
            store,
            current_writer,
            current_bucket_ms,
            last_sequence,
            block_size,
            segment_start_sequence,
            segment_event_count,
        })
    }

    /// Append an event. The bucket is determined from `event.event_time`.
    /// The event's sequence field is assigned by the underlying writer.
    ///
    /// If the event type has no locked schema yet and the payload is non-empty,
    /// its schema is inferred from this event and locked in the manifest.
    /// All subsequent events of the same type are validated and schema-encoded.
    pub fn append(&mut self, mut event: EventRecord) -> io::Result<u64> {
        let bucket = self.store.bucket_for_timestamp(event.event_time);

        if self.current_writer.is_none() || bucket != self.current_bucket_ms {
            self.rotate_to_bucket(bucket)?;
        }

        event = self.apply_schema(event)?;

        let seq = self.current_writer.as_mut().unwrap().append(event)?;
        self.segment_event_count += 1;
        Ok(seq)
    }

    fn apply_schema(&mut self, mut event: EventRecord) -> io::Result<EventRecord> {
        if event.payload.is_empty() {
            return Ok(event);
        }

        let type_id = event.event_type_id;

        if self.store.get_schema(type_id).is_none() {
            // First event of this type -- infer and lock schema
            match PayloadSchema::infer(&event.payload) {
                Ok(schema) => {
                    self.store.lock_schema(type_id, schema);
                    self.store.save_manifest()?;
                }
                Err(_) => {
                    // Non-JSON payload or inference failed -- store raw
                    return Ok(event);
                }
            }
        }

        // Schema is locked -- validate and encode
        if let Some(schema) = self.store.get_schema(type_id) {
            let schema = schema.clone();
            schema
                .validate(&event.payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            event.payload = schema
                .encode_payload(&event.payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            event.payload_codec = PAYLOAD_CODEC_SCHEMA;
        }

        Ok(event)
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
        self.current_writer
            .as_ref()
            .map(|w| w.next_sequence())
            .unwrap_or(self.last_sequence)
    }

    fn rotate_to_bucket(&mut self, bucket_ms: u64) -> io::Result<()> {
        let next_seq = self.close_current_segment()?;

        let path = self.store.segment_path(bucket_ms);
        let path_str = path.to_string_lossy().to_string();

        let writer = FlowWriter::create_at_sequence(
            &path_str,
            self.store.manifest.event_type_count,
            self.block_size,
            next_seq,
        )?;

        self.current_writer = Some(writer);
        self.current_bucket_ms = bucket_ms;
        self.segment_start_sequence = next_seq;
        self.segment_event_count = 0;

        Ok(())
    }

    fn close_current_segment(&mut self) -> io::Result<u64> {
        let next_seq = if let Some(mut writer) = self.current_writer.take() {
            let seq = writer.next_sequence();
            writer.flush()?;
            seq
        } else {
            self.last_sequence
        };
        self.last_sequence = next_seq;
        self.update_current_segment_info()?;
        self.store.save_manifest()?;
        Ok(next_seq)
    }

    fn update_current_segment_info(&mut self) -> io::Result<()> {
        if self.segment_event_count == 0 {
            return Ok(());
        }

        let path = self.store.segment_path(self.current_bucket_ms);
        let byte_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let end_sequence = self.next_sequence().saturating_sub(1);

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
