use crate::format::{self, FLAG_HAS_CAUSATION_ID, FLAG_HAS_CORRELATION_ID};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    /// Storage-layer sequence number. Assigned by the writer; do not set manually.
    pub sequence: u64,
    /// UUID v7 as raw bytes -- the customer-facing stable event identifier.
    pub event_id: [u8; 16],
    /// Integer ID mapping to the event type name (e.g. 0 → "food-item.created.v0").
    pub event_type_id: u16,
    /// Business timestamp: when the event occurred (nanoseconds since Unix epoch).
    pub event_time: u64,
    /// When the event became valid/effective (nanoseconds since Unix epoch).
    pub valid_time: u64,
    /// When Flowcore received and stored the event (nanoseconds since Unix epoch).
    pub recorded_time: u64,
    /// Optional: links this event to a broader operation (UUID as raw bytes).
    pub correlation_id: Option<[u8; 16]>,
    /// Optional: the event that directly caused this one (UUID as raw bytes).
    pub causation_id: Option<[u8; 16]>,
    /// System-level key-value metadata (e.g. TTL, sensitivity flags).
    pub metadata: Vec<(String, String)>,
    /// User-defined payload (opaque bytes -- JSON, protobuf, msgpack, etc.).
    pub payload: Vec<u8>,
}

impl EventRecord {
    /// Convenience constructor for the common case in tests and simple writers.
    /// Sequence is set to 0 and will be overwritten by the writer.
    pub fn new(event_type_id: u16, event_time: u64, payload: Vec<u8>) -> Self {
        Self {
            sequence: 0,
            event_id: [0u8; 16],
            event_type_id,
            event_time,
            valid_time: event_time,
            recorded_time: event_time,
            correlation_id: None,
            causation_id: None,
            metadata: Vec::new(),
            payload,
        }
    }

    /// Wire format:
    ///
    /// [seq: 8][event_id: 16][type: 2][event_time: 8][valid_time: 8]
    /// [recorded_time: 8][flags: 1][correlation_id?: 16][causation_id?: 16]
    /// [metadata_count: 2]([key_len: 2][key][val_len: 2][val])*
    /// [payload_len: 4][payload][crc32c: 4]
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let start = buf.len();

        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&self.event_id);
        buf.extend_from_slice(&self.event_type_id.to_le_bytes());
        buf.extend_from_slice(&self.event_time.to_le_bytes());
        buf.extend_from_slice(&self.valid_time.to_le_bytes());
        buf.extend_from_slice(&self.recorded_time.to_le_bytes());

        let mut flags: u8 = 0;
        if self.correlation_id.is_some() {
            flags |= FLAG_HAS_CORRELATION_ID;
        }
        if self.causation_id.is_some() {
            flags |= FLAG_HAS_CAUSATION_ID;
        }
        buf.push(flags);

        if let Some(ref id) = self.correlation_id {
            buf.extend_from_slice(id);
        }
        if let Some(ref id) = self.causation_id {
            buf.extend_from_slice(id);
        }

        buf.extend_from_slice(&(self.metadata.len() as u16).to_le_bytes());
        for (key, val) in &self.metadata {
            let kb = key.as_bytes();
            let vb = val.as_bytes();
            buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            buf.extend_from_slice(kb);
            buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            buf.extend_from_slice(vb);
        }

        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);

        let crc = format::crc32c(&buf[start..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    /// Decode one record from `data`. Returns the record and bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        if data.len() < format::EVENT_RECORD_MIN_SIZE {
            return Err("buffer too short for event record");
        }

        let mut pos = 0;

        let sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let event_id: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
        pos += 16;

        let event_type_id = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;

        let event_time = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let valid_time = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let recorded_time = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        if data.len() < pos + 1 {
            return Err("buffer too short for flags");
        }
        let flags = data[pos];
        pos += 1;

        let correlation_id = if flags & FLAG_HAS_CORRELATION_ID != 0 {
            if data.len() < pos + 16 {
                return Err("buffer too short for correlation_id");
            }
            let id: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            pos += 16;
            Some(id)
        } else {
            None
        };

        let causation_id = if flags & FLAG_HAS_CAUSATION_ID != 0 {
            if data.len() < pos + 16 {
                return Err("buffer too short for causation_id");
            }
            let id: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            pos += 16;
            Some(id)
        } else {
            None
        };

        if data.len() < pos + 2 {
            return Err("buffer too short for metadata_count");
        }
        let metadata_count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut metadata = Vec::with_capacity(metadata_count);
        for _ in 0..metadata_count {
            if data.len() < pos + 2 {
                return Err("buffer too short for metadata key length");
            }
            let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if data.len() < pos + key_len {
                return Err("buffer too short for metadata key");
            }
            let key = String::from_utf8(data[pos..pos + key_len].to_vec())
                .map_err(|_| "invalid utf-8 in metadata key")?;
            pos += key_len;

            if data.len() < pos + 2 {
                return Err("buffer too short for metadata value length");
            }
            let val_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if data.len() < pos + val_len {
                return Err("buffer too short for metadata value");
            }
            let val = String::from_utf8(data[pos..pos + val_len].to_vec())
                .map_err(|_| "invalid utf-8 in metadata value")?;
            pos += val_len;

            metadata.push((key, val));
        }

        if data.len() < pos + 4 {
            return Err("buffer too short for payload length");
        }
        let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if data.len() < pos + payload_len + 4 {
            return Err("buffer too short for payload or checksum");
        }
        let payload = data[pos..pos + payload_len].to_vec();
        pos += payload_len;

        let stored_crc = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        let computed_crc = format::crc32c(&data[0..pos]);
        if stored_crc != computed_crc {
            return Err("event record checksum mismatch");
        }
        pos += 4;

        Ok((
            EventRecord {
                sequence,
                event_id,
                event_type_id,
                event_time,
                valid_time,
                recorded_time,
                correlation_id,
                causation_id,
                metadata,
                payload,
            },
            pos,
        ))
    }
}
