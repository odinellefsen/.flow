use crate::format;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub sequence: u64,
    pub event_type_id: u16,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

impl EventRecord {
    /// Encoded size: 8 (seq) + 2 (type) + 8 (ts) + 4 (payload_len) + payload + 4 (crc)
    pub fn encoded_size(&self) -> usize {
        format::EVENT_RECORD_MIN_SIZE + self.payload.len()
    }

    /// Serialize into `buf`, appending CRC32C of the record body.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let start = buf.len();
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&self.event_type_id.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        let crc = format::crc32c(&buf[start..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    /// Decode one record from the front of `data`.
    /// Returns the record and the number of bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        if data.len() < format::EVENT_RECORD_MIN_SIZE {
            return Err("buffer too short for event record");
        }

        let sequence = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let event_type_id = u16::from_le_bytes(data[8..10].try_into().unwrap());
        let timestamp = u64::from_le_bytes(data[10..18].try_into().unwrap());
        let payload_len = u32::from_le_bytes(data[18..22].try_into().unwrap()) as usize;

        let total = format::EVENT_RECORD_MIN_SIZE + payload_len;
        if data.len() < total {
            return Err("buffer too short for event payload");
        }

        let payload = data[22..22 + payload_len].to_vec();

        let body_end = 22 + payload_len;
        let stored_crc = u32::from_le_bytes(data[body_end..body_end + 4].try_into().unwrap());
        let computed_crc = format::crc32c(&data[0..body_end]);
        if stored_crc != computed_crc {
            return Err("event record checksum mismatch");
        }

        Ok((
            EventRecord {
                sequence,
                event_type_id,
                timestamp,
                payload,
            },
            total,
        ))
    }
}
