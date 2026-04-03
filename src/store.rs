use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeInfo {
    pub id: u16,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub flow_type: String,
    pub version: u32,
    pub event_type_count: u16,
    pub bucket_duration_ms: u64,
    #[serde(default)]
    pub event_types: Vec<EventTypeInfo>,
    pub segments: Vec<SegmentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub file: String,
    pub bucket_start_ms: u64,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub event_count: u64,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub segment_bucket_ms: u64,
    pub sequence: u64,
}

pub struct FlowStore {
    dir: PathBuf,
    pub manifest: Manifest,
}

impl FlowStore {
    pub fn create(
        dir: &str,
        flow_type: &str,
        event_type_count: u16,
        bucket_duration_ms: u64,
    ) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let manifest = Manifest {
            flow_type: flow_type.to_string(),
            version: 1,
            event_type_count,
            bucket_duration_ms,
            event_types: Vec::new(),
            segments: Vec::new(),
        };

        let store = Self {
            dir: PathBuf::from(dir),
            manifest,
        };
        store.save_manifest()?;

        Ok(store)
    }

    /// Register a human-readable name for an event type ID.
    /// Overwrites if the ID already exists.
    pub fn register_event_type(&mut self, id: u16, name: &str) {
        if let Some(existing) = self.manifest.event_types.iter_mut().find(|e| e.id == id) {
            existing.name = name.to_string();
        } else {
            self.manifest.event_types.push(EventTypeInfo {
                id,
                name: name.to_string(),
            });
            self.manifest.event_types.sort_by_key(|e| e.id);
        }
    }

    /// Look up the name for an event type ID.
    pub fn event_type_name(&self, id: u16) -> Option<&str> {
        self.manifest
            .event_types
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.name.as_str())
    }

    pub fn open(dir: &str) -> io::Result<Self> {
        let dir = PathBuf::from(dir);
        let manifest_path = dir.join("manifest.json");
        let data = fs::read_to_string(&manifest_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("failed to read manifest at {}: {}", manifest_path.display(), e),
            )
        })?;
        let manifest: Manifest = serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self { dir, manifest })
    }

    pub fn save_manifest(&self) -> io::Result<()> {
        let path = self.dir.join("manifest.json");
        let data = serde_json::to_string_pretty(&self.manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(path, data)?;
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn segment_path(&self, bucket_start_ms: u64) -> PathBuf {
        self.dir.join(format!("{}.flow", bucket_start_ms))
    }

    pub fn segment_filename(bucket_start_ms: u64) -> String {
        format!("{}.flow", bucket_start_ms)
    }

    /// Compute the bucket start time for a given timestamp in nanoseconds.
    pub fn bucket_for_timestamp(&self, timestamp_nanos: u64) -> u64 {
        let ts_ms = timestamp_nanos / 1_000_000;
        let bucket = ts_ms - (ts_ms % self.manifest.bucket_duration_ms);
        bucket
    }

    /// Find the segment info for a given bucket start time.
    pub fn segment_for_bucket(&self, bucket_start_ms: u64) -> Option<&SegmentInfo> {
        self.manifest
            .segments
            .iter()
            .find(|s| s.bucket_start_ms == bucket_start_ms)
    }

    /// Find the segment that covers a given timestamp (in milliseconds).
    pub fn segment_for_time_ms(&self, timestamp_ms: u64) -> Option<&SegmentInfo> {
        let bucket = timestamp_ms - (timestamp_ms % self.manifest.bucket_duration_ms);
        self.segment_for_bucket(bucket)
    }

    /// Return segments from a cursor position forward, in order.
    pub fn segments_from(&self, cursor: &Cursor) -> Vec<&SegmentInfo> {
        self.manifest
            .segments
            .iter()
            .filter(|s| s.bucket_start_ms >= cursor.segment_bucket_ms)
            .collect()
    }

    /// Return all segments in order.
    pub fn segments(&self) -> &[SegmentInfo] {
        &self.manifest.segments
    }

    /// Path to a per-event-type Zstd dictionary file in this flow type directory.
    pub fn dict_path(&self, event_type_id: u16) -> std::path::PathBuf {
        self.dir.join(crate::compression::dict_filename(event_type_id))
    }

    /// Add or update a segment entry in the manifest.
    pub fn upsert_segment(&mut self, info: SegmentInfo) {
        if let Some(existing) = self
            .manifest
            .segments
            .iter_mut()
            .find(|s| s.bucket_start_ms == info.bucket_start_ms)
        {
            *existing = info;
        } else {
            self.manifest.segments.push(info);
            self.manifest
                .segments
                .sort_by_key(|s| s.bucket_start_ms);
        }
    }

    /// The next sequence number based on the last segment, or 0 if empty.
    pub fn next_sequence(&self) -> u64 {
        self.manifest
            .segments
            .last()
            .map(|s| s.end_sequence + 1)
            .unwrap_or(0)
    }

    /// The latest segment, if any.
    pub fn latest_segment(&self) -> Option<&SegmentInfo> {
        self.manifest.segments.last()
    }
}
