use std::io;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// UTF-8 string.
    String,
    /// IEEE 754 f64.
    Number,
    /// Boolean.
    Boolean,
    /// Arrays, nested objects -- stored as raw JSON bytes.
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
}

/// Locked schema for one event type, derived from its first payload.
/// All subsequent payloads must have exactly these fields (nulls allowed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadSchema {
    pub fields: Vec<SchemaField>,
}

#[derive(Debug)]
pub enum SchemaError {
    NotJsonObject,
    MissingField(String),
    TypeMismatch { field: String, expected: FieldType, got: String },
    ExtraFields(Vec<String>),
    ParseError(String),
    EncodeError(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::NotJsonObject => write!(f, "payload is not a JSON object"),
            SchemaError::MissingField(n) => write!(f, "missing field: {n}"),
            SchemaError::TypeMismatch { field, expected, got } => {
                write!(f, "field '{field}': expected {expected:?}, got {got}")
            }
            SchemaError::ExtraFields(fields) => write!(f, "extra fields: {fields:?}"),
            SchemaError::ParseError(e) => write!(f, "JSON parse error: {e}"),
            SchemaError::EncodeError(e) => write!(f, "encode error: {e}"),
        }
    }
}

impl From<SchemaError> for io::Error {
    fn from(e: SchemaError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

// ── Inference ─────────────────────────────────────────────────────────────────

impl PayloadSchema {
    /// Derive a schema from the first JSON payload of an event type.
    /// The field order from the JSON object is preserved.
    pub fn infer(payload: &[u8]) -> Result<Self, SchemaError> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| SchemaError::ParseError(e.to_string()))?;

        let obj = value.as_object().ok_or(SchemaError::NotJsonObject)?;

        let fields = obj
            .iter()
            .map(|(name, val)| SchemaField {
                name: name.clone(),
                field_type: infer_field_type(val),
            })
            .collect();

        Ok(PayloadSchema { fields })
    }

    // ── Validation ────────────────────────────────────────────────────────────

    /// Check that a JSON payload conforms to this schema.
    /// Nulls are allowed for any field. Extra fields are rejected.
    pub fn validate(&self, payload: &[u8]) -> Result<(), SchemaError> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| SchemaError::ParseError(e.to_string()))?;

        let obj = value.as_object().ok_or(SchemaError::NotJsonObject)?;

        // Check all schema fields are present (null is fine)
        for field in &self.fields {
            if !obj.contains_key(&field.name) {
                return Err(SchemaError::MissingField(field.name.clone()));
            }
        }

        // Check no extra fields
        let schema_names: std::collections::HashSet<&str> =
            self.fields.iter().map(|f| f.name.as_str()).collect();
        let extra: Vec<String> = obj
            .keys()
            .filter(|k| !schema_names.contains(k.as_str()))
            .cloned()
            .collect();
        if !extra.is_empty() {
            return Err(SchemaError::ExtraFields(extra));
        }

        // Check types for non-null values
        for field in &self.fields {
            let val = &obj[&field.name];
            if val.is_null() {
                continue;
            }
            if !type_matches(val, &field.field_type) {
                return Err(SchemaError::TypeMismatch {
                    field: field.name.clone(),
                    expected: field.field_type.clone(),
                    got: json_type_name(val).to_string(),
                });
            }
        }

        Ok(())
    }

    // ── Encoding ─────────────────────────────────────────────────────────────

    /// Encode a JSON object payload to value-only compact bytes.
    ///
    /// Wire format:
    ///   [null_bitmap: ceil(N/8) bytes]   -- bit i=1 means field i is null
    ///   for each non-null field (schema order):
    ///     String | Json:  [u32 LE len][utf-8 bytes]
    ///     Number:         [f64 LE 8 bytes]
    ///     Boolean:        [u8: 0=false, 1=true]
    pub fn encode_payload(&self, payload: &[u8]) -> Result<Vec<u8>, SchemaError> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| SchemaError::ParseError(e.to_string()))?;
        let obj = value.as_object().ok_or(SchemaError::NotJsonObject)?;

        let n = self.fields.len();
        let bitmap_bytes = (n + 7) / 8;
        let mut null_bitmap = vec![0u8; bitmap_bytes];
        let mut values: Vec<u8> = Vec::new();

        for (i, field) in self.fields.iter().enumerate() {
            let val = obj.get(&field.name).unwrap_or(&Value::Null);
            if val.is_null() {
                null_bitmap[i / 8] |= 1 << (i % 8);
                continue;
            }
            encode_value(val, &field.field_type, &mut values)?;
        }

        let mut out = null_bitmap;
        out.extend_from_slice(&values);
        Ok(out)
    }

    // ── Decoding ─────────────────────────────────────────────────────────────

    /// Decode value-only bytes back to a JSON payload (UTF-8).
    pub fn decode_payload(&self, bytes: &[u8]) -> Result<Vec<u8>, SchemaError> {
        let n = self.fields.len();
        if n == 0 {
            return Ok(b"{}".to_vec());
        }

        let bitmap_bytes = (n + 7) / 8;
        if bytes.len() < bitmap_bytes {
            return Err(SchemaError::EncodeError("buffer too short for null bitmap".into()));
        }

        let null_bitmap = &bytes[..bitmap_bytes];
        let mut pos = bitmap_bytes;

        let mut obj = Map::with_capacity(n);

        for (i, field) in self.fields.iter().enumerate() {
            let is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;
            if is_null {
                obj.insert(field.name.clone(), Value::Null);
                continue;
            }

            let (val, consumed) = decode_value(&bytes[pos..], &field.field_type)?;
            pos += consumed;
            obj.insert(field.name.clone(), val);
        }

        serde_json::to_vec(&Value::Object(obj))
            .map_err(|e| SchemaError::EncodeError(e.to_string()))
    }
}

// ── Field-level helpers ───────────────────────────────────────────────────────

fn infer_field_type(val: &Value) -> FieldType {
    match val {
        Value::String(_) => FieldType::String,
        Value::Number(_) => FieldType::Number,
        Value::Bool(_) => FieldType::Boolean,
        Value::Array(_) | Value::Object(_) | Value::Null => FieldType::Json,
    }
}

fn type_matches(val: &Value, ft: &FieldType) -> bool {
    match ft {
        FieldType::String => val.is_string(),
        FieldType::Number => val.is_number(),
        FieldType::Boolean => val.is_boolean(),
        FieldType::Json => true, // arrays, objects, anything
    }
}

fn json_type_name(val: &Value) -> &'static str {
    match val {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Null => "null",
    }
}

fn encode_value(val: &Value, ft: &FieldType, buf: &mut Vec<u8>) -> Result<(), SchemaError> {
    match ft {
        FieldType::String => {
            let s = val
                .as_str()
                .ok_or_else(|| SchemaError::EncodeError("expected string".into()))?;
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        FieldType::Number => {
            let n = val
                .as_f64()
                .ok_or_else(|| SchemaError::EncodeError("expected number".into()))?;
            buf.extend_from_slice(&n.to_le_bytes());
        }
        FieldType::Boolean => {
            let b = val
                .as_bool()
                .ok_or_else(|| SchemaError::EncodeError("expected boolean".into()))?;
            buf.push(if b { 1 } else { 0 });
        }
        FieldType::Json => {
            let raw = serde_json::to_vec(val)
                .map_err(|e| SchemaError::EncodeError(e.to_string()))?;
            buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            buf.extend_from_slice(&raw);
        }
    }
    Ok(())
}

fn decode_value(buf: &[u8], ft: &FieldType) -> Result<(Value, usize), SchemaError> {
    match ft {
        FieldType::String => {
            if buf.len() < 4 {
                return Err(SchemaError::EncodeError("truncated string length".into()));
            }
            let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < 4 + len {
                return Err(SchemaError::EncodeError("truncated string data".into()));
            }
            let s = std::str::from_utf8(&buf[4..4 + len])
                .map_err(|e| SchemaError::EncodeError(e.to_string()))?;
            Ok((Value::String(s.to_string()), 4 + len))
        }
        FieldType::Number => {
            if buf.len() < 8 {
                return Err(SchemaError::EncodeError("truncated number".into()));
            }
            let f = f64::from_le_bytes(buf[0..8].try_into().unwrap());
            Ok((serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null), 8))
        }
        FieldType::Boolean => {
            if buf.is_empty() {
                return Err(SchemaError::EncodeError("truncated boolean".into()));
            }
            Ok((Value::Bool(buf[0] != 0), 1))
        }
        FieldType::Json => {
            if buf.len() < 4 {
                return Err(SchemaError::EncodeError("truncated json length".into()));
            }
            let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < 4 + len {
                return Err(SchemaError::EncodeError("truncated json data".into()));
            }
            let val: Value = serde_json::from_slice(&buf[4..4 + len])
                .map_err(|e| SchemaError::EncodeError(e.to_string()))?;
            Ok((val, 4 + len))
        }
    }
}
