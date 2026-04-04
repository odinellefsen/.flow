use dotflow::schema::{FieldType, PayloadSchema, SchemaField};

// ── Inference ─────────────────────────────────────────────────────────────────

#[test]
fn infer_flat_object() {
    let payload = br#"{"id":"abc","count":42,"active":true}"#;
    let schema = PayloadSchema::infer(payload).unwrap();

    assert_eq!(schema.fields.len(), 3);

    let by_name = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| f.name == name)
            .unwrap()
            .field_type
            .clone()
    };
    assert_eq!(by_name("id"), FieldType::String);
    assert_eq!(by_name("count"), FieldType::Number);
    assert_eq!(by_name("active"), FieldType::Boolean);
}

#[test]
fn infer_nested_and_array_become_json() {
    let payload = br#"{"id":"x","tags":["a","b"],"meta":{"k":"v"}}"#;
    let schema = PayloadSchema::infer(payload).unwrap();

    let tags_field = schema.fields.iter().find(|f| f.name == "tags").unwrap();
    assert_eq!(tags_field.field_type, FieldType::Json);

    let meta_field = schema.fields.iter().find(|f| f.name == "meta").unwrap();
    assert_eq!(meta_field.field_type, FieldType::Json);
}

#[test]
fn infer_fails_on_non_object() {
    assert!(PayloadSchema::infer(b"[1,2,3]").is_err());
    assert!(PayloadSchema::infer(b"\"string\"").is_err());
    assert!(PayloadSchema::infer(b"42").is_err());
}

// ── Encode / decode round-trips ───────────────────────────────────────────────

#[test]
fn roundtrip_all_scalar_types() {
    let payload = br#"{"name":"Alice","age":30,"verified":true}"#;
    let schema = PayloadSchema::infer(payload).unwrap();

    let encoded = schema.encode_payload(payload).unwrap();
    // Encoded must be smaller than raw JSON
    assert!(encoded.len() < payload.len(), "schema encoding should be smaller");

    let decoded = schema.decode_payload(&encoded).unwrap();
    let original: serde_json::Value = serde_json::from_slice(payload).unwrap();
    let recovered: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(original, recovered);
}

#[test]
fn roundtrip_with_array_field() {
    let payload = br#"{"id":"x","tags":["rust","binary","fast"]}"#;
    let schema = PayloadSchema::infer(payload).unwrap();

    let encoded = schema.encode_payload(payload).unwrap();
    let decoded = schema.decode_payload(&encoded).unwrap();

    let original: serde_json::Value = serde_json::from_slice(payload).unwrap();
    let recovered: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(original, recovered);
}

#[test]
fn roundtrip_with_null_field() {
    let schema = PayloadSchema {
        fields: vec![
            SchemaField { name: "id".into(), field_type: FieldType::String },
            SchemaField { name: "note".into(), field_type: FieldType::String },
        ],
    };

    let payload = br#"{"id":"abc","note":null}"#;
    let encoded = schema.encode_payload(payload).unwrap();
    let decoded = schema.decode_payload(&encoded).unwrap();

    let original: serde_json::Value = serde_json::from_slice(payload).unwrap();
    let recovered: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(original, recovered);
}

#[test]
fn roundtrip_flowcore_event_payload() {
    let payload = br#"{"id":"a4a41742-16f4-4a7e-9cb5-e0048d207a2c","userId":"user_30eqFgTWfuBysprgdMgwvjEezH2","name":"Yes","categoryHierarchy":["China","Faroe Islands","Human ecrementt","poo"]}"#;
    let schema = PayloadSchema::infer(payload).unwrap();

    let encoded = schema.encode_payload(payload).unwrap();
    assert!(
        encoded.len() < payload.len(),
        "encoded {} bytes, raw {} bytes",
        encoded.len(),
        payload.len()
    );

    let decoded = schema.decode_payload(&encoded).unwrap();
    let original: serde_json::Value = serde_json::from_slice(payload).unwrap();
    let recovered: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(original, recovered);
}

// ── Validation ────────────────────────────────────────────────────────────────

#[test]
fn validate_accepts_matching_payload() {
    let first = br#"{"id":"abc","count":1}"#;
    let schema = PayloadSchema::infer(first).unwrap();

    let second = br#"{"id":"def","count":2}"#;
    assert!(schema.validate(second).is_ok());
}

#[test]
fn validate_accepts_null_for_any_field() {
    let first = br#"{"id":"abc","count":1}"#;
    let schema = PayloadSchema::infer(first).unwrap();

    let with_null = br#"{"id":null,"count":2}"#;
    assert!(schema.validate(with_null).is_ok());
}

#[test]
fn validate_rejects_missing_field() {
    let first = br#"{"id":"abc","count":1}"#;
    let schema = PayloadSchema::infer(first).unwrap();

    let missing = br#"{"id":"xyz"}"#;
    assert!(schema.validate(missing).is_err());
}

#[test]
fn validate_rejects_extra_field() {
    let first = br#"{"id":"abc","count":1}"#;
    let schema = PayloadSchema::infer(first).unwrap();

    let extra = br#"{"id":"xyz","count":2,"newField":"surprise"}"#;
    assert!(schema.validate(extra).is_err());
}

#[test]
fn validate_rejects_wrong_type() {
    let first = br#"{"id":"abc","count":1}"#;
    let schema = PayloadSchema::infer(first).unwrap();

    let wrong_type = br#"{"id":"abc","count":"not-a-number"}"#;
    assert!(schema.validate(wrong_type).is_err());
}

// ── Encoding is smaller than raw JSON ─────────────────────────────────────────

#[test]
fn encoding_is_compact() {
    // 10 events with the same schema -- measure average size
    let payloads: Vec<Vec<u8>> = (0..10)
        .map(|i| {
            format!(
                r#"{{"id":"{:036}","userId":"user_{:020}","name":"Item {}","price":{:.2}}}"#,
                i, i, i, i as f64 * 1.5
            )
            .into_bytes()
        })
        .collect();

    let schema = PayloadSchema::infer(&payloads[0]).unwrap();

    let raw_total: usize = payloads.iter().map(|p| p.len()).sum();
    let encoded_total: usize = payloads
        .iter()
        .map(|p| schema.encode_payload(p).unwrap().len())
        .sum();

    assert!(
        encoded_total < raw_total,
        "schema encoding ({encoded_total}) should be smaller than raw ({raw_total})"
    );
}
