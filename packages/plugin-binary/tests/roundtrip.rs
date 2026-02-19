use plugin_binary::{
    apply_changes, detect_changes, schema_definition, schema_json, PluginFile, SCHEMA_KEY,
    SCHEMA_VERSION,
};

fn file_from_bytes(id: &str, path: &str, bytes: &[u8]) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: bytes.to_vec(),
    }
}

#[test]
fn detect_changes_returns_empty_for_identical_bytes() {
    let before = file_from_bytes("f1", "/bin/asset.bin", b"same-bytes");
    let after = file_from_bytes("f1", "/bin/asset.bin", b"same-bytes");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    assert!(changes.is_empty());
}

#[test]
fn detect_changes_emits_single_blob_row_on_change() {
    let before = file_from_bytes("f1", "/bin/asset.bin", b"before");
    let after = file_from_bytes("f1", "/bin/asset.bin", b"after");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");
    assert_eq!(changes.len(), 1);

    let change = &changes[0];
    assert_eq!(change.entity_id, "f1");
    assert_eq!(change.schema_key, SCHEMA_KEY);
    assert_eq!(change.schema_version, SCHEMA_VERSION);
    assert!(change.snapshot_content.is_some());
}

#[test]
fn apply_changes_roundtrips_latest_blob_bytes() {
    let after = file_from_bytes("f1", "/bin/video.mp4", b"\x00\x01\x02\x03\xff");
    let changes = detect_changes(None, after.clone()).expect("detect_changes should succeed");

    let reconstructed = apply_changes(
        file_from_bytes("f1", "/bin/video.mp4", b"irrelevant"),
        changes,
    )
    .expect("apply_changes should succeed");

    assert_eq!(reconstructed, after.data);
}

#[test]
fn schema_json_is_valid_and_matches_constants() {
    let schema = schema_definition();

    let key = schema
        .get("x-lix-key")
        .and_then(serde_json::Value::as_str)
        .expect("schema must define string x-lix-key");
    assert_eq!(key, SCHEMA_KEY);

    let version = schema
        .get("x-lix-version")
        .and_then(serde_json::Value::as_str)
        .expect("schema must define string x-lix-version");
    assert_eq!(version, SCHEMA_VERSION);

    let primary_key = schema
        .get("x-lix-primary-key")
        .and_then(serde_json::Value::as_array)
        .expect("schema must define x-lix-primary-key array");
    assert_eq!(primary_key.len(), 1);
    assert_eq!(primary_key[0].as_str(), Some("/id"));

    let raw = schema_json();
    assert!(raw.contains("\"x-lix-key\": \"lix_binary_blob\""));
}
