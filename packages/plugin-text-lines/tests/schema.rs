use plugin_text_lines::{
    document_schema_definition, document_schema_json, line_schema_definition, line_schema_json,
    manifest_json, DOCUMENT_SCHEMA_KEY, LINE_SCHEMA_KEY, SCHEMA_VERSION,
};

#[test]
fn line_schema_matches_constants() {
    let schema = line_schema_definition();
    assert_eq!(
        schema
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str)
            .expect("x-lix-key must be string"),
        LINE_SCHEMA_KEY
    );
    assert_eq!(
        schema
            .get("x-lix-version")
            .and_then(serde_json::Value::as_str)
            .expect("x-lix-version must be string"),
        SCHEMA_VERSION
    );
}

#[test]
fn document_schema_matches_constants() {
    let schema = document_schema_definition();
    assert_eq!(
        schema
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str)
            .expect("x-lix-key must be string"),
        DOCUMENT_SCHEMA_KEY
    );
    assert_eq!(
        schema
            .get("x-lix-version")
            .and_then(serde_json::Value::as_str)
            .expect("x-lix-version must be string"),
        SCHEMA_VERSION
    );
}

#[test]
fn schema_json_accessors_return_expected_text() {
    let line = line_schema_json();
    let document = document_schema_json();
    assert!(line.contains("\"x-lix-key\": \"text_line\""));
    assert!(line.contains("\"x-lix-version\": \"1\""));
    assert!(document.contains("\"x-lix-key\": \"text_document\""));
    assert!(document.contains("\"x-lix-version\": \"1\""));
}

#[test]
fn manifest_json_has_expected_plugin_identity() {
    let manifest: serde_json::Value =
        serde_json::from_str(manifest_json()).expect("manifest must be valid JSON");
    assert_eq!(
        manifest
            .get("key")
            .and_then(serde_json::Value::as_str)
            .expect("manifest.key must be string"),
        "plugin_text_lines"
    );
    assert_eq!(
        manifest
            .get("runtime")
            .and_then(serde_json::Value::as_str)
            .expect("manifest.runtime must be string"),
        "wasm-component-v1"
    );
}
