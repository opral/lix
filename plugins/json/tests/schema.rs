use plugin_json_v2::{schema_definition, schema_json, SCHEMA_KEY};

#[test]
fn schema_json_is_valid_and_matches_constants() {
    let schema = schema_definition();

    let key = schema
        .get("x-lix-key")
        .and_then(serde_json::Value::as_str)
        .expect("schema must define string x-lix-key");
    assert_eq!(key, SCHEMA_KEY);

    let primary_key = schema
        .get("x-lix-primary-key")
        .and_then(serde_json::Value::as_array)
        .expect("schema must define x-lix-primary-key array");
    assert_eq!(primary_key.len(), 1);
    assert_eq!(primary_key[0].as_str(), Some("/path"));
}

#[test]
fn schema_json_accessor_returns_expected_text() {
    let raw = schema_json();
    assert!(raw.contains("\"x-lix-key\": \"json_pointer\""));
}
