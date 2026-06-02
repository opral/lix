use plugin_json_v2::{MANIFEST_JSON, SCHEMA_KEY, SCHEMA_PATH, schema_definition, schema_json};

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

#[test]
fn manifest_json_has_expected_plugin_identity() {
    let manifest: serde_json::Value =
        serde_json::from_str(MANIFEST_JSON).expect("manifest must be valid JSON");
    assert_eq!(
        manifest
            .get("key")
            .and_then(serde_json::Value::as_str)
            .expect("manifest.key must be string"),
        "plugin_json_v2"
    );
    assert_eq!(
        manifest
            .get("runtime")
            .and_then(serde_json::Value::as_str)
            .expect("manifest.runtime must be string"),
        "wasm-component-v1"
    );
    assert_eq!(
        manifest
            .get("match")
            .and_then(|value| value.get("path_glob"))
            .and_then(serde_json::Value::as_str)
            .expect("manifest.match.path_glob must be string"),
        "*.json"
    );
    let schemas = manifest
        .get("schemas")
        .and_then(serde_json::Value::as_array)
        .expect("manifest.schemas must be an array")
        .iter()
        .map(|value| value.as_str().expect("schema paths must be strings"))
        .collect::<Vec<_>>();
    assert_eq!(schemas, [SCHEMA_PATH]);
}
