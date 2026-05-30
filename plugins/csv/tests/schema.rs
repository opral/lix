use plugin_csv_v2::manifest_json;
use plugin_csv_v2::schemas::{
    DOCUMENT_SCHEMA_KEY, ROW_SCHEMA_KEY, schema_definitions, schema_jsons,
};
use std::collections::BTreeSet;

#[test]
fn schema_definitions_have_expected_keys() {
    let schemas = schema_definitions();

    assert_eq!(schemas.len(), 2);

    let expected_keys = BTreeSet::from([DOCUMENT_SCHEMA_KEY, ROW_SCHEMA_KEY]);

    let mut actual_keys = BTreeSet::new();
    for schema in schemas {
        let key = schema
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str)
            .expect("schema must define string x-lix-key");
        let primary_key = schema
            .get("x-lix-primary-key")
            .and_then(serde_json::Value::as_array)
            .expect("schema must define x-lix-primary-key array");

        actual_keys.insert(key);
        assert_eq!(primary_key.len(), 1);
        assert_eq!(primary_key[0].as_str(), Some("/id"));
    }

    assert_eq!(actual_keys, expected_keys);
}

#[test]
fn schema_json_accessors_return_expected_text() {
    let raw = schema_jsons().join("\n");
    assert!(raw.contains("\"x-lix-key\": \"csv_v2_document\""));
    assert!(raw.contains("\"x-lix-key\": \"csv_v2_row\""));
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
        "plugin_csv_v2"
    );
    assert_eq!(
        manifest
            .get("runtime")
            .and_then(serde_json::Value::as_str)
            .expect("manifest.runtime must be string"),
        "wasm-component-v1"
    );
}
