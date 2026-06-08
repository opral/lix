use plugin_md_v2::MANIFEST_JSON;
use plugin_md_v2::schemas::{
    BLOCK_SCHEMA_KEY, BLOCK_SCHEMA_PATH, DOCUMENT_SCHEMA_KEY, DOCUMENT_SCHEMA_PATH,
    block_schema_definition, block_schema_json, document_schema_definition, document_schema_json,
    schema_definitions, schema_jsons,
};
use std::collections::BTreeSet;

#[test]
fn schema_definitions_have_expected_keys() {
    let schemas = schema_definitions();

    assert_eq!(schemas.len(), 2);

    let expected_keys = BTreeSet::from([DOCUMENT_SCHEMA_KEY, BLOCK_SCHEMA_KEY]);

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
    assert!(raw.contains("\"x-lix-key\": \"markdown_document\""));
    assert!(raw.contains("\"x-lix-key\": \"markdown_block\""));
    assert_eq!(
        document_schema_definition()
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str),
        Some(DOCUMENT_SCHEMA_KEY)
    );
    assert_eq!(
        block_schema_definition()
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str),
        Some(BLOCK_SCHEMA_KEY)
    );
    assert!(!document_schema_json().contains("\"order_key\""));
    assert!(block_schema_json().contains("\"order_key\""));
    assert!(block_schema_json().contains("\"block\""));
    assert!(block_schema_json().contains("inter-block blank lines"));
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
        "plugin_md_v2"
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
        "*.{md,markdown,mdx}"
    );
    assert_eq!(
        manifest
            .get("match")
            .and_then(|value| value.get("content_type"))
            .and_then(serde_json::Value::as_str),
        Some("text")
    );
    let schemas = manifest
        .get("schemas")
        .and_then(serde_json::Value::as_array)
        .expect("manifest.schemas must be an array")
        .iter()
        .map(|value| value.as_str().expect("schema paths must be strings"))
        .collect::<Vec<_>>();
    assert_eq!(schemas, [DOCUMENT_SCHEMA_PATH, BLOCK_SCHEMA_PATH]);
}
