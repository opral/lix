use plugin_md_v2::MANIFEST_JSON;
use plugin_md_v2::schemas::{
    NODE_SCHEMA_KEY, NODE_SCHEMA_PATH, node_schema_definition, node_schema_json,
    schema_definitions, schema_jsons,
};

#[test]
fn exposes_one_self_referencing_markdown_node_schema() {
    let schemas = schema_definitions();
    assert_eq!(schemas.len(), 1);
    assert_eq!(
        schemas[0]
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str),
        Some(NODE_SCHEMA_KEY)
    );
    assert_eq!(
        schemas[0].get("x-lix-primary-key"),
        Some(&serde_json::json!(["/id"]))
    );
    let raw = node_schema_json();
    assert!(raw.contains("\"document\""));
    assert!(raw.contains("\"table_column\""));
    assert!(raw.contains("\"table_cell\""));
    assert!(raw.contains("\"parent_id\""));
    assert!(raw.contains("\"payload\""));
    assert!(raw.contains("\"format\""));
    assert_eq!(
        node_schema_definition()
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str),
        Some(NODE_SCHEMA_KEY)
    );
    assert_eq!(schema_jsons(), &[node_schema_json()]);
}

#[test]
fn manifest_publishes_only_the_v2_schema() {
    let manifest: serde_json::Value = serde_json::from_str(MANIFEST_JSON).unwrap();
    assert_eq!(manifest["key"], "plugin_md_v2");
    assert_eq!(manifest["match"]["path_glob"], "*.{md,markdown}");
    assert_eq!(manifest["schemas"], serde_json::json!([NODE_SCHEMA_PATH]));
}
