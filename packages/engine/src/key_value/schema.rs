use serde_json::{json, Value as JsonValue};
use std::sync::OnceLock;

use crate::LixError;

pub(crate) const KEY_VALUE_GLOBAL_VERSION: &str = "global";

static KEY_VALUE_SCHEMA_DEFINITION: OnceLock<JsonValue> = OnceLock::new();
static KEY_VALUE_SCHEMA_METADATA: OnceLock<KeyValueSchemaMetadata> = OnceLock::new();

struct KeyValueSchemaMetadata {
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
}

pub fn key_value_schema_definition() -> &'static JsonValue {
    KEY_VALUE_SCHEMA_DEFINITION.get_or_init(|| {
        let raw = include_str!("schema.json");
        serde_json::from_str(raw).expect("key_value/schema.json must be valid JSON")
    })
}

#[allow(dead_code)]
pub fn key_value_schema_definition_json() -> &'static str {
    include_str!("schema.json")
}

pub fn key_value_schema_entity_id() -> String {
    format!("{}~{}", key_value_schema_key(), key_value_schema_version())
}

pub fn key_value_schema_seed_insert_sql() -> Result<String, LixError> {
    let snapshot_content = json!({
        "value": key_value_schema_definition()
    })
    .to_string();

    Ok(format!(
        "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{snapshot_content}')",
        snapshot_content = escape_sql_string(&snapshot_content),
    ))
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

pub(crate) fn key_value_schema_key() -> &'static str {
    &key_value_schema_metadata().schema_key
}

pub(crate) fn key_value_schema_version() -> &'static str {
    &key_value_schema_metadata().schema_version
}

pub(crate) fn key_value_file_id() -> &'static str {
    &key_value_schema_metadata().file_id
}

pub(crate) fn key_value_plugin_key() -> &'static str {
    &key_value_schema_metadata().plugin_key
}

fn key_value_schema_metadata() -> &'static KeyValueSchemaMetadata {
    KEY_VALUE_SCHEMA_METADATA.get_or_init(|| {
        let schema = key_value_schema_definition();
        let schema_key = schema
            .get("x-lix-key")
            .and_then(JsonValue::as_str)
            .expect("key_value/schema.json must define string x-lix-key")
            .to_string();
        let schema_version = schema
            .get("x-lix-version")
            .and_then(JsonValue::as_str)
            .expect("key_value/schema.json must define string x-lix-version")
            .to_string();
        let overrides = schema
            .get("x-lix-override-lixcols")
            .and_then(JsonValue::as_object)
            .expect("key_value/schema.json must define object x-lix-override-lixcols");
        let file_id_raw = overrides
            .get("lixcol_file_id")
            .and_then(JsonValue::as_str)
            .expect("key_value/schema.json must define string lixcol_file_id");
        let plugin_key_raw = overrides
            .get("lixcol_plugin_key")
            .and_then(JsonValue::as_str)
            .expect("key_value/schema.json must define string lixcol_plugin_key");

        KeyValueSchemaMetadata {
            schema_key,
            schema_version,
            file_id: decode_lixcol_literal(file_id_raw),
            plugin_key: decode_lixcol_literal(plugin_key_raw),
        }
    })
}

fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('\"').to_string())
}
