use serde_json::{json, Value as JsonValue};
use std::sync::OnceLock;

use crate::LixError;

const KEY_VALUE_SCHEMA_VERSION: &str = "1";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

static KEY_VALUE_SCHEMA_DEFINITION: OnceLock<JsonValue> = OnceLock::new();

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
    format!("{}~{}", KEY_VALUE_SCHEMA_KEY, KEY_VALUE_SCHEMA_VERSION)
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
