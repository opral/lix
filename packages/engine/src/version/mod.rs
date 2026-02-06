use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::builtin_schema::{builtin_schema_definition, builtin_schema_json};
use crate::LixError;

pub(crate) const GLOBAL_VERSION_ID: &str = "global";
pub(crate) const DEFAULT_ACTIVE_VERSION_NAME: &str = "main";

static ACTIVE_VERSION_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();
static VERSION_DESCRIPTOR_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();
static VERSION_TIP_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();

struct SchemaMetadata {
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
    storage_version_id: String,
}

#[allow(dead_code)]
pub(crate) fn active_version_schema_definition() -> &'static JsonValue {
    builtin_schema_definition("lix_active_version")
        .expect("builtin schema 'lix_active_version' must exist")
}

#[allow(dead_code)]
pub(crate) fn active_version_schema_definition_json() -> &'static str {
    builtin_schema_json("lix_active_version")
        .expect("builtin schema 'lix_active_version' must exist")
}

pub(crate) fn active_version_schema_key() -> &'static str {
    &active_version_schema_metadata().schema_key
}

pub(crate) fn active_version_schema_version() -> &'static str {
    &active_version_schema_metadata().schema_version
}

pub(crate) fn active_version_file_id() -> &'static str {
    &active_version_schema_metadata().file_id
}

pub(crate) fn active_version_plugin_key() -> &'static str {
    &active_version_schema_metadata().plugin_key
}

pub(crate) fn active_version_storage_version_id() -> &'static str {
    &active_version_schema_metadata().storage_version_id
}

pub(crate) fn active_version_snapshot_content(entity_id: &str, version_id: &str) -> String {
    serde_json::json!({
        "id": entity_id,
        "version_id": version_id,
    })
    .to_string()
}

pub(crate) fn parse_active_version_snapshot(snapshot_content: &str) -> Result<String, LixError> {
    let parsed: JsonValue = serde_json::from_str(snapshot_content).map_err(|error| LixError {
        message: format!("active version snapshot_content invalid JSON: {error}"),
    })?;

    let value = parsed
        .get("version_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "active version snapshot_content must contain string version_id".to_string(),
        })?;

    if value.is_empty() {
        return Err(LixError {
            message: "active version must not be empty".to_string(),
        });
    }

    Ok(value.to_string())
}

pub(crate) fn version_descriptor_schema_key() -> &'static str {
    &version_descriptor_schema_metadata().schema_key
}

pub(crate) fn version_descriptor_schema_version() -> &'static str {
    &version_descriptor_schema_metadata().schema_version
}

pub(crate) fn version_descriptor_file_id() -> &'static str {
    &version_descriptor_schema_metadata().file_id
}

pub(crate) fn version_descriptor_plugin_key() -> &'static str {
    &version_descriptor_schema_metadata().plugin_key
}

pub(crate) fn version_descriptor_storage_version_id() -> &'static str {
    &version_descriptor_schema_metadata().storage_version_id
}

pub(crate) fn version_descriptor_snapshot_content(
    id: &str,
    name: &str,
    inherits_from_version_id: Option<&str>,
    hidden: bool,
) -> String {
    serde_json::json!({
        "id": id,
        "name": name,
        "inherits_from_version_id": inherits_from_version_id,
        "hidden": hidden,
    })
    .to_string()
}

pub(crate) fn version_tip_schema_key() -> &'static str {
    &version_tip_schema_metadata().schema_key
}

pub(crate) fn version_tip_schema_version() -> &'static str {
    &version_tip_schema_metadata().schema_version
}

pub(crate) fn version_tip_file_id() -> &'static str {
    &version_tip_schema_metadata().file_id
}

pub(crate) fn version_tip_plugin_key() -> &'static str {
    &version_tip_schema_metadata().plugin_key
}

pub(crate) fn version_tip_storage_version_id() -> &'static str {
    &version_tip_schema_metadata().storage_version_id
}

pub(crate) fn version_tip_snapshot_content(
    id: &str,
    commit_id: &str,
    working_commit_id: &str,
) -> String {
    serde_json::json!({
        "id": id,
        "commit_id": commit_id,
        "working_commit_id": working_commit_id,
    })
    .to_string()
}

fn active_version_schema_metadata() -> &'static SchemaMetadata {
    ACTIVE_VERSION_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_active_version"))
}

fn version_descriptor_schema_metadata() -> &'static SchemaMetadata {
    VERSION_DESCRIPTOR_SCHEMA_METADATA
        .get_or_init(|| parse_schema_metadata("lix_version_descriptor"))
}

fn version_tip_schema_metadata() -> &'static SchemaMetadata {
    VERSION_TIP_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_version_tip"))
}

fn parse_schema_metadata(schema_key: &str) -> SchemaMetadata {
    let schema = builtin_schema_definition(schema_key).unwrap_or_else(|| {
        panic!("builtin schema '{schema_key}' must exist");
    });
    let parsed_schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{schema_key}' must define string x-lix-key"))
        .to_string();
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{schema_key}' must define string x-lix-version"))
        .to_string();
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define object x-lix-override-lixcols")
        });
    let file_id_raw = overrides
        .get("lixcol_file_id")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define string lixcol_file_id")
        });
    let plugin_key_raw = overrides
        .get("lixcol_plugin_key")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define string lixcol_plugin_key")
        });
    let storage_version_id = overrides
        .get("lixcol_version_id")
        .and_then(JsonValue::as_str)
        .map(decode_lixcol_literal)
        .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string());

    SchemaMetadata {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: decode_lixcol_literal(file_id_raw),
        plugin_key: decode_lixcol_literal(plugin_key_raw),
        storage_version_id,
    }
}

fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('"').to_string())
}
