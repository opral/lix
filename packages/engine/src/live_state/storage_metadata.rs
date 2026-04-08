use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::builtin::{builtin_schema_definition, decode_lixcol_literal};
use crate::version_state::GLOBAL_VERSION_ID;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BuiltinSchemaStorageLane {
    Global,
    Local,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuiltinSchemaStorageMetadata {
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) storage_lane: BuiltinSchemaStorageLane,
}

static KEY_VALUE_STORAGE_METADATA: OnceLock<BuiltinSchemaStorageMetadata> = OnceLock::new();

#[allow(dead_code)]
pub(crate) fn builtin_schema_storage_metadata(
    schema_key: &str,
) -> Option<BuiltinSchemaStorageMetadata> {
    let schema = builtin_schema_definition(schema_key)?;
    let parsed_schema_key = schema.get("x-lix-key").and_then(JsonValue::as_str)?;
    let schema_version = schema.get("x-lix-version").and_then(JsonValue::as_str)?;
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)?;
    let file_id_raw = overrides
        .get("lixcol_file_id")
        .and_then(JsonValue::as_str)?;
    let plugin_key_raw = overrides
        .get("lixcol_plugin_key")
        .and_then(JsonValue::as_str)?;

    let storage_lane = match overrides.get("lixcol_global").and_then(JsonValue::as_str) {
        Some("true") if GLOBAL_VERSION_ID == "global" => BuiltinSchemaStorageLane::Global,
        _ => BuiltinSchemaStorageLane::Local,
    };

    Some(BuiltinSchemaStorageMetadata {
        schema_key: parsed_schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: decode_lixcol_literal(file_id_raw),
        plugin_key: decode_lixcol_literal(plugin_key_raw),
        storage_lane,
    })
}

pub(crate) fn key_value_storage_metadata() -> &'static BuiltinSchemaStorageMetadata {
    KEY_VALUE_STORAGE_METADATA.get_or_init(|| {
        builtin_schema_storage_metadata("lix_key_value")
            .expect("lix_key_value builtin storage metadata should exist")
    })
}

pub(crate) fn key_value_schema_key() -> &'static str {
    &key_value_storage_metadata().schema_key
}

pub(crate) fn key_value_schema_version() -> &'static str {
    &key_value_storage_metadata().schema_version
}

pub(crate) fn key_value_file_id() -> &'static str {
    &key_value_storage_metadata().file_id
}

pub(crate) fn key_value_plugin_key() -> &'static str {
    &key_value_storage_metadata().plugin_key
}
