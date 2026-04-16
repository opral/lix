use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::{builtin_schema_definition, builtin_schema_storage_defaults};

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
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
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
    let defaults = builtin_schema_storage_defaults(schema_key)?;

    Some(BuiltinSchemaStorageMetadata {
        schema_key: parsed_schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: defaults.file_id.map(str::to_string),
        plugin_key: defaults.plugin_key.map(str::to_string),
        storage_lane: builtin_storage_lane(schema_key),
    })
}

fn builtin_storage_lane(schema_key: &str) -> BuiltinSchemaStorageLane {
    match schema_key {
        "lix_version_ref"
        | "lix_version_descriptor"
        | "lix_active_version"
        | "lix_active_account" => BuiltinSchemaStorageLane::Global,
        _ => BuiltinSchemaStorageLane::Local,
    }
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

#[cfg(test)]
pub(crate) fn key_value_file_id() -> Option<&'static str> {
    key_value_storage_metadata().file_id.as_deref()
}

#[cfg(test)]
pub(crate) fn key_value_plugin_key() -> Option<&'static str> {
    key_value_storage_metadata().plugin_key.as_deref()
}
