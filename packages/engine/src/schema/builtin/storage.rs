use serde_json::Value as JsonValue;

use crate::version::GLOBAL_VERSION_ID;

use super::{builtin_schema_definition, decode_lixcol_literal};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BuiltinSchemaStorageLane {
    Global,
    Versioned,
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
        _ => BuiltinSchemaStorageLane::Versioned,
    };

    Some(BuiltinSchemaStorageMetadata {
        schema_key: parsed_schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: decode_lixcol_literal(file_id_raw),
        plugin_key: decode_lixcol_literal(plugin_key_raw),
        storage_lane,
    })
}
