use std::sync::OnceLock;

use crate::schema::builtin::storage::{
    builtin_schema_storage_metadata, BuiltinSchemaStorageMetadata,
};
use crate::schema::builtin::types::LixActiveVersion;
use crate::version_state::GLOBAL_VERSION_ID;
use crate::LixError;

static ACTIVE_VERSION_STORAGE_METADATA: OnceLock<BuiltinSchemaStorageMetadata> = OnceLock::new();

pub(crate) fn active_version_schema_key() -> &'static str {
    &active_version_storage_metadata().schema_key
}

pub(crate) fn active_version_file_id() -> &'static str {
    &active_version_storage_metadata().file_id
}

pub(crate) fn active_version_storage_version_id() -> &'static str {
    GLOBAL_VERSION_ID
}

pub(crate) fn parse_active_version_snapshot(snapshot_content: &str) -> Result<String, LixError> {
    let parsed: LixActiveVersion =
        serde_json::from_str(snapshot_content).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("active version snapshot_content invalid JSON: {error}"),
        })?;

    if parsed.version_id.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "active version must not be empty".to_string(),
        });
    }

    Ok(parsed.version_id)
}

fn active_version_storage_metadata() -> &'static BuiltinSchemaStorageMetadata {
    ACTIVE_VERSION_STORAGE_METADATA
        .get_or_init(|| builtin_version_storage_metadata("lix_active_version"))
}

fn builtin_version_storage_metadata(schema_key: &str) -> BuiltinSchemaStorageMetadata {
    builtin_schema_storage_metadata(schema_key)
        .unwrap_or_else(|| panic!("builtin version storage metadata '{schema_key}' should exist"))
}
