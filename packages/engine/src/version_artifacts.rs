use std::sync::OnceLock;

use crate::schema::builtin::storage::{
    builtin_schema_storage_metadata, BuiltinSchemaStorageMetadata,
};
use crate::schema::builtin::types::{LixActiveVersion, LixVersionDescriptor, LixVersionRef};
use crate::LixError;

static ACTIVE_VERSION_STORAGE_METADATA: OnceLock<BuiltinSchemaStorageMetadata> = OnceLock::new();
static VERSION_DESCRIPTOR_STORAGE_METADATA: OnceLock<BuiltinSchemaStorageMetadata> =
    OnceLock::new();
static VERSION_REF_STORAGE_METADATA: OnceLock<BuiltinSchemaStorageMetadata> = OnceLock::new();

pub(crate) use crate::schema::builtin::GLOBAL_VERSION_ID;
pub(crate) use crate::version::{
    load_committed_version_head_commit_id, load_committed_version_ref_with_backend,
};

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

pub(crate) fn version_descriptor_schema_key() -> &'static str {
    &version_descriptor_storage_metadata().schema_key
}

pub(crate) fn version_descriptor_schema_version() -> &'static str {
    &version_descriptor_storage_metadata().schema_version
}

pub(crate) fn version_descriptor_file_id() -> &'static str {
    &version_descriptor_storage_metadata().file_id
}

pub(crate) fn version_descriptor_plugin_key() -> &'static str {
    &version_descriptor_storage_metadata().plugin_key
}

pub(crate) fn version_descriptor_snapshot_content(id: &str, name: &str, hidden: bool) -> String {
    serde_json::to_string(&LixVersionDescriptor {
        id: id.to_string(),
        name: Some(name.to_string()),
        hidden,
    })
    .expect("lix_version_descriptor snapshot serialization must succeed")
}

pub(crate) fn version_ref_schema_key() -> &'static str {
    &version_ref_storage_metadata().schema_key
}

pub(crate) fn version_ref_schema_version() -> &'static str {
    &version_ref_storage_metadata().schema_version
}

pub(crate) fn version_ref_file_id() -> &'static str {
    &version_ref_storage_metadata().file_id
}

pub(crate) fn version_ref_plugin_key() -> &'static str {
    &version_ref_storage_metadata().plugin_key
}

pub(crate) fn version_ref_snapshot_content(id: &str, commit_id: &str) -> String {
    serde_json::to_string(&LixVersionRef {
        id: id.to_string(),
        commit_id: commit_id.to_string(),
    })
    .expect("lix_version_ref snapshot serialization must succeed")
}

fn active_version_storage_metadata() -> &'static BuiltinSchemaStorageMetadata {
    ACTIVE_VERSION_STORAGE_METADATA
        .get_or_init(|| builtin_version_storage_metadata("lix_active_version"))
}

fn version_descriptor_storage_metadata() -> &'static BuiltinSchemaStorageMetadata {
    VERSION_DESCRIPTOR_STORAGE_METADATA
        .get_or_init(|| builtin_version_storage_metadata("lix_version_descriptor"))
}

fn version_ref_storage_metadata() -> &'static BuiltinSchemaStorageMetadata {
    VERSION_REF_STORAGE_METADATA.get_or_init(|| builtin_version_storage_metadata("lix_version_ref"))
}

fn builtin_version_storage_metadata(schema_key: &str) -> BuiltinSchemaStorageMetadata {
    builtin_schema_storage_metadata(schema_key)
        .unwrap_or_else(|| panic!("builtin version storage metadata '{schema_key}' should exist"))
}
