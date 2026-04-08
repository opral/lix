use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::builtin::{
    builtin_schema_definition, builtin_schema_json, decode_lixcol_literal,
};
use crate::schema::{LixActiveVersion, LixVersionDescriptor, LixVersionRef};
use crate::LixError;

pub(crate) mod checkpoints;
mod heads;
mod refs;
mod roots;

pub(crate) const GLOBAL_VERSION_ID: &str = "global";
pub(crate) const DEFAULT_ACTIVE_VERSION_NAME: &str = "main";
#[allow(unused_imports)]
pub(crate) use heads::load_current_committed_version_frontier_with_backend;
pub(crate) use heads::{
    load_committed_version_head_commit_id, load_committed_version_ref_with_backend,
    load_committed_version_ref_with_executor,
};
pub(crate) use refs::{
    load_all_local_version_refs_with_executor, load_local_version_head_commit_id_with_executor,
    load_local_version_ref_heads_map_with_executor,
};
pub(crate) use roots::{
    resolve_history_root_facts_with_backend, HistoryRootFacts, HistoryRootTraversal,
    RootCommitResolutionRequest, RootCommitScope, RootLineageScope, RootVersionScope,
};

static VERSION_DESCRIPTOR_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();
static VERSION_REF_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();
static ACTIVE_VERSION_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();

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

pub(crate) fn version_descriptor_snapshot_content(id: &str, name: &str, hidden: bool) -> String {
    serde_json::to_string(&LixVersionDescriptor {
        id: id.to_string(),
        name: Some(name.to_string()),
        hidden,
    })
    .expect("lix_version_descriptor snapshot serialization must succeed")
}

pub(crate) fn parse_version_descriptor_snapshot(
    snapshot_content: &str,
) -> Result<LixVersionDescriptor, LixError> {
    serde_json::from_str(snapshot_content).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version descriptor snapshot_content invalid JSON: {error}"),
    })
}

pub(crate) fn version_ref_schema_key() -> &'static str {
    &version_ref_schema_metadata().schema_key
}

pub(crate) fn version_ref_schema_version() -> &'static str {
    &version_ref_schema_metadata().schema_version
}

pub(crate) fn version_ref_file_id() -> &'static str {
    &version_ref_schema_metadata().file_id
}

pub(crate) fn version_ref_plugin_key() -> &'static str {
    &version_ref_schema_metadata().plugin_key
}

pub(crate) fn version_ref_storage_version_id() -> &'static str {
    &version_ref_schema_metadata().storage_version_id
}

pub(crate) fn version_ref_snapshot_content(id: &str, commit_id: &str) -> String {
    serde_json::to_string(&LixVersionRef {
        id: id.to_string(),
        commit_id: commit_id.to_string(),
    })
    .expect("lix_version_ref snapshot serialization must succeed")
}

pub(crate) fn active_version_schema_key() -> &'static str {
    &active_version_schema_metadata().schema_key
}

pub(crate) fn active_version_file_id() -> &'static str {
    &active_version_schema_metadata().file_id
}

pub(crate) fn active_version_storage_version_id() -> &'static str {
    &active_version_schema_metadata().storage_version_id
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

fn version_descriptor_schema_metadata() -> &'static SchemaMetadata {
    VERSION_DESCRIPTOR_SCHEMA_METADATA
        .get_or_init(|| parse_schema_metadata("lix_version_descriptor"))
}

fn version_ref_schema_metadata() -> &'static SchemaMetadata {
    VERSION_REF_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_version_ref"))
}

fn active_version_schema_metadata() -> &'static SchemaMetadata {
    ACTIVE_VERSION_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_active_version"))
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
    let storage_version_id =
        if overrides.get("lixcol_global").and_then(JsonValue::as_str) == Some("true") {
            GLOBAL_VERSION_ID.to_string()
        } else {
            GLOBAL_VERSION_ID.to_string()
        };

    SchemaMetadata {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: decode_lixcol_literal(file_id_raw),
        plugin_key: decode_lixcol_literal(plugin_key_raw),
        storage_version_id,
    }
}
