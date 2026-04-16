//! Engine-wide version runtime helpers.
//!
//! This subsystem owns built-in version row metadata and snapshot codecs used
//! across catalog, SQL, live-state, init, and session version workflows.

mod frontier;

use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::schema::{builtin_schema_definition, builtin_schema_storage_defaults};
use crate::schema::{LixActiveVersion, LixVersionDescriptor, LixVersionRef};
use crate::LixError;

pub use frontier::CommittedVersionFrontier;

pub const GLOBAL_VERSION_ID: &str = "global";

static VERSION_DESCRIPTOR_SCHEMA_METADATA: OnceLock<VersionRowSchemaMetadata> = OnceLock::new();
static VERSION_REF_SCHEMA_METADATA: OnceLock<VersionRowSchemaMetadata> = OnceLock::new();
static ACTIVE_VERSION_SCHEMA_METADATA: OnceLock<ActiveVersionSchemaMetadata> = OnceLock::new();

struct VersionRowSchemaMetadata {
    schema_key: String,
    schema_version: String,
    file_id: Option<String>,
    plugin_key: Option<String>,
    storage_version_id: String,
}

struct ActiveVersionSchemaMetadata {
    schema_key: String,
    storage_version_id: String,
}

pub fn version_descriptor_schema_key() -> &'static str {
    &version_descriptor_schema_metadata().schema_key
}

pub fn version_descriptor_schema_version() -> &'static str {
    &version_descriptor_schema_metadata().schema_version
}

pub fn version_descriptor_file_id() -> Option<&'static str> {
    version_descriptor_schema_metadata().file_id.as_deref()
}

pub fn version_descriptor_plugin_key() -> Option<&'static str> {
    version_descriptor_schema_metadata().plugin_key.as_deref()
}

pub fn version_ref_schema_key() -> &'static str {
    &version_ref_schema_metadata().schema_key
}

pub fn version_ref_schema_version() -> &'static str {
    &version_ref_schema_metadata().schema_version
}

pub fn version_ref_file_id() -> Option<&'static str> {
    version_ref_schema_metadata().file_id.as_deref()
}

pub fn version_ref_plugin_key() -> Option<&'static str> {
    version_ref_schema_metadata().plugin_key.as_deref()
}

pub fn version_ref_storage_version_id() -> &'static str {
    &version_ref_schema_metadata().storage_version_id
}

pub fn active_version_schema_key() -> &'static str {
    &active_version_schema_metadata().schema_key
}

pub fn active_version_storage_version_id() -> &'static str {
    &active_version_schema_metadata().storage_version_id
}

pub fn version_descriptor_snapshot_content(id: &str, name: &str, hidden: bool) -> String {
    serde_json::to_string(&LixVersionDescriptor {
        id: id.to_string(),
        name: Some(name.to_string()),
        hidden,
    })
    .expect("lix_version_descriptor snapshot serialization must succeed")
}

pub fn parse_version_descriptor_snapshot(
    snapshot_content: &str,
) -> Result<LixVersionDescriptor, LixError> {
    serde_json::from_str(snapshot_content).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version descriptor snapshot_content invalid JSON: {error}"),
    })
}

pub fn version_ref_snapshot_content(id: &str, commit_id: &str) -> String {
    serde_json::to_string(&LixVersionRef {
        id: id.to_string(),
        commit_id: commit_id.to_string(),
    })
    .expect("lix_version_ref snapshot serialization must succeed")
}

pub fn parse_version_ref_snapshot(snapshot_content: &str) -> Result<LixVersionRef, LixError> {
    serde_json::from_str(snapshot_content).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version ref snapshot_content invalid JSON: {error}"),
    })
}

pub fn parse_active_version_snapshot(snapshot_content: &str) -> Result<String, LixError> {
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

fn version_descriptor_schema_metadata() -> &'static VersionRowSchemaMetadata {
    VERSION_DESCRIPTOR_SCHEMA_METADATA
        .get_or_init(|| parse_version_row_schema_metadata("lix_version_descriptor"))
}

fn version_ref_schema_metadata() -> &'static VersionRowSchemaMetadata {
    VERSION_REF_SCHEMA_METADATA.get_or_init(|| parse_version_row_schema_metadata("lix_version_ref"))
}

fn active_version_schema_metadata() -> &'static ActiveVersionSchemaMetadata {
    ACTIVE_VERSION_SCHEMA_METADATA.get_or_init(parse_active_version_schema_metadata)
}

fn parse_version_row_schema_metadata(schema_key: &str) -> VersionRowSchemaMetadata {
    let metadata = parse_builtin_row_schema_metadata(schema_key);

    VersionRowSchemaMetadata {
        schema_key: metadata.schema_key,
        schema_version: metadata.schema_version,
        file_id: metadata.file_id,
        plugin_key: metadata.plugin_key,
        storage_version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

fn parse_active_version_schema_metadata() -> ActiveVersionSchemaMetadata {
    let metadata = parse_builtin_row_schema_metadata("lix_active_version");

    ActiveVersionSchemaMetadata {
        schema_key: metadata.schema_key,
        storage_version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

struct ParsedBuiltinRowSchemaMetadata {
    schema_key: String,
    schema_version: String,
    file_id: Option<String>,
    plugin_key: Option<String>,
}

fn parse_builtin_row_schema_metadata(schema_key: &str) -> ParsedBuiltinRowSchemaMetadata {
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
    let defaults = builtin_schema_storage_defaults(schema_key)
        .unwrap_or_else(|| panic!("builtin schema '{schema_key}' must define storage defaults"));
    ParsedBuiltinRowSchemaMetadata {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: defaults.file_id.map(str::to_string),
        plugin_key: defaults.plugin_key.map(str::to_string),
    }
}
