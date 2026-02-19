use crate::exports::lix::plugin::api::{EntityChange, File, Guest, PluginError};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::OnceLock;

wit_bindgen::generate!({
    path: "../engine/wit",
    world: "plugin",
});

pub const SCHEMA_KEY: &str = "lix_binary_blob";
pub const SCHEMA_VERSION: &str = "1";
const BINARY_BLOB_SCHEMA_JSON: &str = include_str!("../schema/binary_blob.json");

static BINARY_BLOB_SCHEMA: OnceLock<Value> = OnceLock::new();

pub use crate::exports::lix::plugin::api::{
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct BinaryPlugin;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotContent {
    id: String,
    value: String,
}

impl Guest for BinaryPlugin {
    fn detect_changes(
        before: Option<File>,
        after: File,
        _state_context: Option<crate::exports::lix::plugin::api::DetectStateContext>,
    ) -> Result<Vec<EntityChange>, PluginError> {
        let before_base64 = before.as_ref().map(|file| BASE64.encode(&file.data));
        let after_base64 = BASE64.encode(&after.data);

        if before_base64.as_deref() == Some(after_base64.as_str()) {
            return Ok(Vec::new());
        }

        let snapshot_content = serde_json::to_string(&SnapshotContent {
            id: after.id.clone(),
            value: after_base64,
        })
        .map_err(|error| {
            PluginError::Internal(format!("failed to serialize snapshot content: {error}"))
        })?;

        Ok(vec![EntityChange {
            entity_id: after.id,
            schema_key: SCHEMA_KEY.to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content),
        }])
    }

    fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
        let mut candidate: Option<SnapshotContent> = None;

        for change in changes {
            if change.schema_key != SCHEMA_KEY {
                continue;
            }
            if change.schema_version != SCHEMA_VERSION {
                return Err(PluginError::InvalidInput(format!(
                    "unsupported schema_version '{}' for schema_key '{}', expected '{}'",
                    change.schema_version, SCHEMA_KEY, SCHEMA_VERSION
                )));
            }

            let Some(raw_snapshot) = change.snapshot_content else {
                continue;
            };

            if change.entity_id != file.id {
                return Err(PluginError::InvalidInput(format!(
                    "expected entity_id '{}' for file '{}', got '{}'",
                    file.id, file.id, change.entity_id
                )));
            }

            if candidate.is_some() {
                return Err(PluginError::InvalidInput(format!(
                    "expected at most one live '{}' row for file '{}'",
                    SCHEMA_KEY, file.id
                )));
            }

            let parsed: SnapshotContent = serde_json::from_str(&raw_snapshot).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "snapshot_content for file '{}' is invalid JSON: {error}",
                    file.id
                ))
            })?;

            if parsed.id != file.id {
                return Err(PluginError::InvalidInput(format!(
                    "snapshot_content.id '{}' does not match file id '{}'",
                    parsed.id, file.id
                )));
            }

            candidate = Some(parsed);
        }

        let Some(snapshot) = candidate else {
            return Err(PluginError::InvalidInput(format!(
                "expected exactly one live '{}' row for file '{}'",
                SCHEMA_KEY, file.id
            )));
        };

        BASE64.decode(snapshot.value.as_bytes()).map_err(|error| {
            PluginError::InvalidInput(format!("snapshot_content.value must be base64: {error}"))
        })
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    <BinaryPlugin as Guest>::detect_changes(before, after, None)
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<crate::exports::lix::plugin::api::DetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    <BinaryPlugin as Guest>::detect_changes(before, after, state_context)
}

pub fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    <BinaryPlugin as Guest>::apply_changes(file, changes)
}

pub fn schema_json() -> &'static str {
    BINARY_BLOB_SCHEMA_JSON
}

pub fn schema_definition() -> &'static Value {
    BINARY_BLOB_SCHEMA.get_or_init(|| {
        serde_json::from_str(BINARY_BLOB_SCHEMA_JSON)
            .expect("binary blob schema JSON must be valid")
    })
}

export!(BinaryPlugin);
