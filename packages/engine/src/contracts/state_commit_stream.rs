use crate::contracts::TrackedChangeView;
use crate::contracts::{
    MutationOperation, MutationRow, PlannedStateRow, StateCommitStreamChange,
    StateCommitStreamOperation,
};
use crate::{LixError, Value};
use serde_json::Value as JsonValue;

const DETERMINISTIC_SETTINGS_SCHEMA_KEY: &str = "lix_key_value";
const DETERMINISTIC_SETTINGS_ENTITY_ID: &str = "lix_deterministic_mode";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StateCommitStreamFilter {
    pub schema_keys: Vec<String>,
    pub entity_ids: Vec<String>,
    pub file_ids: Vec<String>,
    pub version_ids: Vec<String>,
    pub writer_keys: Vec<String>,
    pub exclude_writer_keys: Vec<String>,
    pub include_untracked: bool,
}

impl Default for StateCommitStreamFilter {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            entity_ids: Vec::new(),
            file_ids: Vec::new(),
            version_ids: Vec::new(),
            writer_keys: Vec::new(),
            exclude_writer_keys: Vec::new(),
            include_untracked: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateCommitStreamRuntimeMetadata {
    pub writer_key: Option<String>,
}

impl StateCommitStreamRuntimeMetadata {
    pub fn from_runtime_writer_key(writer_key: Option<&str>) -> Self {
        Self {
            writer_key: writer_key.map(str::to_string),
        }
    }
}

pub fn state_commit_stream_changes_from_mutations(
    mutations: &[MutationRow],
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Vec<StateCommitStreamChange> {
    if mutations.is_empty() {
        return Vec::new();
    }

    mutations
        .iter()
        .map(|mutation| StateCommitStreamChange {
            operation: map_mutation_operation(&mutation.operation),
            entity_id: mutation.entity_id.clone(),
            schema_key: mutation.schema_key.clone(),
            schema_version: mutation.schema_version.clone(),
            file_id: mutation.file_id.clone(),
            version_id: mutation.version_id.clone(),
            plugin_key: mutation.plugin_key.clone(),
            snapshot_content: mutation.snapshot_content.clone(),
            untracked: mutation.untracked,
            writer_key: runtime_metadata.writer_key.clone(),
        })
        .collect()
}

pub fn state_commit_stream_changes_from_changes<Change: TrackedChangeView>(
    changes: &[Change],
    operation: StateCommitStreamOperation,
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Result<Vec<StateCommitStreamChange>, LixError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    let mut resolved = Vec::with_capacity(changes.len());
    for change in changes {
        let snapshot_content = match change.snapshot_content() {
            Some(snapshot_content) => Some(serde_json::from_str(snapshot_content).map_err(
                |error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "change state commit stream expected JSON snapshot_content text: {error}"
                    ),
                },
            )?),
            None => None,
        };
        resolved.push(StateCommitStreamChange {
            operation,
            entity_id: change.entity_id().to_string(),
            schema_key: change.schema_key().to_string(),
            schema_version: change
                .schema_version()
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "change state commit stream requires schema_version".to_string(),
                })?
                .to_string(),
            file_id: change
                .file_id()
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "change state commit stream requires file_id".to_string(),
                })?
                .to_string(),
            version_id: change.version_id().to_string(),
            plugin_key: change
                .plugin_key()
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "change state commit stream requires plugin_key".to_string(),
                })?
                .to_string(),
            snapshot_content,
            untracked: false,
            writer_key: state_commit_stream_writer_key(change.writer_key(), &runtime_metadata),
        });
    }

    Ok(resolved)
}

pub fn state_commit_stream_changes_from_planned_rows(
    rows: &[PlannedStateRow],
    operation: StateCommitStreamOperation,
    untracked: bool,
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Result<Vec<StateCommitStreamChange>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut resolved = Vec::with_capacity(rows.len());
    for row in rows {
        let file_id = planned_row_required_text(row, "file_id")?;
        let plugin_key = planned_row_required_text(row, "plugin_key")?;
        let schema_version = planned_row_required_text(row, "schema_version")?;
        let snapshot_content = planned_row_snapshot_content(row)?;
        let version_id = row
            .version_id
            .clone()
            .or_else(|| planned_row_optional_text(row, "version_id"));

        resolved.push(StateCommitStreamChange {
            operation,
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version,
            file_id,
            version_id: version_id.ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "planned row state commit stream requires version_id".to_string(),
            })?,
            plugin_key,
            snapshot_content,
            untracked,
            writer_key: runtime_metadata.writer_key.clone(),
        });
    }

    Ok(resolved)
}

pub fn should_invalidate_deterministic_settings_cache(
    mutations: &[MutationRow],
    state_commit_stream_changes: &[StateCommitStreamChange],
) -> bool {
    mutations.iter().any(|row| {
        row.schema_key == DETERMINISTIC_SETTINGS_SCHEMA_KEY
            && row.entity_id == DETERMINISTIC_SETTINGS_ENTITY_ID
    }) || state_commit_stream_changes.iter().any(|change| {
        change.schema_key == DETERMINISTIC_SETTINGS_SCHEMA_KEY
            && change.entity_id == DETERMINISTIC_SETTINGS_ENTITY_ID
    })
}

fn state_commit_stream_writer_key(
    row_writer_key: Option<&str>,
    runtime_metadata: &StateCommitStreamRuntimeMetadata,
) -> Option<String> {
    row_writer_key
        .map(str::to_string)
        .or_else(|| runtime_metadata.writer_key.clone())
}

fn planned_row_required_text(row: &PlannedStateRow, key: &str) -> Result<String, LixError> {
    planned_row_optional_text(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("planned row state commit stream requires '{key}'"),
    })
}

fn planned_row_optional_text(row: &PlannedStateRow, key: &str) -> Option<String> {
    match row.values.get(key) {
        Some(Value::Text(text)) => Some(text.clone()),
        Some(Value::Integer(number)) => Some(number.to_string()),
        _ => None,
    }
}

fn planned_row_snapshot_content(row: &PlannedStateRow) -> Result<Option<JsonValue>, LixError> {
    match row.values.get("snapshot_content") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Json(value)) => Ok(Some(value.clone())),
        Some(Value::Text(text)) => {
            let parsed = serde_json::from_str(text).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "planned row state commit stream expected JSON snapshot_content text: {error}"
                ),
            })?;
            Ok(Some(parsed))
        }
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "planned row state commit stream expected null/text snapshot_content, got {other:?}"
            ),
        }),
    }
}

fn map_mutation_operation(operation: &MutationOperation) -> StateCommitStreamOperation {
    match operation {
        MutationOperation::Insert => StateCommitStreamOperation::Insert,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        state_commit_stream_changes_from_changes, state_commit_stream_changes_from_planned_rows,
        StateCommitStreamOperation, StateCommitStreamRuntimeMetadata,
    };
    use crate::contracts::PlannedStateRow;
    use crate::session::version_ops::commit::StagedChange;
    use crate::Value;
    use std::collections::BTreeMap;

    #[test]
    fn changes_map_to_update_changes() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: Some("file-1".try_into().unwrap()),
                plugin_key: Some("lix".try_into().unwrap()),
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                writer_key: Some("writer-a".to_string()),
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::default(),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].operation, StateCommitStreamOperation::Update);
        assert_eq!(changes[0].entity_id, "entity-1");
        assert_eq!(changes[0].schema_key, "lix_key_value");
        assert_eq!(changes[0].writer_key.as_deref(), Some("writer-a"));
    }

    #[test]
    fn state_commit_stream_uses_runtime_writer_metadata_when_change_omits_it() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: Some("file-1".try_into().unwrap()),
                plugin_key: Some("lix".try_into().unwrap()),
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                writer_key: None,
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(Some("writer-runtime")),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].writer_key.as_deref(), Some("writer-runtime"));
    }

    #[test]
    fn state_commit_stream_prefers_change_writer_key_over_runtime_metadata() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: Some("file-1".try_into().unwrap()),
                plugin_key: Some("lix".try_into().unwrap()),
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                writer_key: Some("writer-change".to_string()),
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(Some("writer-runtime")),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].writer_key.as_deref(), Some("writer-change"));
    }

    #[test]
    fn planned_rows_accept_structured_json_snapshot_content() {
        let mut values = BTreeMap::new();
        values.insert("file_id".to_string(), Value::Text("lix".to_string()));
        values.insert("plugin_key".to_string(), Value::Text("lix".to_string()));
        values.insert("schema_version".to_string(), Value::Text("1".to_string()));
        values.insert(
            "snapshot_content".to_string(),
            Value::Json(serde_json::json!({
                "key": "observe-untracked-external",
                "value": "u1"
            })),
        );

        let changes = state_commit_stream_changes_from_planned_rows(
            &[PlannedStateRow {
                entity_id: "observe-untracked-external".to_string(),
                schema_key: "lix_key_value".to_string(),
                version_id: Some("global".to_string()),
                values,
                writer_key: None,
                tombstone: false,
            }],
            StateCommitStreamOperation::Insert,
            true,
            StateCommitStreamRuntimeMetadata::default(),
        )
        .expect("planned rows should accept structured JSON snapshot_content");

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].snapshot_content,
            Some(serde_json::json!({
                "key": "observe-untracked-external",
                "value": "u1"
            }))
        );
        assert!(changes[0].untracked);
    }
}
