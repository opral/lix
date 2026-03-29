use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::live_state::ReplayCursor;
use crate::schema::builtin::types::{LixCommit, LixCommitEdge, LixVersionDescriptor};

use crate::{CanonicalJson, LixError, Value};

#[derive(Debug, Clone)]
pub(crate) struct ChangeRecord {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub replay_cursor: ReplayCursor,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitRecord {
    pub id: String,
    pub entity_id: String,
    pub snapshot: LixCommit,
    pub replay_cursor: ReplayCursor,
}

#[derive(Debug, Clone)]
pub(crate) struct VersionDescriptorRecord {
    pub id: String,
    pub entity_id: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: CanonicalJson,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub replay_cursor: ReplayCursor,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitEdgeRecord {
    pub id: String,
    pub snapshot: LixCommitEdge,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedData {
    pub changes: BTreeMap<String, ChangeRecord>,
    pub commits: BTreeMap<String, CommitRecord>,
    pub version_descriptors: BTreeMap<String, VersionDescriptorRecord>,
    pub commit_edges: Vec<CommitEdgeRecord>,
}

pub(crate) async fn load_data_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<LoadedData, LixError> {
    let sql = "SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.created_at \
               FROM lix_internal_change c \
               LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id";
    let result = executor.execute(sql, &[]).await?;

    let mut changes = BTreeMap::new();
    let mut commits = BTreeMap::new();
    let mut version_descriptors = BTreeMap::new();
    let mut commit_edges = Vec::new();

    for row in result.rows {
        let id = text_required(&row, 0, "id")?;
        let entity_id = text_required(&row, 1, "entity_id")?;
        let schema_key = text_required(&row, 2, "schema_key")?;
        let schema_version = text_required(&row, 3, "schema_version")?;
        let file_id = text_required(&row, 4, "file_id")?;
        let plugin_key = text_required(&row, 5, "plugin_key")?;
        let snapshot_content_raw = json_text_optional(&row, 6, "snapshot_content")?;
        let metadata_raw = json_text_optional(&row, 7, "metadata")?;
        let snapshot_content = snapshot_content_raw
            .as_ref()
            .map(|s| CanonicalJson::from_text(s))
            .transpose()?;
        let metadata = metadata_raw
            .as_ref()
            .map(|s| CanonicalJson::from_text(s))
            .transpose()?;
        let created_at = text_required(&row, 8, "created_at")?;
        let replay_cursor = ReplayCursor::new(id.clone(), created_at.clone());

        let change = ChangeRecord {
            id: id.clone(),
            entity_id: entity_id.clone(),
            schema_key: schema_key.clone(),
            schema_version: schema_version.clone(),
            file_id: file_id.clone(),
            plugin_key: plugin_key.clone(),
            snapshot_content: snapshot_content.clone(),
            metadata: metadata.clone(),
            created_at: created_at.clone(),
            replay_cursor: replay_cursor.clone(),
        };

        changes.insert(id.clone(), change.clone());

        if schema_key == "lix_commit" {
            if let Some(ref snapshot_canonical) = snapshot_content {
                if let Some(snapshot) = parse_commit_snapshot(snapshot_canonical)? {
                    let candidate = CommitRecord {
                        id: id.clone(),
                        entity_id: entity_id.clone(),
                        snapshot,
                        replay_cursor: replay_cursor.clone(),
                    };
                    upsert_latest_by_entity(&mut commits, candidate, |record| {
                        record.entity_id.clone()
                    });
                }
            }
            continue;
        }

        if schema_key == "lix_version_ref" {
            continue;
        }

        if schema_key == "lix_version_descriptor" {
            if let Some(ref snapshot_canonical) = snapshot_content {
                if parse_version_descriptor_snapshot(snapshot_canonical)?.is_none() {
                    continue;
                }
            }
            let Some(snapshot_canonical) = snapshot_content else {
                continue;
            };
            let candidate = VersionDescriptorRecord {
                id: id.clone(),
                entity_id: entity_id.clone(),
                schema_version: schema_version.clone(),
                file_id: file_id.clone(),
                plugin_key: plugin_key.clone(),
                snapshot_content: snapshot_canonical,
                metadata: metadata.clone(),
                created_at,
                replay_cursor,
            };
            upsert_latest_by_entity(&mut version_descriptors, candidate, |record| {
                record.entity_id.clone()
            });
            continue;
        }

        if schema_key == "lix_commit_edge" {
            if let Some(ref snapshot_canonical) = snapshot_content {
                if let Some(snapshot) = parse_commit_edge_snapshot(snapshot_canonical)? {
                    commit_edges.push(CommitEdgeRecord { id, snapshot });
                }
            }
        }
    }

    commit_edges.sort_by(|a, b| {
        a.snapshot
            .parent_id
            .cmp(&b.snapshot.parent_id)
            .then_with(|| a.snapshot.child_id.cmp(&b.snapshot.child_id))
            .then_with(|| b.id.cmp(&a.id))
    });

    Ok(LoadedData {
        changes,
        commits,
        version_descriptors,
        commit_edges,
    })
}

fn upsert_latest_by_entity<T, F>(target: &mut BTreeMap<String, T>, candidate: T, key: F)
where
    T: Clone + HasOrder,
    F: Fn(&T) -> String,
{
    let entity_key = key(&candidate);
    match target.get(&entity_key) {
        Some(existing) if !candidate.is_newer_than(existing) => {}
        _ => {
            target.insert(entity_key, candidate);
        }
    }
}

trait HasOrder {
    fn replay_cursor(&self) -> &ReplayCursor;

    fn is_newer_than(&self, other: &Self) -> bool {
        self.replay_cursor().is_newer_than(other.replay_cursor())
    }
}

impl HasOrder for CommitRecord {
    fn replay_cursor(&self) -> &ReplayCursor {
        &self.replay_cursor
    }
}

impl HasOrder for VersionDescriptorRecord {
    fn replay_cursor(&self) -> &ReplayCursor {
        &self.replay_cursor
    }
}

fn parse_commit_snapshot(raw: &CanonicalJson) -> Result<Option<LixCommit>, LixError> {
    let mut parsed: LixCommit = raw.parse().map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "materialization: invalid lix_commit snapshot JSON: {}",
            error.description
        ),
    })?;

    if parsed.id.is_empty() {
        return Ok(None);
    }
    parsed.change_ids.retain(|value| !value.is_empty());
    parsed.parent_commit_ids.retain(|value| !value.is_empty());
    parsed.author_account_ids.retain(|value| !value.is_empty());
    Ok(Some(parsed))
}

fn parse_version_descriptor_snapshot(
    raw: &CanonicalJson,
) -> Result<Option<LixVersionDescriptor>, LixError> {
    let parsed: LixVersionDescriptor = raw.parse().map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "materialization: invalid lix_version_descriptor snapshot JSON: {}",
            error.description
        ),
    })?;

    if parsed.id.is_empty() {
        return Ok(None);
    }
    Ok(Some(parsed))
}

fn parse_commit_edge_snapshot(raw: &CanonicalJson) -> Result<Option<LixCommitEdge>, LixError> {
    let parsed: LixCommitEdge = raw.parse().map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "materialization: invalid lix_commit_edge snapshot JSON: {}",
            error.description
        ),
    })?;

    if parsed.parent_id.is_empty() || parsed.child_id.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

fn text_required(row: &[Value], index: usize, label: &str) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("materialization: missing column '{label}' at index {index}"),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "materialization: expected text for column '{label}' at index {index}"
            ),
        }),
    }
}

fn text_optional(row: &[Value], index: usize, label: &str) -> Result<Option<String>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("materialization: missing column '{label}' at index {index}"),
        });
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "materialization: expected nullable text for column '{label}' at index {index}"
            ),
        }),
    }
}

fn json_text_optional(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<Option<CanonicalJson>, LixError> {
    let Some(text) = text_optional(row, index, label)? else {
        return Ok(None);
    };
    CanonicalJson::from_text(text).map(Some).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "materialization: invalid canonical JSON in '{label}': {}",
                error.description
            ),
        )
    })
}
