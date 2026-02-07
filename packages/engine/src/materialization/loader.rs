use std::collections::BTreeMap;

use crate::builtin_schema::types::{
    LixCommit, LixCommitEdge, LixVersionDescriptor, LixVersionPointer,
};

use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone)]
pub(crate) struct ChangeRecord {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitRecord {
    pub id: String,
    pub entity_id: String,
    pub snapshot: LixCommit,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct VersionPointerRecord {
    pub id: String,
    pub snapshot: LixVersionPointer,
    pub created_at: String,
}

#[derive(Debug, Clone)]
struct VersionPointerLatestChange {
    id: String,
    entity_id: String,
    snapshot_content: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct VersionDescriptorRecord {
    pub id: String,
    pub entity_id: String,
    pub snapshot: LixVersionDescriptor,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitEdgeRecord {
    pub id: String,
    pub snapshot: LixCommitEdge,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedData {
    pub changes: BTreeMap<String, ChangeRecord>,
    pub commits: BTreeMap<String, CommitRecord>,
    pub version_pointers: Vec<VersionPointerRecord>,
    pub version_descriptors: BTreeMap<String, VersionDescriptorRecord>,
    pub commit_edges: Vec<CommitEdgeRecord>,
}

pub(crate) async fn load_data(backend: &dyn LixBackend) -> Result<LoadedData, LixError> {
    let sql = "SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content AS snapshot_content, c.created_at \
               FROM lix_internal_change c \
               LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id";
    let result = backend.execute(sql, &[]).await?;

    let mut changes = BTreeMap::new();
    let mut commits = BTreeMap::new();
    let mut version_pointers = Vec::new();
    let mut latest_version_pointer_by_entity: BTreeMap<String, VersionPointerLatestChange> =
        BTreeMap::new();
    let mut version_descriptors = BTreeMap::new();
    let mut commit_edges = Vec::new();

    for row in result.rows {
        let id = text_required(&row, 0, "id")?;
        let entity_id = text_required(&row, 1, "entity_id")?;
        let schema_key = text_required(&row, 2, "schema_key")?;
        let schema_version = text_required(&row, 3, "schema_version")?;
        let file_id = text_required(&row, 4, "file_id")?;
        let plugin_key = text_required(&row, 5, "plugin_key")?;
        let snapshot_content = text_optional(&row, 6, "snapshot_content")?;
        let created_at = text_required(&row, 7, "created_at")?;

        let change = ChangeRecord {
            id: id.clone(),
            entity_id: entity_id.clone(),
            schema_key: schema_key.clone(),
            schema_version: schema_version.clone(),
            file_id: file_id.clone(),
            plugin_key: plugin_key.clone(),
            snapshot_content: snapshot_content.clone(),
            created_at: created_at.clone(),
        };

        changes.insert(id.clone(), change.clone());

        if schema_key == "lix_commit" {
            if let Some(snapshot_raw) = snapshot_content {
                if let Some(snapshot) = parse_commit_snapshot(&snapshot_raw)? {
                    let candidate = CommitRecord {
                        id: id.clone(),
                        entity_id: entity_id.clone(),
                        snapshot,
                        created_at,
                    };
                    upsert_latest_by_entity(&mut commits, candidate, |record| {
                        record.entity_id.clone()
                    });
                }
            }
            continue;
        }

        if schema_key == "lix_version_pointer" {
            let candidate = VersionPointerLatestChange {
                id: id.clone(),
                entity_id: entity_id.clone(),
                snapshot_content: snapshot_content.clone(),
                created_at: created_at.clone(),
            };
            upsert_latest_version_pointer_change(&mut latest_version_pointer_by_entity, candidate);
            continue;
        }

        if schema_key == "lix_version_descriptor" {
            if let Some(snapshot_raw) = snapshot_content {
                if let Some(snapshot) = parse_version_descriptor_snapshot(&snapshot_raw)? {
                    let candidate = VersionDescriptorRecord {
                        id: id.clone(),
                        entity_id: entity_id.clone(),
                        snapshot,
                        created_at,
                    };
                    upsert_latest_by_entity(&mut version_descriptors, candidate, |record| {
                        record.entity_id.clone()
                    });
                }
            }
            continue;
        }

        if schema_key == "lix_commit_edge" {
            if let Some(snapshot_raw) = snapshot_content {
                if let Some(snapshot) = parse_commit_edge_snapshot(&snapshot_raw)? {
                    commit_edges.push(CommitEdgeRecord {
                        id,
                        snapshot,
                        created_at,
                    });
                }
            }
        }
    }

    for latest in latest_version_pointer_by_entity.into_values() {
        let Some(snapshot_raw) = latest.snapshot_content.as_deref() else {
            // Tombstone (NULL snapshot): this version pointer is currently deleted.
            continue;
        };
        if let Some(snapshot) = parse_version_pointer_snapshot(snapshot_raw)? {
            version_pointers.push(VersionPointerRecord {
                id: latest.id,
                snapshot,
                created_at: latest.created_at,
            });
        }
    }

    version_pointers.sort_by(|a, b| {
        a.snapshot
            .id
            .cmp(&b.snapshot.id)
            .then_with(|| a.snapshot.commit_id.cmp(&b.snapshot.commit_id))
            .then_with(|| b.created_at.cmp(&a.created_at))
            .then_with(|| b.id.cmp(&a.id))
    });

    commit_edges.sort_by(|a, b| {
        a.snapshot
            .parent_id
            .cmp(&b.snapshot.parent_id)
            .then_with(|| a.snapshot.child_id.cmp(&b.snapshot.child_id))
            .then_with(|| b.created_at.cmp(&a.created_at))
            .then_with(|| b.id.cmp(&a.id))
    });

    Ok(LoadedData {
        changes,
        commits,
        version_pointers,
        version_descriptors,
        commit_edges,
    })
}

fn upsert_latest_version_pointer_change(
    target: &mut BTreeMap<String, VersionPointerLatestChange>,
    candidate: VersionPointerLatestChange,
) {
    match target.get(&candidate.entity_id) {
        Some(existing)
            if existing.created_at > candidate.created_at
                || (existing.created_at == candidate.created_at && existing.id >= candidate.id) => {
        }
        _ => {
            target.insert(candidate.entity_id.clone(), candidate);
        }
    }
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
    fn created_at_value(&self) -> &str;
    fn id_value(&self) -> &str;

    fn is_newer_than(&self, other: &Self) -> bool {
        self.created_at_value() > other.created_at_value()
            || (self.created_at_value() == other.created_at_value()
                && self.id_value() > other.id_value())
    }
}

impl HasOrder for CommitRecord {
    fn created_at_value(&self) -> &str {
        &self.created_at
    }

    fn id_value(&self) -> &str {
        &self.id
    }
}

impl HasOrder for VersionDescriptorRecord {
    fn created_at_value(&self) -> &str {
        &self.created_at
    }

    fn id_value(&self) -> &str {
        &self.id
    }
}

fn parse_commit_snapshot(raw: &str) -> Result<Option<LixCommit>, LixError> {
    let mut parsed: LixCommit = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("materialization: invalid lix_commit snapshot JSON: {error}"),
    })?;

    if parsed.id.is_empty() {
        return Ok(None);
    }
    parsed.change_ids.retain(|value| !value.is_empty());
    parsed.parent_commit_ids.retain(|value| !value.is_empty());
    parsed.author_account_ids.retain(|value| !value.is_empty());
    parsed.meta_change_ids.retain(|value| !value.is_empty());
    Ok(Some(parsed))
}

fn parse_version_pointer_snapshot(raw: &str) -> Result<Option<LixVersionPointer>, LixError> {
    let parsed: LixVersionPointer = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("materialization: invalid lix_version_pointer snapshot JSON: {error}"),
    })?;

    if parsed.id.is_empty() || parsed.commit_id.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

fn parse_version_descriptor_snapshot(raw: &str) -> Result<Option<LixVersionDescriptor>, LixError> {
    let parsed: LixVersionDescriptor = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("materialization: invalid lix_version_descriptor snapshot JSON: {error}"),
    })?;

    if parsed.id.is_empty() {
        return Ok(None);
    }
    Ok(Some(parsed))
}

fn parse_commit_edge_snapshot(raw: &str) -> Result<Option<LixCommitEdge>, LixError> {
    let parsed: LixCommitEdge = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("materialization: invalid lix_commit_edge snapshot JSON: {error}"),
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
            message: format!("materialization: missing column '{label}' at index {index}"),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!(
                "materialization: expected text for column '{label}' at index {index}"
            ),
        }),
    }
}

fn text_optional(row: &[Value], index: usize, label: &str) -> Result<Option<String>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!("materialization: missing column '{label}' at index {index}"),
        });
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        _ => Err(LixError {
            message: format!(
                "materialization: expected nullable text for column '{label}' at index {index}"
            ),
        }),
    }
}
