use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::{
    canonical_untracked_visibility_write_from_change_visibility, CanonicalChangeWrite,
    CanonicalUntrackedVisibilityWrite, UpdatedVersionRef,
};
use crate::live_state::LiveRow;
use crate::schema::{builtin_schema_definition, builtin_schema_storage_defaults};
use crate::session::version_ops::VersionInfo;
use crate::streams::StateChangeRecord;
use crate::transaction::PendingCommitLane;
use crate::version::GLOBAL_VERSION_ID;
use crate::{
    CanonicalJson, CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId,
    FileId, LixError, VersionId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedChange {
    pub(crate) id: Option<String>,
    pub(crate) entity_id: EntityId,
    pub(crate) schema_key: CanonicalSchemaKey,
    pub(crate) schema_version: Option<CanonicalSchemaVersion>,
    pub(crate) file_id: Option<FileId>,
    pub(crate) plugin_key: Option<CanonicalPluginKey>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) version_id: VersionId,
    pub(crate) origin_key: Option<String>,
    pub(crate) created_at: Option<String>,
}

impl StateChangeRecord for StagedChange {
    fn entity_id(&self) -> &str {
        self.entity_id.as_str()
    }

    fn schema_key(&self) -> &str {
        self.schema_key.as_str()
    }

    fn schema_version(&self) -> Option<&str> {
        self.schema_version.as_ref().map(|value| value.as_str())
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id.as_ref().map(|value| value.as_str())
    }

    fn plugin_key(&self) -> Option<&str> {
        self.plugin_key.as_ref().map(|value| value.as_str())
    }

    fn snapshot_content(&self) -> Option<&str> {
        self.snapshot_content.as_deref()
    }

    fn version_id(&self) -> &str {
        self.version_id.as_str()
    }

    fn origin_key(&self) -> Option<&str> {
        self.origin_key.as_deref()
    }
}

pub(crate) fn tracked_live_rows_from_staged_changes(
    changes: &[StagedChange],
) -> Result<Vec<LiveRow>, LixError> {
    changes
        .iter()
        .map(|change| {
            let schema_version = change.schema_version.as_ref().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live row materialization requires schema_version for '{}:{}'",
                        change.schema_key, change.entity_id
                    ),
                )
            })?;
            let change_id = change.id.clone().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live row materialization requires id for '{}:{}'",
                        change.schema_key, change.entity_id
                    ),
                )
            })?;
            let created_at = change.created_at.clone().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live row materialization requires created_at for '{}:{}'",
                        change.schema_key, change.entity_id
                    ),
                )
            })?;

            Ok(LiveRow {
                entity_id: change.entity_id.to_string(),
                file_id: change.file_id.as_ref().map(ToString::to_string),
                schema_key: change.schema_key.to_string(),
                schema_version: schema_version.to_string(),
                version_id: change.version_id.to_string(),
                plugin_key: change.plugin_key.as_ref().map(ToString::to_string),
                metadata: change.metadata.clone(),
                change_id: Some(change_id),
                global: change.version_id.as_str() == GLOBAL_VERSION_ID,
                untracked: false,
                created_at: Some(created_at.clone()),
                updated_at: Some(created_at),
                snapshot_content: change.snapshot_content.clone(),
            })
        })
        .collect()
}

pub(crate) fn tracked_live_commit_rows_from_canonical_changes(
    canonical_changes: &[CanonicalChangeWrite],
    updates: &[UpdatedVersionRef],
) -> Result<Vec<LiveRow>, LixError> {
    let commit_schema = builtin_live_schema_meta("lix_commit")?;

    canonical_changes
        .iter()
        .filter(|change| change.schema_key.as_str() == "lix_commit")
        .map(|change| {
            let commit_id = change.entity_id.as_str();
            if !updates
                .iter()
                .any(|update| update.commit_id.as_str() == commit_id)
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live commit row materialization requires a version ref update for commit '{}'",
                        commit_id
                    ),
                ));
            }

            Ok(LiveRow {
                entity_id: commit_id.to_string(),
                file_id: commit_schema.file_id.clone(),
                schema_key: "lix_commit".to_string(),
                schema_version: commit_schema.schema_version.clone(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                plugin_key: commit_schema.plugin_key.clone(),
                metadata: change.metadata.as_ref().map(|value| value.as_str().to_string()),
                change_id: Some(change.id.clone()),
                global: true,
                untracked: false,
                created_at: Some(change.created_at.clone()),
                updated_at: Some(change.created_at.clone()),
                snapshot_content: change
                    .snapshot_content
                    .as_ref()
                    .map(|value| value.as_str().to_string()),
            })
        })
        .collect()
}

pub(crate) fn pending_commit_live_row(
    lane: &PendingCommitLane,
    commit_id: &str,
    commit_change_id: &str,
    commit_snapshot: &serde_json::Value,
    created_at: &str,
) -> Result<LiveRow, LixError> {
    let commit_schema = builtin_live_schema_meta("lix_commit")?;
    let _ = lane;

    Ok(LiveRow {
        entity_id: commit_id.to_string(),
        file_id: commit_schema.file_id,
        schema_key: "lix_commit".to_string(),
        schema_version: commit_schema.schema_version,
        version_id: GLOBAL_VERSION_ID.to_string(),
        plugin_key: commit_schema.plugin_key,
        metadata: None,
        change_id: Some(commit_change_id.to_string()),
        global: true,
        untracked: false,
        created_at: Some(created_at.to_string()),
        updated_at: Some(created_at.to_string()),
        snapshot_content: Some(serde_json::to_string(commit_snapshot).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("pending commit live row snapshot serialization failed: {error}"),
            )
        })?),
    })
}

pub(crate) fn untracked_live_rows_from_updated_version_refs(
    updates: &[UpdatedVersionRef],
) -> Vec<LiveRow> {
    updates
        .iter()
        .map(|update| LiveRow {
            entity_id: update.version_id.to_string(),
            file_id: None,
            schema_key: crate::version::version_ref_schema_key().to_string(),
            schema_version: crate::version::version_ref_schema_version().to_string(),
            version_id: crate::version::version_ref_storage_version_id().to_string(),
            plugin_key: None,
            metadata: None,
            change_id: Some(update.change_id.clone()),
            global: true,
            untracked: true,
            created_at: Some(update.created_at.clone()),
            updated_at: Some(update.created_at.clone()),
            snapshot_content: Some(crate::version::version_ref_snapshot_content(
                update.version_id.as_str(),
                &update.commit_id,
            )),
        })
        .collect()
}

pub(crate) fn canonical_changes_from_updated_version_refs(
    updates: &[UpdatedVersionRef],
) -> Result<Vec<CanonicalChangeWrite>, LixError> {
    updates
        .iter()
        .map(|update| {
            let snapshot_content = crate::version::version_ref_snapshot_content(
                update.version_id.as_str(),
                &update.commit_id,
            );
            Ok(CanonicalChangeWrite {
                id: update.change_id.clone(),
                entity_id: update.version_id.to_string().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "updated version ref '{}' is not a valid canonical entity id",
                            update.version_id
                        ),
                    )
                })?,
                schema_key: crate::version::version_ref_schema_key()
                    .to_string()
                    .try_into()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "builtin lix_version_ref schema key is invalid",
                        )
                    })?,
                schema_version: crate::version::version_ref_schema_version()
                    .to_string()
                    .try_into()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "builtin lix_version_ref schema version is invalid",
                        )
                    })?,
                file_id: None,
                plugin_key: None,
                snapshot_content: Some(CanonicalJson::from_text(snapshot_content).map_err(
                    |error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "generated lix_version_ref snapshot is invalid canonical JSON: {}",
                                error.description
                            ),
                        )
                    },
                )?),
                metadata: None,
                created_at: update.created_at.clone(),
            })
        })
        .collect()
}

pub(crate) fn canonical_untracked_visibility_rows_from_updated_version_refs(
    updates: &[UpdatedVersionRef],
) -> Result<Vec<CanonicalUntrackedVisibilityWrite>, LixError> {
    let canonical_changes = canonical_changes_from_updated_version_refs(updates)?;
    Ok(updates
        .iter()
        .zip(canonical_changes.iter())
        .map(|(update, change)| {
            canonical_untracked_visibility_write_from_change_visibility(
                change,
                crate::version::version_ref_storage_version_id(),
                true,
                Some(&update.created_at),
            )
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuiltinLiveSchemaMeta {
    schema_version: String,
    file_id: Option<String>,
    plugin_key: Option<String>,
}

fn builtin_live_schema_meta(schema_key: &str) -> Result<BuiltinLiveSchemaMeta, LixError> {
    let schema = builtin_schema_definition(schema_key).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("builtin live schema '{}' not found", schema_key),
        )
    })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "builtin live schema '{}' is missing string x-lix-version",
                    schema_key
                ),
            )
        })?
        .to_string();
    let defaults = builtin_schema_storage_defaults(schema_key).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "builtin live schema '{}' is missing storage defaults",
                schema_key
            ),
        )
    })?;
    Ok(BuiltinLiveSchemaMeta {
        schema_version,
        file_id: defaults.file_id.map(str::to_string),
        plugin_key: defaults.plugin_key.map(str::to_string),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitArgs {
    pub timestamp: String,
    pub active_accounts: Vec<String>,
    pub changes: Vec<StagedChange>,
    pub versions: BTreeMap<String, VersionInfo>,
    pub force_commit_versions: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitResult {
    pub canonical_changes: Vec<CanonicalChangeWrite>,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}
