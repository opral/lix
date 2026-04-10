use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::CanonicalChangeWrite;
use crate::contracts::change::TrackedChangeView;
use crate::contracts::GLOBAL_VERSION_ID;
use crate::live_state::LiveRow;
use crate::session::version_ops::VersionInfo;
use crate::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, LixError,
    VersionId,
};

use super::UpdatedVersionRef;

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
    pub(crate) writer_key: Option<String>,
    pub(crate) created_at: Option<String>,
}

impl TrackedChangeView for StagedChange {
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

    fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }
}

pub(crate) fn tracked_live_rows_from_staged_changes(
    changes: &[StagedChange],
    execution_writer_key: Option<&str>,
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
            let file_id = change.file_id.as_ref().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live row materialization requires file_id for '{}:{}'",
                        change.schema_key, change.entity_id
                    ),
                )
            })?;
            let plugin_key = change.plugin_key.as_ref().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live row materialization requires plugin_key for '{}:{}'",
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
                file_id: file_id.to_string(),
                schema_key: change.schema_key.to_string(),
                schema_version: schema_version.to_string(),
                version_id: change.version_id.to_string(),
                plugin_key: plugin_key.to_string(),
                metadata: change.metadata.clone(),
                change_id: Some(change_id),
                writer_key: change
                    .writer_key
                    .clone()
                    .or_else(|| execution_writer_key.map(str::to_string)),
                global: change.version_id.as_str() == GLOBAL_VERSION_ID,
                untracked: false,
                created_at: Some(created_at.clone()),
                updated_at: Some(created_at),
                snapshot_content: change.snapshot_content.clone(),
            })
        })
        .collect()
}

pub(crate) fn untracked_live_rows_from_updated_version_refs(
    updates: &[UpdatedVersionRef],
) -> Vec<LiveRow> {
    updates
        .iter()
        .map(|update| LiveRow {
            entity_id: update.version_id.to_string(),
            file_id: crate::contracts::version_artifacts::version_ref_file_id().to_string(),
            schema_key: crate::contracts::version_artifacts::version_ref_schema_key().to_string(),
            schema_version: crate::contracts::version_artifacts::version_ref_schema_version()
                .to_string(),
            version_id: crate::contracts::version_artifacts::version_ref_storage_version_id()
                .to_string(),
            plugin_key: crate::contracts::version_artifacts::version_ref_plugin_key().to_string(),
            metadata: None,
            change_id: None,
            writer_key: None,
            global: true,
            untracked: true,
            created_at: Some(update.created_at.clone()),
            updated_at: Some(update.created_at.clone()),
            snapshot_content: Some(
                crate::contracts::version_artifacts::version_ref_snapshot_content(
                    update.version_id.as_str(),
                    &update.commit_id,
                ),
            ),
        })
        .collect()
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
