use std::collections::BTreeSet;

use crate::engine2::changelog::CanonicalChange;
use crate::engine2::live_state::LiveStateRow;

/// Transaction-hydrated state row.
///
/// This is the row form owned by a write transaction after generated fields
/// have been assigned but before the rows are durably flushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: String,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

impl From<StagedStateRow> for LiveStateRow {
    fn from(row: StagedStateRow) -> Self {
        LiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            plugin_key: row.plugin_key,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            version_id: row.version_id,
        }
    }
}

impl From<&StagedStateRow> for LiveStateRow {
    fn from(row: &StagedStateRow) -> Self {
        LiveStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            plugin_key: row.plugin_key.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            schema_version: row.schema_version.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            global: row.global,
            change_id: row.change_id.clone(),
            commit_id: row.commit_id.clone(),
            untracked: row.untracked,
            version_id: row.version_id.clone(),
        }
    }
}

impl From<StagedStateRow> for CanonicalChange {
    fn from(row: StagedStateRow) -> Self {
        CanonicalChange {
            id: row.change_id,
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            file_id: row.file_id,
            plugin_key: row.plugin_key,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            created_at: row.created_at,
        }
    }
}

/// Transaction-local commit membership accumulated while rows are staged.
///
/// Final commit row materialization owns commit ids, parent heads, and commit
/// row timestamps. Staging only tracks which hydrated tracked changes belong
/// to the future commit for a version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StagedCommitMembers {
    pub(crate) change_ids: BTreeSet<String>,
}

impl StagedCommitMembers {
    pub(crate) fn add_change_id(&mut self, change_id: String) {
        self.change_ids.insert(change_id);
    }

    pub(crate) fn remove_change_id(&mut self, change_id: &str) {
        self.change_ids.remove(change_id);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.change_ids.is_empty()
    }
}
