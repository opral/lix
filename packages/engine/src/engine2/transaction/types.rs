use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::engine2::entity_identity::EntityIdentity;
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::untracked_state::UntrackedStateRow;
use crate::LixError;

/// Incoming state row before transaction hydration.
///
/// Write frontends produce this shape after decoding their own surface. The
/// transaction later assigns generated fields and turns it into a
/// `StagedStateRow`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StageRow {
    pub(crate) entity_id: Option<EntityIdentity>,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// Incoming file payload paired with staged filesystem rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageFileData {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

/// One decoded write batch before transaction hydration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StageWrite {
    Rows {
        rows: Vec<StageRow>,
    },
    RowsWithFileData {
        rows: Vec<StageRow>,
        file_data: Vec<StageFileData>,
        count: u64,
    },
}

/// Result returned after staging a decoded write batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageWriteOutcome {
    pub(crate) count: u64,
}

/// Execution-scoped authority for staging decoded writes into a transaction.
///
/// SQL providers, session APIs, and future write frontends should all target
/// this boundary instead of depending on concrete transaction internals.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait StageWriteStager: Send + Sync {
    async fn stage_write(&self, write: StageWrite) -> Result<StageWriteOutcome, LixError>;
}

/// Transaction-hydrated state row.
///
/// This is the row form owned by a write transaction after generated fields
/// have been assigned but before the rows are durably flushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
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

impl From<StagedStateRow> for UntrackedStateRow {
    fn from(row: StagedStateRow) -> Self {
        UntrackedStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            version_id: row.version_id,
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
    pub(crate) commit_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) change_set_id: String,
    pub(crate) created_at: String,
    pub(crate) change_ids: BTreeSet<String>,
}

impl StagedCommitMembers {
    pub(crate) fn new(
        commit_id: String,
        commit_change_id: String,
        change_set_id: String,
        created_at: String,
    ) -> Self {
        Self {
            commit_id,
            commit_change_id,
            change_set_id,
            created_at,
            change_ids: BTreeSet::new(),
        }
    }

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
