use std::collections::BTreeSet;

use crate::entity_identity::EntityIdentity;
use crate::live_state::LiveStateRow;
use crate::tracked_state::TrackedStateRow;
use crate::untracked_state::UntrackedStateRow;
use crate::RowMetadata;

/// Incoming state row before transaction hydration.
///
/// Write frontends produce this shape after decoding their own surface. The
/// transaction later assigns generated fields and turns it into a
/// `StagedStateRow`.
///
/// SQL providers stage semantic rows, not final storage rows. INSERT providers
/// may omit defaulted snapshot fields and leave `entity_id` unset when the
/// target schema has an `x-lix-primary-key`; transaction normalization applies
/// schema defaults and derives the final identity. Typed UPDATE providers must
/// stage full rewritten snapshots after applying column assignments to the
/// existing row. Raw `lix_state` snapshot updates are replacement writes, not
/// implicit patches.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StageRow {
    pub(crate) entity_id: Option<EntityIdentity>,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<RowMetadata>,
    pub(crate) origin: Option<StageRowOrigin>,
    pub(crate) schema_version: String,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// User-facing write operation that produced one physical staged row.
///
/// Composite SQL surfaces such as `lix_file` lower one logical row into
/// multiple state rows. The transaction layer owns final constraint validation,
/// but error messages should stay in the vocabulary of the logical operation
/// when the caller did not write the physical state schema directly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StageRowOrigin {
    pub(crate) surface: String,
    pub(crate) operation: StageWriteOperation,
    pub(crate) primary_key: Option<LogicalPrimaryKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum StageWriteOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalPrimaryKey {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<String>,
}

/// Incoming file payload paired with staged filesystem rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageFileData {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

/// Existing canonical change adopted into another version's tracked projection.
///
/// Merges use this path when the source side already owns the canonical
/// changelog fact. The target commit references that existing change id and
/// writes a target-version projection row without appending a copied change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageAdoptedChange {
    pub(crate) version_id: String,
    pub(crate) change_id: String,
    pub(crate) projected_row: TrackedStateRow,
}

/// One decoded write batch before transaction hydration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StageWrite {
    Rows {
        mode: StageWriteMode,
        rows: Vec<StageRow>,
    },
    RowsWithFileData {
        mode: StageWriteMode,
        rows: Vec<StageRow>,
        file_data: Vec<StageFileData>,
        count: u64,
    },
    AdoptedChanges {
        changes: Vec<StageAdoptedChange>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageWriteMode {
    Insert,
    Replace,
}

/// Result returned after staging a decoded write batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageWriteOutcome {
    pub(crate) count: u64,
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
    pub(crate) metadata: Option<RowMetadata>,
    pub(crate) origin: Option<StageRowOrigin>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// Transaction-hydrated projection for an adopted canonical change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedAdoptedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<RowMetadata>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
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

impl From<StagedAdoptedStateRow> for LiveStateRow {
    fn from(row: StagedAdoptedStateRow) -> Self {
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
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            version_id: row.version_id,
        }
    }
}

impl From<&StagedAdoptedStateRow> for LiveStateRow {
    fn from(row: &StagedAdoptedStateRow) -> Self {
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
            change_id: Some(row.change_id.clone()),
            commit_id: Some(row.commit_id.clone()),
            untracked: false,
            version_id: row.version_id.clone(),
        }
    }
}

impl From<&StagedAdoptedStateRow> for StagedStateRow {
    fn from(row: &StagedAdoptedStateRow) -> Self {
        StagedStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            origin: None,
            schema_version: row.schema_version.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            global: row.global,
            change_id: Some(row.change_id.clone()),
            commit_id: Some(row.commit_id.clone()),
            untracked: false,
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

/// Transaction-local introduced-change membership accumulated while rows are staged.
///
/// Final commit row materialization owns commit ids, parent heads, and commit
/// row timestamps. Staging only tracks which hydrated tracked changes the
/// future commit introduces for a version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StagedCommitMembers {
    pub(crate) commit_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) change_set_id: String,
    pub(crate) created_at: String,
    pub(crate) change_ids: BTreeSet<String>,
    pub(crate) allow_empty: bool,
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
            allow_empty: false,
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

    pub(crate) fn allow_empty(&mut self) {
        self.allow_empty = true;
    }
}
