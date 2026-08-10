mod descriptor_path;
mod keys;
mod path_index;
mod persistent_map;
mod planner;
mod read;
mod visibility;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::state::{StateRow, StateRowSource, UntrackedStateRow};

/// Filesystem-owned projection of one authenticated state cell. This is a
/// descriptor projection, not a generic state reader or a second storage
/// authority. The source row is always produced by a concrete state view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FilesystemStateRows(Vec<FilesystemStateRow>);

impl FilesystemStateRows {
    pub(crate) fn from_rows(rows: Vec<FilesystemStateRow>) -> Self {
        Self(rows)
    }

    pub(crate) fn from_view_rows(
        rows: Vec<StateRow>,
        branch_id: &str,
        untracked: bool,
    ) -> Result<Self, LixError> {
        rows.into_iter()
            .map(|row| FilesystemStateRow::from_state_row(row, branch_id, untracked))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    pub(crate) fn from_untracked_view_rows(rows: Vec<UntrackedStateRow>) -> Result<Self, LixError> {
        rows.into_iter()
            .map(FilesystemStateRow::from_untracked_state_row)
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn row(&self, index: usize) -> &FilesystemStateRow {
        &self.0[index]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &FilesystemStateRow> {
        self.0.iter()
    }

    fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

#[cfg(test)]
pub(crate) struct FilesystemStateRowsBuilder(Vec<FilesystemStateRow>);

#[cfg(test)]
impl FilesystemStateRowsBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub(crate) fn push_owned(&mut self, row: FilesystemStateRow) {
        self.0.push(row);
    }

    pub(crate) fn finish(self) -> FilesystemStateRows {
        FilesystemStateRows::from_rows(self.0)
    }
}

impl IntoIterator for FilesystemStateRows {
    type Item = FilesystemStateRow;
    type IntoIter = std::vec::IntoIter<FilesystemStateRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FilesystemStateRow {
    fn from_state_row(row: StateRow, branch_id: &str, untracked: bool) -> Result<Self, LixError> {
        let key = crate::forktree::decode_state_key(&row.key)?;
        let deleted = row.value.cell.deleted();
        let snapshot_content = match &row.value.cell {
            crate::forktree::StateCell::Value(value) => Some(value.clone()),
            crate::forktree::StateCell::Null | crate::forktree::StateCell::Tombstone => None,
        };
        Ok(Self {
            entity_pk: key.entity_pk,
            schema_key: key.schema_key,
            file_id: key.file_id,
            snapshot_content,
            metadata: row.value.metadata,
            deleted,
            created_at: row.value.created_at,
            updated_at: row.value.updated_at,
            global: row.source == StateRowSource::Global,
            change_id: Some(row.value.change_id),
            commit_id: Some(row.value.commit_id),
            untracked,
            branch_id: branch_id.to_owned(),
        })
    }

    fn from_untracked_state_row(row: UntrackedStateRow) -> Result<Self, LixError> {
        let snapshot_content = match &row.value.cell {
            crate::forktree::StateCell::Value(value) => Some(value.clone()),
            crate::forktree::StateCell::Null | crate::forktree::StateCell::Tombstone => None,
        };
        let global_branch = uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("GLOBAL_BRANCH_ID must be canonical");
        Ok(Self {
            entity_pk: row.key.entity_pk,
            schema_key: row.key.schema_key,
            file_id: row.key.file_id,
            snapshot_content,
            metadata: row.value.metadata,
            deleted: row.value.cell.deleted(),
            created_at: row.value.created_at,
            updated_at: row.value.updated_at,
            global: row.owner.as_bytes() == global_branch.as_bytes(),
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: uuid::Uuid::from_bytes(*row.owner.as_bytes()).to_string(),
        })
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.file_id.as_deref()
    }

    pub(crate) fn snapshot_content(&self) -> Option<&SharedStr> {
        self.snapshot_content.as_ref()
    }

    pub(crate) fn metadata(&self) -> Option<&SharedStr> {
        self.metadata.as_ref()
    }

    pub(crate) fn created_at(&self) -> &LixTimestamp {
        &self.created_at
    }

    pub(crate) fn updated_at(&self) -> &LixTimestamp {
        &self.updated_at
    }

    pub(crate) fn change_id(&self) -> Option<ChangeId> {
        self.change_id
    }

    pub(crate) fn commit_id(&self) -> Option<CommitId> {
        self.commit_id
    }

    pub(crate) fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub(crate) fn global(&self) -> bool {
        self.global
    }

    pub(crate) fn untracked(&self) -> bool {
        self.untracked
    }

    pub(crate) fn deleted(&self) -> bool {
        self.deleted
    }

    pub(crate) fn entity_pk(&self) -> &EntityPk {
        &self.entity_pk
    }
}

pub(crate) use self::descriptor_path::{DirectoryPathRecord, derive_directory_paths};
pub(crate) use self::path_index::{
    FilesystemPathEntry, FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemPathKind, FilesystemPathSelection,
    ForkTreeFilesystemPathIndexReader, build_path_index,
};
pub(crate) use self::persistent_map::{PersistentMap, PersistentMapRangeCursor};
pub(crate) use self::planner::directory_path_resolvers_from_state_batch;
#[allow(unused_imports)]
pub(crate) use self::planner::{
    BlobRefPluginCheckpoint, BlobRefRowInput, DirectoryDescriptorWriteIntent,
    DirectoryPathResolver, FileDeleteInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
    FilesystemBlobRefKey, FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    FilesystemWritePlan, append_blob_ref_tombstone_row,
    create_directory_path_with_leaf_id_with_resolvers, directory_path_resolvers_from_path_index,
    directory_path_resolvers_from_state_view, filesystem_storage_scope_key, plan_file_delete,
    plan_file_descriptor_write, plan_parsed_directory_path_update_with_resolvers,
    plan_parsed_file_path_update_with_resolvers, plan_parsed_file_path_write_with_resolvers,
    plan_recursive_directory_delete,
};
pub(crate) use self::read::{FilesystemIndex, filesystem_schema_keys};
pub(crate) use self::visibility::VisibleFilesystem;
