use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;

/// One canonical live-state index mutation.
///
/// Tracked and untracked rows share the same identity and live-state tree.
/// A missing `commit_id` means the change is untracked; storage encodes that
/// case with the reserved nil commit id used by the tracked-state index codec.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LiveStateIndexDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// One cheap materialized header from a branch's live-state index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateIndexRow {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

impl LiveStateIndexRow {
    pub(crate) fn untracked(&self) -> bool {
        self.commit_id.is_none()
    }
}

/// Fully hydrated canonical live-state index row.
///
/// The index owns identity and change references. Snapshot and metadata JSON
/// are hydrated from the referenced changelog change in one batched read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedLiveStateIndexRow {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct LiveStateIndexFilter {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateIndexScanRequest {
    pub(crate) branch_id: String,
    pub(crate) filter: LiveStateIndexFilter,
    pub(crate) projection: Vec<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateIndexRowRequest {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
}
