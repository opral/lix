use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;

/// Content-addressed root id for one tracked-state commit-root tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, musli::Encode, musli::Decode)]
pub(crate) struct TrackedStateRootId(#[musli(bytes)] [u8; TRACKED_STATE_HASH_BYTES]);

impl TrackedStateRootId {
    pub(crate) fn new(bytes: [u8; TRACKED_STATE_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; TRACKED_STATE_HASH_BYTES] {
        &self.0
    }
}

/// Root-independent tracked entity primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
}

/// Zero-copy view of primary tracked-state key.
#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
}

#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
pub(crate) struct TrackedSchemaKeyPrefixRef<'a> {
    pub(crate) schema_key: &'a str,
}

#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
pub(crate) struct TrackedSchemaFilePrefixRef<'a> {
    pub(crate) schema_key: &'a str,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
}

/// Zero-copy tracked-state commit-root delta prepared from changelog facts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// Value stored in tracked-state commit-root trees.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateIndexValue {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

impl TrackedStateIndexValue {
    pub(crate) fn created_at(&self) -> LixTimestamp {
        self.created_at
    }

    pub(crate) fn updated_at(&self) -> LixTimestamp {
        self.updated_at
    }
}

/// Zero-copy view of a tracked-state commit-root value.
#[derive(Debug, Clone, Copy, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateIndexValueRef {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// Durable tracked-state root metadata for one commit.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateCommitRoot {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) parent_roots: Vec<TrackedStateCommitRootParent>,
    pub(crate) changed_key_count: u64,
    pub(crate) row_count_estimate: u64,
    pub(crate) tree_height: u32,
    pub(crate) primary_chunk_count: u64,
    pub(crate) primary_chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateCommitRootParent {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
}

/// Materialized tracked-state commit-root row.
///
/// Tracked rows are the serving state that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: untracked local overlay
/// data belongs to `untracked_state`, and the serving `live_state` facade is
/// responsible for combining both sources.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedTrackedStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
}

/// Identity-centered filter for tracked-state scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_pks: Vec<EntityPk>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

/// Requested property set for a tracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateReadColumns {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for tracked-state commit roots.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: TrackedStateFilter,
    #[serde(default)]
    pub(crate) read_columns: TrackedStateReadColumns,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TrackedStateMutation {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) encoded_value: Vec<u8>,
}

impl TrackedStateMutation {
    pub(crate) fn put_encoded(encoded_key: Vec<u8>, encoded_value: Vec<u8>) -> Self {
        Self {
            encoded_key,
            encoded_value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeScanRequest {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl Default for TrackedStateTreeScanRequest {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            entity_pks: Vec::new(),
            file_ids: Vec::new(),
            include_tombstones: true,
            limit: None,
        }
    }
}

impl TrackedStateTreeScanRequest {
    pub(crate) fn matches(&self, key: &TrackedStateKey, value: &TrackedStateIndexValue) -> bool {
        if !self.include_tombstones && value.deleted {
            return false;
        }
        self.matches_key(key)
    }

    pub(crate) fn matches_key(&self, key: &TrackedStateKey) -> bool {
        if !self.schema_keys.is_empty() && !self.schema_keys.contains(&key.schema_key) {
            return false;
        }
        if !self.entity_pks.is_empty() && !self.entity_pks.contains(&key.entity_pk) {
            return false;
        }
        if !self.file_ids.is_empty()
            && !self.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => key.file_id.is_none(),
                NullableKeyFilter::Value(value) => key.file_id.as_ref() == Some(value),
            })
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateApplyResult {
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) row_count: usize,
    pub(crate) tree_height: usize,
    pub(crate) chunk_count: usize,
    pub(crate) chunk_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeDiffEntry {
    pub(crate) before: Option<(TrackedStateKey, TrackedStateIndexValue)>,
    pub(crate) after: Option<(TrackedStateKey, TrackedStateIndexValue)>,
}
