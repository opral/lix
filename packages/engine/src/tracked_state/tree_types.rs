use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::tracked_state::{MaterializedTrackedStateRow, TrackedStateRow};
use crate::{LixError, NullableKeyFilter};

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;

/// Content-addressed root id for one tracked-state tree tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateRootId([u8; TRACKED_STATE_HASH_BYTES]);

impl TrackedStateRootId {
    pub(crate) fn new(bytes: [u8; TRACKED_STATE_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Result<Self, LixError> {
        if bytes.len() != TRACKED_STATE_HASH_BYTES {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked-state tree root id must be {TRACKED_STATE_HASH_BYTES} bytes, got {}",
                    bytes.len()
                ),
            ));
        }
        let mut out = [0_u8; TRACKED_STATE_HASH_BYTES];
        out.copy_from_slice(bytes);
        Ok(Self(out))
    }

    pub(crate) fn as_bytes(&self) -> &[u8; TRACKED_STATE_HASH_BYTES] {
        &self.0
    }
}

/// Root-independent tracked entity identity.
///
/// Version ids intentionally do not appear in the key. Version refs select a
/// commit/root; the tree itself represents the tracked entities visible at that
/// commit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_id: EntityIdentity,
}

/// Borrowed primary tracked-state key.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_id: &'a EntityIdentity,
}

/// Tracked entity payload stored at a commit root.
///
/// This is deliberately the version-independent part of `TrackedStateRow`.
/// Callers project it back to `MaterializedTrackedStateRow` by supplying the version id
/// selected by the version ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateValue {
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) deleted: bool,
}

impl TrackedStateValue {
    pub(crate) fn into_materialized_row(
        self,
        key: TrackedStateKey,
        snapshot_content: Option<String>,
        metadata: Option<String>,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            snapshot_content,
            metadata,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            change_id: self.change_id,
            commit_id: self.commit_id,
        }
    }
}

/// Borrowed tracked-state value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateValueRef<'a> {
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: &'a str,
    pub(crate) updated_at: &'a str,
    pub(crate) change_id: &'a str,
    pub(crate) commit_id: &'a str,
    pub(crate) deleted: bool,
}

/// Borrowed tracked-state write row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateRowRef<'a> {
    pub(crate) key: TrackedStateKeyRef<'a>,
    pub(crate) value: TrackedStateValueRef<'a>,
}

impl TrackedStateRow {
    pub(crate) fn as_ref(&self) -> TrackedStateRowRef<'_> {
        TrackedStateRowRef {
            key: TrackedStateKeyRef {
                schema_key: &self.schema_key,
                file_id: self.file_id.as_deref(),
                entity_id: &self.entity_id,
            },
            value: TrackedStateValueRef {
                snapshot_ref: self.snapshot_ref.as_ref(),
                metadata_ref: self.metadata_ref.as_ref(),
                created_at: &self.created_at,
                updated_at: &self.updated_at,
                change_id: &self.change_id,
                commit_id: &self.commit_id,
                deleted: self.snapshot_ref.is_none(),
            },
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateTreeScanRequest {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) entity_ids: Vec<EntityIdentity>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl TrackedStateTreeScanRequest {
    pub(crate) fn matches(&self, key: &TrackedStateKey, value: &TrackedStateValue) -> bool {
        if !self.include_tombstones && value.deleted {
            return false;
        }
        if !self.schema_keys.is_empty() && !self.schema_keys.contains(&key.schema_key) {
            return false;
        }
        if !self.entity_ids.is_empty() && !self.entity_ids.contains(&key.entity_id) {
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
    pub(crate) persisted_root: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeDiffEntry {
    pub(crate) before: Option<(TrackedStateKey, TrackedStateValue)>,
    pub(crate) after: Option<(TrackedStateKey, TrackedStateValue)>,
}
