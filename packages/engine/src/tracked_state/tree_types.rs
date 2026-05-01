use crate::entity_identity::EntityIdentity;
use crate::tracked_state::TrackedStateRow;
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

impl TrackedStateKey {
    pub(crate) fn from_row(row: &TrackedStateRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            entity_id: row.entity_id.clone(),
        }
    }
}

/// Tracked entity payload stored at a commit root.
///
/// This is deliberately the version-independent part of `TrackedStateRow`.
/// Callers project it back to `TrackedStateRow` by supplying the version id
/// selected by the version ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateValue {
    pub(crate) snapshot: StoredSnapshot,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredSnapshot {
    Missing,
    Inline(String),
    Ref(SnapshotRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotRef {
    pub(crate) codec: SnapshotCodec,
    pub(crate) hash_hex: String,
    pub(crate) uncompressed_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotCodec {
    Raw,
    Zstd,
    JsonChunks,
}

impl TrackedStateValue {
    pub(crate) fn from_row(row: &TrackedStateRow) -> Self {
        Self {
            snapshot: row
                .snapshot_content
                .clone()
                .map(StoredSnapshot::Inline)
                .unwrap_or(StoredSnapshot::Missing),
            metadata: row.metadata.clone(),
            schema_version: row.schema_version.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            change_id: row.change_id.clone(),
            commit_id: row.commit_id.clone(),
            deleted: row.snapshot_content.is_none(),
        }
    }

    pub(crate) fn into_row(self, key: TrackedStateKey) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            snapshot_content: match self.snapshot {
                StoredSnapshot::Inline(snapshot_content) => Some(snapshot_content),
                StoredSnapshot::Missing | StoredSnapshot::Ref(_) => None,
            },
            metadata: self.metadata,
            schema_version: self.schema_version,
            created_at: self.created_at,
            updated_at: self.updated_at,
            change_id: self.change_id,
            commit_id: self.commit_id,
        }
    }

    pub(crate) fn without_snapshot_content(mut self) -> Self {
        self.snapshot = StoredSnapshot::Missing;
        self
    }

    #[cfg(test)]
    pub(crate) fn inline_snapshot_content(&self) -> Option<&str> {
        match &self.snapshot {
            StoredSnapshot::Inline(snapshot_content) => Some(snapshot_content.as_str()),
            StoredSnapshot::Missing | StoredSnapshot::Ref(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrackedStateMutation {
    Put {
        key: TrackedStateKey,
        value: TrackedStateValue,
    },
}

impl TrackedStateMutation {
    pub(crate) fn put(key: TrackedStateKey, value: TrackedStateValue) -> Self {
        Self::Put { key, value }
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
