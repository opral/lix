use crate::changelog::{ChangeLocator, ChangeLocatorRef, ChangeRef};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::{LixError, NullableKeyFilter};

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;

/// Content-addressed root id for one tracked-state projection tree.
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_id: EntityIdentity,
}

/// Zero-copy view of primary tracked-state key.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_id: &'a EntityIdentity,
}

/// Zero-copy tracked-state projection delta prepared from changelog facts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateDeltaRef<'a> {
    pub(crate) change: ChangeRef<'a>,
    pub(crate) locator: ChangeLocatorRef<'a>,
    pub(crate) created_at: &'a str,
    pub(crate) updated_at: &'a str,
}

/// Projection value stored in tracked-state trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateIndexValue {
    pub(crate) change_locator: ChangeLocator,
    pub(crate) deleted: bool,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

/// Zero-copy view of a tracked-state projection value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateIndexValueRef<'a> {
    pub(crate) change_locator: ChangeLocatorRef<'a>,
    pub(crate) deleted: bool,
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: &'a str,
    pub(crate) updated_at: &'a str,
}

/// Durable metadata for the tracked-state projection at one commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateProjectionMetadata {
    pub(crate) commit_id: String,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) parent_roots: Vec<TrackedStateProjectionParent>,
    pub(crate) changed_key_count: u64,
    pub(crate) row_count_estimate: u64,
    pub(crate) tree_height: u32,
    pub(crate) primary_chunk_count: u64,
    pub(crate) primary_chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateProjectionParent {
    pub(crate) commit_id: String,
    pub(crate) root_id: TrackedStateRootId,
}

/// Materialized tracked-state projection row.
///
/// Tracked rows are the projection that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: untracked local overlay
/// data belongs to `untracked_state`, and the serving `live_state` facade is
/// responsible for combining both sources.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedTrackedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
}

/// Identity-centered filter for tracked-state scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_ids: Vec<EntityIdentity>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

/// Requested property set for a tracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for the tracked-state projection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: TrackedStateFilter,
    #[serde(default)]
    pub(crate) projection: TrackedStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one tracked-state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: NullableKeyFilter<String>,
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
    pub(crate) entity_ids: Vec<EntityIdentity>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl Default for TrackedStateTreeScanRequest {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            entity_ids: Vec::new(),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeDiffEntry {
    pub(crate) before: Option<(TrackedStateKey, TrackedStateIndexValue)>,
    pub(crate) after: Option<(TrackedStateKey, TrackedStateIndexValue)>,
}
