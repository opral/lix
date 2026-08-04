use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use bytes::Bytes;

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;

/// Root-independent tracked entity primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
}

/// Zero-copy view of primary tracked-state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
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

/// Physical location of an entity snapshot in immutable Arrow-native state.
///
/// The content digest is carried explicitly. Resolving a physical row must
/// never derive its storage identity from a commit and schema: commits point
/// at state, while immutable state is independently content addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateBaseCoordinate {
    pub(crate) state_set_id: crate::columnar_row_group::ArrowStateSetId,
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}

/// Payload-bearing immutable commit member.
///
/// Root mutation logic consumes only [`TrackedStateDeltaRef`]. Packed commit
/// storage additionally requires the complete authoritative payload; there is
/// no optional/non-authoritative representation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateCommitDeltaRef<'a> {
    pub(crate) delta: TrackedStateDeltaRef<'a>,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
    pub(crate) authored: bool,
}

/// Typed complete-replacement member produced directly by the transaction
/// journal's dominant one-column string identity lane. Invalid mutation
/// states (delete, selected-source payload, origin override) are deliberately
/// unrepresentable, so immutable replacement parts can be sealed without
/// constructing an `EntityPk` per row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateSingleStringReplacementRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a str,
    pub(crate) commit_id: CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
}

/// Value stored in tracked-state commit-root trees.
#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub(crate) fn deleted(&self) -> bool {
        self.deleted
    }
}

/// Zero-copy view of a tracked-state commit-root value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateIndexValueRef {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

/// Key bounds for one existing commit-addressed mutation segment.
///
/// Segment slot order is durable because directly addressable `ChangeId`s
/// encode that slot and the row ordinal. Snapshot compaction may replace the
/// optional root, but must never reorder these entries.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateMutationPart {
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_part: Option<StoredReplacementPart>,
}

/// One collection partition replaced by a certified immutable generation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitDeltaReplacementScope {
    pub(crate) schema_key: String,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitDeltaLifecycleSummary {
    pub(crate) scope: CommitDeltaReplacementScope,
    pub(crate) ordered_identity_digest: [u8; 32],
    pub(crate) uniform_created_at: LixTimestamp,
}

/// Durable certificate binding a replacement generation to its owner and
/// immutable replacement-part directory.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredCommitDeltaReplacementGeneration {
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) scope: CommitDeltaReplacementScope,
    pub(crate) integrity_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredReplacementPartsAuthority {
    pub(crate) directory_digest: [u8; 32],
    pub(crate) uniform_updated_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct StoredReplacementPart {
    /// Compact authored-event sidecar identity. It is not state authority.
    pub(crate) content_digest: [u8; 32],
    /// The immutable Arrow leaf that owns the post-image payload and current
    /// state for this exact key range.
    pub(crate) state_set_id: crate::columnar_row_group::ArrowStateSetId,
    pub(crate) state_group_index: u32,
    pub(crate) payload_refs_digest: [u8; 32],
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) first_address: u32,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// One immutable post-image range in a committed current-state partition.
///
/// This is deliberately distinct from [`CommitStateMutationPart`]. Mutation
/// parts describe what a commit authored; current-state parts describe the
/// strictly ordered, non-overlapping state that readers may serve directly.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStatePartDescriptor {
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    pub(crate) state_set_id: crate::columnar_row_group::ArrowStateSetId,
    pub(crate) state_group_index: u32,
    /// Digest of this Arrow leaf's compact JSON-reference summary.
    pub(crate) payload_refs_digest: [u8; 32],
    pub(crate) row_count: u16,
}

/// Content-addressed root of a persistent current-state range directory.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStatePartDirectoryRoot {
    pub(crate) root_id: [u8; 32],
    pub(crate) descriptor_digest: [u8; 32],
    pub(crate) row_count: u64,
    pub(crate) part_count: u32,
    pub(crate) tree_height: u16,
}

/// One authoritative current-state collection generation.
///
/// The mutation-directory digest binds this rebuildable serving projection to
/// the commit's historical replacement certificate without making the serving
/// directory part of commit semantics.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStatePartSet {
    pub(crate) scope: CommitDeltaReplacementScope,
    pub(crate) directory: CurrentStatePartDirectoryRoot,
}

/// Content-addressed root of the persistent collection-to-state-part catalog.
///
/// One root in each commit manifest replaces an O(collections) copied vector.
/// Unchanged commits reuse the exact root; updates rewrite only the bounded
/// radix path for affected collections.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStateCatalogRoot {
    pub(crate) root_id: [u8; 32],
    pub(crate) entry_count: u32,
    /// Root inherited from the sole parent before applying this commit. This
    /// is serving-layout lineage, not commit-graph ancestry.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) parent_root_id: Option<[u8; 32]>,
    /// Binds this commit, its inherited root, and the resulting content root.
    /// Event-sidecar placement is deliberately independent so state can be
    /// sealed before authored coordinates are finalized.
    pub(crate) transition_digest: [u8; 32],
}

/// Point-addressable immutable mutation inventory owned by one commit.
///
/// The fields intentionally mirror the existing commit-delta directory. This
/// lets the hard-cut manifest become authoritative without changing the
/// bounded LXCD15 identity/event-sidecar codec in the same step.
#[derive(Debug, Clone, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateMutationInventory {
    /// Certified whole-source alias authority. This is never used for a
    /// finite exact selection: when present, the selected source supplies the
    /// complete inherited state and authored members overlay it during replay.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) selected_source_commit_id: Option<[u8; 16]>,
    pub(crate) member_count: u32,
    pub(crate) selection_fingerprint: [u8; 32],
    /// Exact row counts for every directly addressable part. Empty means the
    /// generic locator-indexed layout.
    pub(crate) direct_part_row_counts: Vec<u16>,
    /// Compact physical identities for a complete replacement. Range bounds
    /// live in the rebuildable current-state directory; history can always
    /// recover them by decoding these immutable parts.
    pub(crate) replacement_part_digests: Vec<[u8; 32]>,
    /// Authoritative collection-replacement scope. A miss within this scope
    /// cannot fall through to an older first-parent generation.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_generation: Option<StoredCommitDeltaReplacementGeneration>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_parts: Option<StoredReplacementPartsAuthority>,
    /// Native post-image leaves already sealed by certified typed ingress.
    /// When the state parent has no entry for `single_partition`, these are
    /// published directly instead of replaying and re-encoding mutations.
    pub(crate) sealed_state_parts: Vec<CurrentStatePartDescriptor>,
    /// Tiny commits retain their only part inline so an exact history lookup
    /// remains one backend point read.
    #[musli(bytes)]
    pub(crate) inline_part: Vec<u8>,
    pub(crate) parts: Vec<CommitStateMutationPart>,
}

impl CommitStateMutationInventory {
    pub(crate) fn selected_source_commit_id(&self) -> Option<CommitId> {
        self.selected_source_commit_id
            .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes)))
    }

    pub(crate) fn part_count(&self) -> usize {
        usize::from(!self.inline_part.is_empty())
            + if self.replacement_part_digests.is_empty() {
                self.parts.len()
            } else {
                self.replacement_part_digests.len()
            }
    }
}

/// Single semantic authority for one tracked commit.
///
/// Compact topology projections, point locators, current-state HOT rows, and
/// snapshot tree chunks remain rebuildable serving indexes. None may carry
/// commit semantics absent from this manifest.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateManifest {
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) state_parent_commit_id: Option<CommitId>,
    pub(crate) commit_change_id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) created_at: LixTimestamp,
    pub(crate) mutations: CommitStateMutationInventory,
    pub(crate) current_state_catalog: Box<CurrentStateCatalogRoot>,
}

/// Materialized tracked-state commit-root row.
///
/// Tracked rows are the serving state that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: commit roots contain
/// tracked history only. Mutable untracked rows share the current-state
/// projection with tracked rows, but never enter a commit root or changelog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedTrackedStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
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
    pub(crate) encoded_key: Bytes,
    pub(crate) encoded_value: Bytes,
}

impl TrackedStateMutation {
    pub(crate) fn from_shared(encoded_key: Bytes, encoded_value: Bytes) -> Self {
        Self {
            encoded_key,
            encoded_value,
        }
    }
}

/// An encoded tracked-root mutation batch.
///
/// Every key is a slice of one immutable key arena and every value is a slice
/// of one immutable value arena. The row vector therefore carries descriptors
/// only; moving it through diff, merge, root planning, and tree materialization
/// never clones row-owned payload buffers.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TrackedStateMutationBatch {
    mutations: Vec<TrackedStateMutation>,
}

impl TrackedStateMutationBatch {
    pub(crate) fn from_shared(mutations: Vec<TrackedStateMutation>) -> Self {
        Self { mutations }
    }

    #[cfg(test)]
    pub(crate) fn as_slice(&self) -> &[TrackedStateMutation] {
        &self.mutations
    }

    pub(crate) fn into_mutations(self) -> Vec<TrackedStateMutation> {
        self.mutations
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStatePhysicalScanRequest {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl Default for TrackedStatePhysicalScanRequest {
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

impl TrackedStatePhysicalScanRequest {
    pub(crate) fn matches_ref(
        &self,
        key: TrackedStateKeyRef<'_>,
        value: &TrackedStateIndexValue,
    ) -> bool {
        if !self.include_tombstones && value.deleted {
            return false;
        }
        self.matches_key_ref(key)
    }

    pub(crate) fn matches_key_ref(&self, key: TrackedStateKeyRef<'_>) -> bool {
        if !self.schema_keys.is_empty()
            && !self
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == key.schema_key)
        {
            return false;
        }
        if !self.entity_pks.is_empty() && !self.entity_pks.contains(key.entity_pk) {
            return false;
        }
        if !self.file_ids.is_empty()
            && !self.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => key.file_id.is_none(),
                NullableKeyFilter::Value(value) => key.file_id == Some(value.as_str()),
            })
        {
            return false;
        }
        true
    }
}
