use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::row_payload::TypedRow as WasmTypedRow;
use crate::row_pk::RowPk;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) const TRACKED_STATE_HASH_BYTES: usize = 32;
pub(crate) const COMMIT_STATE_MAX_REPLAY_DEPTH: u16 = 32;
pub(crate) const COMMIT_STATE_MAX_REPLAY_BYTES: u64 = 256 * 1024 * 1024;

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

/// Root-independent tracked row primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) row_pk: RowPk,
}

/// Zero-copy view of primary tracked-state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TrackedStateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
}

/// Zero-copy tracked-state commit-root delta prepared from changelog facts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    /// Account that performed this row's latest write, retained independently
    /// of changelog payload availability.
    pub(crate) author_id: &'a str,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    /// Authenticated canonical `(snapshot, metadata)` identity for locally
    /// authored live rows. `None` deliberately keeps legacy and selected
    /// references on the exact payload-validation path.
    pub(crate) semantic_fingerprint: Option<[u8; 32]>,
}

/// Physical location of a row snapshot in an immutable columnar base.
///
/// Commit deltas carry this coordinate alongside their authoritative payload,
/// allowing exact identity lookups to reconcile an overlay row with its base
/// row without reading a second index. The commit id owns the referenced base
/// layout; group and row ordinals are intentionally fixed-width so the packed
/// commit-delta sidecar remains compact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateBaseCoordinate {
    pub(crate) base_commit_id: CommitId,
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
    pub(crate) metadata: Option<&'a lix_schema::Jsonb>,
    /// Canonical native snapshot bytes. `Some` is a live authored row and
    /// `None` is a tombstone or a payload selected from existing authority.
    pub(crate) snapshot: Option<&'a [u8]>,
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) base_coordinate: Option<TrackedStateBaseCoordinate>,
    pub(crate) authored: bool,
}

/// Typed complete-replacement member produced directly by the transaction
/// journal's dominant one-column string identity lane. Invalid mutation
/// states (delete, selected-source payload, origin override) are deliberately
/// unrepresentable, so immutable replacement parts can be sealed without
/// constructing an `RowPk` per row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateSingleStringReplacementRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a str,
    pub(crate) author_id: &'a str,
    pub(crate) commit_id: CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) metadata: Option<&'a lix_schema::Jsonb>,
    pub(crate) snapshot: &'a [u8],
}

/// One ordered tracked-root mutation with its insert-collision contract.
///
/// Bulk commit assembly keeps this zero-copy form until it has compared the
/// mutation with the parent leaf. That lets the common full-batch path retain
/// only one incoming key/value at a time instead of a second `Vec` plus a
/// cloned absence-guard set.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedStateRootMutationRef<'a> {
    pub(crate) delta: TrackedStateDeltaRef<'a>,
    pub(crate) require_absence: bool,
}

/// Value stored in tracked-state commit-root trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateIndexValue {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) author_id: String,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    /// Hash of the canonical live payload, when it was sealed by an authored
    /// root/delta publication. Tombstones, selected references, and older
    /// formats use `None` and require payload loading for semantic equality.
    pub(crate) semantic_fingerprint: Option<[u8; 32]>,
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
pub(crate) struct TrackedStateIndexValueRef<'a> {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) author_id: &'a str,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) semantic_fingerprint: Option<[u8; 32]>,
}

/// Computes a stable semantic identity for one live tracked-state payload.
/// Durable payload envelopes can encode the same typed row in different ways
/// (including compact built-in and native plugin encodings). Decode before
/// hashing a single canonical typed representation so sync hydration and
/// current-state serving layouts agree. Typed encoding distinguishes SQL NULL
/// from JSON null; metadata uses canonical JSONB binary encoding.
///
/// Protocol-v69 storage payloads whose row frame is already canonical are
/// hashed directly: the canonical representation is then the identity frames
/// derived from the storage envelope followed by the stored row frame, so the
/// decode and re-encode would reproduce the same bytes.
pub(crate) fn tracked_payload_semantic_fingerprint(
    schema_key: &str,
    row_pk: &RowPk,
    snapshot: Option<&[u8]>,
    metadata: Option<&lix_schema::Jsonb>,
) -> Result<Option<[u8; 32]>, crate::LixError> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let mut hasher = SEMANTIC_FINGERPRINT_HASHER.clone();
    hasher.update(&[1]);
    hasher.update(&(schema_key.len() as u64).to_be_bytes());
    hasher.update(schema_key.as_bytes());
    if !hash_verbatim_canonical_snapshot(&mut hasher, row_pk, snapshot) {
        let canonical_snapshot = decoded_canonical_snapshot(schema_key, row_pk, snapshot)?;
        hasher.update(&(canonical_snapshot.len() as u64).to_be_bytes());
        hasher.update(&canonical_snapshot);
    }
    let metadata = metadata
        .map(lix_schema::Jsonb::binary)
        .transpose()
        .map_err(|error| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                format!("tracked-state metadata failed canonical encoding: {error}"),
            )
        })?;
    match metadata.as_ref() {
        Some(bytes) => {
            hasher.update(&[1]);
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        None => {
            hasher.update(&[0]);
        }
    };
    Ok(Some(*hasher.finalize().as_bytes()))
}

/// Key derivation hashes the context string; do it once and clone the state.
static SEMANTIC_FINGERPRINT_HASHER: std::sync::LazyLock<blake3::Hasher> =
    std::sync::LazyLock::new(|| {
        blake3::Hasher::new_derive_key("lix.tracked-state.payload-semantic-fingerprint.v2")
    });

/// Reference canonicalization: decode the durable payload and re-encode it as
/// a self-contained native typed row.
fn decoded_canonical_snapshot(
    schema_key: &str,
    row_pk: &RowPk,
    snapshot: &[u8],
) -> Result<Vec<u8>, crate::LixError> {
    let typed =
        WasmTypedRow::decode_durable_payload(Arc::<[u8]>::from(snapshot), schema_key, row_pk)?;
    crate::plugin::wire::typed::encode_native_row_payload_with_identity(
        &typed.schema_fingerprint,
        &typed.row_pk,
        &typed.row,
    )
    .map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INTERNAL_ERROR,
            format!("tracked-state typed payload failed canonical encoding: {error:?}"),
        )
    })
}

/// Hashes the length-prefixed canonical snapshot without decoding it when the
/// stored payload is a protocol-v69 storage row whose frame is provably
/// verbatim-canonical. Returns `false`, having fed nothing to `hasher`, when
/// the payload needs the reference decode/re-encode path.
fn hash_verbatim_canonical_snapshot(
    hasher: &mut blake3::Hasher,
    row_pk: &RowPk,
    snapshot: &[u8],
) -> bool {
    use crate::plugin::wire::typed::{
        BorrowedNativeValue, NATIVE_IDENTITY_MAX_KEY_COMPONENTS, NATIVE_IDENTITY_PAYLOAD_MAX_BYTES,
        NATIVE_IDENTITY_PAYLOAD_VERSION, canonical_identity_key_frame,
        verbatim_canonical_storage_row_frame,
    };
    use crate::row_pk::RowPkComponent;

    let components = row_pk.components.as_slice();
    let key_count = components.len();
    if key_count == 0 || key_count > NATIVE_IDENTITY_MAX_KEY_COMPONENTS {
        return false;
    }
    fn key_value(component: &RowPkComponent) -> Option<BorrowedNativeValue<'_>> {
        Some(match component {
            RowPkComponent::String(value) => BorrowedNativeValue::Text(value.as_str()),
            RowPkComponent::Uuid(value) => BorrowedNativeValue::Uuid(uuid::Uuid::from_bytes(*value)),
            RowPkComponent::Integer(value) => BorrowedNativeValue::Int8(*value),
            RowPkComponent::Bytes(_) => return None,
        })
    }
    // Measure the identity frames first so nothing reaches the hasher unless
    // the whole canonical snapshot can be streamed.
    let mut key_bytes = 4usize;
    for component in components {
        let Some(value) = key_value(component) else {
            return false;
        };
        if !canonical_identity_key_frame(value, |bytes| key_bytes += bytes.len()) {
            return false;
        }
    }
    let Some((schema_fingerprint, row_frame)) = verbatim_canonical_storage_row_frame(snapshot)
    else {
        return false;
    };
    let canonical_len = 1 + 32 + key_bytes + 4 + row_frame.len();
    if canonical_len > NATIVE_IDENTITY_PAYLOAD_MAX_BYTES {
        return false;
    }
    hasher.update(&(canonical_len as u64).to_be_bytes());
    hasher.update(&[NATIVE_IDENTITY_PAYLOAD_VERSION]);
    hasher.update(&schema_fingerprint);
    hasher.update(&(key_count as u32).to_be_bytes());
    for component in components {
        let value = key_value(component).expect("identity component measured above");
        canonical_identity_key_frame(value, |bytes| {
            hasher.update(bytes);
        });
    }
    hasher.update(&(row_frame.len() as u32).to_be_bytes());
    hasher.update(row_frame);
    true
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
    /// Certifies that this root contains the commit's complete logical state
    /// even though its semantic first parent is not retained as physical tree
    /// ancestry. Ordinary roots must leave this false.
    pub(crate) complete_state_fence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct TrackedStateCommitRootParent {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
}

/// Bounded first-parent replay work carried by a commit's canonical mutation
/// interval.
///
/// Zero debt means the manifest's snapshot root is the canonical serving
/// layout. Nonzero debt means readers reconstruct state from the bounded
/// interval and the manifest must not publish a snapshot root.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateReplayDebt {
    pub(crate) depth: u16,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
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
    pub(crate) content_digest: [u8; 32],
    #[musli(with = crate::storage_codec::option)]
    pub(crate) replacement_part: Option<StoredReplacementPart>,
}

/// One lossless row-columnar generation used directly as authored history.
///
/// The row-group manifest binds every column digest. Uniform lifecycle and
/// origin metadata remain in the commit authority instead of being repeated
/// in every row. Direct change addresses retain their established 512-row
/// logical slots and translate to these larger physical groups by ordinal.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ColumnarMutationPartSet {
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) row_group_set_id: [u8; 16],
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) schema_key: String,
    pub(crate) author_id: String,
    pub(crate) row_count: u32,
    pub(crate) group_row_counts: Vec<u32>,
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    pub(crate) page_first_keys: Vec<Vec<u8>>,
    pub(crate) page_last_keys: Vec<Vec<u8>>,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) origin_key: Option<String>,
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
    #[musli(with = crate::storage_codec::option)]
    pub(crate) fallback_commit_id: Option<[u8; 16]>,
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
    pub(crate) content_digest: [u8; 32],
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
    pub(crate) content_digest: [u8; 32],
    /// Which physical source serves this range, plus that source's own
    /// addressing fields.
    pub(crate) source: CurrentStatePartSource,
    /// First physical row selected from the source part. Descriptor slicing
    /// allows sparse deletes and updates to retain untouched source bytes.
    pub(crate) source_row_offset: u16,
    pub(crate) row_count: u16,
    /// True only for structural slices and authored islands introduced by a
    /// sparse rewrite. Canonical encodes clear this bit, making compaction
    /// self-stabilizing without guessing from physical row density.
    pub(crate) fragmented: bool,
}

/// Physical source of one current-state part, with the addressing fields that
/// source actually uses.
///
/// This was previously a `source_kind: u8` discriminator beside the union of
/// every kind's fields, so each locator carried the other kinds' fields pinned
/// to zero and a hand-written validator re-proved that pinning on every
/// decode. Per-variant fields make those combinations unrepresentable instead
/// of merely rejected, and stop the unused fields from being encoded at all.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
pub(crate) enum CurrentStatePartSource {
    /// An immutable complete-replacement mutation part owned by one commit.
    Replacement(ReplacementPartSource),
    /// A native content-addressed current-state data part. The part's own rows
    /// carry per-row authorship, so the locator needs no additional addressing
    /// fields beyond the descriptor's content digest.
    NativeDataPart,
    /// One authenticated page in a canonical row-group set.
    ColumnarPage(ColumnarPageSource),
}

/// Addressing for a replacement-part source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ReplacementPartSource {
    pub(crate) owner_commit_id: [u8; 16],
    /// Replacement segment index within the owner commit.
    pub(crate) part_index: u32,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// Addressing for one page of a canonical row-group set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ColumnarPageSource {
    /// Physical immutable row-group-set id.
    pub(crate) source_id: [u8; 16],
    pub(crate) owner_commit_id: [u8; 16],
    pub(crate) author_id: String,
    /// Row-group index within the set.
    pub(crate) part_index: u32,
    /// Page index inside `part_index`.
    pub(crate) source_page_index: u16,
    pub(crate) uniform_created_at: LixTimestamp,
    pub(crate) uniform_updated_at: LixTimestamp,
}

/// Manifest-attested root of the unified scope/part serving tree.
///
/// The generic tree owns only authenticated physical routing. These fields
/// bind one result root to the physical serving base and certified mutation
/// authority that produced it. Graph ancestry remains an independent semantic
/// relationship and is not exposed to the tree.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CurrentStateScopedRangeRoot {
    pub(crate) tree: super::scoped_range::ScopedRangeRoot,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) serving_base_commit_id: Option<CommitId>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) serving_base_root_id: Option<[u8; 32]>,
    pub(crate) transition_digest: [u8; 32],
}

/// Cumulative negative-membership certificate for collection scopes.
///
/// A complete filter may have false positives, but never false negatives: a
/// missing schema-family bit therefore proves that no effective graph-parent
/// or selected-source lineage authored any scope for that schema. Coarsening
/// file-scoped collections to their schema avoids cardinality-driven
/// saturation while remaining conservative. Incomplete filters fail closed
/// and carry no bits.
#[derive(Debug, Clone, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateTouchedScopeFilter {
    pub(crate) complete: bool,
    #[musli(bytes)]
    pub(crate) bits: Vec<u8>,
}

/// Point-addressable immutable mutation inventory owned by one commit.
///
/// The fields intentionally mirror the existing commit-delta directory. This
/// lets the hard-cut manifest become authoritative without changing the
/// bounded LXCD16 segment and payload-sidecar codec in the same step.
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
    /// Exact physical row counts for every part that participates in direct
    /// addressing. Empty means the generic locator-indexed layout.
    pub(crate) direct_part_row_counts: Vec<u16>,
    /// Authenticated direct ownership, one little-endian bitset per direct
    /// part. A set bit means the row at that physical ordinal owns the
    /// derivable ChangeId; a clear bit is an authored row that must use its
    /// explicit locator. This keeps mixed typed imports point-addressable
    /// without treating an embedded-id mismatch as fallback authority.
    pub(crate) direct_part_ownership: Vec<Vec<u8>>,
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
    /// Lossless typed post-images that are both the authored mutation payload
    /// and the serving columnar source. When present, legacy LXCD parts are
    /// forbidden rather than retained as a compatibility copy.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) columnar_parts: Option<ColumnarMutationPartSet>,
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
        self.columnar_parts
            .as_ref()
            .map_or(0, |parts| parts.group_row_counts.len())
            + usize::from(!self.inline_part.is_empty())
            + if self.replacement_part_digests.is_empty() {
                self.parts.len()
            } else {
                self.replacement_part_digests.len()
            }
    }

    pub(crate) fn direct_coordinate_owned(
        &self,
        part_index: usize,
        local_row: u16,
    ) -> Option<bool> {
        if let Some(&row_count) = self.direct_part_row_counts.get(part_index) {
            if local_row >= row_count {
                return None;
            }
        }
        let ownership = self.direct_part_ownership.get(part_index)?;
        let ordinal = usize::from(local_row);
        Some(*ownership.get(ordinal / 8)? & (1 << (ordinal % 8)) != 0)
    }

    pub(crate) fn direct_addresses_are_fully_owned(&self) -> bool {
        !self.direct_part_row_counts.is_empty()
            && self
                .direct_part_row_counts
                .iter()
                .enumerate()
                .all(|(part_index, &row_count)| {
                    (0..row_count).all(|local_row| {
                        self.direct_coordinate_owned(part_index, local_row) == Some(true)
                    })
                })
    }

    /// Whether this local inventory can contain a finite selected member whose
    /// payload owner must be resolved through the canonical change locator.
    ///
    /// A selected-source alias is whole-source authority, not a finite
    /// selection. A fully-owned direct-address inventory and columnar parts
    /// are authored history by contract. Mixed/generic layouts require
    /// decoding local members to distinguish authored rows from finite
    /// selections.
    pub(crate) fn may_contain_finite_selected_members(&self) -> bool {
        self.member_count != 0
            && self.selected_source_commit_id.is_none()
            && !self.direct_addresses_are_fully_owned()
            && self.columnar_parts.is_none()
    }
}

/// Native full-state publication provenance, independent of causal parents and
/// selected commit membership. Legacy uncertainty must not certify exclusion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
pub(crate) enum CommitStateIncorporation {
    #[default]
    None,
    Complete(CommitId),
    LegacyUnknown,
}

/// Immutable physical authority for one tracked commit. Causal commit facts
/// remain owned by `changelog.commit`; incorporation records a certified native
/// state source without changing those parents or public commit membership.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitStateManifest {
    pub(crate) commit_id: CommitId,
    pub(crate) incorporation: CommitStateIncorporation,
    /// Physical decode dictionary for authored mutation rows. This is not
    /// commit-account authority: it remains with retained immutable payloads
    /// even if GC removes the semantic commit projection.
    pub(crate) change_account_id: String,
    pub(crate) replay_debt: CommitStateReplayDebt,
    pub(crate) mutations: CommitStateMutationInventory,
    pub(crate) touched_scope_filter: CommitStateTouchedScopeFilter,
    /// Immutable branch scope in which this commit was authored.
    pub(crate) global_scope: bool,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) current_state_scoped_ranges: Option<Box<CurrentStateScopedRangeRoot>>,
    /// Monotonic identity catalog ordered by `(schema_key, row_pk, file_id)`.
    ///
    /// The catalog is published for every commit, including rootless replay
    /// commits. Entries may outlive a row deletion; readers resolve each
    /// candidate against canonical point-in-time state before returning it.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) row_pk_index_root_id: Option<TrackedStateRootId>,
    /// Canonical snapshot metadata when this commit was published as a root
    /// fence. The tree chunks are rebuildable by content hash; this immutable
    /// pointer is the authority that permits readers to serve them.
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_root: Option<Box<TrackedStateCommitRoot>>,
}

/// Materialized tracked-state commit-root row.
///
/// Tracked rows are the serving state that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: commit roots contain
/// tracked history only. Mutable untracked rows share the current-state
/// projection with tracked rows, but never enter a commit root or changelog.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedTrackedStateRow {
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    #[serde(skip)]
    pub(crate) decoded_snapshot: Option<Arc<WasmTypedRow>>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) author_id: String,
}

/// Identity-centered filter for tracked-state scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) row_pks: Vec<RowPk>,
    #[serde(default)]
    pub(crate) row_pk_lower: Option<RowPkRangeBound>,
    #[serde(default)]
    pub(crate) row_pk_upper: Option<RowPkRangeBound>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

/// One canonical bound over the typed primary-key ordering.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowPkRangeBound {
    pub(crate) row_pk: RowPk,
    pub(crate) inclusive: bool,
}

impl TrackedStateFilter {
    pub(crate) fn matches_row_pk(&self, row_pk: &RowPk) -> bool {
        (self.row_pks.is_empty() || self.row_pks.contains(row_pk))
            && row_pk_satisfies_bounds(
                row_pk,
                self.row_pk_lower.as_ref(),
                self.row_pk_upper.as_ref(),
            )
    }

    /// [`Self::matches_row_pk`] against a membership lookup built once per
    /// scan from `self.row_pks`.
    pub(crate) fn matches_row_pk_with(&self, row_pks: &RowPkLookup<'_>, row_pk: &RowPk) -> bool {
        row_pks.contains(row_pk)
            && row_pk_satisfies_bounds(
                row_pk,
                self.row_pk_lower.as_ref(),
                self.row_pk_upper.as_ref(),
            )
    }
}

/// Membership test over a filter's requested row identities.
///
/// A filter usually names a handful of identities, but a declared-column
/// index probe (`WHERE fk IN (...)`) resolves to one identity per matching
/// row. Probing that list linearly for every row a scan visits is quadratic,
/// so scans build one lookup up front and reuse it per row.
pub(crate) enum RowPkLookup<'a> {
    /// No identity constraint: every row matches.
    Any,
    Few(&'a [RowPk]),
    Many(std::collections::HashSet<&'a RowPk>),
}

impl<'a> RowPkLookup<'a> {
    /// Beyond this many identities a hash set beats a linear probe.
    const LINEAR_MAX: usize = 16;

    pub(crate) fn new(row_pks: &'a [RowPk]) -> Self {
        if row_pks.is_empty() {
            Self::Any
        } else if row_pks.len() <= Self::LINEAR_MAX {
            Self::Few(row_pks)
        } else {
            Self::Many(row_pks.iter().collect())
        }
    }

    pub(crate) fn contains(&self, row_pk: &RowPk) -> bool {
        match self {
            Self::Any => true,
            Self::Few(row_pks) => row_pks.contains(row_pk),
            Self::Many(row_pks) => row_pks.contains(row_pk),
        }
    }
}

pub(crate) fn row_pk_satisfies_bounds(
    row_pk: &RowPk,
    lower: Option<&RowPkRangeBound>,
    upper: Option<&RowPkRangeBound>,
) -> bool {
    lower.is_none_or(|bound| row_pk > &bound.row_pk || (bound.inclusive && row_pk == &bound.row_pk))
        && upper.is_none_or(|bound| {
            row_pk < &bound.row_pk || (bound.inclusive && row_pk == &bound.row_pk)
        })
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

impl TrackedStateScanRequest {
    pub(crate) fn is_catalog_identity_only(&self) -> bool {
        self.filter.schema_keys.len() == 1
            && self.filter.schema_keys[0] == "lix_registered_schema"
            && self.filter.row_pks.is_empty()
            && self.filter.row_pk_lower.is_none()
            && self.filter.row_pk_upper.is_none()
            && self.filter.file_ids.len() == 1
            && matches!(&self.filter.file_ids[0], NullableKeyFilter::Null)
            && self.read_columns.columns.len() == 1
            && self.read_columns.columns[0] == "row_pk"
            && self.limit.is_none()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TrackedStateMutation {
    pub(crate) encoded_key: Bytes,
    pub(crate) encoded_value: Bytes,
}

impl TrackedStateMutation {
    #[cfg(test)]
    pub(crate) fn put_encoded(encoded_key: Vec<u8>, encoded_value: Vec<u8>) -> Self {
        Self {
            encoded_key: Bytes::from(encoded_key),
            encoded_value: Bytes::from(encoded_value),
        }
    }

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

    pub(crate) fn len(&self) -> usize {
        self.mutations.len()
    }

    pub(crate) fn first_encoded_key(&self) -> Option<&[u8]> {
        self.mutations
            .first()
            .map(|mutation| mutation.encoded_key.as_ref())
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
pub(crate) struct TrackedStateTreeScanRequest {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) row_pks: Vec<RowPk>,
    pub(crate) row_pk_lower: Option<RowPkRangeBound>,
    pub(crate) row_pk_upper: Option<RowPkRangeBound>,
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

impl Default for TrackedStateTreeScanRequest {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            row_pks: Vec::new(),
            row_pk_lower: None,
            row_pk_upper: None,
            file_ids: Vec::new(),
            include_tombstones: true,
            limit: None,
        }
    }
}

impl TrackedStateTreeScanRequest {
    pub(crate) fn matches(&self, key: &TrackedStateKey, value: &TrackedStateIndexValue) -> bool {
        self.matches_ref(
            TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            },
            value,
        )
    }

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
        if !self.row_pks.is_empty() && !self.row_pks.contains(key.row_pk) {
            return false;
        }
        if !row_pk_satisfies_bounds(
            key.row_pk,
            self.row_pk_lower.as_ref(),
            self.row_pk_upper.as_ref(),
        ) {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateApplyResult {
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) row_count: usize,
    pub(crate) tree_height: usize,
    pub(crate) chunk_count: usize,
    pub(crate) chunk_bytes: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeDiffEntry {
    /// Identity column shared by both sides of a modified row.
    ///
    /// Tree ordering already proves that a modified entry has the same
    /// encoded key on both sides. Keeping one decoded key avoids decoding and
    /// allocating the schema/file/row identity twice before diff and merge
    /// immediately re-share it.
    pub(crate) key: TrackedStateKey,
    pub(crate) before: Option<TrackedStateIndexValue>,
    pub(crate) after: Option<TrackedStateIndexValue>,
}

#[cfg(test)]
mod semantic_fingerprint_tests {
    use super::tracked_payload_semantic_fingerprint;
    use crate::row_pk::RowPk;
    use lix_schema::Jsonb;
    use std::sync::Arc;

    #[test]
    fn fingerprint_is_canonical_and_covers_snapshot_and_metadata() {
        let row_pk = RowPk::single("fingerprint-row");
        let first_snapshot = crate::row_payload::TypedRow::from_test_json_unchecked(
            &row_pk,
            &serde_json::json!({"body":"first"}),
        )
        .unwrap()
        .durable_payload()
        .unwrap();
        let second_snapshot = crate::row_payload::TypedRow::from_test_json_unchecked(
            &row_pk,
            &serde_json::json!({"body":"second"}),
        )
        .unwrap()
        .durable_payload()
        .unwrap();
        let first_metadata = Jsonb::from_value(serde_json::json!({"b": 2, "a": 1}));
        let canonical_binary = first_metadata
            .binary()
            .expect("JSONB should encode canonically")
            .into_owned();
        let second_metadata = Jsonb::from_binary(Arc::<[u8]>::from(canonical_binary))
            .expect("canonical JSONB should decode");

        let first = tracked_payload_semantic_fingerprint(
            "fingerprint_test", &row_pk, Some(&first_snapshot),
            Some(&first_metadata),
        )
        .expect("fingerprint should compute");
        assert_eq!(
            first,
            tracked_payload_semantic_fingerprint(
                "fingerprint_test", &row_pk, Some(&first_snapshot),
                Some(&second_metadata),
            )
            .expect("equivalent JSONB should fingerprint")
        );
        assert_ne!(
            first,
            tracked_payload_semantic_fingerprint(
                "fingerprint_test", &row_pk, Some(&second_snapshot),
                Some(&first_metadata)
            )
            .expect("changed snapshot should fingerprint")
        );
        assert_ne!(
            first,
            tracked_payload_semantic_fingerprint("fingerprint_test", &row_pk, Some(&first_snapshot), None)
                .expect("missing metadata should fingerprint")
        );
        assert_ne!(
            tracked_payload_semantic_fingerprint("fingerprint_test", &row_pk, Some(&first_snapshot), None)
                .expect("SQL NULL metadata should fingerprint"),
            tracked_payload_semantic_fingerprint(
                "fingerprint_test", &row_pk, Some(&first_snapshot),
                Some(&Jsonb::from_value(serde_json::Value::Null)),
            )
            .expect("JSON null metadata should fingerprint")
        );
        assert_eq!(
            tracked_payload_semantic_fingerprint("fingerprint_test", &row_pk, None, Some(&first_metadata))
                .expect("tombstone fingerprint should be absent"),
            None
        );
    }

    /// The fingerprint algorithm before the verbatim fast path existed:
    /// decode, re-encode with identity, hash. Persisted fingerprints were
    /// produced by exactly this code, so every payload must still match it.
    fn reference_fingerprint(
        schema_key: &str,
        row_pk: &RowPk,
        snapshot: Option<&[u8]>,
        metadata: Option<&Jsonb>,
    ) -> Option<[u8; 32]> {
        let snapshot = snapshot?;
        let typed = crate::row_payload::TypedRow::decode_durable_payload(
            Arc::<[u8]>::from(snapshot),
            schema_key,
            row_pk,
        )
        .expect("reference decode");
        let canonical_snapshot = crate::plugin::wire::typed::encode_native_row_payload_with_identity(
            &typed.schema_fingerprint,
            &typed.row_pk,
            &typed.row,
        )
        .expect("reference encode");
        let metadata = metadata.map(|metadata| metadata.binary().expect("metadata binary"));
        let mut hasher =
            blake3::Hasher::new_derive_key("lix.tracked-state.payload-semantic-fingerprint.v2");
        hasher.update(&[1]);
        hasher.update(&(schema_key.len() as u64).to_be_bytes());
        hasher.update(schema_key.as_bytes());
        hasher.update(&(canonical_snapshot.len() as u64).to_be_bytes());
        hasher.update(&canonical_snapshot);
        match metadata.as_ref() {
            Some(bytes) => {
                hasher.update(&[1]);
                hasher.update(&(bytes.len() as u64).to_be_bytes());
                hasher.update(bytes);
            }
            None => {
                hasher.update(&[0]);
            }
        }
        Some(*hasher.finalize().as_bytes())
    }

    fn takes_verbatim_path(row_pk: &RowPk, snapshot: &[u8]) -> bool {
        super::hash_verbatim_canonical_snapshot(&mut blake3::Hasher::new(), row_pk, snapshot)
    }

    fn assert_matches_reference(schema_key: &str, row_pk: &RowPk, snapshot: &[u8]) {
        let metadata_cases = [
            None,
            Some(Jsonb::from_value(serde_json::Value::Null)),
            Some(Jsonb::from_value(serde_json::json!({"z": [1, 2.5, "x"], "a": null}))),
        ];
        for metadata in &metadata_cases {
            assert_eq!(
                tracked_payload_semantic_fingerprint(
                    schema_key,
                    row_pk,
                    Some(snapshot),
                    metadata.as_ref()
                )
                .expect("fingerprint should compute"),
                reference_fingerprint(schema_key, row_pk, Some(snapshot), metadata.as_ref()),
                "fingerprint diverged for schema {schema_key} payload {snapshot:?}"
            );
        }
    }

    fn storage_payload(row: &lix_schema::Row) -> Vec<u8> {
        crate::plugin::wire::typed::encode_native_row_payload(
            &[7; 32],
            &[lix_schema::Value::Text("placeholder".into())],
            row,
        )
        .expect("storage payload encodes")
    }

    fn row_frame(fields: &[(&str, &[u8])]) -> Vec<u8> {
        let mut frame = (fields.len() as u32).to_le_bytes().to_vec();
        for (name, value) in fields {
            frame.extend_from_slice(&(name.len() as u32).to_le_bytes());
            frame.extend_from_slice(name.as_bytes());
            frame.extend_from_slice(value);
        }
        frame
    }

    fn storage_payload_from_frame(frame: &[u8]) -> Vec<u8> {
        let mut payload = vec![crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION];
        payload.extend_from_slice(&[7; 32]);
        payload.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        payload.extend_from_slice(frame);
        payload
    }

    fn jsonb_value_bytes(text: &str) -> Vec<u8> {
        let mut bytes = vec![6];
        bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
        bytes
    }

    #[test]
    fn verbatim_fast_path_is_byte_identical_to_decode_reencode() {
        use lix_schema::Value;
        let row_pks = [
            RowPk::single("plain-id"),
            RowPk::single("ünïcødé ✓ key"),
            RowPk::single(""),
            RowPk::from_schema_values(&[Value::Uuid(uuid::Uuid::from_u128(0x1234_5678))]).unwrap(),
            RowPk::from_schema_values(&[Value::Int8(-42)]).unwrap(),
            RowPk::from_schema_values(&[
                Value::Text("composite".into()),
                Value::Int8(i64::MAX),
                Value::Uuid(uuid::Uuid::from_u128(u128::MAX)),
            ])
            .unwrap(),
        ];
        let json = |value: serde_json::Value| Value::Jsonb(Jsonb::from_value(value));
        let rows = [
            lix_schema::Row::from([("id", Value::Text("x".into()))]),
            lix_schema::Row::from([
                ("a_null", Value::Null),
                ("b_json_null", json(serde_json::Value::Null)),
                ("c_text", Value::Text("héllo \"quoted\" \\ ✓".into())),
                ("d_empty_text", Value::Text(String::new())),
                ("e_uuid", Value::Uuid(uuid::Uuid::from_u128(99))),
                ("f_int", Value::Int8(i64::MIN)),
                ("g_float_zero", Value::Float8(0.0)),
                ("h_float", Value::Float8(-1.25)),
                ("i_float_big", Value::Float8(1e300)),
                ("j_true", Value::Boolean(true)),
                ("k_false", Value::Boolean(false)),
                ("l_timestamp", Value::Timestamptz(1_700_000_000_000)),
            ]),
            lix_schema::Row::from([
                ("declarations", json(serde_json::json!([{"type": "input-variable", "name": "name"}]))),
                ("id", Value::Text("section1.key_1".into())),
                (
                    "pattern",
                    json(serde_json::json!([{"type": "text", "value": "Hello "}, {"type": "expression", "arg": {"type": "variable-reference", "name": "name"}}])),
                ),
                ("numbers", json(serde_json::json!([0, -1, 2.5, 1e16, 10000000000000000_u64, 1e-7, u64::MAX, i64::MIN]))),
                ("object", json(serde_json::json!({"z": {"nested": [true, false, null]}, "a": "\u{0001}\n\t"}))),
                ("scalar_json_string", json(serde_json::json!("just text"))),
                ("scalar_json_number", json(serde_json::json!(3.0))),
            ]),
        ];
        let mut verbatim = 0;
        for row_pk in &row_pks {
            for row in &rows {
                let snapshot = storage_payload(row);
                assert_matches_reference("fingerprint_test", row_pk, &snapshot);
                verbatim += usize::from(takes_verbatim_path(row_pk, &snapshot));
            }
        }
        assert_eq!(
            verbatim,
            row_pks.len() * rows.len(),
            "engine-encoded storage payloads should take the verbatim path"
        );
    }

    #[test]
    fn non_verbatim_payloads_fall_back_to_decode_reencode() {
        let row_pk = RowPk::single("fallback-row");

        // Built-in compact (and compressed) engine payloads.
        let (_, builtin) = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key("lix_key_value")
            .unwrap();
        for size in [3, 8192] {
            let json = serde_json::json!({
                "key": "fallback-row",
                "value": {"text": "β".repeat(size), "nested": [true, null, 42]}
            });
            let compact = crate::row_payload::TypedRow::from_normalized_json(builtin, &row_pk, &json)
                .unwrap()
                .durable_payload()
                .unwrap();
            assert!(!takes_verbatim_path(&row_pk, &compact));
            assert_matches_reference("lix_key_value", &row_pk, &compact);
        }

        // Legacy self-contained identity payload (version 2).
        let row = lix_schema::Row::from([("id", lix_schema::Value::Text("fallback-row".into()))]);
        let identity = crate::plugin::wire::typed::encode_native_row_payload_with_identity(
            &[7; 32],
            &[lix_schema::Value::Text("fallback-row".into())],
            &row,
        )
        .unwrap();
        assert!(!takes_verbatim_path(&row_pk, &identity));
        assert_matches_reference("fingerprint_test", &row_pk, &identity);

        // Columns stored out of lexical order decode, but re-encode sorted.
        let text = |value: &str| {
            let mut bytes = vec![1];
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
            bytes
        };
        let unsorted = storage_payload_from_frame(&row_frame(&[
            ("b", &text("second")),
            ("a", &text("first")),
        ]));
        let sorted = storage_payload_from_frame(&row_frame(&[
            ("a", &text("first")),
            ("b", &text("second")),
        ]));
        assert!(!takes_verbatim_path(&row_pk, &unsorted));
        assert!(takes_verbatim_path(&row_pk, &sorted));
        assert_matches_reference("fingerprint_test", &row_pk, &unsorted);
        assert_eq!(
            tracked_payload_semantic_fingerprint("fingerprint_test", &row_pk, Some(&unsorted), None)
                .unwrap(),
            tracked_payload_semantic_fingerprint("fingerprint_test", &row_pk, Some(&sorted), None)
                .unwrap(),
            "column order is not semantic"
        );

        // Accepted canonical JSONB text whose numbers the decoder re-renders.
        let mut rerendered = 0;
        for number in [
            "100000000000000000000",
            "1000000000000000000000000",
            "123456789012345678901234567890",
            "0.1",
            "1e16",
            "10000000000000000",
        ] {
            let text = format!("[{number}]");
            if lix_schema::validate_canonical_json_text(text.as_bytes()).is_err() {
                continue;
            }
            let payload =
                storage_payload_from_frame(&row_frame(&[("value", &jsonb_value_bytes(&text))]));
            let verbatim =
                lix_schema::validate_verbatim_canonical_json_text(text.as_bytes()).is_ok();
            assert_eq!(takes_verbatim_path(&row_pk, &payload), verbatim);
            rerendered += usize::from(!verbatim);
            assert_matches_reference("fingerprint_test", &row_pk, &payload);
        }
        assert!(rerendered > 0, "expected a re-rendered JSONB number spelling");
    }
}
