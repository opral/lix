#![allow(clippy::cast_possible_truncation, clippy::unnecessary_mut_passed)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::json_store::JsonSlot;
use crate::tracked_state::codec::DecodedTrackedStateKeyShared;
use crate::tracked_state::diff_id::encode_diff_id;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{TrackedStateFilter, TrackedStateStoreReader};

/// Filter for comparing two tracked-state commit roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffRequest {
    pub(crate) filter: TrackedStateFilter,
    pub(crate) retain_payloads: bool,
}

impl Default for TrackedStateDiffRequest {
    fn default() -> Self {
        Self {
            filter: TrackedStateFilter::default(),
            retain_payloads: true,
        }
    }
}

/// Changed tracked-state rows between two commit roots.
#[derive(Debug, Clone, Default)]
pub(crate) struct TrackedStateDiff {
    pub(crate) entries: Vec<TrackedStateDiffEntry>,
    payloads: TrackedStatePayloadBatch,
}

impl PartialEq for TrackedStateDiff {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Eq for TrackedStateDiff {}

/// Change payload columns retained by a tracked-state diff.
///
/// Merge/checkpoint diff validation loads every changed row's immutable
/// payload. Identity-only SQL diffs retain only the live/live comparison
/// subset. Keeping either set behind one `Arc` lets downstream analysis reuse
/// it without cloning records. Logical rows resolve through the change-id
/// ordinal index and borrow the snapshot/metadata columns in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStatePayloadBatch {
    columns: Arc<TrackedStatePayloadColumns>,
}

#[derive(Debug, PartialEq, Eq, Default)]
struct TrackedStatePayloadColumns {
    change_ids: Vec<ChangeId>,
    snapshots: Vec<JsonSlot>,
    metadata: Vec<JsonSlot>,
    id_ordinals: HashMap<ChangeId, u32>,
}

/// Borrowed payload view into a [`TrackedStatePayloadBatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedStatePayloadRef<'a> {
    pub(crate) change_id: ChangeId,
    pub(crate) snapshot: &'a JsonSlot,
    pub(crate) metadata: &'a JsonSlot,
}

/// One changed identity between two commit roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffEntry {
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) kind: TrackedStateDiffKind,
    /// Raw row in the left root.
    ///
    /// This can be a tombstone. Callers that need user-visible semantics
    /// should use `visible_before()` instead of inspecting this directly.
    pub(crate) before: Option<TrackedStateDiffRow>,
    /// Raw row in the right root.
    ///
    /// This can be a tombstone. Keeping the raw tombstone is what lets merge
    /// apply deletes without reloading the source root.
    pub(crate) after: Option<TrackedStateDiffRow>,
}

/// Payload-light tracked-state row carried by diff and merge planning.
///
/// This deliberately stores JSON refs, not JSON payload strings. Diff can
/// compare and report rows from tracked-state tree values without hydrating
/// snapshot or metadata bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffRow {
    /// Shared key owner for the entry and both side rows.
    ///
    /// Tree diff decoding yields an owned key once. Keeping it behind the
    /// typed identity avoids copying schema/file/row buffers into every
    /// before/after row and again into merge planning.
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
}

/// One contiguous identity column shared by a tracked-state diff batch.
///
/// Production diff decoding moves every decoded key into this column once.
/// Entry and side-row identities then carry only an `Arc` to the column plus a
/// compact ordinal; cloning an identity through merge planning never clones a
/// schema key, file id, row pk, or per-key heap owner.
#[derive(Debug)]
struct TrackedStateDiffIdentityBatch {
    keys: TrackedStateDiffKeyStorage,
}

#[derive(Debug)]
enum TrackedStateDiffKeyStorage {
    /// Keeps hand-built single-row tests and point-read helpers ergonomic
    /// without allocating dictionary and one-element column buffers.
    Singleton(TrackedStateDiffKey),
    Batch(TrackedStateDiffKeyColumns),
}

/// Dictionary-encoded identity columns for one diff batch.
///
/// Repeated schema and file identifiers live once in their dictionaries.
/// Every logical key row is one compact ordinal pair plus the typed row pk.
#[derive(Debug)]
struct TrackedStateDiffKeyColumns {
    schema_keys: Vec<SharedStr>,
    file_ids: Vec<SharedStr>,
    rows: Vec<TrackedStateDiffKeyRow>,
}

const DIFF_SMALL_STRING_DICTIONARY_LIMIT: usize = 32;

/// Small-first dictionary builder for repeated diff metadata.
///
/// The common batch has one schema and zero or one file id, so row-sized
/// dictionaries and hash tables would dominate peak memory. A genuinely wide
/// batch promotes once after the small linear dictionary fills.
struct TrackedStateDiffStringInterner {
    values: Vec<SharedStr>,
    ordinals: Option<HashMap<SharedStr, u32>>,
    expected_cardinality: usize,
}

#[derive(Debug)]
struct TrackedStateDiffKeyRow {
    schema_key_ordinal: u32,
    /// `u32::MAX` is the null sentinel. Dictionary sizes are checked before
    /// sealing the batch, so it can never alias a valid file-id ordinal.
    file_id_ordinal: u32,
    row_pk: crate::row_pk::RowPk,
}

#[derive(Debug)]
struct TrackedStateDiffKey {
    schema_key: SharedStr,
    file_id: Option<SharedStr>,
    row_pk: crate::row_pk::RowPk,
}

/// Typed tree-diff stage shared directly with diff validation/classification.
///
/// Identity metadata is dictionary encoded once, row keys occupy one typed
/// column, and both root sides are aligned `TrackedStateIndexValue` columns.
/// No production `TrackedStateTreeDiffEntry` or row-owned key exists between
/// tree traversal and the final public diff entries.
#[derive(Debug, Default)]
pub(crate) struct TrackedStateTreeDiffBatch {
    identities: Option<Arc<TrackedStateDiffIdentityBatch>>,
    before: Vec<Option<TrackedStateIndexValue>>,
    after: Vec<Option<TrackedStateIndexValue>>,
}

pub(crate) struct TrackedStateTreeDiffBatchBuilder {
    schema_keys: TrackedStateDiffStringInterner,
    file_ids: TrackedStateDiffStringInterner,
    rows: Vec<TrackedStateDiffKeyRow>,
    before: Vec<Option<TrackedStateIndexValue>>,
    after: Vec<Option<TrackedStateIndexValue>>,
}

#[derive(Clone, Copy)]
pub(crate) struct TrackedStateTreeDiffRowRef<'a> {
    identities: &'a TrackedStateDiffIdentityBatch,
    ordinal: u32,
    value: &'a TrackedStateIndexValue,
}

/// Root-local tracked-state identity view.
///
/// Equality and ordering are key-based, so identities from different diff
/// batches remain interchangeable in sorted merge comparisons and sets.
#[derive(Clone)]
pub(crate) struct TrackedStateDiffIdentity {
    batch: Arc<TrackedStateDiffIdentityBatch>,
    ordinal: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackedStateDiffKind {
    Added,
    Modified,
    Removed,
}

impl Default for TrackedStatePayloadBatch {
    fn default() -> Self {
        static EMPTY: OnceLock<Arc<TrackedStatePayloadColumns>> = OnceLock::new();
        Self {
            columns: Arc::clone(
                EMPTY.get_or_init(|| Arc::new(TrackedStatePayloadColumns::default())),
            ),
        }
    }
}

impl TrackedStatePayloadBatch {
    /// Seals owned payload slots into deterministic typed columns.
    ///
    /// The input is sorted once by change id. Production callers move slots
    /// directly out of decoded change records, so sealing does not clone
    /// inline payload buffers.
    pub(crate) fn from_payloads(
        payloads: impl IntoIterator<Item = (ChangeId, JsonSlot, JsonSlot)>,
    ) -> Result<Self, LixError> {
        let mut payloads = payloads.into_iter().collect::<Vec<_>>();
        if payloads.is_empty() {
            return Ok(Self::default());
        }
        if payloads.len() > u32::MAX as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state payload batch exceeds the ordinal range",
            ));
        }
        payloads.sort_unstable_by_key(|(change_id, _, _)| *change_id);

        let row_count = payloads.len();
        let mut change_ids = Vec::with_capacity(row_count);
        let mut snapshots = Vec::with_capacity(row_count);
        let mut metadata = Vec::with_capacity(row_count);
        let mut id_ordinals = HashMap::with_capacity(row_count);
        for (ordinal, (change_id, snapshot, row_metadata)) in payloads.into_iter().enumerate() {
            let ordinal = u32::try_from(ordinal).expect("payload row count was bounded to u32");
            if id_ordinals.insert(change_id, ordinal).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-state payload batch contains duplicate change id '{change_id}'"
                    ),
                ));
            }
            change_ids.push(change_id);
            snapshots.push(snapshot);
            metadata.push(row_metadata);
        }
        Ok(Self {
            columns: Arc::new(TrackedStatePayloadColumns {
                change_ids,
                snapshots,
                metadata,
                id_ordinals,
            }),
        })
    }

    pub(crate) fn get(&self, change_id: ChangeId) -> Option<TrackedStatePayloadRef<'_>> {
        let ordinal = *self.columns.id_ordinals.get(&change_id)? as usize;
        Some(TrackedStatePayloadRef {
            change_id: self.columns.change_ids[ordinal],
            snapshot: &self.columns.snapshots[ordinal],
            metadata: &self.columns.metadata[ordinal],
        })
    }

    pub(crate) fn contains(&self, change_id: ChangeId) -> bool {
        self.columns.id_ordinals.contains_key(&change_id)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.columns.change_ids.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.columns.change_ids.len()
    }

    #[cfg(test)]
    pub(crate) fn shares_owner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.columns, &other.columns)
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            // Three dense columns plus one change-id ordinal index, regardless
            // of logical row count.
            4
        }
    }
}

impl TrackedStateDiff {
    pub(crate) fn from_entries(entries: Vec<TrackedStateDiffEntry>) -> Self {
        Self {
            entries,
            payloads: TrackedStatePayloadBatch::default(),
        }
    }

    pub(crate) fn from_entries_with_payloads(
        entries: Vec<TrackedStateDiffEntry>,
        payloads: TrackedStatePayloadBatch,
    ) -> Self {
        Self { entries, payloads }
    }

    pub(crate) fn payloads(&self) -> &TrackedStatePayloadBatch {
        &self.payloads
    }
}

/// Diffs two tracked-state commit roots with hash-guided subtree skipping.
///
/// Commit-root first-parent metadata is bound to the changelog. Payload-
/// retaining consumers validate every emitted row against its immutable
/// change record. Identity-only SQL consumers validate the packed delta leaf
/// and hydrate payloads only for live/live equality classification. Winner
/// reachability, inherited creation time, and absence of omitted unchanged
/// rows belong to the explicit full-root integrity audit; proving those here
/// would make sparse diff O(total rows).
pub(crate) async fn diff_commits<S>(
    reader: &mut TrackedStateStoreReader<S>,
    left_commit_id: &str,
    right_commit_id: &str,
    request: &TrackedStateDiffRequest,
) -> Result<TrackedStateDiff, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let scan_request = scan_request_for_diff(request);
    let tree_diff = reader
        .diff_semantic_tree_entries_at_commits(left_commit_id, right_commit_id, &scan_request)
        .await?;

    // Validate only rows exposed by the hash-guided tree diff. Whole-root
    // coverage validation is an explicit integrity audit; doing it here would
    // turn every sparse diff back into an O(total rows) scan.
    //
    // Rootless rows still come from an independently stored packed-delta
    // index. Merge/checkpoint consumers retain full payload authority; SQL
    // consumers validate the allocation-free leaf index and avoid decoding
    // payload sidecars for added/removed rows.
    let payloads = if request.retain_payloads {
        reader
            .validate_tree_diff_batch_and_load_payloads(&tree_diff)
            .await?
    } else {
        reader
            .load_tree_diff_comparison_payloads(&tree_diff)
            .await?
    };

    // Rows are identity-only; payload equality needs the change records when
    // a live/live pair carries different change ids (cross-branch writes can
    // produce identical content under distinct changes, which must classify
    // as no-diff). Reuse the records loaded for changed-row validation instead
    // of issuing a second changelog read.

    let entries = classify_tree_diff_batch(tree_diff, &payloads)?;

    let diff = TrackedStateDiff::from_entries_with_payloads(entries, payloads);
    Ok(diff)
}

fn classify_tree_diff_batch(
    tree_diff: TrackedStateTreeDiffBatch,
    payloads: &TrackedStatePayloadBatch,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    let row_count = tree_diff.len();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let (identities, before, after) = tree_diff.into_columns();
    let identities = identities.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "non-empty tracked-state tree diff is missing its identity columns",
        )
    })?;
    let mut entries = Vec::with_capacity(row_count);
    for (ordinal, (before, after)) in before.into_iter().zip(after).enumerate() {
        let Some(kind) = classify_diff_values(before.as_ref(), after.as_ref(), payloads) else {
            continue;
        };
        let identity = TrackedStateDiffIdentity::from_batch_ordinal(
            Arc::clone(&identities),
            u32::try_from(ordinal).expect("diff row count was bounded to u32"),
        );
        let before =
            before.map(|value| TrackedStateDiffRow::from_index_value(identity.clone(), value));
        let after =
            after.map(|value| TrackedStateDiffRow::from_index_value(identity.clone(), value));
        entries.push(TrackedStateDiffEntry {
            identity,
            kind,
            before,
            after,
        });
    }
    Ok(entries)
}

fn scan_request_for_diff(request: &TrackedStateDiffRequest) -> TrackedStateTreeScanRequest {
    let mut filter = request.filter.clone();
    filter.include_tombstones = true;
    TrackedStateTreeScanRequest {
        schema_keys: filter.schema_keys,
        row_pks: filter.row_pks,
        file_ids: filter.file_ids,
        include_tombstones: true,
        limit: None,
    }
}

fn classify_diff_values(
    before: Option<&TrackedStateIndexValue>,
    after: Option<&TrackedStateIndexValue>,
    payloads: &TrackedStatePayloadBatch,
) -> Option<TrackedStateDiffKind> {
    match (is_live_value(before), is_live_value(after)) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffKind::Added),
        (Some(_), None) => Some(TrackedStateDiffKind::Removed),
        (Some(before), Some(after)) if tracked_value_payload_eq(before, after, payloads) => None,
        (Some(_), Some(_)) => Some(TrackedStateDiffKind::Modified),
    }
}

fn is_live_value(row: Option<&TrackedStateIndexValue>) -> Option<&TrackedStateIndexValue> {
    row.filter(|row| !row.deleted)
}

fn tracked_value_payload_eq(
    left: &TrackedStateIndexValue,
    right: &TrackedStateIndexValue,
    payloads: &TrackedStatePayloadBatch,
) -> bool {
    if left.change_id == right.change_id {
        return true;
    }
    match (payloads.get(left.change_id), payloads.get(right.change_id)) {
        (Some(left), Some(right)) => {
            left.snapshot == right.snapshot && left.metadata == right.metadata
        }
        _ => false,
    }
}

impl TrackedStateTreeDiffBatchBuilder {
    pub(crate) fn with_row_capacity(row_count: usize) -> Self {
        Self {
            schema_keys: TrackedStateDiffStringInterner::new(row_count),
            file_ids: TrackedStateDiffStringInterner::new(row_count),
            rows: Vec::with_capacity(row_count),
            before: Vec::with_capacity(row_count),
            after: Vec::with_capacity(row_count),
        }
    }

    /// Reserves the aligned columns once after the tree root exposes its
    /// subtree count. Corrupt or overflowing hints are ignored by callers;
    /// logical rows remain checked when the batch seals.
    pub(crate) fn reserve_exact_once(&mut self, row_count: usize) {
        if self.rows.capacity() == 0 {
            let _ = self.rows.try_reserve_exact(row_count);
            let _ = self.before.try_reserve_exact(row_count);
            let _ = self.after.try_reserve_exact(row_count);
            self.schema_keys.set_expected_cardinality(row_count);
            self.file_ids.set_expected_cardinality(row_count);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn push_shared(
        &mut self,
        key: DecodedTrackedStateKeyShared,
        before: Option<TrackedStateIndexValue>,
        after: Option<TrackedStateIndexValue>,
    ) {
        debug_assert!(before.is_some() || after.is_some());
        let schema_key_ordinal = self.schema_keys.intern_shared(key.schema_key);
        let file_id_ordinal = key
            .file_id
            .map_or(u32::MAX, |file_id| self.file_ids.intern_shared(file_id));
        self.rows.push(TrackedStateDiffKeyRow {
            schema_key_ordinal,
            file_id_ordinal,
            row_pk: key.row_pk,
        });
        self.before.push(before);
        self.after.push(after);
    }

    pub(crate) fn finish(self) -> Result<TrackedStateTreeDiffBatch, LixError> {
        let row_count = self.rows.len();
        debug_assert_eq!(self.before.len(), row_count);
        debug_assert_eq!(self.after.len(), row_count);
        if row_count == 0 {
            return Ok(TrackedStateTreeDiffBatch::default());
        }
        if row_count > u32::MAX as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state tree diff batch exceeds the identity ordinal range",
            ));
        }
        let identities = Arc::new(TrackedStateDiffIdentityBatch {
            keys: TrackedStateDiffKeyStorage::Batch(TrackedStateDiffKeyColumns {
                schema_keys: self.schema_keys.finish(),
                file_ids: self.file_ids.finish(),
                rows: self.rows,
            }),
        });
        Ok(TrackedStateTreeDiffBatch {
            identities: Some(identities),
            before: self.before,
            after: self.after,
        })
    }
}

impl TrackedStateTreeDiffBatch {
    pub(crate) fn len(&self) -> usize {
        self.before.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.before.is_empty()
    }

    pub(crate) fn swap_sides(&mut self) {
        std::mem::swap(&mut self.before, &mut self.after);
    }

    pub(crate) fn side_rows(&self) -> impl Iterator<Item = TrackedStateTreeDiffRowRef<'_>> {
        let identities = self.identities.as_deref();
        self.before.iter().zip(&self.after).enumerate().flat_map(
            move |(ordinal, (before, after))| {
                [before.as_ref(), after.as_ref()]
                    .into_iter()
                    .flatten()
                    .map(move |value| TrackedStateTreeDiffRowRef {
                        identities: identities
                            .expect("non-empty tree diff columns retain identities"),
                        ordinal: u32::try_from(ordinal)
                            .expect("tree diff batch row count is bounded to u32"),
                        value,
                    })
            },
        )
    }

    pub(crate) fn comparison_rows(&self) -> Vec<TrackedStateTreeDiffRowRef<'_>> {
        let Some(identities) = self.identities.as_deref() else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        for (ordinal, (before, after)) in self.before.iter().zip(&self.after).enumerate() {
            let (Some(before), Some(after)) = (before.as_ref(), after.as_ref()) else {
                continue;
            };
            if before.deleted || after.deleted || before.change_id == after.change_id {
                continue;
            }
            let ordinal =
                u32::try_from(ordinal).expect("tree diff batch row count is bounded to u32");
            rows.push(TrackedStateTreeDiffRowRef {
                identities,
                ordinal,
                value: before,
            });
            rows.push(TrackedStateTreeDiffRowRef {
                identities,
                ordinal,
                value: after,
            });
        }
        rows
    }

    fn into_columns(
        self,
    ) -> (
        Option<Arc<TrackedStateDiffIdentityBatch>>,
        Vec<Option<TrackedStateIndexValue>>,
        Vec<Option<TrackedStateIndexValue>>,
    ) {
        (self.identities, self.before, self.after)
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            // Identity rows plus two aligned side columns. Tiny dictionaries
            // stay below the large-buffer threshold.
            3
        }
    }

    #[cfg(test)]
    pub(crate) fn row_capacity(&self) -> usize {
        let identity_capacity = self
            .identities
            .as_ref()
            .map_or(0, |identities| match &identities.keys {
                TrackedStateDiffKeyStorage::Singleton(_) => 1,
                TrackedStateDiffKeyStorage::Batch(keys) => keys.rows.capacity(),
            });
        identity_capacity
            .max(self.before.capacity())
            .max(self.after.capacity())
    }

    #[cfg(test)]
    pub(crate) fn into_rows_for_test(
        self,
    ) -> Vec<crate::tracked_state::types::TrackedStateTreeDiffEntry> {
        let (identities, before, after) = self.into_columns();
        let Some(identities) = identities else {
            return Vec::new();
        };
        before
            .into_iter()
            .zip(after)
            .enumerate()
            .map(|(ordinal, (before, after))| {
                let ordinal =
                    u32::try_from(ordinal).expect("test tree diff batch was bounded to u32");
                crate::tracked_state::types::TrackedStateTreeDiffEntry {
                    key: TrackedStateKey {
                        schema_key: identities.schema_key(ordinal).to_owned(),
                        file_id: identities.file_id(ordinal).map(str::to_owned),
                        row_pk: identities.row_pk(ordinal).clone(),
                    },
                    before,
                    after,
                }
            })
            .collect()
    }
}

impl<'a> TrackedStateTreeDiffRowRef<'a> {
    pub(crate) fn schema_key(self) -> &'a str {
        self.identities.schema_key(self.ordinal)
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.identities.file_id(self.ordinal)
    }

    pub(crate) fn row_pk(self) -> &'a crate::row_pk::RowPk {
        self.identities.row_pk(self.ordinal)
    }

    pub(crate) fn change_id(self) -> ChangeId {
        self.value.change_id
    }

    pub(crate) fn commit_id(self) -> CommitId {
        self.value.commit_id
    }

    pub(crate) fn deleted(self) -> bool {
        self.value.deleted
    }

    pub(crate) fn updated_at(self) -> LixTimestamp {
        self.value.updated_at()
    }
}

impl TrackedStateDiffIdentityBatch {
    fn from_keys(keys: Vec<TrackedStateKey>) -> Arc<Self> {
        debug_assert!(!keys.is_empty());
        let row_count = keys.len();
        let mut schema_keys = TrackedStateDiffStringInterner::new(row_count);
        let mut file_ids = TrackedStateDiffStringInterner::new(row_count);
        let mut rows = Vec::with_capacity(row_count);
        for key in keys {
            let schema_key_ordinal = schema_keys.intern_owned(key.schema_key);
            let file_id_ordinal = key
                .file_id
                .map_or(u32::MAX, |file_id| file_ids.intern_owned(file_id));
            rows.push(TrackedStateDiffKeyRow {
                schema_key_ordinal,
                file_id_ordinal,
                row_pk: key.row_pk,
            });
        }
        Arc::new(Self {
            keys: TrackedStateDiffKeyStorage::Batch(TrackedStateDiffKeyColumns {
                schema_keys: schema_keys.finish(),
                file_ids: file_ids.finish(),
                rows,
            }),
        })
    }

    fn from_key_refs<'a>(
        row_count: usize,
        mut key_at: impl FnMut(usize) -> TrackedStateKeyRef<'a>,
    ) -> Arc<Self> {
        debug_assert!(row_count > 0);
        let mut schema_keys = TrackedStateDiffStringInterner::new(row_count);
        let mut file_ids = TrackedStateDiffStringInterner::new(row_count);
        let mut rows = Vec::with_capacity(row_count);
        for ordinal in 0..row_count {
            let key = key_at(ordinal);
            let schema_key_ordinal = schema_keys.intern_str(key.schema_key);
            let file_id_ordinal = key
                .file_id
                .map_or(u32::MAX, |file_id| file_ids.intern_str(file_id));
            rows.push(TrackedStateDiffKeyRow {
                schema_key_ordinal,
                file_id_ordinal,
                row_pk: key.row_pk.clone(),
            });
        }
        Arc::new(Self {
            keys: TrackedStateDiffKeyStorage::Batch(TrackedStateDiffKeyColumns {
                schema_keys: schema_keys.finish(),
                file_ids: file_ids.finish(),
                rows,
            }),
        })
    }

    fn singleton(key: TrackedStateKey) -> Arc<Self> {
        Arc::new(Self {
            keys: TrackedStateDiffKeyStorage::Singleton(TrackedStateDiffKey {
                schema_key: key.schema_key.into(),
                file_id: key.file_id.map(Into::into),
                row_pk: key.row_pk,
            }),
        })
    }

    fn schema_key(&self, ordinal: u32) -> &str {
        match &self.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => {
                debug_assert_eq!(ordinal, 0);
                key.schema_key.as_str()
            }
            TrackedStateDiffKeyStorage::Batch(keys) => {
                let row = &keys.rows[ordinal as usize];
                keys.schema_keys[row.schema_key_ordinal as usize].as_str()
            }
        }
    }

    fn file_id(&self, ordinal: u32) -> Option<&str> {
        match &self.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => {
                debug_assert_eq!(ordinal, 0);
                key.file_id.as_deref()
            }
            TrackedStateDiffKeyStorage::Batch(keys) => {
                let row = &keys.rows[ordinal as usize];
                (row.file_id_ordinal != u32::MAX)
                    .then(|| keys.file_ids[row.file_id_ordinal as usize].as_str())
            }
        }
    }

    fn row_pk(&self, ordinal: u32) -> &crate::row_pk::RowPk {
        match &self.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => {
                debug_assert_eq!(ordinal, 0);
                &key.row_pk
            }
            TrackedStateDiffKeyStorage::Batch(keys) => &keys.rows[ordinal as usize].row_pk,
        }
    }

    #[cfg(test)]
    fn into_key(self, ordinal: u32) -> TrackedStateKey {
        match self.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => {
                debug_assert_eq!(ordinal, 0);
                key.into_key()
            }
            TrackedStateDiffKeyStorage::Batch(keys) => keys.key(ordinal).into_key(),
        }
    }

    fn len(&self) -> usize {
        match &self.keys {
            TrackedStateDiffKeyStorage::Singleton(_) => 1,
            TrackedStateDiffKeyStorage::Batch(keys) => keys.rows.len(),
        }
    }
}

impl TrackedStateDiffStringInterner {
    fn new(expected_cardinality: usize) -> Self {
        Self {
            values: Vec::with_capacity(
                expected_cardinality.min(DIFF_SMALL_STRING_DICTIONARY_LIMIT),
            ),
            ordinals: None,
            expected_cardinality,
        }
    }

    fn set_expected_cardinality(&mut self, expected_cardinality: usize) {
        self.expected_cardinality = self.expected_cardinality.max(expected_cardinality);
        if self.values.capacity() == 0 {
            let small_capacity = self
                .expected_cardinality
                .min(DIFF_SMALL_STRING_DICTIONARY_LIMIT);
            let _ = self.values.try_reserve_exact(small_capacity);
        }
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        if let Some(ordinal) = self.ordinal(value.as_str()) {
            return ordinal;
        }
        self.insert_new(SharedStr::from(value))
    }

    fn intern_str(&mut self, value: &str) -> u32 {
        if let Some(ordinal) = self.ordinal(value) {
            return ordinal;
        }
        self.insert_new(SharedStr::from(value))
    }

    fn intern_shared(&mut self, value: SharedStr) -> u32 {
        if let Some(ordinal) = self.ordinal(value.as_str()) {
            return ordinal;
        }
        self.insert_new(value)
    }

    fn ordinal(&self, value: &str) -> Option<u32> {
        self.ordinals.as_ref().map_or_else(
            || {
                self.values
                    .iter()
                    .position(|candidate| candidate.as_str() == value)
                    .map(|ordinal| {
                        u32::try_from(ordinal)
                            .expect("diff row count bounds each identity dictionary")
                    })
            },
            |ordinals| ordinals.get(value).copied(),
        )
    }

    fn insert_new(&mut self, value: SharedStr) -> u32 {
        if self.ordinals.is_none() && self.values.len() == DIFF_SMALL_STRING_DICTIONARY_LIMIT {
            let target = self.expected_cardinality.max(self.values.len() + 1);
            if target > self.values.capacity() {
                self.values.reserve_exact(target - self.values.len());
            }
            let mut ordinals = HashMap::with_capacity(target);
            for (ordinal, value) in self.values.iter().enumerate() {
                ordinals.insert(
                    value.clone(),
                    u32::try_from(ordinal).expect("diff row count bounds each identity dictionary"),
                );
            }
            self.ordinals = Some(ordinals);
        }

        let ordinal = u32::try_from(self.values.len())
            .expect("diff row count bounds each identity dictionary");
        if let Some(ordinals) = self.ordinals.as_mut() {
            ordinals.insert(value.clone(), ordinal);
        }
        self.values.push(value);
        ordinal
    }

    fn finish(self) -> Vec<SharedStr> {
        self.values
    }
}

impl TrackedStateDiffKeyColumns {
    #[cfg(test)]
    fn key(&self, ordinal: u32) -> TrackedStateDiffKey {
        let row = &self.rows[ordinal as usize];
        TrackedStateDiffKey {
            schema_key: self.schema_keys[row.schema_key_ordinal as usize].clone(),
            file_id: (row.file_id_ordinal != u32::MAX)
                .then(|| self.file_ids[row.file_id_ordinal as usize].clone()),
            row_pk: row.row_pk.clone(),
        }
    }
}

impl TrackedStateDiffKey {
    #[cfg(test)]
    fn into_key(self) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: self.schema_key.to_string(),
            file_id: self.file_id.map(|file_id| file_id.to_string()),
            row_pk: self.row_pk,
        }
    }
}

impl TrackedStateDiffIdentity {
    pub(crate) fn from_key(key: TrackedStateKey) -> Self {
        Self {
            batch: TrackedStateDiffIdentityBatch::singleton(key),
            ordinal: 0,
        }
    }

    /// Moves a complete key batch behind one shared identity owner.
    ///
    /// This is used by accelerators that discover changed keys outside the
    /// tracked-state tree diff. It preserves the same one-owner-per-batch
    /// contract instead of creating a singleton `Arc` allocation per key.
    pub(crate) fn from_key_batch(keys: Vec<TrackedStateKey>) -> Result<Vec<Self>, LixError> {
        let row_count = keys.len();
        if row_count == 0 {
            return Ok(Vec::new());
        }
        if row_count > u32::MAX as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state diff batch exceeds the identity ordinal range",
            ));
        }
        let batch = TrackedStateDiffIdentityBatch::from_keys(keys);
        Ok((0..row_count)
            .map(|ordinal| {
                Self::from_batch_ordinal(
                    Arc::clone(&batch),
                    u32::try_from(ordinal).expect("diff row count was bounded to u32"),
                )
            })
            .collect())
    }

    /// Seals borrowed keys behind one shared identity owner.
    ///
    /// Callers expose stable key views by ordinal. Schema/file values are
    /// interned once into batch dictionaries and row primary keys clone
    /// only their shared descriptors, avoiding a terminal `String` allocation
    /// per discovered key.
    pub(crate) fn from_key_refs<'a>(
        row_count: usize,
        key_at: impl FnMut(usize) -> TrackedStateKeyRef<'a>,
    ) -> Result<Vec<Self>, LixError> {
        if row_count == 0 {
            return Ok(Vec::new());
        }
        if row_count > u32::MAX as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state diff batch exceeds the identity ordinal range",
            ));
        }
        let batch = TrackedStateDiffIdentityBatch::from_key_refs(row_count, key_at);
        Ok((0..row_count)
            .map(|ordinal| {
                Self::from_batch_ordinal(
                    Arc::clone(&batch),
                    u32::try_from(ordinal).expect("diff row count was bounded to u32"),
                )
            })
            .collect())
    }

    fn from_batch_ordinal(batch: Arc<TrackedStateDiffIdentityBatch>, ordinal: u32) -> Self {
        debug_assert!((ordinal as usize) < batch.len());
        Self { batch, ordinal }
    }

    pub(crate) fn schema_key(&self) -> &str {
        self.batch.schema_key(self.ordinal)
    }

    pub(crate) fn as_key_ref(&self) -> TrackedStateKeyRef<'_> {
        TrackedStateKeyRef {
            schema_key: self.schema_key(),
            file_id: self.file_id(),
            row_pk: self.row_pk(),
        }
    }

    /// Clones the shared schema owner without allocating decoded text.
    ///
    /// Downstream typed batches use this when re-dictionary-encoding an
    /// identity at another boundary, such as plugin merge output entering the
    /// transaction pipeline.
    pub(crate) fn schema_key_shared(&self) -> SharedStr {
        match &self.batch.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => key.schema_key.clone(),
            TrackedStateDiffKeyStorage::Batch(keys) => {
                let row = &keys.rows[self.ordinal as usize];
                keys.schema_keys[row.schema_key_ordinal as usize].clone()
            }
        }
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.batch.file_id(self.ordinal)
    }

    /// Clones the shared file-id owner without allocating decoded text.
    pub(crate) fn file_id_shared(&self) -> Option<SharedStr> {
        match &self.batch.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => key.file_id.clone(),
            TrackedStateDiffKeyStorage::Batch(keys) => {
                let row = &keys.rows[self.ordinal as usize];
                (row.file_id_ordinal != u32::MAX)
                    .then(|| keys.file_ids[row.file_id_ordinal as usize].clone())
            }
        }
    }

    pub(crate) fn row_pk(&self) -> &crate::row_pk::RowPk {
        self.batch.row_pk(self.ordinal)
    }

    #[cfg(test)]
    pub(crate) fn into_key(self) -> TrackedStateKey {
        match Arc::try_unwrap(self.batch) {
            Ok(batch) => batch.into_key(self.ordinal),
            Err(batch) => TrackedStateKey {
                schema_key: batch.schema_key(self.ordinal).to_owned(),
                file_id: batch.file_id(self.ordinal).map(str::to_owned),
                row_pk: batch.row_pk(self.ordinal).clone(),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_key_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.batch, &other.batch) && self.ordinal == other.ordinal
    }

    #[cfg(test)]
    pub(crate) fn shares_batch_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.batch, &other.batch)
    }

    #[cfg(test)]
    fn batch_len(&self) -> usize {
        self.batch.len()
    }

    #[cfg(test)]
    fn batch_dictionary_counts(&self) -> (usize, usize) {
        match &self.batch.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => (1, usize::from(key.file_id.is_some())),
            TrackedStateDiffKeyStorage::Batch(keys) => {
                (keys.schema_keys.len(), keys.file_ids.len())
            }
        }
    }

    #[cfg(test)]
    fn batch_dictionary_capacities(&self) -> (usize, usize) {
        match &self.batch.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => (1, usize::from(key.file_id.is_some())),
            TrackedStateDiffKeyStorage::Batch(keys) => {
                (keys.schema_keys.capacity(), keys.file_ids.capacity())
            }
        }
    }
}

impl fmt::Debug for TrackedStateDiffIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrackedStateDiffIdentity")
            .field("schema_key", &self.schema_key())
            .field("file_id", &self.file_id())
            .field("row_pk", self.row_pk())
            .finish()
    }
}

impl PartialEq for TrackedStateDiffIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.file_id() == other.file_id()
            && self.row_pk() == other.row_pk()
    }
}

impl Eq for TrackedStateDiffIdentity {}

impl PartialOrd for TrackedStateDiffIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TrackedStateDiffIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.schema_key()
            .cmp(other.schema_key())
            .then_with(|| self.file_id().cmp(&other.file_id()))
            .then_with(|| self.row_pk().cmp(other.row_pk()))
    }
}

impl Hash for TrackedStateDiffIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema_key().hash(state);
        self.file_id().hash(state);
        self.row_pk().hash(state);
    }
}

impl TrackedStateDiffRow {
    pub(crate) fn from_tree_entry(key: TrackedStateKey, value: TrackedStateIndexValue) -> Self {
        Self::from_index_value(TrackedStateDiffIdentity::from_key(key), value)
    }

    fn from_index_value(identity: TrackedStateDiffIdentity, value: TrackedStateIndexValue) -> Self {
        Self {
            identity,
            deleted: value.deleted,
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        self.identity.schema_key()
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.identity.file_id()
    }

    pub(crate) fn row_pk(&self) -> &crate::row_pk::RowPk {
        self.identity.row_pk()
    }

    pub(crate) fn index_value(&self) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    #[cfg(test)]
    pub(crate) fn into_index_entry(self) -> (TrackedStateKey, TrackedStateIndexValue) {
        let value = self.index_value();
        (self.identity.into_key(), value)
    }
}

impl TrackedStateDiffEntry {
    #[cfg(test)]
    pub(crate) fn before_is_live(&self) -> bool {
        self.visible_before().is_some()
    }

    #[cfg(test)]
    pub(crate) fn after_is_live(&self) -> bool {
        self.visible_after().is_some()
    }

    pub(crate) fn visible_before(&self) -> Option<&TrackedStateDiffRow> {
        self.before.as_ref().filter(|row| !row.deleted)
    }

    #[cfg(test)]
    pub(crate) fn visible_after(&self) -> Option<&TrackedStateDiffRow> {
        self.after.as_ref().filter(|row| !row.deleted)
    }

    /// Stable `diff_id` for this entry.
    ///
    /// The before side is the **visible** before row, so a checkpointed delete
    /// (which leaves a tombstone in the left root) encodes identically to an
    /// identity that was never present. Both describe the same change — "this
    /// row is not there, and now it is" — and every other consumer of the
    /// before side already normalizes them: `classify_hot_working_diff_entry`
    /// filters deleted before rows before choosing the kind, and the
    /// apply/revert precondition treats `None` as "absent or tombstoned".
    /// Encoding the raw tombstone here would make the id depend on whether a
    /// tombstone happens to be retained, which is a storage-layer detail.
    ///
    /// The after side stays **raw** on purpose. A `Removed` entry's after row
    /// *is* the tombstone, and merge relies on it to apply deletes without
    /// reloading the source root. Do not "fix" this asymmetry — the two sides
    /// answer different questions.
    pub(crate) fn diff_id(&self) -> Result<String, LixError> {
        encode_diff_id(
            self.visible_before().map(|row| row.change_id),
            self.after.as_ref().map(|row| row.change_id),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::row_pk::RowPk;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageAdapterRead, StorageWriteSet};
    use crate::tracked_state::types::{
        TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateMutation,
        TrackedStateRootId,
    };
    use crate::tracked_state::{MaterializedTrackedStateRow, TrackedStateContext};

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    fn change_id(label: &str) -> String {
        ChangeId::for_test_label(label).to_string()
    }

    async fn stage_snapshot_authority_for_test(
        read: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        snapshot_root: &TrackedStateCommitRoot,
    ) -> Result<(), LixError> {
        let manifest = crate::tracked_state::storage::load_published_commit_state_manifest(
            read,
            snapshot_root.commit_id,
        )
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "corrupt-root fixture has no commit-state manifest",
            )
        })?;
        let mut manifest = (*manifest).clone();
        manifest.snapshot_root = Some(Box::new(snapshot_root.clone()));
        crate::tracked_state::storage::stage_resealed_commit_state_manifest_for_test(
            writes, &manifest,
        )
    }

    /// A checkpointed delete leaves a tombstone in the left root, so the same
    /// logical history ("this identity is not here, and now it is") reaches the
    /// diff surface with either `before = None` or `before = Some(tombstone)`.
    /// The `diff_id` must not be able to tell those apart, otherwise physically
    /// compacting tombstones away would silently renumber every
    /// delete-then-reinsert diff.
    #[test]
    fn diff_id_is_blind_to_a_tombstoned_before_row() {
        let created_at = ts("2024-01-01T00:00:00.000Z");
        let updated_at = ts("2024-01-02T00:00:00.000Z");
        let after_change_id = ChangeId::for_test_label("reinserted-after");
        let commit_id = CommitId::for_test_label("diff-id-commit");
        let key = || DecodedTrackedStateKeyShared {
            schema_key: SharedStr::from_static("test_schema"),
            file_id: None,
            row_pk: RowPk::single("row-0"),
        };
        let value = |change_id: ChangeId, deleted: bool| TrackedStateIndexValue {
            change_id,
            commit_id,
            deleted,
            created_at,
            updated_at,
        };
        let entry = |before: Option<TrackedStateIndexValue>| {
            let mut batch = TrackedStateTreeDiffBatchBuilder::with_row_capacity(1);
            batch.push_shared(key(), before, Some(value(after_change_id, false)));
            let batch = batch.finish().expect("tree batch should seal");
            classify_tree_diff_batch(batch, &TrackedStatePayloadBatch::default())
                .expect("rows should classify")
                .pop()
                .expect("one entry")
        };

        let never_present = entry(None);
        let tombstoned =
            entry(Some(value(ChangeId::for_test_label("checkpointed-delete"), true)));

        // The fixture is only meaningful if the tombstone actually survives on
        // the entry — otherwise this would pass vacuously against a before-image
        // that was already dropped upstream.
        assert!(
            tombstoned
                .before
                .as_ref()
                .is_some_and(|row| row.deleted),
            "fixture must carry a tombstoned before row"
        );
        assert!(never_present.before.is_none());
        assert_eq!(never_present.kind, TrackedStateDiffKind::Added);
        assert_eq!(tombstoned.kind, TrackedStateDiffKind::Added);

        assert_eq!(
            tombstoned.diff_id().expect("tombstoned diff id"),
            never_present.diff_id().expect("absent diff id"),
            "a retained tombstone must not change the diff_id"
        );

        // Guard the other direction: normalization must not swallow a *live*
        // before row, which is a genuinely different change.
        let modified = entry(Some(value(
            ChangeId::for_test_label("live-before"),
            false,
        )));
        assert_eq!(modified.kind, TrackedStateDiffKind::Modified);
        assert_ne!(
            modified.diff_id().expect("modified diff id"),
            never_present.diff_id().expect("absent diff id"),
            "a live before row must still contribute to the diff_id"
        );
    }

    /// The after side must stay raw: a `Removed` entry's after row *is* the
    /// tombstone, and merge relies on it. If normalization ever leaks across to
    /// `after`, every delete would encode as a sideless diff and fail to encode
    /// at all.
    #[test]
    fn diff_id_keeps_a_tombstoned_after_row() {
        let created_at = ts("2024-01-01T00:00:00.000Z");
        let updated_at = ts("2024-01-02T00:00:00.000Z");
        let commit_id = CommitId::for_test_label("diff-id-commit");
        let mut batch = TrackedStateTreeDiffBatchBuilder::with_row_capacity(1);
        batch.push_shared(
            DecodedTrackedStateKeyShared {
                schema_key: SharedStr::from_static("test_schema"),
                file_id: None,
                row_pk: RowPk::single("row-0"),
            },
            Some(TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("live-before"),
                commit_id,
                deleted: false,
                created_at,
                updated_at,
            }),
            Some(TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("delete-after"),
                commit_id,
                deleted: true,
                created_at,
                updated_at,
            }),
        );
        let batch = batch.finish().expect("tree batch should seal");
        let entry = classify_tree_diff_batch(batch, &TrackedStatePayloadBatch::default())
            .expect("rows should classify")
            .pop()
            .expect("one entry");
        assert_eq!(entry.kind, TrackedStateDiffKind::Removed);
        assert!(entry.after.as_ref().is_some_and(|row| row.deleted));
        let sides = crate::tracked_state::decode_diff_id(&entry.diff_id().expect("removed diff id"))
            .expect("removed diff id should decode");
        assert_eq!(
            sides.after,
            Some(ChangeId::for_test_label("delete-after")),
            "the after tombstone must survive into the diff_id"
        );
        assert_eq!(sides.before, Some(ChangeId::for_test_label("live-before")));
    }

    #[test]
    fn ten_thousand_tree_diff_rows_share_one_ordered_identity_batch() {
        let row_count = 10_000;
        let created_at = ts("2024-01-01T00:00:00.000Z");
        let updated_at = ts("2024-01-02T00:00:00.000Z");
        let change_id = ChangeId::for_test_label("shared-batch-change");
        let commit_id = CommitId::for_test_label("shared-batch-commit");
        let mut tree_entries = TrackedStateTreeDiffBatchBuilder::with_row_capacity(row_count);
        for index in 0..row_count {
            tree_entries.push_shared(
                DecodedTrackedStateKeyShared {
                    schema_key: SharedStr::from_static("test_schema"),
                    file_id: Some(SharedStr::from_static(
                        "01920000-0000-7000-8000-0000000000a2",
                    )),
                    row_pk: RowPk::single(format!("row-{index:05}")),
                },
                None,
                Some(TrackedStateIndexValue {
                    change_id,
                    commit_id,
                    deleted: false,
                    created_at,
                    updated_at,
                }),
            );
        }
        let tree_entries = tree_entries.finish().expect("tree batch should seal");
        assert_eq!(tree_entries.large_buffer_count(), 3);
        let rows = classify_tree_diff_batch(tree_entries, &TrackedStatePayloadBatch::default())
            .expect("tree rows should classify");
        assert_eq!(rows.len(), row_count);
        let first = &rows[0].identity;
        assert_eq!(first.batch_len(), row_count);
        assert_eq!(
            first.batch_dictionary_counts(),
            (1, 1),
            "batch-wide schema and file metadata must be stored once"
        );
        for (index, entry) in rows.iter().enumerate() {
            let identity = &entry.identity;
            assert!(
                first.shares_batch_with(identity),
                "row {index} allocated a different identity owner"
            );
            assert!(entry.before.is_none());
            assert!(
                entry
                    .after
                    .as_ref()
                    .is_some_and(|row| identity.shares_key_with(&row.identity)),
                "row {index} side did not retain its compact identity handle"
            );
            assert_eq!(
                identity
                    .row_pk()
                    .as_single_string_owned()
                    .expect("single identity"),
                format!("row-{index:05}")
            );
            if index > 0 {
                assert!(rows[index - 1].identity < *identity);
            }
        }

        let expected_last_key = rows[row_count - 1].identity.clone().into_key();
        let (lowered_last_key, lowered_last_value) = rows[row_count - 1]
            .after
            .clone()
            .expect("last after row")
            .into_index_entry();
        assert_eq!(lowered_last_key, expected_last_key);
        assert_eq!(lowered_last_value.change_id, change_id);
    }

    #[test]
    fn ten_thousand_borrowed_keys_intern_batch_metadata_once() {
        let row_pks = (0..10_000)
            .map(|index| RowPk::single(format!("row-{index:05}")))
            .collect::<Vec<_>>();
        let identities =
            TrackedStateDiffIdentity::from_key_refs(row_pks.len(), |index| TrackedStateKeyRef {
                schema_key: "shared_schema",
                file_id: Some("shared_file"),
                row_pk: &row_pks[index],
            })
            .expect("borrowed identity batch should seal");

        assert_eq!(identities.len(), row_pks.len());
        assert_eq!(identities[0].batch_len(), row_pks.len());
        assert_eq!(identities[0].batch_dictionary_counts(), (1, 1));
        let capacities = identities[0].batch_dictionary_capacities();
        assert!(
            capacities.0 <= DIFF_SMALL_STRING_DICTIONARY_LIMIT
                && capacities.1 <= DIFF_SMALL_STRING_DICTIONARY_LIMIT,
            "repeated dictionaries retained row-sized capacity: {capacities:?}"
        );
        for identity in &identities[1..] {
            assert!(identities[0].shares_batch_with(identity));
        }
    }

    #[test]
    fn ten_thousand_shared_decoded_keys_retain_one_arena_and_tiny_dictionaries() {
        let row_count = 10_000;
        let mut encoded_arena = Vec::new();
        let mut ranges = Vec::with_capacity(row_count);
        for index in 0..row_count {
            let encoded = crate::tracked_state::codec::encode_key(&TrackedStateKey {
                schema_key: "shared_schema".to_string(),
                file_id: Some("shared_file".to_string()),
                row_pk: RowPk::single(format!("row-{index:05}")),
            });
            let start = encoded_arena.len();
            encoded_arena.extend_from_slice(&encoded);
            ranges.push(start..encoded_arena.len());
        }
        let encoded_arena = bytes::Bytes::from(encoded_arena);
        let arena_start = encoded_arena.as_ptr() as usize;
        let arena_end = arena_start + encoded_arena.len();
        let timestamp = ts("2024-01-01T00:00:00.000Z");
        let mut builder = TrackedStateTreeDiffBatchBuilder::with_row_capacity(row_count);
        for (index, range) in ranges.into_iter().enumerate() {
            let key = crate::tracked_state::codec::decode_key_shared(encoded_arena.slice(range))
                .expect("shared tree key should decode");
            builder.push_shared(
                key,
                None,
                Some(TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label(&format!("change-{index:05}")),
                    commit_id: CommitId::for_test_label("shared-tree-commit"),
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                }),
            );
        }
        let batch = builder.finish().expect("shared tree batch should seal");

        assert_eq!(batch.len(), row_count);
        assert_eq!(batch.large_buffer_count(), 3);
        let identities = batch.identities.as_ref().expect("identity columns");
        let TrackedStateDiffKeyStorage::Batch(keys) = &identities.keys else {
            panic!("tree diff must use batch identity columns");
        };
        assert_eq!((keys.schema_keys.len(), keys.file_ids.len()), (1, 1));
        assert!(
            keys.schema_keys.capacity() <= DIFF_SMALL_STRING_DICTIONARY_LIMIT
                && keys.file_ids.capacity() <= DIFF_SMALL_STRING_DICTIONARY_LIMIT
        );
        for row in &keys.rows {
            for component in row.row_pk.components.iter() {
                if let crate::row_pk::RowPkComponent::String(value) = component {
                    let (pointer, len) = value.retained_buffer_identity();
                    let start = pointer as usize;
                    assert!(
                        start >= arena_start && start.saturating_add(len) <= arena_end,
                        "row key escaped the shared decoded arena"
                    );
                }
            }
        }
    }

    #[test]
    fn ten_thousand_payloads_use_one_arc_owned_column_batch() {
        let payloads = TrackedStatePayloadBatch::from_payloads((0..10_000).map(|index| {
            (
                ChangeId::for_test_label(&format!("payload-{index:05}")),
                JsonSlot::from_json(&format!("{{\"snapshot\":{index}}}")),
                JsonSlot::None,
            )
        }))
        .expect("payload batch should seal");
        let cloned = payloads.clone();

        assert_eq!(payloads.len(), 10_000);
        assert_eq!(
            payloads.large_buffer_count(),
            4,
            "payload row count must not change the number of column buffers"
        );
        assert!(payloads.shares_owner_with(&cloned));
        let last_id = ChangeId::for_test_label("payload-09999");
        let last = cloned.get(last_id).expect("last payload should index");
        assert_eq!(last.change_id, last_id);
        assert_eq!(last.snapshot, &JsonSlot::from_json("{\"snapshot\":9999}"));
    }

    #[test]
    fn ten_thousand_merge_picks_retain_the_source_identity_batch() {
        let row_count = 10_000;
        let timestamp = ts("2024-01-01T00:00:00.000Z");
        let change_id = ChangeId::for_test_label("merge-batch-change");
        let commit_id = CommitId::for_test_label("merge-batch-commit");
        let mut tree_entries = TrackedStateTreeDiffBatchBuilder::with_row_capacity(row_count);
        for index in 0..row_count {
            tree_entries.push_shared(
                DecodedTrackedStateKeyShared {
                    schema_key: SharedStr::from_static("test_schema"),
                    file_id: None,
                    row_pk: RowPk::single(format!("row-{index:05}")),
                },
                None,
                Some(TrackedStateIndexValue {
                    change_id,
                    commit_id,
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                }),
            );
        }
        let entries = classify_tree_diff_batch(
            tree_entries.finish().expect("tree batch should seal"),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("tree rows should classify");
        let source = TrackedStateDiff::from_entries(entries);
        let source_batch = source.entries[0].identity.clone();

        let plan = crate::tracked_state::merge::plan_merge(
            &TrackedStateDiff::default(),
            &source,
            &TrackedStatePayloadBatch::default(),
        )
        .expect("source-only merge should plan");
        drop(source);

        assert_eq!(plan.picks.len(), row_count);
        assert_eq!(
            plan.picks.large_buffer_count(),
            1,
            "merge pick metadata must use one contiguous descriptor buffer"
        );
        assert!(plan.conflicts.is_empty());
        for (index, pick) in plan.picks.iter().enumerate() {
            assert!(
                source_batch.shares_batch_with(&pick.identity),
                "merge pick {index} allocated a new identity owner"
            );
            assert!(
                pick.identity.shares_key_with(&pick.selected_row.identity),
                "merge pick {index} did not retain the same ordinal for its selected row"
            );
        }
    }

    #[tokio::test]
    async fn diff_commits_reports_added_rows() {
        let (storage, tracked_state) = seed_roots(&[], &[row("row-a", None, "after")]).await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Added)]
        );
        assert!(diff.entries[0].before.is_none());
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("after"))
        );
        assert!(!diff.entries[0].before_is_live());
        assert!(diff.entries[0].after_is_live());
        let payload_owner = diff.payloads().clone();
        let after_change_id = diff.entries[0].after.as_ref().expect("added row").change_id;
        assert!(
            payload_owner.get(after_change_id).is_some(),
            "diff must retain the payload loaded during row validation"
        );
        assert!(payload_owner.shares_owner_with(diff.payloads()));
    }

    #[tokio::test]
    async fn diff_commits_reports_removed_rows_when_right_side_is_absent() {
        let (storage, tracked_state) = seed_roots(&[row("row-a", None, "before")], &[]).await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        assert_eq!(
            diff.entries[0]
                .before
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("before"))
        );
        assert!(diff.entries[0].after.is_none());
        assert!(diff.entries[0].before_is_live());
        assert!(!diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_removed_rows_when_right_side_is_tombstone() {
        let (storage, tracked_state) = seed_roots(
            &[row("row-a", None, "before")],
            &[tombstone("row-a", None, "delete")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        let entry = &diff.entries[0];
        assert_eq!(
            entry.after.as_ref().map(|row| row.change_id.to_string()),
            Some(change_id("delete"))
        );
        assert!(
            entry.after.as_ref().is_some_and(|row| row.deleted),
            "removed diff should preserve the right-side tombstone for merge"
        );
        assert!(entry.before_is_live());
        assert!(!entry.after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_added_rows_when_left_side_is_tombstone() {
        let (storage, tracked_state) = seed_roots(
            &[tombstone("row-a", None, "delete")],
            &[row("row-a", None, "after")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Added)]
        );
        let entry = &diff.entries[0];
        assert_eq!(
            entry.before.as_ref().map(|row| row.change_id.to_string()),
            Some(change_id("delete"))
        );
        assert!(
            entry.before.as_ref().is_some_and(|row| row.deleted),
            "added diff should preserve the left-side tombstone for merge"
        );
        assert!(!entry.before_is_live());
        assert!(entry.after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_reports_modified_rows_for_changed_payload() {
        let (storage, tracked_state) = seed_roots(
            &[row_with_value("row-a", None, "before", "one")],
            &[row_with_value("row-a", None, "after", "two")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert!(diff.entries[0].before_is_live());
        assert!(diff.entries[0].after_is_live());
    }

    #[tokio::test]
    async fn diff_commits_omits_unchanged_rows_even_when_metadata_differs_only_by_commit() {
        let (storage, tracked_state) = seed_roots(
            &[row_with_value("row-a", None, "before", "same")],
            &[row_with_value("row-a", None, "after", "same")],
        )
        .await;

        let diff = diff(&storage, &tracked_state).await;

        assert!(diff.entries.is_empty());
    }

    #[tokio::test]
    async fn diff_commits_distinguishes_same_row_with_different_file_id() {
        let (storage, tracked_state) = seed_parent_child_delta(
            &[row(
                "row-a",
                Some("01920000-0000-7000-8000-0000000000a2"),
                "before-a",
            )],
            &[row(
                "row-a",
                Some("01920000-0000-7000-8000-0000000000b2"),
                "after-b",
            )],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].identity.file_id(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );
        assert_eq!(diff.entries[0].kind, TrackedStateDiffKind::Added);
    }

    #[tokio::test]
    async fn diff_commits_filters_by_schema_row_and_file_id() {
        let (storage, tracked_state) = seed_roots(
            &[],
            &[
                row_with_schema(
                    "row-a",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    "schema-a",
                    "change-a",
                ),
                row_with_schema(
                    "row-b",
                    Some("01920000-0000-7000-8000-0000000000b2"),
                    "schema-b",
                    "change-b",
                ),
            ],
        )
        .await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = tracked_state.reader(read);
        let diff = reader
            .diff_commits(
                "left",
                "right",
                &TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema-b".to_string()],
                        row_pks: vec![RowPk::single("row-b")],
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("row-b".to_string(), TrackedStateDiffKind::Added)]
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_row_identity_that_does_not_match_changelog_change() {
        let (storage, tracked_state) = seed_roots(&[], &[row("row-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0].after.as_mut().expect("after row").identity =
            TrackedStateDiffIdentity::from_key(TrackedStateKey {
                schema_key: "test_schema".to_owned(),
                file_id: None,
                row_pk: RowPk::single("row-corrupt"),
            });

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("identity drift must be rejected");

        assert!(
            error
                .message
                .contains("does not match changelog change identity")
                || error.message.contains("changelog commit")
                || error.message.contains("has no authoritative payload"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_missing_changelog_change() {
        let (storage, tracked_state) = seed_roots(&[], &[row("row-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0].after.as_mut().expect("after row").change_id =
            ChangeId::for_test_label("missing-change");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("missing change must be rejected");

        assert!(
            error.message.contains("resolves to payload"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_forged_updated_at() {
        let (storage, tracked_state) = seed_roots(&[], &[row("row-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0]
            .after
            .as_mut()
            .expect("after row")
            .updated_at = ts("2026-01-02T00:00:00Z");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("forged updated_at must be rejected");

        assert!(
            error.message.contains("updated_at does not match"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_validation_rejects_forged_created_at() {
        let (storage, tracked_state) = seed_roots(&[], &[row("row-a", None, "after")]).await;
        let mut diff = diff(&storage, &tracked_state).await;
        diff.entries[0]
            .after
            .as_mut()
            .expect("after row")
            .created_at = ts("2025-12-31T00:00:00Z");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .validate_diff_rows_for_commits_against_changelog(&[(
                diff.entries[0].after.as_ref().expect("after row"),
                "right",
            )])
            .await
            .expect_err("forged created_at must be rejected");

        assert!(
            error.message.contains("created_at"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_update_with_arbitrary_forged_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_times(
                "row-a",
                None,
                "parent-change",
                "old",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_times(
                "row-a",
                None,
                "child-change",
                "new",
                "2026-01-02T00:00:00Z",
                "2026-01-02T00:00:00Z",
            )],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid update should load");
        let row = valid_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("child row should appear");
        let (key, mut value) = row.into_index_entry();
        let updated_at = value.updated_at().to_string();
        value.created_at = LixTimestamp::expect_parse("created_at", "2026-01-03T00:00:00Z");
        value.updated_at = LixTimestamp::expect_parse("updated_at", &updated_at);
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(key, value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("arbitrary forged created_at must be rejected");

        assert!(
            error.message.contains("created_at"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_validates_same_payload_rows_before_classification_drops_them() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "left",
            None,
            &[row_with_value("row-a", None, "left-a", "same")],
        )
        .await
        .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "right-valid",
            None,
            &[row_with_value("row-b", None, "right-b", "same")],
        )
        .await
        .expect("right changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("left", "right-valid", &TrackedStateDiffRequest::default())
            .await
            .expect("valid diff should load");
        let source_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("right row should appear in valid diff");
        let (_source_key, source_value) = source_row.into_index_entry();
        let corrupt_key = TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            row_pk: RowPk::single("row-a"),
        };
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("corrupt commit read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "right-corrupt",
                None,
            )
            .await
            .expect("corrupt commit authority should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("corrupt commit authority should commit");
        }
        let result = {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            let result = crate::tracked_state::tree::TrackedStateTree::new()
                .apply_mutations(
                    &mut read,
                    &mut writes,
                    None,
                    crate::tracked_state::types::TrackedStateMutationBatch::from_shared(vec![
                        TrackedStateMutation::put_encoded(
                            crate::tracked_state::codec::encode_key(&corrupt_key),
                            crate::tracked_state::codec::encode_value(&source_value),
                        ),
                    ]),
                    Some("right-corrupt"),
                )
                .await
                .expect("corrupt root should write");
            stage_snapshot_authority_for_test(
                &read,
                &mut writes,
                &TrackedStateCommitRoot {
                    commit_id: CommitId::for_test_label("right-corrupt"),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 1,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                },
            )
            .await
            .expect("metadata should encode");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("corrupt root should commit");
            result
        };
        assert_eq!(result.row_count, 1);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right-corrupt", &TrackedStateDiffRequest::default())
            .await
            .expect_err("raw same-payload corruption must be rejected before classification");

        assert!(
            error
                .message
                .contains("does not match changelog change identity")
                || error.message.contains("changelog commit")
                || error.message.contains("has no authoritative payload"),
            "unexpected error: {error}"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits(
                "left",
                "right-corrupt",
                &TrackedStateDiffRequest {
                    retain_payloads: false,
                    ..TrackedStateDiffRequest::default()
                },
            )
            .await
            .expect_err("identity-only diff must validate the packed delta leaf");
        assert!(
            error.message.contains("delta index") || error.message.contains("changelog commit"),
            "unexpected identity-only validation error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_stale_ancestor_row_that_is_not_root_winner() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("row-a", None, "parent-change", "old")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("row-a", None, "child-change", "new")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let parent_diff = tracked_state
            .reader(read)
            .diff_commits("left", "parent", &TrackedStateDiffRequest::default())
            .await
            .expect("parent diff should load");
        let stale_row = parent_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("parent row should appear");
        let (stale_key, stale_value) = stale_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(stale_key, stale_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("stale ancestor winner must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_valid_change_from_unreachable_commit_root() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "unrelated",
            None,
            &[row_with_value(
                "row-a",
                None,
                "unrelated-change",
                "value",
            )],
        )
        .await
        .expect("unrelated changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let unrelated_diff = tracked_state
            .reader(read)
            .diff_commits("left", "unrelated", &TrackedStateDiffRequest::default())
            .await
            .expect("valid unrelated diff should load");
        let source_row = unrelated_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("unrelated row should appear in valid diff");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "right-corrupt",
                None,
            )
            .await
            .expect("empty right changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("empty right changelog should commit");
        };
        let result = {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            let result = crate::tracked_state::tree::TrackedStateTree::new()
                .apply_mutations(
                    &mut read,
                    &mut writes,
                    None,
                    crate::tracked_state::types::TrackedStateMutationBatch::from_shared(vec![
                        TrackedStateMutation::put_encoded(
                            crate::tracked_state::codec::encode_key(&source_key),
                            crate::tracked_state::codec::encode_value(&source_value),
                        ),
                    ]),
                    Some("right-corrupt"),
                )
                .await
                .expect("corrupt root should write");
            stage_snapshot_authority_for_test(
                &read,
                &mut writes,
                &TrackedStateCommitRoot {
                    commit_id: CommitId::for_test_label("right-corrupt"),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 1,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                },
            )
            .await
            .expect("metadata should encode");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("corrupt root should commit");
            result
        };
        assert_eq!(result.row_count, 1);

        let error = audit_root(&storage, &tracked_state, "right-corrupt")
            .await
            .expect_err("unreachable valid change must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_second_parent_row_without_commit_root_proof() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("row-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit_with_parents(
                &mut read,
                &mut writes,
                "merge",
                &["target".to_string(), "source".to_string()],
            )
            .await
            .expect("merge changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("merge changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "merge",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("target"),
                root_id: tracked_state_root_id(&storage, "target").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "merge")
            .await
            .expect_err("second-parent row without commit-root proof must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_second_parent_row_with_forged_commit_root_parent() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("row-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit_with_parents(
                &mut read,
                &mut writes,
                "merge",
                &["target".to_string(), "source".to_string()],
            )
            .await
            .expect("merge changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("merge changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "merge",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("source"),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "merge", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged source parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_unrelated_row_with_forged_commit_root_parent() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("row-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits("left", "source", &TrackedStateDiffRequest::default())
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "right-corrupt",
                None,
            )
            .await
            .expect("empty right changelog should write");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("right changelog should commit");
        }
        stage_corrupt_commit_root(
            &storage,
            "right-corrupt",
            vec![(source_key, source_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("source"),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "right-corrupt", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged unrelated parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_forged_parent_metadata_even_for_current_winner_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("row-b", None, "source-b", "source")],
        )
        .await
        .expect("source root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("target"),
            &[row_with_value("row-a", None, "child-a", "current")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let child_diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("child diff should load");
        let child_row = child_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("child row should appear");
        let (child_key, child_value) = child_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(child_key, child_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("source"),
                root_id: tracked_state_root_id(&storage, "source").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("current winner root metadata must still be validated");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_rejects_stale_grandparent_row_with_forged_commit_root_parent() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "grandparent",
            None,
            &[row_with_value("row-a", None, "grandparent-a", "old")],
        )
        .await
        .expect("grandparent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            Some("grandparent"),
            &[row_with_value("row-a", None, "parent-a", "new")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(&storage, &tracked_state, "child", Some("parent"), &[])
            .await
            .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let stale_diff = tracked_state
            .reader(read)
            .diff_commits("left", "grandparent", &TrackedStateDiffRequest::default())
            .await
            .expect("grandparent diff should load");
        let stale_row = stale_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("grandparent row should appear");
        let (stale_key, stale_value) = stale_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(stale_key, stale_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("grandparent"),
                root_id: tracked_state_root_id(&storage, "grandparent").await,
            }],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect_err("forged grandparent parent must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_allows_rows_reachable_through_parent_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, &[])
            .await
            .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("row-a", None, "parent-change", "value")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(&storage, &tracked_state, "child", Some("parent"), &[])
            .await
            .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("left", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("ancestor-reachable row should validate");

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Added)]
        );
    }

    #[tokio::test]
    async fn diff_commits_allows_source_update_with_source_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "target", None, &[])
            .await
            .expect("target root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source-add",
            None,
            &[row_with_times(
                "row-a",
                None,
                "source-add-a",
                "old",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await
        .expect("source add root should write");
        let mut source_update = row_with_times(
            "row-a",
            None,
            "source-update-a",
            "new",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );
        source_update.commit_id = CommitId::for_test_label("source-update");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source-update",
            Some("source-add"),
            std::slice::from_ref(&source_update),
        )
        .await
        .expect("source update root should write");
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_tracked_root_from_materialized_with_parents(
                &mut read,
                &mut writes,
                &tracked_state,
                "merge",
                &["target".to_string(), "source-update".to_string()],
                Some("target"),
                std::slice::from_ref(&source_update),
            )
            .await
            .expect("merge root should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("merge root should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("target", "merge", &TrackedStateDiffRequest::default())
            .await
            .expect("source update should validate");

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Added)]
        );
        let row = diff.entries[0].after.as_ref().expect("after row");
        assert_eq!(row.created_at.to_string(), "2026-01-01T00:00:00.000Z");
        assert_eq!(row.updated_at.to_string(), "2026-01-02T00:00:00.000Z");
        assert_eq!(row.change_id, "source-update-a");
    }

    #[tokio::test]
    async fn full_root_audit_rejects_omitted_inherited_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("row-a", None, "parent-a", "inherited")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("row-b", None, "child-b", "unrelated")],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid child diff should load");
        let unrelated_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "child-b")
                    .cloned()
            })
            .expect("unrelated child row should appear");
        let (unrelated_key, unrelated_value) = unrelated_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(unrelated_key, unrelated_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("omitted inherited row must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_omitted_updated_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("row-a", None, "parent-a", "old")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[
                row_with_value("row-a", None, "child-a", "new"),
                row_with_value("row-b", None, "child-b", "unrelated"),
            ],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let valid_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("valid child diff should load");
        let unrelated_row = valid_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "child-b")
                    .cloned()
            })
            .expect("unrelated child row should appear");
        let (unrelated_key, unrelated_value) = unrelated_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "child",
            vec![(unrelated_key, unrelated_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("omitted updated row must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_shared_omitted_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row_with_value("row-a", None, "parent-a", "shared")],
        )
        .await
        .expect("parent root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "left",
            Some("parent"),
            &[row_with_value("row-b", None, "left-b", "left")],
        )
        .await
        .expect("left root should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "right",
            Some("parent"),
            &[row_with_value("row-c", None, "right-c", "right")],
        )
        .await
        .expect("right root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let left_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "left", &TrackedStateDiffRequest::default())
            .await
            .expect("left diff should load");
        let left_row = left_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "left-b")
                    .cloned()
            })
            .expect("left row should appear");
        let (left_key, left_value) = left_row.into_index_entry();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let right_diff = tracked_state
            .reader(read)
            .diff_commits("parent", "right", &TrackedStateDiffRequest::default())
            .await
            .expect("right diff should load");
        let right_row = right_diff
            .entries
            .iter()
            .find_map(|entry| {
                entry
                    .after
                    .as_ref()
                    .filter(|row| row.change_id == "right-c")
                    .cloned()
            })
            .expect("right row should appear");
        let (right_key, right_value) = right_row.into_index_entry();
        stage_corrupt_commit_root(
            &storage,
            "left",
            vec![(left_key, left_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;
        stage_corrupt_commit_root(
            &storage,
            "right",
            vec![(right_key, right_value)],
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: tracked_state_root_id(&storage, "parent").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "left")
            .await
            .expect_err("shared hidden omission must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_validates_even_when_tree_diff_is_empty() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "source",
            None,
            &[row_with_value("row-a", None, "source-change", "value")],
        )
        .await
        .expect("source root should write");
        write_root_committed_for_test(&storage, &tracked_state, "left-corrupt", None, &[])
            .await
            .expect("left changelog should write");
        write_root_committed_for_test(&storage, &tracked_state, "right-corrupt", None, &[])
            .await
            .expect("right changelog should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let source_diff = tracked_state
            .reader(read)
            .diff_commits(
                "left-corrupt",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("source diff should load");
        let source_row = source_diff
            .entries
            .iter()
            .find_map(|entry| entry.after.clone())
            .expect("source row should appear");
        let (source_key, source_value) = source_row.into_index_entry();

        stage_corrupt_commit_root(
            &storage,
            "left-corrupt",
            vec![(source_key.clone(), source_value.clone())],
            Vec::new(),
        )
        .await;
        stage_corrupt_commit_root(
            &storage,
            "right-corrupt",
            vec![(source_key, source_value)],
            Vec::new(),
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "left-corrupt")
            .await
            .expect_err("identical corrupt roots must still be validated");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn full_root_audit_rejects_forged_parent_metadata_on_empty_root() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "parent", None, &[])
            .await
            .expect("parent root should write");
        write_root_committed_for_test(&storage, &tracked_state, "unrelated", None, &[])
            .await
            .expect("unrelated root should write");
        write_root_committed_for_test(&storage, &tracked_state, "child", Some("parent"), &[])
            .await
            .expect("child root should write");

        stage_corrupt_commit_root(
            &storage,
            "child",
            Vec::new(),
            vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("unrelated"),
                root_id: tracked_state_root_id(&storage, "unrelated").await,
            }],
        )
        .await;

        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("forged empty-root parent metadata must be rejected");

        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );

        stage_corrupt_commit_root(
            &storage,
            "child",
            Vec::new(),
            vec![
                TrackedStateCommitRootParent {
                    commit_id: CommitId::for_test_label("parent"),
                    root_id: tracked_state_root_id(&storage, "parent").await,
                },
                TrackedStateCommitRootParent {
                    commit_id: CommitId::for_test_label("unrelated"),
                    root_id: tracked_state_root_id(&storage, "unrelated").await,
                },
            ],
        )
        .await;
        let error = audit_root(&storage, &tracked_state, "child")
            .await
            .expect_err("extra commit-root parents must be rejected");
        assert!(
            is_commit_root_validation_error(&error),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_parent_and_child_reports_suffix_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
            &mut writes,
            &tracked_state,
            "parent",
            None,
            &[
                row_with_value("row-a", None, "parent-a", "before"),
                row_with_value("row-b", None, "parent-b", "same"),
            ],
        )
        .await
        .expect("parent should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("parent writes should commit");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("child read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
            &mut writes,
            &tracked_state,
            "child",
            Some("parent"),
            &[row_with_value("row-a", None, "child-a", "after")],
        )
        .await
        .expect("child should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert_ne!(
            diff.entries[0].before.as_ref().map(|row| row.change_id),
            diff.entries[0].after.as_ref().map(|row| row.change_id)
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_child_and_parent_reports_reverse_suffix_rows() {
        let (storage, tracked_state) = seed_parent_child_delta(
            &[
                row_with_value("row-a", None, "parent-a", "before"),
                row_with_value("row-b", None, "parent-b", "same"),
            ],
            &[row_with_value("row-a", None, "child-a", "after")],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("child", "parent", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Modified)]
        );
        assert_ne!(
            diff.entries[0].before.as_ref().map(|row| row.change_id),
            diff.entries[0].after.as_ref().map(|row| row.change_id)
        );
    }

    #[tokio::test]
    async fn diff_commits_between_delta_parent_and_child_preserves_suffix_tombstones() {
        let (storage, tracked_state) = seed_parent_child_delta(
            &[
                row_with_value("row-a", None, "parent-a", "before"),
                row_with_value("row-b", None, "parent-b", "same"),
            ],
            &[tombstone("row-a", None, "child-delete")],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let diff = tracked_state
            .reader(read)
            .diff_commits("parent", "child", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load");

        assert_eq!(
            kinds(&diff),
            vec![("row-a".to_string(), TrackedStateDiffKind::Removed)]
        );
        assert!(diff.entries[0].before_is_live());
        assert!(!diff.entries[0].after_is_live());
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("child-delete"))
        );
    }

    async fn diff(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
    ) -> TrackedStateDiff {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        tracked_state
            .reader(read)
            .diff_commits("left", "right", &TrackedStateDiffRequest::default())
            .await
            .expect("diff should load")
    }

    async fn audit_root(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        tracked_state
            .reader(read)
            .validate_commit_root_against_changelog(commit_id)
            .await
    }

    async fn seed_roots(
        left_rows: &[MaterializedTrackedStateRow],
        right_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageAdapter, TrackedStateContext) {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "left", None, left_rows)
            .await
            .expect("left root should write");
        write_root_committed_for_test(&storage, &tracked_state, "right", None, right_rows)
            .await
            .expect("right root should write");
        (storage, tracked_state)
    }

    async fn seed_parent_child_delta(
        parent_rows: &[MaterializedTrackedStateRow],
        child_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageAdapter, TrackedStateContext) {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_committed_for_test(&storage, &tracked_state, "parent", None, parent_rows)
            .await
            .expect("parent should write");
        write_root_committed_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            child_rows,
        )
        .await
        .expect("child should write");
        (storage, tracked_state)
    }

    async fn write_root_committed_for_test(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        write_root_for_test(
            &mut read,
            &mut writes,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(())
    }

    async fn write_root_for_test(
        read: &mut (impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        crate::test_support::stage_tracked_root_from_materialized(
            read,
            writes,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await
    }

    async fn tracked_state_root_id(
        storage: &StorageAdapter,
        commit_id: &str,
    ) -> TrackedStateRootId {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        crate::tracked_state::storage::load_root(&read, commit_id)
            .await
            .expect("root should load")
            .expect("root should exist")
    }

    async fn stage_corrupt_commit_root(
        storage: &StorageAdapter,
        commit_id: &str,
        entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
        parent_roots: Vec<TrackedStateCommitRootParent>,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mutations = entries
            .into_iter()
            .map(|(key, value)| {
                TrackedStateMutation::put_encoded(
                    crate::tracked_state::codec::encode_key(&key),
                    crate::tracked_state::codec::encode_value(&value),
                )
            })
            .collect::<Vec<_>>();
        let changed_key_count = mutations.len() as u64;
        let result = crate::tracked_state::tree::TrackedStateTree::new()
            .apply_mutations(
                &read,
                &mut writes,
                None,
                crate::tracked_state::types::TrackedStateMutationBatch::from_shared(mutations),
                Some(commit_id),
            )
            .await
            .expect("corrupt root should write");
        stage_snapshot_authority_for_test(
            &read,
            &mut writes,
            &TrackedStateCommitRoot {
                commit_id: CommitId::for_test_label(commit_id),
                root_id: result.root_id,
                parent_roots,
                changed_key_count,
                row_count_estimate: result.row_count as u64,
                tree_height: result.tree_height as u32,
            },
        )
        .await
        .expect("metadata should encode");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt root should commit");
    }

    fn kinds(diff: &TrackedStateDiff) -> Vec<(String, TrackedStateDiffKind)> {
        diff.entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .identity
                        .row_pk()
                        .as_single_string_owned()
                        .expect("identity"),
                    entry.kind,
                )
            })
            .collect()
    }

    fn is_commit_root_validation_error(error: &LixError) -> bool {
        error.message.contains("not the first-parent winner")
            || error.message.contains("does not match parent root")
            || error.message.contains("snapshot ancestry disagrees")
            || error
                .message
                .contains("does not match changelog first-parent winners")
            || error.message.contains("contains non-winner identity")
            || error.message.contains("but changelog first parent is")
            || error.message.contains("but its first parent is")
            || error.message.contains("more than one first-parent root")
            || error
                .message
                .contains("nearest available first-parent root")
            || error.message.contains("references unexpected parent")
            || error.message.contains("missing changelog winner")
            || error.message.contains("has change")
            || error.message.contains("omits current changelog change")
            || error.message.contains("omits inherited identity")
            || error
                .message
                .contains("does not preserve inherited identity")
            || error.message.contains("but changelog winner is")
    }

    fn tombstone(
        row_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        let mut row = row(row_pk, file_id, change_id);
        row.snapshot_content = None;
        row.deleted = true;
        row
    }

    fn row(row_pk: &str, file_id: Option<&str>, change_id: &str) -> MaterializedTrackedStateRow {
        row_with_schema(row_pk, file_id, "test_schema", change_id)
    }

    fn row_with_schema(
        row_pk: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(row_pk, file_id, schema_key, change_id, "value")
    }

    fn row_with_value(
        row_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        row_with_schema_and_value(row_pk, file_id, "test_schema", change_id, value)
    }

    fn row_with_times(
        row_pk: &str,
        file_id: Option<&str>,
        change_id: &str,
        value: &str,
        created_at: &str,
        updated_at: &str,
    ) -> MaterializedTrackedStateRow {
        let mut row = row_with_value(row_pk, file_id, change_id, value);
        row.created_at = created_at.to_string();
        row.updated_at = updated_at.to_string();
        row
    }

    fn row_with_schema_and_value(
        row_pk: &str,
        file_id: Option<&str>,
        schema_key: &str,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            row_pk: RowPk::single(row_pk),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(&change_id.replace("change", "commit")),
        }
    }
}
