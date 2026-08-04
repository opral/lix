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
#[cfg(test)]
use crate::tracked_state::types::TrackedStateKey;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKeyRef, TrackedStatePhysicalScanRequest,
};
use crate::tracked_state::{TrackedStateFilter, TrackedStateStoreReader};

/// Filter for comparing two tracked-state commit roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateDiffRequest {
    pub(crate) filter: TrackedStateFilter,
}

impl Default for TrackedStateDiffRequest {
    fn default() -> Self {
        Self {
            filter: TrackedStateFilter::default(),
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
    /// typed identity avoids copying schema/file/entity buffers into every
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
/// schema key, file id, entity pk, or per-key heap owner.
#[derive(Debug)]
struct TrackedStateDiffIdentityBatch {
    keys: TrackedStateDiffKeyColumns,
}

/// Dictionary-encoded identity columns for one diff batch.
///
/// Repeated schema and file identifiers live once in their dictionaries.
/// Every logical key row is one compact ordinal pair plus the typed entity pk.
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
    entity_pk: crate::entity_pk::EntityPk,
}

/// Typed tree-diff stage shared directly with diff validation/classification.
///
/// Identity metadata is dictionary encoded once, entity keys occupy one typed
/// column, and both root sides are aligned `TrackedStateIndexValue` columns.
/// No production `TrackedStateArrowDiffEntry` or row-owned key exists between
/// tree traversal and the final public diff entries.
#[derive(Debug, Default)]
pub(crate) struct TrackedStateArrowDiffBatch {
    identities: Option<Arc<TrackedStateDiffIdentityBatch>>,
    before: Vec<Option<TrackedStateIndexValue>>,
    after: Vec<Option<TrackedStateIndexValue>>,
    payloads: TrackedStatePayloadBatch,
}

pub(crate) struct TrackedStateArrowDiffBatchBuilder {
    schema_keys: TrackedStateDiffStringInterner,
    file_ids: TrackedStateDiffStringInterner,
    rows: Vec<TrackedStateDiffKeyRow>,
    before: Vec<Option<TrackedStateIndexValue>>,
    after: Vec<Option<TrackedStateIndexValue>>,
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
    #[cfg(test)]
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
    let arrow_diff = reader
        .diff_arrow_entries_at_commits(left_commit_id, right_commit_id, &scan_request)
        .await?;

    // Payload equality is resolved from the changed canonical Arrow leaves.
    // The compact authored-event sidecar is not a post-image authority and is
    // therefore neither read nor replayed by structural diff.
    let payloads = arrow_diff.payloads().clone();

    let entries = classify_arrow_diff_batch(arrow_diff, &payloads)?;

    let diff = TrackedStateDiff::from_entries_with_payloads(entries, payloads);
    Ok(diff)
}

fn classify_arrow_diff_batch(
    arrow_diff: TrackedStateArrowDiffBatch,
    payloads: &TrackedStatePayloadBatch,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    let row_count = arrow_diff.len();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let (identities, before, after) = arrow_diff.into_columns();
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

fn scan_request_for_diff(request: &TrackedStateDiffRequest) -> TrackedStatePhysicalScanRequest {
    let mut filter = request.filter.clone();
    filter.include_tombstones = true;
    TrackedStatePhysicalScanRequest {
        schema_keys: filter.schema_keys,
        entity_pks: filter.entity_pks,
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

impl TrackedStateArrowDiffBatchBuilder {
    pub(crate) fn with_row_capacity(row_count: usize) -> Self {
        Self {
            schema_keys: TrackedStateDiffStringInterner::new(row_count),
            file_ids: TrackedStateDiffStringInterner::new(row_count),
            rows: Vec::with_capacity(row_count),
            before: Vec::with_capacity(row_count),
            after: Vec::with_capacity(row_count),
        }
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
            entity_pk: key.entity_pk,
        });
        self.before.push(before);
        self.after.push(after);
    }

    pub(crate) fn finish(self) -> Result<TrackedStateArrowDiffBatch, LixError> {
        let row_count = self.rows.len();
        debug_assert_eq!(self.before.len(), row_count);
        debug_assert_eq!(self.after.len(), row_count);
        if row_count == 0 {
            return Ok(TrackedStateArrowDiffBatch::default());
        }
        if row_count > u32::MAX as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state tree diff batch exceeds the identity ordinal range",
            ));
        }
        let identities = Arc::new(TrackedStateDiffIdentityBatch {
            keys: TrackedStateDiffKeyColumns {
                schema_keys: self.schema_keys.finish(),
                file_ids: self.file_ids.finish(),
                rows: self.rows,
            },
        });
        Ok(TrackedStateArrowDiffBatch {
            identities: Some(identities),
            before: self.before,
            after: self.after,
            payloads: TrackedStatePayloadBatch::default(),
        })
    }
}

impl TrackedStateArrowDiffBatch {
    pub(crate) fn with_payloads(mut self, payloads: TrackedStatePayloadBatch) -> Self {
        self.payloads = payloads;
        self
    }

    pub(crate) fn payloads(&self) -> &TrackedStatePayloadBatch {
        &self.payloads
    }

    pub(crate) fn len(&self) -> usize {
        self.before.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.before.is_empty()
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
}

impl TrackedStateDiffIdentityBatch {
    #[cfg(test)]
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
                entity_pk: key.entity_pk,
            });
        }
        Arc::new(Self {
            keys: TrackedStateDiffKeyColumns {
                schema_keys: schema_keys.finish(),
                file_ids: file_ids.finish(),
                rows,
            },
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
                entity_pk: key.entity_pk.clone(),
            });
        }
        Arc::new(Self {
            keys: TrackedStateDiffKeyColumns {
                schema_keys: schema_keys.finish(),
                file_ids: file_ids.finish(),
                rows,
            },
        })
    }

    fn schema_key(&self, ordinal: u32) -> &str {
        let row = &self.keys.rows[ordinal as usize];
        self.keys.schema_keys[row.schema_key_ordinal as usize].as_str()
    }

    fn file_id(&self, ordinal: u32) -> Option<&str> {
        let row = &self.keys.rows[ordinal as usize];
        (row.file_id_ordinal != u32::MAX)
            .then(|| self.keys.file_ids[row.file_id_ordinal as usize].as_str())
    }

    fn entity_pk(&self, ordinal: u32) -> &crate::entity_pk::EntityPk {
        &self.keys.rows[ordinal as usize].entity_pk
    }

    #[cfg(test)]
    fn into_key(self, ordinal: u32) -> TrackedStateKey {
        self.keys.key(ordinal)
    }

    fn len(&self) -> usize {
        self.keys.rows.len()
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

    #[cfg(test)]
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
    fn key(&self, ordinal: u32) -> TrackedStateKey {
        let row = &self.rows[ordinal as usize];
        TrackedStateKey {
            schema_key: self.schema_keys[row.schema_key_ordinal as usize].to_string(),
            file_id: (row.file_id_ordinal != u32::MAX)
                .then(|| self.file_ids[row.file_id_ordinal as usize].to_string()),
            entity_pk: row.entity_pk.clone(),
        }
    }
}

impl TrackedStateDiffIdentity {
    #[cfg(test)]
    pub(crate) fn from_key(key: TrackedStateKey) -> Self {
        Self::from_key_batch(vec![key])
            .expect("one identity fits the diff ordinal range")
            .pop()
            .expect("one identity was supplied")
    }

    /// Moves a complete key batch behind one shared identity owner.
    ///
    /// This is used by accelerators that discover changed keys outside the
    /// tracked-state tree diff. It preserves the same one-owner-per-batch
    /// contract instead of creating a singleton `Arc` allocation per key.
    #[cfg(test)]
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
    /// interned once into batch dictionaries and entity primary keys clone
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
            entity_pk: self.entity_pk(),
        }
    }

    /// Clones the shared schema owner without allocating decoded text.
    ///
    /// Downstream typed batches use this when re-dictionary-encoding an
    /// identity at another boundary, such as plugin merge output entering the
    /// transaction pipeline.
    pub(crate) fn schema_key_shared(&self) -> SharedStr {
        let row = &self.batch.keys.rows[self.ordinal as usize];
        self.batch.keys.schema_keys[row.schema_key_ordinal as usize].clone()
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.batch.file_id(self.ordinal)
    }

    /// Clones the shared file-id owner without allocating decoded text.
    pub(crate) fn file_id_shared(&self) -> Option<SharedStr> {
        let row = &self.batch.keys.rows[self.ordinal as usize];
        (row.file_id_ordinal != u32::MAX)
            .then(|| self.batch.keys.file_ids[row.file_id_ordinal as usize].clone())
    }

    pub(crate) fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        self.batch.entity_pk(self.ordinal)
    }

    #[cfg(test)]
    pub(crate) fn into_key(self) -> TrackedStateKey {
        match Arc::try_unwrap(self.batch) {
            Ok(batch) => batch.into_key(self.ordinal),
            Err(batch) => TrackedStateKey {
                schema_key: batch.schema_key(self.ordinal).to_owned(),
                file_id: batch.file_id(self.ordinal).map(str::to_owned),
                entity_pk: batch.entity_pk(self.ordinal).clone(),
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
        (
            self.batch.keys.schema_keys.len(),
            self.batch.keys.file_ids.len(),
        )
    }

    #[cfg(test)]
    fn batch_dictionary_capacities(&self) -> (usize, usize) {
        (
            self.batch.keys.schema_keys.capacity(),
            self.batch.keys.file_ids.capacity(),
        )
    }
}

impl fmt::Debug for TrackedStateDiffIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrackedStateDiffIdentity")
            .field("schema_key", &self.schema_key())
            .field("file_id", &self.file_id())
            .field("entity_pk", self.entity_pk())
            .finish()
    }
}

impl PartialEq for TrackedStateDiffIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.file_id() == other.file_id()
            && self.entity_pk() == other.entity_pk()
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
            .then_with(|| self.entity_pk().cmp(other.entity_pk()))
    }
}

impl Hash for TrackedStateDiffIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema_key().hash(state);
        self.file_id().hash(state);
        self.entity_pk().hash(state);
    }
}

impl TrackedStateDiffRow {
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

    pub(crate) fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        self.identity.entity_pk()
    }

    #[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;
    use crate::tracked_state::types::TrackedStateIndexValue;

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    #[test]
    fn ten_thousand_arrow_diff_rows_share_one_ordered_identity_batch() {
        let row_count = 10_000;
        let created_at = ts("2024-01-01T00:00:00.000Z");
        let updated_at = ts("2024-01-02T00:00:00.000Z");
        let change_id = ChangeId::for_test_label("shared-batch-change");
        let commit_id = CommitId::for_test_label("shared-batch-commit");
        let mut arrow_entries = TrackedStateArrowDiffBatchBuilder::with_row_capacity(row_count);
        for index in 0..row_count {
            arrow_entries.push_shared(
                DecodedTrackedStateKeyShared {
                    schema_key: SharedStr::from_static("test_schema"),
                    file_id: Some(SharedStr::from_static(
                        "01920000-0000-7000-8000-0000000000a2",
                    )),
                    entity_pk: EntityPk::single(format!("entity-{index:05}")),
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
        let arrow_entries = arrow_entries.finish().expect("tree batch should seal");
        assert_eq!(arrow_entries.large_buffer_count(), 3);
        let rows = classify_arrow_diff_batch(arrow_entries, &TrackedStatePayloadBatch::default())
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
                    .entity_pk()
                    .as_single_string_owned()
                    .expect("single identity"),
                format!("entity-{index:05}")
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
        let entity_pks = (0..10_000)
            .map(|index| EntityPk::single(format!("entity-{index:05}")))
            .collect::<Vec<_>>();
        let identities =
            TrackedStateDiffIdentity::from_key_refs(entity_pks.len(), |index| TrackedStateKeyRef {
                schema_key: "shared_schema",
                file_id: Some("shared_file"),
                entity_pk: &entity_pks[index],
            })
            .expect("borrowed identity batch should seal");

        assert_eq!(identities.len(), entity_pks.len());
        assert_eq!(identities[0].batch_len(), entity_pks.len());
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
                entity_pk: EntityPk::single(format!("entity-{index:05}")),
            });
            let start = encoded_arena.len();
            encoded_arena.extend_from_slice(&encoded);
            ranges.push(start..encoded_arena.len());
        }
        let encoded_arena = bytes::Bytes::from(encoded_arena);
        let arena_start = encoded_arena.as_ptr() as usize;
        let arena_end = arena_start + encoded_arena.len();
        let timestamp = ts("2024-01-01T00:00:00.000Z");
        let mut builder = TrackedStateArrowDiffBatchBuilder::with_row_capacity(row_count);
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
        let keys = &identities.keys;
        assert_eq!((keys.schema_keys.len(), keys.file_ids.len()), (1, 1));
        assert!(
            keys.schema_keys.capacity() <= DIFF_SMALL_STRING_DICTIONARY_LIMIT
                && keys.file_ids.capacity() <= DIFF_SMALL_STRING_DICTIONARY_LIMIT
        );
        for row in &keys.rows {
            for component in row.entity_pk.components.iter() {
                if let crate::entity_pk::EntityPkComponent::String(value) = component {
                    let (pointer, len) = value.retained_buffer_identity();
                    let start = pointer as usize;
                    assert!(
                        start >= arena_start && start.saturating_add(len) <= arena_end,
                        "entity key escaped the shared decoded arena"
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
        let mut arrow_entries = TrackedStateArrowDiffBatchBuilder::with_row_capacity(row_count);
        for index in 0..row_count {
            arrow_entries.push_shared(
                DecodedTrackedStateKeyShared {
                    schema_key: SharedStr::from_static("test_schema"),
                    file_id: None,
                    entity_pk: EntityPk::single(format!("entity-{index:05}")),
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
        let entries = classify_arrow_diff_batch(
            arrow_entries.finish().expect("tree batch should seal"),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("tree rows should classify");
        let source = TrackedStateDiff::from_entries(entries);
        let source_batch = source.entries[0].identity.clone();

        let plan = crate::tracked_state::merge::plan_merge(&TrackedStateDiff::default(), &source)
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
}
