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
use crate::tracked_state::types::TrackedStateKey;
#[cfg(test)]
use crate::tracked_state::types::TrackedStateKeyRef;

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

#[derive(Debug)]
struct TrackedStateDiffKey {
    schema_key: SharedStr,
    file_id: Option<SharedStr>,
    entity_pk: crate::entity_pk::EntityPk,
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
    pub(crate) fn contains(&self, change_id: ChangeId) -> bool {
        self.columns.id_ordinals.contains_key(&change_id)
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
                entity_pk: key.entity_pk,
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

    #[cfg(test)]
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
                entity_pk: key.entity_pk,
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

    fn entity_pk(&self, ordinal: u32) -> &crate::entity_pk::EntityPk {
        match &self.keys {
            TrackedStateDiffKeyStorage::Singleton(key) => {
                debug_assert_eq!(ordinal, 0);
                &key.entity_pk
            }
            TrackedStateDiffKeyStorage::Batch(keys) => &keys.rows[ordinal as usize].entity_pk,
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

    fn intern_owned(&mut self, value: String) -> u32 {
        if let Some(ordinal) = self.ordinal(value.as_str()) {
            return ordinal;
        }
        self.insert_new(SharedStr::from(value))
    }

    #[cfg(test)]
    fn intern_str(&mut self, value: &str) -> u32 {
        if let Some(ordinal) = self.ordinal(value) {
            return ordinal;
        }
        self.insert_new(SharedStr::from(value))
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

impl TrackedStateDiffKeyColumns {}

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
    /// interned once into batch dictionaries and entity primary keys clone
    /// only their shared descriptors, avoiding a terminal `String` allocation
    /// per discovered key.
    #[cfg(test)]
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

    pub(crate) fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        self.batch.entity_pk(self.ordinal)
    }

    #[cfg(test)]
    pub(crate) fn shares_key_with(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.file_id() == other.file_id()
            && self.entity_pk() == other.entity_pk()
    }

    #[cfg(test)]
    pub(crate) fn shares_batch_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.batch, &other.batch)
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
    pub(crate) fn schema_key(&self) -> &str {
        self.identity.schema_key()
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.identity.file_id()
    }

    pub(crate) fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        self.identity.entity_pk()
    }
}
