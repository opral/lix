use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::mem::size_of;
use std::num::NonZeroU32;
use std::ops::Range;
use std::sync::Arc;

use ahash::RandomState;
use bytes::Bytes;

use super::tracked_head::CertifiedCurrentStatePredecessor;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::{NullableKeyFilter, Value};

/// Terminal owned DTO for consumers that cannot yet borrow a live-state batch.
///
/// HOT materialization and visibility never use this as an intermediate. They
/// exchange [`MaterializedLiveStateBatch`] owners and borrowed row views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedLiveStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: Arc<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchemaKeyId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdId(NonZeroU32);

impl FileIdId {
    fn from_ordinal(ordinal: u32) -> Self {
        Self(
            NonZeroU32::new(
                ordinal
                    .checked_add(1)
                    .expect("live-state file-id ordinal exceeds compact representation"),
            )
            .expect("live-state file-id ordinal is encoded one-based"),
        )
    }

    fn ordinal(self) -> u32 {
        self.0.get() - 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BranchIdId(u32);

/// Dictionary storage shared by every identity column in one live-state batch.
///
/// Schema keys, file ids, and branch ids occupy one contiguous UTF-8 arena.
/// Their typed ordinal columns make repeated batch-wide metadata a four-byte
/// reference instead of another owned allocation on every row.
#[derive(Debug, Clone, Default)]
struct LiveStateStringDictionary {
    bytes: Bytes,
    ranges: Vec<Range<u32>>,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl LiveStateStringDictionary {
    fn get(&self, ordinal: u32) -> &str {
        let range = self
            .ranges
            .get(ordinal as usize)
            .expect("live-state string ordinal belongs to this batch");
        let range = range.start as usize..range.end as usize;
        // SAFETY: the builder appends complete `str` values and records their
        // exact boundaries. `Bytes` preserves that immutable allocation.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[range]) }
    }
}

/// Columnar owner for materialized live-state rows.
///
/// This is the read-side handoff between HOT materialization, visibility, and
/// provider adaptation. Identity strings are dictionary encoded once per
/// batch; payloads retain their existing shared storage buffers. Consumers
/// operate on [`MaterializedLiveStateRowRef`] views and only construct the
/// legacy owned DTO at an API boundary that still requires it.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializedLiveStateBatch {
    strings: LiveStateStringDictionary,
    schema_keys: Vec<SchemaKeyId>,
    file_ids: Vec<Option<FileIdId>>,
    branch_ids: Vec<BranchIdId>,
    entity_pks: Vec<EntityPk>,
    snapshot_content: Vec<Option<SharedStr>>,
    metadata: Vec<Option<SharedStr>>,
    deleted: Vec<bool>,
    created_at: Vec<LixTimestamp>,
    updated_at: Vec<LixTimestamp>,
    global: Vec<bool>,
    change_id: Vec<Option<ChangeId>>,
    commit_id: Vec<Option<CommitId>>,
    untracked: Vec<bool>,
    /// Encoded authoritative HOT predecessor resolved by a durable exact read.
    ///
    /// This is intentionally internal transaction evidence, not a public
    /// projection column. SQL UPDATE can carry it into commit materialization
    /// and avoid reading the same current row a second time.
    durable_predecessor: Vec<Option<CertifiedCurrentStatePredecessor>>,
}

impl MaterializedLiveStateBatch {
    pub(crate) fn from_rows(rows: Vec<MaterializedLiveStateRow>) -> Self {
        let (dictionary_entries, dictionary_bytes) = owned_row_dictionary_capacity(&rows);
        let mut builder = MaterializedLiveStateBatchBuilder::with_dictionary_capacity(
            rows.len(),
            dictionary_entries,
            dictionary_bytes,
        );
        for row in rows {
            builder.push_owned(row);
        }
        builder.finish()
    }

    pub(crate) fn len(&self) -> usize {
        self.entity_pks.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entity_pks.is_empty()
    }

    pub(crate) fn row(&self, index: usize) -> MaterializedLiveStateRowRef<'_> {
        assert!(index < self.len(), "live-state row ordinal out of bounds");
        MaterializedLiveStateRowRef { batch: self, index }
    }

    pub(crate) fn get(&self, index: usize) -> Option<MaterializedLiveStateRowRef<'_>> {
        (index < self.len()).then(|| self.row(index))
    }

    pub(crate) fn iter(&self) -> MaterializedLiveStateBatchIter<'_> {
        MaterializedLiveStateBatchIter {
            batch: self,
            next: 0,
        }
    }

    pub(crate) fn into_rows(self) -> Vec<MaterializedLiveStateRow> {
        let branch_ids = self.terminal_branch_owners();
        (0..self.len())
            .map(|index| {
                let branch_id = Arc::clone(
                    branch_ids[self.branch_ids[index].0 as usize]
                        .as_ref()
                        .expect("terminal branch owner was initialized"),
                );
                self.row(index).to_owned_with_branch(branch_id)
            })
            .collect()
    }

    /// Consumes the batch into snapshot buffers ordered by logical identity.
    ///
    /// Native public reads need only the snapshot payload after visibility has
    /// been resolved. Move each [`SharedStr`] backing buffer into the result
    /// instead of copying its JSON bytes into a second allocation. HOT scans
    /// are already identity ordered in the common case; the permutation below
    /// preserves the previous defensive ordering for mixed serving layouts.
    pub(crate) fn into_identity_ordered_snapshots(mut self) -> Vec<Option<Bytes>> {
        let mut ordinals = (0..self.len()).collect::<Vec<_>>();
        if !ordinals.is_sorted_by(|left, right| {
            let left = self.row(*left);
            let right = self.row(*right);
            left.entity_pk() < right.entity_pk()
                || (left.entity_pk() == right.entity_pk() && left.file_id() <= right.file_id())
        }) {
            ordinals.sort_unstable_by(|left, right| {
                let left = self.row(*left);
                let right = self.row(*right);
                left.entity_pk()
                    .cmp(right.entity_pk())
                    .then_with(|| left.file_id().cmp(&right.file_id()))
            });
        }

        if ordinals.iter().copied().eq(0..self.len()) {
            return self
                .snapshot_content
                .into_iter()
                .map(|snapshot| snapshot.map(SharedStr::into_bytes))
                .collect();
        }

        ordinals
            .into_iter()
            .map(|ordinal| {
                self.snapshot_content[ordinal]
                    .take()
                    .map(SharedStr::into_bytes)
            })
            .collect()
    }

    fn terminal_branch_owners(&self) -> Vec<Option<Arc<str>>> {
        let mut owners = vec![None; self.strings.ranges.len()];
        for branch_id in &self.branch_ids {
            let ordinal = branch_id.0 as usize;
            if owners[ordinal].is_none() {
                owners[ordinal] = Some(Arc::from(self.strings.get(branch_id.0)));
            }
        }
        owners
    }

    pub(crate) fn filter(
        &self,
        mut keep: impl FnMut(MaterializedLiveStateRowRef<'_>) -> bool,
        limit: Option<usize>,
    ) -> Self {
        let capacity = limit.map_or_else(|| self.len(), |limit| limit.min(self.len()));
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(capacity);
        if capacity == 0 && limit.is_some() {
            return builder.finish();
        }
        for row in self.iter() {
            if keep(row) {
                builder.push_ref(row, None);
                if builder.len() == capacity && limit.is_some() {
                    break;
                }
            }
        }
        builder.finish()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_entry_count(&self) -> usize {
        self.strings.ranges.len()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_bytes_len(&self) -> usize {
        self.strings.bytes.len()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_buffer_count(&self) -> usize {
        usize::from(!self.strings.bytes.is_empty())
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_allocation_count(&self) -> usize {
        self.strings.arena_allocation_count
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_large_allocation_count(&self) -> usize {
        self.strings.arena_large_allocation_count
    }

    #[cfg(test)]
    pub(crate) fn entity_column_ptr(&self) -> *const EntityPk {
        self.entity_pks.as_ptr()
    }

    #[cfg(test)]
    fn large_column_allocation_count(&self, threshold: usize) -> usize {
        [
            self.schema_keys.capacity() * size_of::<SchemaKeyId>(),
            self.file_ids.capacity() * size_of::<Option<FileIdId>>(),
            self.branch_ids.capacity() * size_of::<BranchIdId>(),
            self.entity_pks.capacity() * size_of::<EntityPk>(),
            self.snapshot_content.capacity() * size_of::<Option<SharedStr>>(),
            self.metadata.capacity() * size_of::<Option<SharedStr>>(),
            self.deleted.capacity() * size_of::<bool>(),
            self.created_at.capacity() * size_of::<LixTimestamp>(),
            self.updated_at.capacity() * size_of::<LixTimestamp>(),
            self.global.capacity() * size_of::<bool>(),
            self.change_id.capacity() * size_of::<Option<ChangeId>>(),
            self.commit_id.capacity() * size_of::<Option<CommitId>>(),
            self.untracked.capacity() * size_of::<bool>(),
            self.strings.bytes.len(),
            self.strings.ranges.capacity() * size_of::<Range<u32>>(),
        ]
        .into_iter()
        .filter(|bytes| *bytes >= threshold)
        .count()
    }
}

impl From<Vec<MaterializedLiveStateRow>> for MaterializedLiveStateBatch {
    fn from(rows: Vec<MaterializedLiveStateRow>) -> Self {
        Self::from_rows(rows)
    }
}

/// One borrowed row view over a [`MaterializedLiveStateBatch`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaterializedLiveStateRowRef<'a> {
    batch: &'a MaterializedLiveStateBatch,
    index: usize,
}

impl<'a> MaterializedLiveStateRowRef<'a> {
    pub(crate) fn entity_pk(self) -> &'a EntityPk {
        &self.batch.entity_pks[self.index]
    }

    pub(crate) fn schema_key(self) -> &'a str {
        self.batch.strings.get(self.batch.schema_keys[self.index].0)
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.batch.file_ids[self.index].map(|ordinal| self.batch.strings.get(ordinal.ordinal()))
    }

    pub(crate) fn snapshot_content(self) -> Option<&'a SharedStr> {
        self.batch.snapshot_content[self.index].as_ref()
    }

    pub(crate) fn metadata(self) -> Option<&'a SharedStr> {
        self.batch.metadata[self.index].as_ref()
    }

    pub(crate) fn deleted(self) -> bool {
        self.batch.deleted[self.index]
    }

    pub(crate) fn created_at(self) -> LixTimestamp {
        self.batch.created_at[self.index]
    }

    pub(crate) fn updated_at(self) -> LixTimestamp {
        self.batch.updated_at[self.index]
    }

    pub(crate) fn global(self) -> bool {
        self.batch.global[self.index]
    }

    pub(crate) fn change_id(self) -> Option<ChangeId> {
        self.batch.change_id[self.index]
    }

    pub(crate) fn commit_id(self) -> Option<CommitId> {
        self.batch.commit_id[self.index]
    }

    pub(crate) fn untracked(self) -> bool {
        self.batch.untracked[self.index]
    }

    pub(crate) fn durable_predecessor(self) -> Option<&'a CertifiedCurrentStatePredecessor> {
        self.batch.durable_predecessor[self.index].as_ref()
    }

    pub(crate) fn branch_id(self) -> &'a str {
        self.batch.strings.get(self.batch.branch_ids[self.index].0)
    }

    /// Materializes an owned row at a scalar or persistent-index boundary.
    ///
    /// Batch pipeline stages should retain the batch owner and borrow this
    /// view instead. This conversion deliberately remains explicit so an
    /// accidental row-owned intermediate is visible at its call site.
    pub(crate) fn to_owned(self) -> MaterializedLiveStateRow {
        self.to_owned_with_branch(Arc::from(self.branch_id()))
    }

    fn to_owned_with_branch(self, branch_id: Arc<str>) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: self.entity_pk().clone(),
            schema_key: self.schema_key().to_owned(),
            file_id: self.file_id().map(str::to_owned),
            snapshot_content: self.snapshot_content().cloned(),
            metadata: self.metadata().cloned(),
            deleted: self.deleted(),
            created_at: self.created_at(),
            updated_at: self.updated_at(),
            global: self.global(),
            change_id: self.change_id(),
            commit_id: self.commit_id(),
            untracked: self.untracked(),
            branch_id,
        }
    }
}

pub(crate) struct MaterializedLiveStateBatchIter<'a> {
    batch: &'a MaterializedLiveStateBatch,
    next: usize,
}

impl<'a> Iterator for MaterializedLiveStateBatchIter<'a> {
    type Item = MaterializedLiveStateRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next;
        if index == self.batch.len() {
            return None;
        }
        self.next += 1;
        Some(self.batch.row(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.len() - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MaterializedLiveStateBatchIter<'_> {}

/// Aligned exact-read result. Missing slots are represented by `None`; present
/// slots point into one compact materialized batch.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializedLiveStateExactBatch {
    batch: MaterializedLiveStateBatch,
    slots: Vec<Option<u32>>,
}

impl MaterializedLiveStateExactBatch {
    pub(crate) fn new(
        batch: MaterializedLiveStateBatch,
        slots: Vec<Option<u32>>,
    ) -> Result<Self, crate::LixError> {
        if u32::try_from(batch.len()).is_err()
            || slots
                .iter()
                .flatten()
                .any(|ordinal| *ordinal as usize >= batch.len())
        {
            return Err(crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "exact live-state result contains an invalid batch ordinal",
            ));
        }
        Ok(Self { batch, slots })
    }

    #[cfg(test)]
    pub(crate) fn from_rows(rows: Vec<Option<MaterializedLiveStateRow>>) -> Self {
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
        let mut slots = Vec::with_capacity(rows.len());
        for row in rows {
            slots.push(row.map(|row| {
                let ordinal = u32::try_from(builder.len())
                    .expect("exact live-state row count exceeds u32 ordinals");
                builder.push_owned(row);
                ordinal
            }));
        }
        Self {
            batch: builder.finish(),
            slots,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn row(&self, slot: usize) -> Option<MaterializedLiveStateRowRef<'_>> {
        self.slots
            .get(slot)
            .copied()
            .flatten()
            .map(|ordinal| self.batch.row(ordinal as usize))
    }

    /// Consumes an aligned exact result into one compact owner containing only
    /// present rows in request order.
    ///
    /// Durable readers normally already produce identity-ordered slots, in
    /// which case this is a zero-copy move of the underlying batch. Sparse or
    /// deduplicated results are compacted with one batch builder rather than a
    /// `Vec<Option<MaterializedLiveStateRow>>` intermediate.
    pub(crate) fn into_present_batch(self) -> MaterializedLiveStateBatch {
        let Self { batch, slots } = self;
        if slots.len() == batch.len()
            && slots
                .iter()
                .enumerate()
                .all(|(index, slot)| *slot == u32::try_from(index).ok())
        {
            return batch;
        }

        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(
            slots.iter().filter(|slot| slot.is_some()).count(),
        );
        for ordinal in slots.into_iter().flatten() {
            builder.push_ref(batch.row(ordinal as usize), None);
        }
        builder.finish()
    }

    pub(crate) fn into_rows(self) -> Vec<Option<MaterializedLiveStateRow>> {
        let branch_ids = self.batch.terminal_branch_owners();
        self.slots
            .iter()
            .map(|ordinal| {
                ordinal.map(|ordinal| {
                    let index = ordinal as usize;
                    let branch_ordinal = self.batch.branch_ids[index].0 as usize;
                    self.batch.row(index).to_owned_with_branch(Arc::clone(
                        branch_ids[branch_ordinal]
                            .as_ref()
                            .expect("terminal branch owner was initialized"),
                    ))
                })
            })
            .collect()
    }
}

fn owned_row_dictionary_capacity(rows: &[MaterializedLiveStateRow]) -> (usize, usize) {
    let mut seen = HashSet::<&str, FastHashBuilder>::with_capacity_and_hasher(
        rows.len().saturating_mul(3),
        live_state_hash_builder(),
    );
    let mut bytes = 0_usize;
    for row in rows {
        account_dictionary_value(&mut seen, &mut bytes, row.schema_key.as_str());
        if let Some(file_id) = row.file_id.as_deref() {
            account_dictionary_value(&mut seen, &mut bytes, file_id);
        }
        account_dictionary_value(&mut seen, &mut bytes, row.branch_id.as_ref());
    }
    (seen.len(), bytes)
}

fn account_dictionary_value<'a>(
    seen: &mut HashSet<&'a str, FastHashBuilder>,
    bytes: &mut usize,
    value: &'a str,
) {
    if seen.insert(value) {
        *bytes = (*bytes)
            .checked_add(value.len())
            .expect("live-state string dictionary byte count overflow");
    }
}

const SMALL_DICTIONARY_LOOKUP_LIMIT: usize = 32;
const SMALL_DICTIONARY_ARENA_BYTES: usize = 1024;
const NO_DICTIONARY_ORDINAL: u32 = u32::MAX;
#[cfg(test)]
const LARGE_DICTIONARY_ALLOCATION_BYTES: usize = 32 * 1024;

type FastHashBuilder = RandomState;

enum LiveStateStringLookup {
    Small,
    Hashed(HashMap<u64, u32, FastHashBuilder>),
}

/// Arena-first interner for the live-state identity dictionaries.
///
/// Hash buckets point to an ordinal in `ranges`; `collision_next` links the
/// remaining entries with the same 64-bit hash. Keys therefore remain stable
/// across arena growth without retaining one heap allocation per distinct
/// string.
struct LiveStateStringDictionaryBuilder {
    bytes: Vec<u8>,
    ranges: Vec<Range<u32>>,
    collision_next: Vec<u32>,
    lookup: LiveStateStringLookup,
    hash_builder: FastHashBuilder,
    expected_entry_capacity: usize,
    maximum_entry_capacity: usize,
    max_string_len: usize,
    exact_byte_capacity: bool,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl LiveStateStringDictionaryBuilder {
    fn with_capacity(
        row_capacity: usize,
        dictionary_entry_capacity: usize,
        dictionary_byte_capacity: usize,
        exact_byte_capacity: bool,
    ) -> Self {
        let expected_entry_capacity = dictionary_entry_capacity.max(1);
        Self {
            bytes: Vec::with_capacity(dictionary_byte_capacity),
            ranges: Vec::with_capacity(dictionary_entry_capacity),
            collision_next: Vec::with_capacity(dictionary_entry_capacity),
            lookup: LiveStateStringLookup::Small,
            hash_builder: live_state_hash_builder(),
            expected_entry_capacity,
            maximum_entry_capacity: row_capacity
                .saturating_mul(3)
                .max(dictionary_entry_capacity)
                .max(1),
            max_string_len: 0,
            exact_byte_capacity,
            #[cfg(test)]
            arena_allocation_count: usize::from(dictionary_byte_capacity != 0),
            #[cfg(test)]
            arena_large_allocation_count: usize::from(
                dictionary_byte_capacity >= LARGE_DICTIONARY_ALLOCATION_BYTES,
            ),
        }
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        self.intern(value.as_str())
    }

    fn intern_ref(&mut self, value: &str) -> u32 {
        self.intern(value)
    }

    fn intern(&mut self, value: &str) -> u32 {
        if !matches!(&self.lookup, LiveStateStringLookup::Small) {
            return self.intern_hashed(value);
        }
        if let Some(ordinal) = self.find_linear(value) {
            return ordinal;
        }
        if self.ranges.len() == SMALL_DICTIONARY_LOOKUP_LIMIT {
            self.promote_to_hashed(value.len());
            self.intern_hashed(value)
        } else {
            self.append_small(value)
        }
    }

    fn find_linear(&self, value: &str) -> Option<u32> {
        self.ranges
            .iter()
            .position(|range| {
                &self.bytes[range.start as usize..range.end as usize] == value.as_bytes()
            })
            .map(|ordinal| {
                u32::try_from(ordinal).expect("live-state dictionary ordinal exceeds u32")
            })
    }

    fn intern_hashed(&mut self, value: &str) -> u32 {
        let hash = live_state_dictionary_hash(&self.hash_builder, value.as_bytes());
        let mut candidate = match &self.lookup {
            LiveStateStringLookup::Small => {
                unreachable!("hashed dictionary lookup must be promoted first")
            }
            LiveStateStringLookup::Hashed(lookup) => lookup.get(&hash).copied(),
        };
        while let Some(ordinal) = candidate {
            if self.value(ordinal) == value {
                return ordinal;
            }
            let next = self.collision_next[ordinal as usize];
            candidate = (next != NO_DICTIONARY_ORDINAL).then_some(next);
        }
        self.append_hashed(value, hash)
    }

    fn value(&self, ordinal: u32) -> &str {
        let range = &self.ranges[ordinal as usize];
        // SAFETY: `append_bytes` receives a `str` and records that complete
        // value's exact boundaries.
        unsafe {
            std::str::from_utf8_unchecked(&self.bytes[range.start as usize..range.end as usize])
        }
    }

    fn append_small(&mut self, value: &str) -> u32 {
        let ordinal = self.append_bytes(value);
        self.collision_next.push(NO_DICTIONARY_ORDINAL);
        ordinal
    }

    fn append_hashed(&mut self, value: &str, hash: u64) -> u32 {
        let previous_head = match &self.lookup {
            LiveStateStringLookup::Small => {
                unreachable!("hashed dictionary insertion must be promoted first")
            }
            LiveStateStringLookup::Hashed(lookup) => {
                lookup.get(&hash).copied().unwrap_or(NO_DICTIONARY_ORDINAL)
            }
        };
        let ordinal = self.append_bytes(value);
        self.collision_next.push(previous_head);
        let LiveStateStringLookup::Hashed(lookup) = &mut self.lookup else {
            unreachable!("hashed dictionary insertion must retain its lookup")
        };
        lookup.insert(hash, ordinal);
        ordinal
    }

    fn append_bytes(&mut self, value: &str) -> u32 {
        self.max_string_len = self.max_string_len.max(value.len());
        let end = self
            .bytes
            .len()
            .checked_add(value.len())
            .expect("live-state string dictionary byte count overflow");
        let end_u32 = u32::try_from(end).expect("live-state string dictionary exceeds u32 bytes");
        self.ensure_arena_capacity(end);
        let start = u32::try_from(self.bytes.len())
            .expect("live-state string dictionary start exceeds u32 bytes");
        self.bytes.extend_from_slice(value.as_bytes());
        let ordinal =
            u32::try_from(self.ranges.len()).expect("live-state dictionary exceeds u32 rows");
        assert_ne!(
            ordinal, NO_DICTIONARY_ORDINAL,
            "live-state dictionary reserves the terminal u32 ordinal"
        );
        self.ranges.push(start..end_u32);
        ordinal
    }

    fn ensure_arena_capacity(&mut self, required: usize) {
        if required <= self.bytes.capacity() {
            return;
        }
        let projected = match &self.lookup {
            LiveStateStringLookup::Small => SMALL_DICTIONARY_ARENA_BYTES,
            LiveStateStringLookup::Hashed(_) => self
                .maximum_entry_capacity
                .saturating_mul(self.max_string_len),
        };
        let target = required.max(projected);
        self.bytes.reserve_exact(target - self.bytes.len());
        #[cfg(test)]
        {
            self.arena_allocation_count += 1;
            self.arena_large_allocation_count +=
                usize::from(target >= LARGE_DICTIONARY_ALLOCATION_BYTES);
        }
    }

    fn promote_to_hashed(&mut self, incoming_len: usize) {
        self.max_string_len = self.max_string_len.max(incoming_len);
        let projected_entries = self
            .expected_entry_capacity
            .max(self.ranges.len().saturating_add(1));
        let projected_bytes = projected_entries.saturating_mul(self.max_string_len);
        if !self.exact_byte_capacity && projected_bytes > self.bytes.capacity() {
            self.bytes.reserve_exact(projected_bytes - self.bytes.len());
            #[cfg(test)]
            {
                self.arena_allocation_count += 1;
                self.arena_large_allocation_count +=
                    usize::from(projected_bytes >= LARGE_DICTIONARY_ALLOCATION_BYTES);
            }
        }

        let mut lookup =
            HashMap::with_capacity_and_hasher(projected_entries, live_state_hash_builder());
        for ordinal in 0..self.ranges.len() {
            let ordinal =
                u32::try_from(ordinal).expect("live-state dictionary ordinal exceeds u32");
            let hash =
                live_state_dictionary_hash(&self.hash_builder, self.value(ordinal).as_bytes());
            self.collision_next[ordinal as usize] = lookup
                .insert(hash, ordinal)
                .unwrap_or(NO_DICTIONARY_ORDINAL);
        }
        self.lookup = LiveStateStringLookup::Hashed(lookup);
    }

    fn finish(self) -> LiveStateStringDictionary {
        debug_assert!(
            self.ranges
                .iter()
                .all(|range| range.start <= range.end && range.end as usize <= self.bytes.len())
        );
        LiveStateStringDictionary {
            bytes: Bytes::from(self.bytes),
            ranges: self.ranges,
            #[cfg(test)]
            arena_allocation_count: self.arena_allocation_count,
            #[cfg(test)]
            arena_large_allocation_count: self.arena_large_allocation_count,
        }
    }
}

fn live_state_hash_builder() -> FastHashBuilder {
    FastHashBuilder::with_seeds(0, 0, 0, 0)
}

fn live_state_dictionary_hash(hash_builder: &FastHashBuilder, value: &[u8]) -> u64 {
    hash_builder.hash_one(value)
}

/// Temporary builder for a columnar materialized batch.
///
/// Distinct identity values are appended directly to one UTF-8 arena. Small
/// dictionaries use a linear range lookup; larger dictionaries promote to one
/// hash table whose entries are compact arena ordinals. Finish transfers the
/// arena into the immutable batch without copying it.
pub(crate) struct MaterializedLiveStateBatchBuilder {
    strings: LiveStateStringDictionaryBuilder,
    schema_keys: Vec<SchemaKeyId>,
    file_ids: Vec<Option<FileIdId>>,
    branch_ids: Vec<BranchIdId>,
    entity_pks: Vec<EntityPk>,
    snapshot_content: Vec<Option<SharedStr>>,
    metadata: Vec<Option<SharedStr>>,
    deleted: Vec<bool>,
    created_at: Vec<LixTimestamp>,
    updated_at: Vec<LixTimestamp>,
    global: Vec<bool>,
    change_id: Vec<Option<ChangeId>>,
    commit_id: Vec<Option<CommitId>>,
    untracked: Vec<bool>,
    durable_predecessor: Vec<Option<CertifiedCurrentStatePredecessor>>,
}

impl MaterializedLiveStateBatchBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let dictionary_entry_capacity = if capacity == 0 {
            0
        } else {
            capacity.saturating_add(2)
        };
        Self::with_capacities(capacity, dictionary_entry_capacity, 0, false)
    }

    fn with_dictionary_capacity(
        capacity: usize,
        dictionary_entry_capacity: usize,
        dictionary_byte_capacity: usize,
    ) -> Self {
        Self::with_capacities(
            capacity,
            dictionary_entry_capacity,
            dictionary_byte_capacity,
            true,
        )
    }

    fn with_capacities(
        capacity: usize,
        dictionary_entry_capacity: usize,
        dictionary_byte_capacity: usize,
        exact_byte_capacity: bool,
    ) -> Self {
        Self {
            strings: LiveStateStringDictionaryBuilder::with_capacity(
                capacity,
                dictionary_entry_capacity,
                dictionary_byte_capacity,
                exact_byte_capacity,
            ),
            schema_keys: Vec::with_capacity(capacity),
            file_ids: Vec::with_capacity(capacity),
            branch_ids: Vec::with_capacity(capacity),
            entity_pks: Vec::with_capacity(capacity),
            snapshot_content: Vec::with_capacity(capacity),
            metadata: Vec::with_capacity(capacity),
            deleted: Vec::with_capacity(capacity),
            created_at: Vec::with_capacity(capacity),
            updated_at: Vec::with_capacity(capacity),
            global: Vec::with_capacity(capacity),
            change_id: Vec::with_capacity(capacity),
            commit_id: Vec::with_capacity(capacity),
            untracked: Vec::with_capacity(capacity),
            durable_predecessor: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entity_pks.len()
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        self.strings.intern_owned(value)
    }

    fn intern_ref(&mut self, value: &str) -> u32 {
        self.strings.intern_ref(value)
    }

    pub(crate) fn push_owned(&mut self, row: MaterializedLiveStateRow) {
        let schema_key = SchemaKeyId(self.intern_owned(row.schema_key));
        let file_id = row
            .file_id
            .map(|file_id| FileIdId::from_ordinal(self.intern_owned(file_id)));
        let branch_id = BranchIdId(self.intern_ref(row.branch_id.as_ref()));
        self.push_columns(
            schema_key,
            file_id,
            branch_id,
            row.entity_pk,
            row.snapshot_content,
            row.metadata,
            row.deleted,
            row.created_at,
            row.updated_at,
            row.global,
            row.change_id,
            row.commit_id,
            row.untracked,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_materialized(
        &mut self,
        entity_pk: EntityPk,
        schema_key: String,
        file_id: Option<String>,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) -> usize {
        let ordinal = self.len();
        let schema_key = SchemaKeyId(self.intern_owned(schema_key));
        let file_id = file_id.map(|file_id| FileIdId::from_ordinal(self.intern_owned(file_id)));
        let branch_id = BranchIdId(self.intern_ref(branch_id));
        self.push_columns(
            schema_key,
            file_id,
            branch_id,
            entity_pk,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
        );
        ordinal
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_materialized_ref(
        &mut self,
        entity_pk: &EntityPk,
        schema_key: &str,
        file_id: Option<&str>,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) -> usize {
        let ordinal = self.len();
        let schema_key = SchemaKeyId(self.intern_ref(schema_key));
        let file_id = file_id.map(|file_id| FileIdId::from_ordinal(self.intern_ref(file_id)));
        let branch_id = BranchIdId(self.intern_ref(branch_id));
        self.push_columns(
            schema_key,
            file_id,
            branch_id,
            entity_pk.clone(),
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
        );
        ordinal
    }

    pub(crate) fn push_ref(
        &mut self,
        row: MaterializedLiveStateRowRef<'_>,
        branch_override: Option<&str>,
    ) -> usize {
        let ordinal = self.len();
        let schema_key = SchemaKeyId(self.intern_ref(row.schema_key()));
        let file_id = row
            .file_id()
            .map(|file_id| FileIdId::from_ordinal(self.intern_ref(file_id)));
        let branch_id =
            BranchIdId(self.intern_ref(branch_override.unwrap_or_else(|| row.branch_id())));
        self.push_columns(
            schema_key,
            file_id,
            branch_id,
            row.entity_pk().clone(),
            row.snapshot_content().cloned(),
            row.metadata().cloned(),
            row.deleted(),
            row.created_at(),
            row.updated_at(),
            row.global(),
            row.change_id(),
            row.commit_id(),
            row.untracked(),
        );
        self.durable_predecessor
            .last_mut()
            .expect("pushed live-state row has a predecessor slot")
            .clone_from(&row.durable_predecessor().cloned());
        ordinal
    }

    #[allow(clippy::too_many_arguments)]
    fn push_columns(
        &mut self,
        schema_key: SchemaKeyId,
        file_id: Option<FileIdId>,
        branch_id: BranchIdId,
        entity_pk: EntityPk,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
    ) {
        self.schema_keys.push(schema_key);
        self.file_ids.push(file_id);
        self.branch_ids.push(branch_id);
        self.entity_pks.push(entity_pk);
        self.snapshot_content.push(snapshot_content);
        self.metadata.push(metadata);
        self.deleted.push(deleted);
        self.created_at.push(created_at);
        self.updated_at.push(updated_at);
        self.global.push(global);
        self.change_id.push(change_id);
        self.commit_id.push(commit_id);
        self.untracked.push(untracked);
        self.durable_predecessor.push(None);
    }

    pub(crate) fn set_snapshot_content(&mut self, row: usize, value: SharedStr) {
        self.snapshot_content[row] = Some(value);
    }

    pub(crate) fn set_metadata(&mut self, row: usize, value: SharedStr) {
        self.metadata[row] = Some(value);
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        row: usize,
        value: CertifiedCurrentStatePredecessor,
    ) {
        self.durable_predecessor[row] = Some(value);
    }

    pub(crate) fn finish(self) -> MaterializedLiveStateBatch {
        MaterializedLiveStateBatch {
            strings: self.strings.finish(),
            schema_keys: self.schema_keys,
            file_ids: self.file_ids,
            branch_ids: self.branch_ids,
            entity_pks: self.entity_pks,
            snapshot_content: self.snapshot_content,
            metadata: self.metadata,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            global: self.global,
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.untracked,
            durable_predecessor: self.durable_predecessor,
        }
    }
}

impl TryFrom<&MaterializedLiveStateRow> for MaterializedTrackedStateRow {
    type Error = crate::LixError;

    fn try_from(row: &MaterializedLiveStateRow) -> Result<Self, Self::Error> {
        if row.untracked {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state cannot store untracked live-state rows",
            ));
        }
        let Some(change_id) = row.change_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require change_id",
            ));
        };
        let Some(commit_id) = row.commit_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require commit_id",
            ));
        };

        Ok(Self {
            entity_pk: row.entity_pk.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            deleted: row.deleted,
            created_at: row.created_at.to_string(),
            updated_at: row.updated_at.to_string(),
            change_id,
            commit_id,
        })
    }
}

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanField {
    EntityPk,
    FileId,
}

/// Inclusive or exclusive range bound.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Bound {
    pub(crate) value: Value,
    pub(crate) inclusive: bool,
}

/// SQL-free structured scan constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ScanConstraint {
    pub(crate) field: ScanField,
    pub(crate) operator: ScanOperator,
}

/// Structured scan operator aligned with the current planner/storage split.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanOperator {
    Eq(Value),
    In(Vec<Value>),
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}

/// Identity-centered filter for visible live entities.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateFilter {
    #[serde(default)]
    pub(crate) rows: LiveStateRowFilter,
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_pks: Vec<EntityPk>,
    #[serde(default)]
    pub(crate) branch_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) untracked: Option<bool>,
    #[serde(default)]
    pub(crate) constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) enum LiveStateRowFilter {
    #[default]
    All,
    None,
}

/// Requested property set for a live-state scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// First-principles scan request for engine-owned reads.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateScanRequest {
    #[serde(default)]
    pub(crate) filter: LiveStateFilter,
    #[serde(default)]
    pub(crate) projection: LiveStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one visible live-state row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: NullableKeyFilter<String>,
}

/// One concrete visible-row identity in an exact batch read.
///
/// Unlike [`LiveStateFilter`], the identity fields in this request are
/// correlated. Implementations must never expand multiple requests into the
/// Cartesian product of their schema, entity, and file dimensions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveStateExactRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
}

/// Aligned point-read request for visible live-state rows.
///
/// Results preserve `rows` order and cardinality: duplicate identities produce
/// duplicate result slots and missing or tombstoned identities produce `None`
/// unless tombstones are explicitly requested.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct LiveStateExactBatchRequest {
    pub(crate) rows: Vec<LiveStateExactRowRequest>,
    pub(crate) projection: LiveStateProjection,
    pub(crate) untracked: Option<bool>,
    pub(crate) include_tombstones: bool,
}

impl LiveStateExactBatchRequest {
    pub(crate) fn row_scan_request(&self, row: &LiveStateExactRowRequest) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![row.schema_key.clone()],
                entity_pks: vec![row.entity_pk.clone()],
                branch_ids: vec![row.branch_id.clone()],
                file_ids: vec![
                    row.file_id
                        .as_ref()
                        .map_or(NullableKeyFilter::Null, |file_id| {
                            NullableKeyFilter::Value(file_id.clone())
                        }),
                ],
                untracked: self.untracked,
                include_tombstones: self.include_tombstones,
                ..LiveStateFilter::default()
            },
            projection: self.projection.clone(),
            limit: Some(1),
        }
    }
}

/// Borrowed visible-row identity used for overlay composition.
///
/// Overlay maps own only these references and row ordinals. They never clone
/// schema, file, branch, or entity-key storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveStateRowIdentityRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) file_id: Option<&'a str>,
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    fn row(entity_pk: EntityPk) -> MaterializedLiveStateRow {
        let timestamp = LixTimestamp::expect_parse("batch test timestamp", "2026-01-01T00:00:00Z");
        MaterializedLiveStateRow {
            entity_pk,
            schema_key: "shared_schema".to_owned(),
            file_id: Some("shared_file".to_owned()),
            snapshot_content: None,
            metadata: None,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            global: false,
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: Arc::from("shared_branch"),
        }
    }

    #[test]
    fn materialized_batch_stores_repeated_identity_metadata_once() {
        let entity_pk = EntityPk::single("shared_entity");
        let batch = MaterializedLiveStateBatch::from_rows(
            (0..10_000).map(|_| row(entity_pk.clone())).collect(),
        );

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.dictionary_entry_count(), 3);
        assert_eq!(
            batch.dictionary_bytes_len(),
            "shared_schema".len() + "shared_file".len() + "shared_branch".len()
        );
        let first = batch.row(0);
        let last = batch.row(batch.len() - 1);
        assert_eq!(first.schema_key().as_ptr(), last.schema_key().as_ptr());
        assert_eq!(
            first.file_id().expect("file").as_ptr(),
            last.file_id().expect("file").as_ptr()
        );
        assert_eq!(first.branch_id().as_ptr(), last.branch_id().as_ptr());
        assert!(
            batch.large_column_allocation_count(32 * 1024) <= 13,
            "a 10k batch has a constant number of large column buffers"
        );
    }

    #[test]
    fn identity_ordered_snapshots_transfer_shared_payload_buffers() {
        let payload = Bytes::from_static(br#"{"path":"a"}"#);
        let payload_ptr = payload.as_ptr();
        let mut source = row(EntityPk::single("a"));
        source.snapshot_content = Some(
            SharedStr::from_utf8(payload).expect("snapshot fixture should contain valid UTF-8"),
        );
        let snapshots =
            MaterializedLiveStateBatch::from_rows(vec![source]).into_identity_ordered_snapshots();

        let snapshot = snapshots[0].as_ref().expect("snapshot should be present");
        assert_eq!(snapshot.as_ptr(), payload_ptr);
        assert_eq!(snapshot.as_ref(), br#"{"path":"a"}"#);
    }

    #[test]
    fn unordered_snapshots_restore_logical_identity_order() {
        let mut second = row(EntityPk::single("b"));
        second.snapshot_content = Some(SharedStr::from_static(r#"{"path":"b"}"#));
        let mut first = row(EntityPk::single("a"));
        first.snapshot_content = Some(SharedStr::from_static(r#"{"path":"a"}"#));

        let snapshots = MaterializedLiveStateBatch::from_rows(vec![second, first])
            .into_identity_ordered_snapshots();

        assert_eq!(snapshots[0].as_deref(), Some(br#"{"path":"a"}"#.as_slice()));
        assert_eq!(snapshots[1].as_deref(), Some(br#"{"path":"b"}"#.as_slice()));
    }

    #[test]
    fn materialized_batch_uses_one_utf8_arena_for_10k_distinct_file_ids() {
        let entity_pk = EntityPk::single("shared_entity");
        let rows = (0..10_000)
            .map(|index| {
                let mut row = row(entity_pk.clone());
                row.file_id = Some(format!("file-{index:08}"));
                row
            })
            .collect::<Vec<_>>();
        let expected_file_bytes = rows
            .iter()
            .map(|row| row.file_id.as_deref().expect("file id").len())
            .sum::<usize>();

        let batch = MaterializedLiveStateBatch::from_rows(rows);

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.dictionary_entry_count(), 10_002);
        assert_eq!(
            size_of::<Option<FileIdId>>(),
            size_of::<u32>(),
            "nullable file-id ordinals should remain four-byte dictionary references"
        );
        assert_eq!(
            batch.dictionary_bytes_len(),
            "shared_schema".len() + expected_file_bytes + "shared_branch".len()
        );
        assert_eq!(batch.dictionary_arena_buffer_count(), 1);
        assert_eq!(
            batch.dictionary_arena_allocation_count(),
            1,
            "exact preflight should allocate the UTF-8 arena once"
        );
        assert_eq!(
            batch.dictionary_arena_large_allocation_count(),
            1,
            "the batch should perform one large UTF-8 arena allocation"
        );
        assert_eq!(batch.row(0).file_id(), Some("file-00000000"));
        assert_eq!(batch.row(9_999).file_id(), Some("file-00009999"));
        assert_eq!(
            batch.row(0).schema_key().as_ptr(),
            batch.row(9_999).schema_key().as_ptr()
        );
        assert_eq!(
            batch.row(0).branch_id().as_ptr(),
            batch.row(9_999).branch_id().as_ptr()
        );
    }

    #[test]
    fn materialized_builder_promotes_once_for_10k_distinct_file_ids() {
        let timestamp = LixTimestamp::expect_parse("batch test timestamp", "2026-01-01T00:00:00Z");
        let entity_pk = EntityPk::single("shared_entity");
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(10_000);
        for index in 0..10_000 {
            let file_id = format!("file-{index:08}");
            builder.push_materialized_ref(
                &entity_pk,
                "shared_schema",
                Some(&file_id),
                None,
                None,
                false,
                timestamp,
                timestamp,
                false,
                None,
                None,
                true,
                "shared_branch",
            );
        }

        let batch = builder.finish();

        assert_eq!(batch.dictionary_entry_count(), 10_002);
        assert_eq!(batch.dictionary_arena_buffer_count(), 1);
        assert_eq!(
            batch.dictionary_arena_allocation_count(),
            2,
            "the streaming builder should use one small arena allocation and one promoted arena"
        );
        assert_eq!(
            batch.dictionary_arena_large_allocation_count(),
            1,
            "promotion should reserve the only large UTF-8 arena for the batch"
        );
    }

    #[test]
    fn rebatching_borrowed_rows_does_not_retain_per_row_identity_strings() {
        let entity_pk = EntityPk::single("shared_entity");
        let batch = MaterializedLiveStateBatch::from_rows(
            (0..10_000).map(|_| row(entity_pk.clone())).collect(),
        );
        let filtered = batch.filter(|_| true, None);

        assert_eq!(filtered.len(), 10_000);
        assert_eq!(filtered.dictionary_entry_count(), 3);
        assert_eq!(
            filtered.dictionary_bytes_len(),
            batch.dictionary_bytes_len()
        );
        assert_eq!(
            filtered.row(0).schema_key().as_ptr(),
            filtered.row(9_999).schema_key().as_ptr()
        );
    }

    #[test]
    fn filtering_with_a_zero_limit_returns_no_rows() {
        let batch = MaterializedLiveStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
        ]);

        let filtered = batch.filter(|_| true, Some(0));

        assert!(filtered.is_empty());
        assert_eq!(filtered.dictionary_entry_count(), 0);
        assert_eq!(filtered.dictionary_bytes_len(), 0);
    }

    #[test]
    fn exact_present_batch_moves_identity_ordered_owner_without_rebatching() {
        let batch = MaterializedLiveStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
        ]);
        let entity_column = batch.entity_column_ptr();
        let exact = MaterializedLiveStateExactBatch::new(batch, vec![Some(0), Some(1)])
            .expect("identity slots should be valid");

        let present = exact.into_present_batch();

        assert_eq!(present.entity_column_ptr(), entity_column);
        assert_eq!(
            present
                .row(0)
                .entity_pk()
                .as_single_string()
                .expect("single key"),
            "first"
        );
        assert_eq!(
            present
                .row(1)
                .entity_pk()
                .as_single_string()
                .expect("single key"),
            "second"
        );
    }

    #[test]
    fn exact_present_batch_compacts_sparse_slots_in_request_order() {
        let batch = MaterializedLiveStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
        ]);
        let exact =
            MaterializedLiveStateExactBatch::new(batch, vec![Some(1), None, Some(0), Some(1)])
                .expect("sparse slots should be valid");

        let present = exact.into_present_batch();

        assert_eq!(present.len(), 3);
        assert_eq!(
            present
                .row(0)
                .entity_pk()
                .as_single_string()
                .expect("single key"),
            "second"
        );
        assert_eq!(
            present
                .row(1)
                .entity_pk()
                .as_single_string()
                .expect("single key"),
            "first"
        );
        assert_eq!(
            present
                .row(2)
                .entity_pk()
                .as_single_string()
                .expect("single key"),
            "second"
        );
    }
}
