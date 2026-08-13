use std::collections::HashSet;
#[cfg(test)]
use std::mem::size_of;
use std::num::NonZeroU32;
#[cfg(test)]
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use super::tracked_head::{CertifiedCurrentStatePredecessor, ColumnarBaseCoordinate};
use crate::changelog::{ChangeId, CommitId};
use crate::common::{
    FastHashBuilder, LixTimestamp, SharedStr, StringDictionary, StringDictionaryBuilder,
    fast_hash_builder,
};
use crate::entity_pk::EntityPk;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::{NullableKeyFilter, Value};

/// Terminal owned DTO for consumers that cannot yet borrow a live-state batch.
///
/// HOT materialization and visibility never use this as an intermediate. They
/// exchange [`MaterializedHotStateBatch`] owners and borrowed row views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedHotStateRow {
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

/// Columnar owner for materialized live-state rows.
///
/// This is the read-side handoff between HOT materialization, visibility, and
/// provider adaptation. Identity strings are dictionary encoded once per
/// batch; payloads retain their existing shared storage buffers. Consumers
/// operate on [`MaterializedHotStateRowRef`] views and only construct the
/// legacy owned DTO at an API boundary that still requires it.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializedHotStateBatch {
    singleton: Option<Box<MaterializedHotStateSingleton>>,
    /// Schema keys, file ids, and branch ids share one contiguous UTF-8 arena,
    /// so repeated batch-wide metadata costs a four-byte ordinal per row rather
    /// than another owned allocation.
    strings: StringDictionary,
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
    /// Lazily allocated fixed-width coordinate column. Most materialized
    /// batches contain no columnar coordinates, so they pay no per-row
    /// storage. Once present, the default/nil owner is the absent sentinel.
    columnar_base_coordinate: Option<Vec<ColumnarBaseCoordinate>>,
}

/// Row-oriented storage for the overwhelmingly common one-row point-read
/// handoff. Keeping this behind one box avoids allocating every column vector
/// and dictionary index while leaving the bulk columnar owner compact.
#[derive(Debug, Clone)]
struct MaterializedHotStateSingleton {
    row: MaterializedHotStateRow,
    durable_predecessor: Option<CertifiedCurrentStatePredecessor>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

impl MaterializedHotStateBatch {
    pub(crate) fn from_rows(rows: Vec<MaterializedHotStateRow>) -> Self {
        let (dictionary_entries, dictionary_bytes) = owned_row_dictionary_capacity(&rows);
        let mut builder = MaterializedHotStateBatchBuilder::with_dictionary_capacity(
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
        self.singleton
            .as_ref()
            .map_or_else(|| self.entity_pks.len(), |_| 1)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.singleton.is_none() && self.entity_pks.is_empty()
    }

    pub(crate) fn row(&self, index: usize) -> MaterializedHotStateRowRef<'_> {
        assert!(index < self.len(), "live-state row ordinal out of bounds");
        MaterializedHotStateRowRef { batch: self, index }
    }

    pub(crate) fn get(&self, index: usize) -> Option<MaterializedHotStateRowRef<'_>> {
        (index < self.len()).then(|| self.row(index))
    }

    pub(crate) fn iter(&self) -> MaterializedHotStateBatchIter<'_> {
        MaterializedHotStateBatchIter {
            batch: self,
            next: 0,
        }
    }

    pub(crate) fn into_rows(mut self) -> Vec<MaterializedHotStateRow> {
        if let Some(singleton) = self.singleton.take() {
            return vec![singleton.row];
        }
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
        if let Some(singleton) = self.singleton.take() {
            return vec![singleton.row.snapshot_content.map(SharedStr::into_bytes)];
        }
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

    /// Consumes the batch into entity keys ordered by their logical primary
    /// key.  The direct SQL provider uses this when its entire visible
    /// projection is primary-key columns, so no snapshot JSON needs decoding.
    pub(crate) fn into_identity_ordered_primary_keys(mut self) -> Vec<EntityPk> {
        if let Some(singleton) = self.singleton.take() {
            return vec![singleton.row.entity_pk];
        }
        let mut ordinals = (0..self.len()).collect::<Vec<_>>();
        if !ordinals.is_sorted_by(|left, right| self.entity_pks[*left] <= self.entity_pks[*right]) {
            ordinals.sort_unstable_by(|left, right| {
                self.entity_pks[*left].cmp(&self.entity_pks[*right])
            });
        }
        if ordinals.iter().copied().eq(0..self.len()) {
            return self.entity_pks;
        }
        ordinals
            .into_iter()
            .map(|index| self.entity_pks[index].clone())
            .collect()
    }

    fn terminal_branch_owners(&self) -> Vec<Option<Arc<str>>> {
        if let Some(singleton) = &self.singleton {
            return vec![Some(Arc::clone(&singleton.row.branch_id))];
        }
        let mut owners = vec![None; self.strings.len()];
        for branch_id in &self.branch_ids {
            let ordinal = branch_id.0 as usize;
            if owners[ordinal].is_none() {
                owners[ordinal] = Some(Arc::from(self.strings.get(branch_id.0)));
            }
        }
        owners
    }

    fn branch_owner_ordinal(&self, index: usize) -> usize {
        self.singleton
            .as_ref()
            .map_or_else(|| self.branch_ids[index].0 as usize, |_| 0)
    }

    /// Drops every row rejected by `keep`, compacting the columns this batch
    /// already owns.
    ///
    /// **This consumes the batch on purpose.** Filtering used to build a
    /// second columnar owner row by row, which cloned every `SharedStr` and
    /// every `EntityPk` component of every surviving row — one atomic
    /// increment per shared buffer per row, plus the matching decrement when
    /// the source batch was dropped. None of that traffic moves any bytes.
    /// Compacting in place *moves* the same buffers instead, so a filter now
    /// costs no refcount traffic at all.
    ///
    /// The string dictionary is deliberately left uncompacted. It holds one
    /// entry per *distinct* schema key, file id and branch id rather than one
    /// per row, so a dropped row strands no per-row allocation, and every
    /// surviving row keeps the ordinal it was built with.
    pub(crate) fn filter(
        mut self,
        mut keep: impl FnMut(MaterializedHotStateRowRef<'_>) -> bool,
        limit: Option<usize>,
    ) -> Self {
        if limit == Some(0) {
            return Self::default();
        }
        if self.singleton.is_some() {
            return if keep(self.row(0)) {
                self
            } else {
                Self::default()
            };
        }
        let limit = limit.unwrap_or(usize::MAX);
        let mut kept = 0_usize;
        let mut mask = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            // Short-circuits once the limit is reached, so `keep` observes the
            // same prefix of rows a row-by-row rebuild would have shown it.
            let retain = kept < limit && keep(self.row(index));
            kept += usize::from(retain);
            mask.push(retain);
        }
        if kept == mask.len() {
            return self;
        }
        if kept == 0 {
            return Self::default();
        }
        retain_by_mask(&mut self.schema_keys, &mask);
        retain_by_mask(&mut self.file_ids, &mask);
        retain_by_mask(&mut self.branch_ids, &mask);
        retain_by_mask(&mut self.entity_pks, &mask);
        retain_by_mask(&mut self.snapshot_content, &mask);
        retain_by_mask(&mut self.metadata, &mask);
        retain_by_mask(&mut self.deleted, &mask);
        retain_by_mask(&mut self.created_at, &mask);
        retain_by_mask(&mut self.updated_at, &mask);
        retain_by_mask(&mut self.global, &mask);
        retain_by_mask(&mut self.change_id, &mask);
        retain_by_mask(&mut self.commit_id, &mask);
        retain_by_mask(&mut self.untracked, &mask);
        retain_by_mask(&mut self.durable_predecessor, &mask);
        if let Some(coordinates) = self.columnar_base_coordinate.as_mut() {
            retain_by_mask(coordinates, &mask);
        }
        debug_assert_eq!(self.entity_pks.len(), kept);
        self
    }

    #[cfg(test)]
    pub(crate) fn dictionary_entry_count(&self) -> usize {
        if let Some(singleton) = &self.singleton {
            let row = &singleton.row;
            return 2 + usize::from(row.file_id.is_some())
                - usize::from(row.schema_key == row.branch_id.as_ref())
                - usize::from(row.file_id.as_deref().is_some_and(|file_id| {
                    file_id == row.schema_key || file_id == row.branch_id.as_ref()
                }));
        }
        self.strings.len()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_bytes_len(&self) -> usize {
        if let Some(singleton) = &self.singleton {
            let row = &singleton.row;
            let mut bytes = row.schema_key.len();
            if row.branch_id.as_ref() != row.schema_key {
                bytes += row.branch_id.len();
            }
            if let Some(file_id) = row.file_id.as_deref()
                && file_id != row.schema_key
                && file_id != row.branch_id.as_ref()
            {
                bytes += file_id.len();
            }
            return bytes;
        }
        self.strings.byte_len()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_buffer_count(&self) -> usize {
        if self.singleton.is_some() {
            return 0;
        }
        usize::from(!self.strings.is_arena_empty())
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_allocation_count(&self) -> usize {
        if self.singleton.is_some() {
            return 0;
        }
        self.strings.arena_allocation_count()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_large_allocation_count(&self) -> usize {
        if self.singleton.is_some() {
            return 0;
        }
        self.strings.arena_large_allocation_count()
    }

    #[cfg(test)]
    pub(crate) fn entity_column_ptr(&self) -> *const EntityPk {
        if let Some(singleton) = &self.singleton {
            return &singleton.row.entity_pk;
        }
        self.entity_pks.as_ptr()
    }

    #[cfg(test)]
    fn large_column_allocation_count(&self, threshold: usize) -> usize {
        if self.singleton.is_some() {
            return usize::from(size_of::<MaterializedHotStateSingleton>() >= threshold);
        }
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
            self.columnar_base_coordinate
                .as_ref()
                .map_or(0, |coordinates| {
                    coordinates.capacity() * size_of::<ColumnarBaseCoordinate>()
                }),
            self.strings.byte_len(),
            self.strings.ranges_capacity() * size_of::<Range<u32>>(),
        ]
        .into_iter()
        .filter(|bytes| *bytes >= threshold)
        .count()
    }
}

impl From<Vec<MaterializedHotStateRow>> for MaterializedHotStateBatch {
    fn from(rows: Vec<MaterializedHotStateRow>) -> Self {
        Self::from_rows(rows)
    }
}

/// One borrowed row view over a [`MaterializedHotStateBatch`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaterializedHotStateRowRef<'a> {
    batch: &'a MaterializedHotStateBatch,
    index: usize,
}

impl<'a> MaterializedHotStateRowRef<'a> {
    fn singleton(self) -> Option<&'a MaterializedHotStateSingleton> {
        self.batch.singleton.as_deref()
    }

    pub(crate) fn entity_pk(self) -> &'a EntityPk {
        self.singleton().map_or_else(
            || &self.batch.entity_pks[self.index],
            |singleton| &singleton.row.entity_pk,
        )
    }

    pub(crate) fn schema_key(self) -> &'a str {
        self.singleton().map_or_else(
            || self.batch.strings.get(self.batch.schema_keys[self.index].0),
            |singleton| singleton.row.schema_key.as_str(),
        )
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.singleton().map_or_else(
            || {
                self.batch.file_ids[self.index]
                    .map(|ordinal| self.batch.strings.get(ordinal.ordinal()))
            },
            |singleton| singleton.row.file_id.as_deref(),
        )
    }

    pub(crate) fn snapshot_content(self) -> Option<&'a SharedStr> {
        self.singleton().map_or_else(
            || self.batch.snapshot_content[self.index].as_ref(),
            |singleton| singleton.row.snapshot_content.as_ref(),
        )
    }

    pub(crate) fn metadata(self) -> Option<&'a SharedStr> {
        self.singleton().map_or_else(
            || self.batch.metadata[self.index].as_ref(),
            |singleton| singleton.row.metadata.as_ref(),
        )
    }

    pub(crate) fn deleted(self) -> bool {
        self.singleton().map_or_else(
            || self.batch.deleted[self.index],
            |singleton| singleton.row.deleted,
        )
    }

    pub(crate) fn created_at(self) -> LixTimestamp {
        self.singleton().map_or_else(
            || self.batch.created_at[self.index],
            |singleton| singleton.row.created_at,
        )
    }

    pub(crate) fn updated_at(self) -> LixTimestamp {
        self.singleton().map_or_else(
            || self.batch.updated_at[self.index],
            |singleton| singleton.row.updated_at,
        )
    }

    pub(crate) fn global(self) -> bool {
        self.singleton().map_or_else(
            || self.batch.global[self.index],
            |singleton| singleton.row.global,
        )
    }

    pub(crate) fn change_id(self) -> Option<ChangeId> {
        self.singleton().map_or_else(
            || self.batch.change_id[self.index],
            |singleton| singleton.row.change_id,
        )
    }

    pub(crate) fn commit_id(self) -> Option<CommitId> {
        self.singleton().map_or_else(
            || self.batch.commit_id[self.index],
            |singleton| singleton.row.commit_id,
        )
    }

    pub(crate) fn untracked(self) -> bool {
        self.singleton().map_or_else(
            || self.batch.untracked[self.index],
            |singleton| singleton.row.untracked,
        )
    }

    pub(crate) fn durable_predecessor(self) -> Option<&'a CertifiedCurrentStatePredecessor> {
        self.singleton().map_or_else(
            || self.batch.durable_predecessor[self.index].as_ref(),
            |singleton| singleton.durable_predecessor.as_ref(),
        )
    }

    pub(crate) fn columnar_base_coordinate(self) -> Option<ColumnarBaseCoordinate> {
        self.singleton().map_or_else(
            || {
                let coordinate = self
                    .batch
                    .columnar_base_coordinate
                    .as_ref()?
                    .get(self.index)
                    .copied()?;
                (coordinate.base_commit_id != CommitId::default()).then_some(coordinate)
            },
            |singleton| singleton.columnar_base_coordinate,
        )
    }

    pub(crate) fn branch_id(self) -> &'a str {
        self.singleton().map_or_else(
            || self.batch.strings.get(self.batch.branch_ids[self.index].0),
            |singleton| singleton.row.branch_id.as_ref(),
        )
    }

    fn branch_owner(self) -> Arc<str> {
        self.singleton().map_or_else(
            || Arc::from(self.branch_id()),
            |singleton| Arc::clone(&singleton.row.branch_id),
        )
    }

    /// Materializes an owned row at a scalar or persistent-index boundary.
    ///
    /// Batch pipeline stages should retain the batch owner and borrow this
    /// view instead. This conversion deliberately remains explicit so an
    /// accidental row-owned intermediate is visible at its call site.
    pub(crate) fn to_owned(self) -> MaterializedHotStateRow {
        self.to_owned_with_branch(Arc::from(self.branch_id()))
    }

    fn to_owned_with_branch(self, branch_id: Arc<str>) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
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

pub(crate) struct MaterializedHotStateBatchIter<'a> {
    batch: &'a MaterializedHotStateBatch,
    next: usize,
}

impl<'a> Iterator for MaterializedHotStateBatchIter<'a> {
    type Item = MaterializedHotStateRowRef<'a>;

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

impl ExactSizeIterator for MaterializedHotStateBatchIter<'_> {}

/// Aligned exact-read result. Missing slots are represented by `None`; present
/// slots point into one compact materialized batch.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializedHotStateExactBatch {
    batch: MaterializedHotStateBatch,
    slots: Vec<Option<u32>>,
}

impl MaterializedHotStateExactBatch {
    pub(crate) fn new(
        batch: MaterializedHotStateBatch,
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
    pub(crate) fn from_rows(rows: Vec<Option<MaterializedHotStateRow>>) -> Self {
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(rows.len());
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

    pub(crate) fn row(&self, slot: usize) -> Option<MaterializedHotStateRowRef<'_>> {
        self.slots
            .get(slot)
            .copied()
            .flatten()
            .map(|ordinal| self.batch.row(ordinal as usize))
    }

    pub(crate) fn filter(
        &self,
        mut keep: impl FnMut(MaterializedHotStateRowRef<'_>) -> bool,
    ) -> Result<Self, crate::LixError> {
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(self.len());
        let mut slots = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            let Some(row) = self.row(index).filter(|row| keep(*row)) else {
                slots.push(None);
                continue;
            };
            let ordinal = u32::try_from(builder.push_ref(row, None)).map_err(|_| {
                crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    "exact live-state result exceeds u32 rows",
                )
            })?;
            slots.push(Some(ordinal));
        }
        Self::new(builder.finish(), slots)
    }

    /// Consumes an aligned exact result into one compact owner containing only
    /// present rows in request order.
    ///
    /// Durable readers normally already produce identity-ordered slots, in
    /// which case this is a zero-copy move of the underlying batch. Sparse or
    /// deduplicated results are compacted with one batch builder rather than a
    /// `Vec<Option<MaterializedHotStateRow>>` intermediate.
    pub(crate) fn into_present_batch(self) -> MaterializedHotStateBatch {
        let Self { batch, slots } = self;
        if slots.len() == batch.len()
            && slots
                .iter()
                .enumerate()
                .all(|(index, slot)| *slot == u32::try_from(index).ok())
        {
            return batch;
        }

        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(
            slots.iter().filter(|slot| slot.is_some()).count(),
        );
        for ordinal in slots.into_iter().flatten() {
            builder.push_ref(batch.row(ordinal as usize), None);
        }
        builder.finish()
    }

    pub(crate) fn into_rows(self) -> Vec<Option<MaterializedHotStateRow>> {
        let branch_ids = self.batch.terminal_branch_owners();
        self.slots
            .iter()
            .map(|ordinal| {
                ordinal.map(|ordinal| {
                    let index = ordinal as usize;
                    let branch_ordinal = self.batch.branch_owner_ordinal(index);
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

/// Keeps the elements whose `mask` entry is `true`, preserving order.
///
/// `Vec::retain` visits elements in their original order, so one shared mask
/// compacts every parallel column of a batch identically. Elements are moved,
/// never cloned, which is the whole point of filtering in place.
fn retain_by_mask<T>(values: &mut Vec<T>, mask: &[bool]) {
    debug_assert_eq!(values.len(), mask.len());
    let mut index = 0_usize;
    values.retain(|_| {
        let retain = mask[index];
        index += 1;
        retain
    });
}

fn owned_row_dictionary_capacity(rows: &[MaterializedHotStateRow]) -> (usize, usize) {
    let mut seen = HashSet::<&str, FastHashBuilder>::with_capacity_and_hasher(
        rows.len().saturating_mul(3),
        fast_hash_builder(),
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

/// Temporary builder for a columnar materialized batch.
///
/// Distinct identity values are appended directly to one UTF-8 arena. Small
/// dictionaries use a linear range lookup; larger dictionaries promote to one
/// hash table whose entries are compact arena ordinals. Finish transfers the
/// arena into the immutable batch without copying it.
pub(crate) struct MaterializedHotStateBatchBuilder {
    singleton_capacity: bool,
    singleton: Option<Box<MaterializedHotStateSingleton>>,
    strings: StringDictionaryBuilder,
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
    columnar_base_coordinate: Option<Vec<ColumnarBaseCoordinate>>,
}

impl MaterializedHotStateBatchBuilder {
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
        let singleton_capacity = capacity == 1;
        let column_capacity = if singleton_capacity { 0 } else { capacity };
        Self {
            singleton_capacity,
            singleton: None,
            strings: StringDictionaryBuilder::with_capacity(
                // Every row contributes at most a schema key, a file id and a
                // branch id.
                column_capacity.saturating_mul(3),
                if singleton_capacity {
                    0
                } else {
                    dictionary_entry_capacity
                },
                if singleton_capacity {
                    0
                } else {
                    dictionary_byte_capacity
                },
                exact_byte_capacity,
            ),
            schema_keys: Vec::with_capacity(column_capacity),
            file_ids: Vec::with_capacity(column_capacity),
            branch_ids: Vec::with_capacity(column_capacity),
            entity_pks: Vec::with_capacity(column_capacity),
            snapshot_content: Vec::with_capacity(column_capacity),
            metadata: Vec::with_capacity(column_capacity),
            deleted: Vec::with_capacity(column_capacity),
            created_at: Vec::with_capacity(column_capacity),
            updated_at: Vec::with_capacity(column_capacity),
            global: Vec::with_capacity(column_capacity),
            change_id: Vec::with_capacity(column_capacity),
            commit_id: Vec::with_capacity(column_capacity),
            untracked: Vec::with_capacity(column_capacity),
            durable_predecessor: Vec::with_capacity(column_capacity),
            columnar_base_coordinate: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.singleton
            .as_ref()
            .map_or_else(|| self.entity_pks.len(), |_| 1)
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        self.strings.intern_owned(value)
    }

    fn intern_ref(&mut self, value: &str) -> u32 {
        self.strings.intern(value)
    }

    pub(crate) fn push_owned(&mut self, row: MaterializedHotStateRow) {
        if self.singleton_capacity && self.singleton.is_none() && self.entity_pks.is_empty() {
            self.singleton = Some(Box::new(MaterializedHotStateSingleton {
                row,
                durable_predecessor: None,
                columnar_base_coordinate: None,
            }));
            return;
        }
        self.promote_singleton();
        self.push_owned_columnar(row, None, None);
    }

    fn promote_singleton(&mut self) {
        let Some(singleton) = self.singleton.take() else {
            self.singleton_capacity = false;
            return;
        };
        self.singleton_capacity = false;
        self.push_owned_columnar(
            singleton.row,
            singleton.durable_predecessor,
            singleton.columnar_base_coordinate,
        );
    }

    fn push_owned_columnar(
        &mut self,
        row: MaterializedHotStateRow,
        durable_predecessor: Option<CertifiedCurrentStatePredecessor>,
        columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
    ) {
        let MaterializedHotStateRow {
            entity_pk,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        } = row;
        let schema_key = SchemaKeyId(self.intern_owned(schema_key));
        let file_id = file_id.map(|file_id| FileIdId::from_ordinal(self.intern_owned(file_id)));
        let branch_id = BranchIdId(self.intern_ref(branch_id.as_ref()));
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
        *self
            .durable_predecessor
            .last_mut()
            .expect("pushed live-state row has a predecessor slot") = durable_predecessor;
        if let Some(coordinate) = columnar_base_coordinate {
            self.set_columnar_base_coordinate(self.len() - 1, coordinate);
        }
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
        if self.singleton_capacity {
            self.push_owned(MaterializedHotStateRow {
                entity_pk,
                schema_key,
                file_id,
                snapshot_content,
                metadata,
                deleted,
                created_at,
                updated_at,
                global,
                change_id,
                commit_id,
                untracked,
                branch_id: Arc::from(branch_id),
            });
            return ordinal;
        }
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
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_hot_scan_row_handle_clones(entity_pk.shared_handle_count());
        }
        self.push_materialized_interned(
            entity_pk.clone(),
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        )
    }

    /// Appends a row whose identity strings are interned from borrows but
    /// whose primary key is **moved** into the column.
    ///
    /// A decoded HOT scan row already owns its `EntityPk`, and every component
    /// of that key is a `Bytes` slice of the retained physical key. Handing it
    /// to [`Self::push_materialized_ref`] clones each component — an atomic
    /// increment per component per row, immediately followed by the matching
    /// decrement when the decoded row is dropped — to produce a value the
    /// caller was about to discard anyway. Moving it costs nothing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_materialized_interned(
        &mut self,
        entity_pk: EntityPk,
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
        if self.singleton_capacity {
            self.push_owned(MaterializedHotStateRow {
                entity_pk,
                schema_key: schema_key.to_owned(),
                file_id: file_id.map(str::to_owned),
                snapshot_content,
                metadata,
                deleted,
                created_at,
                updated_at,
                global,
                change_id,
                commit_id,
                untracked,
                branch_id: Arc::from(branch_id),
            });
            return ordinal;
        }
        let schema_key = SchemaKeyId(self.intern_ref(schema_key));
        let file_id = file_id.map(|file_id| FileIdId::from_ordinal(self.intern_ref(file_id)));
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

    pub(crate) fn push_ref(
        &mut self,
        row: MaterializedHotStateRowRef<'_>,
        branch_override: Option<&str>,
    ) -> usize {
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_hot_scan_row_handle_clones(
                row.entity_pk().shared_handle_count()
                    + usize::from(row.snapshot_content().is_some())
                    + usize::from(row.metadata().is_some()),
            );
        }
        let ordinal = self.len();
        if self.singleton_capacity {
            let branch_id = branch_override.map_or_else(|| row.branch_owner(), Arc::from);
            let durable_predecessor = row.durable_predecessor().cloned();
            let columnar_base_coordinate = row.columnar_base_coordinate();
            self.push_owned(row.to_owned_with_branch(branch_id));
            if let Some(durable_predecessor) = durable_predecessor {
                self.set_durable_predecessor(ordinal, durable_predecessor);
            }
            if let Some(coordinate) = columnar_base_coordinate {
                self.set_columnar_base_coordinate(ordinal, coordinate);
            }
            return ordinal;
        }
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
        if let Some(coordinate) = row.columnar_base_coordinate() {
            self.set_columnar_base_coordinate(ordinal, coordinate);
        }
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
        if let Some(coordinates) = &mut self.columnar_base_coordinate {
            coordinates.push(ColumnarBaseCoordinate::default());
        }
    }

    pub(crate) fn set_snapshot_content(&mut self, row: usize, value: SharedStr) {
        if let Some(singleton) = self.singleton.as_mut() {
            assert_eq!(row, 0, "singleton live-state row ordinal must be zero");
            singleton.row.snapshot_content = Some(value);
            return;
        }
        self.snapshot_content[row] = Some(value);
    }

    pub(crate) fn set_metadata(&mut self, row: usize, value: SharedStr) {
        if let Some(singleton) = self.singleton.as_mut() {
            assert_eq!(row, 0, "singleton live-state row ordinal must be zero");
            singleton.row.metadata = Some(value);
            return;
        }
        self.metadata[row] = Some(value);
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        row: usize,
        value: CertifiedCurrentStatePredecessor,
    ) {
        if let Some(singleton) = self.singleton.as_mut() {
            assert_eq!(row, 0, "singleton live-state row ordinal must be zero");
            singleton.durable_predecessor = Some(value);
            return;
        }
        self.durable_predecessor[row] = Some(value);
    }

    pub(crate) fn set_columnar_base_coordinate(
        &mut self,
        row: usize,
        value: ColumnarBaseCoordinate,
    ) {
        if let Some(singleton) = self.singleton.as_mut() {
            assert_eq!(row, 0, "singleton live-state row ordinal must be zero");
            singleton.columnar_base_coordinate = Some(value);
            return;
        }
        assert!(row < self.len(), "live-state row ordinal out of bounds");
        self.columnar_base_coordinate.get_or_insert_with(|| {
            vec![ColumnarBaseCoordinate::default(); self.entity_pks.len()]
        })[row] = value;
    }

    pub(crate) fn finish(self) -> MaterializedHotStateBatch {
        MaterializedHotStateBatch {
            singleton: self.singleton,
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
            columnar_base_coordinate: self.columnar_base_coordinate,
        }
    }
}

impl TryFrom<&MaterializedHotStateRow> for MaterializedTrackedStateRow {
    type Error = crate::LixError;

    fn try_from(row: &MaterializedHotStateRow) -> Result<Self, Self::Error> {
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

/// A membership predicate on a column the schema declares as unique or as a
/// foreign key, addressed by its stable ordinal in the schema's index.
///
/// `values` holds one value for an `=` predicate and several for an `IN` list.
/// The set is a disjunction: a row qualifies when its indexed column equals
/// any member.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeclaredColumnEq {
    pub(crate) schema_key: String,
    pub(crate) ordinal: u16,
    pub(crate) values: Vec<crate::hot_state::HotIndexValue>,
}

/// Identity-centered filter for visible live entities.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct HotStateFilter {
    #[serde(default)]
    pub(crate) rows: HotStateRowFilter,
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
    /// Equality on a declared column, to be served by the hot index plane.
    ///
    /// Resolved into [`Self::entity_pks`] before any scan route is chosen, so
    /// no route below this ever sees it. The predicate is *not* removed from
    /// the caller's own filtering when this is set: index entries are
    /// candidates, so the caller's predicate is what rejects stale ones.
    #[serde(default)]
    pub(crate) declared_column_eq: Option<DeclaredColumnEq>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) enum HotStateRowFilter {
    #[default]
    All,
    None,
}

/// Requested property set for a live-state scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct HotStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// First-principles scan request for engine-owned reads.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct HotStateScanRequest {
    #[serde(default)]
    pub(crate) filter: HotStateFilter,
    #[serde(default)]
    pub(crate) projection: HotStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one visible live-state row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HotStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: NullableKeyFilter<String>,
}

/// One concrete visible-row identity in an exact batch read.
///
/// Unlike [`HotStateFilter`], the identity fields in this request are
/// correlated. Implementations must never expand multiple requests into the
/// Cartesian product of their schema, entity, and file dimensions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct HotStateExactRowRequest {
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
pub(crate) struct HotStateExactBatchRequest {
    pub(crate) rows: Vec<HotStateExactRowRequest>,
    pub(crate) projection: HotStateProjection,
    pub(crate) untracked: Option<bool>,
    pub(crate) include_tombstones: bool,
}

impl HotStateExactBatchRequest {
    pub(crate) fn row_scan_request(&self, row: &HotStateExactRowRequest) -> HotStateScanRequest {
        HotStateScanRequest {
            filter: HotStateFilter {
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
                ..HotStateFilter::default()
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
pub(crate) struct HotStateRowIdentityRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) file_id: Option<&'a str>,
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    fn row(entity_pk: EntityPk) -> MaterializedHotStateRow {
        let timestamp = LixTimestamp::expect_parse("batch test timestamp", "2026-01-01T00:00:00Z");
        MaterializedHotStateRow {
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
        let batch = MaterializedHotStateBatch::from_rows(
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
    fn one_row_builder_uses_boxed_singleton_storage() {
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(1);
        builder.push_owned(row(EntityPk::single("only")));
        builder.set_snapshot_content(0, SharedStr::from_static(r#"{"path":"only"}"#));
        builder.set_metadata(0, SharedStr::from_static(r#"{"source":"test"}"#));

        let batch = builder.finish();

        assert!(batch.singleton.is_some());
        assert!(batch.entity_pks.is_empty());
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.row(0).schema_key(), "shared_schema");
        assert_eq!(batch.row(0).file_id(), Some("shared_file"));
        assert_eq!(batch.row(0).branch_id(), "shared_branch");
        assert_eq!(
            batch.row(0).snapshot_content().map(AsRef::as_ref),
            Some(r#"{"path":"only"}"#)
        );
        assert_eq!(
            batch.row(0).metadata().map(AsRef::as_ref),
            Some(r#"{"source":"test"}"#)
        );
    }

    #[test]
    fn singleton_builder_promotes_when_capacity_hint_is_exceeded() {
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(1);
        let coordinate = ColumnarBaseCoordinate {
            base_commit_id: CommitId::for_test_label("batch-coordinate-base"),
            group_index: 3,
            row_index: 19,
        };
        builder.push_owned(row(EntityPk::single("first")));
        builder.set_columnar_base_coordinate(0, coordinate);
        builder.push_owned(row(EntityPk::single("second")));

        let batch = builder.finish();

        assert!(batch.singleton.is_none());
        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch.row(0).entity_pk().as_single_string().unwrap(),
            "first"
        );
        assert_eq!(
            batch.row(1).entity_pk().as_single_string().unwrap(),
            "second"
        );
        assert_eq!(batch.row(0).columnar_base_coordinate(), Some(coordinate));
        assert_eq!(batch.row(1).columnar_base_coordinate(), None);
    }

    #[test]
    fn coordinate_free_multi_row_batch_does_not_allocate_coordinate_column() {
        let batch = MaterializedHotStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
            row(EntityPk::single("third")),
        ]);

        assert!(batch.singleton.is_none());
        assert!(batch.columnar_base_coordinate.is_none());
        assert!(
            batch
                .iter()
                .all(|row| row.columnar_base_coordinate().is_none())
        );
    }

    #[test]
    fn late_coordinate_allocation_backfills_existing_rows_and_extends_with_none() {
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(4);
        builder.push_owned(row(EntityPk::single("first")));
        builder.push_owned(row(EntityPk::single("second")));
        builder.push_owned(row(EntityPk::single("third")));
        assert!(builder.columnar_base_coordinate.is_none());

        let coordinate = ColumnarBaseCoordinate {
            base_commit_id: CommitId::for_test_label("late-coordinate-base"),
            group_index: 7,
            row_index: 23,
        };
        builder.set_columnar_base_coordinate(1, coordinate);
        builder.push_owned(row(EntityPk::single("fourth")));

        let batch = builder.finish();
        assert_eq!(
            batch.columnar_base_coordinate.as_ref().map(Vec::len),
            Some(4)
        );
        assert_eq!(batch.row(0).columnar_base_coordinate(), None);
        assert_eq!(batch.row(1).columnar_base_coordinate(), Some(coordinate));
        assert_eq!(batch.row(2).columnar_base_coordinate(), None);
        assert_eq!(batch.row(3).columnar_base_coordinate(), None);
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
            MaterializedHotStateBatch::from_rows(vec![source]).into_identity_ordered_snapshots();

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

        let snapshots = MaterializedHotStateBatch::from_rows(vec![second, first])
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

        let batch = MaterializedHotStateBatch::from_rows(rows);

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
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(10_000);
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
        let batch = MaterializedHotStateBatch::from_rows(
            (0..10_000).map(|_| row(entity_pk.clone())).collect(),
        );
        let dictionary_bytes_len = batch.dictionary_bytes_len();
        let filtered = batch.filter(|_| true, None);

        assert_eq!(filtered.len(), 10_000);
        assert_eq!(filtered.dictionary_entry_count(), 3);
        assert_eq!(filtered.dictionary_bytes_len(), dictionary_bytes_len);
        assert_eq!(
            filtered.row(0).schema_key().as_ptr(),
            filtered.row(9_999).schema_key().as_ptr()
        );
    }

    #[test]
    fn filtering_moves_surviving_rows_instead_of_cloning_their_buffers() {
        let shared = EntityPk::single("shared_entity");
        let batch = MaterializedHotStateBatch::from_rows(
            (0..1_000)
                .map(|index| {
                    row(if index % 2 == 0 {
                        shared.clone()
                    } else {
                        EntityPk::single("dropped_entity")
                    })
                })
                .collect(),
        );
        let survivors = batch
            .iter()
            .filter(|row| row.entity_pk() == &shared)
            .count();
        assert_eq!(survivors, 500);
        let entity_column = batch.entity_column_ptr();

        let filtered = batch.filter(|row| row.entity_pk() == &shared, None);

        assert_eq!(filtered.len(), 500);
        assert!(filtered.iter().all(|row| row.entity_pk() == &shared));
        // The surviving rows still live in the allocation they were built in.
        // A row-by-row rebuild would have cloned every `EntityPk` component
        // and every `SharedStr` into a second owner, which is the atomic
        // refcount traffic this filter exists to avoid.
        assert_eq!(filtered.entity_column_ptr(), entity_column);
        // The dictionary is carried over rather than rebuilt, so every
        // surviving row still points at the same interned schema key.
        assert_eq!(
            filtered.row(0).schema_key().as_ptr(),
            filtered.row(499).schema_key().as_ptr()
        );
    }

    #[test]
    fn filtering_stops_calling_the_predicate_once_the_limit_is_reached() {
        let batch = MaterializedHotStateBatch::from_rows(
            (0..16).map(|_| row(EntityPk::single("row"))).collect(),
        );
        let mut visited = 0_usize;

        let filtered = batch.filter(
            |_| {
                visited += 1;
                true
            },
            Some(4),
        );

        assert_eq!(filtered.len(), 4);
        assert_eq!(visited, 4);
    }

    #[test]
    fn filtering_with_a_zero_limit_returns_no_rows() {
        let batch = MaterializedHotStateBatch::from_rows(vec![
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
        let batch = MaterializedHotStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
        ]);
        let entity_column = batch.entity_column_ptr();
        let exact = MaterializedHotStateExactBatch::new(batch, vec![Some(0), Some(1)])
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
        let batch = MaterializedHotStateBatch::from_rows(vec![
            row(EntityPk::single("first")),
            row(EntityPk::single("second")),
        ]);
        let exact =
            MaterializedHotStateExactBatch::new(batch, vec![Some(1), None, Some(0), Some(1)])
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
