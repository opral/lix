use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::storage::{
    BufferRange, CommitResult, EncodedMutationBatch, Key, KeyRange, PutBatch, PutEntry, Storage,
    StorageError, StorageWrite, StoredValue, WriteOptions,
};
use crate::storage_adapter::{StorageSpace, StorageWriteSetStats};
use ahash::RandomState;
use bytes::Bytes;

type FastHashBuilder = RandomState;
pub trait IntoStorageSpace {
    fn into_storage_space(self) -> StorageSpace;
}

impl IntoStorageSpace for StorageSpace {
    fn into_storage_space(self) -> StorageSpace {
        self
    }
}

pub trait IntoStorageKey {
    fn into_storage_key(self) -> Key;
}

impl IntoStorageKey for Key {
    fn into_storage_key(self) -> Key {
        self
    }
}

impl IntoStorageKey for Vec<u8> {
    fn into_storage_key(self) -> Key {
        Key(Bytes::from(self))
    }
}

impl IntoStorageKey for &[u8] {
    fn into_storage_key(self) -> Key {
        Key(Bytes::copy_from_slice(self))
    }
}

pub trait IntoStorageValue {
    fn into_storage_value(self) -> StoredValue;
}

impl IntoStorageValue for StoredValue {
    fn into_storage_value(self) -> StoredValue {
        self
    }
}

impl IntoStorageValue for Vec<u8> {
    fn into_storage_value(self) -> StoredValue {
        StoredValue {
            bytes: Bytes::from(self),
        }
    }
}

impl IntoStorageValue for &[u8] {
    fn into_storage_value(self) -> StoredValue {
        StoredValue {
            bytes: Bytes::copy_from_slice(self),
        }
    }
}

pub struct StorageWriteSet {
    groups: Vec<StorageWriteGroup>,
    group_index: HashMap<u32, usize, FastHashBuilder>,
    exclusive_range_deletes: Vec<(StorageSpace, KeyRange)>,
    stats: StorageWriteSetStats,
    // Domain stores can seal a write lane after planning a destructive sweep.
    // The flag carries no storage representation; it only prevents a later
    // domain writer sharing this canonical write set from invalidating the
    // sweep's reachability proof before commit.
    changelog_gc_sealed: bool,
}

impl fmt::Debug for StorageWriteSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageWriteSet")
            .field("groups", &self.groups)
            .field("exclusive_range_deletes", &self.exclusive_range_deletes)
            .field("stats", &self.stats)
            .field("changelog_gc_sealed", &self.changelog_gc_sealed)
            .finish_non_exhaustive()
    }
}

/// One bounded page produced by a storage-native owner at final lowering.
///
/// The source has already validated logical uniqueness and ownership. Pages
/// are deliberately restricted to final point puts so they cannot interact
/// with a later range deletion in the same backend transaction.
#[derive(Clone, Debug)]
struct StorageWriteGroup {
    space: StorageSpace,
    key_arena: MutationArena,
    value_arena: MutationArena,
    puts: Vec<StagedPut>,
    deletes: Vec<ArenaRange>,
    conflicting_declarations: Vec<StorageSpace>,
}

#[derive(Clone, Debug, Default)]
struct MutationArena {
    shared: Vec<Bytes>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArenaRange {
    buffer_index: u32,
    offset: u32,
    length: u32,
}

impl ArenaRange {
    fn new(buffer_index: usize, range: BufferRange) -> Self {
        Self {
            buffer_index: u32::try_from(buffer_index)
                .expect("mutation arena buffer count fits u32"),
            offset: u32::try_from(range.offset()).expect("mutation arena offset fits u32"),
            length: u32::try_from(range.len()).expect("mutation arena range length fits u32"),
        }
    }

    fn buffer_index(self) -> usize {
        self.buffer_index as usize
    }

    fn offset(self) -> usize {
        self.offset as usize
    }

    fn len(self) -> usize {
        self.length as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StagedPut {
    key: ArenaRange,
    value: ArenaRange,
}

#[derive(Clone, Copy)]
enum MutationIndex {
    Put(usize),
    Delete(usize),
}

#[derive(Hash, PartialEq, Eq)]
struct ContentAddressedRef<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

#[derive(Hash, PartialEq, Eq)]
struct ArenaRemap {
    shared_buffer_base: usize,
}

struct FrozenMutationArena {
    shared: Vec<Bytes>,
}

#[cfg(any(test, feature = "storage-benches"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteSetArenaStats {
    pub spaces: usize,
    pub put_descriptors: usize,
    pub delete_descriptors: usize,
    pub put_descriptor_capacity: usize,
    pub delete_descriptor_capacity: usize,
    pub key_inline_bytes: usize,
    pub key_inline_capacity: usize,
    pub key_inline_allocations: usize,
    pub key_shared_buffers: usize,
    pub key_shared_bytes: usize,
    pub key_shared_capacity: usize,
    pub value_inline_bytes: usize,
    pub value_inline_capacity: usize,
    pub value_inline_allocations: usize,
    pub value_shared_buffers: usize,
    pub value_shared_bytes: usize,
    pub value_shared_capacity: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageWriteSetError {
    ConflictingSpaceDeclaration {
        existing: StorageSpace,
        incoming: StorageSpace,
    },
    DuplicateMutation {
        space: StorageSpace,
        key: Key,
    },
    Storage(StorageError),
}

impl StorageWriteSet {
    /// Creates an empty canonical write set.
    ///
    /// Callers must stage at most one final mutation for each `(space, key)`.
    /// The set validates that contract before lowering or commit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a canonical write set with capacity hints.
    pub fn with_capacity(_expected_mutations: usize, expected_spaces: usize) -> Self {
        Self {
            groups: Vec::with_capacity(expected_spaces),
            group_index: HashMap::with_capacity_and_hasher(
                expected_spaces,
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            ),
            exclusive_range_deletes: Vec::new(),
            stats: StorageWriteSetStats::default(),
            changelog_gc_sealed: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exclusive_range_deletes.is_empty()
            && self
                .groups
                .iter()
                .all(|group| group.puts.is_empty() && group.deletes.is_empty())
    }

    /// Conservative encoded-size hint for contiguous backend write batches.
    ///
    /// Sixteen bytes per mutation covers a space prefix, record tag, and
    /// length prefixes for current backends. The fixed tail covers the batch
    /// header and the mutation-revision record appended by the adapter.
    pub(crate) fn backend_batch_capacity_hint_bytes(&self) -> usize {
        let ordinary = self.groups.iter().fold(64_usize, |total, group| {
            let puts = group.puts.iter().fold(0_usize, |bytes, put| {
                bytes
                    .saturating_add(group.key_bytes(put.key).len())
                    .saturating_add(group.value_bytes(put.value).len())
                    .saturating_add(16)
            });
            let deletes = group.deletes.iter().fold(0_usize, |bytes, key| {
                bytes
                    .saturating_add(group.key_bytes(*key).len())
                    .saturating_add(16)
            });
            total.saturating_add(puts).saturating_add(deletes)
        });
        ordinary
    }

    pub fn put<S, K, V>(&mut self, space: S, key: K, value: V)
    where
        S: IntoStorageSpace,
        K: IntoStorageKey,
        V: IntoStorageValue,
    {
        let key = key.into_storage_key();
        let value = value.into_storage_value();
        self.stats.staged_puts += 1;
        self.stats.written_bytes += value.bytes.len() as u64;
        self.group_mut(space.into_storage_space())
            .stage_put(key.0, value.bytes);
    }

    /// Stages a batch of content-addressed puts with one lookup pass over the
    /// already staged lane.
    ///
    /// This is for a caller that has an entire content-addressed batch ready
    /// at once.
    /// Identical entries already staged by an earlier batch are coalesced;
    /// same-key, different-value entries remain duplicate mutations and are
    /// deliberately left for the canonical validator to reject.
    /// Retains one already-encoded contiguous mutation batch without copying
    /// its key or value buffers.
    ///
    /// Domain lowerers should prefer this path when they can count and encode
    /// the full space-local batch in one pass. The write set stores only range
    /// descriptors until backend lowering; the existing [`StorageWrite`] trait
    /// receives lightweight `Bytes` slices at that final boundary.
    pub fn stage_encoded_batch(&mut self, space: StorageSpace, batch: EncodedMutationBatch) {
        if batch.is_empty() {
            return;
        }
        let staged_puts = batch.put_count() as u64;
        let staged_deletes = batch.delete_count() as u64;
        let written_bytes = batch
            .puts()
            .iter()
            .map(|put| put.value.len() as u64)
            .sum::<u64>();
        self.group_mut(space).stage_encoded_batch(batch);
        self.stats.staged_puts += staged_puts;
        self.stats.staged_deletes += staged_deletes;
        self.stats.written_bytes += written_bytes;
    }

    /// Retains one contiguous content-addressed batch while coalescing puts
    /// already present in the same storage-space lane.
    ///
    /// Keys and values stay as ranges over the two incoming arenas. Filtering
    /// removes descriptors only, so a tracked-state chunk batch is still
    /// represented by exactly one key buffer and one value buffer after
    /// duplicate content is discarded.
    pub(crate) fn stage_content_addressed_encoded_batch(
        &mut self,
        space: StorageSpace,
        batch: EncodedMutationBatch,
    ) {
        if batch.is_empty() {
            return;
        }
        let (key_bytes, value_bytes, puts, deletes) = batch.into_parts();
        debug_assert!(
            deletes.is_empty(),
            "content-addressed encoded batches contain puts only"
        );
        let puts = {
            let group = self.group_mut(space);
            let mut existing = HashSet::with_capacity_and_hasher(
                group.puts.len().saturating_add(puts.len()),
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            );
            for put in &group.puts {
                existing.insert(ContentAddressedRef {
                    key: group.key_bytes(put.key),
                    value: group.value_bytes(put.value),
                });
            }
            puts.into_iter()
                .filter(|put| {
                    existing.insert(ContentAddressedRef {
                        key: &key_bytes
                            [put.key.offset()..put.key.offset().saturating_add(put.key.len())],
                        value: &value_bytes[put.value.offset()
                            ..put.value.offset().saturating_add(put.value.len())],
                    })
                })
                .collect::<Vec<_>>()
        };
        let batch = EncodedMutationBatch::try_new(key_bytes, value_bytes, puts, deletes)
            .expect("filtered encoded batch retains validated buffer ranges");
        self.stage_encoded_batch(space, batch);
    }

    pub fn delete<S, K>(&mut self, space: S, key: K)
    where
        S: IntoStorageSpace,
        K: IntoStorageKey,
    {
        let key = key.into_storage_key();
        self.stats.staged_deletes += 1;
        self.group_mut(space.into_storage_space())
            .stage_delete(key.0);
    }

    /// Stages delete keys in one contiguous retained buffer.
    ///
    /// Large maintenance sweeps should use this path instead of retaining one
    /// separately allocated [`Bytes`] buffer per key.
    pub fn delete_batch<S, I, K>(&mut self, space: S, keys: I)
    where
        S: IntoStorageSpace,
        I: IntoIterator<Item = K>,
        K: IntoStorageKey,
    {
        let mut key_bytes = Vec::new();
        let mut deletes = Vec::new();
        for key in keys {
            let key = key.into_storage_key();
            let start = key_bytes.len();
            key_bytes.extend_from_slice(&key.0);
            deletes.push(BufferRange::new(start, key_bytes.len() - start));
        }
        if deletes.is_empty() {
            return;
        }
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(key_bytes),
            Bytes::new(),
            Vec::new(),
            deletes,
        )
        .expect("delete ranges originate in the supplied encoded key buffer");
        self.stage_encoded_batch(space.into_storage_space(), batch);
    }

    /// Stages one storage-native range deletion for a space with no other
    /// mutations in this write set. Keeping the lane exclusive makes its
    /// ordering and duplicate semantics unambiguous while avoiding millions
    /// of materialized point-delete keys for temporary upload metadata.
    pub(crate) fn delete_range_exclusive(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageWriteSetError> {
        let has_points = self
            .group_index
            .get(&space.id())
            .and_then(|index| self.groups.get(*index))
            .is_some_and(|group| !group.puts.is_empty() || !group.deletes.is_empty());
        let has_range = self
            .exclusive_range_deletes
            .iter()
            .any(|(existing, _)| existing.id() == space.id());
        if has_points || has_range {
            return Err(StorageWriteSetError::DuplicateMutation {
                space,
                key: Key(Bytes::new()),
            });
        }
        self.stats.touched_spaces += 1;
        self.exclusive_range_deletes.push((space, range));
        Ok(())
    }

    /// Reserves capacity for a storage space's grouped puts and deletes.
    ///
    /// This is most useful with canonical construction, where domain stores can
    /// often count final mutations before staging them.
    pub fn reserve_space(
        &mut self,
        space: StorageSpace,
        expected_puts: usize,
        expected_deletes: usize,
    ) {
        let group = self.group_mut(space);
        group.puts.reserve(expected_puts);
        group.deletes.reserve(expected_deletes);
        group
            .key_arena
            .reserve_shared(expected_puts.saturating_add(expected_deletes));
        group.value_arena.reserve_shared(expected_puts);
    }

    pub fn extend(&mut self, other: Self) {
        let Self {
            groups,
            exclusive_range_deletes,
            stats,
            changelog_gc_sealed,
            ..
        } = other;
        self.changelog_gc_sealed |= changelog_gc_sealed;
        for group in groups {
            let space = group.space;
            let target = self.group_mut(space);
            target.append(group);
        }
        for (space, range) in exclusive_range_deletes {
            self.delete_range_exclusive(space, range)
                .expect("extended exclusive range-delete spaces remain exclusive");
        }
        self.stats.staged_puts += stats.staged_puts;
        self.stats.staged_deletes += stats.staged_deletes;
        self.stats.written_bytes += stats.written_bytes;
    }

    pub fn stats(&self) -> StorageWriteSetStats {
        self.stats
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub fn arena_stats(&self) -> StorageWriteSetArenaStats {
        let mut stats = StorageWriteSetArenaStats {
            spaces: self.groups.len(),
            ..StorageWriteSetArenaStats::default()
        };
        for group in &self.groups {
            stats.put_descriptors += group.puts.len();
            stats.delete_descriptors += group.deletes.len();
            stats.put_descriptor_capacity += group.puts.capacity();
            stats.delete_descriptor_capacity += group.deletes.capacity();
            stats.key_shared_buffers += group.key_arena.shared.len();
            stats.key_shared_bytes += group.key_arena.shared.iter().map(Bytes::len).sum::<usize>();
            stats.key_shared_capacity += group.key_arena.shared.capacity();
            stats.value_shared_buffers += group.value_arena.shared.len();
            stats.value_shared_bytes += group
                .value_arena
                .shared
                .iter()
                .map(Bytes::len)
                .sum::<usize>();
            stats.value_shared_capacity += group.value_arena.shared.capacity();
        }
        stats
    }

    /// Returns the number of point-delete descriptors staged in each logical
    /// storage space.  This is benchmark/test observability only: production
    /// planning continues to use the aggregate write-set counters and never
    /// depends on this classification.
    #[cfg(any(test, feature = "storage-benches"))]
    pub fn delete_counts_by_space(&self) -> Vec<(StorageSpace, usize)> {
        self.groups
            .iter()
            .filter_map(|group| {
                (!group.deletes.is_empty()).then_some((group.space, group.deletes.len()))
            })
            .collect()
    }

    #[allow(dead_code)] // Activated by the checkpoint GC integration.
    pub(crate) fn seal_changelog_gc(&mut self) {
        self.changelog_gc_sealed = true;
    }

    /// Validates the canonical write-set contract.
    ///
    /// This performs the full duplicate/conflicting-declaration scan before
    /// lowering so the storage never receives ambiguous final mutations.
    pub fn validate(&self) -> Result<(), StorageWriteSetError> {
        self.validate_exclusive_range_deletes()?;
        for group in &self.groups {
            if let Some(incoming) = group.conflicting_declarations.first() {
                return Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                    existing: group.space,
                    incoming: *incoming,
                });
            }
        }

        for group in &self.groups {
            let mut mutations =
                Vec::with_capacity(group.puts.len().saturating_add(group.deletes.len()));
            mutations.extend((0..group.puts.len()).map(MutationIndex::Put));
            mutations.extend((0..group.deletes.len()).map(MutationIndex::Delete));
            mutations.sort_unstable_by(|left, right| {
                group.mutation_key(*left).cmp(group.mutation_key(*right))
            });
            if let Some(duplicate) = mutations.windows(2).find_map(|pair| {
                let left = group.mutation_key(pair[0]);
                let right = group.mutation_key(pair[1]);
                (left == right).then_some(left)
            }) {
                return Err(StorageWriteSetError::DuplicateMutation {
                    space: group.space,
                    key: Key(Bytes::copy_from_slice(duplicate)),
                });
            }
        }
        Ok(())
    }

    pub async fn lower_into<W>(
        mut self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: StorageWrite,
    {
        self.validate_and_sort()?;
        self.lower_sorted_into(write).await
    }

    /// Validates the owned write set while putting each storage batch in its
    /// final key order.
    ///
    /// The old lowering path first copied every key into a transaction-wide
    /// hash map solely to find duplicates, then sorted the original vectors
    /// before sending them to storage. A write group already has exactly one
    /// storage space, so sorting its owned puts/deletes lets us detect every
    /// duplicate by adjacent/merge comparison without cloning the keys or
    /// allocating a second full-workload hash table.
    fn validate_and_sort(&mut self) -> Result<(), StorageWriteSetError> {
        self.validate_exclusive_range_deletes()?;
        for group in &self.groups {
            if let Some(incoming) = group.conflicting_declarations.first() {
                return Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                    existing: group.space,
                    incoming: *incoming,
                });
            }
        }

        for group in &mut self.groups {
            let puts_sorted = group.puts_are_sorted();
            let deletes_sorted = group.deletes_are_sorted();
            #[cfg(feature = "storage-benches")]
            if order_stats_enabled() && !group.puts.is_empty() {
                eprintln!(
                    "write-set-order space={} puts={} puts_sorted={puts_sorted} deletes={} deletes_sorted={deletes_sorted}",
                    group.space.name(),
                    group.puts.len(),
                    group.deletes.len(),
                );
            }
            if !puts_sorted {
                group.sort_puts();
            }
            if !deletes_sorted {
                group.sort_deletes();
            }
            validate_sorted_group(group)?;
        }
        Ok(())
    }

    async fn lower_sorted_into<W>(
        self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: StorageWrite,
    {
        let Self {
            groups,
            exclusive_range_deletes,
            mut stats,
            ..
        } = self;

        for (space, range) in exclusive_range_deletes {
            stats.delete_batches += 1;
            stats.storage_calls += 1;
            write
                .delete_range(space, range)
                .await
                .map_err(StorageWriteSetError::Storage)?;
        }

        for group in groups {
            #[cfg(feature = "storage-benches")]
            if std::env::var_os("LIX_WRITE_SET_SPACE_STATS").is_some() {
                let key_bytes = group
                    .puts
                    .iter()
                    .map(|put| group.key_bytes(put.key).len())
                    .chain(group.deletes.iter().map(|key| group.key_bytes(*key).len()))
                    .sum::<usize>();
                let value_bytes = group
                    .puts
                    .iter()
                    .map(|put| group.value_bytes(put.value).len())
                    .sum::<usize>();
                eprintln!(
                    "write-set-space space={} puts={} deletes={} key_bytes={} value_bytes={}",
                    group.space.name(),
                    group.puts.len(),
                    group.deletes.len(),
                    key_bytes,
                    value_bytes,
                );
            }
            let (space, puts, deletes) = group.lower();
            if !puts.is_empty() {
                stats.put_batches += 1;
                stats.storage_calls += 1;
                write
                    .put_many(space, PutBatch { entries: puts })
                    .await
                    .map_err(StorageWriteSetError::Storage)?;
            }
            if !deletes.is_empty() {
                stats.delete_batches += 1;
                stats.storage_calls += 1;
                write
                    .delete_many(space, &deletes)
                    .await
                    .map_err(StorageWriteSetError::Storage)?;
            }
        }

        Ok(stats)
    }

    pub async fn commit<StorageImpl>(
        self,
        storage: &StorageImpl,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError>
    where
        StorageImpl: Storage,
    {
        let mut write = storage
            .begin_write(opts)
            .await
            .map_err(StorageWriteSetError::Storage)?;
        let stats = match self.lower_into(&mut write).await {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback().await;
                return Err(error);
            }
        };
        let result = write
            .commit()
            .await
            .map_err(StorageWriteSetError::Storage)?;
        Ok((result, stats))
    }

    fn group_mut(&mut self, space: StorageSpace) -> &mut StorageWriteGroup {
        if let Some(index) = self.group_index.get(&space.id()).copied() {
            let group = &mut self.groups[index];
            if group.space != space {
                group.conflicting_declarations.push(space);
            }
            return group;
        }

        let index = self.groups.len();
        self.group_index.insert(space.id(), index);
        self.stats.touched_spaces += 1;
        self.groups.push(StorageWriteGroup::new(space));
        let group = &mut self.groups[index];
        if group.space != space {
            group.conflicting_declarations.push(space);
        }
        group
    }

    fn validate_exclusive_range_deletes(&self) -> Result<(), StorageWriteSetError> {
        for (index, (space, _)) in self.exclusive_range_deletes.iter().enumerate() {
            let conflicts_with_points = self
                .group_index
                .get(&space.id())
                .and_then(|group_index| self.groups.get(*group_index))
                .is_some_and(|group| !group.puts.is_empty() || !group.deletes.is_empty());
            let conflicts_with_range = self.exclusive_range_deletes[index + 1..]
                .iter()
                .any(|(other, _)| other.id() == space.id());
            if conflicts_with_points || conflicts_with_range {
                return Err(StorageWriteSetError::DuplicateMutation {
                    space: *space,
                    key: Key(Bytes::new()),
                });
            }
        }
        Ok(())
    }
}

fn validate_sorted_group(group: &StorageWriteGroup) -> Result<(), StorageWriteSetError> {
    if let Some(duplicate) = group.puts.windows(2).find_map(|pair| {
        let left = group.key_bytes(pair[0].key);
        let right = group.key_bytes(pair[1].key);
        (left == right).then_some(left)
    }) {
        return Err(StorageWriteSetError::DuplicateMutation {
            space: group.space,
            key: Key(Bytes::copy_from_slice(duplicate)),
        });
    }
    if let Some(duplicate) = group.deletes.windows(2).find_map(|pair| {
        let left = group.key_bytes(pair[0]);
        let right = group.key_bytes(pair[1]);
        (left == right).then_some(left)
    }) {
        return Err(StorageWriteSetError::DuplicateMutation {
            space: group.space,
            key: Key(Bytes::copy_from_slice(duplicate)),
        });
    }

    let mut put_index = 0usize;
    let mut delete_index = 0usize;
    while put_index < group.puts.len() && delete_index < group.deletes.len() {
        let put_key = group.key_bytes(group.puts[put_index].key);
        let delete_key = group.key_bytes(group.deletes[delete_index]);
        match put_key.cmp(delete_key) {
            std::cmp::Ordering::Less => put_index += 1,
            std::cmp::Ordering::Greater => delete_index += 1,
            std::cmp::Ordering::Equal => {
                return Err(StorageWriteSetError::DuplicateMutation {
                    space: group.space,
                    key: Key(Bytes::copy_from_slice(put_key)),
                });
            }
        }
    }
    Ok(())
}

impl Default for StorageWriteSet {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            group_index: HashMap::with_hasher(FastHashBuilder::with_seeds(0, 0, 0, 0)),
            exclusive_range_deletes: Vec::new(),
            stats: StorageWriteSetStats::default(),
            changelog_gc_sealed: false,
        }
    }
}

impl StorageWriteGroup {
    fn new(space: StorageSpace) -> Self {
        Self {
            space,
            key_arena: MutationArena::default(),
            value_arena: MutationArena::default(),
            puts: Vec::new(),
            deletes: Vec::new(),
            conflicting_declarations: Vec::new(),
        }
    }

    fn stage_put(&mut self, key: Bytes, value: Bytes) {
        let key = self.key_arena.stage_bytes(key);
        let value = self.value_arena.stage_bytes(value);
        self.puts.push(StagedPut { key, value });
    }

    fn stage_delete(&mut self, key: Bytes) {
        let key = self.key_arena.stage_bytes(key);
        self.deletes.push(key);
    }

    fn stage_encoded_batch(&mut self, batch: EncodedMutationBatch) {
        let (key_bytes, value_bytes, puts, deletes) = batch.into_parts();
        self.puts.reserve(puts.len());
        self.deletes.reserve(deletes.len());
        let key_buffer_index = self.key_arena.import(key_bytes);
        let value_buffer_index = (!puts.is_empty()).then(|| self.value_arena.import(value_bytes));
        self.puts.extend(puts.into_iter().map(|put| StagedPut {
            key: ArenaRange::new(key_buffer_index, put.key),
            value: ArenaRange::new(
                value_buffer_index.expect("an encoded put batch must retain its value buffer"),
                put.value,
            ),
        }));
        self.deletes.extend(
            deletes
                .into_iter()
                .map(|range| ArenaRange::new(key_buffer_index, range)),
        );
    }

    fn key_bytes(&self, range: ArenaRange) -> &[u8] {
        self.key_arena.bytes(range)
    }

    fn value_bytes(&self, range: ArenaRange) -> &[u8] {
        self.value_arena.bytes(range)
    }

    fn mutation_key(&self, mutation: MutationIndex) -> &[u8] {
        match mutation {
            MutationIndex::Put(index) => self.key_bytes(self.puts[index].key),
            MutationIndex::Delete(index) => self.key_bytes(self.deletes[index]),
        }
    }

    fn puts_are_sorted(&self) -> bool {
        self.puts
            .windows(2)
            .all(|pair| self.key_bytes(pair[0].key) <= self.key_bytes(pair[1].key))
    }

    fn deletes_are_sorted(&self) -> bool {
        self.deletes
            .windows(2)
            .all(|pair| self.key_bytes(pair[0]) <= self.key_bytes(pair[1]))
    }

    fn sort_puts(&mut self) {
        let key_arena = &self.key_arena;
        self.puts.sort_unstable_by(|left, right| {
            key_arena.bytes(left.key).cmp(key_arena.bytes(right.key))
        });
    }

    fn sort_deletes(&mut self) {
        let key_arena = &self.key_arena;
        self.deletes
            .sort_unstable_by(|left, right| key_arena.bytes(*left).cmp(key_arena.bytes(*right)));
    }

    fn append(&mut self, other: Self) {
        let Self {
            space,
            key_arena,
            value_arena,
            puts,
            deletes,
            conflicting_declarations,
        } = other;
        debug_assert_eq!(self.space.id(), space.id());
        self.puts.reserve(puts.len());
        self.deletes.reserve(deletes.len());
        let key_remap = self.key_arena.append(key_arena);
        let value_remap = self.value_arena.append(value_arena);
        self.puts.extend(puts.into_iter().map(|put| StagedPut {
            key: key_remap.remap(put.key),
            value: value_remap.remap(put.value),
        }));
        self.deletes
            .extend(deletes.into_iter().map(|key| key_remap.remap(key)));
        self.conflicting_declarations
            .extend(conflicting_declarations);
    }

    fn lower(self) -> (StorageSpace, Vec<PutEntry>, Vec<Key>) {
        let Self {
            space,
            key_arena,
            value_arena,
            puts,
            deletes,
            ..
        } = self;
        let key_arena = key_arena.freeze();
        let value_arena = value_arena.freeze();
        let puts = puts
            .into_iter()
            .map(|put| PutEntry {
                key: Key(key_arena.slice(put.key)),
                value: StoredValue {
                    bytes: value_arena.slice(put.value),
                },
            })
            .collect();
        let deletes = deletes
            .into_iter()
            .map(|key| Key(key_arena.slice(key)))
            .collect();
        (space, puts, deletes)
    }
}

impl MutationArena {
    fn reserve_shared(&mut self, additional: usize) {
        self.shared.reserve(additional);
    }

    fn stage_bytes(&mut self, bytes: Bytes) -> ArenaRange {
        let length = bytes.len();
        let buffer_index = self.import(bytes);
        ArenaRange::new(buffer_index, BufferRange::new(0, length))
    }

    fn import(&mut self, bytes: Bytes) -> usize {
        let index = self.shared.len();
        self.shared.push(bytes);
        index
    }

    fn bytes(&self, range: ArenaRange) -> &[u8] {
        slice_bytes(
            self.shared
                .get(range.buffer_index())
                .expect("staged mutation references a retained shared buffer"),
            range,
        )
    }

    fn append(&mut self, other: Self) -> ArenaRemap {
        let shared_buffer_base = self.shared.len();
        self.shared.extend(other.shared);
        ArenaRemap { shared_buffer_base }
    }

    fn freeze(self) -> FrozenMutationArena {
        FrozenMutationArena {
            shared: self.shared,
        }
    }
}

impl ArenaRemap {
    fn remap(&self, range: ArenaRange) -> ArenaRange {
        ArenaRange::new(
            self.shared_buffer_base + range.buffer_index(),
            BufferRange::new(range.offset(), range.len()),
        )
    }
}

impl FrozenMutationArena {
    fn slice(&self, range: ArenaRange) -> Bytes {
        let bytes = self
            .shared
            .get(range.buffer_index())
            .expect("lowered mutation references a retained shared buffer");
        if range.offset() == 0 && range.len() == bytes.len() {
            return bytes.clone();
        }
        bytes.slice(range.offset()..range.offset() + range.len())
    }
}

fn slice_bytes(bytes: &[u8], range: ArenaRange) -> &[u8] {
    &bytes[range.offset()..range.offset() + range.len()]
}

impl fmt::Display for StorageWriteSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingSpaceDeclaration { existing, incoming } => write!(
                f,
                "conflicting storage space declarations for {:?}: {existing} vs {incoming}",
                existing.id()
            ),
            Self::DuplicateMutation { space, key } => {
                write!(f, "duplicate storage mutation for {space}/{key:?}")
            }
            Self::Storage(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for StorageWriteSetError {}

impl From<StorageError> for StorageWriteSetError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[cfg(feature = "storage-benches")]
fn order_stats_enabled() -> bool {
    use std::sync::LazyLock;
    static ENABLED: LazyLock<bool> =
        LazyLock::new(|| std::env::var_os("LIX_WRITE_SET_ORDER_STATS").is_some());
    *ENABLED
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::storage::{
        BufferRange, CommitResult, EncodedMutationBatch, EncodedMutationBatchError, EncodedPut,
        Key, KeyRange, Memory, PutBatch, StorageError, StorageWrite, StoredValue, WriteOptions,
    };
    use crate::storage_adapter::{StorageSpace, StorageWriteSet, StorageWriteSetError};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space() -> StorageSpace {
        StorageSpace::engine_declared(1, "test.space", crate::storage::ValueSemantics::Mutable)
    }

    #[derive(Default)]
    struct CapturingStorageWrite {
        puts: Vec<(StorageSpace, PutBatch)>,
        deletes: Vec<(StorageSpace, Vec<Key>)>,
    }

    impl StorageWrite for CapturingStorageWrite {
        fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            self.puts.push((space, entries));
            async { Ok(()) }
        }

        fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[Key],
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            self.deletes.push((space, keys.to_vec()));
            async { Ok(()) }
        }

        fn delete_range(
            &mut self,
            _space: StorageSpace,
            _range: KeyRange,
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            async { Ok(()) }
        }

        fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
            async {
                Ok(CommitResult {
                    commit_id: None,
                    stats: Default::default(),
                })
            }
        }

        fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
            async { Ok(()) }
        }
    }

    #[tokio::test]
    async fn write_set_rejects_duplicate_final_mutations_before_storage_write() {
        let storage = Memory::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("a"), value("A"));
        writes.delete(space(), key("a"));

        let error = writes
            .commit(&storage, WriteOptions::default())
            .await
            .expect_err("duplicate mutation");

        assert!(matches!(
            error,
            StorageWriteSetError::DuplicateMutation { .. }
        ));
    }

    #[tokio::test]
    async fn write_set_lowers_batches_and_commits_asynchronously() {
        let storage = Memory::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("b"), value("B"));
        writes.put(space(), key("a"), value("A"));
        writes.delete(space(), key("missing"));

        let (commit, stats) = writes
            .commit(&storage, WriteOptions::default())
            .await
            .expect("commit");

        assert_eq!(stats.put_batches, 1);
        assert_eq!(stats.delete_batches, 1);
        assert_eq!(commit.stats.put_entries, 2);
        assert_eq!(commit.stats.deleted_entries, 1);
    }

    #[tokio::test]
    async fn fallback_mutations_retain_caller_buffers_without_payload_copies() {
        const PUTS: usize = 1024;
        const DELETES: usize = 1024;
        const KEY_BYTES: usize = 1 + size_of::<u32>();
        const VALUE_BYTES: usize = 8;

        let mut writes = StorageWriteSet::new();
        writes.reserve_space(space(), PUTS, DELETES);
        let mut put_key_pointers = Vec::with_capacity(PUTS);
        let mut value_pointers = Vec::with_capacity(PUTS);
        for index in 0..PUTS {
            let mut key = Vec::with_capacity(KEY_BYTES);
            key.push(b'p');
            key.extend_from_slice(&(index as u32).to_be_bytes());
            let key = Bytes::from(key);
            let value = Bytes::from(vec![index as u8; VALUE_BYTES]);
            put_key_pointers.push(key.as_ptr() as usize);
            value_pointers.push(value.as_ptr() as usize);
            writes.put(space(), Key(key), StoredValue { bytes: value });
        }
        let mut delete_key_pointers = Vec::with_capacity(DELETES);
        for index in 0..DELETES {
            let mut key = Vec::with_capacity(KEY_BYTES);
            key.push(b'd');
            key.extend_from_slice(&(index as u32).to_be_bytes());
            let key = Bytes::from(key);
            delete_key_pointers.push(key.as_ptr() as usize);
            writes.delete(space(), Key(key));
        }

        let arenas = writes.arena_stats();
        assert_eq!(arenas.spaces, 1);
        assert_eq!(arenas.put_descriptors, PUTS);
        assert_eq!(arenas.delete_descriptors, DELETES);
        assert_eq!(arenas.key_inline_bytes, 0);
        assert_eq!(arenas.value_inline_bytes, 0);
        assert_eq!(arenas.key_inline_capacity, 0);
        assert_eq!(arenas.value_inline_capacity, 0);
        assert_eq!(arenas.key_inline_allocations, 0);
        assert_eq!(arenas.value_inline_allocations, 0);
        assert_eq!(arenas.key_shared_buffers, PUTS + DELETES);
        assert_eq!(arenas.value_shared_buffers, PUTS);
        assert_eq!(arenas.key_shared_bytes, (PUTS + DELETES) * KEY_BYTES);
        assert_eq!(arenas.value_shared_bytes, PUTS * VALUE_BYTES);
        assert!(arenas.put_descriptor_capacity >= PUTS);
        assert!(arenas.delete_descriptor_capacity >= DELETES);
        assert!(arenas.key_shared_capacity >= PUTS + DELETES);
        assert!(arenas.value_shared_capacity >= PUTS);

        let mut backend = CapturingStorageWrite::default();
        writes
            .lower_into(&mut backend)
            .await
            .expect("lower retained caller buffers");
        let (_, batch) = backend.puts.pop().expect("one put batch");
        assert_eq!(batch.entries.len(), PUTS);
        assert_eq!(
            batch
                .entries
                .iter()
                .map(|entry| entry.key.0.as_ptr() as usize)
                .collect::<Vec<_>>(),
            put_key_pointers
        );
        assert_eq!(
            batch
                .entries
                .iter()
                .map(|entry| entry.value.bytes.as_ptr() as usize)
                .collect::<Vec<_>>(),
            value_pointers
        );
        let (_, deletes) = backend.deletes.pop().expect("one delete batch");
        assert_eq!(deletes.len(), DELETES);
        assert_eq!(
            deletes
                .iter()
                .map(|key| key.0.as_ptr() as usize)
                .collect::<Vec<_>>(),
            delete_key_pointers
        );
    }

    #[tokio::test]
    async fn delete_batch_retains_one_contiguous_key_buffer() {
        assert_eq!(size_of::<super::ArenaRange>(), 12);
        let mut writes = StorageWriteSet::new();
        writes.delete_batch(space(), [key("c"), key("a"), key("b")]);

        let arenas = writes.arena_stats();
        assert_eq!(arenas.delete_descriptors, 3);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.key_shared_bytes, 3);

        let mut backend = CapturingStorageWrite::default();
        writes
            .lower_into(&mut backend)
            .await
            .expect("lower contiguous delete batch");
        let (_, deletes) = backend.deletes.pop().expect("one delete batch");
        assert_eq!(
            deletes.into_iter().map(|key| key.0).collect::<Vec<_>>(),
            [
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]
        );
    }

    #[tokio::test]
    async fn encoded_batch_ingress_retains_caller_key_and_value_buffers() {
        let key_bytes = Bytes::from(b"abc".to_vec());
        let value_bytes = Bytes::from(b"AABB".to_vec());
        let key_probe = key_bytes.clone();
        let value_probe = value_bytes.clone();
        let batch = EncodedMutationBatch::try_new(
            key_bytes,
            value_bytes,
            vec![
                EncodedPut {
                    key: BufferRange::new(0, 1),
                    value: BufferRange::new(0, 2),
                },
                EncodedPut {
                    key: BufferRange::new(2, 1),
                    value: BufferRange::new(2, 2),
                },
            ],
            vec![BufferRange::new(1, 1)],
        )
        .expect("valid encoded batch");

        let mut writes = StorageWriteSet::new();
        writes.stage_encoded_batch(space(), batch);
        let arenas = writes.arena_stats();
        assert_eq!(arenas.key_inline_bytes, 0);
        assert_eq!(arenas.value_inline_bytes, 0);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.value_shared_buffers, 1);
        assert_eq!(arenas.put_descriptors, 2);
        assert_eq!(arenas.delete_descriptors, 1);

        let mut backend = CapturingStorageWrite::default();
        writes
            .lower_into(&mut backend)
            .await
            .expect("lower encoded batch");
        let (_, puts) = backend.puts.pop().expect("one put batch");
        assert_eq!(
            puts.entries[0].key.0.as_ptr(),
            key_probe.as_ptr(),
            "first encoded key must slice the caller buffer"
        );
        assert_eq!(
            puts.entries[1].key.0.as_ptr(),
            key_probe.as_ptr().wrapping_add(2),
            "second encoded key must slice the caller buffer"
        );
        assert_eq!(
            puts.entries[0].value.bytes.as_ptr(),
            value_probe.as_ptr(),
            "first encoded value must slice the caller buffer"
        );
        assert_eq!(
            puts.entries[1].value.bytes.as_ptr(),
            value_probe.as_ptr().wrapping_add(2),
            "second encoded value must slice the caller buffer"
        );
        let (_, deletes) = backend.deletes.pop().expect("one delete batch");
        assert_eq!(deletes[0].0.as_ptr(), key_probe.as_ptr().wrapping_add(1));
    }

    #[test]
    fn content_addressed_encoded_batch_coalesces_without_splitting_arenas() {
        let batch = EncodedMutationBatch::try_new(
            Bytes::from_static(b"aab"),
            Bytes::from_static(b"AAB"),
            vec![
                EncodedPut {
                    key: BufferRange::new(0, 1),
                    value: BufferRange::new(0, 1),
                },
                EncodedPut {
                    key: BufferRange::new(1, 1),
                    value: BufferRange::new(1, 1),
                },
                EncodedPut {
                    key: BufferRange::new(2, 1),
                    value: BufferRange::new(2, 1),
                },
            ],
            Vec::new(),
        )
        .expect("valid content-addressed batch");
        let mut writes = StorageWriteSet::new();
        writes.stage_content_addressed_encoded_batch(space(), batch.clone());
        writes.stage_content_addressed_encoded_batch(space(), batch);

        let arenas = writes.arena_stats();
        assert_eq!(writes.stats().staged_puts, 2);
        assert_eq!(arenas.put_descriptors, 2);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.value_shared_buffers, 1);
        writes
            .validate()
            .expect("identical content-addressed descriptors should coalesce");

        let mut conflicting = StorageWriteSet::new();
        let first = EncodedMutationBatch::try_new(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"first"),
            vec![EncodedPut {
                key: BufferRange::new(0, 3),
                value: BufferRange::new(0, 5),
            }],
            Vec::new(),
        )
        .expect("valid first batch");
        let second = EncodedMutationBatch::try_new(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"other"),
            vec![EncodedPut {
                key: BufferRange::new(0, 3),
                value: BufferRange::new(0, 5),
            }],
            Vec::new(),
        )
        .expect("valid conflicting batch");
        conflicting.stage_content_addressed_encoded_batch(space(), first);
        conflicting.stage_content_addressed_encoded_batch(space(), second);
        assert!(matches!(
            conflicting.validate(),
            Err(StorageWriteSetError::DuplicateMutation { .. })
        ));
    }

    #[tokio::test]
    async fn small_fallback_inputs_retain_existing_bytes() {
        let put_key = Bytes::from(vec![b'k'; 8]);
        let put_value = Bytes::from(vec![b'v'; 8]);
        let delete_key = Bytes::from(vec![b'd'; 8]);
        let put_key_pointer = put_key.as_ptr() as usize;
        let put_value_pointer = put_value.as_ptr() as usize;
        let delete_key_pointer = delete_key.as_ptr() as usize;
        let mut writes = StorageWriteSet::new();
        writes.put(space(), Key(put_key), StoredValue { bytes: put_value });
        writes.delete(space(), Key(delete_key));

        let arenas = writes.arena_stats();
        assert_eq!(arenas.key_inline_bytes, 0);
        assert_eq!(arenas.value_inline_bytes, 0);
        assert_eq!(arenas.key_shared_buffers, 2);
        assert_eq!(arenas.value_shared_buffers, 1);
        assert_eq!(arenas.key_shared_bytes, 16);
        assert_eq!(arenas.value_shared_bytes, 8);

        let mut backend = CapturingStorageWrite::default();
        writes
            .lower_into(&mut backend)
            .await
            .expect("lower retained fallback bytes");
        let put = &backend.puts[0].1.entries[0];
        assert_eq!(put.key.0.as_ptr() as usize, put_key_pointer);
        assert_eq!(put.value.bytes.as_ptr() as usize, put_value_pointer);
        assert_eq!(
            backend.deletes[0].1[0].0.as_ptr() as usize,
            delete_key_pointer
        );
    }

    #[tokio::test]
    async fn extending_write_sets_remaps_source_arenas_without_row_copies() {
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("b"), value("B"));

        let mut other = StorageWriteSet::new();
        other.put(space(), key("a"), value("A"));
        other.delete(space(), key("c"));
        writes.extend(other);

        let arenas = writes.arena_stats();
        assert_eq!(arenas.put_descriptors, 2);
        assert_eq!(arenas.delete_descriptors, 1);
        assert_eq!(arenas.key_shared_buffers, 3);
        assert_eq!(arenas.value_shared_buffers, 2);

        let mut backend = CapturingStorageWrite::default();
        writes
            .lower_into(&mut backend)
            .await
            .expect("lower extended arenas");
        assert_eq!(
            backend.puts[0]
                .1
                .entries
                .iter()
                .map(|entry| entry.key.0.as_ref())
                .collect::<Vec<_>>(),
            vec![b"a".as_slice(), b"b".as_slice()]
        );
        assert_eq!(
            backend.puts[0]
                .1
                .entries
                .iter()
                .map(|entry| entry.value.bytes.as_ref())
                .collect::<Vec<_>>(),
            vec![b"A".as_slice(), b"B".as_slice()]
        );
        assert_eq!(backend.deletes[0].1[0].0.as_ref(), b"c");
    }

    #[test]
    fn encoded_batch_rejects_out_of_bounds_descriptors() {
        let error = EncodedMutationBatch::try_new(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            vec![EncodedPut {
                key: BufferRange::new(1, 1),
                value: BufferRange::new(0, 1),
            }],
            Vec::new(),
        )
        .expect_err("key range exceeds buffer");

        assert!(matches!(
            error,
            EncodedMutationBatchError::PutKeyOutOfBounds { index: 0, .. }
        ));
    }

    #[test]
    fn one_space_id_cannot_change_value_semantics() {
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("mutable"), value("A"));
        writes.put(
            StorageSpace::engine_declared(
                1,
                "test.space",
                crate::storage::ValueSemantics::Immutable,
            ),
            key("immutable"),
            value("B"),
        );

        assert!(matches!(
            writes.validate(),
            Err(StorageWriteSetError::ConflictingSpaceDeclaration { .. })
        ));
    }
}
