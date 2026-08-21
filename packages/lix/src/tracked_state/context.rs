#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure_for_method_calls,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;

use crate::changelog::{ChangeId, ChangeRecordProjection};
use crate::changelog::{
    ChangeRecord, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest,
};
use crate::common::SharedStr;
use crate::row_pk::{RowPk, RowPkComponent};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
#[cfg(test)]
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::codec::{
    EncodedTrackedStateKeyBatch, TrackedStateKeyBatchBuilder, TrackedStateMutationBatchBuilder,
    decode_key_shared, encode_key, encode_key_ref_into, encode_value_ref,
};
use crate::tracked_state::diff::{
    TrackedStateDiff, TrackedStateDiffIdentity, TrackedStateDiffRequest, TrackedStateDiffRow,
    TrackedStatePayloadBatch, TrackedStateTreeDiffBatch, TrackedStateTreeDiffBatchBuilder,
    TrackedStateTreeDiffRowRef, diff_commits,
};
#[cfg(test)]
use crate::tracked_state::merge::{self, TrackedStateMergePlan};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
#[cfg(test)]
use crate::tracked_state::types::TrackedStateMutation;
use crate::tracked_state::types::{
    TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateIndexValue,
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateMutationBatch,
    TrackedStateRootId, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    materialize_batch_from_index_entries, materialize_batch_from_index_entry_refs,
};
use crate::tracked_state::{
    TrackedStateDeltaRef, TrackedStateRootMutationRef, TrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};
use base64::Engine as _;
use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

/// Counts `validate_diff_row_created_at` entries so a probe can distinguish
/// "the validator ran and accepted this row" from "the validator never ran".
/// A null result from a validator that was never reached is indistinguishable
/// from a null result from one that passed, so the count has to be observed
/// rather than assumed.
#[cfg(test)]
pub(crate) static DIFF_ROW_CREATED_AT_VALIDATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
// A right-edge probe is worthwhile for a real append batch, but would add a
// second tree traversal to sparse non-append writes that already use point
// reads. Keep those latency-sensitive writes on the unchanged generic path.
const ORDERED_APPEND_BATCH_MIN_ROWS: usize = 64;
const STREAMING_DOMINANT_APPEND_MIN_ROWS: usize = 32 * 1024;
const STREAMING_DOMINANT_APPEND_PARENT_RATIO: u64 = 16;
// Retain the point cache for latency-sensitive descriptor/registry probes.
// Above this boundary, per-key cache ownership and duplicate adapters dominate
// and the arena-backed encoded replay is the intended bulk path.
const HISTORICAL_ENCODED_LOOKUP_MIN_ROWS: usize = 64;
const NO_FIRST_PARENT_ORDINAL: u32 = u32::MAX;
const NO_ROOTLESS_REPLAY_ORDINAL: u32 = u32::MAX;
const SMALL_FILE_ID_DICTIONARY_CAPACITY: usize = 64;
const ESTIMATED_FILE_ID_BYTES: usize = 36;

#[cfg(test)]
thread_local! {
    static STREAMING_DOMINANT_APPEND_EXECUTIONS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[derive(Clone, Copy)]
enum FirstParentDiffKeySource {
    Interval(u32),
    Inherited(u32),
}

struct FirstParentDiffEntry {
    key_source: FirstParentDiffKeySource,
    after: TrackedStateIndexValue,
    next_hash_collision: u32,
}

struct FirstParentCascadeEntry {
    file_id_start: u32,
    file_id_end: u32,
    value: TrackedStateIndexValue,
    next_hash_collision: u32,
}

#[derive(Clone, Copy)]
struct FirstParentHashHeads {
    key: u32,
    cascade: u32,
}

impl Default for FirstParentHashHeads {
    fn default() -> Self {
        Self {
            key: NO_FIRST_PARENT_ORDINAL,
            cascade: NO_FIRST_PARENT_ORDINAL,
        }
    }
}

/// Flat last-writer overlay for a descendant-to-ancestor delta interval.
///
/// Keys stay in two shared encoded arenas (the interval and the optional
/// inherited baseline candidate batch). Overlay rows carry only an arena
/// ordinal, a compact value, and a collision link. One hash table serves both
/// identity and file-cascade lookup; deterministic output order is established
/// once by sorting dense row ordinals.
struct FirstParentDiffOverlay {
    entries: Vec<FirstParentDiffEntry>,
    cascades: Vec<FirstParentCascadeEntry>,
    cascade_file_ids: String,
    hash_heads: HashMap<u64, FirstParentHashHeads>,
}

struct FirstParentIntervalBatch {
    keys: EncodedTrackedStateKeyBatch,
    values: Vec<TrackedStateIndexValue>,
    commit_ranges: Vec<Range<usize>>,
    cascade_capacity: usize,
}

impl FirstParentDiffOverlay {
    fn with_capacities(row_capacity: usize, cascade_capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(row_capacity),
            cascades: Vec::with_capacity(cascade_capacity),
            cascade_file_ids: String::with_capacity(cascade_capacity.saturating_mul(36)),
            hash_heads: HashMap::with_capacity(row_capacity.saturating_add(cascade_capacity)),
        }
    }

    fn key_for_source<'a>(
        source: FirstParentDiffKeySource,
        interval: &'a EncodedTrackedStateKeyBatch,
        inherited: Option<&'a EncodedTrackedStateKeyBatch>,
    ) -> &'a [u8] {
        match source {
            FirstParentDiffKeySource::Interval(ordinal) => interval
                .get(ordinal as usize)
                .expect("first-parent interval key ordinal is in bounds"),
            FirstParentDiffKeySource::Inherited(ordinal) => inherited
                .expect("inherited key source requires its retained batch")
                .get(ordinal as usize)
                .expect("first-parent inherited key ordinal is in bounds"),
        }
    }

    fn insert_key_if_absent(
        &mut self,
        source: FirstParentDiffKeySource,
        encoded_key: &[u8],
        after: TrackedStateIndexValue,
        interval: &EncodedTrackedStateKeyBatch,
        inherited: Option<&EncodedTrackedStateKeyBatch>,
    ) -> Result<bool, LixError> {
        let hash = xxh3_64(encoded_key);
        let mut ordinal = self
            .hash_heads
            .get(&hash)
            .map_or(NO_FIRST_PARENT_ORDINAL, |heads| heads.key);
        while ordinal != NO_FIRST_PARENT_ORDINAL {
            let entry = &self.entries[ordinal as usize];
            if Self::key_for_source(entry.key_source, interval, inherited) == encoded_key {
                return Ok(false);
            }
            ordinal = entry.next_hash_collision;
        }

        let ordinal = if self.entries.len() >= NO_FIRST_PARENT_ORDINAL as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "first-parent tracked-state diff exceeds the batch ordinal range",
            ));
        } else {
            self.entries.len() as u32
        };
        let heads = self.hash_heads.entry(hash).or_default();
        self.entries.push(FirstParentDiffEntry {
            key_source: source,
            after,
            next_hash_collision: heads.key,
        });
        heads.key = ordinal;
        Ok(true)
    }

    fn cascade_for_file_id(&self, file_id: &str) -> Option<&TrackedStateIndexValue> {
        let mut ordinal = self
            .hash_heads
            .get(&xxh3_64(file_id.as_bytes()))
            .map_or(NO_FIRST_PARENT_ORDINAL, |heads| heads.cascade);
        while ordinal != NO_FIRST_PARENT_ORDINAL {
            let entry = &self.cascades[ordinal as usize];
            let range = entry.file_id_start as usize..entry.file_id_end as usize;
            if &self.cascade_file_ids[range] == file_id {
                return Some(&entry.value);
            }
            ordinal = entry.next_hash_collision;
        }
        None
    }

    fn insert_cascade_if_absent(
        &mut self,
        descriptor_key: TrackedStateKeyRef<'_>,
        value: &TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        let start = self.cascade_file_ids.len();
        append_single_row_pk_external(&mut self.cascade_file_ids, descriptor_key.row_pk)?;
        let end = self.cascade_file_ids.len();
        let file_id = &self.cascade_file_ids[start..end];
        let hash = xxh3_64(file_id.as_bytes());
        let mut ordinal = self
            .hash_heads
            .get(&hash)
            .map_or(NO_FIRST_PARENT_ORDINAL, |heads| heads.cascade);
        while ordinal != NO_FIRST_PARENT_ORDINAL {
            let entry = &self.cascades[ordinal as usize];
            let range = entry.file_id_start as usize..entry.file_id_end as usize;
            if &self.cascade_file_ids[range] == file_id {
                self.cascade_file_ids.truncate(start);
                return Ok(());
            }
            ordinal = entry.next_hash_collision;
        }

        let entry_ordinal = if self.cascades.len() >= NO_FIRST_PARENT_ORDINAL as usize {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "first-parent tracked-state cascade batch exceeds the ordinal range",
            ));
        } else {
            self.cascades.len() as u32
        };
        let file_id_start = u32::try_from(start).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "first-parent tracked-state cascade arena exceeds u32",
            )
        })?;
        let file_id_end = u32::try_from(end).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "first-parent tracked-state cascade arena exceeds u32",
            )
        })?;
        let heads = self.hash_heads.entry(hash).or_default();
        self.cascades.push(FirstParentCascadeEntry {
            file_id_start,
            file_id_end,
            value: value.clone(),
            next_hash_collision: heads.cascade,
        });
        heads.cascade = entry_ordinal;
        Ok(())
    }

    fn compact_sorted(
        &self,
        interval: &EncodedTrackedStateKeyBatch,
        inherited: Option<&EncodedTrackedStateKeyBatch>,
    ) -> Result<(EncodedTrackedStateKeyBatch, Vec<TrackedStateIndexValue>), LixError> {
        let mut ordinals = (0..self.entries.len()).collect::<Vec<_>>();
        ordinals.sort_unstable_by(|&left, &right| {
            Self::key_for_source(self.entries[left].key_source, interval, inherited).cmp(
                Self::key_for_source(self.entries[right].key_source, interval, inherited),
            )
        });
        let encoded_bytes = ordinals.iter().try_fold(0usize, |total, &ordinal| {
            total
                .checked_add(
                    Self::key_for_source(self.entries[ordinal].key_source, interval, inherited)
                        .len(),
                )
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "first-parent tracked-state compact key bytes overflow usize",
                    )
                })
        })?;
        let mut keys = TrackedStateKeyBatchBuilder::with_capacities(ordinals.len(), encoded_bytes);
        let mut values = Vec::with_capacity(ordinals.len());
        for ordinal in ordinals {
            let entry = &self.entries[ordinal];
            keys.push_encoded(Self::key_for_source(entry.key_source, interval, inherited));
            values.push(entry.after.clone());
        }
        Ok((keys.finish_batch(), values))
    }

    #[cfg(test)]
    fn large_buffer_count(&self) -> usize {
        usize::from(!self.entries.is_empty())
            + usize::from(!self.cascades.is_empty())
            + usize::from(!self.cascade_file_ids.is_empty())
            + usize::from(!self.hash_heads.is_empty())
    }

    #[cfg(test)]
    fn retained_owned_key_count(&self) -> usize {
        0
    }
}

struct RootlessReplayEntry {
    key_start: u32,
    key_end: u32,
    file_id_ordinal: u32,
    value: TrackedStateIndexValue,
    next_hash_collision: u32,
}

struct RootlessReplayFileId {
    start: u32,
    end: u32,
    next_hash_collision: u32,
}

/// Mutable flat logical index used while replaying a rootless interval.
///
/// Every identity is encoded once into `key_arena`; the hash table retains
/// only collision-chain ordinals. File ids are dictionary encoded once for
/// the whole replay so commit-wide cascades can scan a compact ordinal column
/// without cloning metadata into every row.
struct RootlessReplayOverlay {
    key_arena: Vec<u8>,
    entries: Vec<RootlessReplayEntry>,
    key_hash_heads: HashMap<u64, u32>,
    file_id_arena: String,
    file_ids: Vec<RootlessReplayFileId>,
    file_id_hash_heads: HashMap<u64, u32>,
    file_id_capacity_hint: usize,
    file_id_dictionary_promoted: bool,
}

/// Sorted immutable endpoint of rootless replay.
///
/// The encoded arena remains shared by materialization and diff lowering;
/// rows contain only ranges, dictionary ordinals, values, and stale replay
/// collision links.
struct RootlessReplayBatch {
    key_arena: Bytes,
    entries: Vec<RootlessReplayEntry>,
}

struct RootlessCascadeEntry {
    file_id_start: u32,
    file_id_end: u32,
    value: TrackedStateIndexValue,
    next_hash_collision: u32,
}

/// Reusable per-commit cascade lookup. Clearing a commit retains all large
/// buffers, so an interval does not allocate a map and owned file id for every
/// descriptor tombstone.
struct RootlessCascadeIndex {
    file_id_arena: String,
    entries: Vec<RootlessCascadeEntry>,
    hash_heads: HashMap<u64, u32>,
    capacity_hint: usize,
    dictionary_promoted: bool,
}

/// Small-first dictionary used by the bulk encoded historical lookup.
///
/// Most exact batches contain many row identities for one file. Starting
/// with the row count would size two distinct-id containers for every row.
/// Once a batch proves it has high file cardinality, both containers promote
/// once to the known upper bound so the unique-file case still has O(1) large
/// allocations.
struct EncodedReplayFileIdDictionary {
    file_ids: Vec<SharedStr>,
    ordinals: HashMap<SharedStr, u32>,
    capacity_hint: usize,
    promoted: bool,
}

impl RootlessReplayOverlay {
    fn with_capacities(row_capacity: usize, encoded_key_capacity: usize) -> Self {
        let file_id_capacity = row_capacity.min(SMALL_FILE_ID_DICTIONARY_CAPACITY);
        Self {
            key_arena: Vec::with_capacity(encoded_key_capacity),
            entries: Vec::with_capacity(row_capacity),
            key_hash_heads: HashMap::with_capacity(row_capacity),
            file_id_arena: String::with_capacity(
                file_id_capacity.saturating_mul(ESTIMATED_FILE_ID_BYTES),
            ),
            file_ids: Vec::with_capacity(file_id_capacity),
            file_id_hash_heads: HashMap::with_capacity(file_id_capacity),
            file_id_capacity_hint: row_capacity,
            file_id_dictionary_promoted: false,
        }
    }

    fn promote_file_id_dictionary_if_needed(&mut self) {
        if self.file_id_dictionary_promoted
            || self.file_ids.len() < SMALL_FILE_ID_DICTIONARY_CAPACITY
            || self.file_id_capacity_hint <= self.file_ids.len()
        {
            return;
        }
        let additional_entries = self
            .file_id_capacity_hint
            .saturating_sub(self.file_ids.len());
        let arena_capacity = self
            .file_id_capacity_hint
            .saturating_mul(ESTIMATED_FILE_ID_BYTES);
        self.file_id_arena
            .reserve_exact(arena_capacity.saturating_sub(self.file_id_arena.len()));
        self.file_ids.reserve_exact(additional_entries);
        self.file_id_hash_heads.reserve(additional_entries);
        self.file_id_dictionary_promoted = true;
    }

    fn key(&self, entry: &RootlessReplayEntry) -> &[u8] {
        &self.key_arena[entry.key_start as usize..entry.key_end as usize]
    }

    fn find_key(&self, encoded_key: &[u8]) -> Option<usize> {
        let mut ordinal = self
            .key_hash_heads
            .get(&xxh3_64(encoded_key))
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        while ordinal != NO_ROOTLESS_REPLAY_ORDINAL {
            let entry = &self.entries[ordinal as usize];
            if self.key(entry) == encoded_key {
                return Some(ordinal as usize);
            }
            ordinal = entry.next_hash_collision;
        }
        None
    }

    fn intern_file_id(&mut self, file_id: Option<&str>) -> Result<u32, LixError> {
        let Some(file_id) = file_id else {
            return Ok(NO_ROOTLESS_REPLAY_ORDINAL);
        };
        let hash = xxh3_64(file_id.as_bytes());
        let mut ordinal = self
            .file_id_hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        while ordinal != NO_ROOTLESS_REPLAY_ORDINAL {
            let entry = &self.file_ids[ordinal as usize];
            if &self.file_id_arena[entry.start as usize..entry.end as usize] == file_id {
                return Ok(ordinal);
            }
            ordinal = entry.next_hash_collision;
        }

        self.promote_file_id_dictionary_if_needed();
        let ordinal = u32::try_from(self.file_ids.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay file dictionary exceeds u32",
            )
        })?;
        let start = u32::try_from(self.file_id_arena.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay file arena exceeds u32",
            )
        })?;
        self.file_id_arena.push_str(file_id);
        let end = u32::try_from(self.file_id_arena.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay file arena exceeds u32",
            )
        })?;
        let head = self
            .file_id_hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        self.file_ids.push(RootlessReplayFileId {
            start,
            end,
            next_hash_collision: head,
        });
        self.file_id_hash_heads.insert(hash, ordinal);
        Ok(ordinal)
    }

    fn insert_new(
        &mut self,
        encoded_key: &[u8],
        key: TrackedStateKeyRef<'_>,
        value: TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        let ordinal = u32::try_from(self.entries.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay exceeds the batch ordinal range",
            )
        })?;
        let file_id_ordinal = self.intern_file_id(key.file_id)?;
        let start = u32::try_from(self.key_arena.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay key arena exceeds u32",
            )
        })?;
        self.key_arena.extend_from_slice(encoded_key);
        let end = u32::try_from(self.key_arena.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay key arena exceeds u32",
            )
        })?;
        let hash = xxh3_64(encoded_key);
        let head = self
            .key_hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        self.entries.push(RootlessReplayEntry {
            key_start: start,
            key_end: end,
            file_id_ordinal,
            value,
            next_hash_collision: head,
        });
        self.key_hash_heads.insert(hash, ordinal);
        Ok(())
    }

    fn insert_baseline(
        &mut self,
        key: TrackedStateKey,
        value: TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        let key_ref = TrackedStateKeyRef {
            schema_key: key.schema_key.as_str(),
            file_id: key.file_id.as_deref(),
            row_pk: &key.row_pk,
        };
        let file_id_ordinal = self.intern_file_id(key_ref.file_id)?;
        let ordinal = u32::try_from(self.entries.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay exceeds the batch ordinal range",
            )
        })?;
        let start = self.key_arena.len();
        let encoded_range = encode_key_ref_into(&mut self.key_arena, key_ref);
        debug_assert_eq!(encoded_range.start, start);
        let key_start = u32::try_from(encoded_range.start).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay key arena exceeds u32",
            )
        })?;
        let key_end = u32::try_from(encoded_range.end).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay key arena exceeds u32",
            )
        })?;
        let hash = xxh3_64(&self.key_arena[encoded_range]);
        let head = self
            .key_hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        self.entries.push(RootlessReplayEntry {
            key_start,
            key_end,
            file_id_ordinal,
            value,
            next_hash_collision: head,
        });
        self.key_hash_heads.insert(hash, ordinal);
        Ok(())
    }

    fn upsert(
        &mut self,
        encoded_key: &[u8],
        key: TrackedStateKeyRef<'_>,
        mut value: TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        if let Some(ordinal) = self.find_key(encoded_key) {
            value.created_at = self.entries[ordinal].value.created_at;
            self.entries[ordinal].value = value;
            return Ok(());
        }
        self.insert_new(encoded_key, key, value)
    }

    fn apply_cascades(&mut self, cascades: &RootlessCascadeIndex) {
        for ordinal in 0..self.entries.len() {
            if self.entries[ordinal].value.deleted
                || self.entries[ordinal].file_id_ordinal == NO_ROOTLESS_REPLAY_ORDINAL
            {
                continue;
            }
            let file_id = &self.file_id_arena[self.file_ids
                [self.entries[ordinal].file_id_ordinal as usize]
                .start as usize
                ..self.file_ids[self.entries[ordinal].file_id_ordinal as usize].end as usize];
            if let Some(cascade) = cascades.get(file_id) {
                self.entries[ordinal].value =
                    cascade_tombstone(cascade, &self.entries[ordinal].value);
            }
        }
    }

    fn finish(mut self) -> RootlessReplayBatch {
        let key_arena = &self.key_arena;
        self.entries.sort_unstable_by(|left, right| {
            key_arena[left.key_start as usize..left.key_end as usize]
                .cmp(&key_arena[right.key_start as usize..right.key_end as usize])
        });
        RootlessReplayBatch {
            key_arena: Bytes::from(self.key_arena),
            entries: self.entries,
        }
    }

    #[cfg(test)]
    fn retained_owned_key_count(&self) -> usize {
        0
    }

    #[cfg(test)]
    fn large_buffer_count(&self) -> usize {
        usize::from(!self.key_arena.is_empty())
            + usize::from(!self.entries.is_empty())
            + usize::from(!self.key_hash_heads.is_empty())
            + usize::from(!self.file_id_arena.is_empty())
            + usize::from(!self.file_ids.is_empty())
            + usize::from(!self.file_id_hash_heads.is_empty())
    }
}

impl RootlessReplayBatch {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn encoded_key(&self, ordinal: usize) -> &[u8] {
        let entry = &self.entries[ordinal];
        &self.key_arena[entry.key_start as usize..entry.key_end as usize]
    }

    fn encoded_key_owned(&self, ordinal: usize) -> Bytes {
        let entry = &self.entries[ordinal];
        self.key_arena
            .slice(entry.key_start as usize..entry.key_end as usize)
    }

    fn value(&self, ordinal: usize) -> &TrackedStateIndexValue {
        &self.entries[ordinal].value
    }

    #[cfg(test)]
    fn retained_owned_key_count(&self) -> usize {
        0
    }
}

impl RootlessCascadeIndex {
    fn with_capacity(capacity: usize) -> Self {
        let initial_capacity = capacity.min(SMALL_FILE_ID_DICTIONARY_CAPACITY);
        Self {
            file_id_arena: String::with_capacity(
                initial_capacity.saturating_mul(ESTIMATED_FILE_ID_BYTES),
            ),
            entries: Vec::with_capacity(initial_capacity),
            hash_heads: HashMap::with_capacity(initial_capacity),
            capacity_hint: capacity,
            dictionary_promoted: false,
        }
    }

    fn promote_dictionary_if_needed(&mut self) {
        if self.dictionary_promoted
            || self.entries.len() < SMALL_FILE_ID_DICTIONARY_CAPACITY
            || self.capacity_hint <= self.entries.len()
        {
            return;
        }
        let additional_entries = self.capacity_hint.saturating_sub(self.entries.len());
        let arena_capacity = self.capacity_hint.saturating_mul(ESTIMATED_FILE_ID_BYTES);
        self.file_id_arena
            .reserve_exact(arena_capacity.saturating_sub(self.file_id_arena.len()));
        self.entries.reserve_exact(additional_entries);
        self.hash_heads.reserve(additional_entries);
        self.dictionary_promoted = true;
    }

    fn clear(&mut self) {
        self.file_id_arena.clear();
        self.entries.clear();
        self.hash_heads.clear();
    }

    fn insert_descriptor(
        &mut self,
        key: TrackedStateKeyRef<'_>,
        value: &TrackedStateIndexValue,
    ) -> Result<(), LixError> {
        if key.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !value.deleted {
            return Ok(());
        }
        let start = self.file_id_arena.len();
        append_single_row_pk_external(&mut self.file_id_arena, key.row_pk)?;
        let end = self.file_id_arena.len();
        let hash = xxh3_64(&self.file_id_arena.as_bytes()[start..end]);
        let mut ordinal = self
            .hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        while ordinal != NO_ROOTLESS_REPLAY_ORDINAL {
            let entry = &self.entries[ordinal as usize];
            let next_hash_collision = entry.next_hash_collision;
            let matches = &self.file_id_arena
                [entry.file_id_start as usize..entry.file_id_end as usize]
                == &self.file_id_arena[start..end];
            if matches {
                self.file_id_arena.truncate(start);
                self.entries[ordinal as usize].value = value.clone();
                return Ok(());
            }
            ordinal = next_hash_collision;
        }
        self.promote_dictionary_if_needed();
        let entry_ordinal = u32::try_from(self.entries.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state cascade index exceeds u32",
            )
        })?;
        let file_id_start = u32::try_from(start).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state cascade arena exceeds u32",
            )
        })?;
        let file_id_end = u32::try_from(end).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state cascade arena exceeds u32",
            )
        })?;
        let head = self
            .hash_heads
            .get(&hash)
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        self.entries.push(RootlessCascadeEntry {
            file_id_start,
            file_id_end,
            value: value.clone(),
            next_hash_collision: head,
        });
        self.hash_heads.insert(hash, entry_ordinal);
        Ok(())
    }

    fn get(&self, file_id: &str) -> Option<&TrackedStateIndexValue> {
        let mut ordinal = self
            .hash_heads
            .get(&xxh3_64(file_id.as_bytes()))
            .copied()
            .unwrap_or(NO_ROOTLESS_REPLAY_ORDINAL);
        while ordinal != NO_ROOTLESS_REPLAY_ORDINAL {
            let entry = &self.entries[ordinal as usize];
            if &self.file_id_arena[entry.file_id_start as usize..entry.file_id_end as usize]
                == file_id
            {
                return Some(&entry.value);
            }
            ordinal = entry.next_hash_collision;
        }
        None
    }
}

impl EncodedReplayFileIdDictionary {
    fn with_capacity_hint(capacity_hint: usize) -> Self {
        let initial_capacity = capacity_hint.min(SMALL_FILE_ID_DICTIONARY_CAPACITY);
        Self {
            file_ids: Vec::with_capacity(initial_capacity),
            ordinals: HashMap::with_capacity(initial_capacity),
            capacity_hint,
            promoted: false,
        }
    }

    fn promote_if_needed(&mut self) {
        if self.promoted
            || self.file_ids.len() < SMALL_FILE_ID_DICTIONARY_CAPACITY
            || self.capacity_hint <= self.file_ids.len()
        {
            return;
        }
        let additional_entries = self.capacity_hint.saturating_sub(self.file_ids.len());
        self.file_ids.reserve_exact(additional_entries);
        self.ordinals.reserve(additional_entries);
        self.promoted = true;
    }

    fn intern(&mut self, file_id: SharedStr) -> Result<u32, LixError> {
        if let Some(&ordinal) = self.ordinals.get(file_id.as_str()) {
            return Ok(ordinal);
        }
        self.promote_if_needed();
        let ordinal = u32::try_from(self.file_ids.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state batch replay file dictionary exceeds u32",
            )
        })?;
        self.ordinals.insert(file_id.clone(), ordinal);
        self.file_ids.push(file_id);
        Ok(ordinal)
    }

    fn into_file_ids(self) -> Vec<SharedStr> {
        self.file_ids
    }
}

fn append_single_row_pk_external(arena: &mut String, row_pk: &RowPk) -> Result<(), LixError> {
    let [component] = row_pk.components.as_slice() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_delta file descriptor tombstone has invalid identity: row primary key is not a single-component tuple",
        ));
    };
    match component {
        RowPkComponent::Uuid(bytes) => {
            write!(arena, "{}", uuid::Uuid::from_bytes(*bytes).as_hyphenated())
                .expect("writing into String cannot fail");
        }
        RowPkComponent::Integer(value) => {
            write!(arena, "{value}").expect("writing into String cannot fail");
        }
        RowPkComponent::String(value) => arena.push_str(value),
        RowPkComponent::Bytes(value) => {
            base64::engine::general_purpose::STANDARD.encode_string(value, arena);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TrackedStateIdentity {
    schema_key: String,
    file_id: Option<String>,
    row_pk: RowPk,
}

struct TrackedStateRowWinner {
    identity: TrackedStateIdentity,
    cascade: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrackedStateRowWinnerKind {
    Direct,
    FileDeleteCascade,
    CollectionGenerationCascade,
}

/// Factory for tracked-state readers, root writers, and commit-root rebuilders.
///
/// Tracked state is stored as content-addressed roots. Branch refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone)]
pub(crate) struct TrackedStateContext {
    tree: TrackedStateTree,
}

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
        }
    }

    /// Creates a commit-id-addressed tracked-state reader.
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        TrackedStateStoreReader {
            store,
            tree: self.tree.clone(),
            point_replay_intervals: HashMap::new(),
            point_value_cache: HashMap::new(),
            commit_delta_value_cache: HashMap::new(),
            point_replay_commits: HashMap::new(),
            commit_delta_point_cache: storage::CommitDeltaPointReadCache::default(),
        }
    }

    /// Creates a tracked-state writer over a caller-owned transaction and write set.
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        TrackedStateWriter {
            chunk_overlay: storage::TrackedStateChunkOverlay::new(),
            staged_roots: BTreeMap::new(),
            transient_chunk_hashes: HashSet::new(),
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    pub(crate) fn writer_with_rebuild_state<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
        state: TrackedStateTransientRebuildState,
    ) -> TrackedStateWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        TrackedStateWriter {
            chunk_overlay: state.chunk_overlay,
            staged_roots: state.staged_roots,
            transient_chunk_hashes: state.transient_chunk_hashes,
            tree: self.tree.clone(),
            store,
            writes,
        }
    }

    /// Creates an explicit tracked-state commit-root rebuilder.
    ///
    /// Normal rooted commits publish immutable root metadata directly. This
    /// rebuilder restores the addressed chunks from changelog facts; rootless
    /// commits are reconstructed only for a transient correctness audit.
    pub(crate) fn root_rebuilder<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedStateRootRebuilder<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        let _ = self;
        TrackedStateRootRebuilder { store, writes }
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
    /// Reused by one reader's repeated historical point probes. SQL history
    /// providers often resolve a descriptor, its ancestors, and plugin state
    /// at the same observed commit.
    point_replay_intervals: HashMap<CommitId, PointReplayInterval>,
    point_value_cache: HashMap<(String, TrackedStateKey), Option<TrackedStateIndexValue>>,
    /// Reuses direct delta lookups when several observed commits share a
    /// rootless first-parent interval. This stays identity-routed: a cache
    /// miss always issues a point read, never an unbounded schema scan.
    commit_delta_value_cache: HashMap<(CommitId, TrackedStateKey), Option<TrackedStateIndexValue>>,
    /// Commit topology and sparse-root probes are shared by all point reads
    /// in one snapshot, so resolving neighbouring observed revisions never
    /// reloads the same changelog records.
    point_replay_commits: HashMap<CommitId, PointReplayCommit>,
    /// Shares decoded immutable manifests between rootless layout discovery
    /// and the later delta scan in this storage snapshot.
    commit_delta_point_cache: storage::CommitDeltaPointReadCache,
}

struct DiffCommitRootValidationCache {
    commit_delta_winners: HashMap<String, HashMap<TrackedStateIdentity, ChangeId>>,
    commit_root_metadata: HashMap<String, TrackedStateCommitRoot>,
    commit_roots: HashMap<String, Option<TrackedStateRootId>>,
    tree_values: HashMap<(TrackedStateRootId, TrackedStateKey), Option<TrackedStateIndexValue>>,
    changelog_first_parents: HashMap<String, Option<CommitId>>,
}

#[derive(Clone)]
struct PointReplayCommit {
    parent_commit_id: Option<CommitId>,
    root_id: Option<TrackedStateRootId>,
    rootless: bool,
    replacement_generation: Option<storage::CommitDeltaReplacementGeneration>,
    state_manifest: Arc<storage::AuthenticatedReplayCommitStateManifest>,
}

/// One immutable first-parent interval shared by every cached suffix view.
/// A reader that discovers `n` commits retains `n` commit ids, not the
/// `n * (n + 1) / 2` ids required by cloning every suffix.
#[derive(Clone)]
struct PointReplayInterval {
    commits: Arc<[CommitId]>,
    start: usize,
    baseline_root: Option<TrackedStateRootId>,
}

impl PointReplayInterval {
    fn new(commits: Vec<CommitId>, baseline_root: Option<TrackedStateRootId>) -> Self {
        Self {
            commits: commits.into(),
            start: 0,
            baseline_root,
        }
    }

    fn commits(&self) -> &[CommitId] {
        &self.commits[self.start..]
    }

    fn cache_suffixes(&self, cache: &mut HashMap<CommitId, Self>) {
        for (offset, commit_id) in self.commits().iter().copied().enumerate() {
            cache.entry(commit_id).or_insert_with(|| Self {
                commits: Arc::clone(&self.commits),
                start: self.start + offset,
                baseline_root: self.baseline_root.clone(),
            });
        }
    }
}

impl DiffCommitRootValidationCache {
    fn new() -> Self {
        Self {
            commit_delta_winners: HashMap::new(),
            commit_root_metadata: HashMap::new(),
            commit_roots: HashMap::new(),
            tree_values: HashMap::new(),
            changelog_first_parents: HashMap::new(),
        }
    }
}

fn collection_generation_cascade_winners(
    winners: &HashMap<TrackedStateIdentity, ChangeId>,
    row_map: &HashMap<TrackedStateIdentity, &TrackedStateIndexValue>,
) -> Result<BTreeMap<(String, Option<String>), ChangeId>, LixError> {
    let mut cascades = BTreeMap::new();
    for (identity, change_id) in winners {
        if identity.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        let Some(value) = row_map.get(identity) else {
            continue;
        };
        if value.deleted || value.change_id != *change_id {
            continue;
        }
        let scope = crate::collection_generation::collection_scope_from_row_pk(&identity.row_pk)?;
        if cascades.insert(scope.clone(), *change_id).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("commit contains duplicate collection-generation scope {scope:?}"),
            ));
        }
    }
    Ok(cascades)
}

impl<S> TrackedStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn scan_batch_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedTrackedStateBatch, LixError> {
        self.scan_batch_at_commit_inner(commit_id, request, None, false)
            .await
    }

    /// Reads one bounded canonical page, excluding the complete identity in
    /// `exclusive_after`. Durable trees prune preceding subtrees before row
    /// materialization, making page cost proportional to the page plus tree
    /// depth instead of all preceding rows.
    pub(crate) async fn scan_batch_at_commit_page(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
        exclusive_after: Option<&TrackedStateKey>,
    ) -> Result<MaterializedTrackedStateBatch, LixError> {
        self.scan_batch_at_commit_inner(commit_id, request, exclusive_after, true)
            .await
    }

    async fn scan_batch_at_commit_inner(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
        exclusive_after: Option<&TrackedStateKey>,
        bounded_tree_page: bool,
    ) -> Result<MaterializedTrackedStateBatch, LixError> {
        let mut tree_request = tree_scan_request_from_tracked(request);
        if bounded_tree_page {
            tree_request.limit = request.limit;
        }
        let materialization = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let durable_root = self.tree.load_root(&self.store, commit_id).await?;
        if request_has_exact_keys(&tree_request) || durable_root.is_some() {
            #[cfg(feature = "storage-benches")]
            if durable_root.is_some() {
                crate::storage_bench::record_tracked_scan_durable_root();
            } else {
                crate::storage_bench::record_tracked_scan_exact_keys();
            }
            let mut entries = if bounded_tree_page
                && !request_has_exact_keys(&tree_request)
                && let Some(root_id) = durable_root.as_ref()
            {
                self.tree
                    .scan_after(&self.store, root_id, &tree_request, exclusive_after)
                    .await?
            } else {
                self.index_entries_from_exact_or_durable_root(
                    commit_id,
                    &tree_request,
                    durable_root.as_ref(),
                )
                .await?
            };
            if !request.filter.include_tombstones {
                entries.retain(|(_, value)| !value.deleted());
            }
            if let Some(limit) = request.limit {
                entries.truncate(limit);
            }
            return materialize_batch_from_index_entries(&self.store, entries, &materialization)
                .await;
        }

        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_tracked_scan_rootless_replay();
        let replay = self
            .replay_index_batch_for_request_at_commit(commit_id, &tree_request)
            .await?;
        let mut decoded_keys = Vec::with_capacity(
            request
                .limit
                .map_or(replay.len(), |limit| limit.min(replay.len())),
        );
        let mut values = Vec::with_capacity(decoded_keys.capacity());
        for ordinal in 0..replay.len() {
            if request
                .limit
                .is_some_and(|limit| decoded_keys.len() >= limit)
            {
                break;
            }
            let key = decode_key_shared(replay.encoded_key_owned(ordinal))?;
            if exclusive_after.is_some_and(|after| {
                compare_tracked_state_key_refs(
                    key.as_ref(),
                    TrackedStateKeyRef {
                        schema_key: &after.schema_key,
                        file_id: after.file_id.as_deref(),
                        row_pk: &after.row_pk,
                    },
                ) != std::cmp::Ordering::Greater
            }) {
                continue;
            }
            if !tree_request.matches_ref(key.as_ref(), replay.value(ordinal)) {
                continue;
            }
            decoded_keys.push(key);
            values.push(replay.value(ordinal).clone());
        }
        let entries = decoded_keys
            .iter()
            .zip(values)
            .map(|(key, value)| (key.as_ref(), value))
            .collect();
        materialize_batch_from_index_entry_refs(&self.store, entries, &materialization).await
    }

    pub(crate) async fn load_projected_batch_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        let key_refs = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            })
            .collect::<Vec<_>>();
        self.load_projected_batch_at_commit_refs(commit_id, &key_refs, projection)
            .await
    }

    /// Resolves exact index facts at a commit without materializing payloads.
    /// Tombstones are returned rather than filtered so admission code can
    /// validate selected merge members in one batched point read.
    pub(crate) async fn index_values_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        self.replay_index_values_for_keys_at_commit(commit_id, keys)
            .await
    }

    pub(crate) async fn load_projected_batch_at_commit_refs(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        if keys.is_empty() {
            return Ok(MaterializedTrackedStateExactBatch::default());
        }
        if u32::try_from(keys.len()).is_err() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact tracked-state request exceeds the batch ordinal range",
            ));
        }

        // Sort compact input ordinals instead of building one heap-owned
        // `Vec<usize>` per distinct key. Duplicates retain their original
        // positions through `unique_ordinal_by_input`.
        let mut ordered_indices = (0..keys.len()).collect::<Vec<_>>();
        ordered_indices.sort_unstable_by(|left, right| {
            compare_tracked_state_key_refs(keys[*left], keys[*right])
        });
        let unique_key_count = 1 + ordered_indices
            .windows(2)
            .filter(|pair| {
                compare_tracked_state_key_refs(keys[pair[0]], keys[pair[1]])
                    != std::cmp::Ordering::Equal
            })
            .count();
        let mut unique_keys = Vec::with_capacity(unique_key_count);
        let mut unique_ordinal_by_input = vec![0_u32; keys.len()];
        let mut offset = 0;
        while offset < ordered_indices.len() {
            let first_index = ordered_indices[offset];
            let unique_ordinal =
                u32::try_from(unique_keys.len()).expect("request row count was bounded to u32");
            unique_keys.push(keys[first_index]);
            let mut end = offset + 1;
            while end < ordered_indices.len()
                && compare_tracked_state_key_refs(keys[ordered_indices[end]], keys[first_index])
                    == std::cmp::Ordering::Equal
            {
                end += 1;
            }
            for &input_index in &ordered_indices[offset..end] {
                unique_ordinal_by_input[input_index] = unique_ordinal;
            }
            offset = end;
        }
        debug_assert_eq!(unique_keys.len(), unique_key_count);

        let values = if unique_keys.len() < HISTORICAL_ENCODED_LOOKUP_MIN_ROWS {
            let owned_keys = unique_keys
                .iter()
                .map(|key| TrackedStateKey {
                    schema_key: key.schema_key.to_owned(),
                    file_id: key.file_id.map(str::to_owned),
                    row_pk: key.row_pk.clone(),
                })
                .collect::<Vec<_>>();
            self.commit_root_values_for_keys(commit_id, &owned_keys)
                .await?
        } else {
            // The key owners above are already unique and ordered. Encode them
            // once into one shared arena, then keep the whole lookup on the
            // encoded bulk path. In particular, rootless history must not
            // route this batch back through the point cache's
            // `BTreeMap<Key, Vec<slot>>` duplicate adapter and allocate one map
            // node and slot vector per row.
            let mut encoded_key_batch =
                TrackedStateKeyBatchBuilder::with_row_capacity(unique_keys.len());
            for &key in &unique_keys {
                encoded_key_batch.push(key);
            }
            let encoded_keys = encoded_key_batch.finish();
            self.commit_root_values_for_unique_encoded_keys(commit_id, &encoded_keys)
                .await?
        };
        if values.len() != unique_keys.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "exact tracked-state read returned {} values for {} unique keys",
                    values.len(),
                    unique_keys.len()
                ),
            ));
        }
        let mut entries = Vec::with_capacity(values.iter().flatten().count());
        let mut present_ordinal_by_unique = vec![None; unique_keys.len()];
        for (unique_ordinal, (key, value)) in unique_keys.into_iter().zip(values).enumerate() {
            if let Some(value) = value {
                present_ordinal_by_unique[unique_ordinal] = Some(
                    u32::try_from(entries.len()).expect("request row count was bounded to u32"),
                );
                entries.push((key, value));
            }
        }
        let slots = unique_ordinal_by_input
            .into_iter()
            .map(|unique_ordinal| present_ordinal_by_unique[unique_ordinal as usize])
            .collect();
        let batch =
            materialize_batch_from_index_entry_refs(&self.store, entries, projection).await?;
        MaterializedTrackedStateExactBatch::new(batch, slots)
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn load_batch_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        self.load_projected_batch_at_commit(commit_id, keys, &ChangeRecordProjection::full())
            .await
    }

    pub(crate) async fn diff_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits(self, left_commit_id, right_commit_id, request).await
    }

    /// Resolves the exact tracked identities affected by descriptor cascades.
    ///
    /// File-descriptor tombstones implicitly delete every tracked row owned by
    /// that file, but those cascade members are not authored commit-delta
    /// rows. Historical keys are ordered by schema before file ID, so a
    /// file-only scan would still walk the whole root. Build the schema
    /// inventory from the visible schema catalog and scan only the resulting
    /// `(schema_key, file_id)` prefixes at the endpoint where the file is
    /// live. If the target changes the catalog, also include both historical
    /// endpoint inventories.
    pub(crate) async fn descriptor_dependency_closure(
        &mut self,
        current_commit_id: &str,
        desired_commit_id: &str,
        dependency_commit_id: &str,
        target_delta: &[(TrackedStateKey, TrackedStateIndexValue)],
        file_ids: &[String],
        visible_schema_keys: &[String],
    ) -> Result<Vec<TrackedStateKey>, LixError> {
        let mut keys = target_delta
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();

        let mut schema_keys = visible_schema_keys.iter().cloned().collect::<BTreeSet<_>>();
        if target_delta
            .iter()
            .any(|(key, _)| key.schema_key == REGISTERED_SCHEMA_KEY)
        {
            schema_keys.extend(
                self.registered_schema_keys_at_commit(desired_commit_id)
                    .await?,
            );
            schema_keys.extend(
                self.registered_schema_keys_at_commit(current_commit_id)
                    .await?,
            );
        }
        let request = TrackedStateScanRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: schema_keys.into_iter().collect(),
                file_ids: file_ids
                    .iter()
                    .cloned()
                    .map(NullableKeyFilter::Value)
                    .collect(),
                ..crate::tracked_state::TrackedStateFilter::default()
            },
            read_columns: crate::tracked_state::TrackedStateReadColumns {
                columns: vec!["change_id".to_string()],
            },
            limit: None,
        };
        let rows = self
            .scan_batch_at_commit(dependency_commit_id, &request)
            .await?;
        keys.extend(rows.iter().map(|row| TrackedStateKey {
            schema_key: row.schema_key().to_owned(),
            file_id: row.file_id().map(str::to_owned),
            row_pk: row.row_pk().clone(),
        }));
        Ok(keys.into_iter().collect())
    }

    async fn registered_schema_keys_at_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<BTreeSet<String>, LixError> {
        let rows = self
            .scan_batch_at_commit(
                commit_id,
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..crate::tracked_state::TrackedStateFilter::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["change_id".to_string()],
                    },
                    limit: None,
                },
            )
            .await?;
        rows.iter()
            .map(|row| {
                row.row_pk().as_single_string_owned().map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("registered schema dependency identity is invalid: {error}"),
                    )
                })
            })
            .collect()
    }

    /// Loads the identities and index values authored or selected by exactly
    /// one commit, without resolving inherited tracked state.
    pub(crate) async fn commit_delta_members(
        &mut self,
        commit_id: CommitId,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        storage::scan_commit_delta_members(&self.store, commit_id).await
    }

    /// Loads only the requested schema ranges from one commit delta. Semantic
    /// classification uses this to avoid hydrating unrelated commit members.
    pub(crate) async fn commit_delta_values_for_schemas(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<storage::DecodedCommitDeltaBatch, LixError> {
        self.scan_replayed_commit_delta_values(commit_id, schema_keys)
            .await
    }

    /// Probes root publication in tests and storage benchmarks. Every current
    /// protocol commit must return true; false is valid only for explicit
    /// legacy-rootless fixtures or damaged storage awaiting repair.
    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn has_durable_commit_root(&self, commit_id: &str) -> Result<bool, LixError> {
        Ok(self.tree.load_root(&self.store, commit_id).await?.is_some())
    }

    pub(crate) async fn validate_diff_rows_for_commits_against_changelog(
        &mut self,
        rows: &[(&TrackedStateDiffRow, &str)],
    ) -> Result<(), LixError> {
        let row_refs = rows.iter().map(|(row, _)| *row).collect::<Vec<_>>();
        let changes = self.load_and_validate_diff_row_changes(&row_refs).await?;
        let mut validation_cache = DiffCommitRootValidationCache::new();
        for (row, expected_commit_id) in rows {
            let change = changes.get(&row.change_id).ok_or_else(|| {
                LixError::unknown(format!(
                    "tracked-state diff row references missing changelog change '{}'",
                    row.change_id
                ))
            })?;
            let winner_identity = tracked_state_winner_identity_for_diff_row(row, change)?;
            self.validate_diff_row_commit_root_membership(
                row,
                expected_commit_id,
                &winner_identity.identity,
                winner_identity.cascade,
                change.created_at,
                &mut validation_cache,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn validate_tree_diff_batch_and_load_payloads(
        &mut self,
        batch: &TrackedStateTreeDiffBatch,
    ) -> Result<TrackedStatePayloadBatch, LixError> {
        let rows = batch.side_rows().collect::<Vec<_>>();
        let changes = self.load_routed_tree_diff_changes(&rows).await?;
        TrackedStatePayloadBatch::from_payloads(
            changes
                .into_iter()
                .map(|(change_id, change)| (change_id, change.snapshot, change.metadata)),
        )
    }

    /// Removes physical tombstones synthesized into persistent roots for a
    /// collection-generation delete. The authenticated marker is the logical
    /// commit member; exposing every retired predecessor would both expand the
    /// commit and require payloads that intentionally do not exist.
    pub(crate) async fn suppress_collection_generation_cascade_tombstones(
        &mut self,
        batch: &mut TrackedStateTreeDiffBatch,
    ) -> Result<(), LixError> {
        use crate::collection_generation::{
            COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
        };

        let mut scopes = BTreeMap::<
            (CommitId, String, Option<String>),
            (TrackedStateKey, Vec<(usize, ChangeId)>),
        >::new();
        for (ordinal, key, _, after) in batch.rows() {
            let Some(after) = after.filter(|after| after.deleted) else {
                continue;
            };
            if key.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
                continue;
            }
            let scope = (key.schema_key.to_owned(), key.file_id.map(str::to_owned));
            let marker_key = TrackedStateKey {
                schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
                file_id: None,
                row_pk: RowPk::single(collection_scope_key(CollectionScopeRef {
                    schema_key: &scope.0,
                    file_id: scope.1.as_deref(),
                })),
            };
            scopes
                .entry((after.commit_id, scope.0, scope.1))
                .or_insert_with(|| (marker_key, Vec::new()))
                .1
                .push((ordinal, after.change_id));
        }
        if scopes.is_empty() {
            return Ok(());
        }

        let mut by_commit =
            BTreeMap::<CommitId, Vec<(TrackedStateKey, Vec<(usize, ChangeId)>)>>::new();
        for ((commit_id, _, _), request) in scopes {
            by_commit.entry(commit_id).or_default().push(request);
        }
        let mut remove = vec![false; batch.len()];
        for (commit_id, requests) in by_commit {
            let keys = requests
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let records =
                storage::load_commit_delta_change_records(&self.store, commit_id, &keys).await?;
            for ((_, candidates), record) in requests.into_iter().zip(records) {
                let Some(record) = record else {
                    continue;
                };
                for (ordinal, change_id) in candidates {
                    if record.change_id == change_id && record.snapshot.is_some() {
                        remove[ordinal] = true;
                    }
                }
            }
        }
        batch.remove_rows(&remove);
        Ok(())
    }

    pub(crate) async fn load_tree_diff_comparison_payloads(
        &mut self,
        batch: &TrackedStateTreeDiffBatch,
    ) -> Result<TrackedStatePayloadBatch, LixError> {
        self.validate_tree_diff_batch_against_delta_index(batch)
            .await?;
        let changes = self
            .load_routed_tree_diff_changes(&batch.comparison_rows())
            .await?;
        TrackedStatePayloadBatch::from_payloads(
            changes
                .into_iter()
                .map(|(change_id, change)| (change_id, change.snapshot, change.metadata)),
        )
    }

    async fn load_commit_delta_values_for_encoded_queries(
        &self,
        state: &storage::AuthenticatedReplayCommitStateManifest,
        encoded_keys: Vec<Bytes>,
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let request_count = encoded_keys.len();
        let mut ordered = encoded_keys.into_iter().enumerate().collect::<Vec<_>>();
        ordered.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        let mut canonical = Vec::with_capacity(ordered.len());
        let mut canonical_ordinal_by_request = vec![0usize; request_count];
        for (request_index, encoded_key) in ordered {
            if canonical
                .last()
                .is_none_or(|previous: &Bytes| previous != &encoded_key)
            {
                canonical.push(encoded_key);
            }
            canonical_ordinal_by_request[request_index] = canonical.len() - 1;
        }
        let values = storage::load_commit_delta_values_encoded_from_replay_manifest(
            &self.store,
            state,
            &canonical,
            &self.commit_delta_point_cache,
        )
        .await?;
        Ok(canonical_ordinal_by_request
            .into_iter()
            .map(|ordinal| values[ordinal].clone())
            .collect())
    }

    async fn validate_tree_diff_batch_against_delta_index(
        &self,
        batch: &TrackedStateTreeDiffBatch,
    ) -> Result<(), LixError> {
        let rows = batch.side_rows().collect::<Vec<_>>();
        let mut by_commit = BTreeMap::<CommitId, Vec<TrackedStateTreeDiffRowRef<'_>>>::new();
        for row in rows {
            by_commit.entry(row.commit_id()).or_default().push(row);
        }
        for (commit_id, commit_rows) in by_commit {
            let state = storage::load_point_replay_commit_state(&self.store, commit_id).await?;
            let mut encoded_keys =
                TrackedStateKeyBatchBuilder::with_row_capacity(commit_rows.len());
            for row in &commit_rows {
                encoded_keys.push(TrackedStateKeyRef {
                    schema_key: row.schema_key(),
                    file_id: row.file_id(),
                    row_pk: row.row_pk(),
                });
            }
            let loaded = match state.as_ref() {
                Some(state) => {
                    self.load_commit_delta_values_for_encoded_queries(state, encoded_keys.finish())
                        .await?
                }
                None => vec![None; commit_rows.len()],
            };
            let mut collection_fallback_rows = Vec::new();
            let mut collection_fallback_keys =
                TrackedStateKeyBatchBuilder::with_row_capacity(commit_rows.len());
            for (index, (row, value)) in commit_rows.iter().zip(&loaded).enumerate() {
                if value.is_none() && row.deleted() {
                    let key = collection_cascade_payload_key(row.schema_key(), row.file_id());
                    collection_fallback_keys.push(TrackedStateKeyRef {
                        schema_key: &key.schema_key,
                        file_id: key.file_id.as_deref(),
                        row_pk: &key.row_pk,
                    });
                    collection_fallback_rows.push(index);
                }
            }
            let collection_fallback_values = match state.as_ref() {
                Some(state) => {
                    self.load_commit_delta_values_for_encoded_queries(
                        state,
                        collection_fallback_keys.finish(),
                    )
                    .await?
                }
                None => vec![None; collection_fallback_rows.len()],
            };
            let mut fallbacks = vec![None; commit_rows.len()];
            for (index, value) in collection_fallback_rows
                .into_iter()
                .zip(collection_fallback_values)
            {
                let row = commit_rows[index];
                if value.as_ref().is_some_and(|value| {
                    value.change_id == row.change_id()
                        && value.commit_id == row.commit_id()
                        && value.deleted
                        && value.updated_at() == row.updated_at()
                }) {
                    fallbacks[index] = value;
                }
            }
            let mut file_fallback_rows = Vec::new();
            let mut file_fallback_keys =
                TrackedStateKeyBatchBuilder::with_row_capacity(commit_rows.len());
            for (index, (row, value)) in commit_rows.iter().zip(&loaded).enumerate() {
                if value.is_none()
                    && fallbacks[index].is_none()
                    && row.deleted()
                    && let Some(file_id) = row.file_id()
                {
                    let key = cascade_payload_key(file_id);
                    file_fallback_keys.push(TrackedStateKeyRef {
                        schema_key: &key.schema_key,
                        file_id: key.file_id.as_deref(),
                        row_pk: &key.row_pk,
                    });
                    file_fallback_rows.push(index);
                }
            }
            let file_fallback_values = match state.as_ref() {
                Some(state) => {
                    self.load_commit_delta_values_for_encoded_queries(
                        state,
                        file_fallback_keys.finish(),
                    )
                    .await?
                }
                None => vec![None; file_fallback_rows.len()],
            };
            for (index, value) in file_fallback_rows.into_iter().zip(file_fallback_values) {
                fallbacks[index] = value;
            }
            for ((row, value), fallback) in commit_rows.iter().zip(loaded).zip(fallbacks) {
                if value.as_ref().is_some_and(|value| {
                    value.change_id == row.change_id()
                        && value.commit_id == row.commit_id()
                        && value.deleted == row.deleted()
                        && value.updated_at() == row.updated_at()
                }) {
                    continue;
                }
                if let Some(cascade) = fallback
                    && cascade.deleted
                    && cascade.change_id == row.change_id()
                    && cascade.commit_id == row.commit_id()
                    && cascade.updated_at() == row.updated_at()
                {
                    continue;
                }
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-state diff row '{}' does not match commit '{}' delta index",
                        row.change_id(),
                        commit_id
                    ),
                ));
            }
        }
        Ok(())
    }

    async fn load_and_validate_diff_row_changes(
        &mut self,
        rows: &[&TrackedStateDiffRow],
    ) -> Result<HashMap<ChangeId, ChangeRecord>, LixError> {
        self.load_routed_diff_changes(rows).await
    }

    async fn validate_diff_row_commit_root_membership(
        &mut self,
        row: &TrackedStateDiffRow,
        root_commit_id: &str,
        winner_identity: &TrackedStateIdentity,
        cascade: bool,
        change_created_at: crate::common::LixTimestamp,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        let key = TrackedStateKey {
            schema_key: row.schema_key().to_owned(),
            file_id: row.file_id().map(str::to_owned),
            row_pk: row.row_pk().clone(),
        };
        let root_metadata = self
            .load_cached_commit_root_metadata(root_commit_id, cache)
            .await?;
        self.validate_commit_root_parent_matches_changelog(root_commit_id, &root_metadata, cache)
            .await?;
        let row_value = row.index_value();
        let mut current_commit_id = root_commit_id.to_string();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current_commit_id.clone()) {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root parent chain contains cycle at commit '{current_commit_id}'"
                )));
            }

            let winner_change_id = self
                .load_cached_commit_delta_winner(&current_commit_id, winner_identity, cache)
                .await?;
            if let Some(winner_change_id) = winner_change_id {
                if winner_change_id == row.change_id {
                    self.validate_diff_row_created_at(
                        row,
                        &key,
                        &current_commit_id,
                        change_created_at,
                    )
                    .await?;
                    return Ok(());
                }
                if !cascade {
                    return Err(LixError::unknown(format!(
                        "tracked-state diff row references changelog change '{}' that is not the first-parent winner for commit '{}' and identity {:?}",
                        row.change_id, root_commit_id, winner_identity
                    )));
                }
            }

            let current_metadata =
                storage::load_snapshot_commit_root(&self.store, &current_commit_id).await?;
            let (parent_commit_id, parent_value, current_is_rootless) = if let Some(metadata) =
                current_metadata
            {
                self.validate_commit_root_parent_matches_changelog(
                    &current_commit_id,
                    &metadata,
                    cache,
                )
                .await?;
                if metadata.complete_state_fence {
                    self.validate_diff_row_created_at(
                        row,
                        &key,
                        &current_commit_id,
                        change_created_at,
                    )
                    .await?;
                    return Ok(());
                }
                let Some(parent) = metadata.parent_roots.first() else {
                    if !metadata.complete_state_fence {
                        return Err(LixError::unknown(format!(
                            "tracked-state commit-root metadata for commit '{current_commit_id}' omits physical ancestry without a complete-state fence"
                        )));
                    }
                    // A rooted commit with a semantic first parent but no
                    // physical parent root is a complete-state fence. The row
                    // was already authenticated against this snapshot and its
                    // changelog change, so there is no inherited physical
                    // ancestry left to prove beyond the fence.
                    self.validate_diff_row_created_at(
                        row,
                        &key,
                        &current_commit_id,
                        change_created_at,
                    )
                    .await?;
                    return Ok(());
                };
                let parent_value = self
                    .load_cached_tree_value(&parent.root_id, &key, cache)
                    .await?;
                (parent.commit_id, parent_value, false)
            } else {
                // Rootless commits have no per-commit snapshot ancestry. The
                // bounded point replay is their complete state proof,
                // including selected-source and engine-owned mutations that
                // are not direct commit-delta winners.
                let root_id = self
                    .load_cached_commit_root_optional(&current_commit_id, cache)
                    .await?;
                debug_assert!(root_id.is_none());
                let current_value = self
                    .replay_index_values_for_keys_at_commit(
                        &current_commit_id,
                        std::slice::from_ref(&key),
                    )
                    .await?
                    .into_iter()
                    .next()
                    .flatten();
                (
                    CommitId::parse_lix(&current_commit_id, "rootless diff row proof commit_id")?,
                    current_value,
                    true,
                )
            };
            if current_is_rootless {
                let structurally_equal = parent_value.as_ref().is_some_and(|proof| {
                    proof.change_id == row_value.change_id
                        && proof.commit_id == row_value.commit_id
                        && proof.deleted == row_value.deleted
                        && proof.updated_at == row_value.updated_at
                });
                if !structurally_equal {
                    return Err(LixError::unknown(format!(
                        "tracked-state commit-root row for commit '{}' does not match rootless replay at '{}' for inherited identity {:?}",
                        root_commit_id,
                        parent_commit_id,
                        tracked_state_identity_from_key(&key)
                    )));
                }
                self.validate_diff_row_created_at(row, &key, &current_commit_id, change_created_at)
                    .await?;
                // Point replay already proved the row against the complete
                // bounded interval. A rootless manifest deliberately has no
                // per-commit snapshot ancestry to walk beyond that proof.
                return Ok(());
            }
            if parent_value.as_ref() != Some(&row_value) {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root row for commit '{}' does not match parent root '{}' for inherited identity {:?}",
                    root_commit_id,
                    parent_commit_id,
                    tracked_state_identity_from_key(&key)
                )));
            }
            current_commit_id = parent_commit_id.to_string();
        }
    }

    async fn validate_commit_root_parent_matches_changelog(
        &mut self,
        commit_id: &str,
        metadata: &TrackedStateCommitRoot,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        if metadata.complete_state_fence {
            return match metadata.parent_roots.as_slice() {
                [] => Ok(()),
                [source] if source.commit_id != commit_id && source.root_id == metadata.root_id => {
                    Ok(())
                }
                _ => Err(LixError::unknown(format!(
                    "tracked-state complete-state fence for commit '{commit_id}' has invalid physical source ancestry"
                ))),
            };
        }
        if metadata.parent_roots.len() > 1 {
            return Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{commit_id}' has more than one first-parent root"
            )));
        }
        let changelog_first_parent = self
            .load_cached_changelog_first_parent(commit_id, cache)
            .await?;
        let expected_parent = match changelog_first_parent {
            Some(first_parent_id) => Some((
                first_parent_id,
                self.load_cached_commit_root_optional(&first_parent_id.to_string(), cache)
                    .await?,
            )),
            None => None,
        };
        match (expected_parent, metadata.parent_roots.first()) {
            (None, None) => Ok(()),
            (Some((expected_parent_id, expected_root)), Some(parent))
                if parent.commit_id == expected_parent_id
                    && expected_root
                        .as_ref()
                        .is_none_or(|expected_root| parent.root_id == *expected_root) =>
            {
                Ok(())
            }
            (Some((expected_parent_id, Some(_))), Some(parent))
                if parent.commit_id == expected_parent_id =>
            {
                Err(LixError::unknown(format!(
                    "tracked-state commit-root metadata for commit '{commit_id}' references stale root for commit-root parent '{expected_parent_id}'"
                )))
            }
            (Some((expected_parent_id, _)), Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{}' references parent '{}' but its first parent is '{}'",
                commit_id, parent.commit_id, expected_parent_id
            ))),
            // Empty physical ancestry is an explicit complete-state fence.
            // The changelog parent remains the semantic history edge; it does
            // not require the snapshot to retain that parent's root.
            (Some((_expected_parent_id, _)), None) if metadata.complete_state_fence => Ok(()),
            (Some((expected_parent_id, _)), None) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for commit '{commit_id}' omits first-parent root '{expected_parent_id}' without a complete-state fence"
            ))),
            (None, Some(parent)) => Err(LixError::unknown(format!(
                "tracked-state commit-root metadata for root commit '{}' references unexpected parent '{}'",
                commit_id, parent.commit_id
            ))),
        }
    }

    async fn load_cached_commit_delta_winners(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<HashMap<TrackedStateIdentity, ChangeId>, LixError> {
        self.ensure_cached_commit_delta_winners(commit_id, cache)
            .await?;
        Ok(cache
            .commit_delta_winners
            .get(commit_id)
            .cloned()
            .expect("commit-delta winners should be cached after loading"))
    }

    async fn load_cached_commit_delta_winner(
        &mut self,
        commit_id: &str,
        identity: &TrackedStateIdentity,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<ChangeId>, LixError> {
        self.ensure_cached_commit_delta_winners(commit_id, cache)
            .await?;
        Ok(cache
            .commit_delta_winners
            .get(commit_id)
            .and_then(|winners| winners.get(identity))
            .copied())
    }

    async fn ensure_cached_commit_delta_winners(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<(), LixError> {
        if cache.commit_delta_winners.contains_key(commit_id) {
            return Ok(());
        }
        let commit_id_typed = CommitId::parse_lix(commit_id, "commit-delta winner commit_id")?;
        let commit_ids = [commit_id_typed];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await?;
        let Some(_) = batch.into_iter().next().and_then(|(_, value)| value) else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' is missing while validating tracked-state commit-root rows"
            )));
        };
        let topology =
            match storage::load_published_commit_state_topology(&self.store, commit_id_typed)
                .await?
            {
                Some(topology) => topology,
                None => {
                    return Err(storage::missing_commit_state_manifest_error(
                        &self.store,
                        commit_id_typed,
                    )
                    .await);
                }
            };
        if topology.mutation_member_count() == 0 {
            cache
                .commit_delta_winners
                .insert(commit_id.to_string(), HashMap::new());
            return Ok(());
        }
        let mut winners = HashMap::new();
        for (key, value) in storage::scan_commit_delta_members(&self.store, commit_id_typed).await?
        {
            if winners
                .insert(
                    TrackedStateIdentity {
                        schema_key: key.schema_key,
                        file_id: key.file_id,
                        row_pk: key.row_pk,
                    },
                    value.change_id,
                )
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit-delta '{commit_id}' contains duplicate tracked identities"),
                ));
            }
        }
        cache
            .commit_delta_winners
            .insert(commit_id.to_string(), winners);
        Ok(())
    }

    async fn load_cached_commit_root_metadata(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<TrackedStateCommitRoot, LixError> {
        if let Some(metadata) = cache.commit_root_metadata.get(commit_id) {
            return Ok(metadata.clone());
        }
        self.load_cached_changelog_first_parent(commit_id, cache)
            .await?;
        let metadata = storage::load_snapshot_commit_root(&self.store, commit_id)
            .await?
            .ok_or_else(|| missing_commit_root_error(commit_id))?;
        cache
            .commit_root_metadata
            .insert(commit_id.to_string(), metadata.clone());
        Ok(metadata)
    }

    async fn load_cached_commit_root_optional(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        if let Some(root_id) = cache.commit_roots.get(commit_id) {
            return Ok(root_id.clone());
        }
        self.load_cached_changelog_first_parent(commit_id, cache)
            .await?;
        let root_id = storage::load_snapshot_commit_root(&self.store, commit_id)
            .await?
            .map(|root| root.root_id);
        cache
            .commit_roots
            .insert(commit_id.to_string(), root_id.clone());
        Ok(root_id)
    }

    async fn load_cached_tree_value(
        &mut self,
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<TrackedStateIndexValue>, LixError> {
        let cache_key = (root_id.clone(), key.clone());
        if let Some(value) = cache.tree_values.get(&cache_key) {
            return Ok(value.clone());
        }
        let value = self
            .tree
            .get_many(&self.store, root_id, std::slice::from_ref(key))
            .await?
            .into_iter()
            .next()
            .flatten();
        cache.tree_values.insert(cache_key, value.clone());
        Ok(value)
    }

    async fn load_cached_changelog_first_parent(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<Option<CommitId>, LixError> {
        if let Some(parent_id) = cache.changelog_first_parents.get(commit_id) {
            return Ok(*parent_id);
        }
        let commit_ids = [CommitId::parse_lix(
            commit_id,
            "changelog first parent commit_id",
        )?];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await?;
        let Some(entry) = batch.into_iter().next().and_then(|(_, value)| value) else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' is missing while validating tracked-state commit-root metadata"
            )));
        };
        let record = entry;
        let parent_id = record.parent_commit_ids.first().copied();
        cache
            .changelog_first_parents
            .insert(commit_id.to_string(), parent_id);
        Ok(parent_id)
    }

    async fn validate_diff_row_created_at(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
        commit_id: &str,
        change_created_at: crate::common::LixTimestamp,
    ) -> Result<(), LixError> {
        #[cfg(test)]
        DIFF_ROW_CREATED_AT_VALIDATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut expected_created_at = change_created_at;
        if let Some(metadata) = storage::load_snapshot_commit_root(&self.store, commit_id).await? {
            if let Some(parent) = metadata.parent_roots.first() {
                let parent_value = self
                    .tree
                    .get_many(&self.store, &parent.root_id, std::slice::from_ref(key))
                    .await?
                    .into_iter()
                    .next()
                    .flatten();
                if let Some(parent_value) = parent_value {
                    expected_created_at = parent_value.created_at();
                }
            }
        } else {
            let typed_commit_id = CommitId::parse_lix(commit_id, "diff row owner commit_id")?;
            let authority_ids = storage::load_commit_state_authority_ids(
                &self.store,
                std::slice::from_ref(&typed_commit_id),
            )
            .await?;
            if authority_ids.into_iter().next().flatten().is_none() {
                return Err(missing_commit_root_error(commit_id));
            }
            if let Some(parent_commit_id) = self
                .load_cached_changelog_first_parent(
                    commit_id,
                    &mut DiffCommitRootValidationCache::new(),
                )
                .await?
            {
                if let Some(parent_value) = self
                    .replay_index_values_for_keys_at_commit(
                        &parent_commit_id.to_string(),
                        std::slice::from_ref(key),
                    )
                    .await?
                    .into_iter()
                    .next()
                    .flatten()
                {
                    expected_created_at = parent_value.created_at();
                }
            }
        }
        if expected_created_at == change_created_at {
            if let Some(merge_parent_created_at) = self
                .load_merge_parent_created_at_for_row(commit_id, row, key)
                .await?
            {
                expected_created_at = merge_parent_created_at;
            }
        }
        if expected_created_at == change_created_at && row.commit_id != commit_id {
            if let Some(source_created_at) =
                self.load_parent_created_at_for_row_commit(row, key).await?
            {
                expected_created_at = source_created_at;
            }
        }
        if row.created_at == expected_created_at {
            return Ok(());
        }
        Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' at commit '{}' created_at '{}' does not match first ancestry timestamp '{}'",
            row.change_id, commit_id, row.created_at, expected_created_at
        )))
    }

    async fn load_merge_parent_created_at_for_row(
        &mut self,
        commit_id: &str,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
    ) -> Result<Option<crate::common::LixTimestamp>, LixError> {
        let commit_ids = [CommitId::parse_lix(commit_id, "merge parent commit_id")?];
        let mut changelog_reader = ChangelogContext::new().reader(&mut self.store);
        let batch = changelog_reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await?;
        let Some(commit) = batch.into_iter().next().and_then(|(_, value)| value) else {
            return Ok(None);
        };
        for parent_id in commit.parent_commit_ids.iter().skip(1) {
            let Some(parent_root) = storage::load_root(&self.store, &parent_id.to_string()).await?
            else {
                continue;
            };
            let parent_value = self
                .tree
                .get_many(&self.store, &parent_root, std::slice::from_ref(key))
                .await?
                .into_iter()
                .next()
                .flatten();
            if let Some(parent_value) = parent_value {
                if parent_value.change_id == row.change_id {
                    return Ok(Some(parent_value.created_at()));
                }
            }
        }
        Ok(None)
    }

    async fn load_parent_created_at_for_row_commit(
        &mut self,
        row: &TrackedStateDiffRow,
        key: &TrackedStateKey,
    ) -> Result<Option<crate::common::LixTimestamp>, LixError> {
        let row_commit_id = row.commit_id.to_string();
        let Some(metadata) =
            storage::load_snapshot_commit_root(&self.store, &row_commit_id).await?
        else {
            return Ok(None);
        };
        let Some(parent) = metadata.parent_roots.first() else {
            return Ok(None);
        };
        let parent_value = self
            .tree
            .get_many(&self.store, &parent.root_id, std::slice::from_ref(key))
            .await?
            .into_iter()
            .next()
            .flatten();
        Ok(parent_value.map(|value| value.created_at()))
    }

    /// Runs the full O(total rows) tracked-root coverage audit.
    ///
    /// Normal diff validates root metadata and changed rows only. Maintenance
    /// and repair tooling can call this when it deliberately needs fsck-level
    /// assurance for every unchanged row too.
    #[cfg(test)]
    pub(crate) async fn validate_commit_root_against_changelog(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        self.validate_tree_rows_at_commit_against_changelog(
            commit_id,
            &TrackedStateTreeScanRequest::default(),
        )
        .await
    }

    #[cfg(test)]
    async fn validate_tree_rows_at_commit_against_changelog(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<(), LixError> {
        let metadata = self
            .load_cached_commit_root_metadata(commit_id, &mut DiffCommitRootValidationCache::new())
            .await?;
        self.validate_commit_root_metadata_against_changelog_with_request(
            commit_id, metadata, request,
        )
        .await
    }

    async fn validate_commit_root_metadata_against_changelog(
        &mut self,
        commit_id: &str,
        metadata: TrackedStateCommitRoot,
    ) -> Result<(), LixError> {
        self.validate_commit_root_metadata_against_changelog_with_request(
            commit_id,
            metadata,
            &TrackedStateTreeScanRequest::default(),
        )
        .await
    }

    async fn validate_commit_root_metadata_against_changelog_with_request(
        &mut self,
        commit_id: &str,
        metadata: TrackedStateCommitRoot,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<(), LixError> {
        let mut validation_cache = DiffCommitRootValidationCache::new();
        validation_cache
            .commit_root_metadata
            .insert(commit_id.to_string(), metadata.clone());
        self.validate_commit_root_parent_matches_changelog(
            commit_id,
            &metadata,
            &mut validation_cache,
        )
        .await?;
        let root = metadata.root_id.clone();
        let rows = self.tree.scan(&self.store, &root, request).await?;
        self.validate_commit_root_coverage(commit_id, request, &rows, &metadata)
            .await?;
        let rows = rows
            .into_iter()
            .map(|(key, value)| TrackedStateDiffRow::from_tree_entry(key, value))
            .collect::<Vec<_>>();
        let row_refs = rows.iter().map(|row| (row, commit_id)).collect::<Vec<_>>();
        self.validate_diff_rows_for_commits_against_changelog(&row_refs)
            .await
    }

    async fn validate_commit_root_coverage(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
        rows: &[(TrackedStateKey, TrackedStateIndexValue)],
        metadata: &TrackedStateCommitRoot,
    ) -> Result<(), LixError> {
        let row_map = rows
            .iter()
            .map(|(key, value)| (tracked_state_identity_from_key(key), value))
            .collect::<HashMap<_, _>>();
        let mut cache = DiffCommitRootValidationCache::new();
        let winners = self
            .load_cached_commit_delta_winners(commit_id, &mut cache)
            .await?;
        let file_delete_cascades = self
            .load_file_delete_cascade_winners(&winners, &row_map)
            .await?;
        let collection_generation_cascades =
            collection_generation_cascade_winners(&winners, &row_map)?;
        for (identity, change_id) in &winners {
            if !tracked_state_identity_matches_tree_request(identity, request) {
                continue;
            }
            let Some(value) = row_map.get(identity) else {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' omits current changelog change '{change_id}' for identity {identity:?}"
                )));
            };
            if &value.change_id != change_id {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' stores change '{}' but changelog winner is '{}' for identity {:?}",
                    value.change_id, change_id, identity
                )));
            }
        }

        let Some(parent) = metadata.parent_roots.first() else {
            return Ok(());
        };
        let parent_rows = self
            .tree
            .scan(&self.store, &parent.root_id, request)
            .await?;
        for (parent_key, parent_value) in parent_rows {
            let identity = tracked_state_identity_from_key(&parent_key);
            if winners.contains_key(&identity) {
                continue;
            }
            let Some(value) = row_map.get(&identity) else {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' omits inherited identity {:?} from parent '{}'",
                    identity, parent.commit_id
                )));
            };
            if !parent_value.deleted
                && let Some(file_id) = parent_key.file_id.as_ref()
                && let Some(cascade_change_id) = file_delete_cascades.get(file_id)
            {
                if value.deleted && &value.change_id == cascade_change_id {
                    continue;
                }
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' does not apply file descriptor cascade change '{cascade_change_id}' to inherited identity {identity:?}"
                )));
            }
            if !parent_value.deleted
                && let Some(cascade_change_id) = collection_generation_cascades
                    .get(&(parent_key.schema_key.clone(), parent_key.file_id.clone()))
            {
                if value.deleted && &value.change_id == cascade_change_id {
                    continue;
                }
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' does not apply collection-generation cascade change '{cascade_change_id}' to inherited identity {identity:?}"
                )));
            }
            if *value != &parent_value {
                return Err(LixError::unknown(format!(
                    "tracked-state commit-root for commit '{commit_id}' does not preserve inherited identity {:?} from parent '{}'",
                    identity, parent.commit_id
                )));
            }
        }
        Ok(())
    }

    async fn load_file_delete_cascade_winners(
        &mut self,
        winners: &HashMap<TrackedStateIdentity, ChangeId>,
        row_map: &HashMap<TrackedStateIdentity, &TrackedStateIndexValue>,
    ) -> Result<HashMap<String, ChangeId>, LixError> {
        let mut candidates = winners
            .iter()
            .filter_map(|(identity, change_id)| {
                if identity.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY {
                    return None;
                }
                let value = row_map.get(identity)?;
                Some((
                    identity.row_pk.as_single_string_owned(),
                    *change_id,
                    value.commit_id,
                    TrackedStateKey {
                        schema_key: identity.schema_key.clone(),
                        file_id: identity.file_id.clone(),
                        row_pk: identity.row_pk.clone(),
                    },
                ))
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }
        candidates.sort_by_key(|(_, change_id, _, _)| *change_id);
        let mut changes = vec![None; candidates.len()];
        let mut by_owner = BTreeMap::<CommitId, Vec<(usize, TrackedStateKey)>>::new();
        for (index, (_, _, owner_commit_id, key)) in candidates.iter().enumerate() {
            by_owner
                .entry(*owner_commit_id)
                .or_default()
                .push((index, key.clone()));
        }
        for (owner_commit_id, requests) in by_owner {
            let keys = requests
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let loaded =
                storage::load_commit_delta_change_records(&self.store, owner_commit_id, &keys)
                    .await?;
            for ((index, _), change) in requests.into_iter().zip(loaded) {
                changes[index] = change;
            }
        }
        let mut cascades = HashMap::new();
        for ((file_id, change_id, _, _), change) in candidates.into_iter().zip(changes) {
            let file_id = file_id?;
            let Some(change) = change else {
                return Err(LixError::unknown(format!(
                    "file descriptor winner references missing packed change '{change_id}'"
                )));
            };
            if change.change_id != change_id {
                return Err(LixError::unknown(format!(
                    "file descriptor winner expects change '{change_id}' but packed authority stores '{}'",
                    change.change_id
                )));
            }
            if change.snapshot.is_none() {
                cascades.insert(file_id, change_id);
            }
        }
        Ok(cascades)
    }

    /// Batched payload-slot load for diff's cross-change equality fallback.
    pub(crate) async fn load_change_payloads(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<TrackedStatePayloadBatch, LixError> {
        let records =
            crate::changelog::load_change_records(&self.store, change_ids.iter().copied()).await?;
        TrackedStatePayloadBatch::from_payloads(
            records
                .into_iter()
                .map(|(change_id, record)| (change_id, record.snapshot, record.metadata)),
        )
    }

    /// Loads diff payloads by the physical commit and exact identity already
    /// carried by endpoint index rows.
    ///
    /// Packed tracked changes intentionally do not have one global storage key
    /// per change id. Routing through their owning commit avoids scanning every
    /// changelog and commit-delta segment for a sparse diff.
    async fn load_routed_diff_changes(
        &mut self,
        rows: &[&TrackedStateDiffRow],
    ) -> Result<HashMap<ChangeId, ChangeRecord>, LixError> {
        let mut by_commit = BTreeMap::<CommitId, Vec<&TrackedStateDiffRow>>::new();
        for row in rows.iter().copied() {
            by_commit.entry(row.commit_id).or_default().push(row);
        }
        let mut records = HashMap::<ChangeId, ChangeRecord>::new();
        for (commit_id, commit_rows) in by_commit {
            let keys = commit_rows
                .iter()
                .map(|row| TrackedStateKey {
                    schema_key: row.schema_key().to_owned(),
                    file_id: row.file_id().map(str::to_owned),
                    row_pk: row.row_pk().clone(),
                })
                .collect::<Vec<_>>();
            let mut loaded =
                storage::load_commit_delta_change_records(&self.store, commit_id, &keys).await?;
            let collection_fallback_rows = commit_rows
                .iter()
                .zip(&loaded)
                .filter_map(|(row, record)| {
                    (record.is_none() && row.deleted)
                        .then(|| collection_cascade_payload_key(row.schema_key(), row.file_id()))
                })
                .collect::<Vec<_>>();
            if !collection_fallback_rows.is_empty() {
                let fallbacks = storage::load_commit_delta_change_records(
                    &self.store,
                    commit_id,
                    &collection_fallback_rows,
                )
                .await?;
                let mut fallbacks = fallbacks.into_iter();
                for (row, record) in commit_rows.iter().zip(&mut loaded) {
                    if record.is_none() && row.deleted {
                        let fallback = fallbacks
                            .next()
                            .expect("one collection fallback was loaded per missing tombstone");
                        if fallback
                            .as_ref()
                            .is_some_and(|fallback| fallback.change_id == row.change_id)
                        {
                            *record = fallback;
                        }
                    }
                }
            }
            let fallback_rows = commit_rows
                .iter()
                .zip(&loaded)
                .filter_map(|(row, record)| {
                    (record.is_none() && row.deleted)
                        .then(|| row.file_id())
                        .flatten()
                        .map(cascade_payload_key)
                })
                .collect::<Vec<_>>();
            if !fallback_rows.is_empty() {
                let fallbacks = storage::load_commit_delta_change_records(
                    &self.store,
                    commit_id,
                    &fallback_rows,
                )
                .await?;
                let mut fallbacks = fallbacks.into_iter();
                for (row, record) in commit_rows.iter().zip(&mut loaded) {
                    if record.is_none() && row.deleted && row.file_id().is_some() {
                        *record = fallbacks
                            .next()
                            .expect("one fallback was loaded per missing file-scoped tombstone");
                    }
                }
            }
            let collection_fallback_rows = commit_rows
                .iter()
                .zip(&loaded)
                .filter_map(|(row, record)| {
                    (record.is_none() && row.deleted)
                        .then(|| collection_cascade_payload_key(row.schema_key(), row.file_id()))
                })
                .collect::<Vec<_>>();
            if !collection_fallback_rows.is_empty() {
                let fallbacks = storage::load_commit_delta_change_records(
                    &self.store,
                    commit_id,
                    &collection_fallback_rows,
                )
                .await?;
                let mut fallbacks = fallbacks.into_iter();
                for (row, record) in commit_rows.iter().zip(&mut loaded) {
                    if record.is_none() && row.deleted {
                        *record = fallbacks
                            .next()
                            .expect("one fallback was loaded per missing collection tombstone");
                    }
                }
            }
            for (row, record) in commit_rows.into_iter().zip(loaded) {
                let record = record.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked-state endpoint row '{}' has no authoritative payload in commit '{}'",
                            row.change_id, commit_id
                        ),
                    )
                })?;
                if record.change_id != row.change_id {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked-state endpoint row '{}' resolves to payload '{}'",
                            row.change_id, record.change_id
                        ),
                    ));
                }
                records.insert(record.change_id, record);
            }
        }
        for row in rows {
            if !row.deleted {
                validate_diff_row_against_changelog(row, &records)?;
            }
        }
        Ok(records)
    }

    async fn load_routed_tree_diff_changes(
        &mut self,
        rows: &[TrackedStateTreeDiffRowRef<'_>],
    ) -> Result<HashMap<ChangeId, ChangeRecord>, LixError> {
        let mut by_commit = BTreeMap::<CommitId, Vec<TrackedStateTreeDiffRowRef<'_>>>::new();
        for row in rows.iter().copied() {
            by_commit.entry(row.commit_id()).or_default().push(row);
        }
        let mut records = HashMap::<ChangeId, ChangeRecord>::new();
        for (commit_id, commit_rows) in by_commit {
            let keys = commit_rows
                .iter()
                .map(|row| TrackedStateKey {
                    schema_key: row.schema_key().to_owned(),
                    file_id: row.file_id().map(str::to_owned),
                    row_pk: row.row_pk().clone(),
                })
                .collect::<Vec<_>>();
            let mut loaded =
                storage::load_commit_delta_change_records(&self.store, commit_id, &keys).await?;
            let collection_fallback_rows = commit_rows
                .iter()
                .zip(&loaded)
                .filter_map(|(row, record)| {
                    (record.is_none() && row.deleted())
                        .then(|| collection_cascade_payload_key(row.schema_key(), row.file_id()))
                })
                .collect::<Vec<_>>();
            if !collection_fallback_rows.is_empty() {
                let fallbacks = storage::load_commit_delta_change_records(
                    &self.store,
                    commit_id,
                    &collection_fallback_rows,
                )
                .await?;
                let mut fallbacks = fallbacks.into_iter();
                for (row, record) in commit_rows.iter().zip(&mut loaded) {
                    if record.is_none() && row.deleted() {
                        let fallback = fallbacks
                            .next()
                            .expect("one collection fallback was loaded per missing tombstone");
                        if fallback
                            .as_ref()
                            .is_some_and(|fallback| fallback.change_id == row.change_id())
                        {
                            *record = fallback;
                        }
                    }
                }
            }
            let fallback_rows = commit_rows
                .iter()
                .zip(&loaded)
                .filter_map(|(row, record)| {
                    (record.is_none() && row.deleted())
                        .then(|| row.file_id())
                        .flatten()
                        .map(cascade_payload_key)
                })
                .collect::<Vec<_>>();
            if !fallback_rows.is_empty() {
                let fallbacks = storage::load_commit_delta_change_records(
                    &self.store,
                    commit_id,
                    &fallback_rows,
                )
                .await?;
                let mut fallbacks = fallbacks.into_iter();
                for (row, record) in commit_rows.iter().zip(&mut loaded) {
                    if record.is_none() && row.deleted() && row.file_id().is_some() {
                        *record = fallbacks
                            .next()
                            .expect("one fallback was loaded per missing file-scoped tombstone");
                    }
                }
            }
            for (row, record) in commit_rows.into_iter().zip(loaded) {
                let record = record.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked-state diff row '{}' has no authoritative payload in commit '{}'",
                            row.change_id(),
                            commit_id
                        ),
                    )
                })?;
                if let Some(existing) = records.insert(record.change_id, record.clone())
                    && existing != record
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "tracked-state diff change '{}' resolves to conflicting packed payloads",
                            row.change_id()
                        ),
                    ));
                }
            }
        }
        for row in rows.iter().copied() {
            validate_tree_diff_row_against_changelog(row, &records)?;
        }
        Ok(records)
    }

    pub(crate) async fn diff_semantic_tree_entries_at_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<TrackedStateTreeDiffBatch, LixError> {
        self.diff_semantic_tree_entries_at_commits_inner(left_commit_id, right_commit_id, request)
            .await
    }

    async fn diff_semantic_tree_entries_at_commits_inner(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<TrackedStateTreeDiffBatch, LixError> {
        let left_root = self.tree.load_root(&self.store, left_commit_id).await?;
        let right_root = if left_commit_id == right_commit_id {
            left_root.clone()
        } else {
            self.tree.load_root(&self.store, right_commit_id).await?
        };
        if left_root.is_none() {
            self.load_point_replay_commit(CommitId::parse_lix(
                left_commit_id,
                "tracked-state diff left commit_id",
            )?)
            .await?;
        }
        if right_root.is_none() && right_commit_id != left_commit_id {
            self.load_point_replay_commit(CommitId::parse_lix(
                right_commit_id,
                "tracked-state diff right commit_id",
            )?)
            .await?;
        }
        if let (Some(_), Some(_)) = (&left_root, &right_root) {
            return self
                .diff_tree_entries_from_roots(left_commit_id, right_commit_id, request)
                .await;
        }
        if let Some(entries) = self
            .diff_tree_entries_from_first_parent_interval(left_commit_id, right_commit_id, request)
            .await?
        {
            return Ok(entries);
        }
        if let Some(mut entries) = self
            .diff_tree_entries_from_first_parent_interval(right_commit_id, left_commit_id, request)
            .await?
        {
            entries.swap_sides();
            return Ok(entries);
        }
        if self
            .exact_current_state_scope_has_equal_hot_state(left_commit_id, right_commit_id, request)
            .await?
            == Some(true)
        {
            return Ok(TrackedStateTreeDiffBatch::default());
        }
        if left_commit_id == right_commit_id {
            return Ok(TrackedStateTreeDiffBatch::default());
        }
        let all_rows = TrackedStateTreeScanRequest {
            include_tombstones: true,
            ..TrackedStateTreeScanRequest::default()
        };
        let left = self
            .replay_index_batch_for_request_at_commit(left_commit_id, &all_rows)
            .await?;
        let right = self
            .replay_index_batch_for_request_at_commit(right_commit_id, &all_rows)
            .await?;
        let mut entries = TrackedStateTreeDiffBatchBuilder::with_row_capacity(
            left.len().saturating_add(right.len()),
        );
        let mut left_ordinal = 0usize;
        let mut right_ordinal = 0usize;
        while left_ordinal < left.len() || right_ordinal < right.len() {
            let ordering = match (left_ordinal < left.len(), right_ordinal < right.len()) {
                (true, true) => left
                    .encoded_key(left_ordinal)
                    .cmp(right.encoded_key(right_ordinal)),
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                (false, false) => break,
            };
            let (encoded_key, before, after) = match ordering {
                std::cmp::Ordering::Less => {
                    let ordinal = left_ordinal;
                    left_ordinal += 1;
                    (
                        left.encoded_key_owned(ordinal),
                        Some(left.value(ordinal).clone()),
                        None,
                    )
                }
                std::cmp::Ordering::Greater => {
                    let ordinal = right_ordinal;
                    right_ordinal += 1;
                    (
                        right.encoded_key_owned(ordinal),
                        None,
                        Some(right.value(ordinal).clone()),
                    )
                }
                std::cmp::Ordering::Equal => {
                    let left_value = left.value(left_ordinal).clone();
                    let right_value = right.value(right_ordinal).clone();
                    let encoded_key = left.encoded_key_owned(left_ordinal);
                    left_ordinal += 1;
                    right_ordinal += 1;
                    (encoded_key, Some(left_value), Some(right_value))
                }
            };
            if before == after {
                continue;
            }
            let key = decode_key_shared(encoded_key)?;
            if request.matches_key_ref(key.as_ref()) {
                entries.push_shared(key, before, after);
            }
        }
        entries.finish()
    }

    /// Proves equal live endpoint state from authenticated serving partitions.
    ///
    /// This proof belongs to semantic diff, which normalizes tombstones to
    /// absence. Raw tree-entry diff must retain tombstone rows for merge and
    /// history consumers and therefore never calls this helper.
    pub(crate) async fn exact_current_state_scope_has_equal_hot_state(
        &self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Option<bool>, LixError> {
        let Some(scope) = exact_current_state_scope(request) else {
            return Ok(None);
        };
        let left_commit_id = CommitId::parse_lix(
            left_commit_id,
            "tracked-state current-state diff left commit_id",
        )?;
        let right_commit_id = CommitId::parse_lix(
            right_commit_id,
            "tracked-state current-state diff right commit_id",
        )?;
        let Some(left_state) =
            storage::load_published_commit_state_topology(&self.store, left_commit_id).await?
        else {
            return Ok(None);
        };
        let Some(right_state) =
            storage::load_published_commit_state_topology(&self.store, right_commit_id).await?
        else {
            return Ok(None);
        };
        let Some(left_root) = left_state.current_state_scoped_ranges() else {
            return Ok(None);
        };
        let Some(right_root) = right_state.current_state_scoped_ranges() else {
            return Ok(None);
        };
        let prefix = super::current_state_envelope::current_state_scope_prefix(&scope)?;
        match super::scoped_range::prove_scoped_range_scope_equal(
            &self.store,
            &left_root.tree,
            &right_root.tree,
            &prefix,
        )
        .await?
        {
            super::scoped_range::ScopedRangeScopeEqualityProof::Equal => Ok(Some(true)),
            super::scoped_range::ScopedRangeScopeEqualityProof::NotProven => Ok(None),
        }
    }

    /// Returns the compact write-set union for an ancestor/descendant
    /// first-parent interval.
    ///
    /// Stale transaction admission needs to know whether a prepared identity
    /// was touched, not the endpoint payload or before value. Reading the
    /// immutable per-commit delta generations directly avoids tree diffing,
    /// ancestor point reads, and changelog payload hydration. `None` means the
    /// pair is not representable by the rootless first-parent journal and the
    /// caller must use the general commit diff.
    pub(crate) async fn changed_identities_in_first_parent_interval(
        &mut self,
        ancestor_commit_id: &str,
        descendant_commit_id: &str,
    ) -> Result<Option<Vec<TrackedStateDiffIdentity>>, LixError> {
        let Some(interval) = self
            .first_parent_interval_between(ancestor_commit_id, descendant_commit_id)
            .await?
        else {
            return Ok(None);
        };
        let mut batches = Vec::with_capacity(interval.len());
        for commit_id in interval {
            batches.push(
                self.scan_replayed_commit_delta_values(commit_id, &[])
                    .await?,
            );
        }
        let row_count = batches
            .iter()
            .try_fold(0usize, |count, batch| count.checked_add(batch.len()))
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "first-parent write-set row count overflows usize",
                )
            })?;
        let mut seen = HashSet::with_capacity(row_count);
        let mut keys = Vec::with_capacity(row_count);
        for batch in &batches {
            for row in batch.iter() {
                let key = row.key_ref();
                if seen.insert(key) {
                    keys.push(key);
                }
            }
        }
        keys.sort_unstable();
        Ok(Some(TrackedStateDiffIdentity::from_key_refs(
            keys.len(),
            |index| keys[index],
        )?))
    }

    /// Diffs an ancestor/descendant pair from immutable per-commit deltas.
    ///
    /// Merge always compares its merge base with each head. Walking only that
    /// first-parent interval makes the common branch case proportional to the
    /// commits and identities changed since the base, rather than every row
    /// inherited by both commits.
    async fn diff_tree_entries_from_first_parent_interval(
        &mut self,
        ancestor_commit_id: &str,
        descendant_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Option<TrackedStateTreeDiffBatch>, LixError> {
        let Some(interval) = self
            .first_parent_interval_between(ancestor_commit_id, descendant_commit_id)
            .await?
        else {
            return Ok(None);
        };

        // Decode each packed segment once, then flatten its encoded identities
        // into one interval-wide arena. The interval is ordered descendant ->
        // ancestor, so the first value observed for a key (or file cascade) is
        // its endpoint value.
        let scanned_schema_keys = schema_keys_with_file_descriptors(&request.schema_keys);
        let mut decoded_batches = Vec::with_capacity(interval.len());
        for commit_id in interval {
            decoded_batches.push(
                self.scan_replayed_commit_delta_values(commit_id, &scanned_schema_keys)
                    .await?,
            );
        }
        let mut row_count = 0usize;
        let mut encoded_bytes = 0usize;
        let mut cascade_capacity = 0usize;
        for batch in &decoded_batches {
            row_count = row_count.checked_add(batch.len()).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "first-parent tracked-state interval row count overflows usize",
                )
            })?;
            for row in batch.iter() {
                encoded_bytes = encoded_bytes
                    .checked_add(row.encoded_key_ref().len())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "first-parent tracked-state interval key bytes overflow usize",
                        )
                    })?;
                let key = row.key_ref();
                if key.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && row.value().deleted {
                    cascade_capacity += 1;
                }
            }
        }
        let mut interval_keys =
            TrackedStateKeyBatchBuilder::with_capacities(row_count, encoded_bytes);
        let mut interval_values = Vec::with_capacity(row_count);
        let mut commit_ranges = Vec::with_capacity(decoded_batches.len());
        for batch in &decoded_batches {
            let start = interval_values.len();
            for row in batch.iter() {
                interval_keys.push_encoded(row.encoded_key_ref());
                interval_values.push(row.value().clone());
            }
            commit_ranges.push(start..interval_values.len());
        }
        let interval_batch = FirstParentIntervalBatch {
            keys: interval_keys.finish_batch(),
            values: interval_values,
            commit_ranges,
            cascade_capacity,
        };
        drop(decoded_batches);

        let mut overlay =
            FirstParentDiffOverlay::with_capacities(row_count, interval_batch.cascade_capacity);
        for commit_range in &interval_batch.commit_ranges {
            // Apply explicit rows against cascades from newer commits first.
            // The cascade in this same commit is registered by the second pass,
            // which preserves "cascade, then explicit overwrite" semantics.
            for ordinal in commit_range.clone() {
                let encoded_key = interval_batch
                    .keys
                    .get_owned(ordinal)
                    .expect("first-parent interval key/value columns are aligned");
                let decoded_key = decode_key_shared(encoded_key)?;
                let key = decoded_key.as_ref();
                if !request.matches_key_ref(key) {
                    continue;
                }
                let value = &interval_batch.values[ordinal];
                let after = if value.deleted {
                    value.clone()
                } else if let Some(cascade) = key
                    .file_id
                    .and_then(|file_id| overlay.cascade_for_file_id(file_id))
                {
                    cascade_tombstone(cascade, value)
                } else {
                    value.clone()
                };
                overlay.insert_key_if_absent(
                    FirstParentDiffKeySource::Interval(u32::try_from(ordinal).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "first-parent tracked-state interval exceeds u32 ordinals",
                        )
                    })?),
                    interval_batch
                        .keys
                        .get(ordinal)
                        .expect("first-parent interval key ordinal is in bounds"),
                    after,
                    &interval_batch.keys,
                    None,
                )?;
            }
            for ordinal in commit_range.clone() {
                let value = &interval_batch.values[ordinal];
                if !value.deleted {
                    continue;
                }
                let encoded_key = interval_batch
                    .keys
                    .get_owned(ordinal)
                    .expect("first-parent interval key/value columns are aligned");
                let decoded_key = decode_key_shared(encoded_key)?;
                let key = decoded_key.as_ref();
                if key.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY {
                    overlay.insert_cascade_if_absent(key, value)?;
                }
            }
        }

        let mut inherited_keys = EncodedTrackedStateKeyBatch::default();
        if !overlay.cascades.is_empty() {
            let mut inherited_request = request.clone();
            inherited_request.include_tombstones = false;
            inherited_request.limit = None;
            let inherited = self
                .replay_index_batch_for_request_at_commit(ancestor_commit_id, &inherited_request)
                .await?;
            let mut key_builder = TrackedStateKeyBatchBuilder::with_row_capacity(inherited.len());
            let mut inherited_values = Vec::with_capacity(inherited.len());
            for ordinal in 0..inherited.len() {
                let key = decode_key_shared(inherited.encoded_key_owned(ordinal))?;
                let Some(cascade) = key
                    .file_id
                    .as_deref()
                    .and_then(|file_id| overlay.cascade_for_file_id(file_id))
                else {
                    continue;
                };
                key_builder.push(key.as_ref());
                inherited_values.push(cascade_tombstone(cascade, inherited.value(ordinal)));
            }
            inherited_keys = key_builder.finish_batch();
            for (ordinal, inherited_value) in inherited_values.into_iter().enumerate() {
                overlay.insert_key_if_absent(
                    FirstParentDiffKeySource::Inherited(u32::try_from(ordinal).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "first-parent inherited tracked-state batch exceeds u32 ordinals",
                        )
                    })?),
                    inherited_keys
                        .get(ordinal)
                        .expect("first-parent inherited key ordinal is in bounds"),
                    inherited_value,
                    &interval_batch.keys,
                    Some(&inherited_keys),
                )?;
            }
        }

        if overlay.entries.is_empty() {
            return Ok(Some(TrackedStateTreeDiffBatch::default()));
        }
        let inherited = (!inherited_keys.is_empty()).then_some(&inherited_keys);
        let (keys, after) = overlay.compact_sorted(&interval_batch.keys, inherited)?;
        let keys = keys.into_slices();
        let before = self
            .replay_index_values_for_encoded_keys_at_commit(ancestor_commit_id, &keys)
            .await?;
        let mut entries = TrackedStateTreeDiffBatchBuilder::with_row_capacity(keys.len());
        for ((encoded_key, after), before) in keys.into_iter().zip(after).zip(before) {
            if before.as_ref() != Some(&after) {
                entries.push_shared(decode_key_shared(encoded_key)?, before, Some(after));
            }
        }
        Ok(Some(entries.finish()?))
    }

    async fn first_parent_interval_between(
        &mut self,
        ancestor_commit_id: &str,
        descendant_commit_id: &str,
    ) -> Result<Option<Vec<CommitId>>, LixError> {
        let ancestor =
            CommitId::parse_lix(ancestor_commit_id, "tracked-state diff ancestor commit_id")?;
        let mut current = CommitId::parse_lix(
            descendant_commit_id,
            "tracked-state diff descendant commit_id",
        )?;
        let mut interval = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if current == ancestor {
                return Ok(Some(interval));
            }
            if !seen.insert(current) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot diff tracked_state commits: first-parent cycle includes '{current}'"
                    ),
                ));
            }
            interval.push(current);
            let replay_commit = self.load_point_replay_commit(current).await?;
            if !replay_commit.rootless {
                return Ok(None);
            }
            let Some(parent_commit_id) = replay_commit.parent_commit_id else {
                return Ok(None);
            };
            current = parent_commit_id;
        }
    }

    async fn diff_tree_entries_from_roots(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<TrackedStateTreeDiffBatch, LixError> {
        let mut cache = DiffCommitRootValidationCache::new();
        let left_root = self
            .load_validated_diff_root(left_commit_id, &mut cache)
            .await?;
        let right_root = if left_commit_id == right_commit_id {
            left_root.clone()
        } else {
            self.load_validated_diff_root(right_commit_id, &mut cache)
                .await?
        };
        self.tree
            .diff(&self.store, Some(&left_root), Some(&right_root), request)
            .await
    }

    async fn load_validated_diff_root(
        &mut self,
        commit_id: &str,
        cache: &mut DiffCommitRootValidationCache,
    ) -> Result<TrackedStateRootId, LixError> {
        let metadata = self
            .load_cached_commit_root_metadata(commit_id, cache)
            .await?;
        self.validate_commit_root_parent_matches_changelog(commit_id, &metadata, cache)
            .await?;
        Ok(metadata.root_id)
    }

    async fn index_entries_from_exact_or_durable_root(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
        durable_root: Option<&TrackedStateRootId>,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        if let Some(keys) = exact_keys_for_request(request) {
            let values = self
                .replay_index_values_for_keys_at_commit(commit_id, &keys)
                .await?;
            let mut entries = keys
                .into_iter()
                .zip(values)
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .filter(|(key, value)| request.matches(key, value))
                .collect::<Vec<_>>();
            if let Some(limit) = request.limit {
                entries.truncate(limit);
            }
            return Ok(entries);
        }
        if let Some(root_id) = durable_root {
            return self.tree.scan(&self.store, &root_id, request).await;
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "non-exact rootless scan bypassed the encoded replay batch",
        ))
    }

    /// Replays a rootless first-parent interval into one sorted encoded batch.
    ///
    /// Baseline and delta identities are copied once into a shared arena.
    /// Logical updates retain only flat ordinals and collision links; no
    /// `BTreeMap<TrackedStateKey, _>`, explicit owned-key set, or per-row key
    /// allocation survives between commits.
    async fn replay_index_batch_for_request_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<RootlessReplayBatch, LixError> {
        let interval = self.point_replay_interval(commit_id).await?;
        let commits = interval.commits();
        let scanned_schema_keys = schema_keys_with_file_descriptors(&request.schema_keys);
        let mut decoded_batches = Vec::with_capacity(commits.len());
        let mut delta_rows = 0usize;
        let mut encoded_key_bytes = 0usize;
        let mut cascade_capacity = 0usize;
        for replay_commit_id in commits {
            let batch = self
                .scan_replayed_commit_delta_values(*replay_commit_id, &scanned_schema_keys)
                .await?;
            delta_rows = delta_rows.checked_add(batch.len()).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "rootless tracked-state replay row count overflows usize",
                )
            })?;
            for row in batch.iter() {
                encoded_key_bytes = encoded_key_bytes
                    .checked_add(row.encoded_key_ref().len())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "rootless tracked-state replay key bytes overflow usize",
                        )
                    })?;
                let key = row.key_ref();
                if key.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && row.value().deleted {
                    cascade_capacity = cascade_capacity.saturating_add(1);
                }
            }
            decoded_batches.push(batch);
        }

        let baseline_request = TrackedStateTreeScanRequest {
            include_tombstones: true,
            limit: None,
            ..request.clone()
        };
        let baseline = if let Some(root_id) = interval.baseline_root.as_ref() {
            self.tree
                .scan(&self.store, root_id, &baseline_request)
                .await?
        } else {
            Vec::new()
        };
        let row_capacity = baseline.len().checked_add(delta_rows).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "rootless tracked-state replay capacity overflows usize",
            )
        })?;
        let baseline_key_capacity = baseline.len().saturating_mul(96);
        let mut overlay = RootlessReplayOverlay::with_capacities(
            row_capacity,
            encoded_key_bytes.saturating_add(baseline_key_capacity),
        );
        for (key, value) in baseline {
            overlay.insert_baseline(key, value)?;
        }

        let mut cascades = RootlessCascadeIndex::with_capacity(cascade_capacity);
        // The interval is head -> ancestor; logical replay applies oldest
        // first. Within one commit, cascades run before explicit upserts.
        for batch in decoded_batches.iter().rev() {
            cascades.clear();
            for row in batch.iter() {
                cascades.insert_descriptor(row.key_ref(), row.value())?;
            }
            overlay.apply_cascades(&cascades);
            for row in batch.iter() {
                let key = row.key_ref();
                if !request.matches_key_ref(key) {
                    continue;
                }
                overlay.upsert(row.encoded_key_ref(), key, row.value().clone())?;
            }
        }
        Ok(overlay.finish())
    }

    /// Resolves only the requested identities across rootless first-parent
    /// history. Commit-delta values are absolute index states, including their
    /// original `created_at`, so the first delta seen while walking backward is
    /// final for that identity. This stops as soon as all requested keys are
    /// resolved instead of reconstructing the full interval.
    async fn replay_index_values_for_keys_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let mut output = vec![None; keys.len()];
        let mut missing = BTreeMap::<TrackedStateKey, Vec<usize>>::new();
        for (index, key) in keys.iter().cloned().enumerate() {
            if let Some(value) = self
                .point_value_cache
                .get(&(commit_id.to_string(), key.clone()))
            {
                output[index] = value.clone();
            } else {
                missing.entry(key).or_default().push(index);
            }
        }
        if missing.is_empty() {
            return Ok(output);
        }
        let missing_keys = missing.keys().cloned().collect::<Vec<_>>();
        let values = self
            .resolve_rootless_index_values_at_commit(commit_id, &missing_keys)
            .await?;
        for (key, value) in missing_keys.into_iter().zip(values) {
            self.point_value_cache
                .insert((commit_id.to_string(), key.clone()), value.clone());
            for index in &missing[&key] {
                output[*index].clone_from(&value);
            }
        }
        Ok(output)
    }

    /// Batch-only first-parent replay over arena-backed encoded identities.
    ///
    /// Diff discovery already owns canonical encoded key slices. Reusing them
    /// for commit-delta and durable-tree point reads avoids allocating an
    /// owned schema/file pair per changed row solely for ancestor resolution.
    async fn replay_index_values_for_encoded_keys_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[Bytes],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let mut file_id_dictionary = EncodedReplayFileIdDictionary::with_capacity_hint(keys.len());
        let mut key_file_id_ordinals = Vec::with_capacity(keys.len());
        for encoded_key in keys {
            let decoded_key = decode_key_shared(encoded_key.clone())?;
            let file_id_ordinal = if let Some(file_id) = decoded_key.file_id {
                file_id_dictionary.intern(file_id)?
            } else {
                u32::MAX
            };
            key_file_id_ordinals.push(file_id_ordinal);
        }
        let file_ids = file_id_dictionary.into_file_ids();
        let mut file_id_order = (0..file_ids.len()).collect::<Vec<_>>();
        file_id_order.sort_unstable_by(|left, right| {
            file_ids[*left].as_str().cmp(file_ids[*right].as_str())
        });
        let mut canonical_ordinal_by_original = vec![0u32; file_ids.len()];
        let mut canonical_file_ids = Vec::with_capacity(file_ids.len());
        for original_ordinal in file_id_order {
            canonical_ordinal_by_original[original_ordinal] =
                u32::try_from(canonical_file_ids.len()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-state batch replay file dictionary exceeds u32",
                    )
                })?;
            canonical_file_ids.push(file_ids[original_ordinal].clone());
        }
        for ordinal in &mut key_file_id_ordinals {
            if *ordinal != u32::MAX {
                *ordinal = canonical_ordinal_by_original[*ordinal as usize];
            }
        }
        let file_ids = canonical_file_ids;

        // Encode each file descriptor key once for the whole replay. Every
        // commit selects shared slices from this arena instead of rebuilding
        // one key allocation per cascade probe.
        let mut descriptor_key_builder =
            TrackedStateKeyBatchBuilder::with_row_capacity(file_ids.len());
        for file_id in &file_ids {
            let row_pk = RowPk::uuid_from_canonical(file_id.as_str()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("validated file ID is not a canonical UUID: {error}"),
                )
            })?;
            descriptor_key_builder.push(TrackedStateKeyRef {
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
                file_id: Some(file_id.as_str()),
                row_pk: &row_pk,
            });
        }
        let descriptor_keys = descriptor_key_builder.finish();

        let interval = self.point_replay_interval(commit_id).await?;
        let mut values = if let Some(root_id) = interval.baseline_root.as_ref() {
            self.tree
                .get_many_encoded(&self.store, root_id, keys)
                .await?
        } else {
            vec![None; keys.len()]
        };
        let mut cascades = vec![None; file_ids.len()];
        for &current_commit_id in interval.commits().iter().rev() {
            let replay_commit = self.load_point_replay_commit(current_commit_id).await?;
            let deltas = storage::load_commit_delta_values_encoded_from_replay_manifest(
                &self.store,
                &replay_commit.state_manifest,
                keys,
                &self.commit_delta_point_cache,
            )
            .await?;
            let descriptor_deltas = storage::load_commit_delta_values_encoded_from_replay_manifest(
                &self.store,
                &replay_commit.state_manifest,
                &descriptor_keys,
                &self.commit_delta_point_cache,
            )
            .await?;
            for (file_id_ordinal, value) in descriptor_deltas.into_iter().enumerate() {
                if let Some(value) = value.filter(|value| value.deleted) {
                    cascades[file_id_ordinal] = Some(value);
                }
            }
            for (index, delta) in deltas.into_iter().enumerate() {
                let file_id_ordinal = key_file_id_ordinals[index];
                if file_id_ordinal != u32::MAX
                    && let Some(cascade) = cascades[file_id_ordinal as usize].as_ref()
                    && let Some(value) = values[index].as_ref().filter(|value| !value.deleted)
                {
                    values[index] = Some(cascade_tombstone(cascade, value));
                }
                let replacement_generation = match replay_commit.replacement_generation.as_ref() {
                    Some(generation) => {
                        let key = decode_key_shared(keys[index].clone())?;
                        replacement_scope_covers_key(&generation.scope, key.as_ref())
                            .then_some(generation)
                    }
                    None => None,
                };
                if let Some(mut delta) = delta {
                    if let Some(generation) = replacement_generation {
                        delta.created_at = generation.lifecycle_summary.uniform_created_at;
                    } else if let Some(previous) = values[index].as_ref() {
                        delta.created_at = previous.created_at;
                    }
                    values[index] = Some(delta);
                } else if replacement_generation.is_some() {
                    values[index] = None;
                }
            }
            for cascade in &mut cascades {
                *cascade = None;
            }
        }
        Ok(values)
    }

    async fn resolve_rootless_index_values_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let requested_commit_id =
            CommitId::parse_lix(commit_id, "rootless point replay commit id")?;
        let head = self.load_point_replay_commit(requested_commit_id).await?;
        let mut encoded = TrackedStateKeyBatchBuilder::with_row_capacity(keys.len());
        for key in keys {
            encoded.push(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            });
        }
        let encoded = encoded.finish();
        // The newest delta is the cheapest and most likely OLTP hit. Probe it
        // once before the persistent post-image so hot keys never pay a
        // catalog traversal, and retain the result for canonical replay.
        let head_values = self
            .load_replayed_commit_delta_values(requested_commit_id, keys)
            .await?;
        let cold_indices = head_values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.is_none().then_some(index))
            .collect::<Vec<_>>();
        if !cold_indices.is_empty() && cold_indices.len() != keys.len() {
            let cold_encoded = cold_indices
                .iter()
                .map(|&index| encoded[index].clone())
                .collect::<Vec<_>>();
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_crud_current_state_scoped_range_attempt();
            let cold_catalog = storage::load_complete_current_state_values_from_replay_manifest(
                &self.store,
                &head.state_manifest,
                &cold_encoded,
            )
            .await;
            #[cfg(feature = "storage-benches")]
            let cold_catalog_error = cold_catalog.is_err();
            if let Ok(Some(cold_values)) = cold_catalog {
                let hot_indices = head_values
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| value.is_some().then_some(index))
                    .collect::<Vec<_>>();
                let hot_keys = hot_indices
                    .iter()
                    .map(|&index| keys[index].clone())
                    .collect::<Vec<_>>();
                // Box the hot-only recursion so this async function keeps a
                // finite future type. The recursive call cannot re-enter this
                // mixed branch because every hot key matched the head delta.
                let hot_values =
                    Box::pin(self.resolve_rootless_index_values_at_commit(commit_id, &hot_keys))
                        .await?;
                let mut values = vec![None; keys.len()];
                for (index, value) in cold_indices.into_iter().zip(cold_values) {
                    values[index] = value;
                }
                for (index, value) in hot_indices.into_iter().zip(hot_values) {
                    values[index] = value;
                }
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_scoped_range_hit();
                return Ok(values);
            }
            #[cfg(feature = "storage-benches")]
            if cold_catalog_error {
                crate::storage_bench::record_crud_current_state_scoped_range_error();
            }
        }
        let catalog = if head_values.iter().all(Option::is_none) {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_crud_current_state_scoped_range_attempt();
            storage::load_complete_current_state_values_from_replay_manifest(
                &self.store,
                &head.state_manifest,
                &encoded,
            )
            .await
        } else {
            Ok(None)
        };
        match catalog {
            Ok(Some(values)) => {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_scoped_range_hit();
                return Ok(values);
            }
            Ok(None) => {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_scoped_range_fallback();
            }
            Err(_) => {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_scoped_range_error();
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_scoped_range_fallback();
            }
        }
        let interval = self.point_replay_interval(commit_id).await?;
        let mut values = if let Some(root_id) = interval.baseline_root.as_ref() {
            self.tree.get_many(&self.store, root_id, keys).await?
        } else {
            vec![None; keys.len()]
        };
        let descriptor_keys = keys
            .iter()
            .map(file_descriptor_key_for_file_scoped_key)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        // Root construction applies oldest mutations first and preserves the
        // first visible `created_at` on ordinary upserts. Certified replacement
        // generations instead carry their lifecycle timestamp explicitly.
        for &current_commit_id in interval.commits().iter().rev() {
            let replay_commit = self.load_point_replay_commit(current_commit_id).await?;
            let deltas = if current_commit_id == requested_commit_id {
                head_values.clone()
            } else {
                self.load_replayed_commit_delta_values(current_commit_id, keys)
                    .await?
            };
            let descriptor_deltas = self
                .load_replayed_commit_delta_values(current_commit_id, &descriptor_keys)
                .await?;
            let mut cascades = BTreeMap::new();
            for (key, value) in descriptor_keys.iter().zip(descriptor_deltas) {
                let Some(value) = value else {
                    continue;
                };
                if let Some(file_id) = file_delete_cascade(key, &value)? {
                    cascades.insert(file_id, value);
                }
            }
            for (index, key) in keys.iter().enumerate() {
                if let Some(cascade) = key
                    .file_id
                    .as_ref()
                    .and_then(|file_id| cascades.get(file_id))
                    && let Some(value) = values[index].as_ref().filter(|value| !value.deleted)
                {
                    values[index] = Some(cascade_tombstone(cascade, value));
                }
                let replacement_generation =
                    replay_commit
                        .replacement_generation
                        .as_ref()
                        .filter(|generation| {
                            replacement_scope_covers_key(
                                &generation.scope,
                                TrackedStateKeyRef {
                                    schema_key: &key.schema_key,
                                    file_id: key.file_id.as_deref(),
                                    row_pk: &key.row_pk,
                                },
                            )
                        });
                if let Some(mut delta) = deltas[index].clone() {
                    if let Some(generation) = replacement_generation {
                        delta.created_at = generation.lifecycle_summary.uniform_created_at;
                    } else if let Some(previous) = values[index].as_ref() {
                        delta.created_at = previous.created_at;
                    }
                    values[index] = Some(delta);
                } else if replacement_generation.is_some() {
                    values[index] = None;
                }
            }
        }
        Ok(values)
    }

    async fn load_point_replay_commit(
        &mut self,
        commit_id: CommitId,
    ) -> Result<PointReplayCommit, LixError> {
        if let Some(cached) = self.point_replay_commits.get(&commit_id) {
            return Ok(cached.clone());
        }
        let (record, manifest) =
            storage::load_commit_record_and_point_replay_state(&self.store, commit_id).await?;
        let record = record.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("cannot point-replay tracked_state for unknown commit '{commit_id}'"),
            )
        })?;
        let manifest = match manifest {
            Some(manifest) => manifest,
            None => {
                return Err(
                    storage::missing_commit_state_manifest_error(&self.store, commit_id).await,
                );
            }
        };
        let rootless = manifest.replay_debt.depth > 0;
        let root_id = manifest
            .snapshot_root
            .as_ref()
            .map(|root| root.root_id.clone());
        let replacement_generation = if rootless && manifest.mutations.member_count != 0 {
            storage::seed_commit_delta_point_cache_from_replay_manifest(
                &manifest,
                &self.commit_delta_point_cache,
            )?
            .replacement_generation
        } else {
            None
        };
        let replay_commit = PointReplayCommit {
            parent_commit_id: record.parent_commit_ids.first().copied(),
            root_id,
            rootless,
            replacement_generation,
            state_manifest: manifest,
        };
        self.point_replay_commits
            .insert(commit_id, replay_commit.clone());
        Ok(replay_commit)
    }

    /// Loads a commit's deltas for point history replay. The cache is keyed by
    /// the physical commit and identity, so overlapping observed revisions
    /// share exact storage reads without widening them into a schema scan.
    async fn load_replayed_commit_delta_values(
        &mut self,
        commit_id: CommitId,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let mut output = vec![None; keys.len()];
        let mut missing = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            if let Some(value) = self.commit_delta_value_cache.get(&(commit_id, key.clone())) {
                output[index] = value.clone();
            } else {
                missing.push((index, key.clone()));
            }
        }
        if missing.is_empty() {
            return Ok(output);
        }
        missing.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        let mut encoded_keys = TrackedStateKeyBatchBuilder::with_row_capacity(missing.len());
        let mut query_ordinals = Vec::with_capacity(missing.len());
        let mut previous_key = None::<&TrackedStateKey>;
        let mut unique_key_count = 0usize;
        for (_, key) in &missing {
            if previous_key != Some(key) {
                encoded_keys.push(TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    row_pk: &key.row_pk,
                });
                previous_key = Some(key);
                unique_key_count += 1;
            }
            query_ordinals.push(unique_key_count - 1);
        }
        let replay_commit = self.load_point_replay_commit(commit_id).await?;
        let values = storage::load_commit_delta_values_encoded_from_replay_manifest(
            &self.store,
            &replay_commit.state_manifest,
            &encoded_keys.finish(),
            &self.commit_delta_point_cache,
        )
        .await?;
        for ((index, key), query_ordinal) in missing.into_iter().zip(query_ordinals) {
            let value = values[query_ordinal].clone();
            self.commit_delta_value_cache
                .insert((commit_id, key), value.clone());
            output[index] = value;
        }
        Ok(output)
    }

    /// Scans one commit's packed deltas into an arena-backed batch.
    ///
    /// Scan discovery intentionally does not populate the row-owned point
    /// cache. First-parent diff retains encoded arena slices for its ancestor
    /// replay, while actual point callers continue to use the identity cache.
    async fn scan_replayed_commit_delta_values(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<storage::DecodedCommitDeltaBatch, LixError> {
        let replay_commit = self.load_point_replay_commit(commit_id).await?;
        let source_manifest = match replay_commit
            .state_manifest
            .mutations
            .selected_source_commit_id()
        {
            Some(source_commit_id) => Some(
                self.load_point_replay_commit(source_commit_id)
                    .await?
                    .state_manifest,
            ),
            None => None,
        };
        storage::scan_commit_delta_values_from_authenticated_states(
            &self.store,
            &replay_commit.state_manifest,
            source_manifest.as_deref(),
            schema_keys,
        )
        .await
    }

    async fn point_replay_interval(
        &mut self,
        commit_id: &str,
    ) -> Result<PointReplayInterval, LixError> {
        let requested_commit_id =
            CommitId::parse_lix(commit_id, "tracked-state point replay commit_id")?;
        if let Some(interval) = self.point_replay_intervals.get(&requested_commit_id) {
            return Ok(interval.clone());
        }

        let mut commits = Vec::new();
        let mut current_commit_id = requested_commit_id;
        let mut seen_commit_ids = HashSet::new();
        let baseline_root = loop {
            if !seen_commit_ids.insert(current_commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot point-replay tracked_state commit '{commit_id}': first-parent cycle includes commit '{current_commit_id}'"
                    ),
                ));
            }
            let replay_commit = self.load_point_replay_commit(current_commit_id).await?;
            if !replay_commit.rootless
                && let Some(root_id) = replay_commit.root_id
            {
                break Some(root_id);
            }
            commits.push(current_commit_id);
            let replay_parent_commit_id = match replay_commit.replacement_generation.as_ref() {
                Some(generation) => generation.fallback_commit_id,
                None => replay_commit.parent_commit_id,
            };
            let Some(parent_commit_id) = replay_parent_commit_id else {
                break None;
            };
            if let Some(tail) = self.point_replay_intervals.get(&parent_commit_id) {
                commits.extend_from_slice(tail.commits());
                break tail.baseline_root.clone();
            }
            current_commit_id = parent_commit_id;
        };
        let interval = PointReplayInterval::new(commits, baseline_root);
        interval.cache_suffixes(&mut self.point_replay_intervals);
        self.point_replay_intervals
            .entry(requested_commit_id)
            .or_insert_with(|| interval.clone());
        Ok(interval)
    }

    /// Resolves one caller-deduplicated encoded key batch without populating
    /// the row-owned point cache.
    ///
    /// Exact historical materialization already retains one compact borrowed
    /// key-reference column for its output batch. Reusing one encoded arena for
    /// both durable-tree and first-parent replay avoids cloning those keys into
    /// a row-owned dedup map solely to prove uniqueness again.
    async fn commit_root_values_for_unique_encoded_keys(
        &mut self,
        commit_id: &str,
        encoded_keys: &[Bytes],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if let Some(root_id) = self.tree.load_root(&self.store, commit_id).await? {
            return self
                .tree
                .get_many_encoded(&self.store, &root_id, encoded_keys)
                .await;
        }
        self.replay_index_values_for_encoded_keys_at_commit(commit_id, encoded_keys)
            .await
    }

    async fn commit_root_values_for_keys(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if let Some(root_id) = self.tree.load_root(&self.store, commit_id).await? {
            return self.tree.get_many(&self.store, &root_id, keys).await;
        }
        self.replay_index_values_for_keys_at_commit(commit_id, keys)
            .await
    }

    /// Plans a three-way merge by diffing both heads against the same base.
    ///
    /// `target_commit_id` is the destination root that should keep its own
    /// changes. `source_commit_id` is the incoming root whose non-conflicting
    /// changes should be applied.
    #[cfg(test)]
    pub(crate) async fn plan_merge(
        &mut self,
        base_commit_id: &str,
        target_commit_id: &str,
        source_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateMergePlan, LixError> {
        let target_diff = self
            .diff_commits(base_commit_id, target_commit_id, request)
            .await?;
        let source_diff = self
            .diff_commits(base_commit_id, source_commit_id, request)
            .await?;
        let fallback_ids = merge::merge_payload_fallback_ids(&target_diff, &source_diff)?;
        let payloads = self.load_change_payloads(&fallback_ids).await?;
        merge::plan_merge(&target_diff, &source_diff, &payloads)
    }
}

fn exact_current_state_scope(
    request: &TrackedStateTreeScanRequest,
) -> Option<storage::CommitDeltaReplacementScope> {
    let [schema_key] = request.schema_keys.as_slice() else {
        return None;
    };
    let [file_id] = request.file_ids.as_slice() else {
        return None;
    };
    let file_id = match file_id {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
        NullableKeyFilter::Any => return None,
    };
    Some(storage::CommitDeltaReplacementScope {
        schema_key: schema_key.clone(),
        file_id,
    })
}

/// Writer for changelog-backed tracked-state commit roots.
pub(crate) struct TrackedStateWriter<'a, S: ?Sized> {
    chunk_overlay: storage::TrackedStateChunkOverlay,
    staged_roots: BTreeMap<String, TrackedStateCommitRoot>,
    transient_chunk_hashes: HashSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]>,
    tree: TrackedStateTree,
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

#[derive(Debug)]
pub(crate) struct TrackedStateTransientRebuildState {
    chunk_overlay: storage::TrackedStateChunkOverlay,
    staged_roots: BTreeMap<String, TrackedStateCommitRoot>,
    transient_chunk_hashes: HashSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]>,
}

impl Default for TrackedStateTransientRebuildState {
    /// Rebuild state exists only for the explicit repair path, so its overlay
    /// rewrites durable chunks instead of trusting an addressed digest.
    fn default() -> Self {
        Self {
            chunk_overlay: storage::TrackedStateChunkOverlay::repairing(),
            staged_roots: BTreeMap::new(),
            transient_chunk_hashes: HashSet::new(),
        }
    }
}

impl TrackedStateTransientRebuildState {
    pub(crate) fn chunk_hashes(
        &self,
    ) -> HashSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]> {
        self.chunk_overlay.chunk_hashes().collect()
    }

    pub(crate) fn mark_new_chunks_transient(
        &mut self,
        previously_known: &HashSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]>,
    ) {
        self.transient_chunk_hashes.extend(
            self.chunk_overlay
                .chunk_hashes()
                .filter(|hash| !previously_known.contains(hash)),
        );
    }
}

/// Explicit commit-root rebuilder created by `TrackedStateContext`.
pub(crate) struct TrackedStateRootRebuilder<'a, S: ?Sized> {
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
}

impl<S> TrackedStateRootRebuilder<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn rebuild_commit_root_at(
        &mut self,
        commit_id: &str,
    ) -> Result<TrackedStateWriteReport, LixError> {
        crate::tracked_state::commit_root_rebuild::rebuild_commit_root_at(self, commit_id).await
    }
}

impl<S> TrackedStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    #[cold]
    #[inline(never)]
    async fn file_cascade_schema_keys(
        &self,
        staged_read: &storage::TrackedStateStagedRead<'_, S>,
        base_root: &TrackedStateRootId,
    ) -> Result<Vec<String>, LixError> {
        // A file-only predicate cannot bound this schema-first tree. Enumerate
        // the actual authenticated schema runs in the same parent root, then
        // issue canonical `(schema_key, file_id)` ranges. This includes
        // private/unregistered rows without trusting a second catalog.
        self.tree.distinct_schema_keys(staged_read, base_root).await
    }

    #[cold]
    #[inline(never)]
    async fn file_cascade_mutations<'a>(
        &self,
        staged_read: &storage::TrackedStateStagedRead<'_, S>,
        base_root: &TrackedStateRootId,
        deltas: &[TrackedStateDeltaRef<'a>],
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, LixError> {
        let mut cascades = BTreeMap::<String, &TrackedStateDeltaRef<'_>>::new();
        for delta in deltas {
            if delta.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !delta.deleted {
                continue;
            }
            let file_id = delta.row_pk.as_single_string_owned().map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("file descriptor tombstone has invalid identity: {error}"),
                )
            })?;
            cascades.insert(file_id, delta);
        }
        let explicit_keys = deltas
            .iter()
            .map(|delta| TrackedStateKey {
                schema_key: delta.schema_key.to_string(),
                file_id: delta.file_id.map(str::to_string),
                row_pk: delta.row_pk.clone(),
            })
            .collect::<BTreeSet<_>>();
        let rows = self
            .tree
            .scan(
                staged_read,
                base_root,
                &TrackedStateTreeScanRequest {
                    schema_keys: self
                        .file_cascade_schema_keys(staged_read, base_root)
                        .await?,
                    file_ids: cascades
                        .keys()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    include_tombstones: false,
                    ..TrackedStateTreeScanRequest::default()
                },
            )
            .await?;
        let mut mutations = BTreeMap::new();
        for (key, value) in rows {
            if explicit_keys.contains(&key) {
                continue;
            }
            let cascade = cascades
                .get(
                    key.file_id
                        .as_deref()
                        .expect("file-filtered tracked row requires file id"),
                )
                .expect("tracked scan only returns requested cascade ids");
            mutations.insert(
                encode_key(&key),
                encode_value_ref(TrackedStateIndexValueRef {
                    change_id: cascade.change_id,
                    commit_id: cascade.commit_id,
                    deleted: true,
                    created_at: value.created_at(),
                    updated_at: cascade.updated_at,
                }),
            );
        }
        Ok(mutations)
    }

    pub(crate) fn into_transient_rebuild_state(self) -> TrackedStateTransientRebuildState {
        TrackedStateTransientRebuildState {
            chunk_overlay: self.chunk_overlay,
            staged_roots: self.staged_roots,
            transient_chunk_hashes: self.transient_chunk_hashes,
        }
    }

    pub(crate) async fn promote_reachable_transient_chunks(
        &mut self,
        root_id: &TrackedStateRootId,
    ) -> Result<(), LixError> {
        if self.transient_chunk_hashes.is_empty() {
            return Ok(());
        }
        let reachable = self
            .tree
            .reachable_chunk_hashes_with_overlay(self.store, &self.chunk_overlay, root_id)
            .await?;
        let promoted = self
            .transient_chunk_hashes
            .intersection(&reachable)
            .copied()
            .collect::<Vec<_>>();
        self.chunk_overlay
            .stage_selected_chunks(self.store, self.writes, promoted.iter().copied())
            .await?;
        for hash in promoted {
            self.transient_chunk_hashes.remove(&hash);
        }
        Ok(())
    }

    pub(crate) fn staged_commit_roots(&self) -> impl Iterator<Item = &TrackedStateCommitRoot> {
        self.staged_roots.values()
    }

    /// Reads exact identities from a root staged in this write set, or from an
    /// already-published root in the coherent base snapshot.
    pub(crate) async fn root_values_at_commit(
        &self,
        commit_id: CommitId,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        let root = match self.staged_roots.get(&commit_id.to_string()) {
            Some(root) => root.clone(),
            None => storage::load_snapshot_commit_root(self.store, &commit_id.to_string())
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_COMMIT_NOT_FOUND,
                        format!("commit '{commit_id}' has no tracked-state root"),
                    )
                })?,
        };
        let staged_read = storage::TrackedStateStagedRead::new(self.store, &self.chunk_overlay);
        self.tree.get_many(&staged_read, &root.root_id, keys).await
    }

    pub(crate) async fn validate_staged_commit_root_against_changelog(
        &self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let typed_commit_id =
            CommitId::parse_lix(commit_id, "staged commit-root validation commit_id")?;
        let metadata = self
            .staged_roots
            .values()
            .find(|metadata| metadata.commit_id == typed_commit_id)
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked-state staged root for commit '{commit_id}' is missing"),
                )
            })?;
        let mut staged_commit_states = Vec::with_capacity(self.staged_roots.len());
        for metadata in self.staged_roots.values() {
            let manifest = storage::load_commit_state_manifest(self.store, metadata.commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "cannot audit rebuilt tracked_state root for commit '{}' without its commit-state manifest",
                            metadata.commit_id
                        ),
                    )
                })?;
            staged_commit_states.push((manifest, metadata.clone()));
        }
        let read = storage::TrackedStateStagedRead::with_commit_state_roots(
            self.store,
            &self.chunk_overlay,
            staged_commit_states,
        )?;
        TrackedStateContext::new()
            .reader(read)
            .validate_commit_root_metadata_against_changelog(commit_id, metadata)
            .await
    }

    pub(crate) async fn stage_commit_root<'a, I>(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        deltas: I,
    ) -> Result<TrackedStateWriteReport, LixError>
    where
        I: IntoIterator<Item = TrackedStateDeltaRef<'a>>,
    {
        self.stage_commit_root_with_absence_guards(
            commit_id,
            parent_commit_id,
            deltas,
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
    }

    /// Publishes a complete-state fence by sharing an already materialized
    /// persistent tree root. Only the small root descriptor is new; no state
    /// rows or tree chunks are visited or copied.
    pub(crate) async fn stage_complete_state_alias(
        &mut self,
        commit_id: CommitId,
        source_commit_id: CommitId,
    ) -> Result<TrackedStateWriteReport, LixError> {
        let source_key = source_commit_id.to_string();
        let source = match self.staged_roots.get(&source_key) {
            Some(root) => root.clone(),
            None => storage::load_snapshot_commit_root(self.store, &source_key)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "checkpoint source '{source_commit_id}' has no materialized tracked-state root"
                        ),
                    )
                })?,
        };
        let root_id = source.root_id;
        self.staged_roots.insert(
            commit_id.to_string(),
            TrackedStateCommitRoot {
                commit_id,
                root_id: root_id.clone(),
                parent_roots: vec![TrackedStateCommitRootParent {
                    commit_id: source_commit_id,
                    root_id: root_id.clone(),
                }],
                changed_key_count: source.row_count_estimate,
                row_count_estimate: source.row_count_estimate,
                tree_height: source.tree_height,
                complete_state_fence: true,
            },
        );
        Ok(TrackedStateWriteReport {
            commit_id,
            root_id,
            changed_rows: 0,
            primary_chunk_puts: 0,
        })
    }

    pub(crate) async fn stage_commit_root_with_absence_guards<'a, I>(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        deltas: I,
        absence_guards: &BTreeSet<TrackedStateKey>,
        certified_replacement_markers: &BTreeSet<TrackedStateKey>,
    ) -> Result<TrackedStateWriteReport, LixError>
    where
        I: IntoIterator<Item = TrackedStateDeltaRef<'a>>,
    {
        let deltas = deltas.into_iter().collect::<Vec<_>>();
        let typed_commit_id =
            CommitId::parse_lix(commit_id, "tracked-state commit root commit_id")?;
        let typed_parent_commit_id = parent_commit_id
            .map(|id| CommitId::parse_lix(id, "tracked-state parent commit_id"))
            .transpose()?;
        let parent_metadata = match parent_commit_id {
            Some(parent_commit_id) => {
                let metadata = match self.staged_roots.get(parent_commit_id) {
                    Some(metadata) => Some(metadata.clone()),
                    None => {
                        storage::load_snapshot_commit_root(self.store, parent_commit_id).await?
                    }
                };
                let Some(metadata) = metadata else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "tracked-state parent root for commit '{parent_commit_id}' is missing"
                        ),
                    ));
                };
                Some(metadata)
            }
            None => None,
        };
        let base_root = parent_metadata
            .as_ref()
            .map(|metadata| metadata.root_id.clone());
        if deltas.is_empty()
            && let Some(parent_metadata) = parent_metadata.as_ref()
        {
            let root_id = parent_metadata.root_id.clone();
            let metadata = TrackedStateCommitRoot {
                commit_id: typed_commit_id,
                root_id: root_id.clone(),
                parent_roots: vec![TrackedStateCommitRootParent {
                    commit_id: typed_parent_commit_id.expect("parent metadata requires parent id"),
                    root_id: root_id.clone(),
                }],
                changed_key_count: 0,
                row_count_estimate: parent_metadata.row_count_estimate,
                tree_height: parent_metadata.tree_height,
                complete_state_fence: false,
            };
            self.staged_roots.insert(commit_id.to_string(), metadata);
            return Ok(TrackedStateWriteReport {
                commit_id: typed_commit_id,
                root_id,
                changed_rows: 0,
                primary_chunk_puts: 0,
            });
        }
        let mut cascade_mutations = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        if let Some(base_root) = base_root.as_ref() {
            let staged_read = storage::TrackedStateStagedRead::new(self.store, &self.chunk_overlay);
            if deltas
                .iter()
                .any(|delta| delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && delta.deleted)
            {
                cascade_mutations = self
                    .file_cascade_mutations(&staged_read, base_root, &deltas)
                    .await?;
            }
            let explicit_keys = (!certified_replacement_markers.is_empty()).then(|| {
                deltas
                    .iter()
                    .map(|delta| TrackedStateKeyRef {
                        schema_key: delta.schema_key,
                        file_id: delta.file_id,
                        row_pk: delta.row_pk,
                    })
                    .collect::<BTreeSet<_>>()
            });
            for marker in deltas.iter().filter(|delta| {
                !delta.deleted
                    && delta.schema_key
                        == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    && certified_replacement_markers.contains(&TrackedStateKey {
                        schema_key: delta.schema_key.to_owned(),
                        file_id: delta.file_id.map(str::to_owned),
                        row_pk: delta.row_pk.clone(),
                    })
            }) {
                let (schema_key, file_id) =
                    crate::collection_generation::collection_scope_from_row_pk(marker.row_pk)?;
                let rows = self
                    .tree
                    .scan(
                        &staged_read,
                        base_root,
                        &TrackedStateTreeScanRequest {
                            schema_keys: vec![schema_key],
                            file_ids: file_id.into_iter().map(NullableKeyFilter::Value).collect(),
                            include_tombstones: false,
                            ..TrackedStateTreeScanRequest::default()
                        },
                    )
                    .await?;
                for (key, value) in rows {
                    let key_ref = TrackedStateKeyRef {
                        schema_key: &key.schema_key,
                        file_id: key.file_id.as_deref(),
                        row_pk: &key.row_pk,
                    };
                    if explicit_keys
                        .as_ref()
                        .expect("certified marker requires explicit-key inventory")
                        .contains(&key_ref)
                    {
                        continue;
                    }
                    cascade_mutations.insert(
                        encode_key(&key),
                        encode_value_ref(TrackedStateIndexValueRef {
                            change_id: marker.change_id,
                            commit_id: marker.commit_id,
                            deleted: true,
                            created_at: value.created_at(),
                            updated_at: marker.updated_at,
                        }),
                    );
                }
            }
        }
        let mut parent_values = if let Some(base_root) = base_root.as_ref() {
            let keys = deltas
                .iter()
                .map(|delta| TrackedStateKeyRef {
                    schema_key: delta.schema_key,
                    file_id: delta.file_id,
                    row_pk: delta.row_pk,
                })
                .collect::<Vec<_>>();
            // A root fence can reconstruct several skipped ordinary commits
            // into this same write set. Read through both staged root metadata
            // and the chunk overlay so a child can preserve `created_at` from
            // an ancestor whose chunks have not reached storage yet.
            let staged_read = storage::TrackedStateStagedRead::new(self.store, &self.chunk_overlay);
            self.tree
                .get_many_refs(&staged_read, base_root, &keys)
                .await?
        } else {
            vec![None; deltas.len()]
        };
        if let Some(base_root) = base_root.as_ref() {
            use crate::collection_generation::{
                COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
            };

            let mut marker_keys = Vec::new();
            let mut marker_ordinals = BTreeMap::<(&str, Option<&str>), usize>::new();
            for delta in &deltas {
                if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
                    continue;
                }
                for scope in [
                    Some((delta.schema_key, None)),
                    delta
                        .file_id
                        .map(|file_id| (delta.schema_key, Some(file_id))),
                ]
                .into_iter()
                .flatten()
                {
                    if marker_ordinals.contains_key(&scope) {
                        continue;
                    }
                    let ordinal = marker_keys.len();
                    marker_keys.push(TrackedStateKey {
                        schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_string(),
                        file_id: None,
                        row_pk: RowPk::single(collection_scope_key(CollectionScopeRef {
                            schema_key: scope.0,
                            file_id: scope.1,
                        })),
                    });
                    marker_ordinals.insert(scope, ordinal);
                }
            }
            if !marker_keys.is_empty() {
                let staged_read =
                    storage::TrackedStateStagedRead::new(self.store, &self.chunk_overlay);
                let marker_values = self
                    .tree
                    .get_many(&staged_read, base_root, &marker_keys)
                    .await?;
                for (delta, parent_value) in deltas.iter().zip(parent_values.iter_mut()) {
                    let Some(value) = parent_value.as_ref() else {
                        continue;
                    };
                    if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
                        continue;
                    }
                    let retired = [
                        Some((delta.schema_key, None)),
                        delta
                            .file_id
                            .map(|file_id| (delta.schema_key, Some(file_id))),
                    ]
                    .into_iter()
                    .flatten()
                    .filter_map(|scope| marker_ordinals.get(&scope).copied())
                    .filter_map(|ordinal| marker_values[ordinal].as_ref())
                    .any(|marker| value.commit_id <= marker.commit_id);
                    if retired {
                        *parent_value = None;
                    }
                }
            }
        }
        let mut mutation_batch = TrackedStateMutationBatchBuilder::with_row_capacity(
            cascade_mutations.len().saturating_add(deltas.len()),
        );
        for (key, value) in cascade_mutations {
            mutation_batch.push_encoded(&key, &value);
        }
        for (delta, parent_value) in deltas.iter().zip(parent_values.iter()) {
            let key = TrackedStateKey {
                schema_key: delta.schema_key.to_string(),
                file_id: delta.file_id.map(str::to_string),
                row_pk: delta.row_pk.clone(),
            };
            if parent_value.as_ref().is_some_and(|value| !value.deleted())
                && absence_guards.contains(&key)
            {
                let row_pk = key
                    .row_pk
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid row_pk>".to_string());
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "primary-key constraint violation on schema '{}': INSERT would duplicate row_pk '{row_pk}'",
                        key.schema_key
                    ),
                ));
            }
            let parent_created_at = parent_value.as_ref().map(|value| value.created_at());
            let created_at = parent_created_at.unwrap_or(delta.created_at);
            let key = TrackedStateKeyRef {
                schema_key: delta.schema_key,
                file_id: delta.file_id,
                row_pk: delta.row_pk,
            };
            let value = TrackedStateIndexValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                deleted: delta.deleted,

                created_at,
                updated_at: delta.updated_at,
            };
            mutation_batch.push(key, value);
        }
        let mutations = mutation_batch.finish();
        let changed_rows = mutations.len();
        let result = self
            .tree
            .apply_mutations_with_overlay(
                self.store,
                self.writes,
                &mut self.chunk_overlay,
                base_root.as_ref(),
                mutations,
                Some(commit_id),
            )
            .await?;
        let metadata = TrackedStateCommitRoot {
            commit_id: typed_commit_id,
            root_id: result.root_id.clone(),
            parent_roots: typed_parent_commit_id
                .zip(base_root.as_ref())
                .map(|(parent_commit_id, root_id)| {
                    vec![TrackedStateCommitRootParent {
                        commit_id: parent_commit_id,
                        root_id: root_id.clone(),
                    }]
                })
                .unwrap_or_default(),
            changed_key_count: u64::try_from(changed_rows).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root changed key count exceeds u64",
                )
            })?,
            row_count_estimate: u64::try_from(result.row_count).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root row count exceeds u64",
                )
            })?,
            tree_height: u32::try_from(result.tree_height).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root tree height exceeds u32",
                )
            })?,
            complete_state_fence: false,
        };
        self.staged_roots.insert(commit_id.to_string(), metadata);

        Ok(TrackedStateWriteReport {
            commit_id: typed_commit_id,
            root_id: result.root_id,
            changed_rows,
            primary_chunk_puts: result.chunk_count,
        })
    }

    /// Attempts the arena-backed ordered root paths used by normal bulk
    /// tracked commits.
    ///
    /// Parentless batches build directly from the borrowed deltas. Append-only
    /// batches keep the existing chunk-reuse patcher, but bypass the generic
    /// point-read and owned-key preparation. Dense overlapping batches stream
    /// through the parent merge. Sparse, unordered, and cascade-bearing sparse
    /// callers stay on the generic path, which preserves latest-write-wins and
    /// file-delete cascade behavior.
    pub(crate) async fn try_stage_bulk_parent_root_from_ordered_mutations<'a, I>(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        mutation_count: usize,
        first_mutation_key: &[u8],
        file_delete_cascades: &BTreeMap<String, TrackedStateDeltaRef<'a>>,
        mutations: I,
    ) -> Result<Option<TrackedStateWriteReport>, LixError>
    where
        I: IntoIterator<Item = Result<TrackedStateRootMutationRef<'a>, LixError>>,
    {
        if mutation_count < 2 {
            return Ok(None);
        }
        let typed_commit_id =
            CommitId::parse_lix(commit_id, "tracked-state commit root commit_id")?;
        let typed_parent_commit_id = parent_commit_id
            .map(|id| CommitId::parse_lix(id, "tracked-state parent commit_id"))
            .transpose()?;
        let parent_metadata = match parent_commit_id {
            Some(parent_commit_id) => Some(match self.staged_roots.get(parent_commit_id) {
                Some(metadata) => metadata.clone(),
                None => storage::load_snapshot_commit_root(self.store, parent_commit_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "tracked-state parent root for commit '{parent_commit_id}' is missing"
                            ),
                        )
                    })?,
            }),
            None => None,
        };
        let mutation_count_u64 = u64::try_from(mutation_count).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_root changed key count exceeds u64",
            )
        })?;
        let dense_parent_batch = parent_metadata.as_ref().is_some_and(|parent_metadata| {
            mutation_count_u64 > parent_metadata.row_count_estimate / 2
        });
        if parent_metadata.is_some()
            && !dense_parent_batch
            && mutation_count < ORDERED_APPEND_BATCH_MIN_ROWS
        {
            return Ok(None);
        }
        let append_only = match parent_metadata.as_ref() {
            Some(parent_metadata) => {
                self.tree
                    .first_key_is_after_root_right_edge(
                        self.store,
                        &self.chunk_overlay,
                        &parent_metadata.root_id,
                        first_mutation_key,
                    )
                    .await?
            }
            None => false,
        };
        // The right-spine patcher is ideal while the durable parent is a
        // meaningful fraction of the output. For a production-sized append at
        // least 16x the parent, rebuilding bounds extra parent work to 6.25%
        // while avoiding mutation arenas and a second owned descriptor vector.
        let dominant_append = mutation_count >= STREAMING_DOMINANT_APPEND_MIN_ROWS
            && append_only
            && file_delete_cascades.is_empty()
            && parent_metadata.as_ref().is_some_and(|parent_metadata| {
                mutation_count_u64
                    >= parent_metadata
                        .row_count_estimate
                        .saturating_mul(STREAMING_DOMINANT_APPEND_PARENT_RATIO)
            });
        #[cfg(test)]
        if dominant_append {
            STREAMING_DOMINANT_APPEND_EXECUTIONS.with(|executions| {
                executions.set(executions.get().saturating_add(1));
            });
        }
        let use_borrowed_batch = parent_metadata.is_none()
            || (append_only && file_delete_cascades.is_empty() && !dominant_append);
        // Match the existing full-rebuild threshold for overlapping roots. A
        // parentless batch needs no reads, while an append-only batch is already
        // the patcher's cheapest case and can skip point reads at any batch
        // density. Cascade-bearing sparse appends remain on the generic path.
        if !use_borrowed_batch && !dense_parent_batch {
            return Ok(None);
        }

        let base_root = parent_metadata
            .as_ref()
            .map(|metadata| metadata.root_id.clone());
        let (result, cascaded_rows) = if use_borrowed_batch {
            let mutation_batch = build_ordered_root_mutation_batch(mutation_count, mutations)?;
            if mutation_batch.first_encoded_key() != Some(first_mutation_key) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state ordered bulk first key does not match its mutation batch",
                ));
            }
            (
                self.tree
                    .apply_mutations_with_overlay(
                        self.store,
                        self.writes,
                        &mut self.chunk_overlay,
                        base_root.as_ref(),
                        mutation_batch,
                        Some(commit_id),
                    )
                    .await?,
                0,
            )
        } else {
            let parent_metadata = parent_metadata
                .as_ref()
                .expect("dense ordered parent merge requires parent metadata");
            self.tree
                .merge_and_stage_ordered_parent_mutations(
                    self.store,
                    self.writes,
                    &mut self.chunk_overlay,
                    &parent_metadata.root_id,
                    mutation_count,
                    file_delete_cascades,
                    mutations,
                    Some(commit_id),
                )
                .await?
        };
        let changed_rows = mutation_count.checked_add(cascaded_rows).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state commit_root changed key count exceeds usize",
            )
        })?;
        let metadata = TrackedStateCommitRoot {
            commit_id: typed_commit_id,
            root_id: result.root_id.clone(),
            parent_roots: typed_parent_commit_id
                .zip(base_root.as_ref())
                .map(|(parent_commit_id, root_id)| {
                    vec![TrackedStateCommitRootParent {
                        commit_id: parent_commit_id,
                        root_id: root_id.clone(),
                    }]
                })
                .unwrap_or_default(),
            changed_key_count: u64::try_from(changed_rows).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root changed key count exceeds u64",
                )
            })?,
            row_count_estimate: u64::try_from(result.row_count).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root row count exceeds u64",
                )
            })?,
            tree_height: u32::try_from(result.tree_height).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state commit_root tree height exceeds u32",
                )
            })?,
            complete_state_fence: false,
        };
        self.staged_roots.insert(commit_id.to_string(), metadata);

        Ok(Some(TrackedStateWriteReport {
            commit_id: typed_commit_id,
            root_id: result.root_id,
            changed_rows,
            primary_chunk_puts: result.chunk_count,
        }))
    }
}

fn build_ordered_root_mutation_batch<'a, I>(
    expected_mutation_count: usize,
    mutations: I,
) -> Result<TrackedStateMutationBatch, LixError>
where
    I: IntoIterator<Item = Result<TrackedStateRootMutationRef<'a>, LixError>>,
{
    let mut batch = TrackedStateMutationBatchBuilder::with_row_capacity(expected_mutation_count);
    let mut actual_mutation_count = 0usize;
    for mutation in mutations {
        if actual_mutation_count == expected_mutation_count {
            return Err(ordered_root_mutation_count_error(
                expected_mutation_count,
                actual_mutation_count.saturating_add(1),
            ));
        }
        let mutation = mutation?;
        let delta = mutation.delta;
        if !batch.push_strictly_ordered(
            TrackedStateKeyRef {
                schema_key: delta.schema_key,
                file_id: delta.file_id,
                row_pk: delta.row_pk,
            },
            TrackedStateIndexValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                deleted: delta.deleted,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            },
        ) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state ordered bulk mutation keys must be strictly ascending",
            ));
        }
        actual_mutation_count += 1;
    }
    if actual_mutation_count != expected_mutation_count {
        return Err(ordered_root_mutation_count_error(
            expected_mutation_count,
            actual_mutation_count,
        ));
    }
    Ok(batch.finish())
}

fn ordered_root_mutation_count_error(expected: usize, actual: usize) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "tracked-state ordered bulk mutation count mismatch: expected {expected}, received {actual}"
        ),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReport {
    pub(crate) commit_id: CommitId,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) changed_rows: usize,
    pub(crate) primary_chunk_puts: usize,
}

fn missing_commit_root_error(commit_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "tracked_state rooted commit authority is missing snapshot metadata for commit '{commit_id}'"
        ),
    )
}

fn tree_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStateTreeScanRequest {
    TrackedStateTreeScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        row_pks: request.filter.row_pks.clone(),
        row_pk_lower: request.filter.row_pk_lower.clone(),
        row_pk_upper: request.filter.row_pk_upper.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        // User limits belong above delta overlay and tombstone visibility.
        // Pushing them into the physical tree can stop on rows that are later
        // hidden, returning too few live rows.
        limit: None,
    }
}

fn compare_tracked_state_key_refs(
    left: TrackedStateKeyRef<'_>,
    right: TrackedStateKeyRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.file_id.cmp(&right.file_id))
        .then_with(|| left.row_pk.cmp(right.row_pk))
}

fn replacement_scope_covers_key(
    scope: &storage::CommitDeltaReplacementScope,
    key: TrackedStateKeyRef<'_>,
) -> bool {
    scope.schema_key == key.schema_key && scope.file_id.as_deref() == key.file_id
}

fn schema_keys_with_file_descriptors(schema_keys: &[String]) -> Vec<String> {
    if schema_keys.is_empty()
        || schema_keys
            .iter()
            .any(|schema_key| schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
    {
        return schema_keys.to_vec();
    }
    let mut schema_keys = schema_keys.to_vec();
    schema_keys.push(FILE_DESCRIPTOR_SCHEMA_KEY.to_string());
    schema_keys
}

fn file_descriptor_key_for_file_scoped_key(
    key: &TrackedStateKey,
) -> Result<Option<TrackedStateKey>, LixError> {
    key.file_id
        .as_ref()
        .map(|file_id| {
            Ok(TrackedStateKey {
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                file_id: Some(file_id.to_string()),
                row_pk: RowPk::uuid_from_canonical(file_id).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("validated file ID is not a canonical UUID: {error}"),
                    )
                })?,
            })
        })
        .transpose()
}

fn file_delete_cascade(
    key: &TrackedStateKey,
    value: &TrackedStateIndexValue,
) -> Result<Option<String>, LixError> {
    file_delete_cascade_ref(
        TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            row_pk: &key.row_pk,
        },
        value,
    )
}

pub(crate) fn descriptor_dependency_cascade_file_ids(
    target_delta: &[(TrackedStateKey, TrackedStateIndexValue)],
) -> Result<Vec<String>, LixError> {
    let mut file_ids = BTreeSet::new();
    for (key, value) in target_delta {
        if let Some(file_id) = file_delete_cascade(key, value)? {
            file_ids.insert(file_id);
        }
    }
    Ok(file_ids.into_iter().collect())
}

fn file_delete_cascade_ref(
    key: TrackedStateKeyRef<'_>,
    value: &TrackedStateIndexValue,
) -> Result<Option<String>, LixError> {
    if key.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !value.deleted {
        return Ok(None);
    }
    key.row_pk
        .as_single_string_owned()
        .map(Some)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta file descriptor tombstone has invalid identity: {error}"
                ),
            )
        })
}

fn cascade_tombstone(
    cascade: &TrackedStateIndexValue,
    inherited: &TrackedStateIndexValue,
) -> TrackedStateIndexValue {
    TrackedStateIndexValue {
        change_id: cascade.change_id,
        commit_id: cascade.commit_id,
        deleted: true,
        created_at: inherited.created_at,
        updated_at: cascade.updated_at,
    }
}

/// Materializes an exact point set only when every identity component is
/// specified. An unconstrained file id could match arbitrary 01920000-0000-7000-8000-000000000442 rows,
/// so it deliberately retains the cold full-replay scan path.
fn request_has_exact_keys(request: &TrackedStateTreeScanRequest) -> bool {
    !request.schema_keys.is_empty()
        && !request.row_pks.is_empty()
        && !request.file_ids.is_empty()
        && !request
            .file_ids
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
}

fn exact_keys_for_request(request: &TrackedStateTreeScanRequest) -> Option<Vec<TrackedStateKey>> {
    if !request_has_exact_keys(request) {
        return None;
    }
    let mut keys = Vec::with_capacity(
        request.schema_keys.len() * request.row_pks.len() * request.file_ids.len(),
    );
    for schema_key in &request.schema_keys {
        for row_pk in &request.row_pks {
            for file_id in &request.file_ids {
                keys.push(TrackedStateKey {
                    schema_key: schema_key.clone(),
                    row_pk: row_pk.clone(),
                    file_id: match file_id {
                        NullableKeyFilter::Null => None,
                        NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
                        NullableKeyFilter::Any => unreachable!("Any was rejected above"),
                    },
                });
            }
        }
    }
    keys.sort();
    keys.dedup();
    Some(keys)
}

fn validate_diff_row_against_changelog(
    row: &TrackedStateDiffRow,
    changes: &HashMap<ChangeId, ChangeRecord>,
) -> Result<(), LixError> {
    let Some(change) = changes.get(&row.change_id) else {
        return Err(LixError::unknown(format!(
            "tracked-state diff row references missing changelog change '{}'",
            row.change_id
        )));
    };
    let winner = tracked_state_winner_kind_for_diff_parts(
        row.schema_key(),
        row.file_id(),
        row.row_pk(),
        row.deleted,
        row.change_id,
        change,
    )?;
    if !matches!(
        winner,
        TrackedStateRowWinnerKind::CollectionGenerationCascade
    ) && row.deleted != change.snapshot.is_none()
    {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' deleted flag does not match changelog snapshot",
            row.change_id
        )));
    }
    if row.updated_at != change.created_at {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{}' updated_at does not match changelog change timestamp",
            row.change_id
        )));
    }
    Ok(())
}

fn validate_tree_diff_row_against_changelog(
    row: TrackedStateTreeDiffRowRef<'_>,
    changes: &HashMap<ChangeId, ChangeRecord>,
) -> Result<(), LixError> {
    let change_id = row.change_id();
    let Some(change) = changes.get(&change_id) else {
        return Err(LixError::unknown(format!(
            "tracked-state diff row references missing changelog change '{change_id}'"
        )));
    };
    let winner_kind = tracked_state_winner_kind_for_diff_parts(
        row.schema_key(),
        row.file_id(),
        row.row_pk(),
        row.deleted(),
        change_id,
        change,
    )?;
    let change_deleted = change.snapshot.is_none();
    if winner_kind != TrackedStateRowWinnerKind::CollectionGenerationCascade
        && row.deleted() != change_deleted
    {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{change_id}' deleted flag does not match changelog snapshot"
        )));
    }
    if row.updated_at() != change.created_at {
        return Err(LixError::unknown(format!(
            "tracked-state diff row for change '{change_id}' updated_at does not match changelog change timestamp"
        )));
    }
    Ok(())
}

fn tracked_state_winner_identity_for_diff_row(
    row: &TrackedStateDiffRow,
    change: &ChangeRecord,
) -> Result<TrackedStateRowWinner, LixError> {
    tracked_state_winner_identity_for_diff_parts(
        row.schema_key(),
        row.file_id(),
        row.row_pk(),
        row.deleted,
        row.change_id,
        change,
    )
}

fn cascade_payload_key(file_id: &str) -> TrackedStateKey {
    TrackedStateKey {
        schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
        file_id: Some(file_id.to_string()),
        row_pk: RowPk::uuid_from_canonical(file_id).unwrap_or_else(|_| RowPk::single(file_id)),
    }
}

fn collection_cascade_payload_key(schema_key: &str, file_id: Option<&str>) -> TrackedStateKey {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
    };
    TrackedStateKey {
        schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
        file_id: None,
        row_pk: RowPk::single(collection_scope_key(CollectionScopeRef {
            schema_key,
            file_id,
        })),
    }
}

fn tracked_state_winner_identity_for_diff_parts(
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
    deleted: bool,
    change_id: ChangeId,
    change: &ChangeRecord,
) -> Result<TrackedStateRowWinner, LixError> {
    match tracked_state_winner_kind_for_diff_parts(
        schema_key, file_id, row_pk, deleted, change_id, change,
    )? {
        TrackedStateRowWinnerKind::Direct => Ok(TrackedStateRowWinner {
            identity: TrackedStateIdentity {
                schema_key: schema_key.to_owned(),
                file_id: file_id.map(str::to_owned),
                row_pk: row_pk.clone(),
            },
            cascade: false,
        }),
        TrackedStateRowWinnerKind::FileDeleteCascade => Ok(TrackedStateRowWinner {
            identity: TrackedStateIdentity {
                schema_key: change.schema_key.clone(),
                file_id: change.file_id.clone(),
                row_pk: change.row_pk.clone(),
            },
            cascade: true,
        }),
        TrackedStateRowWinnerKind::CollectionGenerationCascade => Ok(TrackedStateRowWinner {
            identity: TrackedStateIdentity {
                schema_key: change.schema_key.clone(),
                file_id: change.file_id.clone(),
                row_pk: change.row_pk.clone(),
            },
            cascade: true,
        }),
    }
}

fn tracked_state_winner_kind_for_diff_parts(
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
    deleted: bool,
    change_id: ChangeId,
    change: &ChangeRecord,
) -> Result<TrackedStateRowWinnerKind, LixError> {
    if change.schema_key == schema_key
        && change.file_id.as_deref() == file_id
        && change.row_pk == *row_pk
    {
        return Ok(TrackedStateRowWinnerKind::Direct);
    }
    if change.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && change.snapshot.is_none() && deleted {
        let cascade_file_id = change.row_pk.as_single_string_owned().map_err(|error| {
            LixError::unknown(format!(
                "tracked-state cascade change '{}' has invalid file descriptor identity: {error}",
                change_id
            ))
        })?;
        if file_id == Some(cascade_file_id.as_str()) {
            return Ok(TrackedStateRowWinnerKind::FileDeleteCascade);
        }
    }
    if change.schema_key == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        && change.snapshot.is_some()
        && deleted
    {
        let (cascade_schema_key, cascade_file_id) =
            crate::collection_generation::collection_scope_from_row_pk(&change.row_pk)?;
        if cascade_schema_key == schema_key && cascade_file_id.as_deref() == file_id {
            return Ok(TrackedStateRowWinnerKind::CollectionGenerationCascade);
        }
    }
    Err(LixError::unknown(format!(
        "tracked-state diff row for change '{}' does not match changelog change identity",
        change_id
    )))
}

fn tracked_state_identity_from_key(key: &TrackedStateKey) -> TrackedStateIdentity {
    TrackedStateIdentity {
        schema_key: key.schema_key.clone(),
        file_id: key.file_id.clone(),
        row_pk: key.row_pk.clone(),
    }
}

fn tracked_state_identity_matches_tree_request(
    identity: &TrackedStateIdentity,
    request: &TrackedStateTreeScanRequest,
) -> bool {
    if !request.schema_keys.is_empty() && !request.schema_keys.contains(&identity.schema_key) {
        return false;
    }
    if !request.row_pks.is_empty() && !request.row_pks.contains(&identity.row_pk) {
        return false;
    }
    nullable_key_filter_allows(&request.file_ids, identity.file_id.as_deref())
}

fn nullable_key_filter_allows(filters: &[NullableKeyFilter<String>], value: Option<&str>) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match (filter, value) {
            (NullableKeyFilter::Any, _) => true,
            (NullableKeyFilter::Null, None) => true,
            (NullableKeyFilter::Value(expected), Some(value)) => expected == value,
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::time::Instant;

    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::CommitRecord;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};

    #[test]
    fn exact_current_state_diff_scope_requires_one_schema_and_concrete_file_lane() {
        let request = TrackedStateTreeScanRequest {
            schema_keys: vec!["alpha".to_string()],
            file_ids: vec![NullableKeyFilter::Null],
            row_pks: vec![RowPk::single("optional-residual")],
            ..TrackedStateTreeScanRequest::default()
        };
        assert_eq!(
            exact_current_state_scope(&request),
            Some(storage::CommitDeltaReplacementScope {
                schema_key: "alpha".to_string(),
                file_id: None,
            })
        );
        assert_eq!(
            exact_current_state_scope(&TrackedStateTreeScanRequest {
                file_ids: vec![NullableKeyFilter::Value("file".to_string())],
                ..request.clone()
            }),
            Some(storage::CommitDeltaReplacementScope {
                schema_key: "alpha".to_string(),
                file_id: Some("file".to_string()),
            })
        );
        for file_ids in [
            Vec::new(),
            vec![NullableKeyFilter::Any],
            vec![
                NullableKeyFilter::Null,
                NullableKeyFilter::Value("file".to_string()),
            ],
        ] {
            assert!(
                exact_current_state_scope(&TrackedStateTreeScanRequest {
                    file_ids,
                    ..request.clone()
                })
                .is_none()
            );
        }
        assert!(
            exact_current_state_scope(&TrackedStateTreeScanRequest {
                schema_keys: Vec::new(),
                ..request
            })
            .is_none()
        );
    }

    #[test]
    fn point_replay_interval_suffixes_share_one_backing_buffer() {
        const COMMIT_COUNT: usize = 1_024;
        let commits = (0..COMMIT_COUNT)
            .map(|index| CommitId::for_test_label(&format!("replay-interval-{index}")))
            .collect::<Vec<_>>();
        let interval = PointReplayInterval::new(commits.clone(), None);
        let mut cache = HashMap::new();

        interval.cache_suffixes(&mut cache);

        assert_eq!(cache.len(), COMMIT_COUNT);
        let backing_buffers = cache
            .values()
            .map(|suffix| suffix.commits.as_ptr())
            .collect::<HashSet<_>>();
        assert_eq!(backing_buffers.len(), 1);
        assert_eq!(interval.commits.len(), COMMIT_COUNT);
        assert_eq!(
            cache
                .values()
                .map(|suffix| suffix.commits().len())
                .sum::<usize>(),
            COMMIT_COUNT * (COMMIT_COUNT + 1) / 2,
            "logical suffix coverage must not require physical suffix copies"
        );
        for (index, commit_id) in commits.iter().enumerate() {
            assert_eq!(cache[commit_id].commits(), &commits[index..]);
        }
    }

    #[tokio::test]
    async fn point_replay_interval_reuses_cached_suffix_and_commit_topology() {
        const COMMIT_COUNT: usize = 32;
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "interval-base", None, &[])
            .await
            .expect("baseline root should write");
        let mut parent = "interval-base".to_string();
        let mut commits = Vec::with_capacity(COMMIT_COUNT);
        for index in 0..COMMIT_COUNT {
            let commit = format!("interval-rootless-{index}");
            write_rootless_commit_for_test(&storage, &commit, &parent, &[]).await;
            commits.push(CommitId::for_test_label(&commit));
            parent = commit;
        }
        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("interval read should open"),
        );

        let head = reader
            .point_replay_interval(&parent)
            .await
            .expect("head interval should load");
        assert_eq!(head.commits().len(), COMMIT_COUNT);
        assert!(head.baseline_root.is_some());
        assert_eq!(reader.point_replay_intervals.len(), COMMIT_COUNT);
        assert_eq!(reader.point_replay_commits.len(), COMMIT_COUNT + 1);

        let middle_commit = commits[COMMIT_COUNT / 2];
        let middle_offset = head
            .commits()
            .iter()
            .position(|commit_id| *commit_id == middle_commit)
            .expect("middle commit should belong to the head interval");
        let middle = reader
            .point_replay_interval(&middle_commit.to_string())
            .await
            .expect("cached suffix should load");
        assert_eq!(middle.commits(), &head.commits()[middle_offset..]);
        assert!(Arc::ptr_eq(&head.commits, &middle.commits));
        assert_eq!(reader.point_replay_commits.len(), COMMIT_COUNT + 1);
    }

    fn commit_root_key(label: &str) -> crate::storage_adapter::StorageKey {
        crate::storage_adapter::StorageKey(Bytes::copy_from_slice(
            CommitId::for_test_label(label).as_uuid().as_bytes(),
        ))
    }

    fn stage_snapshot_authority_for_test(
        writes: &mut StorageWriteSet,
        snapshot_root: TrackedStateCommitRoot,
    ) -> Result<(), LixError> {
        let manifest = crate::tracked_state::CommitStateManifest {
            commit_id: snapshot_root.commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: Default::default(),
            mutations: Default::default(),
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: Some(Box::new(snapshot_root)),
        };
        storage::stage_commit_state_manifest(writes, &manifest)
    }

    fn change_id(label: &str) -> String {
        ChangeId::for_test_label(label).to_string()
    }

    #[tokio::test]
    async fn stage_commit_root_requires_parent_commit_root() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("parent read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_empty_changelog_commit(
                &mut read,
                &mut writes,
                "missing-parent",
                None,
            )
            .await
            .expect("parent changelog commit should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("parent changelog commit should commit");
        }
        {
            let mut writes = storage.new_write_set();
            writes.delete(
                storage::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                commit_root_key("missing-parent"),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("missing parent authority should commit");
        }

        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-child",
            Some("missing-parent"),
            &[row("row-child", "change-child", "commit-child")],
        )
        .await
        .expect_err("root staging should require a parent commit root");
    }

    #[tokio::test]
    async fn stage_commit_root_rejects_physically_retained_semantically_missing_parent() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("parent read should open");
            let mut writes = storage.new_write_set();
            let mut writer = tracked_state.writer(&read, &mut writes);
            let parent_row = row("row-parent", "change-parent", "retained-parent");
            writer
                .stage_commit_root(
                    "retained-parent",
                    None,
                    [delta_from_materialized_row(&parent_row)],
                )
                .await
                .expect("physical parent root should stage");
            let parent_root = writer
                .staged_commit_roots()
                .find(|root| root.commit_id == CommitId::for_test_label("retained-parent"))
                .cloned()
                .expect("physical parent root metadata should stage");
            drop(writer);
            stage_snapshot_authority_for_test(&mut writes, parent_root)
                .expect("physical parent manifest should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("physical parent state should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("child read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        let error = writer
            .stage_commit_root("child", Some("retained-parent"), [])
            .await
            .expect_err("physical retention must not authorize a missing semantic parent");
        assert!(error.message.contains("parent root"), "{error:?}");
    }

    #[tokio::test]
    async fn stage_commit_root_writes_commit_root_metadata() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row("row-a", "change-parent", "parent")],
        )
        .await
        .expect("parent root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[
                row("row-a", "change-child-a", "child"),
                row("row-b", "change-child-b", "child"),
            ],
        )
        .await
        .expect("child root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let parent_root = storage::load_root(&read, "parent")
            .await
            .expect("parent root should load")
            .expect("parent root should exist");
        let child_root = storage::load_root(&read, "child")
            .await
            .expect("child root should load")
            .expect("child root should exist");
        let metadata = storage::load_snapshot_commit_root(&read, "child")
            .await
            .expect("metadata should load")
            .expect("metadata should exist");

        assert_eq!(metadata.commit_id, "child");
        assert_eq!(metadata.root_id, child_root);
        assert_eq!(metadata.parent_roots.len(), 1);
        assert_eq!(metadata.parent_roots[0].commit_id, "parent");
        assert_eq!(metadata.parent_roots[0].root_id, parent_root);
        assert_eq!(metadata.changed_key_count, 2);
        assert_eq!(metadata.row_count_estimate, 2);
        assert!(metadata.tree_height >= 1);
    }

    #[tokio::test]
    async fn collection_replacement_marker_retires_omitted_file_rows() {
        use crate::collection_generation::{
            COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
        };

        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut first = row("a", "replacement-a", "replacement-parent");
        first.file_id = Some("replacement.csv".to_owned());
        let mut second = row("b", "replacement-b", "replacement-parent");
        second.file_id = Some("replacement.csv".to_owned());
        write_root_for_test(
            &storage,
            &tracked_state,
            "replacement-parent",
            None,
            &[first, second],
        )
        .await
        .expect("replacement parent should write");

        let scope_key = collection_scope_key(CollectionScopeRef {
            schema_key: "test_schema",
            file_id: Some("replacement.csv"),
        });
        let marker = MaterializedTrackedStateRow {
            row_pk: RowPk::single(scope_key),
            schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            file_id: None,
            snapshot_content: Some(r#"{"live_count":1}"#.into()),
            decoded_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:01Z".to_owned(),
            updated_at: "2026-01-01T00:00:01Z".to_owned(),
            change_id: ChangeId::for_test_label("replacement-marker"),
            commit_id: CommitId::for_test_label("replacement-child"),
        };
        let mut retained = row("b", "replacement-b-next", "replacement-child");
        retained.file_id = Some("replacement.csv".to_owned());
        let marker_key = TrackedStateKey {
            schema_key: marker.schema_key.clone(),
            file_id: marker.file_id.clone(),
            row_pk: marker.row_pk.clone(),
        };
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replacement child read should open");
        let mut writes = StorageWriteSet::new();
        crate::test_support::stage_tracked_root_from_materialized_with_certified_replacement_markers(
            &mut read,
            &mut writes,
            &tracked_state,
            "replacement-child",
            Some("replacement-parent"),
            &[marker, retained],
            &BTreeSet::from([marker_key]),
        )
        .await
        .expect("replacement child should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("replacement child should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("replacement scan should open"),
        );
        let rows = reader
            .scan_batch_at_commit(
                "replacement-child",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_owned()],
                        file_ids: vec![NullableKeyFilter::Value("replacement.csv".to_owned())],
                        ..crate::tracked_state::TrackedStateFilter::default()
                    },
                    ..TrackedStateScanRequest::default()
                },
            )
            .await
            .expect("replacement child should scan")
            .into_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, RowPk::single("b"));
    }

    #[tokio::test]
    async fn ordinary_collection_generation_marker_does_not_retire_omitted_file_rows() {
        use crate::collection_generation::{
            COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
        };

        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut first = row("a", "ordinary-a", "ordinary-parent");
        first.file_id = Some("ordinary.csv".to_owned());
        let mut second = row("b", "ordinary-b", "ordinary-parent");
        second.file_id = Some("ordinary.csv".to_owned());
        write_root_for_test(
            &storage,
            &tracked_state,
            "ordinary-parent",
            None,
            &[first, second],
        )
        .await
        .expect("ordinary parent should write");

        let scope_key = collection_scope_key(CollectionScopeRef {
            schema_key: "test_schema",
            file_id: Some("ordinary.csv"),
        });
        let marker = MaterializedTrackedStateRow {
            row_pk: RowPk::single(scope_key),
            schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            file_id: None,
            snapshot_content: Some(r#"{"live_count":1}"#.into()),
            decoded_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:01Z".to_owned(),
            updated_at: "2026-01-01T00:00:01Z".to_owned(),
            change_id: ChangeId::for_test_label("ordinary-marker"),
            commit_id: CommitId::for_test_label("ordinary-child"),
        };
        let mut retained = row("b", "ordinary-b-next", "ordinary-child");
        retained.file_id = Some("ordinary.csv".to_owned());
        write_root_for_test(
            &storage,
            &tracked_state,
            "ordinary-child",
            Some("ordinary-parent"),
            &[marker, retained],
        )
        .await
        .expect("ordinary child should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("ordinary scan should open"),
        );
        let rows = reader
            .scan_batch_at_commit(
                "ordinary-child",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_owned()],
                        file_ids: vec![NullableKeyFilter::Value("ordinary.csv".to_owned())],
                        ..crate::tracked_state::TrackedStateFilter::default()
                    },
                    ..TrackedStateScanRequest::default()
                },
            )
            .await
            .expect("ordinary child should scan")
            .into_rows();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn large_ordered_root_batch_uses_two_contiguous_arenas() {
        const ROW_COUNT: usize = 4_096;
        let rows = (0..ROW_COUNT)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-arena-{index:05}"),
                    "ordered-arena",
                )
            })
            .collect::<Vec<_>>();
        let batch = build_ordered_root_mutation_batch(
            rows.len(),
            rows.iter().map(|row| {
                Ok(TrackedStateRootMutationRef {
                    delta: delta_from_materialized_row(row),
                    require_absence: true,
                })
            }),
        )
        .expect("strictly ordered borrowed rows should form one batch");
        let mutations = batch.as_slice();
        assert_eq!(mutations.len(), ROW_COUNT);
        for pair in mutations.windows(2) {
            assert_eq!(
                pair[1].encoded_key.as_ptr() as usize,
                pair[0].encoded_key.as_ptr() as usize + pair[0].encoded_key.len(),
                "ordered root keys must be adjacent slices of one arena"
            );
            assert_eq!(
                pair[1].encoded_value.as_ptr() as usize,
                pair[0].encoded_value.as_ptr() as usize + pair[0].encoded_value.len(),
                "ordered root values must be adjacent slices of one arena"
            );
        }
    }

    #[test]
    fn large_first_parent_diff_overlay_retains_ordinals_not_owned_keys() {
        const ROW_COUNT: usize = 10_000;
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000623";
        let mut key_builder = TrackedStateKeyBatchBuilder::with_row_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let row_pk = RowPk::single(format!("row-{index:05}"));
            key_builder.push(TrackedStateKeyRef {
                schema_key: "test_schema",
                file_id: Some(FILE_ID),
                row_pk: &row_pk,
            });
        }
        let interval_keys = key_builder.finish_batch();
        let template = row("template", "flat-overlay-change", "flat-overlay");
        let delta = delta_from_materialized_row(&template);
        let value = TrackedStateIndexValue {
            change_id: delta.change_id,
            commit_id: delta.commit_id,
            deleted: delta.deleted,
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        };
        let mut overlay = FirstParentDiffOverlay::with_capacities(ROW_COUNT, 1);
        for ordinal in 0..ROW_COUNT {
            overlay
                .insert_key_if_absent(
                    FirstParentDiffKeySource::Interval(ordinal as u32),
                    interval_keys.get(ordinal).expect("encoded interval key"),
                    value.clone(),
                    &interval_keys,
                    None,
                )
                .expect("flat overlay row should insert");
        }
        let descriptor_pk =
            RowPk::uuid_from_canonical(FILE_ID).expect("fixture file ID is canonical");
        let mut cascade = value.clone();
        cascade.deleted = true;
        overlay
            .insert_cascade_if_absent(
                TrackedStateKeyRef {
                    schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
                    file_id: Some(FILE_ID),
                    row_pk: &descriptor_pk,
                },
                &cascade,
            )
            .expect("flat cascade should insert");

        assert_eq!(overlay.entries.len(), ROW_COUNT);
        assert_eq!(overlay.retained_owned_key_count(), 0);
        assert_eq!(
            overlay.large_buffer_count(),
            4,
            "rows share one entry column, cascade column/arena, and hash index"
        );
        assert_eq!(interval_keys.large_buffer_count(), 2);
        assert!(interval_keys.encoded_bytes_len() > ROW_COUNT);

        let (sorted_keys, sorted_values) = overlay
            .compact_sorted(&interval_keys, None)
            .expect("flat overlay should compact");
        assert_eq!(sorted_keys.len(), ROW_COUNT);
        assert_eq!(sorted_values.len(), ROW_COUNT);
        assert_eq!(
            sorted_keys.large_buffer_count(),
            2,
            "sorted lowering remains one encoded arena plus one range column"
        );
        assert!(
            sorted_keys
                .iter()
                .zip(sorted_keys.iter().skip(1))
                .all(|(left, right)| left < right),
            "final replay keys must retain deterministic encoded ordering"
        );
    }

    #[test]
    fn ten_thousand_row_rootless_replay_uses_flat_arenas_and_file_dictionary() {
        const ROW_COUNT: usize = 10_000;
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000629";
        let mut keys = TrackedStateKeyBatchBuilder::with_row_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let row_pk = RowPk::single(format!("row-{index:05}"));
            keys.push(TrackedStateKeyRef {
                schema_key: "test_schema",
                file_id: Some(FILE_ID),
                row_pk: &row_pk,
            });
        }
        let keys = keys.finish_batch();
        let template = row("template", "rootless-flat-change", "rootless-flat");
        let delta = delta_from_materialized_row(&template);
        let value = TrackedStateIndexValue {
            change_id: delta.change_id,
            commit_id: delta.commit_id,
            deleted: false,
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        };
        let mut overlay =
            RootlessReplayOverlay::with_capacities(ROW_COUNT, keys.encoded_bytes_len());
        for ordinal in (0..ROW_COUNT).rev() {
            let row_pk = RowPk::single(format!("row-{ordinal:05}"));
            overlay
                .upsert(
                    keys.get(ordinal).expect("encoded replay key"),
                    TrackedStateKeyRef {
                        schema_key: "test_schema",
                        file_id: Some(FILE_ID),
                        row_pk: &row_pk,
                    },
                    value.clone(),
                )
                .expect("flat replay row should insert");
        }

        let key_bytes_before_duplicate = overlay.key_arena.len();
        let duplicate_pk = RowPk::single("row-05000");
        overlay
            .upsert(
                keys.get(5_000).expect("duplicate encoded replay key"),
                TrackedStateKeyRef {
                    schema_key: "test_schema",
                    file_id: Some(FILE_ID),
                    row_pk: &duplicate_pk,
                },
                value.clone(),
            )
            .expect("duplicate replay row should update in place");
        assert_eq!(overlay.entries.len(), ROW_COUNT);
        assert_eq!(overlay.key_arena.len(), key_bytes_before_duplicate);
        assert_eq!(
            overlay.file_ids.len(),
            1,
            "batch-wide file metadata must be dictionary encoded once"
        );
        assert!(!overlay.file_id_dictionary_promoted);
        assert!(
            overlay.file_id_arena.capacity() < ROW_COUNT,
            "one repeated file must not reserve 36 bytes for every row"
        );
        assert!(
            overlay.file_ids.capacity() < ROW_COUNT,
            "one repeated file must keep the ordinal column small"
        );
        assert!(
            overlay.file_id_hash_heads.capacity() < ROW_COUNT,
            "one repeated file must keep the hash index small"
        );
        assert_eq!(overlay.retained_owned_key_count(), 0);
        assert_eq!(
            overlay.large_buffer_count(),
            6,
            "replay owns one flat buffer per typed key, row, hash, and file dictionary column"
        );

        let descriptor_pk =
            RowPk::uuid_from_canonical(FILE_ID).expect("fixture file ID is canonical");
        let mut cascade = value.clone();
        cascade.deleted = true;
        let mut cascades = RootlessCascadeIndex::with_capacity(ROW_COUNT);
        cascades
            .insert_descriptor(
                TrackedStateKeyRef {
                    schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
                    file_id: Some(FILE_ID),
                    row_pk: &descriptor_pk,
                },
                &cascade,
            )
            .expect("descriptor cascade should insert");
        assert!(!cascades.dictionary_promoted);
        assert!(cascades.file_id_arena.capacity() < ROW_COUNT);
        assert!(cascades.entries.capacity() < ROW_COUNT);
        assert!(cascades.hash_heads.capacity() < ROW_COUNT);

        let mut exact_file_ids = EncodedReplayFileIdDictionary::with_capacity_hint(ROW_COUNT);
        for _ in 0..ROW_COUNT {
            assert_eq!(
                exact_file_ids
                    .intern(SharedStr::from_static(FILE_ID))
                    .expect("repeated exact file ID should intern"),
                0
            );
        }
        assert_eq!(exact_file_ids.file_ids.len(), 1);
        assert!(!exact_file_ids.promoted);
        assert!(exact_file_ids.file_ids.capacity() < ROW_COUNT);
        assert!(exact_file_ids.ordinals.capacity() < ROW_COUNT);

        overlay.apply_cascades(&cascades);
        overlay
            .upsert(
                keys.get(5_000).expect("explicit overwrite key"),
                TrackedStateKeyRef {
                    schema_key: "test_schema",
                    file_id: Some(FILE_ID),
                    row_pk: &duplicate_pk,
                },
                value,
            )
            .expect("same-commit explicit row should overwrite its cascade");

        let replay = overlay.finish();
        assert_eq!(replay.len(), ROW_COUNT);
        assert_eq!(replay.retained_owned_key_count(), 0);
        assert!(
            (0..replay.len() - 1)
                .all(|ordinal| replay.encoded_key(ordinal) < replay.encoded_key(ordinal + 1)),
            "flat replay must seal into deterministic encoded-key order"
        );
        assert!(
            !replay.value(5_000).deleted,
            "same-commit explicit update must run after the file cascade"
        );
        assert_eq!(
            replay
                .key_arena
                .windows(FILE_ID.len())
                .filter(|window| *window == FILE_ID.as_bytes())
                .count(),
            ROW_COUNT,
            "the encoded identity arena contains key bytes, while the separate metadata dictionary remains deduplicated"
        );
    }

    #[test]
    fn ten_thousand_unique_file_ids_promote_rootless_dictionaries_once() {
        const FILE_COUNT: usize = 10_000;
        let template = row("template", "rootless-unique-change", "rootless-unique");
        let delta = delta_from_materialized_row(&template);
        let cascade_value = TrackedStateIndexValue {
            change_id: delta.change_id,
            commit_id: delta.commit_id,
            deleted: true,
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        };
        let mut overlay = RootlessReplayOverlay::with_capacities(FILE_COUNT, 0);
        let mut cascades = RootlessCascadeIndex::with_capacity(FILE_COUNT);
        let mut exact_file_ids = EncodedReplayFileIdDictionary::with_capacity_hint(FILE_COUNT);

        for index in 0..FILE_COUNT {
            let file_id = format!("01920000-0000-7000-8000-{index:012}");
            assert_eq!(
                overlay
                    .intern_file_id(Some(&file_id))
                    .expect("unique replay file ID should intern"),
                index as u32
            );
            let descriptor_pk =
                RowPk::uuid_from_canonical(&file_id).expect("fixture file ID is canonical");
            cascades
                .insert_descriptor(
                    TrackedStateKeyRef {
                        schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
                        file_id: Some(&file_id),
                        row_pk: &descriptor_pk,
                    },
                    &cascade_value,
                )
                .expect("unique cascade file ID should intern");
            assert_eq!(
                exact_file_ids
                    .intern(SharedStr::from(file_id))
                    .expect("unique exact file ID should intern"),
                index as u32
            );
        }

        assert_eq!(overlay.file_ids.len(), FILE_COUNT);
        assert!(overlay.file_id_dictionary_promoted);
        assert!(
            overlay.file_id_arena.capacity() >= FILE_COUNT.saturating_mul(ESTIMATED_FILE_ID_BYTES)
        );
        assert!(overlay.file_ids.capacity() >= FILE_COUNT);
        assert!(overlay.file_id_hash_heads.capacity() >= FILE_COUNT);

        assert_eq!(cascades.entries.len(), FILE_COUNT);
        assert!(cascades.dictionary_promoted);
        assert!(
            cascades.file_id_arena.capacity() >= FILE_COUNT.saturating_mul(ESTIMATED_FILE_ID_BYTES)
        );
        assert!(cascades.entries.capacity() >= FILE_COUNT);
        assert!(cascades.hash_heads.capacity() >= FILE_COUNT);

        assert_eq!(exact_file_ids.file_ids.len(), FILE_COUNT);
        assert!(exact_file_ids.promoted);
        assert!(exact_file_ids.file_ids.capacity() >= FILE_COUNT);
        assert!(exact_file_ids.ordinals.capacity() >= FILE_COUNT);
    }

    #[test]
    fn ordered_root_batch_rejects_duplicate_encoded_keys() {
        let rows = [
            row("row-duplicate", "change-duplicate-a", "ordered-duplicate"),
            row("row-duplicate", "change-duplicate-b", "ordered-duplicate"),
        ];
        let error = build_ordered_root_mutation_batch(
            rows.len(),
            rows.iter().map(|row| {
                Ok(TrackedStateRootMutationRef {
                    delta: delta_from_materialized_row(row),
                    require_absence: false,
                })
            }),
        )
        .expect_err("ordered batch must reject duplicate identities");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    }

    #[tokio::test]
    async fn large_parentless_ordered_root_stages_shared_storage_arenas() {
        const ROW_COUNT: usize = 4_096;
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let commit_id = CommitId::for_test_label("ordered-parentless").to_string();
        let rows = (0..ROW_COUNT)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-parentless-{index:05}"),
                    "ordered-parentless",
                )
            })
            .collect::<Vec<_>>();
        let first_key = encoded_key_from_materialized_row(&rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("parentless read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        let report = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &commit_id,
                None,
                rows.len(),
                &first_key,
                &BTreeMap::new(),
                rows.iter().map(|row| {
                    Ok(TrackedStateRootMutationRef {
                        delta: delta_from_materialized_row(row),
                        require_absence: true,
                    })
                }),
            )
            .await
            .expect("parentless ordered root should stage")
            .expect("parentless bulk rows should use the arena-backed path");
        assert_eq!(report.changed_rows, ROW_COUNT);
        drop(writer);

        let arenas = writes.arena_stats();
        assert_eq!(arenas.put_descriptors, report.primary_chunk_puts);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.value_shared_buffers, 1);
        assert_eq!(arenas.key_inline_allocations, 0);
        assert_eq!(arenas.value_inline_allocations, 0);
        assert!(
            arenas.put_descriptors < ROW_COUNT / 4,
            "storage must retain chunk descriptors, not one owned mutation per row"
        );
    }

    #[tokio::test]
    async fn dense_ordered_parent_merge_is_canonical_and_reads_staged_parent_chunks() {
        const PARENT_ROW_COUNT: usize = 8_194;
        const UPDATED_ROW_COUNT: usize = 4_097;
        const NEW_ROW_COUNT: usize = 4_097;
        let bulk_storage = StorageAdapter::new(Memory::new());
        let generic_storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("dense-parent").to_string();
        let child_commit_id = CommitId::for_test_label("dense-child").to_string();

        let parent_rows = (0..PARENT_ROW_COUNT)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-parent-{index:05}"),
                    "dense-parent",
                )
            })
            .collect::<Vec<_>>();
        let mut child_rows = (0..UPDATED_ROW_COUNT)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-child-{index:05}"),
                    "dense-child",
                )
            })
            .chain((0..NEW_ROW_COUNT).map(|index| {
                row(
                    &format!("row-{index:05}-new"),
                    &format!("change-child-new-{index:05}"),
                    "dense-child",
                )
            }))
            .collect::<Vec<_>>();
        child_rows.sort_by(|left, right| left.row_pk.cmp(&right.row_pk));
        for row in &mut child_rows {
            row.created_at = "2026-02-01T00:00:00Z".to_string();
            row.updated_at = "2026-03-01T00:00:00Z".to_string();
        }
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let report = {
            let read = bulk_storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("bulk read should open");
            let mut writes = bulk_storage.new_write_set();
            let mut writer = tracked_state.writer(&read, &mut writes);
            writer
                .stage_commit_root(
                    &parent_commit_id,
                    None,
                    parent_rows.iter().map(delta_from_materialized_row),
                )
                .await
                .expect("parent root should stage");
            let report = writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &child_commit_id,
                    Some(&parent_commit_id),
                    child_rows.len(),
                    &first_child_key,
                    &BTreeMap::new(),
                    child_rows.iter().map(|row| {
                        Ok(TrackedStateRootMutationRef {
                            delta: delta_from_materialized_row(row),
                            require_absence: false,
                        })
                    }),
                )
                .await
                .expect("bulk child root should stage")
                .expect("dense changed rows should take the dense merge");
            let staged_roots = writer.staged_commit_roots().cloned().collect::<Vec<_>>();
            drop(writer);
            for root in staged_roots {
                stage_snapshot_authority_for_test(&mut writes, root)
                    .expect("bulk snapshot authority should stage");
            }
            bulk_storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("bulk roots should commit");
            report
        };
        assert_eq!(report.changed_rows, child_rows.len());

        write_root_for_test(
            &generic_storage,
            &tracked_state,
            &parent_commit_id,
            None,
            &parent_rows,
        )
        .await
        .expect("generic parent commit and root should write");
        {
            let read = generic_storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("generic child read should open");
            let mut writes = generic_storage.new_write_set();
            let mut writer = tracked_state.writer(&read, &mut writes);
            writer
                .stage_commit_root(
                    &child_commit_id,
                    Some(&parent_commit_id),
                    child_rows.iter().map(delta_from_materialized_row),
                )
                .await
                .expect("generic child root should stage");
            let child_root = writer
                .staged_commit_roots()
                .find(|root| root.commit_id == child_commit_id)
                .cloned()
                .expect("generic child metadata should stage");
            drop(writer);
            stage_snapshot_authority_for_test(&mut writes, child_root)
                .expect("generic child authority should stage");
            generic_storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("generic child root should commit");
        }

        let bulk_read = bulk_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("bulk result read should open");
        let bulk_root = storage::load_manifest_snapshot_commit_root(
            &bulk_read,
            CommitId::parse_lix(&child_commit_id, "bulk child fixture").expect("test commit id"),
        )
        .await
        .expect("bulk root should load")
        .expect("bulk child root should exist")
        .root_id;
        let generic_read = generic_storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("generic result read should open");
        let generic_root = storage::load_manifest_snapshot_commit_root(
            &generic_read,
            CommitId::parse_lix(&child_commit_id, "generic child fixture").expect("test commit id"),
        )
        .await
        .expect("generic root should load")
        .expect("generic child root should exist")
        .root_id;
        assert_eq!(bulk_root, generic_root, "dense merge must be canonical");

        let entry = TrackedStateTree::new()
            .get(
                &bulk_read,
                &bulk_root,
                &TrackedStateKey {
                    schema_key: "test_schema".to_string(),
                    file_id: None,
                    row_pk: RowPk::single("row-00000"),
                },
            )
            .await
            .expect("bulk row should load")
            .expect("bulk row should exist");
        assert_eq!(
            entry.created_at(),
            crate::common::LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z")
        );
        assert_eq!(
            entry.updated_at(),
            crate::common::LixTimestamp::expect_parse("updated_at", "2026-03-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn dense_ordered_parent_merge_preserves_live_insert_absence_guards() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("dense-guard-parent").to_string();
        let child_commit_id = CommitId::for_test_label("dense-guard-child").to_string();
        let parent_rows = [
            row("row-a", "change-parent-a", "dense-guard-parent"),
            row("row-b", "change-parent-b", "dense-guard-parent"),
        ];
        let child_rows = [
            row("row-a", "change-child-a", "dense-guard-child"),
            row("row-b", "change-child-b", "dense-guard-child"),
        ];
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("parent root should stage");
        let error = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &child_commit_id,
                Some(&parent_commit_id),
                child_rows.len(),
                &first_child_key,
                &BTreeMap::new(),
                child_rows.iter().map(|row| {
                    Ok(TrackedStateRootMutationRef {
                        delta: delta_from_materialized_row(row),
                        require_absence: true,
                    })
                }),
            )
            .await
            .expect_err("live parent row must reject INSERT");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn dense_ordered_parent_merge_allows_tombstone_reinsertion() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("dense-tombstone-parent").to_string();
        let child_commit_id = CommitId::for_test_label("dense-tombstone-child").to_string();
        let parent_rows = [
            tombstone("row-a", "change-parent-a", "dense-tombstone-parent"),
            row("row-b", "change-parent-b", "dense-tombstone-parent"),
        ];
        let child_rows = [
            row("row-a", "change-child-a", "dense-tombstone-child"),
            row("row-b", "change-child-b", "dense-tombstone-child"),
        ];
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("parent root should stage");
        assert!(
            writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &child_commit_id,
                    Some(&parent_commit_id),
                    child_rows.len(),
                    &first_child_key,
                    &BTreeMap::new(),
                    child_rows.iter().enumerate().map(|(index, row)| {
                        Ok(TrackedStateRootMutationRef {
                            delta: delta_from_materialized_row(row),
                            require_absence: index == 0,
                        })
                    }),
                )
                .await
                .expect("tombstone reinsertion should stage")
                .is_some(),
            "tombstoned parent rows permit INSERT"
        );
    }

    #[tokio::test]
    async fn large_ordered_append_uses_shared_arenas_and_reuses_parent_chunks() {
        const PARENT_ROWS: usize = 4_096;
        const APPENDED_ROWS: usize = 128;
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("dense-append-parent").to_string();
        let child_commit_id = CommitId::for_test_label("dense-append-child").to_string();
        let parent_rows = (0..PARENT_ROWS)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-parent-{index:05}"),
                    "dense-append-parent",
                )
            })
            .collect::<Vec<_>>();
        let child_rows = (0..APPENDED_ROWS)
            .map(|index| {
                row(
                    &format!("row-1{index:04}"),
                    &format!("change-child-{index:05}"),
                    "dense-append-child",
                )
            })
            .collect::<Vec<_>>();
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        let parent_report = writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("parent root should stage");
        let child_report = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &child_commit_id,
                Some(&parent_commit_id),
                child_rows.len(),
                &first_child_key,
                &BTreeMap::new(),
                child_rows.iter().map(|row| {
                    Ok(TrackedStateRootMutationRef {
                        delta: delta_from_materialized_row(row),
                        require_absence: true,
                    })
                }),
            )
            .await
            .expect("append-only root should stage")
            .expect("append-only rows should use the borrowed patcher path");
        assert_eq!(child_report.changed_rows, APPENDED_ROWS);
        assert!(
            child_report.primary_chunk_puts < parent_report.primary_chunk_puts,
            "append staging should retain parent leaf chunks instead of rebuilding the root"
        );
        drop(writer);

        let arenas = writes.arena_stats();
        assert_eq!(arenas.key_shared_buffers, 2);
        assert_eq!(arenas.value_shared_buffers, 2);
        assert_eq!(arenas.key_inline_allocations, 0);
        assert_eq!(arenas.value_inline_allocations, 0);

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("parent and append roots should commit");
        let committed = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("committed append root should open");
        let last_child = child_rows.last().expect("append fixture has rows");
        assert!(
            TrackedStateTree::new()
                .get(
                    &committed,
                    &child_report.root_id,
                    &TrackedStateKey {
                        schema_key: last_child.schema_key.clone(),
                        file_id: last_child.file_id.clone(),
                        row_pk: last_child.row_pk.clone(),
                    },
                )
                .await
                .expect("appended row should load")
                .is_some()
        );
    }

    #[tokio::test]
    async fn dominant_ordered_append_uses_bounded_parent_merge() {
        const PARENT_ROWS: usize = 64;
        const APPENDED_ROWS: usize = STREAMING_DOMINANT_APPEND_MIN_ROWS + 1;
        STREAMING_DOMINANT_APPEND_EXECUTIONS.with(|executions| executions.set(0));

        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("dominant-append-parent").to_string();
        let child_commit_id = CommitId::for_test_label("dominant-append-child").to_string();
        let parent_rows = (0..PARENT_ROWS)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-parent-{index:05}"),
                    "dominant-append-parent",
                )
            })
            .collect::<Vec<_>>();
        let child_rows = (0..APPENDED_ROWS)
            .map(|index| {
                row(
                    &format!("row-1{index:05}"),
                    &format!("change-child-{index:05}"),
                    "dominant-append-child",
                )
            })
            .collect::<Vec<_>>();
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("parent root should stage");
        for (claimed_count, actual_count) in [
            (APPENDED_ROWS, APPENDED_ROWS - 1),
            (APPENDED_ROWS - 1, APPENDED_ROWS),
        ] {
            let error = writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &child_commit_id,
                    Some(&parent_commit_id),
                    claimed_count,
                    &first_child_key,
                    &BTreeMap::new(),
                    child_rows.iter().take(actual_count).map(|row| {
                        Ok(TrackedStateRootMutationRef {
                            delta: delta_from_materialized_row(row),
                            require_absence: true,
                        })
                    }),
                )
                .await
                .expect_err("dominant append must validate its declared mutation count");
            assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
            assert!(error.message.contains("mutation count mismatch"));
        }
        let child_report = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &child_commit_id,
                Some(&parent_commit_id),
                child_rows.len(),
                &first_child_key,
                &BTreeMap::new(),
                child_rows.iter().map(|row| {
                    Ok(TrackedStateRootMutationRef {
                        delta: delta_from_materialized_row(row),
                        require_absence: true,
                    })
                }),
            )
            .await
            .expect("dominant append root should stage")
            .expect("dominant append should use the ordered bulk path");
        assert_eq!(child_report.changed_rows, APPENDED_ROWS);
        STREAMING_DOMINANT_APPEND_EXECUTIONS.with(|executions| {
            assert_eq!(
                executions.get(),
                3,
                "production-sized dominant appends must use the bounded parent merger"
            );
        });
        drop(writer);

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("dominant append root should commit");
        let committed = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("committed dominant append root should open");
        for expected in [
            parent_rows.first().expect("parent fixture has rows"),
            child_rows.last().expect("append fixture has rows"),
        ] {
            assert!(
                TrackedStateTree::new()
                    .get(
                        &committed,
                        &child_report.root_id,
                        &TrackedStateKey {
                            schema_key: expected.schema_key.clone(),
                            file_id: expected.file_id.clone(),
                            row_pk: expected.row_pk.clone(),
                        },
                    )
                    .await
                    .expect("merged row should load")
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn substantial_parent_append_preserves_parent_chunks() {
        const PARENT_ROWS: usize = 4_096;
        const APPENDED_ROWS: usize = STREAMING_DOMINANT_APPEND_MIN_ROWS;
        STREAMING_DOMINANT_APPEND_EXECUTIONS.with(|executions| executions.set(0));

        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("substantial-append-parent").to_string();
        let child_commit_id = CommitId::for_test_label("substantial-append-child").to_string();
        let rebuild_commit_id = CommitId::for_test_label("substantial-append-rebuild").to_string();
        let parent_rows = (0..PARENT_ROWS)
            .map(|index| {
                row(
                    &format!("row-{index:05}"),
                    &format!("change-parent-{index:05}"),
                    "substantial-append-parent",
                )
            })
            .collect::<Vec<_>>();
        let child_rows = (0..APPENDED_ROWS)
            .map(|index| {
                row(
                    &format!("row-1{index:05}"),
                    &format!("change-child-{index:05}"),
                    "substantial-append-child",
                )
            })
            .collect::<Vec<_>>();
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        let _parent_report = writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("parent root should stage");
        let child_report = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &child_commit_id,
                Some(&parent_commit_id),
                child_rows.len(),
                &first_child_key,
                &BTreeMap::new(),
                child_rows.iter().map(|row| {
                    Ok(TrackedStateRootMutationRef {
                        delta: delta_from_materialized_row(row),
                        require_absence: true,
                    })
                }),
            )
            .await
            .expect("append root should stage")
            .expect("append should use the ordered bulk path");
        STREAMING_DOMINANT_APPEND_EXECUTIONS.with(|executions| {
            assert_eq!(
                executions.get(),
                0,
                "an append below the parent ratio must retain right-spine reuse"
            );
        });
        let rebuild_report = writer
            .stage_commit_root(
                &rebuild_commit_id,
                None,
                parent_rows
                    .iter()
                    .chain(child_rows.iter())
                    .map(delta_from_materialized_row),
            )
            .await
            .expect("canonical full rebuild should stage");
        assert!(
            child_report.primary_chunk_puts < rebuild_report.primary_chunk_puts,
            "right-spine append should stage fewer chunks than a full output rebuild"
        );
    }

    #[tokio::test]
    async fn sparse_append_with_file_cascade_stays_on_generic_path() {
        const FILE_ID: &str = "01920000-0000-7000-8000-0000000000a2.json";
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let parent_commit_id = CommitId::for_test_label("cascade-append-parent").to_string();
        let child_commit_id = CommitId::for_test_label("cascade-append-child").to_string();
        let parent_rows = (0..8)
            .map(|index| {
                let mut row = row(
                    &format!("row-{index:02}"),
                    &format!("change-cascade-parent-{index:02}"),
                    "cascade-append-parent",
                );
                row.schema_key = "a_schema".to_string();
                row.file_id = Some(FILE_ID.to_string());
                row
            })
            .collect::<Vec<_>>();
        let mut schema_registration = row(
            "a_schema",
            "change-cascade-schema-registration",
            "cascade-append-parent",
        );
        schema_registration.schema_key = REGISTERED_SCHEMA_KEY.to_string();
        schema_registration.file_id = None;
        let mut descriptor =
            tombstone(FILE_ID, "change-cascade-descriptor", "cascade-append-child");
        descriptor.schema_key = FILE_DESCRIPTOR_SCHEMA_KEY.to_string();
        descriptor.file_id = Some(FILE_ID.to_string());
        let mut tail = row("row-tail", "change-cascade-tail", "cascade-append-child");
        tail.schema_key = "z_schema".to_string();
        let child_rows = [descriptor, tail];
        let first_child_key = encoded_key_from_materialized_row(&child_rows[0]);
        let file_delete_cascades = BTreeMap::from([(
            FILE_ID.to_string(),
            delta_from_materialized_row(&child_rows[0]),
        )]);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cascade append read should open");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        writer
            .stage_commit_root(
                &parent_commit_id,
                None,
                parent_rows
                    .iter()
                    .chain(std::iter::once(&schema_registration))
                    .map(delta_from_materialized_row),
            )
            .await
            .expect("cascade parent root should stage");
        assert!(
            writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &child_commit_id,
                    Some(&parent_commit_id),
                    child_rows.len(),
                    &first_child_key,
                    &file_delete_cascades,
                    child_rows.iter().map(|row| {
                        Ok(TrackedStateRootMutationRef {
                            delta: delta_from_materialized_row(row),
                            require_absence: false,
                        })
                    }),
                )
                .await
                .expect("cascade route selection should succeed")
                .is_none(),
            "a sparse append with file cascades must retain generic cascade planning"
        );
        let child_report = writer
            .stage_commit_root(
                &child_commit_id,
                Some(&parent_commit_id),
                child_rows.iter().map(delta_from_materialized_row),
            )
            .await
            .expect("generic cascade root should stage");
        drop(writer);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("cascade roots should commit");

        let committed = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cascade root should open");
        let cascaded = TrackedStateTree::new()
            .get(
                &committed,
                &child_report.root_id,
                &TrackedStateKey {
                    schema_key: parent_rows[0].schema_key.clone(),
                    file_id: parent_rows[0].file_id.clone(),
                    row_pk: parent_rows[0].row_pk.clone(),
                },
            )
            .await
            .expect("cascaded parent row should load")
            .expect("cascaded parent row should remain as a tombstone");
        assert!(cascaded.deleted());
        assert_eq!(cascaded.change_id, child_rows[0].change_id);
    }

    #[tokio::test]
    async fn file_cascade_discovers_unregistered_file_owned_schema_from_root() {
        const FILE_ID: &str = "01920000-0000-7000-8000-0000000000a3.json";
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();

        let mut descriptor = row(FILE_ID, "descriptor-parent", "parent");
        descriptor.schema_key = FILE_DESCRIPTOR_SCHEMA_KEY.to_string();
        descriptor.file_id = Some(FILE_ID.to_string());
        let mut private_row = row("private-row", "private-parent", "parent");
        private_row.schema_key = "plugin_private_schema".to_string();
        private_row.file_id = Some(FILE_ID.to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[descriptor.clone(), private_row.clone()],
        )
        .await
        .expect("unregistered private row root should stage");

        descriptor.snapshot_content = None;
        descriptor.deleted = true;
        descriptor.change_id = ChangeId::for_test_label("descriptor-delete");
        descriptor.commit_id = CommitId::for_test_label("child");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[descriptor],
        )
        .await
        .expect("authenticated root inventory must find the private schema");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("child root should open");
        let child_root = tracked_state
            .tree
            .load_root(&read, "child")
            .await
            .expect("child root metadata should load")
            .expect("child root must exist");
        let value = TrackedStateTree::new()
            .get(
                &read,
                &child_root,
                &TrackedStateKey {
                    schema_key: private_row.schema_key,
                    file_id: private_row.file_id,
                    row_pk: private_row.row_pk,
                },
            )
            .await
            .expect("private row should remain addressable")
            .expect("private row must be represented by a tombstone");
        assert!(value.deleted());
        assert_eq!(value.commit_id, CommitId::for_test_label("child"));
    }

    #[tokio::test]
    async fn staged_root_audit_failure_does_not_publish_replacement() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-a",
            None,
            &[row("row-a", "change-a", "commit-a")],
        )
        .await
        .expect("committed root should write");
        let original_root = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("original-root read should open");
            storage::load_root(&read, "commit-a")
                .await
                .expect("original root should load")
                .expect("original root should exist")
        };

        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("staged-root read should open");
            let mut writes = storage.new_write_set();
            let mut writer = tracked_state.writer(&read, &mut writes);
            let replacement = writer
                .stage_commit_root(
                    "commit-a",
                    None,
                    std::iter::empty::<TrackedStateDeltaRef<'_>>(),
                )
                .await
                .expect("invalid replacement should stage before audit");
            assert_ne!(replacement.root_id, original_root);

            let error = writer
                .validate_staged_commit_root_against_changelog("commit-a")
                .await
                .expect_err("audit must reject a root that omits the changelog winner");
            assert!(
                error.message.contains("omits current changelog change"),
                "unexpected error: {error}"
            );
            // Dropping the failed staged write set is the publication fence.
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        assert_eq!(
            storage::load_root(&read, "commit-a")
                .await
                .expect("published root should load"),
            Some(original_root)
        );
    }

    #[tokio::test]
    async fn stage_empty_commit_root_reuses_parent_without_tree_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            &[row("row-a", "change-parent", "parent")],
        )
        .await
        .expect("parent root should write");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let parent_metadata = storage::load_snapshot_commit_root(&read, "parent")
            .await
            .expect("parent metadata should load")
            .expect("parent metadata should exist");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&mut read, &mut writes);
        let report = writer
            .stage_commit_root("empty-child", Some("parent"), [])
            .await
            .expect("empty child root should stage");
        let child_root = writer
            .staged_commit_roots()
            .find(|root| root.commit_id == CommitId::for_test_label("empty-child"))
            .cloned()
            .expect("empty child metadata should stage");
        drop(writer);
        stage_snapshot_authority_for_test(&mut writes, child_root)
            .expect("empty child authority should stage");

        assert_eq!(report.changed_rows, 0);
        assert_eq!(report.primary_chunk_puts, 0);
        assert_eq!(report.root_id, parent_metadata.root_id);

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty child root should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        let child_metadata = storage::load_manifest_snapshot_commit_root(
            &read,
            CommitId::for_test_label("empty-child"),
        )
        .await
        .expect("child metadata should load")
        .expect("child metadata should exist");

        assert_eq!(child_metadata.root_id, parent_metadata.root_id);
        assert_eq!(child_metadata.changed_key_count, 0);
        assert_eq!(
            child_metadata.row_count_estimate,
            parent_metadata.row_count_estimate
        );
        assert_eq!(child_metadata.tree_height, parent_metadata.tree_height);
        assert_eq!(child_metadata.parent_roots.len(), 1);
        assert_eq!(child_metadata.parent_roots[0].commit_id, "parent");
        assert_eq!(
            child_metadata.parent_roots[0].root_id,
            parent_metadata.root_id
        );
    }

    #[tokio::test]
    async fn plan_merge_from_roots_applies_source_only_change() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row_with_value("row-a", "change-base", "base", "base")],
            &[row_with_value("row-a", "change-base", "base", "base")],
            &[row_with_value("row-a", "change-source", "source", "source")],
        )
        .await;

        let plan = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert_eq!(merge_pick_ids(&plan), vec!["row-a"]);
        assert!(plan.conflicts.is_empty());
    }

    #[tokio::test]
    async fn plan_merge_from_roots_keeps_target_only_change() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row("row-a", "change-base", "base")],
            &[row("row-a", "change-target", "target")],
            &[row("row-a", "change-base", "base")],
        )
        .await;

        let plan = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[tokio::test]
    async fn plan_merge_from_roots_reports_divergent_modification_conflict() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row_with_value("row-a", "change-base", "base", "base")],
            &[row_with_value("row-a", "change-target", "target", "target")],
            &[row_with_value("row-a", "change-source", "source", "source")],
        )
        .await;

        let plan = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert_eq!(merge_conflict_ids(&plan), vec!["row-a"]);
    }

    #[tokio::test]
    async fn plan_merge_from_roots_applies_source_tombstone() {
        let (storage, tracked_state) = seed_merge_roots(
            &[row("row-a", "change-base", "base")],
            &[row("row-a", "change-base", "base")],
            &[tombstone("row-a", "change-source-delete", "source")],
        )
        .await;

        let plan = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .plan_merge(
                "base",
                "target",
                "source",
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("merge should plan");

        assert_eq!(merge_pick_ids(&plan), vec!["row-a"]);
        assert!(plan.picks[0].source_row().deleted);
        assert_eq!(
            plan.picks[0].source_change_id(),
            change_id("change-source-delete")
        );
    }

    #[tokio::test]
    async fn retired_root_projection_is_not_authoritative() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            &[row_with_value("row-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("commit-state authority should expose the snapshot root");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].kind,
            crate::tracked_state::TrackedStateDiffKind::Modified
        );
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn detached_complete_snapshot_fences_inherited_row_proof_and_accepts_child_writes() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "base", None, &[])
            .await
            .expect("empty base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            Some("base"),
            &[row("row-parent", "change-parent", "parent")],
        )
        .await
        .expect("semantic parent root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "fence",
            Some("parent"),
            &[row("row-fence", "change-fence", "fence")],
        )
        .await
        .expect("fence root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fence manifest read should open");
        let fence_id = CommitId::for_test_label("fence");
        let mut fence_manifest = storage::load_commit_state_manifest(&read, fence_id)
            .await
            .expect("fence manifest should load")
            .expect("fence manifest should exist");
        fence_manifest
            .snapshot_root
            .as_mut()
            .expect("fence snapshot root should exist")
            .parent_roots
            .clear();
        let mut writes = storage.new_write_set();
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &fence_manifest)
            .expect("detached fence manifest should reseal");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("uncertified detached root should commit as corrupt test input");

        let error = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("uncertified proof read should open"),
            )
            .validate_commit_root_against_changelog("fence")
            .await
            .expect_err("an empty physical parent chain needs an explicit complete-state fence");
        assert!(error.message.contains("without a complete-state fence"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("uncertified fence read should open");
        let mut fence_manifest = storage::load_commit_state_manifest(&read, fence_id)
            .await
            .expect("uncertified fence manifest should load")
            .expect("uncertified fence manifest should exist");
        fence_manifest
            .snapshot_root
            .as_mut()
            .expect("fence snapshot root should exist")
            .complete_state_fence = true;
        let mut writes = storage.new_write_set();
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &fence_manifest)
            .expect("certified detached fence manifest should reseal");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("certified detached fence should commit");

        // A normal write can extend the detached full-state snapshot. Its new
        // physical parent is the fence, while the fence's semantic parent is
        // deliberately not a physical dependency.
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("fence"),
            &[row("row-child", "change-child", "child")],
        )
        .await
        .expect("child write should extend the detached fence");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("detached fence verification read should open");
        let mut reader = tracked_state.reader(read);
        let rows = reader
            .scan_batch_at_commit("child", &test_schema_scan_request())
            .await
            .expect("child state should scan through the detached fence")
            .into_rows();
        assert_eq!(rows.len(), 3);
        let diff = reader
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("inherited rows should authenticate at the detached fence");
        assert_eq!(diff.entries.len(), 3);
    }

    #[tokio::test]
    async fn diff_allows_repaired_root_with_rebuilt_ancestor_chain() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "middle",
            Some("base"),
            &[row_with_value("row-a", "change-middle", "middle", "middle")],
        )
        .await
        .expect("middle root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("middle"),
            &[row_with_value("row-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        delete_root_chunk_for_test(&storage, "middle").await;
        delete_root_chunk_for_test(&storage, "child").await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("child root should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should accept repaired nearest-ancestor parent metadata");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_rejects_missing_commit_state_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "middle",
            Some("base"),
            &[row_with_value("row-a", "change-middle", "middle", "middle")],
        )
        .await
        .expect("middle root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("middle"),
            &[row_with_value("row-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        {
            let mut writes = storage.new_write_set();
            for commit_id in ["middle", "child"] {
                writes.delete(
                    storage::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                    commit_root_key(commit_id),
                );
                writes.delete(
                    storage::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
                    commit_root_key(commit_id),
                );
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("commit_root deletes should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect_err("semantic authority cannot be reconstructed from a serving index");
        assert!(error.message.contains("without its commit-state manifest"));
    }

    #[tokio::test]
    async fn explicit_rebuild_errors_on_first_parent_cycle() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-a",
            None,
            &[row_with_value("row-a", "commit-a:change", "commit-a", "a")],
        )
        .await
        .expect("commit-a should commit");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-b",
            Some("commit-a"),
            &[row_with_value("row-b", "commit-b:change", "commit-b", "b")],
        )
        .await
        .expect("commit-b should commit");
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("cycle authority read should open");
            let mut writes = storage.new_write_set();
            let commit_a = CommitId::for_test_label("commit-a");
            let commit_b = CommitId::for_test_label("commit-b");
            let published = storage::load_published_commit_state_manifest(&read, commit_a)
                .await
                .expect("commit-a authority should load")
                .expect("commit-a authority should exist");
            let mut snapshot_root = storage::load_snapshot_commit_root(&read, "commit-a")
                .await
                .expect("commit-a root should load")
                .expect("commit-a root should exist");
            snapshot_root.parent_roots = vec![TrackedStateCommitRootParent {
                commit_id: commit_b,
                root_id: storage::load_root(&read, "commit-b")
                    .await
                    .expect("commit-b root should load")
                    .expect("commit-b root should exist"),
            }];
            let mut corrupt = (*published).clone();
            corrupt.snapshot_root = Some(Box::new(snapshot_root));
            storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &corrupt)
                .expect("corrupt cycle root should stage");
            writes.put(
                crate::changelog::COMMIT_SPACE,
                crate::storage_adapter::StorageKey(Bytes::copy_from_slice(
                    commit_a.as_uuid().as_bytes(),
                )),
                crate::changelog::encode_commit_record(&CommitRecord {
                    touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                    format_version: 3,
                    commit_id: commit_a,
                    generation: 2,
                    parent_commit_ids: vec![commit_b],
                    first_parent_jump_commit_id: commit_a,
                    first_parent_jump_span: 0,
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    created_at: crate::common::LixTimestamp::expect_parse(
                        "created_at",
                        "1970-01-01T00:00:00.000Z",
                    ),
                })
                .expect("corrupt cycle commit should encode"),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("cycle corruption should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("commit-a")
            .await
            .expect_err("first-parent cycle should not rebuild forever");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        // Availability is proven from immutable commit-state authority instead
        // of by walking the ancestry, so a cycle whose members are all rooted
        // is no longer reported by the walk's cycle detector: the walk resumes
        // at the first rooted member. It still fails closed, because replaying
        // the cycle against that root cannot reproduce the root immutable
        // authority names for this commit. The walk's `seen_commit_ids` guard
        // remains the termination proof for cycles with no rooted member.
        assert!(
            error.message.contains("first-parent cycle")
                || error
                    .message
                    .contains("disagrees with immutable commit authority"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_missing_head_root_chunk() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            &[row_with_value("row-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        delete_root_chunk_for_test(&storage, "child").await;

        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect_err("diff should fail before missing root chunk repair");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("child root chunk should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should use repaired root chunk");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_repairs_corrupt_head_root_chunk() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "change-base", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            &[row_with_value("row-a", "change-child", "child", "child")],
        )
        .await
        .expect("child root should write");
        corrupt_root_chunk_for_test(&storage, "child").await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("corrupt child root chunk should repair");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repaired root should commit");

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .diff_commits("base", "child", &test_schema_diff_request())
            .await
            .expect("diff should use repaired root chunk");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0]
                .after
                .as_ref()
                .map(|row| row.change_id.to_string()),
            Some(change_id("change-child"))
        );
    }

    #[tokio::test]
    async fn explicit_rebuild_rejects_stale_immutable_root_missing_inherited_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let inherited = row_with_value("row-a", "change-base", "base", "base");
        let child = row_with_value("row-b", "change-child", "child", "child");
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            std::slice::from_ref(&inherited),
        )
        .await
        .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            std::slice::from_ref(&child),
        )
        .await
        .expect("child root should write");
        overwrite_root_with_rows_for_test(&storage, "child", std::slice::from_ref(&child)).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect_err("rebuild must not rewrite stale immutable root authority");
        assert!(
            error
                .message
                .contains("disagrees with immutable commit authority")
        );
    }

    #[tokio::test]
    async fn scan_rows_filters_by_file() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut file_a = row("row-a", "change-a", "commit-1");
        file_a.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        let mut file_b = row("row-b", "change-b", "commit-1");
        file_b.file_id = Some("01920000-0000-7000-8000-0000000000b2.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[file_a, file_b],
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file scan should use primary root")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_single_string_owned().expect("row pk"),
            "row-a"
        );
        assert_eq!(
            rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000a2.json")
        );
    }

    #[tokio::test]
    async fn file_filtered_header_scan_fetches_primary_payload_only_when_requested() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("row-a", "change-a", "commit-1");
        row.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        let expected_snapshot = row.snapshot_content.clone();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let header_rows = reader
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                        )],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["row_pk".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("header scan should use primary root")
            .into_rows();
        let full_rows = reader
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("full scan should fetch primary payload")
            .into_rows();

        assert_eq!(header_rows[0].snapshot_content, None);
        assert_eq!(full_rows[0].snapshot_content, expected_snapshot);
    }

    #[tokio::test]
    async fn null_file_rows_match_null_file_filter() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let row = row("row-a", "change-a", "commit-1");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("null file scan should use primary tree")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_single_string_owned().expect("row pk"),
            "row-a"
        );
    }

    #[tokio::test]
    async fn mixed_null_and_concrete_file_scan_uses_primary_tree() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let null_row = row("row-null", "change-null", "commit-1");
        let mut file_row = row("row-file", "change-file", "commit-2");
        file_row.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&null_row),
        )
        .await
        .expect("parent root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-2",
            Some("commit-1"),
            std::slice::from_ref(&file_row),
        )
        .await
        .expect("child root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-2",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![
                            NullableKeyFilter::Null,
                            NullableKeyFilter::Value(
                                "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                            ),
                        ],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("mixed scan should use primary tree")
            .into_rows();

        let mut row_pks = rows
            .iter()
            .map(|row| row.row_pk.as_single_string_owned().expect("row pk"))
            .collect::<Vec<_>>();
        row_pks.sort();
        assert_eq!(row_pks, vec!["row-file", "row-null"]);
    }

    #[tokio::test]
    async fn file_filtered_header_scan_filters_tombstones_without_payload_sentinel() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut live = row("row-live", "change-live", "commit-1");
        live.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        let mut deleted = tombstone("row-deleted", "change-delete", "commit-1");
        deleted.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &[live, deleted])
            .await
            .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                        )],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["row_pk".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file scan should use primary root")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_single_string_owned().expect("row pk"),
            "row-live"
        );
    }

    #[tokio::test]
    async fn child_root_tombstone_hides_materialized_base_row() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let base = row("row-a", "change-base", "base");
        let delete = tombstone("row-a", "change-delete", "child");
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            std::slice::from_ref(&base),
        )
        .await
        .expect("base root should write");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("base")
            .await
            .expect("base commit root should materialize");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("materialized base should commit");
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("base"),
            std::slice::from_ref(&delete),
        )
        .await
        .expect("child tombstone root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit("child", &test_schema_scan_request())
            .await
            .expect("child scan should apply tombstone over base root")
            .into_rows();

        assert!(rows.is_empty(), "pending tombstone must hide base row");
    }

    #[tokio::test]
    async fn root_scan_keeps_last_mutation_for_duplicate_key() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[
                row_with_value("row-a", "change-a1", "commit-1", "first"),
                row_with_value("row-b", "change-b", "commit-1", "middle"),
                row_with_value("row-a", "change-a2", "commit-1", "second"),
                tombstone("row-c", "change-c1", "commit-1"),
            ],
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("root should scan")
            .into_rows();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.row_pk.as_single_string_owned().expect("row pk"),
                    row.snapshot_content.as_ref().map(ToString::to_string)
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "row-a".to_string(),
                    Some("{\"value\":\"second\"}".to_string())
                ),
                (
                    "row-b".to_string(),
                    Some("{\"value\":\"middle\"}".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn scan_limit_applies_after_tombstone_visibility() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            &[
                tombstone("row-a", "change-delete", "commit-1"),
                row("row-b", "change-live", "commit-1"),
            ],
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        ..Default::default()
                    },
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("limited scan should apply visibility before limit")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_single_string_owned().expect("row pk"),
            "row-b"
        );
    }

    #[tokio::test]
    async fn file_filtered_scan_limit_applies_after_tombstone_visibility() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut deleted = tombstone("row-a", "change-delete", "commit-1");
        deleted.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        let mut live = row("row-b", "change-live", "commit-1");
        live.file_id = Some("01920000-0000-7000-8000-0000000000a2.json".to_string());
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &[deleted, live])
            .await
            .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000a2.json".to_string(),
                        )],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["row_pk".to_string()],
                    },
                    limit: Some(1),
                },
            )
            .await
            .expect("limited file scan should apply visibility before limit")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_single_string_owned().expect("row pk"),
            "row-b"
        );
    }

    #[tokio::test]
    async fn reads_resolve_large_payload_refs_via_change_records() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("row-a", "change-a", "commit-1", &large_value);
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let loaded = reader
            .load_batch_at_commit(
                "commit-1",
                &[TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    row_pk: row.row_pk.clone(),
                    file_id: None,
                }],
            )
            .await
            .expect("row should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("row should exist");
        let scanned = reader
            .scan_batch_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("rows should scan")
            .into_rows();

        assert_eq!(loaded.snapshot_content, row.snapshot_content);
        assert_eq!(scanned[0].snapshot_content, row.snapshot_content);
    }

    #[tokio::test]
    async fn missing_packed_authority_for_live_row_errors_clearly() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let row = row("row-a", "change-a", "commit-1");
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        // Corrupt immutable authority so its snapshot still exposes the live
        // row while its canonical mutation inventory no longer owns the
        // referenced change id.
        let commit_id = CommitId::for_test_label("commit-1");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority read should open");
        let published = storage::load_commit_state_manifest(&read, commit_id)
            .await
            .expect("authority should load")
            .expect("authority should exist");
        let mut corrupt = published.clone();
        corrupt.mutations = Default::default();
        drop(read);
        let mut writes = storage.new_write_set();
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &corrupt)
            .expect("corrupt authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("delete should commit");

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let error = reader
            .scan_batch_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect_err("materialization must reject missing packed authority");
        assert!(
            error.message.contains("missing from owning commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn inline_threshold_boundary_routes_payloads_deterministically() {
        // The hard cut keeps row payloads out of json_store on both sides of
        // the former outer-JSON threshold. Both must read back identically
        // from their typed payloads.
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        // row_with_value wraps values as {"value":"<v>"} (12 framing bytes);
        // size the inner strings so the stored payloads land exactly at the
        // threshold and one byte over.
        let at_len = crate::json_store::JSON_INLINE_MAX_BYTES;
        let over_len = at_len + 1;
        let rows = [
            row_with_value("row-at", "change-at", "commit-1", &"a".repeat(at_len - 12)),
            row_with_value(
                "row-over",
                "change-over",
                "commit-1",
                &"b".repeat(over_len - 12),
            ),
        ];
        let at_threshold = rows[0].snapshot_content.clone().expect("payload");
        let over_threshold = rows[1].snapshot_content.clone().expect("payload");
        assert_eq!(at_threshold.len(), at_len);
        assert_eq!(over_threshold.len(), over_len);
        write_root_for_test(&storage, &tracked_state, "commit-1", None, &rows)
            .await
            .expect("root should write");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let json_refs = [
            crate::json_store::JsonRef::for_content(at_threshold.as_bytes()),
            crate::json_store::JsonRef::for_content(over_threshold.as_bytes()),
        ];
        let stored = crate::json_store::JsonStoreContext::new()
            .load_bytes_many(
                &read,
                crate::json_store::JsonLoadRequestRef {
                    refs: &json_refs,
                    scope: crate::json_store::JsonReadScopeRef::OutOfBand,
                },
            )
            .await
            .expect("json_store boundary rows should load")
            .into_values();
        assert_eq!(stored[0], None, "threshold payload must avoid json_store");
        assert_eq!(
            stored[1], None,
            "over-threshold payload must avoid json_store"
        );

        let mut reader = tracked_state.reader(read);
        let scanned = reader
            .scan_batch_at_commit("commit-1", &test_schema_scan_request())
            .await
            .expect("rows should scan")
            .into_rows();
        let by_pk = |pk: &str| {
            scanned
                .iter()
                .find(|row| row.row_pk.as_single_string().ok() == Some(pk))
                .expect("row should exist")
                .snapshot_content
                .clone()
        };
        assert_eq!(by_pk("row-at").as_deref(), Some(at_threshold.as_str()));
        assert_eq!(by_pk("row-over").as_deref(), Some(over_threshold.as_str()));
    }

    #[tokio::test]
    async fn commit_root_cache_uses_seen_updated_at_not_change_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut row = row("row-a", "change-a", "commit-1");
        row.created_at = "2026-01-01T00:00:00Z".to_string();
        row.updated_at = "2026-01-02T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let loaded = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_batch_at_commit(
                "commit-1",
                &[TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    row_pk: row.row_pk.clone(),
                    file_id: None,
                }],
            )
            .await
            .expect("row should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("row should exist");

        assert_eq!(loaded.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(loaded.updated_at, "2026-01-02T00:00:00.000Z");
    }

    #[tokio::test]
    async fn updates_preserve_first_visible_created_at_across_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut parent = row("row-a", "change-parent", "parent");
        parent.created_at = "2026-01-01T00:00:00Z".to_string();
        parent.updated_at = "2026-01-01T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "parent",
            None,
            std::slice::from_ref(&parent),
        )
        .await
        .expect("parent root should write");

        let mut child = row("row-a", "change-child", "child");
        child.created_at = "2026-01-02T00:00:00Z".to_string();
        child.updated_at = "2026-01-03T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            std::slice::from_ref(&child),
        )
        .await
        .expect("child root should write");

        let key = TrackedStateKey {
            schema_key: child.schema_key.clone(),
            file_id: child.file_id.clone(),
            row_pk: child.row_pk.clone(),
        };
        let loaded = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_batch_at_commit("child", std::slice::from_ref(&key))
            .await
            .expect("child row should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("child row should exist");
        assert_eq!(loaded.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(loaded.updated_at, "2026-01-03T00:00:00.000Z");

        delete_root_chunk_for_test(&storage, "child").await;
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            tracked_state
                .root_rebuilder(&read, &mut writes)
                .rebuild_commit_root_at("child")
                .await
                .expect("child root should rebuild");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("rebuilt child root should commit");
        }

        let rebuilt = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_batch_at_commit("child", &[key])
            .await
            .expect("rebuilt child row should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("rebuilt child row should exist");
        assert_eq!(rebuilt.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(rebuilt.updated_at, "2026-01-03T00:00:00.000Z");
    }

    #[tokio::test]
    async fn selected_column_scans_do_not_materialize_snapshot_when_snapshot_content_is_omitted() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let large_value = "x".repeat(1536);
        let row = row_with_value("row-a", "change-a", "commit-1", &large_value);
        write_root_for_test(
            &storage,
            &tracked_state,
            "commit-1",
            None,
            std::slice::from_ref(&row),
        )
        .await
        .expect("root should write");

        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                "commit-1",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["row_pk".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rows should scan")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].snapshot_content, None);
    }

    async fn seed_merge_roots(
        base_rows: &[MaterializedTrackedStateRow],
        target_rows: &[MaterializedTrackedStateRow],
        source_rows: &[MaterializedTrackedStateRow],
    ) -> (StorageAdapter, TrackedStateContext) {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "base", None, base_rows)
            .await
            .expect("base root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "target",
            Some("base"),
            target_rows,
        )
        .await
        .expect("target root should write");
        write_root_for_test(
            &storage,
            &tracked_state,
            "source",
            Some("base"),
            source_rows,
        )
        .await
        .expect("source root should write");
        (storage, tracked_state)
    }

    fn merge_pick_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.picks
            .iter()
            .map(|entry| {
                entry
                    .identity()
                    .row_pk()
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    fn merge_conflict_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.conflicts
            .iter()
            .map(|entry| {
                entry
                    .identity
                    .row_pk()
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    async fn write_root_for_test(
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
        crate::test_support::stage_tracked_root_from_materialized(
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

    async fn write_rootless_commit_for_test(
        storage: &StorageAdapter,
        commit_id: &str,
        parent_commit_id: &str,
        rows: &[MaterializedTrackedStateRow],
    ) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rootless commit read should open");
        let mut writes = storage.new_write_set();
        crate::test_support::stage_rootless_tracked_commit_from_materialized(
            &mut read,
            &mut writes,
            commit_id,
            Some(parent_commit_id),
            rows,
        )
        .await
        .expect("rootless commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rootless commit should commit");
    }

    #[tokio::test]
    async fn explicit_rebuild_audits_rootless_commit_without_publishing_unreachable_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "base-a", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_rootless_commit_for_test(
            &storage,
            "rootless",
            "base",
            &[row_with_value("row-a", "rootless-a", "rootless", "updated")],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rebuild read should open");
        let mut writes = storage.new_write_set();
        let report = tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("rootless")
            .await
            .expect("rootless state should pass transient rebuild audit");

        assert_eq!(report.commit_id, CommitId::for_test_label("rootless"));
        assert_eq!(writes.stats().staged_puts, 0);
        assert_eq!(writes.stats().staged_deletes, 0);
        assert_eq!(writes.stats().written_bytes, 0);
    }

    #[tokio::test]
    async fn rooted_rebuild_promotes_only_reachable_chunks_from_rootless_ancestor() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-base", "base-a", "base", "base")],
        )
        .await
        .expect("base root should write");
        let rootless_rows = (0..300)
            .map(|index| {
                row_with_value(
                    &format!("row-{index:04}"),
                    &format!("rootless-{index:04}"),
                    "rootless",
                    "rootless",
                )
            })
            .collect::<Vec<_>>();
        write_rootless_commit_for_test(&storage, "rootless", "base", &rootless_rows).await;
        write_rootless_commit_for_test(
            &storage,
            "child",
            "rootless",
            &[row_with_value("row-0150", "child-0150", "child", "child")],
        )
        .await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture rebuild read should open");
        let plans = crate::tracked_state::commit_root_rebuild::load_rebuild_plans_to_nearest_available_root(
            &read,
            "child",
            true,
        )
        .await
        .expect("fixture plans should load");
        let mut fixture_writes = storage.new_write_set();
        let mut fixture_writer = tracked_state.writer(&read, &mut fixture_writes);
        for plan in plans.iter().rev() {
            crate::tracked_state::commit_root_rebuild::stage_rebuild_plan_with_writer(
                &mut fixture_writer,
                plan,
            )
            .await
            .expect("fixture root should stage");
        }
        let child_commit_id = CommitId::for_test_label("child");
        let child_snapshot_root = fixture_writer
            .staged_commit_roots()
            .find(|root| root.commit_id == child_commit_id)
            .cloned()
            .expect("fixture child root should exist");
        drop(fixture_writer);
        let mut child_manifest = storage::load_commit_state_manifest(&read, child_commit_id)
            .await
            .expect("child manifest should load")
            .expect("child manifest should exist");
        child_manifest.replay_debt = crate::tracked_state::CommitStateReplayDebt::default();
        child_manifest.snapshot_root = Some(Box::new(child_snapshot_root));
        storage::stage_resealed_commit_state_manifest_for_test(
            &mut fixture_writes,
            &child_manifest,
        )
        .expect("rooted child authority should reseal");
        drop(read);
        storage
            .commit_write_set(fixture_writes, StorageWriteOptions::default())
            .await
            .expect("rooted child fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reachability read should open");
        let base_root = storage::load_root(&read, "base")
            .await
            .expect("base root should load")
            .expect("base root should exist");
        let child_root = storage::load_root(&read, "child")
            .await
            .expect("child root should load")
            .expect("child root should exist");
        let empty_overlay = storage::TrackedStateChunkOverlay::new();
        let tree = TrackedStateTree::new();
        let base_reachable = tree
            .reachable_chunk_hashes_with_overlay(&read, &empty_overlay, &base_root)
            .await
            .expect("base reachability should load");
        let child_reachable = tree
            .reachable_chunk_hashes_with_overlay(&read, &empty_overlay, &child_root)
            .await
            .expect("child reachability should load");
        drop(read);

        let all_chunks = stored_tree_chunk_hashes_for_test(&storage).await;
        let mut deletes = storage.new_write_set();
        for hash in all_chunks.difference(&base_reachable) {
            deletes.delete(
                storage::TRACKED_STATE_TREE_CHUNK_SPACE,
                crate::storage_adapter::StorageKey(Bytes::copy_from_slice(hash)),
            );
        }
        storage
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("child-only chunks should delete");
        let before = stored_tree_chunk_hashes_for_test(&storage).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rebuild read should open");
        let mut writes = storage.new_write_set();
        tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at("child")
            .await
            .expect("rooted descendant should rebuild through rootless authority");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("reachable rebuilt chunks should commit");

        let after = stored_tree_chunk_hashes_for_test(&storage).await;
        let added = after.difference(&before).copied().collect::<HashSet<_>>();
        let expected = child_reachable
            .difference(&before)
            .copied()
            .collect::<HashSet<_>>();
        assert_eq!(added, expected);
        let rows = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("verification read should open"),
            )
            .scan_batch_at_commit("child", &test_schema_scan_request())
            .await
            .expect("rebuilt child should scan");
        assert_eq!(rows.len(), 301);
        assert_eq!(
            rows.iter()
                .find(|row| row.row_pk() == &RowPk::single("row-0150"))
                .and_then(|row| row.snapshot_content())
                .map(AsRef::as_ref),
            Some("{\"value\":\"child\"}")
        );
    }

    /// Builds `gen -> a1 -> a2 -> a3 -> b1 -> b2 -> b3` where every commit
    /// after `gen` is a rootless bounded-replay layout, then optionally seals
    /// `a3` as a durable interval root so a replay of the `b` interval has a
    /// resume point.
    async fn write_two_interval_history_for_test(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        seal_midpoint: bool,
    ) {
        write_root_for_test(
            storage,
            tracked_state,
            "gen",
            None,
            &[row_with_value("row-gen", "gen-change", "gen", "gen")],
        )
        .await
        .expect("genesis root should write");
        let mut parent = "gen".to_owned();
        for commit in ["a1", "a2", "a3"] {
            write_rootless_commit_for_test(
                storage,
                commit,
                &parent,
                &[
                    row_with_value(
                        &format!("row-{commit}"),
                        &format!("{commit}-change"),
                        commit,
                        commit,
                    ),
                    // Rewrite a shared key in every commit so the interval is
                    // order sensitive rather than a disjoint insert stream.
                    row_with_value("row-shared", &format!("{commit}-shared"), commit, commit),
                ],
            )
            .await;
            parent = commit.to_owned();
        }
        if seal_midpoint {
            seal_rooted_commit_for_test(storage, tracked_state, "a3").await;
        }
        for commit in ["b1", "b2", "b3"] {
            write_rootless_commit_for_test(
                storage,
                commit,
                &parent,
                &[
                    row_with_value(
                        &format!("row-{commit}"),
                        &format!("{commit}-change"),
                        commit,
                        commit,
                    ),
                    row_with_value("row-shared", &format!("{commit}-shared"), commit, commit),
                ],
            )
            .await;
            parent = commit.to_owned();
        }
    }

    /// Publishes the canonical root for `commit_id` into its immutable
    /// commit-state authority, turning a rootless commit into an interval root.
    async fn seal_rooted_commit_for_test(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("seal read should open");
        let plans = crate::tracked_state::commit_root_rebuild::load_rebuild_plans_to_nearest_available_root(
            &read,
            commit_id,
            true,
        )
        .await
        .expect("seal plans should load");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        for plan in plans.iter().rev() {
            crate::tracked_state::commit_root_rebuild::stage_rebuild_plan_with_writer(
                &mut writer,
                plan,
            )
            .await
            .expect("seal root should stage");
        }
        let typed_commit_id = CommitId::for_test_label(commit_id);
        let snapshot_root = writer
            .staged_commit_roots()
            .find(|root| root.commit_id == typed_commit_id)
            .cloned()
            .expect("sealed root should be staged");
        drop(writer);
        let mut manifest = storage::load_commit_state_manifest(&read, typed_commit_id)
            .await
            .expect("sealed manifest should load")
            .expect("sealed manifest should exist");
        manifest.replay_debt = crate::tracked_state::CommitStateReplayDebt::default();
        manifest.snapshot_root = Some(Box::new(snapshot_root));
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &manifest)
            .expect("sealed authority should reseal");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("sealed root should commit");
    }

    /// Replays `commit_id` the way the commit path closes a rootless interval
    /// and returns `(replay-set size, resulting root)`.
    async fn replay_root_for_test(
        storage: &StorageAdapter,
        tracked_state: &TrackedStateContext,
        commit_id: &str,
    ) -> (usize, TrackedStateRootId) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replay read should open");
        let plans = crate::tracked_state::commit_root_rebuild::load_rebuild_plans_to_nearest_available_root(
            &read,
            commit_id,
            true,
        )
        .await
        .expect("replay plans should load");
        let mut writes = storage.new_write_set();
        let mut writer = tracked_state.writer(&read, &mut writes);
        let mut report = None;
        for plan in plans.iter().rev() {
            report = Some(
                crate::tracked_state::commit_root_rebuild::stage_rebuild_plan_with_writer(
                    &mut writer,
                    plan,
                )
                .await
                .expect("replay root should stage"),
            );
        }
        (
            plans.len(),
            report.expect("replay stages at least one root").root_id,
        )
    }

    /// The resume point is the previous interval root named by immutable
    /// commit-state authority. Resuming from it must produce exactly the root
    /// a from-genesis replay of the same history produces.
    #[tokio::test]
    async fn resumed_interval_root_is_identical_to_from_genesis_root() {
        let tracked_state = TrackedStateContext::new();

        let from_genesis = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&from_genesis, &tracked_state, false).await;
        let (genesis_plans, genesis_root) =
            replay_root_for_test(&from_genesis, &tracked_state, "b3").await;

        let resumed = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&resumed, &tracked_state, true).await;
        let (resumed_plans, resumed_root) =
            replay_root_for_test(&resumed, &tracked_state, "b3").await;

        assert_eq!(
            genesis_plans, 6,
            "without a sealed interval root the replay set is the whole history"
        );
        assert_eq!(
            resumed_plans, 3,
            "a sealed interval root bounds the replay set to its own interval"
        );
        assert_eq!(
            resumed_root, genesis_root,
            "resumed replay must produce a byte-identical state root"
        );
    }

    /// Teeth for the assertion above: pointing the resume point at a readable
    /// but wrong root must change the resulting state root, so a silently bad
    /// resume point cannot pass the byte-identity check.
    #[tokio::test]
    async fn byte_identical_root_check_rejects_a_broken_resume_point() {
        let tracked_state = TrackedStateContext::new();

        let from_genesis = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&from_genesis, &tracked_state, false).await;
        let (_, genesis_root) = replay_root_for_test(&from_genesis, &tracked_state, "b3").await;

        let broken = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&broken, &tracked_state, true).await;
        // Repoint `a3`'s immutable root pointer at `gen`'s root: present,
        // readable, content-addressed — and wrong.
        let read = broken
            .begin_read(StorageReadOptions::default())
            .await
            .expect("broken resume read should open");
        let genesis_metadata = storage::load_snapshot_commit_root(&read, "gen")
            .await
            .expect("genesis root metadata should load")
            .expect("genesis root metadata should exist");
        let mut manifest =
            storage::load_commit_state_manifest(&read, CommitId::for_test_label("a3"))
                .await
                .expect("midpoint manifest should load")
                .expect("midpoint manifest should exist");
        let mut broken_root = *manifest
            .snapshot_root
            .clone()
            .expect("midpoint is sealed by the fixture");
        broken_root.root_id = genesis_metadata.root_id.clone();
        manifest.snapshot_root = Some(Box::new(broken_root));
        let mut writes = broken.new_write_set();
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &manifest)
            .expect("broken authority should reseal");
        drop(read);
        broken
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("broken resume point should commit");

        let (broken_plans, broken_root_id) =
            replay_root_for_test(&broken, &tracked_state, "b3").await;
        assert_eq!(broken_plans, 3, "the broken pointer is still readable");
        assert_ne!(
            broken_root_id, genesis_root,
            "a wrong resume point must not reproduce the canonical state root"
        );
    }

    /// A resume point whose chunk closure is damaged must not be resumed from.
    /// Repair stays total: the walk continues past it and the replay rebuilds
    /// the canonical root from the changelog.
    #[tokio::test]
    async fn damaged_resume_point_falls_back_to_total_replay() {
        let tracked_state = TrackedStateContext::new();

        let from_genesis = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&from_genesis, &tracked_state, false).await;
        let (_, genesis_root) = replay_root_for_test(&from_genesis, &tracked_state, "b3").await;

        let damaged = StorageAdapter::new(Memory::new());
        write_two_interval_history_for_test(&damaged, &tracked_state, true).await;
        delete_root_chunk_for_test(&damaged, "a3").await;

        let (damaged_plans, damaged_root) =
            replay_root_for_test(&damaged, &tracked_state, "b3").await;
        assert_eq!(
            damaged_plans, 6,
            "a damaged interval root must not bound the replay set"
        );
        assert_eq!(
            damaged_root, genesis_root,
            "total replay must still produce the canonical state root"
        );
    }

    /// The commit-path availability probe proves the resume point is
    /// *addressable*, not that its whole chunk closure is intact; explicit
    /// repair still proves the whole closure. Pins both halves, because the
    /// difference between them is the entire cost argument: the commit path
    /// walks one root-to-leaf path, repair walks every row.
    ///
    /// This sweeps every non-root chunk rather than damaging one of them.
    /// A single representative cannot express the property: `Addressable`
    /// walks root -> leftmost child -> leaf, so damage *on* that path
    /// legitimately defeats the bounded probe too, and only damage *off* it
    /// separates the two proofs. Measured on this fixture, 2 of the 40
    /// candidates sit on the bounded path. Selecting one of them by content
    /// hash therefore leaves a ~1-in-20 chance that any future change to the
    /// tree's contents flips this test to a permanent failure that reads like
    /// a commit-root-rebuild regression rather than fixture drift. (An earlier
    /// version selected by `HashSet` iteration order, which made the same
    /// mistake nondeterministically and flaked in roughly 5% of runs.)
    /// Sweeping pins the whole partition instead of guessing a member of it.
    #[tokio::test]
    async fn availability_proof_is_bounded_on_commit_and_total_on_repair() {
        use crate::tracked_state::commit_root_rebuild::RootAvailabilityProof;

        // Wide enough that the tree is more than one chunk, so "the root chunk"
        // and "the closure" are actually different sets.
        let rows = (0..4000)
            .map(|index| {
                row_with_value(
                    &format!("row-{index:06}"),
                    &format!("gen-change-{index}"),
                    "gen",
                    "gen",
                )
            })
            .collect::<Vec<_>>();

        // The damage is a delete, so every candidate needs an undamaged fixture
        // of its own.
        async fn seeded_fixture(rows: &[MaterializedTrackedStateRow]) -> StorageAdapter {
            let tracked_state = TrackedStateContext::new();
            let storage = StorageAdapter::new(Memory::new());
            write_root_for_test(&storage, &tracked_state, "gen", None, rows)
                .await
                .expect("wide genesis root should write");
            write_rootless_commit_for_test(
                &storage,
                "a1",
                "gen",
                &[row_with_value("row-a1", "a1-change", "a1", "a1")],
            )
            .await;
            storage
        }

        let storage = seeded_fixture(&rows).await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("root read should open");
        let root_id = storage::load_root(&read, "gen")
            .await
            .expect("genesis root should load")
            .expect("genesis root should exist");
        drop(read);
        let root_bytes = *root_id.as_bytes();
        let mut candidates = stored_tree_chunk_hashes_for_test(&storage)
            .await
            .into_iter()
            .filter(|chunk| chunk != &root_bytes)
            .collect::<Vec<_>>();
        // Sorted so the sweep order, and any failure message, is reproducible.
        candidates.sort_unstable();
        assert!(
            candidates.len() > 1,
            "fixture must build a multi-chunk tree, got {} non-root chunks",
            candidates.len()
        );
        drop(storage);

        let mut resumed_from_damaged_root = 0usize;
        let mut rejected_damaged_root = 0usize;
        for candidate in &candidates {
            let storage = seeded_fixture(&rows).await;
            let mut writes = storage.new_write_set();
            writes.delete(
                storage::TRACKED_STATE_TREE_CHUNK_SPACE,
                crate::storage_adapter::StorageKey(Bytes::copy_from_slice(candidate)),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("chunk delete should commit");

            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("probe read should open");
            let bounded = crate::tracked_state::commit_root_rebuild::
                load_rebuild_plans_to_nearest_available_root_with_proof(
                    &read,
                    "a1",
                    true,
                    RootAvailabilityProof::Addressable,
                )
                .await
                .expect("bounded probe should load plans");
            let total = crate::tracked_state::commit_root_rebuild::
                load_rebuild_plans_to_nearest_available_root_with_proof(
                    &read,
                    "a1",
                    true,
                    RootAvailabilityProof::Complete,
                )
                .await
                .expect("total probe should load plans");

            // Repair distrusts the chunk plane, so every damaged closure makes
            // it walk back past the resume point — on the bounded path or not.
            assert_eq!(
                total.len(),
                2,
                "explicit repair must reject a resume point with a damaged closure \
                 and replay past it (chunk {candidate:02x?})"
            );
            match bounded.len() {
                1 => resumed_from_damaged_root += 1,
                2 => rejected_damaged_root += 1,
                other => panic!(
                    "bounded probe returned {other} plans for chunk {candidate:02x?}; \
                     expected 1 (resumed) or 2 (walked back)"
                ),
            }
        }

        // The commit path does not re-derive closure completeness: damage its
        // one root-to-leaf walk never touches leaves the root resumable.
        assert!(
            resumed_from_damaged_root > 0,
            "the commit path must resume from a root whose closure is damaged off \
             the bounded path"
        );
        // ...and the bounded probe is not vacuous: damage *on* that walk still
        // rejects the root.
        assert!(
            rejected_damaged_root > 0,
            "the bounded probe must still reject a root whose bounded path is damaged"
        );
        // The whole cost argument: the bounded probe reads one root-to-leaf
        // path, not the closure. If it read the closure, every chunk would
        // reject.
        assert!(
            rejected_damaged_root < candidates.len(),
            "the bounded probe walks one root-to-leaf path, so it cannot notice \
             every chunk in the closure"
        );
    }

    async fn stored_tree_chunk_hashes_for_test(
        storage: &StorageAdapter,
    ) -> HashSet<[u8; crate::tracked_state::types::TRACKED_STATE_HASH_BYTES]> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("chunk inventory read should open");
        let mut cursor = read
            .begin_scan(
                storage::TRACKED_STATE_TREE_CHUNK_SPACE,
                crate::storage_adapter::StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                crate::storage_adapter::StorageBeginScanOptions {
                    projection: crate::storage_adapter::StorageCoreProjection::KeyOnly,
                    ..crate::storage_adapter::StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("chunk inventory scan should begin");
        let (chunk, chunk_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("chunk inventory should scan")
            .into_parts();
        assert!(!chunk_has_more, "test chunk inventory must fit one page");
        chunk
            .into_iter()
            .map(|entry| {
                entry
                    .key
                    .0
                    .as_ref()
                    .try_into()
                    .expect("tracked-state chunk keys are fixed hashes")
            })
            .collect()
    }

    #[tokio::test]
    async fn first_parent_generation_write_set_deduplicates_touched_identities() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(
            &storage,
            &tracked_state,
            "base",
            None,
            &[row_with_value("row-a", "base-a", "base", "base")],
        )
        .await
        .expect("base root should write");
        write_rootless_commit_for_test(
            &storage,
            "generation-1",
            "base",
            &[
                row_with_value("row-a", "generation-1-a", "generation-1", "middle"),
                row_with_value("row-b", "generation-1-b", "generation-1", "created"),
            ],
        )
        .await;
        write_rootless_commit_for_test(
            &storage,
            "generation-2",
            "generation-1",
            &[
                // The endpoint payload returns to the base value, but stale
                // admission must still know that this identity was touched.
                row_with_value("row-a", "generation-2-a", "generation-2", "base"),
                row_with_value("row-c", "generation-2-c", "generation-2", "created"),
            ],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("write-set read should open");
        let identities = tracked_state
            .reader(read)
            .changed_identities_in_first_parent_interval("base", "generation-2")
            .await
            .expect("generation write set should load")
            .expect("rootless first-parent interval should be supported");
        assert_eq!(
            identities
                .iter()
                .map(|identity| { identity.row_pk().as_single_string().unwrap().to_owned() })
                .collect::<Vec<_>>(),
            ["row-a", "row-b", "row-c"]
        );
    }

    #[tokio::test]
    #[ignore = "release-only generation write-set benchmark probe"]
    async fn generation_write_set_benchmark_probe() {
        let rows = std::env::var("LIX_GENERATION_WRITE_SET_BENCH_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(5_000);
        let rounds = std::env::var("LIX_GENERATION_WRITE_SET_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(8);
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let base_rows = (0..rows)
            .map(|index| {
                row_with_value(
                    &format!("row-{index:05}"),
                    &format!("base-change-{index:05}"),
                    "base",
                    "base",
                )
            })
            .collect::<Vec<_>>();
        write_root_for_test(&storage, &tracked_state, "base", None, &base_rows)
            .await
            .expect("benchmark base should write");
        let changed_rows = (0..rows)
            .map(|index| {
                row_with_value(
                    &format!("row-{index:05}"),
                    &format!("changed-{index:05}"),
                    "after",
                    "after",
                )
            })
            .collect::<Vec<_>>();
        write_rootless_commit_for_test(&storage, "after", "base", &changed_rows).await;

        let mut write_set_samples = Vec::with_capacity(rounds);
        let mut diff_samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("write-set benchmark read should open");
            let started = Instant::now();
            let identities = tracked_state
                .reader(read)
                .changed_identities_in_first_parent_interval("base", "after")
                .await
                .expect("generation write set should load")
                .expect("benchmark interval should be rootless");
            write_set_samples.push(started.elapsed());
            assert_eq!(identities.len(), rows);

            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("diff benchmark read should open");
            let started = Instant::now();
            let diff = tracked_state
                .reader(read)
                .diff_commits("base", "after", &TrackedStateDiffRequest::default())
                .await
                .expect("general diff should load");
            diff_samples.push(started.elapsed());
            assert_eq!(diff.entries.len(), rows);
        }
        write_set_samples.sort_unstable();
        diff_samples.sort_unstable();
        let p50 = |samples: &[std::time::Duration]| samples[(samples.len() - 1) / 2];
        let write_set_p50 = p50(&write_set_samples);
        let diff_p50 = p50(&diff_samples);
        println!(
            "{}",
            serde_json::json!({
                "schema": "lix.generation-write-set.v1",
                "rows": rows,
                "rounds": rounds,
                "general_diff_p50_us": diff_p50.as_micros(),
                "generation_write_set_p50_us": write_set_p50.as_micros(),
                "speedup": diff_p50.as_secs_f64() / write_set_p50.as_secs_f64(),
            })
        );
        assert!(
            write_set_p50 < diff_p50,
            "generation write-set discovery should beat general endpoint diffing"
        );
    }

    #[tokio::test]
    async fn large_rootless_exact_batch_uses_encoded_replay_and_preserves_input_slots() {
        const COMMIT_ID: &str = "rootless-bulk-exact";
        const ROW_COUNT: usize = HISTORICAL_ENCODED_LOOKUP_MIN_ROWS;
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "base", None, &[])
            .await
            .expect("base root should write");

        let rows = (0..ROW_COUNT)
            .map(|index| {
                row_with_value(
                    &format!("row-{index:03}"),
                    &format!("rootless-bulk-change-{index:03}"),
                    COMMIT_ID,
                    &format!("value-{index:03}"),
                )
            })
            .collect::<Vec<_>>();
        write_rootless_commit_for_test(&storage, COMMIT_ID, "base", &rows).await;

        let key_for_row = |row: &MaterializedTrackedStateRow| TrackedStateKey {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            row_pk: row.row_pk.clone(),
        };
        let duplicate_key = key_for_row(&rows[ROW_COUNT / 2]);
        let mut requested = Vec::with_capacity(ROW_COUNT + 3);
        requested.push(duplicate_key.clone());
        requested.push(TrackedStateKey {
            schema_key: "test_schema".to_owned(),
            file_id: None,
            row_pk: RowPk::single("missing-row"),
        });
        requested.extend(rows.iter().rev().map(key_for_row));
        requested.push(duplicate_key);

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("bulk historical read should open"),
        );
        assert!(reader.point_value_cache.is_empty());
        let exact = reader
            .load_projected_batch_at_commit(COMMIT_ID, &requested, &ChangeRecordProjection::full())
            .await
            .expect("large rootless exact batch should materialize");

        assert_eq!(exact.len(), requested.len());
        assert!(exact.row(1).is_none(), "missing input must retain its slot");
        for (offset, expected) in rows.iter().rev().enumerate() {
            let actual = exact
                .row(offset + 2)
                .expect("every committed input should remain present");
            assert_eq!(actual.row_pk(), &expected.row_pk);
            assert_eq!(
                actual.snapshot_content().map(SharedStr::as_str),
                expected.snapshot_content.as_deref()
            );
        }
        let first_duplicate = exact.row(0).expect("first duplicate should be present");
        let last_duplicate = exact
            .row(exact.len() - 1)
            .expect("last duplicate should be present");
        assert!(
            std::ptr::eq(first_duplicate.row_pk(), last_duplicate.row_pk()),
            "duplicate input slots should select the same materialized batch row"
        );
        assert!(
            reader.point_value_cache.is_empty(),
            "64+ unique keys must stay on encoded bulk replay instead of populating the row-owned point cache"
        );
        assert!(
            reader.commit_delta_value_cache.is_empty(),
            "encoded bulk replay must not populate the row-owned commit-delta cache"
        );
        assert!(
            reader
                .tree
                .load_root(&reader.store, COMMIT_ID)
                .await
                .expect("root probe should succeed")
                .is_none(),
            "the batch must resolve from rootless deltas, not a rebuilt durable tree"
        );
    }

    #[tokio::test]
    async fn large_rootless_scan_materializes_from_flat_replay_without_row_caches_or_tree() {
        const COMMIT_ID: &str = "rootless-flat-scan";
        const ROW_COUNT: usize = HISTORICAL_ENCODED_LOOKUP_MIN_ROWS;
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        write_root_for_test(&storage, &tracked_state, "base", None, &[])
            .await
            .expect("base root should write");
        let rows = (0..ROW_COUNT)
            .map(|index| {
                row_with_value(
                    &format!("scan-row-{index:03}"),
                    &format!("rootless-scan-change-{index:03}"),
                    COMMIT_ID,
                    &format!("scan-value-{index:03}"),
                )
            })
            .collect::<Vec<_>>();
        write_rootless_commit_for_test(&storage, COMMIT_ID, "base", &rows).await;

        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("rootless scan read should open"),
        );
        let batch = reader
            .scan_batch_at_commit(
                COMMIT_ID,
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_owned()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("large rootless scan should materialize");

        assert_eq!(batch.len(), ROW_COUNT);
        assert!(reader.point_value_cache.is_empty());
        assert!(reader.commit_delta_value_cache.is_empty());
        assert!(
            reader
                .tree
                .load_root(&reader.store, COMMIT_ID)
                .await
                .expect("root probe should succeed")
                .is_none(),
            "flat replay must not rebuild or consult a head tree"
        );
    }

    #[tokio::test]
    async fn rootless_diff_applies_file_cascade_before_same_commit_explicit_row() {
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000624";
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut descriptor = row(FILE_ID, "descriptor-create", "initial");
        descriptor.row_pk =
            RowPk::uuid_from_canonical(FILE_ID).expect("fixture file ID is canonical");
        descriptor.schema_key = FILE_DESCRIPTOR_SCHEMA_KEY.to_string();
        descriptor.file_id = Some(FILE_ID.to_string());
        let mut semantic = row("line-1", "semantic-create", "initial");
        semantic.file_id = Some(FILE_ID.to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "initial",
            None,
            &[descriptor.clone(), semantic.clone()],
        )
        .await
        .expect("initial root should write");

        let mut descriptor_delete = descriptor;
        descriptor_delete.snapshot_content = None;
        descriptor_delete.deleted = true;
        descriptor_delete.change_id = ChangeId::for_test_label("descriptor-delete");
        descriptor_delete.commit_id = CommitId::for_test_label("mixed");
        descriptor_delete.updated_at = "2026-01-02T00:00:00Z".to_string();
        let mut semantic_edit = semantic;
        semantic_edit.snapshot_content = Some(r#"{"value":"explicit"}"#.into());
        semantic_edit.change_id = ChangeId::for_test_label("semantic-explicit");
        semantic_edit.commit_id = CommitId::for_test_label("mixed");
        semantic_edit.updated_at = "2026-01-02T00:00:00Z".to_string();
        write_rootless_commit_for_test(
            &storage,
            "mixed",
            "initial",
            &[descriptor_delete, semantic_edit.clone()],
        )
        .await;

        let diff = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("rootless diff read should open"),
            )
            .diff_commits(
                "initial",
                "mixed",
                &TrackedStateDiffRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![semantic_edit.schema_key],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rootless mixed cascade/explicit diff should succeed");

        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].kind,
            crate::tracked_state::TrackedStateDiffKind::Modified
        );
        let after = diff.entries[0]
            .after
            .as_ref()
            .expect("explicit semantic row should be the endpoint");
        assert!(!after.deleted);
        assert_eq!(
            after.change_id,
            ChangeId::for_test_label("semantic-explicit")
        );
    }

    #[tokio::test]
    async fn rootless_descriptor_cascade_drives_point_scan_diff_and_merge_reads() {
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000521";
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut descriptor = row(FILE_ID, "descriptor-create", "initial");
        descriptor.row_pk =
            RowPk::uuid_from_canonical(FILE_ID).expect("fixture file ID is canonical");
        descriptor.schema_key = FILE_DESCRIPTOR_SCHEMA_KEY.to_string();
        descriptor.file_id = Some(FILE_ID.to_string());
        let mut semantic = row("line-1", "semantic-create", "initial");
        semantic.file_id = Some(FILE_ID.to_string());
        write_root_for_test(
            &storage,
            &tracked_state,
            "initial",
            None,
            &[descriptor.clone(), semantic.clone()],
        )
        .await
        .expect("initial root should write");

        let mut descriptor_delete = descriptor.clone();
        descriptor_delete.snapshot_content = None;
        descriptor_delete.deleted = true;
        descriptor_delete.change_id = ChangeId::for_test_label("descriptor-delete");
        descriptor_delete.commit_id = CommitId::for_test_label("delete");
        descriptor_delete.updated_at = "2026-01-02T00:00:00Z".to_string();
        write_rootless_commit_for_test(
            &storage,
            "delete",
            "initial",
            std::slice::from_ref(&descriptor_delete),
        )
        .await;

        let semantic_key = TrackedStateKey {
            schema_key: semantic.schema_key.clone(),
            file_id: semantic.file_id.clone(),
            row_pk: semantic.row_pk.clone(),
        };
        let mut reader = tracked_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("rootless read should open"),
        );
        let point = reader
            .load_batch_at_commit("delete", std::slice::from_ref(&semantic_key))
            .await
            .expect("rootless cascade point read should succeed")
            .into_rows()
            .pop()
            .flatten()
            .expect("cascaded semantic row should remain addressable");
        assert!(point.deleted);
        assert_eq!(point.change_id, descriptor_delete.change_id);
        assert_eq!(point.created_at, "2026-01-01T00:00:00.000Z");

        let scan = reader
            .scan_batch_at_commit(
                "delete",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![semantic.schema_key.clone()],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        include_tombstones: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rootless cascade scan should succeed")
            .into_rows();
        assert_eq!(scan.len(), 1);
        assert!(scan[0].deleted);
        assert_eq!(scan[0].change_id, descriptor_delete.change_id);

        let cached_point_rows_before_diff = reader.commit_delta_value_cache.len();
        let diff = reader
            .diff_commits(
                "initial",
                "delete",
                &TrackedStateDiffRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![semantic.schema_key.clone()],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rootless cascade diff should succeed");
        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].kind,
            crate::tracked_state::TrackedStateDiffKind::Removed
        );
        assert_eq!(
            reader.commit_delta_value_cache.len(),
            cached_point_rows_before_diff,
            "rootless diff scans must not promote an owned key/value row per mutation"
        );
        drop(reader);

        semantic.change_id = ChangeId::for_test_label("semantic-edit");
        semantic.commit_id = CommitId::for_test_label("source");
        semantic.updated_at = "2026-01-02T00:00:00Z".to_string();
        semantic.snapshot_content = Some(r#"{"value":"source"}"#.into());
        write_rootless_commit_for_test(
            &storage,
            "source",
            "initial",
            std::slice::from_ref(&semantic),
        )
        .await;

        let plan = tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("merge read should open"),
            )
            .plan_merge(
                "initial",
                "delete",
                "source",
                &TrackedStateDiffRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![semantic.schema_key],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rootless delete-vs-edit merge should plan");
        assert!(plan.picks.is_empty());
        assert_eq!(merge_conflict_ids(&plan), vec!["line-1"]);
        assert!(
            plan.conflicts[0]
                .target
                .after
                .as_ref()
                .expect("target cascade should have an after row")
                .deleted
        );
    }

    #[tokio::test]
    async fn file_descriptor_delete_cascade_survives_root_rebuild_and_recreate() {
        const FILE_ID: &str = "01920000-0000-7000-8000-000000000522";
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let mut descriptor = row(FILE_ID, "descriptor-create", "initial");
        descriptor.row_pk =
            RowPk::uuid_from_canonical(FILE_ID).expect("fixture file ID is canonical");
        descriptor.schema_key = FILE_DESCRIPTOR_SCHEMA_KEY.to_string();
        descriptor.file_id = Some(FILE_ID.to_string());
        let mut semantic = row("line-1", "semantic-create", "initial");
        semantic.file_id = Some(FILE_ID.to_string());
        let mut retired = row("retired-blob", "retired-create", "initial");
        retired.file_id = Some(FILE_ID.to_string());
        let mut schema_registration = row("test_schema", "schema-create", "initial");
        schema_registration.schema_key = REGISTERED_SCHEMA_KEY.to_string();
        schema_registration.file_id = None;
        write_root_for_test(
            &storage,
            &tracked_state,
            "initial",
            None,
            &[
                descriptor.clone(),
                semantic,
                retired.clone(),
                schema_registration,
            ],
        )
        .await
        .expect("initial root should write");

        retired.snapshot_content = None;
        retired.deleted = true;
        retired.change_id = ChangeId::for_test_label("retired-delete");
        retired.commit_id = CommitId::for_test_label("prior-delete");
        retired.updated_at = "2026-01-02T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "prior-delete",
            Some("initial"),
            std::slice::from_ref(&retired),
        )
        .await
        .expect("prior file-scoped tombstone root should write");

        let mut descriptor_delete = descriptor.clone();
        descriptor_delete.snapshot_content = None;
        descriptor_delete.deleted = true;
        descriptor_delete.change_id = ChangeId::for_test_label("descriptor-delete");
        descriptor_delete.commit_id = CommitId::for_test_label("delete");
        descriptor_delete.updated_at = "2026-01-03T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "delete",
            Some("prior-delete"),
            &[descriptor_delete],
        )
        .await
        .expect("delete root should write");

        descriptor.change_id = ChangeId::for_test_label("descriptor-recreate");
        descriptor.commit_id = CommitId::for_test_label("recreate");
        descriptor.updated_at = "2026-01-04T00:00:00Z".to_string();
        write_root_for_test(
            &storage,
            &tracked_state,
            "recreate",
            Some("delete"),
            &[descriptor],
        )
        .await
        .expect("recreate root should write");

        for commit_id in ["prior-delete", "delete", "recreate"] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("rebuild read should open");
            let mut writes = storage.new_write_set();
            tracked_state
                .root_rebuilder(&read, &mut writes)
                .rebuild_commit_root_at(commit_id)
                .await
                .expect("descriptor cascade root should rebuild and pass its staged audit");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("rebuilt descriptor cascade root should commit");
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let mut reader = tracked_state.reader(read);
        reader
            .validate_commit_root_against_changelog("delete")
            .await
            .expect("delete root cascade should pass the full changelog audit");
        reader
            .validate_commit_root_against_changelog("recreate")
            .await
            .expect("inherited cascade should pass the full changelog audit");
        let diff = reader
            .diff_commits(
                "initial",
                "delete",
                &TrackedStateDiffRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        row_pks: vec![RowPk::single("line-1")],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("diff should recognize descriptor-driven semantic tombstones");
        assert_eq!(diff.entries.len(), 1);
        assert_eq!(
            diff.entries[0].kind,
            crate::tracked_state::TrackedStateDiffKind::Removed
        );
        let rows = reader
            .scan_batch_at_commit(
                "recreate",
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["test_schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value(FILE_ID.to_string())],
                        row_pks: vec![RowPk::single("line-1")],
                        include_tombstones: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("recreated root should scan")
            .into_rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].deleted);
        assert_eq!(
            rows[0].change_id,
            ChangeId::for_test_label("descriptor-delete")
        );
    }

    async fn delete_root_chunk_for_test(storage: &StorageAdapter, commit_id: &str) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let root_id = storage::load_root(&read, commit_id)
            .await
            .expect("root metadata should load")
            .expect("root metadata should exist");
        let mut writes = storage.new_write_set();
        writes.delete(
            storage::TRACKED_STATE_TREE_CHUNK_SPACE,
            crate::storage_adapter::StorageKey(Bytes::copy_from_slice(root_id.as_bytes())),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root chunk delete should commit");
    }

    async fn corrupt_root_chunk_for_test(storage: &StorageAdapter, commit_id: &str) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let root_id = storage::load_root(&read, commit_id)
            .await
            .expect("root metadata should load")
            .expect("root metadata should exist");
        let mut writes = storage.new_write_set();
        writes.put(
            storage::TRACKED_STATE_TREE_CHUNK_SPACE,
            crate::storage_adapter::StorageKey(Bytes::copy_from_slice(root_id.as_bytes())),
            b"corrupt tracked-state root chunk".as_slice(),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("root chunk corruption should commit");
    }

    async fn overwrite_root_with_rows_for_test(
        storage: &StorageAdapter,
        commit_id: &str,
        rows: &[MaterializedTrackedStateRow],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mutations = rows
            .iter()
            .map(|row| {
                let key = TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    file_id: row.file_id.clone(),
                    row_pk: row.row_pk.clone(),
                };
                let value = TrackedStateIndexValue {
                    change_id: row.change_id.clone(),
                    commit_id: row.commit_id.clone(),
                    deleted: row.deleted,
                    created_at: crate::common::LixTimestamp::expect_parse(
                        "created_at",
                        &row.created_at,
                    ),
                    updated_at: crate::common::LixTimestamp::expect_parse(
                        "updated_at",
                        &row.updated_at,
                    ),
                };
                TrackedStateMutation::put_encoded(
                    encode_key(&key),
                    crate::tracked_state::codec::encode_value(&value),
                )
            })
            .collect::<Vec<_>>();
        let result = TrackedStateTree::new()
            .apply_mutations(
                &read,
                &mut writes,
                None,
                TrackedStateMutationBatch::from_shared(mutations),
                Some(commit_id),
            )
            .await
            .expect("stale root should write");
        let typed_commit_id = CommitId::for_test_label(commit_id);
        let mut stale_root = TrackedStateCommitRoot {
            commit_id: typed_commit_id,
            root_id: result.root_id,
            parent_roots: Vec::new(),
            changed_key_count: rows.len() as u64,
            row_count_estimate: result.row_count as u64,
            tree_height: result.tree_height as u32,
            complete_state_fence: false,
        };
        let published = storage::load_published_commit_state_manifest(&read, typed_commit_id)
            .await
            .expect("commit-state authority should load")
            .expect("root fixture should publish commit-state authority");
        stale_root.parent_roots =
            storage::load_snapshot_commit_root(&read, &typed_commit_id.to_string())
                .await
                .expect("published root should load")
                .map(|root| root.parent_roots)
                .unwrap_or_default();
        let mut stale = (*published).clone();
        stale.snapshot_root = Some(Box::new(stale_root));
        storage::stage_resealed_commit_state_manifest_for_test(&mut writes, &stale)
            .expect("stale authority should encode");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("stale root overwrite should commit");
    }

    fn test_schema_scan_request() -> TrackedStateScanRequest {
        TrackedStateScanRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: vec!["test_schema".to_string()],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_schema_diff_request() -> TrackedStateDiffRequest {
        TrackedStateDiffRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: vec!["test_schema".to_string()],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn tombstone(row_pk: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        let mut row = row(row_pk, change_id, commit_id);
        row.snapshot_content = None;
        row
    }

    fn row(row_pk: &str, change_id: &str, commit_id: &str) -> MaterializedTrackedStateRow {
        row_with_value(row_pk, change_id, commit_id, "value")
    }

    fn row_with_value(
        row_pk: &str,
        change_id: &str,
        commit_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            row_pk: RowPk::single(row_pk),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            decoded_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(commit_id),
        }
    }

    fn delta_from_materialized_row(row: &MaterializedTrackedStateRow) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            schema_key: &row.schema_key,
            file_id: row.file_id.as_deref(),
            row_pk: &row.row_pk,
            change_id: row.change_id,
            commit_id: row.commit_id,
            deleted: row.snapshot_content.is_none(),
            created_at: crate::common::LixTimestamp::expect_parse("created_at", &row.created_at),
            updated_at: crate::common::LixTimestamp::expect_parse("updated_at", &row.updated_at),
        }
    }

    fn encoded_key_from_materialized_row(row: &MaterializedTrackedStateRow) -> Vec<u8> {
        crate::tracked_state::codec::encode_key_ref(TrackedStateKeyRef {
            schema_key: &row.schema_key,
            file_id: row.file_id.as_deref(),
            row_pk: &row.row_pk,
        })
    }
}
