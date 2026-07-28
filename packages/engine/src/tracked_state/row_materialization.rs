use std::collections::HashMap;
#[cfg(test)]
use std::mem::size_of;
use std::ops::Range;

use ahash::RandomState;
use bytes::Bytes;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeRecordProjection, CommitId, MaterializedChangePayload,
    materialize_change_payloads,
};
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef};

#[derive(Debug, Default)]
struct TrackedStateStringDictionary {
    bytes: Bytes,
    ranges: Vec<Range<u32>>,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl TrackedStateStringDictionary {
    fn get(&self, ordinal: u32) -> &str {
        let range = self
            .ranges
            .get(ordinal as usize)
            .expect("tracked-state string ordinal belongs to this batch");
        let range = range.start as usize..range.end as usize;
        // SAFETY: the builder appends complete `str` values and records their
        // exact boundaries in one immutable allocation.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[range]) }
    }

    fn shared(&self, ordinal: u32) -> SharedStr {
        let value = self.get(ordinal);
        SharedStr::from_utf8_slice(self.bytes.clone(), value)
            .expect("tracked-state dictionary value points into its byte arena")
    }
}

#[derive(Debug)]
struct MaterializedTrackedStateDescriptor {
    entity_pk: EntityPk,
    schema_key: u32,
    file_id: Option<u32>,
    snapshot_content: Option<SharedStr>,
    metadata: Option<SharedStr>,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    change_id: ChangeId,
    commit_id: CommitId,
}

/// Typed owner for historical tracked-state materialization.
///
/// Identity strings are dictionary encoded into one immutable byte arena,
/// fixed row metadata occupies one contiguous descriptor column, timestamps
/// and ids stay typed, and JSON slots retain their existing shared buffers.
/// The legacy owned row is constructed only by an explicit terminal adapter.
#[derive(Debug, Default)]
pub(crate) struct MaterializedTrackedStateBatch {
    strings: TrackedStateStringDictionary,
    rows: Vec<MaterializedTrackedStateDescriptor>,
}

impl MaterializedTrackedStateBatch {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn row(&self, index: usize) -> MaterializedTrackedStateRowRef<'_> {
        assert!(
            index < self.rows.len(),
            "tracked-state materialized row ordinal out of bounds"
        );
        MaterializedTrackedStateRowRef { batch: self, index }
    }

    pub(crate) fn iter(&self) -> MaterializedTrackedStateBatchIter<'_> {
        MaterializedTrackedStateBatchIter {
            batch: self,
            next: 0,
        }
    }

    pub(crate) fn into_rows(self) -> Vec<MaterializedTrackedStateRow> {
        self.iter()
            .map(MaterializedTrackedStateRowRef::to_owned)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn from_rows(rows: Vec<MaterializedTrackedStateRow>) -> Result<Self, LixError> {
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(rows.len());
        for row in rows {
            let created_at = LixTimestamp::parse(&row.created_at).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid test tracked-state created_at: {error}"),
                )
            })?;
            let updated_at = LixTimestamp::parse(&row.updated_at).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid test tracked-state updated_at: {error}"),
                )
            })?;
            builder.push(
                TrackedStateKey {
                    entity_pk: row.entity_pk,
                    schema_key: row.schema_key,
                    file_id: row.file_id,
                },
                TrackedStateIndexValue {
                    change_id: row.change_id,
                    commit_id: row.commit_id,
                    deleted: row.deleted,
                    created_at,
                    updated_at,
                },
                row.snapshot_content,
                row.metadata,
            );
        }
        Ok(builder.finish())
    }

    #[cfg(test)]
    pub(crate) fn dictionary_entry_count(&self) -> usize {
        self.strings.ranges.len()
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self, threshold: usize) -> usize {
        [
            self.rows.capacity() * size_of::<MaterializedTrackedStateDescriptor>(),
            self.strings.bytes.len(),
            self.strings.ranges.len() * size_of::<Range<u32>>(),
        ]
        .into_iter()
        .filter(|bytes| *bytes >= threshold)
        .count()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_allocation_count(&self) -> usize {
        self.strings.arena_allocation_count
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_large_allocation_count(&self) -> usize {
        self.strings.arena_large_allocation_count
    }
}

/// Borrowed historical row view over one shared materialization batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaterializedTrackedStateRowRef<'a> {
    batch: &'a MaterializedTrackedStateBatch,
    index: usize,
}

impl<'a> MaterializedTrackedStateRowRef<'a> {
    fn descriptor(self) -> &'a MaterializedTrackedStateDescriptor {
        &self.batch.rows[self.index]
    }

    pub(crate) fn entity_pk(self) -> &'a EntityPk {
        &self.descriptor().entity_pk
    }

    pub(crate) fn schema_key(self) -> &'a str {
        self.batch.strings.get(self.descriptor().schema_key)
    }

    pub(crate) fn schema_key_shared(self) -> SharedStr {
        self.batch.strings.shared(self.descriptor().schema_key)
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.descriptor()
            .file_id
            .map(|ordinal| self.batch.strings.get(ordinal))
    }

    pub(crate) fn file_id_shared(self) -> Option<SharedStr> {
        self.descriptor()
            .file_id
            .map(|ordinal| self.batch.strings.shared(ordinal))
    }

    pub(crate) fn snapshot_content(self) -> Option<&'a SharedStr> {
        self.descriptor().snapshot_content.as_ref()
    }

    pub(crate) fn metadata(self) -> Option<&'a SharedStr> {
        self.descriptor().metadata.as_ref()
    }

    pub(crate) fn deleted(self) -> bool {
        self.descriptor().deleted
    }

    pub(crate) fn created_at(self) -> LixTimestamp {
        self.descriptor().created_at
    }

    pub(crate) fn updated_at(self) -> LixTimestamp {
        self.descriptor().updated_at
    }

    pub(crate) fn change_id(self) -> ChangeId {
        self.descriptor().change_id
    }

    pub(crate) fn commit_id(self) -> CommitId {
        self.descriptor().commit_id
    }

    /// Converts into the legacy DTO only at a terminal compatibility boundary.
    pub(crate) fn to_owned(self) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_pk: self.entity_pk().clone(),
            schema_key: self.schema_key().to_owned(),
            file_id: self.file_id().map(str::to_owned),
            snapshot_content: self.snapshot_content().cloned(),
            metadata: self.metadata().cloned(),
            deleted: self.deleted(),
            created_at: self.created_at().to_string(),
            updated_at: self.updated_at().to_string(),
            change_id: self.change_id(),
            commit_id: self.commit_id(),
        }
    }
}

pub(crate) struct MaterializedTrackedStateBatchIter<'a> {
    batch: &'a MaterializedTrackedStateBatch,
    next: usize,
}

impl<'a> Iterator for MaterializedTrackedStateBatchIter<'a> {
    type Item = MaterializedTrackedStateRowRef<'a>;

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

impl ExactSizeIterator for MaterializedTrackedStateBatchIter<'_> {}

/// Positional exact-read owner. Missing input identities remain `None`, while
/// duplicate present identities can share one materialized batch ordinal.
#[derive(Debug, Default)]
pub(crate) struct MaterializedTrackedStateExactBatch {
    batch: MaterializedTrackedStateBatch,
    slots: Vec<Option<u32>>,
}

impl MaterializedTrackedStateExactBatch {
    pub(crate) fn new(
        batch: MaterializedTrackedStateBatch,
        slots: Vec<Option<u32>>,
    ) -> Result<Self, LixError> {
        if u32::try_from(batch.len()).is_err()
            || slots
                .iter()
                .flatten()
                .any(|ordinal| *ordinal as usize >= batch.len())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact tracked-state result contains an invalid batch ordinal",
            ));
        }
        Ok(Self { batch, slots })
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn row(&self, slot: usize) -> Option<MaterializedTrackedStateRowRef<'_>> {
        self.slots
            .get(slot)
            .copied()
            .flatten()
            .map(|ordinal| self.batch.row(ordinal as usize))
    }

    pub(crate) fn into_rows(self) -> Vec<Option<MaterializedTrackedStateRow>> {
        let Self { batch, slots } = self;
        slots
            .into_iter()
            .map(|ordinal| ordinal.map(|ordinal| batch.row(ordinal as usize).to_owned()))
            .collect()
    }
}

const NO_STRING_ORDINAL: u32 = u32::MAX;
const SMALL_STRING_DICTIONARY_LIMIT: usize = 32;
const SMALL_STRING_ARENA_BYTES: usize = 1024;
#[cfg(test)]
const LARGE_STRING_ALLOCATION_BYTES: usize = 32 * 1024;

enum TrackedStateStringLookup {
    Small,
    Hashed(HashMap<u64, u32, RandomState>),
}

/// Arena-first dictionary builder for historical row identities.
///
/// Hash buckets retain compact ordinals into `ranges`; collision chains are
/// another flat ordinal column. No distinct schema/file value owns a `String`,
/// and finishing transfers the UTF-8 arena into `Bytes` without recopying it.
struct TrackedStateStringDictionaryBuilder {
    bytes: Vec<u8>,
    ranges: Vec<Range<u32>>,
    collision_next: Vec<u32>,
    lookup: TrackedStateStringLookup,
    hash_builder: RandomState,
    expected_entry_capacity: usize,
    maximum_entry_capacity: usize,
    max_string_len: usize,
    exact_byte_capacity: bool,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl TrackedStateStringDictionaryBuilder {
    fn with_capacity(entry_capacity: usize, byte_capacity: usize) -> Self {
        let expected_entry_capacity = entry_capacity.max(1);
        Self {
            bytes: Vec::with_capacity(byte_capacity),
            ranges: Vec::with_capacity(entry_capacity),
            collision_next: Vec::with_capacity(entry_capacity),
            lookup: TrackedStateStringLookup::Small,
            hash_builder: RandomState::with_seeds(0, 0, 0, 0),
            expected_entry_capacity,
            maximum_entry_capacity: expected_entry_capacity,
            max_string_len: 0,
            exact_byte_capacity: byte_capacity != 0,
            #[cfg(test)]
            arena_allocation_count: usize::from(byte_capacity != 0),
            #[cfg(test)]
            arena_large_allocation_count: usize::from(
                byte_capacity >= LARGE_STRING_ALLOCATION_BYTES,
            ),
        }
    }

    fn intern(&mut self, value: &str) -> u32 {
        if !matches!(&self.lookup, TrackedStateStringLookup::Small) {
            return self.intern_hashed(value);
        }
        if let Some(ordinal) = self.find_linear(value) {
            return ordinal;
        }
        if self.ranges.len() == SMALL_STRING_DICTIONARY_LIMIT {
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
                u32::try_from(ordinal).expect("tracked-state string dictionary ordinal exceeds u32")
            })
    }

    fn intern_hashed(&mut self, value: &str) -> u32 {
        let hash = self.hash_builder.hash_one(value.as_bytes());
        let mut candidate = match &self.lookup {
            TrackedStateStringLookup::Small => {
                unreachable!("hashed tracked-state dictionary must be promoted")
            }
            TrackedStateStringLookup::Hashed(lookup) => lookup.get(&hash).copied(),
        };
        while let Some(ordinal) = candidate {
            let range = &self.ranges[ordinal as usize];
            if &self.bytes[range.start as usize..range.end as usize] == value.as_bytes() {
                return ordinal;
            }
            let next = self.collision_next[ordinal as usize];
            candidate = (next != NO_STRING_ORDINAL).then_some(next);
        }

        self.append_hashed(value, hash)
    }

    fn append_small(&mut self, value: &str) -> u32 {
        let ordinal = self.append_bytes(value);
        self.collision_next.push(NO_STRING_ORDINAL);
        ordinal
    }

    fn append_hashed(&mut self, value: &str, hash: u64) -> u32 {
        let previous_head = match &self.lookup {
            TrackedStateStringLookup::Small => {
                unreachable!("hashed tracked-state dictionary must be promoted")
            }
            TrackedStateStringLookup::Hashed(lookup) => {
                lookup.get(&hash).copied().unwrap_or(NO_STRING_ORDINAL)
            }
        };
        let ordinal = self.append_bytes(value);
        self.collision_next.push(previous_head);
        let TrackedStateStringLookup::Hashed(lookup) = &mut self.lookup else {
            unreachable!("hashed tracked-state dictionary must remain promoted")
        };
        lookup.insert(hash, ordinal);
        ordinal
    }

    fn append_bytes(&mut self, value: &str) -> u32 {
        self.max_string_len = self.max_string_len.max(value.len());
        let required = self
            .bytes
            .len()
            .checked_add(value.len())
            .expect("tracked-state string dictionary byte count overflow");
        self.ensure_arena_capacity(required);
        let start = u32::try_from(self.bytes.len())
            .expect("tracked-state string dictionary exceeds u32 bytes");
        self.bytes.extend_from_slice(value.as_bytes());
        let end = u32::try_from(self.bytes.len())
            .expect("tracked-state string dictionary exceeds u32 bytes");
        let ordinal = u32::try_from(self.ranges.len())
            .expect("tracked-state string dictionary exceeds u32 entries");
        assert_ne!(
            ordinal, NO_STRING_ORDINAL,
            "tracked-state string dictionary reserves the terminal u32 ordinal"
        );
        self.ranges.push(start..end);
        ordinal
    }

    fn ensure_arena_capacity(&mut self, required: usize) {
        if required <= self.bytes.capacity() {
            return;
        }
        let projected = match &self.lookup {
            TrackedStateStringLookup::Small => SMALL_STRING_ARENA_BYTES,
            TrackedStateStringLookup::Hashed(_) => self
                .maximum_entry_capacity
                .saturating_mul(self.max_string_len),
        };
        let target = required.max(projected);
        self.bytes.reserve_exact(target - self.bytes.len());
        #[cfg(test)]
        {
            self.arena_allocation_count += 1;
            self.arena_large_allocation_count +=
                usize::from(target >= LARGE_STRING_ALLOCATION_BYTES);
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
                    usize::from(projected_bytes >= LARGE_STRING_ALLOCATION_BYTES);
            }
        }

        let mut lookup = HashMap::with_capacity_and_hasher(
            projected_entries,
            RandomState::with_seeds(0, 0, 0, 1),
        );
        for ordinal in 0..self.ranges.len() {
            let ordinal = u32::try_from(ordinal)
                .expect("tracked-state string dictionary ordinal exceeds u32");
            let range = &self.ranges[ordinal as usize];
            let hash = self
                .hash_builder
                .hash_one(&self.bytes[range.start as usize..range.end as usize]);
            self.collision_next[ordinal as usize] =
                lookup.insert(hash, ordinal).unwrap_or(NO_STRING_ORDINAL);
        }
        self.lookup = TrackedStateStringLookup::Hashed(lookup);
    }

    fn finish(self) -> TrackedStateStringDictionary {
        TrackedStateStringDictionary {
            bytes: Bytes::from(self.bytes),
            ranges: self.ranges,
            #[cfg(test)]
            arena_allocation_count: self.arena_allocation_count,
            #[cfg(test)]
            arena_large_allocation_count: self.arena_large_allocation_count,
        }
    }
}

struct MaterializedTrackedStateBatchBuilder {
    strings: TrackedStateStringDictionaryBuilder,
    rows: Vec<MaterializedTrackedStateDescriptor>,
}

impl MaterializedTrackedStateBatchBuilder {
    #[cfg(test)]
    fn with_capacity(row_count: usize) -> Self {
        Self::with_capacities(row_count, row_count.saturating_mul(2), 0)
    }

    fn with_capacities(
        row_count: usize,
        dictionary_entry_capacity: usize,
        dictionary_byte_capacity: usize,
    ) -> Self {
        Self {
            strings: TrackedStateStringDictionaryBuilder::with_capacity(
                dictionary_entry_capacity,
                dictionary_byte_capacity,
            ),
            rows: Vec::with_capacity(row_count),
        }
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        self.strings.intern(value.as_str())
    }

    fn intern_str(&mut self, value: &str) -> u32 {
        self.strings.intern(value)
    }

    fn push(
        &mut self,
        key: TrackedStateKey,
        value: TrackedStateIndexValue,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
    ) {
        let schema_key = self.intern_owned(key.schema_key);
        let file_id = key.file_id.map(|file_id| self.intern_owned(file_id));
        self.rows.push(MaterializedTrackedStateDescriptor {
            entity_pk: key.entity_pk,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        });
    }

    fn push_ref(
        &mut self,
        key: TrackedStateKeyRef<'_>,
        value: TrackedStateIndexValue,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
    ) {
        let schema_key = self.intern_str(key.schema_key);
        let file_id = key.file_id.map(|file_id| self.intern_str(file_id));
        self.rows.push(MaterializedTrackedStateDescriptor {
            entity_pk: key.entity_pk.clone(),
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        });
    }

    fn finish(self) -> MaterializedTrackedStateBatch {
        MaterializedTrackedStateBatch {
            strings: self.strings.finish(),
            rows: self.rows,
        }
    }
}

/// Materializes tracked-state index entries into one typed batch.
///
/// The durable tracked_state value carries only identity (change id, commit
/// id, flags, timestamps); payloads live once, in the change record the row
/// already references. Hydration is a batched changelog point read per
/// distinct change id, then json_store loads for the rare large payloads.
/// The GC contract makes this sound: a change record is only deletable when
/// no tracked-state row references its change id.
pub(crate) async fn materialize_batch_from_index_entries<S>(
    store: &S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    materialization: &ChangeRecordProjection,
) -> Result<MaterializedTrackedStateBatch, LixError>
where
    S: StorageAdapterRead,
{
    let dictionary_entry_capacity = entries
        .iter()
        .map(|(key, _)| 1 + usize::from(key.file_id.is_some()))
        .sum();
    let mut rows = MaterializedTrackedStateBatchBuilder::with_capacities(
        entries.len(),
        dictionary_entry_capacity,
        0,
    );
    if !materialization.snapshot_content && !materialization.metadata {
        for (key, value) in entries {
            rows.push(key, value, None, None);
        }
        return Ok(rows.finish());
    }

    let payloads = materialize_change_payloads(
        store,
        entries
            .iter()
            .filter(|(_, value)| !value.deleted)
            .map(|(_, value)| value.change_id),
        *materialization,
        "tracked-state row",
    )
    .await?;

    for (key, value) in entries {
        let (snapshot_content, metadata) = if value.deleted {
            (None, None)
        } else {
            shared_payload_fields(
                &payloads,
                TrackedStateKeyRef {
                    schema_key: key.schema_key.as_str(),
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                },
                value.change_id,
            )?
        };
        rows.push(key, value, snapshot_content, metadata);
    }
    Ok(rows.finish())
}

/// Borrowed-key counterpart for exact historical reads.
///
/// The caller retains one compact key-reference column through the async
/// lookup. Identity strings are copied only when first inserted into the
/// materialized batch dictionary, never once per requested row.
pub(crate) async fn materialize_batch_from_index_entry_refs<'a, S>(
    store: &S,
    entries: Vec<(TrackedStateKeyRef<'a>, TrackedStateIndexValue)>,
    materialization: &ChangeRecordProjection,
) -> Result<MaterializedTrackedStateBatch, LixError>
where
    S: StorageAdapterRead,
{
    let dictionary_entry_capacity = entries
        .iter()
        .map(|(key, _)| 1 + usize::from(key.file_id.is_some()))
        .sum();
    let mut rows = MaterializedTrackedStateBatchBuilder::with_capacities(
        entries.len(),
        dictionary_entry_capacity,
        0,
    );
    if !materialization.snapshot_content && !materialization.metadata {
        for (key, value) in entries {
            rows.push_ref(key, value, None, None);
        }
        return Ok(rows.finish());
    }

    let payloads = materialize_change_payloads(
        store,
        entries
            .iter()
            .filter(|(_, value)| !value.deleted)
            .map(|(_, value)| value.change_id),
        *materialization,
        "tracked-state row",
    )
    .await?;

    for (key, value) in entries {
        let (snapshot_content, metadata) = if value.deleted {
            (None, None)
        } else {
            shared_payload_fields(&payloads, key, value.change_id)?
        };
        rows.push_ref(key, value, snapshot_content, metadata);
    }
    Ok(rows.finish())
}

/// Returns cheap views of the materialized payload retained by the batch map.
///
/// A change can back multiple tracked-state rows during historical reads.
/// `SharedStr` lets every use retain the same immutable JSON-store buffer
/// without a use-count map or a new owned string allocation per repeated row.
fn shared_payload_fields(
    payloads: &HashMap<ChangeId, MaterializedChangePayload>,
    key: TrackedStateKeyRef<'_>,
    change_id: ChangeId,
) -> Result<(Option<SharedStr>, Option<SharedStr>), LixError> {
    let payload = payloads.get(&change_id).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked-state row references ChangeRecord '{change_id}' that was not materialized"
            ),
        )
    })?;
    if let Some(identity) = payload.identity.as_ref()
        && (identity.schema_key != key.schema_key
            || identity.entity_pk != *key.entity_pk
            || identity.file_id.as_deref() != key.file_id)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked-state row identity does not match referenced ChangeRecord '{change_id}'"
            ),
        ));
    }
    Ok((payload.snapshot_content.clone(), payload.metadata.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::MaterializedChangeIdentity;
    use crate::entity_pk::{EntityPk, EntityPkComponent};

    fn integer_entity_pk(value: i64) -> EntityPk {
        EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Integer(value)])
            .expect("one integer is a valid entity primary key")
    }

    fn index_value(index: usize) -> TrackedStateIndexValue {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(1_700_000_000_000);
        TrackedStateIndexValue {
            change_id: ChangeId::new(uuid::Uuid::from_u128(
                u128::try_from(index).expect("test index fits u128") + 1,
            )),
            commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    #[test]
    fn ten_thousand_rows_share_identity_dictionary_and_constant_batch_buffers() {
        const ROW_COUNT: usize = 10_000;
        let snapshot = SharedStr::from_static(r#"{"value":"shared"}"#);
        let metadata = SharedStr::from_static(r#"{"impact":"format"}"#);
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let entity_pk =
                integer_entity_pk(i64::try_from(index).expect("test row index fits i64"));
            builder.push_ref(
                TrackedStateKeyRef {
                    schema_key: "shared_schema",
                    file_id: Some("shared_file"),
                    entity_pk: &entity_pk,
                },
                index_value(index),
                Some(snapshot.clone()),
                Some(metadata.clone()),
            );
        }
        let batch = builder.finish();

        assert_eq!(batch.len(), ROW_COUNT);
        assert_eq!(
            batch.dictionary_entry_count(),
            2,
            "schema and file metadata must each be retained once"
        );
        assert_eq!(
            batch.large_buffer_count(4 * 1024),
            1,
            "row count must not change the constant descriptor/dictionary column count"
        );
        let first = batch.row(0);
        let last = batch.row(ROW_COUNT - 1);
        assert_eq!(first.schema_key().as_ptr(), last.schema_key().as_ptr());
        assert_eq!(
            first.file_id().expect("first file").as_ptr(),
            last.file_id().expect("last file").as_ptr()
        );
        let first_schema = first.schema_key_shared();
        let last_schema = last.schema_key_shared();
        let first_file = first.file_id_shared().expect("first shared file");
        assert!(first_schema.shares_buffer_with(&last_schema));
        assert!(
            first_schema.shares_buffer_with(&first_file),
            "all identity strings must retain the same dictionary arena"
        );
        assert!(
            first
                .snapshot_content()
                .expect("first snapshot")
                .shares_buffer_with(last.snapshot_content().expect("last snapshot"))
        );
        assert_eq!(
            first.created_at(),
            LixTimestamp::from_unix_millis_utc_lossy(1_700_000_000_000)
        );
    }

    #[test]
    fn ten_thousand_unique_file_ids_append_to_one_historical_identity_arena() {
        const ROW_COUNT: usize = 10_000;
        let mut builder =
            MaterializedTrackedStateBatchBuilder::with_capacities(ROW_COUNT, ROW_COUNT * 2, 0);
        for index in 0..ROW_COUNT {
            let entity_pk =
                integer_entity_pk(i64::try_from(index).expect("test row index fits i64"));
            let file_id = format!("file-{index:05}");
            builder.push_ref(
                TrackedStateKeyRef {
                    schema_key: "shared_schema",
                    file_id: Some(file_id.as_str()),
                    entity_pk: &entity_pk,
                },
                index_value(index),
                None,
                None,
            );
        }
        let batch = builder.finish();

        assert_eq!(batch.len(), ROW_COUNT);
        assert_eq!(batch.dictionary_entry_count(), ROW_COUNT + 1);
        assert_eq!(
            batch.dictionary_arena_allocation_count(),
            2,
            "the small arena and one promoted arena must cover every unique identity"
        );
        assert_eq!(
            batch.dictionary_arena_large_allocation_count(),
            1,
            "historical identity lowering must allocate one large UTF-8 arena per batch"
        );
        assert_eq!(batch.row(0).file_id(), Some("file-00000"));
        assert_eq!(batch.row(ROW_COUNT - 1).file_id(), Some("file-09999"));
        assert!(
            batch
                .row(0)
                .file_id_shared()
                .expect("first file")
                .shares_buffer_with(
                    &batch
                        .row(ROW_COUNT - 1)
                        .file_id_shared()
                        .expect("last file")
                ),
            "all unique identities must remain slices of the same immutable arena"
        );
    }

    #[test]
    fn projected_exact_batch_preserves_duplicate_and_missing_input_slots() {
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(1);
        builder.push(
            TrackedStateKey {
                schema_key: "message".to_owned(),
                file_id: Some("file.md".to_owned()),
                entity_pk: integer_entity_pk(7),
            },
            index_value(0),
            Some(SharedStr::from_static(r#"{"id":7}"#)),
            None,
        );
        let exact =
            MaterializedTrackedStateExactBatch::new(builder.finish(), vec![Some(0), None, Some(0)])
                .expect("aligned exact batch");

        assert_eq!(exact.len(), 3);
        let first = exact.row(0).expect("first duplicate");
        assert!(exact.row(1).is_none());
        let duplicate = exact.row(2).expect("second duplicate");
        assert_eq!(first.entity_pk(), duplicate.entity_pk());
        assert_eq!(first.schema_key().as_ptr(), duplicate.schema_key().as_ptr());
        assert!(
            first
                .snapshot_content()
                .expect("first payload")
                .shares_buffer_with(duplicate.snapshot_content().expect("duplicate payload"))
        );
    }

    fn fixture(
        schema_key: &str,
    ) -> (
        ChangeId,
        TrackedStateKey,
        HashMap<ChangeId, MaterializedChangePayload>,
        SharedStr,
    ) {
        let change_id = ChangeId::new(uuid::Uuid::from_bytes([7; 16]));
        let key = TrackedStateKey {
            schema_key: schema_key.to_owned(),
            file_id: Some("file.md".to_owned()),
            entity_pk: EntityPk::single("entity"),
        };
        let snapshot = SharedStr::from(r#"{"id":"entity"}"#.to_owned());
        let payload = MaterializedChangePayload {
            identity: Some(MaterializedChangeIdentity {
                schema_key: "message".to_owned(),
                entity_pk: EntityPk::single("entity"),
                file_id: Some("file.md".to_owned()),
            }),
            snapshot_content: Some(snapshot.clone()),
            metadata: None,
        };
        (
            change_id,
            key,
            HashMap::from([(change_id, payload)]),
            snapshot,
        )
    }

    #[test]
    fn repeated_payload_uses_share_the_materialized_json_buffer() {
        let (change_id, key, payloads, source) = fixture("message");

        let key_ref = TrackedStateKeyRef {
            schema_key: key.schema_key.as_str(),
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        };
        let first = shared_payload_fields(&payloads, key_ref, change_id)
            .expect("first payload use")
            .0
            .expect("snapshot");
        let second = shared_payload_fields(&payloads, key_ref, change_id)
            .expect("second payload use")
            .0
            .expect("snapshot");

        assert!(source.shares_buffer_with(&first));
        assert!(first.shares_buffer_with(&second));
    }

    #[test]
    fn payload_identity_mismatch_is_rejected() {
        let (change_id, key, payloads, _) = fixture("wrong-schema");

        let error = shared_payload_fields(
            &payloads,
            TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            },
            change_id,
        )
        .expect_err("mismatched identity must fail");
        assert!(
            error
                .to_string()
                .contains("identity does not match referenced ChangeRecord")
        );
    }
}
