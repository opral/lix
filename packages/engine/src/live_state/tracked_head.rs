//! Materialized serving state for one tracked branch head.
//!
//! Commit roots remain the immutable authority for versioned state. This
//! table is a generation-keyed projection of one branch head which lets the
//! normal live-state path range scan rows and hydrate JSON directly, without
//! replaying every row through the changelog. A manifest binds a generation to
//! both the branch ref's commit and its immutable root. Any mismatch is a
//! cache miss and callers fall back to the commit root.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::LixError;
use crate::NullableKeyFilter;
use crate::changelog::{ChangeId, ChangeRecordProjection, CommitId};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext,
};
use crate::live_state::MaterializedLiveStateRow;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteSet,
};
use crate::storage_codec;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateFilter, TrackedStateKey, TrackedStateRootId,
    TrackedStateScanRequest,
};

// v3 intentionally changes both spaces. A v2 marker can never authorize a
// v3 reader over v2 row bytes, so mixed-version repositories safely take the
// immutable-root fallback until the next normal tracked commit publishes a
// complete v3 generation.
pub(crate) const TRACKED_HEAD_ROW_NAMESPACE: &str = "live_state.tracked_head_row.v3";
pub(crate) const TRACKED_HEAD_MARKER_NAMESPACE: &str = "live_state.tracked_head_marker.v3";
pub(crate) const TRACKED_HEAD_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_000b), TRACKED_HEAD_ROW_NAMESPACE);
pub(crate) const TRACKED_HEAD_MARKER_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_000c), TRACKED_HEAD_MARKER_NAMESPACE);

/// Immutable manifest for the currently readable generation of a branch.
///
/// A new generation is used after a branch ref moves away from the parent of
/// a normal commit. Old rows can remain in storage: they are unreachable
/// without this marker and therefore cannot affect serving reads.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct TrackedHeadMarker {
    head_commit_id: CommitId,
    root_id: TrackedStateRootId,
    generation: CommitId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HeadIdentity {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    entity_pk: EntityPk,
    #[musli(with = storage_codec::option)]
    file_id: Option<String>,
}

/// Write-side representation of a v3 head row.
///
/// This exists only while a transaction is being staged. Read-side code uses
/// [`HeadValueView`], which parses the fixed header directly from RocksDB's
/// returned bytes and never builds this allocation-heavy representation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeadValue {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlot,
    metadata: JsonSlot,
}

impl HeadValue {
    fn as_ref(&self) -> HeadValueRef<'_> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot.as_ref_slot(),
            metadata: self.metadata.as_ref_slot(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueRef<'a> {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlotRef<'a>,
    metadata: JsonSlotRef<'a>,
}

#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
struct BranchRef<'a> {
    branch_id: &'a str,
}

/// Zero-copy normal tracked mutation staged into a head generation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrackedHeadDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
}

impl<'a> TrackedHeadDeltaRef<'a> {
    fn identity(&self, branch_id: &str, generation: CommitId) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: self.schema_key.to_string(),
            entity_pk: self.entity_pk.clone(),
            file_id: self.file_id.map(str::to_string),
        }
    }

    fn value_ref(&self, created_at: LixTimestamp) -> HeadValueRef<'a> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at,
            updated_at: self.updated_at,
            snapshot: if self.deleted {
                JsonSlotRef::None
            } else {
                self.snapshot
            },
            metadata: if self.deleted {
                JsonSlotRef::None
            } else {
                self.metadata
            },
        }
    }
}

/// Factory for tracked-head readers and writers.
#[derive(Clone, Copy, Default)]
pub(crate) struct TrackedHeadContext;

impl TrackedHeadContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> TrackedHeadStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        TrackedHeadStoreReader { store }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> TrackedHeadWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        TrackedHeadWriter { store, writes }
    }
}

/// Direct materializer for the current tracked branch generation.
pub(crate) struct TrackedHeadStoreReader<S> {
    store: S,
}

impl<S> TrackedHeadStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// Returns `None` when this branch has no projection for the canonical
    /// branch ref/root pair. That is a cache miss, not empty tracked state.
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        expected_root: &TrackedStateRootId,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let Some(marker) = self
            .marker_if_current(branch_id, expected_head, expected_root)
            .await?
        else {
            return Ok(None);
        };
        let entries = scan_entries(
            &self.store,
            branch_id,
            marker.generation,
            &request.filter,
            None,
        )
        .await?;
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let mut rows =
            materialize_live_entries(&self.store, entries, projection, branch_id).await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(Some(rows))
    }

    /// Like the immutable-root point batch, preserves input cardinality and
    /// returns tombstones for the visibility layer to resolve.
    pub(crate) async fn load_projected_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        expected_root: &TrackedStateRootId,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        if keys.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let Some(marker) = self
            .marker_if_current(branch_id, expected_head, expected_root)
            .await?
        else {
            return Ok(None);
        };

        let mut output_indices = BTreeMap::<HeadIdentity, Vec<usize>>::new();
        for (index, key) in keys.iter().enumerate() {
            output_indices
                .entry(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation: marker.generation,
                    schema_key: key.schema_key.clone(),
                    entity_pk: key.entity_pk.clone(),
                    file_id: key.file_id.clone(),
                })
                .or_default()
                .push(index);
        }
        let identities = output_indices.keys().cloned().collect::<Vec<_>>();
        let values = load_entry_bytes(&self.store, &identities).await?;
        let entries = identities
            .into_iter()
            .zip(values)
            .filter_map(|(identity, value)| value.map(|value| (identity, value)))
            .collect();
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;
        let mut output = vec![None; keys.len()];
        for row in rows {
            let identity = HeadIdentity {
                branch_id: branch_id.to_string(),
                generation: marker.generation,
                schema_key: row.schema_key.clone(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            };
            if let Some(indices) = output_indices.get(&identity) {
                for &index in indices {
                    output[index] = Some(row.clone());
                }
            }
        }
        Ok(Some(output))
    }

    pub(crate) async fn is_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        expected_root: &TrackedStateRootId,
    ) -> Result<bool, LixError> {
        Ok(self
            .marker_if_current(branch_id, expected_head, expected_root)
            .await?
            .is_some())
    }

    async fn marker_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        expected_root: &TrackedStateRootId,
    ) -> Result<Option<TrackedHeadMarker>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "tracked-head expected commit")?;
        let marker = load_marker(&self.store, branch_id).await?;
        Ok(marker.filter(|marker| {
            marker.head_commit_id == expected_head && marker.root_id == *expected_root
        }))
    }
}

/// Writer for an atomic branch-head projection update.
pub(crate) struct TrackedHeadWriter<'a, S: ?Sized> {
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

impl<S> TrackedHeadWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    /// Incrementally updates a matching parent generation, or creates a fresh
    /// generation from a caller-provided parent snapshot. The latter is used
    /// after branch movement and for old repositories which predate this
    /// serving table.
    pub(crate) async fn stage_commit(
        &mut self,
        branch_id: &str,
        parent_head: Option<CommitId>,
        parent_root: Option<&TrackedStateRootId>,
        new_head: CommitId,
        new_root: TrackedStateRootId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<(), LixError> {
        let marker = load_marker(self.store, branch_id).await?;
        let matches_parent = marker.as_ref().is_some_and(|marker| {
            parent_head.is_some_and(|parent| marker.head_commit_id == parent)
                && parent_root.is_some_and(|root| marker.root_id == *root)
        });
        let generation = if matches_parent {
            marker
                .as_ref()
                .expect("matching tracked-head marker exists")
                .generation
        } else {
            new_head
        };
        let delta_identities = deltas
            .iter()
            .map(|delta| delta.identity(branch_id, generation))
            .collect::<BTreeSet<_>>();

        let mut prior_created_at = BTreeMap::<HeadIdentity, LixTimestamp>::new();
        if matches_parent {
            let identities = deltas
                .iter()
                .map(|delta| delta.identity(branch_id, generation))
                .collect::<Vec<_>>();
            let values = load_entry_bytes(self.store, &identities).await?;
            for (identity, value) in identities.into_iter().zip(values) {
                if let Some(value) = value {
                    prior_created_at.insert(identity, decode_head_value(&value)?.created_at);
                }
            }
        } else if let Some(rows) = parent_rows {
            self.writes
                .reserve_space(TRACKED_HEAD_ROW_SPACE, rows.len() + deltas.len(), 0);
            for row in rows {
                let identity = HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: row.schema_key,
                    entity_pk: row.entity_pk,
                    file_id: row.file_id,
                };
                let value = HeadValue {
                    change_id: row.change_id,
                    commit_id: row.commit_id,
                    deleted: row.deleted,
                    created_at: LixTimestamp::expect_parse(
                        "tracked-head parent created_at",
                        &row.created_at,
                    ),
                    updated_at: LixTimestamp::expect_parse(
                        "tracked-head parent updated_at",
                        &row.updated_at,
                    ),
                    snapshot: row
                        .snapshot_content
                        .as_deref()
                        .map_or(JsonSlot::None, JsonSlot::from_json),
                    metadata: row
                        .metadata
                        .as_deref()
                        .map_or(JsonSlot::None, JsonSlot::from_json),
                };
                // The child delta below owns the final mutation for an
                // overlapping key. Keep its parent value only to preserve
                // `created_at`; staging both would violate the write-set's
                // one-put-per-key invariant.
                if !delta_identities.contains(&identity) {
                    stage_put(self.writes, &identity, &value)?;
                }
                prior_created_at.insert(identity, value.created_at);
            }
        } else {
            self.writes
                .reserve_space(TRACKED_HEAD_ROW_SPACE, deltas.len(), 0);
        }

        for delta in deltas {
            let identity = delta.identity(branch_id, generation);
            let created_at = prior_created_at
                .get(&identity)
                .copied()
                .unwrap_or(delta.created_at);
            stage_put_ref(self.writes, &identity, &delta.value_ref(created_at))?;
        }
        stage_marker(
            self.writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: new_head,
                root_id: new_root,
                generation,
            },
        )?;
        Ok(())
    }
}

async fn load_marker(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<TrackedHeadMarker>, LixError> {
    let key = marker_key(branch_id)?;
    let result = PointReadPlan::new(TRACKED_HEAD_MARKER_SPACE, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .next()
        .flatten()
        .map(decode_marker_value)
        .transpose()
}

/// Loads the physical v3 values without decoding them. This keeps the owning
/// `Bytes` allocation alive until the direct materializer has copied only the
/// selected output fields into the final serving row.
async fn load_entry_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = identities
        .iter()
        .map(|identity| StorageKey(Bytes::from(encode_row_key(identity))))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(TRACKED_HEAD_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HeadIdentity, Bytes)>, LixError> {
    if let Some(identities) = exact_filter_identities(branch_id, generation, filter) {
        let values = load_entry_bytes(store, &identities).await?;
        return Ok(identities
            .into_iter()
            .zip(values)
            .filter_map(|(identity, value)| value.map(|value| (identity, value)))
            .take(limit.unwrap_or(usize::MAX))
            .collect());
    }

    let mut prefixes = scan_prefixes(branch_id, generation, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            TRACKED_HEAD_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                return Ok(rows);
            }
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        limit_rows: remaining
                            .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_row_key(entry.key.0.as_ref())?;
                if !matches_filter(&identity, filter) {
                    continue;
                }
                rows.push((identity, full_value_bytes(entry.value)?));
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(rows);
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

fn exact_filter_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadIdentity>> {
    if filter.schema_keys.is_empty()
        || filter.entity_pks.is_empty()
        || filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
    {
        return None;
    }
    let mut identities = Vec::with_capacity(
        filter.schema_keys.len() * filter.entity_pks.len() * filter.file_ids.len(),
    );
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            for file_id in &filter.file_ids {
                let file_id = match file_id {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(value) => Some(value.clone()),
                    NullableKeyFilter::Any => unreachable!("Any rejected above"),
                };
                identities.push(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: schema_key.clone(),
                    entity_pk: entity_pk.clone(),
                    file_id,
                });
            }
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

fn scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Vec<Vec<u8>> {
    let scope = encode_scope_prefix(branch_id, generation);
    if filter.schema_keys.is_empty() {
        return vec![scope];
    }
    let mut prefixes = Vec::new();
    for schema_key in &filter.schema_keys {
        let mut schema_prefix = scope.clone();
        write_key_string(&mut schema_prefix, schema_key, KEY_PART_FINAL);
        if filter.entity_pks.is_empty() {
            prefixes.push(schema_prefix);
            continue;
        }
        for entity_pk in &filter.entity_pks {
            let mut entity_prefix = schema_prefix.clone();
            write_entity_pk(&mut entity_prefix, entity_pk);
            if filter.file_ids.is_empty()
                || filter
                    .file_ids
                    .iter()
                    .any(|filter| matches!(filter, NullableKeyFilter::Any))
            {
                prefixes.push(entity_prefix);
                continue;
            }
            for file_id in &filter.file_ids {
                let file_id = match file_id {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(value) => Some(value.as_str()),
                    NullableKeyFilter::Any => unreachable!("Any handled above"),
                };
                let mut prefix = entity_prefix.clone();
                write_file_id(&mut prefix, file_id);
                prefixes.push(prefix);
            }
        }
    }
    prefixes
}

fn matches_filter(identity: &HeadIdentity, filter: &TrackedStateFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&identity.schema_key))
        && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&identity.entity_pk))
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => identity.file_id.is_none(),
                NullableKeyFilter::Value(value) => identity.file_id.as_ref() == Some(value),
            }))
}

fn stage_marker(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    marker: &TrackedHeadMarker,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_HEAD_MARKER_SPACE,
        StorageKey(Bytes::from(marker_key(branch_id)?)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("tracked-head marker", marker)?),
        },
    );
    Ok(())
}

fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    stage_put_ref(writes, identity, &value.as_ref())
}

fn stage_put_ref(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValueRef<'_>,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_HEAD_ROW_SPACE,
        StorageKey(Bytes::from(encode_row_key(identity))),
        StorageValue {
            bytes: Bytes::from(encode_head_value(value)?),
        },
    );
    Ok(())
}

fn marker_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("tracked-head marker key", &BranchRef { branch_id })
}

fn encode_row_key(identity: &HeadIdentity) -> Vec<u8> {
    let mut out = encode_scope_prefix(&identity.branch_id, identity.generation);
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut out, &identity.entity_pk);
    write_file_id(&mut out, identity.file_id.as_deref());
    out
}

fn decode_row_key(bytes: &[u8]) -> Result<HeadIdentity, LixError> {
    let mut offset = 0usize;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("branch id has an invalid terminator"));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("has trailing bytes"));
    }
    Ok(HeadIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
        file_id,
    })
}

const KEY_ESCAPE: u8 = 0xff;
const KEY_PART_FINAL: u8 = 0x00;
const KEY_PART_MORE: u8 = 0x01;
const FILE_ID_NONE: u8 = 0x00;
const FILE_ID_SOME: u8 = 0x01;
const GENERATION_BYTES: usize = 16;

/// Order-preserving tracked-head key encoding.
///
/// The head table is the normal read serving index, so its storage ordering is
/// also the visible row ordering: `(branch, generation, schema, entity,
/// file)`. Musli's storage encoding is excellent for values and structural
/// prefixes, but length-prefixed strings do not preserve lexical order. This
/// codec retains exact prefix scans while making every table scan already
/// ordered and duplicate-free for one branch generation.
fn encode_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    let mut out = Vec::with_capacity(branch_id.len() + 2 + GENERATION_BYTES);
    write_key_string(&mut out, branch_id, KEY_PART_FINAL);
    out.extend_from_slice(generation.as_uuid().as_bytes());
    out
}

fn write_entity_pk(out: &mut Vec<u8>, entity_pk: &EntityPk) {
    debug_assert!(
        !entity_pk.parts.is_empty(),
        "tracked-head entity primary keys must be non-empty"
    );
    for (index, part) in entity_pk.parts.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.parts.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        write_key_string(out, part, terminator);
    }
}

fn write_file_id(out: &mut Vec<u8>, file_id: Option<&str>) {
    match file_id {
        None => out.push(FILE_ID_NONE),
        Some(file_id) => {
            out.push(FILE_ID_SOME);
            write_key_string(out, file_id, KEY_PART_FINAL);
        }
    }
}

fn write_key_string(out: &mut Vec<u8>, value: &str, terminator: u8) {
    for &byte in value.as_bytes() {
        if byte == KEY_PART_FINAL {
            out.extend_from_slice(&[KEY_PART_FINAL, KEY_ESCAPE]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[KEY_PART_FINAL, terminator]);
}

fn read_generation(bytes: &[u8], offset: &mut usize) -> Result<CommitId, LixError> {
    let end = offset
        .checked_add(GENERATION_BYTES)
        .ok_or_else(|| key_codec_error("generation offset overflow"))?;
    let generation = bytes
        .get(*offset..end)
        .ok_or_else(|| key_codec_error("is truncated before generation"))?;
    let mut uuid = [0; GENERATION_BYTES];
    uuid.copy_from_slice(generation);
    *offset = end;
    Ok(CommitId::new(uuid::Uuid::from_bytes(uuid)))
}

fn read_entity_pk(bytes: &[u8], offset: &mut usize) -> Result<EntityPk, LixError> {
    let mut parts = Vec::new();
    loop {
        let (part, terminator) = read_key_string(bytes, offset, "entity primary key")?;
        parts.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error(
                    "entity primary key has an invalid terminator",
                ));
            }
        }
    }
    EntityPk::from_parts(parts).map_err(|error| {
        key_codec_error(&format!("contains an invalid entity primary key: {error}"))
    })
}

fn read_file_id(bytes: &[u8], offset: &mut usize) -> Result<Option<String>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("is truncated before file id"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_key_string(bytes, offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(key_codec_error("file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        _ => Err(key_codec_error("has an invalid file id tag")),
    }
}

fn read_key_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(String, u8), LixError> {
    let mut out = Vec::new();
    loop {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        *offset += 1;
        if byte != KEY_PART_FINAL {
            out.push(byte);
            continue;
        }
        let terminator = *bytes
            .get(*offset)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        *offset += 1;
        if terminator == KEY_ESCAPE {
            out.push(KEY_PART_FINAL);
            continue;
        }
        let value = String::from_utf8(out).map_err(|error| {
            key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
        })?;
        return Ok((value, terminator));
    }
}

fn key_codec_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head row key: {message}"),
    )
}

fn decode_marker_value(value: StorageProjectedValue) -> Result<TrackedHeadMarker, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked-head marker read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("tracked-head marker", &bytes)
}

/// v3 head values are intentionally a small, fixed-header wire record rather
/// than a general Musli struct. The normal read path needs only these fields,
/// and decoding a Musli `JsonSlot` first allocated an intermediate value for
/// every row before it was copied into a live-state row.
///
/// ```text
///  0      format version (3)
///  1      deleted + snapshot/metadata kinds
///  2..18  change UUID
/// 18..34  commit UUID
/// 34..42  created_at packed timestamp (big endian)
/// 42..50  updated_at packed timestamp (big endian)
/// 50..54  snapshot payload byte length (big endian u32)
/// 54..58  metadata payload byte length (big endian u32)
/// 58..    snapshot payload, then metadata payload
/// ```
///
/// Slot payloads are either inline UTF-8 JSON or a fixed 32-byte `JsonRef`.
/// This makes parsing bounded and lets the scan path build the final
/// `MaterializedLiveStateRow` in one pass.
const HEAD_VALUE_VERSION: u8 = 3;
const HEAD_VALUE_HEADER_BYTES: usize = 58;
const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_SLOT_MASK: u8 = 0b11;
const HEAD_VALUE_ALLOWED_FLAGS: u8 = HEAD_VALUE_DELETED
    | (HEAD_VALUE_SLOT_MASK << HEAD_VALUE_SNAPSHOT_SHIFT)
    | (HEAD_VALUE_SLOT_MASK << HEAD_VALUE_METADATA_SHIFT);
const HEAD_SLOT_NONE: u8 = 0;
const HEAD_SLOT_REF: u8 = 1;
const HEAD_SLOT_INLINE: u8 = 2;
const UUID_BYTES: usize = 16;
const JSON_REF_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadSlotView<'a> {
    None,
    Ref(JsonRef),
    Inline(&'a str),
}

#[derive(Debug, Clone, Copy)]
struct HeadValueView<'a> {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotView<'a>,
    metadata: HeadSlotView<'a>,
}

fn encode_head_value(value: &HeadValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let snapshot_kind = encoded_slot_kind(value.snapshot);
    let metadata_kind = encoded_slot_kind(value.metadata);
    if value.deleted && (snapshot_kind != HEAD_SLOT_NONE || metadata_kind != HEAD_SLOT_NONE) {
        return Err(head_value_error(
            "deleted tracked-head rows must not carry JSON payloads",
        ));
    }
    let snapshot_len = encoded_slot_len(value.snapshot);
    let metadata_len = encoded_slot_len(value.metadata);
    let capacity = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .and_then(|bytes| bytes.checked_add(metadata_len))
        .ok_or_else(|| head_value_error("encoded row length overflow"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(HEAD_VALUE_VERSION);
    let mut flags = if value.deleted { HEAD_VALUE_DELETED } else { 0 };
    flags |= snapshot_kind << HEAD_VALUE_SNAPSHOT_SHIFT;
    flags |= metadata_kind << HEAD_VALUE_METADATA_SHIFT;
    bytes.push(flags);
    bytes.extend_from_slice(value.change_id.as_uuid().as_bytes());
    bytes.extend_from_slice(value.commit_id.as_uuid().as_bytes());
    bytes.extend_from_slice(&value.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(snapshot_len)
            .map_err(|_| head_value_error("snapshot payload exceeds v3 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(metadata_len)
            .map_err(|_| head_value_error("metadata payload exceeds v3 u32 limit"))?
            .to_be_bytes(),
    );
    append_slot_payload(&mut bytes, value.snapshot);
    append_slot_payload(&mut bytes, value.metadata);
    debug_assert_eq!(bytes.len(), capacity);
    Ok(bytes)
}

fn encoded_slot_kind(slot: JsonSlotRef<'_>) -> u8 {
    match slot {
        JsonSlotRef::None => HEAD_SLOT_NONE,
        JsonSlotRef::Ref(_) => HEAD_SLOT_REF,
        JsonSlotRef::Inline(_) => HEAD_SLOT_INLINE,
    }
}

fn encoded_slot_len(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 0,
        JsonSlotRef::Ref(_) => JSON_REF_BYTES,
        JsonSlotRef::Inline(json) => json.len(),
    }
}

fn append_slot_payload(bytes: &mut Vec<u8>, slot: JsonSlotRef<'_>) {
    match slot {
        JsonSlotRef::None => {}
        JsonSlotRef::Ref(json_ref) => bytes.extend_from_slice(json_ref.as_hash_bytes()),
        JsonSlotRef::Inline(json) => bytes.extend_from_slice(json.as_bytes()),
    }
}

fn full_value_bytes(value: StorageProjectedValue) -> Result<Bytes, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "tracked-head row read unexpectedly omitted its value",
        ));
    };
    Ok(bytes)
}

fn decode_head_value(bytes: &[u8]) -> Result<HeadValueView<'_>, LixError> {
    if bytes.len() < HEAD_VALUE_HEADER_BYTES {
        return Err(head_value_error("row is shorter than the v3 fixed header"));
    }
    if bytes[0] != HEAD_VALUE_VERSION {
        return Err(head_value_error(&format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }
    let flags = bytes[1];
    if flags & !HEAD_VALUE_ALLOWED_FLAGS != 0 {
        return Err(head_value_error("row has unknown v3 flag bits"));
    }
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let change_id = ChangeId::new(uuid_from_head_bytes(&bytes[2..18], "change id")?);
    let commit_id = CommitId::new(uuid_from_head_bytes(&bytes[18..34], "commit id")?);
    let created_at = LixTimestamp::from_packed(read_u64(&bytes[34..42], "created_at")?)
        .map_err(|error| head_value_error(&format!("invalid created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(&bytes[42..50], "updated_at")?)
        .map_err(|error| head_value_error(&format!("invalid updated_at: {error}")))?;
    let snapshot_len = usize::try_from(read_u32(&bytes[50..54], "snapshot length")?)
        .map_err(|_| head_value_error("snapshot length exceeds usize"))?;
    let metadata_len = usize::try_from(read_u32(&bytes[54..58], "metadata length")?)
        .map_err(|_| head_value_error("metadata length exceeds usize"))?;
    let snapshot_end = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .ok_or_else(|| head_value_error("snapshot payload length overflow"))?;
    let metadata_end = snapshot_end
        .checked_add(metadata_len)
        .ok_or_else(|| head_value_error("metadata payload length overflow"))?;
    if metadata_end != bytes.len() {
        return Err(head_value_error(
            "row payload lengths do not match the buffer",
        ));
    }
    let snapshot = decode_slot(
        snapshot_kind,
        &bytes[HEAD_VALUE_HEADER_BYTES..snapshot_end],
        "snapshot",
    )?;
    let metadata = decode_slot(
        metadata_kind,
        &bytes[snapshot_end..metadata_end],
        "metadata",
    )?;
    let deleted = flags & HEAD_VALUE_DELETED != 0;
    if deleted && (snapshot != HeadSlotView::None || metadata != HeadSlotView::None) {
        return Err(head_value_error(
            "deleted tracked-head rows must not carry JSON payloads",
        ));
    }
    Ok(HeadValueView {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
    })
}

fn uuid_from_head_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes.try_into().map_err(|_| {
        head_value_error(&format!(
            "{field} must have {UUID_BYTES} bytes in the v3 header"
        ))
    })?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn read_u64(bytes: &[u8], field: &str) -> Result<u64, LixError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| head_value_error(&format!("{field} has an invalid fixed-header width")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u32(bytes: &[u8], field: &str) -> Result<u32, LixError> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| head_value_error(&format!("{field} has an invalid fixed-header width")))?;
    Ok(u32::from_be_bytes(bytes))
}

fn decode_slot<'a>(kind: u8, bytes: &'a [u8], field: &str) -> Result<HeadSlotView<'a>, LixError> {
    match kind {
        HEAD_SLOT_NONE if bytes.is_empty() => Ok(HeadSlotView::None),
        HEAD_SLOT_NONE => Err(head_value_error(&format!(
            "{field} none slot must have an empty payload"
        ))),
        HEAD_SLOT_REF if bytes.len() == JSON_REF_BYTES => {
            let hash: [u8; JSON_REF_BYTES] = bytes.try_into().map_err(|_| {
                head_value_error(&format!(
                    "{field} ref payload must have {JSON_REF_BYTES} bytes"
                ))
            })?;
            Ok(HeadSlotView::Ref(JsonRef::from_hash_bytes(hash)))
        }
        HEAD_SLOT_REF => Err(head_value_error(&format!(
            "{field} ref payload must have {JSON_REF_BYTES} bytes"
        ))),
        HEAD_SLOT_INLINE => std::str::from_utf8(bytes)
            .map(HeadSlotView::Inline)
            .map_err(|error| {
                head_value_error(&format!("{field} inline payload is not UTF-8: {error}"))
            }),
        _ => Err(head_value_error(&format!(
            "{field} has an unknown slot kind {kind}"
        ))),
    }
}

fn head_value_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head v3 row: {message}"),
    )
}

#[derive(Clone, Copy)]
enum DeferredJsonField {
    Snapshot,
    Metadata,
}

struct DeferredJson {
    row_index: usize,
    field: DeferredJsonField,
    json_ref: JsonRef,
}

/// Builds serving rows directly from a v3 wire value. The only allocations
/// here are the final `String` fields and identities which the public row type
/// requires; there is no `HeadValue`/`MaterializedTrackedStateRow` staging
/// layer to drop after each scan.
async fn materialize_live_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: Vec<(HeadIdentity, Bytes)>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    let mut json_refs = Vec::new();
    let mut deferred = Vec::new();
    let mut rows = Vec::with_capacity(entries.len());
    for (identity, bytes) in entries {
        let value = decode_head_value(&bytes)?;
        let row_index = rows.len();
        let snapshot_content = materialize_live_slot(
            !value.deleted && projection.snapshot_content,
            value.snapshot,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Snapshot,
        );
        let metadata = materialize_live_slot(
            !value.deleted && projection.metadata,
            value.metadata,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Metadata,
        );
        rows.push(MaterializedLiveStateRow {
            entity_pk: identity.entity_pk,
            schema_key: identity.schema_key,
            file_id: identity.file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted,
            created_at: value.created_at.to_string(),
            updated_at: value.updated_at.to_string(),
            global,
            change_id: Some(value.change_id),
            commit_id: Some(value.commit_id),
            untracked: false,
            branch_id: branch_id.to_string(),
        });
    }
    if json_refs.is_empty() {
        return Ok(rows);
    }
    let mut json_values = JsonStoreContext::new()
        .load_bytes_many(
            store,
            JsonLoadRequestRef {
                refs: &json_refs,
                scope: JsonReadScopeRef::OutOfBand,
            },
        )
        .await?
        .into_values();
    for (index, deferred) in deferred.into_iter().enumerate() {
        let bytes = json_values
            .get_mut(index)
            .ok_or_else(|| head_value_error("lost an out-of-band JSON value index"))?
            .take()
            .ok_or_else(|| {
                head_value_error(&format!(
                    "row is missing JSON payload '{}'",
                    deferred.json_ref.to_hex()
                ))
            })?;
        let json = String::from_utf8(bytes).map_err(|error| {
            head_value_error(&format!("out-of-band JSON payload is not UTF-8: {error}"))
        })?;
        let row = rows
            .get_mut(deferred.row_index)
            .ok_or_else(|| head_value_error("lost an out-of-band JSON row index"))?;
        match deferred.field {
            DeferredJsonField::Snapshot => row.snapshot_content = Some(json),
            DeferredJsonField::Metadata => row.metadata = Some(json),
        }
    }
    Ok(rows)
}

fn materialize_live_slot(
    include: bool,
    slot: HeadSlotView<'_>,
    json_refs: &mut Vec<JsonRef>,
    deferred: &mut Vec<DeferredJson>,
    row_index: usize,
    field: DeferredJsonField,
) -> Option<String> {
    if !include {
        return None;
    }
    match slot {
        HeadSlotView::None => None,
        HeadSlotView::Inline(json) => Some(json.to_string()),
        HeadSlotView::Ref(json_ref) => {
            json_refs.push(json_ref);
            deferred.push(DeferredJson {
                row_index,
                field,
                json_ref,
            });
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::{JsonWritePlacementRef, NormalizedJsonRef};
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("test timestamp", value)
    }

    fn root(label: &str) -> TrackedStateRootId {
        TrackedStateRootId::new(*blake3::hash(label.as_bytes()).as_bytes())
    }

    fn identity(branch_id: &str, generation: CommitId, entity: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        }
    }

    fn head_value(change: &str, commit_id: CommitId) -> HeadValue {
        HeadValue {
            change_id: ChangeId::for_test_label(change),
            commit_id,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            snapshot: JsonSlot::from_json("{\"value\":true}"),
            metadata: JsonSlot::None,
        }
    }

    #[test]
    fn v3_value_codec_roundtrips_fixed_header_inline_and_ref_slots() {
        let snapshot_ref = JsonRef::from_hash_bytes([7; JSON_REF_BYTES]);
        let value = HeadValueRef {
            change_id: ChangeId::for_test_label("change"),
            commit_id: CommitId::for_test_label("commit"),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-02T00:00:00Z"),
            snapshot: JsonSlotRef::Inline("{\"snapshot\":true}"),
            metadata: JsonSlotRef::Ref(&snapshot_ref),
        };

        let bytes = encode_head_value(&value).expect("encode v3 row");
        assert_eq!(bytes[0], HEAD_VALUE_VERSION);
        assert_eq!(
            bytes.len(),
            HEAD_VALUE_HEADER_BYTES + "{\"snapshot\":true}".len() + JSON_REF_BYTES
        );
        let decoded = decode_head_value(&bytes).expect("decode v3 row");
        assert_eq!(decoded.change_id, value.change_id);
        assert_eq!(decoded.commit_id, value.commit_id);
        assert_eq!(decoded.created_at, value.created_at);
        assert_eq!(decoded.updated_at, value.updated_at);
        assert_eq!(
            decoded.snapshot,
            HeadSlotView::Inline("{\"snapshot\":true}")
        );
        assert_eq!(decoded.metadata, HeadSlotView::Ref(snapshot_ref));
    }

    #[tokio::test]
    async fn direct_live_materializer_honors_projection_and_batches_out_of_band_refs() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let root = root("head");
        let long_metadata = format!("\"{}\"", "x".repeat(300));
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        let refs = json_writer
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::new(&long_metadata)],
            )
            .expect("stage out-of-band metadata");
        let metadata_ref = refs[0];
        let row_identity = identity(branch_id, generation, "row");
        stage_put(
            &mut writes,
            &row_identity,
            &HeadValue {
                change_id: ChangeId::for_test_label("change"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-02T00:00:00Z"),
                snapshot: JsonSlot::from_json("{\"snapshot\":true}"),
                metadata: JsonSlot::Ref(metadata_ref),
            },
        )
        .expect("stage v3 row");
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: head,
                root_id: root.clone(),
                generation,
            },
        )
        .expect("stage v3 marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit v3 head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open projection read");
        let metadata_only = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &root,
                &TrackedStateScanRequest {
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["metadata".to_string()],
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("scan v3 head")
            .expect("matching marker");
        assert_eq!(metadata_only.len(), 1);
        assert_eq!(metadata_only[0].snapshot_content, None);
        assert_eq!(
            metadata_only[0].metadata.as_deref(),
            Some(long_metadata.as_str())
        );
        assert_eq!(metadata_only[0].branch_id, branch_id);
        assert!(!metadata_only[0].global);
        assert!(!metadata_only[0].untracked);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open point read");
        let keys = vec![
            TrackedStateKey {
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            },
            TrackedStateKey {
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            },
        ];
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &root,
                &keys,
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("point read v3 head")
            .expect("matching marker");
        assert_eq!(rows.len(), 2);
        for row in rows.into_iter().flatten() {
            assert_eq!(row.snapshot_content.as_deref(), Some("{\"snapshot\":true}"));
            assert_eq!(row.metadata.as_deref(), Some(long_metadata.as_str()));
            assert_eq!(row.change_id, Some(ChangeId::for_test_label("change")));
            assert_eq!(row.commit_id, Some(head));
        }
    }

    #[test]
    fn row_key_roundtrips_and_preserves_logical_order() {
        let generation = CommitId::for_test_label("generation");
        let strings = ["", "\0", "a", "a\0", "a\u{1}", "z", "é"];
        let mut identities = Vec::new();
        for schema_key in strings {
            for entity_first in strings {
                for entity_pk in [
                    EntityPk::single(entity_first),
                    EntityPk::from_parts(vec![entity_first.to_string(), "tail".to_string()])
                        .expect("tuple entity key should be valid"),
                ] {
                    for file_id in [None, Some(""), Some("a"), Some("a\0")] {
                        identities.push(HeadIdentity {
                            branch_id: "branch\0name".to_string(),
                            generation,
                            schema_key: schema_key.to_string(),
                            entity_pk: entity_pk.clone(),
                            file_id: file_id.map(str::to_string),
                        });
                    }
                }
            }
        }
        identities.sort();
        identities.dedup();

        for identity in &identities {
            let encoded = encode_row_key(identity);
            assert_eq!(
                decode_row_key(&encoded).expect("row key should decode"),
                *identity
            );
        }

        let mut by_encoded = identities
            .iter()
            .cloned()
            .map(|identity| (encode_row_key(&identity), identity))
            .collect::<Vec<_>>();
        by_encoded.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            by_encoded
                .iter()
                .map(|(_, identity)| identity)
                .collect::<Vec<_>>(),
            identities.iter().collect::<Vec<_>>()
        );
        for (index, (encoded, _)) in by_encoded.iter().enumerate() {
            for (other_index, (other, _)) in by_encoded.iter().enumerate() {
                if index != other_index {
                    assert!(
                        !other.starts_with(encoded),
                        "complete row key {index} prefixes row key {other_index}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn head_scan_is_logically_ordered_and_unique() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let root = root("head");
        let identities = vec![
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-z".to_string(),
                entity_pk: EntityPk::single("entity-a"),
                file_id: None,
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                entity_pk: EntityPk::single("entity-z"),
                file_id: Some("file-a".to_string()),
            },
            HeadIdentity {
                branch_id: "branch".to_string(),
                generation,
                schema_key: "schema-a".to_string(),
                entity_pk: EntityPk::single("entity-a"),
                file_id: None,
            },
        ];
        let mut expected = identities.clone();
        expected.sort();

        let mut writes = StorageWriteSet::new();
        for (index, identity) in identities.iter().rev().enumerate() {
            stage_put(
                &mut writes,
                identity,
                &head_value(&format!("change-{index}"), head),
            )
            .expect("stage row");
        }
        stage_marker(
            &mut writes,
            "branch",
            &TrackedHeadMarker {
                head_commit_id: head,
                root_id: root.clone(),
                generation,
            },
        )
        .expect("stage marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit head table");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                "branch",
                &head.to_string(),
                &root,
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan")
            .expect("marker should match");
        assert_eq!(rows.len(), expected.len());
        assert_eq!(
            rows.into_iter()
                .map(|row| (row.schema_key, row.entity_pk, row.file_id))
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|identity| (identity.schema_key, identity.entity_pk, identity.file_id))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn marker_gates_generations_and_rows_roundtrip() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let identity = identity("branch", generation, "row");
        let value = HeadValue {
            change_id: ChangeId::for_test_label("change"),
            commit_id: head,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:01Z"),
            snapshot: JsonSlot::from_json("{\"id\":\"row\"}"),
            metadata: JsonSlot::None,
        };
        let root = root("head");
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &value).expect("stage row");
        stage_marker(
            &mut writes,
            "branch",
            &TrackedHeadMarker {
                head_commit_id: head,
                root_id: root.clone(),
                generation,
            },
        )
        .expect("stage marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit table");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                "branch",
                &head.to_string(),
                &root,
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"row\"}")
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open mismatch read");
        assert!(
            TrackedHeadContext::new()
                .reader(read)
                .scan_live_rows_if_current(
                    "branch",
                    &CommitId::for_test_label("other").to_string(),
                    &root,
                    &TrackedStateScanRequest::default(),
                )
                .await
                .expect("scan mismatch")
                .is_none()
        );
    }

    #[tokio::test]
    async fn incremental_commit_preserves_first_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let entity_pk = EntityPk::single("row");
        let first_head = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let first_root = root("first-root");
        let second_root = root("second-root");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                None,
                first_head,
                first_root.clone(),
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("first-change"),
                    commit_id: first_head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":1}"),
                    metadata: JsonSlotRef::None,
                }],
                None,
            )
            .await
            .expect("stage first head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit first head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open second read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(first_head),
                Some(&first_root),
                second_head,
                second_root.clone(),
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-change"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                None,
            )
            .await
            .expect("stage second head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit second head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open verify read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &second_root,
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan second head")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(rows[0].updated_at, "2026-01-02T00:00:00.000Z");
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }

    #[tokio::test]
    async fn bootstrap_overlays_parent_identity_without_duplicate_write() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let entity_pk = EntityPk::single("row");
        let parent_head = CommitId::for_test_label("parent-head");
        let child_head = CommitId::for_test_label("child-head");
        let parent_root = root("parent-root");
        let child_root = root("child-root");
        let parent_rows = vec![MaterializedTrackedStateRow {
            entity_pk: entity_pk.clone(),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            change_id: ChangeId::for_test_label("parent-change"),
            commit_id: parent_head,
        }];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(parent_head),
                Some(&parent_root),
                child_head,
                child_root.clone(),
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("child-change"),
                    commit_id: child_head,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                Some(parent_rows),
            )
            .await
            .expect("stage bootstrap");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("overlapping bootstrap must commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open verify read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &child_head.to_string(),
                &child_root,
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan child head")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].change_id,
            Some(ChangeId::for_test_label("child-change"))
        );
        assert_eq!(rows[0].created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }
}
