//! Materialized serving state for one tracked branch head.
//!
//! Commit roots are sparse historical checkpoints. This table is the durable,
//! generation-keyed serving state for one branch head, letting the normal
//! live-state path range scan rows and hydrate JSON directly without replaying
//! changelog history. A marker binds a generation to the branch ref's commit.
//! Any mismatch is a cache miss and callers take the historical fallback.

use std::collections::{BTreeMap, BTreeSet};

use std::sync::Arc;

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
    MaterializedTrackedStateRow, TrackedStateFilter, TrackedStateKey, TrackedStateScanRequest,
};

// v5 makes the durable tracked head authoritative for normal current reads.
// A physical record owns every file-backed member of one logical entity PK.
// Public entity reads know `(branch, schema, entity_pk)` but intentionally do
// not invent a `file_id`; keeping those members together lets that common
// lookup be a RocksDB point get rather than a prefix scan. Repositories use a
// protocol gate, so v4 bytes are never interpreted as v5 groups.
pub(crate) const TRACKED_HEAD_GROUP_NAMESPACE: &str = "live_state.tracked_head_group.v5";
pub(crate) const TRACKED_HEAD_MEMBER_NAMESPACE: &str = "live_state.tracked_head_member.v5";
pub(crate) const TRACKED_HEAD_MARKER_NAMESPACE: &str = "live_state.tracked_head_marker.v5";
pub(crate) const TRACKED_HEAD_GROUP_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0012), TRACKED_HEAD_GROUP_NAMESPACE);
/// File-id projection for explicit file-backed identities.
///
/// The group value remains authoritative for normal logical-PK reads. This
/// narrow projection avoids turning `file_id = ?` reads into an unbounded
/// group-value fetch when a logical PK has many file members. Its physical
/// order is `(branch, generation, schema, file_id, entity_pk)`, so both an
/// exact full identity and a schema-scoped file-id scan avoid unpacking
/// unrelated entity groups.
pub(crate) const TRACKED_HEAD_MEMBER_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0013), TRACKED_HEAD_MEMBER_NAMESPACE);
pub(crate) const TRACKED_HEAD_MARKER_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0014), TRACKED_HEAD_MARKER_NAMESPACE);

/// Immutable manifest for the currently readable generation of a branch.
///
/// A new generation is used after a branch ref moves away from the parent of
/// a normal commit. Old rows can remain in storage: they are unreachable
/// without this marker and therefore cannot affect serving reads.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct TrackedHeadMarker {
    head_commit_id: CommitId,
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

/// The physical v5 key for all current members of one logical entity PK.
///
/// `file_id` is deliberately not a key part. It remains part of the packed
/// group value so a tombstone or a file-backed variant affects only its own
/// full row identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HeadGroupIdentity {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    entity_pk: EntityPk,
}

/// The portion of a head-row key that varies within one branch generation.
///
/// A full table scan already constrains `branch_id` and `generation` in the
/// RocksDB prefix. Keeping that immutable scope out of every decoded row
/// avoids parsing and allocating the same two key parts 10,000 times.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HeadRowIdentity {
    schema_key: String,
    entity_pk: EntityPk,
    file_id: Option<String>,
}

impl HeadIdentity {
    fn into_row_identity(self) -> HeadRowIdentity {
        HeadRowIdentity {
            schema_key: self.schema_key,
            entity_pk: self.entity_pk,
            file_id: self.file_id,
        }
    }

    fn into_group_identity(self) -> HeadGroupIdentity {
        HeadGroupIdentity {
            branch_id: self.branch_id,
            generation: self.generation,
            schema_key: self.schema_key,
            entity_pk: self.entity_pk,
        }
    }

    fn group_identity(&self) -> HeadGroupIdentity {
        HeadGroupIdentity {
            branch_id: self.branch_id.clone(),
            generation: self.generation,
            schema_key: self.schema_key.clone(),
            entity_pk: self.entity_pk.clone(),
        }
    }
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
    /// branch ref. That is a cache miss, not empty tracked state.
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let Some(marker) = self.marker_if_current(branch_id, expected_head).await? else {
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
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        if keys.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let Some(marker) = self.marker_if_current(branch_id, expected_head).await? else {
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
        let groups = output_indices
            .keys()
            .filter(|identity| identity.file_id.is_none())
            .map(HeadIdentity::group_identity)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let member_identities = output_indices
            .keys()
            .filter(|identity| identity.file_id.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let values = load_group_bytes(&self.store, &groups).await?;
        let mut entries = Vec::new();
        for (group, value) in groups.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            for member in decode_head_group_members(&value)? {
                let identity = HeadIdentity {
                    branch_id: group.branch_id.clone(),
                    generation: group.generation,
                    schema_key: group.schema_key.clone(),
                    entity_pk: group.entity_pk.clone(),
                    file_id: member.file_id,
                };
                if output_indices.contains_key(&identity) {
                    entries.push((identity.into_row_identity(), member.value));
                }
            }
        }
        let member_values = load_member_bytes(&self.store, &member_identities).await?;
        for (identity, value) in member_identities.into_iter().zip(member_values) {
            if let Some(value) = value {
                entries.push((identity.into_row_identity(), value));
            }
        }
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;
        let rows_by_identity = rows
            .into_iter()
            .map(|row| {
                (
                    HeadRowIdentity {
                        schema_key: row.schema_key.clone(),
                        entity_pk: row.entity_pk.clone(),
                        file_id: row.file_id.clone(),
                    },
                    row,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut output = vec![None; keys.len()];
        for (identity, indices) in output_indices {
            if let Some(row) = rows_by_identity.get(&identity.into_row_identity()) {
                for index in indices {
                    output[index] = Some(row.clone());
                }
            }
        }
        Ok(Some(output))
    }

    /// Returns the durable serving generation exactly when the marker is
    /// bound to `expected_head`. Commit staging passes this value directly to
    /// the writer so a serial child needs one marker point read, not two.
    pub(crate) async fn generation_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self
            .marker_if_current(branch_id, expected_head)
            .await?
            .map(|marker| marker.generation))
    }

    async fn marker_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
    ) -> Result<Option<TrackedHeadMarker>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "tracked-head expected commit")?;
        let marker = load_marker(&self.store, branch_id).await?;
        Ok(marker.filter(|marker| marker.head_commit_id == expected_head))
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
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<(), LixError> {
        let matches_parent = parent_generation.is_some();
        let generation = parent_generation.unwrap_or(new_head);

        let mut seen_delta_identities = BTreeSet::new();
        for delta in deltas {
            let identity = delta.identity(branch_id, generation);
            if !seen_delta_identities.insert(identity.clone()) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-head commit contains duplicate mutation for schema '{}' entity_pk '{:?}' file_id '{:?}'",
                        identity.schema_key, identity.entity_pk, identity.file_id
                    ),
                ));
            }
        }

        let mut groups = if matches_parent {
            let group_identities = deltas
                .iter()
                .map(|delta| delta.identity(branch_id, generation).into_group_identity())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let values = load_group_bytes(self.store, &group_identities).await?;
            let mut groups = BTreeMap::new();
            for (identity, value) in group_identities.into_iter().zip(values) {
                let members = value
                    .as_deref()
                    .map(decode_head_group_member_map)
                    .transpose()?
                    .unwrap_or_default();
                groups.insert(identity, members);
            }
            groups
        } else {
            let mut groups =
                BTreeMap::<HeadGroupIdentity, BTreeMap<Option<String>, Vec<u8>>>::new();
            for row in parent_rows.unwrap_or_default() {
                let key = TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    entity_pk: row.entity_pk.clone(),
                    file_id: row.file_id.clone(),
                };
                if absence_guards.contains(&key) && !row.deleted {
                    return Err(tracked_head_duplicate_insert_error(&key));
                }
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
                let members = groups.entry(identity.group_identity()).or_default();
                if members
                    .insert(
                        identity.file_id.clone(),
                        encode_head_value(&value.as_ref())?,
                    )
                    .is_some()
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-head bootstrap contains duplicate full row identity",
                    ));
                }
            }
            groups
        };

        for delta in deltas {
            let identity = delta.identity(branch_id, generation);
            let key = TrackedStateKey {
                schema_key: identity.schema_key.clone(),
                entity_pk: identity.entity_pk.clone(),
                file_id: identity.file_id.clone(),
            };
            let members = groups.entry(identity.group_identity()).or_default();
            let created_at = if let Some(existing) = members.get(&identity.file_id) {
                let existing = decode_head_value(existing)?;
                if absence_guards.contains(&key) && !existing.deleted {
                    return Err(tracked_head_duplicate_insert_error(&key));
                }
                existing.created_at
            } else {
                delta.created_at
            };
            members.insert(
                identity.file_id,
                encode_head_value(&delta.value_ref(created_at))?,
            );
        }

        self.writes
            .reserve_space(TRACKED_HEAD_GROUP_SPACE, groups.len(), 0);
        let explicit_member_count = groups
            .values()
            .map(|members| members.keys().filter(|file_id| file_id.is_some()).count())
            .sum();
        self.writes
            .reserve_space(TRACKED_HEAD_MEMBER_SPACE, explicit_member_count, 0);
        for (identity, members) in groups {
            stage_put_group_members(self.writes, &identity, &members)?;
            for (file_id, value) in &members {
                if file_id.is_some() {
                    stage_put_member_bytes(self.writes, &identity, file_id.as_deref(), value)?;
                }
            }
        }
        stage_marker(
            self.writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: new_head,
                generation,
            },
        )?;
        Ok(())
    }
}

fn tracked_head_duplicate_insert_error(key: &TrackedStateKey) -> LixError {
    let entity_pk = key
        .entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate entity_pk '{entity_pk}'",
            key.schema_key
        ),
    )
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

/// Loads packed v5 groups without materializing their members.
async fn load_group_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadGroupIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = identities
        .iter()
        .map(|identity| StorageKey(Bytes::from(encode_group_key(identity))))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(TRACKED_HEAD_GROUP_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

/// Loads the explicit-file member projection. Its physical access pattern is
/// intentionally the same single-key point lookup as v4 so file-id queries
/// do not become proportional to the size of their logical PK group.
async fn load_member_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    debug_assert!(identities.iter().all(|identity| identity.file_id.is_some()));
    let keys = identities
        .iter()
        .map(|identity| StorageKey(Bytes::from(encode_member_key(identity))))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(TRACKED_HEAD_MEMBER_SPACE, &keys)
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
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    if let Some(identities) = exact_explicit_member_identities(branch_id, generation, filter) {
        let values = load_member_bytes(store, &identities).await?;
        return Ok(identities
            .into_iter()
            .zip(values)
            .filter_map(|(identity, value)| {
                value.map(|value| (identity.into_row_identity(), value))
            })
            .take(limit.unwrap_or(usize::MAX))
            .collect());
    }
    if let Some(prefixes) = explicit_member_scan_prefixes(branch_id, generation, filter) {
        let mut rows = scan_explicit_member_entries(store, prefixes, filter).await?;
        // Member projection keys are ordered by `file_id` before `entity_pk`.
        // Restore the public logical order when callers request multiple file
        // ids; the group route below is already ordered that way.
        rows.sort_by(|(left, _), (right, _)| left.cmp(right));
        rows.dedup_by(|(left, _), (right, _)| left == right);
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        return Ok(rows);
    }
    if let Some(groups) = exact_group_identities(branch_id, generation, filter) {
        let values = load_group_bytes(store, &groups).await?;
        let mut rows = Vec::new();
        for (group, value) in groups.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            extend_group_entries(&mut rows, group, value, filter, limit)?;
            if limit.is_some_and(|limit| rows.len() >= limit) {
                return Ok(rows);
            }
        }
        return Ok(rows);
    }

    let scope = encode_scope_prefix(branch_id, generation);
    let mut prefixes = scan_prefixes(&scope, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            TRACKED_HEAD_GROUP_SPACE,
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
                let identity = decode_group_key_in_scope(entry.key.0.as_ref(), &scope)?;
                extend_group_entries(
                    &mut rows,
                    HeadGroupIdentity {
                        branch_id: branch_id.to_string(),
                        generation,
                        schema_key: identity.schema_key,
                        entity_pk: identity.entity_pk,
                    },
                    full_value_bytes(entry.value)?,
                    filter,
                    limit,
                )?;
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

fn exact_group_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadGroupIdentity>> {
    if filter.schema_keys.is_empty() || filter.entity_pks.is_empty() {
        return None;
    }
    let mut identities = Vec::with_capacity(filter.schema_keys.len() * filter.entity_pks.len());
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            identities.push(HeadGroupIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: schema_key.clone(),
                entity_pk: entity_pk.clone(),
            });
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

fn exact_explicit_member_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadIdentity>> {
    exact_group_identities(branch_id, generation, filter)?;
    if filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut identities = Vec::with_capacity(
        filter.schema_keys.len() * filter.entity_pks.len() * filter.file_ids.len(),
    );
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            for file_id in &filter.file_ids {
                let NullableKeyFilter::Value(file_id) = file_id else {
                    unreachable!("explicit member filter checked above");
                };
                identities.push(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: schema_key.clone(),
                    entity_pk: entity_pk.clone(),
                    file_id: Some(file_id.clone()),
                });
            }
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

/// A schema-scoped `file_id = ?` lookup cannot use the grouped primary
/// serving record without decoding every logical PK in that schema. Explicit
/// member rows use a file-id-first suffix solely for this access pattern.
///
/// Exact `(schema, entity_pk, file_id)` requests take the point-read route
/// above. If the schema is unknown, no useful member-space prefix exists and
/// we correctly retain the general group scan fallback.
fn explicit_member_scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<Vec<u8>>> {
    if filter.schema_keys.is_empty()
        || filter.file_ids.is_empty()
        || !filter.entity_pks.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut prefixes = Vec::with_capacity(filter.schema_keys.len() * filter.file_ids.len());
    for schema_key in &filter.schema_keys {
        for file_id in &filter.file_ids {
            let NullableKeyFilter::Value(file_id) = file_id else {
                unreachable!("explicit member scan filter checked above");
            };
            let mut prefix = encode_scope_prefix(branch_id, generation);
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            write_file_id(&mut prefix, Some(file_id));
            prefixes.push(prefix);
        }
    }
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

async fn scan_explicit_member_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    prefixes: Vec<Vec<u8>>,
    filter: &TrackedStateFilter,
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            TRACKED_HEAD_MEMBER_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_member_key(entry.key.0.as_ref())?.into_row_identity();
                if matches_filter(&identity, filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

fn scan_prefixes(scope: &[u8], filter: &TrackedStateFilter) -> Vec<Vec<u8>> {
    if filter.schema_keys.is_empty() {
        return vec![scope.to_vec()];
    }
    let mut prefixes = Vec::new();
    for schema_key in &filter.schema_keys {
        let mut schema_prefix = scope.to_vec();
        write_key_string(&mut schema_prefix, schema_key, KEY_PART_FINAL);
        if filter.entity_pks.is_empty() {
            prefixes.push(schema_prefix);
            continue;
        }
        for entity_pk in &filter.entity_pks {
            let mut entity_prefix = schema_prefix.clone();
            write_entity_pk(&mut entity_prefix, entity_pk);
            prefixes.push(entity_prefix);
        }
    }
    prefixes
}

fn extend_group_entries(
    rows: &mut Vec<(HeadRowIdentity, Bytes)>,
    group: HeadGroupIdentity,
    value: Bytes,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<(), LixError> {
    for member in decode_head_group_members(&value)? {
        let identity = HeadRowIdentity {
            schema_key: group.schema_key.clone(),
            entity_pk: group.entity_pk.clone(),
            file_id: member.file_id,
        };
        if matches_filter(&identity, filter) {
            rows.push((identity, member.value));
            if limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
    }
    Ok(())
}

fn matches_filter(identity: &HeadRowIdentity, filter: &TrackedStateFilter) -> bool {
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

#[cfg(test)]
fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    stage_put_ref(writes, identity, &value.as_ref())
}

#[cfg(test)]
fn stage_put_ref(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValueRef<'_>,
) -> Result<(), LixError> {
    let mut members = BTreeMap::new();
    members.insert(identity.file_id.clone(), encode_head_value(value)?);
    stage_put_group_members(writes, &identity.group_identity(), &members)?;
    if identity.file_id.is_some() {
        stage_put_member_bytes(
            writes,
            &identity.group_identity(),
            identity.file_id.as_deref(),
            &members[&identity.file_id],
        )?;
    }
    Ok(())
}

fn stage_put_group_members(
    writes: &mut StorageWriteSet,
    identity: &HeadGroupIdentity,
    members: &BTreeMap<Option<String>, Vec<u8>>,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_HEAD_GROUP_SPACE,
        StorageKey(Bytes::from(encode_group_key(identity))),
        StorageValue {
            bytes: Bytes::from(encode_head_group_members(members)?),
        },
    );
    Ok(())
}

fn stage_put_member_bytes(
    writes: &mut StorageWriteSet,
    group: &HeadGroupIdentity,
    file_id: Option<&str>,
    value: &[u8],
) -> Result<(), LixError> {
    let Some(file_id) = file_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked-head explicit member projection requires a file_id",
        ));
    };
    let identity = HeadIdentity {
        branch_id: group.branch_id.clone(),
        generation: group.generation,
        schema_key: group.schema_key.clone(),
        entity_pk: group.entity_pk.clone(),
        file_id: Some(file_id.to_string()),
    };
    writes.put(
        TRACKED_HEAD_MEMBER_SPACE,
        StorageKey(Bytes::from(encode_member_key(&identity))),
        StorageValue {
            bytes: Bytes::copy_from_slice(value),
        },
    );
    Ok(())
}

fn marker_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("tracked-head marker key", &BranchRef { branch_id })
}

fn encode_group_key(identity: &HeadGroupIdentity) -> Vec<u8> {
    let mut out = encode_scope_prefix(&identity.branch_id, identity.generation);
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut out, &identity.entity_pk);
    out
}

fn encode_member_key(identity: &HeadIdentity) -> Vec<u8> {
    debug_assert!(identity.file_id.is_some());
    let mut out = encode_scope_prefix(&identity.branch_id, identity.generation);
    write_key_string(&mut out, &identity.schema_key, KEY_PART_FINAL);
    write_file_id(&mut out, identity.file_id.as_deref());
    write_entity_pk(&mut out, &identity.entity_pk);
    out
}

#[cfg(test)]
fn decode_group_key(bytes: &[u8]) -> Result<HeadGroupIdentity, LixError> {
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
    if offset != bytes.len() {
        return Err(key_codec_error("group key has trailing bytes"));
    }
    Ok(HeadGroupIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
    })
}

fn decode_member_key(bytes: &[u8]) -> Result<HeadIdentity, LixError> {
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
    let file_id = read_file_id(bytes, &mut offset)?;
    if file_id.is_none() {
        return Err(key_codec_error("member key must contain a file id"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("member key has trailing bytes"));
    }
    Ok(HeadIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
        file_id,
    })
}

/// Decodes only the mutable suffix of a group key from a prefix-scoped scan.
///
/// `ScanPlan::prefix` already constrains the branch and generation. We still
/// verify the fixed scope before parsing the suffix so a malformed storage key
/// cannot be interpreted as a row from the wrong generation.
fn decode_group_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "does not begin with the scanned branch-generation scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error("schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("group key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id: None,
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
    let start = *offset;
    let mut cursor = start;
    // The normal generated IDs do not contain the escaped NUL byte. Decode
    // that common case directly from the RocksDB key instead of first growing
    // a temporary `Vec<u8>` one byte at a time.
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            let value = std::str::from_utf8(&bytes[start..cursor - 2])
                .map_err(|error| key_codec_error(&format!("{field} is not UTF-8: {error}")))?;
            *offset = cursor;
            return Ok((value.to_owned(), terminator));
        }
        break;
    }

    // Escaped NUL bytes are rare but remain fully supported. Seed the owned
    // buffer with the prefix before the first escape, then decode the rest.
    let mut out = Vec::with_capacity(cursor.saturating_sub(start) + 16);
    out.extend_from_slice(&bytes[start..cursor - 2]);
    out.push(KEY_PART_FINAL);
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            out.push(byte);
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator == KEY_ESCAPE {
            out.push(KEY_PART_FINAL);
            continue;
        }
        let value = String::from_utf8(out).map_err(|error| {
            key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
        })?;
        *offset = cursor;
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

/// v5 packs all file-backed members of one logical entity PK into one
/// canonical current-state group. The individual member payload is the proven
/// fixed-header v3 value below; only the outer framing is new.
///
/// ```text
///  0      group format version (1)
///  1..5   member count (big endian u32)
///  repeated:
///    0      file-id tag (0 = none, 1 = UTF-8 string)
///    1..5   file-id byte length when tag = 1 (big endian u32)
///    ...    file-id UTF-8 bytes when tag = 1
///    ...    v3 member byte length (big endian u32)
///    ...    v3 member bytes
/// ```
///
/// Members are strictly sorted by Rust's `Option<String>` ordering. This
/// makes scans deterministic and, more importantly, rejects silently
/// duplicated full identities at the storage boundary.
const HEAD_GROUP_VALUE_VERSION: u8 = 1;
const HEAD_GROUP_HEADER_BYTES: usize = 5;

struct HeadGroupMemberBytes {
    file_id: Option<String>,
    value: Bytes,
}

fn encode_head_group_members(
    members: &BTreeMap<Option<String>, Vec<u8>>,
) -> Result<Vec<u8>, LixError> {
    let member_count =
        u32::try_from(members.len()).map_err(|_| head_group_error("member count exceeds u32"))?;
    let payload_len = members.iter().try_fold(0usize, |total, (file_id, value)| {
        let file_id_len = file_id.as_ref().map_or(0, String::len);
        u32::try_from(file_id_len).map_err(|_| head_group_error("file id exceeds u32"))?;
        let file_id_bytes = if file_id.is_some() {
            4usize
                .checked_add(file_id_len)
                .ok_or_else(|| head_group_error("group value length overflow"))?
        } else {
            0
        };
        u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
        decode_head_value(value)?;
        let member_len = 1usize
            .checked_add(file_id_bytes)
            .and_then(|length| length.checked_add(4))
            .and_then(|length| length.checked_add(value.len()))
            .ok_or_else(|| head_group_error("group value length overflow"))?;
        total
            .checked_add(member_len)
            .ok_or_else(|| head_group_error("group value length overflow"))
    })?;
    let capacity = HEAD_GROUP_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| head_group_error("group value length overflow"))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.push(HEAD_GROUP_VALUE_VERSION);
    encoded.extend_from_slice(&member_count.to_be_bytes());
    for (file_id, value) in members {
        match file_id {
            None => encoded.push(FILE_ID_NONE),
            Some(file_id) => {
                encoded.push(FILE_ID_SOME);
                let file_id_len = u32::try_from(file_id.len())
                    .map_err(|_| head_group_error("file id exceeds u32"))?;
                encoded.extend_from_slice(&file_id_len.to_be_bytes());
                encoded.extend_from_slice(file_id.as_bytes());
            }
        }
        let value_len =
            u32::try_from(value.len()).map_err(|_| head_group_error("member value exceeds u32"))?;
        encoded.extend_from_slice(&value_len.to_be_bytes());
        encoded.extend_from_slice(value);
    }
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

fn decode_head_group_members(bytes: &[u8]) -> Result<Vec<HeadGroupMemberBytes>, LixError> {
    if bytes.len() < HEAD_GROUP_HEADER_BYTES {
        return Err(head_group_error("value is shorter than the fixed header"));
    }
    if bytes[0] != HEAD_GROUP_VALUE_VERSION {
        return Err(head_group_error(&format!(
            "unsupported group format version {}",
            bytes[0]
        )));
    }
    let member_count = usize::try_from(read_u32(&bytes[1..5], "group member count")?)
        .map_err(|_| head_group_error("member count exceeds usize"))?;
    let mut offset = HEAD_GROUP_HEADER_BYTES;
    let mut prior_file_id = None::<Option<String>>;
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        let tag = *bytes
            .get(offset)
            .ok_or_else(|| head_group_error("is truncated before member file id"))?;
        offset += 1;
        let file_id = match tag {
            FILE_ID_NONE => None,
            FILE_ID_SOME => {
                let file_id_len = read_group_u32(bytes, &mut offset, "member file-id length")?;
                let file_id_end = offset
                    .checked_add(file_id_len)
                    .ok_or_else(|| head_group_error("member file-id length overflow"))?;
                let file_id = bytes
                    .get(offset..file_id_end)
                    .ok_or_else(|| head_group_error("is truncated in member file id"))?;
                offset = file_id_end;
                Some(
                    std::str::from_utf8(file_id)
                        .map_err(|error| {
                            head_group_error(&format!("member file id is not UTF-8: {error}"))
                        })?
                        .to_string(),
                )
            }
            _ => return Err(head_group_error("has an invalid member file-id tag")),
        };
        if prior_file_id
            .as_ref()
            .is_some_and(|prior| prior >= &file_id)
        {
            return Err(head_group_error(
                "members are not strictly ordered by file id",
            ));
        }
        let value_len = read_group_u32(bytes, &mut offset, "member value length")?;
        let value_end = offset
            .checked_add(value_len)
            .ok_or_else(|| head_group_error("member value length overflow"))?;
        let value = bytes
            .get(offset..value_end)
            .ok_or_else(|| head_group_error("is truncated in member value"))?;
        decode_head_value(value)?;
        offset = value_end;
        prior_file_id = Some(file_id.clone());
        members.push(HeadGroupMemberBytes {
            file_id,
            value: Bytes::copy_from_slice(value),
        });
    }
    if offset != bytes.len() {
        return Err(head_group_error("has trailing bytes"));
    }
    Ok(members)
}

fn decode_head_group_member_map(
    bytes: &[u8],
) -> Result<BTreeMap<Option<String>, Vec<u8>>, LixError> {
    Ok(decode_head_group_members(bytes)?
        .into_iter()
        .map(|member| (member.file_id, member.value.to_vec()))
        .collect())
}

fn read_group_u32(bytes: &[u8], offset: &mut usize, field: &str) -> Result<usize, LixError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| head_group_error(&format!("{field} offset overflow")))?;
    let value = read_u32(
        bytes
            .get(*offset..end)
            .ok_or_else(|| head_group_error(&format!("is truncated before {field}")))?,
        field,
    )?;
    *offset = end;
    usize::try_from(value).map_err(|_| head_group_error(&format!("{field} exceeds usize")))
}

fn head_group_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head v5 group: {message}"),
    )
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
    entries: Vec<(HeadRowIdentity, Bytes)>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let branch_id = Arc::<str>::from(branch_id);
    let global = branch_id.as_ref() == crate::GLOBAL_BRANCH_ID;
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
            created_at: value.created_at,
            updated_at: value.updated_at,
            global,
            change_id: Some(value.change_id),
            commit_id: Some(value.commit_id),
            untracked: false,
            branch_id: Arc::clone(&branch_id),
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

    #[test]
    fn v5_group_codec_roundtrips_sorted_members_and_rejects_corruption() {
        let mut members = BTreeMap::new();
        members.insert(
            None,
            encode_head_value(&head_value("none", CommitId::for_test_label("head")).as_ref())
                .expect("encode none member"),
        );
        members.insert(
            Some("file-a".to_string()),
            encode_head_value(&head_value("file-a", CommitId::for_test_label("head")).as_ref())
                .expect("encode file-a member"),
        );
        members.insert(
            Some("file-b".to_string()),
            encode_head_value(&head_value("file-b", CommitId::for_test_label("head")).as_ref())
                .expect("encode file-b member"),
        );

        let encoded = encode_head_group_members(&members).expect("encode group");
        let decoded = decode_head_group_members(&encoded).expect("decode group");
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].file_id, None);
        assert_eq!(decoded[1].file_id.as_deref(), Some("file-a"));
        assert_eq!(decoded[2].file_id.as_deref(), Some("file-b"));
        assert_eq!(
            decode_head_value(&decoded[2].value)
                .expect("member should preserve v3 payload")
                .change_id,
            ChangeId::for_test_label("file-b")
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_head_group_members(&trailing).is_err());

        let mut bad_version = encoded;
        bad_version[0] = HEAD_GROUP_VALUE_VERSION + 1;
        assert!(decode_head_group_members(&bad_version).is_err());
    }

    #[tokio::test]
    async fn direct_live_materializer_honors_projection_and_batches_out_of_band_refs() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
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
        assert_eq!(metadata_only[0].branch_id.as_ref(), branch_id);
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

    #[tokio::test]
    async fn explicit_file_id_reads_use_single_member_projection() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let head = CommitId::for_test_label("head");
        let entity_pk = EntityPk::single("row");
        let second_entity_pk = EntityPk::single("row-2");
        let deltas = [
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: None,
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("none"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"none\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-a"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("file-a"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"a\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-b"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("file-b"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"b\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("file-b"),
                entity_pk: &second_entity_pk,
                change_id: ChangeId::for_test_label("second-file-b"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"second-b\"}"),
                metadata: JsonSlotRef::None,
            },
        ];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open write read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(branch_id, None, head, &deltas, &BTreeSet::new(), None)
            .await
            .expect("stage grouped head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit grouped head");

        let group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
        };
        let second_group = HeadGroupIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: second_entity_pk.clone(),
        };
        let explicit_member = HeadIdentity {
            branch_id: branch_id.to_string(),
            generation: head,
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: Some("file-b".to_string()),
        };
        let member_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open member verification read");
        let member = PointReadPlan::new(
            TRACKED_HEAD_MEMBER_SPACE,
            &[StorageKey(Bytes::from(encode_member_key(&explicit_member)))],
        )
        .materialize(&member_read, StorageGetOptions::default())
        .await
        .expect("member projection should load")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(
            member.is_some(),
            "explicit file member needs a point record"
        );
        drop(member_read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open logical PK read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![entity_pk.clone()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("logical PK read should execute")
            .expect("marker should match");
        assert_eq!(
            rows.iter()
                .map(|row| row.file_id.as_deref())
                .collect::<Vec<_>>(),
            vec![None, Some("file-a"), Some("file-b")]
        );

        // Remove only the group to prove the exact-file read never needs to
        // fetch/parse sibling members. This is intentionally an impossible
        // committed state in production; it validates the physical route.
        let mut writes = StorageWriteSet::new();
        writes.delete(
            TRACKED_HEAD_GROUP_SPACE,
            StorageKey(Bytes::from(encode_group_key(&group))),
        );
        writes.delete(
            TRACKED_HEAD_GROUP_SPACE,
            StorageKey(Bytes::from(encode_group_key(&second_group))),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("remove group for route proof");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open filtered file scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![entity_pk.clone()],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("filtered file scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_id.as_deref(), Some("file-b"));

        // A schema-scoped `file_id = ?` query also stays on the member
        // projection. This is the access pattern used by filesystem-backed
        // entity scans, where the entity PK is not known before the query.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-id scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value("file-b".to_string())],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("file-id scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| row.entity_pk.as_single_string().expect("single key"))
                .collect::<Vec<_>>(),
            vec!["row", "row-2"]
        );
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("file-b"))
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open explicit file read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    entity_pk,
                    file_id: Some("file-b".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_ref().expect("explicit member should resolve");
        assert_eq!(row.file_id.as_deref(), Some("file-b"));
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));
    }

    #[test]
    fn group_and_member_keys_roundtrip_and_preserve_logical_order() {
        let generation = CommitId::for_test_label("generation");
        let strings = ["", "\0", "a", "a\0", "a\u{1}", "z", "é"];
        let mut groups = Vec::new();
        for schema_key in strings {
            for entity_first in strings {
                for entity_pk in [
                    EntityPk::single(entity_first),
                    EntityPk::from_parts(vec![entity_first.to_string(), "tail".to_string()])
                        .expect("tuple entity key should be valid"),
                ] {
                    groups.push(HeadGroupIdentity {
                        branch_id: "branch\0name".to_string(),
                        generation,
                        schema_key: schema_key.to_string(),
                        entity_pk: entity_pk.clone(),
                    });
                }
            }
        }
        groups.sort();
        groups.dedup();

        for identity in &groups {
            let encoded = encode_group_key(identity);
            assert_eq!(
                decode_group_key(&encoded).expect("group key should decode"),
                *identity
            );
            let scope = encode_scope_prefix(&identity.branch_id, identity.generation);
            assert_eq!(
                decode_group_key_in_scope(&encoded, &scope)
                    .expect("scope-decoded row key should decode"),
                HeadRowIdentity {
                    schema_key: identity.schema_key.clone(),
                    entity_pk: identity.entity_pk.clone(),
                    file_id: None,
                }
            );
        }

        let mut by_encoded = groups
            .iter()
            .cloned()
            .map(|identity| (encode_group_key(&identity), identity))
            .collect::<Vec<_>>();
        by_encoded.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            by_encoded
                .iter()
                .map(|(_, identity)| identity)
                .collect::<Vec<_>>(),
            groups.iter().collect::<Vec<_>>()
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

        let member = HeadIdentity {
            branch_id: "branch\0name".to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("entity"),
            file_id: Some("file\0id".to_string()),
        };
        assert_eq!(
            decode_member_key(&encode_member_key(&member)).expect("member key should decode"),
            member
        );
    }

    #[tokio::test]
    async fn head_scan_is_logically_ordered_and_unique() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
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
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan")
            .expect("marker should match");
        assert_eq!(rows.len(), expected.len());
        assert!(
            Arc::ptr_eq(&rows[0].branch_id, &rows[1].branch_id),
            "one head scan should share its branch allocation across rows"
        );
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
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &value).expect("stage row");
        stage_marker(
            &mut writes,
            "branch",
            &TrackedHeadMarker {
                head_commit_id: head,
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
                first_head,
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
                &BTreeSet::new(),
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
                second_head,
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
                &BTreeSet::new(),
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
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan second head")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].updated_at, ts("2026-01-02T00:00:00Z"));
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }

    #[tokio::test]
    async fn incremental_singleton_insert_rejects_existing_live_row() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut writes = StorageWriteSet::new();
        stage_put(
            &mut writes,
            &identity,
            &head_value("first-change", generation),
        )
        .expect("stage existing live row");
        stage_marker(
            &mut writes,
            branch_id,
            &TrackedHeadMarker {
                head_commit_id: generation,
                generation,
            },
        )
        .expect("stage existing head marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit existing head");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open singleton insert read");
        let mut writes = StorageWriteSet::new();
        let absence_guards = BTreeSet::from([TrackedStateKey {
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: None,
        }]);
        let error = TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
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
                &absence_guards,
                None,
            )
            .await
            .expect_err("singleton INSERT must reject an existing live row");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn bootstrap_overlays_parent_identity_without_duplicate_write() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let entity_pk = EntityPk::single("row");
        let parent_head = CommitId::for_test_label("parent-head");
        let child_head = CommitId::for_test_label("child-head");
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
                None,
                child_head,
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
                &BTreeSet::new(),
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
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
    }
}
