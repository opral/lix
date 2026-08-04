//! Unified, materialized live state for one branch head.
//!
//! The V21 hot state has one authoritative file-first row per full identity
//! plus one conservative file-membership marker per schema. Each row is tagged
//! `tracked` or `untracked`: tracked mutations also enter history, while
//! untracked mutations exist only in this serving plane. Normal reads consult
//! this single row index rather than merging a tracked snapshot with an
//! untracked overlay.

mod hot;

pub(crate) use hot::{
    ArrowIdentityMembership, EntityColumnarGroupSource, EntityColumnarOverlayRow, HOT_FILE_SPACE,
    HOT_ROW_SPACE, HotStateTransactionCache, HotTrackedSnapshot, ROOT_CURRENT_BASE_SPACE,
    materialize_certified_root_rows,
};

/// Stable physical address of a row in immutable Arrow-native state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColumnarBaseCoordinate {
    pub(crate) state_set_id: crate::columnar_row_group::ArrowStateSetId,
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::LixError;
use crate::NullableKeyFilter;
use crate::branch::BranchHeadControl;
#[cfg(test)]
use crate::branch::stage_branch_head_control;
use crate::changelog::{ChangeId, ChangeRecordProjection, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
#[cfg(test)]
use crate::json_store::JsonSlot;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlotRef, JsonStoreContext, JsonStoreWriter,
};
use crate::live_state::{
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
    MaterializedLiveStateRow, MaterializedLiveStateRowRef,
};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions,
    StorageKey, StoragePrefix, StorageProjectedValue, StorageScanOptions, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::storage_codec;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDiff, TrackedStateDiffRequest, TrackedStateFilter,
    TrackedStateKey, TrackedStateKeyRef, TrackedStateScanRequest,
};

/// A checkpoint-relative direct diff assembled from the current-state
/// generation.
/// This is internal plumbing for SQL working-diff and checkpoint compaction;
/// the public API remains the existing tracked-state diff representation.
pub(crate) struct TrackedWorkingDiff {
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) diff: TrackedStateDiff,
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
}

/// Test-only owned representation of a hot current-state row.
///
/// This exists only while a transaction is being staged. Read-side code uses
/// [`HeadValueView`], which parses the fixed header directly from RocksDB's
/// returned bytes and never builds this allocation-heavy representation.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeadValue {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlot,
    metadata: JsonSlot,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

#[cfg(test)]
impl HeadValue {
    fn as_ref(&self) -> HeadValueRef<'_> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.untracked,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot.as_ref_slot(),
            metadata: self.metadata.as_ref_slot(),
            columnar_base_coordinate: self.columnar_base_coordinate,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueRef<'a> {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: JsonSlotRef<'a>,
    metadata: JsonSlotRef<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// Zero-copy tracked mutation staged into a current-state generation.
///
/// This narrow convenience type keeps historical writers explicit. Normal
/// serving publication converts it to [`CurrentStateDeltaRef`], which is also
/// able to carry history-free untracked mutations.
#[cfg(any(test, feature = "storage-benches"))]
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

#[cfg(any(test, feature = "storage-benches"))]
impl<'a> TrackedHeadDeltaRef<'a> {
    fn as_current(&self) -> CurrentStateDeltaRef<'a> {
        CurrentStateDeltaRef {
            schema_key: self.schema_key,
            file_id: self.file_id,
            entity_pk: self.entity_pk,
            change_id: Some(self.change_id),
            commit_id: Some(self.commit_id),
            untracked: false,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot: self.snapshot,
            metadata: self.metadata,
            columnar_base_coordinate: None,
        }
    }
}

/// One mutation of the authoritative current serving state.
///
/// `tracked` mutations have both IDs and may create tombstones. `untracked`
/// mutations have neither ID; deletion removes the member physically. This
/// is deliberately the single write representation for the hot state plane,
/// so callers never stage a separate untracked overlay.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CurrentStateDeltaRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
    pub(crate) columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// Durable exact-read evidence aligned with a transaction delta.
///
/// The branch-control CAS protects this predecessor through publication. A
/// writer may therefore reuse the resolved value instead of issuing the same
/// root or HOT point read again during commit materialization.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CertifiedCurrentStatePredecessorRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) value: &'a CertifiedCurrentStatePredecessor,
}

#[derive(Debug, Clone)]
pub(crate) enum CertifiedCurrentStatePredecessor {
    Encoded(Bytes),
    ArrowRoot(ArrowRootHeadValue),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowRootHeadValue {
    change_id: ChangeId,
    commit_id: CommitId,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

impl<'a> CurrentStateDeltaRef<'a> {
    fn value_ref(&self, created_at: LixTimestamp) -> HeadValueRef<'a> {
        HeadValueRef {
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.untracked,
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
            columnar_base_coordinate: self.columnar_base_coordinate,
        }
    }

    fn validate(self) -> Result<(), LixError> {
        match (self.untracked, self.change_id, self.commit_id, self.deleted) {
            (false, Some(_), Some(_), _) | (true, None, None, false | true) => Ok(()),
            (false, _, _, _) => Err(head_value_error(
                "tracked current-state mutation must carry change_id and commit_id",
            )),
            (true, _, _, _) => Err(head_value_error(
                "untracked current-state mutation must not carry change_id or commit_id",
            )),
        }
    }

    fn physically_deletes(self) -> bool {
        self.untracked && self.deleted
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
    pub(crate) fn reader<S>(&self, store: S) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: None,
            decoded_columns: None,
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn cached_reader<S>(
        &self,
        store: S,
        decoded_columns: crate::live_state::EntityDecodedColumnCache,
    ) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: None,
            decoded_columns: Some(decoded_columns),
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn transaction_reader<S>(
        &self,
        store: S,
        cache: Arc<HotStateTransactionCache>,
    ) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: Some(cache),
            decoded_columns: None,
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn cached_transaction_reader<S>(
        &self,
        store: S,
        cache: Arc<HotStateTransactionCache>,
        decoded_columns: crate::live_state::EntityDecodedColumnCache,
    ) -> hot::HotStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        hot::HotStateStoreReader {
            store,
            transaction_cache: Some(cache),
            decoded_columns: Some(decoded_columns),
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> hot::HotStateWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        hot::HotStateWriter { store, writes }
    }

    /// Reclaims derived current-state generations that no durable branch
    /// control can select and returns their history-free payload refs. Both
    /// the authoritative hot rows and their key-only file membership index are
    /// generation-scoped, so the control is the one ownership root for both
    /// spaces. The caller compares the returned refs with its complete live
    /// payload set before staging physical JSON deletion.
    ///
    /// A non-current control still owns its generation. Committed tracked
    /// state remains reachable through the immutable commit root, while this
    /// generation preserves the branch's history-free untracked members until
    /// a fresh serving generation is published.
    pub(crate) async fn stage_collect_stale_current_state_generations<S>(
        &self,
        store: &S,
        writes: &mut StorageWriteSet,
        controls: &[(String, BranchHeadControl)],
    ) -> Result<Vec<JsonRef>, LixError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        hot::stage_collect_stale_hot_generations(store, writes, controls).await
    }
}

/// Converts the branch-control plane into the exact derived generations that
/// are still reachable. A branch generation is meaningful only together with
/// its branch id; a generation UUID alone is not a repository-global root.
fn active_current_state_generations(
    controls: &[(String, BranchHeadControl)],
) -> BTreeSet<(String, CommitId)> {
    controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), control.generation))
        .collect()
}

fn current_state_duplicate_delta_error(delta: &CurrentStateDeltaRef<'_>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "current-state commit contains duplicate mutation for schema '{}' entity_pk '{:?}' file_id '{:?}'",
            delta.schema_key, delta.entity_pk, delta.file_id
        ),
    )
}

fn collect_retired_untracked_json_refs(
    existing: HeadValueView<'_>,
    delta: &CurrentStateDeltaRef<'_>,
    retired: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) {
    debug_assert!(existing.untracked);
    if !delta.untracked {
        return;
    }
    for old_slot in [existing.snapshot, existing.metadata] {
        let HeadSlotView::Ref(old_ref) = old_slot else {
            continue;
        };
        let retained_by_successor = !delta.physically_deletes()
            && [delta.snapshot, delta.metadata].into_iter().any(
                |new_slot| matches!(new_slot, JsonSlotRef::Ref(new_ref) if new_ref == &old_ref),
            );
        if !retained_by_successor {
            retired.insert(*old_ref.as_hash_array());
        }
    }
}

fn reject_guarded_live_member(
    absence_guards: &BTreeSet<TrackedStateKey>,
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    if absence_guards.is_empty() || existing.deleted {
        return Ok(());
    }
    let key = TrackedStateKey {
        schema_key: delta.schema_key.to_string(),
        entity_pk: delta.entity_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    if absence_guards.contains(&key) {
        return Err(tracked_head_duplicate_insert_error(&key));
    }
    Ok(())
}

/// Checks a sorted zero-copy INSERT guard selection.
///
/// Normal transaction publication carries INSERT intent as prepared-row
/// ordinals. Lowering those ordinals to borrowed key views avoids allocating
/// one owned key and one tree node per mutation merely to test the matching
/// current-state delta.
fn reject_borrowed_guarded_live_member(
    absence_guards: &[TrackedStateKeyRef<'_>],
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    if absence_guards.is_empty() || existing.deleted {
        return Ok(());
    }
    let guarded = absence_guards
        .binary_search_by(|guard| {
            guard
                .schema_key
                .cmp(delta.schema_key)
                .then_with(|| guard.entity_pk.cmp(delta.entity_pk))
                .then_with(|| guard.file_id.cmp(&delta.file_id))
        })
        .is_ok();
    if guarded {
        return Err(tracked_head_duplicate_insert_error_ref(
            delta.schema_key,
            delta.entity_pk,
        ));
    }
    Ok(())
}

/// Retention is an identity property, not a mutable value column. An UPDATE
/// is planned against the current row and therefore preserves it; an INSERT
/// finding an existing identity is rejected by `absence_guards` above. This
/// additional fence makes an accidental tracked↔untracked promotion fail
/// closed even on an internal write path that did not originate in SQL.
fn reject_retention_change(
    delta: &CurrentStateDeltaRef<'_>,
    existing: HeadValueView<'_>,
) -> Result<(), LixError> {
    // A tracked tombstone is still the durable identity owner. Letting an
    // untracked member overwrite it would erase the tracked checkpoint
    // baseline and make a later diff silently miss the removal. Retention is
    // therefore immutable while any physical member exists; untracked delete
    // removes its member entirely, after which a new tracked insert is a new
    // identity and cannot affect historical diff state.
    if existing.untracked != delta.untracked {
        if existing.untracked {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "cannot insert tracked row in schema '{}' entity_pk {:?}: a canonical untracked row already exists; delete it first",
                    delta.schema_key, delta.entity_pk,
                ),
            ));
        }
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "cannot change retention for existing current-state row in schema '{}' entity_pk {:?}; delete it before inserting it as {}",
                delta.schema_key,
                delta.entity_pk,
                if delta.untracked {
                    "untracked"
                } else {
                    "tracked"
                },
            ),
        ));
    }
    Ok(())
}

fn tracked_head_duplicate_insert_error(key: &TrackedStateKey) -> LixError {
    tracked_head_duplicate_insert_error_ref(&key.schema_key, &key.entity_pk)
}

fn tracked_head_duplicate_insert_error_ref(schema_key: &str, entity_pk: &EntityPk) -> LixError {
    let entity_pk = entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate entity_pk '{entity_pk}'",
            schema_key
        ),
    )
}

#[cfg(test)]
fn stage_test_current_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    head_commit_id: CommitId,
    generation: CommitId,
    working_diff_checkpoint_commit_id: Option<CommitId>,
) -> Result<(), LixError> {
    let timestamp = LixTimestamp::expect_parse(
        "tracked-head test control timestamp",
        "2026-01-01T00:00:00Z",
    );
    stage_branch_head_control(
        writes,
        branch_id,
        BranchHeadControl {
            head_commit_id,
            generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            untracked_schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("tracked-head-test-control"),
        },
    )
}

/// Publishes the checkpoint epoch that owns the sparse working-diff indexes.
/// The surrounding branch-control CAS makes this marker, the current hot rows,
/// and the current branch head one atomic visibility boundary.
#[cfg(test)]
fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    hot::stage_test_hot_value(writes, identity, value)
}

const KEY_ESCAPE: u8 = 0xff;
const KEY_PART_FINAL: u8 = 0x00;
const KEY_PART_MORE: u8 = 0x01;
const FILE_ID_NONE: u8 = 0x00;
const FILE_ID_SOME: u8 = 0x01;
const GENERATION_BYTES: usize = 16;
const ENTITY_PK_CODEC_V1: u8 = 0x01;
const ENTITY_PK_UUID: u8 = 0x00;
const ENTITY_PK_INTEGER: u8 = 0x01;
const ENTITY_PK_STRING: u8 = 0x02;
const ENTITY_PK_BYTES: u8 = 0x03;

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
        !entity_pk.components.is_empty(),
        "tracked-head entity primary keys must be non-empty"
    );
    out.push(ENTITY_PK_CODEC_V1);
    for (index, component) in entity_pk.components.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.components.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        match component {
            crate::entity_pk::EntityPkComponent::Uuid(bytes) => {
                out.push(ENTITY_PK_UUID);
                out.extend_from_slice(bytes);
                out.push(terminator);
            }
            crate::entity_pk::EntityPkComponent::Integer(value) => {
                out.push(ENTITY_PK_INTEGER);
                let ordered = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
                out.extend_from_slice(&ordered.to_be_bytes());
                out.push(terminator);
            }
            crate::entity_pk::EntityPkComponent::String(value) => {
                out.push(ENTITY_PK_STRING);
                write_key_bytes(out, value.as_bytes(), terminator);
            }
            crate::entity_pk::EntityPkComponent::Bytes(value) => {
                out.push(ENTITY_PK_BYTES);
                write_key_bytes(out, value, terminator);
            }
        }
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
    write_key_bytes(out, value.as_bytes(), terminator);
}

fn write_key_bytes(out: &mut Vec<u8>, value: &[u8], terminator: u8) {
    for &byte in value {
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
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key version"))?;
    *offset += 1;
    if version != ENTITY_PK_CODEC_V1 {
        return Err(key_codec_error(&format!(
            "has unsupported entity primary key codec version {version}"
        )));
    }
    let mut components = SmallVec::new();
    loop {
        let (part, terminator) = read_entity_pk_part(bytes, offset)?;
        components.push(part);
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
    EntityPk::from_components(components).map_err(|error| {
        key_codec_error(&format!("contains an invalid entity primary key: {error}"))
    })
}

fn read_entity_pk_part(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<(crate::entity_pk::EntityPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key part tag"))?;
    *offset += 1;
    match tag {
        ENTITY_PK_STRING => {
            let (value, terminator) = read_key_string(bytes, offset, "entity primary key")?;
            Ok((
                crate::entity_pk::EntityPkComponent::String(value.into()),
                terminator,
            ))
        }
        ENTITY_PK_BYTES => {
            let (value, terminator) = read_key_bytes(bytes, offset, "entity primary key bytes")?;
            Ok((
                crate::entity_pk::EntityPkComponent::Bytes(value.into()),
                terminator,
            ))
        }
        ENTITY_PK_UUID => {
            let uuid_end = offset
                .checked_add(16)
                .ok_or_else(|| key_codec_error("UUIDv7 entity primary key offset overflow"))?;
            let uuid_bytes: [u8; 16] = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| key_codec_error("is truncated in UUIDv7 entity primary key"))?
                .try_into()
                .expect("UUIDv7 slice has fixed length");
            let terminator = bytes
                .get(uuid_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after UUIDv7 entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "UUIDv7 entity primary key has an invalid terminator",
                ));
            }
            *offset = uuid_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Uuid(uuid_bytes),
                terminator,
            ))
        }
        ENTITY_PK_INTEGER => {
            let integer_end = offset
                .checked_add(8)
                .ok_or_else(|| key_codec_error("integer entity primary key offset overflow"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| key_codec_error("is truncated in integer entity primary key"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = bytes
                .get(integer_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after integer entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "integer entity primary key has an invalid terminator",
                ));
            }
            *offset = integer_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Integer(i64::from_be_bytes(
                    (ordered ^ (1_u64 << 63)).to_be_bytes(),
                )),
                terminator,
            ))
        }
        _ => Err(key_codec_error(
            "has an unknown entity primary key part tag",
        )),
    }
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
    let (value, terminator) = read_key_bytes(bytes, offset, field)?;
    let value = String::from_utf8(value).map_err(|error| {
        key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
    })?;
    Ok((value, terminator))
}

fn read_key_bytes(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Vec<u8>, u8), LixError> {
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
            *offset = cursor;
            return Ok((bytes[start..cursor - 2].to_vec(), terminator));
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
        *offset = cursor;
        return Ok((out, terminator));
    }
}

fn key_codec_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid tracked-head row key: {message}"),
    )
}

/// Current-state values are intentionally a small, fixed-header wire record rather
/// than a general Musli struct. The normal read path needs only these fields,
/// and decoding a Musli `JsonSlot` first allocated an intermediate value for
/// every row before it was copied into a live-state row.
///
/// ```text
///  0      format version (8)
///  1      deleted + untracked + snapshot/metadata kinds
///  2..18  change UUID
/// 18..34  commit UUID
/// 34..42  created_at packed timestamp (big endian)
/// 42..50  updated_at packed timestamp (big endian)
/// 50..54  snapshot payload byte length (big endian u32)
/// 54..58  metadata payload byte length (big endian u32)
/// 58      columnar base-coordinate presence (0 or 1)
/// 59..    snapshot payload, metadata payload, then an optional 40-byte state coordinate
/// ```
///
/// Slot payloads are either inline UTF-8 JSON or a fixed 32-byte `JsonRef`.
const HEAD_VALUE_VERSION: u8 = 10;
const HEAD_VALUE_HEADER_BYTES: usize = 59;
const COLUMNAR_BASE_COORDINATE_BYTES: usize = 32 + 4 + 4;
const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_UNTRACKED: u8 = 0b0010_0000;
const HEAD_VALUE_SLOT_MASK: u8 = 0b11;
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
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotView<'a>,
    metadata: HeadSlotView<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

impl CertifiedCurrentStatePredecessor {
    pub(crate) fn created_at(&self) -> Result<LixTimestamp, LixError> {
        Ok(self.view()?.created_at)
    }

    fn view(&self) -> Result<HeadValueView<'_>, LixError> {
        match self {
            Self::Encoded(bytes) => decode_head_value(bytes),
            Self::ArrowRoot(value) => Ok(HeadValueView {
                change_id: Some(value.change_id),
                commit_id: Some(value.commit_id),
                untracked: false,
                deleted: value.deleted,
                created_at: value.created_at,
                updated_at: value.updated_at,
                snapshot: HeadSlotView::None,
                metadata: HeadSlotView::None,
                columnar_base_coordinate: value.columnar_base_coordinate,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum HeadSlotEncode<'a> {
    None,
    Ref(JsonRef),
    Inline(&'a str),
}

impl<'a> From<JsonSlotRef<'a>> for HeadSlotEncode<'a> {
    fn from(value: JsonSlotRef<'a>) -> Self {
        match value {
            JsonSlotRef::None => Self::None,
            JsonSlotRef::Ref(value) => Self::Ref(*value),
            JsonSlotRef::Inline(json) => Self::Inline(json),
        }
    }
}

impl<'a> From<HeadSlotView<'a>> for HeadSlotEncode<'a> {
    fn from(value: HeadSlotView<'a>) -> Self {
        match value {
            HeadSlotView::None => Self::None,
            HeadSlotView::Ref(value) => Self::Ref(value),
            HeadSlotView::Inline(json) => Self::Inline(json),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadValueEncode<'a> {
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: HeadSlotEncode<'a>,
    metadata: HeadSlotEncode<'a>,
    columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

fn encode_head_value(value: &HeadValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    append_head_value(&mut bytes, value)?;
    Ok(bytes)
}

fn append_head_value(
    bytes: &mut Vec<u8>,
    value: &HeadValueRef<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    append_head_value_parts(
        bytes,
        HeadValueEncode {
            change_id: value.change_id,
            commit_id: value.commit_id,
            untracked: value.untracked,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            snapshot: value.snapshot.into(),
            metadata: value.metadata.into(),
            columnar_base_coordinate: value.columnar_base_coordinate,
        },
    )
}

fn append_head_value_parts(
    bytes: &mut Vec<u8>,
    value: HeadValueEncode<'_>,
) -> Result<std::ops::Range<usize>, LixError> {
    let snapshot_kind = encoded_slot_kind(value.snapshot);
    let metadata_kind = encoded_slot_kind(value.metadata);
    if value.deleted && (snapshot_kind != HEAD_SLOT_NONE || metadata_kind != HEAD_SLOT_NONE) {
        return Err(head_value_error(
            "deleted current-state rows must not carry JSON payloads",
        ));
    }
    match (
        value.untracked,
        value.change_id,
        value.commit_id,
        value.deleted,
    ) {
        (false, Some(_), Some(_), _) | (true, None, None, false) => {}
        (true, _, _, true) => {
            return Err(head_value_error(
                "untracked current-state rows must be deleted physically",
            ));
        }
        (false, _, _, _) => {
            return Err(head_value_error(
                "tracked current-state rows must carry change_id and commit_id",
            ));
        }
        (true, _, _, false) => {
            return Err(head_value_error(
                "untracked current-state rows must not carry change_id or commit_id",
            ));
        }
    }
    if value.untracked && value.columnar_base_coordinate.is_some() {
        return Err(head_value_error(
            "untracked current-state rows must not carry an columnar base coordinate",
        ));
    }
    if value
        .columnar_base_coordinate
        .is_some_and(|coordinate| coordinate.state_set_id.as_bytes() == [0; 32])
    {
        return Err(head_value_error(
            "columnar base coordinate must carry a nonzero state digest",
        ));
    }
    let snapshot_len = encoded_slot_len(value.snapshot);
    let metadata_len = encoded_slot_len(value.metadata);
    let capacity = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .and_then(|bytes| bytes.checked_add(metadata_len))
        .and_then(|bytes| {
            bytes.checked_add(
                value
                    .columnar_base_coordinate
                    .map_or(0, |_| COLUMNAR_BASE_COORDINATE_BYTES),
            )
        })
        .ok_or_else(|| head_value_error("encoded row length overflow"))?;
    let start = bytes.len();
    bytes.reserve(capacity);
    bytes.push(HEAD_VALUE_VERSION);
    let mut flags = if value.deleted { HEAD_VALUE_DELETED } else { 0 };
    if value.untracked {
        flags |= HEAD_VALUE_UNTRACKED;
    }
    flags |= snapshot_kind << HEAD_VALUE_SNAPSHOT_SHIFT;
    flags |= metadata_kind << HEAD_VALUE_METADATA_SHIFT;
    bytes.push(flags);
    bytes.extend_from_slice(value.change_id.unwrap_or_default().as_uuid().as_bytes());
    bytes.extend_from_slice(value.commit_id.unwrap_or_default().as_uuid().as_bytes());
    bytes.extend_from_slice(&value.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(snapshot_len)
            .map_err(|_| head_value_error("snapshot payload exceeds v8 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(metadata_len)
            .map_err(|_| head_value_error("metadata payload exceeds v8 u32 limit"))?
            .to_be_bytes(),
    );
    bytes.push(u8::from(value.columnar_base_coordinate.is_some()));
    append_slot_payload(bytes, value.snapshot);
    append_slot_payload(bytes, value.metadata);
    if let Some(coordinate) = value.columnar_base_coordinate {
        bytes.extend_from_slice(&coordinate.state_set_id.as_bytes());
        bytes.extend_from_slice(&coordinate.group_index.to_be_bytes());
        bytes.extend_from_slice(&coordinate.row_index.to_be_bytes());
    }
    debug_assert_eq!(bytes.len() - start, capacity);
    Ok(start..bytes.len())
}

fn encoded_slot_kind(slot: HeadSlotEncode<'_>) -> u8 {
    match slot {
        HeadSlotEncode::None => HEAD_SLOT_NONE,
        HeadSlotEncode::Ref(_) => HEAD_SLOT_REF,
        HeadSlotEncode::Inline(_) => HEAD_SLOT_INLINE,
    }
}

fn encoded_slot_len(slot: HeadSlotEncode<'_>) -> usize {
    match slot {
        HeadSlotEncode::None => 0,
        HeadSlotEncode::Ref(_) => JSON_REF_BYTES,
        HeadSlotEncode::Inline(json) => json.len(),
    }
}

fn append_slot_payload(bytes: &mut Vec<u8>, slot: HeadSlotEncode<'_>) {
    match slot {
        HeadSlotEncode::None => {}
        HeadSlotEncode::Ref(json_ref) => bytes.extend_from_slice(json_ref.as_hash_bytes()),
        HeadSlotEncode::Inline(json) => bytes.extend_from_slice(json.as_bytes()),
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
        return Err(head_value_error("row is shorter than the v10 fixed header"));
    }
    if bytes[0] != HEAD_VALUE_VERSION {
        return Err(head_value_error(&format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }
    let flags = bytes[1];
    if flags & 0b1100_0000 != 0 {
        return Err(head_value_error("row uses reserved v10 flag bits"));
    }
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let change_uuid = uuid_from_head_bytes(&bytes[2..18], "change id")?;
    let commit_uuid = uuid_from_head_bytes(&bytes[18..34], "commit id")?;
    let created_at = LixTimestamp::from_packed(read_u64(&bytes[34..42], "created_at")?)
        .map_err(|error| head_value_error(&format!("invalid created_at: {error}")))?;
    let updated_at = LixTimestamp::from_packed(read_u64(&bytes[42..50], "updated_at")?)
        .map_err(|error| head_value_error(&format!("invalid updated_at: {error}")))?;
    let snapshot_len = usize::try_from(read_u32(&bytes[50..54], "snapshot length")?)
        .map_err(|_| head_value_error("snapshot length exceeds usize"))?;
    let metadata_len = usize::try_from(read_u32(&bytes[54..58], "metadata length")?)
        .map_err(|_| head_value_error("metadata length exceeds usize"))?;
    let has_columnar_base_coordinate = match bytes[58] {
        0 => false,
        1 => true,
        _ => return Err(head_value_error("invalid columnar base-coordinate tag")),
    };
    let snapshot_end = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .ok_or_else(|| head_value_error("snapshot payload length overflow"))?;
    let metadata_end = snapshot_end
        .checked_add(metadata_len)
        .ok_or_else(|| head_value_error("metadata payload length overflow"))?;
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
    let mut payload_offset = metadata_end;
    let columnar_base_coordinate = if has_columnar_base_coordinate {
        let state_set_id = crate::columnar_row_group::ArrowStateSetId::from_digest(
            take_head_bytes(bytes, &mut payload_offset, 32, "columnar state-set digest")?
                .try_into()
                .expect("fixed Arrow state-set digest"),
        );
        if state_set_id.as_bytes() == [0; 32] {
            return Err(head_value_error(
                "columnar base coordinate has a zero state digest",
            ));
        }
        let group_index = read_u32(
            take_head_bytes(bytes, &mut payload_offset, 4, "columnar base group index")?,
            "columnar base group index",
        )?;
        let row_index = read_u32(
            take_head_bytes(bytes, &mut payload_offset, 4, "columnar base row index")?,
            "columnar base row index",
        )?;
        Some(ColumnarBaseCoordinate {
            state_set_id,
            group_index,
            row_index,
        })
    } else {
        None
    };
    if payload_offset != bytes.len() {
        return Err(head_value_error(
            "row payload lengths do not match the buffer",
        ));
    }
    let deleted = flags & HEAD_VALUE_DELETED != 0;
    let untracked = flags & HEAD_VALUE_UNTRACKED != 0;
    if deleted && (snapshot != HeadSlotView::None || metadata != HeadSlotView::None) {
        return Err(head_value_error(
            "deleted current-state rows must not carry JSON payloads",
        ));
    }
    let (change_id, commit_id) = if untracked {
        if deleted {
            return Err(head_value_error(
                "untracked current-state rows must be deleted physically",
            ));
        }
        if change_uuid != uuid::Uuid::nil() || commit_uuid != uuid::Uuid::nil() {
            return Err(head_value_error(
                "untracked current-state rows must use nil change and commit ids",
            ));
        }
        if columnar_base_coordinate.is_some() {
            return Err(head_value_error(
                "untracked current-state rows must not carry an columnar base coordinate",
            ));
        }
        (None, None)
    } else {
        if change_uuid == uuid::Uuid::nil() || commit_uuid == uuid::Uuid::nil() {
            return Err(head_value_error(
                "tracked current-state rows must use non-nil change and commit ids",
            ));
        }
        (
            Some(ChangeId::new(change_uuid)),
            Some(CommitId::new(commit_uuid)),
        )
    };
    Ok(HeadValueView {
        change_id,
        commit_id,
        untracked,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
        columnar_base_coordinate,
    })
}

fn take_head_bytes<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
    field: &str,
) -> Result<&'a [u8], LixError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| head_value_error(format!("{field} offset overflow")))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| head_value_error(format!("{field} is truncated")))?;
    *offset = end;
    Ok(value)
}

fn uuid_from_head_bytes(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes.try_into().map_err(|_| {
        head_value_error(&format!(
            "{field} must have {UUID_BYTES} bytes in the v8 header"
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

fn head_value_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid hot live-state row: {message}"),
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

trait LiveMaterializationIdentity {
    #[allow(clippy::too_many_arguments)]
    fn push_materialized(
        self,
        rows: &mut MaterializedLiveStateBatchBuilder,
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
    );
}

impl LiveMaterializationIdentity for HeadRowIdentity {
    fn push_materialized(
        self,
        rows: &mut MaterializedLiveStateBatchBuilder,
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
    ) {
        rows.push_materialized(
            self.entity_pk,
            self.schema_key,
            self.file_id,
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
        );
    }
}

impl LiveMaterializationIdentity for TrackedStateKeyRef<'_> {
    fn push_materialized(
        self,
        rows: &mut MaterializedLiveStateBatchBuilder,
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
    ) {
        rows.push_materialized_ref(
            self.entity_pk,
            self.schema_key,
            self.file_id,
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
        );
    }
}

/// Builds serving rows directly from a V5 hot-row value. Inline JSON remains a
/// range over the immutable head-value buffer, while out-of-band JSON retains
/// the `JsonStore` buffer. There is no per-row payload `String` or intermediate
/// `HeadValue`/`MaterializedTrackedStateRow` staging layer.
async fn materialize_live_entries<I>(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: Vec<(I, Bytes)>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    I: LiveMaterializationIdentity,
{
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    let mut json_refs = Vec::new();
    let mut deferred = Vec::new();
    let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(entries.len());
    for (identity, bytes) in entries {
        let value = decode_head_value(&bytes)?;
        let row_index = rows.len();
        let snapshot_content = materialize_live_slot(
            !value.deleted && projection.snapshot_content,
            &bytes,
            value.snapshot,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Snapshot,
        );
        let metadata = materialize_live_slot(
            !value.deleted && projection.metadata,
            &bytes,
            value.metadata,
            &mut json_refs,
            &mut deferred,
            row_index,
            DeferredJsonField::Metadata,
        );
        identity.push_materialized(
            &mut rows,
            snapshot_content,
            metadata,
            value.deleted,
            value.created_at,
            value.updated_at,
            global,
            value.change_id,
            value.commit_id,
            value.untracked,
            branch_id,
        );
        if let Some(coordinate) = value.columnar_base_coordinate {
            rows.set_columnar_base_coordinate(row_index, coordinate);
        }
        rows.set_durable_predecessor(row_index, CertifiedCurrentStatePredecessor::Encoded(bytes));
    }
    if json_refs.is_empty() {
        return Ok(rows.finish());
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
        let json = SharedStr::from_utf8(bytes).map_err(|error| {
            head_value_error(&format!("out-of-band JSON payload is not UTF-8: {error}"))
        })?;
        match deferred.field {
            DeferredJsonField::Snapshot => {
                rows.set_snapshot_content(deferred.row_index, json);
            }
            DeferredJsonField::Metadata => rows.set_metadata(deferred.row_index, json),
        }
    }
    Ok(rows.finish())
}

fn materialize_live_slot(
    include: bool,
    owner: &Bytes,
    slot: HeadSlotView<'_>,
    json_refs: &mut Vec<JsonRef>,
    deferred: &mut Vec<DeferredJson>,
    row_index: usize,
    field: DeferredJsonField,
) -> Option<SharedStr> {
    if !include {
        return None;
    }
    match slot {
        HeadSlotView::None => None,
        HeadSlotView::Inline(json) => Some(
            SharedStr::from_utf8_slice(owner.clone(), json)
                .expect("decoded inline JSON points into its head-value buffer"),
        ),
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
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
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
            change_id: Some(ChangeId::for_test_label(change)),
            commit_id: Some(commit_id),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            snapshot: JsonSlot::from_json("{\"value\":true}"),
            metadata: JsonSlot::None,
            columnar_base_coordinate: None,
        }
    }

    #[tokio::test]
    async fn entity_snapshot_scan_restores_logical_primary_key_order() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let control = BranchHeadControl {
            head_commit_id: head,
            generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            untracked_schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
        };
        let mut writes = StorageWriteSet::new();
        for (entity, file_id) in [("a", "z-file"), ("b", "a-file")] {
            let identity = HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single(entity),
                file_id: Some(file_id.to_string()),
            };
            stage_put(
                &mut writes,
                &identity,
                &HeadValue {
                    change_id: Some(ChangeId::for_test_label(entity)),
                    commit_id: Some(head),
                    untracked: false,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: JsonSlot::from_json(&format!(r#"{{"entity":"{entity}"}}"#)),
                    metadata: JsonSlot::None,
                    columnar_base_coordinate: None,
                },
            )
            .expect("stage file-backed row");
        }
        stage_branch_head_control(&mut writes, branch_id, control)
            .expect("stage matching branch control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit file-backed rows");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open entity snapshot read");
        let snapshots = TrackedHeadContext::new()
            .reader(read)
            .scan_entity_snapshots(branch_id, control, "schema", &[], None)
            .await
            .expect("scan snapshots");
        let snapshots = snapshots
            .into_iter()
            .map(|snapshot| {
                String::from_utf8(snapshot.expect("row has a snapshot").to_vec())
                    .expect("snapshot is UTF-8")
            })
            .collect::<Vec<_>>();
        assert_eq!(snapshots, [r#"{"entity":"a"}"#, r#"{"entity":"b"}"#]);
    }

    #[tokio::test]
    async fn file_id_reads_use_file_first_primary_values() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let head = CommitId::for_test_label("head");
        let control = BranchHeadControl {
            head_commit_id: head,
            generation: head,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            untracked_schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("branch-ref"),
        };
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
                file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("01920000-0000-7000-8000-0000000000a2"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"a\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label("01920000-0000-7000-8000-0000000000b2"),
                commit_id: head,
                deleted: false,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: JsonSlotRef::Inline("{\"value\":\"b\"}"),
                metadata: JsonSlotRef::None,
            },
            TrackedHeadDeltaRef {
                schema_key: "schema",
                file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                entity_pk: &second_entity_pk,
                change_id: ChangeId::for_test_label("second-01920000-0000-7000-8000-0000000000b2"),
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
            .expect("stage hot head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit hot head");

        let projection_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file schema marker verification read");
        let projection_rows = ScanPlan::prefix(
            HOT_FILE_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&projection_read, StorageScanOptions::default())
        .await
        .expect("file schema markers should scan")
        .value
        .entries;
        assert_eq!(
            projection_rows.len(),
            1,
            "file rows share one conservative schema marker"
        );
        drop(projection_read);

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
            vec![
                None,
                Some("01920000-0000-7000-8000-0000000000a2"),
                Some("01920000-0000-7000-8000-0000000000b2")
            ]
        );

        // A null-file predicate selects only the null-file hot row.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open null file-id read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![entity_pk.clone()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("null file-id read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_id, None);

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
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("filtered file scan should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );

        // A schema-scoped `file_id = ?` query reads the hydrated file-first
        // primary range directly. This is the access pattern used by
        // filesystem-backed entity scans, where the entity PK is not known
        // before the query.
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
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
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
                .all(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
        );

        // The branch control validates the published hot generation. Exact
        // file identity and schema-scoped file-id reads route through the V18
        // file-first primary index.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open control-gated file-id scan");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows(
                branch_id,
                control,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        file_ids: vec![NullableKeyFilter::Value(
                            "01920000-0000-7000-8000-0000000000b2".to_string(),
                        )],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("control-bound file-id scan should execute");
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
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
                    file_id: Some("01920000-0000-7000-8000-0000000000b2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("marker should match");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_ref().expect("explicit file row should resolve");
        assert_eq!(
            row.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open control-gated exact file read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows(
                branch_id,
                control,
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    entity_pk: EntityPk::single("row"),
                    file_id: Some("01920000-0000-7000-8000-0000000000b2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("control-bound exact file read should execute");
        assert_eq!(rows.len(), 1);
        let row = rows[0]
            .as_ref()
            .expect("explicit file row should resolve through its projection");
        assert_eq!(
            row.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000b2")
        );
        assert_eq!(row.snapshot_content.as_deref(), Some("{\"value\":\"b\"}"));
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
                file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
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
        stage_test_current_control(&mut writes, "branch", head, generation, None)
            .expect("stage current control");
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
    async fn branch_control_gates_generations_and_rows_roundtrip() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("generation");
        let head = CommitId::for_test_label("head");
        let identity = identity("branch", generation, "row");
        let value = HeadValue {
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(head),
            untracked: false,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:01Z"),
            snapshot: JsonSlot::from_json("{\"id\":\"row\"}"),
            metadata: JsonSlot::None,
            columnar_base_coordinate: None,
        };
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &value).expect("stage row");
        stage_test_current_control(&mut writes, "branch", head, generation, None)
            .expect("stage current control");
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
    async fn incremental_row_update_preserves_siblings_and_file_schema_marker() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");

        let mut writes = StorageWriteSet::new();
        for (file_id, change_id) in [
            (None, "none-first"),
            (
                Some("01920000-0000-7000-8000-0000000000a2"),
                "01920000-0000-7000-8000-0000000000a2-first",
            ),
            (
                Some("01920000-0000-7000-8000-0000000000b2"),
                "01920000-0000-7000-8000-0000000000b2-first",
            ),
        ] {
            stage_put(
                &mut writes,
                &HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: "schema".to_string(),
                    entity_pk: entity_pk.clone(),
                    file_id: file_id.map(str::to_string),
                },
                &head_value(change_id, generation),
            )
            .expect("stage initial hot row");
        }
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage initial current control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit initial hot rows");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open incremental read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label(
                        "01920000-0000-7000-8000-0000000000a2-second",
                    ),
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
            .expect("stage direct row update");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct row update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open logical verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan hot rows")
            .expect("matching marker");
        assert_eq!(rows.len(), 3);
        let none = rows
            .iter()
            .find(|row| row.file_id.is_none())
            .expect("null-file row remains");
        let file_a = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000a2"))
            .expect("changed file row remains");
        let file_b = rows
            .iter()
            .find(|row| row.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000b2"))
            .expect("untouched file row remains");
        assert_eq!(none.change_id, Some(ChangeId::for_test_label("none-first")));
        assert_eq!(
            file_a.change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000a2-second"
            ))
        );
        assert_eq!(
            file_b.change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000b2-first"
            ))
        );
        assert_eq!(file_a.created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(file_a.updated_at, ts("2026-01-02T00:00:00Z"));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open file-schema marker verification read");
        let projections = ScanPlan::prefix(
            HOT_FILE_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("scan file-schema markers")
        .value
        .entries;
        assert_eq!(projections.len(), 1);
        assert!(
            projections.into_iter().all(|projection| {
                full_value_bytes(projection.value).is_ok_and(|bytes| bytes.is_empty())
            }),
            "schema membership markers remain key-only"
        );
    }

    #[tokio::test]
    async fn incremental_row_update_does_not_decode_unrelated_hot_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let unrelated_pk = EntityPk::single("unrelated");

        let mut initial_writes = StorageWriteSet::new();
        stage_put(
            &mut initial_writes,
            &HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                entity_pk: entity_pk.clone(),
                file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            },
            &head_value("01920000-0000-7000-8000-0000000000a2-first", generation),
        )
        .expect("stage target hot row");
        stage_put(
            &mut initial_writes,
            &HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: "schema".to_string(),
                entity_pk: unrelated_pk,
                file_id: None,
            },
            &head_value("unrelated", generation),
        )
        .expect("stage unrelated hot row");
        stage_test_current_control(&mut initial_writes, branch_id, generation, generation, None)
            .expect("stage initial control");
        storage
            .commit_write_set(initial_writes, StorageWriteOptions::default())
            .await
            .expect("commit initial hot rows");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open corruption fixture read");
        let unrelated_key = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("scan hot rows for fixture")
        .value
        .entries
        .into_iter()
        .find_map(|entry| {
            let value = full_value_bytes(entry.value).ok()?;
            let value = decode_head_value(&value).ok()?;
            (value.change_id == Some(ChangeId::for_test_label("unrelated"))).then_some(entry.key)
        })
        .expect("find unrelated row key");
        drop(read);
        let mut corrupt_writes = StorageWriteSet::new();
        corrupt_writes.put(
            HOT_ROW_SPACE,
            unrelated_key,
            StorageValue {
                bytes: Bytes::from_static(b"corrupt unrelated hot row"),
            },
        );
        storage
            .commit_write_set(corrupt_writes, StorageWriteOptions::default())
            .await
            .expect("commit corrupt unrelated row");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open targeted incremental read");
        let mut writes = StorageWriteSet::new();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label(
                        "01920000-0000-7000-8000-0000000000a2-second",
                    ),
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
            .expect("unrelated corrupt row must not block a direct update");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct hot-row update");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact file verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &[TrackedStateKey {
                    schema_key: "schema".to_string(),
                    entity_pk,
                    file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("exact file read should execute")
            .expect("matching current control");
        assert_eq!(
            rows[0]
                .as_ref()
                .expect("target file row survives")
                .change_id,
            Some(ChangeId::for_test_label(
                "01920000-0000-7000-8000-0000000000a2-second"
            ))
        );
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
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage existing current control");
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
    async fn incremental_guarded_insert_resurrects_tombstone_with_first_created_at() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch";
        let generation = CommitId::for_test_label("first-head");
        let second_head = CommitId::for_test_label("second-head");
        let entity_pk = EntityPk::single("row");
        let identity = identity(branch_id, generation, "row");

        let mut tombstone = head_value("first-delete", generation);
        tombstone.deleted = true;
        tombstone.updated_at = ts("2026-01-02T00:00:00Z");
        tombstone.snapshot = JsonSlot::None;
        tombstone.metadata = JsonSlot::None;
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &identity, &tombstone).expect("stage existing tombstone");
        stage_test_current_control(&mut writes, branch_id, generation, generation, None)
            .expect("stage existing current control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit existing tombstone");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open guarded resurrection read");
        let mut writes = StorageWriteSet::new();
        let absence_guards = BTreeSet::from([TrackedStateKey {
            schema_key: "schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: None,
        }]);
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                Some(generation),
                second_head,
                &[TrackedHeadDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: ChangeId::for_test_label("second-insert"),
                    commit_id: second_head,
                    deleted: false,
                    created_at: ts("2026-01-03T00:00:00Z"),
                    updated_at: ts("2026-01-03T00:00:00Z"),
                    snapshot: JsonSlotRef::Inline("{\"value\":2}"),
                    metadata: JsonSlotRef::None,
                }],
                &absence_guards,
                None,
            )
            .await
            .expect("guarded INSERT may resurrect a tombstone");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit guarded resurrection");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open resurrection verification read");
        let rows = TrackedHeadContext::new()
            .reader(read)
            .scan_live_rows_if_current(
                branch_id,
                &second_head.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("scan resurrected row")
            .expect("matching marker");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(rows[0].updated_at, ts("2026-01-03T00:00:00Z"));
        assert_eq!(
            rows[0].change_id,
            Some(ChangeId::for_test_label("second-insert"))
        );
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("{\"value\":2}"));
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
            snapshot_content: Some("{\"value\":1}".into()),
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

    #[tokio::test]
    async fn current_state_gc_keeps_only_control_bound_untracked_generations() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "main";
        let active_generation = CommitId::for_test_label("active-current-generation");
        let stale_generation = CommitId::for_test_label("stale-current-generation");
        let active_snapshot = JsonRef::from_hash_bytes([7; JSON_REF_BYTES]);
        let stale_snapshot = JsonRef::from_hash_bytes([9; JSON_REF_BYTES]);
        let timestamp = ts("2026-01-01T00:00:00Z");
        let active_identity = HeadIdentity {
            branch_id: branch_id.to_string(),
            generation: active_generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("active-row"),
            file_id: Some("active-file".to_string()),
        };
        let stale_identity = HeadIdentity {
            branch_id: branch_id.to_string(),
            generation: stale_generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single("stale-row"),
            file_id: Some("stale-file".to_string()),
        };
        let active_control = BranchHeadControl {
            head_commit_id: active_generation,
            generation: active_generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            untracked_schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("active-current-ref"),
        };
        let controls = vec![(branch_id.to_string(), active_control)];

        let untracked = |snapshot| HeadValue {
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlot::Ref(snapshot),
            metadata: JsonSlot::None,
            columnar_base_coordinate: None,
        };
        let mut writes = StorageWriteSet::new();
        stage_put(&mut writes, &active_identity, &untracked(active_snapshot))
            .expect("stage active untracked hot row");
        stage_put(&mut writes, &stale_identity, &untracked(stale_snapshot))
            .expect("stage stale untracked hot row");
        stage_branch_head_control(&mut writes, branch_id, active_control)
            .expect("stage active branch control");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit current-state GC fixture");

        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open current-state GC read"),
        );
        let rooted = TrackedHeadContext::new()
            .reader(read.clone())
            .untracked_json_refs(&controls)
            .await
            .expect("discover active untracked payload roots");
        assert_eq!(rooted, vec![active_snapshot]);

        let mut gc_writes = StorageWriteSet::new();
        let stale_refs = TrackedHeadContext::new()
            .stage_collect_stale_current_state_generations(&read, &mut gc_writes, &controls)
            .await
            .expect("stage stale current-state cleanup");
        assert_eq!(stale_refs, vec![stale_snapshot]);
        drop(read);
        storage
            .commit_write_set(gc_writes, StorageWriteOptions::default())
            .await
            .expect("commit stale current-state cleanup");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open current-state GC verification read");
        for (label, space) in [
            ("primary hot row", HOT_ROW_SPACE),
            ("file-schema hot markers", HOT_FILE_SPACE),
        ] {
            let entries = ScanPlan::prefix(
                space,
                StoragePrefix {
                    bytes: Bytes::new(),
                },
            )
            .collect(&read, StorageScanOptions::default())
            .await
            .expect("scan current-state hot storage")
            .value
            .entries;
            assert_eq!(entries.len(), 1, "only active {label} survives GC");
            let encoded =
                full_value_bytes(entries.into_iter().next().expect("one hot value").value)
                    .expect("hot value is present");
            if space == HOT_FILE_SPACE {
                assert!(encoded.is_empty(), "file-schema markers remain key-only");
                continue;
            }
            let value = decode_head_value(&encoded).expect("active hot value decodes");
            assert!(value.untracked, "active hot value remains untracked");
            match value.snapshot {
                HeadSlotView::Ref(snapshot) => assert_eq!(snapshot, active_snapshot),
                _ => panic!("active hot value must retain its JSON reference"),
            }
        }
    }
}
