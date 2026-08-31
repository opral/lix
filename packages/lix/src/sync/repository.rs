//! Repository-wide sync publication and read model.
//!
//! The mutable protocol state is deliberately tiny: one monotonically
//! increasing repository sequence and one replica receipt. Branch heads stay
//! in Lix's ordinary branch controls and commits stay in the changelog.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchHeadControl, BranchHeadControlContext,
    branch_head_control_precondition, stage_branch_head_control, stage_delete_branch_head_control,
};
use crate::changelog::{
    COMMIT_RECORD_FORMAT_VERSION, COMMIT_SPACE, ChangeId, ChangeLoadRequest, ChangeRecord,
    ChangeRecordProjection, ChangelogAppend, ChangelogContext, ChangelogReader, ChangelogWriter,
    CommitId, CommitLoadRequest, CommitRecord, CommitTouchedScopeDigest, commit_key,
    materialize_known_change_payloads_in_order, next_first_parent_jump,
};
use crate::commit_graph::CommitGraphContext;
use crate::common::LixTimestamp;
use crate::hot_state::{
    CompleteWorkingDiffMode, CurrentStateDeltaRef, HotTrackedSnapshot, TrackedHeadContext,
    TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, stage_tracked_working_diff_epoch,
};
use crate::row_pk::RowPk;
use crate::storage_adapter::{
    Storage, StorageAdapter, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StorageGetManyRequest, StorageGetOptions, StorageKey, StoragePrecondition, StoragePrefix,
    StorageProjectedValue, StorageReadOptions, StorageSpace, StorageSpaceId, StorageWriteOptions,
    StorageWriteSet, ValueSemantics, exact_get_many,
};
use crate::tracked_state::{
    CertifiedCommitStateTopologyParent, CommitDeltaChangeLocator, CommitStateManifest,
    CommitStateMutationInventory, CommitStateReplayDebt, MaterializedTrackedStateRow,
    StagedCommitStateManifest, TRACKED_STATE_CHANGE_LOCATOR_SPACE, TrackedStateChunkOverlay,
    TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef, TrackedStateDiffRequest,
    TrackedStateFilter, TrackedStateKey, TrackedStateKeyRef, TrackedStateReadColumns,
    TrackedStateRootId, TrackedStateScanRequest, commit_delta_member_scopes,
    commit_history_is_deferred, deferred_commit_global_scope, direct_change_locator,
    encode_key_ref, incomplete_touched_scope_filter, load_change_record_by_id,
    load_commit_state_manifest, load_published_commit_state_topology,
    stage_certified_commit_state_manifest_with_handle, stage_change_locators,
    stage_commit_history_available, stage_commit_history_deferred_with_scope,
    stage_commit_state_manifest_with_handle,
    stage_current_state_scoped_ranges_from_complete_state_source,
    stage_current_state_scoped_ranges_from_topology, stage_imported_addressable_commit_deltas,
    stage_row_pk_index_from_deltas, stage_row_pk_index_from_members, staged_commit_delta_members,
    staged_commit_delta_segment_bytes,
};
use crate::{Lix, LixError};

use super::commit::{
    SyncCommit, SyncCommitMember, export_sync_commit, load_sync_commit,
    load_sync_commit_state_alias, stage_materialized_sync_state_alias,
};
use super::protocol::{
    SyncBlobManifest, SyncBranchHead, SyncCommitHeader, SyncEvent, SyncHistoryBoundary,
    SyncHistoryResponse, SyncPushRequest, SyncPushResponse, SyncRefUpdate,
    SyncRepositoryPullResponse, SyncSnapshotRow, SyncSnapshotRowPage,
};

fn immutable_object_mismatch(kind: &str, id: impl ToString) -> LixError {
    let id = id.to_string();
    LixError::new(
        super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE,
        format!("sync {kind} '{id}' already exists with different content"),
    )
    .with_details(serde_json::json!({
        "objectKind": kind,
        "objectId": id,
    }))
}

pub(super) fn sync_commit_blob_ids(commits: &[SyncCommit]) -> Result<BTreeSet<String>, LixError> {
    let mut ids = BTreeSet::new();
    for member in commits
        .iter()
        .flat_map(|commit| &commit.members)
        .filter(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
    {
        let blob_id = member
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("blob_hash"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "live sync binary blob ref has no blob_hash",
                )
            })?;
        super::validate_blake3_id(blob_id, "sync binary blob ref blob_hash")?;
        ids.insert(blob_id.to_owned());
    }
    Ok(ids)
}

fn append_bounded_inline_blob(
    inline_blobs: &mut Vec<SyncBlobManifest>,
    encoded_len: &mut usize,
    manifest: SyncBlobManifest,
    context: &str,
) -> Result<bool, LixError> {
    let manifest_len = serde_json::to_vec(&manifest)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode {context}: {error}"),
            )
        })?
        .len();
    let addition = manifest_len.saturating_add(usize::from(!inline_blobs.is_empty()));
    if encoded_len.saturating_add(addition) > super::MAX_SYNC_PULL_RESPONSE_BYTES {
        return Ok(false);
    }
    *encoded_len += addition;
    inline_blobs.push(manifest);
    Ok(true)
}

/// Loads either representation that can own a canonical change payload.
///
/// Snapshot bootstrap stores every currently-live change in the standalone
/// changelog namespace, including changes whose deferred commit body has not
/// arrived yet. Ordinary commit import may instead address the same payload
/// through its commit delta. History hydration must recognize both forms as
/// one logical change, otherwise filling a deferred body collides with the
/// already-readable hot snapshot row.
async fn load_existing_sync_change(
    read: &(impl StorageAdapterRead + ?Sized),
    change_id: ChangeId,
) -> Result<Option<ChangeRecord>, LixError> {
    let change_ids = [change_id];
    let mut changelog = ChangelogContext::new().reader(read);
    let stored = changelog
        .load_changes(ChangeLoadRequest {
            change_ids: &change_ids,
        })
        .await?
        .into_iter()
        .next()
        .expect("one changelog change was requested")
        .1;
    match stored {
        Some(change) => Ok(Some(change)),
        None => load_change_record_by_id(read, change_id).await,
    }
}

fn sync_change_records_equal(
    existing: &ChangeRecord,
    incoming: &ChangeRecord,
) -> Result<bool, LixError> {
    if existing.format_version != incoming.format_version
        || existing.change_id != incoming.change_id
        || existing.account_id != incoming.account_id
        || existing.schema_key != incoming.schema_key
        || existing.row_pk != incoming.row_pk
        || existing.file_id != incoming.file_id
        || existing.metadata != incoming.metadata
        || existing.created_at != incoming.created_at
        || existing.origin_key != incoming.origin_key
    {
        return Ok(false);
    }
    match (&existing.snapshot, &incoming.snapshot) {
        (None, None) => Ok(true),
        (Some(left), Some(right)) if left == right => Ok(true),
        (Some(left), Some(right)) => {
            let left = crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
                left.clone().into(),
                &existing.schema_key,
                &existing.row_pk,
            )?;
            let right = crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
                right.clone().into(),
                &incoming.schema_key,
                &incoming.row_pk,
            )?;
            Ok(left == right)
        }
        (None, Some(_)) | (Some(_), None) => Ok(false),
    }
}

fn stage_imported_commit_body(
    writes: &mut StorageWriteSet,
    commit: &ParsedCommit,
    imported_authored_change_ids: &mut BTreeSet<ChangeId>,
    selected_fallbacks: &mut BTreeMap<ChangeId, CommitDeltaChangeLocator>,
    authored: &mut BTreeMap<ChangeId, CommitDeltaChangeLocator>,
) -> Result<CommitStateMutationInventory, LixError> {
    let deltas = commit
        .members
        .iter()
        .map(|member| member.as_commit_delta(commit.commit_id))
        .collect::<Vec<_>>();
    let mut authored_change_ids = BTreeSet::new();
    let addressable = commit
        .members
        .iter()
        .map(|member| {
            if !member.authored {
                return false;
            }
            authored_change_ids.insert(member.change_id);
            direct_change_locator(member.change_id)
                .is_some_and(|locator| locator.commit_id == commit.commit_id)
        })
        .collect::<Vec<_>>();
    let staged = stage_imported_addressable_commit_deltas(writes, &deltas, &addressable)?;
    for ((member, assigned), addressable) in commit
        .members
        .iter()
        .zip(&staged.assigned_change_ids)
        .zip(&addressable)
    {
        if *addressable && member.change_id != *assigned {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync commit contains a noncanonical authored change id",
            ));
        }
    }
    imported_authored_change_ids.extend(authored_change_ids.iter().copied());
    for locator in staged.locators.iter().cloned() {
        if authored_change_ids.contains(&locator.change_id) {
            authored.insert(locator.change_id, locator);
        } else {
            selected_fallbacks
                .entry(locator.change_id)
                .or_insert(locator);
        }
    }
    Ok(staged.mutation_inventory().clone())
}

async fn stage_missing_selected_change_locators(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    selected_fallbacks: BTreeMap<ChangeId, CommitDeltaChangeLocator>,
) -> Result<(), LixError> {
    if selected_fallbacks.is_empty() {
        return Ok(());
    }
    let locators = selected_fallbacks.into_values().collect::<Vec<_>>();
    let keys = locators
        .iter()
        .map(|locator| {
            StorageKey(Bytes::copy_from_slice(
                locator.change_id.as_uuid().as_bytes(),
            ))
        })
        .collect::<Vec<_>>();
    let existing = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            keys: &keys,
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let missing = locators
        .into_iter()
        .zip(keys)
        .zip(existing.values)
        .filter_map(|((locator, key), existing)| {
            existing.is_none().then(|| {
                preconditions.push(StoragePrecondition::KeyAbsent {
                    space: TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                    key,
                });
                locator
            })
        })
        .collect::<Vec<_>>();
    stage_change_locators(writes, &missing);
    Ok(())
}

fn format_sync_state_root_id(root_id: &TrackedStateRootId) -> String {
    blake3::Hash::from_bytes(*root_id.as_bytes())
        .to_hex()
        .to_string()
}

fn parse_sync_state_root_id(value: &str) -> Result<TrackedStateRootId, LixError> {
    let hash = blake3::Hash::from_hex(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync stateRootId must be 64 lowercase hexadecimal characters: {error}"),
        )
    })?;
    if hash.to_hex().as_str() != value {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync stateRootId must use canonical lowercase hexadecimal",
        ));
    }
    Ok(TrackedStateRootId::new(*hash.as_bytes()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncLiveValueRootId([u8; 32]);

fn format_sync_live_value_root_id(root_id: &SyncLiveValueRootId) -> String {
    blake3::Hash::from_bytes(root_id.0).to_hex().to_string()
}

fn parse_sync_live_value_root_id(value: &str) -> Result<SyncLiveValueRootId, LixError> {
    let hash = blake3::Hash::from_hex(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync live value root must be 64 lowercase hexadecimal characters: {error}"),
        )
    })?;
    if hash.to_hex().as_str() != value {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync live value root must use canonical lowercase hexadecimal",
        ));
    }
    Ok(SyncLiveValueRootId(*hash.as_bytes()))
}

struct SyncLiveValueRowRef<'a> {
    schema_key: &'a str,
    file_id: Option<&'a str>,
    row_pk: &'a RowPk,
    change_id: ChangeId,
    commit_id: CommitId,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot_json: &'a str,
    metadata_json: Option<&'a str>,
    change_account_id: &'a str,
    change_created_at: LixTimestamp,
    origin_key: Option<&'a str>,
}

#[derive(Clone)]
struct SyncLiveValueRow {
    schema_key: String,
    file_id: Option<String>,
    row_pk: RowPk,
    change_id: ChangeId,
    commit_id: CommitId,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot_json: String,
    metadata_json: Option<String>,
    change_account_id: String,
    change_created_at: LixTimestamp,
    origin_key: Option<String>,
}

impl SyncLiveValueRow {
    fn as_ref(&self) -> SyncLiveValueRowRef<'_> {
        SyncLiveValueRowRef {
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            row_pk: &self.row_pk,
            change_id: self.change_id,
            commit_id: self.commit_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            snapshot_json: &self.snapshot_json,
            metadata_json: self.metadata_json.as_deref(),
            change_account_id: &self.change_account_id,
            change_created_at: self.change_created_at,
            origin_key: self.origin_key.as_deref(),
        }
    }
}

fn canonical_sync_jsonb(value: &str, context: &str) -> Result<Vec<u8>, LixError> {
    let value = serde_json::from_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {context} while certifying sync live state: {error}"),
        )
    })?;
    lix_schema::Jsonb::from_value(value)
        .binary()
        .map(|bytes| bytes.into_owned())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode {context} while certifying sync live state: {error}"),
            )
        })
}

fn hash_len_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn sync_live_value_root<'a>(
    rows: impl IntoIterator<Item = SyncLiveValueRowRef<'a>>,
) -> Result<SyncLiveValueRootId, LixError> {
    let mut members = BTreeMap::<Vec<u8>, [u8; 32]>::new();
    for row in rows {
        let key = encode_key_ref(TrackedStateKeyRef {
            schema_key: row.schema_key,
            file_id: row.file_id,
            row_pk: row.row_pk,
        });
        let snapshot = canonical_sync_jsonb(row.snapshot_json, "row snapshot")?;
        let metadata = row
            .metadata_json
            .map(|metadata| canonical_sync_jsonb(metadata, "row metadata"))
            .transpose()?;
        let mut member = blake3::Hasher::new();
        member.update(b"lix.sync.live-value.row.v2\0");
        hash_len_prefixed(&mut member, &key);
        member.update(row.change_id.as_uuid().as_bytes());
        member.update(row.commit_id.as_uuid().as_bytes());
        member.update(&row.created_at.packed().to_be_bytes());
        member.update(&row.updated_at.packed().to_be_bytes());
        hash_len_prefixed(&mut member, &snapshot);
        match metadata {
            Some(metadata) => {
                member.update(&[1]);
                hash_len_prefixed(&mut member, &metadata);
            }
            None => {
                member.update(&[0]);
            }
        };
        hash_len_prefixed(&mut member, row.change_account_id.as_bytes());
        member.update(&row.change_created_at.packed().to_be_bytes());
        match row.origin_key {
            Some(origin_key) => {
                member.update(&[1]);
                hash_len_prefixed(&mut member, origin_key.as_bytes());
            }
            None => {
                member.update(&[0]);
            }
        }
        if members.insert(key, *member.finalize().as_bytes()).is_some() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync live value certificate contains a duplicate row identity",
            ));
        }
    }
    let mut root = blake3::Hasher::new();
    root.update(b"lix.sync.live-value.root.v2\0");
    root.update(
        &u64::try_from(members.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for (key, digest) in members {
        hash_len_prefixed(&mut root, &key);
        root.update(&digest);
    }
    Ok(SyncLiveValueRootId(*root.finalize().as_bytes()))
}

fn sync_header_from_record(record: &CommitRecord, global_scope: bool) -> SyncCommitHeader {
    SyncCommitHeader {
        commit_id: record.commit_id.to_string(),
        parent_commit_ids: record
            .parent_commit_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        base_commit_id: record.base_commit_id.map(|id| id.to_string()),
        account_id: record.account_id.clone(),
        created_at: record.created_at.to_string(),
        global_scope,
        generation: record.generation,
        first_parent_jump_commit_id: (record.first_parent_jump_span > 0)
            .then(|| record.first_parent_jump_commit_id.to_string()),
        first_parent_jump_span: (record.first_parent_jump_span > 0)
            .then_some(record.first_parent_jump_span),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotRowCursor {
    schema_key: String,
    file_id: Option<String>,
    row_pk: serde_json::Value,
}

fn encode_snapshot_row_cursor(
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> Result<String, LixError> {
    let wire = SnapshotRowCursor {
        schema_key: schema_key.to_owned(),
        file_id: file_id.map(str::to_owned),
        row_pk: row_pk.as_typed_json_array_value()?,
    };
    let bytes = serde_json::to_vec(&wire).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync snapshot continuation: {error}"),
        )
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_snapshot_row_cursor(value: &str) -> Result<(String, Option<String>, RowPk), LixError> {
    if value.len() > 4096 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync snapshot continuation is too large",
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("decode sync snapshot continuation: {error}"),
            )
        })?;
    let wire: SnapshotRowCursor = serde_json::from_slice(&bytes).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("decode sync snapshot continuation: {error}"),
        )
    })?;
    let row_pk = RowPk::from_typed_json_array_value(&wire.row_pk).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("decode sync snapshot continuation row identity: {error}"),
        )
    })?;
    Ok((wire.schema_key, wire.file_id, row_pk))
}

pub(crate) const SYNC_SEQUENCE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0010),
    "sync.repository_sequence.v1",
    ValueSemantics::Mutable,
);

pub(crate) const SYNC_REPOSITORY_EVENT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0011),
    "sync.repository_event.v1",
    ValueSemantics::Immutable,
);

pub(crate) const SYNC_REPLICA_STATE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0014),
    "sync.replica_state.v3",
    ValueSemantics::Mutable,
);

// Authority admission and replica receipts are mutually exclusive repository
// roles, so their distinct keys share the existing private sync-control space.
// Keeping the marker here avoids expanding the public registered-space list.
pub(crate) const SYNC_AUTHORITY_STATE_SPACE: StorageSpace = SYNC_REPLICA_STATE_SPACE;

const SEQUENCE_KEY: &[u8] = b"repository";
const REPLICA_STATE_KEY: &[u8] = b"repository";
const AUTHORITY_STATE_KEY: &[u8] = b"authority";
pub(crate) const AUTHORITY_STATE_VALUE: &[u8] = b"certified-authority-v4";
const AMBIGUOUS_REPLICA_STATE_CODE: &str = "LIX_ERROR_SYNC_REPLICA_STATE_AMBIGUOUS";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RepositoryEventRecord {
    cursor: u64,
    commit_ids: Vec<String>,
    ref_updates: Vec<SyncRefUpdate>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncReplicaState {
    active_account_id: String,
    cursor: u64,
    authoritative_branches: BTreeMap<String, AuthoritativeBranchCoordinate>,
    /// Locally computed complete-value roots at each installed authority ref.
    /// This is private cache metadata, never part of the sync wire.
    #[serde(default)]
    certified_branch_roots: BTreeMap<String, CertifiedBranchRoots>,
    /// Last cursor whose locally computed roots were compared with an
    /// authority Snapshot response.
    #[serde(default)]
    snapshot_certified_cursor: Option<u64>,
    /// Restore operations are branch intent, not graph-shape inference. They
    /// are recorded in the same atomic commit as `lix_restore` so reconnect,
    /// crash recovery, and post-restore local commits preserve the exact CAS
    /// coordinate that the user intended to reset.
    #[serde(default)]
    pending_resets: BTreeMap<String, PendingSyncReset>,
    /// Commit objects observed from the authority but not yet made reachable
    /// from a fully converged set of authority refs.
    ///
    /// Large offline outboxes publish dependency-closed commit batches before
    /// their final ref update. Remembering those acknowledgements makes the
    /// next batch advance instead of rebuilding the same first 512 commits.
    authority_known_commit_ids: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CertifiedBranchRoots {
    head_state_root_id: String,
    checkpoint_state_root_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingSyncReset {
    expected_authority_head_commit_id: String,
    expected_authority_checkpoint_commit_id: String,
    restore_target_commit_id: String,
    /// A commit on the restored local chain that the authority is already
    /// known to have. This is computed once, when restore is rare, rather than
    /// rescanning history on every ordinary sync push.
    authority_known_ancestor_commit_id: Option<String>,
    /// The actual ref head included in the latest prepared reset request. It
    /// can differ from the restore target when local commits follow restore.
    #[serde(default)]
    prepared_reset_head_commit_id: Option<String>,
    /// Reset request heads prepared before a newer restore replaced them. An
    /// acknowledgement for one of these rebases, rather than erases, the
    /// newest local restore intent.
    #[serde(default)]
    superseded_prepared_reset_head_commit_ids: BTreeSet<String>,
}

const MAX_SUPERSEDED_RESET_HEADS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SyncReplicaBinding {
    Unbound,
    Bound { account_id: String },
    Ambiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitialSyncSnapshotInstall {
    Installed,
    ExistingRepository,
    Ambiguous,
}

/// The authority's complete coordinate for one known branch.
///
/// Absence from `SyncReplicaState::authoritative_branches` means the branch has
/// never been observed. A present `Deleted` value is a durable deletion. A
/// headed branch always carries its checkpoint, making the invalid split-map
/// state "head without checkpoint" unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "state"
)]
enum AuthoritativeBranchCoordinate {
    Deleted,
    Headed {
        head_commit_id: String,
        checkpoint_commit_id: String,
    },
}

impl AuthoritativeBranchCoordinate {
    fn from_wire(
        head_commit_id: Option<String>,
        checkpoint_commit_id: Option<String>,
        context: &str,
    ) -> Result<Self, LixError> {
        match (head_commit_id, checkpoint_commit_id) {
            (None, None) => Ok(Self::Deleted),
            (Some(head_commit_id), Some(checkpoint_commit_id)) => Ok(Self::Headed {
                head_commit_id,
                checkpoint_commit_id,
            }),
            _ => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("{context} head and checkpoint must be paired"),
            )),
        }
    }

    fn head_commit_id(&self) -> Option<&str> {
        match self {
            Self::Deleted => None,
            Self::Headed { head_commit_id, .. } => Some(head_commit_id),
        }
    }

    fn checkpoint_commit_id(&self) -> Option<&str> {
        match self {
            Self::Deleted => None,
            Self::Headed {
                checkpoint_commit_id,
                ..
            } => Some(checkpoint_commit_id),
        }
    }
}

struct ReplicaStatePublication<'a> {
    expected_cursor: u64,
    expected_state_raw: &'a Bytes,
    state: &'a SyncReplicaState,
}

pub(crate) fn replica_state_key() -> StorageKey {
    StorageKey(Bytes::from_static(REPLICA_STATE_KEY))
}

pub(crate) fn authority_state_key() -> StorageKey {
    StorageKey(Bytes::from_static(AUTHORITY_STATE_KEY))
}

async fn load_authority_state(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<Bytes>, LixError> {
    let key = authority_state_key();
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_AUTHORITY_STATE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    values
        .values
        .into_iter()
        .next()
        .flatten()
        .map(|value| match value {
            StorageProjectedValue::FullValue(value) => Ok(value),
            StorageProjectedValue::KeyOnly => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync authority state read omitted its value",
            )),
        })
        .transpose()
}

/// Durably fences a repository into authority ownership before server
/// sessions can publish writes. Reopening a server accepts the exact marker;
/// a plain engine over the same storage never receives the process-local
/// adapter admission and therefore fails its atomic marker-absence fence.
pub(crate) async fn admit_sync_authority_storage<StorageImpl>(
    adapter: &StorageAdapter<StorageImpl>,
    expected_mutation_revision: Option<Bytes>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    for _ in 0..2 {
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if load_replica_state(&read).await?.0.is_some() {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "a certified replica cache cannot be opened as repository authority",
            ));
        }
        match load_authority_state(&read).await? {
            Some(value) if value.as_ref() == AUTHORITY_STATE_VALUE => {
                adapter.admit_sync_authority_writer();
                return Ok(());
            }
            Some(_) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync authority state marker is corrupt or unsupported",
                ));
            }
            None => {}
        }
        drop(read);
        let mut writes = adapter.new_write_set();
        writes.put(
            SYNC_AUTHORITY_STATE_SPACE,
            authority_state_key(),
            AUTHORITY_STATE_VALUE.to_vec(),
        );
        match adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![
                        crate::storage_adapter::repository_mutation_revision_precondition(
                            expected_mutation_revision.clone(),
                        ),
                    ],
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await
        {
            Ok(_) => {
                adapter.admit_sync_authority_writer();
                return Ok(());
            }
            Err(_) => {
                // Another server admission may have installed the same marker.
                // Re-read once and accept only its exact protocol value.
            }
        }
    }
    Err(LixError::new(
        LixError::CODE_TRANSACTION_CONFLICT,
        "repository authority ownership changed during server admission",
    ))
}

async fn load_replica_state(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<(Option<SyncReplicaState>, Option<Bytes>), LixError> {
    let key = replica_state_key();
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_REPLICA_STATE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let raw = values
        .values
        .into_iter()
        .next()
        .flatten()
        .map(|value| match value {
            StorageProjectedValue::FullValue(value) => Ok(value),
            StorageProjectedValue::KeyOnly => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync replica state read omitted its value",
            )),
        })
        .transpose()?;
    let state = raw
        .as_ref()
        .filter(|raw| raw.as_ref() != AUTHORITY_STATE_VALUE)
        .map(|raw| {
            serde_json::from_slice(raw).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode sync replica state: {error}"),
                )
            })
        })
        .transpose()?;
    Ok((state, raw))
}

pub(crate) async fn load_sync_replica_account(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<String>, LixError> {
    Ok(load_replica_state(read)
        .await?
        .0
        .map(|state| state.active_account_id))
}

pub(super) async fn inspect_sync_replica_binding(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<SyncReplicaBinding, LixError> {
    let exact = load_replica_state(read).await?.0;
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SYNC_REPLICA_STATE_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let keys = cursor.next_page(2).await?;
    match (exact, keys.len()) {
        (None, 0) => Ok(SyncReplicaBinding::Unbound),
        (None, _) => Ok(SyncReplicaBinding::Ambiguous),
        (Some(state), 1) => Ok(SyncReplicaBinding::Bound {
            account_id: state.active_account_id,
        }),
        (Some(_), _) => Ok(SyncReplicaBinding::Ambiguous),
    }
}

/// Moves the one legacy URL-keyed sync receipt to the repository-scoped key.
///
/// The raw bytes are preserved so repository admission cannot alter cursor or
/// outbox acknowledgement state. Lix supports exactly one sync authority per
/// local store, so more than one receipt is ambiguous and must fail closed.
pub(crate) async fn migrate_legacy_sync_replica_state<StorageImpl>(
    adapter: &StorageAdapter<StorageImpl>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let read = adapter.begin_read(StorageReadOptions::default()).await?;
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SYNC_REPLICA_STATE_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let (entries, _) = cursor.next_page(2).await?.into_parts();
    if entries.is_empty() {
        return Ok(());
    }
    if entries.len() != 1 {
        return Err(ambiguous_replica_state_error());
    }
    let entry = entries.into_iter().next().expect("one replica-state row");
    drop(cursor);
    if entry.key == replica_state_key() {
        return Ok(());
    }
    let StorageProjectedValue::FullValue(raw) = entry.value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "legacy sync replica-state scan omitted its value",
        ));
    };
    serde_json::from_slice::<SyncReplicaState>(&raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode sync replica state: {error}"),
        )
    })?;

    let canonical_key = replica_state_key();
    let mut writes = adapter.new_write_set();
    writes.put(
        SYNC_REPLICA_STATE_SPACE,
        canonical_key.clone(),
        raw.to_vec(),
    );
    writes.delete(SYNC_REPLICA_STATE_SPACE, entry.key.clone());
    drop(read);
    let commit = adapter
        .commit_certified_replica_write_set(
            super::certified_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions: vec![
                    StoragePrecondition::KeyAbsent {
                        space: SYNC_REPLICA_STATE_SPACE,
                        key: canonical_key,
                    },
                    StoragePrecondition::KeyValueEquals {
                        space: SYNC_REPLICA_STATE_SPACE,
                        key: entry.key,
                        expected: raw,
                    },
                ],
                await_durable: true,
                ..StorageWriteOptions::default()
            },
        )
        .await;
    match commit {
        Ok(_) => Ok(()),
        Err(error) => {
            let error = LixError::from(error);
            if error.code != LixError::CODE_TRANSACTION_CONFLICT
                && error.code != LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
            {
                return Err(error);
            }
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            match inspect_sync_replica_binding(&read).await? {
                SyncReplicaBinding::Bound { .. } => Ok(()),
                SyncReplicaBinding::Unbound | SyncReplicaBinding::Ambiguous => Err(error),
            }
        }
    }
}

fn ambiguous_replica_state_error() -> LixError {
    LixError::new(
        AMBIGUOUS_REPLICA_STATE_CODE,
        "sync replica contains multiple durable authority receipts",
    )
}

pub(crate) async fn has_any_sync_replica_state(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<bool, LixError> {
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SYNC_REPLICA_STATE_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync replica-state scan omitted its value",
                ));
            };
            if entry.key != authority_state_key() || value.as_ref() != AUTHORITY_STATE_VALUE {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Commit bodies named by retained repository events remain part of the
/// authority's replay contract until those events expire.
pub(crate) async fn load_replayable_repository_event_commit_ids(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<BTreeSet<CommitId>, LixError> {
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SYNC_REPOSITORY_EVENT_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let mut commit_ids = BTreeSet::new();
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "repository sync event scan omitted its value",
                ));
            };
            let record: RepositoryEventRecord =
                serde_json::from_slice(&value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("decode repository sync event for GC retention: {error}"),
                    )
                })?;
            for commit_id in record.commit_ids {
                commit_ids.insert(CommitId::parse_lix(
                    &commit_id,
                    "repository sync event commit id",
                )?);
            }
        }
    }
    Ok(commit_ids)
}

/// Commits that must remain exportable until every configured authority has
/// acknowledged them. Compact checkpoints add a physical-state source edge
/// outside canonical parent ancestry, so the pending outbox closure follows
/// both edge kinds. Once an authority reports an id as known, traversal stops
/// there and ordinary checkpoint GC can reclaim older interval history.
pub(crate) async fn load_pending_sync_export_commit_ids(
    read: &(impl StorageAdapterRead + ?Sized),
    controls: &[(String, BranchHeadControl)],
) -> Result<BTreeSet<CommitId>, LixError> {
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SYNC_REPLICA_STATE_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let mut states = Vec::new();
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync replica-state retention scan omitted its value",
                ));
            };
            if entry.key == authority_state_key() && value.as_ref() == AUTHORITY_STATE_VALUE {
                continue;
            }
            states.push(
                serde_json::from_slice::<SyncReplicaState>(&value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("decode sync replica state for GC retention: {error}"),
                    )
                })?,
            );
        }
    }

    let mut retained = BTreeSet::new();
    for state in states {
        let mut known = BTreeSet::new();
        for coordinate in state.authoritative_branches.values() {
            if let AuthoritativeBranchCoordinate::Headed {
                head_commit_id,
                checkpoint_commit_id,
            } = coordinate
            {
                known.insert(CommitId::parse_lix(
                    head_commit_id,
                    "sync GC authority head",
                )?);
                known.insert(CommitId::parse_lix(
                    checkpoint_commit_id,
                    "sync GC authority checkpoint",
                )?);
            }
        }
        for commit_id in state.authority_known_commit_ids {
            known.insert(CommitId::parse_lix(
                &commit_id,
                "sync GC authority-known commit",
            )?);
        }

        let mut pending = controls
            .iter()
            .flat_map(|(_, control)| {
                [
                    Some(control.head_commit_id),
                    control.working_diff_checkpoint_commit_id,
                ]
            })
            .flatten()
            .collect::<Vec<_>>();
        let mut seen = BTreeSet::new();
        while let Some(commit_id) = pending.pop() {
            if known.contains(&commit_id) || !seen.insert(commit_id) {
                continue;
            }
            let record = load_commit_record(read, commit_id).await?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("pending sync export commit '{commit_id}' has no commit record"),
                )
            })?;
            retained.insert(commit_id);
            pending.extend(record.parent_commit_ids.iter().copied());
            if let Some(alias) = load_sync_commit_state_alias(read, commit_id).await? {
                pending.push(CommitId::parse_lix(
                    &alias.source_commit_id,
                    "pending sync export complete-state source",
                )?);
            }
        }
    }
    Ok(retained)
}

fn stage_replica_state(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    state: &SyncReplicaState,
    previous: Option<Bytes>,
) -> Result<(), LixError> {
    let key = replica_state_key();
    writes.put(
        SYNC_REPLICA_STATE_SPACE,
        key.clone(),
        serde_json::to_vec(state).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync replica state: {error}"),
            )
        })?,
    );
    preconditions.push(match previous {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: SYNC_REPLICA_STATE_SPACE,
            key,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: SYNC_REPLICA_STATE_SPACE,
            key,
        },
    });
    Ok(())
}

/// Records the active replica's exact authority coordinate for an ancestor restore.
/// The caller commits these writes atomically with the restored branch ref.
pub(crate) async fn stage_sync_restore_intents(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    remote_id: &str,
    restore_targets: &BTreeMap<String, CommitId>,
) -> Result<(), LixError> {
    if restore_targets.is_empty() {
        return Ok(());
    }
    let (state, previous) = load_replica_state(read).await?;
    let Some(mut state) = state else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("sync replica restore has no state for remote '{remote_id}'"),
        ));
    };
    let mut changed = false;
    for (branch_id, restore_target) in restore_targets {
        let Some(AuthoritativeBranchCoordinate::Headed {
            head_commit_id,
            checkpoint_commit_id,
        }) = state.authoritative_branches.get(branch_id).cloned()
        else {
            continue;
        };
        let restore_target = restore_target.to_string();
        let previous_intent = state
            .pending_resets
            .get(branch_id)
            .cloned()
            .filter(|pending| {
                pending.expected_authority_head_commit_id == head_commit_id
                    && pending.expected_authority_checkpoint_commit_id == checkpoint_commit_id
            });
        let target = CommitId::parse_lix(&restore_target, "pending sync restore target")?;
        let authority_known_ancestor_commit_id = find_restore_authority_boundary(
            read,
            &state,
            previous_intent.as_ref(),
            &head_commit_id,
            &checkpoint_commit_id,
            target,
        )
        .await?
        .map(|commit_id| commit_id.to_string());
        let mut superseded_prepared_reset_head_commit_ids = previous_intent
            .as_ref()
            .map(|pending| pending.superseded_prepared_reset_head_commit_ids.clone())
            .unwrap_or_default();
        if let Some(prepared_head) = previous_intent
            .as_ref()
            .and_then(|pending| pending.prepared_reset_head_commit_id.as_ref())
            .filter(|prepared_head| *prepared_head != &restore_target)
            && superseded_prepared_reset_head_commit_ids.len() < MAX_SUPERSEDED_RESET_HEADS
        {
            superseded_prepared_reset_head_commit_ids.insert(prepared_head.clone());
        }
        let intent = PendingSyncReset {
            expected_authority_head_commit_id: head_commit_id,
            expected_authority_checkpoint_commit_id: checkpoint_commit_id,
            restore_target_commit_id: restore_target,
            authority_known_ancestor_commit_id,
            prepared_reset_head_commit_id: None,
            superseded_prepared_reset_head_commit_ids,
        };
        if state.pending_resets.get(branch_id) != Some(&intent) {
            state.pending_resets.insert(branch_id.clone(), intent);
            changed = true;
        }
    }
    if changed {
        stage_replica_state(writes, preconditions, &state, previous)?;
    }
    Ok(())
}

async fn find_restore_authority_boundary(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &SyncReplicaState,
    previous_intent: Option<&PendingSyncReset>,
    current_authority_head: &str,
    current_authority_checkpoint: &str,
    target: CommitId,
) -> Result<Option<CommitId>, LixError> {
    let mut authority_seen = BTreeSet::new();
    let mut primary_authority_pending = VecDeque::new();
    let mut fallback_authority_pending = VecDeque::new();
    {
        let mut enqueue_primary_authority = |commit_id: CommitId| {
            if authority_seen.insert(commit_id) {
                primary_authority_pending.push_back(commit_id);
            }
        };
        enqueue_primary_authority(CommitId::parse_lix(
            current_authority_head,
            "sync authoritative head",
        )?);
        enqueue_primary_authority(CommitId::parse_lix(
            current_authority_checkpoint,
            "sync authoritative checkpoint",
        )?);
        if let Some(previous_boundary) = previous_intent
            .and_then(|pending| pending.authority_known_ancestor_commit_id.as_deref())
        {
            enqueue_primary_authority(CommitId::parse_lix(
                previous_boundary,
                "pending sync restore authority-known ancestor",
            )?);
        }
    }
    {
        let mut enqueue_fallback_authority = |commit_id: CommitId| {
            if authority_seen.insert(commit_id) {
                fallback_authority_pending.push_back(commit_id);
            }
        };
        for coordinate in state.authoritative_branches.values() {
            if let AuthoritativeBranchCoordinate::Headed {
                head_commit_id,
                checkpoint_commit_id,
            } = coordinate
            {
                enqueue_fallback_authority(CommitId::parse_lix(
                    head_commit_id,
                    "sync authoritative head",
                )?);
                enqueue_fallback_authority(CommitId::parse_lix(
                    checkpoint_commit_id,
                    "sync authoritative checkpoint",
                )?);
            }
        }
        for commit_id in &state.authority_known_commit_ids {
            enqueue_fallback_authority(CommitId::parse_lix(
                commit_id,
                "sync authority-known commit",
            )?);
        }
    }

    if authority_seen.contains(&target) {
        return Ok(Some(target));
    }
    let mut target_seen = BTreeSet::from([target]);
    let mut target_pending = VecDeque::from([target]);
    while !target_pending.is_empty()
        || !primary_authority_pending.is_empty()
        || !fallback_authority_pending.is_empty()
    {
        let (authority_commit_id, primary) = primary_authority_pending
            .pop_front()
            .map(|commit_id| (Some(commit_id), true))
            .unwrap_or_else(|| (fallback_authority_pending.pop_front(), false));
        if let Some(commit_id) = authority_commit_id
            && let Some(record) = load_commit_record(read, commit_id).await?
        {
            for parent in record.parent_commit_ids {
                if target_seen.contains(&parent) {
                    return Ok(Some(parent));
                }
                if authority_seen.insert(parent) {
                    if primary {
                        primary_authority_pending.push_back(parent);
                    } else {
                        fallback_authority_pending.push_back(parent);
                    }
                }
            }
        }
        if let Some(commit_id) = target_pending.pop_front()
            && let Some(record) = load_commit_record(read, commit_id).await?
        {
            for parent in record.parent_commit_ids {
                if authority_seen.contains(&parent) {
                    return Ok(Some(parent));
                }
                if target_seen.insert(parent) {
                    target_pending.push_back(parent);
                }
            }
        }
    }
    Ok(None)
}

fn sequence_key() -> StorageKey {
    StorageKey(Bytes::from_static(SEQUENCE_KEY))
}

fn event_key(cursor: u64) -> StorageKey {
    StorageKey(Bytes::copy_from_slice(&cursor.to_be_bytes()))
}

async fn load_sequence(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<(u64, Option<Bytes>), LixError> {
    let key = sequence_key();
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_SEQUENCE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let raw = values
        .values
        .into_iter()
        .next()
        .flatten()
        .map(|value| match value {
            StorageProjectedValue::FullValue(value) => Ok(value),
            StorageProjectedValue::KeyOnly => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "repository sync sequence read omitted its value",
            )),
        })
        .transpose()?;
    let cursor = match raw.as_ref() {
        None => 0,
        Some(raw) if raw.len() == 8 => u64::from_be_bytes(
            raw.as_ref()
                .try_into()
                .expect("repository sequence length was checked"),
        ),
        Some(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "repository sync sequence is corrupt",
            ));
        }
    };
    Ok((cursor, raw))
}

struct ParsedMember {
    change_id: ChangeId,
    authored: bool,
    schema_key: String,
    file_id: Option<String>,
    row_pk: RowPk,
    deleted: bool,
    snapshot_json: Option<String>,
    metadata_json: Option<String>,
    snapshot: Option<Vec<u8>>,
    metadata: Option<lix_schema::Jsonb>,
    row_created_at: LixTimestamp,
    row_updated_at: LixTimestamp,
    change_created_at: LixTimestamp,
    change_account_id: String,
    origin_key: Option<String>,
}

struct ParsedSnapshotRow {
    branch_id: String,
    schema_key: String,
    file_id: Option<String>,
    row_pk: RowPk,
    change_id: ChangeId,
    commit_id: CommitId,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    change_account_id: String,
    change_created_at: LixTimestamp,
    origin_key: Option<String>,
    snapshot_json: String,
    metadata_json: Option<String>,
    snapshot: Vec<u8>,
    metadata: Option<lix_schema::Jsonb>,
}

impl ParsedSnapshotRow {
    fn change_record(&self) -> ChangeRecord {
        ChangeRecord {
            format_version: 2,
            change_id: self.change_id,
            account_id: self.change_account_id.clone(),
            schema_key: self.schema_key.clone(),
            row_pk: self.row_pk.clone(),
            file_id: self.file_id.clone(),
            snapshot: Some(self.snapshot.clone()),
            metadata: self.metadata.clone(),
            created_at: self.change_created_at,
            origin_key: self.origin_key.clone(),
        }
    }

    fn as_root_delta(&self) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            row_pk: &self.row_pk,
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: false,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

fn sync_live_value_root_from_snapshot_rows<'a>(
    rows: impl IntoIterator<Item = &'a ParsedSnapshotRow>,
) -> Result<SyncLiveValueRootId, LixError> {
    sync_live_value_root(rows.into_iter().map(|row| SyncLiveValueRowRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        row_pk: &row.row_pk,
        change_id: row.change_id,
        commit_id: row.commit_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot_json: &row.snapshot_json,
        metadata_json: row.metadata_json.as_deref(),
        change_account_id: &row.change_account_id,
        change_created_at: row.change_created_at,
        origin_key: row.origin_key.as_deref(),
    }))
}

fn snapshot_rows_hot_snapshot<'a>(
    branch_id: &str,
    rows: impl IntoIterator<Item = &'a ParsedSnapshotRow>,
) -> Result<HotTrackedSnapshot, LixError> {
    HotTrackedSnapshot::from_materialized_rows(
        rows.into_iter()
            .filter(|row| {
                branch_id == crate::GLOBAL_BRANCH_ID
                    || row.schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
            })
            .map(|row| {
                Ok(MaterializedTrackedStateRow {
                    row_pk: row.row_pk.clone(),
                    schema_key: row.schema_key.clone(),
                    file_id: row.file_id.clone(),
                    snapshot_content: Some(row.snapshot_json.clone().into()),
                    decoded_snapshot: Some(std::sync::Arc::new(
                        crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
                            std::sync::Arc::from(row.snapshot.clone()),
                            &row.schema_key,
                            &row.row_pk,
                        )?,
                    )),
                    metadata: row.metadata_json.clone().map(Into::into),
                    deleted: false,
                    created_at: row.created_at.to_string(),
                    updated_at: row.updated_at.to_string(),
                    change_id: row.change_id,
                    commit_id: row.commit_id,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?,
    )
}

fn snapshot_rows_with_inherited_catalog<'a>(
    branch_id: &str,
    all_rows: &'a [ParsedSnapshotRow],
    local_rows: &[&'a ParsedSnapshotRow],
) -> BTreeMap<(String, Option<String>, RowPk), &'a ParsedSnapshotRow> {
    let mut rows = BTreeMap::new();
    if branch_id != crate::GLOBAL_BRANCH_ID {
        for row in all_rows.iter().filter(|row| {
            row.branch_id == crate::GLOBAL_BRANCH_ID && row.schema_key == "lix_registered_schema"
        }) {
            rows.insert(
                (
                    row.schema_key.clone(),
                    row.file_id.clone(),
                    row.row_pk.clone(),
                ),
                row,
            );
        }
    }
    for &row in local_rows {
        rows.insert(
            (
                row.schema_key.clone(),
                row.file_id.clone(),
                row.row_pk.clone(),
            ),
            row,
        );
    }
    rows
}

impl ParsedMember {
    fn change_record(&self) -> ChangeRecord {
        ChangeRecord {
            format_version: 2,
            change_id: self.change_id,
            account_id: self.change_account_id.clone(),
            schema_key: self.schema_key.clone(),
            row_pk: self.row_pk.clone(),
            file_id: self.file_id.clone(),
            snapshot: self.snapshot.clone(),
            metadata: self.metadata.clone(),
            created_at: self.change_created_at,
            origin_key: self.origin_key.clone(),
        }
    }

    fn as_root_delta(&self, commit_id: CommitId) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            row_pk: &self.row_pk,
            change_id: self.change_id,
            commit_id,
            deleted: self.deleted,
            created_at: self.row_created_at,
            updated_at: self.row_updated_at,
        }
    }

    fn as_commit_delta(&self, commit_id: CommitId) -> TrackedStateCommitDeltaRef<'_> {
        TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &self.schema_key,
                file_id: self.file_id.as_deref(),
                row_pk: &self.row_pk,
                change_id: self.change_id,
                commit_id,
                deleted: self.deleted,
                created_at: self.row_created_at,
                updated_at: self.row_updated_at,
            },
            snapshot: self.snapshot.as_deref(),
            metadata: self.metadata.as_ref(),
            origin_key: self.origin_key.as_deref(),
            base_coordinate: None,
            authored: self.authored,
        }
    }

    fn as_current_delta(&self, commit_id: CommitId) -> CurrentStateDeltaRef<'_> {
        CurrentStateDeltaRef {
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            row_pk: &self.row_pk,
            change_id: Some(self.change_id),
            commit_id: Some(commit_id),
            untracked: false,
            deleted: self.deleted,
            created_at: self.row_created_at,
            updated_at: self.row_updated_at,
            snapshot: self.snapshot.as_deref(),
            metadata: self.metadata.as_ref(),
            columnar_base_coordinate: None,
        }
    }
}

fn selected_payload_matches_authored(selected: &ParsedMember, authored: &ParsedMember) -> bool {
    authored.authored
        && selected.change_id == authored.change_id
        && selected.schema_key == authored.schema_key
        && selected.file_id == authored.file_id
        && selected.row_pk == authored.row_pk
        && selected.deleted == authored.deleted
        && selected.snapshot_json == authored.snapshot_json
        && selected.metadata_json == authored.metadata_json
        && selected.change_account_id == authored.change_account_id
        && selected.change_created_at == authored.change_created_at
        && selected.origin_key == authored.origin_key
}

struct ParsedCommit {
    wire: SyncCommit,
    commit_id: CommitId,
    parent_commit_ids: Vec<CommitId>,
    base_commit_id: Option<CommitId>,
    account_id: String,
    created_at: LixTimestamp,
    selected_source_commit_id: Option<CommitId>,
    state_alias: Option<(CommitId, TrackedStateRootId)>,
    members: Vec<ParsedMember>,
}

#[derive(Clone)]
struct ParsedSyncHeader {
    commit_id: CommitId,
    parent_commit_ids: Vec<CommitId>,
    base_commit_id: Option<CommitId>,
    account_id: String,
    created_at: LixTimestamp,
    global_scope: bool,
    generation: u64,
    first_parent_jump_commit_id: CommitId,
    first_parent_jump_span: u64,
}

impl ParsedSyncHeader {
    fn parse(header: &SyncCommitHeader) -> Result<Self, LixError> {
        let commit_id = CommitId::parse_lix(&header.commit_id, "sync commit header")?;
        if header.account_id.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync commit header accountId must not be empty",
            ));
        }
        let mut unique_parents = BTreeSet::new();
        let parent_commit_ids = header
            .parent_commit_ids
            .iter()
            .map(|parent| CommitId::parse_lix(parent, "sync parent header"))
            .map(|parent| {
                let parent = parent?;
                if parent == commit_id {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync commit header cannot be its own parent",
                    ));
                }
                if !unique_parents.insert(parent) {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync commit header parent ids must be unique",
                    ));
                }
                Ok(parent)
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let (first_parent_jump_commit_id, first_parent_jump_span) = match (
            &header.first_parent_jump_commit_id,
            header.first_parent_jump_span,
        ) {
            (None, None) if parent_commit_ids.len() != 1 => (commit_id, 0),
            (Some(jump), Some(span)) if parent_commit_ids.len() == 1 && span > 0 => (
                CommitId::parse_lix(jump, "sync first-parent jump header")?,
                span,
            ),
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync header jump id/span must be paired for linear commits and absent for root/merge commits",
                ));
            }
        };
        let base_commit_id = header
            .base_commit_id
            .as_deref()
            .map(|base| CommitId::parse_lix(base, "sync base commit header"))
            .transpose()?;
        if base_commit_id == Some(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync commit header cannot use itself as its base",
            ));
        }
        if header.global_scope && base_commit_id.is_some() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "global sync commit header must not have a base",
            ));
        }
        if !header.global_scope && base_commit_id.is_none() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "local sync commit header must have a base",
            ));
        }
        Ok(Self {
            commit_id,
            parent_commit_ids,
            base_commit_id,
            account_id: header.account_id.clone(),
            created_at: parse_sync_timestamp("sync header createdAt", &header.created_at)?,
            global_scope: header.global_scope,
            generation: header.generation,
            first_parent_jump_commit_id,
            first_parent_jump_span,
        })
    }

    fn record(&self) -> CommitRecord {
        CommitRecord {
            format_version: COMMIT_RECORD_FORMAT_VERSION,
            commit_id: self.commit_id,
            generation: self.generation,
            parent_commit_ids: self.parent_commit_ids.clone(),
            base_commit_id: self.base_commit_id,
            first_parent_jump_commit_id: self.first_parent_jump_commit_id,
            first_parent_jump_span: self.first_parent_jump_span,
            account_id: self.account_id.clone(),
            created_at: self.created_at,
            touched_scope_digest: CommitTouchedScopeDigest::opaque(),
        }
    }
}

fn validate_sync_header_set(
    headers: &BTreeMap<CommitId, ParsedSyncHeader>,
    context: &str,
) -> Result<(), LixError> {
    let mut remaining = headers.keys().copied().collect::<BTreeSet<_>>();
    let mut resolved = BTreeSet::new();
    while !remaining.is_empty() {
        let ready = remaining.iter().copied().find(|commit_id| {
            headers[commit_id]
                .parent_commit_ids
                .iter()
                .all(|parent| !headers.contains_key(parent) || resolved.contains(parent))
        });
        let Some(commit_id) = ready else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("{context} commit header graph contains a cycle"),
            ));
        };
        remaining.remove(&commit_id);
        resolved.insert(commit_id);
    }

    for header in headers.values() {
        if let Some(base_commit_id) = header.base_commit_id
            && let Some(base) = headers.get(&base_commit_id)
            && !base.global_scope
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "{context} header '{}' uses non-global base '{}'",
                    header.commit_id, base_commit_id
                ),
            ));
        }
        let known_parent_generations = header
            .parent_commit_ids
            .iter()
            .filter_map(|parent| headers.get(parent).map(|parent| parent.generation))
            .collect::<Vec<_>>();
        if header.parent_commit_ids.is_empty() && header.generation != 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "{context} root header '{}' has invalid generation",
                    header.commit_id
                ),
            ));
        }
        if !header.parent_commit_ids.is_empty() && header.generation == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "{context} non-root header '{}' has invalid generation",
                    header.commit_id
                ),
            ));
        }
        if let Some(max_known_parent) = known_parent_generations.iter().copied().max() {
            let exact = max_known_parent
                .checked_add(1)
                .ok_or_else(|| LixError::unknown("sync history generation overflow"))?;
            let all_parents_known =
                known_parent_generations.len() == header.parent_commit_ids.len();
            if (all_parents_known && header.generation != exact)
                || (!all_parents_known && header.generation <= max_known_parent)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "{context} header '{}' has invalid generation",
                        header.commit_id
                    ),
                ));
            }
        }
        if header.first_parent_jump_span > 0 {
            let jump = headers
                .get(&header.first_parent_jump_commit_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "{context} header '{}' is missing jump boundary '{}'",
                            header.commit_id, header.first_parent_jump_commit_id
                        ),
                    )
                })?;
            if header.generation.checked_sub(header.first_parent_jump_span) != Some(jump.generation)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "{context} header '{}' has an invalid jump span",
                        header.commit_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncImportPurpose {
    AuthorityPush,
    ReplicaDelta,
    History,
}

impl ParsedCommit {
    fn parse(wire: &SyncCommit) -> Result<Self, LixError> {
        wire.validate()?;
        let commit_id = CommitId::parse_lix(&wire.commit_id, "sync commit id")?;
        let parent_commit_ids = wire
            .parent_commit_ids
            .iter()
            .map(|parent| CommitId::parse_lix(parent, "sync parent commit id"))
            .collect::<Result<Vec<_>, _>>()?;
        let created_at = parse_sync_timestamp("sync commit createdAt", &wire.created_at)?;
        let base_commit_id = wire
            .base_commit_id
            .as_deref()
            .map(|base| CommitId::parse_lix(base, "sync base commit id"))
            .transpose()?;
        if base_commit_id == Some(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync commit cannot use itself as its base",
            ));
        }
        let selected_source_commit_id = wire
            .selected_source_commit_id
            .as_deref()
            .map(|source| CommitId::parse_lix(source, "sync selected source commit id"))
            .transpose()?;
        let state_alias = wire
            .state_alias
            .as_ref()
            .map(|alias| {
                let source = CommitId::parse_lix(
                    &alias.source_commit_id,
                    "sync complete-state source commit id",
                )?;
                let root = blake3::Hash::from_hex(&alias.state_root_id).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync complete-state alias has an invalid stateRootId",
                    )
                })?;
                Ok::<_, LixError>((source, TrackedStateRootId::new(*root.as_bytes())))
            })
            .transpose()?;
        let members = wire
            .members
            .iter()
            .map(parse_sync_member)
            .collect::<Result<Vec<_>, LixError>>()?;
        Ok(Self {
            wire: wire.clone(),
            commit_id,
            parent_commit_ids,
            base_commit_id,
            account_id: wire.account_id.clone(),
            created_at,
            selected_source_commit_id,
            state_alias,
            members,
        })
    }

    fn dependencies(&self) -> impl Iterator<Item = CommitId> + '_ {
        self.parent_commit_ids
            .iter()
            .copied()
            .chain(self.base_commit_id)
            .chain(self.state_alias.iter().map(|(source, _)| *source))
    }
}

fn parse_sync_member(member: &SyncCommitMember) -> Result<ParsedMember, LixError> {
    let snapshot_json = member
        .snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| LixError::unknown(format!("encode sync member snapshot: {error}")))?;
    let metadata_json = member
        .metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| LixError::unknown(format!("encode sync member metadata: {error}")))?;
    let row_pk = RowPk::from_typed_json_array_value(&member.row_pk).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync member rowPk is invalid: {error}"),
        )
    })?;
    let snapshot = member
        .snapshot
        .as_ref()
        .map(|value| encode_sync_typed_snapshot(&member.schema_key, &row_pk, value))
        .transpose()?;
    let metadata = member.metadata.clone().map(lix_schema::Jsonb::from_value);
    Ok(ParsedMember {
        change_id: ChangeId::parse_lix(&member.change_id, "sync member change id")?,
        authored: member.authored,
        schema_key: member.schema_key.clone(),
        file_id: member.file_id.clone(),
        row_pk,
        deleted: member.deleted,
        snapshot,
        metadata,
        snapshot_json,
        metadata_json,
        row_created_at: parse_sync_timestamp("sync member rowCreatedAt", &member.row_created_at)?,
        row_updated_at: parse_sync_timestamp("sync member rowUpdatedAt", &member.row_updated_at)?,
        change_created_at: parse_sync_timestamp(
            "sync member changeCreatedAt",
            &member.change_created_at,
        )?,
        change_account_id: member.change_account_id.clone(),
        origin_key: member.origin_key.clone(),
    })
}

fn parse_sync_timestamp(context: &str, value: &str) -> Result<LixTimestamp, LixError> {
    LixTimestamp::parse(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{context} is invalid: {error}"),
        )
    })
}

async fn certified_sync_live_value_root_with_imports(
    read: &(impl StorageAdapterRead + ?Sized),
    parsed: &BTreeMap<CommitId, ParsedCommit>,
    target: CommitId,
) -> Result<SyncLiveValueRootId, LixError> {
    let mut pending = Vec::new();
    let mut cursor = target;
    let mut rows = loop {
        let Some(commit) = parsed.get(&cursor) else {
            break load_sync_live_value_rows_at_commit(read, cursor).await?;
        };
        pending.push(cursor);
        if let Some((source, _)) = commit.state_alias {
            cursor = source;
        } else if let Some(parent) = commit.parent_commit_ids.first() {
            cursor = *parent;
        } else {
            break BTreeMap::new();
        }
    };

    for commit_id in pending.into_iter().rev() {
        let commit = &parsed[&commit_id];
        for member in &commit.members {
            let key = encode_key_ref(TrackedStateKeyRef {
                schema_key: &member.schema_key,
                file_id: member.file_id.as_deref(),
                row_pk: &member.row_pk,
            });
            if member.deleted {
                rows.remove(&key);
                continue;
            }
            let snapshot_json = member.snapshot_json.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "live sync member has no snapshot while certifying imported state",
                )
            })?;
            // Commit deltas carry the mutation timestamp as rowCreatedAt even
            // for updates. Materialized lifecycle semantics preserve the
            // original creation timestamp while the coordinate remains live.
            let created_at = rows
                .get(&key)
                .map_or(member.row_created_at, |row| row.created_at);
            rows.insert(
                key,
                SyncLiveValueRow {
                    schema_key: member.schema_key.clone(),
                    file_id: member.file_id.clone(),
                    row_pk: member.row_pk.clone(),
                    change_id: member.change_id,
                    commit_id,
                    created_at,
                    updated_at: member.row_updated_at,
                    snapshot_json,
                    metadata_json: member.metadata_json.clone(),
                    change_account_id: member.change_account_id.clone(),
                    change_created_at: member.change_created_at,
                    origin_key: member.origin_key.clone(),
                },
            );
        }
    }
    sync_live_value_root(rows.values().map(SyncLiveValueRow::as_ref))
}

async fn load_sync_live_value_rows_at_commit(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<BTreeMap<Vec<u8>, SyncLiveValueRow>, LixError> {
    let mut tracked = TrackedStateContext::new().reader(read);
    let tracked_rows = tracked
        .scan_batch_at_commit(&commit_id.to_string(), &TrackedStateScanRequest::default())
        .await?;
    let mut rows = BTreeMap::new();
    for row in tracked_rows.iter() {
        let change = load_existing_sync_change(read, row.change_id())
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync live-value row change '{}' is missing",
                        row.change_id()
                    ),
                )
            })?;
        let key = encode_key_ref(TrackedStateKeyRef {
            schema_key: row.schema_key(),
            file_id: row.file_id(),
            row_pk: row.row_pk(),
        });
        rows.insert(
            key,
            SyncLiveValueRow {
                schema_key: row.schema_key().to_owned(),
                file_id: row.file_id().map(str::to_owned),
                row_pk: row.row_pk().clone(),
                change_id: row.change_id(),
                commit_id: row.commit_id(),
                created_at: row.created_at(),
                updated_at: row.updated_at(),
                snapshot_json: row
                    .snapshot_content()
                    .expect("a live tracked row has snapshot content")
                    .to_string(),
                metadata_json: row.metadata().map(ToString::to_string),
                change_account_id: change.account_id,
                change_created_at: change.created_at,
                origin_key: change.origin_key,
            },
        );
    }
    Ok(rows)
}

fn parse_snapshot_row(row: &SyncSnapshotRow) -> Result<ParsedSnapshotRow, LixError> {
    let snapshot = row
        .snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| LixError::unknown(format!("encode sync snapshot row: {error}")))?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "live sync snapshot row has no snapshot",
            )
        })?;
    let metadata = row
        .metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| LixError::unknown(format!("encode sync snapshot metadata: {error}")))?;
    let row_pk = RowPk::from_typed_json_array_value(&row.row_pk).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync snapshot rowPk is invalid: {error}"),
        )
    })?;
    let snapshot_value = row
        .snapshot
        .as_ref()
        .expect("live sync snapshot was checked above");
    let typed_snapshot = encode_sync_typed_snapshot(&row.schema_key, &row_pk, snapshot_value)?;
    Ok(ParsedSnapshotRow {
        branch_id: row.branch_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        row_pk,
        change_id: ChangeId::parse_lix(&row.change_id, "sync snapshot change id")?,
        commit_id: CommitId::parse_lix(&row.commit_id, "sync snapshot row commit id")?,
        created_at: parse_sync_timestamp("sync snapshot createdAt", &row.created_at)?,
        updated_at: parse_sync_timestamp("sync snapshot updatedAt", &row.updated_at)?,
        change_account_id: row.change_account_id.clone(),
        change_created_at: parse_sync_timestamp(
            "sync snapshot changeCreatedAt",
            &row.change_created_at,
        )?,
        origin_key: row.origin_key.clone(),
        snapshot_json: snapshot.clone(),
        metadata_json: metadata.clone(),
        snapshot: typed_snapshot,
        metadata: row.metadata.clone().map(lix_schema::Jsonb::from_value),
    })
}

fn encode_sync_typed_snapshot(
    schema_key: &str,
    row_pk: &RowPk,
    snapshot: &serde_json::Value,
) -> Result<Vec<u8>, LixError> {
    crate::plugin::runtime::WasmTypedRow::from_builtin_json(schema_key, row_pk, snapshot)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync row for schema '{schema_key}' has different content than its declared Schema v1 identity: {}",
                    error.message
                ),
            )
        })?
        .durable_payload()
        .map(|payload| payload.to_vec())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("sync row for schema '{schema_key}' is not durably encodable: {error:?}"),
            )
        })
}

async fn load_commit_record(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitRecord>, LixError> {
    let ids = [commit_id];
    Ok(ChangelogContext::new()
        .reader(read)
        .load_commits(CommitLoadRequest { commit_ids: &ids })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, record)| record))
}

async fn commit_reaches_ancestor(
    read: &(impl StorageAdapterRead + ?Sized),
    descendant: CommitId,
    ancestor: CommitId,
) -> Result<bool, LixError> {
    let mut pending = vec![descendant];
    let mut seen = BTreeSet::new();
    while let Some(commit_id) = pending.pop() {
        if commit_id == ancestor {
            return Ok(true);
        }
        if !seen.insert(commit_id) {
            continue;
        }
        let Some(record) = load_commit_record(read, commit_id).await? else {
            // Snapshot sync deliberately omits cold history bodies. Equality
            // with the requested ancestor is checked before this load, so an
            // absent older body is an unknown ancestry boundary, not local
            // corruption. Another merge parent can still prove reachability,
            // so keep walking every locally-known path before classifying the
            // relation as unknown/non-reachable.
            continue;
        };
        pending.extend(record.parent_commit_ids);
    }
    Ok(false)
}

fn sync_ref_change_id(branch_id: &str, head_commit_id: Option<CommitId>) -> ChangeId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.sync.repository_ref_change.v1");
    hasher.update(&(branch_id.len() as u64).to_be_bytes());
    hasher.update(branch_id.as_bytes());
    match head_commit_id {
        Some(commit_id) => hasher.update(commit_id.as_uuid().as_bytes()),
        None => hasher.update(&[0; 16]),
    };
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    // Keep the derived standalone change outside the commit-address sentinel
    // and direct member address zero.
    if bytes[12..] == [0; 4] {
        bytes[15] = 1;
    }
    ChangeId::new(uuid::Uuid::from_bytes(bytes))
}

fn sync_ref_change_record(
    branch_id: &str,
    head: CommitId,
    account_id: &str,
    created_at: LixTimestamp,
) -> Result<ChangeRecord, LixError> {
    let snapshot = serde_json::json!({
        "id": branch_id,
        "commit_id": head.to_string(),
    });
    let row_pk = RowPk::uuid_from_canonical(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync branch id is not a canonical UUID: {error}"),
        )
    })?;
    let snapshot = encode_sync_typed_snapshot(BRANCH_REF_SCHEMA_KEY, &row_pk, &snapshot)?;
    Ok(ChangeRecord {
        format_version: 2,
        change_id: sync_ref_change_id(branch_id, Some(head)),
        account_id: account_id.to_owned(),
        schema_key: BRANCH_REF_SCHEMA_KEY.to_owned(),
        row_pk,
        file_id: None,
        snapshot: Some(snapshot),
        metadata: None,
        created_at,
        origin_key: None,
    })
}

async fn load_sync_hot_snapshot(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    commit_id: CommitId,
) -> Result<HotTrackedSnapshot, LixError> {
    let mut rows: Vec<MaterializedTrackedStateRow> = TrackedStateContext::new()
        .reader(read)
        .scan_batch_at_commit(
            &commit_id.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns::default(),
                limit: None,
            },
        )
        .await?
        .into_rows()
        .into_iter()
        .filter(|row| {
            branch_id == crate::GLOBAL_BRANCH_ID
                || row.schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
        })
        .collect();
    if branch_id != crate::GLOBAL_BRANCH_ID {
        let node = CommitGraphContext::new()
            .reader(read)
            .load_node(&commit_id)
            .await?
            .ok_or_else(|| LixError::commit_not_found(commit_id.to_string(), "sync", "hot head"))?;
        if let Some(base_commit_id) = node.base_commit_id {
            let inherited_catalog = TrackedStateContext::new()
                .reader(read)
                .scan_batch_at_commit(
                    &base_commit_id.to_string(),
                    &TrackedStateScanRequest {
                        filter: TrackedStateFilter {
                            schema_keys: vec!["lix_registered_schema".to_owned()],
                            include_tombstones: true,
                            ..TrackedStateFilter::default()
                        },
                        read_columns: TrackedStateReadColumns::default(),
                        limit: None,
                    },
                )
                .await?
                .into_rows();
            for inherited in inherited_catalog {
                if !rows.iter().any(|local| {
                    local.schema_key == inherited.schema_key
                        && local.file_id == inherited.file_id
                        && local.row_pk == inherited.row_pk
                }) {
                    rows.push(inherited);
                }
            }
        }
    }
    HotTrackedSnapshot::from_materialized_rows(rows)
}

#[allow(clippy::too_many_arguments)]
async fn stage_sync_hot_delta(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    previous_generation: CommitId,
    head: CommitId,
    deltas: &[CurrentStateDeltaRef<'_>],
    absence_guards: &BTreeSet<TrackedStateKey>,
    checkpoint: CommitId,
    coverage: &mut WorkingDiffIndexCoverage,
) -> Result<CommitId, LixError> {
    TrackedHeadContext::new()
        .writer(read, writes)
        .stage_current_state_with_working_diff(
            branch_id,
            Some(previous_generation),
            head,
            deltas,
            absence_guards,
            None,
            Some(checkpoint),
            coverage,
        )
        .await
}

/// Stages the only authority-side sync metadata needed by an ordinary Lix
/// transaction. Commit bodies remain in their native immutable stores.
pub(crate) async fn stage_repository_transaction_event<R>(
    read: &R,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    commits: &[SyncCommit],
    published_controls: &BTreeMap<String, Option<BranchHeadControl>>,
) -> Result<Option<RepositoryEventRecord>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut commit_ids = commits
        .iter()
        .map(|commit| commit.commit_id.clone())
        .collect::<Vec<_>>();
    let branch_ids = published_controls.keys().cloned().collect::<Vec<_>>();
    let observed = BranchHeadControlContext::new()
        .reader(read)
        .load_many(&branch_ids)
        .await?;
    let mut ref_updates = branch_ids
        .into_iter()
        .zip(observed)
        .filter_map(|(branch_id, before)| {
            let after = published_controls[&branch_id];
            let checkpoint_commit_id = after
                .map(|control| {
                    control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("published sync branch '{branch_id}' has no checkpoint cursor"),
                        )
                    })
                })
                .transpose();
            let checkpoint_commit_id = match checkpoint_commit_id {
                Ok(checkpoint_commit_id) => checkpoint_commit_id,
                Err(error) => return Some(Err(error)),
            };
            let before_coordinate = before.map(|control| {
                (
                    control.head_commit_id,
                    control.working_diff_checkpoint_commit_id,
                )
            });
            let after_coordinate = after.map(|control| {
                (
                    control.head_commit_id,
                    control.working_diff_checkpoint_commit_id,
                )
            });
            if before_coordinate == after_coordinate {
                return None;
            }
            Some(Ok(SyncRefUpdate {
                branch_id,
                expected_head_commit_id: before.map(|control| control.head_commit_id.to_string()),
                expected_checkpoint_commit_id: before
                    .and_then(|control| control.working_diff_checkpoint_commit_id)
                    .map(|checkpoint| checkpoint.to_string()),
                head_commit_id: after.map(|control| control.head_commit_id.to_string()),
                checkpoint_commit_id: checkpoint_commit_id.map(|checkpoint| checkpoint.to_string()),
            }))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    if commit_ids.is_empty() && ref_updates.is_empty() {
        return Ok(None);
    }
    commit_ids.sort();
    commit_ids.dedup();
    ref_updates.sort_by(|left, right| left.branch_id.cmp(&right.branch_id));

    Ok(Some(
        stage_repository_event(read, writes, preconditions, commit_ids, ref_updates).await?,
    ))
}

/// Rejects an Authority transaction before its atomic storage commit when the
/// event it would publish cannot be fetched within the protocol response cap.
/// The borrowed projection is byte-for-byte the public JSON shape without
/// cloning large member payloads merely to measure it.
pub(crate) fn validate_repository_transaction_event_transfer(
    event: &RepositoryEventRecord,
    materialized_commits: &[SyncCommit],
) -> Result<(), LixError> {
    let commits_by_id = materialized_commits
        .iter()
        .map(|commit| (commit.commit_id.as_str(), commit))
        .collect::<BTreeMap<_, _>>();
    let commits = event
        .commit_ids
        .iter()
        .map(|commit_id| {
            commits_by_id.get(commit_id.as_str()).copied().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "authority sync event references unstaged commit '{commit_id}' during preflight"
                    ),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if commits.len() != materialized_commits.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "authority sync preflight materialized a commit outside its repository event",
        ));
    }
    let encoded_len = super::encoded_delta_event_len(event.cursor, &commits, &event.ref_updates)?;
    let transfer_limit = repository_transaction_event_transfer_limit();
    if encoded_len > transfer_limit {
        return Err(LixError::new(
            "LIX_ERROR_SYNC_ITEM_TOO_LARGE",
            format!(
                "authority transaction would publish a {} byte sync event, exceeding the {} byte transfer limit",
                encoded_len, transfer_limit,
            ),
        ));
    }
    Ok(())
}

fn repository_transaction_event_transfer_limit() -> usize {
    #[cfg(test)]
    if let Some(limit) = TEST_REPOSITORY_TRANSACTION_EVENT_TRANSFER_LIMIT.with(Cell::get) {
        return limit;
    }
    super::MAX_SYNC_PULL_RESPONSE_BYTES
}

#[cfg(test)]
thread_local! {
    static TEST_REPOSITORY_TRANSACTION_EVENT_TRANSFER_LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
struct TestRepositoryTransactionEventTransferLimit(Option<usize>);

#[cfg(test)]
impl TestRepositoryTransactionEventTransferLimit {
    fn install(limit: usize) -> Self {
        let previous = TEST_REPOSITORY_TRANSACTION_EVENT_TRANSFER_LIMIT
            .with(|current| current.replace(Some(limit)));
        Self(previous)
    }
}

#[cfg(test)]
impl Drop for TestRepositoryTransactionEventTransferLimit {
    fn drop(&mut self) {
        TEST_REPOSITORY_TRANSACTION_EVENT_TRANSFER_LIMIT.with(|current| current.set(self.0));
    }
}

async fn stage_repository_event<R>(
    read: &R,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    mut commit_ids: Vec<String>,
    mut ref_updates: Vec<SyncRefUpdate>,
) -> Result<RepositoryEventRecord, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    commit_ids.sort();
    ref_updates.sort_by(|left, right| left.branch_id.cmp(&right.branch_id));
    let (cursor, raw_sequence) = load_sequence(read).await?;
    let cursor = cursor
        .checked_add(1)
        .ok_or_else(|| LixError::unknown("repository sync cursor overflow"))?;
    let record = RepositoryEventRecord {
        cursor,
        commit_ids,
        ref_updates,
    };
    let encoded = serde_json::to_vec(&record).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode repository sync event: {error}"),
        )
    })?;
    writes.put(SYNC_REPOSITORY_EVENT_SPACE, event_key(cursor), encoded);
    preconditions.push(StoragePrecondition::KeyAbsent {
        space: SYNC_REPOSITORY_EVENT_SPACE,
        key: event_key(cursor),
    });
    let key = sequence_key();
    writes.put(
        SYNC_SEQUENCE_SPACE,
        key.clone(),
        cursor.to_be_bytes().to_vec(),
    );
    preconditions.push(match raw_sequence {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: SYNC_SEQUENCE_SPACE,
            key,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: SYNC_SEQUENCE_SPACE,
            key,
        },
    });
    Ok(record)
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn load_sync_repository_cursor(
        &self,
        _remote_id: &str,
    ) -> Result<Option<u64>, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        Ok(load_replica_state(&read).await?.0.map(|state| state.cursor))
    }

    pub(crate) async fn sync_repository_cursor_is_certified(
        &self,
        cursor: u64,
    ) -> Result<bool, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        Ok(load_replica_state(&read)
            .await?
            .0
            .is_some_and(|state| state.cursor == cursor && state.snapshot_certified_cursor == Some(cursor)))
    }

    pub(crate) async fn validate_sync_repository_account(
        &self,
        _remote_id: &str,
        active_account_id: &str,
    ) -> Result<(), LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let expected = load_sync_replica_account(&read).await?;
        if expected
            .as_deref()
            .is_some_and(|expected| expected != active_account_id)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync authority active account changed for this replica",
            ));
        }
        Ok(())
    }

    /// Rejects a persisted replica whose visible branch coordinates contain
    /// work that was never acknowledged by its authority.
    ///
    /// Protocol v6 is a hard cut: replicas are certified serving caches, not
    /// peers with an upload outbox. A divergent pre-v6 cache must be discarded
    /// and bootstrapped again instead of being merged or published.
    pub(crate) async fn validate_sync_hot_state_authoritative(&self) -> Result<(), LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let Some(state) = load_replica_state(&read).await?.0 else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync replica is missing its authoritative receipt",
            ));
        };
        let local = BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await?
            .into_iter()
            .map(|(branch_id, control)| {
                (
                    branch_id,
                    AuthoritativeBranchCoordinate::Headed {
                        head_commit_id: control.head_commit_id.to_string(),
                        checkpoint_commit_id: control
                            .working_diff_checkpoint_commit_id
                            .unwrap_or(control.head_commit_id)
                            .to_string(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let authoritative = state
            .authoritative_branches
            .into_iter()
            .filter(|(_, coordinate)| {
                matches!(coordinate, AuthoritativeBranchCoordinate::Headed { .. })
            })
            .collect::<BTreeMap<_, _>>();
        if state.certified_branch_roots.len() != authoritative.len() {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "persisted sync replica predates certified HOT receipts",
            )
            .with_hint(
                "discard this pre-v6 replica cache and bootstrap a certified HOT snapshot from the authority",
            ));
        }
        if local == authoritative {
            return Ok(());
        }
        Err(LixError::new(
            super::SYNC_PROTOCOL_MISMATCH_CODE,
            "persisted sync replica contains unacknowledged local state",
        )
        .with_hint(
            "discard this pre-v6 replica cache and bootstrap a certified HOT snapshot from the authority",
        ))
    }

    /// Verifies the installed replica receipt against one immutable authority
    /// snapshot. Complete-value roots were computed when each delta was
    /// installed, so an already-caught-up fence reads only O(branches)
    /// metadata and never rescans current rows.
    pub(crate) async fn validate_sync_publication_snapshot(
        &self,
        snapshot: &SyncRepositoryPullResponse,
    ) -> Result<(), LixError> {
        let SyncRepositoryPullResponse::Snapshot {
            cursor, branches, ..
        } = snapshot
        else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync publication fence requires snapshot metadata",
            ));
        };
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let (state, previous) = load_replica_state(&read).await?;
        let Some(mut state) = state else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync publication fence lost its replica receipt",
            ));
        };
        if state.cursor != *cursor {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "sync publication fence cursor changed before certification",
            ));
        }
        let branch_ids = branches
            .iter()
            .map(|branch| branch.branch_id.clone())
            .collect::<Vec<_>>();
        let local = BranchHeadControlContext::new()
            .reader(&read)
            .load_many(&branch_ids)
            .await?;
        let headed_branch_count = state
            .authoritative_branches
            .values()
            .filter(|coordinate| {
                matches!(coordinate, AuthoritativeBranchCoordinate::Headed { .. })
            })
            .count();
        if branches.len() != headed_branch_count
            || branches.len() != state.certified_branch_roots.len()
        {
            return Err(LixError::new(
                super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE,
                "sync publication fence branch set differs from its local certificate",
            ));
        }
        for (branch, local) in branches.iter().zip(local) {
            let expected = AuthoritativeBranchCoordinate::from_wire(
                branch.head_commit_id.clone(),
                branch.checkpoint_commit_id.clone(),
                "sync publication snapshot branch",
            )?;
            if state.authoritative_branches.get(&branch.branch_id) != Some(&expected) {
                return Err(LixError::new(
                    super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE,
                    format!(
                        "sync publication fence branch '{}' coordinate is not installed",
                        branch.branch_id
                    ),
                ));
            }
            let local_coordinate = local.map(|control| {
                AuthoritativeBranchCoordinate::Headed {
                    head_commit_id: control.head_commit_id.to_string(),
                    checkpoint_commit_id: control
                        .working_diff_checkpoint_commit_id
                        .unwrap_or(control.head_commit_id)
                        .to_string(),
                }
            });
            if local_coordinate.as_ref() != Some(&expected) {
                return Err(LixError::new(
                    super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE,
                    format!(
                        "sync publication fence branch '{}' local control diverged",
                        branch.branch_id
                    ),
                ));
            }
            let expected_roots = CertifiedBranchRoots {
                head_state_root_id: branch.hot_state_root_id.clone(),
                checkpoint_state_root_id: branch.checkpoint_state_root_id.clone(),
            };
            if state.certified_branch_roots.get(&branch.branch_id) != Some(&expected_roots) {
                return Err(LixError::new(
                    super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE,
                    format!(
                        "sync publication fence branch '{}' live-state roots diverged",
                        branch.branch_id
                    ),
                ));
            }
        }
        let previous = previous.expect("decoded sync replica state has source bytes");
        state.snapshot_certified_cursor = Some(*cursor);
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_replica_state(&mut writes, &mut preconditions, &state, Some(previous))?;
        drop(read);
        adapter
            .commit_certified_replica_write_set(
                super::certified_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn build_sync_push(
        &self,
        remote_id: &str,
        max_items: usize,
    ) -> Result<Option<SyncPushRequest>, LixError> {
        if max_items == 0 || max_items > super::MAX_SYNC_REQUEST_ITEMS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync push item limit must be between 1 and {}",
                    super::MAX_SYNC_REQUEST_ITEMS
                ),
            ));
        }
        let adapter = self.storage_adapter();
        let mut attempted_reconciliations = BTreeSet::new();
        let mut remaining_reconciliations = None;
        loop {
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let Some(state) = load_replica_state(&read).await?.0 else {
                return Ok(None);
            };
            let local_controls = BranchHeadControlContext::new()
                .reader(&read)
                .scan()
                .await?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            let mut known = BTreeSet::new();
            for coordinate in state.authoritative_branches.values() {
                if let AuthoritativeBranchCoordinate::Headed {
                    head_commit_id,
                    checkpoint_commit_id,
                } = coordinate
                {
                    known.insert(CommitId::parse_lix(
                        head_commit_id,
                        "sync authoritative head",
                    )?);
                    known.insert(CommitId::parse_lix(
                        checkpoint_commit_id,
                        "sync authoritative checkpoint",
                    )?);
                }
            }
            for commit_id in &state.authority_known_commit_ids {
                known.insert(CommitId::parse_lix(
                    commit_id,
                    "sync authority-known commit",
                )?);
            }
            let mut commit_ids = BTreeSet::new();
            let mut reset_known_commit_ids = BTreeSet::new();
            let mut ref_updates = Vec::new();
            let mut ref_updates_without_payload = BTreeSet::new();
            let branch_ids = local_controls
                .keys()
                .chain(state.authoritative_branches.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            remaining_reconciliations.get_or_insert(branch_ids.len());
            // Divergence can be created locally without a new remote event (for
            // example reset to an old commit followed by a write). Remember one
            // reconciliation candidate, but first construct every independent
            // dependency-ready push. One conflicted branch must not hold the
            // repository's other refs behind its merge.
            let mut pending_reconciliation = None;
            for branch_id in branch_ids {
                let local_control = local_controls.get(&branch_id).copied();
                let local = local_control.map(|control| control.head_commit_id);
                let local_checkpoint = local_control
                    .map(|control| {
                        control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("local sync branch '{branch_id}' has no checkpoint cursor"),
                            )
                        })
                    })
                    .transpose()?;
                let authoritative_coordinate = state.authoritative_branches.get(&branch_id);
                let authoritative = authoritative_coordinate
                    .and_then(AuthoritativeBranchCoordinate::head_commit_id)
                    .map(|head| CommitId::parse_lix(head, "sync authoritative head"))
                    .transpose()?;
                let authoritative_checkpoint = authoritative_coordinate
                    .and_then(AuthoritativeBranchCoordinate::checkpoint_commit_id)
                    .map(|checkpoint| {
                        CommitId::parse_lix(checkpoint, "sync authoritative checkpoint")
                    })
                    .transpose()?;
                let active_reset = match (
                    state.pending_resets.get(&branch_id),
                    local,
                    authoritative,
                    authoritative_checkpoint,
                ) {
                    (Some(intent), Some(_local), Some(authority), Some(checkpoint))
                        if intent.expected_authority_head_commit_id == authority
                            && intent.expected_authority_checkpoint_commit_id == checkpoint =>
                    {
                        let target = CommitId::parse_lix(
                            &intent.restore_target_commit_id,
                            "pending sync restore target",
                        )?;
                        let boundary = intent
                            .authority_known_ancestor_commit_id
                            .as_deref()
                            .map(|boundary| {
                                CommitId::parse_lix(
                                    boundary,
                                    "pending sync restore authority-known ancestor",
                                )
                            })
                            .transpose()?;
                        Some((target, boundary))
                    }
                    _ => None,
                };
                let active_reset_target = active_reset.map(|(target, _)| target);
                let active_reset_boundary = active_reset.and_then(|(_, boundary)| boundary);
                if let Some(boundary) = active_reset_boundary {
                    reset_known_commit_ids.insert(boundary);
                }
                if local == authoritative && local_checkpoint == authoritative_checkpoint {
                    continue;
                }
                // Authority-known ancestry is enough to make a commit payload
                // dependency-complete, but it does not make a stale ref update
                // safe. Reconciliation owns every truly divergent ref.
                let authority_reaches_local = if active_reset_target.is_some() {
                    false
                } else {
                    match (local, authoritative) {
                        (Some(local), Some(authoritative)) if local == authoritative => true,
                        (Some(local), Some(authoritative)) => {
                            let local_reaches_authority =
                                commit_reaches_ancestor(&read, local, authoritative).await?;
                            let authority_reaches_local = !local_reaches_authority
                                && commit_reaches_ancestor(&read, authoritative, local).await?;
                            let checkpoint_advances_authority = if local_reaches_authority
                                || authority_reaches_local
                            {
                                false
                            } else {
                                match (local_checkpoint, authoritative_checkpoint) {
                                    (Some(local_checkpoint), Some(authoritative_checkpoint)) => {
                                        local_checkpoint != authoritative_checkpoint
                                            && commit_reaches_ancestor(
                                                &read,
                                                local_checkpoint,
                                                authoritative_checkpoint,
                                            )
                                            .await?
                                    }
                                    _ => false,
                                }
                            };
                            if !local_reaches_authority
                                && !authority_reaches_local
                                && !checkpoint_advances_authority
                            {
                                if pending_reconciliation.is_none() {
                                    pending_reconciliation =
                                        Some((branch_id.clone(), local, authoritative));
                                }
                                continue;
                            }
                            authority_reaches_local
                        }
                        _ => false,
                    }
                };
                if let Some(local_head) = local {
                    if let Some(authority_head) = authoritative
                        && authority_reaches_local
                    {
                        // A local ancestor without a matching durable restore
                        // intent is merely behind. Never infer destructive
                        // branch intent from graph shape.
                        if pending_reconciliation.is_none() {
                            pending_reconciliation =
                                Some((branch_id.clone(), local_head, authority_head));
                        }
                        continue;
                    }
                    let mut reached_authority = authoritative.is_none()
                        || (active_reset.is_some() && active_reset_boundary.is_none());
                    let mut pending = vec![local_head];
                    let mut branch_commit_ids = BTreeSet::new();
                    while let Some(cursor) = pending.pop() {
                        if Some(cursor) == authoritative
                            || Some(cursor) == active_reset_boundary
                            || known.contains(&cursor)
                        {
                            reached_authority = true;
                            continue;
                        }
                        if !branch_commit_ids.insert(cursor) {
                            continue;
                        }
                        let record = load_commit_record(&read, cursor).await?.ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_COMMIT_NOT_FOUND,
                                format!("local sync head '{cursor}' has no commit record"),
                            )
                        })?;
                        pending.extend(record.parent_commit_ids.iter().copied());
                        pending.extend(record.base_commit_id);
                        if let Some(alias) = load_sync_commit_state_alias(&read, cursor).await? {
                            pending.push(CommitId::parse_lix(
                                &alias.source_commit_id,
                                "local sync complete-state source",
                            )?);
                        }
                    }
                    if authoritative.is_some() && !reached_authority {
                        // Pull/apply owns divergence reconciliation. Never publish
                        // a stale expected head or overwrite pending local work.
                        // One divergent branch must not stall independent refs.
                        continue;
                    }
                    let reset_has_no_payload =
                        branch_commit_ids.is_empty() && active_reset_target.is_some();
                    commit_ids.extend(branch_commit_ids);
                    if reset_has_no_payload {
                        ref_updates_without_payload.insert(branch_id.clone());
                    }
                }
                ref_updates.push(SyncRefUpdate {
                    branch_id: branch_id.clone(),
                    expected_head_commit_id: authoritative.map(|head| head.to_string()),
                    expected_checkpoint_commit_id: authoritative_coordinate
                        .and_then(AuthoritativeBranchCoordinate::checkpoint_commit_id)
                        .map(str::to_owned),
                    head_commit_id: local.map(|head| head.to_string()),
                    checkpoint_commit_id: local_checkpoint.map(|checkpoint| checkpoint.to_string()),
                });
            }
            if ref_updates.is_empty() {
                let Some((branch_id, local_head, authoritative_head)) = pending_reconciliation
                else {
                    return Ok(None);
                };
                let remaining = remaining_reconciliations
                    .as_mut()
                    .expect("reconciliation budget initializes with branch ids");
                if *remaining == 0 {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "sync reconciliation exceeded the initial branch count",
                    ));
                }
                *remaining -= 1;
                if !attempted_reconciliations.insert((
                    branch_id.clone(),
                    local_head,
                    authoritative_head,
                )) {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "sync reconciliation for branch '{branch_id}' did not advance its head"
                        ),
                    ));
                }
                drop(read);
                let authoritative_checkpoint = state
                    .authoritative_branches
                    .get(&branch_id)
                    .and_then(AuthoritativeBranchCoordinate::checkpoint_commit_id)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("sync authority branch '{branch_id}' has no checkpoint"),
                        )
                    })?;
                self.reconcile_sync_branch(
                    &branch_id,
                    &local_head.to_string(),
                    &authoritative_head.to_string(),
                    authoritative_checkpoint,
                )
                .await?;
                continue;
            }
            let mut remaining = BTreeMap::new();
            for commit_id in commit_ids {
                let commit = load_sync_commit(&read, commit_id).await?.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_COMMIT_NOT_FOUND,
                        format!("local sync commit '{commit_id}' is missing"),
                    )
                })?;
                remaining.insert(commit_id, commit);
            }
            let mut included = known.clone();
            included.extend(reset_known_commit_ids);
            let mut commits = Vec::with_capacity(max_items.min(remaining.len()));
            while commits.len() < max_items && !remaining.is_empty() {
                let ready = remaining
                    .iter()
                    .find(|(_, commit)| {
                        commit
                            .parent_commit_ids
                            .iter()
                            .chain(commit.base_commit_id.iter())
                            .chain(
                                commit
                                    .state_alias
                                    .iter()
                                    .map(|alias| &alias.source_commit_id),
                            )
                            .map(|dependency| {
                                CommitId::parse_lix(dependency, "sync commit dependency")
                            })
                            .all(|dependency| dependency.is_ok_and(|id| included.contains(&id)))
                    })
                    .map(|(commit_id, _)| *commit_id)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "local sync commit graph has an unavailable dependency or cycle",
                        )
                    })?;
                included.insert(ready);
                commits.push(remaining.remove(&ready).expect("ready sync commit exists"));
            }

            let mut capacity = max_items.saturating_sub(commits.len());
            let mut selected_ref_updates = Vec::new();
            for update in ref_updates {
                if capacity == 0 {
                    break;
                }
                let target_is_ready = match update.head_commit_id.as_deref() {
                    None => true,
                    Some(_) if ref_updates_without_payload.contains(&update.branch_id) => true,
                    Some(head) => included.contains(&CommitId::parse_lix(head, "sync ref target")?),
                };
                if target_is_ready {
                    selected_ref_updates.push(update);
                    capacity -= 1;
                }
            }
            if commits.is_empty() && selected_ref_updates.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "bounded sync push could not select a dependency-complete item",
                ));
            }
            let mut request = SyncPushRequest {
                commits,
                ref_updates: selected_ref_updates,
                inline_blobs: Vec::new(),
            };
            let prepared_reset_heads = request
                .ref_updates
                .iter()
                .filter_map(|update| {
                    let intent = state.pending_resets.get(&update.branch_id)?;
                    let head = update.head_commit_id.as_ref()?;
                    Some((update.branch_id.clone(), (intent.clone(), head.clone())))
                })
                .collect::<BTreeMap<_, _>>();
            let blob_ids = sync_commit_blob_ids(&request.commits)?;
            drop(read);
            let commit_refs = request.commits.iter().collect::<Vec<_>>();
            let mut encoded_len =
                super::encoded_delta_event_len(u64::MAX, &commit_refs, &request.ref_updates)?;
            for blob_id in blob_ids {
                let manifest = self.get_sync_inline_blob_manifest(&blob_id).await?;
                if let Some(manifest) = manifest {
                    if !append_bounded_inline_blob(
                        &mut request.inline_blobs,
                        &mut encoded_len,
                        manifest,
                        "inline sync blob",
                    )? {
                        // Inline transfer is optional. Keep the largest
                        // bounded prefix and let the ordinary manifest/chunk
                        // lane carry this and all later blobs.
                        break;
                    }
                }
            }
            if !prepared_reset_heads.is_empty()
                && !self
                    .mark_pending_reset_heads(remote_id, &prepared_reset_heads)
                    .await?
            {
                // A restore or pull changed the durable intent while this
                // request was being built. Rebuild from its new coordinate.
                continue;
            }
            return Ok(Some(request));
        }
    }

    #[cfg(test)]
    async fn mark_pending_reset_heads(
        &self,
        _remote_id: &str,
        prepared: &BTreeMap<String, (PendingSyncReset, String)>,
    ) -> Result<bool, LixError> {
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let (Some(mut state), previous) = load_replica_state(&read).await? else {
            return Ok(false);
        };
        let mut changed = false;
        for (branch_id, (expected, prepared_head)) in prepared {
            let Some(current) = state.pending_resets.get_mut(branch_id) else {
                return Ok(false);
            };
            if current != expected {
                return Ok(false);
            }
            if current.prepared_reset_head_commit_id.as_deref() != Some(prepared_head) {
                current.prepared_reset_head_commit_id = Some(prepared_head.clone());
                changed = true;
            }
            if current
                .superseded_prepared_reset_head_commit_ids
                .remove(prepared_head)
            {
                changed = true;
            }
        }
        if !changed {
            return Ok(true);
        }
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_replica_state(&mut writes, &mut preconditions, &state, previous)?;
        drop(read);
        adapter
            .commit_certified_replica_write_set(
                super::certified_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(true)
    }

    pub(crate) async fn apply_sync_repository_pull(
        &self,
        _remote_id: &str,
        response: &SyncRepositoryPullResponse,
    ) -> Result<(), LixError> {
        match response {
            SyncRepositoryPullResponse::Snapshot { .. } => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot metadata must be completed with commit bodies, headers, and row pages",
            )),
            SyncRepositoryPullResponse::Delta { cursor, events } => {
                let (mut state, expected_state_raw) = {
                    let adapter = self.storage_adapter();
                    let read = adapter.begin_read(StorageReadOptions::default()).await?;
                    let (state, raw) = load_replica_state(&read).await?;
                    let state = state.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "sync delta requires an initialized replica cursor",
                        )
                    })?;
                    let raw = raw.expect("decoded sync replica state has source bytes");
                    (state, raw)
                };
                // A long-poll timeout carries no new repository state. The
                // persisted cursor already proves this heartbeat was applied,
                // so do not rescan branches or durably rewrite identical
                // replica state every time an idle poll expires.
                if events.is_empty() {
                    if state.cursor != *cursor {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "sync delta cursor does not match its final event",
                        ));
                    }
                    return Ok(());
                }

                let expected_cursor = state.cursor;
                let branch_ids = events
                    .iter()
                    .flat_map(|event| event.ref_updates.iter())
                    .map(|update| update.branch_id.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let local_coordinates = {
                    let adapter = self.storage_adapter();
                    let read = adapter.begin_read(StorageReadOptions::default()).await?;
                    BranchHeadControlContext::new()
                        .reader(&read)
                        .load_observed(&branch_ids)
                        .await?
                        .into_iter()
                        .zip(&branch_ids)
                        .map(|(observation, branch_id)| {
                            let head = observation
                                .control
                                .map(|control| control.head_commit_id.to_string());
                            let checkpoint = observation
                                .control
                                .and_then(|control| control.working_diff_checkpoint_commit_id)
                                .map(|checkpoint| checkpoint.to_string());
                            (branch_id.clone(), (head, checkpoint))
                        })
                        .collect::<BTreeMap<_, _>>()
                };
                // A v6 replica is a certified serving cache. It must still be
                // at the exact authority coordinate named by its durable
                // receipt before the next certified delta can be installed.
                // Divergence is evidence of an incompatible/pre-v6 cache, not
                // a merge input.
                for branch_id in &branch_ids {
                    let local = local_coordinates
                        .get(branch_id)
                        .expect("delta branch was loaded once");
                    let authoritative = state.authoritative_branches.get(branch_id);
                    let authoritative_head =
                        authoritative.and_then(AuthoritativeBranchCoordinate::head_commit_id);
                    let authoritative_checkpoint =
                        authoritative.and_then(AuthoritativeBranchCoordinate::checkpoint_commit_id);
                    if (local.0.as_deref(), local.1.as_deref())
                        != (authoritative_head, authoritative_checkpoint)
                    {
                        return Err(LixError::new(
                            super::SYNC_PROTOCOL_MISMATCH_CODE,
                            format!(
                                "sync replica branch '{branch_id}' diverged from its certified authority coordinate"
                            ),
                        )
                        .with_hint(
                            "discard this incompatible replica cache and bootstrap a certified HOT snapshot",
                        ));
                    }
                }
                let mut commits = BTreeMap::new();
                let mut inline_blobs = BTreeMap::new();
                let mut branch_chains =
                    BTreeMap::<String, (Option<String>, Option<String>)>::new();
                for event in events {
                    let next_cursor = state
                        .cursor
                        .checked_add(1)
                        .ok_or_else(|| LixError::unknown("sync replica cursor overflow"))?;
                    if event.cursor != next_cursor {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "sync delta event cursor is not contiguous",
                        ));
                    }
                    for commit in &event.commits {
                        if let Some(existing) =
                            commits.insert(commit.commit_id.clone(), commit.clone())
                            && existing != *commit
                        {
                            return Err(immutable_object_mismatch("commit", &commit.commit_id));
                        }
                    }
                    for manifest in &event.inline_blobs {
                        if let Some(existing) =
                            inline_blobs.insert(manifest.blob_id.clone(), manifest.clone())
                            && existing != *manifest
                        {
                            return Err(immutable_object_mismatch(
                                "blob manifest",
                                &manifest.blob_id,
                            ));
                        }
                    }
                    for update in &event.ref_updates {
                        let authoritative_coordinate =
                            state.authoritative_branches.get(&update.branch_id);
                        let authoritative = authoritative_coordinate
                            .and_then(AuthoritativeBranchCoordinate::head_commit_id);
                        let authoritative_checkpoint = authoritative_coordinate
                            .and_then(AuthoritativeBranchCoordinate::checkpoint_commit_id);
                        if update.expected_head_commit_id.as_deref() != authoritative
                            || update.expected_checkpoint_commit_id.as_deref()
                                != authoritative_checkpoint
                        {
                            return Err(LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!(
                                    "sync delta ref '{}' does not continue its authoritative coordinate",
                                    update.branch_id
                                ),
                            ));
                        }
                        let next_authoritative = AuthoritativeBranchCoordinate::from_wire(
                            update.head_commit_id.clone(),
                            update.checkpoint_commit_id.clone(),
                            "sync delta ref",
                        )?;
                        let chain = branch_chains
                            .entry(update.branch_id.clone())
                            .or_insert_with(|| {
                                (
                                    update.head_commit_id.clone(),
                                    update.checkpoint_commit_id.clone(),
                                )
                            });
                        chain.0 = update.head_commit_id.clone();
                        chain.1 = update.checkpoint_commit_id.clone();
                        state
                            .authoritative_branches
                            .insert(update.branch_id.clone(), next_authoritative);
                    }
                    state.cursor = event.cursor;
                }
                if state.cursor != *cursor {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync delta cursor does not match its final event",
                    ));
                }

                let mut applicable_refs = Vec::new();
                for (branch_id, (head, checkpoint)) in &branch_chains {
                    let local = local_coordinates
                        .get(branch_id)
                        .expect("folded delta branch was loaded once");
                    applicable_refs.push(SyncRefUpdate {
                        branch_id: branch_id.clone(),
                        expected_head_commit_id: local.0.clone(),
                        expected_checkpoint_commit_id: local.1.clone(),
                        head_commit_id: head.clone(),
                        checkpoint_commit_id: checkpoint.clone(),
                    });
                }
                let commits = commits.into_values().collect::<Vec<_>>();
                let inline_blobs = inline_blobs.into_values().collect::<Vec<_>>();
                let parsed = commits
                    .iter()
                    .map(ParsedCommit::parse)
                    .map(|result| result.map(|commit| (commit.commit_id, commit)))
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                let adapter = self.storage_adapter();
                let read = adapter.begin_read(StorageReadOptions::default()).await?;
                let mut roots = BTreeMap::<CommitId, SyncLiveValueRootId>::new();
                for (branch_id, (head, checkpoint)) in &branch_chains {
                    match (head.as_deref(), checkpoint.as_deref()) {
                        (Some(head), Some(checkpoint)) => {
                            let head = CommitId::parse_lix(head, "sync delta branch head")?;
                            let checkpoint =
                                CommitId::parse_lix(checkpoint, "sync delta branch checkpoint")?;
                            let head_root = if let Some(root) = roots.get(&head) {
                                root.clone()
                            } else {
                                let root = certified_sync_live_value_root_with_imports(
                                    &read, &parsed, head,
                                )
                                .await?;
                                roots.insert(head, root.clone());
                                root
                            };
                            let checkpoint_root = if let Some(root) = roots.get(&checkpoint) {
                                root.clone()
                            } else {
                                let root = certified_sync_live_value_root_with_imports(
                                    &read, &parsed, checkpoint,
                                )
                                .await?;
                                roots.insert(checkpoint, root.clone());
                                root
                            };
                            state.certified_branch_roots.insert(
                                branch_id.clone(),
                                CertifiedBranchRoots {
                                    head_state_root_id: format_sync_live_value_root_id(&head_root),
                                    checkpoint_state_root_id: format_sync_live_value_root_id(
                                        &checkpoint_root,
                                    ),
                                },
                            );
                        }
                        (None, None) => {
                            state.certified_branch_roots.remove(branch_id);
                        }
                        _ => unreachable!("sync coordinate validation already paired head/checkpoint"),
                    }
                }
                drop(read);
                Box::pin(self.import_sync_repository(
                    &SyncPushRequest {
                        commits,
                        ref_updates: applicable_refs,
                        inline_blobs,
                    },
                    SyncImportPurpose::ReplicaDelta,
                    None,
                    Some(ReplicaStatePublication {
                        expected_cursor,
                        expected_state_raw: &expected_state_raw,
                        state: &state,
                    }),
                ))
                .await?;
                Ok(())
            }
        }
    }

    pub(crate) async fn try_install_initial_sync_snapshot(
        &self,
        remote_id: &str,
        active_account_id: &str,
        metadata: &SyncRepositoryPullResponse,
        head_commits: &[SyncCommit],
        commit_headers: &[SyncCommitHeader],
        rows: &[SyncSnapshotRow],
        checkpoint_roots: &BTreeMap<String, String>,
    ) -> Result<InitialSyncSnapshotInstall, LixError> {
        self.set_sync_replica_remote_id(remote_id)?;
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let SyncRepositoryPullResponse::Snapshot {
            cursor,
            lix_id,
            default_branch_id,
            branches,
        } = metadata
        else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync repository snapshot installer requires snapshot metadata",
            ));
        };
        self.install_sync_snapshot(
            remote_id,
            active_account_id,
            *cursor,
            lix_id,
            default_branch_id,
            branches,
            head_commits,
            commit_headers,
            rows,
            checkpoint_roots,
        )
        .await
    }

    pub(crate) async fn align_sync_branch_checkpoint(
        &self,
        branch_id: &str,
        expected_head_commit_id: &str,
        checkpoint_commit_id: &str,
    ) -> Result<(), LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    format!("sync branch '{branch_id}' disappeared during reconciliation"),
                )
            })?;
        if control.head_commit_id != expected_head_commit_id {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                format!("sync branch '{branch_id}' changed during reconciliation"),
            ));
        }
        let current_checkpoint = control
            .working_diff_checkpoint_commit_id
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync branch '{branch_id}' has no checkpoint cursor"),
                )
            })?
            .to_string();
        drop(read);
        if current_checkpoint == checkpoint_commit_id {
            return Ok(());
        }
        Box::pin(self.import_sync_repository(
            &SyncPushRequest {
                commits: Vec::new(),
                ref_updates: vec![SyncRefUpdate {
                    branch_id: branch_id.to_owned(),
                    expected_head_commit_id: Some(expected_head_commit_id.to_owned()),
                    expected_checkpoint_commit_id: Some(current_checkpoint),
                    head_commit_id: Some(expected_head_commit_id.to_owned()),
                    checkpoint_commit_id: Some(checkpoint_commit_id.to_owned()),
                }],
                inline_blobs: Vec::new(),
            },
            SyncImportPurpose::ReplicaDelta,
            None,
            None,
        ))
        .await
        .map(|_| ())
    }

    #[cfg(test)]
    async fn store_replica_state(
        &self,
        remote_id: &str,
        state: SyncReplicaState,
    ) -> Result<(), LixError> {
        self.set_sync_replica_remote_id(remote_id)?;
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let (_, previous) = load_replica_state(&read).await?;
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_replica_state(&mut writes, &mut preconditions, &state, previous)?;
        drop(read);
        adapter
            .commit_certified_replica_write_set(
                super::certified_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn install_sync_snapshot(
        &self,
        remote_id: &str,
        active_account_id: &str,
        cursor: u64,
        lix_id: &str,
        default_branch_id: &str,
        branches: &[SyncBranchHead],
        head_commits: &[SyncCommit],
        commit_headers: &[SyncCommitHeader],
        rows: &[SyncSnapshotRow],
        checkpoint_roots: &BTreeMap<String, String>,
    ) -> Result<InitialSyncSnapshotInstall, LixError> {
        super::validate_sync_remote_id(remote_id)?;
        let mut parsed_heads = BTreeMap::new();
        for commit in head_commits {
            let commit = ParsedCommit::parse(commit)?;
            if parsed_heads.insert(commit.commit_id, commit).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot contains duplicate commit bodies",
                ));
            }
        }
        let parsed_rows = rows
            .iter()
            .map(parse_snapshot_row)
            .collect::<Result<Vec<_>, _>>()?;
        let default_branch_row_pk = RowPk::single(crate::init::DEFAULT_BRANCH_KEY);
        let lix_id_row_pk = RowPk::single(crate::init::LIX_ID_KEY);
        let mut tracked_lix_ids = parsed_rows
            .iter()
            .filter(|row| {
                row.branch_id == crate::GLOBAL_BRANCH_ID
                    && row.schema_key == "lix_key_value"
                    && row.file_id.is_none()
                    && row.row_pk == lix_id_row_pk
            })
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.snapshot_json)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("value")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    })
            });
        if tracked_lix_ids.next().flatten().as_deref() != Some(lix_id)
            || tracked_lix_ids.next().is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot lixId disagrees with its canonical tracked row",
            ));
        }
        let mut tracked_default_branch_ids = parsed_rows
            .iter()
            .filter(|row| {
                row.branch_id == crate::GLOBAL_BRANCH_ID
                    && row.schema_key == "lix_key_value"
                    && row.file_id.is_none()
                    && row.row_pk == default_branch_row_pk
            })
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.snapshot_json)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("value")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    })
            });
        if tracked_default_branch_ids.next().flatten().as_deref() != Some(default_branch_id)
            || tracked_default_branch_ids.next().is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot defaultBranchId disagrees with its canonical tracked row",
            ));
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        match inspect_sync_replica_binding(&read).await? {
            SyncReplicaBinding::Unbound => {}
            SyncReplicaBinding::Bound { .. } => {
                return Ok(InitialSyncSnapshotInstall::ExistingRepository);
            }
            SyncReplicaBinding::Ambiguous => {
                return Ok(InitialSyncSnapshotInstall::Ambiguous);
            }
        }
        let branch_ids = branches
            .iter()
            .map(|branch| branch.branch_id.clone())
            .collect::<Vec<_>>();
        if branch_ids.iter().collect::<BTreeSet<_>>().len() != branch_ids.len() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot contains duplicate branch ids",
            ));
        }
        if branches
            .iter()
            .filter(|branch| branch.branch_id == default_branch_id)
            .count()
            != 1
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot must contain exactly one headed default branch",
            ));
        }
        let observed = BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&branch_ids)
            .await?;
        let local_global_head = BranchHeadControlContext::new()
            .reader(&read)
            .load(crate::GLOBAL_BRANCH_ID)
            .await?
            .map(|control| control.head_commit_id);
        for (branch, observation) in branches.iter().zip(&observed) {
            let incoming_head = branch
                .head_commit_id
                .as_deref()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync snapshot branch has no head",
                    )
                })
                .and_then(|head| CommitId::parse_lix(head, "sync snapshot branch head"))?;
            let Some(local) = observation.control else {
                continue;
            };
            if local.head_commit_id == incoming_head {
                continue;
            }
            let record = load_commit_record(&read, local.head_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "local sync branch '{}' lost head commit '{}'",
                            branch.branch_id, local.head_commit_id
                        ),
                    )
                })?;
            let pristine_topology = if branch.branch_id == crate::GLOBAL_BRANCH_ID {
                record.parent_commit_ids.is_empty() && record.base_commit_id.is_none()
            } else {
                record.base_commit_id == local_global_head
                    && record
                        .base_commit_id
                        .is_some_and(|base| record.parent_commit_ids.as_slice() == [base])
            };
            let pristine_initialization = pristine_topology
                && record.account_id == crate::SYSTEM_ACCOUNT_ID
                && local.head_commit_id == local.tracked_generation
                && local.working_diff_checkpoint_commit_id == Some(local.head_commit_id)
                && local.current_state_revision == 0
                && local.created_at == local.updated_at;
            if !pristine_initialization {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    format!(
                        "sync snapshot cannot replace locally advanced branch '{}' at '{}'",
                        branch.branch_id, local.head_commit_id
                    ),
                ));
            }
        }
        let mut records = BTreeMap::<CommitId, CommitRecord>::new();
        let mut header_by_id = BTreeMap::new();
        for header in commit_headers {
            let parsed = ParsedSyncHeader::parse(header)?;
            if header_by_id.insert(parsed.commit_id, parsed).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot contains duplicate commit headers",
                ));
            }
        }
        validate_sync_header_set(&header_by_id, "sync snapshot")?;
        let mut appended_records = Vec::with_capacity(header_by_id.len());
        for header in header_by_id.values() {
            if let Some(existing) = load_commit_record(&read, header.commit_id).await? {
                if existing != header.record() {
                    return Err(immutable_object_mismatch("commit", header.commit_id));
                }
                let existing_scope =
                    match load_published_commit_state_topology(&read, header.commit_id).await? {
                        Some(topology) => Some(topology.global_scope()),
                        None => deferred_commit_global_scope(&read, header.commit_id).await?,
                    };
                if existing_scope.is_some_and(|scope| scope != header.global_scope) {
                    return Err(immutable_object_mismatch("commit", header.commit_id));
                }
                records.insert(header.commit_id, existing);
            } else {
                let record = header.record();
                records.insert(header.commit_id, record.clone());
                appended_records.push(record);
            }
        }
        for (commit_id, commit) in &parsed_heads {
            let header = header_by_id.get(commit_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync snapshot head '{commit_id}' has no certified header"),
                )
            })?;
            if header.parent_commit_ids != commit.parent_commit_ids
                || header.base_commit_id != commit.base_commit_id
                || header.account_id != commit.account_id
                || header.created_at != commit.created_at
                || header.global_scope != commit.wire.global_scope
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync snapshot head '{commit_id}' body disagrees with its header"),
                ));
            }
        }
        let mut head_ids = BTreeSet::new();
        let mut checkpoint_ids = BTreeSet::new();
        for branch in branches {
            let head = branch
                .head_commit_id
                .as_deref()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync snapshot branch has no head",
                    )
                })
                .and_then(|head| CommitId::parse_lix(head, "sync snapshot branch head"))?;
            if !parsed_heads.contains_key(&head) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync snapshot head '{head}' has no exact commit body"),
                ));
            }
            head_ids.insert(head);
            checkpoint_ids.insert(CommitId::parse_lix(
                branch
                    .checkpoint_commit_id
                    .as_deref()
                    .expect("validated checkpoint"),
                "sync snapshot branch checkpoint",
            )?);
        }
        let mut snapshot_body_ids = head_ids
            .union(&checkpoint_ids)
            .copied()
            .collect::<BTreeSet<_>>();
        // A local head or checkpoint is not a complete state authority
        // without its pinned global base, so the snapshot ships that base's
        // body inline. Accept exactly the bases the accepted bodies name;
        // bases are global commits and carry no base of their own.
        let inline_base_ids = snapshot_body_ids
            .iter()
            .filter_map(|commit_id| parsed_heads.get(commit_id))
            .filter_map(|commit| commit.base_commit_id)
            .collect::<BTreeSet<_>>();
        snapshot_body_ids.extend(inline_base_ids);
        let required_checkpoint_roots = branches
            .iter()
            .filter_map(|branch| {
                let head = branch.head_commit_id.as_deref()?;
                let checkpoint = branch.checkpoint_commit_id.as_deref()?;
                (head != checkpoint).then_some(checkpoint.to_owned())
            })
            .collect::<BTreeSet<_>>();
        if checkpoint_roots.keys().cloned().collect::<BTreeSet<_>>() != required_checkpoint_roots {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync snapshot checkpoint roots do not match its branch coordinates",
            ));
        }
        for body_id in parsed_heads.keys() {
            if !snapshot_body_ids.contains(body_id) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync snapshot body '{body_id}' is neither a branch head nor checkpoint"
                    ),
                ));
            }
        }

        let advertised_branches = branch_ids.iter().collect::<BTreeSet<_>>();
        let mut row_coordinates = BTreeSet::new();
        for row in &parsed_rows {
            let checkpoint_row_owner =
                CommitId::parse_lix(&row.branch_id, "sync snapshot checkpoint row owner")
                    .ok()
                    .is_some_and(|commit_id| checkpoint_ids.contains(&commit_id));
            if !advertised_branches.contains(&row.branch_id) && !checkpoint_row_owner {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync snapshot row references unadvertised branch '{}'",
                        row.branch_id
                    ),
                ));
            }
            if !row_coordinates.insert((
                row.branch_id.clone(),
                row.schema_key.clone(),
                row.file_id.clone(),
                row.row_pk.clone(),
            )) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot repeats a row coordinate",
                ));
            }
        }
        // A page sequence is complete only if its reconstructed canonical root
        // equals the head certificate. This turns an omitted/truncated page
        // into an atomic bootstrap failure instead of silently deleting rows.
        for branch in branches {
            let head = CommitId::parse_lix(
                branch.head_commit_id.as_deref().expect("validated head"),
                "sync snapshot branch head",
            )?;
            let expected_root = parse_sync_live_value_root_id(&branch.hot_state_root_id)?;
            let actual_root = sync_live_value_root_from_snapshot_rows(
                parsed_rows
                    .iter()
                    .filter(|row| row.branch_id == branch.branch_id),
            )?;
            if actual_root != expected_root {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync snapshot rows for branch '{}' do not match head '{}' stateRootId",
                        branch.branch_id, head
                    ),
                ));
            }
        }
        for (checkpoint_id, root_id) in checkpoint_roots {
            CommitId::parse_lix(checkpoint_id, "sync snapshot checkpoint root")?;
            let expected_root = parse_sync_live_value_root_id(root_id)?;
            let actual_root = sync_live_value_root_from_snapshot_rows(
                parsed_rows
                    .iter()
                    .filter(|row| row.branch_id == *checkpoint_id),
            )?;
            if actual_root != expected_root {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync snapshot rows for checkpoint '{checkpoint_id}' do not match its liveStateRootId"
                    ),
                ));
            }
        }

        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        for commit_id in header_by_id.keys().copied() {
            if parsed_heads.contains_key(&commit_id) {
                stage_commit_history_available(&mut writes, commit_id);
            } else {
                stage_commit_history_deferred_with_scope(
                    &mut writes,
                    commit_id,
                    header_by_id[&commit_id].global_scope,
                );
            }
        }
        for (commit_id, commit) in &parsed_heads {
            if let Some(alias) = &commit.wire.state_alias {
                stage_materialized_sync_state_alias(&mut writes, *commit_id, alias)?;
            }
        }
        let mut changes = BTreeMap::<ChangeId, ChangeRecord>::new();
        for row in &parsed_rows {
            let change = row.change_record();
            if changes
                .insert(change.change_id, change.clone())
                .is_some_and(|existing| existing != change)
            {
                return Err(immutable_object_mismatch("change", change.change_id));
            }
        }
        for branch in branches {
            let head = CommitId::parse_lix(
                branch.head_commit_id.as_deref().expect("validated head"),
                "sync snapshot branch head",
            )?;
            let record = &records[&head];
            let change = sync_ref_change_record(
                &branch.branch_id,
                head,
                &record.account_id,
                record.created_at,
            )?;
            if changes
                .insert(change.change_id, change.clone())
                .is_some_and(|existing| existing != change)
            {
                return Err(immutable_object_mismatch(
                    "branch ref change",
                    change.change_id,
                ));
            }
        }
        let mut head_mutations = BTreeMap::new();
        // A sparse bootstrap can include a checkpoint/head selection while the
        // commit that originally authored one of its changes remains deferred.
        // The selected payload is self-contained, so let it temporarily own
        // the canonical locator. If an authored body is present in the same
        // snapshot it wins; later history hydration replaces the fallback with
        // the original authored locator.
        let mut selected_fallback_locators = BTreeMap::new();
        let mut authored_locators = BTreeMap::new();
        let mut imported_authored_change_ids = BTreeSet::new();
        for commit in parsed_heads
            .iter()
            .filter(|(commit_id, _)| snapshot_body_ids.contains(commit_id))
            .map(|(_, commit)| commit)
        {
            let mutations = stage_imported_commit_body(
                &mut writes,
                commit,
                &mut imported_authored_change_ids,
                &mut selected_fallback_locators,
                &mut authored_locators,
            )?;
            for member in &commit.members {
                let change = member.change_record();
                if changes
                    .insert(change.change_id, change.clone())
                    .is_some_and(|existing| existing != change)
                {
                    return Err(immutable_object_mismatch("change", change.change_id));
                }
            }
            head_mutations.insert(commit.commit_id, mutations);
        }
        selected_fallback_locators
            .retain(|change_id, _| !imported_authored_change_ids.contains(change_id));
        stage_change_locators(
            &mut writes,
            &selected_fallback_locators.into_values().collect::<Vec<_>>(),
        );
        stage_change_locators(
            &mut writes,
            &authored_locators.into_values().collect::<Vec<_>>(),
        );
        ChangelogContext::new()
            .writer(&mut &read, &mut writes)
            .stage_certified_sparse_append(ChangelogAppend {
                commits: appended_records,
                changes: changes.into_values().collect(),
            })
            .await?;

        let mut row_pk_index_overlay = TrackedStateChunkOverlay::new();
        for head in snapshot_body_ids.iter().copied() {
            // A sparse snapshot may advertise a checkpoint coordinate whose
            // header/body is intentionally deferred. Its rows still seed the
            // hot checkpoint baseline; immutable state authority is installed
            // later through the structured history-demand path.
            let (Some(record), Some(header)) = (records.get(&head), header_by_id.get(&head)) else {
                continue;
            };
            let row_owner = branches
                .iter()
                .find(|branch| branch.head_commit_id.as_deref() == Some(head.to_string().as_str()))
                .map(|branch| branch.branch_id.clone())
                .unwrap_or_else(|| head.to_string());
            let head_rows = parsed_rows
                .iter()
                .filter(|row| row.branch_id == row_owner)
                .collect::<Vec<_>>();
            let root_deltas = head_rows
                .iter()
                .map(|row| row.as_root_delta())
                .collect::<Vec<_>>();
            let tracked_context = TrackedStateContext::new();
            let mut tracked_writer = tracked_context.writer(&read, &mut writes);
            tracked_writer
                .stage_commit_root(&head.to_string(), None, root_deltas)
                .await?;
            let mut snapshot_root = tracked_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == head)
                .cloned()
                .ok_or_else(|| LixError::unknown("sync snapshot did not stage its root"))?;
            drop(tracked_writer);
            snapshot_root.changed_key_count = u64::try_from(
                parsed_heads
                    .get(&head)
                    .map_or(head_rows.len(), |commit| commit.members.len()),
            )
            .map_err(|_| LixError::unknown("sync head mutation count exceeds u64"))?;
            snapshot_root.complete_state_fence = true;
            let row_pk_index_root_id = stage_row_pk_index_from_deltas(
                &read,
                &mut writes,
                &mut row_pk_index_overlay,
                head_rows.iter().map(|row| row.as_root_delta()),
                head,
            )
            .await?;
            let mutations = head_mutations.remove(&head).unwrap_or_default();
            stage_commit_state_manifest_with_handle(
                &mut writes,
                &CommitStateManifest {
                    commit_id: head,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations,
                    touched_scope_filter: incomplete_touched_scope_filter(),
                    global_scope: header.global_scope,
                    current_state_scoped_ranges: None,
                    row_pk_index_root_id,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )?;
        }

        for ((branch, observation), head) in
            branches
                .iter()
                .zip(observed)
                .zip(branches.iter().map(|branch| {
                    CommitId::parse_lix(
                        branch.head_commit_id.as_deref().expect("validated head"),
                        "sync snapshot branch head",
                    )
                    .expect("validated head")
                }))
        {
            preconditions.push(branch_head_control_precondition(
                &branch.branch_id,
                observation.raw_token,
            )?);
            let branch_rows = parsed_rows
                .iter()
                .filter(|row| row.branch_id == branch.branch_id)
                .collect::<Vec<_>>();
            let checkpoint = CommitId::parse_lix(
                branch
                    .checkpoint_commit_id
                    .as_deref()
                    .expect("validated checkpoint"),
                "sync snapshot branch checkpoint",
            )?;
            let checkpoint_rows = if checkpoint == head {
                branch_rows.clone()
            } else {
                parsed_rows
                    .iter()
                    .filter(|row| row.branch_id == checkpoint)
                    .collect::<Vec<_>>()
            };
            let current_rows =
                snapshot_rows_with_inherited_catalog(&branch.branch_id, &parsed_rows, &branch_rows);
            let checkpoint_rows = snapshot_rows_with_inherited_catalog(
                &branch.branch_id,
                &parsed_rows,
                &checkpoint_rows,
            );
            let current_snapshot =
                snapshot_rows_hot_snapshot(&branch.branch_id, current_rows.into_values())?;
            let checkpoint_snapshot =
                snapshot_rows_hot_snapshot(&branch.branch_id, checkpoint_rows.into_values())?;
            let mut coverage = WorkingDiffIndexCoverage::default();
            let (_, schemas) = TrackedHeadContext::new()
                .writer(&read, &mut writes)
                .stage_complete_current_state_with_working_diff(
                    &branch.branch_id,
                    head,
                    current_snapshot,
                    observation
                        .control
                        .map(|control| control.tracked_generation),
                    &[],
                    &[],
                    &BTreeSet::new(),
                    if checkpoint == head {
                        CompleteWorkingDiffMode::ResetClean
                    } else {
                        CompleteWorkingDiffMode::Rebase {
                            checkpoint_commit_id: checkpoint,
                            checkpoint: checkpoint_snapshot,
                        }
                    },
                    &mut coverage,
                )
                .await?;
            stage_tracked_working_diff_epoch(
                &mut writes,
                &branch.branch_id,
                TrackedWorkingDiffEpoch {
                    checkpoint_commit_id: checkpoint,
                    generation: head,
                    coverage,
                },
            )?;
            let record = &records[&head];
            let mut control = BranchHeadControl {
                head_commit_id: head,
                tracked_generation: head,
                current_state_revision: observation.control.map_or(0, |control| {
                    control.current_state_revision.saturating_add(1)
                }),
                working_diff_checkpoint_commit_id: Some(checkpoint),
                created_at: observation
                    .control
                    .map_or(record.created_at, |control| control.created_at),
                updated_at: record.created_at,
                ref_change_id: sync_ref_change_id(&branch.branch_id, Some(head)),
                schema_presence_bloom: [0; 4],
            };
            control.note_schemas(schemas.iter().map(String::as_str));
            stage_branch_head_control(&mut writes, &branch.branch_id, control)?;
        }
        let authoritative_branches = branches
            .iter()
            .map(|branch| {
                Ok((
                    branch.branch_id.clone(),
                    AuthoritativeBranchCoordinate::from_wire(
                        branch.head_commit_id.clone(),
                        branch.checkpoint_commit_id.clone(),
                        "sync snapshot branch",
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LixError>>()?;
        let certified_branch_roots = branches
            .iter()
            .map(|branch| {
                (
                    branch.branch_id.clone(),
                    CertifiedBranchRoots {
                        head_state_root_id: branch.hot_state_root_id.clone(),
                        checkpoint_state_root_id: branch.checkpoint_state_root_id.clone(),
                    },
                )
            })
            .collect();
        stage_replica_state(
            &mut writes,
            &mut preconditions,
            &SyncReplicaState {
                active_account_id: active_account_id.to_owned(),
                cursor,
                authoritative_branches,
                certified_branch_roots,
                snapshot_certified_cursor: Some(cursor),
                pending_resets: BTreeMap::new(),
                authority_known_commit_ids: BTreeSet::new(),
            },
            None,
        )?;
        preconditions.push(StoragePrecondition::RangeEmpty {
            space: SYNC_REPLICA_STATE_SPACE,
            range: StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()?,
        });
        crate::json_store::stage_json_publication_fence(&read, &mut writes, &mut preconditions)
            .await?;
        drop(read);
        adapter
            .commit_certified_replica_write_set(
                super::certified_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        self.notify_observers_for_sync();
        self.sync_mode_state().notify_sync_change();
        Ok(InitialSyncSnapshotInstall::Installed)
    }

    pub(crate) async fn push_sync_repository(
        &self,
        request: &SyncPushRequest,
    ) -> Result<SyncPushResponse, LixError> {
        Box::pin(self.import_sync_repository(request, SyncImportPurpose::AuthorityPush, None, None))
            .await
    }

    pub(crate) async fn import_sync_history_boundaries(
        &self,
        commits: &[SyncCommit],
        boundaries: &[SyncHistoryBoundary],
        rows: &[SyncSnapshotRow],
    ) -> Result<(), LixError> {
        Box::pin(self.import_sync_repository(
            &SyncPushRequest {
                commits: commits.to_vec(),
                ref_updates: Vec::new(),
                inline_blobs: Vec::new(),
            },
            SyncImportPurpose::History,
            Some((boundaries, rows)),
            None,
        ))
        .await?;
        Ok(())
    }

    pub(crate) async fn import_sync_history_headers(
        &self,
        headers: &[SyncCommitHeader],
    ) -> Result<(), LixError> {
        let mut parsed = BTreeMap::new();
        for header in headers {
            let header = ParsedSyncHeader::parse(header)?;
            if parsed.insert(header.commit_id, header).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync history contains duplicate commit headers",
                ));
            }
        }
        validate_sync_header_set(&parsed, "sync history")?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut new_records = Vec::new();
        let mut writes = adapter.new_write_set();
        for header in parsed.values() {
            if let Some(existing) = load_commit_record(&read, header.commit_id).await? {
                if existing != header.record() {
                    return Err(immutable_object_mismatch("commit", header.commit_id));
                }
                let existing_scope =
                    match load_published_commit_state_topology(&read, header.commit_id).await? {
                        Some(topology) => Some(topology.global_scope()),
                        None => deferred_commit_global_scope(&read, header.commit_id).await?,
                    };
                if existing_scope.is_some_and(|scope| scope != header.global_scope) {
                    return Err(immutable_object_mismatch("commit", header.commit_id));
                }
            } else {
                new_records.push(header.record());
                stage_commit_history_deferred_with_scope(
                    &mut writes,
                    header.commit_id,
                    header.global_scope,
                );
            }
        }
        if new_records.is_empty() {
            return Ok(());
        }
        ChangelogContext::new()
            .writer(&mut &read, &mut writes)
            .stage_certified_sparse_append(ChangelogAppend {
                commits: new_records,
                changes: Vec::new(),
            })
            .await?;
        let mut preconditions = Vec::new();
        crate::json_store::stage_json_publication_fence(&read, &mut writes, &mut preconditions)
            .await?;
        drop(read);
        adapter
            .commit_certified_replica_write_set(
                super::certified_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn import_sync_repository(
        &self,
        request: &SyncPushRequest,
        purpose: SyncImportPurpose,
        history_boundaries: Option<(&[SyncHistoryBoundary], &[SyncSnapshotRow])>,
        replica_publication: Option<ReplicaStatePublication<'_>>,
    ) -> Result<SyncPushResponse, LixError> {
        // Sync imports publish the same repository state that foreground
        // auto-commit and checkpoint transactions read. Serialize at this
        // direct writer boundary so a read that has entered its quiescent
        // retry cannot be expired repeatedly by the sync worker. Do not hold
        // this guard across `apply_sync_repository_pull`: divergent branch
        // reconciliation deliberately returns to the ordinary transaction
        // machinery, which acquires this gate for its merge commit.
        let _collaboration_guard = self.lock_collaboration_writes().await;
        if purpose == SyncImportPurpose::History && !request.ref_updates.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync history import cannot update refs",
            ));
        }
        let referenced_blob_ids = sync_commit_blob_ids(&request.commits)?;
        let mut inline_blob_ids = BTreeSet::new();
        for manifest in &request.inline_blobs {
            if !inline_blob_ids.insert(manifest.blob_id.clone()) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync request repeats inline blob '{}'", manifest.blob_id),
                ));
            }
            if !referenced_blob_ids.contains(&manifest.blob_id) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync request includes unreferenced inline blob '{}'",
                        manifest.blob_id
                    ),
                ));
            }
            if manifest.inline_bytes_base64.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync hot-path blob '{}' has no inline payload",
                        manifest.blob_id
                    ),
                ));
            }
        }
        let mut parsed = BTreeMap::new();
        for wire in &request.commits {
            let commit = ParsedCommit::parse(wire)?;
            if parsed.insert(commit.commit_id, commit).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync push contains duplicate commit ids",
                ));
            }
        }
        let mut boundary_roots = BTreeMap::new();
        let mut boundary_rows = BTreeMap::<CommitId, Vec<ParsedSnapshotRow>>::new();
        let mut boundary_coordinates = BTreeSet::new();
        if let Some((boundaries, rows)) = history_boundaries {
            if purpose != SyncImportPurpose::History {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot boundaries are only valid for history import",
                ));
            }
            for boundary in boundaries {
                let commit_id =
                    CommitId::parse_lix(&boundary.commit_id, "sync history boundary commit id")?;
                let root = parse_sync_state_root_id(&boundary.live_state_root_id)?;
                if boundary_roots.insert(commit_id, root).is_some() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync history contains duplicate boundaries",
                    ));
                }
            }
            for commit_id in boundary_roots.keys() {
                if !parsed.contains_key(commit_id) {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("sync history boundary '{commit_id}' has no canonical commit body"),
                    ));
                }
                boundary_rows.insert(*commit_id, Vec::new());
            }
            for commit in parsed.values() {
                let external_parent = commit
                    .parent_commit_ids
                    .iter()
                    .any(|parent| !parsed.contains_key(parent));
                if external_parent && !boundary_roots.contains_key(&commit.commit_id) {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "sync history commit '{}' has external topology but no boundary",
                            commit.commit_id
                        ),
                    ));
                }
            }
            for row in rows {
                let commit_id =
                    CommitId::parse_lix(&row.branch_id, "sync history boundary snapshot branch")?;
                let parsed_row = parse_snapshot_row(row)?;
                if !boundary_coordinates.insert((
                    commit_id,
                    parsed_row.schema_key.clone(),
                    parsed_row.file_id.clone(),
                    parsed_row.row_pk.clone(),
                )) {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync history boundary repeats a row coordinate",
                    ));
                }
                boundary_rows
                    .get_mut(&commit_id)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!(
                                "sync history snapshot rows reference unrelated boundary '{commit_id}'"
                            ),
                        )
                    })?
                    .push(parsed_row);
            }
        }
        let mut parsed_refs = Vec::with_capacity(request.ref_updates.len());
        let mut branch_ids = Vec::with_capacity(request.ref_updates.len());
        let mut seen_branches = BTreeSet::new();
        for update in &request.ref_updates {
            RowPk::uuid_from_canonical(&update.branch_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync ref branchId is invalid: {error}"),
                )
            })?;
            if !seen_branches.insert(update.branch_id.clone()) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync push contains duplicate branch ref updates",
                ));
            }
            let expected = update
                .expected_head_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync expected ref head"))
                .transpose()?;
            let expected_checkpoint = update
                .expected_checkpoint_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync expected checkpoint"))
                .transpose()?;
            let head = update
                .head_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync ref head"))
                .transpose()?;
            let checkpoint = update
                .checkpoint_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync ref checkpoint"))
                .transpose()?;
            if expected.is_some() != expected_checkpoint.is_some()
                || head.is_some() != checkpoint.is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync ref head and checkpoint coordinates must be paired",
                ));
            }
            branch_ids.push(update.branch_id.clone());
            parsed_refs.push((
                update.clone(),
                expected,
                expected_checkpoint,
                head,
                checkpoint,
            ));
        }

        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let default_branch_id = self.repository_default_branch_id_for_sync(&read).await?;
        if parsed_refs.iter().any(|(update, _, _, head, _)| {
            (update.branch_id == default_branch_id || update.branch_id == crate::GLOBAL_BRANCH_ID)
                && head.is_none()
        }) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync push cannot delete the repository default or global branch",
            ));
        }
        let observations = BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&branch_ids)
            .await?;
        let mut changed_refs = Vec::new();
        for ((update, expected, expected_checkpoint, head, checkpoint), observation) in
            parsed_refs.iter().zip(&observations)
        {
            let current = observation.control.map(|control| control.head_commit_id);
            let current_checkpoint = observation
                .control
                .and_then(|control| control.working_diff_checkpoint_commit_id);
            if current == *head && current_checkpoint == *checkpoint {
                continue;
            }
            if current != *expected || current_checkpoint != *expected_checkpoint {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    format!(
                        "sync ref '{}' expected coordinate ({:?}, {:?}), found ({:?}, {:?})",
                        update.branch_id,
                        expected.map(|id| id.to_string()),
                        expected_checkpoint.map(|id| id.to_string()),
                        current.map(|id| id.to_string()),
                        current_checkpoint.map(|id| id.to_string()),
                    ),
                ));
            }
            changed_refs.push((update.clone(), *head, *checkpoint));
        }

        // Ref CAS is the cheap, side-effect-free gate. Authority admission
        // requires complete local blob content; replica/history import only
        // requires the manifest registered by the transport's lazy-CAS lane.
        for commit in parsed.values() {
            for member in commit
                .members
                .iter()
                .filter(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
            {
                let blob_hash = member
                    .snapshot_json
                    .as_deref()
                    .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
                    .and_then(|snapshot| {
                        snapshot
                            .get("blob_hash")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    })
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "live sync binary blob ref has no blob_hash",
                        )
                    })?;
                let available = inline_blob_ids.contains(&blob_hash)
                    || match purpose {
                        SyncImportPurpose::AuthorityPush => self
                            .get_sync_blob_manifest_with_collaboration_guard(&blob_hash)
                            .await?
                            .is_some(),
                        SyncImportPurpose::ReplicaDelta | SyncImportPurpose::History => {
                            self.has_sync_blob_manifest(&blob_hash).await?
                        }
                    };
                if !available {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("sync commit references unavailable binary blob '{blob_hash}'"),
                    ));
                }
            }
        }

        let mut existing = BTreeSet::new();
        let mut deferred_existing = BTreeSet::new();
        let mut records = BTreeMap::<CommitId, CommitRecord>::new();
        let mut published_topologies = BTreeMap::new();
        for (commit_id, commit) in &parsed {
            match load_sync_commit(&read, *commit_id).await {
                Ok(Some(stored)) => {
                    let materialized_external_boundary =
                        boundary_rows.contains_key(commit_id) && commit.members.is_empty();
                    if stored != commit.wire && !materialized_external_boundary {
                        return Err(immutable_object_mismatch("commit", commit_id));
                    }
                    existing.insert(*commit_id);
                    records.insert(
                        *commit_id,
                        load_commit_record(&read, *commit_id)
                            .await?
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!("existing sync commit '{commit_id}' lost its record"),
                                )
                            })?,
                    );
                    published_topologies.insert(
                        *commit_id,
                        load_published_commit_state_topology(&read, *commit_id)
                            .await?
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "existing sync commit '{commit_id}' has no state authority"
                                    ),
                                )
                            })?,
                    );
                }
                Ok(None) => {}
                Err(error) if error.code == "LIX_SYNC_HISTORY_REQUIRED" => {
                    let record = load_commit_record(&read, *commit_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("deferred sync commit '{commit_id}' lost its header"),
                            )
                        })?;
                    if record.parent_commit_ids != commit.parent_commit_ids
                        || record.base_commit_id != commit.base_commit_id
                        || record.account_id != commit.account_id
                        || record.created_at != commit.created_at
                        || deferred_commit_global_scope(&read, *commit_id)
                            .await?
                            .is_some_and(|scope| scope != commit.wire.global_scope)
                    {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!(
                                "deferred sync commit '{commit_id}' body disagrees with its header"
                            ),
                        ));
                    }
                    records.insert(*commit_id, record);
                    deferred_existing.insert(*commit_id);
                }
                Err(error) => return Err(error),
            }
        }
        if purpose == SyncImportPurpose::History
            && parsed.keys().any(|commit_id| {
                !existing.contains(commit_id) && !deferred_existing.contains(commit_id)
            })
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync history hydration can only fill known deferred commit headers",
            ));
        }

        // Resolve every dependency outside this push before constructing the
        // write set. Internal dependencies are supplied by the staged maps.
        let dependencies = parsed
            .iter()
            .flat_map(|(commit_id, commit)| {
                commit
                    .parent_commit_ids
                    .iter()
                    .copied()
                    .chain(commit.base_commit_id)
                    .chain(
                        (!boundary_rows.contains_key(commit_id))
                            .then_some(commit.state_alias.as_ref())
                            .flatten()
                            .map(|(source, _)| *source),
                    )
            })
            .filter(|dependency| !parsed.contains_key(dependency))
            .collect::<BTreeSet<_>>();
        let required_external_topologies = parsed
            .iter()
            .filter(|(commit_id, _)| !boundary_rows.contains_key(commit_id))
            .flat_map(|(_, commit)| commit.dependencies())
            .filter(|dependency| !parsed.contains_key(dependency))
            .collect::<BTreeSet<_>>();
        for dependency in dependencies {
            records.insert(
                dependency,
                load_commit_record(&read, dependency)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_COMMIT_NOT_FOUND,
                            format!("sync commit dependency '{dependency}' does not exist"),
                        )
                    })?,
            );
            if required_external_topologies.contains(&dependency) {
                published_topologies.insert(
                    dependency,
                    load_published_commit_state_topology(&read, dependency)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("sync dependency '{dependency}' has no state authority"),
                            )
                        })?,
                );
            }
        }
        for commit in parsed.values() {
            let Some(base_commit_id) = commit.base_commit_id else {
                continue;
            };
            let base_is_global = parsed
                .get(&base_commit_id)
                .map(|base| base.wire.global_scope)
                .or_else(|| {
                    published_topologies
                        .get(&base_commit_id)
                        .map(|topology| topology.global_scope())
                })
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_COMMIT_NOT_FOUND,
                        format!("sync base commit '{base_commit_id}' has no state authority"),
                    )
                })?;
            if !base_is_global {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync base commit '{base_commit_id}' is not global"),
                ));
            }
        }
        for (_, _, _, head, checkpoint) in &parsed_refs {
            if let Some(head) = head
                && !parsed.contains_key(head)
                && !records.contains_key(head)
            {
                records.insert(
                    *head,
                    load_commit_record(&read, *head).await?.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_COMMIT_NOT_FOUND,
                            format!("sync ref target '{head}' does not exist"),
                        )
                    })?,
                );
            }
            if let Some(checkpoint) = checkpoint
                && !parsed.contains_key(checkpoint)
                && !records.contains_key(checkpoint)
            {
                records.insert(
                    *checkpoint,
                    load_commit_record(&read, *checkpoint)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_COMMIT_NOT_FOUND,
                                format!("sync ref checkpoint '{checkpoint}' does not exist"),
                            )
                        })?,
                );
            }
        }
        for (update, _, _, head, checkpoint) in &parsed_refs {
            if update.branch_id != crate::GLOBAL_BRANCH_ID {
                continue;
            }
            for target in [head, checkpoint].into_iter().flatten() {
                let (global_scope, base_commit_id) = if let Some(commit) = parsed.get(target) {
                    (commit.wire.global_scope, commit.base_commit_id)
                } else {
                    let record = records.get(target).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_COMMIT_NOT_FOUND,
                            format!("sync global ref target '{target}' does not exist"),
                        )
                    })?;
                    let global_scope = load_published_commit_state_topology(&read, *target)
                        .await?
                        .map(|topology| topology.global_scope())
                        .or(deferred_commit_global_scope(&read, *target).await?)
                        .unwrap_or(false);
                    (global_scope, record.base_commit_id)
                };
                if !global_scope || base_commit_id.is_some() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "sync global branch target '{target}' is not a base-native global commit"
                        ),
                    ));
                }
            }
        }

        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        for (commit_id, commit) in &parsed {
            if !existing.contains(commit_id)
                && let Some(alias) = &commit.wire.state_alias
            {
                stage_materialized_sync_state_alias(&mut writes, *commit_id, alias)?;
            }
        }
        for manifest in &request.inline_blobs {
            super::blob::stage_inline_sync_blob(&mut writes, manifest)?;
        }
        if !request.inline_blobs.is_empty() {
            crate::binary_cas::stage_transfer_publication_fence(
                &read,
                &mut writes,
                &mut preconditions,
            )
            .await?;
        }
        for commit_id in parsed.keys().copied() {
            stage_commit_history_available(&mut writes, commit_id);
        }

        // Every protocol commit is a root fence. Build roots first with one
        // writer so children can use parents staged earlier in this request.
        // Ordinary and merge membership is the complete logical delta relative
        // to the first parent. A metadata-only checkpoint instead carries an
        // authenticated complete-state alias and no members; its semantic
        // parent and physical source are distinct dependency edges.
        let mut imported_roots = BTreeMap::new();
        let mut root_remaining = parsed
            .keys()
            .filter(|commit_id| !existing.contains(commit_id))
            .copied()
            .collect::<BTreeSet<_>>();
        let external_first_parents = root_remaining
            .iter()
            .filter(|commit_id| !boundary_rows.contains_key(commit_id))
            .filter_map(|commit_id| parsed[commit_id].parent_commit_ids.first().copied())
            .filter(|parent| !root_remaining.contains(parent))
            .collect::<BTreeSet<_>>();
        let mut materialized_parent_roots = BTreeMap::new();
        for parent in external_first_parents {
            let rooted = load_commit_state_manifest(&read, parent)
                .await?
                .is_some_and(|manifest| manifest.snapshot_root.is_some());
            if !rooted {
                let mut tracked = TrackedStateContext::new().reader(&read);
                let rows = tracked
                    .scan_batch_at_commit(&parent.to_string(), &TrackedStateScanRequest::default())
                    .await?;
                materialized_parent_roots.insert(parent, rows);
            }
        }
        let tracked_context = TrackedStateContext::new();
        let mut tracked_writer = tracked_context.writer(&read, &mut writes);
        for (parent, rows) in &materialized_parent_roots {
            let deltas = rows.iter().map(|row| TrackedStateDeltaRef {
                schema_key: row.schema_key(),
                file_id: row.file_id(),
                row_pk: row.row_pk(),
                change_id: row.change_id(),
                commit_id: row.commit_id(),
                deleted: row.deleted(),
                created_at: row.created_at(),
                updated_at: row.updated_at(),
            });
            tracked_writer
                .stage_commit_root(&parent.to_string(), None, deltas)
                .await?;
        }
        while !root_remaining.is_empty() {
            let ready = root_remaining.iter().copied().find(|commit_id| {
                if boundary_rows.contains_key(commit_id) {
                    return true;
                }
                let commit = &parsed[commit_id];
                let first_parent_ready = commit.parent_commit_ids.first().is_none_or(|parent| {
                    !root_remaining.contains(parent) || imported_roots.contains_key(parent)
                });
                let alias_source_ready = commit.state_alias.as_ref().is_none_or(|(source, _)| {
                    !root_remaining.contains(source) || imported_roots.contains_key(source)
                });
                first_parent_ready && alias_source_ready
            });
            let Some(commit_id) = ready else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync root dependency graph contains a cycle",
                ));
            };
            let commit = &parsed[&commit_id];
            let state_parent_commit_id =
                commit.parent_commit_ids.first().copied().filter(|parent| {
                    commit.wire.global_scope
                        || !parsed
                            .get(parent)
                            .map(|parent| parent.wire.global_scope)
                            .or_else(|| {
                                published_topologies
                                    .get(parent)
                                    .map(|topology| topology.global_scope())
                            })
                            .or_else(|| {
                                records
                                    .get(parent)
                                    .map(|record| record.base_commit_id.is_none())
                            })
                            .unwrap_or(false)
                });
            let parent = if boundary_rows.contains_key(&commit_id) {
                None
            } else {
                state_parent_commit_id.map(|parent| parent.to_string())
            };
            let root_deltas = if let Some(rows) = boundary_rows.get(&commit_id) {
                rows.iter()
                    .map(ParsedSnapshotRow::as_root_delta)
                    .collect::<Vec<_>>()
            } else {
                commit
                    .members
                    .iter()
                    .map(|member| member.as_root_delta(commit_id))
                    .collect::<Vec<_>>()
            };
            if boundary_rows.contains_key(&commit_id) {
                tracked_writer
                    .stage_commit_root(&commit_id.to_string(), None, root_deltas)
                    .await?;
            } else if let Some((source_commit_id, _)) = &commit.state_alias {
                tracked_writer
                    .stage_complete_state_alias(commit_id, *source_commit_id)
                    .await?;
            } else {
                tracked_writer
                    .stage_commit_root(&commit_id.to_string(), parent.as_deref(), root_deltas)
                    .await?;
            }
            let mut root = tracked_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == commit_id)
                .cloned()
                .ok_or_else(|| LixError::unknown("sync import did not stage its commit root"))?;
            if !boundary_rows.contains_key(&commit_id)
                && let Some((_, expected_root)) = &commit.state_alias
                && &root.root_id != expected_root
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync complete-state alias for '{commit_id}' does not match its source root"
                    ),
                ));
            }
            if commit.state_alias.is_none()
                && !boundary_rows.contains_key(&commit_id)
                && root.parent_roots.first().map(|root| root.commit_id) != state_parent_commit_id
            {
                return Err(LixError::unknown(format!(
                    "sync import root '{commit_id}' did not retain physical overlay parent '{state_parent_commit_id:?}'",
                )));
            }
            if boundary_rows.contains_key(&commit_id) {
                let expected_root = &boundary_roots[&commit_id];
                if &root.root_id != expected_root {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "sync history boundary rows for '{commit_id}' do not match liveStateRootId"
                        ),
                    ));
                }
                root.changed_key_count = u64::try_from(commit.members.len())
                    .map_err(|_| LixError::unknown("sync history mutation count exceeds u64"))?;
                root.complete_state_fence = true;
                debug_assert!(root.parent_roots.is_empty());
            }
            imported_roots.insert(commit_id, root);
            root_remaining.remove(&commit_id);
        }
        if purpose == SyncImportPurpose::AuthorityPush {
            let authored_by_change = parsed
                .values()
                .flat_map(|commit| commit.members.iter())
                .filter(|member| member.authored)
                .map(|member| (member.change_id, member))
                .collect::<BTreeMap<_, _>>();
            for commit in parsed.values() {
                let selected = commit
                    .members
                    .iter()
                    .filter(|member| !member.authored)
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    continue;
                }
                let source = if commit.parent_commit_ids.len() > 1 {
                    let source_id = commit.selected_source_commit_id.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!("sync merge '{}' has no selected source", commit.commit_id),
                        )
                    })?;
                    let keys = selected
                        .iter()
                        .map(|member| TrackedStateKey {
                            schema_key: member.schema_key.clone(),
                            file_id: member.file_id.clone(),
                            row_pk: member.row_pk.clone(),
                        })
                        .collect::<Vec<_>>();
                    let values =
                        if parsed.contains_key(&source_id) && !existing.contains(&source_id) {
                            tracked_writer
                                .root_values_at_commit(source_id, &keys)
                                .await?
                        } else {
                            TrackedStateContext::new()
                                .reader(&read)
                                .index_values_at_commit(&source_id.to_string(), &keys)
                                .await?
                        };
                    Some((source_id, values))
                } else {
                    None
                };
                for (index, member) in selected.iter().enumerate() {
                    if let Some((source_id, source_values)) = &source {
                        let source_value = source_values[index].as_ref().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!(
                                    "sync merge '{}' selects a row absent from source '{}'",
                                    commit.commit_id, source_id
                                ),
                            )
                        })?;
                        if source_value.change_id != member.change_id
                            || source_value.deleted != member.deleted
                            || source_value.created_at != member.row_created_at
                            || source_value.updated_at != member.row_updated_at
                        {
                            return Err(LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!(
                                    "sync merge '{}' selected row disagrees with source '{}'",
                                    commit.commit_id, source_id
                                ),
                            ));
                        }
                    }
                    // A non-merge checkpoint is a complete, source-less state
                    // selection. Its member payload is the protocol authority;
                    // requiring every selected change body to have arrived in
                    // an earlier push recreates an ordering-only side channel.
                    if source.is_none() {
                        continue;
                    }
                    if let Some(authored) = authored_by_change.get(&member.change_id) {
                        if !selected_payload_matches_authored(member, authored) {
                            return Err(LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!(
                                    "sync commit '{}' selected payload disagrees with its authored change",
                                    commit.commit_id
                                ),
                            ));
                        }
                        continue;
                    }
                    let change = load_existing_sync_change(&read, member.change_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!(
                                    "sync commit '{}' selected change '{}' is unavailable",
                                    commit.commit_id, member.change_id
                                ),
                            )
                        })?;
                    if change.account_id != member.change_account_id
                        || change.created_at != member.change_created_at
                        || change.origin_key != member.origin_key
                    {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!(
                                "sync commit '{}' selected change metadata disagrees with its source",
                                commit.commit_id
                            ),
                        ));
                    }
                    let (_, payload) = materialize_known_change_payloads_in_order(
                        std::iter::once(change),
                        ChangeRecordProjection::full(),
                    )?
                    .into_iter()
                    .next()
                    .expect("one selected change materializes once");
                    let identity = payload.identity.expect("full projection has identity");
                    if identity.schema_key != member.schema_key
                        || identity.file_id != member.file_id
                        || identity.row_pk != member.row_pk
                        || payload.snapshot_content.as_deref() != member.snapshot_json.as_deref()
                        || payload.metadata.as_deref() != member.metadata_json.as_deref()
                    {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!(
                                "sync commit '{}' selected payload disagrees with source change '{}'",
                                commit.commit_id, member.change_id
                            ),
                        ));
                    }
                }
            }
        }
        drop(tracked_writer);

        let mut staged_manifests = BTreeMap::<CommitId, StagedCommitStateManifest>::new();
        let mut row_pk_index_overlay = TrackedStateChunkOverlay::new();
        let mut appended_records = Vec::new();
        let mut appended_changes = BTreeMap::<ChangeId, ChangeRecord>::new();
        for row in boundary_rows.values().flatten() {
            let change = row.change_record();
            match load_existing_sync_change(&read, change.change_id).await? {
                Some(existing) if existing != change => {
                    return Err(immutable_object_mismatch("change", change.change_id));
                }
                Some(_) => {}
                None => match appended_changes.entry(change.change_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(change);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &change => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(immutable_object_mismatch("change", change.change_id));
                    }
                },
            }
        }
        let mut newly_imported = Vec::new();
        let mut selected_fallback_locators = BTreeMap::new();
        let mut authored_locators = BTreeMap::new();
        let mut imported_authored_change_ids = BTreeSet::new();
        let mut remaining = parsed
            .keys()
            .filter(|commit_id| !existing.contains(commit_id))
            .copied()
            .collect::<BTreeSet<_>>();
        while !remaining.is_empty() {
            let ready = remaining.iter().copied().find(|commit_id| {
                if boundary_rows.contains_key(commit_id) {
                    return true;
                }
                parsed[commit_id].dependencies().all(|dependency| {
                    if parsed.contains_key(&dependency) && !existing.contains(&dependency) {
                        staged_manifests.contains_key(&dependency)
                    } else {
                        records.contains_key(&dependency)
                    }
                })
            });
            let Some(commit_id) = ready else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync commit graph contains a cycle",
                ));
            };
            let commit = &parsed[&commit_id];
            // Commit membership comes only from the canonical body. Boundary
            // rows certify a complete state root but never redefine a delta.
            let mutations = stage_imported_commit_body(
                &mut writes,
                commit,
                &mut imported_authored_change_ids,
                &mut selected_fallback_locators,
                &mut authored_locators,
            )?;
            for member in &commit.members {
                let change = member.change_record();
                match load_existing_sync_change(&read, change.change_id).await? {
                    Some(existing) if !sync_change_records_equal(&existing, &change)? => {
                        return Err(immutable_object_mismatch("change", change.change_id));
                    }
                    Some(_) => {}
                    None => match appended_changes.entry(change.change_id) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(change);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &change => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(immutable_object_mismatch("change", change.change_id));
                        }
                    },
                }
            }
            let touched_scope_digest = if commit.state_alias.is_some() {
                CommitTouchedScopeDigest::opaque()
            } else {
                match commit_delta_member_scopes(commit_id, &mutations)? {
                    Some(scopes) => CommitTouchedScopeDigest::exact(scopes.iter()),
                    None => CommitTouchedScopeDigest::opaque(),
                }
            };
            let row_pk_base_commit_id = if boundary_rows.contains_key(&commit_id) {
                None
            } else {
                commit
                    .state_alias
                    .as_ref()
                    .map(|(source_commit_id, _)| *source_commit_id)
                    .or_else(|| {
                        commit.parent_commit_ids.first().copied().filter(|parent| {
                            commit.wire.global_scope
                                || !parsed
                                    .get(parent)
                                    .map(|parent| parent.wire.global_scope)
                                    .or_else(|| {
                                        published_topologies
                                            .get(parent)
                                            .map(|topology| topology.global_scope())
                                    })
                                    .or_else(|| {
                                        records
                                            .get(parent)
                                            .map(|record| record.base_commit_id.is_none())
                                    })
                                    .unwrap_or(false)
                        })
                    })
            };
            let row_pk_base_root = row_pk_base_commit_id.and_then(|base_id| {
                staged_manifests
                    .get(&base_id)
                    .and_then(|manifest| manifest.row_pk_index_root_id.as_ref())
                    .or_else(|| {
                        published_topologies
                            .get(&base_id)
                            .and_then(|manifest| manifest.row_pk_index_root_id())
                    })
            });
            if row_pk_base_commit_id.is_some() && row_pk_base_root.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync commit '{commit_id}' has no row-PK index for serving base '{}'",
                        row_pk_base_commit_id.expect("checked above")
                    ),
                ));
            }
            let row_pk_index_root_id = if let Some(rows) = boundary_rows.get(&commit_id) {
                stage_row_pk_index_from_deltas(
                    &read,
                    &mut writes,
                    &mut row_pk_index_overlay,
                    rows.iter().map(ParsedSnapshotRow::as_root_delta),
                    commit_id,
                )
                .await?
            } else {
                let staged_segments =
                    staged_commit_delta_segment_bytes(&writes, commit_id, &mutations)?;
                let row_pk_members = staged_commit_delta_members(
                    &read,
                    commit_id,
                    &commit.account_id,
                    &mutations,
                    staged_segments,
                )
                .await?;
                stage_row_pk_index_from_members(
                    &read,
                    &mut writes,
                    &mut row_pk_index_overlay,
                    row_pk_base_root,
                    &row_pk_members,
                    commit_id,
                )
                .await?
            };
            let staged_manifest = if boundary_rows.contains_key(&commit_id) {
                stage_commit_state_manifest_with_handle(
                    &mut writes,
                    &CommitStateManifest {
                        commit_id,
                        change_account_id: commit.account_id.clone(),
                        replay_debt: CommitStateReplayDebt::default(),
                        mutations,
                        touched_scope_filter: incomplete_touched_scope_filter(),
                        global_scope: commit.wire.global_scope,
                        current_state_scoped_ranges: None,
                        row_pk_index_root_id,
                        snapshot_root: Some(Box::new(
                            imported_roots
                                .remove(&commit_id)
                                .expect("every history boundary staged a root"),
                        )),
                    },
                )?
            } else {
                let parent_inputs = commit
                    .parent_commit_ids
                    .iter()
                    .map(|parent| {
                        staged_manifests
                            .get(parent)
                            .map(CertifiedCommitStateTopologyParent::Staged)
                            .or_else(|| {
                                published_topologies
                                    .get(parent)
                                    .map(CertifiedCommitStateTopologyParent::PublishedTopology)
                            })
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!("sync parent '{parent}' lost its state authority"),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let publication = if let Some((source_commit_id, _)) = commit.state_alias {
                    let source = staged_manifests
                        .get(&source_commit_id)
                        .map(CertifiedCommitStateTopologyParent::Staged)
                        .or_else(|| {
                            published_topologies
                                .get(&source_commit_id)
                                .map(CertifiedCommitStateTopologyParent::PublishedTopology)
                        })
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "sync complete-state source '{source_commit_id}' lost its state authority"
                                ),
                            )
                        })?;
                    stage_current_state_scoped_ranges_from_complete_state_source(
                        &read,
                        &mut writes,
                        &parent_inputs,
                        source,
                        commit_id,
                        &commit.account_id,
                        &mutations,
                    )
                    .await?
                } else {
                    stage_current_state_scoped_ranges_from_topology(
                        &read,
                        &mut writes,
                        &parent_inputs,
                        None,
                        commit_id,
                        &commit.account_id,
                        &mutations,
                    )
                    .await?
                };
                let manifest = CommitStateManifest {
                    commit_id,
                    change_account_id: commit.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations,
                    touched_scope_filter: publication.touched_scope_filter().clone(),
                    global_scope: commit.wire.global_scope,
                    current_state_scoped_ranges: publication.root(),
                    row_pk_index_root_id,
                    snapshot_root: Some(Box::new(
                        imported_roots
                            .remove(&commit_id)
                            .expect("every new sync commit staged a root"),
                    )),
                };
                stage_certified_commit_state_manifest_with_handle(
                    &mut writes,
                    &manifest,
                    &publication,
                )?
            };

            let generation = commit
                .parent_commit_ids
                .iter()
                .map(|parent| records[parent].generation)
                .max()
                .map_or(Ok(0), |generation| {
                    generation
                        .checked_add(1)
                        .ok_or_else(|| LixError::unknown("sync commit generation overflow"))
                })?;
            if let Some(parent) = commit.parent_commit_ids.first()
                && commit.parent_commit_ids.len() == 1
            {
                let jump = records[parent].first_parent_jump_commit_id;
                let missing_jump_record = match records.get(&jump) {
                    Some(_) => None,
                    None => Some(load_commit_record(&read, jump).await?.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("sync parent jump target '{jump}' is missing"),
                        )
                    })?),
                };
                if let Some(jump_record) = missing_jump_record {
                    records.insert(jump, jump_record);
                }
            }
            let parent_record = match commit.parent_commit_ids.as_slice() {
                [parent] => Some(&records[parent]),
                _ => None,
            };
            let parent_jump =
                parent_record.map(|parent| &records[&parent.first_parent_jump_commit_id]);
            let first_parent_jump = next_first_parent_jump(
                commit_id,
                &commit.parent_commit_ids,
                parent_record,
                parent_jump,
            )?;
            let record = CommitRecord {
                format_version: COMMIT_RECORD_FORMAT_VERSION,
                commit_id,
                generation,
                parent_commit_ids: commit.parent_commit_ids.clone(),
                base_commit_id: commit.base_commit_id,
                first_parent_jump_commit_id: first_parent_jump.0,
                first_parent_jump_span: first_parent_jump.1,
                account_id: commit.account_id.clone(),
                created_at: commit.created_at,
                touched_scope_digest,
            };
            if deferred_existing.contains(&commit_id) {
                let certified = records
                    .get(&commit_id)
                    .expect("deferred body retained its certified header");
                if certified.generation != record.generation
                    || certified.parent_commit_ids != record.parent_commit_ids
                    || certified.first_parent_jump_commit_id != record.first_parent_jump_commit_id
                    || certified.first_parent_jump_span != record.first_parent_jump_span
                    || certified.account_id != record.account_id
                    || certified.created_at != record.created_at
                {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "sync history body '{commit_id}' disagrees with its certified topology"
                        ),
                    ));
                }
            }
            if !deferred_existing.contains(&commit_id) {
                preconditions.push(StoragePrecondition::KeyAbsent {
                    space: COMMIT_SPACE,
                    key: StorageKey(Bytes::from(commit_key(commit_id))),
                });
            }
            records.insert(commit_id, record.clone());
            staged_manifests.insert(commit_id, staged_manifest);
            if !deferred_existing.contains(&commit_id) {
                appended_records.push(record);
                newly_imported.push(commit_id.to_string());
            }
            remaining.remove(&commit_id);
        }

        selected_fallback_locators
            .retain(|change_id, _| !imported_authored_change_ids.contains(change_id));
        stage_missing_selected_change_locators(
            &read,
            &mut writes,
            &mut preconditions,
            selected_fallback_locators,
        )
        .await?;
        stage_change_locators(
            &mut writes,
            &authored_locators.into_values().collect::<Vec<_>>(),
        );

        for (update, head, _) in &changed_refs {
            let Some(head) = head else {
                continue;
            };
            let record = &records[head];
            let change = sync_ref_change_record(
                &update.branch_id,
                *head,
                &record.account_id,
                record.created_at,
            )?;
            match load_existing_sync_change(&read, change.change_id).await? {
                Some(existing) if !sync_change_records_equal(&existing, &change)? => {
                    return Err(immutable_object_mismatch(
                        "branch ref change",
                        change.change_id,
                    ));
                }
                Some(_) => {}
                None => {
                    appended_changes.insert(change.change_id, change);
                }
            }
        }

        if !appended_records.is_empty() || !appended_changes.is_empty() {
            ChangelogContext::new()
                .writer(&mut &read, &mut writes)
                .stage_append(ChangelogAppend {
                    commits: appended_records,
                    changes: appended_changes.into_values().collect(),
                })
                .await?;
        }

        let hydrated_history = !deferred_existing.is_empty();
        // A direct, authored fast-forward already has every immutable object,
        // payload, and root staged in this write set. Publishing those objects
        // merely so the hot-head writer can read them back in a second durable
        // transaction adds a crash boundary without adding validation. Derive
        // the hot delta from the authenticated child commit and let the branch
        // control CAS publish commit, head, and replica receipt atomically.
        let certified_replica_delta =
            purpose == SyncImportPurpose::ReplicaDelta && replica_publication.is_some();
        let atomic_fast_forward = !certified_replica_delta
            && !changed_refs.is_empty()
            && changed_refs.iter().all(|(update, head, checkpoint)| {
                let observation_index = branch_ids
                    .iter()
                    .position(|branch_id| branch_id == &update.branch_id)
                    .expect("changed ref came from request");
                let Some(previous) = observations[observation_index].control else {
                    return false;
                };
                let Some(head) = head else {
                    return false;
                };
                let Some(commit) = parsed.get(head) else {
                    return false;
                };
                !existing.contains(head)
                    && commit.parent_commit_ids.as_slice() == [previous.head_commit_id]
                    && commit.members.iter().all(|member| member.authored)
                    && previous.working_diff_checkpoint_commit_id == *checkpoint
            });
        // Immutable commits and roots are safe to publish before a ref. This
        // makes the second transaction a small atomic `(head, checkpoint)`
        // publication whose hash-guided hot-state diff can read both roots.
        // A failed CAS can leave only unreachable immutable objects.
        let (read, mut writes, mut preconditions, observations) = if !atomic_fast_forward
            && !changed_refs.is_empty()
            && (!newly_imported.is_empty() || hydrated_history)
        {
            crate::json_store::stage_json_publication_fence(&read, &mut writes, &mut preconditions)
                .await?;
            drop(read);
            let options = StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..StorageWriteOptions::default()
            };
            if self.sync_mode_state().role() == super::SyncRole::Replica
                && purpose != SyncImportPurpose::AuthorityPush
            {
                adapter
                    .commit_certified_replica_write_set(
                        super::certified_replica_write_capability(),
                        writes,
                        options,
                    )
                    .await?;
            } else {
                adapter.commit_write_set(writes, options).await?;
            }
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let observations = BranchHeadControlContext::new()
                .reader(&read)
                .load_observed(&branch_ids)
                .await?;
            (read, adapter.new_write_set(), Vec::new(), observations)
        } else {
            (read, writes, preconditions, observations)
        };

        let mut published_ref_updates = Vec::new();
        for (update, head, checkpoint) in &changed_refs {
            let observation_index = branch_ids
                .iter()
                .position(|branch_id| branch_id == &update.branch_id)
                .expect("changed ref came from request");
            let observation = &observations[observation_index];
            let current_head = observation.control.map(|control| control.head_commit_id);
            let current_checkpoint = observation
                .control
                .and_then(|control| control.working_diff_checkpoint_commit_id);
            if current_head == *head && current_checkpoint == *checkpoint {
                continue;
            }
            let expected_head = update
                .expected_head_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync expected ref head"))
                .transpose()?;
            let expected_checkpoint = update
                .expected_checkpoint_commit_id
                .as_deref()
                .map(|id| CommitId::parse_lix(id, "sync expected checkpoint"))
                .transpose()?;
            if current_head != expected_head || current_checkpoint != expected_checkpoint {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    format!(
                        "sync ref '{}' changed while immutable commits were admitted",
                        update.branch_id
                    ),
                ));
            }
            preconditions.push(branch_head_control_precondition(
                &update.branch_id,
                observation.raw_token.clone(),
            )?);
            let Some(head) = head else {
                stage_delete_branch_head_control(&mut writes, &update.branch_id)?;
                published_ref_updates.push(update.clone());
                continue;
            };
            let checkpoint = checkpoint.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync ref '{}' has a head but no checkpoint",
                        update.branch_id
                    ),
                )
            })?;
            let previous = observation.control;
            let (generation, coverage) = if let Some(previous) = previous
                && previous.working_diff_checkpoint_commit_id == Some(checkpoint)
            {
                let epoch = TrackedHeadContext::new()
                    .reader(&read)
                    .working_diff_epoch(&update.branch_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "sync branch '{}' has a checkpoint cursor but no working-diff epoch",
                                update.branch_id
                            ),
                        )
                    })?;
                if epoch.checkpoint_commit_id != checkpoint
                    || epoch.generation != previous.tracked_generation
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "sync branch '{}' has a stale working-diff epoch",
                            update.branch_id
                        ),
                    ));
                }
                let mut coverage = epoch.coverage;
                let generation = if atomic_fast_forward {
                    let commit = parsed
                        .get(head)
                        .expect("atomic fast-forward target was validated");
                    let keys = commit
                        .members
                        .iter()
                        .map(|member| TrackedStateKey {
                            schema_key: member.schema_key.clone(),
                            file_id: member.file_id.clone(),
                            row_pk: member.row_pk.clone(),
                        })
                        .collect::<Vec<_>>();
                    let before = TrackedStateContext::new()
                        .reader(&read)
                        .index_values_at_commit(&previous.head_commit_id.to_string(), &keys)
                        .await?;
                    let absence_guards = keys
                        .iter()
                        .zip(before)
                        .filter_map(|(key, before)| {
                            before
                                .as_ref()
                                .is_none_or(|before| before.deleted)
                                .then(|| key.clone())
                        })
                        .collect::<BTreeSet<_>>();
                    let deltas = commit
                        .members
                        .iter()
                        .map(|member| member.as_current_delta(*head))
                        .collect::<Vec<_>>();
                    stage_sync_hot_delta(
                        &read,
                        &mut writes,
                        &update.branch_id,
                        previous.tracked_generation,
                        *head,
                        &deltas,
                        &absence_guards,
                        checkpoint,
                        &mut coverage,
                    )
                    .await?
                } else {
                    let diff = TrackedStateContext::new()
                        .reader(&read)
                        .diff_commits(
                            &previous.head_commit_id.to_string(),
                            &head.to_string(),
                            &TrackedStateDiffRequest::default(),
                        )
                        .await?;
                    let mut deltas = Vec::with_capacity(diff.entries.len());
                    let mut absence_guards = BTreeSet::new();
                    for entry in &diff.entries {
                        let after = entry.after.as_ref().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "sync tracked-state diff removed a row without a tombstone",
                            )
                        })?;
                        let payload = diff.payloads().get(after.change_id).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("sync diff lost payload for change '{}'", after.change_id),
                            )
                        })?;
                        if entry.visible_before().is_none() {
                            absence_guards.insert(TrackedStateKey {
                                schema_key: entry.identity.schema_key().to_owned(),
                                file_id: entry.identity.file_id().map(str::to_owned),
                                row_pk: entry.identity.row_pk().clone(),
                            });
                        }
                        deltas.push(CurrentStateDeltaRef {
                            schema_key: entry.identity.schema_key(),
                            file_id: entry.identity.file_id(),
                            row_pk: entry.identity.row_pk(),
                            change_id: Some(after.change_id),
                            commit_id: Some(after.commit_id),
                            untracked: false,
                            deleted: after.deleted,
                            created_at: after.created_at,
                            updated_at: after.updated_at,
                            snapshot: payload.snapshot,
                            metadata: payload.metadata,
                            columnar_base_coordinate: None,
                        });
                    }
                    stage_sync_hot_delta(
                        &read,
                        &mut writes,
                        &update.branch_id,
                        previous.tracked_generation,
                        *head,
                        &deltas,
                        &absence_guards,
                        checkpoint,
                        &mut coverage,
                    )
                    .await?
                };
                (generation, coverage)
            } else {
                let current = load_sync_hot_snapshot(&read, &update.branch_id, *head).await?;
                let checkpoint_snapshot =
                    load_sync_hot_snapshot(&read, &update.branch_id, checkpoint).await?;
                let generation = CommitId::with_change_address_space(uuid::Uuid::now_v7());
                let mut coverage = WorkingDiffIndexCoverage::default();
                TrackedHeadContext::new()
                    .writer(&read, &mut writes)
                    .stage_complete_current_state_with_working_diff(
                        &update.branch_id,
                        generation,
                        current,
                        previous.map(|control| control.tracked_generation),
                        &[],
                        &[],
                        &BTreeSet::new(),
                        if *head == checkpoint {
                            CompleteWorkingDiffMode::ResetClean
                        } else {
                            CompleteWorkingDiffMode::Rebase {
                                checkpoint_commit_id: checkpoint,
                                checkpoint: checkpoint_snapshot,
                            }
                        },
                        &mut coverage,
                    )
                    .await?;
                (generation, coverage)
            };
            stage_tracked_working_diff_epoch(
                &mut writes,
                &update.branch_id,
                TrackedWorkingDiffEpoch {
                    checkpoint_commit_id: checkpoint,
                    generation,
                    coverage,
                },
            )?;
            let head_record = records.get(head).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_COMMIT_NOT_FOUND,
                    format!("sync ref target '{head}' does not exist"),
                )
            })?;
            let mut control = BranchHeadControl {
                head_commit_id: *head,
                tracked_generation: generation,
                current_state_revision: observation.control.map_or(Ok(0), |control| {
                    control
                        .current_state_revision
                        .checked_add(1)
                        .ok_or_else(|| LixError::unknown("branch current-state revision overflow"))
                })?,
                working_diff_checkpoint_commit_id: Some(checkpoint),
                created_at: observation
                    .control
                    .map_or(head_record.created_at, |control| control.created_at),
                updated_at: head_record.created_at,
                ref_change_id: sync_ref_change_id(&update.branch_id, Some(*head)),
                schema_presence_bloom: [0; 4],
            };
            control.schema_presence_bloom = [u64::MAX; 4];
            stage_branch_head_control(&mut writes, &update.branch_id, control)?;
            published_ref_updates.push(update.clone());
        }

        if let Some(publication) = &replica_publication {
            if publication.state.cursor <= publication.expected_cursor {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync replica publication must advance its event cursor",
                ));
            }
            let (_stored_state, previous) = load_replica_state(&read).await?;
            if previous.as_ref() != Some(publication.expected_state_raw) {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "sync replica state changed while its repository event was admitted",
                ));
            }
            stage_replica_state(&mut writes, &mut preconditions, publication.state, previous)?;
        }
        crate::json_store::stage_json_publication_fence(&read, &mut writes, &mut preconditions)
            .await?;
        let (current_cursor, _) = load_sequence(&read).await?;
        if newly_imported.is_empty()
            && published_ref_updates.is_empty()
            && !hydrated_history
            && replica_publication.is_none()
        {
            return Ok(SyncPushResponse {
                cursor: current_cursor,
            });
        }
        if !published_ref_updates.is_empty() {
            // A remote head movement can reveal filesystem and account rows
            // selected from older commits even when the pushed tip authored
            // neither schema. Invalidate the same derived read caches as a
            // local transaction before publishing the ref atomically.
            crate::filesystem::stage_path_index_revision(&mut writes);
            crate::account::stage_account_revision(&mut writes);
        }
        let cursor = if purpose == SyncImportPurpose::AuthorityPush
            && (!newly_imported.is_empty() || !published_ref_updates.is_empty())
        {
            let event_commit_ids = if published_ref_updates.is_empty() {
                newly_imported
            } else {
                request
                    .commits
                    .iter()
                    .map(|commit| commit.commit_id.clone())
                    .collect()
            };
            stage_repository_event(
                &read,
                &mut writes,
                &mut preconditions,
                event_commit_ids,
                published_ref_updates,
            )
            .await?
            .cursor
        } else {
            current_cursor
        };
        drop(read);
        let options = StorageWriteOptions {
            preconditions,
            await_durable: true,
            ..StorageWriteOptions::default()
        };
        if self.sync_mode_state().role() == super::SyncRole::Replica
            && purpose != SyncImportPurpose::AuthorityPush
        {
            adapter
                .commit_certified_replica_write_set(
                    super::certified_replica_write_capability(),
                    writes,
                    options,
                )
                .await?;
        } else {
            adapter.commit_write_set(writes, options).await?;
        }
        self.notify_observers_for_sync();
        self.sync_mode_state().notify_sync_change();
        Ok(SyncPushResponse { cursor })
    }

    pub(crate) async fn sync_history_demand_ids(
        &self,
        commit_ids: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut pending = BTreeSet::new();
        for commit_id in commit_ids {
            let parsed = CommitId::parse_lix(&commit_id, "sync history demand commit id")?;
            if load_commit_record(&read, parsed).await?.is_none()
                || commit_history_is_deferred(&read, parsed).await?
            {
                pending.insert(commit_id);
            }
        }
        Ok(pending)
    }

    pub(crate) async fn sync_checkpoint_history_demand_ids(
        &self,
        commit_ids: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, LixError> {
        if commit_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        let requested = commit_ids
            .iter()
            .map(|commit_id| {
                let parsed =
                    CommitId::parse_lix(commit_id, "sync checkpoint history demand commit id")?;
                Ok((
                    RowPk::uuid_from_canonical(&parsed.to_string())
                        .expect("a canonical commit UUID is a canonical checkpoint row key"),
                    commit_id.clone(),
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let Some(global) = BranchHeadControlContext::new()
            .reader(&read)
            .load(crate::GLOBAL_BRANCH_ID)
            .await?
        else {
            return Ok(BTreeSet::new());
        };
        let mut tracked = TrackedStateContext::new().reader(&read);
        let rows = tracked
            .scan_batch_at_commit(
                &global.head_commit_id.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![crate::checkpoint::CHECKPOINT_SCHEMA_KEY.to_owned()],
                        row_pks: requested.iter().map(|(row_pk, _)| row_pk.clone()).collect(),
                        ..TrackedStateFilter::default()
                    },
                    limit: Some(requested.len()),
                    ..TrackedStateScanRequest::default()
                },
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                requested
                    .iter()
                    .find(|(row_pk, _)| row_pk == row.row_pk())
                    .map(|(_, commit_id)| commit_id.clone())
            })
            .collect())
    }

    async fn complete_history_live_root_at_commit(
        &self,
        read: &(impl StorageAdapterRead + ?Sized),
        commit_id: CommitId,
    ) -> Result<TrackedStateRootId, LixError> {
        let mut tracked = TrackedStateContext::new().reader(read);
        let rows = tracked
            .scan_batch_at_commit(&commit_id.to_string(), &TrackedStateScanRequest::default())
            .await?;
        let deltas = rows.iter().map(|row| TrackedStateDeltaRef {
            schema_key: row.schema_key(),
            file_id: row.file_id(),
            row_pk: row.row_pk(),
            change_id: row.change_id(),
            commit_id: row.commit_id(),
            deleted: false,
            created_at: row.created_at(),
            updated_at: row.updated_at(),
        });
        let mut transient_writes = self.storage_adapter().new_write_set();
        let tracked_context = TrackedStateContext::new();
        let mut writer = tracked_context.writer(read, &mut transient_writes);
        writer
            .stage_commit_root(&commit_id.to_string(), None, deltas)
            .await?;
        Ok(writer
            .staged_commit_roots()
            .find(|root| root.commit_id == commit_id)
            .expect("complete live root was staged")
            .root_id
            .clone())
    }

    async fn complete_sync_live_value_root_at_commit(
        &self,
        read: &(impl StorageAdapterRead + ?Sized),
        commit_id: CommitId,
    ) -> Result<SyncLiveValueRootId, LixError> {
        let rows = load_sync_live_value_rows_at_commit(read, commit_id).await?;
        sync_live_value_root(rows.values().map(SyncLiveValueRow::as_ref))
    }

    pub(crate) async fn sync_history(
        &self,
        head: &str,
        limit: usize,
    ) -> Result<SyncHistoryResponse, LixError> {
        if limit == 0 || limit > super::MAX_SYNC_HISTORY_PAGE_SIZE {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync history limit must be between 1 and {}",
                    super::MAX_SYNC_HISTORY_PAGE_SIZE
                ),
            ));
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let head = CommitId::parse_lix(head, "sync history head")?;
        load_commit_record(&read, head)
            .await?
            .ok_or_else(|| LixError::commit_not_found(head.to_string(), "sync_history", "head"))?;
        let mut next = Some(head);
        let mut newest_first = Vec::with_capacity(limit);
        while let Some(commit_id) = next
            && newest_first.len() < limit
        {
            let record = load_commit_record(&read, commit_id).await?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync history chain is missing commit '{commit_id}'"),
                )
            })?;
            next = record.parent_commit_ids.first().copied();
            newest_first.push(record);
        }
        let body_ids = newest_first
            .iter()
            .map(|record| record.commit_id)
            .collect::<BTreeSet<_>>();
        let external_base_ids = newest_first
            .iter()
            .filter_map(|record| record.base_commit_id)
            .filter(|base| !body_ids.contains(base))
            .collect::<BTreeSet<_>>();
        let mut boundary_ids = newest_first
            .iter()
            .filter(|record| {
                record
                    .parent_commit_ids
                    .iter()
                    .any(|parent| !body_ids.contains(parent))
            })
            .map(|record| record.commit_id)
            .collect::<BTreeSet<_>>();
        // A local commit is not a complete state authority without its pinned
        // global base. When that base is outside the bounded body page, ship
        // it as a materialized boundary just like an external causal parent.
        // Its canonical body travels as an out-of-page dependency below.
        boundary_ids.extend(external_base_ids.iter().copied());
        let mut commits = Vec::with_capacity(newest_first.len() + external_base_ids.len());
        for record in newest_first.iter().rev() {
            commits.push(
                load_sync_commit(&read, record.commit_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "sync history commit '{}' has no complete body",
                                record.commit_id
                            ),
                        )
                    })?,
            );
        }
        // Dependency bodies do not consume causal history-page slots. Ship
        // external pinned global bases in the same response so the receiver
        // can publish each immutable commit authority exactly once without an
        // additional request per local commit.
        for commit_id in &external_base_ids {
            commits.push(load_sync_commit(&read, *commit_id).await?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync history base dependency '{commit_id}' has no complete body"),
                )
            })?);
        }
        // A complete-state alias is O(1) only when the receiver already owns
        // its physical source (the normal ordered-delta case). History demand
        // cannot assume that, and GC may have retired the source's semantic
        // commit record while retaining its chunks. Mark the checkpoint itself
        // as a paginated state boundary so history remains self-contained
        // without expanding one commit body past the response cap.
        boundary_ids.extend(
            commits
                .iter()
                .filter_map(|commit| {
                    commit
                        .state_alias
                        .as_ref()
                        .map(|_| CommitId::parse_lix(&commit.commit_id, "sync checkpoint boundary"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let mut header_ids = body_ids.clone();
        for record in &newest_first {
            header_ids.extend(record.parent_commit_ids.iter().copied());
            header_ids.extend(record.base_commit_id);
            if record.first_parent_jump_span > 0 {
                header_ids.insert(record.first_parent_jump_commit_id);
            }
        }
        let mut pending_header_ids = header_ids.iter().copied().collect::<Vec<_>>();
        while let Some(commit_id) = pending_header_ids.pop() {
            let record = load_commit_record(&read, commit_id).await?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync history boundary '{commit_id}' is missing"),
                )
            })?;
            if record.first_parent_jump_span > 0
                && header_ids.insert(record.first_parent_jump_commit_id)
            {
                pending_header_ids.push(record.first_parent_jump_commit_id);
            }
            if let Some(base_commit_id) = record.base_commit_id
                && header_ids.insert(base_commit_id)
            {
                pending_header_ids.push(base_commit_id);
            }
        }
        let mut commit_headers = Vec::with_capacity(header_ids.len());
        for commit_id in header_ids {
            let record = load_commit_record(&read, commit_id)
                .await?
                .expect("validated history boundary remains present");
            let global_scope =
                match load_published_commit_state_topology(&read, record.commit_id).await? {
                    Some(topology) => topology.global_scope(),
                    None => deferred_commit_global_scope(&read, record.commit_id)
                        .await?
                        .unwrap_or(false),
                };
            commit_headers.push(sync_header_from_record(&record, global_scope));
        }
        let mut boundaries = Vec::with_capacity(boundary_ids.len());
        for commit_id in boundary_ids {
            let live_state_root_id = self
                .complete_history_live_root_at_commit(&read, commit_id)
                .await?;
            boundaries.push(SyncHistoryBoundary {
                commit_id: commit_id.to_string(),
                live_state_root_id: format_sync_state_root_id(&live_state_root_id),
            });
        }
        Ok(SyncHistoryResponse {
            commits,
            commit_headers,
            boundaries,
        })
    }

    pub(crate) async fn pull_sync_snapshot_rows(
        &self,
        branch_id: &str,
        head_commit_id: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<SyncSnapshotRowPage, LixError> {
        super::validate_sync_branch_id(branch_id)?;
        if limit == 0 || limit > super::MAX_SYNC_REQUEST_ITEMS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync snapshot row limit must be between 1 and {}",
                    super::MAX_SYNC_REQUEST_ITEMS
                ),
            ));
        }
        let head = CommitId::parse_lix(head_commit_id, "sync snapshot row head")?;
        let after = continuation
            .map(decode_snapshot_row_cursor)
            .transpose()?
            .map(|(schema_key, file_id, row_pk)| TrackedStateKey {
                schema_key,
                file_id,
                row_pk,
            });
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if load_commit_record(&read, head).await?.is_none() {
            return Err(LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                format!("sync snapshot row head '{head}' does not exist"),
            ));
        }
        let mut tracked = TrackedStateContext::new().reader(&read);
        let request = TrackedStateScanRequest {
            limit: Some(limit.saturating_add(1)),
            ..TrackedStateScanRequest::default()
        };
        let batch = tracked
            .scan_batch_at_commit_page(head_commit_id, &request, after.as_ref())
            .await?;
        let mut selected = batch.iter().collect::<Vec<_>>();
        let has_more = selected.len() > limit;
        if has_more {
            selected.pop();
        }
        let next = if has_more {
            let row = selected.last().expect("positive page limit emitted a row");
            Some(encode_snapshot_row_cursor(
                row.schema_key(),
                row.file_id(),
                row.row_pk(),
            )?)
        } else {
            None
        };
        let mut rows = Vec::with_capacity(selected.len());
        for row in selected {
            let change = load_change_record_by_id(&read, row.change_id())
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("sync snapshot row change '{}' is missing", row.change_id()),
                    )
                })?;
            rows.push(SyncSnapshotRow {
                branch_id: branch_id.to_owned(),
                schema_key: row.schema_key().to_owned(),
                file_id: row.file_id().map(str::to_owned),
                row_pk: row.row_pk().as_typed_json_array_value()?,
                snapshot: row
                    .snapshot_content()
                    .map(|value| serde_json::from_str(value.as_str()))
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode sync snapshot row: {error}"),
                        )
                    })?,
                metadata: row
                    .metadata()
                    .map(|value| serde_json::from_str(value.as_str()))
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode sync snapshot metadata: {error}"),
                        )
                    })?,
                change_id: row.change_id().to_string(),
                commit_id: row.commit_id().to_string(),
                created_at: row.created_at().to_string(),
                updated_at: row.updated_at().to_string(),
                change_account_id: change.account_id,
                change_created_at: change.created_at.to_string(),
                origin_key: change.origin_key,
            });
        }
        Ok(SyncSnapshotRowPage {
            branch_id: branch_id.to_owned(),
            head_commit_id: head_commit_id.to_owned(),
            rows,
            continuation: next,
        })
    }

    pub(crate) async fn pull_sync_repository(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<SyncRepositoryPullResponse, LixError> {
        if limit == 0 || limit > super::MAX_SYNC_REQUEST_ITEMS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync pull limit must be between 1 and {}",
                    super::MAX_SYNC_REQUEST_ITEMS
                ),
            ));
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let (head_cursor, _) = load_sequence(&read).await?;

        let Some(after) = after else {
            drop(read);
            return self.build_sync_snapshot().await;
        };
        if after > head_cursor {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync cursor {after} is ahead of repository cursor {head_cursor}"),
            ));
        }
        let count = usize::try_from(head_cursor - after)
            .unwrap_or(usize::MAX)
            .min(limit);
        let keys = (1..=count)
            .map(|offset| event_key(after + u64::try_from(offset).expect("pull limit fits u64")))
            .collect::<Vec<_>>();
        let values = exact_get_many(
            &read,
            &[StorageGetManyRequest {
                space: SYNC_REPOSITORY_EVENT_SPACE,
                keys: &keys,
                opts: StorageGetOptions::default(),
            }],
        )
        .await?;
        let mut events = Vec::with_capacity(count);
        for value in values.values {
            let Some(StorageProjectedValue::FullValue(value)) = value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "repository sync event is missing below its sequence head",
                ));
            };
            let record: RepositoryEventRecord =
                serde_json::from_slice(&value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("decode repository sync event: {error}"),
                    )
                })?;
            let mut commits = Vec::with_capacity(record.commit_ids.len());
            for commit_id in &record.commit_ids {
                commits.push(export_sync_commit(self, commit_id).await?.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("repository sync event references missing commit '{commit_id}'"),
                    )
                })?);
            }
            let ref_updates = record.ref_updates;
            let commit_refs = commits.iter().collect::<Vec<_>>();
            let mut inline_blobs = Vec::new();
            let mut encoded_len =
                super::encoded_delta_event_len(record.cursor, &commit_refs, &ref_updates)?;
            for blob_id in sync_commit_blob_ids(&commits)? {
                let manifest = self.get_sync_inline_blob_manifest(&blob_id).await?;
                if let Some(manifest) = manifest {
                    if !append_bounded_inline_blob(
                        &mut inline_blobs,
                        &mut encoded_len,
                        manifest,
                        "inline sync event blob",
                    )? {
                        break;
                    }
                }
            }
            events.push(SyncEvent {
                cursor: record.cursor,
                commits,
                ref_updates,
                inline_blobs,
            });
        }
        let cursor = events.last().map_or(after, |event| event.cursor);
        Ok(SyncRepositoryPullResponse::Delta { cursor, events })
    }

    async fn build_sync_snapshot(&self) -> Result<SyncRepositoryPullResponse, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let (cursor, _) = load_sequence(&read).await?;
        let default_branch_id = self.repository_default_branch_id_for_sync(&read).await?;
        let controls = BranchHeadControlContext::default()
            .reader(&read)
            .scan()
            .await?;
        let mut branches = Vec::with_capacity(controls.len());
        for (branch_id, control) in &controls {
            let hot_state_root_id = self
                .complete_sync_live_value_root_at_commit(&read, control.head_commit_id)
                .await?;
            let checkpoint = control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync branch '{branch_id}' has no checkpoint cursor"),
                )
            })?;
            let checkpoint_state_root_id = self
                .complete_sync_live_value_root_at_commit(&read, checkpoint)
                .await?;
            branches.push(SyncBranchHead {
                branch_id: branch_id.clone(),
                head_commit_id: Some(control.head_commit_id.to_string()),
                checkpoint_commit_id: Some(checkpoint.to_string()),
                checkpoint_state_root_id: format_sync_live_value_root_id(&checkpoint_state_root_id),
                hot_state_root_id: format_sync_live_value_root_id(&hot_state_root_id),
            });
        }
        let metadata = SyncRepositoryPullResponse::Snapshot {
            cursor,
            lix_id: self.lix_id().to_owned(),
            default_branch_id,
            branches,
        };
        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::hot_state::{HotStateContext, HotStateRowRequest};
    use crate::storage::{
        BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, Memory,
        MemoryRead, MemoryWrite, PutBatch, ReadOptions, ScanCursor, Storage, StorageError,
        StorageRead, StorageSpace, StorageWrite, WriteOptions,
    };
    use crate::storage_adapter::SharedStorageAdapterRead;
    use crate::{
        CreateBranchOptions, GLOBAL_BRANCH_ID, Lix, MergeBranchOptions, NullableKeyFilter,
        SwitchBranchOptions, Value, open_lix,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const TEST_REMOTE: &str = "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001";

    #[derive(Clone, Default)]
    struct SyncAccountingStorage {
        inner: Memory,
        forbidden_operations: Arc<AtomicU64>,
        branch_control_scans: Arc<AtomicU64>,
        write_transactions: Arc<AtomicU64>,
        atomic_ref_receipts: Arc<AtomicU64>,
        branch_publications_without_receipt: Arc<AtomicU64>,
        fail_next_replica_receipt: Arc<AtomicU64>,
        bootstrap_commit_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
    }

    struct SyncAccountingRead {
        inner: MemoryRead,
        forbidden_operations: Arc<AtomicU64>,
        branch_control_scans: Arc<AtomicU64>,
    }

    struct SyncAccountingWrite {
        inner: MemoryWrite,
        write_transactions: Arc<AtomicU64>,
        atomic_ref_receipts: Arc<AtomicU64>,
        branch_publications_without_receipt: Arc<AtomicU64>,
        fail_next_replica_receipt: Arc<AtomicU64>,
        bootstrap_commit_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
        touches_branch_control: bool,
        touches_replica_state: bool,
    }

    fn is_logical_space(space: StorageSpace, logical: StorageSpace) -> bool {
        space.id.0 & 0x3fff_ffff == logical.id.0
    }

    impl SyncAccountingStorage {
        fn reset(&self) {
            self.forbidden_operations.store(0, Ordering::Relaxed);
            self.branch_control_scans.store(0, Ordering::Relaxed);
            self.write_transactions.store(0, Ordering::Relaxed);
            self.atomic_ref_receipts.store(0, Ordering::Relaxed);
            self.branch_publications_without_receipt
                .store(0, Ordering::Relaxed);
        }

        fn forbidden_operation_count(&self) -> u64 {
            self.forbidden_operations.load(Ordering::Relaxed)
        }

        fn branch_control_scan_count(&self) -> u64 {
            self.branch_control_scans.load(Ordering::Relaxed)
        }

        fn write_transaction_count(&self) -> u64 {
            self.write_transactions.load(Ordering::Relaxed)
        }

        fn atomic_ref_receipt_count(&self) -> u64 {
            self.atomic_ref_receipts.load(Ordering::Relaxed)
        }

        fn branch_publication_without_receipt_count(&self) -> u64 {
            self.branch_publications_without_receipt
                .load(Ordering::Relaxed)
        }

        fn fail_next_replica_receipt(&self) {
            self.fail_next_replica_receipt.store(1, Ordering::Relaxed);
        }

        fn gate_bootstrap_commits(&self, participants: usize) {
            *self
                .bootstrap_commit_barrier
                .lock()
                .expect("bootstrap gate should lock") =
                Some(Arc::new(tokio::sync::Barrier::new(participants)));
        }
    }

    impl Storage for SyncAccountingStorage {
        type Read<'a>
            = SyncAccountingRead
        where
            Self: 'a;
        type Write<'a>
            = SyncAccountingWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            Ok(SyncAccountingRead {
                inner: self.inner.begin_read(options).await?,
                forbidden_operations: Arc::clone(&self.forbidden_operations),
                branch_control_scans: Arc::clone(&self.branch_control_scans),
            })
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.forbidden_operations.fetch_add(1, Ordering::Relaxed);
            Ok(SyncAccountingWrite {
                inner: self.inner.begin_write(options).await?,
                write_transactions: Arc::clone(&self.write_transactions),
                atomic_ref_receipts: Arc::clone(&self.atomic_ref_receipts),
                branch_publications_without_receipt: Arc::clone(
                    &self.branch_publications_without_receipt,
                ),
                fail_next_replica_receipt: Arc::clone(&self.fail_next_replica_receipt),
                bootstrap_commit_barrier: Arc::clone(&self.bootstrap_commit_barrier),
                touches_branch_control: false,
                touches_replica_state: false,
            })
        }
    }

    impl StorageWrite for SyncAccountingWrite {
        async fn replace_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.touches_branch_control |=
                is_logical_space(space, crate::branch::BRANCH_HEAD_CONTROL_SPACE);
            self.touches_replica_state |= is_logical_space(space, SYNC_REPLICA_STATE_SPACE);
            self.inner.replace_many(space, entries).await
        }

        async fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.touches_branch_control |=
                is_logical_space(space, crate::branch::BRANCH_HEAD_CONTROL_SPACE);
            self.touches_replica_state |= is_logical_space(space, SYNC_REPLICA_STATE_SPACE);
            self.inner.put_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[Key],
        ) -> Result<(), StorageError> {
            self.touches_branch_control |=
                is_logical_space(space, crate::branch::BRANCH_HEAD_CONTROL_SPACE);
            self.touches_replica_state |= is_logical_space(space, SYNC_REPLICA_STATE_SPACE);
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
            range: KeyRange,
        ) -> Result<(), StorageError> {
            self.touches_branch_control |=
                is_logical_space(space, crate::branch::BRANCH_HEAD_CONTROL_SPACE);
            self.touches_replica_state |= is_logical_space(space, SYNC_REPLICA_STATE_SPACE);
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, StorageError> {
            self.write_transactions.fetch_add(1, Ordering::Relaxed);
            let bootstrap_commit_barrier = if self.touches_replica_state {
                self.bootstrap_commit_barrier
                    .lock()
                    .expect("bootstrap gate should lock")
                    .clone()
            } else {
                None
            };
            if let Some(barrier) = bootstrap_commit_barrier {
                barrier.wait().await;
            }
            if self.touches_replica_state
                && self.fail_next_replica_receipt.swap(0, Ordering::Relaxed) != 0
            {
                return Err(StorageError::Io(
                    "injected replica receipt failure".to_owned(),
                ));
            }
            if self.touches_branch_control && self.touches_replica_state {
                self.atomic_ref_receipts.fetch_add(1, Ordering::Relaxed);
            } else if self.touches_branch_control {
                self.branch_publications_without_receipt
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.commit().await
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }

    impl StorageRead for SyncAccountingRead {
        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            options: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.forbidden_operations.fetch_add(1, Ordering::Relaxed);
            if is_logical_space(space, crate::branch::BRANCH_HEAD_CONTROL_SPACE) {
                self.branch_control_scans.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.begin_scan(space, range, options).await
        }
    }

    async fn close_history_boundary_bodies(authority: &Lix, history: &mut SyncHistoryResponse) {
        let missing = history
            .boundaries
            .iter()
            .filter(|boundary| {
                !history
                    .commits
                    .iter()
                    .any(|commit| commit.commit_id == boundary.commit_id)
            })
            .map(|boundary| boundary.commit_id.clone())
            .collect::<Vec<_>>();
        let mut headers = history
            .commit_headers
            .drain(..)
            .map(|header| (header.commit_id.clone(), header))
            .collect::<BTreeMap<_, _>>();
        for commit_id in missing {
            let dependency = authority
                .sync_history(&commit_id, 1)
                .await
                .expect("external history boundary body should load");
            for commit in dependency.commits {
                if !history
                    .commits
                    .iter()
                    .any(|existing| existing.commit_id == commit.commit_id)
                {
                    history.commits.push(commit);
                }
            }
            headers.extend(
                dependency
                    .commit_headers
                    .into_iter()
                    .map(|header| (header.commit_id.clone(), header)),
            );
        }
        history.commit_headers = headers.into_values().collect();
    }

    #[tokio::test]
    async fn ancestry_walk_stops_at_an_absent_lazy_history_boundary() {
        let lix = open_lix().await.expect("open Lix");
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open ancestry read");
        let absent = CommitId::parse_lix(
            "01920000-0000-7000-8000-000000000501",
            "absent history commit",
        )
        .expect("test commit ID parses");
        let other = CommitId::parse_lix(
            "01920000-0000-7000-8000-000000000502",
            "other history commit",
        )
        .expect("test commit ID parses");

        assert!(
            !commit_reaches_ancestor(&read, absent, other)
                .await
                .expect("absent lazy history is an unknown boundary"),
            "an omitted cold commit cannot prove reachability",
        );
    }

    #[tokio::test]
    async fn ancestry_walk_explores_other_merge_parents_after_a_sparse_boundary() {
        let lix = open_lix().await.expect("open Lix");
        let snapshot = lix
            .pull_sync_repository(None, 1)
            .await
            .expect("load initial snapshot");
        let (_, ancestor) = default_head(&snapshot);
        let ancestor =
            CommitId::parse_lix(&ancestor, "known merge ancestor").expect("ancestor id parses");
        let absent = CommitId::for_test_label("absent-secondary-merge-parent");
        let merge = CommitId::for_test_label("sparse-merge-descendant");

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open ancestor read");
        let ancestor_record = load_commit_record(&read, ancestor)
            .await
            .expect("load ancestor")
            .expect("ancestor exists");
        drop(read);
        let mut merge_header = sync_header_from_record(&ancestor_record, false);
        merge_header.commit_id = merge.to_string();
        // The walker is LIFO. Put the absent parent last so it is visited
        // before the known ancestor and cannot mask the reachable path.
        merge_header.parent_commit_ids = vec![ancestor.to_string(), absent.to_string()];
        merge_header.generation = ancestor_record.generation.saturating_add(1);
        merge_header.first_parent_jump_commit_id = None;
        merge_header.first_parent_jump_span = None;
        lix.import_sync_history_headers(&[merge_header])
            .await
            .expect("install sparse merge header");

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open ancestry read");
        assert!(
            commit_reaches_ancestor(&read, merge, ancestor)
                .await
                .expect("known merge parent proves reachability"),
            "an absent sibling parent must not hide a known ancestor path",
        );
    }

    #[tokio::test]
    async fn applying_a_sync_delta_invalidates_observers_without_waiting_for_storage_polling() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        write_key_value(&authority, "observed-sync-value", "before").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let params = [Value::Text("observed-sync-value".to_owned())];
        replica
            .set_sync_role(super::super::SyncRole::Disabled)
            .expect("local observer fixture should bypass public replica routing");
        let mut events = replica
            .observe("SELECT value FROM lix_key_value WHERE key = $1", &params)
            .expect("observe replica value");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should be restored before applying the delta");
        events
            .next()
            .await
            .expect("initial observer evaluation succeeds")
            .expect("initial observer event exists");

        write_key_value(&authority, "observed-sync-value", "after").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("authority delta should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("replica should apply remote delta");

        let event = tokio::time::timeout(Duration::from_millis(100), events.next())
            .await
            .expect("direct sync apply should wake observers before the 250 ms storage poll")
            .expect("remote observer evaluation succeeds")
            .expect("remote observer event exists");
        assert_eq!(
            event.rows.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("observed value decodes"),
            serde_json::json!("after"),
        );
    }

    #[tokio::test]
    async fn authority_rejects_unsynchronized_untracked_mutations() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let error = authority
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('authority-local-only', 'forbidden', true)",
                &[],
            )
            .await
            .expect_err("authority must not accept state omitted from sync");
        assert_eq!(error.code, "LIX_AUTHORITY_UNTRACKED_UNSUPPORTED");
        assert!(
            authority
                .execute(
                    "SELECT * FROM lix_key_value WHERE key = 'authority-local-only'",
                    &[],
                )
                .await
                .expect("rejected untracked row absence should query")
                .rows()
                .is_empty()
        );
    }

    #[tokio::test]
    #[ignore = "lix_change joins are server-first history in v3"]
    async fn consecutive_file_deltas_reach_an_active_joined_content_observer() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/remote.md".to_owned()),
                    Value::Text(String::new()),
                ],
            )
            .await
            .expect("remote file create should commit");
        let created = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("file-create delta should load");
        let SyncRepositoryPullResponse::Delta {
            cursor: created_cursor,
            events: _,
        } = &created
        else {
            panic!("file-create pull should be a delta");
        };
        hydrate_delta_blobs(&authority, &replica, &created).await;
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &created)
            .await
            .expect("replica should apply file create");

        let file_id = authority
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text("/remote.md".to_owned())],
            )
            .await
            .expect("remote file id should load")
            .rows()[0]
            .get::<String>("id")
            .expect("remote file id should decode");
        let params = [
            Value::Text(file_id.clone()),
            Value::Text(default_head(&snapshot).0),
        ];
        let mut observed = replica
            .observe(
                "SELECT file.id AS id, file.content AS content, file.path AS path, \
                 file.lixcol_change_id AS change_id, change.origin_key AS origin_key, \
                 $2 AS active_branch_id FROM lix_file AS file \
                 LEFT JOIN lix_change AS change ON change.id = file.lixcol_change_id \
                 AND change.file_id = file.id WHERE file.id = $1",
                &params,
            )
            .expect("joined Atelier file observer should open");
        let initial = observed
            .next()
            .await
            .expect("initial file observation should succeed")
            .expect("initial file observation should exist");
        assert_eq!(
            initial.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("initial content should decode"),
            Vec::<u8>::new(),
        );

        authority
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE id = $2",
                &[
                    Value::Text("# right-marker\n".to_owned()),
                    Value::Text(file_id),
                ],
            )
            .await
            .expect("remote editor write should commit");
        let edited = authority
            .pull_sync_repository(Some(*created_cursor), 128)
            .await
            .expect("file-edit delta should load");
        hydrate_delta_blobs(&authority, &replica, &edited).await;
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &edited)
            .await
            .expect("replica should apply file edit");

        let next = tokio::time::timeout(Duration::from_millis(100), observed.next())
            .await
            .expect("file edit should wake the active observer")
            .expect("file edit observation should succeed")
            .expect("file edit observation should exist");
        assert_eq!(
            next.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("updated content should decode"),
            b"# right-marker\n",
        );
    }

    #[tokio::test]
    async fn empty_sync_delta_does_not_scan_or_open_a_write_transaction() {
        let storage = SyncAccountingStorage::default();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        replica
            .store_replica_state(
                TEST_REMOTE,
                SyncReplicaState {
                    active_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                    cursor: 0,
                    authoritative_branches: BTreeMap::from([(
                        receipt.main_branch_id,
                        AuthoritativeBranchCoordinate::Headed {
                            head_commit_id: receipt.initial_commit_id.clone(),
                            checkpoint_commit_id: receipt.initial_commit_id,
                        },
                    )]),
                    certified_branch_roots: BTreeMap::new(),
                    snapshot_certified_cursor: None,
                    pending_resets: BTreeMap::new(),
                    authority_known_commit_ids: BTreeSet::new(),
                },
            )
            .await
            .expect("replica cursor should initialize");
        storage.reset();

        replica
            .apply_sync_repository_pull(
                TEST_REMOTE,
                &SyncRepositoryPullResponse::Delta {
                    cursor: 0,
                    events: Vec::new(),
                },
            )
            .await
            .expect("an unchanged heartbeat should be accepted");

        assert_eq!(
            storage.forbidden_operation_count(),
            0,
            "an unchanged heartbeat must not scan branches or open a write transaction",
        );
    }

    #[tokio::test]
    async fn delta_ref_and_cursor_publish_atomically_without_post_scan() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let (branch_id, _) = default_head(&snapshot);
        let (history, rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        let storage = SyncAccountingStorage::default();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect("snapshot should initialize replica");

        write_key_value(&authority, "scan-free-delta", "after").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("authority delta should load");
        let SyncRepositoryPullResponse::Delta {
            cursor: delta_cursor,
            events,
        } = &delta
        else {
            panic!("incremental pull should return a delta");
        };
        let [event] = events.as_slice() else {
            panic!("one authority transaction should publish one event");
        };
        let [update] = event.ref_updates.as_slice() else {
            panic!("the authority write should publish one ref update");
        };
        let expected_head = update
            .head_commit_id
            .clone()
            .expect("the updated branch should remain headed");
        replica
            .import_sync_repository(
                &SyncPushRequest {
                    commits: event.commits.clone(),
                    ref_updates: Vec::new(),
                    inline_blobs: event.inline_blobs.clone(),
                },
                SyncImportPurpose::ReplicaDelta,
                None,
                None,
            )
            .await
            .expect("fixture should preload immutable commit bodies");
        storage.reset();
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("replica should apply remote delta");

        assert_eq!(
            storage.branch_control_scan_count(),
            0,
            "the atomic event receipt needs no post-publication convergence scan",
        );
        assert_eq!(
            storage.write_transaction_count(),
            1,
            "a ready ref event should need one storage publication",
        );
        assert_eq!(
            storage.atomic_ref_receipt_count(),
            1,
            "the branch ref and replica receipt must share that publication",
        );
        assert_eq!(
            storage.branch_publication_without_receipt_count(),
            0,
            "the imported ref must never become visible without its cursor",
        );
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("atomic publication read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("published branch should load")
            .expect("published branch should exist");
        let stored_state = load_replica_state(&read)
            .await
            .expect("published receipt should load")
            .0
            .expect("published receipt should exist");
        assert_eq!(control.head_commit_id.to_string(), expected_head);
        assert_eq!(stored_state.cursor, *delta_cursor);
    }

    #[tokio::test]
    async fn replica_public_mutations_fail_before_creating_local_state() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "authority-only", "before").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let (_, initial_head) = default_head(&snapshot);

        let second_engine = open_lix()
            .with_storage(replica.storage_adapter().storage().clone())
            .await
            .expect("a second plain engine should open the same replica storage");
        let second_error = second_engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('second-engine', 'forbidden')",
                &[],
            )
            .await
            .expect_err("the durable receipt must fence a process-local disabled role");
        assert_eq!(second_error.code, "LIX_REPLICA_CACHE_READ_ONLY");
        assert!(
            second_engine
                .execute(
                    "SELECT * FROM lix_key_value WHERE key = 'second-engine'",
                    &[]
                )
                .await
                .expect("fenced row absence should query")
                .rows()
                .is_empty()
        );

        let error = replica
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('local-only', 'forbidden')",
                &[],
            )
            .await
            .expect_err("replica SQL writes must be authority-routed");
        assert_eq!(error.code, super::super::AUTHORITY_EXECUTION_REQUIRED_CODE);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details["executionKind"].as_str()),
            Some("mutation"),
        );
        let checkpoint_error = replica
            .create_checkpoint()
            .await
            .expect_err("replica checkpoints must be authority-routed");
        assert_eq!(
            checkpoint_error.code,
            super::super::AUTHORITY_EXECUTION_REQUIRED_CODE
        );
        assert_eq!(
            current_branch_head(&replica).await,
            CommitId::parse_lix(&initial_head, "initial replica head").expect("head parses"),
        );
        let local_rows = replica
            .execute(
                "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'local-only'",
                &[],
            )
            .await
            .expect("certified hot reads remain local");
        assert_eq!(
            local_rows.rows()[0]
                .get::<i64>("count")
                .expect("count decodes"),
            0,
        );
    }

    #[tokio::test]
    async fn one_delta_page_folds_a_same_branch_chain_into_one_receipt_publication() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let storage = SyncAccountingStorage::default();
        let replica =
            accounting_replica_from_snapshot(&authority, &snapshot, storage.clone()).await;
        let params = [Value::Text("folded-chain".to_owned())];
        replica
            .set_sync_role(super::super::SyncRole::Disabled)
            .expect("local observer fixture should bypass public replica routing");
        let mut observed = replica
            .observe("SELECT value FROM lix_key_value WHERE key = $1", &params)
            .expect("observe folded value");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should be restored before applying the delta");
        observed
            .next()
            .await
            .expect("initial observer evaluation succeeds")
            .expect("initial observer event exists");

        write_key_value(&authority, "folded-chain", "first").await;
        write_key_value(&authority, "folded-chain", "final").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("two-event delta should load");
        let SyncRepositoryPullResponse::Delta { events, cursor, .. } = &delta else {
            panic!("incremental pull should be a delta");
        };
        assert_eq!(events.len(), 2, "fixture must cross two authority events");
        storage.reset();

        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("folded page should apply");

        assert_eq!(
            storage.write_transaction_count(),
            2,
            "all immutable bodies share one publication and the final ref/receipt share one publication",
        );
        assert_eq!(storage.atomic_ref_receipt_count(), 1);
        assert_eq!(storage.branch_publication_without_receipt_count(), 0);
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("cursor should load"),
            Some(*cursor),
        );
        assert_eq!(read_key_value(&replica, "folded-chain").await, "final");
        let event = observed
            .next()
            .await
            .expect("folded observer evaluation succeeds")
            .expect("folded observer event exists");
        assert_eq!(
            event.rows.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("observed value decodes"),
            serde_json::json!("final"),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), observed.next())
                .await
                .is_err(),
            "one folded page should emit one observer delivery",
        );
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn direct_file_fast_forwards_publish_once_and_fail_atomically() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let (branch_id, _) = default_head(&snapshot);
        let storage = SyncAccountingStorage::default();
        let replica =
            accounting_replica_from_snapshot(&authority, &snapshot, storage.clone()).await;
        let params = [Value::Text("/atomic-fast-forward.md".to_owned())];
        let mut observed = replica
            .observe("SELECT content FROM lix_file WHERE path = $1", &params)
            .expect("file observer should open");
        assert!(
            observed
                .next()
                .await
                .expect("initial observation should succeed")
                .expect("initial observation should exist")
                .rows
                .rows()
                .is_empty()
        );

        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/atomic-fast-forward.md".to_owned()),
                    Value::Text("created".to_owned()),
                ],
            )
            .await
            .expect("authority file create should commit");
        let created = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("file create delta should load");
        assert!(matches!(
            &created,
            SyncRepositoryPullResponse::Delta { events, .. }
                if !events[0].inline_blobs.is_empty()
        ));
        storage.reset();
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &created)
            .await
            .expect("file create should apply");
        assert_eq!(storage.write_transaction_count(), 1);
        assert_eq!(storage.atomic_ref_receipt_count(), 1);
        let created_event = observed
            .next()
            .await
            .expect("created file observation should succeed")
            .expect("created file observation should exist");
        assert_eq!(
            created_event.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("created content should decode"),
            b"created",
        );
        let SyncRepositoryPullResponse::Delta {
            cursor: created_cursor,
            events: created_events,
            ..
        } = &created
        else {
            panic!("file create should produce a delta");
        };
        let created_head = created_events[0].ref_updates[0]
            .head_commit_id
            .clone()
            .expect("created branch should remain headed");

        authority
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE path = $2",
                &[
                    Value::Text("edited".to_owned()),
                    Value::Text("/atomic-fast-forward.md".to_owned()),
                ],
            )
            .await
            .expect("authority file edit should commit");
        let edited = authority
            .pull_sync_repository(Some(*created_cursor), 128)
            .await
            .expect("file edit delta should load");
        storage.reset();
        storage.fail_next_replica_receipt();
        let SyncRepositoryPullResponse::Delta {
            cursor: edited_cursor,
            events,
        } = &edited
        else {
            panic!("file edit should produce a delta");
        };
        let edited_commit_id =
            CommitId::parse_lix(&events[0].commits[0].commit_id, "edited fixture commit")
                .expect("edited commit id should parse");
        let edited_blob_id = events[0].inline_blobs[0].blob_id.clone();
        let error = replica
            .apply_sync_repository_pull(TEST_REMOTE, &edited)
            .await
            .expect_err("injected publication failure should reject the whole fast-forward");
        assert!(error.message.contains("injected replica receipt failure"));
        assert_eq!(storage.write_transaction_count(), 1);
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("old cursor should remain readable"),
            Some(*created_cursor),
        );
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("atomicity read should open");
        assert!(
            load_commit_record(&read, edited_commit_id)
                .await
                .expect("failed commit lookup should succeed")
                .is_none(),
            "a failed atomic publication must not leak the immutable commit",
        );
        drop(read);
        assert!(
            !replica
                .has_sync_blob_manifest(&edited_blob_id)
                .await
                .expect("failed inline manifest lookup should succeed"),
            "a failed atomic publication must not leak the inline blob",
        );
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("atomicity control read should reopen");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("branch control should load")
            .expect("branch control should exist");
        assert_eq!(control.head_commit_id.to_string(), created_head);
        drop(read);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), observed.next())
                .await
                .is_err(),
            "a failed atomic publication must not notify the warm observer",
        );

        storage.reset();
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &edited)
            .await
            .expect("replaying the file edit should apply");
        assert_eq!(storage.write_transaction_count(), 1);
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("edited cursor should load"),
            Some(*edited_cursor),
        );
        let edited_event = observed
            .next()
            .await
            .expect("edited file observation should succeed")
            .expect("edited file observation should exist");
        assert_eq!(
            edited_event.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("edited content should decode"),
            b"edited",
        );

        authority
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/atomic-fast-forward.md".to_owned())],
            )
            .await
            .expect("authority file delete should commit");
        let deleted = authority
            .pull_sync_repository(Some(*edited_cursor), 128)
            .await
            .expect("file delete delta should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &deleted)
            .await
            .expect("file delete should apply atomically");
        assert!(
            observed
                .next()
                .await
                .expect("deleted file observation should succeed")
                .expect("deleted file observation should exist")
                .rows
                .rows()
                .is_empty()
        );
        let SyncRepositoryPullResponse::Delta {
            cursor: deleted_cursor,
            ..
        } = &deleted
        else {
            panic!("file delete should produce a delta");
        };

        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/atomic-fast-forward.md".to_owned()),
                    Value::Text("recreated".to_owned()),
                ],
            )
            .await
            .expect("authority file recreate should commit");
        let recreated = authority
            .pull_sync_repository(Some(*deleted_cursor), 128)
            .await
            .expect("file recreate delta should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &recreated)
            .await
            .expect("tombstoned file recreate should apply atomically");
        let recreated_event = observed
            .next()
            .await
            .expect("recreated file observation should succeed")
            .expect("recreated file observation should exist");
        assert_eq!(
            recreated_event.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("recreated content should decode"),
            b"recreated",
        );
        let SyncRepositoryPullResponse::Delta {
            cursor: recreated_cursor,
            ..
        } = &recreated
        else {
            panic!("file recreate should produce a delta");
        };
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST('' AS BYTEA))",
                &[Value::Text("/empty-fast-forward.md".to_owned())],
            )
            .await
            .expect("empty authority file should commit");
        let empty = authority
            .pull_sync_repository(Some(*recreated_cursor), 128)
            .await
            .expect("empty file delta should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = &empty else {
            panic!("empty file should produce a delta");
        };
        assert!(
            events[0].inline_blobs.is_empty(),
            "an intrinsic empty file references no CAS blob",
        );
        storage.reset();
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &empty)
            .await
            .expect("empty inline file should apply atomically");
        assert_eq!(
            storage.write_transaction_count(),
            1,
            "empty file commit, head, and receipt publish once without a blob lane",
        );
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn inline_blob_admission_is_referenced_authenticated_and_atomic() {
        let authority_storage = SyncAccountingStorage::default();
        Engine::initialize(authority_storage.clone())
            .await
            .expect("authority storage should initialize");
        let authority = open_lix()
            .with_storage(authority_storage.clone())
            .await
            .expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot else {
            panic!("fixture should start from a snapshot");
        };
        let replica = replica_from_snapshot(
            &authority,
            &authority
                .pull_sync_repository(None, 1)
                .await
                .expect("replica snapshot should load"),
        )
        .await;
        replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/inline-admission.md".to_owned()),
                    Value::Text("authenticated inline payload".to_owned()),
                ],
            )
            .await
            .expect("replica file write should commit");
        let request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("inline push should build")
            .expect("file write should be pending");
        assert_eq!(request.inline_blobs.len(), 1);
        let commit_id = CommitId::parse_lix(&request.commits[0].commit_id, "inline fixture commit")
            .expect("fixture commit id should parse");

        let mut unreferenced = request.clone();
        let unrelated = crate::binary_cas::CanonicalBlobManifest::from_bytes(b"unrelated");
        unreferenced.inline_blobs[0] = SyncBlobManifest {
            blob_id: unrelated.blob_id.to_hex(),
            size_bytes: unrelated.size_bytes,
            chunks: unrelated
                .chunks
                .iter()
                .map(|chunk| super::super::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: Some(
                base64::engine::general_purpose::STANDARD.encode(b"unrelated"),
            ),
        };
        let error = authority
            .push_sync_repository(&unreferenced)
            .await
            .expect_err("unreferenced inline blob must be rejected");
        assert!(error.message.contains("unreferenced inline blob"));

        let mut tampered = request.clone();
        tampered.inline_blobs[0].inline_bytes_base64 =
            Some(base64::engine::general_purpose::STANDARD.encode(b"tampered inline payload"));
        authority
            .push_sync_repository(&tampered)
            .await
            .expect_err("tampered inline blob must be rejected");
        assert_eq!(
            authority
                .pull_sync_repository(Some(cursor), 1)
                .await
                .expect("unchanged authority delta should load"),
            SyncRepositoryPullResponse::Delta {
                cursor,
                events: Vec::new(),
            },
        );
        let read = authority
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("atomicity read should open");
        assert!(
            load_commit_record(&read, commit_id)
                .await
                .expect("rejected commit lookup should succeed")
                .is_none()
        );
        drop(read);

        authority_storage.reset();
        authority
            .push_sync_repository(&request)
            .await
            .expect("authenticated referenced inline blob should publish");
        assert_eq!(
            authority_storage.write_transaction_count(),
            1,
            "authority must publish inline CAS, commit, ref, and event once",
        );
        let delta = authority
            .pull_sync_repository(Some(cursor), 1)
            .await
            .expect("published inline delta should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = delta else {
            panic!("published response should be a delta");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].inline_blobs, request.inline_blobs);
        let history = authority
            .execute(
                "SELECT path, content FROM lix_history('lix_file') WHERE path = $1",
                &[Value::Text("/inline-admission.md".to_owned())],
            )
            .await
            .expect("public file history query should remain readable after inline sync");
        assert!(
            history.rows().iter().any(|row| {
                row.get::<Vec<u8>>("content")
                    .is_ok_and(|content| content == b"authenticated inline payload")
            }),
            "public file history should expose the synced commit",
        );
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn large_blob_inline_discovery_is_metadata_only_for_push_and_pull() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("replica snapshot should load");
        let replica_storage = SyncAccountingStorage::default();
        let replica =
            accounting_replica_from_snapshot(&authority, &snapshot, replica_storage.clone()).await;
        replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text("/large-local.bin".to_owned()),
                    Value::Blob(vec![7_u8; 65 * 1024].into()),
                ],
            )
            .await
            .expect("large local file should commit");
        replica_storage.reset();
        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("large push should build")
            .expect("large file should be pending");
        assert!(push.inline_blobs.is_empty());
        assert_eq!(
            replica_storage.write_transaction_count(),
            0,
            "large outbound discovery must not canonicalize or publish transfer chunks",
        );

        let authority_storage = SyncAccountingStorage::default();
        let receipt = Engine::initialize(authority_storage.clone())
            .await
            .expect("accounted authority storage should initialize");
        let accounted_authority = open_lix()
            .with_storage(authority_storage.clone())
            .await
            .expect("accounted authority should open");
        accounted_authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("accounted authority role should install");
        accounted_authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text("/large-authority.bin".to_owned()),
                    Value::Blob(vec![9_u8; 65 * 1024].into()),
                ],
            )
            .await
            .expect("large authority file should commit");
        authority_storage.reset();
        let delta = accounted_authority
            .pull_sync_repository(Some(0), 1)
            .await
            .expect("large authority event should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = delta else {
            panic!("large authority pull should be a delta");
        };
        assert_eq!(events.len(), 1);
        assert!(events[0].inline_blobs.is_empty());
        assert_eq!(
            authority_storage.write_transaction_count(),
            0,
            "large inbound discovery must not canonicalize or publish transfer chunks",
        );
        assert_eq!(
            receipt.main_branch_id,
            accounted_authority
                .active_branch_id()
                .await
                .expect("accounted authority branch should load"),
        );
    }

    #[tokio::test]
    async fn one_delta_page_folds_independent_branch_updates() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let main_branch_id = authority
            .active_branch_id()
            .await
            .expect("main branch should load");
        let secondary = authority
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000001777".to_owned()),
                name: "folded-secondary".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("secondary branch should be created");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("branch-complete snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let storage = SyncAccountingStorage::default();
        let replica =
            accounting_replica_from_snapshot(&authority, &snapshot, storage.clone()).await;

        authority
            .switch_branch(SwitchBranchOptions {
                branch_id: secondary.id.clone(),
            })
            .await
            .expect("authority should select secondary branch");
        write_key_value(&authority, "secondary-fold", "secondary").await;
        authority
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id.clone(),
            })
            .await
            .expect("authority should restore main branch");
        write_key_value(&authority, "main-fold", "main").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("independent branch delta should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = &delta else {
            panic!("incremental pull should be a delta");
        };
        assert_eq!(
            events.len(),
            3,
            "fixture contains the stale secondary checkout refresh plus one user event per branch"
        );
        storage.reset();

        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("independent branch page should apply");

        assert_eq!(
            storage.write_transaction_count(),
            2,
            "the atomic page publication is followed by one local composite-base refresh",
        );
        assert_eq!(storage.atomic_ref_receipt_count(), 1);
        assert_eq!(read_key_value(&replica, "main-fold").await, "main");
        replica
            .switch_branch(SwitchBranchOptions {
                branch_id: secondary.id,
            })
            .await
            .expect("replica should select synchronized secondary branch");
        assert_eq!(
            read_key_value(&replica, "secondary-fold").await,
            "secondary",
        );
    }

    #[tokio::test]
    async fn durable_replica_fence_makes_pre_v6_divergence_unconstructable() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        write_key_value(&authority, "divergent-base", "base").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let storage = SyncAccountingStorage::default();
        let replica =
            accounting_replica_from_snapshot(&authority, &snapshot, storage.clone()).await;
        replica
            .set_sync_role(super::super::SyncRole::Disabled)
            .expect("the process-local role can be stale without weakening the durable fence");
        let error = replica
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('local-divergent', 'local')",
                &[],
            )
            .await
            .expect_err("a durable authority receipt must make local divergence impossible");
        assert_eq!(error.code, "LIX_REPLICA_CACHE_READ_ONLY");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("test restores replica role");
        write_key_value(&authority, "remote-divergent-a", "remote-a").await;
        write_key_value(&authority, "remote-divergent-b", "remote-b").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("divergent page should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = &delta else {
            panic!("incremental pull should be a delta");
        };
        assert_eq!(events.len(), 2);

        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("the still-certified replica should accept authority state");
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("new cursor should be readable"),
            Some(match delta {
                SyncRepositoryPullResponse::Delta { cursor, .. } => cursor,
                SyncRepositoryPullResponse::Snapshot { .. } => unreachable!(),
            }),
            "the authority receipt advances only with the certified page",
        );
        assert!(
            replica
                .execute(
                    "SELECT * FROM lix_key_value WHERE key = 'local-divergent'",
                    &[],
                )
                .await
                .expect("the rejected row absence should query")
                .rows()
                .is_empty()
        );
        assert_eq!(
            read_key_value(&replica, "remote-divergent-b").await,
            "remote-b"
        );
    }

    fn default_head(snapshot: &SyncRepositoryPullResponse) -> (String, String) {
        let SyncRepositoryPullResponse::Snapshot {
            default_branch_id,
            branches,
            ..
        } = snapshot
        else {
            panic!("initial pull must be a snapshot");
        };
        let head = branches
            .iter()
            .find(|branch| &branch.branch_id == default_branch_id)
            .and_then(|branch| branch.head_commit_id.clone())
            .expect("default branch must have a head");
        (default_branch_id.clone(), head)
    }

    fn authoritative_coordinates(
        branches: &[SyncBranchHead],
    ) -> BTreeMap<String, AuthoritativeBranchCoordinate> {
        branches
            .iter()
            .map(|branch| {
                (
                    branch.branch_id.clone(),
                    AuthoritativeBranchCoordinate::from_wire(
                        branch.head_commit_id.clone(),
                        branch.checkpoint_commit_id.clone(),
                        "test snapshot branch",
                    )
                    .expect("snapshot branch coordinate should be complete"),
                )
            })
            .collect()
    }

    async fn snapshot_parts<AuthorityStorage>(
        authority: &Lix<AuthorityStorage>,
        snapshot: &SyncRepositoryPullResponse,
    ) -> (
        SyncHistoryResponse,
        Vec<SyncSnapshotRow>,
        BTreeMap<String, String>,
    )
    where
        AuthorityStorage: Storage + Clone + Send + Sync + 'static,
    {
        let SyncRepositoryPullResponse::Snapshot { branches, .. } = snapshot else {
            unreachable!("initial pull is a snapshot");
        };
        let mut commits = BTreeMap::new();
        let mut headers = BTreeMap::new();
        let mut boundaries = BTreeMap::new();
        let snapshot_commits = branches
            .iter()
            .flat_map(|branch| {
                [
                    branch.head_commit_id.as_deref(),
                    branch.checkpoint_commit_id.as_deref(),
                ]
            })
            .flatten()
            .collect::<BTreeSet<_>>();
        for head in snapshot_commits {
            let page = authority
                .sync_history(head, 1)
                .await
                .expect("snapshot head history should load");
            commits.extend(
                page.commits
                    .into_iter()
                    .map(|commit| (commit.commit_id.clone(), commit)),
            );
            headers.extend(
                page.commit_headers
                    .into_iter()
                    .map(|header| (header.commit_id.clone(), header)),
            );
            boundaries.extend(
                page.boundaries
                    .into_iter()
                    .map(|boundary| (boundary.commit_id.clone(), boundary)),
            );
        }
        let checkpoint_roots = branches
            .iter()
            .filter_map(|branch| {
                let head = branch.head_commit_id.as_deref()?;
                let checkpoint = branch.checkpoint_commit_id.as_deref()?;
                if head == checkpoint {
                    return None;
                }
                Some((
                    checkpoint.to_owned(),
                    branch.checkpoint_state_root_id.clone(),
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let history = SyncHistoryResponse {
            commits: commits.into_values().collect(),
            commit_headers: headers.into_values().collect(),
            boundaries: boundaries.into_values().collect(),
        };
        let mut rows = Vec::new();
        for branch in branches {
            let Some(head) = branch.head_commit_id.as_deref() else {
                continue;
            };
            let mut continuation = None;
            loop {
                let page = authority
                    .pull_sync_snapshot_rows(
                        &branch.branch_id,
                        head,
                        continuation.as_deref(),
                        super::super::MAX_SYNC_REQUEST_ITEMS,
                    )
                    .await
                    .expect("snapshot row page should load");
                rows.extend(page.rows);
                let Some(next) = page.continuation else {
                    break;
                };
                continuation = Some(next);
            }
        }
        let checkpoint_targets = branches
            .iter()
            .filter_map(|branch| {
                let head = branch.head_commit_id.as_deref()?;
                let checkpoint = branch.checkpoint_commit_id.as_deref()?;
                (head != checkpoint).then_some(checkpoint)
            })
            .collect::<BTreeSet<_>>();
        for checkpoint in checkpoint_targets {
            let mut continuation = None;
            loop {
                let page = authority
                    .pull_sync_snapshot_rows(
                        checkpoint,
                        checkpoint,
                        continuation.as_deref(),
                        super::super::MAX_SYNC_REQUEST_ITEMS,
                    )
                    .await
                    .expect("checkpoint snapshot row page should load");
                rows.extend(page.rows);
                let Some(next) = page.continuation else {
                    break;
                };
                continuation = Some(next);
            }
        }
        (history, rows, checkpoint_roots)
    }

    fn install_publication_fence_responder_for_test<StorageImpl>(lix: &mut Lix<StorageImpl>)
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(8);
        lix.set_sync_demand_sender_for_test(demand_tx);
        tokio::spawn(async move {
            while let Some(demand) = demand_rx.recv().await {
                assert!(
                    demand.is_publication_barrier_for_test(),
                    "the synthetic replica fixture only answers publication barriers",
                );
                demand.succeed_for_test();
            }
        });
    }

    async fn replica_from_snapshot<AuthorityStorage>(
        authority: &Lix<AuthorityStorage>,
        snapshot: &SyncRepositoryPullResponse,
    ) -> Lix<Memory>
    where
        AuthorityStorage: Storage + Clone + Send + Sync + 'static,
    {
        let (branch_id, _) = default_head(snapshot);
        let (history, rows, checkpoint_roots) = snapshot_parts(authority, snapshot).await;
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let mut replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect("snapshot should initialize replica");
        install_publication_fence_responder_for_test(&mut replica);
        replica
    }

    #[tokio::test]
    async fn concurrent_initial_snapshots_publish_one_repository_binding() {
        let authority = open_lix().await.expect("authority should open");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (branch_id, _) = default_head(&snapshot);
        let (history, rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        let storage = SyncAccountingStorage::default();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let first = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("first opener should open");
        let second = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("second opener should open");
        for opener in [&first, &second] {
            opener
                .set_sync_role(super::super::SyncRole::Replica)
                .expect("replica role should install");
        }

        storage.gate_bootstrap_commits(2);
        let (first_result, second_result) = tokio::join!(
            first.try_install_initial_sync_snapshot(
                "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000003",
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            ),
            second.try_install_initial_sync_snapshot(
                "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000004",
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            ),
        );

        let results = [first_result, second_result];
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Ok(InitialSyncSnapshotInstall::Installed)))
                .count(),
            1,
            "exactly one bootstrap may claim an unbound replica"
        );
        let loser = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .expect("the competing bootstrap should lose atomically");
        assert_eq!(loser.code, LixError::CODE_TRANSACTION_CONFLICT);

        let serialized_loser = if matches!(results[0], Ok(InitialSyncSnapshotInstall::Installed)) {
            second
                .try_install_initial_sync_snapshot(
                    "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000004",
                    crate::ANONYMOUS_ACCOUNT_ID,
                    &snapshot,
                    &history.commits,
                    &history.commit_headers,
                    &rows,
                    &checkpoint_roots,
                )
                .await
        } else {
            first
                .try_install_initial_sync_snapshot(
                    "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000003",
                    crate::ANONYMOUS_ACCOUNT_ID,
                    &snapshot,
                    &history.commits,
                    &history.commit_headers,
                    &rows,
                    &checkpoint_roots,
                )
                .await
        }
        .expect("the coherent binding scan should recognize the same repository");
        assert_eq!(
            serialized_loser,
            InitialSyncSnapshotInstall::ExistingRepository
        );

        let adapter = first.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("published replica state should be readable");
        let mut cursor = read
            .begin_scan(
                SYNC_REPLICA_STATE_SPACE,
                StoragePrefix {
                    bytes: Bytes::new(),
                }
                .to_range()
                .expect("full replica-state range should be valid"),
                StorageBeginScanOptions::default(),
            )
            .await
            .expect("replica-state scan should open");
        assert_eq!(
            cursor
                .next_page(2)
                .await
                .expect("replica-state scan should complete")
                .len(),
            1,
            "the repository must have one binding owner"
        );
    }

    async fn hydrate_delta_blobs<AuthorityStorage, ReplicaStorage>(
        authority: &Lix<AuthorityStorage>,
        replica: &Lix<ReplicaStorage>,
        response: &SyncRepositoryPullResponse,
    ) where
        AuthorityStorage: Storage + Clone + Send + Sync + 'static,
        ReplicaStorage: Storage + Clone + Send + Sync + 'static,
    {
        let SyncRepositoryPullResponse::Delta { events, .. } = response else {
            panic!("blob hydration requires a delta");
        };
        let blob_ids = events
            .iter()
            .flat_map(|event| &event.commits)
            .flat_map(|commit| &commit.members)
            .filter(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
            .filter_map(|member| {
                member
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.get("blob_hash"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<BTreeSet<_>>();
        for blob_id in blob_ids {
            let manifest = authority
                .get_sync_blob_manifest(blob_id)
                .await
                .expect("delta blob manifest should load")
                .expect("delta blob manifest should exist");
            replica
                .register_deferred_sync_blob_manifest(&manifest)
                .await
                .expect("delta blob should hydrate inline");
        }
    }

    async fn accounting_replica_from_snapshot(
        authority: &Lix<Memory>,
        snapshot: &SyncRepositoryPullResponse,
        storage: SyncAccountingStorage,
    ) -> Lix<SyncAccountingStorage> {
        let (branch_id, _) = default_head(snapshot);
        let (history, rows, checkpoint_roots) = snapshot_parts(authority, snapshot).await;
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let mut replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect("snapshot should initialize replica");
        install_publication_fence_responder_for_test(&mut replica);
        replica
    }

    async fn write_key_value<StorageImpl>(lix: &Lix<StorageImpl>, key: &str, value: &str)
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
        )
        .await
        .expect("key-value write should commit");
    }

    async fn read_key_value<StorageImpl>(lix: &Lix<StorageImpl>, key: &str) -> String
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let value = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.to_owned())],
            )
            .await
            .expect("key-value read should succeed")
            .rows()
            .first()
            .expect("key-value row should exist")
            .get::<Value>("value")
            .expect("key-value value should decode");
        match value {
            Value::Jsonb(value) => value
                .as_json_string()
                .expect("key-value JSON should contain a string"),
            Value::Text(value) => value,
            other => panic!("unexpected key-value representation: {other:?}"),
        }
    }

    async fn current_branch_head<StorageImpl>(lix: &Lix<StorageImpl>) -> String
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        lix.execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("active branch head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("active branch head should decode")
    }

    async fn working_diff_count(lix: &Lix<Memory>) -> i64 {
        let branch_id = lix
            .active_branch_id()
            .await
            .expect("active branch should remain readable");
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch control read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("active branch control should remain readable")
            .expect("active branch control should exist");
        TrackedHeadContext::new()
            .reader(&read)
            .working_diff_for_control(&branch_id, control, &TrackedStateDiffRequest::default())
            .await
            .expect("working diff should remain readable")
            .expect("certified working diff should be installed")
            .diff
            .entries
            .len() as i64
    }

    async fn read_file_content(lix: &Lix<Memory>, path: &str) -> Value {
        lix.execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("file content should remain readable")
        .rows()[0]
            .get::<Value>("content")
            .expect("file content should decode")
    }

    async fn publish_pending(replica: &Lix<Memory>, authority: &Lix<Memory>) -> SyncPushRequest {
        let request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("pending push should build")
            .expect("replica should have pending work");
        authority
            .push_sync_repository(&request)
            .await
            .expect("authority should accept pending work");
        request
    }

    async fn hydrate_history_commit(
        authority: &Lix<Memory>,
        replica: &Lix<Memory>,
        commit_id: &str,
    ) {
        let page = authority
            .sync_history(commit_id, 1)
            .await
            .expect("history page should load");
        let mut boundary_rows = Vec::new();
        for boundary in &page.boundaries {
            let rows = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("history boundary rows should load");
            assert_eq!(rows.continuation, None);
            boundary_rows.extend(rows.rows);
        }
        replica
            .import_sync_history_headers(&page.commit_headers)
            .await
            .expect("history headers should import");
        replica
            .import_sync_history_boundaries(&page.commits, &page.boundaries, &boundary_rows)
            .await
            .expect("history bodies should import");
    }

    async fn transfer_commit_blobs(
        source: &Lix<Memory>,
        target: &Lix<Memory>,
        commits: &[SyncCommit],
    ) {
        let blob_ids = commits
            .iter()
            .flat_map(|commit| commit.members.iter())
            .filter(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
            .filter_map(|member| member.snapshot.as_ref())
            .filter_map(|snapshot| snapshot["blob_hash"].as_str().map(str::to_owned))
            .collect::<BTreeSet<_>>();
        for blob_id in blob_ids {
            let manifest = source
                .get_sync_blob_manifest(&blob_id)
                .await
                .expect("outbound manifest should load")
                .expect("outbound manifest should exist");
            for chunk in &manifest.chunks {
                let bytes = source
                    .get_sync_chunk(&chunk.chunk_id)
                    .await
                    .expect("outbound chunk should load")
                    .expect("outbound chunk should exist");
                target
                    .put_sync_chunk(&chunk.chunk_id, &bytes)
                    .await
                    .expect("authority should accept chunk");
            }
            target
                .register_sync_blob_manifest(&manifest)
                .await
                .expect("authority should accept manifest");
        }
    }

    async fn publish_pending_with_blobs(
        replica: &Lix<Memory>,
        authority: &Lix<Memory>,
    ) -> SyncPushRequest {
        let request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("pending push should build")
            .expect("replica should have pending work");
        transfer_commit_blobs(replica, authority, &request.commits).await;
        authority
            .push_sync_repository(&request)
            .await
            .expect("authority should accept pending work");
        request
    }

    async fn assert_root_parent_is_complete(lix: &Lix<Memory>, commit_id: &str) {
        let commit_id = CommitId::parse_lix(commit_id, "test commit").expect("valid commit id");
        let read = lix
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test read should open");
        let record = load_commit_record(&read, commit_id)
            .await
            .expect("record should load")
            .expect("record should exist");
        let manifest = load_commit_state_manifest(&read, commit_id)
            .await
            .expect("manifest should load")
            .expect("manifest should exist");
        if let Some(root) = manifest.snapshot_root {
            assert_eq!(
                root.parent_roots.first().map(|parent| parent.commit_id),
                record.parent_commit_ids.first().copied(),
                "root metadata must retain the changelog first parent for {commit_id}",
            );
        }
    }

    #[tokio::test]
    async fn ordinary_authority_transaction_publishes_the_canonical_stored_commit() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let before = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = before else {
            panic!("initial pull should be a snapshot");
        };

        write_key_value(&authority, "authority-preflight", "canonical").await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 1)
            .await
            .expect("ordinary authority event should be pullable");
        let SyncRepositoryPullResponse::Delta { events, .. } = delta else {
            panic!("cursor pull should be a delta");
        };
        let event = events
            .first()
            .expect("ordinary transaction should emit one event");
        assert_eq!(event.commits.len(), 1);
        let stored = export_sync_commit(&authority, &event.commits[0].commit_id)
            .await
            .expect("stored commit should export")
            .expect("event commit should exist");
        assert_eq!(
            event.commits[0], stored,
            "preflight and post-commit export must share one canonical codec",
        );
    }

    #[tokio::test]
    async fn authority_restore_publishes_a_ref_event_and_converges_a_replica() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        write_key_value(&authority, "restore-sync", "target").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("target snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let (branch_id, target_head) = default_head(&snapshot);
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        write_key_value(&authority, "restore-sync", "abandoned").await;
        let abandoned_head = authority
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("abandoned head should read")
            .rows()[0]
            .get::<String>("id")
            .expect("abandoned head should decode");
        authority
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(target_head.clone())],
            )
            .await
            .expect("ancestor restore should succeed");

        let delta = authority
            .pull_sync_repository(Some(cursor), 16)
            .await
            .expect("write and restore events should be pullable");
        let SyncRepositoryPullResponse::Delta { events, .. } = &delta else {
            panic!("cursor pull should be a delta");
        };
        assert!(events.iter().any(|event| {
            event.ref_updates.iter().any(|update| {
                update.branch_id == branch_id
                    && update.expected_head_commit_id.as_deref() == Some(abandoned_head.as_str())
                    && update.head_commit_id.as_deref() == Some(target_head.as_str())
            })
        }));

        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("replica should apply restore delta");
        assert_eq!(read_key_value(&replica, "restore-sync").await, "target");
        assert_eq!(working_diff_count(&replica).await, 0);
    }

    #[tokio::test]
    async fn oversized_ordinary_authority_event_is_rejected_atomically() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let before = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = before else {
            panic!("initial pull should be a snapshot");
        };
        let _limit = TestRepositoryTransactionEventTransferLimit::install(128);

        let error = authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                &[
                    Value::Text("rejected-authority-event".to_owned()),
                    Value::Text("must-not-commit".to_owned()),
                ],
            )
            .await
            .expect_err("an unpullable authority event must fail before commit");
        assert_eq!(error.code, "LIX_ERROR_SYNC_ITEM_TOO_LARGE");

        let result = authority
            .execute(
                "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = $1",
                &[Value::Text("rejected-authority-event".to_owned())],
            )
            .await
            .expect("rejected row count should remain readable");
        assert_eq!(
            result.rows()[0]
                .get::<i64>("count")
                .expect("count should decode"),
            0,
        );
        let after = authority
            .pull_sync_repository(Some(cursor), 1)
            .await
            .expect("rejected transaction must not advance the cursor");
        assert_eq!(
            after,
            SyncRepositoryPullResponse::Delta {
                cursor,
                events: Vec::new(),
            },
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas are read-only serving caches"]
    async fn snapshot_bootstraps_non_root_head_without_parent_bodies_and_is_immediately_writable() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "bootstrap-seed", "parent-body-omitted").await;
        write_key_value(&authority, "bootstrap", "visible").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("non-root snapshot should load");
        let (_, head) = default_head(&snapshot);
        let history = authority
            .sync_history(&head, 128)
            .await
            .expect("head history should load");
        let head_commits = &history.commits;
        let commit_headers = &history.commit_headers;
        let head_header = commit_headers
            .iter()
            .find(|header| header.commit_id == head)
            .expect("head topology should be present");
        assert!(
            !head_header.parent_commit_ids.is_empty(),
            "the regression requires a non-root head",
        );
        let deferred_parent = head_header.parent_commit_ids[0].clone();
        assert_eq!(
            head_commits
                .iter()
                .filter(|commit| commit.commit_id == head)
                .count(),
            1,
            "the exact head body should be included once",
        );
        assert!(
            head_header.parent_commit_ids.iter().all(|parent| {
                head_commits
                    .iter()
                    .all(|commit| &commit.commit_id != parent)
            }),
            "snapshot bootstrap must not depend on parent commit bodies",
        );
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        assert_eq!(read_key_value(&replica, "bootstrap").await, "visible");
        let history_error = replica
            .sync_history(&deferred_parent, 1)
            .await
            .expect_err("a header-only ancestor must demand lazy history");
        assert_eq!(history_error.code, "LIX_SYNC_HISTORY_REQUIRED");
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica read should open");
        let global_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global branch control should load")
            .expect("global branch control should exist");
        let account_pk = RowPk::uuid_from_canonical(crate::ANONYMOUS_ACCOUNT_ID)
            .expect("anonymous account id is canonical");
        let physical_key = crate::hot_state::encode_hot_row_key_for_test(
            GLOBAL_BRANCH_ID,
            global_control.tracked_generation,
            "lix_account",
            &account_pk,
            None,
        );
        let physical_account = exact_get_many(
            &read,
            &[StorageGetManyRequest {
                space: crate::hot_state::ROW_SPACE,
                keys: &[StorageKey(Bytes::from(physical_key))],
                opts: StorageGetOptions::default(),
            }],
        )
        .await
        .expect("physical global account lookup should succeed")
        .values
        .into_iter()
        .next()
        .flatten();
        let account = HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
            .reader(SharedStorageAdapterRead::new(read))
            .load_row(&HotStateRowRequest {
                schema_key: "lix_account".to_owned(),
                branch_id: GLOBAL_BRANCH_ID.to_owned(),
                row_pk: account_pk,
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("global account lookup should succeed");
        assert!(
            account.is_some(),
            "snapshot global generation {} must publish the active account (physical row present: {})",
            global_control.tracked_generation,
            physical_account.is_some(),
        );
        replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/after-bootstrap.txt".to_owned()),
                    Value::Text("works".to_owned()),
                ],
            )
            .await
            .expect("the first file write after bootstrap should succeed");
        replica
            .create_checkpoint()
            .await
            .expect("the first checkpoint after bootstrap should succeed");
        let files = replica
            .execute(
                "SELECT path FROM lix_file WHERE path = '/after-bootstrap.txt'",
                &[],
            )
            .await
            .expect("the new file should be readable");
        assert_eq!(files.rows().len(), 1);
    }

    #[tokio::test]
    async fn deep_snapshot_imports_a_bounded_sparse_jump_header_closure() {
        let authority = open_lix().await.expect("authority should open");
        for generation in 0..32 {
            write_key_value(
                &authority,
                "deep-history",
                &format!("generation-{generation}"),
            )
            .await;
        }
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("deep snapshot metadata should load");
        let (_, head) = default_head(&snapshot);
        let history = authority
            .sync_history(&head, 1)
            .await
            .expect("deep head history should load");
        let header_ids = history
            .commit_headers
            .iter()
            .map(|header| header.commit_id.as_str())
            .collect::<BTreeSet<_>>();
        assert!(
            header_ids.len() <= 6,
            "bootstrap topology must stay bounded independently of history depth",
        );
        for header in &history.commit_headers {
            if let Some(jump) = header.first_parent_jump_commit_id.as_deref() {
                assert!(
                    header_ids.contains(jump),
                    "jump target {jump} for {} must be present",
                    header.commit_id,
                );
            }
        }
        replica_from_snapshot(&authority, &snapshot).await;
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica-authored checkpoint semantics were removed"]
    async fn checkpoint_head_roundtrips_through_snapshot_import() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "checkpoint-roundtrip", "selected").await;
        authority
            .create_checkpoint()
            .await
            .expect("authority checkpoint should succeed");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("checkpoint snapshot should load");
        let (_, head) = default_head(&snapshot);
        let authority_history = authority
            .sync_history(&head, 1)
            .await
            .expect("authority checkpoint history should load");
        let authority_commit = authority_history
            .commits
            .first()
            .cloned()
            .expect("authority checkpoint body exists");
        assert!(authority_commit.state_alias.is_some());
        assert!(authority_commit.members.is_empty());
        let mut checkpoint_rows = Vec::new();
        for boundary in &authority_history.boundaries {
            let page = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("checkpoint boundary rows should load");
            assert_eq!(page.continuation, None);
            checkpoint_rows.extend(page.rows);
        }
        let mut expanded_checkpoint = authority_commit.clone();
        expanded_checkpoint.state_alias = None;
        expanded_checkpoint.members = checkpoint_rows
            .iter()
            .filter(|row| {
                row.schema_key == "lix_key_value"
                    && row.row_pk
                        == serde_json::json!([{"type": "string", "value": "checkpoint-roundtrip"}])
            })
            .map(|row| SyncCommitMember {
                change_id: row.change_id.clone(),
                authored: false,
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
                row_pk: row.row_pk.clone(),
                deleted: false,
                snapshot: row.snapshot.clone(),
                metadata: row.metadata.clone(),
                row_created_at: row.created_at.clone(),
                row_updated_at: row.updated_at.clone(),
                change_account_id: row.change_account_id.clone(),
                change_created_at: row.change_created_at.clone(),
                origin_key: row.origin_key.clone(),
            })
            .collect();
        let mut forged_checkpoint = expanded_checkpoint.clone();
        forged_checkpoint.commit_id =
            CommitId::for_test_label("forged-checkpoint-selected-change").to_string();
        forged_checkpoint.parent_commit_ids = vec![head.clone()];
        let forged_member = forged_checkpoint
            .members
            .iter_mut()
            .find(|member| !member.authored)
            .expect("checkpoint should contain a selected member");
        forged_member.change_id = ChangeId::for_test_label("unknown-checkpoint-change").to_string();
        let forged_id = forged_checkpoint.commit_id.clone();
        authority
            .push_sync_repository(&SyncPushRequest {
                commits: vec![forged_checkpoint.clone()],
                ref_updates: Vec::new(),
                inline_blobs: Vec::new(),
            })
            .await
            .expect("a source-less complete checkpoint owns its selected payloads");
        let forged_history = authority
            .sync_history(&forged_id, 1)
            .await
            .expect("complete checkpoint history should load");
        assert_eq!(
            forged_history.commits[0].commit_id,
            forged_checkpoint.commit_id
        );
        let mut repeated_checkpoint = expanded_checkpoint.clone();
        repeated_checkpoint.commit_id =
            CommitId::for_test_label("repeated-checkpoint-selected-change").to_string();
        repeated_checkpoint.parent_commit_ids = vec![head.clone()];
        let repeated_id = repeated_checkpoint.commit_id.clone();
        authority
            .push_sync_repository(&SyncPushRequest {
                commits: vec![repeated_checkpoint.clone()],
                ref_updates: Vec::new(),
                inline_blobs: Vec::new(),
            })
            .await
            .expect("a repeated selected checkpoint should import");
        let selected_ids = expanded_checkpoint
            .members
            .iter()
            .filter(|member| !member.authored)
            .map(|member| {
                ChangeId::parse_lix(&member.change_id, "checkpoint selected test change")
                    .expect("selected change id should parse")
            })
            .collect::<Vec<_>>();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let mut deleted_locators = StorageWriteSet::default();
        crate::tracked_state::stage_delete_change_locators(
            &mut deleted_locators,
            selected_ids.iter().copied(),
        );
        replica
            .storage_adapter()
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                deleted_locators,
                StorageWriteOptions::default(),
            )
            .await
            .expect("test should emulate a sparse replica missing selected locators");
        let mut repeated_history = authority
            .sync_history(&repeated_id, 1)
            .await
            .expect("detached selected history should load");
        close_history_boundary_bodies(&authority, &mut repeated_history).await;
        let mut repeated_boundary_rows = Vec::new();
        for boundary in &repeated_history.boundaries {
            let page = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("detached selected boundary rows should load");
            assert_eq!(page.continuation, None);
            repeated_boundary_rows.extend(page.rows);
        }
        replica
            .import_sync_history_headers(&repeated_history.commit_headers)
            .await
            .expect("detached selected headers should import");
        replica
            .import_sync_history_boundaries(
                &repeated_history.commits,
                &repeated_history.boundaries,
                &repeated_boundary_rows,
            )
            .await
            .expect("detached selected body should import");
        assert_eq!(
            working_diff_count(&replica).await,
            0,
            "checkpoint metadata is not a user-visible working change",
        );
        write_key_value(&replica, "checkpoint-roundtrip-dirty", "one change").await;
        assert_eq!(
            working_diff_count(&replica).await,
            1,
            "only the post-checkpoint user row should be dirty",
        );
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica change read should open");
        let locator_keys = selected_ids
            .iter()
            .map(|change_id| StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())))
            .collect::<Vec<_>>();
        let restored_locators = exact_get_many(
            &read,
            &[StorageGetManyRequest {
                space: TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                keys: &locator_keys,
                opts: StorageGetOptions::default(),
            }],
        )
        .await
        .expect("selected locator rows should load");
        assert!(
            restored_locators.values.iter().all(Option::is_some),
            "ordinary history import must restore every missing selected locator",
        );
        let selected_records = ChangelogContext::new()
            .reader(&read)
            .load_changes(ChangeLoadRequest {
                change_ids: &selected_ids,
            })
            .await
            .expect("selected checkpoint changes should load");
        assert!(
            selected_records.iter().all(|(_, record)| record.is_some()),
            "checkpoint snapshot must persist selected change payloads"
        );
        let packed_changes = crate::tracked_state::scan_change_records_from_commit_deltas(&read)
            .await
            .expect("sparse checkpoint selections must remain readable by lix_change scans");
        assert!(
            selected_ids.iter().all(|change_id| packed_changes
                .iter()
                .any(|change| change.change_id == *change_id)),
            "a later history import must publish selected locator fallbacks too",
        );
        let packed_change_ids = packed_changes
            .iter()
            .map(|change| change.change_id)
            .collect::<BTreeSet<_>>();
        assert!(
            selected_ids
                .iter()
                .all(|change_id| packed_change_ids.contains(change_id)),
            "deferred authored bodies must use the selected checkpoint payload as a locator fallback",
        );
        let authored_commits = selected_ids
            .iter()
            .map(|change_id| {
                direct_change_locator(*change_id)
                    .expect("test change should encode its authored commit")
                    .commit_id
            })
            .collect::<BTreeSet<_>>();
        drop(read);
        let replica_commit = loop {
            match replica.sync_history(&head, 1).await {
                Ok(history) => {
                    break history
                        .commits
                        .into_iter()
                        .next()
                        .expect("replica checkpoint body exists");
                }
                Err(error) if error.code == "LIX_SYNC_HISTORY_REQUIRED" => {
                    let commit_ids = error
                        .details
                        .as_ref()
                        .and_then(|details| details["commitIds"].as_array())
                        .expect("history demand ids")
                        .iter()
                        .map(|id| id.as_str().expect("history demand id").to_owned())
                        .collect::<Vec<_>>();
                    for commit_id in commit_ids {
                        let history = authority
                            .sync_history(&commit_id, 1)
                            .await
                            .expect("deferred checkpoint dependency should load");
                        replica
                            .import_sync_history_headers(&history.commit_headers)
                            .await
                            .expect("checkpoint dependency headers should import");
                        replica
                            .import_sync_history_boundaries(&history.commits, &[], &[])
                            .await
                            .expect("checkpoint dependency bodies should import");
                    }
                }
                Err(error) => panic!("replica checkpoint history should load: {error:?}"),
            }
        };
        assert_eq!(replica_commit.commit_id, authority_commit.commit_id);
        assert_eq!(
            replica_commit.parent_commit_ids,
            authority_commit.parent_commit_ids
        );
        assert_eq!(replica_commit.members, authority_commit.members);

        for authored_commit in authored_commits {
            let mut history = authority
                .sync_history(&authored_commit.to_string(), 1)
                .await
                .expect("authored history should load");
            close_history_boundary_bodies(&authority, &mut history).await;
            let mut boundary_rows = Vec::new();
            for boundary in &history.boundaries {
                let mut continuation = None;
                loop {
                    let page = authority
                        .pull_sync_snapshot_rows(
                            &boundary.commit_id,
                            &boundary.commit_id,
                            continuation.as_deref(),
                            super::super::MAX_SYNC_REQUEST_ITEMS,
                        )
                        .await
                        .expect("authored history boundary rows should load");
                    boundary_rows.extend(page.rows);
                    let Some(next) = page.continuation else {
                        break;
                    };
                    continuation = Some(next);
                }
            }
            replica
                .import_sync_history_headers(&history.commit_headers)
                .await
                .expect("authored headers should import");
            replica
                .import_sync_history_boundaries(
                    &history.commits,
                    &history.boundaries,
                    &boundary_rows,
                )
                .await
                .expect("authored body should replace the selected locator fallback");
        }
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("hydrated replica change read should open");
        let hydrated_changes = crate::tracked_state::scan_change_records_from_commit_deltas(&read)
            .await
            .expect("hydrating authored bodies must preserve canonical change scans");
        for change_id in &selected_ids {
            assert_eq!(
                hydrated_changes
                    .iter()
                    .filter(|change| change.change_id == *change_id)
                    .count(),
                1,
                "history hydration must replace, not duplicate, the selected locator fallback",
            );
            assert_eq!(
                hydrated_changes
                    .iter()
                    .find(|change| change.change_id == *change_id),
                packed_changes
                    .iter()
                    .find(|change| change.change_id == *change_id),
                "history hydration must preserve the checkpoint-certified selected payload",
            );
        }
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn checkpoint_push_includes_its_distinct_state_alias_source() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "checkpoint-source-base", "authority").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("initial snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        write_key_value(&replica, "checkpoint-source-local-1", "replica-1a").await;
        let first_source_parent_id = replica
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("first checkpoint source parent should load")
            .rows()[0]
            .get::<String>("id")
            .expect("first checkpoint source parent should decode");
        write_key_value(&replica, "checkpoint-source-local-1", "replica-1b").await;
        let first_source_id = replica
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("checkpoint source head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("checkpoint source head should decode");
        replica
            .create_checkpoint()
            .await
            .expect("first replica checkpoint should succeed");
        let first_checkpoint_id = replica
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("first checkpoint head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("first checkpoint head should decode");

        write_key_value(&replica, "checkpoint-source-local-2", "replica-2").await;
        let second_source_id = replica
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("second checkpoint source head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("second checkpoint source head should decode");
        replica
            .create_checkpoint()
            .await
            .expect("second replica checkpoint should succeed");
        let second_checkpoint_id = replica
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("checkpoint head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("checkpoint head should decode");
        let canonical_checkpoint = export_sync_commit(&replica, &second_checkpoint_id)
            .await
            .expect("checkpoint should export before GC")
            .expect("checkpoint body should exist before GC");

        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("GC read should open");
        let mut gc_writes = adapter.new_write_set();
        crate::gc::stage_repository_gc(SharedStorageAdapterRead::new(read), &mut gc_writes)
            .await
            .expect("pre-push GC should stage");
        adapter
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                gc_writes,
                StorageWriteOptions::default(),
            )
            .await
            .expect("pre-push GC should commit");

        let request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("checkpoint push should build")
            .expect("checkpoint push should be pending");
        let first_checkpoint_index = request
            .commits
            .iter()
            .position(|commit| commit.commit_id == first_checkpoint_id)
            .expect("push should contain the first checkpoint body");
        let first_checkpoint = &request.commits[first_checkpoint_index];
        assert_eq!(
            first_checkpoint
                .state_alias
                .as_ref()
                .expect("first checkpoint alias exists")
                .source_commit_id,
            first_source_id,
        );
        assert!(
            !first_checkpoint
                .parent_commit_ids
                .contains(&first_source_id),
            "the fixture must exercise a state source distinct from semantic ancestry",
        );
        let first_source_index = request
            .commits
            .iter()
            .position(|commit| commit.commit_id == first_source_id)
            .expect("push must retain the first alias source through pre-push GC");
        let first_source_parent_index = request
            .commits
            .iter()
            .position(|commit| commit.commit_id == first_source_parent_id)
            .expect("push must retain the alias source parent through pre-push GC");
        let second_source_index = request
            .commits
            .iter()
            .position(|commit| commit.commit_id == second_source_id)
            .expect("push must include the second alias source body");
        let second_checkpoint_index = request
            .commits
            .iter()
            .position(|commit| commit.commit_id == second_checkpoint_id)
            .expect("push should contain the second checkpoint body");
        assert!(first_source_parent_index < first_source_index);
        assert!(first_source_index < first_checkpoint_index);
        assert!(first_checkpoint_index < second_source_index);
        assert!(second_source_index < second_checkpoint_index);
        authority
            .push_sync_repository(&request)
            .await
            .expect("dependency-complete checkpoint push should import");
        assert_eq!(
            export_sync_commit(&authority, &second_checkpoint_id)
                .await
                .expect("imported checkpoint should export")
                .expect("imported checkpoint body should exist"),
            canonical_checkpoint,
            "materialized checkpoint export must preserve canonical wire authority",
        );
    }

    #[tokio::test]
    async fn history_batch_prefers_authored_locator_over_selected_fallback() {
        let authority = open_lix().await.expect("authority should open");
        let baseline = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("baseline snapshot should load");
        let replica = replica_from_snapshot(&authority, &baseline).await;

        write_key_value(&authority, "same-batch-locator", "authored").await;
        authority
            .create_checkpoint()
            .await
            .expect("authority checkpoint should succeed");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("checkpoint snapshot should load");
        let (_, head) = default_head(&snapshot);
        let mut checkpoint_history = authority
            .sync_history(&head, 1)
            .await
            .expect("checkpoint body should load");
        close_history_boundary_bodies(&authority, &mut checkpoint_history).await;
        let mut checkpoint_rows = Vec::new();
        for boundary in &checkpoint_history.boundaries {
            let page = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("checkpoint boundary rows should load");
            assert_eq!(page.continuation, None);
            checkpoint_rows.extend(page.rows);
        }
        let shared_change_id = checkpoint_rows
            .iter()
            .find(|row| {
                row.schema_key == "lix_key_value"
                    && row.row_pk
                        == serde_json::json!([{"type": "string", "value": "same-batch-locator"}])
            })
            .map(|row| row.change_id.clone())
            .expect("checkpoint should select the authored working change");
        let shared_change_id_parsed =
            ChangeId::parse_lix(&shared_change_id, "same-batch shared change")
                .expect("shared change id should parse");
        let authored_commit_id = direct_change_locator(shared_change_id_parsed)
            .expect("locally authored change should encode its commit address")
            .commit_id;
        let mut authored_history = authority
            .sync_history(&authored_commit_id.to_string(), 1)
            .await
            .expect("selected change's authored body should load");
        close_history_boundary_bodies(&authority, &mut authored_history).await;
        assert!(
            authored_history.commits[0]
                .members
                .iter()
                .any(|member| { member.authored && member.change_id == shared_change_id })
        );
        replica
            .import_sync_history_headers(&authored_history.commit_headers)
            .await
            .expect("authored headers should import");
        replica
            .import_sync_history_headers(&checkpoint_history.commit_headers)
            .await
            .expect("checkpoint headers should import");
        let commits = authored_history
            .commits
            .into_iter()
            .chain(checkpoint_history.commits)
            .collect::<Vec<_>>();
        let boundaries = authored_history
            .boundaries
            .into_iter()
            .chain(checkpoint_history.boundaries)
            .collect::<Vec<_>>();
        let mut boundary_rows = Vec::new();
        for boundary in &boundaries {
            let page = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("history boundary rows should load");
            assert_eq!(page.continuation, None);
            boundary_rows.extend(page.rows);
        }

        replica
            .import_sync_history_boundaries(&commits, &boundaries, &boundary_rows)
            .await
            .expect("one batch may contain selected and authored copies of one change");

        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica change scan should open");
        let changes = crate::tracked_state::scan_change_records_from_commit_deltas(&read)
            .await
            .expect("imported changes should scan");
        assert_eq!(
            changes
                .iter()
                .filter(|change| change.change_id == shared_change_id_parsed)
                .count(),
            1,
            "selected fallback must not duplicate an authored change imported in the same batch",
        );
        drop(read);

        // Reproduce the sparse-replica failure: the selected checkpoint has a
        // canonical standalone payload, but its packed locator is absent.
        let adapter = replica.storage_adapter();
        let mut missing_locator = adapter.new_write_set();
        crate::tracked_state::stage_delete_change_locators(
            &mut missing_locator,
            [shared_change_id_parsed],
        );
        adapter
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                missing_locator,
                StorageWriteOptions::default(),
            )
            .await
            .expect("missing-locator fixture should commit");
        let missing_locator_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-locator scan should open");
        let changes =
            crate::tracked_state::scan_change_records_from_commit_deltas(&missing_locator_read)
                .await
                .expect("standalone canonical payload should recover a missing locator");
        assert_eq!(
            changes
                .iter()
                .filter(|change| change.change_id == shared_change_id_parsed)
                .count(),
            1,
            "authored and selected packed copies must remain deduplicated without a locator",
        );
        drop(missing_locator_read);

        let retirement_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authored retirement read should open");
        let authored_manifest = load_commit_state_manifest(&retirement_read, authored_commit_id)
            .await
            .expect("authored manifest lookup should succeed")
            .expect("authored manifest should exist before reclamation");
        let mut retirement = adapter.new_write_set();
        crate::tracked_state::stage_delete_commit_state_manifest_for_gc(
            &retirement_read,
            &mut retirement,
            authored_commit_id,
            &authored_manifest,
        )
        .await
        .expect("authored physical authority should retire");
        drop(retirement_read);
        adapter
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                retirement,
                StorageWriteOptions::default(),
            )
            .await
            .expect("authored retirement should commit");

        let warm = replica
            .open_another_session()
            .await
            .expect("warm sparse replica should reopen");
        let warm_read = warm
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("warm sparse change scan should open");
        let changes = crate::tracked_state::scan_change_records_from_commit_deltas(&warm_read)
            .await
            .expect("selected checkpoint should use its standalone canonical fallback");
        assert_eq!(
            changes
                .iter()
                .filter(|change| change.change_id == shared_change_id_parsed)
                .count(),
            0,
            "manually retiring the final physical commit authority removes it from history scans",
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas are read-only serving caches"]
    async fn snapshot_live_state_omits_tombstones_and_remains_writable() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/deleted-before-snapshot.txt".to_owned()),
                    Value::Text("old".to_owned()),
                ],
            )
            .await
            .expect("file create should succeed");
        authority
            .create_checkpoint()
            .await
            .expect("create checkpoint should succeed");
        authority
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/deleted-before-snapshot.txt".to_owned())],
            )
            .await
            .expect("file delete should succeed");
        authority
            .create_checkpoint()
            .await
            .expect("delete checkpoint should succeed");

        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let absent = replica
            .execute(
                "SELECT path FROM lix_file WHERE path = $1",
                &[Value::Text("/deleted-before-snapshot.txt".to_owned())],
            )
            .await
            .expect("live-state read should succeed");
        assert!(
            absent.rows().is_empty(),
            "snapshot must not revive tombstones"
        );

        replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/deleted-before-snapshot.txt".to_owned()),
                    Value::Text("new".to_owned()),
                ],
            )
            .await
            .expect("recreating a deleted file after bootstrap should succeed");
        replica
            .create_checkpoint()
            .await
            .expect("checkpoint after tombstone-free bootstrap should succeed");
    }

    #[tokio::test]
    async fn exact_push_is_atomic_idempotent_and_creates_a_writable_checkpoint_epoch() {
        let source = open_lix().await.expect("source should open");
        write_key_value(&source, "immutable-collision", "original").await;
        let source_snapshot = source
            .pull_sync_repository(None, 1)
            .await
            .expect("source snapshot should load");
        let (_, source_head) = default_head(&source_snapshot);
        let source_history = source
            .sync_history(&source_head, 100)
            .await
            .expect("source history should load");
        let source_commit = source_history
            .commits
            .iter()
            .find(|commit| commit.commit_id == source_head)
            .cloned()
            .expect("source head commit should exist");
        let source_commits = source_history.commits.clone();
        if let Some(base) = source_commit.base_commit_id.as_deref() {
            assert!(
                source_commits.iter().any(|commit| commit.commit_id == base),
                "history inlines the pinned base dependency",
            );
        }

        let target = open_lix().await.expect("target should open");
        let branch_id = "01920000-0000-7000-8000-000000001499".to_string();
        let request = SyncPushRequest {
            commits: source_commits,
            ref_updates: vec![SyncRefUpdate {
                branch_id: branch_id.clone(),
                expected_head_commit_id: None,
                expected_checkpoint_commit_id: None,
                head_commit_id: Some(source_head.clone()),
                checkpoint_commit_id: Some(source_head.clone()),
            }],
            inline_blobs: Vec::new(),
        };
        let first = target
            .push_sync_repository(&request)
            .await
            .expect("exact push should succeed");
        let second = target
            .push_sync_repository(&request)
            .await
            .expect("exact replay should be idempotent");
        assert_eq!(second.cursor, first.cursor);
        let mut conflicting = request.clone();
        conflicting
            .commits
            .iter_mut()
            .flat_map(|commit| &mut commit.members)
            .find(|member| member.schema_key == "lix_key_value")
            .expect("dependency closure contains the key-value member")
            .snapshot
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
            .expect("key-value snapshot is an object")
            .insert("value".to_owned(), serde_json::json!("different"));
        let error = target
            .push_sync_repository(&conflicting)
            .await
            .expect_err("same commit id with different content must fail");
        assert_eq!(
            error.code,
            super::super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE
        );
        assert!(error.message.contains("different content"));
        assert_eq!(
            target
                .sync_history(&source_head, 1)
                .await
                .expect("imported history should load")
                .commits
                .into_iter()
                .find(|commit| commit.commit_id == source_head),
            Some(source_commit)
        );

        target
            .switch_branch(SwitchBranchOptions {
                branch_id: branch_id.clone(),
            })
            .await
            .expect("imported branch should be switchable");
        target
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/after-sync.txt".to_string()),
                    Value::Text("works".to_string()),
                ],
            )
            .await
            .expect("first file write after sync must have a checkpoint cursor");

        let adapter = target.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("control should load")
            .expect("control should exist");
        assert!(control.working_diff_checkpoint_commit_id.is_some());
    }

    #[tokio::test]
    #[ignore = "v3 replicas have no offline commit outbox"]
    async fn more_than_one_push_window_of_offline_commits_drains_without_repeating_a_batch() {
        let authority = open_lix().await.expect("authority should open");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        for index in 0..=super::super::MAX_SYNC_REQUEST_ITEMS {
            write_key_value(&replica, "offline-window", &format!("value-{index}")).await;
        }

        let mut pushed_commit_ids = BTreeSet::new();
        let mut push_count = 0usize;
        loop {
            let Some(request) = replica
                .build_sync_push(TEST_REMOTE, super::super::MAX_SYNC_REQUEST_ITEMS)
                .await
                .expect("bounded offline push should build")
            else {
                break;
            };
            assert!(
                request.commits.len() + request.ref_updates.len()
                    <= super::super::MAX_SYNC_REQUEST_ITEMS,
                "every request must obey the shared item limit",
            );
            for commit in &request.commits {
                assert!(
                    pushed_commit_ids.insert(commit.commit_id.clone()),
                    "an acknowledged commit batch must not be rebuilt",
                );
            }
            let receipt = authority
                .push_sync_repository(&request)
                .await
                .expect("authority should accept bounded push");
            let cursor = replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("replica cursor should load")
                .expect("replica should be initialized");
            let delta = authority
                .pull_sync_repository(Some(cursor), super::super::MAX_SYNC_REQUEST_ITEMS)
                .await
                .expect("authority acknowledgement delta should load");
            assert!(
                match &delta {
                    SyncRepositoryPullResponse::Delta { cursor, .. } => *cursor >= receipt.cursor,
                    SyncRepositoryPullResponse::Snapshot { .. } => false,
                },
                "delta should cover the accepted push",
            );
            replica
                .apply_sync_repository_pull(TEST_REMOTE, &delta)
                .await
                .expect("replica should persist the authority acknowledgement");
            let read = replica
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("acknowledgement state read should open");
            let acknowledged = load_replica_state(&read)
                .await
                .expect("acknowledgement state should load")
                .0
                .expect("replica state should exist");
            assert!(
                acknowledged.authority_known_commit_ids.len() <= 1,
                "a linear multi-page upload retains only its unattached frontier",
            );
            push_count += 1;
        }

        assert!(push_count >= 2, "the fixture must cross one push window");
        assert_eq!(
            pushed_commit_ids.len(),
            super::super::MAX_SYNC_REQUEST_ITEMS + 1,
        );
        assert_eq!(
            read_key_value(&authority, "offline-window").await,
            format!("value-{}", super::super::MAX_SYNC_REQUEST_ITEMS),
        );
    }

    #[tokio::test]
    async fn ref_conflict_does_not_import_commit() {
        let source = open_lix().await.expect("source should open");
        source
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/conflict.txt".to_string()),
                    Value::Text("content".to_string()),
                ],
            )
            .await
            .expect("source write should succeed");
        let source_snapshot = source
            .pull_sync_repository(None, 1)
            .await
            .expect("source snapshot should load");
        let (_, source_head) = default_head(&source_snapshot);
        let source_commit = source
            .sync_history(&source_head, 1)
            .await
            .expect("source history should load")
            .commits
            .into_iter()
            .next()
            .expect("source head commit should exist");

        let target = open_lix().await.expect("target should open");
        let target_snapshot = target
            .pull_sync_repository(None, 1)
            .await
            .expect("target snapshot should load");
        let (target_branch, _) = default_head(&target_snapshot);
        let error = target
            .push_sync_repository(&SyncPushRequest {
                commits: vec![source_commit],
                ref_updates: vec![SyncRefUpdate {
                    branch_id: target_branch,
                    expected_head_commit_id: None,
                    expected_checkpoint_commit_id: None,
                    head_commit_id: Some(source_head.clone()),
                    checkpoint_commit_id: Some(source_head.clone()),
                }],
                inline_blobs: Vec::new(),
            })
            .await
            .expect_err("stale ref CAS must fail");
        assert_eq!(
            error.code,
            LixError::CODE_TRANSACTION_CONFLICT,
            "unexpected pre-CAS failure: {error:?}",
        );
        let error = target
            .sync_history(&source_head, 1)
            .await
            .expect_err("missing history head must fail");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
    }

    #[tokio::test]
    async fn authority_rejects_deleting_the_repository_default_branch() {
        let authority = open_lix().await.expect("authority should open");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (default_branch_id, default_head_id) = default_head(&snapshot);

        let error = authority
            .push_sync_repository(&SyncPushRequest {
                commits: Vec::new(),
                ref_updates: vec![SyncRefUpdate {
                    branch_id: default_branch_id.clone(),
                    expected_head_commit_id: Some(default_head_id.clone()),
                    expected_checkpoint_commit_id: Some(default_head_id.clone()),
                    head_commit_id: None,
                    checkpoint_commit_id: None,
                }],
                inline_blobs: Vec::new(),
            })
            .await
            .expect_err("the repository default branch cannot be deleted");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let after = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should remain readable");
        assert_eq!(default_head(&after), (default_branch_id, default_head_id));
    }

    #[tokio::test]
    async fn repository_events_keep_deleted_branch_commit_bodies_replayable_after_gc() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .set_sync_role(super::super::SyncRole::Authority)
            .expect("authority role should install");
        let main_branch_id = authority
            .active_branch_id()
            .await
            .expect("main branch should load");
        let branch = authority
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000001778".to_owned()),
                name: "event-retention".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("disposable branch should be created");
        authority
            .switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            })
            .await
            .expect("disposable branch should become active");
        write_key_value(&authority, "event-retention", "must-survive-gc").await;
        authority
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id,
            })
            .await
            .expect("main branch should become active again");
        authority
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.id)],
            )
            .await
            .expect("disposable branch should be deleted");

        let adapter = authority.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("GC read should open");
        let mut writes = adapter.new_write_set();
        crate::gc::stage_repository_gc(SharedStorageAdapterRead::new(read), &mut writes)
            .await
            .expect("repository GC should stage");
        adapter
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                writes,
                StorageWriteOptions::default(),
            )
            .await
            .expect("repository GC should commit");

        let replay = authority
            .pull_sync_repository(Some(0), super::super::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("every retained event must still export its commit bodies");
        let SyncRepositoryPullResponse::Delta { events, .. } = replay else {
            panic!("cursor pull should return a delta");
        };
        assert!(
            events.iter().any(|event| {
                event.commits.iter().any(|commit| {
                    commit.members.iter().any(|member| {
                        member.schema_key == "lix_key_value"
                            && member
                                .snapshot
                                .as_ref()
                                .and_then(|snapshot| snapshot.get("key"))
                                .and_then(serde_json::Value::as_str)
                                == Some("event-retention")
                    })
                })
            }),
            "the deleted branch's replayable event body disappeared",
        );
    }

    #[tokio::test]
    async fn authority_preserves_a_noncanonical_looking_change_via_locator_fallback() {
        let source = open_lix().await.expect("source should open");
        write_key_value(&source, "forged-address", "payload").await;
        let snapshot = source
            .pull_sync_repository(None, 1)
            .await
            .expect("source snapshot should load");
        let (_, head) = default_head(&snapshot);
        let mut commits = source
            .sync_history(&head, super::super::MAX_SYNC_HISTORY_PAGE_SIZE)
            .await
            .expect("source history should load")
            .commits;
        let commit = commits
            .iter_mut()
            .find(|commit| commit.commit_id == head)
            .expect("source head should have a body");
        let member = commit
            .members
            .iter_mut()
            .find(|member| member.authored)
            .expect("written commit should have an authored member");
        let mut forged = *CommitId::parse_lix(&head, "forged commit")
            .expect("head should parse")
            .as_uuid()
            .as_bytes();
        forged[12..].copy_from_slice(&50_u32.to_be_bytes());
        member.change_id = ChangeId::new(uuid::Uuid::from_bytes(forged)).to_string();

        let target = open_lix().await.expect("target authority should open");

        target
            .push_sync_repository(&SyncPushRequest {
                commits: commits.clone(),
                ref_updates: Vec::new(),
                inline_blobs: Vec::new(),
            })
            .await
            .expect("the complete commit's authoritative id should use locator fallback");
        assert_eq!(
            target
                .sync_history(&head, 1)
                .await
                .expect("imported history should load")
                .commits
                .into_iter()
                .find(|candidate| candidate.commit_id == head),
            commits.into_iter().find(|commit| commit.commit_id == head),
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_metadata_without_the_default_branch() {
        let authority = open_lix().await.expect("authority should open");
        let mut snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (history, rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        let (default_branch_id, _) = default_head(&snapshot);
        let SyncRepositoryPullResponse::Snapshot { branches, .. } = &mut snapshot else {
            unreachable!("initial pull is a snapshot");
        };
        branches.retain(|branch| branch.branch_id != default_branch_id);

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&default_branch_id))
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        let error = replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect_err("default-less metadata must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("headed default branch"));
    }

    #[tokio::test]
    async fn snapshot_rejects_a_default_branch_that_disagrees_with_tracked_state() {
        let authority = open_lix().await.expect("authority should open");
        let secondary = authority
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000001501".to_owned()),
                name: "secondary".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("secondary branch should be created");
        let mut snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (history, rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        let SyncRepositoryPullResponse::Snapshot {
            default_branch_id, ..
        } = &mut snapshot
        else {
            unreachable!("initial pull is a snapshot");
        };
        *default_branch_id = secondary.id.clone();

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&secondary.id))
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        let error = replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect_err("metadata cannot redefine the tracked repository default");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("canonical tracked row"));
    }

    #[tokio::test]
    async fn snapshot_rejects_and_preserves_a_locally_advanced_same_id_branch() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "authority-only", "remote").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("authority snapshot should load");
        let (branch_id, _) = default_head(&snapshot);

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("local storage should initialize with the same branch id");
        let local = open_lix()
            .with_storage(storage)
            .await
            .expect("local repository should open");
        write_key_value(&local, "local-only", "must-survive").await;
        let local_head_before = local
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("local head should load")
            .rows()[0]
            .get::<String>("id")
            .expect("local head should be text");
        local
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        let (history, rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;

        let error = local
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect_err("snapshot must not orphan a locally advanced same-id branch");
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
        assert!(error.message.contains("locally advanced branch"));
        local
            .set_sync_role(super::super::SyncRole::Disabled)
            .expect("a rejected bootstrap should return the fixture to standalone mode");
        assert_eq!(read_key_value(&local, "local-only").await, "must-survive");
        let local_head_after = local
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("local head should remain readable")
            .rows()[0]
            .get::<String>("id")
            .expect("local head should be text");
        assert_eq!(local_head_after, local_head_before);
        assert_eq!(
            local
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("cursor query should succeed"),
            None,
            "failed snapshot must remain atomic",
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_an_incomplete_row_sequence_without_advancing_cursor() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "complete-a", "one").await;
        write_key_value(&authority, "complete-b", "two").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot metadata should load");
        let (branch_id, _) = default_head(&snapshot);
        let (history, mut rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        assert!(rows.len() > 1, "fixture must have more than one live row");
        let omitted = rows
            .iter()
            .position(|row| {
                row.schema_key == "lix_key_value"
                    && row
                        .snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.get("key"))
                        .and_then(serde_json::Value::as_str)
                        == Some("complete-a")
            })
            .expect("fixture row should exist");
        rows.remove(omitted);

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        let error = replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect_err("an incomplete row sequence must fail root verification");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("do not match head"));
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("cursor query should succeed"),
            None,
            "failed root verification must not advance the replica cursor",
        );
    }

    #[tokio::test]
    async fn snapshot_live_value_root_rejects_tampered_content_without_advancing_cursor() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "certified-content", "authority").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot metadata should load");
        let (branch_id, _) = default_head(&snapshot);
        let (history, mut rows, checkpoint_roots) = snapshot_parts(&authority, &snapshot).await;
        let row = rows
            .iter_mut()
            .find(|row| {
                row.schema_key == "lix_key_value"
                    && row
                        .snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.get("key"))
                        .and_then(serde_json::Value::as_str)
                        == Some("certified-content")
            })
            .expect("fixture row should exist");
        row.snapshot.as_mut().expect("live row has content")["value"] =
            serde_json::json!("tampered");

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .expect("replica storage should initialize");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica should open");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role should install");
        let error = replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect_err("tampered content must fail live-value verification");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .expect("cursor query should succeed"),
            None,
        );
    }

    #[tokio::test]
    async fn snapshot_row_pages_are_complete_ordered_and_exclusive() {
        let authority = open_lix().await.expect("authority should open");
        for index in 0..8 {
            write_key_value(&authority, &format!("page-{index:02}"), "value").await;
        }
        for index in 0..6 {
            authority
                .execute(
                    "DELETE FROM lix_key_value WHERE key = $1",
                    &[Value::Text(format!("page-{index:02}"))],
                )
                .await
                .expect("tombstone-heavy page fixture should delete a row");
        }
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot metadata should load");
        let (branch_id, head) = default_head(&snapshot);
        let complete = authority
            .pull_sync_snapshot_rows(
                &branch_id,
                &head,
                None,
                super::super::MAX_SYNC_REQUEST_ITEMS,
            )
            .await
            .expect("complete page should load");
        assert!(complete.continuation.is_none());

        let mut paged = Vec::new();
        let mut continuation = None;
        loop {
            let page = authority
                .pull_sync_snapshot_rows(&branch_id, &head, continuation.as_deref(), 3)
                .await
                .expect("bounded page should load");
            paged.extend(page.rows);
            let Some(next) = page.continuation else {
                break;
            };
            assert_ne!(continuation.as_deref(), Some(next.as_str()));
            continuation = Some(next);
        }
        assert_eq!(paged, complete.rows);
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn cold_merge_history_page_boundaries_cover_every_external_parent() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "shared").await;
        let base_snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("base snapshot should load");
        let cold = replica_from_snapshot(&authority, &base_snapshot).await;
        let left = replica_from_snapshot(&authority, &base_snapshot).await;
        let right = replica_from_snapshot(&authority, &base_snapshot).await;

        write_key_value(&left, "left", "authority-first").await;
        write_key_value(&right, "right", "secondary-parent").await;
        publish_pending(&left, &authority).await;
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        right
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("right replica should reconcile into a merge");
        let merge_push = publish_pending(&right, &authority).await;
        let merge_head = merge_push.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("merge push should advance the branch");

        let mut page = authority
            .sync_history(&merge_head, 2)
            .await
            .expect("cold merge page should load");
        let body_ids = page
            .commits
            .iter()
            .map(|commit| commit.commit_id.as_str())
            .collect::<BTreeSet<_>>();
        let merge = page
            .commits
            .iter()
            .find(|commit| commit.commit_id == merge_head)
            .expect("page should contain its merge head");
        assert_eq!(merge.parent_commit_ids.len(), 2);
        assert!(
            body_ids.contains(merge.parent_commit_ids[0].as_str()),
            "the merge first parent should be in the page"
        );
        assert!(
            merge
                .parent_commit_ids
                .iter()
                .any(|parent| !body_ids.contains(parent.as_str())),
            "the merge secondary parent should be outside the page"
        );
        let boundary_ids = page
            .boundaries
            .iter()
            .map(|boundary| boundary.commit_id.as_str())
            .collect::<BTreeSet<_>>();
        assert!(
            boundary_ids.contains(merge_head.as_str()),
            "a merge with an external secondary parent must be a boundary"
        );
        assert_eq!(
            boundary_ids.len(),
            3,
            "the merge, oldest causal body, and external global base all need boundaries"
        );
        let expected_page_commits = page.commits.clone();
        close_history_boundary_bodies(&authority, &mut page).await;

        let mut rows = Vec::new();
        for boundary in &page.boundaries {
            let mut continuation = None;
            loop {
                let row_page = authority
                    .pull_sync_snapshot_rows(
                        &boundary.commit_id,
                        &boundary.commit_id,
                        continuation.as_deref(),
                        super::super::MAX_SYNC_REQUEST_ITEMS,
                    )
                    .await
                    .expect("boundary snapshot rows should load");
                rows.extend(row_page.rows);
                let Some(next) = row_page.continuation else {
                    break;
                };
                continuation = Some(next);
            }
        }
        cold.import_sync_history_headers(&page.commit_headers)
            .await
            .expect("cold replica should accept sparse topology");
        cold.import_sync_history_boundaries(&page.commits, &page.boundaries, &rows)
            .await
            .expect("cold replica should import a merge page with two boundaries");
        assert_eq!(
            cold.sync_history(&merge_head, 2)
                .await
                .expect("imported merge history should be readable")
                .commits,
            expected_page_commits,
        );
    }

    #[tokio::test]
    async fn history_headers_reject_a_forged_generation() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "generation", "parent").await;
        write_key_value(&authority, "generation", "child").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot metadata should load");
        let (_, head) = default_head(&snapshot);
        let mut history = authority
            .sync_history(&head, 1)
            .await
            .expect("history should load");
        history
            .commit_headers
            .iter_mut()
            .find(|header| header.commit_id == head)
            .expect("head header should exist")
            .generation += 7;
        let replica = open_lix().await.expect("replica should open");
        let error = replica
            .import_sync_history_headers(&history.commit_headers)
            .await
            .expect_err("forged generation must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("invalid generation"));
    }

    #[tokio::test]
    async fn deferred_history_body_rejects_header_scope_mismatch() {
        let authority = open_lix().await.expect("authority should open");
        let base = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("base snapshot should load");
        let replica = replica_from_snapshot(&authority, &base).await;
        write_key_value(&authority, "scope-mismatch", "body").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("updated snapshot should load");
        let (_, head) = default_head(&snapshot);
        let history = authority
            .sync_history(&head, 1)
            .await
            .expect("history should load");
        replica
            .import_sync_history_headers(&history.commit_headers)
            .await
            .expect("certified headers should import");
        let mut rows = Vec::new();
        for boundary in &history.boundaries {
            let page = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("boundary rows should load");
            assert!(page.continuation.is_none());
            rows.extend(page.rows);
        }

        let mut forged = history.commits.clone();
        forged[0].global_scope = !forged[0].global_scope;
        let error = replica
            .import_sync_history_boundaries(&forged, &history.boundaries, &rows)
            .await
            .expect_err("history body must retain the header's immutable scope");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("global sync commit must not have a base"),
            "unexpected error: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn history_headers_reject_self_and_duplicate_parents() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "header-parent", "child").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot metadata should load");
        let (_, head) = default_head(&snapshot);
        let history = authority
            .sync_history(&head, 1)
            .await
            .expect("history should load");
        let original = history
            .commit_headers
            .iter()
            .find(|header| header.commit_id == head)
            .expect("head header should exist")
            .clone();
        let replica = open_lix().await.expect("replica should open");

        let mut self_parent = original.clone();
        self_parent.parent_commit_ids = vec![self_parent.commit_id.clone()];
        let error = replica
            .import_sync_history_headers(&[self_parent])
            .await
            .expect_err("a header cannot be its own parent");
        assert!(error.message.contains("own parent"));

        let mut duplicate_parent = original;
        let parent = duplicate_parent
            .parent_commit_ids
            .first()
            .expect("written head should have a parent")
            .clone();
        duplicate_parent.parent_commit_ids = vec![parent.clone(), parent];
        let error = replica
            .import_sync_history_headers(&[duplicate_parent])
            .await
            .expect_err("header parents must be unique");
        assert!(error.message.contains("must be unique"));
    }

    #[tokio::test]
    async fn history_headers_reject_a_sparse_two_node_cycle() {
        let authority = open_lix().await.expect("authority should open");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, head) = default_head(&snapshot);
        let template = authority
            .sync_history(&head, 1)
            .await
            .expect("history should load")
            .commit_headers
            .into_iter()
            .next()
            .expect("a template header should exist");
        let first = CommitId::for_test_label("sparse-cycle-first").to_string();
        let second = CommitId::for_test_label("sparse-cycle-second").to_string();
        let omitted = CommitId::for_test_label("sparse-cycle-omitted").to_string();
        let mut first_header = template.clone();
        first_header.commit_id.clone_from(&first);
        first_header.parent_commit_ids = vec![second.clone(), omitted.clone()];
        first_header.generation = 2;
        first_header.first_parent_jump_commit_id = None;
        first_header.first_parent_jump_span = None;
        let mut second_header = template;
        second_header.commit_id = second;
        second_header.parent_commit_ids = vec![first, omitted];
        second_header.generation = 2;
        second_header.first_parent_jump_commit_id = None;
        second_header.first_parent_jump_span = None;

        let replica = open_lix().await.expect("replica should open");
        let error = replica
            .import_sync_history_headers(&[first_header, second_header])
            .await
            .expect_err("sparse boundaries cannot conceal an in-batch cycle");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("graph contains a cycle"));
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot create local restore children"]
    async fn durable_restore_with_a_local_child_does_not_stall_an_unrelated_branch_push() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "one").await;
        write_key_value(&authority, "tip", "two").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (main_branch_id, authority_head) = default_head(&snapshot);
        let history = authority
            .sync_history(&authority_head, 1)
            .await
            .expect("authority head history should load");
        let historical_head = history
            .commits
            .iter()
            .find(|commit| commit.commit_id == authority_head)
            .and_then(|commit| commit.parent_commit_ids.first())
            .cloned()
            .expect("authority head should have a historical parent");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let unrelated_branch_id = "01920000-0000-7000-8000-000000001500".to_owned();
        replica
            .create_branch(CreateBranchOptions {
                id: Some(unrelated_branch_id.clone()),
                name: "unrelated local branch".to_owned(),
                from_commit_id: Some(authority_head.clone()),
            })
            .await
            .expect("unrelated branch should be created");
        replica
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id.clone(),
            })
            .await
            .expect("fixture should restore the synchronized main branch");
        let historical = authority
            .sync_history(&historical_head, 1)
            .await
            .expect("restore target history should load");
        let mut boundary_rows = Vec::new();
        for boundary in &historical.boundaries {
            let rows = authority
                .pull_sync_snapshot_rows(
                    &boundary.commit_id,
                    &boundary.commit_id,
                    None,
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .expect("restore boundary rows should load");
            boundary_rows.extend(rows.rows);
        }
        replica
            .import_sync_history_headers(&historical.commit_headers)
            .await
            .expect("restore history headers should import");
        replica
            .import_sync_history_boundaries(
                &historical.commits,
                &historical.boundaries,
                &boundary_rows,
            )
            .await
            .expect("restore history should import");
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(historical_head.clone())],
            )
            .await
            .expect("historical restore should succeed");
        write_key_value(&replica, "after-restore", "local-child").await;

        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("push should build")
            .expect("reset and unrelated branch should both remain publishable");
        let reset = push
            .ref_updates
            .iter()
            .find(|update| update.branch_id == main_branch_id)
            .expect("historical reset must produce a ref CAS");
        assert_eq!(
            reset.expected_head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
        assert_ne!(
            reset.head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
        assert_ne!(
            reset.head_commit_id.as_deref(),
            Some(historical_head.as_str()),
            "the post-restore local child must be published with the reset"
        );
        assert!(
            !push.commits.is_empty(),
            "post-restore local commits must accompany the ref reset"
        );
        assert!(
            push.ref_updates
                .iter()
                .any(|update| update.branch_id == unrelated_branch_id),
            "one reset branch must not stall an independent new branch",
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn restore_to_an_unpublished_local_ancestor_exports_the_target() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "published-base", "authority").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        write_key_value(&replica, "offline-target", "kept").await;
        let unpublished_target = current_branch_head(&replica).await;
        write_key_value(&replica, "offline-abandoned", "removed").await;
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(unpublished_target.clone())],
            )
            .await
            .expect("restore to the first offline commit should succeed");

        let request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("reset push should build")
            .expect("unpublished reset should have pending work");
        assert!(
            request
                .commits
                .iter()
                .any(|commit| commit.commit_id == unpublished_target),
            "the authority cannot accept the reset until its target is exported"
        );
        authority
            .push_sync_repository(&request)
            .await
            .expect("authority should accept a dependency-complete reset");
        assert_eq!(current_branch_head(&authority).await, unpublished_target);
        assert_eq!(read_key_value(&authority, "offline-target").await, "kept");
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn restore_losing_a_ref_cas_adopts_the_advanced_authority_without_retry() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-race", "target").await;
        write_key_value(&authority, "restore-race", "abandoned").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, authority_before) = default_head(&snapshot);
        let target = authority
            .sync_history(&authority_before, 1)
            .await
            .expect("authority history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let remote_writer = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &target).await;
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(target)],
            )
            .await
            .expect("offline restore should succeed");

        write_key_value(&remote_writer, "restore-race", "authority-wins").await;
        let published = publish_pending(&remote_writer, &authority).await;
        let authority_after = published.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("authority should advance");
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("the failed reset coordinate should reconcile");

        assert_eq!(
            read_key_value(&replica, "restore-race").await,
            "authority-wins"
        );
        assert_eq!(
            current_branch_head(&replica).await,
            authority_after,
            "a pure restored ancestor adopts the authority without a merge"
        );
        assert!(
            replica
                .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
                .await
                .expect("post-race outbox should inspect")
                .is_none(),
            "the superseded reset must not be retried against the new authority"
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn newer_restore_survives_an_older_reset_acknowledgement() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-chain", "oldest").await;
        write_key_value(&authority, "restore-chain", "middle").await;
        write_key_value(&authority, "restore-chain", "tip").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, authority_head) = default_head(&snapshot);
        let middle = authority
            .sync_history(&authority_head, 1)
            .await
            .expect("tip history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let oldest = authority
            .sync_history(&middle, 1)
            .await
            .expect("middle history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &middle).await;
        hydrate_history_commit(&authority, &replica, &oldest).await;

        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(middle.clone())],
            )
            .await
            .expect("first restore should succeed");
        write_key_value(&replica, "after-first-restore", "local-child").await;
        let first_prepared_head = current_branch_head(&replica).await;
        let first = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("first reset should build")
            .expect("first reset should be pending");
        authority
            .push_sync_repository(&first)
            .await
            .expect("authority should accept the first reset");

        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(oldest.clone())],
            )
            .await
            .expect("newer restore should replace the in-flight intent");
        let acknowledgement = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("first reset acknowledgement should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &acknowledgement)
            .await
            .expect("older acknowledgement should preserve the newer restore");

        let second = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("rebased reset should build")
            .expect("newer restore must remain pending");
        assert_eq!(second.ref_updates.len(), 1);
        assert_eq!(
            second.ref_updates[0].expected_head_commit_id.as_deref(),
            Some(first_prepared_head.as_str())
        );
        assert_eq!(
            second.ref_updates[0].head_commit_id.as_deref(),
            Some(oldest.as_str())
        );
        authority
            .push_sync_repository(&second)
            .await
            .expect("authority should accept the rebased newer reset");
        assert_eq!(current_branch_head(&authority).await, oldest);
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn newer_restore_to_unpublished_descendant_reuses_known_boundary() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-boundary", "ancestor").await;
        write_key_value(&authority, "restore-boundary", "authority-tip").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, authority_head) = default_head(&snapshot);
        let ancestor = authority
            .sync_history(&authority_head, 1)
            .await
            .expect("authority history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &ancestor).await;

        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(ancestor)],
            )
            .await
            .expect("first restore should succeed");
        write_key_value(&replica, "offline-chain", "keep").await;
        let desired_descendant = current_branch_head(&replica).await;
        write_key_value(&replica, "offline-chain", "discard").await;
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(desired_descendant.clone())],
            )
            .await
            .expect("second restore should succeed");

        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("descendant reset should build")
            .expect("descendant reset should remain publishable");
        assert_eq!(push.ref_updates.len(), 1);
        assert_eq!(
            push.ref_updates[0].expected_head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
        assert_eq!(
            push.ref_updates[0].head_commit_id.as_deref(),
            Some(desired_descendant.as_str())
        );
        assert!(
            push.commits
                .iter()
                .any(|commit| commit.commit_id == desired_descendant),
            "the unpublished descendant must be included"
        );
        authority
            .push_sync_repository(&push)
            .await
            .expect("authority should accept the descendant reset");
        assert_eq!(current_branch_head(&authority).await, desired_descendant);
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn newer_restore_to_authority_ancestor_recomputes_known_boundary() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-reboundary", "ancestor").await;
        write_key_value(&authority, "restore-reboundary", "authority-tip").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, authority_head) = default_head(&snapshot);
        let authority_ancestor = authority
            .sync_history(&authority_head, 1)
            .await
            .expect("authority history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &authority_ancestor).await;

        write_key_value(&replica, "unpublished-chain", "first").await;
        let unpublished_ancestor = current_branch_head(&replica).await;
        write_key_value(&replica, "unpublished-chain", "second").await;
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(unpublished_ancestor)],
            )
            .await
            .expect("first restore should succeed");
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(authority_ancestor.clone())],
            )
            .await
            .expect("second restore should succeed");

        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("rebased ancestor reset should build")
            .expect("authority ancestor reset should remain publishable");
        assert!(push.commits.is_empty());
        assert_eq!(push.ref_updates.len(), 1);
        assert_eq!(
            push.ref_updates[0].expected_head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
        assert_eq!(
            push.ref_updates[0].head_commit_id.as_deref(),
            Some(authority_ancestor.as_str())
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn restore_to_second_parent_lineage_uses_common_authority_boundary() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-merge-base", "shared").await;
        let shared_ancestor = current_branch_head(&authority).await;
        write_key_value(&authority, "authority-lineage", "tip").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (main_branch_id, authority_head) = default_head(&snapshot);
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &shared_ancestor).await;

        let side = replica
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000001991".to_owned()),
                name: "restore-side-lineage".to_owned(),
                from_commit_id: Some(shared_ancestor),
            })
            .await
            .expect("side branch should fork from the shared ancestor");
        replica
            .switch_branch(SwitchBranchOptions {
                branch_id: side.id.clone(),
            })
            .await
            .expect("side branch should become active");
        write_key_value(&replica, "side-lineage", "target").await;
        let side_target = current_branch_head(&replica).await;
        replica
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id.clone(),
            })
            .await
            .expect("main branch should become active");
        replica
            .merge_branch(MergeBranchOptions {
                source_branch_id: side.id,
            })
            .await
            .expect("side lineage should merge into main");
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(side_target.clone())],
            )
            .await
            .expect("restore to the second-parent lineage should succeed");

        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("second-parent reset should build")
            .expect("second-parent reset should remain publishable");
        let main_update = push
            .ref_updates
            .iter()
            .find(|update| update.branch_id == main_branch_id)
            .expect("main reset ref should be included");
        assert_eq!(
            main_update.expected_head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
        assert_eq!(
            main_update.head_commit_id.as_deref(),
            Some(side_target.as_str())
        );
        assert!(
            push.commits
                .iter()
                .any(|commit| commit.commit_id == side_target),
            "the second-parent target must be exported from the shared boundary"
        );
    }

    #[tokio::test]
    async fn pull_publication_rejects_same_cursor_replica_state_change() {
        let replica = open_lix().await.expect("replica should open");
        let original = SyncReplicaState {
            active_account_id: "account-a".to_owned(),
            cursor: 7,
            authoritative_branches: BTreeMap::new(),
            certified_branch_roots: BTreeMap::new(),
            snapshot_certified_cursor: None,
            pending_resets: BTreeMap::new(),
            authority_known_commit_ids: BTreeSet::new(),
        };
        replica
            .store_replica_state(TEST_REMOTE, original.clone())
            .await
            .expect("initial replica state should store");
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica state read should open");
        let (_, expected_state_raw) = load_replica_state(&read)
            .await
            .expect("replica state should load");
        let expected_state_raw = expected_state_raw.expect("replica state should have bytes");
        drop(read);

        let mut folded = original.clone();
        folded.cursor = 8;
        let mut concurrent = original;
        concurrent
            .authority_known_commit_ids
            .insert(CommitId::with_change_address_space(uuid::Uuid::now_v7()).to_string());
        replica
            .store_replica_state(TEST_REMOTE, concurrent.clone())
            .await
            .expect("same-cursor metadata update should store");

        let error = replica
            .import_sync_repository(
                &SyncPushRequest {
                    commits: Vec::new(),
                    ref_updates: Vec::new(),
                    inline_blobs: Vec::new(),
                },
                SyncImportPurpose::ReplicaDelta,
                None,
                Some(ReplicaStatePublication {
                    expected_cursor: 7,
                    expected_state_raw: &expected_state_raw,
                    state: &folded,
                }),
            )
            .await
            .expect_err("stale full-state admission must conflict");
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica state read should reopen");
        assert_eq!(
            load_replica_state(&read)
                .await
                .expect("replica state should load")
                .0,
            Some(concurrent),
            "the concurrent same-cursor state must remain intact"
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot author restore intents"]
    async fn restore_child_racing_authority_keeps_local_first_parent() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "restore-base", "target").await;
        write_key_value(&authority, "restore-base", "abandoned").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, authority_before) = default_head(&snapshot);
        let target = authority
            .sync_history(&authority_before, 1)
            .await
            .expect("authority history should load")
            .commits[0]
            .parent_commit_ids[0]
            .clone();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let remote_writer = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &target).await;
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(target)],
            )
            .await
            .expect("offline restore should succeed");
        write_key_value(&replica, "local-only", "survives").await;
        let local_head = current_branch_head(&replica).await;

        write_key_value(&remote_writer, "authority-only", "survives").await;
        let published = publish_pending(&remote_writer, &authority).await;
        let authority_head = published.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("authority should advance");
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("restore-child divergence should reconcile");
        assert_eq!(read_key_value(&replica, "local-only").await, "survives");
        assert_eq!(read_key_value(&replica, "authority-only").await, "survives");

        let push = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("merge push should build")
            .expect("server-winning merge should be pending");
        let merge_head = push.ref_updates[0]
            .head_commit_id
            .as_deref()
            .expect("merge ref should have a head");
        let merge = push
            .commits
            .iter()
            .find(|commit| commit.commit_id == merge_head)
            .expect("merge commit should be exported");
        assert_eq!(
            merge.parent_commit_ids,
            vec![local_head, authority_head],
            "the local state parent stays first and authority history stays as the merge parent"
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot diverge or reconcile"]
    async fn divergent_child_reconciles_without_waiting_for_another_remote_event() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "shared").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (branch_id, _) = default_head(&snapshot);
        let local = replica_from_snapshot(&authority, &snapshot).await;
        let remote_writer = replica_from_snapshot(&authority, &snapshot).await;
        write_key_value(&local, "after-old-head", "local").await;
        write_key_value(&remote_writer, "authority-advanced", "remote").await;
        let published = publish_pending(&remote_writer, &authority).await;
        let authority_head = published.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("remote push should advance authority");
        local
            .import_sync_repository(
                &SyncPushRequest {
                    commits: published.commits.clone(),
                    ref_updates: Vec::new(),
                    inline_blobs: published.inline_blobs.clone(),
                },
                SyncImportPurpose::ReplicaDelta,
                None,
                None,
            )
            .await
            .expect("consumed delta commits should be locally available");

        // Model a consumed cursor whose authority receipt was persisted before
        // local reconciliation (the same graph shape as reset-to-old + write).
        let SyncRepositoryPullResponse::Snapshot { branches, .. } = &snapshot else {
            unreachable!("fixture response is a snapshot");
        };
        let mut authoritative_branches = authoritative_coordinates(branches);
        let checkpoint_commit_id = authoritative_branches[&branch_id]
            .checkpoint_commit_id()
            .expect("authority branch should have a checkpoint")
            .to_owned();
        authoritative_branches.insert(
            branch_id.clone(),
            AuthoritativeBranchCoordinate::Headed {
                head_commit_id: authority_head.clone(),
                checkpoint_commit_id,
            },
        );
        local
            .store_replica_state(
                TEST_REMOTE,
                SyncReplicaState {
                    active_account_id: local.active_account_id().to_owned(),
                    cursor: 1,
                    authoritative_branches,
                    certified_branch_roots: BTreeMap::new(),
                    snapshot_certified_cursor: None,
                    pending_resets: BTreeMap::new(),
                    authority_known_commit_ids: BTreeSet::new(),
                },
            )
            .await
            .expect("authority receipt should store");

        let push = local
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("build should proactively reconcile")
            .expect("reconciled merge should be publishable");
        let update = push
            .ref_updates
            .iter()
            .find(|update| update.branch_id == branch_id)
            .expect("reconciled branch should be present");
        assert_eq!(
            update.expected_head_commit_id.as_deref(),
            Some(authority_head.as_str()),
        );
        assert_ne!(
            update.head_commit_id.as_deref(),
            Some(authority_head.as_str())
        );
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot diverge or publish refs"]
    async fn divergent_branch_does_not_starve_an_independent_ready_ref() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "shared").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (divergent_branch_id, base_head) = default_head(&snapshot);
        let local = replica_from_snapshot(&authority, &snapshot).await;
        let remote_writer = replica_from_snapshot(&authority, &snapshot).await;
        let independent_branch_id = "01920000-0000-7000-8000-000000001501".to_owned();
        local
            .create_branch(CreateBranchOptions {
                id: Some(independent_branch_id.clone()),
                name: "independent ready branch".to_owned(),
                from_commit_id: Some(base_head.clone()),
            })
            .await
            .expect("independent branch should be created");
        local
            .switch_branch(SwitchBranchOptions {
                branch_id: divergent_branch_id.clone(),
            })
            .await
            .expect("fixture should return to the branch that will diverge");

        write_key_value(&local, "local-divergence", "local").await;
        let local_head_before = {
            let read = local
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("local head read should open");
            BranchHeadControlContext::new()
                .reader(&read)
                .load(&divergent_branch_id)
                .await
                .expect("local head should load")
                .expect("local branch should exist")
                .head_commit_id
        };

        write_key_value(&remote_writer, "authority-divergence", "remote").await;
        let published = publish_pending(&remote_writer, &authority).await;
        let authority_head = published.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("remote push should advance authority");
        local
            .import_sync_repository(
                &SyncPushRequest {
                    commits: published.commits,
                    ref_updates: Vec::new(),
                    inline_blobs: published.inline_blobs,
                },
                SyncImportPurpose::ReplicaDelta,
                None,
                None,
            )
            .await
            .expect("authority commits should import without moving local refs");

        let SyncRepositoryPullResponse::Snapshot { branches, .. } = &snapshot else {
            unreachable!("fixture response is a snapshot");
        };
        let mut authoritative_branches = authoritative_coordinates(branches);
        let checkpoint_commit_id = authoritative_branches[&divergent_branch_id]
            .checkpoint_commit_id()
            .expect("authority branch should have a checkpoint")
            .to_owned();
        authoritative_branches.insert(
            divergent_branch_id.clone(),
            AuthoritativeBranchCoordinate::Headed {
                head_commit_id: authority_head,
                checkpoint_commit_id,
            },
        );
        local
            .store_replica_state(
                TEST_REMOTE,
                SyncReplicaState {
                    active_account_id: local.active_account_id().to_owned(),
                    cursor: 1,
                    authoritative_branches,
                    certified_branch_roots: BTreeMap::new(),
                    snapshot_certified_cursor: None,
                    pending_resets: BTreeMap::new(),
                    authority_known_commit_ids: BTreeSet::new(),
                },
            )
            .await
            .expect("authority receipt should store");

        let push = local
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("independent push should build")
            .expect("independent branch ref should be ready");
        assert!(
            push.ref_updates
                .iter()
                .any(|update| update.branch_id == independent_branch_id),
            "the ready branch must publish before divergent reconciliation",
        );
        assert!(
            push.ref_updates
                .iter()
                .all(|update| update.branch_id != divergent_branch_id),
            "the stale expected head must not be published",
        );

        let local_head_after = {
            let read = local
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("post-build local head read should open");
            BranchHeadControlContext::new()
                .reader(&read)
                .load(&divergent_branch_id)
                .await
                .expect("post-build local head should load")
                .expect("post-build local branch should exist")
                .head_commit_id
        };
        assert_eq!(
            local_head_after, local_head_before,
            "building independent work must not reconcile the divergent branch first",
        );
    }

    #[test]
    fn authoritative_branch_coordinate_is_complete_and_round_trips() {
        let headed = AuthoritativeBranchCoordinate::from_wire(
            Some("head".to_owned()),
            Some("checkpoint".to_owned()),
            "test coordinate",
        )
        .expect("a complete headed coordinate should parse");
        assert_eq!(
            serde_json::to_value(&headed).expect("headed coordinate should encode"),
            serde_json::json!({
                "state": "headed",
                "headCommitId": "head",
                "checkpointCommitId": "checkpoint",
            }),
        );
        assert_eq!(
            serde_json::from_value::<AuthoritativeBranchCoordinate>(
                serde_json::to_value(&headed).expect("headed coordinate should encode"),
            )
            .expect("headed coordinate should decode"),
            headed,
        );
        assert_eq!(
            AuthoritativeBranchCoordinate::from_wire(None, None, "test coordinate")
                .expect("a deleted coordinate should parse"),
            AuthoritativeBranchCoordinate::Deleted,
        );
        for (head, checkpoint) in [
            (Some("head".to_owned()), None),
            (None, Some("checkpoint".to_owned())),
        ] {
            assert_eq!(
                AuthoritativeBranchCoordinate::from_wire(head, checkpoint, "test coordinate")
                    .expect_err("a partial coordinate must be rejected")
                    .code,
                LixError::CODE_INVALID_PARAM,
            );
        }
        assert!(
            serde_json::from_value::<AuthoritativeBranchCoordinate>(serde_json::json!({
                "state": "headed",
                "headCommitId": "head",
            }))
            .is_err(),
            "persisted headed coordinates require a checkpoint",
        );
    }

    #[tokio::test]
    async fn replica_state_persists_and_enforces_the_authority_account() {
        let replica = open_lix().await.expect("replica should open");
        replica
            .store_replica_state(
                TEST_REMOTE,
                SyncReplicaState {
                    active_account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
                    cursor: 7,
                    authoritative_branches: BTreeMap::new(),
                    certified_branch_roots: BTreeMap::new(),
                    snapshot_certified_cursor: None,
                    pending_resets: BTreeMap::new(),
                    authority_known_commit_ids: BTreeSet::new(),
                },
            )
            .await
            .expect("replica identity should store durably");
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica state read should open");
        assert_eq!(
            load_sync_replica_account(&read)
                .await
                .expect("replica account should decode")
                .as_deref(),
            Some(crate::SYSTEM_ACCOUNT_ID),
        );
        drop(read);
        replica
            .validate_sync_repository_account(TEST_REMOTE, crate::SYSTEM_ACCOUNT_ID)
            .await
            .expect("the same authority account should reconnect");
        let error = replica
            .validate_sync_repository_account(TEST_REMOTE, crate::ANONYMOUS_ACCOUNT_ID)
            .await
            .expect_err("a changed authority account must fail closed");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("account changed"));
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot diverge or reconcile"]
    async fn divergent_replicas_merge_different_rows_and_push_from_new_authority_head() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "non-root").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let left = replica_from_snapshot(&authority, &snapshot).await;
        let right = replica_from_snapshot(&authority, &snapshot).await;

        write_key_value(&left, "left", "from-left").await;
        write_key_value(&right, "right", "from-right").await;
        let right_pending = right
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("right pending push should build")
            .expect("right should have pending work");
        for commit in &right_pending.commits {
            assert_root_parent_is_complete(&right, &commit.commit_id).await;
        }
        let left_push = publish_pending(&left, &authority).await;
        let left_head = left_push.ref_updates[0]
            .head_commit_id
            .clone()
            .expect("left push advances its ref");

        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        right
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("right replica should reconcile divergence");
        let right_push = right
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("reconciled push should build")
            .expect("merge commit should remain pending");
        assert_eq!(
            right_push.ref_updates[0].expected_head_commit_id.as_deref(),
            Some(left_head.as_str()),
            "the retry CAS must start from the newly pulled authority head",
        );
        authority
            .push_sync_repository(&right_push)
            .await
            .expect("authority should accept reconciled merge");

        assert_eq!(read_key_value(&authority, "left").await, "from-left");
        assert_eq!(read_key_value(&authority, "right").await, "from-right");
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot diverge or reconcile"]
    async fn divergent_file_insert_and_edit_preserve_checkpoint_cursor_and_both_rows() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/existing.md".to_owned()),
                    Value::Text("base".to_owned()),
                ],
            )
            .await
            .expect("base file should commit");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (branch_id, _) = default_head(&snapshot);
        let inserting_replica = replica_from_snapshot(&authority, &snapshot).await;
        let editing_replica = replica_from_snapshot(&authority, &snapshot).await;
        let (snapshot_history, _, _) = snapshot_parts(&authority, &snapshot).await;
        transfer_commit_blobs(&authority, &inserting_replica, &snapshot_history.commits).await;
        transfer_commit_blobs(&authority, &editing_replica, &snapshot_history.commits).await;

        inserting_replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/created.md".to_owned()),
                    Value::Text("created".to_owned()),
                ],
            )
            .await
            .expect("new file should commit locally");
        editing_replica
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE path = $2",
                &[
                    Value::Text("edited".to_owned()),
                    Value::Text("/existing.md".to_owned()),
                ],
            )
            .await
            .expect("existing file should edit locally");

        for replica in [&inserting_replica, &editing_replica] {
            let read = replica
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("pre-reconcile control read should open");
            let control = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .expect("pre-reconcile control should load")
                .expect("pre-reconcile control should exist");
            assert!(
                control.working_diff_checkpoint_commit_id.is_some(),
                "ordinary local file writes must preserve the checkpoint cursor",
            );
            assert!(
                working_diff_count(replica).await > 0,
                "ordinary local file writes must remain dirty against the checkpoint",
            );
        }

        publish_pending_with_blobs(&editing_replica, &authority).await;
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        let SyncRepositoryPullResponse::Delta { events, .. } = &delta else {
            panic!("a cursor pull should return a delta");
        };
        for event in events {
            transfer_commit_blobs(&authority, &inserting_replica, &event.commits).await;
        }
        inserting_replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("file divergence should reconcile");

        let adapter = inserting_replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch control read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("branch control should load")
            .expect("branch control should exist");
        assert!(
            control.working_diff_checkpoint_commit_id.is_some(),
            "reconciliation must preserve the private checkpoint cursor",
        );
        drop(read);

        let paths = inserting_replica
            .execute(
                "SELECT path FROM lix_file WHERE path IN ('/created.md', '/existing.md') ORDER BY path",
                &[],
            )
            .await
            .expect("both reconciled files should remain queryable");
        assert_eq!(paths.rows().len(), 2);
        assert!(
            working_diff_count(&inserting_replica).await > 0,
            "reconciliation must retain the pending insert in working diff",
        );
        assert_eq!(
            read_file_content(&inserting_replica, "/existing.md").await,
            Value::Blob(b"edited".to_vec().into()),
        );

        let retry = inserting_replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("reconciled push should build")
            .expect("created file should remain pending");
        transfer_commit_blobs(&inserting_replica, &authority, &retry.commits).await;
        authority
            .push_sync_repository(&retry)
            .await
            .expect("authority should accept the reconciled file edit");
        let authority_paths = authority
            .execute(
                "SELECT path FROM lix_file WHERE path IN ('/created.md', '/existing.md') ORDER BY path",
                &[],
            )
            .await
            .expect("authority should expose both files");
        assert_eq!(authority_paths.rows().len(), 2);
        assert_eq!(
            read_file_content(&authority, "/existing.md").await,
            Value::Blob(b"edited".to_vec().into()),
        );
    }

    #[tokio::test]
    async fn fresh_snapshot_preserves_dirty_state_against_the_shared_checkpoint() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "dirty-bootstrap", "baseline").await;
        let checkpoint = authority
            .create_checkpoint()
            .await
            .expect("explicit checkpoint should commit");
        let checkpoint = CommitId::parse_lix(&checkpoint.commit_id, "bootstrap checkpoint")
            .expect("checkpoint id should parse");
        write_key_value(&authority, "dirty-bootstrap", "dirty after checkpoint").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("dirty snapshot should load");
        let (branch_id, head) = default_head(&snapshot);
        assert_ne!(head, checkpoint.to_string());

        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica branch read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("replica branch should load")
            .expect("replica branch should exist");
        assert_eq!(control.head_commit_id.to_string(), head);
        assert_eq!(control.working_diff_checkpoint_commit_id, Some(checkpoint),);
        drop(read);
        assert!(working_diff_count(&replica).await > 0);
        assert_eq!(
            read_key_value(&replica, "dirty-bootstrap").await,
            "dirty after checkpoint",
        );
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica-authored write semantics were removed"]
    async fn ordinary_remote_write_preserves_the_shared_checkpoint_baseline() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/shared.md".to_owned()),
                    Value::Text("baseline".to_owned()),
                ],
            )
            .await
            .expect("baseline file should commit");
        let checkpoint = authority
            .create_checkpoint()
            .await
            .expect("explicit checkpoint should commit");
        let checkpoint = CommitId::parse_lix(&checkpoint.commit_id, "baseline checkpoint")
            .expect("checkpoint id should parse");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = &snapshot else {
            panic!("initial pull should return a snapshot");
        };
        let cursor = *cursor;
        let (branch_id, _) = default_head(&snapshot);
        let origin = replica_from_snapshot(&authority, &snapshot).await;
        let peer = replica_from_snapshot(&authority, &snapshot).await;

        origin
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE path = $2",
                &[
                    Value::Text("ordinary write".to_owned()),
                    Value::Text("/shared.md".to_owned()),
                ],
            )
            .await
            .expect("ordinary file write should commit locally");
        publish_pending_with_blobs(&origin, &authority).await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("published write should load as a delta");
        let SyncRepositoryPullResponse::Delta { events, .. } = &delta else {
            panic!("cursor pull should return a delta");
        };
        for event in events {
            transfer_commit_blobs(&authority, &origin, &event.commits).await;
            transfer_commit_blobs(&authority, &peer, &event.commits).await;
        }
        origin
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("origin should acknowledge its published write");
        peer.apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("peer should apply the published write");

        for (name, replica) in [("origin", &origin), ("peer", &peer)] {
            let adapter = replica.storage_adapter();
            let read = adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("branch control read should open");
            let control = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .expect("branch control should load")
                .expect("branch control should exist");
            assert_eq!(
                control.working_diff_checkpoint_commit_id,
                Some(checkpoint),
                "{name} must retain the explicit checkpoint as its working-diff cursor",
            );
            drop(read);
            assert!(
                working_diff_count(replica).await > 0,
                "{name} must keep the ordinary write dirty against the explicit checkpoint",
            );
        }
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica-authored outbox semantics were removed"]
    async fn remote_checkpoint_advances_the_shared_baseline_and_clears_working_diff() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "checkpoint-sync", "baseline").await;
        authority
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (branch_id, _) = default_head(&snapshot);
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = &snapshot else {
            panic!("initial pull should return a snapshot");
        };
        let mut cursor = *cursor;
        let origin = replica_from_snapshot(&authority, &snapshot).await;
        let peer = replica_from_snapshot(&authority, &snapshot).await;

        origin
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/checkpoint-sync.md".to_owned()),
                    Value::Text("dirty".to_owned()),
                ],
            )
            .await
            .expect("replica file write should commit");
        publish_pending_with_blobs(&origin, &authority).await;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("ordinary delta should load");
        cursor = match &delta {
            SyncRepositoryPullResponse::Delta { cursor, .. } => *cursor,
            _ => panic!("cursor pull should return a delta"),
        };
        if let SyncRepositoryPullResponse::Delta { events, .. } = &delta {
            for event in events {
                transfer_commit_blobs(&authority, &peer, &event.commits).await;
                transfer_commit_blobs(&authority, &origin, &event.commits).await;
            }
        }
        peer.apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("peer should apply ordinary write");
        assert!(working_diff_count(&peer).await > 0);

        origin
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("origin should acknowledge ordinary write");
        let _checkpoint = origin
            .create_checkpoint()
            .await
            .expect("replica checkpoint should commit");
        assert_eq!(
            working_diff_count(&origin).await,
            0,
            "origin checkpoint should be locally clean",
        );
        let origin_read = origin
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("origin control read should open");
        let origin_control = BranchHeadControlContext::new()
            .reader(&origin_read)
            .load(&branch_id)
            .await
            .expect("origin control should load")
            .expect("origin control should exist");
        let origin_checkpoint_diff = TrackedStateContext::new()
            .reader(&origin_read)
            .diff_commits(
                &origin_control
                    .working_diff_checkpoint_commit_id
                    .expect("origin checkpoint")
                    .to_string(),
                &origin_control.head_commit_id.to_string(),
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("origin checkpoint root diff should load");
        assert!(
            origin_checkpoint_diff.entries.iter().all(|entry| {
                entry.identity.schema_key() == crate::checkpoint::CHECKPOINT_SCHEMA_KEY
            }),
            "origin checkpoint child should differ only by checkpoint metadata: {:?}",
            origin_checkpoint_diff.entries,
        );
        let origin_checkpoint_id = origin_control
            .working_diff_checkpoint_commit_id
            .expect("origin checkpoint")
            .to_string();
        let scanned_checkpoint = BranchHeadControlContext::new()
            .reader(&origin_read)
            .scan()
            .await
            .expect("origin controls should scan")
            .into_iter()
            .find(|(id, _)| id == &branch_id)
            .and_then(|(_, control)| control.working_diff_checkpoint_commit_id)
            .expect("scanned origin checkpoint")
            .to_string();
        assert_eq!(scanned_checkpoint, origin_checkpoint_id);
        let preview_push = origin
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("checkpoint push should build")
            .expect("checkpoint push should exist");
        if let Some(update) = preview_push
            .ref_updates
            .iter()
            .find(|update| update.branch_id == branch_id)
        {
            assert_eq!(
                update.checkpoint_commit_id.as_deref(),
                Some(origin_checkpoint_id.as_str()),
                "first outbox build must use the exact local checkpoint coordinate",
            );
        }
        drop(origin_read);
        let mut checkpoint_ref = None;
        for _ in 0..8 {
            let before_push_read = origin
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("before push read");
            let before_push_checkpoint = BranchHeadControlContext::new()
                .reader(&before_push_read)
                .scan()
                .await
                .expect("before push controls")
                .into_iter()
                .find(|(id, _)| id == &branch_id)
                .and_then(|(_, control)| control.working_diff_checkpoint_commit_id)
                .expect("before push checkpoint")
                .to_string();
            assert_eq!(before_push_checkpoint, origin_checkpoint_id);
            drop(before_push_read);
            let checkpoint_push = publish_pending_with_blobs(&origin, &authority).await;
            let published_active = checkpoint_push
                .ref_updates
                .into_iter()
                .find(|update| update.branch_id == branch_id);
            if let Some(update) = &published_active {
                assert_eq!(
                    update.checkpoint_commit_id.as_deref(),
                    Some(origin_checkpoint_id.as_str()),
                    "bounded outbox ref must use the exact local checkpoint coordinate",
                );
            }
            let delta = authority
                .pull_sync_repository(Some(cursor), 128)
                .await
                .expect("checkpoint upload receipt should pull");
            if let SyncRepositoryPullResponse::Delta {
                cursor: next_cursor,
                events,
            } = &delta
            {
                for event in events {
                    transfer_commit_blobs(&authority, &origin, &event.commits).await;
                    transfer_commit_blobs(&authority, &peer, &event.commits).await;
                }
                cursor = *next_cursor;
            }
            origin
                .apply_sync_repository_pull(TEST_REMOTE, &delta)
                .await
                .expect("origin should acknowledge checkpoint upload page");
            peer.apply_sync_repository_pull(TEST_REMOTE, &delta)
                .await
                .expect("peer should apply checkpoint upload page");
            let loop_read = origin
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("loop origin read");
            let loop_control = BranchHeadControlContext::new()
                .reader(&loop_read)
                .load(&branch_id)
                .await
                .expect("loop origin control")
                .expect("loop origin branch");
            assert_eq!(
                loop_control
                    .working_diff_checkpoint_commit_id
                    .expect("loop checkpoint")
                    .to_string(),
                origin_checkpoint_id,
                "checkpoint upload acknowledgement must not rewrite local checkpoint",
            );
            drop(loop_read);
            if let Some(update) = published_active {
                checkpoint_ref = Some(update);
                break;
            }
        }
        let checkpoint_ref = checkpoint_ref
            .expect("bounded checkpoint upload should eventually publish the active branch");
        let pushed_head = checkpoint_ref
            .head_commit_id
            .as_deref()
            .expect("headed ref");
        let pushed_checkpoint = checkpoint_ref
            .checkpoint_commit_id
            .as_deref()
            .expect("headed ref checkpoint");
        assert_eq!(
            pushed_checkpoint, origin_checkpoint_id,
            "outbox must publish the exact local checkpoint coordinate",
        );
        let imported_checkpoint_history = authority
            .sync_history(pushed_checkpoint, 1)
            .await
            .expect("imported checkpoint history");
        assert!(
            imported_checkpoint_history
                .boundaries
                .iter()
                .any(|boundary| boundary.commit_id == pushed_checkpoint),
            "checkpoint history must paginate its aliased state as a self-contained boundary",
        );
        let imported_checkpoint = imported_checkpoint_history
            .commits
            .into_iter()
            .next()
            .expect("imported checkpoint body");
        assert!(
            imported_checkpoint.members.is_empty(),
            "checkpoint boundary should not expand selected file members on the wire: {:?}",
            imported_checkpoint.members,
        );
        assert!(
            imported_checkpoint.state_alias.is_some(),
            "checkpoint wire body must carry its authenticated complete-state alias",
        );
        let read = authority
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority diff read should open");
        let root_diff = TrackedStateContext::new()
            .reader(&read)
            .diff_commits(
                pushed_checkpoint,
                pushed_head,
                &TrackedStateDiffRequest::default(),
            )
            .await
            .expect("checkpoint-to-head diff should load");
        assert!(
            root_diff.entries.iter().all(
                |entry| entry.identity.schema_key() == crate::checkpoint::CHECKPOINT_SCHEMA_KEY
            ),
            "checkpoint child should differ only by checkpoint metadata: {:?}",
            root_diff.entries,
        );
        drop(read);
        let read = authority
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority control read should open");
        let authority_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("authority control should load")
            .expect("authority control should exist");
        assert_eq!(authority_control.head_commit_id.to_string(), pushed_head);
        assert_eq!(
            authority_control
                .working_diff_checkpoint_commit_id
                .map(|commit| commit.to_string())
                .as_deref(),
            Some(pushed_checkpoint),
        );
        drop(read);
        assert_eq!(
            working_diff_count(&authority).await,
            0,
            "authority checkpoint should be clean",
        );
        let peer_diff = peer
            .execute(
                "SELECT id, diff_type \
                 FROM lix_diff('lix_file', $1, lix_active_branch_commit_id())",
                &[Value::Text(pushed_checkpoint.to_owned())],
            )
            .await
            .expect("peer working diff should load");
        assert_eq!(
            peer_diff.rows().len(),
            0,
            "peer checkpoint should be clean, found {:?}",
            peer_diff.rows(),
        );
        let peer_read = peer
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("peer change scan should open");
        let peer_changes = crate::tracked_state::scan_change_records_from_commit_deltas(&peer_read)
            .await
            .expect("peer must resolve every imported checkpoint member");
        assert!(
            peer_changes.iter().any(|change| {
                change.schema_key == "lix_file_descriptor" && change.file_id.is_some()
            }),
            "peer change scan should include the synchronized file",
        );
        drop(peer_read);
        let read = peer
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("peer branch read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .expect("peer branch should load")
            .expect("peer branch should exist");
        assert_eq!(control.head_commit_id.to_string(), pushed_head);
        assert_eq!(
            control
                .working_diff_checkpoint_commit_id
                .map(|commit| commit.to_string())
                .as_deref(),
            Some(pushed_checkpoint),
        );
    }

    #[tokio::test]
    #[ignore = "generic historical diff is server-first in v3"]
    async fn sparse_snapshot_can_diff_from_checkpoint_without_authored_bodies() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/history.md".to_owned()),
                    Value::Text("before".to_owned()),
                ],
            )
            .await
            .expect("baseline file should commit");
        let checkpoint = authority
            .create_checkpoint()
            .await
            .expect("checkpoint should commit")
            .commit_id;
        authority
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE path = $2",
                &[
                    Value::Text("after".to_owned()),
                    Value::Text("/history.md".to_owned()),
                ],
            )
            .await
            .expect("post-checkpoint file update should commit");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, head) = default_head(&snapshot);
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        let diff = replica
            .execute(
                "SELECT diff_type FROM lix_diff('lix_file', $1, $2)",
                &[Value::Text(checkpoint), Value::Text(head)],
            )
            .await
            .expect("checkpoint diff should use its authenticated sparse snapshot");
        assert_eq!(diff.rows().len(), 1);
        assert_eq!(
            diff.rows()[0]
                .get::<String>("diff_type")
                .expect("diff type should decode"),
            "modified",
        );
    }

    #[tokio::test]
    #[ignore = "historical state is server-first in v3"]
    async fn sparse_snapshot_history_reads_files_added_at_checkpoint_state() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/baseline.md".to_owned()),
                    Value::Text("baseline".to_owned()),
                ],
            )
            .await
            .expect("baseline file should commit");
        authority
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit");
        let added_file = authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA)) RETURNING id",
                &[
                    Value::Text("/added.md".to_owned()),
                    Value::Text("added".to_owned()),
                ],
            )
            .await
            .expect("added file should commit");
        let added_file_id = added_file.rows()[0]
            .get::<String>("id")
            .expect("added file id should decode");
        let checkpoint = authority
            .create_checkpoint()
            .await
            .expect("latest checkpoint should commit")
            .commit_id;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let (_, snapshot_head) = default_head(&snapshot);
        assert_eq!(checkpoint, snapshot_head);
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        crate::sql2::reset_file_history_anchor_probe_census();

        let history = loop {
            match replica
                .execute(
                    "SELECT id, path FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
                    &[
                        Value::Text(checkpoint.clone()),
                        Value::Text(added_file_id.clone()),
                    ],
                )
                .await
            {
                Ok(history) => break history,
                Err(error) if error.code == "LIX_SYNC_HISTORY_REQUIRED" => {
                    let commit_ids = error
                        .details
                        .as_ref()
                        .and_then(|details| details["commitIds"].as_array())
                        .expect("history demand ids")
                        .iter()
                        .map(|id| id.as_str().expect("history demand id").to_owned())
                        .collect::<Vec<_>>();
                    for commit_id in commit_ids {
                        let page = authority
                            .sync_history(&commit_id, 1)
                            .await
                            .expect("deferred history should load from authority");
                        let mut boundary_rows = Vec::new();
                        for boundary in &page.boundaries {
                            let rows = authority
                                .pull_sync_snapshot_rows(
                                    &boundary.commit_id,
                                    &boundary.commit_id,
                                    None,
                                    super::super::MAX_SYNC_REQUEST_ITEMS,
                                )
                                .await
                                .expect("history boundary rows should load");
                            assert_eq!(rows.continuation, None);
                            boundary_rows.extend(rows.rows);
                        }
                        replica
                            .import_sync_history_headers(&page.commit_headers)
                            .await
                            .expect("history headers should import");
                        replica
                            .import_sync_history_boundaries(
                                &page.commits,
                                &page.boundaries,
                                &boundary_rows,
                            )
                            .await
                            .expect("history bodies should import");
                    }
                }
                Err(error) => panic!("checkpoint history should load: {error:?}"),
            }
        };
        let (anchor_probes, anchor_probe_hits) = crate::sql2::file_history_anchor_probe_census();
        assert!(anchor_probes >= 1, "top-k history should probe its anchor");
        assert_eq!(
            anchor_probe_hits, 1,
            "the selected checkpoint change should resolve at the anchor"
        );
        assert_eq!(history.rows().len(), 1);
        assert_eq!(
            history.rows()[0]
                .get::<String>("path")
                .expect("history path should decode"),
            "/added.md",
        );
    }

    #[tokio::test]
    #[ignore = "historical state is server-first in v3"]
    async fn warm_replica_history_reads_files_added_by_a_later_checkpoint() {
        let authority = open_lix().await.expect("authority should open");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/baseline.md".to_owned()),
                    Value::Text("baseline".to_owned()),
                ],
            )
            .await
            .expect("baseline file should commit");
        let previous_checkpoint = authority
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit")
            .commit_id;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("baseline snapshot should load");
        let SyncRepositoryPullResponse::Snapshot { cursor, .. } = snapshot.clone() else {
            panic!("initial pull should be a snapshot");
        };
        let replica = replica_from_snapshot(&authority, &snapshot).await;

        let added_file = authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA)) RETURNING id",
                &[
                    Value::Text("/added-later.md".to_owned()),
                    Value::Text("added later".to_owned()),
                ],
            )
            .await
            .expect("later file should commit");
        let added_file_id = added_file.rows()[0]
            .get::<String>("id")
            .expect("later file id should decode");
        let latest_checkpoint = authority
            .create_checkpoint()
            .await
            .expect("later checkpoint should commit")
            .commit_id;
        let delta = authority
            .pull_sync_repository(Some(cursor), 128)
            .await
            .expect("later checkpoint delta should load");
        hydrate_delta_blobs(&authority, &replica, &delta).await;
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("warm replica should apply the checkpoint delta");

        let diff = loop {
            match replica
                .execute(
                    "SELECT id AS file_id FROM lix_diff('lix_file', $1, $2)",
                    &[
                        Value::Text(previous_checkpoint.clone()),
                        Value::Text(latest_checkpoint.clone()),
                    ],
                )
                .await
            {
                Ok(diff) => break diff,
                Err(error)
                    if error.code == "LIX_SYNC_HISTORY_REQUIRED"
                        || error.code == LixError::CODE_COMMIT_NOT_FOUND =>
                {
                    let details = error.details.as_ref().expect("history demand details");
                    let commit_ids = if error.code == "LIX_SYNC_HISTORY_REQUIRED" {
                        details["commitIds"]
                            .as_array()
                            .expect("history demand ids")
                            .iter()
                            .map(|id| id.as_str().expect("history demand id").to_owned())
                            .collect::<Vec<_>>()
                    } else {
                        vec![
                            details["commit_id"]
                                .as_str()
                                .expect("missing graph commit id")
                                .to_owned(),
                        ]
                    };
                    for commit_id in commit_ids {
                        let mut page = authority
                            .sync_history(&commit_id, 100)
                            .await
                            .expect("deferred diff history should load from authority");
                        close_history_boundary_bodies(&authority, &mut page).await;
                        let mut boundary_rows = Vec::new();
                        for boundary in &page.boundaries {
                            let rows = authority
                                .pull_sync_snapshot_rows(
                                    &boundary.commit_id,
                                    &boundary.commit_id,
                                    None,
                                    super::super::MAX_SYNC_REQUEST_ITEMS,
                                )
                                .await
                                .expect("history boundary rows should load");
                            assert_eq!(rows.continuation, None);
                            boundary_rows.extend(rows.rows);
                        }
                        replica
                            .import_sync_history_headers(&page.commit_headers)
                            .await
                            .expect("history headers should import");
                        replica
                            .import_sync_history_boundaries(
                                &page.commits,
                                &page.boundaries,
                                &boundary_rows,
                            )
                            .await
                            .expect("history bodies should import");
                    }
                }
                Err(error) => panic!("checkpoint diff should load: {error:?}"),
            }
        };
        assert!(
            diff.rows().iter().any(|row| {
                row.get::<String>("file_id")
                    .is_ok_and(|file_id| file_id == added_file_id)
            }),
            "checkpoint diff should expose the later file id",
        );

        let history = replica
            .execute(
                "SELECT id, path FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
                &[
                    Value::Text(latest_checkpoint),
                    Value::Text(added_file_id),
                ],
            )
            .await
            .expect("hydrated warm checkpoint history should be queryable");
        assert_eq!(history.rows().len(), 1);
        assert_eq!(
            history.rows()[0]
                .get::<String>("path")
                .expect("history path should decode"),
            "/added-later.md",
        );
    }

    #[tokio::test]
    #[ignore = "pre-v3 replica outbox semantics were removed"]
    async fn authority_rejects_a_stale_expected_checkpoint_coordinate() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "checkpoint-cas", "baseline").await;
        authority
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit");
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        write_key_value(&replica, "checkpoint-cas", "changed").await;
        let mut request = replica
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("push should build")
            .expect("write should be pending");
        let update = request
            .ref_updates
            .first_mut()
            .expect("pending write should update a ref");
        update.expected_checkpoint_commit_id =
            Some(CommitId::for_test_label("stale-expected-sync-checkpoint").to_string());
        let error = authority
            .push_sync_repository(&request)
            .await
            .expect_err("stale checkpoint CAS must fail");
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
        assert!(error.message.contains("expected coordinate"));
    }

    #[tokio::test]
    #[ignore = "v3 replicas cannot diverge or reconcile"]
    async fn divergent_same_row_deterministically_keeps_authority_value() {
        let authority = open_lix().await.expect("authority should open");
        write_key_value(&authority, "base", "non-root").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot should load");
        let left = replica_from_snapshot(&authority, &snapshot).await;
        let right = replica_from_snapshot(&authority, &snapshot).await;

        write_key_value(&left, "shared", "authority-wins").await;
        write_key_value(&right, "shared", "local-pending").await;
        let right_pending = right
            .build_sync_push(TEST_REMOTE, crate::sync::MAX_SYNC_REQUEST_ITEMS)
            .await
            .expect("right pending push should build")
            .expect("right should have pending work");
        for commit in &right_pending.commits {
            assert_root_parent_is_complete(&right, &commit.commit_id).await;
        }
        publish_pending(&left, &authority).await;
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("authority delta should load");
        right
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("same-row divergence should reconcile");
        assert_eq!(
            read_key_value(&right, "shared").await,
            "authority-wins",
            "the authority wins an unresolved same-row conflict",
        );

        {
            let read = right
                .storage_adapter()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("reconciled controls should load");
            for (branch_id, control) in BranchHeadControlContext::new()
                .reader(&read)
                .scan()
                .await
                .expect("reconciled controls should scan")
            {
                assert!(
                    control.working_diff_checkpoint_commit_id.is_some(),
                    "reconciled branch '{branch_id}' must retain its checkpoint"
                );
            }
        }

        publish_pending(&right, &authority).await;
        assert_eq!(read_key_value(&authority, "shared").await, "authority-wins",);
    }
}
