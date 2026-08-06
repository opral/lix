//! Checkpoint recovery roots and repository garbage collection.
//!
//! Recovery refs are local, mutable roots. They deliberately live outside the
//! changelog: rotating a ref must not create history that itself keeps the
//! recovered commit alive. The checkpoint transaction stages the rotation in
//! the same storage write set that publishes the compacted checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use bytes::Bytes;

use crate::branch::BranchHeadControlContext;
#[cfg(any(test, feature = "storage-benches"))]
use crate::changelog::ChangeScanRequest;
use crate::changelog::{
    CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_SPACE, ChangeId, ChangelogContext,
    ChangelogReader, CommitId, CommitLoadRequest, GcLiveSet, GcPlan, GcRepairSet, GcRoot,
    GcSweepSet, change_key, commit_change_id_key, commit_key,
};
#[cfg(test)]
use crate::changelog::{ChangeRecord, CommitScanRequest};
use crate::commit_graph::CommitGraphContext;
#[cfg(test)]
use crate::json_store::JsonRef;
#[cfg(test)]
use crate::json_store::{
    JsonSlot, JsonStoreContext, JsonStoreWriter, UntrackedJsonReclaimCandidate,
};
#[cfg(test)]
use crate::live_state::TrackedHeadContext;
#[cfg(test)]
use crate::live_state::stage_collect_stale_working_diff_indexes;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions,
    StorageKey, StoragePrecondition, StoragePrefix, StorageProjectedValue, StorageScanOptions,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, storage_codec};

pub(crate) const CHECKPOINT_RECOVERY_REF_NAMESPACE: &str = "checkpoint.recovery_ref.v3";
pub(crate) const CHECKPOINT_RECOVERY_REF_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0008_0001),
    CHECKPOINT_RECOVERY_REF_NAMESPACE,
);
pub(crate) const CHECKPOINT_GC_STATE_NAMESPACE: &str = "checkpoint.gc_state.v1";
pub(crate) const CHECKPOINT_GC_STATE_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0002), CHECKPOINT_GC_STATE_NAMESPACE);
/// Authenticated publication deltas are the sole ordinary-GC input.  The
/// queue is mutable because its head/tail are CAS-protected, while each
/// record is immutable after publication and addressed by a monotonic slot.
pub(crate) const GC_REACHABILITY_DELTA_NAMESPACE: &str = "gc.reachability_delta.v1";
pub(crate) const GC_REACHABILITY_DELTA_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0003), GC_REACHABILITY_DELTA_NAMESPACE);
pub(crate) const GC_REACHABILITY_QUEUE_NAMESPACE: &str = "gc.reachability_queue.v1";
pub(crate) const GC_REACHABILITY_QUEUE_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0004), GC_REACHABILITY_QUEUE_NAMESPACE);

const CHECKPOINT_RECOVERY_REF_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_GC_STATE_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_GC_STATE_KEY: &[u8] = b"repository";
const GC_REACHABILITY_FORMAT_VERSION: u32 = 2;
const GC_REACHABILITY_QUEUE_KEY: &[u8] = b"queue";
const GC_REACHABILITY_BATCH_LIMIT: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRecoveryRef {
    pub(crate) branch_id: String,
    pub(crate) recovered_head_commit_id: CommitId,
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) interval_has_commits: bool,
}

/// Repository-global maintenance debt.
///
/// Checkpoint publication and recovery remain branch-local, but collection is
/// repository-wide. One singleton prevents redundant full sweeps when several
/// branches checkpoint concurrently or become due at the same time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CheckpointGcState {
    pub(crate) checkpoint_sequence: u64,
    pub(crate) last_gc_sequence: u64,
    pub(crate) collectible_interval_count: u64,
}

impl CheckpointGcState {
    pub(crate) fn add_collectible_interval(&mut self, interval_has_commits: bool) {
        if !interval_has_commits {
            return;
        }
        self.collectible_interval_count = self.collectible_interval_count.saturating_add(1);
    }

    pub(crate) fn has_collectible_debt(self) -> bool {
        self.collectible_interval_count > 0
    }

    pub(crate) fn mark_collected(&mut self) {
        self.last_gc_sequence = self.checkpoint_sequence;
        self.collectible_interval_count = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointPublication {
    pub(crate) recovery_ref: CheckpointRecoveryRef,
    pub(crate) gc_state: CheckpointGcState,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct CheckpointRecoveryRefKey<'a> {
    branch_id: &'a str,
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCheckpointRecoveryRef {
    format_version: u32,
    branch_id: String,
    recovered_head_commit_id: CommitId,
    checkpoint_commit_id: CommitId,
    interval_has_commits: bool,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCheckpointGcState {
    format_version: u32,
    checkpoint_sequence: u64,
    last_gc_sequence: u64,
    collectible_interval_count: u64,
}

/// One branch-root transition.  The digest fields bind the delta to the
/// exact control bytes observed by the publisher; a queue record with a
/// forged root or stale CAS token is rejected before any physical delete is
/// staged.
#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredRootReachabilityDelta {
    branch_id: String,
    #[musli(with = storage_codec::option)]
    old_root: Option<CommitId>,
    #[musli(with = storage_codec::option)]
    new_root: Option<CommitId>,
    #[musli(with = storage_codec::option)]
    old_control: Option<crate::branch::BranchHeadControl>,
    #[musli(with = storage_codec::option)]
    new_control: Option<crate::branch::BranchHeadControl>,
    old_control_digest: [u8; 32],
    new_control_digest: [u8; 32],
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredRootReachabilityBatch {
    format_version: u32,
    sequence: u64,
    deltas: Vec<StoredRootReachabilityDelta>,
    checkpoint_roots: Vec<CommitId>,
    digest: [u8; 32],
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredRootReachabilityBatchBody {
    format_version: u32,
    sequence: u64,
    deltas: Vec<StoredRootReachabilityDelta>,
    checkpoint_roots: Vec<CommitId>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredReachabilityQueue {
    format_version: u32,
    head_sequence: u64,
    tail_sequence: u64,
    next_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootReachabilityDelta {
    pub(crate) branch_id: String,
    pub(crate) old_root: Option<CommitId>,
    pub(crate) new_root: Option<CommitId>,
    pub(crate) old_control: Option<crate::branch::BranchHeadControl>,
    pub(crate) new_control: Option<crate::branch::BranchHeadControl>,
    pub(crate) old_control_digest: [u8; 32],
    pub(crate) new_control_digest: [u8; 32],
}

/// Stages one branch's recovery-root rotation.
///
/// The caller owns the surrounding transaction. Replacing the key drops the
/// prior interval from the next GC root set without ever exposing a
/// checkpoint that lacks durable commit-delta reconstruction authority.
pub(crate) fn stage_recovery_ref_rotation(
    writes: &mut StorageWriteSet,
    recovery: &CheckpointRecoveryRef,
) -> Result<(), LixError> {
    if recovery.branch_id.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "checkpoint recovery branch id must not be empty",
        ));
    }
    let key = recovery_ref_key(&recovery.branch_id)?;
    let value = storage_codec::encode(
        "checkpoint recovery ref",
        &StoredCheckpointRecoveryRef {
            format_version: CHECKPOINT_RECOVERY_REF_FORMAT_VERSION,
            branch_id: recovery.branch_id.clone(),
            recovered_head_commit_id: recovery.recovered_head_commit_id,
            checkpoint_commit_id: recovery.checkpoint_commit_id,
            interval_has_commits: recovery.interval_has_commits,
        },
    )?;
    writes.put(
        CHECKPOINT_RECOVERY_REF_SPACE,
        StorageKey(Bytes::from(key)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

pub(crate) fn stage_checkpoint_gc_state(
    writes: &mut StorageWriteSet,
    state: &CheckpointGcState,
) -> Result<(), LixError> {
    validate_checkpoint_gc_state(*state)?;
    let value = storage_codec::encode(
        "checkpoint GC state",
        &StoredCheckpointGcState {
            format_version: CHECKPOINT_GC_STATE_FORMAT_VERSION,
            checkpoint_sequence: state.checkpoint_sequence,
            last_gc_sequence: state.last_gc_sequence,
            collectible_interval_count: state.collectible_interval_count,
        },
    )?;
    writes.put(
        CHECKPOINT_GC_STATE_SPACE,
        StorageKey(Bytes::from_static(CHECKPOINT_GC_STATE_KEY)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

/// Seeds the empty authenticated frontier during repository initialization.
/// There is deliberately no reader-side migration: a repository without this
/// control is an old protocol and ordinary GC fails closed.
pub(crate) fn stage_reachability_queue_seed(writes: &mut StorageWriteSet) -> Result<(), LixError> {
    let value = storage_codec::encode(
        "GC reachability queue",
        &StoredReachabilityQueue {
            format_version: GC_REACHABILITY_FORMAT_VERSION,
            head_sequence: 0,
            tail_sequence: 0,
            next_sequence: 1,
        },
    )?;
    writes.put(
        GC_REACHABILITY_QUEUE_SPACE,
        StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

#[cfg(test)]
pub(crate) async fn ensure_reachability_queue_for_test(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
) -> Result<bool, LixError> {
    let result = PointReadPlan::new(
        GC_REACHABILITY_QUEUE_SPACE,
        &[StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY))],
    )
    .materialize(read, StorageGetOptions::default())
    .await?;
    if result.value.into_iter().next().flatten().is_none() {
        stage_reachability_queue_seed(writes)?;
        return Ok(true);
    }
    Ok(false)
}

fn reachability_sequence_key(sequence: u64) -> StorageKey {
    StorageKey(Bytes::copy_from_slice(&sequence.to_be_bytes()))
}

async fn load_reachability_queue(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<(StoredReachabilityQueue, Bytes), LixError> {
    let result = PointReadPlan::new(
        GC_REACHABILITY_QUEUE_SPACE,
        &[StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    let Some(Some(StorageProjectedValue::FullValue(bytes))) = result.value.into_iter().next()
    else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "authenticated GC reachability queue is missing",
        ));
    };
    let queue: StoredReachabilityQueue = storage_codec::decode("GC reachability queue", &bytes)?;
    if queue.format_version != GC_REACHABILITY_FORMAT_VERSION
        || queue.next_sequence == 0
        || (queue.head_sequence == 0) != (queue.tail_sequence == 0)
        || (queue.head_sequence != 0 && queue.head_sequence > queue.tail_sequence)
        || queue.tail_sequence >= queue.next_sequence
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "authenticated GC reachability queue has invalid bounds",
        ));
    }
    Ok((queue, bytes))
}

fn root_control_digest(raw: Option<&Bytes>) -> [u8; 32] {
    match raw {
        Some(raw) => *blake3::hash(raw.as_ref()).as_bytes(),
        None => *blake3::hash(b"lix.gc.root.absent.v1").as_bytes(),
    }
}

fn validate_stored_root_reachability_delta(
    delta: &StoredRootReachabilityDelta,
) -> Result<(), LixError> {
    if delta.branch_id.is_empty()
        || delta.old_control_digest == [0; 32]
        || delta.new_control_digest == [0; 32]
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "GC reachability delta has an invalid branch or control digest",
        ));
    }
    if root_control_digest_for_control(delta.old_control.as_ref())? != delta.old_control_digest
        || root_control_digest_for_control(delta.new_control.as_ref())? != delta.new_control_digest
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "GC reachability delta for branch '{}' has a control digest mismatch",
                delta.branch_id
            ),
        ));
    }
    match (delta.old_root, delta.old_control) {
        (Some(root), Some(control)) if control.head_commit_id == root => {}
        (None, None) => {}
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "GC reachability delta for branch '{}' has an invalid old-root binding",
                    delta.branch_id
                ),
            ));
        }
    }
    match (delta.new_root, delta.new_control) {
        (Some(root), Some(control)) if control.head_commit_id == root => {}
        (None, None) => {}
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "GC reachability delta for branch '{}' has an invalid new-root binding",
                    delta.branch_id
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn root_control_digest_for_control(
    control: Option<&crate::branch::BranchHeadControl>,
) -> Result<[u8; 32], LixError> {
    let Some(control) = control else {
        return Ok(root_control_digest(None));
    };
    let bytes = storage_codec::encode("branch-head control", control)?;
    Ok(root_control_digest(Some(&Bytes::from(bytes))))
}

fn encode_reachability_batch_body(
    sequence: u64,
    deltas: Vec<StoredRootReachabilityDelta>,
    checkpoint_roots: Vec<CommitId>,
) -> Result<(StoredRootReachabilityBatchBody, [u8; 32]), LixError> {
    let body = StoredRootReachabilityBatchBody {
        format_version: GC_REACHABILITY_FORMAT_VERSION,
        sequence,
        deltas,
        checkpoint_roots,
    };
    let encoded = storage_codec::encode("GC reachability delta body", &body)?;
    Ok((body, *blake3::hash(&encoded).as_bytes()))
}

/// Appends one transaction's complete root transition set.  A single queue
/// CAS makes branch advance/delete and checkpoint pin publication atomic with
/// their controls; consumers never infer liveness from semantic parent rows.
pub(crate) async fn stage_reachability_delta_batch(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    deltas: &[RootReachabilityDelta],
    checkpoint_roots: &[CommitId],
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    if deltas.is_empty() && checkpoint_roots.is_empty() {
        return Ok(());
    }
    let (mut queue, raw_queue) = load_reachability_queue(read).await?;
    let sequence = queue.next_sequence;
    queue.next_sequence = queue.next_sequence.checked_add(1).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "GC reachability queue sequence overflow",
        )
    })?;
    if queue.head_sequence == 0 {
        queue.head_sequence = sequence;
    }
    queue.tail_sequence = sequence;
    let stored_deltas = deltas
        .iter()
        .map(|delta| StoredRootReachabilityDelta {
            branch_id: delta.branch_id.clone(),
            old_root: delta.old_root,
            new_root: delta.new_root,
            old_control: delta.old_control,
            new_control: delta.new_control,
            old_control_digest: delta.old_control_digest,
            new_control_digest: delta.new_control_digest,
        })
        .collect::<Vec<_>>();
    let checkpoint_roots = checkpoint_roots.to_vec();
    let (body, digest) = encode_reachability_batch_body(sequence, stored_deltas, checkpoint_roots)?;
    let batch = StoredRootReachabilityBatch {
        format_version: GC_REACHABILITY_FORMAT_VERSION,
        sequence,
        deltas: body.deltas,
        checkpoint_roots: body.checkpoint_roots,
        digest,
    };
    writes.put(
        GC_REACHABILITY_DELTA_SPACE,
        reachability_sequence_key(sequence),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("GC reachability delta", &batch)?),
        },
    );
    writes.put(
        GC_REACHABILITY_QUEUE_SPACE,
        StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("GC reachability queue", &queue)?),
        },
    );
    preconditions.push(StoragePrecondition::KeyValueEquals {
        space: GC_REACHABILITY_QUEUE_SPACE,
        key: StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        expected: raw_queue,
    });
    Ok(())
}

async fn load_reachability_batches(
    store: &(impl StorageAdapterRead + ?Sized),
    queue: &StoredReachabilityQueue,
) -> Result<Vec<(u64, StoredRootReachabilityBatch)>, LixError> {
    if queue.head_sequence == 0 {
        return Ok(Vec::new());
    }
    let end = queue
        .head_sequence
        .saturating_add(GC_REACHABILITY_BATCH_LIMIT as u64)
        .min(queue.tail_sequence.saturating_add(1));
    let keys = (queue.head_sequence..end)
        .map(reachability_sequence_key)
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(GC_REACHABILITY_DELTA_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut batches = Vec::with_capacity(keys.len());
    for (sequence, value) in (queue.head_sequence..end).zip(result.value) {
        let Some(StorageProjectedValue::FullValue(bytes)) = value else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("GC reachability delta {sequence} is missing"),
            ));
        };
        let batch: StoredRootReachabilityBatch =
            storage_codec::decode("GC reachability delta", &bytes)?;
        if batch.format_version != GC_REACHABILITY_FORMAT_VERSION || batch.sequence != sequence {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("GC reachability delta {sequence} has invalid identity"),
            ));
        }
        let (body, digest) = encode_reachability_batch_body(
            batch.sequence,
            batch.deltas.clone(),
            batch.checkpoint_roots.clone(),
        )?;
        let _ = body;
        if digest != batch.digest {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("GC reachability delta {sequence} digest mismatch"),
            ));
        }
        batches.push((sequence, batch));
    }
    Ok(batches)
}

/// Returns the exact standalone semantic facts and the authenticated reason
/// currently known for each one. This is benchmark-only attribution: the
/// ordinary collector never scans CHANGE_SPACE, and this helper is called
/// outside the measured planner phase.
#[cfg(feature = "storage-benches")]
pub(crate) async fn audit_repository_gc_standalone_refs<S>(
    store: &S,
) -> Result<Vec<String>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let controls = BranchHeadControlContext::new().reader(store).scan().await?;
    let active_refs = controls
        .iter()
        .map(|(_, control)| control.ref_change_id)
        .collect::<BTreeSet<_>>();
    let (queue, _) = load_reachability_queue(store).await?;
    let batches = load_reachability_batches(store, &queue).await?;
    let mut active_root_ids = controls
        .iter()
        .flat_map(|(_, control)| {
            std::iter::once(control.head_commit_id).chain(control.working_diff_checkpoint_commit_id)
        })
        .collect::<BTreeSet<_>>();
    active_root_ids.extend(
        load_recovery_refs(store)
            .await?
            .into_iter()
            .map(|recovery| recovery.recovered_head_commit_id),
    );
    active_root_ids.extend(
        batches
            .iter()
            .flat_map(|(_, batch)| batch.checkpoint_roots.iter().copied()),
    );
    let mut active_dependency_ids = BTreeSet::new();
    let mut pending = active_root_ids.iter().copied().collect::<Vec<_>>();
    let mut replay_debt_ids = Vec::new();
    let mut seen_manifests = BTreeSet::new();
    while let Some(commit_id) = pending.pop() {
        if !seen_manifests.insert(commit_id) {
            continue;
        }
        let Some(manifest) =
            crate::tracked_state::load_commit_state_manifest(store, commit_id).await?
        else {
            continue;
        };
        if let Some(source) = manifest.mutations.selected_source_commit_id() {
            active_dependency_ids.insert(source);
            pending.push(source);
        }
        if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
            if let Some(base) = root.serving_base_commit_id {
                active_dependency_ids.insert(base);
            }
        }
        if let Some(snapshot_root) = manifest.snapshot_root.as_ref() {
            active_dependency_ids.extend(
                snapshot_root
                    .parent_roots
                    .iter()
                    .map(|parent| parent.commit_id),
            );
        }
        if manifest.replay_debt.depth != 0 {
            replay_debt_ids.push(commit_id);
        }
    }
    if !replay_debt_ids.is_empty() {
        let replay_nodes = CommitGraphContext::new()
            .reader(store)
            .load_nodes(&replay_debt_ids)
            .await?;
        for (_, node) in replay_nodes {
            if let Some(node) = node {
                active_dependency_ids.extend(node.parent_commit_ids);
            }
        }
    }
    let mut retired_refs = BTreeMap::new();
    for (_, batch) in batches {
        for delta in batch.deltas {
            if let Some(control) = delta.old_control {
                retired_refs.insert(control.ref_change_id, delta.old_root);
            }
        }
    }
    let mut reader = ChangelogContext::new().reader(store);
    let mut entries = Vec::new();
    let mut start_after = None::<String>;
    loop {
        let batch = reader
            .scan_changes(ChangeScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1_024),
            })
            .await?;
        for change in batch.entries {
            let reason = if active_refs.contains(&change.change_id) {
                "active_branch_ref"
            } else if let Some(old_root) = retired_refs.get(&change.change_id) {
                if let Some(root) = old_root {
                    if active_root_ids.contains(root) {
                        "retired_delta_old_control:active_root_pin"
                    } else if active_dependency_ids.contains(root) {
                        "retired_delta_old_control:history_dependency_pin"
                    } else {
                        "retired_delta_old_control:reclaimable"
                    }
                } else {
                    "retired_delta_old_control:reclaimable"
                }
            } else {
                "unclassified_no_frontier_delta"
            };
            if let Some(old_root) = retired_refs.get(&change.change_id) {
                entries.push(format!(
                    "{}:{reason}:old_root={}",
                    change.change_id,
                    old_root.map_or_else(|| "none".to_owned(), |root| root.to_string())
                ));
            } else if reason == "unclassified_no_frontier_delta" {
                entries.push(format!(
                    "{}:{reason}:schema={}:account={}:origin={}",
                    change.change_id,
                    change.schema_key,
                    change.account_id,
                    change.origin_key.as_deref().unwrap_or("none")
                ));
            } else {
                entries.push(format!("{}:{reason}", change.change_id));
            }
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    Ok(entries)
}

pub(crate) async fn load_recovery_refs(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<CheckpointRecoveryRef>, LixError> {
    let plan = ScanPlan::prefix(
        CHECKPOINT_RECOVERY_REF_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut refs = BTreeMap::new();
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
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint recovery scan unexpectedly omitted its value",
                ));
            };
            let stored: StoredCheckpointRecoveryRef =
                storage_codec::decode("checkpoint recovery ref", &bytes)?;
            if stored.format_version != CHECKPOINT_RECOVERY_REF_FORMAT_VERSION {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint recovery ref for branch '{}' has unsupported format version {}",
                        stored.branch_id, stored.format_version
                    ),
                ));
            }
            let expected_key = recovery_ref_key(&stored.branch_id)?;
            if entry.key.0.as_ref() != expected_key.as_slice() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint recovery ref key does not match branch '{}'",
                        stored.branch_id
                    ),
                ));
            }
            refs.insert(
                stored.branch_id.clone(),
                CheckpointRecoveryRef {
                    branch_id: stored.branch_id,
                    recovered_head_commit_id: stored.recovered_head_commit_id,
                    checkpoint_commit_id: stored.checkpoint_commit_id,
                    interval_has_commits: stored.interval_has_commits,
                },
            );
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(refs.into_values().collect())
}

#[cfg(test)]
async fn stage_sweep_unreachable_content_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    space: StorageSpace,
    live: &BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let node_id = <[u8; 32]>::try_from(entry.key.0.as_ref()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "content-addressed space '{}' contains a malformed key",
                        space.name
                    ),
                )
            })?;
            if !live.contains(&node_id) {
                writes.delete(space, entry.key);
            }
        }
        if !page.value.has_more {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn load_recovery_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<CheckpointRecoveryRef>, LixError> {
    let key = recovery_ref_key(branch_id)?;
    let result = PointReadPlan::new(
        CHECKPOINT_RECOVERY_REF_SPACE,
        &[StorageKey(Bytes::from(key))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    let Some(StorageProjectedValue::FullValue(bytes)) = result.value.into_iter().next().flatten()
    else {
        return Ok(None);
    };
    let stored: StoredCheckpointRecoveryRef =
        storage_codec::decode("checkpoint recovery ref", &bytes)?;
    validate_stored_recovery_ref(&stored, branch_id)?;
    Ok(Some(CheckpointRecoveryRef {
        branch_id: stored.branch_id,
        recovered_head_commit_id: stored.recovered_head_commit_id,
        checkpoint_commit_id: stored.checkpoint_commit_id,
        interval_has_commits: stored.interval_has_commits,
    }))
}

pub(crate) async fn load_checkpoint_gc_state(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<CheckpointGcState, LixError> {
    let result = PointReadPlan::new(
        CHECKPOINT_GC_STATE_SPACE,
        &[StorageKey(Bytes::from_static(CHECKPOINT_GC_STATE_KEY))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    let Some(StorageProjectedValue::FullValue(bytes)) = result.value.into_iter().next().flatten()
    else {
        return Ok(CheckpointGcState::default());
    };
    let stored: StoredCheckpointGcState = storage_codec::decode("checkpoint GC state", &bytes)?;
    if stored.format_version != CHECKPOINT_GC_STATE_FORMAT_VERSION {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "checkpoint GC state has unsupported format version {}",
                stored.format_version
            ),
        ));
    }
    let state = CheckpointGcState {
        checkpoint_sequence: stored.checkpoint_sequence,
        last_gc_sequence: stored.last_gc_sequence,
        collectible_interval_count: stored.collectible_interval_count,
    };
    validate_checkpoint_gc_state(state)?;
    Ok(state)
}

fn validate_checkpoint_gc_state(state: CheckpointGcState) -> Result<(), LixError> {
    let checkpoint_age = state
        .checkpoint_sequence
        .checked_sub(state.last_gc_sequence);
    if state.checkpoint_sequence == 0
        || checkpoint_age.is_none()
        || (checkpoint_age == Some(0) && state.has_collectible_debt())
        || checkpoint_age.is_some_and(|age| state.collectible_interval_count > age)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "checkpoint GC state has inconsistent sequence or debt counters",
        ));
    }
    Ok(())
}

fn recovery_ref_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "checkpoint recovery ref key",
        &CheckpointRecoveryRefKey { branch_id },
    )
}

fn validate_stored_recovery_ref(
    stored: &StoredCheckpointRecoveryRef,
    expected_branch_id: &str,
) -> Result<(), LixError> {
    if stored.format_version != CHECKPOINT_RECOVERY_REF_FORMAT_VERSION {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "checkpoint recovery ref for branch '{}' has unsupported format version {}",
                stored.branch_id, stored.format_version
            ),
        ));
    }
    if stored.branch_id != expected_branch_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "checkpoint recovery ref key does not match branch '{}'",
                stored.branch_id
            ),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RepositoryGcSweep {
    pub(crate) tracked_commit_roots: Vec<CommitId>,
    pub(crate) standalone_changes: Vec<ChangeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RepositoryGcPlan {
    pub(crate) changelog: GcPlan,
    pub(crate) sweep: RepositoryGcSweep,
    pub(crate) profile: RepositoryGcProfile,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RepositoryGcProfile {
    pub(crate) root_discovery_us: u64,
    pub(crate) changelog_us: u64,
    pub(crate) tracked_root_stage_us: u64,
    pub(crate) total_us: u64,
}

/// Plans and stages logical repository GC against one pinned read.
///
/// The caller must serialize this operation with repository writes and commit
/// `writes` atomically. Planning and mutation are deliberately separated from
/// storage commit so checkpoint/session code can retain lifecycle control.
/// Content-addressed tree/CAS orphan repair is intentionally an offline path;
/// out-of-band JSON is reclaimed here only from explicit ownership-loss
/// candidates.
/// Ordinary GC consumes only authenticated publication deltas.  Branch-head
/// controls and checkpoint recovery refs are the complete active-root set;
/// no semantic parent walk or full-space inventory discovery is permitted on
/// this path.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) async fn stage_repository_gc<S>(
    store: S,
    writes: &mut StorageWriteSet,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let mut preconditions = Vec::new();
    stage_repository_gc_with_preconditions(store, writes, &mut preconditions).await
}

pub(crate) async fn stage_repository_gc_with_preconditions<S>(
    store: S,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let started = Instant::now();
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let recovery_refs = load_recovery_refs(&store).await?;
    let mut active_roots = controls
        .iter()
        .flat_map(|(_, control)| {
            std::iter::once(control.head_commit_id).chain(control.working_diff_checkpoint_commit_id)
        })
        .collect::<BTreeSet<_>>();
    active_roots.extend(recovery_refs.iter().flat_map(|recovery| {
        [
            recovery.recovered_head_commit_id,
            recovery.checkpoint_commit_id,
        ]
    }));
    if active_roots.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "authenticated GC active-root set is empty",
        ));
    }

    let (queue, raw_queue) = load_reachability_queue(&store).await?;
    let batches = load_reachability_batches(&store, &queue).await?;
    // Checkpoint pins carried by the authenticated batches are folded into
    // the same active-root set before any retirement candidate is evaluated.
    // Recovery refs normally provide the same roots; retaining both proofs
    // makes a torn ref rotation fail closed instead of silently reclaiming a
    // checkpoint-only authority.
    active_roots.extend(
        batches
            .iter()
            .flat_map(|(_, batch)| batch.checkpoint_roots.iter().copied()),
    );
    if batches.is_empty() {
        return Ok(RepositoryGcPlan {
            changelog: GcPlan {
                roots: active_roots
                    .iter()
                    .copied()
                    .map(GcRoot::BranchHead)
                    .collect(),
                live: GcLiveSet {
                    commits: active_roots.iter().copied().collect(),
                    changes: Vec::new(),
                    payloads: Vec::new(),
                },
                sweep: GcSweepSet {
                    commits: Vec::new(),
                    commit_change_ids: Vec::new(),
                    changes: Vec::new(),
                    json_payloads: Vec::new(),
                },
                repair: GcRepairSet::default(),
            },
            sweep: RepositoryGcSweep {
                tracked_commit_roots: Vec::new(),
                standalone_changes: Vec::new(),
            },
            profile: RepositoryGcProfile {
                root_discovery_us: elapsed_micros(started),
                changelog_us: 0,
                tracked_root_stage_us: 0,
                total_us: elapsed_micros(started),
            },
        });
    }
    let mut active_authority_ids = active_roots.clone();
    let mut active_dependency_ids = BTreeSet::new();
    let mut active_semantic_dependency_ids = BTreeSet::new();
    let mut active_manifests = BTreeMap::new();
    let mut pending = active_roots.iter().copied().collect::<Vec<_>>();
    while let Some(commit_id) = pending.pop() {
        if active_manifests.contains_key(&commit_id) {
            continue;
        }
        let manifest = crate::tracked_state::load_commit_state_manifest(&store, commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("active GC root '{commit_id}' has no authenticated physical manifest"),
                )
            })?;
        if let Some(source) = manifest.mutations.selected_source_commit_id() {
            active_authority_ids.insert(source);
            pending.push(source);
        }
        if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
            if let Some(base) = root.serving_base_commit_id {
                active_dependency_ids.insert(base);
            }
        }
        if let Some(snapshot_root) = manifest.snapshot_root.as_ref() {
            active_dependency_ids.extend(
                snapshot_root
                    .parent_roots
                    .iter()
                    .map(|parent| parent.commit_id),
            );
        }
        active_manifests.insert(commit_id, manifest);
    }

    let mut active_mutation_nodes = BTreeSet::new();
    let mut active_scoped_nodes = BTreeSet::new();
    let mut active_current_parts = BTreeSet::new();
    let replay_debt_ids = active_manifests
        .iter()
        .filter_map(|(commit_id, manifest)| (manifest.replay_debt.depth != 0).then_some(*commit_id))
        .collect::<Vec<_>>();
    // Rootless active commits retain their semantic parents for bounded replay,
    // but unrelated retired roots must not be blocked by a repository-global
    // replay-debt flag. Resolve only the direct parent closure of the active
    // replay-debt roots; this is bounded by active roots and preserves the
    // history dependency proof without a changelog sweep.
    if !replay_debt_ids.is_empty() {
        let replay_nodes = CommitGraphContext::new()
            .reader(store.clone())
            .load_nodes(&replay_debt_ids)
            .await?;
        for (_, node) in replay_nodes.iter() {
            if let Some(node) = node {
                active_dependency_ids.extend(node.parent_commit_ids.iter().copied());
                active_semantic_dependency_ids.extend(node.parent_commit_ids.iter().copied());
            }
        }
    }
    let scoped_roots = active_manifests
        .values()
        .filter_map(|manifest| {
            manifest
                .current_state_scoped_ranges
                .as_ref()
                .map(|root| root.tree.clone())
        })
        .collect::<Vec<_>>();
    if !scoped_roots.is_empty() {
        let reachable =
            crate::tracked_state::validate_scoped_range_trees(&store, &scoped_roots).await?;
        active_scoped_nodes.extend(reachable.node_ids);
        for part in reachable.parts {
            let descriptor =
                crate::tracked_state::current_state_descriptor_from_scoped_range_part(&part)?;
            if descriptor.source_kind == 1 {
                active_current_parts.insert(descriptor.content_digest);
            }
        }
    }
    validate_live_native_parts(&store, &active_current_parts).await?;
    let active_ids = active_manifests.keys().copied().collect::<Vec<_>>();
    let mutation_roots =
        crate::tracked_state::load_commit_mutation_directory_roots(&store, &active_ids).await?;
    for root in mutation_roots.into_iter().flatten() {
        active_mutation_nodes.extend(
            crate::tracked_state::collect_mutation_directory_node_ids(&store, &root).await?,
        );
    }

    // A delta is a candidate until every active history/checkpoint/replay
    // dependency releases the old root.  Never consume a batch containing a
    // blocked candidate: dropping that signal would make the root permanently
    // unreclaimable after the pin is later released.  The whole batch remains
    // at the queue head; this intentionally delays later deltas but preserves
    // the authenticated publication order and retry semantics.
    let mut blocked_sequences = BTreeSet::new();
    for (sequence, batch) in &batches {
        for delta in &batch.deltas {
            validate_stored_root_reachability_delta(delta)?;
            let Some(old_root) = delta.old_root else {
                continue;
            };
            if !retirement_is_proven(
                old_root,
                delta.new_root,
                &active_authority_ids,
                &active_dependency_ids,
            ) || replay_debt_ids.contains(&old_root)
            {
                blocked_sequences.insert(*sequence);
            }
        }
    }

    let mut next_queue = queue;
    let queue_head = next_queue.head_sequence;
    let mut consumed_through = queue_head;
    let mut queue_open = true;
    let mut reclaimed_commits = Vec::new();
    let mut reclaimed_standalone_changes = BTreeSet::new();
    let mut reclaimed_checkpoint_branches = BTreeSet::new();
    for (sequence, batch) in batches {
        if queue_open && blocked_sequences.contains(&sequence) {
            queue_open = false;
        }
        if queue_open {
            consumed_through = sequence.saturating_add(1);
        }
        for delta in batch.deltas {
            validate_stored_root_reachability_delta(&delta)?;
            // Branch checkpoint rows are derived serving state owned by the
            // branch lifecycle, not by a tracked root.  Process the
            // authenticated deletion signal before inspecting `old_root` so
            // a branch deleted before its first rooted publication cannot
            // strand its checkpoint prefix forever.  A recreated branch has
            // a live control and therefore keeps the shared branch-id range.
            if delta.new_control.is_none()
                && !controls
                    .iter()
                    .any(|(active_branch_id, _)| active_branch_id == &delta.branch_id)
                && reclaimed_checkpoint_branches.insert(delta.branch_id.clone())
            {
                crate::transaction::stage_delete_branch_plugin_checkpoints(
                    &store,
                    writes,
                    &delta.branch_id,
                )
                .await?;
            }
            let Some(old_root) = delta.old_root else {
                continue;
            };
            let physical_retirement = retirement_is_proven(
                old_root,
                delta.new_root,
                &active_authority_ids,
                &active_dependency_ids,
            ) && !replay_debt_ids.contains(&old_root);
            let semantic_retirement = retirement_is_proven(
                old_root,
                delta.new_root,
                &active_roots,
                &active_semantic_dependency_ids,
            );
            if semantic_retirement {
                stage_delete_semantic_commit_projection(&store, writes, old_root).await?;
            }
            if !physical_retirement {
                continue;
            }
            let manifest = crate::tracked_state::load_commit_state_manifest(&store, old_root)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("retired GC root '{old_root}' has no authenticated manifest"),
                    )
                })?;
            let retired_root =
                crate::tracked_state::load_commit_mutation_directory_roots(&store, &[old_root])
                    .await?
                    .into_iter()
                    .next()
                    .flatten();
            if let Some(root) = retired_root {
                let nodes =
                    crate::tracked_state::collect_mutation_directory_node_ids(&store, &root)
                        .await?;
                for node_id in nodes.difference(&active_mutation_nodes) {
                    writes.delete(
                        crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
                        StorageKey(Bytes::copy_from_slice(node_id)),
                    );
                }
            }
            if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
                let reachable = crate::tracked_state::validate_scoped_range_trees(
                    &store,
                    std::slice::from_ref(&root.tree),
                )
                .await?;
                for node_id in reachable.node_ids.difference(&active_scoped_nodes) {
                    writes.delete(
                        crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
                        StorageKey(Bytes::copy_from_slice(node_id)),
                    );
                }
                for part in reachable.parts {
                    let descriptor =
                        crate::tracked_state::current_state_descriptor_from_scoped_range_part(
                            &part,
                        )?;
                    if descriptor.source_kind == 1
                        && !active_current_parts.contains(&descriptor.content_digest)
                    {
                        writes.delete(
                            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
                            StorageKey(Bytes::copy_from_slice(&descriptor.content_digest)),
                        );
                        writes.delete(
                            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
                            StorageKey(Bytes::copy_from_slice(&descriptor.payload_refs_digest)),
                        );
                    }
                }
            }
            crate::tracked_state::stage_delete_commit_state_manifest_for_gc(
                &store, writes, old_root, &manifest,
            )
            .await?;
            if let Some(control) = delta.old_control.as_ref() {
                if !controls
                    .iter()
                    .any(|(_, active)| active.ref_change_id == control.ref_change_id)
                {
                    let key = StorageKey(Bytes::from(change_key(control.ref_change_id)));
                    let existing = PointReadPlan::new(CHANGE_SPACE, std::slice::from_ref(&key))
                        .materialize(&store, StorageGetOptions::default())
                        .await?
                        .value
                        .into_iter()
                        .next()
                        .flatten();
                    if existing.is_none() {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "retired branch control references missing standalone change '{}'",
                                control.ref_change_id
                            ),
                        ));
                    }
                    writes.delete(CHANGE_SPACE, key);
                    reclaimed_standalone_changes.insert(control.ref_change_id);
                }
            }
            reclaimed_commits.push(old_root);
        }
    }

    if consumed_through > queue_head {
        consumed_through = consumed_through
            .min(queue_head.saturating_add(GC_REACHABILITY_BATCH_LIMIT as u64))
            .min(next_queue.tail_sequence.saturating_add(1));
        for sequence in next_queue.head_sequence..consumed_through {
            writes.delete(
                GC_REACHABILITY_DELTA_SPACE,
                reachability_sequence_key(sequence),
            );
        }
        if consumed_through > next_queue.tail_sequence {
            next_queue.head_sequence = 0;
            next_queue.tail_sequence = 0;
        } else {
            next_queue.head_sequence = consumed_through;
        }
        writes.put(
            GC_REACHABILITY_QUEUE_SPACE,
            StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
            StorageValue {
                bytes: Bytes::from(storage_codec::encode("GC reachability queue", &next_queue)?),
            },
        );
        // The queue CAS is the publication/consumption fence.  The caller
        // carries this exact read token into the backend write options.
        preconditions.push(StoragePrecondition::KeyValueEquals {
            space: GC_REACHABILITY_QUEUE_SPACE,
            key: StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
            expected: raw_queue,
        });
        writes.seal_changelog_gc();
    }

    Ok(RepositoryGcPlan {
        changelog: GcPlan {
            roots: active_roots
                .iter()
                .copied()
                .map(GcRoot::BranchHead)
                .collect(),
            live: GcLiveSet {
                commits: active_authority_ids.into_iter().collect(),
                changes: Vec::new(),
                payloads: Vec::new(),
            },
            sweep: GcSweepSet {
                commits: Vec::new(),
                commit_change_ids: Vec::new(),
                changes: Vec::new(),
                json_payloads: Vec::new(),
            },
            repair: GcRepairSet::default(),
        },
        sweep: RepositoryGcSweep {
            tracked_commit_roots: reclaimed_commits,
            standalone_changes: reclaimed_standalone_changes.into_iter().collect(),
        },
        profile: RepositoryGcProfile {
            root_discovery_us: elapsed_micros(started),
            changelog_us: 0,
            tracked_root_stage_us: 0,
            total_us: elapsed_micros(started),
        },
    })
}

/// Every authenticated active scoped-range descriptor must have its native
/// payload present before ordinary GC is allowed to stage any sweep.  The
/// descriptor is authority for the digest, but not proof that the immutable
/// payload still exists; treating a missing payload as an empty live set would
/// silently turn corruption into deletion.
async fn validate_live_native_parts<S>(
    store: &S,
    digests: &BTreeSet<[u8; 32]>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if digests.is_empty() {
        return Ok(());
    }
    let keys = digests
        .iter()
        .map(|digest| StorageKey(Bytes::copy_from_slice(digest)))
        .collect::<Vec<_>>();
    let presence = PointReadPlan::new(crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE, &keys)
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    if presence
        .value
        .into_iter()
        .any(|value| !matches!(value, Some(StorageProjectedValue::KeyOnly)))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "live current-state directory references a missing native data part",
        ));
    }
    Ok(())
}

/// Recovery-only verifier retained for explicit rebuild tooling and tests.
/// Ordinary maintenance never calls this path: it would rediscover liveness
/// by scanning the changelog and every immutable inventory.
#[cfg(test)]
async fn stage_repository_gc_full_recovery<S>(
    store: S,
    writes: &mut StorageWriteSet,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let total_started = Instant::now();
    let phase_started = Instant::now();
    // Controls are the complete ownership root for the generation-keyed
    // current-state plane. Read them once so untracked payload discovery and
    // derived-generation sweeping use exactly the same pinned publication
    // view.
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let mut roots = TrackedHeadContext::new()
        .reader(store.clone())
        .untracked_json_refs(&controls)
        .await?
        .into_iter()
        .map(GcRoot::CurrentPayload)
        .collect::<Vec<_>>();
    // Branch controls, not their public `lix_branch_ref` projection rows,
    // are the authoritative tracked-history roots.
    for (_branch_id, control) in &controls {
        roots.push(GcRoot::BranchHead(control.head_commit_id));
        // The synthesized public `lix_branch_ref` row exposes this standalone
        // immutable change id. Keep its currently published ledger fact live
        // without making the mutable control a flat live-state row again.
        roots.push(GcRoot::StandaloneChange(control.ref_change_id));
    }
    for recovery in load_recovery_refs(&store).await? {
        roots.push(GcRoot::BranchHead(recovery.recovered_head_commit_id));
    }
    let root_discovery_us = elapsed_micros(phase_started);

    let phase_started = Instant::now();
    let changelog_plan = plan_and_stage_authority_gc(&store, writes, &roots).await?;
    let changelog_us = elapsed_micros(phase_started);
    let swept_snapshot_authorities = changelog_plan.sweep.commits.clone();
    // Removing a dead changelog commit invalidates its commit-addressed delta
    // inventory. Immutable tree/CAS payloads remain content-addressed
    // maintenance work, but delta rows have no shared ownership and must be
    // reclaimed in the same logical GC pass.
    let phase_started = Instant::now();
    // Old serving generations are derived data. Removing them in the same
    // atomic sweep as their untracked payload-root withdrawal prevents stale
    // branch generations from accumulating indefinitely.
    let stale_untracked_refs = TrackedHeadContext::new()
        .stage_collect_stale_current_state_generations(&store, writes, &controls)
        .await?;
    // The changelog plan contains every payload reachable from tracked
    // history plus the active untracked roots supplied above. A retired
    // untracked JSON ref is only a deletion candidate: content-addressed
    // payloads can be shared with another current row or reachable history,
    // so absence from this complete live set is the required proof.
    let live_payloads = changelog_plan
        .live
        .payloads
        .iter()
        .map(|json_ref| *json_ref.as_hash_array())
        .collect::<BTreeSet<_>>();
    // `collect_garbage` already staged deletes for dead changelog payloads.
    // Avoid emitting a second mutation for a content hash shared with a stale
    // untracked generation; `StorageWriteSet` deliberately rejects duplicate
    // final mutations, even when both are deletes.
    let changelog_swept_payloads = changelog_plan
        .sweep
        .json_payloads
        .iter()
        .map(|json_ref| *json_ref.as_hash_array())
        .collect::<BTreeSet<_>>();
    let mut reclaimable_untracked_refs = stale_untracked_refs
        .into_iter()
        .map(|json_ref| *json_ref.as_hash_array())
        .collect::<BTreeSet<_>>();
    let mut consumed_candidate_keys = Vec::new();
    for candidate in JsonStoreContext::new()
        .scan_untracked_reclaim_candidates(&store)
        .await?
    {
        let UntrackedJsonReclaimCandidate { key, json_ref } = candidate;
        let Some(json_ref) = json_ref else {
            // Candidate records are derived hints. A malformed one cannot
            // safely name a JSON payload, but it must not become permanent
            // maintenance debt.
            consumed_candidate_keys.push(key);
            continue;
        };
        if !live_payloads.contains(json_ref.as_hash_array()) {
            reclaimable_untracked_refs.insert(*json_ref.as_hash_array());
            consumed_candidate_keys.push(key);
        }
        // Keep a live candidate. If a shared owner later disappears through a
        // different lifecycle path, the next GC can still prove reclamation
        // without relying on that path to recreate this hint.
    }
    let reclaimable_untracked_refs = reclaimable_untracked_refs
        .into_iter()
        .filter(|hash| !live_payloads.contains(hash) && !changelog_swept_payloads.contains(hash))
        .map(JsonRef::from_hash_bytes)
        .collect::<Vec<_>>();
    let json_writer = JsonStoreContext::new().writer();
    json_writer.stage_delete_refs(writes, reclaimable_untracked_refs);
    JsonStoreWriter::stage_delete_untracked_reclaim_candidates(writes, consumed_candidate_keys);
    // Checkpoint publication leaves prior dirty-index generations unreachable
    // in O(1). Reclaim those auxiliary records only in the asynchronous GC
    // pass so a foreground checkpoint never pays a history-sized delete cost.
    stage_collect_stale_working_diff_indexes(&store, writes).await?;
    let tracked_root_stage_us = elapsed_micros(phase_started);

    Ok(RepositoryGcPlan {
        changelog: changelog_plan,
        sweep: RepositoryGcSweep {
            tracked_commit_roots: swept_snapshot_authorities,
            standalone_changes: Vec::new(),
        },
        profile: RepositoryGcProfile {
            root_discovery_us,
            changelog_us,
            tracked_root_stage_us,
            total_us: elapsed_micros(total_started),
        },
    })
}

#[cfg(test)]
async fn plan_and_stage_authority_gc<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    roots: &[GcRoot],
) -> Result<GcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let commits = scan_all_gc_commits(store.clone()).await?;
    let standalone_changes = scan_all_gc_standalone_changes(store.clone()).await?;
    let packed = crate::tracked_state::scan_commit_delta_inventory(store).await?;

    if let Some(change_id) = standalone_changes.keys().find(|change_id| {
        packed.commits.values().any(|entry| {
            entry
                .members
                .iter()
                .any(|member| member.value.change_id == **change_id)
        })
    }) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "change '{change_id}' is stored as both a standalone fact and a packed commit member"
            ),
        ));
    }

    let mut live_commits = BTreeSet::new();
    let mut pending = roots
        .iter()
        .filter_map(|root| match root {
            GcRoot::BranchHead(commit_id) => Some(*commit_id),
            GcRoot::StandaloneChange(_) | GcRoot::CurrentPayload(_) => None,
        })
        .collect::<Vec<_>>();
    while let Some(commit_id) = pending.pop() {
        if !live_commits.insert(commit_id) {
            continue;
        }
        let commit = commits.get(commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("garbage-collection root references missing commit '{commit_id}'"),
            )
        })?;
        pending.extend(commits.parent_commit_ids(commit).iter().copied());
    }

    // GC is destructive, so every live semantic commit must have an immutable
    // physical manifest before payload reachability or sweep mutations are
    // derived. Missing physical authority must not silently turn a live commit
    // into an empty mutation owner.
    for commit_id in &live_commits {
        packed.commits.get(commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("live commit '{commit_id}' has no commit-state authority"),
            )
        })?;
    }

    // Mutation parts are immutable: direct ChangeIds encode their physical
    // part slot and ordinal. Retain selected-source and authored-owner parts
    // instead of rewriting them into a surviving commit. Logical commit
    // reachability remains independent, so an authority owner may disappear
    // from `lix_commit` while its immutable mutation part remains addressable.
    let referenced_change_ids = live_commits
        .iter()
        .map(|commit_id| {
            packed
                .commits
                .get(commit_id)
                .expect("live commit-state authorities were validated")
        })
        .flat_map(|entry| entry.members.iter().map(|member| member.value.change_id))
        .collect::<BTreeSet<_>>();
    let mut retained_authority_commits = live_commits.clone();
    let mut authority_roots = live_commits
        .iter()
        .map(|commit_id| {
            packed
                .commits
                .get(commit_id)
                .expect("live commit-state authorities were validated")
        })
        .filter_map(|entry| entry.selected_source_commit_id)
        .collect::<Vec<_>>();
    authority_roots.extend(
        packed
            .commits
            .iter()
            .filter(|(commit_id, _)| !live_commits.contains(commit_id))
            .filter_map(|(commit_id, entry)| {
                entry
                    .members
                    .iter()
                    .any(|member| {
                        member.authored && referenced_change_ids.contains(&member.value.change_id)
                    })
                    .then_some(*commit_id)
            }),
    );
    while let Some(commit_id) = authority_roots.pop() {
        if !retained_authority_commits.insert(commit_id) {
            continue;
        }
        let authority = packed.commits.get(&commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("canonical mutation authority '{commit_id}' is missing"),
            )
        })?;
        if let Some(source_commit_id) = authority.selected_source_commit_id {
            authority_roots.push(source_commit_id);
        }
    }
    let mut live_scoped_range_nodes = BTreeSet::<[u8; 32]>::new();
    let mut live_current_state_data_parts = BTreeSet::<[u8; 32]>::new();
    let mut live_current_state_ref_summaries = BTreeMap::<[u8; 32], [u8; 32]>::new();
    let mut live_current_state_payload_hashes = BTreeSet::<[u8; 32]>::new();
    let mut scoped_roots = BTreeMap::new();
    let mut authority_manifests = BTreeMap::new();
    let live_commit_ids = live_commits.iter().copied().collect::<Vec<_>>();
    let loaded_live_manifests =
        crate::tracked_state::load_commit_state_manifests(store, &live_commit_ids).await?;
    for (commit_id, manifest) in live_commit_ids.into_iter().zip(loaded_live_manifests) {
        let manifest = manifest.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("live commit '{commit_id}' has no commit-state authority"),
            )
        })?;
        authority_manifests.insert(commit_id, manifest.clone());
        let Some(root) = manifest.current_state_scoped_ranges.as_ref() else {
            continue;
        };
        if let Some(previous) = scoped_roots.insert(root.tree.root_id, root.tree.clone()) {
            if previous != root.tree {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "live current-state scoped ranges disagree about root '{:?}'",
                        root.tree.root_id
                    ),
                ));
            }
        }
    }
    let serving_base_ids = authority_manifests
        .values()
        .filter_map(|manifest| {
            manifest
                .current_state_scoped_ranges
                .as_ref()
                .and_then(|root| root.serving_base_commit_id)
        })
        .filter(|base_id| !authority_manifests.contains_key(base_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if let Some(base_id) = serving_base_ids
        .iter()
        .find(|base_id| !retained_authority_commits.contains(base_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("current-state serving base '{base_id}' is not retained authority"),
        ));
    }
    let loaded_serving_bases =
        crate::tracked_state::load_commit_state_manifests(store, &serving_base_ids).await?;
    for (base_id, manifest) in serving_base_ids.into_iter().zip(loaded_serving_bases) {
        let manifest = manifest.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("current-state serving base '{base_id}' has no commit-state authority"),
            )
        })?;
        authority_manifests.insert(base_id, manifest);
    }
    for commit_id in &live_commits {
        let manifest = authority_manifests
            .get(commit_id)
            .expect("live commit authority was loaded");
        let serving_base = manifest
            .current_state_scoped_ranges
            .as_ref()
            .and_then(|root| root.serving_base_commit_id)
            .and_then(|base_id| authority_manifests.get(&base_id));
        crate::tracked_state::validate_current_state_scoped_range_serving_base_manifest(
            manifest,
            serving_base,
        )?;
    }

    let scoped_roots = scoped_roots.values().cloned().collect::<Vec<_>>();
    let reachable = crate::tracked_state::validate_scoped_range_trees(store, &scoped_roots).await?;
    live_scoped_range_nodes.extend(reachable.node_ids);
    let mut live_columnar_sources = BTreeSet::<(CommitId, [u8; 16], [u8; 32])>::new();
    for part in reachable.parts {
        let descriptor =
            crate::tracked_state::current_state_descriptor_from_scoped_range_part(&part)?;
        match descriptor.source_kind {
            0 => {
                let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
                if !packed.commits.contains_key(&owner) {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "live scoped range references missing immutable-part owner '{owner}'"
                        ),
                    ));
                }
                retained_authority_commits.insert(owner);
            }
            1 => {
                live_current_state_data_parts.insert(descriptor.content_digest);
                if let Some(previous) = live_current_state_ref_summaries
                    .insert(descriptor.content_digest, descriptor.payload_refs_digest)
                    && previous != descriptor.payload_refs_digest
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "native scoped-range descriptors disagree about payload refs",
                    ));
                }
            }
            2 => {
                let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
                if !packed.commits.contains_key(&owner) {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("live scoped range references missing columnar owner '{owner}'"),
                    ));
                }
                live_columnar_sources.insert((
                    owner,
                    descriptor.source_id,
                    descriptor.content_digest,
                ));
                retained_authority_commits.insert(owner);
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "live scoped range contains an unknown part source",
                ));
            }
        }
    }
    for (owner, source_id, content_digest) in live_columnar_sources {
        let authority = match authority_manifests.get(&owner) {
            Some(authority) => authority.clone(),
            None => crate::tracked_state::load_commit_state_manifest(store, owner)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("live scoped range columnar owner '{owner}' has no authority"),
                    )
                })?,
        };
        let Some(parts) = authority.mutations.columnar_parts.as_ref() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "live columnar scoped range owner has no columnar mutation authority",
            ));
        };
        if parts.owner_commit_id != *owner.as_uuid().as_bytes()
            || parts.row_group_set_id != source_id
            || parts.manifest_digest != content_digest
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "live columnar scoped range descriptor disagrees with owner authority",
            ));
        }
    }

    let native_keys = live_current_state_ref_summaries
        .keys()
        .map(|digest| StorageKey(Bytes::copy_from_slice(digest)))
        .collect::<Vec<_>>();
    let native_refs = PointReadPlan::new(
        crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        &native_keys,
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    for ((_, refs_digest), value) in live_current_state_ref_summaries
        .iter()
        .zip(native_refs.value)
    {
        let bytes = match value {
            Some(StorageProjectedValue::FullValue(bytes)) => bytes,
            Some(StorageProjectedValue::KeyOnly) | None => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "live current-state directory references a missing payload-ref summary",
                ));
            }
        };
        live_current_state_payload_hashes.extend(
            crate::tracked_state::decode_current_state_data_part_refs(refs_digest, &bytes)?,
        );
    }
    let native_part_presence = PointReadPlan::new(
        crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
        &native_keys,
    )
    .materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::KeyOnly,
        },
    )
    .await?;
    if native_part_presence
        .value
        .into_iter()
        .any(|value| !matches!(value, Some(StorageProjectedValue::KeyOnly)))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "live current-state directory references a missing native data part",
        ));
    }
    if let Some((commit_id, source_commit_id)) = live_commits.iter().find_map(|commit_id| {
        packed
            .commits
            .get(commit_id)
            .and_then(|entry| entry.selected_source_commit_id)
            .filter(|source_commit_id| !packed.commits.contains_key(source_commit_id))
            .map(|source_commit_id| (*commit_id, source_commit_id))
    }) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "live commit '{commit_id}' references missing mutation authority '{source_commit_id}'"
            ),
        ));
    }
    let retained_authority_ids = retained_authority_commits
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let retained_mutation_roots =
        crate::tracked_state::load_commit_mutation_directory_roots(store, &retained_authority_ids)
            .await?;
    let mut unique_mutation_roots = BTreeMap::new();
    for (commit_id, root) in retained_authority_ids
        .into_iter()
        .zip(retained_mutation_roots)
    {
        if !packed.commits.contains_key(&commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("retained mutation authority '{commit_id}' is missing"),
            ));
        }
        let Some(root) = root else {
            continue;
        };
        if let Some(previous) = unique_mutation_roots.insert(root.root_id, root.clone())
            && previous != root
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "retained mutation authorities disagree about a shared directory root",
            ));
        }
    }
    let mut live_mutation_directory_nodes = BTreeSet::new();
    for root in unique_mutation_roots.values() {
        live_mutation_directory_nodes
            .extend(crate::tracked_state::collect_mutation_directory_node_ids(store, root).await?);
    }
    let sweep_authority_commits = packed
        .commits
        .keys()
        .filter(|commit_id| !retained_authority_commits.contains(commit_id))
        .copied()
        .collect::<Vec<_>>();

    let standalone_root_ids = roots
        .iter()
        .filter_map(|root| match root {
            GcRoot::StandaloneChange(change_id) => Some(*change_id),
            GcRoot::BranchHead(_) | GcRoot::CurrentPayload(_) => None,
        })
        .collect::<BTreeSet<_>>();
    if let Some(change_id) = standalone_root_ids
        .iter()
        .find(|change_id| !standalone_changes.contains_key(change_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("garbage-collection root references missing standalone change '{change_id}'"),
        ));
    }

    let mut live_change_ids = standalone_root_ids.clone();
    let mut live_payload_hashes = roots
        .iter()
        .filter_map(|root| match root {
            GcRoot::CurrentPayload(json_ref) => Some(*json_ref.as_hash_array()),
            GcRoot::BranchHead(_) | GcRoot::StandaloneChange(_) => None,
        })
        .collect::<BTreeSet<_>>();
    live_payload_hashes.extend(live_current_state_payload_hashes);
    for change_id in &standalone_root_ids {
        collect_change_payload_hashes(
            standalone_changes
                .get(change_id)
                .expect("standalone GC root existence validated"),
            &mut live_payload_hashes,
        );
    }
    for commit_id in &retained_authority_commits {
        let entry = packed
            .commits
            .get(commit_id)
            .expect("retained mutation authorities were validated");
        for member in &entry.members {
            live_change_ids.insert(member.value.change_id);
            collect_change_payload_hashes(&member.change, &mut live_payload_hashes);
        }
    }

    let (sweep_commits, sweep_commit_change_ids): (Vec<CommitId>, Vec<ChangeId>) = commits
        .iter()
        .filter(|commit| !live_commits.contains(&commit.commit_id))
        .map(|commit| (commit.commit_id, commit.change_id))
        .unzip();
    let sweep_changes = standalone_changes
        .keys()
        .filter(|change_id| !standalone_root_ids.contains(change_id))
        .copied()
        .collect::<Vec<_>>();

    let mut dead_payload_hashes = BTreeSet::new();
    for commit_id in &sweep_authority_commits {
        if let Some(entry) = packed.commits.get(commit_id) {
            for member in &entry.members {
                collect_change_payload_hashes(&member.change, &mut dead_payload_hashes);
            }
        }
    }
    for change_id in &sweep_changes {
        collect_change_payload_hashes(
            standalone_changes
                .get(change_id)
                .expect("sweep change came from standalone inventory"),
            &mut dead_payload_hashes,
        );
    }
    let sweep_json_payloads = dead_payload_hashes
        .difference(&live_payload_hashes)
        .copied()
        .map(JsonRef::from_hash_bytes)
        .collect::<Vec<_>>();

    let dead_packed_change_ids = sweep_authority_commits
        .iter()
        .filter_map(|commit_id| packed.commits.get(commit_id))
        .flat_map(|entry| entry.members.iter().map(|member| member.value.change_id))
        .collect::<BTreeSet<_>>();
    crate::tracked_state::stage_delete_change_locators(
        writes,
        dead_packed_change_ids.difference(&live_change_ids).copied(),
    );
    let relocated_locators = retained_authority_commits
        .iter()
        .filter_map(|commit_id| {
            packed
                .commits
                .get(commit_id)
                .map(|entry| (*commit_id, entry))
        })
        .flat_map(|(commit_id, entry)| {
            let dead_packed_change_ids = &dead_packed_change_ids;
            entry.members.iter().filter_map(move |member| {
                dead_packed_change_ids
                    .contains(&member.value.change_id)
                    .then_some(member)
                    .filter(|member| member.authored)
                    .map(|member| crate::tracked_state::CommitDeltaChangeLocator {
                        change_id: member.value.change_id,
                        commit_id,
                        segment_index: member.segment_index,
                        ordinal: u16::try_from(member.ordinal)
                            .expect("commit-delta segment row count fits u16"),
                    })
            })
        })
        .fold(BTreeMap::new(), |mut locators, locator| {
            locators.entry(locator.change_id).or_insert(locator);
            locators
        })
        .into_values()
        .collect::<Vec<_>>();
    crate::tracked_state::stage_change_locators(writes, &relocated_locators);

    writes.delete_batch(
        COMMIT_SPACE,
        sweep_commits.iter().map(|commit_id| commit_key(*commit_id)),
    );
    stage_sweep_unreachable_content_nodes(
        store,
        writes,
        crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
        &live_scoped_range_nodes,
    )
    .await?;
    stage_sweep_unreachable_content_nodes(
        store,
        writes,
        crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
        &live_mutation_directory_nodes,
    )
    .await?;
    stage_sweep_unreachable_content_nodes(
        store,
        writes,
        crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        &live_current_state_data_parts,
    )
    .await?;
    stage_sweep_unreachable_content_nodes(
        store,
        writes,
        crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
        &live_current_state_data_parts,
    )
    .await?;
    for commit_id in &sweep_authority_commits {
        if let Some(entry) = packed.commits.get(commit_id) {
            let schema_keys = entry
                .members
                .iter()
                .map(|member| member.key.schema_key.as_str())
                .collect::<BTreeSet<_>>();
            for schema_key in schema_keys {
                crate::columnar_row_group::stage_delete_row_group_set(
                    store,
                    writes,
                    crate::live_state::entity_row_group_set_id(*commit_id, schema_key),
                )
                .await?;
            }
            crate::tracked_state::stage_delete_commit_delta_inventory_entry(
                writes, *commit_id, entry,
            )?;
        }
    }
    writes.delete_batch(
        COMMIT_CHANGE_ID_SPACE,
        sweep_commit_change_ids
            .iter()
            .map(|change_id| commit_change_id_key(*change_id)),
    );
    writes.delete_batch(
        CHANGE_SPACE,
        sweep_changes.iter().map(|change_id| change_key(*change_id)),
    );
    JsonStoreContext::new()
        .writer()
        .stage_delete_refs(writes, sweep_json_payloads.iter().copied());
    writes.seal_changelog_gc();

    Ok(GcPlan {
        roots: roots.to_vec(),
        live: GcLiveSet {
            commits: live_commits.into_iter().collect(),
            changes: live_change_ids.into_iter().collect(),
            payloads: live_payload_hashes
                .into_iter()
                .map(JsonRef::from_hash_bytes)
                .collect(),
        },
        sweep: GcSweepSet {
            commits: sweep_commits,
            commit_change_ids: sweep_commit_change_ids,
            changes: sweep_changes,
            json_payloads: sweep_json_payloads,
        },
        repair: GcRepairSet::default(),
    })
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct GcCommitInventoryEntry {
    commit_id: CommitId,
    change_id: ChangeId,
    parent_start: usize,
    parent_len: usize,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct GcCommitInventory {
    entries: Vec<GcCommitInventoryEntry>,
    parent_commit_ids: Vec<CommitId>,
}

#[cfg(test)]
impl GcCommitInventory {
    fn get(&self, commit_id: CommitId) -> Option<&GcCommitInventoryEntry> {
        self.entries
            .binary_search_by_key(&commit_id, |entry| entry.commit_id)
            .ok()
            .map(|index| &self.entries[index])
    }

    fn parent_commit_ids(&self, entry: &GcCommitInventoryEntry) -> &[CommitId] {
        &self.parent_commit_ids[entry.parent_start..entry.parent_start + entry.parent_len]
    }

    fn iter(&self) -> impl Iterator<Item = &GcCommitInventoryEntry> {
        self.entries.iter()
    }
}

#[cfg(test)]
async fn scan_all_gc_commits<S>(store: S) -> Result<GcCommitInventory, LixError>
where
    S: StorageAdapterRead,
{
    let mut reader = ChangelogContext::new().reader(store);
    let mut commits = GcCommitInventory::default();
    let mut start_after = None::<String>;
    loop {
        let batch = reader
            .scan_commits(CommitScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1_024),
            })
            .await?;
        for commit in batch.entries {
            if commits
                .entries
                .last()
                .is_some_and(|previous| previous.commit_id >= commit.commit_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "garbage collection found duplicate or out-of-order commit '{}'",
                        commit.commit_id
                    ),
                ));
            }
            let parent_start = commits.parent_commit_ids.len();
            let parent_len = commit.parent_commit_ids.len();
            commits
                .parent_commit_ids
                .extend(commit.parent_commit_ids.into_iter());
            commits.entries.push(GcCommitInventoryEntry {
                commit_id: commit.commit_id,
                change_id: commit.change_id,
                parent_start,
                parent_len,
            });
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    Ok(commits)
}

#[cfg(test)]
async fn scan_all_gc_standalone_changes<S>(
    store: S,
) -> Result<BTreeMap<ChangeId, ChangeRecord>, LixError>
where
    S: StorageAdapterRead,
{
    let mut reader = ChangelogContext::new().reader(store);
    let mut changes = BTreeMap::new();
    let mut start_after = None::<String>;
    loop {
        let batch = reader
            .scan_changes(ChangeScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1_024),
            })
            .await?;
        for change in batch.entries {
            if changes.insert(change.change_id, change.clone()).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "garbage collection found duplicate standalone change '{}'",
                        change.change_id
                    ),
                ));
            }
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    Ok(changes)
}

#[cfg(test)]
fn collect_change_payload_hashes(change: &ChangeRecord, hashes: &mut BTreeSet<[u8; 32]>) {
    for slot in [&change.snapshot, &change.metadata] {
        if let JsonSlot::Ref(json_ref) = slot {
            hashes.insert(*json_ref.as_hash_array());
        }
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn retirement_is_proven(
    old_root: CommitId,
    new_root: Option<CommitId>,
    active_authority_ids: &BTreeSet<CommitId>,
    active_dependency_ids: &BTreeSet<CommitId>,
) -> bool {
    new_root != Some(old_root)
        && !active_authority_ids.contains(&old_root)
        && !active_dependency_ids.contains(&old_root)
}

/// Removes the semantic commit projection once its root interval is no longer
/// reachable. Physical tracked-state authority may remain alive as a selected
/// source or serving dependency, so this decision is intentionally separate
/// from manifest/CAS retirement.
async fn stage_delete_semantic_commit_projection<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let commit_ids = [commit_id];
    let record = ChangelogContext::new()
        .reader(store)
        .load_commits(CommitLoadRequest {
            commit_ids: &commit_ids,
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, record)| record);
    let Some(record) = record else {
        // A prior GC pass may already have removed the semantic projection
        // while its physical authority remained pinned.
        return Ok(());
    };
    if record.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit projection key for '{commit_id}' contains '{}'",
                record.commit_id
            ),
        ));
    }
    writes.delete(COMMIT_SPACE, StorageKey(Bytes::from(commit_key(commit_id))));
    writes.delete(
        COMMIT_CHANGE_ID_SPACE,
        StorageKey(Bytes::from(commit_change_id_key(record.change_id))),
    );
    writes.delete(
        CHANGE_SPACE,
        StorageKey(Bytes::from(change_key(record.change_id))),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::branch::{BranchHeadControl, BranchHeadControlContext, stage_branch_head_control};
    use crate::changelog::{
        ChangeId, ChangeLoadRequest, ChangeRecord, ChangelogAppend, ChangelogContext,
        ChangelogReader, ChangelogWriter, CommitId, CommitLoadRequest, CommitRecord, GcRoot,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::json_store::{
        JsonRef, JsonSlot, JsonStoreContext, JsonWritePlacementRef, NormalizedJson,
        NormalizedJsonRef, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
    };
    use crate::live_state::{CurrentStateDeltaRef, TrackedHeadContext, WorkingDiffIndexCoverage};
    use crate::storage_adapter::{
        Memory, PointReadPlan, SharedStorageAdapterRead, StorageAdapter, StorageGetOptions,
        StorageKey, StorageReadOptions, StorageSpace, StorageValue, StorageWriteOptions,
    };
    use crate::tracked_state::{
        CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
        TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef,
        load_change_record_by_id, scan_change_records_from_commit_deltas,
        scan_commit_delta_inventory, stage_addressable_commit_deltas_with_selected_source,
        stage_change_locators, stage_commit_deltas_for_commit_state, stage_commit_state_manifest,
    };
    use crate::{GLOBAL_BRANCH_ID, Value, engine::Engine};
    use bytes::Bytes;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use super::{
        CheckpointGcState, CheckpointRecoveryRef, RootReachabilityDelta, load_checkpoint_gc_state,
        load_reachability_batches, load_reachability_queue, load_recovery_ref, load_recovery_refs,
        retirement_is_proven, root_control_digest_for_control, stage_checkpoint_gc_state,
        stage_reachability_delta_batch, stage_reachability_queue_seed, stage_recovery_ref_rotation,
    };

    #[tokio::test]
    async fn recovery_ref_rotation_replaces_only_the_target_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let first_main = recovery("main", "main-old-1", "main-checkpoint-1");
        let first_other = recovery("other", "other-old-1", "other-checkpoint-1");
        let mut writes = storage.new_write_set();
        stage_recovery_ref_rotation(&mut writes, &first_main)
            .expect("first main recovery ref should stage");
        stage_recovery_ref_rotation(&mut writes, &first_other)
            .expect("other recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("initial recovery refs should commit");

        let mut second_main = recovery("main", "main-old-2", "main-checkpoint-2");
        second_main.interval_has_commits = false;
        let mut writes = storage.new_write_set();
        stage_recovery_ref_rotation(&mut writes, &second_main)
            .expect("second main recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rotated recovery ref should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("recovery read should open");
        assert_eq!(
            load_recovery_refs(&read)
                .await
                .expect("recovery refs should load"),
            vec![second_main.clone(), first_other]
        );
        assert_eq!(
            load_recovery_ref(&read, "main")
                .await
                .expect("main recovery ref should load"),
            Some(second_main)
        );
    }

    #[tokio::test]
    async fn reachability_delta_queue_round_trips_with_authenticated_digest() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("queue seed should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("queue seed should commit");

        let old_root = CommitId::for_test_label("delta-old-root");
        let new_root = CommitId::for_test_label("delta-new-root");
        let timestamp =
            LixTimestamp::expect_parse("reachability delta test timestamp", "2026-01-01T00:00:00Z");
        let old_control = BranchHeadControl {
            head_commit_id: old_root,
            tracked_generation: old_root,
            untracked_generation: old_root,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("delta-old-control"),
            schema_presence_bloom: [0; 4],
        };
        let new_control = BranchHeadControl {
            head_commit_id: new_root,
            tracked_generation: new_root,
            untracked_generation: new_root,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("delta-new-control"),
            schema_presence_bloom: [0; 4],
        };
        let delta = RootReachabilityDelta {
            branch_id: "main".to_string(),
            old_root: Some(old_root),
            new_root: Some(new_root),
            old_control: Some(old_control),
            new_control: Some(new_control),
            old_control_digest: root_control_digest_for_control(Some(&old_control))
                .expect("old control should encode"),
            new_control_digest: root_control_digest_for_control(Some(&new_control))
                .expect("new control should encode"),
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("queue read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            std::slice::from_ref(&delta),
            &[new_root],
            &mut preconditions,
        )
        .await
        .expect("delta should stage");
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("delta should commit atomically");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("queue read should reopen");
        let (queue, _) = load_reachability_queue(&read)
            .await
            .expect("queue should decode");
        let batches = load_reachability_batches(&read, &queue)
            .await
            .expect("delta should decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.checkpoint_roots, vec![new_root]);
        assert_eq!(batches[0].1.deltas[0].old_root, Some(old_root));
    }

    #[test]
    fn retirement_requires_history_and_pin_dependency_closure() {
        let old = CommitId::for_test_label("history-old");
        let new = CommitId::for_test_label("history-new");
        let active = BTreeSet::from([new]);
        let mut dependencies = BTreeSet::from([old]);
        assert!(!retirement_is_proven(
            old,
            Some(new),
            &active,
            &dependencies
        ));
        // Once history/diff/undo/redo/checkpoint pins release the old root,
        // the delta is a valid physical-retirement proof.
        dependencies.clear();
        assert!(retirement_is_proven(old, Some(new), &active, &dependencies));
    }

    #[tokio::test]
    async fn repository_gc_state_round_trips() {
        let storage = StorageAdapter::new(Memory::new());
        let expected = CheckpointGcState {
            checkpoint_sequence: 129,
            last_gc_sequence: 64,
            collectible_interval_count: 65,
        };
        let mut writes = storage.new_write_set();
        stage_checkpoint_gc_state(&mut writes, &expected)
            .expect("checkpoint GC state should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("checkpoint GC state should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("GC-state read should open");
        assert_eq!(
            load_checkpoint_gc_state(&read)
                .await
                .expect("checkpoint GC state should load"),
            expected
        );
    }

    #[tokio::test]
    async fn shared_current_state_nodes_are_swept_by_reachability_not_owner() {
        let storage = StorageAdapter::new(Memory::new());
        let live_id = [1u8; 32];
        let dead_id = [2u8; 32];
        let mut writes = storage.new_write_set();
        for space in [
            crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
            crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        ] {
            for node_id in [live_id, dead_id] {
                writes.put(
                    space,
                    StorageKey(Bytes::copy_from_slice(&node_id)),
                    StorageValue {
                        bytes: Bytes::from_static(b"node"),
                    },
                );
            }
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("shared-node fixture should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("shared-node read should open");
        let mut sweep = storage.new_write_set();
        let live = BTreeSet::from([live_id]);
        for space in [
            crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
            crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        ] {
            super::stage_sweep_unreachable_content_nodes(&read, &mut sweep, space, &live)
                .await
                .expect("content-addressed sweep should stage");
        }
        drop(read);
        storage
            .commit_write_set(sweep, StorageWriteOptions::default())
            .await
            .expect("content-addressed sweep should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-sweep read should open");
        let keys = [
            StorageKey(Bytes::copy_from_slice(&live_id)),
            StorageKey(Bytes::copy_from_slice(&dead_id)),
        ];
        for space in [
            crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
            crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
        ] {
            let loaded = PointReadPlan::new(space, &keys)
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("post-sweep nodes should load");
            assert!(loaded.value[0].is_some(), "shared live node must survive");
            assert!(loaded.value[1].is_none(), "unreachable node must be swept");
        }
    }

    #[tokio::test]
    async fn repository_gc_keeps_heads_rooted_only_by_v6_controls() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let receipt = crate::init::initialize(storage.clone(), &tracked_state)
            .await
            .expect("repository should initialize");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("GC read should open"),
        );
        let mut writes = storage.new_write_set();
        let plan = super::stage_repository_gc_full_recovery(read, &mut writes)
            .await
            .expect("GC should plan from direct branch controls");
        let initial_commit = CommitId::parse_lix(&receipt.initial_commit_id, "initial commit")
            .expect("initial receipt should contain a commit id");
        assert!(
            !plan.changelog.sweep.commits.contains(&initial_commit),
            "the initial commit must stay live even though init writes no flat lix_branch_ref row"
        );
        let controls = BranchHeadControlContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("control read should open"),
            )
            .scan()
            .await
            .expect("v6 controls should scan");
        assert_eq!(
            controls.len(),
            2,
            "init should publish both direct controls"
        );
        for (_branch_id, control) in controls {
            assert!(
                plan.changelog.live.changes.contains(&control.ref_change_id),
                "the control's current public branch-ref ledger change must stay live"
            );
            assert!(
                !plan
                    .changelog
                    .sweep
                    .changes
                    .contains(&control.ref_change_id),
                "GC must not sweep a direct control's current public branch-ref ledger change"
            );
        }
    }

    #[tokio::test]
    async fn repository_gc_reclaims_retired_untracked_update_without_inventory_sweep() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        let old_value = serde_json::json!({
            "payload": "old-".repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
        });
        let old_json = old_value.to_string();
        let old_ref = key_value_snapshot_ref("gc-update-untracked", &old_value);
        let new_value = serde_json::json!({
            "payload": "new-".repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
        });
        let new_json = new_value.to_string();
        let new_ref = key_value_snapshot_ref("gc-update-untracked", &new_value);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('gc-update-untracked', lix_json($1), true)",
                &[Value::Text(old_json)],
            )
            .await
            .expect("old untracked value should write");
        session
            .execute(
                "UPDATE lix_key_value SET value = lix_json($1) \
                 WHERE key = 'gc-update-untracked'",
                &[Value::Text(new_json)],
            )
            .await
            .expect("untracked value should update in the same generation");

        // A bare JSON object has no reclaim candidate. It proves this GC is
        // deliberately targeted rather than a full JSON-store inventory scan.
        let bare_orphan_ref =
            stage_bare_json(&storage, &format!("\"{}\"", "bare-".repeat(1024))).await;

        run_repository_gc(&storage).await;

        assert!(
            !json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, old_ref).await,
            "the superseded untracked payload should be reclaimed"
        );
        assert!(
            json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, new_ref).await,
            "the current untracked payload must remain stored"
        );
        assert!(
            json_ref_exists(
                &storage,
                crate::json_store::store::JSON_SPACE,
                bare_orphan_ref,
            )
            .await,
            "candidate GC must not scan and sweep unrelated JSON"
        );
        assert!(
            !json_ref_exists(&storage, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, old_ref,).await,
            "a dead payload should consume its reclamation candidate"
        );

        let visible = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'gc-update-untracked'",
                &[],
            )
            .await
            .expect("live untracked value should remain readable after GC");
        assert_eq!(visible.rows()[0].values(), &[Value::Json(new_value)]);
    }

    #[tokio::test]
    async fn repository_gc_retains_active_history_diff_undo_redo_and_reclaims_deleted_branch_refs()
    {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        let schema = serde_json::json!({
            "x-lix-key": "gc_history_fixture",
            "x-lix-primary-key": ["/path"],
            "type": "object",
            "required": ["path", "value"],
            "properties": {
                "path": { "type": "string" },
                "value": { "type": ["object", "array", "string", "number", "integer", "boolean", "null"] }
            },
            "additionalProperties": false
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("history fixture schema should register");
        let baseline = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .expect("history baseline should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("history baseline should have a commit id");
        session
            .execute(
                "INSERT INTO gc_history_fixture (path, value) VALUES ('/row', lix_json('{\"v\":1}'))",
                &[],
            )
            .await
            .expect("history fixture first commit should publish");
        session
            .execute(
                "UPDATE gc_history_fixture SET value = lix_json('{\"v\":2}') WHERE path = '/row'",
                &[],
            )
            .await
            .expect("history fixture second commit should publish");
        let branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-000000000009".to_owned()),
                name: "gc-history-dead".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("history fixture branch should create");
        session
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.id)],
            )
            .await
            .expect("history fixture branch should delete");

        let before_changes = {
            let storage_adapter = StorageAdapter::new(storage.clone());
            let read = storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("history fixture pre-GC read should open");
            super::scan_all_gc_standalone_changes(read)
                .await
                .expect("history fixture standalone scan should load")
        };
        let active_refs_before = {
            let storage_adapter = StorageAdapter::new(storage.clone());
            let read = storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("history fixture control read should open");
            BranchHeadControlContext::new()
                .reader(read)
                .scan()
                .await
                .expect("history fixture controls should load")
                .into_iter()
                .map(|(_, control)| control.ref_change_id)
                .collect::<BTreeSet<_>>()
        };
        run_repository_gc(&storage).await;
        let after_changes = {
            let storage_adapter = StorageAdapter::new(storage.clone());
            let read = storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("history fixture post-GC read should open");
            super::scan_all_gc_standalone_changes(read)
                .await
                .expect("history fixture standalone post-GC scan should load")
        };
        let reclaimed = before_changes
            .keys()
            .filter(|change_id| !after_changes.contains_key(change_id))
            .copied()
            .collect::<BTreeSet<_>>();
        assert!(
            !reclaimed.is_empty(),
            "deleted branch refs should be reclaimed"
        );
        assert!(
            reclaimed.is_disjoint(&active_refs_before),
            "GC must not reclaim an active branch reference"
        );
        assert!(
            reclaimed.iter().any(|change_id| {
                before_changes
                    .get(change_id)
                    .is_some_and(|change| change.schema_key == "lix_branch_ref")
            }),
            "the retired branch's standalone reference must be reclaimed"
        );
        let after = after_changes.len();
        let before = before_changes.len();
        assert!(after < before, "deleted branch refs should be reclaimed");

        drop(session);
        let reopened_engine = Engine::new(storage.clone())
            .await
            .expect("repository should reopen after GC");
        let session = reopened_engine
            .open_workspace_session()
            .await
            .expect("reopened workspace session should open");
        let head = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .expect("history head should remain readable")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("history head should have a commit id");
        let diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'gc_history_fixture'",
                &[Value::Text(baseline), Value::Text(head)],
            )
            .await
            .expect("active history diff should survive GC");
        assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
        session.undo().await.expect("active undo should survive GC");
        session.redo().await.expect("active redo should survive GC");
    }

    #[tokio::test]
    async fn repository_gc_reclaims_retired_untracked_delete() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        let deleted_value = serde_json::json!({
            "payload": "delete-".repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
        });
        let deleted_json = deleted_value.to_string();
        let deleted_ref = key_value_snapshot_ref("gc-delete-untracked", &deleted_value);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('gc-delete-untracked', lix_json($1), true)",
                &[Value::Text(deleted_json)],
            )
            .await
            .expect("untracked value should write");
        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'gc-delete-untracked'",
                &[],
            )
            .await
            .expect("untracked value should delete physically");

        run_repository_gc(&storage).await;

        assert!(
            !json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, deleted_ref,).await,
            "a deleted untracked payload should be reclaimed"
        );
        assert!(
            !json_ref_exists(
                &storage,
                UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
                deleted_ref,
            )
            .await,
            "a reclaimed deletion should consume its candidate"
        );
    }

    #[tokio::test]
    async fn repository_gc_keeps_candidate_reachable_from_tracked_history() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        let shared_value = serde_json::json!({
            "payload": "shared-".repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
        });
        let shared_json = shared_value.to_string();
        let shared_ref = key_value_snapshot_ref("gc-tracked-candidate", &shared_value);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('gc-tracked-candidate', lix_json($1))",
                &[Value::Text(shared_json)],
            )
            .await
            .expect("tracked owner should write");

        // Candidate records are merely ownership-loss hints. Injecting one
        // for a tracked payload exercises the exact same liveness proof that
        // protects a hash shared with retained history.
        stage_untracked_reclaim_candidate(&storage, shared_ref).await;

        run_repository_gc(&storage).await;

        assert!(
            json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, shared_ref).await,
            "reachable tracked history must retain a candidate payload"
        );
        assert!(
            json_ref_exists(&storage, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, shared_ref,).await,
            "a candidate rooted by history must survive for later re-evaluation"
        );
        let tracked = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'gc-tracked-candidate'",
                &[],
            )
            .await
            .expect("tracked owner should remain readable");
        assert_eq!(tracked.rows()[0].values(), &[Value::Json(shared_value)]);
    }

    #[tokio::test]
    async fn authority_gc_rejects_missing_live_commit_state_before_staging_deletes() {
        let storage = StorageAdapter::new(Memory::new());
        let live = gc_authority_record("gc-missing-live-authority");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-authority fixture read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: vec![live.clone()],
                changes: Vec::new(),
            })
            .await
            .expect("missing-authority commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("missing-authority fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("missing-authority GC read should open"),
        );
        let mut gc_writes = storage.new_write_set();
        let error = super::plan_and_stage_authority_gc(
            &read,
            &mut gc_writes,
            &[GcRoot::BranchHead(live.commit_id)],
        )
        .await
        .expect_err("GC must reject a live commit without hard-cut authority");
        assert!(error.message.contains("has no commit-state authority"));
        assert!(
            gc_writes.is_empty(),
            "authority validation must precede every destructive GC mutation"
        );
    }

    #[tokio::test]
    async fn authority_gc_retains_tombstone_only_checkpoint_alias_source() {
        let storage = Memory::new();
        let storage_adapter = StorageAdapter::new(storage);
        let source_commit = CommitId::for_test_label("gc-tombstone-alias-source");
        let alias_commit = CommitId::for_test_label("gc-tombstone-alias-live");
        let authority_commit = CommitId::for_test_label("gc-tombstone-alias-authority");
        let live_head = CommitId::for_test_label("gc-tombstone-alias-head");
        let source_change = packed_change(
            "gc-tombstone-alias-source-change",
            "deleted-source-member",
            JsonSlot::None,
        );
        let marker_change = packed_change(
            "gc-tombstone-alias-marker-change",
            "deleted-local-marker",
            JsonSlot::None,
        );
        let timestamp =
            LixTimestamp::expect_parse("tombstone alias timestamp", "2026-01-01T00:00:00Z");
        let commits = [
            CommitRecord {
                format_version: 2,
                commit_id: source_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label("gc-tombstone-alias-source-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 2,
                commit_id: alias_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label("gc-tombstone-alias-live-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 2,
                commit_id: authority_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label("gc-tombstone-alias-authority-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 2,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![alias_commit, authority_commit],
                change_id: ChangeId::for_test_label("gc-tombstone-alias-head-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
        ];

        let mut read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("tombstone alias fixture read should open");
        let mut writes = storage_adapter.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: commits.to_vec(),
                changes: Vec::new(),
            })
            .await
            .expect("tombstone alias headers should stage");
        let base_coordinate = crate::tracked_state::TrackedStateBaseCoordinate {
            base_commit_id: source_commit,
            group_index: 7,
            row_index: 11,
        };
        let mut source_deltas =
            commit_delta_refs(source_commit, std::slice::from_ref(&source_change));
        source_deltas[0].base_coordinate = Some(base_coordinate);
        let source_stage = stage_commit_deltas_for_commit_state(&mut writes, &source_deltas)
            .expect("source tombstone should stage");
        stage_change_locators(&mut writes, &source_stage.locators);
        let authority_deltas =
            commit_delta_refs(authority_commit, std::slice::from_ref(&source_change));
        let authority_stage = stage_commit_deltas_for_commit_state(&mut writes, &authority_deltas)
            .expect("surviving tombstone authority should stage");
        let alias_local = commit_delta_refs(alias_commit, std::slice::from_ref(&marker_change));
        let alias_stage = stage_addressable_commit_deltas_with_selected_source(
            &mut writes,
            &alias_local,
            &[false],
            source_commit,
        )
        .expect("tombstone-only alias should stage");
        let inventories = BTreeMap::from([
            (source_commit, source_stage.mutation_inventory().clone()),
            (
                authority_commit,
                authority_stage.mutation_inventory().clone(),
            ),
            (alias_commit, alias_stage.mutation_inventory().clone()),
        ]);
        let scope = crate::tracked_state::scoped_range::ScopedRangePrefix::try_from_components([
            b"gc-selected-source-empty".as_slice(),
        ])
        .expect("GC selected-source scope should encode");
        let tree = crate::tracked_state::scoped_range::stage_scoped_range_tree(
            &mut writes,
            [(
                crate::tracked_state::scoped_range::ScopedRangeCoverageMarker {
                    scope,
                    row_count: 0,
                    part_count: 0,
                },
                Vec::new(),
            )],
        )
        .expect("GC selected-source empty serving tree should stage");
        let source_inventory = inventories[&source_commit].clone();
        let source_root = crate::tracked_state::attest_scoped_range_root(
            source_commit,
            None,
            &source_inventory,
            tree.clone(),
        )
        .expect("source serving root should attest");
        let mut source_manifest = test_commit_state_manifest(&commits[0], source_inventory);
        source_manifest.current_state_scoped_ranges = Some(Box::new(source_root.clone()));
        crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
            &mut writes,
            &source_manifest,
        )
        .expect("source serving authority should stage");

        let alias_inventory = inventories[&alias_commit].clone();
        let alias_root = crate::tracked_state::attest_scoped_range_root(
            alias_commit,
            Some((source_commit, &source_root)),
            &alias_inventory,
            tree,
        )
        .expect("selected-source serving root should attest");
        let mut alias_manifest = test_commit_state_manifest(&commits[1], alias_inventory);
        alias_manifest.current_state_scoped_ranges = Some(Box::new(alias_root));
        crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
            &mut writes,
            &alias_manifest,
        )
        .expect("selected-source serving authority should stage");

        for record in &commits[2..] {
            let manifest = test_commit_state_manifest(
                record,
                inventories
                    .get(&record.commit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
            crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
                &mut writes,
                &manifest,
            )
            .expect("GC retained-authority fixture manifest should stage");
        }
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tombstone alias fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("tombstone alias GC read should open"),
        );
        let mut writes = storage_adapter.new_write_set();
        let plan = super::plan_and_stage_authority_gc(
            &read,
            &mut writes,
            &[GcRoot::BranchHead(live_head)],
        )
        .await
        .expect("tombstone alias GC should plan");
        assert!(plan.sweep.commits.contains(&source_commit));
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tombstone alias GC should commit");

        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("tombstone alias verification read should open");
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("materialized tombstone alias inventory should load");
        assert!(inventory.commits.contains_key(&source_commit));
        let alias = inventory
            .commits
            .get(&alias_commit)
            .expect("live alias should remain readable");
        assert_eq!(alias.selected_source_commit_id, Some(source_commit));
        assert_eq!(alias.members.len(), 2);
        assert!(alias.members.iter().all(|member| member.value.deleted));
        assert_eq!(
            alias
                .members
                .iter()
                .find(|member| member.value.change_id == source_change.change_id)
                .and_then(|member| member.base_coordinate),
            Some(base_coordinate),
            "retained immutable authority must preserve the columnar-base address"
        );
        assert_eq!(
            alias
                .members
                .iter()
                .filter(|member| member.authored)
                .count(),
            1,
            "the local marker stays authored while the borrowed cascade tombstone does not"
        );
        scan_change_records_from_commit_deltas(&read)
            .await
            .expect("retained cascade tombstone authority must preserve canonical history");
    }

    #[tokio::test]
    async fn authority_gc_retains_immutable_packed_owner_and_sweeps_dead_standalone_fact() {
        let storage = Memory::new();
        let storage_adapter = StorageAdapter::new(storage.clone());
        let shared_ref = stage_bare_json(&storage, r#"{"payload":"shared"}"#).await;
        let dead_only_ref = stage_bare_json(&storage, r#"{"payload":"dead-only"}"#).await;
        let live_standalone_ref =
            stage_bare_json(&storage, r#"{"payload":"live-standalone"}"#).await;

        let live_parent = CommitId::for_test_label("authority-gc-live-parent");
        let live_head = CommitId::for_test_label("authority-gc-live-head");
        let dead_commit = CommitId::for_test_label("authority-gc-dead");
        let live_member = packed_change(
            "authority-gc-live-member",
            "live-member",
            JsonSlot::Ref(shared_ref),
        );
        let dead_shared_member = live_member.clone();
        let dead_only_member = packed_change(
            "authority-gc-dead-only-member",
            "dead-only-member",
            JsonSlot::Ref(dead_only_ref),
        );
        let live_standalone = packed_change(
            "authority-gc-live-standalone",
            "live-standalone",
            JsonSlot::Ref(live_standalone_ref),
        );
        let dead_standalone = packed_change(
            "authority-gc-dead-standalone",
            "dead-standalone",
            JsonSlot::Ref(dead_only_ref),
        );
        let timestamp =
            LixTimestamp::expect_parse("authority GC timestamp", "2026-01-01T00:00:00.000Z");
        let commits = vec![
            CommitRecord {
                format_version: 2,
                commit_id: live_parent,
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label("authority-gc-live-parent-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 2,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![live_parent],
                change_id: ChangeId::for_test_label("authority-gc-live-head-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 2,
                commit_id: dead_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label("authority-gc-dead-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
        ];

        let mut read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open");
        let mut writes = storage_adapter.new_write_set();
        let mut writer = ChangelogContext::new().writer(&mut read, &mut writes);
        writer
            .stage_append(ChangelogAppend {
                commits: commits.clone(),
                changes: vec![live_standalone.clone(), dead_standalone.clone()],
            })
            .await
            .expect("authority GC changelog fixture should stage");
        drop(writer);
        let mut live_deltas = commit_delta_refs(live_parent, std::slice::from_ref(&live_member));
        // The surviving copy is a merge/checkpoint selection. Once the
        // original owner dies, GC promotes it through locator relocation.
        live_deltas[0].authored = false;
        let live_stage = stage_commit_deltas_for_commit_state(&mut writes, &live_deltas)
            .expect("live packed member should stage");
        let dead_members = vec![dead_shared_member.clone(), dead_only_member.clone()];
        let dead_deltas = commit_delta_refs(dead_commit, &dead_members);
        let dead_stage = stage_commit_deltas_for_commit_state(&mut writes, &dead_deltas)
            .expect("dead packed members should stage");
        let dead_locators = dead_stage.locators.clone();
        let inventories = BTreeMap::from([
            (live_parent, live_stage.mutation_inventory().clone()),
            (dead_commit, dead_stage.mutation_inventory().clone()),
        ]);
        for record in &commits {
            let mutations = inventories
                .get(&record.commit_id)
                .cloned()
                .unwrap_or_default();
            let mut manifest = test_commit_state_manifest(record, mutations);
            manifest.replay_debt = CommitStateReplayDebt::default();
            manifest.snapshot_root = Some(Box::new(test_snapshot_root(record.commit_id)));
            stage_commit_state_manifest(&mut writes, &manifest)
                .expect("rooted GC fixture commit-state manifest should stage");
        }
        let sidecar_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let sidecar_batch = RecordBatch::try_new(
            Arc::clone(&sidecar_schema),
            vec![Arc::new(StringArray::from(vec!["value"]))],
        )
        .expect("sidecar batch");
        let sidecar = crate::columnar_row_group::encode_row_group_set(
            "authority_gc",
            sidecar_schema,
            &[sidecar_batch],
        )
        .expect("encode GC sidecar");
        for commit_id in [live_parent, dead_commit] {
            crate::columnar_row_group::stage_row_group_set(
                &mut writes,
                crate::live_state::entity_row_group_set_id(commit_id, "authority_gc"),
                &sidecar,
            )
            .expect("stage GC sidecar");
        }
        // Point the shared change at the owner about to be collected. GC must
        // relocate it to the surviving physical copy, not delete the index.
        stage_change_locators(&mut writes, &dead_locators);
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority GC fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("GC read should open"),
        );
        let mut writes = storage_adapter.new_write_set();
        let plan = super::plan_and_stage_authority_gc(
            &read,
            &mut writes,
            &[
                GcRoot::BranchHead(live_head),
                GcRoot::StandaloneChange(live_standalone.change_id),
            ],
        )
        .await
        .expect("authority GC should plan");
        assert_eq!(plan.sweep.commits, vec![dead_commit]);
        assert_eq!(plan.sweep.changes, vec![dead_standalone.change_id]);
        assert!(
            !plan.sweep.json_payloads.contains(&shared_ref),
            "a payload shared with live packed history must stay live"
        );
        assert!(
            !plan.sweep.json_payloads.contains(&dead_only_ref),
            "co-resident immutable part members and their payloads remain reachable"
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority GC sweep should commit");

        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let mut reader = ChangelogContext::new().reader(&read);
        let commit_ids = [live_parent, live_head, dead_commit];
        let commits = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("commit headers should load");
        assert_eq!(
            commits
                .iter()
                .map(|(_, value)| value.is_some())
                .collect::<Vec<_>>(),
            [true, true, false]
        );
        let change_ids = [live_standalone.change_id, dead_standalone.change_id];
        let changes = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await
            .expect("standalone facts should load");
        assert_eq!(
            changes
                .iter()
                .map(|(_, value)| value.is_some())
                .collect::<Vec<_>>(),
            [true, false]
        );
        assert!(
            load_change_record_by_id(&read, live_member.change_id)
                .await
                .expect("retained live authority should load")
                .is_some()
        );
        assert!(
            load_change_record_by_id(&read, dead_only_member.change_id)
                .await
                .expect("co-resident authority lookup should succeed")
                .is_some()
        );
        let canonical_changes = scan_change_records_from_commit_deltas(&read)
            .await
            .expect("post-GC packed changes should stream");
        assert!(
            canonical_changes
                .iter()
                .any(|change| change.change_id == live_member.change_id)
        );
        assert!(
            canonical_changes
                .iter()
                .any(|change| change.change_id == dead_only_member.change_id)
        );
        let inventory = scan_commit_delta_inventory(&read)
            .await
            .expect("post-GC packed inventory should scan");
        assert!(inventory.commits.contains_key(&live_parent));
        assert!(inventory.commits.contains_key(&dead_commit));
        assert!(
            crate::tracked_state::load_snapshot_commit_root(&read, &live_parent.to_string())
                .await
                .expect("live snapshot lookup should succeed")
                .is_some(),
            "a retained semantic commit keeps its immutable snapshot authority"
        );
        assert!(
            crate::tracked_state::load_snapshot_commit_root(&read, &dead_commit.to_string())
                .await
                .expect("dead snapshot lookup should succeed")
                .is_none(),
            "retained selected-source bytes must not authorize a swept semantic commit"
        );
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, dead_commit)
                .await
                .expect("retained physical source lookup should succeed")
                .is_some(),
            "the test must retain dead mutation authority for the live selected source"
        );
        assert!(
            crate::columnar_row_group::load_row_group_manifest(
                &read,
                crate::live_state::entity_row_group_set_id(live_parent, "authority_gc"),
            )
            .await
            .expect("load live sidecar")
            .is_some(),
            "reachable commit sidecars must survive repository GC"
        );
        assert!(
            crate::columnar_row_group::load_row_group_manifest(
                &read,
                crate::live_state::entity_row_group_set_id(dead_commit, "authority_gc"),
            )
            .await
            .expect("load retained authority sidecar")
            .is_some(),
            "sidecars co-owned by retained immutable authority must survive"
        );
        drop(read);
        assert!(json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, shared_ref).await);
        assert!(
            json_ref_exists(
                &storage,
                crate::json_store::store::JSON_SPACE,
                dead_only_ref,
            )
            .await
        );
        assert!(
            json_ref_exists(
                &storage,
                crate::json_store::store::JSON_SPACE,
                live_standalone_ref,
            )
            .await
        );
    }

    #[tokio::test]
    async fn repository_gc_reclaims_candidate_after_last_live_untracked_owner_disappears() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let shared_ref = stage_bare_json(
            &storage,
            &serde_json::json!({
                "payload": "shared-untracked-"
                    .repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
            })
            .to_string(),
        )
        .await;
        stage_untracked_current_owner(&storage, "gc-untracked-owner-a", Some(shared_ref)).await;
        stage_untracked_current_owner(&storage, "gc-untracked-owner-b", Some(shared_ref)).await;
        stage_untracked_current_owner(&storage, "gc-untracked-owner-a", None).await;

        run_repository_gc(&storage).await;

        assert!(
            json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, shared_ref).await,
            "the second live untracked owner must retain its payload"
        );
        assert!(
            json_ref_exists(&storage, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, shared_ref,).await,
            "the candidate must survive until the last untracked owner disappears"
        );

        stage_untracked_current_owner(&storage, "gc-untracked-owner-b", None).await;

        run_repository_gc(&storage).await;

        assert!(
            !json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, shared_ref).await,
            "the final owner's payload should be reclaimed after its deletion"
        );
        assert!(
            !json_ref_exists(&storage, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, shared_ref,).await,
            "reclaiming the payload should consume the durable candidate"
        );
    }

    async fn stage_untracked_current_owner(
        storage: &Memory,
        entity_pk_value: &str,
        snapshot: Option<JsonRef>,
    ) {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("current-state owner read should open");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global control should load")
            .expect("global control should exist");
        let entity_pk = EntityPk::single(entity_pk_value);
        let snapshot_slot = snapshot.map_or(JsonSlot::None, JsonSlot::Ref);
        let timestamp =
            LixTimestamp::expect_parse("untracked GC owner timestamp", "2026-01-01T00:00:00Z");
        let mut writes = storage_adapter.new_write_set();
        let mut coverage = WorkingDiffIndexCoverage::default();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_current_state_with_working_diff(
                GLOBAL_BRANCH_ID,
                Some(control.tracked_generation),
                control.head_commit_id,
                &[CurrentStateDeltaRef {
                    schema_key: "gc_untracked_owner",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: None,
                    commit_id: None,
                    untracked: true,
                    deleted: snapshot.is_none(),
                    created_at: timestamp,
                    updated_at: timestamp,
                    snapshot: snapshot_slot.as_ref_slot(),
                    metadata: crate::json_store::JsonSlotRef::None,
                    columnar_base_coordinate: None,
                }],
                &BTreeSet::new(),
                None,
                None,
                None,
                &mut coverage,
            )
            .await
            .expect("untracked current-state owner should stage");
        stage_branch_head_control(
            &mut writes,
            GLOBAL_BRANCH_ID,
            control
                .next_current_state_revision()
                .expect("current-state revision should advance"),
        )
        .expect("current-state control should stage");
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("untracked current-state owner should commit");
    }

    async fn stage_untracked_reclaim_candidate(storage: &Memory, json_ref: JsonRef) {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        crate::json_store::JsonStoreWriter::stage_untracked_reclaim_candidates(
            &mut writes,
            [json_ref],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("reclaim candidate should commit");
    }

    async fn run_repository_gc(storage: &Memory) {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = SharedStorageAdapterRead::new(
            storage_adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("GC read should open"),
        );
        let mut writes = storage_adapter.new_write_set();
        super::stage_repository_gc_full_recovery(read, &mut writes)
            .await
            .expect("repository GC should stage");
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repository GC should commit");
    }

    async fn stage_bare_json(storage: &Memory, content: &str) -> JsonRef {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let normalized = NormalizedJson::from_arc_unchecked(Arc::from(content));
        let mut writes = storage_adapter.new_write_set();
        let json_ref = JsonStoreContext::new()
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::from(&normalized)],
            )
            .expect("bare JSON should stage")
            .pop()
            .expect("one bare JSON ref should be returned");
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("bare JSON should persist");
        json_ref
    }

    fn packed_change(change_label: &str, entity_label: &str, snapshot: JsonSlot) -> ChangeRecord {
        ChangeRecord {
            format_version: 2,
            change_id: ChangeId::for_test_label(change_label),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            entity_pk: EntityPk::single(entity_label),
            schema_key: "authority_gc".to_string(),
            file_id: None,
            snapshot,
            metadata: JsonSlot::None,
            created_at: LixTimestamp::expect_parse(
                "authority GC change timestamp",
                "2026-01-01T00:00:00.000Z",
            ),
            origin_key: None,
        }
    }

    fn gc_authority_record(label: &str) -> CommitRecord {
        CommitRecord {
            format_version: 2,
            commit_id: CommitId::for_test_label(label),
            generation: 0,
            parent_commit_ids: Vec::new(),
            change_id: ChangeId::for_test_label(&format!("{label}-header")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::expect_parse(
                "authority GC record timestamp",
                "2026-01-01T00:00:00.000Z",
            ),
        }
    }

    fn commit_delta_refs<'a>(
        commit_id: CommitId,
        changes: &'a [ChangeRecord],
    ) -> Vec<TrackedStateCommitDeltaRef<'a>> {
        changes
            .iter()
            .map(|change| TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    entity_pk: &change.entity_pk,
                    change_id: change.change_id,
                    commit_id,
                    deleted: change.snapshot.is_none(),
                    created_at: change.created_at,
                    updated_at: change.created_at,
                },
                snapshot: change.snapshot.as_ref_slot(),
                metadata: change.metadata.as_ref_slot(),
                origin_key: change.origin_key.as_deref(),
                base_coordinate: None,
                authored: true,
            })
            .collect()
    }

    fn test_commit_state_manifest(
        record: &CommitRecord,
        mutations: CommitStateMutationInventory,
    ) -> CommitStateManifest {
        CommitStateManifest {
            commit_id: record.commit_id,
            change_account_id: record.account_id.clone(),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    fn test_snapshot_root(commit_id: CommitId) -> crate::tracked_state::TrackedStateCommitRoot {
        crate::tracked_state::TrackedStateCommitRoot {
            commit_id,
            root_id: crate::tracked_state::TrackedStateRootId::new(
                *blake3::hash(commit_id.as_uuid().as_bytes()).as_bytes(),
            ),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 64,
        }
    }

    async fn json_ref_exists(storage: &Memory, space: StorageSpace, json_ref: JsonRef) -> bool {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("JSON verification read should open");
        PointReadPlan::new(
            space,
            &[StorageKey(Bytes::copy_from_slice(json_ref.as_hash_bytes()))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("JSON verification read should succeed")
        .value
        .into_iter()
        .next()
        .flatten()
        .is_some()
    }

    fn key_value_snapshot_ref(key: &str, value: &serde_json::Value) -> JsonRef {
        let snapshot = serde_json::json!({
            "key": key,
            "value": value,
        })
        .to_string();
        JsonRef::for_content(snapshot.as_bytes())
    }

    fn recovery(branch_id: &str, recovered_head: &str, checkpoint: &str) -> CheckpointRecoveryRef {
        CheckpointRecoveryRef {
            branch_id: branch_id.to_string(),
            recovered_head_commit_id: CommitId::for_test_label(recovered_head),
            checkpoint_commit_id: CommitId::for_test_label(checkpoint),
            interval_has_commits: true,
        }
    }
}
