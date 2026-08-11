//! Checkpoint recovery roots and repository garbage collection.
//!
//! Recovery refs are local, mutable roots. They deliberately live outside the
//! changelog: rotating a ref must not create history that itself keeps the
//! recovered commit alive. The checkpoint transaction stages the rotation in
//! the same storage write set that publishes the compacted checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::time::Instant;

use bytes::Bytes;

use crate::branch::{
    BranchHeadControl, BranchHeadControlContext, BranchHeadTrackedReachability,
    branch_head_control_precondition,
};
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
use crate::json_store::{JsonSlot, JsonStoreContext};
use crate::live_state::TrackedHeadContext;
#[cfg(test)]
use crate::live_state::stage_collect_stale_working_diff_indexes;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StorageGetOptions, StorageKey, StorageKeyRange, StoragePrecondition, StoragePrefix,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
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
/// Rebuildable metadata for an explicit tree-chunk sweep epoch. It is never
/// consulted as a serving root; the active root closure is re-derived from
/// branch/recovery controls and authenticated commit manifests.
pub(crate) const GC_TREE_SWEEP_EPOCH_NAMESPACE: &str = "gc.tree_sweep_epoch.v1";
pub(crate) const GC_TREE_SWEEP_EPOCH_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0005), GC_TREE_SWEEP_EPOCH_NAMESPACE);
/// One rebuildable live-chunk mark per content hash for the current epoch.
pub(crate) const GC_TREE_SWEEP_MARK_NAMESPACE: &str = "gc.tree_sweep_mark.v1";
pub(crate) const GC_TREE_SWEEP_MARK_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0006), GC_TREE_SWEEP_MARK_NAMESPACE);
/// CAS-protected bounded cursor for an in-progress tree-chunk enumeration.
pub(crate) const GC_TREE_SWEEP_CURSOR_NAMESPACE: &str = "gc.tree_sweep_cursor.v1";
pub(crate) const GC_TREE_SWEEP_CURSOR_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0007), GC_TREE_SWEEP_CURSOR_NAMESPACE);

const CHECKPOINT_RECOVERY_REF_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_GC_STATE_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_GC_STATE_KEY: &[u8] = b"repository";
const GC_REACHABILITY_FORMAT_VERSION: u32 = 2;
const GC_REACHABILITY_QUEUE_KEY: &[u8] = b"queue";
const GC_REACHABILITY_BATCH_LIMIT: usize = 64;
#[allow(dead_code)]
const GC_TREE_SWEEP_EPOCH_KEY: &[u8] = b"epoch";
#[allow(dead_code)]
const GC_TREE_SWEEP_CURSOR_KEY: &[u8] = b"cursor";
#[allow(dead_code)]
const GC_TREE_SWEEP_FORMAT_VERSION: u32 = 1;
#[allow(dead_code)]
const GC_TREE_SWEEP_PAGE_ROWS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthenticatedControlCommitReachability {
    chronology_roots: BTreeSet<CommitId>,
    serving_dependencies: BTreeSet<CommitId>,
    history_dependencies: BTreeSet<CommitId>,
}

async fn authenticated_control_commit_reachability<S>(
    store: &S,
    controls: &[(String, BranchHeadControl)],
) -> Result<AuthenticatedControlCommitReachability, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let projections = controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), control.tracked_reachability()))
        .collect::<Vec<(String, BranchHeadTrackedReachability)>>();
    let chronology_roots = projections
        .iter()
        .flat_map(|(_, projection)| projection.chronology_roots)
        .flatten()
        .collect();
    let serving_dependencies = TrackedHeadContext::new()
        .reader(store)
        .tracked_serving_commit_dependencies(&projections)
        .await?;
    let mut history_dependencies = BTreeSet::new();
    for (branch_id, projection) in &projections {
        let head = projection.chronology_roots[0].ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("branch '{branch_id}' has no authenticated history head"),
            )
        })?;
        let floor = projection.serving_checkpoint_commit_id;
        let mut graph = CommitGraphContext::new().reader(store);
        let mut current = head;
        let mut visited = BTreeSet::new();
        while Some(current) != floor {
            if !visited.insert(current) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "branch '{branch_id}' has a cycle in its retained undo interval at '{current}'"
                    ),
                ));
            }
            let node = graph.load_node(&current).await?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "branch '{branch_id}' retained undo interval references missing commit '{current}'"
                    ),
                )
            })?;
            let Some(parent) = node.parent_commit_ids.first().copied() else {
                let Some(checkpoint) = floor else { break };
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "branch '{branch_id}' retained undo interval does not reach checkpoint '{checkpoint}'"
                    ),
                ));
            };
            history_dependencies.insert(parent);
            current = parent;
        }
    }
    Ok(AuthenticatedControlCommitReachability {
        chronology_roots,
        serving_dependencies,
        history_dependencies,
    })
}

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
    old_control: Option<BranchHeadControl>,
    #[musli(with = storage_codec::option)]
    new_control: Option<BranchHeadControl>,
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

/// Authenticated metadata for one explicit tree-chunk sweep epoch. The root
/// and closure digests are evidence for resumption only; immutable manifests
/// and branch/recovery controls remain the sole logical root authority.
#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
#[allow(dead_code)]
struct StoredTreeSweepEpoch {
    format_version: u32,
    epoch_id: u64,
    queue_digest: [u8; 32],
    root_set_digest: [u8; 32],
    live_chunk_digest: [u8; 32],
    live_chunk_count: u64,
}

/// CAS-protected bounded enumeration cursor. A completed cursor is retained
/// until the next epoch so an interrupted/reopened maintenance pass can
/// distinguish a resumable epoch from a fresh one.
#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
#[allow(dead_code)]
struct StoredTreeSweepCursor {
    format_version: u32,
    epoch_id: u64,
    #[musli(with = storage_codec::option)]
    last_key: Option<Vec<u8>>,
    scanned_rows: u64,
    deleted_rows: u64,
    complete: bool,
}

/// A mark is rebuildable inventory, not payload truth. The hash in the key,
/// value, and epoch are all checked before a sweep can delete an unmarked
/// tree chunk.
#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
#[allow(dead_code)]
struct StoredTreeSweepMark {
    format_version: u32,
    epoch_id: u64,
    chunk_hash: [u8; 32],
}

/// In-memory state for one authenticated sweep epoch. The live set is loaded
/// and verified once when the session starts or reopens; ordinary queue-prefix
/// GC never consults it.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct TreeSweepEpochSession {
    epoch: StoredTreeSweepEpoch,
    cursor: StoredTreeSweepCursor,
    raw_cursor: Bytes,
    live_chunks: BTreeSet<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootReachabilityDelta {
    pub(crate) branch_id: String,
    pub(crate) old_root: Option<CommitId>,
    pub(crate) new_root: Option<CommitId>,
    pub(crate) old_control: Option<BranchHeadControl>,
    pub(crate) new_control: Option<BranchHeadControl>,
    pub(crate) old_control_digest: [u8; 32],
    pub(crate) new_control_digest: [u8; 32],
}

/// One authenticated checkpoint replacement that is still pending physical
/// retirement.
///
/// Callers receive only the typed canonical replacement. The queue remains
/// GC-owned maintenance state and never becomes a merge-time chronology
/// reader. A caller may use this proof only while publishing from the same
/// coherent read: the ordinary reachability-batch writer then CASes the exact
/// queue row observed by this resolver.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PendingCheckpointReplacement {
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) checkpoint_branch_id: String,
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

/// Retires branch-local checkpoint serving context with its canonical branch
/// control. A deleted branch must not keep recovered history or compacted
/// checkpoints live through an orphan recovery row.
pub(crate) fn stage_delete_recovery_ref(
    writes: &mut StorageWriteSet,
    branch_id: &str,
) -> Result<(), LixError> {
    writes.delete(
        CHECKPOINT_RECOVERY_REF_SPACE,
        StorageKey(Bytes::from(recovery_ref_key(branch_id)?)),
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
    control: Option<&BranchHeadControl>,
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
    // This is the central authenticated root-publication boundary. Rotate the
    // binary-CAS epoch in this same atomic write even when the transition only
    // revives an existing commit and stages no blob bytes.
    crate::binary_cas::stage_mutation_epoch(read, writes, preconditions).await?;
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
    fold_reachability_batches(
        store,
        queue,
        Some(GC_REACHABILITY_BATCH_LIMIT),
        Vec::new(),
        |batches, sequence, batch| {
            batches.push((sequence, batch));
            Ok(())
        },
    )
    .await
}

/// Authenticates and consumes a contiguous prefix of the queue in bounded
/// pages. This is the single queue-row decoder for ordinary retirement,
/// repository CAS marking, and tree-sweep root closure; the callback owns all
/// domain-specific accumulation.
///
/// For `N` selected batches this performs `O(N)` row/decoding work and keeps
/// `O(GC_REACHABILITY_BATCH_LIMIT + accumulator)` memory. `None` consumes the
/// complete queue snapshot; `Some(limit)` consumes at most that many rows.
/// The caller must bind publication to the raw queue token loaded alongside
/// `queue` when the accumulated result can authorize deletion.
async fn fold_reachability_batches<S, A, F>(
    store: &S,
    queue: &StoredReachabilityQueue,
    limit: Option<usize>,
    mut accumulator: A,
    mut consume: F,
) -> Result<A, LixError>
where
    S: StorageAdapterRead + ?Sized,
    F: FnMut(&mut A, u64, StoredRootReachabilityBatch) -> Result<(), LixError>,
{
    if queue.head_sequence == 0 || limit == Some(0) {
        return Ok(accumulator);
    }
    let queue_end = queue.tail_sequence.saturating_add(1);
    let selected_end = limit.map_or(queue_end, |limit| {
        queue
            .head_sequence
            .saturating_add(u64::try_from(limit).unwrap_or(u64::MAX))
            .min(queue_end)
    });
    let mut sequence = queue.head_sequence;
    while sequence < selected_end {
        let page_end = sequence
            .saturating_add(GC_REACHABILITY_BATCH_LIMIT as u64)
            .min(selected_end);
        let keys = (sequence..page_end)
            .map(reachability_sequence_key)
            .collect::<Vec<_>>();
        let result = PointReadPlan::new(GC_REACHABILITY_DELTA_SPACE, &keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        for (expected_sequence, value) in (sequence..page_end).zip(result.value) {
            let Some(StorageProjectedValue::FullValue(bytes)) = value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("GC reachability delta {expected_sequence} is missing"),
                ));
            };
            consume(
                &mut accumulator,
                expected_sequence,
                decode_reachability_batch(expected_sequence, &bytes)?,
            )?;
        }
        sequence = page_end;
    }
    Ok(accumulator)
}

fn decode_reachability_batch(
    expected_sequence: u64,
    bytes: &[u8],
) -> Result<StoredRootReachabilityBatch, LixError> {
    let batch: StoredRootReachabilityBatch = storage_codec::decode("GC reachability delta", bytes)?;
    if batch.format_version != GC_REACHABILITY_FORMAT_VERSION || batch.sequence != expected_sequence
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("GC reachability delta {expected_sequence} has invalid identity"),
        ));
    }
    let (_, digest) = encode_reachability_batch_body(
        batch.sequence,
        batch.deltas.clone(),
        batch.checkpoint_roots.clone(),
    )?;
    if digest != batch.digest {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("GC reachability delta {expected_sequence} digest mismatch"),
        ));
    }
    Ok(batch)
}

#[allow(dead_code)]
fn tree_sweep_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

#[allow(dead_code)]
fn tree_sweep_digest(hashes: &BTreeSet<[u8; 32]>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for hash in hashes {
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

async fn collect_all_reachability_checkpoint_roots<S>(
    store: &S,
    queue: &StoredReachabilityQueue,
) -> Result<BTreeSet<CommitId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    fold_reachability_batches(
        store,
        queue,
        None,
        BTreeSet::new(),
        |checkpoint_roots, _, batch| {
            checkpoint_roots.extend(batch.checkpoint_roots);
            Ok(())
        },
    )
    .await
}

/// Resolves a still-pending checkpoint replacement for an explicit branch
/// source, or proves that the source remains reachable through ordinary
/// canonical chronology.
///
/// A checkpoint replacement is accepted only when one authenticated pending
/// batch binds `source_commit_id -> checkpoint_commit_id` through matching
/// branch controls and includes that checkpoint in the same batch's root set.
/// Branch publication must retain this read through the ordinary reachability
/// batch writer, whose queue CAS prevents GC consumption and branch creation
/// from both committing from the same observation.
pub(crate) async fn resolve_pending_checkpoint_replacement<S>(
    store: &S,
    source_commit_id: CommitId,
) -> Result<Option<PendingCheckpointReplacement>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let (queue, _) = load_reachability_queue(store).await?;
    let candidates =
        fold_reachability_batches(store, &queue, None, Vec::new(), |candidates, _, batch| {
            let checkpoint_roots = batch
                .checkpoint_roots
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            for delta in &batch.deltas {
                validate_stored_root_reachability_delta(delta)?;
                if delta.old_root != Some(source_commit_id) {
                    continue;
                }
                let Some(checkpoint_commit_id) = delta.new_root else {
                    continue;
                };
                let Some(new_control) = delta.new_control else {
                    continue;
                };
                if new_control.working_diff_checkpoint_commit_id == Some(checkpoint_commit_id)
                    && checkpoint_roots.contains(&checkpoint_commit_id)
                {
                    candidates.push((checkpoint_commit_id, delta.branch_id.clone()));
                }
            }
            Ok(())
        })
        .await?;
    match candidates.len() {
        0 => {}
        1 => {
            let (checkpoint_commit_id, checkpoint_branch_id) = candidates
                .into_iter()
                .next()
                .expect("single checkpoint replacement candidate");
            return Ok(Some(PendingCheckpointReplacement {
                checkpoint_commit_id,
                checkpoint_branch_id,
            }));
        }
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{source_commit_id}' has ambiguous pending checkpoint replacements"
                ),
            ));
        }
    }

    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let roots = controls
        .into_iter()
        .flat_map(|(_, control)| control.tracked_reachability().chronology_roots)
        .flatten()
        .collect::<BTreeSet<_>>();
    if roots.contains(&source_commit_id) {
        return Ok(None);
    }
    let mut graph = CommitGraphContext::new().reader(store.clone());
    for root in roots {
        if graph
            .reachable_nodes(&root)
            .await?
            .iter()
            .any(|reachable| reachable.commit.commit_id == source_commit_id)
        {
            return Ok(None);
        }
    }
    Err(LixError::commit_not_found(
        source_commit_id.to_string(),
        "create_branch",
        "commit_source",
    )
    .with_hint(
        "The commit is no longer an authenticated branchable root after checkpoint compaction.",
    ))
}

#[allow(dead_code)]
async fn load_tree_sweep_root_closure<S>(
    store: &S,
) -> Result<([u8; 32], [u8; 32], Bytes, BTreeSet<[u8; 32]>), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let (queue, raw_queue) = load_reachability_queue(store).await?;
    let closure = load_authenticated_repository_retention(store, &controls, &queue).await?;
    let mut roots = BTreeSet::new();
    for manifest in closure.manifests.values() {
        if let Some(snapshot_root) = manifest.snapshot_root.as_ref() {
            roots.insert(*snapshot_root.root_id.as_bytes());
            for parent in &snapshot_root.parent_roots {
                roots.insert(*parent.root_id.as_bytes());
            }
        }
    }
    if roots.is_empty() {
        return Err(tree_sweep_error(
            "tree sweep authenticated root set has no tracked tree roots",
        ));
    }
    let root_set_digest = tree_sweep_digest(&roots);
    let typed_roots = roots
        .iter()
        .copied()
        .map(crate::tracked_state::TrackedStateRootId::new)
        .collect::<Vec<_>>();
    let live_chunks =
        crate::tracked_state::collect_reachable_tree_chunk_hashes(store, &typed_roots).await?;
    if live_chunks.is_empty() {
        return Err(tree_sweep_error(
            "tree sweep authenticated root closure is empty",
        ));
    }
    Ok((
        root_set_digest,
        tree_sweep_digest(&live_chunks),
        raw_queue,
        live_chunks,
    ))
}

#[allow(dead_code)]
async fn scan_tree_sweep_marks<S>(
    store: &S,
    expected_epoch: Option<u64>,
) -> Result<(BTreeSet<[u8; 32]>, BTreeSet<[u8; 32]>), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut previous_key = None;
    let mut current = BTreeSet::new();
    let mut all = BTreeSet::new();
    let mut cursor = store
        .begin_scan(
            GC_TREE_SWEEP_MARK_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let page = cursor.next_page(GC_TREE_SWEEP_PAGE_ROWS).await?;
        if page.has_more && page.entries.is_empty() {
            return Err(tree_sweep_error(
                "tree sweep mark scan reported has_more with an empty page",
            ));
        }
        for entry in &page.entries {
            if entry.key.0.len() != 32
                || previous_key
                    .as_ref()
                    .is_some_and(|previous: &StorageKey| entry.key <= *previous)
            {
                return Err(tree_sweep_error(
                    "tree sweep mark scan keys are malformed or non-increasing",
                ));
            }
            let key_hash: [u8; 32] = entry
                .key
                .0
                .as_ref()
                .try_into()
                .map_err(|_| tree_sweep_error("tree sweep mark key is not 32 bytes"))?;
            let StorageProjectedValue::FullValue(bytes) = &entry.value else {
                return Err(tree_sweep_error(
                    "tree sweep mark scan returned a key-only value",
                ));
            };
            let mark: StoredTreeSweepMark = storage_codec::decode("tree sweep mark", bytes)
                .map_err(|error| {
                    tree_sweep_error(format!("tree sweep mark is malformed: {error}"))
                })?;
            if mark.format_version != GC_TREE_SWEEP_FORMAT_VERSION || mark.chunk_hash != key_hash {
                return Err(tree_sweep_error(
                    "tree sweep mark identity or format is invalid",
                ));
            }
            all.insert(key_hash);
            if expected_epoch.is_none_or(|epoch| mark.epoch_id == epoch) {
                current.insert(key_hash);
            }
            previous_key = Some(entry.key.clone());
        }
        if page.entries.is_empty() {
            if page.has_more {
                return Err(tree_sweep_error(
                    "tree sweep mark scan has_more without a resume key",
                ));
            }
            break;
        }
        if !page.has_more {
            break;
        }
    }
    Ok((current, all))
}

#[allow(dead_code)]
async fn load_optional_tree_sweep_row<S, T>(
    store: &S,
    space: StorageSpace,
    key: &'static [u8],
    label: &'static str,
) -> Result<Option<(T, Bytes)>, LixError>
where
    S: StorageAdapterRead + ?Sized,
    T: for<'de> musli::Decode<'de, musli::mode::Binary, musli::alloc::Global>,
{
    let key = StorageKey(Bytes::from_static(key));
    let value = PointReadPlan::new(space, std::slice::from_ref(&key))
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(StorageProjectedValue::FullValue(bytes)) = value else {
        return Ok(None);
    };
    let decoded = storage_codec::decode(label, &bytes)?;
    Ok(Some((decoded, bytes)))
}

/// Starts a new authenticated tree sweep epoch. The expensive root closure
/// and mark inventory are built once here; ordinary queue-prefix GC never
/// calls this function.
#[allow(dead_code)]
pub(crate) async fn begin_tree_sweep_epoch<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<TreeSweepEpochSession, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let old_epoch = load_optional_tree_sweep_row::<S, StoredTreeSweepEpoch>(
        store,
        GC_TREE_SWEEP_EPOCH_SPACE,
        GC_TREE_SWEEP_EPOCH_KEY,
        "tree sweep epoch",
    )
    .await?;
    let old_cursor = load_optional_tree_sweep_row::<S, StoredTreeSweepCursor>(
        store,
        GC_TREE_SWEEP_CURSOR_SPACE,
        GC_TREE_SWEEP_CURSOR_KEY,
        "tree sweep cursor",
    )
    .await?;
    if old_epoch.is_some() != old_cursor.is_some() {
        return Err(tree_sweep_error(
            "tree sweep epoch and cursor lifecycle rows disagree",
        ));
    }
    if old_cursor
        .as_ref()
        .is_some_and(|(cursor, _)| !cursor.complete)
    {
        return Err(tree_sweep_error(
            "tree sweep epoch is already active; reopen it instead of creating a second epoch",
        ));
    }
    let (root_set_digest, live_chunk_digest, raw_queue, live_chunks) =
        load_tree_sweep_root_closure(store).await?;
    let epoch_id = old_epoch
        .as_ref()
        .map_or(1, |(epoch, _)| epoch.epoch_id.saturating_add(1));
    let epoch = StoredTreeSweepEpoch {
        format_version: GC_TREE_SWEEP_FORMAT_VERSION,
        epoch_id,
        queue_digest: *blake3::hash(&raw_queue).as_bytes(),
        root_set_digest,
        live_chunk_digest,
        live_chunk_count: live_chunks.len() as u64,
    };
    let cursor = StoredTreeSweepCursor {
        format_version: GC_TREE_SWEEP_FORMAT_VERSION,
        epoch_id,
        last_key: None,
        scanned_rows: 0,
        deleted_rows: 0,
        complete: false,
    };
    let (current_marks, all_marks) = scan_tree_sweep_marks(store, None).await?;
    let _ = current_marks;
    for hash in all_marks.difference(&live_chunks) {
        writes.delete(
            GC_TREE_SWEEP_MARK_SPACE,
            StorageKey(Bytes::copy_from_slice(hash)),
        );
    }
    for hash in &live_chunks {
        let mark = StoredTreeSweepMark {
            format_version: GC_TREE_SWEEP_FORMAT_VERSION,
            epoch_id,
            chunk_hash: *hash,
        };
        writes.put(
            GC_TREE_SWEEP_MARK_SPACE,
            StorageKey(Bytes::copy_from_slice(hash)),
            StorageValue {
                bytes: Bytes::from(storage_codec::encode("tree sweep mark", &mark)?),
            },
        );
    }
    let epoch_bytes = Bytes::from(storage_codec::encode("tree sweep epoch", &epoch)?);
    let cursor_bytes = Bytes::from(storage_codec::encode("tree sweep cursor", &cursor)?);
    writes.put(
        GC_TREE_SWEEP_EPOCH_SPACE,
        StorageKey(Bytes::from_static(GC_TREE_SWEEP_EPOCH_KEY)),
        StorageValue { bytes: epoch_bytes },
    );
    writes.put(
        GC_TREE_SWEEP_CURSOR_SPACE,
        StorageKey(Bytes::from_static(GC_TREE_SWEEP_CURSOR_KEY)),
        StorageValue {
            bytes: cursor_bytes.clone(),
        },
    );
    preconditions.push(StoragePrecondition::KeyValueEquals {
        space: GC_REACHABILITY_QUEUE_SPACE,
        key: StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        expected: raw_queue,
    });
    if let Some((_, raw)) = old_epoch {
        preconditions.push(StoragePrecondition::KeyValueEquals {
            space: GC_TREE_SWEEP_EPOCH_SPACE,
            key: StorageKey(Bytes::from_static(GC_TREE_SWEEP_EPOCH_KEY)),
            expected: raw,
        });
    } else {
        preconditions.push(StoragePrecondition::KeyAbsent {
            space: GC_TREE_SWEEP_EPOCH_SPACE,
            key: StorageKey(Bytes::from_static(GC_TREE_SWEEP_EPOCH_KEY)),
        });
    }
    if let Some((_, raw)) = old_cursor {
        preconditions.push(StoragePrecondition::KeyValueEquals {
            space: GC_TREE_SWEEP_CURSOR_SPACE,
            key: StorageKey(Bytes::from_static(GC_TREE_SWEEP_CURSOR_KEY)),
            expected: raw,
        });
    } else {
        preconditions.push(StoragePrecondition::KeyAbsent {
            space: GC_TREE_SWEEP_CURSOR_SPACE,
            key: StorageKey(Bytes::from_static(GC_TREE_SWEEP_CURSOR_KEY)),
        });
    }
    Ok(TreeSweepEpochSession {
        epoch,
        cursor,
        raw_cursor: cursor_bytes,
        live_chunks,
    })
}

/// Reopens an active or completed epoch. Mark rows are fully authenticated
/// against the persisted count/digest before any page may stage a delete.
#[allow(dead_code)]
pub(crate) async fn open_tree_sweep_epoch<S>(
    store: &S,
) -> Result<Option<TreeSweepEpochSession>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let Some((epoch, _)) = load_optional_tree_sweep_row::<S, StoredTreeSweepEpoch>(
        store,
        GC_TREE_SWEEP_EPOCH_SPACE,
        GC_TREE_SWEEP_EPOCH_KEY,
        "tree sweep epoch",
    )
    .await?
    else {
        return Ok(None);
    };
    if epoch.format_version != GC_TREE_SWEEP_FORMAT_VERSION {
        return Err(tree_sweep_error("tree sweep epoch format is unsupported"));
    }
    let Some((cursor, raw_cursor)) = load_optional_tree_sweep_row::<S, StoredTreeSweepCursor>(
        store,
        GC_TREE_SWEEP_CURSOR_SPACE,
        GC_TREE_SWEEP_CURSOR_KEY,
        "tree sweep cursor",
    )
    .await?
    else {
        return Err(tree_sweep_error("tree sweep cursor is missing"));
    };
    if cursor.format_version != GC_TREE_SWEEP_FORMAT_VERSION || cursor.epoch_id != epoch.epoch_id {
        return Err(tree_sweep_error("tree sweep cursor identity is invalid"));
    }
    let (live_chunks, all_marks) = scan_tree_sweep_marks(store, Some(epoch.epoch_id)).await?;
    if live_chunks != all_marks
        || live_chunks.len() as u64 != epoch.live_chunk_count
        || tree_sweep_digest(&live_chunks) != epoch.live_chunk_digest
    {
        return Err(tree_sweep_error(
            "tree sweep mark inventory disagrees with epoch closure or contains stale rows",
        ));
    }
    Ok(Some(TreeSweepEpochSession {
        epoch,
        cursor,
        raw_cursor,
        live_chunks,
    }))
}

/// Enumerates one bounded tree-chunk page and stages only chunks absent from
/// the authenticated epoch mark set. Every key/value and cursor transition is
/// validated before staging; the queue and cursor CAS fences make interruption
/// and root publication races fail closed.
#[allow(dead_code)]
pub(crate) async fn stage_tree_sweep_epoch_page<S>(
    store: &S,
    session: &mut TreeSweepEpochSession,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if session.cursor.complete {
        return Ok(true);
    }
    let (queue, raw_queue) = load_reachability_queue(store).await?;
    if *blake3::hash(&raw_queue).as_bytes() != session.epoch.queue_digest {
        return Err(tree_sweep_error(
            "tree sweep root publication advanced during an active epoch",
        ));
    }
    let range = StorageKeyRange {
        lower: session
            .cursor
            .last_key
            .as_ref()
            .map_or(Bound::Unbounded, |key| {
                Bound::Excluded(StorageKey(Bytes::copy_from_slice(key)))
            }),
        upper: Bound::Unbounded,
    };
    let mut cursor = store
        .begin_scan(
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let page = cursor.next_page(GC_TREE_SWEEP_PAGE_ROWS).await?;
    if page.has_more && page.entries.is_empty() {
        return Err(tree_sweep_error(
            "tree sweep tree-chunk scan reported has_more with an empty page",
        ));
    }
    let previous = session.cursor.last_key.as_deref();
    let mut hashes = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        if entry.key.0.len() != 32
            || previous.is_some_and(|previous| entry.key.0.as_ref() <= previous)
            || hashes
                .last()
                .is_some_and(|last: &[u8; 32]| entry.key.0.as_ref() <= last.as_slice())
        {
            return Err(tree_sweep_error(
                "tree sweep tree-chunk keys are malformed or non-increasing",
            ));
        }
        let hash: [u8; 32] = entry
            .key
            .0
            .as_ref()
            .try_into()
            .map_err(|_| tree_sweep_error("tree sweep tree-chunk key is not 32 bytes"))?;
        let StorageProjectedValue::FullValue(bytes) = &entry.value else {
            return Err(tree_sweep_error(
                "tree sweep tree-chunk scan returned a key-only value",
            ));
        };
        if *blake3::hash(bytes).as_bytes() != hash {
            return Err(tree_sweep_error(
                "tree sweep tree-chunk value hash disagrees with its key",
            ));
        }
        hashes.push(hash);
    }
    let mark_keys = hashes
        .iter()
        .map(|hash| StorageKey(Bytes::copy_from_slice(hash)))
        .collect::<Vec<_>>();
    let marks = if mark_keys.is_empty() {
        Vec::new()
    } else {
        PointReadPlan::new(GC_TREE_SWEEP_MARK_SPACE, &mark_keys)
            .materialize(store, StorageGetOptions::default())
            .await?
            .value
    };
    if marks.len() != hashes.len() {
        return Err(tree_sweep_error(
            "tree sweep mark point read returned the wrong cardinality",
        ));
    }
    let mut deletes = Vec::new();
    for (hash, mark) in hashes.iter().zip(marks) {
        let marked = match mark {
            None => false,
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(tree_sweep_error(
                    "tree sweep mark point read returned a key-only value",
                ));
            }
            Some(StorageProjectedValue::FullValue(bytes)) => {
                let mark: StoredTreeSweepMark = storage_codec::decode("tree sweep mark", &bytes)
                    .map_err(|error| {
                        tree_sweep_error(format!("tree sweep mark is malformed: {error}"))
                    })?;
                if mark.format_version != GC_TREE_SWEEP_FORMAT_VERSION
                    || mark.epoch_id != session.epoch.epoch_id
                    || mark.chunk_hash != *hash
                {
                    return Err(tree_sweep_error(
                        "tree sweep mark does not authenticate the current epoch chunk",
                    ));
                }
                true
            }
        };
        if marked != session.live_chunks.contains(hash) {
            return Err(tree_sweep_error(
                "tree sweep mark completeness disagrees with authenticated closure",
            ));
        }
        if !marked {
            deletes.push(*hash);
        }
    }

    // A complete keyspace scan must prove that every authenticated live mark
    // still has its physical chunk before any delete is staged.  This catches
    // a live chunk deleted between epoch creation and the final page (the
    // mark inventory alone cannot distinguish that from a clean sweep).
    if !page.has_more {
        let live_keys = session
            .live_chunks
            .iter()
            .map(|hash| StorageKey(Bytes::copy_from_slice(hash)))
            .collect::<Vec<_>>();
        for chunk_keys in live_keys.chunks(GC_TREE_SWEEP_PAGE_ROWS) {
            let values = PointReadPlan::new(
                crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                chunk_keys,
            )
            .materialize(store, StorageGetOptions::default())
            .await?
            .value;
            if values.len() != chunk_keys.len() {
                return Err(tree_sweep_error(
                    "tree sweep final live-chunk validation returned the wrong cardinality",
                ));
            }
            for (key, value) in chunk_keys.iter().zip(values) {
                let Some(StorageProjectedValue::FullValue(bytes)) = value else {
                    return Err(tree_sweep_error(
                        "tree sweep final live-chunk validation found a missing or key-only chunk",
                    ));
                };
                if blake3::hash(&bytes).as_bytes().as_slice() != key.0.as_ref() {
                    return Err(tree_sweep_error(
                        "tree sweep final live-chunk validation found a corrupt chunk",
                    ));
                }
            }
        }
    }

    for hash in &deletes {
        writes.delete(
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(hash)),
        );
    }
    let next_cursor = StoredTreeSweepCursor {
        format_version: GC_TREE_SWEEP_FORMAT_VERSION,
        epoch_id: session.epoch.epoch_id,
        last_key: page.entries.last().map(|entry| entry.key.0.to_vec()),
        scanned_rows: session
            .cursor
            .scanned_rows
            .saturating_add(hashes.len() as u64),
        deleted_rows: session
            .cursor
            .deleted_rows
            .saturating_add(deletes.len() as u64),
        complete: !page.has_more,
    };
    let next_raw = Bytes::from(storage_codec::encode("tree sweep cursor", &next_cursor)?);
    writes.put(
        GC_TREE_SWEEP_CURSOR_SPACE,
        StorageKey(Bytes::from_static(GC_TREE_SWEEP_CURSOR_KEY)),
        StorageValue {
            bytes: next_raw.clone(),
        },
    );
    preconditions.push(StoragePrecondition::KeyValueEquals {
        space: GC_REACHABILITY_QUEUE_SPACE,
        key: StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        expected: raw_queue,
    });
    preconditions.push(StoragePrecondition::KeyValueEquals {
        space: GC_TREE_SWEEP_CURSOR_SPACE,
        key: StorageKey(Bytes::from_static(GC_TREE_SWEEP_CURSOR_KEY)),
        expected: session.raw_cursor.clone(),
    });
    session.cursor = next_cursor;
    session.raw_cursor = next_raw;
    let _ = queue;
    Ok(session.cursor.complete)
}

/// Returns the exact standalone semantic facts and the authenticated reason
/// currently known for each one. This is benchmark-only attribution: the
/// ordinary collector never scans CHANGE_SPACE, and this helper is called
/// outside the measured planner phase.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) async fn audit_repository_gc_standalone_refs<S>(
    store: &S,
) -> Result<Vec<String>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let controls = BranchHeadControlContext::new().reader(store).scan().await?;
    let active_refs = controls
        .iter()
        .map(|(_, control)| control.ref_change_id)
        .collect::<BTreeSet<_>>();
    let (queue, _) = load_reachability_queue(store).await?;
    let batches = load_reachability_batches(store, &queue).await?;
    let closure = load_authenticated_repository_retention(store, &controls, &queue).await?;
    let active_root_ids = closure.chronology_roots;
    let active_dependency_ids = closure
        .physical_authorities
        .union(&closure.physical_dependencies)
        .copied()
        .chain(closure.semantic_dependencies.iter().copied())
        .collect::<BTreeSet<_>>();
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
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut refs = BTreeMap::new();
    let mut cursor = store
        .begin_scan(
            CHECKPOINT_RECOVERY_REF_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    loop {
        let page = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?;
        for entry in page.entries {
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
        if !page.has_more {
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
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = store
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let page = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?;
        for entry in page.entries {
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
        if !page.has_more {
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

/// Returns the bounded checkpoint parent consumed by the first ordinary
/// commit on a branch created from a recovered historical head.
///
/// The recovery row and branch control are serving context only. Once this
/// returns, commit publication records the checkpoint as an ordinary graph
/// parent; merge/history readers never consult either serving record.
pub(crate) async fn resolve_checkpoint_branch_parent(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    head_commit_id: CommitId,
    working_diff_checkpoint_commit_id: Option<CommitId>,
) -> Result<Option<CommitId>, LixError> {
    let Some(recovery) = load_recovery_ref(store, branch_id).await? else {
        return Ok(None);
    };
    if recovery.recovered_head_commit_id != head_commit_id {
        return Ok(None);
    }
    if !recovery.interval_has_commits
        || recovery.checkpoint_commit_id == head_commit_id
        || working_diff_checkpoint_commit_id != Some(recovery.checkpoint_commit_id)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("branch '{branch_id}' has an invalid pending checkpoint ancestry bridge"),
        ));
    }
    Ok(Some(recovery.checkpoint_commit_id))
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
    pub(crate) binary_cas: crate::binary_cas::BinaryCasGcSweep,
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

/// Extends the existing retirement proof with every commit the point reader
/// can still visit from an active logical root.
///
/// Semantic first-parent edges come only from the commit graph. A replacement
/// generation may substitute its authenticated physical fallback, exactly as
/// `TrackedStateStoreReader::point_replay_interval` does. Physical serving
/// owners are retained directly by their authenticated inventory and do not
/// acquire point-replay chronology merely because their original manifest had
/// replay debt. Malformed debt, missing state, or a cycle aborts the complete
/// GC transaction before any sweep is staged.
async fn collect_active_point_replay_dependencies<S>(
    store: &S,
    active_manifests: &BTreeMap<CommitId, crate::tracked_state::CommitStateManifest>,
    logical_start_ids: &BTreeSet<CommitId>,
    physical_dependencies: &mut BTreeSet<CommitId>,
    semantic_dependencies: &mut BTreeSet<CommitId>,
    cas_logical_dependencies: &mut BTreeSet<CommitId>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let mut graph = CommitGraphContext::new().reader(store.clone());
    let mut completed = BTreeSet::new();
    let mut loaded_manifests = active_manifests.clone();
    let mut discovered_physical = BTreeSet::new();
    let mut discovered_semantic = BTreeSet::new();
    let mut discovered_cas_logical = BTreeSet::new();

    for (start_commit_id, start_manifest) in active_manifests {
        if start_manifest.replay_debt.depth == 0 || !logical_start_ids.contains(start_commit_id) {
            continue;
        }
        let start_commit_id = *start_commit_id;
        discovered_semantic.insert(start_commit_id);
        let mut current_commit_id = start_commit_id;
        let mut path = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            if completed.contains(&current_commit_id) {
                break;
            }
            if !seen.insert(current_commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "active GC point-replay dependency cycle from '{start_commit_id}' includes '{current_commit_id}'"
                    ),
                ));
            }
            path.push(current_commit_id);

            let node = graph
                .load_node(&current_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "active GC point-replay commit '{current_commit_id}' has no authenticated graph node"
                        ),
                    )
                })?;
            let manifest = match loaded_manifests.get(&current_commit_id) {
                Some(manifest) => manifest.clone(),
                None => {
                    let manifest = crate::tracked_state::load_commit_state_manifest(
                        store,
                        current_commit_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "active GC point-replay commit '{current_commit_id}' has no authenticated physical manifest"
                            ),
                        )
                    })?;
                    loaded_manifests.insert(current_commit_id, manifest.clone());
                    manifest
                }
            };
            if manifest.replay_debt.depth == 0 {
                break;
            }

            let replacement_fallback = manifest
                .mutations
                .replacement_generation
                .as_ref()
                .filter(|_| manifest.mutations.member_count != 0)
                .map(|generation| {
                    generation
                        .fallback_commit_id
                        .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes)))
                });
            let uses_replacement_fallback = replacement_fallback.is_some();
            if uses_replacement_fallback && manifest.replay_debt.depth != 1 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "active GC replacement replay commit '{current_commit_id}' has malformed replay debt"
                    ),
                ));
            }
            let next_commit_id =
                replacement_fallback.unwrap_or_else(|| node.parent_commit_ids.first().copied());
            let Some(next_commit_id) = next_commit_id else {
                if manifest.replay_debt.depth != 1 {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "active GC point-replay commit '{current_commit_id}' has unresolved replay debt"
                        ),
                    ));
                }
                break;
            };

            let next_manifest = match loaded_manifests.get(&next_commit_id) {
                Some(manifest) => manifest.clone(),
                None => {
                    let manifest = crate::tracked_state::load_commit_state_manifest(
                        store,
                        next_commit_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "active GC point-replay dependency '{next_commit_id}' has no authenticated physical manifest"
                            ),
                        )
                    })?;
                    loaded_manifests.insert(next_commit_id, manifest.clone());
                    manifest
                }
            };
            if !uses_replacement_fallback
                && next_manifest.replay_debt.depth + 1 != manifest.replay_debt.depth
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "active GC point-replay debt disagrees across '{current_commit_id}' -> '{next_commit_id}'"
                    ),
                ));
            }

            discovered_physical.insert(next_commit_id);
            discovered_semantic.insert(next_commit_id);
            discovered_cas_logical.insert(next_commit_id);
            current_commit_id = next_commit_id;
        }
        completed.extend(path);
    }
    physical_dependencies.extend(discovered_physical);
    semantic_dependencies.extend(discovered_semantic);
    cas_logical_dependencies.extend(discovered_cas_logical);
    Ok(())
}

/// One authenticated retained-root closure shared by every GC planner and
/// audit consumer. Chronology remains owned by branch controls, recovery refs,
/// and checkpoint pins. The remaining sets are derived serving authorities,
/// never additional roots.
#[derive(Debug)]
struct AuthenticatedServingDependencyClosure {
    chronology_roots: BTreeSet<CommitId>,
    physical_authorities: BTreeSet<CommitId>,
    physical_dependencies: BTreeSet<CommitId>,
    semantic_dependencies: BTreeSet<CommitId>,
    cas_logical_dependencies: BTreeSet<CommitId>,
    manifests: BTreeMap<CommitId, crate::tracked_state::CommitStateManifest>,
    mutation_nodes: BTreeSet<[u8; 32]>,
    scoped_nodes: BTreeSet<[u8; 32]>,
    native_parts: BTreeSet<[u8; 32]>,
}

async fn load_authenticated_serving_dependency_closure<S>(
    store: &S,
    chronology_roots: BTreeSet<CommitId>,
    serving_dependencies: BTreeSet<CommitId>,
    history_dependencies: BTreeSet<CommitId>,
) -> Result<AuthenticatedServingDependencyClosure, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    if chronology_roots.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "authenticated GC active-root set is empty",
        ));
    }

    let replay_start_ids = chronology_roots
        .iter()
        .chain(serving_dependencies.iter())
        .chain(history_dependencies.iter())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut physical_authorities = chronology_roots.clone();
    let mut physical_dependencies = serving_dependencies.clone();
    physical_dependencies.extend(history_dependencies.iter().copied());
    let mut semantic_dependencies = serving_dependencies;
    semantic_dependencies.extend(history_dependencies.iter().copied());
    let mut cas_logical_dependencies = history_dependencies;
    let mut manifests = BTreeMap::new();
    let mut pending = chronology_roots.iter().copied().collect::<Vec<_>>();
    while let Some(commit_id) = pending.pop() {
        if manifests.contains_key(&commit_id) {
            continue;
        }
        let manifest = crate::tracked_state::load_commit_state_manifest(store, commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "active GC root or authority '{commit_id}' has no authenticated physical manifest"
                    ),
                )
            })?;
        if let Some(source) = manifest.mutations.selected_source_commit_id() {
            physical_authorities.insert(source);
            pending.push(source);
        }
        if let Some(root) = manifest.current_state_scoped_ranges.as_ref() {
            if let Some(base) = root.serving_base_commit_id {
                physical_dependencies.insert(base);
            }
        }
        if let Some(snapshot_root) = manifest.snapshot_root.as_ref() {
            physical_dependencies.extend(
                snapshot_root
                    .parent_roots
                    .iter()
                    .map(|parent| parent.commit_id),
            );
        }
        manifests.insert(commit_id, manifest);
    }

    let mut scoped_nodes = BTreeSet::new();
    let mut native_parts = BTreeSet::new();
    let scoped_roots = manifests
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
            crate::tracked_state::validate_scoped_range_trees(store, &scoped_roots).await?;
        scoped_nodes.extend(reachable.node_ids);
        for part in reachable.parts {
            let descriptor =
                crate::tracked_state::current_state_descriptor_from_scoped_range_part(&part)?;
            match descriptor.source_kind {
                0 | 2 => {
                    physical_authorities.insert(CommitId::new(uuid::Uuid::from_bytes(
                        descriptor.owner_commit_id,
                    )));
                }
                1 => {
                    native_parts.insert(descriptor.content_digest);
                }
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "active current-state scoped range has an unknown source kind",
                    ));
                }
            }
        }
    }
    let native_commit_dependencies = validate_live_native_parts(store, &native_parts).await?;
    physical_dependencies.extend(native_commit_dependencies.iter().copied());
    semantic_dependencies.extend(native_commit_dependencies);

    let missing_dependency_ids = physical_dependencies
        .iter()
        .filter(|commit_id| !manifests.contains_key(commit_id))
        .copied()
        .collect::<Vec<_>>();
    let missing_dependency_manifests =
        crate::tracked_state::load_commit_state_manifests(store, &missing_dependency_ids).await?;
    for (commit_id, manifest) in missing_dependency_ids
        .into_iter()
        .zip(missing_dependency_manifests)
    {
        let manifest = manifest.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "active GC serving dependency '{commit_id}' has no authenticated physical manifest"
                ),
            )
        })?;
        manifests.insert(commit_id, manifest);
    }
    collect_active_point_replay_dependencies(
        store,
        &manifests,
        &replay_start_ids,
        &mut physical_dependencies,
        &mut semantic_dependencies,
        &mut cas_logical_dependencies,
    )
    .await?;
    semantic_dependencies.extend(cas_logical_dependencies.iter().copied());

    let selected_owner_sources = physical_authorities
        .union(&physical_dependencies)
        .copied()
        .collect::<Vec<_>>();
    let selected_owner_source_manifests =
        crate::tracked_state::load_commit_state_manifests(store, &selected_owner_sources).await?;
    for (commit_id, manifest) in selected_owner_sources
        .iter()
        .copied()
        .zip(selected_owner_source_manifests)
    {
        let manifest = manifest.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "active GC dependency '{commit_id}' has no authenticated physical manifest"
                ),
            )
        })?;
        let may_contain_finite_selected_members =
            manifest.mutations.may_contain_finite_selected_members();
        manifests.insert(commit_id, manifest);
        if may_contain_finite_selected_members {
            physical_authorities.extend(
                crate::tracked_state::load_local_selected_change_owner_commit_ids(store, commit_id)
                    .await?,
            );
        }
    }

    let retained_physical_ids = physical_authorities
        .union(&physical_dependencies)
        .copied()
        .collect::<Vec<_>>();
    let retained_physical_manifests =
        crate::tracked_state::load_commit_state_manifests(store, &retained_physical_ids).await?;
    for (commit_id, manifest) in retained_physical_ids
        .iter()
        .copied()
        .zip(retained_physical_manifests)
    {
        let manifest = manifest.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "active GC dependency '{commit_id}' has no authenticated physical manifest"
                ),
            )
        })?;
        manifests.insert(commit_id, manifest);
    }

    let mut mutation_nodes = BTreeSet::new();
    let mutation_roots =
        crate::tracked_state::load_commit_mutation_directory_roots(store, &retained_physical_ids)
            .await?;
    for root in mutation_roots.into_iter().flatten() {
        mutation_nodes
            .extend(crate::tracked_state::collect_mutation_directory_node_ids(store, &root).await?);
    }

    Ok(AuthenticatedServingDependencyClosure {
        chronology_roots,
        physical_authorities,
        physical_dependencies,
        semantic_dependencies,
        cas_logical_dependencies,
        manifests,
        mutation_nodes,
        scoped_nodes,
        native_parts,
    })
}

async fn load_authenticated_repository_retention<S>(
    store: &S,
    controls: &[(String, BranchHeadControl)],
    queue: &StoredReachabilityQueue,
) -> Result<AuthenticatedServingDependencyClosure, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let control_reachability = authenticated_control_commit_reachability(store, controls).await?;
    let mut chronology_roots = control_reachability.chronology_roots;
    chronology_roots.extend(
        load_recovery_refs(store)
            .await?
            .into_iter()
            .flat_map(|recovery| {
                [
                    recovery.recovered_head_commit_id,
                    recovery.checkpoint_commit_id,
                ]
            }),
    );
    chronology_roots.extend(collect_all_reachability_checkpoint_roots(store, queue).await?);
    load_authenticated_serving_dependency_closure(
        store,
        chronology_roots,
        control_reachability.serving_dependencies,
        control_reachability.history_dependencies,
    )
    .await
}

/// Plans and stages logical repository GC against one pinned read.
///
/// The caller must serialize this operation with repository writes and commit
/// `writes` atomically. Planning and mutation are deliberately separated from
/// storage commit so checkpoint/session code can retain lifecycle control.
/// Content-addressed tree/CAS orphan repair is intentionally an offline path;
/// out-of-band JSON is reclaimed here only from explicit ownership-loss
/// candidates.
/// Ordinary GC consumes only authenticated publication deltas. Branch-head
/// controls and checkpoint recovery refs are the complete active-root set;
/// the only semantic walk permitted here is the exact point-replay dependency
/// closure of those roots. Full-space inventory discovery remains forbidden.
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
    let mut staged_preconditions = Vec::new();
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let branch_ids = controls
        .iter()
        .map(|(branch_id, _)| branch_id.clone())
        .collect::<Vec<_>>();
    let observed_controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .load_observed(&branch_ids)
        .await?;
    for ((branch_id, control), observed) in controls.iter().zip(observed_controls) {
        if observed.control != Some(*control) {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("branch '{branch_id}' changed while GC roots were being observed"),
            ));
        }
        staged_preconditions.push(branch_head_control_precondition(
            branch_id,
            observed.raw_token,
        )?);
    }
    let (queue, raw_queue) = load_reachability_queue(&store).await?;
    // Every repository sweep publishes against the exact queue snapshot used
    // for root discovery, even when the bounded retirement window is empty or
    // blocked and no queue row is consumed.
    staged_preconditions.push(StoragePrecondition::KeyValueEquals {
        space: GC_REACHABILITY_QUEUE_SPACE,
        key: StorageKey(Bytes::from_static(GC_REACHABILITY_QUEUE_KEY)),
        expected: raw_queue,
    });
    let batches = load_reachability_batches(&store, &queue).await?;
    let AuthenticatedServingDependencyClosure {
        chronology_roots: active_roots,
        physical_authorities: active_authority_ids,
        physical_dependencies: active_dependency_ids,
        semantic_dependencies: active_semantic_dependency_ids,
        cas_logical_dependencies: active_cas_dependency_ids,
        manifests: _,
        mutation_nodes: active_mutation_nodes,
        scoped_nodes: active_scoped_nodes,
        native_parts: active_current_parts,
    } = load_authenticated_repository_retention(&store, &controls, &queue).await?;

    // Derive both physical retirement and logical CAS retention from the one
    // authenticated serving closure. In particular, do not perform a second
    // replay-graph walk for CAS: a queue-old root is retained exactly when it
    // is already a dependency of the active closure. This makes the retained
    // owner set deterministic and prevents a separately reconstructed CAS
    // authority from racing semantic projection retirement.
    let mut blocked_sequences = BTreeSet::new();
    let mut blocked_physical_dependency_ids = BTreeSet::new();
    let mut blocked_history_dependency_ids = BTreeSet::new();
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
            ) {
                blocked_sequences.insert(*sequence);
                blocked_physical_dependency_ids.insert(old_root);
            }
            if !retirement_is_proven(
                old_root,
                delta.new_root,
                &active_roots,
                &active_cas_dependency_ids,
            ) {
                blocked_history_dependency_ids.insert(old_root);
            }
        }
    }

    // Physical selected-source/current-base owners remain manifest authority,
    // while CAS marking receives only logical chronology and authenticated
    // history/undo/replay dependencies. Feeding physical selected sources to
    // CAS directly would resurrect rows masked by the logical retained root.
    let retained_root_ids = active_authority_ids
        .union(&active_dependency_ids)
        .copied()
        .chain(blocked_physical_dependency_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let retained_cas_root_ids = active_roots
        .iter()
        .copied()
        .chain(active_cas_dependency_ids.iter().copied())
        .chain(blocked_history_dependency_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let mut blob_roots =
        crate::filesystem::collect_gc_binary_blob_roots(&store, &controls, &retained_cas_root_ids)
            .await?;
    blob_roots.extend(
        crate::plugin::collect_gc_wasm_blob_roots(&store, &controls, &retained_cas_root_ids)
            .await?,
    );
    let upload_chunks =
        crate::session::stage_reclaimable_upload_receipts(&store, writes, &blob_roots).await?;
    let binary_cas =
        crate::binary_cas::stage_gc_reclamation(&store, writes, &blob_roots, &upload_chunks)
            .await?;
    crate::binary_cas::stage_mutation_epoch(&store, writes, &mut staged_preconditions).await?;

    if batches.is_empty() {
        preconditions.extend(staged_preconditions);
        return Ok(RepositoryGcPlan {
            changelog: GcPlan {
                roots: active_roots
                    .iter()
                    .copied()
                    .map(GcRoot::BranchHead)
                    .collect(),
                live: GcLiveSet {
                    commits: retained_root_ids.into_iter().collect(),
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
                binary_cas,
            },
            profile: RepositoryGcProfile {
                root_discovery_us: elapsed_micros(started),
                changelog_us: 0,
                tracked_root_stage_us: 0,
                total_us: elapsed_micros(started),
            },
        });
    }

    // A delta is a candidate until every active history/checkpoint/replay
    // dependency releases the old root.  Never consume a batch containing a
    // blocked candidate: dropping that signal would make the root permanently
    // unreclaimable after the pin is later released.  The whole batch remains
    // at the queue head; this intentionally delays later deltas but preserves
    // the authenticated publication order and retry semantics.
    let mut next_queue = queue;
    let queue_head = next_queue.head_sequence;
    let mut consumed_through = queue_head;
    let mut queue_open = true;
    let mut reclaimed_commits = BTreeSet::new();
    let mut reclaimed_semantic_commits = BTreeSet::new();
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
            );
            let semantic_retirement = retirement_is_proven(
                old_root,
                delta.new_root,
                &active_roots,
                &active_semantic_dependency_ids,
            );
            if semantic_retirement && reclaimed_semantic_commits.insert(old_root) {
                stage_delete_semantic_commit_projection(&store, writes, old_root).await?;
            }
            if !physical_retirement {
                continue;
            }
            if reclaimed_commits.insert(old_root) {
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
            }
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
        writes.seal_changelog_gc();
    }

    preconditions.extend(staged_preconditions);
    Ok(RepositoryGcPlan {
        changelog: GcPlan {
            roots: active_roots
                .iter()
                .copied()
                .map(GcRoot::BranchHead)
                .collect(),
            live: GcLiveSet {
                commits: retained_root_ids.into_iter().collect(),
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
            tracked_commit_roots: reclaimed_commits.into_iter().collect(),
            standalone_changes: reclaimed_standalone_changes.into_iter().collect(),
            binary_cas,
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
) -> Result<BTreeSet<CommitId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if digests.is_empty() {
        return Ok(BTreeSet::new());
    }
    let keys = digests
        .iter()
        .map(|digest| StorageKey(Bytes::copy_from_slice(digest)))
        .collect::<Vec<_>>();
    let loaded = PointReadPlan::new(crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut commit_ids = BTreeSet::new();
    for (digest, value) in digests.iter().zip(loaded.value) {
        let Some(StorageProjectedValue::FullValue(bytes)) = value else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "live current-state directory references a missing native data part",
            ));
        };
        commit_ids.extend(
            crate::tracked_state::decode_current_state_data_part_commit_ids(digest, &bytes)?,
        );
    }
    Ok(commit_ids)
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
    // state plane. Read them once so root discovery and derived-generation
    // sweeping use exactly the same pinned publication view.
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let mut roots = Vec::new();
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
    // Old serving generations are derived data and can be removed after the
    // authoritative history roots have been established.
    TrackedHeadContext::new()
        .stage_collect_stale_current_state_generations(&store, writes, &controls)
        .await?;
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
            binary_cas: Default::default(),
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
    #[cfg(feature = "default_wasm_runtime")]
    use std::io::{Cursor, Write as _};
    #[cfg(feature = "default_wasm_runtime")]
    use std::path::Path;
    use std::sync::Arc;

    use crate::branch::{
        BranchHeadControl, BranchHeadControlContext, branch_head_control_precondition,
        stage_branch_head_control,
    };
    use crate::changelog::{
        ChangeId, ChangeLoadRequest, ChangeRecord, ChangelogAppend, ChangelogContext,
        ChangelogReader, ChangelogWriter, CommitId, CommitLoadRequest, CommitRecord, GcRoot,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::json_store::{
        JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJson,
        NormalizedJsonRef,
    };
    use crate::live_state::{CurrentStateDeltaRef, TrackedHeadContext, WorkingDiffIndexCoverage};
    use crate::storage_adapter::{
        Memory, PointReadPlan, SharedStorageAdapterRead, StorageAdapter, StorageGetOptions,
        StorageKey, StoragePrecondition, StorageReadOptions, StorageSpace, StorageValue,
        StorageWriteOptions, StorageWriteSet,
    };
    use crate::storage_codec;
    use crate::tracked_state::{
        CommitDeltaLifecycleSummary, CommitDeltaReplacementGeneration, CommitDeltaReplacementScope,
        CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
        TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateCommitRootParent,
        TrackedStateContext, TrackedStateDeltaRef, TrackedStateIndexValue, TrackedStateRootId,
        TrackedStateSingleStringReplacementRef, load_change_record_by_id,
        scan_change_records_from_commit_deltas, scan_commit_delta_inventory,
        stage_addressable_commit_deltas_with_selected_source, stage_change_locators,
        stage_commit_deltas_for_commit_state, stage_commit_state_manifest,
        stage_ordered_addressable_replacement_parts,
    };
    use crate::{GLOBAL_BRANCH_ID, LixError, Value, engine::Engine};
    use bytes::Bytes;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use super::{
        CheckpointGcState, CheckpointRecoveryRef, GC_REACHABILITY_DELTA_SPACE,
        GC_REACHABILITY_QUEUE_SPACE, GC_TREE_SWEEP_FORMAT_VERSION, GC_TREE_SWEEP_MARK_SPACE,
        RootReachabilityDelta, StoredTreeSweepMark, authenticated_control_commit_reachability,
        begin_tree_sweep_epoch, collect_all_reachability_checkpoint_roots,
        load_checkpoint_gc_state, load_reachability_batches, load_reachability_queue,
        load_recovery_ref, load_recovery_refs, open_tree_sweep_epoch,
        resolve_pending_checkpoint_replacement, retirement_is_proven,
        root_control_digest_for_control, stage_checkpoint_gc_state, stage_delete_recovery_ref,
        stage_reachability_delta_batch, stage_reachability_queue_seed, stage_recovery_ref_rotation,
        stage_tree_sweep_epoch_page,
    };

    async fn append_checkpoint_batch(
        storage: &StorageAdapter<Memory>,
        checkpoint_roots: &[CommitId],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("queue append read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            &[],
            checkpoint_roots,
            &mut preconditions,
        )
        .await
        .expect("checkpoint batch should stage");
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
            .expect("checkpoint batch should commit");
    }

    #[tokio::test]
    async fn destructive_consumers_share_the_complete_tracked_control_projection() {
        let head = CommitId::with_change_address_space(
            *CommitId::for_test_label("control-projection-head").as_uuid(),
        );
        let tracked_generation = CommitId::for_test_label("control-projection-generation");
        let working_diff = CommitId::with_change_address_space(
            *CommitId::for_test_label("control-projection-working-diff").as_uuid(),
        );
        let timestamp =
            LixTimestamp::expect_parse("control projection timestamp", "2026-01-01T00:00:00Z");
        let controls = vec![(
            "main".to_owned(),
            BranchHeadControl {
                head_commit_id: head,
                tracked_generation,
                current_state_revision: 7,
                working_diff_checkpoint_commit_id: Some(working_diff),
                created_at: timestamp,
                updated_at: timestamp,
                ref_change_id: ChangeId::for_test_label("control-projection-ref"),
                schema_presence_bloom: [u64::MAX; 4],
            },
        )];

        let projection = controls[0].1.tracked_reachability();
        assert_eq!(
            projection.chronology_roots,
            [Some(head), Some(working_diff)]
        );
        assert_eq!(projection.serving_generation, tracked_generation);
        assert_eq!(projection.serving_checkpoint_commit_id, Some(working_diff));

        // A valid empty generation has no commit-state manifest because the
        // selector UUID is not chronology. Its serving dependency projection
        // is simply empty, while the semantic roots remain explicit.
        let storage = StorageAdapter::new(Memory::new());
        let checkpoint_record =
            replay_commit_record("control-projection-working-diff", 0, None, timestamp);
        let head_record = replay_commit_record(
            "control-projection-head",
            1,
            Some(checkpoint_record.commit_id),
            timestamp,
        );
        assert_eq!(checkpoint_record.commit_id, working_diff);
        assert_eq!(head_record.commit_id, head);
        persist_replay_closure_fixture(
            &storage,
            storage.new_write_set(),
            &[checkpoint_record, head_record],
            &[],
        )
        .await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("control projection read should open");
        let reachability = authenticated_control_commit_reachability(&read, &controls)
            .await
            .expect("rootless serving generation should be valid");
        assert_eq!(
            reachability.chronology_roots,
            BTreeSet::from([head, working_diff])
        );
        assert!(reachability.serving_dependencies.is_empty());
        assert!(!reachability.chronology_roots.contains(&tracked_generation));
        assert!(!reachability.chronology_roots.contains(&tracked_generation));
    }

    #[tokio::test]
    async fn ordinary_gc_accepts_rootless_tracked_serving_generation() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp = LixTimestamp::expect_parse(
            "rootless serving generation timestamp",
            "2026-01-01T00:00:00Z",
        );
        let old_root = replay_commit_record("rootless-serving-old", 0, None, timestamp);
        let active = replay_commit_record(
            "rootless-serving-active",
            1,
            Some(old_root.commit_id),
            timestamp,
        );
        let mut old_manifest =
            test_commit_state_manifest(&old_root, CommitStateMutationInventory::default());
        old_manifest.replay_debt = CommitStateReplayDebt::default();
        old_manifest.snapshot_root = Some(Box::new(test_snapshot_root(old_root.commit_id)));
        let mut active_manifest =
            test_commit_state_manifest(&active, CommitStateMutationInventory::default());
        active_manifest.replay_debt = CommitStateReplayDebt::default();
        active_manifest.snapshot_root = Some(Box::new(test_snapshot_root(active.commit_id)));

        let control_ref = ChangeId::for_test_label("rootless-serving-control");
        let old_control = replay_branch_control(old_root.commit_id, control_ref, timestamp);
        let serving_generation = CommitId::for_test_label("rootless-serving-generation");
        let mut active_control = replay_branch_control(active.commit_id, control_ref, timestamp);
        active_control.tracked_generation = serving_generation;

        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("rootless serving queue should seed");
        stage_branch_head_control(&mut writes, "main", active_control)
            .expect("rootless serving control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[old_root.clone(), active.clone()],
            &[old_manifest, active_manifest],
        )
        .await;
        stage_replay_root_delta(
            &storage,
            RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(old_root.commit_id),
                new_root: Some(active.commit_id),
                old_control: Some(old_control),
                new_control: Some(active_control),
                old_control_digest: root_control_digest_for_control(Some(&old_control))
                    .expect("old rootless serving control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&active_control))
                    .expect("active rootless serving control should encode"),
            },
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rootless serving manifest read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, serving_generation)
                .await
                .expect("rootless serving manifest absence should load")
                .is_none()
        );
        drop(read);

        let plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            plan.sweep
                .tracked_commit_roots
                .contains(&old_root.commit_id)
        );
        assert!(
            !plan
                .sweep
                .tracked_commit_roots
                .contains(&serving_generation)
        );
    }

    #[tokio::test]
    async fn active_point_replay_closure_uses_replacement_fallback_and_fails_closed() {
        let timestamp =
            LixTimestamp::expect_parse("replay closure timestamp", "2026-01-01T00:00:00Z");

        // A certified replacement fallback is reader-equivalent chronology.
        // A logical history/undo root retains both its physical and semantic
        // dependency instead of the graph parent used by first-parent replay.
        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("replay-fallback-root", 0, None, timestamp);
        let fallback =
            replay_commit_record("replay-fallback-base", 1, Some(root.commit_id), timestamp);
        let active =
            replay_commit_record("replay-fallback-active", 1, Some(root.commit_id), timestamp);
        let mut writes = storage.new_write_set();
        let active_inventory = stage_replacement_inventory(
            &mut writes,
            active.commit_id,
            Some(fallback.commit_id),
            "replay-fallback",
            timestamp,
        );
        let mut root_manifest =
            test_commit_state_manifest(&root, CommitStateMutationInventory::default());
        root_manifest.replay_debt = CommitStateReplayDebt::default();
        root_manifest.snapshot_root = Some(Box::new(test_snapshot_root(root.commit_id)));
        let mut fallback_manifest =
            test_commit_state_manifest(&fallback, CommitStateMutationInventory::default());
        fallback_manifest.replay_debt = CommitStateReplayDebt::default();
        fallback_manifest.snapshot_root = Some(Box::new(test_snapshot_root(fallback.commit_id)));
        let active_manifest = test_commit_state_manifest(&active, active_inventory);
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[root.clone(), fallback.clone(), active.clone()],
            &[root_manifest, fallback_manifest, active_manifest.clone()],
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("replacement replay read should open"),
        );
        let mut physical = BTreeSet::new();
        let mut semantic = BTreeSet::new();
        let mut cas = BTreeSet::new();
        let active_manifests = BTreeMap::from([(active.commit_id, active_manifest)]);
        super::collect_active_point_replay_dependencies(
            &read,
            &active_manifests,
            &BTreeSet::from([active.commit_id]),
            &mut physical,
            &mut semantic,
            &mut cas,
        )
        .await
        .expect("authenticated replacement fallback should close");
        assert_eq!(physical, BTreeSet::from([fallback.commit_id]));
        assert_eq!(
            semantic,
            BTreeSet::from([active.commit_id, fallback.commit_id])
        );
        assert_eq!(cas, BTreeSet::from([fallback.commit_id]));

        // The same bytes reached only through a selected physical owner are
        // retained directly by that manifest and its mutation inventory. They
        // do not enter point replay or acquire semantic/CAS chronology after
        // recovery and undo roots release the interval.
        let mut physical_only = BTreeSet::new();
        let mut semantic_only = BTreeSet::new();
        let mut cas_only = BTreeSet::new();
        super::collect_active_point_replay_dependencies(
            &read,
            &active_manifests,
            &BTreeSet::new(),
            &mut physical_only,
            &mut semantic_only,
            &mut cas_only,
        )
        .await
        .expect("authenticated physical-only fallback should close");
        assert!(physical_only.is_empty());
        assert!(semantic_only.is_empty());
        assert!(cas_only.is_empty());

        // A rootless physical owner without a public graph projection remains
        // valid serving authority. Its authenticated manifest and mutation
        // inventory are consumed directly; replay debt from its former
        // logical role must not promote it back into chronology.
        let storage = StorageAdapter::new(Memory::new());
        let missing_graph = replay_commit_record("replay-missing-graph", 1, None, timestamp);
        let missing_graph_manifest =
            test_commit_state_manifest(&missing_graph, CommitStateMutationInventory::default());
        let mut writes = storage.new_write_set();
        crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
            &mut writes,
            &missing_graph_manifest,
        )
        .expect("missing-graph physical manifest should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("missing-graph fixture should commit");
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("missing-graph replay read should open"),
        );
        let mut physical_only = BTreeSet::new();
        let mut semantic_only = BTreeSet::new();
        let mut cas_only = BTreeSet::new();
        super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(missing_graph.commit_id, missing_graph_manifest.clone())]),
            &BTreeSet::new(),
            &mut physical_only,
            &mut semantic_only,
            &mut cas_only,
        )
        .await
        .expect("physical-only owner must not require semantic graph chronology");
        assert!(physical_only.is_empty());
        assert!(semantic_only.is_empty());
        assert!(cas_only.is_empty());

        // The identical bytes named by a logical history/undo/root role do
        // require public chronology and must still fail closed.
        let error = super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(missing_graph.commit_id, missing_graph_manifest)]),
            &BTreeSet::from([missing_graph.commit_id]),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
        )
        .await
        .expect_err("missing semantic graph node must fail closed");
        assert!(error.message.contains("no authenticated graph node"));

        // The inverse omission is equally fatal: semantic chronology cannot
        // authorize point replay through a missing physical manifest.
        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("replay-missing-manifest-root", 0, None, timestamp);
        let child = replay_commit_record(
            "replay-missing-manifest-child",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let child_manifest =
            test_commit_state_manifest(&child, CommitStateMutationInventory::default());
        persist_replay_closure_fixture(
            &storage,
            storage.new_write_set(),
            &[root, child.clone()],
            std::slice::from_ref(&child_manifest),
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("missing-manifest replay read should open"),
        );
        let mut physical = BTreeSet::new();
        let mut semantic = BTreeSet::new();
        let error = super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(child.commit_id, child_manifest)]),
            &BTreeSet::from([child.commit_id]),
            &mut physical,
            &mut semantic,
            &mut BTreeSet::new(),
        )
        .await
        .expect_err("missing physical replay manifest must fail closed");
        assert!(error.message.contains("no authenticated physical manifest"));
        assert!(physical.is_empty());
        assert!(semantic.is_empty());

        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("replay-malformed-root", 0, None, timestamp);
        let fallback = replay_commit_record(
            "replay-malformed-fallback",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let active = replay_commit_record(
            "replay-malformed-replacement",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let mut writes = storage.new_write_set();
        let inventory = stage_replacement_inventory(
            &mut writes,
            active.commit_id,
            Some(fallback.commit_id),
            "replay-malformed-replacement",
            timestamp,
        );
        let mut root_manifest =
            test_commit_state_manifest(&root, CommitStateMutationInventory::default());
        root_manifest.replay_debt = CommitStateReplayDebt::default();
        root_manifest.snapshot_root = Some(Box::new(test_snapshot_root(root.commit_id)));
        let mut fallback_manifest =
            test_commit_state_manifest(&fallback, CommitStateMutationInventory::default());
        fallback_manifest.replay_debt = CommitStateReplayDebt::default();
        fallback_manifest.snapshot_root = Some(Box::new(test_snapshot_root(fallback.commit_id)));
        let mut active_manifest = test_commit_state_manifest(&active, inventory);
        active_manifest.replay_debt.depth = 2;
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[root, fallback, active.clone()],
            &[root_manifest, fallback_manifest, active_manifest.clone()],
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("malformed replacement read should open"),
        );
        let error = super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(active.commit_id, active_manifest)]),
            &BTreeSet::from([active.commit_id]),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
        )
        .await
        .expect_err("replacement replay depth other than one must fail closed");
        assert!(error.message.contains("malformed replay debt"));

        // Non-decreasing ordinary replay debt is malformed. Detection occurs
        // before either dependency set can authorize a retirement.
        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("replay-debt-root", 0, None, timestamp);
        let child = replay_commit_record("replay-debt-child", 1, Some(root.commit_id), timestamp);
        let mut root_manifest =
            test_commit_state_manifest(&root, CommitStateMutationInventory::default());
        root_manifest.replay_debt = CommitStateReplayDebt::default();
        root_manifest.snapshot_root = Some(Box::new(test_snapshot_root(root.commit_id)));
        let mut child_manifest =
            test_commit_state_manifest(&child, CommitStateMutationInventory::default());
        child_manifest.replay_debt.depth = 2;
        persist_replay_closure_fixture(
            &storage,
            storage.new_write_set(),
            &[root.clone(), child.clone()],
            &[root_manifest, child_manifest.clone()],
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("malformed replay read should open"),
        );
        let mut physical = BTreeSet::new();
        let mut semantic = BTreeSet::new();
        let error = super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(child.commit_id, child_manifest)]),
            &BTreeSet::from([child.commit_id]),
            &mut physical,
            &mut semantic,
            &mut BTreeSet::new(),
        )
        .await
        .expect_err("non-decreasing replay debt must fail closed");
        assert!(error.message.contains("replay debt disagrees"));
        assert!(physical.is_empty());
        assert!(semantic.is_empty());

        // Replacement fallbacks are authenticated physical links, but they
        // are not allowed to form a second cyclic chronology.
        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("replay-cycle-root", 0, None, timestamp);
        let left = replay_commit_record("replay-cycle-left", 1, Some(root.commit_id), timestamp);
        let right = replay_commit_record("replay-cycle-right", 1, Some(root.commit_id), timestamp);
        let mut writes = storage.new_write_set();
        let left_inventory = stage_replacement_inventory(
            &mut writes,
            left.commit_id,
            Some(right.commit_id),
            "replay-cycle-left",
            timestamp,
        );
        let right_inventory = stage_replacement_inventory(
            &mut writes,
            right.commit_id,
            Some(left.commit_id),
            "replay-cycle-right",
            timestamp,
        );
        let mut root_manifest =
            test_commit_state_manifest(&root, CommitStateMutationInventory::default());
        root_manifest.replay_debt = CommitStateReplayDebt::default();
        root_manifest.snapshot_root = Some(Box::new(test_snapshot_root(root.commit_id)));
        let left_manifest = test_commit_state_manifest(&left, left_inventory);
        let right_manifest = test_commit_state_manifest(&right, right_inventory);
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[root, left.clone(), right.clone()],
            &[root_manifest, left_manifest.clone(), right_manifest],
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("cyclic replay read should open"),
        );
        let error = super::collect_active_point_replay_dependencies(
            &read,
            &BTreeMap::from([(left.commit_id, left_manifest)]),
            &BTreeSet::from([left.commit_id]),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
        )
        .await
        .expect_err("replacement fallback cycle must fail closed");
        assert!(error.message.contains("dependency cycle"));
    }

    #[tokio::test]
    async fn authenticated_closure_accepts_rootless_selected_owner_without_semantic_projection() {
        let timestamp =
            LixTimestamp::expect_parse("rootless selected owner timestamp", "2026-01-01T00:00:00Z");
        let owner = replay_commit_record("rootless-selected-owner", 1, None, timestamp);
        let active = replay_commit_record("rootless-selected-alias", 0, None, timestamp);

        let selected_change = packed_change(
            "rootless-selected-owner-change",
            "rootless-selected-owner-row",
            JsonSlot::Inline(r#"{"selected":true}"#.into()),
        );
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let owner_deltas =
            commit_delta_refs(owner.commit_id, std::slice::from_ref(&selected_change));
        let owner_stage = stage_commit_deltas_for_commit_state(&mut writes, &owner_deltas)
            .expect("rootless selected-owner payload should stage");
        let mut alias_deltas =
            commit_delta_refs(active.commit_id, std::slice::from_ref(&selected_change));
        alias_deltas[0].authored = false;
        let alias_stage = stage_addressable_commit_deltas_with_selected_source(
            &mut writes,
            &alias_deltas,
            &[false],
            owner.commit_id,
        )
        .expect("rootless selected alias should stage");
        let owner_manifest =
            test_commit_state_manifest(&owner, owner_stage.mutation_inventory().clone());
        let mut active_manifest =
            test_commit_state_manifest(&active, alias_stage.mutation_inventory().clone());
        active_manifest.replay_debt = CommitStateReplayDebt::default();
        active_manifest.snapshot_root = Some(Box::new(test_snapshot_root(active.commit_id)));

        // The owner's semantic projection has already retired. The active
        // alias still authenticates the owner's immutable mutation inventory,
        // exactly like a selected current-state generation in production.
        persist_replay_closure_fixture(
            &storage,
            writes,
            std::slice::from_ref(&active),
            &[owner_manifest, active_manifest],
        )
        .await;
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("rootless selected-owner read should open"),
        );
        let closure = super::load_authenticated_serving_dependency_closure(
            &read,
            BTreeSet::from([active.commit_id]),
            BTreeSet::new(),
            BTreeSet::new(),
        )
        .await
        .expect("rootless selected owner must remain valid physical authority");
        assert!(closure.physical_authorities.contains(&owner.commit_id));
        assert!(!closure.semantic_dependencies.contains(&owner.commit_id));
        assert!(!closure.cas_logical_dependencies.contains(&owner.commit_id));
    }

    #[tokio::test]
    async fn ordinary_gc_releases_finite_selected_owner_only_after_checkpoint_release() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("selected owner timestamp", "2026-01-01T00:00:00Z");
        let owner = replay_commit_record("selected-owner-source", 0, None, timestamp);
        let checkpoint = replay_commit_record(
            "selected-owner-checkpoint",
            1,
            Some(owner.commit_id),
            timestamp,
        );
        let released = replay_commit_record(
            "selected-owner-released",
            2,
            Some(checkpoint.commit_id),
            timestamp,
        );
        let selected_change = packed_change(
            "selected-owner-change",
            "selected-owner-row",
            JsonSlot::Inline(r#"{"selected":true}"#.into()),
        );

        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("selected-owner queue should seed");
        let owner_deltas =
            commit_delta_refs(owner.commit_id, std::slice::from_ref(&selected_change));
        let owner_stage = stage_commit_deltas_for_commit_state(&mut writes, &owner_deltas)
            .expect("selected owner payload should stage");
        stage_change_locators(&mut writes, &owner_stage.locators);
        let mut checkpoint_deltas =
            commit_delta_refs(checkpoint.commit_id, std::slice::from_ref(&selected_change));
        checkpoint_deltas[0].authored = false;
        let checkpoint_stage =
            stage_commit_deltas_for_commit_state(&mut writes, &checkpoint_deltas)
                .expect("finite selected checkpoint member should stage");

        let mut owner_manifest =
            test_commit_state_manifest(&owner, owner_stage.mutation_inventory().clone());
        owner_manifest.replay_debt = CommitStateReplayDebt::default();
        owner_manifest.snapshot_root = Some(Box::new(test_snapshot_root(owner.commit_id)));
        let mut checkpoint_manifest =
            test_commit_state_manifest(&checkpoint, checkpoint_stage.mutation_inventory().clone());
        checkpoint_manifest.replay_debt = CommitStateReplayDebt::default();
        checkpoint_manifest.snapshot_root =
            Some(Box::new(test_snapshot_root(checkpoint.commit_id)));
        let mut released_manifest =
            test_commit_state_manifest(&released, CommitStateMutationInventory::default());
        released_manifest.replay_debt = CommitStateReplayDebt::default();
        released_manifest.snapshot_root = Some(Box::new(test_snapshot_root(released.commit_id)));

        let control_ref = ChangeId::for_test_label("selected-owner-control");
        let owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
        let checkpoint_control =
            replay_branch_control(checkpoint.commit_id, control_ref, timestamp);
        let released_control = replay_branch_control(released.commit_id, control_ref, timestamp);
        stage_branch_head_control(&mut writes, "main", checkpoint_control)
            .expect("checkpoint control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[owner.clone(), checkpoint.clone(), released.clone()],
            &[owner_manifest, checkpoint_manifest, released_manifest],
        )
        .await;
        stage_replay_root_delta(
            &storage,
            RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(owner.commit_id),
                new_root: Some(checkpoint.commit_id),
                old_control: Some(owner_control),
                new_control: Some(checkpoint_control),
                old_control_digest: root_control_digest_for_control(Some(&owner_control))
                    .expect("owner control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&checkpoint_control))
                    .expect("checkpoint control should encode"),
            },
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("selected-owner dependency read should open");
        assert_eq!(
            crate::tracked_state::load_local_selected_change_owner_commit_ids(
                &read,
                checkpoint.commit_id,
            )
            .await
            .expect("selected owner should resolve"),
            BTreeSet::from([owner.commit_id])
        );
        drop(read);
        let closure = load_audited_repository_retention(&storage).await;
        assert!(closure.physical_authorities.contains(&owner.commit_id));

        let retained_plan = run_ordinary_repository_gc(&storage).await;
        assert!(retained_plan.sweep.tracked_commit_roots.is_empty());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retained selected-owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("retained selected owner should load")
                .is_some(),
            "the finite selected locator keeps its physical owner live"
        );
        drop(read);

        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, "main", released_control)
            .expect("released control should stage");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("selected-owner release read should open");
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            &[RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(checkpoint.commit_id),
                new_root: Some(released.commit_id),
                old_control: Some(checkpoint_control),
                new_control: Some(released_control),
                old_control_digest: root_control_digest_for_control(Some(&checkpoint_control))
                    .expect("checkpoint control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&released_control))
                    .expect("released control should encode"),
            }],
            &[],
            &mut preconditions,
        )
        .await
        .expect("selected-owner release delta should stage");
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
            .expect("selected-owner release should publish atomically");

        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            released_plan
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("released selected-owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("released owner absence should load")
                .is_none(),
            "the owner is reclaimable after the final checkpoint releases the selection"
        );
    }

    #[tokio::test]
    async fn ordinary_gc_releases_scoped_descriptor_owner_only_after_root_release() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("scoped owner timestamp", "2026-01-01T00:00:00Z");
        let owner = replay_commit_record("scoped-part-owner", 0, None, timestamp);
        let checkpoint = replay_commit_record(
            "scoped-part-checkpoint",
            1,
            Some(owner.commit_id),
            timestamp,
        );
        let released = replay_commit_record(
            "scoped-part-released",
            2,
            Some(checkpoint.commit_id),
            timestamp,
        );
        let scope = CommitDeltaReplacementScope {
            schema_key: "scoped_part_owner".to_owned(),
            file_id: None,
        };
        let entity_pk = EntityPk::single("row");
        let encoded_key =
            crate::tracked_state::encode_key_ref(crate::tracked_state::TrackedStateKeyRef {
                schema_key: &scope.schema_key,
                file_id: None,
                entity_pk: &entity_pk,
            });

        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("scoped owner queue should seed");
        let owner_inventory = stage_replacement_inventory(
            &mut writes,
            owner.commit_id,
            None,
            &scope.schema_key,
            timestamp,
        );
        let descriptor = crate::tracked_state::CurrentStatePartDescriptor {
            first_key: encoded_key.clone(),
            last_key: encoded_key,
            content_digest: owner_inventory.replacement_part_digests[0],
            payload_refs_digest: [0; 32],
            source_kind: 0,
            source_id: [0; 16],
            owner_commit_id: *owner.commit_id.as_uuid().as_bytes(),
            part_index: 0,
            source_page_index: 0,
            source_row_offset: 0,
            row_count: 1,
            fragmented: false,
            uniform_created_at: timestamp,
            uniform_updated_at: timestamp,
        };
        let part = crate::tracked_state::current_state_envelope::scoped_range_part_from_current_state_descriptor(
            &scope,
            &descriptor,
        )
        .expect("scoped owner descriptor should encode");
        let marker = crate::tracked_state::scoped_range::ScopedRangeCoverageMarker {
            scope: part.scope.clone(),
            row_count: 1,
            part_count: 1,
        };
        let tree = crate::tracked_state::scoped_range::stage_scoped_range_tree(
            &mut writes,
            [(marker, vec![part])],
        )
        .expect("scoped owner tree should stage");
        let checkpoint_inventory = CommitStateMutationInventory::default();
        let scoped_root = crate::tracked_state::attest_scoped_range_root(
            checkpoint.commit_id,
            None,
            &checkpoint_inventory,
            tree,
        )
        .expect("scoped owner root should attest");

        let mut owner_manifest = test_commit_state_manifest(&owner, owner_inventory);
        owner_manifest.replay_debt = CommitStateReplayDebt::default();
        owner_manifest.snapshot_root = Some(Box::new(test_snapshot_root(owner.commit_id)));
        let mut checkpoint_manifest = test_commit_state_manifest(&checkpoint, checkpoint_inventory);
        checkpoint_manifest.replay_debt = CommitStateReplayDebt::default();
        checkpoint_manifest.snapshot_root =
            Some(Box::new(test_snapshot_root(checkpoint.commit_id)));
        checkpoint_manifest.current_state_scoped_ranges = Some(Box::new(scoped_root));
        let mut released_manifest =
            test_commit_state_manifest(&released, CommitStateMutationInventory::default());
        released_manifest.replay_debt = CommitStateReplayDebt::default();
        released_manifest.snapshot_root = Some(Box::new(test_snapshot_root(released.commit_id)));

        let control_ref = ChangeId::for_test_label("scoped-owner-control");
        let owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
        let checkpoint_control =
            replay_branch_control(checkpoint.commit_id, control_ref, timestamp);
        let released_control = replay_branch_control(released.commit_id, control_ref, timestamp);
        stage_branch_head_control(&mut writes, "main", checkpoint_control)
            .expect("scoped owner checkpoint control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[owner.clone(), checkpoint.clone(), released.clone()],
            &[owner_manifest, checkpoint_manifest, released_manifest],
        )
        .await;
        stage_replay_root_delta(
            &storage,
            RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(owner.commit_id),
                new_root: Some(checkpoint.commit_id),
                old_control: Some(owner_control),
                new_control: Some(checkpoint_control),
                old_control_digest: root_control_digest_for_control(Some(&owner_control))
                    .expect("scoped owner control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&checkpoint_control))
                    .expect("scoped checkpoint control should encode"),
            },
        )
        .await;

        let closure = load_audited_repository_retention(&storage).await;
        assert!(closure.physical_authorities.contains(&owner.commit_id));

        assert!(
            run_ordinary_repository_gc(&storage)
                .await
                .sweep
                .tracked_commit_roots
                .is_empty()
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retained scoped owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("retained scoped owner should load")
                .is_some()
        );
        drop(read);

        publish_replay_root_release(&storage, "main", checkpoint_control, released_control).await;
        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            released_plan
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("released scoped owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("released scoped owner absence should load")
                .is_none()
        );
    }

    #[tokio::test]
    async fn ordinary_gc_releases_native_row_owner_only_after_scoped_root_release() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("native row owner timestamp", "2026-01-01T00:00:00Z");
        let owner = replay_commit_record("native-row-owner", 0, None, timestamp);
        let checkpoint =
            replay_commit_record("native-row-checkpoint", 1, Some(owner.commit_id), timestamp);
        let released = replay_commit_record(
            "native-row-released",
            2,
            Some(checkpoint.commit_id),
            timestamp,
        );
        let row = crate::tracked_state::CurrentStateDataRow {
            encoded_key: b"native-row".to_vec(),
            value: TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("native-row-change"),
                commit_id: owner.commit_id,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
            },
            snapshot: JsonSlot::Inline(r#"{"native":true}"#.into()),
            metadata: JsonSlot::None,
        };
        let encoded = crate::tracked_state::encode_current_state_data_part(
            std::slice::from_ref(&row),
            &mut None,
        )
        .expect("native row part should encode");
        let scope = CommitDeltaReplacementScope {
            schema_key: "native_row_owner".to_owned(),
            file_id: None,
        };
        let descriptor = crate::tracked_state::CurrentStatePartDescriptor {
            first_key: encoded.first_key.clone(),
            last_key: encoded.last_key.clone(),
            content_digest: encoded.digest,
            payload_refs_digest: encoded.refs_digest,
            source_kind: 1,
            source_id: [0; 16],
            owner_commit_id: [0; 16],
            part_index: 0,
            source_page_index: 0,
            source_row_offset: 0,
            row_count: encoded.row_count,
            fragmented: false,
            uniform_created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            uniform_updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
        };
        let part = crate::tracked_state::current_state_envelope::scoped_range_part_from_current_state_descriptor(
            &scope,
            &descriptor,
        )
        .expect("native row descriptor should encode");
        let marker = crate::tracked_state::scoped_range::ScopedRangeCoverageMarker {
            scope: part.scope.clone(),
            row_count: 1,
            part_count: 1,
        };

        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("native row queue should seed");
        writes.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            StorageKey(Bytes::copy_from_slice(&encoded.digest)),
            StorageValue {
                bytes: encoded.bytes.clone(),
            },
        );
        writes.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
            StorageKey(Bytes::copy_from_slice(&encoded.refs_digest)),
            StorageValue {
                bytes: encoded.refs_bytes,
            },
        );
        let tree = crate::tracked_state::scoped_range::stage_scoped_range_tree(
            &mut writes,
            [(marker, vec![part])],
        )
        .expect("native row scoped tree should stage");
        let snapshot_entity_pk = EntityPk::single("native-row");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native serving snapshot read should open");
        let tracked_state = TrackedStateContext::new();
        let mut snapshot_writer = tracked_state.writer(&read, &mut writes);
        snapshot_writer
            .stage_commit_root(
                &checkpoint.commit_id.to_string(),
                None,
                [TrackedStateDeltaRef {
                    schema_key: &scope.schema_key,
                    file_id: None,
                    entity_pk: &snapshot_entity_pk,
                    change_id: row.value.change_id,
                    commit_id: row.value.commit_id,
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                }],
            )
            .await
            .expect("native serving snapshot should stage");
        let checkpoint_snapshot_root = snapshot_writer
            .staged_commit_roots()
            .find(|root| root.commit_id == checkpoint.commit_id)
            .cloned()
            .expect("native serving snapshot metadata should stage");
        drop(snapshot_writer);
        drop(read);
        let inventory = CommitStateMutationInventory::default();
        let scoped_root = crate::tracked_state::attest_scoped_range_root(
            checkpoint.commit_id,
            None,
            &inventory,
            tree,
        )
        .expect("native row scoped root should attest");

        let mut owner_manifest =
            test_commit_state_manifest(&owner, CommitStateMutationInventory::default());
        owner_manifest.replay_debt = CommitStateReplayDebt::default();
        owner_manifest.snapshot_root = Some(Box::new(test_snapshot_root(owner.commit_id)));
        let mut checkpoint_manifest = test_commit_state_manifest(&checkpoint, inventory);
        checkpoint_manifest.replay_debt = CommitStateReplayDebt::default();
        checkpoint_manifest.snapshot_root = Some(Box::new(checkpoint_snapshot_root));
        checkpoint_manifest.current_state_scoped_ranges = Some(Box::new(scoped_root));
        let mut released_manifest =
            test_commit_state_manifest(&released, CommitStateMutationInventory::default());
        released_manifest.replay_debt = CommitStateReplayDebt::default();
        released_manifest.snapshot_root = Some(Box::new(test_snapshot_root(released.commit_id)));

        let control_ref = ChangeId::for_test_label("native-row-control");
        let owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
        let mut checkpoint_control =
            replay_branch_control(checkpoint.commit_id, control_ref, timestamp);
        let serving_generation = CommitId::for_test_label("native-row-serving-generation");
        checkpoint_control.tracked_generation = serving_generation;
        let released_control = replay_branch_control(released.commit_id, control_ref, timestamp);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native serving-base read should open");
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_root_current_base("main", serving_generation, checkpoint.commit_id);
        drop(read);
        stage_branch_head_control(&mut writes, "main", checkpoint_control)
            .expect("native row checkpoint control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[owner.clone(), checkpoint.clone(), released.clone()],
            &[owner_manifest, checkpoint_manifest, released_manifest],
        )
        .await;
        stage_replay_root_delta(
            &storage,
            RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(owner.commit_id),
                new_root: Some(checkpoint.commit_id),
                old_control: Some(owner_control),
                new_control: Some(checkpoint_control),
                old_control_digest: root_control_digest_for_control(Some(&owner_control))
                    .expect("native owner control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&checkpoint_control))
                    .expect("native checkpoint control should encode"),
            },
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native dependency read should open");
        assert_eq!(
            super::validate_live_native_parts(&read, &BTreeSet::from([encoded.digest]))
                .await
                .expect("native owner should decode"),
            BTreeSet::from([owner.commit_id])
        );
        drop(read);
        let closure = load_audited_repository_retention(&storage).await;
        assert!(closure.physical_dependencies.contains(&owner.commit_id));

        // A present native part is authenticated content, not presence-only
        // liveness. Corrupt bytes must abort the complete plan before any
        // queue consumption or retirement mutation is staged.
        let native_key = StorageKey(Bytes::copy_from_slice(&encoded.digest));
        let mut remove = storage.new_write_set();
        remove.delete(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            native_key.clone(),
        );
        storage
            .commit_write_set(remove, StorageWriteOptions::default())
            .await
            .expect("native part removal should commit");
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            native_key.clone(),
            StorageValue {
                bytes: Bytes::from_static(b"malformed-native-current-state-part"),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("malformed native part should commit");
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("malformed native GC read should open"),
        );
        let mut failed_writes = storage.new_write_set();
        let mut failed_preconditions = Vec::new();
        super::stage_repository_gc_with_preconditions(
            read,
            &mut failed_writes,
            &mut failed_preconditions,
        )
        .await
        .expect_err("malformed native owner must fail the whole GC plan");
        assert!(failed_writes.is_empty());
        assert!(failed_preconditions.is_empty());

        let mut remove = storage.new_write_set();
        remove.delete(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            native_key.clone(),
        );
        storage
            .commit_write_set(remove, StorageWriteOptions::default())
            .await
            .expect("malformed native part removal should commit");
        let mut restore = storage.new_write_set();
        restore.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            native_key,
            StorageValue {
                bytes: encoded.bytes.clone(),
            },
        );
        storage
            .commit_write_set(restore, StorageWriteOptions::default())
            .await
            .expect("authenticated native part restoration should commit");

        assert!(
            run_ordinary_repository_gc(&storage)
                .await
                .sweep
                .tracked_commit_roots
                .is_empty()
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retained native owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("retained native owner should load")
                .is_some()
        );
        drop(read);

        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, "main", released_control)
            .expect("native released control should stage");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native release read should open");
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            &[RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(checkpoint.commit_id),
                new_root: Some(released.commit_id),
                old_control: Some(checkpoint_control),
                new_control: Some(released_control),
                old_control_digest: root_control_digest_for_control(Some(&checkpoint_control))
                    .expect("native checkpoint control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&released_control))
                    .expect("native released control should encode"),
            }],
            &[],
            &mut preconditions,
        )
        .await
        .expect("native owner release should stage");
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
            .expect("native owner release should publish");

        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            released_plan
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("released native owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("released native owner absence should load")
                .is_none()
        );
    }

    async fn tree_sweep_fixture() -> (
        StorageAdapter<Memory>,
        CommitId,
        [u8; 32],
        [u8; 32],
        [u8; 32],
    ) {
        let storage = StorageAdapter::new(Memory::new());
        let (root_hash, root_bytes) = crate::tracked_state::test_gc_leaf_chunk(b"");
        let (dead_hash, dead_bytes) = crate::tracked_state::test_gc_leaf_chunk(b"dead");
        let (dead_hash_two, dead_bytes_two) = crate::tracked_state::test_gc_leaf_chunk(b"dead-2");
        let commit_id = CommitId::for_test_label("tree-sweep-root");
        let timestamp =
            LixTimestamp::expect_parse("tree sweep fixture timestamp", "2026-01-01T00:00:00Z");
        let manifest = CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt::default(),
            mutations: CommitStateMutationInventory::default(),
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: Some(Box::new(TrackedStateCommitRoot {
                commit_id,
                root_id: TrackedStateRootId::new(root_hash),
                parent_roots: vec![TrackedStateCommitRootParent {
                    commit_id,
                    root_id: TrackedStateRootId::new(root_hash),
                }],
                changed_key_count: 0,
                row_count_estimate: 0,
                tree_height: 1,
                primary_chunk_count: 1,
                primary_chunk_bytes: root_bytes.len() as u64,
            })),
        };
        let control = BranchHeadControl {
            head_commit_id: commit_id,
            tracked_generation: commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(commit_id),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label("tree-sweep-ref"),
            schema_presence_bloom: [0; 4],
        };
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("queue should stage");
        stage_commit_state_manifest(&mut writes, &manifest).expect("manifest should stage");
        stage_branch_head_control(&mut writes, "main", control).expect("control should stage");
        for (hash, bytes) in [
            (root_hash, root_bytes),
            (dead_hash, dead_bytes),
            (dead_hash_two, dead_bytes_two),
        ] {
            writes.put(
                crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                StorageKey(Bytes::copy_from_slice(&hash)),
                StorageValue { bytes },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tree sweep fixture should commit");
        (storage, commit_id, root_hash, dead_hash, dead_hash_two)
    }

    #[tokio::test]
    async fn gc_sweep_branch_guard_rejects_concurrent_publication() {
        let (storage, commit_id, _root_hash, _dead_hash, _dead_hash_two) =
            tree_sweep_fixture().await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open");
        let observed = BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&["main".to_owned()])
            .await
            .expect("branch control should load")
            .pop()
            .expect("branch control observation should exist");
        let guard = branch_head_control_precondition("main", observed.raw_token)
            .expect("branch control guard should encode");
        drop(read);

        let mut concurrent = storage.new_write_set();
        let mut advanced = observed.control.expect("fixture control should exist");
        advanced.head_commit_id = CommitId::for_test_label("concurrent-head");
        advanced.tracked_generation = advanced.head_commit_id;
        advanced.tracked_generation = advanced.head_commit_id;
        advanced.current_state_revision = advanced.current_state_revision.saturating_add(1);
        stage_branch_head_control(&mut concurrent, "main", advanced)
            .expect("concurrent control should stage");
        storage
            .commit_write_set(concurrent, StorageWriteOptions::default())
            .await
            .expect("concurrent publication should commit");

        let mut stale_sweep = storage.new_write_set();
        stale_sweep.put(
            GC_REACHABILITY_QUEUE_SPACE,
            StorageKey(Bytes::from_static(b"stale-sweep-marker")),
            StorageValue {
                bytes: Bytes::from_static(b"stale-sweep-marker"),
            },
        );
        let error = storage
            .commit_write_set(
                stale_sweep,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale GC publication must be rejected");
        assert!(error.to_string().contains("precondition"));
        assert_ne!(advanced.head_commit_id, commit_id);
    }

    #[tokio::test]
    async fn offline_sweep_and_audit_share_retained_physical_owner_closure() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("shared offline owner timestamp", "2026-01-01T00:00:00Z");
        let owner = CommitId::for_test_label("shared-offline-physical-owner");
        let active = CommitId::for_test_label("shared-offline-active-root");
        let retired_ref = ChangeId::for_test_label("shared-offline-retired-ref");
        let active_ref = ChangeId::for_test_label("shared-offline-active-ref");
        let (owner_hash, owner_bytes) =
            crate::tracked_state::test_gc_leaf_chunk(b"shared-offline-owner");
        let (active_hash, active_bytes) =
            crate::tracked_state::test_gc_leaf_chunk(b"shared-offline-active");
        let owner_root = TrackedStateCommitRoot {
            commit_id: owner,
            root_id: TrackedStateRootId::new(owner_hash),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: owner_bytes.len() as u64,
        };
        let active_root = TrackedStateCommitRoot {
            commit_id: active,
            root_id: TrackedStateRootId::new(active_hash),
            parent_roots: vec![TrackedStateCommitRootParent {
                commit_id: owner,
                root_id: owner_root.root_id.clone(),
            }],
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: active_bytes.len() as u64,
        };
        let manifest = |commit_id, snapshot_root| CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt::default(),
            mutations: CommitStateMutationInventory::default(),
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: Some(Box::new(snapshot_root)),
        };
        let owner_control = replay_branch_control(owner, retired_ref, timestamp);
        let active_control = replay_branch_control(active, active_ref, timestamp);
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("shared offline queue should seed");
        stage_branch_head_control(&mut writes, "main", active_control)
            .expect("shared offline control should stage");
        stage_commit_state_manifest(&mut writes, &manifest(owner, owner_root))
            .expect("shared offline owner manifest should stage");
        stage_commit_state_manifest(&mut writes, &manifest(active, active_root))
            .expect("shared offline active manifest should stage");
        for (hash, bytes) in [(owner_hash, owner_bytes), (active_hash, active_bytes)] {
            writes.put(
                crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                StorageKey(Bytes::copy_from_slice(&hash)),
                StorageValue { bytes },
            );
        }
        let mut retired_change = packed_change(
            "shared-offline-retired-ref",
            "shared-offline-retired-row",
            JsonSlot::Inline(r#"{"retained":true}"#.into()),
        );
        retired_change.change_id = retired_ref;
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("shared offline changelog read should open");
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: Vec::new(),
                changes: vec![retired_change],
            })
            .await
            .expect("shared offline retired ref should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("shared offline fixture should commit");
        stage_replay_root_delta(
            &storage,
            RootReachabilityDelta {
                branch_id: "main".to_owned(),
                old_root: Some(owner),
                new_root: Some(active),
                old_control: Some(owner_control),
                new_control: Some(active_control),
                old_control_digest: root_control_digest_for_control(Some(&owner_control))
                    .expect("shared offline old control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&active_control))
                    .expect("shared offline active control should encode"),
            },
        )
        .await;

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("shared offline consumer read should open"),
        );
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .expect("shared offline controls should load");
        let (queue, _) = load_reachability_queue(&read)
            .await
            .expect("shared offline queue should load");
        let closure = super::load_authenticated_repository_retention(&read, &controls, &queue)
            .await
            .expect("shared offline owner closure should authenticate");
        assert!(closure.physical_dependencies.contains(&owner));
        let (_, _, _, live_chunks) = super::load_tree_sweep_root_closure(&read)
            .await
            .expect("offline sweep should retain the physical owner tree");
        assert_eq!(live_chunks, BTreeSet::from([owner_hash, active_hash]));
        let audit = super::audit_repository_gc_standalone_refs(&read)
            .await
            .expect("standalone audit should consume the same closure");
        assert_eq!(
            audit,
            vec![format!(
                "{}:retired_delta_old_control:history_dependency_pin:old_root={owner}",
                retired_ref
            )]
        );
    }

    #[tokio::test]
    async fn tree_sweep_epoch_closes_roots_once_and_reclaims_only_unmarked_chunks() {
        let (storage, _commit_id, _root_hash, dead_hash, dead_hash_two) =
            tree_sweep_fixture().await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::<StoragePrecondition>::new();
        let _session = begin_tree_sweep_epoch(&&read, &mut writes, &mut preconditions)
            .await
            .expect("epoch closure should build once");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("epoch metadata should publish atomically");
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopened epoch read should open");
        let mut reopened = open_tree_sweep_epoch(&read)
            .await
            .expect("epoch should reopen")
            .expect("epoch should exist");
        assert_eq!(reopened.epoch.live_chunk_count, 1);
        assert_eq!(reopened.live_chunks.len(), 1);
        let mut page_writes = storage.new_write_set();
        let mut page_preconditions = Vec::new();
        assert!(
            stage_tree_sweep_epoch_page(
                &read,
                &mut reopened,
                &mut page_writes,
                &mut page_preconditions,
            )
            .await
            .expect("tree page should stage")
        );
        storage
            .commit_write_set(
                page_writes,
                StorageWriteOptions {
                    preconditions: page_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("tree page should commit");
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-sweep read should open");
        let keys = [
            StorageKey(Bytes::copy_from_slice(&dead_hash)),
            StorageKey(Bytes::copy_from_slice(&dead_hash_two)),
        ];
        let result =
            PointReadPlan::new(crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE, &keys)
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("post-sweep chunks should read");
        assert!(result.value.into_iter().all(|value| value.is_none()));
        assert!(
            open_tree_sweep_epoch(&read)
                .await
                .expect("completed epoch should reopen")
                .expect("completed epoch should remain")
                .cursor
                .complete
        );
    }

    #[tokio::test]
    async fn tree_sweep_epoch_fails_closed_on_missing_mark_or_corrupt_chunk() {
        let (storage, _commit_id, _root_hash, dead_hash, _dead_hash_two) =
            tree_sweep_fixture().await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        begin_tree_sweep_epoch(&&read, &mut writes, &mut preconditions)
            .await
            .expect("epoch should begin");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("epoch should publish");
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopened epoch read should open");
        let session = open_tree_sweep_epoch(&read)
            .await
            .expect("epoch should load")
            .expect("epoch should exist");
        let live_hash = *session.live_chunks.iter().next().expect("root mark");
        let epoch_id = session.epoch.epoch_id;
        drop(read);
        let mut remove_mark = storage.new_write_set();
        remove_mark.delete(
            GC_TREE_SWEEP_MARK_SPACE,
            StorageKey(Bytes::copy_from_slice(&live_hash)),
        );
        storage
            .commit_write_set(remove_mark, StorageWriteOptions::default())
            .await
            .expect("test mark deletion should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-mark read should open");
        assert!(
            open_tree_sweep_epoch(&read)
                .await
                .expect_err("missing live mark must fail closed")
                .to_string()
                .contains("mark inventory")
        );
        drop(read);

        // Restore the mark and corrupt an unmarked chunk. The page validates
        // every key/value before it stages any delete, so the failed page has
        // an empty write set and cannot partially reclaim its siblings.
        let mut restore = storage.new_write_set();
        restore.put(
            GC_TREE_SWEEP_MARK_SPACE,
            StorageKey(Bytes::copy_from_slice(&live_hash)),
            StorageValue {
                bytes: Bytes::from(
                    storage_codec::encode(
                        "tree sweep mark",
                        &StoredTreeSweepMark {
                            format_version: GC_TREE_SWEEP_FORMAT_VERSION,
                            epoch_id,
                            chunk_hash: live_hash,
                        },
                    )
                    .expect("mark should encode"),
                ),
            },
        );
        storage
            .commit_write_set(restore, StorageWriteOptions::default())
            .await
            .expect("mark restoration should commit");
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&dead_hash)),
            StorageValue {
                bytes: Bytes::from_static(b"corrupt-tree-chunk"),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("test corruption should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corruption read should open");
        let mut session = open_tree_sweep_epoch(&read)
            .await
            .expect("restored epoch should load")
            .expect("restored epoch should exist");
        let mut page_writes = storage.new_write_set();
        let mut page_preconditions = Vec::new();
        assert!(
            stage_tree_sweep_epoch_page(
                &read,
                &mut session,
                &mut page_writes,
                &mut page_preconditions,
            )
            .await
            .is_err()
        );
        assert!(page_writes.is_empty(), "failed page must stage zero writes");
    }

    #[tokio::test]
    async fn tree_sweep_epoch_fails_closed_when_live_chunk_disappears() {
        let (storage, _commit_id, root_hash, _dead_hash, _dead_hash_two) =
            tree_sweep_fixture().await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fixture read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        begin_tree_sweep_epoch(&&read, &mut writes, &mut preconditions)
            .await
            .expect("epoch should begin");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("epoch should publish");
        drop(read);

        let mut remove_root = storage.new_write_set();
        remove_root.delete(
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&root_hash)),
        );
        storage
            .commit_write_set(remove_root, StorageWriteOptions::default())
            .await
            .expect("test root deletion should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("missing-root read should open");
        let mut session = open_tree_sweep_epoch(&read)
            .await
            .expect("epoch should load")
            .expect("epoch should exist");
        let mut page_writes = storage.new_write_set();
        let mut page_preconditions = Vec::new();
        assert!(
            stage_tree_sweep_epoch_page(
                &read,
                &mut session,
                &mut page_writes,
                &mut page_preconditions,
            )
            .await
            .is_err(),
            "missing live chunk must fail closed"
        );
        assert!(
            page_writes.is_empty(),
            "missing live chunk must stage zero deletes"
        );
    }

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
        drop(read);

        let mut writes = storage.new_write_set();
        stage_delete_recovery_ref(&mut writes, "main")
            .expect("deleted branch recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("deleted branch recovery ref should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retired recovery read should open");
        assert_eq!(
            load_recovery_ref(&read, "main")
                .await
                .expect("retired main recovery ref should load"),
            None
        );
        assert!(
            load_recovery_ref(&read, "other")
                .await
                .expect("other recovery ref should load")
                .is_some(),
            "retiring one branch must not remove another branch's serving context"
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

    fn checkpoint_replacement_delta(
        label: &str,
        old_root: CommitId,
        new_root: CommitId,
    ) -> RootReachabilityDelta {
        let timestamp = LixTimestamp::expect_parse(
            "checkpoint replacement test timestamp",
            "2026-01-01T00:00:00Z",
        );
        let old_control = BranchHeadControl {
            head_commit_id: old_root,
            tracked_generation: old_root,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label(&format!("{label}-old-control")),
            schema_presence_bloom: [0; 4],
        };
        let new_control = BranchHeadControl {
            head_commit_id: new_root,
            tracked_generation: new_root,
            current_state_revision: 1,
            working_diff_checkpoint_commit_id: Some(new_root),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: ChangeId::for_test_label(&format!("{label}-new-control")),
            schema_presence_bloom: [0; 4],
        };
        RootReachabilityDelta {
            branch_id: label.to_owned(),
            old_root: Some(old_root),
            new_root: Some(new_root),
            old_control: Some(old_control),
            new_control: Some(new_control),
            old_control_digest: root_control_digest_for_control(Some(&old_control))
                .expect("old replacement control should encode"),
            new_control_digest: root_control_digest_for_control(Some(&new_control))
                .expect("new replacement control should encode"),
        }
    }

    async fn append_replacement_delta(
        storage: &StorageAdapter<Memory>,
        delta: RootReachabilityDelta,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replacement publication read should open");
        let checkpoint = delta.new_root.expect("replacement has a new root");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            std::slice::from_ref(&delta),
            &[checkpoint],
            &mut preconditions,
        )
        .await
        .expect("replacement delta should stage");
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
            .expect("replacement delta should commit");
    }

    #[tokio::test]
    async fn pending_checkpoint_replacement_is_unique_and_queue_cas_fenced() {
        let storage = StorageAdapter::new(Memory::new());
        let mut seed = storage.new_write_set();
        stage_reachability_queue_seed(&mut seed).expect("queue seed should stage");
        storage
            .commit_write_set(seed, StorageWriteOptions::default())
            .await
            .expect("queue seed should commit");

        let recovered = CommitId::for_test_label("pending-replacement-recovered");
        let checkpoint = CommitId::for_test_label("pending-replacement-checkpoint");
        append_replacement_delta(
            &storage,
            checkpoint_replacement_delta("main", recovered, checkpoint),
        )
        .await;

        let stale_read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("branch publication read should open"),
        );
        let proof = resolve_pending_checkpoint_replacement(&stale_read, recovered)
            .await
            .expect("unique pending replacement should authenticate")
            .expect("pending replacement should exist");
        assert_eq!(proof.checkpoint_commit_id, checkpoint);
        assert_eq!(proof.checkpoint_branch_id, "main");

        let mut stale_writes = storage.new_write_set();
        let mut stale_preconditions = Vec::new();
        stage_reachability_delta_batch(
            &stale_read,
            &mut stale_writes,
            &[],
            &[checkpoint],
            &mut stale_preconditions,
        )
        .await
        .expect("branch-side queue fence should stage");

        let second_checkpoint = CommitId::for_test_label("pending-replacement-checkpoint-2");
        append_replacement_delta(
            &storage,
            checkpoint_replacement_delta("main", recovered, second_checkpoint),
        )
        .await;
        let stale_error = storage
            .commit_write_set(
                stale_writes,
                StorageWriteOptions {
                    preconditions: stale_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale branch publication must lose the queue CAS");
        assert!(stale_error.to_string().contains("precondition"));

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("ambiguous replacement read should open"),
        );
        let ambiguous = resolve_pending_checkpoint_replacement(&read, recovered)
            .await
            .expect_err("two pending direct replacements must fail closed");
        assert!(ambiguous.message.contains("ambiguous"));
    }

    #[tokio::test]
    async fn recovery_ref_without_pending_replacement_is_not_branchable_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("queue seed should stage");
        let recovered = CommitId::for_test_label("consumed-replacement-recovered");
        let checkpoint = CommitId::for_test_label("consumed-replacement-checkpoint");
        stage_recovery_ref_rotation(
            &mut writes,
            &CheckpointRecoveryRef {
                branch_id: "main".to_owned(),
                recovered_head_commit_id: recovered,
                checkpoint_commit_id: checkpoint,
                interval_has_commits: true,
            },
        )
        .expect("recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("recovery-only fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("recovery-only read should open"),
        );
        let error = resolve_pending_checkpoint_replacement(&read, recovered)
            .await
            .expect_err("a mutable recovery ref must not replace a consumed queue proof");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_pending_checkpoint_replacement_fails_closed() {
        let storage = StorageAdapter::new(Memory::new());
        let mut seed = storage.new_write_set();
        stage_reachability_queue_seed(&mut seed).expect("queue seed should stage");
        storage
            .commit_write_set(seed, StorageWriteOptions::default())
            .await
            .expect("queue seed should commit");

        let recovered = CommitId::for_test_label("malformed-replacement-recovered");
        let checkpoint = CommitId::for_test_label("malformed-replacement-checkpoint");
        let mut malformed = checkpoint_replacement_delta("main", recovered, checkpoint);
        malformed
            .new_control
            .as_mut()
            .expect("replacement has new control")
            .working_diff_checkpoint_commit_id = None;
        malformed.new_control_digest =
            root_control_digest_for_control(malformed.new_control.as_ref())
                .expect("malformed control should still encode");
        append_replacement_delta(&storage, malformed).await;

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("malformed replacement read should open"),
        );
        let error = resolve_pending_checkpoint_replacement(&read, recovered)
            .await
            .expect_err("mapping without an exact checkpoint baseline must fail closed");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
    }

    #[tokio::test]
    async fn authenticated_queue_fold_streams_beyond_one_page_and_fails_closed_on_corruption() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("queue seed should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("queue seed should commit");
        let roots = (0..65)
            .map(|index| CommitId::for_test_label(&format!("queue-root-{index}")))
            .collect::<Vec<_>>();
        for root in &roots {
            append_checkpoint_batch(&storage, std::slice::from_ref(root)).await;
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("full queue read should open");
        let (queue, _) = load_reachability_queue(&read)
            .await
            .expect("queue should load");
        assert_eq!(
            load_reachability_batches(&read, &queue)
                .await
                .expect("bounded retirement batches should load")
                .len(),
            64
        );
        assert_eq!(
            collect_all_reachability_checkpoint_roots(&read, &queue)
                .await
                .expect("full root fold should load"),
            roots.iter().copied().collect()
        );
        drop(read);

        let mut corrupt = storage.new_write_set();
        corrupt.put(
            GC_REACHABILITY_DELTA_SPACE,
            super::reachability_sequence_key(65),
            StorageValue {
                bytes: Bytes::from_static(b"corrupt-authenticated-queue-batch"),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("queue corruption fixture should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt queue read should open");
        assert!(
            collect_all_reachability_checkpoint_roots(&read, &queue)
                .await
                .is_err(),
            "the shared decoder must authenticate every page"
        );
    }

    async fn stage_root_only_branch_publication(
        storage: &StorageAdapter<Memory>,
        new_control: BranchHeadControl,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("new branch publication read should open");
        let mut publication = storage.new_write_set();
        stage_branch_head_control(&mut publication, "queue-race-branch", new_control)
            .expect("new branch control should stage");
        let delta = RootReachabilityDelta {
            branch_id: "queue-race-branch".to_owned(),
            old_root: None,
            new_root: Some(new_control.head_commit_id),
            old_control: None,
            new_control: Some(new_control),
            old_control_digest: root_control_digest_for_control(None).unwrap(),
            new_control_digest: root_control_digest_for_control(Some(&new_control)).unwrap(),
        };
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(&read, &mut publication, &[delta], &[], &mut preconditions)
            .await
            .expect("new branch queue batch should stage");
        (publication, preconditions)
    }

    async fn queue_publication_race_rejects_stale_peer(blocked_queue: bool, gc_first: bool) {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("race repository should initialize");
        let storage = StorageAdapter::new(backend);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan staging read should open");
        let mut orphan_writes = storage.new_write_set();
        let orphan = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut orphan_writes)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                b"queue-race-orphan".to_vec(),
            ))
            .await
            .expect("orphan payload should stage");
        drop(read);
        storage
            .commit_write_set(orphan_writes, StorageWriteOptions::default())
            .await
            .expect("orphan payload should commit");

        let (main_branch_id, main_control) = BranchHeadControlContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("main control read should open"),
            )
            .scan()
            .await
            .expect("branch controls should load")
            .into_iter()
            .find(|(branch_id, _)| branch_id != GLOBAL_BRANCH_ID)
            .expect("workspace branch control should exist");
        if blocked_queue {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("blocked batch read should open");
            let mut writes = storage.new_write_set();
            let mut preconditions = Vec::new();
            let delta = RootReachabilityDelta {
                branch_id: main_branch_id,
                old_root: Some(main_control.head_commit_id),
                new_root: Some(main_control.head_commit_id),
                old_control: Some(main_control),
                new_control: Some(main_control),
                old_control_digest: root_control_digest_for_control(Some(&main_control)).unwrap(),
                new_control_digest: root_control_digest_for_control(Some(&main_control)).unwrap(),
            };
            stage_reachability_delta_batch(&read, &mut writes, &[delta], &[], &mut preconditions)
                .await
                .expect("blocked batch should stage");
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
                .expect("blocked batch should commit");
        }

        let stale_read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("stale sweep read should open"),
        );
        let mut stale_writes = storage.new_write_set();
        let mut stale_preconditions = Vec::new();
        let plan = super::stage_repository_gc_with_preconditions(
            stale_read,
            &mut stale_writes,
            &mut stale_preconditions,
        )
        .await
        .expect("stale sweep should stage");
        assert_eq!(plan.sweep.binary_cas.reclaimed_chunk_rows, 1);

        let mut new_control = main_control;
        new_control.ref_change_id = ChangeId::for_test_label("queue-race-new-branch-ref");
        let (publication, publication_preconditions) =
            stage_root_only_branch_publication(&storage, new_control).await;
        if gc_first {
            storage
                .commit_write_set(
                    stale_writes,
                    StorageWriteOptions {
                        preconditions: stale_preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .expect("GC should win the root-publication epoch");
            let error = storage
                .commit_write_set(
                    publication,
                    StorageWriteOptions {
                        preconditions: publication_preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .expect_err("stale root-only publication must lose after GC");
            assert!(error.to_string().contains("precondition"));
            let (retry, retry_preconditions) =
                stage_root_only_branch_publication(&storage, new_control).await;
            storage
                .commit_write_set(
                    retry,
                    StorageWriteOptions {
                        preconditions: retry_preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .expect("fresh root-only publication retry should commit");
            let control = BranchHeadControlContext::new()
                .reader(
                    storage
                        .begin_read(StorageReadOptions::default())
                        .await
                        .expect("root-only retry verification read should open"),
                )
                .load("queue-race-branch")
                .await
                .expect("root-only retry control should load");
            assert_eq!(control, Some(new_control));
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("GC-first orphan verification read should open");
            let mut reader = crate::binary_cas::BinaryCasContext::new().reader(read);
            assert!(
                reader
                    .load_bytes_many(&[orphan.hash])
                    .await
                    .expect("GC-first orphan verification should load")
                    .into_vec()[0]
                    .is_none()
            );
            return;
        }
        storage
            .commit_write_set(
                publication,
                StorageWriteOptions {
                    preconditions: publication_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("new branch publication should commit");

        let error = storage
            .commit_write_set(
                stale_writes,
                StorageWriteOptions {
                    preconditions: stale_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale sweep must not commit after queue publication");
        assert!(error.to_string().contains("precondition"));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan verification read should open");
        let mut reader = crate::binary_cas::BinaryCasContext::new().reader(read);
        assert!(
            reader
                .load_bytes_many(&[orphan.hash])
                .await
                .expect("orphan should remain readable")
                .into_vec()[0]
                .is_some()
        );
    }

    #[tokio::test]
    async fn repository_gc_epoch_serializes_root_only_publication_in_both_orders() {
        queue_publication_race_rejects_stale_peer(false, false).await;
        queue_publication_race_rejects_stale_peer(true, false).await;
        queue_publication_race_rejects_stale_peer(false, true).await;
        queue_publication_race_rejects_stale_peer(true, true).await;
    }

    #[tokio::test]
    async fn repository_gc_marks_binary_roots_from_checkpoint_batches_beyond_retirement_window() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("full-queue repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("full-queue repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("full-queue session should open");
        let live_bytes = b"checkpoint-batch-65-live-blob";
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/queued.bin', $1)",
                &[Value::Blob(live_bytes.to_vec().into())],
            )
            .await
            .expect("queued file should publish");
        let retained_commit = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .expect("retained commit should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("retained commit id should exist");
        let retained_commit = CommitId::parse_lix(&retained_commit, "retained queued commit")
            .expect("retained commit id should parse");
        session
            .execute("DELETE FROM lix_file WHERE path = '/queued.bin'", &[])
            .await
            .expect("queued file should retire from current state");
        let current_commit = session
            .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .expect("current commit should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("current commit id should exist");
        let current_commit = CommitId::parse_lix(&current_commit, "current queued commit")
            .expect("current commit id should parse");

        let storage = StorageAdapter::new(backend.clone());
        loop {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("queue length read should open");
            let (queue, _) = load_reachability_queue(&read)
                .await
                .expect("queue length should load");
            let pending = if queue.head_sequence == 0 {
                0
            } else {
                queue.tail_sequence - queue.head_sequence + 1
            };
            drop(read);
            if pending >= 64 {
                break;
            }
            append_checkpoint_batch(&storage, &[current_commit]).await;
        }
        append_checkpoint_batch(&storage, &[retained_commit]).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan staging read should open");
        let mut orphan_writes = storage.new_write_set();
        let orphan = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut orphan_writes)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                b"unrelated-full-queue-orphan".to_vec(),
            ))
            .await
            .expect("unrelated orphan should stage");
        drop(read);
        storage
            .commit_write_set(orphan_writes, StorageWriteOptions::default())
            .await
            .expect("unrelated orphan should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("repository sweep read should open"),
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan =
            super::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
                .await
                .expect("full-queue repository sweep should stage");
        assert!(plan.sweep.binary_cas.reclaimed_chunk_rows >= 1);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("full-queue repository sweep should commit");

        drop(session);
        drop(engine);
        let _reopened = Engine::new(backend.clone())
            .await
            .expect("repository should reopen after full-queue sweep");
        let read = StorageAdapter::new(backend)
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cold CAS read should open");
        let mut reader = crate::binary_cas::BinaryCasContext::new().reader(read);
        let blobs = reader
            .load_bytes_many(&[
                crate::binary_cas::BlobId::from_content(live_bytes),
                orphan.hash,
            ])
            .await
            .expect("cold CAS read should succeed")
            .into_vec();
        assert_eq!(blobs[0].as_deref(), Some(live_bytes.as_slice()));
        assert!(
            blobs[1].is_none(),
            "unrelated CAS garbage must be reclaimed"
        );
    }

    #[cfg(feature = "default_wasm_runtime")]
    #[tokio::test]
    async fn repository_gc_keeps_plugin_wasm_for_cold_runtime_execution() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("plugin-GC repository should initialize");
        let engine = Engine::new_with_wasm_runtime(
            backend.clone(),
            crate::default_wasm_runtime::runtime().expect("WASM runtime should initialize"),
        )
        .await
        .expect("plugin-GC repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("plugin-GC session should open");
        let (archive, wasm) = gc_csv_plugin_archive();
        let wasm_hash = crate::binary_cas::BlobId::from_content(&wasm);
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES \
                 ('/.lix/plugins/plugin_csv.lixplugin', $1)",
                &[Value::Blob(archive.into())],
            )
            .await
            .expect("CSV plugin should install");
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/owned.csv', $1)",
                &[Value::Blob(b"before,gc\n".to_vec().into())],
            )
            .await
            .expect("installed plugin should execute before GC");

        let storage = StorageAdapter::new(backend.clone());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("plugin-GC orphan read should open");
        let mut orphan_writes = storage.new_write_set();
        let orphan = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut orphan_writes)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                b"plugin-gc-unrelated-orphan".to_vec(),
            ))
            .await
            .expect("plugin-GC orphan should stage");
        drop(read);
        storage
            .commit_write_set(orphan_writes, StorageWriteOptions::default())
            .await
            .expect("plugin-GC orphan should commit");

        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, wasm_hash, true).await;
        assert_binary_cas_presence(&storage, orphan.hash, false).await;

        // A new Engine owns an empty runtime-host factory/actor cache. Updating
        // the semantic file therefore has to cold-load the registry-owned WASM
        // from binary CAS and execute it after the sweep.
        drop(session);
        drop(engine);
        let engine = Engine::new_with_wasm_runtime(
            backend.clone(),
            crate::default_wasm_runtime::runtime().expect("cold WASM runtime should initialize"),
        )
        .await
        .expect("plugin-GC repository should cold reopen");
        let session = engine
            .open_workspace_session()
            .await
            .expect("cold plugin-GC session should open");
        session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = '/owned.csv'",
                &[Value::Blob(b"after,gc\n".to_vec().into())],
            )
            .await
            .expect("cold plugin runtime should load and execute after GC");
        let content = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/owned.csv'",
                &[],
            )
            .await
            .expect("cold plugin output should remain readable");
        assert_eq!(
            content.rows()[0].get::<Vec<u8>>("content").unwrap(),
            b"after,gc\n"
        );
        assert_binary_cas_presence(&storage, wasm_hash, true).await;
    }

    #[cfg(feature = "default_wasm_runtime")]
    #[tokio::test]
    async fn repository_gc_reclaims_plugin_wasm_only_after_final_registry_root_releases() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("shared-plugin repository should initialize");
        let engine = Engine::new_with_wasm_runtime(
            backend.clone(),
            crate::default_wasm_runtime::runtime().expect("WASM runtime should initialize"),
        )
        .await
        .expect("shared-plugin repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("shared-plugin session should open");
        let (archive, wasm) = gc_csv_plugin_archive();
        let wasm_hash = crate::binary_cas::BlobId::from_content(&wasm);
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES \
                 ('/.lix/plugins/plugin_csv.lixplugin', $1)",
                &[Value::Blob(archive.into())],
            )
            .await
            .expect("shared CSV plugin should install");
        let storage = StorageAdapter::new(backend);
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, wasm_hash, true).await;
        let branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-000000000035".to_owned()),
                name: "plugin-gc-retained".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("retained plugin branch should create");
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, wasm_hash, true).await;
        session
            .execute(
                "DELETE FROM lix_file WHERE path = '/.lix/plugins/plugin_csv.lixplugin'",
                &[],
            )
            .await
            .expect("plugin should uninstall from main");
        session
            .create_checkpoint()
            .await
            .expect("uninstalled main should release its undo interval");
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, wasm_hash, true).await;

        session
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.id)],
            )
            .await
            .expect("final plugin registry branch should retire");
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, wasm_hash, false).await;
    }

    #[tokio::test]
    async fn repository_gc_fails_closed_on_corrupt_current_plugin_registry() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("corrupt-registry repository should initialize");
        let storage = StorageAdapter::new(backend.clone());
        let corrupt_registry = serde_json::json!({
            "key": crate::plugin::PLUGIN_REGISTRY_KEY,
            "value": {
                "version": 1,
                "plugin_count": 1,
                "generation": "corrupt",
                "plugins": [],
            },
        })
        .to_string();
        let corrupt_snapshot = stage_bare_json(&backend, &corrupt_registry).await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt-registry control read should open");
        let (branch_id, control) = BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .expect("corrupt-registry controls should load")
            .into_iter()
            .find(|(branch_id, _)| branch_id != GLOBAL_BRANCH_ID)
            .expect("workspace branch control should exist");
        let timestamp =
            LixTimestamp::expect_parse("corrupt registry timestamp", "2026-01-01T00:00:00Z");
        let entity_pk = EntityPk::single(crate::plugin::PLUGIN_REGISTRY_KEY);
        let mut writes = storage.new_write_set();
        let mut coverage = WorkingDiffIndexCoverage::default();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_current_state_with_working_diff(
                &branch_id,
                Some(control.tracked_generation),
                control.tracked_generation,
                &[CurrentStateDeltaRef {
                    schema_key: "lix_key_value",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: Some(ChangeId::for_test_label("corrupt-plugin-registry")),
                    commit_id: Some(control.head_commit_id),
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                    snapshot: JsonSlot::Ref(corrupt_snapshot).as_ref_slot(),
                    metadata: JsonSlotRef::None,
                    columnar_base_coordinate: None,
                }],
                &BTreeSet::new(),
                None,
                None,
                None,
                &mut coverage,
            )
            .await
            .expect("corrupt registry row should stage");
        stage_branch_head_control(
            &mut writes,
            &branch_id,
            control
                .next_current_state_revision()
                .expect("corrupt-registry control should advance"),
        )
        .expect("corrupt-registry control should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt-registry fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("corrupt-registry GC read should open"),
        );
        let mut sweep = storage.new_write_set();
        let mut preconditions = Vec::new();
        let error =
            super::stage_repository_gc_with_preconditions(read, &mut sweep, &mut preconditions)
                .await
                .expect_err("corrupt current plugin registry must fail GC closed");
        assert!(error.message.contains("unsupported version"), "{error:?}");
        assert!(sweep.is_empty(), "corruption must stage no GC mutations");
    }

    #[cfg(feature = "default_wasm_runtime")]
    fn gc_csv_plugin_archive() -> (Vec<u8>, Vec<u8>) {
        let wasm = std::fs::read(Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")))
            .expect("CSV plugin WASM should read");
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (path, bytes) in [
            (
                "manifest.json",
                include_str!("../../../plugins/csv/manifest.json").as_bytes(),
            ),
            (
                "schema/csv_table.json",
                include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
            ),
            (
                "schema/csv_row.json",
                include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
            ),
            ("plugin.wasm", wasm.as_slice()),
        ] {
            writer
                .start_file(path, options)
                .expect("plugin archive entry should start");
            writer
                .write_all(bytes)
                .expect("plugin archive entry should write");
        }
        (
            writer
                .finish()
                .expect("plugin archive should finish")
                .into_inner(),
            wasm,
        )
    }

    async fn run_binary_repository_gc(storage: &StorageAdapter<Memory>) {
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("binary repository-GC read should open"),
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        super::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
            .await
            .expect("binary repository GC should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("binary repository GC should commit");
    }

    async fn assert_binary_cas_presence(
        storage: &StorageAdapter<Memory>,
        hash: crate::binary_cas::BlobId,
        expected: bool,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("binary-CAS presence read should open");
        let mut reader = crate::binary_cas::BinaryCasContext::new().reader(read);
        let present = reader
            .load_bytes_many(&[hash])
            .await
            .expect("binary-CAS presence should load")
            .into_vec()[0]
            .is_some();
        assert_eq!(present, expected);
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
                "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (lix_json($1), false)",
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
    async fn repository_gc_retains_historical_file_blob_until_replay_dependency_releases() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("historical blob repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("historical blob repository should open");
        let main = engine
            .open_workspace_session()
            .await
            .expect("historical blob main session should open");
        let branch = main
            .create_branch(crate::CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000000a".to_owned()),
                name: "gc-history-disposable".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("historical disposable branch should create");
        let branch_id = branch.id;
        let session = engine
            .open_session(branch_id.clone())
            .await
            .expect("historical disposable session should open");
        let v1 = b"historical-file-v1";
        let v2 = vec![0xa5; 256];
        let v1_hash = crate::binary_cas::BlobId::from_content(v1);
        let v2_hash = crate::binary_cas::BlobId::from_content(&v2);
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/history.bin', $1)",
                &[Value::Blob(v1.to_vec().into())],
            )
            .await
            .expect("historical file v1 should publish");
        let root_a = session
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id.clone())],
            )
            .await
            .expect("historical root A should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("historical root A should exist");
        session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = '/history.bin'",
                &[Value::Blob(v2.clone().into())],
            )
            .await
            .expect("historical file v2 should publish");
        let root_b = session
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id.clone())],
            )
            .await
            .expect("historical root B should load")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("historical root B should exist");
        let storage = StorageAdapter::new(backend.clone());
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, v1_hash, true).await;
        assert_binary_cas_presence(&storage, v2_hash, true).await;

        drop(session);
        drop(main);
        drop(engine);
        let reopened = Engine::new(backend.clone())
            .await
            .expect("historical blob repository should cold reopen");
        let session = reopened
            .open_session(branch_id.clone())
            .await
            .expect("historical disposable branch should cold reopen");
        let diff = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_binary_blob_ref'",
                &[Value::Text(root_a), Value::Text(root_b)],
            )
            .await
            .expect("historical blob diff should remain authenticated");
        assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
        session
            .undo()
            .await
            .expect("historical blob undo should remain available");
        let undone = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/history.bin'",
                &[],
            )
            .await
            .expect("historical v1 should read after undo");
        assert_eq!(undone.rows()[0].get::<Vec<u8>>("content").unwrap(), v1);
        session
            .redo()
            .await
            .expect("historical blob redo should remain available");
        let redone = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/history.bin'",
                &[],
            )
            .await
            .expect("historical v2 should read after redo");
        assert_eq!(redone.rows()[0].get::<Vec<u8>>("content").unwrap(), v2);
        drop(session);

        let main = reopened
            .open_workspace_session()
            .await
            .expect("historical main session should reopen");
        main.execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.clone())],
        )
        .await
        .expect("historical disposable branch should delete");
        drop(main);
        drop(reopened);

        // Deletion publishes the authenticated B -> None retirement frontier.
        // This fixture is below the queue page limit, so one production sweep
        // must consume that frontier without creating a checkpoint alias for A.
        run_binary_repository_gc(&storage).await;
        assert_binary_cas_presence(&storage, v1_hash, false).await;
        assert_binary_cas_presence(&storage, v2_hash, false).await;

        let final_engine = Engine::new(backend)
            .await
            .expect("historical repository should reopen after branch retirement");
        let final_main = final_engine
            .open_workspace_session()
            .await
            .expect("historical main should open after branch retirement");
        let branches = final_main
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id)],
            )
            .await
            .expect("retired branch absence should read");
        assert_eq!(branches.rows()[0].get::<i64>("entries").unwrap(), 0);
    }

    #[tokio::test]
    async fn repository_gc_keeps_payload_reachable_from_history() {
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

        run_repository_gc(&storage).await;

        assert!(
            json_ref_exists(&storage, crate::json_store::store::JSON_SPACE, shared_ref).await,
            "reachable history must retain its payload"
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

    fn replay_commit_record(
        label: &str,
        generation: u64,
        parent: Option<CommitId>,
        created_at: LixTimestamp,
    ) -> CommitRecord {
        let commit_id =
            CommitId::with_change_address_space(*CommitId::for_test_label(label).as_uuid());
        CommitRecord {
            format_version: 2,
            commit_id,
            generation,
            parent_commit_ids: parent.into_iter().collect(),
            change_id: ChangeId::for_test_label(&format!("{label}-header")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at,
        }
    }

    fn replay_branch_control(
        commit_id: CommitId,
        ref_change_id: ChangeId,
        timestamp: LixTimestamp,
    ) -> BranchHeadControl {
        BranchHeadControl {
            head_commit_id: commit_id,
            tracked_generation: commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(commit_id),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id,
            schema_presence_bloom: [0; 4],
        }
    }

    fn stage_replacement_inventory(
        writes: &mut StorageWriteSet,
        commit_id: CommitId,
        fallback_commit_id: Option<CommitId>,
        schema_key: &str,
        timestamp: LixTimestamp,
    ) -> CommitStateMutationInventory {
        let scope = CommitDeltaReplacementScope {
            schema_key: schema_key.to_owned(),
            file_id: None,
        };
        let generation = CommitDeltaReplacementGeneration {
            scope: scope.clone(),
            fallback_commit_id,
            lifecycle_summary: CommitDeltaLifecycleSummary {
                scope,
                ordered_identity_digest: *blake3::hash(schema_key.as_bytes()).as_bytes(),
                uniform_created_at: timestamp,
            },
        };
        let staged = stage_ordered_addressable_replacement_parts(
            writes,
            std::iter::once(Ok(TrackedStateSingleStringReplacementRef {
                schema_key,
                file_id: None,
                entity_pk: "row",
                commit_id,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: JsonSlotRef::Inline("{}"),
                metadata: JsonSlotRef::None,
            })),
            &generation,
        )
        .expect("replacement replay inventory should stage");
        let mut inventory = staged.mutation_inventory().clone();
        inventory.parts.clear();
        inventory
    }

    async fn persist_replay_closure_fixture(
        storage: &StorageAdapter<Memory>,
        mut writes: StorageWriteSet,
        records: &[CommitRecord],
        manifests: &[CommitStateManifest],
    ) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replay fixture read should open");
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: records.to_vec(),
                changes: Vec::new(),
            })
            .await
            .expect("replay fixture commits should stage");
        for manifest in manifests {
            crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
                &mut writes,
                manifest,
            )
            .expect("replay fixture manifest should stage");
            if let Some(root) = manifest.snapshot_root.as_ref() {
                let (hash, bytes) = test_snapshot_chunk(manifest.commit_id);
                if root.root_id.as_bytes() == &hash {
                    writes.put(
                        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                        StorageKey(Bytes::copy_from_slice(&hash)),
                        StorageValue { bytes },
                    );
                }
            }
        }
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("replay fixture should commit");
    }

    async fn stage_replay_root_delta(
        storage: &StorageAdapter<Memory>,
        delta: RootReachabilityDelta,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replay delta read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            std::slice::from_ref(&delta),
            &[],
            &mut preconditions,
        )
        .await
        .expect("replay delta should stage");
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
            .expect("replay delta should commit");
    }

    async fn publish_replay_root_release(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        old_control: BranchHeadControl,
        new_control: BranchHeadControl,
    ) {
        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, branch_id, new_control)
            .expect("released replay control should stage");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replay release read should open");
        let mut preconditions = Vec::new();
        stage_reachability_delta_batch(
            &read,
            &mut writes,
            &[RootReachabilityDelta {
                branch_id: branch_id.to_owned(),
                old_root: Some(old_control.head_commit_id),
                new_root: Some(new_control.head_commit_id),
                old_control: Some(old_control),
                new_control: Some(new_control),
                old_control_digest: root_control_digest_for_control(Some(&old_control))
                    .expect("old replay control should encode"),
                new_control_digest: root_control_digest_for_control(Some(&new_control))
                    .expect("new replay control should encode"),
            }],
            &[],
            &mut preconditions,
        )
        .await
        .expect("replay release delta should stage");
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
            .expect("replay release should publish");
    }

    async fn run_ordinary_repository_gc(
        storage: &StorageAdapter<Memory>,
    ) -> super::RepositoryGcPlan {
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("ordinary GC read should open"),
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan =
            super::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
                .await
                .expect("ordinary GC should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("ordinary GC should commit");
        plan
    }

    async fn load_audited_repository_retention(
        storage: &StorageAdapter<Memory>,
    ) -> super::AuthenticatedServingDependencyClosure {
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("audited retention read should open"),
        );
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .expect("audited retention controls should load");
        let (queue, _) = load_reachability_queue(&read)
            .await
            .expect("audited retention queue should load");
        let closure = super::load_authenticated_repository_retention(&read, &controls, &queue)
            .await
            .expect("serving-dependency closure should authenticate");
        super::audit_repository_gc_standalone_refs(&read)
            .await
            .expect("standalone audit should consume the same closure");
        closure
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

    fn test_snapshot_chunk(commit_id: CommitId) -> ([u8; 32], Bytes) {
        crate::tracked_state::test_gc_leaf_chunk(commit_id.as_uuid().as_bytes())
    }

    fn test_snapshot_root(commit_id: CommitId) -> TrackedStateCommitRoot {
        let (root_hash, root_bytes) = test_snapshot_chunk(commit_id);
        TrackedStateCommitRoot {
            commit_id,
            root_id: TrackedStateRootId::new(root_hash),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: root_bytes.len() as u64,
        }
    }

    async fn assert_offline_sweep_and_audit_fail_closed(
        storage: &StorageAdapter<Memory>,
        expected_message: &str,
    ) {
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("offline authority read should open"),
        );
        let sweep_error = super::load_tree_sweep_root_closure(&read)
            .await
            .expect_err("offline tree sweep must fail closed");
        let audit_error = super::audit_repository_gc_standalone_refs(&read)
            .await
            .expect_err("standalone audit must fail closed");
        assert_eq!(sweep_error.message, audit_error.message);
        assert!(
            sweep_error.message.contains(expected_message),
            "unexpected shared authority error: {}",
            sweep_error.message
        );
    }

    #[tokio::test]
    async fn offline_sweep_and_audit_reject_missing_and_malformed_required_authority() {
        const MUTABLE_MANIFEST_SPACE: StorageSpace = StorageSpace::mutable(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.id,
            "tracked_state.commit_state_manifest.v7",
        );

        let (storage, commit_id, _, _, _) = tree_sweep_fixture().await;
        let mut writes = storage.new_write_set();
        writes.delete(
            MUTABLE_MANIFEST_SPACE,
            crate::tracked_state::commit_state_authority_key(commit_id),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("required manifest removal should commit");
        assert_offline_sweep_and_audit_fail_closed(&storage, "incomplete split physical authority")
            .await;

        let (storage, commit_id, _, _, _) = tree_sweep_fixture().await;
        let mut writes = storage.new_write_set();
        writes.put(
            MUTABLE_MANIFEST_SPACE,
            crate::tracked_state::commit_state_authority_key(commit_id),
            StorageValue {
                bytes: Bytes::from_static(b"malformed-commit-state-authority"),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("malformed manifest should commit");
        assert_offline_sweep_and_audit_fail_closed(&storage, "unsupported format").await;
    }

    #[tokio::test]
    async fn offline_sweep_and_audit_reject_non_decreasing_and_cyclic_replay_authority() {
        let timestamp = LixTimestamp::expect_parse(
            "offline replay authority timestamp",
            "2026-01-01T00:00:00Z",
        );

        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("offline-replay-debt-root", 0, None, timestamp);
        let child = replay_commit_record(
            "offline-replay-debt-child",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let mut root_manifest =
            test_commit_state_manifest(&root, CommitStateMutationInventory::default());
        root_manifest.replay_debt.depth = 2;
        let mut child_manifest =
            test_commit_state_manifest(&child, CommitStateMutationInventory::default());
        child_manifest.replay_debt.depth = 2;
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("replay-debt queue should seed");
        stage_branch_head_control(
            &mut writes,
            "main",
            replay_branch_control(
                child.commit_id,
                ChangeId::for_test_label("offline-replay-debt-ref"),
                timestamp,
            ),
        )
        .expect("replay-debt control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[root, child],
            &[root_manifest, child_manifest],
        )
        .await;
        assert_offline_sweep_and_audit_fail_closed(&storage, "replay debt disagrees").await;

        let storage = StorageAdapter::new(Memory::new());
        let root = replay_commit_record("offline-replay-cycle-root", 0, None, timestamp);
        let left = replay_commit_record(
            "offline-replay-cycle-left",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let right = replay_commit_record(
            "offline-replay-cycle-right",
            1,
            Some(root.commit_id),
            timestamp,
        );
        let mut writes = storage.new_write_set();
        stage_reachability_queue_seed(&mut writes).expect("replay-cycle queue should seed");
        let left_inventory = stage_replacement_inventory(
            &mut writes,
            left.commit_id,
            Some(right.commit_id),
            "offline-replay-cycle-left",
            timestamp,
        );
        let right_inventory = stage_replacement_inventory(
            &mut writes,
            right.commit_id,
            Some(left.commit_id),
            "offline-replay-cycle-right",
            timestamp,
        );
        stage_branch_head_control(
            &mut writes,
            "main",
            replay_branch_control(
                left.commit_id,
                ChangeId::for_test_label("offline-replay-cycle-ref"),
                timestamp,
            ),
        )
        .expect("replay-cycle control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[root.clone(), left.clone(), right.clone()],
            &[
                test_commit_state_manifest(&root, CommitStateMutationInventory::default()),
                test_commit_state_manifest(&left, left_inventory),
                test_commit_state_manifest(&right, right_inventory),
            ],
        )
        .await;
        assert_offline_sweep_and_audit_fail_closed(&storage, "dependency cycle").await;
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
