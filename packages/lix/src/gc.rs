//! Checkpoint recovery roots and repository garbage collection.
//!
//! Recovery refs are local, mutable roots. They deliberately live outside the
//! changelog: rotating a ref must not create history that itself keeps the
//! recovered commit alive. The checkpoint transaction stages the rotation in
//! the same storage write set that publishes the compacted checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use bytes::Bytes;

use crate::branch::{
    BranchHeadControl, BranchHeadControlContext, BranchHeadTrackedReachability,
    branch_head_control_precondition,
};
use crate::changelog::{ChangeId, CommitId, GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet};
#[cfg(test)]
use crate::changelog::{ChangeRecord, CommitScanRequest};
#[cfg(any(test, feature = "storage-benches"))]
use crate::changelog::{ChangeScanRequest, ChangelogContext, ChangelogReader};
use crate::commit_graph::CommitGraphContext;
use crate::hot_state::TrackedHeadContext;
use crate::hot_state::stage_collect_stale_working_diff_indexes;
use crate::json_store::{JsonRef, JsonSlot, JsonStoreContext};
#[cfg(test)]
use crate::storage_adapter::StorageCoreProjection;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageBeginScanOptions, StorageGetOptions, StorageKey,
    StoragePrecondition, StoragePrefix, StorageProjectedValue, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet,
};
use crate::tracked_state::RetainedPhysicalState;
use crate::{LixError, storage_codec};

pub(crate) const CHECKPOINT_RECOVERY_REF_NAMESPACE: &str = "checkpoint.recovery_ref.v3";
pub(crate) const CHECKPOINT_RECOVERY_REF_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0008_0001),
    CHECKPOINT_RECOVERY_REF_NAMESPACE,
);
pub(crate) const CHECKPOINT_GC_STATE_NAMESPACE: &str = "checkpoint.gc_state.v1";
pub(crate) const CHECKPOINT_GC_STATE_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_0002), CHECKPOINT_GC_STATE_NAMESPACE);
const CHECKPOINT_RECOVERY_REF_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_GC_STATE_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_GC_STATE_KEY: &[u8] = b"repository";
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
    /// Live commit-manifest count observed by the last successful reclaim.
    ///
    /// Approximate between sweeps and trued up by every sweep, which already
    /// enumerates the plane. This is a trigger heuristic and never an
    /// authority: drift costs a slightly early or late sweep and nothing else.
    pub(crate) live_manifest_estimate: u64,
    /// Retired commits per collectible interval observed by the last
    /// successful reclaim. Same status: approximate and self-correcting.
    pub(crate) yield_per_interval_estimate: u64,
    /// Consecutive reclaim attempts that failed before reaching
    /// `mark_collected`. Damps the retry so a persistently failing sweep
    /// cannot re-arm a full repository pass on every checkpoint.
    pub(crate) consecutive_reclaim_failures: u64,
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

    /// Records one successful reclaim and re-derives both trigger estimates
    /// from what that sweep actually observed.
    pub(crate) fn mark_collected(&mut self, reclaimed_commits: u64, live_manifest_count: u64) {
        let intervals = self.collectible_interval_count.max(1);
        self.yield_per_interval_estimate = reclaimed_commits / intervals;
        self.live_manifest_estimate = live_manifest_count;
        self.last_gc_sequence = self.checkpoint_sequence;
        self.collectible_interval_count = 0;
        self.consecutive_reclaim_failures = 0;
    }

    /// Records one reclaim attempt that did not reach [`Self::mark_collected`].
    pub(crate) fn note_reclaim_failure(&mut self) {
        self.consecutive_reclaim_failures = self.consecutive_reclaim_failures.saturating_add(1);
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
    live_manifest_estimate: u64,
    yield_per_interval_estimate: u64,
    consecutive_reclaim_failures: u64,
}

/// One authenticated checkpoint replacement that is still pending physical
/// retirement.
///
/// The replacement is read straight off the branch's canonical recovery ref,
/// which the checkpoint transaction rotates in the same atomic write set that
/// publishes the checkpoint commit. There is no separate publication ledger to
/// consult, and therefore no second place a stale replacement can survive.
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
            live_manifest_estimate: state.live_manifest_estimate,
            yield_per_interval_estimate: state.yield_per_interval_estimate,
            consecutive_reclaim_failures: state.consecutive_reclaim_failures,
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

/// Resolves a still-pending checkpoint replacement for an explicit branch
/// source, or proves that the source remains reachable through ordinary
/// canonical chronology.
///
/// A checkpoint replacement is accepted only when a branch's canonical
/// recovery ref binds `source_commit_id -> checkpoint_commit_id` and that
/// branch's live control still serves the same checkpoint. Recovery-ref
/// rotation, checkpoint publication and the control move are one atomic write
/// set, so this single read is the whole authority; branch publication holds
/// the branch-control precondition that makes the observation binding.
pub(crate) async fn resolve_pending_checkpoint_replacement<S>(
    store: &S,
    source_commit_id: CommitId,
) -> Result<Option<PendingCheckpointReplacement>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let controls = BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?;
    let mut candidates = Vec::new();
    for recovery in load_recovery_refs(store).await? {
        if recovery.recovered_head_commit_id != source_commit_id {
            continue;
        }
        let Some((_, control)) = controls
            .iter()
            .find(|(branch_id, _)| branch_id == &recovery.branch_id)
        else {
            continue;
        };
        if control.working_diff_checkpoint_commit_id == Some(recovery.checkpoint_commit_id) {
            candidates.push((recovery.checkpoint_commit_id, recovery.branch_id));
        }
    }
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
    // Loading the closure is the point: the audit must fail exactly where the
    // planner would, so a fixture that cannot be planned is never reported as
    // a clean standalone-change inventory.
    let _closure = load_authenticated_repository_retention(store, &controls).await?;
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
            } else {
                "unclassified_no_live_control"
            };
            if reason == "unclassified_no_live_control" {
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
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
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
        if !page_has_more {
            break;
        }
    }
    Ok(refs.into_values().collect())
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
        live_manifest_estimate: stored.live_manifest_estimate,
        yield_per_interval_estimate: stored.yield_per_interval_estimate,
        consecutive_reclaim_failures: stored.consecutive_reclaim_failures,
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
    /// Live commit manifests this sweep had to scan to plan. Feeds the reclaim
    /// trigger's inventory estimate; in-memory only, never persisted here.
    pub(crate) live_manifest_count: u64,
    pub(crate) tracked_commit_roots: Vec<CommitId>,
    pub(crate) standalone_changes: Vec<ChangeId>,
    /// Derived serving rows reclaimed from branch generations that no live
    /// branch control selects any more.
    pub(crate) reclaimed_generation_rows: u64,
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
    /// Graph-reachable commits this sweep found with no physical manifest --
    /// history a sweep predating the history-retention fix already reclaimed.
    /// Reported so the condition is observable; never a retention input.
    pub(crate) history_manifests_missing: u64,
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
    mutation_nodes: BTreeSet<[u8; 32]>,
    scoped_nodes: BTreeSet<[u8; 32]>,
    native_parts: BTreeSet<[u8; 32]>,
    /// Payload-ref summary digests of the retained native current-state parts.
    ///
    /// The scoped-range descriptor is the only place the `content_digest ->
    /// payload_refs_digest` pairing exists, so the walk has to carry it out.
    /// Rediscovering it later would mean re-reading the scoped-range trees.
    native_part_refs_digests: BTreeMap<[u8; 32], [u8; 32]>,
    /// Graph-reachable commits with no physical manifest, i.e. commits whose
    /// history delta a pre-fix sweep already reclaimed. Counted so the
    /// condition is observable; never a retention input.
    history_manifests_missing: u64,
}

async fn load_authenticated_serving_dependency_closure<S>(
    store: &S,
    chronology_roots: BTreeSet<CommitId>,
    serving_dependencies: BTreeSet<CommitId>,
    history_dependencies: BTreeSet<CommitId>,
    // Commits reachable from the roots through canonical parent links.
    //
    // These retain **both** planes. It is tempting to retain only the semantic
    // projection here and let the physical delta segments go, on the grounds
    // that compaction is supposed to free them — that is what this code did,
    // and it silently truncated row history. A row `_history()` row is
    // served out of the per-commit delta, not out of the projection:
    // `CommitGraphContext::change_history_from_commit` walks the graph and then
    // calls `load_member_changes`, which reads
    // `load_commit_delta_members_with_payloads_for_schemas`. That function
    // returns an empty member list for a commit whose replay state is gone, so
    // a commit whose delta was retired is indistinguishable from a commit that
    // changed nothing, and the history rows disappear without an error.
    //
    // What compaction frees is the *unreachable* interior: an intra-interval
    // commit stops being on the canonical parent chain once the checkpoint
    // supersedes it, so it never enters this set and is still retired. The set
    // retained here is the checkpoint chain, which compaction shortens.
    graph_reachable: BTreeSet<CommitId>,
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
    // Retain the delta of every graph-reachable commit that still has one.
    //
    // Tolerating absence is deliberate, and it is scoped to exactly this set.
    // A repository swept by the code that shipped before this fix has commits
    // that are graph-reachable and have no manifest, because that sweep deleted
    // it; those manifests are gone and cannot be recomputed. Demanding one here
    // would abort every future sweep on such a repository -- silently, because
    // `collect_checkpoint_garbage_best_effort` swallows the error, so
    // collection would simply stop forever while writes kept succeeding.
    //
    // Nothing else is relaxed. Serving dependencies, selected-source owners and
    // physical authorities keep their existing hard demand, so a manifest
    // missing from any of those classes still fails the sweep exactly as it
    // does today. Within this set we *cannot* tell a commit swept before the
    // fix from a manifest missing for a reason that should not happen -- after
    // this fix no graph-reachable commit is ever swept, so absence is either
    // legacy or corruption and nothing at hand separates them. Both are
    // tolerated and both are counted, so the condition is observable rather
    // than silent.
    let graph_reachable_ids = graph_reachable.iter().copied().collect::<Vec<_>>();
    let graph_reachable_manifests =
        crate::tracked_state::load_commit_state_manifests(store, &graph_reachable_ids).await?;
    let mut history_manifests_missing = 0u64;
    for (commit_id, manifest) in graph_reachable_ids
        .into_iter()
        .zip(graph_reachable_manifests)
    {
        if manifest.is_some() {
            physical_dependencies.insert(commit_id);
        } else {
            history_manifests_missing += 1;
        }
    }
    if history_manifests_missing > 0 {
        tracing::warn!(
            history_manifests_missing,
            "repository contains commits whose history delta was reclaimed by a garbage \
             collection sweep predating the history-retention fix; their row history is \
             permanently truncated and cannot be recovered"
        );
    }
    semantic_dependencies.extend(graph_reachable);
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
    let mut native_part_refs_digests = BTreeMap::new();
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

    // Physical serving authority is recursive: a checkpoint can select a
    // member owned by another commit, whose body inventory can in turn name a
    // scoped/native source owned elsewhere. Resolve that authenticated graph
    // to a fixed point before planning any deletion. A one-pass ordering can
    // retain the owner manifest but still sweep the body object it certifies.
    let mut selected_owner_scanned = BTreeSet::new();
    loop {
        let before = (
            manifests.len(),
            physical_authorities.len(),
            physical_dependencies.len(),
            scoped_nodes.len(),
            native_parts.len(),
        );

        let required_ids = physical_authorities
            .union(&physical_dependencies)
            .copied()
            .filter(|commit_id| !manifests.contains_key(commit_id))
            .collect::<Vec<_>>();
        let required_manifests =
            crate::tracked_state::load_commit_state_manifests(store, &required_ids).await?;
        for (commit_id, manifest) in required_ids.into_iter().zip(required_manifests) {
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

        let selected_owner_sources = manifests
            .iter()
            .filter_map(|(commit_id, manifest)| {
                (manifest.mutations.may_contain_finite_selected_members()
                    && !selected_owner_scanned.contains(commit_id))
                .then_some(*commit_id)
            })
            .collect::<Vec<_>>();
        for commit_id in selected_owner_sources {
            selected_owner_scanned.insert(commit_id);
            physical_authorities.extend(
                crate::tracked_state::load_local_selected_change_owner_commit_ids(store, commit_id)
                    .await?,
            );
        }

        let scoped_roots = manifests
            .values()
            .filter_map(|manifest| {
                manifest
                    .current_state_scoped_ranges
                    .as_ref()
                    .map(|root| root.tree.clone())
            })
            .collect::<Vec<_>>();
        let mut descriptors = manifests
            .values()
            .filter_map(|manifest| manifest.authored_history_bodies.as_ref())
            .flat_map(|inventory| inventory.descriptors.iter().cloned())
            .collect::<Vec<_>>();
        if !scoped_roots.is_empty() {
            let reachable =
                crate::tracked_state::validate_scoped_range_trees(store, &scoped_roots).await?;
            scoped_nodes.extend(reachable.node_ids);
            descriptors.extend(
                reachable
                    .parts
                    .iter()
                    .map(crate::tracked_state::current_state_descriptor_from_scoped_range_part)
                    .collect::<Result<Vec<_>, LixError>>()?,
            );
        }
        for descriptor in descriptors {
            match descriptor.source {
                crate::tracked_state::CurrentStatePartSource::Replacement(source) => {
                    physical_authorities.insert(CommitId::new(uuid::Uuid::from_bytes(
                        source.owner_commit_id,
                    )));
                }
                crate::tracked_state::CurrentStatePartSource::ColumnarPage(source) => {
                    physical_authorities.insert(CommitId::new(uuid::Uuid::from_bytes(
                        source.owner_commit_id,
                    )));
                }
                crate::tracked_state::CurrentStatePartSource::NativeDataPart {
                    payload_refs_digest,
                } => {
                    native_parts.insert(descriptor.content_digest);
                    if let Some(previous) = native_part_refs_digests
                        .insert(descriptor.content_digest, payload_refs_digest)
                        && previous != payload_refs_digest
                    {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "native current-state descriptors disagree about payload refs",
                        ));
                    }
                }
            }
        }
        physical_dependencies.extend(
            crate::tracked_state::load_native_current_state_part_owners(store, &native_parts)
                .await?,
        );

        let after = (
            manifests.len(),
            physical_authorities.len(),
            physical_dependencies.len(),
            scoped_nodes.len(),
            native_parts.len(),
        );
        if before == after
            && physical_authorities
                .union(&physical_dependencies)
                .all(|commit_id| manifests.contains_key(commit_id))
        {
            break;
        }
    }
    // Native body objects carry immutable row provenance and may outlive the
    // semantic commit projection that authored them. Retain their physical
    // manifest owner, but do not turn a body-object back-edge into semantic
    // commit reachability; checkpoint compaction is allowed to retire that
    // projection while current state and history keep the canonical bytes.

    let retained_physical_ids = physical_authorities
        .union(&physical_dependencies)
        .copied()
        .collect::<Vec<_>>();

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
        mutation_nodes,
        scoped_nodes,
        native_parts,
        native_part_refs_digests,
        history_manifests_missing,
    })
}

async fn load_authenticated_repository_retention<S>(
    store: &S,
    controls: &[(String, BranchHeadControl)],
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
    let graph_reachable = collect_ref_reachable_commit_ids(store, &chronology_roots).await?;
    load_authenticated_serving_dependency_closure(
        store,
        chronology_roots,
        control_reachability.serving_dependencies,
        control_reachability.history_dependencies,
        graph_reachable,
    )
    .await
}

/// Every commit reachable from the authenticated roots through canonical parent
/// links.
///
/// This is the reachability the public history surfaces actually read: a
/// `_history()` query walks the commit graph, so a commit on that chain is
/// load-bearing no matter how far below the serving checkpoint it sits — and
/// load-bearing in **both** planes, because a row history row is served out
/// of the commit delta while only the commit metadata comes from the
/// projection. Retaining the projection alone leaves the walk finding the
/// commit and reading zero members from it, which is silent truncation. The
/// ledger expressed the same retention by pinning every checkpoint commit it
/// had ever seen, forever, in a row it could never consume. Walking refs states
/// it directly, costs one commit-record read per reachable commit, and shrinks
/// as compaction shortens the chain.
async fn collect_ref_reachable_commit_ids<S>(
    store: &S,
    roots: &BTreeSet<CommitId>,
) -> Result<BTreeSet<CommitId>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let mut graph = CommitGraphContext::new().reader(store);
    let mut reachable = BTreeSet::new();
    let mut pending = roots.iter().copied().collect::<Vec<_>>();
    while let Some(commit_id) = pending.pop() {
        if !reachable.insert(commit_id) {
            continue;
        }
        let Some(node) = graph.load_node(&commit_id).await? else {
            continue;
        };
        pending.extend(node.parent_commit_ids.iter().copied());
    }
    Ok(reachable)
}

/// Derives this sweep's retirement candidates.
///
/// This replaces the `gc.reachability_delta.v1` publication ledger. That ledger
/// stored one ~439 B row per branch-head publication recording an `old_root ->
/// new_root` transition so a later sweep would know which commit had been
/// superseded. The manifest plane already states the same thing and states it
/// better: a commit owns a manifest exactly while it owns physical state, and
/// [`crate::tracked_state::stage_retire_commit_physical_state`] deletes it. So
/// the inventory *is* the outstanding-work list, it shrinks as work completes,
/// and it needs no publication-time write at all.
///
/// Deliberately not a walk from refs. A commit can stop being reachable from
/// any ref while still owning physical state — every commit of a deleted branch
/// is in that position — and a refs walk cannot name those at all. Liveness
/// still comes only from refs: this list is filtered against the authenticated
/// retention closure, and nothing here is ever treated as a root.
///
/// The list is unordered on purpose. The ledger consumed a queue in
/// publication order and stopped at the first blocked row, so one permanently
/// live root at the head — genesis always is one, because engine-bootstrap rows
/// authored at init stay live and keep naming it — froze every younger
/// candidate behind it forever. Here a pinned candidate holds back only itself.
async fn derive_retirement_candidates<S>(store: &S) -> Result<Vec<CommitId>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    crate::tracked_state::scan_commit_state_manifest_commit_ids(store).await
}

/// Plans which out-of-band JSON payloads this sweep may reclaim.
///
/// Every owner class that can hold a `JsonSlot::Ref` after the write set
/// commits is enumerated here, and each one is reached from the walk rather
/// than from a scan of the payload plane or of the changelog:
///
/// 1. **Published hot rows.** The serving plane of every live branch
///    generation, tracked rows included. Untracked rows are the only owner of
///    their payload; tracked rows are a derived cache, but a serving read
///    materializes a payload straight out of this plane, so a ref here that no
///    longer resolves is a read failure.
/// 2. **Retained native current-state parts.** Their payload-ref summaries are
///    carried out of the scoped-range walk by the retention closure.
/// 3. **Surviving commit deltas.** `surviving_commits` is exactly the candidate
///    set this sweep could not prove retirable, which is exactly the set of
///    physical manifests still standing afterwards, so this is a bounded
///    per-commit inventory walk over the commits GC has already decided to
///    keep — not a repository-global commit scan.
/// 4. **Standalone branch-ref facts.** The shipping sweep never deletes a
///    standalone change, so the fact each control names stays an owner. Branch
///    ref snapshots are small enough to inline today; enumerating them anyway
///    costs one point read per branch and removes the dependence on that.
///
/// Being a superset here is always safe and being a subset never is, so where
/// the two arguments were close — the tracked hot rows, the branch-ref facts —
/// this deliberately takes the wider set.
struct JsonPayloadReclamation {
    live: BTreeSet<[u8; 32]>,
    sweep: Vec<JsonRef>,
}

async fn plan_json_payload_reclamation<S>(
    store: &S,
    controls: &[(String, BranchHeadControl)],
    retired_commits: &BTreeSet<CommitId>,
    surviving_commits: &BTreeSet<CommitId>,
    released_part_refs_digests: &BTreeMap<[u8; 32], [u8; 32]>,
    retained_part_refs_digests: &BTreeMap<[u8; 32], [u8; 32]>,
) -> Result<JsonPayloadReclamation, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    // Every await here is boxed. This planner is reached from
    // `stage_repository_gc_with_preconditions`, whose future is already close
    // to the test harness's 2 MiB worker stack; inlining these state machines
    // aborted `cas_gc_history_retention` with a stack overflow, which passes on
    // the parent commit. Keep them boxed.
    //
    // Candidates: named only by state this sweep is deleting.
    let mut candidates = BTreeSet::new();
    for commit_id in retired_commits {
        Box::pin(crate::tracked_state::collect_local_commit_delta_json_refs(
            store,
            *commit_id,
            &mut candidates,
        ))
        .await?;
    }
    Box::pin(crate::tracked_state::collect_current_state_part_json_refs(
        store,
        released_part_refs_digests,
        &mut candidates,
    ))
    .await?;

    // Live: named by state that outlives this sweep.
    let mut live = BTreeSet::new();
    Box::pin(
        TrackedHeadContext::new()
            .reader(store.clone())
            .collect_hot_json_refs(controls, false, &mut live),
    )
    .await?;
    Box::pin(crate::tracked_state::collect_current_state_part_json_refs(
        store,
        retained_part_refs_digests,
        &mut live,
    ))
    .await?;
    for commit_id in surviving_commits {
        Box::pin(crate::tracked_state::collect_local_commit_delta_json_refs(
            store, *commit_id, &mut live,
        ))
        .await?;
    }
    let ref_change_ids = controls
        .iter()
        .map(|(_, control)| control.ref_change_id)
        .collect::<BTreeSet<_>>();
    for record in Box::pin(crate::changelog::load_change_records(
        store,
        ref_change_ids.into_iter(),
    ))
    .await?
    .values()
    {
        for slot in [&record.snapshot, &record.metadata] {
            if let JsonSlot::Ref(json_ref) = slot {
                live.insert(*json_ref.as_hash_array());
            }
        }
    }

    let sweep = candidates
        .difference(&live)
        .copied()
        .map(JsonRef::from_hash_bytes)
        .collect::<Vec<_>>();
    Ok(JsonPayloadReclamation { live, sweep })
}

/// Plans and stages logical repository GC against one pinned read.
///
/// The caller must serialize this operation with repository writes and commit
/// `writes` atomically. Planning and mutation are deliberately separated from
/// storage commit so checkpoint/session code can retain lifecycle control.
/// Content-addressed tree/CAS orphan repair is intentionally an offline path.
///
/// **Out-of-band JSON payloads are reclaimed here**, by descending the same
/// walk one level further rather than by any second authority. Until this
/// change the plane leaked at **one payload per superseded edit**, independent
/// of checkpoint cadence: measured by `e2e/examples/e1_json_leak.rs` over a
/// shape x cadence x edits matrix, rewriting one row 1000 times left 1004
/// payload rows where 1 was live, at every cadence (never / every 10 / every
/// 100). The `insert` control arm, where every payload stays live, leaked
/// exactly 3 rows at every size, so `leaked = edits + 3` and the rewrite arm's
/// growth was the superseded payloads and nothing else.
///
/// **That baseline is only meaningful because the probe checkpoints.** Without
/// one the sweep proves *nothing* retirable -- 0 commit-state manifests retired
/// across 1000 edits -- so "0 payloads reclaimed" was equally consistent with
/// "the sweep had no work to do". With a checkpoint every 10 edits the same
/// stream retires 1095 manifests, which is what established that the owning
/// commits really were being retired underneath the payloads.
///
/// The reclamation keeps the plane's two halves on opposite sides of the
/// retirement decision. A hash becomes a **candidate** only because this write
/// set is deleting the row that named it — a retired commit's own delta
/// members, or a native current-state part whose payload-ref summary this
/// retirement removes. It is **live** if any owner that outlives the sweep
/// names it; see [`plan_json_payload_reclamation`] for that enumeration.
/// Neither half scans the payload plane, so an unreferenced row that no
/// retirement produced is left alone rather than swept on the strength of a
/// live set being complete.
///
/// Ordinary GC derives its candidates from the physical manifest inventory and
/// proves liveness only from refs: branch-head controls and checkpoint recovery
/// refs are the complete active-root set, and the walk from those roots through
/// canonical parent links, plus the exact point-replay dependency closure, is
/// the only reachability implementation. Semantic *inventory* discovery — a
/// changelog or commit-space scan that rediscovers liveness — remains
/// forbidden; the manifest scan is an inventory of the physical state being
/// collected, never a source of retention.
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
    let AuthenticatedServingDependencyClosure {
        chronology_roots: active_roots,
        physical_authorities: active_authority_ids,
        physical_dependencies: active_dependency_ids,
        semantic_dependencies: active_semantic_dependency_ids,
        cas_logical_dependencies: active_cas_dependency_ids,
        mutation_nodes: active_mutation_nodes,
        scoped_nodes: active_scoped_nodes,
        native_parts: active_current_parts,
        native_part_refs_digests: active_current_part_refs_digests,
        history_manifests_missing,
    } = load_authenticated_repository_retention(&store, &controls).await?;

    // Retirement candidates are derived here, not read from a ledger.
    let candidates = derive_retirement_candidates(&store).await?;
    // The inventory this sweep had to scan. Feeds the reclaim trigger's
    // self-correcting denominator; captured before `candidates` is consumed.
    let live_manifest_count = candidates.len() as u64;

    // Derive both physical retirement and logical CAS retention from the one
    // authenticated serving closure. In particular, do not perform a second
    // replay-graph walk for CAS: a candidate root is retained exactly when it
    // is already a dependency of the active closure. This makes the retained
    // owner set deterministic and prevents a separately reconstructed CAS
    // authority from racing semantic projection retirement.
    let mut blocked_physical_dependency_ids = BTreeSet::new();
    let mut blocked_history_dependency_ids = BTreeSet::new();
    for commit_id in candidates.iter().copied() {
        if !retirement_is_proven(commit_id, &active_authority_ids, &active_dependency_ids) {
            blocked_physical_dependency_ids.insert(commit_id);
        }
        if !retirement_is_proven(commit_id, &active_roots, &active_cas_dependency_ids) {
            blocked_history_dependency_ids.insert(commit_id);
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
        crate::plugin::runtime::collect_gc_wasm_blob_roots(
            &store,
            &controls,
            &retained_cas_root_ids,
        )
        .await?,
    );
    let upload_chunks =
        crate::session::stage_reclaimable_upload_receipts(&store, writes, &blob_roots).await?;
    let binary_cas =
        crate::binary_cas::stage_gc_reclamation(&store, writes, &blob_roots, &upload_chunks)
            .await?;
    crate::binary_cas::stage_cas_reclamation_fence(&store, writes, &mut staged_preconditions)
        .await?;

    // Retire the derived candidates. Nothing here is ordered and nothing is
    // capped: a candidate that is still pinned holds back only itself, and the
    // two planes prove retirement independently. The ledger could do neither —
    // it consumed a queue in publication order and stopped at the first blocked
    // row, so one permanently live root at the head froze every younger
    // candidate behind it forever.
    let mut reclaimed_commits = BTreeSet::new();
    let mut reclaimed_semantic_commits = BTreeSet::new();
    let mut released_part_refs_digests = BTreeMap::new();
    for commit_id in candidates {
        if retirement_is_proven(commit_id, &active_roots, &active_semantic_dependency_ids)
            && reclaimed_semantic_commits.insert(commit_id)
        {
            crate::changelog::stage_delete_commit_projection(&store, writes, commit_id).await?;
        }
        if blocked_physical_dependency_ids.contains(&commit_id) {
            continue;
        }
        if reclaimed_commits.insert(commit_id) {
            crate::tracked_state::stage_retire_commit_physical_state(
                &store,
                writes,
                commit_id,
                RetainedPhysicalState {
                    mutation_nodes: &active_mutation_nodes,
                    scoped_nodes: &active_scoped_nodes,
                    native_parts: &active_current_parts,
                },
                &mut released_part_refs_digests,
            )
            .await?;
        }
    }
    // One boxed step, not three awaits inlined here. This function's future is
    // already close to the harness's 2 MiB worker stack: three inline await
    // points over the segment-decode and hot-scan chains aborted
    // `cas_gc_history_retention` with a stack overflow, which passes on the
    // parent commit.
    let JsonPayloadReclamation {
        live: live_json_hashes,
        sweep: sweep_json_payloads,
    } = Box::pin(plan_json_payload_reclamation(
        &store,
        &controls,
        &reclaimed_commits,
        &blocked_physical_dependency_ids,
        &released_part_refs_digests,
        &active_current_part_refs_digests,
    ))
    .await?;
    if !sweep_json_payloads.is_empty() {
        JsonStoreContext::new()
            .writer()
            .stage_delete_refs(writes, sweep_json_payloads.iter().copied());
        Box::pin(crate::json_store::stage_json_reclamation_fence(
            &store,
            writes,
            &mut staged_preconditions,
        ))
        .await?;
    }
    if !reclaimed_semantic_commits.is_empty() {
        writes.seal_changelog_gc();
    }

    // Checkpoint publication rotates the authenticated sparse dirty-index
    // marker in O(1). Retire every now-unreachable epoch here, under the same
    // observed branch-control preconditions as the rest of repository GC.
    stage_collect_stale_working_diff_indexes(&store, writes).await?;

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
                payloads: live_json_hashes
                    .into_iter()
                    .map(JsonRef::from_hash_bytes)
                    .collect(),
            },
            sweep: GcSweepSet {
                commits: Vec::new(),
                commit_change_ids: Vec::new(),
                changes: Vec::new(),
                json_payloads: sweep_json_payloads,
            },
            repair: GcRepairSet::default(),
        },
        sweep: RepositoryGcSweep {
            live_manifest_count,
            tracked_commit_roots: reclaimed_commits.into_iter().collect(),
            // Superseded branch-ref facts and stale serving generations are
            // retired by the publication that supersedes them, in that same
            // write set. A sweep has no such debt left to report.
            standalone_changes: Vec::new(),
            reclaimed_generation_rows: 0,
            binary_cas,
        },
        profile: RepositoryGcProfile {
            history_manifests_missing,
            root_discovery_us: elapsed_micros(started),
            changelog_us: 0,
            tracked_root_stage_us: 0,
            total_us: elapsed_micros(started),
        },
    })
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
    // Checkpoint publication leaves prior dirty-index generations unreachable
    // in O(1). Reclaim those auxiliary records only in the asynchronous GC
    // pass so a foreground checkpoint never pays a history-sized delete cost.
    stage_collect_stale_working_diff_indexes(&store, writes).await?;
    let tracked_root_stage_us = elapsed_micros(phase_started);

    Ok(RepositoryGcPlan {
        changelog: changelog_plan,
        sweep: RepositoryGcSweep {
            // The recovery-only rebuild path never feeds the reclaim trigger;
            // it rediscovers liveness by scanning rather than from the
            // manifest inventory, so it has no inventory figure to report.
            live_manifest_count: 0,
            tracked_commit_roots: swept_snapshot_authorities,
            standalone_changes: Vec::new(),
            reclaimed_generation_rows: 0,
            binary_cas: Default::default(),
        },
        profile: RepositoryGcProfile {
            // The recovery-only rebuild path rediscovers liveness by scanning;
            // it does not consult the graph-reachable manifest set at all.
            history_manifests_missing: 0,
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
        match descriptor.source {
            crate::tracked_state::CurrentStatePartSource::Replacement(source) => {
                let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
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
            crate::tracked_state::CurrentStatePartSource::NativeDataPart {
                payload_refs_digest,
            } => {
                live_current_state_data_parts.insert(descriptor.content_digest);
                if let Some(previous) = live_current_state_ref_summaries
                    .insert(descriptor.content_digest, payload_refs_digest)
                    && previous != payload_refs_digest
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "native scoped-range descriptors disagree about payload refs",
                    ));
                }
            }
            crate::tracked_state::CurrentStatePartSource::ColumnarPage(source) => {
                let owner = CommitId::new(uuid::Uuid::from_bytes(source.owner_commit_id));
                if !packed.commits.contains_key(&owner) {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("live scoped range references missing columnar owner '{owner}'"),
                    ));
                }
                live_columnar_sources.insert((owner, source.source_id, descriptor.content_digest));
                retained_authority_commits.insert(owner);
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

    crate::changelog::stage_delete_commits(writes, sweep_commits.iter().copied());
    crate::tracked_state::stage_sweep_unreachable_content_nodes(
        store,
        writes,
        RetainedPhysicalState {
            mutation_nodes: &live_mutation_directory_nodes,
            scoped_nodes: &live_scoped_range_nodes,
            native_parts: &live_current_state_data_parts,
        },
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
                    crate::hot_state::row_group_set_id(*commit_id, schema_key),
                )
                .await?;
            }
            crate::tracked_state::stage_delete_commit_delta_inventory_entry(
                writes, *commit_id, entry,
            )?;
        }
    }
    crate::changelog::stage_delete_changes(writes, sweep_changes.iter().copied());
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
            let change_id = commit.change_id();
            commits
                .parent_commit_ids
                .extend(commit.parent_commit_ids.into_iter());
            commits.entries.push(GcCommitInventoryEntry {
                commit_id: commit.commit_id,
                change_id,
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
    candidate: CommitId,
    active_authority_ids: &BTreeSet<CommitId>,
    active_dependency_ids: &BTreeSet<CommitId>,
) -> bool {
    !active_authority_ids.contains(&candidate) && !active_dependency_ids.contains(&candidate)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
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
    use crate::row_pk::RowPk;
    use crate::hot_state::{CurrentStateDeltaRef, TrackedHeadContext, WorkingDiffIndexCoverage};
    use crate::json_store::{
        JsonRef, JsonSlot, JsonSlotRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJson,
        NormalizedJsonRef,
    };
    use crate::storage_adapter::{
        MAX_SCAN_PAGE_ROWS, Memory, PointReadPlan, SharedStorageAdapterRead, StorageAdapter,
        StorageBeginScanOptions, StorageCoreProjection, StorageGetOptions, StorageKey,
        StoragePrefix, StorageReadOptions, StorageSpace, StorageValue, StorageWriteOptions,
        StorageWriteSet,
    };
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
        CHECKPOINT_GC_STATE_SPACE, CheckpointGcState, CheckpointRecoveryRef,
        authenticated_control_commit_reachability, derive_retirement_candidates,
        load_checkpoint_gc_state, load_recovery_ref, load_recovery_refs,
        resolve_pending_checkpoint_replacement, retirement_is_proven, stage_checkpoint_gc_state,
        stage_delete_recovery_ref, stage_recovery_ref_rotation,
    };

    async fn space_inventory<R>(read: &R, space: StorageSpace) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let range = StoragePrefix {
            bytes: Bytes::new(),
        }
        .to_range()
        .expect("valid empty test inventory prefix");
        let mut cursor = read
            .begin_scan(
                space,
                range,
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("begin test inventory scan");
        let mut inventory = Vec::new();
        loop {
            let (page, has_more) = cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await
                .expect("scan complete test inventory")
                .into_parts();
            inventory.extend(page.into_iter().map(|entry| {
                let value = match entry.value {
                    crate::storage_adapter::StorageProjectedValue::KeyOnly => Vec::new(),
                    crate::storage_adapter::StorageProjectedValue::FullValue(value) => {
                        value.to_vec()
                    }
                };
                (entry.key.0.to_vec(), value)
            }));
            if !has_more {
                return inventory;
            }
        }
    }

    #[tokio::test]
    async fn destructive_consumers_share_the_complete_tracked_control_projection() {
        let head = CommitId::with_change_address_space(
            *CommitId::for_test_label("control-projection-head").as_uuid(),
        );
        let tracked_generation = CommitId::for_test_label("control-projection-tracked");
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
    }

    /// Builds `old_root -> active` with the head at `active`, optionally
    /// omitting `old_root`'s physical manifest to model a repository already
    /// swept by the code that shipped before the history-retention fix.
    async fn history_retention_fixture(with_old_manifest: bool) -> super::RepositoryGcPlan {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("history retention timestamp", "2026-01-01T00:00:00Z");
        let old_root = replay_commit_record("history-retained-old", 0, None, timestamp);
        let active = replay_commit_record(
            "history-retained-active",
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

        let control_ref = ChangeId::for_test_label("history-retained-control");
        let active_control = replay_branch_control(active.commit_id, control_ref, timestamp);
        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, "main", active_control)
            .expect("history retention control should stage");
        let manifests = if with_old_manifest {
            vec![old_manifest, active_manifest]
        } else {
            vec![active_manifest]
        };
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[old_root.clone(), active.clone()],
            &manifests,
        )
        .await;

        let plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            !plan
                .sweep
                .tracked_commit_roots
                .contains(&old_root.commit_id),
            "an ancestor of the live head is what _history() reads; it must not be retired"
        );
        plan
    }

    /// The fix: a commit reachable from the head through parent links keeps its
    /// physical delta, because that delta is what a row `_history()` row is
    /// served out of.
    #[tokio::test]
    async fn ordinary_gc_retains_the_delta_of_a_graph_reachable_commit() {
        let plan = history_retention_fixture(true).await;
        assert_eq!(
            plan.profile.history_manifests_missing, 0,
            "a repository with intact history reports nothing missing"
        );
    }

    /// The tolerance, and its inversion against the test above: the same
    /// repository with that manifest already reclaimed by a pre-fix sweep must
    /// still complete a sweep, and must say so rather than tolerating silently.
    ///
    /// Demanding the manifest here would abort every future sweep on such a
    /// repository, and `collect_checkpoint_garbage_best_effort` swallows the
    /// error, so collection would stop permanently while writes kept
    /// succeeding. Worse, `checkpoint_gc_due` derives its age limit from
    /// `last_gc_sequence`, which only a *successful* sweep advances, so the
    /// due-predicate would latch: every later checkpoint would pay for a
    /// doomed full-repository sweep, forever.
    #[tokio::test]
    async fn ordinary_gc_tolerates_and_counts_history_reclaimed_before_the_fix() {
        let plan = history_retention_fixture(false).await;
        assert_eq!(
            plan.profile.history_manifests_missing, 1,
            "the commit whose delta a pre-fix sweep reclaimed must be counted, not swallowed"
        );
    }

    #[tokio::test]
    async fn ordinary_gc_accepts_rootless_tracked_serving_generation() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp = LixTimestamp::expect_parse(
            "rootless serving generation timestamp",
            "2026-01-01T00:00:00Z",
        );
        // The checkpoint parents onto a fresh interval base rather than onto
        // `old_root`, which is what compaction actually produces: `old_root` is an
        // interior commit of the interval the checkpoint closes, so the
        // checkpoint supersedes it and it leaves the first-parent chain.
        //
        // This fixture used to parent the checkpoint directly onto `old_root` --
        // a raw parent chain no real checkpoint ever produces. That kept
        // `old_root` reachable from the live head forever, and a commit reachable
        // from the head now keeps its delta because that delta is what an
        // row `_history()` row is served out of. The release under test here
        // is a rootless tracked serving generation, not graph reachability, so the fixture models the
        // compaction instead of contradicting it.
        let base = replay_commit_record("rootless-serving-base", 0, None, timestamp);
        let old_root =
            replay_commit_record("rootless-serving-old", 1, Some(base.commit_id), timestamp);
        let active = replay_commit_record(
            "rootless-serving-active",
            1,
            Some(base.commit_id),
            timestamp,
        );
        let mut base_manifest =
            test_commit_state_manifest(&base, CommitStateMutationInventory::default());
        base_manifest.replay_debt = CommitStateReplayDebt::default();
        base_manifest.snapshot_root = Some(Box::new(test_snapshot_root(base.commit_id)));
        let mut old_manifest =
            test_commit_state_manifest(&old_root, CommitStateMutationInventory::default());
        old_manifest.replay_debt = CommitStateReplayDebt::default();
        old_manifest.snapshot_root = Some(Box::new(test_snapshot_root(old_root.commit_id)));
        let mut active_manifest =
            test_commit_state_manifest(&active, CommitStateMutationInventory::default());
        active_manifest.replay_debt = CommitStateReplayDebt::default();
        active_manifest.snapshot_root = Some(Box::new(test_snapshot_root(active.commit_id)));

        let control_ref = ChangeId::for_test_label("rootless-serving-control");
        let _old_control = replay_branch_control(old_root.commit_id, control_ref, timestamp);
        let serving_generation = CommitId::for_test_label("rootless-serving-generation");
        let mut active_control = replay_branch_control(active.commit_id, control_ref, timestamp);
        active_control.tracked_generation = serving_generation;

        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, "main", active_control)
            .expect("rootless serving control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[base.clone(), old_root.clone(), active.clone()],
            &[base_manifest, old_manifest, active_manifest],
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
        // The interval base is still on the head's first-parent chain, so it is
        // what `_history()` reads and must survive the same sweep. Without this
        // the fixture would pass just as well against a collector that retires
        // the whole chain.
        assert!(
            !plan.sweep.tracked_commit_roots.contains(&base.commit_id),
            "a commit reachable from the live head owns row history and must not be retired"
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
            BTreeSet::new(),
        )
        .await
        .expect("rootless selected owner must remain valid physical authority");
        assert!(closure.physical_authorities.contains(&owner.commit_id));
        assert!(!closure.semantic_dependencies.contains(&owner.commit_id));
        assert!(!closure.cas_logical_dependencies.contains(&owner.commit_id));
    }

    // `crate::storage_bench` exists only under `feature = "storage-benches"`,
    // so every `#[cfg(test)]` item that reaches into it must carry the same
    // gate. Without it `cargo check -p lix --tests` — `cfg(test)` with the
    // feature off, which is what a plain `cargo test -p lix` builds — fails
    // to compile the whole lib test target, while `--all-features` and CI
    // stay green.
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn ordinary_gc_releases_finite_selected_owner_only_after_checkpoint_release() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("selected owner timestamp", "2026-01-01T00:00:00Z");
        // The checkpoint parents onto a fresh interval base rather than onto
        // `owner`, which is what compaction actually produces: `owner` is an
        // interior commit of the interval the checkpoint closes, so the
        // checkpoint supersedes it and it leaves the first-parent chain.
        //
        // This fixture used to parent the checkpoint directly onto `owner` --
        // a raw parent chain no real checkpoint ever produces. That kept
        // `owner` reachable from the live head forever, and a commit reachable
        // from the head now keeps its delta because that delta is what an
        // row `_history()` row is served out of. The release under test here
        // is the finite selected-source owner pin, not graph reachability, so the fixture models the
        // compaction instead of contradicting it.
        let base = replay_commit_record("selected-owner-base", 0, None, timestamp);
        let owner = replay_commit_record("selected-owner-source", 0, None, timestamp);
        let checkpoint = replay_commit_record(
            "selected-owner-checkpoint",
            1,
            Some(base.commit_id),
            timestamp,
        );
        let mut base_manifest =
            test_commit_state_manifest(&base, CommitStateMutationInventory::default());
        base_manifest.replay_debt = CommitStateReplayDebt::default();
        base_manifest.snapshot_root = Some(Box::new(test_snapshot_root(base.commit_id)));
        // `released` parents onto the interval base too, because the commit
        // holding the selection is itself interior. A commit that is still
        // graph-reachable keeps its delta, and that delta is what names the
        // selected source -- so while the selecting commit is on the chain the
        // owner is correctly pinned by it, and the release under test could
        // never be reached. Superseding the selector is the event that
        // releases the owner, which is exactly what this test is named for.
        let released = replay_commit_record(
            "selected-owner-released",
            1,
            Some(base.commit_id),
            timestamp,
        );
        let selected_change = packed_change(
            "selected-owner-change",
            "selected-owner-row",
            JsonSlot::Inline(r#"{"selected":true}"#.into()),
        );

        let mut writes = storage.new_write_set();
        let owner_deltas =
            commit_delta_refs(owner.commit_id, std::slice::from_ref(&selected_change));
        let owner_stage = stage_commit_deltas_for_commit_state(&mut writes, &owner_deltas)
            .expect("selected owner payload should stage");
        stage_change_locators(&mut writes, &owner_stage.locators);
        let selected_locator_change_id = owner_stage
            .locators
            .first()
            .expect("the owner stages one locator")
            .change_id;
        let mut checkpoint_deltas =
            commit_delta_refs(checkpoint.commit_id, std::slice::from_ref(&selected_change));
        checkpoint_deltas[0].authored = false;
        let checkpoint_stage =
            stage_commit_deltas_for_commit_state(&mut writes, &checkpoint_deltas)
                .expect("finite selected checkpoint member should stage");

        let owner_mutations = owner_stage.mutation_inventory().clone();
        let owner_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("selected owner certification read should open");
        let owner_body = crate::tracked_state::certify_authored_current_state_body(
            &owner_read,
            &mut writes,
            owner.commit_id,
            &owner.account_id,
            &owner_mutations,
            true,
            owner_deltas.iter().copied(),
        )
        .await
        .expect("selected owner body should certify");
        let mut owner_publication = crate::tracked_state::
            stage_current_state_scoped_ranges_from_published_topology_parent(
                &owner_read,
                &mut writes,
                None,
                owner.commit_id,
                &owner.account_id,
                &owner_mutations,
                owner_body,
            )
            .await
            .expect("selected owner current state should publish");
        owner_publication
            .certify_authored_history_bodies(
                &owner_read,
                &mut writes,
                &owner.account_id,
                &owner_mutations,
            )
            .await
            .expect("selected owner history body should certify");
        crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
            &mut writes,
            &CommitStateManifest {
                commit_id: owner.commit_id,
                change_account_id: owner.account_id.clone(),
                replay_debt: CommitStateReplayDebt::default(),
                mutations: owner_mutations,
                touched_scope_filter: owner_publication.touched_scope_filter().clone(),
                current_state_scoped_ranges: owner_publication.root(),
                authored_history_bodies: owner_publication.authored_history_bodies(),
                snapshot_root: Some(Box::new(test_snapshot_root(owner.commit_id))),
            },
            &owner_publication,
        )
        .expect("selected owner certified manifest should stage");
        drop(owner_read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("selected owner certified authority should commit");
        let mut writes = storage.new_write_set();
        let mut checkpoint_manifest =
            test_commit_state_manifest(&checkpoint, checkpoint_stage.mutation_inventory().clone());
        checkpoint_manifest.replay_debt = CommitStateReplayDebt::default();
        checkpoint_manifest.snapshot_root =
            Some(Box::new(test_snapshot_root(checkpoint.commit_id)));
        stage_empty_current_state_root(&mut writes, &mut checkpoint_manifest, None);
        let mut released_manifest =
            test_commit_state_manifest(&released, CommitStateMutationInventory::default());
        released_manifest.replay_debt = CommitStateReplayDebt::default();
        released_manifest.snapshot_root = Some(Box::new(test_snapshot_root(released.commit_id)));

        let control_ref = ChangeId::for_test_label("selected-owner-control");
        let _owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
        let checkpoint_control =
            replay_branch_control(checkpoint.commit_id, control_ref, timestamp);
        let released_control = replay_branch_control(released.commit_id, control_ref, timestamp);
        stage_branch_head_control(&mut writes, "main", checkpoint_control)
            .expect("checkpoint control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[
                base.clone(),
                owner.clone(),
                checkpoint.clone(),
                released.clone(),
            ],
            &[base_manifest, checkpoint_manifest],
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

        // The not-yet-published `released` commit is a candidate the moment its
        // manifest exists, and reclaiming it is correct: nothing references it.
        // What must not move is the pinned owner.
        let retained_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            !retained_plan
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
        );
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
        // Co-ownership guard for locator reclamation: the checkpoint also
        // carries this change as a selected member, so a sweep that retired
        // anything must not have taken the row out from under the live owner.
        assert!(
            locator_row_exists(&read, selected_locator_change_id).await,
            "a locator whose owner is retained must survive the sweep"
        );
        drop(read);

        publish_branch_head_release(&storage, "main", released_control, released_manifest).await;

        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            released_plan
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
        );
        assert!(
            !released_plan
                .sweep
                .tracked_commit_roots
                .contains(&base.commit_id),
            "the interval base stays on the head's first-parent chain and owns row history"
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
        assert!(
            !locator_row_exists(&read, selected_locator_change_id).await,
            "retiring the owning commit reclaims its change-locator row"
        );
    }

    #[cfg(feature = "storage-benches")]
    /// Is there still a row in the change-locator plane for `change_id`?
    async fn locator_row_exists<R>(read: &R, change_id: ChangeId) -> bool
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let wanted = change_id.as_uuid().as_bytes().to_vec();
        space_inventory(
            read,
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        )
        .await
        .into_iter()
        .any(|(key, _)| key == wanted)
    }

    #[tokio::test]
    async fn ordinary_gc_keeps_certified_scoped_descriptor_body_owner_after_root_release() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("scoped owner timestamp", "2026-01-01T00:00:00Z");
        // The checkpoint parents onto a fresh interval base rather than onto
        // `owner`, which is what compaction actually produces: `owner` is an
        // interior commit of the interval the checkpoint closes, so the
        // checkpoint supersedes it and it leaves the first-parent chain.
        //
        // This fixture used to parent the checkpoint directly onto `owner` --
        // a raw parent chain no real checkpoint ever produces. That kept
        // `owner` reachable from the live head forever, and a commit reachable
        // from the head now keeps its delta because that delta is what an
        // row `_history()` row is served out of. The release under test here
        // is the scoped-descriptor owner pin, not graph reachability, so the fixture models the
        // compaction instead of contradicting it.
        let base = replay_commit_record("scoped-part-base", 0, None, timestamp);
        let owner = replay_commit_record("scoped-part-owner", 1, Some(base.commit_id), timestamp);
        let checkpoint =
            replay_commit_record("scoped-part-checkpoint", 1, Some(base.commit_id), timestamp);
        let mut base_manifest =
            test_commit_state_manifest(&base, CommitStateMutationInventory::default());
        base_manifest.replay_debt = CommitStateReplayDebt::default();
        base_manifest.snapshot_root = Some(Box::new(test_snapshot_root(base.commit_id)));
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
        let row_pk = RowPk::single("row");
        let encoded_key =
            crate::tracked_state::encode_key_ref(crate::tracked_state::TrackedStateKeyRef {
                schema_key: &scope.schema_key,
                file_id: None,
                row_pk: &row_pk,
            });

        let mut writes = storage.new_write_set();
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
            source: crate::tracked_state::CurrentStatePartSource::Replacement(
                crate::tracked_state::ReplacementPartSource {
                    owner_commit_id: *owner.commit_id.as_uuid().as_bytes(),
                    part_index: 0,
                    uniform_created_at: timestamp,
                    uniform_updated_at: timestamp,
                },
            ),
            source_row_offset: 0,
            row_count: 1,
            fragmented: false,
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
        let _owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
        let checkpoint_control =
            replay_branch_control(checkpoint.commit_id, control_ref, timestamp);
        let released_control = replay_branch_control(released.commit_id, control_ref, timestamp);
        stage_branch_head_control(&mut writes, "main", checkpoint_control)
            .expect("scoped owner checkpoint control should stage");
        persist_replay_closure_fixture(
            &storage,
            writes,
            &[
                base.clone(),
                owner.clone(),
                checkpoint.clone(),
                released.clone(),
            ],
            &[base_manifest, owner_manifest, checkpoint_manifest],
        )
        .await;

        let closure = load_audited_repository_retention(&storage).await;
        assert!(closure.physical_authorities.contains(&owner.commit_id));

        assert!(
            !run_ordinary_repository_gc(&storage)
                .await
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
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

        publish_branch_head_release(&storage, "main", released_control, released_manifest).await;
        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            !released_plan
                .sweep
                .tracked_commit_roots
                .contains(&base.commit_id),
            "the interval base stays on the head's first-parent chain and owns row history"
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("released scoped owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("certified scoped owner should load after descriptor release")
                .is_some(),
            "the owner's certified native body remains independent physical authority"
        );
    }

    #[tokio::test]
    async fn ordinary_gc_keeps_native_row_owner_for_reachable_checkpoint_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("native row owner timestamp", "2026-01-01T00:00:00Z");
        // The checkpoint parents onto a fresh interval base rather than onto
        // `owner`, which is what compaction actually produces: `owner` is an
        // interior commit of the interval the checkpoint closes, so the
        // checkpoint supersedes it and it leaves the first-parent chain.
        //
        // This fixture used to parent the checkpoint directly onto `owner` --
        // a raw parent chain no real checkpoint ever produces. That kept
        // `owner` reachable from the live head forever, and a commit reachable
        // from the head now keeps its delta because that delta is what an
        // row `_history()` row is served out of. The release under test here
        // is the native-row owner pin, not graph reachability, so the fixture models the
        // compaction instead of contradicting it.
        let base = replay_commit_record("native-row-base", 0, None, timestamp);
        let owner = replay_commit_record("native-row-owner", 1, Some(base.commit_id), timestamp);
        let checkpoint =
            replay_commit_record("native-row-checkpoint", 1, Some(base.commit_id), timestamp);
        let mut base_manifest =
            test_commit_state_manifest(&base, CommitStateMutationInventory::default());
        base_manifest.replay_debt = CommitStateReplayDebt::default();
        base_manifest.snapshot_root = Some(Box::new(test_snapshot_root(base.commit_id)));
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
            source: crate::tracked_state::CurrentStatePartSource::NativeDataPart {
                payload_refs_digest: encoded.refs_digest,
            },
            source_row_offset: 0,
            row_count: encoded.row_count,
            fragmented: false,
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
        writes.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
            StorageKey(Bytes::copy_from_slice(&encoded.digest)),
            StorageValue {
                bytes: encoded.bytes.clone(),
            },
        );
        writes.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
            StorageKey(Bytes::copy_from_slice(&encoded.digest)),
            StorageValue {
                bytes: encoded.refs_bytes.clone(),
            },
        );
        let tree = crate::tracked_state::scoped_range::stage_scoped_range_tree(
            &mut writes,
            [(marker, vec![part])],
        )
        .expect("native row scoped tree should stage");
        let snapshot_row_pk = RowPk::single("native-row");
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
                    row_pk: &snapshot_row_pk,
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
        let _owner_control = replay_branch_control(owner.commit_id, control_ref, timestamp);
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
            &[
                base.clone(),
                owner.clone(),
                checkpoint.clone(),
                released.clone(),
            ],
            &[base_manifest, owner_manifest, checkpoint_manifest],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native dependency read should open");
        assert_eq!(
            crate::tracked_state::load_native_current_state_part_owners(
                &read,
                &BTreeSet::from([encoded.digest]),
            )
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
        restore.put(
            crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
            StorageKey(Bytes::copy_from_slice(&encoded.digest)),
            StorageValue {
                bytes: encoded.refs_bytes.clone(),
            },
        );
        storage
            .commit_write_set(restore, StorageWriteOptions::default())
            .await
            .expect("authenticated native part restoration should commit");

        assert!(
            !run_ordinary_repository_gc(&storage)
                .await
                .sweep
                .tracked_commit_roots
                .contains(&owner.commit_id)
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

        publish_branch_head_release(&storage, "main", released_control, released_manifest).await;

        let released_plan = run_ordinary_repository_gc(&storage).await;
        assert!(
            !released_plan
                .sweep
                .tracked_commit_roots
                .contains(&base.commit_id),
            "the interval base stays on the head's first-parent chain and owns row history"
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("released native owner read should open");
        assert!(
            crate::tracked_state::load_commit_state_manifest(&read, owner.commit_id)
                .await
                .expect("reachable-checkpoint native owner should load")
                .is_some(),
            "the reachable checkpoint remains authenticated native body authority"
        );
    }

    async fn gc_sweep_fixture() -> (
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
            authored_history_bodies: None,
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
        let (storage, commit_id, _root_hash, _dead_hash, _dead_hash_two) = gc_sweep_fixture().await;
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
        advanced.current_state_revision = advanced.current_state_revision.saturating_add(1);
        stage_branch_head_control(&mut concurrent, "main", advanced)
            .expect("concurrent control should stage");
        storage
            .commit_write_set(concurrent, StorageWriteOptions::default())
            .await
            .expect("concurrent publication should commit");

        let mut stale_sweep = storage.new_write_set();
        stale_sweep.put(
            CHECKPOINT_GC_STATE_SPACE,
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
    async fn retention_closure_and_audit_share_retained_physical_owner() {
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
        };
        let manifest = |commit_id, snapshot_root| CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt::default(),
            mutations: CommitStateMutationInventory::default(),
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            authored_history_bodies: None,
            snapshot_root: Some(Box::new(snapshot_root)),
        };
        let _owner_control = replay_branch_control(owner, retired_ref, timestamp);
        let active_control = replay_branch_control(active, active_ref, timestamp);
        let mut writes = storage.new_write_set();
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
        let closure = super::load_authenticated_repository_retention(&read, &controls)
            .await
            .expect("shared offline owner closure should authenticate");
        assert!(closure.physical_dependencies.contains(&owner));
        // The one retention closure is also what keeps the retired owner's
        // tracked tree root alive; there is no second root walk to consult.
        let retained_ids = closure
            .physical_authorities
            .union(&closure.physical_dependencies)
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(retained_ids, vec![owner.min(active), owner.max(active)]);
        let closure_tree_roots =
            crate::tracked_state::load_commit_state_manifests(&read, &retained_ids)
                .await
                .expect("retained manifests should load")
                .into_iter()
                .flatten()
                .filter_map(|manifest| manifest.snapshot_root)
                .flat_map(|root| {
                    std::iter::once(*root.root_id.as_bytes()).chain(
                        root.parent_roots
                            .iter()
                            .map(|parent| *parent.root_id.as_bytes())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<BTreeSet<_>>();
        assert_eq!(
            closure_tree_roots,
            BTreeSet::from([owner_hash, active_hash])
        );
        let audit = super::audit_repository_gc_standalone_refs(&read)
            .await
            .expect("standalone audit should consume the same closure");
        // With the publication ledger gone the audit no longer has a stored
        // `old_control` to attribute a superseded ref change to. A branch-ref
        // change that no live control claims is simply unclassified: the
        // publication that supersedes it now deletes it in the same write set,
        // so a surviving one is a fixture artefact, not deferred GC debt.
        assert_eq!(
            audit,
            vec![format!(
                "{retired_ref}:unclassified_no_live_control:schema=authority_gc:account={}:origin=none",
                crate::ANONYMOUS_ACCOUNT_ID
            )]
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
    async fn recovery_ref_without_live_serving_control_is_not_branchable_authority() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
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
            .expect_err("a recovery ref no live control still serves is not a branchable root");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
    }

    /// A snapshot whose normalized JSON is comfortably past
    /// `JSON_INLINE_MAX_BYTES`, and distinct for every `revision`.
    ///
    /// Both properties are load-bearing and both were got wrong by an earlier
    /// probe: a payload at or under 1 KiB never reaches the store at all, and
    /// the store is content addressed, so re-writing byte-identical content
    /// dedups onto one row and leaks nothing.
    #[cfg(feature = "storage-benches")]
    fn out_of_band_payload(revision: usize) -> String {
        let filler = format!("rev-{revision:08}-");
        let mut body = String::with_capacity(2_048);
        while body.len() < 1_800 {
            body.push_str(&filler);
        }
        serde_json::json!({ "revision": revision, "body": body }).to_string()
    }

    /// Registers one of several **identically shaped** payload tables.
    ///
    /// A row's out-of-band payload is its whole snapshot, and the snapshot
    /// carries the row's primary key — so two rows can only share a payload
    /// when their snapshots are byte-identical, which means the same key with
    /// the same value under a *different* schema. The schema key lives in the
    /// storage key, not in the snapshot, so these tables are exactly the
    /// distinct owners a co-ownership fixture needs. (An earlier version used
    /// two different paths, believed it had proved dedup, and was measuring two
    /// unrelated payloads.)
    #[cfg(feature = "storage-benches")]
    async fn register_payload_schema<S>(
        session: &crate::session::SessionContext<S>,
        schema_key: &str,
    ) where
        S: crate::storage::Storage + Clone + Send + Sync + 'static,
    {
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": schema_key,
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (CAST($1 AS JSONB), false, false)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("payload fixture schema should register");
    }

    /// Undo-to-last-checkpoint must survive reclaim at any cadence.
    ///
    /// The retention floor for the undo interval is
    /// `BranchHeadTrackedReachability::serving_checkpoint_commit_id`, read from
    /// the *live* branch head control at sweep time -- see
    /// `authenticated_control_commit_reachability`. It is not derived from the
    /// reclaim schedule or from any stored sequence, so no cadence can move it.
    ///
    /// This test pins that property mechanically by running the full shipping
    /// sweep after **every single commit**, which is strictly more aggressive
    /// than any cadence the engine would ever schedule. If a future change ties
    /// the undo floor to reclaim scheduling, this fails loudly.
    ///
    /// # Two things this fixture already paid for
    ///
    /// **1. The churn ordering is load-bearing.** `update -> checkpoint ->
    /// sweep` retires commits; `checkpoint -> update -> sweep` retires
    /// *nothing*, because the sweep then never observes a released interval.
    /// Two earlier versions of this test used the second ordering and retired
    /// zero across every iteration. If you reorder the churn loop below, expect
    /// the `retired_total` guard to fire — that is the guard working, not a
    /// broken fixture. Measured onset with the correct ordering, checkpointing
    /// every revision: `tracked_roots` is 0, 1, 0, then 2 from revision 4 on.
    ///
    /// **2. `tracked_commit_roots` is the counter that observes retirement.**
    /// The plausibly-named `plan.changelog.sweep.commits` reads **0 at every
    /// iteration** on this fixture while `tracked_commit_roots` is non-zero, so
    /// a guard written against it would fail forever against working code — and
    /// would eventually be "fixed" by deleting the guard. Do not swap the
    /// counter without re-running the measurement.
    ///
    /// # Keep the non-vacuity guard
    ///
    /// `retired_total > 0` is the most important line here. It failed two
    /// fixtures that would otherwise have gone green while proving nothing at
    /// all about undo retention: a sweep that retires nothing trivially
    /// preserves undo. The assertions below are only evidence because the sweep
    /// underneath them is doing real work.
    /// # This test is about undo, not history
    ///
    /// **Undo survives reclaim; row history does not.** These are different
    /// properties over different planes, and this test asserts only the first.
    /// Do not read its name as "reclaim is safe for history".
    ///
    /// Undo reads current state, which the sweep leaves intact. `_history()`
    /// reads each commit's *delta*, and the sweep deletes those. The commit
    /// record survives -- `collect_ref_reachable_commit_ids` feeds
    /// `graph_reachable` into `semantic_dependencies`, so
    /// `stage_delete_commit_projection` retains it -- but the delta does not,
    /// because `graph_reachable` never reaches `physical_dependencies`, so
    /// `stage_retire_commit_physical_state` frees it. The loss is silent by
    /// construction: `load_point_replay_commit_state` returning `None` makes a
    /// commit whose manifest was retired indistinguishable from one that
    /// changed nothing, so the walk emits zero rows and no error is possible.
    ///
    /// Measured on this engine -- checkpointed rounds, then one sweep, history
    /// depth before -> after: 4 -> 3, 8 -> 3, 12 -> 3, with **zero** semantic
    /// projections deleted in every case. The survivor count is a constant,
    /// not a fraction, so the loss grows without bound as a repository ages.
    ///
    /// This is a known defect under repair at the time of writing; the
    /// retention fix belongs to `physical_dependencies`, not here. When it
    /// lands, a sibling test should pin history the way this one pins undo.
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn undo_to_last_checkpoint_survives_reclaim_after_every_commit() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("repository should open");
        let session = engine.open_session().await.expect("session should open");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "gc_undo_cadence_fixture",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("undo cadence fixture schema should register");
        session
            .execute(
                "INSERT INTO gc_undo_cadence_fixture (path, value) VALUES ('/row', 'v0')",
                &[],
            )
            .await
            .expect("seed row should publish");

        // Churn phase: `update -> checkpoint -> sweep`, the ordering that
        // actually retires. See the note above before changing it.
        const CHURN: usize = 8;
        let mut retired_total = 0usize;
        for revision in 1..=CHURN {
            session
                .execute(
                    "UPDATE gc_undo_cadence_fixture SET value = $1 WHERE path = '/row'",
                    &[Value::Text(format!("v{revision}"))],
                )
                .await
                .expect("churn commit should publish");
            session
                .create_checkpoint()
                .await
                .expect("churn checkpoint should publish");
            let plan = run_shipping_repository_gc(&backend).await;
            retired_total += plan.sweep.tracked_commit_roots.len();
        }

        // Non-vacuity. Do not remove: a sweep that never retires anything would
        // let every assertion below pass while proving nothing about retention.
        assert!(
            retired_total > 0,
            "the fixture must actually retire commits, or the undo assertions below are vacuous"
        );

        // Undo phase: two commits past the last checkpoint, still sweeping
        // after each, so the retained undo interval is exactly {v9, v10}.
        for revision in (CHURN + 1)..=(CHURN + 2) {
            session
                .execute(
                    "UPDATE gc_undo_cadence_fixture SET value = $1 WHERE path = '/row'",
                    &[Value::Text(format!("v{revision}"))],
                )
                .await
                .expect("post-checkpoint commit should publish");
            run_shipping_repository_gc(&backend).await;
        }

        // Undo must walk back to the last checkpoint's state (v8). Undo *past*
        // the last checkpoint is deliberately not asserted: checkpointing is
        // permitted to retire the last-1 interval.
        for expected in ["v9", "v8"] {
            session
                .undo()
                .await
                .expect("undo must survive reclaim at every commit");
            let value = session
                .execute(
                    "SELECT value FROM gc_undo_cadence_fixture WHERE path = '/row'",
                    &[],
                )
                .await
                .expect("undone row should read")
                .rows()[0]
                .get::<String>("value")
                .expect("undone row should have a value");
            assert_eq!(
                value, expected,
                "undo landed on the wrong revision after reclaim"
            );
        }
    }

    /// Demonstrates, mechanically, that the in-record `format_version` cannot
    /// gate an arity change to `StoredCheckpointGcState`.
    ///
    /// Every storage record is `#[musli(packed)]` -- positional and untagged --
    /// so a reader's field count is part of its wire contract. Decoding fails on
    /// **arity**, before any field value (including `format_version`) can be
    /// inspected. The upgrade mechanism for such a change is therefore
    /// `REPOSITORY_PROTOCOL_VALUE`, which makes `Engine::new` reject the
    /// repository outright, not the per-record version field.
    ///
    /// Reading the derive is not evidence for this; the readers below are
    /// constructed and run.
    #[test]
    fn packed_arity_change_cannot_be_gated_by_the_in_record_format_version() {
        // Today's shipping shape: format_version + 3 counters.
        #[derive(musli::Encode, musli::Decode)]
        #[musli(packed)]
        struct ArityOld {
            format_version: u32,
            checkpoint_sequence: u64,
            last_gc_sequence: u64,
            collectible_interval_count: u64,
        }

        // Proposed shape: the same, plus the two self-correcting estimates the
        // ratio trigger needs.
        #[derive(musli::Encode, musli::Decode)]
        #[musli(packed)]
        struct ArityNew {
            format_version: u32,
            checkpoint_sequence: u64,
            last_gc_sequence: u64,
            collectible_interval_count: u64,
            live_manifest_estimate: u64,
            yield_per_interval_estimate: u64,
        }

        let new_bytes = crate::storage_codec::encode(
            "arity demo new",
            &ArityNew {
                format_version: 2,
                checkpoint_sequence: 7,
                last_gc_sequence: 3,
                collectible_interval_count: 4,
                live_manifest_estimate: 512,
                yield_per_interval_estimate: 9,
            },
        )
        .expect("new record should encode");

        let old_bytes = crate::storage_codec::encode(
            "arity demo old",
            &ArityOld {
                format_version: 1,
                checkpoint_sequence: 7,
                last_gc_sequence: 3,
                collectible_interval_count: 4,
            },
        )
        .expect("old record should encode");

        // Direction 1: an OLD reader against a NEW record.
        let old_reads_new = crate::storage_codec::decode::<ArityOld>("arity demo", &new_bytes);
        let old_reads_new_err = old_reads_new
            .err()
            .expect("a 4-field reader must reject a 6-field record");
        println!(
            "ARITY old_reader_vs_new_record: {}",
            old_reads_new_err.message
        );

        // Direction 2: a NEW reader against an OLD record.
        let new_reads_old = crate::storage_codec::decode::<ArityNew>("arity demo", &old_bytes);
        let new_reads_old_err = new_reads_old
            .err()
            .expect("a 6-field reader must reject a 4-field record");
        println!(
            "ARITY new_reader_vs_old_record: {}",
            new_reads_old_err.message
        );

        // The sharp point: bumping `format_version` inside the record does not
        // help, because a same-arity record still decodes cleanly and a
        // different-arity record never reaches the field at all. So the version
        // field can express "same shape, new meaning" and can never express
        // "new shape".
        let bumped = crate::storage_codec::encode(
            "arity demo bumped",
            &ArityOld {
                format_version: 999,
                checkpoint_sequence: 7,
                last_gc_sequence: 3,
                collectible_interval_count: 4,
            },
        )
        .expect("bumped record should encode");
        let decoded = crate::storage_codec::decode::<ArityOld>("arity demo", &bumped)
            .expect("a same-arity record decodes regardless of its version value");
        assert_eq!(
            decoded.format_version, 999,
            "the version field is only observable once arity already matched"
        );
        println!(
            "ARITY same_arity_decodes_despite_version=999 checkpoint_sequence={}",
            decoded.checkpoint_sequence
        );
    }

    #[cfg(feature = "storage-benches")]
    async fn run_shipping_repository_gc(backend: &Memory) -> super::RepositoryGcPlan {
        let storage = StorageAdapter::new(backend.clone());
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("payload GC read should open"),
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan =
            super::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
                .await
                .expect("payload GC should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("payload GC should commit");
        plan
    }

    #[cfg(feature = "storage-benches")]
    async fn json_payload_refs(backend: &Memory) -> BTreeSet<JsonRef> {
        let storage = StorageAdapter::new(backend.clone());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("payload census read should open");
        space_inventory(&read, crate::json_store::JSON_SPACE)
            .await
            .into_iter()
            .map(|(key, _)| {
                JsonRef::from_hash_bytes(
                    <[u8; 32]>::try_from(key.as_ref()).expect("payload keys are 32-byte hashes"),
                )
            })
            .collect()
    }

    /// Names the payload row one statement added, without reproducing the
    /// engine's JSON normalization in the test. Re-deriving the content address
    /// here would make the fixture depend on a normalization detail rather than
    /// on the reachability behaviour under test.
    #[cfg(feature = "storage-benches")]
    async fn payload_ref_added_by<F, Fut>(backend: &Memory, publish: F) -> JsonRef
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let before = json_payload_refs(backend).await;
        publish().await;
        let after = json_payload_refs(backend).await;
        let mut added = after.difference(&before).copied().collect::<Vec<_>>();
        assert_eq!(
            added.len(),
            1,
            "the fixture statement must add exactly one out-of-band payload row"
        );
        added.pop().expect("one added payload ref")
    }

    /// A payload named by two owners, one of which this sweep retires, must
    /// survive.
    ///
    /// This is the case the reclamation exists to *not* break, and it states
    /// the dedup hazard exactly: the store is content addressed, so the second
    /// owner does **not** write a second payload — it resolves onto the row the
    /// first owner already produced. Retiring the commit that happened to
    /// author it first must therefore not be read as "nobody names this".
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn repository_gc_keeps_a_payload_a_second_owner_still_names() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("shared-payload repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("shared-payload repository should open");
        let session = engine
            .open_session()
            .await
            .expect("shared-payload session should open");
        register_payload_schema(&session, "gc_payload_row").await;
        register_payload_schema(&session, "gc_payload_mirror").await;

        let first_owner_branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-0000000000b1".to_owned()),
                name: "gc-payload-first-owner".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("first-owner branch should create");
        let first_owner = engine
            .open_session_at(first_owner_branch.id.clone())
            .await
            .expect("first-owner branch session should open");

        let shared = out_of_band_payload(0);
        let shared_ref = payload_ref_added_by(&backend, || async {
            first_owner
                .execute(
                    "INSERT INTO gc_payload_row (path, value) VALUES ('/shared', CAST($1 AS JSONB))",
                    &[Value::Text(shared.clone())],
                )
                .await
                .expect("first owner should publish");
        })
        .await;
        let before_mirror = json_payload_refs(&backend).await;
        session
            .execute(
                "INSERT INTO gc_payload_mirror (path, value) VALUES ('/shared', CAST($1 AS JSONB))",
                &[Value::Text(shared.clone())],
            )
            .await
            .expect("second owner should publish");
        assert_eq!(
            json_payload_refs(&backend).await,
            before_mirror,
            "the premise of this test is that identical content dedups onto one row"
        );

        // Retire only the branch that authored the shared payload. The mirror
        // row remains independently reachable from main and keeps naming the
        // same content-addressed JSON row.
        drop(first_owner);
        session
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(first_owner_branch.id)],
            )
            .await
            .expect("first-owner branch should retire");

        let plan = run_shipping_repository_gc(&backend).await;
        assert!(
            !plan.sweep.tracked_commit_roots.is_empty(),
            "the fixture must actually retire commits, or nothing is being tested"
        );
        assert!(
            !plan.changelog.sweep.json_payloads.contains(&shared_ref),
            "a payload a second owner still names must never be proposed for deletion"
        );
        assert!(
            plan.changelog.live.payloads.contains(&shared_ref),
            "the shared payload must be proven live, not merely absent from the sweep"
        );
        assert!(
            json_ref_exists(&backend, crate::json_store::JSON_SPACE, shared_ref).await,
            "the shared payload row must survive the sweep"
        );

        let value = session
            .execute(
                "SELECT value FROM gc_payload_mirror WHERE path = '/shared'",
                &[],
            )
            .await
            .expect("second owner should still read")
            .rows()[0]
            .get::<serde_json::Value>("value")
            .expect("second owner should still carry its payload");
        assert!(
            value.to_string().contains("rev-00000000-"),
            "the surviving owner must materialize the original payload: {value}"
        );
    }

    /// The co-ownership case a naive per-commit delete gets wrong: one payload
    /// named by tracked history *and* by more than one untracked row.
    ///
    /// Untracked rows live only in the hot serving plane — no commit names
    /// them — so a live set derived from commits alone deletes this payload out
    /// from under both of them.
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn repository_gc_keeps_a_payload_co_owned_by_history_and_untracked_rows() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("co-owned payload repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("co-owned payload repository should open");
        let session = engine
            .open_session()
            .await
            .expect("co-owned payload session should open");
        for schema_key in [
            "gc_payload_row",
            "gc_payload_untracked_a",
            "gc_payload_untracked_b",
        ] {
            register_payload_schema(&session, schema_key).await;
        }

        let shared = out_of_band_payload(0);
        let shared_ref = payload_ref_added_by(&backend, || async {
            session
                .execute(
                    "INSERT INTO gc_payload_row (path, value) VALUES ('/co', CAST($1 AS JSONB))",
                    &[Value::Text(shared.clone())],
                )
                .await
                .expect("tracked owner should publish");
        })
        .await;
        let before_untracked = json_payload_refs(&backend).await;
        for table in ["gc_payload_untracked_a", "gc_payload_untracked_b"] {
            session
                .execute(
                    &format!(
                        "INSERT INTO {table} (path, value, lixcol_untracked) \
                         VALUES ('/co', CAST($1 AS JSONB), true)"
                    ),
                    &[Value::Text(shared.clone())],
                )
                .await
                .expect("untracked owner should publish");
        }
        assert_eq!(
            json_payload_refs(&backend).await,
            before_untracked,
            "both untracked rows must resolve onto the payload row history already owns"
        );

        // Retire the tracked owner's commit while both untracked rows stay.
        session
            .execute("DELETE FROM gc_payload_row WHERE path = '/co'", &[])
            .await
            .expect("tracked owner should delete");
        for revision in 1..=6 {
            session
                .execute(
                    "INSERT INTO gc_payload_row (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(format!("/churn-{revision}")),
                        Value::Text(out_of_band_payload(revision)),
                    ],
                )
                .await
                .expect("churn should publish");
            session
                .create_checkpoint()
                .await
                .expect("churn checkpoint should publish");
        }
        session
            .create_checkpoint()
            .await
            .expect("releasing checkpoint should publish");

        let plan = run_shipping_repository_gc(&backend).await;
        assert!(
            !plan.sweep.tracked_commit_roots.is_empty(),
            "the fixture must actually retire commits, or nothing is being tested"
        );
        assert!(
            !plan.changelog.sweep.json_payloads.contains(&shared_ref),
            "a payload two untracked rows still name must never be proposed for deletion"
        );
        assert!(
            json_ref_exists(&backend, crate::json_store::JSON_SPACE, shared_ref).await,
            "the co-owned payload row must survive the sweep"
        );
        for table in ["gc_payload_untracked_a", "gc_payload_untracked_b"] {
            let rows = session
                .execute(
                    &format!("SELECT value FROM {table} WHERE path = '/co'"),
                    &[],
                )
                .await
                .expect("untracked owner should still read");
            assert_eq!(
                rows.rows().len(),
                1,
                "untracked row in '{table}' must survive"
            );
            let value = rows.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("untracked owner should still carry its payload");
            assert!(
                value.to_string().contains("rev-00000000-"),
                "the untracked owner must materialize the original payload: {value}"
            );
        }
    }

    /// The leak this reclamation exists to close: superseded payloads are
    /// actually reclaimed, and the live one is not.
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn repository_gc_reclaims_superseded_out_of_band_payloads() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("superseded payload repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("superseded payload repository should open");
        let session = engine
            .open_session()
            .await
            .expect("superseded payload session should open");
        register_payload_schema(&session, "gc_payload_row").await;

        session
            .execute(
                "INSERT INTO gc_payload_row (path, value) VALUES ('/row', CAST($1 AS JSONB))",
                &[Value::Text(out_of_band_payload(0))],
            )
            .await
            .expect("first revision should publish");
        let mut live_ref = JsonRef::default();
        for revision in 1..=16 {
            live_ref = payload_ref_added_by(&backend, || async {
                session
                    .execute(
                        "UPDATE gc_payload_row SET value = CAST($1 AS JSONB) WHERE path = '/row'",
                        &[Value::Text(out_of_band_payload(revision))],
                    )
                    .await
                    .expect("rewrite should publish");
            })
            .await;
            if revision % 4 == 0 {
                session
                    .create_checkpoint()
                    .await
                    .expect("cadence checkpoint should publish");
            }
        }
        session
            .create_checkpoint()
            .await
            .expect("releasing checkpoint should publish");

        let before = json_payload_refs(&backend).await.len();
        let plan = run_shipping_repository_gc(&backend).await;
        let after = json_payload_refs(&backend).await.len();
        assert!(
            !plan.changelog.sweep.json_payloads.is_empty(),
            "superseded payloads must be proposed for deletion"
        );
        assert_eq!(
            before - after,
            plan.changelog.sweep.json_payloads.len(),
            "every proposed payload delete must actually remove a row"
        );
        assert!(
            after < before,
            "the payload plane must shrink: {before} -> {after}"
        );
        assert!(
            json_ref_exists(&backend, crate::json_store::JSON_SPACE, live_ref).await,
            "the surviving revision's payload must not be reclaimed"
        );
        let value = session
            .execute("SELECT value FROM gc_payload_row WHERE path = '/row'", &[])
            .await
            .expect("live row should still read")
            .rows()[0]
            .get::<serde_json::Value>("value")
            .expect("live row should still carry its payload");
        assert!(
            value.to_string().contains("rev-00000016-"),
            "the live row must still materialize its payload: {value}"
        );
    }

    #[tokio::test]
    async fn repository_gc_keeps_current_untracked_file_blob_across_cold_reopen() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("untracked-file repository should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("untracked-file repository should open");
        let session = engine
            .open_session()
            .await
            .expect("untracked-file session should open");
        let live_bytes = b"current-only-untracked-file-blob";
        session
            .execute(
                "INSERT INTO lix_file (path, content, lixcol_untracked) \
                 VALUES ('/current-only.bin', $1, true)",
                &[Value::Blob(live_bytes.to_vec().into())],
            )
            .await
            .expect("untracked file should publish");

        let storage = StorageAdapter::new(backend.clone());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("unrelated orphan staging read should open");
        let mut orphan_writes = storage.new_write_set();
        let orphan = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut orphan_writes)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                b"untracked-file-unrelated-orphan".to_vec(),
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
                .expect("untracked-file GC read should open"),
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan =
            super::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
                .await
                .expect("untracked-file GC should stage");
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
            .expect("untracked-file GC should commit");

        drop(session);
        drop(engine);
        let reopened = Engine::new(backend.clone())
            .await
            .expect("repository should cold reopen after untracked-file GC");
        let reopened_session = reopened
            .open_session()
            .await
            .expect("cold untracked-file session should open");
        let content = reopened_session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/current-only.bin'",
                &[],
            )
            .await
            .expect("cold untracked file should read");
        assert_eq!(
            content.rows()[0].get::<Vec<u8>>("content").unwrap(),
            live_bytes
        );

        let read = StorageAdapter::new(backend)
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan verification read should open");
        let mut reader = crate::binary_cas::BinaryCasContext::new().reader(read);
        assert!(
            reader
                .load_bytes_many(&[orphan.hash])
                .await
                .expect("orphan verification should load")
                .into_vec()[0]
                .is_none(),
            "unrelated binary-CAS garbage must still be reclaimed"
        );
    }

    #[cfg(feature = "default_wasm_runtime")]
    #[cfg(any())]
    #[tokio::test]
    async fn repository_gc_keeps_plugin_wasm_for_cold_runtime_execution() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("plugin-GC repository should initialize");
        let engine = Engine::new_with_wasm_runtime(
            backend.clone(),
            crate::plugin::runtime::default::runtime().expect("WASM runtime should initialize"),
        )
        .await
        .expect("plugin-GC repository should open");
        let session = engine
            .open_session()
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
            crate::plugin::runtime::default::runtime()
                .expect("cold WASM runtime should initialize"),
        )
        .await
        .expect("plugin-GC repository should cold reopen");
        let session = engine
            .open_session()
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
    #[cfg(any())]
    #[tokio::test]
    async fn repository_gc_reclaims_plugin_wasm_only_after_final_registry_root_releases() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("shared-plugin repository should initialize");
        let engine = Engine::new_with_wasm_runtime(
            backend.clone(),
            crate::plugin::runtime::default::runtime().expect("WASM runtime should initialize"),
        )
        .await
        .expect("shared-plugin repository should open");
        let session = engine
            .open_session()
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
            "key": crate::plugin::runtime::PLUGIN_REGISTRY_KEY,
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
            .expect("repository branch control should exist");
        let timestamp =
            LixTimestamp::expect_parse("corrupt registry timestamp", "2026-01-01T00:00:00Z");
        let row_pk = RowPk::single(crate::plugin::runtime::PLUGIN_REGISTRY_KEY);
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
                    row_pk: &row_pk,
                    change_id: Some(ChangeId::for_test_label("corrupt-plugin-registry")),
                    commit_id: Some(control.head_commit_id),
                    untracked: false,
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
    #[cfg(any())]
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

    /// The derivation replaces `gc.reachability_delta.v1`. It must name every
    /// commit that still owns physical state — including one no ref can reach,
    /// which is what a deleted branch leaves behind and what a walk from refs
    /// structurally cannot find — and it must shrink as retirement succeeds.
    #[tokio::test]
    async fn derived_candidates_are_the_commits_that_still_own_physical_state() {
        let storage = StorageAdapter::new(Memory::new());
        let timestamp =
            LixTimestamp::expect_parse("derived candidate timestamp", "2026-01-01T00:00:00Z");
        let head = replay_commit_record("derived-candidate-head", 0, None, timestamp);
        // No commit record and no parent link binds this one to anything.
        let orphan = replay_commit_record("derived-candidate-orphan", 0, None, timestamp);
        let mut head_manifest =
            test_commit_state_manifest(&head, CommitStateMutationInventory::default());
        head_manifest.replay_debt = CommitStateReplayDebt::default();
        head_manifest.snapshot_root = Some(Box::new(test_snapshot_root(head.commit_id)));
        let mut orphan_manifest =
            test_commit_state_manifest(&orphan, CommitStateMutationInventory::default());
        orphan_manifest.replay_debt = CommitStateReplayDebt::default();
        orphan_manifest.snapshot_root = Some(Box::new(test_snapshot_root(orphan.commit_id)));
        persist_replay_closure_fixture(
            &storage,
            storage.new_write_set(),
            &[head.clone()],
            &[head_manifest, orphan_manifest],
        )
        .await;

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("derived candidate read should open"),
        );
        let derived = derive_retirement_candidates(&read)
            .await
            .expect("candidates should derive from the manifest plane alone");
        assert_eq!(
            derived.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([head.commit_id, orphan.commit_id]),
            "every commit owning physical state is a candidate, reachable or not"
        );
        drop(read);

        // Retiring the physical state is what removes a candidate. Nothing is
        // remembered about the retirement; the inventory simply gets shorter.
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("retirement read should open"),
        );
        let mut writes = storage.new_write_set();
        crate::tracked_state::stage_retire_commit_physical_state(
            &read,
            &mut writes,
            orphan.commit_id,
            crate::tracked_state::RetainedPhysicalState {
                mutation_nodes: &BTreeSet::new(),
                scoped_nodes: &BTreeSet::new(),
                native_parts: &BTreeSet::new(),
            },
            &mut BTreeMap::new(),
        )
        .await
        .expect("orphan physical state should retire");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("orphan retirement should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("post-retirement read should open"),
        );
        let derived = derive_retirement_candidates(&read)
            .await
            .expect("candidates should derive after retirement");
        assert_eq!(derived, vec![head.commit_id]);
    }

    #[test]
    fn retirement_requires_history_and_pin_dependency_closure() {
        let old = CommitId::for_test_label("history-old");
        let new = CommitId::for_test_label("history-new");
        let active = BTreeSet::from([new]);
        let mut dependencies = BTreeSet::from([old]);
        assert!(!retirement_is_proven(old, &active, &dependencies));
        // Once history/diff/undo/redo/checkpoint pins release the old root,
        // the derived candidate is a valid physical-retirement proof.
        dependencies.clear();
        assert!(retirement_is_proven(old, &active, &dependencies));
        // A live root is never a candidate: chronology roots are the authority
        // set, so the retirement proof rejects them without a second check.
        assert!(!retirement_is_proven(new, &active, &dependencies));
    }

    #[tokio::test]
    async fn repository_gc_state_round_trips() {
        let storage = StorageAdapter::new(Memory::new());
        // Distinct values per field: a positional `#[musli(packed)]` record
        // will round-trip a permuted field order undetected if the values
        // collide.
        let expected = CheckpointGcState {
            checkpoint_sequence: 129,
            last_gc_sequence: 64,
            collectible_interval_count: 65,
            live_manifest_estimate: 4_096,
            yield_per_interval_estimate: 7,
            consecutive_reclaim_failures: 3,
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
        crate::tracked_state::stage_sweep_unreachable_content_nodes(
            &read,
            &mut sweep,
            crate::tracked_state::RetainedPhysicalState {
                mutation_nodes: &live,
                scoped_nodes: &live,
                native_parts: &live,
            },
        )
        .await
        .expect("content-addressed sweep should stage");
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
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: source_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: source_commit,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: alias_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: alias_commit,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: authority_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: authority_commit,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![alias_commit, authority_commit],
                first_parent_jump_commit_id: live_head,
                first_parent_jump_span: 0,
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
        source_manifest.authored_history_bodies =
            crate::tracked_state::certify_authored_history_body_inventory_for_test(
                &read,
                &mut writes,
                source_commit,
                &commits[0].account_id,
                &source_manifest.mutations,
                &source_root,
            )
            .await
            .expect("source tombstone body inventory should certify")
            .map(Box::new);
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
            tree.clone(),
        )
        .expect("selected-source serving root should attest");
        let mut alias_manifest = test_commit_state_manifest(&commits[1], alias_inventory);
        alias_manifest.authored_history_bodies =
            crate::tracked_state::certify_authored_history_body_inventory_for_test(
                &read,
                &mut writes,
                alias_commit,
                &commits[1].account_id,
                &alias_manifest.mutations,
                &alias_root,
            )
            .await
            .expect("selected-source local tombstone body inventory should certify")
            .map(Box::new);
        alias_manifest.current_state_scoped_ranges = Some(Box::new(alias_root));
        crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
            &mut writes,
            &alias_manifest,
        )
        .expect("selected-source serving authority should stage");

        for record in &commits[2..] {
            let mut manifest = test_commit_state_manifest(
                record,
                inventories
                    .get(&record.commit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
            if record.commit_id == authority_commit {
                manifest.current_state_scoped_ranges = Some(Box::new(
                    crate::tracked_state::attest_scoped_range_root(
                        authority_commit,
                        None,
                        &manifest.mutations,
                        tree.clone(),
                    )
                    .expect("tombstone authority serving root should attest"),
                ));
                manifest.authored_history_bodies = crate::tracked_state::
                    certify_authored_history_body_inventory_for_test(
                        &read,
                        &mut writes,
                        authority_commit,
                        &record.account_id,
                        &manifest.mutations,
                        manifest
                            .current_state_scoped_ranges
                            .as_deref()
                            .expect("tombstone authority root should exist"),
                    )
                    .await
                    .expect("authority tombstone body inventory should certify")
                    .map(Box::new);
            }
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
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: live_parent,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: live_parent,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![live_parent],
                first_parent_jump_commit_id: live_head,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                commit_id: dead_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: dead_commit,
                first_parent_jump_span: 0,
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
        live_deltas.sort_by_key(|delta| {
            crate::tracked_state::encode_key_ref(crate::tracked_state::TrackedStateKeyRef {
                schema_key: delta.delta.schema_key,
                file_id: delta.delta.file_id,
                row_pk: delta.delta.row_pk,
            })
        });
        let dead_members = vec![dead_shared_member.clone(), dead_only_member.clone()];
        let mut dead_deltas = commit_delta_refs(dead_commit, &dead_members);
        dead_deltas.sort_by_key(|delta| {
            crate::tracked_state::encode_key_ref(crate::tracked_state::TrackedStateKeyRef {
                schema_key: delta.delta.schema_key,
                file_id: delta.delta.file_id,
                row_pk: delta.delta.row_pk,
            })
        });
        let dead_stage = stage_commit_deltas_for_commit_state(&mut writes, &dead_deltas)
            .expect("dead packed members should stage");
        let dead_locators = dead_stage.locators.clone();
        let dead_record = commits
            .iter()
            .find(|record| record.commit_id == dead_commit)
            .expect("dead GC fixture record should exist");
        let mut dead_manifest = test_commit_state_manifest(
            dead_record,
            dead_stage.mutation_inventory().clone(),
        );
        dead_manifest.replay_debt = CommitStateReplayDebt::default();
        dead_manifest.snapshot_root = Some(Box::new(test_snapshot_root(dead_commit)));
        let dead_authority = stage_certified_native_manifest_fixture(
            &read,
            &mut writes,
            &mut dead_manifest,
            &dead_deltas,
            None,
        )
        .await;

        let live_stage = stage_commit_deltas_for_commit_state(&mut writes, &live_deltas)
            .expect("live selected packed member should stage");
        let live_record = commits
            .iter()
            .find(|record| record.commit_id == live_parent)
            .expect("live GC fixture record should exist");
        let mut live_manifest = test_commit_state_manifest(
            live_record,
            live_stage.mutation_inventory().clone(),
        );
        live_manifest.replay_debt = CommitStateReplayDebt::default();
        live_manifest.snapshot_root = Some(Box::new(test_snapshot_root(live_parent)));
        stage_certified_native_manifest_fixture(
            &read,
            &mut writes,
            &mut live_manifest,
            &live_deltas,
            Some(crate::tracked_state::CertifiedCommitStateTopologyParent::Staged(
                &dead_authority,
            )),
        )
        .await;

        let live_head_record = commits
            .iter()
            .find(|record| record.commit_id == live_head)
            .expect("live-head GC fixture record should exist");
        let mut live_head_manifest = test_commit_state_manifest(
            live_head_record,
            CommitStateMutationInventory::default(),
        );
        live_head_manifest.replay_debt = CommitStateReplayDebt::default();
        live_head_manifest.snapshot_root = Some(Box::new(test_snapshot_root(live_head)));
        stage_commit_state_manifest(&mut writes, &live_head_manifest)
            .expect("empty rooted GC fixture commit-state manifest should stage");
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
                crate::hot_state::row_group_set_id(commit_id, "authority_gc"),
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
                crate::hot_state::row_group_set_id(live_parent, "authority_gc"),
            )
            .await
            .expect("load live sidecar")
            .is_some(),
            "reachable commit sidecars must survive repository GC"
        );
        assert!(
            crate::columnar_row_group::load_row_group_manifest(
                &read,
                crate::hot_state::row_group_set_id(dead_commit, "authority_gc"),
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

    fn packed_change(change_label: &str, row_label: &str, snapshot: JsonSlot) -> ChangeRecord {
        ChangeRecord {
            format_version: 2,
            change_id: ChangeId::for_test_label(change_label),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            row_pk: RowPk::single(row_label),
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
        let commit_id = CommitId::for_test_label(label);
        CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
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
        let (first_parent_jump_commit_id, first_parent_jump_span) =
            parent.map_or((commit_id, 0), |parent| (parent, 1));
        CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation,
            parent_commit_ids: parent.into_iter().collect(),
            first_parent_jump_commit_id,
            first_parent_jump_span,
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
                row_pk: "row",
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
        let mut serving_manifests = BTreeMap::new();
        for manifest in manifests {
            let mut manifest = manifest.clone();
            if (manifest.mutations.member_count != 0
                || manifest.mutations.selected_source_commit_id().is_some())
                && manifest.current_state_scoped_ranges.is_none()
            {
                let serving_base = manifest
                    .mutations
                    .selected_source_commit_id()
                    .map(|source_commit_id| {
                        serving_manifests
                            .get(&source_commit_id)
                            .cloned()
                            .unwrap_or_else(|| {
                                panic!("replay fixture selected source has no serving root")
                            })
                    });
                stage_empty_current_state_root(
                    &mut writes,
                    &mut manifest,
                    serving_base.as_ref(),
                );
            }
            if manifest.current_state_scoped_ranges.is_some() {
                serving_manifests.insert(manifest.commit_id, manifest.clone());
            }
            crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
                &mut writes,
                &manifest,
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

    /// Moves a branch head. With the reachability ledger gone this is the whole
    /// publication a sweep needs to see: the superseded root is derived from the
    /// new head's canonical parent links.
    async fn publish_branch_head_release(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        new_control: BranchHeadControl,
        manifest: CommitStateManifest,
    ) {
        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, branch_id, new_control)
            .expect("released control should stage");
        persist_replay_closure_fixture(storage, writes, &[], std::slice::from_ref(&manifest)).await;
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

    /// Counts serving rows still addressed by one branch generation.
    async fn hot_generation_rows(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        generation: CommitId,
    ) -> usize {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("generation census read should open");
        let prefix = StoragePrefix {
            bytes: Bytes::from(crate::hot_state::hot_generation_scope_prefix(
                branch_id, generation,
            )),
        };
        let mut rows = 0;
        let mut cursor = crate::storage_adapter::StorageAdapterRead::begin_scan(
            &read,
            crate::hot_state::ROW_SPACE,
            prefix.to_range().expect("generation prefix should range"),
            StorageBeginScanOptions::default(),
        )
        .await
        .expect("generation census scan should open");
        loop {
            let (page, page_has_more) = cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await
                .expect("generation census page should load")
                .into_parts();
            rows += page.len();
            if !page_has_more {
                break;
            }
        }
        rows
    }

    /// A deleted branch strands its whole serving generation. The one
    /// reachability walk already knows which generations the live controls
    /// select, so the ordinary pass must reclaim it — no second sweeper, no
    /// retention ledger.
    #[tokio::test]
    async fn branch_deletion_reclaims_its_serving_generation_without_a_sweep() {
        let backend = Memory::new();
        Engine::initialize(backend.clone())
            .await
            .expect("generation fixture should initialize");
        let engine = Engine::new(backend.clone())
            .await
            .expect("generation fixture should open");
        let session = engine
            .open_session()
            .await
            .expect("generation fixture session should open");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "gc_generation_fixture",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("generation fixture schema should register");
        for row in 0..16 {
            session
                .execute(
                    "INSERT INTO gc_generation_fixture (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(format!("/row/{row}")),
                        Value::Text(format!(r#"{{"v":{row}}}"#)),
                    ],
                )
                .await
                .expect("generation fixture row should publish");
        }
        let branch = session
            .create_branch(crate::CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-0000000000a1".to_owned()),
                name: "gc-generation-dead".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("generation fixture branch should create");
        let branch_session = engine
            .open_session_at(branch.id.clone())
            .await
            .expect("generation fixture branch session should open");
        for row in 0..16 {
            branch_session
                .execute(
                    "UPDATE gc_generation_fixture SET value = CAST($2 AS JSONB) WHERE path = $1",
                    &[
                        Value::Text(format!("/row/{row}")),
                        Value::Text(format!(r#"{{"v":{}}}"#, row + 100)),
                    ],
                )
                .await
                .expect("generation fixture branch row should publish");
        }
        let storage = StorageAdapter::new(backend.clone());
        let generation = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("generation control read should open");
            BranchHeadControlContext::new()
                .reader(&read)
                .scan()
                .await
                .expect("generation controls should load")
                .into_iter()
                .find(|(branch_id, _)| branch_id == &branch.id)
                .expect("the disposable branch must have a control")
                .1
                .tracked_generation
        };
        let seeded = hot_generation_rows(&storage, &branch.id, generation).await;
        assert!(
            seeded > 0,
            "the disposable branch must materialize its own serving generation"
        );

        drop(branch_session);
        session
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.id.clone())],
            )
            .await
            .expect("generation fixture branch should delete");
        // A serving generation is reachable from exactly one place: its branch
        // control. Deleting that control retires the generation in the same
        // atomic write set, so there is nothing left for a sweep to find and
        // nothing for a publication ledger to remember on its behalf.
        assert_eq!(
            hot_generation_rows(&storage, &branch.id, generation).await,
            0,
            "branch deletion must retire its own serving generation ({seeded} rows)"
        );
        let plan = run_ordinary_repository_gc(&storage).await;
        assert_eq!(
            plan.sweep.reclaimed_generation_rows, 0,
            "the sweep has no generation debt left to collect"
        );
        assert_eq!(
            hot_generation_rows(&storage, &branch.id, generation).await,
            0,
            "no serving row of a deleted branch generation may survive GC"
        );

        // The surviving branch must still read exactly what it wrote.
        let survivors = session
            .execute("SELECT COUNT(*) AS rows FROM gc_generation_fixture", &[])
            .await
            .expect("surviving branch should still read")
            .rows()[0]
            .get::<i64>("rows")
            .expect("row count should decode");
        assert_eq!(survivors, 16);
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
        let closure = super::load_authenticated_repository_retention(&read, &controls)
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
                    row_pk: &change.row_pk,
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
            authored_history_bodies: None,
            snapshot_root: None,
        }
    }

    async fn stage_certified_native_manifest_fixture(
        read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        manifest: &mut CommitStateManifest,
        deltas: &[TrackedStateCommitDeltaRef<'_>],
        serving_parent: Option<crate::tracked_state::CertifiedCommitStateTopologyParent<'_>>,
    ) -> crate::tracked_state::StagedCommitStateManifest {
        let certified_body = crate::tracked_state::certify_authored_current_state_body(
            read,
            writes,
            manifest.commit_id,
            &manifest.change_account_id,
            &manifest.mutations,
            serving_parent.is_none(),
            deltas.iter().copied(),
        )
        .await
        .expect("GC fixture authored current-state body should certify");
        let mut publication = crate::tracked_state::stage_current_state_scoped_ranges_from_topology(
            read,
            writes,
            serving_parent.as_slice(),
            None,
            manifest.commit_id,
            &manifest.change_account_id,
            &manifest.mutations,
            certified_body,
        )
        .await
        .expect("GC fixture native current-state root should publish");
        publication
            .certify_authored_history_bodies(
                read,
                writes,
                &manifest.change_account_id,
                &manifest.mutations,
            )
            .await
            .expect("GC fixture authored history body inventory should certify");
        manifest.touched_scope_filter = publication.touched_scope_filter().clone();
        manifest.current_state_scoped_ranges = publication.root();
        manifest.authored_history_bodies = publication.authored_history_bodies();
        crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
            writes,
            manifest,
            &publication,
        )
        .expect("certified GC fixture manifest should stage")
    }

    fn stage_empty_current_state_root(
        writes: &mut StorageWriteSet,
        manifest: &mut CommitStateManifest,
        serving_base: Option<&CommitStateManifest>,
    ) {
        let scope = crate::tracked_state::scoped_range::ScopedRangePrefix::try_from_components([
            b"gc-fixture-empty".as_slice(),
        ])
        .expect("GC fixture scope should encode");
        let tree = crate::tracked_state::scoped_range::stage_scoped_range_tree(
            writes,
            [(
                crate::tracked_state::scoped_range::ScopedRangeCoverageMarker {
                    scope,
                    row_count: 0,
                    part_count: 0,
                },
                Vec::new(),
            )],
        )
        .expect("GC fixture empty serving tree should stage");
        manifest.current_state_scoped_ranges = Some(Box::new(
            crate::tracked_state::attest_scoped_range_root(
                manifest.commit_id,
                serving_base.map(|base| {
                    (
                        base.commit_id,
                        base.current_state_scoped_ranges
                            .as_deref()
                            .expect("GC fixture serving base should have a root"),
                    )
                }),
                &manifest.mutations,
                tree,
            )
            .expect("GC fixture serving root should attest"),
        ));
    }

    fn test_snapshot_chunk(commit_id: CommitId) -> ([u8; 32], Bytes) {
        crate::tracked_state::test_gc_leaf_chunk(commit_id.as_uuid().as_bytes())
    }

    fn test_snapshot_root(commit_id: CommitId) -> TrackedStateCommitRoot {
        let (root_hash, _root_bytes) = test_snapshot_chunk(commit_id);
        TrackedStateCommitRoot {
            commit_id,
            root_id: TrackedStateRootId::new(root_hash),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
        }
    }

    /// The retention closure is the single reachability implementation, and the
    /// standalone audit reads it. Both must reject the same malformed authority
    /// with the same reason.
    async fn assert_retention_closure_and_audit_fail_closed(
        storage: &StorageAdapter<Memory>,
        expected_message: &str,
    ) {
        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("offline authority read should open"),
        );
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .expect("offline authority controls should load");
        let closure_error = super::load_authenticated_repository_retention(&read, &controls)
            .await
            .expect_err("the retention closure must fail closed");
        let audit_error = super::audit_repository_gc_standalone_refs(&read)
            .await
            .expect_err("standalone audit must fail closed");
        assert_eq!(closure_error.message, audit_error.message);
        assert!(
            closure_error.message.contains(expected_message),
            "unexpected shared authority error: {}",
            closure_error.message
        );
    }

    #[tokio::test]
    async fn retention_closure_and_audit_reject_missing_and_malformed_required_authority() {
        // States the corruption-test need at the call site instead of smuggling
        // it in as a second `StorageSpace::mutable` declaration that reads like
        // a canonical one. This was the last raw re-declaration in the engine,
        // and closing it lets `UNCHECKED_SPACE_IDS` go empty.
        const MUTABLE_MANIFEST_SPACE: StorageSpace =
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
                .mutable_view_for_corruption_test();

        let (storage, commit_id, _, _, _) = gc_sweep_fixture().await;
        let mut writes = storage.new_write_set();
        writes.delete(
            MUTABLE_MANIFEST_SPACE,
            crate::tracked_state::commit_state_authority_key(commit_id),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("required manifest removal should commit");
        assert_retention_closure_and_audit_fail_closed(
            &storage,
            "incomplete split physical authority",
        )
        .await;

        let (storage, commit_id, _, _, _) = gc_sweep_fixture().await;
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
        assert_retention_closure_and_audit_fail_closed(&storage, "unsupported format").await;
    }

    #[tokio::test]
    async fn retention_closure_and_audit_reject_non_decreasing_and_cyclic_replay_authority() {
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
        assert_retention_closure_and_audit_fail_closed(&storage, "replay debt disagrees").await;

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
        assert_retention_closure_and_audit_fail_closed(&storage, "dependency cycle").await;
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

    fn recovery(branch_id: &str, recovered_head: &str, checkpoint: &str) -> CheckpointRecoveryRef {
        CheckpointRecoveryRef {
            branch_id: branch_id.to_string(),
            recovered_head_commit_id: CommitId::for_test_label(recovered_head),
            checkpoint_commit_id: CommitId::for_test_label(checkpoint),
            interval_has_commits: true,
        }
    }
}
