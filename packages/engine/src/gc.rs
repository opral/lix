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
use crate::changelog::{
    CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_SPACE, ChangeId, ChangeRecord, ChangeScanRequest,
    ChangelogContext, ChangelogReader, CommitId, CommitScanRequest, GcLiveSet, GcPlan, GcRepairSet,
    GcRoot, GcSweepSet, change_key, commit_change_id_key, commit_key,
};
use crate::json_store::{
    JsonRef, JsonSlot, JsonStoreContext, JsonStoreWriter, UntrackedJsonReclaimCandidate,
};
use crate::live_state::{TrackedHeadContext, stage_collect_stale_working_diff_indexes};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions,
    StorageKey, StoragePrefix, StorageProjectedValue, StorageScanOptions, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet,
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

const CHECKPOINT_RECOVERY_REF_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_GC_STATE_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_GC_STATE_KEY: &[u8] = b"repository";

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

#[derive(musli::Encode, musli::Decode)]
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
pub(crate) async fn stage_repository_gc<S>(
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
        },
        profile: RepositoryGcProfile {
            root_discovery_us,
            changelog_us,
            tracked_root_stage_us,
            total_us: elapsed_micros(total_started),
        },
    })
}

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

    // GC is destructive, so the hard-cut commit-state manifest must be
    // present and agree with every live changelog topology projection before
    // payload reachability or sweep mutations are derived. Missing authority
    // must not silently turn a live commit into an empty mutation owner.
    for commit_id in &live_commits {
        let commit = commits
            .get(*commit_id)
            .expect("live commit existence was checked during graph walk");
        let authority = packed.commits.get(commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("live commit '{commit_id}' has no commit-state authority"),
            )
        })?;
        let topology = &authority.authority;
        let manifest_rootless = topology.replay_debt.depth > 0;
        if topology.generation != commit.generation
            || topology.parent_commit_ids != commits.parent_commit_ids(commit)
            || topology.commit_change_id != commit.change_id
            || topology.account_id != commit.account_id
            || topology.created_at != commit.created_at
            || manifest_rootless != commit.tracked_state_rootless
            || topology.replay_debt.depth != commit.tracked_state_rootless_depth
            || topology.replay_debt.rows != commit.tracked_state_rootless_rows
            || topology.replay_debt.bytes != commit.tracked_state_rootless_bytes
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("live commit '{commit_id}' disagrees with its commit-state authority"),
            ));
        }
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
        if !packed.commits.contains_key(&commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("canonical mutation authority '{commit_id}' is missing"),
            ));
        }
    }
    let mut live_catalog_nodes = BTreeSet::<[u8; 32]>::new();
    let mut live_current_state_directory_nodes = BTreeSet::<[u8; 32]>::new();
    let mut live_current_state_data_parts = BTreeSet::<[u8; 32]>::new();
    let mut live_current_state_ref_summaries = BTreeMap::<[u8; 32], [u8; 32]>::new();
    let mut live_current_state_payload_hashes = BTreeSet::<[u8; 32]>::new();
    let mut catalog_roots = BTreeMap::new();
    let mut live_manifests = BTreeMap::new();
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
        live_manifests.insert(commit_id, manifest.clone());
        let Some(root) = manifest.current_state_catalog.as_ref() else {
            continue;
        };
        if let Some(previous) = catalog_roots.insert(root.root_id, root.clone()) {
            if previous.entry_count != root.entry_count {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "live current-state catalogs disagree about root '{:?}'",
                        root.root_id
                    ),
                ));
            }
        }
    }
    for manifest in live_manifests.values() {
        let parent = manifest
            .parent_commit_ids
            .first()
            .and_then(|parent_id| live_manifests.get(parent_id));
        crate::tracked_state::validate_current_state_catalog_parent_manifest(manifest, parent)?;
        crate::tracked_state::validate_current_state_catalog_transition_root(
            store, manifest, parent,
        )
        .await?;
    }

    // Catalog roots and immutable directory roots are content addressed and
    // may be shared by many live commits. Validate each distinct authority and
    // directory only once; otherwise a long unchanged branch interval turns
    // GC into O(commits * scopes * directory parts) work.
    let catalog_root_values = catalog_roots
        .values()
        .map(|root| (**root).clone())
        .collect::<Vec<_>>();
    let (catalog_nodes, unique_entries) =
        crate::tracked_state::load_current_state_catalog_reachability_many(
            store,
            &catalog_root_values,
        )
        .await?;
    live_catalog_nodes.extend(catalog_nodes);
    let mut catalog_entries = BTreeMap::new();
    for set in unique_entries {
        let identity = storage_codec::encode("GC current-state catalog entry", &set)?;
        catalog_entries.entry(identity).or_insert(set);
    }

    let mut directory_roots = BTreeMap::new();
    for set in catalog_entries.values() {
        if let Some(previous) = directory_roots.insert(set.directory.root_id, set.directory.clone())
        {
            if previous != set.directory {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "live current-state catalogs disagree about directory root '{:?}'",
                        set.directory.root_id
                    ),
                ));
            }
        }
    }
    let directories = directory_roots.values().cloned().collect::<Vec<_>>();
    let (directory_nodes, descriptor_sets) =
        crate::tracked_state::load_current_state_part_directory_reachability_many(
            store,
            &directories,
        )
        .await?;
    live_current_state_directory_nodes.extend(directory_nodes);
    for descriptors in descriptor_sets {
        for descriptor in descriptors {
            match descriptor.source_kind {
                0 => {
                    let owner = CommitId::new(uuid::Uuid::from_bytes(descriptor.owner_commit_id));
                    if !packed.commits.contains_key(&owner) {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "live current-state directory references missing immutable-part owner '{owner}'"
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
                            "native current-state descriptors disagree about payload refs",
                        ));
                    }
                }
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "live current-state directory contains an unknown part source",
                    ));
                }
            }
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
        crate::tracked_state::CURRENT_STATE_CATALOG_SPACE,
        &live_catalog_nodes,
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
        crate::tracked_state::CURRENT_STATE_PART_DIRECTORY_SPACE,
        &live_current_state_directory_nodes,
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

#[derive(Clone, Debug)]
struct GcCommitInventoryEntry {
    commit_id: CommitId,
    generation: u64,
    tracked_state_rootless: bool,
    tracked_state_rootless_depth: u16,
    tracked_state_rootless_rows: u64,
    tracked_state_rootless_bytes: u64,
    change_id: ChangeId,
    account_id: String,
    created_at: crate::common::LixTimestamp,
    parent_start: usize,
    parent_len: usize,
}

#[derive(Debug, Default)]
struct GcCommitInventory {
    entries: Vec<GcCommitInventoryEntry>,
    parent_commit_ids: Vec<CommitId>,
}

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
                generation: commit.generation,
                tracked_state_rootless: commit.tracked_state_rootless,
                tracked_state_rootless_depth: commit.tracked_state_rootless_depth,
                tracked_state_rootless_rows: commit.tracked_state_rootless_rows,
                tracked_state_rootless_bytes: commit.tracked_state_rootless_bytes,
                change_id: commit.change_id,
                account_id: commit.account_id,
                created_at: commit.created_at,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::branch::{BranchHeadControlContext, stage_branch_head_control};
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
    use crate::{Engine, GLOBAL_BRANCH_ID, Value};
    use bytes::Bytes;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use super::{
        CheckpointGcState, CheckpointRecoveryRef, load_checkpoint_gc_state, load_recovery_ref,
        load_recovery_refs, stage_checkpoint_gc_state, stage_recovery_ref_rotation,
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
            crate::tracked_state::CURRENT_STATE_CATALOG_SPACE,
            crate::tracked_state::CURRENT_STATE_PART_DIRECTORY_SPACE,
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
            crate::tracked_state::CURRENT_STATE_CATALOG_SPACE,
            crate::tracked_state::CURRENT_STATE_PART_DIRECTORY_SPACE,
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
            crate::tracked_state::CURRENT_STATE_CATALOG_SPACE,
            crate::tracked_state::CURRENT_STATE_PART_DIRECTORY_SPACE,
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
        let plan = super::stage_repository_gc(read, &mut writes)
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
    async fn authority_gc_rejects_live_topology_drift_before_staging_deletes() {
        let storage = StorageAdapter::new(Memory::new());
        let live = gc_authority_record("gc-live-authority-drift");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority-drift fixture read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: vec![live.clone()],
                changes: Vec::new(),
            })
            .await
            .expect("authority-drift commit should stage");
        let mut drifted = CommitStateManifest {
            commit_id: live.commit_id,
            generation: live.generation,
            parent_commit_ids: live.parent_commit_ids.clone(),
            commit_change_id: live.change_id,
            account_id: live.account_id.clone(),
            created_at: live.created_at,
            replay_debt: CommitStateReplayDebt {
                depth: live.tracked_state_rootless_depth,
                rows: live.tracked_state_rootless_rows,
                bytes: live.tracked_state_rootless_bytes,
            },
            mutations: CommitStateMutationInventory::default(),
            current_state_catalog: None,
            current_state_coverage_anchor: None,
            snapshot_root: None,
        };
        drifted.generation += 1;
        stage_commit_state_manifest(&mut writes, &drifted)
            .expect("internally valid but projection-drifted authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authority-drift fixture should commit");

        let read = SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("authority-drift GC read should open"),
        );
        let mut gc_writes = storage.new_write_set();
        let error = super::plan_and_stage_authority_gc(
            &read,
            &mut gc_writes,
            &[GcRoot::BranchHead(live.commit_id)],
        )
        .await
        .expect_err("GC must reject topology projection drift");
        assert!(
            error
                .message
                .contains("disagrees with its commit-state authority")
        );
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
                format_version: 1,
                commit_id: source_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 1,
                tracked_state_rootless_rows: 1,
                tracked_state_rootless_bytes: 1,
                change_id: ChangeId::for_test_label("gc-tombstone-alias-source-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 1,
                commit_id: alias_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 1,
                tracked_state_rootless_rows: 1,
                tracked_state_rootless_bytes: 1,
                change_id: ChangeId::for_test_label("gc-tombstone-alias-live-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 1,
                commit_id: authority_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 1,
                tracked_state_rootless_rows: 1,
                tracked_state_rootless_bytes: 1,
                change_id: ChangeId::for_test_label("gc-tombstone-alias-authority-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 1,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![alias_commit, authority_commit],
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 2,
                tracked_state_rootless_rows: 2,
                tracked_state_rootless_bytes: 2,
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
        for record in &commits {
            stage_test_commit_state_manifest(
                &mut writes,
                record,
                inventories
                    .get(&record.commit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
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
                format_version: 1,
                commit_id: live_parent,
                generation: 0,
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 1,
                tracked_state_rootless_rows: 1,
                tracked_state_rootless_bytes: 1,
                change_id: ChangeId::for_test_label("authority-gc-live-parent-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 1,
                commit_id: live_head,
                generation: 1,
                parent_commit_ids: vec![live_parent],
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 2,
                tracked_state_rootless_rows: 2,
                tracked_state_rootless_bytes: 2,
                change_id: ChangeId::for_test_label("authority-gc-live-head-header"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
            },
            CommitRecord {
                format_version: 1,
                commit_id: dead_commit,
                generation: 0,
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: true,
                tracked_state_rootless_depth: 1,
                tracked_state_rootless_rows: 1,
                tracked_state_rootless_bytes: 1,
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
            stage_test_commit_state_manifest(
                &mut writes,
                record,
                inventories
                    .get(&record.commit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
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
                Some(control.generation),
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
        super::stage_repository_gc(read, &mut writes)
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
            format_version: 1,
            commit_id: CommitId::for_test_label(label),
            generation: 0,
            parent_commit_ids: Vec::new(),
            tracked_state_rootless: true,
            tracked_state_rootless_depth: 1,
            tracked_state_rootless_rows: 0,
            tracked_state_rootless_bytes: 0,
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

    fn stage_test_commit_state_manifest(
        writes: &mut crate::storage_adapter::StorageWriteSet,
        record: &CommitRecord,
        mutations: CommitStateMutationInventory,
    ) {
        stage_commit_state_manifest(
            writes,
            &CommitStateManifest {
                commit_id: record.commit_id,
                generation: record.generation,
                parent_commit_ids: record.parent_commit_ids.clone(),
                commit_change_id: record.change_id,
                account_id: record.account_id.clone(),
                created_at: record.created_at,
                replay_debt: CommitStateReplayDebt {
                    depth: record.tracked_state_rootless_depth,
                    rows: record.tracked_state_rootless_rows,
                    bytes: record.tracked_state_rootless_bytes,
                },
                mutations,
                current_state_catalog: None,
                current_state_coverage_anchor: None,
                snapshot_root: None,
            },
        )
        .expect("GC fixture commit-state manifest should stage");
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
