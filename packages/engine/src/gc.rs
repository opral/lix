//! Checkpoint recovery roots and repository garbage collection.
//!
//! Recovery refs are local, mutable roots. They deliberately live outside the
//! changelog: rotating a ref must not create history that itself keeps the
//! recovered commit alive. The checkpoint transaction stages the rotation in
//! the same storage write set that publishes the compacted checkpoint.

use std::collections::BTreeMap;
use std::time::Instant;

use bytes::Bytes;

use crate::branch::BranchHeadControlContext;
use crate::changelog::{ChangelogContext, ChangelogWriter, CommitId, GcPlan, GcRoot};
use crate::live_state::{LiveStateIndexContext, stage_collect_stale_working_diff_indexes};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteSet,
};
use crate::{LixError, storage_codec};

pub(crate) const CHECKPOINT_RECOVERY_REF_NAMESPACE: &str = "checkpoint.recovery_ref.v3";
pub(crate) const CHECKPOINT_RECOVERY_REF_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0008_0001),
    CHECKPOINT_RECOVERY_REF_NAMESPACE,
);
pub(crate) const CHECKPOINT_GC_STATE_NAMESPACE: &str = "checkpoint.gc_state.v1";
pub(crate) const CHECKPOINT_GC_STATE_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0008_0002), CHECKPOINT_GC_STATE_NAMESPACE);

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
/// prior interval from the next GC root set without ever exposing an
/// intermediate root-less checkpoint.
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
/// Content-addressed tree/CAS orphan repair is intentionally an offline path.
pub(crate) async fn stage_repository_gc<S>(
    store: S,
    writes: &mut StorageWriteSet,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let total_started = Instant::now();
    let phase_started = Instant::now();
    let flat_rows = LiveStateIndexContext::new()
        .reader(store.clone())
        .scan_all_index_rows()
        .await?;
    let mut roots = flat_rows
        .iter()
        .map(|row| GcRoot::StandaloneChange(row.change_id))
        .collect::<Vec<_>>();
    // v6 branch controls, not their public `lix_branch_ref` projection rows,
    // are the authoritative GC roots. Explicit lifecycle rows may remain in
    // flat state for SQL compatibility, but a stale projection must never
    // keep a superseded commit alive.
    for (_branch_id, control) in BranchHeadControlContext::new()
        .reader(store.clone())
        .scan()
        .await?
    {
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
    let mut changelog_store = store.clone();
    let changelog_plan = ChangelogContext::new()
        .writer(&mut changelog_store, writes)
        .collect_garbage(&roots)
        .await?;
    let changelog_us = elapsed_micros(phase_started);
    // Changelog reachability is the logical correctness boundary. A
    // tracked root has the same commit id, so dead root metadata can be
    // deleted directly without inventorying every retained root.
    let sweep_tracked_commit_roots = changelog_plan.sweep.commits.clone();

    // Removing a dead changelog commit invalidates its derived root metadata
    // and its commit-addressed delta index. Immutable tree/CAS payloads remain
    // content-addressed maintenance work, but delta rows have no shared
    // ownership and must be reclaimed in the same logical GC pass.
    let phase_started = Instant::now();
    for commit_id in &sweep_tracked_commit_roots {
        crate::tracked_state::stage_delete_commit_root(writes, *commit_id);
        crate::tracked_state::stage_delete_commit_deltas(&store, writes, *commit_id).await?;
    }
    // Checkpoint publication leaves prior dirty-index generations unreachable
    // in O(1). Reclaim those auxiliary records only in the asynchronous GC
    // pass so a foreground checkpoint never pays a history-sized delete cost.
    stage_collect_stale_working_diff_indexes(&store, writes).await?;
    let tracked_root_stage_us = elapsed_micros(phase_started);

    Ok(RepositoryGcPlan {
        changelog: changelog_plan,
        sweep: RepositoryGcSweep {
            tracked_commit_roots: sweep_tracked_commit_roots,
        },
        profile: RepositoryGcProfile {
            root_discovery_us,
            changelog_us,
            tracked_root_stage_us,
            total_us: elapsed_micros(total_started),
        },
    })
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use crate::branch::BranchHeadControlContext;
    use crate::changelog::CommitId;
    use crate::live_state::LiveStateIndexContext;
    use crate::storage_adapter::{
        Memory, SharedStorageAdapterRead, StorageAdapter, StorageReadOptions, StorageWriteOptions,
    };
    use crate::tracked_state::TrackedStateContext;

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
    async fn repository_gc_keeps_heads_rooted_only_by_v6_controls() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let live_index = LiveStateIndexContext::new();
        let receipt = crate::init::initialize(storage.clone(), &tracked_state, &live_index)
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

    fn recovery(branch_id: &str, recovered_head: &str, checkpoint: &str) -> CheckpointRecoveryRef {
        CheckpointRecoveryRef {
            branch_id: branch_id.to_string(),
            recovered_head_commit_id: CommitId::for_test_label(recovered_head),
            checkpoint_commit_id: CommitId::for_test_label(checkpoint),
            interval_has_commits: true,
        }
    }
}
