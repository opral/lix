//! Deferred repository-GC boundary.
//!
//! The former repository collector owned deleted tracked-state, changelog,
//! branch-control, and binary-CAS spaces.  Those physical owners are no
//! longer part of the storage layout.  GC publication will be lowered through
//! the transaction-owned ForkTree reachability plan in the W5 wave; until
//! then every old entry point fails closed before producing a write plan.

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

fn gc_not_lowered() -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "repository GC and checkpoint recovery publication are not lowered through ForkTree yet",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRecoveryRef {
    pub(crate) branch_id: String,
    pub(crate) recovered_head_commit_id: CommitId,
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) interval_has_commits: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CheckpointGcState {
    pub(crate) checkpoint_sequence: u64,
    pub(crate) last_gc_sequence: u64,
    pub(crate) collectible_interval_count: u64,
}

impl CheckpointGcState {
    pub(crate) fn add_collectible_interval(&mut self, interval_has_commits: bool) {
        if interval_has_commits {
            self.collectible_interval_count = self.collectible_interval_count.saturating_add(1);
        }
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

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcPlan {
    pub(crate) changelog: RepositoryGcChangelog,
    pub(crate) sweep: RepositoryGcSweep,
    pub(crate) profile: RepositoryGcProfile,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcChangelog {
    pub(crate) sweep: RepositoryGcChangelogSweep,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcChangelogSweep {
    pub(crate) commits: Vec<()>,
    pub(crate) changes: Vec<()>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcSweep {
    pub(crate) tracked_commit_roots: Vec<()>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcProfile {
    pub(crate) root_discovery_us: u64,
    pub(crate) changelog_us: u64,
    pub(crate) tracked_root_stage_us: u64,
    pub(crate) total_us: u64,
}

pub(crate) fn stage_recovery_ref_rotation(
    _writes: &mut StorageWriteSet,
    _recovery: &CheckpointRecoveryRef,
) -> Result<(), LixError> {
    Err(gc_not_lowered())
}

pub(crate) fn stage_checkpoint_gc_state(
    _writes: &mut StorageWriteSet,
    _state: &CheckpointGcState,
) -> Result<(), LixError> {
    Err(gc_not_lowered())
}

pub(crate) fn stage_reachability_queue_seed(_writes: &mut StorageWriteSet) -> Result<(), LixError> {
    Err(gc_not_lowered())
}

pub(crate) async fn load_recovery_ref<R>(
    _read: &R,
    _branch_id: &str,
) -> Result<Option<CheckpointRecoveryRef>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    Err(gc_not_lowered())
}

pub(crate) async fn load_checkpoint_gc_state<R>(_read: &R) -> Result<CheckpointGcState, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    Err(gc_not_lowered())
}

pub(crate) async fn stage_repository_gc<S>(
    _store: S,
    _writes: &mut StorageWriteSet,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead,
{
    Err(gc_not_lowered())
}

pub(crate) async fn stage_repository_gc_with_preconditions<S>(
    _store: S,
    _writes: &mut StorageWriteSet,
    _preconditions: &mut Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead,
{
    Err(gc_not_lowered())
}
