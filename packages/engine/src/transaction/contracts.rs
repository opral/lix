use crate::live_state::tracked::{TrackedWriteOperation, TrackedWriteRow};
use crate::live_state::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use crate::session::contracts::SessionStateDelta;
use crate::state::stream::StateCommitStreamChange;
use crate::LixError;

use super::write_plan::{WriteDelta, WriteJournal};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct TransactionDelta {
    #[serde(default)]
    pub tracked_writes: Vec<TrackedWriteRow>,
    #[serde(default)]
    pub untracked_writes: Vec<UntrackedWriteRow>,
}

impl TransactionDelta {
    pub fn is_empty(&self) -> bool {
        self.tracked_writes.is_empty() && self.untracked_writes.is_empty()
    }
}

#[derive(Clone, Default)]
pub struct TransactionJournal {
    inner: WriteJournal,
}

impl std::fmt::Debug for TransactionJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionJournal")
            .field("staged_count", &self.staged_count())
            .field("continuation_safe", &self.continuation_safe())
            .field("aggregated_delta", &self.aggregated_delta())
            .finish()
    }
}

impl TransactionJournal {
    pub fn stage(&mut self, delta: TransactionDelta) -> Result<(), LixError> {
        if delta.is_empty() {
            return Ok(());
        }
        self.inner
            .stage_delta(WriteDelta::from_public_delta(delta)?)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn staged_count(&self) -> usize {
        self.inner.staged_count()
    }

    pub fn continuation_safe(&self) -> bool {
        self.inner.continuation_safe()
    }

    pub fn aggregated_delta(&self) -> TransactionDelta {
        self.inner.aggregated_public_delta()
    }

    pub(crate) fn write_journal(&self) -> &WriteJournal {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CommitOutcome {
    pub tracked_upserts: usize,
    pub tracked_tombstones: usize,
    pub untracked_upserts: usize,
    pub untracked_deletes: usize,
}

impl CommitOutcome {
    pub fn merge(&mut self, other: CommitOutcome) {
        self.tracked_upserts += other.tracked_upserts;
        self.tracked_tombstones += other.tracked_tombstones;
        self.untracked_upserts += other.untracked_upserts;
        self.untracked_deletes += other.untracked_deletes;
    }

    pub(crate) fn from_tracked_writes(writes: &[TrackedWriteRow]) -> Self {
        let mut outcome = Self::default();
        for write in writes {
            match write.operation {
                TrackedWriteOperation::Upsert => outcome.tracked_upserts += 1,
                TrackedWriteOperation::Tombstone => outcome.tracked_tombstones += 1,
            }
        }
        outcome
    }

    pub(crate) fn from_untracked_writes(writes: &[UntrackedWriteRow]) -> Self {
        let mut outcome = Self::default();
        for write in writes {
            match write.operation {
                UntrackedWriteOperation::Upsert => outcome.untracked_upserts += 1,
                UntrackedWriteOperation::Delete => outcome.untracked_deletes += 1,
            }
        }
        outcome
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct TransactionCommitOutcome {
    #[serde(default, skip_serializing_if = "SessionStateDelta::is_empty")]
    pub session_delta: SessionStateDelta,
    #[serde(default)]
    pub invalidate_deterministic_settings_cache: bool,
    #[serde(default)]
    pub invalidate_installed_plugins_cache: bool,
    #[serde(default)]
    pub refresh_public_surface_registry: bool,
    #[serde(default)]
    pub state_commit_stream_changes: Vec<StateCommitStreamChange>,
}

impl TransactionCommitOutcome {
    pub(crate) fn merge(&mut self, other: TransactionCommitOutcome) {
        self.session_delta.merge(other.session_delta);
        self.invalidate_deterministic_settings_cache |=
            other.invalidate_deterministic_settings_cache;
        self.invalidate_installed_plugins_cache |= other.invalidate_installed_plugins_cache;
        self.refresh_public_surface_registry |= other.refresh_public_surface_registry;
        self.state_commit_stream_changes
            .extend(other.state_commit_stream_changes);
    }
}
