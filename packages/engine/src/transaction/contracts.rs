use std::collections::BTreeSet;

use crate::live_tracked_state::{TrackedWriteOperation, TrackedWriteRow};
use crate::live_untracked_state::{UntrackedWriteOperation, UntrackedWriteRow};
use crate::LixError;

use super::write_plan::{MutationJournal, TxnDelta};

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
    inner: MutationJournal,
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
        self.inner.stage_delta(TxnDelta::from_public_delta(delta)?)
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

    pub(crate) fn mutation_journal(&self) -> &MutationJournal {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CommitOutcome {
    pub tracked_upserts: usize,
    pub tracked_tombstones: usize,
    pub untracked_upserts: usize,
    pub untracked_deletes: usize,
    #[serde(default)]
    pub ensured_untracked_schemas: Vec<String>,
}

impl CommitOutcome {
    pub fn merge(&mut self, other: CommitOutcome) {
        self.tracked_upserts += other.tracked_upserts;
        self.tracked_tombstones += other.tracked_tombstones;
        self.untracked_upserts += other.untracked_upserts;
        self.untracked_deletes += other.untracked_deletes;
        let mut ensured = self
            .ensured_untracked_schemas
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        ensured.extend(other.ensured_untracked_schemas);
        self.ensured_untracked_schemas = ensured.into_iter().collect();
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
