use async_trait::async_trait;

use crate::contracts::PendingView;
use crate::contracts::TrackedCommitExecutionOutcome;
use crate::contracts::{LixFunctionProvider, SharedFunctionProvider};
use crate::contracts::{PendingPublicCommitSession, PreparedPublicReadArtifact};
#[cfg(test)]
use crate::contracts::{
    TrackedWriteOperation, TrackedWriteRow, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::LixError;
use crate::{LixBackendTransaction, QueryResult};

#[cfg(not(test))]
use crate::execution::write::buffered::TrackedTxnUnit;
#[cfg(test)]
use crate::execution::write::buffered::{TrackedTxnUnit, WriteDelta, WriteJournal};
use crate::execution::write::filesystem::runtime::BinaryBlobWrite;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct TransactionDelta {
    #[serde(default)]
    pub tracked_writes: Vec<TrackedWriteRow>,
    #[serde(default)]
    pub untracked_writes: Vec<UntrackedWriteRow>,
}

#[cfg(test)]
#[derive(Clone, Default)]
pub struct TransactionJournal {
    inner: WriteJournal,
}

#[cfg(test)]
impl std::fmt::Debug for TransactionJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionJournal")
            .field("staged_count", &self.staged_count())
            .field("continuation_safe", &self.continuation_safe())
            .field("aggregated_delta", &self.aggregated_delta())
            .finish()
    }
}

#[cfg(test)]
impl TransactionJournal {
    pub fn stage(&mut self, delta: TransactionDelta) -> Result<(), LixError> {
        if delta.tracked_writes.is_empty() && delta.untracked_writes.is_empty() {
            return Ok(());
        }
        self.inner
            .stage_delta(WriteDelta::from_public_delta(delta)?)
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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CommitOutcome {
    pub tracked_upserts: usize,
    pub tracked_tombstones: usize,
    pub untracked_upserts: usize,
    pub untracked_deletes: usize,
}

#[cfg(test)]
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

#[async_trait(?Send)]
pub(crate) trait WriteExecutionBindings {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError>;

    async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        writes: &[BinaryBlobWrite],
    ) -> Result<(), LixError>;

    async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError>;

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Result<(), LixError>;

    async fn execute_public_tracked_append_txn_with_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        unit: &TrackedTxnUnit,
        pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
    ) -> Result<TrackedCommitExecutionOutcome, LixError>;
}

#[derive(Default)]
pub(crate) struct DeferredTransactionSideEffects {
    pub(crate) filesystem_state:
        crate::execution::write::filesystem::runtime::FilesystemTransactionState,
}
