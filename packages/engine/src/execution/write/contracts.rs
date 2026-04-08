use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::contracts::artifacts::{
    CanonicalCommitReceipt, PendingPublicCommitSession, PreparedPublicReadArtifact,
    PublicDomainChange, RowIdentity, SessionStateDelta, StateCommitStreamChange,
};
#[cfg(test)]
use crate::contracts::artifacts::{
    TrackedWriteOperation, TrackedWriteRow, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::contracts::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::contracts::traits::PendingView;
use crate::LixError;
use crate::{LixBackendTransaction, QueryResult};

#[cfg(test)]
use crate::execution::write::buffered::{TrackedTxnUnit, WriteDelta, WriteJournal};
#[cfg(not(test))]
use crate::execution::write::buffered::TrackedTxnUnit;
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedCommitExecutionOutcome {
    pub(crate) receipt: Option<CanonicalCommitReceipt>,
    pub(crate) applied_domain_changes: Vec<PublicDomainChange>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) next_active_version_id: Option<String>,
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

    async fn apply_writer_key_annotations_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        annotations: &BTreeMap<RowIdentity, Option<String>>,
    ) -> Result<(), LixError>;
}

#[derive(Default)]
pub(crate) struct DeferredTransactionSideEffects {
    pub(crate) filesystem_state:
        crate::execution::write::filesystem::runtime::FilesystemTransactionState,
}

#[derive(Clone)]
pub(crate) struct PreparedWriteRuntimeState {
    deterministic_mode_enabled: bool,
    functions: SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
}

impl std::fmt::Debug for PreparedWriteRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWriteRuntimeState")
            .field(
                "deterministic_mode_enabled",
                &self.deterministic_mode_enabled,
            )
            .finish_non_exhaustive()
    }
}

impl PreparedWriteRuntimeState {
    pub(crate) fn new(
        deterministic_mode_enabled: bool,
        functions: SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Self {
        Self {
            deterministic_mode_enabled,
            functions,
        }
    }

    pub(crate) fn functions(&self) -> &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>> {
        &self.functions
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferedWriteExecutionInput {
    writer_key: Option<String>,
    active_version_id: String,
    active_account_ids: Vec<String>,
}

impl BufferedWriteExecutionInput {
    pub(crate) fn new(
        writer_key: Option<String>,
        active_version_id: impl Into<String>,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            writer_key,
            active_version_id: active_version_id.into(),
            active_account_ids,
        }
    }

    pub(crate) fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }

    pub(crate) fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    pub(crate) fn active_account_ids(&self) -> &[String] {
        &self.active_account_ids
    }

    pub(crate) fn apply_session_delta(&mut self, delta: &SessionStateDelta) {
        if let Some(version_id) = &delta.next_active_version_id {
            self.active_version_id = version_id.clone();
        }
        if let Some(active_account_ids) = &delta.next_active_account_ids {
            self.active_account_ids = active_account_ids.clone();
        }
    }
}
