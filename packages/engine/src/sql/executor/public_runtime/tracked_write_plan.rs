use crate::filesystem::runtime::FilesystemTransactionState;
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::sql::physical_plan::TrackedWriteExecution;

use super::super::compiled::CompiledExecution;
use super::PreparedPublicWrite;

#[derive(Clone)]
pub(crate) struct TrackedTxnUnit {
    pub(crate) public_writes: Vec<PreparedPublicWrite>,
    pub(crate) public_write: PreparedPublicWrite,
    pub(crate) execution: TrackedWriteExecution,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) runtime_state: ExecutionRuntimeState,
    pub(crate) writer_key: Option<String>,
}

impl TrackedTxnUnit {
    pub(crate) fn should_emit_observe_tick(&self) -> bool {
        self.has_compiler_only_filesystem_changes()
            || !self
                .execution
                .semantic_effects
                .state_commit_stream_changes
                .is_empty()
    }

    pub(crate) fn has_compiler_only_filesystem_changes(&self) -> bool {
        self.execution.domain_change_batch.is_none() && !self.filesystem_state.files.is_empty()
    }

    pub(crate) fn is_merged_transaction_plan(&self) -> bool {
        self.public_writes.len() > 1
    }
}

pub(crate) fn build_tracked_txn_unit(
    public_write: &PreparedPublicWrite,
    execution: &TrackedWriteExecution,
    prepared: &CompiledExecution,
    writer_key: Option<&str>,
) -> TrackedTxnUnit {
    TrackedTxnUnit {
        public_writes: vec![public_write.clone()],
        public_write: public_write.clone(),
        execution: execution.clone(),
        filesystem_state: prepared.intent.filesystem_state.clone(),
        runtime_state: prepared.runtime_state.clone(),
        writer_key: writer_key.map(str::to_string),
    }
}
