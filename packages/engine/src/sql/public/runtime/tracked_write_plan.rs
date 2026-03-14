use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::filesystem::pending_file_writes::PendingFileWrite;
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::shared_path::PreparedExecutionContext;

use super::{PreparedPublicWrite, TrackedWriteExecution};

#[derive(Clone)]
pub(crate) struct TrackedWriteTxnPlan {
    pub(crate) public_write: PreparedPublicWrite,
    pub(crate) execution: TrackedWriteExecution,
    pub(crate) pending_file_writes: Vec<PendingFileWrite>,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) writer_key: Option<String>,
}

impl TrackedWriteTxnPlan {
    pub(crate) fn should_emit_observe_tick(&self) -> bool {
        self.execution.lazy_exact_file_update.is_some()
            || !self
                .execution
                .semantic_effects
                .state_commit_stream_changes
                .is_empty()
    }

    pub(crate) fn has_lazy_exact_file_update(&self) -> bool {
        self.execution.lazy_exact_file_update.is_some()
    }
}

pub(crate) fn build_tracked_write_txn_plan(
    public_write: &PreparedPublicWrite,
    execution: &TrackedWriteExecution,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> TrackedWriteTxnPlan {
    TrackedWriteTxnPlan {
        public_write: public_write.clone(),
        execution: execution.clone(),
        pending_file_writes: prepared.intent.pending_file_writes.clone(),
        functions: prepared.functions.clone(),
        writer_key: writer_key.map(str::to_string),
    }
}
