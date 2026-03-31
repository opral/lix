mod compile;
mod effects;
mod execute;
mod internal_apply;
mod planned_write;
mod planned_write_runner;
mod runtime;
mod tracked_apply;
mod untracked_apply;

pub(crate) use crate::commit::PendingPublicCommitSession;
pub(crate) use crate::filesystem::runtime::{
    build_filesystem_payload_domain_changes_insert,
    filesystem_transaction_state_has_binary_payloads, merge_filesystem_transaction_state,
    FilesystemTransactionFileState, FilesystemTransactionState,
};
pub(crate) use crate::sql::executor::contracts::effects::{
    FilesystemPayloadDomainChange, PlanEffects,
};
pub(crate) use crate::sql::executor::contracts::planned_statement::MutationRow;
pub(crate) use crate::sql::executor::execution_program::ExecutionContext;
pub(crate) use crate::sql::executor::{
    build_tracked_txn_unit, semantic_plan_effects_from_domain_changes,
    state_commit_stream_operation, PreparedPublicWrite, TrackedTxnUnit,
};
pub(crate) use crate::sql::logical_plan::public_ir::{
    OptionalTextPatch, PlannedStateRow, WriteLane, WriteMode,
};
pub(crate) use crate::sql::logical_plan::ResultContract;
pub(crate) use crate::sql::physical_plan::{
    PublicWriteExecutionPartition, UntrackedWriteExecution,
};
pub(crate) use crate::sql::semantic_ir::semantics::domain_changes::DomainChangeBatch;
pub(crate) use crate::transaction::PendingTransactionView;

pub(crate) use effects::mirror_public_registered_schema_bootstrap_rows;
pub(crate) use execute::{
    execute_bound_statement_template_instance_in_borrowed_write_transaction,
    execute_bound_statement_template_instance_in_write_transaction,
    execute_parsed_statements_in_borrowed_write_transaction,
    execute_parsed_statements_in_write_transaction, execute_with_options_in_write_transaction,
};
pub(crate) use planned_write::{
    BufferedWriteJournal, PendingFilesystemOverlay, PendingRegisteredSchemaOverlay,
    PendingSemanticOverlay, PendingSemanticRow, PendingSemanticStorage,
    PendingWorkspaceWriterKeyOverlay, PlannedInternalWriteUnit, PlannedPublicUntrackedWriteUnit,
    PlannedWriteDelta, PlannedWriteUnit,
};
pub(crate) use planned_write_runner::execute_planned_write_delta;
pub(crate) use runtime::{
    empty_public_write_execution_outcome, execute_compiled_execution_step_with_transaction,
    execute_internal_execution_with_transaction, CompiledExecutionRoute, CompiledExecutionStep,
    CompiledExecutionStepResult, SqlExecutionOutcome,
};
