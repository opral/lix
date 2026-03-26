mod compile;
mod effects;
mod execute;
mod planned_write;
mod planned_write_runner;
mod runtime;

pub(crate) use crate::sql::execution::contracts::effects::{
    FilesystemPayloadDomainChange, PlanEffects,
};
pub(crate) use crate::sql::execution::contracts::planned_statement::MutationRow;
pub(crate) use crate::sql::execution::contracts::result_contract::ResultContract;
pub(crate) use crate::sql::execution::execution_program::ExecutionContext;
pub(crate) use crate::filesystem::runtime::{
    build_filesystem_payload_domain_changes_insert, filesystem_transaction_state_has_binary_payloads,
    merge_filesystem_transaction_state, FilesystemTransactionFileState,
    FilesystemTransactionState,
};
pub(crate) use crate::transaction::PendingTransactionView;
pub(crate) use crate::sql::execution::shared_path::{
    apply_public_version_last_checkpoint_side_effects, empty_public_write_execution_outcome,
};
pub(crate) use crate::sql::public::planner::ir::{
    OptionalTextPatch, PlannedStateRow, WriteLane, WriteMode,
};
pub(crate) use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
pub(crate) use crate::sql::public::runtime::{
    build_tracked_txn_unit, semantic_plan_effects_from_domain_changes,
    state_commit_stream_operation, PreparedPublicWrite, PublicWriteExecutionPartition,
    TrackedTxnUnit, UntrackedWriteExecution,
};
pub(crate) use crate::sql_support::text::escape_sql_string;
pub(crate) use crate::canonical::pending_session::{
    PendingPublicCommitSession,
};

pub(crate) use execute::{
    execute_bound_statement_template_instance_in_borrowed_write_transaction,
    execute_bound_statement_template_instance_in_write_transaction,
    execute_parsed_statements_in_borrowed_write_transaction,
    execute_parsed_statements_in_write_transaction, execute_program_with_new_write_transaction,
    execute_with_options_in_write_transaction,
};
pub(crate) use planned_write::{
    BufferedWriteJournal, PendingFilesystemOverlay, PendingRegisteredSchemaOverlay,
    PendingSemanticOverlay, PendingSemanticRow, PendingSemanticStorage,
    PlannedInternalWriteUnit, PlannedPublicUntrackedWriteUnit, PlannedWriteDelta,
    PlannedWriteUnit,
};
pub(crate) use planned_write_runner::execute_planned_write_delta;
pub(crate) use effects::mirror_public_registered_schema_bootstrap_rows;
pub(crate) use runtime::{
    execute_compiled_execution_step_with_transaction, execute_internal_execution_with_transaction,
    CompiledExecution, CompiledExecutionBody, CompiledExecutionRoute, CompiledExecutionStep,
    CompiledExecutionStepResult, CompiledInternalExecution, SqlExecutionOutcome,
};
