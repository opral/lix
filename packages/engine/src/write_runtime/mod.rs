pub(crate) mod buffered;
pub(crate) mod commit;
mod contracts;
mod deterministic_sequence;
mod effective_state;
mod execution;
mod filesystem_state;
pub(crate) mod overlay;
mod read_context;
mod resolve_write_plan;
mod selector_reads;
pub(crate) mod sql_adapter;

use async_trait::async_trait;

use crate::engine::DeferredTransactionSideEffects;
use crate::session::execution_context::ExecutionContext;
use crate::sql::prepare::execution_program::BoundStatementTemplateInstance;
use crate::{LixError, QueryResult};

pub(crate) use buffered::PlannedWriteDelta;
pub(crate) use contracts::BufferedWriteExecutionContext;
pub use contracts::{
    CommitOutcome, TransactionCommitOutcome, TransactionDelta, TransactionJournal,
};
pub(crate) use deterministic_sequence::{
    build_ensure_runtime_sequence_row_sql, build_persist_sequence_highest_batch,
    build_update_runtime_sequence_highest_sql, load_runtime_sequence_start_in_transaction,
};
pub(crate) use execution::BorrowedWriteTransaction;
pub use execution::WriteTransaction;
pub(crate) use overlay::PendingTransactionView;
pub use read_context::ReadContext;
#[cfg(test)]
pub(crate) use resolve_write_plan::resolve_write_plan;
pub(crate) use resolve_write_plan::resolve_write_plan_with_functions;
pub(crate) use selector_reads::execute_public_query_with_optional_pending_transaction_view;

#[async_trait(?Send)]
pub(crate) trait WriteProgramExecutor {
    async fn execute_bound_statement_template_instance_in_write_transaction(
        &self,
        write_transaction: &mut WriteTransaction<'_>,
        bound_statement_template: &BoundStatementTemplateInstance,
        allow_internal_tables: bool,
        context: &mut ExecutionContext,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
    ) -> Result<QueryResult, LixError>;

    async fn execute_bound_statement_template_instance_in_borrowed_write_transaction(
        &self,
        write_transaction: &mut BorrowedWriteTransaction<'_>,
        bound_statement_template: &BoundStatementTemplateInstance,
        allow_internal_tables: bool,
        context: &mut ExecutionContext,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
    ) -> Result<QueryResult, LixError>;
}
