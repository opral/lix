use async_trait::async_trait;

use crate::engine::DeferredTransactionSideEffects;
use crate::sql::executor::execution_program::{BoundStatementTemplateInstance, ExecutionContext};
pub(crate) use crate::transaction::{BorrowedWriteTransaction, WriteTransaction};
use crate::{LixError, QueryResult};

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
