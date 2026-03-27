use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, TransactionBackendAdapter,
};
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::execute_prepared::execute_prepared_with_transaction;
use crate::sql::execution::execution_program::{
    BoundStatementTemplateInstance, ExecutionContext, ExecutionProgram,
};
use crate::sql::execution::runtime_state::ExecutionRuntimeState;
use crate::sql::execution::shared_path::{self, PreparationPolicy};
use crate::sql::public::runtime::execute_prepared_public_read;
use crate::session::contracts::SessionExecutionMode;
use crate::transaction::sql_adapter::CompiledExecutionRoute;
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, TransactionMode};

pub(crate) fn transaction_mode_for_committed_read_program(
    execution_mode: SessionExecutionMode,
    runtime_state: &ExecutionRuntimeState,
) -> TransactionMode {
    match execution_mode {
        SessionExecutionMode::CommittedRead => TransactionMode::Read,
        SessionExecutionMode::CommittedRuntimeMutation => {
            if runtime_state.settings().enabled {
                TransactionMode::Write
            } else {
                TransactionMode::Read
            }
        }
        SessionExecutionMode::WriteTransaction => TransactionMode::Write,
    }
}

async fn execute_bound_statement_template_instance_in_committed_read_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &ExecutionContext,
) -> Result<QueryResult, LixError> {
    let parsed_statements = std::slice::from_ref(bound_statement_template.statement());
    let runtime_state = context.execution_runtime_state().expect(
        "committed execution should install an execution runtime state before step compilation",
    );
    if runtime_state.settings().enabled && transaction.mode() == TransactionMode::Write {
        runtime_state
            .ensure_sequence_initialized_in_transaction(engine, transaction)
            .await?;
    }
    let compiled = match shared_path::compile_execution_step_from_template_instance_with_backend(
        engine,
        &TransactionBackendAdapter::new(transaction),
        None,
        bound_statement_template,
        context.active_version_id.as_str(),
        &context.active_account_ids,
        context.options.writer_key.as_deref(),
        allow_internal_tables,
        Some(&context.public_surface_registry),
        Some(runtime_state),
        PreparationPolicy {
            skip_side_effect_collection: false,
        },
    )
    .await
    {
        Ok(compiled) => compiled,
        Err(error) => {
            let backend = TransactionBackendAdapter::new(transaction);
            return Err(normalize_sql_execution_error_with_backend(
                &backend,
                error,
                parsed_statements,
            )
            .await);
        }
    };

    match compiled.route() {
        CompiledExecutionRoute::PublicRead(public_read) => {
            let backend = TransactionBackendAdapter::new(transaction);
            match execute_prepared_public_read(&backend, public_read).await {
                Ok(result) => Ok(result),
                Err(error) => Err(normalize_sql_execution_error_with_backend(
                    &backend,
                    error,
                    parsed_statements,
                )
                .await),
            }
        }
        CompiledExecutionRoute::Internal(internal) => {
            let internal_result =
                match execute_prepared_with_transaction(transaction, &internal.prepared_statements)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let backend = TransactionBackendAdapter::new(transaction);
                        return Err(normalize_sql_execution_error_with_backend(
                            &backend,
                            LixError::from(error),
                            parsed_statements,
                        )
                        .await);
                    }
                };
            Ok(public_result_from_contract(
                compiled.execution().result_contract,
                &internal_result,
            ))
        }
        CompiledExecutionRoute::PlannedWriteDelta(_) | CompiledExecutionRoute::PublicWriteNoop => {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "committed read execution compiled a write-routed step unexpectedly",
            ))
        }
    }
}

fn public_result_from_contract(
    contract: ResultContract,
    internal_result: &QueryResult,
) -> QueryResult {
    match contract {
        ResultContract::DmlNoReturning => QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        ResultContract::Select | ResultContract::DmlReturning | ResultContract::Other => {
            internal_result.clone()
        }
    }
}

pub(crate) async fn execute_execution_program_in_committed_read_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in program.steps() {
        let result = execute_bound_statement_template_instance_in_committed_read_transaction(
            engine,
            transaction,
            step,
            allow_internal_tables,
            context,
        )
        .await?;
        results.push(result);
    }

    context
        .execution_runtime_state()
        .expect("committed execution should retain its runtime state until flush")
        .flush_in_transaction(engine, transaction)
        .await?;

    Ok(ExecuteResult {
        statements: results,
    })
}
