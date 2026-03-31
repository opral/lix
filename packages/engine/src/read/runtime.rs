use crate::contracts::artifacts::ResultContract;
use crate::engine::{
    normalize_sql_execution_error_with_backend, Engine, TransactionBackendAdapter,
};
use crate::read::contracts::committed_read_mode_from_prepared_public_read;
use crate::session::contracts::SessionExecutionMode;
use crate::sql::executor::execute_prepared::execute_prepared_with_transaction;
use crate::sql::executor::execution_program::{
    BoundStatementTemplateInstance, ExecutionContext, ExecutionProgram,
};
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::sql::executor::{
    compile_execution_from_template_instance_with_backend, execute_prepared_public_read,
    CompiledExecution, PreparationPolicy,
};
use crate::{
    ExecuteResult, LixBackend, LixBackendTransaction, LixError, QueryResult, TransactionMode,
};
use sqlparser::ast::Statement;
use std::time::Instant;

pub(crate) struct PreparedCommittedReadProgram {
    pub(crate) transaction_mode: TransactionMode,
    steps: Vec<PreparedCommittedReadStep>,
}

struct PreparedCommittedReadStep {
    bound_statement_template: BoundStatementTemplateInstance,
    compiled: Option<CompiledExecution>,
    source_statement: Statement,
}

pub(crate) async fn prepare_committed_read_program(
    engine: &Engine,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    execution_mode: SessionExecutionMode,
) -> Result<PreparedCommittedReadProgram, LixError> {
    let runtime_state = context.execution_runtime_state().expect(
        "committed execution should install an execution runtime state before step preparation",
    );
    let precompile_steps = !matches!(
        execution_mode,
        SessionExecutionMode::CommittedRuntimeMutation
    ) || !runtime_state.settings().enabled;
    let mut mode =
        baseline_transaction_mode_for_committed_read_program(execution_mode, runtime_state);
    let mut steps = Vec::new();

    for step in program.steps() {
        let compiled = if precompile_steps {
            let compiled = compile_bound_statement_template_instance_for_committed_read(
                engine,
                engine.backend.as_ref(),
                step,
                allow_internal_tables,
                context,
                runtime_state,
            )
            .await?;
            mode = merge_committed_read_transaction_mode(
                mode,
                transaction_mode_for_committed_read_execution(&compiled)?,
            );
            Some(compiled)
        } else {
            None
        };

        steps.push(PreparedCommittedReadStep {
            bound_statement_template: step.clone(),
            compiled,
            source_statement: step.statement().clone(),
        });
    }

    Ok(PreparedCommittedReadProgram {
        transaction_mode: mode,
        steps,
    })
}

async fn compile_bound_statement_template_instance_for_committed_read(
    engine: &Engine,
    backend: &dyn LixBackend,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    runtime_state: &ExecutionRuntimeState,
) -> Result<CompiledExecution, LixError> {
    let parsed_statements = std::slice::from_ref(bound_statement_template.statement());
    match compile_execution_from_template_instance_with_backend(
        engine,
        backend,
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
        Ok(compiled) => Ok(compiled),
        Err(error) => {
            Err(normalize_sql_execution_error_with_backend(backend, error, parsed_statements).await)
        }
    }
}

fn baseline_transaction_mode_for_committed_read_program(
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

fn merge_committed_read_transaction_mode(
    current: TransactionMode,
    next: TransactionMode,
) -> TransactionMode {
    match (current, next) {
        (TransactionMode::Write, _) | (_, TransactionMode::Write) => TransactionMode::Write,
        (TransactionMode::Deferred, _) | (_, TransactionMode::Deferred) => {
            TransactionMode::Deferred
        }
        _ => TransactionMode::Read,
    }
}

fn transaction_mode_for_committed_read_execution(
    compiled: &CompiledExecution,
) -> Result<TransactionMode, LixError> {
    if compiled.plain_explain().is_some() {
        return Ok(TransactionMode::Read);
    }
    if let Some(public_read) = compiled.public_read() {
        return Ok(committed_read_mode_from_prepared_public_read(public_read).transaction_mode());
    }
    if compiled.internal_execution().is_some() {
        return if compiled.read_only_query {
            Ok(TransactionMode::Read)
        } else {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "committed read routing compiled a non-read internal step unexpectedly",
            ))
        };
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read routing compiled a public write unexpectedly",
    ))
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
    prepared: &PreparedCommittedReadProgram,
    allow_internal_tables: bool,
    context: &ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in &prepared.steps {
        let compiled_on_demand = if step.compiled.is_some() {
            None
        } else {
            let runtime_state = context.execution_runtime_state().expect(
                "committed execution should install an execution runtime state before step compilation",
            );
            if runtime_state.settings().enabled && transaction.mode() == TransactionMode::Write {
                runtime_state
                    .ensure_sequence_initialized_in_transaction(engine, transaction)
                    .await?;
            }

            let backend = TransactionBackendAdapter::new(transaction);
            Some(
                compile_bound_statement_template_instance_for_committed_read(
                    engine,
                    &backend,
                    &step.bound_statement_template,
                    allow_internal_tables,
                    context,
                    runtime_state,
                )
                .await?,
            )
        };
        let compiled = step
            .compiled
            .as_ref()
            .or(compiled_on_demand.as_ref())
            .expect(
            "compiled committed read step should be available after eager or on-demand preparation",
        );

        let result = execute_compiled_committed_read_in_transaction(
            transaction,
            compiled,
            &step.source_statement,
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

async fn execute_compiled_committed_read_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    compiled: &CompiledExecution,
    source_statement: &Statement,
) -> Result<QueryResult, LixError> {
    let parsed_statements = std::slice::from_ref(source_statement);
    if let Some(explain) = compiled.plain_explain() {
        return explain.render_query_result();
    }
    if let Some(public_read) = compiled.public_read() {
        let backend = TransactionBackendAdapter::new(transaction);
        let execution_started = Instant::now();
        return match execute_prepared_public_read(&backend, public_read).await {
            Ok(result) => {
                if let Some(explain) = compiled.analyzed_explain() {
                    explain.render_analyzed_query_result(&result, execution_started.elapsed())
                } else {
                    Ok(result)
                }
            }
            Err(error) => {
                Err(
                    normalize_sql_execution_error_with_backend(&backend, error, parsed_statements)
                        .await,
                )
            }
        };
    }
    if let Some(internal) = compiled.internal_execution() {
        let execution_started = Instant::now();
        let internal_result =
            execute_prepared_with_transaction(transaction, &internal.prepared_statements)
                .await
                .map_err(LixError::from);
        let internal_result = match internal_result {
            Ok(result) => result,
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
        let public_result = public_result_from_contract(compiled.result_contract, &internal_result);
        if let Some(explain) = compiled.analyzed_explain() {
            return explain
                .render_analyzed_query_result(&public_result, execution_started.elapsed());
        }
        return Ok(public_result);
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read execution compiled a write-routed step unexpectedly",
    ))
}
