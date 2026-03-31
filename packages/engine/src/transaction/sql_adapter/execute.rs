use async_trait::async_trait;
use sqlparser::ast::Statement;
use std::time::Duration;

use crate::commit::PendingPublicCommitSession;
use crate::engine::{DeferredTransactionSideEffects, Engine};
use crate::sql::executor::execution_program::{
    execute_execution_program_with_borrowed_write_transaction,
    execute_execution_program_with_write_transaction, BoundStatementTemplateInstance,
    ExecutionContext, ExecutionProgram,
};
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::transaction::PendingTransactionView;
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, Value};

use super::compile::{
    bind_single_statement_template, compile_sql_buffered_write_command, SqlBufferedWriteCommand,
};
use super::effects::{
    apply_buffered_write_planning_effects, command_metadata, complete_sql_command_execution,
    refresh_public_surface_registry_from_pending_transaction_view,
};
use super::{execute_compiled_execution_step_with_transaction, CompiledExecutionStepResult};
use crate::transaction::buffered_write_runner::execute_buffered_write_input;
use crate::transaction::commands::{
    BufferedWriteAdapter, BufferedWriteExecutionResult, BufferedWriteScope,
};
use crate::transaction::contracts::TransactionCommitOutcome;
use crate::transaction::execution::{BorrowedWriteTransaction, WriteTransaction};

pub(crate) async fn execute_parsed_statements_in_write_transaction(
    engine: &Engine,
    write_transaction: &mut WriteTransaction<'_>,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    parse_duration: Option<Duration>,
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut()?.dialect();
    let runtime_bindings = context.runtime_binding_values()?;
    let program = ExecutionProgram::compile(
        parsed_statements,
        params,
        dialect,
        &runtime_bindings,
        parse_duration,
    )?;
    ensure_execution_runtime_state_for_write_scope(
        engine,
        write_transaction.backend_transaction_mut()?,
        context,
    )
    .await?;
    execute_execution_program_with_write_transaction(
        engine,
        write_transaction,
        &program,
        allow_internal_tables,
        context,
    )
    .await
}

pub(crate) async fn execute_parsed_statements_in_borrowed_write_transaction(
    engine: &Engine,
    write_transaction: &mut BorrowedWriteTransaction<'_>,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    parse_duration: Option<Duration>,
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut().dialect();
    let runtime_bindings = context.runtime_binding_values()?;
    let program = ExecutionProgram::compile(
        parsed_statements,
        params,
        dialect,
        &runtime_bindings,
        parse_duration,
    )?;
    ensure_execution_runtime_state_for_write_scope(
        engine,
        write_transaction.backend_transaction_mut(),
        context,
    )
    .await?;
    execute_execution_program_with_borrowed_write_transaction(
        engine,
        write_transaction,
        &program,
        allow_internal_tables,
        context,
    )
    .await
}

pub(crate) async fn execute_with_options_in_write_transaction(
    engine: &Engine,
    write_transaction: &mut WriteTransaction<'_>,
    sql: &str,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    let bound_template = {
        let transaction = write_transaction.backend_transaction_mut()?;
        bind_single_statement_template(transaction, sql, params, allow_internal_tables, context)?
    };
    ensure_execution_runtime_state_for_write_scope(
        engine,
        write_transaction.backend_transaction_mut()?,
        context,
    )
    .await?;
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_bound_statement_template_instance_in_buffered_write_scope(
        engine,
        &mut scope,
        &bound_template,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
}

async fn ensure_execution_runtime_state_for_write_scope(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    if context.execution_runtime_state().is_some() {
        return Ok(());
    }
    let backend = crate::engine::TransactionBackendAdapter::new(transaction);
    let runtime_state = ExecutionRuntimeState::prepare(engine, &backend).await?;
    context.set_execution_runtime_state(runtime_state);
    Ok(())
}

pub(crate) async fn execute_bound_statement_template_instance_in_write_transaction(
    engine: &Engine,
    write_transaction: &mut WriteTransaction<'_>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_bound_statement_template_instance_in_buffered_write_scope(
        engine,
        &mut scope,
        bound_statement_template,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
}

pub(crate) async fn execute_bound_statement_template_instance_in_borrowed_write_transaction(
    engine: &Engine,
    write_transaction: &mut BorrowedWriteTransaction<'_>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    let mut scope = SqlBufferedWriteScope::Borrowed(write_transaction);
    execute_bound_statement_template_instance_in_buffered_write_scope(
        engine,
        &mut scope,
        bound_statement_template,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
}

async fn execute_bound_statement_template_instance_in_buffered_write_scope(
    engine: &Engine,
    write_transaction: &mut SqlBufferedWriteScope<'_, '_>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    execute_buffered_write_input(
        engine,
        write_transaction,
        &SqlBufferedWriteAdapter,
        bound_statement_template,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
}

struct SqlBufferedWriteAdapter;

enum SqlBufferedWriteScope<'scope, 'txn> {
    Owned(&'scope mut WriteTransaction<'txn>),
    Borrowed(&'scope mut BorrowedWriteTransaction<'txn>),
}

#[async_trait(?Send)]
impl BufferedWriteScope<SqlBufferedWriteAdapter> for SqlBufferedWriteScope<'_, '_> {
    fn backend_transaction_mut(&mut self) -> Result<&mut dyn LixBackendTransaction, LixError> {
        match self {
            Self::Owned(write_transaction) => write_transaction.backend_transaction_mut(),
            Self::Borrowed(write_transaction) => Ok(write_transaction.backend_transaction_mut()),
        }
    }

    fn buffered_write_journal_is_empty(&self) -> bool {
        match self {
            Self::Owned(write_transaction) => write_transaction.buffered_write_journal_is_empty(),
            Self::Borrowed(write_transaction) => {
                write_transaction.buffered_write_journal_is_empty()
            }
        }
    }

    fn buffered_write_pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.buffered_write_pending_transaction_view()
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.buffered_write_pending_transaction_view()
            }
        }
    }

    fn can_stage_planned_write_delta(
        &self,
        delta: &crate::transaction::PlannedWriteDelta,
    ) -> Result<bool, LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.can_stage_planned_write_delta(delta)
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.can_stage_planned_write_delta(delta)
            }
        }
    }

    fn stage_planned_write_delta(
        &mut self,
        delta: crate::transaction::PlannedWriteDelta,
    ) -> Result<(), LixError> {
        match self {
            Self::Owned(write_transaction) => write_transaction.stage_planned_write_delta(delta),
            Self::Borrowed(write_transaction) => write_transaction.stage_planned_write_delta(delta),
        }
    }

    fn clear_pending_public_commit_session(&mut self) {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.clear_pending_public_commit_session()
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.clear_pending_public_commit_session()
            }
        }
    }

    fn take_pending_public_commit_session(&mut self) -> Option<PendingPublicCommitSession> {
        match self {
            Self::Owned(write_transaction) => {
                std::mem::take(write_transaction.pending_public_commit_session_mut())
            }
            Self::Borrowed(write_transaction) => {
                std::mem::take(write_transaction.pending_public_commit_session_mut())
            }
        }
    }

    fn restore_pending_public_commit_session(
        &mut self,
        session: Option<PendingPublicCommitSession>,
    ) {
        match self {
            Self::Owned(write_transaction) => {
                *write_transaction.pending_public_commit_session_mut() = session;
            }
            Self::Borrowed(write_transaction) => {
                *write_transaction.pending_public_commit_session_mut() = session;
            }
        }
    }

    fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome {
        match self {
            Self::Owned(write_transaction) => write_transaction.buffered_write_commit_outcome_mut(),
            Self::Borrowed(write_transaction) => {
                write_transaction.buffered_write_commit_outcome_mut()
            }
        }
    }

    async fn flush_buffered_write_journal(
        &mut self,
        engine: &Engine,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await
            }
            Self::Borrowed(write_transaction) => {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await
            }
        }
    }
}

#[async_trait(?Send)]
impl BufferedWriteAdapter for SqlBufferedWriteAdapter {
    type Input = BoundStatementTemplateInstance;
    type Command = SqlBufferedWriteCommand;
    type Context = ExecutionContext;
    type PendingTransactionView = PendingTransactionView;
    type PendingPublicCommitSession = PendingPublicCommitSession;

    async fn compile_command(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&PendingTransactionView>,
        input: &BoundStatementTemplateInstance,
        allow_internal_tables: bool,
        context: &ExecutionContext,
        skip_side_effect_collection: bool,
    ) -> Result<SqlBufferedWriteCommand, LixError> {
        compile_sql_buffered_write_command(
            engine,
            transaction,
            pending_transaction_view,
            input,
            allow_internal_tables,
            context,
            skip_side_effect_collection,
        )
        .await
    }

    fn command_metadata(
        &self,
        command: &SqlBufferedWriteCommand,
    ) -> Result<crate::transaction::commands::BufferedWriteCommandMetadata, LixError> {
        command_metadata(command)
    }

    fn apply_planning_effects(
        &self,
        command: &SqlBufferedWriteCommand,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        apply_buffered_write_planning_effects(command, context)
    }

    async fn refresh_public_surface_registry_from_pending_transaction_view(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&PendingTransactionView>,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        refresh_public_surface_registry_from_pending_transaction_view(
            transaction,
            pending_transaction_view,
            context,
        )
        .await
    }

    async fn execute_command(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&PendingTransactionView>,
        pending_public_commit_session: &mut Option<PendingPublicCommitSession>,
        command: &SqlBufferedWriteCommand,
        context: &mut ExecutionContext,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
    ) -> Result<BufferedWriteExecutionResult, LixError> {
        let step_result = execute_compiled_execution_step_with_transaction(
            engine,
            transaction,
            &command.compiled,
            std::slice::from_ref(&command.statement),
            pending_transaction_view,
            Some(pending_public_commit_session),
            context.options.writer_key.as_deref(),
        )
        .await?;

        match step_result {
            CompiledExecutionStepResult::Immediate(public_result) => {
                Ok(BufferedWriteExecutionResult {
                    public_result,
                    clear_pending_public_commit_session: false,
                    commit_outcome: TransactionCommitOutcome::default(),
                })
            }
            CompiledExecutionStepResult::Outcome(execution) => {
                complete_sql_command_execution(
                    engine,
                    transaction,
                    command,
                    execution,
                    context,
                    deferred_side_effects,
                    skip_side_effect_collection,
                )
                .await
            }
        }
    }
}
