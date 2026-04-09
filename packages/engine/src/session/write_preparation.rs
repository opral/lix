use std::time::Duration;

use sqlparser::ast::Statement;

use crate::backend::TransactionBackendAdapter;
use crate::contracts::artifacts::{
    PendingPublicCommitSession, PreparedPublicWriteExecutionPartition, PreparedWriteStep,
    SessionStateDelta,
};
use crate::contracts::traits::PendingView;
use crate::execution::write::buffered_write_transaction::{
    BorrowedBufferedWriteTransaction, BufferedWriteTransaction,
};
use crate::execution::write::{
    command_metadata, complete_sql_command_execution,
    execute_prepared_write_execution_step_with_transaction, BufferedWriteCommandMetadata,
    BufferedWriteExecutionRoute, BufferedWriteSessionEffects, DeferredTransactionSideEffects,
    PendingTransactionView, PlannedWriteDelta, PreparedWriteExecutionStep,
    PreparedWriteExecutionStepResult,
};
use crate::session::collaborators::WriteExecutionCollaborators;
use crate::session::execution_context::ExecutionContext;
use crate::session::write_pipeline::{
    bootstrap_prepared_write_preparation_context, ensure_execution_runtime_state_for_write_scope,
    prepare_buffered_write_execution_step, PreparedWriteContextStamp,
    PreparedWriteExecutionBoundary,
};
#[cfg(test)]
use crate::sql::parser::parse_sql_with_timing;
#[cfg(test)]
use crate::sql::prepare::execution_program::{StatementTemplate, StatementTemplateCacheKey};
use crate::sql::prepare::{BoundStatementTemplateInstance, ExecutionProgram};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedWriteContextInvalidation {
    None,
    RegenerateFromPendingView,
    RegenerateFromCommittedState,
}

impl PreparedWriteContextInvalidation {
    fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

pub(crate) async fn execute_parsed_statements_in_write_transaction(
    collaborators: &dyn WriteExecutionCollaborators,
    write_transaction: &mut BufferedWriteTransaction<'_>,
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
        collaborators,
        write_transaction.backend_transaction_mut()?,
        context,
    )
    .await?;
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_execution_program_with_buffered_write_scope(
        collaborators,
        &mut scope,
        &program,
        allow_internal_tables,
        context,
    )
    .await
}

pub(crate) async fn execute_parsed_statements_in_borrowed_write_transaction(
    collaborators: &dyn WriteExecutionCollaborators,
    write_transaction: &mut BorrowedBufferedWriteTransaction<'_>,
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
        collaborators,
        write_transaction.backend_transaction_mut(),
        context,
    )
    .await?;
    let mut scope = SqlBufferedWriteScope::Borrowed(write_transaction);
    execute_execution_program_with_buffered_write_scope(
        collaborators,
        &mut scope,
        &program,
        allow_internal_tables,
        context,
    )
    .await
}

pub(crate) async fn execute_execution_program_with_write_transaction(
    collaborators: &dyn WriteExecutionCollaborators,
    write_transaction: &mut BufferedWriteTransaction<'_>,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_execution_program_with_buffered_write_scope(
        collaborators,
        &mut scope,
        program,
        allow_internal_tables,
        context,
    )
    .await
}

async fn execute_execution_program_with_buffered_write_scope(
    collaborators: &dyn WriteExecutionCollaborators,
    write_transaction: &mut SqlBufferedWriteScope<'_, '_>,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in program.steps() {
        let result = execute_bound_statement_template_instance_in_buffered_write_scope(
            collaborators,
            write_transaction,
            step,
            allow_internal_tables,
            context,
            None,
            false,
        )
        .await?;
        results.push(result);
    }

    if crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(
        program.source_statements(),
    ) {
        write_transaction.mark_installed_plugins_cache_invalidation_pending();
    }

    Ok(ExecuteResult {
        statements: results,
    })
}

async fn execute_bound_statement_template_instance_in_buffered_write_scope(
    collaborators: &dyn WriteExecutionCollaborators,
    write_transaction: &mut SqlBufferedWriteScope<'_, '_>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    loop {
        let pending_transaction_view =
            write_transaction.buffered_write_pending_transaction_view()?;
        let prepared_context = {
            let transaction = write_transaction.backend_transaction_mut()?;
            bootstrap_prepared_write_preparation_context(
                transaction,
                pending_transaction_view.as_ref(),
                context,
            )
            .await?
        };
        let command = {
            let transaction = write_transaction.backend_transaction_mut()?;
            prepare_buffered_write_execution_step(
                collaborators,
                transaction,
                pending_transaction_view.as_ref(),
                bound_statement_template,
                &prepared_context,
                allow_internal_tables,
                context,
                skip_side_effect_collection,
            )
            .await
        };
        let command: PreparedWriteExecutionBoundary = match command {
            Ok(command) => command,
            Err(error) if !write_transaction.buffered_write_journal_is_empty() => {
                write_transaction
                    .flush_buffered_write_journal(collaborators, context)
                    .await?;
                let _ = error;
                continue;
            }
            Err(error) => return Err(error),
        };
        debug_assert_eq!(
            command.prepared_context_stamp(),
            PreparedWriteContextStamp::capture(context),
            "prepared write boundary should carry the current prepared-context stamp",
        );
        let command = command.into_execution_step()?;

        let metadata = command_metadata(&command)?;
        if let Some(statement_delta) = metadata.planned_write_delta.clone() {
            let continuation_safe =
                write_transaction.can_stage_planned_write_delta(&statement_delta)?;
            if !write_transaction.buffered_write_journal_is_empty() && !continuation_safe {
                write_transaction
                    .flush_buffered_write_journal(collaborators, context)
                    .await?;
                continue;
            }

            write_transaction.stage_planned_write_delta(statement_delta)?;
            if continuation_safe {
                apply_buffered_write_planning_effects(&command, context)?;
            }
            let invalidation = prepared_write_context_invalidation_for_metadata(&metadata);
            if !invalidation.is_none() {
                write_transaction.mark_public_surface_registry_refresh_pending();
                let pending_transaction_view =
                    write_transaction.buffered_write_pending_transaction_view()?;
                let transaction = write_transaction.backend_transaction_mut()?;
                apply_prepared_write_context_invalidation(
                    transaction,
                    pending_transaction_view.as_ref(),
                    context,
                    invalidation,
                )
                .await?;
            }
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        if should_flush_before_command(&metadata, write_transaction) {
            write_transaction
                .flush_buffered_write_journal(collaborators, context)
                .await?;
            continue;
        }

        let mut pending_public_commit_session =
            write_transaction.take_pending_public_commit_session();
        let step_result = {
            let transaction = write_transaction.backend_transaction_mut()?;
            execute_prepared_write_execution_step_with_transaction(
                collaborators,
                transaction,
                &command,
                pending_transaction_view.as_ref(),
                Some(&mut pending_public_commit_session),
            )
            .await?
        };
        write_transaction.restore_pending_public_commit_session(pending_public_commit_session);

        match step_result {
            PreparedWriteExecutionStepResult::Immediate(public_result) => return Ok(public_result),
            PreparedWriteExecutionStepResult::Outcome(execution) => {
                let execution_input = context.buffered_write_execution_input();
                let execution = {
                    let transaction = write_transaction.backend_transaction_mut()?;
                    complete_sql_command_execution(
                        collaborators,
                        transaction,
                        &command,
                        execution,
                        &execution_input,
                        deferred_side_effects,
                        skip_side_effect_collection,
                    )
                    .await?
                };
                {
                    let invalidation = apply_completed_sql_command_session_effects(
                        context,
                        &execution.session_effects,
                    );
                    if !invalidation.is_none() {
                        let transaction = write_transaction.backend_transaction_mut()?;
                        apply_prepared_write_context_invalidation(
                            transaction,
                            None,
                            context,
                            invalidation,
                        )
                        .await?;
                    }
                }

                if execution.clear_pending_public_commit_session {
                    write_transaction.clear_pending_public_commit_session();
                }
                write_transaction
                    .buffered_write_commit_outcome_mut()
                    .merge(execution.commit_outcome);
                return Ok(execution.public_result);
            }
        }
    }
}

fn apply_buffered_write_planning_effects(
    step: &PreparedWriteExecutionStep,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    context.apply_session_state_delta(&planning_session_delta(step.prepared()));
    Ok(())
}

fn prepared_write_context_invalidation_for_metadata(
    metadata: &BufferedWriteCommandMetadata,
) -> PreparedWriteContextInvalidation {
    if metadata.registry_mutated_during_planning {
        PreparedWriteContextInvalidation::RegenerateFromPendingView
    } else {
        PreparedWriteContextInvalidation::None
    }
}

fn prepared_write_context_invalidation_for_session_effects(
    effects: &BufferedWriteSessionEffects,
) -> PreparedWriteContextInvalidation {
    if effects.public_surface_registry_effect.is_none() {
        PreparedWriteContextInvalidation::None
    } else {
        PreparedWriteContextInvalidation::RegenerateFromCommittedState
    }
}

async fn apply_prepared_write_context_invalidation(
    transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&PendingTransactionView>,
    context: &mut ExecutionContext,
    invalidation: PreparedWriteContextInvalidation,
) -> Result<Option<PreparedWriteContextStamp>, LixError> {
    let registry = match invalidation {
        PreparedWriteContextInvalidation::None => return Ok(None),
        PreparedWriteContextInvalidation::RegenerateFromPendingView => {
            crate::session::pending_reads::build_surface_registry(
                &TransactionBackendAdapter::new(transaction),
                pending_transaction_view.map(|view| view as &dyn PendingView),
            )
            .await?
        }
        PreparedWriteContextInvalidation::RegenerateFromCommittedState => {
            crate::session::pending_reads::build_surface_registry(
                &TransactionBackendAdapter::new(transaction),
                None,
            )
            .await?
        }
    };
    context.install_public_surface_registry(registry);
    Ok(Some(PreparedWriteContextStamp::capture(context)))
}

fn apply_completed_sql_command_session_effects(
    context: &mut ExecutionContext,
    effects: &BufferedWriteSessionEffects,
) -> PreparedWriteContextInvalidation {
    context.apply_session_state_delta(&effects.session_delta);
    prepared_write_context_invalidation_for_session_effects(effects)
}

fn planning_session_delta(prepared: &PreparedWriteStep) -> SessionStateDelta {
    if let Some(public_write) = prepared.public_write() {
        return public_write
            .materialization()
            .map(|execution| {
                execution.partitions.iter().fold(
                    SessionStateDelta::default(),
                    |mut delta, partition| {
                        match partition {
                            PreparedPublicWriteExecutionPartition::Tracked(tracked) => {
                                delta.merge(tracked.semantic_effects.session_delta.clone());
                            }
                            PreparedPublicWriteExecutionPartition::Untracked(untracked) => {
                                delta.merge(untracked.semantic_effects.session_delta.clone());
                            }
                        }
                        delta
                    },
                )
            })
            .unwrap_or_default();
    }

    prepared
        .internal_write()
        .map(|internal| internal.effects.session_delta.clone())
        .unwrap_or_default()
}

#[cfg(test)]
fn bind_single_statement_template(
    transaction: &mut dyn LixBackendTransaction,
    sql: &str,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<BoundStatementTemplateInstance, LixError> {
    let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
    let parsed_statements = parsed.statements;
    if parsed_statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "execute_with_options_in_write_transaction expects exactly one SQL statement"
                    .to_string(),
        });
    }

    let dialect = transaction.dialect();
    let cache_key = StatementTemplateCacheKey::new(
        sql,
        dialect,
        allow_internal_tables,
        context.public_surface_registry_generation(),
    );
    let template = match context.cached_statement_template(&cache_key) {
        Some(template) => template,
        None => {
            let (template, _) = StatementTemplate::compile(
                parsed_statements[0].clone(),
                dialect,
                params.len(),
                crate::sql::parser::placeholders::PlaceholderState::new(),
            )?;
            context.cache_statement_template(cache_key, template.clone());
            template
        }
    };
    let runtime_bindings = context.runtime_binding_values()?;
    template.bind(params, &runtime_bindings, Some(parsed.parse_duration))
}

fn should_flush_before_command(
    metadata: &BufferedWriteCommandMetadata,
    write_transaction: &SqlBufferedWriteScope<'_, '_>,
) -> bool {
    match metadata.route {
        BufferedWriteExecutionRoute::Internal => {
            !write_transaction.buffered_write_journal_is_empty()
                && !metadata.has_materialization_plan
        }
        BufferedWriteExecutionRoute::PublicReadCommitted => {
            !write_transaction.buffered_write_journal_is_empty()
        }
        BufferedWriteExecutionRoute::Other => false,
    }
}

enum SqlBufferedWriteScope<'scope, 'txn> {
    Owned(&'scope mut BufferedWriteTransaction<'txn>),
    Borrowed(&'scope mut BorrowedBufferedWriteTransaction<'txn>),
}

impl SqlBufferedWriteScope<'_, '_> {
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

    fn can_stage_planned_write_delta(&self, delta: &PlannedWriteDelta) -> Result<bool, LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.can_stage_planned_write_delta(delta)
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.can_stage_planned_write_delta(delta)
            }
        }
    }

    fn stage_planned_write_delta(&mut self, delta: PlannedWriteDelta) -> Result<(), LixError> {
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

    fn buffered_write_commit_outcome_mut(
        &mut self,
    ) -> &mut crate::execution::write::TransactionCommitOutcome {
        match self {
            Self::Owned(write_transaction) => write_transaction.buffered_write_commit_outcome_mut(),
            Self::Borrowed(write_transaction) => {
                write_transaction.buffered_write_commit_outcome_mut()
            }
        }
    }

    fn mark_public_surface_registry_refresh_pending(&mut self) {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.mark_public_surface_registry_refresh_pending()
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.mark_public_surface_registry_refresh_pending()
            }
        }
    }

    async fn flush_buffered_write_journal(
        &mut self,
        bindings: &dyn crate::execution::write::WriteExecutionBindings,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        let mut execution_input = context.buffered_write_execution_input();
        match self {
            Self::Owned(write_transaction) => {
                write_transaction
                    .flush_buffered_write_journal(bindings, &mut execution_input)
                    .await
            }
            Self::Borrowed(write_transaction) => {
                write_transaction
                    .flush_buffered_write_journal(bindings, &mut execution_input)
                    .await
            }
        }?;
        context.apply_buffered_write_execution_input(&execution_input);
        Ok(())
    }

    fn mark_installed_plugins_cache_invalidation_pending(&mut self) {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.mark_installed_plugins_cache_invalidation_pending()
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.mark_installed_plugins_cache_invalidation_pending()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::wasm::NoopWasmRuntime;
    use crate::{boot, BootArgs, Engine, ExecuteOptions, QueryResult, Session, SqlDialect};
    use async_trait::async_trait;
    use std::sync::Arc;

    struct NoopBackend;

    struct NoopTransaction;

    #[async_trait(?Send)]
    impl crate::LixBackend for NoopBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this unit test backend",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackendTransaction for NoopTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn test_engine() -> Arc<Engine> {
        Arc::new(boot(BootArgs::new(
            Box::new(NoopBackend),
            Arc::new(NoopWasmRuntime),
        )))
    }

    fn test_session(engine: &Arc<Engine>) -> Session {
        Session::new_for_test(
            crate::session::collaborators::SessionCollaborators::new(engine.session_services()),
            "version-test".to_string(),
            Vec::new(),
        )
    }

    #[test]
    fn statement_template_cache_is_shared_across_repeated_calls_in_one_session() {
        let engine = test_engine();
        let session = test_session(&engine);
        let sql = "SELECT 1";
        let cache_key = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let mut transaction = NoopTransaction;

        let mut first_context = session.new_execution_context(ExecuteOptions::default());
        assert!(
            first_context
                .cached_statement_template(&cache_key)
                .is_none(),
            "cache should start empty for a fresh session runtime"
        );

        bind_single_statement_template(&mut transaction, sql, &[], false, &mut first_context)
            .expect("first template bind should succeed");
        assert!(
            first_context
                .cached_statement_template(&cache_key)
                .is_some(),
            "first bind should populate the session-owned statement template cache"
        );

        let second_context = session.new_execution_context(ExecuteOptions::default());
        assert!(
            second_context
                .cached_statement_template(&cache_key)
                .is_some(),
            "a new execution context in the same session should reuse the cached template"
        );
    }

    #[test]
    fn registry_generation_bumps_are_session_local_and_create_new_cache_namespaces() {
        let engine = test_engine();
        let session_a = test_session(&engine);
        let session_b = test_session(&engine);
        let sql = "SELECT 1";
        let cache_key_v0 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let cache_key_v1 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 1);
        let mut transaction = NoopTransaction;

        let mut initial_context = session_a.new_execution_context(ExecuteOptions::default());
        bind_single_statement_template(&mut transaction, sql, &[], false, &mut initial_context)
            .expect("initial template bind should succeed");
        assert!(
            initial_context
                .cached_statement_template(&cache_key_v0)
                .is_some(),
            "initial cache namespace should contain the first template"
        );
        assert_eq!(session_a.snapshot().public_surface_registry_generation, 0);
        assert_eq!(session_b.snapshot().public_surface_registry_generation, 0);

        let mut bumped_context = session_a.new_execution_context(ExecuteOptions::default());
        bumped_context.bump_public_surface_registry_generation();
        assert_eq!(session_a.snapshot().public_surface_registry_generation, 1);
        assert_eq!(
            session_b.snapshot().public_surface_registry_generation,
            0,
            "another session should not inherit the bumped registry generation"
        );

        let mut session_a_after_bump = session_a.new_execution_context(ExecuteOptions::default());
        assert!(
            session_a_after_bump
                .cached_statement_template(&cache_key_v1)
                .is_none(),
            "new registry generations should start with a fresh cache namespace"
        );
        bind_single_statement_template(
            &mut transaction,
            sql,
            &[],
            false,
            &mut session_a_after_bump,
        )
        .expect("template bind after generation bump should succeed");
        assert!(
            session_a_after_bump
                .cached_statement_template(&cache_key_v1)
                .is_some(),
            "binding after the bump should populate the new cache namespace"
        );

        let session_b_context = session_b.new_execution_context(ExecuteOptions::default());
        assert!(
            session_b_context
                .cached_statement_template(&cache_key_v0)
                .is_none(),
            "another session should not see session-local template cache entries"
        );
    }
}
