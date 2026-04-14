use std::time::Duration;

use sqlparser::ast::Statement;

use crate::execution::{render_analyzed_explain_result, render_plain_explain_result};
#[cfg(test)]
use crate::sql::parse_sql_with_timing;
#[cfg(test)]
use crate::sql::PlaceholderState;
use crate::sql::{BoundStatementInstance, SessionStateDelta, StatementBatch};
#[cfg(test)]
use crate::sql::{StatementTemplate, StatementTemplateCacheKey};
use crate::transaction::overlay::PendingOverlay;
use crate::transaction::pipeline::{
    command_metadata, complete_sql_command_execution, empty_public_write_execution_outcome,
    execute_direct_execution_with_transaction,
};
use crate::transaction::{
    apply_schema_registrations_in_transaction,
    normalize_sql_error_with_transaction_and_relation_names, BorrowedBufferedWriteTransaction,
    BufferedWriteCommandMetadata, BufferedWriteFlushClass, BufferedWriteSessionEffects,
    BufferedWriteTransaction, DeferredCommitEffects, PendingCommitState, PendingWriteOverlay,
    PreparedDirectWriteArtifact, PreparedPublicWriteExecutionPartition, PreparedWriteStatement,
    SessionCompilerState, TransactionWriteDelta, WriteCommand, WriteExecutionContext, WritePath,
    WriteResult,
};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, Value};

use super::{
    build_write_preparation_context, ensure_function_bindings_for_write_scope,
    prepare_buffered_write_execution_step,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedWriteContextInvalidation {
    None,
    RegenerateFromPendingOverlay,
    RegenerateFromCommittedState,
}

impl PreparedWriteContextInvalidation {
    fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

pub(crate) async fn execute_parsed_statements_in_write_transaction(
    execution_context: &dyn WriteExecutionContext,
    write_transaction: &mut BufferedWriteTransaction<'_>,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
    parse_duration: Option<Duration>,
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut()?.dialect();
    let runtime_bindings = context.runtime_binding_values()?;
    let statement_batch = StatementBatch::compile(
        parsed_statements,
        params,
        dialect,
        &runtime_bindings,
        parse_duration,
    )?;
    ensure_function_bindings_for_write_scope(
        execution_context,
        write_transaction.backend_transaction_mut()?,
        context,
    )
    .await?;
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_statement_batch_with_buffered_write_scope(
        execution_context,
        &mut scope,
        &statement_batch,
        allow_internal_relations,
        context,
    )
    .await
}

pub(crate) async fn execute_parsed_statements_in_borrowed_write_transaction(
    execution_context: &dyn WriteExecutionContext,
    write_transaction: &mut BorrowedBufferedWriteTransaction<'_>,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
    parse_duration: Option<Duration>,
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut().dialect();
    let runtime_bindings = context.runtime_binding_values()?;
    let statement_batch = StatementBatch::compile(
        parsed_statements,
        params,
        dialect,
        &runtime_bindings,
        parse_duration,
    )?;
    ensure_function_bindings_for_write_scope(
        execution_context,
        write_transaction.backend_transaction_mut(),
        context,
    )
    .await?;
    let mut scope = SqlBufferedWriteScope::Borrowed(write_transaction);
    execute_statement_batch_with_buffered_write_scope(
        execution_context,
        &mut scope,
        &statement_batch,
        allow_internal_relations,
        context,
    )
    .await
}

pub(crate) async fn execute_statement_batch_with_write_transaction(
    execution_context: &dyn WriteExecutionContext,
    write_transaction: &mut BufferedWriteTransaction<'_>,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
) -> Result<ExecuteResult, LixError> {
    let mut scope = SqlBufferedWriteScope::Owned(write_transaction);
    execute_statement_batch_with_buffered_write_scope(
        execution_context,
        &mut scope,
        statement_batch,
        allow_internal_relations,
        context,
    )
    .await
}

async fn execute_statement_batch_with_buffered_write_scope(
    execution_context: &dyn WriteExecutionContext,
    write_transaction: &mut SqlBufferedWriteScope<'_, '_>,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in statement_batch.steps() {
        let result = execute_bound_statement_in_buffered_write_scope(
            execution_context,
            write_transaction,
            step,
            allow_internal_relations,
            context,
            None,
            false,
        )
        .await?;
        results.push(result);
    }

    if crate::sql::should_invalidate_installed_plugins_cache_for_statements(
        statement_batch.source_statements(),
    ) {
        write_transaction.mark_installed_plugins_cache_invalidation_pending();
    }

    Ok(ExecuteResult {
        statements: results,
    })
}

async fn execute_bound_statement_in_buffered_write_scope(
    execution_context: &dyn WriteExecutionContext,
    write_transaction: &mut SqlBufferedWriteScope<'_, '_>,
    bound_statement: &BoundStatementInstance,
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
    deferred_commit_effects: Option<&mut DeferredCommitEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    loop {
        let pending_write_overlay = write_transaction.buffered_write_pending_write_overlay()?;
        let prepared_context = {
            let transaction = write_transaction.backend_transaction_mut()?;
            build_write_preparation_context(transaction, pending_write_overlay.as_ref(), context)
                .await?
        };
        let command = {
            let transaction = write_transaction.backend_transaction_mut()?;
            prepare_buffered_write_execution_step(
                execution_context,
                transaction,
                pending_write_overlay.as_ref(),
                bound_statement,
                &prepared_context,
                allow_internal_relations,
                context,
                skip_side_effect_collection,
            )
            .await
        };
        let command: WriteCommand = match command {
            Ok(command) => command,
            Err(error) if !write_transaction.buffered_write_journal_is_empty() => {
                write_transaction
                    .flush_journal(execution_context, context)
                    .await?;
                let _ = error;
                continue;
            }
            Err(error) => return Err(error),
        };

        let metadata = command_metadata(&command)?;
        if let Some(statement_delta) = metadata.transaction_write_delta.clone() {
            let continuation_safe =
                write_transaction.can_stage_transaction_write_delta(&statement_delta)?;
            if !write_transaction.buffered_write_journal_is_empty() && !continuation_safe {
                write_transaction
                    .flush_journal(execution_context, context)
                    .await?;
                continue;
            }

            write_transaction.stage_transaction_write_delta(statement_delta)?;
            if continuation_safe {
                apply_buffered_write_planning_effects(&command, context)?;
            }
            let invalidation = prepared_write_context_invalidation_for_metadata(&metadata);
            if !invalidation.is_none() {
                write_transaction.mark_public_surface_registry_refresh_pending();
                let pending_write_overlay =
                    write_transaction.buffered_write_pending_write_overlay()?;
                let transaction = write_transaction.backend_transaction_mut()?;
                apply_prepared_write_context_invalidation(
                    transaction,
                    pending_write_overlay.as_ref(),
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
                .flush_journal(execution_context, context)
                .await?;
            continue;
        }

        let mut pending_commit_state = write_transaction.take_pending_commit_state();
        let write_result = {
            let transaction = write_transaction.backend_transaction_mut()?;
            execute_write_command_with_transaction(
                execution_context,
                transaction,
                &command,
                pending_write_overlay.as_ref(),
                Some(&mut pending_commit_state),
            )
            .await?
        };
        write_transaction.restore_pending_commit_state(pending_commit_state);

        match write_result {
            WriteResult::Immediate(public_result) => return Ok(public_result),
            WriteResult::Outcome(write_outcome) => {
                let execution_input = context.buffered_write_execution_input();
                let buffered_write_outcome = {
                    let transaction = write_transaction.backend_transaction_mut()?;
                    complete_sql_command_execution(
                        execution_context,
                        transaction,
                        &command,
                        write_outcome,
                        &execution_input,
                        deferred_commit_effects,
                        skip_side_effect_collection,
                    )
                    .await?
                };
                {
                    let invalidation = apply_completed_sql_command_session_effects(
                        context,
                        &buffered_write_outcome.session_effects,
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

                if buffered_write_outcome.clear_pending_commit_state {
                    write_transaction.clear_pending_commit_state();
                }
                write_transaction
                    .buffered_write_commit_outcome_mut()
                    .merge(buffered_write_outcome.commit_outcome);
                return Ok(buffered_write_outcome.public_result);
            }
        }
    }
}

async fn execute_write_command_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    command: &WriteCommand,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    pending_commit_state: Option<&mut Option<PendingCommitState>>,
) -> Result<WriteResult, LixError> {
    match command.path() {
        WritePath::ExplainOnly => execute_explain_write_command(command),
        WritePath::PendingRead(public_read) | WritePath::CommittedRead(public_read) => {
            execute_read_query_write_command(
                execution_context,
                transaction,
                command,
                pending_write_overlay,
                public_read,
            )
            .await
        }
        WritePath::BufferedDelta(delta) => {
            let write_outcome = delta
                .execute(execution_context, transaction, pending_commit_state)
                .await?;
            Ok(WriteResult::Outcome(write_outcome))
        }
        WritePath::NoopWrite => Ok(WriteResult::Outcome(empty_public_write_execution_outcome())),
        WritePath::DirectWrite(direct) => {
            execute_direct_write_command(transaction, command, direct).await
        }
    }
}

fn execute_explain_write_command(command: &WriteCommand) -> Result<WriteResult, LixError> {
    let template = command
        .diagnostic_context()
        .plain_explain_template
        .as_ref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "plain explain path expected a non-analyze explain template",
            )
        })?;
    Ok(WriteResult::Immediate(render_plain_explain_result(
        template,
    )?))
}

async fn execute_read_query_write_command(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    command: &WriteCommand,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    public_read: &crate::sql::PreparedPublicRead,
) -> Result<WriteResult, LixError> {
    let execution_started = std::time::Instant::now();
    let public_result = match execution_context
        .execute_pending_overlay_public_read(
            transaction,
            pending_write_overlay.map(|view| view as &dyn PendingOverlay),
            public_read,
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let normalized = normalize_sql_error_with_transaction_and_relation_names(
                transaction,
                error,
                command.diagnostic_context().relation_names(),
            )
            .await;
            return Err(normalized);
        }
    };
    if let Some(template) = command
        .diagnostic_context()
        .analyzed_explain_template
        .as_ref()
    {
        return Ok(WriteResult::Immediate(render_analyzed_explain_result(
            template,
            &public_result,
            execution_started.elapsed(),
        )?));
    }
    Ok(WriteResult::Immediate(public_result))
}

async fn execute_direct_write_command(
    transaction: &mut dyn LixBackendTransaction,
    command: &WriteCommand,
    direct: &PreparedDirectWriteArtifact,
) -> Result<WriteResult, LixError> {
    apply_schema_registrations_in_transaction(transaction, command.schema_registrations()).await?;
    let execution_started = std::time::Instant::now();
    match execute_direct_execution_with_transaction(
        transaction,
        direct,
        command.prepared().result_contract,
        command.function_bindings().provider(),
        direct.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)
    {
        Ok(write_outcome) => {
            if let Some(template) = command
                .diagnostic_context()
                .analyzed_explain_template
                .as_ref()
            {
                return Ok(WriteResult::Immediate(render_analyzed_explain_result(
                    template,
                    &write_outcome.public_result,
                    execution_started.elapsed(),
                )?));
            }
            Ok(WriteResult::Outcome(write_outcome))
        }
        Err(error) => {
            let normalized = normalize_sql_error_with_transaction_and_relation_names(
                transaction,
                error,
                command.diagnostic_context().relation_names(),
            )
            .await;
            Err(LixError {
                code: normalized.code,
                description: format!(
                    "transaction direct execution failed: {}",
                    normalized.description
                ),
            })
        }
    }
}

fn apply_buffered_write_planning_effects(
    step: &WriteCommand,
    context: &mut SessionCompilerState,
) -> Result<(), LixError> {
    context.apply_session_state_delta(&planning_session_delta(step.prepared()));
    Ok(())
}

fn prepared_write_context_invalidation_for_metadata(
    metadata: &BufferedWriteCommandMetadata,
) -> PreparedWriteContextInvalidation {
    if metadata.registry_mutated_during_planning {
        PreparedWriteContextInvalidation::RegenerateFromPendingOverlay
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
    pending_write_overlay: Option<&PendingWriteOverlay>,
    context: &mut SessionCompilerState,
    invalidation: PreparedWriteContextInvalidation,
) -> Result<(), LixError> {
    let registry = match invalidation {
        PreparedWriteContextInvalidation::None => return Ok(()),
        PreparedWriteContextInvalidation::RegenerateFromPendingOverlay => {
            let function_bindings = context.function_bindings().expect(
                "prepared write context invalidation requires initialized function bindings",
            );
            let backend = crate::backend::transaction_backend_view(transaction);
            crate::transaction::build_public_read_surface_registry_with_pending_overlay(
                &backend,
                pending_write_overlay.map(|view| view as &dyn PendingOverlay),
                function_bindings.provider(),
            )
            .await?
        }
        PreparedWriteContextInvalidation::RegenerateFromCommittedState => {
            let function_bindings = context.function_bindings().expect(
                "prepared write context invalidation requires initialized function bindings",
            );
            let backend = crate::backend::transaction_backend_view(transaction);
            crate::transaction::build_public_read_surface_registry_with_pending_overlay(
                &backend,
                None,
                function_bindings.provider(),
            )
            .await?
        }
    };
    context.install_public_surface_registry(registry);
    Ok(())
}

fn apply_completed_sql_command_session_effects(
    context: &mut SessionCompilerState,
    effects: &BufferedWriteSessionEffects,
) -> PreparedWriteContextInvalidation {
    context.apply_session_state_delta(&effects.session_delta);
    prepared_write_context_invalidation_for_session_effects(effects)
}

fn planning_session_delta(prepared: &PreparedWriteStatement) -> SessionStateDelta {
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
        .direct_write()
        .map(|direct| direct.effects.session_delta.clone())
        .unwrap_or_default()
}

#[cfg(test)]
fn bind_single_statement_template(
    transaction: &mut dyn LixBackendTransaction,
    sql: &str,
    params: &[Value],
    allow_internal_relations: bool,
    context: &mut SessionCompilerState,
) -> Result<BoundStatementInstance, LixError> {
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
        allow_internal_relations,
        context.public_surface_registry_generation(),
    );
    let template = match context.cached_statement_template(&cache_key) {
        Some(template) => template,
        None => {
            let (template, _) = StatementTemplate::compile(
                parsed_statements[0].clone(),
                dialect,
                params.len(),
                PlaceholderState::new(),
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
    match metadata.flush_class {
        BufferedWriteFlushClass::DirectWrite => {
            !write_transaction.buffered_write_journal_is_empty()
                && !metadata.has_materialization_plan
        }
        BufferedWriteFlushClass::CommittedRead => {
            !write_transaction.buffered_write_journal_is_empty()
        }
        BufferedWriteFlushClass::NoPreFlush => false,
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

    fn buffered_write_pending_write_overlay(
        &self,
    ) -> Result<Option<PendingWriteOverlay>, LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.buffered_write_pending_write_overlay()
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.buffered_write_pending_write_overlay()
            }
        }
    }

    fn can_stage_transaction_write_delta(
        &self,
        delta: &TransactionWriteDelta,
    ) -> Result<bool, LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.can_stage_transaction_write_delta(delta)
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.can_stage_transaction_write_delta(delta)
            }
        }
    }

    fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError> {
        match self {
            Self::Owned(write_transaction) => {
                write_transaction.stage_transaction_write_delta(delta)
            }
            Self::Borrowed(write_transaction) => {
                write_transaction.stage_transaction_write_delta(delta)
            }
        }
    }

    fn clear_pending_commit_state(&mut self) {
        match self {
            Self::Owned(write_transaction) => write_transaction.clear_pending_commit_state(),
            Self::Borrowed(write_transaction) => write_transaction.clear_pending_commit_state(),
        }
    }

    fn take_pending_commit_state(&mut self) -> Option<PendingCommitState> {
        match self {
            Self::Owned(write_transaction) => {
                std::mem::take(write_transaction.pending_commit_state_mut())
            }
            Self::Borrowed(write_transaction) => {
                std::mem::take(write_transaction.pending_commit_state_mut())
            }
        }
    }

    fn restore_pending_commit_state(&mut self, session: Option<PendingCommitState>) {
        match self {
            Self::Owned(write_transaction) => {
                *write_transaction.pending_commit_state_mut() = session;
            }
            Self::Borrowed(write_transaction) => {
                *write_transaction.pending_commit_state_mut() = session;
            }
        }
    }

    fn buffered_write_commit_outcome_mut(
        &mut self,
    ) -> &mut crate::transaction::TransactionCommitOutcome {
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

    async fn flush_journal(
        &mut self,
        execution_context: &dyn WriteExecutionContext,
        context: &mut SessionCompilerState,
    ) -> Result<(), LixError> {
        let mut execution_input = context.buffered_write_execution_input();
        match self {
            Self::Owned(write_transaction) => {
                write_transaction
                    .flush_journal(execution_context, &mut execution_input)
                    .await
            }
            Self::Borrowed(write_transaction) => {
                write_transaction
                    .flush_journal(execution_context, &mut execution_input)
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
    use crate::wasm::NoopWasmRuntime;
    use crate::{ExecuteOptions, Lix, LixConfig, QueryResult, Session, SqlDialect};
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
            _mode: crate::TransactionBeginMode,
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
            self.begin_transaction(crate::TransactionBeginMode::Write)
                .await
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackendTransaction for NoopTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionBeginMode {
            crate::TransactionBeginMode::Write
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

    fn test_lix() -> Arc<Lix> {
        Arc::new(Lix::boot(LixConfig::new(
            Box::new(NoopBackend),
            Arc::new(NoopWasmRuntime),
        )))
    }

    fn test_session(lix: &Arc<Lix>) -> Session {
        Session::new_for_test(
            lix.engine().session_host(),
            "version-test".to_string(),
            Vec::new(),
        )
    }

    #[test]
    fn statement_template_cache_is_shared_across_repeated_calls_in_one_session() {
        let lix = test_lix();
        let session = test_session(&lix);
        let sql = "SELECT 1";
        let cache_key = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let mut transaction = NoopTransaction;

        let mut first_context = session.new_compiler_state(ExecuteOptions::default());
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

        let second_context = session.new_compiler_state(ExecuteOptions::default());
        assert!(
            second_context
                .cached_statement_template(&cache_key)
                .is_some(),
            "a new execution context in the same session should reuse the cached template"
        );
    }

    #[test]
    fn registry_generation_bumps_are_session_local_and_create_new_cache_namespaces() {
        let lix = test_lix();
        let session_a = test_session(&lix);
        let session_b = test_session(&lix);
        let sql = "SELECT 1";
        let cache_key_v0 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 0);
        let cache_key_v1 = StatementTemplateCacheKey::new(sql, SqlDialect::Sqlite, false, 1);
        let mut transaction = NoopTransaction;

        let mut initial_context = session_a.new_compiler_state(ExecuteOptions::default());
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

        let mut bumped_context = session_a.new_compiler_state(ExecuteOptions::default());
        bumped_context.bump_public_surface_registry_generation();
        assert_eq!(session_a.snapshot().public_surface_registry_generation, 1);
        assert_eq!(
            session_b.snapshot().public_surface_registry_generation,
            0,
            "another session should not inherit the bumped registry generation"
        );

        let mut session_a_after_bump = session_a.new_compiler_state(ExecuteOptions::default());
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

        let session_b_context = session_b.new_compiler_state(ExecuteOptions::default());
        assert!(
            session_b_context
                .cached_statement_template(&cache_key_v0)
                .is_none(),
            "another session should not see session-local template cache entries"
        );
    }
}
