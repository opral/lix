use crate::engine::{DeferredTransactionSideEffects, Engine, TransactionBackendAdapter};
use crate::sql::execution::execution_program::{
    execute_compiled_execution_step_with_transaction,
    execute_execution_program_with_borrowed_write_transaction,
    execute_execution_program_with_write_transaction, BoundStatementTemplateInstance,
    CompiledExecution, CompiledExecutionRoute, CompiledExecutionStepResult, ExecutionContext,
    ExecutionProgram, StatementTemplate, StatementTemplateCacheKey,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry, PendingTransactionView,
};
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, public_surface_registry_mutations,
    PublicWriteExecutionPartition,
};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult, Value};
use sqlparser::ast::Statement;

use super::execution::{BorrowedWriteTransaction, WriteTransaction};

enum BufferedWriteScope<'scope, 'txn> {
    Owned(&'scope mut WriteTransaction<'txn>),
    Borrowed(&'scope mut BorrowedWriteTransaction<'txn>),
}

impl BufferedWriteScope<'_, '_> {
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
        delta: &super::write_plan::PlannedWriteDelta,
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
        delta: super::write_plan::PlannedWriteDelta,
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

    fn pending_public_commit_session_mut(
        &mut self,
    ) -> &mut Option<crate::sql::execution::shared_path::PendingPublicCommitSession> {
        match self {
            Self::Owned(write_transaction) => write_transaction.pending_public_commit_session_mut(),
            Self::Borrowed(write_transaction) => {
                write_transaction.pending_public_commit_session_mut()
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

pub(crate) async fn execute_parsed_statements_in_write_transaction(
    engine: &Engine,
    write_transaction: &mut WriteTransaction<'_>,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut()?.dialect();
    let program = ExecutionProgram::compile(parsed_statements, params, dialect)?;
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
) -> Result<ExecuteResult, LixError> {
    let dialect = write_transaction.backend_transaction_mut().dialect();
    let program = ExecutionProgram::compile(parsed_statements, params, dialect)?;
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
    let mut scope = BufferedWriteScope::Owned(write_transaction);
    execute_with_options_in_buffered_write_scope(
        engine,
        &mut scope,
        sql,
        params,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
}

async fn execute_with_options_in_buffered_write_scope(
    engine: &Engine,
    write_transaction: &mut BufferedWriteScope<'_, '_>,
    sql: &str,
    params: &[Value],
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
    if parsed_statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "execute_with_options_in_write_transaction expects exactly one SQL statement"
                    .to_string(),
        });
    }
    let dialect = write_transaction.backend_transaction_mut()?.dialect();
    let cache_key = StatementTemplateCacheKey::new(
        sql,
        dialect,
        allow_internal_tables,
        context.public_surface_registry_generation,
    );
    let template = match context.statement_template_cache.get(&cache_key) {
        Some(template) => template.clone(),
        None => {
            let template = StatementTemplate::compile_with_registry(
                parsed_statements[0].clone(),
                &context.public_surface_registry,
                dialect,
                params.len(),
            )?;
            context
                .statement_template_cache
                .insert(cache_key, template.clone());
            template
        }
    };
    let bound_template = template.bind(params)?;
    execute_bound_statement_template_instance_in_buffered_write_scope(
        engine,
        write_transaction,
        &bound_template,
        allow_internal_tables,
        context,
        deferred_side_effects,
        skip_side_effect_collection,
    )
    .await
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
    let mut scope = BufferedWriteScope::Owned(write_transaction);
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
    let mut scope = BufferedWriteScope::Borrowed(write_transaction);
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
    write_transaction: &mut BufferedWriteScope<'_, '_>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &mut ExecutionContext,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError> {
    let writer_key = context.options.writer_key.clone();
    let _defer_side_effects = deferred_side_effects.is_some();
    let parsed_statements = std::slice::from_ref(bound_statement_template.statement());
    loop {
        let pending_transaction_view =
            write_transaction.buffered_write_pending_transaction_view()?;
        let program = {
            let backend =
                TransactionBackendAdapter::new(write_transaction.backend_transaction_mut()?);
            shared_path::compile_execution_step_from_template_instance_with_backend(
                engine,
                &backend,
                pending_transaction_view.as_ref(),
                bound_statement_template,
                context.active_version_id.as_str(),
                writer_key.as_deref(),
                allow_internal_tables,
                Some(&context.public_surface_registry),
                shared_path::PreparationPolicy {
                    skip_side_effect_collection,
                },
            )
            .await
        };
        let program = match program {
            Ok(program) => program,
            Err(error) if !write_transaction.buffered_write_journal_is_empty() => {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await?;
                let _ = error;
                continue;
            }
            Err(error) => {
                let backend =
                    TransactionBackendAdapter::new(write_transaction.backend_transaction_mut()?);
                return Err(crate::engine::normalize_sql_execution_error_with_backend(
                    &backend,
                    error,
                    &parsed_statements,
                )
                .await);
            }
        };
        let has_materialization_plan = program.has_materialization_plan();
        let write_is_bufferable = program.is_bufferable_write(bound_statement_template.statement());
        if write_is_bufferable {
            let statement_delta = program
                .planned_write_delta()
                .cloned()
                .expect("bufferable write must have a transaction delta");
            let continuation_safe =
                write_transaction.can_stage_planned_write_delta(&statement_delta)?;
            if !write_transaction.buffered_write_journal_is_empty() && !continuation_safe {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await?;
                continue;
            }

            write_transaction.stage_planned_write_delta(statement_delta)?;
            let registry_mutated =
                prepared_execution_mutates_public_surface_registry(program.execution())?;
            if continuation_safe {
                apply_buffered_write_planning_effects(
                    program.execution(),
                    &mut context.public_surface_registry,
                    &mut context.public_surface_registry_generation,
                    &mut context.public_surface_registry_dirty,
                    &mut context.active_version_id,
                )?;
            }
            if registry_mutated {
                let pending_transaction_view =
                    write_transaction.buffered_write_pending_transaction_view()?;
                refresh_public_surface_registry_from_pending_transaction_view(
                    write_transaction.backend_transaction_mut()?,
                    &mut context.public_surface_registry,
                    &mut context.public_surface_registry_generation,
                    &mut context.public_surface_registry_dirty,
                    pending_transaction_view.as_ref(),
                )
                .await?;
            }
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        if matches!(program.route(), CompiledExecutionRoute::Internal(_))
            && !write_transaction.buffered_write_journal_is_empty()
            && !has_materialization_plan
        {
            write_transaction
                .flush_buffered_write_journal(engine, context)
                .await?;
            continue;
        }

        if let CompiledExecutionRoute::PublicRead(public_read) = program.route() {
            if !write_transaction.buffered_write_journal_is_empty()
                && matches!(
                    shared_path::prepared_public_read_transaction_mode(public_read),
                    shared_path::PreparedPublicReadTransactionMode::MaterializedState
                )
            {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await?;
                continue;
            }
        }

        let mut pending_public_commit_session =
            std::mem::take(write_transaction.pending_public_commit_session_mut());
        let step_result = {
            let transaction = write_transaction.backend_transaction_mut()?;
            execute_compiled_execution_step_with_transaction(
                engine,
                transaction,
                &program,
                &parsed_statements,
                pending_transaction_view.as_ref(),
                Some(&mut pending_public_commit_session),
                writer_key.as_deref(),
            )
            .await?
        };
        *write_transaction.pending_public_commit_session_mut() = pending_public_commit_session;
        let execution = match step_result {
            CompiledExecutionStepResult::Immediate(public_result) => return Ok(public_result),
            CompiledExecutionStepResult::Outcome(execution) => execution,
        };

        if execution.plan_effects_override.is_none()
            && !matches!(
                bound_statement_template.statement(),
                sqlparser::ast::Statement::Query(_) | sqlparser::ast::Statement::Explain { .. }
            )
        {
            write_transaction.clear_pending_public_commit_session();
        }

        if let Some(public_write) = program.execution().public_write() {
            let mut mutations = public_surface_registry_mutations(public_write)?;
            if apply_public_surface_registry_mutations(
                &mut context.public_surface_registry,
                &mut mutations,
            )? {
                context.bump_public_surface_registry_generation();
                context.public_surface_registry_dirty = true;
            }
        } else if prepared_execution_mutates_public_surface_registry(program.execution())? {
            let backend =
                TransactionBackendAdapter::new(write_transaction.backend_transaction_mut()?);
            context.public_surface_registry =
                SurfaceRegistry::bootstrap_with_backend(&backend).await?;
            context.bump_public_surface_registry_generation();
            context.public_surface_registry_dirty = true;
        }

        let active_effects = execution
            .plan_effects_override
            .as_ref()
            .unwrap_or(&program.execution().effects);

        if let Some(version_id) = &active_effects.next_active_version_id {
            context.active_version_id = version_id.clone();
        }

        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
        engine.maybe_invalidate_deterministic_settings_cache(
            program
                .execution()
                .internal_execution()
                .as_ref()
                .map(|internal| internal.mutations.as_slice())
                .unwrap_or(&[]),
            &state_commit_stream_changes,
        );

        let write_handled_by_planned_write = program.planned_write_delta().is_some();

        if write_handled_by_planned_write {
        } else if skip_side_effect_collection && deferred_side_effects.is_none() {
        } else if let Some(deferred) = deferred_side_effects {
            crate::sql::execution::runtime_effects::merge_filesystem_transaction_state(
                &mut deferred.filesystem_state,
                &program.execution().intent.filesystem_state,
            );
        } else {
            let filesystem_payload_changes_already_committed =
                shared_path::public_write_filesystem_payload_changes_already_committed(
                    program.execution(),
                );
            let binary_blob_writes =
                crate::sql::execution::runtime_effects::binary_blob_writes_from_filesystem_state(
                    &program.execution().intent.filesystem_state,
                );
            if !filesystem_payload_changes_already_committed {
                engine
                    .persist_binary_blob_writes_in_transaction(
                        write_transaction.backend_transaction_mut()?,
                        &binary_blob_writes,
                    )
                    .await
                    .map_err(|error| LixError {
                        code: error.code,
                        description: format!(
                            "transaction pending filesystem payload persistence failed: {}",
                            error.description
                        ),
                    })?;
            }
            let filesystem_finalization = if filesystem_payload_changes_already_committed {
                None
            } else {
                Some(
                    engine
                        .compile_filesystem_finalization_from_state_in_transaction(
                            write_transaction.backend_transaction_mut()?,
                            &program.execution().intent.filesystem_state,
                            writer_key.as_deref(),
                            program
                                .execution()
                                .internal_execution()
                                .as_ref()
                                .map(|internal| internal.mutations.as_slice())
                                .unwrap_or(&[]),
                        )
                        .await
                        .map_err(|error| LixError {
                            code: error.code,
                            description: format!(
                                "transaction filesystem finalization compilation failed: {}",
                                error.description
                            ),
                        })?,
                )
            };
            if let Some(filesystem_finalization) = filesystem_finalization.as_ref() {
                engine
                    .persist_filesystem_payload_domain_changes_in_transaction(
                        write_transaction.backend_transaction_mut()?,
                        &filesystem_finalization.payload_domain_changes(),
                    )
                    .await
                    .map_err(|error| LixError {
                        code: error.code,
                        description: format!(
                            "transaction tracked filesystem side-effect persistence failed: {}",
                            error.description
                        ),
                    })?;
            }
            if filesystem_finalization
                .as_ref()
                .is_some_and(|compiled| compiled.should_run_gc)
            {
                engine
                    .garbage_collect_unreachable_binary_cas_in_transaction(
                        write_transaction.backend_transaction_mut()?,
                    )
                    .await
                    .map_err(|error| LixError {
                        code: error.code,
                        description: format!(
                            "transaction binary CAS garbage collection failed: {}",
                            error.description
                        ),
                    })?;
            }
        }
        if !write_handled_by_planned_write {
            engine
                .persist_runtime_sequence_in_transaction(
                    write_transaction.backend_transaction_mut()?,
                    program.execution().settings,
                    program.execution().sequence_start,
                    &program.execution().functions,
                )
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction runtime-sequence persistence failed: {}",
                        error.description
                    ),
                })?;
        }

        context
            .pending_state_commit_stream_changes
            .extend(state_commit_stream_changes);
        return Ok(execution.public_result);
    }
}

pub(crate) async fn refresh_public_surface_registry_from_pending_transaction_view(
    transaction: &mut dyn LixBackendTransaction,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_generation: &mut u64,
    public_surface_registry_dirty: &mut bool,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<(), LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    *public_surface_registry =
        shared_path::bootstrap_public_surface_registry_with_pending_transaction_view(
            &backend,
            pending_transaction_view,
        )
        .await?;
    *public_surface_registry_generation += 1;
    *public_surface_registry_dirty = true;
    Ok(())
}

pub(crate) fn apply_buffered_write_planning_effects(
    execution: &CompiledExecution,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_generation: &mut u64,
    public_surface_registry_dirty: &mut bool,
    active_version_id: &mut String,
) -> Result<(), LixError> {
    if let Some(public_write) = execution.public_write() {
        let mut mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(public_surface_registry, &mut mutations)? {
            *public_surface_registry_generation += 1;
            *public_surface_registry_dirty = true;
        }
        if let Some(next_active_version_id) =
            public_write_execution_next_active_version_id(public_write)
        {
            *active_version_id = next_active_version_id;
        }
    } else if let Some(version_id) = &execution.effects.next_active_version_id {
        *active_version_id = version_id.clone();
    }
    Ok(())
}

fn public_write_execution_next_active_version_id(
    public_write: &crate::sql::public::runtime::PreparedPublicWrite,
) -> Option<String> {
    public_write.materialization().and_then(|execution| {
        execution
            .partitions
            .iter()
            .rev()
            .find_map(|partition| match partition {
                PublicWriteExecutionPartition::Tracked(tracked) => {
                    tracked.semantic_effects.next_active_version_id.clone()
                }
                PublicWriteExecutionPartition::Untracked(untracked) => {
                    untracked.semantic_effects.next_active_version_id.clone()
                }
            })
    })
}
