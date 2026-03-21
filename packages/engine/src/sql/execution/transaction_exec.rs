use crate::engine::{DeferredTransactionSideEffects, Engine, TransactionBackendAdapter};
use crate::sql::execution::execution_program::{
    execute_compiled_execution_step_with_transaction, execute_execution_program_with_transaction,
    BoundStatementTemplateInstance, CompiledExecution, CompiledExecutionRoute,
    CompiledExecutionStepResult, ExecutionContext, ExecutionProgram, StatementTemplate,
    StatementTemplateCacheKey,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path;
use crate::sql::execution::shared_path::prepared_execution_mutates_public_surface_registry;
use crate::sql::execution::write_txn_plan::MutationJournal;
use crate::sql::execution::write_txn_runner::run_txn_delta_with_transaction;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, public_surface_registry_mutations,
    PublicWriteExecutionPartition,
};
use crate::{LixError, LixTransaction, QueryResult, Value};
use sqlparser::ast::Statement;

impl Engine {
    pub(crate) async fn execute_parsed_statements_in_transaction_core(
        &self,
        transaction: &mut dyn LixTransaction,
        parsed_statements: Vec<Statement>,
        params: &[Value],
        allow_internal_tables: bool,
        context: &mut ExecutionContext,
    ) -> Result<crate::ExecuteResult, LixError> {
        let program = ExecutionProgram::compile(parsed_statements, params, transaction.dialect())?;
        execute_execution_program_with_transaction(
            self,
            transaction,
            &program,
            allow_internal_tables,
            context,
        )
        .await
    }

    pub(crate) async fn flush_mutation_journal_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        context: &mut ExecutionContext,
    ) -> Result<(), LixError> {
        let Some(delta) = context.mutation_journal.take_staged_delta() else {
            return Ok(());
        };
        let execution = run_txn_delta_with_transaction(
            self,
            transaction,
            &delta,
            Some(&mut context.pending_public_commit_session),
        )
        .await?;
        let active_effects = execution
            .plan_effects_override
            .as_ref()
            .cloned()
            .unwrap_or_default();
        if let Some(version_id) = &active_effects.next_active_version_id {
            context.active_version_id = version_id.clone();
        }
        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
        self.maybe_invalidate_deterministic_settings_cache(&[], &state_commit_stream_changes);
        context
            .pending_state_commit_stream_changes
            .extend(state_commit_stream_changes);
        context.observe_tick_already_emitted |= execution.observe_tick_emitted;
        if context.public_surface_registry_dirty {
            let backend = TransactionBackendAdapter::new(transaction);
            context.public_surface_registry =
                SurfaceRegistry::bootstrap_with_backend(&backend).await?;
            context.bump_public_surface_registry_generation();
        }
        Ok(())
    }

    pub(crate) async fn execute_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
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
                    "execute_with_options_in_transaction expects exactly one SQL statement"
                        .to_string(),
            });
        }
        let cache_key = StatementTemplateCacheKey::new(
            sql,
            transaction.dialect(),
            allow_internal_tables,
            context.public_surface_registry_generation,
        );
        let template = match context.statement_template_cache.get(&cache_key) {
            Some(template) => template.clone(),
            None => {
                let template = StatementTemplate::compile_with_registry(
                    parsed_statements[0].clone(),
                    &context.public_surface_registry,
                    transaction.dialect(),
                    params.len(),
                )?;
                context
                    .statement_template_cache
                    .insert(cache_key, template.clone());
                template
            }
        };
        let bound_template = template.bind(params)?;
        self.execute_bound_statement_template_instance_in_transaction(
            transaction,
            &bound_template,
            allow_internal_tables,
            context,
            deferred_side_effects,
            skip_side_effect_collection,
        )
        .await
    }

    pub(crate) async fn execute_bound_statement_template_instance_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
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
            let pending_transaction_view = context.mutation_journal.pending_transaction_view()?;
            let program = {
                let backend = TransactionBackendAdapter::new(transaction);
                shared_path::compile_execution_step_from_template_instance_with_backend(
                    self,
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
                Err(error) if !context.mutation_journal.is_empty() => {
                    self.flush_mutation_journal_in_transaction(transaction, context)
                        .await?;
                    let _ = error;
                    continue;
                }
                Err(error) => {
                    let backend = TransactionBackendAdapter::new(transaction);
                    return Err(crate::engine::normalize_sql_execution_error_with_backend(
                        &backend,
                        error,
                        &parsed_statements,
                    )
                    .await);
                }
            };
            let has_materialization_plan = program.has_materialization_plan();
            let write_is_bufferable =
                program.is_bufferable_write(bound_statement_template.statement());
            if write_is_bufferable {
                let statement_delta = program
                    .txn_delta()
                    .cloned()
                    .expect("bufferable write must have a transaction delta");
                let continuation_safe =
                    context.mutation_journal.can_stage_delta(&statement_delta)?;
                if !context.mutation_journal.is_empty() && !continuation_safe {
                    self.flush_mutation_journal_in_transaction(transaction, context)
                        .await?;
                    continue;
                }

                context.mutation_journal.stage_delta(statement_delta)?;
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
                    refresh_public_surface_registry_from_pending_transaction_view(
                        transaction,
                        &mut context.public_surface_registry,
                        &mut context.public_surface_registry_generation,
                        &mut context.public_surface_registry_dirty,
                        &context.mutation_journal,
                    )
                    .await?;
                }
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                });
            }

            if matches!(program.route(), CompiledExecutionRoute::Internal(_))
                && !context.mutation_journal.is_empty()
                && !has_materialization_plan
            {
                // Non-public reads still execute against the committed transaction view.
                // Flush first until that path has its own transaction-local read engine.
                self.flush_mutation_journal_in_transaction(transaction, context)
                    .await?;
                continue;
            }

            if let CompiledExecutionRoute::PublicRead(public_read) = program.route() {
                if !context.mutation_journal.is_empty()
                    && matches!(
                        shared_path::prepared_public_read_transaction_mode(public_read),
                        shared_path::PreparedPublicReadTransactionMode::MaterializedState
                    )
                {
                    self.flush_mutation_journal_in_transaction(transaction, context)
                        .await?;
                    continue;
                }
            }

            let execution = match execute_compiled_execution_step_with_transaction(
                self,
                transaction,
                &program,
                &parsed_statements,
                pending_transaction_view.as_ref(),
                Some(&mut context.pending_public_commit_session),
                writer_key.as_deref(),
            )
            .await?
            {
                CompiledExecutionStepResult::Immediate(public_result) => return Ok(public_result),
                CompiledExecutionStepResult::Outcome(execution) => execution,
            };

            if execution.plan_effects_override.is_none()
                && !matches!(
                    bound_statement_template.statement(),
                    sqlparser::ast::Statement::Query(_) | sqlparser::ast::Statement::Explain { .. }
                )
            {
                context.pending_public_commit_session = None;
            }

            if let Some(public_write) = program.execution().public_write() {
                let mutations = public_surface_registry_mutations(public_write)?;
                if apply_public_surface_registry_mutations(
                    &mut context.public_surface_registry,
                    &mutations,
                )? {
                    context.bump_public_surface_registry_generation();
                    context.public_surface_registry_dirty = true;
                }
            } else if prepared_execution_mutates_public_surface_registry(program.execution())? {
                let backend = TransactionBackendAdapter::new(transaction);
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

            let mut state_commit_stream_changes =
                active_effects.state_commit_stream_changes.clone();
            state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
            self.maybe_invalidate_deterministic_settings_cache(
                program
                    .execution()
                    .internal_execution()
                    .as_ref()
                    .map(|internal| internal.mutations.as_slice())
                    .unwrap_or(&[]),
                &state_commit_stream_changes,
            );

            let write_handled_by_runner = program.txn_delta().is_some();

            if write_handled_by_runner {
                // The universal write runner owns all transactional DB side effects for writes.
            } else if skip_side_effect_collection && deferred_side_effects.is_none() {
                // Internal callers can request executing SQL rewrite/validation without
                // file side-effect collection/persistence/invalidation.
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
                    self.persist_binary_blob_writes_in_transaction(
                        transaction,
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
                // Live public filesystem writes already commit descriptor and payload domain changes
                // through the append boundary. Re-deriving payload effects from pre-commit state
                // inside the same transaction can observe incomplete runtime state and abort the
                // transaction on Postgres.
                let filesystem_finalization = if filesystem_payload_changes_already_committed {
                    None
                } else {
                    Some(
                        self.compile_filesystem_finalization_from_state_in_transaction(
                            transaction,
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
                    self.persist_filesystem_payload_domain_changes_in_transaction(
                        transaction,
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
                    self.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
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
            if !write_handled_by_runner {
                self.persist_runtime_sequence_in_transaction(
                    transaction,
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
}

async fn refresh_public_surface_registry_from_pending_transaction_view(
    transaction: &mut dyn LixTransaction,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_generation: &mut u64,
    public_surface_registry_dirty: &mut bool,
    mutation_journal: &MutationJournal,
) -> Result<(), LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    let pending_transaction_view = mutation_journal.pending_transaction_view()?;
    *public_surface_registry =
        shared_path::bootstrap_public_surface_registry_with_pending_transaction_view(
            &backend,
            pending_transaction_view.as_ref(),
        )
        .await?;
    *public_surface_registry_generation += 1;
    *public_surface_registry_dirty = true;
    Ok(())
}

fn apply_buffered_write_planning_effects(
    execution: &CompiledExecution,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_generation: &mut u64,
    public_surface_registry_dirty: &mut bool,
    active_version_id: &mut String,
) -> Result<(), LixError> {
    if let Some(public_write) = execution.public_write() {
        let mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(public_surface_registry, &mutations)? {
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

pub(crate) fn public_write_execution_next_active_version_id(
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
