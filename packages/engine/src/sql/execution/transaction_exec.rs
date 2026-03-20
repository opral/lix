use crate::engine::{
    normalize_sql_execution_error_with_backend, DeferredTransactionSideEffects, Engine,
    SharedTransactionCore, TransactionBackendAdapter,
};
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::execute;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path;
use crate::sql::execution::shared_path::{
    prepared_execution_mutates_public_surface_registry, top_level_write_target_name,
};
use crate::sql::execution::write_txn_plan::{
    build_write_txn_plan, MutationJournal, WriteTxnRunMode,
};
use crate::sql::execution::write_txn_runner::run_write_txn_plan_with_transaction;
use crate::sql::public::catalog::{SurfaceFamily, SurfaceRegistry};
use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql::public::planner::canonicalize::canonicalize_write;
use crate::sql::public::planner::semantics::write_analysis::analyze_write;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, execute_prepared_public_read,
    finalize_prepared_public_read_result, public_surface_registry_mutations,
    PublicWriteExecutionPartition,
};
use crate::{
    ExecuteOptions, LixError, LixTransaction, QueryResult, StateCommitStreamChange, Value,
};
use sqlparser::ast::Statement;

impl Engine {
    pub(crate) async fn execute_parsed_statements_in_transaction_core(
        &self,
        transaction: &mut dyn LixTransaction,
        parsed_statements: Vec<Statement>,
        sql: &str,
        params: &[Value],
        allow_internal_tables: bool,
        core: &mut SharedTransactionCore,
    ) -> Result<crate::ExecuteResult, LixError> {
        let previous_active_version_id = core.active_version_id.clone();
        let result = if parsed_statements.len() > 1 {
            self.execute_statement_script_with_options_in_transaction(
                transaction,
                parsed_statements.clone(),
                params,
                &core.options,
                allow_internal_tables,
                &mut core.public_surface_registry,
                &mut core.public_surface_registry_dirty,
                &mut core.active_version_id,
                &mut core.mutation_journal,
                &mut core.pending_state_commit_stream_changes,
                &mut core.pending_public_commit_session,
                &mut core.observe_tick_already_emitted,
            )
            .await?
        } else {
            let single_statement_result = self
                .execute_with_options_in_transaction(
                    transaction,
                    sql,
                    params,
                    &core.options,
                    allow_internal_tables,
                    &mut core.public_surface_registry,
                    &mut core.public_surface_registry_dirty,
                    &mut core.active_version_id,
                    &mut core.mutation_journal,
                    None,
                    false,
                    &mut core.pending_state_commit_stream_changes,
                    &mut core.pending_public_commit_session,
                    &mut core.observe_tick_already_emitted,
                )
                .await?;
            crate::ExecuteResult {
                statements: vec![single_statement_result],
            }
        };
        if core.active_version_id != previous_active_version_id {
            core.active_version_changed = true;
        }
        if should_invalidate_installed_plugins_cache_for_statements(&parsed_statements) {
            core.installed_plugins_cache_invalidation_pending = true;
        }
        Ok(result)
    }

    pub(crate) async fn flush_mutation_journal_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        public_surface_registry: &mut SurfaceRegistry,
        public_surface_registry_dirty: &mut bool,
        active_version_id: &mut String,
        mutation_journal: &mut MutationJournal,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_commit_session: &mut Option<shared_path::PendingPublicCommitSession>,
        observe_tick_already_emitted: &mut bool,
    ) -> Result<(), LixError> {
        let Some(plan) = mutation_journal.take_staged_write_plan() else {
            return Ok(());
        };
        let execution = run_write_txn_plan_with_transaction(
            self,
            transaction,
            &plan,
            WriteTxnRunMode::Borrowed,
            Some(pending_public_commit_session),
        )
        .await?;
        let active_effects = execution
            .plan_effects_override
            .as_ref()
            .cloned()
            .unwrap_or_default();
        if let Some(version_id) = &active_effects.next_active_version_id {
            *active_version_id = version_id.clone();
        }
        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
        self.maybe_invalidate_deterministic_settings_cache(&[], &state_commit_stream_changes);
        pending_state_commit_stream_changes.extend(state_commit_stream_changes);
        *observe_tick_already_emitted |= execution.observe_tick_emitted;
        if *public_surface_registry_dirty {
            let backend = TransactionBackendAdapter::new(transaction);
            *public_surface_registry = SurfaceRegistry::bootstrap_with_backend(&backend).await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        allow_internal_tables: bool,
        public_surface_registry: &mut SurfaceRegistry,
        public_surface_registry_dirty: &mut bool,
        active_version_id: &mut String,
        mutation_journal: &mut MutationJournal,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_commit_session: &mut Option<shared_path::PendingPublicCommitSession>,
        observe_tick_already_emitted: &mut bool,
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
        let writer_key = options.writer_key.as_deref();
        let _defer_side_effects = deferred_side_effects.is_some();
        loop {
            let pending_registered_schema_overlay =
                mutation_journal.pending_registered_schema_overlay()?;
            let pending_semantic_overlay = mutation_journal.pending_semantic_overlay()?;
            let pending_filesystem_overlay = mutation_journal.pending_filesystem_overlay();
            let pending_overlay_can_continue = statement_can_prepare_against_pending_overlay(
                public_surface_registry,
                transaction.dialect(),
                &parsed_statements[0],
                params,
                active_version_id.as_str(),
                writer_key,
                pending_semantic_overlay.is_some(),
                pending_filesystem_overlay.is_some(),
            );
            if !mutation_journal.is_empty()
                && ((!mutation_journal.continuation_safe()
                    && pending_registered_schema_overlay.is_none()
                    && !pending_overlay_can_continue)
                    || statement_requires_flushed_pending_buffer(&parsed_statements[0]))
            {
                self.flush_mutation_journal_in_transaction(
                    transaction,
                    public_surface_registry,
                    public_surface_registry_dirty,
                    active_version_id,
                    mutation_journal,
                    pending_state_commit_stream_changes,
                    pending_public_commit_session,
                    observe_tick_already_emitted,
                )
                .await?;
            }

            let prepared = {
                let backend = TransactionBackendAdapter::new(transaction);
                shared_path::prepare_execution_with_backend(
                    self,
                    &backend,
                    pending_registered_schema_overlay.as_ref(),
                    pending_semantic_overlay.as_ref(),
                    pending_filesystem_overlay.as_ref(),
                    &parsed_statements,
                    params,
                    active_version_id.as_str(),
                    writer_key,
                    allow_internal_tables,
                    Some(&*public_surface_registry),
                    shared_path::PreparationPolicy {
                        skip_side_effect_collection,
                    },
                )
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction prepare_execution_with_backend failed: {}",
                        error.description
                    ),
                })?
            };

            let write_txn_plan = build_write_txn_plan(&prepared, writer_key);
            let write_is_bufferable = write_txn_plan.is_some()
                && !matches!(prepared.plan.result_contract, ResultContract::DmlReturning)
                && !matches!(
                    parsed_statements[0],
                    Statement::Query(_) | Statement::Explain { .. }
                );
            if write_is_bufferable {
                let statement_plan =
                    write_txn_plan.expect("bufferable write must have a transaction plan");
                let continuation_safe = mutation_journal.can_stage_write_plan(&statement_plan)?;
                if !mutation_journal.is_empty() && !continuation_safe {
                    self.flush_mutation_journal_in_transaction(
                        transaction,
                        public_surface_registry,
                        public_surface_registry_dirty,
                        active_version_id,
                        mutation_journal,
                        pending_state_commit_stream_changes,
                        pending_public_commit_session,
                        observe_tick_already_emitted,
                    )
                    .await?;
                    continue;
                }

                mutation_journal.stage_write_plan(statement_plan)?;
                if continuation_safe {
                    apply_buffered_write_planning_effects(
                        &prepared,
                        public_surface_registry,
                        public_surface_registry_dirty,
                        active_version_id,
                    )?;
                } else if prepared_execution_mutates_public_surface_registry(&prepared)? {
                    *public_surface_registry_dirty = true;
                }
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                });
            }

            if !mutation_journal.is_empty() {
                self.flush_mutation_journal_in_transaction(
                    transaction,
                    public_surface_registry,
                    public_surface_registry_dirty,
                    active_version_id,
                    mutation_journal,
                    pending_state_commit_stream_changes,
                    pending_public_commit_session,
                    observe_tick_already_emitted,
                )
                .await?;
                continue;
            }

            let execution = if let Some(plan) = write_txn_plan.as_ref() {
                run_write_txn_plan_with_transaction(
                    self,
                    transaction,
                    plan,
                    WriteTxnRunMode::Borrowed,
                    Some(pending_public_commit_session),
                )
                .await?
            } else if let Some(public_read) = prepared
                .public_read
                .as_ref()
                .and_then(|prepared| prepared.direct_plan().map(|_| prepared))
            {
                let backend = TransactionBackendAdapter::new(transaction);
                let public_result = execute_prepared_public_read(&backend, public_read).await?;
                return Ok(public_result);
            } else {
                match execute::execute_plan_sql_with_transaction(
                    transaction,
                    &prepared.plan,
                    prepared.plan.requirements.should_refresh_file_cache,
                    &prepared.functions,
                    writer_key,
                )
                .await
                .map_err(LixError::from)
                {
                    Ok(execution) => execution,
                    Err(error) => {
                        let backend = TransactionBackendAdapter::new(transaction);
                        let normalized = normalize_sql_execution_error_with_backend(
                            &backend,
                            error,
                            &parsed_statements,
                        )
                        .await;
                        return Err(LixError {
                            code: normalized.code,
                            description: format!(
                                "transaction legacy plan execution failed: {}",
                                normalized.description
                            ),
                        });
                    }
                }
            };

            if execution.plan_effects_override.is_none()
                && !matches!(
                    parsed_statements[0],
                    sqlparser::ast::Statement::Query(_) | sqlparser::ast::Statement::Explain { .. }
                )
            {
                *pending_public_commit_session = None;
            }

            if let Some(public_write) = prepared.public_write.as_ref() {
                let mutations = public_surface_registry_mutations(public_write)?;
                if apply_public_surface_registry_mutations(public_surface_registry, &mutations)? {
                    *public_surface_registry_dirty = true;
                }
            } else if prepared_execution_mutates_public_surface_registry(&prepared)? {
                let backend = TransactionBackendAdapter::new(transaction);
                *public_surface_registry =
                    SurfaceRegistry::bootstrap_with_backend(&backend).await?;
                *public_surface_registry_dirty = true;
            }

            let active_effects = execution
                .plan_effects_override
                .as_ref()
                .unwrap_or(&prepared.plan.effects);

            if let Some(version_id) = &active_effects.next_active_version_id {
                *active_version_id = version_id.clone();
            }

            let mut state_commit_stream_changes =
                active_effects.state_commit_stream_changes.clone();
            state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
            self.maybe_invalidate_deterministic_settings_cache(
                &prepared.plan.preprocess.mutations,
                &state_commit_stream_changes,
            );

            let write_handled_by_runner = write_txn_plan.is_some();

            if write_handled_by_runner {
                // The universal write runner owns all transactional DB side effects for writes.
            } else if skip_side_effect_collection && deferred_side_effects.is_none() {
                // Internal callers can request executing SQL rewrite/validation without
                // file side-effect collection/persistence/invalidation.
            } else if let Some(deferred) = deferred_side_effects {
                crate::sql::execution::runtime_effects::merge_filesystem_transaction_state(
                    &mut deferred.filesystem_state,
                    &prepared.intent.filesystem_state,
                );
            } else {
                let filesystem_payload_changes_already_committed =
                    shared_path::public_write_filesystem_payload_changes_already_committed(
                        &prepared,
                    );
                let binary_blob_writes =
                    crate::sql::execution::runtime_effects::binary_blob_writes_from_filesystem_state(
                        &prepared.intent.filesystem_state,
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
                            &prepared.intent.filesystem_state,
                            writer_key,
                            &prepared.plan.preprocess.mutations,
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
                    prepared.settings,
                    prepared.sequence_start,
                    &prepared.functions,
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

            pending_state_commit_stream_changes.extend(state_commit_stream_changes);
            let public_result = if let Some(public_read) = prepared.public_read.as_ref() {
                finalize_prepared_public_read_result(execution.public_result, public_read)
            } else {
                execution.public_result
            };
            return Ok(public_result);
        }
    }
}

fn statement_requires_flushed_pending_buffer(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_) | Statement::Explain { .. })
}

pub(crate) fn statement_can_prepare_against_pending_overlay(
    public_surface_registry: &SurfaceRegistry,
    dialect: crate::SqlDialect,
    statement: &Statement,
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_state_overlay: bool,
    allow_filesystem_overlay: bool,
) -> bool {
    let Some(target_name) = top_level_write_target_name(statement) else {
        return false;
    };
    let Some(binding) = public_surface_registry.bind_relation_name(&target_name) else {
        return false;
    };

    if matches!(statement, Statement::Insert(_)) {
        return match binding.descriptor.surface_family {
            SurfaceFamily::State => allow_state_overlay,
            SurfaceFamily::Filesystem => {
                allow_filesystem_overlay
                    && matches!(
                        target_name.as_str(),
                        "lix_file"
                            | "lix_file_by_version"
                            | "lix_directory"
                            | "lix_directory_by_version"
                    )
            }
            _ => false,
        };
    }

    match binding.descriptor.surface_family {
        SurfaceFamily::State if allow_state_overlay => {}
        SurfaceFamily::Filesystem
            if allow_filesystem_overlay
                && matches!(target_name.as_str(), "lix_file" | "lix_file_by_version") => {}
        _ => return false,
    }

    let bound_statement = BoundStatement::from_statement(
        statement.clone(),
        params.to_vec(),
        ExecutionContext {
            dialect: Some(dialect),
            writer_key: writer_key.map(str::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let Ok(canonicalized) = canonicalize_write(bound_statement, public_surface_registry) else {
        return false;
    };
    let Ok(planned_write) = analyze_write(&canonicalized) else {
        return false;
    };

    matches!(
        planned_write.command.operation_kind,
        crate::sql::public::planner::ir::WriteOperationKind::Update
            | crate::sql::public::planner::ir::WriteOperationKind::Delete
    ) && planned_write.command.selector.exact_only
}

fn apply_buffered_write_planning_effects(
    prepared: &shared_path::PreparedExecutionContext,
    public_surface_registry: &mut SurfaceRegistry,
    public_surface_registry_dirty: &mut bool,
    active_version_id: &mut String,
) -> Result<(), LixError> {
    if let Some(public_write) = prepared.public_write.as_ref() {
        let mutations = public_surface_registry_mutations(public_write)?;
        if apply_public_surface_registry_mutations(public_surface_registry, &mutations)? {
            *public_surface_registry_dirty = true;
        }
        if let Some(next_active_version_id) =
            public_write_execution_next_active_version_id(public_write)
        {
            *active_version_id = next_active_version_id;
        }
    } else if let Some(version_id) = &prepared.plan.effects.next_active_version_id {
        *active_version_id = version_id.clone();
    }
    Ok(())
}

pub(crate) fn public_write_execution_next_active_version_id(
    public_write: &crate::sql::public::runtime::PreparedPublicWrite,
) -> Option<String> {
    public_write.execution.as_ref().and_then(|execution| {
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
