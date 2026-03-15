use crate::engine::{Engine, TransactionBackendAdapter};
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry,
};
use crate::sql::execution::write_txn_plan::build_write_txn_plan;
use crate::sql::execution::write_txn_runner::run_write_txn_plan_with_transaction;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, classify_public_execution_route_with_registry,
    public_surface_registry_mutations, PublicExecutionRoute, PublicWriteExecutionPartition,
};
use crate::state::internal::script::{
    coalesce_vtable_inserts_in_statement_list, prepare_statement_script_sql_statements,
};
use crate::state::stream::StateCommitStreamChange;
use crate::{ExecuteOptions, ExecuteResult, LixError, LixTransaction, Value};
use sqlparser::ast::Statement;

impl Engine {
    pub(crate) async fn execute_transaction_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_statement_script_with_options(
            statements,
            params,
            &options,
            allow_internal_tables,
        )
        .await
    }

    pub(crate) async fn execute_statement_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<ExecuteResult, LixError> {
        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.require_active_version_id()?;
        let mut public_surface_registry = self.public_surface_registry();
        let starting_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let mut pending_public_append_session = None;
        let mut observe_tick_already_emitted = false;
        let mut public_surface_registry_dirty = false;
        let installed_plugins_cache_invalidation_pending =
            should_invalidate_installed_plugins_cache_for_statements(&statements);
        let result = self
            .execute_statement_script_with_options_in_transaction(
                transaction.as_mut(),
                statements,
                params,
                options,
                allow_internal_tables,
                &mut public_surface_registry,
                &mut public_surface_registry_dirty,
                &mut active_version_id,
                &mut pending_state_commit_stream_changes,
                &mut pending_public_append_session,
                &mut observe_tick_already_emitted,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if !observe_tick_already_emitted && !pending_state_commit_stream_changes.is_empty() {
            self.append_observe_tick_in_transaction(
                transaction.as_mut(),
                options.writer_key.as_deref(),
            )
            .await?;
        }
        transaction.commit().await?;
        if active_version_id != starting_active_version_id {
            self.set_active_version_id(active_version_id);
        }
        if installed_plugins_cache_invalidation_pending {
            self.invalidate_installed_plugins_cache()?;
        }
        if public_surface_registry_dirty {
            self.refresh_public_surface_registry().await?;
        }
        self.emit_state_commit_stream_changes(pending_state_commit_stream_changes);
        Ok(result)
    }

    pub(crate) async fn execute_statement_script_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        original_statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
        allow_internal_tables: bool,
        public_surface_registry: &mut crate::sql::public::catalog::SurfaceRegistry,
        public_surface_registry_dirty: &mut bool,
        active_version_id: &mut String,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_append_session: &mut Option<
            crate::sql::execution::shared_path::PendingPublicAppendSession,
        >,
        observe_tick_already_emitted: &mut bool,
    ) -> Result<ExecuteResult, LixError> {
        let result_statement_count = original_statements.len();
        let script_statements = coalesce_vtable_inserts_in_statement_list(original_statements)?;
        let sql_statements = prepare_statement_script_sql_statements(
            script_statements.clone(),
            params,
            transaction.dialect(),
        )?;
        self.execute_statement_script_as_combined_write_txn_in_transaction(
            transaction,
            &script_statements,
            params,
            &sql_statements,
            result_statement_count,
            options,
            allow_internal_tables,
            public_surface_registry,
            public_surface_registry_dirty,
            active_version_id,
            pending_state_commit_stream_changes,
            pending_public_append_session,
            observe_tick_already_emitted,
        )
        .await
    }

    async fn execute_statement_script_as_combined_write_txn_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        original_statements: &[Statement],
        params: &[Value],
        sql_statements: &[(String, Vec<Value>)],
        result_statement_count: usize,
        options: &ExecuteOptions,
        allow_internal_tables: bool,
        public_surface_registry: &mut crate::sql::public::catalog::SurfaceRegistry,
        public_surface_registry_dirty: &mut bool,
        active_version_id: &mut String,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_append_session: &mut Option<
            crate::sql::execution::shared_path::PendingPublicAppendSession,
        >,
        observe_tick_already_emitted: &mut bool,
    ) -> Result<ExecuteResult, LixError> {
        if original_statements.len() != sql_statements.len() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "statement script preparation produced a mismatched statement count"
                    .to_string(),
            });
        }

        let writer_key = options.writer_key.as_deref();
        let executable_statements = original_statements
            .iter()
            .zip(sql_statements.iter())
            .filter(|(statement, _)| !is_transaction_control(statement))
            .collect::<Vec<_>>();
        if executable_statements.is_empty() {
            return Ok(empty_mutating_script_result(result_statement_count));
        }

        let internal_only_script = original_statements.iter().all(|statement| {
            classify_public_execution_route_with_registry(
                public_surface_registry,
                std::slice::from_ref(statement),
            )
            .is_none()
        });
        let internal_only_mutating_script = internal_only_script
            && original_statements.iter().all(|statement| {
                !matches!(statement, Statement::Query(_) | Statement::Explain { .. })
            });
        if internal_only_mutating_script {
            let backend = TransactionBackendAdapter::new(transaction);
            let combined_prepared = shared_path::prepare_execution_with_backend(
                self,
                &backend,
                original_statements,
                params,
                active_version_id.as_str(),
                writer_key,
                allow_internal_tables,
                Some(public_surface_registry),
                shared_path::PreparationPolicy {
                    skip_side_effect_collection: false,
                },
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "statement script combined prepare_execution_with_backend failed: {}",
                    error.description
                ),
            })?;
            let Some(combined_plan) = build_write_txn_plan(&combined_prepared, writer_key) else {
                return Ok(empty_mutating_script_result(result_statement_count));
            };
            let execution = run_write_txn_plan_with_transaction(
                self,
                transaction,
                &combined_plan,
                crate::sql::execution::write_txn_plan::WriteTxnRunMode::Borrowed,
                Some(pending_public_append_session),
            )
            .await?;

            let active_effects = execution
                .plan_effects_override
                .as_ref()
                .cloned()
                .unwrap_or_default();
            let mut state_commit_stream_changes =
                active_effects.state_commit_stream_changes.clone();
            state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
            if let Some(version_id) = &active_effects.next_active_version_id {
                *active_version_id = version_id.clone();
            }

            self.maybe_invalidate_deterministic_settings_cache(
                &combined_prepared.plan.preprocess.mutations,
                &state_commit_stream_changes,
            );
            if prepared_execution_mutates_public_surface_registry(&combined_prepared)? {
                let backend = TransactionBackendAdapter::new(transaction);
                *public_surface_registry =
                    crate::sql::public::catalog::SurfaceRegistry::bootstrap_with_backend(&backend)
                        .await?;
                *public_surface_registry_dirty = true;
            }

            pending_state_commit_stream_changes.extend(state_commit_stream_changes);
            return Ok(empty_mutating_script_result(result_statement_count));
        }

        let public_mutating_only_script = executable_statements.iter().all(|(statement, _)| {
            !matches!(statement, Statement::Query(_) | Statement::Explain { .. })
                && classify_public_execution_route_with_registry(
                    public_surface_registry,
                    std::slice::from_ref(*statement),
                ) == Some(PublicExecutionRoute::Write)
        });
        if public_mutating_only_script {
            let defer_runtime_sequence_load = !allow_internal_tables
                && !crate::filesystem::pending_file_writes::statements_require_generated_file_insert_ids(
                    original_statements,
                );
            let (shared_settings, shared_sequence_start, shared_functions) = {
                let backend = TransactionBackendAdapter::new(transaction);
                self.prepare_runtime_functions_with_backend(&backend, defer_runtime_sequence_load)
                    .await?
            };
            let mut combined_plan = crate::sql::execution::write_txn_plan::WriteTxnPlan::default();
            let mut planning_registry = public_surface_registry.clone();
            let mut planning_active_version_id = active_version_id.clone();
            let mut registry_dirty = false;

            for (_statement, (sql, statement_params)) in &executable_statements {
                let parsed_statement = parse_sql(sql).map_err(LixError::from).and_then(
                    |statements| match statements.as_slice() {
                        [statement] => Ok(statement.clone()),
                        _ => Err(LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "statement script combined public path expected exactly one prepared statement",
                        )),
                    },
                )?;
                let prepared = {
                    let backend = TransactionBackendAdapter::new(transaction);
                    shared_path::prepare_execution_with_backend(
                        self,
                        &backend,
                        std::slice::from_ref(&parsed_statement),
                        statement_params,
                        planning_active_version_id.as_str(),
                        writer_key,
                        allow_internal_tables,
                        Some(&planning_registry),
                        shared_path::PreparationPolicy {
                            skip_side_effect_collection: false,
                        },
                    )
                    .await
                    .map_err(|error| LixError {
                        code: error.code,
                        description: format!(
                            "statement script combined public prepare_execution_with_backend failed: {}",
                            error.description
                        ),
                    })?
                };
                let Some(statement_plan) = build_write_txn_plan(&prepared, writer_key) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "public mutating transaction statement did not lower to a write transaction plan",
                    ));
                };
                let mut statement_plan = statement_plan;
                statement_plan.bind_runtime(
                    shared_settings,
                    shared_sequence_start,
                    shared_functions.clone(),
                );
                combined_plan.extend(statement_plan);

                if let Some(public_write) = prepared.public_write.as_ref() {
                    let mutations = public_surface_registry_mutations(public_write)?;
                    if apply_public_surface_registry_mutations(&mut planning_registry, &mutations)?
                    {
                        registry_dirty = true;
                    }
                    if let Some(next_active_version_id) =
                        public_write_execution_next_active_version_id(public_write)
                    {
                        planning_active_version_id = next_active_version_id;
                    }
                }
            }

            let mut local_pending_append_session = None;
            let execution = run_write_txn_plan_with_transaction(
                self,
                transaction,
                &combined_plan,
                crate::sql::execution::write_txn_plan::WriteTxnRunMode::Borrowed,
                if combined_plan.units.len() > 1 {
                    Some(&mut local_pending_append_session)
                } else {
                    None
                },
            )
            .await?;
            let active_effects = execution
                .plan_effects_override
                .as_ref()
                .cloned()
                .unwrap_or_default();
            let mut state_commit_stream_changes =
                active_effects.state_commit_stream_changes.clone();
            state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
            if let Some(version_id) = &active_effects.next_active_version_id {
                *active_version_id = version_id.clone();
            }
            self.maybe_invalidate_deterministic_settings_cache(&[], &state_commit_stream_changes);
            if registry_dirty {
                *public_surface_registry = planning_registry;
                *public_surface_registry_dirty = true;
            }
            pending_state_commit_stream_changes.extend(state_commit_stream_changes);
            *observe_tick_already_emitted |= execution.observe_tick_emitted;
            return Ok(empty_mutating_script_result(result_statement_count));
        }

        let mut results = Vec::with_capacity(result_statement_count);
        for (_statement, (sql, statement_params)) in executable_statements {
            let result = self
                .execute_with_options_in_transaction(
                    transaction,
                    sql,
                    statement_params,
                    options,
                    allow_internal_tables,
                    public_surface_registry,
                    public_surface_registry_dirty,
                    active_version_id,
                    None,
                    false,
                    pending_state_commit_stream_changes,
                    pending_public_append_session,
                )
                .await?;
            results.push(result);
        }
        Ok(ExecuteResult {
            statements: results,
        })
    }
}

fn is_transaction_control(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

fn empty_mutating_script_result(statement_count: usize) -> ExecuteResult {
    ExecuteResult {
        statements: vec![
            crate::QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            };
            statement_count
        ],
    }
}

fn public_write_execution_next_active_version_id(
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
