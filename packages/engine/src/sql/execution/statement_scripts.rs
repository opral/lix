use crate::engine::{Engine, PendingWriteTxnBuffer, TransactionBackendAdapter};
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry,
};
use crate::sql::execution::transaction_exec::public_write_execution_next_active_version_id;
use crate::sql::execution::write_txn_plan::{
    build_write_txn_plan, write_txn_plan_is_independent_filesystem,
};
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, classify_public_execution_route_with_registry,
    public_surface_registry_mutations, PublicExecutionRoute,
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
        let mut transaction = self.begin_write_unit().await?;
        let mut core = self.new_shared_transaction_core(options.clone())?;
        let installed_plugins_cache_invalidation_pending =
            should_invalidate_installed_plugins_cache_for_statements(&statements);
        core.installed_plugins_cache_invalidation_pending =
            installed_plugins_cache_invalidation_pending;
        let result = self
            .execute_statement_script_with_options_in_transaction(
                transaction.as_mut(),
                statements,
                params,
                options,
                allow_internal_tables,
                &mut core.public_surface_registry,
                &mut core.public_surface_registry_dirty,
                &mut core.active_version_id,
                &mut core.pending_write_txn_buffer,
                &mut core.pending_state_commit_stream_changes,
                &mut core.pending_public_commit_session,
                &mut core.observe_tick_already_emitted,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        self.prepare_transaction_core_for_commit(transaction.as_mut(), &mut core)
            .await?;
        transaction.commit().await?;
        self.finalize_committed_transaction_core(core).await?;
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
        pending_write_txn_buffer: &mut Option<PendingWriteTxnBuffer>,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_commit_session: &mut Option<
            crate::sql::execution::shared_path::PendingPublicCommitSession,
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
        self.flush_pending_write_txn_buffer_in_transaction(
            transaction,
            public_surface_registry,
            public_surface_registry_dirty,
            active_version_id,
            pending_write_txn_buffer,
            pending_state_commit_stream_changes,
            pending_public_commit_session,
            observe_tick_already_emitted,
        )
        .await?;
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
            pending_write_txn_buffer,
            pending_state_commit_stream_changes,
            pending_public_commit_session,
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
        pending_write_txn_buffer: &mut Option<PendingWriteTxnBuffer>,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_commit_session: &mut Option<
            crate::sql::execution::shared_path::PendingPublicCommitSession,
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
            if prepared_execution_mutates_public_surface_registry(&combined_prepared)? {
                *public_surface_registry_dirty = true;
            }
            crate::sql::execution::transaction_exec::append_pending_write_txn_buffer(
                pending_write_txn_buffer,
                combined_plan,
                false,
            );
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

            if registry_dirty {
                *public_surface_registry = planning_registry;
                *public_surface_registry_dirty = true;
            }
            *active_version_id = planning_active_version_id;
            let combined_continuation_safe =
                write_txn_plan_is_independent_filesystem(&combined_plan);
            crate::sql::execution::transaction_exec::append_pending_write_txn_buffer(
                pending_write_txn_buffer,
                combined_plan,
                combined_continuation_safe,
            );
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
                    pending_write_txn_buffer,
                    None,
                    false,
                    pending_state_commit_stream_changes,
                    pending_public_commit_session,
                    observe_tick_already_emitted,
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
