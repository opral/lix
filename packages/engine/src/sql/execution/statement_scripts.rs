use crate::engine::{Engine, TransactionBackendAdapter};
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::shared_path::{
    self, prepared_execution_mutates_public_surface_registry,
};
use crate::sql::execution::write_txn_plan::build_write_txn_plan;
use crate::sql::execution::write_txn_runner::run_write_txn_plan_with_transaction;
use crate::sql::public::runtime::classify_public_execution_route_with_registry;
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
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if !pending_state_commit_stream_changes.is_empty() {
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
    ) -> Result<ExecuteResult, LixError> {
        if original_statements.len() != sql_statements.len() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "statement script preparation produced a mismatched statement count"
                    .to_string(),
            });
        }

        let writer_key = options.writer_key.as_deref();
        let mut executable_statements = Vec::new();
        for (statement, statement_sql) in original_statements.iter().zip(sql_statements.iter()) {
            if matches!(
                statement,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                continue;
            }
            executable_statements.push((statement, statement_sql));
        }
        if executable_statements.is_empty() {
            return Ok(ExecuteResult {
                statements: vec![
                    crate::QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    };
                    result_statement_count
                ],
            });
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
                return Ok(ExecuteResult {
                    statements: vec![
                        crate::QueryResult {
                            rows: Vec::new(),
                            columns: Vec::new(),
                        };
                        result_statement_count
                    ],
                });
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
            return Ok(ExecuteResult {
                statements: vec![
                    crate::QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    };
                    result_statement_count
                ],
            });
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
