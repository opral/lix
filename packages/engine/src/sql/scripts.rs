use super::super::*;
use crate::internal_state::script::prepare_statement_script_sql_statements;
use super::semantics::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;

impl Engine {
    pub(crate) async fn execute_transaction_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_statement_script_with_options(statements, params, &options)
            .await
    }

    pub(crate) async fn execute_statement_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.require_active_version_id()?;
        let starting_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let installed_plugins_cache_invalidation_pending =
            should_invalidate_installed_plugins_cache_for_statements(&statements);
        let result = self
            .execute_statement_script_with_options_in_transaction(
                transaction.as_mut(),
                statements,
                params,
                options,
                &mut active_version_id,
                &mut pending_state_commit_stream_changes,
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
        self.emit_state_commit_stream_changes(pending_state_commit_stream_changes);
        Ok(result)
    }

    pub(crate) async fn execute_statement_script_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        original_statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
    ) -> Result<ExecuteResult, LixError> {
        let can_defer_side_effects = false;
        let mut deferred_side_effects = if can_defer_side_effects {
            let CollectedExecutionSideEffects {
                pending_file_writes,
                pending_file_delete_targets,
                ..
            } = {
                let backend = TransactionBackendAdapter::new(transaction);
                self.collect_execution_side_effects_with_backend_from_statements(
                    &backend,
                    &original_statements,
                    params,
                    active_version_id,
                    options.writer_key.as_deref(),
                )
                .await?
            };
            Some(DeferredTransactionSideEffects {
                pending_file_writes,
                pending_file_delete_targets: pending_file_delete_targets.clone(),
            })
        } else {
            None
        };
        let sql_statements = prepare_statement_script_sql_statements(
            original_statements,
            params,
            transaction.dialect(),
        )?;
        let skip_statement_side_effect_collection = deferred_side_effects.is_some();

        let mut statement_results = Vec::with_capacity(sql_statements.len());
        for (sql, statement_params) in sql_statements {
            let result = if skip_statement_side_effect_collection {
                self.execute_with_options_in_transaction(
                    transaction,
                    &sql,
                    &statement_params,
                    options,
                    active_version_id,
                    deferred_side_effects.as_mut(),
                    true,
                    pending_state_commit_stream_changes,
                )
                .await
            } else {
                self.execute_with_options_in_transaction(
                    transaction,
                    &sql,
                    &statement_params,
                    options,
                    active_version_id,
                    None,
                    false,
                    pending_state_commit_stream_changes,
                )
                .await
            };

            match result {
                Ok(query_result) => {
                    statement_results.push(query_result);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        if let Some(side_effects) = deferred_side_effects.as_mut() {
            self.flush_deferred_transaction_side_effects_in_transaction(
                transaction,
                side_effects,
                options.writer_key.as_deref(),
            )
            .await?;
        }
        Ok(ExecuteResult {
            statements: statement_results,
        })
    }
}
