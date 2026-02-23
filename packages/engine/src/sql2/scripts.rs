use super::super::*;
use super::planning::bind_once::bind_script_placeholders_once;
use super::planning::script::{
    coalesce_lix_file_transaction_statements, coalesce_vtable_inserts_in_statement_list,
};
use super::semantics::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;

impl Engine {
    pub(crate) async fn execute_transaction_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        self.execute_statement_script_with_options(statements, params, &options)
            .await
    }

    pub(crate) async fn execute_statement_script_with_options(
        &self,
        statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.active_version_id.read().unwrap().clone();
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
    ) -> Result<QueryResult, LixError> {
        let coalesced_statements = if params.is_empty() {
            coalesce_lix_file_transaction_statements(
                &original_statements,
                Some(transaction.dialect()),
            )
        } else {
            None
        };
        let can_defer_side_effects = false;
        let mut deferred_side_effects = if can_defer_side_effects {
            let CollectedExecutionSideEffects {
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes: filesystem_tracked_domain_changes,
                untracked_filesystem_update_domain_changes,
                ..
            } = {
                let backend = TransactionBackendAdapter::new(transaction);
                self.collect_execution_side_effects_with_backend_from_statements(
                    &backend,
                    &original_statements,
                    params,
                    active_version_id,
                    options.writer_key.as_deref(),
                    false,
                    false,
                )
                .await?
            };
            Some(DeferredTransactionSideEffects {
                pending_file_writes,
                pending_file_delete_targets: pending_file_delete_targets.clone(),
                detected_file_domain_changes: filesystem_tracked_domain_changes,
                untracked_filesystem_update_domain_changes,
                file_cache_invalidation_targets: pending_file_delete_targets,
            })
        } else {
            None
        };
        let sql_statements = if let Some(coalesced) = coalesced_statements {
            coalesced
                .into_iter()
                .map(|sql| (sql, Vec::new()))
                .collect::<Vec<_>>()
        } else if params.is_empty() {
            coalesce_vtable_inserts_in_statement_list(original_statements)?
                .into_iter()
                .map(|statement| (statement.to_string(), Vec::new()))
                .collect::<Vec<_>>()
        } else {
            bind_script_placeholders_once(&original_statements, params, transaction.dialect())
                .map_err(LixError::from)?
        };
        let skip_statement_side_effect_collection = deferred_side_effects.is_some();

        let mut last_result = QueryResult { rows: Vec::new() };
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
                    last_result = query_result;
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
        Ok(last_result)
    }
}
