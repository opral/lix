use super::super::*;
use super::execution::{run, shared_path};
use super::planning::parse::parse_sql;

impl Engine {
    pub(crate) async fn execute_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
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
        let defer_side_effects = deferred_side_effects.is_some();
        let prepared = {
            let backend = TransactionBackendAdapter::new(transaction);
            shared_path::prepare_execution_with_backend(
                self,
                &backend,
                &parsed_statements,
                params,
                active_version_id.as_str(),
                writer_key,
                shared_path::PreparationPolicy {
                    allow_plugin_cache: false,
                    detect_plugin_file_changes: !defer_side_effects,
                    skip_side_effect_collection,
                },
            )
            .await?
        };

        let execution = match run::execute_plan_sql_with_transaction(
            transaction,
            &prepared.plan,
            &prepared.intent.detected_file_domain_changes,
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
                return Err(normalize_sql_execution_error_with_backend(
                    &backend,
                    error,
                    &parsed_statements,
                )
                .await);
            }
        };

        if let Some(version_id) = &prepared.plan.effects.next_active_version_id {
            *active_version_id = version_id.clone();
        }

        let cache_targets = shared_path::derive_cache_targets(
            &prepared.plan,
            execution.postprocess_file_cache_targets.clone(),
        );
        let mut state_commit_stream_changes =
            prepared.plan.effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
        let should_run_binary_gc = should_run_binary_cas_gc(
            &prepared.plan.preprocess.mutations,
            &prepared.intent.detected_file_domain_changes,
        );

        if skip_side_effect_collection && deferred_side_effects.is_none() {
            // Internal callers can request executing SQL rewrite/validation without
            // file side-effect collection/persistence/invalidation.
        } else if let Some(deferred) = deferred_side_effects {
            deferred
                .pending_file_writes
                .extend(prepared.intent.pending_file_writes.clone());
            deferred.file_data_cache_invalidation_targets.extend(
                cache_targets
                    .file_data_cache_invalidation_targets
                    .iter()
                    .cloned(),
            );
            deferred.file_path_cache_invalidation_targets.extend(
                cache_targets
                    .file_path_cache_invalidation_targets
                    .iter()
                    .cloned(),
            );
            if !execution.plugin_changes_committed {
                deferred
                    .detected_file_domain_changes
                    .extend(prepared.intent.detected_file_domain_changes.clone());
            }
            deferred.untracked_filesystem_update_domain_changes.extend(
                prepared
                    .intent
                    .untracked_filesystem_update_domain_changes
                    .clone(),
            );
        } else {
            if !execution.plugin_changes_committed
                && !prepared.intent.detected_file_domain_changes.is_empty()
            {
                self.persist_detected_file_domain_changes_in_transaction(
                    transaction,
                    &prepared.intent.detected_file_domain_changes,
                )
                .await?;
            }
            if !prepared
                .intent
                .untracked_filesystem_update_domain_changes
                .is_empty()
            {
                self.persist_untracked_file_domain_changes_in_transaction(
                    transaction,
                    &prepared.intent.untracked_filesystem_update_domain_changes,
                )
                .await?;
            }
            self.persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await?;
            self.persist_pending_file_path_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await?;
            self.ensure_builtin_binary_blob_store_for_targets_in_transaction(
                transaction,
                &cache_targets.file_data_cache_invalidation_targets,
            )
            .await?;
            if should_run_binary_gc {
                self.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
                    .await?;
            }
            self.invalidate_file_data_cache_entries_in_transaction(
                transaction,
                &cache_targets.file_data_cache_invalidation_targets,
            )
            .await?;
            self.invalidate_file_path_cache_entries_in_transaction(
                transaction,
                &cache_targets.file_path_cache_invalidation_targets,
            )
            .await?;
        }
        self.persist_runtime_sequence_with_backend(
            &TransactionBackendAdapter::new(transaction),
            prepared.settings,
            prepared.sequence_start,
            &prepared.functions,
        )
        .await?;

        pending_state_commit_stream_changes.extend(state_commit_stream_changes);
        Ok(execution.public_result)
    }
}
