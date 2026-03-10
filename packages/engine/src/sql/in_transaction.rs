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
        let _defer_side_effects = deferred_side_effects.is_some();
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

        let execution = match shared_path::maybe_execute_sql2_write_with_transaction(
            self,
            transaction,
            &prepared,
            writer_key,
        )
        .await
        {
            Ok(Some(execution)) => execution,
            Ok(None) => match run::execute_plan_sql_with_transaction(
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
            },
            Err(error) => {
                return Err(LixError {
                    code: error.code,
                    description: format!(
                        "transaction sql2 write execution failed: {}",
                        error.description
                    ),
                })
            }
        };

        let active_effects = execution
            .plan_effects_override
            .as_ref()
            .unwrap_or(&prepared.plan.effects);

        if let Some(version_id) = &active_effects.next_active_version_id {
            *active_version_id = version_id.clone();
        }

        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());

        if skip_side_effect_collection && deferred_side_effects.is_none() {
            // Internal callers can request executing SQL rewrite/validation without
            // file side-effect collection/persistence/invalidation.
        } else if let Some(deferred) = deferred_side_effects {
            deferred
                .pending_file_writes
                .extend(prepared.intent.pending_file_writes.clone());
        } else {
            let filesystem_payload_changes_already_committed =
                shared_path::sql2_commits_filesystem_payload_domain_changes(&prepared);
            if !filesystem_payload_changes_already_committed {
                self.persist_pending_file_data_updates_in_transaction(
                    transaction,
                    &prepared.intent.pending_file_writes,
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
            // Live sql2 filesystem writes already commit descriptor and payload domain changes
            // through the append boundary. Re-deriving payload effects from pre-commit state
            // inside the same transaction can observe incomplete runtime state and abort the
            // transaction on Postgres.
            let filesystem_payload_domain_changes = if filesystem_payload_changes_already_committed
            {
                Vec::new()
            } else {
                self.collect_live_filesystem_payload_domain_changes_in_transaction(
                    transaction,
                    &prepared.intent.pending_file_writes,
                    &prepared.intent.pending_file_delete_targets,
                    writer_key,
                )
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "transaction filesystem payload-domain-change collection failed: {}",
                        error.description
                    ),
                })?
            };
            let filesystem_payload_domain_changes =
                dedupe_filesystem_payload_domain_changes(&filesystem_payload_domain_changes);
            if !filesystem_payload_domain_changes.is_empty()
                && !filesystem_payload_changes_already_committed
            {
                self.persist_filesystem_payload_domain_changes_in_transaction(
                    transaction,
                    &filesystem_payload_domain_changes,
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
            if !filesystem_payload_changes_already_committed
                && should_run_binary_cas_gc(
                    &prepared.plan.preprocess.mutations,
                    &filesystem_payload_domain_changes,
                )
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
        self.persist_runtime_sequence_with_backend(
            &TransactionBackendAdapter::new(transaction),
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

        pending_state_commit_stream_changes.extend(state_commit_stream_changes);
        Ok(execution.public_result)
    }
}
