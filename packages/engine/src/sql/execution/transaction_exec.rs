use crate::engine::{
    dedupe_filesystem_payload_domain_changes, normalize_sql_execution_error_with_backend,
    should_run_binary_cas_gc, DeferredTransactionSideEffects, Engine, TransactionBackendAdapter,
};
use crate::sql::execution::execute;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path;
use crate::sql::execution::shared_path::prepared_execution_mutates_public_surface_registry;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::runtime::{
    apply_public_surface_registry_mutations, decode_public_read_result,
    public_surface_registry_mutations,
};
use crate::{
    ExecuteOptions, LixError, LixTransaction, QueryResult, StateCommitStreamChange, Value,
};

impl Engine {
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
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
        pending_public_append_session: &mut Option<shared_path::PendingPublicAppendSession>,
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
                allow_internal_tables,
                Some(public_surface_registry),
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

        let execution = match shared_path::maybe_execute_public_write_with_transaction(
            self,
            transaction,
            &prepared,
            writer_key,
            Some(pending_public_append_session),
        )
        .await
        {
            Ok(Some(execution)) => execution,
            Ok(None) => match execute::execute_plan_sql_with_transaction(
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
                        "transaction public write execution failed: {}",
                        error.description
                    ),
                })
            }
        };

        if execution.plan_effects_override.is_none()
            && !matches!(
                parsed_statements[0],
                sqlparser::ast::Statement::Query(_) | sqlparser::ast::Statement::Explain { .. }
            )
        {
            *pending_public_append_session = None;
        }

        if let Some(public_write) = prepared.public_write.as_ref() {
            let mutations = public_surface_registry_mutations(public_write)?;
            if apply_public_surface_registry_mutations(public_surface_registry, &mutations)? {
                *public_surface_registry_dirty = true;
            }
        } else if prepared_execution_mutates_public_surface_registry(&prepared)? {
            let backend = TransactionBackendAdapter::new(transaction);
            *public_surface_registry = SurfaceRegistry::bootstrap_with_backend(&backend).await?;
            *public_surface_registry_dirty = true;
        }

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
                shared_path::public_write_filesystem_payload_changes_already_committed(&prepared);
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
            // Live public filesystem writes already commit descriptor and payload domain changes
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

        pending_state_commit_stream_changes.extend(state_commit_stream_changes);
        let public_result = if let Some(public_read) = prepared.public_read.as_ref() {
            decode_public_read_result(execution.public_result, &public_read.lowered_read)
        } else {
            execution.public_result
        };
        Ok(public_result)
    }
}
