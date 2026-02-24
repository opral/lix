use super::super::*;
use super::ast::utils::parse_sql_statements;
use super::contracts::planned_statement::MutationOperation;
use super::contracts::postprocess_actions::PostprocessPlan;
use super::execution::execute_prepared::execute_prepared_with_transaction;
use super::execution::followup::{
    build_delete_followup_statements, build_update_followup_statements,
};
use super::semantics::state_resolution::canonical::is_query_only_statements;
use super::semantics::state_resolution::effects::{
    active_version_from_mutations, active_version_from_update_validations,
};
use super::semantics::state_resolution::optimize::should_refresh_file_cache_for_statements;
use super::surfaces::registry::preprocess_with_surfaces;

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
        let parsed_statements = parse_sql_statements(sql)?;
        let writer_key = options.writer_key.as_deref();
        let defer_side_effects = deferred_side_effects.is_some();
        let read_only_query = is_query_only_statements(&parsed_statements);
        let should_refresh_file_cache =
            !read_only_query && should_refresh_file_cache_for_statements(&parsed_statements);
        let (
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes,
            contract_detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
            settings,
            sequence_start,
            functions,
            output,
        ) = {
            let backend = TransactionBackendAdapter::new(transaction);
            if read_only_query {
                self.maybe_refresh_working_change_projection_for_read_query(
                    &backend,
                    active_version_id.as_str(),
                )
                .await?;
            }
            self.maybe_materialize_reads_with_backend_from_statements(
                &backend,
                &parsed_statements,
                active_version_id,
            )
            .await?;
            let CollectedExecutionSideEffects {
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes_by_statement,
                detected_file_domain_changes,
                untracked_filesystem_update_domain_changes,
            } = if skip_side_effect_collection || read_only_query {
                CollectedExecutionSideEffects {
                    pending_file_writes: Vec::new(),
                    pending_file_delete_targets: BTreeSet::new(),
                    detected_file_domain_changes_by_statement: Vec::new(),
                    detected_file_domain_changes: Vec::new(),
                    untracked_filesystem_update_domain_changes: Vec::new(),
                }
            } else {
                self.collect_execution_side_effects_with_backend_from_statements(
                    &backend,
                    &parsed_statements,
                    params,
                    active_version_id,
                    writer_key,
                    false,
                    !defer_side_effects,
                )
                .await?
            };
            let (settings, sequence_start, functions) = self
                .prepare_runtime_functions_with_backend(&backend)
                .await?;
            let contract_detected_file_domain_changes_by_statement =
                detected_file_domain_changes_by_statement.clone();
            let output = preprocess_with_surfaces(
                &backend,
                &self.cel_evaluator,
                parsed_statements.clone(),
                params,
                functions.clone(),
                &contract_detected_file_domain_changes_by_statement,
                writer_key,
            )
            .await?;
            if !output.mutations.is_empty() {
                validate_inserts(&backend, &self.schema_cache, &output.mutations).await?;
            }
            if !output.update_validations.is_empty() {
                validate_updates(
                    &backend,
                    &self.schema_cache,
                    &output.update_validations,
                    params,
                )
                .await?;
            }

            (
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes.clone(),
                detected_file_domain_changes,
                untracked_filesystem_update_domain_changes,
                settings,
                sequence_start,
                functions,
                output,
            )
        };
        let state_commit_stream_changes =
            state_commit_stream_changes_from_mutations(&output.mutations, writer_key);

        let next_active_version_id_from_mutations =
            active_version_from_mutations(&output.mutations)?;
        let next_active_version_id_from_updates =
            active_version_from_update_validations(&output.update_validations)?;
        for registration in &output.registrations {
            for statement in
                register_schema_sql_statements(&registration.schema_key, transaction.dialect())
            {
                transaction.execute(&statement, &[]).await?;
            }
        }

        let mut postprocess_file_cache_targets = BTreeSet::new();
        let mut plugin_changes_committed = false;
        let prepared_statements = output.prepared_statements.clone();
        let result = match output.postprocess.as_ref() {
            None => {
                let result =
                    execute_prepared_with_transaction(transaction, &prepared_statements).await?;
                let tracked_insert_mutation_present = output.mutations.iter().any(|mutation| {
                    mutation.operation == MutationOperation::Insert && !mutation.untracked
                });
                if tracked_insert_mutation_present && !detected_file_domain_changes.is_empty() {
                    plugin_changes_committed = true;
                }
                result
            }
            Some(postprocess_plan) => {
                let result =
                    execute_prepared_with_transaction(transaction, &prepared_statements).await?;
                match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        if should_refresh_file_cache {
                            postprocess_file_cache_targets.extend(
                                collect_postprocess_file_cache_targets(
                                    &result.rows,
                                    &plan.schema_key,
                                )?,
                            );
                        }
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        if should_refresh_file_cache {
                            postprocess_file_cache_targets.extend(
                                collect_postprocess_file_cache_targets(
                                    &result.rows,
                                    &plan.schema_key,
                                )?,
                            );
                        }
                    }
                }
                let additional_schema_keys = detected_file_domain_changes
                    .iter()
                    .map(|change| change.schema_key.clone())
                    .collect::<BTreeSet<_>>();
                for schema_key in additional_schema_keys {
                    for statement in
                        register_schema_sql_statements(&schema_key, transaction.dialect())
                    {
                        transaction.execute(&statement, &[]).await?;
                    }
                }
                let mut followup_functions = functions.clone();
                let followup_params = output
                    .prepared_statements
                    .first()
                    .map(|statement| statement.params.as_slice())
                    .unwrap_or(&[]);
                let followup_statements = match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        build_update_followup_statements(
                            transaction,
                            plan,
                            &result.rows,
                            &contract_detected_file_domain_changes,
                            writer_key,
                            &mut followup_functions,
                        )
                        .await?
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        build_delete_followup_statements(
                            transaction,
                            plan,
                            &result.rows,
                            followup_params,
                            &contract_detected_file_domain_changes,
                            writer_key,
                            &mut followup_functions,
                        )
                        .await?
                    }
                };
                execute_prepared_with_transaction(transaction, &followup_statements).await?;
                plugin_changes_committed = true;
                result
            }
        };

        if let Some(version_id) =
            next_active_version_id_from_mutations.or(next_active_version_id_from_updates)
        {
            *active_version_id = version_id;
        }

        let file_cache_refresh_targets = if should_refresh_file_cache {
            let mut targets = direct_state_file_cache_refresh_targets(&output.mutations);
            targets.extend(postprocess_file_cache_targets);
            targets
        } else {
            BTreeSet::new()
        };
        let descriptor_cache_eviction_targets =
            file_descriptor_cache_eviction_targets(&output.mutations);
        let mut file_cache_invalidation_targets = file_cache_refresh_targets;
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets);
        let should_run_binary_gc =
            should_run_binary_cas_gc(&output.mutations, &detected_file_domain_changes);

        if skip_side_effect_collection && deferred_side_effects.is_none() {
            // Internal callers can request executing SQL rewrite/validation without
            // file side-effect collection/persistence/invalidation.
        } else if let Some(deferred) = deferred_side_effects {
            deferred.pending_file_writes.extend(pending_file_writes);
            deferred
                .file_cache_invalidation_targets
                .extend(file_cache_invalidation_targets.iter().cloned());
            if !plugin_changes_committed {
                deferred
                    .detected_file_domain_changes
                    .extend(detected_file_domain_changes);
            }
            deferred
                .untracked_filesystem_update_domain_changes
                .extend(untracked_filesystem_update_domain_changes);
        } else {
            if !plugin_changes_committed && !detected_file_domain_changes.is_empty() {
                self.persist_detected_file_domain_changes_in_transaction(
                    transaction,
                    &detected_file_domain_changes,
                )
                .await?;
            }
            if !untracked_filesystem_update_domain_changes.is_empty() {
                self.persist_untracked_file_domain_changes_in_transaction(
                    transaction,
                    &untracked_filesystem_update_domain_changes,
                )
                .await?;
            }
            self.persist_pending_file_data_updates_in_transaction(
                transaction,
                &pending_file_writes,
            )
            .await?;
            self.persist_pending_file_path_updates_in_transaction(
                transaction,
                &pending_file_writes,
            )
            .await?;
            self.ensure_builtin_binary_blob_store_for_targets_in_transaction(
                transaction,
                &file_cache_invalidation_targets,
            )
            .await?;
            if should_run_binary_gc {
                self.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
                    .await?;
            }
            self.invalidate_file_data_cache_entries_in_transaction(
                transaction,
                &file_cache_invalidation_targets,
            )
            .await?;
            self.invalidate_file_path_cache_entries_in_transaction(
                transaction,
                &file_cache_invalidation_targets,
            )
            .await?;
        }
        self.persist_runtime_sequence_with_backend(
            &TransactionBackendAdapter::new(transaction),
            settings,
            sequence_start,
            &functions,
        )
        .await?;

        pending_state_commit_stream_changes.extend(state_commit_stream_changes);
        Ok(result)
    }
}
