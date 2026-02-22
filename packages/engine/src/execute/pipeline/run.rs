use std::collections::BTreeSet;

use super::super::super::*;
use super::super::execute_prepared_with_transaction;
use crate::sql::{
    compile_statement_with_state, parse_sql_statements_with_dialect,
    prepare_statement_block_with_transaction_flag, PlaceholderState, StatementBlock,
};

impl Engine {
    pub(crate) async fn execute_v2(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<QueryResult, LixError> {
        if !allow_internal_tables && !self.access_to_internal {
            reject_internal_table_access(sql)?;
        }

        let parsed_statements = parse_sql_statements_with_dialect(sql, self.backend.dialect())?;
        let installed_plugins_cache_invalidation_pending =
            should_invalidate_installed_plugins_cache_for_statements(&parsed_statements);

        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.active_version_id.read().unwrap().clone();
        let starting_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let result = self
            .execute_statement_block_with_options_in_transaction(
                transaction.as_mut(),
                parsed_statements,
                params,
                &options,
                &mut active_version_id,
                false,
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

    pub(crate) async fn execute_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
    ) -> Result<QueryResult, LixError> {
        let parsed_statements = parse_sql_statements_with_dialect(sql, transaction.dialect())?;
        self.execute_statement_block_with_options_in_transaction(
            transaction,
            parsed_statements,
            params,
            options,
            active_version_id,
            skip_side_effect_collection,
            pending_state_commit_stream_changes,
        )
        .await
    }

    pub(crate) async fn execute_statement_block_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        parsed_statements: Vec<Statement>,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
    ) -> Result<QueryResult, LixError> {
        let StatementBlock { statements } = prepare_statement_block_with_transaction_flag(parsed_statements)?;
        let mut last_result = QueryResult { rows: Vec::new() };
        let mut placeholder_state = PlaceholderState::new();

        for statement in statements {
            let (result, next_placeholder_state) = self
                .execute_statement_with_options_in_transaction(
                    transaction,
                    statement,
                    params,
                    placeholder_state,
                    options,
                    active_version_id,
                    skip_side_effect_collection,
                    pending_state_commit_stream_changes,
                )
                .await?;
            last_result = result;
            placeholder_state = next_placeholder_state;
        }

        Ok(last_result)
    }

    async fn execute_statement_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        statement: Statement,
        params: &[Value],
        placeholder_state: PlaceholderState,
        options: &ExecuteOptions,
        active_version_id: &mut String,
        skip_side_effect_collection: bool,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
    ) -> Result<(QueryResult, PlaceholderState), LixError> {
        let writer_key = options.writer_key.as_deref();
        let statement_list = std::slice::from_ref(&statement);
        let read_only_query = is_query_only_statements(statement_list);
        let should_refresh_file_cache =
            !read_only_query && should_refresh_file_cache_for_statements(statement_list);

        let (settings, sequence_start, functions, output, next_placeholder_state) = {
            let backend = TransactionBackendAdapter::new(transaction);

            if read_only_query {
                self.maybe_refresh_working_change_projection_for_read_query(
                    &backend,
                    active_version_id.as_str(),
                )
                .await?;
            }

            let (settings, sequence_start, functions) = self
                .prepare_runtime_functions_with_backend(&backend)
                .await?;

            let (compiled, next_placeholder_state) = compile_statement_with_state(
                &backend,
                &self.cel_evaluator,
                statement,
                params,
                functions.clone(),
                writer_key,
                placeholder_state,
            )
            .await?;
            let output = compiled;

            self.run_read_maintenance_with_backend_from_plan(
                &backend,
                &output.maintenance_requirements,
                &output.prepared_statements,
                active_version_id.as_str(),
            )
            .await?;

            if !output.mutations.is_empty() {
                validate_inserts(&backend, &self.schema_cache, &output.mutations).await?;
            }
            if !output.update_validations.is_empty() {
                let validation_params = single_prepared_statement_params(
                    &output.prepared_statements,
                    "update validation",
                )?;
                validate_updates(
                    &backend,
                    &self.schema_cache,
                    &output.update_validations,
                    validation_params,
                )
                .await?;
            }

            (
                settings,
                sequence_start,
                functions,
                output,
                next_placeholder_state,
            )
        };

        let state_commit_stream_changes =
            state_commit_stream_changes_from_mutations(&output.mutations, writer_key);
        pending_state_commit_stream_changes.extend(state_commit_stream_changes);

        let next_active_version_id_from_mutations =
            active_version_from_mutations(&output.mutations)?;
        let next_active_version_id_from_updates =
            active_version_from_update_validations(&output.update_validations)?;

        for registration in output.registrations {
            for register_sql in
                register_schema_sql_statements(&registration.schema_key, transaction.dialect())
            {
                transaction.execute(&register_sql, &[]).await?;
            }
        }

        let mut postprocess_file_cache_targets = BTreeSet::new();
        let mut plugin_changes_committed = false;
        let mut pending_file_writes = Vec::new();
        let mut pending_file_delete_targets = BTreeSet::new();
        let mut detected_file_domain_changes = Vec::new();
        let mut untracked_filesystem_update_domain_changes = Vec::new();
        let result = match output.postprocess {
            None => {
                let result =
                    execute_prepared_with_transaction(transaction, &output.prepared_statements)
                        .await?;
                if !skip_side_effect_collection && !read_only_query {
                    let CollectedExecutionSideEffects {
                        pending_file_writes: collected_pending_file_writes,
                        pending_file_delete_targets: collected_pending_file_delete_targets,
                        detected_file_domain_changes: collected_detected_file_domain_changes,
                        untracked_filesystem_update_domain_changes:
                            collected_untracked_filesystem_update_domain_changes,
                    } = {
                        let backend = TransactionBackendAdapter::new(transaction);
                        self.collect_execution_side_effects_with_backend_from_mutations(
                            &backend,
                            &output.mutations,
                            writer_key,
                        )
                        .await?
                    };
                    pending_file_writes = collected_pending_file_writes;
                    pending_file_delete_targets = collected_pending_file_delete_targets;
                    detected_file_domain_changes = collected_detected_file_domain_changes;
                    untracked_filesystem_update_domain_changes =
                        collected_untracked_filesystem_update_domain_changes;
                }
                result
            }
            Some(postprocess_plan) => {
                let result =
                    execute_prepared_with_transaction(transaction, &output.prepared_statements)
                        .await?;
                if !skip_side_effect_collection && !read_only_query {
                    match &postprocess_plan {
                        PostprocessPlan::VtableUpdate(plan) => {
                            let (
                                row_pending_file_writes,
                                tracked_update_changes,
                                untracked_update_changes,
                            ) = {
                                let backend = TransactionBackendAdapter::new(transaction);
                                let row_pending_file_writes = self
                                    .collect_filesystem_update_pending_file_writes_from_update_rows(
                                        &backend,
                                        &plan.schema_key,
                                        &result.rows,
                                    )
                                    .await?;
                                let (tracked_update_changes, untracked_update_changes) = self
                                    .collect_filesystem_update_detected_file_domain_changes_from_update_rows(
                                        &backend,
                                        &plan.schema_key,
                                        &result.rows,
                                        writer_key,
                                    )
                                    .await?;
                                (
                                    row_pending_file_writes,
                                    tracked_update_changes,
                                    untracked_update_changes,
                                )
                            };
                            if !row_pending_file_writes.is_empty() {
                                let detected_file_changes_by_statement = {
                                    let backend = TransactionBackendAdapter::new(transaction);
                                    self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                                        &backend,
                                        std::slice::from_ref(&row_pending_file_writes),
                                        false,
                                    )
                                    .await?
                                };
                                let detected_file_changes = detected_file_changes_by_statement
                                    .into_iter()
                                    .next()
                                    .unwrap_or_default();
                                detected_file_domain_changes.extend(
                                    detected_file_domain_changes_from_detected_file_changes(
                                        &detected_file_changes,
                                        writer_key,
                                    ),
                                );
                                pending_file_writes.extend(row_pending_file_writes);
                            }

                            detected_file_domain_changes.extend(tracked_update_changes);
                            untracked_filesystem_update_domain_changes
                                .extend(untracked_update_changes);

                            let authoritative_data_writes = {
                                let backend = TransactionBackendAdapter::new(transaction);
                                self.collect_filesystem_update_data_pending_file_writes_from_rows(
                                    &backend,
                                    &plan.schema_key,
                                    plan.file_data_assignment.as_ref(),
                                    &result.rows,
                                )
                                .await?
                            };
                            if !authoritative_data_writes.is_empty() {
                                let detected_file_changes_by_statement = {
                                    let backend = TransactionBackendAdapter::new(transaction);
                                    self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                                        &backend,
                                        std::slice::from_ref(&authoritative_data_writes),
                                        false,
                                    )
                                    .await?
                                };
                                let detected_file_changes = detected_file_changes_by_statement
                                    .into_iter()
                                    .next()
                                    .unwrap_or_default();
                                detected_file_domain_changes.extend(
                                    detected_file_domain_changes_from_detected_file_changes(
                                        &detected_file_changes,
                                        writer_key,
                                    ),
                                );
                                pending_file_writes.extend(authoritative_data_writes);
                            }

                            detected_file_domain_changes =
                                dedupe_detected_file_domain_changes(&detected_file_domain_changes);
                            untracked_filesystem_update_domain_changes =
                                dedupe_detected_file_domain_changes(
                                    &untracked_filesystem_update_domain_changes,
                                );
                        }
                        PostprocessPlan::VtableDelete(plan) => {
                            let (row_pending_file_writes, row_pending_file_delete_targets) = {
                                let backend = TransactionBackendAdapter::new(transaction);
                                self.collect_filesystem_delete_side_effects_from_delete_rows(
                                    &backend,
                                    &plan.schema_key,
                                    &result.rows,
                                )
                                .await?
                            };
                            if !row_pending_file_writes.is_empty() {
                                let detected_file_changes_by_statement = {
                                    let backend = TransactionBackendAdapter::new(transaction);
                                    self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                                        &backend,
                                        std::slice::from_ref(&row_pending_file_writes),
                                        false,
                                    )
                                    .await?
                                };
                                let detected_file_changes = detected_file_changes_by_statement
                                    .into_iter()
                                    .next()
                                    .unwrap_or_default();
                                detected_file_domain_changes.extend(
                                    detected_file_domain_changes_from_detected_file_changes(
                                        &detected_file_changes,
                                        writer_key,
                                    ),
                                );
                                pending_file_writes.extend(row_pending_file_writes);
                            }
                            pending_file_delete_targets.extend(row_pending_file_delete_targets);
                            detected_file_domain_changes =
                                dedupe_detected_file_domain_changes(&detected_file_domain_changes);
                        }
                    }
                }
                match &postprocess_plan {
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
                    for register_sql in
                        register_schema_sql_statements(&schema_key, transaction.dialect())
                    {
                        transaction.execute(&register_sql, &[]).await?;
                    }
                }
                let mut followup_functions = functions.clone();
                let followup_statements = match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        build_update_followup_sql(
                            transaction,
                            &plan,
                            &result.rows,
                            &detected_file_domain_changes,
                            writer_key,
                            &mut followup_functions,
                        )
                        .await?
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        let followup_params = single_prepared_statement_params(
                            &output.prepared_statements,
                            "delete followup",
                        )?;
                        build_delete_followup_sql(
                            transaction,
                            &plan,
                            &result.rows,
                            followup_params,
                            &detected_file_domain_changes,
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

        if !skip_side_effect_collection {
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

        Ok((result, next_placeholder_state))
    }
}

fn single_prepared_statement_params<'a>(
    prepared_statements: &'a [crate::sql::PreparedStatement],
    context: &str,
) -> Result<&'a [Value], LixError> {
    if prepared_statements.len() != 1 {
        return Err(LixError {
            message: format!(
                "{context} requires exactly one prepared statement, got {}",
                prepared_statements.len()
            ),
        });
    }
    Ok(prepared_statements[0].params.as_slice())
}
