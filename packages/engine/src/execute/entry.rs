use super::super::*;
use super::*;

impl Engine {
    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.wasm_runtime.clone()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.state_commit_stream_bus.emit(changes);
    }

    pub(crate) async fn load_installed_plugins_with_backend(
        &self,
        backend: &dyn LixBackend,
        allow_cache: bool,
    ) -> Result<Vec<InstalledPlugin>, LixError> {
        if allow_cache {
            let cached = self
                .installed_plugins_cache
                .read()
                .map_err(|_| LixError {
                    message: "installed plugin cache lock poisoned".to_string(),
                })?
                .clone();
            if let Some(plugins) = cached {
                return Ok(plugins);
            }
        }

        let loaded = crate::plugin::runtime::load_installed_plugins(backend).await?;
        if allow_cache {
            let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
                message: "installed plugin cache lock poisoned".to_string(),
            })?;
            *guard = Some(loaded.clone());
        }
        Ok(loaded)
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
            message: "installed plugin cache lock poisoned".to_string(),
        })?;
        *guard = None;
        let mut component_guard = self.plugin_component_cache.lock().map_err(|_| LixError {
            message: "plugin component cache lock poisoned".to_string(),
        })?;
        component_guard.clear();
        Ok(())
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        self.execute_impl(sql, params, options, false).await
    }

    pub(crate) async fn execute_internal(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        self.execute_impl(sql, params, options, true).await
    }

    pub(crate) async fn execute_impl(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<QueryResult, LixError> {
        if !allow_internal_tables && !self.access_to_internal {
            reject_internal_table_access(sql)?;
        }
        let parsed_statements = parse_sql_statements(sql)?;
        if let Some(statements) =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
        {
            return self
                .execute_transaction_script_with_options(statements, params, options)
                .await;
        }

        let read_only_query = is_query_only_statements(&parsed_statements);
        let active_version_id = self.active_version_id.read().unwrap().clone();
        let writer_key = options.writer_key.as_deref();
        let history_requirements =
            crate::sql::collect_history_requirements_for_statements_with_backend(
                self.backend.as_ref(),
                &parsed_statements,
                params,
            )
            .await?;
        if read_only_query {
            self.maybe_refresh_working_change_projection_for_read_query(
                self.backend.as_ref(),
                &active_version_id,
            )
            .await?;
        }
        self.maybe_materialize_reads_with_backend_from_statements(
            self.backend.as_ref(),
            &parsed_statements,
            &active_version_id,
            &history_requirements,
        )
        .await?;
        let should_refresh_file_cache =
            !read_only_query && should_refresh_file_cache_for_statements(&parsed_statements);
        let CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        } = if read_only_query {
            CollectedExecutionSideEffects {
                pending_file_writes: Vec::new(),
                pending_file_delete_targets: BTreeSet::new(),
                detected_file_domain_changes_by_statement: Vec::new(),
                detected_file_domain_changes: Vec::new(),
                untracked_filesystem_update_domain_changes: Vec::new(),
            }
        } else {
            self.collect_execution_side_effects_with_backend_from_statements(
                self.backend.as_ref(),
                &parsed_statements,
                params,
                &active_version_id,
                writer_key,
                true,
                true,
            )
            .await?
        };
        let (settings, sequence_start, functions) = self
            .prepare_runtime_functions_with_backend(self.backend.as_ref())
            .await?;

        let output =
            match preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(
                self.backend.as_ref(),
                &self.cel_evaluator,
                parsed_statements.clone(),
                params,
                functions.clone(),
                &detected_file_domain_changes_by_statement,
                writer_key,
            )
            .await
            {
                Ok(output) => output,
                Err(error)
                    if should_sequentialize_postprocess_multi_statement_with_statements(
                        &parsed_statements,
                        params,
                        &error,
                    ) =>
                {
                    return self
                        .execute_multi_statement_sequential_with_options(sql, params, &options)
                        .await;
                }
                Err(error) => return Err(error),
            };
        let state_commit_stream_changes =
            state_commit_stream_changes_from_mutations(&output.mutations, writer_key);
        let next_active_version_id_from_mutations =
            active_version_from_mutations(&output.mutations)?;
        let next_active_version_id_from_updates =
            active_version_from_update_validations(&output.update_validations)?;
        if !output.mutations.is_empty() {
            validate_inserts(self.backend.as_ref(), &self.schema_cache, &output.mutations).await?;
        }
        if !output.update_validations.is_empty() {
            validate_updates(
                self.backend.as_ref(),
                &self.schema_cache,
                &output.update_validations,
                params,
            )
            .await?;
        }
        for registration in output.registrations {
            register_schema(self.backend.as_ref(), &registration.schema_key).await?;
        }
        let mut postprocess_file_cache_targets = BTreeSet::new();
        let mut plugin_changes_committed = false;
        let result = match output.postprocess {
            None => {
                let result = execute_prepared_with_backend(
                    self.backend.as_ref(),
                    &output.prepared_statements,
                )
                .await?;
                let tracked_insert_mutation_present = output.mutations.iter().any(|mutation| {
                    mutation.operation == MutationOperation::Insert && !mutation.untracked
                });
                if tracked_insert_mutation_present && !detected_file_domain_changes.is_empty() {
                    plugin_changes_committed = true;
                }
                Ok(result)
            }
            Some(postprocess_plan) => {
                let mut transaction = self.backend.begin_transaction().await?;
                let result = match execute_prepared_with_transaction(
                    transaction.as_mut(),
                    &output.prepared_statements,
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
                match &postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        if should_refresh_file_cache {
                            let targets = match collect_postprocess_file_cache_targets(
                                &result.rows,
                                &plan.schema_key,
                            ) {
                                Ok(targets) => targets,
                                Err(error) => {
                                    let _ = transaction.rollback().await;
                                    return Err(error);
                                }
                            };
                            postprocess_file_cache_targets.extend(targets);
                        }
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        if should_refresh_file_cache {
                            let targets = match collect_postprocess_file_cache_targets(
                                &result.rows,
                                &plan.schema_key,
                            ) {
                                Ok(targets) => targets,
                                Err(error) => {
                                    let _ = transaction.rollback().await;
                                    return Err(error);
                                }
                            };
                            postprocess_file_cache_targets.extend(targets);
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
                        if let Err(error) = transaction.execute(&statement, &[]).await {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    }
                }
                let mut followup_functions = functions.clone();
                let followup_statements = match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => match build_update_followup_sql(
                        transaction.as_mut(),
                        &plan,
                        &result.rows,
                        &detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(statements) => statements,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    },
                    PostprocessPlan::VtableDelete(plan) => match build_delete_followup_sql(
                        transaction.as_mut(),
                        &plan,
                        &result.rows,
                        &output.params,
                        &detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(statements) => statements,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    },
                };
                if let Err(error) =
                    execute_prepared_with_transaction(transaction.as_mut(), &followup_statements)
                        .await
                {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
                transaction.commit().await?;
                plugin_changes_committed = true;
                Ok(result)
            }
        }?;

        self.persist_runtime_sequence_with_backend(
            self.backend.as_ref(),
            settings,
            sequence_start,
            &functions,
        )
        .await?;

        if let Some(version_id) =
            next_active_version_id_from_mutations.or(next_active_version_id_from_updates)
        {
            self.set_active_version_id(version_id);
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
        let mut file_cache_invalidation_targets = file_cache_refresh_targets.clone();
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets);
        let should_run_binary_gc =
            should_run_binary_cas_gc(&output.mutations, &detected_file_domain_changes);

        if !plugin_changes_committed && !detected_file_domain_changes.is_empty() {
            self.persist_detected_file_domain_changes(&detected_file_domain_changes)
                .await?;
        }
        if !untracked_filesystem_update_domain_changes.is_empty() {
            self.persist_untracked_file_domain_changes(&untracked_filesystem_update_domain_changes)
                .await?;
        }
        self.persist_pending_file_data_updates(&pending_file_writes)
            .await?;
        self.persist_pending_file_path_updates(&pending_file_writes)
            .await?;
        self.ensure_builtin_binary_blob_store_for_targets(&file_cache_invalidation_targets)
            .await?;
        if should_run_binary_gc {
            self.garbage_collect_unreachable_binary_cas().await?;
        }
        self.invalidate_file_data_cache_entries(&file_cache_invalidation_targets)
            .await?;
        self.invalidate_file_path_cache_entries(&file_cache_invalidation_targets)
            .await?;
        self.refresh_file_data_for_versions(file_cache_refresh_targets)
            .await?;
        if should_invalidate_installed_plugins_cache_for_statements(&parsed_statements) {
            self.invalidate_installed_plugins_cache()?;
        }
        self.emit_state_commit_stream_changes(state_commit_stream_changes);

        Ok(result)
    }
    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        crate::checkpoint::create_checkpoint(self).await
    }

    /// Exports a portable snapshot as SQLite3 file bytes written via chunk stream.
    pub async fn export_snapshot(
        &self,
        writer: &mut dyn crate::SnapshotChunkWriter,
    ) -> Result<(), LixError> {
        self.backend.export_snapshot(writer).await
    }

    pub async fn restore_from_snapshot(
        &self,
        reader: &mut dyn crate::SnapshotChunkReader,
    ) -> Result<(), LixError> {
        self.backend.restore_from_snapshot(reader).await?;
        self.load_and_cache_active_version().await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }

    pub async fn materialization_plan(
        &self,
        req: &MaterializationRequest,
    ) -> Result<MaterializationPlan, LixError> {
        crate::materialization::materialization_plan(self.backend.as_ref(), req).await
    }

    pub async fn apply_materialization_plan(
        &self,
        plan: &MaterializationPlan,
    ) -> Result<MaterializationApplyReport, LixError> {
        crate::materialization::apply_materialization_plan(self.backend.as_ref(), plan).await
    }

    pub async fn materialize(
        &self,
        req: &MaterializationRequest,
    ) -> Result<MaterializationReport, LixError> {
        let plan = crate::materialization::materialization_plan(self.backend.as_ref(), req).await?;
        let apply =
            crate::materialization::apply_materialization_plan(self.backend.as_ref(), &plan)
                .await?;

        crate::plugin::runtime::materialize_file_data_with_plugins(
            self.backend.as_ref(),
            self.wasm_runtime.as_ref(),
            &plan,
        )
        .await?;

        Ok(MaterializationReport { plan, apply })
    }
}
