use std::collections::BTreeSet;

use super::super::*;
use super::execution::{apply_effects_post_commit, apply_effects_tx, run};
use super::planning::derive_requirements::derive_plan_requirements;
use super::planning::parse::parse_sql;
use super::planning::plan::build_execution_plan;
use super::type_bridge::{to_sql_mutations, to_sql_update_validations};

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
        self.execute_impl_sql2(sql, params, options, allow_internal_tables)
            .await
    }

    pub(crate) async fn execute_impl_sql2(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<QueryResult, LixError> {
        if !allow_internal_tables && !self.access_to_internal {
            reject_internal_table_access(sql)?;
        }

        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        if let Some(statements) =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
        {
            return self
                .execute_transaction_script_with_options(statements, params, options)
                .await;
        }
        if parsed_statements.len() > 1 {
            return self
                .execute_statement_script_with_options(parsed_statements, params, &options)
                .await;
        }

        let requirements = derive_plan_requirements(&parsed_statements);
        let active_version_id = self.active_version_id.read().unwrap().clone();
        let writer_key = options.writer_key.as_deref();

        if requirements.read_only_query {
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
        )
        .await?;

        let CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        } = if requirements.read_only_query {
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

        let plan = build_execution_plan(
            self.backend.as_ref(),
            &self.cel_evaluator,
            parsed_statements.clone(),
            params,
            functions.clone(),
            &detected_file_domain_changes_by_statement,
            writer_key,
        )
        .await
        .map_err(LixError::from)?;
        let sql_mutations = to_sql_mutations(&plan.preprocess.mutations);
        let sql_update_validations = to_sql_update_validations(&plan.preprocess.update_validations);

        if !sql_mutations.is_empty() {
            validate_inserts(self.backend.as_ref(), &self.schema_cache, &sql_mutations).await?;
        }
        if !sql_update_validations.is_empty() {
            validate_updates(
                self.backend.as_ref(),
                &self.schema_cache,
                &sql_update_validations,
                params,
            )
            .await?;
        }

        let execution = run::execute_plan_sql(
            self,
            &plan,
            &detected_file_domain_changes,
            plan.requirements.should_refresh_file_cache,
            &functions,
            writer_key,
        )
        .await
        .map_err(LixError::from)?;

        run::persist_runtime_sequence(self, settings, sequence_start, &functions).await?;

        if let Some(version_id) = &plan.effects.next_active_version_id {
            self.set_active_version_id(version_id.clone());
        }

        let file_cache_refresh_targets = if plan.requirements.should_refresh_file_cache {
            let mut targets = direct_state_file_cache_refresh_targets(&sql_mutations);
            targets.extend(execution.postprocess_file_cache_targets);
            targets
        } else {
            BTreeSet::new()
        };
        let descriptor_cache_eviction_targets =
            file_descriptor_cache_eviction_targets(&sql_mutations);
        let mut file_cache_invalidation_targets = file_cache_refresh_targets.clone();
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets.clone());

        apply_effects_tx::apply_sql_backed_effects(
            self,
            &sql_mutations,
            &pending_file_writes,
            &pending_file_delete_targets,
            &detected_file_domain_changes,
            &untracked_filesystem_update_domain_changes,
            execution.plugin_changes_committed,
            &file_cache_invalidation_targets,
        )
        .await?;

        apply_effects_post_commit::apply_runtime_post_commit_effects(
            self,
            file_cache_refresh_targets,
            plan.requirements.should_invalidate_installed_plugins_cache,
            plan.effects.state_commit_stream_changes,
        )
        .await?;

        Ok(execution.result)
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
