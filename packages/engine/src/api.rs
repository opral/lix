use super::sql::execution::{apply_effects_post_commit, apply_effects_tx, run, shared_path};
use super::sql::planning::parse::parse_sql;
use super::sql::planning::script::extract_explicit_transaction_script_from_statements;
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
        self.execute_impl_sql(sql, params, options, false).await
    }

    pub(crate) async fn execute_internal(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        self.execute_impl_sql(sql, params, options, true).await
    }

    pub(crate) async fn execute_impl_sql(
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

        let active_version_id = self.active_version_id.read().unwrap().clone();
        let writer_key = options.writer_key.as_deref();
        let prepared = shared_path::prepare_execution_with_backend(
            self,
            self.backend.as_ref(),
            &parsed_statements,
            params,
            &active_version_id,
            writer_key,
            shared_path::PreparationPolicy {
                allow_plugin_cache: true,
                detect_plugin_file_changes: true,
                skip_side_effect_collection: false,
            },
        )
        .await?;

        let execution = run::execute_plan_sql(
            self,
            &prepared.plan,
            &prepared.detected_file_domain_changes,
            prepared.plan.requirements.should_refresh_file_cache,
            &prepared.functions,
            writer_key,
        )
        .await
        .map_err(LixError::from)?;

        run::persist_runtime_sequence(
            self,
            prepared.settings,
            prepared.sequence_start,
            &prepared.functions,
        )
        .await?;

        if let Some(version_id) = &prepared.plan.effects.next_active_version_id {
            self.set_active_version_id(version_id.clone());
        }

        let cache_targets = shared_path::derive_cache_targets(
            &prepared.plan,
            execution.postprocess_file_cache_targets.clone(),
            &prepared.pending_file_delete_targets,
        );

        apply_effects_tx::apply_sql_backed_effects(
            self,
            &prepared.plan.preprocess.mutations,
            &prepared.pending_file_writes,
            &prepared.detected_file_domain_changes,
            &prepared.untracked_filesystem_update_domain_changes,
            execution.plugin_changes_committed,
            &cache_targets.file_cache_invalidation_targets,
        )
        .await?;

        let mut state_commit_stream_changes = prepared.plan.effects.state_commit_stream_changes;
        state_commit_stream_changes.extend(execution.state_commit_stream_changes);

        apply_effects_post_commit::apply_runtime_post_commit_effects(
            self,
            cache_targets.file_cache_refresh_targets,
            prepared
                .plan
                .requirements
                .should_invalidate_installed_plugins_cache,
            state_commit_stream_changes,
        )
        .await?;

        Ok(execution.result)
    }

    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        crate::checkpoint::create_checkpoint(self).await
    }

    pub async fn create_version(
        &self,
        options: crate::CreateVersionOptions,
    ) -> Result<crate::CreateVersionResult, LixError> {
        crate::version::create_version(self, options).await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        crate::version::switch_version(self, version_id).await
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
