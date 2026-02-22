use super::super::*;

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
        self.execute_v2(sql, params, options, allow_internal_tables)
            .await
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
