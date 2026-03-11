use super::*;
use crate::errors;
use crate::runtime_post_commit;
use crate::runtime_sql_effects;
use crate::sql::execution::execute;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path;
use crate::state::internal::script::extract_explicit_transaction_script_from_statements;

impl Engine {
    pub async fn open(&self) -> Result<(), LixError> {
        if !self.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        self.load_and_cache_active_version().await?;
        Ok(())
    }

    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.wasm_runtime.clone()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) async fn execute_backend_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.backend.execute(sql, params).await
    }

    pub(crate) async fn append_observe_tick(
        &self,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        match writer_key {
            Some(writer_key) => {
                self.backend
                    .execute(
                        "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                         VALUES (CURRENT_TIMESTAMP, $1)",
                        &[Value::Text(writer_key.to_string())],
                    )
                    .await?;
            }
            None => {
                self.backend
                    .execute(
                        "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                         VALUES (CURRENT_TIMESTAMP, NULL)",
                        &[],
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn append_observe_tick_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        match writer_key {
            Some(writer_key) => {
                transaction
                    .execute(
                        "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                         VALUES (CURRENT_TIMESTAMP, $1)",
                        &[Value::Text(writer_key.to_string())],
                    )
                    .await?;
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                         VALUES (CURRENT_TIMESTAMP, NULL)",
                        &[],
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.state_commit_stream_bus.emit(changes);
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
        *guard = None;
        let mut component_guard = self.plugin_component_cache.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin component cache lock poisoned".to_string(),
        })?;
        component_guard.clear();
        Ok(())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_impl_sql(sql, params, options, false).await
    }

    pub(crate) async fn execute_internal(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_impl_sql(sql, params, options, true).await
    }

    pub(crate) async fn execute_impl_sql(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<ExecuteResult, LixError> {
        let allow_internal_sql = allow_internal_tables || self.access_to_internal;

        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        if !allow_internal_sql {
            reject_internal_table_writes(&parsed_statements)?;
        }
        if let Some(statements) =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
        {
            return self
                .execute_transaction_script_with_options(
                    statements,
                    params,
                    options,
                    allow_internal_sql,
                )
                .await;
        }
        if !allow_internal_sql && contains_transaction_control_statement(&parsed_statements) {
            return Err(errors::transaction_control_statement_denied_error());
        }
        if parsed_statements.len() > 1 {
            return self
                .execute_statement_script_with_options(
                    parsed_statements,
                    params,
                    &options,
                    allow_internal_sql,
                )
                .await;
        }

        let active_version_id = if allow_internal_tables {
            self.require_active_version_id()
                .unwrap_or_else(|_| GLOBAL_VERSION_ID.to_string())
        } else {
            self.require_active_version_id()?
        };
        let writer_key = options.writer_key.as_deref();
        let prepared = shared_path::prepare_execution_with_backend(
            self,
            self.backend.as_ref(),
            &parsed_statements,
            params,
            &active_version_id,
            writer_key,
            allow_internal_sql,
            shared_path::PreparationPolicy {
                skip_side_effect_collection: false,
            },
        )
        .await?;

        let execution =
            match shared_path::maybe_execute_sql2_write_with_backend(self, &prepared, writer_key)
                .await
            {
                Ok(Some(execution)) => execution,
                Ok(None) => match execute::execute_plan_sql(
                    self,
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
                        return Err(normalize_sql_execution_error_with_backend(
                            self.backend.as_ref(),
                            error,
                            &parsed_statements,
                        )
                        .await)
                    }
                },
                Err(error) => return Err(error),
            };

        execute::persist_runtime_sequence(
            self,
            prepared.settings,
            prepared.sequence_start,
            &prepared.functions,
        )
        .await?;

        let active_effects = execution
            .plan_effects_override
            .as_ref()
            .unwrap_or(&prepared.plan.effects);
        let effects_are_authoritative = execution.plan_effects_override.is_some();

        if let Some(version_id) = &active_effects.next_active_version_id {
            self.set_active_version_id(version_id.clone());
        }

        let cache_targets = shared_path::derive_cache_targets(
            &prepared.plan,
            active_effects,
            effects_are_authoritative,
            execution.postprocess_file_cache_targets.clone(),
        );

        runtime_sql_effects::apply_sql_backed_effects(
            self,
            &prepared.plan.preprocess.mutations,
            &prepared.intent.pending_file_writes,
            &prepared.intent.pending_file_delete_targets,
            execution.plugin_changes_committed,
            shared_path::sql2_filesystem_payload_changes_already_committed(&prepared),
            writer_key,
        )
        .await?;

        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes);
        let should_emit_observe_tick = !state_commit_stream_changes.is_empty();

        runtime_post_commit::apply_runtime_post_commit_effects(
            self,
            cache_targets.file_cache_refresh_targets,
            if effects_are_authoritative {
                false
            } else {
                prepared
                    .plan
                    .requirements
                    .should_invalidate_installed_plugins_cache
            },
            should_emit_observe_tick,
            options.writer_key.as_deref(),
            state_commit_stream_changes,
        )
        .await?;

        Ok(ExecuteResult {
            statements: vec![execution.public_result],
        })
    }

    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        crate::state::checkpoint::create_checkpoint(self).await
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
        crate::state::materialization::materialization_plan(self.backend.as_ref(), req).await
    }

    pub async fn apply_materialization_plan(
        &self,
        plan: &MaterializationPlan,
    ) -> Result<MaterializationApplyReport, LixError> {
        crate::state::materialization::apply_materialization_plan(self.backend.as_ref(), plan).await
    }

    pub async fn materialize(
        &self,
        req: &MaterializationRequest,
    ) -> Result<MaterializationReport, LixError> {
        let plan =
            crate::state::materialization::materialization_plan(self.backend.as_ref(), req).await?;
        let apply =
            crate::state::materialization::apply_materialization_plan(self.backend.as_ref(), &plan)
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

fn contains_transaction_control_statement(statements: &[Statement]) -> bool {
    statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    })
}
