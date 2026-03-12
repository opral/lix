use crate::engine::{
    normalize_sql_execution_error_with_backend, reject_internal_table_writes, Engine,
    ExecuteOptions,
};
use crate::errors;
use crate::sql::ast::utils::bind_sql;
use crate::sql::ast::walk::object_name_matches;
use crate::sql::common::ast::lower_statement;
use crate::sql::execution::execute;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::shared_path;
use crate::sql::execution::shared_path::prepared_execution_mutates_public_surface_registry;
use crate::sql::execution::transaction_session::execute_public_sql;
use crate::sql::public::runtime::classify_public_execution_route_with_registry;
use crate::state::internal::inline_functions::inline_lix_functions_with_provider;
use crate::state::internal::script::extract_explicit_transaction_script_from_statements;
use crate::state::internal::statement_references_internal_state_vtable;
use crate::state::materialization::{
    MaterializationApplyReport, MaterializationPlan, MaterializationReport, MaterializationRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteResult, LixError, LixTransaction, QueryResult, Value};
use sqlparser::ast::{Expr, Function, Statement, Visit, Visitor};
use std::ops::ControlFlow;

impl Engine {
    #[doc(hidden)]
    pub async fn open_existing(&self) -> Result<(), LixError> {
        if !self.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        self.load_and_cache_active_version().await?;
        self.refresh_public_surface_registry().await?;
        Ok(())
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
        let mut state = self.public_sql_state.lock().await;
        execute_public_sql(
            self,
            &mut state,
            &self.public_sql_transaction_open,
            sql,
            params,
            options,
        )
        .await
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
        let allow_internal_sql = allow_internal_tables || self.access_to_internal();

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
        if should_use_plain_backend_read_route(&self.public_surface_registry(), &parsed_statements)?
        {
            return self
                .execute_plain_backend_read(sql, params, &parsed_statements)
                .await;
        }
        let prepared = shared_path::prepare_execution_with_backend(
            self,
            self.backend.as_ref(),
            &parsed_statements,
            params,
            &active_version_id,
            writer_key,
            allow_internal_sql,
            None,
            shared_path::PreparationPolicy {
                skip_side_effect_collection: false,
            },
        )
        .await?;
        let public_surface_registry_dirty =
            prepared_execution_mutates_public_surface_registry(&prepared)?;

        let execution =
            match shared_path::maybe_execute_public_write_with_backend(self, &prepared, writer_key)
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

        let _file_cache_refresh_targets = shared_path::derive_cache_targets(
            &prepared.plan,
            active_effects,
            effects_are_authoritative,
            execution.postprocess_file_cache_targets.clone(),
        )
        .file_cache_refresh_targets;

        let filesystem_payload_domain_changes = self
            .collect_live_filesystem_payload_domain_changes(
                &prepared.intent.pending_file_writes,
                &prepared.intent.pending_file_delete_targets,
                writer_key,
            )
            .await?;
        let filesystem_payload_domain_changes =
            crate::engine::dedupe_filesystem_payload_domain_changes(
                &filesystem_payload_domain_changes,
            );
        let payload_domain_changes_to_persist =
            if shared_path::public_write_filesystem_payload_changes_already_committed(&prepared) {
                Vec::new()
            } else if execution.plugin_changes_committed {
                crate::engine::dedupe_filesystem_payload_domain_changes(
                    &filesystem_payload_domain_changes,
                )
            } else {
                filesystem_payload_domain_changes.clone()
            };
        let should_run_binary_gc = crate::engine::should_run_binary_cas_gc(
            &prepared.plan.preprocess.mutations,
            &filesystem_payload_domain_changes,
        );

        if !shared_path::public_write_filesystem_payload_changes_already_committed(&prepared) {
            self.persist_pending_file_data_updates(&prepared.intent.pending_file_writes)
                .await?;
        }
        if !payload_domain_changes_to_persist.is_empty() {
            self.persist_filesystem_payload_domain_changes(&payload_domain_changes_to_persist)
                .await?;
        }
        if should_run_binary_gc {
            self.garbage_collect_unreachable_binary_cas().await?;
        }

        let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
        state_commit_stream_changes.extend(execution.state_commit_stream_changes);
        let should_emit_observe_tick = !state_commit_stream_changes.is_empty();

        if !effects_are_authoritative
            && prepared
                .plan
                .requirements
                .should_invalidate_installed_plugins_cache
        {
            self.invalidate_installed_plugins_cache()?;
        }
        if should_emit_observe_tick {
            self.append_observe_tick(options.writer_key.as_deref())
                .await?;
        }
        if public_surface_registry_dirty {
            self.refresh_public_surface_registry().await?;
        }
        self.emit_state_commit_stream_changes(state_commit_stream_changes);

        Ok(ExecuteResult {
            statements: vec![execution.public_result],
        })
    }

    async fn execute_plain_backend_read(
        &self,
        _sql: &str,
        params: &[Value],
        parsed_statements: &[Statement],
    ) -> Result<ExecuteResult, LixError> {
        let uses_runtime_functions =
            plain_backend_read_uses_runtime_functions(&parsed_statements[0]);
        let (statement, settings, sequence_start, functions) = if uses_runtime_functions {
            let (settings, sequence_start, functions) = self
                .prepare_runtime_functions_with_backend(self.backend.as_ref())
                .await?;
            let mut provider = functions.clone();
            (
                inline_lix_functions_with_provider(parsed_statements[0].clone(), &mut provider),
                Some(settings),
                Some(sequence_start),
                Some(functions),
            )
        } else {
            (parsed_statements[0].clone(), None, None, None)
        };
        let lowered = lower_statement(statement, self.backend.dialect())?;
        let bound = bind_sql(&lowered.to_string(), params, self.backend.dialect())?;
        match self.backend.execute(&bound.sql, &bound.params).await {
            Ok(result) => {
                if let (Some(settings), Some(sequence_start), Some(functions)) =
                    (settings, sequence_start, functions.as_ref())
                {
                    execute::persist_runtime_sequence(self, settings, sequence_start, functions)
                        .await?;
                }
                Ok(ExecuteResult {
                    statements: vec![result],
                })
            }
            Err(error) => Err(normalize_sql_execution_error_with_backend(
                self.backend.as_ref(),
                error,
                parsed_statements,
            )
            .await),
        }
    }

    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        self.ensure_no_open_public_sql_transaction("create_checkpoint")?;
        crate::state::checkpoint::create_checkpoint(self).await
    }

    pub async fn create_version(
        &self,
        options: crate::CreateVersionOptions,
    ) -> Result<crate::CreateVersionResult, LixError> {
        self.ensure_no_open_public_sql_transaction("create_version")?;
        crate::version::create_version(self, options).await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        self.ensure_no_open_public_sql_transaction("switch_version")?;
        crate::version::switch_version(self, version_id).await
    }

    /// Exports a portable snapshot as SQLite3 file bytes written via chunk stream.
    pub async fn export_snapshot(
        &self,
        writer: &mut dyn crate::SnapshotChunkWriter,
    ) -> Result<(), LixError> {
        self.ensure_no_open_public_sql_transaction("export_snapshot")?;
        self.backend.export_snapshot(writer).await
    }

    pub async fn restore_from_snapshot(
        &self,
        reader: &mut dyn crate::SnapshotChunkReader,
    ) -> Result<(), LixError> {
        self.backend.restore_from_snapshot(reader).await?;
        self.load_and_cache_active_version().await?;
        self.refresh_public_surface_registry().await?;
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
            self.wasm_runtime_ref(),
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

fn should_use_plain_backend_read_route(
    registry: &crate::sql::public::catalog::SurfaceRegistry,
    parsed_statements: &[Statement],
) -> Result<bool, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(false);
    }
    if !matches!(
        parsed_statements[0],
        Statement::Query(_) | Statement::Explain { .. }
    ) {
        return Ok(false);
    }
    if statement_references_internal_state_vtable(&parsed_statements[0]) {
        return Ok(false);
    }

    Ok(classify_public_execution_route_with_registry(registry, parsed_statements).is_none())
}

fn plain_backend_read_uses_runtime_functions(statement: &Statement) -> bool {
    struct Collector {
        matched: bool,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::Function(function) if is_runtime_function(function) => {
                    self.matched = true;
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }

    let mut collector = Collector { matched: false };
    let _ = statement.visit(&mut collector);
    collector.matched
}

fn is_runtime_function(function: &Function) -> bool {
    object_name_matches(&function.name, "lix_uuid_v7")
        || object_name_matches(&function.name, "lix_timestamp")
}
