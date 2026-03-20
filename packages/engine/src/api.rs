use crate::engine::{
    normalize_sql_execution_error_with_backend, reject_internal_table_writes,
    reject_public_create_table, Engine, ExecuteOptions,
};
use crate::errors;
use crate::init::init_backend;
use crate::sql::ast::utils::bind_sql;
use crate::sql::ast::walk::object_name_matches;
use crate::sql::common::ast::lower_statement;
use crate::sql::execution::execute;
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::post_commit_effects::apply_owned_execution_post_commit_effects;
use crate::sql::execution::shared_path;
use crate::sql::execution::shared_path::prepared_execution_mutates_public_surface_registry;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::execution::write_txn_plan::build_write_txn_plan;
use crate::sql::execution::write_txn_runner::run_write_txn_plan_with_backend;
use crate::sql::public::runtime::{
    classify_public_execution_route_with_registry, execute_prepared_public_read,
    finalize_prepared_public_read_result,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::internal::inline_functions::inline_lix_functions_with_provider;
use crate::state::internal::script::extract_explicit_transaction_script_from_statements;
use crate::state::internal::statement_references_internal_state_vtable;
use crate::state::internal::write_program::WriteProgram;
use crate::state::live_state::ensure_live_state_ready;
use crate::state::materialization::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildReport, LiveStateRebuildRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteResult, LixError, LixTransaction, QueryResult, Value};
use sqlparser::ast::{Expr, Function, Statement, Visit, Visitor};
use std::ops::ControlFlow;

impl Engine {
    pub(crate) fn build_observe_tick_insert_sql(&self, writer_key: Option<&str>) -> String {
        match writer_key {
            Some(writer_key) => format!(
                "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                 VALUES (CURRENT_TIMESTAMP, '{}')",
                escape_sql_string(writer_key)
            ),
            None => "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                      VALUES (CURRENT_TIMESTAMP, NULL)"
                .to_string(),
        }
    }

    #[doc(hidden)]
    pub async fn open_existing(&self) -> Result<(), LixError> {
        if !self.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        ensure_live_state_ready(self.backend.as_ref()).await?;
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

    pub(crate) async fn append_observe_tick_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        let mut program = WriteProgram::new();
        program.push_statement(self.build_observe_tick_insert_sql(writer_key), Vec::new());
        execute_write_program_with_transaction(transaction, program).await?;
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
        let allow_internal_sql = allow_internal_tables || self.access_to_internal();

        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        if !allow_internal_sql {
            reject_public_create_table(&parsed_statements)?;
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
        let public_surface_registry = self.public_surface_registry();
        let prepared = shared_path::prepare_execution_with_backend(
            self,
            self.backend.as_ref(),
            None,
            None,
            None,
            &parsed_statements,
            params,
            &active_version_id,
            writer_key,
            allow_internal_sql,
            Some(&public_surface_registry),
            shared_path::PreparationPolicy {
                skip_side_effect_collection: false,
            },
        )
        .await?;
        let public_surface_registry_dirty =
            prepared_execution_mutates_public_surface_registry(&prepared)?;
        let direct_public_read = prepared
            .public_read
            .as_ref()
            .and_then(|prepared| prepared.direct_plan().map(|_| prepared));

        let write_txn_plan = build_write_txn_plan(&prepared, writer_key);
        if let Some(public_read) = direct_public_read {
            let result = execute_prepared_public_read(self.backend.as_ref(), public_read).await?;
            return Ok(ExecuteResult {
                statements: vec![result],
            });
        }

        let (execution, write_owned_transaction_committed) = if let Some(plan) = write_txn_plan {
            match run_write_txn_plan_with_backend(self, &plan, None).await {
                Ok(execution) => (execution, true),
                Err(error) => return Err(error),
            }
        } else {
            match execute::execute_plan_sql(
                self,
                &prepared.plan,
                prepared.plan.requirements.should_refresh_file_cache,
                &prepared.functions,
                writer_key,
            )
            .await
            .map_err(LixError::from)
            {
                Ok(execution) => (execution, false),
                Err(error) => {
                    return Err(normalize_sql_execution_error_with_backend(
                        self.backend.as_ref(),
                        error,
                        &parsed_statements,
                    )
                    .await)
                }
            }
        };

        apply_owned_execution_post_commit_effects(
            self,
            &prepared,
            &execution,
            writer_key,
            write_owned_transaction_committed,
            public_surface_registry_dirty,
        )
        .await?;

        let public_result = if let Some(public_read) = prepared.public_read.as_ref() {
            finalize_prepared_public_read_result(execution.public_result, public_read)
        } else {
            execution.public_result
        };

        Ok(ExecuteResult {
            statements: vec![public_result],
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
                .prepare_runtime_functions_with_backend(self.backend.as_ref(), false)
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
        crate::state::checkpoint::create_checkpoint(self).await
    }

    pub async fn undo(&self) -> Result<crate::UndoResult, LixError> {
        crate::undo_redo::undo(self).await
    }

    pub async fn undo_with_options(
        &self,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        crate::undo_redo::undo_with_options(self, options).await
    }

    pub async fn redo(&self) -> Result<crate::RedoResult, LixError> {
        crate::undo_redo::redo(self).await
    }

    pub async fn redo_with_options(
        &self,
        options: crate::RedoOptions,
    ) -> Result<crate::RedoResult, LixError> {
        crate::undo_redo::redo_with_options(self, options).await
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

    /// Exports a portable image as SQLite3 file bytes written via chunk stream.
    pub async fn export_image(
        &self,
        writer: &mut dyn crate::ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.backend.export_image(writer).await
    }

    pub async fn restore_from_image(
        &self,
        reader: &mut dyn crate::ImageChunkReader,
    ) -> Result<(), LixError> {
        self.backend.restore_from_image(reader).await?;
        init_backend(self.backend.as_ref()).await?;
        ensure_live_state_ready(self.backend.as_ref()).await?;
        self.load_and_cache_active_version().await?;
        self.refresh_public_surface_registry().await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }

    pub async fn live_state_rebuild_plan(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError> {
        crate::state::materialization::live_state_rebuild_plan(self.backend.as_ref(), req).await
    }

    pub async fn apply_live_state_rebuild_plan(
        &self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError> {
        crate::state::materialization::apply_live_state_rebuild_plan(self.backend.as_ref(), plan)
            .await
    }

    pub async fn rebuild_live_state(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildReport, LixError> {
        let plan =
            crate::state::materialization::live_state_rebuild_plan(self.backend.as_ref(), req)
                .await?;
        let apply = crate::state::materialization::apply_live_state_rebuild_plan(
            self.backend.as_ref(),
            &plan,
        )
        .await?;

        if let Err(error) = crate::plugin::runtime::materialize_file_data_with_plugins(
            self.backend.as_ref(),
            self.wasm_runtime_ref(),
            &plan,
        )
        .await
        {
            let _ = crate::state::live_state::mark_live_state_mode_with_backend(
                self.backend.as_ref(),
                crate::state::live_state::LiveStateMode::NeedsRebuild,
            )
            .await;
            return Err(error);
        }

        Ok(LiveStateRebuildReport { plan, apply })
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
