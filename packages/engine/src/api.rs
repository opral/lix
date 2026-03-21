use crate::engine::{
    reject_internal_table_writes, reject_public_create_table, Engine, ExecuteOptions,
};
use crate::errors;
use crate::init::init_backend;
use crate::sql::execution::execution_program::{
    execute_execution_program_with_backend, ExecutionProgram,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::internal::script::extract_explicit_transaction_script_from_statements;
use crate::state::internal::write_program::WriteProgram;
use crate::state::live_state::ensure_live_state_ready;
use crate::state::materialization::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildReport, LiveStateRebuildRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteResult, LixError, LixTransaction, QueryResult, Value};
use sqlparser::ast::Statement;

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
        let explicit_transaction_script =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
                .is_some();
        if !allow_internal_sql
            && contains_transaction_control_statement(&parsed_statements)
            && !explicit_transaction_script
        {
            return Err(errors::transaction_control_statement_denied_error());
        }

        let active_version_id = if allow_internal_tables {
            self.require_active_version_id()
                .unwrap_or_else(|_| GLOBAL_VERSION_ID.to_string())
        } else {
            self.require_active_version_id()?
        };
        let program = ExecutionProgram::compile(parsed_statements, params, self.backend.dialect())?;
        execute_execution_program_with_backend(
            self,
            &program,
            options,
            active_version_id,
            allow_internal_sql,
        )
        .await
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

    pub async fn merge_version(
        &self,
        options: crate::MergeVersionOptions,
    ) -> Result<crate::MergeVersionResult, LixError> {
        crate::version::merge_version(self, options).await
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
