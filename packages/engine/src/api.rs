use crate::backend::program::WriteProgram;
use crate::backend::program_runner::execute_write_program_with_transaction;
use crate::engine::Engine;
use crate::errors;
use crate::live_state::{
    mark_mode_with_backend, require_ready, LiveStateApplyReport, LiveStateMode,
    LiveStateRebuildPlan, LiveStateRebuildReport, LiveStateRebuildRequest,
};
use crate::sql_support::text::escape_sql_string;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

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
        require_ready(self.backend.as_ref()).await?;
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
        transaction: &mut dyn LixBackendTransaction,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        let mut program = WriteProgram::new();
        program.push_statement(self.build_observe_tick_insert_sql(writer_key), Vec::new());
        execute_write_program_with_transaction(transaction, program).await?;
        Ok(())
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
        require_ready(self.backend.as_ref()).await?;
        self.refresh_public_surface_registry().await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }

    pub async fn live_state_rebuild_plan(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError> {
        crate::live_state::rebuild_plan(self.backend.as_ref(), req).await
    }

    pub async fn apply_live_state_rebuild_plan(
        &self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError> {
        crate::live_state::apply_rebuild_plan(self.backend.as_ref(), plan).await
    }

    pub async fn rebuild_live_state(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildReport, LixError> {
        let plan = crate::live_state::rebuild_plan(self.backend.as_ref(), req).await?;
        let apply = crate::live_state::apply_rebuild_plan(self.backend.as_ref(), &plan).await?;

        if let Err(error) = crate::plugin::runtime::materialize_file_data_with_plugins(
            self.backend.as_ref(),
            self.wasm_runtime_ref(),
            &plan,
        )
        .await
        {
            let _ =
                mark_mode_with_backend(self.backend.as_ref(), LiveStateMode::NeedsRebuild).await;
            return Err(error);
        }

        Ok(LiveStateRebuildReport { plan, apply })
    }
}
