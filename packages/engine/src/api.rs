use crate::backend::{ImageChunkReader, ImageChunkWriter};
use crate::engine::Engine;
use crate::errors;
use crate::live_state::{
    mark_mode_with_backend, LiveStateApplyReport, LiveStateMode, LiveStateRebuildPlan,
    LiveStateRebuildReport, LiveStateRebuildRequest, ProjectionStatus,
};
use crate::LixError;

impl Engine {
    #[doc(hidden)]
    pub async fn open_existing(&self) -> Result<(), LixError> {
        if !self.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        self.refresh_public_surface_registry().await?;
        Ok(())
    }

    /// Exports a portable image as SQLite3 file bytes written via chunk stream.
    pub async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        self.backend().export_image(writer).await
    }

    pub async fn restore_from_image(
        &self,
        reader: &mut dyn ImageChunkReader,
    ) -> Result<(), LixError> {
        self.backend().restore_from_image(reader).await?;
        self.clear_public_surface_registry();
        self.refresh_public_surface_registry().await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }

    pub async fn live_state_projection_status(&self) -> Result<ProjectionStatus, LixError> {
        crate::live_state::projection_status(self.backend().as_ref()).await
    }

    pub async fn live_state_rebuild_plan(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError> {
        crate::live_state::rebuild_plan(self.backend().as_ref(), req).await
    }

    pub async fn apply_live_state_rebuild_plan(
        &self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError> {
        crate::live_state::apply_rebuild_plan(self.backend().as_ref(), plan).await
    }

    pub async fn rebuild_live_state(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildReport, LixError> {
        let plan = crate::live_state::rebuild_plan(self.backend().as_ref(), req).await?;
        let apply = crate::live_state::apply_rebuild_plan(self.backend().as_ref(), &plan).await?;

        if let Err(error) = crate::filesystem_materialization::materialize_file_data_with_plugins(
            self.backend().as_ref(),
            self.runtime().as_ref(),
            &plan,
        )
        .await
        {
            let _ =
                mark_mode_with_backend(self.backend().as_ref(), LiveStateMode::NeedsRebuild).await;
            return Err(error);
        }

        Ok(LiveStateRebuildReport { plan, apply })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_existing_allows_stale_live_state_and_reports_projection_status() {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime should build");
                runtime.block_on(async {
                    let (backend, engine, _session) = crate::test_support::boot_test_engine()
                        .await
                        .expect("test engine should boot");
                    crate::live_state::mark_mode_with_backend(
                        &backend,
                        LiveStateMode::NeedsRebuild,
                    )
                    .await
                    .expect("marking live_state stale should succeed");

                    engine
                        .open_existing()
                        .await
                        .expect("open_existing should not fail just because live_state is stale");

                    let status = engine
                        .live_state_projection_status()
                        .await
                        .expect("projection status should load");
                    assert_eq!(status.projections.len(), 1);
                    assert_eq!(
                        status.projections[0].projection,
                        crate::live_state::DerivedProjectionId::LiveState
                    );
                    assert_eq!(
                        status.projections[0].mode,
                        crate::live_state::ProjectionReplayMode::NeedsRebuild
                    );
                });
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should not panic");
    }
}
