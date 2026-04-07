use std::sync::Arc;

use crate::contracts::functions::{clone_boxed_function_provider, SharedFunctionProvider};
use crate::contracts::projection::ProjectionRegistry;
use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::CompiledSchemaCache;
use crate::engine::Engine;
use crate::common::errors;
use crate::execution_runtime::ExecutionRuntimeState;
use crate::image::ImageChunkWriter;
use crate::runtime::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::runtime::streams::{
    StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::sql::prepare::SqlPreparationSeed;
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionMode};
use async_trait::async_trait;

#[async_trait(?Send)]
pub(crate) trait WriteExecutionCollaborators {
    fn projection_registry(&self) -> &ProjectionRegistry;

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache;

    fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlPreparationSeed<'a>;

    async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<ExecutionRuntimeState, LixError>;
}

pub(crate) struct SessionCollaborators {
    engine: Arc<Engine>,
}

impl SessionCollaborators {
    pub(crate) fn new(engine: Arc<Engine>) -> Arc<Self> {
        Arc::new(Self { engine })
    }

    pub(crate) async fn ensure_initialized(&self) -> Result<(), LixError> {
        if !self.engine.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        Ok(())
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.engine.backend()
    }

    pub(crate) fn access_to_internal(&self) -> bool {
        self.engine.runtime().access_to_internal()
    }

    pub(crate) async fn begin_write_unit(
        &self,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.engine.runtime().begin_write_unit().await
    }

    pub(crate) async fn begin_read_unit(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.engine.runtime().begin_read_unit(mode).await
    }

    pub(crate) fn public_surface_registry(&self) -> SurfaceRegistry {
        self.engine.public_surface_registry()
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.engine.install_public_surface_registry(registry);
    }

    pub(crate) async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.engine.load_public_surface_registry().await
    }

    pub(crate) async fn export_image(
        &self,
        writer: &mut dyn ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.backend().export_image(writer).await
    }

    pub(crate) fn projection_registry(&self) -> &ProjectionRegistry {
        self.engine.projection_registry().as_ref()
    }

    pub(crate) fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.engine.runtime().schema_cache()
    }

    pub(crate) fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlPreparationSeed<'a> {
        SqlPreparationSeed {
            dialect: self.backend().dialect(),
            functions: clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    pub(crate) async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<ExecutionRuntimeState, LixError> {
        let (settings, functions) = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(ExecutionRuntimeState::from_prepared_parts(
            settings, functions,
        ))
    }

    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        self.engine
            .prepare_runtime_functions_with_backend(backend)
            .await
    }

    pub(crate) async fn ensure_version_exists(&self, version_id: &str) -> Result<(), LixError> {
        crate::version::context::ensure_version_exists_with_backend(
            self.backend().as_ref(),
            version_id,
        )
        .await
    }

    pub(crate) async fn create_version_in_session(
        &self,
        session: &crate::Session,
        options: crate::CreateVersionOptions,
    ) -> Result<crate::CreateVersionResult, LixError> {
        crate::version::create_version_in_session(session, options).await
    }

    pub(crate) async fn merge_version_in_session(
        &self,
        session: &crate::Session,
        options: crate::MergeVersionOptions,
    ) -> Result<crate::MergeVersionResult, LixError> {
        crate::version::merge_version_in_session(session, options).await
    }

    pub(crate) async fn undo_with_options_in_session(
        &self,
        session: &crate::Session,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        crate::version::undo_redo::undo_with_options_in_session(session, options).await
    }

    pub(crate) async fn redo_with_options_in_session(
        &self,
        session: &crate::Session,
        options: crate::RedoOptions,
    ) -> Result<crate::RedoResult, LixError> {
        crate::version::undo_redo::redo_with_options_in_session(session, options).await
    }

    pub(crate) fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.engine.state_commit_stream(filter)
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.engine
            .runtime()
            .emit_state_commit_stream_changes(changes);
    }

    pub(crate) fn invalidate_deterministic_settings_cache(&self) {
        self.engine
            .runtime()
            .invalidate_deterministic_settings_cache();
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.engine.invalidate_installed_plugins_cache()
    }
}

#[async_trait(?Send)]
impl WriteExecutionCollaborators for SessionCollaborators {
    fn projection_registry(&self) -> &ProjectionRegistry {
        SessionCollaborators::projection_registry(self)
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        SessionCollaborators::compiled_schema_cache(self)
    }

    fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlPreparationSeed<'a> {
        SessionCollaborators::sql_preparation_seed(self, functions, surface_registry)
    }

    async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<ExecutionRuntimeState, LixError> {
        SessionCollaborators::prepare_execution_runtime_state(self, backend).await
    }
}

#[async_trait(?Send)]
impl WriteExecutionCollaborators for Engine {
    fn projection_registry(&self) -> &ProjectionRegistry {
        self.projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.runtime().schema_cache()
    }

    fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlPreparationSeed<'a> {
        SqlPreparationSeed {
            dialect: self.backend().dialect(),
            functions: clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<ExecutionRuntimeState, LixError> {
        let (settings, functions) = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(ExecutionRuntimeState::from_prepared_parts(
            settings, functions,
        ))
    }
}
