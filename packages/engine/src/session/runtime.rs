use std::sync::Arc;

use async_trait::async_trait;

use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::contracts::CompiledSchemaCache;
use crate::contracts::{
    clone_boxed_function_provider, DynFunctionProvider, FunctionRuntimeState,
    SharedFunctionProvider,
};
use crate::image::ImageChunkWriter;
use crate::session::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::session::host::{SessionHost, WriteExecutionServices};
use crate::sql::SqlCompilerSeed;
use crate::streams::{StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter};
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionBeginMode};

pub(crate) struct SessionRuntime {
    services: Arc<dyn SessionHost>,
}

impl SessionRuntime {
    pub(crate) fn new(services: Arc<dyn SessionHost>) -> Arc<Self> {
        Arc::new(Self { services })
    }

    pub(crate) async fn ensure_initialized(&self) -> Result<(), LixError> {
        self.services.ensure_initialized().await
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.services.backend()
    }

    pub(crate) fn access_to_internal(&self) -> bool {
        self.services.access_to_internal()
    }

    pub(crate) async fn begin_write_unit(
        &self,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.services.begin_write_unit().await
    }

    pub(crate) async fn begin_read_unit(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.services.begin_read_unit(mode).await
    }

    pub(crate) fn public_surface_registry(&self) -> SurfaceRegistry {
        self.services.public_surface_registry()
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.services.install_public_surface_registry(registry);
    }

    pub(crate) async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.services.load_public_surface_registry().await
    }

    pub(crate) async fn export_image(
        &self,
        writer: &mut dyn ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.services.export_image(writer).await
    }

    pub(crate) fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.services.catalog_projection_registry()
    }

    pub(crate) fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.services.compiled_schema_cache()
    }

    pub(crate) fn sql_compiler_seed<'a>(
        &'a self,
        functions: &'a DynFunctionProvider,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlCompilerSeed<'a> {
        SqlCompilerSeed {
            dialect: self.backend().dialect(),
            functions: clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    pub(crate) async fn prepare_function_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<FunctionRuntimeState, LixError> {
        let (settings, functions) = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(FunctionRuntimeState::from_prepared_parts(
            settings.enabled,
            &functions,
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
        self.services
            .prepare_runtime_functions_with_backend(backend)
            .await
    }

    pub(crate) async fn ensure_version_exists(&self, version_id: &str) -> Result<(), LixError> {
        crate::session::version_ops::context::ensure_version_exists_with_backend(
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
        crate::session::version_ops::create_version_in_session(session, options).await
    }

    pub(crate) async fn merge_version_in_session(
        &self,
        session: &crate::Session,
        options: crate::MergeVersionOptions,
    ) -> Result<crate::MergeVersionResult, LixError> {
        crate::session::version_ops::merge_version_in_session(session, options).await
    }

    pub(crate) async fn undo_with_options_in_session(
        &self,
        session: &crate::Session,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        crate::session::version_ops::undo_redo::undo_with_options_in_session(session, options).await
    }

    pub(crate) async fn redo_with_options_in_session(
        &self,
        session: &crate::Session,
        options: crate::RedoOptions,
    ) -> Result<crate::RedoResult, LixError> {
        crate::session::version_ops::undo_redo::redo_with_options_in_session(session, options).await
    }

    pub(crate) fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.services.state_commit_stream(filter)
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.services.emit_state_commit_stream_changes(changes);
    }

    pub(crate) fn invalidate_deterministic_settings_cache(&self) {
        self.services.invalidate_deterministic_settings_cache();
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.services.invalidate_installed_plugins_cache()
    }
}

#[async_trait(?Send)]
impl WriteExecutionServices for SessionRuntime {
    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        SessionRuntime::catalog_projection_registry(self)
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        SessionRuntime::compiled_schema_cache(self)
    }

    fn sql_compiler_seed<'a>(
        &'a self,
        functions: &'a DynFunctionProvider,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlCompilerSeed<'a> {
        SessionRuntime::sql_compiler_seed(self, functions, surface_registry)
    }

    async fn prepare_function_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<FunctionRuntimeState, LixError> {
        SessionRuntime::prepare_function_runtime_state(self, backend).await
    }
}
