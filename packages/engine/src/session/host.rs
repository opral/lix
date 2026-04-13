use async_trait::async_trait;

use std::sync::Arc;

use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::contracts::CompiledSchemaCache;
use crate::contracts::{
    clone_boxed_function_provider, DynFunctionProvider, FunctionBindings, SharedFunctionProvider,
};
use crate::image::ImageChunkWriter;
use crate::session::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::sql::SqlCompilerSeed;
use crate::streams::{StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter};
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionBeginMode};

#[async_trait(?Send)]
pub(crate) trait SessionHost: Send + Sync {
    async fn ensure_initialized(&self) -> Result<(), LixError>;

    fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync>;

    fn access_to_internal(&self) -> bool;

    async fn begin_write_unit(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError>;

    async fn begin_read_unit(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError>;

    fn public_surface_registry(&self) -> SurfaceRegistry;

    fn install_public_surface_registry(&self, registry: SurfaceRegistry);

    async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError>;

    async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError>;

    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry;

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache;

    async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    >;

    fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream;

    fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>);

    fn invalidate_deterministic_settings_cache(&self);

    fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError>;
}

#[derive(Clone, Copy)]
pub(crate) struct SessionExecutionContext<'a> {
    host: &'a dyn SessionHost,
}

impl<'a> SessionExecutionContext<'a> {
    pub(crate) fn new(host: &'a dyn SessionHost) -> Self {
        Self { host }
    }

    pub(crate) fn session_host(&self) -> &'a dyn SessionHost {
        self.host
    }
}

pub(crate) fn sql_compiler_seed_from_host<'a>(
    host: &'a dyn SessionHost,
    functions: &'a DynFunctionProvider,
    surface_registry: &'a SurfaceRegistry,
) -> SqlCompilerSeed<'a> {
    SqlCompilerSeed {
        dialect: host.backend().dialect(),
        functions: clone_boxed_function_provider(functions),
        surface_registry,
    }
}

pub(crate) async fn prepare_function_bindings_with_host(
    host: &dyn SessionHost,
    backend: &dyn LixBackend,
) -> Result<FunctionBindings, LixError> {
    let (settings, functions) = host.prepare_runtime_functions_with_backend(backend).await?;
    Ok(FunctionBindings::from_prepared_parts(
        settings.enabled,
        &functions,
    ))
}
