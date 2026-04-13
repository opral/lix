use std::sync::Arc;

use async_trait::async_trait;

use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::contracts::CompiledSchemaCache;
use crate::contracts::{DynFunctionProvider, FunctionRuntimeState, SharedFunctionProvider};
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

#[async_trait(?Send)]
pub(crate) trait WriteExecutionServices: crate::transaction::WriteExecutionHost {
    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry;

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache;

    fn sql_compiler_seed<'a>(
        &'a self,
        functions: &'a DynFunctionProvider,
        surface_registry: &'a SurfaceRegistry,
    ) -> SqlCompilerSeed<'a>;

    async fn prepare_function_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<FunctionRuntimeState, LixError>;
}
