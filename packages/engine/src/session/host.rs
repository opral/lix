use async_trait::async_trait;

use std::sync::{Arc, OnceLock};

use crate::backend::TransactionBeginMode;
use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::functions::{
    clone_boxed_function_provider, DynFunctionProvider, FunctionBindings, LixFunctionProvider,
};
use crate::image::ImageChunkWriter;
use crate::schema::CompiledSchemaCache;
use crate::sql::SqlCompilerSeed;
use crate::streams::{StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter};
use crate::{LixBackend, LixBackendTransaction, LixError};

use super::Session;

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
    ) -> Result<DynFunctionProvider, LixError>;

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
    let functions = host.prepare_runtime_functions_with_backend(backend).await?;
    Ok(FunctionBindings::from_prepared_parts(
        functions.deterministic_sequence_enabled(),
        &functions,
    ))
}

pub(crate) async fn open_workspace_session(
    session_host: Arc<dyn SessionHost>,
) -> Result<Session, LixError> {
    Session::open_workspace(session_host).await
}

pub(crate) async fn opened_workspace_session(
    session_host: &Arc<dyn SessionHost>,
    workspace_session: &OnceLock<Arc<Session>>,
) -> Result<Arc<Session>, LixError> {
    if let Some(session) = workspace_session.get() {
        return Ok(Arc::clone(session));
    }

    let session = Arc::new(open_workspace_session(Arc::clone(session_host)).await?);
    let _ = workspace_session.set(Arc::clone(&session));
    Ok(workspace_session.get().map(Arc::clone).unwrap_or(session))
}

pub(crate) fn require_workspace_session<'a>(
    workspace_session: &'a OnceLock<Arc<Session>>,
) -> Result<&'a Arc<Session>, LixError> {
    workspace_session
        .get()
        .ok_or_else(crate::common::not_initialized_error)
}
