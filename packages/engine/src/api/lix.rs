//! Public `Lix` shell types and workspace-session forwarding APIs.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::backend::{ImageChunkReader, ImageChunkWriter};
use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::contracts::CompiledSchemaCache;
use crate::live_state::{
    mark_mode_with_backend, LiveStateApplyReport, LiveStateMode, LiveStateRebuildPlan,
    LiveStateRebuildReport, LiveStateRebuildRequest, ProjectionStatus,
};
use crate::runtime::deterministic_mode::{
    global_deterministic_settings_storage_scope, parse_deterministic_settings_value,
    DeterministicSettings, RuntimeFunctionProvider,
};
use crate::runtime::functions::SharedFunctionProvider;
use crate::runtime::streams::{
    StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::runtime::wasm::WasmRuntime;
use crate::runtime::Runtime;
use crate::session::observe::observe_owned_session;
use crate::streams::StateCommitStream as PublicStateCommitStream;
use crate::{
    AdditionalSessionOptions, CreateCheckpointResult, CreateVersionOptions, CreateVersionResult,
    ExecuteOptions, ExecuteResult, LixBackend, LixBackendTransaction, LixError,
    MergeVersionOptions, MergeVersionResult, ObserveEventsOwned, ObserveQuery, RedoOptions,
    RedoResult, Session, SessionTransaction, TransactionMode, UndoOptions, UndoResult, Value,
};

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone)]
pub struct BootKeyValue {
    pub key: String,
    pub value: JsonValue,
    pub lixcol_global: Option<bool>,
    pub lixcol_untracked: Option<bool>,
}

pub struct LixConfig {
    pub backend: Box<dyn LixBackend + Send + Sync>,
    pub wasm_runtime: Arc<dyn WasmRuntime>,
    pub key_values: Vec<BootKeyValue>,
    access_to_internal: bool,
}

impl LixConfig {
    pub fn new(
        backend: Box<dyn LixBackend + Send + Sync>,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Self {
        Self {
            backend,
            wasm_runtime,
            key_values: Vec::new(),
            access_to_internal: false,
        }
    }

    #[doc(hidden)]
    pub fn with_access_to_internal(mut self, access_to_internal: bool) -> Self {
        self.access_to_internal = access_to_internal;
        self
    }
}

fn infer_boot_deterministic_settings(key_values: &[BootKeyValue]) -> Option<DeterministicSettings> {
    key_values.iter().rev().find_map(|key_value| {
        if key_value.key != DETERMINISTIC_MODE_KEY {
            return None;
        }
        let settings = parse_deterministic_settings_value(&key_value.value);
        settings.enabled.then_some(settings)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitResult {
    pub initialized: bool,
}

/// Repository handle and workspace session shell.
///
/// `Lix::open(...)` opens the workspace session eagerly so callers can start
/// executing immediately through `Lix`.
///
/// `Lix` is the simplest public entrypoint:
/// - it is the repository handle
/// - it forwards the workspace-session APIs directly
/// - it can open additional scoped [`Session`] values when work needs a
///   different active version or active-account selection
///
/// ```text
/// let lix = Lix::open(config).await?;
/// let rows = lix.execute("SELECT * FROM lix_state", &[]).await?;
/// ```
pub struct Lix {
    runtime: Arc<Runtime>,
    boot_key_values: Vec<BootKeyValue>,
    // `Lix` is the public shell around the workspace session. `Lix::open(...)`
    // populates this eagerly, while the hidden `Lix::boot(...)` path fills it after
    // initialize/open_existing for internal tests.
    workspace_session: OnceLock<Arc<Session>>,
}

impl Clone for Lix {
    fn clone(&self) -> Self {
        let cloned = Self {
            runtime: Arc::clone(&self.runtime),
            boot_key_values: self.boot_key_values.clone(),
            workspace_session: OnceLock::new(),
        };
        if let Some(session) = self.workspace_session.get() {
            let _ = cloned.workspace_session.set(Arc::clone(session));
        }
        cloned
    }
}

impl Lix {
    #[doc(hidden)]
    pub fn boot(config: LixConfig) -> Self {
        let boot_deterministic_settings = infer_boot_deterministic_settings(&config.key_values);
        let catalog_projection_registry =
            Arc::new(crate::catalog::builtin_catalog_projection_registry().clone());
        Self {
            runtime: Arc::new(Runtime::new(
                config.backend,
                config.wasm_runtime,
                config.access_to_internal,
                boot_deterministic_settings,
                crate::catalog::build_builtin_surface_registry(),
                catalog_projection_registry,
            )),
            boot_key_values: config.key_values,
            workspace_session: OnceLock::new(),
        }
    }

    pub(crate) fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.runtime.backend()
    }

    pub(crate) fn boot_key_values(&self) -> &[BootKeyValue] {
        &self.boot_key_values
    }

    pub(crate) fn public_surface_registry(&self) -> SurfaceRegistry {
        self.runtime.public_surface_registry()
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.runtime.install_public_surface_registry(registry);
    }

    pub(crate) fn clear_public_surface_registry(&self) {
        self.runtime.clear_public_surface_registry();
    }

    pub(crate) async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.runtime
            .load_public_surface_registry_from_backend()
            .await
    }

    pub(crate) async fn refresh_public_surface_registry(
        &self,
    ) -> Result<SurfaceRegistry, LixError> {
        let registry = self.load_public_surface_registry().await?;
        self.install_public_surface_registry(registry.clone());
        Ok(registry)
    }

    pub(crate) fn catalog_projection_registry(&self) -> &Arc<CatalogProjectionRegistry> {
        self.runtime.catalog_projection_registry()
    }

    pub(crate) fn try_mark_init_in_progress(&self) -> Result<(), LixError> {
        self.runtime.try_mark_init_in_progress()
    }

    pub(crate) fn deterministic_boot_pending(&self) -> bool {
        self.runtime.deterministic_boot_pending()
    }

    pub(crate) fn clear_deterministic_boot_pending(&self) {
        self.runtime.clear_deterministic_boot_pending();
    }

    pub(crate) fn mark_init_completed(&self) {
        self.runtime.mark_init_completed();
    }

    pub(crate) fn reset_init_state(&self) {
        self.runtime.reset_init_state();
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.runtime.invalidate_installed_plugins_cache()
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
        let storage_scope = global_deterministic_settings_storage_scope();
        self.runtime
            .prepare_runtime_functions_with_backend(backend, &storage_scope)
            .await
    }

    pub(crate) fn session_services(
        &self,
    ) -> Arc<dyn crate::session::collaborators::SessionServices> {
        Arc::new(LixRuntimeSessionServices::new(Arc::clone(&self.runtime)))
    }

    pub(crate) async fn open_workspace_session(&self) -> Result<Session, LixError> {
        Session::open_workspace(crate::session::collaborators::SessionCollaborators::new(
            self.session_services(),
        ))
        .await
    }

    pub(crate) async fn opened_workspace_session(&self) -> Result<Arc<Session>, LixError> {
        if let Some(session) = self.workspace_session.get() {
            return Ok(Arc::clone(session));
        }

        let session = Arc::new(self.open_workspace_session().await?);
        let _ = self.workspace_session.set(Arc::clone(&session));
        Ok(self
            .workspace_session
            .get()
            .map(Arc::clone)
            .unwrap_or(session))
    }

    pub(crate) fn workspace_session(&self) -> Result<&Arc<Session>, LixError> {
        self.workspace_session
            .get()
            .ok_or_else(crate::common::errors::not_initialized_error)
    }

    /// Opens the repository and eagerly initializes the workspace session used
    /// by the convenience methods on `Lix`.
    ///
    /// The simplest happy path is:
    ///
    /// ```text
    /// let lix = Lix::open(config).await?;
    /// let result = lix.execute("SELECT 1 + 1", &[]).await?;
    /// ```
    pub async fn open(config: LixConfig) -> Result<Self, LixError> {
        let lix = Self::boot(config);
        lix.open_existing().await?;
        Ok(lix)
    }

    pub async fn init(config: LixConfig) -> Result<InitResult, LixError> {
        let lix = Self::boot(config);
        let initialized = lix.initialize_if_needed().await?;
        Ok(InitResult { initialized })
    }

    #[doc(hidden)]
    pub async fn open_existing(&self) -> Result<(), LixError> {
        if crate::live_state::load_mode_with_backend(self.backend().as_ref()).await?
            == crate::live_state::LiveStateMode::Uninitialized
        {
            return Err(crate::common::errors::not_initialized_error());
        }
        self.refresh_public_surface_registry().await?;
        let _ = self.opened_workspace_session().await?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.opened_workspace_session()
            .await?
            .execute(sql, params)
            .await
    }

    pub async fn active_version_id(&self) -> Result<String, LixError> {
        Ok(self.opened_workspace_session().await?.active_version_id())
    }

    pub async fn active_account_ids(&self) -> Result<Vec<String>, LixError> {
        Ok(self.opened_workspace_session().await?.active_account_ids())
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.opened_workspace_session()
            .await?
            .execute_with_options(sql, params, options)
            .await
    }

    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEventsOwned, LixError> {
        observe_owned_session(Arc::clone(self.workspace_session()?), query)
    }

    /// Opens an additional scoped [`Session`].
    ///
    /// Any selector override omitted from `options` inherits the current
    /// workspace-session value from `Lix`.
    ///
    /// The returned [`Session`] is an additional scoped working context. This
    /// lets one repository handle operate against multiple active versions at
    /// the same time without duplicating `Lix` itself.
    ///
    /// ```text
    /// let lix = Lix::open(config).await?;
    ///
    /// let feature = lix
    ///     .open_additional_session(AdditionalSessionOptions {
    ///         active_version_id: Some("feature".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let release = lix
    ///     .open_additional_session(AdditionalSessionOptions {
    ///         active_version_id: Some("release".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let feature_rows = feature.execute("SELECT * FROM lix_state", &[]).await?;
    /// let release_rows = release.execute("SELECT * FROM lix_state", &[]).await?;
    /// # let _ = (feature_rows, release_rows);
    /// ```
    pub async fn open_additional_session(
        &self,
        options: AdditionalSessionOptions,
    ) -> Result<Session, LixError> {
        self.opened_workspace_session()
            .await?
            .open_additional_session(options)
            .await
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        self.opened_workspace_session()
            .await?
            .create_version(options)
            .await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        self.opened_workspace_session()
            .await?
            .switch_version(version_id)
            .await
    }

    pub async fn set_active_account_ids(
        &self,
        active_account_ids: Vec<String>,
    ) -> Result<(), LixError> {
        self.opened_workspace_session()
            .await?
            .set_active_account_ids(active_account_ids)
            .await
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionResult, LixError> {
        self.opened_workspace_session()
            .await?
            .merge_version(options)
            .await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointResult, LixError> {
        self.opened_workspace_session()
            .await?
            .create_checkpoint()
            .await
    }

    pub async fn undo(&self) -> Result<UndoResult, LixError> {
        self.opened_workspace_session().await?.undo().await
    }

    pub async fn undo_with_options(&self, options: UndoOptions) -> Result<UndoResult, LixError> {
        self.opened_workspace_session()
            .await?
            .undo_with_options(options)
            .await
    }

    pub async fn redo(&self) -> Result<RedoResult, LixError> {
        self.opened_workspace_session().await?.redo().await
    }

    pub async fn redo_with_options(&self, options: RedoOptions) -> Result<RedoResult, LixError> {
        self.opened_workspace_session()
            .await?
            .redo_with_options(options)
            .await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        self.opened_workspace_session()
            .await?
            .install_plugin(archive_bytes)
            .await
    }

    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        self.opened_workspace_session()
            .await?
            .register_schema(schema)
            .await
    }

    pub async fn export_image(&self) -> Result<Vec<u8>, LixError> {
        let mut writer = VecImageWriter::default();
        self.opened_workspace_session()
            .await?
            .export_image(&mut writer)
            .await?;
        Ok(writer.bytes)
    }

    #[doc(hidden)]
    pub async fn export_image_to_writer(
        &self,
        writer: &mut dyn ImageChunkWriter,
    ) -> Result<(), LixError> {
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
        if let Some(session) = self.workspace_session.get() {
            session.reload_workspace_state_from_backend().await?;
        } else {
            let _ = self.opened_workspace_session().await?;
        }
        Ok(())
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> PublicStateCommitStream {
        self.runtime().state_commit_stream(filter)
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

        if let Err(error) =
            crate::execution::write::filesystem::materialize::materialize_file_data_with_plugins(
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

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<SessionTransaction<'_>, LixError> {
        let _ = self.opened_workspace_session().await?;
        self.workspace_session()?
            .begin_transaction_with_options(options)
            .await
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut SessionTransaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let _ = self.opened_workspace_session().await?;
        self.workspace_session()?.transaction(options, f).await
    }
}

#[async_trait(?Send)]
impl crate::session::collaborators::WriteExecutionCollaborators for Lix {
    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.catalog_projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn crate::contracts::CompiledSchemaCache {
        self.runtime().schema_cache()
    }

    fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> crate::sql::prepare::SqlPreparationSeed<'a> {
        crate::sql::prepare::SqlPreparationSeed {
            dialect: self.backend().dialect(),
            functions: crate::contracts::clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<crate::runtime::execution_state::ExecutionRuntimeState, LixError> {
        let (settings, functions) = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(
            crate::runtime::execution_state::ExecutionRuntimeState::from_prepared_parts(
                settings, functions,
            ),
        )
    }
}

#[async_trait(?Send)]
impl crate::execution::write::WriteExecutionBindings for Lix {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        pending_view: Option<&dyn crate::contracts::PendingView>,
        public_read: &crate::contracts::PreparedPublicReadArtifact,
    ) -> Result<crate::QueryResult, LixError> {
        crate::session::write_execution_bindings::execute_prepared_public_read_with_registry(
            self.catalog_projection_registry().as_ref(),
            transaction,
            pending_view,
            public_read,
        )
        .await
    }

    async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        writes: &[crate::execution::write::filesystem::runtime::BinaryBlobWrite],
    ) -> Result<(), LixError> {
        crate::session::write_execution_bindings::persist_binary_blob_writes(transaction, writes)
            .await
    }

    async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
    ) -> Result<(), LixError> {
        crate::session::write_execution_bindings::garbage_collect_unreachable_binary_cas(
            transaction,
        )
        .await
    }

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        functions: &SharedFunctionProvider<Box<dyn crate::contracts::LixFunctionProvider + Send>>,
    ) -> Result<(), LixError> {
        crate::session::write_execution_bindings::persist_runtime_sequence(transaction, functions)
            .await
    }

    async fn execute_public_tracked_append_txn_with_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        unit: &crate::execution::write::buffered::TrackedTxnUnit,
        pending_commit_session: Option<&mut Option<crate::contracts::PendingPublicCommitSession>>,
    ) -> Result<crate::contracts::TrackedCommitExecutionOutcome, LixError> {
        crate::session::write_execution_bindings::execute_public_tracked_append(
            transaction,
            unit,
            pending_commit_session,
        )
        .await
    }
}

pub(crate) struct LixRuntimeSessionServices {
    runtime: Arc<Runtime>,
}

impl LixRuntimeSessionServices {
    pub(crate) fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }
}

#[async_trait(?Send)]
impl crate::session::collaborators::SessionServices for LixRuntimeSessionServices {
    async fn ensure_initialized(&self) -> Result<(), LixError> {
        if crate::live_state::load_mode_with_backend(self.runtime.backend().as_ref()).await?
            == crate::live_state::LiveStateMode::Uninitialized
        {
            return Err(crate::common::errors::not_initialized_error());
        }
        Ok(())
    }

    fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.runtime.backend()
    }

    fn access_to_internal(&self) -> bool {
        self.runtime.access_to_internal()
    }

    async fn begin_write_unit(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.runtime.begin_write_unit().await
    }

    async fn begin_read_unit(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.runtime.begin_read_unit(mode).await
    }

    fn public_surface_registry(&self) -> SurfaceRegistry {
        self.runtime.public_surface_registry()
    }

    fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.runtime.install_public_surface_registry(registry);
    }

    async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.runtime
            .load_public_surface_registry_from_backend()
            .await
    }

    async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        self.runtime.backend().export_image(writer).await
    }

    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.runtime.catalog_projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.runtime.schema_cache()
    }

    async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        let storage_scope = global_deterministic_settings_storage_scope();
        self.runtime
            .prepare_runtime_functions_with_backend(backend, &storage_scope)
            .await
    }

    fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.runtime.state_commit_stream(filter)
    }

    fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.runtime.emit_state_commit_stream_changes(changes);
    }

    fn invalidate_deterministic_settings_cache(&self) {
        self.runtime.invalidate_deterministic_settings_cache();
    }

    fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.runtime.invalidate_installed_plugins_cache()
    }
}

#[derive(Default)]
struct VecImageWriter {
    bytes: Vec<u8>,
}

#[async_trait(?Send)]
impl ImageChunkWriter for VecImageWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::sql::parser::parse_sql(sql) else {
        return false;
    };
    crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(&statements)
}

#[cfg(test)]
mod tests {
    use super::should_invalidate_installed_plugins_cache_for_sql;
    use super::*;
    use crate::runtime::wasm::NoopWasmRuntime;
    use crate::sql::analysis::state_resolution::canonical::is_query_only_statements;
    use crate::sql::binder::{advance_placeholder_state_for_statement_ast, bind_sql_with_state};
    use crate::sql::optimizer::optimize_state_resolution;
    use crate::sql::parser::parse_sql_statements;
    use crate::sql::parser::placeholders::PlaceholderState;
    use crate::sql::prepare::script::extract_explicit_transaction_script_from_statements;
    use crate::TransactionMode;
    use crate::{
        ExecuteOptions, LixBackend, LixBackendTransaction, LixConfig, LixError, QueryResult,
        Session, SqlDialect, Value,
    };
    use async_trait::async_trait;
    use sqlparser::ast::Statement;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct TestBackend {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
    }

    struct TestTransaction {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
        mode: TransactionMode,
    }

    #[async_trait(?Send)]
    impl LixBackend for TestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.to_ascii_lowercase().contains("unknown_table") {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "no such table: unknown_table".to_string(),
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            mode: TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(TestTransaction {
                commit_called: Arc::clone(&self.commit_called),
                rollback_called: Arc::clone(&self.rollback_called),
                mode,
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for TestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> TransactionMode {
            self.mode
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.to_ascii_lowercase().contains("unknown_table") {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "no such table: unknown_table".to_string(),
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            self.commit_called.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            self.rollback_called.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn refresh_cache_detection_matches_lix_state_writes() {
        assert!(should_refresh_file_cache_for_sql(
            "UPDATE lix_state SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
        assert!(should_refresh_file_cache_for_sql(
            "DELETE FROM lix_state_by_version WHERE file_id = 'f'"
        ));
        assert!(should_refresh_file_cache_for_sql(
            "UPDATE lix_state_by_version SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
        assert!(should_refresh_file_cache_for_sql(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, snapshot_content) VALUES ('/x', 'json_pointer', 'f', '{}')"
        ));
    }

    #[test]
    fn refresh_cache_detection_ignores_non_target_tables() {
        assert!(!should_refresh_file_cache_for_sql(
            "SELECT * FROM lix_state WHERE file_id = 'f'"
        ));
        assert!(!should_refresh_file_cache_for_sql(
            "UPDATE lix_state_history SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
    }

    #[test]
    fn query_only_detection_matches_select_statements() {
        assert!(is_query_only_sql("SELECT path, data FROM lix_file"));
        assert!(is_query_only_sql(
            "SELECT path FROM lix_file; SELECT id FROM lix_version"
        ));
    }

    #[test]
    fn query_only_detection_rejects_mutations() {
        assert!(!is_query_only_sql(
            "SELECT path FROM lix_file; UPDATE lix_file SET path = '/x' WHERE id = 'f'"
        ));
        assert!(!is_query_only_sql(
            "UPDATE lix_file SET path = '/x' WHERE id = 'f'"
        ));
    }

    #[test]
    fn unknown_read_query_returns_unknown_table_error() {
        std::thread::Builder::new()
            .name("unknown-read-query-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build tokio runtime");
                runtime.block_on(async {
                    let commit_called = Arc::new(AtomicBool::new(false));
                    let rollback_called = Arc::new(AtomicBool::new(false));
                    let lix = Arc::new(Lix::boot(LixConfig::new(
                        Box::new(TestBackend {
                            commit_called,
                            rollback_called,
                        }),
                        Arc::new(NoopWasmRuntime),
                    )));
                    let session = Session::new_for_test(
                        crate::session::collaborators::SessionCollaborators::new(
                            lix.session_services(),
                        ),
                        "version-test".to_string(),
                        Vec::new(),
                    );

                    let error = session
                        .execute("SELECT * FROM unknown_table", &[])
                        .await
                        .expect_err("unknown relation query should fail");

                    assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
                });
            })
            .expect("spawn unknown read query test thread")
            .join()
            .expect("unknown read query test thread should succeed");
    }

    #[test]
    fn plugin_cache_invalidation_detects_filesystem_mutations() {
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "INSERT INTO lix_file (id, path, data) VALUES ('f', '/.lix/plugins/k.lixplugin', X'00')"
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "UPDATE lix_file_by_version SET data = X'01' WHERE id = 'f' AND lixcol_version_id = 'global'"
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "DELETE FROM lix_file_by_version WHERE id = 'f' AND lixcol_version_id = 'global'"
        ));
        assert!(!should_invalidate_installed_plugins_cache_for_sql(
            "SELECT * FROM lix_file WHERE id = 'f'"
        ));
    }

    #[tokio::test]
    async fn transaction_plugin_cache_invalidation_happens_after_commit() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let lix = Arc::new(Lix::boot(LixConfig::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        )));
        let session = Session::new_for_test(
            crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
            "version-test".to_string(),
            Vec::new(),
        );

        {
            let mut cache = lix
                .runtime()
                .installed_plugins_cache()
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }

        let mut tx = session
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.mark_installed_plugins_cache_invalidation_pending()
            .expect("mark plugin cache invalidation");

        assert!(
            lix.runtime()
                .installed_plugins_cache()
                .read()
                .expect("installed plugins cache lock")
                .is_some(),
            "cache should remain populated before commit"
        );

        tx.commit().await.expect("commit should succeed");
        assert!(commit_called.load(Ordering::SeqCst));
        assert!(!rollback_called.load(Ordering::SeqCst));
        assert!(
            lix.runtime()
                .installed_plugins_cache()
                .read()
                .expect("installed plugins cache lock")
                .is_none(),
            "cache should be invalidated after successful commit"
        );
    }

    #[tokio::test]
    async fn transaction_plugin_cache_invalidation_skips_rollback() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let lix = Arc::new(Lix::boot(LixConfig::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        )));
        let session = Session::new_for_test(
            crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
            "version-test".to_string(),
            Vec::new(),
        );

        {
            let mut cache = lix
                .runtime()
                .installed_plugins_cache()
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }

        let mut tx = session
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.mark_installed_plugins_cache_invalidation_pending()
            .expect("mark plugin cache invalidation");
        tx.rollback().await.expect("rollback should succeed");

        assert!(!commit_called.load(Ordering::SeqCst));
        assert!(rollback_called.load(Ordering::SeqCst));
        assert!(
            lix.runtime()
                .installed_plugins_cache()
                .read()
                .expect("installed plugins cache lock")
                .is_some(),
            "cache should remain populated after rollback"
        );
    }

    #[test]
    fn filesystem_side_effect_scan_advances_placeholder_state_across_statements() {
        let mut statements = parse_sql_statements(
            "UPDATE lix_file SET path = ? WHERE id = 'file-a'; \
             UPDATE lix_file SET path = ? WHERE id = 'file-b'",
        )
        .expect("parse sql");
        assert_eq!(statements.len(), 2);

        let params = vec![
            Value::Text("/docs/a.json".to_string()),
            Value::Text("/archive/b.json".to_string()),
        ];
        let mut placeholder_state = PlaceholderState::new();
        advance_placeholder_state_for_statement_ast(
            &mut statements[0],
            params.len(),
            &mut placeholder_state,
        )
        .expect("advance placeholder state for first statement");

        let bound = bind_sql_with_state("SELECT ?", &params, SqlDialect::Sqlite, placeholder_state)
            .expect("bind placeholder with carried state");
        assert_eq!(bound.sql, "SELECT ?1");
        assert_eq!(bound.params.len(), 1);
        assert_eq!(bound.params[0], Value::Text("/archive/b.json".to_string()));
    }

    #[test]
    fn extract_explicit_transaction_script_parses_begin_commit_wrapper() {
        let parsed = extract_explicit_transaction_script(
            "BEGIN; INSERT INTO lix_file (id, path, data) VALUES ('f1', '/a', x'01'); COMMIT;",
            &[],
        )
        .expect("parse transaction script");

        let statements = parsed.expect("expected explicit transaction script");
        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Insert(_)));
    }

    fn is_query_only_sql(sql: &str) -> bool {
        parse_sql_statements(sql)
            .map(|statements| is_query_only_statements(&statements))
            .unwrap_or(false)
    }

    fn should_refresh_file_cache_for_sql(sql: &str) -> bool {
        parse_sql_statements(sql)
            .map(|statements| {
                optimize_state_resolution(
                    &statements,
                    crate::sql::analysis::state_resolution::canonical::canonicalize_state_resolution(
                        &statements,
                    ),
                )
                .optimized
                .should_refresh_file_cache
            })
            .unwrap_or(false)
    }

    fn extract_explicit_transaction_script(
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Vec<Statement>>, LixError> {
        let statements = parse_sql_statements(sql)?;
        extract_explicit_transaction_script_from_statements(&statements, params)
    }

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
                    let (backend, _lix, _session) = crate::test_support::boot_test_engine()
                        .await
                        .expect("test engine should boot");
                    let lix = crate::Lix::boot(crate::LixConfig::new(
                        Box::new(backend.clone()),
                        std::sync::Arc::new(crate::wasm::NoopWasmRuntime),
                    ));
                    crate::live_state::mark_mode_with_backend(
                        &backend,
                        LiveStateMode::NeedsRebuild,
                    )
                    .await
                    .expect("marking live_state stale should succeed");

                    lix.open_existing()
                        .await
                        .expect("open_existing should not fail just because live_state is stale");

                    let status = lix
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
