use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use jsonschema::JSONSchema;

use super::deterministic_settings::{load_global_runtime_settings, DeterministicSettings};
use crate::backend::TransactionBeginMode;
use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::functions::{
    clone_boxed_function_provider, DynFunctionProvider, FunctionBindings, LixFunctionProvider,
    RuntimeFunctionProvider, SharedFunctionProvider,
};
use crate::plugin::{
    invalidate_installed_plugins_cache, CachedPluginComponent, InstalledPlugin,
    PluginComponentHost, PluginMaterializationHost,
};
use crate::schema::CompiledSchemaCache;
use crate::schema::SchemaKey;
use crate::streams::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::wasm::WasmRuntime;
use crate::{LixBackend, LixError};

const INIT_STATE_NOT_STARTED: u8 = 0;
const INIT_STATE_IN_PROGRESS: u8 = 1;
const INIT_STATE_COMPLETED: u8 = 2;

pub(crate) struct Engine {
    backend: Arc<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    schema_cache: SchemaCache,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
    deterministic_settings_cache: RwLock<Option<DeterministicSettings>>,
    init_state: AtomicU8,
    /// When true, the backend connection has an active transaction started by
    /// the init path. `begin_write_unit()` uses savepoints instead of BEGIN.
    in_init_transaction: AtomicBool,
    savepoint_counter: AtomicU64,
    public_surface_registry: RwLock<SurfaceRegistry>,
    catalog_projection_registry: Arc<CatalogProjectionRegistry>,
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
}

impl Engine {
    pub(crate) fn new(
        backend: Box<dyn LixBackend + Send + Sync>,
        wasm_runtime: Arc<dyn WasmRuntime>,
        access_to_internal: bool,
        boot_deterministic_settings: Option<DeterministicSettings>,
        public_surface_registry: SurfaceRegistry,
        catalog_projection_registry: Arc<CatalogProjectionRegistry>,
    ) -> Self {
        let deterministic_boot_pending = boot_deterministic_settings.is_some();
        Self {
            backend: Arc::from(backend),
            wasm_runtime,
            schema_cache: SchemaCache::new(),
            boot_deterministic_settings,
            deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
            deterministic_settings_cache: RwLock::new(boot_deterministic_settings),
            init_state: AtomicU8::new(INIT_STATE_NOT_STARTED),
            in_init_transaction: AtomicBool::new(false),
            savepoint_counter: AtomicU64::new(0),
            public_surface_registry: RwLock::new(public_surface_registry),
            catalog_projection_registry,
            access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
        }
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        &self.backend
    }

    pub(crate) fn schema_cache(&self) -> &SchemaCache {
        &self.schema_cache
    }

    pub(crate) fn access_to_internal(&self) -> bool {
        self.access_to_internal
    }

    pub(crate) fn public_surface_registry(&self) -> SurfaceRegistry {
        self.public_surface_registry
            .read()
            .expect("public surface registry lock poisoned")
            .clone()
    }

    pub(crate) fn catalog_projection_registry(&self) -> &Arc<CatalogProjectionRegistry> {
        &self.catalog_projection_registry
    }

    pub(crate) fn session_host(self: &Arc<Self>) -> Arc<dyn crate::session::SessionHost> {
        Arc::new(EngineSessionHost {
            engine: Arc::clone(self),
        })
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        let mut guard = self
            .public_surface_registry
            .write()
            .expect("public surface registry lock poisoned");
        *guard = registry;
    }

    pub(crate) fn clear_public_surface_registry(&self) {
        self.install_public_surface_registry(SurfaceRegistry::default());
    }

    pub(crate) async fn load_public_surface_registry_from_backend(
        &self,
    ) -> Result<SurfaceRegistry, LixError> {
        let functions = self
            .prepare_runtime_functions_with_backend(self.backend().as_ref())
            .await?;
        let functions = clone_boxed_function_provider(&functions);
        crate::catalog::load_public_surface_registry_with_backend(
            self.backend().as_ref(),
            None,
            crate::cel::shared_runtime(),
            &functions,
        )
        .await
    }

    pub(crate) fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) fn latest_state_commit_sequence(&self) -> Option<u64> {
        self.state_commit_stream_bus.latest_sequence()
    }

    pub(crate) fn emit_state_commit_stream_changes(
        &self,
        changes: Vec<StateCommitStreamChange>,
    ) -> Option<u64> {
        self.state_commit_stream_bus.emit(changes)
    }

    pub(crate) fn deterministic_boot_pending(&self) -> bool {
        self.deterministic_boot_pending.load(Ordering::SeqCst)
    }

    pub(crate) fn boot_deterministic_settings(&self) -> Option<DeterministicSettings> {
        self.boot_deterministic_settings
    }

    pub(crate) fn cached_deterministic_settings(&self) -> Option<DeterministicSettings> {
        *self
            .deterministic_settings_cache
            .read()
            .expect("deterministic settings cache lock poisoned")
    }

    pub(crate) fn cache_deterministic_settings(&self, settings: DeterministicSettings) {
        *self
            .deterministic_settings_cache
            .write()
            .expect("deterministic settings cache lock poisoned") = Some(settings);
    }

    pub(crate) fn clear_deterministic_boot_pending(&self) {
        self.deterministic_boot_pending
            .store(false, Ordering::SeqCst);
    }

    pub(crate) fn invalidate_deterministic_settings_cache(&self) {
        *self
            .deterministic_settings_cache
            .write()
            .expect("deterministic settings cache lock poisoned") = None;
    }

    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<DynFunctionProvider, LixError> {
        let settings = if self.deterministic_boot_pending() {
            self.boot_deterministic_settings()
                .unwrap_or_else(DeterministicSettings::disabled)
        } else if let Some(settings) = self.cached_deterministic_settings() {
            settings
        } else {
            let settings = load_global_runtime_settings(backend).await?;
            self.cache_deterministic_settings(settings);
            settings
        };

        Ok(SharedFunctionProvider::new(Box::new(
            RuntimeFunctionProvider::new(
                settings.enabled,
                settings.uuid_v7_enabled,
                settings.timestamp_enabled,
                settings.timestamp_shuffle_enabled,
                None,
            ),
        )))
    }

    pub(crate) fn try_mark_init_in_progress(&self) -> Result<(), LixError> {
        self.init_state
            .compare_exchange(
                INIT_STATE_NOT_STARTED,
                INIT_STATE_IN_PROGRESS,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|_| crate::common::already_initialized_error())
    }

    pub(crate) fn mark_init_completed(&self) {
        self.init_state
            .store(INIT_STATE_COMPLETED, Ordering::SeqCst);
    }

    pub(crate) fn reset_init_state(&self) {
        self.init_state
            .store(INIT_STATE_NOT_STARTED, Ordering::SeqCst);
    }

    pub(crate) async fn begin_write_unit(
        &self,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
        if self.in_init_transaction.load(Ordering::SeqCst) {
            let id = self.savepoint_counter.fetch_add(1, Ordering::SeqCst);
            self.backend.begin_savepoint(&format!("sp_{id}")).await
        } else {
            self.backend
                .begin_transaction(TransactionBeginMode::Write)
                .await
        }
    }

    pub(crate) async fn begin_read_unit(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
        self.backend.begin_transaction(mode).await
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        invalidate_installed_plugins_cache(self)
    }

    #[cfg(test)]
    pub(crate) fn installed_plugins_cache(&self) -> &RwLock<Option<Vec<InstalledPlugin>>> {
        &self.installed_plugins_cache
    }
}

pub(crate) struct EngineSessionHost {
    engine: Arc<Engine>,
}

#[async_trait(?Send)]
impl crate::session::SessionHost for EngineSessionHost {
    async fn ensure_initialized(&self) -> Result<(), LixError> {
        if crate::live_state::load_mode_with_backend(self.engine.backend().as_ref()).await?
            == crate::live_state::LiveStateMode::Uninitialized
        {
            return Err(crate::common::not_initialized_error());
        }
        Ok(())
    }

    fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.engine.backend()
    }

    fn access_to_internal(&self) -> bool {
        self.engine.access_to_internal()
    }

    async fn begin_write_unit(
        &self,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
        self.engine.begin_write_unit().await
    }

    async fn begin_read_unit(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
        self.engine.begin_read_unit(mode).await
    }

    fn public_surface_registry(&self) -> SurfaceRegistry {
        self.engine.public_surface_registry()
    }

    fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.engine.install_public_surface_registry(registry);
    }

    async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.engine
            .load_public_surface_registry_from_backend()
            .await
    }

    async fn export_image(
        &self,
        writer: &mut dyn crate::image::ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.engine.backend().export_image(writer).await
    }

    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.engine.catalog_projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.engine.schema_cache()
    }

    async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<DynFunctionProvider, LixError> {
        self.engine
            .prepare_runtime_functions_with_backend(backend)
            .await
    }

    fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.engine.state_commit_stream(filter)
    }

    fn latest_state_commit_sequence(&self) -> Option<u64> {
        self.engine.latest_state_commit_sequence()
    }

    fn emit_state_commit_stream_changes(
        &self,
        changes: Vec<StateCommitStreamChange>,
    ) -> Option<u64> {
        self.engine.emit_state_commit_stream_changes(changes)
    }

    fn invalidate_deterministic_settings_cache(&self) {
        self.engine.invalidate_deterministic_settings_cache();
    }

    fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.engine.invalidate_installed_plugins_cache()
    }
}

#[async_trait(?Send)]
impl crate::transaction::WriteExecutionContext for Engine {
    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.catalog_projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.schema_cache()
    }

    fn sql_compiler_seed<'a>(
        &'a self,
        functions: &'a DynFunctionProvider,
        surface_registry: &'a SurfaceRegistry,
    ) -> crate::sql::SqlCompilerSeed<'a> {
        crate::sql::SqlCompilerSeed {
            dialect: self.backend().dialect(),
            functions: clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    async fn prepare_function_bindings(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<FunctionBindings, LixError> {
        let functions = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(FunctionBindings::from_prepared_parts(
            functions.deterministic_sequence_enabled(),
            &functions,
        ))
    }

    async fn execute_pending_overlay_public_read(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        pending_overlay: Option<&dyn crate::transaction::PendingOverlay>,
        public_read: &crate::sql::PreparedPublicRead,
    ) -> Result<crate::QueryResult, LixError> {
        crate::session::execute_prepared_public_read_with_registry(
            self.catalog_projection_registry().as_ref(),
            transaction,
            pending_overlay,
            public_read,
        )
        .await
    }

    async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        writes: &[crate::transaction::BinaryBlobWrite],
    ) -> Result<(), LixError> {
        crate::session::persist_binary_blob_writes_in_transaction(transaction, writes).await
    }

    async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
    ) -> Result<(), LixError> {
        crate::session::garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
    }

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Result<(), LixError> {
        crate::session::persist_runtime_sequence_in_transaction(transaction, functions).await
    }

    async fn execute_public_tracked_append_txn_with_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        unit: &crate::transaction::TrackedTxnUnit,
        pending_commit_state: Option<&mut Option<crate::transaction::PendingCommitState>>,
    ) -> Result<crate::transaction::TrackedCommitExecutionOutcome, LixError> {
        crate::session::execute_public_tracked_append_txn_with_transaction(
            transaction,
            unit,
            pending_commit_state,
        )
        .await
    }
}

impl PluginComponentHost for Engine {
    fn plugin_component_cache(&self) -> &Mutex<BTreeMap<String, CachedPluginComponent>> {
        &self.plugin_component_cache
    }

    fn wasm_runtime(&self) -> &Arc<dyn WasmRuntime> {
        &self.wasm_runtime
    }
}

impl PluginMaterializationHost for Engine {
    fn plugin_backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        &self.backend
    }

    fn installed_plugins_cache(&self) -> &RwLock<Option<Vec<InstalledPlugin>>> {
        &self.installed_plugins_cache
    }
}

#[derive(Debug, Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<SchemaKey, Arc<JSONSchema>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn read(
        &self,
    ) -> std::sync::LockResult<std::sync::RwLockReadGuard<'_, HashMap<SchemaKey, Arc<JSONSchema>>>>
    {
        self.inner.read()
    }

    pub(crate) fn write(
        &self,
    ) -> std::sync::LockResult<std::sync::RwLockWriteGuard<'_, HashMap<SchemaKey, Arc<JSONSchema>>>>
    {
        self.inner.write()
    }
}

impl CompiledSchemaCache for SchemaCache {
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>> {
        self.read().ok().and_then(|guard| guard.get(key).cloned())
    }

    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>) {
        if let Ok(mut guard) = self.write() {
            guard.insert(key, schema);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::LixFunctionProvider;
    use crate::wasm::NoopWasmRuntime;
    use crate::{Lix, LixConfig, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct CountingBackend {
        execute_calls: Arc<AtomicUsize>,
    }

    #[async_trait(?Send)]
    impl LixBackend for CountingBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::backend::TransactionBeginMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this test",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    #[tokio::test]
    async fn caches_disabled_deterministic_settings_until_invalidated() {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            execute_calls: Arc::clone(&execute_calls),
        };
        let lix = Lix::boot(LixConfig::new(Box::new(backend), Arc::new(NoopWasmRuntime)));

        let functions = lix
            .engine()
            .prepare_runtime_functions_with_backend(lix.engine().backend().as_ref())
            .await
            .expect("first runtime preparation should succeed");
        assert!(!functions.deterministic_sequence_enabled());
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "first call should read deterministic settings from the backend"
        );

        let _functions = lix
            .engine()
            .prepare_runtime_functions_with_backend(lix.engine().backend().as_ref())
            .await
            .expect("second runtime preparation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "disabled deterministic settings should be served from cache"
        );

        lix.engine().invalidate_deterministic_settings_cache();

        let _functions = lix
            .engine()
            .prepare_runtime_functions_with_backend(lix.engine().backend().as_ref())
            .await
            .expect("runtime preparation after invalidation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            2,
            "cache invalidation should force a backend refresh"
        );
    }
}
