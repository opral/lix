use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use jsonschema::JSONSchema;

use super::streams::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::binary_cas::load_blob_data_by_hash;
use crate::catalog::{CatalogProjectionRegistry, SurfaceRegistry};
use crate::contracts::SharedFunctionProvider;
use crate::contracts::{
    clone_boxed_function_provider, CompiledSchemaCache, FilesystemPluginMaterializer,
    InstalledPlugin, WasmComponentInstance, WasmRuntime,
};
use crate::live_state::{list_installed_plugin_archive_refs, PluginArchiveRef};
use crate::schema::SchemaKey;
use crate::services::plugin_archive::{
    invoke_apply_changes_export, load_installed_plugin_from_archive_bytes,
};
use crate::session::deterministic_mode::{
    global_deterministic_settings_storage_scope, load_runtime_settings, DeterministicSettings,
    PersistedKeyValueStorageScope, RuntimeFunctionProvider,
};
use crate::{LixBackend, LixError, TransactionBeginMode};

const INIT_STATE_NOT_STARTED: u8 = 0;
const INIT_STATE_IN_PROGRESS: u8 = 1;
const INIT_STATE_COMPLETED: u8 = 2;

struct CachedPluginComponent {
    wasm: Vec<u8>,
    instance: Arc<dyn WasmComponentInstance>,
}

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
        let storage_scope = global_deterministic_settings_storage_scope();
        let (_, functions) = self
            .prepare_runtime_functions_with_backend(self.backend().as_ref(), &storage_scope)
            .await?;
        let functions = clone_boxed_function_provider(&functions);
        crate::catalog::load_public_surface_registry_with_backend(
            self.backend().as_ref(),
            crate::services::cel_runtime::shared_runtime(),
            &functions,
        )
        .await
    }

    pub(crate) fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.state_commit_stream_bus.emit(changes);
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
        storage_scope: &PersistedKeyValueStorageScope,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        let settings = if self.deterministic_boot_pending() {
            self.boot_deterministic_settings()
                .unwrap_or_else(DeterministicSettings::disabled)
        } else if let Some(settings) = self.cached_deterministic_settings() {
            settings
        } else {
            let settings = load_runtime_settings(backend, storage_scope).await?;
            self.cache_deterministic_settings(settings);
            settings
        };

        let functions = SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, None));
        Ok((settings, functions))
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

    async fn load_or_init_plugin_component(
        &self,
        plugin: &InstalledPlugin,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        {
            let guard = self.plugin_component_cache.lock().map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "plugin component cache lock poisoned".to_string(),
            })?;
            if let Some(cached) = guard.get(&plugin.key) {
                if cached.wasm == plugin.wasm {
                    return Ok(cached.instance.clone());
                }
            }
        }

        let initialized = self
            .wasm_runtime
            .init_component(plugin.wasm.clone(), crate::contracts::WasmLimits::default())
            .await?;
        let mut guard = self.plugin_component_cache.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin component cache lock poisoned".to_string(),
        })?;
        if let Some(cached) = guard.get(&plugin.key) {
            if cached.wasm == plugin.wasm {
                return Ok(cached.instance.clone());
            }
        }
        guard.insert(
            plugin.key.clone(),
            CachedPluginComponent {
                wasm: plugin.wasm.clone(),
                instance: initialized.clone(),
            },
        );
        Ok(initialized)
    }

    async fn apply_changes_with_plugin(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError> {
        let instance = self.load_or_init_plugin_component(plugin).await?;
        invoke_apply_changes_export(instance.as_ref(), payload).await
    }

    async fn load_installed_plugins_with_runtime_cache(
        &self,
    ) -> Result<Vec<InstalledPlugin>, LixError> {
        if let Some(cached) = self
            .installed_plugins_cache
            .read()
            .map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "installed plugin cache lock poisoned".to_string(),
            })?
            .clone()
        {
            return Ok(cached);
        }

        let plugins = self.load_installed_plugins_from_backend().await?;
        let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
        *guard = Some(plugins.clone());
        Ok(plugins)
    }

    async fn load_installed_plugins_from_backend(&self) -> Result<Vec<InstalledPlugin>, LixError> {
        let archive_refs = list_installed_plugin_archive_refs(self.backend().as_ref()).await?;
        let mut plugins = Vec::with_capacity(archive_refs.len());
        for archive_ref in archive_refs {
            plugins.push(
                self.load_installed_plugin_from_archive_ref(&archive_ref)
                    .await?,
            );
        }
        Ok(plugins)
    }

    async fn load_installed_plugin_from_archive_ref(
        &self,
        archive_ref: &PluginArchiveRef,
    ) -> Result<InstalledPlugin, LixError> {
        let Some(plugin_key) = crate::contracts::plugin_key_from_archive_path(&archive_ref.path)
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: unsupported plugin archive path '{}'",
                    archive_ref.path
                ),
            });
        };
        let archive_bytes = load_blob_data_by_hash(self.backend().as_ref(), &archive_ref.blob_hash)
            .await?
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                    archive_ref.blob_hash, archive_ref.path, archive_ref.file_id
                ),
            })?;
        if archive_bytes.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: archive '{}' is empty",
                    archive_ref.path
                ),
            });
        }
        load_installed_plugin_from_archive_bytes(&plugin_key, &archive_ref.path, &archive_bytes)
    }

    pub(crate) fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
        *guard = None;
        let mut component_guard = self.plugin_component_cache.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin component cache lock poisoned".to_string(),
        })?;
        component_guard.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn installed_plugins_cache(&self) -> &RwLock<Option<Vec<InstalledPlugin>>> {
        &self.installed_plugins_cache
    }
}

#[async_trait(?Send)]
impl FilesystemPluginMaterializer for Engine {
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError> {
        self.load_installed_plugins_with_runtime_cache().await
    }

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError> {
        self.apply_changes_with_plugin(plugin, payload).await
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
    use crate::contracts::{InstalledPlugin, PluginRuntime, WasmLimits};
    use crate::services::wasm_runtime::NoopWasmRuntime;
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
            _mode: crate::TransactionBeginMode,
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

        let (settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("first runtime preparation should succeed");
        assert!(!settings.enabled);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "first call should read deterministic settings from the backend"
        );

        let (_settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("second runtime preparation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "disabled deterministic settings should be served from cache"
        );

        lix.engine().invalidate_deterministic_settings_cache();

        let (_settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("runtime preparation after invalidation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            2,
            "cache invalidation should force a backend refresh"
        );
    }

    #[derive(Default)]
    struct CountingRuntime {
        init_calls: Arc<AtomicUsize>,
    }

    struct NoopComponent;

    #[async_trait(?Send)]
    impl WasmRuntime for CountingRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(NoopComponent))
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopComponent {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn component_cache_reinitializes_when_same_key_wasm_changes() {
        let runtime = Arc::new(CountingRuntime::default());
        let engine = Engine::new(
            Box::new(CountingBackend {
                execute_calls: Arc::new(AtomicUsize::new(0)),
            }),
            runtime.clone(),
            false,
            None,
            crate::catalog::build_builtin_surface_registry(),
            Arc::new(crate::catalog::builtin_catalog_projection_registry().clone()),
        );
        let mut plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: "{}".to_string(),
            wasm: vec![1],
        };

        engine
            .load_or_init_plugin_component(&plugin)
            .await
            .expect("first init should succeed");
        engine
            .load_or_init_plugin_component(&plugin)
            .await
            .expect("second lookup should reuse cache");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);

        plugin.wasm = vec![2];
        engine
            .load_or_init_plugin_component(&plugin)
            .await
            .expect("changed wasm should reinitialize instance");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 2);
    }
}
