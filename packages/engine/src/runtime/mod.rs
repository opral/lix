use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub(crate) mod cel;
pub(crate) mod deterministic_mode;
pub(crate) mod execution_state;
pub(crate) mod functions;
pub mod image;
pub(crate) mod plugin;
mod sql_compiler_metadata;
pub mod streams;
pub mod wasm;

use crate::backend::QueryExecutor;
use crate::contracts::artifacts::MutationRow;
use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::CompiledSchemaCache;
use crate::runtime::cel::CelEvaluator;
use crate::runtime::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::runtime::functions::SharedFunctionProvider;
use crate::runtime::plugin::runtime::CachedPluginComponent;
use crate::runtime::plugin::types::InstalledPlugin;
use crate::runtime::streams::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::runtime::wasm::WasmRuntime;
use crate::schema::builtin::storage::key_value_schema_key;
use crate::schema::SchemaKey;
use crate::{
    LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionMode, Value,
};
use async_trait::async_trait;
use jsonschema::JSONSchema;
use sqlparser::ast::Statement;

const INIT_STATE_NOT_STARTED: u8 = 0;
const INIT_STATE_IN_PROGRESS: u8 = 1;
const INIT_STATE_COMPLETED: u8 = 2;

pub(crate) struct Runtime {
    backend: Arc<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    cel_evaluator: CelEvaluator,
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
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
}

impl Runtime {
    pub(crate) fn new(
        backend: Box<dyn LixBackend + Send + Sync>,
        wasm_runtime: Arc<dyn WasmRuntime>,
        access_to_internal: bool,
        boot_deterministic_settings: Option<DeterministicSettings>,
    ) -> Self {
        let deterministic_boot_pending = boot_deterministic_settings.is_some();
        Self {
            backend: Arc::from(backend),
            wasm_runtime,
            cel_evaluator: CelEvaluator::new(),
            schema_cache: SchemaCache::new(),
            boot_deterministic_settings,
            deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
            deterministic_settings_cache: RwLock::new(boot_deterministic_settings),
            init_state: AtomicU8::new(INIT_STATE_NOT_STARTED),
            in_init_transaction: AtomicBool::new(false),
            savepoint_counter: AtomicU64::new(0),
            public_surface_registry: RwLock::new(SurfaceRegistry::with_builtin_surfaces()),
            access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
        }
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        &self.backend
    }

    pub(crate) fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        Arc::clone(&self.wasm_runtime)
    }

    pub(crate) fn wasm_runtime_ref(&self) -> &dyn WasmRuntime {
        self.wasm_runtime.as_ref()
    }

    pub(crate) fn cel_evaluator(&self) -> &CelEvaluator {
        &self.cel_evaluator
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

    pub(crate) async fn refresh_public_surface_registry(&self) -> Result<(), LixError> {
        let registry = SurfaceRegistry::bootstrap_with_backend(self.backend.as_ref()).await?;
        let mut guard = self
            .public_surface_registry
            .write()
            .expect("public surface registry lock poisoned");
        *guard = registry;
        Ok(())
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

    pub(crate) fn try_mark_init_in_progress(&self) -> Result<(), LixError> {
        self.init_state
            .compare_exchange(
                INIT_STATE_NOT_STARTED,
                INIT_STATE_IN_PROGRESS,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|_| crate::errors::already_initialized_error())
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
            self.backend.begin_transaction(TransactionMode::Write).await
        }
    }

    pub(crate) async fn begin_read_unit(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
        self.backend.begin_transaction(mode).await
    }

    pub(crate) fn should_invalidate_deterministic_settings_cache(
        &self,
        mutations: &[MutationRow],
        state_commit_stream_changes: &[StateCommitStreamChange],
    ) -> bool {
        mutations.iter().any(|row| {
            row.schema_key == key_value_schema_key()
                && row.entity_id == crate::runtime::deterministic_mode::deterministic_mode_key()
        }) || state_commit_stream_changes.iter().any(|change| {
            change.schema_key == key_value_schema_key()
                && change.entity_id == crate::runtime::deterministic_mode::deterministic_mode_key()
        })
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

#[async_trait(?Send)]
pub(crate) trait RuntimeHost {
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

    async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError>;

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        settings: DeterministicSettings,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
impl RuntimeHost for Runtime {
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
        Runtime::prepare_runtime_functions_with_backend(self, backend).await
    }

    async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        Runtime::ensure_runtime_sequence_initialized_in_transaction(self, transaction, functions)
            .await
    }

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        settings: DeterministicSettings,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        Runtime::persist_runtime_sequence_in_transaction(self, transaction, settings, functions)
            .await
    }
}

pub(crate) struct TransactionBackendAdapter<'a> {
    dialect: SqlDialect,
    transaction: Mutex<*mut (dyn LixBackendTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

pub(crate) async fn normalize_sql_execution_error_with_backend(
    backend: &dyn LixBackend,
    error: LixError,
    statements: &[Statement],
) -> LixError {
    crate::errors::classification::normalize_sql_error_with_backend(backend, error, statements)
        .await
}

pub(crate) use sql_compiler_metadata::load_sql_compiler_metadata;

// SAFETY: `TransactionBackendAdapter` is only used inside a single async execution flow.
// Internal access to the raw transaction pointer is serialized with a mutex.
unsafe impl<'a> Send for TransactionBackendAdapter<'a> {}
// SAFETY: see `Send` impl above.
unsafe impl<'a> Sync for TransactionBackendAdapter<'a> {}

impl<'a> TransactionBackendAdapter<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction as *mut (dyn LixBackendTransaction + 'a)),
            _lifetime: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<'a> QueryExecutor for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixBackendTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }
}

#[async_trait(?Send)]
impl<'a> LixBackend for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixBackendTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(
        &self,
        _mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "nested transactions are not supported via TransactionBackendAdapter"
                .to_string(),
        })
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "savepoints are not supported via TransactionBackendAdapter".to_string(),
        })
    }
}
