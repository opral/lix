use crate::cel::CelEvaluator;
use crate::deterministic_mode::{deterministic_mode_key, DeterministicSettings};
use crate::key_value::key_value_schema_key;
use crate::plugin::types::InstalledPlugin;
use crate::schema::schema_from_registered_snapshot;
use crate::sql::execution::execution_program::ExecutionContext;
use crate::sql::execution::parse::parse_sql;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::sql::public::validation::SchemaCache;
use crate::state::stream::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::transaction::{
    execute_parsed_statements_in_write_transaction, TransactionCommitOutcome, WriteTransaction,
};
use crate::WasmRuntime;
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};
use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectNamePart, Statement, TableFactor, TableObject};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::sql::execution::contracts::effects::FilesystemPayloadDomainChange;
use crate::sql::execution::contracts::planned_statement::MutationRow;

pub use crate::boot::{boot, BootAccount, BootArgs, BootKeyValue};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
pub(crate) const INIT_STATE_NOT_STARTED: u8 = 0;
pub(crate) const INIT_STATE_IN_PROGRESS: u8 = 1;
pub(crate) const INIT_STATE_COMPLETED: u8 = 2;
const REGISTER_SCHEMA_HELPER_SQL: &str =
    "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))";

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

pub struct Engine {
    pub(crate) backend: Arc<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    pub(crate) cel_evaluator: CelEvaluator,
    pub(crate) schema_cache: SchemaCache,
    boot_key_values: Vec<BootKeyValue>,
    boot_active_account: Option<BootAccount>,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
    deterministic_settings_cache: RwLock<Option<DeterministicSettings>>,
    init_state: AtomicU8,
    /// When true, the backend connection has an active transaction started by
    /// the init path. `begin_write_unit()` uses savepoints instead of BEGIN.
    in_init_transaction: AtomicBool,
    savepoint_counter: AtomicU64,
    active_version_id: RwLock<Option<String>>,
    public_surface_registry: RwLock<SurfaceRegistry>,
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, crate::plugin::runtime::CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
    pub(crate) observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::observe::SharedObserveSource>>>>,
}

#[must_use = "EngineTransaction must be committed or rolled back"]
pub struct EngineTransaction<'a> {
    pub(crate) engine: &'a Engine,
    pub(crate) write_transaction: Option<WriteTransaction<'a>>,
    pub(crate) context: ExecutionContext,
}

impl Engine {
    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        let mut transaction = self
            .begin_transaction_with_options(ExecuteOptions::default())
            .await?;
        transaction.register_schema(schema).await?;
        transaction.commit().await
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<EngineTransaction<'_>, LixError> {
        let transaction = self.begin_write_unit().await?;
        Ok(EngineTransaction {
            engine: self,
            write_transaction: Some(WriteTransaction::new_buffered_write(transaction)),
            context: self.new_execution_context(options)?,
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut EngineTransaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let mut transaction = self.begin_transaction_with_options(options).await?;
        match std::panic::AssertUnwindSafe(f(&mut transaction))
            .catch_unwind()
            .await
        {
            Ok(Ok(value)) => {
                transaction.commit().await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
            Err(payload) => {
                let _ = transaction.rollback().await;
                std::panic::resume_unwind(payload);
            }
        }
    }

    pub(crate) fn new_execution_context(
        &self,
        options: ExecuteOptions,
    ) -> Result<ExecutionContext, LixError> {
        let active_version_id = self.require_active_version_id()?;
        Ok(self.new_execution_context_with_active_version(options, active_version_id))
    }

    pub(crate) fn new_execution_context_with_active_version(
        &self,
        options: ExecuteOptions,
        active_version_id: String,
    ) -> ExecutionContext {
        ExecutionContext::new(options, self.public_surface_registry(), active_version_id)
    }

    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.wasm_runtime.clone()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) fn access_to_internal(&self) -> bool {
        self.access_to_internal
    }

    pub(crate) fn wasm_runtime_ref(&self) -> &dyn WasmRuntime {
        self.wasm_runtime.as_ref()
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

    pub(crate) fn boot_key_values(&self) -> &[BootKeyValue] {
        &self.boot_key_values
    }

    pub(crate) fn boot_active_account(&self) -> Option<&BootAccount> {
        self.boot_active_account.as_ref()
    }

    pub(crate) fn require_active_version_id(&self) -> Result<String, LixError> {
        let guard = self.active_version_id.read().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "active version cache lock poisoned".to_string(),
        })?;
        guard
            .clone()
            .ok_or_else(crate::errors::not_initialized_error)
    }

    pub(crate) fn clear_active_version_id(&self) {
        let mut guard = self.active_version_id.write().unwrap();
        *guard = None;
    }

    pub(crate) fn set_active_version_id(&self, version_id: String) {
        let mut guard = self.active_version_id.write().unwrap();
        if guard.as_ref() == Some(&version_id) {
            return;
        }
        *guard = Some(version_id);
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

    pub(crate) fn mark_init_completed(&self) {
        self.init_state
            .store(INIT_STATE_COMPLETED, Ordering::SeqCst);
    }

    pub(crate) fn reset_init_state(&self) {
        self.init_state
            .store(INIT_STATE_NOT_STARTED, Ordering::SeqCst);
    }

    pub(crate) async fn apply_transaction_commit_outcome(
        &self,
        mut outcome: TransactionCommitOutcome,
    ) -> Result<(), LixError> {
        if let Some(version_id) = outcome.next_active_version_id.take() {
            self.set_active_version_id(version_id);
        }
        if outcome.invalidate_deterministic_settings_cache {
            self.invalidate_deterministic_settings_cache();
        }
        if outcome.invalidate_installed_plugins_cache {
            self.invalidate_installed_plugins_cache()?;
        }
        if outcome.refresh_public_surface_registry {
            self.refresh_public_surface_registry().await?;
        }
        self.emit_state_commit_stream_changes(std::mem::take(
            &mut outcome.state_commit_stream_changes,
        ));
        Ok(())
    }

    /// Begin an isolated unit of work on the backend.
    ///
    /// During normal operation, this starts a real transaction (`BEGIN IMMEDIATE`).
    /// During init (when an outer transaction is active on the connection),
    /// this uses a savepoint instead to avoid nested `BEGIN` errors.
    pub(crate) async fn begin_write_unit(
        &self,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
        if self.in_init_transaction.load(Ordering::SeqCst) {
            let id = self.savepoint_counter.fetch_add(1, Ordering::SeqCst);
            self.backend.begin_savepoint(&format!("sp_{id}")).await
        } else {
            self.backend.begin_transaction().await
        }
    }

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.state_commit_stream_bus.emit(changes);
    }

    pub(crate) fn should_invalidate_deterministic_settings_cache(
        &self,
        mutations: &[MutationRow],
        state_commit_stream_changes: &[StateCommitStreamChange],
    ) -> bool {
        mutations.iter().any(|row| {
            row.schema_key == key_value_schema_key() && row.entity_id == deterministic_mode_key()
        }) || state_commit_stream_changes.iter().any(|change| {
            change.schema_key == key_value_schema_key()
                && change.entity_id == deterministic_mode_key()
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
}

impl<'a> EngineTransaction<'a> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn mark_installed_plugins_cache_invalidation_pending(
        &mut self,
    ) -> Result<(), LixError> {
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .mark_installed_plugins_cache_invalidation_pending();
        Ok(())
    }

    pub(crate) fn record_state_commit_stream_changes(
        &mut self,
        changes: Vec<StateCommitStreamChange>,
    ) -> Result<(), LixError> {
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .record_state_commit_stream_changes(changes);
        Ok(())
    }

    pub async fn register_schema(&mut self, schema: &JsonValue) -> Result<(), LixError> {
        let snapshot = serde_json::json!({ "value": schema });
        let (schema_key, _) = schema_from_registered_snapshot(&snapshot)?;
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .register_schema(
                crate::live_state::SchemaRegistration::with_registered_snapshot(
                    schema_key.schema_key.clone(),
                    snapshot,
                ),
            )?;
        let schema_json = serde_json::to_string(schema).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to serialize schema definition: {error}"),
        })?;
        self.execute(REGISTER_SCHEMA_HELPER_SQL, &[Value::Text(schema_json)])
            .await?;
        Ok(())
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        if !self.engine.access_to_internal() {
            reject_public_create_table(&parsed_statements)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        self.execute_parsed_with_access(parsed_statements, params, self.engine.access_to_internal())
            .await
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        self.execute_parsed_with_access(parsed_statements, params, true)
            .await
    }

    async fn execute_parsed_with_access(
        &mut self,
        parsed_statements: Vec<Statement>,
        params: &[Value],
        allow_internal_tables: bool,
    ) -> Result<crate::ExecuteResult, LixError> {
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        execute_parsed_statements_in_write_transaction(
            self.engine,
            write_transaction,
            parsed_statements,
            params,
            allow_internal_tables,
            &mut self.context,
        )
        .await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let write_transaction = self.write_transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        let context = std::mem::replace(
            &mut self.context,
            self.engine
                .new_execution_context(ExecuteOptions::default())?,
        );
        let outcome = write_transaction
            .commit_buffered_write(self.engine, context)
            .await?;
        self.engine.apply_transaction_commit_outcome(outcome).await
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let write_transaction = self.write_transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        write_transaction.rollback_buffered_write().await
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn crate::LixBackendTransaction, LixError> {
        self.write_transaction_mut()?.backend_transaction_mut()
    }

    pub(crate) fn write_transaction_mut(&mut self) -> Result<&mut WriteTransaction<'a>, LixError> {
        Ok(self
            .write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?)
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.write_transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}

pub(crate) struct TransactionBackendAdapter<'a> {
    dialect: crate::SqlDialect,
    transaction: Mutex<*mut (dyn LixBackendTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

#[derive(Default)]
pub(crate) struct DeferredTransactionSideEffects {
    pub(crate) filesystem_state: crate::filesystem::runtime::FilesystemTransactionState,
}

pub(crate) fn reject_internal_table_writes(statements: &[Statement]) -> Result<(), LixError> {
    for statement in statements {
        if statement_mutates_protected_lix_relation(statement) {
            return Err(crate::errors::internal_table_access_denied_error());
        }
    }
    Ok(())
}

pub(crate) fn reject_public_create_table(statements: &[Statement]) -> Result<(), LixError> {
    if statements
        .iter()
        .any(|statement| matches!(statement, Statement::CreateTable(_)))
    {
        return Err(crate::errors::public_create_table_denied_error());
    }
    Ok(())
}

fn statement_mutates_protected_lix_relation(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => object_name_is_internal_storage_relation(name),
            _ => false,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => object_name_is_internal_storage_relation(name),
            _ => false,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            tables.iter().any(|table| match &table.relation {
                TableFactor::Table { name, .. } => object_name_is_internal_storage_relation(name),
                _ => false,
            })
        }
        Statement::AlterTable(alter) => object_name_is_protected_lix_ddl_target(&alter.name),
        Statement::CreateIndex(create_index) => {
            object_name_is_protected_lix_ddl_target(&create_index.table_name)
        }
        Statement::CreateTrigger(create_trigger) => {
            object_name_is_protected_lix_ddl_target(&create_trigger.table_name)
                || create_trigger
                    .referenced_table_name
                    .as_ref()
                    .map(object_name_is_protected_lix_ddl_target)
                    .unwrap_or(false)
        }
        Statement::DropTrigger(drop_trigger) => drop_trigger
            .table_name
            .as_ref()
            .map(object_name_is_protected_lix_ddl_target)
            .unwrap_or(false),
        Statement::Drop { names, table, .. } => {
            names.iter().any(object_name_is_protected_lix_ddl_target)
                || table
                    .as_ref()
                    .map(object_name_is_protected_lix_ddl_target)
                    .unwrap_or(false)
        }
        Statement::Truncate(truncate) => truncate
            .table_names
            .iter()
            .any(|target| object_name_is_protected_lix_ddl_target(&target.name)),
        _ => false,
    }
}

fn object_name_to_relation(name: &sqlparser::ast::ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.to_ascii_lowercase())
}

fn object_name_is_internal_storage_relation(name: &sqlparser::ast::ObjectName) -> bool {
    object_name_to_relation(name)
        .map(|relation| relation.starts_with("lix_internal_"))
        .unwrap_or(false)
}

fn object_name_is_protected_lix_ddl_target(name: &sqlparser::ast::ObjectName) -> bool {
    let Some(relation) = object_name_to_relation(name) else {
        return false;
    };

    relation.starts_with("lix_internal_")
        || crate::sql::public::catalog::builtin_public_surface_names()
            .iter()
            .any(|surface| surface.eq_ignore_ascii_case(&relation))
}

pub(crate) async fn normalize_sql_execution_error_with_backend(
    backend: &dyn LixBackend,
    error: LixError,
    statements: &[Statement],
) -> LixError {
    crate::errors::classification::normalize_sql_error_with_backend(backend, error, statements)
        .await
}

#[cfg(test)]
fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::sql::execution::parse::parse_sql(sql) else {
        return false;
    };
    crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(&statements)
}

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

#[async_trait::async_trait(?Send)]
impl<'a> crate::backend::QueryExecutor for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> crate::SqlDialect {
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

#[async_trait::async_trait(?Send)]
impl<'a> LixBackend for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> crate::SqlDialect {
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

    async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
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

impl Engine {
    pub(crate) fn from_boot_args(
        args: BootArgs,
        boot_deterministic_settings: Option<DeterministicSettings>,
    ) -> Self {
        let deterministic_boot_pending = boot_deterministic_settings.is_some();
        Self {
            backend: Arc::from(args.backend),
            wasm_runtime: args.wasm_runtime,
            cel_evaluator: CelEvaluator::new(),
            schema_cache: SchemaCache::new(),
            boot_key_values: args.key_values,
            boot_active_account: args.active_account,
            boot_deterministic_settings,
            deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
            deterministic_settings_cache: RwLock::new(boot_deterministic_settings),
            init_state: AtomicU8::new(INIT_STATE_NOT_STARTED),
            in_init_transaction: AtomicBool::new(false),
            savepoint_counter: AtomicU64::new(0),
            active_version_id: RwLock::new(None),
            public_surface_registry: RwLock::new(SurfaceRegistry::with_builtin_surfaces()),
            access_to_internal: args.access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
        }
    }
}

pub(crate) fn direct_state_file_cache_refresh_targets(
    mutations: &[MutationRow],
) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.file_id != "lix")
        .filter(|mutation| mutation.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|mutation| mutation.schema_key != DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|mutation| (mutation.file_id.clone(), mutation.version_id.clone()))
        .collect()
}

pub(crate) fn should_run_binary_cas_gc(
    mutations: &[MutationRow],
    filesystem_payload_domain_changes: &[FilesystemPayloadDomainChange],
) -> bool {
    mutations
        .iter()
        .any(|mutation| !mutation.untracked && mutation.schema_key == BINARY_BLOB_REF_SCHEMA_KEY)
        || filesystem_payload_domain_changes
            .iter()
            .any(|change| change.schema_key == BINARY_BLOB_REF_SCHEMA_KEY)
}

trait DedupableFilesystemPayloadChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str, bool);
}

impl DedupableFilesystemPayloadChange for FilesystemPayloadDomainChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str, bool) {
        (
            &self.file_id,
            &self.version_id,
            &self.schema_key,
            &self.entity_id,
            self.untracked,
        )
    }
}

fn dedupe_detected_changes<T>(changes: &[T]) -> Vec<T>
where
    T: DedupableFilesystemPayloadChange + Clone,
{
    let mut latest_by_key: BTreeMap<(&str, &str, &str, &str, bool), usize> = BTreeMap::new();
    for (index, change) in changes.iter().enumerate() {
        latest_by_key.insert(change.dedupe_key(), index);
    }

    let mut ordered_indexes = latest_by_key.into_values().collect::<Vec<_>>();
    ordered_indexes.sort_unstable();
    ordered_indexes
        .into_iter()
        .filter_map(|index| changes.get(index).cloned())
        .collect()
}

pub(crate) fn dedupe_filesystem_payload_domain_changes(
    changes: &[FilesystemPayloadDomainChange],
) -> Vec<FilesystemPayloadDomainChange> {
    dedupe_detected_changes(changes)
}

pub(crate) fn builtin_schema_entity_id(schema: &JsonValue) -> Result<String, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "builtin schema must define string x-lix-key".to_string(),
        })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "builtin schema must define string x-lix-version".to_string(),
        })?;

    Ok(format!("{schema_key}~{schema_version}"))
}

#[cfg(test)]
mod tests {
    use super::{
        boot, should_invalidate_installed_plugins_cache_for_sql, BootArgs, ExecuteOptions,
    };
    use crate::backend::{LixBackend, LixBackendTransaction, SqlDialect};
    use crate::schema::live_layout::untracked_live_table_name;
    use crate::sql::analysis::state_resolution::canonical::is_query_only_statements;
    use crate::sql::analysis::state_resolution::effects::active_version_from_update_validations;
    use crate::sql::analysis::state_resolution::optimize::should_refresh_file_cache_for_statements;
    use crate::sql::execution::contracts::planned_statement::UpdateValidationPlan;
    use crate::sql::internal::script::extract_explicit_transaction_script_from_statements;
    use crate::sql_support::binding::{
        advance_placeholder_state_for_statement_ast, bind_sql_with_state, parse_sql_statements,
        PlaceholderState,
    };
    use crate::version::active_version_schema_key;
    use crate::{LixError, NoopWasmRuntime, QueryResult, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Expr, Statement};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    struct TestBackend {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
    }

    struct TestTransaction {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
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

        async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(TestTransaction {
                commit_called: Arc::clone(&self.commit_called),
                rollback_called: Arc::clone(&self.rollback_called),
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction().await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for TestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
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
    fn detects_active_version_update_with_single_quoted_schema_key() {
        let table = untracked_live_table_name("lix_active_version");
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE {table} SET writer_key = NULL WHERE schema_key = '{}' AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-single");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-single"));
    }

    #[test]
    fn detects_active_version_update_with_double_quoted_schema_key() {
        let table = untracked_live_table_name("lix_active_version");
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE {table} SET writer_key = NULL WHERE schema_key = \"{}\" AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-double");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-double"));
    }

    #[test]
    fn ignores_non_active_version_schema_key() {
        let table = untracked_live_table_name("lix_active_version");
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE {table} SET writer_key = NULL WHERE schema_key = 'other_schema' AND entity_id = 'main'",
        ));
        let plan = update_validation_plan(where_clause, "v-other");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected, None);
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
        assert!(!should_refresh_file_cache_for_sql(
            "UPDATE lix_state_by_version SET snapshot_content = '{}' WHERE file_id = 'f'"
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
                    let engine = boot(BootArgs::new(
                        Box::new(TestBackend {
                            commit_called,
                            rollback_called,
                        }),
                        Arc::new(NoopWasmRuntime),
                    ));
                    engine.set_active_version_id("version-test".to_string());

                    let error = engine
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
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }
        engine.set_active_version_id("version-test".to_string());

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.mark_installed_plugins_cache_invalidation_pending()
            .expect("mark plugin cache invalidation");

        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_some(),
            "cache should remain populated before commit"
        );

        tx.commit().await.expect("commit should succeed");
        assert!(commit_called.load(Ordering::SeqCst));
        assert!(!rollback_called.load(Ordering::SeqCst));
        assert!(
            engine
                .installed_plugins_cache
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
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }
        engine.set_active_version_id("version-test".to_string());

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.mark_installed_plugins_cache_invalidation_pending()
            .expect("mark plugin cache invalidation");
        tx.rollback().await.expect("rollback should succeed");

        assert!(!commit_called.load(Ordering::SeqCst));
        assert!(rollback_called.load(Ordering::SeqCst));
        assert!(
            engine
                .installed_plugins_cache
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
            .map(|statements| should_refresh_file_cache_for_statements(&statements))
            .unwrap_or(false)
    }

    fn extract_explicit_transaction_script(
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Vec<Statement>>, LixError> {
        let statements = parse_sql_statements(sql)?;
        extract_explicit_transaction_script_from_statements(&statements, params)
    }

    fn parse_update_where_clause(sql: &str) -> Expr {
        let mut statements = parse_sql_statements(sql).expect("parse sql");
        let statement = statements.remove(0);
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };
        update.selection.expect("where clause")
    }

    fn update_validation_plan(where_clause: Expr, version_id: &str) -> UpdateValidationPlan {
        UpdateValidationPlan {
            delete: false,
            table: untracked_live_table_name("lix_active_version"),
            where_clause: Some(where_clause),
            snapshot_content: Some(json!({
                "id": "main",
                "version_id": version_id
            })),
            snapshot_patch: None,
        }
    }
}
