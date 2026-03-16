use crate::cel::CelEvaluator;
use crate::deterministic_mode::{deterministic_mode_key, DeterministicSettings};
use crate::key_value::key_value_schema_key;
use crate::plugin::types::InstalledPlugin;
use crate::sql::execution::transaction_session::PublicSqlSessionState;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::state::stream::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::state::validation::SchemaCache;
use crate::WasmRuntime;
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};
use futures_util::lock::Mutex as AsyncMutex;
use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectNamePart, Statement, TableFactor, TableObject};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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
    active_version_id: RwLock<Option<String>>,
    public_surface_registry: RwLock<SurfaceRegistry>,
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, crate::plugin::runtime::CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
    pub(crate) public_sql_state: AsyncMutex<PublicSqlSessionState>,
    pub(crate) public_sql_transaction_open: AtomicBool,
    pub(crate) observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::observe::SharedObserveSource>>>>,
}

#[must_use = "EngineTransaction must be committed or rolled back"]
pub struct EngineTransaction<'a> {
    pub(crate) engine: &'a Engine,
    pub(crate) transaction: Option<Box<dyn LixTransaction + 'a>>,
    pub(crate) options: ExecuteOptions,
    pub(crate) public_surface_registry: SurfaceRegistry,
    pub(crate) active_version_id: String,
    pub(crate) active_version_changed: bool,
    pub(crate) installed_plugins_cache_invalidation_pending: bool,
    pub(crate) public_surface_registry_dirty: bool,
    pub(crate) pending_state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) observe_tick_already_emitted: bool,
    pub(crate) pending_public_commit_session:
        Option<crate::sql::execution::shared_path::PendingPublicCommitSession>,
}

impl Engine {
    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.wasm_runtime.clone()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.state_commit_stream_bus.subscribe(filter)
    }

    pub(crate) fn backend_ref(&self) -> &(dyn LixBackend + Send + Sync) {
        self.backend.as_ref()
    }

    pub(crate) fn access_to_internal(&self) -> bool {
        self.access_to_internal
    }

    pub(crate) fn ensure_no_open_public_sql_transaction(
        &self,
        operation: &str,
    ) -> Result<(), LixError> {
        if self.public_sql_transaction_open.load(Ordering::SeqCst) {
            return Err(crate::errors::operation_blocked_by_active_transaction_error(operation));
        }
        Ok(())
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

    pub(crate) fn emit_state_commit_stream_changes(&self, changes: Vec<StateCommitStreamChange>) {
        self.state_commit_stream_bus.emit(changes);
    }

    pub(crate) fn maybe_invalidate_deterministic_settings_cache(
        &self,
        mutations: &[MutationRow],
        state_commit_stream_changes: &[StateCommitStreamChange],
    ) {
        let touched = mutations.iter().any(|row| {
            row.schema_key == key_value_schema_key() && row.entity_id == deterministic_mode_key()
        }) || state_commit_stream_changes.iter().any(|change| {
            change.schema_key == key_value_schema_key()
                && change.entity_id == deterministic_mode_key()
        });

        if touched {
            self.invalidate_deterministic_settings_cache();
        }
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

pub(crate) struct TransactionBackendAdapter<'a> {
    dialect: crate::SqlDialect,
    transaction: Mutex<*mut (dyn LixTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

pub(crate) struct CollectedExecutionSideEffects {
    pub(crate) pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pub(crate) pending_file_delete_targets: BTreeSet<(String, String)>,
}

#[derive(Default)]
pub(crate) struct DeferredTransactionSideEffects {
    pub(crate) pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
}

pub(crate) fn reject_internal_table_writes(statements: &[Statement]) -> Result<(), LixError> {
    for statement in statements {
        if statement_writes_to_lix_internal_table(statement) {
            return Err(crate::errors::internal_table_access_denied_error());
        }
    }
    Ok(())
}

fn statement_writes_to_lix_internal_table(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => object_name_is_lix_internal(name),
            _ => false,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => object_name_is_lix_internal(name),
            _ => false,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            tables.iter().any(|table| match &table.relation {
                TableFactor::Table { name, .. } => object_name_is_lix_internal(name),
                _ => false,
            })
        }
        _ => false,
    }
}

fn object_name_is_lix_internal(name: &sqlparser::ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| {
            ident
                .value
                .to_ascii_lowercase()
                .starts_with("lix_internal_")
        })
        .unwrap_or(false)
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
    pub(crate) fn new(transaction: &'a mut dyn LixTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction as *mut (dyn LixTransaction + 'a)),
            _lifetime: PhantomData,
        }
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
        // SAFETY: the pointer is created from a live `&mut dyn LixTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "nested transactions are not supported".to_string(),
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
            active_version_id: RwLock::new(None),
            public_surface_registry: RwLock::new(SurfaceRegistry::with_builtin_surfaces()),
            access_to_internal: args.access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
            public_sql_state: AsyncMutex::new(PublicSqlSessionState::default()),
            public_sql_transaction_open: AtomicBool::new(false),
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

pub(crate) fn collect_postprocess_file_cache_targets(
    rows: &[Vec<Value>],
    schema_key: &str,
) -> Result<BTreeSet<(String, String)>, LixError> {
    if schema_key == FILE_DESCRIPTOR_SCHEMA_KEY || schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
        return Ok(BTreeSet::new());
    }

    let mut targets = BTreeSet::new();
    for row in rows {
        let Some(file_id) = row.get(1) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "postprocess file cache refresh expected file_id column".to_string(),
            });
        };
        let Some(version_id) = row.get(2) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "postprocess file cache refresh expected version_id column"
                    .to_string(),
            });
        };
        let Value::Text(file_id) = file_id else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "postprocess file cache refresh expected text file_id, got {file_id:?}"
                ),
            });
        };
        let Value::Text(version_id) = version_id else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "postprocess file cache refresh expected text version_id, got {version_id:?}"
                ),
            });
        };
        if file_id == "lix" {
            continue;
        }
        targets.insert((file_id.clone(), version_id.clone()));
    }

    Ok(targets)
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
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::sql::analysis::history_reads::file_history_read_materialization_required_for_statements;
    use crate::sql::analysis::state_resolution::canonical::is_query_only_statements;
    use crate::sql::analysis::state_resolution::effects::active_version_from_update_validations;
    use crate::sql::analysis::state_resolution::optimize::should_refresh_file_cache_for_statements;
    use crate::sql::ast::utils::{
        advance_placeholder_state_for_statement_ast, bind_sql_with_state, parse_sql_statements,
        PlaceholderState,
    };
    use crate::sql::execution::contracts::planned_statement::UpdateValidationPlan;
    use crate::state::internal::script::extract_explicit_transaction_script_from_statements;
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

    #[derive(Default)]
    struct PlainReadBackend {
        executed_sql: Arc<std::sync::Mutex<Vec<String>>>,
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

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(TestTransaction {
                commit_called: Arc::clone(&self.commit_called),
                rollback_called: Arc::clone(&self.rollback_called),
            }))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for TestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
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

    #[async_trait(?Send)]
    impl LixBackend for PlainReadBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql
                .lock()
                .expect("executed_sql lock")
                .push(sql.to_string());
            if sql.contains("lix_deterministic_mode")
                || sql.contains("lix_internal_live_untracked_v1")
                || sql.contains("lix_internal_registered_schema_bootstrap")
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("plain backend read should not execute preparation SQL: {sql}"),
                ));
            }
            if sql.trim() == "SELECT 1 + 1" {
                return Ok(QueryResult {
                    rows: vec![vec![Value::Integer(2)]],
                    columns: vec!["?column?".to_string()],
                });
            }
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected SQL in PlainReadBackend: {sql}"),
            ))
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "plain backend read should not begin a transaction",
            ))
        }
    }

    #[test]
    fn detects_active_version_update_with_single_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_live_untracked_v1 SET snapshot_content = 'x' WHERE schema_key = '{}' AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-single");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-single"));
    }

    #[test]
    fn detects_active_version_update_with_double_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_live_untracked_v1 SET snapshot_content = 'x' WHERE schema_key = \"{}\" AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-double");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-double"));
    }

    #[test]
    fn ignores_non_active_version_schema_key() {
        let where_clause = parse_update_where_clause(
            "UPDATE lix_internal_live_untracked_v1 SET snapshot_content = 'x' WHERE schema_key = 'other_schema' AND entity_id = 'main'",
        );
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
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{}' WHERE file_id = 'f'"
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

    #[tokio::test]
    async fn unknown_read_query_returns_unknown_table_error() {
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
    }

    #[tokio::test]
    async fn plain_backend_read_bypasses_public_preparation() {
        let backend = PlainReadBackend::default();
        let executed_sql = Arc::clone(&backend.executed_sql);
        let engine = boot(BootArgs::new(Box::new(backend), Arc::new(NoopWasmRuntime)));
        engine.set_active_version_id("version-test".to_string());

        let result = engine
            .execute("SELECT 1 + 1", &[])
            .await
            .expect("plain backend read should succeed");

        assert_eq!(result.statements[0].rows, vec![vec![Value::Integer(2)]]);
        let executed = executed_sql.lock().expect("executed_sql lock");
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0], "SELECT 1 + 1");
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
        tx.installed_plugins_cache_invalidation_pending = true;

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
        tx.installed_plugins_cache_invalidation_pending = true;
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
    fn file_history_materialization_detection_includes_insert_select_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file_history \
             WHERE id = 'file-a' \
             LIMIT 1",
        ));
    }

    #[test]
    fn file_history_materialization_detection_includes_select_where_subquery_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "SELECT 1 \
             WHERE EXISTS (\
                SELECT 1 FROM lix_file_history WHERE id = 'file-a'\
             )",
        ));
    }

    #[test]
    fn file_history_materialization_detection_includes_update_where_subquery_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "UPDATE some_table \
             SET payload = 'x' \
             WHERE EXISTS (\
                 SELECT 1 FROM lix_file_history \
                 WHERE id = 'file-a' \
             )",
        ));
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

    fn file_history_read_materialization_required_for_sql(sql: &str) -> bool {
        parse_sql_statements(sql)
            .map(|statements| {
                file_history_read_materialization_required_for_statements(&statements)
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
            kind: crate::sql::execution::contracts::planned_statement::UpdateValidationKind::Update,
            table: "lix_internal_live_untracked_v1".to_string(),
            where_clause: Some(where_clause),
            snapshot_content: Some(json!({
                "id": "main",
                "version_id": version_id
            })),
            snapshot_patch: None,
        }
    }
}
