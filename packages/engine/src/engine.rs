use crate::account::{
    account_file_id, account_plugin_key, account_schema_key, account_schema_version,
    account_snapshot_content, account_storage_version_id, active_account_file_id,
    active_account_plugin_key, active_account_schema_key, active_account_schema_version,
    active_account_snapshot_content, active_account_storage_version_id,
};
use crate::builtin_schema::types::LixVersionDescriptor;
use crate::builtin_schema::{builtin_schema_definition, builtin_schema_keys};
use crate::cel::CelEvaluator;
use crate::deterministic_mode::DeterministicSettings;
use crate::init::init_backend;
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::materialization::{
    MaterializationApplyReport, MaterializationPlan, MaterializationReport, MaterializationRequest,
};
use crate::plugin::types::InstalledPlugin;
use crate::state_commit_stream::{
    StateCommitStream, StateCommitStreamBus, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::validation::SchemaCache;
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, parse_active_version_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::WasmRuntime;
use crate::{ExecuteResult, LixBackend, LixError, LixTransaction, QueryResult, Value};
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

#[path = "api.rs"]
mod api;
#[path = "engine_in_transaction.rs"]
mod engine_in_transaction;
#[path = "engine_transaction.rs"]
mod engine_transaction;
#[path = "init/active_version.rs"]
mod init_active_version;
#[path = "init/bootstrap.rs"]
mod init_bootstrap;
#[path = "init/seed.rs"]
mod init_seed;
#[path = "plugin/install.rs"]
mod plugin_install;
#[path = "runtime_functions.rs"]
mod runtime_functions;
#[path = "runtime_effects.rs"]
mod runtime_effects;
#[path = "statement_scripts.rs"]
mod statement_scripts;
#[path = "query_history/mod.rs"]
pub(crate) mod query_history;
#[path = "query_semantics/mod.rs"]
pub(crate) mod query_semantics;
#[path = "query_storage/mod.rs"]
pub(crate) mod query_storage;
#[path = "sql_ast/mod.rs"]
pub(crate) mod sql_ast;

use crate::query_runtime::contracts::effects::FilesystemPayloadDomainChange;
use crate::query_runtime::contracts::planned_statement::MutationRow;
use crate::query_runtime::parse::parse_sql;
use self::query_semantics::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use self::query_storage::sql_text::escape_sql_string;

pub use crate::boot::{
    boot, init_lix, BootAccount, BootArgs, BootKeyValue, InitLixArgs, InitLixResult,
};

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

pub type EngineTransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, LixError>> + 'a>>;

pub struct Engine {
    pub(crate) backend: Box<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    pub(crate) cel_evaluator: CelEvaluator,
    pub(crate) schema_cache: SchemaCache,
    boot_key_values: Vec<BootKeyValue>,
    boot_active_account: Option<BootAccount>,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
    init_state: AtomicU8,
    active_version_id: RwLock<Option<String>>,
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, crate::plugin::runtime::CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
    pub(crate) observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::observe::SharedObserveSource>>>>,
    active_transactions: Mutex<BTreeMap<u64, EngineTransaction<'static>>>,
    next_transaction_handle_id: AtomicU64,
}

#[must_use = "EngineTransaction must be committed or rolled back"]
pub struct EngineTransaction<'a> {
    engine: &'a Engine,
    transaction: Option<Box<dyn LixTransaction + 'a>>,
    options: ExecuteOptions,
    active_version_id: String,
    active_version_changed: bool,
    installed_plugins_cache_invalidation_pending: bool,
    pending_state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pending_sql2_append_session: Option<crate::query_runtime::shared_path::PendingSql2AppendSession>,
}

impl<'a> EngineTransaction<'a> {
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        if !self.engine.access_to_internal {
            let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        self.execute_with_access(sql, params).await
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_access(sql, params).await
    }

    async fn execute_with_access(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        let previous_active_version_id = self.active_version_id.clone();
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        let transaction = self.transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        let result = if parsed_statements.len() > 1 {
            self.engine
                .execute_statement_script_with_options_in_transaction(
                    transaction.as_mut(),
                    parsed_statements.clone(),
                    params,
                    &self.options,
                    &mut self.active_version_id,
                    &mut self.pending_state_commit_stream_changes,
                    &mut self.pending_sql2_append_session,
                )
                .await?
        } else {
            let single_statement_result = self
                .engine
                .execute_with_options_in_transaction(
                    transaction.as_mut(),
                    sql,
                    params,
                    &self.options,
                    &mut self.active_version_id,
                    None,
                    false,
                    &mut self.pending_state_commit_stream_changes,
                    &mut self.pending_sql2_append_session,
                )
                .await?;
            ExecuteResult {
                statements: vec![single_statement_result],
            }
        };
        if self.active_version_id != previous_active_version_id {
            self.active_version_changed = true;
        }
        if should_invalidate_installed_plugins_cache_for_statements(&parsed_statements) {
            self.installed_plugins_cache_invalidation_pending = true;
        }
        Ok(result)
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let mut transaction = self.transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        let should_emit_observe_tick = !self.pending_state_commit_stream_changes.is_empty();
        if should_emit_observe_tick {
            self.engine
                .append_observe_tick_in_transaction(
                    transaction.as_mut(),
                    self.options.writer_key.as_deref(),
                )
                .await?;
        }
        transaction.commit().await?;
        if self.active_version_changed {
            self.engine
                .set_active_version_id(std::mem::take(&mut self.active_version_id));
        }
        if self.installed_plugins_cache_invalidation_pending {
            self.engine.invalidate_installed_plugins_cache()?;
        }
        self.engine.emit_state_commit_stream_changes(std::mem::take(
            &mut self.pending_state_commit_stream_changes,
        ));
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self.transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        transaction.rollback().await
    }
}

impl Engine {
    pub(crate) fn backend_ref(&self) -> &(dyn LixBackend + Send + Sync) {
        self.backend.as_ref()
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
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
    pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pending_file_delete_targets: BTreeSet<(String, String)>,
}

fn reject_internal_table_writes(statements: &[Statement]) -> Result<(), LixError> {
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
    crate::error_classification::normalize_sql_error_with_backend(backend, error, statements).await
}

#[cfg(test)]
fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = parse_sql(sql) else {
        return false;
    };
    should_invalidate_installed_plugins_cache_for_statements(&statements)
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
            backend: args.backend,
            wasm_runtime: args.wasm_runtime,
            cel_evaluator: CelEvaluator::new(),
            schema_cache: SchemaCache::new(),
            boot_key_values: args.key_values,
            boot_active_account: args.active_account,
            boot_deterministic_settings,
            deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
            init_state: AtomicU8::new(INIT_STATE_NOT_STARTED),
            active_version_id: RwLock::new(None),
            access_to_internal: args.access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_transactions: Mutex::new(BTreeMap::new()),
            next_transaction_handle_id: AtomicU64::new(1),
        }
    }
}

fn collapse_pending_file_writes_for_transaction(
    writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
) -> Vec<crate::filesystem::pending_file_writes::PendingFileWrite> {
    let mut collapsed =
        Vec::<crate::filesystem::pending_file_writes::PendingFileWrite>::with_capacity(
            writes.len(),
        );
    let mut index_by_key = BTreeMap::<(String, String), usize>::new();

    for write in writes {
        let key = (write.file_id.clone(), write.version_id.clone());
        if let Some(index) = index_by_key.get(&key).copied() {
            let existing = &mut collapsed[index];
            existing.after_path = write.after_path.clone();
            existing.after_data = write.after_data.clone();
            existing.data_is_authoritative =
                existing.data_is_authoritative || write.data_is_authoritative;
            if existing.before_path.is_none() {
                existing.before_path = write.before_path.clone();
            }
            if existing.before_data.is_none() {
                existing.before_data = write.before_data.clone();
            }
            continue;
        }

        index_by_key.insert(key, collapsed.len());
        collapsed.push(write.clone());
    }

    collapsed
}

fn direct_state_file_cache_refresh_targets(
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
    fn dedupe_key(&self) -> (&str, &str, &str, &str);
}

impl DedupableFilesystemPayloadChange for FilesystemPayloadDomainChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.file_id,
            &self.version_id,
            &self.schema_key,
            &self.entity_id,
        )
    }
}

fn dedupe_detected_changes<T>(changes: &[T]) -> Vec<T>
where
    T: DedupableFilesystemPayloadChange + Clone,
{
    let mut latest_by_key: BTreeMap<(&str, &str, &str, &str), usize> = BTreeMap::new();
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

fn builtin_schema_entity_id(schema: &JsonValue) -> Result<String, LixError> {
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
    use crate::engine::sql_ast::utils::{
        advance_placeholder_state_for_statement_ast, bind_sql_with_state, parse_sql_statements,
        PlaceholderState,
    };
    use crate::engine::sql_ast::walk::contains_transaction_control_statement;
    use crate::query_runtime::contracts::planned_statement::UpdateValidationPlan;
    use crate::engine::query_history::plugin_inputs::file_history_read_materialization_required_for_statements;
    use crate::internal_state::script::extract_explicit_transaction_script_from_statements;
    use crate::engine::query_semantics::state_resolution::canonical::is_query_only_statements;
    use crate::engine::query_semantics::state_resolution::effects::active_version_from_update_validations;
    use crate::engine::query_semantics::state_resolution::optimize::should_refresh_file_cache_for_statements;
    use crate::engine::Engine;
    use crate::plugin::types::{InstalledPlugin, PluginRuntime};
    use crate::version::active_version_schema_key;
    use crate::{
        ExecuteResult, LixError, NoopWasmRuntime, QueryResult, SnapshotChunkReader, Value,
        WasmComponentInstance,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Expr, Statement};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};
    struct TestBackend {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
        active_version_snapshot: Arc<RwLock<String>>,
        restored_active_version_snapshot: String,
    }

    struct TestTransaction {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
    }

    #[derive(Default)]
    struct EmptySnapshotReader;

    struct NoopWasmComponentInstance;

    fn active_version_snapshot_json(version_id: &str) -> String {
        serde_json::json!({ "id": "main", "version_id": version_id }).to_string()
    }

    #[async_trait(?Send)]
    impl LixBackend for TestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_state_untracked")
                && sql.contains("SELECT snapshot_content")
            {
                let snapshot = self
                    .active_version_snapshot
                    .read()
                    .expect("active_version_snapshot lock")
                    .clone();
                return Ok(QueryResult {
                    rows: vec![vec![Value::Text(snapshot)]],
                    columns: vec!["snapshot_content".to_string()],
                });
            }
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

        async fn restore_from_snapshot(
            &self,
            reader: &mut dyn SnapshotChunkReader,
        ) -> Result<(), LixError> {
            while reader.read_chunk().await?.is_some() {}
            let mut guard = self
                .active_version_snapshot
                .write()
                .expect("active_version_snapshot lock");
            *guard = self.restored_active_version_snapshot.clone();
            Ok(())
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
    impl SnapshotChunkReader for EmptySnapshotReader {
        async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopWasmComponentInstance {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn detects_active_version_update_with_single_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = '{}' AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-single");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-single"));
    }

    #[test]
    fn detects_active_version_update_with_double_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = \"{}\" AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-double");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-double"));
    }

    #[test]
    fn ignores_non_active_version_schema_key() {
        let where_clause = parse_update_where_clause(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = 'other_schema' AND entity_id = 'main'",
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
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
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
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
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
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
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

    #[tokio::test]
    async fn restore_from_snapshot_refreshes_active_version_and_plugin_caches() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let active_version_snapshot = Arc::new(RwLock::new(active_version_snapshot_json("before")));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::clone(&active_version_snapshot),
                restored_active_version_snapshot: active_version_snapshot_json("after"),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(vec![InstalledPlugin {
                key: "k".to_string(),
                runtime: PluginRuntime::WasmComponentV1,
                api_version: "0.1.0".to_string(),
                path_glob: "*.json".to_string(),
                content_type: None,
                entry: "plugin.wasm".to_string(),
                manifest_json: "{}".to_string(),
                wasm: vec![0],
            }]);
        }
        {
            let mut components = engine
                .plugin_component_cache
                .lock()
                .expect("plugin component cache lock");
            components.insert(
                "k".to_string(),
                crate::plugin::runtime::CachedPluginComponent {
                    wasm: vec![0],
                    instance: Arc::new(NoopWasmComponentInstance) as Arc<dyn WasmComponentInstance>,
                },
            );
        }

        let mut reader = EmptySnapshotReader;
        engine
            .restore_from_snapshot(&mut reader)
            .await
            .expect("restore_from_snapshot should succeed");

        assert_eq!(
            engine
                .active_version_id
                .read()
                .expect("active_version_id lock")
                .as_deref(),
            Some("after")
        );
        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_none(),
            "installed plugin cache should be invalidated after restore"
        );
        assert!(
            engine
                .plugin_component_cache
                .lock()
                .expect("plugin component cache lock")
                .is_empty(),
            "plugin component cache should be cleared after restore"
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
            table: "lix_internal_state_untracked".to_string(),
            where_clause: Some(where_clause),
            snapshot_content: Some(json!({
                "id": "main",
                "version_id": version_id
            })),
            snapshot_patch: None,
        }
    }
}
