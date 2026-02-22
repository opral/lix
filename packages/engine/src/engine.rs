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
use crate::plugin::manifest::parse_plugin_manifest_json;
use crate::plugin::types::{InstalledPlugin, PluginManifest};
use crate::schema_registry::register_schema_sql_statements;
use crate::sql::{
    active_version_from_mutations, active_version_from_update_validations,
    build_delete_followup_sql, build_update_followup_sql, escape_sql_string,
    is_query_only_statements, preprocess_sql, should_invalidate_installed_plugins_cache_for_sql,
    should_invalidate_installed_plugins_cache_for_statements,
    should_refresh_file_cache_for_statements, DetectedFileDomainChange,
    MutationRow, PostprocessPlan,
};
use crate::state_commit_stream::{
    state_commit_stream_changes_from_mutations, StateCommitStream, StateCommitStreamBus,
    StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::validation::{validate_inserts, validate_updates, SchemaCache};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, parse_active_version_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id, DEFAULT_ACTIVE_VERSION_NAME, GLOBAL_VERSION_ID,
};
use crate::WasmRuntime;
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};
use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

#[path = "execute/mod.rs"]
mod execute;
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

pub use crate::boot::{boot, BootAccount, BootArgs, BootKeyValue};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

pub type EngineTransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, LixError>> + 'a>>;

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    cel_evaluator: CelEvaluator,
    schema_cache: SchemaCache,
    boot_key_values: Vec<BootKeyValue>,
    boot_active_account: Option<BootAccount>,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
    active_version_id: RwLock<String>,
    access_to_internal: bool,
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, crate::plugin::runtime::CachedPluginComponent>>,
    state_commit_stream_bus: Arc<StateCommitStreamBus>,
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
}

impl<'a> EngineTransaction<'a> {
    pub async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        if !self.engine.access_to_internal {
            reject_internal_table_access(sql)?;
        }
        self.execute_with_access(sql, params).await
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.execute_with_access(sql, params).await
    }

    async fn execute_with_access(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        let previous_active_version_id = self.active_version_id.clone();
        let transaction = self.transaction.as_mut().ok_or_else(|| LixError {
            message: "transaction is no longer active".to_string(),
        })?;
        let result = self
            .engine
            .execute_with_options_in_transaction(
                transaction.as_mut(),
                sql,
                params,
                &self.options,
                &mut self.active_version_id,
                false,
                &mut self.pending_state_commit_stream_changes,
            )
            .await?;
        if self.active_version_id != previous_active_version_id {
            self.active_version_changed = true;
        }
        if should_invalidate_installed_plugins_cache_for_sql(sql, self.engine.dialect()) {
            self.installed_plugins_cache_invalidation_pending = true;
        }
        Ok(result)
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let transaction = self.transaction.take().ok_or_else(|| LixError {
            message: "transaction is no longer active".to_string(),
        })?;
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
            message: "transaction is no longer active".to_string(),
        })?;
        transaction.rollback().await
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}

struct TransactionBackendAdapter<'a> {
    dialect: crate::SqlDialect,
    transaction: Mutex<*mut (dyn LixTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

pub(crate) struct CollectedExecutionSideEffects {
    pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pending_file_delete_targets: BTreeSet<(String, String)>,
    detected_file_domain_changes: Vec<DetectedFileDomainChange>,
    untracked_filesystem_update_domain_changes: Vec<DetectedFileDomainChange>,
}

fn reject_internal_table_access(sql: &str) -> Result<(), LixError> {
    if sql.to_ascii_lowercase().contains("lix_internal_") {
        return Err(LixError {
            message:
                "queries against lix_internal_* tables are not allowed; use public lix_* views"
                    .to_string(),
        });
    }
    Ok(())
}

// SAFETY: `TransactionBackendAdapter` is only used inside a single async execution flow.
// Internal access to the raw transaction pointer is serialized with a mutex.
unsafe impl<'a> Send for TransactionBackendAdapter<'a> {}
// SAFETY: see `Send` impl above.
unsafe impl<'a> Sync for TransactionBackendAdapter<'a> {}

impl<'a> TransactionBackendAdapter<'a> {
    fn new(transaction: &'a mut dyn LixTransaction) -> Self {
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
            message: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        Err(LixError {
            message: "nested transactions are not supported".to_string(),
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
            active_version_id: RwLock::new(GLOBAL_VERSION_ID.to_string()),
            access_to_internal: args.access_to_internal,
            installed_plugins_cache: RwLock::new(None),
            plugin_component_cache: Mutex::new(BTreeMap::new()),
            state_commit_stream_bus: Arc::new(StateCommitStreamBus::default()),
        }
    }

    pub(crate) fn dialect(&self) -> crate::SqlDialect {
        self.backend.dialect()
    }
}

fn file_name_and_extension_from_path(path: &str) -> Option<(String, Option<String>)> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let file_name = trimmed.rsplit('/').next()?;
    if file_name.is_empty() {
        return None;
    }
    let last_dot = file_name.rfind('.');
    let (name, extension) = match last_dot {
        Some(index) if index > 0 => {
            let name = file_name[..index].to_string();
            let extension = file_name[index + 1..].to_string();
            let extension = if extension.is_empty() {
                None
            } else {
                Some(extension)
            };
            (name, extension)
        }
        _ => (file_name.to_string(), None),
    };
    if name.is_empty() {
        return None;
    }
    Some((name, extension))
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

fn file_descriptor_cache_eviction_targets(mutations: &[MutationRow]) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|mutation| mutation.snapshot_content.is_none())
        .map(|mutation| (mutation.entity_id.clone(), mutation.version_id.clone()))
        .collect()
}

fn should_run_binary_cas_gc(
    mutations: &[MutationRow],
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> bool {
    mutations
        .iter()
        .any(|mutation| !mutation.untracked && mutation.schema_key == BINARY_BLOB_REF_SCHEMA_KEY)
        || detected_file_domain_changes
            .iter()
            .any(|change| change.schema_key == BINARY_BLOB_REF_SCHEMA_KEY)
}

fn collect_postprocess_file_cache_targets(
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
                message: "postprocess file cache refresh expected file_id column".to_string(),
            });
        };
        let Some(version_id) = row.get(2) else {
            return Err(LixError {
                message: "postprocess file cache refresh expected version_id column".to_string(),
            });
        };
        let Value::Text(file_id) = file_id else {
            return Err(LixError {
                message: format!(
                    "postprocess file cache refresh expected text file_id, got {file_id:?}"
                ),
            });
        };
        let Value::Text(version_id) = version_id else {
            return Err(LixError {
                message: format!(
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

trait DedupableDetectedFileChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str);
}

impl DedupableDetectedFileChange for crate::plugin::runtime::DetectedFileChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.file_id,
            &self.version_id,
            &self.schema_key,
            &self.entity_id,
        )
    }
}

impl DedupableDetectedFileChange for DetectedFileDomainChange {
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
    T: DedupableDetectedFileChange + Clone,
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

fn dedupe_detected_file_changes(
    changes: &[crate::plugin::runtime::DetectedFileChange],
) -> Vec<crate::plugin::runtime::DetectedFileChange> {
    dedupe_detected_changes(changes)
}

fn dedupe_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<DetectedFileDomainChange> {
    dedupe_detected_changes(changes)
}

fn detected_file_domain_changes_from_detected_file_changes(
    changes: &[crate::plugin::runtime::DetectedFileChange],
    writer_key: Option<&str>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .iter()
        .map(|change| DetectedFileDomainChange {
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            version_id: change.version_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: None,
            writer_key: writer_key.map(ToString::to_string),
        })
        .collect()
}

#[cfg(test)]
fn detected_file_domain_changes_with_writer_key(
    changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .iter()
        .map(|change| {
            let mut next = change.clone();
            next.writer_key = writer_key.map(ToString::to_string);
            next
        })
        .collect()
}

fn builtin_schema_entity_id(schema: &JsonValue) -> Result<String, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "builtin schema must define string x-lix-key".to_string(),
        })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "builtin schema must define string x-lix-version".to_string(),
        })?;

    Ok(format!("{schema_key}~{schema_version}"))
}

#[cfg(test)]
mod tests {
    use super::{
        boot, detected_file_domain_changes_from_detected_file_changes,
        detected_file_domain_changes_with_writer_key, file_descriptor_cache_eviction_targets,
        BootArgs, ExecuteOptions,
    };
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::plugin::types::{InstalledPlugin, PluginRuntime};
    use crate::sql::{
        active_version_from_update_validations, extract_explicit_transaction_script,
        file_history_read_materialization_required_for_sql,
        file_read_materialization_scope_for_sql, is_query_only_sql, parse_sql_statements,
        should_invalidate_installed_plugins_cache_for_sql, should_refresh_file_cache_for_sql,
        FileReadMaterializationScope, MutationRow,
    };
    use crate::sql::{DetectedFileDomainChange, UpdateValidationPlan};
    use crate::version::active_version_schema_key;
    use crate::{
        LixError, NoopWasmRuntime, QueryResult, SnapshotChunkReader, Value, WasmComponentInstance,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Expr, Statement};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    struct TestBackend {
        dialect: SqlDialect,
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
        active_version_snapshot: Arc<RwLock<String>>,
        restored_active_version_snapshot: String,
        transaction_exec_log: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
        file_descriptor_execution_rows: Arc<RwLock<Vec<(String, String, Option<String>)>>>,
    }

    struct TestTransaction {
        dialect: SqlDialect,
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
        transaction_exec_log: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
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
            self.dialect
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
                });
            }
            if sql.contains("FROM lix_internal_state_vtable")
                && sql.contains("schema_key = 'lix_file_descriptor'")
                && sql.contains("untracked = 0")
            {
                let rows = self
                    .file_descriptor_execution_rows
                    .read()
                    .expect("file_descriptor_execution_rows lock")
                    .iter()
                    .map(|(entity_id, version_id, snapshot_content)| {
                        vec![
                            Value::Text(entity_id.clone()),
                            Value::Text(version_id.clone()),
                            match snapshot_content {
                                Some(snapshot_content) => Value::Text(snapshot_content.clone()),
                                None => Value::Null,
                            },
                        ]
                    })
                    .collect();
                return Ok(QueryResult { rows });
            }
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(TestTransaction {
                dialect: self.dialect,
                commit_called: Arc::clone(&self.commit_called),
                rollback_called: Arc::clone(&self.rollback_called),
                transaction_exec_log: Arc::clone(&self.transaction_exec_log),
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
            self.dialect
        }

        async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            self.transaction_exec_log
                .lock()
                .expect("transaction_exec_log lock")
                .push((sql.to_string(), params.to_vec()));
            Ok(QueryResult { rows: Vec::new() })
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
            "INSERT INTO lix_state (entity_id, schema_key, file_id, version_id, snapshot_content) VALUES ('/x', 'json_pointer', 'f', 'v', '{}')"
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

    #[test]
    fn plugin_cache_invalidation_detects_internal_plugin_mutations_only() {
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "INSERT INTO lix_internal_plugin (key, runtime, api_version, match_path_glob, entry, manifest_json, wasm, created_at, updated_at) VALUES ('k', 'wasm-component-v1', '0.1.0', '*.json', 'plugin.wasm', '{}', X'00', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z')",
            SqlDialect::Sqlite,
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "UPDATE lix_internal_plugin SET match_path_glob = '*.md' WHERE key = 'k'",
            SqlDialect::Sqlite,
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "DELETE FROM lix_internal_plugin WHERE key = 'k'",
            SqlDialect::Sqlite,
        ));
        assert!(!should_invalidate_installed_plugins_cache_for_sql(
            "SELECT * FROM lix_internal_plugin WHERE key = 'k'",
            SqlDialect::Sqlite,
        ));
    }

    #[tokio::test]
    async fn unified_execute_multi_statement_runs_inside_single_transaction() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        engine
            .execute("SELECT 1; SELECT 2;", &[], ExecuteOptions::default())
            .await
            .expect("multi-statement execution should succeed");

        assert!(commit_called.load(Ordering::SeqCst));
        assert!(!rollback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn side_effect_collection_is_derived_from_execution_state_for_mutated_targets() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(vec![
                    (
                        "file-placeholder-state".to_string(),
                        "global".to_string(),
                        Some("{\"path\":\"/from-execution-state.txt\"}".to_string()),
                    ),
                    ("deleted-file".to_string(), "global".to_string(), None),
                ])),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let side_effects = engine
            .collect_execution_side_effects_with_backend_from_mutations(
                engine.backend.as_ref(),
                &[
                    MutationRow {
                        entity_id: "file-placeholder-state".to_string(),
                        schema_key: "lix_file_descriptor".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "global".to_string(),
                        plugin_key: "lix".to_string(),
                        snapshot_content: Some(serde_json::json!({
                            "path": "/from-mutation-plan.txt"
                        })),
                        untracked: false,
                    },
                    MutationRow {
                        entity_id: "deleted-file".to_string(),
                        schema_key: "lix_file_descriptor".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        version_id: "global".to_string(),
                        plugin_key: "lix".to_string(),
                        snapshot_content: Some(serde_json::json!({
                            "path": "/from-mutation-plan-delete-ignored.txt"
                        })),
                        untracked: false,
                    },
                ],
                None,
            )
            .await
            .expect("side-effect collection should be derived from executed descriptor state");

        assert_eq!(side_effects.pending_file_writes.len(), 2);
        assert!(side_effects.pending_file_writes.iter().any(|write| {
            write.file_id == "file-placeholder-state"
                && write.version_id == "global"
                && write.after_path.as_deref() == Some("/from-execution-state.txt")
                && !write.data_is_authoritative
        }));
        assert!(side_effects.pending_file_writes.iter().any(|write| {
            write.file_id == "deleted-file"
                && write.version_id == "global"
                && write.after_path.is_none()
                && write.data_is_authoritative
        }));
        assert!(side_effects
            .pending_file_delete_targets
            .contains(&("deleted-file".to_string(), "global".to_string())));
        assert!(side_effects
            .untracked_filesystem_update_domain_changes
            .is_empty());
    }

    #[tokio::test]
    async fn filesystem_update_side_effects_are_derived_from_update_returning_rows() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
            Value::Text("plugin-1".to_string()),
            Value::Text("1".to_string()),
            Value::Text("{\"path\":\"/docs/archive/file.json\"}".to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-01-01T00:00:00.000Z".to_string()),
        ]];
        let (tracked, untracked) = engine
            .collect_filesystem_update_detected_file_domain_changes_from_update_rows(
                engine.backend.as_ref(),
                "lix_file_descriptor",
                &rows,
                Some("writer-1"),
            )
            .await
            .expect("filesystem update row side-effects should succeed");

        assert_eq!(tracked.len(), 2);
        assert!(tracked
            .iter()
            .all(|change| change.schema_key == "lix_directory_descriptor"));
        assert!(tracked
            .iter()
            .all(|change| change.version_id == "version-1"));
        assert!(tracked
            .iter()
            .all(|change| change.writer_key.as_deref() == Some("writer-1")));
        assert!(untracked.is_empty());
    }

    #[tokio::test]
    async fn filesystem_update_pending_writes_are_derived_from_update_returning_rows() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
            Value::Text("plugin-1".to_string()),
            Value::Text("1".to_string()),
            Value::Text("{\"path\":\"/docs/report.md\"}".to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-01-01T00:00:00.000Z".to_string()),
        ]];
        let writes = engine
            .collect_filesystem_update_pending_file_writes_from_update_rows(
                engine.backend.as_ref(),
                "lix_file_descriptor",
                &rows,
            )
            .await
            .expect("filesystem update pending writes should be derivable from rows");

        assert_eq!(writes.len(), 1);
        let write = &writes[0];
        assert_eq!(write.file_id, "file-1");
        assert_eq!(write.version_id, "version-1");
        assert_eq!(write.after_path.as_deref(), Some("/docs/report.md"));
        assert!(!write.data_is_authoritative);
    }

    #[tokio::test]
    async fn filesystem_update_data_pending_writes_are_derived_from_rows_and_plan() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
            Value::Text("plugin-1".to_string()),
            Value::Text("1".to_string()),
            Value::Text("{\"path\":\"/docs/report.md\"}".to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-01-01T00:00:00.000Z".to_string()),
        ]];
        let file_data_assignment = crate::sql::FileDataAssignmentPlan::Uniform(vec![1, 2, 3, 4]);
        let writes = engine
            .collect_filesystem_update_data_pending_file_writes_from_rows(
                engine.backend.as_ref(),
                "lix_file_descriptor",
                Some(&file_data_assignment),
                &rows,
            )
            .await
            .expect("filesystem data writes should be derivable from rows and plan");

        assert_eq!(writes.len(), 1);
        let write = &writes[0];
        assert_eq!(write.file_id, "file-1");
        assert_eq!(write.version_id, "version-1");
        assert_eq!(write.after_path.as_deref(), Some("/docs/report.md"));
        assert!(write.data_is_authoritative);
        assert_eq!(write.after_data, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn filesystem_update_data_pending_writes_apply_by_file_id_plan() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
            Value::Text("plugin-1".to_string()),
            Value::Text("1".to_string()),
            Value::Text("{\"path\":\"/docs/report.md\"}".to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-01-01T00:00:00.000Z".to_string()),
        ]];
        let file_data_assignment = crate::sql::FileDataAssignmentPlan::ByFileId(
            [("file-1".to_string(), vec![9, 8, 7])]
                .into_iter()
                .collect(),
        );
        let writes = engine
            .collect_filesystem_update_data_pending_file_writes_from_rows(
                engine.backend.as_ref(),
                "lix_file_descriptor",
                Some(&file_data_assignment),
                &rows,
            )
            .await
            .expect("filesystem case data writes should derive from by-file-id plan");

        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].file_id, "file-1");
        assert!(writes[0].data_is_authoritative);
        assert_eq!(writes[0].after_data, vec![9, 8, 7]);
    }

    #[tokio::test]
    async fn filesystem_delete_side_effects_are_derived_from_delete_returning_rows() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
        ]];
        let (writes, delete_targets) = engine
            .collect_filesystem_delete_side_effects_from_delete_rows(
                engine.backend.as_ref(),
                "lix_file_descriptor",
                &rows,
            )
            .await
            .expect("filesystem delete side-effects should be derivable from rows");

        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].file_id, "file-1");
        assert_eq!(writes[0].version_id, "version-1");
        assert!(writes[0].after_path.is_none());
        assert!(writes[0].data_is_authoritative);
        assert!(delete_targets.contains(&("file-1".to_string(), "version-1".to_string())));
    }

    #[tokio::test]
    async fn unified_execute_sqlite_keeps_placeholder_progression_across_script_statements() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log: Arc::clone(&transaction_exec_log),
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        engine
            .execute(
                "BEGIN; SELECT ?3; SELECT ?; COMMIT;",
                &[
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                    Value::Integer(4),
                ],
                ExecuteOptions::default(),
            )
            .await
            .expect("script execution should succeed");

        let executed_params = transaction_exec_log
            .lock()
            .expect("transaction_exec_log lock")
            .iter()
            .map(|(_, params)| params.clone())
            .collect::<Vec<_>>();
        assert!(
            executed_params.contains(&vec![Value::Integer(3)]),
            "first statement should bind ?3 to the third provided value"
        );
        assert!(
            executed_params.contains(&vec![Value::Integer(4)]),
            "second statement should continue ordinal progression for anonymous placeholders"
        );
    }

    #[tokio::test]
    async fn unified_execute_postgres_keeps_explicit_placeholder_binding_per_statement() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Postgres,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log: Arc::clone(&transaction_exec_log),
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        engine
            .execute(
                "BEGIN; SELECT $2; SELECT $1; COMMIT;",
                &[Value::Integer(10), Value::Integer(20)],
                ExecuteOptions::default(),
            )
            .await
            .expect("script execution should succeed");

        let executed_params = transaction_exec_log
            .lock()
            .expect("transaction_exec_log lock")
            .iter()
            .map(|(_, params)| params.clone())
            .collect::<Vec<_>>();
        assert!(
            executed_params.contains(&vec![Value::Integer(20)]),
            "first statement should bind $2 to the second provided value"
        );
        assert!(
            executed_params.contains(&vec![Value::Integer(10)]),
            "second statement should bind $1 to the first provided value"
        );
    }

    #[tokio::test]
    async fn unified_execute_sqlite_deduplicates_explicit_placeholders_per_statement() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log: Arc::clone(&transaction_exec_log),
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
            }),
            Arc::new(NoopWasmRuntime),
        ));

        engine
            .execute(
                "BEGIN; SELECT ?1, ?1; SELECT ?; COMMIT;",
                &[Value::Integer(10), Value::Integer(20)],
                ExecuteOptions::default(),
            )
            .await
            .expect("script execution should succeed");

        let executed_params = transaction_exec_log
            .lock()
            .expect("transaction_exec_log lock")
            .iter()
            .map(|(_, params)| params.clone())
            .collect::<Vec<_>>();
        assert!(
            executed_params.contains(&vec![Value::Integer(10)]),
            "first statement should deduplicate ?1 references"
        );
        assert!(
            executed_params.contains(&vec![Value::Integer(20)]),
            "second statement should continue from the next ordinal after ?1"
        );
    }

    #[tokio::test]
    async fn transaction_plugin_cache_invalidation_happens_after_commit() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
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
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
                active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json(
                    "global",
                ))),
                restored_active_version_snapshot: active_version_snapshot_json("global"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
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
        let transaction_exec_log = Arc::new(Mutex::new(Vec::new()));
        let active_version_snapshot = Arc::new(RwLock::new(active_version_snapshot_json("before")));
        let engine = boot(BootArgs::new(
            Box::new(TestBackend {
                dialect: SqlDialect::Sqlite,
                commit_called,
                rollback_called,
                active_version_snapshot: Arc::clone(&active_version_snapshot),
                restored_active_version_snapshot: active_version_snapshot_json("after"),
                transaction_exec_log,
                file_descriptor_execution_rows: Arc::new(RwLock::new(Vec::new())),
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
                .as_str(),
            "after"
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
    fn file_read_materialization_scope_detects_insert_select_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file WHERE id = 'file-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_insert_select_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file_by_version \
             WHERE id = 'file-a' AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_projection_subquery_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT (\
                SELECT data FROM lix_file_by_version \
                WHERE id = 'file-a' AND lixcol_version_id = 'version-a'\
             ) AS payload",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_where_exists_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT 1 \
             WHERE EXISTS (\
                SELECT 1 FROM lix_file WHERE id = 'file-a'\
             )",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_join_on_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT t.id \
             FROM some_table t \
             LEFT JOIN other_table o \
               ON EXISTS (SELECT 1 FROM lix_file WHERE id = 'file-a')",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_update_where_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE some_table \
             SET payload = 'x' \
             WHERE id IN (SELECT id FROM lix_file WHERE id = 'file-a')",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_ignores_update_target_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE id = 'file-a'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn file_read_materialization_scope_ignores_update_target_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file_by_version \
             SET path = '/renamed.json' \
             WHERE id = 'file-a' \
               AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn regression_update_target_lix_file_with_data_predicate_requires_active_materialization_scope()
    {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE data IS NOT NULL",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_by_version_with_data_predicate_requires_all_versions_scope(
    ) {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file_by_version \
             SET path = '/renamed.json' \
             WHERE data IS NOT NULL \
               AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn regression_update_target_lix_file_string_literal_data_does_not_trigger_materialization() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE metadata = 'data'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn regression_update_target_lix_file_exists_subquery_data_requires_active_materialization_scope(
    ) {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE EXISTS (SELECT 1 WHERE data IS NOT NULL)",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_case_data_requires_active_materialization_scope() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE CASE WHEN data IS NULL THEN 0 ELSE 1 END = 1",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_tuple_data_requires_active_materialization_scope() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE (data, id) IN (('x', 'file-a'))",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn detected_file_domain_changes_apply_writer_key_fallback() {
        let detected = vec![crate::plugin::runtime::DetectedFileChange {
            entity_id: "entity-1".to_string(),
            schema_key: "schema-1".to_string(),
            schema_version: "1.0".to_string(),
            file_id: "file-1".to_string(),
            version_id: "version-1".to_string(),
            plugin_key: "plugin-1".to_string(),
            snapshot_content: Some("{\"a\":1}".to_string()),
        }];

        let with_writer_key =
            detected_file_domain_changes_from_detected_file_changes(&detected, Some("writer-123"));
        assert_eq!(with_writer_key[0].writer_key.as_deref(), Some("writer-123"));
    }

    #[test]
    fn detected_file_domain_changes_writer_key_is_overwritten_by_execution_writer() {
        let detected = vec![DetectedFileDomainChange {
            entity_id: "entity-1".to_string(),
            schema_key: "schema-1".to_string(),
            schema_version: "1.0".to_string(),
            file_id: "file-1".to_string(),
            version_id: "version-1".to_string(),
            plugin_key: "plugin-1".to_string(),
            snapshot_content: Some("{\"a\":1}".to_string()),
            metadata: None,
            writer_key: Some("writer-stale".to_string()),
        }];

        let with_writer_key = detected_file_domain_changes_with_writer_key(&detected, None);
        assert_eq!(with_writer_key[0].writer_key, None);

        let with_writer_key =
            detected_file_domain_changes_with_writer_key(&detected, Some("writer-current"));
        assert_eq!(
            with_writer_key[0].writer_key.as_deref(),
            Some("writer-current")
        );
    }

    #[test]
    fn file_read_materialization_scope_detects_delete_where_subquery_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "DELETE FROM some_table \
             WHERE EXISTS (\
                 SELECT 1 FROM lix_file_by_version \
                 WHERE id = 'file-a' AND lixcol_version_id = 'version-a'\
             )",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
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
    fn descriptor_delete_targets_cache_eviction_by_entity_id_and_version_id() {
        let targets = file_descriptor_cache_eviction_targets(&[
            MutationRow {
                entity_id: "file-a".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: false,
            },
            MutationRow {
                entity_id: "ignored-dir".to_string(),
                schema_key: "lix_directory_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: false,
            },
            MutationRow {
                entity_id: "ignored-untracked".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: true,
            },
        ]);

        assert_eq!(targets.len(), 1);
        assert!(targets.contains(&("file-a".to_string(), "version-a".to_string())));
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
