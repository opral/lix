use crate::contracts::artifacts::{FilesystemPayloadDomainChange, MutationRow};
use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::functions::SharedFunctionProvider;
use crate::runtime::streams::{
    StateCommitStream, StateCommitStreamChange, StateCommitStreamFilter,
};
use crate::runtime::wasm::WasmRuntime;
use crate::runtime::Runtime;
use crate::{LixBackend, LixBackendTransaction, LixError};
use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectNamePart, Statement, TableFactor, TableObject};
use std::collections::BTreeMap;
use std::sync::Arc;

pub use crate::boot::{boot, BootAccount, BootArgs, BootKeyValue};

const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

pub struct Engine {
    runtime: Arc<Runtime>,
    boot_key_values: Vec<BootKeyValue>,
    boot_active_account: Option<BootAccount>,
}

impl Engine {
    pub(crate) fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.runtime.backend()
    }

    pub async fn open_workspace_session(self: &Arc<Self>) -> Result<crate::Session, LixError> {
        crate::Session::open_workspace(Arc::clone(self)).await
    }

    pub async fn open_session(
        self: &Arc<Self>,
        options: crate::OpenSessionOptions,
    ) -> Result<crate::Session, LixError> {
        let workspace = self.open_workspace_session().await?;
        workspace.open_session(options).await
    }

    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.runtime.wasm_runtime()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.runtime.state_commit_stream(filter)
    }

    pub(crate) fn wasm_runtime_ref(&self) -> &dyn WasmRuntime {
        self.runtime.wasm_runtime_ref()
    }

    pub(crate) fn deterministic_boot_pending(&self) -> bool {
        self.runtime.deterministic_boot_pending()
    }

    pub(crate) fn boot_key_values(&self) -> &[BootKeyValue] {
        &self.boot_key_values
    }

    pub(crate) fn boot_active_account(&self) -> Option<&BootAccount> {
        self.boot_active_account.as_ref()
    }

    pub(crate) fn public_surface_registry(&self) -> crate::contracts::surface::SurfaceRegistry {
        self.runtime.public_surface_registry()
    }

    pub(crate) async fn refresh_public_surface_registry(&self) -> Result<(), LixError> {
        self.runtime.refresh_public_surface_registry().await
    }

    pub(crate) fn try_mark_init_in_progress(&self) -> Result<(), LixError> {
        self.runtime.try_mark_init_in_progress()
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

    pub(crate) fn should_invalidate_deterministic_settings_cache(
        &self,
        mutations: &[MutationRow],
        state_commit_stream_changes: &[StateCommitStreamChange],
    ) -> bool {
        self.runtime
            .should_invalidate_deterministic_settings_cache(mutations, state_commit_stream_changes)
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
        self.runtime
            .prepare_runtime_functions_with_backend(backend)
            .await
    }

    pub(crate) async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        self.runtime
            .ensure_runtime_sequence_initialized_in_transaction(transaction, functions)
            .await
    }

    pub(crate) async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        settings: DeterministicSettings,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        self.runtime
            .persist_runtime_sequence_in_transaction(transaction, settings, functions)
            .await
    }
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
        || crate::contracts::surface::builtin_public_surface_names()
            .iter()
            .any(|surface| surface.eq_ignore_ascii_case(&relation))
}

#[cfg(test)]
fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::sql::parser::parse_sql(sql) else {
        return false;
    };
    crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements(&statements)
}

impl Engine {
    pub(crate) fn from_boot_args(
        args: BootArgs,
        boot_deterministic_settings: Option<DeterministicSettings>,
    ) -> Self {
        Self {
            runtime: Arc::new(Runtime::new(
                args.backend,
                args.wasm_runtime,
                args.access_to_internal,
                boot_deterministic_settings,
            )),
            boot_key_values: args.key_values,
            boot_active_account: args.active_account,
        }
    }
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
    use super::{boot, should_invalidate_installed_plugins_cache_for_sql, BootArgs};
    use crate::backend::{LixBackend, LixBackendTransaction, SqlDialect, TransactionMode};
    use crate::runtime::wasm::NoopWasmRuntime;
    use crate::sql::analysis::state_resolution::canonical::is_query_only_statements;
    use crate::sql::binder::{advance_placeholder_state_for_statement_ast, bind_sql_with_state};
    use crate::sql::internal::script::extract_explicit_transaction_script_from_statements;
    use crate::sql::optimizer::optimize_state_resolution;
    use crate::sql::parser::parse_sql_statements;
    use crate::sql::parser::placeholders::PlaceholderState;
    use crate::{ExecuteOptions, LixError, QueryResult, Session, Value};
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
                    let engine = Arc::new(boot(BootArgs::new(
                        Box::new(TestBackend {
                            commit_called,
                            rollback_called,
                        }),
                        Arc::new(NoopWasmRuntime),
                    )));
                    let session = Session::new_for_test(
                        Arc::clone(&engine),
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
        let engine = Arc::new(boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        )));
        let session =
            Session::new_for_test(Arc::clone(&engine), "version-test".to_string(), Vec::new());

        {
            let mut cache = engine
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
            engine
                .runtime()
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
            engine
                .runtime()
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
        let engine = Arc::new(boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        )));
        let session =
            Session::new_for_test(Arc::clone(&engine), "version-test".to_string(), Vec::new());

        {
            let mut cache = engine
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
            engine
                .runtime()
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
}
