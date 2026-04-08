use async_trait::async_trait;
use crate::contracts::surface::SurfaceRegistry;
use crate::projections::ProjectionRegistry;
use crate::runtime::deterministic_mode::global_deterministic_settings_storage_scope;
use crate::runtime::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::runtime::functions::SharedFunctionProvider;
use crate::runtime::streams::{StateCommitStream, StateCommitStreamFilter};
use crate::runtime::wasm::WasmRuntime;
use crate::runtime::Runtime;
use crate::{LixBackend, LixError};
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub use crate::boot::{boot, BootArgs, BootKeyValue};

pub struct Engine {
    runtime: Arc<Runtime>,
    boot_key_values: Vec<BootKeyValue>,
}

struct EngineSessionServices {
    engine: Arc<Engine>,
}

impl EngineSessionServices {
    fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[async_trait(?Send)]
impl crate::session::collaborators::SessionServices for EngineSessionServices {
    async fn ensure_initialized(&self) -> Result<(), LixError> {
        if !self.engine.is_initialized().await? {
            return Err(crate::common::errors::not_initialized_error());
        }
        Ok(())
    }

    fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.engine.backend()
    }

    fn access_to_internal(&self) -> bool {
        self.engine.runtime().access_to_internal()
    }

    async fn begin_write_unit(&self) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
        self.engine.runtime().begin_write_unit().await
    }

    async fn begin_read_unit(
        &self,
        mode: crate::TransactionMode,
    ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
        self.engine.runtime().begin_read_unit(mode).await
    }

    fn public_surface_registry(&self) -> SurfaceRegistry {
        self.engine.public_surface_registry()
    }

    fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.engine.install_public_surface_registry(registry);
    }

    async fn load_public_surface_registry(&self) -> Result<SurfaceRegistry, LixError> {
        self.engine.load_public_surface_registry().await
    }

    async fn export_image(
        &self,
        writer: &mut dyn crate::image::ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.engine.backend().export_image(writer).await
    }

    fn projection_registry(&self) -> &ProjectionRegistry {
        self.engine.projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn crate::contracts::traits::CompiledSchemaCache {
        self.engine.runtime().schema_cache()
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
        self.engine
            .prepare_runtime_functions_with_backend(backend)
            .await
    }

    fn state_commit_stream(
        &self,
        filter: crate::runtime::streams::StateCommitStreamFilter,
    ) -> crate::runtime::streams::StateCommitStream {
        self.engine.state_commit_stream(filter)
    }

    fn emit_state_commit_stream_changes(
        &self,
        changes: Vec<crate::runtime::streams::StateCommitStreamChange>,
    ) {
        self.engine
            .runtime()
            .emit_state_commit_stream_changes(changes);
    }

    fn invalidate_deterministic_settings_cache(&self) {
        self.engine
            .runtime()
            .invalidate_deterministic_settings_cache();
    }

    fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        self.engine.invalidate_installed_plugins_cache()
    }
}

#[async_trait(?Send)]
impl crate::session::collaborators::WriteExecutionCollaborators for Engine {
    fn projection_registry(&self) -> &ProjectionRegistry {
        self.projection_registry().as_ref()
    }

    fn compiled_schema_cache(&self) -> &dyn crate::contracts::traits::CompiledSchemaCache {
        self.runtime().schema_cache()
    }

    fn sql_preparation_seed<'a>(
        &'a self,
        functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
        surface_registry: &'a SurfaceRegistry,
    ) -> crate::sql::prepare::SqlPreparationSeed<'a> {
        crate::sql::prepare::SqlPreparationSeed {
            dialect: self.backend().dialect(),
            functions: crate::contracts::functions::clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    async fn prepare_execution_runtime_state(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<crate::runtime::execution_state::ExecutionRuntimeState, LixError> {
        let (settings, functions) = self.prepare_runtime_functions_with_backend(backend).await?;
        Ok(crate::runtime::execution_state::ExecutionRuntimeState::from_prepared_parts(
            settings, functions,
        ))
    }
}

#[async_trait(?Send)]
impl crate::execution::write::WriteExecutionBindings for Engine {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        pending_view: Option<&dyn crate::contracts::traits::PendingView>,
        public_read: &crate::contracts::artifacts::PreparedPublicReadArtifact,
    ) -> Result<crate::QueryResult, LixError> {
        crate::session::write_execution_bindings::execute_prepared_public_read_with_registry(
            self.projection_registry().as_ref(),
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
        functions: &SharedFunctionProvider<
            Box<dyn crate::contracts::functions::LixFunctionProvider + Send>,
        >,
    ) -> Result<(), LixError> {
        crate::session::write_execution_bindings::persist_runtime_sequence(
            transaction,
            functions,
        )
        .await
    }

    async fn execute_public_tracked_append_txn_with_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        unit: &crate::execution::write::buffered::TrackedTxnUnit,
        pending_commit_session: Option<
            &mut Option<crate::contracts::artifacts::PendingPublicCommitSession>,
        >,
    ) -> Result<crate::execution::write::TrackedCommitExecutionOutcome, LixError> {
        crate::session::write_execution_bindings::execute_public_tracked_append(
            transaction,
            unit,
            pending_commit_session,
        )
        .await
    }

    async fn apply_writer_key_annotations_in_transaction(
        &self,
        transaction: &mut dyn crate::LixBackendTransaction,
        annotations: &std::collections::BTreeMap<
            crate::contracts::artifacts::RowIdentity,
            Option<String>,
        >,
    ) -> Result<(), LixError> {
        let mut executor = &mut *transaction;
        crate::schema::annotations::writer_key::apply_workspace_writer_key_annotations_with_executor(
            &mut executor,
            annotations,
        )
        .await
    }
}

impl Engine {
    pub(crate) fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub(crate) fn backend(&self) -> &Arc<dyn LixBackend + Send + Sync> {
        self.runtime.backend()
    }

    pub async fn open_session(self: &Arc<Self>) -> Result<crate::Session, LixError> {
        crate::Session::open_workspace(crate::session::collaborators::SessionCollaborators::new(
            self.session_services(),
        ))
        .await
    }

    pub(crate) fn session_services(
        self: &Arc<Self>,
    ) -> Arc<dyn crate::session::collaborators::SessionServices> {
        Arc::new(EngineSessionServices::new(Arc::clone(self)))
    }

    pub fn wasm_runtime(&self) -> Arc<dyn WasmRuntime> {
        self.runtime.wasm_runtime()
    }

    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> StateCommitStream {
        self.runtime.state_commit_stream(filter)
    }

    pub(crate) fn deterministic_boot_pending(&self) -> bool {
        self.runtime.deterministic_boot_pending()
    }

    pub(crate) fn boot_key_values(&self) -> &[BootKeyValue] {
        &self.boot_key_values
    }

    pub(crate) fn public_surface_registry(&self) -> crate::contracts::surface::SurfaceRegistry {
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

    pub(crate) fn projection_registry(&self) -> &Arc<ProjectionRegistry> {
        self.runtime.projection_registry()
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
        let projection_registry =
            Arc::new(crate::projections::builtin_projection_registry().clone());
        Self {
            runtime: Arc::new(Runtime::new(
                args.backend,
                args.wasm_runtime,
                args.access_to_internal,
                boot_deterministic_settings,
                crate::schema::build_builtin_surface_registry(),
                projection_registry,
            )),
            boot_key_values: args.key_values,
        }
    }
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
    use crate::runtime::wasm::NoopWasmRuntime;
    use crate::sql::analysis::state_resolution::canonical::is_query_only_statements;
    use crate::sql::binder::{advance_placeholder_state_for_statement_ast, bind_sql_with_state};
    use crate::sql::internal::script::extract_explicit_transaction_script_from_statements;
    use crate::sql::optimizer::optimize_state_resolution;
    use crate::sql::parser::parse_sql_statements;
    use crate::sql::parser::placeholders::PlaceholderState;
    use crate::TransactionMode;
    use crate::{
        ExecuteOptions, LixBackend, LixBackendTransaction, LixError, QueryResult, Session,
        SqlDialect, Value,
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
                    let engine = Arc::new(boot(BootArgs::new(
                        Box::new(TestBackend {
                            commit_called,
                            rollback_called,
                        }),
                        Arc::new(NoopWasmRuntime),
                    )));
                    let session = Session::new_for_test(
                        crate::session::collaborators::SessionCollaborators::new(
                            engine.session_services(),
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
        let engine = Arc::new(boot(BootArgs::new(
            Box::new(TestBackend {
                commit_called: Arc::clone(&commit_called),
                rollback_called: Arc::clone(&rollback_called),
            }),
            Arc::new(NoopWasmRuntime),
        )));
        let session = Session::new_for_test(
            crate::session::collaborators::SessionCollaborators::new(engine.session_services()),
            "version-test".to_string(),
            Vec::new(),
        );

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
        let session = Session::new_for_test(
            crate::session::collaborators::SessionCollaborators::new(engine.session_services()),
            "version-test".to_string(),
            Vec::new(),
        );

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
