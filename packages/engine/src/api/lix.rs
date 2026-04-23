//! Public `Lix` shell types and workspace-session forwarding APIs.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use super::engine::Engine;
use crate::backend::{ImageChunkReader, ImageChunkWriter, TransactionBeginMode};
use crate::live_state::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildReport, LiveStateRebuildRequest,
    ProjectionStatus,
};
use crate::streams::{StateCommitStream as PublicStateCommitStream, StateCommitStreamFilter};
use crate::wasm::WasmRuntime;
use crate::{
    AdditionalSessionOptions, CreateCheckpointResult, CreateVersionOptions, CreateVersionResult,
    ExecuteOptions, ExecuteResult, LixBackend, LixError, MergeVersionOptions, MergeVersionResult,
    ObserveEventsOwned, ObserveOptions, ObserveQuery, RedoOptions, RedoResult, Session,
    SessionTransaction, UndoOptions, UndoResult, Value, WriteReceipt,
};

use super::deterministic_settings::{
    parse_deterministic_settings_value, DeterministicSettings, DETERMINISTIC_MODE_KEY,
};

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
        }
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
    engine: Arc<Engine>,
    boot_key_values: Vec<BootKeyValue>,
    // `Lix` is the public shell around the workspace session. `Lix::open(...)`
    // populates this eagerly, while the hidden `Lix::boot(...)` path fills it after
    // initialize/open_existing for internal tests.
    workspace_session: OnceLock<Arc<Session>>,
}

impl Clone for Lix {
    fn clone(&self) -> Self {
        let cloned = Self {
            engine: Arc::clone(&self.engine),
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
            engine: Arc::new(Engine::new(
                config.backend,
                config.wasm_runtime,
                boot_deterministic_settings,
                crate::catalog::build_builtin_surface_registry(),
                catalog_projection_registry,
            )),
            boot_key_values: config.key_values,
            workspace_session: OnceLock::new(),
        }
    }

    pub(crate) fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub(crate) fn boot_key_values(&self) -> &[BootKeyValue] {
        &self.boot_key_values
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
        if crate::live_state::load_mode_with_backend(self.engine.backend().as_ref()).await?
            == crate::live_state::LiveStateMode::Uninitialized
        {
            return Err(crate::common::not_initialized_error());
        }
        let registry = self
            .engine
            .load_public_surface_registry_from_backend()
            .await?;
        self.engine.install_public_surface_registry(registry);
        let session_host = self.engine.session_host();
        let _ = crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .execute(sql, params)
            .await
    }

    pub async fn active_version_id(&self) -> Result<String, LixError> {
        let session_host = self.engine.session_host();
        Ok(
            crate::session::opened_workspace_session(&session_host, &self.workspace_session)
                .await?
                .active_version_id(),
        )
    }

    pub async fn active_account_ids(&self) -> Result<Vec<String>, LixError> {
        let session_host = self.engine.session_host();
        Ok(
            crate::session::opened_workspace_session(&session_host, &self.workspace_session)
                .await?
                .active_account_ids(),
        )
    }

    /// Returns the workspace session's default delivery origin.
    ///
    /// This is the origin used by `exclude_self()` and inherited write
    /// execution, not a row-level metadata field.
    pub fn origin_key(&self) -> Result<&str, LixError> {
        Ok(crate::session::require_workspace_session(&self.workspace_session)?.origin_key())
    }

    /// Waits until the receipt's state-commit fence has been emitted.
    ///
    /// This is the engine-level optimistic-ack path. It uses the receipt
    /// returned by write execution and does not inspect row-visible origin
    /// metadata.
    pub async fn wait_for_write_receipt(&self, receipt: &WriteReceipt) -> Result<(), LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .wait_for_write_receipt(receipt)
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .execute_with_options(sql, params, options)
            .await
    }

    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEventsOwned, LixError> {
        Session::observe_owned(
            Arc::clone(crate::session::require_workspace_session(
                &self.workspace_session,
            )?),
            query,
        )
    }

    /// Observes a query with explicit delivery filters.
    ///
    /// `ObserveOptions` filters on delivery metadata such as `origin_key`; it
    /// does not require the query itself to project origin columns.
    pub fn observe_with_options(
        &self,
        query: ObserveQuery,
        options: ObserveOptions,
    ) -> Result<ObserveEventsOwned, LixError> {
        Session::observe_owned_with_options(
            Arc::clone(crate::session::require_workspace_session(
                &self.workspace_session,
            )?),
            query,
            options,
        )
    }

    /// Subscribes to committed semantic change batches.
    ///
    /// Origin filters operate on delivery metadata attached to each batch, not
    /// on durable row columns.
    pub fn state_commit_stream(&self, filter: StateCommitStreamFilter) -> PublicStateCommitStream {
        if let Some(session) = self.workspace_session.get() {
            return session.state_commit_stream(filter);
        }
        self.engine
            .state_commit_stream(filter.resolved_without_session_origin())
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
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .open_additional_session(options)
            .await
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .create_version(options)
            .await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .switch_version(version_id)
            .await
    }

    pub async fn set_active_account_ids(
        &self,
        active_account_ids: Vec<String>,
    ) -> Result<(), LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .set_active_account_ids(active_account_ids)
            .await
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .merge_version(options)
            .await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .create_checkpoint()
            .await
    }

    pub async fn undo(&self) -> Result<UndoResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .undo()
            .await
    }

    pub async fn undo_with_options(&self, options: UndoOptions) -> Result<UndoResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .undo_with_options(options)
            .await
    }

    pub async fn redo(&self) -> Result<RedoResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .redo()
            .await
    }

    pub async fn redo_with_options(&self, options: RedoOptions) -> Result<RedoResult, LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .redo_with_options(options)
            .await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .install_plugin(archive_bytes)
            .await
    }

    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?
            .register_schema(schema)
            .await
    }

    pub async fn export_image(&self) -> Result<Vec<u8>, LixError> {
        let mut writer = VecImageWriter::default();
        let session_host = self.engine.session_host();
        crate::session::opened_workspace_session(&session_host, &self.workspace_session)
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
        self.engine.backend().export_image(writer).await
    }

    pub async fn restore_from_image(
        &self,
        reader: &mut dyn ImageChunkReader,
    ) -> Result<(), LixError> {
        self.engine.backend().restore_from_image(reader).await?;
        self.engine.clear_public_surface_registry();
        let registry = self
            .engine
            .load_public_surface_registry_from_backend()
            .await?;
        self.engine.install_public_surface_registry(registry);
        self.engine.invalidate_installed_plugins_cache()?;
        if let Some(session) = self.workspace_session.get() {
            session.reload_workspace_state_from_backend().await?;
        } else {
            let session_host = self.engine.session_host();
            let _ =
                crate::session::opened_workspace_session(&session_host, &self.workspace_session)
                    .await?;
        }
        Ok(())
    }

    pub async fn live_state_projection_status(&self) -> Result<ProjectionStatus, LixError> {
        crate::live_state::projection_status(self.engine.backend().as_ref()).await
    }

    /// Runs repository-owned maintenance compaction for stale untracked journal
    /// rows that are no longer being compacted opportunistically on write.
    ///
    /// This keeps tracked history append-only while letting the engine prune
    /// superseded untracked rows below the durable consumer watermark.
    pub async fn compact_untracked_change_journal(&self) -> Result<u64, LixError> {
        let mut transaction = self
            .engine
            .backend()
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        let deleted =
            crate::canonical::compact_stale_untracked_changes_in_transaction(transaction.as_mut())
                .await?;
        transaction.commit().await?;
        Ok(deleted as u64)
    }

    pub async fn live_state_rebuild_plan(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError> {
        crate::live_state::rebuild_plan(self.engine.backend().as_ref(), req).await
    }

    pub async fn apply_live_state_rebuild_plan(
        &self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError> {
        let mut transaction = self
            .engine
            .backend()
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        let apply_result = crate::live_state::apply_rebuild_plan(transaction.as_mut(), plan).await;
        match apply_result {
            Ok(report) => {
                transaction.commit().await?;
                Ok(report)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn rebuild_live_state(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildReport, LixError> {
        let mut transaction = self
            .engine
            .backend()
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        let rebuild_result = crate::live_state::rebuild(transaction.as_mut(), req).await;
        let report = match rebuild_result {
            Ok(report) => {
                transaction.commit().await?;
                report
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if let Err(error) = crate::live_state::rebuild_file_payloads_with_plugins(
            self.engine.backend().as_ref(),
            self.engine.as_ref(),
            &report.plan,
        )
        .await
        {
            let mut transaction = self
                .engine
                .backend()
                .begin_transaction(TransactionBeginMode::Write)
                .await?;
            let mark_result = crate::live_state::mark_mode_in_transaction(
                transaction.as_mut(),
                crate::live_state::LiveStateMode::NeedsRebuild,
            )
            .await;
            match mark_result {
                Ok(()) => {
                    transaction.commit().await?;
                }
                Err(mark_error) => {
                    let _ = transaction.rollback().await;
                    return Err(mark_error);
                }
            }
            return Err(error);
        }

        Ok(report)
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<SessionTransaction<'_>, LixError> {
        let session_host = self.engine.session_host();
        let _ = crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?;
        crate::session::require_workspace_session(&self.workspace_session)?
            .begin_transaction_with_options(options)
            .await
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut SessionTransaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let session_host = self.engine.session_host();
        let _ = crate::session::opened_workspace_session(&session_host, &self.workspace_session)
            .await?;
        crate::session::require_workspace_session(&self.workspace_session)?
            .transaction(options, f)
            .await
    }
}

#[derive(Default)]
struct VecImageWriter {
    bytes: Vec<u8>,
}

#[async_trait]
impl ImageChunkWriter for VecImageWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::sql::parse_sql_statements(sql) else {
        return false;
    };
    crate::sql::should_invalidate_installed_plugins_cache_for_statements(&statements)
}

#[cfg(test)]
mod tests {
    use super::should_invalidate_installed_plugins_cache_for_sql;
    use super::*;
    use crate::backend::TransactionBeginMode;
    use crate::sql::{
        advance_placeholder_state_for_statement_ast, bind_sql_with_state,
        extract_explicit_transaction_script, is_query_only_statements, optimize_state_resolution,
        parse_sql_statements, PlaceholderState,
    };
    use crate::wasm::NoopWasmRuntime;
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
        mode: TransactionBeginMode,
    }

    #[async_trait]
    impl LixBackend for TestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.to_ascii_lowercase().contains("unknown_table") {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "no such table: unknown_table".to_string(),
                    hint: None,
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            mode: TransactionBeginMode,
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
            self.begin_transaction(TransactionBeginMode::Write).await
        }
    }

    #[async_trait]
    impl LixBackendTransaction for TestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> TransactionBeginMode {
            self.mode
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.to_ascii_lowercase().contains("unknown_table") {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "no such table: unknown_table".to_string(),
                    hint: None,
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
            lix.engine().session_host(),
            "version-test".to_string(),
            Vec::new(),
        );

        {
            let mut cache = lix
                .engine()
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
            lix.engine()
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
            lix.engine()
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
            lix.engine().session_host(),
            "version-test".to_string(),
            Vec::new(),
        );

        {
            let mut cache = lix
                .engine()
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
            lix.engine()
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
        let parsed = parse_explicit_transaction_script(
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
                    crate::sql::canonicalize_state_resolution(&statements),
                )
                .optimized
                .should_refresh_file_cache
            })
            .unwrap_or(false)
    }

    fn parse_explicit_transaction_script(
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Vec<Statement>>, LixError> {
        let statements = parse_sql_statements(sql)?;
        extract_explicit_transaction_script(&statements, params)
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
                        crate::live_state::LiveStateMode::NeedsRebuild,
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
