//! Session and workspace-selector orchestration.
//!
//! `Session` owns workspace-scoped selectors such as the active version and
//! active accounts. Those selectors may be persisted for the workspace session
//! or kept ephemeral for additional scoped sessions, but they are distinct
//! from canonical version refs and committed graph state.

pub(crate) mod checkpoint_ops;
pub(crate) mod host;
mod init;
pub(crate) mod observe;
pub(crate) mod plugin;
pub(crate) mod public_read_execution;
mod runtime;
mod state;
pub(crate) mod version_ops;
pub(crate) mod workspace;
pub(crate) mod write_execution_context;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures_util::FutureExt;
use sqlparser::ast::Statement;

use crate::catalog::SurfaceRegistry;
use crate::diagnostics::transaction_control_statement_denied_error;
use crate::execution::execute_prepared_read_batch_in_committed_read_transaction;
use crate::functions::FunctionBindings;
use crate::image::ImageChunkWriter;
use crate::plugin::{prepare_registered_schema_write_statement, PluginInstallWriteContext};
use crate::session::workspace::{
    load_workspace_active_account_ids, persist_workspace_selectors,
    require_workspace_active_version_id,
};
#[cfg(test)]
use crate::sql::parse_sql;
use crate::sql::{
    extract_explicit_transaction_script, parse_sql_with_timing,
    prepare_committed_read_batch_in_transaction, prepare_committed_read_batch_with_backend,
    reject_internal_table_writes, reject_public_create_table, CommittedReadContext,
    QueryDependency, StatementBatch,
};
use crate::transaction::{
    ensure_function_bindings_for_write_scope, execute_parsed_statements_in_write_transaction,
    execute_statement_batch_with_write_transaction, prepared_write_function_bindings_for_execution,
};
use crate::transaction::{stage_prepared_write_statement, BufferedWriteTransaction};
use crate::transaction::{
    PendingCommitState, SessionCompilerCache, SessionCompilerCacheHandle, SessionCompilerState,
    TransactionCommitOutcome,
};
use crate::{ExecuteResult, LixError, Value};
pub(crate) use host::{
    opened_workspace_session, prepare_function_bindings_with_host, require_workspace_session,
    sql_compiler_seed_from_host, SessionExecutionContext, SessionHost,
};

pub(crate) use init::{init, load_checkpoint_version_heads_for_init};
pub use runtime::ExecuteOptions;
pub(crate) use runtime::SessionExecutionMode;
pub(crate) use state::SessionStateSnapshot;

pub(crate) async fn execute_prepared_public_read_with_registry(
    projection_registry: &crate::catalog::CatalogProjectionRegistry,
    transaction: &mut dyn crate::LixBackendTransaction,
    pending_overlay: Option<&dyn crate::transaction::PendingOverlay>,
    public_read: &crate::sql::PreparedPublicRead,
) -> Result<crate::QueryResult, LixError> {
    write_execution_context::execute_prepared_public_read_with_registry(
        projection_registry,
        transaction,
        pending_overlay,
        public_read,
    )
    .await
}

pub(crate) async fn persist_binary_blob_writes_in_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
    writes: &[crate::transaction::BinaryBlobWrite],
) -> Result<(), LixError> {
    write_execution_context::persist_binary_blob_writes(transaction, writes).await
}

pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
) -> Result<(), LixError> {
    write_execution_context::garbage_collect_unreachable_binary_cas(transaction).await
}

pub(crate) async fn persist_runtime_sequence_in_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
    functions: &crate::functions::SharedFunctionProvider<
        Box<dyn crate::functions::LixFunctionProvider + Send>,
    >,
) -> Result<(), LixError> {
    write_execution_context::persist_runtime_sequence(transaction, functions).await
}

pub(crate) async fn execute_public_tracked_append_txn_with_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
    unit: &crate::transaction::TrackedTxnUnit,
    pending_commit_state: Option<&mut Option<PendingCommitState>>,
) -> Result<crate::transaction::TrackedCommitExecutionOutcome, LixError> {
    write_execution_context::execute_public_tracked_append(transaction, unit, pending_commit_state)
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct AdditionalSessionOptions {
    /// Ephemeral workspace selector override for the additional scoped session.
    ///
    /// This does not mutate replica-local version heads or committed history.
    pub active_version_id: Option<String>,
    #[serde(default)]
    /// Ephemeral workspace account-selector override for the additional scoped
    /// session.
    pub active_account_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Persistence {
    Workspace,
    Ephemeral,
}

/// Additional scoped working context.
///
/// `Session` owns session-scoped selectors such as the active version and
/// active accounts. The workspace session inside [`crate::Lix`] uses this same
/// type internally, and additional sessions can be opened explicitly when a
/// caller needs a different scoped view over the same repository.
pub struct Session {
    session_host: Arc<dyn SessionHost>,
    // Session-local runtime state. Workspace sessions persist these selectors
    // through `crate::session::workspace`; extra sessions keep them ephemeral.
    active_version_id: RwLock<String>,
    active_account_ids: RwLock<Vec<String>>,
    public_surface_registry: RwLock<SurfaceRegistry>,
    compiler_cache: SessionCompilerCacheHandle,
    #[allow(dead_code)]
    observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::session::observe::SharedObserveSource>>>>,
    active_version_generation: AtomicU64,
    active_account_generation: AtomicU64,
    runtime_generation: AtomicU64,
    persistence: Persistence,
}

pub struct SessionTransaction<'a> {
    session_host: &'a dyn SessionHost,
    session: &'a Session,
    pub(crate) write_transaction: Option<BufferedWriteTransaction<'a>>,
    pub(crate) context: SessionCompilerState,
}

impl Session {
    pub(crate) async fn open_workspace(
        session_host: Arc<dyn SessionHost>,
    ) -> Result<Self, LixError> {
        session_host.ensure_initialized().await?;
        let active_version_id =
            require_workspace_active_version_id(session_host.backend().as_ref()).await?;
        let active_account_ids = load_workspace_active_account_ids(session_host.backend().as_ref())
            .await?
            .unwrap_or_default();
        let registry = session_host.public_surface_registry();
        Ok(Self {
            session_host,
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            public_surface_registry: RwLock::new(registry),
            compiler_cache: SessionCompilerCache::new(),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_version_generation: AtomicU64::new(0),
            active_account_generation: AtomicU64::new(0),
            runtime_generation: AtomicU64::new(0),
            persistence: Persistence::Workspace,
        })
    }

    /// Opens an additional scoped session with optional workspace-selector
    /// overrides.
    ///
    /// The returned session may read or write committed state, but these
    /// overrides only affect workspace selection for that session.
    pub async fn open_additional_session(
        &self,
        options: AdditionalSessionOptions,
    ) -> Result<Self, LixError> {
        let active_version_id = options
            .active_version_id
            .unwrap_or_else(|| self.active_version_id());
        let active_account_ids = options
            .active_account_ids
            .unwrap_or_else(|| self.active_account_ids());
        Ok(Self {
            session_host: Arc::clone(&self.session_host),
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            public_surface_registry: RwLock::new(self.public_surface_registry()),
            compiler_cache: SessionCompilerCache::new(),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_version_generation: AtomicU64::new(0),
            active_account_generation: AtomicU64::new(0),
            runtime_generation: AtomicU64::new(0),
            persistence: Persistence::Ephemeral,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        session_host: Arc<dyn SessionHost>,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            public_surface_registry: RwLock::new(session_host.public_surface_registry()),
            session_host,
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            compiler_cache: SessionCompilerCache::new(),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_version_generation: AtomicU64::new(0),
            active_account_generation: AtomicU64::new(0),
            runtime_generation: AtomicU64::new(0),
            persistence: Persistence::Ephemeral,
        }
    }

    pub(crate) fn session_host(&self) -> &dyn SessionHost {
        self.session_host.as_ref()
    }

    pub(crate) fn execution_context(&self) -> SessionExecutionContext<'_> {
        SessionExecutionContext::new(self.session_host())
    }

    pub fn active_version_id(&self) -> String {
        self.active_version_id
            .read()
            .expect("session active version lock poisoned")
            .clone()
    }

    pub fn active_account_ids(&self) -> Vec<String> {
        self.active_account_ids
            .read()
            .expect("session active account ids lock poisoned")
            .clone()
    }

    #[allow(dead_code)]
    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.compiler_cache.public_surface_registry_generation()
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> SessionStateSnapshot {
        SessionStateSnapshot {
            active_version_id: self.active_version_id(),
            active_account_ids: self.active_account_ids(),
            generation: self.runtime_generation(),
            public_surface_registry_generation: self.public_surface_registry_generation(),
        }
    }

    pub(crate) fn public_surface_registry(&self) -> SurfaceRegistry {
        self.public_surface_registry
            .read()
            .expect("session public surface registry lock poisoned")
            .clone()
    }

    #[allow(dead_code)]
    pub(crate) fn observe_shared_sources(
        &self,
    ) -> &Mutex<BTreeMap<String, Arc<Mutex<crate::session::observe::SharedObserveSource>>>> {
        &self.observe_shared_sources
    }

    pub(crate) fn runtime_generation(&self) -> u64 {
        self.runtime_generation.load(Ordering::SeqCst)
    }

    pub(crate) fn dependency_generation(&self, dependency: QueryDependency) -> u64 {
        match dependency {
            QueryDependency::ActiveVersion => self.active_version_generation.load(Ordering::SeqCst),
            QueryDependency::ActiveAccounts => {
                self.active_account_generation.load(Ordering::SeqCst)
            }
            QueryDependency::PublicSurfaceRegistryGeneration => {
                self.public_surface_registry_generation()
            }
        }
    }

    pub(crate) fn dependency_generations(
        &self,
        dependencies: &BTreeSet<QueryDependency>,
    ) -> BTreeMap<QueryDependency, u64> {
        dependencies
            .iter()
            .copied()
            .map(|dependency| (dependency, self.dependency_generation(dependency)))
            .collect()
    }

    /// Creates committed version descriptor/ref state rooted at a source
    /// version head.
    ///
    /// This mutates canonical owners but does not switch the caller's
    /// workspace selector to the new version.
    pub async fn create_version(
        &self,
        options: crate::CreateVersionOptions,
    ) -> Result<crate::CreateVersionResult, LixError> {
        crate::session::version_ops::create_version_in_session(self, options).await
    }

    /// Creates a canonical checkpoint label on committed history for the
    /// current workspace-selected version.
    ///
    /// Replay status remains local projection state; this API only mutates
    /// checkpoint label facts plus derived checkpoint-history helpers.
    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        crate::session::checkpoint_ops::create_checkpoint_in_session(self).await
    }

    /// Merges one committed version head into another.
    ///
    /// This may update replica-local version heads and rebuild derived
    /// projections, but it does not change workspace selectors unless the
    /// caller separately updates them.
    pub async fn merge_version(
        &self,
        options: crate::MergeVersionOptions,
    ) -> Result<crate::MergeVersionResult, LixError> {
        crate::session::version_ops::merge_version_in_session(self, options).await
    }

    pub async fn undo(&self) -> Result<crate::UndoResult, LixError> {
        Box::pin(self.undo_with_options(crate::UndoOptions::default())).await
    }

    pub async fn undo_with_options(
        &self,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        Box::pin(
            crate::session::version_ops::undo_redo::undo_with_options_in_session(self, options),
        )
        .await
    }

    pub async fn redo(&self) -> Result<crate::RedoResult, LixError> {
        Box::pin(self.redo_with_options(crate::RedoOptions::default())).await
    }

    pub async fn redo_with_options(
        &self,
        options: crate::RedoOptions,
    ) -> Result<crate::RedoResult, LixError> {
        Box::pin(
            crate::session::version_ops::undo_redo::redo_with_options_in_session(self, options),
        )
        .await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        crate::session::plugin::install_plugin_in_session(self, archive_bytes).await
    }

    pub async fn register_schema(&self, schema: &serde_json::Value) -> Result<(), LixError> {
        let mut transaction = self
            .begin_transaction_with_options(ExecuteOptions::default())
            .await?;
        transaction.register_schema(schema).await?;
        transaction.commit().await
    }

    pub async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        self.session_host.export_image(writer).await
    }

    pub(crate) fn new_compiler_state(&self, options: ExecuteOptions) -> SessionCompilerState {
        SessionCompilerState::new(
            options.writer_key,
            self.public_surface_registry(),
            Arc::clone(&self.compiler_cache),
            self.active_version_id(),
            self.active_account_ids(),
        )
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.replace_local_public_surface_registry(registry.clone());
        if self.should_persist_workspace_selectors() {
            self.session_host.install_public_surface_registry(registry);
        }
    }

    fn replace_local_public_surface_registry(&self, registry: SurfaceRegistry) {
        *self
            .public_surface_registry
            .write()
            .expect("session public surface registry lock poisoned") = registry;
        self.compiler_cache
            .bump_public_surface_registry_generation();
        self.bump_runtime_generation();
    }

    pub(crate) async fn refresh_public_surface_registry(&self) -> Result<(), LixError> {
        let registry = self.session_host.load_public_surface_registry().await?;
        self.install_public_surface_registry(registry);
        Ok(())
    }

    pub(crate) async fn reload_workspace_state_from_backend(&self) -> Result<(), LixError> {
        if !self.should_persist_workspace_selectors() {
            return Ok(());
        }

        let active_version_id =
            require_workspace_active_version_id(self.session_host.backend().as_ref()).await?;
        let active_account_ids =
            load_workspace_active_account_ids(self.session_host.backend().as_ref())
                .await?
                .unwrap_or_default();

        self.replace_active_version_id(active_version_id);
        self.replace_active_account_ids(active_account_ids);
        self.refresh_public_surface_registry().await?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.execute_impl_sql(sql, params, options, false).await
    }

    pub(crate) async fn execute_impl_sql(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_relations: bool,
    ) -> Result<ExecuteResult, LixError> {
        let allow_internal_sql = allow_internal_relations || self.session_host.access_to_internal();

        let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
        let parsed_statements = parsed.statements;
        if !allow_internal_sql {
            reject_public_create_table(&parsed_statements)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        let explicit_transaction_script =
            extract_explicit_transaction_script(&parsed_statements, params)?.is_some();
        if !allow_internal_sql
            && contains_transaction_control_statement(&parsed_statements)
            && !explicit_transaction_script
        {
            return Err(transaction_control_statement_denied_error());
        }

        let mut context = self.new_compiler_state(options);
        let runtime_bindings = context.runtime_binding_values()?;
        let statement_batch = StatementBatch::compile(
            parsed_statements,
            params,
            self.session_host.backend().dialect(),
            &runtime_bindings,
            Some(parsed.parse_duration),
        )?;
        let execution_mode =
            classify_session_execution_mode(&statement_batch, explicit_transaction_script);
        let function_bindings = prepare_function_bindings_with_host(
            self.session_host.as_ref(),
            self.session_host.backend().as_ref(),
        )
        .await?;
        context.set_function_bindings(function_bindings.clone());
        let execution_context = self.execution_context();

        let result = match execution_mode {
            SessionExecutionMode::CommittedRead => {
                let committed_read_context = committed_read_context(
                    &context,
                    self.session_host.as_ref(),
                    &function_bindings,
                    execution_mode,
                );
                let prepared_read_batch = prepare_committed_read_batch_with_backend(
                    self.session_host.backend().as_ref(),
                    &statement_batch,
                    allow_internal_sql,
                    &committed_read_context,
                )
                .await?;
                let mut transaction = self
                    .session_host
                    .begin_read_unit(prepared_read_batch.transaction_mode)
                    .await?;
                let result = execute_prepared_read_batch_in_committed_read_transaction(
                    transaction.as_mut(),
                    &execution_context,
                    &prepared_read_batch,
                )
                .await;
                match result {
                    Ok(result) => {
                        transaction.commit().await?;
                        context.clear_function_bindings();
                        Ok(result)
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        context.clear_function_bindings();
                        Err(error)
                    }
                }
            }
            SessionExecutionMode::CommittedRuntimeMutation => {
                let function_bindings = context
                    .function_bindings()
                    .expect("committed execution should retain function bindings during execution");

                if !function_bindings.deterministic_enabled() {
                    let committed_read_context = committed_read_context(
                        &context,
                        self.session_host.as_ref(),
                        function_bindings,
                        execution_mode,
                    );
                    let prepared_read_batch = prepare_committed_read_batch_with_backend(
                        self.session_host.backend().as_ref(),
                        &statement_batch,
                        allow_internal_sql,
                        &committed_read_context,
                    )
                    .await?;
                    let mut transaction = self
                        .session_host
                        .begin_read_unit(prepared_read_batch.transaction_mode)
                        .await?;
                    let result = execute_prepared_read_batch_in_committed_read_transaction(
                        transaction.as_mut(),
                        &execution_context,
                        &prepared_read_batch,
                    )
                    .await;
                    match result {
                        Ok(result) => {
                            transaction.commit().await?;
                            context.clear_function_bindings();
                            Ok(result)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            context.clear_function_bindings();
                            Err(error)
                        }
                    }
                } else {
                    let mut transaction = self
                        .session_host
                        .begin_read_unit(crate::TransactionBeginMode::Write)
                        .await?;
                    let mut runtime_functions = function_bindings.provider().clone();
                    crate::transaction::ensure_runtime_sequence_initialized_in_transaction(
                        transaction.as_mut(),
                        &mut runtime_functions,
                    )
                    .await?;
                    let committed_read_context = committed_read_context(
                        &context,
                        self.session_host.as_ref(),
                        function_bindings,
                        execution_mode,
                    );
                    let prepared_read_batch = {
                        prepare_committed_read_batch_in_transaction(
                            transaction.as_mut(),
                            &statement_batch,
                            allow_internal_sql,
                            &committed_read_context,
                        )
                        .await?
                    };
                    let result = execute_prepared_read_batch_in_committed_read_transaction(
                        transaction.as_mut(),
                        &execution_context,
                        &prepared_read_batch,
                    )
                    .await;
                    match result {
                        Ok(result) => {
                            crate::transaction::persist_runtime_sequence_in_transaction(
                                transaction.as_mut(),
                                function_bindings.provider(),
                            )
                            .await?;
                            transaction.commit().await?;
                            context.clear_function_bindings();
                            Ok(result)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            context.clear_function_bindings();
                            Err(error)
                        }
                    }
                }
            }
            SessionExecutionMode::WriteTransaction => {
                let transaction = self.session_host.begin_write_unit().await?;
                let mut write_transaction = BufferedWriteTransaction::new(transaction);

                let result = execute_statement_batch_with_write_transaction(
                    &execution_context,
                    &mut write_transaction,
                    &statement_batch,
                    allow_internal_sql,
                    &mut context,
                )
                .await;

                match result {
                    Ok(result) => {
                        context.clear_function_bindings();
                        let outcome = write_transaction
                            .commit(&execution_context, context.buffered_write_execution_input())
                            .await?;
                        self.apply_transaction_commit_outcome(outcome).await?;
                        Ok(result)
                    }
                    Err(error) => {
                        let _ = write_transaction.rollback().await;
                        context.clear_function_bindings();
                        Err(error)
                    }
                }
            }
        };
        result
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<SessionTransaction<'_>, LixError> {
        let transaction = self.session_host.begin_write_unit().await?;
        Ok(SessionTransaction {
            session_host: self.session_host.as_ref(),
            session: self,
            write_transaction: Some(BufferedWriteTransaction::new(transaction)),
            context: self.new_compiler_state(options),
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut SessionTransaction<'_>,
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

    /// Replaces the session or workspace active-version selector.
    ///
    /// This does not move canonical version heads; it only changes which
    /// committed head later default-scoped reads and writes target.
    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        if version_id.trim().is_empty() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "version_id must be a non-empty string",
            ));
        }
        ensure_version_exists(self, &version_id).await?;
        self.apply_selector_changes(
            Some(version_id),
            None,
            self.should_persist_workspace_selectors(),
        )
        .await
    }

    /// Replaces the session or workspace active-account selector set.
    pub async fn set_active_account_ids(
        &self,
        active_account_ids: Vec<String>,
    ) -> Result<(), LixError> {
        self.apply_selector_changes(
            None,
            Some(active_account_ids),
            self.should_persist_workspace_selectors(),
        )
        .await
    }

    pub(crate) fn replace_active_version_id(&self, version_id: String) {
        let mut guard = self
            .active_version_id
            .write()
            .expect("session active version lock poisoned");
        if *guard != version_id {
            *guard = version_id;
            self.active_version_generation
                .fetch_add(1, Ordering::SeqCst);
            self.bump_runtime_generation();
        }
    }

    pub(crate) fn replace_active_account_ids(&self, active_account_ids: Vec<String>) {
        let mut guard = self
            .active_account_ids
            .write()
            .expect("session active account ids lock poisoned");
        if *guard != active_account_ids {
            *guard = active_account_ids;
            self.active_account_generation
                .fetch_add(1, Ordering::SeqCst);
            self.bump_runtime_generation();
        }
    }

    pub(crate) fn bump_runtime_generation(&self) {
        self.runtime_generation.fetch_add(1, Ordering::SeqCst);
    }

    fn should_persist_workspace_selectors(&self) -> bool {
        matches!(self.persistence, Persistence::Workspace)
    }

    async fn apply_selector_changes(
        &self,
        next_active_version_id: Option<String>,
        next_active_account_ids: Option<Vec<String>>,
        persist_workspace: bool,
    ) -> Result<(), LixError> {
        if let Some(version_id) = next_active_version_id.as_ref() {
            self.replace_active_version_id(version_id.clone());
        }
        if let Some(active_account_ids) = next_active_account_ids.as_ref() {
            self.replace_active_account_ids(active_account_ids.clone());
        }
        if persist_workspace {
            persist_workspace_selectors(
                self.session_host.backend().as_ref(),
                next_active_version_id.as_deref(),
                next_active_account_ids.as_deref(),
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn apply_transaction_commit_outcome(
        &self,
        mut outcome: TransactionCommitOutcome,
    ) -> Result<(), LixError> {
        let persist_workspace =
            self.should_persist_workspace_selectors() || outcome.session_delta.persist_workspace;
        self.apply_selector_changes(
            outcome.session_delta.next_active_version_id.take(),
            outcome.session_delta.next_active_account_ids.take(),
            persist_workspace,
        )
        .await?;
        if outcome.invalidate_deterministic_settings_cache {
            self.session_host.invalidate_deterministic_settings_cache();
        }
        if outcome.invalidate_installed_plugins_cache {
            self.session_host.invalidate_installed_plugins_cache()?;
        }
        if outcome.refresh_public_surface_registry {
            self.refresh_public_surface_registry().await?;
        }
        self.session_host
            .emit_state_commit_stream_changes(std::mem::take(
                &mut outcome.state_commit_stream_changes,
            ));
        Ok(())
    }
}

fn baseline_committed_read_transaction_mode(
    execution_mode: SessionExecutionMode,
    function_bindings: &FunctionBindings,
) -> crate::TransactionBeginMode {
    match execution_mode {
        SessionExecutionMode::CommittedRead => crate::TransactionBeginMode::Read,
        SessionExecutionMode::CommittedRuntimeMutation => {
            if function_bindings.deterministic_enabled() {
                crate::TransactionBeginMode::Write
            } else {
                crate::TransactionBeginMode::Read
            }
        }
        SessionExecutionMode::WriteTransaction => crate::TransactionBeginMode::Write,
    }
}

fn committed_read_context<'a>(
    context: &'a SessionCompilerState,
    session_host: &'a dyn SessionHost,
    function_bindings: &'a FunctionBindings,
    execution_mode: SessionExecutionMode,
) -> CommittedReadContext<'a> {
    CommittedReadContext {
        active_version_id: context.active_version_id.as_str(),
        active_account_ids: &context.active_account_ids,
        writer_key: context.writer_key.as_deref(),
        compiler_seed: sql_compiler_seed_from_host(
            session_host,
            function_bindings.provider(),
            &context.public_surface_registry,
        ),
        base_transaction_mode: baseline_committed_read_transaction_mode(
            execution_mode,
            function_bindings,
        ),
    }
}

fn classify_session_execution_mode(
    statement_batch: &StatementBatch,
    explicit_transaction_script: bool,
) -> SessionExecutionMode {
    if !explicit_transaction_script && statement_batch.is_plain_committed_read() {
        if statement_batch
            .statement_effects()
            .requires_deterministic_sequence_persistence
        {
            SessionExecutionMode::CommittedRuntimeMutation
        } else {
            SessionExecutionMode::CommittedRead
        }
    } else {
        SessionExecutionMode::WriteTransaction
    }
}

impl<'a> SessionTransaction<'a> {
    pub(crate) fn session_host(&self) -> &'a dyn SessionHost {
        self.session_host
    }

    pub(crate) fn execution_context(&self) -> SessionExecutionContext<'a> {
        SessionExecutionContext::new(self.session_host())
    }

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
        changes: Vec<crate::streams::StateCommitStreamChange>,
    ) -> Result<(), LixError> {
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .record_state_commit_stream_changes(changes);
        Ok(())
    }

    pub(crate) fn record_canonical_commit_receipt(
        &mut self,
        receipt: crate::session::version_ops::commit::CanonicalCommitReceipt,
    ) -> Result<(), LixError> {
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .record_canonical_commit_receipt(receipt);
        Ok(())
    }

    pub async fn register_schema(&mut self, schema: &serde_json::Value) -> Result<(), LixError> {
        let session_host = self.session_host;
        let execution_context = SessionExecutionContext::new(session_host);
        let write_transaction = self
            .write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
        ensure_function_bindings_for_write_scope(
            &execution_context,
            write_transaction.backend_transaction_mut()?,
            &mut self.context,
        )
        .await?;
        let plugin_install_context = PluginInstallWriteContext::new(
            prepared_write_function_bindings_for_execution(
                self.context
                    .function_bindings()
                    .expect("register_schema should prepare function bindings"),
            ),
            self.context.public_surface_registry.clone(),
            self.context.active_account_ids.clone(),
            self.context.writer_key.clone(),
        );
        let statement = prepare_registered_schema_write_statement(schema, &plugin_install_context)?;
        let write_transaction = self
            .write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
        stage_prepared_write_statement(write_transaction, statement)?;
        Ok(())
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
        let parsed_statements = parsed.statements;
        if !self.session_host.access_to_internal() {
            reject_public_create_table(&parsed_statements)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        let execution_context = self.execution_context();
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        execute_parsed_statements_in_write_transaction(
            &execution_context,
            write_transaction,
            parsed_statements,
            params,
            self.session_host.access_to_internal(),
            &mut self.context,
            Some(parsed.parse_duration),
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
        let parsed_statements = parsed.statements;
        let execution_context = self.execution_context();
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        execute_parsed_statements_in_write_transaction(
            &execution_context,
            write_transaction,
            parsed_statements,
            params,
            true,
            &mut self.context,
            Some(parsed.parse_duration),
        )
        .await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let write_transaction = self.write_transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        let execution_context = self.execution_context();
        let outcome = write_transaction
            .commit(
                &execution_context,
                self.context.buffered_write_execution_input(),
            )
            .await?;
        self.session.apply_transaction_commit_outcome(outcome).await
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let write_transaction = self.write_transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        write_transaction.rollback().await
    }

    #[allow(dead_code)]
    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn crate::LixBackendTransaction, LixError> {
        self.write_transaction_mut()?.backend_transaction_mut()
    }

    #[allow(dead_code)]
    pub(crate) fn write_transaction_mut(
        &mut self,
    ) -> Result<&mut BufferedWriteTransaction<'a>, LixError> {
        match self.write_transaction.as_mut() {
            Some(transaction) => Ok(transaction),
            None => Err(LixError::unknown("transaction is no longer active")),
        }
    }
}

impl Drop for SessionTransaction<'_> {
    fn drop(&mut self) {
        if self.write_transaction.is_some() && !std::thread::panicking() {
            panic!("SessionTransaction dropped without commit() or rollback()");
        }
    }
}

async fn ensure_version_exists(session: &Session, version_id: &str) -> Result<(), LixError> {
    crate::session::version_ops::context::ensure_version_exists_with_backend(
        session.session_host.backend().as_ref(),
        version_id,
    )
    .await
}

fn contains_transaction_control_statement(statements: &[Statement]) -> bool {
    statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rusqlite::types::{Value as SqliteValue, ValueRef};

    #[derive(Clone)]
    struct RecordingBackend {
        connection: Arc<Mutex<rusqlite::Connection>>,
        modes: Arc<Mutex<Vec<crate::TransactionBeginMode>>>,
        executed_sql: Arc<Mutex<Vec<String>>>,
    }

    struct RecordingTransaction {
        connection: Arc<Mutex<rusqlite::Connection>>,
        mode: crate::TransactionBeginMode,
        executed_sql: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBackend {
        fn new() -> Self {
            Self {
                connection: Arc::new(Mutex::new(
                    rusqlite::Connection::open_in_memory().expect("open sqlite memory db"),
                )),
                modes: Arc::new(Mutex::new(Vec::new())),
                executed_sql: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn modes(&self) -> Vec<crate::TransactionBeginMode> {
            self.modes.lock().expect("recorded modes lock").clone()
        }

        fn clear_modes(&self) {
            self.modes.lock().expect("recorded modes lock").clear();
        }

        fn executed_sql(&self) -> Vec<String> {
            self.executed_sql.lock().expect("recorded sql lock").clone()
        }
    }

    fn run_with_large_stack<F, Fut>(factory: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("session-mode-tests".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime should build")
                    .block_on(factory());
            })
            .expect("session mode test thread should spawn")
            .join()
            .expect("session mode test thread should not panic");
    }

    #[async_trait(?Send)]
    impl crate::LixBackend for RecordingBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(
            &self,
            sql: &str,
            params: &[crate::Value],
        ) -> Result<crate::QueryResult, crate::LixError> {
            self.executed_sql
                .lock()
                .expect("recorded sql lock")
                .push(sql.to_string());
            let connection = self.connection.lock().expect("sqlite connection lock");
            execute_sql(&connection, sql, params)
        }

        async fn begin_transaction(
            &self,
            mode: crate::TransactionBeginMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
            self.modes.lock().expect("recorded modes lock").push(mode);
            {
                let connection = self.connection.lock().expect("sqlite connection lock");
                connection
                    .execute_batch(match mode {
                        crate::TransactionBeginMode::Read
                        | crate::TransactionBeginMode::Deferred => "BEGIN",
                        crate::TransactionBeginMode::Write => "BEGIN IMMEDIATE",
                    })
                    .map_err(sqlite_error)?;
            }
            Ok(Box::new(RecordingTransaction {
                connection: Arc::clone(&self.connection),
                mode,
                executed_sql: Arc::clone(&self.executed_sql),
            }))
        }

        async fn begin_savepoint(
            &self,
            name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
            {
                let connection = self.connection.lock().expect("sqlite connection lock");
                connection
                    .execute_batch(&format!("SAVEPOINT {name}"))
                    .map_err(sqlite_error)?;
            }
            self.modes
                .lock()
                .expect("recorded modes lock")
                .push(crate::TransactionBeginMode::Write);
            Ok(Box::new(RecordingTransaction {
                connection: Arc::clone(&self.connection),
                mode: crate::TransactionBeginMode::Write,
                executed_sql: Arc::clone(&self.executed_sql),
            }))
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackendTransaction for RecordingTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionBeginMode {
            self.mode
        }

        async fn execute(
            &mut self,
            sql: &str,
            params: &[crate::Value],
        ) -> Result<crate::QueryResult, crate::LixError> {
            self.executed_sql
                .lock()
                .expect("recorded sql lock")
                .push(sql.to_string());
            let connection = self.connection.lock().expect("sqlite connection lock");
            execute_sql(&connection, sql, params)
        }

        async fn commit(self: Box<Self>) -> Result<(), crate::LixError> {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection.execute_batch("COMMIT").map_err(sqlite_error)
        }

        async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection.execute_batch("ROLLBACK").map_err(sqlite_error)
        }
    }

    fn test_lix(backend: RecordingBackend) -> Arc<crate::Lix> {
        Arc::new(crate::Lix::boot(crate::LixConfig::new(
            Box::new(backend),
            Arc::new(crate::wasm::NoopWasmRuntime),
        )))
    }

    fn execute_sql(
        connection: &rusqlite::Connection,
        sql: &str,
        params: &[crate::Value],
    ) -> Result<crate::QueryResult, crate::LixError> {
        let bindings = params.iter().map(to_sqlite_value).collect::<Vec<_>>();
        let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
        let column_count = statement.column_count();
        let columns = statement
            .column_names()
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();

        if column_count == 0 {
            statement
                .execute(rusqlite::params_from_iter(bindings))
                .map_err(sqlite_error)?;
            return Ok(crate::QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut rows = statement
            .query(rusqlite::params_from_iter(bindings))
            .map_err(sqlite_error)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                values.push(from_sqlite_value(row.get_ref(index).map_err(sqlite_error)?));
            }
            out.push(values);
        }

        Ok(crate::QueryResult { rows: out, columns })
    }

    fn to_sqlite_value(value: &crate::Value) -> SqliteValue {
        match value {
            crate::Value::Null => SqliteValue::Null,
            crate::Value::Boolean(value) => SqliteValue::Integer(i64::from(*value)),
            crate::Value::Integer(value) => SqliteValue::Integer(*value),
            crate::Value::Real(value) => SqliteValue::Real(*value),
            crate::Value::Text(value) => SqliteValue::Text(value.clone()),
            crate::Value::Json(value) => SqliteValue::Text(value.to_string()),
            crate::Value::Blob(value) => SqliteValue::Blob(value.clone()),
        }
    }

    fn from_sqlite_value(value: ValueRef<'_>) -> crate::Value {
        match value {
            ValueRef::Null => crate::Value::Null,
            ValueRef::Integer(value) => crate::Value::Integer(value),
            ValueRef::Real(value) => crate::Value::Real(value),
            ValueRef::Text(value) => {
                crate::Value::Text(String::from_utf8_lossy(value).into_owned())
            }
            ValueRef::Blob(value) => crate::Value::Blob(value.to_vec()),
        }
    }

    fn sqlite_error(error: rusqlite::Error) -> crate::LixError {
        crate::LixError::new("LIX_ERROR_UNKNOWN", error.to_string())
    }

    #[test]
    fn plain_reads_use_read_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let result = session
                .execute("SELECT 1", &[])
                .await
                .expect("plain read should succeed");
            assert_eq!(result.statements[0].rows[0][0], crate::Value::Integer(1));
            assert_eq!(backend.modes(), vec![crate::TransactionBeginMode::Read]);
        });
    }

    #[test]
    fn deterministic_reads_classify_as_committed_runtime_mutation() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );
            let parsed_statements =
                parse_sql("SELECT lix_uuid_v7()").expect("parse SQL should succeed");
            let runtime_bindings = session
                .new_compiler_state(ExecuteOptions::default())
                .runtime_binding_values()
                .expect("runtime bindings should succeed");
            let statement_batch = StatementBatch::compile(
                parsed_statements,
                &[],
                crate::SqlDialect::Sqlite,
                &runtime_bindings,
                None,
            )
            .expect("statement batch compilation should succeed");

            assert_eq!(
                classify_session_execution_mode(&statement_batch, false),
                SessionExecutionMode::CommittedRuntimeMutation
            );
        });
    }

    #[test]
    fn explicit_transaction_scripts_use_write_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let _ = session.execute("BEGIN; SELECT 1; COMMIT;", &[]).await;
            assert_eq!(backend.modes(), vec![crate::TransactionBeginMode::Write]);
        });
    }

    #[test]
    fn history_public_reads_use_deferred_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            backend.clear_modes();
            let session = crate::session::host::open_workspace_session(lix.engine().session_host())
                .await
                .expect("workspace session should open");

            let result = session
                .execute("SELECT COUNT(*) AS c FROM lix_state_history", &[])
                .await
                .expect("direct public history read should succeed");
            assert_eq!(result.statements[0].rows.len(), 1);
            assert_eq!(backend.modes(), vec![crate::TransactionBeginMode::Deferred]);
        });
    }

    #[test]
    fn lowered_committed_public_reads_use_read_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            backend.clear_modes();
            let session = crate::session::host::open_workspace_session(lix.engine().session_host())
                .await
                .expect("workspace session should open");

            let result = session
                .execute("SELECT id FROM lix_version ORDER BY id LIMIT 1", &[])
                .await
                .expect("materialized public read should succeed");
            assert_eq!(result.statements[0].rows.len(), 1);
            assert_eq!(backend.modes(), vec![crate::TransactionBeginMode::Read]);
        });
    }

    #[test]
    fn lowered_committed_only_public_reads_use_read_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            let session = crate::session::host::open_workspace_session(lix.engine().session_host())
                .await
                .expect("workspace session should open");

            for sql in [
                "SELECT id FROM lix_change WHERE entity_id = 'entity-1'",
                "SELECT entity_id FROM lix_working_changes WHERE schema_key = 'lix_key_value'",
                "SELECT id FROM lix_file WHERE id = 'file-1'",
                "SELECT id FROM lix_directory_by_version WHERE id = 'dir-1' AND lixcol_version_id = 'global'",
            ] {
                backend.clear_modes();
                session
                    .execute(sql, &[])
                    .await
                    .unwrap_or_else(|error| panic!("lowered committed-only read should succeed for `{sql}`: {error:?}"));
                assert_eq!(
                    backend.modes(),
                    vec![crate::TransactionBeginMode::Read],
                    "lowered committed-only read should stay on TransactionBeginMode::Read for `{sql}`",
                );
            }
        });
    }

    #[test]
    fn parser_stage_internal_relation_guard_rejects_before_backend_execution() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let error = session
                .execute(
                    "INSERT INTO lix_internal_snapshot (id, content) VALUES ('x', NULL)",
                    &[],
                )
                .await
                .expect_err("internal storage write should be rejected before execution");

            assert_eq!(error.code, "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED");
            assert!(
                backend.modes().is_empty(),
                "parser-stage guard should reject before opening a transaction"
            );
            assert!(
                backend.executed_sql().is_empty(),
                "parser-stage guard should reject before reaching backend execution"
            );
        });
    }

    #[test]
    fn parser_stage_public_create_table_guard_rejects_before_backend_execution() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let error = session
                .execute("CREATE TABLE user_data (id TEXT)", &[])
                .await
                .expect_err("public CREATE TABLE should be rejected before execution");

            assert_eq!(error.code, "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED");
            assert!(
                backend.modes().is_empty(),
                "parser-stage guard should reject before opening a transaction"
            );
            assert!(
                backend.executed_sql().is_empty(),
                "parser-stage guard should reject before reaching backend execution"
            );
        });
    }
}
