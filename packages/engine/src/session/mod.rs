//! Session and workspace-selector orchestration.
//!
//! `Session` owns workspace-scoped selectors such as the active version and
//! active accounts. Those selectors may be persisted for the workspace session
//! or kept ephemeral for additional scoped sessions, but they are distinct
//! from canonical version refs and committed graph state.

pub(crate) mod checkpoint_ops;
pub(crate) mod collaborators;
pub(crate) mod execution_context;
pub(crate) mod observe;
pub(crate) mod pending_reads;
pub(crate) mod plugin;
mod public_surface_registry;
pub(crate) mod read_execution_bindings;
pub(crate) mod read_preparation;
mod selector_reads;
pub(crate) mod state_selector;
pub(crate) mod version_ops;
pub(crate) mod workspace;
pub(crate) mod write_execution_bindings;
pub(crate) mod write_pipeline;
pub(crate) mod write_preparation;
pub(crate) mod write_resolution;
pub(crate) mod write_validation;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures_util::FutureExt;
use sqlparser::ast::Statement;

use crate::catalog::SurfaceRegistry;
use crate::contracts::ExecuteOptions;
use crate::contracts::TransactionCommitOutcome;
use crate::contracts::{SessionDependency, SessionExecutionMode, SessionStateSnapshot};
use crate::diagnostics::transaction_control_statement_denied_error;
use crate::execution::read::execute_prepared_read_program_in_committed_read_transaction;
use crate::execution::write::buffered_write_transaction::BufferedWriteTransaction;
use crate::execution::write::{
    prepare_registered_schema_write_step, stage_prepared_write_step, SemanticWriteContext,
};
use crate::image::ImageChunkWriter;
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::session::collaborators::SessionCollaborators;
use crate::session::execution_context::{
    ExecutionContext, SessionExecutionRuntime, SessionExecutionRuntimeHandle,
};
pub(crate) use crate::session::public_surface_registry::apply_registered_schema_snapshot_to_surface_registry;
pub(crate) use crate::session::selector_reads::SessionWriteSelectorResolver;
use crate::session::workspace::{
    load_workspace_active_account_ids, persist_workspace_selectors,
    require_workspace_active_version_id,
};
use crate::session::write_pipeline::{
    ensure_execution_runtime_state_for_write_scope, prepared_write_runtime_state_for_execution,
};
use crate::session::write_preparation::{
    execute_execution_program_with_write_transaction,
    execute_parsed_statements_in_write_transaction,
};
#[cfg(test)]
use crate::sql::parser::parse_sql;
use crate::sql::parser::parse_sql_with_timing;
use crate::sql::prepare::script::extract_explicit_transaction_script_from_statements;
use crate::sql::prepare::{
    prepare_committed_read_program_in_transaction, prepare_committed_read_program_with_backend,
    CommittedReadProgramContext, ExecutionProgram,
};
use crate::sql::support::{reject_internal_table_writes, reject_public_create_table};
use crate::{ExecuteResult, LixError, Value};

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
    collaborators: Arc<SessionCollaborators>,
    // Session-local runtime state. Workspace sessions persist these selectors
    // through `crate::session::workspace`; extra sessions keep them ephemeral.
    active_version_id: RwLock<String>,
    active_account_ids: RwLock<Vec<String>>,
    public_surface_registry: RwLock<SurfaceRegistry>,
    execution_runtime: SessionExecutionRuntimeHandle,
    #[allow(dead_code)]
    observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::session::observe::SharedObserveSource>>>>,
    active_version_generation: AtomicU64,
    active_account_generation: AtomicU64,
    runtime_generation: AtomicU64,
    persistence: Persistence,
}

pub struct SessionTransaction<'a> {
    collaborators: &'a SessionCollaborators,
    session: &'a Session,
    pub(crate) write_transaction: Option<BufferedWriteTransaction<'a>>,
    pub(crate) context: ExecutionContext,
}

impl Session {
    pub(crate) async fn open_workspace(
        collaborators: Arc<SessionCollaborators>,
    ) -> Result<Self, LixError> {
        collaborators.ensure_initialized().await?;
        let active_version_id =
            require_workspace_active_version_id(collaborators.backend().as_ref()).await?;
        let active_account_ids =
            load_workspace_active_account_ids(collaborators.backend().as_ref())
                .await?
                .unwrap_or_default();
        let registry = collaborators.public_surface_registry();
        Ok(Self {
            collaborators,
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            public_surface_registry: RwLock::new(registry),
            execution_runtime: SessionExecutionRuntime::new(),
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
            collaborators: Arc::clone(&self.collaborators),
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            public_surface_registry: RwLock::new(self.public_surface_registry()),
            execution_runtime: SessionExecutionRuntime::new(),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_version_generation: AtomicU64::new(0),
            active_account_generation: AtomicU64::new(0),
            runtime_generation: AtomicU64::new(0),
            persistence: Persistence::Ephemeral,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        collaborators: Arc<SessionCollaborators>,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            public_surface_registry: RwLock::new(collaborators.public_surface_registry()),
            collaborators,
            active_version_id: RwLock::new(active_version_id),
            active_account_ids: RwLock::new(active_account_ids),
            execution_runtime: SessionExecutionRuntime::new(),
            observe_shared_sources: Mutex::new(BTreeMap::new()),
            active_version_generation: AtomicU64::new(0),
            active_account_generation: AtomicU64::new(0),
            runtime_generation: AtomicU64::new(0),
            persistence: Persistence::Ephemeral,
        }
    }

    pub(crate) fn collaborators(&self) -> &SessionCollaborators {
        self.collaborators.as_ref()
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
        self.execution_runtime.public_surface_registry_generation()
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

    pub(crate) fn dependency_generation(&self, dependency: SessionDependency) -> u64 {
        match dependency {
            SessionDependency::ActiveVersion => {
                self.active_version_generation.load(Ordering::SeqCst)
            }
            SessionDependency::ActiveAccounts => {
                self.active_account_generation.load(Ordering::SeqCst)
            }
            SessionDependency::PublicSurfaceRegistryGeneration => {
                self.public_surface_registry_generation()
            }
        }
    }

    pub(crate) fn dependency_generations(
        &self,
        dependencies: &BTreeSet<SessionDependency>,
    ) -> BTreeMap<SessionDependency, u64> {
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
        self.collaborators
            .create_version_in_session(self, options)
            .await
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
        self.collaborators
            .merge_version_in_session(self, options)
            .await
    }

    pub async fn undo(&self) -> Result<crate::UndoResult, LixError> {
        Box::pin(self.undo_with_options(crate::UndoOptions::default())).await
    }

    pub async fn undo_with_options(
        &self,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        Box::pin(
            self.collaborators
                .undo_with_options_in_session(self, options),
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
            self.collaborators
                .redo_with_options_in_session(self, options),
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
        self.collaborators.export_image(writer).await
    }

    pub(crate) fn new_execution_context(&self, options: ExecuteOptions) -> ExecutionContext {
        ExecutionContext::new(
            options,
            self.public_surface_registry(),
            Arc::clone(&self.execution_runtime),
            self.active_version_id(),
            self.active_account_ids(),
        )
    }

    pub(crate) fn install_public_surface_registry(&self, registry: SurfaceRegistry) {
        self.replace_local_public_surface_registry(registry.clone());
        if self.should_persist_workspace_selectors() {
            self.collaborators.install_public_surface_registry(registry);
        }
    }

    fn replace_local_public_surface_registry(&self, registry: SurfaceRegistry) {
        *self
            .public_surface_registry
            .write()
            .expect("session public surface registry lock poisoned") = registry;
        self.execution_runtime
            .bump_public_surface_registry_generation();
        self.bump_runtime_generation();
    }

    pub(crate) async fn refresh_public_surface_registry(&self) -> Result<(), LixError> {
        let registry = self.collaborators.load_public_surface_registry().await?;
        self.install_public_surface_registry(registry);
        Ok(())
    }

    pub(crate) async fn reload_workspace_state_from_backend(&self) -> Result<(), LixError> {
        if !self.should_persist_workspace_selectors() {
            return Ok(());
        }

        let active_version_id =
            require_workspace_active_version_id(self.collaborators.backend().as_ref()).await?;
        let active_account_ids =
            load_workspace_active_account_ids(self.collaborators.backend().as_ref())
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
        allow_internal_tables: bool,
    ) -> Result<ExecuteResult, LixError> {
        let allow_internal_sql = allow_internal_tables || self.collaborators.access_to_internal();

        let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
        let parsed_statements = parsed.statements;
        if !allow_internal_sql {
            reject_public_create_table(&parsed_statements)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        let explicit_transaction_script =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
                .is_some();
        if !allow_internal_sql
            && contains_transaction_control_statement(&parsed_statements)
            && !explicit_transaction_script
        {
            return Err(transaction_control_statement_denied_error());
        }

        let mut context = self.new_execution_context(options);
        let runtime_bindings = context.runtime_binding_values()?;
        let program = ExecutionProgram::compile(
            parsed_statements,
            params,
            self.collaborators.backend().dialect(),
            &runtime_bindings,
            Some(parsed.parse_duration),
        )?;
        let execution_mode = classify_session_execution_mode(&program, explicit_transaction_script);
        let runtime_state = self
            .collaborators
            .prepare_execution_runtime_state(self.collaborators.backend().as_ref())
            .await?;
        context.set_execution_runtime_state(runtime_state.clone());

        let result = match execution_mode {
            SessionExecutionMode::CommittedRead => {
                let committed_read_context = committed_read_program_context(
                    &context,
                    self.collaborators.as_ref(),
                    &runtime_state,
                    execution_mode,
                );
                let prepared_committed_read = prepare_committed_read_program_with_backend(
                    self.collaborators.backend().as_ref(),
                    &program,
                    allow_internal_sql,
                    &committed_read_context,
                )
                .await?;
                let mut transaction = self
                    .collaborators
                    .begin_read_unit(prepared_committed_read.transaction_mode)
                    .await?;
                let result = execute_prepared_read_program_in_committed_read_transaction(
                    transaction.as_mut(),
                    self.collaborators.as_ref(),
                    &prepared_committed_read,
                )
                .await;
                match result {
                    Ok(result) => {
                        transaction.commit().await?;
                        context.clear_execution_runtime_state();
                        Ok(result)
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        context.clear_execution_runtime_state();
                        Err(error)
                    }
                }
            }
            SessionExecutionMode::CommittedRuntimeMutation => {
                let runtime_state = context.execution_runtime_state().expect(
                    "committed execution should retain an execution runtime state during execution",
                );

                if !runtime_state.settings().enabled {
                    let committed_read_context = committed_read_program_context(
                        &context,
                        self.collaborators.as_ref(),
                        runtime_state,
                        execution_mode,
                    );
                    let prepared_committed_read = prepare_committed_read_program_with_backend(
                        self.collaborators.backend().as_ref(),
                        &program,
                        allow_internal_sql,
                        &committed_read_context,
                    )
                    .await?;
                    let mut transaction = self
                        .collaborators
                        .begin_read_unit(prepared_committed_read.transaction_mode)
                        .await?;
                    let result = execute_prepared_read_program_in_committed_read_transaction(
                        transaction.as_mut(),
                        self.collaborators.as_ref(),
                        &prepared_committed_read,
                    )
                    .await;
                    match result {
                        Ok(result) => {
                            transaction.commit().await?;
                            context.clear_execution_runtime_state();
                            Ok(result)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            context.clear_execution_runtime_state();
                            Err(error)
                        }
                    }
                } else {
                    let mut transaction = self
                        .collaborators
                        .begin_read_unit(crate::TransactionMode::Write)
                        .await?;
                    let mut runtime_functions = runtime_state.provider().clone();
                    crate::runtime::deterministic_mode::ensure_runtime_sequence_initialized_in_transaction(
                        transaction.as_mut(),
                        &mut runtime_functions,
                    )
                    .await?;
                    let committed_read_context = committed_read_program_context(
                        &context,
                        self.collaborators.as_ref(),
                        runtime_state,
                        execution_mode,
                    );
                    let prepared_committed_read = {
                        prepare_committed_read_program_in_transaction(
                            transaction.as_mut(),
                            &program,
                            allow_internal_sql,
                            &committed_read_context,
                        )
                        .await?
                    };
                    let result = execute_prepared_read_program_in_committed_read_transaction(
                        transaction.as_mut(),
                        self.collaborators.as_ref(),
                        &prepared_committed_read,
                    )
                    .await;
                    match result {
                        Ok(result) => {
                            crate::runtime::deterministic_mode::persist_runtime_sequence_in_transaction(
                                transaction.as_mut(),
                                runtime_state.provider(),
                            )
                            .await?;
                            transaction.commit().await?;
                            context.clear_execution_runtime_state();
                            Ok(result)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            context.clear_execution_runtime_state();
                            Err(error)
                        }
                    }
                }
            }
            SessionExecutionMode::WriteTransaction => {
                let transaction = self.collaborators.begin_write_unit().await?;
                let mut write_transaction = BufferedWriteTransaction::new(transaction);

                let result = execute_execution_program_with_write_transaction(
                    self.collaborators.as_ref(),
                    &mut write_transaction,
                    &program,
                    allow_internal_sql,
                    &mut context,
                )
                .await;

                match result {
                    Ok(result) => {
                        context.clear_execution_runtime_state();
                        let outcome = write_transaction
                            .commit_buffered_write(
                                self.collaborators.as_ref(),
                                context.buffered_write_execution_input(),
                            )
                            .await?;
                        self.apply_transaction_commit_outcome(outcome).await?;
                        Ok(result)
                    }
                    Err(error) => {
                        let _ = write_transaction.rollback_buffered_write().await;
                        context.clear_execution_runtime_state();
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
        let transaction = self.collaborators.begin_write_unit().await?;
        Ok(SessionTransaction {
            collaborators: self.collaborators.as_ref(),
            session: self,
            write_transaction: Some(BufferedWriteTransaction::new(transaction)),
            context: self.new_execution_context(options),
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
                self.collaborators.backend().as_ref(),
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
            self.collaborators.invalidate_deterministic_settings_cache();
        }
        if outcome.invalidate_installed_plugins_cache {
            self.collaborators.invalidate_installed_plugins_cache()?;
        }
        if outcome.refresh_public_surface_registry {
            self.refresh_public_surface_registry().await?;
        }
        self.collaborators
            .emit_state_commit_stream_changes(std::mem::take(
                &mut outcome.state_commit_stream_changes,
            ));
        Ok(())
    }
}

fn baseline_committed_read_transaction_mode(
    execution_mode: SessionExecutionMode,
    runtime_state: &ExecutionRuntimeState,
) -> crate::TransactionMode {
    match execution_mode {
        SessionExecutionMode::CommittedRead => crate::TransactionMode::Read,
        SessionExecutionMode::CommittedRuntimeMutation => {
            if runtime_state.settings().enabled {
                crate::TransactionMode::Write
            } else {
                crate::TransactionMode::Read
            }
        }
        SessionExecutionMode::WriteTransaction => crate::TransactionMode::Write,
    }
}

fn committed_read_program_context<'a>(
    context: &'a ExecutionContext,
    collaborators: &'a SessionCollaborators,
    runtime_state: &'a ExecutionRuntimeState,
    execution_mode: SessionExecutionMode,
) -> CommittedReadProgramContext<'a> {
    CommittedReadProgramContext {
        active_version_id: context.active_version_id.as_str(),
        active_account_ids: &context.active_account_ids,
        writer_key: context.options.writer_key.as_deref(),
        preparation_seed: collaborators
            .sql_preparation_seed(runtime_state.provider(), &context.public_surface_registry),
        base_transaction_mode: baseline_committed_read_transaction_mode(
            execution_mode,
            runtime_state,
        ),
    }
}

fn classify_session_execution_mode(
    program: &ExecutionProgram,
    explicit_transaction_script: bool,
) -> SessionExecutionMode {
    if !explicit_transaction_script && program.is_plain_committed_read() {
        if program
            .runtime_effects()
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
    pub(crate) fn collaborators(&self) -> &'a SessionCollaborators {
        self.collaborators
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
        changes: Vec<crate::contracts::StateCommitStreamChange>,
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
        let write_transaction = self
            .write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
        ensure_execution_runtime_state_for_write_scope(
            self.collaborators,
            write_transaction.backend_transaction_mut()?,
            &mut self.context,
        )
        .await?;
        let semantic_context = SemanticWriteContext::new(
            prepared_write_runtime_state_for_execution(
                self.context
                    .execution_runtime_state()
                    .expect("register_schema should prepare write runtime state"),
            ),
            self.context.public_surface_registry.clone(),
            self.context.active_account_ids.clone(),
            self.context.options.writer_key.clone(),
        );
        let step = prepare_registered_schema_write_step(schema, &semantic_context)?;
        let write_transaction = self
            .write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
        stage_prepared_write_step(write_transaction, step)?;
        Ok(())
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed = parse_sql_with_timing(sql).map_err(LixError::from)?;
        let parsed_statements = parsed.statements;
        if !self.collaborators.access_to_internal() {
            reject_public_create_table(&parsed_statements)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        execute_parsed_statements_in_write_transaction(
            self.collaborators,
            write_transaction,
            parsed_statements,
            params,
            self.collaborators.access_to_internal(),
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
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        execute_parsed_statements_in_write_transaction(
            self.collaborators,
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
        let outcome = write_transaction
            .commit_buffered_write(
                self.session.collaborators.as_ref(),
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
        write_transaction.rollback_buffered_write().await
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
    session
        .collaborators
        .ensure_version_exists(version_id)
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
        modes: Arc<Mutex<Vec<crate::TransactionMode>>>,
        executed_sql: Arc<Mutex<Vec<String>>>,
    }

    struct RecordingTransaction {
        connection: Arc<Mutex<rusqlite::Connection>>,
        mode: crate::TransactionMode,
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

        fn modes(&self) -> Vec<crate::TransactionMode> {
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
            mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, crate::LixError> {
            self.modes.lock().expect("recorded modes lock").push(mode);
            {
                let connection = self.connection.lock().expect("sqlite connection lock");
                connection
                    .execute_batch(match mode {
                        crate::TransactionMode::Read | crate::TransactionMode::Deferred => "BEGIN",
                        crate::TransactionMode::Write => "BEGIN IMMEDIATE",
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
                .push(crate::TransactionMode::Write);
            Ok(Box::new(RecordingTransaction {
                connection: Arc::clone(&self.connection),
                mode: crate::TransactionMode::Write,
                executed_sql: Arc::clone(&self.executed_sql),
            }))
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackendTransaction for RecordingTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
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
            Arc::new(crate::runtime::wasm::NoopWasmRuntime),
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
                crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
                "version-test".to_string(),
                Vec::new(),
            );

            let result = session
                .execute("SELECT 1", &[])
                .await
                .expect("plain read should succeed");
            assert_eq!(result.statements[0].rows[0][0], crate::Value::Integer(1));
            assert_eq!(backend.modes(), vec![crate::TransactionMode::Read]);
        });
    }

    #[test]
    fn deterministic_reads_classify_as_committed_runtime_mutation() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
                "version-test".to_string(),
                Vec::new(),
            );
            let parsed_statements =
                parse_sql("SELECT lix_uuid_v7()").expect("parse SQL should succeed");
            let runtime_bindings = session
                .new_execution_context(ExecuteOptions::default())
                .runtime_binding_values()
                .expect("runtime bindings should succeed");
            let program = ExecutionProgram::compile(
                parsed_statements,
                &[],
                crate::SqlDialect::Sqlite,
                &runtime_bindings,
                None,
            )
            .expect("execution program compilation should succeed");

            assert_eq!(
                classify_session_execution_mode(&program, false),
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
                crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
                "version-test".to_string(),
                Vec::new(),
            );

            let _ = session.execute("BEGIN; SELECT 1; COMMIT;", &[]).await;
            assert_eq!(backend.modes(), vec![crate::TransactionMode::Write]);
        });
    }

    #[test]
    fn history_public_reads_use_deferred_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            backend.clear_modes();
            let session = lix
                .open_workspace_session()
                .await
                .expect("workspace session should open");

            let result = session
                .execute("SELECT COUNT(*) AS c FROM lix_state_history", &[])
                .await
                .expect("direct public history read should succeed");
            assert_eq!(result.statements[0].rows.len(), 1);
            assert_eq!(backend.modes(), vec![crate::TransactionMode::Deferred]);
        });
    }

    #[test]
    fn lowered_committed_public_reads_use_read_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            backend.clear_modes();
            let session = lix
                .open_workspace_session()
                .await
                .expect("workspace session should open");

            let result = session
                .execute("SELECT id FROM lix_version ORDER BY id LIMIT 1", &[])
                .await
                .expect("materialized public read should succeed");
            assert_eq!(result.statements[0].rows.len(), 1);
            assert_eq!(backend.modes(), vec![crate::TransactionMode::Read]);
        });
    }

    #[test]
    fn lowered_committed_only_public_reads_use_read_transaction_mode() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            lix.initialize().await.expect("lix init should succeed");
            let session = lix
                .open_workspace_session()
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
                    vec![crate::TransactionMode::Read],
                    "lowered committed-only read should stay on TransactionMode::Read for `{sql}`",
                );
            }
        });
    }

    #[test]
    fn parser_stage_internal_storage_guard_rejects_before_backend_execution() {
        run_with_large_stack(|| async move {
            let backend = RecordingBackend::new();
            let lix = test_lix(backend.clone());
            let session = Session::new_for_test(
                crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
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
                crate::session::collaborators::SessionCollaborators::new(lix.session_services()),
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
