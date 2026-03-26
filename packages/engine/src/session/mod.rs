pub mod contracts;
pub(crate) mod workspace;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures_util::FutureExt;
use sqlparser::ast::Statement;

use crate::engine::{
    reject_internal_table_writes, reject_public_create_table, Engine, ExecuteOptions,
};
use crate::errors;
use crate::live_state::raw::{load_exact_row_with_backend, RawStorage};
use crate::sql::execution::execution_program::{
    execute_execution_program_with_write_transaction, ExecutionContext, ExecutionProgram,
    SessionExecutionRuntime, SessionExecutionRuntimeHandle,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::internal::script::extract_explicit_transaction_script_from_statements;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::transaction::{TransactionCommitOutcome, WriteTransaction};
use crate::version::{
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{ExecuteResult, LixError, Value};

use contracts::{SessionDependency, SessionStateSnapshot};
use workspace::{
    load_workspace_active_account_ids, persist_workspace_active_account_ids,
    persist_workspace_active_version_id, require_workspace_active_version_id,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct OpenSessionOptions {
    pub active_version_id: Option<String>,
    #[serde(default)]
    pub active_account_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Persistence {
    Workspace,
    Ephemeral,
}

pub struct Session {
    engine: Arc<Engine>,
    // Session-local runtime state. Workspace sessions persist these values back
    // through `session/workspace.rs`; extra sessions keep them ephemeral.
    active_version_id: RwLock<String>,
    active_account_ids: RwLock<Vec<String>>,
    public_surface_registry: RwLock<SurfaceRegistry>,
    execution_runtime: SessionExecutionRuntimeHandle,
    #[allow(dead_code)]
    observe_shared_sources:
        Mutex<BTreeMap<String, Arc<Mutex<crate::observe::SharedObserveSource>>>>,
    active_version_generation: AtomicU64,
    active_account_generation: AtomicU64,
    runtime_generation: AtomicU64,
    persistence: Persistence,
}

pub struct SessionTransaction<'a> {
    pub(crate) engine: &'a Engine,
    session: &'a Session,
    pub(crate) write_transaction: Option<WriteTransaction<'a>>,
    pub(crate) context: ExecutionContext,
}

impl Session {
    pub(crate) async fn open_workspace(engine: Arc<Engine>) -> Result<Self, LixError> {
        if !engine.is_initialized().await? {
            return Err(errors::not_initialized_error());
        }
        let active_version_id =
            require_workspace_active_version_id(engine.backend.as_ref()).await?;
        let active_account_ids =
            match load_workspace_active_account_ids(engine.backend.as_ref()).await? {
                Some(active_account_ids) => active_account_ids,
                None => match engine.boot_active_account() {
                    Some(account) => {
                        let active_account_ids = vec![account.id.clone()];
                        persist_workspace_active_account_ids(
                            engine.backend.as_ref(),
                            &active_account_ids,
                        )
                        .await?;
                        active_account_ids
                    }
                    None => Vec::new(),
                },
            };
        let registry = engine.public_surface_registry();
        Ok(Self {
            engine,
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

    pub async fn open_session(&self, options: OpenSessionOptions) -> Result<Self, LixError> {
        let active_version_id = options
            .active_version_id
            .unwrap_or_else(|| self.active_version_id());
        let active_account_ids = options
            .active_account_ids
            .unwrap_or_else(|| self.active_account_ids());
        Ok(Self {
            engine: Arc::clone(&self.engine),
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
        engine: Arc<Engine>,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            public_surface_registry: RwLock::new(engine.public_surface_registry()),
            engine,
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

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
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
    ) -> &Mutex<BTreeMap<String, Arc<Mutex<crate::observe::SharedObserveSource>>>> {
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

    pub async fn create_version(
        &self,
        options: crate::CreateVersionOptions,
    ) -> Result<crate::CreateVersionResult, LixError> {
        crate::version::create_version_in_session(self, options).await
    }

    pub async fn create_checkpoint(&self) -> Result<crate::CreateCheckpointResult, LixError> {
        crate::state::checkpoint::create_checkpoint_in_session(self).await
    }

    pub async fn merge_version(
        &self,
        options: crate::MergeVersionOptions,
    ) -> Result<crate::MergeVersionResult, LixError> {
        crate::version::merge_version_in_session(self, options).await
    }

    pub async fn undo(&self) -> Result<crate::UndoResult, LixError> {
        self.undo_with_options(crate::UndoOptions::default()).await
    }

    pub async fn undo_with_options(
        &self,
        options: crate::UndoOptions,
    ) -> Result<crate::UndoResult, LixError> {
        crate::undo_redo::undo_with_options_in_session(self, options).await
    }

    pub async fn redo(&self) -> Result<crate::RedoResult, LixError> {
        self.redo_with_options(crate::RedoOptions::default()).await
    }

    pub async fn redo_with_options(
        &self,
        options: crate::RedoOptions,
    ) -> Result<crate::RedoResult, LixError> {
        crate::undo_redo::redo_with_options_in_session(self, options).await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        crate::plugin::install::install_plugin_in_session(self, archive_bytes).await
    }

    pub async fn register_schema(&self, schema: &serde_json::Value) -> Result<(), LixError> {
        let mut transaction = self
            .begin_transaction_with_options(ExecuteOptions::default())
            .await?;
        transaction.register_schema(schema).await?;
        transaction.commit().await
    }

    pub async fn export_image(
        &self,
        writer: &mut dyn crate::ImageChunkWriter,
    ) -> Result<(), LixError> {
        self.engine.backend.export_image(writer).await
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
        let allow_internal_sql = allow_internal_tables || self.engine.access_to_internal();

        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
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
            return Err(errors::transaction_control_statement_denied_error());
        }

        let transaction = self.engine.begin_write_unit().await?;
        let mut write_transaction = WriteTransaction::new_buffered_write(transaction);
        let mut context = self.new_execution_context(options);
        let runtime_bindings = context.runtime_binding_values()?;
        let program = ExecutionProgram::compile(
            parsed_statements,
            params,
            self.engine.backend.dialect(),
            &runtime_bindings,
        )?;

        let result = execute_execution_program_with_write_transaction(
            self.engine.as_ref(),
            &mut write_transaction,
            &program,
            allow_internal_sql,
            &mut context,
        )
        .await;

        match result {
            Ok(result) => {
                let outcome = write_transaction
                    .commit_buffered_write(self.engine.as_ref(), context)
                    .await?;
                self.apply_transaction_commit_outcome(outcome).await?;
                Ok(result)
            }
            Err(error) => {
                let _ = write_transaction.rollback_buffered_write().await;
                Err(error)
            }
        }
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<SessionTransaction<'_>, LixError> {
        let transaction = self.engine.begin_write_unit().await?;
        Ok(SessionTransaction {
            engine: self.engine.as_ref(),
            session: self,
            write_transaction: Some(WriteTransaction::new_buffered_write(transaction)),
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

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        if version_id.trim().is_empty() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "version_id must be a non-empty string",
            ));
        }
        ensure_version_exists(self, &version_id).await?;
        self.replace_active_version_id(version_id.clone());
        if matches!(self.persistence, Persistence::Workspace) {
            persist_workspace_active_version_id(self.engine.backend.as_ref(), &version_id).await?;
        }
        Ok(())
    }

    pub async fn set_active_account_ids(
        &self,
        active_account_ids: Vec<String>,
    ) -> Result<(), LixError> {
        self.replace_active_account_ids(active_account_ids.clone());
        if matches!(self.persistence, Persistence::Workspace) {
            persist_workspace_active_account_ids(self.engine.backend.as_ref(), &active_account_ids)
                .await?;
        }
        Ok(())
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

    pub(crate) async fn apply_transaction_commit_outcome(
        &self,
        mut outcome: TransactionCommitOutcome,
    ) -> Result<(), LixError> {
        if let Some(version_id) = outcome.session_delta.next_active_version_id.take() {
            self.replace_active_version_id(version_id.clone());
            if matches!(self.persistence, Persistence::Workspace) {
                persist_workspace_active_version_id(self.engine.backend.as_ref(), &version_id)
                    .await?;
            }
        }
        if let Some(active_account_ids) = outcome.session_delta.next_active_account_ids.take() {
            self.replace_active_account_ids(active_account_ids.clone());
            if matches!(self.persistence, Persistence::Workspace) {
                persist_workspace_active_account_ids(
                    self.engine.backend.as_ref(),
                    &active_account_ids,
                )
                .await?;
            }
        }
        if outcome.invalidate_deterministic_settings_cache {
            self.engine.invalidate_deterministic_settings_cache();
        }
        if outcome.invalidate_installed_plugins_cache {
            self.engine.invalidate_installed_plugins_cache()?;
        }
        if outcome.refresh_public_surface_registry {
            let registry =
                SurfaceRegistry::bootstrap_with_backend(self.engine.backend.as_ref()).await?;
            *self
                .public_surface_registry
                .write()
                .expect("session public surface registry lock poisoned") = registry.clone();
            self.bump_runtime_generation();
            if matches!(self.persistence, Persistence::Workspace) {
                self.engine.refresh_public_surface_registry().await?;
            }
        }
        self.engine.emit_state_commit_stream_changes(std::mem::take(
            &mut outcome.state_commit_stream_changes,
        ));
        Ok(())
    }
}

impl<'a> SessionTransaction<'a> {
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
        changes: Vec<crate::StateCommitStreamChange>,
    ) -> Result<(), LixError> {
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .record_state_commit_stream_changes(changes);
        Ok(())
    }

    pub async fn register_schema(&mut self, schema: &serde_json::Value) -> Result<(), LixError> {
        let snapshot = serde_json::json!({ "value": schema });
        let (schema_key, _) = crate::schema::schema_from_registered_snapshot(&snapshot)?;
        self.write_transaction
            .as_mut()
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?
            .register_schema(
                crate::live_state::SchemaRegistration::with_registered_snapshot(
                    schema_key.schema_key.clone(),
                    snapshot,
                ),
            )?;
        let schema_json = serde_json::to_string(schema).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize schema definition: {error}"),
            )
        })?;
        self.execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
            &[Value::Text(schema_json)],
        )
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
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        crate::transaction::execute_parsed_statements_in_write_transaction(
            self.engine,
            write_transaction,
            parsed_statements,
            params,
            self.engine.access_to_internal(),
            &mut self.context,
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        let write_transaction = self.write_transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        crate::transaction::execute_parsed_statements_in_write_transaction(
            self.engine,
            write_transaction,
            parsed_statements,
            params,
            true,
            &mut self.context,
        )
        .await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let write_transaction = self.write_transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        let placeholder = ExecutionContext::new(
            ExecuteOptions::default(),
            self.context.public_surface_registry.clone(),
            self.context.session_runtime(),
            self.context.active_version_id.clone(),
            self.context.active_account_ids.clone(),
        );
        let context = std::mem::replace(&mut self.context, placeholder);
        let outcome = write_transaction
            .commit_buffered_write(self.engine, context)
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
    pub(crate) fn write_transaction_mut(&mut self) -> Result<&mut WriteTransaction<'a>, LixError> {
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
    let row = load_exact_row_with_backend(
        session.engine.backend.as_ref(),
        RawStorage::Tracked,
        version_descriptor_schema_key(),
        version_descriptor_storage_version_id(),
        version_id,
        Some(version_descriptor_file_id()),
    )
    .await?;
    if row
        .as_ref()
        .is_none_or(|row| row.plugin_key() != version_descriptor_plugin_key())
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version '{version_id}' does not exist"),
        ));
    }
    Ok(())
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
