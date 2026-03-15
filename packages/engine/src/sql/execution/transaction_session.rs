use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sqlparser::ast::Statement;

use crate::engine::reject_internal_table_writes;
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::parse::parse_sql;
use crate::sql::public::catalog::SurfaceRegistry;
use crate::state::stream::StateCommitStreamChange;
use crate::{
    errors, Engine, ExecuteOptions, ExecuteResult, LixBackend, LixError, LixTransaction,
    QueryResult, Value,
};

#[derive(Default)]
pub(crate) struct PublicSqlSessionState {
    session_transaction: Option<SessionTransaction>,
}

pub(crate) async fn execute_public_sql(
    engine: &Engine,
    state: &mut PublicSqlSessionState,
    sql_transaction_open: &AtomicBool,
    sql: &str,
    params: &[Value],
    options: ExecuteOptions,
) -> Result<ExecuteResult, LixError> {
    let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
    if !engine.access_to_internal() {
        reject_internal_table_writes(&parsed_statements)?;
    }

    if let Some(control) = classify_public_transaction_control(&parsed_statements)? {
        return execute_transaction_control(engine, state, sql_transaction_open, control, options)
            .await;
    }

    if state.session_transaction.is_some() {
        if contains_any_transaction_control_statement(&parsed_statements) {
            return Err(errors::transaction_control_statement_denied_error());
        }
        return execute_in_active_transaction(
            state,
            engine,
            parsed_statements,
            sql,
            params,
            options,
        )
        .await;
    }

    engine.execute_impl_sql(sql, params, options, false).await
}

struct SessionTransaction {
    _backend: Arc<dyn LixBackend + Send + Sync>,
    transaction: Option<Box<dyn LixTransaction + 'static>>,
    options: ExecuteOptions,
    public_surface_registry: SurfaceRegistry,
    active_version_id: String,
    active_version_changed: bool,
    installed_plugins_cache_invalidation_pending: bool,
    public_surface_registry_dirty: bool,
    pending_state_commit_stream_changes: Vec<StateCommitStreamChange>,
    observe_tick_already_emitted: bool,
    pending_public_append_session:
        Option<crate::sql::execution::shared_path::PendingPublicAppendSession>,
}

impl SessionTransaction {
    fn validate_options(&self, options: &ExecuteOptions) -> Result<(), LixError> {
        if let Some(writer_key) = options.writer_key.as_deref() {
            if self.options.writer_key.as_deref() != Some(writer_key) {
                return Err(errors::transaction_writer_key_conflict_error(
                    self.options.writer_key.as_deref(),
                    writer_key,
                ));
            }
        }
        Ok(())
    }

    async fn commit(mut self, engine: &Engine) -> Result<(), LixError> {
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "transaction is no longer active"))?;
        if !self.observe_tick_already_emitted
            && !self.pending_state_commit_stream_changes.is_empty()
        {
            engine
                .append_observe_tick_in_transaction(
                    transaction.as_mut(),
                    self.options.writer_key.as_deref(),
                )
                .await?;
        }
        transaction.commit().await?;
        if self.active_version_changed {
            engine.set_active_version_id(self.active_version_id);
        }
        if self.installed_plugins_cache_invalidation_pending {
            engine.invalidate_installed_plugins_cache()?;
        }
        if self.public_surface_registry_dirty {
            engine.refresh_public_surface_registry().await?;
        }
        engine.emit_state_commit_stream_changes(self.pending_state_commit_stream_changes);
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "transaction is no longer active"))?;
        transaction.rollback().await
    }
}

#[derive(Clone, Copy)]
enum PublicTransactionControl {
    Begin,
    Commit,
    Rollback,
}

async fn execute_transaction_control(
    engine: &Engine,
    state: &mut PublicSqlSessionState,
    sql_transaction_open: &AtomicBool,
    control: PublicTransactionControl,
    options: ExecuteOptions,
) -> Result<ExecuteResult, LixError> {
    match control {
        PublicTransactionControl::Begin => {
            if state.session_transaction.is_some() {
                return Err(errors::transaction_already_active_error());
            }
            let active_version_id = engine.require_active_version_id()?;
            let backend = Arc::clone(&engine.backend);
            let transaction = backend.begin_transaction().await?;
            let transaction = unsafe {
                // SAFETY: the transaction may borrow from the backend allocation.
                // Keeping an `Arc` clone in `SessionTransaction` keeps that allocation
                // alive for at least as long as the transaction object.
                std::mem::transmute::<Box<dyn LixTransaction + '_>, Box<dyn LixTransaction + 'static>>(
                    transaction,
                )
            };
            state.session_transaction = Some(SessionTransaction {
                _backend: backend,
                transaction: Some(transaction),
                options,
                public_surface_registry: engine.public_surface_registry(),
                active_version_id,
                active_version_changed: false,
                installed_plugins_cache_invalidation_pending: false,
                public_surface_registry_dirty: false,
                pending_state_commit_stream_changes: Vec::new(),
                observe_tick_already_emitted: false,
                pending_public_append_session: None,
            });
            sql_transaction_open.store(true, Ordering::SeqCst);
            Ok(empty_execute_result())
        }
        PublicTransactionControl::Commit => {
            let Some(session_transaction) = state.session_transaction.take() else {
                return Err(errors::no_active_transaction_error("COMMIT"));
            };
            sql_transaction_open.store(false, Ordering::SeqCst);
            session_transaction.commit(engine).await?;
            Ok(empty_execute_result())
        }
        PublicTransactionControl::Rollback => {
            let Some(session_transaction) = state.session_transaction.take() else {
                return Err(errors::no_active_transaction_error("ROLLBACK"));
            };
            sql_transaction_open.store(false, Ordering::SeqCst);
            session_transaction.rollback().await?;
            Ok(empty_execute_result())
        }
    }
}

async fn execute_in_active_transaction(
    state: &mut PublicSqlSessionState,
    engine: &Engine,
    parsed_statements: Vec<Statement>,
    sql: &str,
    params: &[Value],
    options: ExecuteOptions,
) -> Result<ExecuteResult, LixError> {
    let session_transaction = state
        .session_transaction
        .as_mut()
        .ok_or_else(errors::transaction_already_active_error)?;
    session_transaction.validate_options(&options)?;
    let previous_active_version_id = session_transaction.active_version_id.clone();
    let transaction = session_transaction
        .transaction
        .as_deref_mut()
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "transaction is no longer active"))?;
    let result = if parsed_statements.len() > 1 {
        engine
            .execute_statement_script_with_options_in_transaction(
                transaction,
                parsed_statements.clone(),
                params,
                &session_transaction.options,
                false,
                &mut session_transaction.public_surface_registry,
                &mut session_transaction.public_surface_registry_dirty,
                &mut session_transaction.active_version_id,
                &mut session_transaction.pending_state_commit_stream_changes,
                &mut session_transaction.pending_public_append_session,
                &mut session_transaction.observe_tick_already_emitted,
            )
            .await?
    } else {
        let query_result = engine
            .execute_with_options_in_transaction(
                transaction,
                sql,
                params,
                &session_transaction.options,
                false,
                &mut session_transaction.public_surface_registry,
                &mut session_transaction.public_surface_registry_dirty,
                &mut session_transaction.active_version_id,
                None,
                false,
                &mut session_transaction.pending_state_commit_stream_changes,
                &mut session_transaction.pending_public_append_session,
            )
            .await?;
        ExecuteResult {
            statements: vec![query_result],
        }
    };
    if session_transaction.active_version_id != previous_active_version_id {
        session_transaction.active_version_changed = true;
    }
    if should_invalidate_installed_plugins_cache_for_statements(&parsed_statements) {
        session_transaction.installed_plugins_cache_invalidation_pending = true;
    }
    Ok(result)
}

fn classify_public_transaction_control(
    statements: &[Statement],
) -> Result<Option<PublicTransactionControl>, LixError> {
    let [statement] = statements else {
        return Ok(None);
    };
    match statement {
        Statement::StartTransaction {
            begin: true,
            modes,
            transaction,
            modifier,
            statements,
            exception,
            ..
        } => {
            if modes.is_empty()
                && transaction.is_none()
                && modifier.is_none()
                && statements.is_empty()
                && exception.is_none()
            {
                Ok(Some(PublicTransactionControl::Begin))
            } else {
                Err(errors::transaction_control_statement_denied_error())
            }
        }
        Statement::Commit {
            chain,
            end,
            modifier,
        } => {
            if !chain && !end && modifier.is_none() {
                Ok(Some(PublicTransactionControl::Commit))
            } else {
                Err(errors::transaction_control_statement_denied_error())
            }
        }
        Statement::Rollback { chain, savepoint } => {
            if !chain && savepoint.is_none() {
                Ok(Some(PublicTransactionControl::Rollback))
            } else {
                Err(errors::transaction_control_statement_denied_error())
            }
        }
        Statement::Savepoint { .. } | Statement::ReleaseSavepoint { .. } => {
            Err(errors::transaction_control_statement_denied_error())
        }
        _ => Ok(None),
    }
}

fn contains_any_transaction_control_statement(statements: &[Statement]) -> bool {
    statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
        )
    })
}

fn empty_execute_result() -> ExecuteResult {
    ExecuteResult {
        statements: vec![QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        }],
    }
}
