use crate::engine::{reject_internal_table_writes, Engine, EngineTransaction, ExecuteOptions};
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::parse::parse_sql;
use crate::{ExecuteResult, LixError, Value};
use futures_util::FutureExt;
use std::future::Future;
use std::pin::Pin;

impl Engine {
    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<EngineTransaction<'_>, LixError> {
        self.ensure_no_open_public_sql_transaction("begin_transaction")?;
        let transaction = self.backend.begin_transaction().await?;
        Ok(EngineTransaction {
            engine: self,
            transaction: Some(transaction),
            options,
            public_surface_registry: self.public_surface_registry(),
            active_version_id: self.require_active_version_id()?,
            active_version_changed: false,
            installed_plugins_cache_invalidation_pending: false,
            public_surface_registry_dirty: false,
            pending_state_commit_stream_changes: Vec::new(),
            pending_public_append_session: None,
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut EngineTransaction<'_>,
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
}

impl EngineTransaction<'_> {
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        if !self.engine.access_to_internal() {
            let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        self.execute_with_access(sql, params, self.engine.access_to_internal())
            .await
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_access(sql, params, true).await
    }

    async fn execute_with_access(
        &mut self,
        sql: &str,
        params: &[Value],
        allow_internal_tables: bool,
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
                    allow_internal_tables,
                    &mut self.public_surface_registry,
                    &mut self.public_surface_registry_dirty,
                    &mut self.active_version_id,
                    &mut self.pending_state_commit_stream_changes,
                    &mut self.pending_public_append_session,
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
                    allow_internal_tables,
                    &mut self.public_surface_registry,
                    &mut self.public_surface_registry_dirty,
                    &mut self.active_version_id,
                    None,
                    false,
                    &mut self.pending_state_commit_stream_changes,
                    &mut self.pending_public_append_session,
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
        if self.public_surface_registry_dirty {
            self.engine.refresh_public_surface_registry().await?;
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

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}
