use super::*;
use crate::sql::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
use crate::sql::execution::parse::parse_sql;
use std::sync::atomic::Ordering;

impl Engine {
    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<EngineTransaction<'_>, LixError> {
        let transaction = self.backend.begin_transaction().await?;
        Ok(EngineTransaction {
            engine: self,
            transaction: Some(transaction),
            options,
            active_version_id: self.require_active_version_id()?,
            active_version_changed: false,
            installed_plugins_cache_invalidation_pending: false,
            pending_state_commit_stream_changes: Vec::new(),
            pending_sql2_append_session: None,
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(&'tx mut EngineTransaction<'_>) -> EngineTransactionFuture<'tx, T>,
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

    pub async fn begin_transaction_handle_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<u64, LixError> {
        let transaction = self.begin_transaction_with_options(options).await?;
        let handle = self
            .next_transaction_handle_id
            .fetch_add(1, Ordering::Relaxed);
        let transaction = unsafe {
            std::mem::transmute::<EngineTransaction<'_>, EngineTransaction<'static>>(transaction)
        };
        self.put_transaction_handle(handle, transaction)?;
        Ok(handle)
    }

    pub async fn execute_in_transaction_handle(
        &self,
        handle: u64,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        let mut transaction = self.take_transaction_handle(handle)?;
        let result = transaction.execute(sql, params).await;
        self.put_transaction_handle(handle, transaction)?;
        result
    }

    pub async fn commit_transaction_handle(&self, handle: u64) -> Result<(), LixError> {
        let transaction = self.take_transaction_handle(handle)?;
        transaction.commit().await
    }

    pub async fn rollback_transaction_handle(&self, handle: u64) -> Result<(), LixError> {
        let transaction = self.take_transaction_handle(handle)?;
        transaction.rollback().await
    }

    fn take_transaction_handle(&self, handle: u64) -> Result<EngineTransaction<'static>, LixError> {
        let mut guard = self.active_transactions.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction registry lock poisoned".to_string(),
        })?;
        guard
            .remove(&handle)
            .ok_or_else(crate::errors::transaction_handle_not_found_error)
    }

    fn put_transaction_handle(
        &self,
        handle: u64,
        transaction: EngineTransaction<'static>,
    ) -> Result<(), LixError> {
        let mut guard = self.active_transactions.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction registry lock poisoned".to_string(),
        })?;
        guard.insert(handle, transaction);
        Ok(())
    }
}

impl EngineTransaction<'_> {
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        if !self.engine.access_to_internal {
            let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        self.execute_with_access(sql, params, self.engine.access_to_internal)
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
                    allow_internal_tables,
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

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}
