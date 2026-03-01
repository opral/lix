use super::super::*;
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
            active_version_id: self.active_version_id.read().unwrap().clone(),
            active_version_changed: false,
            installed_plugins_cache_invalidation_pending: false,
            pending_state_commit_stream_changes: Vec::new(),
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
    ) -> Result<QueryResult, LixError> {
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
            title: "Unknown error".to_string(),
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
            title: "Unknown error".to_string(),
            description: "transaction registry lock poisoned".to_string(),
        })?;
        guard.insert(handle, transaction);
        Ok(())
    }
}
