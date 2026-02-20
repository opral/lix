use super::super::*;

impl Engine {
    pub(crate) async fn begin_transaction_with_options(
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
}
