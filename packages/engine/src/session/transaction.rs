use std::sync::Arc;

use crate::functions::FunctionContext;
use crate::transaction::{open_transaction, Transaction};
use crate::LixError;

use super::context::SessionTransactionGuard;
use super::SessionContext;

pub struct SessionTransaction {
    pub(super) transaction: Option<Transaction>,
    pub(super) runtime_functions: FunctionContext,
    _transaction_guard: SessionTransactionGuard,
}

impl SessionContext {
    pub async fn begin_transaction(&self) -> Result<SessionTransaction, LixError> {
        self.ensure_open()?;
        let transaction_guard = self.reserve_session_transaction()?;
        let opened = match open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.plugin_context),
        )
        .await
        {
            Ok(opened) => opened,
            Err(error) => {
                return Err(error);
            }
        };
        Ok(SessionTransaction {
            transaction: Some(opened.transaction),
            runtime_functions: opened.runtime_functions,
            _transaction_guard: transaction_guard,
        })
    }
}

impl SessionTransaction {
    pub(super) fn transaction_mut(&mut self) -> Result<&mut Transaction, LixError> {
        self.transaction
            .as_mut()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        let result = transaction
            .commit(&self.runtime_functions)
            .await
            .map(|_| ());
        result
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        let result = transaction.rollback().await;
        result
    }
}

pub(crate) fn transaction_state_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", message)
}
