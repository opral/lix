use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::functions::FunctionContext;
use crate::transaction::{open_transaction, Transaction};
use crate::LixError;

use super::SessionContext;

pub struct SessionTransaction {
    pub(super) transaction: Option<Transaction>,
    pub(super) runtime_functions: FunctionContext,
    active_transaction: Arc<AtomicBool>,
    closed: bool,
}

impl SessionContext {
    pub async fn begin_transaction(&self) -> Result<SessionTransaction, LixError> {
        self.ensure_open()?;
        let active_transaction = self.active_transaction_flag();
        if active_transaction
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(transaction_state_error(
                "Lix handle already has an active transaction",
            ));
        }
        let opened = match open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await
        {
            Ok(opened) => opened,
            Err(error) => {
                active_transaction.store(false, Ordering::SeqCst);
                return Err(error);
            }
        };
        Ok(SessionTransaction {
            transaction: Some(opened.transaction),
            runtime_functions: opened.runtime_functions,
            active_transaction,
            closed: false,
        })
    }
}

impl SessionTransaction {
    pub(super) fn transaction_mut(&mut self) -> Result<&mut Transaction, LixError> {
        if self.closed {
            return Err(transaction_state_error("Lix transaction is closed"));
        }
        self.transaction
            .as_mut()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        self.closed = true;
        let result = transaction
            .commit(&self.runtime_functions)
            .await
            .map(|_| ());
        self.active_transaction.store(false, Ordering::SeqCst);
        result
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        self.closed = true;
        let result = transaction.rollback().await;
        self.active_transaction.store(false, Ordering::SeqCst);
        result
    }
}

impl Drop for SessionTransaction {
    fn drop(&mut self) {
        self.active_transaction.store(false, Ordering::SeqCst);
    }
}

pub(crate) fn transaction_state_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", message)
}
