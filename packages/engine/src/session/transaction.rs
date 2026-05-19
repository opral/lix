use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::functions::FunctionContext;
use crate::storage::{InMemoryStorageBackend, StorageBackend};
use crate::transaction::{open_transaction, Transaction};
use crate::LixError;
use tokio::sync::OwnedMutexGuard;

use super::context::{closed_error, SessionOperationGuard, SessionTransactionGuard};
use super::SessionContext;

pub struct SessionTransaction<B: StorageBackend = InMemoryStorageBackend> {
    pub(super) transaction: Option<Transaction<B>>,
    pub(super) runtime_functions: FunctionContext,
    closed: Arc<AtomicBool>,
    operation_in_progress: Arc<AtomicUsize>,
    operation_watch: tokio::sync::watch::Sender<usize>,
    _transaction_guard: SessionTransactionGuard,
    _write_guard: OwnedMutexGuard<()>,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn begin_transaction(&self) -> Result<SessionTransaction<B>, LixError> {
        self.ensure_open()?;
        let transaction_guard = self.reserve_session_transaction()?;
        let write_guard = Arc::clone(&self.write_lock).lock_owned().await;
        let _operation_guard = self.begin_session_operation()?;
        let mut opened = match open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await
        {
            Ok(opened) => opened,
            Err(error) => {
                return Err(error);
            }
        };
        self.ensure_open()?;
        opened
            .transaction
            .attach_commit_boundary(self.transaction_commit_boundary());
        Ok(SessionTransaction {
            transaction: Some(opened.transaction),
            runtime_functions: opened.runtime_functions,
            closed: self.closed_flag(),
            operation_in_progress: self.operation_in_progress_flag(),
            operation_watch: self.operation_watch(),
            _transaction_guard: transaction_guard,
            _write_guard: write_guard,
        })
    }
}

impl<B> SessionTransaction<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    pub(super) fn transaction_mut(&mut self) -> Result<&mut Transaction<B>, LixError> {
        self.ensure_session_open()?;
        self.transaction
            .as_mut()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        self.ensure_session_open()?;
        let closed = Arc::clone(&self.closed);
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        let result = transaction
            .commit_checked(&self.runtime_functions, || {
                if closed.load(Ordering::SeqCst) {
                    return Err(closed_error());
                }
                Ok(())
            })
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

    pub(super) fn ensure_session_open(&self) -> Result<(), LixError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_error());
        }
        Ok(())
    }

    pub(super) fn begin_session_operation(&self) -> Result<SessionOperationGuard, LixError> {
        self.ensure_session_open()?;
        let previous = self.operation_in_progress.fetch_add(1, Ordering::SeqCst);
        self.operation_watch.send_replace(previous + 1);
        if let Err(error) = self.ensure_session_open() {
            let remaining = self.operation_in_progress.fetch_sub(1, Ordering::SeqCst) - 1;
            self.operation_watch.send_replace(remaining);
            return Err(error);
        }
        Ok(SessionOperationGuard {
            operation_in_progress: Arc::clone(&self.operation_in_progress),
            operation_watch: self.operation_watch.clone(),
        })
    }
}

pub(crate) fn transaction_state_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", message)
}
