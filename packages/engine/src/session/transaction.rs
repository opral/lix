use std::sync::Arc;

use crate::functions::{DeterministicRuntimeGuard, FunctionContext};
use crate::observe_invalidation::ObserveInvalidation;
use crate::storage_adapter::Memory;
use crate::storage_adapter::Storage;
use tokio::sync::Notify;

use crate::LixError;
#[cfg(test)]
use crate::transaction::CommitBoundaryGuard;
use crate::transaction::{
    CommitBoundaryState, Transaction, TransactionCommitBoundary, open_transaction,
};

use super::SessionContext;
use super::context::{SessionWriteAccess, closed_error};

#[expect(missing_debug_implementations)]
pub struct SessionTransaction<StorageImpl: Storage = Memory> {
    pub(super) transaction: Option<Transaction<StorageImpl>>,
    pub(super) runtime_functions: FunctionContext,
    transaction_manager: SessionTransactionManager,
    observe_invalidation: Arc<ObserveInvalidation>,
    _deterministic_runtime_guard: Option<DeterministicRuntimeGuard>,
    write_access: Option<SessionWriteAccess>,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub async fn begin_transaction(&self) -> Result<SessionTransaction<StorageImpl>, LixError> {
        self.ensure_open()?;
        let write_access = self.begin_explicit_session_write_access().await?;
        let deterministic_runtime_guard = if self.deterministic_mode_enabled().await? {
            Some(self.lock_deterministic_runtime().await)
        } else {
            None
        };
        let mut opened = match open_transaction(
            &self.mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            self.plugin_host.clone(),
            Arc::clone(&self.branch_ctx),
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
        self.transaction_manager()
            .mark_explicit_transaction_open()?;
        Ok(SessionTransaction {
            transaction: Some(opened.transaction),
            runtime_functions: opened.runtime_functions,
            transaction_manager: self.transaction_manager(),
            observe_invalidation: Arc::clone(&self.observe_invalidation),
            _deterministic_runtime_guard: deterministic_runtime_guard,
            write_access: Some(write_access),
        })
    }
}

impl<StorageImpl> SessionTransaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(super) fn transaction_mut(&mut self) -> Result<&mut Transaction<StorageImpl>, LixError> {
        self.ensure_session_open()?;
        self.transaction
            .as_mut()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))
    }

    pub fn active_branch_id(&self) -> Result<&str, LixError> {
        self.ensure_session_open()?;
        self.transaction
            .as_ref()
            .map(Transaction::active_branch_id)
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let operation_guard = self.begin_session_commit_operation()?;
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;
        let result = transaction.commit(&self.runtime_functions).await;
        drop(operation_guard);
        let outcome = result?;
        drop(self.write_access.take());
        self.observe_invalidation
            .bump_if_storage_changed(&outcome.storage_stats);
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| transaction_state_error("Lix transaction is closed"))?;

        transaction.rollback().await
    }

    pub(super) fn ensure_session_open(&self) -> Result<(), LixError> {
        self.transaction_manager.ensure_open()
    }

    pub(super) fn begin_session_operation(&self) -> Result<SessionOperationGuard, LixError> {
        self.transaction_manager.begin_transaction_operation()
    }

    fn begin_session_commit_operation(&self) -> Result<SessionOperationGuard, LixError> {
        self.transaction_manager
            .begin_transaction_commit_operation()
    }
}

pub(crate) fn transaction_state_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", message)
}

#[derive(Clone)]
pub(super) struct SessionTransactionManager {
    inner: Arc<SessionTransactionManagerInner>,
}

struct SessionTransactionManagerInner {
    state: std::sync::Mutex<SessionTransactionState>,
    state_changed: Notify,
    commit_boundary: CommitBoundaryState,
}

#[derive(Debug, Default)]
enum SessionTransactionState {
    #[default]
    OpenIdle,
    OpenOperation {
        active_operations: usize,
    },
    OpenTransaction {
        active_operations: usize,
        owner: TransactionOwner,
    },
    Closing {
        active_operations: usize,
    },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionOwner {
    Automatic,
    ExplicitOpening,
    Explicit,
    ExplicitCommitting,
}

impl SessionTransactionManager {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(SessionTransactionManagerInner {
                state: std::sync::Mutex::new(SessionTransactionState::default()),
                state_changed: Notify::new(),
                commit_boundary: CommitBoundaryState::new(),
            }),
        }
    }

    pub(super) async fn close(&self) -> Result<(), LixError> {
        let mut commit_rx = self.inner.commit_boundary.subscribe();
        loop {
            let commit_gate = if self.inner.commit_boundary.is_active() {
                None
            } else {
                self.inner.commit_boundary.try_lock_commit()
            };
            if let Some(_commit_gate) = commit_gate {
                {
                    let mut state = self.lock_state();
                    if state.has_explicit_transaction() {
                        return Err(active_transaction_error());
                    }
                    let active_operations = state.active_operations();
                    *state = if active_operations == 0 {
                        SessionTransactionState::Closed
                    } else {
                        SessionTransactionState::Closing { active_operations }
                    };
                }
                self.inner.state_changed.notify_waiters();
                break;
            }

            let notified = self.inner.state_changed.notified();
            tokio::select! {
                () = notified => {}
                result = commit_rx.changed() => {
                    if result.is_err() {
                        self.inner.state_changed.notify_waiters();
                    }
                }
            }
        }

        loop {
            let notified = self.inner.state_changed.notified();
            {
                let mut state = self.lock_state();
                let commit_count = *commit_rx.borrow_and_update();
                if state.active_operations() == 0
                    && commit_count == 0
                    && !self.inner.commit_boundary.is_active()
                {
                    *state = SessionTransactionState::Closed;
                    break;
                }
            }
            tokio::select! {
                () = notified => {}
                result = commit_rx.changed() => {
                    if result.is_err() {
                        self.inner.state_changed.notify_waiters();
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn is_closed(&self) -> bool {
        self.lock_state().is_closed()
    }

    pub(super) fn ensure_open(&self) -> Result<(), LixError> {
        if self.is_closed() {
            return Err(closed_error());
        }
        Ok(())
    }

    pub(super) fn ensure_observe_registration_allowed(&self) -> Result<(), LixError> {
        self.lock_state().ensure_observe_registration_allowed()
    }

    pub(super) async fn begin_waitable_session_operation(
        &self,
    ) -> Result<SessionOperationGuard, LixError> {
        loop {
            let notified = self.inner.state_changed.notified();
            let should_wait = {
                let mut state = self.lock_state();
                if state.is_automatic_transaction_in_progress() {
                    true
                } else {
                    state.begin_operation(SessionOperationScope::Session)?;
                    false
                }
            };
            if should_wait {
                notified.await;
                continue;
            }
            self.inner.state_changed.notify_waiters();

            if let Err(error) = self.ensure_open() {
                self.finish_operation();
                return Err(error);
            }

            return Ok(SessionOperationGuard {
                manager: self.clone(),
            });
        }
    }

    pub(super) fn begin_transaction_operation(&self) -> Result<SessionOperationGuard, LixError> {
        self.begin_operation(SessionOperationScope::Transaction)
    }

    pub(super) fn begin_transaction_commit_operation(
        &self,
    ) -> Result<SessionOperationGuard, LixError> {
        self.begin_operation(SessionOperationScope::TransactionCommit)
    }

    fn begin_operation(
        &self,
        scope: SessionOperationScope,
    ) -> Result<SessionOperationGuard, LixError> {
        {
            let mut state = self.lock_state();
            state.begin_operation(scope)?;
        }
        self.inner.state_changed.notify_waiters();

        if let Err(error) = self.ensure_open() {
            self.finish_operation();
            return Err(error);
        }

        Ok(SessionOperationGuard {
            manager: self.clone(),
        })
    }

    pub(super) async fn begin_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        loop {
            let notified = self.inner.state_changed.notified();
            let wait_for_session_operation = {
                let mut state = self.lock_state();
                if state.is_session_operation_in_progress() {
                    true
                } else {
                    state.begin_write_lease(TransactionOwner::Automatic)?;
                    false
                }
            };
            if wait_for_session_operation {
                notified.await;
                continue;
            }
            self.inner.state_changed.notify_waiters();
            return self.open_reserved_write_lease();
        }
    }

    pub(super) fn begin_explicit_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        self.begin_write_lease_for(TransactionOwner::ExplicitOpening)
    }

    fn begin_write_lease_for(
        &self,
        owner: TransactionOwner,
    ) -> Result<SessionWriteLease, LixError> {
        {
            let mut state = self.lock_state();
            state.begin_write_lease(owner)?;
        }
        self.inner.state_changed.notify_waiters();

        self.open_reserved_write_lease()
    }

    fn open_reserved_write_lease(&self) -> Result<SessionWriteLease, LixError> {
        let operation_guard = SessionOperationGuard {
            manager: self.clone(),
        };
        if let Err(error) = self.ensure_open() {
            drop(operation_guard);
            self.finish_transaction();
            return Err(error);
        }
        let transaction_guard = SessionTransactionGuard {
            manager: self.clone(),
        };
        Ok(SessionWriteLease {
            _transaction_guard: transaction_guard,
            _operation_guard: operation_guard,
        })
    }

    #[cfg(test)]
    pub(super) fn begin_commit(&self) -> CommitBoundaryGuard {
        self.inner.commit_boundary.begin()
    }

    pub(super) fn mark_explicit_transaction_open(&self) -> Result<(), LixError> {
        {
            let mut state = self.lock_state();
            state.mark_explicit_transaction_open()?;
        }
        self.inner.state_changed.notify_waiters();
        Ok(())
    }

    pub(super) fn transaction_commit_boundary(&self) -> TransactionCommitBoundary {
        let manager = self.clone();
        TransactionCommitBoundary::new(
            self.inner.commit_boundary.clone(),
            Arc::new(move || manager.ensure_open()),
        )
    }

    fn finish_operation(&self) {
        {
            let mut state = self.lock_state();
            state.finish_operation();
        }
        self.inner.state_changed.notify_waiters();
    }

    fn finish_transaction(&self) {
        {
            let mut state = self.lock_state();
            state.finish_transaction();
        }
        self.inner.state_changed.notify_waiters();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SessionTransactionState> {
        self.inner
            .state
            .lock()
            .expect("session transaction manager lock should not poison")
    }

    #[cfg(test)]
    pub(super) fn operation_count_for_test(&self) -> usize {
        self.lock_state().active_operations()
    }

    #[cfg(test)]
    pub(super) fn commit_in_progress_for_test(&self) -> bool {
        self.inner.commit_boundary.is_active()
    }

    #[cfg(test)]
    pub(super) fn active_transaction_for_test(&self) -> bool {
        matches!(
            *self.lock_state(),
            SessionTransactionState::OpenTransaction { .. }
        )
    }
}

impl SessionTransactionState {
    fn is_closed(&self) -> bool {
        matches!(self, Self::Closing { .. } | Self::Closed)
    }

    fn active_operations(&self) -> usize {
        match self {
            Self::OpenIdle | Self::Closed => 0,
            Self::OpenOperation { active_operations }
            | Self::OpenTransaction {
                active_operations, ..
            }
            | Self::Closing { active_operations } => *active_operations,
        }
    }

    fn has_explicit_transaction(&self) -> bool {
        matches!(
            self,
            Self::OpenTransaction {
                owner: TransactionOwner::Explicit,
                ..
            }
        )
    }

    fn is_session_operation_in_progress(&self) -> bool {
        matches!(self, Self::OpenOperation { .. })
    }

    fn is_automatic_transaction_in_progress(&self) -> bool {
        matches!(
            self,
            Self::OpenTransaction {
                owner: TransactionOwner::Automatic,
                ..
            }
        )
    }

    fn ensure_observe_registration_allowed(&self) -> Result<(), LixError> {
        match self {
            Self::OpenIdle
            | Self::OpenOperation { .. }
            | Self::OpenTransaction {
                owner: TransactionOwner::Automatic,
                ..
            } => Ok(()),
            Self::OpenTransaction { .. } => Err(active_transaction_error()),
            Self::Closing { .. } | Self::Closed => Err(closed_error()),
        }
    }

    fn begin_operation(&mut self, scope: SessionOperationScope) -> Result<(), LixError> {
        match self {
            Self::OpenIdle => {
                if matches!(scope, SessionOperationScope::Session) {
                    *self = Self::OpenOperation {
                        active_operations: 1,
                    };
                    Ok(())
                } else {
                    Err(active_transaction_error())
                }
            }
            Self::OpenOperation { active_operations } => {
                if matches!(scope, SessionOperationScope::Session) {
                    *active_operations += 1;
                    Ok(())
                } else {
                    Err(active_transaction_error())
                }
            }
            Self::OpenTransaction {
                active_operations,
                owner,
            } => match scope {
                SessionOperationScope::Session => Err(active_transaction_error()),
                SessionOperationScope::Transaction => {
                    *active_operations += 1;
                    Ok(())
                }
                SessionOperationScope::TransactionCommit => {
                    if *owner != TransactionOwner::Explicit {
                        return Err(active_transaction_error());
                    }
                    *owner = TransactionOwner::ExplicitCommitting;
                    *active_operations += 1;
                    Ok(())
                }
            },
            Self::Closing { .. } | Self::Closed => Err(closed_error()),
        }
    }

    fn begin_write_lease(&mut self, owner: TransactionOwner) -> Result<(), LixError> {
        match self {
            Self::OpenIdle => {
                *self = Self::OpenTransaction {
                    active_operations: 1,
                    owner,
                };
                Ok(())
            }
            Self::OpenOperation { .. } | Self::OpenTransaction { .. } => {
                Err(active_transaction_error())
            }
            Self::Closing { .. } | Self::Closed => Err(closed_error()),
        }
    }

    fn mark_explicit_transaction_open(&mut self) -> Result<(), LixError> {
        match self {
            Self::OpenTransaction {
                active_operations: 1,
                owner,
            } if *owner == TransactionOwner::ExplicitOpening => {
                *owner = TransactionOwner::Explicit;
                Ok(())
            }
            Self::Closing { .. } | Self::Closed => Err(closed_error()),
            _ => {
                panic!("explicit transaction should be opening before it is marked open");
            }
        }
    }

    fn finish_operation(&mut self) {
        match self {
            Self::OpenOperation { active_operations } => {
                *active_operations = active_operations
                    .checked_sub(1)
                    .expect("session operation count should not underflow");
                if *active_operations == 0 {
                    *self = Self::OpenIdle;
                }
            }
            Self::OpenTransaction {
                active_operations, ..
            } => {
                *active_operations = active_operations
                    .checked_sub(1)
                    .expect("session operation count should not underflow");
            }
            Self::Closing { active_operations } => {
                *active_operations = active_operations
                    .checked_sub(1)
                    .expect("session operation count should not underflow");
                if *active_operations == 0 {
                    *self = Self::Closed;
                }
            }
            Self::OpenIdle | Self::Closed => {
                panic!("session operation count should not underflow");
            }
        }
    }

    fn finish_transaction(&mut self) {
        match self {
            Self::OpenTransaction {
                active_operations: 0,
                ..
            } => {
                *self = Self::OpenIdle;
            }
            Self::OpenTransaction { .. } | Self::Closing { .. } | Self::Closed => {}
            Self::OpenIdle | Self::OpenOperation { .. } => {
                panic!("session transaction should be active before it is finished");
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SessionOperationScope {
    Session,
    Transaction,
    TransactionCommit,
}

fn active_transaction_error() -> LixError {
    transaction_state_error(
        "Lix handle has an active transaction; use the transaction handle for reads and writes until it is committed or rolled back",
    )
}

pub(super) struct SessionWriteLease {
    _operation_guard: SessionOperationGuard,
    _transaction_guard: SessionTransactionGuard,
}

pub(super) struct SessionTransactionGuard {
    manager: SessionTransactionManager,
}

impl Drop for SessionTransactionGuard {
    fn drop(&mut self) {
        self.manager.finish_transaction();
    }
}

pub(super) struct SessionOperationGuard {
    manager: SessionTransactionManager,
}

impl Drop for SessionOperationGuard {
    fn drop(&mut self) {
        self.manager.finish_operation();
    }
}
