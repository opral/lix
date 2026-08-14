use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, oneshot};
use tracing::Instrument as _;

use super::{
    Transaction, TransactionCommitOutcome, commit_transaction_cohort,
    transaction_is_file_cohort_eligible, transactions_can_share_cohort,
};
use crate::LixError;
use crate::functions::FunctionContext;
use crate::observe_invalidation::ObserveInvalidation;
use crate::storage_adapter::Storage;

const COMMIT_QUEUE_CAPACITY: usize = 256;
const COMMIT_COHORT_CAPACITY: usize = 256;
struct CommitRequest<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    transaction: Transaction<StorageImpl>,
    runtime_functions: FunctionContext,
    result: oneshot::Sender<Result<TransactionCommitOutcome, LixError>>,
    file_cohort_eligible: bool,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct CommitCoordinator<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    inner: Arc<CommitCoordinatorInner<StorageImpl>>,
}

struct CommitCoordinatorInner<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    observe_invalidation: Arc<ObserveInvalidation>,
    capacity: Arc<Semaphore>,
    state: Mutex<CommitCoordinatorState<StorageImpl>>,
    #[cfg(test)]
    stats: CommitCoordinatorStats,
}

struct CommitCoordinatorState<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    running: bool,
    queue: VecDeque<CommitRequest<StorageImpl>>,
}

impl<StorageImpl> Default for CommitCoordinatorState<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    fn default() -> Self {
        Self {
            running: false,
            queue: VecDeque::new(),
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct CommitCoordinatorStats {
    cohort_count: AtomicUsize,
    commit_count: AtomicUsize,
    max_cohort_size: AtomicUsize,
}

impl<StorageImpl> CommitCoordinator<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
        observe_invalidation: Arc<ObserveInvalidation>,
    ) -> Self {
        Self {
            inner: Arc::new(CommitCoordinatorInner {
                collaboration_write_gate,
                observe_invalidation,
                capacity: Arc::new(Semaphore::new(COMMIT_QUEUE_CAPACITY)),
                state: Mutex::new(CommitCoordinatorState::default()),
                #[cfg(test)]
                stats: CommitCoordinatorStats::default(),
            }),
        }
    }

    pub(crate) async fn commit(
        &self,
        transaction: Transaction<StorageImpl>,
        runtime_functions: FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let capacity = Arc::clone(&self.inner.capacity)
            .acquire_owned()
            .await
            .map_err(|_| coordinator_closed())?;
        let file_cohort_eligible = transaction_is_file_cohort_eligible(&transaction);
        let (result, receive) = oneshot::channel();
        let leads = self.enqueue(CommitRequest {
            transaction,
            runtime_functions,
            result,
            file_cohort_eligible,
            _capacity: capacity,
        });
        if leads {
            #[cfg(not(target_family = "wasm"))]
            self.spawn_driver()?;
            #[cfg(target_family = "wasm")]
            self.drive().await;
        }
        receive.await.map_err(|_| coordinator_closed())?
    }

    #[cfg(not(target_family = "wasm"))]
    fn spawn_driver(&self) -> Result<(), LixError> {
        let coordinator = self.clone();
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("lix-commit-coordinator".to_owned())
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                runtime.block_on(coordinator.drive());
            })
            .map(|_| ())
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("start transaction commit coordinator: {error}"),
                )
            })
    }

    fn enqueue(&self, request: CommitRequest<StorageImpl>) -> bool {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.queue.push_back(request);
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        }
    }

    async fn drive(&self) {
        let mut driver = CommitDriverGuard::new(&self.inner);
        #[cfg(target_family = "wasm")]
        tokio::task::yield_now().await;
        loop {
            let cohort = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let take = state.queue.len().min(COMMIT_COHORT_CAPACITY);
                if take == 0 {
                    state.running = false;
                    driver.disarm();
                    return;
                }
                let compatible = state
                    .queue
                    .front()
                    .map(|leader| {
                        state
                            .queue
                            .iter()
                            .take(take)
                            .take_while(|candidate| {
                                transactions_can_share_cohort(
                                    &leader.transaction,
                                    &candidate.transaction,
                                    leader.file_cohort_eligible,
                                    candidate.file_cohort_eligible,
                                )
                            })
                            .count()
                    })
                    .unwrap_or(0);
                // An ineligible request is an intentional singleton and may
                // not poison the compatible semantic wave behind it.
                let take = compatible.max(1);
                state.queue.drain(..take).collect::<Vec<_>>()
            };
            #[cfg(test)]
            {
                self.inner
                    .stats
                    .cohort_count
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .stats
                    .commit_count
                    .fetch_add(cohort.len(), Ordering::Relaxed);
                self.inner
                    .stats
                    .max_cohort_size
                    .fetch_max(cohort.len(), Ordering::Relaxed);
            }
            let _gate = self
                .inner
                .collaboration_write_gate
                .lock()
                .instrument(tracing::debug_span!(
                    target: "lix_transaction",
                    "lix.transaction.commit_cohort",
                    cohort_size = cohort.len(),
                ))
                .await;
            let mut senders = Vec::with_capacity(cohort.len());
            let mut inputs = Vec::with_capacity(cohort.len());
            for request in cohort {
                senders.push((request.result, request._capacity));
                inputs.push((request.transaction, request.runtime_functions));
            }
            let mut outcomes = Box::pin(commit_transaction_cohort(inputs)).await;
            if let Some(outcome) = outcomes.iter().find_map(|result| result.as_ref().ok()) {
                self.inner
                    .observe_invalidation
                    .bump_if_storage_changed(&outcome.storage_stats);
            }
            for outcome in outcomes.iter_mut().flatten() {
                *outcome = TransactionCommitOutcome::default();
            }
            debug_assert_eq!(outcomes.len(), senders.len());
            for ((sender, _capacity), outcome) in senders.into_iter().zip(outcomes) {
                let _ = sender.send(outcome);
            }
        }
    }

    #[cfg(test)]
    fn stats(&self) -> (usize, usize, usize) {
        (
            self.inner.stats.cohort_count.load(Ordering::Relaxed),
            self.inner.stats.commit_count.load(Ordering::Relaxed),
            self.inner.stats.max_cohort_size.load(Ordering::Relaxed),
        )
    }
}

struct CommitDriverGuard<'a, StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    inner: &'a CommitCoordinatorInner<StorageImpl>,
    armed: bool,
}

impl<'a, StorageImpl> CommitDriverGuard<'a, StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    fn new(inner: &'a CommitCoordinatorInner<StorageImpl>) -> Self {
        Self { inner, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<StorageImpl> Drop for CommitDriverGuard<'_, StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.running = false;
        state.queue.clear();
    }
}

fn coordinator_closed() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "transaction commit coordinator closed unexpectedly",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::Memory;

    #[test]
    fn coordinator_capacity_accepts_realtime_collaboration_wave() {
        assert!(COMMIT_COHORT_CAPACITY >= 100);
        assert!(COMMIT_QUEUE_CAPACITY >= COMMIT_COHORT_CAPACITY);
        let coordinator = CommitCoordinator::<Memory>::new(
            Arc::new(tokio::sync::Mutex::new(())),
            Arc::new(ObserveInvalidation::new()),
        );
        assert_eq!(coordinator.stats(), (0, 0, 0));
    }
}
