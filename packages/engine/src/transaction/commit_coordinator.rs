use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Semaphore, oneshot};
use tracing::Instrument as _;

use super::{Transaction, TransactionCommitOutcome};
use crate::LixError;
use crate::functions::FunctionContext;
use crate::storage_adapter::Storage;

const COMMIT_QUEUE_CAPACITY: usize = 256;
const COMMIT_BATCH_CAPACITY: usize = 16;

type CommitFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;
type CommitJob = Box<dyn FnOnce() -> CommitFuture + Send + 'static>;

#[derive(Clone)]
pub(crate) struct CommitCoordinator {
    inner: Arc<CommitCoordinatorInner>,
}

struct CommitCoordinatorInner {
    collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    capacity: Arc<Semaphore>,
    state: Mutex<CommitCoordinatorState>,
    #[cfg(test)]
    stats: CommitCoordinatorStats,
}

#[derive(Default)]
struct CommitCoordinatorState {
    running: bool,
    queue: VecDeque<CommitJob>,
}

#[cfg(test)]
#[derive(Default)]
struct CommitCoordinatorStats {
    batch_count: AtomicUsize,
    commit_count: AtomicUsize,
    max_batch_size: AtomicUsize,
}

impl CommitCoordinator {
    pub(crate) fn new(collaboration_write_gate: Arc<tokio::sync::Mutex<()>>) -> Self {
        Self {
            inner: Arc::new(CommitCoordinatorInner {
                collaboration_write_gate,
                capacity: Arc::new(Semaphore::new(COMMIT_QUEUE_CAPACITY)),
                state: Mutex::new(CommitCoordinatorState::default()),
                #[cfg(test)]
                stats: CommitCoordinatorStats::default(),
            }),
        }
    }

    pub(crate) async fn commit<StorageImpl>(
        &self,
        transaction: Transaction<StorageImpl>,
        runtime_functions: FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let permit = Arc::clone(&self.inner.capacity)
            .acquire_owned()
            .await
            .map_err(|_| coordinator_closed())?;
        let (result, receive) = oneshot::channel();
        let job: CommitJob = Box::new(move || {
            Box::pin(async move {
                let outcome = transaction.commit(&runtime_functions).await;
                let _ = result.send(outcome);
                drop(permit);
            })
        });
        self.enqueue(job).await;
        receive.await.map_err(|_| coordinator_closed())?
    }

    async fn enqueue(&self, job: CommitJob) {
        let leads = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.queue.push_back(job);
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        if leads {
            self.drive().await;
        }
    }

    async fn drive(&self) {
        let mut driver = CommitDriverGuard::new(&self.inner);
        tokio::task::yield_now().await;
        loop {
            let batch = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let take = state.queue.len().min(COMMIT_BATCH_CAPACITY);
                if take == 0 {
                    state.running = false;
                    driver.disarm();
                    return;
                }
                state.queue.drain(..take).collect::<Vec<_>>()
            };
            #[cfg(test)]
            {
                self.inner.stats.batch_count.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .stats
                    .commit_count
                    .fetch_add(batch.len(), Ordering::Relaxed);
                self.inner
                    .stats
                    .max_batch_size
                    .fetch_max(batch.len(), Ordering::Relaxed);
            }
            let _gate = self
                .inner
                .collaboration_write_gate
                .lock()
                .instrument(tracing::debug_span!(
                    target: "lix_transaction",
                    "lix.transaction.commit_batch",
                    batch_size = batch.len(),
                ))
                .await;
            for job in batch {
                job().await;
            }
        }
    }

    #[cfg(test)]
    fn stats(&self) -> (usize, usize, usize) {
        (
            self.inner.stats.batch_count.load(Ordering::Relaxed),
            self.inner.stats.commit_count.load(Ordering::Relaxed),
            self.inner.stats.max_batch_size.load(Ordering::Relaxed),
        )
    }
}

struct CommitDriverGuard<'a> {
    inner: &'a CommitCoordinatorInner,
    armed: bool,
}

impl<'a> CommitDriverGuard<'a> {
    fn new(inner: &'a CommitCoordinatorInner) -> Self {
        Self { inner, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CommitDriverGuard<'_> {
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

    #[tokio::test]
    async fn concurrent_jobs_drain_in_bounded_cohorts() {
        let coordinator = CommitCoordinator::new(Arc::new(tokio::sync::Mutex::new(())));
        let completed = Arc::new(AtomicUsize::new(0));
        let jobs = (0..40).map(|_| {
            let coordinator = coordinator.clone();
            let completed = Arc::clone(&completed);
            async move {
                let _permit = Arc::clone(&coordinator.inner.capacity)
                    .acquire_owned()
                    .await
                    .unwrap();
                coordinator
                    .enqueue(Box::new(move || {
                        Box::pin(async move {
                            completed.fetch_add(1, Ordering::Relaxed);
                        })
                    }))
                    .await;
            }
        });
        futures_util::future::join_all(jobs).await;

        let (batches, commits, max_batch_size) = coordinator.stats();
        assert_eq!(completed.load(Ordering::Relaxed), 40);
        assert_eq!(commits, 40);
        assert!(batches >= 3);
        assert_eq!(max_batch_size, COMMIT_BATCH_CAPACITY);
    }
}
