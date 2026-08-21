use std::collections::VecDeque;
#[cfg(not(test))]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, oneshot};
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use super::{
    Transaction, TransactionCommitOutcome, commit_transaction_cohort,
    transaction_is_file_cohort_eligible, transactions_can_share_cohort,
};
use crate::LixError;
use crate::functions::FunctionContext;
use crate::observe_invalidation::ObserveInvalidation;
use crate::storage_adapter::Storage;
use crate::telemetry::{
    ActiveTelemetrySpan, TelemetryAttribute, TelemetryContext, TelemetrySink, TelemetrySpanLink,
    TelemetrySpanStatus, current_telemetry_context, next_commit_cohort_id, spans,
};

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
    telemetry_context: Option<TelemetryContext>,
    tracing_parent: tracing::Span,
    tracing_dispatch: tracing::Dispatch,
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
    telemetry: Option<Arc<dyn TelemetrySink>>,
    checkpoint_gc_running: AtomicBool,
    checkpoint_gc_not_before_sequence: AtomicU64,
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
        telemetry: Option<Arc<dyn TelemetrySink>>,
    ) -> Self {
        Self {
            inner: Arc::new(CommitCoordinatorInner {
                collaboration_write_gate,
                observe_invalidation,
                capacity: Arc::new(Semaphore::new(COMMIT_QUEUE_CAPACITY)),
                telemetry,
                checkpoint_gc_running: AtomicBool::new(false),
                checkpoint_gc_not_before_sequence: AtomicU64::new(0),
                state: Mutex::new(CommitCoordinatorState::default()),
                #[cfg(test)]
                stats: CommitCoordinatorStats::default(),
            }),
        }
    }

    /// Coalesces repository-wide checkpoint maintenance across every session
    /// sharing this coordinator. Foreground checkpoints only attempt this
    /// atomic transition; they never wait for maintenance ownership.
    pub(crate) fn try_begin_checkpoint_gc(&self, checkpoint_sequence: u64) -> bool {
        if checkpoint_sequence
            < self
                .inner
                .checkpoint_gc_not_before_sequence
                .load(Ordering::Acquire)
        {
            return false;
        }
        self.inner
            .checkpoint_gc_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Process-local fallback when even the durable failure counter loses its
    /// CAS race. This keeps sustained contention from immediately launching a
    /// fresh full repository plan at the next checkpoint.
    pub(crate) fn defer_checkpoint_gc_until(&self, checkpoint_sequence: u64) {
        self.inner
            .checkpoint_gc_not_before_sequence
            .fetch_max(checkpoint_sequence, Ordering::AcqRel);
    }

    pub(crate) fn finish_checkpoint_gc(&self) {
        self.inner
            .checkpoint_gc_running
            .store(false, Ordering::Release);
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
        let telemetry_context = current_telemetry_context().or_else(|| {
            self.inner
                .telemetry
                .as_ref()
                .map(|sink| TelemetryContext::root(Arc::clone(sink)))
        });
        let (result, receive) = oneshot::channel();
        let leads = self.enqueue(CommitRequest {
            transaction,
            runtime_functions,
            result,
            file_cohort_eligible,
            telemetry_context,
            tracing_parent: tracing::Span::current(),
            tracing_dispatch: tracing::dispatcher::get_default(Clone::clone),
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
        crate::background_task::spawn("lix-commit-coordinator", move || async move {
            // `background_task` block_on-pins this future on a default 2 MiB
            // thread. Keep `drive` itself on the heap so commit wrappers cannot
            // inflate that stack.
            Box::pin(coordinator.drive()).await;
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
            let transaction_count = cohort.len();
            let telemetry_context = cohort_telemetry_context(&cohort);
            let tracing_parent = cohort.first().map_or_else(tracing::Span::none, |request| {
                request.tracing_parent.clone()
            });
            let tracing_dispatch = cohort
                .first()
                .map(|request| request.tracing_dispatch.clone());
            let mut senders = Vec::with_capacity(transaction_count);
            let mut inputs = Vec::with_capacity(cohort.len());
            for request in cohort {
                senders.push((request.result, request._capacity));
                inputs.push((request.transaction, request.runtime_functions));
            }
            let commit_and_notify = async {
                let outcomes = Box::pin(commit_transaction_cohort(inputs)).await;
                if let Some(outcome) = outcomes.iter().find_map(|result| result.as_ref().ok()) {
                    let notify = ActiveTelemetrySpan::start_current(
                        &spans::TRANSACTION_NOTIFY,
                        vec![TelemetryAttribute::u64(
                            "lix.transaction.count",
                            u64::try_from(transaction_count).unwrap_or(u64::MAX),
                        )],
                    );
                    let _entered = notify.as_ref().map(ActiveTelemetrySpan::enter);
                    self.inner
                        .observe_invalidation
                        .bump_if_storage_changed(&outcome.storage_stats);
                    drop(_entered);
                    if let Some(notify) = notify {
                        notify.finish(TelemetrySpanStatus::Ok, Vec::new());
                    }
                }
                outcomes
            }
            .instrument(tracing_parent);
            let commit_and_notify = match tracing_dispatch {
                Some(dispatch) => commit_and_notify.with_subscriber(dispatch),
                None => commit_and_notify.with_current_subscriber(),
            };
            let mut outcomes = match telemetry_context.as_ref() {
                Some(context) => Box::pin(context.instrument(commit_and_notify)).await,
                None => Box::pin(commit_and_notify).await,
            };
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

fn cohort_telemetry_context<StorageImpl>(
    cohort: &[CommitRequest<StorageImpl>],
) -> Option<TelemetryContext>
where
    StorageImpl: Storage + 'static,
{
    attach_cohort_parent_contexts(cohort.iter().filter_map(|request| request.telemetry_context.clone()))
}

fn attach_cohort_parent_contexts(
    contexts: impl IntoIterator<Item = TelemetryContext>,
) -> Option<TelemetryContext> {
    let contexts = contexts.into_iter().collect::<Vec<_>>();
    let links = contexts
        .iter()
        .filter_map(TelemetryContext::as_link)
        .collect::<Vec<TelemetrySpanLink>>();
    contexts.into_iter().next().map(|context| {
        context
            .with_commit_cohort_id(next_commit_cohort_id())
            .with_links(links)
    })
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
    use crate::telemetry::{CallbackTelemetrySink, TelemetryContext};

    #[test]
    fn cohort_context_links_every_logical_transaction() {
        let completed = Mutex::new(Vec::new());
        let captured = Arc::new(completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new({
            let captured = Arc::clone(&captured);
            move |span| captured.lock().expect("spans").push(span)
        }));
        let context = attach_cohort_parent_contexts([
            TelemetryContext::for_test(Arc::clone(&sink), "trace-a", "span-a"),
            TelemetryContext::for_test(Arc::clone(&sink), "trace-b", "span-b"),
        ])
        .expect("cohort context");
        futures_lite::future::block_on(TelemetryContext::instrument(
            &context,
            async {
                let span = ActiveTelemetrySpan::start_current(
                    &spans::TRANSACTION_STORAGE,
                    vec![TelemetryAttribute::u64("lix.transaction.count", 2)],
                )
                .expect("storage enabled");
                span.finish(TelemetrySpanStatus::Ok, Vec::new());
            },
        ));
        let spans = captured.lock().expect("spans");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start.name, "lix.transaction.storage");
        assert_eq!(spans[0].start.parent_span_id.as_deref(), Some("span-a"));
        assert_eq!(spans[0].start.links.len(), 2);
        assert_eq!(spans[0].start.links[0].span_id, "span-a");
        assert_eq!(spans[0].start.links[1].span_id, "span-b");
        assert!(
            spans[0]
                .start
                .attributes
                .iter()
                .any(|attribute| attribute.key == "lix.commit_cohort_id")
        );
    }

    #[test]
    fn coordinator_capacity_accepts_realtime_collaboration_wave() {
        assert!(COMMIT_COHORT_CAPACITY >= 100);
        assert!(COMMIT_QUEUE_CAPACITY >= COMMIT_COHORT_CAPACITY);
        let coordinator = CommitCoordinator::<Memory>::new(
            Arc::new(tokio::sync::Mutex::new(())),
            Arc::new(ObserveInvalidation::new()),
            None,
        );
        assert_eq!(coordinator.stats(), (0, 0, 0));
    }
}
