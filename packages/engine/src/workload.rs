use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;
#[cfg(test)]
use tokio::time::Instant;
#[cfg(not(test))]
use web_time::Instant;

use crate::LixError;

/// Scheduling intent for one SQL statement or atomic batch.
///
/// The coordinator never delays foreground admission. Opted-in background work
/// normally defers at statement or batch boundaries while foreground work is
/// active, with a bounded suppression period so a continuously busy foreground
/// cannot starve maintenance, indexing, or synchronization forever.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionPriority {
    #[default]
    Foreground,
    Background,
}

const FOREGROUND_QUIET_WINDOW: Duration = Duration::from_micros(250);
const MAX_BACKGROUND_SUPPRESSION: Duration = Duration::from_millis(5);
const MAX_BACKGROUND_QUEUE_DEPTH: usize = 64;

#[derive(Debug)]
pub(crate) struct WorkloadCoordinator {
    state: Mutex<CoordinatorState>,
    active_foreground: AtomicUsize,
    background_demand: AtomicBool,
}

#[derive(Debug)]
struct CoordinatorState {
    active_background: bool,
    background_queue: VecDeque<BackgroundWaiter>,
    next_waiter_id: u64,
    foreground_protect_until: Instant,
    background_epoch_deadline: Option<Instant>,
}

impl Default for WorkloadCoordinator {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(CoordinatorState {
                active_background: false,
                background_queue: VecDeque::new(),
                next_waiter_id: 0,
                foreground_protect_until: now,
                background_epoch_deadline: None,
            }),
            active_foreground: AtomicUsize::new(0),
            background_demand: AtomicBool::new(false),
        }
    }
}

impl WorkloadCoordinator {
    pub(crate) async fn acquire(
        &self,
        priority: ExecutionPriority,
    ) -> Result<WorkloadPermit<'_>, LixError> {
        match priority {
            ExecutionPriority::Foreground => Ok(self.acquire_foreground()),
            ExecutionPriority::Background => self.acquire_background().await,
        }
    }

    fn acquire_foreground(&self) -> WorkloadPermit<'_> {
        self.active_foreground.fetch_add(1, Ordering::AcqRel);
        if self.background_demand.load(Ordering::Acquire) {
            let now = Instant::now();
            let mut state = self.lock_state();
            state.foreground_protect_until = state
                .foreground_protect_until
                .max(now + FOREGROUND_QUIET_WINDOW);
        }
        WorkloadPermit {
            coordinator: self,
            priority: ExecutionPriority::Foreground,
            queue_wait: Duration::ZERO,
            queue_depth: 0,
        }
    }

    async fn acquire_background(&self) -> Result<WorkloadPermit<'_>, LixError> {
        let queued_at = Instant::now();
        let (waiter_id, queue_depth, notify) = {
            let now = Instant::now();
            let mut state = self.lock_state();
            if state.background_queue.len() >= MAX_BACKGROUND_QUEUE_DEPTH {
                return Err(LixError::new(
                    LixError::CODE_WORKLOAD_QUEUE_FULL,
                    "background execution queue is full",
                )
                .with_hint("Retry background work with backoff after admitted work completes.")
                .with_details(serde_json::json!({
                    "priority": "background",
                    "maxQueueDepth": MAX_BACKGROUND_QUEUE_DEPTH,
                })));
            }
            let waiter_id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
            let was_empty = state.background_queue.is_empty();
            let notify = Arc::new(Notify::new());
            state.background_queue.push_back(BackgroundWaiter {
                id: waiter_id,
                notify: Arc::clone(&notify),
            });
            self.background_demand.store(true, Ordering::Release);
            state.foreground_protect_until = state
                .foreground_protect_until
                .max(now + FOREGROUND_QUIET_WINDOW);
            if was_empty && !state.active_background {
                state.background_epoch_deadline = Some(now + MAX_BACKGROUND_SUPPRESSION);
            }
            (waiter_id, state.background_queue.len(), notify)
        };
        let mut queued = QueuedBackground {
            coordinator: self,
            waiter_id,
            active: true,
        };

        loop {
            let notified = notify.notified();
            let wake_at = {
                let now = Instant::now();
                let mut state = self.lock_state();
                if state.background_queue.front().map(|waiter| waiter.id) == Some(waiter_id)
                    && !state.active_background
                {
                    let foreground_active = self.active_foreground.load(Ordering::Acquire) != 0;
                    let quiet = !foreground_active && now >= state.foreground_protect_until;
                    let suppression_expired = state
                        .background_epoch_deadline
                        .is_some_and(|deadline| now >= deadline);
                    if quiet || suppression_expired {
                        let granted = state.background_queue.pop_front();
                        debug_assert_eq!(granted.map(|waiter| waiter.id), Some(waiter_id));
                        state.active_background = true;
                        state.background_epoch_deadline = None;
                        queued.active = false;
                        return Ok(WorkloadPermit {
                            coordinator: self,
                            priority: ExecutionPriority::Background,
                            queue_wait: queued_at.elapsed(),
                            queue_depth,
                        });
                    }

                    match (foreground_active, state.background_epoch_deadline) {
                        (false, Some(deadline)) => {
                            Some(deadline.min(state.foreground_protect_until))
                        }
                        (false, None) => Some(state.foreground_protect_until),
                        (true, deadline) => deadline,
                    }
                } else {
                    None
                }
            };

            if let Some(wake_at) = wake_at {
                if wake_at <= Instant::now() {
                    tokio::task::yield_now().await;
                } else {
                    tokio::select! {
                        () = notified => {},
                        () = sleep_until_deadline(wake_at) => {},
                    }
                }
            } else {
                notified.await;
            }
        }
    }

    fn release(&self, priority: ExecutionPriority) {
        if priority == ExecutionPriority::Foreground {
            if self.background_demand.load(Ordering::Acquire) {
                let now = Instant::now();
                let mut state = self.lock_state();
                state.foreground_protect_until = state
                    .foreground_protect_until
                    .max(now + FOREGROUND_QUIET_WINDOW);
                let previous = self.active_foreground.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(previous > 0);
            } else {
                let previous = self.active_foreground.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(previous > 0);
            }
            return;
        }

        let now = Instant::now();
        let mut state = self.lock_state();
        state.active_background = false;
        if !state.background_queue.is_empty() {
            state.background_epoch_deadline = Some(now + MAX_BACKGROUND_SUPPRESSION);
        } else {
            self.background_demand.store(false, Ordering::Release);
        }
        let next = state
            .background_queue
            .front()
            .map(|waiter| Arc::clone(&waiter.notify));
        drop(state);
        if let Some(next) = next {
            next.notify_one();
        }
    }

    fn cancel_background_waiter(&self, waiter_id: u64) {
        let now = Instant::now();
        let mut state = self.lock_state();
        let was_front = state.background_queue.front().map(|waiter| waiter.id) == Some(waiter_id);
        state
            .background_queue
            .retain(|queued| queued.id != waiter_id);
        if was_front && !state.active_background {
            state.background_epoch_deadline =
                (!state.background_queue.is_empty()).then_some(now + MAX_BACKGROUND_SUPPRESSION);
        }
        if state.background_queue.is_empty() && !state.active_background {
            self.background_demand.store(false, Ordering::Release);
        }
        let next = was_front
            .then(|| {
                state
                    .background_queue
                    .front()
                    .map(|waiter| Arc::clone(&waiter.notify))
            })
            .flatten();
        drop(state);
        if let Some(next) = next {
            next.notify_one();
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CoordinatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug)]
struct BackgroundWaiter {
    id: u64,
    notify: Arc<Notify>,
}

async fn sleep_until_deadline(deadline: Instant) {
    #[cfg(test)]
    tokio::time::sleep_until(deadline).await;

    #[cfg(not(test))]
    futures_timer::Delay::new(deadline.saturating_duration_since(Instant::now())).await;
}

#[derive(Debug)]
pub(crate) struct WorkloadPermit<'a> {
    coordinator: &'a WorkloadCoordinator,
    priority: ExecutionPriority,
    queue_wait: Duration,
    queue_depth: usize,
}

impl WorkloadPermit<'_> {
    pub(crate) fn queue_wait(&self) -> Duration {
        self.queue_wait
    }

    pub(crate) fn queue_depth(&self) -> usize {
        self.queue_depth
    }
}

impl Drop for WorkloadPermit<'_> {
    fn drop(&mut self) {
        self.coordinator.release(self.priority);
    }
}

struct QueuedBackground<'a> {
    coordinator: &'a WorkloadCoordinator,
    waiter_id: u64,
    active: bool,
}

impl Drop for QueuedBackground<'_> {
    fn drop(&mut self) {
        if self.active {
            self.coordinator.cancel_background_waiter(self.waiter_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn foreground_burst_suppresses_background_across_inter_call_gaps() {
        let coordinator = Arc::new(WorkloadCoordinator::default());
        let first = coordinator
            .acquire(ExecutionPriority::Foreground)
            .await
            .expect("foreground admission");
        let (entered_tx, mut entered_rx) = oneshot::channel();
        let background = Arc::clone(&coordinator);
        let task = tokio::spawn(async move {
            let _permit = background
                .acquire(ExecutionPriority::Background)
                .await
                .expect("background admission");
            let _ = entered_tx.send(());
        });

        tokio::task::yield_now().await;
        assert!(entered_rx.try_recv().is_err());
        drop(first);
        for _ in 0..1_000 {
            let foreground = coordinator
                .acquire(ExecutionPriority::Foreground)
                .await
                .expect("foreground admission");
            drop(foreground);
            assert!(entered_rx.try_recv().is_err());
        }
        tokio::time::advance(FOREGROUND_QUIET_WINDOW / 2).await;
        assert!(entered_rx.try_recv().is_err());

        tokio::time::advance(MAX_BACKGROUND_SUPPRESSION).await;
        entered_rx.await.expect("background eventually enters");
        task.await.expect("background task completes");
    }

    #[tokio::test(start_paused = true)]
    async fn foreground_never_waits_for_forced_background_progress() {
        let coordinator = Arc::new(WorkloadCoordinator::default());
        let foreground = coordinator
            .acquire(ExecutionPriority::Foreground)
            .await
            .expect("foreground admission");
        let background = Arc::clone(&coordinator);
        let (release_tx, release_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _permit = background
                .acquire(ExecutionPriority::Background)
                .await
                .expect("background admission");
            let _ = release_rx.await;
        });

        tokio::time::advance(MAX_BACKGROUND_SUPPRESSION).await;
        tokio::task::yield_now().await;
        let second_foreground = coordinator
            .acquire(ExecutionPriority::Foreground)
            .await
            .expect("foreground admission");
        assert_eq!(coordinator.active_foreground.load(Ordering::Acquire), 2);
        drop(second_foreground);
        drop(foreground);
        let _ = release_tx.send(());
        task.await.expect("background task completes");
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_queued_background_request_removes_it() {
        let coordinator = Arc::new(WorkloadCoordinator::default());
        let foreground = coordinator
            .acquire(ExecutionPriority::Foreground)
            .await
            .expect("foreground admission");
        let background = Arc::clone(&coordinator);
        let task = tokio::spawn(async move {
            let _permit = background
                .acquire(ExecutionPriority::Background)
                .await
                .expect("background admission");
        });
        tokio::task::yield_now().await;
        assert_eq!(coordinator.lock_state().background_queue.len(), 1);
        task.abort();
        let _ = task.await;
        assert!(coordinator.lock_state().background_queue.is_empty());
        drop(foreground);
    }

    #[tokio::test(start_paused = true)]
    async fn aborting_active_background_releases_the_next_waiter() {
        let coordinator = Arc::new(WorkloadCoordinator::default());
        tokio::time::advance(FOREGROUND_QUIET_WINDOW).await;
        let first_coordinator = Arc::clone(&coordinator);
        let (first_entered_tx, first_entered_rx) = oneshot::channel();
        let first = tokio::spawn(async move {
            let _permit = first_coordinator
                .acquire(ExecutionPriority::Background)
                .await
                .expect("first background admission");
            let _ = first_entered_tx.send(());
            std::future::pending::<()>().await;
        });
        first_entered_rx.await.expect("first background enters");

        let second_coordinator = Arc::clone(&coordinator);
        let (second_entered_tx, second_entered_rx) = oneshot::channel();
        let second = tokio::spawn(async move {
            let _permit = second_coordinator
                .acquire(ExecutionPriority::Background)
                .await
                .expect("second background admission");
            let _ = second_entered_tx.send(());
        });
        tokio::task::yield_now().await;
        first.abort();
        let _ = first.await;
        second_entered_rx.await.expect("second background enters");
        second.await.expect("second background completes");
    }

    #[tokio::test(start_paused = true)]
    async fn background_admission_queue_is_bounded() {
        let coordinator = Arc::new(WorkloadCoordinator::default());
        let foreground = coordinator
            .acquire(ExecutionPriority::Foreground)
            .await
            .expect("foreground admission");
        let mut queued = Vec::new();
        for _ in 0..MAX_BACKGROUND_QUEUE_DEPTH {
            let background = Arc::clone(&coordinator);
            queued.push(tokio::spawn(async move {
                let _permit = background.acquire(ExecutionPriority::Background).await;
            }));
        }
        while coordinator.lock_state().background_queue.len() < MAX_BACKGROUND_QUEUE_DEPTH {
            tokio::task::yield_now().await;
        }
        let error = coordinator
            .acquire(ExecutionPriority::Background)
            .await
            .expect_err("queue must reject excess background work");
        assert_eq!(error.code, LixError::CODE_WORKLOAD_QUEUE_FULL);
        for task in queued {
            task.abort();
            let _ = task.await;
        }
        drop(foreground);
    }
}
