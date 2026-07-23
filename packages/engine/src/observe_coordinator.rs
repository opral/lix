use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::watch;

use crate::{ExecuteResult, LixError, Value};

const MAX_CACHED_GENERATIONS_PER_QUERY: usize = 4;
const MAX_WEAK_QUERY_ENTRIES_BEFORE_PRUNE: usize = 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ObserveSessionScope {
    Workspace,
    Pinned(String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObserveQueryKey {
    scope: ObserveSessionScope,
    sql: String,
    params: Vec<ObserveParamKey>,
}

impl ObserveQueryKey {
    pub(crate) fn new(
        scope: ObserveSessionScope,
        sql: impl Into<String>,
        params: &[Value],
    ) -> Result<Self, LixError> {
        let params = params
            .iter()
            .map(ObserveParamKey::from_value)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            scope,
            sql: sql.into(),
            params,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObserveParamKey {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(u64),
    Text(String),
    Json(String),
    Blob(crate::Blob),
}

impl ObserveParamKey {
    fn from_value(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Boolean(value) => Ok(Self::Boolean(*value)),
            Value::Integer(value) => Ok(Self::Integer(*value)),
            Value::Real(value) => Ok(Self::Real(value.to_bits())),
            Value::Text(value) => Ok(Self::Text(value.clone())),
            Value::Json(value) => {
                let json = serde_json::to_string(value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_UNKNOWN,
                        format!("failed to serialize observe JSON parameter: {error}"),
                    )
                })?;
                Ok(Self::Json(json))
            }
            Value::Blob(value) => Ok(Self::Blob(value.clone())),
        }
    }
}

impl Hash for ObserveParamKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::Integer(value) => value.hash(state),
            Self::Real(value) => value.hash(state),
            Self::Text(value) | Self::Json(value) => value.hash(state),
            Self::Blob(value) => value.hash(state),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ObserveCoordinator {
    queries: Mutex<HashMap<ObserveQueryKey, Weak<ObserveQueryState>>>,
}

impl ObserveCoordinator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn state_for(&self, key: &ObserveQueryKey) -> Arc<ObserveQueryState> {
        let mut queries = self
            .queries
            .lock()
            .expect("observe coordinator lock should not poison");
        if let Some(state) = queries.get(key).and_then(Weak::upgrade) {
            return state;
        }
        if queries.len() > MAX_WEAK_QUERY_ENTRIES_BEFORE_PRUNE {
            queries.retain(|_, state| state.strong_count() > 0);
        }

        let state = Arc::new(ObserveQueryState::new());
        queries.insert(key.clone(), Arc::downgrade(&state));
        state
    }
}

#[derive(Debug)]
pub(crate) struct ObserveQueryState {
    inner: Mutex<ObserveQueryStateInner>,
    changes: watch::Sender<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObserveSharedContent {
    pub(crate) generation: u64,
    pub(crate) compared_generation: Option<u64>,
    pub(crate) matches_compared_generation: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ObserveQueryEvaluation {
    pub(crate) rows: ExecuteResult,
    pub(crate) shared_content: Option<ObserveSharedContent>,
}

impl ObserveQueryEvaluation {
    pub(crate) fn unshared(rows: ExecuteResult) -> Self {
        Self {
            rows,
            shared_content: None,
        }
    }

    pub(crate) fn rows_changed_since(
        &self,
        last_rows: Option<&ExecuteResult>,
        last_shared_content: Option<ObserveSharedContent>,
    ) -> bool {
        if let (Some(previous), Some(current)) = (last_shared_content, self.shared_content)
            && current.compared_generation == Some(previous.generation)
        {
            return !current.matches_compared_generation;
        }

        last_rows.is_none_or(|last_rows| *last_rows != self.rows)
    }
}

impl ObserveQueryState {
    pub(crate) async fn evaluate<F, Fut>(
        &self,
        generation: u64,
        compare_results: bool,
        evaluate: F,
    ) -> Result<ObserveQueryEvaluation, LixError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ExecuteResult, LixError>>,
    {
        let mut changes = self.subscribe();

        loop {
            {
                let mut inner = self.lock();
                if let Some(cached) = inner.cached.get(&generation) {
                    return Ok(cached.evaluation(generation));
                }

                if inner.in_flight.insert(generation) {
                    break;
                }

                let _ = changes.borrow_and_update();
            }

            if changes.changed().await.is_err() {
                return evaluate().await.map(ObserveQueryEvaluation::unshared);
            }
        }

        let guard = InFlightGuard::new(self, generation, compare_results);
        match evaluate().await {
            Ok(rows) => Ok(guard.finish_success(rows)),
            Err(error) => {
                guard.finish_without_cache();
                Err(error)
            }
        }
    }

    fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Mutex::new(ObserveQueryStateInner::default()),
            changes,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObserveQueryStateInner> {
        self.inner
            .lock()
            .expect("observe query state lock should not poison")
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn send_change(&self, sequence: u64) {
        self.changes.send_replace(sequence);
    }
}

#[derive(Debug, Default)]
struct ObserveQueryStateInner {
    cached: BTreeMap<u64, CachedObserveResult>,
    in_flight: BTreeSet<u64>,
    change_sequence: u64,
}

#[derive(Clone, Debug)]
struct CachedObserveResult {
    rows: ExecuteResult,
    compared_generation: Option<u64>,
    matches_compared_generation: bool,
}

impl CachedObserveResult {
    fn evaluation(&self, generation: u64) -> ObserveQueryEvaluation {
        ObserveQueryEvaluation {
            rows: self.rows.clone(),
            shared_content: Some(ObserveSharedContent {
                generation,
                compared_generation: self.compared_generation,
                matches_compared_generation: self.matches_compared_generation,
            }),
        }
    }
}

struct InFlightGuard<'a> {
    state: &'a ObserveQueryState,
    generation: u64,
    compare_results: bool,
    active: bool,
}

impl<'a> InFlightGuard<'a> {
    fn new(state: &'a ObserveQueryState, generation: u64, compare_results: bool) -> Self {
        Self {
            state,
            generation,
            compare_results,
            active: true,
        }
    }

    fn finish_success(mut self, rows: ExecuteResult) -> ObserveQueryEvaluation {
        self.finish(Some(rows))
            .expect("successful observe evaluation must produce a cached result")
    }

    fn finish_without_cache(mut self) {
        let _ = self.finish(None);
    }

    fn finish(&mut self, rows: Option<ExecuteResult>) -> Option<ObserveQueryEvaluation> {
        if !self.active {
            return None;
        }
        self.active = false;
        let mut inner = self.state.lock();
        inner.in_flight.remove(&self.generation);
        let evaluation = rows.map(|rows| {
            let (compared_generation, matches_compared_generation) = if self.compare_results {
                inner
                    .cached
                    .range(..self.generation)
                    .next_back()
                    .map(|(generation, cached)| (Some(*generation), cached.rows == rows))
                    .unwrap_or((None, false))
            } else {
                (None, false)
            };
            let cached = CachedObserveResult {
                rows,
                compared_generation,
                matches_compared_generation,
            };
            let evaluation = cached.evaluation(self.generation);
            inner.cached.insert(self.generation, cached);
            while inner.cached.len() > MAX_CACHED_GENERATIONS_PER_QUERY {
                let Some(oldest_generation) = inner.cached.first_key_value().map(|(key, _)| *key)
                else {
                    break;
                };
                inner.cached.remove(&oldest_generation);
            }
            evaluation
        });
        inner.change_sequence = inner.change_sequence.saturating_add(1);
        self.state.send_change(inner.change_sequence);
        evaluation
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        let _ = self.finish(None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::oneshot;

    use super::{ObserveQueryEvaluation, ObserveQueryState, ObserveSharedContent};
    use crate::{ExecuteResult, Value};

    fn blob_result(byte: u8) -> ExecuteResult {
        ExecuteResult::from_rows(
            vec!["data".to_string()],
            vec![vec![Value::Blob(vec![byte; 1024].into())]],
        )
    }

    fn blob_pointer(result: &ExecuteResult) -> *const u8 {
        match result.rows()[0].get_index(0) {
            Some(Value::Blob(bytes)) => bytes.as_ptr(),
            value => panic!("expected blob result, got {value:?}"),
        }
    }

    #[tokio::test]
    async fn evaluate_cleans_up_in_flight_generation_when_leader_is_cancelled() {
        let state = Arc::new(ObserveQueryState::new());
        let (started_tx, started_rx) = oneshot::channel();
        let leader_state = Arc::clone(&state);
        let leader = tokio::spawn(async move {
            leader_state
                .evaluate(1, false, || async move {
                    let _ = started_tx.send(());
                    std::future::pending().await
                })
                .await
        });
        started_rx
            .await
            .expect("leader evaluation should have started");

        leader.abort();
        let _ = leader.await;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            state.evaluate(1, false, || async {
                Ok(ExecuteResult::from_rows_affected(0))
            }),
        )
        .await
        .expect("cancelled in-flight generation should not strand followers")
        .expect("replacement evaluation should succeed");

        assert_eq!(result.rows, ExecuteResult::from_rows_affected(0));
    }

    #[tokio::test]
    async fn evaluate_does_not_cache_errors_for_generation() {
        let state = ObserveQueryState::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let first_attempts = Arc::clone(&attempts);
        let first = state
            .evaluate(1, false, || async move {
                first_attempts.fetch_add(1, Ordering::SeqCst);
                Err(crate::LixError::new(crate::LixError::CODE_UNKNOWN, "boom"))
            })
            .await;
        assert!(first.is_err());

        let second_attempts = Arc::clone(&attempts);
        let second = state
            .evaluate(1, false, || async move {
                second_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(ExecuteResult::from_rows_affected(0))
            })
            .await
            .expect("second evaluation should not reuse the first error");

        assert_eq!(second.rows, ExecuteResult::from_rows_affected(0));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "failed evaluations must not be cached for followers"
        );
    }

    #[tokio::test]
    async fn identical_generations_share_one_comparison_without_reusing_results() {
        let state = ObserveQueryState::new();
        let first = state
            .evaluate(1, true, || async { Ok(blob_result(b'a')) })
            .await
            .expect("first evaluation should succeed");
        let second = state
            .evaluate(2, true, || async { Ok(blob_result(b'a')) })
            .await
            .expect("second evaluation should succeed");

        let first_content = first
            .shared_content
            .expect("coordinated result should carry comparison metadata");
        let second_content = second
            .shared_content
            .expect("coordinated result should carry comparison metadata");
        assert_eq!(first_content.compared_generation, None);
        assert_eq!(second_content.compared_generation, Some(1));
        assert!(second_content.matches_compared_generation);
        assert_ne!(
            blob_pointer(&first.rows),
            blob_pointer(&second.rows),
            "each generation must retain its freshly evaluated result"
        );
    }

    #[tokio::test]
    async fn comparison_starts_without_losing_single_observer_generation_identity() {
        let state = ObserveQueryState::new();
        let first = state
            .evaluate(1, false, || async { Ok(blob_result(b'a')) })
            .await
            .expect("first evaluation should succeed");
        let second = state
            .evaluate(2, false, || async { Ok(blob_result(b'a')) })
            .await
            .expect("second evaluation should succeed");
        let third = state
            .evaluate(3, true, || async { Ok(blob_result(b'a')) })
            .await
            .expect("fanout evaluation should succeed");

        let first_content = first
            .shared_content
            .expect("first generation should carry its identity");
        let second_content = second
            .shared_content
            .expect("second generation should carry its identity");
        let third_content = third
            .shared_content
            .expect("fanout generation should carry its comparison");
        assert_eq!(first_content.generation, 1);
        assert_eq!(first_content.compared_generation, None);
        assert_eq!(second_content.generation, 2);
        assert_eq!(second_content.compared_generation, None);
        assert_eq!(third_content.compared_generation, Some(2));
        assert!(third_content.matches_compared_generation);
    }

    #[tokio::test]
    async fn changed_and_out_of_order_generations_keep_direct_comparisons() {
        let state = ObserveQueryState::new();
        let first = state
            .evaluate(1, true, || async { Ok(blob_result(b'a')) })
            .await
            .expect("first evaluation should succeed");
        let third = state
            .evaluate(3, true, || async { Ok(blob_result(b'a')) })
            .await
            .expect("third evaluation should succeed");
        let second = state
            .evaluate(2, true, || async { Ok(blob_result(b'b')) })
            .await
            .expect("late second evaluation should succeed");

        let first_content = first.shared_content.expect("first comparison metadata");
        let third_content = third.shared_content.expect("third comparison metadata");
        let second_content = second.shared_content.expect("second comparison metadata");
        assert_eq!(first_content.compared_generation, None);
        assert_eq!(third_content.compared_generation, Some(1));
        assert!(third_content.matches_compared_generation);
        assert_eq!(second_content.compared_generation, Some(1));
        assert!(!second_content.matches_compared_generation);

        let skipped_equal = ObserveQueryEvaluation {
            rows: blob_result(b'a'),
            shared_content: Some(ObserveSharedContent {
                generation: 4,
                compared_generation: Some(3),
                matches_compared_generation: false,
            }),
        };
        assert!(
            !skipped_equal.rows_changed_since(Some(&first.rows), Some(first_content)),
            "a skipped comparison must fall back to exact row equality"
        );

        let cached_third = state
            .evaluate(3, true, || async {
                panic!("cached generation must not re-evaluate")
            })
            .await
            .expect("cached third evaluation should succeed");
        assert_eq!(cached_third.shared_content, Some(third_content));
    }
}

#[cfg(test)]
mod performance_tests;
