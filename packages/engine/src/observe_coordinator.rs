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
    Blob(Vec<u8>),
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

impl ObserveQueryState {
    pub(crate) async fn evaluate<F, Fut>(
        &self,
        generation: u64,
        evaluate: F,
    ) -> Result<ExecuteResult, LixError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ExecuteResult, LixError>>,
    {
        let mut changes = self.subscribe();

        loop {
            {
                let mut inner = self.lock();
                if let Some(rows) = inner.cached.get(&generation) {
                    return Ok(rows.clone());
                }

                if !inner.in_flight.contains(&generation) {
                    inner.in_flight.insert(generation);
                    break;
                }

                let _ = changes.borrow_and_update();
            }

            if changes.changed().await.is_err() {
                return evaluate().await;
            }
        }

        let guard = InFlightGuard::new(self, generation);
        match evaluate().await {
            Ok(rows) => {
                guard.finish_success(rows.clone());
                Ok(rows)
            }
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
    cached: BTreeMap<u64, ExecuteResult>,
    in_flight: BTreeSet<u64>,
    change_sequence: u64,
}

struct InFlightGuard<'a> {
    state: &'a ObserveQueryState,
    generation: u64,
    active: bool,
}

impl<'a> InFlightGuard<'a> {
    fn new(state: &'a ObserveQueryState, generation: u64) -> Self {
        Self {
            state,
            generation,
            active: true,
        }
    }

    fn finish_success(mut self, rows: ExecuteResult) {
        self.finish(Some(rows));
    }

    fn finish_without_cache(mut self) {
        self.finish(None);
    }

    fn finish(&mut self, rows: Option<ExecuteResult>) {
        if !self.active {
            return;
        }
        self.active = false;
        let mut inner = self.state.lock();
        inner.in_flight.remove(&self.generation);
        if let Some(rows) = rows {
            inner.cached.insert(self.generation, rows);
            while inner.cached.len() > MAX_CACHED_GENERATIONS_PER_QUERY {
                let Some(oldest_generation) = inner.cached.first_key_value().map(|(key, _)| *key)
                else {
                    break;
                };
                inner.cached.remove(&oldest_generation);
            }
        }
        inner.change_sequence = inner.change_sequence.saturating_add(1);
        self.state.send_change(inner.change_sequence);
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.finish(None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::oneshot;

    use super::ObserveQueryState;
    use crate::ExecuteResult;

    #[tokio::test]
    async fn evaluate_cleans_up_in_flight_generation_when_leader_is_cancelled() {
        let state = Arc::new(ObserveQueryState::new());
        let (started_tx, started_rx) = oneshot::channel();
        let leader_state = Arc::clone(&state);
        let leader = tokio::spawn(async move {
            leader_state
                .evaluate(1, || async move {
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
            state.evaluate(1, || async { Ok(ExecuteResult::from_rows_affected(0)) }),
        )
        .await
        .expect("cancelled in-flight generation should not strand followers")
        .expect("replacement evaluation should succeed");

        assert_eq!(result, ExecuteResult::from_rows_affected(0));
    }

    #[tokio::test]
    async fn evaluate_does_not_cache_errors_for_generation() {
        let state = ObserveQueryState::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let first_attempts = Arc::clone(&attempts);
        let first = state
            .evaluate(1, || async move {
                first_attempts.fetch_add(1, Ordering::SeqCst);
                Err(crate::LixError::new(crate::LixError::CODE_UNKNOWN, "boom"))
            })
            .await;
        assert!(first.is_err());

        let second_attempts = Arc::clone(&attempts);
        let second = state
            .evaluate(1, || async move {
                second_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(ExecuteResult::from_rows_affected(0))
            })
            .await
            .expect("second evaluation should not reuse the first error");

        assert_eq!(second, ExecuteResult::from_rows_affected(0));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "failed evaluations must not be cached for followers"
        );
    }
}
