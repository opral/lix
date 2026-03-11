use crate::engine::sql_ast::utils::parse_sql_statements;
use crate::engine::Engine;
use crate::errors;
use crate::query_runtime::dependency_spec::{
    dependency_spec_to_state_commit_stream_filter, derive_dependency_spec_from_statements,
};
use crate::state_commit_stream::StateCommitStream;
use crate::{LixError, QueryResult, Value, WireValue};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

const OBSERVE_TICK_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OBSERVE_FOLLOWER_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

impl ObserveQuery {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub rows: QueryResult,
    pub state_commit_sequence: Option<u64>,
}

pub struct ObserveEvents<'a> {
    engine: &'a Engine,
    state: ObserveState,
}

pub struct ObserveEventsOwned {
    engine: Arc<Engine>,
    state: ObserveState,
}

struct ObserveState {
    source_key: String,
    source: Arc<Mutex<SharedObserveSource>>,
    subscriber_id: u64,
    next_sequence: u64,
    closed: bool,
}

struct PollingGuard {
    source: Arc<Mutex<SharedObserveSource>>,
    armed: bool,
}

#[derive(Clone, Default)]
struct ObserveWriterKeyFilter {
    include: BTreeSet<String>,
    exclude: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct ObserveTickRow {
    tick_seq: i64,
    writer_key: Option<String>,
}

#[derive(Debug, Clone)]
struct SharedObserveEvent {
    generation: u64,
    rows: QueryResult,
    state_commit_sequence: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct SharedObserveSubscriberCursor {
    last_seen_generation: Option<u64>,
    initial_generation: Option<u64>,
}

pub(crate) struct SharedObserveSource {
    query: ObserveQuery,
    state_commits: StateCommitStream,
    writer_key_filter: ObserveWriterKeyFilter,
    last_seen_tick_seq: Option<i64>,
    last_result: Option<QueryResult>,
    latest_event: Option<SharedObserveEvent>,
    events: VecDeque<SharedObserveEvent>,
    next_generation: u64,
    initialized: bool,
    closed: bool,
    polling: bool,
    next_subscriber_id: u64,
    subscribers: BTreeMap<u64, SharedObserveSubscriberCursor>,
}

enum PollWork {
    Initial {
        query: ObserveQuery,
    },
    StateCommit {
        query: ObserveQuery,
        state_commit_sequence: u64,
    },
    External {
        query: ObserveQuery,
        last_seen_tick_seq: Option<i64>,
        writer_key_filter: ObserveWriterKeyFilter,
    },
}

struct PollOutcome {
    maybe_rows: Option<(QueryResult, Option<u64>)>,
    update_last_seen_tick_seq: Option<Option<i64>>,
    mark_initialized: bool,
}

impl SharedObserveSource {
    fn new(
        query: ObserveQuery,
        state_commits: StateCommitStream,
        writer_key_filter: ObserveWriterKeyFilter,
    ) -> Self {
        Self {
            query,
            state_commits,
            writer_key_filter,
            last_seen_tick_seq: None,
            last_result: None,
            latest_event: None,
            events: VecDeque::new(),
            next_generation: 0,
            initialized: false,
            closed: false,
            polling: false,
            next_subscriber_id: 1,
            subscribers: BTreeMap::new(),
        }
    }

    fn register_subscriber(&mut self) -> u64 {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id = self.next_subscriber_id.saturating_add(1);
        let initial_generation = if self.initialized {
            self.latest_event.as_ref().map(|event| event.generation)
        } else {
            None
        };
        self.subscribers.insert(
            subscriber_id,
            SharedObserveSubscriberCursor {
                last_seen_generation: None,
                initial_generation,
            },
        );
        subscriber_id
    }

    fn remove_subscriber(&mut self, subscriber_id: u64) {
        self.subscribers.remove(&subscriber_id);
        self.prune_events();
    }

    fn has_subscribers(&self) -> bool {
        !self.subscribers.is_empty()
    }

    fn append_event(&mut self, rows: QueryResult, state_commit_sequence: Option<u64>) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        let event = SharedObserveEvent {
            generation,
            rows,
            state_commit_sequence,
        };
        self.latest_event = Some(event.clone());
        self.events.push_back(event);
    }

    fn take_next_event_for_subscriber(&mut self, subscriber_id: u64) -> Option<SharedObserveEvent> {
        let cursor = self.subscribers.get_mut(&subscriber_id)?;

        if let Some(initial_generation) = cursor.initial_generation {
            if let Some(initial_event) = self
                .events
                .iter()
                .find(|event| event.generation == initial_generation)
                .cloned()
            {
                cursor.initial_generation = None;
                cursor.last_seen_generation = Some(initial_event.generation);
                self.prune_events();
                return Some(initial_event);
            }
            if let Some(initial_event) = self
                .latest_event
                .as_ref()
                .filter(|event| event.generation == initial_generation)
                .cloned()
            {
                cursor.initial_generation = None;
                cursor.last_seen_generation = Some(initial_event.generation);
                self.prune_events();
                return Some(initial_event);
            }
            cursor.initial_generation = None;
        }

        let next_event = self
            .events
            .iter()
            .find(|event| {
                cursor
                    .last_seen_generation
                    .is_none_or(|last_seen| event.generation > last_seen)
            })
            .cloned();

        if let Some(next_event) = &next_event {
            cursor.last_seen_generation = Some(next_event.generation);
            self.prune_events();
        }

        next_event
    }

    fn prune_events(&mut self) {
        if self.events.is_empty() {
            return;
        }

        if self.subscribers.is_empty() {
            self.events.clear();
            return;
        }

        let mut min_seen_generation: Option<u64> = None;
        for cursor in self.subscribers.values() {
            let Some(last_seen_generation) = cursor.last_seen_generation else {
                return;
            };
            min_seen_generation = Some(
                min_seen_generation
                    .map(|previous| previous.min(last_seen_generation))
                    .unwrap_or(last_seen_generation),
            );
        }

        let Some(min_seen_generation) = min_seen_generation else {
            return;
        };
        while self
            .events
            .front()
            .is_some_and(|event| event.generation <= min_seen_generation)
        {
            self.events.pop_front();
        }
    }
}

impl ObserveEvents<'_> {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine).await
    }

    pub fn close(&mut self) {
        self.state.close_with_engine(self.engine);
    }
}

impl Drop for ObserveEvents<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveEventsOwned {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine.as_ref()).await
    }

    pub fn close(&mut self) {
        self.state.close_with_engine(self.engine.as_ref());
    }
}

impl Drop for ObserveEventsOwned {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveState {
    async fn next_with_engine(
        &mut self,
        engine: &Engine,
    ) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed {
            return Ok(None);
        }

        loop {
            if self.closed {
                return Ok(None);
            }

            if let Some(shared_event) = self.try_take_shared_event()? {
                return Ok(Some(self.make_event(shared_event)));
            }

            let role = {
                let mut source = lock_shared_source(&self.source)?;
                if source.closed {
                    return Ok(None);
                }
                if source.polling {
                    false
                } else {
                    source.polling = true;
                    true
                }
            };

            if role {
                let mut polling_guard = PollingGuard::new(Arc::clone(&self.source));
                let poll_result = self.poll_shared_source_once(engine).await;
                if let Ok(mut source) = lock_shared_source(&self.source) {
                    source.polling = false;
                }
                polling_guard.disarm();
                poll_result?;
            } else {
                observe_poll_sleep(OBSERVE_FOLLOWER_POLL_INTERVAL).await;
            }
        }
    }

    fn try_take_shared_event(&mut self) -> Result<Option<SharedObserveEvent>, LixError> {
        let mut source = lock_shared_source(&self.source)?;
        if source.closed {
            return Ok(None);
        }
        Ok(source.take_next_event_for_subscriber(self.subscriber_id))
    }

    async fn poll_shared_source_once(&mut self, engine: &Engine) -> Result<(), LixError> {
        let work = {
            let source = lock_shared_source(&self.source)?;
            if source.closed {
                return Ok(());
            }

            if !source.initialized {
                PollWork::Initial {
                    query: source.query.clone(),
                }
            } else if let Some(batch) = source.state_commits.try_next() {
                PollWork::StateCommit {
                    query: source.query.clone(),
                    state_commit_sequence: batch.sequence,
                }
            } else {
                PollWork::External {
                    query: source.query.clone(),
                    last_seen_tick_seq: source.last_seen_tick_seq,
                    writer_key_filter: source.writer_key_filter.clone(),
                }
            }
        };

        let outcome = match work {
            PollWork::Initial { query } => {
                let rows = execute_observe_query(engine, &query).await?;
                let latest_tick_seq = latest_observe_tick_seq(engine).await?;
                PollOutcome {
                    maybe_rows: Some((rows, None)),
                    update_last_seen_tick_seq: Some(latest_tick_seq),
                    mark_initialized: true,
                }
            }
            PollWork::StateCommit {
                query,
                state_commit_sequence,
            } => {
                let rows = execute_observe_query(engine, &query).await?;
                PollOutcome {
                    maybe_rows: Some((rows, Some(state_commit_sequence))),
                    update_last_seen_tick_seq: None,
                    mark_initialized: true,
                }
            }
            PollWork::External {
                query,
                last_seen_tick_seq,
                writer_key_filter,
            } => {
                observe_poll_sleep(OBSERVE_TICK_POLL_INTERVAL).await;
                let observed_ticks = observe_ticks_since(engine, last_seen_tick_seq).await?;
                if observed_ticks.is_empty() {
                    PollOutcome {
                        maybe_rows: None,
                        update_last_seen_tick_seq: None,
                        mark_initialized: true,
                    }
                } else {
                    let mut next_last_seen_tick_seq = last_seen_tick_seq;
                    let mut should_reexecute = false;
                    for tick in observed_ticks {
                        next_last_seen_tick_seq = Some(tick.tick_seq);
                        if writer_key_filter.matches_external_tick(tick.writer_key.as_deref()) {
                            should_reexecute = true;
                        }
                    }

                    if !should_reexecute {
                        PollOutcome {
                            maybe_rows: None,
                            update_last_seen_tick_seq: Some(next_last_seen_tick_seq),
                            mark_initialized: true,
                        }
                    } else {
                        let rows = execute_observe_query(engine, &query).await?;
                        PollOutcome {
                            maybe_rows: Some((rows, None)),
                            update_last_seen_tick_seq: Some(next_last_seen_tick_seq),
                            mark_initialized: true,
                        }
                    }
                }
            }
        };

        self.apply_poll_outcome(outcome)
    }

    fn apply_poll_outcome(&self, outcome: PollOutcome) -> Result<(), LixError> {
        let mut source = lock_shared_source(&self.source)?;
        if source.closed {
            return Ok(());
        }

        if outcome.mark_initialized {
            source.initialized = true;
        }
        if let Some(last_seen_tick_seq) = outcome.update_last_seen_tick_seq {
            source.last_seen_tick_seq = last_seen_tick_seq;
        }

        if let Some((rows, state_commit_sequence)) = outcome.maybe_rows {
            let changed = source
                .last_result
                .as_ref()
                .is_none_or(|previous| *previous != rows);
            if changed {
                source.last_result = Some(rows.clone());
                source.append_event(rows, state_commit_sequence);
            }
        }

        Ok(())
    }

    fn make_event(&mut self, shared_event: SharedObserveEvent) -> ObserveEvent {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        ObserveEvent {
            sequence,
            rows: shared_event.rows,
            state_commit_sequence: shared_event.state_commit_sequence,
        }
    }

    fn close_with_engine(&mut self, engine: &Engine) {
        if self.closed {
            return;
        }
        self.closed = true;
        let _ = unregister_observe_subscriber(
            engine,
            &self.source_key,
            &self.source,
            self.subscriber_id,
        );
    }
}

impl PollingGuard {
    fn new(source: Arc<Mutex<SharedObserveSource>>) -> Self {
        Self {
            source,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PollingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut source) = self.source.lock() {
            source.polling = false;
        }
    }
}

impl ObserveWriterKeyFilter {
    fn matches_external_tick(&self, writer_key: Option<&str>) -> bool {
        if !self.include.is_empty() {
            let Some(writer_key) = writer_key else {
                return false;
            };
            if !self.include.contains(writer_key) {
                return false;
            }
        }

        if let Some(writer_key) = writer_key {
            if self.exclude.contains(writer_key) {
                return false;
            }
        }

        true
    }
}

async fn execute_observe_query(
    engine: &Engine,
    query: &ObserveQuery,
) -> Result<QueryResult, LixError> {
    let result = Box::pin(engine.execute(&query.sql, &query.params)).await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "observe query",
            1,
            result.statements.len(),
        ));
    };
    Ok(statement.clone())
}

impl Engine {
    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEvents<'_>, LixError> {
        let state = build_observe_state(self, query)?;
        Ok(ObserveEvents {
            engine: self,
            state,
        })
    }
}

pub fn observe_owned(
    engine: Arc<Engine>,
    query: ObserveQuery,
) -> Result<ObserveEventsOwned, LixError> {
    let state = build_observe_state(engine.as_ref(), query)?;
    Ok(ObserveEventsOwned { engine, state })
}

fn build_observe_state(engine: &Engine, query: ObserveQuery) -> Result<ObserveState, LixError> {
    let source_key = observe_source_key(&query)?;
    let source = acquire_or_create_shared_source(engine, &source_key, query)?;
    let subscriber_id = {
        let mut shared = lock_shared_source(&source)?;
        if shared.closed {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "observe shared source is closed".to_string(),
            });
        }
        shared.register_subscriber()
    };

    Ok(ObserveState {
        source_key,
        source,
        subscriber_id,
        next_sequence: 0,
        closed: false,
    })
}

fn observe_source_key(query: &ObserveQuery) -> Result<String, LixError> {
    let wire_params = query
        .params
        .iter()
        .map(WireValue::try_from_engine)
        .collect::<Result<Vec<_>, _>>()?;
    let params = serde_json::to_string(&wire_params).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("failed to serialize observe wire params for dedup key: {error}"),
    })?;
    Ok(format!("{}\n--params:{params}", query.sql))
}

fn acquire_or_create_shared_source(
    engine: &Engine,
    source_key: &str,
    query: ObserveQuery,
) -> Result<Arc<Mutex<SharedObserveSource>>, LixError> {
    loop {
        if let Some(existing_source) = lock_observe_registry(engine)?.get(source_key).cloned() {
            let is_closed = lock_shared_source(&existing_source)?.closed;
            if is_closed {
                let mut registry = lock_observe_registry(engine)?;
                if registry
                    .get(source_key)
                    .is_some_and(|current| Arc::ptr_eq(current, &existing_source))
                {
                    registry.remove(source_key);
                }
                continue;
            }
            return Ok(existing_source);
        }

        let new_source = Arc::new(Mutex::new(build_shared_observe_source(
            engine,
            query.clone(),
        )?));
        let mut registry = lock_observe_registry(engine)?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            registry.entry(source_key.to_string())
        {
            entry.insert(Arc::clone(&new_source));
            return Ok(new_source);
        }
    }
}

fn build_shared_observe_source(
    engine: &Engine,
    query: ObserveQuery,
) -> Result<SharedObserveSource, LixError> {
    let statements = parse_sql_statements(&query.sql)?;
    if statements.is_empty()
        || !statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "observe requires one or more SELECT statements".to_string(),
        });
    }

    let dependency_spec = derive_dependency_spec_from_statements(&statements, &query.params)?;
    let filter = dependency_spec_to_state_commit_stream_filter(&dependency_spec);
    let writer_key_filter = ObserveWriterKeyFilter {
        include: filter.writer_keys.iter().cloned().collect(),
        exclude: filter.exclude_writer_keys.iter().cloned().collect(),
    };
    let state_commits = engine.state_commit_stream(filter);

    Ok(SharedObserveSource::new(
        query,
        state_commits,
        writer_key_filter,
    ))
}

fn unregister_observe_subscriber(
    engine: &Engine,
    source_key: &str,
    source: &Arc<Mutex<SharedObserveSource>>,
    subscriber_id: u64,
) -> Result<(), LixError> {
    let should_remove_registry_entry = {
        let mut shared = lock_shared_source(source)?;
        if shared.closed {
            true
        } else {
            shared.remove_subscriber(subscriber_id);
            if shared.has_subscribers() {
                false
            } else {
                shared.closed = true;
                shared.state_commits.close();
                true
            }
        }
    };

    if should_remove_registry_entry {
        let mut registry = lock_observe_registry(engine)?;
        if registry
            .get(source_key)
            .is_some_and(|current| Arc::ptr_eq(current, source))
        {
            registry.remove(source_key);
        }
    }

    Ok(())
}

fn lock_observe_registry<'a>(
    engine: &'a Engine,
) -> Result<MutexGuard<'a, BTreeMap<String, Arc<Mutex<SharedObserveSource>>>>, LixError> {
    engine.observe_shared_sources.lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "observe shared source registry lock poisoned".to_string(),
    })
}

fn lock_shared_source<'a>(
    source: &'a Arc<Mutex<SharedObserveSource>>,
) -> Result<MutexGuard<'a, SharedObserveSource>, LixError> {
    source.lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "observe shared source lock poisoned".to_string(),
    })
}

#[cfg(not(target_arch = "wasm32"))]
async fn observe_poll_sleep(duration: Duration) {
    futures_timer::Delay::new(duration).await;
}

#[cfg(target_arch = "wasm32")]
async fn observe_poll_sleep(duration: Duration) {
    let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

async fn latest_observe_tick_seq(engine: &Engine) -> Result<Option<i64>, LixError> {
    let result = Box::pin(engine.execute_backend_sql(
        "SELECT tick_seq \
         FROM lix_internal_observe_tick \
         ORDER BY tick_seq DESC \
         LIMIT 1",
        &[],
    ))
    .await?;
    let Some(first_row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(first_value) = first_row.first() else {
        return Ok(None);
    };
    Ok(Some(parse_observe_tick_seq(first_value)?))
}

async fn observe_ticks_since(
    engine: &Engine,
    last_seen_tick_seq: Option<i64>,
) -> Result<Vec<ObserveTickRow>, LixError> {
    let result = if let Some(last_seen) = last_seen_tick_seq {
        Box::pin(engine.execute_backend_sql(
            "SELECT tick_seq, writer_key \
             FROM lix_internal_observe_tick \
             WHERE tick_seq > $1 \
             ORDER BY tick_seq ASC",
            &[Value::Integer(last_seen)],
        ))
        .await?
    } else {
        Box::pin(engine.execute_backend_sql(
            "SELECT tick_seq, writer_key \
             FROM lix_internal_observe_tick \
             ORDER BY tick_seq ASC",
            &[],
        ))
        .await?
    };

    let mut ticks = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        let tick_seq = parse_observe_tick_seq(row.first().ok_or(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "failed to read observe tick sequence: row has no tick_seq column".to_string(),
        })?)?;

        let writer_key =
            parse_observe_tick_writer_key(
                row.get(1).ok_or(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description:
                        "failed to read observe tick writer key: row has no writer_key column"
                            .to_string(),
                })?,
            )?;

        ticks.push(ObserveTickRow {
            tick_seq,
            writer_key,
        });
    }
    Ok(ticks)
}

fn parse_observe_tick_seq(value: &Value) -> Result<i64, LixError> {
    match value {
        Value::Integer(value) => Ok(*value),
        Value::Real(value) => Ok(*value as i64),
        Value::Text(value) => value.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick sequence text: {error}"),
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick sequence value: {other:?}"),
        }),
    }
}

fn parse_observe_tick_writer_key(value: &Value) -> Result<Option<String>, LixError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick writer key value: {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        observe_source_key, ObserveEvent, ObserveEvents, ObserveQuery, OBSERVE_TICK_POLL_INTERVAL,
    };
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::{boot, BootArgs, LixError, NoopWasmRuntime, QueryResult, Value};
    use async_trait::async_trait;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    async fn next_observe_event(observed: &mut ObserveEvents<'_>, label: &str) -> ObserveEvent {
        tokio::time::timeout(Duration::from_secs(1), observed.next())
            .await
            .unwrap_or_else(|_| panic!("{label} next should not time out"))
            .unwrap_or_else(|error| panic!("{label} next should succeed: {error:?}"))
            .unwrap_or_else(|| panic!("{label} event should exist"))
    }

    fn run_observe_test_with_large_stack<F, Fut>(factory: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("observe-test".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("tokio runtime")
                    .block_on(factory());
            })
            .expect("spawn observe test thread")
            .join()
            .expect("observe test thread should not panic");
    }

    struct CountingObserveBackend {
        observe_query_hits: Arc<AtomicUsize>,
    }

    struct CountingObserveTransaction;

    #[async_trait(?Send)]
    impl LixBackend for CountingObserveBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("observe-shared-sentinel") {
                self.observe_query_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![Value::Text("observe-shared-sentinel".to_string())]],
                    columns: vec!["marker".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_observe_tick") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["tick_seq".to_string(), "writer_key".to_string()],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(CountingObserveTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for CountingObserveTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[test]
    fn observe_dedups_initial_query_execution_across_identical_subscribers() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let engine = boot(BootArgs::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            ));
            engine.set_active_version_id("version-test".to_string());

            let query = ObserveQuery::new("SELECT 'observe-shared-sentinel' AS marker", vec![]);
            let mut observed_a = engine
                .observe(query.clone())
                .expect("observe should succeed");
            let mut observed_b = engine.observe(query).expect("observe should succeed");

            let event_a = next_observe_event(&mut observed_a, "observe_a").await;
            let event_b = next_observe_event(&mut observed_b, "observe_b").await;

            assert_eq!(
                event_a.rows.rows,
                vec![vec![Value::Text("observe-shared-sentinel".to_string())]]
            );
            assert_eq!(
                event_b.rows.rows,
                vec![vec![Value::Text("observe-shared-sentinel".to_string())]]
            );
            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                1,
                "identical observe subscribers should share initial query execution"
            );
        });
    }

    #[test]
    fn observe_late_subscriber_reuses_cached_initial_snapshot() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let engine = boot(BootArgs::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            ));
            engine.set_active_version_id("version-test".to_string());

            let query = ObserveQuery::new("SELECT 'observe-shared-sentinel' AS marker", vec![]);
            let mut observed_a = engine
                .observe(query.clone())
                .expect("observe should succeed");

            let _initial_a = next_observe_event(&mut observed_a, "observe_a").await;

            let mut observed_b = engine.observe(query).expect("observe should succeed");
            let event_b = next_observe_event(&mut observed_b, "observe_b").await;

            assert_eq!(
                event_b.rows.rows,
                vec![vec![Value::Text("observe-shared-sentinel".to_string())]]
            );
            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                1,
                "late identical subscriber should reuse shared cached initial snapshot"
            );

            observed_a.close();
            observed_b.close();
            tokio::time::sleep(OBSERVE_TICK_POLL_INTERVAL).await;

            let mut observed_c = engine
                .observe(ObserveQuery::new(
                    "SELECT 'observe-shared-sentinel' AS marker",
                    vec![],
                ))
                .expect("observe should succeed");
            let _initial_c = next_observe_event(&mut observed_c, "observe_c").await;

            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                2,
                "new observer after all subscribers close should execute a fresh initial query"
            );

            let _ = engine
                .execute("SELECT 1", &[])
                .await
                .expect("sanity execute should succeed");
        });
    }

    #[test]
    fn observe_source_key_serializes_params_with_canonical_wire_kinds() {
        let query = ObserveQuery::new(
            "SELECT ?1, ?2, ?3, ?4, ?5, ?6",
            vec![
                Value::Null,
                Value::Boolean(true),
                Value::Integer(7),
                Value::Real(1.25),
                Value::Text("hello".to_string()),
                Value::Blob(vec![1, 2, 3]),
            ],
        );

        let key = observe_source_key(&query).expect("observe source key should be generated");
        assert!(key.contains("\"kind\":\"null\""));
        assert!(key.contains("\"kind\":\"bool\""));
        assert!(key.contains("\"kind\":\"int\""));
        assert!(key.contains("\"kind\":\"float\""));
        assert!(key.contains("\"kind\":\"text\""));
        assert!(key.contains("\"kind\":\"blob\""));
        assert!(!key.contains("\"kind\":\"Null\""));
        assert!(!key.contains("\"kind\":\"Bool\""));
        assert!(!key.contains("\"kind\":\"Integer\""));
        assert!(!key.contains("\"kind\":\"Real\""));
        assert!(!key.contains("\"kind\":\"Text\""));
        assert!(!key.contains("\"kind\":\"Blob\""));
    }

    #[test]
    fn observe_source_key_is_stable_for_identical_query_and_params() {
        let query_a = ObserveQuery::new(
            "SELECT ?1, ?2",
            vec![Value::Text("same".to_string()), Value::Integer(1)],
        );
        let query_b = ObserveQuery::new(
            "SELECT ?1, ?2",
            vec![Value::Text("same".to_string()), Value::Integer(1)],
        );

        let key_a = observe_source_key(&query_a).expect("first key should be generated");
        let key_b = observe_source_key(&query_b).expect("second key should be generated");
        assert_eq!(key_a, key_b);
    }
}
