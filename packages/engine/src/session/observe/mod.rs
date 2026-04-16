use crate::common::{unexpected_statement_count_error, WireValue};
use crate::session::Session;
use crate::sql::{
    dependency_spec_to_state_commit_stream_filter, derive_dependency_spec, parse_sql_statements,
    QueryDependency,
};
use crate::streams::StateCommitStream;
use crate::{LixError, QueryResult, Value, WriteReceipt};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

const OBSERVE_TICK_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OBSERVE_FOLLOWER_POLL_INTERVAL: Duration = Duration::from_millis(25);
pub(crate) const OBSERVE_TICK_TABLE: &str = "lix_internal_observe_tick";
mod init;

pub(crate) use init::init;

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

/// Delivery policy for reactive observe subscriptions.
///
/// These filters apply to delivery metadata carried on state-commit batches and
/// observe ticks. They do not inspect row-visible columns or require query SQL
/// to reference `origin_key`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveOptions {
    /// Suppress changes authored by this session's effective `origin_key`.
    #[serde(default)]
    pub exclude_self: bool,
    /// Only deliver batches whose delivery metadata carries one of these
    /// origin keys.
    #[serde(default)]
    pub include_origin_keys: Vec<String>,
    /// Drop batches whose delivery metadata carries one of these origin keys.
    #[serde(default)]
    pub exclude_origin_keys: Vec<String>,
}

impl ObserveOptions {
    pub fn exclude_self() -> Self {
        Self {
            exclude_self: true,
            ..Self::default()
        }
    }
}

/// Observe result event.
///
/// `state_commit_sequence` is the matching delivery-sequence fence from the
/// underlying state-commit stream when the event was caused by a committed
/// state change. Pure session-state or external tick re-executions leave it
/// as `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub rows: QueryResult,
    pub state_commit_sequence: Option<u64>,
}

pub struct ObserveEvents<'a> {
    session: &'a Session,
    state: ObserveState,
}

pub struct ObserveEventsOwned {
    session: Arc<Session>,
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
struct ObserveOriginFilter {
    include: BTreeSet<String>,
    exclude: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ResolvedObserveOptions {
    include_origin_keys: Vec<String>,
    exclude_origin_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct ObserveTickRow {
    tick_seq: i64,
    origin_key: Option<String>,
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
    origin_filter: ObserveOriginFilter,
    query_dependencies: BTreeSet<QueryDependency>,
    last_seen_tick_seq: Option<i64>,
    last_seen_query_dependency_generations: BTreeMap<QueryDependency, u64>,
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
        query_dependency_generations: BTreeMap<QueryDependency, u64>,
    },
    SessionState {
        query: ObserveQuery,
        query_dependency_generations: BTreeMap<QueryDependency, u64>,
    },
    StateCommit {
        query: ObserveQuery,
        state_commit_sequence: u64,
        query_dependency_generations: BTreeMap<QueryDependency, u64>,
    },
    External {
        query: ObserveQuery,
        last_seen_tick_seq: Option<i64>,
        origin_filter: ObserveOriginFilter,
        query_dependency_generations: BTreeMap<QueryDependency, u64>,
    },
}

struct PollOutcome {
    maybe_rows: Option<(QueryResult, Option<u64>)>,
    update_last_seen_tick_seq: Option<Option<i64>>,
    update_last_seen_query_dependency_generations: Option<BTreeMap<QueryDependency, u64>>,
    mark_initialized: bool,
}

impl SharedObserveSource {
    fn new(
        query: ObserveQuery,
        state_commits: StateCommitStream,
        origin_filter: ObserveOriginFilter,
        query_dependencies: BTreeSet<QueryDependency>,
    ) -> Self {
        Self {
            query,
            state_commits,
            origin_filter,
            query_dependencies,
            last_seen_tick_seq: None,
            last_seen_query_dependency_generations: BTreeMap::new(),
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

    fn query_dependency_generations_changed(
        &self,
        query_dependency_generations: &BTreeMap<QueryDependency, u64>,
    ) -> bool {
        self.query_dependencies.iter().any(|dependency| {
            self.last_seen_query_dependency_generations
                .get(dependency)
                .copied()
                .unwrap_or_default()
                != query_dependency_generations
                    .get(dependency)
                    .copied()
                    .unwrap_or_default()
        })
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
        self.state.next_with_session(self.session).await
    }

    /// Waits until this subscription observes the matching write receipt.
    ///
    /// This is the preferred optimistic-UI acknowledgement path for callers
    /// that want the concrete observe update, not just the global fence.
    pub async fn wait_for_write_receipt(
        &mut self,
        receipt: &WriteReceipt,
    ) -> Result<Option<ObserveEvent>, LixError> {
        let Some(target_sequence) = receipt.state_commit_sequence else {
            return Ok(None);
        };

        loop {
            let Some(event) = self.next().await? else {
                return Ok(None);
            };
            if event
                .state_commit_sequence
                .is_some_and(|sequence| sequence >= target_sequence)
            {
                return Ok(Some(event));
            }
        }
    }

    pub fn close(&mut self) {
        self.state.close_with_session(self.session);
    }
}

impl Drop for ObserveEvents<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveEventsOwned {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_session(self.session.as_ref()).await
    }

    /// Owned-session variant of [`ObserveEvents::wait_for_write_receipt`].
    pub async fn wait_for_write_receipt(
        &mut self,
        receipt: &WriteReceipt,
    ) -> Result<Option<ObserveEvent>, LixError> {
        let Some(target_sequence) = receipt.state_commit_sequence else {
            return Ok(None);
        };

        loop {
            let Some(event) = self.next().await? else {
                return Ok(None);
            };
            if event
                .state_commit_sequence
                .is_some_and(|sequence| sequence >= target_sequence)
            {
                return Ok(Some(event));
            }
        }
    }

    pub fn close(&mut self) {
        self.state.close_with_session(self.session.as_ref());
    }
}

impl Drop for ObserveEventsOwned {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveState {
    async fn next_with_session(
        &mut self,
        session: &Session,
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
                let poll_result = self.poll_shared_source_once(session).await;
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

    async fn poll_shared_source_once(&mut self, session: &Session) -> Result<(), LixError> {
        let work = {
            let source = lock_shared_source(&self.source)?;
            let query_dependency_generations =
                session.dependency_generations(&source.query_dependencies);
            if source.closed {
                return Ok(());
            }

            if !source.initialized {
                PollWork::Initial {
                    query: source.query.clone(),
                    query_dependency_generations,
                }
            } else if source.query_dependency_generations_changed(&query_dependency_generations) {
                PollWork::SessionState {
                    query: source.query.clone(),
                    query_dependency_generations,
                }
            } else if let Some(batch) = source.state_commits.try_next() {
                PollWork::StateCommit {
                    query: source.query.clone(),
                    state_commit_sequence: batch.sequence,
                    query_dependency_generations,
                }
            } else {
                PollWork::External {
                    query: source.query.clone(),
                    last_seen_tick_seq: source.last_seen_tick_seq,
                    origin_filter: source.origin_filter.clone(),
                    query_dependency_generations,
                }
            }
        };

        let outcome = match work {
            PollWork::Initial {
                query,
                query_dependency_generations,
            } => {
                let latest_tick_seq =
                    latest_observe_tick_seq(session.session_host().backend().as_ref()).await?;
                let rows = execute_observe_query(session, &query).await?;
                PollOutcome {
                    maybe_rows: Some((rows, None)),
                    update_last_seen_tick_seq: Some(latest_tick_seq),
                    update_last_seen_query_dependency_generations: Some(
                        query_dependency_generations,
                    ),
                    mark_initialized: true,
                }
            }
            PollWork::SessionState {
                query,
                query_dependency_generations,
            } => {
                let rows = execute_observe_query(session, &query).await?;
                PollOutcome {
                    maybe_rows: Some((rows, None)),
                    update_last_seen_tick_seq: None,
                    update_last_seen_query_dependency_generations: Some(
                        query_dependency_generations,
                    ),
                    mark_initialized: true,
                }
            }
            PollWork::StateCommit {
                query,
                state_commit_sequence,
                query_dependency_generations,
            } => {
                let rows = execute_observe_query(session, &query).await?;
                PollOutcome {
                    maybe_rows: Some((rows, Some(state_commit_sequence))),
                    update_last_seen_tick_seq: None,
                    update_last_seen_query_dependency_generations: Some(
                        query_dependency_generations,
                    ),
                    mark_initialized: true,
                }
            }
            PollWork::External {
                query,
                last_seen_tick_seq,
                origin_filter,
                query_dependency_generations,
            } => {
                observe_poll_sleep(OBSERVE_TICK_POLL_INTERVAL).await;
                let observed_ticks = observe_ticks_since(
                    session.session_host().backend().as_ref(),
                    last_seen_tick_seq,
                )
                .await?;
                if observed_ticks.is_empty() {
                    PollOutcome {
                        maybe_rows: None,
                        update_last_seen_tick_seq: None,
                        update_last_seen_query_dependency_generations: Some(
                            query_dependency_generations,
                        ),
                        mark_initialized: true,
                    }
                } else {
                    let mut next_last_seen_tick_seq = last_seen_tick_seq;
                    let mut should_reexecute = false;
                    for tick in observed_ticks {
                        next_last_seen_tick_seq = Some(tick.tick_seq);
                        if origin_filter.matches_external_tick(tick.origin_key.as_deref()) {
                            should_reexecute = true;
                        }
                    }

                    if !should_reexecute {
                        PollOutcome {
                            maybe_rows: None,
                            update_last_seen_tick_seq: Some(next_last_seen_tick_seq),
                            update_last_seen_query_dependency_generations: Some(
                                query_dependency_generations,
                            ),
                            mark_initialized: true,
                        }
                    } else {
                        let rows = execute_observe_query(session, &query).await?;
                        PollOutcome {
                            maybe_rows: Some((rows, None)),
                            update_last_seen_tick_seq: Some(next_last_seen_tick_seq),
                            update_last_seen_query_dependency_generations: Some(
                                query_dependency_generations,
                            ),
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
        if let Some(last_seen_query_dependency_generations) =
            outcome.update_last_seen_query_dependency_generations
        {
            source.last_seen_query_dependency_generations = last_seen_query_dependency_generations;
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

    fn close_with_session(&mut self, session: &Session) {
        if self.closed {
            return;
        }
        self.closed = true;
        let _ = unregister_observe_subscriber(
            session,
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

impl ObserveOriginFilter {
    fn matches_external_tick(&self, origin_key: Option<&str>) -> bool {
        if !self.include.is_empty() {
            let Some(origin_key) = origin_key else {
                return false;
            };
            if !self.include.contains(origin_key) {
                return false;
            }
        }

        if let Some(origin_key) = origin_key {
            if self.exclude.contains(origin_key) {
                return false;
            }
        }

        true
    }
}

async fn execute_observe_query(
    session: &Session,
    query: &ObserveQuery,
) -> Result<QueryResult, LixError> {
    let result = Box::pin(session.execute(&query.sql, &query.params)).await?;
    extract_single_observe_query_result(result)
}

fn extract_single_observe_query_result(
    result: crate::ExecuteResult,
) -> Result<QueryResult, LixError> {
    let [statement] = result.statements.as_slice() else {
        return Err(unexpected_statement_count_error(
            "observe query",
            1,
            result.statements.len(),
        ));
    };
    Ok(statement.clone())
}

impl Session {
    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEvents<'_>, LixError> {
        self.observe_with_options(query, ObserveOptions::default())
    }

    pub fn observe_with_options(
        &self,
        query: ObserveQuery,
        options: ObserveOptions,
    ) -> Result<ObserveEvents<'_>, LixError> {
        let state = build_observe_state(self, query, options)?;
        Ok(ObserveEvents {
            session: self,
            state,
        })
    }

    pub(crate) fn observe_owned(
        session: Arc<Self>,
        query: ObserveQuery,
    ) -> Result<ObserveEventsOwned, LixError> {
        Self::observe_owned_with_options(session, query, ObserveOptions::default())
    }

    pub(crate) fn observe_owned_with_options(
        session: Arc<Self>,
        query: ObserveQuery,
        options: ObserveOptions,
    ) -> Result<ObserveEventsOwned, LixError> {
        let state = build_observe_state(session.as_ref(), query, options)?;
        Ok(ObserveEventsOwned { session, state })
    }
}

fn build_observe_state(
    session: &Session,
    query: ObserveQuery,
    options: ObserveOptions,
) -> Result<ObserveState, LixError> {
    let resolved_options = resolve_observe_options(session, &options);
    let source_key = observe_source_key_for_session(session, &query, &resolved_options)?;
    let source = acquire_or_create_shared_source(session, &source_key, query, resolved_options)?;
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

fn observe_source_key_for_session(
    session: &Session,
    query: &ObserveQuery,
    options: &ResolvedObserveOptions,
) -> Result<String, LixError> {
    let options = serde_json::to_string(options).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("failed to serialize observe options for dedup key: {error}"),
    })?;
    Ok(format!(
        "{}\n--runtime:{}\n--observe-options:{options}",
        observe_source_key(query)?,
        session_runtime_namespace(session),
    ))
}

fn resolve_observe_options(session: &Session, options: &ObserveOptions) -> ResolvedObserveOptions {
    let mut include_origin_keys = BTreeSet::new();
    for origin_key in &options.include_origin_keys {
        let origin_key = origin_key.trim();
        if !origin_key.is_empty() {
            include_origin_keys.insert(origin_key.to_string());
        }
    }

    let mut exclude_origin_keys = BTreeSet::new();
    for origin_key in &options.exclude_origin_keys {
        let origin_key = origin_key.trim();
        if !origin_key.is_empty() {
            exclude_origin_keys.insert(origin_key.to_string());
        }
    }

    if options.exclude_self {
        let origin_key = session.origin_key().trim();
        if !origin_key.is_empty() {
            exclude_origin_keys.insert(origin_key.to_string());
        }
    }

    ResolvedObserveOptions {
        include_origin_keys: include_origin_keys.into_iter().collect(),
        exclude_origin_keys: exclude_origin_keys.into_iter().collect(),
    }
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
    session: &Session,
    source_key: &str,
    query: ObserveQuery,
    options: ResolvedObserveOptions,
) -> Result<Arc<Mutex<SharedObserveSource>>, LixError> {
    loop {
        if let Some(existing_source) = lock_observe_registry(session)?.get(source_key).cloned() {
            let is_closed = lock_shared_source(&existing_source)?.closed;
            if is_closed {
                let mut registry = lock_observe_registry(session)?;
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
            session,
            query.clone(),
            options.clone(),
        )?));
        let mut registry = lock_observe_registry(session)?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            registry.entry(source_key.to_string())
        {
            entry.insert(Arc::clone(&new_source));
            return Ok(new_source);
        }
    }
}

fn build_shared_observe_source(
    session: &Session,
    query: ObserveQuery,
    options: ResolvedObserveOptions,
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

    let dependency_spec = derive_dependency_spec(&statements, &query.params)?;
    let mut filter = dependency_spec_to_state_commit_stream_filter(&dependency_spec);
    filter
        .include_origin_keys
        .extend(options.include_origin_keys.iter().cloned());
    filter
        .exclude_origin_keys
        .extend(options.exclude_origin_keys.iter().cloned());
    let origin_filter = ObserveOriginFilter {
        include: filter.include_origin_keys.iter().cloned().collect(),
        exclude: filter.exclude_origin_keys.iter().cloned().collect(),
    };
    let state_commits = session.state_commit_stream(filter);

    Ok(SharedObserveSource::new(
        query,
        state_commits,
        origin_filter,
        dependency_spec.query_dependencies,
    ))
}

fn unregister_observe_subscriber(
    session: &Session,
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
        let mut registry = lock_observe_registry(session)?;
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
    session: &'a Session,
) -> Result<MutexGuard<'a, BTreeMap<String, Arc<Mutex<SharedObserveSource>>>>, LixError> {
    session
        .observe_shared_sources()
        .lock()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "observe shared source registry lock poisoned".to_string(),
        })
}

fn session_runtime_namespace(session: &Session) -> String {
    format!("session:{session:p}")
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

async fn latest_observe_tick_seq(backend: &dyn crate::LixBackend) -> Result<Option<i64>, LixError> {
    let result = Box::pin(backend.execute(
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
    backend: &dyn crate::LixBackend,
    last_seen_tick_seq: Option<i64>,
) -> Result<Vec<ObserveTickRow>, LixError> {
    let result = if let Some(last_seen) = last_seen_tick_seq {
        Box::pin(backend.execute(
            "SELECT tick_seq, origin_key \
             FROM lix_internal_observe_tick \
             WHERE tick_seq > $1 \
             ORDER BY tick_seq ASC",
            &[Value::Integer(last_seen)],
        ))
        .await?
    } else {
        Box::pin(backend.execute(
            "SELECT tick_seq, origin_key \
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

        let origin_key =
            parse_observe_tick_origin_key(
                row.get(1).ok_or(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description:
                        "failed to read observe tick origin key: row has no origin_key column"
                            .to_string(),
                })?,
            )?;

        ticks.push(ObserveTickRow {
            tick_seq,
            origin_key,
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

fn parse_observe_tick_origin_key(value: &Value) -> Result<Option<String>, LixError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick origin key value: {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_observe_state, observe_source_key, ObserveEvent, ObserveEvents, ObserveOptions,
        ObserveQuery, OBSERVE_TICK_POLL_INTERVAL,
    };
    use crate::wasm::NoopWasmRuntime;
    use crate::{
        ExecuteOptions, Lix, LixBackend, LixBackendTransaction, LixConfig, LixError, QueryResult,
        Session, SqlDialect, Value,
    };
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

    struct CountingObserveTransaction {
        observe_query_hits: Arc<AtomicUsize>,
    }

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
                    columns: vec!["tick_seq".to_string(), "origin_key".to_string()],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::backend::TransactionBeginMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(CountingObserveTransaction {
                observe_query_hits: Arc::clone(&self.observe_query_hits),
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::backend::TransactionBeginMode::Write)
                .await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for CountingObserveTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
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
                    columns: vec!["tick_seq".to_string(), "origin_key".to_string()],
                });
            }
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
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            )));
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let query = ObserveQuery::new("SELECT 'observe-shared-sentinel' AS marker", vec![]);
            let mut observed_a = session
                .observe(query.clone())
                .expect("observe should succeed");
            let mut observed_b = session.observe(query).expect("observe should succeed");

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
    fn observe_does_not_dedup_across_distinct_origin_filters() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            )));
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let query = ObserveQuery::new("SELECT 'observe-shared-sentinel' AS marker", vec![]);
            let mut observed_a = session
                .observe(query.clone())
                .expect("observe should succeed");
            let mut observed_b = session
                .observe_with_options(
                    query,
                    ObserveOptions {
                        exclude_origin_keys: vec!["worker-a".to_string()],
                        ..ObserveOptions::default()
                    },
                )
                .expect("observe should succeed");

            let _event_a = next_observe_event(&mut observed_a, "observe_a").await;
            let _event_b = next_observe_event(&mut observed_b, "observe_b").await;

            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                2,
                "observe subscribers with distinct origin filters should not share a dedup key"
            );
        });
    }

    #[test]
    fn observe_late_subscriber_reuses_cached_initial_snapshot() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            )));
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let query = ObserveQuery::new("SELECT 'observe-shared-sentinel' AS marker", vec![]);
            let mut observed_a = session
                .observe(query.clone())
                .expect("observe should succeed");

            let _initial_a = next_observe_event(&mut observed_a, "observe_a").await;

            let mut observed_b = session.observe(query).expect("observe should succeed");
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

            let mut observed_c = session
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

            let _ = session
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

    #[test]
    fn observe_does_not_reexecute_for_unrelated_session_dependency_changes() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            )));
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let mut state = build_observe_state(
                &session,
                ObserveQuery::new(
                    "SELECT 'observe-shared-sentinel' AS marker, lix_active_version_id() AS version_id",
                    vec![],
                ),
                ObserveOptions::default(),
            )
            .expect("observe state should build");

            state
                .poll_shared_source_once(&session)
                .await
                .expect("initial poll should succeed");
            assert_eq!(observe_query_hits.load(Ordering::SeqCst), 1);

            session.replace_active_account_ids(vec!["acct-a".to_string()]);

            state
                .poll_shared_source_once(&session)
                .await
                .expect("unrelated dependency poll should succeed");
            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                1,
                "active-account changes should not reexecute observes that only depend on active version",
            );

            state.close_with_session(&session);
        });
    }

    #[test]
    fn observe_reexecutes_when_public_surface_registry_generation_changes() {
        run_observe_test_with_large_stack(|| async move {
            let observe_query_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(CountingObserveBackend {
                    observe_query_hits: Arc::clone(&observe_query_hits),
                }),
                Arc::new(NoopWasmRuntime),
            )));
            let session = Session::new_for_test(
                lix.engine().session_host(),
                "version-test".to_string(),
                Vec::new(),
            );

            let mut state = build_observe_state(
                &session,
                ObserveQuery::new(
                    "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1",
                    vec![],
                ),
                ObserveOptions::default(),
            )
            .expect("observe state should build");

            state
                .poll_shared_source_once(&session)
                .await
                .expect("initial poll should succeed");
            assert_eq!(observe_query_hits.load(Ordering::SeqCst), 1);

            let mut context = session.new_compiler_state(ExecuteOptions::default());
            context.bump_public_surface_registry_generation();

            state
                .poll_shared_source_once(&session)
                .await
                .expect("registry invalidation poll should succeed");
            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                2,
                "public-surface observes should reexecute when the session registry generation changes",
            );

            state.close_with_session(&session);
        });
    }
}
