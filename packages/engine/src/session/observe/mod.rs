use crate::common::{unexpected_statement_count_error, WireValue};
use crate::execution::execute_prepared_read_batch_in_committed_read_transaction;
use crate::session::prepare_function_bindings_with_host;
use crate::session::Session;
use crate::sql::{
    dependency_spec_to_state_commit_stream_filter, derive_dependency_spec, parse_sql_statements,
    prepare_committed_read_batch_in_transaction, prepare_committed_read_batch_with_backend,
    QueryDependency, StatementBatch,
};
use crate::streams::{
    latest_durable_state_commit_cursor, latest_durable_state_commit_cursor_in_transaction,
    DurableStateCommitCursor, StateCommitStream,
};
use crate::{ExecuteOptions, LixError, QueryResult, Value, WriteReceipt};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::cmp::Ordering;
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

/// Delivery policy for reactive observe subscriptions.
///
/// These filters apply to delivery metadata carried on in-process
/// `state_commit_stream` batches. They do not inspect row-visible columns or
/// require query SQL to reference `origin_key`.
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
/// `frontier` is the durable snapshot frontier read from the same snapshot
/// that produced `rows`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub rows: QueryResult,
    pub frontier: Option<ObserveFrontier>,
}

/// Durable frontier attached to an observe snapshot.
///
/// This is the canonical snapshot marker for an observe result. It is not an
/// in-process delivery sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveFrontier {
    pub change_id: String,
    pub created_at: String,
    pub visibility_append_seq: i64,
}

impl ObserveFrontier {
    fn from_durable_cursor(cursor: DurableStateCommitCursor) -> Self {
        Self {
            change_id: cursor.change_id,
            created_at: cursor.created_at,
            visibility_append_seq: cursor.visibility_append_seq,
        }
    }

    fn from_write_receipt_floor(receipt: &WriteReceipt) -> Option<Self> {
        let latest_ref = receipt
            .canonical_commit
            .as_ref()?
            .updated_version_refs
            .iter()
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.change_id.cmp(&right.change_id))
            })?;
        Some(Self {
            change_id: latest_ref.change_id.clone(),
            created_at: latest_ref.created_at.clone(),
            visibility_append_seq: 0,
        })
    }

    fn is_at_or_after(&self, other: &Self) -> bool {
        !self.cmp(other).is_lt()
    }
}

impl Ord for ObserveFrontier {
    fn cmp(&self, other: &Self) -> Ordering {
        self.created_at
            .cmp(&other.created_at)
            .then_with(|| self.change_id.cmp(&other.change_id))
            .then_with(|| self.visibility_append_seq.cmp(&other.visibility_append_seq))
    }
}

impl PartialOrd for ObserveFrontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ResolvedObserveOptions {
    include_origin_keys: Vec<String>,
    exclude_origin_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct ObservePreparedQuery {
    params: Vec<Value>,
    statements: Vec<Statement>,
}

#[derive(Debug, Clone)]
struct ObserveSnapshotEvaluation {
    rows: QueryResult,
    frontier: Option<ObserveFrontier>,
}

#[derive(Debug, Clone, Default)]
struct ObserveCorrectnessState {
    last_rows: Option<QueryResult>,
    last_frontier: Option<ObserveFrontier>,
    dependency_generations: BTreeMap<QueryDependency, u64>,
}

#[derive(Debug, Clone)]
struct SharedObserveEvent {
    generation: u64,
    rows: QueryResult,
    frontier: Option<ObserveFrontier>,
}

#[derive(Debug, Clone, Default)]
struct SharedObserveSubscriberCursor {
    last_seen_generation: Option<u64>,
    initial_generation: Option<u64>,
}

pub(crate) struct SharedObserveSource {
    prepared_query: ObservePreparedQuery,
    state_commits: StateCommitStream,
    query_dependencies: BTreeSet<QueryDependency>,
    correctness: ObserveCorrectnessState,
    latest_event: Option<SharedObserveEvent>,
    events: VecDeque<SharedObserveEvent>,
    next_generation: u64,
    initialized: bool,
    closed: bool,
    polling: bool,
    next_subscriber_id: u64,
    subscribers: BTreeMap<u64, SharedObserveSubscriberCursor>,
}

struct PollWork {
    prepared_query: ObservePreparedQuery,
    should_reevaluate: bool,
    query_dependency_generations: BTreeMap<QueryDependency, u64>,
    mark_initialized: bool,
}

struct PollOutcome {
    maybe_snapshot: Option<ObserveSnapshotEvaluation>,
    update_dependency_generations: Option<BTreeMap<QueryDependency, u64>>,
    mark_initialized: bool,
}

impl SharedObserveSource {
    fn new(
        prepared_query: ObservePreparedQuery,
        state_commits: StateCommitStream,
        query_dependencies: BTreeSet<QueryDependency>,
    ) -> Self {
        Self {
            prepared_query,
            state_commits,
            query_dependencies,
            correctness: ObserveCorrectnessState::default(),
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
            self.correctness
                .dependency_generations
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

    fn append_event(&mut self, rows: QueryResult, frontier: Option<ObserveFrontier>) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        let event = SharedObserveEvent {
            generation,
            rows,
            frontier,
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
        let Some(target_frontier) = ObserveFrontier::from_write_receipt_floor(receipt) else {
            return Ok(None);
        };

        loop {
            let Some(event) = self.next().await? else {
                return Ok(None);
            };
            if event
                .frontier
                .as_ref()
                .is_some_and(|frontier| frontier.is_at_or_after(&target_frontier))
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
        let Some(target_frontier) = ObserveFrontier::from_write_receipt_floor(receipt) else {
            return Ok(None);
        };

        loop {
            let Some(event) = self.next().await? else {
                return Ok(None);
            };
            if event
                .frontier
                .as_ref()
                .is_some_and(|frontier| frontier.is_at_or_after(&target_frontier))
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
                PollWork {
                    prepared_query: source.prepared_query.clone(),
                    should_reevaluate: true,
                    query_dependency_generations,
                    mark_initialized: true,
                }
            } else if source.query_dependency_generations_changed(&query_dependency_generations) {
                PollWork {
                    prepared_query: source.prepared_query.clone(),
                    should_reevaluate: true,
                    query_dependency_generations,
                    mark_initialized: true,
                }
            } else if source.state_commits.try_next().is_some() {
                PollWork {
                    prepared_query: source.prepared_query.clone(),
                    should_reevaluate: true,
                    query_dependency_generations,
                    mark_initialized: true,
                }
            } else {
                let prepared_query = source.prepared_query.clone();
                let last_frontier = source.correctness.last_frontier.clone();
                drop(source);

                observe_poll_sleep(OBSERVE_TICK_POLL_INTERVAL).await;
                let latest_frontier =
                    latest_durable_state_commit_cursor(session.session_host().backend().as_ref())
                        .await?;
                let latest_frontier = latest_frontier.map(ObserveFrontier::from_durable_cursor);
                let should_reexecute = match (latest_frontier.as_ref(), last_frontier.as_ref()) {
                    (Some(latest), Some(last)) => latest.is_at_or_after(last) && latest != last,
                    (Some(_), None) => true,
                    _ => false,
                };

                PollWork {
                    prepared_query,
                    should_reevaluate: should_reexecute,
                    query_dependency_generations,
                    mark_initialized: true,
                }
            }
        };

        let maybe_snapshot = if work.should_reevaluate {
            Some(evaluate_observe_snapshot(session, &work.prepared_query).await?)
        } else {
            None
        };

        let outcome = if let Some(snapshot) = maybe_snapshot {
            PollOutcome {
                maybe_snapshot: Some(snapshot),
                update_dependency_generations: Some(work.query_dependency_generations),
                mark_initialized: work.mark_initialized,
            }
        } else {
            PollOutcome {
                maybe_snapshot: None,
                update_dependency_generations: Some(work.query_dependency_generations),
                mark_initialized: work.mark_initialized,
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
        if let Some(dependency_generations) = outcome.update_dependency_generations {
            source.correctness.dependency_generations = dependency_generations;
        }

        if let Some(snapshot) = outcome.maybe_snapshot {
            let ObserveSnapshotEvaluation { rows, frontier } = snapshot;
            let changed = source
                .correctness
                .last_rows
                .as_ref()
                .is_none_or(|previous| *previous != rows);
            source.correctness.last_frontier = frontier.clone();
            source.correctness.last_rows = Some(rows.clone());
            if changed {
                source.append_event(rows, frontier);
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
            frontier: shared_event.frontier,
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

impl ObservePreparedQuery {
    fn new(query: ObserveQuery) -> Result<Self, LixError> {
        let statements = parse_sql_statements(&query.sql)?;
        if statements.is_empty()
            || !statements
                .iter()
                .all(|statement| matches!(statement, Statement::Query(_)))
        {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "observe requires one or more SELECT statements".to_string(),
                hint: None,
            });
        }

        Ok(Self {
            params: query.params,
            statements,
        })
    }
}

async fn evaluate_observe_snapshot(
    session: &Session,
    prepared_query: &ObservePreparedQuery,
) -> Result<ObserveSnapshotEvaluation, LixError> {
    let mut context = session.new_compiler_state(ExecuteOptions::default());
    let runtime_bindings = context.runtime_binding_values()?;
    let statement_batch = StatementBatch::compile(
        prepared_query.statements.clone(),
        &prepared_query.params,
        session.session_host().backend().dialect(),
        &runtime_bindings,
        None,
    )?;
    let execution_mode = super::classify_session_execution_mode(&statement_batch, false);
    let function_bindings = prepare_function_bindings_with_host(
        session.session_host(),
        session.session_host().backend().as_ref(),
    )
    .await?;
    context.set_function_bindings(function_bindings.clone());
    let committed_read_context = super::committed_read_context(
        &context,
        session.session_host(),
        &function_bindings,
        execution_mode,
    );

    let transaction_mode = prepare_committed_read_batch_with_backend(
        session.session_host().backend().as_ref(),
        &statement_batch,
        &committed_read_context,
    )
    .await?
    .transaction_mode;

    let mut transaction = session
        .session_host()
        .begin_read_unit(transaction_mode)
        .await?;

    let prepared_read_batch = prepare_committed_read_batch_in_transaction(
        transaction.as_mut(),
        &statement_batch,
        &committed_read_context,
    )
    .await;
    let outcome = match prepared_read_batch {
        Ok(prepared_read_batch) => {
            let execute_result = execute_prepared_read_batch_in_committed_read_transaction(
                transaction.as_mut(),
                &session.execution_context(),
                &prepared_read_batch,
            )
            .await;
            match execute_result {
                Ok(result) => {
                    let rows = extract_single_observe_query_result(result)?;
                    let frontier =
                        latest_durable_state_commit_cursor_in_transaction(transaction.as_mut())
                            .await?
                            .map(ObserveFrontier::from_durable_cursor);
                    transaction.commit().await?;
                    Ok(ObserveSnapshotEvaluation { rows, frontier })
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    };
    context.clear_function_bindings();
    outcome
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
                hint: None,
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
        hint: None,
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
        hint: None,
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
    let prepared_query = ObservePreparedQuery::new(query)?;
    let dependency_spec =
        derive_dependency_spec(&prepared_query.statements, &prepared_query.params)?;
    let mut filter = dependency_spec_to_state_commit_stream_filter(&dependency_spec);
    filter
        .include_origin_keys
        .extend(options.include_origin_keys.iter().cloned());
    filter
        .exclude_origin_keys
        .extend(options.exclude_origin_keys.iter().cloned());
    let state_commits = session.session_host().state_commit_stream(filter.clone());

    Ok(SharedObserveSource::new(
        prepared_query,
        state_commits,
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
            hint: None,
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
        hint: None,
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

#[cfg(test)]
mod tests {
    use super::{
        build_observe_state, lock_shared_source, observe_source_key, ObserveEvent, ObserveEvents,
        ObserveFrontier, ObserveOptions, ObserveQuery, ObserveSnapshotEvaluation, PollOutcome,
        OBSERVE_TICK_POLL_INTERVAL,
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

    struct SnapshotObserveBackend {
        backend_query_hits: Arc<AtomicUsize>,
        backend_frontier_hits: Arc<AtomicUsize>,
        transaction_query_hits: Arc<AtomicUsize>,
        transaction_frontier_hits: Arc<AtomicUsize>,
    }

    struct SnapshotObserveTransaction {
        transaction_query_hits: Arc<AtomicUsize>,
        transaction_frontier_hits: Arc<AtomicUsize>,
    }

    fn observe_change_source_row() -> QueryResult {
        QueryResult {
            rows: vec![vec![
                Value::Text("change-observe-1".to_string()),
                Value::Text("entity-observe-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Text("2026-01-01T00:00:00Z".to_string()),
                Value::Boolean(false),
                Value::Null,
            ]],
            columns: vec![
                "id".to_string(),
                "entity_id".to_string(),
                "schema_key".to_string(),
                "schema_version".to_string(),
                "file_id".to_string(),
                "plugin_key".to_string(),
                "metadata".to_string(),
                "created_at".to_string(),
                "untracked".to_string(),
                "snapshot_content".to_string(),
            ],
        }
    }

    #[async_trait]
    impl LixBackend for CountingObserveBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change ch") {
                self.observe_query_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(observe_change_source_row());
            }
            if sql.contains("FROM lix_internal_change") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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

    #[async_trait]
    impl LixBackendTransaction for CountingObserveTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change ch") {
                self.observe_query_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(observe_change_source_row());
            }
            if sql.contains("FROM lix_internal_change") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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

    #[async_trait]
    impl LixBackend for SnapshotObserveBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change ch") {
                self.backend_query_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(observe_change_source_row());
            }
            if sql.contains("ORDER BY created_at DESC, id DESC") {
                self.backend_frontier_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("change-1".to_string()),
                        Value::Text("2026-01-01T00:00:00Z".to_string()),
                    ]],
                    columns: vec!["id".to_string(), "created_at".to_string()],
                });
            }
            if sql.contains("COALESCE(MAX(append_seq), 0)") {
                return Ok(QueryResult {
                    rows: vec![vec![Value::Integer(0)]],
                    columns: vec!["append_seq".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_change") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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
            Ok(Box::new(SnapshotObserveTransaction {
                transaction_query_hits: Arc::clone(&self.transaction_query_hits),
                transaction_frontier_hits: Arc::clone(&self.transaction_frontier_hits),
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

    #[async_trait]
    impl LixBackendTransaction for SnapshotObserveTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Read
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change ch") {
                self.transaction_query_hits.fetch_add(1, Ordering::SeqCst);
                return Ok(observe_change_source_row());
            }
            if sql.contains("ORDER BY created_at DESC, id DESC") {
                self.transaction_frontier_hits
                    .fetch_add(1, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("change-1".to_string()),
                        Value::Text("2026-01-01T00:00:00Z".to_string()),
                    ]],
                    columns: vec!["id".to_string(), "created_at".to_string()],
                });
            }
            if sql.contains("COALESCE(MAX(append_seq), 0)") {
                return Ok(QueryResult {
                    rows: vec![vec![Value::Integer(0)]],
                    columns: vec!["append_seq".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_change") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
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

            let query = ObserveQuery::new(
                "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1",
                vec![],
            );
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

            let query = ObserveQuery::new(
                "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1",
                vec![],
            );
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

            let query = ObserveQuery::new(
                "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1",
                vec![],
            );
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
                    "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1",
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
                .execute("SELECT id FROM lix_version ORDER BY id LIMIT 1", &[])
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
                    "SELECT 'observe-shared-sentinel' AS marker, lix_active_version_id() AS version_id FROM lix_change LIMIT 1",
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

    #[test]
    fn observe_session_invalidation_reexecutes_without_emitting_when_rows_do_not_change() {
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
            let initial_generation = {
                let source = lock_shared_source(&state.source).expect("lock shared source");
                source
                    .latest_event
                    .as_ref()
                    .expect("initial event should exist")
                    .generation
            };

            let mut context = session.new_compiler_state(ExecuteOptions::default());
            context.bump_public_surface_registry_generation();

            state
                .poll_shared_source_once(&session)
                .await
                .expect("registry invalidation poll should succeed");

            assert_eq!(
                observe_query_hits.load(Ordering::SeqCst),
                2,
                "session invalidation should reexecute the canonical snapshot path",
            );
            let source = lock_shared_source(&state.source).expect("lock shared source");
            assert_eq!(
                source.latest_event.as_ref().map(|event| event.generation),
                Some(initial_generation),
                "session invalidation should not synthesize a new event when rows are unchanged",
            );
        });
    }

    #[test]
    fn observe_evaluates_rows_and_frontier_inside_one_transaction_snapshot() {
        run_observe_test_with_large_stack(|| async move {
            let backend_query_hits = Arc::new(AtomicUsize::new(0));
            let backend_frontier_hits = Arc::new(AtomicUsize::new(0));
            let transaction_query_hits = Arc::new(AtomicUsize::new(0));
            let transaction_frontier_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(SnapshotObserveBackend {
                    backend_query_hits: Arc::clone(&backend_query_hits),
                    backend_frontier_hits: Arc::clone(&backend_frontier_hits),
                    transaction_query_hits: Arc::clone(&transaction_query_hits),
                    transaction_frontier_hits: Arc::clone(&transaction_frontier_hits),
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
                    "SELECT 'observe-snapshot-sentinel' AS marker FROM lix_change LIMIT 1",
                    vec![],
                ),
                ObserveOptions::default(),
            )
            .expect("observe state should build");

            state
                .poll_shared_source_once(&session)
                .await
                .expect("initial poll should succeed");

            assert_eq!(
                backend_query_hits.load(Ordering::SeqCst),
                0,
                "observe query should execute inside the read transaction snapshot",
            );
            assert_eq!(
                backend_frontier_hits.load(Ordering::SeqCst),
                0,
                "durable frontier should be read inside the same transaction snapshot",
            );
            assert_eq!(
                transaction_query_hits.load(Ordering::SeqCst),
                1,
                "observe query should execute exactly once inside the transaction",
            );
            assert_eq!(
                transaction_frontier_hits.load(Ordering::SeqCst),
                1,
                "frontier should be loaded exactly once inside the transaction",
            );

            state.close_with_session(&session);
        });
    }

    #[test]
    fn observe_updates_last_frontier_on_noop_snapshot_without_emitting() {
        run_observe_test_with_large_stack(|| async move {
            let backend_query_hits = Arc::new(AtomicUsize::new(0));
            let backend_frontier_hits = Arc::new(AtomicUsize::new(0));
            let transaction_query_hits = Arc::new(AtomicUsize::new(0));
            let transaction_frontier_hits = Arc::new(AtomicUsize::new(0));
            let lix = Arc::new(Lix::boot(LixConfig::new(
                Box::new(SnapshotObserveBackend {
                    backend_query_hits: Arc::clone(&backend_query_hits),
                    backend_frontier_hits: Arc::clone(&backend_frontier_hits),
                    transaction_query_hits: Arc::clone(&transaction_query_hits),
                    transaction_frontier_hits: Arc::clone(&transaction_frontier_hits),
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
                    "SELECT 'observe-snapshot-sentinel' AS marker FROM lix_change LIMIT 1",
                    vec![],
                ),
                ObserveOptions::default(),
            )
            .expect("observe state should build");

            state
                .poll_shared_source_once(&session)
                .await
                .expect("initial poll should succeed");

            let initial_generation = {
                let source = lock_shared_source(&state.source).expect("lock shared source");
                source
                    .latest_event
                    .as_ref()
                    .expect("initial event should exist")
                    .generation
            };

            let advanced_frontier = Some(ObserveFrontier {
                change_id: "change-2".to_string(),
                created_at: "2026-01-02T00:00:00Z".to_string(),
                visibility_append_seq: 0,
            });

            state
                .apply_poll_outcome(PollOutcome {
                    maybe_snapshot: Some(ObserveSnapshotEvaluation {
                        rows: QueryResult {
                            rows: vec![vec![Value::Text("observe-snapshot-sentinel".to_string())]],
                            columns: vec!["marker".to_string()],
                        },
                        frontier: advanced_frontier.clone(),
                    }),
                    update_dependency_generations: None,
                    mark_initialized: true,
                })
                .expect("noop snapshot outcome should apply");

            {
                let source = lock_shared_source(&state.source).expect("lock shared source");
                assert_eq!(source.correctness.last_frontier, advanced_frontier);
                assert_eq!(
                    source.latest_event.as_ref().map(|event| event.generation),
                    Some(initial_generation),
                    "noop snapshot should not append a new event",
                );
                assert_eq!(
                    source.events.len(),
                    1,
                    "noop snapshot should not queue a new event"
                );
            }

            state.close_with_session(&session);
        });
    }
}
