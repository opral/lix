//! Canonical HTTP transport for independent sessions on a workspace-mode
//! [`lix_sdk::Lix`] handle.

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, StatusCode, header::CACHE_CONTROL},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use lix_sdk::{
    CreateBranchOptions, ExecuteBatchStatement, ExecuteOptions, ExecuteResult, Lix, LixError,
    ObserveEvent, ObserveEvents, Storage, SwitchBranchOptions, Value, WireValue,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinHandle,
};
use tracing::{Instrument as _, instrument::WithSubscriber as _};

/// Stable URL prefix owned by the Lix server protocol.
pub const PROTOCOL_PATH: &str = "/lix/v1";
/// Current wire protocol version.
pub const PROTOCOL_VERSION: u32 = 1;
/// Header carrying the opaque server-issued session capability.
pub const SESSION_ID_HEADER: &str = "lix-session-id";
/// Default maximum number of live remote sessions for one workspace.
pub const DEFAULT_MAX_SESSIONS: usize = 64;
/// Default idle lifetime for a remote session.
pub const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_mins(30);
/// Default JSON request ceiling. Base64 expands blobs by roughly one third,
/// so 64 MiB carries the engine's 32 MiB maximum plugin archive with room for
/// the SQL envelope and also covers ordinary larger document blobs.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
/// Maximum number of queries multiplexed onto one observation stream.
pub const MAX_MULTIPLEX_SUBSCRIPTIONS: usize = 32;

const SESSION_TOKEN_BYTES: usize = 32;
const SESSION_TOKEN_HEX_LEN: usize = SESSION_TOKEN_BYTES * 2;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Resource limits for one workspace's remote protocol sessions.
#[derive(Clone, Copy, Debug)]
pub struct ProtocolServerOptions {
    pub max_sessions: usize,
    pub session_idle_timeout: Duration,
    pub max_request_body_bytes: usize,
}

impl Default for ProtocolServerOptions {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_SESSIONS,
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
        }
    }
}

/// Persistent canonical protocol server for one Lix workspace.
///
/// A server owns one root [`Lix`] and opens every remote client as an
/// independent workspace session on that root's existing engine. Clones share
/// the same bounded in-memory session registry.
#[expect(missing_debug_implementations)]
pub struct LixProtocolServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    inner: Arc<ServerInner<S>>,
}

impl<S> Clone for LixProtocolServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct ServerInner<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    root: Arc<Lix<S>>,
    options: ProtocolServerOptions,
    registry: AsyncMutex<SessionRegistry<S>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ServerLifecycle {
    Open,
    Closing,
    Closed,
}

struct SessionRegistry<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lifecycle: ServerLifecycle,
    sessions: HashMap<String, Arc<SessionRecord<S>>>,
}

struct SessionRecord<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix: Arc<Lix<S>>,
    last_used: Mutex<Instant>,
    leases: AtomicUsize,
}

impl<S> SessionRecord<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(lix: Lix<S>, now: Instant) -> Self {
        Self {
            lix: Arc::new(lix),
            last_used: Mutex::new(now),
            leases: AtomicUsize::new(0),
        }
    }

    fn acquire(&self, now: Instant) {
        self.leases.fetch_add(1, Ordering::AcqRel);
        *self
            .last_used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
    }

    fn release(&self, now: Instant) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
        let previous = self.leases.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "session lease count underflow");
    }

    fn lease_count(&self) -> usize {
        self.leases.load(Ordering::Acquire)
    }

    fn last_used(&self) -> Instant {
        *self
            .last_used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_idle_expired(&self, now: Instant, timeout: Duration) -> bool {
        self.lease_count() == 0 && now.saturating_duration_since(self.last_used()) >= timeout
    }
}

struct SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session_id: String,
    record: Arc<SessionRecord<S>>,
}

impl<S> SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(session_id: String, record: Arc<SessionRecord<S>>) -> Self {
        record.acquire(Instant::now());
        Self { session_id, record }
    }

    async fn run<T, F, Fut>(&self, operation: F) -> Result<T, LixError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Lix<S>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, LixError>> + 'static,
    {
        let runtime = Handle::try_current().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("access Lix server runtime: {error}"),
            )
        })?;
        let lix = Arc::clone(&self.record.lix);
        // `spawn_blocking` work is detached when its JoinHandle is dropped.
        // Keep a lease inside that work so an HTTP timeout/cancellation cannot
        // make an operation look idle and eligible for session eviction while
        // it is still running.
        let operation_lease = self.clone();
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        tokio::task::spawn_blocking(move || {
            let _operation_lease = operation_lease;
            tracing::dispatcher::with_default(&dispatch, || {
                parent.in_scope(|| runtime.block_on(operation(lix)))
            })
        })
        .await
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("join Lix server operation: {error}"),
            )
        })?
    }

    fn observe(&self, sql: &str, params: &[Value]) -> Result<ServerObserve<S>, LixError> {
        Ok(ServerObserve {
            events: Arc::new(Mutex::new(self.record.lix.observe(sql, params)?)),
        })
    }
}

impl<S> Clone for SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self::new(self.session_id.clone(), Arc::clone(&self.record))
    }
}

impl<S> Drop for SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.record.release(Instant::now());
    }
}

#[derive(Clone)]
struct HandlerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    server: LixProtocolServer<S>,
}

impl<S> HandlerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn lease(&self, session_id: &str) -> Result<SessionLease<S>, ApiError> {
        self.server.lease(session_id).await
    }
}

struct ServerObserve<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    events: Arc<Mutex<ObserveEvents<S>>>,
}

impl<S> ServerObserve<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn next(&self) -> Result<Option<ObserveEvent>, LixError> {
        let runtime = Handle::try_current().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("access Lix observe runtime: {error}"),
            )
        })?;
        let events = Arc::clone(&self.events);
        let (cancel_on_drop, cancel) = tokio::sync::oneshot::channel::<()>();
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let result = tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                parent.in_scope(|| {
                    let mut events = events.lock().map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("lock Lix observe stream: {error}"),
                        )
                    })?;
                    runtime.block_on(async {
                        tokio::select! {
                            result = events.next() => result,
                            _ = cancel => Err(LixError::new(
                                LixError::CODE_CLOSED,
                                "Lix observe wait was cancelled",
                            )),
                        }
                    })
                })
            })
        })
        .await
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("join Lix observe operation: {error}"),
            )
        })?;
        drop(cancel_on_drop);
        result
    }
}

impl<S> LixProtocolServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a protocol server with the default session limits.
    pub fn new(root: Arc<Lix<S>>) -> Self {
        Self::with_options(root, ProtocolServerOptions::default())
            .expect("default protocol server options must be valid")
    }

    /// Creates a protocol server with explicit per-workspace session limits.
    pub fn with_options(
        root: Arc<Lix<S>>,
        options: ProtocolServerOptions,
    ) -> Result<Self, LixError> {
        if options.max_sessions == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "protocol max_sessions must be greater than zero",
            ));
        }
        if options.max_request_body_bytes == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "protocol max_request_body_bytes must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(ServerInner {
                root,
                options,
                registry: AsyncMutex::new(SessionRegistry {
                    lifecycle: ServerLifecycle::Open,
                    sessions: HashMap::new(),
                }),
            }),
        })
    }

    /// Builds the Axum router backed by this persistent server registry.
    pub fn router(&self) -> Router {
        let state = HandlerState {
            server: self.clone(),
        };
        let protected = Router::new()
            .route("/lix/v1/execute", post(execute::<S>))
            .route("/lix/v1/execute-batch", post(execute_batch::<S>))
            .route("/lix/v1/branch/create", post(create_branch::<S>))
            .route("/lix/v1/branch/switch", post(switch_branch::<S>))
            .route("/lix/v1/observe", post(observe::<S>))
            .route("/lix/v1/observe/multiplex", post(observe_multiplex::<S>))
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                require_session::<S>,
            ));
        Router::new()
            .route("/lix/v1", get(handshake::<S>))
            .route("/lix/v1/", get(handshake::<S>))
            .route("/lix/v1/session", delete(delete_session::<S>))
            .merge(protected)
            .layer(DefaultBodyLimit::max(
                self.inner.options.max_request_body_bytes,
            ))
            .with_state(state)
    }

    /// Closes every child session and finally the root workspace session.
    /// Repeated calls are safe.
    pub async fn close(&self) -> Result<(), LixError> {
        let mut registry = self.inner.registry.lock().await;
        if registry.lifecycle == ServerLifecycle::Closed {
            return Ok(());
        }
        registry.lifecycle = ServerLifecycle::Closing;
        let sessions = registry
            .sessions
            .drain()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for record in sessions {
            if let Err(error) = record.lix.close().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Err(error) = self.inner.root.close().await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        registry.lifecycle = ServerLifecycle::Closed;
        first_error.map_or(Ok(()), Err)
    }

    async fn create_session(&self) -> Result<SessionLease<S>, ApiError> {
        let mut registry = self.inner.registry.lock().await;
        ensure_server_open(registry.lifecycle)?;

        let now = Instant::now();
        let expired_ids = registry
            .sessions
            .iter()
            .filter(|(_, record)| {
                record.is_idle_expired(now, self.inner.options.session_idle_timeout)
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in expired_ids {
            if let Some(record) = registry.sessions.remove(&session_id) {
                close_removed_session(record).await;
            }
        }

        if registry.sessions.len() >= self.inner.options.max_sessions {
            let lru_idle_id = registry
                .sessions
                .iter()
                .filter(|(_, record)| record.lease_count() == 0)
                .min_by_key(|(_, record)| record.last_used())
                .map(|(session_id, _)| session_id.clone());
            let Some(lru_idle_id) = lru_idle_id else {
                return Err(ApiError::capacity());
            };
            if let Some(record) = registry.sessions.remove(&lru_idle_id) {
                close_removed_session(record).await;
            }
        }

        let child = self.inner.root.open_workspace_session().await?;
        let session_id = loop {
            let candidate = generate_session_id()?;
            if !registry.sessions.contains_key(&candidate) {
                break candidate;
            }
        };
        let record = Arc::new(SessionRecord::new(child, now));
        registry
            .sessions
            .insert(session_id.clone(), Arc::clone(&record));
        Ok(SessionLease::new(session_id, record))
    }

    async fn lease(&self, session_id: &str) -> Result<SessionLease<S>, ApiError> {
        let mut registry = self.inner.registry.lock().await;
        ensure_server_open(registry.lifecycle)?;
        let Some(record) = registry.sessions.get(session_id).cloned() else {
            return Err(ApiError::session_gone());
        };
        if record.is_idle_expired(Instant::now(), self.inner.options.session_idle_timeout) {
            let removed = registry.sessions.remove(session_id);
            drop(registry);
            if let Some(removed) = removed {
                close_removed_session(removed).await;
            }
            return Err(ApiError::session_gone());
        }
        Ok(SessionLease::new(session_id.to_string(), record))
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), ApiError> {
        let mut registry = self.inner.registry.lock().await;
        ensure_server_open(registry.lifecycle)?;
        let record = registry.sessions.remove(session_id);
        drop(registry);
        if let Some(record) = record {
            record.lix.close().await?;
        }
        Ok(())
    }
}

async fn close_removed_session<S>(record: Arc<SessionRecord<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if let Err(error) = record.lix.close().await {
        tracing::warn!(
            code = %error.code,
            message = %error.message,
            "failed to close an evicted Lix protocol session"
        );
    }
}

fn generate_session_id() -> Result<String, ApiError> {
    let mut bytes = [0_u8; SESSION_TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|error| {
        ApiError::from(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("generate Lix protocol session identifier: {error}"),
        ))
    })?;
    let mut encoded = String::with_capacity(SESSION_TOKEN_HEX_LEN);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn ensure_server_open(lifecycle: ServerLifecycle) -> Result<(), ApiError> {
    if lifecycle == ServerLifecycle::Open {
        Ok(())
    } else {
        Err(ApiError::server_closed())
    }
}

fn optional_session_id(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let mut values = headers.get_all(SESSION_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::invalid_session_id(
            "Lix-Session-Id must be sent exactly once",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| ApiError::invalid_session_id("Lix-Session-Id must contain visible ASCII"))?;
    if value.len() != SESSION_TOKEN_HEX_LEN
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApiError::invalid_session_id(
            "Lix-Session-Id has an invalid format",
        ));
    }
    Ok(Some(value.to_string()))
}

fn required_session_id(headers: &HeaderMap) -> Result<String, ApiError> {
    optional_session_id(headers)?.ok_or_else(ApiError::session_required)
}

async fn require_session<S>(
    State(state): State<HandlerState<S>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session_id = required_session_id(request.headers())?;
    let lease = state.lease(&session_id).await?;
    request.extensions_mut().insert(lease);
    Ok(next.run(request).await)
}

/// Returns an Axum handler for an existing persistent protocol server.
///
/// The returned router is already mounted at [`PROTOCOL_PATH`]. Hosts should
/// merge it into their application and keep auth, workspace resolution, and
/// storage lifecycle outside this package.
pub fn handler<S>(server: LixProtocolServer<S>) -> Router
where
    S: Storage + Clone + Send + Sync + 'static,
{
    server.router()
}

async fn handshake<S>(
    State(state): State<HandlerState<S>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lease = match optional_session_id(&headers)? {
        Some(session_id) => state.lease(&session_id).await?,
        None => state.server.create_session().await?,
    };
    let active_branch_id = lease
        .run(|lix| async move { lix.active_branch_id().await })
        .await?;
    Ok((
        [(CACHE_CONTROL, "no-store")],
        Json(HandshakeResponse {
            protocol_version: PROTOCOL_VERSION,
            active_branch_id,
            session_id: lease.session_id.clone(),
        }),
    ))
}

async fn delete_session<S>(
    State(state): State<HandlerState<S>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session_id = required_session_id(&headers)?;
    state.server.delete_session(&session_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn execute<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let params = decode_params(request.params)?;
    let options = request.options.into();
    let result = lease
        .run(move |lix| async move { lix.execute_with_options(&sql, &params, options).await })
        .await?;
    Ok(Json(ExecuteResponse::try_from(result)?))
}

async fn execute_batch<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<ExecuteBatchRequest>,
) -> Result<Json<Vec<ExecuteResponse>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.statements.is_empty() {
        return Err(ApiError::bad_request("statements must not be empty"));
    }
    let statements = request
        .statements
        .into_iter()
        .enumerate()
        .map(|(index, statement)| {
            Ok(ExecuteBatchStatement {
                sql: required_non_empty(statement.sql, "statements[].sql")?,
                params: decode_params_at(statement.params, Some(index))?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let options = request.options.into();
    let results = lease
        .run(move |lix| async move { lix.execute_batch_with_options(&statements, options).await })
        .await?;
    Ok(Json(
        results
            .into_iter()
            .map(ExecuteResponse::try_from)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

async fn create_branch<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<CreateBranchRequest>,
) -> Result<Json<CreateBranchResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let options = CreateBranchOptions {
        id: request.id,
        name: required_non_empty(request.name, "name")?,
        from_commit_id: request.from_commit_id,
    };
    let receipt = lease
        .run(move |lix| async move { lix.create_branch(options).await })
        .await?;
    Ok(Json(CreateBranchResponse {
        id: receipt.id,
        name: receipt.name,
        hidden: receipt.hidden,
        commit_id: receipt.commit_id,
    }))
}

async fn switch_branch<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<SwitchBranchRequest>,
) -> Result<Json<SwitchBranchResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let options = SwitchBranchOptions {
        branch_id: required_non_empty(request.branch_id, "branchId")?,
    };
    let receipt = lease
        .run(move |lix| async move { lix.switch_branch(options).await })
        .await?;
    Ok(Json(SwitchBranchResponse {
        branch_id: receipt.branch_id,
    }))
}

async fn observe<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<ObserveRequest>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let params = decode_params(request.params)?;
    let events = lease.observe(&sql, &params)?;
    let stream = async_stream::stream! {
        let _lease = lease;
        loop {
            match events.next().await {
                Ok(Some(event)) => match ObserveEventResponse::try_from(event) {
                    Ok(payload) => yield Ok(sse_json_event("next", &payload)),
                    Err(error) => {
                        yield Ok(sse_json_event("error", &ErrorEnvelope::from_lix_error(&error)));
                        break;
                    }
                },
                Ok(None) => break,
                Err(error) => {
                    yield Ok(sse_json_event("error", &ErrorEnvelope::from_lix_error(&error)));
                    break;
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn observe_multiplex<S>(
    Extension(lease): Extension<SessionLease<S>>,
    Json(request): Json<MultiplexObserveRequest>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.subscriptions.is_empty() {
        return Err(ApiError::bad_request("subscriptions must not be empty"));
    }
    if request.subscriptions.len() > MAX_MULTIPLEX_SUBSCRIPTIONS {
        return Err(ApiError::bad_request(format!(
            "subscriptions must contain at most {MAX_MULTIPLEX_SUBSCRIPTIONS} entries"
        )));
    }
    let (sender, mut receiver) = mpsc::channel::<MultiplexObserveMessage>(64);
    let mut tasks = Vec::with_capacity(request.subscriptions.len());
    for subscription in request.subscriptions {
        let subscription_id = required_non_empty(subscription.id, "subscriptions[].id")?;
        let sql = required_non_empty(subscription.sql, "subscriptions[].sql")?;
        let params = decode_params(subscription.params)?;
        let events = lease.observe(&sql, &params)?;
        let sender = sender.clone();
        let parent = tracing::Span::current();
        tasks.push(tokio::spawn(
            async move {
                loop {
                    let message = match events.next().await {
                        Ok(Some(event)) => match ObserveEventResponse::try_from(event) {
                            Ok(payload) => MultiplexObserveMessage::Next {
                                subscription_id: subscription_id.clone(),
                                payload,
                            },
                            Err(error) => MultiplexObserveMessage::Error {
                                subscription_id: subscription_id.clone(),
                                error: ErrorEnvelope::from_lix_error(&error),
                            },
                        },
                        Ok(None) => break,
                        Err(error) => MultiplexObserveMessage::Error {
                            subscription_id: subscription_id.clone(),
                            error: ErrorEnvelope::from_lix_error(&error),
                        },
                    };
                    let terminal = matches!(message, MultiplexObserveMessage::Error { .. });
                    if sender.send(message).await.is_err() || terminal {
                        break;
                    }
                }
            }
            .instrument(parent)
            .with_current_subscriber(),
        ));
    }
    drop(sender);
    let stream = async_stream::stream! {
        let _lease = lease;
        let _task_guard = ObserveTaskGuard(tasks);
        while let Some(message) = receiver.recv().await {
            match message {
                MultiplexObserveMessage::Next { subscription_id, payload } => {
                    yield Ok(sse_json_event("next", &MultiplexObserveEventResponse {
                        subscription_id,
                        sequence: payload.sequence,
                        mutation_sequence: payload.mutation_sequence,
                        result: payload.result,
                    }));
                }
                MultiplexObserveMessage::Error { subscription_id, error } => {
                    yield Ok(sse_json_event("error", &MultiplexObserveErrorResponse {
                        subscription_id,
                        error,
                    }));
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn sse_json_event<T: Serialize>(event: &'static str, payload: &T) -> Event {
    match serde_json::to_string(payload) {
        Ok(data) => Event::default().event(event).data(data),
        Err(error) => Event::default().event("error").data(
            serde_json::to_string(&ErrorEnvelope::from_lix_error(&LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to serialize SSE event: {error}"),
            )))
            .unwrap_or_else(|_| {
                "{\"error\":{\"code\":\"LIX_INTERNAL_ERROR\",\"message\":\"internal server error\"}}"
                    .to_string()
            }),
        ),
    }
}

fn decode_params(params: Vec<WireValue>) -> Result<Vec<Value>, ApiError> {
    decode_params_at(params, None)
}

fn decode_params_at(
    params: Vec<WireValue>,
    statement_index: Option<usize>,
) -> Result<Vec<Value>, ApiError> {
    params
        .into_iter()
        .enumerate()
        .map(|(parameter_index, value)| {
            value.try_into_engine().map_err(|error| {
                let mut details = serde_json::json!({
                    "parameterIndex": parameter_index,
                    "sourceCode": error.code,
                });
                if let Some(statement_index) = statement_index {
                    details["statementIndex"] = statement_index.into();
                }
                ApiError::from(
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "invalid SQL parameter at index {parameter_index}: {}",
                            error.message
                        ),
                    )
                    .with_details(details),
                )
            })
        })
        .collect()
}

fn required_non_empty(value: Option<String>, field: &'static str) -> Result<String, ApiError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(ApiError::bad_request(format!("{field} is required"))),
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ErrorEnvelope,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorEnvelope::from_parts("LIX_INVALID_ARGUMENT", message, None, None),
        }
    }

    fn session_required() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_SESSION_REQUIRED",
                "Lix-Session-Id is required; initialize the client with GET /lix/v1",
                None,
                None,
            ),
        }
    }

    fn invalid_session_id(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_SESSION_INVALID",
                message,
                None,
                None,
            ),
        }
    }

    fn session_gone() -> Self {
        Self {
            status: StatusCode::GONE,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_SESSION_GONE",
                "the Lix protocol session is unknown, expired, or closed; open a new client session",
                None,
                None,
            ),
        }
    }

    fn capacity() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_SESSION_CAPACITY",
                "all Lix protocol session slots are currently active",
                Some("retry after an active request or observation stream closes".to_string()),
                None,
            ),
        }
    }

    fn server_closed() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_SERVER_CLOSED",
                "the Lix protocol server is closing or closed",
                None,
                None,
            ),
        }
    }
}

impl From<LixError> for ApiError {
    fn from(error: LixError) -> Self {
        Self {
            status: status_for_lix_error(&error),
            body: ErrorEnvelope::from_lix_error(&error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl ErrorEnvelope {
    fn from_lix_error(error: &LixError) -> Self {
        Self::from_parts(
            error.code.clone(),
            error.message.clone(),
            error.hint.clone(),
            error.details.clone(),
        )
    }

    fn from_parts(
        code: impl Into<String>,
        message: impl Into<String>,
        hint: Option<String>,
        details: Option<serde_json::Value>,
    ) -> Self {
        Self {
            error: ErrorBody {
                code: code.into(),
                message: message.into(),
                hint,
                details,
            },
        }
    }
}

fn status_for_lix_error(error: &LixError) -> StatusCode {
    match error.code.as_str() {
        LixError::CODE_BRANCH_NOT_FOUND
        | LixError::CODE_COMMIT_NOT_FOUND
        | LixError::CODE_TABLE_NOT_FOUND
        | LixError::CODE_COLUMN_NOT_FOUND => StatusCode::NOT_FOUND,
        LixError::CODE_CLOSED => StatusCode::CONFLICT,
        LixError::CODE_INTERNAL_ERROR => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    protocol_version: u32,
    active_branch_id: String,
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteRequest {
    sql: Option<String>,
    #[serde(default)]
    params: Vec<WireValue>,
    #[serde(default)]
    options: ExecuteOptionsRequest,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteOptionsRequest {
    origin_key: Option<String>,
}

impl From<ExecuteOptionsRequest> for ExecuteOptions {
    fn from(value: ExecuteOptionsRequest) -> Self {
        Self {
            origin_key: value.origin_key,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteBatchRequest {
    #[serde(default)]
    statements: Vec<ExecuteBatchStatementRequest>,
    #[serde(default)]
    options: ExecuteOptionsRequest,
}

#[derive(Debug, Deserialize)]
struct ExecuteBatchStatementRequest {
    sql: Option<String>,
    #[serde(default)]
    params: Vec<WireValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteResponse {
    columns: Vec<String>,
    rows: Vec<Vec<WireValue>>,
    rows_affected: u64,
    notices: Vec<lix_sdk::LixNotice>,
}

impl TryFrom<ExecuteResult> for ExecuteResponse {
    type Error = LixError;

    fn try_from(result: ExecuteResult) -> Result<Self, Self::Error> {
        let rows = result
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(WireValue::try_from_engine)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            columns: result.columns().to_vec(),
            rows,
            rows_affected: result.rows_affected(),
            notices: result.notices().to_vec(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchRequest {
    id: Option<String>,
    name: Option<String>,
    from_commit_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchResponse {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchRequest {
    branch_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchResponse {
    branch_id: String,
}

#[derive(Debug, Deserialize)]
struct ObserveRequest {
    sql: Option<String>,
    #[serde(default)]
    params: Vec<WireValue>,
}

#[derive(Debug, Deserialize)]
struct MultiplexObserveRequest {
    #[serde(default)]
    subscriptions: Vec<MultiplexObserveSubscription>,
}

#[derive(Debug, Deserialize)]
struct MultiplexObserveSubscription {
    id: Option<String>,
    sql: Option<String>,
    #[serde(default)]
    params: Vec<WireValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ObserveEventResponse {
    sequence: u64,
    mutation_sequence: u64,
    result: ExecuteResponse,
}

impl TryFrom<ObserveEvent> for ObserveEventResponse {
    type Error = LixError;

    fn try_from(event: ObserveEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: event.sequence,
            mutation_sequence: event.mutation_sequence,
            result: ExecuteResponse::try_from(event.rows)?,
        })
    }
}

enum MultiplexObserveMessage {
    Next {
        subscription_id: String,
        payload: ObserveEventResponse,
    },
    Error {
        subscription_id: String,
        error: ErrorEnvelope,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObserveEventResponse {
    subscription_id: String,
    sequence: u64,
    mutation_sequence: u64,
    result: ExecuteResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObserveErrorResponse {
    subscription_id: String,
    #[serde(flatten)]
    error: ErrorEnvelope,
}

struct ObserveTaskGuard(Vec<JoinHandle<()>>);

impl Drop for ObserveTaskGuard {
    fn drop(&mut self) {
        for task in self.0.drain(..) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt as _;
    use lix_sdk::{
        Memory, OpenLixOptions, TracingTelemetrySink, open_lix, open_lix_with_telemetry,
    };
    use serde_json::{Value as JsonValue, json};
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt as _;
    use tracing::Subscriber;
    use tracing_subscriber::{
        layer::{Context as LayerContext, Layer},
        prelude::*,
        registry::LookupSpan,
    };

    #[derive(Clone, Debug)]
    struct CapturedSpan {
        parent: Option<tracing::span::Id>,
        name: &'static str,
    }

    #[derive(Clone, Default)]
    struct CaptureLayer {
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            context: LayerContext<'_, S>,
        ) {
            let parent = attributes.parent().cloned().or_else(|| {
                attributes
                    .is_contextual()
                    .then(|| context.current_span().id().cloned())
                    .flatten()
            });
            self.spans
                .lock()
                .expect("capture spans")
                .push(CapturedSpan {
                    parent,
                    name: attributes.metadata().name(),
                });
        }
    }

    struct TestApp {
        server: LixProtocolServer<Memory>,
        router: Router,
    }

    async fn app() -> TestApp {
        app_with_options(ProtocolServerOptions::default()).await
    }

    async fn app_with_options(options: ProtocolServerOptions) -> TestApp {
        let lix = Arc::new(
            open_lix(OpenLixOptions::<Memory>::default())
                .await
                .expect("open lix"),
        );
        let server = LixProtocolServer::with_options(lix, options).expect("protocol server");
        let router = handler(server.clone());
        TestApp { server, router }
    }

    async fn app_with_tracing_telemetry() -> TestApp {
        let lix = Arc::new(
            open_lix_with_telemetry(
                OpenLixOptions::<Memory>::default(),
                Arc::new(TracingTelemetrySink::new()),
            )
            .await
            .expect("open lix"),
        );
        let server = LixProtocolServer::new(lix);
        let router = handler(server.clone());
        TestApp { server, router }
    }

    async fn request(
        app: &Router,
        method: &str,
        uri: &str,
        session_id: Option<&str>,
        body: Option<JsonValue>,
    ) -> Response {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(session_id) = session_id {
            builder = builder.header(SESSION_ID_HEADER, session_id);
        }
        let body = body.map_or_else(Body::empty, |body| {
            builder
                .headers_mut()
                .expect("request builder headers")
                .insert(
                    axum::http::header::CONTENT_TYPE,
                    axum::http::HeaderValue::from_static("application/json"),
                );
            Body::from(body.to_string())
        });
        app.clone()
            .oneshot(builder.body(body).expect("request"))
            .await
            .expect("response")
    }

    async fn new_session(app: &Router) -> (String, JsonValue) {
        let response = request(app, "GET", "/lix/v1", None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&axum::http::HeaderValue::from_static("no-store"))
        );
        let body = response_json(response).await;
        let session_id = body["sessionId"].as_str().expect("session id").to_string();
        (session_id, body)
    }

    async fn response_json(response: Response) -> JsonValue {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json")
    }

    async fn error_code(response: Response) -> String {
        response_json(response).await["error"]["code"]
            .as_str()
            .expect("error code")
            .to_string()
    }

    #[tokio::test]
    async fn handshake_issues_and_resumes_a_256_bit_server_session() {
        let app = app().await;
        let (session_id, first) = new_session(&app.router).await;
        assert_eq!(first["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(session_id.len(), SESSION_TOKEN_HEX_LEN);
        assert!(
            session_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );

        let resumed = request(&app.router, "GET", "/lix/v1/", Some(&session_id), None).await;
        assert_eq!(resumed.status(), StatusCode::OK);
        let resumed = response_json(resumed).await;
        assert_eq!(resumed["sessionId"], session_id);
        assert_eq!(resumed["activeBranchId"], first["activeBranchId"]);
    }

    #[tokio::test]
    async fn protected_routes_require_a_well_formed_live_session() {
        let app = app().await;
        let missing = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            None,
            Some(json!({ "sql": "SELECT 1" })),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(missing).await,
            "LIX_ERROR_PROTOCOL_SESSION_REQUIRED"
        );

        let malformed = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some("not-a-session"),
            Some(json!({ "sql": "SELECT 1" })),
        )
        .await;
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(malformed).await,
            "LIX_ERROR_PROTOCOL_SESSION_INVALID"
        );

        let unknown = "0".repeat(SESSION_TOKEN_HEX_LEN);
        let gone = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&unknown),
            Some(json!({ "sql": "SELECT 1" })),
        )
        .await;
        assert_eq!(gone.status(), StatusCode::GONE);
        assert_eq!(error_code(gone).await, "LIX_ERROR_PROTOCOL_SESSION_GONE");
    }

    #[tokio::test]
    async fn separate_protocol_sessions_are_routed_and_closed_independently() {
        let app = app().await;
        let (first, _) = new_session(&app.router).await;
        let (second, _) = new_session(&app.router).await;
        assert_ne!(first, second);

        for session_id in [&first, &second] {
            let response = request(
                &app.router,
                "POST",
                "/lix/v1/execute",
                Some(session_id),
                Some(json!({ "sql": "SELECT 1" })),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        let deleted = request(&app.router, "DELETE", "/lix/v1/session", Some(&first), None).await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        let deleted_again =
            request(&app.router, "DELETE", "/lix/v1/session", Some(&first), None).await;
        assert_eq!(deleted_again.status(), StatusCode::NO_CONTENT);

        let first_gone = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&first),
            Some(json!({ "sql": "SELECT 1" })),
        )
        .await;
        assert_eq!(first_gone.status(), StatusCode::GONE);
        let second_alive = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&second),
            Some(json!({ "sql": "SELECT 2" })),
        )
        .await;
        assert_eq!(second_alive.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn handshake_and_switch_share_the_workspace_selector() {
        let app = app().await;
        let (first_session, before) = new_session(&app.router).await;
        let (second_session, _) = new_session(&app.router).await;
        let active = before["activeBranchId"]
            .as_str()
            .expect("active branch")
            .to_string();
        let created = request(
            &app.router,
            "POST",
            "/lix/v1/branch/create",
            Some(&first_session),
            Some(json!({ "name": "Draft" })),
        )
        .await;
        let draft = response_json(created).await["id"]
            .as_str()
            .expect("draft id")
            .to_string();
        assert_ne!(active, draft);
        let switched = request(
            &app.router,
            "POST",
            "/lix/v1/branch/switch",
            Some(&first_session),
            Some(json!({ "branchId": draft })),
        )
        .await;
        assert_eq!(switched.status(), StatusCode::OK);
        let after = request(&app.router, "GET", "/lix/v1/", Some(&second_session), None).await;
        assert_eq!(response_json(after).await["activeBranchId"], draft);
    }

    #[tokio::test]
    async fn execute_batch_is_atomic_and_returns_each_result() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    { "sql": "SELECT 1 AS value", "params": [] },
                    { "sql": "SELECT 2 AS value", "params": [] }
                ]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body.as_array().map(Vec::len), Some(2));
        assert_eq!(body[0]["rows"][0][0], json!({ "kind": "int", "value": 1 }));
        assert_eq!(body[1]["rows"][0][0], json!({ "kind": "int", "value": 2 }));
    }

    #[tokio::test]
    async fn default_body_limit_accepts_blobs_larger_than_axums_two_megabyte_default() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let blob = WireValue::try_from_engine(&Value::Blob(vec![0x41; 2 * 1024 * 1024]))
            .expect("large blob should encode");
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                "params": [
                    { "kind": "text", "value": "/large.bin" },
                    blob,
                ]
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn configured_body_limit_rejects_oversized_json() {
        let app = app_with_options(ProtocolServerOptions {
            max_request_body_bytes: 1_024,
            ..ProtocolServerOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [{ "kind": "text", "value": "x".repeat(2_048) }]
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn multiplex_observe_rejects_unbounded_subscription_fanout() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let subscriptions = (0..=MAX_MULTIPLEX_SUBSCRIPTIONS)
            .map(|index| json!({ "id": format!("observe-{index}"), "sql": "SELECT 1" }))
            .collect::<Vec<_>>();
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/observe/multiplex",
            Some(&session_id),
            Some(json!({ "subscriptions": subscriptions })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn execute_keeps_sql_span_under_protocol_request_across_blocking_runtime() {
        let capture = CaptureLayer::default();
        let spans = Arc::clone(&capture.spans);
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture));
        let protocol_span = tracing::info_span!("lix.protocol.request");
        let protocol_span_id = protocol_span.id().expect("protocol span id");

        let app = app_with_tracing_telemetry().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .instrument(protocol_span)
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let spans = spans.lock().expect("capture spans");
        let sql_span = spans
            .iter()
            .find(|span| span.name == "lix.sql.query")
            .expect("SQL span");
        assert_eq!(sql_span.parent.as_ref(), Some(&protocol_span_id));
    }

    #[tokio::test]
    async fn idle_timeout_expires_a_session_with_gone() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 4,
            session_idle_timeout: Duration::ZERO,
            ..ProtocolServerOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let expired = request(&app.router, "GET", "/lix/v1", Some(&session_id), None).await;
        assert_eq!(expired.status(), StatusCode::GONE);
        assert_eq!(error_code(expired).await, "LIX_ERROR_PROTOCOL_SESSION_GONE");
    }

    #[tokio::test]
    async fn capacity_evicts_the_least_recently_used_idle_session() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ProtocolServerOptions::default()
        })
        .await;
        let (first, _) = new_session(&app.router).await;
        let (second, _) = new_session(&app.router).await;

        let first_response = request(&app.router, "GET", "/lix/v1", Some(&first), None).await;
        assert_eq!(first_response.status(), StatusCode::GONE);
        let second_response = request(&app.router, "GET", "/lix/v1", Some(&second), None).await;
        assert_eq!(second_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn active_sse_lease_cannot_be_evicted_for_capacity() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ProtocolServerOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let observe_response = request(
            &app.router,
            "POST",
            "/lix/v1/observe",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .await;
        assert_eq!(observe_response.status(), StatusCode::OK);

        let at_capacity = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(at_capacity.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            error_code(at_capacity).await,
            "LIX_ERROR_PROTOCOL_SESSION_CAPACITY"
        );

        drop(observe_response);
        let replacement = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(replacement.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cancelled_http_future_keeps_its_detached_operation_leased() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ProtocolServerOptions::default()
        })
        .await;
        let lease = app.server.create_session().await.expect("session lease");
        let record = Arc::clone(&lease.record);
        let (started_sender, started) = tokio::sync::oneshot::channel();
        let (finish_sender, finish) = tokio::sync::oneshot::channel();
        let (done_sender, done) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(async move {
            lease
                .run(move |_lix| async move {
                    started_sender.send(()).expect("signal start");
                    finish.await.expect("finish operation");
                    done_sender.send(()).expect("signal completion");
                    Ok(())
                })
                .await
        });
        started.await.expect("operation started");

        operation.abort();
        assert!(
            operation
                .await
                .expect_err("outer HTTP-equivalent future was cancelled")
                .is_cancelled()
        );
        let at_capacity = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(at_capacity.status(), StatusCode::SERVICE_UNAVAILABLE);

        finish_sender.send(()).expect("release detached operation");
        done.await.expect("detached operation completed");
        while record.lease_count() != 0 {
            tokio::task::yield_now().await;
        }
        let replacement = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(replacement.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn close_drains_children_and_root_and_is_idempotent() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let child = {
            let registry = app.server.inner.registry.lock().await;
            Arc::clone(
                &registry
                    .sessions
                    .get(&session_id)
                    .expect("registered child")
                    .lix,
            )
        };
        let root = Arc::clone(&app.server.inner.root);

        app.server.close().await.expect("close server");
        app.server.close().await.expect("close server again");
        assert_eq!(
            child
                .execute("SELECT 1", &[])
                .await
                .expect_err("child closed")
                .code,
            LixError::CODE_CLOSED
        );
        assert_eq!(
            root.execute("SELECT 1", &[])
                .await
                .expect_err("root closed")
                .code,
            LixError::CODE_CLOSED
        );
        let handshake = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(handshake.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            error_code(handshake).await,
            "LIX_ERROR_PROTOCOL_SERVER_CLOSED"
        );
    }

    #[tokio::test]
    async fn zero_capacity_is_rejected() {
        let lix = open_lix(OpenLixOptions::<Memory>::default())
            .await
            .expect("open lix");
        let result = LixProtocolServer::with_options(
            Arc::new(lix),
            ProtocolServerOptions {
                max_sessions: 0,
                session_idle_timeout: Duration::from_secs(1),
                ..ProtocolServerOptions::default()
            },
        );
        let Err(error) = result else {
            panic!("zero capacity must be rejected");
        };
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
}
