//! Canonical HTTP transport for independent pinned sessions on a workspace-mode
//! root [`lix_sdk::Lix`] handle.

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Query, Request, State},
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
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    future::Future,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, Notify, RwLock as AsyncRwLock, mpsc, watch},
    task::JoinHandle,
};
use tower_http::{
    compression::{
        CompressionLayer, CompressionLevel,
        predicate::{DefaultPredicate, Predicate as _, SizeAbove},
    },
    decompression::RequestDecompressionLayer,
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
const MIN_COMPRESSION_BODY_BYTES: u16 = 32 * 1024;
/// Maximum number of queries multiplexed onto one observation stream.
pub const MAX_MULTIPLEX_SUBSCRIPTIONS: usize = 32;

/// Maximum number of request blobs retained by one remote session.
const MAX_REQUEST_BLOB_CACHE_ENTRIES: usize = 8;
/// Blobs below this size are cheaper to send whole than to retain and hash.
const MIN_REQUEST_BLOB_CACHE_BYTES: usize = 32 * 1024;
/// Maximum aggregate bytes retained by one remote session's request blob cache.
const MAX_REQUEST_BLOB_CACHE_BYTES: usize = 2 * 1024 * 1024;
const BLOB_BASE_MISSING_CODE: &str = "LIX_REMOTE_BLOB_BASE_MISSING";

const SESSION_TOKEN_BYTES: usize = 32;
const SESSION_TOKEN_HEX_LEN: usize = SESSION_TOKEN_BYTES * 2;
const SESSION_OPEN_GATE_CLOSING: usize = 1 << (usize::BITS - 1);
const SESSION_OPEN_GATE_COUNT_MASK: usize = !SESSION_OPEN_GATE_CLOSING;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Resource limits for one workspace's remote protocol sessions.
#[derive(Clone, Copy, Debug)]
pub struct ProtocolServerOptions {
    /// Maximum number of retained remote sessions and their per-session caches.
    ///
    /// Handshakes may briefly validate up to this many lightweight candidate
    /// handles alongside retained sessions. A candidate is registered only
    /// after validation succeeds, and the retained sessions plus their
    /// per-session caches never exceed this limit.
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
/// independent branch-pinned session on that root's existing engine. Clones
/// share the same bounded in-memory session registry.
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
    session_open_gate: Arc<SessionOpenGate>,
    close_started: Once,
    close_result: watch::Sender<Option<Result<(), LixError>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Default)]
struct SessionOpenGate {
    state: AtomicUsize,
    drained: Notify,
}

struct PendingSessionOpen {
    gate: Arc<SessionOpenGate>,
    active: bool,
}

impl SessionOpenGate {
    fn reserve(self: &Arc<Self>, limit: usize) -> Result<PendingSessionOpen, ApiError> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & SESSION_OPEN_GATE_CLOSING != 0 {
                return Err(ApiError::server_closed());
            }
            if (state & SESSION_OPEN_GATE_COUNT_MASK) >= limit {
                return Err(ApiError::capacity());
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(PendingSessionOpen {
                        gate: Arc::clone(self),
                        active: true,
                    });
                }
                Err(current) => state = current,
            }
        }
    }

    fn start_closing(&self) {
        self.state
            .fetch_or(SESSION_OPEN_GATE_CLOSING, Ordering::AcqRel);
    }

    fn pending(&self) -> usize {
        self.state.load(Ordering::Acquire) & SESSION_OPEN_GATE_COUNT_MASK
    }
}

impl PendingSessionOpen {
    fn commit(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let previous = self.gate.state.fetch_sub(1, Ordering::AcqRel);
        let previous_pending = previous & SESSION_OPEN_GATE_COUNT_MASK;
        debug_assert!(previous_pending > 0, "pending session open count underflow");
        if previous_pending == 1 {
            self.gate.drained.notify_one();
        }
    }
}

impl Drop for PendingSessionOpen {
    fn drop(&mut self) {
        self.release();
    }
}

struct SessionRecord<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix: Arc<AsyncRwLock<Arc<Lix<S>>>>,
    last_used: Mutex<Instant>,
    leases: AtomicUsize,
    request_blobs: Mutex<RequestBlobCache>,
}

#[derive(Default)]
struct RequestBlobCache {
    entries: HashMap<String, Arc<[u8]>>,
    insertion_order: VecDeque<String>,
    total_bytes: usize,
}

impl RequestBlobCache {
    fn get(&self, sha256: &str) -> Option<Arc<[u8]>> {
        self.entries.get(sha256).cloned()
    }

    fn insert(&mut self, candidate: CachedRequestBlob) {
        if !is_request_blob_cacheable(candidate.bytes.len())
            || self.entries.contains_key(&candidate.sha256)
        {
            return;
        }
        while self.entries.len() >= MAX_REQUEST_BLOB_CACHE_ENTRIES
            || self
                .total_bytes
                .checked_add(candidate.bytes.len())
                .is_none_or(|total| total > MAX_REQUEST_BLOB_CACHE_BYTES)
        {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.len());
            }
        }
        self.total_bytes += candidate.bytes.len();
        self.insertion_order.push_back(candidate.sha256.clone());
        self.entries.insert(candidate.sha256, candidate.bytes);
    }
}

struct CachedRequestBlob {
    sha256: String,
    bytes: Arc<[u8]>,
}

impl<S> SessionRecord<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(lix: Lix<S>, now: Instant) -> Self {
        Self {
            lix: Arc::new(AsyncRwLock::new(Arc::new(lix))),
            last_used: Mutex::new(now),
            leases: AtomicUsize::new(0),
            request_blobs: Mutex::new(RequestBlobCache::default()),
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

    fn request_blob(&self, sha256: &str) -> Option<Arc<[u8]>> {
        self.request_blobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(sha256)
    }

    fn cache_request_blobs(&self, candidates: Vec<CachedRequestBlob>) {
        let mut cache = self
            .request_blobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for candidate in candidates {
            cache.insert(candidate);
        }
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
                parent.in_scope(|| {
                    runtime.block_on(async move {
                        let lix = lix.read().await;
                        operation(Arc::clone(&lix)).await
                    })
                })
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

    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<lix_sdk::SwitchBranchReceipt, LixError> {
        let runtime = Handle::try_current().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("access Lix server runtime: {error}"),
            )
        })?;
        let lix = Arc::clone(&self.record.lix);
        let operation_lease = self.clone();
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        tokio::task::spawn_blocking(move || {
            let _operation_lease = operation_lease;
            tracing::dispatcher::with_default(&dispatch, || {
                parent.in_scope(|| {
                    runtime.block_on(async move {
                        let mut lix = lix.write().await;
                        let (switched, receipt) = lix.switch_branch_session(options).await?;
                        *lix = Arc::new(switched);
                        Ok(receipt)
                    })
                })
            })
        })
        .await
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("join Lix server branch switch: {error}"),
            )
        })?
    }

    async fn observe(&self, sql: &str, params: &[Value]) -> Result<ServerObserve<S>, LixError> {
        let lix = self.record.lix.read().await;
        Ok(ServerObserve {
            events: Arc::new(Mutex::new(lix.observe(sql, params)?)),
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
        if options.max_sessions > SESSION_OPEN_GATE_COUNT_MASK {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "protocol max_sessions exceeds the supported session-open limit",
            ));
        }
        if options.max_request_body_bytes == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "protocol max_request_body_bytes must be greater than zero",
            ));
        }
        let (close_result, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(ServerInner {
                root,
                options,
                registry: AsyncMutex::new(SessionRegistry {
                    lifecycle: ServerLifecycle::Open,
                    sessions: HashMap::new(),
                }),
                session_open_gate: Arc::new(SessionOpenGate::default()),
                close_started: Once::new(),
                close_result,
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
            .layer(
                CompressionLayer::new()
                    .gzip(true)
                    .quality(CompressionLevel::Precise(2))
                    .compress_when(
                        DefaultPredicate::new().and(SizeAbove::new(MIN_COMPRESSION_BODY_BYTES)),
                    ),
            )
            .layer(DefaultBodyLimit::max(
                self.inner.options.max_request_body_bytes,
            ))
            // This must be outside DefaultBodyLimit so the configured limit
            // applies to expanded JSON rather than attacker-controlled gzip.
            .layer(RequestDecompressionLayer::new().gzip(true))
            .with_state(state)
    }

    /// Returns whether this server can be dropped without invalidating a live
    /// remote session.
    ///
    /// Expired, unleased sessions are idle. Concurrent registry work is
    /// conservatively treated as active so an eviction decision cannot race a
    /// handshake, request lease, session release, or shutdown.
    pub fn is_idle(&self) -> bool {
        let Ok(registry) = self.inner.registry.try_lock() else {
            return false;
        };
        if self.inner.session_open_gate.pending() != 0 {
            return false;
        }
        let now = Instant::now();
        registry
            .sessions
            .values()
            .all(|record| record.is_idle_expired(now, self.inner.options.session_idle_timeout))
    }

    /// Closes every child session and finally the root workspace session.
    /// Repeated calls are safe.
    pub async fn close(&self) -> Result<(), LixError> {
        let mut close_result = self.inner.close_result.subscribe();
        self.inner.close_started.call_once(|| {
            self.inner.session_open_gate.start_closing();
            let server = self.clone();
            tokio::spawn(async move {
                let closing_server = server.clone();
                let result =
                    match tokio::spawn(async move { closing_server.close_once().await }).await {
                        Ok(result) => result,
                        Err(error) => Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("join Lix protocol server close: {error}"),
                        )),
                    };
                server.inner.close_result.send_replace(Some(result));
            });
        });
        loop {
            let completed = close_result.borrow().clone();
            if let Some(result) = completed {
                return result;
            }
            close_result
                .changed()
                .await
                .expect("protocol server owns its close result channel");
        }
    }

    async fn close_once(&self) -> Result<(), LixError> {
        {
            let mut registry = self.inner.registry.lock().await;
            registry.lifecycle = ServerLifecycle::Closing;
        }
        while self.inner.session_open_gate.pending() != 0 {
            self.inner.session_open_gate.drained.notified().await;
        }
        let sessions = {
            let mut registry = self.inner.registry.lock().await;
            registry
                .sessions
                .drain()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
        };
        let mut first_error = None;
        for record in sessions {
            if let Err(error) = close_session_record(&record).await
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
        let mut registry = self.inner.registry.lock().await;
        registry.lifecycle = ServerLifecycle::Closed;
        first_error.map_or(Ok(()), Err)
    }

    async fn create_session(
        &self,
        initial_active_branch_id: Option<String>,
    ) -> Result<SessionLease<S>, ApiError> {
        let pending_open = self.reserve_session_open()?;

        let active_branch_id = match initial_active_branch_id {
            Some(active_branch_id) => active_branch_id,
            None => self.inner.root.active_branch_id().await?,
        };
        // Validate and open the pinned child before evicting any idle session.
        // A stale client branch preference therefore cannot consume capacity or
        // evict another client.
        let child = self.inner.root.open_session(active_branch_id).await?;

        let mut registry = self.inner.registry.lock().await;
        if let Err(error) = ensure_server_open(registry.lifecycle) {
            drop(registry);
            close_unregistered_session(child).await;
            return Err(error);
        }
        let session_id = loop {
            let candidate = match generate_session_id() {
                Ok(candidate) => candidate,
                Err(error) => {
                    drop(registry);
                    close_unregistered_session(child).await;
                    return Err(error);
                }
            };
            if !registry.sessions.contains_key(&candidate) {
                break candidate;
            }
        };
        let now = Instant::now();
        let expired_ids = registry
            .sessions
            .iter()
            .filter(|(_, record)| {
                record.is_idle_expired(now, self.inner.options.session_idle_timeout)
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        let mut removed_sessions = Vec::with_capacity(expired_ids.len().saturating_add(1));
        for session_id in expired_ids {
            if let Some(record) = registry.sessions.remove(&session_id) {
                removed_sessions.push(record);
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
                drop(registry);
                for record in removed_sessions {
                    close_removed_session(record).await;
                }
                close_unregistered_session(child).await;
                return Err(ApiError::capacity());
            };
            if let Some(record) = registry.sessions.remove(&lru_idle_id) {
                removed_sessions.push(record);
            }
        }
        let record = Arc::new(SessionRecord::new(child, now));
        registry
            .sessions
            .insert(session_id.clone(), Arc::clone(&record));
        let lease = SessionLease::new(session_id, record);
        drop(registry);
        for record in removed_sessions {
            close_removed_session(record).await;
        }
        pending_open.commit();
        Ok(lease)
    }

    fn reserve_session_open(&self) -> Result<PendingSessionOpen, ApiError> {
        self.inner
            .session_open_gate
            .reserve(self.inner.options.max_sessions)
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
            close_session_record(&record).await?;
        }
        Ok(())
    }
}

async fn close_session_record<S>(record: &SessionRecord<S>) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = record.lix.write().await;
    lix.close().await
}

async fn close_removed_session<S>(record: Arc<SessionRecord<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if let Err(error) = close_session_record(&record).await {
        tracing::warn!(
            code = %error.code,
            message = %error.message,
            "failed to close an evicted Lix protocol session"
        );
    }
}

async fn close_unregistered_session<S>(session: Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if let Err(error) = session.close().await {
        tracing::warn!(
            code = %error.code,
            message = %error.message,
            "failed to close an unregistered Lix protocol session"
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
    Query(request): Query<HandshakeRequest>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lease = match optional_session_id(&headers)? {
        Some(session_id) => {
            if request.active_branch_id.is_some() {
                return Err(ApiError::bad_request(
                    "activeBranchId is only allowed when creating a session",
                ));
            }
            state.lease(&session_id).await?
        }
        None => {
            let active_branch_id = match request.active_branch_id {
                Some(active_branch_id) if !active_branch_id.trim().is_empty() => {
                    Some(active_branch_id)
                }
                Some(_) => {
                    return Err(ApiError::bad_request(
                        "activeBranchId must be a non-empty string",
                    ));
                }
                None => None,
            };
            state.server.create_session(active_branch_id).await?
        }
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
            capabilities: ProtocolCapabilities {
                request_blob_splice: true,
            },
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
    let mut reconstructed_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let mut cache_candidates = Vec::new();
    let decoded = decode_request_params(
        request.params,
        None,
        request.cache_blobs,
        &mut reconstructed_bytes_remaining,
        &mut cache_candidate_bytes_remaining,
        &mut cache_candidates,
        |sha256| lease.record.request_blob(sha256),
    )?;
    let options = request.options.into();
    let params = decoded.values;
    let result = lease
        .run(move |lix| async move { lix.execute_with_options(&sql, &params, options).await })
        .await?;
    lease.record.cache_request_blobs(cache_candidates);
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
    let mut cache_candidates = Vec::new();
    let mut reconstructed_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let statements = request
        .statements
        .into_iter()
        .enumerate()
        .map(|(index, statement)| {
            let decoded = decode_request_params(
                statement.params,
                Some(index),
                request.cache_blobs,
                &mut reconstructed_bytes_remaining,
                &mut cache_candidate_bytes_remaining,
                &mut cache_candidates,
                |sha256| lease.record.request_blob(sha256),
            )?;
            Ok(ExecuteBatchStatement {
                sql: required_non_empty(statement.sql, "statements[].sql")?,
                params: decoded.values,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let options = request.options.into();
    let results = lease
        .run(move |lix| async move { lix.execute_batch_with_options(&statements, options).await })
        .await?;
    lease.record.cache_request_blobs(cache_candidates);
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
    let receipt = lease.switch_branch(options).await?;
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
    let events = lease.observe(&sql, &params).await?;
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
        let events = lease.observe(&sql, &params).await?;
        let sender = sender.clone();
        let parent = tracing::Span::current();
        tasks.push(tokio::spawn(
            async move {
                let mut blob_base = None;
                loop {
                    let message = match events.next().await {
                        Ok(Some(event)) => {
                            match multiplex_observe_payload(event, blob_base.as_ref()) {
                                Ok((payload, next_blob_base)) => {
                                    let message = MultiplexObserveMessage::Next {
                                        subscription_id: subscription_id.clone(),
                                        payload,
                                    };
                                    if sender.send(message).await.is_err() {
                                        break;
                                    }
                                    blob_base = next_blob_base;
                                    continue;
                                }
                                Err(error) => MultiplexObserveMessage::Error {
                                    subscription_id: subscription_id.clone(),
                                    error: ErrorEnvelope::from_lix_error(&error),
                                },
                            }
                        }
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
                        delta: payload.delta,
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

struct DecodedRequestParams {
    values: Vec<Value>,
}

fn decode_request_params(
    params: Vec<RequestWireValue>,
    statement_index: Option<usize>,
    cache_full_blobs: bool,
    reconstructed_bytes_remaining: &mut usize,
    cache_candidate_bytes_remaining: &mut usize,
    cache_candidates: &mut Vec<CachedRequestBlob>,
    lookup_blob: impl Fn(&str) -> Option<Arc<[u8]>>,
) -> Result<DecodedRequestParams, ApiError> {
    let mut values = Vec::with_capacity(params.len());
    for (parameter_index, value) in params.into_iter().enumerate() {
        match value {
            RequestWireValue::Value(value) => {
                let value = value.try_into_engine().map_err(|error| {
                    invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        error.code,
                        error.message,
                    )
                })?;
                if cache_full_blobs
                    && let Value::Blob(bytes) = &value
                    && is_request_blob_cacheable(bytes.len())
                    && bytes.len() <= *cache_candidate_bytes_remaining
                {
                    prepare_cache_candidate(
                        sha256_hex(bytes),
                        bytes,
                        cache_candidate_bytes_remaining,
                        cache_candidates,
                    );
                }
                values.push(value);
            }
            RequestWireValue::BlobSplice(splice) => {
                let base_sha256 = splice.base_sha256;
                let result_sha256 = splice.result_sha256;
                if !is_lowercase_sha256(&base_sha256) {
                    return Err(invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice baseSha256 must be a lowercase SHA-256 hex digest",
                    ));
                }
                if !is_lowercase_sha256(&result_sha256) {
                    return Err(invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice resultSha256 must be a lowercase SHA-256 hex digest",
                    ));
                }
                let Some(base) = lookup_blob(&base_sha256) else {
                    return Err(ApiError::blob_base_missing(
                        base_sha256,
                        parameter_index,
                        statement_index,
                    ));
                };
                let prefix = usize::try_from(splice.prefix_bytes).map_err(|_| {
                    invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice prefixBytes is too large",
                    )
                })?;
                let suffix = usize::try_from(splice.suffix_bytes).map_err(|_| {
                    invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice suffixBytes is too large",
                    )
                })?;
                if prefix > base.len()
                    || suffix > base.len()
                    || prefix.saturating_add(suffix) > base.len()
                {
                    return Err(invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice prefix and suffix must not overlap the cached base",
                    ));
                }
                let insert = WireValue::Blob {
                    base64: splice.insert_base64,
                }
                .try_into_engine()
                .map_err(|error| {
                    invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        error.code,
                        format!("invalid blob splice insertBase64: {}", error.message),
                    )
                })?;
                let Value::Blob(insert) = insert else {
                    unreachable!("WireValue::Blob must decode to Value::Blob")
                };
                let reconstructed_len = prefix
                    .checked_add(insert.len())
                    .and_then(|length| length.checked_add(suffix))
                    .ok_or_else(|| {
                        invalid_parameter_error(
                            parameter_index,
                            statement_index,
                            LixError::CODE_INVALID_PARAM,
                            "reconstructed blob size overflows the server address space",
                        )
                    })?;
                if reconstructed_len > *reconstructed_bytes_remaining {
                    return Err(invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "aggregate reconstructed blobs exceed the {MAX_REQUEST_BLOB_CACHE_BYTES}-byte request limit"
                        ),
                    ));
                }
                *reconstructed_bytes_remaining -= reconstructed_len;
                let mut reconstructed = Vec::with_capacity(reconstructed_len);
                reconstructed.extend_from_slice(&base[..prefix]);
                reconstructed.extend_from_slice(&insert);
                reconstructed.extend_from_slice(&base[base.len() - suffix..]);
                let actual_sha256 = sha256_hex(&reconstructed);
                if actual_sha256 != result_sha256 {
                    return Err(invalid_parameter_error(
                        parameter_index,
                        statement_index,
                        LixError::CODE_INVALID_PARAM,
                        "blob splice resultSha256 does not match the reconstructed bytes",
                    ));
                }
                prepare_cache_candidate(
                    result_sha256,
                    &reconstructed,
                    cache_candidate_bytes_remaining,
                    cache_candidates,
                );
                values.push(Value::Blob(reconstructed.into()));
            }
        }
    }
    Ok(DecodedRequestParams { values })
}

fn prepare_cache_candidate(
    sha256: String,
    bytes: &[u8],
    bytes_remaining: &mut usize,
    candidates: &mut Vec<CachedRequestBlob>,
) {
    if !is_request_blob_cacheable(bytes.len())
        || bytes.len() > *bytes_remaining
        || candidates
            .iter()
            .any(|candidate| candidate.sha256 == sha256)
    {
        return;
    }
    *bytes_remaining -= bytes.len();
    candidates.push(CachedRequestBlob {
        sha256,
        bytes: Arc::from(bytes.to_vec()),
    });
}

fn invalid_parameter_error(
    parameter_index: usize,
    statement_index: Option<usize>,
    source_code: impl Into<String>,
    message: impl Into<String>,
) -> ApiError {
    let mut details = serde_json::json!({
        "parameterIndex": parameter_index,
        "sourceCode": source_code.into(),
    });
    if let Some(statement_index) = statement_index {
        details["statementIndex"] = statement_index.into();
    }
    ApiError::from(
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "invalid SQL parameter at index {parameter_index}: {}",
                message.into()
            ),
        )
        .with_details(details),
    )
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_request_blob_cacheable(length: usize) -> bool {
    (MIN_REQUEST_BLOB_CACHE_BYTES..=MAX_REQUEST_BLOB_CACHE_BYTES).contains(&length)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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

    fn blob_base_missing(
        base_sha256: String,
        parameter_index: usize,
        statement_index: Option<usize>,
    ) -> Self {
        let mut details = serde_json::json!({
            "baseSha256": base_sha256,
            "parameterIndex": parameter_index,
        });
        if let Some(statement_index) = statement_index {
            details["statementIndex"] = statement_index.into();
        }
        Self {
            status: StatusCode::CONFLICT,
            body: ErrorEnvelope::from_parts(
                BLOB_BASE_MISSING_CODE,
                "the blob splice base is not available in this remote session",
                Some("retry the request with the complete blob".to_string()),
                Some(details),
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

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeRequest {
    active_branch_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    protocol_version: u32,
    active_branch_id: String,
    session_id: String,
    capabilities: ProtocolCapabilities,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolCapabilities {
    request_blob_splice: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteRequest {
    sql: Option<String>,
    #[serde(default)]
    params: Vec<RequestWireValue>,
    #[serde(default)]
    options: ExecuteOptionsRequest,
    #[serde(default)]
    cache_blobs: bool,
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
    #[serde(default)]
    cache_blobs: bool,
}

#[derive(Debug, Deserialize)]
struct ExecuteBatchStatementRequest {
    sql: Option<String>,
    #[serde(default)]
    params: Vec<RequestWireValue>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RequestWireValue {
    BlobSplice(RequestBlobSplice),
    Value(WireValue),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestBlobSplice {
    #[serde(rename = "kind")]
    _kind: RequestBlobSpliceKind,
    base_sha256: String,
    result_sha256: String,
    prefix_bytes: u64,
    suffix_bytes: u64,
    insert_base64: String,
}

#[derive(Debug, Deserialize)]
enum RequestBlobSpliceKind {
    #[serde(rename = "blob-splice")]
    BlobSplice,
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
        payload: MultiplexObservePayload,
    },
    Error {
        subscription_id: String,
        error: ErrorEnvelope,
    },
}

struct BlobDeltaBase {
    sequence: u64,
    // ExecuteResult has immutable shared backing, so retaining the transport
    // base does not copy the point-read blob for every subscription.
    rows: ExecuteResult,
}

impl BlobDeltaBase {
    fn bytes(&self) -> &[u8] {
        point_blob_bytes(&self.rows).expect("blob delta bases are point blob results")
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObservePayload {
    sequence: u64,
    mutation_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ExecuteResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<SingleBlobSplice>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SingleBlobSplice {
    kind: &'static str,
    base_sequence: u64,
    prefix_bytes: u64,
    suffix_bytes: u64,
    insert_base64: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObserveEventResponse {
    subscription_id: String,
    sequence: u64,
    mutation_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ExecuteResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<SingleBlobSplice>,
}

const MIN_BLOB_DELTA_BYTES: usize = 32 * 1024;
const BLOB_DELTA_COMPARE_CHUNK_BYTES: usize = 64;
// Compared with only the full blob's Base64 length, this deliberately
// overestimates every delta-only JSON/SSE field. The shared event envelope is
// omitted from both sides, so passing the 90% test guarantees >10% savings.
const BLOB_DELTA_ENVELOPE_BUDGET_BYTES: usize = 512;

fn multiplex_observe_payload(
    event: ObserveEvent,
    base: Option<&BlobDeltaBase>,
) -> Result<(MultiplexObservePayload, Option<BlobDeltaBase>), LixError> {
    let next_base = point_blob_bytes(&event.rows).map(|_| BlobDeltaBase {
        sequence: event.sequence,
        rows: event.rows.clone(),
    });
    let delta = match base.zip(next_base.as_ref()) {
        Some((base, next)) if base.sequence.checked_add(1) == Some(event.sequence) => {
            single_blob_splice(base, next)?
        }
        _ => None,
    };

    let payload = if let Some(delta) = delta {
        MultiplexObservePayload {
            sequence: event.sequence,
            mutation_sequence: event.mutation_sequence,
            result: None,
            delta: Some(delta),
        }
    } else {
        MultiplexObservePayload {
            sequence: event.sequence,
            mutation_sequence: event.mutation_sequence,
            result: Some(ExecuteResponse::try_from(event.rows)?),
            delta: None,
        }
    };
    Ok((payload, next_base))
}

fn point_blob_bytes(result: &ExecuteResult) -> Option<&[u8]> {
    if result.columns() != ["data"]
        || result.rows().len() != 1
        || result.rows_affected() != 0
        || !result.notices().is_empty()
    {
        return None;
    }
    match result.rows()[0].values() {
        [Value::Blob(bytes)] => Some(bytes),
        _ => None,
    }
}

fn single_blob_splice(
    base: &BlobDeltaBase,
    next: &BlobDeltaBase,
) -> Result<Option<SingleBlobSplice>, LixError> {
    if next.bytes().len() < MIN_BLOB_DELTA_BYTES {
        return Ok(None);
    }
    let base_bytes = base.bytes();
    let next_bytes = next.bytes();
    let prefix_bytes = common_blob_prefix_len(base_bytes, next_bytes);
    let max_suffix = base_bytes
        .len()
        .saturating_sub(prefix_bytes)
        .min(next_bytes.len().saturating_sub(prefix_bytes));
    let suffix_bytes = common_blob_suffix_len(base_bytes, next_bytes, max_suffix);
    let insert_end = next_bytes.len().saturating_sub(suffix_bytes);
    let insert = &next_bytes[prefix_bytes..insert_end];
    let Some(full_base64_bytes) = padded_base64_len(next_bytes.len()) else {
        return Ok(None);
    };
    let Some(delta_base64_bytes) = padded_base64_len(insert.len()) else {
        return Ok(None);
    };
    let Some(delta_estimate) = delta_base64_bytes.checked_add(BLOB_DELTA_ENVELOPE_BUDGET_BYTES)
    else {
        return Ok(None);
    };
    if delta_estimate.saturating_mul(10) >= full_base64_bytes.saturating_mul(9) {
        return Ok(None);
    }
    let WireValue::Blob {
        base64: insert_base64,
    } = WireValue::try_from_engine(&Value::Blob(insert.to_vec().into()))?
    else {
        unreachable!("blob wire conversion must return a blob")
    };
    Ok(Some(SingleBlobSplice {
        kind: "single-blob-splice",
        base_sequence: base.sequence,
        prefix_bytes: u64::try_from(prefix_bytes).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "blob delta prefix is too large",
            )
        })?,
        suffix_bytes: u64::try_from(suffix_bytes).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "blob delta suffix is too large",
            )
        })?,
        insert_base64,
    }))
}

#[inline]
fn common_blob_prefix_len(left: &[u8], right: &[u8]) -> usize {
    if left.first() != right.first() {
        return 0;
    }
    let limit = left.len().min(right.len());
    let mut matched = 0;
    while limit - matched >= BLOB_DELTA_COMPARE_CHUNK_BYTES {
        let end = matched + BLOB_DELTA_COMPARE_CHUNK_BYTES;
        if left[matched..end] != right[matched..end] {
            break;
        }
        matched = end;
    }
    while matched < limit && left[matched] == right[matched] {
        matched += 1;
    }
    matched
}

#[inline]
fn common_blob_suffix_len(left: &[u8], right: &[u8], limit: usize) -> usize {
    debug_assert!(limit <= left.len().min(right.len()));
    if limit == 0 || left.last() != right.last() {
        return 0;
    }
    let mut matched = 0;
    while limit - matched >= BLOB_DELTA_COMPARE_CHUNK_BYTES {
        let left_start = left.len() - matched - BLOB_DELTA_COMPARE_CHUNK_BYTES;
        let right_start = right.len() - matched - BLOB_DELTA_COMPARE_CHUNK_BYTES;
        if left[left_start..left_start + BLOB_DELTA_COMPARE_CHUNK_BYTES]
            != right[right_start..right_start + BLOB_DELTA_COMPARE_CHUNK_BYTES]
        {
            break;
        }
        matched += BLOB_DELTA_COMPARE_CHUNK_BYTES;
    }
    while matched < limit {
        let left_index = left.len() - matched - 1;
        let right_index = right.len() - matched - 1;
        if left[left_index] != right[right_index] {
            break;
        }
        matched += 1;
    }
    matched
}

fn padded_base64_len(bytes: usize) -> Option<usize> {
    bytes.checked_add(2)?.checked_div(3)?.checked_mul(4)
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
    use flate2::{Compression, read::GzDecoder, write::GzEncoder};
    use http_body_util::BodyExt as _;
    use lix_sdk::{
        Memory, MemoryRead, MemoryWrite, OpenLixOptions, ReadOptions, StorageError,
        TracingTelemetrySink, WriteOptions, open_lix, open_lix_with_telemetry,
    };
    use serde_json::{Value as JsonValue, json};
    use std::{
        io::{Read as _, Write as _},
        sync::{Arc, Mutex},
    };
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

    #[derive(Clone, Debug)]
    struct GatedReadStorage {
        inner: Memory,
        first_reads: Arc<GatedReads>,
    }

    #[derive(Debug)]
    struct GatedReads {
        remaining: AtomicUsize,
        barrier: tokio::sync::Barrier,
    }

    impl GatedReadStorage {
        fn new(participants: usize) -> Self {
            Self {
                inner: Memory::new(),
                first_reads: Arc::new(GatedReads {
                    remaining: AtomicUsize::new(0),
                    barrier: tokio::sync::Barrier::new(participants),
                }),
            }
        }

        fn gate_next_reads(&self, count: usize) {
            self.first_reads.remaining.store(count, Ordering::Release);
        }
    }

    impl Storage for GatedReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self
                .first_reads
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                self.first_reads.barrier.wait().await;
            }
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
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
        new_session_at(app, None).await
    }

    async fn new_session_at(app: &Router, active_branch_id: Option<&str>) -> (String, JsonValue) {
        let uri = active_branch_id.map_or_else(
            || "/lix/v1".to_string(),
            |active_branch_id| format!("/lix/v1?activeBranchId={active_branch_id}"),
        );
        let response = request(app, "GET", &uri, None, None).await;
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

    fn wire_blob_json(bytes: &[u8]) -> JsonValue {
        let value = WireValue::try_from_engine(&Value::Blob(bytes.to_vec().into()))
            .expect("blob should encode");
        serde_json::to_value(value).expect("wire blob should serialize")
    }

    fn blob_splice_json(
        base: &[u8],
        result: &[u8],
        prefix_bytes: usize,
        suffix_bytes: usize,
        insert: &[u8],
    ) -> JsonValue {
        let insert_base64 = wire_blob_json(insert)["base64"]
            .as_str()
            .expect("blob base64")
            .to_string();
        json!({
            "kind": "blob-splice",
            "baseSha256": sha256_hex(base),
            "resultSha256": sha256_hex(result),
            "prefixBytes": prefix_bytes,
            "suffixBytes": suffix_bytes,
            "insertBase64": insert_base64,
        })
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).expect("gzip input");
        encoder.finish().expect("finish gzip")
    }

    async fn gzip_response_json(response: Response) -> JsonValue {
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_ENCODING),
            Some(&axum::http::HeaderValue::from_static("gzip"))
        );
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("compressed body")
            .to_bytes();
        let mut decoder = GzDecoder::new(bytes.as_ref());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).expect("decode gzip");
        serde_json::from_slice(&decoded).expect("compressed json")
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
        assert_eq!(first["capabilities"]["requestBlobSplice"], true);
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
    async fn pinned_protocol_sessions_switch_branches_independently() {
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

        let first_after = request(&app.router, "GET", "/lix/v1/", Some(&first_session), None).await;
        assert_eq!(response_json(first_after).await["activeBranchId"], draft);
        let second_after =
            request(&app.router, "GET", "/lix/v1/", Some(&second_session), None).await;
        assert_eq!(response_json(second_after).await["activeBranchId"], active);

        let inserted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&first_session),
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('remote-pinned-only', 'draft')"
            })),
        )
        .await;
        assert_eq!(inserted.status(), StatusCode::OK);
        let main_count = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&second_session),
            Some(json!({
                "sql": "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'remote-pinned-only'"
            })),
        )
        .await;
        assert_eq!(main_count.status(), StatusCode::OK);
        assert_eq!(
            response_json(main_count).await["rows"][0][0],
            json!({ "kind": "int", "value": 0 })
        );

        let first_sql_branch = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&first_session),
            Some(json!({ "sql": "SELECT lix_active_branch_id() AS branch_id" })),
        )
        .await;
        assert_eq!(first_sql_branch.status(), StatusCode::OK);
        assert_eq!(
            response_json(first_sql_branch).await["rows"][0][0],
            json!({ "kind": "text", "value": draft })
        );
        let second_sql_branch = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&second_session),
            Some(json!({ "sql": "SELECT lix_active_branch_id() AS branch_id" })),
        )
        .await;
        assert_eq!(second_sql_branch.status(), StatusCode::OK);
        assert_eq!(
            response_json(second_sql_branch).await["rows"][0][0],
            json!({ "kind": "text", "value": active })
        );

        let (initial_draft_session, initial_draft) =
            new_session_at(&app.router, Some(&draft)).await;
        assert_eq!(initial_draft["activeBranchId"], draft);
        let draft_count = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&initial_draft_session),
            Some(json!({
                "sql": "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'remote-pinned-only'"
            })),
        )
        .await;
        assert_eq!(draft_count.status(), StatusCode::OK);
        assert_eq!(
            response_json(draft_count).await["rows"][0][0],
            json!({ "kind": "int", "value": 1 })
        );
    }

    #[tokio::test]
    async fn invalid_initial_branch_does_not_create_a_protocol_session() {
        let app = app().await;
        let before = app.server.inner.registry.lock().await.sessions.len();
        let response = request(
            &app.router,
            "GET",
            "/lix/v1?activeBranchId=missing-branch",
            None,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(error_code(response).await, LixError::CODE_BRANCH_NOT_FOUND);
        assert_eq!(
            app.server.inner.registry.lock().await.sessions.len(),
            before
        );
    }

    #[tokio::test]
    async fn invalid_initial_branch_does_not_evict_an_idle_session_at_capacity() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            ..ProtocolServerOptions::default()
        })
        .await;
        let (existing_session, _) = new_session(&app.router).await;
        let invalid = request(
            &app.router,
            "GET",
            "/lix/v1?activeBranchId=missing-branch",
            None,
            None,
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::NOT_FOUND);

        let resumed = request(&app.router, "GET", "/lix/v1", Some(&existing_session), None).await;
        assert_eq!(resumed.status(), StatusCode::OK);
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
    async fn execute_reconstructs_cached_blob_splices_and_caches_each_result() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let cached = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1 AS data",
                "params": [wire_blob_json(&base)],
                "cacheBlobs": true,
            })),
        )
        .await;
        assert_eq!(cached.status(), StatusCode::OK);

        let first_insert = b"BETA";
        let replace_at = base.len() / 2;
        let mut first = base.clone();
        first[replace_at..replace_at + first_insert.len()].copy_from_slice(first_insert);
        let first_response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1 AS data",
                "params": [blob_splice_json(
                    &base,
                    &first,
                    replace_at,
                    base.len() - replace_at - first_insert.len(),
                    first_insert,
                )],
            })),
        )
        .await;
        assert_eq!(first_response.status(), StatusCode::OK);
        assert_eq!(
            response_json(first_response).await["rows"][0][0],
            wire_blob_json(&first)
        );

        let second_insert = b"!";
        let mut second = first.clone();
        second.extend_from_slice(second_insert);
        let second_response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1 AS data",
                "params": [blob_splice_json(
                    &first,
                    &second,
                    first.len(),
                    0,
                    second_insert,
                )],
            })),
        )
        .await;
        assert_eq!(second_response.status(), StatusCode::OK);
        assert_eq!(
            response_json(second_response).await["rows"][0][0],
            wire_blob_json(&second)
        );
    }

    #[tokio::test]
    async fn execute_batch_accepts_blob_splices() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let cached = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [wire_blob_json(&base)],
                "cacheBlobs": true,
            })),
        )
        .await;
        assert_eq!(cached.status(), StatusCode::OK);

        let mut result = base.clone();
        result[0] = b'b';
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    {
                        "sql": "SELECT $1 AS data",
                        "params": [blob_splice_json(
                            &base,
                            &result,
                            0,
                            base.len() - 1,
                            b"b",
                        )],
                    },
                    { "sql": "SELECT 2 AS value" },
                ],
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body[0]["rows"][0][0], wire_blob_json(&result));
        assert_eq!(body[1]["rows"][0][0], json!({ "kind": "int", "value": 2 }));
    }

    #[tokio::test]
    async fn missing_blob_splice_base_fails_before_sql_mutation() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let absent_base = b"not cached";
        let result = b"replacement";
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                "params": [
                    { "kind": "text", "value": "/must-not-exist.bin" },
                    blob_splice_json(absent_base, result, 0, 0, result),
                ],
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(error_code(response).await, BLOB_BASE_MISSING_CODE);

        let read = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT data FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/must-not-exist.bin" }],
            })),
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(response_json(read).await["rows"], json!([]));
    }

    #[tokio::test]
    async fn execute_batch_bounds_aggregate_blob_reconstruction_before_mutation() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MAX_REQUEST_BLOB_CACHE_BYTES / 2];
        let cached = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                "params": [
                    { "kind": "text", "value": "/aggregate-base.bin" },
                    wire_blob_json(&base),
                ],
                "cacheBlobs": true,
            })),
        )
        .await;
        assert_eq!(cached.status(), StatusCode::OK);

        let mut result = base.clone();
        result.push(b'b');
        let splice = blob_splice_json(&base, &result, base.len(), 0, b"b");
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    {
                        "sql": "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                        "params": [
                            { "kind": "text", "value": "/must-not-execute.bin" },
                            splice,
                        ],
                    },
                    {
                        "sql": "SELECT $1",
                        "params": [blob_splice_json(
                            &base,
                            &result,
                            base.len(),
                            0,
                            b"b",
                        )],
                    },
                ],
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(response).await, LixError::CODE_INVALID_PARAM);

        let read = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT data FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/must-not-execute.bin" }],
            })),
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(response_json(read).await["rows"], json!([]));
    }

    #[tokio::test]
    async fn failed_execute_does_not_publish_full_blob_cache_candidates() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let failed = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "NOT VALID SQL",
                "params": [wire_blob_json(&base)],
                "cacheBlobs": true,
            })),
        )
        .await;
        assert_ne!(failed.status(), StatusCode::OK);

        let mut result = base.clone();
        result[0] = b'b';
        let missing = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [blob_splice_json(
                    &base,
                    &result,
                    0,
                    base.len() - 1,
                    b"b",
                )],
            })),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::CONFLICT);
        assert_eq!(error_code(missing).await, BLOB_BASE_MISSING_CODE);
    }

    #[tokio::test]
    async fn malformed_and_hash_mismatched_blob_splices_are_rejected() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let cached = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [wire_blob_json(&base)],
                "cacheBlobs": true,
            })),
        )
        .await;
        assert_eq!(cached.status(), StatusCode::OK);

        let overlap = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [blob_splice_json(
                    &base,
                    &base,
                    base.len(),
                    1,
                    b"",
                )],
            })),
        )
        .await;
        assert_eq!(overlap.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(overlap).await, LixError::CODE_INVALID_PARAM);

        let mismatch = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT $1",
                "params": [{
                    "kind": "blob-splice",
                    "baseSha256": sha256_hex(&base),
                    "resultSha256": "0".repeat(64),
                    "prefixBytes": base.len(),
                    "suffixBytes": 0,
                    "insertBase64": wire_blob_json(b"")["base64"],
                }],
            })),
        )
        .await;
        assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(mismatch).await, LixError::CODE_INVALID_PARAM);
    }

    #[test]
    fn request_blob_cache_evicts_by_entry_and_byte_limits() {
        let mut cache = RequestBlobCache::default();
        for index in 0..=MAX_REQUEST_BLOB_CACHE_ENTRIES {
            cache.insert(CachedRequestBlob {
                sha256: format!("entry-{index}"),
                bytes: Arc::from(vec![
                    u8::try_from(index).expect("test index should fit");
                    MIN_REQUEST_BLOB_CACHE_BYTES
                ]),
            });
        }
        assert_eq!(cache.entries.len(), MAX_REQUEST_BLOB_CACHE_ENTRIES);
        assert!(cache.get("entry-0").is_none());
        assert!(
            cache
                .get(&format!("entry-{MAX_REQUEST_BLOB_CACHE_ENTRIES}"))
                .is_some()
        );

        cache.insert(CachedRequestBlob {
            sha256: "too-large".to_string(),
            bytes: Arc::from(vec![0_u8; MAX_REQUEST_BLOB_CACHE_BYTES + 1]),
        });
        assert!(cache.get("too-large").is_none());
        assert!(cache.total_bytes <= MAX_REQUEST_BLOB_CACHE_BYTES);

        cache.insert(CachedRequestBlob {
            sha256: "too-small".to_string(),
            bytes: Arc::from(vec![0_u8; MIN_REQUEST_BLOB_CACHE_BYTES - 1]),
        });
        assert!(cache.get("too-small").is_none());
    }

    #[test]
    fn request_cache_candidates_share_one_bounded_clone_budget() {
        let mut remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
        let mut candidates = Vec::new();
        let first = vec![b'a'; MAX_REQUEST_BLOB_CACHE_BYTES / 2];
        prepare_cache_candidate(sha256_hex(&first), &first, &mut remaining, &mut candidates);
        let after_first = remaining;
        prepare_cache_candidate(sha256_hex(&first), &first, &mut remaining, &mut candidates);
        assert_eq!(remaining, after_first, "duplicate should reuse candidate");

        let over_remaining = vec![b'b'; after_first + 1];
        prepare_cache_candidate(
            sha256_hex(&over_remaining),
            &over_remaining,
            &mut remaining,
            &mut candidates,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(remaining, after_first);
    }

    #[tokio::test]
    async fn default_body_limit_accepts_blobs_larger_than_axums_two_megabyte_default() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let blob = WireValue::try_from_engine(&Value::Blob(vec![0x41; 2 * 1024 * 1024].into()))
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
    async fn compressed_execute_requests_are_expanded_before_json_extraction() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let request_body = json!({
            "sql": "SELECT $1",
            "params": [{ "kind": "text", "value": "x".repeat(64 * 1024) }]
        })
        .to_string();
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/execute")
                    .header(SESSION_ID_HEADER, session_id)
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .header(axum::http::header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(gzip(request_body.as_bytes())))
                    .expect("compressed request"),
            )
            .await
            .expect("compressed response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await["rows"][0][0]["value"],
            "x".repeat(64 * 1024)
        );
    }

    #[tokio::test]
    async fn configured_body_limit_applies_to_expanded_json() {
        let app = app_with_options(ProtocolServerOptions {
            max_request_body_bytes: 1_024,
            ..ProtocolServerOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let request_body = json!({
            "sql": "SELECT $1",
            "params": [{ "kind": "text", "value": "x".repeat(2_048) }]
        })
        .to_string();
        let response = app
            .router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/execute")
                    .header(SESSION_ID_HEADER, session_id)
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .header(axum::http::header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(gzip(request_body.as_bytes())))
                    .expect("compressed request"),
            )
            .await
            .expect("compressed response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn large_finite_json_responses_use_gzip_but_sse_does_not() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let request_body = json!({
            "sql": "SELECT $1",
            "params": [{ "kind": "text", "value": "x".repeat(64 * 1024) }]
        });
        let mut request_builder = Request::builder()
            .method("POST")
            .uri("/lix/v1/execute")
            .header(SESSION_ID_HEADER, &session_id)
            .header(axum::http::header::ACCEPT_ENCODING, "gzip")
            .header(axum::http::header::CONTENT_TYPE, "application/json");
        let response = app
            .router
            .clone()
            .oneshot(
                request_builder
                    .body(Body::from(request_body.to_string()))
                    .expect("gzip response request"),
            )
            .await
            .expect("gzip response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            gzip_response_json(response).await["rows"][0][0]["value"],
            "x".repeat(64 * 1024)
        );

        let observe_body = json!({
            "subscriptions": [{
                "id": "large",
                "sql": "SELECT $1",
                "params": [{ "kind": "text", "value": "x".repeat(64 * 1024) }]
            }]
        });
        request_builder = Request::builder()
            .method("POST")
            .uri("/lix/v1/observe/multiplex")
            .header(SESSION_ID_HEADER, session_id)
            .header(axum::http::header::ACCEPT_ENCODING, "gzip")
            .header(axum::http::header::CONTENT_TYPE, "application/json");
        let response = app
            .router
            .oneshot(
                request_builder
                    .body(Body::from(observe_body.to_string()))
                    .expect("SSE response request"),
            )
            .await
            .expect("SSE response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_TYPE),
            Some(&axum::http::HeaderValue::from_static("text/event-stream"))
        );
        assert!(
            response
                .headers()
                .get(axum::http::header::CONTENT_ENCODING)
                .is_none()
        );
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

    fn point_blob_event(sequence: u64, bytes: Vec<u8>) -> ObserveEvent {
        ObserveEvent {
            sequence,
            mutation_sequence: sequence,
            rows: ExecuteResult::from_rows(
                vec!["data".to_string()],
                vec![vec![Value::Blob(bytes.into())]],
            ),
        }
    }

    fn apply_blob_splice(base: &[u8], delta: SingleBlobSplice) -> Vec<u8> {
        let Value::Blob(insert) = WireValue::Blob {
            base64: delta.insert_base64,
        }
        .try_into_engine()
        .expect("delta insert should decode") else {
            panic!("delta insert should be a blob")
        };
        let prefix = usize::try_from(delta.prefix_bytes).expect("prefix should fit");
        let suffix = usize::try_from(delta.suffix_bytes).expect("suffix should fit");
        let mut next = Vec::with_capacity(prefix + insert.len() + base.len() - suffix);
        next.extend_from_slice(&base[..prefix]);
        next.extend_from_slice(&insert);
        next.extend_from_slice(&base[base.len() - suffix..]);
        next
    }

    #[test]
    fn chunked_blob_edge_detection_matches_scalar_reference() {
        for len in [0, 1, 63, 64, 65, 127, 128, 129, 4_096] {
            let left = vec![b'a'; len];
            let mut variants = vec![left.clone()];
            if len > 0 {
                for index in [0, len / 2, len - 1] {
                    let mut changed = left.clone();
                    changed[index] = b'b';
                    variants.push(changed);
                }
            }
            let mut longer = left.clone();
            longer.push(b'b');
            variants.push(longer);

            for right in variants {
                let expected_prefix = left
                    .iter()
                    .zip(&right)
                    .take_while(|(left, right)| left == right)
                    .count();
                let suffix_limit = left
                    .len()
                    .saturating_sub(expected_prefix)
                    .min(right.len().saturating_sub(expected_prefix));
                let expected_suffix = left
                    .iter()
                    .rev()
                    .zip(right.iter().rev())
                    .take(suffix_limit)
                    .take_while(|(left, right)| left == right)
                    .count();
                assert_eq!(
                    common_blob_prefix_len(&left, &right),
                    expected_prefix,
                    "prefix mismatch for lengths {} and {}",
                    left.len(),
                    right.len()
                );
                assert_eq!(
                    common_blob_suffix_len(&left, &right, suffix_limit),
                    expected_suffix,
                    "suffix mismatch for lengths {} and {}",
                    left.len(),
                    right.len()
                );
            }
        }
    }

    #[test]
    fn multiplex_blob_delta_base_reuses_event_storage() {
        let event = point_blob_event(0, vec![b'a'; 1024 * 1024]);
        let event_bytes = point_blob_bytes(&event.rows).expect("point blob event");
        let event_ptr = event_bytes.as_ptr();
        let (_, base) = multiplex_observe_payload(event, None).expect("initial payload");
        let base = base.expect("blob base");

        assert_eq!(base.bytes().as_ptr(), event_ptr);
    }

    #[test]
    fn multiplex_blob_delta_roundtrips_replace_insert_and_delete() {
        let initial = vec![b'a'; 100 * 1024];
        let (payload, mut base) =
            multiplex_observe_payload(point_blob_event(0, initial.clone()), None)
                .expect("initial payload");
        assert!(payload.result.is_some());
        assert!(payload.delta.is_none());

        let mut replaced = initial.clone();
        replaced[50_000..50_032].fill(b'b');
        let (payload, next_base) =
            multiplex_observe_payload(point_blob_event(1, replaced.clone()), base.as_ref())
                .expect("replacement delta");
        assert_eq!(
            apply_blob_splice(&initial, payload.delta.expect("replacement splice")),
            replaced
        );
        base = next_base;

        let mut inserted = replaced.clone();
        inserted.splice(40_000..40_000, [b'x'; 32]);
        let (payload, next_base) =
            multiplex_observe_payload(point_blob_event(2, inserted.clone()), base.as_ref())
                .expect("insert delta");
        assert_eq!(
            apply_blob_splice(&replaced, payload.delta.expect("insert splice")),
            inserted
        );
        base = next_base;

        let mut deleted = inserted.clone();
        deleted.drain(60_000..60_032);
        let (payload, _) =
            multiplex_observe_payload(point_blob_event(3, deleted.clone()), base.as_ref())
                .expect("delete delta");
        assert_eq!(
            apply_blob_splice(&inserted, payload.delta.expect("delete splice")),
            deleted
        );
    }

    #[test]
    fn multiplex_blob_delta_requires_more_than_ten_percent_wire_saving() {
        let initial = vec![b'a'; 1024 * 1024];
        let (_, base) = multiplex_observe_payload(point_blob_event(0, initial.clone()), None)
            .expect("initial payload");

        let replace_89_percent = initial.len() * 89 / 100;
        let mut next = initial.clone();
        let start = (initial.len() - replace_89_percent) / 2;
        next[start..start + replace_89_percent].fill(b'b');
        let (payload, _) = multiplex_observe_payload(point_blob_event(1, next), base.as_ref())
            .expect("89 percent payload");
        assert!(payload.delta.is_some());
        assert!(payload.result.is_none());

        let replace_90_percent = initial.len() * 90 / 100;
        let mut next = initial.clone();
        let start = (initial.len() - replace_90_percent) / 2;
        next[start..start + replace_90_percent].fill(b'b');
        let (payload, _) = multiplex_observe_payload(point_blob_event(1, next), base.as_ref())
            .expect("90 percent payload");
        assert!(payload.result.is_some());
        assert!(payload.delta.is_none());
    }

    #[test]
    fn multiplex_blob_full_fallback_becomes_the_next_delta_base() {
        let initial = vec![b'a'; 100 * 1024];
        let (_, base) =
            multiplex_observe_payload(point_blob_event(0, initial), None).expect("initial payload");
        let replacement = vec![b'b'; 100 * 1024];
        let (payload, base) =
            multiplex_observe_payload(point_blob_event(1, replacement.clone()), base.as_ref())
                .expect("full fallback");
        assert!(payload.result.is_some());

        let mut localized = replacement.clone();
        localized[50_000] = b'c';
        let (payload, _) =
            multiplex_observe_payload(point_blob_event(2, localized.clone()), base.as_ref())
                .expect("localized delta");
        assert_eq!(
            apply_blob_splice(&replacement, payload.delta.expect("localized splice")),
            localized
        );
    }

    #[test]
    #[ignore = "manual observation fanout performance diagnostic"]
    fn multiplex_blob_delta_fanout_perf() {
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        const SAMPLES: usize = 60;
        for size_mib in [1_usize, 10] {
            let initial = vec![b'a'; size_mib * 1024 * 1024];
            let mut localized = initial.clone();
            let middle = localized.len() / 2;
            localized[middle] = b'b';
            let event = point_blob_event(1, localized);
            for fanout in [1_usize, 4, 16] {
                let bases = (0..fanout)
                    .map(|_| {
                        multiplex_observe_payload(point_blob_event(0, initial.clone()), None)
                            .expect("initial payload")
                            .1
                            .expect("blob base")
                    })
                    .collect::<Vec<_>>();
                let mut samples = Vec::with_capacity(SAMPLES);
                for _ in 0..SAMPLES {
                    let started = Instant::now();
                    for base in &bases {
                        black_box(
                            multiplex_observe_payload(event.clone(), Some(base))
                                .expect("delta payload"),
                        );
                    }
                    samples.push(started.elapsed());
                }
                samples.sort_unstable();
                let p50 = samples[SAMPLES / 2];
                let p95 = samples[SAMPLES * 95 / 100];
                let total_bytes = u32::try_from(size_mib * 1024 * 1024 * fanout)
                    .expect("diagnostic byte count should fit u32");
                let throughput = |elapsed: Duration| {
                    f64::from(total_bytes) / elapsed.as_secs_f64() / (1024.0 * 1024.0)
                };
                eprintln!(
                    "observe_fanout size_mib={size_mib} subscribers={fanout} p50_us={} p95_us={} logical_mib_s_p50={:.1}",
                    p50.as_micros(),
                    p95.as_micros(),
                    throughput(p50),
                );
            }
        }
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
    async fn server_idleness_tracks_live_expired_and_mutating_sessions() {
        let app = app().await;
        assert!(app.server.is_idle());

        let (session_id, _) = new_session(&app.router).await;
        assert!(!app.server.is_idle());

        let registry = app.server.inner.registry.lock().await;
        assert!(!app.server.is_idle());
        drop(registry);

        let deleted = request(
            &app.router,
            "DELETE",
            "/lix/v1/session",
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        assert!(app.server.is_idle());

        let expired = app_with_options(ProtocolServerOptions {
            session_idle_timeout: Duration::ZERO,
            ..ProtocolServerOptions::default()
        })
        .await;
        let (_session_id, _) = new_session(&expired.router).await;
        assert!(expired.server.is_idle());
    }

    #[tokio::test]
    async fn pending_session_open_reservations_are_bounded_and_cancel_safe() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            ..ProtocolServerOptions::default()
        })
        .await;
        let pending = app
            .server
            .reserve_session_open()
            .expect("reserve pending session open");
        assert!(!app.server.is_idle());

        let Err(at_capacity) = app.server.reserve_session_open() else {
            panic!("pending opens must be bounded");
        };
        assert_eq!(at_capacity.status, StatusCode::SERVICE_UNAVAILABLE);

        drop(pending);
        assert!(app.server.is_idle());
        drop(
            app.server
                .reserve_session_open()
                .expect("released reservation can be reused"),
        );
    }

    #[tokio::test]
    async fn close_waits_for_pending_session_opens_before_closing_the_root() {
        let app = app().await;
        let pending = app
            .server
            .reserve_session_open()
            .expect("reserve pending session open");
        let server = app.server.clone();
        let closing = tokio::spawn(async move { server.close().await });

        loop {
            let lifecycle = app.server.inner.registry.lock().await.lifecycle;
            if lifecycle == ServerLifecycle::Closing {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!closing.is_finished());
        let Err(closed) = app.server.reserve_session_open() else {
            panic!("closing server must reject new reservations");
        };
        assert_eq!(closed.status, StatusCode::SERVICE_UNAVAILABLE);

        drop(pending);
        closing
            .await
            .expect("join server close")
            .expect("close server");
        assert_eq!(
            app.server.inner.registry.lock().await.lifecycle,
            ServerLifecycle::Closed
        );
    }

    #[tokio::test]
    async fn close_waits_for_eviction_cleanup_in_a_pending_session_open() {
        let app = app_with_options(ProtocolServerOptions {
            max_sessions: 1,
            ..ProtocolServerOptions::default()
        })
        .await;
        let first = app
            .server
            .create_session(None)
            .await
            .expect("open first session");
        let first_session_id = first.session_id.clone();
        let first_record = Arc::clone(&first.record);
        drop(first);

        // Keep eviction cleanup blocked after the replacement is registered.
        // Shutdown must continue to track the whole create operation, not only
        // the child open and registry mutation.
        let first_session_read = first_record.lix.read().await;
        let branch_id = app
            .server
            .inner
            .root
            .active_branch_id()
            .await
            .expect("active branch");
        let server = app.server.clone();
        let replacement = tokio::spawn(async move { server.create_session(Some(branch_id)).await });

        loop {
            let replaced = {
                let registry = app.server.inner.registry.lock().await;
                registry.sessions.len() == 1 && !registry.sessions.contains_key(&first_session_id)
            };
            if replaced {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(app.server.inner.session_open_gate.pending(), 1);

        let server = app.server.clone();
        let mut closing = tokio::spawn(async move { server.close().await });
        loop {
            if app.server.inner.registry.lock().await.lifecycle != ServerLifecycle::Open {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut closing)
                .await
                .is_err(),
            "close completed before pending eviction cleanup"
        );

        drop(first_session_read);
        let replacement = replacement
            .await
            .expect("join replacement open")
            .expect("open replacement session");
        drop(replacement);
        closing
            .await
            .expect("join server close")
            .expect("close server");
    }

    #[tokio::test]
    async fn cancelled_close_caller_does_not_cancel_server_shutdown() {
        let app = app().await;
        let pending = app
            .server
            .reserve_session_open()
            .expect("reserve pending session open");
        let server = app.server.clone();
        let closing = tokio::spawn(async move { server.close().await });

        loop {
            if app.server.inner.registry.lock().await.lifecycle == ServerLifecycle::Closing {
                break;
            }
            tokio::task::yield_now().await;
        }
        closing.abort();
        assert!(
            closing
                .await
                .expect_err("close caller should be cancelled")
                .is_cancelled()
        );

        drop(pending);
        app.server
            .close()
            .await
            .expect("detached server close should complete");
        assert_eq!(
            app.server.inner.registry.lock().await.lifecycle,
            ServerLifecycle::Closed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_session_opens_do_not_hold_the_registry_lock_during_storage_reads() {
        const SESSION_COUNT: usize = 8;
        let storage = GatedReadStorage::new(SESSION_COUNT);
        let root = Arc::new(
            open_lix(OpenLixOptions::new(storage.clone()))
                .await
                .expect("open Lix"),
        );
        let branch_id = root.active_branch_id().await.expect("active branch");
        let server = LixProtocolServer::with_options(
            root,
            ProtocolServerOptions {
                max_sessions: SESSION_COUNT,
                ..ProtocolServerOptions::default()
            },
        )
        .expect("protocol server");
        storage.gate_next_reads(SESSION_COUNT);

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..SESSION_COUNT {
            let server = server.clone();
            let branch_id = branch_id.clone();
            tasks.spawn(async move {
                server
                    .create_session(Some(branch_id))
                    .await
                    .expect("open protocol session")
            });
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = tasks.join_next().await {
                drop(result.expect("join session open"));
            }
        })
        .await
        .expect("all session opens should reach the storage barrier concurrently");
        assert_eq!(
            server.inner.registry.lock().await.sessions.len(),
            SESSION_COUNT
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "manual concurrent session-open performance diagnostic"]
    async fn concurrent_session_open_perf() {
        const OPERATIONS: usize = 512;
        const OPERATIONS_AS_F64: f64 = 512.0;
        for concurrency in [1_usize, 8, 32, 64] {
            let app = app_with_options(ProtocolServerOptions {
                max_sessions: OPERATIONS + 16,
                ..ProtocolServerOptions::default()
            })
            .await;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let batch_started = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..OPERATIONS {
                let router = app.router.clone();
                let semaphore = Arc::clone(&semaphore);
                tasks.spawn(async move {
                    let permit = semaphore.acquire_owned().await.expect("semaphore open");
                    let request_started = Instant::now();
                    let response = request(&router, "GET", "/lix/v1", None, None).await;
                    assert_eq!(response.status(), StatusCode::OK);
                    drop(permit);
                    request_started.elapsed()
                });
            }
            let mut samples = Vec::with_capacity(OPERATIONS);
            while let Some(result) = tasks.join_next().await {
                samples.push(result.expect("join handshake"));
            }
            let elapsed = batch_started.elapsed();
            samples.sort_unstable();
            let p50 = samples[OPERATIONS / 2];
            let p95 = samples[OPERATIONS * 95 / 100];
            eprintln!(
                "session_open concurrency={concurrency} operations={OPERATIONS} ops_s={:.1} p50_us={} p95_us={} elapsed_ms={}",
                OPERATIONS_AS_F64 / elapsed.as_secs_f64(),
                p50.as_micros(),
                p95.as_micros(),
                elapsed.as_millis(),
            );
        }
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
        let lease = app
            .server
            .create_session(None)
            .await
            .expect("session lease");
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
            let current = Arc::clone(
                &registry
                    .sessions
                    .get(&session_id)
                    .expect("registered child")
                    .lix,
            );
            drop(registry);
            let current = current.read().await;
            Arc::clone(&*current)
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
