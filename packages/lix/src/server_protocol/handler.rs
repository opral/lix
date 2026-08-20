//! Canonical Lix Server Protocol implementation for independent pinned sessions.

#![cfg_attr(test, allow(clippy::large_futures))]

use crate::session::ExecuteOptions;
#[cfg(test)]
use crate::session::media_upload::FILE_UPLOAD_PART_BYTES;
use crate::sync::{
    MAX_SYNC_PULL_RESPONSE_BYTES, MAX_SYNC_REQUEST_ITEMS, SYNC_LONG_POLL_TIMEOUT, SyncBlobManifest,
    SyncBlobRegistration, SyncPushRequest, SyncPushResponse, SyncRepositoryPullResponse,
};
use bytes::Bytes;
use futures_core::Stream;
use http::{
    HeaderMap, HeaderName, Method, Request, Response as HttpResponse, StatusCode,
    header::{ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
};
use http_body::{Body, Frame, SizeHint};
use lix::storage::Storage;
use lix::{
    Blob, CreateBranchOptions, ExecuteBatchStatement, ExecuteIdempotency, ExecuteResult,
    ExecuteStatementMetadata, ExecutionDisposition, Lix, LixError, LixTransaction, ObserveEvent,
    ObserveEvents, SwitchBranchOptions, Value, VerifiedRequestBlob, WireValue,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::Infallible,
    future::Future,
    io,
    mem::size_of,
    pin::Pin,
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc, watch},
    task::JoinHandle,
};
use tracing::{Instrument as _, instrument::WithSubscriber as _};

const MAX_SYNC_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_SYNC_BLOB_MANIFEST_CHUNKS: usize = 16_384;

/// Request accepted by the canonical protocol handler.
pub type ServerProtocolRequest = Request<ServerProtocolBody>;
/// Response returned by the canonical protocol handler.
pub type ServerProtocolResponse = HttpResponse<ServerProtocolBody>;
type Response = ServerProtocolResponse;

/// Framework-neutral response body used for JSON, binary, and SSE responses.
pub struct ServerProtocolBody {
    inner: ServerProtocolBodyInner,
}

impl std::fmt::Debug for ServerProtocolBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerProtocolBody")
            .field(
                "streaming",
                &matches!(self.inner, ServerProtocolBodyInner::Stream(_)),
            )
            .finish()
    }
}

enum ServerProtocolBodyInner {
    Full(Option<Bytes>),
    Stream(Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>),
}

impl ServerProtocolBody {
    pub fn empty() -> Self {
        Self::full(Bytes::new())
    }

    pub fn full(bytes: impl Into<Bytes>) -> Self {
        Self {
            inner: ServerProtocolBodyInner::Full(Some(bytes.into())),
        }
    }

    pub fn stream(stream: impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static) -> Self {
        Self {
            inner: ServerProtocolBodyInner::Stream(Box::pin(stream)),
        }
    }

    async fn into_bytes(mut self, limit: usize) -> Result<Bytes, ApiError> {
        let mut collected = Vec::new();
        while let Some(frame) =
            std::future::poll_fn(|context| Pin::new(&mut self).poll_frame(context)).await
        {
            let frame = frame
                .map_err(|error| ApiError::bad_request(format!("read request body: {error}")))?;
            if let Ok(data) = frame.into_data() {
                let next_len = collected.len().saturating_add(data.len());
                if next_len > limit {
                    return Err(ApiError::payload_too_large(limit));
                }
                collected.extend_from_slice(&data);
            }
        }
        Ok(Bytes::from(collected))
    }
}

impl From<Bytes> for ServerProtocolBody {
    fn from(value: Bytes) -> Self {
        Self::full(value)
    }
}

impl From<Vec<u8>> for ServerProtocolBody {
    fn from(value: Vec<u8>) -> Self {
        Self::full(value)
    }
}

impl From<String> for ServerProtocolBody {
    fn from(value: String) -> Self {
        Self::full(value)
    }
}

impl From<&'static str> for ServerProtocolBody {
    fn from(value: &'static str) -> Self {
        Self::full(value)
    }
}

impl Body for ServerProtocolBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.inner {
            ServerProtocolBodyInner::Full(bytes) => {
                Poll::Ready(bytes.take().map(|bytes| Ok(Frame::data(bytes))))
            }
            ServerProtocolBodyInner::Stream(stream) => stream
                .as_mut()
                .poll_next(context)
                .map(|item| item.map(|result| result.map(Frame::data))),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.inner, ServerProtocolBodyInner::Full(None))
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            ServerProtocolBodyInner::Full(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            ServerProtocolBodyInner::Full(None) => SizeHint::with_exact(0),
            ServerProtocolBodyInner::Stream(_) => SizeHint::default(),
        }
    }
}

struct Json<T>(T);

trait IntoResponse {
    fn into_response(self) -> Response;
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(bytes) => {
                let mut response = HttpResponse::new(ServerProtocolBody::full(bytes));
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
                response
            }
            Err(error) => HttpResponse::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, "application/json")
                .body(ServerProtocolBody::full(format!(
                    "{{\"error\":{{\"code\":\"LIX_ERROR_INTERNAL\",\"message\":\"serialize protocol response: {error}\"}}}}"
                )))
                .expect("static protocol response is valid"),
        }
    }
}

impl IntoResponse for StatusCode {
    fn into_response(self) -> Response {
        HttpResponse::builder()
            .status(self)
            .body(ServerProtocolBody::empty())
            .expect("status-only protocol response is valid")
    }
}

impl<T> IntoResponse for (StatusCode, Json<T>)
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        let (status, body) = self;
        let mut response = body.into_response();
        *response.status_mut() = status;
        response
    }
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl<T> IntoResponse for ([(HeaderName, T); 1], Json<HandshakeResponse>)
where
    T: TryInto<http::HeaderValue>,
    T::Error: std::fmt::Debug,
{
    fn into_response(self) -> Response {
        let (headers, body) = self;
        let mut response = body.into_response();
        for (name, value) in headers {
            response.headers_mut().insert(
                name,
                value.try_into().expect("static protocol header is valid"),
            );
        }
        response
    }
}

#[derive(Default)]
struct Event {
    event: Option<&'static str>,
    data: String,
}

impl Event {
    fn event(mut self, event: &'static str) -> Self {
        self.event = Some(event);
        self
    }

    fn data(mut self, data: String) -> Self {
        self.data = data;
        self
    }

    fn encode(self) -> Bytes {
        let mut encoded = String::new();
        if let Some(event) = self.event {
            encoded.push_str("event: ");
            encoded.push_str(event);
            encoded.push('\n');
        }
        for line in self.data.lines() {
            encoded.push_str("data: ");
            encoded.push_str(line);
            encoded.push('\n');
        }
        encoded.push('\n');
        Bytes::from(encoded)
    }
}

/// Stable URL prefix owned by the Lix Server Protocol.
pub const PROTOCOL_PATH: &str = "/lix/v1";
/// Current wire protocol version.
pub const PROTOCOL_VERSION: u32 = 2;
/// Canonical method and path registry for protocol hosts and conformance tools.
pub const SERVER_PROTOCOL_ENDPOINTS: &[(&str, &str)] = &[
    ("GET", "/lix/v1"),
    ("DELETE", "/lix/v1/session"),
    ("POST", "/lix/v1/execute"),
    ("POST", "/lix/v1/execute-batch"),
    ("POST", "/lix/v1/sync/push"),
    ("GET", "/lix/v1/sync/pull"),
    ("GET", "/lix/v1/sync/history"),
    ("GET", "/lix/v1/sync/blob"),
    ("POST", "/lix/v1/sync/blob"),
    ("GET", "/lix/v1/sync/chunk"),
    ("PUT", "/lix/v1/sync/chunk"),
    ("POST", "/lix/v1/transaction/begin"),
    ("POST", "/lix/v1/transaction/execute"),
    ("POST", "/lix/v1/transaction/commit"),
    ("POST", "/lix/v1/transaction/rollback"),
    ("GET", "/lix/v1/file"),
    ("POST", "/lix/v1/file/upsert"),
    ("POST", "/lix/v1/file/upsert-batch"),
    ("POST", "/lix/v1/branch/create"),
    ("POST", "/lix/v1/checkpoint/create"),
    ("POST", "/lix/v1/undo"),
    ("POST", "/lix/v1/redo"),
    ("POST", "/lix/v1/branch/switch"),
    ("POST", "/lix/v1/observe"),
    ("POST", "/lix/v1/observe/multiplex"),
];
/// Header carrying the opaque server-issued session capability.
pub const SESSION_ID_HEADER: &str = "lix-session-id";
/// Standard request identity for replay-safe SQL mutations.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// Internal capability binding requests to one remote transaction lifecycle.
pub const TRANSACTION_ID_HEADER: &str = "lix-transaction-id";
/// Header distinguishing a missing file from a present empty file on the raw
/// binary file-read endpoint.
pub const FILE_FOUND_HEADER: &str = "lix-file-found";
/// Client-generated identity for sequential resumable file parts.
pub const FILE_UPLOAD_ID_HEADER: &str = "lix-upload-id";
/// Default maximum number of live remote sessions for one repository.
pub const DEFAULT_MAX_SESSIONS: usize = 64;
/// Default idle lifetime for a remote session.
pub const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_mins(30);
/// Default JSON request ceiling. Base64 expands blobs by roughly one third,
/// so 64 MiB carries the engine's 32 MiB maximum plugin archive with room for
/// the SQL envelope and also covers ordinary larger document blobs.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
/// Largest number of file entries accepted by one native batch request.
///
/// Keeping this bounded makes the fast path predictable for the normal bulk
/// upload shape while keeping one request from monopolizing a session.
const MAX_BINARY_FILE_UPSERT_BATCH_ENTRIES: usize = 1024;
/// Maximum number of queries multiplexed onto one observation stream.
pub const MAX_MULTIPLEX_SUBSCRIPTIONS: usize = 32;

/// Maximum number of request blobs retained by one remote session.
const MAX_REQUEST_BLOB_CACHE_ENTRIES: usize = 8;
/// Blobs below this size are cheaper to send whole than to retain and hash.
const MIN_REQUEST_BLOB_CACHE_BYTES: usize = 32 * 1024;
/// Maximum aggregate bytes retained by one remote session's request blob
/// cache. This holds one exact 10.68 MB production CSV (or 10 MB JSON) base,
/// while inserting its similarly sized successor evicts the predecessor.
const MAX_REQUEST_BLOB_CACHE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum request-base bytes retained across every remote session for one
/// repository. Once full, additional bases simply use the existing complete-
/// blob retry path; request correctness never depends on cache admission.
pub const DEFAULT_MAX_REQUEST_BLOB_CACHE_BYTES: usize = 128 * 1024 * 1024;
const BLOB_BASE_MISSING_CODE: &str = "LIX_REMOTE_BLOB_BASE_MISSING";
/// Maximum bytes held by one raw file-download response body at a time.
const FILE_READ_STREAM_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

const SESSION_TOKEN_BYTES: usize = 32;
const SESSION_TOKEN_HEX_LEN: usize = SESSION_TOKEN_BYTES * 2;
const MAX_COMPLETED_REMOTE_TRANSACTIONS: usize = 8;
const MAX_IDEMPOTENCY_COMPONENT_BYTES: usize = 255;
const SESSION_OPEN_GATE_CLOSING: usize = 1 << (usize::BITS - 1);
const SESSION_OPEN_GATE_COUNT_MASK: usize = !SESSION_OPEN_GATE_CLOSING;
const SESSION_ACTIVITY_TRANSACTION: usize = 1 << (usize::BITS - 1);
const SESSION_ACTIVITY_LEASE_COUNT_MASK: usize = !SESSION_ACTIVITY_TRANSACTION;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Principal selected by the host after authenticating the outer request.
///
/// This value is deliberately separate from HTTP request data. A protocol
/// client can therefore never claim another account or replay namespace by
/// supplying a header, query parameter, or request extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerProtocolPrincipal {
    /// Explicit anonymous access selected by the host.
    Anonymous,
    /// A host-authenticated Lix account and stable idempotency namespace.
    Authenticated {
        account_id: String,
        idempotency_scope: String,
    },
}

impl ServerProtocolPrincipal {
    fn account_id(&self) -> &str {
        match self {
            Self::Anonymous => lix::ANONYMOUS_ACCOUNT_ID,
            Self::Authenticated { account_id, .. } => account_id,
        }
    }

    fn idempotency_scope(&self) -> Option<String> {
        match self {
            Self::Anonymous => None,
            Self::Authenticated {
                account_id,
                idempotency_scope,
            } => Some(format!(
                "account:{}:{account_id}{idempotency_scope}",
                account_id.len()
            )),
        }
    }
}

/// Trusted, host-owned metadata for one protocol request.
#[derive(Clone, Debug)]
pub struct ServerProtocolContext {
    pub principal: ServerProtocolPrincipal,
    pub durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
}

impl ServerProtocolContext {
    /// Creates context for explicitly anonymous access.
    pub fn anonymous() -> Self {
        Self {
            principal: ServerProtocolPrincipal::Anonymous,
            durable_terminal_storage_notifier: None,
        }
    }
}

/// Resource limits for one repository's Lix Server Protocol sessions.
#[derive(Clone, Copy, Debug)]
pub struct ServerProtocolOptions {
    /// Maximum number of retained remote sessions and their per-session caches.
    ///
    /// Handshakes may briefly validate up to this many lightweight candidate
    /// handles alongside retained sessions. A candidate is registered only
    /// after validation succeeds, and the retained sessions plus their
    /// per-session caches never exceed this limit.
    pub max_sessions: usize,
    pub session_idle_timeout: Duration,
    pub max_request_body_bytes: usize,
    /// Maximum request-base bytes retained across all sessions.
    pub max_request_blob_cache_bytes: usize,
}

impl Default for ServerProtocolOptions {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_SESSIONS,
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_request_blob_cache_bytes: DEFAULT_MAX_REQUEST_BLOB_CACHE_BYTES,
        }
    }
}

/// Persistent canonical protocol server for one Lix repository.
///
/// A server owns one root [`Lix`] and opens every remote client as an
/// independent branch-pinned session on that root's existing engine. Clones
/// share the same bounded in-memory session registry.
#[expect(missing_debug_implementations)]
pub struct LixServerProtocol<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    inner: Arc<ServerInner<S>>,
}

impl<S> Clone for LixServerProtocol<S>
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
    options: ServerProtocolOptions,
    registry: AsyncMutex<SessionRegistry<S>>,
    request_blob_budget: Arc<RequestBlobCacheBudget>,
    sync_push_gate: Arc<AsyncMutex<()>>,
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
    lix: Arc<Lix<S>>,
    principal: ServerProtocolPrincipal,
    transactions: AsyncMutex<RemoteTransactionRegistry<S>>,
    last_used: Mutex<Instant>,
    activity: Arc<SessionActivity>,
    request_blobs: Mutex<RequestBlobCache>,
    max_reconstructed_request_blob_bytes: usize,
}

struct RemoteTransactionRegistry<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    active: Option<ActiveRemoteTransaction<S>>,
    completed: VecDeque<CompletedRemoteTransaction>,
}

struct ActiveRemoteTransaction<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    id: String,
    transaction: LixTransaction<S>,
    _pin: RemoteTransactionPin,
}

#[derive(Debug)]
struct RemoteTransactionPin {
    activity: Arc<SessionActivity>,
}

impl RemoteTransactionPin {
    fn acquire(activity: Arc<SessionActivity>) -> Result<Self, LixError> {
        activity.acquire_transaction()?;
        Ok(Self { activity })
    }
}

impl Drop for RemoteTransactionPin {
    fn drop(&mut self) {
        self.activity.release_transaction();
    }
}

#[derive(Debug, Default)]
struct SessionActivity {
    state: AtomicUsize,
}

impl SessionActivity {
    fn acquire_lease(&self) {
        let previous = self.state.fetch_add(1, Ordering::AcqRel);
        assert!(
            previous & SESSION_ACTIVITY_LEASE_COUNT_MASK < SESSION_ACTIVITY_LEASE_COUNT_MASK,
            "session lease count overflow"
        );
    }

    fn release_lease(&self) {
        let previous = self.state.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous & SESSION_ACTIVITY_LEASE_COUNT_MASK > 0,
            "session lease count underflow"
        );
    }

    fn acquire_transaction(&self) -> Result<(), LixError> {
        let previous = self
            .state
            .fetch_or(SESSION_ACTIVITY_TRANSACTION, Ordering::AcqRel);
        if previous & SESSION_ACTIVITY_TRANSACTION != 0 {
            return Err(remote_transaction_state_error(
                "Lix session already has an active transaction",
            ));
        }
        Ok(())
    }

    fn release_transaction(&self) {
        let previous = self
            .state
            .fetch_and(!SESSION_ACTIVITY_TRANSACTION, Ordering::AcqRel);
        assert!(
            previous & SESSION_ACTIVITY_TRANSACTION != 0,
            "session transaction pin was not active"
        );
    }

    #[cfg(test)]
    fn lease_count(&self) -> usize {
        self.state.load(Ordering::Acquire) & SESSION_ACTIVITY_LEASE_COUNT_MASK
    }

    fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == 0
    }

    #[cfg(test)]
    fn transaction_is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) & SESSION_ACTIVITY_TRANSACTION != 0
    }
}

#[derive(Clone)]
struct CompletedRemoteTransaction {
    id: String,
    result: Result<RemoteTransactionOutcome, LixError>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RemoteTransactionOutcome {
    Committed,
    RolledBack,
}

impl<S> Default for RemoteTransactionRegistry<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            active: None,
            completed: VecDeque::new(),
        }
    }
}

struct RequestBlobCacheBudget {
    max_bytes: usize,
    total_bytes: AtomicUsize,
}

impl RequestBlobCacheBudget {
    fn new(max_bytes: usize) -> Self {
        debug_assert!(max_bytes > 0, "request blob cache budget must be positive");
        Self {
            max_bytes,
            total_bytes: AtomicUsize::new(0),
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.total_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.max_bytes {
                return false;
            }
            match self.total_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        let previous = self.total_bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(
            previous >= bytes,
            "request blob cache budget accounting underflow"
        );
    }
}

struct RequestBlobCache {
    entries: HashMap<String, VerifiedRequestBlob>,
    insertion_order: VecDeque<String>,
    total_bytes: usize,
    budget: Arc<RequestBlobCacheBudget>,
}

impl RequestBlobCache {
    fn new(budget: Arc<RequestBlobCacheBudget>) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            total_bytes: 0,
            budget,
        }
    }

    fn get(&self, sha256: &str) -> Option<VerifiedRequestBlob> {
        self.entries.get(sha256).cloned()
    }

    fn insert(&mut self, candidate: CachedRequestBlob) {
        let candidate_bytes = candidate.blob.blob().len();
        if !is_request_blob_cacheable(candidate_bytes)
            || self.entries.contains_key(candidate.blob.sha256())
        {
            return;
        }

        let mut projected_entries = self.entries.len();
        let mut projected_bytes = self.total_bytes;
        let mut eviction_count = 0usize;
        while projected_entries >= MAX_REQUEST_BLOB_CACHE_ENTRIES
            || projected_bytes
                .checked_add(candidate_bytes)
                .is_none_or(|total| total > MAX_REQUEST_BLOB_CACHE_BYTES)
        {
            let oldest = self
                .insertion_order
                .get(eviction_count)
                .expect("cache insertion order covers every retained entry");
            let removed = self
                .entries
                .get(oldest)
                .expect("cache insertion order contains only retained entries");
            projected_entries -= 1;
            projected_bytes -= removed.blob().len();
            eviction_count += 1;
        }

        loop {
            let evicted_bytes = self.total_bytes - projected_bytes;
            let additional_bytes = candidate_bytes.saturating_sub(evicted_bytes);
            if self.budget.try_reserve(additional_bytes) {
                break;
            }
            let Some(oldest) = self.insertion_order.get(eviction_count) else {
                return;
            };
            let removed = self
                .entries
                .get(oldest)
                .expect("cache insertion order contains only retained entries");
            projected_bytes -= removed.blob().len();
            eviction_count += 1;
        }

        let evicted_bytes = self.total_bytes - projected_bytes;
        for _ in 0..eviction_count {
            let oldest = self
                .insertion_order
                .pop_front()
                .expect("planned cache eviction has an insertion-order entry");
            let removed = self
                .entries
                .remove(&oldest)
                .expect("planned cache eviction has a retained entry");
            self.total_bytes -= removed.blob().len();
        }
        self.total_bytes = self
            .total_bytes
            .checked_add(candidate_bytes)
            .expect("the per-session cache limit bounds retained bytes");
        let sha256 = candidate.blob.sha256().to_owned();
        self.insertion_order.push_back(sha256.clone());
        self.entries.insert(sha256, candidate.blob);
        if evicted_bytes > candidate_bytes {
            self.budget.release(evicted_bytes - candidate_bytes);
        }
    }
}

impl Drop for RequestBlobCache {
    fn drop(&mut self) {
        self.budget.release(self.total_bytes);
    }
}

struct CachedRequestBlob {
    blob: VerifiedRequestBlob,
}

impl<S> SessionRecord<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        lix: Lix<S>,
        principal: ServerProtocolPrincipal,
        now: Instant,
        max_reconstructed_request_blob_bytes: usize,
        request_blob_budget: Arc<RequestBlobCacheBudget>,
    ) -> Self {
        Self {
            lix: Arc::new(lix),
            principal,
            transactions: AsyncMutex::new(RemoteTransactionRegistry::default()),
            last_used: Mutex::new(now),
            activity: Arc::new(SessionActivity::default()),
            request_blobs: Mutex::new(RequestBlobCache::new(request_blob_budget)),
            max_reconstructed_request_blob_bytes,
        }
    }

    fn acquire(&self, now: Instant) {
        self.activity.acquire_lease();
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
        self.activity.release_lease();
    }

    #[cfg(test)]
    fn lease_count(&self) -> usize {
        self.activity.lease_count()
    }

    fn last_used(&self) -> Instant {
        *self
            .last_used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_idle_expired(&self, now: Instant, timeout: Duration) -> bool {
        self.is_idle() && now.saturating_duration_since(self.last_used()) >= timeout
    }

    fn is_idle(&self) -> bool {
        self.activity.is_idle()
    }

    fn request_blob(&self, sha256: &str) -> Option<VerifiedRequestBlob> {
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
    durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
}

impl<S> SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        session_id: String,
        record: Arc<SessionRecord<S>>,
        durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
    ) -> Self {
        record.acquire(Instant::now());
        Self {
            session_id,
            record,
            durable_terminal_storage_notifier,
        }
    }

    async fn run_detached<T, Fut>(
        &self,
        operation: Fut,
        join_context: &'static str,
    ) -> Result<T, LixError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, LixError>> + Send + 'static,
    {
        // Dropping a Tokio JoinHandle detaches its task. Keep a lease inside
        // durable work so HTTP cancellation cannot make an active session look
        // idle or eligible for eviction before the operation reaches its
        // terminal boundary.
        let operation_lease = self.clone();
        let durable_terminal_storage_notifier = self.durable_terminal_storage_notifier.clone();
        let parent = tracing::Span::current();
        tokio::spawn(
            async move {
                let _operation_lease = operation_lease;
                let result = operation.await;
                if let (Some(notifier), Err(error)) = (&durable_terminal_storage_notifier, &result)
                {
                    notifier.signal_if_terminal(error);
                }
                result
            }
            .instrument(parent)
            .with_current_subscriber(),
        )
        .await
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("{join_context}: {error}"),
            )
        })?
    }

    async fn run_durable<T, F, Fut>(&self, operation: F) -> Result<T, LixError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Lix<S>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, LixError>> + Send + 'static,
    {
        let lix = Arc::clone(&self.record.lix);
        self.run_detached(
            async move { operation(lix).await },
            "join Lix server operation",
        )
        .await
    }

    /// Runs a read inline on Tokio. Dropping the request future directly drops
    /// the engine future and its session read lock; no cancellation relay or
    /// second executor is involved.
    async fn run_cancellable_read<T, F, Fut>(&self, operation: F) -> Result<T, LixError>
    where
        F: FnOnce(Arc<Lix<S>>) -> Fut + Send,
        Fut: Future<Output = Result<T, LixError>> + Send,
    {
        let result = operation(Arc::clone(&self.record.lix)).await;
        if let (Some(notifier), Err(error)) = (&self.durable_terminal_storage_notifier, &result) {
            notifier.signal_if_terminal(error);
        }
        result
    }

    async fn execute(
        &self,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
        metadata: ExecuteStatementMetadata,
        idempotency: Option<ExecuteIdempotency>,
    ) -> Result<ExecuteResult, LixError> {
        let disposition = self.record.lix.execution_disposition(&sql)?;
        match disposition {
            ExecutionDisposition::CancellableRead => {
                self.run_cancellable_read(move |lix| async move {
                    let idempotency = bind_idempotency_to_session_branch(&lix, idempotency).await?;
                    lix.execute_with_idempotency_and_options_and_metadata(
                        sql,
                        params,
                        options,
                        metadata,
                        idempotency,
                    )
                    .await
                })
                .await
            }
            ExecutionDisposition::Durable => {
                self.run_durable(move |lix| async move {
                    let idempotency = bind_idempotency_to_session_branch(&lix, idempotency).await?;
                    lix.execute_with_idempotency_and_options_and_metadata(
                        sql,
                        params,
                        options,
                        metadata,
                        idempotency,
                    )
                    .await
                })
                .await
            }
        }
    }

    async fn execute_batch(
        &self,
        statements: Vec<ExecuteBatchStatement>,
        options: ExecuteOptions,
        statement_metadata: Vec<ExecuteStatementMetadata>,
        idempotency: Option<ExecuteIdempotency>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let disposition = self.record.lix.execute_batch_disposition(&statements)?;
        match disposition {
            ExecutionDisposition::CancellableRead => {
                self.run_cancellable_read(move |lix| async move {
                    let idempotency = bind_idempotency_to_session_branch(&lix, idempotency).await?;
                    lix.execute_batch_with_idempotency_and_options_and_metadata(
                        statements,
                        options,
                        statement_metadata,
                        idempotency,
                    )
                    .await
                })
                .await
            }
            ExecutionDisposition::Durable => {
                self.run_durable(move |lix| async move {
                    let idempotency = bind_idempotency_to_session_branch(&lix, idempotency).await?;
                    lix.execute_batch_with_idempotency_and_options_and_metadata(
                        statements,
                        options,
                        statement_metadata,
                        idempotency,
                    )
                    .await
                })
                .await
            }
        }
    }

    async fn begin_transaction(&self) -> Result<String, LixError> {
        let mut transactions = self.record.transactions.lock().await;
        if transactions.active.is_some() {
            return Err(remote_transaction_state_error(
                "Lix session already has an active transaction",
            ));
        }
        let lix = Arc::clone(&self.record.lix);
        let id = generate_capability_id()?;
        let pin = RemoteTransactionPin::acquire(Arc::clone(&self.record.activity))?;
        transactions.active = Some(ActiveRemoteTransaction {
            id: id.clone(),
            transaction: lix.begin_transaction().await?,
            _pin: pin,
        });
        Ok(id)
    }

    async fn transaction_execute(
        &self,
        transaction_id: String,
        sql: String,
        params: Vec<Value>,
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let record = Arc::clone(&self.record);
        let (cancel_on_drop, mut cancelled) = tokio::sync::oneshot::channel::<()>();
        let result = self.run_detached(
            async move {
                let mut transactions = record.transactions.lock().await;
                let mut active = transactions.active.take().ok_or_else(|| {
                    completed_transaction_error(&transactions, &transaction_id).unwrap_or_else(
                        || remote_transaction_state_error("Lix session has no active transaction"),
                    )
                })?;
                if active.id != transaction_id {
                    transactions.active = Some(active);
                    return Err(remote_transaction_state_error(
                        "remote transaction capability does not match the active transaction",
                    ));
                }
                let execution = active.transaction.execute_with_options(sql, params, options);
                tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        let rollback = active.transaction.rollback().await;
                        let completed = rollback
                            .map(|()| RemoteTransactionOutcome::RolledBack);
                        transactions.completed.push_back(CompletedRemoteTransaction {
                            id: transaction_id,
                            result: completed.clone(),
                        });
                        while transactions.completed.len() > MAX_COMPLETED_REMOTE_TRANSACTIONS {
                            transactions.completed.pop_front();
                        }
                        match completed {
                            Ok(_) => Err(LixError::new(
                                LixError::CODE_CLOSED,
                                "cancelled remote transaction statement rolled back its transaction",
                            )),
                            Err(error) => Err(error),
                        }
                    }
                    result = execution => {
                        transactions.active = Some(active);
                        result
                    }
                }
            },
            "join Lix server transaction operation",
        )
        .await;
        drop(cancel_on_drop);
        result
    }

    async fn commit_transaction(&self, transaction_id: String) -> Result<(), LixError> {
        self.finish_transaction(transaction_id, RemoteTransactionOutcome::Committed)
            .await
    }

    async fn rollback_transaction(&self, transaction_id: String) -> Result<(), LixError> {
        self.finish_transaction(transaction_id, RemoteTransactionOutcome::RolledBack)
            .await
    }

    async fn finish_transaction(
        &self,
        transaction_id: String,
        requested: RemoteTransactionOutcome,
    ) -> Result<(), LixError> {
        let record = Arc::clone(&self.record);
        self.run_detached(
            async move {
                let mut transactions = record.transactions.lock().await;
                let Some(active) = transactions.active.take() else {
                    return completed_transaction_result(&transactions, &transaction_id, requested);
                };
                if active.id != transaction_id {
                    transactions.active = Some(active);
                    return Err(remote_transaction_state_error(
                        "remote transaction capability does not match the active transaction",
                    ));
                }
                let result = match requested {
                    RemoteTransactionOutcome::Committed => active
                        .transaction
                        .commit()
                        .await
                        .map(|()| RemoteTransactionOutcome::Committed),
                    RemoteTransactionOutcome::RolledBack => active
                        .transaction
                        .rollback()
                        .await
                        .map(|()| RemoteTransactionOutcome::RolledBack),
                };
                transactions
                    .completed
                    .push_back(CompletedRemoteTransaction {
                        id: transaction_id,
                        result: result.clone(),
                    });
                while transactions.completed.len() > MAX_COMPLETED_REMOTE_TRANSACTIONS {
                    transactions.completed.pop_front();
                }
                result.map(|_| ())
            },
            "join Lix server transaction finalization",
        )
        .await
    }

    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<lix::SwitchBranchReceipt, LixError> {
        let lix = Arc::clone(&self.record.lix);
        self.run_detached(
            async move { lix.switch_branch(options).await },
            "join Lix server branch switch",
        )
        .await
    }

    async fn observe(
        &self,
        sql: &str,
        params: &[Value],
        terminal_sender: TerminalStorageStreamSender,
    ) -> Result<ServerObserve<S>, LixError> {
        Ok(ServerObserve {
            events: AsyncMutex::new(self.record.lix.observe(sql, params)?),
            terminal_sender,
        })
    }
}

fn remote_transaction_state_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", message)
}

fn completed_transaction_error<S>(
    transactions: &RemoteTransactionRegistry<S>,
    transaction_id: &str,
) -> Option<LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    transactions
        .completed
        .iter()
        .any(|completed| completed.id == transaction_id)
        .then(|| remote_transaction_state_error("Lix transaction is closed"))
}

fn completed_transaction_result<S>(
    transactions: &RemoteTransactionRegistry<S>,
    transaction_id: &str,
    requested: RemoteTransactionOutcome,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let completed = transactions
        .completed
        .iter()
        .find(|completed| completed.id == transaction_id)
        .ok_or_else(|| remote_transaction_state_error("Lix session has no such transaction"))?;
    match &completed.result {
        Ok(actual) if *actual == requested => Ok(()),
        Ok(_) => Err(remote_transaction_state_error(
            "Lix transaction already completed with a different outcome",
        )),
        Err(error) => Err(error.clone()),
    }
}

async fn bind_idempotency_to_session_branch<S>(
    lix: &Lix<S>,
    idempotency: Option<ExecuteIdempotency>,
) -> Result<Option<ExecuteIdempotency>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    match idempotency {
        Some(idempotency) => Ok(Some(idempotency.with_branch(lix.active_branch_id().await?))),
        None => Ok(None),
    }
}

impl<S> Clone for SessionLease<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self::new(
            self.session_id.clone(),
            Arc::clone(&self.record),
            self.durable_terminal_storage_notifier.clone(),
        )
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
    server: LixServerProtocol<S>,
}

impl<S> HandlerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn lease(
        &self,
        session_id: &str,
        durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
    ) -> Result<SessionLease<S>, ApiError> {
        self.server
            .lease(session_id, durable_terminal_storage_notifier)
            .await
    }
}

struct ServerObserve<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    events: AsyncMutex<ObserveEvents<S>>,
    terminal_sender: TerminalStorageStreamSender,
}

impl<S> ServerObserve<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn next(&self) -> Result<Option<ObserveEvent>, LixError> {
        let result = self.events.lock().await.next().await;
        if let Err(error) = &result {
            self.terminal_sender.signal_if_terminal(error);
        }
        result
    }
}

fn result_response<T>(result: Result<T, ApiError>) -> Response
where
    T: IntoResponse,
{
    result.map_or_else(IntoResponse::into_response, IntoResponse::into_response)
}

fn decode_query<T>(query: Option<&str>) -> Result<T, ApiError>
where
    T: for<'de> Deserialize<'de> + Default,
{
    query.map_or_else(
        || Ok(T::default()),
        |query| {
            serde_urlencoded::from_str(query)
                .map_err(|error| ApiError::bad_request(format!("invalid query string: {error}")))
        },
    )
}

fn method_not_allowed() -> Response {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "LIX_ERROR_METHOD_NOT_ALLOWED",
        "the HTTP method is not defined for this Lix Server Protocol path",
    )
    .into_response()
}

fn not_found() -> Response {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "LIX_ERROR_PROTOCOL_PATH_NOT_FOUND",
        "the path is not part of the Lix Server Protocol",
    )
    .into_response()
}

fn is_known_protocol_path(path: &str) -> bool {
    SERVER_PROTOCOL_ENDPOINTS
        .iter()
        .any(|(_, endpoint_path)| *endpoint_path == path)
}

impl<S> LixServerProtocol<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a protocol server with the default session limits.
    ///
    /// Open `root` with [`lix::OpenLixBuilder::as_protocol_root`] so attaching
    /// a sink does not emit `lix.opened` for the internal handle. Handshake
    /// session creation remains the client bind.
    pub fn new(root: Arc<Lix<S>>) -> Self {
        Self::with_options(root, ServerProtocolOptions::default())
            .expect("default protocol server options must be valid")
    }

    /// Creates a protocol server with explicit per-repository session limits.
    pub fn with_options(
        root: Arc<Lix<S>>,
        options: ServerProtocolOptions,
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
        if options.max_request_blob_cache_bytes == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "protocol max_request_blob_cache_bytes must be greater than zero",
            ));
        }
        root.set_sync_role(crate::sync::SyncRole::Authority)?;
        let (close_result, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(ServerInner {
                root,
                options,
                registry: AsyncMutex::new(SessionRegistry {
                    lifecycle: ServerLifecycle::Open,
                    sessions: HashMap::new(),
                }),
                request_blob_budget: Arc::new(RequestBlobCacheBudget::new(
                    options.max_request_blob_cache_bytes,
                )),
                sync_push_gate: Arc::new(AsyncMutex::new(())),
                session_open_gate: Arc::new(SessionOpenGate::default()),
                close_started: Once::new(),
                close_result,
            }),
        })
    }

    /// Handles one canonical protocol request.
    ///
    /// Hosts authenticate before calling this method, strip their outer
    /// repository prefix, decompress the request body, and pass the resulting
    /// trusted principal through `context`. The protocol owns every method and
    /// path beginning at [`PROTOCOL_PATH`].
    pub fn handle(
        &self,
        request: ServerProtocolRequest,
        context: ServerProtocolContext,
    ) -> impl Future<Output = ServerProtocolResponse> + 'static {
        let server = self.clone();
        async move { Box::pin(server.dispatch(request, context)).await }
    }

    async fn dispatch(
        self,
        request: ServerProtocolRequest,
        context: ServerProtocolContext,
    ) -> ServerProtocolResponse {
        let state = HandlerState {
            server: self.clone(),
        };
        let (parts, body) = request.into_parts();
        if parts.headers.contains_key(http::header::CONTENT_ENCODING) {
            return ApiError::unsupported_media_type(
                "hosts must decode Content-Encoding before protocol dispatch",
            )
            .into_response();
        }
        let path = parts.uri.path().to_owned();
        let method = parts.method.clone();
        if matches!(path.as_str(), "/lix/v1" | "/lix/v1/") {
            if method != Method::GET {
                return method_not_allowed();
            }
            let query = match decode_query::<HandshakeRequest>(parts.uri.query()) {
                Ok(query) => query,
                Err(error) => return error.into_response(),
            };
            return result_response(
                Box::pin(handshake(state, query, parts.headers, context)).await,
            );
        }

        if path == "/lix/v1/session" {
            if method != Method::DELETE {
                return method_not_allowed();
            }
            return result_response(delete_session(state, parts.headers, context).await);
        }

        let session_id = match required_session_id(&parts.headers) {
            Ok(session_id) => session_id,
            Err(error) => return error.into_response(),
        };
        let lease = match state
            .lease(
                &session_id,
                context.durable_terminal_storage_notifier.clone(),
            )
            .await
        {
            Ok(lease) => lease,
            Err(error) => return error.into_response(),
        };
        if let Err(error) = validate_principal(&lease, &context.principal) {
            return error.into_response();
        }
        let scope = context.principal.idempotency_scope();
        let consumes_json = matches!(
            (&method, path.as_str()),
            (&Method::POST, "/lix/v1/execute")
                | (&Method::POST, "/lix/v1/execute-batch")
                | (&Method::POST, "/lix/v1/sync/push")
                | (&Method::POST, "/lix/v1/sync/blob")
                | (&Method::POST, "/lix/v1/transaction/execute")
                | (&Method::POST, "/lix/v1/branch/create")
                | (&Method::POST, "/lix/v1/branch/switch")
                | (&Method::POST, "/lix/v1/observe")
                | (&Method::POST, "/lix/v1/observe/multiplex")
        );
        if consumes_json && let Err(error) = require_json_content_type(&parts.headers) {
            return error.into_response();
        }
        if matches!(
            (&method, path.as_str()),
            (&Method::PUT, "/lix/v1/sync/chunk")
        ) && let Err(error) = require_octet_stream_content_type(&parts.headers)
        {
            return error.into_response();
        }
        let consumes_body = consumes_json
            || matches!(
                (&method, path.as_str()),
                (&Method::POST, "/lix/v1/file/upsert")
                    | (&Method::POST, "/lix/v1/file/upsert-batch")
                    | (&Method::PUT, "/lix/v1/sync/chunk")
            );
        let body = if consumes_body {
            let body_limit = if matches!(
                (&method, path.as_str()),
                (&Method::PUT, "/lix/v1/sync/chunk")
            ) {
                MAX_SYNC_CHUNK_BYTES.min(self.inner.options.max_request_body_bytes)
            } else {
                self.inner.options.max_request_body_bytes
            };
            match body.into_bytes(body_limit).await {
                Ok(body) => body,
                Err(error) => return error.into_response(),
            }
        } else {
            Bytes::new()
        };

        macro_rules! json_request {
            ($ty:ty) => {
                match serde_json::from_slice::<$ty>(&body) {
                    Ok(value) => Json(value),
                    Err(error) => {
                        return ApiError::bad_request(format!("invalid JSON request: {error}"))
                            .into_response();
                    }
                }
            };
        }

        match (&method, path.as_str()) {
            (&Method::POST, "/lix/v1/execute") => result_response(
                execute(lease, scope, parts.headers, json_request!(ExecuteRequest)).await,
            ),
            (&Method::POST, "/lix/v1/execute-batch") => result_response(
                execute_batch(
                    lease,
                    scope,
                    parts.headers,
                    json_request!(ExecuteBatchRequest),
                )
                .await,
            ),
            (&Method::POST, "/lix/v1/sync/push") => result_response(
                sync_push(self.clone(), lease, json_request!(SyncPushRequest)).await,
            ),
            (&Method::GET, "/lix/v1/sync/pull") => {
                let query = match decode_query::<SyncPullRequest>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(sync_pull(lease, query).await)
            }
            (&Method::GET, "/lix/v1/sync/history") => {
                let query = match decode_query::<SyncHistoryQuery>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(sync_history(lease, query).await)
            }
            (&Method::GET, "/lix/v1/sync/blob") => {
                let query = match decode_query::<SyncBlobQuery>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(sync_get_blob(lease, query).await)
            }
            (&Method::POST, "/lix/v1/sync/blob") => {
                if parts.uri.query().is_some() {
                    return ApiError::bad_request(
                        "sync blob registration does not accept query parameters",
                    )
                    .into_response();
                }
                result_response(sync_register_blob(lease, json_request!(SyncBlobManifest)).await)
            }
            (&Method::GET, "/lix/v1/sync/chunk") => {
                let query = match decode_query::<SyncChunkQuery>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(sync_get_chunk(lease, query).await)
            }
            (&Method::PUT, "/lix/v1/sync/chunk") => {
                let query = match decode_query::<SyncChunkQuery>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(sync_put_chunk(lease, query, body).await)
            }
            (&Method::POST, "/lix/v1/transaction/begin") => {
                result_response(begin_transaction(lease).await)
            }
            (&Method::POST, "/lix/v1/transaction/execute") => result_response(
                transaction_execute(lease, parts.headers, json_request!(ExecuteRequest)).await,
            ),
            (&Method::POST, "/lix/v1/transaction/commit") => {
                result_response(commit_transaction(lease, parts.headers).await)
            }
            (&Method::POST, "/lix/v1/transaction/rollback") => {
                result_response(rollback_transaction(lease, parts.headers).await)
            }
            (&Method::GET, "/lix/v1/file") => {
                let query = match decode_query::<BinaryFileReadRequest>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(read_file_content(lease, query, parts.headers).await)
            }
            (&Method::POST, "/lix/v1/file/upsert") => {
                let query = match decode_query::<BinaryFileUpdateRequest>(parts.uri.query()) {
                    Ok(query) => query,
                    Err(error) => return error.into_response(),
                };
                result_response(upsert_file_content(lease, query, parts.headers, body).await)
            }
            (&Method::POST, "/lix/v1/file/upsert-batch") => {
                result_response(upsert_file_content_batch(lease, body).await)
            }
            (&Method::POST, "/lix/v1/branch/create") => {
                result_response(create_branch(lease, json_request!(CreateBranchRequest)).await)
            }
            (&Method::POST, "/lix/v1/checkpoint/create") => {
                result_response(create_checkpoint(lease).await)
            }
            (&Method::POST, "/lix/v1/undo") => result_response(undo(lease).await),
            (&Method::POST, "/lix/v1/redo") => result_response(redo(lease).await),
            (&Method::POST, "/lix/v1/branch/switch") => {
                result_response(switch_branch(lease, json_request!(SwitchBranchRequest)).await)
            }
            (&Method::POST, "/lix/v1/observe") => {
                result_response(observe(lease, json_request!(ObserveRequest)).await)
            }
            (&Method::POST, "/lix/v1/observe/multiplex") => result_response(
                observe_multiplex(lease, json_request!(MultiplexObserveRequest)).await,
            ),
            (_, known) if is_known_protocol_path(known) => method_not_allowed(),
            _ => not_found(),
        }
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

    /// Closes every child session and finally the root repository session.
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
        initial_active_account_id: Option<String>,
        principal: Option<ServerProtocolPrincipal>,
        durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
    ) -> Result<SessionLease<S>, ApiError> {
        let pending_open = self.reserve_session_open()?;

        let active_branch_id = match initial_active_branch_id {
            Some(active_branch_id) => active_branch_id,
            None => match self.inner.root.active_branch_id().await {
                Ok(active_branch_id) => active_branch_id,
                Err(error) => {
                    if let Some(notifier) = &durable_terminal_storage_notifier {
                        notifier.signal_if_terminal(&error);
                    }
                    return Err(error.into());
                }
            },
        };
        // Validate and open the pinned child before evicting any idle session.
        // An invalid requested branch therefore cannot consume capacity or
        // evict another client.
        let active_account_id =
            initial_active_account_id.unwrap_or_else(|| lix::ANONYMOUS_ACCOUNT_ID.to_string());
        let child = match self
            .inner
            .root
            .open_internal_session(active_branch_id, active_account_id)
            .await
        {
            Ok(child) => child,
            Err(error) => {
                if let Some(notifier) = &durable_terminal_storage_notifier {
                    notifier.signal_if_terminal(&error);
                }
                return Err(error.into());
            }
        };

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
                .filter(|(_, record)| record.is_idle())
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
        let record = Arc::new(SessionRecord::new(
            child,
            principal.unwrap_or(ServerProtocolPrincipal::Anonymous),
            now,
            self.inner.options.max_request_body_bytes,
            Arc::clone(&self.inner.request_blob_budget),
        ));
        registry
            .sessions
            .insert(session_id.clone(), Arc::clone(&record));
        let lease = SessionLease::new(session_id, record, durable_terminal_storage_notifier);
        drop(registry);
        for record in removed_sessions {
            close_removed_session(record).await;
        }
        pending_open.commit();
        lease.record.lix.bind_session();
        Ok(lease)
    }

    fn reserve_session_open(&self) -> Result<PendingSessionOpen, ApiError> {
        self.inner
            .session_open_gate
            .reserve(self.inner.options.max_sessions)
    }

    async fn lease(
        &self,
        session_id: &str,
        durable_terminal_storage_notifier: Option<DurableTerminalStorageNotifier>,
    ) -> Result<SessionLease<S>, ApiError> {
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
        Ok(SessionLease::new(
            session_id.to_string(),
            record,
            durable_terminal_storage_notifier,
        ))
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
    let rollback_result = match record.transactions.lock().await.active.take() {
        Some(active) => active.transaction.rollback().await,
        None => Ok(()),
    };
    let close_result = record.lix.close().await;
    rollback_result.and(close_result)
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
    generate_capability_id().map_err(ApiError::from)
}

fn generate_capability_id() -> Result<String, LixError> {
    let mut bytes = [0_u8; SESSION_TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("generate Lix protocol session identifier: {error}"),
        )
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

fn required_transaction_id(headers: &HeaderMap) -> Result<String, ApiError> {
    let mut values = headers.get_all(TRANSACTION_ID_HEADER).iter();
    let value = values.next().ok_or_else(|| {
        ApiError::from(remote_transaction_state_error(
            "Lix-Transaction-Id is required",
        ))
    })?;
    if values.next().is_some() {
        return Err(ApiError::from(remote_transaction_state_error(
            "Lix-Transaction-Id must be sent exactly once",
        )));
    }
    let value = value.to_str().map_err(|_| {
        ApiError::from(remote_transaction_state_error(
            "Lix-Transaction-Id must contain visible ASCII",
        ))
    })?;
    if value.len() != SESSION_TOKEN_HEX_LEN
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApiError::from(remote_transaction_state_error(
            "Lix-Transaction-Id has an invalid format",
        )));
    }
    Ok(value.to_owned())
}

async fn handshake<S>(
    state: HandlerState<S>,
    request: HandshakeRequest,
    headers: HeaderMap,
    context: ServerProtocolContext,
) -> Result<impl IntoResponse, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let durable_terminal_storage_notifier = context.durable_terminal_storage_notifier.clone();
    let lease = match optional_session_id(&headers)? {
        Some(session_id) => {
            if request.active_branch_id.is_some() {
                return Err(ApiError::bad_request(
                    "activeBranchId is only allowed when creating a session",
                ));
            }
            let lease = state
                .lease(&session_id, durable_terminal_storage_notifier.clone())
                .await?;
            validate_principal(&lease, &context.principal)?;
            lease
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
            let active_account_id = context.principal.account_id().to_owned();
            if matches!(
                context.principal,
                ServerProtocolPrincipal::Authenticated { .. }
            ) {
                Box::pin(state.server.inner.root.ensure_account(
                    &active_account_id,
                    &active_account_id,
                    "human",
                ))
                .await?;
            }
            state
                .server
                .create_session(
                    active_branch_id,
                    Some(active_account_id),
                    Some(context.principal),
                    durable_terminal_storage_notifier,
                )
                .await?
        }
    };
    let active_branch_id = lease
        .run_cancellable_read(|lix| async move { lix.active_branch_id().await })
        .await?;
    let active_account_id = lease
        .run_cancellable_read(|lix| async move { Ok(lix.active_account_id().to_string()) })
        .await?;
    Ok((
        [(CACHE_CONTROL, "no-store")],
        Json(HandshakeResponse {
            protocol_version: PROTOCOL_VERSION,
            active_branch_id,
            active_account_id,
            session_id: lease.session_id.clone(),
            capabilities: ProtocolCapabilities {
                binary_file_upsert: true,
                binary_file_upsert_batch: true,
                binary_file_read: true,
                sync_push: true,
                sync_pull: true,
                sync_history: true,
                sync_blob: true,
                sync_chunk: true,
            },
        }),
    ))
}

async fn delete_session<S>(
    state: HandlerState<S>,
    headers: HeaderMap,
    context: ServerProtocolContext,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session_id = required_session_id(&headers)?;
    match state.server.lease(&session_id, None).await {
        Ok(lease) => {
            validate_principal(&lease, &context.principal)?;
            drop(lease);
        }
        Err(error) if error.status == StatusCode::GONE => return Ok(StatusCode::NO_CONTENT),
        Err(error) => return Err(error),
    }
    state.server.delete_session(&session_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn validate_principal<S>(
    lease: &SessionLease<S>,
    principal: &ServerProtocolPrincipal,
) -> Result<(), ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if lease.record.principal == *principal {
        Ok(())
    } else {
        Err(ApiError::account_mismatch())
    }
}

async fn execute<S>(
    lease: SessionLease<S>,
    scope: Option<String>,
    headers: HeaderMap,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let reconstructed_bytes_limit = lease.record.max_reconstructed_request_blob_bytes;
    let mut reconstructed_bytes_remaining = reconstructed_bytes_limit;
    let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let mut cache_candidates = Vec::new();
    let decoded = decode_request_params(
        request.params,
        None,
        request.cache_blobs,
        reconstructed_bytes_limit,
        &mut reconstructed_bytes_remaining,
        &mut cache_candidate_bytes_remaining,
        &mut cache_candidates,
        |sha256| lease.record.request_blob(sha256),
    )?;
    let options: ExecuteOptions = request.options.into();
    let params = decoded.values;
    let metadata = decoded.metadata;
    let idempotency = execute_idempotency(
        &headers,
        scope,
        &sql,
        &params,
        options.origin_key.as_deref(),
    )?;
    let result = lease
        .execute(sql, params, options, metadata, idempotency)
        .await?;
    lease.record.cache_request_blobs(cache_candidates);
    Ok(Json(ExecuteResponse::try_from(result)?))
}

async fn execute_batch<S>(
    lease: SessionLease<S>,
    scope: Option<String>,
    headers: HeaderMap,
    Json(request): Json<ExecuteBatchRequest>,
) -> Result<Json<Vec<ExecuteResponse>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.statements.is_empty() {
        return Err(ApiError::bad_request("statements must not be empty"));
    }
    let mut cache_candidates = Vec::new();
    let reconstructed_bytes_limit = lease.record.max_reconstructed_request_blob_bytes;
    let mut reconstructed_bytes_remaining = reconstructed_bytes_limit;
    let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
    let decoded_statements = request
        .statements
        .into_iter()
        .enumerate()
        .map(|(index, statement)| {
            let decoded = decode_request_params(
                statement.params,
                Some(index),
                request.cache_blobs,
                reconstructed_bytes_limit,
                &mut reconstructed_bytes_remaining,
                &mut cache_candidate_bytes_remaining,
                &mut cache_candidates,
                |sha256| lease.record.request_blob(sha256),
            )?;
            Ok((
                ExecuteBatchStatement {
                    sql: required_non_empty(statement.sql, "statements[].sql")?,
                    params: decoded.values,
                    label: statement.label,
                },
                decoded.metadata,
            ))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let (statements, statement_metadata): (Vec<_>, Vec<_>) = decoded_statements.into_iter().unzip();
    let options: ExecuteOptions = request.options.into();
    let idempotency =
        execute_batch_idempotency(&headers, scope, &statements, options.origin_key.as_deref())?;
    let results = lease
        .execute_batch(statements, options, statement_metadata, idempotency)
        .await?;
    lease.record.cache_request_blobs(cache_candidates);
    Ok(Json(
        results
            .into_iter()
            .map(ExecuteResponse::try_from)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

async fn sync_push<S>(
    server: LixServerProtocol<S>,
    lease: SessionLease<S>,
    Json(request): Json<SyncPushRequest>,
) -> Result<Json<SyncPushResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.commits.is_empty() && request.ref_updates.is_empty() {
        return Err(ApiError::bad_request(
            "sync push requires at least one commit or ref update",
        ));
    }
    if request
        .commits
        .len()
        .saturating_add(request.ref_updates.len())
        > MAX_SYNC_REQUEST_ITEMS
    {
        return Err(ApiError::bad_request(format!(
            "sync push accepts at most {MAX_SYNC_REQUEST_ITEMS} total commits and ref updates",
        )));
    }
    let active_account_id = lease
        .run_cancellable_read(|lix| async move { Ok(lix.active_account_id().to_string()) })
        .await?;
    if request
        .commits
        .iter()
        .any(|commit| commit.account_id != active_account_id)
    {
        return Err(ApiError::account_mismatch());
    }
    ensure_sync_push_event_fits(&request, MAX_SYNC_PULL_RESPONSE_BYTES)?;
    let push_gate = Arc::clone(&server.inner.sync_push_gate);
    let response = lease
        .run_durable(move |lix| {
            let push_gate = Arc::clone(&push_gate);
            async move {
                // Cancellation cannot release repository ordering while the
                // immutable objects, refs, and event cursor are committing.
                let _guard = push_gate.lock().await;
                lix.push_sync_repository(&request).await
            }
        })
        .await?;
    Ok(Json(response))
}

fn ensure_sync_push_event_fits(
    request: &SyncPushRequest,
    max_bytes: usize,
) -> Result<(), ApiError> {
    let commits = request.commits.iter().collect::<Vec<_>>();
    let encoded_len =
        crate::sync::encoded_delta_event_len(u64::MAX, &commits, &request.ref_updates)?;
    if encoded_len > max_bytes {
        return Err(ApiError::sync_push_event_too_large(max_bytes));
    }
    Ok(())
}

async fn sync_pull<S>(
    lease: SessionLease<S>,
    request: SyncPullRequest,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.limit == 0 || request.limit > MAX_SYNC_REQUEST_ITEMS {
        return Err(ApiError::bad_request(format!(
            "sync pull limit must be between 1 and {MAX_SYNC_REQUEST_ITEMS}",
        )));
    }
    let snapshot_page_requested = request.snapshot_branch_id.is_some()
        || request.snapshot_head_commit_id.is_some()
        || request.snapshot_after.is_some();
    if snapshot_page_requested {
        if request.after.is_some()
            || request
                .snapshot_branch_id
                .as_deref()
                .is_none_or(str::is_empty)
            || request
                .snapshot_head_commit_id
                .as_deref()
                .is_none_or(str::is_empty)
            || request
                .snapshot_after
                .as_ref()
                .is_some_and(|continuation| continuation.is_empty() || continuation.len() > 4096)
        {
            return Err(ApiError::bad_request(
                "snapshot row paging requires snapshotBranchId and snapshotHeadCommitId, an optional bounded snapshotAfter, and no after cursor",
            ));
        }
        let branch_id = request
            .snapshot_branch_id
            .expect("validated snapshot branch id");
        let head_commit_id = request
            .snapshot_head_commit_id
            .expect("validated snapshot head commit id");
        let continuation = request.snapshot_after;
        let limit = request.limit;
        let response = lease
            .run_cancellable_read(move |lix| async move {
                lix.pull_sync_snapshot_rows(
                    &branch_id,
                    &head_commit_id,
                    continuation.as_deref(),
                    limit,
                )
                .await
            })
            .await?;
        return bounded_sync_json_response(
            response,
            "sync snapshot rows",
            MAX_SYNC_PULL_RESPONSE_BYTES,
        );
    }
    let mut change_watcher = lease.record.lix.sync_mode_state().change_watcher();
    // A cursor-neutral wake asks us to re-read; it must not buy another full
    // timeout. Otherwise ordinary repository activity can keep one HTTP
    // request alive past the client's deadline and force needless reconnects.
    let long_poll_deadline = tokio::time::Instant::now() + SYNC_LONG_POLL_TIMEOUT;
    let response = loop {
        // Subscribe before reading the cursor, closing the change-between-read-
        // and-wait race without a periodic poll.
        let after = request.after;
        let limit = request.limit;
        let response = lease
            .run_cancellable_read(
                move |lix| async move { lix.pull_sync_repository(after, limit).await },
            )
            .await?;
        let at_head = matches!(
            &response,
            SyncRepositoryPullResponse::Delta { cursor, events }
                if events.is_empty() && request.after.is_some_and(|after| *cursor <= after)
        );
        // Omitting `after` is a finite hot-state snapshot. A delta request is
        // the mandatory long-poll form.
        if request.after.is_none() || !at_head {
            break response;
        }
        tokio::select! {
            result = change_watcher.changed() => {
                if result.is_ok() {
                    continue;
                }
                break response;
            },
            _ = tokio::time::sleep_until(long_poll_deadline) => break response,
        }
    };
    bounded_sync_json_response(response, "sync pull", MAX_SYNC_PULL_RESPONSE_BYTES)
}

async fn sync_history<S>(
    lease: SessionLease<S>,
    request: SyncHistoryQuery,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let response = lease
        .run_cancellable_read(move |lix| async move {
            lix.sync_history(&request.head, request.limit).await
        })
        .await?;
    bounded_sync_json_response(response, "sync history", MAX_SYNC_PULL_RESPONSE_BYTES)
}

fn bounded_sync_json_response<T>(
    value: T,
    operation: &'static str,
    max_bytes: usize,
) -> Result<Response, ApiError>
where
    T: Serialize,
{
    let encoded = serde_json::to_vec(&value).map_err(|error| {
        ApiError::from(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode {operation} response: {error}"),
        ))
    })?;
    if encoded.len() > max_bytes {
        return Err(ApiError::sync_response_too_large(operation, max_bytes));
    }
    Ok(HttpResponse::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(ServerProtocolBody::full(encoded))
        .expect("static sync response headers are valid"))
}

async fn sync_get_blob<S>(
    lease: SessionLease<S>,
    request: SyncBlobQuery,
) -> Result<Json<SyncBlobManifest>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let blob_id = required_sync_cas_id(request.blob_id, "blobId")?;
    let requested_blob_id = blob_id.clone();
    let manifest = lease
        .run_cancellable_read(move |lix| async move {
            lix.get_sync_blob_manifest(&requested_blob_id).await
        })
        .await?
        .ok_or_else(|| sync_cas_not_found("blob", &blob_id))?;
    Ok(Json(manifest))
}

async fn sync_register_blob<S>(
    lease: SessionLease<S>,
    Json(manifest): Json<SyncBlobManifest>,
) -> Result<Json<SyncBlobRegistration>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    validate_sync_blob_manifest(&manifest)?;
    let registration = lease
        .run_durable(move |lix| async move { lix.register_sync_blob_manifest(&manifest).await })
        .await?;
    Ok(Json(registration))
}

async fn sync_get_chunk<S>(
    lease: SessionLease<S>,
    request: SyncChunkQuery,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let chunk_id = required_sync_cas_id(request.chunk_id, "chunkId")?;
    let requested_chunk_id = chunk_id.clone();
    let bytes = lease
        .run_cancellable_read(
            move |lix| async move { lix.get_sync_chunk(&requested_chunk_id).await },
        )
        .await?
        .ok_or_else(|| sync_cas_not_found("chunk", &chunk_id))?;
    if bytes.is_empty() || bytes.len() > MAX_SYNC_CHUNK_BYTES {
        return Err(ApiError::from(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "stored sync chunk violates the protocol size bound",
        )));
    }
    let content_length = bytes.len();
    let mut response = Response::new(ServerProtocolBody::from(Bytes::from(bytes)));
    response.headers_mut().insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        http::HeaderValue::from_str(&content_length.to_string()).map_err(|_| {
            ApiError::from(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync chunk response length cannot be encoded",
            ))
        })?,
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        http::HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    Ok(response)
}

async fn sync_put_chunk<S>(
    lease: SessionLease<S>,
    request: SyncChunkQuery,
    bytes: Bytes,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let chunk_id = required_sync_cas_id(request.chunk_id, "chunkId")?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request(
            "sync chunks must contain at least one byte",
        ));
    }
    lease
        .run_durable(move |lix| async move { lix.put_sync_chunk(&chunk_id, &bytes).await })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn required_sync_cas_id(value: Option<String>, field: &str) -> Result<String, ApiError> {
    let value = value.ok_or_else(|| ApiError::bad_request(format!("{field} is required")))?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApiError::bad_request(format!(
            "{field} must be a 64-character lowercase BLAKE3 hex digest",
        )));
    }
    Ok(value)
}

fn validate_sync_blob_manifest(manifest: &SyncBlobManifest) -> Result<(), ApiError> {
    required_sync_cas_id(Some(manifest.blob_id.clone()), "blobId")?;
    if manifest.chunks.len() > MAX_SYNC_BLOB_MANIFEST_CHUNKS {
        return Err(ApiError::bad_request(format!(
            "sync blob manifests accept at most {MAX_SYNC_BLOB_MANIFEST_CHUNKS} chunks",
        )));
    }
    let mut declared_size = 0_u64;
    for chunk in &manifest.chunks {
        required_sync_cas_id(Some(chunk.chunk_id.clone()), "chunks[].chunkId")?;
        if chunk.size_bytes == 0 || chunk.size_bytes > MAX_SYNC_CHUNK_BYTES as u64 {
            return Err(ApiError::bad_request(format!(
                "sync blob chunk sizes must be between 1 and {MAX_SYNC_CHUNK_BYTES} bytes",
            )));
        }
        declared_size = declared_size.checked_add(chunk.size_bytes).ok_or_else(|| {
            ApiError::bad_request("sync blob manifest size overflows an unsigned 64-bit integer")
        })?;
    }
    if declared_size != manifest.size_bytes {
        return Err(ApiError::bad_request(
            "sync blob manifest sizeBytes must equal the sum of its chunk sizes",
        ));
    }
    Ok(())
}

fn sync_cas_not_found(kind: &str, id: &str) -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "LIX_ERROR_SYNC_CAS_NOT_FOUND",
        format!("sync {kind} '{id}' does not exist"),
    )
}

async fn begin_transaction<S>(
    lease: SessionLease<S>,
) -> Result<Json<BeginTransactionResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Ok(Json(BeginTransactionResponse {
        transaction_id: lease.begin_transaction().await?,
    }))
}

async fn transaction_execute<S>(
    lease: SessionLease<S>,
    headers: HeaderMap,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let reconstructed_bytes_limit = lease.record.max_reconstructed_request_blob_bytes;
    let mut reconstructed_bytes_remaining = reconstructed_bytes_limit;
    let mut cache_candidate_bytes_remaining = 0;
    let mut cache_candidates = Vec::new();
    let decoded = decode_request_params(
        request.params,
        None,
        false,
        reconstructed_bytes_limit,
        &mut reconstructed_bytes_remaining,
        &mut cache_candidate_bytes_remaining,
        &mut cache_candidates,
        |sha256| lease.record.request_blob(sha256),
    )?;
    let result = lease
        .transaction_execute(
            required_transaction_id(&headers)?,
            sql,
            decoded.values,
            request.options.into(),
        )
        .await?;
    Ok(Json(ExecuteResponse::try_from(result)?))
}

async fn commit_transaction<S>(
    lease: SessionLease<S>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lease
        .commit_transaction(required_transaction_id(&headers)?)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn rollback_transaction<S>(
    lease: SessionLease<S>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lease
        .rollback_transaction(required_transaction_id(&headers)?)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Upserts one file from an octet-stream body.
///
/// This intentionally covers only the dominant file-transfer shape. It keeps
/// the normal execute response envelope, but avoids JSON base64 expansion and
/// decoding for the file payload itself.
async fn upsert_file_content<S>(
    lease: SessionLease<S>,
    request: BinaryFileUpdateRequest,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = required_non_empty(request.path, "path")?;
    if let Some(content_range) = parse_file_upload_content_range(&headers, body.len())? {
        let upload_id = headers
            .get(FILE_UPLOAD_ID_HEADER)
            .ok_or_else(|| ApiError::bad_request("Content-Range requires Lix-Upload-Id"))?
            .to_str()
            .map_err(|_| ApiError::bad_request("Lix-Upload-Id must be ASCII"))?
            .to_owned();
        let progress = lease
            .run_durable(move |lix| async move {
                lix.upsert_file_content_part(
                    upload_id,
                    path,
                    content_range.start,
                    content_range.total,
                    body,
                )
                .await
            })
            .await?;
        let mut response = Json(ExecuteResponse::try_from(
            ExecuteResult::from_rows_affected(u64::from(progress.finalized)),
        )?)
        .into_response();
        if !progress.finalized {
            *response.status_mut() = StatusCode::PERMANENT_REDIRECT;
            if progress.next_offset > 0 {
                response.headers_mut().insert(
                    RANGE,
                    http::HeaderValue::from_str(&format!("bytes=0-{}", progress.next_offset - 1))
                        .map_err(|_| ApiError::bad_request("upload offset cannot be encoded"))?,
                );
            }
        }
        return Ok(response);
    }
    let result = lease
        .run_durable(move |lix| async move { lix.upsert_file_content(path, body).await })
        .await?;
    Ok(Json(ExecuteResponse::try_from(
        ExecuteResult::from_rows_affected(result),
    )?)
    .into_response())
}

#[derive(Debug, Clone, Copy)]
struct FileUploadContentRange {
    start: u64,
    total: u64,
}

fn parse_file_upload_content_range(
    headers: &HeaderMap,
    body_len: usize,
) -> Result<Option<FileUploadContentRange>, ApiError> {
    let Some(value) = headers.get(CONTENT_RANGE) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::bad_request("Content-Range must be ASCII"))?;
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| ApiError::bad_request("Content-Range must use bytes"))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| ApiError::bad_request("invalid Content-Range"))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| ApiError::bad_request("invalid Content-Range"))?;
    let start = start
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request("invalid Content-Range start"))?;
    let end = end
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request("invalid Content-Range end"))?;
    let total = total
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request("invalid Content-Range total"))?;
    let declared_len = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| ApiError::bad_request("invalid Content-Range bounds"))?;
    if declared_len != body_len as u64 || end >= total {
        return Err(ApiError::bad_request(
            "Content-Range does not match the request body or total size",
        ));
    }
    Ok(Some(FileUploadContentRange { start, total }))
}

/// Upserts a bounded batch of files from one deterministic octet-stream frame.
///
/// The frame is `u32be entry_count`, followed by one `u32be path_length`,
/// `u32be content_length`, UTF-8 path, and raw content sequence per entry. Content
/// payloads remain `Bytes` slices through the SDK boundary; only paths need a
/// `String` allocation.
async fn upsert_file_content_batch<S>(
    lease: SessionLease<S>,
    body: Bytes,
) -> Result<Json<ExecuteResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let writes = parse_binary_file_upsert_batch(body)?;
    let result = lease
        .run_durable(move |lix| async move { lix.upsert_file_content_batch(writes).await })
        .await?;
    Ok(Json(ExecuteResponse::try_from(
        ExecuteResult::from_rows_affected(result),
    )?))
}

fn parse_binary_file_upsert_batch(body: Bytes) -> Result<Vec<(String, Blob)>, ApiError> {
    let mut offset = 0;
    let count = usize::try_from(read_binary_file_upsert_batch_u32(
        &body,
        &mut offset,
        "entry count",
    )?)
    .map_err(|_| ApiError::bad_request("batch frame entry count does not fit this platform"))?;
    if count == 0 {
        return Err(ApiError::bad_request(
            "batch frame must contain at least one file entry",
        ));
    }
    if count > MAX_BINARY_FILE_UPSERT_BATCH_ENTRIES {
        return Err(ApiError::bad_request(format!(
            "batch frame has {count} file entries; maximum is {MAX_BINARY_FILE_UPSERT_BATCH_ENTRIES}",
        )));
    }

    let mut writes = Vec::with_capacity(count);
    let mut paths = HashSet::with_capacity(count);
    for entry_index in 0..count {
        let path_length = usize::try_from(read_binary_file_upsert_batch_u32(
            &body,
            &mut offset,
            "path length",
        )?)
        .map_err(|_| {
            ApiError::bad_request(format!(
                "batch frame path length for entry {entry_index} does not fit this platform",
            ))
        })?;
        let content_length = usize::try_from(read_binary_file_upsert_batch_u32(
            &body,
            &mut offset,
            "content length",
        )?)
        .map_err(|_| {
            ApiError::bad_request(format!(
                "batch frame content length for entry {entry_index} does not fit this platform",
            ))
        })?;
        let path_range = take_binary_file_upsert_batch_range(
            &body,
            &mut offset,
            path_length,
            "path",
            entry_index,
        )?;
        let path = std::str::from_utf8(&body[path_range])
            .map_err(|_| {
                ApiError::bad_request(format!(
                    "batch frame path for entry {entry_index} must be valid UTF-8",
                ))
            })?
            .to_owned();
        // Reject duplicate entries as a malformed frame before opening an
        // engine transaction. Payload bytes remain zero-copy; only bounded
        // path metadata is cloned for this protocol-level validation.
        if !paths.insert(path.clone()) {
            return Err(ApiError::bad_request(format!(
                "batch frame contains duplicate path for entry {entry_index}",
            )));
        }
        let content_range = take_binary_file_upsert_batch_range(
            &body,
            &mut offset,
            content_length,
            "content",
            entry_index,
        )?;
        // `Bytes::slice` preserves the request allocation. Do not replace it
        // with `to_vec`: large file bodies are intentionally forwarded as
        // shared immutable Blob storage.
        writes.push((path, Blob::from(body.slice(content_range))));
    }

    if offset != body.len() {
        return Err(ApiError::bad_request(
            "batch frame contains trailing bytes after its declared entries",
        ));
    }
    Ok(writes)
}

fn read_binary_file_upsert_batch_u32(
    body: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<u32, ApiError> {
    let end = offset.checked_add(size_of::<u32>()).ok_or_else(|| {
        ApiError::bad_request(format!("batch frame offset overflow while reading {field}"))
    })?;
    let bytes: [u8; size_of::<u32>()] = body
        .get(*offset..end)
        .ok_or_else(|| {
            ApiError::bad_request(format!("batch frame is truncated while reading {field}"))
        })?
        .try_into()
        .map_err(|_| {
            ApiError::bad_request(format!("batch frame is truncated while reading {field}"))
        })?;
    *offset = end;
    Ok(u32::from_be_bytes(bytes))
}

fn take_binary_file_upsert_batch_range(
    body: &Bytes,
    offset: &mut usize,
    length: usize,
    field: &str,
    entry_index: usize,
) -> Result<std::ops::Range<usize>, ApiError> {
    let start = *offset;
    let end = start.checked_add(length).ok_or_else(|| {
        ApiError::bad_request(format!(
            "batch frame {field} length for entry {entry_index} overflows its offset",
        ))
    })?;
    if end > body.len() {
        return Err(ApiError::bad_request(format!(
            "batch frame is truncated while reading {field} for entry {entry_index}",
        )));
    }
    *offset = end;
    Ok(start..end)
}

/// Reads one file as a raw octet stream.
///
/// `Lix-File-Found: true` distinguishes a present empty file from a missing
/// file (`false`), whose body is also empty. The response is explicitly
/// non-cacheable because the session's rendered plugin view is part of the
/// collaboration protocol.
async fn read_file_content<S>(
    lease: SessionLease<S>,
    request: BinaryFileReadRequest,
    headers: HeaderMap,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = required_non_empty(request.path, "path")?;
    let requested_range = parse_single_file_range(&headers)?;
    let partial = requested_range.is_some();
    let first_requested_range = requested_range
        .as_ref()
        .map(|range| {
            range.start
                ..range
                    .end
                    .min(range.start.saturating_add(FILE_READ_STREAM_WINDOW_BYTES))
        })
        .unwrap_or(0..FILE_READ_STREAM_WINDOW_BYTES);
    let first_path = path.clone();
    let data = lease
        .run_cancellable_read(move |lix| async move {
            lix.read_file_content(first_path, Some(first_requested_range))
                .await
        })
        .await?;
    let (found, body, file_range) = match data {
        None => ("false", ServerProtocolBody::empty(), None),
        Some(read) => {
            let first_range = read.range();
            let total_size = read.total_size();
            let content_identity = read.content_identity().to_owned();
            if partial && total_size == 0 {
                return Err(ApiError::bad_request("file read range is not satisfiable"));
            }
            let response_range = requested_range
                .as_ref()
                .map(|range| range.start..range.end.min(total_size))
                .unwrap_or(0..total_size);
            let next_offset = first_range.end;
            let end_offset = response_range.end;
            let first = read.into_content().into_bytes();
            let body_stream = async_stream::stream! {
                yield Ok::<Bytes, io::Error>(first);
                let mut next_offset = next_offset;
                while next_offset < end_offset {
                    let next_end = end_offset.min(
                        next_offset.saturating_add(FILE_READ_STREAM_WINDOW_BYTES),
                    );
                    let read_path = path.clone();
                    let read = match lease
                        .run_cancellable_read(move |lix| async move {
                            lix.read_file_content(read_path, Some(next_offset..next_end))
                                .await
                        })
                        .await {
                            Ok(Some(read)) => read,
                            Ok(None) => {
                                yield Err(io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "file disappeared while its response was streaming",
                                ));
                                break;
                            }
                            Err(error) => {
                                yield Err(file_stream_error(error));
                                break;
                            }
                        };
                    let actual_range = read.range();
                    if read.content_identity() != content_identity {
                        yield Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "file changed while its response was streaming",
                        ));
                        break;
                    }
                    next_offset = actual_range.end;
                    yield Ok(read.into_content().into_bytes());
                }
            };
            let body = ServerProtocolBody::stream(body_stream);
            ("true", body, Some((response_range, total_size)))
        }
    };
    let mut response = Response::new(body);
    if partial && file_range.is_some() {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    }
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, http::HeaderValue::from_static("no-store"));
    headers.insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        HeaderName::from_static(FILE_FOUND_HEADER),
        http::HeaderValue::from_static(found),
    );
    headers.insert(ACCEPT_RANGES, http::HeaderValue::from_static("bytes"));
    if let Some((range, total_size)) = file_range {
        let body_length = range.end - range.start;
        headers.insert(
            CONTENT_LENGTH,
            http::HeaderValue::from_str(&body_length.to_string())
                .map_err(|_| ApiError::bad_request("file range length is invalid"))?,
        );
        if partial {
            let value = format!(
                "bytes {}-{}/{}",
                range.start,
                range.end.saturating_sub(1),
                total_size
            );
            headers.insert(
                CONTENT_RANGE,
                http::HeaderValue::from_str(&value)
                    .map_err(|_| ApiError::bad_request("file content range is invalid"))?,
            );
        }
    }
    Ok(response)
}

fn file_stream_error(error: LixError) -> io::Error {
    io::Error::other(error.to_string())
}

/// Parses the common single, forward byte range. Multipart and suffix ranges
/// are deliberately outside the media happy path and would force a more
/// complicated response shape.
fn parse_single_file_range(headers: &HeaderMap) -> Result<Option<std::ops::Range<u64>>, ApiError> {
    let Some(value) = headers.get(RANGE) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::bad_request("Range must be valid ASCII"))?;
    let value = value
        .strip_prefix("bytes=")
        .ok_or_else(|| ApiError::bad_request("only byte ranges are supported"))?;
    if value.contains(',') {
        return Err(ApiError::bad_request(
            "multiple file ranges are not supported",
        ));
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| ApiError::bad_request("Range must use start-end form"))?;
    if start.is_empty() {
        return Err(ApiError::bad_request(
            "suffix file ranges are not supported",
        ));
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request("Range start must be an unsigned integer"))?;
    let end_exclusive = if end.is_empty() {
        u64::MAX
    } else {
        end.parse::<u64>()
            .map_err(|_| ApiError::bad_request("Range end must be an unsigned integer"))?
            .checked_add(1)
            .ok_or_else(|| ApiError::bad_request("Range end is too large"))?
    };
    if start >= end_exclusive {
        return Err(ApiError::bad_request("Range start must not exceed its end"));
    }
    Ok(Some(start..end_exclusive))
}

async fn create_branch<S>(
    lease: SessionLease<S>,
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
        .run_durable(move |lix| async move { lix.create_branch(options).await })
        .await?;
    Ok(Json(CreateBranchResponse {
        id: receipt.id,
        name: receipt.name,
        hidden: receipt.hidden,
        commit_id: receipt.commit_id,
    }))
}

async fn create_checkpoint<S>(
    lease: SessionLease<S>,
) -> Result<Json<CreateCheckpointResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = lease
        .run_durable(move |lix| async move { lix.create_checkpoint().await })
        .await?;
    Ok(Json(CreateCheckpointResponse {
        commit_id: receipt.commit_id,
    }))
}

async fn undo<S>(lease: SessionLease<S>) -> Result<Json<UndoResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = lease
        .run_durable(move |lix| async move { lix.undo().await })
        .await?;
    Ok(Json(UndoResponse {
        branch_id: receipt.branch_id,
        target_commit_id: receipt.target_commit_id,
        inverse_commit_id: receipt.inverse_commit_id,
    }))
}

async fn redo<S>(lease: SessionLease<S>) -> Result<Json<RedoResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = lease
        .run_durable(move |lix| async move { lix.redo().await })
        .await?;
    Ok(Json(RedoResponse {
        branch_id: receipt.branch_id,
        target_commit_id: receipt.target_commit_id,
        replay_commit_id: receipt.replay_commit_id,
    }))
}

async fn switch_branch<S>(
    lease: SessionLease<S>,
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
    lease: SessionLease<S>,
    Json(request): Json<ObserveRequest>,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let params = decode_params(request.params)?;
    let (terminal_sender, terminal_signal) = TerminalStorageStreamSignal::new();
    let events = lease
        .observe(&sql, &params, terminal_sender.clone())
        .await?;
    let stream = async_stream::stream! {
        let _lease = lease;
        let terminal_sender = terminal_sender;
        loop {
            match events.next().await {
                Ok(Some(event)) => match ObserveEventResponse::try_from(event) {
                    Ok(payload) => yield Ok::<Event, Infallible>(sse_json_event("next", &payload)),
                    Err(error) => {
                        terminal_sender.signal_if_terminal(&error);
                        yield Ok::<Event, Infallible>(sse_json_event("error", &ErrorEnvelope::from_lix_error(&error)));
                        break;
                    }
                },
                Ok(None) => break,
                Err(error) => {
                    terminal_sender.signal_if_terminal(&error);
                    yield Ok::<Event, Infallible>(sse_json_event("error", &ErrorEnvelope::from_lix_error(&error)));
                    break;
                }
            }
        }
    };
    let mut response = sse_response(stream);
    response.extensions_mut().insert(terminal_signal);
    Ok(response)
}

async fn observe_multiplex<S>(
    lease: SessionLease<S>,
    Json(request): Json<MultiplexObserveRequest>,
) -> Result<Response, ApiError>
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
    let groups = group_multiplex_subscriptions(request.subscriptions)?;
    let (sender, mut receiver) = mpsc::channel::<MultiplexObserveMessage>(64);
    let (terminal_sender, terminal_signal) = TerminalStorageStreamSignal::new();
    // Own each group task while opening the remaining observations. Dropping a
    // bare JoinHandle detaches it, which could otherwise leave an abandoned
    // observation (and its terminal-storage sender) alive if a later open
    // fails or the response body is never polled.
    let mut task_guard = Some(ObserveTaskGuard(Vec::with_capacity(groups.len())));
    for group in groups {
        let events = lease
            .observe(&group.sql, &group.params, terminal_sender.clone())
            .await?;
        let sender = sender.clone();
        let terminal_sender = terminal_sender.clone();
        let parent = tracing::Span::current();
        task_guard
            .as_mut()
            .expect("multiplex task guard exists before creating the stream")
            .0
            .push(tokio::spawn(
                async move {
                    let mut delta_base = None;
                    'events: loop {
                        let messages = match events.next().await {
                            Ok(Some(event)) => {
                                match multiplex_observe_payload(event, delta_base.as_ref()) {
                                    Ok((payload, next_delta_base)) => {
                                        delta_base = next_delta_base;
                                        let payload = Arc::new(payload);
                                        group
                                            .subscription_ids
                                            .iter()
                                            .map(|subscription_id| MultiplexObserveMessage::Next {
                                                subscription_id: subscription_id.clone(),
                                                payload: Arc::clone(&payload),
                                            })
                                            .collect::<Vec<_>>()
                                    }
                                    Err(error) => {
                                        terminal_sender.signal_if_terminal(&error);
                                        let error = ErrorEnvelope::from_lix_error(&error);
                                        group
                                            .subscription_ids
                                            .iter()
                                            .map(|subscription_id| MultiplexObserveMessage::Error {
                                                subscription_id: subscription_id.clone(),
                                                error: error.clone(),
                                            })
                                            .collect()
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                terminal_sender.signal_if_terminal(&error);
                                let error = ErrorEnvelope::from_lix_error(&error);
                                group
                                    .subscription_ids
                                    .iter()
                                    .map(|subscription_id| MultiplexObserveMessage::Error {
                                        subscription_id: subscription_id.clone(),
                                        error: error.clone(),
                                    })
                                    .collect()
                            }
                        };
                        let terminal = messages.first().is_some_and(|message| {
                            matches!(message, MultiplexObserveMessage::Error { .. })
                        });
                        for message in messages {
                            if sender.send(message).await.is_err() {
                                break 'events;
                            }
                        }
                        if terminal {
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
        let _terminal_sender = terminal_sender;
        let mut task_guard = task_guard;
        while let Some(message) = receiver.recv().await {
            match message {
                MultiplexObserveMessage::Next { subscription_id, payload } => {
                    yield Ok::<Event, Infallible>(sse_json_event("next", &MultiplexObserveEventResponse {
                        subscription_id: &subscription_id,
                        payload: payload.as_ref(),
                    }));
                }
                MultiplexObserveMessage::Error { subscription_id, error } => {
                    let terminal_storage = error.is_terminal_storage_error();
                    // Abort live siblings before yielding. A yield suspends
                    // this stream until the client asks for another frame;
                    // waiting until after it would retain sibling reads when
                    // a client stops after the terminal error.
                    if terminal_storage {
                        drop(task_guard.take());
                    }
                    yield Ok::<Event, Infallible>(sse_json_event("error", &MultiplexObserveErrorResponse {
                        subscription_id,
                        error,
                    }));
                    if terminal_storage {
                        break;
                    }
                }
            }
        }
    };
    let mut response = sse_response(stream);
    response.extensions_mut().insert(terminal_signal);
    Ok(response)
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

fn sse_response(
    stream: impl Stream<Item = Result<Event, Infallible>> + Send + 'static,
) -> Response {
    let stream = async_stream::stream! {
        futures_util::pin_mut!(stream);
        let mut keep_alive = tokio::time::interval(Duration::from_secs(15));
        keep_alive.tick().await;
        loop {
            tokio::select! {
                event = futures_util::StreamExt::next(&mut stream) => match event {
                    Some(Ok(event)) => yield Ok::<Bytes, io::Error>(event.encode()),
                    Some(Err(never)) => match never {},
                    None => break,
                },
                _ = keep_alive.tick() => {
                    yield Ok(Bytes::from_static(b": keep-alive\n\n"));
                }
            }
        }
    };
    HttpResponse::builder()
        .header(CONTENT_TYPE, "text/event-stream")
        .header(CACHE_CONTROL, "no-cache")
        .body(ServerProtocolBody::stream(stream))
        .expect("static SSE response is valid")
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
    metadata: ExecuteStatementMetadata,
}

fn decode_request_params(
    params: Vec<RequestWireValue>,
    statement_index: Option<usize>,
    cache_full_blobs: bool,
    reconstructed_bytes_limit: usize,
    reconstructed_bytes_remaining: &mut usize,
    cache_candidate_bytes_remaining: &mut usize,
    cache_candidates: &mut Vec<CachedRequestBlob>,
    lookup_blob: impl Fn(&str) -> Option<VerifiedRequestBlob>,
) -> Result<DecodedRequestParams, ApiError> {
    let mut values = Vec::with_capacity(params.len());
    let mut parameter_blob_splices = Vec::with_capacity(params.len());
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
                        VerifiedRequestBlob::verify(bytes.clone()),
                        cache_candidate_bytes_remaining,
                        cache_candidates,
                    );
                }
                values.push(value);
                parameter_blob_splices.push(None);
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
                if prefix > base.blob().len()
                    || suffix > base.blob().len()
                    || prefix.saturating_add(suffix) > base.blob().len()
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
                            "aggregate reconstructed blobs exceed the {reconstructed_bytes_limit}-byte request limit"
                        ),
                    ));
                }
                *reconstructed_bytes_remaining -= reconstructed_len;
                let (reconstructed, provenance) = base
                    .reconstruct_splice(&base_sha256, &result_sha256, prefix, suffix, insert)
                    .map_err(|error| {
                        invalid_parameter_error(
                            parameter_index,
                            statement_index,
                            error.code,
                            error.message,
                        )
                    })?;
                prepare_cache_candidate(
                    reconstructed.clone(),
                    cache_candidate_bytes_remaining,
                    cache_candidates,
                );
                values.push(Value::Blob(reconstructed.blob().clone()));
                parameter_blob_splices.push(Some(provenance));
            }
        }
    }
    Ok(DecodedRequestParams {
        values,
        metadata: ExecuteStatementMetadata {
            parameter_blob_splices,
            ..ExecuteStatementMetadata::default()
        },
    })
}

fn prepare_cache_candidate(
    blob: VerifiedRequestBlob,
    bytes_remaining: &mut usize,
    candidates: &mut Vec<CachedRequestBlob>,
) {
    if !is_request_blob_cacheable(blob.blob().len())
        || blob.blob().len() > *bytes_remaining
        || candidates
            .iter()
            .any(|candidate| candidate.blob.sha256() == blob.sha256())
    {
        return;
    }
    *bytes_remaining -= blob.blob().len();
    candidates.push(CachedRequestBlob { blob });
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

#[cfg(test)]
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
    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorEnvelope::from_parts(code, message, None, None),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorEnvelope::from_parts("LIX_INVALID_ARGUMENT", message, None, None),
        }
    }

    fn payload_too_large(limit: usize) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("request body exceeds the {limit}-byte protocol limit"),
        )
    }

    fn sync_response_too_large(operation: &str, limit: usize) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE",
            format!("{operation} response exceeds the {limit}-byte protocol limit"),
        )
    }

    fn sync_push_event_too_large(limit: usize) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("sync push would create an event above the {limit}-byte pull response limit"),
        )
    }

    fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "LIX_ERROR_UNSUPPORTED_CONTENT_ENCODING",
            message,
        )
    }

    fn account_mismatch() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            body: ErrorEnvelope::from_parts(
                "LIX_ERROR_PROTOCOL_ACCOUNT_MISMATCH",
                "The authenticated account does not own this Lix session.",
                None,
                None,
            ),
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
        let terminal_storage = self.body.is_terminal_storage_error();
        let mut response = (self.status, Json(self.body)).into_response();
        if terminal_storage {
            response.extensions_mut().insert(TerminalStorageResponse);
        }
        response
    }
}

/// Returns whether a protocol response reports a storage instance that cannot
/// serve another request and must be replaced.
///
/// This is in-process metadata rather than a wire header. It lets an outer
/// server retire only the terminal runtime without parsing a serialized error
/// body or treating transient storage failures as terminal.
pub fn is_terminal_storage_response(response: &Response) -> bool {
    response
        .extensions()
        .get::<TerminalStorageResponse>()
        .is_some()
}

#[derive(Clone, Debug)]
struct TerminalStorageResponse;

/// A one-way in-process signal for an SSE response that reports a terminal
/// storage error after its HTTP headers have already been sent.
///
/// The signal resolves to `true` only for terminal storage errors; it resolves
/// to `false` when the observation ends normally or its response body is
/// dropped. This deliberately avoids coupling an outer server to SSE framing.
#[derive(Clone, Debug)]
pub struct TerminalStorageStreamSignal {
    receiver: watch::Receiver<bool>,
}

#[derive(Clone, Debug)]
struct TerminalStorageStreamSender {
    sender: watch::Sender<bool>,
}

impl TerminalStorageStreamSignal {
    fn new() -> (TerminalStorageStreamSender, Self) {
        let (sender, receiver) = watch::channel(false);
        (TerminalStorageStreamSender { sender }, Self { receiver })
    }

    /// Waits for either a terminal-storage error or normal stream completion.
    pub async fn wait_for_terminal_storage(mut self) -> bool {
        wait_for_terminal_storage(&mut self.receiver).await
    }
}

impl TerminalStorageStreamSender {
    fn signal_if_terminal(&self, error: &LixError) {
        if is_terminal_storage_error_code(&error.code) {
            self.sender.send_replace(true);
        }
    }
}

/// Request-scoped sender that preserves a terminal storage result after its
/// HTTP future is cancelled.
#[derive(Clone, Debug)]
pub struct DurableTerminalStorageNotifier {
    sender: watch::Sender<bool>,
}

/// Receiver for a request's terminal storage result.
#[derive(Clone, Debug)]
pub struct DurableTerminalStorageSignal {
    receiver: watch::Receiver<bool>,
}

/// Creates the request extension and receiver used to observe a terminal
/// storage result after its HTTP request is cancelled.
pub fn durable_terminal_storage_signal()
-> (DurableTerminalStorageNotifier, DurableTerminalStorageSignal) {
    let (sender, receiver) = watch::channel(false);
    (
        DurableTerminalStorageNotifier { sender },
        DurableTerminalStorageSignal { receiver },
    )
}

impl DurableTerminalStorageSignal {
    /// Waits for a terminal storage error or for every notifier clone to drop
    /// without one.
    pub async fn wait_for_terminal_storage(mut self) -> bool {
        wait_for_terminal_storage(&mut self.receiver).await
    }
}

impl DurableTerminalStorageNotifier {
    fn signal_if_terminal(&self, error: &LixError) {
        if is_terminal_storage_error_code(&error.code) {
            self.sender.send_replace(true);
        }
    }
}

async fn wait_for_terminal_storage(receiver: &mut watch::Receiver<bool>) -> bool {
    loop {
        if *receiver.borrow() {
            return true;
        }
        if receiver.changed().await.is_err() {
            return false;
        }
    }
}

/// Returns the terminal-storage signal attached to a successful observation
/// response, if the response streams errors after sending HTTP headers.
pub fn terminal_storage_stream_signal(response: &Response) -> Option<TerminalStorageStreamSignal> {
    response
        .extensions()
        .get::<TerminalStorageStreamSignal>()
        .cloned()
}

#[derive(Clone, Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Clone, Debug, Serialize)]
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

    fn is_terminal_storage_error(&self) -> bool {
        is_terminal_storage_error_code(&self.error.code)
    }
}

fn is_terminal_storage_error_code(code: &str) -> bool {
    matches!(
        code,
        LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED
    )
}

fn status_for_lix_error(error: &LixError) -> StatusCode {
    match error.code.as_str() {
        LixError::CODE_BRANCH_NOT_FOUND
        | LixError::CODE_COMMIT_NOT_FOUND
        | LixError::CODE_TABLE_NOT_FOUND
        | LixError::CODE_COLUMN_NOT_FOUND => StatusCode::NOT_FOUND,
        LixError::CODE_CLOSED | LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED => {
            StatusCode::CONFLICT
        }
        LixError::CODE_IDEMPOTENCY_KEY_REUSED | LixError::CODE_TRANSACTION_CONFLICT => {
            StatusCode::CONFLICT
        }
        LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
        | LixError::CODE_STORAGE_DURABILITY_UNAVAILABLE => StatusCode::SERVICE_UNAVAILABLE,
        LixError::CODE_IDEMPOTENCY_RESPONSE_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
        LixError::CODE_PLUGIN_OBSERVATION_STALE => StatusCode::GONE,
        LixError::CODE_INTERNAL_ERROR => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HandshakeRequest {
    active_branch_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    protocol_version: u32,
    active_branch_id: String,
    active_account_id: String,
    session_id: String,
    capabilities: ProtocolCapabilities,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BeginTransactionResponse {
    transaction_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SyncPullRequest {
    #[serde(default)]
    after: Option<u64>,
    #[serde(default = "default_sync_pull_limit")]
    limit: usize,
    #[serde(default)]
    snapshot_branch_id: Option<String>,
    #[serde(default)]
    snapshot_head_commit_id: Option<String>,
    #[serde(default)]
    snapshot_after: Option<String>,
}

impl Default for SyncPullRequest {
    fn default() -> Self {
        Self {
            after: None,
            limit: default_sync_pull_limit(),
            snapshot_branch_id: None,
            snapshot_head_commit_id: None,
            snapshot_after: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SyncHistoryQuery {
    head: String,
    limit: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SyncBlobQuery {
    blob_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SyncChunkQuery {
    chunk_id: Option<String>,
}

const fn default_sync_pull_limit() -> usize {
    128
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
// These are independently negotiated wire capabilities; grouping them would
// change the flat handshake response without reducing protocol complexity.
#[allow(clippy::struct_excessive_bools)]
struct ProtocolCapabilities {
    binary_file_upsert: bool,
    binary_file_upsert_batch: bool,
    binary_file_read: bool,
    sync_push: bool,
    sync_pull: bool,
    sync_history: bool,
    sync_blob: bool,
    sync_chunk: bool,
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
struct BinaryFileUpdateRequest {
    path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct BinaryFileReadRequest {
    path: Option<String>,
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
    #[serde(default)]
    label: Option<String>,
}

#[derive(Serialize)]
struct ExecuteFingerprint<'a> {
    sql: &'a str,
    params: &'a [Value],
    origin_key: Option<&'a str>,
    label: Option<&'a str>,
}

#[derive(Serialize)]
struct ExecuteBatchFingerprint<'a> {
    statements: Vec<ExecuteFingerprint<'a>>,
    origin_key: Option<&'a str>,
}

fn execute_idempotency(
    headers: &HeaderMap,
    scope: Option<String>,
    sql: &str,
    params: &[Value],
    origin_key: Option<&str>,
) -> Result<Option<ExecuteIdempotency>, ApiError> {
    let Some(key) = optional_idempotency_key(headers)? else {
        return Ok(None);
    };
    let fingerprint = idempotency_fingerprint(
        "execute",
        &ExecuteFingerprint {
            sql,
            params,
            origin_key,
            label: None,
        },
    )?;
    Ok(Some(ExecuteIdempotency::new(scope, key, fingerprint)))
}

fn execute_batch_idempotency(
    headers: &HeaderMap,
    scope: Option<String>,
    statements: &[ExecuteBatchStatement],
    origin_key: Option<&str>,
) -> Result<Option<ExecuteIdempotency>, ApiError> {
    let Some(key) = optional_idempotency_key(headers)? else {
        return Ok(None);
    };
    let fingerprint = idempotency_fingerprint(
        "execute-batch",
        &ExecuteBatchFingerprint {
            statements: statements
                .iter()
                .map(|statement| ExecuteFingerprint {
                    sql: &statement.sql,
                    params: &statement.params,
                    origin_key: None,
                    label: statement.label.as_deref(),
                })
                .collect(),
            origin_key,
        },
    )?;
    Ok(Some(ExecuteIdempotency::new(scope, key, fingerprint)))
}

fn optional_idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::bad_request(
            "Idempotency-Key must be sent at most once",
        ));
    }
    let value = value.to_str().map_err(|_| {
        ApiError::bad_request("Idempotency-Key must contain visible ASCII characters")
    })?;
    if value.is_empty()
        || value.len() > MAX_IDEMPOTENCY_COMPONENT_BYTES
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ApiError::bad_request(format!(
            "Idempotency-Key must contain 1 to {MAX_IDEMPOTENCY_COMPONENT_BYTES} visible ASCII characters"
        )));
    }
    Ok(Some(value.to_string()))
}

fn require_json_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .unwrap_or_default();
    if media_type.eq_ignore_ascii_case("application/json")
        || media_type.to_ascii_lowercase().ends_with("+json")
    {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "LIX_ERROR_UNSUPPORTED_CONTENT_TYPE",
            "JSON protocol requests require Content-Type application/json",
        ))
    }
}

fn require_octet_stream_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .unwrap_or_default();
    if media_type.eq_ignore_ascii_case("application/octet-stream") {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "LIX_ERROR_UNSUPPORTED_CONTENT_TYPE",
            "raw sync chunk requests require Content-Type application/octet-stream",
        ))
    }
}

fn idempotency_fingerprint(
    operation: &'static str,
    payload: &impl Serialize,
) -> Result<[u8; 32], ApiError> {
    #[derive(Serialize)]
    struct Envelope<'a, T: ?Sized> {
        version: u8,
        operation: &'static str,
        payload: &'a T,
    }

    let bytes = serde_json::to_vec(&Envelope {
        version: 1,
        operation,
        payload,
    })
    .map_err(|error| {
        ApiError::from(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("serialize idempotency request fingerprint: {error}"),
        ))
    })?;
    Ok(Sha256::digest(bytes).into())
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
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    columns: Vec<String>,
    rows: Vec<Vec<WireValue>>,
    rows_affected: u64,
    notices: Vec<lix::LixNotice>,
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
            statement_index: result.statement_index(),
            label: result.label().map(str::to_owned),
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateCheckpointResponse {
    commit_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UndoResponse {
    branch_id: String,
    target_commit_id: String,
    inverse_commit_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RedoResponse {
    branch_id: String,
    target_commit_id: String,
    replay_commit_id: String,
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

#[derive(Debug, Eq, Hash, PartialEq)]
struct MultiplexObserveGroupKey {
    sql: String,
    params_json: String,
}

struct MultiplexObserveGroup {
    subscription_ids: Vec<String>,
    sql: String,
    params: Vec<Value>,
}

fn group_multiplex_subscriptions(
    subscriptions: Vec<MultiplexObserveSubscription>,
) -> Result<Vec<MultiplexObserveGroup>, ApiError> {
    let mut group_indexes = HashMap::<MultiplexObserveGroupKey, usize>::new();
    let mut groups = Vec::<MultiplexObserveGroup>::new();
    for subscription in subscriptions {
        let subscription_id = required_non_empty(subscription.id, "subscriptions[].id")?;
        let sql = required_non_empty(subscription.sql, "subscriptions[].sql")?;
        let params_json = serde_json::to_string(&subscription.params).map_err(|error| {
            ApiError::from(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode multiplex observation parameters: {error}"),
            ))
        })?;
        let key = MultiplexObserveGroupKey {
            sql: sql.clone(),
            params_json,
        };
        if let Some(index) = group_indexes.get(&key).copied() {
            groups[index].subscription_ids.push(subscription_id);
            continue;
        }
        let index = groups.len();
        groups.push(MultiplexObserveGroup {
            subscription_ids: vec![subscription_id],
            sql,
            params: decode_params(subscription.params)?,
        });
        group_indexes.insert(key, index);
    }
    Ok(groups)
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
        payload: Arc<MultiplexObservePayload>,
    },
    Error {
        subscription_id: String,
        error: ErrorEnvelope,
    },
}

struct ObserveDeltaBase {
    sequence: u64,
    // ExecuteResult has immutable shared backing, so retaining the transport
    // base does not copy an observed result for every subscription.
    rows: ExecuteResult,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObservePayload {
    sequence: u64,
    mutation_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ExecuteResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<ObserveDelta>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ObserveDelta {
    SingleBlobSplice(SingleBlobSplice),
    RowSplice(RowSplice),
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
struct RowSplice {
    kind: &'static str,
    base_sequence: u64,
    prefix_rows: u64,
    delete_rows: u64,
    insert_rows: Vec<Vec<WireValue>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultiplexObserveEventResponse<'a> {
    subscription_id: &'a str,
    #[serde(flatten)]
    payload: &'a MultiplexObservePayload,
}

const MIN_BLOB_DELTA_BYTES: usize = 32 * 1024;
const BLOB_DELTA_COMPARE_CHUNK_BYTES: usize = 64;
// Compared with only the full blob's Base64 length, this deliberately
// overestimates every delta-only JSON/SSE field. The shared event envelope is
// omitted from both sides, so passing the 90% test guarantees >10% savings.
const BLOB_DELTA_ENVELOPE_BUDGET_BYTES: usize = 512;
// Row deltas are only useful on result sets large enough to outweigh their
// fixed sequence and splice metadata. Every wire row with one value is at
// least 24 JSON bytes, so this lower bound makes the 90% saving gate
// conservative without serializing the entire next result just to decide.
const MIN_ROW_DELTA_ROWS: usize = 16;
const MIN_FULL_ROW_WIRE_BYTES: usize = 24;
const ROW_DELTA_ENVELOPE_BUDGET_BYTES: usize = 256;

fn multiplex_observe_payload(
    event: ObserveEvent,
    base: Option<&ObserveDeltaBase>,
) -> Result<(MultiplexObservePayload, Option<ObserveDeltaBase>), LixError> {
    let next_base = ObserveDeltaBase {
        sequence: event.sequence,
        rows: event.rows.clone(),
    };
    let delta = match base {
        Some(base) if base.sequence.checked_add(1) == Some(event.sequence) => {
            if let Some(delta) = single_blob_splice(base, &next_base)? {
                Some(ObserveDelta::SingleBlobSplice(delta))
            } else {
                row_splice(base, &next_base)?.map(ObserveDelta::RowSplice)
            }
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
    Ok((payload, Some(next_base)))
}

fn point_blob_bytes(result: &ExecuteResult) -> Option<&[u8]> {
    if result.columns() != ["content"]
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
    base: &ObserveDeltaBase,
    next: &ObserveDeltaBase,
) -> Result<Option<SingleBlobSplice>, LixError> {
    let (Some(base_bytes), Some(next_bytes)) =
        (point_blob_bytes(&base.rows), point_blob_bytes(&next.rows))
    else {
        return Ok(None);
    };
    if next_bytes.len() < MIN_BLOB_DELTA_BYTES {
        return Ok(None);
    }
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

fn row_splice(
    base: &ObserveDeltaBase,
    next: &ObserveDeltaBase,
) -> Result<Option<RowSplice>, LixError> {
    if base.rows.columns() != next.rows.columns()
        || base.rows.rows_affected() != next.rows.rows_affected()
        || base.rows.notices() != next.rows.notices()
        || next.rows.columns().is_empty()
    {
        return Ok(None);
    }
    let base_rows = base.rows.rows();
    let next_rows = next.rows.rows();
    if next_rows.len() < MIN_ROW_DELTA_ROWS {
        return Ok(None);
    }
    let prefix_rows = base_rows
        .iter()
        .zip(next_rows)
        .take_while(|(base, next)| base == next)
        .count();
    if prefix_rows == base_rows.len() && prefix_rows == next_rows.len() {
        return Ok(None);
    }
    let max_suffix_rows = base_rows
        .len()
        .saturating_sub(prefix_rows)
        .min(next_rows.len().saturating_sub(prefix_rows));
    let mut suffix_rows = 0;
    while suffix_rows < max_suffix_rows {
        let base_index = base_rows.len() - suffix_rows - 1;
        let next_index = next_rows.len() - suffix_rows - 1;
        if base_rows[base_index] != next_rows[next_index] {
            break;
        }
        suffix_rows += 1;
    }
    let insert_end = next_rows.len().saturating_sub(suffix_rows);
    let insert_rows = next_rows[prefix_rows..insert_end]
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(WireValue::try_from_engine)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let insert_wire_bytes = serde_json::to_vec(&insert_rows)
        .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?
        .len();
    let Some(full_wire_lower_bound) = next_rows.len().checked_mul(MIN_FULL_ROW_WIRE_BYTES) else {
        return Ok(None);
    };
    let Some(delta_wire_estimate) = insert_wire_bytes.checked_add(ROW_DELTA_ENVELOPE_BUDGET_BYTES)
    else {
        return Ok(None);
    };
    if delta_wire_estimate.saturating_mul(10) >= full_wire_lower_bound.saturating_mul(9) {
        return Ok(None);
    }
    Ok(Some(RowSplice {
        kind: "row-splice",
        base_sequence: base.sequence,
        prefix_rows: u64::try_from(prefix_rows).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "row delta prefix is too large",
            )
        })?,
        delete_rows: u64::try_from(base_rows.len() - prefix_rows - suffix_rows).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "row delta delete count is too large",
            )
        })?,
        insert_rows,
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
    use crate::common::RequestBlobSpliceProvenance;
    use flate2::{Compression, write::GzEncoder};
    use http::Request;
    use http_body_util::BodyExt as _;
    use lix::storage::{
        BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, MemoryRead,
        MemoryWrite, PutBatch, ReadDurability, ReadOptions, ScanCursor, SpaceId, Storage,
        StorageError, StorageRead, StorageSpace, StorageWrite, WriteOptions,
    };
    use lix::telemetry::{
        CallbackTelemetrySink, CompletedTelemetrySpan, TelemetrySpanKind, TracingTelemetrySink,
    };
    use lix::{Blob, Memory, open_lix};
    use serde_json::{Value as JsonValue, json};
    use std::io::Write as _;
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    use tracing::Subscriber;
    use tracing_subscriber::{
        layer::{Context as LayerContext, Layer},
        prelude::*,
        registry::LookupSpan,
    };

    static TEST_IDEMPOTENCY_KEY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn openapi_contains_every_canonical_method_and_path() {
        let openapi = include_str!("../../server-protocol.openapi.yaml");
        for (method, path) in SERVER_PROTOCOL_ENDPOINTS {
            assert!(
                openapi.contains(&format!("  {path}:")),
                "OpenAPI is missing {path}"
            );
            let operation_id = match (*method, *path) {
                ("GET", "/lix/v1") => "handshake",
                ("DELETE", "/lix/v1/session") => "deleteSession",
                ("POST", "/lix/v1/execute") => "execute",
                ("POST", "/lix/v1/execute-batch") => "executeBatch",
                ("POST", "/lix/v1/sync/push") => "syncPush",
                ("GET", "/lix/v1/sync/pull") => "syncPull",
                ("GET", "/lix/v1/sync/history") => "syncHistory",
                ("GET", "/lix/v1/sync/blob") => "syncGetBlob",
                ("POST", "/lix/v1/sync/blob") => "syncRegisterBlob",
                ("GET", "/lix/v1/sync/chunk") => "syncGetChunk",
                ("PUT", "/lix/v1/sync/chunk") => "syncPutChunk",
                ("POST", "/lix/v1/transaction/begin") => "beginTransaction",
                ("POST", "/lix/v1/transaction/execute") => "transactionExecute",
                ("POST", "/lix/v1/transaction/commit") => "commitTransaction",
                ("POST", "/lix/v1/transaction/rollback") => "rollbackTransaction",
                ("GET", "/lix/v1/file") => "readFile",
                ("POST", "/lix/v1/file/upsert") => "upsertFile",
                ("POST", "/lix/v1/file/upsert-batch") => "upsertFileBatch",
                ("POST", "/lix/v1/branch/create") => "createBranch",
                ("POST", "/lix/v1/checkpoint/create") => "createCheckpoint",
                ("POST", "/lix/v1/undo") => "undo",
                ("POST", "/lix/v1/redo") => "redo",
                ("POST", "/lix/v1/branch/switch") => "switchBranch",
                ("POST", "/lix/v1/observe") => "observe",
                ("POST", "/lix/v1/observe/multiplex") => "observeMultiplex",
                _ => panic!("endpoint registry needs an OpenAPI operation mapping"),
            };
            assert!(
                openapi.contains(&format!("operationId: {operation_id}")),
                "OpenAPI is missing {method} {path}"
            );
        }
    }

    #[test]
    fn openapi_sync_wire_tracks_the_exact_snapshot_and_merge_dtos() {
        let openapi = include_str!("../../server-protocol.openapi.yaml");
        assert!(openapi.contains("required: [kind, cursor, lixId, defaultBranchId, branches]"));
        assert!(openapi.contains("required: [branchId, headCommitId, hotStateRootId]"));
        assert!(openapi.contains("required: [branchId, headCommitId, rows, continuation]"));
        assert!(openapi.contains("required: [commits, commitHeaders, boundaries]"));
        assert!(!openapi.contains("headCommits:"));
        assert!(openapi.contains(
            "required: [commitId, parentCommitIds, accountId, createdAt, selectedSourceCommitId, members]"
        ));
        assert!(openapi.contains("changeAccountId: { type: string, minLength: 1 }"));
        assert!(openapi.contains("changeCreatedAt: { type: string, minLength: 1 }"));
        assert_eq!(
            openapi
                .matches("rowPk: { $ref: \"#/components/schemas/SyncRowPk\" }")
                .count(),
            2,
            "commit members and snapshot rows must share the lossless typed rowPk schema",
        );
        for row_pk_type in ["uuid", "integer", "string", "bytes"] {
            assert!(openapi.contains(&format!("type: {{ const: {row_pk_type} }}")));
        }
        assert!(!openapi.contains("rowPk: {}"));
        assert!(!openapi.contains("sourceCommitId:"));
    }

    #[test]
    fn bounded_sync_json_response_rejects_oversized_encoded_bodies() {
        let error = bounded_sync_json_response(
            json!({ "payload": "larger than the test limit" }),
            "sync history",
            8,
        )
        .expect_err("encoded response should exceed the test limit");
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error.body.error.code, "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE");
        assert!(error.body.error.message.contains("sync history response"));

        let response = bounded_sync_json_response(json!({ "ok": true }), "sync pull", 64)
            .expect("small response should fit");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE),
            Some(&http::HeaderValue::from_static("application/json"))
        );
    }

    #[test]
    fn sync_push_admission_rejects_an_event_that_cannot_be_pulled() {
        let request = SyncPushRequest {
            commits: Vec::new(),
            ref_updates: vec![crate::sync::SyncRefUpdate {
                branch_id: uuid::Uuid::now_v7().to_string(),
                expected_head_commit_id: None,
                head_commit_id: None,
            }],
        };
        ensure_sync_push_event_fits(&request, 1024)
            .expect("the small synthetic event should fit a normal envelope");
        let error = ensure_sync_push_event_fits(&request, 1)
            .expect_err("the authority must reject an unpullable event before committing it");
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error.body.error.code, "LIX_ERROR_REQUEST_BODY_TOO_LARGE");
        assert!(error.body.error.message.contains("pull response limit"));
    }

    #[test]
    fn authenticated_idempotency_namespaces_include_the_account() {
        let principal = |account_id: &str| ServerProtocolPrincipal::Authenticated {
            account_id: account_id.to_owned(),
            idempotency_scope: "shared-provider-scope".to_owned(),
        };
        assert_ne!(
            principal("account-a").idempotency_scope(),
            principal("account-b").idempotency_scope(),
        );
    }

    type Body = ServerProtocolBody;

    trait TestStorage: Storage + Clone + Send + Sync + 'static {}

    impl<S> TestStorage for S where S: Storage + Clone + Send + Sync + 'static {}

    /// Memory commits synchronously but deliberately declines durable reads.
    /// This wrapper gives protocol idempotency tests a durable tier with the
    /// same immediately committed state, without changing Memory's production
    /// capability contract.
    #[derive(Clone, Debug, Default)]
    struct DurableMemoryStorage(Memory);

    impl DurableMemoryStorage {
        fn new() -> Self {
            Self(Memory::new())
        }
    }

    impl Storage for DurableMemoryStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(
            &self,
            mut options: ReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            options.durability = ReadDurability::Visible;
            self.0.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.0.begin_write(options).await
        }
    }

    #[derive(Clone)]
    struct Router<S: TestStorage> {
        server: LixServerProtocol<S>,
    }

    impl<S> Router<S>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        async fn oneshot(self, mut request: Request<Body>) -> Result<Response, Infallible> {
            let durable_terminal_storage_notifier = request
                .extensions_mut()
                .remove::<DurableTerminalStorageNotifier>();
            Ok(self
                .server
                .handle(
                    request,
                    ServerProtocolContext {
                        principal: ServerProtocolPrincipal::Anonymous,
                        durable_terminal_storage_notifier,
                    },
                )
                .await)
        }
    }

    fn handler<S>(server: LixServerProtocol<S>) -> Router<S>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        Router { server }
    }

    struct TestSseStream {
        body: ServerProtocolBody,
        buffered: Vec<u8>,
    }

    impl TestSseStream {
        fn new(response: Response) -> Self {
            assert_eq!(response.status(), StatusCode::OK);
            Self {
                body: response.into_body(),
                buffered: Vec::new(),
            }
        }

        async fn next(&mut self) -> JsonValue {
            loop {
                if let Some(end) = self
                    .buffered
                    .windows(2)
                    .position(|window| window == b"\n\n")
                {
                    let frame = self.buffered.drain(..end + 2).collect::<Vec<_>>();
                    let frame = std::str::from_utf8(&frame[..end]).expect("SSE event UTF-8");
                    if !frame.starts_with("event: next\n") {
                        continue;
                    }
                    let data = frame
                        .lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .expect("SSE next event data");
                    return serde_json::from_str(data).expect("SSE next event JSON");
                }
                let frame = tokio::time::timeout(Duration::from_secs(30), self.body.frame())
                    .await
                    .expect("SSE event timed out")
                    .expect("SSE stream should stay open")
                    .expect("SSE frame should be valid");
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                self.buffered.extend_from_slice(&data);
            }
        }
    }

    #[test]
    fn terminal_storage_responses_are_non_retryable_conflicts_and_mark_runtime_recovery() {
        for code in [LixError::CODE_STORAGE_FENCED, LixError::CODE_STORAGE_CLOSED] {
            let response =
                ApiError::from(LixError::new(code, "the storage instance stopped")).into_response();

            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert!(is_terminal_storage_response(&response));
        }
    }

    #[test]
    fn unknown_commit_outcome_is_non_retryable_but_does_not_retire_the_runtime() {
        let response = ApiError::from(LixError::new(
            LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN,
            "the storage commit outcome is unknown",
        ))
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!is_terminal_storage_response(&response));
    }

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
        lix: Arc<Lix<Memory>>,
        server: LixServerProtocol<Memory>,
        router: Router<Memory>,
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

    #[derive(Clone)]
    struct BlockingReadStorage {
        inner: Memory,
        gate: Arc<BlockingReadGate>,
    }

    struct BlockingReadGate {
        remaining: AtomicUsize,
        entered: Notify,
        release: Notify,
    }

    impl BlockingReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                gate: Arc::new(BlockingReadGate {
                    remaining: AtomicUsize::new(0),
                    entered: Notify::new(),
                    release: Notify::new(),
                }),
            }
        }

        fn block_next_read(&self) {
            assert_eq!(
                self.gate.remaining.swap(1, Ordering::AcqRel),
                0,
                "test read gate must be idle before arming"
            );
        }

        async fn wait_for_blocked_read(&self) {
            self.gate.entered.notified().await;
        }

        fn release_blocked_read(&self) {
            self.gate.release.notify_one();
        }

        fn assert_next_read_remains_armed_and_disarm(&self) {
            assert_eq!(
                self.gate.remaining.swap(0, Ordering::AcqRel),
                1,
                "expected the next storage read gate to remain armed",
            );
        }
    }

    impl Storage for BlockingReadStorage {
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
                .gate
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                self.gate.entered.notify_one();
                self.gate.release.notified().await;
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

    #[derive(Clone)]
    struct BlockingFencedWriteStorage {
        inner: Memory,
        gate: Arc<BlockingFencedWriteGate>,
    }

    struct BlockingFencedWriteGate {
        remaining: AtomicUsize,
        entered: AtomicBool,
        entered_notify: Notify,
        release: Notify,
    }

    impl BlockingFencedWriteStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                gate: Arc::new(BlockingFencedWriteGate {
                    remaining: AtomicUsize::new(0),
                    entered: AtomicBool::new(false),
                    entered_notify: Notify::new(),
                    release: Notify::new(),
                }),
            }
        }

        fn block_next_write(&self) {
            assert_eq!(
                self.gate.remaining.swap(1, Ordering::AcqRel),
                0,
                "test write gate must be idle before arming"
            );
            self.gate.entered.store(false, Ordering::Release);
        }

        async fn wait_for_blocked_write(&self) {
            loop {
                let notified = self.gate.entered_notify.notified();
                if self.gate.entered.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        fn release_blocked_write(&self) {
            self.gate.release.notify_waiters();
        }
    }

    impl Storage for BlockingFencedWriteStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            if self
                .gate
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                let release = self.gate.release.notified();
                self.gate.entered.store(true, Ordering::Release);
                self.gate.entered_notify.notify_waiters();
                release.await;
                return Err(StorageError::Fenced);
            }
            self.inner.begin_write(options).await
        }
    }

    /// The branch-control space, identified the way the storage layer
    /// identifies it: by space id.
    ///
    /// This gate has to recognise the authoritative branch-control read that
    /// `switch_branch` issues. It used to recognise it by *name*, and a space
    /// name carries the record encoding version, so it is expected to churn:
    /// when `untracked_generation` left the packed record in `76834a1c9`,
    /// `branch.head_control.v10` became `v11`. The gate then matched nothing,
    /// never armed, and `wait_for_blocked_branch_control_read` sat until its
    /// 1 s budget expired — which read as "the branch switch stopped issuing
    /// its authoritative read" when the read was in fact happening the whole
    /// time. A prefix match (`starts_with("branch.head_control.")`) survives a
    /// version bump but still breaks on an actual rename, because it is still
    /// a contract on the name.
    ///
    /// The id is the durable identity: it is the first four bytes of every
    /// physical key, so changing it for a live space is a layout break rather
    /// than a routine bump, and `0x0004_0020` came through `v10 -> v11`
    /// untouched. The declaration this pins to is
    /// `lix::registered_spaces::BRANCH_HEAD_CONTROL_SPACE`, which is `pub` only
    /// under the `storage-benches` feature that this crate deliberately does
    /// not enable; the id is therefore restated here rather than imported.
    const BRANCH_HEAD_CONTROL_SPACE_ID: SpaceId = SpaceId(0x0004_0020);

    #[derive(Clone)]
    struct BlockingFencedBranchControlReadStorage {
        inner: Memory,
        gate: Arc<BlockingFencedBranchControlReadGate>,
    }

    struct BlockingFencedBranchControlRead {
        inner: MemoryRead,
        gate: Arc<BlockingFencedBranchControlReadGate>,
    }

    struct BlockingFencedBranchControlReadGate {
        remaining: AtomicUsize,
        entered: AtomicBool,
        entered_notify: Notify,
        release: Notify,
    }

    impl BlockingFencedBranchControlReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                gate: Arc::new(BlockingFencedBranchControlReadGate {
                    remaining: AtomicUsize::new(0),
                    entered: AtomicBool::new(false),
                    entered_notify: Notify::new(),
                    release: Notify::new(),
                }),
            }
        }

        fn block_next_branch_control_read(&self) {
            assert_eq!(
                self.gate.remaining.swap(1, Ordering::AcqRel),
                0,
                "test branch-control read gate must be idle before arming"
            );
            self.gate.entered.store(false, Ordering::Release);
        }

        async fn wait_for_blocked_branch_control_read(&self) {
            loop {
                let notified = self.gate.entered_notify.notified();
                if self.gate.entered.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        fn release_blocked_branch_control_read(&self) {
            self.gate.release.notify_waiters();
        }
    }

    impl Storage for BlockingFencedBranchControlReadStorage {
        type Read<'a>
            = BlockingFencedBranchControlRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            Ok(BlockingFencedBranchControlRead {
                inner: self.inner.begin_read(options).await?,
                gate: Arc::clone(&self.gate),
            })
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    impl StorageRead for BlockingFencedBranchControlRead {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            let reads_branch_control = requests
                .iter()
                .any(|request| request.space.id == BRANCH_HEAD_CONTROL_SPACE_ID);
            if reads_branch_control
                && self
                    .gate
                    .remaining
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                let release = self.gate.release.notified();
                self.gate.entered.store(true, Ordering::Release);
                self.gate.entered_notify.notify_waiters();
                release.await;
                return Err(StorageError::Fenced);
            }
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            options: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.inner.begin_scan(space, range, options).await
        }
    }

    #[derive(Clone)]
    struct PostCommitUnknownStorage {
        inner: Memory,
        fail_next_commit: Arc<AtomicBool>,
    }

    impl PostCommitUnknownStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                fail_next_commit: Arc::new(AtomicBool::new(false)),
            }
        }

        fn fail_next_commit(&self) {
            self.fail_next_commit.store(true, Ordering::Release);
        }
    }

    struct PostCommitUnknownWrite {
        inner: MemoryWrite,
        fail_after_commit: bool,
    }

    impl Storage for PostCommitUnknownStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = PostCommitUnknownWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            Ok(PostCommitUnknownWrite {
                inner: self.inner.begin_write(options).await?,
                fail_after_commit: self.fail_next_commit.swap(false, Ordering::AcqRel),
            })
        }
    }

    impl StorageWrite for PostCommitUnknownWrite {
        async fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[Key],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
            range: KeyRange,
        ) -> Result<(), StorageError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, StorageError> {
            let result = self.inner.commit().await?;
            if self.fail_after_commit {
                return Err(StorageError::CommitOutcomeUnknown(
                    "injected post-commit failure".to_string(),
                ));
            }
            Ok(result)
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }

    #[derive(Clone)]
    struct FencedReadStorage {
        inner: Memory,
        fenced: Arc<AtomicBool>,
        fenced_read_count: Arc<AtomicUsize>,
        fenced_read: Arc<Notify>,
    }

    impl FencedReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                fenced: Arc::new(AtomicBool::new(false)),
                fenced_read_count: Arc::new(AtomicUsize::new(0)),
                fenced_read: Arc::new(Notify::new()),
            }
        }

        fn fence_reads(&self) {
            self.fenced.store(true, Ordering::Release);
        }

        async fn wait_for_fenced_read(&self) {
            loop {
                let notified = self.fenced_read.notified();
                if self.fenced_read_count.load(Ordering::Acquire) != 0 {
                    return;
                }
                notified.await;
            }
        }
    }

    impl Storage for FencedReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self.fenced.load(Ordering::Acquire) {
                self.fenced_read_count.fetch_add(1, Ordering::AcqRel);
                self.fenced_read.notify_waiters();
                return Err(StorageError::Fenced);
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

    #[tokio::test]
    async fn fenced_storage_does_not_affect_resumed_cached_handshake() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server.clone());
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let session_id = lease.session_id.clone();
        drop(lease);

        storage.fence_reads();
        let (notifier, signal) = durable_terminal_storage_signal();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/lix/v1")
                    .header(SESSION_ID_HEADER, &session_id)
                    .extension(notifier)
                    .body(Body::empty())
                    .expect("resumed handshake request"),
            )
            .await
            .expect("resumed handshake response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(storage.fenced_read_count.load(Ordering::Acquire), 0);
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), signal.wait_for_terminal_storage())
                .await
                .expect("cached handshake notifier should close"),
            "a cached resumed handshake must not report a terminal storage signal",
        );
        server
            .delete_session(&session_id)
            .await
            .expect("close resumed handshake session");
    }

    #[tokio::test]
    async fn fenced_new_handshake_reports_terminal_storage_signal() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let router = handler(LixServerProtocol::new(root));

        storage.fence_reads();
        let (notifier, signal) = durable_terminal_storage_signal();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/lix/v1")
                    .extension(notifier)
                    .body(Body::empty())
                    .expect("new handshake request"),
            )
            .await
            .expect("new handshake response");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), signal.wait_for_terminal_storage(),)
                .await
                .expect("fenced handshake should wake the request observer"),
            "the new handshake should preserve its terminal storage result"
        );
    }

    #[tokio::test]
    async fn fenced_new_handshake_with_explicit_branch_reports_terminal_storage_signal() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let active_branch_id = root
            .active_branch_id()
            .await
            .expect("read initial active branch");
        let router = handler(LixServerProtocol::new(root));

        storage.fence_reads();
        let (notifier, signal) = durable_terminal_storage_signal();
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/lix/v1?activeBranchId={active_branch_id}"))
                    .extension(notifier)
                    .body(Body::empty())
                    .expect("new handshake request"),
            )
            .await
            .expect("new handshake response");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), signal.wait_for_terminal_storage(),)
                .await
                .expect("fenced handshake should wake the request observer"),
            "the explicit-branch handshake should preserve its terminal storage result"
        );
    }

    #[tokio::test]
    async fn fenced_observe_next_reports_terminal_storage_before_returning_error() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let (terminal_sender, terminal_signal) = TerminalStorageStreamSignal::new();
        let events = lease
            .observe("SELECT 1", &[], terminal_sender)
            .await
            .expect("start observation");

        storage.fence_reads();
        let error = events
            .next()
            .await
            .expect_err("fenced observation should return a storage error");
        assert_eq!(error.code, LixError::CODE_STORAGE_FENCED);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                terminal_signal.wait_for_terminal_storage(),
            )
            .await
            .expect("fenced observation should wake the response observer"),
            "the observation must record its terminal storage error before the caller can be cancelled"
        );
    }

    #[derive(Clone)]
    struct FailOneBlockedReadStorage {
        inner: Memory,
        gate: Arc<FailOneBlockedReadGate>,
    }

    struct FailOneBlockedReadGate {
        remaining: AtomicUsize,
        pause_next_read: AtomicBool,
        paused_read: AtomicBool,
        paused_entered: Notify,
        paused_release: Notify,
        failing_entered: Notify,
        failing_release: Notify,
        sibling_entered: Notify,
        sibling_release: Notify,
        sibling_stopped: Arc<Notify>,
    }

    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    struct PausedReadGuard {
        gate: Arc<FailOneBlockedReadGate>,
    }

    impl Drop for PausedReadGuard {
        fn drop(&mut self) {
            self.gate.paused_release.notify_waiters();
        }
    }

    impl FailOneBlockedReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                gate: Arc::new(FailOneBlockedReadGate {
                    remaining: AtomicUsize::new(0),
                    pause_next_read: AtomicBool::new(false),
                    paused_read: AtomicBool::new(false),
                    paused_entered: Notify::new(),
                    paused_release: Notify::new(),
                    failing_entered: Notify::new(),
                    failing_release: Notify::new(),
                    sibling_entered: Notify::new(),
                    sibling_release: Notify::new(),
                    sibling_stopped: Arc::new(Notify::new()),
                }),
            }
        }

        fn fail_one_and_block_a_sibling(&self) {
            assert_eq!(
                self.gate.remaining.swap(2, Ordering::AcqRel),
                0,
                "test read gate must be idle before arming"
            );
        }

        fn pause_next_read(&self) -> PausedReadGuard {
            assert!(
                !self.gate.pause_next_read.swap(true, Ordering::AcqRel),
                "test read pause must be idle before arming"
            );
            self.gate.paused_read.store(false, Ordering::Release);
            PausedReadGuard {
                gate: Arc::clone(&self.gate),
            }
        }

        async fn wait_for_paused_read(&self) {
            loop {
                let notified = self.gate.paused_entered.notified();
                if self.gate.paused_read.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        async fn wait_for_failing_read(&self) {
            self.gate.failing_entered.notified().await;
        }

        async fn wait_for_blocked_sibling(&self) {
            self.gate.sibling_entered.notified().await;
        }

        async fn wait_for_stopped_sibling(&self) {
            self.gate.sibling_stopped.notified().await;
        }

        fn release_failing_read(&self) {
            self.gate.failing_release.notify_one();
        }

        fn release_blocked_sibling(&self) {
            self.gate.sibling_release.notify_one();
        }
    }

    impl Storage for FailOneBlockedReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self.gate.pause_next_read.swap(false, Ordering::AcqRel) {
                let release = self.gate.paused_release.notified();
                self.gate.paused_read.store(true, Ordering::Release);
                self.gate.paused_entered.notify_waiters();
                release.await;
            }
            let role = self
                .gate
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .ok();
            match role {
                Some(2) => {
                    self.gate.failing_entered.notify_one();
                    self.gate.failing_release.notified().await;
                    Err(StorageError::Fenced)
                }
                Some(1) => {
                    self.gate.sibling_entered.notify_one();
                    let _stopped = NotifyOnDrop(Arc::clone(&self.gate.sibling_stopped));
                    self.gate.sibling_release.notified().await;
                    self.inner.begin_read(options).await
                }
                Some(_) | None => self.inner.begin_read(options).await,
            }
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    #[tokio::test]
    async fn fenced_observe_after_headers_signals_and_ends_stream() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server);
        let (session_id, _) = new_session(&router).await;

        let response = request(
            &router,
            "POST",
            "/lix/v1/observe",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let terminal_signal = terminal_storage_stream_signal(&response)
            .expect("successful observe response must expose its fence signal");

        // The router has already returned 200 headers. Fencing now can only
        // reach callers through the SSE body, not an ordinary ApiError.
        storage.fence_reads();
        let body = tokio::time::timeout(Duration::from_secs(1), response.into_body().collect())
            .await
            .expect("fenced observe stream should finish")
            .expect("fenced observe body");
        let body = body.to_bytes();
        let body = std::str::from_utf8(&body).expect("SSE body is UTF-8");
        assert!(
            body.contains("event: error"),
            "expected SSE error, got {body}"
        );
        assert!(
            body.contains("\"code\":\"LIX_STORAGE_FENCED\""),
            "expected fenced storage error, got {body}"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                terminal_signal.wait_for_terminal_storage(),
            )
            .await
            .expect("fence signal should resolve"),
            "SSE fence signal should report the terminal storage error"
        );
    }

    #[tokio::test]
    async fn fenced_external_observe_watcher_signals_and_ends_stream() {
        let storage = FencedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server);
        let (session_id, _) = new_session(&router).await;

        let response = request(
            &router,
            "POST",
            "/lix/v1/observe",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let terminal_signal = terminal_storage_stream_signal(&response)
            .expect("successful observe response must expose its fence signal");
        let mut body = response.into_body();

        let initial = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("observe stream should produce its initial frame")
            .expect("observe stream should contain its initial frame")
            .expect("observe stream initial frame");
        let initial = initial
            .into_data()
            .expect("observe stream initial frame should contain data");
        let initial = std::str::from_utf8(&initial).expect("SSE body is UTF-8");
        assert!(
            initial.contains("event: next"),
            "expected initial SSE event, got {initial}"
        );

        // The initial snapshot has already started the external mutation
        // watcher. Fencing its next poll must wake the active observation with
        // the terminal error rather than leave it waiting for invalidation.
        storage.fence_reads();
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_fenced_read())
            .await
            .expect("external watcher should observe the fenced storage read");

        let terminal = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("fenced observe watcher should produce its terminal frame")
            .expect("fenced observe watcher should contain its terminal frame")
            .expect("fenced observe watcher terminal frame");
        let terminal = terminal
            .into_data()
            .expect("fenced observe watcher terminal frame should contain data");
        let terminal = std::str::from_utf8(&terminal).expect("SSE body is UTF-8");
        assert!(
            terminal.contains("event: error"),
            "expected SSE error, got {terminal}"
        );
        assert!(
            terminal.contains("\"code\":\"LIX_STORAGE_FENCED\""),
            "expected fenced storage error, got {terminal}"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), body.frame())
                .await
                .expect("fenced observe watcher should finish")
                .is_none(),
            "fenced observe watcher should reach EOF"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                terminal_signal.wait_for_terminal_storage(),
            )
            .await
            .expect("fence signal should resolve"),
            "SSE fence signal should report the terminal storage error"
        );
    }

    #[tokio::test]
    async fn fenced_multiplex_observe_aborts_live_sibling_before_next_body_poll() {
        let storage = FailOneBlockedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server);
        let (session_id, _) = new_session(&router).await;

        let _priming_observer = prime_external_observe_watcher(&router, &session_id).await;
        let _paused_watcher = storage.pause_next_read();
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_paused_read())
            .await
            .expect("external watcher should pause before multiplex fault injection");
        storage.fail_one_and_block_a_sibling();
        let response = request(
            &router,
            "POST",
            "/lix/v1/observe/multiplex",
            Some(&session_id),
            Some(json!({
                "subscriptions": [
                    { "id": "fenced", "sql": "SELECT 1", "params": [] },
                    { "id": "live", "sql": "SELECT 2", "params": [] }
                ]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let terminal_signal = terminal_storage_stream_signal(&response)
            .expect("successful multiplex response must expose its fence signal");

        // Both observation tasks are live behind already-sent headers. Leave
        // one blocked after returning the other a terminal fence error. The
        // sibling must be aborted before yielding that error, because a client
        // can stop polling after its terminal frame.
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_failing_read())
            .await
            .expect("one observation should reach the failing read");
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_blocked_sibling())
            .await
            .expect("the sibling observation should remain live");
        storage.release_failing_read();

        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("fenced multiplex stream should produce its terminal frame")
            .expect("fenced multiplex body should contain a frame")
            .expect("fenced multiplex body frame");
        let frame = frame
            .into_data()
            .expect("fenced multiplex frame should contain data");
        let frame = std::str::from_utf8(&frame).expect("SSE body is UTF-8");
        assert!(
            frame.contains("event: error"),
            "expected SSE error, got {frame}"
        );
        assert!(
            frame.contains("\"code\":\"LIX_STORAGE_FENCED\""),
            "expected fenced storage error, got {frame}"
        );
        let sibling_stopped =
            tokio::time::timeout(Duration::from_secs(1), storage.wait_for_stopped_sibling()).await;
        // Keep a failing regression self-cleaning: the old implementation
        // leaves this task blocked until explicitly released.
        storage.release_blocked_sibling();
        sibling_stopped.expect("fenced multiplex stream should abort its live sibling");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), body.frame())
                .await
                .expect("fenced multiplex stream should finish")
                .is_none(),
            "fenced multiplex stream should reach EOF"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                terminal_signal.wait_for_terminal_storage(),
            )
            .await
            .expect("fence signal should resolve"),
            "multiplex fence signal should report the terminal storage error"
        );
    }

    #[tokio::test]
    async fn dropped_multiplex_response_aborts_unpolled_workers() {
        let storage = FailOneBlockedReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server);
        let (session_id, _) = new_session(&router).await;

        let _priming_observer = prime_external_observe_watcher(&router, &session_id).await;
        let _paused_watcher = storage.pause_next_read();
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_paused_read())
            .await
            .expect("external watcher should pause before multiplex fault injection");
        storage.fail_one_and_block_a_sibling();
        let response = request(
            &router,
            "POST",
            "/lix/v1/observe/multiplex",
            Some(&session_id),
            Some(json!({
                "subscriptions": [
                    { "id": "first", "sql": "SELECT 1", "params": [] },
                    { "id": "second", "sql": "SELECT 2", "params": [] }
                ]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let terminal_signal = terminal_storage_stream_signal(&response)
            .expect("successful multiplex response must expose its fence signal");
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_failing_read())
            .await
            .expect("one observation should reach the failing read");
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_blocked_sibling())
            .await
            .expect("the sibling observation should remain live");

        drop(response);
        let signalled = tokio::time::timeout(
            Duration::from_secs(1),
            terminal_signal.wait_for_terminal_storage(),
        )
        .await;
        // Keep the failure mode self-cleaning: detached workers from the old
        // implementation are still blocked until these permits are released.
        storage.release_failing_read();
        storage.release_blocked_sibling();
        assert!(
            !signalled.expect("dropping an unpolled response should release its signal"),
            "an unpolled response must not retain a terminal-storage sender"
        );
    }

    async fn prime_external_observe_watcher(
        router: &Router<impl TestStorage>,
        session_id: &str,
    ) -> Body {
        let response = request(
            router,
            "POST",
            "/lix/v1/observe",
            Some(session_id),
            Some(json!({ "sql": "SELECT 0", "params": [] })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("priming observe should produce an initial frame")
            .expect("priming observe stream should not end immediately")
            .expect("priming observe initial frame should be valid");
        body
    }

    async fn app() -> TestApp {
        app_with_options(ServerProtocolOptions::default()).await
    }

    async fn app_with_options(options: ServerProtocolOptions) -> TestApp {
        let lix = Arc::new(open_lix().as_protocol_root().await.expect("open lix"));
        let server =
            LixServerProtocol::with_options(lix.clone(), options).expect("protocol server");
        let router = handler(server.clone());
        TestApp {
            lix,
            server,
            router,
        }
    }

    async fn router_with_storage<S>(storage: S) -> Router<S>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let lix = Arc::new(
            open_lix()
                .with_storage(storage)
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        handler(LixServerProtocol::new(lix))
    }

    #[tokio::test]
    async fn sql_mutations_require_an_idempotency_key() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request_with_headers(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            &[],
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('no-key', 'rejected')"
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"]["code"],
            LixError::CODE_IDEMPOTENCY_KEY_REQUIRED
        );
        let persisted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'no-key'"
            })),
        )
        .await;
        assert_eq!(persisted.status(), StatusCode::OK);
        assert_eq!(
            response_json(persisted).await["rows"][0][0],
            json!({ "kind": "int", "value": 0 })
        );
    }

    #[tokio::test]
    async fn idempotency_key_reuse_with_a_different_mutation_is_rejected() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let headers = [(IDEMPOTENCY_KEY_HEADER, "reuse-mismatch")];
        let first = request_with_headers(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            &headers,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('first', 'one')"
            })),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);

        let reused = request_with_headers(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            &headers,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('second', 'two')"
            })),
        )
        .await;
        assert_eq!(reused.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(reused).await["error"]["code"],
            LixError::CODE_IDEMPOTENCY_KEY_REUSED
        );
    }

    #[tokio::test]
    async fn idempotency_key_reuse_on_another_branch_is_rejected() {
        let app = app().await;
        let (main_session, _) = new_session(&app.router).await;
        let created = request(
            &app.router,
            "POST",
            "/lix/v1/branch/create",
            Some(&main_session),
            Some(json!({ "name": "Idempotency draft" })),
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);
        let draft_branch = response_json(created).await["id"]
            .as_str()
            .expect("draft branch id")
            .to_string();
        let (draft_session, _) = new_session_at(&app.router, Some(&draft_branch)).await;
        let headers = [(IDEMPOTENCY_KEY_HEADER, "same-key-different-branch")];
        let body = json!({
            "sql": "INSERT INTO lix_key_value (key, value) VALUES ('branch-bound-key', 'main')"
        });

        let first = request_with_headers(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&main_session),
            &headers,
            Some(body.clone()),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);

        let reused = request_with_headers(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&draft_session),
            &headers,
            Some(body),
        )
        .await;
        assert_eq!(reused.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(reused).await["error"]["code"],
            LixError::CODE_IDEMPOTENCY_KEY_REUSED
        );

        let draft_count = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&draft_session),
            Some(json!({
                "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'branch-bound-key'"
            })),
        )
        .await;
        assert_eq!(draft_count.status(), StatusCode::OK);
        assert_eq!(
            response_json(draft_count).await["rows"][0][0],
            json!({ "kind": "int", "value": 0 })
        );
    }

    #[tokio::test]
    async fn idempotent_batch_hydrates_sparse_history_then_replays_one_durable_result() {
        let authority = open_lix().await.expect("open history authority");
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/history.md', CAST('one' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed historical file");
        let file_id = authority
            .execute("SELECT id FROM lix_file WHERE path = '/history.md'", &[])
            .await
            .expect("load historical file id")
            .rows()[0]
            .get::<String>("id")
            .expect("file id");
        authority
            .create_checkpoint()
            .await
            .expect("checkpoint first version");
        authority
            .execute(
                "UPDATE lix_file SET content = CAST('two' AS BYTEA) WHERE id = $1",
                &[Value::Text(file_id)],
            )
            .await
            .expect("write second historical version");

        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("load sparse snapshot metadata");
        let SyncRepositoryPullResponse::Snapshot {
            default_branch_id,
            branches,
            ..
        } = &snapshot
        else {
            panic!("initial pull must be a snapshot")
        };
        let branch = branches
            .iter()
            .find(|branch| branch.branch_id == *default_branch_id)
            .expect("default branch metadata");
        let head = branch.head_commit_id.clone().expect("default branch head");
        let mut bootstrap_commits = std::collections::BTreeMap::new();
        let mut bootstrap_headers = std::collections::BTreeMap::new();
        for branch_head in branches
            .iter()
            .filter_map(|branch| branch.head_commit_id.as_deref())
        {
            let page = authority
                .sync_history(branch_head, 1)
                .await
                .expect("load exact branch head body and sparse headers");
            bootstrap_commits.extend(
                page.commits
                    .into_iter()
                    .map(|commit| (commit.commit_id.clone(), commit)),
            );
            for header in page.commit_headers {
                match bootstrap_headers.entry(header.commit_id.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(header);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        assert_eq!(entry.get(), &header, "duplicate sparse header agrees");
                    }
                }
            }
        }
        let parent = bootstrap_commits[&head]
            .parent_commit_ids
            .first()
            .cloned()
            .expect("head has a deferred parent");
        let mut snapshot_rows = Vec::new();
        for branch in branches {
            let Some(branch_head) = branch.head_commit_id.as_deref() else {
                continue;
            };
            let mut continuation = None;
            loop {
                let page = authority
                    .pull_sync_snapshot_rows(
                        &branch.branch_id,
                        branch_head,
                        continuation.as_deref(),
                        512,
                    )
                    .await
                    .expect("load hot snapshot rows");
                snapshot_rows.extend(page.rows);
                let Some(next) = page.continuation else {
                    break;
                };
                continuation = Some(next);
            }
        }

        let storage = DurableMemoryStorage::new();
        crate::engine::Engine::initialize_with_main_branch_id(
            storage.clone(),
            Some(default_branch_id),
        )
        .await
        .expect("initialize sparse replica storage");
        let mut replica = open_lix()
            .with_storage(storage)
            .await
            .expect("open sparse replica");
        replica
            .set_sync_role(crate::sync::SyncRole::Replica)
            .expect("set sparse replica role");
        replica
            .apply_sync_repository_snapshot(
                "test://server-protocol-history",
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &bootstrap_commits.into_values().collect::<Vec<_>>(),
                &bootstrap_headers.into_values().collect::<Vec<_>>(),
                &snapshot_rows,
            )
            .await
            .expect("install sparse replica snapshot");

        let (demand_tx, mut demand_rx) = mpsc::channel(1);
        replica.set_sync_demand_sender_for_test(demand_tx);
        let replica = Arc::new(replica);
        let hydrate_replica = Arc::clone(&replica);
        let hydrate_authority = authority.clone();
        let hydration = tokio::spawn(async move {
            let demand = demand_rx.recv().await.expect("history demand arrives");
            let history = hydrate_authority
                .sync_history(&parent, crate::sync::MAX_SYNC_HISTORY_PAGE_SIZE)
                .await
                .expect("load demanded history");
            let historical_blob_ids = history
                .commits
                .iter()
                .flat_map(|commit| &commit.members)
                .filter(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
                .filter_map(|member| {
                    member
                        .snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.get("blob_hash"))
                        .and_then(serde_json::Value::as_str)
                })
                .collect::<std::collections::BTreeSet<_>>();
            for blob_id in historical_blob_ids {
                let manifest = hydrate_authority
                    .get_sync_blob_manifest(blob_id)
                    .await
                    .expect("load historical blob manifest")
                    .expect("historical blob manifest exists");
                hydrate_replica
                    .register_deferred_sync_blob_manifest(&manifest)
                    .await
                    .expect("register historical blob manifest lazily");
            }
            let mut boundary_rows = Vec::new();
            for boundary in &history.boundaries {
                let mut continuation = None;
                loop {
                    let page = hydrate_authority
                        .pull_sync_snapshot_rows(
                            &boundary.commit_id,
                            &boundary.commit_id,
                            continuation.as_deref(),
                            512,
                        )
                        .await
                        .expect("load demanded boundary rows");
                    boundary_rows.extend(page.rows);
                    let Some(next) = page.continuation else {
                        break;
                    };
                    continuation = Some(next);
                }
            }
            hydrate_replica
                .import_sync_history_headers(&history.commit_headers)
                .await
                .expect("import demanded headers");
            hydrate_replica
                .import_sync_history_boundaries(
                    &history.commits,
                    &history.boundaries,
                    &boundary_rows,
                )
                .await
                .expect("import demanded bodies");
            demand.succeed_for_test();
        });

        let router = handler(LixServerProtocol::new(replica));
        let (session_id, _) = new_session(&router).await;
        let headers = [(IDEMPOTENCY_KEY_HEADER, "sparse-history-batch")];
        let body = json!({
            "statements": [
                {
                    "sql": format!(
                        "SELECT COUNT(*) AS versions FROM lix_file_history('{head}')"
                    ),
                    "params": []
                },
                {
                    "sql": "INSERT INTO lix_key_value (key, value) VALUES ('history-hydrated', 'once') RETURNING value",
                    "params": []
                }
            ]
        });
        let first = request_with_headers(
            &router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            &headers,
            Some(body.clone()),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let first = response_json(first).await;
        assert!(first[0]["rows"][0][0]["value"].as_i64().unwrap_or(0) >= 2);
        hydration.await.expect("history hydration task");

        let replay = request_with_headers(
            &router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            &headers,
            Some(body),
        )
        .await;
        let replay_status = replay.status();
        let replay = response_json(replay).await;
        assert_eq!(replay_status, StatusCode::OK, "replay response: {replay}");
        assert_eq!(replay, first);

        let count = request(
            &router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'history-hydrated'"
            })),
        )
        .await;
        assert_eq!(count.status(), StatusCode::OK);
        assert_eq!(
            response_json(count).await["rows"][0][0],
            json!({ "kind": "int", "value": 1 })
        );
    }

    #[tokio::test]
    async fn keyed_write_without_durable_receipt_proof_is_not_acknowledged() {
        let storage = PostCommitUnknownStorage::new();
        let lix = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let server = LixServerProtocol::new(lix);
        let router = handler(server);
        let (session_id, _) = new_session(&router).await;
        let headers = [(IDEMPOTENCY_KEY_HEADER, "memory-has-no-durable-proof")];

        storage.fail_next_commit();
        let response = request_with_headers(
            &router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            &headers,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('unknown-commit', 'written')"
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let error = response_json(response).await;
        assert_eq!(
            error["error"]["code"],
            LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
        );
        assert_eq!(error["error"]["details"]["retryable"], true);
        assert_eq!(
            error["error"]["details"]["retryScope"],
            "same-idempotency-key"
        );
        assert_eq!(error["error"]["details"]["outcome"], "unknown");

        let persisted = request(
            &router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'unknown-commit'"
            })),
        )
        .await;
        assert_eq!(persisted.status(), StatusCode::OK);
        assert_eq!(
            response_json(persisted).await["rows"][0][0],
            json!({ "kind": "int", "value": 1 })
        );
    }

    async fn app_with_tracing_telemetry() -> TestApp {
        let lix = Arc::new(
            open_lix()
                .with_telemetry(Arc::new(TracingTelemetrySink::new()))
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::new(lix.clone());
        let router = handler(server.clone());
        TestApp {
            lix,
            server,
            router,
        }
    }

    async fn request(
        app: &Router<impl TestStorage>,
        method: &str,
        uri: &str,
        session_id: Option<&str>,
        body: Option<JsonValue>,
    ) -> Response {
        let idempotency_key = (method == "POST"
            && matches!(uri, "/lix/v1/execute" | "/lix/v1/execute-batch"))
        .then(|| {
            format!(
                "server-protocol-test-{}",
                TEST_IDEMPOTENCY_KEY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            )
        });
        let idempotency_headers = idempotency_key
            .as_deref()
            .map(|key| (IDEMPOTENCY_KEY_HEADER, key))
            .into_iter()
            .collect::<Vec<_>>();
        request_with_headers(app, method, uri, session_id, &idempotency_headers, body).await
    }

    async fn begin_remote_transaction(app: &Router<impl TestStorage>, session_id: &str) -> String {
        let response = request(
            app,
            "POST",
            "/lix/v1/transaction/begin",
            Some(session_id),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        response_json(response).await["transactionId"]
            .as_str()
            .expect("transaction ID")
            .to_owned()
    }

    async fn remote_transaction_request(
        app: &Router<impl TestStorage>,
        method: &str,
        uri: &str,
        session_id: &str,
        transaction_id: &str,
        body: Option<JsonValue>,
    ) -> Response {
        request_with_headers(
            app,
            method,
            uri,
            Some(session_id),
            &[(TRANSACTION_ID_HEADER, transaction_id)],
            body,
        )
        .await
    }

    async fn request_with_headers(
        app: &Router<impl TestStorage>,
        method: &str,
        uri: &str,
        session_id: Option<&str>,
        headers: &[(&str, &str)],
        body: Option<JsonValue>,
    ) -> Response {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(session_id) = session_id {
            builder = builder.header(SESSION_ID_HEADER, session_id);
        }
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let body = body.map_or_else(Body::empty, |body| {
            builder
                .headers_mut()
                .expect("request builder headers")
                .insert(
                    CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
            Body::from(body.to_string())
        });
        app.clone()
            .oneshot(builder.body(body).expect("request"))
            .await
            .expect("response")
    }

    async fn new_session(app: &Router<impl TestStorage>) -> (String, JsonValue) {
        new_session_at(app, None).await
    }

    async fn new_session_at(
        app: &Router<impl TestStorage>,
        active_branch_id: Option<&str>,
    ) -> (String, JsonValue) {
        let uri = active_branch_id.map_or_else(
            || "/lix/v1".to_string(),
            |active_branch_id| format!("/lix/v1?activeBranchId={active_branch_id}"),
        );
        let response = request(app, "GET", &uri, None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&http::HeaderValue::from_static("no-store"))
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

    #[test]
    fn request_blob_splice_metadata_stays_aligned_with_its_sql_parameter() {
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let mut result = base.clone();
        result[8..12].copy_from_slice(b"BETA");
        let base_sha256 = sha256_hex(&base);
        let params = vec![
            json!({ "kind": "text", "value": "file-a" }),
            blob_splice_json(&base, &result, 8, base.len() - 12, b"BETA"),
            wire_blob_json(b"full"),
        ]
        .into_iter()
        .map(|value| serde_json::from_value(value).expect("request parameter should decode"))
        .collect();
        let mut reconstructed_bytes_remaining = DEFAULT_MAX_REQUEST_BODY_BYTES;
        let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
        let mut cache_candidates = Vec::new();

        let decoded = decode_request_params(
            params,
            None,
            false,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
            &mut reconstructed_bytes_remaining,
            &mut cache_candidate_bytes_remaining,
            &mut cache_candidates,
            |sha256| {
                (sha256 == base_sha256).then(|| VerifiedRequestBlob::verify(base.clone().into()))
            },
        )
        .expect("splice parameters should decode");

        let Value::Blob(result_blob) = &decoded.values[1] else {
            panic!("splice should decode to a blob parameter");
        };
        assert_eq!(result_blob.as_ref(), result.as_slice());
        assert_eq!(decoded.metadata.parameter_blob_splices.len(), 3);
        assert!(decoded.metadata.parameter_blob_splices[0].is_none());
        let provenance = decoded.metadata.parameter_blob_splices[1]
            .as_ref()
            .expect("splice metadata should remain at parameter two");
        let expected = RequestBlobSpliceProvenance::new_validated(
            &base,
            result_blob,
            &base_sha256,
            &sha256_hex(&result),
            8,
            base.len() - 12,
            b"BETA".to_vec(),
        )
        .expect("expected splice provenance should validate");
        assert_eq!(provenance, &expected);
        assert!(decoded.metadata.parameter_blob_splices[2].is_none());
    }

    #[test]
    fn request_blob_splice_reconstructs_the_exact_large_csv_fixture() {
        const EXACT_CSV_BYTES: usize = 10_680_000;
        const _: () = {
            assert!(EXACT_CSV_BYTES > 10 * 1024 * 1024);
            assert!(EXACT_CSV_BYTES <= MAX_REQUEST_BLOB_CACHE_BYTES);
        };

        let base: Blob = vec![b'a'; EXACT_CSV_BYTES].into();
        let mut result = base.to_vec();
        let edit_offset = result.len() / 2;
        result[edit_offset] = b'b';
        let base_sha256 = sha256_hex(&base);
        let params = vec![
            serde_json::from_value(blob_splice_json(
                &base,
                &result,
                edit_offset,
                base.len() - edit_offset - 1,
                b"b",
            ))
            .expect("large splice request parameter should decode"),
        ];
        let mut reconstructed_bytes_remaining = DEFAULT_MAX_REQUEST_BODY_BYTES;
        let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
        let mut cache_candidates = Vec::new();

        let decoded = decode_request_params(
            params,
            None,
            false,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
            &mut reconstructed_bytes_remaining,
            &mut cache_candidate_bytes_remaining,
            &mut cache_candidates,
            |sha256| (sha256 == base_sha256).then(|| VerifiedRequestBlob::verify(base.clone())),
        )
        .expect("the exact 10.68 MB CSV splice should reconstruct");

        let Value::Blob(reconstructed) = &decoded.values[0] else {
            panic!("large splice should decode to a blob parameter");
        };
        assert_eq!(reconstructed.as_ref(), result.as_slice());
        let provenance = decoded.metadata.parameter_blob_splices[0]
            .as_ref()
            .expect("large splice provenance should survive decoding");
        let expected = RequestBlobSpliceProvenance::new_validated(
            &base,
            reconstructed,
            &base_sha256,
            &sha256_hex(&result),
            edit_offset,
            base.len() - edit_offset - 1,
            b"b".to_vec(),
        )
        .expect("expected large splice provenance should validate");
        assert_eq!(provenance, &expected);
        assert_eq!(
            reconstructed_bytes_remaining,
            DEFAULT_MAX_REQUEST_BODY_BYTES - EXACT_CSV_BYTES
        );
        assert_eq!(cache_candidates.len(), 1);
        assert_eq!(cache_candidates[0].blob.blob().as_ref(), result.as_slice());
        assert_eq!(
            cache_candidates[0].blob.blob().as_ptr(),
            reconstructed.as_ptr(),
            "SQL, provenance, and the successor cache must share one reconstructed payload"
        );
    }

    #[test]
    fn batch_blob_splice_metadata_stays_aligned_by_statement() {
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let base_sha256 = sha256_hex(&base);
        let decode = |parameter_index: usize, insert: u8| {
            let mut result = base.clone();
            result[parameter_index] = insert;
            let splice = blob_splice_json(
                &base,
                &result,
                parameter_index,
                base.len() - parameter_index - 1,
                &[insert],
            );
            let mut wire = vec![json!({ "kind": "text", "value": "unrelated" })];
            wire.resize_with(parameter_index, || json!({ "kind": "null", "value": null }));
            wire.push(splice);
            let params = wire
                .into_iter()
                .map(|value| {
                    serde_json::from_value(value).expect("batch request parameter should decode")
                })
                .collect();
            let mut reconstructed_bytes_remaining = DEFAULT_MAX_REQUEST_BODY_BYTES;
            let mut cache_candidate_bytes_remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
            let mut cache_candidates = Vec::new();
            decode_request_params(
                params,
                Some(parameter_index),
                false,
                DEFAULT_MAX_REQUEST_BODY_BYTES,
                &mut reconstructed_bytes_remaining,
                &mut cache_candidate_bytes_remaining,
                &mut cache_candidates,
                |sha256| {
                    (sha256 == base_sha256)
                        .then(|| VerifiedRequestBlob::verify(base.clone().into()))
                },
            )
            .expect("batch splice should decode")
        };

        let first = decode(1, b'1');
        let second = decode(3, b'2');
        assert!(first.metadata.parameter_blob_splices[0].is_none());
        let Value::Blob(first_result) = &first.values[1] else {
            panic!("first splice should decode to a blob");
        };
        let first_expected = RequestBlobSpliceProvenance::new_validated(
            &base,
            first_result,
            &base_sha256,
            &sha256_hex(first_result),
            1,
            base.len() - 2,
            vec![b'1'],
        )
        .expect("first expected provenance should validate");
        assert_eq!(
            first.metadata.parameter_blob_splices[1].as_ref(),
            Some(&first_expected)
        );
        assert!(second.metadata.parameter_blob_splices[1].is_none());
        assert!(second.metadata.parameter_blob_splices[2].is_none());
        let Value::Blob(second_result) = &second.values[3] else {
            panic!("second splice should decode to a blob");
        };
        let second_expected = RequestBlobSpliceProvenance::new_validated(
            &base,
            second_result,
            &base_sha256,
            &sha256_hex(second_result),
            3,
            base.len() - 4,
            vec![b'2'],
        )
        .expect("second expected provenance should validate");
        assert_eq!(
            second.metadata.parameter_blob_splices[3].as_ref(),
            Some(&second_expected)
        );
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).expect("gzip input");
        encoder.finish().expect("finish gzip")
    }

    async fn error_code(response: Response) -> String {
        response_json(response).await["error"]["code"]
            .as_str()
            .expect("error code")
            .to_string()
    }

    fn binary_file_upsert_batch_body(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(
            &u32::try_from(entries.len())
                .expect("test batch entry count should fit the wire format")
                .to_be_bytes(),
        );
        for (path, data) in entries {
            body.extend_from_slice(
                &u32::try_from(path.len())
                    .expect("test batch path should fit the wire format")
                    .to_be_bytes(),
            );
            body.extend_from_slice(
                &u32::try_from(data.len())
                    .expect("test batch data should fit the wire format")
                    .to_be_bytes(),
            );
            body.extend_from_slice(path.as_bytes());
            body.extend_from_slice(data);
        }
        body
    }

    async fn binary_file_upsert_batch_request(
        app: &Router<impl TestStorage>,
        session_id: Option<&str>,
        body: Vec<u8>,
    ) -> Response {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/lix/v1/file/upsert-batch")
            .header(CONTENT_TYPE, "application/octet-stream");
        if let Some(session_id) = session_id {
            builder = builder.header(SESSION_ID_HEADER, session_id);
        }
        app.clone()
            .oneshot(
                builder
                    .body(Body::from(body))
                    .expect("binary batch request"),
            )
            .await
            .expect("binary batch response")
    }

    fn opened_spans(spans: &[CompletedTelemetrySpan]) -> Vec<&CompletedTelemetrySpan> {
        spans
            .iter()
            .filter(|span| span.start.kind == TelemetrySpanKind::LixOpened)
            .collect()
    }

    async fn app_with_callback_telemetry(
        spans: Arc<Mutex<Vec<CompletedTelemetrySpan>>>,
    ) -> TestApp {
        let captured = Arc::clone(&spans);
        let lix = Arc::new(
            open_lix()
                .with_telemetry(Arc::new(CallbackTelemetrySink::new(move |span| {
                    captured.lock().expect("spans").push(span);
                })))
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::new(lix.clone());
        let router = handler(server.clone());
        TestApp {
            lix,
            server,
            router,
        }
    }

    #[tokio::test]
    async fn handshake_that_creates_a_session_emits_one_opened_span() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let app = app_with_callback_telemetry(Arc::clone(&spans)).await;
        assert_eq!(opened_spans(&spans.lock().expect("spans")).len(), 0);

        let (session_id, first) = new_session(&app.router).await;
        assert_eq!(opened_spans(&spans.lock().expect("spans")).len(), 1);

        let resumed = request(&app.router, "GET", "/lix/v1/", Some(&session_id), None).await;
        assert_eq!(resumed.status(), StatusCode::OK);
        assert_eq!(opened_spans(&spans.lock().expect("spans")).len(), 1);

        let execute = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .await;
        assert_eq!(execute.status(), StatusCode::OK);
        {
            let spans = spans.lock().expect("spans");
            assert_eq!(opened_spans(&spans).len(), 1);
            assert!(
                spans
                    .iter()
                    .any(|span| span.start.kind == TelemetrySpanKind::SqlQuery),
                "SQL spans still work after handshake bind"
            );
        }

        let (second_session_id, second) = new_session(&app.router).await;
        assert_ne!(second_session_id, session_id);
        assert_eq!(second["activeBranchId"], first["activeBranchId"]);
        assert_eq!(opened_spans(&spans.lock().expect("spans")).len(), 2);
    }

    #[tokio::test]
    async fn handshake_without_a_sink_emits_no_opened_span() {
        let app = app().await;
        let (_session_id, _) = new_session(&app.router).await;
        assert!(app.server.inner.root.telemetry().is_none());
    }

    #[tokio::test]
    async fn handshake_issues_and_resumes_a_256_bit_server_session() {
        let app = app().await;
        let (session_id, first) = new_session(&app.router).await;
        assert_eq!(first["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(first["activeAccountId"], lix::ANONYMOUS_ACCOUNT_ID);
        assert!(first["capabilities"].get("requestBlobSplice").is_none());
        assert_eq!(first["capabilities"]["binaryFileUpsert"], true);
        assert_eq!(first["capabilities"]["binaryFileUpsertBatch"], true);
        assert_eq!(first["capabilities"]["binaryFileRead"], true);
        assert_eq!(first["capabilities"]["syncPush"], true);
        assert_eq!(first["capabilities"]["syncPull"], true);
        assert_eq!(first["capabilities"]["syncHistory"], true);
        assert_eq!(first["capabilities"]["syncBlob"], true);
        assert_eq!(first["capabilities"]["syncChunk"], true);
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

    #[test]
    fn sync_history_query_is_one_head_and_limit() {
        let query = decode_query::<SyncHistoryQuery>(Some("head=one&limit=10"))
            .expect("history page query");
        assert_eq!(query.head, "one");
        assert_eq!(query.limit, 10);
        assert!(decode_query::<SyncHistoryQuery>(Some("head=one")).is_err());
        assert!(decode_query::<SyncHistoryQuery>(Some("commitId=legacy")).is_err());
        assert!(decode_query::<SyncHistoryQuery>(Some("head=one&before=two")).is_err());
    }

    #[test]
    fn sync_ref_compare_and_swap_conflicts_are_http_conflicts() {
        let error = LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "sync ref compare-and-swap failed",
        );
        assert_eq!(status_for_lix_error(&error), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn sync_surface_is_a_hard_cut_to_push_pull_and_history() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;

        for path in ["/lix/v1/sync/admit", "/lix/v1/sync/branches"] {
            let response = request(&app.router, "GET", path, Some(&session_id), None).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }

        let empty_push = request(
            &app.router,
            "POST",
            "/lix/v1/sync/push",
            Some(&session_id),
            Some(json!({ "commits": [], "refUpdates": [] })),
        )
        .await;
        assert_eq!(empty_push.status(), StatusCode::BAD_REQUEST);

        for path in [
            "/lix/v1/sync/pull?limit=0",
            "/lix/v1/sync/pull?schemas=legacy",
            "/lix/v1/sync/pull?snapshotBranchId=branch",
            "/lix/v1/sync/pull?snapshotBranchId=branch&snapshotHeadCommitId=head&after=1",
            "/lix/v1/sync/history",
            "/lix/v1/sync/history?head=legacy",
        ] {
            let response = request(&app.router, "GET", path, Some(&session_id), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }
    }

    #[tokio::test]
    async fn sync_snapshot_wire_is_metadata_only_and_rows_are_paged_by_immutable_head() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "GET",
            "/lix/v1/sync/pull",
            Some(&session_id),
            None,
        )
        .await;
        let status = response.status();
        let snapshot = response_json(response).await;
        assert_eq!(status, StatusCode::OK, "{snapshot}");
        assert_eq!(snapshot["kind"], "snapshot");
        assert!(snapshot.get("headCommits").is_none());
        assert!(snapshot.get("commitHeaders").is_none());
        assert!(snapshot.get("rows").is_none());

        let branches = snapshot["branches"]
            .as_array()
            .expect("snapshot branches should be an array");
        let mut row_count = 0usize;
        for branch in branches {
            let Some(head_commit_id) = branch["headCommitId"].as_str() else {
                continue;
            };
            let branch_id = branch["branchId"].as_str().expect("branch id");
            let mut continuation = None;
            loop {
                let suffix = continuation
                    .as_ref()
                    .map(|continuation: &String| format!("&snapshotAfter={continuation}"))
                    .unwrap_or_default();
                let page = request(
                    &app.router,
                    "GET",
                    &format!(
                        "/lix/v1/sync/pull?snapshotBranchId={branch_id}&snapshotHeadCommitId={head_commit_id}&limit=1{suffix}"
                    ),
                    Some(&session_id),
                    None,
                )
                .await;
                let status = page.status();
                let page = response_json(page).await;
                assert_eq!(status, StatusCode::OK, "{page}");
                assert_eq!(page["branchId"], branch_id);
                assert_eq!(page["headCommitId"], head_commit_id);
                let rows = page["rows"].as_array().expect("row page rows");
                assert!(rows.len() <= 1);
                assert!(rows.iter().all(|row| row["branchId"] == branch_id));
                row_count += rows.len();
                continuation = page["continuation"].as_str().map(str::to_owned);
                if continuation.is_none() {
                    break;
                }
            }
        }
        assert!(row_count > 0, "bootstrap fixture should expose hot rows");
    }

    #[tokio::test]
    async fn sync_history_and_push_roundtrip_commit_scoped_merge_provenance() {
        let app = app().await;
        let source_branch = app
            .lix
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-000000001501".to_owned()),
                name: "sync selected source".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("source branch should be created");
        let source = app
            .lix
            .open_another_session()
            .await
            .expect("source session should open");
        source
            .switch_branch(SwitchBranchOptions {
                branch_id: source_branch.id.clone(),
            })
            .await
            .expect("source session should switch branches");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('sync-source', 'selected')",
                &[],
            )
            .await
            .expect("source branch should diverge");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('sync-source-older', 'selected')",
                &[],
            )
            .await
            .expect("source branch should span more than one commit");
        app.lix
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('sync-target', 'authored')",
                &[],
            )
            .await
            .expect("target branch should diverge");
        let merge = app
            .lix
            .merge_branch(lix::MergeBranchOptions {
                source_branch_id: source_branch.id,
            })
            .await
            .expect("disjoint branches should merge");
        let merge_commit_id = merge
            .created_merge_commit_id
            .expect("diverged branches should create a merge commit");

        let (session_id, _) = new_session(&app.router).await;
        let history_path = format!("/lix/v1/sync/history?head={merge_commit_id}&limit=1");
        let history = request(&app.router, "GET", &history_path, Some(&session_id), None).await;
        assert_eq!(history.status(), StatusCode::OK);
        let history = response_json(history).await;
        let merge_commit = history["commits"]
            .as_array()
            .expect("history commits should be an array")
            .iter()
            .find(|commit| commit["commitId"] == merge_commit_id)
            .expect("history should include the requested merge commit")
            .clone();
        assert_eq!(
            merge_commit["selectedSourceCommitId"],
            merge.source_head_before_commit_id
        );
        let members = merge_commit["members"]
            .as_array()
            .expect("merge members should be an array");
        assert!(!members.is_empty());
        assert!(members.iter().any(|member| member["authored"] == false));
        assert!(
            members
                .iter()
                .all(|member| member.get("sourceCommitId").is_none())
        );

        let mut pending = vec![merge_commit_id.clone()];
        let mut imported = std::collections::BTreeMap::<String, JsonValue>::new();
        while let Some(commit_id) = pending.pop() {
            if imported.contains_key(&commit_id) {
                continue;
            }
            let response = app
                .lix
                .sync_history(&commit_id, 1)
                .await
                .expect("source history closure should load");
            for commit in response.commits {
                for dependency in &commit.parent_commit_ids {
                    if !imported.contains_key(dependency) {
                        pending.push(dependency.clone());
                    }
                }
                imported.insert(
                    commit.commit_id.clone(),
                    serde_json::to_value(commit).expect("history commit should encode"),
                );
            }
        }

        let mut forged_commits = imported.values().cloned().collect::<Vec<_>>();
        let forged_merge = forged_commits
            .iter_mut()
            .find(|commit| commit["commitId"] == merge_commit_id)
            .expect("forged graph contains merge");
        let forged_selected = forged_merge["members"]
            .as_array_mut()
            .expect("merge members array")
            .iter_mut()
            .find(|member| member["authored"] == false)
            .expect("merge has a selected member");
        forged_selected["snapshot"] = json!({"key": "forged", "value": "forged"});
        let forged_target = self::app().await;
        let forged_request: SyncPushRequest = serde_json::from_value(json!({
            "commits": forged_commits,
            "refUpdates": [{
                "branchId": "01920000-0000-7000-8000-000000001503",
                "expectedHeadCommitId": null,
                "headCommitId": merge_commit_id,
            }]
        }))
        .expect("forged request should decode");
        let forged = forged_target
            .lix
            .push_sync_repository(&forged_request)
            .await
            .expect_err("forged selected payload must fail authority validation");
        assert_eq!(forged.code, LixError::CODE_INVALID_PARAM);

        let target = self::app().await;
        let replay_request: SyncPushRequest = serde_json::from_value(json!({
            "commits": imported.into_values().collect::<Vec<_>>(),
            "refUpdates": [{
                "branchId": "01920000-0000-7000-8000-000000001502",
                "expectedHeadCommitId": null,
                "headCommitId": merge_commit_id,
            }]
        }))
        .expect("roundtrip request should decode");
        target
            .lix
            .push_sync_repository(&replay_request)
            .await
            .expect("valid full graph should import");
        let (target_session_id, _) = new_session(&target.router).await;

        let roundtrip = request(
            &target.router,
            "GET",
            &history_path,
            Some(&target_session_id),
            None,
        )
        .await;
        assert_eq!(roundtrip.status(), StatusCode::OK);
        let roundtrip = response_json(roundtrip).await;
        assert_eq!(roundtrip["commits"], history["commits"]);
        assert_eq!(roundtrip["commitHeaders"], history["commitHeaders"]);
        assert_eq!(roundtrip["boundaries"], history["boundaries"]);
    }

    #[tokio::test]
    async fn sync_cas_rejects_aliases_invalid_media_and_chunk_bounds() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let digest = "0".repeat(64);

        for path in [
            "/lix/v1/sync/blob?sha256=legacy",
            "/lix/v1/sync/blob?blobId=ABC",
            "/lix/v1/sync/chunk?chunkId=ABC",
        ] {
            let response = request(&app.router, "GET", path, Some(&session_id), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }
        let legacy_registration = request(
            &app.router,
            "POST",
            "/lix/v1/sync/blob?sha256=legacy",
            Some(&session_id),
            Some(json!({ "blobId": digest, "sizeBytes": 0, "chunks": [] })),
        )
        .await;
        assert_eq!(legacy_registration.status(), StatusCode::BAD_REQUEST);
        let dynamic_path = request(
            &app.router,
            "GET",
            &format!("/lix/v1/sync/chunk/{digest}"),
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(dynamic_path.status(), StatusCode::NOT_FOUND);
        for path in [
            format!("/lix/v1/sync/blob?blobId={digest}"),
            format!("/lix/v1/sync/chunk?chunkId={digest}"),
        ] {
            let response = request(&app.router, "GET", &path, Some(&session_id), None).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            assert_eq!(error_code(response).await, "LIX_ERROR_SYNC_CAS_NOT_FOUND");
        }

        let wrong_blob_media = Request::builder()
            .method("POST")
            .uri("/lix/v1/sync/blob")
            .header(SESSION_ID_HEADER, &session_id)
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::from("{}"))
            .expect("wrong manifest media request");
        let response = app
            .router
            .clone()
            .oneshot(wrong_blob_media)
            .await
            .expect("wrong manifest media response");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let wrong_chunk_media = Request::builder()
            .method("PUT")
            .uri(format!("/lix/v1/sync/chunk?chunkId={digest}"))
            .header(SESSION_ID_HEADER, &session_id)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("x"))
            .expect("wrong chunk media request");
        let response = app
            .router
            .clone()
            .oneshot(wrong_chunk_media)
            .await
            .expect("wrong chunk media response");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let empty_chunk = Request::builder()
            .method("PUT")
            .uri(format!("/lix/v1/sync/chunk?chunkId={digest}"))
            .header(SESSION_ID_HEADER, &session_id)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::empty())
            .expect("empty chunk request");
        let response = app
            .router
            .clone()
            .oneshot(empty_chunk)
            .await
            .expect("empty chunk response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let oversized_chunk = Request::builder()
            .method("PUT")
            .uri(format!("/lix/v1/sync/chunk?chunkId={digest}"))
            .header(SESSION_ID_HEADER, &session_id)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(vec![0; MAX_SYNC_CHUNK_BYTES + 1]))
            .expect("oversized chunk request");
        let response = app
            .router
            .clone()
            .oneshot(oversized_chunk)
            .await
            .expect("oversized chunk response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let invalid_manifest = request(
            &app.router,
            "POST",
            "/lix/v1/sync/blob",
            Some(&session_id),
            Some(json!({
                "blobId": digest,
                "sizeBytes": MAX_SYNC_CHUNK_BYTES as u64 + 1,
                "chunks": [{
                    "chunkId": "0".repeat(64),
                    "sizeBytes": MAX_SYNC_CHUNK_BYTES as u64 + 1
                }]
            })),
        )
        .await;
        assert_eq!(invalid_manifest.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn sync_cas_roundtrips_one_chunk_without_a_presence_route() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let bytes = b"one canonical sync chunk";
        let chunk_id = blake3::hash(bytes).to_hex().to_string();
        let manifest = json!({
            "blobId": chunk_id,
            "sizeBytes": bytes.len(),
            "chunks": [{ "chunkId": chunk_id, "sizeBytes": bytes.len() }]
        });

        let missing = request(
            &app.router,
            "POST",
            "/lix/v1/sync/blob",
            Some(&session_id),
            Some(manifest.clone()),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::OK);
        let missing = response_json(missing).await;
        assert_eq!(missing["missingChunkIds"], json!([chunk_id]));

        let put = Request::builder()
            .method("PUT")
            .uri(format!("/lix/v1/sync/chunk?chunkId={chunk_id}"))
            .header(SESSION_ID_HEADER, &session_id)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(bytes.to_vec()))
            .expect("put chunk request");
        let put = app
            .router
            .clone()
            .oneshot(put)
            .await
            .expect("put chunk response");
        assert_eq!(put.status(), StatusCode::NO_CONTENT);

        let registration = request(
            &app.router,
            "POST",
            "/lix/v1/sync/blob",
            Some(&session_id),
            Some(manifest.clone()),
        )
        .await;
        assert_eq!(registration.status(), StatusCode::OK);
        let registration = response_json(registration).await;
        assert!(
            registration["missingChunkIds"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let blob = request(
            &app.router,
            "GET",
            &format!("/lix/v1/sync/blob?blobId={chunk_id}"),
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(blob.status(), StatusCode::OK);
        assert_eq!(response_json(blob).await, manifest);

        let chunk = request(
            &app.router,
            "GET",
            &format!("/lix/v1/sync/chunk?chunkId={chunk_id}"),
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(chunk.status(), StatusCode::OK);
        assert_eq!(
            chunk.headers().get(CONTENT_TYPE),
            Some(&http::HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            chunk.into_body().collect().await.unwrap().to_bytes(),
            &bytes[..]
        );

        let presence = request(
            &app.router,
            "GET",
            &format!("/lix/v1/sync/presence?chunkId={chunk_id}"),
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(presence.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn handshake_rejects_client_controlled_account_identity() {
        let app = app().await;
        let response = request(
            &app.router,
            "GET",
            "/lix/v1?activeAccountId=00000000-0000-7000-8000-000000000001",
            None,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn trusted_account_binding_overrides_creation_and_rejects_cross_account_resume() {
        let app = app().await;
        let create = Request::builder()
            .uri("/lix/v1")
            .body(Body::empty())
            .expect("trusted handshake request");
        let response = app
            .server
            .handle(
                create,
                ServerProtocolContext {
                    principal: ServerProtocolPrincipal::Authenticated {
                        account_id: lix::SYSTEM_ACCOUNT_ID.to_string(),
                        idempotency_scope: "system-test".to_string(),
                    },
                    durable_terminal_storage_notifier: None,
                },
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["activeAccountId"], lix::SYSTEM_ACCOUNT_ID);
        let session_id = body["sessionId"].as_str().expect("session id");

        let resume = Request::builder()
            .uri("/lix/v1")
            .header(SESSION_ID_HEADER, session_id)
            .body(Body::empty())
            .expect("cross-account resume request");
        let response = app
            .server
            .handle(resume, ServerProtocolContext::anonymous())
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            error_code(response).await,
            "LIX_ERROR_PROTOCOL_ACCOUNT_MISMATCH"
        );

        let changed_scope = Request::builder()
            .uri("/lix/v1")
            .header(SESSION_ID_HEADER, session_id)
            .body(Body::empty())
            .expect("changed-scope resume request");
        let response = app
            .server
            .handle(
                changed_scope,
                ServerProtocolContext {
                    principal: ServerProtocolPrincipal::Authenticated {
                        account_id: lix::SYSTEM_ACCOUNT_ID.to_string(),
                        idempotency_scope: "different-system-scope".to_string(),
                    },
                    durable_terminal_storage_notifier: None,
                },
            )
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn sync_push_rejects_a_commit_authored_by_another_account() {
        let app = app().await;
        let principal = ServerProtocolPrincipal::Authenticated {
            account_id: lix::SYSTEM_ACCOUNT_ID.to_string(),
            idempotency_scope: "sync-account-binding".to_string(),
        };
        let handshake = Request::builder()
            .uri("/lix/v1")
            .body(Body::empty())
            .expect("trusted handshake request");
        let response = app
            .server
            .handle(
                handshake,
                ServerProtocolContext {
                    principal: principal.clone(),
                    durable_terminal_storage_notifier: None,
                },
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let session_id = response_json(response).await["sessionId"]
            .as_str()
            .expect("session id")
            .to_owned();

        let foreign_commit = json!({
            "commitId": crate::changelog::CommitId::for_test_label("foreign-sync-author").to_string(),
            "parentCommitIds": [],
            "accountId": lix::ANONYMOUS_ACCOUNT_ID,
            "createdAt": "2026-08-19T00:00:00Z",
            "selectedSourceCommitId": null,
            "members": [],
        });
        let push = Request::builder()
            .method(Method::POST)
            .uri("/lix/v1/sync/push")
            .header(SESSION_ID_HEADER, session_id)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "commits": [foreign_commit],
                    "refUpdates": [],
                }))
                .expect("encode sync push"),
            ))
            .expect("foreign account push request");
        let response = app
            .server
            .handle(
                push,
                ServerProtocolContext {
                    principal,
                    durable_terminal_storage_notifier: None,
                },
            )
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            error_code(response).await,
            "LIX_ERROR_PROTOCOL_ACCOUNT_MISMATCH"
        );
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
    async fn binary_file_upsert_accepts_raw_bytes_and_preserves_execute_response_shape() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let inserted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                "params": [
                    { "kind": "text", "value": "/payload.bin" },
                    { "kind": "blob", "base64": "b2xk" }
                ]
            })),
        )
        .await;
        assert_eq!(inserted.status(), StatusCode::OK);

        let payload = vec![0, 1, 2, 3, 255];
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(payload.clone()))
                    .expect("binary file upsert request"),
            )
            .await
            .expect("binary file upsert response");
        assert_eq!(response.status(), StatusCode::OK);
        let response = response_json(response).await;
        assert_eq!(response["rowsAffected"], 1);
        assert_eq!(response["columns"], json!([]));
        assert_eq!(response["rows"], json!([]));

        let selected = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/payload.bin" }]
            })),
        )
        .await;
        assert_eq!(selected.status(), StatusCode::OK);
        assert_eq!(
            response_json(selected).await["rows"][0][0],
            wire_blob_json(&payload)
        );

        // An empty body is a real empty file, not an absent request payload.
        // The filesystem fast path tombstones a prior blob reference when
        // appropriate and leaves the file itself visible.
        let emptied = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::empty())
                    .expect("empty binary file upsert request"),
            )
            .await
            .expect("empty binary file upsert response");
        assert_eq!(emptied.status(), StatusCode::OK);
        assert_eq!(response_json(emptied).await["rowsAffected"], 1);

        let empty_selected = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/payload.bin" }]
            })),
        )
        .await;
        assert_eq!(empty_selected.status(), StatusCode::OK);
        assert_eq!(
            response_json(empty_selected).await["rows"][0][0],
            wire_blob_json(&[])
        );

        let created_payload = vec![4, 5, 6];
        let created = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fcreated.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(created_payload.clone()))
                    .expect("binary file create request"),
            )
            .await
            .expect("binary file create response");
        assert_eq!(created.status(), StatusCode::OK);
        assert_eq!(response_json(created).await["rowsAffected"], 1);

        let created_selected = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/created.bin" }]
            })),
        )
        .await;
        assert_eq!(created_selected.status(), StatusCode::OK);
        assert_eq!(
            response_json(created_selected).await["rows"][0][0],
            wire_blob_json(&created_payload)
        );
    }

    #[tokio::test]
    async fn binary_file_upsert_resumes_with_content_range_and_publishes_on_final_part() {
        let app = app().await;
        let (first_session, _) = new_session(&app.router).await;
        let first = vec![0x41; FILE_UPLOAD_PART_BYTES];
        let tail = vec![0x42; 9];
        let total = first.len() + tail.len();
        let first_response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fproxy.mov")
                    .header(SESSION_ID_HEADER, &first_session)
                    .header(FILE_UPLOAD_ID_HEADER, "proxy-upload-1")
                    .header(
                        CONTENT_RANGE,
                        format!("bytes 0-{}/{total}", first.len() - 1),
                    )
                    .body(Body::from(first))
                    .expect("first upload part"),
            )
            .await
            .expect("first upload response");
        assert_eq!(first_response.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            first_response.headers()[RANGE],
            format!("bytes=0-{}", FILE_UPLOAD_PART_BYTES - 1)
        );

        // The durable upload receipt is repository state, not session memory.
        let (resumed_session, _) = new_session(&app.router).await;
        let final_response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fproxy.mov")
                    .header(SESSION_ID_HEADER, &resumed_session)
                    .header(FILE_UPLOAD_ID_HEADER, "proxy-upload-1")
                    .header(
                        CONTENT_RANGE,
                        format!("bytes {}-{}/{total}", FILE_UPLOAD_PART_BYTES, total - 1),
                    )
                    .body(Body::from(tail))
                    .expect("final upload part"),
            )
            .await
            .expect("final upload response");
        assert_eq!(final_response.status(), StatusCode::OK);
        assert_eq!(response_json(final_response).await["rowsAffected"], 1);

        let boundary = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fproxy.mov")
                    .header(SESSION_ID_HEADER, &resumed_session)
                    .header(
                        RANGE,
                        format!(
                            "bytes={}-{}",
                            FILE_UPLOAD_PART_BYTES - 2,
                            FILE_UPLOAD_PART_BYTES + 1
                        ),
                    )
                    .body(Body::empty())
                    .expect("boundary range read"),
            )
            .await
            .expect("boundary range response");
        assert_eq!(boundary.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            boundary
                .into_body()
                .collect()
                .await
                .expect("boundary body")
                .to_bytes()
                .as_ref(),
            b"AABB"
        );
    }

    #[tokio::test]
    async fn binary_file_read_returns_raw_bytes_and_distinguishes_empty_and_missing() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let payload = (0..FILE_READ_STREAM_WINDOW_BYTES as usize + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let inserted = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(payload.clone()))
                    .expect("binary file seed request"),
            )
            .await
            .expect("binary file seed response");
        assert_eq!(inserted.status(), StatusCode::OK);

        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::empty())
                    .expect("binary file read request"),
            )
            .await
            .expect("binary file read response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&http::HeaderValue::from_static("no-store"))
        );
        assert_eq!(
            response.headers().get(CONTENT_TYPE),
            Some(&http::HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            response.headers().get(FILE_FOUND_HEADER),
            Some(&http::HeaderValue::from_static("true"))
        );
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .expect("read body")
                .to_bytes()
                .as_ref(),
            payload.as_slice()
        );

        let ranged = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(RANGE, "bytes=1-3")
                    .body(Body::empty())
                    .expect("ranged binary file read request"),
            )
            .await
            .expect("ranged binary file read response");
        assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            ranged.headers().get(ACCEPT_RANGES),
            Some(&http::HeaderValue::from_static("bytes"))
        );
        assert_eq!(
            ranged.headers().get(CONTENT_RANGE),
            Some(
                &http::HeaderValue::from_str(&format!("bytes 1-3/{}", payload.len()))
                    .expect("content range header")
            )
        );
        assert_eq!(
            ranged
                .into_body()
                .collect()
                .await
                .expect("ranged read body")
                .to_bytes()
                .as_ref(),
            &payload[1..4]
        );

        let concurrent = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::empty())
                    .expect("concurrent binary file read request"),
            )
            .await
            .expect("concurrent binary file read response");
        let mut concurrent_body = concurrent.into_body();
        let first_frame = concurrent_body
            .frame()
            .await
            .expect("first stream frame")
            .expect("first stream frame should succeed")
            .into_data()
            .expect("first stream frame should contain data");
        assert_eq!(first_frame.len(), FILE_READ_STREAM_WINDOW_BYTES as usize);

        let replacement = vec![0xee; payload.len()];
        let replaced = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::from(replacement))
                    .expect("concurrent binary file replacement request"),
            )
            .await
            .expect("concurrent binary file replacement response");
        assert_eq!(replaced.status(), StatusCode::OK);
        let stream_error = concurrent_body
            .frame()
            .await
            .expect("changed stream should produce a terminal frame")
            .expect_err("changed stream must not mix file versions");
        assert!(stream_error.to_string().contains("file changed"));

        let emptied = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::empty())
                    .expect("empty binary file update request"),
            )
            .await
            .expect("empty binary file update response");
        assert_eq!(emptied.status(), StatusCode::OK);

        let empty = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::empty())
                    .expect("empty binary file read request"),
            )
            .await
            .expect("empty binary file read response");
        assert_eq!(empty.status(), StatusCode::OK);
        assert_eq!(
            empty.headers().get(FILE_FOUND_HEADER),
            Some(&http::HeaderValue::from_static("true"))
        );
        assert!(
            empty
                .into_body()
                .collect()
                .await
                .expect("empty read body")
                .to_bytes()
                .is_empty()
        );
        let empty_range = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fpayload.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(RANGE, "bytes=0-")
                    .body(Body::empty())
                    .expect("empty ranged binary file read request"),
            )
            .await
            .expect("empty ranged read response");
        assert_eq!(empty_range.status(), StatusCode::BAD_REQUEST);

        let missing = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lix/v1/file?path=%2Fmissing.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::empty())
                    .expect("missing binary file read request"),
            )
            .await
            .expect("missing binary file read response");
        assert_eq!(missing.status(), StatusCode::OK);
        assert_eq!(
            missing.headers().get(FILE_FOUND_HEADER),
            Some(&http::HeaderValue::from_static("false"))
        );
        assert!(
            missing
                .into_body()
                .collect()
                .await
                .expect("missing read body")
                .to_bytes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn binary_file_read_requires_a_live_session_and_valid_path() {
        let app = app().await;
        let missing_session = request(
            &app.router,
            "GET",
            "/lix/v1/file?path=%2Fpayload.bin",
            None,
            None,
        )
        .await;
        assert_eq!(missing_session.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(missing_session).await,
            "LIX_ERROR_PROTOCOL_SESSION_REQUIRED"
        );

        let (session_id, _) = new_session(&app.router).await;
        let invalid_path = request(
            &app.router,
            "GET",
            "/lix/v1/file?path=relative.bin",
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(invalid_path.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(invalid_path).await,
            "LIX_ERROR_PATH_MISSING_LEADING_SLASH"
        );
    }

    #[tokio::test]
    async fn binary_file_upsert_requires_a_live_session_and_valid_path() {
        let app = app().await;
        let missing_session = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Fpayload.bin")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(vec![1_u8]))
                    .expect("missing session request"),
            )
            .await
            .expect("missing session response");
        assert_eq!(missing_session.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(missing_session).await,
            "LIX_ERROR_PROTOCOL_SESSION_REQUIRED"
        );

        let (session_id, _) = new_session(&app.router).await;
        let invalid_path = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=relative.bin")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(vec![1_u8]))
                    .expect("invalid path request"),
            )
            .await
            .expect("invalid path response");
        assert_eq!(invalid_path.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(invalid_path).await,
            "LIX_ERROR_PATH_MISSING_LEADING_SLASH"
        );
    }

    #[tokio::test]
    async fn binary_file_upsert_batch_accepts_raw_frames_and_preserves_execute_response_shape() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let seeded = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                "params": [
                    { "kind": "text", "value": "/batch/updated.bin" },
                    { "kind": "blob", "base64": "b2xk" }
                ]
            })),
        )
        .await;
        assert_eq!(seeded.status(), StatusCode::OK);

        let response = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            binary_file_upsert_batch_body(&[
                ("/batch/updated.bin", b"updated"),
                ("/batch/created.bin", b"created"),
                ("/batch/empty.bin", b""),
            ]),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = response_json(response).await;
        assert_eq!(response["rowsAffected"], 3);
        assert_eq!(response["columns"], json!([]));
        assert_eq!(response["rows"], json!([]));

        for (path, expected) in [
            ("/batch/updated.bin", &b"updated"[..]),
            ("/batch/created.bin", &b"created"[..]),
            ("/batch/empty.bin", &b""[..]),
        ] {
            let selected = request(
                &app.router,
                "POST",
                "/lix/v1/execute",
                Some(&session_id),
                Some(json!({
                    "sql": "SELECT content FROM lix_file WHERE path = $1",
                    "params": [{ "kind": "text", "value": path }]
                })),
            )
            .await;
            assert_eq!(selected.status(), StatusCode::OK);
            assert_eq!(
                response_json(selected).await["rows"][0][0],
                wire_blob_json(expected)
            );
        }
    }

    #[tokio::test]
    async fn binary_file_upsert_batch_requires_a_live_session_and_preserves_engine_path_errors() {
        let app = app().await;
        let missing_session = binary_file_upsert_batch_request(
            &app.router,
            None,
            binary_file_upsert_batch_body(&[("/batch/missing-session.bin", b"payload")]),
        )
        .await;
        assert_eq!(missing_session.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(missing_session).await,
            "LIX_ERROR_PROTOCOL_SESSION_REQUIRED"
        );

        let (session_id, _) = new_session(&app.router).await;
        let invalid_path = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            binary_file_upsert_batch_body(&[
                ("/batch/must-not-write.bin", b"first"),
                ("relative.bin", b"invalid"),
            ]),
        )
        .await;
        assert_eq!(invalid_path.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error_code(invalid_path).await,
            "LIX_ERROR_PATH_MISSING_LEADING_SLASH"
        );

        let selected = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/batch/must-not-write.bin" }]
            })),
        )
        .await;
        assert_eq!(selected.status(), StatusCode::OK);
        assert!(
            response_json(selected).await["rows"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn binary_file_upsert_batch_rejects_malformed_trailing_and_duplicate_frames() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;

        let malformed = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            vec![0, 0, 0, 1, 0, 0],
        )
        .await;
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(malformed).await, "LIX_INVALID_ARGUMENT");

        let mut trailing = binary_file_upsert_batch_body(&[("/batch/trailing.bin", b"payload")]);
        trailing.push(0);
        let trailing =
            binary_file_upsert_batch_request(&app.router, Some(&session_id), trailing).await;
        assert_eq!(trailing.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(trailing).await, "LIX_INVALID_ARGUMENT");

        let duplicate = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            binary_file_upsert_batch_body(&[
                ("/batch/duplicate.bin", b"first"),
                ("/batch/duplicate.bin", b"second"),
            ]),
        )
        .await;
        assert_eq!(duplicate.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(duplicate).await, "LIX_INVALID_ARGUMENT");

        let duplicate_selected = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/batch/duplicate.bin" }]
            })),
        )
        .await;
        assert_eq!(duplicate_selected.status(), StatusCode::OK);
        assert!(
            response_json(duplicate_selected).await["rows"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let empty = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            0_u32.to_be_bytes().to_vec(),
        )
        .await;
        assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(empty).await, "LIX_INVALID_ARGUMENT");

        let over_limit = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            u32::try_from(MAX_BINARY_FILE_UPSERT_BATCH_ENTRIES + 1)
                .expect("test entry count should fit the wire format")
                .to_be_bytes()
                .to_vec(),
        )
        .await;
        assert_eq!(over_limit.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(over_limit).await, "LIX_INVALID_ARGUMENT");
    }

    #[test]
    fn binary_file_upsert_batch_parser_slices_payload_bytes_without_copying_them() {
        let path = "/batch/slice.bin";
        let payload = b"payload";
        let data_offset = size_of::<u32>() * 3 + path.len();
        let body = Bytes::from(binary_file_upsert_batch_body(&[(path, payload)]));
        let expected_data_pointer = body[data_offset..].as_ptr();

        let writes = parse_binary_file_upsert_batch(body).expect("valid batch frame");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, path);
        assert_eq!(writes[0].1.as_ref(), payload);
        assert_eq!(writes[0].1.as_bytes().as_ptr(), expected_data_pointer);
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
    async fn create_checkpoint_returns_the_new_pinned_session_head() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let inserted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-test', 'working')"
            })),
        )
        .await;
        assert_eq!(inserted.status(), StatusCode::OK);

        let created = request(
            &app.router,
            "POST",
            "/lix/v1/checkpoint/create",
            Some(&session_id),
            None,
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);
        let checkpoint_id = response_json(created).await["commitId"]
            .as_str()
            .expect("checkpoint commit id")
            .to_string();

        let head = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT lix_active_branch_commit_id() AS commit_id"
            })),
        )
        .await;
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            response_json(head).await["rows"][0][0],
            json!({ "kind": "text", "value": checkpoint_id })
        );
    }

    #[tokio::test]
    async fn undo_and_redo_mutate_the_pinned_branch() {
        let app = app().await;
        let (session_id, handshake) = new_session(&app.router).await;
        let inserted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('remote-undo', 'yes')"
            })),
        )
        .await;
        assert_eq!(inserted.status(), StatusCode::OK);

        let undone = request(&app.router, "POST", "/lix/v1/undo", Some(&session_id), None).await;
        assert_eq!(undone.status(), StatusCode::OK);
        let undone = response_json(undone).await;
        assert_eq!(undone["branchId"], handshake["activeBranchId"]);
        assert!(undone["targetCommitId"].is_string());
        assert!(undone["inverseCommitId"].is_string());

        let redone = request(&app.router, "POST", "/lix/v1/redo", Some(&session_id), None).await;
        assert_eq!(redone.status(), StatusCode::OK);
        let redone = response_json(redone).await;
        assert_eq!(redone["branchId"], handshake["activeBranchId"]);
        assert_eq!(redone["targetCommitId"], undone["targetCommitId"]);
        assert!(redone["replayCommitId"].is_string());
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
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            ..ServerProtocolOptions::default()
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
    async fn execute_batch_metadata_and_returning_work_in_memory() {
        assert_execute_batch_metadata(Memory::default()).await;
    }

    async fn assert_execute_batch_metadata<S>(storage: S)
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let router = router_with_storage(storage).await;
        let (session_id, _) = new_session(&router).await;
        let response = request(
            &router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    {
                        "label": "same-label",
                        "sql": "INSERT INTO lix_key_value (key, value) VALUES ('adapter-batch', 'one') RETURNING key, value",
                        "params": []
                    },
                    {
                        "label": "same-label",
                        "sql": "UPDATE lix_key_value SET value = 'two' WHERE key = 'adapter-batch' RETURNING key, value",
                        "params": []
                    },
                    {
                        "sql": "SELECT value FROM lix_key_value WHERE key = 'adapter-batch'",
                        "params": []
                    }
                ]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body[0]["statementIndex"], 0);
        assert_eq!(body[1]["statementIndex"], 1);
        assert_eq!(body[2]["statementIndex"], 2);
        assert_eq!(body[0]["label"], "same-label");
        assert_eq!(body[1]["label"], "same-label");
        assert!(body[2].get("label").is_none());
        assert_eq!(body[0]["rowsAffected"], 1);
        assert_eq!(
            body[0]["rows"][0][1],
            json!({ "kind": "jsonb", "value": "one" })
        );
        assert_eq!(
            body[1]["rows"][0][1],
            json!({ "kind": "jsonb", "value": "two" })
        );
        assert_eq!(
            body[2]["rows"][0][0],
            json!({ "kind": "jsonb", "value": "two" })
        );

        let failed = request(
            &router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    {
                        "sql": "INSERT INTO lix_key_value (key, value) VALUES ('adapter-rollback', 'written')",
                        "params": []
                    },
                    { "sql": "SELECT id FROM lix_history('lix_file', 'one', 'two')", "params": [] }
                ]
            })),
        )
        .await;
        assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(failed).await["error"]["details"]["statementIndex"],
            1
        );
        let persisted = request(
            &router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'adapter-rollback'"
            })),
        )
        .await;
        assert_eq!(persisted.status(), StatusCode::OK);
        assert_eq!(
            response_json(persisted).await["rows"][0][0],
            json!({ "kind": "int", "value": 0 })
        );
    }

    #[tokio::test]
    async fn remote_transaction_commit_and_rollback_preserve_one_session_snapshot() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;

        let transaction_id = begin_remote_transaction(&app.router, &session_id).await;
        let staged = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/execute",
            &session_id,
            &transaction_id,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('remote-tx', 'committed')"
            })),
        )
        .await;
        assert_eq!(staged.status(), StatusCode::OK);
        let outside = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({ "sql": "SELECT 1" })),
        )
        .await;
        assert_eq!(outside.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(outside).await["error"]["code"],
            "LIX_INVALID_TRANSACTION_STATE"
        );
        let committed = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/commit",
            &session_id,
            &transaction_id,
            None,
        )
        .await;
        assert_eq!(committed.status(), StatusCode::NO_CONTENT);
        let replayed_commit = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/commit",
            &session_id,
            &transaction_id,
            None,
        )
        .await;
        assert_eq!(replayed_commit.status(), StatusCode::NO_CONTENT);

        let transaction_id = begin_remote_transaction(&app.router, &session_id).await;
        let staged = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/execute",
            &session_id,
            &transaction_id,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('remote-rollback', 'discarded')"
            })),
        )
        .await;
        assert_eq!(staged.status(), StatusCode::OK);
        let rolled_back = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/rollback",
            &session_id,
            &transaction_id,
            None,
        )
        .await;
        assert_eq!(rolled_back.status(), StatusCode::NO_CONTENT);

        let visible = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT key FROM lix_key_value WHERE key IN ('remote-tx', 'remote-rollback') ORDER BY key"
            })),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);
        assert_eq!(
            response_json(visible).await["rows"],
            json!([[{ "kind": "text", "value": "remote-tx" }]])
        );
    }

    #[tokio::test]
    async fn same_base_remote_transactions_compose_disjoint_semantic_writes() {
        let app = app().await;
        let (first, _) = new_session(&app.router).await;
        let (second, _) = new_session(&app.router).await;
        let first_transaction = begin_remote_transaction(&app.router, &first).await;
        let second_transaction = begin_remote_transaction(&app.router, &second).await;
        for (session_id, transaction_id, key) in [
            (&first, &first_transaction, "first-disjoint"),
            (&second, &second_transaction, "second-disjoint"),
        ] {
            let staged = remote_transaction_request(
                &app.router,
                "POST",
                "/lix/v1/transaction/execute",
                session_id,
                transaction_id,
                Some(json!({
                    "sql": "INSERT INTO lix_key_value (key, value) VALUES ($1, $1)",
                    "params": [{ "kind": "text", "value": key }]
                })),
            )
            .await;
            assert_eq!(staged.status(), StatusCode::OK);
        }
        for (session_id, transaction_id) in
            [(&first, &first_transaction), (&second, &second_transaction)]
        {
            let committed = remote_transaction_request(
                &app.router,
                "POST",
                "/lix/v1/transaction/commit",
                session_id,
                transaction_id,
                None,
            )
            .await;
            assert_eq!(committed.status(), StatusCode::NO_CONTENT);
        }

        let visible = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&first),
            Some(json!({
                "sql": "SELECT key FROM lix_key_value WHERE key LIKE '%-disjoint' ORDER BY key"
            })),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);
        assert_eq!(
            response_json(visible).await["rows"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
    }

    #[tokio::test]
    async fn same_base_remote_transactions_converge_with_column_lww() {
        let app = app().await;
        let (first, _) = new_session(&app.router).await;
        let (second, _) = new_session(&app.router).await;
        let first_transaction = begin_remote_transaction(&app.router, &first).await;
        let second_transaction = begin_remote_transaction(&app.router, &second).await;
        for (session_id, transaction_id, value) in [
            (&first, &first_transaction, "first"),
            (&second, &second_transaction, "second"),
        ] {
            let staged = remote_transaction_request(
                &app.router,
                "POST",
                "/lix/v1/transaction/execute",
                session_id,
                transaction_id,
                Some(json!({
                    "sql": "INSERT INTO lix_key_value (key, value) VALUES ('same-base', $1)",
                    "params": [{ "kind": "text", "value": value }]
                })),
            )
            .await;
            assert_eq!(staged.status(), StatusCode::OK);
        }

        let first_commit = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/commit",
            &first,
            &first_transaction,
            None,
        )
        .await;
        assert_eq!(first_commit.status(), StatusCode::NO_CONTENT);
        let second_commit = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/commit",
            &second,
            &second_transaction,
            None,
        )
        .await;
        assert_eq!(second_commit.status(), StatusCode::NO_CONTENT);

        // Ordinary rows no longer surface a stale same-key INSERT as a
        // uniqueness error. The stale transaction is reconciled against the
        // first commit and every session converges on the same ranked value.
        let visible_from_second = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&second),
            Some(json!({
                "sql": "SELECT value FROM lix_key_value WHERE key = 'same-base'",
                "params": []
            })),
        )
        .await;
        assert_eq!(visible_from_second.status(), StatusCode::OK);
        let visible_from_first = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&first),
            Some(json!({
                "sql": "SELECT value FROM lix_key_value WHERE key = 'same-base'",
                "params": []
            })),
        )
        .await;
        assert_eq!(visible_from_first.status(), StatusCode::OK);

        let second_value = response_json(visible_from_second).await["rows"][0][0].clone();
        let first_value = response_json(visible_from_first).await["rows"][0][0].clone();
        assert_eq!(first_value, second_value);
        assert!(
            [
                json!({ "kind": "jsonb", "value": "first" }),
                json!({ "kind": "jsonb", "value": "second" }),
            ]
            .contains(&first_value)
        );
    }

    #[tokio::test]
    async fn execute_batch_rejects_non_finite_results_before_committing() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": [
                    {
                        "sql": "INSERT INTO lix_key_value (key, value) VALUES ('nonfinite-batch', 'written')",
                        "params": []
                    },
                    { "sql": "SELECT 0e0 / 0e0 AS value", "params": [] }
                ]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(response).await, LixError::CODE_TYPE_MISMATCH);

        let persisted = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT key FROM lix_key_value WHERE key = 'nonfinite-batch'"
            })),
        )
        .await;
        assert_eq!(persisted.status(), StatusCode::OK);
        assert_eq!(response_json(persisted).await["rows"], json!([]));
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
                "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
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
                "sql": "SELECT content FROM lix_file WHERE path = $1",
                "params": [{ "kind": "text", "value": "/must-not-exist.bin" }],
            })),
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(response_json(read).await["rows"], json!([]));
    }

    #[tokio::test]
    async fn execute_batch_bounds_aggregate_blob_reconstruction_before_mutation() {
        const TEST_RECONSTRUCTION_LIMIT: usize = 128 * 1024;
        let app = app_with_options(ServerProtocolOptions {
            max_request_body_bytes: TEST_RECONSTRUCTION_LIMIT,
            ..ServerProtocolOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let base = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES];
        let cached = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
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
        let statements = (0..4)
            .map(|index| {
                if index == 0 {
                    json!({
                        "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                        "params": [
                            { "kind": "text", "value": "/must-not-execute.bin" },
                            splice.clone(),
                        ],
                    })
                } else {
                    json!({ "sql": "SELECT $1", "params": [splice.clone()] })
                }
            })
            .collect::<Vec<_>>();
        assert!(result.len() * statements.len() > TEST_RECONSTRUCTION_LIMIT);
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some(json!({
                "statements": statements,
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
                "sql": "SELECT content FROM lix_file WHERE path = $1",
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
        let mut cache = RequestBlobCache::new(Arc::new(RequestBlobCacheBudget::new(
            DEFAULT_MAX_REQUEST_BLOB_CACHE_BYTES,
        )));
        let mut inserted = Vec::new();
        for index in 0..=MAX_REQUEST_BLOB_CACHE_ENTRIES {
            let blob = VerifiedRequestBlob::verify(
                vec![
                    u8::try_from(index).expect("test index should fit");
                    MIN_REQUEST_BLOB_CACHE_BYTES
                ]
                .into(),
            );
            inserted.push(blob.sha256().to_owned());
            cache.insert(CachedRequestBlob { blob });
        }
        assert_eq!(cache.entries.len(), MAX_REQUEST_BLOB_CACHE_ENTRIES);
        assert!(cache.get(&inserted[0]).is_none());
        assert!(
            cache
                .get(&inserted[MAX_REQUEST_BLOB_CACHE_ENTRIES])
                .is_some()
        );

        let too_large =
            VerifiedRequestBlob::verify(vec![0_u8; MAX_REQUEST_BLOB_CACHE_BYTES + 1].into());
        let too_large_sha256 = too_large.sha256().to_owned();
        cache.insert(CachedRequestBlob { blob: too_large });
        assert!(cache.get(&too_large_sha256).is_none());
        assert!(cache.total_bytes <= MAX_REQUEST_BLOB_CACHE_BYTES);

        let too_small =
            VerifiedRequestBlob::verify(vec![0_u8; MIN_REQUEST_BLOB_CACHE_BYTES - 1].into());
        let too_small_sha256 = too_small.sha256().to_owned();
        cache.insert(CachedRequestBlob { blob: too_small });
        assert!(cache.get(&too_small_sha256).is_none());
    }

    #[test]
    fn request_blob_cache_reuses_verified_full_blob_without_payload_clone() {
        let source: Blob = vec![b'a'; MIN_REQUEST_BLOB_CACHE_BYTES].into();
        let source_ptr = source.as_ptr();
        let verified = VerifiedRequestBlob::verify(source);
        let sha256 = verified.sha256().to_owned();
        let mut cache = RequestBlobCache::new(Arc::new(RequestBlobCacheBudget::new(
            DEFAULT_MAX_REQUEST_BLOB_CACHE_BYTES,
        )));

        cache.insert(CachedRequestBlob { blob: verified });

        let cached = cache.get(&sha256).expect("full blob should be retained");
        assert_eq!(cached.blob().as_ptr(), source_ptr);
    }

    #[test]
    fn request_blob_cache_retains_and_rotates_exact_large_file_bases() {
        const EXACT_CSV_BYTES: usize = 10_680_000;
        const _: () = {
            assert!(EXACT_CSV_BYTES <= MAX_REQUEST_BLOB_CACHE_BYTES);
            assert!(EXACT_CSV_BYTES * 2 > MAX_REQUEST_BLOB_CACHE_BYTES);
        };
        let mut cache = RequestBlobCache::new(Arc::new(RequestBlobCacheBudget::new(
            DEFAULT_MAX_REQUEST_BLOB_CACHE_BYTES,
        )));

        let base = VerifiedRequestBlob::verify(vec![b'a'; EXACT_CSV_BYTES].into());
        let base_sha256 = base.sha256().to_owned();
        cache.insert(CachedRequestBlob { blob: base });
        assert_eq!(
            cache.get(&base_sha256).map(|blob| blob.blob().len()),
            Some(EXACT_CSV_BYTES)
        );

        let successor = VerifiedRequestBlob::verify(vec![b'b'; EXACT_CSV_BYTES].into());
        let successor_sha256 = successor.sha256().to_owned();
        cache.insert(CachedRequestBlob { blob: successor });
        assert!(cache.get(&base_sha256).is_none());
        assert_eq!(
            cache.get(&successor_sha256).map(|blob| blob.blob().len()),
            Some(EXACT_CSV_BYTES)
        );
        assert_eq!(cache.total_bytes, EXACT_CSV_BYTES);
    }

    #[test]
    fn request_blob_caches_share_one_repository_byte_budget() {
        let budget = Arc::new(RequestBlobCacheBudget::new(
            MIN_REQUEST_BLOB_CACHE_BYTES * 2,
        ));
        let mut first = RequestBlobCache::new(Arc::clone(&budget));
        let mut second = RequestBlobCache::new(Arc::clone(&budget));
        let mut third = RequestBlobCache::new(Arc::clone(&budget));
        let blob = |marker| {
            let mut bytes = vec![b'x'; MIN_REQUEST_BLOB_CACHE_BYTES];
            bytes[0] = marker;
            VerifiedRequestBlob::verify(bytes.into())
        };
        let first_blob = blob(b'1');
        let first_sha256 = first_blob.sha256().to_owned();
        let second_blob = blob(b'2');
        let second_sha256 = second_blob.sha256().to_owned();
        let third_blob = blob(b'3');
        let third_sha256 = third_blob.sha256().to_owned();

        first.insert(CachedRequestBlob { blob: first_blob });
        second.insert(CachedRequestBlob { blob: second_blob });
        third.insert(CachedRequestBlob {
            blob: third_blob.clone(),
        });
        assert!(first.get(&first_sha256).is_some());
        assert!(second.get(&second_sha256).is_some());
        assert!(
            third.get(&third_sha256).is_none(),
            "a full repository budget must decline another session cache admission"
        );
        assert_eq!(
            budget.total_bytes.load(Ordering::Acquire),
            MIN_REQUEST_BLOB_CACHE_BYTES * 2
        );

        let first_successor = blob(b'4');
        let first_successor_sha256 = first_successor.sha256().to_owned();
        first.insert(CachedRequestBlob {
            blob: first_successor,
        });
        assert!(first.get(&first_sha256).is_none());
        assert!(first.get(&first_successor_sha256).is_some());
        assert_eq!(
            budget.total_bytes.load(Ordering::Acquire),
            MIN_REQUEST_BLOB_CACHE_BYTES * 2,
            "an equal-size local rotation must retain its budget under contention"
        );

        drop(first);
        third.insert(CachedRequestBlob { blob: third_blob });
        assert!(
            third.get(&third_sha256).is_some(),
            "dropping a session cache must release its repository budget"
        );
        assert_eq!(
            budget.total_bytes.load(Ordering::Acquire),
            MIN_REQUEST_BLOB_CACHE_BYTES * 2
        );
    }

    #[test]
    fn request_cache_candidates_share_one_bounded_payload_budget() {
        let mut remaining = MAX_REQUEST_BLOB_CACHE_BYTES;
        let mut candidates = Vec::new();
        let first = vec![b'a'; MAX_REQUEST_BLOB_CACHE_BYTES / 2];
        let first = VerifiedRequestBlob::verify(first.into());
        prepare_cache_candidate(first.clone(), &mut remaining, &mut candidates);
        let after_first = remaining;
        prepare_cache_candidate(first, &mut remaining, &mut candidates);
        assert_eq!(remaining, after_first, "duplicate should reuse candidate");

        let over_remaining = vec![b'b'; after_first + 1];
        prepare_cache_candidate(
            VerifiedRequestBlob::verify(over_remaining.into()),
            &mut remaining,
            &mut candidates,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(remaining, after_first);
    }

    #[tokio::test]
    async fn configured_body_limit_accepts_blobs_larger_than_two_megabytes() {
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
                "sql": "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
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
        let app = app_with_options(ServerProtocolOptions {
            max_request_body_bytes: 1_024,
            ..ServerProtocolOptions::default()
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
    async fn configured_body_limit_rejects_oversized_binary_file_upsert() {
        let app = app_with_options(ServerProtocolOptions {
            max_request_body_bytes: 1_024,
            ..ServerProtocolOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/file/upsert?path=%2Foversized.bin")
                    .header(SESSION_ID_HEADER, session_id)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(vec![0_u8; 2_048]))
                    .expect("oversized binary file upsert request"),
            )
            .await
            .expect("oversized binary file upsert response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn configured_body_limit_rejects_oversized_binary_file_upsert_batch() {
        let app = app_with_options(ServerProtocolOptions {
            max_request_body_bytes: 1_024,
            ..ServerProtocolOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let oversized = vec![0_u8; 2_048];
        let response = binary_file_upsert_batch_request(
            &app.router,
            Some(&session_id),
            binary_file_upsert_batch_body(&[("/batch/oversized.bin", &oversized)]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn encoded_requests_must_be_decompressed_by_the_host() {
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
                    .header(CONTENT_TYPE, "application/json")
                    .header(http::header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(gzip(request_body.as_bytes())))
                    .expect("compressed request"),
            )
            .await
            .expect("compressed response");

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn json_routes_require_a_json_content_type() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = app
            .router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/execute")
                    .header(SESSION_ID_HEADER, session_id)
                    .header(IDEMPOTENCY_KEY_HEADER, "content-type-test")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from(r#"{"sql":"SELECT 1"}"#))
                    .expect("plain-text JSON request"),
            )
            .await
            .expect("plain-text JSON response");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            error_code(response).await,
            "LIX_ERROR_UNSUPPORTED_CONTENT_TYPE"
        );
    }

    #[tokio::test]
    async fn encoded_requests_are_rejected_before_body_dispatch() {
        let app = app_with_options(ServerProtocolOptions {
            max_request_body_bytes: 1_024,
            ..ServerProtocolOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let response = app
            .router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/execute")
                    .header(SESSION_ID_HEADER, session_id)
                    .header(CONTENT_TYPE, "application/json")
                    .header(http::header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(vec![0_u8; 2_048]))
                    .expect("compressed request"),
            )
            .await
            .expect("compressed response");

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn protocol_returns_uncompressed_responses_for_host_adaptation() {
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
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .header(CONTENT_TYPE, "application/json");
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
        assert!(
            response
                .headers()
                .get(http::header::CONTENT_ENCODING)
                .is_none()
        );
        assert_eq!(
            response_json(response).await["rows"][0][0]["value"],
            "x".repeat(64 * 1024)
        );

        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/execute")
                    .header(SESSION_ID_HEADER, &session_id)
                    .header(http::header::ACCEPT_ENCODING, "zstd")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(request_body.to_string()))
                    .expect("zstd response request"),
            )
            .await
            .expect("zstd response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(http::header::CONTENT_ENCODING)
                .is_none()
        );
        assert_eq!(
            response_json(response).await["rows"][0][0]["value"],
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
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .header(CONTENT_TYPE, "application/json");
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
            response.headers().get(CONTENT_TYPE),
            Some(&http::HeaderValue::from_static("text/event-stream"))
        );
        assert!(
            response
                .headers()
                .get(http::header::CONTENT_ENCODING)
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

    #[tokio::test]
    async fn multiplex_observe_fans_one_identical_snapshot_to_each_subscription() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let response = request(
            &app.router,
            "POST",
            "/lix/v1/observe/multiplex",
            Some(&session_id),
            Some(json!({
                "subscriptions": [
                    {
                        "id": "first",
                        "sql": "SELECT $1 AS value",
                        "params": [{ "kind": "text", "value": "shared" }]
                    },
                    {
                        "id": "second",
                        "sql": "SELECT $1 AS value",
                        "params": [{ "kind": "text", "value": "shared" }]
                    }
                ]
            })),
        )
        .await;
        let mut events = TestSseStream::new(response);
        let first = events.next().await;
        let second = events.next().await;

        assert_eq!(
            [
                first["subscriptionId"].as_str(),
                second["subscriptionId"].as_str()
            ],
            [Some("first"), Some("second")]
        );
        for event in [first, second] {
            assert_eq!(event["sequence"], 0);
            assert_eq!(event["result"]["rows"][0][0]["value"], "shared");
        }
    }

    #[test]
    fn multiplex_observe_groups_identical_queries_for_shared_fanout() {
        let subscriptions = vec![
            MultiplexObserveSubscription {
                id: Some("first".to_owned()),
                sql: Some("SELECT $1".to_owned()),
                params: vec![WireValue::Text {
                    value: "same".to_owned(),
                }],
            },
            MultiplexObserveSubscription {
                id: Some("second".to_owned()),
                sql: Some("SELECT $1".to_owned()),
                params: vec![WireValue::Text {
                    value: "same".to_owned(),
                }],
            },
            MultiplexObserveSubscription {
                id: Some("different".to_owned()),
                sql: Some("SELECT $1".to_owned()),
                params: vec![WireValue::Text {
                    value: "other".to_owned(),
                }],
            },
        ];

        let groups = group_multiplex_subscriptions(subscriptions).expect("valid subscriptions");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].subscription_ids, ["first", "second"]);
        assert_eq!(groups[1].subscription_ids, ["different"]);
    }

    fn point_blob_event(sequence: u64, bytes: Vec<u8>) -> ObserveEvent {
        ObserveEvent {
            sequence,
            mutation_sequence: sequence,
            rows: ExecuteResult::from_rows(
                vec!["content".to_string()],
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

    fn expect_blob_splice(delta: ObserveDelta) -> SingleBlobSplice {
        match delta {
            ObserveDelta::SingleBlobSplice(delta) => delta,
            ObserveDelta::RowSplice(_) => panic!("expected a blob splice"),
        }
    }

    fn expect_row_splice(delta: ObserveDelta) -> RowSplice {
        match delta {
            ObserveDelta::SingleBlobSplice(_) => panic!("expected a row splice"),
            ObserveDelta::RowSplice(delta) => delta,
        }
    }

    fn tabular_event(sequence: u64, columns: Vec<String>, rows: Vec<Vec<Value>>) -> ObserveEvent {
        ObserveEvent {
            sequence,
            mutation_sequence: sequence,
            rows: ExecuteResult::from_rows(columns, rows),
        }
    }

    fn numbered_rows(count: usize) -> Vec<Vec<Value>> {
        (0..count)
            .map(|index| vec![Value::Text(format!("row-{index:03}"))])
            .collect()
    }

    fn apply_row_splice(base: &[Vec<Value>], delta: RowSplice) -> Vec<Vec<Value>> {
        let prefix = usize::try_from(delta.prefix_rows).expect("prefix should fit");
        let delete = usize::try_from(delta.delete_rows).expect("delete count should fit");
        let insert = delta
            .insert_rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(WireValue::try_into_engine)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("row splice values should decode")
            })
            .collect::<Vec<_>>();
        let mut next = Vec::with_capacity(prefix + insert.len() + base.len() - prefix - delete);
        next.extend_from_slice(&base[..prefix]);
        next.extend(insert);
        next.extend_from_slice(&base[prefix + delete..]);
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

        assert_eq!(
            point_blob_bytes(&base.rows)
                .expect("blob base should retain its blob")
                .as_ptr(),
            event_ptr
        );
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
            apply_blob_splice(
                &initial,
                expect_blob_splice(payload.delta.expect("replacement splice")),
            ),
            replaced
        );
        base = next_base;

        let mut inserted = replaced.clone();
        inserted.splice(40_000..40_000, [b'x'; 32]);
        let (payload, next_base) =
            multiplex_observe_payload(point_blob_event(2, inserted.clone()), base.as_ref())
                .expect("insert delta");
        assert_eq!(
            apply_blob_splice(
                &replaced,
                expect_blob_splice(payload.delta.expect("insert splice")),
            ),
            inserted
        );
        base = next_base;

        let mut deleted = inserted.clone();
        deleted.drain(60_000..60_032);
        let (payload, _) =
            multiplex_observe_payload(point_blob_event(3, deleted.clone()), base.as_ref())
                .expect("delete delta");
        assert_eq!(
            apply_blob_splice(
                &inserted,
                expect_blob_splice(payload.delta.expect("delete splice")),
            ),
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
            apply_blob_splice(
                &replacement,
                expect_blob_splice(payload.delta.expect("localized splice")),
            ),
            localized
        );
    }

    #[test]
    fn multiplex_row_delta_roundtrips_replace_insert_and_delete() {
        let initial = numbered_rows(64);
        let (_, mut base) = multiplex_observe_payload(
            tabular_event(0, vec!["value".to_string()], initial.clone()),
            None,
        )
        .expect("initial payload");

        let mut replaced = initial.clone();
        replaced[32] = vec![Value::Text("replacement".to_string())];
        let (payload, next_base) = multiplex_observe_payload(
            tabular_event(1, vec!["value".to_string()], replaced.clone()),
            base.as_ref(),
        )
        .expect("replacement delta");
        assert_eq!(
            apply_row_splice(
                &initial,
                expect_row_splice(payload.delta.expect("replacement splice")),
            ),
            replaced
        );
        base = next_base;

        let mut inserted = replaced.clone();
        inserted.insert(20, vec![Value::Text("inserted".to_string())]);
        let (payload, next_base) = multiplex_observe_payload(
            tabular_event(2, vec!["value".to_string()], inserted.clone()),
            base.as_ref(),
        )
        .expect("insert delta");
        assert_eq!(
            apply_row_splice(
                &replaced,
                expect_row_splice(payload.delta.expect("insert splice")),
            ),
            inserted
        );
        base = next_base;

        let mut deleted = inserted.clone();
        deleted.remove(40);
        let (payload, _) = multiplex_observe_payload(
            tabular_event(3, vec!["value".to_string()], deleted.clone()),
            base.as_ref(),
        )
        .expect("delete delta");
        assert_eq!(
            apply_row_splice(
                &inserted,
                expect_row_splice(payload.delta.expect("delete splice")),
            ),
            deleted
        );
    }

    #[test]
    fn multiplex_row_delta_falls_back_for_noop_metadata_and_large_changes() {
        let initial = numbered_rows(64);
        let (_, base) = multiplex_observe_payload(
            tabular_event(0, vec!["value".to_string()], initial.clone()),
            None,
        )
        .expect("initial payload");

        let (payload, _) = multiplex_observe_payload(
            tabular_event(1, vec!["value".to_string()], initial.clone()),
            base.as_ref(),
        )
        .expect("no-op payload");
        assert!(payload.result.is_some());
        assert!(payload.delta.is_none());

        let (payload, _) = multiplex_observe_payload(
            tabular_event(1, vec!["other".to_string()], initial.clone()),
            base.as_ref(),
        )
        .expect("metadata fallback payload");
        assert!(payload.result.is_some());
        assert!(payload.delta.is_none());

        let mut mostly_replaced = initial;
        for row in &mut mostly_replaced[..58] {
            *row = vec![Value::Text("changed".to_string())];
        }
        let (payload, _) = multiplex_observe_payload(
            tabular_event(1, vec!["value".to_string()], mostly_replaced),
            base.as_ref(),
        )
        .expect("large replacement payload");
        assert!(payload.result.is_some());
        assert!(payload.delta.is_none());
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
                let mut repeated_samples = Vec::with_capacity(SAMPLES);
                let mut shared_samples = Vec::with_capacity(SAMPLES);
                for _ in 0..SAMPLES {
                    let started = Instant::now();
                    for base in &bases {
                        black_box(
                            multiplex_observe_payload(event.clone(), Some(base))
                                .expect("delta payload"),
                        );
                    }
                    repeated_samples.push(started.elapsed());

                    let started = Instant::now();
                    let (payload, _) = multiplex_observe_payload(event.clone(), bases.first())
                        .expect("shared delta payload");
                    let payload = Arc::new(payload);
                    for _ in 0..fanout {
                        black_box(Arc::clone(&payload));
                    }
                    shared_samples.push(started.elapsed());
                }
                repeated_samples.sort_unstable();
                shared_samples.sort_unstable();
                let repeated_p50 = repeated_samples[SAMPLES / 2];
                let repeated_p95 = repeated_samples[SAMPLES * 95 / 100];
                let shared_p50 = shared_samples[SAMPLES / 2];
                let shared_p95 = shared_samples[SAMPLES * 95 / 100];
                let total_bytes = u32::try_from(size_mib * 1024 * 1024 * fanout)
                    .expect("diagnostic byte count should fit u32");
                let throughput = |elapsed: Duration| {
                    f64::from(total_bytes) / elapsed.as_secs_f64() / (1024.0 * 1024.0)
                };
                eprintln!(
                    "observe_fanout size_mib={size_mib} subscribers={fanout} repeated_p50_us={} repeated_p95_us={} shared_p50_us={} shared_p95_us={} speedup_p50={:.2} logical_mib_s_p50={:.1}",
                    repeated_p50.as_micros(),
                    repeated_p95.as_micros(),
                    shared_p50.as_micros(),
                    shared_p95.as_micros(),
                    repeated_p50.as_secs_f64() / shared_p50.as_secs_f64(),
                    throughput(shared_p50),
                );
            }
        }
    }

    #[tokio::test]
    async fn execute_keeps_sql_span_under_protocol_request_on_native_runtime() {
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
            .find(|span| {
                span.name == "lix.sql.query" && span.parent.as_ref() == Some(&protocol_span_id)
            })
            .expect("SQL span under protocol request");
        assert_eq!(sql_span.parent.as_ref(), Some(&protocol_span_id));
    }

    #[tokio::test]
    async fn idle_timeout_expires_a_session_with_gone() {
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 4,
            session_idle_timeout: Duration::ZERO,
            ..ServerProtocolOptions::default()
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

        let expired = app_with_options(ServerProtocolOptions {
            session_idle_timeout: Duration::ZERO,
            ..ServerProtocolOptions::default()
        })
        .await;
        let (_session_id, _) = new_session(&expired.router).await;
        assert!(expired.server.is_idle());
    }

    #[tokio::test]
    async fn pending_session_open_reservations_are_bounded_and_cancel_safe() {
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            ..ServerProtocolOptions::default()
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
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            ..ServerProtocolOptions::default()
        })
        .await;
        let first = app
            .server
            .create_session(None, None, None, None)
            .await
            .expect("open first session");
        let first_session_id = first.session_id.clone();
        let first_record = Arc::clone(&first.record);
        drop(first);

        // Keep eviction cleanup blocked after the replacement is registered.
        // Shutdown must continue to track the whole create operation, not only
        // the child open and registry mutation.
        let first_transactions = first_record.transactions.lock().await;
        let branch_id = app
            .server
            .inner
            .root
            .active_branch_id()
            .await
            .expect("active branch");
        let server = app.server.clone();
        let replacement = tokio::spawn(async move {
            server
                .create_session(Some(branch_id), None, None, None)
                .await
        });

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

        drop(first_transactions);
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
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open Lix"),
        );
        let branch_id = root.active_branch_id().await.expect("active branch");
        let server = LixServerProtocol::with_options(
            root,
            ServerProtocolOptions {
                max_sessions: SESSION_COUNT,
                ..ServerProtocolOptions::default()
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
                    .create_session(Some(branch_id), None, None, None)
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
            let app = app_with_options(ServerProtocolOptions {
                max_sessions: OPERATIONS + 16,
                ..ServerProtocolOptions::default()
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
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ServerProtocolOptions::default()
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
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ServerProtocolOptions::default()
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
    async fn active_remote_transaction_pins_session_until_terminal_request() {
        let app = app_with_options(ServerProtocolOptions {
            max_sessions: 1,
            session_idle_timeout: Duration::from_mins(1),
            ..ServerProtocolOptions::default()
        })
        .await;
        let (session_id, _) = new_session(&app.router).await;
        let transaction_id = begin_remote_transaction(&app.router, &session_id).await;
        let record = app
            .server
            .inner
            .registry
            .lock()
            .await
            .sessions
            .get(&session_id)
            .cloned()
            .expect("transaction session should remain registered");
        *record
            .last_used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Instant::now() - Duration::from_mins(2);

        let at_capacity = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(at_capacity.status(), StatusCode::SERVICE_UNAVAILABLE);
        let rolled_back = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/rollback",
            &session_id,
            &transaction_id,
            None,
        )
        .await;
        assert_eq!(rolled_back.status(), StatusCode::NO_CONTENT);

        let replacement = request(&app.router, "GET", "/lix/v1", None, None).await;
        assert_eq!(replacement.status(), StatusCode::OK);
    }

    #[test]
    fn remote_transaction_pin_is_exclusive_and_releases_on_drop() {
        let activity = Arc::new(SessionActivity::default());
        let pin = RemoteTransactionPin::acquire(Arc::clone(&activity)).expect("first pin opens");
        assert!(activity.transaction_is_active());
        let error = RemoteTransactionPin::acquire(Arc::clone(&activity))
            .expect_err("a second transaction pin must be rejected");
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

        drop(pin);

        assert!(!activity.transaction_is_active());
        let replacement =
            RemoteTransactionPin::acquire(Arc::clone(&activity)).expect("released pin reopens");
        drop(replacement);
        assert!(!activity.transaction_is_active());
    }

    #[test]
    fn session_activity_keeps_lease_count_and_transaction_pin_coherent() {
        let activity = Arc::new(SessionActivity::default());
        activity.acquire_lease();
        let transaction =
            RemoteTransactionPin::acquire(Arc::clone(&activity)).expect("transaction pin opens");
        activity.acquire_lease();

        assert_eq!(activity.lease_count(), 2);
        assert!(activity.transaction_is_active());
        assert!(!activity.is_idle());

        activity.release_lease();
        drop(transaction);
        assert_eq!(activity.lease_count(), 1);
        assert!(!activity.transaction_is_active());
        assert!(!activity.is_idle());

        activity.release_lease();
        assert!(activity.is_idle());
    }

    #[tokio::test]
    async fn server_shutdown_discards_abandoned_remote_transaction_and_session() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let transaction_id = begin_remote_transaction(&app.router, &session_id).await;
        let staged = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/execute",
            &session_id,
            &transaction_id,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('abandoned', 'staged')"
            })),
        )
        .await;
        assert_eq!(staged.status(), StatusCode::OK);
        let visible = app
            .server
            .inner
            .root
            .execute(
                "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'abandoned'",
                &[],
            )
            .await
            .expect("root should read committed state");
        assert_eq!(visible.rows()[0].get::<i64>("count").unwrap(), 0);

        app.server.close().await.expect("server should close");
        assert!(app.server.inner.registry.lock().await.sessions.is_empty());
    }

    #[tokio::test]
    async fn cancelled_cancellable_read_releases_session_for_close_and_replacement() {
        let storage = BlockingReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::with_options(
            root,
            ServerProtocolOptions {
                max_sessions: 1,
                session_idle_timeout: Duration::from_mins(1),
                ..ServerProtocolOptions::default()
            },
        )
        .expect("protocol server");
        let router = handler(server.clone());
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let session_id = lease.session_id.clone();
        drop(lease);

        storage.block_next_read();
        let read_router = router.clone();
        let read_session_id = session_id.clone();
        let operation = tokio::spawn(async move {
            request(
                &read_router,
                "POST",
                "/lix/v1/execute",
                Some(&read_session_id),
                Some(json!({ "sql": "SELECT 1", "params": [] })),
            )
            .await
        });
        storage.wait_for_blocked_read().await;

        operation.abort();
        assert!(
            operation
                .await
                .expect_err("outer HTTP-equivalent future was cancelled")
                .is_cancelled()
        );

        let close =
            tokio::time::timeout(Duration::from_secs(1), server.delete_session(&session_id)).await;
        // Keep a failing regression self-cleaning: if cancellation ever stops
        // reaching storage, release the blocked read before asserting below.
        storage.release_blocked_read();
        close
            .expect("cancelled read must release the session read lock")
            .expect("close cancelled read session");

        let (replacement, _) = new_session(&router).await;
        let retry = request(
            &router,
            "POST",
            "/lix/v1/execute",
            Some(&replacement),
            Some(json!({ "sql": "SELECT 1", "params": [] })),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn resumed_handshake_uses_cached_session_identity_and_closes() {
        let storage = BlockingReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server.clone());
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let session_id = lease.session_id.clone();
        drop(lease);

        storage.block_next_read();
        let resumed = tokio::time::timeout(
            Duration::from_secs(1),
            request(&router, "GET", "/lix/v1", Some(&session_id), None),
        )
        .await
        .expect("resumed handshake should use cached session identity");
        assert_eq!(resumed.status(), StatusCode::OK);
        storage.assert_next_read_remains_armed_and_disarm();

        tokio::time::timeout(Duration::from_secs(1), server.delete_session(&session_id))
            .await
            .expect("cached handshake must release the session lease")
            .expect("close resumed handshake session");
    }

    #[tokio::test]
    async fn cancelled_raw_file_read_releases_session_for_close() {
        let storage = BlockingReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server.clone());
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let session_id = lease.session_id.clone();
        drop(lease);

        storage.block_next_read();
        let read_router = router.clone();
        let read_session_id = session_id.clone();
        let operation = tokio::spawn(async move {
            request(
                &read_router,
                "GET",
                "/lix/v1/file?path=%2Fpayload.bin",
                Some(&read_session_id),
                None,
            )
            .await
        });
        storage.wait_for_blocked_read().await;

        operation.abort();
        assert!(
            operation
                .await
                .expect_err("outer HTTP-equivalent future was cancelled")
                .is_cancelled()
        );

        let close =
            tokio::time::timeout(Duration::from_secs(1), server.delete_session(&session_id)).await;
        // Keep a failing regression self-cleaning: if cancellation ever stops
        // reaching storage, release the blocked read before asserting below.
        storage.release_blocked_read();
        close
            .expect("cancelled file read must release the session read lock")
            .expect("close cancelled file read session");
    }

    #[tokio::test]
    async fn cancelled_durable_operation_reports_terminal_storage_after_caller_cancellation() {
        let storage = BlockingFencedWriteStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::with_options(
            root,
            ServerProtocolOptions {
                max_sessions: 1,
                session_idle_timeout: Duration::from_mins(1),
                ..ServerProtocolOptions::default()
            },
        )
        .expect("protocol server");
        let router = handler(server.clone());
        let lease = server
            .create_session(None, None, None, None)
            .await
            .expect("session lease");
        let session_id = lease.session_id.clone();
        let record = Arc::clone(&lease.record);
        drop(lease);
        let (notifier, signal) = durable_terminal_storage_signal();
        storage.block_next_write();
        let operation_router = router.clone();
        let operation_session_id = session_id.clone();
        let operation = tokio::spawn(async move {
            operation_router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/lix/v1/execute")
                        .header(SESSION_ID_HEADER, operation_session_id)
                        .header(IDEMPOTENCY_KEY_HEADER, "detached-write")
                        .header(CONTENT_TYPE, "application/json")
                        .extension(notifier)
                        .body(Body::from(
                            json!({
                                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('detached-write', 'value')"
                            })
                            .to_string(),
                        ))
                        .expect("durable execute request"),
                )
                .await
                .expect("protocol router response")
        });
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_blocked_write())
            .await
            .expect("durable operation should begin its write");

        operation.abort();
        assert!(
            operation
                .await
                .expect_err("outer HTTP-equivalent future was cancelled")
                .is_cancelled()
        );
        let at_capacity = request(&router, "GET", "/lix/v1", None, None).await;
        assert_eq!(at_capacity.status(), StatusCode::SERVICE_UNAVAILABLE);

        storage.release_blocked_write();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), signal.wait_for_terminal_storage(),)
                .await
                .expect("detached terminal result should wake the request observer"),
            "the detached durable operation should preserve its terminal storage result"
        );
        while record.lease_count() != 0 {
            tokio::task::yield_now().await;
        }
        let replacement = request(&router, "GET", "/lix/v1", None, None).await;
        assert_eq!(replacement.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cancelled_remote_commit_releases_transaction_pin_after_detached_work() {
        let storage = BlockingFencedWriteStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::with_options(
            root,
            ServerProtocolOptions {
                max_sessions: 1,
                session_idle_timeout: Duration::from_mins(1),
                ..ServerProtocolOptions::default()
            },
        )
        .expect("protocol server");
        let router = handler(server.clone());
        let (session_id, _) = new_session(&router).await;
        let transaction_id = begin_remote_transaction(&router, &session_id).await;
        let staged = remote_transaction_request(
            &router,
            "POST",
            "/lix/v1/transaction/execute",
            &session_id,
            &transaction_id,
            Some(json!({
                "sql": "INSERT INTO lix_key_value (key, value) VALUES ('cancelled-commit', 'value')"
            })),
        )
        .await;
        assert_eq!(staged.status(), StatusCode::OK);
        let record = server
            .inner
            .registry
            .lock()
            .await
            .sessions
            .get(&session_id)
            .cloned()
            .expect("transaction session remains registered");

        storage.block_next_write();
        let commit_router = router.clone();
        let commit_session_id = session_id.clone();
        let commit_transaction_id = transaction_id.clone();
        let commit = tokio::spawn(async move {
            remote_transaction_request(
                &commit_router,
                "POST",
                "/lix/v1/transaction/commit",
                &commit_session_id,
                &commit_transaction_id,
                None,
            )
            .await
        });
        storage.wait_for_blocked_write().await;

        commit.abort();
        assert!(
            commit
                .await
                .expect_err("outer commit request was cancelled")
                .is_cancelled()
        );
        assert_eq!(
            request(&router, "GET", "/lix/v1", None, None)
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "detached storage work must keep its operation lease"
        );

        storage.release_blocked_write();
        while record.lease_count() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(!record.activity.transaction_is_active());
        assert_eq!(
            request(&router, "GET", "/lix/v1", None, None)
                .await
                .status(),
            StatusCode::OK,
            "dropped transaction state must release the lifecycle pin"
        );
    }

    #[tokio::test]
    async fn cancelled_remote_transaction_statement_rolls_back_instead_of_replaying() {
        let app = app().await;
        let (session_id, _) = new_session(&app.router).await;
        let transaction_id = begin_remote_transaction(&app.router, &session_id).await;
        let record = app
            .server
            .inner
            .registry
            .lock()
            .await
            .sessions
            .get(&session_id)
            .cloned()
            .expect("transaction session remains registered");
        let registry_guard = record.transactions.lock().await;

        let router = app.router.clone();
        let request_session_id = session_id.clone();
        let request_transaction_id = transaction_id.clone();
        let statement = tokio::spawn(async move {
            remote_transaction_request(
                &router,
                "POST",
                "/lix/v1/transaction/execute",
                &request_session_id,
                &request_transaction_id,
                Some(json!({
                    "sql": "INSERT INTO lix_key_value (key, value) VALUES ('cancelled-statement', 'must-not-commit')"
                })),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while record.lease_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached transaction statement should retain its own lease");

        statement.abort();
        assert!(
            statement
                .await
                .expect_err("outer transaction statement request was cancelled")
                .is_cancelled()
        );
        drop(registry_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if record
                    .transactions
                    .lock()
                    .await
                    .completed
                    .iter()
                    .any(|completed| completed.id == transaction_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled statement should finish rollback");

        let commit = remote_transaction_request(
            &app.router,
            "POST",
            "/lix/v1/transaction/commit",
            &session_id,
            &transaction_id,
            None,
        )
        .await;
        assert_eq!(commit.status(), StatusCode::BAD_REQUEST);

        let result = request(
            &app.router,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some(json!({
                "sql": "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'cancelled-statement'"
            })),
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(
            response_json(result).await["rows"][0][0],
            json!({ "kind": "int", "value": 0 })
        );
    }

    #[tokio::test]
    async fn cancelled_branch_switch_reports_terminal_storage_after_caller_cancellation() {
        let storage = BlockingFencedBranchControlReadStorage::new();
        let root = Arc::new(
            open_lix()
                .with_storage(storage.clone())
                .as_protocol_root()
                .await
                .expect("open lix"),
        );
        let server = LixServerProtocol::new(root);
        let router = handler(server.clone());
        let (session_id, _) = new_session(&router).await;
        let created = request(
            &router,
            "POST",
            "/lix/v1/branch/create",
            Some(&session_id),
            Some(json!({ "name": "Detached switch target" })),
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);
        let branch_id = response_json(created).await["id"]
            .as_str()
            .expect("created branch id")
            .to_string();

        let (notifier, signal) = durable_terminal_storage_signal();
        let lease = server
            .lease(&session_id, Some(notifier))
            .await
            .expect("session lease");
        storage.block_next_branch_control_read();
        let operation =
            tokio::spawn(
                async move { lease.switch_branch(SwitchBranchOptions { branch_id }).await },
            );
        tokio::time::timeout(
            Duration::from_secs(1),
            storage.wait_for_blocked_branch_control_read(),
        )
        .await
        .expect(
            "branch switch should begin its authoritative branch-control read. \
             If this timed out, check BRANCH_HEAD_CONTROL_SPACE_ID against \
             lix::registered_spaces::BRANCH_HEAD_CONTROL_SPACE before concluding \
             the read is gone: a gate that stops recognising the space presents \
             exactly like a read that stopped happening",
        );

        operation.abort();
        assert!(
            operation
                .await
                .expect_err("outer HTTP-equivalent future was cancelled")
                .is_cancelled()
        );

        storage.release_blocked_branch_control_read();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), signal.wait_for_terminal_storage(),)
                .await
                .expect("detached branch switch terminal result should wake the request observer"),
            "the detached branch switch should preserve its terminal storage result"
        );
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
        let lix = open_lix().as_protocol_root().await.expect("open lix");
        let result = LixServerProtocol::with_options(
            Arc::new(lix),
            ServerProtocolOptions {
                max_sessions: 0,
                session_idle_timeout: Duration::from_secs(1),
                ..ServerProtocolOptions::default()
            },
        );
        let Err(error) = result else {
            panic!("zero capacity must be rejected");
        };
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let result = LixServerProtocol::with_options(
            Arc::new(
                open_lix()
                    .as_protocol_root()
                    .await
                    .expect("open second lix"),
            ),
            ServerProtocolOptions {
                max_request_blob_cache_bytes: 0,
                ..ServerProtocolOptions::default()
            },
        );
        let Err(error) = result else {
            panic!("zero request blob cache capacity must be rejected");
        };
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
}
