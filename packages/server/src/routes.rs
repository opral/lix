use crate::LixRuntimeManager;
use crate::store::{LixRuntimeError, LixService};
use crate::telemetry::InFlightSqlRegistry;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use lix_sdk::server_protocol::{self, ServerProtocolContext, ServerProtocolPrincipal};
use serde::Serialize;
use serde_json::json;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tower_http::{
    compression::{
        CompressionLayer, CompressionLevel,
        predicate::{DefaultPredicate, NotForContentType, Predicate as _, SizeAbove},
    },
    decompression::RequestDecompressionLayer,
};
use tracing::Instrument as _;

const TRUSTED_ACCOUNT_ID_HEADER: &str = "x-lix-account-id";
const TRUSTED_IDEMPOTENCY_SCOPE_HEADER: &str = "x-lix-idempotency-scope";
const MAX_TRUSTED_PRINCIPAL_BYTES: usize = 255;
const MIN_COMPRESSION_BODY_BYTES: u16 = 32 * 1024;

#[derive(Debug)]
struct TrustedRequestPrincipal {
    account_id: String,
    idempotency_scope: String,
}

#[derive(Clone)]
struct AppState {
    manager: Arc<LixRuntimeManager>,
    internal_token: Option<Arc<str>>,
    protocol_timeout: Duration,
    request_id_key: [u8; 32],
    request_sequence: Arc<AtomicU64>,
    in_flight_sql: InFlightSqlRegistry,
}

struct TracedBody {
    inner: Body,
    span: tracing::Span,
}

/// Keeps the Lix's one shared Engine alive until a storage-backed stream ends.
/// Dropping the request-scoped service lease as soon as headers are returned
/// would let LRU eviction open a second Engine while an SSE observation or
/// snapshot download from the first Engine is still active. Buffered protocol
/// bodies do not need this lease and must not delay terminal-runtime recovery.
struct LixLeaseBody {
    inner: Body,
    lix: Option<Arc<LixService>>,
}

enum CancellationRecoveryMode {
    WaitForTerminalStorage(server_protocol::DurableTerminalStorageSignal),
    RecoverOnDrop,
    Disarmed,
}

/// Recovers the exact runtime if an HTTP future is cancelled after it has
/// started protocol work. Terminal storage results are reported from both
/// durable work and cancellable reads; cancellation itself remains nonterminal.
struct CancellationRecovery {
    mode: CancellationRecoveryMode,
    manager: Arc<LixRuntimeManager>,
    lix_id: String,
    service: Arc<LixService>,
}

impl CancellationRecovery {
    fn new(
        signal: server_protocol::DurableTerminalStorageSignal,
        manager: Arc<LixRuntimeManager>,
        lix_id: String,
        service: Arc<LixService>,
    ) -> Self {
        Self {
            mode: CancellationRecoveryMode::WaitForTerminalStorage(signal),
            manager,
            lix_id,
            service,
        }
    }

    fn recover_on_drop(&mut self) {
        self.mode = CancellationRecoveryMode::RecoverOnDrop;
    }

    fn disarm(&mut self) {
        self.mode = CancellationRecoveryMode::Disarmed;
    }
}

impl Drop for CancellationRecovery {
    fn drop(&mut self) {
        let mode = std::mem::replace(&mut self.mode, CancellationRecoveryMode::Disarmed);
        let manager = Arc::clone(&self.manager);
        let lix_id = self.lix_id.clone();
        let service = Arc::clone(&self.service);
        match mode {
            CancellationRecoveryMode::WaitForTerminalStorage(signal) => {
                tokio::spawn(async move {
                    if signal.wait_for_terminal_storage().await {
                        tracing::warn!(
                            lix_id = lix_id.as_str(),
                            "cancelled protocol request hit a terminal storage error; recovering runtime"
                        );
                        manager.recover(&lix_id, &service).await;
                    }
                });
            }
            CancellationRecoveryMode::RecoverOnDrop => {
                tokio::spawn(async move {
                    manager.recover(&lix_id, &service).await;
                });
            }
            CancellationRecoveryMode::Disarmed => {}
        }
    }
}

impl http_body::Body for TracedBody {
    type Data = <Body as http_body::Body>::Data;
    type Error = <Body as http_body::Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let span = self.span.clone();
        let _entered = span.enter();
        Pin::new(&mut self.inner).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl http_body::Body for LixLeaseBody {
    type Data = <Body as http_body::Body>::Data;
    type Error = <Body as http_body::Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(context);
        if matches!(&frame, Poll::Ready(None)) {
            // A completed stream no longer needs to prevent LRU eviction or
            // terminal-runtime cleanup, even when its response object remains
            // retained by an in-process caller.
            self.lix.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pub fn router(
    manager: Arc<LixRuntimeManager>,
    internal_token: Option<String>,
    protocol_timeout: Duration,
    in_flight_sql: InFlightSqlRegistry,
) -> Router {
    let request_id_key = protocol_request_id_key(internal_token.as_deref());
    Router::new()
        .route("/healthz", get(healthz))
        .route("/lix/v1/{lix_id}", any(lix_protocol_root))
        .route("/lix/v1/{lix_id}/", any(lix_protocol_root))
        .route("/lix/v1/{lix_id}/{*protocol_path}", any(lix_protocol))
        .with_state(AppState {
            manager,
            internal_token: internal_token.map(Arc::from),
            protocol_timeout,
            request_id_key,
            request_sequence: Arc::new(AtomicU64::new(0)),
            in_flight_sql,
        })
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .zstd(true)
                .quality(CompressionLevel::Precise(2))
                .compress_when(
                    DefaultPredicate::new()
                        .and(SizeAbove::new(MIN_COMPRESSION_BODY_BYTES))
                        .and(NotForContentType::const_new(
                            server_protocol::SNAPSHOT_MEDIA_TYPE,
                        )),
                ),
        )
        // The canonical protocol applies its body limit while consuming the
        // expanded stream, so decompression must happen before dispatch.
        .layer(RequestDecompressionLayer::new().gzip(true))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    ok: bool,
    service: &'static str,
    storage_backend: &'static str,
    storage_layout: &'static str,
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    // The storage identity belongs to the runtime manager. Do not accept it
    // from a request or a benchmark invocation: reports must describe the
    // backend the server actually opened.
    Json(HealthResponse {
        ok: true,
        service: "lix-server",
        storage_backend: state.manager.storage_backend(),
        storage_layout: state.manager.storage_layout(),
    })
}

async fn lix_protocol(
    State(state): State<AppState>,
    Path((lix_id, protocol_path)): Path<(String, String)>,
    request: Request<Body>,
) -> Response {
    lix_protocol_route(state, lix_id, protocol_path, request).await
}

async fn lix_protocol_root(
    State(state): State<AppState>,
    Path(lix_id): Path<String>,
    request: Request<Body>,
) -> Response {
    lix_protocol_route(state, lix_id, String::new(), request).await
}

async fn lix_protocol_route(
    state: AppState,
    lix_id: String,
    protocol_path: String,
    request: Request<Body>,
) -> Response {
    let request_id = protocol_request_id(&state, &lix_id, &protocol_path);
    let span = tracing::info_span!(
        "lix.protocol.request",
        "otel.name" = "Lix protocol request",
        "otel.kind" = "server",
        "http.request.method" = %request.method(),
        "http.route" = "/lix/v1/{lix_id}/{*protocol_path}",
        "lix.lix.id" = %lix_id,
        "lix.protocol.path" = %protocol_path,
        "lix.request.id" = %request_id,
        "lix.request.phase" = tracing::field::Empty,
        "lix.sql.active.count" = tracing::field::Empty,
        "lix.sql.active.operations" = tracing::field::Empty,
        "lix.sql.active.execution_kinds" = tracing::field::Empty,
        "lix.sql.active.fingerprints" = tracing::field::Empty,
        "http.response.status_code" = tracing::field::Empty,
        "lix.error.code" = tracing::field::Empty,
        "otel.status_code" = tracing::field::Empty,
    );
    let response = lix_protocol_inner(state, lix_id, protocol_path, request_id, request)
        .instrument(span.clone())
        .await;
    if response.status() != StatusCode::GATEWAY_TIMEOUT {
        span.record("lix.request.phase", "completed");
    }
    span.record("http.response.status_code", response.status().as_u16());
    if response.status().is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::new(TracedBody { inner: body, span }))
}

async fn lix_protocol_inner(
    state: AppState,
    lix_id: String,
    protocol_path: String,
    request_id: String,
    mut request: Request<Body>,
) -> Response {
    tracing::info!(
        request_id,
        lix_id,
        protocol_path,
        method = %request.method(),
        "protocol request started"
    );
    if !authorized(request.headers(), state.internal_token.as_deref()) {
        let mut response = protocol_error(
            StatusCode::UNAUTHORIZED,
            "LIX_ERROR_UNAUTHENTICATED",
            "Invalid internal service token.",
            None,
            None,
        );
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    let canonical_lix_id = uuid::Uuid::parse_str(&lix_id)
        .ok()
        .map(|id| id.hyphenated().to_string());
    if canonical_lix_id.as_deref() != Some(lix_id.as_str()) {
        return protocol_error(
            StatusCode::NOT_FOUND,
            "LIX_NOT_FOUND",
            "Lix not found.",
            None,
            None,
        );
    }
    // The bearer token authenticates the outer host route. It is not
    // part of the canonical Lix Server Protocol request.
    request.headers_mut().remove(header::AUTHORIZATION);

    let trusted_principal =
        match take_trusted_principal(&mut request, state.internal_token.is_some()) {
            Ok(principal) => principal,
            Err(message) => {
                return protocol_error(
                    StatusCode::BAD_REQUEST,
                    "LIX_INVALID_ARGUMENT",
                    message,
                    None,
                    None,
                );
            }
        };
    let principal = trusted_principal.map_or(ServerProtocolPrincipal::Anonymous, |principal| {
        ServerProtocolPrincipal::Authenticated {
            account_id: principal.account_id,
            idempotency_scope: principal.idempotency_scope,
        }
    });
    let protocol_started_at = Instant::now();
    let runtime =
        match tokio::time::timeout(state.protocol_timeout, state.manager.get(&lix_id)).await {
            Ok(Ok(runtime)) => runtime,
            Ok(Err(error)) => return lix_error(error),
            Err(_elapsed) => {
                tracing::warn!(
                    lix_id,
                    timeout_secs = state.protocol_timeout.as_secs(),
                    "lix runtime admission exceeded its deadline"
                );
                return lix_error(LixRuntimeError::AtCapacity {
                    max: state.manager.max_open_lixes(),
                });
            }
        };

    let (notifier, signal) = server_protocol::durable_terminal_storage_signal();
    let context = ServerProtocolContext {
        principal,
        durable_terminal_storage_notifier: Some(notifier),
    };
    let mut cancellation_recovery = CancellationRecovery::new(
        signal,
        Arc::clone(&state.manager),
        lix_id.clone(),
        Arc::clone(&runtime),
    );

    // Acquisition above and this protocol call are both bounded. The
    // object-store client has its own request budget, but a wedged connection
    // (e.g. after a host sleep) can still hang past it — without these bounds
    // requests never answer and a runtime can remain wedged for later calls.
    let handler_future = runtime.handle_protocol(request, context);
    let response = match tokio::time::timeout(state.protocol_timeout, handler_future).await {
        Ok(response) => response,
        Err(_elapsed) => {
            let activities = state.in_flight_sql.current();
            let phase = if activities.is_empty() {
                "protocol_handler"
            } else {
                "sql_execution"
            };
            let operations =
                sql_activity_values(&activities, |activity| activity.operation.as_deref());
            let execution_kinds =
                sql_activity_values(&activities, |activity| activity.execution_kind.as_deref());
            let fingerprints =
                sql_activity_values(&activities, |activity| activity.fingerprint.as_deref());
            let span = tracing::Span::current();
            span.record("lix.request.phase", phase);
            span.record("lix.sql.active.count", activities.len() as u64);
            span.record("lix.sql.active.operations", operations.as_str());
            span.record("lix.sql.active.execution_kinds", execution_kinds.as_str());
            span.record("lix.sql.active.fingerprints", fingerprints.as_str());
            tracing::warn!(
                lix_id,
                request_id,
                phase,
                active_sql_count = activities.len(),
                active_sql_operations = operations,
                active_sql_execution_kinds = execution_kinds,
                active_sql_fingerprints = fingerprints,
                timeout_secs = state.protocol_timeout.as_secs(),
                "protocol request exceeded its deadline; recovering runtime"
            );
            cancellation_recovery.recover_on_drop();
            state.manager.recover(&lix_id, &runtime).await;
            cancellation_recovery.disarm();
            return protocol_timeout_response();
        }
    };
    record_sql_failures(
        &state.in_flight_sql,
        response.status(),
        &request_id,
        &lix_id,
        &protocol_path,
    );
    if server_protocol::is_terminal_storage_response(&response) {
        tracing::warn!(
            lix_id = lix_id.as_str(),
            "protocol request hit a terminal storage error; recovering runtime"
        );
        // The failed operation is deliberately not replayed: SlateDB reports
        // a terminal storage failure, but a mutation's outcome can still be
        // unknown to the caller. The protocol response marks that explicitly.
        cancellation_recovery.recover_on_drop();
        state.manager.recover(&lix_id, &runtime).await;
    }
    if let Some(terminal_signal) = server_protocol::terminal_storage_stream_signal(&response) {
        let manager = Arc::clone(&state.manager);
        let recovery_runtime = Arc::clone(&runtime);
        let recovery_lix_id = lix_id.clone();
        // Streaming responses can hit storage after sending their HTTP status.
        // Keep the terminal signal in-process rather than parsing body bytes,
        // and do not retain this runtime after a normal stream end.
        tokio::spawn(async move {
            if terminal_signal.wait_for_terminal_storage().await {
                tracing::warn!(
                    lix_id = recovery_lix_id.as_str(),
                    "protocol stream hit a terminal storage error; recovering runtime"
                );
                manager.recover(&recovery_lix_id, &recovery_runtime).await;
            }
        });
    }
    cancellation_recovery.disarm();
    let protocol_duration_ms = protocol_started_at.elapsed().as_secs_f64() * 1_000.0;
    let holds_lix_lease = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|content_type| content_type.split(';').next())
        .is_some_and(|media_type| {
            let media_type = media_type.trim();
            media_type.eq_ignore_ascii_case("text/event-stream")
                || media_type.eq_ignore_ascii_case(server_protocol::SNAPSHOT_MEDIA_TYPE)
        });
    let (mut parts, body) = response.into_parts();
    if let Ok(server_timing) = HeaderValue::from_str(&format!(
        "lix-server-protocol;dur={protocol_duration_ms:.3}"
    )) {
        parts.headers.insert(
            header::HeaderName::from_static("server-timing"),
            server_timing,
        );
    }
    let body = Body::new(body);
    if holds_lix_lease {
        Response::from_parts(
            parts,
            Body::new(LixLeaseBody {
                inner: body,
                lix: Some(runtime),
            }),
        )
    } else {
        Response::from_parts(parts, body)
    }
}

fn record_sql_failures(
    registry: &InFlightSqlRegistry,
    response_status: StatusCode,
    request_id: &str,
    lix_id: &str,
    protocol_path: &str,
) {
    let failures = registry.take_errors();
    if failures.is_empty() {
        return;
    }

    let error_codes = sql_activity_values(&failures, |activity| activity.error_code.as_deref());
    let span = tracing::Span::current();
    span.record("lix.error.code", error_codes.as_str());
    span.record("otel.status_code", "ERROR");

    for failure in failures {
        tracing::warn!(
            request_id,
            lix_id,
            protocol_path,
            "http.response.status_code" = response_status.as_u16(),
            "lix.error.code" = failure.error_code.as_deref().unwrap_or("unavailable"),
            "lix.sql.fingerprint" = failure.fingerprint.as_deref().unwrap_or("unavailable"),
            "db.operation.name" = failure.operation.as_deref().unwrap_or("unavailable"),
            "lix.execution.kind" = failure.execution_kind.as_deref().unwrap_or("unavailable"),
            "lix.batch.index" = ?failure.batch_index,
            "Lix SQL protocol request failed"
        );
    }
}

fn protocol_request_id_key(internal_token: Option<&str>) -> [u8; 32] {
    blake3::derive_key(
        "lix server protocol request identity v1",
        internal_token.unwrap_or("local-development").as_bytes(),
    )
}

fn protocol_request_id(state: &AppState, lix_id: &str, protocol_path: &str) -> String {
    let sequence = state.request_sequence.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = blake3::Hasher::new_keyed(&state.request_id_key);
    hasher.update(b"lix-server.protocol.request.v1\0");
    hasher.update(&timestamp.to_le_bytes());
    hasher.update(&sequence.to_le_bytes());
    hasher.update(lix_id.as_bytes());
    hasher.update(protocol_path.as_bytes());
    hasher.finalize().to_hex().as_str()[..24].to_string()
}

fn sql_activity_values<'a>(
    activities: &'a [crate::telemetry::InFlightSqlActivity],
    value: impl Fn(&'a crate::telemetry::InFlightSqlActivity) -> Option<&'a str>,
) -> String {
    let mut values = Vec::new();
    for activity in activities {
        let Some(value) = value(activity) else {
            continue;
        };
        if !values.contains(&value) {
            values.push(value);
        }
    }
    if values.is_empty() {
        "unavailable".to_string()
    } else {
        values.join(",")
    }
}

fn take_trusted_principal(
    request: &mut Request<Body>,
    trusted_headers_enabled: bool,
) -> Result<Option<TrustedRequestPrincipal>, &'static str> {
    let account_id = take_trusted_header(request, TRUSTED_ACCOUNT_ID_HEADER)?;
    let idempotency_scope = take_trusted_header(request, TRUSTED_IDEMPOTENCY_SCOPE_HEADER)?;
    if !trusted_headers_enabled && (account_id.is_some() || idempotency_scope.is_some()) {
        return Err("Trusted principal headers require LIX_SERVER_INTERNAL_TOKEN.");
    }
    let Some(account_id) = account_id else {
        if idempotency_scope.is_some() {
            return Err("x-lix-idempotency-scope requires x-lix-account-id.");
        }
        return Ok(None);
    };
    let idempotency_scope = idempotency_scope.unwrap_or_else(|| account_id.clone());
    Ok(Some(TrustedRequestPrincipal {
        account_id,
        idempotency_scope,
    }))
}

fn take_trusted_header(
    request: &mut Request<Body>,
    name: &'static str,
) -> Result<Option<String>, &'static str> {
    let value = {
        let mut values = request.headers().get_all(name).iter();
        let value = values.next().cloned();
        if values.next().is_some() {
            return Err("Trusted principal headers must be sent at most once.");
        }
        value
    };
    request.headers_mut().remove(name);
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| "Trusted principal headers must contain visible ASCII characters.")?;
    if value.is_empty()
        || value.len() > MAX_TRUSTED_PRINCIPAL_BYTES
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err("Trusted principal headers must contain 1 to 255 visible ASCII characters.");
    }
    Ok(Some(value.to_string()))
}

fn protocol_timeout_response() -> Response {
    // Lix deliberately lets durable protocol work outlive the cancelled HTTP
    // future. This outer layer cannot distinguish an idempotent SQL write
    // from a read-shaped operation that persists runtime state, so it must
    // preserve an unknown outcome rather than infer replay safety from a URL
    // or header alone.
    protocol_error(
        StatusCode::GATEWAY_TIMEOUT,
        "LIX_ERROR_LIX_TIMEOUT",
        "The lix request timed out before its outcome was known.",
        Some("Do not automatically retry this request; a mutation may still complete.".to_string()),
        Some(json!({
        "operation": "lix_request",
        "retryable": false,
        "outcome": "unknown",
        })),
    )
}

fn authorized(headers: &HeaderMap, internal_token: Option<&str>) -> bool {
    let Some(internal_token) = internal_token else {
        return true;
    };
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == internal_token)
}

fn lix_error(error: LixRuntimeError) -> Response {
    match error {
        LixRuntimeError::InvalidId => protocol_error(
            StatusCode::BAD_REQUEST,
            "LIX_INVALID_ARGUMENT",
            "Invalid lix ID.",
            None,
            Some(json!({
                "operation": "lix_open",
                "retryable": false,
            })),
        ),
        LixRuntimeError::AtCapacity { max } => protocol_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIX_ERROR_LIX_CAPACITY",
            "The lix service is at capacity.",
            Some("Retry after an active lix closes.".to_string()),
            Some(json!({
                "maxOpenLixes": max,
                "operation": "lix_open",
                "retryable": true,
            })),
        )
        .with_retry_after(),
        LixRuntimeError::Migrating {
            from_version,
            to_version,
        } => protocol_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIX_ERROR_LIX_MIGRATING",
            "The lix repository is being migrated.",
            Some("Retry after the migration completes.".to_string()),
            Some(json!({
                "fromVersion": from_version,
                "toVersion": to_version,
                "operation": "lix_open",
                "retryable": true,
            })),
        )
        .with_retry_after(),
        LixRuntimeError::MigrationFailed {
            from_version,
            to_version,
        } => protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "LIX_ERROR_LIX_MIGRATION_FAILED",
            "The lix repository migration failed.",
            Some("Contact the service operator to recover the repository.".to_string()),
            Some(json!({
                "fromVersion": from_version,
                "toVersion": to_version,
                "operation": "lix_open",
                "retryable": false,
            })),
        ),
        LixRuntimeError::UpgradeFailed => protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "LIX_ERROR_LIX_MIGRATION_FAILED",
            "The lix repository upgrade failed.",
            Some("Contact the service operator to recover the repository.".to_string()),
            Some(json!({
                "operation": "lix_open",
                "retryable": false,
            })),
        ),
        LixRuntimeError::Recovering => protocol_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIX_ERROR_LIX_RECOVERING",
            "The lix is recovering.",
            Some("Retry the request.".to_string()),
            Some(json!({
                "operation": "lix_open",
                "retryable": true,
            })),
        )
        .with_retry_after(),
        LixRuntimeError::ShuttingDown => protocol_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIX_ERROR_LIX_SHUTTING_DOWN",
            "The lix server is shutting down.",
            Some("Retry the request on a healthy server.".to_string()),
            Some(json!({
                "operation": "lix_open",
                "retryable": true,
            })),
        )
        .with_retry_after(),
        LixRuntimeError::Cleanup(error) => {
            tracing::error!(error = %error, "lix cache cleanup could not complete");
            protocol_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "LIX_ERROR_LIX_CACHE_CLEANUP",
                "The lix service cache needs operator repair.",
                Some(
                    "Contact the service operator to repair the cache and restart the server."
                        .to_string(),
                ),
                Some(json!({
                    "operation": "lix_open",
                    "retryable": false,
                })),
            )
        }
        LixRuntimeError::Open(error) if is_unsupported_storage_format(&error) => {
            tracing::warn!(
                error = %error,
                error_code = "LIX_ERROR_STORAGE_FORMAT_UNSUPPORTED",
                "lix uses an unsupported storage format"
            );
            protocol_error(
                StatusCode::CONFLICT,
                "LIX_ERROR_STORAGE_FORMAT_UNSUPPORTED",
                "This lix uses an unsupported storage format.",
                Some("Create a new lix.".to_string()),
                Some(json!({
                    "operation": "lix_open",
                    "retryable": false,
                })),
            )
        }
        LixRuntimeError::Open(error) => {
            tracing::error!(error = %error, "failed to open lix runtime");
            protocol_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                lix_sdk::LixError::CODE_INTERNAL_ERROR,
                "Unable to open lix.",
                None,
                Some(json!({
                    "operation": "lix_open",
                    "retryable": false,
                })),
            )
        }
    }
}

fn is_unsupported_storage_format(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<lix_sdk::LixError>()
            .is_some_and(|error| {
                error.code == "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT"
                    || (error.code == lix_sdk::LixError::CODE_INTERNAL_ERROR
                        && error
                            .message
                            .contains("tracked_state commit_root has an unsupported format"))
            })
    })
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

fn protocol_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    hint: Option<String>,
    details: Option<serde_json::Value>,
) -> Response {
    let code = code.into();
    tracing::Span::current().record("lix.error.code", code.as_str());
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                code,
                message: message.into(),
                hint,
                details,
            },
        }),
    )
        .into_response()
}

trait RetryAfterResponse {
    fn with_retry_after(self) -> Self;
}

impl RetryAfterResponse for Response {
    fn with_retry_after(mut self) -> Self {
        self.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use flate2::{Compression, write::GzEncoder};
    use http_body_util::BodyExt;
    use lix_sdk::{Value as LixValue, WireValue};
    use serde_json::{Value as JsonValue, json};
    use std::{
        io::Write as _,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tower::ServiceExt as _;

    const TEST_PROTOCOL_TIMEOUT: Duration = Duration::from_secs(60);
    const TEST_INTERNAL_TOKEN: &str = "test-internal-token";
    const LIX_A: &str = "11111111-1111-4111-8111-111111111111";
    const LIX_B: &str = "22222222-2222-4222-8222-222222222222";
    const LIX_MARKDOWN: &str = "33333333-3333-4333-8333-333333333333";
    static TEST_IDEMPOTENCY_KEY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    fn test_router() -> Router {
        let manager = LixRuntimeManager::new_in_memory(4);
        router(
            manager,
            None,
            TEST_PROTOCOL_TIMEOUT,
            InFlightSqlRegistry::default(),
        )
    }

    fn trusted_test_router() -> Router {
        let manager = LixRuntimeManager::new_in_memory(4);
        router(
            manager,
            Some(TEST_INTERNAL_TOKEN.to_string()),
            TEST_PROTOCOL_TIMEOUT,
            InFlightSqlRegistry::default(),
        )
    }

    async fn json_body(response: Response) -> JsonValue {
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("response body")
                .to_bytes(),
        )
        .expect("response JSON")
    }

    #[tokio::test]
    async fn exposes_only_the_lix_addressed_protocol() {
        let app = test_router();
        let unscoped = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/")
                    .body(Body::empty())
                    .expect("unscoped request"),
            )
            .await
            .expect("unscoped response");
        assert_eq!(unscoped.status(), StatusCode::NOT_FOUND);

        let legacy = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lixes/11111111-1111-4111-8111-111111111111/lix/v1/")
                    .body(Body::empty())
                    .expect("legacy request"),
            )
            .await
            .expect("legacy response");
        assert_eq!(legacy.status(), StatusCode::NOT_FOUND);

        let handshake = app
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), StatusCode::OK);
        assert!(
            handshake
                .headers()
                .get("server-timing")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("lix-server-protocol;dur=")),
            "Lix protocol responses expose the canonical Server-Timing metric"
        );
        let handshake = json_body(handshake).await;
        assert_eq!(
            handshake["protocolVersion"],
            json!(server_protocol::PROTOCOL_VERSION)
        );
        assert_eq!(
            handshake["activeAccountId"],
            json!(lix_sdk::ANONYMOUS_ACCOUNT_ID)
        );
        assert!(handshake["sessionId"].as_str().is_some());
    }

    #[tokio::test]
    async fn snapshot_is_an_authenticated_canonical_protocol_stream() {
        const ACCOUNT_ID: &str = "01920000-0000-7000-8000-000000000601";
        let response = trusted_test_router()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/snapshot")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {TEST_INTERNAL_TOKEN}"),
                    )
                    .header(TRUSTED_ACCOUNT_ID_HEADER, ACCOUNT_ID)
                    .header(header::ACCEPT_ENCODING, "gzip")
                    .body(Body::empty())
                    .expect("snapshot request"),
            )
            .await
            .expect("snapshot response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static(
                server_protocol::SNAPSHOT_MEDIA_TYPE
            ))
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store, no-transform"))
        );
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("snapshot body")
            .to_bytes();
        assert!(bytes.starts_with(b"LIXSNAP"));
    }

    #[tokio::test]
    async fn malformed_protocol_ids_do_not_open_storage() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let app = router(
            Arc::clone(&manager),
            None,
            TEST_PROTOCOL_TIMEOUT,
            InFlightSqlRegistry::default(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/not-a-uuid/snapshot")
                    .body(Body::empty())
                    .expect("malformed target request"),
            )
            .await
            .expect("malformed target response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(manager.cached_lix_count().await, 0);
    }

    #[tokio::test]
    async fn trusted_account_is_provisioned_bound_and_attributed_end_to_end() {
        const ACCOUNT_ID: &str = "01920000-0000-7000-8000-000000000601";
        const OTHER_ACCOUNT_ID: &str = "01920000-0000-7000-8000-000000000602";
        let app = trusted_test_router();
        let handshake = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/22222222-2222-4222-8222-222222222222/")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {TEST_INTERNAL_TOKEN}"),
                    )
                    .header(TRUSTED_ACCOUNT_ID_HEADER, ACCOUNT_ID)
                    .body(Body::empty())
                    .expect("trusted handshake request"),
            )
            .await
            .expect("trusted handshake response");
        assert_eq!(handshake.status(), StatusCode::OK);
        let handshake = json_body(handshake).await;
        assert_eq!(handshake["activeAccountId"], ACCOUNT_ID);
        let session_id = handshake["sessionId"].as_str().expect("session id");

        let insert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/22222222-2222-4222-8222-222222222222/execute")
                    .header(server_protocol::SESSION_ID_HEADER, session_id)
                    .header(server_protocol::IDEMPOTENCY_KEY_HEADER, "account-e2e-insert")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_INTERNAL_TOKEN}"))
                    .header(TRUSTED_ACCOUNT_ID_HEADER, ACCOUNT_ID)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"INSERT INTO lix_key_value (key, value) VALUES ('account-e2e', $1)","params":[{"kind":"jsonb","value":true}]}"#,
                    ))
                    .expect("attributed insert request"),
            )
            .await
            .expect("attributed insert response");
        assert_eq!(
            insert.status(),
            StatusCode::OK,
            "{:?}",
            json_body(insert).await
        );

        let attribution = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/22222222-2222-4222-8222-222222222222/execute")
                    .header(server_protocol::SESSION_ID_HEADER, session_id)
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_INTERNAL_TOKEN}"))
                    .header(TRUSTED_ACCOUNT_ID_HEADER, ACCOUNT_ID)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"SELECT account_id FROM lix_change WHERE schema_key = 'lix_key_value' ORDER BY created_at DESC LIMIT 1"}"#,
                    ))
                    .expect("attribution query request"),
            )
            .await
            .expect("attribution query response");
        assert_eq!(attribution.status(), StatusCode::OK);
        assert_eq!(
            json_body(attribution).await["rows"][0][0]["value"],
            ACCOUNT_ID
        );

        let cross_account = app
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/22222222-2222-4222-8222-222222222222/")
                    .header(server_protocol::SESSION_ID_HEADER, session_id)
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {TEST_INTERNAL_TOKEN}"),
                    )
                    .header(TRUSTED_ACCOUNT_ID_HEADER, OTHER_ACCOUNT_ID)
                    .body(Body::empty())
                    .expect("cross-account resume request"),
            )
            .await
            .expect("cross-account resume response");
        assert_eq!(cross_account.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            json_body(cross_account).await["error"]["code"],
            "LIX_ERROR_PROTOCOL_ACCOUNT_MISMATCH"
        );
    }

    #[tokio::test]
    async fn decompresses_gzip_before_canonical_protocol_dispatch() {
        let app = test_router();
        let handshake = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/33333333-3333-4333-8333-333333333333/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        let session_id = json_body(handshake).await["sessionId"]
            .as_str()
            .expect("session id")
            .to_owned();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(br#"{"sql":"SELECT 1 AS value","params":[]}"#)
            .expect("compress execute request");
        let body = encoder.finish().expect("finish gzip request");
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/33333333-3333-4333-8333-333333333333/execute")
                    .header(server_protocol::SESSION_ID_HEADER, session_id)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(body))
                    .expect("compressed execute request"),
            )
            .await
            .expect("compressed execute response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["rows"][0][0]["value"], 1);
    }

    #[tokio::test]
    async fn health_does_not_open_a_lix_or_require_authentication() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            json_body(response).await,
            json!({
                "ok": true,
                "service": "lix-server",
                "storageBackend": "slatedb",
                "storageLayout": "lix-slatedb-lz4-v1",
            })
        );
    }

    #[tokio::test]
    async fn internal_token_protects_lix_routes() {
        let manager = LixRuntimeManager::new_in_memory(4);
        let app = router(
            manager,
            Some("secret".to_string()),
            TEST_PROTOCOL_TIMEOUT,
            InFlightSqlRegistry::default(),
        );

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("unauthorized request"),
            )
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            json_body(unauthorized).await["error"],
            json!({
                "code": "LIX_ERROR_UNAUTHENTICATED",
                "message": "Invalid internal service token.",
            })
        );

        let authorized = app
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .expect("authorized request"),
            )
            .await
            .expect("authorized response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn isolates_lix_data() {
        let app = test_router();
        let lix_a_session = open_protocol_session(app.clone(), LIX_A).await;
        let insert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/execute")
                    .header(
                        server_protocol::SESSION_ID_HEADER,
                        &lix_a_session,
                    )
                    .header(server_protocol::IDEMPOTENCY_KEY_HEADER, "isolate-lix-a")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"INSERT INTO lix_file (path, content) VALUES ('/only-a.txt', CAST('A' AS BYTEA))"}"#,
                    ))
                    .expect("insert request"),
            )
            .await
            .expect("insert response");
        assert_eq!(
            insert.status(),
            StatusCode::OK,
            "{:?}",
            json_body(insert).await
        );

        for (lix_id, expected) in [(LIX_A, 1), (LIX_B, 0)] {
            let session_id = if lix_id == LIX_A {
                lix_a_session.clone()
            } else {
                open_protocol_session(app.clone(), lix_id).await
            };
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!(
                            "/lix/v1/{lix_id}/execute"
                        ))
                        .header(server_protocol::SESSION_ID_HEADER, session_id)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{"sql":"SELECT COUNT(*) AS count FROM lix_file WHERE path = '/only-a.txt'"}"#,
                        ))
                        .expect("count request"),
                )
                .await
                .expect("count response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(json_body(response).await["rows"][0][0]["value"], expected);
        }
    }

    #[tokio::test]
    async fn strict_remote_sessions_merge_disjoint_markdown_blob_edits() {
        let archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(
                "../vendor/lix/packages/js-sdk/dist/bundled-plugins/plugin_markdown_incremental_v2.lixplugin",
            );
        let Ok(markdown_plugin) = std::fs::read(&archive_path) else {
            // The root pnpm install supplies bundled plugins. Keep standalone
            // Rust development usable before that generated artifact exists.
            eprintln!(
                "skipping native Markdown protocol test; build {} first",
                archive_path.display()
            );
            return;
        };

        let app = test_router();
        let lix_id = LIX_MARKDOWN;
        let installer = open_protocol_session(app.clone(), lix_id).await;
        expect_protocol_ok(
            protocol_execute(
                app.clone(),
                lix_id,
                &installer,
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                vec![
                    wire_value(LixValue::Text(
                        "/.lix/plugins/plugin_markdown_incremental_v2.lixplugin".to_string(),
                    )),
                    wire_value(LixValue::Blob(markdown_plugin.into())),
                ],
            )
            .await,
        )
        .await;

        let session_a = open_protocol_session(app.clone(), lix_id).await;
        let session_b = open_protocol_session(app.clone(), lix_id).await;
        let path = "/shared.md";
        let seed = b"# Shared\n\nLeft: 0\n\nRight: 0\n".to_vec();
        expect_protocol_ok(
            protocol_execute(
                app.clone(),
                lix_id,
                &session_a,
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                vec![
                    wire_value(LixValue::Text(path.to_string())),
                    wire_value(LixValue::Blob(seed.clone().into())),
                ],
            )
            .await,
        )
        .await;

        for session_id in [&session_a, &session_b] {
            let delivered = expect_protocol_ok(
                protocol_execute(
                    app.clone(),
                    lix_id,
                    session_id,
                    "SELECT content FROM lix_file WHERE path = $1",
                    vec![wire_value(LixValue::Text(path.to_string()))],
                )
                .await,
            )
            .await;
            assert_eq!(wire_blob(&delivered), seed);
        }

        let write_a = protocol_execute(
            app.clone(),
            lix_id,
            &session_a,
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            vec![
                wire_value(LixValue::Blob(
                    b"# Shared\n\nLeft: A\n\nRight: 0\n".to_vec().into(),
                )),
                wire_value(LixValue::Text(path.to_string())),
            ],
        );
        let write_b = protocol_execute(
            app.clone(),
            lix_id,
            &session_b,
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            vec![
                wire_value(LixValue::Blob(
                    b"# Shared\n\nLeft: 0\n\nRight: B\n".to_vec().into(),
                )),
                wire_value(LixValue::Text(path.to_string())),
            ],
        );
        let (write_a, write_b) = tokio::join!(write_a, write_b);
        expect_protocol_ok(write_a).await;
        expect_protocol_ok(write_b).await;

        let merged = expect_protocol_ok(
            protocol_execute(
                app.clone(),
                lix_id,
                &session_a,
                "SELECT content FROM lix_file WHERE path = $1",
                vec![wire_value(LixValue::Text(path.to_string()))],
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8(wire_blob(&merged)).expect("merged Markdown should be UTF-8"),
            "# Shared\n\nLeft: A\n\nRight: B\n"
        );
    }

    #[tokio::test]
    async fn malformed_lix_ids_are_not_found_before_storage_open() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/contains%20space/")
                    .body(Body::empty())
                    .expect("unsafe request"),
            )
            .await
            .expect("unsafe response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(response).await["error"]["code"], "LIX_NOT_FOUND");
    }

    #[tokio::test]
    async fn unsupported_storage_format_is_a_terminal_structured_error() {
        let response = lix_error(LixRuntimeError::Open(anyhow::Error::new(
            lix_sdk::LixError::new(
                "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT",
                "repository uses an unsupported storage protocol; recreate the repository",
            ),
        )));

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
        assert_eq!(
            json_body(response).await["error"],
            json!({
                "code": "LIX_ERROR_STORAGE_FORMAT_UNSUPPORTED",
                "message": "This lix uses an unsupported storage format.",
                "hint": "Create a new lix.",
                "details": {
                    "operation": "lix_open",
                    "retryable": false,
                },
            })
        );
    }

    #[tokio::test]
    async fn migrating_lix_is_a_retryable_structured_state() {
        let response = lix_error(LixRuntimeError::Migrating {
            from_version: 68,
            to_version: 71,
        });

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        assert_eq!(
            json_body(response).await["error"],
            json!({
                "code": "LIX_ERROR_LIX_MIGRATING",
                "message": "The lix repository is being migrated.",
                "hint": "Retry after the migration completes.",
                "details": {
                    "fromVersion": 68,
                    "toVersion": 71,
                    "operation": "lix_open",
                    "retryable": true,
                },
            })
        );
    }

    #[tokio::test]
    async fn failed_lix_migration_is_a_terminal_structured_error() {
        let response = lix_error(LixRuntimeError::MigrationFailed {
            from_version: 68,
            to_version: 71,
        });

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
        assert_eq!(
            json_body(response).await["error"],
            json!({
                "code": "LIX_ERROR_LIX_MIGRATION_FAILED",
                "message": "The lix repository migration failed.",
                "hint": "Contact the service operator to recover the repository.",
                "details": {
                    "fromVersion": 68,
                    "toVersion": 71,
                    "operation": "lix_open",
                    "retryable": false,
                },
            })
        );
    }

    #[tokio::test]
    async fn pre_progress_upgrade_failure_is_a_terminal_structured_error() {
        let response = lix_error(LixRuntimeError::UpgradeFailed);

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
        assert_eq!(
            json_body(response).await["error"],
            json!({
                "code": "LIX_ERROR_LIX_MIGRATION_FAILED",
                "message": "The lix repository upgrade failed.",
                "hint": "Contact the service operator to recover the repository.",
                "details": {
                    "operation": "lix_open",
                    "retryable": false,
                },
            })
        );
    }

    #[tokio::test]
    async fn unexpected_lix_open_errors_are_redacted() {
        let response = lix_error(LixRuntimeError::Open(anyhow::anyhow!(
            "secret object-store failure"
        )));

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json_body(response).await["error"],
            json!({
                "code": "LIX_INTERNAL_ERROR",
                "message": "Unable to open lix.",
                "details": {
                    "operation": "lix_open",
                    "retryable": false,
                },
            })
        );
    }

    #[tokio::test]
    async fn cache_cleanup_errors_require_operator_repair_and_are_redacted() {
        let response = lix_error(LixRuntimeError::Cleanup(Arc::from(
            "delete /cache/tenant-a/secret-object failed",
        )));

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            json!({
                "code": "LIX_ERROR_LIX_CACHE_CLEANUP",
                "message": "The lix service cache needs operator repair.",
                "hint": "Contact the service operator to repair the cache and restart the server.",
                "details": {
                    "operation": "lix_open",
                    "retryable": false,
                },
            })
        );
        assert!(!body.to_string().contains("tenant-a"));
    }

    #[tokio::test]
    async fn timed_out_protocol_requests_are_not_advertised_as_retryable() {
        let response = protocol_timeout_response();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
        let error = json_body(response).await["error"].clone();
        assert_eq!(error["code"], "LIX_ERROR_LIX_TIMEOUT");
        assert_eq!(error["details"]["operation"], "lix_request");
        assert_eq!(error["details"]["retryable"], false);
        assert_eq!(error["details"]["outcome"], "unknown");
    }

    #[test]
    fn trusted_principal_is_removed_from_headers_before_protocol_dispatch() {
        let mut request = Request::builder()
            .header(TRUSTED_ACCOUNT_ID_HEADER, "user-123")
            .header(TRUSTED_IDEMPOTENCY_SCOPE_HEADER, "provider:user-123")
            .body(Body::empty())
            .expect("trusted scope request");

        let principal = take_trusted_principal(&mut request, true)
            .expect("valid trusted scope")
            .expect("trusted scope should be present");
        assert_eq!(principal.account_id, "user-123");
        assert_eq!(principal.idempotency_scope, "provider:user-123");

        assert!(request.headers().get(TRUSTED_ACCOUNT_ID_HEADER).is_none());
        assert!(
            request
                .headers()
                .get(TRUSTED_IDEMPOTENCY_SCOPE_HEADER)
                .is_none()
        );
    }

    #[test]
    fn trusted_principal_is_rejected_without_internal_auth() {
        let mut request = Request::builder()
            .header(TRUSTED_ACCOUNT_ID_HEADER, "user-123")
            .body(Body::empty())
            .expect("untrusted principal request");

        let error = take_trusted_principal(&mut request, false).unwrap_err();

        assert_eq!(
            error,
            "Trusted principal headers require LIX_SERVER_INTERNAL_TOKEN."
        );
    }

    #[tokio::test]
    async fn client_cannot_select_an_active_account_in_the_handshake() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111?activeAccountId=spoofed")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cancelled_nonterminal_request_does_not_recover_its_runtime() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let (notifier, signal) = server_protocol::durable_terminal_storage_signal();
        let cancellation_recovery = CancellationRecovery::new(
            signal,
            Arc::clone(&manager),
            LIX_A.to_string(),
            Arc::clone(&service),
        );

        drop(notifier);
        drop(cancellation_recovery);
        tokio::task::yield_now().await;

        let still_active = manager.get(LIX_A).await.expect("active lix runtime");
        assert!(Arc::ptr_eq(&service, &still_active));
    }

    #[tokio::test]
    async fn cancellation_during_terminal_response_recovery_still_starts_runtime_recovery() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let (notifier, signal) = server_protocol::durable_terminal_storage_signal();
        let mut cancellation_recovery = CancellationRecovery::new(
            signal,
            Arc::clone(&manager),
            LIX_A.to_string(),
            Arc::clone(&service),
        );

        drop(notifier);
        cancellation_recovery.recover_on_drop();
        drop(cancellation_recovery);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match manager.get(LIX_A).await {
                    Err(LixRuntimeError::Recovering) => return,
                    Ok(_) => tokio::time::sleep(Duration::from_millis(1)).await,
                    Err(error) => panic!("unexpected runtime state: {error}"),
                }
            }
        })
        .await
        .expect("cancellation recovery should start runtime recovery");
        drop(service);

        // Recovery closes the protocol on a blocking task. Do not let the
        // Tokio test runtime start shutting down while that task still needs
        // the runtime's drivers to finish. Reopening the same ID joins the
        // cleanup tombstone and proves the old runtime was fully retired.
        let replacement = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match manager.get(LIX_A).await {
                    Ok(replacement) => return replacement,
                    Err(LixRuntimeError::Recovering) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(error) => panic!("unexpected replacement runtime state: {error}"),
                }
            }
        })
        .await
        .expect("cancellation recovery should finish");
        drop(replacement);
        manager
            .shutdown()
            .await
            .expect("replacement runtime should close cleanly");
    }

    #[tokio::test]
    async fn retryable_lix_errors_include_retry_after() {
        for error in [
            LixRuntimeError::AtCapacity { max: 4 },
            LixRuntimeError::Migrating {
                from_version: 68,
                to_version: 71,
            },
            LixRuntimeError::Recovering,
            LixRuntimeError::ShuttingDown,
        ] {
            let response = lix_error(error);
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "1");
            assert_eq!(
                json_body(response).await["error"]["details"]["retryable"],
                true
            );
        }
    }

    async fn protocol_execute(
        app: Router,
        lix_id: &str,
        session_id: &str,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/lix/v1/{lix_id}/execute"))
                .header(server_protocol::SESSION_ID_HEADER, session_id)
                .header(
                    server_protocol::IDEMPOTENCY_KEY_HEADER,
                    format!(
                        "lix-server-test-{}",
                        TEST_IDEMPOTENCY_KEY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                    ),
                )
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "sql": sql, "params": params }).to_string(),
                ))
                .expect("protocol execute request"),
        )
        .await
        .expect("protocol execute response")
    }

    async fn expect_protocol_ok(response: Response) -> JsonValue {
        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::OK, "protocol response: {body}");
        body
    }

    fn wire_value(value: LixValue) -> JsonValue {
        serde_json::to_value(
            WireValue::try_from_engine(&value).expect("test value should encode to wire format"),
        )
        .expect("wire value should serialize")
    }

    fn wire_blob(response: &JsonValue) -> Vec<u8> {
        let wire: WireValue = serde_json::from_value(response["rows"][0][0].clone())
            .expect("execute response should contain a wire value");
        let LixValue::Blob(bytes) = wire
            .try_into_engine()
            .expect("wire blob should decode into an engine value")
        else {
            panic!("execute response should contain a blob");
        };
        bytes.to_vec()
    }

    async fn open_protocol_session(app: Router, lix_id: &str) -> String {
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/lix/v1/{lix_id}/"))
                    .body(Body::empty())
                    .expect("protocol handshake request"),
            )
            .await
            .expect("protocol handshake response");
        assert_eq!(response.status(), StatusCode::OK);
        json_body(response).await["sessionId"]
            .as_str()
            .expect("handshake session id")
            .to_string()
    }
}
