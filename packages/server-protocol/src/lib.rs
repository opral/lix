//! Canonical HTTP transport for a workspace-mode [`lix_sdk::Lix`] handle.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use lix_sdk::{
    CreateBranchOptions, ExecuteBatchStatement, ExecuteOptions, ExecuteResult, Lix, LixError,
    ObserveEvent, ObserveEvents, Storage, SwitchBranchOptions, Value, WireValue,
};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{runtime::Handle, sync::mpsc, task::JoinHandle};

/// Stable URL prefix owned by the Lix server protocol.
pub const PROTOCOL_PATH: &str = "/lix/v1";
/// Current wire protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone)]
struct HandlerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix: Arc<Lix<S>>,
}

impl<S> HandlerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
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
        let lix = Arc::clone(&self.lix);
        tokio::task::spawn_blocking(move || runtime.block_on(operation(lix)))
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
            events: Arc::new(Mutex::new(self.lix.observe(sql, params)?)),
        })
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
        let result = tokio::task::spawn_blocking(move || {
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

/// Returns an Axum handler for the complete canonical Lix HTTP protocol.
///
/// The returned router is already mounted at [`PROTOCOL_PATH`]. Hosts should
/// merge it into their application and keep auth, workspace resolution, and
/// storage lifecycle outside this package.
pub fn handler<S>(lix: Arc<Lix<S>>) -> Router
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/lix/v1", get(handshake::<S>))
        .route("/lix/v1/", get(handshake::<S>))
        .route("/lix/v1/execute", post(execute::<S>))
        .route("/lix/v1/execute-batch", post(execute_batch::<S>))
        .route("/lix/v1/branch/create", post(create_branch::<S>))
        .route("/lix/v1/branch/switch", post(switch_branch::<S>))
        .route("/lix/v1/observe", post(observe::<S>))
        .route("/lix/v1/observe/multiplex", post(observe_multiplex::<S>))
        .with_state(HandlerState { lix })
}

async fn handshake<S>(
    State(state): State<HandlerState<S>>,
) -> Result<Json<HandshakeResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let active_branch_id = state
        .run(|lix| async move { lix.active_branch_id().await })
        .await?;
    Ok(Json(HandshakeResponse {
        protocol_version: PROTOCOL_VERSION,
        active_branch_id,
    }))
}

async fn execute<S>(
    State(state): State<HandlerState<S>>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let params = decode_params(request.params)?;
    let options = request.options.into();
    let result = state
        .run(move |lix| async move { lix.execute_with_options(&sql, &params, options).await })
        .await?;
    Ok(Json(ExecuteResponse::try_from(result)?))
}

async fn execute_batch<S>(
    State(state): State<HandlerState<S>>,
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
    let results = state
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
    State(state): State<HandlerState<S>>,
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
    let receipt = state
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
    State(state): State<HandlerState<S>>,
    Json(request): Json<SwitchBranchRequest>,
) -> Result<Json<SwitchBranchResponse>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let options = SwitchBranchOptions {
        branch_id: required_non_empty(request.branch_id, "branchId")?,
    };
    let receipt = state
        .run(move |lix| async move { lix.switch_branch(options).await })
        .await?;
    Ok(Json(SwitchBranchResponse {
        branch_id: receipt.branch_id,
    }))
}

async fn observe<S>(
    State(state): State<HandlerState<S>>,
    Json(request): Json<ObserveRequest>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let sql = required_non_empty(request.sql, "sql")?;
    let params = decode_params(request.params)?;
    let events = state.observe(&sql, &params)?;
    let stream = async_stream::stream! {
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
    State(state): State<HandlerState<S>>,
    Json(request): Json<MultiplexObserveRequest>,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if request.subscriptions.is_empty() {
        return Err(ApiError::bad_request("subscriptions must not be empty"));
    }
    let (sender, mut receiver) = mpsc::channel::<MultiplexObserveMessage>(64);
    let mut tasks = Vec::with_capacity(request.subscriptions.len());
    for subscription in request.subscriptions {
        let subscription_id = required_non_empty(subscription.id, "subscriptions[].id")?;
        let sql = required_non_empty(subscription.sql, "subscriptions[].sql")?;
        let params = decode_params(subscription.params)?;
        let events = state.observe(&sql, &params)?;
        let sender = sender.clone();
        tasks.push(tokio::spawn(async move {
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
        }));
    }
    drop(sender);
    let stream = async_stream::stream! {
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
    use lix_sdk::{Memory, OpenLixOptions, open_lix};
    use serde_json::{Value as JsonValue, json};
    use tower::ServiceExt as _;

    async fn app() -> Router {
        let lix = Arc::new(
            open_lix(OpenLixOptions::<Memory>::default())
                .await
                .expect("open lix"),
        );
        handler(lix)
    }

    async fn json_request(app: Router, method: &str, uri: &str, body: JsonValue) -> Response {
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response")
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

    #[tokio::test]
    async fn handshake_and_switch_share_the_workspace_selector() {
        let app = app().await;
        let before = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let active = response_json(before).await["activeBranchId"]
            .as_str()
            .expect("active branch")
            .to_string();
        let created = json_request(
            app.clone(),
            "POST",
            "/lix/v1/branch/create",
            json!({ "name": "Draft" }),
        )
        .await;
        let draft = response_json(created).await["id"]
            .as_str()
            .expect("draft id")
            .to_string();
        assert_ne!(active, draft);
        let switched = json_request(
            app.clone(),
            "POST",
            "/lix/v1/branch/switch",
            json!({ "branchId": draft }),
        )
        .await;
        assert_eq!(switched.status(), StatusCode::OK);
        let after = app
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response_json(after).await["activeBranchId"], draft);
    }

    #[tokio::test]
    async fn execute_batch_is_atomic_and_returns_each_result() {
        let response = json_request(
            app().await,
            "POST",
            "/lix/v1/execute-batch",
            json!({
                "statements": [
                    { "sql": "SELECT 1 AS value", "params": [] },
                    { "sql": "SELECT 2 AS value", "params": [] }
                ]
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body.as_array().map(Vec::len), Some(2));
        assert_eq!(body[0]["rows"][0][0], json!({ "kind": "int", "value": 1 }));
        assert_eq!(body[1]["rows"][0][0], json!({ "kind": "int", "value": 2 }));
    }
}
