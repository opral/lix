//! One Lix Server Protocol client.
//!
//! Hosts supply HTTP (`url` + `fetch` + auth headers). Sessions stay in
//! memory. A 410 `LIX_ERROR_PROTOCOL_SESSION_GONE` or 503
//! `LIX_ERROR_PROTOCOL_SERVER_CLOSED` recovers once: clear the token,
//! handshake with no session header, pin the last known branch, retry the
//! request once, then throw. A second 410 is not recovered.

mod blobs;
mod gzip;
mod http;
mod observe;
mod sse;
mod wire;

#[cfg(test)]
mod tests;

pub use http::{
    ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse, ProtocolHttpStream,
    ProtocolHttpStreamResponse,
};
pub use observe::RemoteObserveEvents;

use blobs::{PreparedRequestParams, RequestBlobCache, request_blob_slot};
use observe::ObservationHub;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use wire::{
    BLOB_BASE_MISSING, BeginTransactionResponse, CreateBranchRequest, CreateBranchResponse,
    CreateCheckpointResponse, ErrorEnvelope, ExecuteBatchRequest, ExecuteBatchStatementRequest,
    ExecuteOptionsRequest, ExecuteRequest, ExecuteResponse, HandshakeResponse, PROTOCOL_ERROR,
    PROTOCOL_VERSION, RedoResponse, RequestWireValue, SERVER_CLOSED, SESSION_GONE,
    SESSION_ID_HEADER, SwitchBranchRequest, SwitchBranchResponse, TRANSACTION_ID_HEADER,
    UndoResponse,
};
use crate::{
    CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, ExecuteBatchStatement,
    ExecuteResult, LixError, RedoReceipt, SwitchBranchOptions, SwitchBranchReceipt,
    UndoReceipt, Value, WireValue,
};

const HTTP_STATUS_DETAIL: &str = "httpStatus";

#[derive(Clone, Debug, Default)]
pub struct OpenRemoteOptions {
    pub initial_active_branch_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RemoteExecuteOptions {
    pub origin_key: Option<String>,
    pub idempotency_key: Option<String>,
}

#[expect(missing_debug_implementations)]
pub struct RemoteTransaction<H: ProtocolHttp> {
    inner: Arc<ClientInner<H>>,
    transaction_id: String,
    active: Mutex<bool>,
}

impl<H: ProtocolHttp + 'static> RemoteTransaction<H> {
    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: RemoteExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        if !*self.active.lock().await {
            return Err(transaction_closed());
        }
        let inner = Arc::clone(&self.inner);
        let op = Arc::clone(&inner);
        let transaction_id = self.transaction_id.clone();
        let sql = sql.to_owned();
        let params = params.to_vec();
        inner
            .enqueue(async move {
                op.transaction_execute(&transaction_id, &sql, &params, options)
                    .await
            })
            .await
    }

    pub async fn commit(&self) -> Result<(), LixError> {
        if !*self.active.lock().await {
            return Err(transaction_closed());
        }
        let inner = Arc::clone(&self.inner);
        let op = Arc::clone(&inner);
        let transaction_id = self.transaction_id.clone();
        inner
            .enqueue(async move {
                op.request_method_empty("transaction/commit", "POST", true, Some(&transaction_id))
                    .await
            })
            .await?;
        *self.active.lock().await = false;
        Ok(())
    }

    pub async fn rollback(&self) -> Result<(), LixError> {
        if !*self.active.lock().await {
            return Err(transaction_closed());
        }
        let inner = Arc::clone(&self.inner);
        let op = Arc::clone(&inner);
        let transaction_id = self.transaction_id.clone();
        inner
            .enqueue(async move {
                op.request_method_empty(
                    "transaction/rollback",
                    "POST",
                    true,
                    Some(&transaction_id),
                )
                .await
            })
            .await?;
        *self.active.lock().await = false;
        Ok(())
    }
}

#[expect(missing_debug_implementations)]
pub struct ServerProtocolClient<H: ProtocolHttp> {
    inner: Arc<ClientInner<H>>,
}

pub(crate) struct ClientState {
    session_id: Option<String>,
    active_branch_id: Option<String>,
    active_account_id: Option<String>,
    blob_cache: RequestBlobCache,
}

pub(crate) struct ClientInner<H: ProtocolHttp> {
    pub http: H,
    pub initial_active_branch_id: Option<String>,
    pub state: Mutex<ClientState>,
    pub queue: Mutex<()>,
    pub hub: ObservationHub,
    pub accepting: AtomicBool,
}

impl<H: ProtocolHttp + 'static> ServerProtocolClient<H> {
    async fn queued<T, F, Fut>(&self, operation: F) -> Result<T, LixError>
    where
        F: FnOnce(Arc<ClientInner<H>>) -> Fut,
        Fut: Future<Output = Result<T, LixError>>,
    {
        let inner = Arc::clone(&self.inner);
        let op = Arc::clone(&inner);
        inner.enqueue(operation(op)).await
    }

    pub async fn open(http: H, options: OpenRemoteOptions) -> Result<Self, LixError> {
        if options
            .initial_active_branch_id
            .as_ref()
            .is_some_and(|id| id.is_empty())
        {
            return Err(LixError::new(
                PROTOCOL_ERROR,
                "initialActiveBranchId must be a non-empty string",
            ));
        }
        let inner = Arc::new(ClientInner {
            http,
            initial_active_branch_id: options.initial_active_branch_id,
            state: Mutex::new(ClientState {
                session_id: None,
                active_branch_id: None,
                active_account_id: None,
                blob_cache: RequestBlobCache::new(),
            }),
            queue: Mutex::new(()),
            hub: ObservationHub::new(),
            accepting: AtomicBool::new(true),
        });
        inner.handshake(None, false).await?;
        Ok(Self { inner })
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: RemoteExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.inner.assert_open()?;
        let sql = sql.to_owned();
        let params = params.to_vec();
        self.queued(|inner| async move { inner.execute_inner(&sql, &params, options, true).await })
            .await
    }

    pub async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        options: RemoteExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.inner.assert_open()?;
        let statements = statements.to_vec();
        self.queued(|inner| async move { inner.execute_batch_inner(&statements, options).await })
            .await
    }

    pub async fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<RemoteObserveEvents, LixError> {
        self.inner.assert_open()?;
        ObservationHub::observe(Arc::clone(&self.inner), sql.to_owned(), params.to_vec()).await
    }

    pub async fn begin_transaction(&self) -> Result<RemoteTransaction<H>, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let begun: BeginTransactionResponse = inner
                .request_json("transaction/begin", "POST", &[], None, &[], true)
                .await?;
            if begun.transaction_id.is_empty() {
                return Err(protocol_error(
                    "begin transaction response.transactionId must be a string",
                ));
            }
            Ok(RemoteTransaction {
                inner: Arc::clone(&inner),
                transaction_id: begun.transaction_id,
                active: Mutex::new(true),
            })
        })
        .await
    }

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move { inner.cached_or_refresh_branch().await })
            .await
    }

    pub async fn active_account_id(&self) -> Result<String, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move { inner.cached_or_refresh_account().await })
            .await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let body = serde_json::to_vec(&CreateBranchRequest {
                id: options.id,
                name: options.name,
                from_commit_id: options.from_commit_id,
            })
            .map_err(|error| protocol_error(format!("encode create branch: {error}")))?;
            let value: CreateBranchResponse = inner
                .request_json("branch/create", "POST", &[], Some(&body), &[], true)
                .await?;
            Ok(CreateBranchReceipt {
                id: value.id,
                name: value.name,
                hidden: value.hidden,
                commit_id: value.commit_id,
            })
        })
        .await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let value: CreateCheckpointResponse = inner
                .request_json("checkpoint/create", "POST", &[], None, &[], true)
                .await?;
            if value.commit_id.is_empty() {
                return Err(protocol_error("create checkpoint response is invalid"));
            }
            Ok(CreateCheckpointReceipt {
                commit_id: value.commit_id,
                change_id: String::new(),
            })
        })
        .await
    }

    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let value: UndoResponse = inner
                .request_json("undo", "POST", &[], None, &[], true)
                .await?;
            Ok(UndoReceipt {
                branch_id: value.branch_id,
                target_commit_id: value.target_commit_id,
                inverse_commit_id: value.inverse_commit_id,
            })
        })
        .await
    }

    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let value: RedoResponse = inner
                .request_json("redo", "POST", &[], None, &[], true)
                .await?;
            Ok(RedoReceipt {
                branch_id: value.branch_id,
                target_commit_id: value.target_commit_id,
                replay_commit_id: value.replay_commit_id,
            })
        })
        .await
    }

    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        self.inner.assert_open()?;
        self.queued(|inner| async move {
            let body = serde_json::to_vec(&SwitchBranchRequest {
                branch_id: &options.branch_id,
            })
            .map_err(|error| protocol_error(format!("encode switch branch: {error}")))?;
            match inner
                .request_json::<SwitchBranchResponse>(
                    "branch/switch",
                    "POST",
                    &[],
                    Some(&body),
                    &[],
                    true,
                )
                .await
            {
                Ok(value) => {
                    if value.branch_id != options.branch_id {
                        inner.clear_cached_branch().await;
                        inner.hub.restart(Arc::clone(&inner));
                        return Err(protocol_error("switch branch response is invalid"));
                    }
                    inner.set_cached_branch(options.branch_id.clone()).await;
                    inner.hub.restart(Arc::clone(&inner));
                    Ok(SwitchBranchReceipt {
                        branch_id: options.branch_id,
                    })
                }
                Err(error) => {
                    if request_was_attempted(&error) && !is_definitive_client_error(&error) {
                        inner.clear_cached_branch().await;
                        inner.hub.restart(Arc::clone(&inner));
                    }
                    Err(error)
                }
            }
        })
        .await
    }

    pub async fn close(&self) -> Result<(), LixError> {
        if !self.inner.accepting.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.hub.close();
        self.queued(|inner| async move {
            inner.hub.close();
            inner
                .request_method_empty("session", "DELETE", false, None)
                .await
                .or_else(|error| {
                    if error.code == SESSION_GONE
                        || matches!(http_status(&error), Some(410 | 204 | 404))
                    {
                        Ok(())
                    } else {
                        Err(error)
                    }
                })
        })
        .await
    }
}

impl<H: ProtocolHttp + 'static> ClientInner<H> {
    fn assert_open(&self) -> Result<(), LixError> {
        if self.accepting.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(LixError::new(LixError::CODE_CLOSED, "Lix is closed"))
        }
    }

    pub(crate) async fn enqueue<T, F>(&self, operation: F) -> Result<T, LixError>
    where
        F: Future<Output = Result<T, LixError>>,
    {
        // Close flips `accepting` then enqueues DELETE, matching the JS client.
        let _guard = self.queue.lock().await;
        operation.await
    }

    pub(crate) async fn recover_session(&self) -> Result<(), LixError> {
        let branch = {
            let mut state = self.state.lock().await;
            state.session_id = None;
            state.active_branch_id.clone()
        };
        self.handshake(branch, true).await
    }

    async fn handshake(
        &self,
        pin_branch: Option<String>,
        replacing: bool,
    ) -> Result<(), LixError> {
        let mut query = Vec::new();
        let session_present = self.state.lock().await.session_id.is_some();
        if !session_present {
            if let Some(branch) = pin_branch.or_else(|| self.initial_active_branch_id.clone()) {
                query.push(("activeBranchId".into(), branch));
            }
        }
        let handshake: HandshakeResponse = self
            .request_json_once("", "GET", &query, None, &[])
            .await?;
        decode_handshake(&handshake)?;
        let mut state = self.state.lock().await;
        if !replacing
            && state.session_id.is_some()
            && state.session_id.as_deref() != Some(handshake.session_id.as_str())
        {
            return Err(protocol_error(
                "Lix Server Protocol handshake changed sessionId",
            ));
        }
        state.session_id = Some(handshake.session_id);
        state.active_branch_id = Some(handshake.active_branch_id);
        state.active_account_id = Some(handshake.active_account_id);
        Ok(())
    }

    async fn cached_or_refresh_branch(&self) -> Result<String, LixError> {
        if let Some(branch) = self.state.lock().await.active_branch_id.clone() {
            return Ok(branch);
        }
        self.handshake(None, false).await?;
        self.state
            .lock()
            .await
            .active_branch_id
            .clone()
            .ok_or_else(|| protocol_error("handshake omitted activeBranchId"))
    }

    async fn cached_or_refresh_account(&self) -> Result<String, LixError> {
        if let Some(account) = self.state.lock().await.active_account_id.clone() {
            return Ok(account);
        }
        self.handshake(None, false).await?;
        self.state
            .lock()
            .await
            .active_account_id
            .clone()
            .ok_or_else(|| protocol_error("handshake omitted activeAccountId"))
    }

    async fn set_cached_branch(&self, branch_id: String) {
        self.state.lock().await.active_branch_id = Some(branch_id);
    }

    async fn clear_cached_branch(&self) {
        self.state.lock().await.active_branch_id = None;
    }

    pub(crate) async fn execute_inner(
        &self,
        sql: &str,
        params: &[Value],
        options: RemoteExecuteOptions,
        cache_blobs: bool,
    ) -> Result<ExecuteResult, LixError> {
        let prepared = if cache_blobs {
            let state = self.state.lock().await;
            state
                .blob_cache
                .prepare(params, |index| request_blob_slot("execute", sql, index, None))?
        } else {
            PreparedRequestParams {
                params: params
                    .iter()
                    .map(|param| WireValue::try_from_engine(param).map(RequestWireValue::Value))
                    .collect::<Result<Vec<_>, _>>()?,
                full_params: Vec::new(),
                cache_updates: Vec::new(),
                cache_blobs: false,
                has_delta: false,
            }
        };
        let request_options = options.origin_key.clone().map(|origin_key| {
            ExecuteOptionsRequest {
                origin_key: Some(origin_key),
            }
        });
        let idempotency = idempotency_key(options.idempotency_key.as_deref())?;
        let send = |params: &[RequestWireValue]| {
            let body = serde_json::to_vec(&ExecuteRequest {
                sql,
                params,
                options: request_options.clone(),
                cache_blobs: prepared.cache_blobs,
            })
            .map_err(|error| protocol_error(format!("encode execute: {error}")))?;
            let extra = vec![(
                wire::IDEMPOTENCY_KEY_HEADER.to_owned(),
                idempotency.clone(),
            )];
            Ok::<_, LixError>((body, extra))
        };
        let (body, extra) = send(&prepared.params)?;
        let response = match self
            .request_json::<ExecuteResponse>("execute", "POST", &[], Some(&body), &extra, true)
            .await
        {
            Ok(response) => response,
            Err(error)
                if prepared.has_delta && error.code == BLOB_BASE_MISSING =>
            {
                let (body, extra) = send(&prepared.full_params)?;
                self.request_json("execute", "POST", &[], Some(&body), &extra, true)
                    .await?
            }
            Err(error) => return Err(error),
        };
        if cache_blobs {
            self.state
                .lock()
                .await
                .blob_cache
                .commit(&prepared.cache_updates);
        }
        decode_execute_response(&response)
    }

    async fn execute_batch_inner(
        &self,
        statements: &[ExecuteBatchStatement],
        options: RemoteExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let mut prepared_statements = Vec::with_capacity(statements.len());
        {
            let state = self.state.lock().await;
            for (statement_index, statement) in statements.iter().enumerate() {
                let prepared = state.blob_cache.prepare(&statement.params, |param_index| {
                    request_blob_slot(
                        "batch",
                        &statement.sql,
                        param_index,
                        Some(statement_index),
                    )
                })?;
                prepared_statements.push((statement, prepared));
            }
        }
        let cache_blobs = prepared_statements
            .iter()
            .any(|(_, prepared)| prepared.cache_blobs);
        let has_delta = prepared_statements
            .iter()
            .any(|(_, prepared)| prepared.has_delta);
        let request_options = options.origin_key.clone().map(|origin_key| {
            ExecuteOptionsRequest {
                origin_key: Some(origin_key),
            }
        });
        let idempotency = idempotency_key(options.idempotency_key.as_deref())?;
        let encode = |full: bool| {
            let body = serde_json::to_vec(&ExecuteBatchRequest {
                statements: &prepared_statements
                    .iter()
                    .map(|(statement, prepared)| ExecuteBatchStatementRequest {
                        sql: statement.sql.clone(),
                        params: if full {
                            prepared.full_params.clone()
                        } else {
                            prepared.params.clone()
                        },
                        label: statement.label.clone(),
                    })
                    .collect::<Vec<_>>(),
                options: request_options.clone(),
                cache_blobs,
            })
            .map_err(|error| protocol_error(format!("encode execute batch: {error}")))?;
            Ok::<_, LixError>(body)
        };
        let extra = vec![(
            wire::IDEMPOTENCY_KEY_HEADER.to_owned(),
            idempotency,
        )];
        let body = encode(false)?;
        let value: Vec<ExecuteResponse> = match self
            .request_json("execute-batch", "POST", &[], Some(&body), &extra, true)
            .await
        {
            Ok(value) => value,
            Err(error) if has_delta && error.code == BLOB_BASE_MISSING => {
                let body = encode(true)?;
                self.request_json("execute-batch", "POST", &[], Some(&body), &extra, true)
                    .await?
            }
            Err(error) => return Err(error),
        };
        self.state.lock().await.blob_cache.commit(
            &prepared_statements
                .iter()
                .flat_map(|(_, prepared)| prepared.cache_updates.iter().cloned())
                .collect::<Vec<_>>(),
        );
        value.iter().map(decode_execute_response).collect()
    }

    async fn transaction_execute(
        &self,
        transaction_id: &str,
        sql: &str,
        params: &[Value],
        options: RemoteExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let wire_params = params
            .iter()
            .map(|param| WireValue::try_from_engine(param).map(RequestWireValue::Value))
            .collect::<Result<Vec<_>, _>>()?;
        let body = serde_json::to_vec(&ExecuteRequest {
            sql,
            params: &wire_params,
            options: options.origin_key.map(|origin_key| ExecuteOptionsRequest {
                origin_key: Some(origin_key),
            }),
            cache_blobs: false,
        })
        .map_err(|error| protocol_error(format!("encode transaction execute: {error}")))?;
        let extra = vec![(
            TRANSACTION_ID_HEADER.to_owned(),
            transaction_id.to_owned(),
        )];
        let response: ExecuteResponse = self
            .request_json("transaction/execute", "POST", &[], Some(&body), &extra, true)
            .await?;
        decode_execute_response(&response)
    }

    async fn request_json<T: DeserializeOwned>(
        &self,
        path: &str,
        method: &'static str,
        query: &[(String, String)],
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
        recover: bool,
    ) -> Result<T, LixError> {
        match self
            .request_json_once::<T>(path, method, query, body, extra_headers)
            .await
        {
            Ok(value) => Ok(value),
            Err(error) if recover && is_recoverable_session_error(&error) => {
                self.recover_session().await?;
                self.request_json_once(path, method, query, body, extra_headers)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn request_json_once<T: DeserializeOwned>(
        &self,
        path: &str,
        method: &'static str,
        query: &[(String, String)],
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<T, LixError> {
        let accept = if path == "observe/multiplex" {
            "text/event-stream"
        } else {
            "application/json"
        };
        let mut headers = request_headers(self, accept, body).await?;
        headers.extend(extra_headers.iter().cloned());
        let body = match body {
            Some(body) => {
                let (encoded, compressed) = gzip::maybe_gzip_json(body);
                if compressed {
                    headers.push(("content-encoding".into(), "gzip".into()));
                }
                if !encoded.is_empty() {
                    headers.push(("content-type".into(), "application/json".into()));
                }
                Some(encoded)
            }
            None => None,
        };
        let response = self
            .http
            .request(ProtocolHttpRequest {
                method,
                path: path.to_owned(),
                query: query.to_vec(),
                headers,
                body,
            })
            .await
            .map_err(map_transport_error)?;
        if response.status == 204 {
            return serde_json::from_value(serde_json::Value::Null)
                .map_err(|error| protocol_error(format!("empty response: {error}")));
        }
        if !(200..300).contains(&response.status) {
            return Err(error_from_http(&response.body, response.status));
        }
        if response.body.is_empty() {
            return serde_json::from_value(serde_json::Value::Null)
                .map_err(|error| protocol_error(format!("empty response: {error}")));
        }
        serde_json::from_slice(&response.body).map_err(|_| {
            protocol_error(format!(
                "remote response {} did not contain valid JSON",
                response.status
            ))
        })
    }

    async fn request_method_empty(
        &self,
        path: &str,
        method: &'static str,
        recover: bool,
        transaction_id: Option<&str>,
    ) -> Result<(), LixError> {
        let send = || async {
            let mut headers = request_headers(self, "application/json", None).await?;
            if let Some(transaction_id) = transaction_id {
                headers.push((TRANSACTION_ID_HEADER.to_owned(), transaction_id.to_owned()));
            }
            let response = self
                .http
                .request(ProtocolHttpRequest {
                    method,
                    path: path.to_owned(),
                    query: Vec::new(),
                    headers,
                    body: None,
                })
                .await
                .map_err(map_transport_error)?;
            if response.status == 204 || (200..300).contains(&response.status) {
                Ok(())
            } else {
                Err(error_from_http(&response.body, response.status))
            }
        };
        match send().await {
            Ok(()) => Ok(()),
            Err(error) if recover && is_recoverable_session_error(&error) => {
                self.recover_session().await?;
                send().await
            }
            Err(error) => Err(error),
        }
    }
}

fn transaction_closed() -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
}

pub(crate) async fn request_headers<H: ProtocolHttp>(
    inner: &ClientInner<H>,
    accept: &str,
    body: Option<&[u8]>,
) -> Result<Vec<(String, String)>, LixError> {
    let _ = body;
    let session = inner.state.lock().await.session_id.clone();
    let mut headers = vec![("accept".into(), accept.to_owned())];
    if let Some(session) = session {
        headers.push((SESSION_ID_HEADER.to_owned(), session));
    }
    Ok(headers)
}

pub(crate) fn decode_execute_response(response: &ExecuteResponse) -> Result<ExecuteResult, LixError> {
    if response.rows.iter().any(|row| row.len() != response.columns.len()) {
        return Err(protocol_error(
            "execute result row has the wrong number of values",
        ));
    }
    let rows = response
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .cloned()
                .map(WireValue::try_into_engine)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExecuteResult::from_protocol_response(
        response.statement_index,
        response.label.clone(),
        response.columns.clone(),
        rows,
        response.rows_affected,
        response.notices.clone(),
    ))
}

fn decode_handshake(handshake: &HandshakeResponse) -> Result<(), LixError> {
    if handshake.protocol_version != PROTOCOL_VERSION {
        return Err(protocol_error(format!(
            "unsupported Lix Server Protocol version: {}",
            handshake.protocol_version
        )));
    }
    if handshake.active_branch_id.is_empty() {
        return Err(protocol_error(
            "Lix Server Protocol handshake requires activeBranchId",
        ));
    }
    if handshake.active_account_id.is_empty() {
        return Err(protocol_error(
            "Lix Server Protocol handshake requires activeAccountId",
        ));
    }
    if handshake.session_id.is_empty()
        || handshake.session_id.len() > 256
        || !handshake
            .session_id
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(protocol_error(
            "Lix Server Protocol handshake requires a valid sessionId",
        ));
    }
    Ok(())
}

fn idempotency_key(provided: Option<&str>) -> Result<String, LixError> {
    let key = provided
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
    if key.is_empty()
        || key.len() > 255
        || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "options.idempotencyKey must contain 1 to 255 visible ASCII characters",
        ));
    }
    Ok(key)
}

pub(crate) fn protocol_error(message: impl Into<String>) -> LixError {
    LixError::new(PROTOCOL_ERROR, message)
}

pub(crate) fn with_status(mut error: LixError, status: u16) -> LixError {
    let mut details = error
        .details
        .take()
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = details.as_object_mut() {
        object.insert(HTTP_STATUS_DETAIL.to_owned(), serde_json::json!(status));
    }
    error.details = Some(details);
    error
}

pub(crate) fn http_status(error: &LixError) -> Option<u16> {
    error
        .details
        .as_ref()
        .and_then(|details| details.get(HTTP_STATUS_DETAIL))
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
}

pub(crate) fn is_recoverable_session_error(error: &LixError) -> bool {
    error.code == SESSION_GONE || error.code == SERVER_CLOSED
}

fn is_definitive_client_error(error: &LixError) -> bool {
    http_status(error).is_some_and(|status| (400..500).contains(&status) && status != 408 && status != 429)
}

fn request_was_attempted(error: &LixError) -> bool {
    http_status(error).is_some() || error.code == wire::REMOTE_UNAVAILABLE
}

fn error_from_http(body: &[u8], status: u16) -> LixError {
    if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(body) {
        let mut error = LixError::new(
            envelope
                .error
                .code
                .unwrap_or_else(|| wire::REMOTE_REQUEST_FAILED.to_owned()),
            envelope.error.message.unwrap_or_else(|| {
                format!("Remote Lix request failed with status {status}")
            }),
        );
        error.hint = envelope.error.hint;
        error.details = envelope.error.details;
        return with_status(error, status);
    }
    with_status(
        LixError::new(
            wire::REMOTE_REQUEST_FAILED,
            format!("Remote Lix request failed with status {status}"),
        )
        .with_details(if body.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::json!({
                "body": String::from_utf8_lossy(&body[..body.len().min(1000)]),
            })
        }),
        status,
    )
}

fn map_transport_error(error: LixError) -> LixError {
    if error.code.is_empty() {
        LixError::new(wire::REMOTE_UNAVAILABLE, "The remote Lix server is unavailable")
            .with_details(serde_json::json!({ "cause": error.message }))
    } else {
        error
    }
}

