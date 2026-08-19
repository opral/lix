//! Host-pluggable Lix Server Protocol client.

mod blobs;
mod http;
mod observe;
mod sse;
mod wire;

#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, ExecuteBatchStatement,
    ExecuteResult, LixError, RedoReceipt, SwitchBranchReceipt, UndoReceipt, Value,
};

use blobs::{BlobCache, PreparedRequestParams, request_blob_slot};
use wire::{
    BLOB_BASE_MISSING_CODE, BeginTransactionResponse, CreateBranchRequestBody,
    CreateBranchResponseBody, CreateCheckpointResponseBody, EmptyBody, ErrorEnvelope,
    ExecuteBatchRequestBody, ExecuteBatchStatementBody, ExecuteOptionsBody, ExecuteRequestBody,
    ExecuteResponseBody, HandshakeResponse, IDEMPOTENCY_KEY_HEADER, RedoResponseBody,
    SESSION_HEADER, SERVER_PROTOCOL_VERSION, SwitchBranchRequestBody, SwitchBranchResponseBody,
    TRANSACTION_HEADER, UndoResponseBody, closed_error, encode_engine_values,
    is_recoverable_session_error, protocol_error, remote_error, unsupported_remote_operation,
    validate_session_id,
};

pub use http::{
    ProtocolByteStream, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse, ProtocolHttpStream,
};
pub use observe::ProtocolObserveEvents;
pub use wire::{SERVER_CLOSED_CODE, SESSION_GONE_CODE};

const MIN_COMPRESSIBLE_JSON_BYTES: usize = 32 * 1024;
const COMPRESSION_SAMPLE_BYTES: usize = 32 * 1024;
const MAX_COMPRESSION_SAMPLE_RATIO: f64 = 0.7;
const MAX_COMPRESSED_BODY_RATIO: f64 = 0.9;

#[derive(Debug, Clone, Default)]
pub struct ProtocolExecuteOptions {
    pub origin_key: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug)]
struct ClientState {
    session_id: Option<String>,
    active_branch_id: Option<String>,
    active_account_id: Option<String>,
    blobs: BlobCache,
}

pub struct ClientCore<H> {
    http: Arc<H>,
    base_url: String,
    state: Arc<std::sync::Mutex<ClientState>>,
    operation_lock: Arc<Mutex<()>>,
    accepting: Arc<AtomicBool>,
}

impl<H> Clone for ClientCore<H> {
    fn clone(&self) -> Self {
        Self {
            http: Arc::clone(&self.http),
            base_url: self.base_url.clone(),
            state: Arc::clone(&self.state),
            operation_lock: Arc::clone(&self.operation_lock),
            accepting: Arc::clone(&self.accepting),
        }
    }
}

impl<H> std::fmt::Debug for ClientCore<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCore")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

pub struct ProtocolClient<H: ProtocolHttp> {
    core: ClientCore<H>,
    observe: observe::ObservationHub<ClientCore<H>>,
}

impl<H: ProtocolHttp> Clone for ProtocolClient<H> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            observe: self.observe.clone(),
        }
    }
}

impl<H: ProtocolHttp> std::fmt::Debug for ProtocolClient<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolClient").finish_non_exhaustive()
    }
}

impl<H> std::ops::Deref for ProtocolClient<H>
where
    H: ProtocolHttp,
{
    type Target = ClientCore<H>;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct ProtocolTransaction<H> {
    core: ClientCore<H>,
    transaction_id: String,
    active: AtomicBool,
}

impl<H> std::fmt::Debug for ProtocolTransaction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolTransaction")
            .field("transaction_id", &self.transaction_id)
            .finish()
    }
}

pub async fn open_protocol_client<H: ProtocolHttp + Clone + 'static>(
    http: H,
    base_url: impl Into<String>,
    initial_active_branch_id: Option<String>,
) -> Result<ProtocolClient<H>, LixError> {
    if let Some(branch_id) = &initial_active_branch_id
        && branch_id.is_empty()
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "initialActiveBranchId must be a non-empty string",
        ));
    }
    let core = ClientCore {
        http: Arc::new(http),
        base_url: normalize_protocol_base_url(&base_url.into())?,
        state: Arc::new(std::sync::Mutex::new(ClientState {
            session_id: None,
            active_branch_id: None,
            active_account_id: None,
            blobs: BlobCache::default(),
        })),
        operation_lock: Arc::new(Mutex::new(())),
        accepting: Arc::new(AtomicBool::new(true)),
    };
    core.handshake_create(initial_active_branch_id.as_deref())
        .await?;
    let observe = observe::ObservationHub::new(core.clone());
    Ok(ProtocolClient { core, observe })
}

impl<H: ProtocolHttp> ClientCore<H> {
    pub fn session_id(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .session_id
            .clone()
    }

    pub fn join_path(&self, path: &str) -> Result<String, LixError> {
        let base = url::Url::parse(&self.base_url).map_err(|error| {
            protocol_error(format!("invalid Lix Server Protocol base URL: {error}"))
        })?;
        Ok(base
            .join(path)
            .map_err(|error| protocol_error(format!("invalid Lix Server Protocol path: {error}")))?
            .to_string())
    }

    async fn enqueue<T, F, Fut>(&self, operation: F) -> Result<T, LixError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, LixError>>,
    {
        let _guard = self.operation_lock.lock().await;
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(closed_error());
        }
        operation().await
    }

    async fn handshake_create(&self, active_branch_id: Option<&str>) -> Result<(), LixError> {
        let mut url = url::Url::parse(&self.base_url).map_err(|error| {
            protocol_error(format!("invalid Lix Server Protocol base URL: {error}"))
        })?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(branch_id) = active_branch_id {
                query.append_pair("activeBranchId", branch_id);
            }
        }
        if url.query() == Some("") {
            url.set_query(None);
        }
        let handshake = self.request_handshake(url.to_string(), false).await?;
        self.apply_handshake(handshake)?;
        Ok(())
    }

    async fn handshake_resume(&self) -> Result<HandshakeResponse, LixError> {
        self.request_handshake(self.base_url.clone(), true).await
    }

    async fn recover_session_once(&self) -> Result<(), LixError> {
        let branch_id = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.session_id = None;
            state.active_branch_id.clone()
        };
        self.handshake_create(branch_id.as_deref()).await
    }

    async fn request_handshake(
        &self,
        url: String,
        include_session: bool,
    ) -> Result<HandshakeResponse, LixError> {
        let value = self
            .request_json::<HandshakeResponse, EmptyBody>(
                "GET",
                url,
                include_session,
                None,
                None,
                "json",
            )
            .await?;
        if value.protocol_version != SERVER_PROTOCOL_VERSION {
            return Err(protocol_error(format!(
                "unsupported Lix Server Protocol version: {}",
                value.protocol_version
            )));
        }
        if value.active_branch_id.is_empty() {
            return Err(protocol_error(
                "Lix Server Protocol handshake requires activeBranchId",
            ));
        }
        if value.active_account_id.is_empty() {
            return Err(protocol_error(
                "Lix Server Protocol handshake requires activeAccountId",
            ));
        }
        validate_session_id(&value.session_id)?;
        Ok(value)
    }

    fn apply_handshake(&self, handshake: HandshakeResponse) -> Result<(), LixError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.session_id = Some(handshake.session_id);
        state.active_branch_id = Some(handshake.active_branch_id);
        state.active_account_id = Some(handshake.active_account_id);
        Ok(())
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<ExecuteResult, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                self.execute_raw(
                    sql,
                    params,
                    options.clone(),
                    None,
                    true,
                )
                .await
            })
            .await
        })
        .await
    }

    pub async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| self.execute_batch_raw(statements, options.clone()))
                .await
        })
        .await
    }

    pub async fn execute_raw(
        &self,
        sql: &str,
        params: &[Value],
        options: Option<ProtocolExecuteOptions>,
        extra_headers: Option<Vec<(String, String)>>,
        cache_blobs: bool,
    ) -> Result<ExecuteResult, LixError> {
        let prepared = if cache_blobs {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .blobs
                .prepare(params, |index| request_blob_slot("execute", sql, index, None))
        } else {
            PreparedRequestParams {
                params: encode_engine_values(params)?,
                full_params: encode_engine_values(params)?,
                cache_updates: Vec::new(),
                cache_blobs: false,
                has_delta: false,
            }
        };
        let request_options = options.as_ref().and_then(|options| {
            options.origin_key.as_ref().map(|origin_key| ExecuteOptionsBody {
                origin_key: Some(origin_key.clone()),
            })
        });
        let mut headers = extra_headers.unwrap_or_default();
        if cache_blobs {
            headers.push((
                IDEMPOTENCY_KEY_HEADER.to_owned(),
                idempotency_key(options.as_ref())?,
            ));
        }
        let url = self.join_path("execute")?;
        let result = self
            .request_with_blob_fallback(&prepared, |params| {
                let body = ExecuteRequestBody {
                    sql: sql.to_owned(),
                    params: params.to_vec(),
                    options: request_options.clone(),
                    cache_blobs: prepared.cache_blobs,
                };
                self.request_json(
                    "POST",
                    url.clone(),
                    true,
                    Some(headers.clone()),
                    Some(body),
                    "json",
                )
            })
            .await?;
        if cache_blobs {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .blobs
                .commit(&prepared.cache_updates);
        }
        Ok(result)
    }

    async fn execute_batch_raw(
        &self,
        statements: &[ExecuteBatchStatement],
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let prepared: Vec<(String, Option<String>, PreparedRequestParams)> = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            statements
                .iter()
                .enumerate()
                .map(|(statement_index, statement)| {
                    (
                        statement.sql.clone(),
                        statement.label.clone(),
                        state.blobs.prepare(&statement.params, |param_index| {
                            request_blob_slot(
                                "batch",
                                &statement.sql,
                                param_index,
                                Some(statement_index),
                            )
                        }),
                    )
                })
                .collect()
        };
        let cache_blobs = prepared.iter().any(|(_, _, item)| item.cache_blobs);
        let has_delta = prepared.iter().any(|(_, _, item)| item.has_delta);
        let request_options = options.as_ref().and_then(|options| {
            options.origin_key.as_ref().map(|origin_key| ExecuteOptionsBody {
                origin_key: Some(origin_key.clone()),
            })
        });
        let headers = vec![(
            IDEMPOTENCY_KEY_HEADER.to_owned(),
            idempotency_key(options.as_ref())?,
        )];
        let url = self.join_path("execute-batch")?;
        let request = |full: bool| {
            let statements = prepared
                .iter()
                .map(|(sql, label, item)| ExecuteBatchStatementBody {
                    sql: sql.clone(),
                    params: if full {
                        item.full_params.clone()
                    } else {
                        item.params.clone()
                    },
                    label: label.clone(),
                })
                .collect();
            let body = ExecuteBatchRequestBody {
                statements,
                options: request_options.clone(),
                cache_blobs,
            };
            self.request_json::<Vec<ExecuteResponseBody>, _>(
                "POST",
                url.clone(),
                true,
                Some(headers.clone()),
                Some(body),
                "json",
            )
        };
        let value = match request(false).await {
            Err(error) if has_delta && error.code == BLOB_BASE_MISSING_CODE => request(true).await?,
            other => other?,
        };
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .blobs
            .commit(
                &prepared
                    .iter()
                    .flat_map(|(_, _, item)| item.cache_updates.clone())
                    .collect::<Vec<_>>(),
            );
        value
            .into_iter()
            .map(ExecuteResponseBody::into_execute_result)
            .collect()
    }

    async fn request_with_blob_fallback<F, Fut>(
        &self,
        prepared: &PreparedRequestParams,
        request: F,
    ) -> Result<ExecuteResult, LixError>
    where
        F: Fn(&[RequestWireValue]) -> Fut,
        Fut: Future<Output = Result<ExecuteResponseBody, LixError>>,
    {
        let result = match request(&prepared.params).await {
            Err(error) if prepared.has_delta && error.code == BLOB_BASE_MISSING_CODE => {
                request(&prepared.full_params).await?
            }
            other => other?,
        };
        result.into_execute_result()
    }

    async fn with_session_recovery<T, F, Fut>(&self, mut operation: F) -> Result<T, LixError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, LixError>>,
    {
        match operation().await {
            Err(error) if is_recoverable_session_error(&error) => {
                self.recover_session_once().await?;
                operation().await
            }
            other => other,
        }
    }

    pub async fn begin_transaction(&self) -> Result<ProtocolTransaction<H>, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                let begun = self
                    .request_json::<BeginTransactionResponse, EmptyBody>(
                        "POST",
                        self.join_path("transaction/begin")?,
                        true,
                        None,
                        Some(EmptyBody {}),
                        "json",
                    )
                    .await?;
                if begun.transaction_id.is_empty() {
                    return Err(protocol_error(
                        "begin transaction response.transactionId must be a string",
                    ));
                }
                Ok(ProtocolTransaction {
                    core: self.clone(),
                    transaction_id: begun.transaction_id,
                    active: AtomicBool::new(true),
                })
            })
            .await
        })
        .await
    }

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.enqueue(|| async {
            let cached = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active_branch_id
                .clone();
            if let Some(branch_id) = cached {
                return Ok(branch_id);
            }
            let handshake = self.handshake_resume().await?;
            let current = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .session_id
                .clone();
            if current.as_deref() != Some(handshake.session_id.as_str()) {
                return Err(protocol_error(
                    "Lix Server Protocol handshake changed sessionId",
                ));
            }
            self.apply_handshake(handshake)?;
            Ok(self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active_branch_id
                .clone()
                .ok_or_else(|| {
                    protocol_error("Lix Server Protocol handshake requires activeBranchId")
                })?)
        })
        .await
    }

    pub async fn active_account_id(&self) -> Result<String, LixError> {
        self.enqueue(|| async {
            let cached = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active_account_id
                .clone();
            if let Some(account_id) = cached {
                return Ok(account_id);
            }
            let handshake = self.handshake_resume().await?;
            let current = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .session_id
                .clone();
            if current.as_deref() != Some(handshake.session_id.as_str()) {
                return Err(protocol_error(
                    "Lix Server Protocol handshake changed sessionId",
                ));
            }
            self.apply_handshake(handshake)?;
            Ok(self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active_account_id
                .clone()
                .ok_or_else(|| {
                    protocol_error("Lix Server Protocol handshake requires activeAccountId")
                })?)
        })
        .await
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                let body = CreateBranchRequestBody {
                    id: options.id.clone(),
                    name: options.name.clone(),
                    from_commit_id: options.from_commit_id.clone(),
                };
                let value = self
                    .request_json::<CreateBranchResponseBody, _>(
                        "POST",
                        self.join_path("branch/create")?,
                        true,
                        None,
                        Some(body),
                        "json",
                    )
                    .await?;
                Ok(CreateBranchReceipt {
                    id: value.id,
                    name: value.name,
                    hidden: value.hidden,
                    commit_id: value.commit_id,
                })
            })
            .await
        })
        .await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                let value = self
                    .request_json::<CreateCheckpointResponseBody, EmptyBody>(
                        "POST",
                        self.join_path("checkpoint/create")?,
                        true,
                        None,
                        Some(EmptyBody {}),
                        "json",
                    )
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
        })
        .await
    }

    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                let value = self
                    .request_json::<UndoResponseBody, EmptyBody>(
                        "POST",
                        self.join_path("undo")?,
                        true,
                        None,
                        Some(EmptyBody {}),
                        "json",
                    )
                    .await?;
                Ok(UndoReceipt {
                    branch_id: value.branch_id,
                    target_commit_id: value.target_commit_id,
                    inverse_commit_id: value.inverse_commit_id,
                })
            })
            .await
        })
        .await
    }

    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.enqueue(|| async {
            self.with_session_recovery(|| async {
                let value = self
                    .request_json::<RedoResponseBody, EmptyBody>(
                        "POST",
                        self.join_path("redo")?,
                        true,
                        None,
                        Some(EmptyBody {}),
                        "json",
                    )
                    .await?;
                Ok(RedoReceipt {
                    branch_id: value.branch_id,
                    target_commit_id: value.target_commit_id,
                    replay_commit_id: value.replay_commit_id,
                })
            })
            .await
        })
        .await
    }

    pub async fn switch_branch(&self, branch_id: &str) -> Result<SwitchBranchReceipt, LixError> {
        self.enqueue(|| async {
            let result = self
                .with_session_recovery(|| async {
                    let body = SwitchBranchRequestBody { branch_id };
                    let value = self
                        .request_json::<SwitchBranchResponseBody, _>(
                            "POST",
                            self.join_path("branch/switch")?,
                            true,
                            None,
                            Some(body),
                            "json",
                        )
                        .await?;
                    if value.branch_id != branch_id {
                        return Err(protocol_error("switch branch response is invalid"));
                    }
                    Ok(value)
                })
                .await;
            match result {
                Ok(value) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .active_branch_id = Some(value.branch_id.clone());
                    Ok(SwitchBranchReceipt {
                        branch_id: value.branch_id,
                    })
                }
                Err(error) => {
                    if error_has_http_status(&error) && !is_definitive_client_error(&error) {
                        self.state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .active_branch_id = None;
                    }
                    Err(error)
                }
            }
        })
        .await
    }

    pub fn unsupported(&self, operation: &str) -> LixError {
        let _ = self;
        unsupported_remote_operation(operation)
    }

    async fn request_json<T, B>(
        &self,
        method: &str,
        url: String,
        include_session: bool,
        extra_headers: Option<Vec<(String, String)>>,
        body: Option<B>,
        response_kind: &str,
    ) -> Result<T, LixError>
    where
        T: serde::de::DeserializeOwned,
        B: Serialize,
    {
        let mut headers = vec![("accept".to_owned(), "application/json".to_owned())];
        if include_session && let Some(session_id) = self.session_id() {
            headers.push((SESSION_HEADER.to_owned(), session_id));
        }
        if let Some(extra) = extra_headers {
            headers.extend(extra);
        }
        let request_body = if method == "GET" || method == "DELETE" {
            None
        } else if let Some(body) = body {
            let encoded = serde_json::to_vec(&body).map_err(|error| {
                protocol_error(format!("could not encode Lix Server Protocol body: {error}"))
            })?;
            if encoded == b"{}" {
                None
            } else {
                let (encoded, compressed) = maybe_compress_json(encoded)?;
                headers.push(("content-type".to_owned(), "application/json".to_owned()));
                if compressed {
                    headers.push(("content-encoding".to_owned(), "gzip".to_owned()));
                }
                Some(encoded)
            }
        } else {
            None
        };
        let response = self
            .http
            .request(ProtocolHttpRequest {
                method: method.to_owned(),
                url,
                headers,
                body: request_body,
            })
            .await?;
        if response.status == 204 || response_kind == "empty" {
            if !is_success_status(response.status) {
                return Err(error_from_http_response(&response));
            }
            return serde_json::from_value(serde_json::Value::Null).or_else(|_| {
                serde_json::from_value(serde_json::json!({}))
                    .map_err(|_| protocol_error("empty remote response could not be decoded"))
            });
        }
        if !is_success_status(response.status) {
            return Err(error_from_http_response(&response));
        }
        serde_json::from_slice(&response.body).map_err(|_| {
            protocol_error(format!(
                "remote response {} did not contain valid JSON",
                response.status
            ))
        })
    }
}

impl<H: ProtocolHttp + Clone + 'static> ProtocolClient<H> {
    pub async fn observe(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ProtocolObserveEvents<ClientCore<H>>, LixError> {
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(closed_error());
        }
        self.observe.observe(sql.to_owned(), params).await
    }

    pub async fn switch_branch_and_restart(
        &self,
        branch_id: &str,
    ) -> Result<SwitchBranchReceipt, LixError> {
        let result = self.core.switch_branch(branch_id).await;
        match &result {
            Ok(_) => self.observe.restart(),
            Err(error) if !is_definitive_client_error(error) => self.observe.restart(),
            Err(_) => {}
        }
        result
    }

    pub async fn close(&self) -> Result<(), LixError> {
        if !self.accepting.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.observe.close().await;
        let _guard = self.operation_lock.lock().await;
        let result = self
            .request_json::<(), EmptyBody>(
                "DELETE",
                self.join_path("session")?,
                true,
                None,
                Some(EmptyBody {}),
                "empty",
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) if is_recoverable_session_error(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl<H: ProtocolHttp> ProtocolTransaction<H> {
    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<ExecuteResult, LixError> {
        self.assert_active()?;
        self.core
            .enqueue(|| async {
                self.assert_active()?;
                let request_options = options.as_ref().and_then(|options| {
                    options.origin_key.as_ref().map(|origin_key| ExecuteOptionsBody {
                        origin_key: Some(origin_key.clone()),
                    })
                });
                let body = ExecuteRequestBody {
                    sql: sql.to_owned(),
                    params: encode_engine_values(params)?,
                    options: request_options.clone(),
                    cache_blobs: false,
                };
                self.core
                    .request_json::<ExecuteResponseBody, _>(
                        "POST",
                        self.core.join_path("transaction/execute")?,
                        true,
                        Some(vec![(
                            TRANSACTION_HEADER.to_owned(),
                            self.transaction_id.clone(),
                        )]),
                        Some(body),
                        "json",
                    )
                    .await?
                    .into_execute_result()
            })
            .await
    }

    pub async fn commit(&self) -> Result<(), LixError> {
        self.assert_active()?;
        self.core
            .enqueue(|| async {
                self.assert_active()?;
                self.core
                    .request_json::<(), EmptyBody>(
                        "POST",
                        self.core.join_path("transaction/commit")?,
                        true,
                        Some(vec![(
                            TRANSACTION_HEADER.to_owned(),
                            self.transaction_id.clone(),
                        )]),
                        Some(EmptyBody {}),
                        "empty",
                    )
                    .await?;
                self.active.store(false, Ordering::SeqCst);
                Ok(())
            })
            .await
    }

    pub async fn rollback(&self) -> Result<(), LixError> {
        self.assert_active()?;
        self.core
            .enqueue(|| async {
                self.assert_active()?;
                self.core
                    .request_json::<(), EmptyBody>(
                        "POST",
                        self.core.join_path("transaction/rollback")?,
                        true,
                        Some(vec![(
                            TRANSACTION_HEADER.to_owned(),
                            self.transaction_id.clone(),
                        )]),
                        Some(EmptyBody {}),
                        "empty",
                    )
                    .await?;
                self.active.store(false, Ordering::SeqCst);
                Ok(())
            })
            .await
    }

    fn assert_active(&self) -> Result<(), LixError> {
        if self.active.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(LixError::new(
                "LIX_INVALID_TRANSACTION_STATE",
                "Lix transaction is closed",
            ))
        }
    }
}

fn normalize_protocol_base_url(value: &str) -> Result<String, LixError> {
    let mut parsed = url::Url::parse(value).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "openLix() remote server url must be an absolute URL",
        )
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "openLix() remote server url must use http or https",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "openLix() remote server url must not contain a query or fragment",
        ));
    }
    let mut path = parsed.path().trim_end_matches('/').to_owned();
    if !path.ends_with("/lix/v1") {
        path.push_str("/lix/v1");
    }
    path.push('/');
    parsed.set_path(&path);
    Ok(parsed.to_string())
}

fn maybe_compress_json(body: Vec<u8>) -> Result<(Bytes, bool), LixError> {
    if body.len() < MIN_COMPRESSIBLE_JSON_BYTES {
        return Ok((Bytes::from(body), false));
    }
    let sample_len = body.len().min(COMPRESSION_SAMPLE_BYTES);
    let sample = gzip_bytes(&body[..sample_len])?;
    if (sample.len() as f64) > (sample_len as f64) * MAX_COMPRESSION_SAMPLE_RATIO {
        return Ok((Bytes::from(body), false));
    }
    let compressed = gzip_bytes(&body)?;
    if (compressed.len() as f64) > (body.len() as f64) * MAX_COMPRESSED_BODY_RATIO {
        return Ok((Bytes::from(body), false));
    }
    Ok((Bytes::from(compressed), true))
}

fn gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, LixError> {
    use std::io::Write as _;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(1));
    encoder
        .write_all(bytes)
        .and_then(|()| encoder.finish())
        .map_err(|error| protocol_error(format!("could not gzip Lix Server Protocol body: {error}")))
}

fn idempotency_key(options: Option<&ProtocolExecuteOptions>) -> Result<String, LixError> {
    let key = match options.and_then(|options| options.idempotency_key.as_deref()) {
        Some(key) => key.to_owned(),
        None => uuid::Uuid::new_v4().to_string(),
    };
    if !(1..=255).contains(&key.len())
        || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "options.idempotencyKey must contain 1 to 255 visible ASCII characters",
        ));
    }
    Ok(key)
}

fn error_from_http_response(response: &ProtocolHttpResponse) -> LixError {
    if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(&response.body) {
        let mut details = envelope
            .error
            .details
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = details.as_object_mut() {
            object.insert("httpStatus".to_owned(), serde_json::json!(response.status));
        } else {
            details = serde_json::json!({
                "httpStatus": response.status,
                "body": details,
            });
        }
        let mut error = remote_error(
            envelope
                .error
                .code
                .unwrap_or_else(|| "LIX_REMOTE_REQUEST_FAILED".to_owned()),
            envelope.error.message.unwrap_or_else(|| {
                format!(
                    "Remote Lix request failed with status {}",
                    response.status
                )
            }),
        )
        .with_details(details);
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return error;
    }
    let details = if response.body.is_empty() {
        serde_json::json!({ "httpStatus": response.status })
    } else {
        serde_json::json!({
            "httpStatus": response.status,
            "body": String::from_utf8_lossy(&response.body).chars().take(1000).collect::<String>(),
        })
    };
    remote_error(
        "LIX_REMOTE_REQUEST_FAILED",
        format!(
            "Remote Lix request failed with status {}",
            response.status
        ),
    )
    .with_details(details)
}

fn is_success_status(status: u16) -> bool {
    (200..300).contains(&status)
}

fn error_http_status(error: &LixError) -> Option<u64> {
    error
        .details
        .as_ref()
        .and_then(|details| details.get("httpStatus"))
        .and_then(serde_json::Value::as_u64)
}

fn error_has_http_status(error: &LixError) -> bool {
    error_http_status(error).is_some()
}

fn is_definitive_client_error(error: &LixError) -> bool {
    let status = error_http_status(error).unwrap_or(0);
    (400..500).contains(&status) && status != 408 && status != 429
}

use wire::RequestWireValue;
