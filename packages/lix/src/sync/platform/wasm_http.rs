//! Browser fetch mechanics for the repository-scoped sync protocol.

use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::JsFuture;

use crate::LixError;
use crate::sync::{
    MAX_SYNC_HISTORY_COMMIT_IDS, MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT,
    SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRowPage, SyncTransport, SyncTransportFuture,
    validate_blake3_id, validate_sync_remote_id,
};

const SESSION_HEADER: &str = "lix-session-id";
pub const BROWSER_TRANSPORT_CONFIG_HEADER: &str = "x-lix-internal-browser-transport";

#[derive(Clone)]
struct BrowserTransportConfig {
    header_provider: Option<Function>,
    fetch: Option<Function>,
}

thread_local! {
    static BROWSER_TRANSPORT_CONFIGS: RefCell<HashMap<String, BrowserTransportConfig>> =
        RefCell::new(HashMap::new());
}

pub fn register_browser_sync_transport(
    id: String,
    header_provider: Option<Function>,
    fetch: Option<Function>,
) {
    BROWSER_TRANSPORT_CONFIGS.with(|configs| {
        configs.borrow_mut().insert(
            id,
            BrowserTransportConfig {
                header_provider,
                fetch,
            },
        );
    });
}

pub fn unregister_browser_sync_transport(id: &str) {
    BROWSER_TRANSPORT_CONFIGS.with(|configs| {
        configs.borrow_mut().remove(id);
    });
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    session_id: String,
    active_account_id: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    hint: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct HttpSyncTransport {
    repository_url: String,
    protocol_url: String,
    headers: Vec<(String, String)>,
    header_provider: Option<Function>,
    fetch: Option<Function>,
    session_id: String,
    active_account_id: String,
}

impl HttpSyncTransport {
    pub(crate) async fn connect(
        repository_url: &str,
        headers: &[(String, String)],
    ) -> Result<Self, LixError> {
        let repository_url = repository_url.trim_end_matches('/').to_owned();
        validate_sync_remote_id(&repository_url)?;
        let config_id = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(BROWSER_TRANSPORT_CONFIG_HEADER))
            .map(|(_, value)| value.clone());
        let config = config_id.as_deref().and_then(|id| {
            BROWSER_TRANSPORT_CONFIGS.with(|configs| configs.borrow().get(id).cloned())
        });
        if config_id.is_some() && config.is_none() {
            return Err(LixError::new(
                LixError::CODE_CLOSED,
                "browser sync transport callbacks are no longer registered",
            ));
        }
        let headers = headers
            .iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case(BROWSER_TRANSPORT_CONFIG_HEADER))
            .cloned()
            .collect::<Vec<_>>();
        let protocol_url = format!("{repository_url}/lix/v1");
        let resolved_headers = resolve_request_headers(
            &headers,
            config
                .as_ref()
                .and_then(|value| value.header_provider.as_ref()),
        )
        .await?;
        let response = fetch(
            &protocol_url,
            "GET",
            &resolved_headers,
            None,
            config.as_ref().and_then(|value| value.fetch.as_ref()),
        )
        .await?;
        let handshake: HandshakeResponse = decode_response(response, "open sync session")?;
        if handshake.session_id.is_empty() || handshake.session_id.len() > 4096 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid session identity",
            ));
        }
        crate::row_pk::RowPk::uuid_from_canonical(&handshake.active_account_id).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid active account identity",
            )
        })?;
        Ok(Self {
            repository_url,
            protocol_url,
            headers,
            header_provider: config
                .as_ref()
                .and_then(|value| value.header_provider.clone()),
            fetch: config.and_then(|value| value.fetch),
            session_id: handshake.session_id,
            active_account_id: handshake.active_account_id,
        })
    }

    pub(crate) fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    async fn session_headers(&self) -> Result<Vec<(String, String)>, LixError> {
        let mut headers =
            resolve_request_headers(&self.headers, self.header_provider.as_ref()).await?;
        headers.push((SESSION_HEADER.to_owned(), self.session_id.clone()));
        Ok(headers)
    }
}

impl SyncTransport for HttpSyncTransport {
    fn remote_id(&self) -> &str {
        &self.repository_url
    }

    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn push<'a>(
        &'a self,
        request: &'a SyncPushRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            let body = json_body(request, "encode sync push")?;
            let mut headers = self.session_headers().await?;
            headers.push(("content-type".to_owned(), "application/json".to_owned()));
            let response = fetch(
                &format!("{}/sync/push", self.protocol_url),
                "POST",
                &headers,
                Some(body),
                self.fetch.as_ref(),
            )
            .await?;
            decode_response(response, "push sync commits")
        })
    }

    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            let url = match after {
                Some(after) => format!(
                    "{}/sync/pull?after={after}&limit={limit}",
                    self.protocol_url
                ),
                None => format!("{}/sync/pull?limit={limit}", self.protocol_url),
            };
            let headers = self.session_headers().await?;
            let response = fetch(&url, "GET", &headers, None, self.fetch.as_ref()).await?;
            decode_response(response, "pull sync repository")
        })
    }

    fn snapshot_rows<'a>(
        &'a self,
        branch_id: &'a str,
        head_commit_id: &'a str,
        continuation: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncSnapshotRowPage> {
        Box::pin(async move {
            let mut url = format!(
                "{}/sync/pull?snapshotBranchId={}&snapshotHeadCommitId={}&limit={limit}",
                self.protocol_url,
                encode_query(branch_id),
                encode_query(head_commit_id),
            );
            if let Some(continuation) = continuation {
                url.push_str("&snapshotAfter=");
                url.push_str(&encode_query(continuation));
            }
            let response = fetch(
                &url,
                "GET",
                &self.session_headers().await?,
                None,
                self.fetch.as_ref(),
            )
            .await?;
            decode_response(response, "load sync snapshot rows")
        })
    }

    fn history<'a>(
        &'a self,
        commit_ids: &'a [String],
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            if commit_ids.is_empty() || commit_ids.len() > MAX_SYNC_HISTORY_COMMIT_IDS {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync history requires 1 through {MAX_SYNC_HISTORY_COMMIT_IDS} commit IDs"
                    ),
                ));
            }
            let query = commit_ids
                .iter()
                .map(|commit_id| format!("commitId={}", encode_query(commit_id)))
                .collect::<Vec<_>>()
                .join("&");
            let response = fetch(
                &format!("{}/sync/history?{query}", self.protocol_url),
                "GET",
                &self.session_headers().await?,
                None,
                self.fetch.as_ref(),
            )
            .await?;
            decode_response(response, "load sync history")
        })
    }

    fn get_blob<'a>(
        &'a self,
        blob_id: &'a str,
    ) -> SyncTransportFuture<'a, Option<SyncBlobManifest>> {
        Box::pin(async move {
            validate_blake3_id(blob_id, "blob ID")?;
            let response = fetch(
                &format!(
                    "{}/sync/blob?blobId={}",
                    self.protocol_url,
                    encode_query(blob_id)
                ),
                "GET",
                &self.session_headers().await?,
                None,
                self.fetch.as_ref(),
            )
            .await?;
            decode_optional_response(response, "load sync blob manifest")
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            validate_blake3_id(&manifest.blob_id, "blob ID")?;
            let body = json_body(manifest, "encode sync blob manifest")?;
            let mut headers = self.session_headers().await?;
            headers.push(("content-type".to_owned(), "application/json".to_owned()));
            let response = fetch(
                &format!("{}/sync/blob", self.protocol_url),
                "POST",
                &headers,
                Some(body),
                self.fetch.as_ref(),
            )
            .await?;
            decode_response(response, "register sync blob manifest")
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let response = fetch(
                &format!(
                    "{}/sync/chunk?chunkId={}",
                    self.protocol_url,
                    encode_query(chunk_id)
                ),
                "GET",
                &self.session_headers().await?,
                None,
                self.fetch.as_ref(),
            )
            .await?;
            if response.status == 404 {
                return Ok(None);
            }
            ensure_success_ref(&response, "load sync chunk")?;
            Ok(Some(response.body))
        })
    }

    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let mut headers = self.session_headers().await?;
            headers.push((
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ));
            let body: JsValue = Uint8Array::from(bytes).into();
            let response = fetch(
                &format!(
                    "{}/sync/chunk?chunkId={}",
                    self.protocol_url,
                    encode_query(chunk_id)
                ),
                "PUT",
                &headers,
                Some(body),
                self.fetch.as_ref(),
            )
            .await?;
            ensure_success(response, "store sync chunk")
        })
    }
}

struct FetchResponse {
    status: u16,
    status_text: String,
    body: Vec<u8>,
}

async fn resolve_request_headers(
    static_headers: &[(String, String)],
    provider: Option<&Function>,
) -> Result<Vec<(String, String)>, LixError> {
    let Some(provider) = provider else {
        return Ok(static_headers.to_vec());
    };
    let value = provider
        .call0(&JsValue::UNDEFINED)
        .map_err(js_transport_error)?;
    let value = JsFuture::from(Promise::resolve(&value))
        .await
        .map_err(js_transport_error)?;
    if !Array::is_array(&value) {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "browser sync header provider must return [name, value] pairs",
        ));
    }
    let dynamic = Array::from(&value)
        .iter()
        .map(|pair| {
            if !Array::is_array(&pair) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "browser sync header provider must return [name, value] pairs",
                ));
            }
            let pair = Array::from(&pair);
            let name = pair.get(0).as_string();
            let value = pair.get(1).as_string();
            match (pair.length(), name, value) {
                (2, Some(name), Some(value)) => Ok((name, value)),
                _ => Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "browser sync header provider must return [name, value] pairs",
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut headers = static_headers.to_vec();
    headers.extend(dynamic);
    Ok(headers)
}

async fn fetch(
    url: &str,
    method: &str,
    headers: &[(String, String)],
    body: Option<JsValue>,
    fetch_override: Option<&Function>,
) -> Result<FetchResponse, LixError> {
    let init = Object::new();
    let global = js_sys::global();
    let controller_constructor = Reflect::get(&global, &"AbortController".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?;
    let controller =
        Reflect::construct(&controller_constructor, &Array::new()).map_err(js_transport_error)?;
    let signal = Reflect::get(&controller, &"signal".into()).map_err(js_transport_error)?;
    Reflect::set(&init, &"signal".into(), &signal).map_err(js_transport_error)?;
    // Dropping the shared pull future must stop the browser request too. A
    // dropped `JsFuture` does not cancel its Promise, so cancellation lives at
    // this adapter boundary through AbortController.
    let controller: Object = controller.into();
    let timeout_controller = controller.clone();
    let timeout_callback: Closure<dyn FnMut()> = Closure::wrap(Box::new(move || {
        abort_controller(&timeout_controller);
    }));
    let set_timeout = Reflect::get(&global, &"setTimeout".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?;
    let timeout_ms =
        (SYNC_LONG_POLL_TIMEOUT + std::time::Duration::from_secs(5)).as_millis() as f64;
    let timeout_handle = set_timeout
        .call2(
            &global,
            timeout_callback.as_ref(),
            &JsValue::from_f64(timeout_ms),
        )
        .map_err(js_transport_error)?;
    let mut abort_on_drop = AbortOnDrop {
        controller,
        timeout: Some(BrowserTimeout {
            global: global.clone().into(),
            handle: timeout_handle,
            _callback: timeout_callback,
        }),
        armed: true,
    };
    Reflect::set(&init, &"method".into(), &method.into()).map_err(js_transport_error)?;
    Reflect::set(&init, &"credentials".into(), &"include".into()).map_err(js_transport_error)?;
    let header_pairs = Array::new();
    for (name, value) in headers {
        let pair = Array::new();
        pair.push(&name.into());
        pair.push(&value.into());
        header_pairs.push(&pair);
    }
    Reflect::set(&init, &"headers".into(), &header_pairs).map_err(js_transport_error)?;
    if let Some(body) = body {
        Reflect::set(&init, &"body".into(), &body).map_err(js_transport_error)?;
    }

    let fetch = match fetch_override {
        Some(fetch) => fetch.clone(),
        None => Reflect::get(&global, &"fetch".into())
            .map_err(js_transport_error)?
            .dyn_into::<Function>()
            .map_err(js_transport_error)?,
    };
    let this = if fetch_override.is_some() {
        JsValue::UNDEFINED
    } else {
        global.clone().into()
    };
    let promise = fetch
        .call2(&this, &url.into(), &init)
        .map_err(js_transport_error)?
        .dyn_into::<Promise>()
        .map_err(js_transport_error)?;
    let response = JsFuture::from(promise).await.map_err(js_transport_error)?;
    let status = Reflect::get(&response, &"status".into())
        .map_err(js_transport_error)?
        .as_f64()
        .unwrap_or_default() as u16;
    let status_text = Reflect::get(&response, &"statusText".into())
        .map_err(js_transport_error)?
        .as_string()
        .unwrap_or_default();
    let array_buffer = Reflect::get(&response, &"arrayBuffer".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?
        .call0(&response)
        .map_err(js_transport_error)?
        .dyn_into::<Promise>()
        .map_err(js_transport_error)?;
    let body = JsFuture::from(array_buffer)
        .await
        .map_err(js_transport_error)?;
    let body = Uint8Array::new(&body).to_vec();
    if body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes"),
        ));
    }
    abort_on_drop.disarm();
    Ok(FetchResponse {
        status,
        status_text,
        body,
    })
}

struct AbortOnDrop {
    controller: Object,
    timeout: Option<BrowserTimeout>,
    armed: bool,
}

struct BrowserTimeout {
    global: JsValue,
    handle: JsValue,
    _callback: Closure<dyn FnMut()>,
}

impl AbortOnDrop {
    fn disarm(&mut self) {
        self.clear_timeout();
        self.armed = false;
    }

    fn clear_timeout(&mut self) {
        let Some(timeout) = self.timeout.take() else {
            return;
        };
        if let Ok(clear_timeout) = Reflect::get(&timeout.global, &"clearTimeout".into())
            && let Ok(clear_timeout) = clear_timeout.dyn_into::<Function>()
        {
            let _ = clear_timeout.call1(&timeout.global, &timeout.handle);
        }
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.clear_timeout();
        if !self.armed {
            return;
        }
        abort_controller(&self.controller);
    }
}

fn abort_controller(controller: &Object) {
    if let Ok(abort) = Reflect::get(controller, &"abort".into())
        && let Ok(abort) = abort.dyn_into::<Function>()
    {
        let _ = abort.call0(controller);
    }
}

fn json_body(value: &impl serde::Serialize, operation: &str) -> Result<JsValue, LixError> {
    serde_json::to_string(value)
        .map(JsValue::from)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("{operation}: {error}"),
            )
        })
}

fn encode_query(value: &str) -> String {
    String::from(js_sys::encode_uri_component(value))
}

fn ensure_success(response: FetchResponse, operation: &str) -> Result<(), LixError> {
    ensure_success_ref(&response, operation)
}

fn ensure_success_ref(response: &FetchResponse, operation: &str) -> Result<(), LixError> {
    if (200..300).contains(&response.status) {
        return Ok(());
    }
    Err(response_error(response, operation))
}

fn decode_response<T>(response: FetchResponse, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    ensure_success_ref(&response, operation)?;
    serde_json::from_slice(&response.body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

fn decode_optional_response<T>(
    response: FetchResponse,
    operation: &str,
) -> Result<Option<T>, LixError>
where
    T: serde::de::DeserializeOwned,
{
    if response.status == 404 {
        return Ok(None);
    }
    decode_response(response, operation).map(Some)
}

fn response_error(response: &FetchResponse, operation: &str) -> LixError {
    if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(&response.body)
        && !envelope.error.code.is_empty()
    {
        let mut error = LixError::new(
            envelope.error.code,
            format!("{operation}: {}", envelope.error.message),
        );
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return error;
    }
    if response.status == 413 {
        return LixError::new(
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("{operation} exceeded an HTTP intermediary transfer limit"),
        );
    }
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "{operation} failed with {} {}: {}",
            response.status,
            response.status_text,
            String::from_utf8_lossy(&response.body)
        ),
    )
}

fn js_transport_error(error: JsValue) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("browser sync fetch failed: {error:?}"),
    )
}
