use js_sys::{Array, Function, Object, Promise, Reflect};
use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

use crate::LixError;
use crate::sync::{
    MAX_SYNC_PULL_RESPONSE_BYTES, SyncAdmission, SyncBranch, SyncPullResponse, SyncTransactionPack,
    SyncTransport, SyncTransportFuture, validate_sync_branch_id, validate_sync_remote_id,
};

const SESSION_HEADER: &str = "lix-session-id";
const IDEMPOTENCY_HEADER: &str = "idempotency-key";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    active_branch_id: String,
    session_id: String,
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
    session_id: String,
    branch_id: String,
}

impl HttpSyncTransport {
    pub(crate) async fn connect(
        repository_url: &str,
        headers: &[(String, String)],
        active_branch_id: Option<&str>,
    ) -> Result<Self, LixError> {
        let repository_url = repository_url.trim_end_matches('/').to_owned();
        validate_sync_remote_id(&repository_url)?;
        let protocol_url = format!("{repository_url}/lix/v1");
        let handshake_url = match active_branch_id {
            Some(branch_id) => format!(
                "{protocol_url}?activeBranchId={}",
                js_sys::encode_uri_component(branch_id)
            ),
            None => protocol_url.clone(),
        };
        let response = fetch(&handshake_url, "GET", headers, None).await?;
        let handshake: HandshakeResponse = decode_response(response, "open sync session")?;
        validate_sync_branch_id(&handshake.active_branch_id)?;
        if handshake.session_id.is_empty() || handshake.session_id.len() > 4096 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid session identity",
            ));
        }
        Ok(Self {
            repository_url,
            protocol_url,
            headers: headers.to_vec(),
            session_id: handshake.session_id,
            branch_id: handshake.active_branch_id,
        })
    }

    pub(crate) fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub(crate) async fn close(self) -> Result<(), LixError> {
        let headers = self.session_headers(None);
        let response = fetch(
            &format!("{}/session", self.protocol_url),
            "DELETE",
            &headers,
            None,
        )
        .await?;
        ensure_success(response, "close sync session")
    }

    fn session_headers(&self, idempotency_key: Option<&str>) -> Vec<(String, String)> {
        let mut headers = self.headers.clone();
        headers.push((SESSION_HEADER.to_owned(), self.session_id.clone()));
        if let Some(key) = idempotency_key {
            headers.push((IDEMPOTENCY_HEADER.to_owned(), key.to_owned()));
        }
        headers
    }
}

impl SyncTransport for HttpSyncTransport {
    fn remote_id(&self) -> &str {
        &self.repository_url
    }

    fn admit<'a>(
        &'a self,
        pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async move {
            let body = serde_json::to_string(pack).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("encode sync transaction: {error}"),
                )
            })?;
            let mut headers = self.session_headers(Some(&pack.operation_id));
            headers.push(("content-type".to_owned(), "application/json".to_owned()));
            let response = fetch(
                &format!("{}/sync/admit", self.protocol_url),
                "POST",
                &headers,
                Some(&body),
            )
            .await?;
            decode_response(response, "admit sync transaction")
        })
    }

    fn pull<'a>(
        &'a self,
        branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async move {
            let mut url = format!(
                "{}/sync/pull?after={after_cursor}&limit={limit}&branch={}",
                self.protocol_url,
                js_sys::encode_uri_component(branch_id)
            );
            if !schema_keys.is_empty() {
                url.push_str("&schemas=");
                let schemas = js_sys::encode_uri_component(&schema_keys.join(","));
                url.push_str(&String::from(schemas));
            }
            let response = fetch(&url, "GET", &self.session_headers(None), None).await?;
            decode_response(response, "pull sync transactions")
        })
    }

    fn list_branches<'a>(&'a self) -> SyncTransportFuture<'a, Vec<SyncBranch>> {
        Box::pin(async move {
            let response = fetch(
                &format!("{}/sync/branches", self.protocol_url),
                "GET",
                &self.session_headers(None),
                None,
            )
            .await?;
            decode_response(response, "list sync branches")
        })
    }

    fn has_authoritative_branch_catalog(&self) -> bool {
        true
    }
}

struct FetchResponse {
    status: u16,
    status_text: String,
    body: String,
}

async fn fetch(
    url: &str,
    method: &str,
    headers: &[(String, String)],
    body: Option<&str>,
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
    // Dropping the Rust future is how the shared runtime interrupts a held
    // long poll for a local write or shutdown. A JavaScript Promise keeps the
    // underlying request alive after its JsFuture is dropped, so tie that
    // cancellation boundary to AbortController explicitly.
    let mut abort_on_drop = AbortOnDrop {
        controller: controller.into(),
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
        Reflect::set(&init, &"body".into(), &body.into()).map_err(js_transport_error)?;
    }

    let fetch = Reflect::get(&global, &"fetch".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?;
    let promise = fetch
        .call2(&global, &url.into(), &init)
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
    let text = Reflect::get(&response, &"text".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?
        .call0(&response)
        .map_err(js_transport_error)?
        .dyn_into::<Promise>()
        .map_err(js_transport_error)?;
    let body = JsFuture::from(text)
        .await
        .map_err(js_transport_error)?
        .as_string()
        .unwrap_or_default();
    if body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes"),
        ));
    }
    abort_on_drop.armed = false;
    Ok(FetchResponse {
        status,
        status_text,
        body,
    })
}

struct AbortOnDrop {
    controller: Object,
    armed: bool,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(abort) = Reflect::get(&self.controller, &"abort".into())
            && let Ok(abort) = abort.dyn_into::<Function>()
        {
            let _ = abort.call0(&self.controller);
        }
    }
}

fn ensure_success(response: FetchResponse, operation: &str) -> Result<(), LixError> {
    if (200..300).contains(&response.status) {
        return Ok(());
    }
    Err(response_error(&response, operation))
}

fn decode_response<T>(response: FetchResponse, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    if !(200..300).contains(&response.status) {
        return Err(response_error(&response, operation));
    }
    serde_json::from_str(&response.body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

fn response_error(response: &FetchResponse, operation: &str) -> LixError {
    if let Ok(envelope) = serde_json::from_str::<ErrorResponse>(&response.body)
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
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "{operation} failed with {} {}: {}",
            response.status, response.status_text, response.body
        ),
    )
}

fn js_transport_error(error: JsValue) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("browser sync fetch failed: {error:?}"),
    )
}
