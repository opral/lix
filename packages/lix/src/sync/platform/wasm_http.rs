//! Browser fetch mechanics for the repository-scoped sync protocol.

use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use std::cell::RefCell;
use std::collections::HashMap;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::JsFuture;

use super::super::http::{
    HTTP_TIMEOUT, HttpSyncTransport, Method, RawHttpClient, RawHttpRequest, RawHttpResponse,
    response_too_large,
};
use crate::LixError;
use crate::sync::SyncTransportFuture;

#[doc(hidden)]
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

#[doc(hidden)]
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

#[doc(hidden)]
pub fn unregister_browser_sync_transport(id: &str) {
    BROWSER_TRANSPORT_CONFIGS.with(|configs| {
        configs.borrow_mut().remove(id);
    });
}

#[derive(Clone)]
pub(crate) struct BrowserHttpClient {
    headers: Vec<(String, String)>,
    header_provider: Option<Function>,
    fetch: Option<Function>,
}

impl std::fmt::Debug for BrowserHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserHttpClient")
            .field("headers", &self.headers)
            .field("has_header_provider", &self.header_provider.is_some())
            .field("has_fetch_override", &self.fetch.is_some())
            .finish()
    }
}

impl HttpSyncTransport<BrowserHttpClient> {
    pub(crate) async fn connect(
        repository_url: &str,
        headers: &[(String, String)],
    ) -> Result<Self, LixError> {
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
        let client = BrowserHttpClient {
            headers: headers
                .iter()
                .filter(|(name, _)| !name.eq_ignore_ascii_case(BROWSER_TRANSPORT_CONFIG_HEADER))
                .cloned()
                .collect(),
            header_provider: config
                .as_ref()
                .and_then(|value| value.header_provider.clone()),
            fetch: config.and_then(|value| value.fetch),
        };
        Self::connect_with(client, repository_url).await
    }
}

impl RawHttpClient for BrowserHttpClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let mut headers =
                resolve_request_headers(&self.headers, self.header_provider.as_ref()).await?;
            headers.extend(request.headers);
            fetch(
                &request.url,
                match request.method {
                    Method::Get => "GET",
                    Method::Post => "POST",
                    Method::Put => "PUT",
                },
                &headers,
                request.body,
                self.fetch.as_ref(),
                request.operation,
                request.response_limit,
            )
            .await
        })
    }
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
    body: Option<Vec<u8>>,
    fetch_override: Option<&Function>,
    operation: &str,
    response_limit: usize,
) -> Result<RawHttpResponse, LixError> {
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
    // A dropped `JsFuture` does not cancel its Promise. Keep cancellation at
    // the fetch boundary through AbortController.
    let controller: Object = controller.into();
    let timeout_controller = controller.clone();
    let timeout_callback: Closure<dyn FnMut()> = Closure::wrap(Box::new(move || {
        abort_controller(&timeout_controller);
    }));
    let set_timeout = Reflect::get(&global, &"setTimeout".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?;
    let timeout_handle = set_timeout
        .call2(
            &global,
            timeout_callback.as_ref(),
            &JsValue::from_f64(HTTP_TIMEOUT.as_millis() as f64),
        )
        .map_err(js_transport_error)?;
    let mut abort_on_drop = AbortOnDrop {
        controller: controller.clone(),
        timeout: Some(BrowserTimeout {
            global: global.clone().into(),
            handle: timeout_handle,
            _callback: timeout_callback,
        }),
        armed: true,
    };
    Reflect::set(&init, &"method".into(), &method.into()).map_err(js_transport_error)?;
    Reflect::set(&init, &"credentials".into(), &"include".into()).map_err(js_transport_error)?;
    Reflect::set(
        &init,
        &"lixResponseLimit".into(),
        &JsValue::from_f64(response_limit as f64),
    )
    .map_err(js_transport_error)?;
    let header_pairs = Array::new();
    for (name, value) in headers {
        let pair = Array::new();
        pair.push(&name.into());
        pair.push(&value.into());
        header_pairs.push(&pair);
    }
    Reflect::set(&init, &"headers".into(), &header_pairs).map_err(js_transport_error)?;
    if let Some(body) = body {
        let body: JsValue = Uint8Array::from(body.as_slice()).into();
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
    let body = read_response_body(&response, response_limit, operation, &controller).await?;
    abort_on_drop.disarm();
    Ok(RawHttpResponse {
        status,
        status_text,
        body,
    })
}

async fn read_response_body(
    response: &JsValue,
    response_limit: usize,
    operation: &str,
    controller: &Object,
) -> Result<Vec<u8>, LixError> {
    let stream = Reflect::get(response, &"body".into()).map_err(js_transport_error)?;
    if stream.is_null() || stream.is_undefined() {
        return Ok(Vec::new());
    }
    let reader = Reflect::get(&stream, &"getReader".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?
        .call0(&stream)
        .map_err(js_transport_error)?;
    let read = Reflect::get(&reader, &"read".into())
        .map_err(js_transport_error)?
        .dyn_into::<Function>()
        .map_err(js_transport_error)?;
    let mut body = Vec::new();
    loop {
        let result = read
            .call0(&reader)
            .map_err(js_transport_error)?
            .dyn_into::<Promise>()
            .map_err(js_transport_error)?;
        let result = JsFuture::from(result).await.map_err(js_transport_error)?;
        let done = Reflect::get(&result, &"done".into())
            .map_err(js_transport_error)?
            .as_bool()
            .unwrap_or(false);
        if done {
            release_reader(&reader);
            return Ok(body);
        }
        let chunk = Reflect::get(&result, &"value".into())
            .map_err(js_transport_error)?
            .dyn_into::<Uint8Array>()
            .map_err(js_transport_error)?;
        let chunk_len = usize::try_from(chunk.length()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "browser sync response chunk length exceeds usize",
            )
        })?;
        if body.len().saturating_add(chunk_len) > response_limit {
            abort_controller(controller);
            cancel_reader(&reader);
            return Err(response_too_large(operation));
        }
        let offset = body.len();
        body.resize(offset + chunk_len, 0);
        chunk.copy_to(&mut body[offset..]);
    }
}

fn cancel_reader(reader: &JsValue) {
    let Ok(cancel) = Reflect::get(reader, &"cancel".into()) else {
        return;
    };
    let Ok(cancel) = cancel.dyn_into::<Function>() else {
        return;
    };
    let Ok(result) = cancel.call0(reader) else {
        return;
    };
    let _ = result;
    release_reader(reader);
}

fn release_reader(reader: &JsValue) {
    if let Ok(release) = Reflect::get(reader, &"releaseLock".into())
        && let Ok(release) = release.dyn_into::<Function>()
    {
        let _ = release.call0(reader);
    }
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
        if self.armed {
            abort_controller(&self.controller);
        }
    }
}

fn abort_controller(controller: &Object) {
    if let Ok(abort) = Reflect::get(controller, &"abort".into())
        && let Ok(abort) = abort.dyn_into::<Function>()
    {
        let _ = abort.call0(controller);
    }
}

fn js_transport_error(error: JsValue) -> LixError {
    let code = Reflect::get(&error, &"code".into())
        .ok()
        .and_then(|code| code.as_string())
        .unwrap_or_else(|| LixError::CODE_INTERNAL_ERROR.to_owned());
    let detail = Reflect::get(&error, &"message".into())
        .ok()
        .and_then(|message| message.as_string())
        .unwrap_or_else(|| format!("{error:?}"));
    LixError::new(code, format!("browser sync fetch failed: {detail}"))
}
