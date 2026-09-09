use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use js_sys::{Array, Function, Promise, Reflect, Uint8Array};
use lix::LixError;
use lix::server_protocol::client::{
    ClientCore, ProtocolClient, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse,
    ProtocolHttpStream, ProtocolObserveEvents, ProtocolTransaction, open_protocol_client,
};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use super::{
    OpenAnotherSessionOptionsDto, execute_result_to_js, from_js, lix_error_to_js, values_from_js,
};

#[wasm_bindgen]
pub struct WasmRemoteLix {
    inner: ProtocolClient<JsHttp>,
}

super::session::wasm_session_methods!(WasmRemoteLix);

impl WasmRemoteLix {
    fn instrument_operation<F: Future>(&self, future: F) -> F {
        future
    }
}

#[wasm_bindgen]
pub struct WasmRemoteLixTransaction {
    inner: Option<ProtocolTransaction<JsHttp>>,
}

#[wasm_bindgen]
pub struct WasmRemoteObserveEvents {
    inner: RefCell<Option<ProtocolObserveEvents<ClientCore<JsHttp>>>>,
    next_in_flight: RefCell<bool>,
    pending_close: Cell<bool>,
}

#[derive(Clone)]
struct JsHttp {
    fetch: Function,
    headers: JsValue,
}

struct JsCancelOnDrop(Option<Arc<dyn Fn()>>);

// AbortController alone does not release a reader returned by a custom fetch.
// Keep reader ownership in the stream, including when it is never polled.
struct JsStreamReader(JsValue);

impl Drop for JsStreamReader {
    fn drop(&mut self) {
        let reader = self.0.clone();
        let cancel = Reflect::get(&reader, &JsValue::from_str("cancel"))
            .ok()
            .and_then(|value| value.dyn_into::<Function>().ok())
            .and_then(|cancel| cancel.call0(&reader).ok());
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(cancel) = cancel {
                let _ = JsFuture::from(Promise::from(cancel)).await;
            }
            if let Ok(release) = Reflect::get(&reader, &JsValue::from_str("releaseLock"))
                && let Ok(release) = release.dyn_into::<Function>()
            {
                let _ = release.call0(&reader);
            }
        });
    }
}

impl Drop for JsCancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

impl ProtocolHttp for JsHttp {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError> {
        let response = send_js_http(self, &request, false).await?;
        let status = js_status(&response)?;
        let headers = js_headers(&response)?;
        let body = js_array_buffer(&response).await?;
        Ok(ProtocolHttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStream, LixError> {
        let (response, cancel) = send_js_http_cancellable(self, &request).await?;
        let mut cancel_on_error = JsCancelOnDrop(Some(cancel.clone()));
        let status = js_status(&response)?;
        let headers = js_headers(&response)?;
        let empty_error_body = !(200..300).contains(&status)
            && Reflect::get(&response, &JsValue::from_str("body"))
                .is_ok_and(|body| body.is_null() || body.is_undefined());
        let body: lix::server_protocol::client::ProtocolByteStream = if empty_error_body {
            Box::pin(futures_util::stream::empty())
        } else {
            js_body_stream(&response)?
        };
        cancel_on_error.0 = None;
        Ok(ProtocolHttpStream {
            status,
            headers,
            body,
            cancel,
        })
    }

    async fn sleep(&self, duration: Duration) {
        js_sleep(duration).await;
    }

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()>>>) {
        wasm_bindgen_futures::spawn_local(fut);
    }
}

#[wasm_bindgen(js_name = openRemote)]
pub async fn open_remote(
    url: String,
    fetch: Function,
    headers: JsValue,
    initial_active_branch_id: Option<String>,
) -> Result<WasmRemoteLix, JsValue> {
    console_error_panic_hook::set_once();
    let http = JsHttp { fetch, headers };
    let inner = open_protocol_client(http.clone(), url.clone(), initial_active_branch_id)
        .await
        .map_err(lix_error_to_js)?;
    Ok(WasmRemoteLix { inner })
}

#[wasm_bindgen]
impl WasmRemoteLix {
    #[wasm_bindgen(js_name = replicaRecoverySources)]
    pub async fn replica_recovery_sources(&self) -> Result<JsValue, JsValue> {
        Err(lix_error_to_js(LixError::new(
            "LIX_ERROR_LOCAL_STORAGE_REQUIRED",
            "Replica recovery requires a local storage-backed Lix handle",
        )))
    }

    #[wasm_bindgen(js_name = exportReplicaRecovery)]
    pub async fn export_replica_recovery(&self, _id: String) -> Result<JsValue, JsValue> {
        Err(lix_error_to_js(LixError::new(
            "LIX_ERROR_LOCAL_STORAGE_REQUIRED",
            "Replica recovery requires a local storage-backed Lix handle",
        )))
    }

    #[wasm_bindgen(js_name = recoverReplica)]
    pub async fn recover_replica(&self, _id: String) -> Result<JsValue, JsValue> {
        Err(lix_error_to_js(LixError::new(
            "LIX_ERROR_LOCAL_STORAGE_REQUIRED",
            "Replica recovery requires a local storage-backed Lix handle",
        )))
    }

    #[wasm_bindgen(js_name = setTelemetryParent)]
    pub fn set_telemetry_parent(&self, _parent: Option<JsValue>) {
        // Remote repositories execute in the server process and do not own a
        // local telemetry sink. Keep the binding shape uniform; distributed
        // context propagation belongs to the server protocol transport.
    }

    #[wasm_bindgen(js_name = openAnotherSession)]
    pub async fn open_another_session(&self, options: JsValue) -> Result<WasmRemoteLix, JsValue> {
        let options: OpenAnotherSessionOptionsDto = from_js(options)?;
        let inner = crate::session::SessionOperations::open_another_session(
            &self.inner,
            options.branch_id,
            options.account_id,
        )
        .await
        .map_err(lix_error_to_js)?;
        Ok(WasmRemoteLix { inner })
    }

    #[wasm_bindgen(js_name = exportSnapshot)]
    pub fn export_snapshot(&self) -> super::WasmSnapshotExport {
        let (sender, receiver) = async_channel::bounded(1);
        let (completion_sender, completion) = async_channel::bounded(1);
        let inner = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            // Closing the receiver cancels both pending HTTP setup and reads.
            // The protocol stream owns transport cancellation on drop.
            futures_lite::future::race(
                async {
                    let result = async {
                        let mut export =
                            crate::session::SessionOperations::export_snapshot(&inner).await?;
                        while let Some(chunk) = export.next().await? {
                            if sender.send(Ok(Some(chunk.to_vec()))).await.is_err() {
                                return Ok::<(), LixError>(());
                            }
                        }
                        Ok(())
                    }
                    .await;
                    let _ = sender.send(result.map(|()| None)).await;
                },
                async { sender.closed().await },
            )
            .await;
            let _ = completion_sender.send(()).await;
        });
        super::WasmSnapshotExport {
            receiver,
            completion,
            canceled: Cell::new(false),
        }
    }

    #[wasm_bindgen(js_name = observe)]
    pub async fn observe(
        &self,
        sql: String,
        params: JsValue,
    ) -> Result<WasmRemoteObserveEvents, JsValue> {
        let params = values_from_js(params)?;
        let inner = crate::session::SessionOperations::observe(&self.inner, &sql, &params)
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmRemoteObserveEvents {
            inner: RefCell::new(Some(inner)),
            next_in_flight: RefCell::new(false),
            pending_close: Cell::new(false),
        })
    }

    #[wasm_bindgen(js_name = beginTransaction)]
    pub async fn begin_transaction(&self) -> Result<WasmRemoteLixTransaction, JsValue> {
        let inner = crate::session::SessionOperations::begin_transaction(&self.inner)
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmRemoteLixTransaction { inner: Some(inner) })
    }

    #[wasm_bindgen(js_name = importFilesystemPaths)]
    pub async fn import_filesystem_paths(&self, _paths: JsValue) -> Result<(), JsValue> {
        Err(lix_error_to_js(
            self.inner.unsupported("importFilesystemPaths"),
        ))
    }

    #[wasm_bindgen(js_name = syncDiskToLix)]
    pub async fn sync_disk_to_lix(&self) -> Result<(), JsValue> {
        Err(lix_error_to_js(self.inner.unsupported("syncDiskToLix")))
    }

    #[wasm_bindgen(js_name = close)]
    pub async fn close(&self) -> Result<(), JsValue> {
        crate::session::SessionOperations::close(&self.inner)
            .await
            .map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmRemoteLixTransaction {
    #[wasm_bindgen(js_name = execute)]
    pub async fn execute(
        &mut self,
        sql: String,
        params: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let params = values_from_js(params)?;
        let options = super::session_execute_options_from_js(options)?;
        let result =
            crate::session::TransactionOperations::execute(&mut self.inner, &sql, &params, options)
                .await
                .map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = commit)]
    pub async fn commit(&mut self) -> Result<(), JsValue> {
        crate::session::TransactionOperations::commit(&mut self.inner)
            .await
            .map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = rollback)]
    pub async fn rollback(&mut self) -> Result<(), JsValue> {
        crate::session::TransactionOperations::rollback(&mut self.inner)
            .await
            .map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmRemoteObserveEvents {
    #[wasm_bindgen(js_name = setTelemetryParent)]
    pub fn set_telemetry_parent(&self, _parent: Option<JsValue>) {}

    #[wasm_bindgen(js_name = next)]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if *self.next_in_flight.borrow() {
            return Err(super::observe_next_in_flight_error());
        }
        *self.next_in_flight.borrow_mut() = true;
        let result = async {
            let Some(events) = self.inner.borrow_mut().take() else {
                return Ok(JsValue::UNDEFINED);
            };
            let next = events.next().await;
            *self.inner.borrow_mut() = Some(events);
            if self.pending_close.get()
                && let Some(events) = self.inner.borrow().as_ref()
            {
                events.close();
            }
            match next {
                Ok(Some(event)) => observe_event_to_js(event),
                Ok(None) => Ok(JsValue::UNDEFINED),
                Err(error) => Err(lix_error_to_js(error)),
            }
        }
        .await;
        *self.next_in_flight.borrow_mut() = false;
        result
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&self) {
        self.pending_close.set(true);
        if let Some(events) = self.inner.borrow().as_ref() {
            events.close();
        }
    }
}

fn observe_event_to_js(event: lix::ObserveEvent) -> Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    let set = |key: &str, value: JsValue| {
        Reflect::set(&object, &JsValue::from_str(key), &value)
            .map_err(|_| JsValue::from(js_sys::Error::new("could not encode observe event")))
    };
    set("sequence", JsValue::from_f64(event.sequence as f64))?;
    set(
        "mutationSequence",
        JsValue::from_f64(event.mutation_sequence as f64),
    )?;
    set("rows", execute_result_to_js(event.rows)?)?;
    Ok(object.into())
}

async fn send_js_http(
    http: &JsHttp,
    request: &ProtocolHttpRequest,
    _stream: bool,
) -> Result<JsValue, LixError> {
    send_js_http_cancellable(http, request)
        .await
        .map(|(response, _)| response)
}

async fn send_js_http_cancellable(
    http: &JsHttp,
    request: &ProtocolHttpRequest,
) -> Result<(JsValue, Arc<dyn Fn()>), LixError> {
    let init = js_sys::Object::new();
    set_js(&init, "method", JsValue::from_str(&request.method))?;
    let headers = resolve_caller_headers(&http.headers).await?;
    delete_header(&headers, "lix-session-id")?;
    delete_header(&headers, "content-encoding")?;
    for (name, value) in &request.headers {
        if name.eq_ignore_ascii_case("lix-session-id") {
            continue;
        }
        append_header(&headers, name, value)?;
    }
    for (name, value) in &request.headers {
        if name.eq_ignore_ascii_case("lix-session-id") {
            append_header(&headers, name, value)?;
        }
    }
    set_js(&init, "headers", headers)?;
    if let Some(body) = &request.body {
        set_js(&init, "body", Uint8Array::from(body.as_ref()).into())?;
    }
    let controller = Reflect::get(&js_sys::global(), &JsValue::from_str("AbortController"))
        .ok()
        .and_then(|ctor| ctor.dyn_into::<Function>().ok())
        .and_then(|ctor| Reflect::construct(&ctor, &Array::new()).ok());
    let cancel: Arc<dyn Fn()> = if let Some(controller) = controller.clone() {
        set_js(
            &init,
            "signal",
            Reflect::get(&controller, &JsValue::from_str("signal")).unwrap_or(JsValue::UNDEFINED),
        )?;
        Arc::new(move || {
            if let Ok(abort) = Reflect::get(&controller, &JsValue::from_str("abort"))
                && let Ok(abort) = abort.dyn_into::<Function>()
            {
                let _ = abort.call0(&controller);
            }
        })
    } else {
        Arc::new(|| {})
    };
    let promise = http
        .fetch
        .call2(&JsValue::UNDEFINED, &JsValue::from_str(&request.url), &init)
        .map_err(|error| fetch_unavailable(error))?;
    let mut cancel_on_drop = JsCancelOnDrop(Some(cancel.clone()));
    let response = JsFuture::from(Promise::from(promise))
        .await
        .map_err(fetch_unavailable)?;
    cancel_on_drop.0 = None;
    Ok((response, cancel))
}

async fn resolve_caller_headers(headers: &JsValue) -> Result<JsValue, LixError> {
    if headers.is_undefined() || headers.is_null() {
        return Ok(js_headers_ctor(None)?);
    }
    let resolved = if let Some(function) = headers.dyn_ref::<Function>() {
        let value = function
            .call0(&JsValue::UNDEFINED)
            .map_err(|error| header_error(error))?;
        if let Some(promise) = value.dyn_ref::<Promise>() {
            JsFuture::from(promise.clone())
                .await
                .map_err(header_error)?
        } else {
            value
        }
    } else {
        headers.clone()
    };
    js_headers_ctor(Some(resolved))
}

fn js_headers_ctor(init: Option<JsValue>) -> Result<JsValue, LixError> {
    let ctor = Reflect::get(&js_sys::global(), &JsValue::from_str("Headers"))
        .map_err(|_| protocol_bridge("Headers is not available"))?;
    let ctor = ctor
        .dyn_into::<Function>()
        .map_err(|_| protocol_bridge("Headers is not available"))?;
    match init {
        Some(init) => Reflect::construct(&ctor, &Array::of1(&init)).map_err(header_error),
        None => Reflect::construct(&ctor, &Array::new()).map_err(header_error),
    }
}

fn append_header(headers: &JsValue, name: &str, value: &str) -> Result<(), LixError> {
    let set = Reflect::get(headers, &JsValue::from_str("set"))
        .map_err(|_| protocol_bridge("Headers.set is not available"))?;
    let set = set
        .dyn_into::<Function>()
        .map_err(|_| protocol_bridge("Headers.set is not available"))?;
    set.call2(headers, &JsValue::from_str(name), &JsValue::from_str(value))
        .map_err(|error| header_error(error))?;
    Ok(())
}

fn delete_header(headers: &JsValue, name: &str) -> Result<(), LixError> {
    let delete = Reflect::get(headers, &JsValue::from_str("delete"))
        .map_err(|_| protocol_bridge("Headers.delete is not available"))?;
    let delete = delete
        .dyn_into::<Function>()
        .map_err(|_| protocol_bridge("Headers.delete is not available"))?;
    delete
        .call1(headers, &JsValue::from_str(name))
        .map_err(|error| header_error(error))?;
    Ok(())
}

fn js_status(response: &JsValue) -> Result<u16, LixError> {
    Reflect::get(response, &JsValue::from_str("status"))
        .ok()
        .and_then(|value| value.as_f64())
        .map(|value| value as u16)
        .ok_or_else(|| protocol_bridge("remote response status is missing"))
}

fn js_headers(response: &JsValue) -> Result<Vec<(String, String)>, LixError> {
    let headers = Reflect::get(response, &JsValue::from_str("headers"))
        .map_err(|_| protocol_bridge("remote response headers are missing"))?;
    if headers.is_null() || headers.is_undefined() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    if let Some(value) = js_header_get(&headers, "content-type") {
        out.push(("content-type".to_owned(), value));
    }
    if let Some(value) = js_header_get(&headers, "content-encoding") {
        out.push(("content-encoding".to_owned(), value));
    }
    let iterable = Reflect::get(&headers, &JsValue::from_str("entries"))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
        .and_then(|function| function.call0(&headers).ok())
        .unwrap_or_else(|| headers.clone());
    let array = Array::from(&iterable);
    for entry in array.iter() {
        let pair = Array::from(&entry);
        if pair.length() >= 2
            && let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string())
        {
            if !out
                .iter()
                .any(|(existing, _)| existing.eq_ignore_ascii_case(&name))
            {
                out.push((name, value));
            }
        }
    }
    Ok(out)
}

fn js_header_get(headers: &JsValue, name: &str) -> Option<String> {
    let get = Reflect::get(headers, &JsValue::from_str("get"))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())?;
    get.call1(headers, &JsValue::from_str(name))
        .ok()
        .and_then(|value| value.as_string())
}

async fn js_array_buffer(response: &JsValue) -> Result<Bytes, LixError> {
    let function = Reflect::get(response, &JsValue::from_str("arrayBuffer"))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
        .ok_or_else(|| protocol_bridge("remote response arrayBuffer is missing"))?;
    let promise = function
        .call0(response)
        .map_err(|error| fetch_unavailable(error))?;
    let buffer = JsFuture::from(Promise::from(promise))
        .await
        .map_err(fetch_unavailable)?;
    Ok(Bytes::from(Uint8Array::new(&buffer).to_vec()))
}

fn js_body_stream(
    response: &JsValue,
) -> Result<lix::server_protocol::client::ProtocolByteStream, LixError> {
    let body = Reflect::get(response, &JsValue::from_str("body"))
        .map_err(|_| protocol_bridge("remote observe response has no body"))?;
    if body.is_null() || body.is_undefined() {
        return Err(LixError::new(
            "LIX_SERVER_PROTOCOL_ERROR",
            "remote observe response has no body",
        ));
    }
    let get_reader = Reflect::get(&body, &JsValue::from_str("getReader"))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
        .ok_or_else(|| protocol_bridge("remote observe response has no body"))?;
    let reader = get_reader
        .call0(&body)
        .map_err(|_| protocol_bridge("remote observe response has no body"))?;
    let reader = JsStreamReader(reader);
    Ok(Box::pin(async_stream::stream! {
        let reader_guard = reader;
        let reader = &reader_guard.0;
        loop {
            let read = match Reflect::get(&reader, &JsValue::from_str("read"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
            {
                Some(function) => function,
                None => break,
            };
            let promise = match read.call0(&reader) {
                Ok(value) => Promise::from(value),
                Err(error) => {
                    yield Err(fetch_unavailable(error));
                    break;
                }
            };
            match JsFuture::from(promise).await {
                Ok(chunk) => {
                    let done = Reflect::get(&chunk, &JsValue::from_str("done"))
                        .ok()
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false);
                    if done {
                        break;
                    }
                    let value = Reflect::get(&chunk, &JsValue::from_str("value"))
                        .unwrap_or(JsValue::UNDEFINED);
                    yield Ok(Bytes::from(Uint8Array::new(&value).to_vec()));
                }
                Err(error) => {
                    yield Err(fetch_unavailable(error));
                    break;
                }
            }
        }
    }))
}

async fn js_sleep(duration: Duration) {
    let millis = duration.as_millis() as i32;
    let promise = Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        if let Ok(set_timeout) = Reflect::get(&global, &JsValue::from_str("setTimeout"))
            && let Ok(set_timeout) = set_timeout.dyn_into::<Function>()
        {
            let _ = set_timeout.call2(&global, &resolve, &JsValue::from_f64(millis.max(0) as f64));
        } else {
            let _ = resolve.call0(&JsValue::UNDEFINED);
        }
    });
    let _ = JsFuture::from(promise).await;
}

fn set_js(object: &js_sys::Object, key: &str, value: JsValue) -> Result<(), LixError> {
    Reflect::set(object, &JsValue::from_str(key), &value)
        .map_err(|_| protocol_bridge("could not encode fetch init"))?;
    Ok(())
}

fn fetch_unavailable(error: JsValue) -> LixError {
    LixError::new(
        "LIX_REMOTE_UNAVAILABLE",
        "The remote Lix server is unavailable",
    )
    .with_details(serde_json::json!({
        "cause": js_error_message(error),
    }))
}

fn header_error(error: JsValue) -> LixError {
    LixError::new("LIX_REMOTE_CONFIGURATION_ERROR", js_error_message(error))
}

fn protocol_bridge(message: &str) -> LixError {
    LixError::new("LIX_SERVER_PROTOCOL_ERROR", message)
}

fn js_error_message(value: JsValue) -> String {
    value
        .as_string()
        .or_else(|| js_sys::Error::from(value.clone()).message().as_string())
        .unwrap_or_else(|| format!("{value:?}"))
}
