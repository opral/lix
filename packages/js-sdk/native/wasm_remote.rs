//! WASM binding for the Rust Lix Server Protocol client.
//!
//! JavaScript supplies the repository URL, `fetch`, and auth headers. The
//! protocol, session, recover-once, gzip, blob splices, and observe hub live
//! in `lix::server_protocol::client`.

#![allow(missing_debug_implementations)]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::{AbortHandle, Abortable};
use js_sys::{Function, Promise, Reflect, Uint8Array};
use lix::server_protocol::client::{
    OpenRemoteOptions, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse, ProtocolHttpStream,
    ProtocolHttpStreamResponse, RemoteExecuteOptions, ServerProtocolClient,
};
use lix::{
    CreateBranchOptions as RsCreateBranchOptions, LixError, SwitchBranchOptions as RsSwitchBranchOptions,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::wasm::{
    batch_statements_from_js, execute_result_to_js, from_js, lix_error_to_js, observe_event_to_js,
    to_js, values_from_js,
};

#[wasm_bindgen]
pub struct WasmRemoteLix {
    inner: ServerProtocolClient<JsProtocolHttp>,
}

#[wasm_bindgen]
pub struct WasmRemoteTransaction {
    inner: Option<lix::server_protocol::client::RemoteTransaction<JsProtocolHttp>>,
}

#[wasm_bindgen]
pub struct WasmRemoteObserveEvents {
    inner: RefCell<Option<lix::server_protocol::client::RemoteObserveEvents>>,
    closed: Cell<bool>,
    next_abort: RefCell<Option<AbortHandle>>,
}

#[wasm_bindgen(js_name = openRemote)]
pub async fn open_remote(
    url: String,
    fetch: Function,
    headers: Option<Function>,
    initial_active_branch_id: Option<String>,
) -> Result<WasmRemoteLix, JsValue> {
    console_error_panic_hook::set_once();
    let http = JsProtocolHttp {
        base_url: url,
        fetch,
        headers,
    };
    let inner = ServerProtocolClient::open(
        http,
        OpenRemoteOptions {
            initial_active_branch_id,
        },
    )
    .await
    .map_err(lix_error_to_js)?;
    Ok(WasmRemoteLix { inner })
}

#[wasm_bindgen]
impl WasmRemoteLix {
    #[wasm_bindgen(js_name = execute)]
    pub async fn execute(
        &self,
        sql: String,
        params: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let params = values_from_js(params)?;
        let options = remote_execute_options_from_js(options)?;
        let result = self
            .inner
            .execute(&sql, &params, options)
            .await
            .map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = executeBatch)]
    pub async fn execute_batch(
        &self,
        statements: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let statements = batch_statements_from_js(statements)?;
        let options = remote_execute_options_from_js(options)?;
        let results = self
            .inner
            .execute_batch(&statements, options)
            .await
            .map_err(lix_error_to_js)?;
        let encoded = results
            .into_iter()
            .map(execute_result_to_js)
            .collect::<Result<Vec<_>, _>>()?;
        to_js_array(encoded)
    }

    #[wasm_bindgen(js_name = observe)]
    pub async fn observe(
        &self,
        sql: String,
        params: JsValue,
    ) -> Result<WasmRemoteObserveEvents, JsValue> {
        let params = values_from_js(params)?;
        let inner = self
            .inner
            .observe(&sql, &params)
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmRemoteObserveEvents {
            inner: RefCell::new(Some(inner)),
            closed: Cell::new(false),
            next_abort: RefCell::new(None),
        })
    }

    #[wasm_bindgen(js_name = beginTransaction)]
    pub async fn begin_transaction(&self) -> Result<WasmRemoteTransaction, JsValue> {
        let inner = self
            .inner
            .begin_transaction()
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmRemoteTransaction { inner: Some(inner) })
    }

    #[wasm_bindgen(js_name = activeBranchId)]
    pub async fn active_branch_id(&self) -> Result<String, JsValue> {
        self.inner.active_branch_id().await.map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = activeAccountId)]
    pub async fn active_account_id(&self) -> Result<String, JsValue> {
        self.inner.active_account_id().await.map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = createBranch)]
    pub async fn create_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: CreateBranchOptionsDto = from_js(options)?;
        let receipt = self
            .inner
            .create_branch(RsCreateBranchOptions {
                id: options.id,
                name: options.name,
                from_commit_id: options.from_commit_id,
            })
            .await
            .map_err(lix_error_to_js)?;
        to_js(&CreateBranchReceiptDto {
            id: receipt.id,
            name: receipt.name,
            hidden: receipt.hidden,
            commit_id: receipt.commit_id,
        })
    }

    #[wasm_bindgen(js_name = createCheckpoint)]
    pub async fn create_checkpoint(&self) -> Result<JsValue, JsValue> {
        let receipt = self
            .inner
            .create_checkpoint()
            .await
            .map_err(lix_error_to_js)?;
        to_js(&CreateCheckpointReceiptDto {
            commit_id: receipt.commit_id,
        })
    }

    #[wasm_bindgen(js_name = undo)]
    pub async fn undo(&self) -> Result<JsValue, JsValue> {
        let receipt = self.inner.undo().await.map_err(lix_error_to_js)?;
        to_js(&UndoReceiptDto {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            inverse_commit_id: receipt.inverse_commit_id,
        })
    }

    #[wasm_bindgen(js_name = redo)]
    pub async fn redo(&self) -> Result<JsValue, JsValue> {
        let receipt = self.inner.redo().await.map_err(lix_error_to_js)?;
        to_js(&RedoReceiptDto {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            replay_commit_id: receipt.replay_commit_id,
        })
    }

    #[wasm_bindgen(js_name = switchBranch)]
    pub async fn switch_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: SwitchBranchOptionsDto = from_js(options)?;
        let receipt = self
            .inner
            .switch_branch(RsSwitchBranchOptions {
                branch_id: options.branch_id,
            })
            .await
            .map_err(lix_error_to_js)?;
        to_js(&SwitchBranchReceiptDto {
            branch_id: receipt.branch_id,
        })
    }

    #[wasm_bindgen(js_name = importFilesystemPaths)]
    pub async fn import_filesystem_paths(&self, _paths: JsValue) -> Result<(), JsValue> {
        Err(lix_error_to_js(
            ServerProtocolClient::<JsProtocolHttp>::unsupported_local_operation(
                "importFilesystemPaths",
            ),
        ))
    }

    #[wasm_bindgen(js_name = mergeBranchPreview)]
    pub async fn merge_branch_preview(&self, _options: JsValue) -> Result<JsValue, JsValue> {
        Err(lix_error_to_js(
            ServerProtocolClient::<JsProtocolHttp>::unsupported_local_operation("mergeBranchPreview"),
        ))
    }

    #[wasm_bindgen(js_name = mergeBranch)]
    pub async fn merge_branch(&self, _options: JsValue) -> Result<JsValue, JsValue> {
        Err(lix_error_to_js(
            ServerProtocolClient::<JsProtocolHttp>::unsupported_local_operation("mergeBranch"),
        ))
    }

    #[wasm_bindgen(js_name = syncDiskToLix)]
    pub async fn sync_disk_to_lix(&self) -> Result<(), JsValue> {
        Err(lix_error_to_js(
            ServerProtocolClient::<JsProtocolHttp>::unsupported_local_operation("syncDiskToLix"),
        ))
    }

    #[wasm_bindgen(js_name = close)]
    pub async fn close(&self) -> Result<(), JsValue> {
        self.inner.close().await.map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmRemoteTransaction {
    #[wasm_bindgen(js_name = execute)]
    pub async fn execute(
        &self,
        sql: String,
        params: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let params = values_from_js(params)?;
        let options = remote_execute_options_from_js(options)?;
        let inner = self.inner.as_ref().ok_or_else(transaction_closed_error)?;
        let result = inner
            .execute(&sql, &params, options)
            .await
            .map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = commit)]
    pub async fn commit(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        inner.commit().await.map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = rollback)]
    pub async fn rollback(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        inner.rollback().await.map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmRemoteObserveEvents {
    #[wasm_bindgen(js_name = next)]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.closed.get() {
            return Ok(JsValue::UNDEFINED);
        }
        let inner = self
            .inner
            .borrow_mut()
            .take()
            .ok_or_else(observe_next_in_flight_error)?;
        let (abort, registration) = AbortHandle::new_pair();
        self.next_abort.borrow_mut().replace(abort);
        let result = Abortable::new(inner.next(), registration).await;
        self.next_abort.borrow_mut().take();
        let result = match result {
            Ok(result) if !self.closed.get() => result,
            Ok(_) | Err(_) => {
                inner.close();
                Ok(None)
            }
        };
        self.inner.borrow_mut().replace(inner);
        let Some(event) = result.map_err(lix_error_to_js)? else {
            return Ok(JsValue::UNDEFINED);
        };
        observe_event_to_js(event)
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&self) {
        self.closed.set(true);
        if let Some(abort) = self.next_abort.borrow_mut().take() {
            abort.abort();
        } else if let Some(inner) = self.inner.borrow_mut().as_mut() {
            inner.close();
        }
    }
}

struct JsProtocolHttp {
    base_url: String,
    fetch: Function,
    headers: Option<Function>,
}

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but ProtocolHttp requires Send"
)]
unsafe impl Send for JsProtocolHttp {}
unsafe impl Sync for JsProtocolHttp {}

#[async_trait]
impl ProtocolHttp for JsProtocolHttp {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError> {
        let response = self.fetch_response(&request, false).await?;
        let status = js_status(&response)?;
        let headers = js_header_pairs(&response)?;
        let body = js_response_bytes(&response).await?;
        Ok(ProtocolHttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStreamResponse, LixError> {
        let response = match self.fetch_response(&request, true).await {
            Ok(response) => response,
            Err(error)
                if error.code == "LIX_REMOTE_CONFIGURATION_ERROR"
                    || error.code == "LIX_REMOTE_UNAVAILABLE" =>
            {
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let status = js_status(&response)?;
        let headers = js_header_pairs(&response)?;
        let body = Reflect::get(&response, &JsValue::from_str("body")).map_err(js_to_lix)?;
        if body.is_null() || body.is_undefined() {
            return Err(LixError::new(
                "LIX_SERVER_PROTOCOL_ERROR",
                "remote observe response has no body",
            ));
        }
        let get_reader = Reflect::get(&body, &JsValue::from_str("getReader")).map_err(js_to_lix)?;
        let reader = Function::from(get_reader)
            .call0(&body)
            .map_err(js_to_lix)?;
        Ok(ProtocolHttpStreamResponse {
            status,
            headers,
            body: Box::new(JsByteStream { reader }),
        })
    }

    async fn sleep(&self, duration: Duration) {
        let millis = duration.as_millis().min(u128::from(u32::MAX)) as f64;
        let promise = Promise::new(&mut |resolve, _reject| {
            let global = js_sys::global();
            if let Ok(set_timeout) = Reflect::get(&global, &JsValue::from_str("setTimeout")) {
                let _ = Function::from(set_timeout).call2(
                    &global,
                    &resolve,
                    &JsValue::from_f64(millis),
                );
            }
        });
        let _ = SendJsFuture(JsFuture::from(promise)).await;
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        wasm_bindgen_futures::spawn_local(async move {
            future.await;
        });
    }
}

impl JsProtocolHttp {
    async fn fetch_response(
        &self,
        request: &ProtocolHttpRequest,
        observe: bool,
    ) -> Result<JsValue, LixError> {
        let url = self.request_url(request)?;
        let headers = match self.merge_headers(&request.headers).await {
            Ok(headers) => headers,
            Err(error) if observe => {
                return Err(LixError::new(
                    "LIX_REMOTE_CONFIGURATION_ERROR",
                    "Remote Lix observation headers could not be resolved",
                )
                .with_details(serde_json::json!({ "cause": error.message })));
            }
            Err(error) => return Err(error),
        };
        let init = js_sys::Object::new();
        Reflect::set(
            &init,
            &JsValue::from_str("method"),
            &JsValue::from_str(request.method),
        )
        .map_err(js_to_lix)?;
        Reflect::set(&init, &JsValue::from_str("headers"), &headers).map_err(js_to_lix)?;
        if let Some(body) = &request.body {
            let bytes = Uint8Array::new_with_length(u32::try_from(body.len()).unwrap_or(u32::MAX));
            bytes.copy_from(body);
            Reflect::set(&init, &JsValue::from_str("body"), &bytes).map_err(js_to_lix)?;
        }
        let called = self
            .fetch
            .call2(&JsValue::UNDEFINED, &url, &init)
            .map_err(|cause| transport_unavailable(observe, &js_error_message(&cause)))?;
        let promise = Promise::from(called);
        SendJsFuture(JsFuture::from(promise))
            .await
            .map_err(|cause| transport_unavailable(observe, &js_error_message(&cause)))
    }

    fn request_url(&self, request: &ProtocolHttpRequest) -> Result<JsValue, LixError> {
        let relative = if request.query.is_empty() {
            request.path.clone()
        } else {
            let query = js_query_string(&request.query)?;
            if request.path.is_empty() {
                format!("?{query}")
            } else {
                format!("{}?{query}", request.path)
            }
        };
        let ctor = Function::from(
            Reflect::get(&js_sys::global(), &JsValue::from_str("URL")).map_err(js_to_lix)?,
        );
        let args = js_sys::Array::of2(&JsValue::from_str(&relative), &JsValue::from_str(&self.base_url));
        Reflect::construct(&ctor, &args).map_err(js_to_lix)
    }

    async fn merge_headers(
        &self,
        protocol_headers: &[(String, String)],
    ) -> Result<JsValue, LixError> {
        let headers_ctor = Function::from(
            Reflect::get(&js_sys::global(), &JsValue::from_str("Headers")).map_err(js_to_lix)?,
        );
        let init = if let Some(resolve) = &self.headers {
            let resolved = resolve.call0(&JsValue::UNDEFINED).map_err(js_to_lix)?;
            if resolved.is_instance_of::<Promise>() {
                SendJsFuture(JsFuture::from(Promise::from(resolved)))
                    .await
                    .map_err(js_to_lix)?
            } else {
                resolved
            }
        } else {
            JsValue::UNDEFINED
        };
        let args = if init.is_undefined() || init.is_null() {
            js_sys::Array::new()
        } else {
            js_sys::Array::of1(&init)
        };
        let headers = Reflect::construct(&headers_ctor, &args).map_err(js_to_lix)?;
        let delete = Function::from(
            Reflect::get(&headers, &JsValue::from_str("delete")).map_err(js_to_lix)?,
        );
        let _ = delete.call1(&headers, &JsValue::from_str("content-encoding"));
        let set =
            Function::from(Reflect::get(&headers, &JsValue::from_str("set")).map_err(js_to_lix)?);
        for (name, value) in protocol_headers {
            set.call2(
                &headers,
                &JsValue::from_str(name),
                &JsValue::from_str(value),
            )
            .map_err(js_to_lix)?;
        }
        Ok(headers)
    }
}

struct JsByteStream {
    reader: JsValue,
}

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but ProtocolHttpStream requires Send"
)]
unsafe impl Send for JsByteStream {}

#[async_trait]
impl ProtocolHttpStream for JsByteStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        let read = Reflect::get(&self.reader, &JsValue::from_str("read")).map_err(js_to_lix)?;
        let result = SendJsFuture(JsFuture::from(Promise::from(
            Function::from(read)
                .call0(&self.reader)
                .map_err(js_to_lix)?,
        )))
        .await
        .map_err(js_to_lix)?;
        let done = Reflect::get(&result, &JsValue::from_str("done"))
            .ok()
            .is_some_and(|value| value.is_truthy());
        if done {
            return Ok(None);
        }
        let value = Reflect::get(&result, &JsValue::from_str("value")).map_err(js_to_lix)?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        Ok(Some(Uint8Array::new(&value).to_vec()))
    }
}

struct SendJsFuture(JsFuture);

unsafe impl Send for SendJsFuture {}

impl Future for SendJsFuture {
    type Output = Result<JsValue, JsValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteExecuteOptionsDto {
    origin_key: Option<String>,
    idempotency_key: Option<String>,
}

fn remote_execute_options_from_js(
    options: Option<JsValue>,
) -> Result<RemoteExecuteOptions, JsValue> {
    match options {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let options: RemoteExecuteOptionsDto = from_js(value)?;
            Ok(RemoteExecuteOptions {
                origin_key: options.origin_key,
                idempotency_key: options.idempotency_key,
            })
        }
        _ => Ok(RemoteExecuteOptions::default()),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchOptionsDto {
    id: Option<String>,
    name: String,
    from_commit_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchReceiptDto {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateCheckpointReceiptDto {
    commit_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UndoReceiptDto {
    branch_id: String,
    target_commit_id: String,
    inverse_commit_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RedoReceiptDto {
    branch_id: String,
    target_commit_id: String,
    replay_commit_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchOptionsDto {
    branch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchReceiptDto {
    branch_id: String,
}

fn to_js_array(values: Vec<JsValue>) -> Result<JsValue, JsValue> {
    let array = js_sys::Array::new();
    for value in values {
        array.push(&value);
    }
    Ok(array.into())
}

fn js_query_string(query: &[(String, String)]) -> Result<String, LixError> {
    let ctor = Function::from(
        Reflect::get(&js_sys::global(), &JsValue::from_str("URLSearchParams")).map_err(js_to_lix)?,
    );
    let params = Reflect::construct(&ctor, &js_sys::Array::new()).map_err(js_to_lix)?;
    let append =
        Function::from(Reflect::get(&params, &JsValue::from_str("append")).map_err(js_to_lix)?);
    for (name, value) in query {
        append
            .call2(
                &params,
                &JsValue::from_str(name),
                &JsValue::from_str(value),
            )
            .map_err(js_to_lix)?;
    }
    Reflect::get(&params, &JsValue::from_str("toString"))
        .ok()
        .and_then(|value| Function::from(value).call0(&params).ok())
        .and_then(|value| value.as_string())
        .ok_or_else(|| LixError::new("LIX_SERVER_PROTOCOL_ERROR", "could not encode query string"))
}

fn js_status(response: &JsValue) -> Result<u16, LixError> {
    Reflect::get(response, &JsValue::from_str("status"))
        .ok()
        .and_then(|value| value.as_f64())
        .and_then(|value| u16::try_from(value as u64).ok())
        .ok_or_else(|| LixError::new("LIX_SERVER_PROTOCOL_ERROR", "fetch response has no status"))
}

fn js_header_pairs(response: &JsValue) -> Result<Vec<(String, String)>, LixError> {
    let headers = Reflect::get(response, &JsValue::from_str("headers")).map_err(js_to_lix)?;
    let pairs = std::rc::Rc::new(RefCell::new(Vec::new()));
    let collected = std::rc::Rc::clone(&pairs);
    let callback = Closure::<dyn FnMut(JsValue, JsValue)>::new(move |value: JsValue, name: JsValue| {
        if let (Some(name), Some(value)) = (name.as_string(), value.as_string()) {
            collected.borrow_mut().push((name, value));
        }
    });
    let for_each =
        Function::from(Reflect::get(&headers, &JsValue::from_str("forEach")).map_err(js_to_lix)?);
    for_each
        .call1(&headers, callback.as_ref().unchecked_ref())
        .map_err(js_to_lix)?;
    drop(callback);
    Ok(pairs.take())
}

async fn js_response_bytes(response: &JsValue) -> Result<Vec<u8>, LixError> {
    let array_buffer = Function::from(
        Reflect::get(response, &JsValue::from_str("arrayBuffer")).map_err(js_to_lix)?,
    )
    .call0(response)
    .map_err(js_to_lix)?;
    let buffer = SendJsFuture(JsFuture::from(Promise::from(array_buffer)))
        .await
        .map_err(js_to_lix)?;
    Ok(Uint8Array::new(&buffer).to_vec())
}

fn transport_unavailable(observe: bool, cause: &str) -> LixError {
    LixError::new(
        "LIX_REMOTE_UNAVAILABLE",
        if observe {
            "The remote Lix observation stream is unavailable"
        } else {
            "The remote Lix server is unavailable"
        },
    )
    .with_details(serde_json::json!({ "cause": cause }))
}

fn js_to_lix(error: JsValue) -> LixError {
    LixError::new(
        "LIX_SERVER_PROTOCOL_ERROR",
        format!("JavaScript transport error: {}", js_error_message(&error)),
    )
}

fn js_error_message(value: &JsValue) -> String {
    if let Some(message) = value.as_string() {
        return message;
    }
    Reflect::get(value, &JsValue::from_str("message"))
        .ok()
        .and_then(|value| value.as_string())
        .unwrap_or_else(|| format!("{value:?}"))
}

fn transaction_closed_error() -> JsValue {
    lix_error_to_js(LixError::new(
        "LIX_INVALID_TRANSACTION_STATE",
        "Lix transaction is closed",
    ))
}

fn observe_next_in_flight_error() -> JsValue {
    lix_error_to_js(
        LixError::new(
            "LIX_OBSERVE_NEXT_IN_FLIGHT",
            "ObserveEvents.next() is already in flight",
        )
        .with_hint("Await the pending next() call before calling next() again."),
    )
}
