#[cfg(target_feature = "atomics")]
compile_error!("The JavaScript component bridge requires single-threaded WebAssembly");
use crate::component_runtime::{Dispatch, Request, error};
use async_trait::async_trait;
use lix::{LixError, plugin::runtime::component_host::ComponentHost};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
#[allow(missing_debug_implementations)]
pub struct BrowserComponentHost {
    inner: Arc<dyn ComponentHost>,
}
#[wasm_bindgen]
impl BrowserComponentHost {
    pub fn call(&self, method: &str, arguments: &str) -> Result<String, JsValue> {
        let arguments =
            serde_json::from_str(arguments).map_err(|e| JsValue::from_str(&e.to_string()))?;
        self.inner
            .dispatch(method, arguments)
            .map(|v| v.to_string())
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

// The SDK's wasm32-unknown-unknown engine and its JS handles live exclusively in
// one worker. No shared-memory/threaded wasm target is supported by this binding.
struct BrowserDispatch(js_sys::Function);
unsafe impl Send for BrowserDispatch {}
unsafe impl Sync for BrowserDispatch {}
struct SendFuture(JsFuture);
unsafe impl Send for SendFuture {}
impl Future for SendFuture {
    type Output = Result<JsValue, JsValue>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
pub fn create(dispatch: js_sys::Function) -> Arc<dyn Dispatch> {
    Arc::new(BrowserDispatch(dispatch))
}

fn js_error(value: JsValue) -> LixError {
    error(
        value
            .as_string()
            .or_else(|| {
                js_sys::Reflect::get(&value, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
            })
            .unwrap_or_else(|| "component execution failed".into()),
    )
}

impl BrowserDispatch {
    fn start(&self, request: Request) -> Result<SendFuture, LixError> {
        let object = js_sys::Object::new();
        js_sys::Reflect::set(&object, &"operation".into(), &request.operation.into())
            .map_err(js_error)?;
        js_sys::Reflect::set(&object, &"data".into(), &request.data.into()).map_err(js_error)?;
        if let Some(bytes) = request.bytes {
            js_sys::Reflect::set(
                &object,
                &"bytes".into(),
                &js_sys::Uint8Array::from(bytes.as_slice()),
            )
            .map_err(js_error)?;
        }
        if let Some(inner) = request.host {
            js_sys::Reflect::set(
                &object,
                &"host".into(),
                &BrowserComponentHost { inner }.into(),
            )
            .map_err(js_error)?;
        }
        let promise = self
            .0
            .call1(&JsValue::UNDEFINED, &object)
            .map_err(js_error)?;
        let promise = promise.dyn_into::<js_sys::Promise>().map_err(js_error)?;
        Ok(SendFuture(JsFuture::from(promise)))
    }
}
#[async_trait]
impl Dispatch for BrowserDispatch {
    async fn request(&self, request: Request) -> Result<String, LixError> {
        self.start(request)?
            .await
            .map_err(js_error)?
            .as_string()
            .ok_or_else(|| error("invalid host response"))
    }
    fn release(&self, operation: &str, id: u64) {
        if let Ok(future) = self.start(Request {
            operation: operation.into(),
            data: serde_json::json!({"id":id}).to_string(),
            bytes: None,
            host: None,
        }) {
            wasm_bindgen_futures::spawn_local(async move {
                let _ = future.await;
            });
        }
    }
}
