use crate::component_runtime::{Dispatch, Request, error};
use async_trait::async_trait;
use lix::{LixError, plugin::runtime::component_host::ComponentHost};
use napi::{
    bindgen_prelude::*,
    threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
};
use napi_derive::napi;
use std::sync::Arc;

#[napi]
#[allow(missing_debug_implementations)]
pub struct NativeComponentHost {
    inner: Arc<dyn ComponentHost>,
}
#[napi]
impl NativeComponentHost {
    #[napi]
    pub fn call(&self, method: String, arguments: String) -> Result<String> {
        let arguments =
            serde_json::from_str(&arguments).map_err(|e| Error::from_reason(e.to_string()))?;
        self.inner
            .dispatch(&method, arguments)
            .map(|v| v.to_string())
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}

#[napi(object, object_from_js = false)]
#[allow(missing_debug_implementations)]
pub struct NativeComponentRequest {
    pub operation: String,
    pub data: String,
    pub bytes: Option<Buffer>,
    pub host: Option<NativeComponentHost>,
}
impl From<Request> for NativeComponentRequest {
    fn from(request: Request) -> Self {
        Self {
            operation: request.operation,
            data: request.data,
            bytes: request.bytes.map(Buffer::from),
            host: request.host.map(|inner| NativeComponentHost { inner }),
        }
    }
}

type Callback = ThreadsafeFunction<
    NativeComponentRequest,
    Promise<String>,
    NativeComponentRequest,
    Status,
    false,
    true,
>;
pub type JsDispatch<'a> = Function<'a, NativeComponentRequest, Promise<String>>;
struct NativeDispatch(Callback);

pub fn create(dispatch: JsDispatch<'_>) -> Result<Arc<dyn Dispatch>> {
    Ok(Arc::new(NativeDispatch(
        dispatch
            .build_threadsafe_function()
            .weak::<true>()
            .build()?,
    )))
}

#[async_trait]
impl Dispatch for NativeDispatch {
    async fn request(&self, request: Request) -> std::result::Result<String, LixError> {
        self.0
            .call_async_catch(request.into())
            .await
            .map_err(error)?
            .await
            .map_err(error)
    }
    fn release(&self, operation: &str, id: u64) {
        let _ = self.0.call(
            NativeComponentRequest {
                operation: operation.into(),
                data: serde_json::json!({"id":id}).to_string(),
                bytes: None,
                host: None,
            },
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}
