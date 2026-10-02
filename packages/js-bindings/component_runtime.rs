//! Shared SDK adapter: both bindings delegate component execution to the same JS host.
use async_trait::async_trait;
use lix::plugin::runtime::{
    PluginCapabilities, WasmRuntime,
    component_host::{
        self, ComponentCompiler, ComponentGuest, ComponentGuestFactory, ComponentHost,
    },
};
use lix::{LixError, wasm::WasmLimits};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[cfg(not(target_family = "wasm"))]
pub(crate) use crate::component_runtime_napi as platform;
#[cfg(target_family = "wasm")]
pub(crate) use crate::component_runtime_wasm as platform;

#[allow(missing_debug_implementations)]
pub struct Request {
    pub operation: String,
    pub data: String,
    pub bytes: Option<Vec<u8>>,
    pub host: Option<Arc<dyn ComponentHost>>,
}

#[async_trait]
pub trait Dispatch: Send + Sync {
    async fn request(&self, request: Request) -> Result<String, LixError>;
    fn release(&self, operation: &str, id: u64);
}

pub fn runtime(dispatch: Arc<dyn Dispatch>) -> Arc<dyn WasmRuntime> {
    component_host::runtime(Arc::new(Compiler(dispatch)))
}

fn limits_json(limits: WasmLimits) -> Value {
    json!({"maxMemoryBytes": limits.max_memory_bytes.to_string(),
        "maxFuel": limits.max_fuel.map(|v| v.to_string()),
        "timeoutMs": limits.timeout_ms.map(|v| v.to_string())})
}

pub fn error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("JavaScript component runtime: {message}"),
    )
}

fn response_id(response: &str) -> Result<u64, LixError> {
    serde_json::from_str::<Value>(response)
        .map_err(error)?
        .get("id")
        .and_then(Value::as_u64)
        .filter(|id| *id <= 9_007_199_254_740_991)
        .ok_or_else(|| error("invalid component handle"))
}

struct Compiler(Arc<dyn Dispatch>);
struct Factory {
    dispatch: Arc<dyn Dispatch>,
    id: u64,
}
struct Guest {
    dispatch: Arc<dyn Dispatch>,
    id: u64,
}

// The JS promise can outlive its awaiting Rust future. Keep ownership of its
// prospective handle until the factory/guest has been materialized in Rust.
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
struct HandleRequest {
    dispatch: Arc<dyn Dispatch>,
    id: u64,
    completed: bool,
}
impl Drop for HandleRequest {
    fn drop(&mut self) {
        self.dispatch.release(
            if self.completed {
                "finishRequest"
            } else {
                "cancelRequest"
            },
            self.id,
        );
    }
}
async fn request_handle(
    dispatch: Arc<dyn Dispatch>,
    operation: &str,
    mut data: Value,
    bytes: Option<Vec<u8>>,
) -> Result<u64, LixError> {
    let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
    if id > 9_007_199_254_740_991 {
        return Err(error("component request space exhausted"));
    }
    data["requestId"] = json!(id);
    let mut request = HandleRequest {
        dispatch: dispatch.clone(),
        id,
        completed: false,
    };
    let result = dispatch
        .request(Request {
            operation: operation.into(),
            data: data.to_string(),
            bytes,
            host: None,
        })
        .await;
    if result.is_err() {
        request.completed = true;
    }
    let result = result?;
    let handle = response_id(&result)?;
    request.completed = true;
    Ok(handle)
}

#[async_trait]
impl ComponentCompiler for Compiler {
    async fn compile(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
        capabilities: PluginCapabilities,
    ) -> Result<Arc<dyn ComponentGuestFactory>, LixError> {
        let id = request_handle(
            self.0.clone(),
            "compile",
            json!({"limits": limits_json(limits), "capabilities": capabilities}),
            Some(bytes),
        )
        .await?;
        Ok(Arc::new(Factory {
            dispatch: self.0.clone(),
            id,
        }))
    }
}

#[async_trait]
impl ComponentGuestFactory for Factory {
    async fn instantiate(&self) -> Result<Arc<dyn ComponentGuest>, LixError> {
        let id = request_handle(
            self.dispatch.clone(),
            "instantiate",
            json!({"id": self.id}),
            None,
        )
        .await?;
        Ok(Arc::new(Guest {
            dispatch: self.dispatch.clone(),
            id,
        }))
    }
}

#[async_trait]
impl ComponentGuest for Guest {
    async fn invoke(
        &self,
        operation: &str,
        input: Value,
        host: Arc<dyn ComponentHost>,
        limits: WasmLimits,
    ) -> Result<(), LixError> {
        let response = self.dispatch.request(Request {
            operation: "invoke".into(),
            data: json!({"id": self.id, "operation": operation, "input": input, "limits": limits_json(limits)}).to_string(),
            bytes: None, host: Some(host),
        }).await?;
        let result: Value = serde_json::from_str(&response).map_err(error)?;
        if let Some(rejection) = result.get("pluginError") {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                rejection.to_string(),
            ));
        }
        Ok(())
    }
}
impl Drop for Factory {
    fn drop(&mut self) {
        self.dispatch.release("disposeFactory", self.id);
    }
}
impl Drop for Guest {
    fn drop(&mut self) {
        self.dispatch.release("disposeGuest", self.id);
    }
}
