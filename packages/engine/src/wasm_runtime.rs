use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmLimits {
    pub max_memory_bytes: u64,
    pub max_fuel: Option<u64>,
    pub timeout_ms: Option<u64>,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,
            max_fuel: None,
            timeout_ms: None,
        }
    }
}

#[async_trait(?Send)]
pub trait WasmRuntime: Send + Sync {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError>;
}

#[async_trait(?Send)]
pub trait WasmComponentInstance: Send + Sync {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError>;

    async fn close(&self) -> Result<(), LixError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopWasmRuntime;

#[async_trait(?Send)]
impl WasmRuntime for NoopWasmRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "wasm runtime is required to execute plugins; provide a non-noop runtime"
                .to_string(),
        })
    }
}
