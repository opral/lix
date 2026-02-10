use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadWasmComponentRequest {
    pub key: String,
    pub bytes: Vec<u8>,
    pub world: String,
    pub limits: WasmLimits,
}

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
    async fn load_component(
        &self,
        request: LoadWasmComponentRequest,
    ) -> Result<Arc<dyn WasmInstance>, LixError>;
}

#[async_trait(?Send)]
pub trait WasmInstance: Send + Sync {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError>;
}
