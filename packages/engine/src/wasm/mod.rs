use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginFile {
    pub filename: Option<String>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginEntityState {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: String,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginDetectedChange {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmLimits {
    /// Maximum bytes available to each guest linear memory. With Wasmtime's
    /// standard 64 KiB pages, non-page-aligned values permit only the complete
    /// pages that fit below this bound.
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

#[async_trait]
pub trait WasmRuntime: Send + Sync {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError>;
}

#[async_trait]
pub trait WasmComponentInstance: Send + Sync {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError>;

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError>;

    async fn close(&self) -> Result<(), LixError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedWasmRuntime;

#[async_trait]
impl WasmRuntime for UnsupportedWasmRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin execution requires a configured WASM component runtime",
        ))
    }
}
