use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;

mod component_v2;

pub use component_v2::*;

/// Backward-compatible public path for Component v2 runtime implementations.
///
/// Engine code should import these facade types directly from `crate::wasm`.
pub mod v2 {
    pub use super::component_v2::*;
}

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
    /// Approximate wall-clock deadline for guest execution. Runtime
    /// implementations must renew the deadline before every exported guest
    /// invocation so a warm component receives a fresh budget on each call.
    pub timeout_ms: Option<u64>,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            // Recursive plugins retain semantic indexes alongside accepted
            // source bytes. The sandbox remains bounded, but 64 MiB is below
            // the measured working set of the 10 MiB / 220k-node JSON case.
            max_memory_bytes: 256 * 1024 * 1024,
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

    /// Compiles a v2 Component once so its immutable machine code can be
    /// shared by many file actors. Each actor must subsequently call
    /// [`WasmComponentV2Factory::instantiate_actor`] to obtain an isolated
    /// Store/instance; v2 document handles must never be shared across actors.
    async fn compile_component_v2(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
        Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "the configured WASM runtime does not support wasm-component-v2",
        ))
    }
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
