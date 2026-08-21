use std::sync::Arc;

use async_trait::async_trait;

use crate::{LixError, wasm::WasmLimits};

use super::{PluginCapabilities, contract::WasmComponentFactory};

/// Host-owned immutable arena primitives for Component plugins.
///
/// These values are independent of a Wasm Store and remain valid across branch
/// switches, actor eviction, and cold reopen.
pub mod v1 {
    pub use crate::plugin::runtime::arena::{
        Acceptance, Archive, ByteArena, ByteEdit, Digest, Error, FormatLayout, MapArena, Metrics,
        PerformanceMeasurement, REQUIRED_BASELINE_MEMORY_REDUCTION, REQUIRED_BASELINE_SPEEDUP,
        Root, StatePageLayout, Store, Transaction, compare_to_baseline,
    };
}

/// Runtime contract for the Lix plugin Component protocol.
#[async_trait]
pub trait WasmRuntime: Send + Sync {
    /// Compiles a plugin Component once so immutable machine code can be shared
    /// by many file actors.
    async fn compile_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
        capabilities: PluginCapabilities,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedWasmRuntime;

#[async_trait]
impl WasmRuntime for UnsupportedWasmRuntime {
    async fn compile_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
        _capabilities: PluginCapabilities,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin execution requires a configured WASM component runtime",
        ))
    }
}
