use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;

mod component_v2;

pub use component_v2::*;

/// Public path for Component v2 runtime implementations.
///
/// Engine code should import these facade types directly from `crate::wasm`.
pub mod v2 {
    pub use super::component_v2::*;
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
            max_memory_bytes: 64 * 1024 * 1024,
            max_fuel: None,
            timeout_ms: None,
        }
    }
}

/// Runtime contract for the incremental Component v2 protocol.
#[async_trait]
pub trait WasmRuntime: Send + Sync {
    /// Compiles a Component once so immutable machine code can be shared by
    /// many file actors. Each actor must subsequently call
    /// [`WasmComponentV2Factory::instantiate_actor`] to obtain an isolated
    /// Store/instance; document handles never cross actor boundaries.
    async fn compile_component_v2(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedWasmRuntime;

#[async_trait]
impl WasmRuntime for UnsupportedWasmRuntime {
    async fn compile_component_v2(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin execution requires a configured WASM component v2 runtime",
        ))
    }
}
