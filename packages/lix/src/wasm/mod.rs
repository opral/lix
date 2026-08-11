//! General WebAssembly compute configuration for Lix.
//!
//! Plugin-specific component contracts live in [`crate::plugin::runtime`].

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
