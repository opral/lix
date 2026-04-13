use std::sync::Arc;

use async_trait::async_trait;

use crate::contracts::{WasmComponentInstance, WasmLimits, WasmRuntime};
use crate::LixError;

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopWasmRuntime;

#[async_trait(?Send)]
impl WasmRuntime for NoopWasmRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "wasm runtime is required to execute plugins; provide a non-noop runtime"
                .to_string(),
        })
    }
}
