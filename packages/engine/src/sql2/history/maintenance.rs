use crate::{LixBackend, LixError, WasmRuntime};
use std::sync::Arc;

pub(crate) async fn materialize_missing_file_history_with_plugins(
    backend: &dyn LixBackend,
    wasm_runtime: Arc<dyn WasmRuntime>,
) -> Result<(), LixError> {
    crate::plugin::runtime::materialize_missing_file_history_data_with_plugins(
        backend,
        wasm_runtime.as_ref(),
    )
    .await
}
