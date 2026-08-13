use std::sync::Arc;

use async_trait::async_trait;
use lix::plugin::runtime::{
    WasmByteOutputsHandle, WasmChangeCursorHandle, WasmChangePage, WasmComponentActor,
    WasmComponentFactory, WasmDocumentHandle, WasmEditCursorHandle, WasmEditPage,
    WasmEntityTransition, WasmEntityUpdate, WasmFileTransition, WasmFileUpdate,
    WasmOpenEntitiesInput, WasmOpenFileInput, WasmRuntime, WasmTransitionHandle,
    WasmTransitionLimits,
};
use lix::wasm::WasmLimits;
use lix::{LixError, open_lix};

struct EmbeddingRuntime;
struct EmbeddingFactory;

#[async_trait]
impl WasmRuntime for EmbeddingRuntime {
    async fn compile_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        Ok(Arc::new(EmbeddingFactory))
    }
}

#[async_trait]
impl WasmComponentFactory for EmbeddingFactory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentActor>, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "the compile-only embedding fixture never instantiates an actor",
        ))
    }
}

// Keep the complete actor method boundary reachable from `lix::plugin::runtime`.
#[allow(dead_code)]
fn actor_contract_types_are_public(
    _: WasmTransitionLimits,
    _: WasmOpenFileInput,
    _: WasmOpenEntitiesInput,
    _: WasmFileUpdate,
    _: WasmEntityUpdate,
    _: WasmDocumentHandle,
    _: WasmFileTransition,
    _: WasmEntityTransition,
    _: WasmTransitionHandle,
    _: WasmChangeCursorHandle,
    _: WasmEditCursorHandle,
    _: WasmByteOutputsHandle,
    _: WasmChangePage,
    _: WasmEditPage,
) {
}

#[tokio::test]
async fn custom_runtime_is_usable_through_the_public_sdk() {
    let lix = open_lix()
        .with_wasm_runtime(Arc::new(EmbeddingRuntime))
        .await
        .expect("a custom runtime should open an otherwise empty workspace");

    lix.close().await.expect("workspace should close");
}
