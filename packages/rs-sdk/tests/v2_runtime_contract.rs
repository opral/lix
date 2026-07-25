use std::sync::Arc;

use async_trait::async_trait;
use lix_sdk::{
    LixError, Memory, OpenLixOptions, WasmByteOutputsHandle, WasmChangeCursorHandle,
    WasmChangePage, WasmComponentV2Actor, WasmComponentV2Factory, WasmDocumentHandle,
    WasmEditCursorHandle, WasmEditPage, WasmEntityTransition, WasmEntityUpdate, WasmFileTransition,
    WasmFileUpdate, WasmLimits, WasmOpenEntitiesInput, WasmOpenFileInput, WasmRuntime,
    WasmTransitionHandle, WasmTransitionLimits, open_lix,
};

struct EmbeddingRuntime;
struct EmbeddingFactory;

#[async_trait]
impl WasmRuntime for EmbeddingRuntime {
    async fn compile_component_v2(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
        Ok(Arc::new(EmbeddingFactory))
    }
}

#[async_trait]
impl WasmComponentV2Factory for EmbeddingFactory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV2Actor>, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "the compile-only embedding fixture never instantiates an actor",
        ))
    }
}

// Keep the complete actor method boundary reachable from `lix_sdk`, rather
// than forcing embedders to add a direct `lix_engine` dependency merely to
// implement a custom Component API v2 runtime.
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
async fn custom_v2_runtime_is_usable_through_the_public_sdk() {
    let lix =
        open_lix(OpenLixOptions::<Memory>::default().with_wasm_runtime(Arc::new(EmbeddingRuntime)))
            .await
            .expect("a custom v2 runtime should open an otherwise empty workspace");

    lix.close().await.expect("workspace should close");
}
