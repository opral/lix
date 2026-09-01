use std::sync::Arc;

use async_trait::async_trait;
use lix::plugin::runtime::{
    PluginCapabilities, WasmByteOutputsHandle, WasmChangeCursorHandle, WasmChangePage,
    WasmComponentActor, WasmComponentFactory, WasmDocumentHandle, WasmEditCursorHandle,
    WasmEditPage, WasmFileTransition, WasmFileUpdate, WasmOpenFileInput, WasmOpenRowsInput,
    WasmRowTransition, WasmRowUpdate, WasmRuntime, WasmTransitionHandle, WasmTransitionLimits,
};
use lix::wasm::WasmLimits;
use lix::{LixError, ServerMode, ServerOptions, open_lix};

struct EmbeddingRuntime;
struct EmbeddingFactory;

#[async_trait]
impl WasmRuntime for EmbeddingRuntime {
    async fn compile_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
        _capabilities: PluginCapabilities,
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
    _: WasmOpenRowsInput,
    _: WasmFileUpdate,
    _: WasmRowUpdate,
    _: WasmDocumentHandle,
    _: WasmFileTransition,
    _: WasmRowTransition,
    _: WasmTransitionHandle,
    _: WasmChangeCursorHandle,
    _: WasmEditCursorHandle,
    _: WasmByteOutputsHandle,
    _: WasmChangePage,
    _: WasmEditPage,
) {
}

#[test]
fn connected_server_builder_public_contract_remains_available() {
    let options = ServerOptions {
        mode: ServerMode::Sync,
        url: "https://example.invalid/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc".to_owned(),
        headers: vec![("authorization".to_owned(), "Bearer test".to_owned())],
    };
    let _builder = open_lix().with_server(options);
}

#[test]
fn public_sdk_opens_writes_and_reads_without_a_tokio_runtime() {
    futures_lite::future::block_on(async {
        let lix = open_lix().await.expect("open Lix under plain block_on");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('executor', CAST('true' AS JSONB))",
            &[],
        )
        .await
        .expect("write under plain block_on");
        let result = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'executor'",
                &[],
            )
            .await
            .expect("read under plain block_on");
        assert_eq!(result.rows().len(), 1);
        lix.close().await.expect("close under plain block_on");
    });
}

#[tokio::test]
async fn custom_runtime_is_usable_through_the_public_sdk() {
    let lix = open_lix()
        .with_wasm_runtime(Arc::new(EmbeddingRuntime))
        .await
        .expect("a custom runtime should open an otherwise empty workspace");

    lix.close().await.expect("workspace should close");
}
