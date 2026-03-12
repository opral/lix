use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    observe_owned, BootKeyValue, CreateCheckpointResult, CreateVersionOptions, CreateVersionResult,
    Engine, EngineConfig, ExecuteOptions, ExecuteResult, LixBackend, LixError, ObserveEventsOwned,
    ObserveQuery, SnapshotChunkWriter, Value, WasmRuntime,
};

pub struct LixConfig {
    pub backend: Box<dyn LixBackend + Send + Sync>,
    pub wasm_runtime: Arc<dyn WasmRuntime>,
    pub key_values: Vec<BootKeyValue>,
}

impl LixConfig {
    pub fn new(
        backend: Box<dyn LixBackend + Send + Sync>,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Self {
        Self {
            backend,
            wasm_runtime,
            key_values: Vec::new(),
        }
    }

    fn into_engine_config(self) -> EngineConfig {
        EngineConfig {
            backend: self.backend,
            wasm_runtime: self.wasm_runtime,
            key_values: self.key_values,
            active_account: None,
            access_to_internal: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitResult {
    pub initialized: bool,
}

#[derive(Clone)]
pub struct Lix {
    engine: Arc<Engine>,
}

impl Lix {
    pub async fn open(config: LixConfig) -> Result<Self, LixError> {
        let engine = Engine::open(config.into_engine_config()).await?;
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    pub async fn init(config: LixConfig) -> Result<InitResult, LixError> {
        let result = Engine::open_or_init(config.into_engine_config()).await?;
        Ok(InitResult {
            initialized: result.initialized,
        })
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.engine.execute_with_options(sql, params, options).await
    }

    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEventsOwned, LixError> {
        observe_owned(Arc::clone(&self.engine), query)
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        self.engine.create_version(options).await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        self.engine.switch_version(version_id).await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointResult, LixError> {
        self.engine.create_checkpoint().await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        self.engine.install_plugin(archive_bytes).await
    }

    pub async fn export_snapshot(&self) -> Result<Vec<u8>, LixError> {
        let mut writer = VecSnapshotWriter::default();
        self.engine.export_snapshot(&mut writer).await?;
        Ok(writer.bytes)
    }
}

#[derive(Default)]
struct VecSnapshotWriter {
    bytes: Vec<u8>,
}

#[async_trait(?Send)]
impl SnapshotChunkWriter for VecSnapshotWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}
