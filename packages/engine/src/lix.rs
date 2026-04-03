use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::runtime::image::ImageChunkWriter;
use crate::runtime::wasm::WasmRuntime;
use crate::{
    boot::EngineConfig, observe::observe_owned_session, BootKeyValue, CreateCheckpointResult,
    CreateVersionOptions, CreateVersionResult, Engine, ExecuteOptions, ExecuteResult, LixBackend,
    LixError, MergeVersionOptions, MergeVersionResult, ObserveEventsOwned, ObserveQuery,
    OpenSessionOptions, RedoOptions, RedoResult, Session, UndoOptions, UndoResult, Value,
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
    session: Arc<Session>,
}

impl Lix {
    pub async fn open(config: LixConfig) -> Result<Self, LixError> {
        let engine = Arc::new(Engine::open(config.into_engine_config()).await?);
        let session = engine.open_session().await?;
        Ok(Self {
            session: Arc::new(session),
        })
    }

    pub async fn init(config: LixConfig) -> Result<InitResult, LixError> {
        let initialized = Engine::open_or_init(config.into_engine_config()).await?;
        Ok(InitResult { initialized })
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.execute(sql, params).await
    }

    pub async fn active_version_id(&self) -> Result<String, LixError> {
        Ok(self.session.active_version_id())
    }

    pub async fn active_account_ids(&self) -> Result<Vec<String>, LixError> {
        Ok(self.session.active_account_ids())
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        self.session
            .execute_with_options(sql, params, options)
            .await
    }

    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEventsOwned, LixError> {
        observe_owned_session(Arc::clone(&self.session), query)
    }

    /// Opens a child session with optional workspace-selector overrides.
    ///
    /// This changes workspace selection for the child session only; it does
    /// not mutate replica-local version heads or committed history.
    pub async fn open_child_session(&self, options: OpenSessionOptions) -> Result<Self, LixError> {
        let session = self.session.open_child_session(options).await?;
        Ok(Self {
            session: Arc::new(session),
        })
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        self.session.create_version(options).await
    }

    /// Updates the active workspace version selector without moving committed
    /// version heads.
    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        self.session.switch_version(version_id).await
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionResult, LixError> {
        self.session.merge_version(options).await
    }

    /// Creates a canonical checkpoint label for the current workspace-selected
    /// version. Replay progress remains separate replica-local state.
    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointResult, LixError> {
        self.session.create_checkpoint().await
    }

    pub async fn undo(&self) -> Result<UndoResult, LixError> {
        self.session.undo().await
    }

    pub async fn undo_with_options(&self, options: UndoOptions) -> Result<UndoResult, LixError> {
        self.session.undo_with_options(options).await
    }

    pub async fn redo(&self) -> Result<RedoResult, LixError> {
        self.session.redo().await
    }

    pub async fn redo_with_options(&self, options: RedoOptions) -> Result<RedoResult, LixError> {
        self.session.redo_with_options(options).await
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        self.session.install_plugin(archive_bytes).await
    }

    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        self.session.register_schema(schema).await
    }

    pub async fn export_image(&self) -> Result<Vec<u8>, LixError> {
        let mut writer = VecImageWriter::default();
        self.session.export_image(&mut writer).await?;
        Ok(writer.bytes)
    }
}

#[derive(Default)]
struct VecImageWriter {
    bytes: Vec<u8>,
}

#[async_trait(?Send)]
impl ImageChunkWriter for VecImageWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}
