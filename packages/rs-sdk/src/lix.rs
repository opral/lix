use lix_engine::{
    CreateVersionOptions, CreateVersionReceipt as CreateVersionResult, Engine, ExecuteResult,
    LixError, MergeVersionOptions, MergeVersionReceipt as MergeVersionResult, SessionContext,
    SwitchVersionOptions, SwitchVersionReceipt as SwitchVersionResult, Value,
};

use crate::in_memory_backend::InMemoryBackend;

/// Options for opening a Lix workspace session.
#[derive(Debug, Clone, Default)]
pub struct OpenLixOptions;

/// Workspace-session handle for a Lix repository.
pub struct Lix {
    _engine: Engine,
    session: SessionContext,
}

/// Opens a new in-memory Lix workspace session.
pub async fn open_lix(_options: OpenLixOptions) -> Result<Lix, LixError> {
    let backend = InMemoryBackend::new();
    Engine::initialize(Box::new(backend.clone())).await?;
    let engine = Engine::new(Box::new(backend)).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        session,
    })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.execute(sql, params).await
    }

    pub async fn active_version_id(&self) -> Result<String, LixError> {
        self.session.active_version_id().await
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        self.session.create_version(options).await
    }

    pub async fn switch_version(
        &self,
        options: SwitchVersionOptions,
    ) -> Result<SwitchVersionResult, LixError> {
        let (_session, receipt) = self.session.switch_version(options).await?;
        Ok(receipt)
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionResult, LixError> {
        self.session.merge_version(options).await
    }

    pub async fn close(self) -> Result<(), LixError> {
        Ok(())
    }
}
