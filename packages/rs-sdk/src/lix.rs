use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendReadTransaction, BackendWriteTransaction, CreateVersionOptions,
    CreateVersionReceipt as CreateVersionResult, Engine, ExecuteResult, LixError,
    MergeVersionOptions, MergeVersionPreview, MergeVersionPreviewOptions,
    MergeVersionReceipt as MergeVersionResult, SessionContext, SwitchVersionOptions,
    SwitchVersionReceipt as SwitchVersionResult, Value,
};

use crate::in_memory_backend::InMemoryBackend;

/// Options for opening a Lix workspace session.
#[derive(Default)]
pub struct OpenLixOptions {
    pub backend: Option<Box<dyn Backend + Send + Sync>>,
}

/// Workspace-session handle for a Lix repository.
pub struct Lix {
    _engine: Engine,
    session: SessionContext,
    backend: SharedBackend,
    backend_closed: AtomicBool,
}

/// Opens a Lix workspace session.
///
/// If `options.backend` is omitted, a fresh in-memory backend is used. If a
/// backend is supplied, it is opened when already initialized and initialized
/// first when empty.
pub async fn open_lix(options: OpenLixOptions) -> Result<Lix, LixError> {
    let backend: Box<dyn Backend + Send + Sync> = options
        .backend
        .unwrap_or_else(|| Box::new(InMemoryBackend::new()));
    let backend = SharedBackend::new(backend);
    let engine = open_or_initialize_engine(&backend).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        session,
        backend,
        backend_closed: AtomicBool::new(false),
    })
}

impl Lix {
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    /// While a transaction is active, call `execute()` on the transaction
    /// handle instead.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.execute(sql, params).await
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction, LixError> {
        Ok(LixTransaction {
            inner: self.session.begin_transaction().await?,
        })
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

    pub async fn merge_version_preview(
        &self,
        options: MergeVersionPreviewOptions,
    ) -> Result<MergeVersionPreview, LixError> {
        self.session.merge_version_preview(options).await
    }

    pub async fn close(&self) -> Result<(), LixError> {
        self.session.close().await?;
        if !self.backend_closed.swap(true, Ordering::SeqCst) {
            self.backend.close().await?;
        }
        Ok(())
    }
}

pub struct LixTransaction {
    inner: lix_engine::SessionTransaction,
}

impl LixTransaction {
    /// Executes one SQL statement inside this transaction.
    ///
    /// Writes are staged until `commit()`. Reads use the transaction overlay,
    /// so they can observe writes staged by earlier calls on this handle.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.inner.execute(sql, params).await
    }

    pub async fn commit(self) -> Result<(), LixError> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

async fn open_or_initialize_engine(backend: &SharedBackend) -> Result<Engine, LixError> {
    match Engine::new(Box::new(backend.clone())).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(Box::new(backend.clone())).await?;
            Engine::new(Box::new(backend.clone())).await
        }
        Err(error) => Err(error),
    }
}

#[derive(Clone)]
struct SharedBackend {
    inner: Arc<dyn Backend + Send + Sync>,
}

impl SharedBackend {
    fn new(backend: Box<dyn Backend + Send + Sync>) -> Self {
        Self {
            inner: Arc::from(backend),
        }
    }
}

#[async_trait]
impl Backend for SharedBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.inner.begin_read_transaction().await
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        self.inner.begin_write_transaction().await
    }

    async fn destroy(&self) -> Result<(), LixError> {
        self.inner.destroy().await
    }

    async fn close(&self) -> Result<(), LixError> {
        self.inner.close().await
    }
}
