use lix_engine::{
    Backend, CreateVersionOptions, CreateVersionReceipt as CreateVersionResult, Engine,
    ExecuteResult, InMemoryBackend, LixError, MergeVersionOptions, MergeVersionPreview,
    MergeVersionPreviewOptions, MergeVersionReceipt as MergeVersionResult, SessionContext,
    SwitchVersionOptions, SwitchVersionReceipt as SwitchVersionResult, Value,
};

/// Options for opening a Lix workspace session.
#[derive(Default)]
pub struct OpenLixOptions<B = InMemoryBackend> {
    pub backend: Option<B>,
}

/// Workspace-session handle for a Lix repository.
pub struct Lix<B = InMemoryBackend>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    _engine: Engine<B>,
    session: SessionContext<B>,
}

/// Opens a Lix workspace session.
///
/// If `options.backend` is omitted, a fresh in-memory backend is used. If a
/// backend is supplied, it is opened when already initialized and initialized
/// first when empty.
pub async fn open_lix(options: OpenLixOptions) -> Result<Lix, LixError> {
    open_lix_with_backend(options.backend.unwrap_or_else(InMemoryBackend::new)).await
}

pub async fn open_lix_with_backend<B>(backend: B) -> Result<Lix<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    let engine = open_or_initialize_engine(backend).await?;
    let session = engine.open_workspace_session().await?;
    Ok(Lix {
        _engine: engine,
        session,
    })
}

impl<B> Lix<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    /// Executes one DataFusion SQL statement against this Lix session.
    ///
    /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
    /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog tables
    /// and transaction statements such as `sqlite_master`, `BEGIN`, and
    /// `COMMIT` are not part of this contract; use `information_schema` for
    /// catalog inspection. Lix owns transaction boundaries for each statement.
    /// While a transaction is active, call `execute()` on the transaction
    /// handle instead.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.session.execute(sql, params).await
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction<B>, LixError> {
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
        self.session.close().await
    }
}

pub struct LixTransaction<B = InMemoryBackend>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    inner: lix_engine::SessionTransaction<B>,
}

impl<B> LixTransaction<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
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

async fn open_or_initialize_engine<B>(backend: B) -> Result<Engine<B>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    match Engine::new(backend.clone()).await {
        Ok(engine) => Ok(engine),
        Err(error) if error.code == "LIX_ERROR_NOT_INITIALIZED" => {
            Engine::initialize(backend.clone()).await?;
            Engine::new(backend).await
        }
        Err(error) => Err(error),
    }
}
