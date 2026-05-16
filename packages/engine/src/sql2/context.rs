use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobHash};
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::commit_store::{CommitStoreContext, CommitStoreReader};
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreContext;
use crate::json_store::JsonStoreReader;
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRowRequest, LiveStateScanRequest,
    MaterializedLiveStateRow,
};
use crate::storage::{
    KvEntryPage, KvExistsBatch, KvGetRequest, KvKeyPage, KvScanRequest, KvValueBatch, KvValuePage,
    ScopedStorageReader, StorageContext, StorageReadScope, StorageReadTransaction, StorageReader,
};
use crate::transaction::types::{TransactionWrite, TransactionWriteOutcome};
use crate::version::{VersionHead, VersionRefReader};
use crate::LixError;

pub(crate) type SqlCommitStoreQuerySource = CommitStoreQuerySource<SqlReadStore>;
pub(crate) type SqlJsonReader = JsonStoreReader<ScopedStorageReader<SqlReadStore>>;

#[derive(Clone)]
pub(crate) struct SqlReadStore {
    inner: SqlReadStoreInner,
}

#[derive(Clone)]
enum SqlReadStoreInner {
    Scoped(ScopedStorageReader<Box<dyn StorageReadTransaction + Send + Sync + 'static>>),
    Write(SqlWriteContext),
}

impl SqlReadStore {
    pub(crate) fn scoped(
        store: ScopedStorageReader<Box<dyn StorageReadTransaction + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            inner: SqlReadStoreInner::Scoped(store),
        }
    }

    fn write(write_ctx: SqlWriteContext) -> Self {
        Self {
            inner: SqlReadStoreInner::Write(write_ctx),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CommitStoreQuerySource<S> {
    pub(crate) commit_store_reader: Arc<CommitStoreReader<ScopedStorageReader<S>>>,
    pub(crate) json_reader: JsonStoreReader<ScopedStorageReader<S>>,
}

/// Read-only execution boundary for `sql2::execute_sql(...)`.
///
/// Session and transaction orchestration stay above `sql2`. They provide the
/// execution-scoped committed read context for each call.
///
/// This trait is for read SQL session construction. Write SQL should use
/// `SqlWriteExecutionContext` so transaction-scoped reads and staging stay in
/// the transaction capability instead of flowing through committed read
/// sources.
#[allow(dead_code)]
pub(crate) trait SqlExecutionContext {
    fn active_version_id(&self) -> &str;
    fn live_state(&self) -> Arc<dyn LiveStateReader>;
    fn functions(&self) -> FunctionProviderHandle;
    fn commit_store_query_source(&self) -> SqlCommitStoreQuerySource;
    fn commit_graph(&self) -> Box<dyn CommitGraphReader>;
    fn version_ref(&self) -> Arc<dyn VersionRefReader>;
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;
}

/// Write-capable SQL runtime boundary.
///
/// Providers that mutate engine state should target this shape instead of
/// reaching through session/backend escape hatches. The request and write
/// payloads stay in the existing engine forms so this boundary centralizes
/// authority without adding another translation layer.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait SqlWriteExecutionContext: Send {
    fn active_version_id(&self) -> &str;
    fn functions(&self) -> FunctionProviderHandle;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;

    fn storage_context(&self) -> Option<StorageContext> {
        None
    }

    fn commit_store_context(&self) -> Option<Arc<CommitStoreContext>> {
        None
    }

    fn supports_committed_read_surfaces(&self) -> bool {
        self.storage_context().is_some() && self.commit_store_context().is_some()
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError>;

    async fn read_get_values(&mut self, _request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "storage reads are unavailable in this SQL write context",
        ))
    }

    async fn read_exists_many(
        &mut self,
        _request: KvGetRequest,
    ) -> Result<KvExistsBatch, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "storage reads are unavailable in this SQL write context",
        ))
    }

    async fn read_scan_keys(&mut self, _request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "storage reads are unavailable in this SQL write context",
        ))
    }

    async fn read_scan_values(&mut self, _request: KvScanRequest) -> Result<KvValuePage, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "storage reads are unavailable in this SQL write context",
        ))
    }

    async fn read_scan_entries(
        &mut self,
        _request: KvScanRequest,
    ) -> Result<KvEntryPage, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "storage reads are unavailable in this SQL write context",
        ))
    }

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;

    async fn load_version_head(&mut self, version_id: &str) -> Result<Option<String>, LixError>;

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError>;
}

#[derive(Clone)]
pub(crate) struct SqlWriteContext {
    ptr: Arc<SqlWriteContextPtr>,
    gate: Arc<Mutex<()>>,
}

struct SqlWriteContextPtr(NonNull<dyn SqlWriteExecutionContext>);

// DataFusion stores providers as owned Send + Sync trait objects. This context
// is only constructed for one write execution and never outlives the borrowed
// transaction context that owns it.
unsafe impl Send for SqlWriteContextPtr {}
unsafe impl Sync for SqlWriteContextPtr {}

impl SqlWriteContext {
    pub(crate) fn new(ctx: &mut dyn SqlWriteExecutionContext) -> Self {
        let ptr = NonNull::from(ctx);
        let ptr = unsafe {
            std::mem::transmute::<
                NonNull<dyn SqlWriteExecutionContext + '_>,
                NonNull<dyn SqlWriteExecutionContext + 'static>,
            >(ptr)
        };
        Self {
            ptr: Arc::new(SqlWriteContextPtr(ptr)),
            gate: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        unsafe { self.ptr.0.as_ref().functions() }
    }

    pub(crate) fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(WriteContextBlobDataReader::new(self.clone()))
    }

    pub(crate) fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        unsafe { self.ptr.0.as_ref().list_visible_schemas() }
    }

    pub(crate) fn active_version_id(&self) -> String {
        unsafe { self.ptr.0.as_ref().active_version_id().to_string() }
    }

    pub(crate) async fn load_bytes_many(
        &self,
        hashes: &[BlobHash],
    ) -> Result<BlobBytesBatch, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .load_bytes_many(hashes)
                .await
        }
    }

    pub(crate) async fn commit_store_query_source(
        &self,
    ) -> Result<SqlCommitStoreQuerySource, LixError> {
        let commit_store =
            unsafe { self.ptr.0.as_ref().commit_store_context() }.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "transaction read-only commit-store surfaces are unavailable in this write context",
                )
            })?;
        let read_scope = StorageReadScope::new(SqlReadStore::write(self.clone()));
        Ok(CommitStoreQuerySource {
            commit_store_reader: Arc::new(commit_store.reader(read_scope.store())),
            json_reader: JsonStoreContext::new().reader(read_scope.store()),
        })
    }

    pub(crate) fn commit_graph(&self) -> Result<Box<dyn CommitGraphReader>, LixError> {
        if !self.supports_committed_read_surfaces() {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "transaction read-only history surfaces are unavailable in this write context",
            ));
        }
        Ok(Box::new(
            CommitGraphContext::new().reader(SqlReadStore::write(self.clone())),
        ))
    }

    pub(crate) fn supports_committed_read_surfaces(&self) -> bool {
        unsafe { self.ptr.0.as_ref().supports_committed_read_surfaces() }
    }

    async fn read_get_values(&self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .read_get_values(request)
                .await
        }
    }

    async fn read_exists_many(&self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .read_exists_many(request)
                .await
        }
    }

    async fn read_scan_keys(&self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .read_scan_keys(request)
                .await
        }
    }

    async fn read_scan_values(&self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .read_scan_values(request)
                .await
        }
    }

    async fn read_scan_entries(&self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .read_scan_entries(request)
                .await
        }
    }

    pub(crate) async fn scan_live_state(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .scan_live_state(request)
                .await
        }
    }

    pub(crate) async fn load_version_head(
        &self,
        version_id: &str,
    ) -> Result<Option<String>, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .load_version_head(version_id)
                .await
        }
    }

    pub(crate) async fn stage_write(
        &self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .stage_write(write)
                .await
        }
    }
}

#[async_trait]
impl StorageReader for SqlReadStore {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        match &mut self.inner {
            SqlReadStoreInner::Scoped(store) => store.get_values(request).await,
            SqlReadStoreInner::Write(write_ctx) => write_ctx.read_get_values(request).await,
        }
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        match &mut self.inner {
            SqlReadStoreInner::Scoped(store) => store.exists_many(request).await,
            SqlReadStoreInner::Write(write_ctx) => write_ctx.read_exists_many(request).await,
        }
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        match &mut self.inner {
            SqlReadStoreInner::Scoped(store) => store.scan_keys(request).await,
            SqlReadStoreInner::Write(write_ctx) => write_ctx.read_scan_keys(request).await,
        }
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        match &mut self.inner {
            SqlReadStoreInner::Scoped(store) => store.scan_values(request).await,
            SqlReadStoreInner::Write(write_ctx) => write_ctx.read_scan_values(request).await,
        }
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        match &mut self.inner {
            SqlReadStoreInner::Scoped(store) => store.scan_entries(request).await,
            SqlReadStoreInner::Write(write_ctx) => write_ctx.read_scan_entries(request).await,
        }
    }
}

pub(crate) struct WriteContextBlobDataReader {
    ctx: SqlWriteContext,
}

impl WriteContextBlobDataReader {
    pub(crate) fn new(ctx: SqlWriteContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl BlobDataReader for WriteContextBlobDataReader {
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        self.ctx.load_bytes_many(hashes).await
    }
}

#[derive(Clone)]
pub(crate) enum WriteAccess {
    ReadOnly,
    Write { ctx: SqlWriteContext },
}

impl WriteAccess {
    pub(crate) fn read_only() -> Self {
        Self::ReadOnly
    }

    pub(crate) fn write(ctx: SqlWriteContext) -> Self {
        Self::Write { ctx }
    }

    pub(crate) fn require_write(
        &self,
        action: &str,
    ) -> Result<SqlWriteContext, datafusion::error::DataFusionError> {
        match self {
            Self::Write { ctx } => Ok(ctx.clone()),
            Self::ReadOnly => Err(datafusion::error::DataFusionError::Execution(format!(
                "{action} requires a write transaction"
            ))),
        }
    }

    pub(crate) fn is_write(&self) -> bool {
        matches!(self, Self::Write { .. })
    }
}

pub(crate) struct WriteContextLiveStateReader {
    ctx: SqlWriteContext,
}

impl WriteContextLiveStateReader {
    pub(crate) fn new(ctx: SqlWriteContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl LiveStateReader for WriteContextLiveStateReader {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.ctx.scan_live_state(request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        let mut rows = self
            .ctx
            .scan_live_state(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_ids: vec![request.entity_id.clone()],
                    version_ids: vec![request.version_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    ..LiveStateFilter::default()
                },
                projection: Default::default(),
                limit: Some(1),
            })
            .await?;
        Ok(rows.pop())
    }
}

pub(crate) struct WriteContextVersionRefReader {
    ctx: SqlWriteContext,
}

impl WriteContextVersionRefReader {
    pub(crate) fn new(ctx: SqlWriteContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl VersionRefReader for WriteContextVersionRefReader {
    async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, LixError> {
        Ok(self
            .ctx
            .load_version_head(version_id)
            .await?
            .map(|commit_id| VersionHead {
                version_id: version_id.to_string(),
                commit_id,
            }))
    }

    async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "scan_heads is not available through sql2 write context",
        ))
    }
}
