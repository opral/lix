use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::LixError;
use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobHash};
use crate::branch::{BranchHead, BranchRefReader};
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexReader, FilesystemPathIndexRequest,
    UncachedFilesystemPathIndexReader,
};
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreReader;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::plugin::PluginRuntimeHost;
use crate::storage::StorageRead;
use crate::transaction::types::{TransactionWrite, TransactionWriteOutcome};
use crate::wasm::UnsupportedWasmRuntime;

pub(crate) type SqlChangelogQuerySource<S> = ChangelogQuerySource<S>;
pub(crate) type SqlHistoryQuerySource<S> = HistoryQuerySource<S>;
pub(crate) type SqlJsonReader<S> = JsonStoreReader<S>;

#[derive(Clone)]
pub(crate) struct HistoryQuerySource<S> {
    pub(crate) json_reader: JsonStoreReader<S>,
}

#[derive(Clone)]
pub(crate) struct ChangelogQuerySource<S> {
    pub(crate) store: S,
    pub(crate) json_reader: JsonStoreReader<S>,
}

/// Read-only context used while executing one SQL statement.
///
/// Session and transaction orchestration stay above `sql2`. They provide the
/// execution-scoped committed read context for each call.
///
/// This trait is for read SQL session construction. Write SQL should use
/// `SqlWriteExecutionContext` so transaction-scoped reads and staging stay in
/// the transaction capability instead of flowing through committed read
/// sources.
pub(crate) trait SqlExecutionContext {
    type ReadStore: StorageRead + Clone + Send + Sync + 'static;

    fn active_branch_id(&self) -> &str;
    fn live_state(&self) -> Arc<dyn LiveStateReader>;
    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(UncachedFilesystemPathIndexReader::new(self.live_state()))
    }
    fn functions(&self) -> FunctionProviderHandle;
    fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore>;
    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore>;
    fn commit_graph(&self) -> Box<dyn CommitGraphReader>;
    fn branch_ref(&self) -> Arc<dyn BranchRefReader>;
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;

    fn plugin_host(&self) -> PluginRuntimeHost {
        PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime))
    }
}

/// Write-capable SQL runtime boundary.
///
/// Providers that mutate engine state should target this shape instead of
/// reaching through session/backend escape hatches. The request and write
/// payloads stay in the existing engine forms so this boundary centralizes
/// authority without adding another translation layer.
#[async_trait]
pub(crate) trait SqlWriteExecutionContext: Send {
    fn active_branch_id(&self) -> &str;
    fn functions(&self) -> FunctionProviderHandle;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;
    fn plugin_host(&self) -> PluginRuntimeHost {
        PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime))
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError>;

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let rows = self.scan_live_state(&request.live_state_request()).await?;
        Ok(Arc::new(FilesystemPathIndex::from_live_rows(rows)?))
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError>;

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

    pub(crate) fn active_branch_id(&self) -> String {
        unsafe { self.ptr.0.as_ref().active_branch_id().to_string() }
    }

    pub(crate) fn plugin_host(&self) -> PluginRuntimeHost {
        unsafe { self.ptr.0.as_ref().plugin_host() }
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

    pub(crate) async fn load_branch_head(
        &self,
        branch_id: &str,
    ) -> Result<Option<CommitId>, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .load_branch_head(branch_id)
                .await
        }
    }

    pub(crate) async fn filesystem_path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .filesystem_path_index(request)
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
                    entity_pks: vec![request.entity_pk.clone()],
                    branch_ids: vec![request.branch_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    ..LiveStateFilter::default()
                },
                projection: LiveStateProjection::default(),
                limit: Some(1),
            })
            .await?;
        Ok(rows.pop())
    }
}

#[async_trait]
impl FilesystemPathIndexReader for WriteContextLiveStateReader {
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        self.ctx.filesystem_path_index(request).await
    }
}

pub(crate) struct WriteContextBranchRefReader {
    ctx: SqlWriteContext,
}

impl WriteContextBranchRefReader {
    pub(crate) fn new(ctx: SqlWriteContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl BranchRefReader for WriteContextBranchRefReader {
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Ok(self
            .ctx
            .load_branch_head(branch_id)
            .await?
            .map(|commit_id| BranchHead {
                branch_id: branch_id.to_string(),
                commit_id,
            }))
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "scan_heads is not available through sql2 write context",
        ))
    }
}
