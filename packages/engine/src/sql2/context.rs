use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::binary_cas::BlobDataReader;
use crate::engine2::changelog::ChangelogReader;
use crate::engine2::commit_graph::CommitGraphReader;
use crate::engine2::functions::FunctionProviderHandle;
use crate::engine2::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::engine2::transaction::types::{StageWrite, StageWriteOutcome};
use crate::engine2::version_ref::{VersionHead, VersionRefReader};
use crate::LixError;

/// Single execution boundary for `sql2::execute_sql(...)`.
///
/// Session and transaction orchestration stay above `sql2`. They provide the
/// execution-scoped visible live-state context for each call.
///
/// Catalog lookup/registration will likely join this boundary later, but we
/// are intentionally not carrying it yet until the new DataFusion-owned path
/// actually needs it.
#[allow(dead_code)]
pub(crate) trait SqlExecutionContext {
    fn active_version_id(&self) -> &str;
    fn live_state(&self) -> Arc<dyn LiveStateReader>;
    fn functions(&self) -> FunctionProviderHandle;
    fn changelog(&self) -> Arc<dyn ChangelogReader>;
    fn commit_graph(&self) -> Box<dyn CommitGraphReader>;
    fn version_ref(&self) -> Arc<dyn VersionRefReader>;
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;
}

/// Write-capable SQL runtime boundary.
///
/// Providers that mutate engine2 state should target this shape instead of
/// reaching through session/backend escape hatches. The request and write
/// payloads stay in the existing engine2 forms so this boundary centralizes
/// authority without adding another translation layer.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait SqlWriteExecutionContext {
    fn active_version_id(&self) -> &str;
    fn functions(&self) -> FunctionProviderHandle;
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError>;

    async fn load_version_head(&mut self, version_id: &str) -> Result<Option<String>, LixError>;

    async fn stage_write(&mut self, write: StageWrite) -> Result<StageWriteOutcome, LixError>;
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
        unsafe { self.ptr.0.as_ref().blob_reader() }
    }

    pub(crate) fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        unsafe { self.ptr.0.as_ref().list_visible_schemas() }
    }

    pub(crate) fn active_version_id(&self) -> String {
        unsafe { self.ptr.0.as_ref().active_version_id().to_string() }
    }

    pub(crate) async fn scan_live_state(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
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
        write: StageWrite,
    ) -> Result<StageWriteOutcome, LixError> {
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
        operation: &str,
    ) -> Result<SqlWriteContext, datafusion::error::DataFusionError> {
        match self {
            Self::Write { ctx } => Ok(ctx.clone()),
            Self::ReadOnly => Err(datafusion::error::DataFusionError::Execution(format!(
                "{operation} requires a write transaction"
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
    ) -> Result<Vec<LiveStateRow>, LixError> {
        self.ctx.scan_live_state(request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
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
