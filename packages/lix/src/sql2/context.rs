use std::collections::BTreeSet;
use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::LixError;
use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobId};
use crate::branch::{BranchHead, BranchRefReader};
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexReader, FilesystemPathIndexRequest,
    UncachedFilesystemPathIndexReader,
};
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonStoreReader;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateReader, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateExactBatch,
};
use crate::plugin::PluginRuntimeHost;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::TrackedStateScanRequest;
use crate::transaction_types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch, RawWriteBatch,
    TransactionWrite, TransactionWriteMode, TransactionWriteOutcome, TypedMutationJournalBatch,
};
use crate::wasm::UnsupportedWasmRuntime;

use super::change_materialization::MaterializedChange;
use super::{PublicCatalog, SessionFileViews};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiffCommand {
    Revert,
    Apply,
    CreateCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiffCommandOutcome {
    pub(crate) rows_affected: u64,
    pub(crate) commit_id: Option<String>,
}

pub(crate) type SqlChangelogQuerySource<S> = ChangelogQuerySource<S>;
pub(crate) type SqlHistoryQuerySource<S> = HistoryQuerySource<S>;

pub(crate) struct CertifiedHistoryChange {
    pub(crate) commit_id: CommitId,
    pub(crate) change: MaterializedChange,
}

#[async_trait]
pub(crate) trait CertifiedHistoryReader: Send + Sync {
    async fn scan(
        &self,
        commit_ids: &BTreeSet<CommitId>,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<CertifiedHistoryChange>, LixError>;
}

#[derive(Clone)]
pub(crate) struct HistoryQuerySource<S> {
    pub(crate) store: S,
    pub(crate) json_reader: JsonStoreReader<S>,
    pub(crate) certified_history_reader: Option<Arc<dyn CertifiedHistoryReader>>,
    /// Active-branch head pinned by the SQL session that owns this provider.
    ///
    /// History scans use this commit when the query does not provide an
    /// explicit time-travel anchor. Keeping it beside the snapshot-scoped JSON
    /// reader prevents a later branch-head lookup from mixing snapshots.
    pub(crate) default_as_of_commit_id: String,
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
#[async_trait]
pub(crate) trait SqlExecutionContext: Sync {
    type ReadStore: StorageAdapterRead + Clone + Send + Sync + 'static;

    fn active_branch_id(&self) -> &str;
    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        super::session::new_sql_session_context()
    }
    fn datafusion_read_session(&self) -> super::planning_cache::PooledReadSession {
        super::planning_cache::PooledReadSession::standalone(self.datafusion_session())
    }
    async fn sql_planning_environment(
        &self,
    ) -> Result<
        Option<(
            Arc<super::SqlPlanningCache<crate::catalog::CatalogFingerprint>>,
            crate::catalog::CatalogFingerprint,
        )>,
        LixError,
    > {
        Ok(None)
    }
    fn active_account_id(&self) -> &str {
        crate::ANONYMOUS_ACCOUNT_ID
    }
    fn hot_state(&self) -> Arc<dyn HotStateReader>;
    /// Supplies the committed tracked-head entity snapshot capability when the
    /// read context can prove it is scoped to one immutable storage snapshot.
    /// Generic and transaction contexts intentionally retain the default
    /// materialized-row path.
    fn entity_snapshot_reader(&self) -> Option<Arc<dyn super::EntitySnapshotReader>> {
        None
    }
    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(UncachedFilesystemPathIndexReader::new(self.hot_state()))
    }
    fn functions(&self) -> FunctionProviderHandle;
    fn history_query_source(
        &self,
        default_as_of_commit_id: String,
    ) -> SqlHistoryQuerySource<Self::ReadStore>;
    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore>;
    fn commit_graph(&self) -> Box<dyn CommitGraphReader>;
    fn branch_ref(&self) -> Arc<dyn BranchRefReader>;
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    /// Loads runtime-defined SQL entity metadata when provider selection could
    /// not be satisfied entirely by compile-time system surfaces.
    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;

    /// Loads reusable public-surface metadata for this read snapshot.
    ///
    /// The default keeps lightweight test/read contexts simple. Session
    /// contexts override it with their revision-keyed catalog cache; providers
    /// themselves remain scoped to the current storage snapshot.
    async fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        Ok(Arc::new(PublicCatalog::from_visible_schemas(
            &self.load_visible_schemas().await?,
        )?))
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime))
    }

    fn session_file_views(&self) -> Option<SessionFileViews> {
        None
    }
}

/// Write-capable SQL runtime boundary.
///
/// Providers that mutate engine state should target this shape instead of
/// reaching through session/storage escape hatches. The request and write
/// payloads stay in the existing engine forms so this boundary centralizes
/// authority without adding another translation layer.
#[async_trait]
pub(crate) trait SqlWriteExecutionContext: Send {
    fn active_branch_id(&self) -> &str;
    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        super::session::new_sql_session_context()
    }
    fn active_account_id(&self) -> &str {
        crate::ANONYMOUS_ACCOUNT_ID
    }
    fn functions(&self) -> FunctionProviderHandle;
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;
    fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        Ok(Arc::new(PublicCatalog::from_visible_schemas(
            &self.list_visible_schemas()?,
        )?))
    }
    fn schema_catalog_snapshot(&self) -> Option<Arc<crate::catalog::CatalogSnapshot>> {
        None
    }
    fn plugin_host(&self) -> PluginRuntimeHost {
        PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime))
    }

    fn session_file_views(&self) -> Option<SessionFileViews> {
        None
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError>;

    async fn scan_hot_state_batch(
        &mut self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError>;

    async fn load_exact_hot_state_batch(
        &mut self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError>;

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let rows = self
            .scan_hot_state_batch(&request.hot_state_request())
            .await?;
        Ok(Arc::new(FilesystemPathIndex::from_live_batch(&rows)?))
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError>;

    async fn load_collection_generation(
        &mut self,
        _branch_id: &str,
        _scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        Ok(None)
    }

    async fn load_exact_collection_live_count(
        &mut self,
        _branch_id: &str,
        _scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<u64>, LixError> {
        Ok(None)
    }

    fn has_staged_collection_rows(
        &self,
        _branch_id: &str,
        _scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<bool, LixError> {
        Ok(false)
    }

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError>;

    async fn stage_parameter_batch_insert(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Insert,
            rows,
        })
        .await
    }

    async fn stage_certified_parameter_batch_insert(
        &mut self,
        rows: CertifiedParameterInsertBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_parameter_batch_insert(rows.into_raw()?).await
    }

    async fn stage_parameter_batch_replace(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await
    }

    async fn stage_certified_parameter_batch_replace(
        &mut self,
        rows: CertifiedParameterReplacementBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_parameter_batch_replace(rows.into_raw()?).await
    }

    async fn stage_typed_mutation_journal_replace(
        &mut self,
        rows: TypedMutationJournalBatch,
    ) -> Result<TransactionWriteOutcome, LixError>;

    async fn can_stage_typed_mutation_journal_replace(
        &mut self,
        schema_key: &str,
        live_count: u64,
        ordered_identity_digest: [u8; 32],
    ) -> Result<bool, LixError>;

    async fn execute_diff_command(
        &mut self,
        _command: DiffCommand,
        _diff_ids: Vec<String>,
    ) -> Result<DiffCommandOutcome, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "diff commands are not supported by this write context",
        ))
    }

    fn staged_commit_id(&self, _branch_id: &str) -> Result<Option<String>, LixError> {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct SqlWriteContext {
    ptr: Arc<SqlWriteContextPtr>,
    gate: Arc<Mutex<()>>,
    alias_depth: Arc<std::sync::atomic::AtomicUsize>,
    alias_label: Arc<std::sync::Mutex<Option<&'static str>>>,
    explicit_insert_columns: Option<Arc<BTreeSet<String>>>,
    write_targets: Option<Arc<super::providers::WriteTargetRegistry>>,
}

struct SqlWriteContextPtr(NonNull<dyn SqlWriteExecutionContext>);

// DataFusion stores providers as owned Send + Sync trait objects. This context
// is only constructed for one write execution and never outlives the borrowed
// transaction context that owns it.
impl Drop for SqlWriteContextPtr {
    fn drop(&mut self) {
        expsnd_log(&format!(
            "DROP addr={:x}",
            self.0.as_ptr() as *const () as usize
        ));
    }
}

unsafe impl Send for SqlWriteContextPtr {}
unsafe impl Sync for SqlWriteContextPtr {}

pub(crate) fn expsnd_log(line: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/root/claude5/expSND-liveness.log")
    {
        let _ = writeln!(file, "{line}");
    }
}

static EXPSND_MUT_ONCE: std::sync::Once = std::sync::Once::new();
static EXPSND_SHARED_ONCE: std::sync::Once = std::sync::Once::new();

pub(crate) struct MutBorrowProbe {
    depth: Arc<std::sync::atomic::AtomicUsize>,
    label: Arc<std::sync::Mutex<Option<&'static str>>>,
}

impl Drop for MutBorrowProbe {
    fn drop(&mut self) {
        self.depth
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        *self.label.lock().unwrap() = None;
    }
}

impl SqlWriteContext {
    /// Marks the region in which a reconstituted `&mut` to the transaction
    /// context is live. The returned guard spans the `.await`.
    fn enter_mut_borrow(&self, label: &'static str) -> MutBorrowProbe {
        EXPSND_MUT_ONCE.call_once(|| expsnd_log("MUT_BORROW_REACHED"));
        expsnd_log(&format!(
            "MUT_ENTER addr={:x} label={label}",
            self.ptr.0.as_ptr() as *const () as usize
        ));
        self.alias_depth
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.alias_label.lock().unwrap() = Some(label);
        MutBorrowProbe {
            depth: Arc::clone(&self.alias_depth),
            label: Arc::clone(&self.alias_label),
        }
    }

    /// Every ungated shared accessor calls this before producing a `&dyn` into
    /// the same object.
    fn probe_shared_access(&self, accessor: &'static str) {
        EXPSND_SHARED_ONCE.call_once(|| expsnd_log("SHARED_ACCESS_REACHED"));
        expsnd_log(&format!(
            "SHARED addr={:x} accessor={accessor}",
            self.ptr.0.as_ptr() as *const () as usize
        ));
        let depth = self
            .alias_depth
            .load(std::sync::atomic::Ordering::SeqCst);
        if depth > 0 {
            let live = self.alias_label.lock().unwrap().unwrap_or("<unknown>");
            panic!(
                "SQLWRITECTX_ALIAS: shared accessor `{accessor}` produced &dyn while \
                 `&mut` from `{live}` was live (depth={depth})\n{}",
                std::backtrace::Backtrace::force_capture()
            );
        }
    }
}

impl SqlWriteContext {
    pub(crate) fn new(ctx: &mut dyn SqlWriteExecutionContext) -> Self {
        let ptr = NonNull::from(ctx);
        let ptr = unsafe {
            std::mem::transmute::<
                NonNull<dyn SqlWriteExecutionContext + '_>,
                NonNull<dyn SqlWriteExecutionContext + 'static>,
            >(ptr)
        };
        expsnd_log(&format!(
            "NEW addr={:x}",
            ptr.as_ptr() as *const () as usize
        ));
        Self {
            ptr: Arc::new(SqlWriteContextPtr(ptr)),
            gate: Arc::new(Mutex::new(())),
            alias_depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            alias_label: Arc::new(std::sync::Mutex::new(None)),
            explicit_insert_columns: None,
            write_targets: Some(Arc::new(super::providers::WriteTargetRegistry::default())),
        }
    }

    pub(crate) fn with_explicit_insert_columns(
        mut self,
        columns: Option<BTreeSet<String>>,
    ) -> Self {
        self.explicit_insert_columns = columns.map(Arc::new);
        self
    }

    pub(crate) fn explicit_insert_columns(&self) -> Option<&BTreeSet<String>> {
        self.explicit_insert_columns.as_deref()
    }

    pub(crate) fn write_targets(
        &self,
    ) -> Result<Arc<super::providers::WriteTargetRegistry>, LixError> {
        self.write_targets.clone().ok_or_else(|| {
            LixError::unknown("physical SQL write target cannot own a write-target registry")
        })
    }

    pub(crate) fn into_physical_target(mut self) -> Self {
        self.write_targets = None;
        self
    }

    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        self.probe_shared_access("functions");
        unsafe { self.ptr.0.as_ref().functions() }
    }

    pub(crate) fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(WriteContextBlobDataReader::new(self.clone()))
    }

    pub(crate) fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        self.probe_shared_access("public_catalog");
        unsafe { self.ptr.0.as_ref().public_catalog() }
    }

    pub(crate) fn active_branch_id(&self) -> String {
        self.probe_shared_access("active_branch_id");
        unsafe { self.ptr.0.as_ref().active_branch_id().to_string() }
    }

    pub(crate) fn active_account_id(&self) -> String {
        self.probe_shared_access("active_account_id");
        unsafe { self.ptr.0.as_ref().active_account_id().to_string() }
    }

    pub(crate) fn plugin_host(&self) -> PluginRuntimeHost {
        self.probe_shared_access("plugin_host");
        unsafe { self.ptr.0.as_ref().plugin_host() }
    }

    pub(crate) fn session_file_views(&self) -> Option<SessionFileViews> {
        self.probe_shared_access("session_file_views");
        unsafe { self.ptr.0.as_ref().session_file_views() }
    }

    pub(crate) async fn scan_hot_state_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let _guard = self.gate.lock().await;
        let _alias_probe = self.enter_mut_borrow("scan_hot_state_batch");
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .scan_hot_state_batch(request)
                .await
        }
    }

    pub(crate) async fn load_exact_hot_state_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        let _guard = self.gate.lock().await;
        let _alias_probe = self.enter_mut_borrow("load_exact_hot_state_batch");
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .load_exact_hot_state_batch(request)
                .await
        }
    }

    pub(crate) async fn load_bytes_many(
        &self,
        hashes: &[BlobId],
    ) -> Result<BlobBytesBatch, LixError> {
        let _guard = self.gate.lock().await;
        let _alias_probe = self.enter_mut_borrow("load_bytes_many");
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
        let _alias_probe = self.enter_mut_borrow("load_branch_head");
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
        let _alias_probe = self.enter_mut_borrow("filesystem_path_index");
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
        let _alias_probe = self.enter_mut_borrow("stage_write");
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

    pub(crate) async fn execute_diff_command(
        &self,
        command: DiffCommand,
        diff_ids: Vec<String>,
    ) -> Result<DiffCommandOutcome, LixError> {
        let _guard = self.gate.lock().await;
        let _alias_probe = self.enter_mut_borrow("execute_diff_command");
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .execute_diff_command(command, diff_ids)
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
    async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
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

    pub(crate) fn into_write_context(self) -> Option<SqlWriteContext> {
        match self {
            Self::ReadOnly => None,
            Self::Write { ctx } => Some(ctx),
        }
    }
}

pub(crate) struct WriteContextHotStateReader {
    ctx: SqlWriteContext,
}

impl WriteContextHotStateReader {
    pub(crate) fn new(ctx: SqlWriteContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl HotStateReader for WriteContextHotStateReader {
    async fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.ctx.scan_hot_state_batch(request).await
    }

    async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.ctx.load_exact_hot_state_batch(request).await
    }
}

#[async_trait]
impl FilesystemPathIndexReader for WriteContextHotStateReader {
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
