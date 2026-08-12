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
    shared: Arc<SqlWriteContextShared>,
    explicit_insert_columns: Option<Arc<BTreeSet<String>>>,
    write_targets: Option<Arc<super::providers::WriteTargetRegistry>>,
}

struct SqlWriteContextPtr(NonNull<dyn SqlWriteExecutionContext>, u64);

/// EXPSND probe registry.
pub(crate) struct ExpsndRegistry {
    next_gen: u64,
    /// addr -> generation of the pointee currently alive there
    alive: std::collections::HashMap<usize, u64>,
    /// (addr, gen) -> number of live `SqlWriteContextPtr` for that pointee
    live_ptrs: std::collections::HashMap<(usize, u64), usize>,
}

pub(crate) static EXPSND: std::sync::LazyLock<std::sync::Mutex<ExpsndRegistry>> =
    std::sync::LazyLock::new(|| {
        std::sync::Mutex::new(ExpsndRegistry {
            next_gen: 0,
            alive: std::collections::HashMap::new(),
            live_ptrs: std::collections::HashMap::new(),
        })
    });

pub(crate) fn expsnd_log(line: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/root/claude5/expSND-deref.log")
    {
        let _ = writeln!(file, "{line}");
    }
}

/// Called by `Transaction::drop`. Retires the pointee and reports whether any
/// pointer to it was still alive.
pub(crate) fn expsnd_retire_pointee(addr: usize) {
    let mut registry = EXPSND.lock().unwrap();
    if let Some(generation) = registry.alive.remove(&addr) {
        let outlived = registry
            .live_ptrs
            .get(&(addr, generation))
            .copied()
            .unwrap_or(0);
        drop(registry);
        if outlived > 0 {
            expsnd_log(&format!(
                "OUTLIVED addr={addr:x} gen={generation} live_ptrs={outlived}"
            ));
        }
    }
}

impl Drop for SqlWriteContextPtr {
    fn drop(&mut self) {
        let addr = self.0.as_ptr() as *const () as usize;
        let mut registry = EXPSND.lock().unwrap();
        if let Some(count) = registry.live_ptrs.get_mut(&(addr, self.1)) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                registry.live_ptrs.remove(&(addr, self.1));
            }
        }
    }
}


/// Values captured from the write execution context at construction time.
///
/// These were previously read back through `SqlWriteContextPtr` on every call,
/// producing a `&dyn` into the transaction context with no synchronization
/// while the gated methods could concurrently hold a reconstituted `&mut` to
/// the same object. Every one of them is a cheap getter returning owned,
/// `Arc`'d, or cloned data that is stable for this context's lifetime, so
/// capturing them here removes that shared borrow outright rather than
/// serializing it. Nothing below reads through the raw pointer.
struct SqlWriteContextShared {
    functions: FunctionProviderHandle,
    /// Stored as the `Result` it was: the underlying catalog is memoized on a
    /// fingerprint that is fixed for the context's lifetime
    /// (`sql_schema_snapshot` is assigned once at construction and never
    /// reassigned), so a captured value cannot go stale.
    public_catalog: Result<Arc<PublicCatalog>, LixError>,
    active_branch_id: String,
    active_account_id: String,
    plugin_host: PluginRuntimeHost,
    /// A shared `Arc<Mutex<..>>` handle, not a snapshot: mutations made through
    /// a captured clone are observed by every other holder.
    session_file_views: Option<SessionFileViews>,
}

// DataFusion stores providers as owned Send + Sync trait objects. This context
// is only constructed for one write execution and never outlives the borrowed
// transaction context that owns it.
//
// SAFETY SCOPE: the pointer is now reached only by the gate-serialized methods
// that reconstitute `&mut`. The shared accessors read `SqlWriteContextShared`
// and never touch it, so no `&dyn` into the transaction context can be alive
// while one of those `&mut` borrows is held.
unsafe impl Send for SqlWriteContextPtr {}
unsafe impl Sync for SqlWriteContextPtr {}

impl SqlWriteContext {
    /// Panics if a gated site is about to dereference a pointee whose
    /// `Transaction` has already been destroyed.
    fn expsnd_check_alive(&self, site: &'static str) {
        let addr = self.ptr.0.as_ptr() as *const () as usize;
        let generation = self.ptr.1;
        let live = EXPSND.lock().unwrap().alive.get(&addr).copied();
        if live != Some(generation) {
            panic!(
                "EXPSND_DEREF_AFTER_DEATH: gated `{site}` dereferenced pointee {addr:x} \
                 (gen {generation}, live now {live:?}) whose Transaction is gone\n{}",
                std::backtrace::Backtrace::force_capture()
            );
        }
    }
}

impl SqlWriteContext {
    pub(crate) fn new(ctx: &mut dyn SqlWriteExecutionContext) -> Self {
        // Capture the shared surface while the `&mut` borrow is still held
        // legitimately, so no later call has to forge one.
        let shared = Arc::new(SqlWriteContextShared {
            functions: ctx.functions(),
            public_catalog: ctx.public_catalog(),
            active_branch_id: ctx.active_branch_id().to_string(),
            active_account_id: ctx.active_account_id().to_string(),
            plugin_host: ctx.plugin_host(),
            session_file_views: ctx.session_file_views(),
        });
        let ptr = NonNull::from(ctx);
        let expsnd_generation = {
            let addr = ptr.as_ptr() as *const () as usize;
            let mut registry = EXPSND.lock().unwrap();
            let generation = match registry.alive.get(&addr) {
                Some(generation) => *generation,
                None => {
                    registry.next_gen += 1;
                    let generation = registry.next_gen;
                    registry.alive.insert(addr, generation);
                    generation
                }
            };
            *registry.live_ptrs.entry((addr, generation)).or_insert(0usize) += 1;
            generation
        };
        let ptr = unsafe {
            std::mem::transmute::<
                NonNull<dyn SqlWriteExecutionContext + '_>,
                NonNull<dyn SqlWriteExecutionContext + 'static>,
            >(ptr)
        };
        Self {
            ptr: Arc::new(SqlWriteContextPtr(ptr, expsnd_generation)),
            gate: Arc::new(Mutex::new(())),
            shared,
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
        self.shared.functions.clone()
    }

    pub(crate) fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(WriteContextBlobDataReader::new(self.clone()))
    }

    pub(crate) fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        self.shared.public_catalog.clone()
    }

    pub(crate) fn active_branch_id(&self) -> String {
        self.shared.active_branch_id.clone()
    }

    pub(crate) fn active_account_id(&self) -> String {
        self.shared.active_account_id.clone()
    }

    pub(crate) fn plugin_host(&self) -> PluginRuntimeHost {
        self.shared.plugin_host.clone()
    }

    pub(crate) fn session_file_views(&self) -> Option<SessionFileViews> {
        self.shared.session_file_views.clone()
    }

    pub(crate) async fn scan_hot_state_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let _guard = self.gate.lock().await;
        self.expsnd_check_alive("scan_hot_state_batch");
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
        self.expsnd_check_alive("load_exact_hot_state_batch");
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
        self.expsnd_check_alive("load_bytes_many");
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
        self.expsnd_check_alive("load_branch_head");
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
        self.expsnd_check_alive("filesystem_path_index");
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
        self.expsnd_check_alive("stage_write");
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
        self.expsnd_check_alive("execute_diff_command");
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
