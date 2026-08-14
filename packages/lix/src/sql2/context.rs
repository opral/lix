use std::collections::BTreeSet;
use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::LixError;
use crate::binary_cas::BlobId;
use crate::branch::{BranchHead, BranchRefReader};
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexReader, FilesystemPathIndexRequest,
};
use crate::functions::FunctionProviderHandle;
use crate::plugin::PluginRuntimeHost;
use crate::storage_adapter::StorageAdapterRead;
use crate::transaction::types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch, RawWriteBatch,
    TransactionWrite, TransactionWriteMode, TransactionWriteOutcome, TypedMutationJournalBatch,
};
use crate::plugin::runtime::UnsupportedWasmRuntime;

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

#[derive(Clone)]
pub(crate) struct ChangelogQuerySource<S> {
    pub(crate) forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
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

    /// The concrete authenticated committed-state owner for this SQL read.
    /// Providers receive this retained view directly; they do not acquire a
    /// replacement reader or lower through a request-shaped compatibility
    /// surface.
    fn state_view(&self) -> &crate::state::ForkTreeStateView<Self::ReadStore>;

    fn active_branch_id(&self) -> &str;
    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        super::session::new_sql_session_context()
    }
    fn datafusion_read_session(&self) -> datafusion::prelude::SessionContext {
        self.datafusion_session()
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
    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader>;
    fn functions(&self) -> FunctionProviderHandle;
    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore>;
    fn commit_graph(&self) -> Box<dyn CommitGraphReader>;
    fn branch_ref(&self) -> Arc<dyn BranchRefReader>;
    fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "authenticated ForkTree blob reader is unavailable for this SQL read context",
        ))
    }
    /// Loads runtime-defined SQL row metadata when provider selection could
    /// not be satisfied entirely by compile-time system surfaces.
    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError>;

    /// Loads reusable public-surface metadata for this read snapshot.
    ///
    /// The default keeps lightweight test/read contexts simple. Session
    /// contexts override it with their authenticated row-fingerprint catalog
    /// cache; providers themselves remain scoped to the current storage
    /// snapshot.
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
    type ReadStore: StorageAdapterRead + Clone + Send + Sync + 'static;

    /// The concrete transaction state owner for this SQL write.  The
    /// associated storage type is part of the boundary so DataFusion cannot
    /// erase the retained ForkTree read behind an untyped adapter.
    fn state_view(&self) -> &crate::state::TransactionStateView<Self::ReadStore>;

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

    /// Supplies the authenticated payload reader for pre-image SQL reads.
    ///
    /// A transaction may override this with a reader bound to its retained
    /// opening view. The default is deliberately unavailable so lightweight
    /// test/write contexts cannot silently fall back to BlobId-only reads.
    fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "authenticated ForkTree blob reader is unavailable for this SQL write context",
        ))
    }

    #[cfg(test)]
    fn record_file_exact_batch(&mut self, _request: &crate::sql2::providers::FileExactBatchPlan) {}

    #[cfg(test)]
    fn record_file_scan(&mut self) {}

    /// Resolves an unpublished inline file payload from this transaction's
    /// write buffer. The caller supplies the authenticated BlobId from the
    /// current state row; implementations must return bytes only when that
    /// identity matches. Committed payloads remain the responsibility of the
    /// retained ForkTree blob reader.
    fn load_staged_file_bytes_for_owner(
        &self,
        _branch_id: &str,
        _file_id: &str,
        _expected: BlobId,
    ) -> Result<Option<Vec<u8>>, LixError> {
        Ok(None)
    }

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let _ = request;
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "filesystem path index is unavailable through this write context",
        ))
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

    /// Stage a branch-selector publication intent.  This is deliberately
    /// separate from live-state rows: the transaction lowerer consumes the
    /// intent while building the single ForkTree publication plan.
    async fn stage_branch_ref_intent(
        &mut self,
        branch_id: &str,
        commit_id: Option<CommitId>,
        create: bool,
    ) -> Result<(), LixError> {
        let _ = (branch_id, commit_id, create);
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "branch selector publication is unavailable in this write context",
        ))
    }

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
pub(crate) struct SqlWriteContext<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    ptr: Arc<SqlWriteContextPtr<R>>,
    gate: Arc<Mutex<()>>,
    explicit_insert_columns: Option<Arc<BTreeSet<String>>>,
    write_targets: Option<Arc<super::providers::WriteTargetRegistry<R>>>,
}

struct SqlWriteContextPtr<R>(NonNull<dyn SqlWriteExecutionContext<ReadStore = R>>)
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static;

// DataFusion stores providers as owned Send + Sync trait objects. This context
// is only constructed for one write execution and never outlives the borrowed
// transaction context that owns it.
unsafe impl<R> Send for SqlWriteContextPtr<R> where
    R: StorageAdapterRead + Clone + Send + Sync + 'static
{
}
unsafe impl<R> Sync for SqlWriteContextPtr<R> where
    R: StorageAdapterRead + Clone + Send + Sync + 'static
{
}

impl<R> SqlWriteContext<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>) -> Self {
        let ptr = NonNull::from(ctx);
        let ptr = unsafe {
            std::mem::transmute::<
                NonNull<dyn SqlWriteExecutionContext<ReadStore = R> + '_>,
                NonNull<dyn SqlWriteExecutionContext<ReadStore = R> + 'static>,
            >(ptr)
        };
        Self {
            ptr: Arc::new(SqlWriteContextPtr(ptr)),
            gate: Arc::new(Mutex::new(())),
            explicit_insert_columns: None,
            write_targets: Some(Arc::new(super::providers::WriteTargetRegistry::<R>::new())),
        }
    }

    pub(crate) fn state_view(&self) -> &crate::state::TransactionStateView<R> {
        unsafe { self.ptr.0.as_ref().state_view() }
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
    ) -> Result<Arc<super::providers::WriteTargetRegistry<R>>, LixError> {
        self.write_targets.clone().ok_or_else(|| {
            LixError::unknown("physical SQL write target cannot own a write-target registry")
        })
    }

    pub(crate) fn into_physical_target(mut self) -> Self {
        self.write_targets = None;
        self
    }

    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        unsafe { self.ptr.0.as_ref().functions() }
    }

    pub(crate) fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        unsafe { self.ptr.0.as_ref().public_catalog() }
    }

    pub(crate) fn active_branch_id(&self) -> String {
        unsafe { self.ptr.0.as_ref().active_branch_id().to_string() }
    }

    pub(crate) fn visible_schema_keys(&self) -> Result<Vec<String>, LixError> {
        unsafe { self.ptr.0.as_ref().list_visible_schemas()? }
            .iter()
            .map(crate::schema::schema_key_from_definition)
            .map(|result| result.map(|key| key.schema_key))
            .collect()
    }

    pub(crate) fn active_account_id(&self) -> String {
        unsafe { self.ptr.0.as_ref().active_account_id().to_string() }
    }

    pub(crate) fn plugin_host(&self) -> PluginRuntimeHost {
        unsafe { self.ptr.0.as_ref().plugin_host() }
    }

    pub(crate) fn session_file_views(&self) -> Option<SessionFileViews> {
        unsafe { self.ptr.0.as_ref().session_file_views() }
    }

    pub(crate) fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        unsafe { self.ptr.0.as_ref().authenticated_blob_reader() }
    }

    #[cfg(test)]
    pub(crate) fn record_file_exact_batch(
        &self,
        request: &crate::sql2::providers::FileExactBatchPlan,
    ) {
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .expect("SQL write context pointer should be valid")
                .record_file_exact_batch(request);
        }
    }

    #[cfg(test)]
    pub(crate) fn record_file_scan(&self) {
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .expect("SQL write context pointer should be valid")
                .record_file_scan();
        }
    }

    pub(crate) fn load_staged_file_bytes_for_owner(
        &self,
        branch_id: &str,
        file_id: &str,
        expected: BlobId,
    ) -> Result<Option<Vec<u8>>, LixError> {
        unsafe {
            self.ptr
                .0
                .as_ref()
                .load_staged_file_bytes_for_owner(branch_id, file_id, expected)
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

    pub(crate) async fn stage_branch_ref_intent(
        &self,
        branch_id: &str,
        commit_id: Option<CommitId>,
        create: bool,
    ) -> Result<(), LixError> {
        let _guard = self.gate.lock().await;
        unsafe {
            self.ptr
                .0
                .as_ptr()
                .as_mut()
                .unwrap()
                .stage_branch_ref_intent(branch_id, commit_id, create)
                .await
        }
    }

    pub(crate) async fn execute_diff_command(
        &self,
        command: DiffCommand,
        diff_ids: Vec<String>,
    ) -> Result<DiffCommandOutcome, LixError> {
        let _guard = self.gate.lock().await;
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

#[derive(Clone)]
pub(crate) enum WriteAccess<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    ReadOnly,
    Write { ctx: SqlWriteContext<R> },
}

impl<R> WriteAccess<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    pub(crate) fn read_only() -> Self {
        Self::ReadOnly
    }

    pub(crate) fn write(ctx: SqlWriteContext<R>) -> Self {
        Self::Write { ctx }
    }

    pub(crate) fn into_write_context(self) -> Option<SqlWriteContext<R>> {
        match self {
            Self::ReadOnly => None,
            Self::Write { ctx } => Some(ctx),
        }
    }
}

#[async_trait]
impl<R> FilesystemPathIndexReader for SqlWriteContext<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        self.filesystem_path_index(request).await
    }
}

#[async_trait]
impl<R> BranchRefReader for SqlWriteContext<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Ok(SqlWriteContext::load_branch_head(self, branch_id)
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
