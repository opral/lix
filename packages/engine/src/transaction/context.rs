use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobHash};
use crate::catalog::CatalogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::domain::Domain;
use crate::entity_identity::EntityIdentity;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::overlay_scan_rows;
use crate::live_state::{
    LiveStateContext, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::session::{SessionMode, WORKSPACE_VERSION_KEY};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::storage::{
    InMemoryStorageBackend, StorageBackend, StorageReadOptions, StorageWriteOptions,
    StorageWriteSetStats,
};
use crate::storage::{StorageContext, StorageRead, StorageReadScope, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateStoreReader};
use crate::transaction::commit;
use crate::transaction::normalization::{
    normalize_transaction_write_row, remember_pending_registered_schema,
    NormalizedTransactionWriteRow, REGISTERED_SCHEMA_KEY,
};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{PreparedWriteSet, TransactionWriteBuffer};
use crate::transaction::types::{
    stage_json_from_value, PreparedStateRow, PreparedTransactionWrite, StagedCommitChangeRef,
    TransactionFileData, TransactionJson, TransactionWrite, TransactionWriteMode,
    TransactionWriteOutcome, TransactionWriteRow,
};
use crate::transaction::validation::{validate_prepared_writes, TransactionValidationInput};
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome {
    pub(crate) storage_stats: StorageWriteSetStats,
}

/// One execution-scoped transaction capability for engine write paths.
///
/// This is intentionally not a session-wide kitchen sink. It owns the backend
/// write transaction for one `SessionContext::execute(...)` call and projects
/// accepted SQL/provider writes back into the SQL DAG through an engine-local live-state
/// overlay.
///
/// Transaction invariant: this is the capability for engine operations
/// that may write. Write-relevant reads must be exposed from this transaction,
/// after the backend write transaction has begun, rather than from session-level
/// helpers.
pub(crate) struct Transaction<B: StorageBackend = InMemoryStorageBackend> {
    active_version_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    version_ctx: Arc<VersionContext>,
    schema_resolver: TransactionSchemaResolver,
    staged_writes: Arc<TransactionWriteBuffer>,
    staged_storage_writes: StorageWriteSet,
    storage: StorageContext<B>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    commit_boundary: Option<TransactionCommitBoundary>,
}

#[derive(Clone)]
pub(crate) struct TransactionCommitBoundary {
    state: CommitBoundaryState,
    pre_commit_check: Arc<dyn Fn() -> Result<(), LixError> + Send + Sync>,
}

impl TransactionCommitBoundary {
    pub(crate) fn new(
        state: CommitBoundaryState,
        pre_commit_check: Arc<dyn Fn() -> Result<(), LixError> + Send + Sync>,
    ) -> Self {
        Self {
            state,
            pre_commit_check,
        }
    }

    fn begin(&self) -> CommitBoundaryGuard {
        self.state.begin()
    }

    fn check(&self) -> Result<(), LixError> {
        (self.pre_commit_check)()
    }

    fn commit<T>(&self, commit: impl FnOnce() -> Result<T, LixError>) -> Result<T, LixError> {
        let _gate = self.state.lock_durable_commit();
        self.check()?;
        commit()
    }
}

#[derive(Clone)]
pub(crate) struct CommitBoundaryState {
    active_count: Arc<AtomicUsize>,
    durable_commit_gate: Arc<std::sync::Mutex<()>>,
    watch: tokio::sync::watch::Sender<usize>,
}

impl CommitBoundaryState {
    pub(crate) fn new() -> Self {
        let (watch, _) = tokio::sync::watch::channel(0);
        Self {
            active_count: Arc::new(AtomicUsize::new(0)),
            durable_commit_gate: Arc::new(std::sync::Mutex::new(())),
            watch,
        }
    }

    pub(crate) fn begin(&self) -> CommitBoundaryGuard {
        let previous = self.active_count.fetch_add(1, Ordering::SeqCst);
        self.watch.send_replace(previous + 1);
        CommitBoundaryGuard {
            state: self.clone(),
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active_count.load(Ordering::SeqCst)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active_count() > 0
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<usize> {
        self.watch.subscribe()
    }

    pub(crate) fn lock_durable_commit(&self) -> std::sync::MutexGuard<'_, ()> {
        self.durable_commit_gate
            .lock()
            .expect("commit boundary durable commit gate should not poison")
    }

    pub(crate) fn try_lock_durable_commit(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        match self.durable_commit_gate.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("commit boundary durable commit gate should not poison")
            }
        }
    }
}

pub(crate) struct CommitBoundaryGuard {
    state: CommitBoundaryState,
}

impl Drop for CommitBoundaryGuard {
    fn drop(&mut self) {
        let remaining = self.state.active_count.fetch_sub(1, Ordering::SeqCst) - 1;
        self.state.watch.send_replace(remaining);
    }
}

pub(crate) fn begin_commit_boundary(
    boundary: Option<&TransactionCommitBoundary>,
) -> Option<CommitBoundaryGuard> {
    let boundary = boundary?;
    Some(boundary.begin())
}

fn check_commit_boundary(boundary: Option<&TransactionCommitBoundary>) -> Result<(), LixError> {
    if let Some(boundary) = boundary {
        boundary.check()?;
    }
    Ok(())
}

pub(crate) fn commit_at_boundary<T>(
    boundary: Option<&TransactionCommitBoundary>,
    commit: impl FnOnce() -> Result<T, LixError>,
) -> Result<T, LixError> {
    match boundary {
        Some(boundary) => boundary.commit(commit),
        None => commit(),
    }
}

impl<B> Transaction<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL/provider hooks.
    async fn open(
        mode: &SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
    ) -> Result<OpenTransaction<B>, LixError> {
        let read = storage.begin_read(StorageReadOptions::default())?;
        let setup_result = async {
            let active_version_id =
                resolve_active_version_id(mode, live_state.as_ref(), version_ctx.as_ref(), &read)
                    .await?;
            let runtime_functions = {
                let runtime_live_state = live_state.reader(&read);
                FunctionContext::prepare(&runtime_live_state).await?
            };
            let functions = runtime_functions.provider();
            let visible_schemas = {
                let visible_live_state = live_state.reader(&read);
                catalog_context
                    .schema_jsons_for_sql_read_planning(&visible_live_state, &active_version_id)
                    .await?
            };
            let schema_facts = {
                let visible_live_state = live_state.reader(&read);
                catalog_context
                    .schema_facts_for_domain(
                        &visible_live_state,
                        &Domain::schema_catalog(active_version_id.clone(), true),
                    )
                    .await?
            };
            Ok::<_, LixError>((
                active_version_id,
                runtime_functions,
                functions,
                visible_schemas,
                schema_facts,
            ))
        }
        .await;
        let (active_version_id, runtime_functions, functions, visible_schemas, schema_facts) =
            match setup_result {
                Ok(result) => result,
                Err(error) => {
                    return Err(error);
                }
            };
        let mut schema_resolver = TransactionSchemaResolver::new(catalog_context);
        schema_resolver.remember_schema_facts(
            &Domain::schema_catalog(active_version_id.clone(), true),
            schema_facts,
        );
        let staged_writes = Arc::new(TransactionWriteBuffer::new(functions.clone()));
        Ok(OpenTransaction {
            transaction: Self {
                active_version_id,
                live_state,
                tracked_state,
                binary_cas,
                version_ctx,
                schema_resolver,
                staged_writes,
                staged_storage_writes: StorageWriteSet::new(),
                storage,
                visible_schemas,
                functions,
                commit_boundary: None,
            },
            runtime_functions,
        })
    }

    /// Commits prepared writes, runtime function state, and the backend transaction.
    ///
    /// Commit owns the execution boundary: prepared rows become changelog
    /// facts, version-ref updates, and visible live_state rows before the
    /// backend transaction is committed.
    #[allow(dead_code)]
    pub(crate) async fn commit(
        self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let mut transaction = self;
        let commit_boundary = transaction.commit_boundary.clone();
        let prepared_writes = match transaction.staged_writes.drain() {
            Ok(prepared_writes) => prepared_writes,
            Err(error) => {
                return Err(error);
            }
        };
        let _commit_guard = begin_commit_boundary(commit_boundary.as_ref());
        check_commit_boundary(commit_boundary.as_ref())?;
        if let Err(error) = transaction
            .validate_prepared_writes_by_version(&prepared_writes)
            .await
        {
            return Err(error);
        }
        let mut read = transaction
            .storage
            .begin_read(StorageReadOptions::default())?;
        let mut writes = match commit::commit_prepared_writes(
            &transaction.binary_cas,
            transaction.version_ctx.as_ref(),
            Some(runtime_functions),
            &mut read,
            prepared_writes,
        )
        .await
        {
            Ok(writes) => writes,
            Err(error) => return Err(error),
        };
        writes.extend(transaction.staged_storage_writes);
        let storage_stats = commit_at_boundary(commit_boundary.as_ref(), || {
            let (_commit, stats) = transaction
                .storage
                .commit_write_set(writes, StorageWriteOptions::default())?;
            Ok(stats)
        })?;
        Ok(TransactionCommitOutcome { storage_stats })
    }

    pub(crate) fn attach_commit_boundary(&mut self, boundary: TransactionCommitBoundary) {
        self.commit_boundary = Some(boundary);
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        Ok(())
    }

    /// Stages one decoded write batch into this transaction.
    ///
    /// This is the programmatic write entrypoint used by non-SQL APIs. The
    /// transaction still owns preparation from `TransactionWriteRow` into
    /// `PreparedStateRow`, so generated timestamps, change ids, commit ids, and
    /// commit change refs stay in one place.
    #[allow(dead_code)]
    pub(crate) async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        require_valid_transaction_write_storage_scopes(&write)?;
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(transaction_write_row_count(
                &write,
            ));
            crate::storage_bench::record_transaction_untracked_rows(
                transaction_write_untracked_row_count(&write),
            );
        }
        self.require_existing_transaction_write_version_ids(&write)
            .await?;
        let write = self.prepare_transaction_write(write).await?;
        self.staged_writes.stage_write(write)
    }

    async fn prepare_transaction_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<PreparedTransactionWrite, LixError> {
        Ok(match write {
            TransactionWrite::Rows { mode, rows } => PreparedTransactionWrite::Rows {
                mode,
                rows: self.prepare_transaction_rows(rows).await?,
            },
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
            } => PreparedTransactionWrite::RowsWithFileData {
                mode,
                rows: self.prepare_transaction_rows(rows).await?,
                file_data,
                count,
            },
        })
    }

    async fn prepare_transaction_rows(
        &mut self,
        rows: Vec<TransactionWriteRow>,
    ) -> Result<Vec<PreparedStateRow>, LixError> {
        let row_count = rows.len();
        let staged = self.staged_writes.staging_overlay()?;
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let live_state = self.live_state.reader(&read);
        let mut rows_by_scope = BTreeMap::<Domain, Vec<(usize, TransactionWriteRow)>>::new();
        for (index, row) in rows.into_iter().enumerate() {
            rows_by_scope
                .entry(Domain::schema_catalog(
                    row.schema_scope_version_id().to_string(),
                    row.untracked,
                ))
                .or_default()
                .push((index, row));
        }

        let mut prepared_rows = Vec::with_capacity(row_count);
        prepared_rows.resize_with(row_count, || None);
        for (domain, rows) in rows_by_scope {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&live_state, &staged, &domain)
                .await?;
            for (_, row) in &rows {
                if row.schema_key != REGISTERED_SCHEMA_KEY {
                    continue;
                }
                if row.file_id.is_some() {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        "lix_registered_schema rows must not be scoped to a file",
                    )
                    .with_hint("Schema definitions are scoped by version and durability only; write them with null file_id."));
                }
                remember_pending_registered_schema(
                    row.snapshot.as_ref().map(TransactionJson::value),
                    Domain::schema_catalog(
                        row.schema_scope_version_id().to_string(),
                        row.untracked,
                    ),
                    catalog,
                )?;
            }
            let normalized_rows = rows
                .into_iter()
                .map(|(index, row)| {
                    normalize_transaction_write_row(row, catalog, functions.clone())
                        .map(|row| (index, row))
                })
                .collect::<Result<Vec<_>, _>>()?;
            for (index, row) in normalized_rows {
                prepared_rows[index] = Some(prepare_state_row(row, &functions)?);
            }
        }
        Ok(prepared_rows
            .into_iter()
            .map(|row| {
                row.expect("every row should be prepared exactly once by schema scope grouping")
            })
            .collect())
    }

    async fn validate_prepared_writes_by_version(
        &mut self,
        prepared_writes: &PreparedWriteSet,
    ) -> Result<(), LixError> {
        let validation_index = prepared_writes.validation_index();
        for scope in validation_index.schema_scopes() {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_version();
            let version_prepared_writes = validation_index.validation_set_for_schema_scope(scope);
            let read = self.storage.begin_read(StorageReadOptions::default())?;
            let live_state = self.live_state.reader(&read);
            let schema_catalog = self
                .schema_resolver
                .catalog_for_validation(&live_state, scope)
                .await?;
            validate_prepared_writes(TransactionValidationInput::new(
                &version_prepared_writes,
                &schema_catalog,
                &live_state,
            ))
            .await?;
        }
        Ok(())
    }

    /// Convenience helper for programmatic APIs that only stage state rows.
    #[allow(dead_code)]
    pub(crate) async fn stage_rows(
        &mut self,
        rows: Vec<TransactionWriteRow>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await
    }

    async fn require_existing_transaction_write_version_ids(
        &mut self,
        write: &TransactionWrite,
    ) -> Result<(), LixError> {
        let version_ids = transaction_write_version_ids(write);
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let reader = self.version_ctx.ref_reader(&read);
        for version_id in version_ids {
            if version_id == GLOBAL_VERSION_ID {
                continue;
            }
            if reader.load_head_commit_id(&version_id).await?.is_none() {
                return Err(LixError::version_not_found(
                    version_id,
                    "stage_write",
                    "target",
                ));
            }
        }
        Ok(())
    }

    /// Returns the active version resolved inside this write transaction.
    pub(crate) fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    /// Returns this transaction's prepared runtime functions.
    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    pub(crate) fn sql_read_execution_context(
        &self,
    ) -> Result<TransactionSqlReadExecutionContext<B::Read<'_>>, LixError> {
        let read_store = self.storage.begin_read(StorageReadOptions::default())?;
        let staged = self.staged_writes.staging_overlay()?;
        Ok(TransactionSqlReadExecutionContext {
            active_version_id: self.active_version_id.clone(),
            read_store,
            live_state: Arc::clone(&self.live_state),
            binary_cas: Arc::clone(&self.binary_cas),
            version_ctx: Arc::clone(&self.version_ctx),
            visible_schemas: self.visible_schemas.clone(),
            functions: self.functions.clone(),
            staged,
        })
    }

    /// Advances a version ref without staging tracked rows.
    ///
    /// Fast-forward merges use this path because the commit graph already
    /// contains the source head; the target ref only needs to move to it.
    pub(crate) async fn advance_version_ref(
        &mut self,
        version_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let timestamp = self.functions.call_timestamp();
        let canonical_row = prepare_version_ref_row(version_id, commit_id, &timestamp)?;
        self.version_ctx
            .stage_canonical_ref_rows(&mut self.staged_storage_writes, &[canonical_row.row])
    }

    pub(crate) fn stage_merge_commit(
        &self,
        version_id: String,
        source_parent_commit_id: String,
        selected_changes: impl IntoIterator<Item = StagedCommitChangeRef>,
    ) -> Result<String, LixError> {
        let commit_id = self
            .staged_writes
            .stage_selected_commit_change_refs(version_id.clone(), selected_changes)?;
        self.staged_writes
            .add_commit_parent(version_id, source_parent_commit_id)?;
        Ok(commit_id)
    }

    /// Creates a version-ref reader scoped to this write transaction.
    pub(crate) fn version_ref_reader(&mut self) -> impl VersionRefReader + '_ {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        self.version_ctx.ref_reader(read)
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) fn tracked_state_reader(
        &mut self,
    ) -> TrackedStateStoreReader<StorageReadScope<B::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        self.tracked_state.reader(read)
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) fn commit_graph_reader(
        &mut self,
    ) -> CommitGraphStoreReader<StorageReadScope<B::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        CommitGraphContext::new().reader(read)
    }
}

pub(crate) struct TransactionSqlReadExecutionContext<R> {
    active_version_id: String,
    read_store: StorageReadScope<R>,
    live_state: Arc<LiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    version_ctx: Arc<VersionContext>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    staged: crate::transaction::staging::PreparedStateRowOverlay,
}

impl<R> TransactionSqlReadExecutionContext<R>
where
    R: crate::storage::StorageBackendRead,
{
    pub(crate) fn close(self) -> Result<(), LixError> {
        self.read_store.close().map_err(Into::into)
    }
}

impl<R> SqlExecutionContext for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage::StorageBackendRead + Clone + Send + Sync + 'static,
{
    type ReadStore = StorageReadScope<R>;

    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn crate::live_state::LiveStateReader> {
        Arc::new(TransactionReadLiveStateReader {
            base: self.live_state.reader(self.read_store.clone()),
            staged: self.staged.clone(),
        })
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            json_reader: crate::json_store::JsonStoreContext::new().reader(self.read_store.store()),
        }
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            store: self.read_store.clone(),
            json_reader: crate::json_store::JsonStoreContext::new().reader(self.read_store.store()),
        }
    }

    fn commit_graph(&self) -> Box<dyn crate::commit_graph::CommitGraphReader> {
        Box::new(CommitGraphContext::new().reader(self.read_store.clone()))
    }

    fn version_ref(&self) -> Arc<dyn VersionRefReader> {
        Arc::new(self.version_ctx.ref_reader(self.read_store.clone()))
    }

    fn blob_reader(&self) -> Arc<dyn crate::binary_cas::BlobDataReader> {
        Arc::new(self.binary_cas.reader(self.read_store.clone()))
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }
}

struct TransactionReadLiveStateReader<R> {
    base: crate::live_state::LiveStateStoreReader<StorageReadScope<R>>,
    staged: crate::transaction::staging::PreparedStateRowOverlay,
}

#[async_trait]
impl<R> crate::live_state::LiveStateReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage::StorageBackendRead + Clone + Send + Sync,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        overlay_scan_rows(&self.base, &self.staged, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        Ok(self
            .scan_rows(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_ids: vec![request.entity_id.clone()],
                    version_ids: vec![request.version_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .into_iter()
            .next())
    }
}

fn prepare_state_row(
    normalized: NormalizedTransactionWriteRow,
    functions: &FunctionProviderHandle,
) -> Result<PreparedStateRow, LixError> {
    let NormalizedTransactionWriteRow {
        row,
        snapshot,
        schema_plan_id,
        facts,
    } = normalized;
    let updated_at = row.updated_at.unwrap_or_else(|| functions.call_timestamp());
    let snapshot = snapshot
        .map(|value| stage_json_from_value(value, "prepared row snapshot_content"))
        .transpose()?;
    let metadata = row
        .metadata
        .map(|value| stage_json_from_value(value, "prepared row metadata"))
        .transpose()?;
    Ok(PreparedStateRow {
        schema_plan_id,
        facts,
        entity_id: row.entity_id.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "normalized transaction write row is missing entity_id",
            )
        })?,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot,
        metadata,
        origin: row.origin,
        created_at: row.created_at.unwrap_or_else(|| updated_at.clone()),
        updated_at,
        global: row.global,
        change_id: if row.untracked {
            row.change_id
        } else {
            Some(row.change_id.unwrap_or_else(|| functions.call_uuid_v7()))
        },
        commit_id: row.commit_id,
        untracked: row.untracked,
        version_id: row.version_id,
    })
}

pub(crate) struct OpenTransaction<B: StorageBackend = InMemoryStorageBackend> {
    pub(crate) transaction: Transaction<B>,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction<B>(
    mode: &SessionMode,
    storage: StorageContext<B>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    version_ctx: Arc<VersionContext>,
    catalog_context: Arc<CatalogContext>,
) -> Result<OpenTransaction<B>, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    Transaction::open(
        mode,
        storage,
        live_state,
        tracked_state,
        binary_cas,
        version_ctx,
        catalog_context,
    )
    .await
}

#[async_trait]
impl<B> SqlWriteExecutionContext for Transaction<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        self.binary_cas.reader(&read).load_bytes_many(hashes).await
    }

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let base = self.live_state.reader(&read);
        overlay_scan_rows(&base, &staged, request).await
    }

    async fn load_version_head(&mut self, version_id: &str) -> Result<Option<String>, LixError> {
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let result = self
            .version_ctx
            .ref_reader(&read)
            .load_head_commit_id(version_id)
            .await;
        result
    }

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Transaction::stage_write(self, write).await
    }
}

fn transaction_write_version_ids(write: &TransactionWrite) -> BTreeSet<String> {
    match write {
        TransactionWrite::Rows { rows, .. } => transaction_write_row_version_ids(rows),
        TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } => transaction_write_row_version_ids(rows)
            .into_iter()
            .chain(stage_file_data_version_ids(file_data))
            .collect(),
    }
}

#[cfg(feature = "storage-benches")]
fn transaction_write_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.len(),
        TransactionWrite::RowsWithFileData { rows, .. } => rows.len(),
    }
}

#[cfg(feature = "storage-benches")]
fn transaction_write_untracked_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.iter().filter(|row| row.untracked).count(),
        TransactionWrite::RowsWithFileData { rows, .. } => {
            rows.iter().filter(|row| row.untracked).count()
        }
    }
}

fn require_valid_transaction_write_storage_scopes(
    write: &TransactionWrite,
) -> Result<(), LixError> {
    match write {
        TransactionWrite::Rows { rows, .. } => {
            require_valid_transaction_write_row_storage_scopes(rows)
        }
        TransactionWrite::RowsWithFileData { rows, .. } => {
            require_valid_transaction_write_row_storage_scopes(rows)
        }
    }
}

fn require_valid_transaction_write_row_storage_scopes(
    rows: &[TransactionWriteRow],
) -> Result<(), LixError> {
    for row in rows {
        require_valid_storage_scope(row.version_id.as_str(), row.global)?;
    }
    Ok(())
}

fn require_valid_storage_scope(version_id: &str, global: bool) -> Result<(), LixError> {
    if global != (version_id == GLOBAL_VERSION_ID) {
        return Err(LixError::new(
            LixError::CODE_INVALID_STORAGE_SCOPE,
            format!("invalid storage scope: version_id='{version_id}', global={global}"),
        ));
    }
    Ok(())
}

fn transaction_write_row_version_ids(rows: &[TransactionWriteRow]) -> BTreeSet<String> {
    rows.iter().map(|row| row.version_id.clone()).collect()
}

fn stage_file_data_version_ids(file_data: &[TransactionFileData]) -> BTreeSet<String> {
    file_data
        .iter()
        .map(|write| write.version_id.clone())
        .collect()
}

async fn resolve_active_version_id(
    mode: &SessionMode,
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { version_id } => Ok(version_id.clone()),
        SessionMode::Workspace => load_workspace_version_id(live_state, version_ctx, read).await,
    }
}

async fn load_workspace_version_id(
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
) -> Result<String, LixError> {
    let row = live_state
        .reader(read)
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: EntityIdentity::single(WORKSPACE_VERSION_KEY),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector is missing lix_key_value:lix_workspace_version_id",
            )
        })?;
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace version selector is missing snapshot_content",
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace version selector snapshot is invalid JSON: {error}"),
        )
    })?;
    let version_id = snapshot
        .get("value")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector value must be a non-empty string",
            )
        })?
        .to_string();

    let head = version_ctx
        .ref_reader(read)
        .load_head_commit_id(&version_id)
        .await?;
    if head.is_none() {
        return Err(LixError::version_not_found(
            version_id,
            "load_workspace_version_id",
            "workspace_selector",
        ));
    }

    Ok(version_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::changelog::ChangelogReader;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions};
    use crate::tracked_state::{TrackedStateKey, TrackedStateScanRequest};
    use crate::transaction::types::TransactionJson;
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};
    use crate::version::VersionContext;
    use crate::NullableKeyFilter;
    use crate::GLOBAL_VERSION_ID;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "schema-fixture-commit";

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let tracked_state = Arc::new(crate::tracked_state::TrackedStateContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::clone(&tracked_state),
            Arc::clone(&binary_cas),
            Arc::clone(&version_ctx),
            Arc::clone(&catalog_context),
        )
        .await
        .expect("transaction should open");
        let mut transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        transaction
            .stage_rows(vec![
                key_value_stage_row("tracked-programmatic", "tracked", false),
                key_value_stage_row("untracked-programmatic", "untracked", true),
            ])
            .await
            .expect("programmatic rows should stage");
        transaction
            .commit(&runtime_functions)
            .await
            .expect("transaction should commit");

        let tracked_row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("tracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("tracked row should load")
            .expect("tracked row should exist");
        let tracked_change_id = tracked_row
            .change_id
            .as_ref()
            .expect("tracked row should have a change id")
            .clone();
        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[tracked_change_id],
            })
            .await
            .expect("changelog should load tracked change");
        assert!(
            matches!(
                changes.entries.as_slice(),
                [Some(change)]
                    if change.entity_id.as_single_string_owned().as_deref()
                        == Ok("tracked-programmatic")
            ),
            "tracked staged row should be appended to changelog"
        );

        let head_commit_id = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load")
            .expect("tracked commit should advance the global version ref");

        let tracked_row = crate::tracked_state::TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_rows_at_commit(
                &head_commit_id,
                &[TrackedStateKey {
                    schema_key: "lix_key_value".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(
                        "tracked-programmatic",
                    ),
                    file_id: None,
                }],
            )
            .await
            .expect("tracked state should load")
            .pop()
            .flatten()
            .expect("tracked row should be present in tracked state");
        assert_eq!(tracked_row.commit_id, head_commit_id);
        assert_eq!(
            tracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"tracked-programmatic","value":"tracked"}"#)
        );

        let untracked_row = crate::untracked_state::UntrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("untracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("untracked state should load")
            .expect("untracked row should be present in untracked state");
        assert_eq!(
            untracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"untracked-programmatic","value":"untracked"}"#)
        );

        let live_untracked_row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&crate::live_state::LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("untracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load")
            .expect("untracked row should be visible through live state");
        assert!(live_untracked_row.untracked);
        assert!(live_untracked_row.global);
        assert_eq!(live_untracked_row.version_id, GLOBAL_VERSION_ID);

        let tracked_rows = crate::tracked_state::TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .scan_rows_at_commit(&head_commit_id, &TrackedStateScanRequest::default())
            .await
            .expect("tracked state should scan");
        assert!(
            tracked_rows
                .iter()
                .all(|row| row.entity_id.as_single_string_owned().as_deref()
                    != Ok("untracked-programmatic")),
            "untracked staged rows should not be written into tracked state"
        );
    }

    #[tokio::test]
    async fn commit_validates_staged_rows_before_persistence() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::new(crate::tracked_state::TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            Arc::clone(&version_ctx),
            Arc::clone(&catalog_context),
        )
        .await
        .expect("transaction should open");
        let mut transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        let mut invalid_row = key_value_stage_row("invalid-programmatic", "invalid", false);
        invalid_row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({"key": "invalid-programmatic"}),
        ));
        transaction
            .stage_rows(vec![invalid_row])
            .await
            .expect("invalid row should still reach commit validation");

        let error = transaction
            .commit(&runtime_functions)
            .await
            .expect_err("validation should reject before persistence");
        assert!(
            error.message.contains("snapshot_content validation failed"),
            "validation error should explain the rejected schema data: {error:?}"
        );

        let head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load after failed commit");
        assert_eq!(
            head.as_deref(),
            Some(SCHEMA_FIXTURE_COMMIT_ID),
            "validation failure must not advance the version ref"
        );
    }

    #[tokio::test]
    async fn commit_rejects_non_object_metadata_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let (live_state, _binary_cas, version_ref, runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("invalid-metadata", "value", false);
        row.metadata = Some(TransactionJson::from_value_for_test(json!("not-an-object")));
        transaction
            .stage_rows(vec![row])
            .await
            .expect("row should stage before metadata validation");

        let error = transaction
            .commit(&runtime_functions)
            .await
            .expect_err("non-object metadata should fail commit validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("metadata") && error.message.contains("JSON object"),
            "error should explain metadata object validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &live_state,
            &version_ref,
            "invalid-metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("unknown-schema", "value", false);
        row.schema_key = "missing_schema".to_string();

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("unknown schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .message
                .contains("schema 'missing_schema' is not visible"),
            "error should explain missing schema visibility: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_missing_version_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("ghost-version-row", "value", false);
        row.version_id = "ghost-version".to_string();
        row.global = false;

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("missing version should be rejected before staging");

        assert_eq!(error.code, LixError::CODE_VERSION_NOT_FOUND);
        assert!(
            error
                .message
                .contains("version 'ghost-version' was not found"),
            "error should explain missing version: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_invalid_storage_scope_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("invalid-storage-scope", "value", false);
        row.version_id = GLOBAL_VERSION_ID.to_string();
        row.global = false;

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("invalid storage scope should be rejected before staging");

        assert_eq!(error.code, LixError::CODE_INVALID_STORAGE_SCOPE);
        assert!(
            error.message.contains("version_id='global', global=false"),
            "error should explain invalid storage scope: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_invalid_snapshot_json_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("invalid-json", "value", false);
        row.snapshot = Some(TransactionJson::from_value_for_test(json!("not-an-object")));

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("non-object snapshot should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("must be a JSON object"),
            "error should explain invalid snapshot shape: {error:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_snapshot_that_violates_json_schema_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let (live_state, _binary_cas, version_ref, runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("schema-mismatch", "value", false);
        row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({"key": "schema-mismatch"}),
        ));
        transaction
            .stage_rows(vec![row])
            .await
            .expect("row should stage before JSON Schema validation");

        let error = transaction
            .commit(&runtime_functions)
            .await
            .expect_err("JSON Schema mismatch should fail commit validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("snapshot_content validation failed"),
            "error should explain JSON Schema validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &live_state,
            &version_ref,
            "schema-mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("malformed-registered-schema", "value", false);
        row.schema_key = "lix_registered_schema".to_string();
        row.snapshot = Some(TransactionJson::from_value_for_test(json!({
            "value": {
                "x-lix-key": "malformed_registered_schema",
                "x-lix-primary-key": ["id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }
        })));
        row.entity_id = None;

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("malformed registered schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("x-lix-primary-key"),
            "error should explain malformed registered schema: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_primary_key_entity_id_mismatch_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _version_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("right-id", "value", false);
        row.entity_id = Some(crate::entity_identity::EntityIdentity::single("wrong-id"));

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("entity id mismatch should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .message
                .contains("does not match x-lix-primary-key derived entity_id"),
            "error should explain entity id mismatch: {error:?}"
        );
    }

    async fn open_test_transaction(
        backend: &InMemoryStorageBackend,
    ) -> (
        Arc<LiveStateContext>,
        Arc<BinaryCasContext>,
        Arc<VersionContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage,
            Arc::clone(&live_state),
            Arc::new(crate::tracked_state::TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            Arc::clone(&version_ctx),
            catalog_context,
        )
        .await
        .expect("transaction should open");
        let transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        (
            live_state,
            binary_cas,
            version_ctx,
            runtime_functions,
            transaction,
        )
    }

    async fn seed_visible_schema_rows(storage: StorageContext) {
        let mut writes = StorageWriteSet::new();
        let rows = crate::schema::seed_schema_definitions()
            .into_iter()
            .map(|schema| {
                let key = crate::schema::schema_key_from_definition(schema)
                    .expect("seed schema key should derive");
                let snapshot_content = json!({ "value": schema }).to_string();
                crate::tracked_state::MaterializedTrackedStateRow {
                    entity_id: crate::schema::registered_schema_entity_id(&key.schema_key)
                        .expect("registered schema identity should derive"),
                    schema_key: "lix_registered_schema".to_string(),
                    file_id: None,
                    snapshot_content: Some(snapshot_content),
                    metadata: None,
                    deleted: false,
                    created_at: "1970-01-01T00:00:00.000Z".to_string(),
                    updated_at: "1970-01-01T00:00:00.000Z".to_string(),
                    change_id: format!("schema-fixture-{}", key.schema_key),
                    commit_id: SCHEMA_FIXTURE_COMMIT_ID.to_string(),
                }
            })
            .collect::<Vec<_>>();
        let version_ref_row = prepare_version_ref_row(
            GLOBAL_VERSION_ID,
            SCHEMA_FIXTURE_COMMIT_ID,
            "1970-01-01T00:00:00.000Z",
        )
        .expect("schema fixture version ref should stage");
        let mut read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("schema fixture read should open");
        crate::test_support::stage_tracked_root_from_materialized(
            &mut read,
            &mut writes,
            &crate::tracked_state::TrackedStateContext::new(),
            SCHEMA_FIXTURE_COMMIT_ID,
            None,
            &rows,
        )
        .await
        .expect("schema fixture rows should stage");
        crate::untracked_state::UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows([version_ref_row.row.as_ref()])
            .expect("schema fixture version ref should stage");
        storage
            .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
            .expect("schema fixture transaction should commit");
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageContext,
        live_state: &LiveStateContext,
        version_ctx: &VersionContext,
        rejected_entity_id: &str,
    ) {
        let head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load after failed commit");
        assert_eq!(
            head.as_deref(),
            Some(SCHEMA_FIXTURE_COMMIT_ID),
            "validation failure must not advance the version ref"
        );
        let row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&crate::live_state::LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single(rejected_entity_id),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load after failed commit");
        assert_eq!(
            row, None,
            "validation failure must happen before live-state persistence"
        );
    }

    fn key_value_stage_row(key: &str, value: &str, untracked: bool) -> TransactionWriteRow {
        TransactionWriteRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single(key)),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot: Some(TransactionJson::from_value_for_test(json!({
                "key": key,
                "value": value,
            }))),
            metadata: None,
            origin: None,
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked,
            version_id: GLOBAL_VERSION_ID.to_string(),
        }
    }
}
