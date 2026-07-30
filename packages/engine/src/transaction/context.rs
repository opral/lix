#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobDataReader, BlobHash};
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchHeadControlContext, BranchRefReader,
    branch_ref_stage_row,
};
use crate::catalog::{
    CatalogContext, CatalogFingerprint, CatalogSnapshot, SchemaPlanId, load_catalog_revision,
    stage_catalog_revision,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, CommitId, load_change_records,
    materialize_known_change_payloads,
};
use crate::checkpoint::{
    CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_history_from_head, checkpoint_marker_stage_row,
    latest_checkpoint_at_head,
};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    BlobRefRowInput, DERIVED_FILE_REF_SCHEMA_KEY, DerivedFileRefRowInput, FilesystemPathIndex,
    FilesystemPathIndexCache, FilesystemPathIndexReader, FilesystemPathIndexRequest,
    FilesystemPathKind, FilesystemRowContext, append_blob_ref_tombstone_row,
    append_derived_file_ref_tombstone_row, load_path_index_revision,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::gc::{
    CheckpointGcState, CheckpointPublication, CheckpointRecoveryRef, load_checkpoint_gc_state,
    load_recovery_ref,
};
#[cfg(test)]
use crate::live_state::LiveStateRowRequest;
use crate::live_state::{
    LiveStateContext, LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter,
    LiveStateProjection, LiveStateScanRequest, MaterializedLiveStateBatch,
    MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch, MaterializedLiveStateRow,
    MaterializedLiveStateRowRef, StagedLiveStateRows, TrackedHeadContext, TrackedWorkingDiff,
    overlay_load_exact_batch, overlay_scan_batch,
};
use crate::plugin::{
    ArcByteSource, BoundCreateContext, CompiledPluginCatalog, FileBytesSha256,
    LiveBatchEntitySource, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginActorCache,
    PluginActorColdInstall, PluginActorColdOpen, PluginActorKey, PluginActorLease,
    PluginActorStore, PluginActorStorePermit, PluginArchiveInstallPlan, PluginContentType,
    PluginFileOwner, PluginMaterialization, PluginObservation, PluginRegistry, PluginRegistryEntry,
    PluginRegistryEntryInput, PluginRuntimeHost, V2SchemaAllowlist, ValidatedConflictTransition,
    ValidatedFileTransition, ValidatedSameLengthOutputSplice, VecEntityChangeSource,
    VecEntityConflictSource, VecEntitySource, build_file_update_splices, canonicalize_v2_snapshot,
    drain_conflict_transition_resolutions, drain_entity_transition_edits,
    drain_file_transition_changes, host_entity_change_with_lazy_snapshot,
    host_entity_with_lazy_snapshot, inferred_media_type_for_path, is_plugin_storage_path,
    is_reservation_key, local_mutation_identity, materialize_keyless_creates,
    plugin_archive_file_id_matches, plugin_install_plan_from_archive_path,
    plugin_key_from_archive_delete_origin, plugin_state_live_state_projection,
    require_existing_id_authorities, reservation_tombstone_row, reserve_create_row,
    transport_splice_preserves_git_text, transport_splice_preserves_utf8, validate_create_changes,
    validate_create_reservation,
};
use crate::session::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotency, ExecuteIdempotencyReceipt, SessionMode,
    encode_receipt, load_workspace_branch_id_from_index,
};
use crate::sql2::{
    CertifiedHistoryChange, CertifiedHistoryReader, ChangelogQuerySource, DiffCommand,
    HistoryQuerySource, MaterializedChange, SessionFileViewKey, SessionFileViewMutation,
    SessionFileViews, SessionPluginFileView, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::sql2::{SqlPlanningCache, SqlWriteExecutionContext};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    Memory, StoragePrecondition, StorageReadOptions, StorageWriteOptions, StorageWriteSetStats,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateScanRequest,
    TrackedStateStoreReader,
};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedRowFacts, REGISTERED_SCHEMA_KEY, normalize_raw_write_row_in_place,
    remember_pending_registered_schema,
};
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{
    PreparedStateRowOverlay, PreparedWriteSet, TransactionWriteBuffer,
};
use crate::transaction::types::{
    PreparedRowFacts, PreparedStateBatch, PreparedTransactionWrite, RawWriteBatch, RawWriteRowRef,
    StagedCommitChangeBatch, StagedCommitChangeBatchBuilder, TransactionFileData, TransactionJson,
    TransactionWrite, TransactionWriteMode, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteOutcome, TransactionWriteRow, canonicalize_transaction_json_batch,
    stage_json_from_value,
};

pub(crate) struct CertifiedHistoryStoreReader<S> {
    store: S,
}

impl<S> CertifiedHistoryStoreReader<S> {
    pub(crate) const fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> CertifiedHistoryReader for CertifiedHistoryStoreReader<S>
where
    S: StorageAdapterRead + Send + Sync,
{
    async fn scan(
        &self,
        commit_ids: &BTreeSet<CommitId>,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<CertifiedHistoryChange>, LixError> {
        Ok(
            crate::live_state::scan_certified_history_rows(&self.store, commit_ids, request)
                .await?
                .into_iter()
                .filter_map(|row| {
                    Some(CertifiedHistoryChange {
                        commit_id: row.commit_id?,
                        change: MaterializedChange {
                            id: row.change_id?.to_string(),
                            entity_pk: row.entity_pk,
                            schema_key: row.schema_key,
                            file_id: row.file_id,
                            snapshot_content: row.snapshot_content,
                            metadata: row.metadata,
                            created_at: row.created_at.to_string(),
                            origin_key: None,
                        },
                    })
                })
                .collect(),
        )
    }
}
use crate::transaction::validation::{
    TransactionValidationInput, fresh_plugin_file_import_certificate,
    prepared_tracked_rows_have_row_local_certificates, validate_certified_fresh_plugin_file_import,
    validate_certified_tracked_insert_identities, validate_prepared_writes,
};
use crate::wasm::{
    WasmChangeEffect, WasmComponentV2Actor, WasmComponentV2Factory, WasmConflictUpdate,
    WasmDocumentHandle, WasmEntityChange, WasmEntityConflict, WasmEntityKey, WasmEntityUpdate,
    WasmFileDescriptor, WasmFileUpdate, WasmHostBytes, WasmHostEntity, WasmHostEntityChanges,
    WasmOpenEntitiesInput, WasmOpenFileInput, WasmPluginSelection, WasmTransitionLimits,
};
use crate::{LixError, NullableKeyFilter, SqlQueryResult, Value};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome {
    pub(crate) storage_stats: StorageWriteSetStats,
}

/// The durable identity and byte proof of one plugin materialization.
///
/// The semantic root fences actor state. Blob-backed plugins retain a CAS
/// reference; derived plugins retain only the exact rendered byte fingerprint.
#[derive(Debug, Clone)]
struct VisibleV2Materialization {
    semantic_root: String,
    bytes: VisibleV2MaterializationBytes,
}

#[derive(Debug, Clone)]
enum VisibleV2MaterializationBytes {
    Blob {
        hash: BlobHash,
    },
    Derived {
        path: String,
        sha256: FileBytesSha256,
        size_bytes: u64,
    },
}

#[cfg(test)]
fn decode_visible_v2_materialization(
    row: &MaterializedLiveStateRow,
    file_id: &str,
) -> Result<VisibleV2Materialization, LixError> {
    decode_visible_v2_materialization_parts(
        row.schema_key.as_str(),
        row.change_id,
        row.snapshot_content.as_deref(),
        file_id,
    )
}

fn decode_visible_v2_materialization_ref(
    row: MaterializedLiveStateRowRef<'_>,
    file_id: &str,
) -> Result<VisibleV2Materialization, LixError> {
    decode_visible_v2_materialization_parts(
        row.schema_key(),
        row.change_id(),
        row.snapshot_content().map(|content| content.as_str()),
        file_id,
    )
}

fn decode_visible_v2_materialization_parts(
    schema_key: &str,
    change_id: Option<ChangeId>,
    snapshot_content: Option<&str>,
    file_id: &str,
) -> Result<VisibleV2Materialization, LixError> {
    let semantic_root = change_id.map(|root| root.to_string()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("v2 materialization root for file '{file_id}' is missing change_id"),
        )
    })?;
    let snapshot = snapshot_content.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "owned v2 plugin file '{file_id}' materialization is missing its durable proof"
            ),
        )
    })?;
    let bytes = match schema_key {
        BLOB_REF_SCHEMA_KEY => {
            let snapshot: PluginUpgradeBlobRefSnapshot =
                serde_json::from_str(snapshot).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "owned v2 plugin file '{file_id}' has an invalid blob reference: {error}"
                        ),
                    )
                })?;
            if snapshot.id != file_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{file_id}' materialization identity does not match its file scope"
                    ),
                ));
            }
            VisibleV2MaterializationBytes::Blob {
                hash: BlobHash::from_hex(&snapshot.blob_hash)?,
            }
        }
        DERIVED_FILE_REF_SCHEMA_KEY => {
            let snapshot: DerivedFileRefSnapshot = serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{file_id}' has an invalid derived materialization: {error}"
                    ),
                )
            })?;
            if snapshot.id != file_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{file_id}' materialization identity does not match its file scope"
                    ),
                ));
            }
            let sha256 = FileBytesSha256::from_lower_hex(&snapshot.sha256).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{file_id}' has an invalid derived SHA-256 proof"
                    ),
                )
            })?;
            VisibleV2MaterializationBytes::Derived {
                path: snapshot.path,
                sha256,
                size_bytes: snapshot.size_bytes,
            }
        }
        schema_key => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned v2 plugin file '{file_id}' materialization uses unsupported schema '{schema_key}'"
                ),
            ));
        }
    };
    Ok(VisibleV2Materialization {
        semantic_root,
        bytes,
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TransactionPathIndexBuildStats {
    builds: usize,
    descriptor_rows: usize,
}

#[cfg(test)]
static TRANSACTION_PATH_INDEX_BUILD_STATS: Mutex<TransactionPathIndexBuildStats> =
    Mutex::new(TransactionPathIndexBuildStats {
        builds: 0,
        descriptor_rows: 0,
    });

#[cfg(test)]
fn reset_transaction_path_index_build_stats() {
    *TRANSACTION_PATH_INDEX_BUILD_STATS
        .lock()
        .expect("transaction path index build stats lock") =
        TransactionPathIndexBuildStats::default();
}

#[cfg(test)]
fn transaction_path_index_build_stats() -> TransactionPathIndexBuildStats {
    *TRANSACTION_PATH_INDEX_BUILD_STATS
        .lock()
        .expect("transaction path index build stats lock")
}

#[cfg(test)]
fn record_transaction_path_index_build(descriptor_rows: usize) {
    let mut stats = TRANSACTION_PATH_INDEX_BUILD_STATS
        .lock()
        .expect("transaction path index build stats lock");
    stats.builds += 1;
    stats.descriptor_rows += descriptor_rows;
}

/// One execution-scoped transaction capability for engine write paths.
///
/// This is intentionally not a session-wide kitchen sink. It owns the storage
/// write transaction for one `SessionContext::execute(...)` call and projects
/// accepted SQL/provider writes back into the SQL DAG through an engine-local live-state
/// overlay.
///
/// Transaction invariant: this is the capability for engine operations
/// that may write. Write-relevant reads must be exposed from this transaction,
/// after the storage write transaction has begun, rather than from session-level
/// helpers.
pub(crate) struct Transaction<StorageImpl: Storage = Memory> {
    active_branch_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    schema_resolver: TransactionSchemaResolver,
    /// SQL binding is snapshot-isolated at transaction open. Schema writes
    /// staged later in this transaction affect validation but become visible
    /// to SQL planning only after commit opens a new transaction snapshot.
    sql_schema_snapshot: Arc<CatalogSnapshot>,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    storage: StorageAdapter<StorageImpl>,
    functions: FunctionProviderHandle,
    /// Tracked-state revision observed by the coherent transaction-open read.
    /// Durable tracked publication must still be based on this revision;
    /// untracked current-state writes do not invalidate the tracked snapshot.
    opening_tracked_mutation_revision: Option<Bytes>,
    commit_boundary: Option<TransactionCommitBoundary>,
    trust_filesystem_planner: bool,
    origin_key: Option<SharedStr>,
    idempotency_receipt: Option<(crate::storage_adapter::StorageKey, Vec<u8>)>,
    session_file_views: SessionFileViews,
    pending_file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    pending_plugin_actor_publications: Vec<PendingPluginActorPublication>,
    plugin_generation_read_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    plugin_generation_upgrade_guard: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
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

    async fn commit<T, F>(&self, commit: impl FnOnce() -> F) -> Result<T, LixError>
    where
        F: Future<Output = Result<T, LixError>>,
    {
        let _gate = self.state.lock_commit().await;
        self.check()?;
        commit().await
    }
}

#[derive(Clone)]
pub(crate) struct CommitBoundaryState {
    active_count: Arc<AtomicUsize>,
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    watch: tokio::sync::watch::Sender<usize>,
}

impl CommitBoundaryState {
    pub(crate) fn new() -> Self {
        let (watch, _) = tokio::sync::watch::channel(0);
        Self {
            active_count: Arc::new(AtomicUsize::new(0)),
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
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

    pub(crate) async fn lock_commit(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_gate.lock().await
    }

    pub(crate) fn try_lock_commit(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        self.commit_gate.try_lock().ok()
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

pub(crate) async fn commit_at_boundary<T, F>(
    boundary: Option<&TransactionCommitBoundary>,
    commit: impl FnOnce() -> F,
) -> Result<T, LixError>
where
    F: Future<Output = Result<T, LixError>>,
{
    match boundary {
        Some(boundary) => boundary.commit(commit).await,
        None => commit().await,
    }
}

impl<StorageImpl> Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn ensure_opening_snapshot_is_current(
        &self,
    ) -> impl Future<Output = Result<(), LixError>> + Send + 'static {
        let storage = self.storage.clone();
        let opening_revision = self.opening_tracked_mutation_revision.clone();
        async move {
            let current = storage.load_tracked_mutation_revision().await?;
            if current == opening_revision {
                return Ok(());
            }
            Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "transaction snapshot is stale because tracked state changed after it opened",
            )
            .with_hint("Retry the transaction against the latest committed state."))
        }
    }

    /// Opens an execution-scoped staging area for SQL/provider hooks.
    async fn open(
        mode: &SessionMode,
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        plugin_host: PluginRuntimeHost,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        session_file_views: SessionFileViews,
    ) -> Result<OpenTransaction<StorageImpl>, LixError> {
        let read =
            SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
        let setup_result = async {
            let active_branch_id =
                resolve_active_branch_id(mode, live_state.as_ref(), branch_ctx.as_ref(), &read)
                    .await?;
            let runtime_functions = FunctionContext::prepare(&read).await?;
            let functions = runtime_functions.provider();
            let (sql_schema_catalog, tracked_schema_catalog) = {
                let catalog_revision = load_catalog_revision(&read).await?;
                let visible_live_state = live_state.reader(&read);
                let sql_schema_catalog = catalog_context
                    .compiled_catalog_for_transaction_open(
                        &visible_live_state,
                        &Domain::schema_catalog(active_branch_id.clone(), true),
                        catalog_revision.as_ref(),
                    )
                    .await?;
                // SQL planning needs the untracked-visible catalog, while
                // normal tracked mutations normalize against the tracked
                // catalog. Pin both under the same revision at open so the
                // first write never falls back to a catalog scan.
                let tracked_schema_catalog = catalog_context
                    .compiled_catalog_for_transaction_open(
                        &visible_live_state,
                        &Domain::schema_catalog(active_branch_id.clone(), false),
                        catalog_revision.as_ref(),
                    )
                    .await?;
                (sql_schema_catalog, tracked_schema_catalog)
            };
            let opening_tracked_mutation_revision =
                StorageAdapter::<StorageImpl>::load_tracked_mutation_revision_from_read(&read)
                    .await?;
            Ok::<_, LixError>((
                active_branch_id,
                runtime_functions,
                functions,
                sql_schema_catalog,
                tracked_schema_catalog,
                opening_tracked_mutation_revision,
            ))
        }
        .await;
        let (
            active_branch_id,
            runtime_functions,
            functions,
            sql_schema_catalog,
            tracked_schema_catalog,
            opening_tracked_mutation_revision,
        ) = match setup_result {
            Ok(result) => result,
            Err(error) => {
                return Err(error);
            }
        };
        drop(read);
        let mut schema_resolver = TransactionSchemaResolver::new(Arc::clone(&catalog_context));
        schema_resolver.remember_compiled_catalog(
            &Domain::schema_catalog(active_branch_id.clone(), true),
            Arc::clone(&sql_schema_catalog),
        );
        schema_resolver.remember_compiled_catalog(
            &Domain::schema_catalog(active_branch_id.clone(), false),
            tracked_schema_catalog,
        );
        let staged_writes = Arc::new(TransactionWriteBuffer::new(functions.clone()));
        Ok(OpenTransaction {
            transaction: Self {
                active_branch_id,
                live_state,
                tracked_state,
                binary_cas,
                plugin_host,
                branch_ctx,
                schema_resolver,
                sql_schema_snapshot: sql_schema_catalog,
                sql_planning_cache,
                staged_writes,
                filesystem_path_index_cache: Arc::new(FilesystemPathIndexCache::default()),
                filesystem_path_index_epoch: Arc::new(AtomicUsize::new(0)),
                storage,
                functions,
                opening_tracked_mutation_revision,
                commit_boundary: None,
                trust_filesystem_planner: false,
                origin_key: None,
                idempotency_receipt: None,
                session_file_views,
                pending_file_view_mutations: BTreeMap::new(),
                pending_plugin_actor_publications: Vec::new(),
                plugin_generation_read_guard: None,
                plugin_generation_upgrade_guard: None,
            },
            runtime_functions,
        })
    }

    /// Commits prepared writes, runtime function state, and the storage transaction.
    ///
    /// Commit owns the execution boundary: prepared rows become changelog
    /// facts, branch-ref updates, and visible live_state rows before the
    /// storage transaction is committed.
    pub(crate) async fn commit(
        self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let mut transaction = self;
        let commit_boundary = transaction.commit_boundary.clone();
        let prepared_writes = match transaction.staged_writes.drain() {
            Ok(prepared_writes) => prepared_writes,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
        };
        transaction
            .uncache_completed_plugin_actors_for_large_file_writes(&prepared_writes)
            .await;
        let tracked_state_changed = prepared_writes.state_rows.iter().any(|row| !row.untracked)
            || !prepared_writes.commit_change_refs_by_branch.is_empty()
            || !prepared_writes.extra_commit_parents_by_branch.is_empty();
        let has_untracked_state_writes = prepared_writes.state_rows.iter().any(|row| row.untracked);
        // Untracked rows are mutable current state, but their validation can read
        // tracked schemas, parents, uniqueness owners, or filesystem state.
        // Fence that snapshot without rotating the tracked revision: normal
        // tracked transactions remain independent of untracked-only commits.
        let requires_tracked_snapshot_fence = tracked_state_changed || has_untracked_state_writes;
        let catalog_revision_changed = prepared_writes_change_catalog(&prepared_writes);
        let _commit_guard = begin_commit_boundary(commit_boundary.as_ref());
        if let Err(error) = check_commit_boundary(commit_boundary.as_ref()) {
            transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return Err(error);
        }
        // Validate and materialize from one coherent storage snapshot. The
        // final write's tracked-state precondition fences the decisions made
        // here, including plugin-produced prepared rows.
        let commit_read_storage = transaction.storage.clone();
        let mut read = SharedStorageAdapterRead::new(
            commit_read_storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let commit_parent_heads = match commit::resolve_prepared_commit_parent_heads(
            transaction.branch_ctx.as_ref(),
            &read,
            &prepared_writes,
            true,
        )
        .await
        {
            Ok(commit_parent_heads) => commit_parent_heads,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = transaction
            .validate_prepared_writes_by_branch(&read, &prepared_writes)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_validation"
            ))
            .await
        {
            transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return Err(error);
        }
        let filesystem_delta_rows =
            if prepared_writes_require_filesystem_index_rebuild(&prepared_writes) {
                Vec::new()
            } else {
                prepared_writes
                    .state_rows
                    .iter()
                    .filter(|row| {
                        matches!(
                            row.schema_key.as_str(),
                            "lix_file_descriptor"
                                | "lix_directory_descriptor"
                                | BLOB_REF_SCHEMA_KEY
                        )
                    })
                    .map(MaterializedLiveStateRow::from)
                    .collect::<Vec<_>>()
            };
        let previous_filesystem_revision = if filesystem_delta_rows.is_empty() {
            None
        } else {
            load_path_index_revision(&read).await.ok().flatten()
        };
        let (mut writes, materialization_preconditions) =
            match commit::commit_prepared_writes_with_parent_heads(
                &transaction.binary_cas,
                Some(runtime_functions),
                &commit_parent_heads,
                &mut read,
                prepared_writes,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_materialization"
            ))
            .await
            {
                Ok(writes) => writes,
                Err(error) => {
                    transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                    return Err(error);
                }
            };
        if catalog_revision_changed {
            stage_catalog_revision(&mut writes);
        }
        if tracked_state_changed {
            StorageAdapter::<StorageImpl>::stage_tracked_mutation_revision(&mut writes);
        }
        let mut write_options = StorageWriteOptions::default();
        write_options
            .preconditions
            .extend(materialization_preconditions);
        if requires_tracked_snapshot_fence {
            write_options.preconditions.push(
                StorageAdapter::<StorageImpl>::tracked_mutation_revision_precondition(
                    transaction.opening_tracked_mutation_revision.clone(),
                ),
            );
        }
        if let Some((key, value)) = transaction.idempotency_receipt.take() {
            writes.put(EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, key.clone(), value);
            // The mutation and this receipt share one atomic storage commit.
            // A protocol acknowledgement may replay only from a durable
            // receipt, so ask the storage to cross its durability boundary
            // before it reports this commit as successful.
            write_options.idempotency_key = Some(key.0.clone());
            write_options
                .preconditions
                .push(StoragePrecondition::KeyAbsent {
                    space: EXECUTE_IDEMPOTENCY_RECEIPT_SPACE.id,
                    key,
                });
        }
        // Keep the prepared commit's storage borrow independent from the
        // transaction so deterministic preparation failures can still drain
        // prospective plugin actor documents before returning.
        let commit_storage = transaction.storage.clone();
        let prepared_commit = match commit_storage
            .prepare_write_set(writes, write_options)
            .await
        {
            Ok(prepared_commit) => prepared_commit,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error.into());
            }
        };
        let storage_stats = commit_at_boundary(commit_boundary.as_ref(), || async move {
            let (_commit, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
        let post_commit_read_storage = transaction.storage.clone();
        if !filesystem_delta_rows.is_empty()
            && incremental_filesystem_index_enabled()
            && let Ok(next_read) = post_commit_read_storage
                .begin_read(StorageReadOptions::default())
                .await
        {
            let next_read = SharedStorageAdapterRead::new(next_read);
            if let Ok(next_revision) = load_path_index_revision(&next_read).await {
                transaction.live_state.advance_filesystem_path_indexes(
                    previous_filesystem_revision.as_deref(),
                    next_revision.as_deref(),
                    &filesystem_delta_rows,
                );
            }
        }
        for publication in std::mem::take(&mut transaction.pending_plugin_actor_publications) {
            let session_key = publication.session_key().clone();
            match publication.publish().await {
                Ok((key, view)) => {
                    transaction
                        .pending_file_view_mutations
                        .insert(key.clone(), SessionFileViewMutation::Set { key, view });
                }
                Err(_) => {
                    // Actor/materialization publication is derived state. A
                    // durable commit remains successful; revoke the private
                    // view so the next exact read cold-opens safely.
                    transaction.pending_file_view_mutations.insert(
                        session_key.clone(),
                        SessionFileViewMutation::Remove { key: session_key },
                    );
                }
            }
        }
        transaction.session_file_views.apply_mutations(
            std::mem::take(&mut transaction.pending_file_view_mutations).into_values(),
        );
        Ok(TransactionCommitOutcome { storage_stats })
    }

    /// Large import documents are more valuable as transient parser state than
    /// as cache entries. Release their completed Stores before validation and
    /// materialization so guest arenas do not overlap the atomic storage
    /// batch. The durable semantic rows and exact materialized bytes remain
    /// authoritative; a later semantic edit takes the ordinary cold path.
    async fn uncache_completed_plugin_actors_for_large_file_writes(
        &mut self,
        prepared_writes: &PreparedWriteSet,
    ) {
        const MAX_RETAINED_IMPORT_BYTES: usize = 8 * 1024 * 1024;

        let large_files = prepared_writes
            .file_data_writes
            .iter()
            .filter(|write| write.len() > MAX_RETAINED_IMPORT_BYTES)
            .map(|write| (write.branch_id.as_str(), write.file_id.as_str()))
            .collect::<BTreeSet<_>>();
        if large_files.is_empty() {
            return;
        }

        for index in (0..self.pending_plugin_actor_publications.len()).rev() {
            if self.pending_plugin_actor_publications[index].retains_large_import_actor() {
                continue;
            }
            let session_key = self.pending_plugin_actor_publications[index].session_key();
            if !large_files
                .contains(&(session_key.branch_id.as_str(), session_key.file_id.as_str()))
            {
                continue;
            }
            let publication = self
                .pending_plugin_actor_publications
                .remove(index)
                .into_uncached()
                .await;
            self.pending_plugin_actor_publications
                .insert(index, publication);
        }
    }

    pub(crate) fn attach_commit_boundary(&mut self, boundary: TransactionCommitBoundary) {
        self.commit_boundary = Some(boundary);
    }

    pub(crate) fn trust_serialized_filesystem_planner(&mut self) {
        self.trust_filesystem_planner = true;
    }

    /// Admits a Store for a file that has no durable plugin actor yet.
    ///
    /// Fresh documents are independent until this transaction commits. When
    /// their pending Stores fill the working set, retire the oldest completed
    /// candidate after its durable rows and materialization have been staged,
    /// then reuse that admission slot.
    async fn admit_fresh_plugin_store(
        &mut self,
        current_publications: &mut Vec<PendingPluginActorPublication>,
    ) -> Result<PluginActorStorePermit, LixError> {
        loop {
            match self.plugin_host.actor_cache().admit_store() {
                Ok(permit) => return Ok(permit),
                Err(error) if error.code == LixError::CODE_PLUGIN_RESOURCE_LIMIT => {
                    if retire_oldest_completed_actor(current_publications).await
                        || retire_oldest_completed_actor(
                            &mut self.pending_plugin_actor_publications,
                        )
                        .await
                    {
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Rolls back the storage transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    pub(crate) async fn rollback(mut self) -> Result<(), LixError> {
        self.discard_pending_plugin_actor_publications().await;
        Ok(())
    }

    async fn discard_pending_plugin_actor_publications(&mut self) {
        discard_plugin_actor_publications(std::mem::take(
            &mut self.pending_plugin_actor_publications,
        ))
        .await;
    }

    /// Stages one decoded write batch into this transaction.
    ///
    /// This is the programmatic write entrypoint used by non-SQL APIs. The
    /// transaction owns the `RawWriteBatch` → `PreparedStateBatch` transition,
    /// so generated timestamps, change ids, commit ids, and commit change refs
    /// stay in one batch pipeline.
    pub(crate) async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        // Staging is transaction-local. Commit validates and materializes from
        // one coherent snapshot, then fences that snapshot in the durable
        // write. Re-checking it for every staged batch only repeats point
        // reads; it cannot make a stale write publish successfully.
        if !transaction_write_has_plugin_lifecycle_candidate(&write) {
            // Acquire before normalization, plugin/state reads, or actor work.
            // The owned guard remains on this transaction through its durable
            // commit, so an upgrade cannot preflight across an in-flight
            // ordinary mutation and then swap authority ahead of it.
            self.ensure_plugin_generation_read_guard().await;
        }
        self.reject_write_after_collection_replacement(&write)?;
        require_valid_transaction_write_storage_scopes(&write)?;
        // A normal write targets the branch this transaction already opened
        // against, so its existence is checked together with the coherent
        // commit snapshot. Preserve the useful early error for the uncommon
        // programmatic cross-branch write: normalization cannot meaningfully
        // resolve a schema from a branch that does not exist.
        if transaction_write_targets_non_active_branch(&write, &self.active_branch_id) {
            self.require_existing_transaction_write_branch_ids(&write)
                .await?;
        }
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(transaction_write_row_count(
                &write,
            ));
            crate::storage_bench::record_transaction_untracked_rows(
                transaction_write_untracked_row_count(&write),
            );
        }
        let (write, file_view_mutations, actor_publications) =
            self.reconcile_plugin_write(write).await?;
        if let Err(error) = require_valid_reconciled_transaction_write_storage_scopes(&write) {
            discard_plugin_actor_publications(actor_publications).await;
            return Err(error);
        }
        let write = match self
            .prepare_transaction_write(write)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_prepare_rows"
            ))
            .await
        {
            Ok(write) => write,
            Err(error) => {
                discard_plugin_actor_publications(actor_publications).await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .preflight_derived_path_stability_before_stage(&write)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_path_preflight"
            ))
            .await
        {
            discard_plugin_actor_publications(actor_publications).await;
            return Err(error);
        }
        let affects_filesystem_path_index =
            prepared_transaction_write_affects_filesystem_path_index(&write);
        let outcome = match tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_buffer_stage"
        )
        .in_scope(|| self.staged_writes.stage_write(write))
        {
            Ok(outcome) => outcome,
            Err(error) => {
                discard_plugin_actor_publications(actor_publications).await;
                return Err(error);
            }
        };
        if affects_filesystem_path_index {
            self.filesystem_path_index_epoch
                .fetch_add(1, Ordering::SeqCst);
        }
        self.pending_file_view_mutations.extend(file_view_mutations);
        self.pending_plugin_actor_publications
            .extend(actor_publications);
        Ok(outcome)
    }

    fn reject_write_after_collection_replacement(
        &self,
        write: &TransactionWrite,
    ) -> Result<(), LixError> {
        let rows = match write {
            TransactionWrite::Rows { rows, .. }
            | TransactionWrite::RowsWithFileData { rows, .. } => rows,
        };
        let staged = self.staged_writes.staging_overlay()?;
        for row in rows.iter() {
            if row.schema_key.as_str()
                == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            {
                continue;
            }
            if StagedLiveStateRows::collection_replaced(
                &staged,
                row.branch_id,
                row.schema_key,
                row.file_id.map(SharedStr::as_str),
            )? {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "collection '{}' was deleted earlier in this transaction",
                        row.schema_key
                    ),
                )
                .with_hint(
                    "Commit the collection deletion before recreating rows in its next generation.",
                ));
            }
        }
        Ok(())
    }

    /// Validates descriptor-path changes against the current batch before it
    /// enters the transaction overlay. The prospective overlay is deliberately
    /// ephemeral: a rejected move must not leave a partial staged row behind.
    async fn preflight_derived_path_stability_before_stage(
        &mut self,
        write: &PreparedTransactionWrite,
    ) -> Result<(), LixError> {
        let prepared_rows = prepared_transaction_write_rows(write);
        if !prepared_rows.iter().any(|row| {
            !row.global
                && !row.untracked
                && matches!(
                    row.schema_key.as_str(),
                    FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                )
        }) {
            return Ok(());
        }
        let prospective_rows = materialized_live_state_batch_from_prepared(prepared_rows);
        let staged = self.staged_writes.staging_overlay()?;
        let prospective = ProspectiveStagedRows {
            staged: staged.clone(),
            rows: prospective_rows,
        };
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read);
        preflight_derived_path_stability(&base, &staged, &prospective, &prospective.rows).await
    }

    /// Runs the stateless conflict resolver for one pinned v2 plugin
    /// generation. Unlike normal file mutation this creates no persistent
    /// document actor: a merge may need to resolve one row from a large file,
    /// and the resulting rows are rendered once by the ordinary staged-write
    /// reconciliation path.
    ///
    /// The caller supplies a registry entry loaded from the historical merge
    /// roots, not the mutable current registry. This keeps `b` selection
    /// and plugin code selection deterministic across merge direction and
    /// retries.
    pub(crate) async fn resolve_v2_plugin_conflicts(
        &mut self,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        conflicts: Vec<WasmEntityConflict<WasmHostBytes>>,
    ) -> Result<ValidatedConflictTransition, LixError> {
        self.ensure_plugin_generation_read_guard().await;
        let limits = WasmTransitionLimits::default();
        let expected_count = conflicts.len();
        let source = VecEntityConflictSource::new(conflicts, limits)?;
        let wasm_hash = BlobHash::from_hex(plugin.wasm_blob_hash())?;
        let factory = match self
            .plugin_host
            .cached_plugin_v2_factory(plugin.key(), wasm_hash)?
        {
            Some(factory) => factory,
            None => {
                let read = SharedStorageAdapterRead::new(
                    self.storage
                        .begin_read(StorageReadOptions::default())
                        .await?,
                );
                let reader = self.binary_cas.reader(read);
                let wasm = load_transaction_blob_bytes(&reader, &self.staged_writes, &[wasm_hash])
                    .await?
                    .into_vec()
                    .into_iter()
                    .next()
                    .flatten()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!(
                                "plugin registry references missing WASM blob '{}'",
                                wasm_hash.to_hex()
                            ),
                        )
                    })?;
                let installed = plugin.to_installed_plugin(wasm)?;
                self.plugin_host
                    .load_or_compile_v2_factory(&installed)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_conflict_factory_compile"
                    ))
                    .await?
            }
        };

        // Static conflict resolution is short lived, but it still owns a
        // Wasmtime Store and must honor the same workspace-wide admission
        // bound as retained document actors.
        let permit = self.plugin_host.actor_cache().admit_store()?;
        let actor = factory
            .instantiate_actor()
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_conflict_actor_instantiate"
            ))
            .await?;
        let mut store = PluginActorStore::new(actor, permit);
        let transition = match store
            .actor_mut()
            .resolve_conflicts(
                limits,
                WasmConflictUpdate {
                    descriptor,
                    conflicts: Box::new(source),
                },
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_resolve_conflicts"
            ))
            .await
        {
            Ok(transition) => transition,
            Err(error) => {
                let _ = store.actor_mut().retire().await;
                return Err(error);
            }
        };
        let validated = match drain_conflict_transition_resolutions(
            store.actor_mut(),
            transition,
            expected_count,
            limits,
        )
        .await
        {
            Ok(validated) => validated,
            Err(error) => {
                let _ = store.actor_mut().retire().await;
                return Err(error);
            }
        };
        store.actor_mut().retire().await?;
        self.plugin_host
            .record_v2_transition_counters(validated.counters);
        Ok(validated)
    }

    async fn scan_visible_live_state_batch(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read);
        overlay_scan_batch(&base, &staged, request).await
    }

    async fn visible_v2_materialization(
        &mut self,
        key: &PluginFileWriteKey,
    ) -> Result<Option<VisibleV2Materialization>, LixError> {
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![
                        BLOB_REF_SCHEMA_KEY.to_string(),
                        DERIVED_FILE_REF_SCHEMA_KEY.to_string(),
                    ],
                    entity_pks: vec![validated_uuid_entity_pk(&key.file_id)?],
                    branch_ids: vec![key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(key.file_id.clone())],
                    untracked: Some(key.untracked),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            })
            .await?;
        if rows.len() > 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "v2 materialization lookup returned duplicate rows for file '{}'",
                    key.file_id
                ),
            ));
        }
        rows.get(0)
            .map(|row| decode_visible_v2_materialization_ref(row, &key.file_id))
            .transpose()
    }

    async fn cold_open_v2_semantic_actor(
        &mut self,
        actor_key: &PluginActorKey,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentV2Factory>,
        current_publications: &mut Vec<PendingPluginActorPublication>,
    ) -> Result<PluginObservation, LixError> {
        let cache = self.plugin_host.actor_cache();
        let _cold_open_guard = cache.cold_open_guard().await;
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read.clone());
        let file_key = PluginFileWriteKey {
            branch_id: actor_key.branch_id.clone(),
            global: false,
            untracked: false,
            file_id: actor_key.file_id.clone(),
        };
        let blob_rows = overlay_scan_batch(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![
                        BLOB_REF_SCHEMA_KEY.to_string(),
                        DERIVED_FILE_REF_SCHEMA_KEY.to_string(),
                    ],
                    entity_pks: vec![validated_uuid_entity_pk(&actor_key.file_id)?],
                    branch_ids: vec![actor_key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(actor_key.file_id.clone())],
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        if blob_rows.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                format!(
                    "owned v2 plugin file '{}' must have exactly one visible materialization; found {}",
                    actor_key.file_id,
                    blob_rows.len()
                ),
            ));
        }
        let materialization =
            decode_visible_v2_materialization_ref(blob_rows.row(0), &actor_key.file_id)?;
        if !matches!(
            (plugin.materialization(), &materialization.bytes),
            (
                PluginMaterialization::Blob,
                &VisibleV2MaterializationBytes::Blob { .. }
            ) | (
                PluginMaterialization::Derived,
                &VisibleV2MaterializationBytes::Derived { .. }
            )
        ) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned v2 plugin file '{}' materialization does not match plugin '{}' contract",
                    actor_key.file_id,
                    plugin.key()
                ),
            ));
        }
        let semantic_root = materialization.semantic_root.clone();
        let cold_open = cache.prepare_cold_open(actor_key, &semantic_root).await?;
        let mut cold_install: PluginActorColdInstall = match cold_open {
            PluginActorColdOpen::Ready(observation) => return Ok(observation),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let store_permit = loop {
            match cache.admit_cold_store(&mut cold_install) {
                Ok(permit) => break permit,
                Err(error) if error.code == LixError::CODE_PLUGIN_RESOURCE_LIMIT => {
                    if retire_oldest_completed_actor(current_publications).await
                        || retire_oldest_completed_actor(
                            &mut self.pending_plugin_actor_publications,
                        )
                        .await
                    {
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };
        let limits = WasmTransitionLimits::default();
        let rows = overlay_scan_batch(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: plugin.schema_keys().to_vec(),
                    branch_ids: vec![actor_key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(actor_key.file_id.clone())],
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_state_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let entity_ordinals =
            v2_host_entity_ordinals_from_live_batch(&rows, &file_key, plugin.schema_keys())?;
        let entity_count = entity_ordinals.len();
        let mut actor = factory.instantiate_actor().await?;
        if let VisibleV2MaterializationBytes::Derived { path, .. } = &materialization.bytes
            && descriptor.path.as_deref() != Some(path.as_str())
        {
            let _ = actor.retire().await;
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned v2 plugin file '{}' derived materialization was rendered at '{}' but now resolves to '{}'",
                    actor_key.file_id,
                    path,
                    descriptor.path.as_deref().unwrap_or_default(),
                ),
            ));
        }
        let materialization_bytes = materialization.bytes.clone();
        let validated = match materialization_bytes {
            VisibleV2MaterializationBytes::Blob { hash } => {
                let base_blob_reader = self.binary_cas.reader(read);
                let materialized_bytes: crate::Blob = load_transaction_blob_bytes(
                    &base_blob_reader,
                    &self.staged_writes,
                    &[hash],
                )
                .await?
                .into_vec()
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "owned v2 plugin file '{}' references missing materialized blob '{}'",
                            actor_key.file_id,
                            hash.to_hex()
                        ),
                    )
                })?
                .into();
                let source = LiveBatchEntitySource::new(rows, entity_ordinals, limits)?;
                let transition = match actor
                    .open_entities(
                        limits,
                        WasmOpenEntitiesInput {
                            descriptor,
                            entities: Box::new(source),
                            accepted: Some(Arc::new(ArcByteSource::new(
                                materialized_bytes.clone(),
                            ))),
                        },
                    )
                    .await
                {
                    Ok(transition) => transition,
                    Err(error) => {
                        let _ = actor.retire().await;
                        return Err(error);
                    }
                };
                match drain_entity_transition_edits(
                    actor.as_mut(),
                    transition,
                    materialized_bytes.as_ref(),
                    Some(materialized_bytes.clone()),
                    None,
                    limits,
                )
                .await
                {
                    Ok(validated) => validated,
                    Err(error) => {
                        let _ = actor.retire().await;
                        return Err(error);
                    }
                }
            }
            VisibleV2MaterializationBytes::Derived {
                sha256, size_bytes, ..
            } => {
                let source = LiveBatchEntitySource::new(rows, entity_ordinals, limits)?;
                let transition = match actor
                    .open_entities(
                        limits,
                        WasmOpenEntitiesInput {
                            descriptor,
                            entities: Box::new(source),
                            accepted: None,
                        },
                    )
                    .await
                {
                    Ok(transition) => transition,
                    Err(error) => {
                        let _ = actor.retire().await;
                        return Err(error);
                    }
                };
                let empty: crate::Blob = Vec::new().into();
                let validated = match drain_entity_transition_edits(
                    actor.as_mut(),
                    transition,
                    empty.as_ref(),
                    None,
                    None,
                    limits,
                )
                .await
                {
                    Ok(validated) => validated,
                    Err(error) => {
                        let _ = actor.retire().await;
                        return Err(error);
                    }
                };
                if validated.bytes.len() as u64 != size_bytes
                    || FileBytesSha256::compute(&validated.bytes) != sha256
                {
                    let _ = actor.retire().await;
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "owned v2 plugin file '{}' derived materialization does not reproduce its durable proof",
                            actor_key.file_id
                        ),
                    ));
                }
                validated
            }
        };
        let materialized_bytes = validated.bytes.clone();
        let materialized_bytes_sha256 = match materialization.bytes {
            VisibleV2MaterializationBytes::Blob { .. } => validated.bytes_sha256,
            VisibleV2MaterializationBytes::Derived { .. } => {
                Some(FileBytesSha256::compute(&materialized_bytes))
            }
        };
        let mut counters = validated.counters;
        counters.full_state_semantic_rows_materialized =
            u64::try_from(entity_count).unwrap_or(u64::MAX);
        counters.full_document_reparses = 1;
        counters.full_renderer_invocations = 1;
        self.plugin_host.record_v2_transition_counters(counters);
        cache
            .install_cold_if_absent(
                cold_install,
                actor_key.clone(),
                PluginActorStore::new(actor, store_permit),
                validated.document,
                materialized_bytes,
                materialized_bytes_sha256,
                Arc::<str>::from(semantic_root),
            )
            .await
    }

    /// Leases an acknowledged actor, cold-opening only when cache eviction is
    /// the sole reason the observation no longer resolves.
    ///
    /// The durable semantic root must still exactly match the root delivered
    /// with the observation. A concurrent committed transition therefore
    /// remains stale and cannot be mistaken for benign working-set eviction.
    async fn lease_or_reopen_observed_v2_actor(
        &mut self,
        observation: &PluginObservation,
        actor_key: &PluginActorKey,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentV2Factory>,
        current_publications: &mut Vec<PendingPluginActorPublication>,
    ) -> Result<PluginActorLease, LixError> {
        let cache = self.plugin_host.actor_cache();
        match cache.lease_for_transition(observation).await {
            Ok(lease) => return Ok(lease),
            Err(error) if error.code == LixError::CODE_PLUGIN_OBSERVATION_STALE => {
                let file_key = PluginFileWriteKey {
                    branch_id: actor_key.branch_id.clone(),
                    global: false,
                    untracked: false,
                    file_id: actor_key.file_id.clone(),
                };
                let Some(visible_materialization) =
                    self.visible_v2_materialization(&file_key).await?
                else {
                    return Err(error);
                };
                if visible_materialization.semantic_root != observation.semantic_root() {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }

        let reopened = self
            .cold_open_v2_semantic_actor(
                actor_key,
                plugin,
                descriptor,
                factory,
                current_publications,
            )
            .await?;
        cache.lease_for_transition(&reopened).await
    }

    async fn load_visible_exact_live_state_batch(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read);
        overlay_load_exact_batch(&base, &staged, request).await
    }

    /// Drops `format-only` upserts that are semantically identical to the
    /// currently accepted durable entity. The exact-row lookup keeps this
    /// proportional to the sparse format-only output instead of hydrating the
    /// complete file graph.
    async fn suppress_v2_format_only_noops(
        &mut self,
        plugin: &PluginRegistryEntry,
        changes: WasmHostEntityChanges,
        file_key: &PluginFileWriteKey,
    ) -> Result<(WasmHostEntityChanges, BTreeSet<WasmEntityKey>), LixError> {
        let format_only_keys = changes
            .changes
            .iter()
            .filter_map(|change| match change {
                WasmEntityChange::Upsert {
                    entity,
                    effect: WasmChangeEffect::FormatOnly,
                } => Some(entity.key.clone()),
                WasmEntityChange::Create { .. }
                | WasmEntityChange::Upsert { .. }
                | WasmEntityChange::Delete(_) => None,
            })
            .collect::<Vec<_>>();
        if format_only_keys.is_empty() {
            return Ok((changes, BTreeSet::new()));
        }

        let requests = format_only_keys
            .iter()
            .map(|key| {
                Ok(LiveStateExactRowRequest {
                    schema_key: key.schema_key.to_string(),
                    branch_id: file_key.branch_id.clone(),
                    entity_pk: plugin_entity_pk(plugin, key)?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let current = self
            .load_visible_exact_live_state_batch(&LiveStateExactBatchRequest {
                rows: requests,
                projection: plugin_state_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let observed_existing = format_only_keys
            .iter()
            .enumerate()
            .filter(|(slot, _)| current.row(*slot).is_some())
            .map(|(_, key)| key.clone())
            .collect();
        Ok((
            suppress_v2_format_only_noops_against_batch(changes, &format_only_keys, &current)?,
            observed_existing,
        ))
    }

    /// Materializes keyless creates and returns the one durable mutation
    /// reservation row when this transition creates at least one entity.
    /// Existing keyed updates are checked with exact sparse authority reads.
    async fn v2_create_rows(
        &mut self,
        plugin: &PluginRegistryEntry,
        changes: &mut WasmHostEntityChanges,
        bound: BoundCreateContext,
        file_key: &PluginFileWriteKey,
        existing_reservation: Option<&MaterializedLiveStateRow>,
        known_existing_authorities: Option<&BTreeSet<WasmEntityKey>>,
    ) -> Result<RawWriteBatch, LixError> {
        let mut validation = validate_create_changes(plugin, changes)?;
        if let Some(known) = known_existing_authorities {
            validation
                .existing_authorities
                .retain(|key| !known.contains(key));
        }
        materialize_keyless_creates(changes, bound.creates())?;
        if !validation.requires_reservation && validation.existing_authorities.is_empty() {
            return Ok(RawWriteBatch::new());
        }

        let exact_rows = validation
            .existing_authorities
            .iter()
            .map(|key| {
                let [id] = key.entity_pk.as_slice() else {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "creatable schema '{}' requires one UUID primary-key component",
                            key.schema_key
                        ),
                    ));
                };
                Ok(LiveStateExactRowRequest {
                    schema_key: key.schema_key.to_string(),
                    branch_id: file_key.branch_id.clone(),
                    entity_pk: EntityPk::uuid_from_canonical(id).map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("v2 plugin emitted invalid entity_pk: {error}"),
                        )
                    })?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let loaded = if exact_rows.is_empty() {
            MaterializedLiveStateExactBatch::default()
        } else {
            self.load_visible_exact_live_state_batch(&LiveStateExactBatchRequest {
                rows: exact_rows,
                projection: plugin_state_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?
        };
        require_existing_id_authorities(
            plugin,
            &validation.existing_authorities,
            &loaded,
            &file_key.file_id,
            &file_key.branch_id,
        )?;

        let mut rows = RawWriteBatch::with_capacity(usize::from(validation.requires_reservation));
        if validation.requires_reservation {
            if let Some(row) = reserve_create_row(
                existing_reservation,
                bound,
                &file_key.file_id,
                &file_key.branch_id,
            )? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn preflight_v2_create(
        &mut self,
        bound: BoundCreateContext,
        file_key: &PluginFileWriteKey,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        self.preflight_v2_creates(&[(bound, file_key.clone())])
            .await
            .map(|mut rows| rows.pop().expect("one preflight produces one result"))
    }

    async fn preflight_v2_creates(
        &mut self,
        requests: &[(BoundCreateContext, PluginFileWriteKey)],
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let loaded = self
            .load_visible_exact_live_state_batch(&LiveStateExactBatchRequest {
                rows: requests
                    .iter()
                    .map(|(bound, file_key)| LiveStateExactRowRequest {
                        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                        branch_id: file_key.branch_id.clone(),
                        entity_pk: EntityPk::single(bound.reservation_key()),
                        file_id: Some(file_key.file_id.clone()),
                    })
                    .collect(),
                projection: plugin_state_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let mut existing_rows = Vec::with_capacity(requests.len());
        for (index, (bound, file_key)) in requests.iter().enumerate() {
            let existing = loaded.row(index).map(MaterializedLiveStateRowRef::to_owned);
            validate_create_reservation(
                existing.as_ref(),
                *bound,
                &file_key.file_id,
                &file_key.branch_id,
            )?;
            existing_rows.push(existing);
        }
        Ok(existing_rows)
    }

    async fn v2_id_reservation_tombstones(
        &mut self,
        file_key: &PluginFileWriteKey,
    ) -> Result<RawWriteBatch, LixError> {
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                    branch_ids: vec![file_key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(file_key.file_id.clone())],
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            })
            .await?;
        let mut tombstones = RawWriteBatch::with_capacity(rows.len());
        for row in rows.iter() {
            let Ok(key) = row.entity_pk().as_single_string() else {
                continue;
            };
            if is_reservation_key(key) {
                tombstones.push(reservation_tombstone_row(
                    key,
                    &file_key.file_id,
                    &file_key.branch_id,
                )?);
            }
        }
        Ok(tombstones)
    }

    /// Replacing one materialization representation must retire the other
    /// schema explicitly. A raw blob can predate plugin ownership, so inspect
    /// the visible row rather than inferring that fact from the incoming write.
    async fn opposite_materialization_tombstone(
        &mut self,
        file_key: &PluginFileWriteKey,
        target: PluginMaterialization,
    ) -> Result<RawWriteBatch, LixError> {
        let Some(visible) = self.visible_v2_materialization(file_key).await? else {
            return Ok(RawWriteBatch::new());
        };
        let context = FilesystemRowContext {
            branch_id: file_key.branch_id.clone(),
            global: file_key.global,
            untracked: file_key.untracked,
            file_id: None,
            metadata: None,
        };
        let mut rows = RawWriteBatch::with_capacity(1);
        match (target, visible.bytes) {
            (PluginMaterialization::Derived, VisibleV2MaterializationBytes::Blob { .. }) => {
                append_blob_ref_tombstone_row(&mut rows, file_key.file_id.clone(), context);
            }
            (PluginMaterialization::Blob, VisibleV2MaterializationBytes::Derived { .. }) => {
                append_derived_file_ref_tombstone_row(&mut rows, file_key.file_id.clone(), context);
            }
            _ => {}
        }
        Ok(rows)
    }

    async fn reconcile_plugin_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<
        (
            ReconciledTransactionWrite,
            BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
            Vec<PendingPluginActorPublication>,
        ),
        LixError,
    > {
        match write {
            TransactionWrite::Rows { mode, mut rows } => {
                reject_external_plugin_registry_rows(&rows)?;
                let count = rows.len() as u64;
                let mut file_data = Vec::new();
                let mut reconciliation = self
                    .plugin_write_reconciliation(&mut rows, &mut file_data)
                    .await?;
                let mut rows = reconciliation.take_reconciled_rows(rows);
                for (file_key, version) in &reconciliation.materialization_versions {
                    let target = if reconciliation
                        .derived_materializations
                        .contains_key(file_key)
                    {
                        PluginMaterialization::Derived
                    } else {
                        PluginMaterialization::Blob
                    };
                    let mut materialization_rows = self
                        .opposite_materialization_tombstone(file_key, target)
                        .await?;
                    let materialized_row_index = materialization_rows.len();
                    if let Some(proof) = reconciliation.derived_materializations.get(file_key) {
                        DerivedFileRefRowInput {
                            file_id: file_key.file_id.clone(),
                            path: proof.path.clone(),
                            sha256: proof.sha256.to_lower_hex(),
                            size_bytes: proof.size_bytes,
                            context: FilesystemRowContext {
                                branch_id: file_key.branch_id.clone(),
                                global: file_key.global,
                                untracked: file_key.untracked,
                                file_id: None,
                                metadata: None,
                            },
                        }
                        .append_to(&mut materialization_rows)?;
                    } else {
                        let payload = file_data
                            .iter()
                            .find(|write| PluginFileWriteKey::from(*write) == *file_key)
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "v2 semantic materialization payload for file '{}' is missing",
                                        file_key.file_id
                                    ),
                                )
                            })?;
                        BlobRefRowInput {
                            file_id: file_key.file_id.clone(),
                            blob_hash: payload
                                .blob_hash()
                                .unwrap_or_else(|| BlobHash::from_content(payload.data())),
                            size_bytes: payload.len(),
                            context: FilesystemRowContext {
                                branch_id: file_key.branch_id.clone(),
                                global: file_key.global,
                                untracked: file_key.untracked,
                                file_id: None,
                                metadata: None,
                            },
                        }
                        .append_to(&mut materialization_rows)?;
                    }
                    materialization_rows.set_change_id(
                        materialized_row_index,
                        Some(SharedStr::from(version.clone())),
                    );
                    mark_plugin_reconciliation_batch(&mut materialization_rows, 0)?;
                    rows.append_raw_batch(materialization_rows);
                }
                let write = if file_data.is_empty() {
                    ReconciledTransactionWrite::Rows { mode, rows }
                } else {
                    ReconciledTransactionWrite::RowsWithFileData {
                        mode,
                        rows,
                        file_data,
                        count,
                    }
                };
                Ok((
                    write,
                    reconciliation.file_view_mutations,
                    reconciliation.actor_publications,
                ))
            }
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                mut file_data,
                count,
            } => {
                let mut rows = rows;
                reject_external_plugin_registry_rows(&rows)?;
                let mut reconciliation = self
                    .plugin_write_reconciliation(&mut rows, &mut file_data)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_reconciliation"
                    ))
                    .await?;
                let mut rows = reconciliation.take_reconciled_rows(rows);
                rows.retain_raw(|row| {
                    !reconciliation
                        .materialization_versions
                        .keys()
                        .any(|key| key.matches_materialization_row(row))
                        && !reconciliation
                            .file_keys
                            .iter()
                            .any(|key| key.matches_materialization_row(row))
                })?;
                for (file_key, version) in &reconciliation.materialization_versions {
                    let target = if reconciliation
                        .derived_materializations
                        .contains_key(file_key)
                    {
                        PluginMaterialization::Derived
                    } else {
                        PluginMaterialization::Blob
                    };
                    let mut materialization_rows = self
                        .opposite_materialization_tombstone(file_key, target)
                        .await?;
                    let materialized_row_index = materialization_rows.len();
                    if let Some(proof) = reconciliation.derived_materializations.get(file_key) {
                        DerivedFileRefRowInput {
                            file_id: file_key.file_id.clone(),
                            path: proof.path.clone(),
                            sha256: proof.sha256.to_lower_hex(),
                            size_bytes: proof.size_bytes,
                            context: FilesystemRowContext {
                                branch_id: file_key.branch_id.clone(),
                                global: file_key.global,
                                untracked: file_key.untracked,
                                file_id: None,
                                metadata: None,
                            },
                        }
                        .append_to(&mut materialization_rows)?;
                    } else {
                        let payload = file_data
                            .iter()
                            .find(|write| PluginFileWriteKey::from(*write) == *file_key)
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "v2 materialization payload for file '{}' is missing",
                                        file_key.file_id
                                    ),
                                )
                            })?;
                        BlobRefRowInput {
                            file_id: file_key.file_id.clone(),
                            blob_hash: payload
                                .blob_hash()
                                .unwrap_or_else(|| BlobHash::from_content(payload.data())),
                            size_bytes: payload.len(),
                            context: FilesystemRowContext {
                                branch_id: file_key.branch_id.clone(),
                                global: file_key.global,
                                untracked: file_key.untracked,
                                file_id: None,
                                metadata: None,
                            },
                        }
                        .append_to(&mut materialization_rows)?;
                    }
                    materialization_rows.set_change_id(
                        materialized_row_index,
                        Some(SharedStr::from(version.clone())),
                    );
                    mark_plugin_reconciliation_batch(&mut materialization_rows, 0)?;
                    rows.append_raw_batch(materialization_rows);
                }
                let file_data = file_data
                    .into_iter()
                    .filter(|write| {
                        let key = PluginFileWriteKey::from(write);
                        !reconciliation.file_keys.contains(&key)
                            && !reconciliation.derived_materializations.contains_key(&key)
                            && (!write.is_empty()
                                || reconciliation.materialized_file_keys.contains(&key))
                    })
                    .collect();
                Ok((
                    ReconciledTransactionWrite::RowsWithFileData {
                        mode,
                        rows,
                        file_data,
                        count,
                    },
                    reconciliation.file_view_mutations,
                    reconciliation.actor_publications,
                ))
            }
        }
    }

    fn acknowledged_session_plugin_view(
        &self,
        key: &SessionFileViewKey,
        plugin: &PluginRegistryEntry,
        owner_change_id: &str,
    ) -> Option<SessionPluginFileView> {
        if let Some(mutation) = self.pending_file_view_mutations.get(key) {
            return match mutation {
                SessionFileViewMutation::Set { view, .. }
                    if view.plugin_key == plugin.key()
                        && view.plugin_generation == plugin.archive_blob_hash()
                        && view.owner_change_id == owner_change_id =>
                {
                    Some(view.clone())
                }
                SessionFileViewMutation::Set { .. } | SessionFileViewMutation::Remove { .. } => {
                    None
                }
            };
        }
        self.session_file_views.plugin_file_view(
            key,
            plugin.key(),
            plugin.archive_blob_hash(),
            owner_change_id,
        )
    }

    fn acknowledged_session_plugin_observation(
        &self,
        key: &SessionFileViewKey,
        plugin: &PluginRegistryEntry,
        owner_change_id: &str,
    ) -> Option<PluginObservation> {
        self.acknowledged_session_plugin_view(key, plugin, owner_change_id)
            .and_then(|view| view.observation)
    }

    async fn ensure_plugin_generation_read_guard(&mut self) {
        if self.plugin_generation_read_guard.is_none()
            && self.plugin_generation_upgrade_guard.is_none()
        {
            self.plugin_generation_read_guard =
                Some(self.plugin_host.acquire_plugin_generation_read().await);
        }
    }

    async fn ensure_plugin_generation_upgrade_guard(&mut self) -> Result<(), LixError> {
        if self.plugin_generation_upgrade_guard.is_some() {
            return Ok(());
        }
        if self.plugin_generation_read_guard.is_some() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "a transaction cannot install or uninstall a plugin after staging an ordinary file mutation",
            )
            .with_hint(
                "Stage plugin lifecycle changes before plugin-owned file writes in the same transaction.",
            ));
        }
        self.plugin_generation_upgrade_guard =
            Some(self.plugin_host.acquire_plugin_generation_upgrade().await);
        Ok(())
    }

    /// Reconciles plugin lifecycle, ownership, and state for one logical write
    /// batch against one storage snapshot.
    ///
    /// The first and only mandatory current-state lookup is the small durable
    /// registry row. An empty registry returns before owner, filesystem,
    /// matcher, state, archive, CAS, or WASM work. Non-empty registries use
    /// batched owner/state/CAS reads and execute plugin calls in input order.
    async fn plugin_write_reconciliation(
        &mut self,
        rows: &mut RawWriteBatch,
        file_data: &mut Vec<TransactionFileData>,
    ) -> Result<PluginWriteReconciliation, LixError> {
        let input_row_count = rows.len();
        let mut reconciliation = PluginWriteReconciliation::default();
        let mut lifecycle = BTreeMap::<PluginLifecycleKey, Option<PluginRegistryEntry>>::new();
        let mut lifecycle_schema_keys = Vec::<PluginLifecycleKey>::new();
        let mut lifecycle_schema_rows = RawWriteBatch::new();
        let mut current_install_schema_definitions =
            BTreeMap::<PluginLifecycleKey, BTreeMap<String, JsonValue>>::new();
        let mut current_install_wasm = BTreeMap::<BlobHash, Vec<u8>>::new();
        let mut branch_ids = BTreeSet::<String>::new();

        // Parse each archive exactly once. The original ZIP remains the file
        // payload; the extracted component is staged as a second CAS payload.
        for write in file_data.iter_mut() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            if !is_plugin_storage_path(path) {
                if !write.global && !write.untracked {
                    branch_ids.insert(write.branch_id.clone());
                }
                continue;
            }
            let plan = plugin_install_plan_from_archive_path(
                path,
                write.data(),
                &write.branch_id,
                write.global,
                write.untracked,
            )?;
            if write.file_id != plan.archive_file_id {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "plugin archive '{}' must use deterministic file id '{}'",
                        plan.plugin_key, plan.archive_file_id
                    ),
                ));
            }
            let archive_blob_hash = write.blob_hash().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "plugin archive payload must not be empty",
                )
            })?;
            let PluginArchiveInstallPlan {
                plugin_key,
                archive_file_id,
                parsed,
                schema_rows,
            } = plan;
            let entry = PluginRegistryEntry::new(PluginRegistryEntryInput {
                key: plugin_key.clone(),
                runtime: parsed.manifest.runtime,
                api_version: parsed.manifest.api_version.clone(),
                path_glob: parsed.manifest.file_match.path_glob.clone(),
                content_type: parsed.manifest.file_match.content_type,
                entry: parsed.manifest.entry.clone(),
                schema_keys: parsed.schema_keys.clone(),
                create_schema_keys: parsed.create_schema_keys.clone(),
                manifest_json: parsed.normalized_manifest_json.clone(),
                archive_file_id,
                archive_path: path.to_string(),
                archive_blob_hash: archive_blob_hash.to_hex(),
                wasm_blob_hash: parsed.wasm_hash.to_hex(),
            })?;
            let lifecycle_key = PluginLifecycleKey {
                branch_id: write.branch_id.clone(),
                plugin_key,
            };
            if lifecycle
                .insert(lifecycle_key.clone(), Some(entry))
                .is_some()
            {
                return Err(duplicate_plugin_lifecycle_mutation());
            }
            let schema_definitions = schema_rows
                .iter()
                .map(|row| {
                    let schema_key = row
                        .entity_pk
                        .as_ref()
                        .and_then(|entity_pk| entity_pk.as_single_string().ok())
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin schema row has an invalid identity",
                            )
                        })?
                        .to_string();
                    let definition = row
                        .snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.get("value"))
                        .cloned()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin schema row is missing its definition",
                            )
                        })?;
                    Ok((schema_key, definition))
                })
                .collect::<Result<BTreeMap<_, _>, LixError>>()?;
            current_install_schema_definitions.insert(lifecycle_key.clone(), schema_definitions);
            current_install_wasm
                .entry(parsed.wasm_hash)
                .or_insert_with(|| parsed.wasm_bytes.clone());
            write.add_auxiliary_payload(parsed.wasm_bytes);
            lifecycle_schema_keys.extend(std::iter::repeat_n(
                lifecycle_key.clone(),
                schema_rows.len(),
            ));
            lifecycle_schema_rows.append(schema_rows);
            branch_ids.insert(write.branch_id.clone());
        }

        // A canonical archive descriptor tombstone is the uninstall signal.
        // Other descriptor tombstones are ownership cleanup candidates.
        let mut deleted_file_keys = BTreeMap::<PluginFileWriteKey, Option<TransactionJson>>::new();
        for row in rows.iter().take(input_row_count) {
            if row.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || row.snapshot.is_some() {
                continue;
            }
            let Some(file_id) = row
                .entity_pk
                .as_ref()
                .and_then(|entity_pk| entity_pk.as_single_string_owned().ok())
            else {
                continue;
            };
            if let Some(plugin_key) = row
                .origin
                .as_ref()
                .and_then(|origin| plugin_key_from_archive_delete_origin(&origin.surface))
                .filter(|plugin_key| plugin_archive_file_id_matches(&file_id, plugin_key))
            {
                if row.global || row.untracked || row.branch_id == GLOBAL_BRANCH_ID {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "plugin uninstall requires a tracked branch-local archive",
                    ));
                }
                let lifecycle_key = PluginLifecycleKey {
                    branch_id: row.branch_id.to_string(),
                    plugin_key: plugin_key.to_string(),
                };
                if lifecycle.insert(lifecycle_key, None).is_some() {
                    return Err(duplicate_plugin_lifecycle_mutation());
                }
                branch_ids.insert(row.branch_id.to_string());
                continue;
            }
            if row.global || row.untracked {
                continue;
            }
            let key = PluginFileWriteKey {
                branch_id: row.branch_id.to_string(),
                global: false,
                untracked: false,
                file_id,
            };
            deleted_file_keys
                .entry(key)
                .or_insert_with(|| row.metadata.cloned());
            branch_ids.insert(row.branch_id.to_string());
        }

        // Registered-schema writes are rare lifecycle operations, but they
        // must still consult the registry even when no file data is present.
        // Otherwise a later public UPDATE/DELETE could invalidate an active
        // plugin's durable state contract behind the registry's back.
        for row in rows.iter().take(input_row_count) {
            if row.schema_key == REGISTERED_SCHEMA_KEY && !row.global && !row.untracked {
                branch_ids.insert(row.branch_id.to_string());
            }
        }

        // Ordinary semantic DML carries no filesystem payload. A tracked,
        // file-scoped row may nevertheless belong to an active plugin and
        // therefore needs the small branch registry lookup before the host can
        // decide whether an entity-to-file transition is required.
        for row in rows.iter().take(input_row_count) {
            if !row.global && !row.untracked && row.file_id.is_some() {
                branch_ids.insert(row.branch_id.to_string());
            }
        }

        if branch_ids.is_empty() {
            return Ok(reconciliation);
        }

        // The gate is acquired before the first registry/owner/state snapshot
        // and retained on the transaction through commit or rollback. Shared
        // guards let ordinary file transitions remain concurrent; lifecycle
        // mutations exclude them across preflight and the authority swap.
        if lifecycle.is_empty() {
            self.ensure_plugin_generation_read_guard().await;
        } else {
            self.ensure_plugin_generation_upgrade_guard().await?;
        }

        let staged = self.staged_writes.staging_overlay()?;
        let storage = self.storage.clone();
        let read =
            SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
        let base = self.live_state.reader(read.clone());

        if !lifecycle_schema_rows.is_empty() {
            let mut desired_schemas = BTreeMap::<(String, EntityPk), (String, JsonValue)>::new();
            for (lifecycle_key, row) in lifecycle_schema_keys
                .iter()
                .zip(lifecycle_schema_rows.iter())
            {
                let entity_pk = row.entity_pk.cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its entity identity",
                    )
                })?;
                let snapshot = row.snapshot.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its definition",
                    )
                })?;
                let identity = (row.branch_id.to_string(), entity_pk);
                let definition = snapshot.value().clone();
                if let Some((other_plugin, other_definition)) = desired_schemas.get(&identity)
                    && other_definition != &definition
                {
                    return Err(plugin_schema_collision_error(
                        &lifecycle_key.plugin_key,
                        &identity.1,
                        Some(other_plugin),
                    ));
                }
                desired_schemas.insert(identity, (lifecycle_key.plugin_key.clone(), definition));
            }

            let schema_rows = overlay_scan_batch(
                &base,
                &staged,
                &LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        entity_pks: desired_schemas
                            .keys()
                            .map(|(_, entity_pk)| entity_pk.clone())
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect(),
                        branch_ids: desired_schemas
                            .keys()
                            .map(|(branch_id, _)| branch_id.clone())
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect(),
                        file_ids: vec![NullableKeyFilter::Null],
                        untracked: Some(false),
                        ..Default::default()
                    },
                    projection: plugin_registry_live_state_projection(),
                    ..Default::default()
                },
            )
            .await?;
            let mut existing_schemas = BTreeMap::<(String, EntityPk), JsonValue>::new();
            for row in schema_rows.iter() {
                let Some(snapshot) = row.snapshot_content().map(|content| content.as_str()) else {
                    continue;
                };
                existing_schemas.insert(
                    (row.branch_id().to_string(), row.entity_pk().clone()),
                    serde_json::from_str(snapshot).map_err(|error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            format!("invalid existing registered schema snapshot: {error}"),
                        )
                    })?,
                );
            }
            // Programmatic writes may pair a schema mutation with a plugin
            // archive in one transaction batch. Model those rows after the
            // visible snapshot before checking the derived plugin rows.
            for row in rows.iter().take(input_row_count) {
                if row.schema_key != REGISTERED_SCHEMA_KEY
                    || row.global
                    || row.untracked
                    || row.file_id.is_some()
                {
                    continue;
                }
                let Some(entity_pk) = row.entity_pk.cloned() else {
                    continue;
                };
                let identity = (row.branch_id.to_string(), entity_pk);
                if !desired_schemas.contains_key(&identity) {
                    continue;
                }
                match row.snapshot {
                    Some(snapshot) => {
                        existing_schemas.insert(identity, snapshot.value().clone());
                    }
                    None => {
                        existing_schemas.remove(&identity);
                    }
                }
            }
            for (identity, (plugin_key, definition)) in &desired_schemas {
                if let Some(existing) = existing_schemas.get(identity)
                    && existing != definition
                {
                    return Err(plugin_schema_collision_error(plugin_key, &identity.1, None));
                }
            }
            rows.append(lifecycle_schema_rows);
        }

        let registry_rows = overlay_load_exact_batch(
            &base,
            &staged,
            &LiveStateExactBatchRequest {
                rows: branch_ids
                    .iter()
                    .map(|branch_id| LiveStateExactRowRequest {
                        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                        branch_id: branch_id.clone(),
                        entity_pk: EntityPk::single(PLUGIN_REGISTRY_KEY),
                        file_id: None,
                    })
                    .collect(),
                projection: plugin_registry_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            },
        )
        .await?;
        let mut registries = BTreeMap::<String, PluginRegistry>::new();
        let mut changed_registry_branches = BTreeSet::<String>::new();
        let mut generation_upgrades = Vec::<PluginGenerationUpgrade>::new();
        let mut derived_plugin_uninstalls = BTreeMap::<String, BTreeSet<String>>::new();
        if registry_rows.len() != branch_ids.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable plugin registry lookup expected {} aligned slots, got {}",
                    branch_ids.len(),
                    registry_rows.len()
                ),
            ));
        }
        for (slot, branch_id) in branch_ids.iter().enumerate() {
            // Registry decoding is the terminal plugin-parser boundary. Keep
            // the exact batch owner alive and materialize at most this one
            // scalar DTO while the parser validates it.
            let row = registry_rows
                .row(slot)
                .map(MaterializedLiveStateRowRef::to_owned);
            registries.insert(
                branch_id.clone(),
                PluginRegistry::from_optional_live_state_row(row.as_ref(), branch_id)?,
            );
        }
        for (key, mutation) in lifecycle {
            let registry = registries
                .get_mut(&key.branch_id)
                .expect("lifecycle branch should have a loaded registry");
            match mutation {
                Some(plugin) => {
                    let replacement = plugin.clone();
                    if let Some(previous) = registry.upsert(plugin)?
                        && previous != replacement
                    {
                        generation_upgrades.push(PluginGenerationUpgrade {
                            branch_id: key.branch_id.clone(),
                            previous,
                            replacement,
                        });
                    }
                }
                None => {
                    if let Some(removed) = registry.remove(&key.plugin_key)?
                        && removed.materialization() == PluginMaterialization::Derived
                    {
                        derived_plugin_uninstalls
                            .entry(key.branch_id.clone())
                            .or_default()
                            .insert(removed.key().to_string());
                    }
                }
            }
            changed_registry_branches.insert(key.branch_id);
        }
        for row in rows.iter().take(input_row_count) {
            if row.schema_key != REGISTERED_SCHEMA_KEY || row.global || row.untracked {
                continue;
            }
            let Some(schema_key) = row
                .entity_pk
                .as_ref()
                .and_then(|entity_pk| entity_pk.as_single_string().ok())
            else {
                continue;
            };
            let Some(plugin) = registries.get(row.branch_id.as_str()).and_then(|registry| {
                registry
                    .plugins()
                    .iter()
                    .find(|plugin| plugin.schema_keys().iter().any(|key| key == schema_key))
            }) else {
                continue;
            };
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!(
                    "registered schema '{schema_key}' is owned by active plugin '{}'; uninstall the plugin before migrating or deleting that schema",
                    plugin.key()
                ),
            ));
        }
        if !generation_upgrades.is_empty() {
            let base_blob_reader = self.binary_cas.reader(read.clone());
            preflight_owned_v2_generation_upgrades(
                &self.plugin_host,
                &base,
                &staged,
                &base_blob_reader,
                &self.staged_writes,
                &generation_upgrades,
                &current_install_wasm,
                &current_install_schema_definitions,
            )
            .await?;
        }
        if !derived_plugin_uninstalls.is_empty() {
            preflight_derived_plugin_uninstalls(
                &base,
                &staged,
                &derived_plugin_uninstalls,
                &deleted_file_keys,
            )
            .await?;
        }
        for branch_id in changed_registry_branches {
            rows.push(
                registries
                    .get(&branch_id)
                    .expect("changed registry branch should remain loaded")
                    .write_row(&branch_id)?,
            );
        }

        // The dominant no-plugin path ends here. In particular, it does not
        // inspect descriptors or owners left behind by an uninstall.
        let active_branch_ids = branch_ids
            .iter()
            .filter(|branch_id| {
                registries
                    .get(*branch_id)
                    .is_some_and(|registry| !registry.is_empty())
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        if active_branch_ids.is_empty() && deleted_file_keys.is_empty() {
            for write in file_data.iter().filter(|write| {
                !write.global
                    && !write.untracked
                    && write
                        .path
                        .as_deref()
                        .is_some_and(|path| !is_plugin_storage_path(path))
            }) {
                reconciliation.remove_session_file_view(SessionFileViewKey::new(
                    &write.branch_id,
                    &write.file_id,
                ));
            }
            mark_plugin_reconciliation_batch(rows, input_row_count)?;
            return Ok(reconciliation);
        }

        let mut candidate_file_keys = BTreeSet::<PluginFileWriteKey>::new();
        for write in file_data.iter() {
            if write.global
                || write.untracked
                || !active_branch_ids.contains(&write.branch_id)
                || write.path.as_deref().is_none_or(is_plugin_storage_path)
            {
                continue;
            }
            candidate_file_keys.insert(PluginFileWriteKey::from(write));
        }
        for key in deleted_file_keys.keys() {
            candidate_file_keys.insert(key.clone());
        }
        for row in rows.iter().take(input_row_count) {
            if row.global
                || row.untracked
                || !active_branch_ids.contains(row.branch_id.as_str())
                || row.file_id.is_none()
            {
                continue;
            }
            candidate_file_keys.insert(PluginFileWriteKey {
                branch_id: row.branch_id.to_string(),
                global: false,
                untracked: false,
                file_id: row
                    .file_id
                    .as_ref()
                    .map(ToString::to_string)
                    .expect("candidate semantic row has a file id"),
            });
        }
        if candidate_file_keys.is_empty() {
            mark_plugin_reconciliation_batch(rows, input_row_count)?;
            return Ok(reconciliation);
        }

        let owner_rows = overlay_load_exact_batch(
            &base,
            &staged,
            &LiveStateExactBatchRequest {
                rows: candidate_file_keys
                    .iter()
                    .map(|key| LiveStateExactRowRequest {
                        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                        branch_id: key.branch_id.clone(),
                        entity_pk: EntityPk::single(PLUGIN_OWNER_KEY),
                        file_id: Some(key.file_id.clone()),
                    })
                    .collect(),
                projection: plugin_registry_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            },
        )
        .await?;
        let mut owners = BTreeMap::<PluginFileWriteKey, PluginFileOwner>::new();
        let mut owner_change_ids = BTreeMap::<PluginFileWriteKey, String>::new();
        for row in (0..owner_rows.len()).filter_map(|slot| owner_rows.row(slot)) {
            let branch_id = row.branch_id().to_string();
            let owner_row = row.to_owned();
            let Some(owner) = PluginFileOwner::from_live_state_row(&owner_row, &branch_id)? else {
                continue;
            };
            let owner_change_id = row.change_id().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "durable plugin owner for file '{}' on branch '{branch_id}' is missing change_id",
                        owner.file_id()
                    ),
                )
            })?;
            let key = PluginFileWriteKey {
                branch_id,
                global: false,
                untracked: false,
                file_id: owner.file_id().to_string(),
            };
            if owners.insert(key.clone(), owner).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "durable plugin owner lookup returned duplicate file rows",
                ));
            }
            owner_change_ids.insert(key, owner_change_id.to_string());
        }

        let mut catalogs = BTreeMap::<String, Arc<CompiledPluginCatalog>>::new();
        for branch_id in &active_branch_ids {
            let registry = registries
                .get(branch_id)
                .expect("active branch should have a registry");
            catalogs.insert(
                branch_id.clone(),
                self.plugin_host.compiled_plugin_catalog(registry)?,
            );
        }

        let file_data_keys = file_data
            .iter()
            .map(PluginFileWriteKey::from)
            .collect::<BTreeSet<_>>();
        let mut unresolved_semantic_groups =
            BTreeMap::<PluginFileWriteKey, (PluginRegistryEntry, String, Vec<usize>)>::new();
        for (row_index, row) in rows.iter().take(input_row_count).enumerate() {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            if row.global || row.untracked || !active_branch_ids.contains(row.branch_id.as_str()) {
                continue;
            }
            let registry = registries
                .get(row.branch_id.as_str())
                .expect("active semantic-write branch has a registry");
            let schema_is_plugin_owned = registry.plugins().iter().any(|plugin| {
                plugin
                    .schema_keys()
                    .binary_search_by(|key| key.as_str().cmp(row.schema_key.as_str()))
                    .is_ok()
            });
            if !schema_is_plugin_owned {
                continue;
            }
            let file_key = PluginFileWriteKey {
                branch_id: row.branch_id.to_string(),
                global: false,
                untracked: false,
                file_id: file_id.to_string(),
            };
            let owner = owners.get(&file_key).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "plugin-owned schema '{}' cannot be written for unowned file '{}'",
                        row.schema_key, file_id
                    ),
                )
            })?;
            let plugin = registry.plugin(owner.plugin_key()).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "file '{}' names unavailable plugin owner '{}'",
                        file_id,
                        owner.plugin_key()
                    ),
                )
            })?;
            if plugin
                .schema_keys()
                .binary_search_by(|key| key.as_str().cmp(row.schema_key.as_str()))
                .is_err()
                || owner
                    .schema_keys()
                    .binary_search_by(|key| key.as_str().cmp(row.schema_key.as_str()))
                    .is_err()
            {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "schema '{}' is not owned by file '{}' plugin '{}'",
                        row.schema_key,
                        file_id,
                        plugin.key()
                    ),
                ));
            }
            if file_data_keys.contains(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot mutate both bytes and semantic entities for v2 plugin file '{file_id}'"
                    ),
                )
                .with_hint("submit either the byte mutation or the resolved entity mutations"));
            }
            if deleted_file_keys.contains_key(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot delete v2 plugin file '{file_id}' and mutate its semantic entities"
                    ),
                ));
            }
            let owner_change_id = owner_change_ids.get(&file_key).cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "durable v2 plugin owner for file '{file_id}' is missing its incarnation"
                    ),
                )
            })?;
            let group = unresolved_semantic_groups
                .entry(file_key)
                .or_insert_with(|| (plugin.clone(), owner_change_id.clone(), Vec::new()));
            if group.0.key() != plugin.key() || group.1 != owner_change_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "semantic writes for file '{file_id}' resolve to ambiguous plugin ownership"
                    ),
                ));
            }
            group.2.push(row_index);
        }

        let mut semantic_groups = BTreeMap::<PluginFileWriteKey, PluginV2SemanticWriteGroup>::new();
        if !unresolved_semantic_groups.is_empty() {
            let request = FilesystemPathIndexRequest::new(
                unresolved_semantic_groups
                    .keys()
                    .map(|key| key.branch_id.clone())
                    .collect(),
            );
            let path_index = self.filesystem_path_index(&request).await?;
            for (file_key, (plugin, owner_change_id, row_indices)) in unresolved_semantic_groups {
                let entries = path_index
                    .exact_file_id_entries(&file_key.file_id)
                    .into_iter()
                    .filter(|entry| {
                        let live = entry.live_row();
                        entry.kind == FilesystemPathKind::File
                            && entry.id() == file_key.file_id
                            && live.branch_id.as_ref() == file_key.branch_id
                            && !live.global
                            && !live.untracked
                    })
                    .collect::<Vec<_>>();
                let [entry] = entries.as_slice() else {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "owned v2 plugin file '{}' must resolve to exactly one tracked path; found {}",
                            file_key.file_id,
                            entries.len()
                        ),
                    ));
                };
                let catalog = catalogs
                    .get(&file_key.branch_id)
                    .expect("semantic-write branch has a compiled plugin catalog");
                if !catalog.matches_plugin(plugin.key(), &entry.path) {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "owned v2 plugin '{}' no longer matches file path '{}'",
                            plugin.key(),
                            entry.path
                        ),
                    ));
                }
                semantic_groups.insert(
                    file_key,
                    PluginV2SemanticWriteGroup {
                        plugin,
                        path: entry.path.clone(),
                        filename: entry.name.clone(),
                        owner_change_id,
                        row_indices,
                    },
                );
            }
        }

        let mut selected_plugins = BTreeMap::<PluginFileWriteKey, PluginRegistryEntry>::new();
        let mut content_classification_bytes = BTreeMap::<PluginFileWriteKey, u64>::new();
        let selection_span = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.plugin_selection"
        );
        let selection_guard = selection_span.enter();
        for write in file_data.iter() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            if write.global
                || write.untracked
                || is_plugin_storage_path(path)
                || !active_branch_ids.contains(&write.branch_id)
            {
                continue;
            }
            let file_key = PluginFileWriteKey::from(write);
            let catalog = catalogs
                .get(&write.branch_id)
                .expect("active plugin branch should have a compiled catalog");
            let registry = registries
                .get(&write.branch_id)
                .expect("active plugin branch should have a registry");

            // A warm v2 actor already carries an exact, generation-bound
            // selection. Reuse it only while every matcher-relevant identity
            // is unchanged. UTF-8 and Git-text constraints have separate
            // bounded trusted-splice proofs; all inconclusive, binary, blind,
            // cold, or path-reselected writes use the ordinary classifier below.
            let warm_owned_plugin = owners.get(&file_key).and_then(|owner| {
                let plugin = registry.plugin(owner.plugin_key())?;
                if !catalog.matches_plugin(plugin.key(), path) {
                    return None;
                }
                let owner_change_id = owner_change_ids.get(&file_key)?;
                let session_key = SessionFileViewKey::new(&write.branch_id, &write.file_id);
                let observation = self.acknowledged_session_plugin_observation(
                    &session_key,
                    plugin,
                    owner_change_id,
                )?;
                if observation.key().path != path {
                    return None;
                }
                let content_type_still_matches = match plugin.content_type() {
                    None => true,
                    Some(PluginContentType::Text) => {
                        write.splice_provenance().is_some_and(|provenance| {
                            observation.bytes_sha256().is_some_and(|digest| {
                                digest.matches_lower_hex(provenance.base_sha256())
                            }) && transport_splice_preserves_utf8(write.data(), provenance)
                        })
                    }
                    Some(PluginContentType::GitText) => {
                        write.splice_provenance().is_some_and(|provenance| {
                            observation.bytes_sha256().is_some_and(|digest| {
                                digest.matches_lower_hex(provenance.base_sha256())
                            }) && transport_splice_preserves_git_text(write.data(), provenance)
                        })
                    }
                    Some(PluginContentType::Binary) => false,
                };
                content_type_still_matches.then_some(plugin)
            });

            let (plugin, classified_bytes) = warm_owned_plugin.map_or_else(
                || catalog.select_for_bytes_with_classification_work(path, write.data()),
                |plugin| (Some(plugin), 0),
            );
            if classified_bytes != 0 {
                content_classification_bytes.insert(file_key.clone(), classified_bytes);
            }
            let Some(plugin) = plugin else {
                continue;
            };
            selected_plugins.insert(file_key, plugin.clone());
        }
        drop(selection_guard);

        let mut state_groups = BTreeMap::<PluginStateGroupKey, PluginStateGroup>::new();
        for (key, owner) in &owners {
            // Descriptor deletion cascades file-scoped current state in the
            // head materializer. Avoid hydrating the full plugin graph only
            // to persist one historical tombstone per semantic entity.
            if deleted_file_keys.contains_key(key) {
                continue;
            }
            let selected = selected_plugins.get(key);
            let semantic = semantic_groups.get(key).map(|group| &group.plugin);
            // A same-owner v2 write is authorized by an exact document
            // observation and must not hydrate the complete durable graph.
            // Lifecycle removal/reselection still needs the old rows so it
            // can tombstone every schema owned by the previous plugin.
            if selected
                .or(semantic)
                .is_some_and(|selected| selected.key() == owner.plugin_key())
            {
                continue;
            }
            let group_key = PluginStateGroupKey {
                branch_id: key.branch_id.clone(),
                plugin_key: owner.plugin_key().to_string(),
            };
            let group = state_groups.entry(group_key).or_default();
            group.file_ids.insert(key.file_id.clone());
            group
                .schema_keys
                .extend(owner.schema_keys().iter().cloned());
            if let Some(selected) = selected.or(semantic)
                && selected.key() == owner.plugin_key()
            {
                group
                    .schema_keys
                    .extend(selected.schema_keys().iter().cloned());
            }
        }
        let mut state_batches =
            Vec::<MaterializedLiveStateBatch>::with_capacity(state_groups.len());
        let mut state_group_keys = Vec::<PluginStateGroupKey>::with_capacity(state_groups.len());
        for (group_key, group) in state_groups {
            let rows = overlay_scan_batch(
                &base,
                &staged,
                &LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: group.schema_keys.into_iter().collect(),
                        branch_ids: vec![group_key.branch_id.clone()],
                        file_ids: group
                            .file_ids
                            .iter()
                            .cloned()
                            .map(NullableKeyFilter::Value)
                            .collect(),
                        untracked: Some(false),
                        ..Default::default()
                    },
                    projection: plugin_state_live_state_projection(),
                    ..Default::default()
                },
            )
            .await?;
            u32::try_from(state_batches.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin reconciliation state batch count exceeds u32 ordinals",
                )
            })?;
            u32::try_from(rows.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin reconciliation state row count exceeds u32 ordinals",
                )
            })?;
            state_group_keys.push(group_key);
            state_batches.push(rows);
        }
        let state_row_count = state_batches
            .iter()
            .map(MaterializedLiveStateBatch::len)
            .sum();
        let mut state_rows = Vec::<PluginStateBatchRow>::with_capacity(state_row_count);
        for (batch_index, batch) in state_batches.iter().enumerate() {
            let batch_index =
                u32::try_from(batch_index).expect("plugin state batch count was checked");
            for (row_index, row) in batch.iter().enumerate() {
                if row.file_id().is_some() {
                    state_rows.push(PluginStateBatchRow {
                        batch_index,
                        row_index: u32::try_from(row_index)
                            .expect("plugin state batch row count was checked"),
                    });
                }
            }
        }
        state_rows.sort_unstable_by(|left, right| {
            left.batch_index.cmp(&right.batch_index).then_with(|| {
                let left = state_batches[left.batch_index as usize]
                    .row(left.row_index as usize)
                    .file_id()
                    .expect("selected plugin state row carries file_id");
                let right = state_batches[right.batch_index as usize]
                    .row(right.row_index as usize)
                    .file_id()
                    .expect("selected plugin state row carries file_id");
                left.cmp(right)
            })
        });
        let mut state_by_file =
            std::iter::repeat_with(BTreeMap::<String, std::ops::Range<usize>>::new)
                .take(state_group_keys.len())
                .collect::<Vec<_>>();
        let mut selection_start = 0;
        while selection_start < state_rows.len() {
            let selected = state_rows[selection_start];
            let file_id = state_batches[selected.batch_index as usize]
                .row(selected.row_index as usize)
                .file_id()
                .expect("selected plugin state row carries file_id");
            let mut selection_end = selection_start + 1;
            while selection_end < state_rows.len() {
                let candidate = state_rows[selection_end];
                if candidate.batch_index != selected.batch_index
                    || state_batches[candidate.batch_index as usize]
                        .row(candidate.row_index as usize)
                        .file_id()
                        != Some(file_id)
                {
                    break;
                }
                selection_end += 1;
            }
            state_by_file[selected.batch_index as usize]
                .insert(file_id.to_owned(), selection_start..selection_end);
            selection_start = selection_end;
        }

        let mut selected_entries = BTreeMap::<PluginBranchEntryKey, PluginRegistryEntry>::new();
        for (file_key, entry) in &selected_plugins {
            selected_entries
                .entry(PluginBranchEntryKey {
                    branch_id: file_key.branch_id.clone(),
                    plugin_key: entry.key().to_string(),
                })
                .or_insert_with(|| entry.clone());
        }
        for (file_key, group) in &semantic_groups {
            selected_entries
                .entry(PluginBranchEntryKey {
                    branch_id: file_key.branch_id.clone(),
                    plugin_key: group.plugin.key().to_string(),
                })
                .or_insert_with(|| group.plugin.clone());
        }

        // Resolve warm factories by their fixed content hash before asking the
        // CAS for bytes. The factory can then instantiate one isolated actor
        // per file without recompiling the component.
        let mut component_v2_factories =
            BTreeMap::<PluginBranchEntryKey, Arc<dyn WasmComponentV2Factory>>::new();
        let mut cold_v2_entries = BTreeMap::<PluginBranchEntryKey, PluginRegistryEntry>::new();
        for (key, entry) in selected_entries {
            let hash = BlobHash::from_hex(entry.wasm_blob_hash())?;
            let cached_factory = self
                .plugin_host
                .cached_plugin_v2_factory(entry.key(), hash)?;
            if let Some(factory) = cached_factory {
                component_v2_factories.insert(key, factory);
            } else {
                cold_v2_entries.insert(key, entry);
            }
        }

        let mut wasm_by_hash = current_install_wasm;
        let mut missing_hashes = Vec::<BlobHash>::new();
        for entry in cold_v2_entries.values() {
            let hash = BlobHash::from_hex(entry.wasm_blob_hash())?;
            if !wasm_by_hash.contains_key(&hash) && !missing_hashes.contains(&hash) {
                missing_hashes.push(hash);
            }
        }
        if !missing_hashes.is_empty() {
            let base_blob_reader = self.binary_cas.reader(read.clone());
            let loaded = load_transaction_blob_bytes(
                &base_blob_reader,
                &self.staged_writes,
                &missing_hashes,
            )
            .await?
            .into_vec();
            for (hash, bytes) in missing_hashes.into_iter().zip(loaded) {
                let bytes = bytes.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "plugin registry references missing WASM blob '{}'",
                            hash.to_hex()
                        ),
                    )
                })?;
                wasm_by_hash.insert(hash, bytes);
            }
        }
        for (key, entry) in cold_v2_entries {
            let hash = BlobHash::from_hex(entry.wasm_blob_hash())?;
            let wasm = wasm_by_hash.get(&hash).cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "plugin registry references unavailable WASM blob '{}'",
                        hash.to_hex()
                    ),
                )
            })?;
            let plugin = entry.to_installed_plugin(wasm)?;
            let factory = self
                .plugin_host
                .load_or_compile_v2_factory(&plugin)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_factory_compile"
                ))
                .await?;
            component_v2_factories.insert(key, factory);
        }

        let mut reconciled_file_keys = BTreeSet::<PluginFileWriteKey>::new();
        let fresh_file_indices = file_data
            .iter()
            .enumerate()
            .filter_map(|(index, write)| {
                let path = write.path.as_deref()?;
                if write.global
                    || write.untracked
                    || is_plugin_storage_path(path)
                    || !active_branch_ids.contains(&write.branch_id)
                {
                    return None;
                }
                let file_key = PluginFileWriteKey::from(write);
                (!owners.contains_key(&file_key) && selected_plugins.contains_key(&file_key))
                    .then_some(index)
            })
            .collect::<Vec<_>>();

        let fresh_parallelism = self.plugin_host.max_live_plugin_stores();
        for file_indices in fresh_file_indices.chunks(fresh_parallelism) {
            let mut prepared_opens = Vec::with_capacity(file_indices.len());
            let mut prepared_session_keys = BTreeSet::new();
            for &file_index in file_indices {
                let write = &file_data[file_index];
                let path = write
                    .path
                    .as_deref()
                    .expect("fresh plugin candidate has a path")
                    .to_string();
                let file_key = PluginFileWriteKey::from(write);
                let selected = selected_plugins
                    .get(&file_key)
                    .expect("fresh plugin candidate has a selection")
                    .clone();
                let installed_key = PluginBranchEntryKey {
                    branch_id: write.branch_id.clone(),
                    plugin_key: selected.key().to_string(),
                };
                let factory = component_v2_factories
                    .get(&installed_key)
                    .expect("selected v2 plugin should have a compiled factory")
                    .clone();
                let desired_owner =
                    PluginFileOwner::from_registry_entry(write.file_id.clone(), &selected)?;
                let owner_change_id = self.functions.call_uuid_v7().to_string();
                let mut owner_row = desired_owner.write_row(&write.branch_id)?;
                owner_row.change_id = Some(owner_change_id.clone());
                let actor_key = PluginActorKey {
                    branch_id: write.branch_id.clone(),
                    file_id: write.file_id.clone(),
                    path,
                    owner_change_id: owner_change_id.clone(),
                    plugin_key: selected.key().to_string(),
                    plugin_generation: selected.archive_blob_hash().to_string(),
                };
                let view = PendingPluginActorView {
                    session_key: SessionFileViewKey::new(&write.branch_id, &write.file_id),
                    plugin_key: selected.key().to_string(),
                    plugin_generation: selected.archive_blob_hash().to_string(),
                    owner_change_id,
                    semantic_chainable: false,
                    retain_large_import_actor: selected.api_version()
                        != crate::wasm::WASM_COMPONENT_V3_PROTOTYPE_API_VERSION,
                };
                if !prepared_session_keys.insert(view.session_key.clone())
                    || self
                        .pending_plugin_actor_publications
                        .iter()
                        .chain(reconciliation.actor_publications.iter())
                        .any(|publication| publication.session_key() == &view.session_key)
                {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "one transaction cannot transition v2 plugin file '{}' more than once",
                            write.file_id
                        ),
                    )
                    .with_hint("combine the byte edits into one file update"));
                }

                let descriptor = v2_file_descriptor(write, &selected);
                let schemas = V2SchemaAllowlist::from_catalog(
                    selected.schema_keys(),
                    Arc::clone(&self.sql_schema_snapshot),
                )?;
                let mutation_identity = write.mutation_identity().unwrap_or_else(|| {
                    local_mutation_identity(self.functions.call_uuid_v7().into_bytes())
                });
                let create_context = BoundCreateContext::bind(mutation_identity, &actor_key)?;
                let materialization_version = self.functions.call_uuid_v7().to_string();
                let submitted_bytes = write.payload().shared_bytes();
                let cold_limits = WasmTransitionLimits::for_cold_file_bytes(
                    u64::try_from(submitted_bytes.len()).unwrap_or(u64::MAX),
                );
                prepared_opens.push(PreparedFreshPluginOpen {
                    file_index,
                    file_key,
                    selected,
                    owner_row,
                    actor_key,
                    view,
                    materialization_version,
                    submitted_bytes,
                    create_context,
                    existing_create_reservation: None,
                    factory,
                    descriptor,
                    schemas,
                    cold_limits,
                });
            }

            let preflight_requests = prepared_opens
                .iter()
                .map(|prepared| (prepared.create_context, prepared.file_key.clone()))
                .collect::<Vec<_>>();
            let existing_rows = match self.preflight_v2_creates(&preflight_requests).await {
                Ok(existing_rows) => existing_rows,
                Err(error) => {
                    discard_plugin_actor_publications(std::mem::take(
                        &mut reconciliation.actor_publications,
                    ))
                    .await;
                    return Err(error);
                }
            };
            for (prepared, existing) in prepared_opens.iter_mut().zip(existing_rows) {
                prepared.existing_create_reservation = existing;
            }

            let mut store_permits = Vec::with_capacity(prepared_opens.len());
            for _ in 0..prepared_opens.len() {
                match self
                    .admit_fresh_plugin_store(&mut reconciliation.actor_publications)
                    .await
                {
                    Ok(permit) => store_permits.push(permit),
                    Err(error) => {
                        discard_plugin_actor_publications(std::mem::take(
                            &mut reconciliation.actor_publications,
                        ))
                        .await;
                        return Err(error);
                    }
                }
            }

            let pending_opens = prepared_opens
                .into_iter()
                .zip(store_permits)
                .map(|(prepared, store_permit)| {
                    let PreparedFreshPluginOpen {
                        file_index,
                        file_key,
                        selected,
                        owner_row,
                        actor_key,
                        view,
                        materialization_version,
                        submitted_bytes,
                        create_context,
                        existing_create_reservation,
                        factory,
                        descriptor,
                        schemas,
                        cold_limits,
                    } = prepared;
                    let source_bytes = submitted_bytes.clone();
                    let creates = create_context.creates();
                    let task = tokio::spawn(async move {
                        let mut actor = factory
                            .instantiate_actor()
                            .instrument(tracing::debug_span!(
                                target: "lix_perf",
                                "lix.perf.plugin_actor_instantiate"
                            ))
                            .await?;
                        let transition = actor
                            .open_file(
                                cold_limits,
                                WasmOpenFileInput {
                                    descriptor,
                                    file: Arc::new(ArcByteSource::new(source_bytes)),
                                    creates,
                                },
                            )
                            .instrument(tracing::debug_span!(
                                target: "lix_perf",
                                "lix.perf.plugin_open_file"
                            ))
                            .await?;
                        let validated = drain_file_transition_changes(
                            actor.as_mut(),
                            transition,
                            &schemas,
                            cold_limits,
                        )
                        .instrument(tracing::debug_span!(
                            target: "lix_perf",
                            "lix.perf.plugin_open_file_drain"
                        ))
                        .await?;
                        Ok((actor, validated))
                    });
                    PendingFreshPluginOpen {
                        file_index,
                        file_key,
                        selected,
                        owner_row,
                        actor_key,
                        view,
                        materialization_version,
                        submitted_bytes,
                        create_context,
                        existing_create_reservation,
                        store_permit,
                        task: Some(task),
                    }
                })
                .collect::<Vec<_>>();

            let mut completed_opens = Vec::with_capacity(pending_opens.len());
            let mut first_error = None;
            for mut pending in pending_opens {
                let result = pending
                    .task
                    .take()
                    .expect("fresh plugin worker task is present")
                    .await;
                match result {
                    Ok(Ok((actor, validated))) => {
                        completed_opens.push((pending, actor, validated));
                    }
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("fresh plugin worker failed: {error}"),
                            )
                        });
                    }
                }
            }
            if let Some(error) = first_error {
                discard_plugin_actor_publications(std::mem::take(
                    &mut reconciliation.actor_publications,
                ))
                .await;
                return Err(error);
            }

            for (pending, actor, validated) in completed_opens {
                let certified_row_count = validated
                    .certified_batches
                    .iter()
                    .map(|batch| batch.row_count)
                    .sum::<u64>();
                file_data[pending.file_index]
                    .set_certified_entity_batches(validated.certified_batches);
                let mut changes = validated.changes;
                let create_rows = self
                    .v2_create_rows(
                        &pending.selected,
                        &mut changes,
                        pending.create_context,
                        &pending.file_key,
                        pending.existing_create_reservation.as_ref(),
                        None,
                    )
                    .await?;
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&pending.file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes = u64::try_from(changes.entity_change_count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(certified_row_count);
                self.plugin_host.record_v2_transition_counters(counters);

                rows.push(pending.owner_row);
                rows.append(create_rows);
                let write = &mut file_data[pending.file_index];
                let context = FilesystemRowContext {
                    branch_id: write.branch_id.clone(),
                    global: false,
                    untracked: false,
                    file_id: None,
                    metadata: None,
                };
                append_plugin_change_rows_from_v2(
                    rows,
                    &pending.selected,
                    changes,
                    &write.file_id,
                    &context,
                )?;
                match pending.selected.materialization() {
                    PluginMaterialization::Blob => {
                        reconciliation
                            .materialized_file_keys
                            .insert(pending.file_key.clone());
                    }
                    PluginMaterialization::Derived => {
                        reconciliation.derived_materializations.insert(
                            pending.file_key.clone(),
                            DerivedMaterializationProof::from_bytes(
                                &pending.submitted_bytes,
                                pending.actor_key.path.clone(),
                            ),
                        );
                    }
                }
                reconciliation.materialization_versions.insert(
                    pending.file_key.clone(),
                    pending.materialization_version.clone(),
                );
                reconciliation
                    .actor_publications
                    .push(PendingPluginActorPublication::New {
                        cache: self.plugin_host.actor_cache(),
                        key: pending.actor_key,
                        store: PluginActorStore::new(actor, pending.store_permit),
                        document: validated.document,
                        bytes: pending.submitted_bytes,
                        semantic_root: Arc::from(pending.materialization_version),
                        view: pending.view,
                    });
                reconciled_file_keys.insert(pending.file_key);
            }
        }

        for write in file_data.iter_mut() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            if write.global
                || write.untracked
                || is_plugin_storage_path(path)
                || !active_branch_ids.contains(&write.branch_id)
            {
                continue;
            }
            let file_key = PluginFileWriteKey::from(&*write);
            if reconciled_file_keys.contains(&file_key) {
                continue;
            }
            let owner = owners.get(&file_key);
            let selected = selected_plugins.get(&file_key);
            let context = FilesystemRowContext {
                branch_id: write.branch_id.clone(),
                global: false,
                untracked: false,
                file_id: None,
                metadata: None,
            };
            let old_state = owner
                .and_then(|owner| {
                    let group_index = state_group_keys
                        .binary_search_by(|group| {
                            group
                                .branch_id
                                .as_str()
                                .cmp(write.branch_id.as_str())
                                .then_with(|| group.plugin_key.as_str().cmp(owner.plugin_key()))
                        })
                        .ok()?;
                    state_by_file[group_index].get(write.file_id.as_str())
                })
                .map(|range| &state_rows[range.clone()])
                .unwrap_or_default();

            if owner.is_some_and(|owner| {
                selected.is_none_or(|selected| selected.key() != owner.plugin_key())
            }) {
                rows.append(plugin_state_tombstone_batch(
                    old_state,
                    &state_batches,
                    &write.file_id,
                    &context,
                ));
                rows.append(self.v2_id_reservation_tombstones(&file_key).await?);
            }

            let Some(selected) = selected else {
                reconciliation.remove_session_file_view(SessionFileViewKey::new(
                    &write.branch_id,
                    &write.file_id,
                ));
                if owner.is_some() {
                    rows.push(PluginFileOwner::delete_row(
                        write.file_id.clone(),
                        &write.branch_id,
                    )?);
                }
                reconciled_file_keys.insert(file_key);
                continue;
            };
            let installed_key = PluginBranchEntryKey {
                branch_id: write.branch_id.clone(),
                plugin_key: selected.key().to_string(),
            };
            let factory = component_v2_factories
                .get(&installed_key)
                .expect("selected v2 plugin should have a compiled factory")
                .clone();
            let same_plugin_owner = owner.is_some_and(|owner| owner.plugin_key() == selected.key());
            let session_key = SessionFileViewKey::new(&write.branch_id, &write.file_id);
            let current_owner_change_id = same_plugin_owner
                .then(|| owner_change_ids.get(&file_key).cloned())
                .flatten();
            let desired_owner =
                PluginFileOwner::from_registry_entry(write.file_id.clone(), selected)?;
            let owner_needs_write = plugin_owner_needs_write(owner, &desired_owner);
            let owner_change_id = if owner_needs_write {
                let owner_change_id = self.functions.call_uuid_v7().to_string();
                let mut owner_row = desired_owner.write_row(&write.branch_id)?;
                owner_row.change_id = Some(owner_change_id.clone());
                rows.push(owner_row);
                owner_change_id
            } else {
                current_owner_change_id.clone().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "durable v2 plugin owner for file '{}' is missing its incarnation",
                            write.file_id
                        ),
                    )
                })?
            };
            let path = path.to_string();
            let actor_key = PluginActorKey {
                branch_id: write.branch_id.clone(),
                file_id: write.file_id.clone(),
                path: path.clone(),
                owner_change_id: owner_change_id.clone(),
                plugin_key: selected.key().to_string(),
                plugin_generation: selected.archive_blob_hash().to_string(),
            };
            let view = PendingPluginActorView {
                session_key,
                plugin_key: selected.key().to_string(),
                plugin_generation: selected.archive_blob_hash().to_string(),
                owner_change_id,
                semantic_chainable: false,
                retain_large_import_actor: selected.api_version()
                    != crate::wasm::WASM_COMPONENT_V3_PROTOTYPE_API_VERSION,
            };
            if self
                .pending_plugin_actor_publications
                .iter()
                .chain(reconciliation.actor_publications.iter())
                .any(|publication| publication.session_key() == &view.session_key)
            {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one transaction cannot transition v2 plugin file '{}' more than once",
                        write.file_id
                    ),
                )
                .with_hint("combine the byte edits into one file update"));
            }
            let descriptor = v2_file_descriptor(write, selected);
            let limits = WasmTransitionLimits::default();
            let schemas = V2SchemaAllowlist::from_catalog(
                selected.schema_keys(),
                Arc::clone(&self.sql_schema_snapshot),
            )?;
            let mutation_identity = write.mutation_identity().unwrap_or_else(|| {
                local_mutation_identity(self.functions.call_uuid_v7().into_bytes())
            });
            let create_context = BoundCreateContext::bind(mutation_identity, &actor_key)?;
            let creates = create_context.creates();
            let existing_create_reservation =
                match self.preflight_v2_create(create_context, &file_key).await {
                    Ok(existing) => existing,
                    Err(error) => {
                        discard_plugin_actor_publications(std::mem::take(
                            &mut reconciliation.actor_publications,
                        ))
                        .await;
                        return Err(error);
                    }
                };
            let materialization_version = self.functions.call_uuid_v7().to_string();
            let submitted_bytes = write.payload().shared_bytes();
            let mut verified_same_length_blob_splice = None;

            let (changes, publication, materialized_bytes, create_rows) = if same_plugin_owner {
                let acknowledged_view = self.acknowledged_session_plugin_view(
                    &view.session_key,
                    selected,
                    current_owner_change_id
                        .as_deref()
                        .expect("same-owner v2 file should have an owner incarnation"),
                );
                let observation = match acknowledged_view {
                    Some(view) => match view.observation {
                        Some(observation) => observation,
                        None => {
                            self.cold_open_v2_semantic_actor(
                                &actor_key,
                                selected,
                                descriptor.clone(),
                                Arc::clone(&factory),
                                &mut reconciliation.actor_publications,
                            )
                            .await?
                        }
                    },
                    None if self
                        .pending_file_view_mutations
                        .contains_key(&view.session_key)
                        || self
                            .session_file_views
                            .has_plugin_file_at_path(&actor_key.branch_id, &actor_key.path) =>
                    {
                        return Err(LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "the acknowledged v2 file identity no longer matches this write",
                        )
                        .with_hint("read the exact file bytes again before retrying the edit"));
                    }
                    None => {
                        self.cold_open_v2_semantic_actor(
                            &actor_key,
                            selected,
                            descriptor.clone(),
                            Arc::clone(&factory),
                            &mut reconciliation.actor_publications,
                        )
                        .await?
                    }
                };
                if !v2_actor_key_is_descriptor_successor(observation.key(), &actor_key) {
                    return Err(LixError::new(
                        LixError::CODE_PLUGIN_OBSERVATION_STALE,
                        "the acknowledged v2 file identity no longer matches this write",
                    )
                    .with_hint("read the exact file bytes again before retrying the edit"));
                }
                let before_descriptor = v2_file_descriptor_from_actor_key(observation.key());
                let after_descriptor = descriptor.clone();
                // Acquire serialization first, reopening a benignly evicted
                // observation only while its exact durable root is unchanged.
                // Then read the root again: a second local session may have
                // committed while this request waited for the actor.
                let mut lease = self
                    .lease_or_reopen_observed_v2_actor(
                        &observation,
                        &actor_key,
                        selected,
                        descriptor.clone(),
                        Arc::clone(&factory),
                        &mut reconciliation.actor_publications,
                    )
                    .await?;
                let visible_materialization = self
                    .visible_v2_materialization(&file_key)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "the acknowledged v2 file no longer has a visible materialization root",
                        )
                        .with_hint("read the exact file bytes again before retrying the edit")
                    })?;
                lease.require_accepted_semantic_root(&visible_materialization.semantic_root)?;
                let observation_is_current =
                    observation.semantic_root() == visible_materialization.semantic_root;
                let observed_bytes = lease.observed_bytes();
                let built_splices = tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_splice_discovery"
                )
                .in_scope(|| {
                    build_file_update_splices(
                        &observed_bytes,
                        lease.observed_bytes_sha256(),
                        write.data(),
                        write.splice_provenance(),
                        limits,
                    )
                })?;
                let submitted_bytes_sha256 = built_splices.after_sha256;
                let host_full_diff_bytes_compared = built_splices.full_diff_bytes_compared;
                let same_length_blob_splice = built_splices.same_length_replacement();
                let observed_source = ArcByteSource::new(observed_bytes.clone());
                let submitted_source = ArcByteSource::new(submitted_bytes.clone());
                let observed_document = lease.observed_document();
                lease.begin_guest_call()?;
                let detection_input = match lease.actor_mut().fork_document(observed_document).await
                {
                    Ok(document) => document,
                    Err(error) => return Err(lease.handle_guest_call_error(error)),
                };
                let detection_transition = match lease
                    .actor_mut()
                    .file_changed(
                        detection_input,
                        limits,
                        WasmFileUpdate {
                            before_descriptor: before_descriptor.clone(),
                            after_descriptor: after_descriptor.clone(),
                            before: Arc::new(observed_source),
                            edits: built_splices.edits,
                            after: Arc::new(submitted_source),
                            creates,
                        },
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_file_changed"
                    ))
                    .await
                {
                    Ok(transition) => transition,
                    Err(error) => return Err(lease.handle_guest_call_error(error)),
                };
                let detected_transition = match drain_file_transition_changes(
                    lease.actor_mut(),
                    detection_transition,
                    &schemas,
                    limits,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_drain_changes"
                ))
                .await
                {
                    Ok(transition) => transition,
                    Err(error) => return Err(lease.handle_guest_call_error(error)),
                };
                if let Err(error) = lease.actor_mut().drop_document(detection_input).await {
                    return Err(lease.handle_guest_call_error(error));
                }

                let certified_row_count = detected_transition
                    .certified_batches
                    .iter()
                    .map(|batch| batch.row_count)
                    .sum::<u64>();
                write.set_certified_entity_batches(detected_transition.certified_batches.clone());
                let detection_document = detected_transition.document;
                let mut counters = detected_transition.counters;
                let (mut changes, observed_existing_authorities) = match self
                    .suppress_v2_format_only_noops(selected, detected_transition.changes, &file_key)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_suppress_noops"
                    ))
                    .await
                {
                    Ok(changes) => changes,
                    Err(error) => {
                        if let Err(cleanup_error) =
                            lease.actor_mut().drop_document(detection_document).await
                        {
                            return Err(lease.handle_guest_call_error(cleanup_error));
                        }
                        return Err(lease.handle_guest_call_error(error));
                    }
                };
                let create_rows = match self
                    .v2_create_rows(
                        selected,
                        &mut changes,
                        create_context,
                        &file_key,
                        existing_create_reservation.as_ref(),
                        Some(&observed_existing_authorities),
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_create_rows"
                    ))
                    .await
                {
                    Ok(rows) => rows,
                    Err(error) => {
                        if let Err(cleanup_error) =
                            lease.actor_mut().drop_document(detection_document).await
                        {
                            return Err(lease.handle_guest_call_error(cleanup_error));
                        }
                        return Err(lease.handle_guest_call_error(error));
                    }
                };
                let (successor_document, materialized_bytes, materialized_bytes_sha256) =
                    if observation_is_current {
                        // The actor lease serializes this file and the durable
                        // root still equals the acknowledged observation. The
                        // validated file successor is therefore already the
                        // exact merge result; rendering the same sparse change
                        // onto the same base would only repeat guest work.
                        verified_same_length_blob_splice = match visible_materialization.bytes {
                            VisibleV2MaterializationBytes::Blob { hash } => same_length_blob_splice
                                .map(|(offset, length)| (hash, offset, length)),
                            VisibleV2MaterializationBytes::Derived { .. } => None,
                        };
                        (
                            detection_document,
                            submitted_bytes.clone(),
                            submitted_bytes_sha256,
                        )
                    } else {
                        // Detection happened against a historical session
                        // document. Apply its sparse merge-resolved delta to
                        // the actor's current accepted document so concurrent
                        // different-entity edits compose and same-entity edits
                        // obey transaction commit order.
                        if let Err(error) =
                            lease.actor_mut().drop_document(detection_document).await
                        {
                            return Err(lease.handle_guest_call_error(error));
                        }
                        let current_document = lease.accepted_document();
                        let current_bytes = lease.accepted_bytes();
                        let change_source =
                            match VecEntityChangeSource::new(changes.clone(), limits) {
                                Ok(source) => source,
                                Err(error) => {
                                    return Err(lease.handle_guest_call_error(error));
                                }
                            };
                        let renderer_input =
                            match lease.actor_mut().fork_document(current_document).await {
                                Ok(document) => document,
                                Err(error) => return Err(lease.handle_guest_call_error(error)),
                            };
                        let renderer_transition = match lease
                            .actor_mut()
                            .entities_changed(
                                renderer_input,
                                limits,
                                WasmEntityUpdate {
                                    before_descriptor,
                                    after_descriptor,
                                    before: Arc::new(ArcByteSource::new(current_bytes.clone())),
                                    changes: Box::new(change_source),
                                },
                            )
                            .await
                        {
                            Ok(transition) => transition,
                            Err(error) => return Err(lease.handle_guest_call_error(error)),
                        };
                        let rendered_transition = match drain_entity_transition_edits(
                            lease.actor_mut(),
                            renderer_transition,
                            &current_bytes,
                            None,
                            None,
                            limits,
                        )
                        .await
                        {
                            Ok(transition) => transition,
                            Err(error) => return Err(lease.handle_guest_call_error(error)),
                        };
                        if let Err(error) = lease.actor_mut().drop_document(renderer_input).await {
                            return Err(lease.handle_guest_call_error(error));
                        }
                        counters.accumulate(rendered_transition.counters);
                        counters.shared_renderer_cache_hits = 1;
                        (
                            rendered_transition.document,
                            rendered_transition.bytes.clone(),
                            rendered_transition.bytes_sha256,
                        )
                    };
                counters.host_full_diff_bytes_compared = host_full_diff_bytes_compared;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.private_document_cache_hits = 1;
                counters.durable_semantic_changes = u64::try_from(changes.entity_change_count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(certified_row_count);
                self.plugin_host.record_v2_transition_counters(counters);
                lease.complete_guest_call(
                    successor_document,
                    materialized_bytes.clone(),
                    materialized_bytes_sha256,
                    materialization_version.clone(),
                )?;
                (
                    changes,
                    PendingPluginActorPublication::Existing {
                        lease,
                        successor_key: actor_key,
                        view,
                    },
                    materialized_bytes,
                    create_rows,
                )
            } else {
                let store_permit = self
                    .admit_fresh_plugin_store(&mut reconciliation.actor_publications)
                    .await?;
                let mut actor = factory
                    .instantiate_actor()
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_actor_instantiate"
                    ))
                    .await?;
                let source = ArcByteSource::new(submitted_bytes.clone());
                let cold_limits = WasmTransitionLimits::for_cold_file_bytes(
                    u64::try_from(submitted_bytes.len()).unwrap_or(u64::MAX),
                );
                let transition = actor
                    .open_file(
                        cold_limits,
                        WasmOpenFileInput {
                            descriptor,
                            file: Arc::new(source),
                            creates,
                        },
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_open_file"
                    ))
                    .await?;
                let validated = drain_file_transition_changes(
                    actor.as_mut(),
                    transition,
                    &schemas,
                    cold_limits,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_open_file_drain"
                ))
                .await?;
                let certified_row_count = validated
                    .certified_batches
                    .iter()
                    .map(|batch| batch.row_count)
                    .sum::<u64>();
                write.set_certified_entity_batches(validated.certified_batches);
                let mut changes = validated.changes;
                let create_rows = self
                    .v2_create_rows(
                        selected,
                        &mut changes,
                        create_context,
                        &file_key,
                        existing_create_reservation.as_ref(),
                        None,
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_create_rows"
                    ))
                    .await?;
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes = u64::try_from(changes.entity_change_count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(certified_row_count);
                self.plugin_host.record_v2_transition_counters(counters);
                (
                    changes,
                    PendingPluginActorPublication::New {
                        cache: self.plugin_host.actor_cache(),
                        key: actor_key,
                        store: PluginActorStore::new(actor, store_permit),
                        document: validated.document,
                        bytes: submitted_bytes.clone(),
                        semantic_root: Arc::from(materialization_version.clone()),
                        view,
                    },
                    submitted_bytes.clone(),
                    create_rows,
                )
            };
            rows.append(create_rows);
            let change_rows = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_change_rows"
            )
            .in_scope(|| {
                append_plugin_change_rows_from_v2(rows, selected, changes, &write.file_id, &context)
            });
            if let Err(error) = change_rows {
                publication.discard().await;
                discard_plugin_actor_publications(std::mem::take(
                    &mut reconciliation.actor_publications,
                ))
                .await;
                return Err(error);
            }
            match selected.materialization() {
                PluginMaterialization::Blob => {
                    if materialized_bytes.as_ref() != write.data() {
                        write.replace_data(materialized_bytes);
                    } else if let Some((visible_base_blob_hash, offset, length)) =
                        verified_same_length_blob_splice
                    {
                        write.set_verified_same_length_blob_splice(
                            visible_base_blob_hash,
                            offset,
                            length,
                        );
                    }
                    reconciliation
                        .materialized_file_keys
                        .insert(file_key.clone());
                }
                PluginMaterialization::Derived => {
                    reconciliation.derived_materializations.insert(
                        file_key.clone(),
                        DerivedMaterializationProof::from_bytes(&materialized_bytes, path.clone()),
                    );
                }
            }
            reconciliation
                .materialization_versions
                .insert(file_key.clone(), materialization_version);
            reconciliation.actor_publications.push(publication);
            reconciled_file_keys.insert(file_key);
        }

        // Promote only the semantic path. Ordinary SQL and file batches keep
        // their original Vec allocation through reconciliation.
        let mut reconciled_rows = if semantic_groups.is_empty() {
            None
        } else {
            Some(ReconciledRowBatch::promote_raw_rows(rows))
        };
        for (file_key, group) in semantic_groups {
            let session_key = SessionFileViewKey::new(&file_key.branch_id, &file_key.file_id);
            if reconciliation
                .actor_publications
                .iter()
                .any(|publication| publication.session_key() == &session_key)
            {
                discard_plugin_actor_publications(std::mem::take(
                    &mut reconciliation.actor_publications,
                ))
                .await;
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot transition v2 plugin file '{}' more than once",
                        file_key.file_id
                    ),
                ));
            }
            let installed_key = PluginBranchEntryKey {
                branch_id: file_key.branch_id.clone(),
                plugin_key: group.plugin.key().to_string(),
            };
            let factory = component_v2_factories
                .get(&installed_key)
                .expect("semantic v2 plugin should have a compiled factory")
                .clone();
            let descriptor = WasmFileDescriptor {
                path: Some(group.path.clone()),
                media_type: inferred_media_type_for_path(Some(&group.path)).map(str::to_owned),
                plugin: WasmPluginSelection {
                    plugin_key: group.plugin.key().to_string(),
                    generation: group.plugin.archive_blob_hash().to_string(),
                },
            };
            let actor_key = PluginActorKey {
                branch_id: file_key.branch_id.clone(),
                file_id: file_key.file_id.clone(),
                path: group.path.clone(),
                owner_change_id: group.owner_change_id.clone(),
                plugin_key: group.plugin.key().to_string(),
                plugin_generation: group.plugin.archive_blob_hash().to_string(),
            };
            // Move semantic sources out exactly once. Their typed slots retain
            // the original batch positions while the plugin renderer runs.
            let semantic_rows = {
                let rows = reconciled_rows
                    .as_mut()
                    .expect("semantic groups promote the reconciled row batch");
                rows.take_raw_rows_at(&group.row_indices)?
            };
            let prepared = self
                .prepare_transaction_rows(semantic_rows)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_semantic_prepare_rows"
                ))
                .await?;
            if prepared.iter().any(|row| {
                row.branch_id.as_str() != file_key.branch_id
                    || row.file_id.map(SharedStr::as_str) != Some(file_key.file_id.as_str())
                    || row.global
                    || row.untracked
                    || group
                        .plugin
                        .schema_keys()
                        .binary_search_by(|key| key.as_str().cmp(row.schema_key.as_str()))
                        .is_err()
            }) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "normalized semantic rows escaped v2 plugin file '{}' ownership",
                        file_key.file_id
                    ),
                ));
            }
            let limits = WasmTransitionLimits::default();
            let changes = v2_host_changes_from_prepared_rows(&prepared, limits)?;
            if changes.entity_change_count() == 0 {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "v2 semantic write batch must contain at least one entity change",
                ));
            }
            let view = PendingPluginActorView {
                session_key: session_key.clone(),
                plugin_key: group.plugin.key().to_string(),
                plugin_generation: group.plugin.archive_blob_hash().to_string(),
                owner_change_id: group.owner_change_id.clone(),
                semantic_chainable: true,
                retain_large_import_actor: group.plugin.api_version()
                    != crate::wasm::WASM_COMPONENT_V3_PROTOTYPE_API_VERSION,
            };

            let prior_index = self
                .pending_plugin_actor_publications
                .iter()
                .position(|publication| publication.session_key() == &session_key);
            let prior_publication =
                prior_index.map(|index| self.pending_plugin_actor_publications.remove(index));
            let was_chained = prior_publication.is_some();
            let (lease, successor_key, publication_view) = match prior_publication {
                Some(PendingPluginActorPublication::Existing {
                    lease,
                    successor_key,
                    view: prior_view,
                }) if successor_key == actor_key
                    && prior_view.semantic_chainable
                    && prior_view.plugin_key == view.plugin_key
                    && prior_view.plugin_generation == view.plugin_generation
                    && prior_view.owner_change_id == view.owner_change_id =>
                {
                    (lease, successor_key, prior_view)
                }
                Some(publication) => {
                    self.pending_plugin_actor_publications.push(publication);
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "semantic entity writes cannot follow a byte or identity transition for v2 plugin file '{}' in the same transaction",
                            file_key.file_id
                        ),
                    )
                    .with_hint("commit the byte transition before editing semantic entities"));
                }
                None => {
                    let cache = self.plugin_host.actor_cache();
                    let visible_materialization = self
                        .visible_v2_materialization(&file_key)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                                format!(
                                    "owned v2 plugin file '{}' has no visible materialization root",
                                    file_key.file_id
                                ),
                            )
                        })?;
                    let cold_open = cache
                        .prepare_cold_open(&actor_key, &visible_materialization.semantic_root)
                        .instrument(tracing::debug_span!(
                            target: "lix_perf",
                            "lix.perf.plugin_semantic_actor_lookup"
                        ))
                        .await?;
                    let observation = match cold_open {
                        PluginActorColdOpen::Ready(observation) => observation,
                        PluginActorColdOpen::Build(cold_install) => {
                            drop(cold_install);
                            self.cold_open_v2_semantic_actor(
                                &actor_key,
                                &group.plugin,
                                descriptor.clone(),
                                factory,
                                &mut reconciliation.actor_publications,
                            )
                            .instrument(tracing::debug_span!(
                                target: "lix_perf",
                                "lix.perf.plugin_semantic_actor_cold_open"
                            ))
                            .await?
                        }
                    };
                    if observation.key() != &actor_key {
                        return Err(LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            format!(
                                "v2 semantic write actor identity no longer matches file '{}'",
                                file_key.file_id
                            ),
                        ));
                    }
                    (
                        cache
                            .lease_for_transition(&observation)
                            .instrument(tracing::debug_span!(
                                target: "lix_perf",
                                "lix.perf.plugin_semantic_actor_lease"
                            ))
                            .await?,
                        actor_key,
                        view,
                    )
                }
            };
            let visible_materialization = match self.visible_v2_materialization(&file_key).await {
                Ok(Some(materialization)) => materialization,
                Ok(None) => {
                    let publication = PendingPluginActorPublication::Existing {
                        lease,
                        successor_key,
                        view: publication_view,
                    };
                    if was_chained {
                        self.pending_plugin_actor_publications.push(publication);
                    } else {
                        publication.discard().await;
                    }
                    return Err(LixError::new(
                        LixError::CODE_PLUGIN_OBSERVATION_STALE,
                        format!(
                            "owned v2 plugin file '{}' lost its materialization root",
                            file_key.file_id
                        ),
                    ));
                }
                Err(error) => {
                    let publication = PendingPluginActorPublication::Existing {
                        lease,
                        successor_key,
                        view: publication_view,
                    };
                    if was_chained {
                        self.pending_plugin_actor_publications.push(publication);
                    } else {
                        publication.discard().await;
                    }
                    return Err(error);
                }
            };
            let materialization_version = self.functions.call_uuid_v7().to_string();
            let transition = render_v2_semantic_changes_with_lease(
                lease,
                successor_key,
                publication_view,
                descriptor,
                changes,
                &visible_materialization.semantic_root,
                &materialization_version,
                limits,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_semantic_render"
            ))
            .await;
            let (publication, rendered_bytes, same_length_output_splice, counters) =
                match transition {
                    Ok(transition) => transition,
                    Err((error, publication)) => {
                        if was_chained {
                            self.pending_plugin_actor_publications.push(publication);
                        } else {
                            publication.discard().await;
                        }
                        discard_plugin_actor_publications(std::mem::take(
                            &mut reconciliation.actor_publications,
                        ))
                        .await;
                        return Err(error);
                    }
                };
            self.plugin_host.record_v2_transition_counters(counters);
            match group.plugin.materialization() {
                PluginMaterialization::Blob => {
                    let VisibleV2MaterializationBytes::Blob { hash } =
                        visible_materialization.bytes
                    else {
                        publication.discard().await;
                        discard_plugin_actor_publications(std::mem::take(
                            &mut reconciliation.actor_publications,
                        ))
                        .await;
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!(
                                "owned v2 plugin file '{}' blob contract has no visible CAS materialization",
                                file_key.file_id
                            ),
                        ));
                    };
                    let rendered_file = semantic_rendered_file_data(
                        file_key.file_id.clone(),
                        group.path,
                        group.filename,
                        file_key.branch_id.clone(),
                        hash,
                        rendered_bytes,
                        same_length_output_splice,
                    );
                    file_data.push(rendered_file);
                    reconciliation
                        .materialized_file_keys
                        .insert(file_key.clone());
                }
                PluginMaterialization::Derived => {
                    reconciliation.derived_materializations.insert(
                        file_key.clone(),
                        DerivedMaterializationProof::from_bytes(
                            &rendered_bytes,
                            group.path.clone(),
                        ),
                    );
                }
            }
            reconciliation
                .materialization_versions
                .insert(file_key.clone(), materialization_version);
            let rows = reconciled_rows
                .as_mut()
                .expect("semantic groups promote the reconciled row batch");
            rows.put_prepared_batch_at(&group.row_indices, prepared)?;
            reconciliation.actor_publications.push(publication);
            reconciled_file_keys.insert(file_key);
        }

        for (file_key, _metadata) in deleted_file_keys {
            if reconciled_file_keys.contains(&file_key) {
                continue;
            }
            let reservation_tombstones = self.v2_id_reservation_tombstones(&file_key).await?;
            if let Some(rows) = &mut reconciled_rows {
                rows.append_raw_batch(reservation_tombstones);
            } else {
                rows.append(reservation_tombstones);
            }
            if !owners.contains_key(&file_key) {
                reconciliation.remove_session_file_view(SessionFileViewKey::new(
                    &file_key.branch_id,
                    &file_key.file_id,
                ));
                continue;
            }
            reconciliation.remove_session_file_view(SessionFileViewKey::new(
                &file_key.branch_id,
                &file_key.file_id,
            ));
            let owner_tombstone =
                PluginFileOwner::delete_row(file_key.file_id, &file_key.branch_id)?;
            if let Some(rows) = &mut reconciled_rows {
                rows.push_raw(owner_tombstone);
            } else {
                rows.push(owner_tombstone);
            }
        }

        if let Some(mut rows) = reconciled_rows {
            rows.mark_plugin_reconciliation_rows_from(input_row_count)?;
            reconciliation.reconciled_rows = Some(rows);
        } else {
            mark_plugin_reconciliation_batch(rows, input_row_count)?;
        }
        Ok(reconciliation)
    }

    async fn prepare_transaction_write(
        &mut self,
        write: ReconciledTransactionWrite,
    ) -> Result<PreparedTransactionWrite, LixError> {
        Ok(match write {
            ReconciledTransactionWrite::Rows { mode, rows } => PreparedTransactionWrite::Rows {
                mode,
                rows: self.prepare_reconciled_rows(rows).await?,
            },
            ReconciledTransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
            } => PreparedTransactionWrite::RowsWithFileData {
                mode,
                rows: self.prepare_reconciled_rows(rows).await?,
                file_data,
                count,
            },
        })
    }

    async fn prepare_reconciled_rows(
        &mut self,
        rows: ReconciledRowBatch,
    ) -> Result<PreparedStateBatch, LixError> {
        match rows {
            ReconciledRowBatch::Raw(rows) => self.prepare_transaction_rows(rows).await,
            ReconciledRowBatch::Mixed(mut mixed) => {
                if mixed
                    .slots
                    .iter()
                    .any(|slot| matches!(slot, ReconciledRowSlot::Extracted))
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "transaction preparation observed an unfilled semantic slot",
                    ));
                }

                let raw_count = mixed
                    .slots
                    .iter()
                    .filter(|slot| matches!(slot, ReconciledRowSlot::Raw(_)))
                    .count();
                let mut raw_ordinals = Vec::with_capacity(raw_count);
                for slot in &mut mixed.slots {
                    if let ReconciledRowSlot::Raw(ordinal) = *slot {
                        raw_ordinals.push(ordinal as usize);
                        *slot = ReconciledRowSlot::Extracted;
                    }
                }
                let raw_rows = mixed.raw.take_rows(&raw_ordinals);
                let prepared_raw_rows = self
                    .prepare_transaction_rows_with_homogeneous(raw_rows, false)
                    .await?;
                if prepared_raw_rows.len() != raw_count {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "raw transaction preparation changed the reconciled row count",
                    ));
                }

                let raw_base = mixed.prepared.len();
                mixed.prepared.append(prepared_raw_rows);
                let mut next_raw = 0usize;
                let mut source_by_destination = Vec::with_capacity(mixed.slots.len());
                for slot in mixed.slots {
                    match slot {
                        ReconciledRowSlot::Prepared(ordinal) => {
                            source_by_destination.push(ordinal as usize);
                        }
                        ReconciledRowSlot::Extracted => {
                            source_by_destination.push(raw_base + next_raw);
                            next_raw += 1;
                        }
                        ReconciledRowSlot::Raw(_) => {
                            unreachable!(
                                "all raw reconciled slots were extracted before preparation"
                            )
                        }
                    }
                }
                if next_raw != raw_count {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "reconciled raw row preparation did not fill every matching slot",
                    ));
                }
                mixed.prepared.select_rows(&source_by_destination);
                Ok(mixed.prepared)
            }
        }
    }

    async fn prepare_transaction_rows(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<PreparedStateBatch, LixError> {
        self.prepare_transaction_rows_with_homogeneous(rows, true)
            .await
    }

    async fn prepare_transaction_rows_with_homogeneous(
        &mut self,
        rows: RawWriteBatch,
        allow_homogeneous: bool,
    ) -> Result<PreparedStateBatch, LixError> {
        let row_count = rows.len();
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let live_state = self.live_state.reader(&read);
        if allow_homogeneous && let Some(domain) = homogeneous_row_normalization_domain(&rows) {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&live_state, &staged, &domain)
                .await?;
            let mut scalar_facts = PreparedScalarBatch::with_capacity(rows.len());
            let mut rows = rows;
            for index in 0..rows.len() {
                let normalized =
                    normalize_raw_write_row_in_place(&mut rows, index, catalog, functions.clone())?;
                scalar_facts.push(plan_prepared_row_scalars(
                    rows.row(index),
                    normalized,
                    &functions,
                )?);
            }
            if rows.iter().any(|row| {
                row.snapshot
                    .is_some_and(TransactionJson::requires_batch_canonicalization)
            }) {
                canonicalize_transaction_json_batch(
                    rows.snapshot_slots_mut(),
                    "prepared snapshot_content",
                )?;
            }
            if rows.iter().any(|row| {
                row.metadata
                    .is_some_and(TransactionJson::requires_batch_canonicalization)
            }) {
                canonicalize_transaction_json_batch(
                    rows.metadata_slots_mut(),
                    "prepared metadata",
                )?;
            }
            let json_count = rows
                .iter()
                .map(|row| {
                    usize::from(row.snapshot.is_some()) + usize::from(row.metadata.is_some())
                })
                .sum();
            let mut prepared_rows = PreparedStateBatch::with_dense_capacity(row_count, json_count);
            for index in 0..row_count {
                push_prepared_state_row_from_planned_parts(
                    &mut prepared_rows,
                    &mut rows,
                    index,
                    scalar_facts.row(index),
                    self.origin_key.as_ref(),
                )?;
            }
            return Ok(prepared_rows);
        }
        let mut rows = rows;
        let mut normalized_facts = Vec::with_capacity(row_count);
        normalized_facts.resize_with(row_count, || None);
        let mut scalar_facts = PreparedScalarBatch::with_capacity(row_count);
        let mut scalar_ordinal_by_row = vec![usize::MAX; row_count];
        let mut rows_by_scope = BTreeMap::<Domain, Vec<usize>>::new();
        for (index, row) in rows.iter().enumerate() {
            rows_by_scope
                .entry(Domain::schema_catalog(
                    row.schema_scope_branch_id().to_string(),
                    row.untracked,
                ))
                .or_default()
                .push(index);
        }

        for (domain, row_indices) in rows_by_scope {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&live_state, &staged, &domain)
                .await?;
            for &index in &row_indices {
                let row = rows.row(index);
                if row.schema_key != REGISTERED_SCHEMA_KEY {
                    continue;
                }
                if row.file_id.is_some() {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        "lix_registered_schema rows must not be scoped to a file",
                    )
                    .with_hint("Schema definitions are scoped by branch and durability only; write them with null file_id."));
                }
                remember_pending_registered_schema(
                    row.snapshot.map(TransactionJson::value),
                    Domain::schema_catalog(row.schema_scope_branch_id().to_string(), row.untracked),
                    catalog,
                )?;
            }
            for &index in &row_indices {
                normalized_facts[index] = Some(normalize_raw_write_row_in_place(
                    &mut rows,
                    index,
                    catalog,
                    functions.clone(),
                )?);
            }
            // Preserve the historical domain-by-domain provider/error order:
            // normalize every row in this domain, then scalar-plan those same
            // rows before advancing to the next domain.
            for index in row_indices {
                let scalar = plan_prepared_row_scalars(
                    rows.row(index),
                    normalized_facts[index]
                        .take()
                        .expect("normalized domain row must have facts"),
                    &functions,
                )?;
                scalar_ordinal_by_row[index] = scalar_facts.schema_plan_ids.len();
                scalar_facts.push(scalar);
            }
        }

        if rows.iter().any(|row| {
            row.snapshot
                .is_some_and(TransactionJson::requires_batch_canonicalization)
        }) {
            canonicalize_transaction_json_batch(
                rows.snapshot_slots_mut(),
                "prepared snapshot_content",
            )?;
        }
        if rows.iter().any(|row| {
            row.metadata
                .is_some_and(TransactionJson::requires_batch_canonicalization)
        }) {
            canonicalize_transaction_json_batch(rows.metadata_slots_mut(), "prepared metadata")?;
        }

        let json_count = rows
            .iter()
            .map(|row| usize::from(row.snapshot.is_some()) + usize::from(row.metadata.is_some()))
            .sum();
        let mut prepared_rows = PreparedStateBatch::with_dense_capacity(row_count, json_count);
        for (index, &scalar_ordinal) in scalar_ordinal_by_row.iter().enumerate() {
            debug_assert_ne!(scalar_ordinal, usize::MAX);
            push_prepared_state_row_from_planned_parts(
                &mut prepared_rows,
                &mut rows,
                index,
                scalar_facts.row(scalar_ordinal),
                self.origin_key.as_ref(),
            )?;
        }
        Ok(prepared_rows)
    }

    async fn validate_prepared_writes_by_branch(
        &mut self,
        read: &(impl StorageAdapterRead + ?Sized),
        prepared_writes: &PreparedWriteSet,
    ) -> Result<(), LixError> {
        if prepared_tracked_rows_have_row_local_certificates(&prepared_writes.state_rows) {
            // Row-local certificates avoid rebuilding the O(rows) validation
            // index, but they do not prove that a public INSERT identity is
            // absent from committed state.
            if !prepared_writes.insert_selection.is_empty() {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_transaction_validation_branch();
                let live_state = self.live_state.reader(read);
                validate_certified_tracked_insert_identities(&live_state, prepared_writes)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.validation.insert_identities"
                    ))
                    .await?;
            }
            return Ok(());
        }
        if self.trust_filesystem_planner
            && let Some(certificate) = fresh_plugin_file_import_certificate(prepared_writes)
        {
            // The certificate proves that every omitted file-scoped row has
            // completed row-local validation, has no transaction-wide schema
            // constraint, and is owned by this exact pending planner-created
            // descriptor. Keep public INSERT absence validation against this
            // coherent commit snapshot before skipping the O(rows) index.
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let live_state = self.live_state.reader(read);
            validate_certified_fresh_plugin_file_import(&live_state, certificate).await?;
            return Ok(());
        }
        let validation_index = prepared_writes.validation_index();
        for scope in validation_index.schema_scopes() {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let branch_prepared_writes = validation_index.validation_set_for_schema_scope(scope);
            let live_state = self.live_state.reader(read);
            let schema_catalog = self
                .schema_resolver
                .catalog_for_validation(&live_state, scope)
                .await?;
            let mut validation_input = TransactionValidationInput::new(
                &branch_prepared_writes,
                schema_catalog,
                &live_state,
            );
            if self.trust_filesystem_planner {
                validation_input = validation_input.with_trusted_filesystem_planner();
            }
            validate_prepared_writes(validation_input).await?;
        }
        Ok(())
    }

    /// Convenience helper for programmatic APIs that only stage state rows.
    pub(crate) async fn stage_rows(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await
    }

    async fn require_existing_transaction_write_branch_ids(
        &mut self,
        write: &TransactionWrite,
    ) -> Result<(), LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let reader = self.branch_ctx.ref_reader(read);
        for branch_id in transaction_write_branch_ids(write) {
            if branch_id == GLOBAL_BRANCH_ID {
                continue;
            }
            if reader.load_head_commit_id(&branch_id).await?.is_none() {
                return Err(LixError::branch_not_found(
                    branch_id,
                    "stage_write",
                    "target",
                ));
            }
        }
        Ok(())
    }

    /// Returns the active branch resolved inside this write transaction.
    pub(crate) fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    /// Stages the protocol replay receipt into this transaction's final
    /// storage write set. The receipt is guarded by `KeyAbsent` during commit,
    /// so it either publishes with the SQL mutation or not at all.
    pub(crate) fn stage_execute_idempotency_receipt(
        &mut self,
        idempotency: &ExecuteIdempotency,
        receipt: &ExecuteIdempotencyReceipt,
    ) -> Result<(), LixError> {
        if self.idempotency_receipt.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "a transaction may stage only one execute idempotency receipt",
            ));
        }
        self.idempotency_receipt = Some(encode_receipt(idempotency, receipt)?);
        Ok(())
    }

    /// Returns the content identity of the SQL schema catalog captured when
    /// this transaction opened.
    pub(crate) fn sql_catalog_fingerprint(&self) -> &CatalogFingerprint {
        self.sql_schema_snapshot.fingerprint()
    }

    pub(crate) fn sql_public_catalog(&self) -> Result<Arc<crate::sql2::PublicCatalog>, LixError> {
        self.sql_planning_cache
            .public_catalog(self.sql_catalog_fingerprint(), || {
                Ok(self.sql_schema_snapshot.schema_jsons())
            })
    }

    pub(crate) fn prepare_sql_write_logical_plan(
        &self,
        sql: &str,
        statement: &DataFusionStatement,
    ) -> Result<crate::sql2::SqlLogicalPlan, LixError> {
        let fingerprint = self.sql_catalog_fingerprint();
        if let Some(plan) =
            self.sql_planning_cache
                .write_plan(sql, fingerprint, &self.active_branch_id)
        {
            return Ok(crate::sql2::create_write_logical_plan_from_template(plan));
        }

        let catalog = self.sql_public_catalog()?;
        let plan = crate::sql2::create_write_plan_template_from_parsed(
            statement,
            catalog.as_ref(),
            &self.active_branch_id,
        )?;
        self.sql_planning_cache.remember_write_plan(
            sql,
            fingerprint.clone(),
            &self.active_branch_id,
            &plan,
        );
        Ok(crate::sql2::create_write_logical_plan_from_template(plan))
    }

    /// Returns this transaction's prepared runtime functions.
    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    pub(crate) fn replace_origin_key(&mut self, origin_key: Option<String>) -> Option<String> {
        std::mem::replace(&mut self.origin_key, origin_key.map(SharedStr::from)).map(Into::into)
    }

    pub(crate) async fn execute_read_sql_statement(
        &mut self,
        sql: String,
        statement: DataFusionStatement,
        params: Vec<Value>,
    ) -> Result<SqlQueryResult, LixError> {
        let storage = self.storage.clone();
        let read = storage.begin_read(StorageReadOptions::default()).await?;
        let active_branch_id = self.active_branch_id.clone();
        let live_state = Arc::clone(&self.live_state);
        let binary_cas = Arc::clone(&self.binary_cas);
        let branch_ctx = Arc::clone(&self.branch_ctx);
        let visible_schemas = self.sql_visible_schemas();
        let functions = self.functions.clone();
        let staged = self.staged_writes.staging_overlay()?;
        let staged_writes = Arc::clone(&self.staged_writes);
        let filesystem_path_index_cache = Arc::clone(&self.filesystem_path_index_cache);
        let filesystem_path_index_epoch = Arc::clone(&self.filesystem_path_index_epoch);
        let plugin_host = self.plugin_host.clone();

        with_static_transaction_sql_read::<StorageImpl, _, _>(read, |read_store| async move {
            let read_ctx = TransactionSqlReadExecutionContext {
                active_branch_id,
                read_store,
                live_state,
                binary_cas,
                branch_ctx,
                visible_schemas,
                functions,
                staged,
                staged_writes,
                filesystem_path_index_cache,
                filesystem_path_index_epoch,
                plugin_host,
            };
            let result = crate::sql2::execute_transaction_read_statement_from_parsed(
                &read_ctx, self, &sql, statement, &params,
            )
            .await;
            drop(read_ctx);
            result
        })
        .await
    }

    fn sql_visible_schemas(&self) -> Vec<JsonValue> {
        self.sql_schema_snapshot.schema_jsons()
    }

    /// Advances a branch ref without staging tracked rows.
    ///
    /// Fast-forward merges use this path because the commit graph already
    /// contains the source head; the target ref only needs to move to it.
    pub(crate) async fn advance_branch_ref(
        &mut self,
        branch_id: &str,
        commit_id: CommitId,
    ) -> Result<(), LixError> {
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push(branch_ref_stage_row(branch_id, &commit_id));
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await?;
        Ok(())
    }

    pub(crate) fn stage_merge_commit(
        &self,
        branch_id: String,
        source_parent_commit_id: CommitId,
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<String, LixError> {
        let commit_id = self
            .staged_writes
            .stage_selected_commit_change_refs(branch_id.clone(), selected_changes)?;
        self.staged_writes
            .add_commit_parent(branch_id, source_parent_commit_id)?;
        Ok(commit_id)
    }

    pub(crate) fn stage_checkpoint_commit(
        &self,
        branch_id: String,
        previous_checkpoint_commit_id: CommitId,
        recovered_head_commit_id: CommitId,
        interval_has_commits: bool,
        gc_state: CheckpointGcState,
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<String, LixError> {
        let commit_id = self
            .staged_writes
            .stage_selected_commit_change_refs(branch_id.clone(), selected_changes)?;
        let checkpoint_commit_id = CommitId::parse_lix(&commit_id, "staged checkpoint commit id")?;
        self.staged_writes
            .set_first_commit_parent(branch_id.clone(), previous_checkpoint_commit_id)?;
        self.staged_writes
            .add_checkpoint_publication(CheckpointPublication {
                recovery_ref: CheckpointRecoveryRef {
                    branch_id,
                    recovered_head_commit_id,
                    checkpoint_commit_id,
                    interval_has_commits,
                },
                gc_state,
            })?;
        Ok(commit_id)
    }

    /// Loads the branch-local recovery root and repository-global maintenance
    /// state from one storage snapshot.
    pub(crate) async fn checkpoint_publication_state(
        &mut self,
        branch_id: &str,
    ) -> Result<(Option<CheckpointRecoveryRef>, CheckpointGcState), LixError> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let recovery_ref = load_recovery_ref(&read, branch_id).await?;
        let gc_state = load_checkpoint_gc_state(&read).await?;
        Ok((recovery_ref, gc_state))
    }

    /// Creates a branch-ref reader scoped to this write transaction.
    pub(crate) async fn branch_ref_reader(&mut self) -> impl BranchRefReader + '_ {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open transaction read scope");
        self.branch_ctx
            .ref_reader(SharedStorageAdapterRead::new(read))
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) async fn tracked_state_reader(
        &mut self,
    ) -> TrackedStateStoreReader<SharedStorageAdapterRead<StorageImpl::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open transaction read scope");
        self.tracked_state
            .reader(SharedStorageAdapterRead::new(read))
    }

    /// Attempts the current branch's checkpoint-relative direct diff from one
    /// transaction-scoped snapshot. A missing or stale accelerator is an
    /// ordinary `None`; callers retain the historical tracked-state oracle.
    pub(crate) async fn working_diff_at_head(
        &self,
        branch_id: &str,
        head_commit_id: CommitId,
        request: &TrackedStateDiffRequest,
    ) -> Result<Option<TrackedWorkingDiff>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let Some(control) = BranchHeadControlContext::new()
            .reader(read.clone())
            .load(branch_id)
            .await?
        else {
            return Ok(None);
        };
        if control.head_commit_id != head_commit_id {
            return Ok(None);
        }
        TrackedHeadContext::new()
            .reader(read)
            .working_diff_for_control(branch_id, control, request)
            .await
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) async fn commit_graph_reader(
        &mut self,
    ) -> CommitGraphStoreReader<SharedStorageAdapterRead<StorageImpl::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open transaction read scope");
        CommitGraphContext::new().reader(SharedStorageAdapterRead::new(read))
    }

    async fn execute_apply_or_revert(
        &mut self,
        command: DiffCommand,
        diff_ids: Vec<String>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let selections = diff_ids
            .iter()
            .map(|diff_id| {
                crate::tracked_state::decode_diff_id(diff_id)
                    .map(|sides| (diff_id.as_str(), sides))
                    .map_err(|_| stale_or_unknown_diff_id())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let change_ids = selections
            .iter()
            .flat_map(|(_, sides)| [sides.before, sides.after])
            .flatten()
            .collect::<BTreeSet<_>>();
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let packed_records =
            futures_util::future::try_join_all(change_ids.iter().copied().map(|change_id| {
                let read = &read;
                async move {
                    crate::tracked_state::load_change_record_by_id(read, change_id)
                        .await
                        .map(|record| (change_id, record))
                }
            }))
            .await?;
        let mut records = packed_records
            .into_iter()
            .filter_map(|(change_id, record)| record.map(|record| (change_id, record)))
            .collect::<HashMap<_, _>>();
        let missing = change_ids
            .into_iter()
            .filter(|change_id| !records.contains_key(change_id))
            .collect::<Vec<_>>();
        records.extend(load_change_records(&read, missing.into_iter()).await?);
        let mut payloads = materialize_known_change_payloads(
            &read,
            records.values().cloned(),
            ChangeRecordProjection::full(),
        )
        .await?;
        drop(read);
        let branch_id = self.active_branch_id.clone();
        let mut identities = BTreeSet::new();
        let mut plans = Vec::with_capacity(selections.len());
        for (diff_id, sides) in selections {
            let before = sides
                .before
                .map(|id| required_diff_change(&records, id, diff_id))
                .transpose()?;
            let after = sides
                .after
                .map(|id| required_diff_change(&records, id, diff_id))
                .transpose()?;
            let identity = diff_record_identity(before.or(after).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("diff_id '{diff_id}' has no resolvable side"),
                )
            })?);
            if let (Some(before), Some(after)) = (before, after)
                && diff_record_identity(before) != diff_record_identity(after)
            {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!("diff_id '{diff_id}' joins changes for different entities"),
                ));
            }
            if !identities.insert(identity.clone()) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "diff command selection contains more than one row for the same entity",
                ));
            }
            let (expected, target) = match command {
                DiffCommand::Revert => (sides.after, sides.before),
                DiffCommand::Apply => (sides.before, sides.after),
                DiffCommand::CreateCheckpoint => unreachable!(),
            };
            plans.push((diff_id, identity, expected, target));
        }
        let request = LiveStateExactBatchRequest {
            rows: plans
                .iter()
                .map(
                    |(_, (schema_key, entity_pk, file_id), _, _)| LiveStateExactRowRequest {
                        schema_key: schema_key.clone(),
                        branch_id: branch_id.clone(),
                        entity_pk: entity_pk.clone(),
                        file_id: file_id.clone(),
                    },
                )
                .collect(),
            projection: LiveStateProjection::default(),
            untracked: Some(false),
            include_tombstones: true,
        };
        let current = self
            .load_visible_exact_live_state_batch(&request)
            .await?
            .into_rows();
        let mut target_change_ids = Vec::new();
        let mut rows = RawWriteBatch::with_capacity(plans.len());
        for ((diff_id, (schema_key, entity_pk, file_id), expected, target), current) in
            plans.into_iter().zip(current)
        {
            let current_matches = match expected {
                Some(expected) => current
                    .as_ref()
                    .and_then(|row| row.change_id)
                    .is_some_and(|change_id| change_id == expected),
                None => current.as_ref().is_none_or(|row| row.deleted),
            };
            if !current_matches {
                return Err(stale_or_unknown_diff_id());
            }
            if let Some(target) = target {
                required_diff_change(&records, target, diff_id)?;
                target_change_ids.push(target);
            } else {
                rows.push(TransactionWriteRow {
                    entity_pk: Some(entity_pk),
                    schema_key: schema_key.into(),
                    file_id: file_id.map(Into::into),
                    snapshot: None,
                    metadata: None,
                    origin: None,
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: branch_id.clone().into(),
                });
            }
        }
        if !target_change_ids.is_empty() {
            for change_id in target_change_ids {
                let payload = payloads.remove(&change_id).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("materialized diff target '{change_id}' is missing"),
                    )
                })?;
                let identity = payload.identity.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "materialized diff target is missing its identity",
                    )
                })?;
                rows.push(TransactionWriteRow {
                    entity_pk: Some(identity.entity_pk),
                    schema_key: identity.schema_key.into(),
                    file_id: identity.file_id.map(Into::into),
                    snapshot: parse_materialized_diff_json(
                        payload.snapshot_content,
                        "diff target",
                    )?,
                    metadata: parse_materialized_diff_json(
                        payload.metadata,
                        "diff target metadata",
                    )?,
                    origin: None,
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: branch_id.clone().into(),
                });
            }
        }
        if !rows.is_empty() {
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            })
            .await?;
        }
        Ok(crate::sql2::DiffCommandOutcome {
            rows_affected: diff_ids.len() as u64,
            commit_id: self
                .staged_writes
                .commit_id_for_branch(&branch_id)?
                .map(|commit_id| commit_id.to_string()),
        })
    }

    async fn execute_checkpoint_selection(
        &mut self,
        diff_ids: Vec<String>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let branch_id = self.active_branch_id.clone();
        let (previous_recovery, mut gc_state) =
            self.checkpoint_publication_state(&branch_id).await?;
        let head_commit_id = self
            .load_branch_head(&branch_id)
            .await?
            .ok_or_else(|| LixError::branch_not_found(&branch_id, "create checkpoint", "target"))?;
        let direct_checkpoint = {
            let mut tracked = self.tracked_state_reader().await;
            latest_checkpoint_at_head(&mut tracked, &head_commit_id, &branch_id).await?
        };
        let previous_checkpoint_commit_id = match direct_checkpoint {
            Some(commit_id) => commit_id,
            None => {
                let mut graph = self.commit_graph_reader().await;
                checkpoint_history_from_head(&mut graph, &head_commit_id)
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("branch '{branch_id}' has no checkpoint baseline"),
                        )
                    })?
                    .commit_id
            }
        };
        let diff = {
            let mut tracked = self.tracked_state_reader().await;
            tracked
                .diff_commits(
                    &previous_checkpoint_commit_id.to_string(),
                    &head_commit_id.to_string(),
                    &TrackedStateDiffRequest::default(),
                )
                .await?
        };
        let requested = diff_ids.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != diff_ids.len() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "checkpoint selection contains duplicate diff_id rows",
            ));
        }
        let mut matched = BTreeSet::new();
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(diff.entries.len());
        let mut unselected = StagedCommitChangeBatchBuilder::with_capacity(diff.entries.len());
        for entry in diff
            .entries
            .into_iter()
            .filter(|entry| entry.identity.schema_key() != CHECKPOINT_MARKER_SCHEMA_KEY)
        {
            let diff_id = crate::tracked_state::encode_diff_id(
                entry.before.as_ref().map(|row| row.change_id),
                entry.after.as_ref().map(|row| row.change_id),
            )?;
            let target = entry.after.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("working diff '{diff_id}' has no target row"),
                )
            })?;
            if requested.contains(&diff_id) {
                matched.insert(diff_id);
                push_checkpoint_selected_change(&mut selected, target, entry.kind);
            } else {
                push_checkpoint_selected_change(&mut unselected, target, entry.kind);
            }
        }
        let selected = selected.finish();
        let unselected = unselected.finish();
        if matched != requested {
            return Err(stale_or_unknown_diff_id());
        }
        let interval_has_commits = head_commit_id != previous_checkpoint_commit_id;
        gc_state.checkpoint_sequence =
            gc_state.checkpoint_sequence.checked_add(1).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint sequence overflow",
                )
            })?;
        if let Some(previous_recovery) = previous_recovery {
            gc_state.add_collectible_interval(previous_recovery.interval_has_commits);
        }

        let checkpoint_commit_id = if unselected.is_empty() {
            let mut marker_rows = RawWriteBatch::with_capacity(1);
            marker_rows.push(checkpoint_marker_stage_row(&branch_id));
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: marker_rows,
            })
            .await?;
            self.stage_checkpoint_commit(
                branch_id.clone(),
                previous_checkpoint_commit_id,
                head_commit_id,
                interval_has_commits,
                gc_state,
                selected,
            )?
        } else {
            let checkpoint_commit_id = self.staged_writes.stage_intermediate_commit(
                branch_id.clone(),
                previous_checkpoint_commit_id,
                selected,
            )?;
            let mut marker_rows = RawWriteBatch::with_capacity(1);
            marker_rows.push(checkpoint_marker_stage_row(&branch_id));
            let marker = self.prepare_transaction_rows(marker_rows).await?;
            self.staged_writes
                .stage_intermediate_rows(checkpoint_commit_id, marker)?;
            self.staged_writes
                .stage_selected_commit_change_refs(branch_id.clone(), unselected)?;
            self.staged_writes
                .set_first_commit_parent(branch_id.clone(), checkpoint_commit_id)?;
            self.staged_writes
                .add_checkpoint_publication(CheckpointPublication {
                    recovery_ref: CheckpointRecoveryRef {
                        branch_id: branch_id.clone(),
                        recovered_head_commit_id: head_commit_id,
                        checkpoint_commit_id,
                        interval_has_commits,
                    },
                    gc_state,
                })?;
            checkpoint_commit_id.to_string()
        };
        Ok(crate::sql2::DiffCommandOutcome {
            rows_affected: diff_ids.len() as u64,
            commit_id: Some(checkpoint_commit_id),
        })
    }

    pub(crate) async fn execute_diff_command_query_owned(
        &mut self,
        command: DiffCommand,
        query_sql: String,
        params: Vec<Value>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let statement = crate::sql2::parse_statement(&query_sql)?;
        let result = self
            .execute_read_sql_statement(query_sql, statement, params)
            .await?;
        if result.columns.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "diff command query must return exactly one column, got {}",
                    result.columns.len()
                ),
            ));
        }
        let mut diff_ids = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let [Value::Text(diff_id)] = row.as_slice() else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "diff command query must return non-null text diff_id values",
                ));
            };
            diff_ids.push(diff_id.clone());
        }
        if diff_ids.is_empty() {
            return Ok(crate::sql2::DiffCommandOutcome {
                rows_affected: 0,
                commit_id: None,
            });
        }
        self.execute_diff_command(command, diff_ids).await
    }
}

fn required_diff_change<'a>(
    records: &'a HashMap<ChangeId, ChangeRecord>,
    change_id: ChangeId,
    _diff_id: &str,
) -> Result<&'a ChangeRecord, LixError> {
    records.get(&change_id).ok_or_else(stale_or_unknown_diff_id)
}

fn stale_or_unknown_diff_id() -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        "stale or unknown diff_id; re-evaluate the source diff and retry",
    )
}

fn diff_record_identity(record: &ChangeRecord) -> (String, EntityPk, Option<String>) {
    (
        record.schema_key.clone(),
        record.entity_pk.clone(),
        record.file_id.clone(),
    )
}

fn parse_materialized_diff_json(
    json: Option<SharedStr>,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    json.map(|json| {
        let value = serde_json::from_str(json.as_ref()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to materialize {context} JSON: {error}"),
            )
        })?;
        TransactionJson::from_value(value, context)
    })
    .transpose()
}

fn push_checkpoint_selected_change(
    selected: &mut StagedCommitChangeBatchBuilder,
    row: crate::tracked_state::TrackedStateDiffRow,
    kind: TrackedStateDiffKind,
) {
    let created_at = match kind {
        TrackedStateDiffKind::Added => row.updated_at,
        TrackedStateDiffKind::Modified | TrackedStateDiffKind::Removed => row.created_at,
    };
    selected.push(
        row.identity,
        row.commit_id,
        row.change_id,
        row.deleted,
        created_at,
        row.updated_at,
    );
}

fn incremental_filesystem_index_enabled() -> bool {
    #[cfg(test)]
    if std::env::var_os("LIX_PATH_INDEX_BENCH_DISABLE_INCREMENTAL").is_some() {
        return false;
    }
    true
}

pub(crate) struct TransactionSqlReadExecutionContext<R: crate::storage_adapter::StorageRead> {
    active_branch_id: String,
    read_store: SharedStorageAdapterRead<R>,
    live_state: Arc<LiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    branch_ctx: Arc<BranchContext>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    staged: PreparedStateRowOverlay,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    plugin_host: PluginRuntimeHost,
}

#[async_trait]
impl<R> SqlExecutionContext for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    type ReadStore = SharedStorageAdapterRead<R>;

    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    fn live_state(&self) -> Arc<dyn crate::live_state::LiveStateReader> {
        Arc::new(TransactionReadLiveStateReader {
            base: self.live_state.reader(self.read_store.clone()),
            read_store: self.read_store.clone(),
            staged: self.staged.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
        })
    }

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(TransactionReadLiveStateReader {
            base: self.live_state.reader(self.read_store.clone()),
            read_store: self.read_store.clone(),
            staged: self.staged.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
        })
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn history_query_source(
        &self,
        default_as_of_commit_id: String,
    ) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            store: self.read_store.clone(),
            json_reader: crate::json_store::JsonStoreContext::new().reader(self.read_store.clone()),
            certified_history_reader: Some(Arc::new(CertifiedHistoryStoreReader::new(
                self.read_store.clone(),
            ))),
            default_as_of_commit_id,
        }
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            store: self.read_store.clone(),
            json_reader: crate::json_store::JsonStoreContext::new().reader(self.read_store.clone()),
        }
    }

    fn commit_graph(&self) -> Box<dyn crate::commit_graph::CommitGraphReader> {
        Box::new(CommitGraphContext::new().reader(self.read_store.clone()))
    }

    fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
        Arc::new(self.branch_ctx.ref_reader(self.read_store.clone()))
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(TransactionBlobDataReader {
            base: Arc::new(self.binary_cas.reader(self.read_store.clone())),
            staged_writes: Arc::clone(&self.staged_writes),
        })
    }

    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
    }
}

struct TransactionBlobDataReader {
    base: Arc<dyn BlobDataReader>,
    staged_writes: Arc<TransactionWriteBuffer>,
}

#[async_trait]
impl BlobDataReader for TransactionBlobDataReader {
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        load_transaction_blob_bytes(self.base.as_ref(), &self.staged_writes, hashes).await
    }
}

async fn load_transaction_blob_bytes(
    base: &dyn BlobDataReader,
    staged_writes: &TransactionWriteBuffer,
    hashes: &[BlobHash],
) -> Result<BlobBytesBatch, LixError> {
    let mut entries = staged_writes
        .load_staged_file_bytes_many(hashes)?
        .into_vec();
    let mut missing_indices = Vec::new();
    let mut missing_hashes = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.is_none() {
            missing_indices.push(index);
            missing_hashes.push(hashes[index]);
        }
    }
    if missing_hashes.is_empty() {
        return Ok(BlobBytesBatch::new(entries));
    }

    let base_entries = base.load_bytes_many(&missing_hashes).await?.into_vec();
    if base_entries.len() != missing_indices.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "transaction blob read expected {} fallback rows, got {}",
                missing_indices.len(),
                base_entries.len()
            ),
        ));
    }
    for (index, entry) in missing_indices.into_iter().zip(base_entries) {
        entries[index] = entry;
    }
    Ok(BlobBytesBatch::new(entries))
}

struct TransactionReadLiveStateReader<R: crate::storage_adapter::StorageRead> {
    base: crate::live_state::LiveStateStoreReader<SharedStorageAdapterRead<R>>,
    read_store: SharedStorageAdapterRead<R>,
    staged: PreparedStateRowOverlay,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
}

#[async_trait]
impl<R> crate::live_state::LiveStateReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        overlay_scan_batch(&self.base, &self.staged, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        overlay_load_exact_batch(&self.base, &self.staged, request).await
    }
}

#[async_trait]
impl<R> FilesystemPathIndexReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage_adapter::StorageRead + Send + 'static,
{
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let descriptor_epoch = self.filesystem_path_index_epoch.load(Ordering::SeqCst);
        if descriptor_epoch == 0 {
            return self.base.path_index(request).await;
        }
        // The revision probe is only a cache-freshness optimization. Preserve the
        // pre-cache overlay behavior if a storage fault affects that single key.
        let cache_revision = load_path_index_revision(&self.read_store)
            .await
            .ok()
            .map(|revision| transaction_path_index_cache_revision(revision, descriptor_epoch));
        if let Some(cache_revision) = cache_revision.as_deref()
            && let Some(index) = self
                .filesystem_path_index_cache
                .get(request, Some(cache_revision))
        {
            return Ok(index);
        }
        let rows =
            overlay_scan_batch(&self.base, &self.staged, &request.live_state_request()).await?;
        #[cfg(test)]
        record_transaction_path_index_build(rows.len());
        let index = Arc::new(FilesystemPathIndex::from_live_batch(&rows)?);
        Ok(match cache_revision {
            Some(cache_revision) => {
                self.filesystem_path_index_cache
                    .insert(request, Some(&cache_revision), index)
            }
            None => index,
        })
    }
}

/// Runs one transaction SQL read using a widened storage-read lifetime.
///
/// DataFusion requires provider state to be `'static`, but transaction reads
/// are scoped to the current storage snapshot. Keep this bridge private to
/// transaction SQL execution so no crate-level API can receive the widened
/// storage read handle.
async fn with_static_transaction_sql_read<StorageImpl, F, Fut>(
    read: StorageAdapterReadScope<StorageImpl::Read<'_>>,
    f: F,
) -> Result<SqlQueryResult, LixError>
where
    StorageImpl: Storage + 'static,
    F: FnOnce(SharedStorageAdapterRead<StorageImpl::Read<'static>>) -> Fut,
    Fut: Future<Output = Result<SqlQueryResult, LixError>>,
{
    // SAFETY: the widened read is wrapped immediately in `SharedStorageAdapterRead`,
    // only passed into this private SQL execution closure, and explicitly
    // dropped before returning. Escaped clones are detected by `finish()`.
    let read = unsafe { assume_static_storage_read::<StorageImpl>(read) };
    let read = SharedStorageAdapterRead::new(read);
    let finish = read.clone();
    let result = f(read).await;
    let finish_result = finish.finish().map_err(LixError::from);
    match (result, finish_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (_, Err(finish_error)) => Err(finish_error),
    }
}

/// Erases the storage borrow lifetime for scoped transaction SQL execution.
///
/// # Safety
///
/// The returned read scope must not outlive the storage value that produced
/// `read`, and it must be dropped before the enclosing SQL execution returns.
unsafe fn assume_static_storage_read<StorageImpl>(
    read: StorageAdapterReadScope<StorageImpl::Read<'_>>,
) -> StorageAdapterReadScope<StorageImpl::Read<'static>>
where
    StorageImpl: Storage + 'static,
{
    let read = std::mem::ManuallyDrop::new(read);
    unsafe {
        std::ptr::read(
            std::ptr::from_ref(&*read)
                .cast::<StorageAdapterReadScope<StorageImpl::Read<'static>>>(),
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct PreparedScalarRow {
    schema_plan_id: SchemaPlanId,
    facts: PreparedRowFacts,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
}

/// Fixed-width scalar columns planned in source order before JSON lowering.
///
/// Function-provider calls and scalar parse errors retain their historical
/// row-by-row order, while snapshots and metadata still canonicalize once as
/// a contiguous batch after all semantic normalization succeeds.
struct PreparedScalarBatch {
    schema_plan_ids: Vec<SchemaPlanId>,
    facts: Vec<PreparedRowFacts>,
    created_at: Vec<LixTimestamp>,
    updated_at: Vec<LixTimestamp>,
    change_ids: Vec<Option<ChangeId>>,
    commit_ids: Vec<Option<CommitId>>,
}

impl PreparedScalarBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            schema_plan_ids: Vec::with_capacity(capacity),
            facts: Vec::with_capacity(capacity),
            created_at: Vec::with_capacity(capacity),
            updated_at: Vec::with_capacity(capacity),
            change_ids: Vec::with_capacity(capacity),
            commit_ids: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, row: PreparedScalarRow) {
        self.schema_plan_ids.push(row.schema_plan_id);
        self.facts.push(row.facts);
        self.created_at.push(row.created_at);
        self.updated_at.push(row.updated_at);
        self.change_ids.push(row.change_id);
        self.commit_ids.push(row.commit_id);
    }

    fn row(&self, index: usize) -> PreparedScalarRow {
        PreparedScalarRow {
            schema_plan_id: self.schema_plan_ids[index],
            facts: self.facts[index],
            created_at: self.created_at[index],
            updated_at: self.updated_at[index],
            change_id: self.change_ids[index],
            commit_id: self.commit_ids[index],
        }
    }
}

fn plan_prepared_row_scalars(
    row: RawWriteRowRef<'_>,
    normalized: NormalizedRowFacts,
    functions: &FunctionProviderHandle,
) -> Result<PreparedScalarRow, LixError> {
    let NormalizedRowFacts {
        schema_plan_id,
        facts,
    } = normalized;
    let updated_at = match row.updated_at {
        Some(updated_at) => parse_prepared_timestamp("updated_at", updated_at)?,
        None => functions.call_timestamp(),
    };
    let created_at = match row.created_at {
        Some(created_at) => parse_prepared_timestamp("created_at", created_at)?,
        None => updated_at,
    };
    if row.entity_pk.is_none() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "normalized transaction write row is missing entity_pk",
        ));
    }
    let change_id = Some(match row.change_id {
        Some(change_id) => ChangeId::parse_lix(change_id, "prepared row change_id")?,
        None => ChangeId::from(functions.call_uuid_v7()),
    });
    let commit_id = row
        .commit_id
        .map(|id| CommitId::parse_lix(id, "prepared row commit_id"))
        .transpose()?;
    Ok(PreparedScalarRow {
        schema_plan_id,
        facts,
        created_at,
        updated_at,
        change_id,
        commit_id,
    })
}

fn push_prepared_state_row_from_planned_parts(
    prepared: &mut PreparedStateBatch,
    rows: &mut RawWriteBatch,
    row_index: usize,
    scalar: PreparedScalarRow,
    origin_key: Option<&SharedStr>,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    let schema_key = row.schema_key.clone();
    let file_id = row.file_id.cloned();
    let origin = row.origin.cloned();
    let global = row.global;
    let untracked = row.untracked;
    let branch_id = row.branch_id.clone();
    let snapshot = rows
        .take_snapshot(row_index)
        .map(|value| stage_json_from_value(value, "prepared row snapshot_content"))
        .transpose()?;
    let metadata = rows
        .take_metadata(row_index)
        .map(|value| stage_json_from_value(value, "prepared row metadata"))
        .transpose()?;
    let entity_pk = rows.take_entity_pk(row_index).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "normalized transaction write row is missing entity_pk",
        )
    })?;
    prepared.push_parts(
        scalar.schema_plan_id,
        scalar.facts,
        entity_pk,
        schema_key,
        file_id,
        snapshot,
        metadata,
        origin,
        origin_key,
        scalar.created_at,
        scalar.updated_at,
        global,
        scalar.change_id,
        scalar.commit_id,
        untracked,
        branch_id,
    );
    Ok(())
}

/// Returns the sole schema-catalog scope for a straightforward statement
/// batch. The SQL path normally reaches this with one entity schema and one
/// durability, so it can normalize rows in input order without allocating the
/// generic scope/reordering maps. Schema registration stays on the generic
/// path because registrations can change the catalog for later rows.
fn homogeneous_row_normalization_domain(rows: &RawWriteBatch) -> Option<Domain> {
    let first = rows.get(0)?;
    if first.schema_key == REGISTERED_SCHEMA_KEY {
        return None;
    }
    let branch_id = first.schema_scope_branch_id();
    let untracked = first.untracked;
    rows.iter()
        .all(|row| {
            row.schema_key != REGISTERED_SCHEMA_KEY
                && row.untracked == untracked
                && row.schema_scope_branch_id() == branch_id
        })
        .then(|| Domain::schema_catalog(branch_id.to_string(), untracked))
}

fn parse_prepared_timestamp(column: &str, timestamp: &str) -> Result<LixTimestamp, LixError> {
    LixTimestamp::parse(timestamp).map_err(|error| {
        LixError::unknown(format!(
            "invalid {column} timestamp for prepared state row: {error}"
        ))
    })
}

fn prepared_writes_change_catalog(prepared_writes: &PreparedWriteSet) -> bool {
    prepared_writes.state_rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            REGISTERED_SCHEMA_KEY | BRANCH_REF_SCHEMA_KEY
        )
    }) || prepared_writes
        .commit_change_refs_by_branch
        .values()
        .flat_map(crate::transaction::types::StagedCommitChangeRefs::selected_changes)
        .any(|change_ref| change_ref.schema_key() == REGISTERED_SCHEMA_KEY)
}

fn prepared_writes_require_filesystem_index_rebuild(prepared_writes: &PreparedWriteSet) -> bool {
    prepared_writes
        .state_rows
        .iter()
        .any(|row| row.schema_key == BRANCH_REF_SCHEMA_KEY)
        || prepared_writes
            .commit_change_refs_by_branch
            .values()
            .flat_map(crate::transaction::types::StagedCommitChangeRefs::selected_changes)
            .any(|change_ref| {
                matches!(
                    change_ref.schema_key(),
                    "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
                )
            })
}

pub(crate) struct OpenTransaction<StorageImpl: Storage = Memory> {
    pub(crate) transaction: Transaction<StorageImpl>,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction<StorageImpl>(
    mode: &SessionMode,
    storage: StorageAdapter<StorageImpl>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    session_file_views: SessionFileViews,
) -> Result<OpenTransaction<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Transaction::open(
        mode,
        storage,
        live_state,
        tracked_state,
        binary_cas,
        plugin_host,
        branch_ctx,
        catalog_context,
        sql_planning_cache,
        session_file_views,
    )
    .await
}

#[async_trait]
impl<StorageImpl> SqlWriteExecutionContext for Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.sql_visible_schemas())
    }

    fn public_catalog(&self) -> Result<Arc<crate::sql2::PublicCatalog>, LixError> {
        self.sql_public_catalog()
    }

    fn schema_catalog_snapshot(&self) -> Option<Arc<CatalogSnapshot>> {
        Some(Arc::clone(&self.sql_schema_snapshot))
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
    }

    fn session_file_views(&self) -> Option<SessionFileViews> {
        // A read in an explicit transaction may expose staged plugin bytes.
        // Publishing that observation into the session would leak uncommitted
        // state and can wait forever behind this transaction's actor lease.
        None
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.binary_cas.reader(read);
        load_transaction_blob_bytes(&base, &self.staged_writes, hashes).await
    }

    async fn scan_live_state_batch(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_visible_live_state_batch(request).await
    }

    async fn load_exact_live_state_batch(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        self.load_visible_exact_live_state_batch(request).await
    }

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let descriptor_epoch = self.filesystem_path_index_epoch.load(Ordering::SeqCst);
        if descriptor_epoch == 0 {
            return self.live_state.reader(read).path_index(request).await;
        }
        // The revision probe is only a cache-freshness optimization. Preserve the
        // pre-cache overlay behavior if a storage fault affects that single key.
        let cache_revision = load_path_index_revision(&read)
            .await
            .ok()
            .map(|revision| transaction_path_index_cache_revision(revision, descriptor_epoch));
        if let Some(cache_revision) = cache_revision.as_deref()
            && let Some(index) = self
                .filesystem_path_index_cache
                .get(request, Some(cache_revision))
        {
            return Ok(index);
        }
        let staged = self.staged_writes.staging_overlay()?;
        let base = self.live_state.reader(read);
        let rows = overlay_scan_batch(&base, &staged, &request.live_state_request()).await?;
        #[cfg(test)]
        record_transaction_path_index_build(rows.len());
        let index = Arc::new(FilesystemPathIndex::from_live_batch(&rows)?);
        Ok(match cache_revision {
            Some(cache_revision) => {
                self.filesystem_path_index_cache
                    .insert(request, Some(&cache_revision), index)
            }
            None => index,
        })
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );

        self.branch_ctx
            .ref_reader(read)
            .load_head_commit_id(branch_id)
            .await
    }

    async fn load_collection_generation(
        &mut self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let Some(control) = BranchHeadControlContext::new()
            .reader(read.clone())
            .load(branch_id)
            .await?
        else {
            return Ok(None);
        };
        let mut generation = TrackedHeadContext::new()
            .reader(read)
            .collection_generation(branch_id, control.generation, scope)
            .await?;
        let staged = self.staged_writes.staging_overlay()?;
        if StagedLiveStateRows::collection_replaced(
            &staged,
            branch_id,
            scope.schema_key,
            scope.file_id,
        )? {
            generation.live_count = 0;
        }
        Ok(Some(generation))
    }

    fn has_staged_collection_rows(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<bool, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let file_ids = scope
            .file_id
            .map(|file_id| vec![NullableKeyFilter::Value(file_id.to_string())])
            .unwrap_or_default();
        Ok(!staged
            .staged_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![scope.schema_key.to_string()],
                    branch_ids: vec![branch_id.to_string()],
                    file_ids,
                    include_tombstones: true,
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })?
            .is_empty())
    }

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Self::stage_write(self, write).await
    }

    async fn execute_diff_command(
        &mut self,
        command: DiffCommand,
        diff_ids: Vec<String>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        match command {
            DiffCommand::Revert | DiffCommand::Apply => {
                self.execute_apply_or_revert(command, diff_ids).await
            }
            DiffCommand::CreateCheckpoint => self.execute_checkpoint_selection(diff_ids).await,
        }
    }

    fn staged_commit_id(&self, branch_id: &str) -> Result<Option<String>, LixError> {
        self.staged_writes
            .commit_id_for_branch(branch_id)
            .map(|commit_id| commit_id.map(|commit_id| commit_id.to_string()))
    }
}

fn prepared_transaction_write_affects_filesystem_path_index(
    write: &PreparedTransactionWrite,
) -> bool {
    prepared_transaction_write_rows(write).iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            "lix_file_descriptor"
                | "lix_directory_descriptor"
                | BLOB_REF_SCHEMA_KEY
                | BRANCH_REF_SCHEMA_KEY
        )
    })
}

fn prepared_transaction_write_rows(write: &PreparedTransactionWrite) -> &PreparedStateBatch {
    match write {
        PreparedTransactionWrite::Rows { rows, .. }
        | PreparedTransactionWrite::RowsWithFileData { rows, .. } => rows,
    }
}

fn materialized_live_state_batch_from_prepared(
    rows: &PreparedStateBatch,
) -> MaterializedLiveStateBatch {
    let mut output = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
    for row in rows.iter() {
        output.push_materialized_ref(
            row.entity_pk,
            row.schema_key.as_str(),
            row.file_id.map(SharedStr::as_str),
            row.snapshot.map(|snapshot| snapshot.materialize_shared()),
            row.metadata.map(|metadata| metadata.materialize_shared()),
            row.snapshot.is_none(),
            row.created_at,
            row.updated_at,
            row.global,
            row.change_id,
            row.commit_id,
            row.untracked,
            row.branch_id.as_str(),
        );
    }
    output.finish()
}

fn transaction_path_index_cache_revision(
    filesystem_revision: Option<Vec<u8>>,
    descriptor_epoch: usize,
) -> Vec<u8> {
    let mut cache_revision = b"transaction-path-index-v1".to_vec();
    cache_revision.extend_from_slice(&descriptor_epoch.to_be_bytes());
    match filesystem_revision {
        Some(revision) => {
            cache_revision.push(1);
            cache_revision.extend_from_slice(&revision.len().to_be_bytes());
            cache_revision.extend_from_slice(&revision);
        }
        None => cache_revision.push(0),
    }
    cache_revision
}

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const V2_FORMAT_ONLY_METADATA_JSON: &str = r#"{"impact":"format"}"#;

fn v2_format_only_metadata() -> TransactionJson {
    TransactionJson::from_certified_shared_normalized_metadata(SharedStr::from_static(
        V2_FORMAT_ONLY_METADATA_JSON,
    ))
}

fn v2_file_descriptor(
    write: &TransactionFileData,
    plugin: &PluginRegistryEntry,
) -> WasmFileDescriptor {
    WasmFileDescriptor {
        path: write.path.clone(),
        media_type: inferred_media_type_for_path(write.path.as_deref()).map(str::to_owned),
        plugin: WasmPluginSelection {
            plugin_key: plugin.key().to_string(),
            generation: plugin.archive_blob_hash().to_string(),
        },
    }
}

fn v2_file_descriptor_from_actor_key(key: &PluginActorKey) -> WasmFileDescriptor {
    WasmFileDescriptor {
        path: Some(key.path.clone()),
        media_type: inferred_media_type_for_path(Some(&key.path)).map(str::to_owned),
        plugin: WasmPluginSelection {
            plugin_key: key.plugin_key.clone(),
            generation: key.plugin_generation.clone(),
        },
    }
}

fn v2_actor_key_is_descriptor_successor(
    observed: &PluginActorKey,
    desired: &PluginActorKey,
) -> bool {
    observed.branch_id == desired.branch_id
        && observed.file_id == desired.file_id
        && observed.owner_change_id == desired.owner_change_id
        && observed.plugin_key == desired.plugin_key
        && observed.plugin_generation == desired.plugin_generation
}

#[cfg(test)]
fn v2_create_context(seed: [u8; 16], actor_key: &PluginActorKey) -> crate::wasm::WasmCreateContext {
    BoundCreateContext::bind(local_mutation_identity(seed), actor_key)
        .expect("local mutation seeds are generated as UUIDv7")
        .creates()
}

fn suppress_v2_format_only_noops_against_batch(
    changes: WasmHostEntityChanges,
    keys: &[WasmEntityKey],
    accepted: &MaterializedLiveStateExactBatch,
) -> Result<WasmHostEntityChanges, LixError> {
    if keys.len() != accepted.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "format-only lookup returned the wrong cardinality",
        ));
    }
    let accepted = keys
        .iter()
        .enumerate()
        .map(|(slot, key)| (key, accepted.row(slot)))
        .collect::<BTreeMap<_, _>>();
    let mut effective = Vec::with_capacity(changes.changes.len());
    for change in changes.changes {
        let is_noop = match &change {
            WasmEntityChange::Upsert {
                entity,
                effect: WasmChangeEffect::FormatOnly,
            } => {
                let Some(Some(base)) = accepted.get(&entity.key) else {
                    effective.push(change);
                    continue;
                };
                let Some(base_snapshot) = base.snapshot_content() else {
                    effective.push(change);
                    continue;
                };
                let canonical = match &entity.snapshot_content {
                    WasmHostBytes::CanonicalJson(json) => json,
                    WasmHostBytes::Inline(_) | WasmHostBytes::Source(_) => {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "validated v2 guest changes must own parsed canonical snapshots",
                        ));
                    }
                };
                let candidate = canonical.normalized();
                if candidate == base_snapshot.as_str() {
                    true
                } else {
                    // New writes retain exact canonical bytes, so equality is
                    // normally one slice comparison. Historical rows may use
                    // a different key order or equivalent escape spelling;
                    // canonicalize that base exactly once only on mismatch.
                    candidate.as_bytes()
                        == canonicalize_v2_snapshot(base_snapshot.as_bytes())?.as_slice()
                }
            }
            WasmEntityChange::Create { .. }
            | WasmEntityChange::Upsert { .. }
            | WasmEntityChange::Delete(_) => false,
        };
        if !is_noop {
            effective.push(change);
        }
    }
    Ok(WasmHostEntityChanges { changes: effective })
}

fn append_plugin_change_rows_from_v2(
    rows: &mut RawWriteBatch,
    plugin: &PluginRegistryEntry,
    changes: WasmHostEntityChanges,
    file_id: &str,
    context: &FilesystemRowContext,
) -> Result<(), LixError> {
    let allowed_schema_keys = plugin
        .schema_keys()
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    rows.reserve(changes.entity_change_count());
    let mut interned_schema_keys = BTreeMap::<SharedStr, SharedStr>::new();
    let file_id = SharedStr::from(file_id);
    let branch_id = SharedStr::from(context.branch_id.as_str());
    // Format-only is a typed guest effect, so its exact engine metadata is
    // already known-valid canonical JSON. Retain one static byte owner for the
    // complete batch instead of parsing and serializing the same DOM per row.
    let format_only_metadata = v2_format_only_metadata();
    for change in changes.changes {
        let (key, snapshot_content, effect) = match change {
            WasmEntityChange::Create { .. } => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "keyless create was not materialized before transaction staging",
                ));
            }
            WasmEntityChange::Delete(key) => (key, None, WasmChangeEffect::Content),
            WasmEntityChange::Upsert { entity, effect } => {
                let snapshot = match entity.snapshot_content {
                    WasmHostBytes::CanonicalJson(json) => {
                        TransactionJson::from_canonical_batch(json)
                    }
                    WasmHostBytes::Inline(_) | WasmHostBytes::Source(_) => {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "validated v2 guest changes must own parsed canonical snapshots",
                        ));
                    }
                };
                (entity.key, Some(snapshot), effect)
            }
        };
        let entity_pk = plugin_entity_pk(plugin, &key)?;
        let schema_key = interned_schema_keys
            .entry(key.schema_key)
            .or_insert_with_key(|key| key.as_str().into())
            .clone();
        if !allowed_schema_keys.contains(schema_key.as_str()) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "plugin '{}' emitted schema key '{}' that is not declared in its manifest",
                    plugin.key(),
                    schema_key
                ),
            ));
        }
        rows.push_parts(
            Some(entity_pk),
            schema_key,
            Some(file_id.clone()),
            snapshot_content,
            (effect == WasmChangeEffect::FormatOnly).then(|| format_only_metadata.clone()),
            None,
            None,
            None,
            context.global,
            None,
            None,
            context.untracked,
            branch_id.clone(),
        );
    }
    Ok(())
}

fn plugin_entity_pk(
    plugin: &PluginRegistryEntry,
    key: &WasmEntityKey,
) -> Result<EntityPk, LixError> {
    let result = if plugin
        .create_schema_keys()
        .binary_search_by(|candidate| candidate.as_str().cmp(key.schema_key.as_str()))
        .is_ok()
    {
        let [id] = key.entity_pk.as_slice() else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "creatable schema '{}' requires one UUID primary-key component",
                    key.schema_key
                ),
            ));
        };
        EntityPk::uuid_from_canonical(id)
    } else {
        EntityPk::from_shared_parts(key.entity_pk.iter().cloned())
    };
    result.map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("v2 plugin emitted invalid entity_pk: {error}"),
        )
    })
}

fn v2_host_entities_from_live_batch_ordinals(
    rows: &MaterializedLiveStateBatch,
    ordinals: &[u32],
    limits: WasmTransitionLimits,
) -> Result<Vec<WasmHostEntity>, LixError> {
    let mut entities = Vec::with_capacity(ordinals.len());
    for ordinal in ordinals {
        let row = rows.get(*ordinal as usize).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin state selection references a row outside its batch owner",
            )
        })?;
        let Some(snapshot_content) = row.snapshot_content() else {
            continue;
        };
        entities.push(host_entity_with_lazy_snapshot(
            WasmEntityKey::from_owned_parts(
                row.schema_key().to_owned(),
                row.entity_pk().clone().into_parts(),
            ),
            snapshot_content.clone().into_bytes(),
            limits,
        )?);
    }
    entities.sort_by(|left, right| left.key.cmp(&right.key));
    for pair in entities.windows(2) {
        if pair[0].key == pair[1].key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "durable v2 entity hydration returned duplicate keys",
            ));
        }
    }
    Ok(entities)
}

fn v2_host_entity_ordinals_from_live_batch(
    rows: &MaterializedLiveStateBatch,
    file_key: &PluginFileWriteKey,
    schema_keys: &[String],
) -> Result<Vec<u32>, LixError> {
    let mut ordinals = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row.branch_id() == file_key.branch_id
                && row.file_id() == Some(file_key.file_id.as_str())
                && !row.global()
                && !row.untracked()
                && row.snapshot_content().is_some()
                && schema_keys
                    .binary_search_by(|schema_key| schema_key.as_str().cmp(row.schema_key()))
                    .is_ok()
        })
        .map(|(ordinal, _)| {
            u32::try_from(ordinal).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "durable plugin state exceeds u32 row ordinals",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    ordinals.sort_unstable_by(|left, right| {
        let left = rows.row(*left as usize);
        let right = rows.row(*right as usize);
        left.schema_key()
            .cmp(right.schema_key())
            .then_with(|| left.entity_pk().cmp(right.entity_pk()))
    });
    for pair in ordinals.windows(2) {
        let left = rows.row(pair[0] as usize);
        let right = rows.row(pair[1] as usize);
        if left.schema_key() == right.schema_key() && left.entity_pk() == right.entity_pk() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "durable v2 entity hydration returned duplicate keys",
            ));
        }
    }
    Ok(ordinals)
}

fn v2_host_changes_from_prepared_rows(
    rows: &PreparedStateBatch,
    limits: WasmTransitionLimits,
) -> Result<WasmHostEntityChanges, LixError> {
    let mut changes = rows
        .iter()
        .map(|row| {
            if row.global || row.untracked || row.file_id.is_none() {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "v2 semantic rendering requires tracked, branch-local, file-scoped rows",
                ));
            }
            let key = WasmEntityKey::from_owned_parts(
                row.schema_key.clone(),
                row.entity_pk.clone().into_parts(),
            );
            match row.snapshot {
                Some(snapshot) => {
                    let format_only = row.metadata.is_some_and(|metadata| {
                        metadata.normalized() == V2_FORMAT_ONLY_METADATA_JSON
                    });
                    let effect = if format_only {
                        WasmChangeEffect::FormatOnly
                    } else {
                        WasmChangeEffect::Content
                    };
                    host_entity_change_with_lazy_snapshot(
                        key,
                        snapshot.materialize_shared().into_bytes().into(),
                        effect,
                        limits,
                    )
                }
                None => Ok(WasmEntityChange::Delete(key)),
            }
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    changes.sort_by(|left, right| left.entity_key().cmp(&right.entity_key()));
    for pair in changes.windows(2) {
        if pair[0].entity_key() == pair[1].entity_key() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "one v2 semantic write batch cannot contain the same entity key more than once",
            ));
        }
    }
    Ok(WasmHostEntityChanges { changes })
}

fn reject_external_plugin_registry_rows(rows: &RawWriteBatch) -> Result<(), LixError> {
    for row in rows {
        if row.schema_key != KEY_VALUE_SCHEMA_KEY {
            continue;
        }
        let entity_key = row
            .entity_pk
            .and_then(|entity_pk| entity_pk.as_single_string().ok());
        let snapshot_key = row
            .snapshot
            .and_then(|snapshot| snapshot.get("key"))
            .and_then(JsonValue::as_str);
        let reserved = [entity_key, snapshot_key]
            .into_iter()
            .flatten()
            .find(|key| {
                matches!(*key, PLUGIN_REGISTRY_KEY | PLUGIN_OWNER_KEY) || is_reservation_key(key)
            });
        if let Some(key) = reserved {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!("'{key}' is reserved for engine-managed plugin state"),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginFileWriteKey {
    branch_id: String,
    global: bool,
    untracked: bool,
    file_id: String,
}

impl PluginFileWriteKey {
    fn matches_blob_ref_row(&self, row: RawWriteRowRef<'_>) -> bool {
        row.schema_key == BLOB_REF_SCHEMA_KEY
            && row.branch_id.as_str() == self.branch_id
            && row.global == self.global
            && row.untracked == self.untracked
            && row.file_id.map(SharedStr::as_str) == Some(self.file_id.as_str())
    }

    fn matches_derived_file_ref_row(&self, row: RawWriteRowRef<'_>) -> bool {
        row.schema_key == DERIVED_FILE_REF_SCHEMA_KEY
            && row.branch_id.as_str() == self.branch_id
            && row.global == self.global
            && row.untracked == self.untracked
            && row.file_id.map(SharedStr::as_str) == Some(self.file_id.as_str())
    }

    fn matches_materialization_row(&self, row: RawWriteRowRef<'_>) -> bool {
        self.matches_blob_ref_row(row) || self.matches_derived_file_ref_row(row)
    }
}

impl From<&TransactionFileData> for PluginFileWriteKey {
    fn from(write: &TransactionFileData) -> Self {
        Self {
            branch_id: write.branch_id.clone(),
            global: write.global,
            untracked: write.untracked,
            file_id: write.file_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct DerivedMaterializationProof {
    path: String,
    sha256: FileBytesSha256,
    size_bytes: usize,
}

impl DerivedMaterializationProof {
    fn from_bytes(bytes: &[u8], path: String) -> Self {
        Self {
            path,
            sha256: FileBytesSha256::compute(bytes),
            size_bytes: bytes.len(),
        }
    }
}

/// Internal write shape after plugin reconciliation.
///
/// The public transaction boundary remains row-oriented, but semantic plugin
/// sources can be prepared before the renderer runs. This typed stage keeps
/// those exact prepared rows in their batch positions without manufacturing an
/// invalid `TransactionWriteRow` sentinel or serializing a row fingerprint.
enum ReconciledTransactionWrite {
    Rows {
        mode: TransactionWriteMode,
        rows: ReconciledRowBatch,
    },
    RowsWithFileData {
        mode: TransactionWriteMode,
        rows: ReconciledRowBatch,
        file_data: Vec<TransactionFileData>,
        count: u64,
    },
}

#[derive(Debug)]
enum ReconciledRowBatch {
    /// The allocation-preserving path for ordinary SQL and file writes.
    Raw(RawWriteBatch),
    /// Promoted only when semantic rows have already been supplied to a plugin
    /// renderer. Slot order is the original transaction row order.
    Mixed(ReconciledMixedBatch),
}

#[derive(Debug)]
struct ReconciledMixedBatch {
    slots: Vec<ReconciledRowSlot>,
    raw: RawWriteBatch,
    prepared: PreparedStateBatch,
}

#[derive(Debug, Clone, Copy)]
enum ReconciledRowSlot {
    Raw(u32),
    Prepared(u32),
    /// Transient ownership marker while one semantic group is being prepared
    /// and rendered. A completed reconciliation must not expose this variant.
    Extracted,
}

impl ReconciledRowBatch {
    fn raw(rows: RawWriteBatch) -> Self {
        Self::Raw(rows)
    }

    fn promote_raw_rows(rows: &mut RawWriteBatch) -> Self {
        Self::from_raw_slots(std::mem::take(rows))
    }

    fn from_raw_slots(rows: RawWriteBatch) -> Self {
        let mut slots = Vec::with_capacity(rows.len());
        slots.extend((0..rows.len()).map(|ordinal| {
            ReconciledRowSlot::Raw(
                u32::try_from(ordinal).expect("reconciled raw ordinal must fit u32"),
            )
        }));
        Self::Mixed(ReconciledMixedBatch {
            slots,
            raw: rows,
            prepared: PreparedStateBatch::new(),
        })
    }

    fn promote(&mut self) -> &mut ReconciledMixedBatch {
        if let Self::Raw(rows) = self {
            let rows = std::mem::take(rows);
            let mut slots = Vec::with_capacity(rows.len());
            slots.extend((0..rows.len()).map(|ordinal| {
                ReconciledRowSlot::Raw(
                    u32::try_from(ordinal).expect("reconciled raw ordinal must fit u32"),
                )
            }));
            *self = Self::Mixed(ReconciledMixedBatch {
                slots,
                raw: rows,
                prepared: PreparedStateBatch::new(),
            });
        }
        let Self::Mixed(mixed) = self else {
            unreachable!("raw reconciled rows were just promoted");
        };
        mixed
    }

    fn take_raw_rows_at(&mut self, source_indices: &[usize]) -> Result<RawWriteBatch, LixError> {
        let mixed = self.promote();
        let mut raw_ordinals = Vec::with_capacity(source_indices.len());
        for &source_index in source_indices {
            let slot = mixed.slots.get_mut(source_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("semantic row index {source_index} is outside the reconciled batch"),
                )
            })?;
            let ReconciledRowSlot::Raw(ordinal) = *slot else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "semantic row at batch index {source_index} was extracted more than once"
                    ),
                ));
            };
            raw_ordinals.push(ordinal as usize);
            *slot = ReconciledRowSlot::Extracted;
        }
        Ok(mixed.raw.take_rows(&raw_ordinals))
    }

    fn put_prepared_batch_at(
        &mut self,
        source_indices: &[usize],
        prepared: PreparedStateBatch,
    ) -> Result<(), LixError> {
        if source_indices.len() != prepared.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "semantic preparation changed the reconciled row count",
            ));
        }
        let mixed = self.promote();
        let prepared_base = mixed.prepared.len();
        for (prepared_index, &source_index) in source_indices.iter().enumerate() {
            let slot = mixed.slots.get_mut(source_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("semantic row index {source_index} is outside the reconciled batch"),
                )
            })?;
            if !matches!(slot, ReconciledRowSlot::Extracted) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "prepared semantic row at batch index {source_index} has no extracted source"
                    ),
                ));
            }
            *slot = ReconciledRowSlot::Prepared(
                u32::try_from(prepared_base + prepared_index)
                    .expect("prepared reconciled ordinal must fit u32"),
            );
        }
        mixed.prepared.append(prepared);
        Ok(())
    }

    fn push_raw(&mut self, row: TransactionWriteRow) {
        match self {
            Self::Raw(rows) => rows.push(row),
            Self::Mixed(mixed) => {
                let ordinal =
                    u32::try_from(mixed.raw.len()).expect("reconciled raw ordinal must fit u32");
                mixed.raw.push(row);
                mixed.slots.push(ReconciledRowSlot::Raw(ordinal));
            }
        }
    }

    fn append_raw_batch(&mut self, rows: RawWriteBatch) {
        match self {
            Self::Raw(batch) => batch.append(rows),
            Self::Mixed(mixed) => {
                let additional = rows.len();
                let base = mixed.raw.len();
                mixed.raw.reserve(additional);
                mixed.slots.reserve(additional);
                mixed.raw.append(rows);
                for ordinal in base..base + additional {
                    mixed.slots.push(ReconciledRowSlot::Raw(
                        u32::try_from(ordinal).expect("reconciled raw ordinal must fit u32"),
                    ));
                }
            }
        }
    }

    fn retain_raw(
        &mut self,
        mut retain: impl FnMut(RawWriteRowRef<'_>) -> bool,
    ) -> Result<(), LixError> {
        match self {
            Self::Raw(rows) => rows.retain(|row| retain(row)),
            Self::Mixed(mixed) => {
                if mixed
                    .slots
                    .iter()
                    .any(|slot| matches!(slot, ReconciledRowSlot::Extracted))
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "reconciled row retention observed an unfilled semantic slot",
                    ));
                }
                mixed.slots.retain(|slot| match *slot {
                    ReconciledRowSlot::Raw(ordinal) => retain(mixed.raw.row(ordinal as usize)),
                    ReconciledRowSlot::Prepared(_) => true,
                    ReconciledRowSlot::Extracted => {
                        unreachable!("extracted semantic slots were rejected before retention")
                    }
                });
            }
        }
        Ok(())
    }

    fn mark_plugin_reconciliation_rows_from(
        &mut self,
        source_index: usize,
    ) -> Result<(), LixError> {
        let origin = plugin_reconciliation_origin();
        match self {
            Self::Raw(rows) => {
                if source_index > rows.len() {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin reconciliation row boundary exceeds the raw batch",
                    ));
                }
                for index in source_index..rows.len() {
                    rows.set_origin(index, Some(origin.clone()));
                }
            }
            Self::Mixed(mixed) => {
                let slots = mixed.slots.get_mut(source_index..).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin reconciliation row boundary exceeds the mixed batch",
                    )
                })?;
                for slot in slots {
                    match *slot {
                        ReconciledRowSlot::Raw(ordinal) => {
                            mixed.raw.set_origin(ordinal as usize, Some(origin.clone()));
                        }
                        ReconciledRowSlot::Prepared(_) => {}
                        ReconciledRowSlot::Extracted => {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin reconciliation completed with an unfilled semantic slot",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn require_valid_storage_scopes(&self) -> Result<(), LixError> {
        match self {
            Self::Raw(rows) => require_valid_transaction_write_row_storage_scopes(rows),
            Self::Mixed(mixed) => {
                for slot in &mixed.slots {
                    match *slot {
                        ReconciledRowSlot::Raw(ordinal) => {
                            let row = mixed.raw.row(ordinal as usize);
                            require_valid_storage_scope(row.branch_id.as_str(), row.global)?;
                        }
                        ReconciledRowSlot::Prepared(ordinal) => {
                            let row = mixed.prepared.row(ordinal as usize);
                            require_valid_storage_scope(row.branch_id.as_str(), row.global)?;
                        }
                        ReconciledRowSlot::Extracted => {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "reconciled write exposed an unfilled semantic slot",
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Default)]
struct PluginWriteReconciliation {
    file_keys: BTreeSet<PluginFileWriteKey>,
    materialized_file_keys: BTreeSet<PluginFileWriteKey>,
    materialization_versions: BTreeMap<PluginFileWriteKey, String>,
    derived_materializations: BTreeMap<PluginFileWriteKey, DerivedMaterializationProof>,
    file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    actor_publications: Vec<PendingPluginActorPublication>,
    reconciled_rows: Option<ReconciledRowBatch>,
}

struct PreparedFreshPluginOpen {
    file_index: usize,
    file_key: PluginFileWriteKey,
    selected: PluginRegistryEntry,
    owner_row: TransactionWriteRow,
    actor_key: PluginActorKey,
    view: PendingPluginActorView,
    materialization_version: String,
    submitted_bytes: crate::Blob,
    create_context: BoundCreateContext,
    existing_create_reservation: Option<MaterializedLiveStateRow>,
    factory: Arc<dyn WasmComponentV2Factory>,
    descriptor: WasmFileDescriptor,
    schemas: V2SchemaAllowlist,
    cold_limits: WasmTransitionLimits,
}

struct PendingFreshPluginOpen {
    file_index: usize,
    file_key: PluginFileWriteKey,
    selected: PluginRegistryEntry,
    owner_row: TransactionWriteRow,
    actor_key: PluginActorKey,
    view: PendingPluginActorView,
    materialization_version: String,
    submitted_bytes: crate::Blob,
    create_context: BoundCreateContext,
    existing_create_reservation: Option<MaterializedLiveStateRow>,
    store_permit: PluginActorStorePermit,
    task: Option<
        tokio::task::JoinHandle<
            Result<(Box<dyn WasmComponentV2Actor>, ValidatedFileTransition), LixError>,
        >,
    >,
}

impl PluginWriteReconciliation {
    fn remove_session_file_view(&mut self, key: SessionFileViewKey) {
        self.file_view_mutations
            .insert(key.clone(), SessionFileViewMutation::Remove { key });
    }

    fn take_reconciled_rows(&mut self, raw_rows: RawWriteBatch) -> ReconciledRowBatch {
        self.reconciled_rows
            .take()
            .unwrap_or_else(|| ReconciledRowBatch::raw(raw_rows))
    }
}

struct PendingPluginActorView {
    session_key: SessionFileViewKey,
    plugin_key: String,
    plugin_generation: String,
    owner_change_id: String,
    semantic_chainable: bool,
    retain_large_import_actor: bool,
}

enum PendingPluginActorPublication {
    Existing {
        lease: PluginActorLease,
        successor_key: PluginActorKey,
        view: PendingPluginActorView,
    },
    New {
        cache: PluginActorCache,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: crate::Blob,
        semantic_root: Arc<str>,
        view: PendingPluginActorView,
    },
    /// The plugin transition has already produced its durable rows, but the
    /// bounded working set does not keep its private Wasm Store alive.
    /// Keeping this marker preserves the normal one-transition-per-file
    /// validation and gives the session a non-authoritative file view after
    /// commit, forcing a cold open before any later edit.
    Uncached {
        path: String,
        view: PendingPluginActorView,
    },
}

impl PendingPluginActorPublication {
    async fn into_uncached(self) -> Self {
        match self {
            Self::Existing {
                lease,
                successor_key,
                view,
            } => {
                let _ = lease.discard_successor().await;
                Self::Uncached {
                    path: successor_key.path,
                    view,
                }
            }
            Self::New {
                mut store,
                key,
                document,
                view,
                ..
            } => {
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
                Self::Uncached {
                    path: key.path,
                    view,
                }
            }
            publication @ Self::Uncached { .. } => publication,
        }
    }

    async fn discard(self) {
        match self {
            Self::Existing { lease, .. } => {
                let _ = lease.discard_successor().await;
            }
            Self::New {
                mut store,
                document,
                ..
            } => {
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
            }
            Self::Uncached { .. } => {}
        }
    }

    async fn publish(self) -> Result<(SessionFileViewKey, SessionPluginFileView), LixError> {
        let (observation, view, path) = match self {
            Self::Existing {
                lease,
                successor_key,
                view,
            } => {
                let path = successor_key.path.clone();
                (
                    Some(lease.commit_successor_as(successor_key).await?),
                    view,
                    path,
                )
            }
            Self::New {
                cache,
                key,
                store,
                document,
                bytes,
                semantic_root,
                view,
            } => {
                let path = key.path.clone();
                (
                    Some(cache.install(key, store, document, bytes, semantic_root)),
                    view,
                    path,
                )
            }
            Self::Uncached { path, view } => (None, view, path),
        };
        Ok((
            view.session_key,
            SessionPluginFileView {
                path,
                plugin_key: view.plugin_key,
                plugin_generation: view.plugin_generation,
                owner_change_id: view.owner_change_id,
                observation,
            },
        ))
    }

    fn session_key(&self) -> &SessionFileViewKey {
        match self {
            Self::Existing { view, .. } | Self::New { view, .. } | Self::Uncached { view, .. } => {
                &view.session_key
            }
        }
    }

    fn retains_large_import_actor(&self) -> bool {
        match self {
            Self::Existing { view, .. } | Self::New { view, .. } | Self::Uncached { view, .. } => {
                view.retain_large_import_actor
            }
        }
    }
}

/// Releases the oldest completed Store retained only for post-commit cache
/// publication. Existing-file successors keep their durable staged rows but
/// become uncached. Dropping their leases makes the predecessor slot evictable
/// only when no concurrent transition references it; an active or waiting
/// same-file lease therefore still preserves serialization.
async fn retire_oldest_completed_actor(
    publications: &mut Vec<PendingPluginActorPublication>,
) -> bool {
    let Some(index) = publications.iter().position(|publication| {
        matches!(
            publication,
            PendingPluginActorPublication::Existing { .. }
                | PendingPluginActorPublication::New { .. }
        )
    }) else {
        return false;
    };
    let publication = publications.remove(index).into_uncached().await;
    publications.insert(index, publication);
    true
}

/// Builds the file write for an accepted semantic renderer transition.
///
/// Builds the blob-backed file write for an accepted semantic renderer
/// transition. Derived materializations deliberately bypass this path: they
/// retain only their rendered-byte proof and never stage a CAS payload.
fn semantic_rendered_file_data(
    file_id: String,
    path: String,
    filename: String,
    branch_id: String,
    base_blob_hash: BlobHash,
    rendered_bytes: crate::Blob,
    same_length_output_splice: Option<ValidatedSameLengthOutputSplice>,
) -> TransactionFileData {
    let mut rendered_file = TransactionFileData::new(
        file_id,
        Some(path),
        Some(filename),
        branch_id,
        false,
        false,
        rendered_bytes,
    )
    .with_had_blob_ref(true)
    .with_base_blob_hash(Some(base_blob_hash));
    if let Some(splice) = same_length_output_splice {
        rendered_file.set_verified_same_length_blob_splice(
            base_blob_hash,
            splice.offset,
            splice.length,
        );
    }
    rendered_file
}

async fn render_v2_semantic_changes_with_lease(
    mut lease: PluginActorLease,
    successor_key: PluginActorKey,
    view: PendingPluginActorView,
    descriptor: WasmFileDescriptor,
    changes: WasmHostEntityChanges,
    visible_root: &str,
    materialization_version: &str,
    limits: WasmTransitionLimits,
) -> Result<
    (
        PendingPluginActorPublication,
        crate::Blob,
        Option<ValidatedSameLengthOutputSplice>,
        crate::wasm::WasmTransitionCounters,
    ),
    (LixError, PendingPluginActorPublication),
> {
    let publication = |lease| PendingPluginActorPublication::Existing {
        lease,
        successor_key: successor_key.clone(),
        view: PendingPluginActorView {
            session_key: view.session_key.clone(),
            plugin_key: view.plugin_key.clone(),
            plugin_generation: view.plugin_generation.clone(),
            owner_change_id: view.owner_change_id.clone(),
            semantic_chainable: view.semantic_chainable,
            retain_large_import_actor: view.retain_large_import_actor,
        },
    };
    let change_count = u64::try_from(changes.entity_change_count()).unwrap_or(u64::MAX);
    let change_source = match VecEntityChangeSource::new(changes, limits) {
        Ok(source) => source,
        Err(error) => return Err((error, publication(lease))),
    };
    let call = match lease.begin_pending_guest_call() {
        Ok(call) => call,
        Err(error) => return Err((error, publication(lease))),
    };
    if call.semantic_root() != visible_root {
        let error = LixError::new(
            LixError::CODE_PLUGIN_OBSERVATION_STALE,
            "v2 semantic write base no longer matches visible semantic state",
        );
        let error = lease.handle_pending_guest_call_error(call, error);
        return Err((error, publication(lease)));
    }
    let base_document = call.document();
    let base_bytes = call.bytes();
    let renderer_input = match lease.actor_mut().fork_document(base_document).await {
        Ok(document) => document,
        Err(error) => {
            let error = lease.handle_pending_guest_call_error(call, error);
            return Err((error, publication(lease)));
        }
    };
    let renderer_transition = match lease
        .actor_mut()
        .entities_changed(
            renderer_input,
            limits,
            WasmEntityUpdate {
                before_descriptor: descriptor.clone(),
                after_descriptor: descriptor,
                before: Arc::new(ArcByteSource::new(base_bytes.clone())),
                changes: Box::new(change_source),
            },
        )
        .await
    {
        Ok(transition) => transition,
        Err(error) => {
            let error = lease.handle_pending_guest_call_error(call, error);
            return Err((error, publication(lease)));
        }
    };
    let rendered = match drain_entity_transition_edits(
        lease.actor_mut(),
        renderer_transition,
        &base_bytes,
        None,
        None,
        limits,
    )
    .await
    {
        Ok(rendered) => rendered,
        Err(error) => {
            let error = lease.handle_pending_guest_call_error(call, error);
            return Err((error, publication(lease)));
        }
    };
    if let Err(error) = lease.actor_mut().drop_document(renderer_input).await {
        let error = lease.handle_pending_guest_call_error(call, error);
        return Err((error, publication(lease)));
    }
    let rendered_bytes = rendered.bytes.clone();
    let same_length_output_splice = rendered.same_length_output_splice;
    let mut counters = rendered.counters;
    counters.private_document_cache_hits = 1;
    counters.durable_semantic_changes = change_count;
    if let Err(error) = lease
        .complete_pending_guest_call(
            call,
            rendered.document,
            rendered.bytes,
            rendered.bytes_sha256,
            materialization_version.to_string(),
        )
        .await
    {
        return Err((error, publication(lease)));
    }
    Ok((
        publication(lease),
        rendered_bytes,
        same_length_output_splice,
        counters,
    ))
}

async fn discard_plugin_actor_publications(publications: Vec<PendingPluginActorPublication>) {
    for publication in publications {
        publication.discard().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginLifecycleKey {
    branch_id: String,
    plugin_key: String,
}

#[derive(Debug, Clone)]
struct PluginGenerationUpgrade {
    branch_id: String,
    previous: PluginRegistryEntry,
    replacement: PluginRegistryEntry,
}

#[derive(Debug, serde::Deserialize)]
struct PluginUpgradeBlobRefSnapshot {
    id: String,
    blob_hash: String,
}

#[derive(Debug, serde::Deserialize)]
struct DerivedFileRefSnapshot {
    id: String,
    path: String,
    sha256: String,
    size_bytes: u64,
}

/// A derived file has no raw-CAS fallback: its durable semantic rows and proof
/// can be rendered only through the generation named by its owner. Refuse to
/// remove that generation while any live owner still names it. Descriptor
/// deletes in this same transaction are allowed because their regular cleanup
/// removes the owner and semantic state before commit.
async fn preflight_derived_plugin_uninstalls(
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl StagedLiveStateRows,
    uninstalls: &BTreeMap<String, BTreeSet<String>>,
    deleted_file_keys: &BTreeMap<PluginFileWriteKey, Option<TransactionJson>>,
) -> Result<(), LixError> {
    let owner_rows = overlay_scan_batch(
        base,
        staged,
        &LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                entity_pks: vec![EntityPk::single(PLUGIN_OWNER_KEY)],
                branch_ids: uninstalls.keys().cloned().collect(),
                untracked: Some(false),
                ..Default::default()
            },
            projection: plugin_registry_live_state_projection(),
            ..Default::default()
        },
    )
    .await?;

    for row in owner_rows.iter() {
        let branch_id = row.branch_id().to_string();
        let Some(uninstalled_plugins) = uninstalls.get(&branch_id) else {
            continue;
        };
        let owner_row = row.to_owned();
        let Some(owner) = PluginFileOwner::from_live_state_row(&owner_row, &branch_id)? else {
            continue;
        };
        if !uninstalled_plugins.contains(owner.plugin_key()) {
            continue;
        }
        let file_key = PluginFileWriteKey {
            branch_id: branch_id.clone(),
            global: false,
            untracked: false,
            file_id: owner.file_id().to_string(),
        };
        if deleted_file_keys.contains_key(&file_key) {
            continue;
        }
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "cannot uninstall derived-materialization plugin '{}' while file '{}' remains owned",
                owner.plugin_key(),
                owner.file_id(),
            ),
        )
        .with_hint(
            "Delete every owned file before uninstalling this plugin; its semantic rows require the plugin to render their bytes.",
        ));
    }

    Ok(())
}

/// Derived bytes are a function of both durable semantic rows and the file
/// descriptor supplied to the component. A path-only descriptor rewrite has
/// no semantic transition with which to update the proof, so reject it rather
/// than silently serving a changed (or absent) rendering. Relocation is a
/// delete-and-recreate operation, which gives the component a new explicit
/// semantic transition and proof.
async fn preflight_derived_path_stability(
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl StagedLiveStateRows,
    prospective: &impl StagedLiveStateRows,
    prospective_rows: &MaterializedLiveStateBatch,
) -> Result<(), LixError> {
    let mut final_descriptor_rows = prospective_rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            !row.global()
                && !row.untracked()
                && matches!(
                    row.schema_key(),
                    FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                )
        })
        .map(|(ordinal, _)| ordinal)
        .collect::<Vec<_>>();
    final_descriptor_rows.sort_unstable_by(|left, right| {
        let left_row = prospective_rows.row(*left);
        let right_row = prospective_rows.row(*right);
        (
            left_row.branch_id(),
            left_row.schema_key(),
            left_row.entity_pk(),
            left_row.file_id(),
            *left,
        )
            .cmp(&(
                right_row.branch_id(),
                right_row.schema_key(),
                right_row.entity_pk(),
                right_row.file_id(),
                *right,
            ))
    });
    // The final row for an identity wins when the prospective batch contains
    // multiple mutations, matching transaction staging. Reverse/deduplicate
    // keeps that row without allocating one owned map key per descriptor.
    final_descriptor_rows.reverse();
    final_descriptor_rows.dedup_by(|left, right| {
        let left = prospective_rows.row(*left);
        let right = prospective_rows.row(*right);
        left.branch_id() == right.branch_id()
            && left.schema_key() == right.schema_key()
            && left.entity_pk() == right.entity_pk()
            && left.file_id() == right.file_id()
    });
    final_descriptor_rows.reverse();
    if final_descriptor_rows.is_empty() {
        return Ok(());
    }
    let prior_descriptors = overlay_load_exact_batch(
        base,
        staged,
        &LiveStateExactBatchRequest {
            rows: final_descriptor_rows
                .iter()
                .map(|ordinal| {
                    let row = prospective_rows.row(*ordinal);
                    LiveStateExactRowRequest {
                        schema_key: row.schema_key().to_string(),
                        branch_id: row.branch_id().to_string(),
                        entity_pk: row.entity_pk().clone(),
                        file_id: row.file_id().map(str::to_owned),
                    }
                })
                .collect(),
            projection: plugin_registry_live_state_projection(),
            untracked: Some(false),
            include_tombstones: false,
        },
    )
    .await?;
    let mut moved_files = BTreeSet::<(String, String)>::new();
    let mut moved_directory_branches = BTreeSet::<String>::new();
    for (slot, ordinal) in final_descriptor_rows.iter().enumerate() {
        let next = prospective_rows.row(*ordinal);
        let Some(previous) = prior_descriptors.row(slot) else {
            continue;
        };
        if previous.deleted()
            || previous.snapshot_content().is_none()
            || next.deleted()
            || next.snapshot_content().is_none()
            || descriptor_parent_and_name(previous)? == descriptor_parent_and_name(next)?
        {
            continue;
        }
        let branch_id = next.branch_id().to_string();
        match next.schema_key() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let file_id = next.entity_pk().as_single_string_owned().map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("file descriptor path update has an invalid identity: {error}"),
                    )
                })?;
                moved_files.insert((branch_id, file_id));
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                moved_directory_branches.insert(branch_id);
            }
            _ => unreachable!("descriptor filter above is exhaustive"),
        }
    }
    preflight_derived_file_path_moves(base, staged, &moved_files).await?;
    if !moved_directory_branches.is_empty() {
        preflight_derived_directory_path_moves(
            base,
            staged,
            prospective,
            &moved_directory_branches,
        )
        .await?;
    }
    Ok(())
}

async fn preflight_derived_file_path_moves(
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl StagedLiveStateRows,
    moved_files: &BTreeSet<(String, String)>,
) -> Result<(), LixError> {
    if moved_files.is_empty() {
        return Ok(());
    }
    let proofs = overlay_load_exact_batch(
        base,
        staged,
        &LiveStateExactBatchRequest {
            rows: moved_files
                .iter()
                .map(|(branch_id, file_id)| {
                    Ok(LiveStateExactRowRequest {
                        schema_key: DERIVED_FILE_REF_SCHEMA_KEY.to_string(),
                        branch_id: branch_id.clone(),
                        entity_pk: validated_uuid_entity_pk(file_id)?,
                        file_id: Some(file_id.clone()),
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?,
            projection: plugin_registry_live_state_projection(),
            untracked: Some(false),
            include_tombstones: false,
        },
    )
    .await?;
    for (slot, (branch_id, file_id)) in moved_files.iter().enumerate() {
        let Some(proof) = proofs.row(slot) else {
            continue;
        };
        let VisibleV2MaterializationBytes::Derived { .. } =
            decode_visible_v2_materialization_ref(proof, file_id)?.bytes
        else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "derived materialization row for file '{file_id}' did not decode as a derived proof"
                ),
            ));
        };
        return Err(derived_path_move_error(file_id, branch_id, None, None));
    }
    Ok(())
}

async fn preflight_derived_directory_path_moves(
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl StagedLiveStateRows,
    prospective: &impl StagedLiveStateRows,
    branch_ids: &BTreeSet<String>,
) -> Result<(), LixError> {
    let request = FilesystemPathIndexRequest::new(branch_ids.iter().cloned().collect());
    let before_rows = overlay_scan_batch(base, staged, &request.live_state_request()).await?;
    let before = FilesystemPathIndex::from_live_batch(&before_rows)?;
    let after_rows = overlay_scan_batch(base, prospective, &request.live_state_request()).await?;
    let after = FilesystemPathIndex::from_live_batch(&after_rows)?;
    let proof_rows = overlay_scan_batch(
        base,
        staged,
        &LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![DERIVED_FILE_REF_SCHEMA_KEY.to_string()],
                branch_ids: branch_ids.iter().cloned().collect(),
                untracked: Some(false),
                ..Default::default()
            },
            projection: plugin_registry_live_state_projection(),
            ..Default::default()
        },
    )
    .await?;
    for row in proof_rows.iter() {
        if row.global() || row.untracked() || row.deleted() || row.snapshot_content().is_none() {
            continue;
        }
        let branch_id = row.branch_id().to_string();
        let file_id = row
            .file_id()
            .map(str::to_owned)
            .or_else(|| row.entity_pk().as_single_string_owned().ok())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "derived materialization proof is missing its file identity",
                )
            })?;
        let VisibleV2MaterializationBytes::Derived { .. } =
            decode_visible_v2_materialization_ref(row, &file_id)?.bytes
        else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "derived materialization row for file '{file_id}' did not decode as a derived proof"
                ),
            ));
        };
        let before_path = derived_file_path_for_branch(&before, &branch_id, &file_id)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "derived-materialization file '{file_id}' has no tracked descriptor before its path update"
                ),
            )
        })?;
        let Some(after_path) = derived_file_path_for_branch(&after, &branch_id, &file_id)? else {
            continue;
        };
        if before_path != after_path {
            return Err(derived_path_move_error(
                &file_id,
                &branch_id,
                Some(&before_path),
                Some(&after_path),
            ));
        }
    }
    Ok(())
}

fn descriptor_parent_and_name(
    row: MaterializedLiveStateRowRef<'_>,
) -> Result<(Option<String>, String), LixError> {
    let snapshot = row
        .snapshot_content()
        .map(|content| content.as_str())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                "path descriptor is missing its snapshot",
            )
        })?;
    match row.schema_key() {
        FILE_DESCRIPTOR_SCHEMA_KEY => {
            let snapshot: PathFileDescriptorSnapshot =
                serde_json::from_str(snapshot).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("invalid lix_file_descriptor path snapshot: {error}"),
                    )
                })?;
            Ok((snapshot.directory_id, snapshot.name))
        }
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            let snapshot: PathDirectoryDescriptorSnapshot = serde_json::from_str(snapshot)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("invalid lix_directory_descriptor path snapshot: {error}"),
                    )
                })?;
            Ok((snapshot.parent_id, snapshot.name))
        }
        schema_key => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("path comparison received non-descriptor schema '{schema_key}'"),
        )),
    }
}

fn derived_path_move_error(
    file_id: &str,
    branch_id: &str,
    before_path: Option<&str>,
    after_path: Option<&str>,
) -> LixError {
    let message = match (before_path, after_path) {
        (Some(before_path), Some(after_path)) => format!(
            "cannot move derived-materialization file '{file_id}' from '{before_path}' to '{after_path}' without a semantic relocation transition"
        ),
        _ => format!(
            "cannot move derived-materialization file '{file_id}' on branch '{branch_id}' without a semantic relocation transition"
        ),
    };
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message).with_hint(
        "Delete the file, then recreate it at its destination so the plugin can publish a new derived proof.",
    )
}

#[derive(Debug, serde::Deserialize)]
struct PathFileDescriptorSnapshot {
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct PathDirectoryDescriptorSnapshot {
    parent_id: Option<String>,
    name: String,
}

fn derived_file_path_for_branch(
    index: &FilesystemPathIndex,
    branch_id: &str,
    file_id: &str,
) -> Result<Option<String>, LixError> {
    let matches = index
        .exact_file_id_entries(file_id)
        .into_iter()
        .filter(|entry| {
            entry.kind == FilesystemPathKind::File
                && entry.key.branch_id() == branch_id
                && !entry.key.global()
                && !entry.key.is_untracked()
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [entry] => Ok(Some(entry.path.clone())),
        _ => Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "derived-materialization file '{file_id}' resolves to {} tracked descriptors on branch '{branch_id}'",
                matches.len()
            ),
        )),
    }
}

/// Read-only composition of the already-staged transaction overlay and one
/// prepared batch that has not yet been admitted to it. The current batch is
/// appended last so its identities win exactly as they would in
/// `TransactionWriteBuffer::stage_write`, without making an error irreversible.
struct ProspectiveStagedRows {
    staged: PreparedStateRowOverlay,
    rows: MaterializedLiveStateBatch,
}

impl StagedLiveStateRows for ProspectiveStagedRows {
    fn staged_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let staged = self.staged.staged_batch(request)?;
        let prospective_count = self
            .rows
            .iter()
            .filter(|row| prospective_row_matches_scan(*row, request))
            .count();
        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(
            staged
                .len()
                .checked_add(prospective_count)
                .expect("prospective staged row count overflow"),
        );
        for row in staged.iter() {
            rows.push_ref(row, None);
        }
        for row in self
            .rows
            .iter()
            .filter(|row| prospective_row_matches_scan(*row, request))
        {
            rows.push_ref(row, None);
        }
        Ok(rows.finish())
    }

    fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let staged = self.staged.load_exact_batch(request)?;
        if staged.len() != request.rows.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "prospective staged exact read expected {} base slots, got {}",
                    request.rows.len(),
                    staged.len()
                ),
            ));
        }
        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
        let mut slots = Vec::with_capacity(request.rows.len());
        let mut prospective_ordinals = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .untracked
                    .is_none_or(|untracked| row.untracked() == untracked)
            })
            .map(|(ordinal, _)| ordinal)
            .collect::<Vec<_>>();
        prospective_ordinals.sort_unstable_by(|left, right| {
            let left_row = self.rows.row(*left);
            let right_row = self.rows.row(*right);
            (
                left_row.schema_key(),
                left_row.entity_pk(),
                left_row.file_id(),
                left_row.branch_id(),
                *left,
            )
                .cmp(&(
                    right_row.schema_key(),
                    right_row.entity_pk(),
                    right_row.file_id(),
                    right_row.branch_id(),
                    *right,
                ))
        });
        for (slot, requested) in request.rows.iter().enumerate() {
            let upper = prospective_ordinals.partition_point(|ordinal| {
                prospective_exact_identity_cmp(self.rows.row(*ordinal), requested)
                    != std::cmp::Ordering::Greater
            });
            let winner = upper
                .checked_sub(1)
                .map(|ordinal| self.rows.row(prospective_ordinals[ordinal]))
                .filter(|row| {
                    prospective_exact_identity_cmp(*row, requested) == std::cmp::Ordering::Equal
                });
            let winner = winner.or_else(|| staged.row(slot));
            let Some(row) = winner else {
                slots.push(None);
                continue;
            };
            if row.deleted() && !request.include_tombstones {
                slots.push(None);
                continue;
            }
            let ordinal = u32::try_from(rows.push_ref(row, None)).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "prospective staged exact batch exceeds u32 rows",
                )
            })?;
            slots.push(Some(ordinal));
        }
        MaterializedLiveStateExactBatch::new(rows.finish(), slots)
    }
}

fn prospective_row_matches_scan(
    row: MaterializedLiveStateRowRef<'_>,
    request: &LiveStateScanRequest,
) -> bool {
    let filter = &request.filter;
    (filter.schema_keys.is_empty()
        || filter
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == row.schema_key()))
        && (filter.entity_pks.is_empty() || filter.entity_pks.contains(row.entity_pk()))
        && (filter.branch_ids.is_empty()
            || filter
                .branch_ids
                .iter()
                .any(|branch_id| branch_id == row.branch_id())
            || (row.branch_id() == GLOBAL_BRANCH_ID
                && filter
                    .branch_ids
                    .iter()
                    .any(|branch_id| branch_id != GLOBAL_BRANCH_ID)))
        && filter
            .untracked
            .is_none_or(|untracked| row.untracked() == untracked)
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|file_id| match file_id {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => row.file_id().is_none(),
                NullableKeyFilter::Value(file_id) => row.file_id() == Some(file_id.as_str()),
            }))
}

fn prospective_exact_identity_cmp(
    row: MaterializedLiveStateRowRef<'_>,
    requested: &LiveStateExactRowRequest,
) -> std::cmp::Ordering {
    (
        row.schema_key(),
        row.entity_pk(),
        row.file_id(),
        row.branch_id(),
    )
        .cmp(&(
            requested.schema_key.as_str(),
            &requested.entity_pk,
            requested.file_id.as_deref(),
            requested.branch_id.as_str(),
        ))
}

/// Proves that replacing a component generation cannot reinterpret any
/// currently owned file. The replacement factory is deliberately used only
/// as a disposable verifier: accepted actors are cold-opened later under the
/// new generation key, after the registry row commits.
async fn preflight_owned_v2_generation_upgrades(
    host: &PluginRuntimeHost,
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl StagedLiveStateRows,
    base_blob_reader: &dyn BlobDataReader,
    staged_writes: &TransactionWriteBuffer,
    upgrades: &[PluginGenerationUpgrade],
    install_wasm: &BTreeMap<BlobHash, Vec<u8>>,
    install_schema_definitions: &BTreeMap<PluginLifecycleKey, BTreeMap<String, JsonValue>>,
) -> Result<(), LixError> {
    let branch_ids = upgrades
        .iter()
        .map(|upgrade| upgrade.branch_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let owner_rows = overlay_scan_batch(
        base,
        staged,
        &LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                entity_pks: vec![EntityPk::single(PLUGIN_OWNER_KEY)],
                branch_ids: branch_ids.clone(),
                untracked: Some(false),
                ..Default::default()
            },
            projection: plugin_registry_live_state_projection(),
            ..Default::default()
        },
    )
    .await?;

    let upgrade_indexes = upgrades
        .iter()
        .enumerate()
        .map(|(index, upgrade)| {
            (
                (
                    upgrade.branch_id.clone(),
                    upgrade.previous.key().to_string(),
                ),
                index,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut owners_by_upgrade = vec![Vec::<PluginFileOwner>::new(); upgrades.len()];
    for row in owner_rows.iter() {
        let branch_id = row.branch_id().to_string();
        let owner_row = row.to_owned();
        let Some(owner) = PluginFileOwner::from_live_state_row(&owner_row, &branch_id)? else {
            continue;
        };
        let Some(index) = upgrade_indexes
            .get(&(branch_id.to_string(), owner.plugin_key().to_string()))
            .copied()
        else {
            continue;
        };
        if owners_by_upgrade[index]
            .iter()
            .any(|current| current.file_id() == owner.file_id())
        {
            return Err(plugin_upgrade_error(
                &upgrades[index],
                owner.file_id(),
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "durable owner lookup returned a duplicate file",
                ),
            ));
        }
        owners_by_upgrade[index].push(owner);
    }

    if owners_by_upgrade.iter().all(Vec::is_empty) {
        return Ok(());
    }

    let descriptor_rows = overlay_scan_batch(
        base,
        staged,
        &FilesystemPathIndexRequest::new(branch_ids).live_state_request(),
    )
    .await?;
    let path_index = FilesystemPathIndex::from_live_batch(&descriptor_rows)?;

    let owned_schema_keys = upgrades
        .iter()
        .zip(&owners_by_upgrade)
        .filter(|(_, owners)| !owners.is_empty())
        .flat_map(|(upgrade, _)| upgrade.previous.schema_keys().iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(EntityPk::single)
        .collect::<Vec<_>>();
    let registered_schema_rows = overlay_scan_batch(
        base,
        staged,
        &LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                entity_pks: owned_schema_keys,
                branch_ids: upgrades
                    .iter()
                    .zip(&owners_by_upgrade)
                    .filter(|(_, owners)| !owners.is_empty())
                    .map(|(upgrade, _)| upgrade.branch_id.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                file_ids: vec![NullableKeyFilter::Null],
                untracked: Some(false),
                ..Default::default()
            },
            projection: plugin_registry_live_state_projection(),
            ..Default::default()
        },
    )
    .await?;
    let mut registered_schema_definitions = BTreeMap::<(String, String), JsonValue>::new();
    for row in registered_schema_rows.iter() {
        let schema_key = row.entity_pk().as_single_string().map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema has an invalid identity: {error}"),
            )
        })?;
        let Some(snapshot) = row.snapshot_content().map(|content| content.as_str()) else {
            continue;
        };
        let snapshot: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema snapshot is invalid JSON: {error}"),
            )
        })?;
        let definition = snapshot.get("value").cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema '{schema_key}' is missing its definition"),
            )
        })?;
        if registered_schema_definitions
            .insert(
                (row.branch_id().to_string(), schema_key.to_string()),
                definition,
            )
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema '{schema_key}' has duplicate definitions"),
            ));
        }
    }

    for (upgrade, mut owners) in upgrades.iter().zip(owners_by_upgrade) {
        if owners.is_empty() {
            continue;
        }
        upgrade
            .previous
            .validate_owned_v2_upgrade_contract(&upgrade.replacement)?;
        if upgrade.previous.materialization() == PluginMaterialization::Derived {
            return Err(plugin_upgrade_error(
                upgrade,
                owners[0].file_id(),
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "generation upgrades for derived-materialization plugins are not supported yet",
                )
                .with_hint(
                    "Move or delete every owned file before replacing this plugin generation.",
                ),
            ));
        }
        owners.sort_by(|left, right| left.file_id().cmp(right.file_id()));
        let lifecycle_key = PluginLifecycleKey {
            branch_id: upgrade.branch_id.clone(),
            plugin_key: upgrade.replacement.key().to_string(),
        };
        let replacement_definitions = install_schema_definitions
            .get(&lifecycle_key)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "replacement plugin '{}' schema definitions are unavailable during upgrade preflight",
                        upgrade.replacement.key()
                    ),
                )
            })?;
        validate_owned_upgrade_schema_definitions(
            upgrade,
            owners[0].file_id(),
            &registered_schema_definitions,
            replacement_definitions,
        )?;
        for owner in &owners {
            if owner.schema_keys() != upgrade.previous.schema_keys() {
                return Err(plugin_upgrade_error(
                    upgrade,
                    owner.file_id(),
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "durable owner schema keys do not match the authoritative registry generation",
                    ),
                ));
            }
        }

        let file_ids = owners
            .iter()
            .map(|owner| owner.file_id().to_string())
            .collect::<Vec<_>>();
        let file_id_filters = file_ids
            .iter()
            .cloned()
            .map(NullableKeyFilter::Value)
            .collect::<Vec<_>>();
        let state_rows = overlay_scan_batch(
            base,
            staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: upgrade.previous.schema_keys().to_vec(),
                    branch_ids: vec![upgrade.branch_id.clone()],
                    file_ids: file_id_filters.clone(),
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_state_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut state_ordinals = Vec::<u32>::with_capacity(state_rows.len());
        for (ordinal, row) in state_rows.iter().enumerate() {
            let Some(file_id) = row.file_id() else {
                continue;
            };
            if row.branch_id() == upgrade.branch_id
                && !row.global()
                && !row.untracked()
                && row.snapshot_content().is_some()
                && upgrade
                    .previous
                    .schema_keys()
                    .binary_search_by(|schema_key| schema_key.as_str().cmp(row.schema_key()))
                    .is_ok()
                && file_ids
                    .binary_search_by(|candidate| candidate.as_str().cmp(file_id))
                    .is_ok()
            {
                state_ordinals.push(u32::try_from(ordinal).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin upgrade state batch exceeds u32 rows",
                    )
                })?);
            }
        }
        state_ordinals.sort_unstable_by(|left, right| {
            let left = state_rows.row(*left as usize);
            let right = state_rows.row(*right as usize);
            left.file_id()
                .cmp(&right.file_id())
                .then_with(|| left.schema_key().cmp(right.schema_key()))
                .then_with(|| left.entity_pk().cmp(right.entity_pk()))
        });
        let mut state_by_file = BTreeMap::<String, std::ops::Range<usize>>::new();
        let mut start = 0;
        while start < state_ordinals.len() {
            let file_id = state_rows
                .row(state_ordinals[start] as usize)
                .file_id()
                .expect("selected plugin upgrade state row carries file_id");
            let mut end = start + 1;
            while end < state_ordinals.len()
                && state_rows.row(state_ordinals[end] as usize).file_id() == Some(file_id)
            {
                end += 1;
            }
            state_by_file.insert(file_id.to_owned(), start..end);
            start = end;
        }

        let blob_rows = overlay_scan_batch(
            base,
            staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    entity_pks: file_ids
                        .iter()
                        .map(|file_id| validated_uuid_entity_pk(file_id))
                        .collect::<Result<Vec<_>, _>>()?,
                    branch_ids: vec![upgrade.branch_id.clone()],
                    file_ids: file_id_filters,
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut materialized_hash_by_file = BTreeMap::<String, BlobHash>::new();
        for row in blob_rows.iter() {
            let Some(file_id) = row.file_id() else {
                continue;
            };
            if row.branch_id() != upgrade.branch_id || row.global() || row.untracked() {
                continue;
            }
            let Some(snapshot) = row.snapshot_content().map(|content| content.as_str()) else {
                continue;
            };
            let snapshot: PluginUpgradeBlobRefSnapshot =
                serde_json::from_str(snapshot).map_err(|error| {
                    plugin_upgrade_error(
                        upgrade,
                        file_id,
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("invalid materialized blob reference: {error}"),
                        ),
                    )
                })?;
            if snapshot.id != file_id {
                return Err(plugin_upgrade_error(
                    upgrade,
                    file_id,
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "materialized blob reference identity does not match its file scope",
                    ),
                ));
            }
            let hash = BlobHash::from_hex(&snapshot.blob_hash)
                .map_err(|error| plugin_upgrade_error(upgrade, file_id, error))?;
            if materialized_hash_by_file
                .insert(file_id.to_string(), hash)
                .is_some()
            {
                return Err(plugin_upgrade_error(
                    upgrade,
                    file_id,
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "materialized blob lookup returned a duplicate file",
                    ),
                ));
            }
        }

        let hashes = owners
            .iter()
            .map(|owner| {
                materialized_hash_by_file
                    .get(owner.file_id())
                    .copied()
                    .ok_or_else(|| {
                        plugin_upgrade_error(
                            upgrade,
                            owner.file_id(),
                            LixError::new(
                                LixError::CODE_INVALID_PLUGIN,
                                "owned v2 file is missing its materialized blob reference",
                            ),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let materialized_bytes =
            load_transaction_blob_bytes(base_blob_reader, staged_writes, &hashes)
                .await?
                .into_vec();
        if materialized_bytes.len() != owners.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin upgrade materialized blob batch length mismatch",
            ));
        }

        let wasm_hash = BlobHash::from_hex(upgrade.replacement.wasm_blob_hash())?;
        let wasm = install_wasm.get(&wasm_hash).cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "replacement plugin '{}' WASM payload is unavailable during upgrade preflight",
                    upgrade.replacement.key()
                ),
            )
        })?;
        let installed = upgrade.replacement.to_installed_plugin(wasm)?;
        let factory = host.load_or_compile_v2_factory(&installed).await?;
        let limits = WasmTransitionLimits::default();

        for (owner, expected) in owners.iter().zip(materialized_bytes) {
            let expected: crate::Blob = expected
                .ok_or_else(|| {
                    plugin_upgrade_error(
                        upgrade,
                        owner.file_id(),
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "owned v2 file references a missing materialized blob",
                        ),
                    )
                })?
                .into();
            let matches = path_index
                .exact_file_id_entries(owner.file_id())
                .into_iter()
                .filter(|entry| {
                    let row = entry.live_row();
                    entry.kind == FilesystemPathKind::File
                        && entry.id() == owner.file_id()
                        && row.branch_id.as_ref() == upgrade.branch_id
                        && !row.global
                        && !row.untracked
                })
                .collect::<Vec<_>>();
            let [entry] = matches.as_slice() else {
                return Err(plugin_upgrade_error(
                    upgrade,
                    owner.file_id(),
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "owned v2 file must resolve to exactly one tracked descriptor, found {}",
                            matches.len()
                        ),
                    ),
                ));
            };
            let range = state_by_file.get(owner.file_id()).cloned().unwrap_or(0..0);
            let entities = v2_host_entities_from_live_batch_ordinals(
                &state_rows,
                &state_ordinals[range],
                limits,
            )?;
            let store_permit = host
                .actor_cache()
                .admit_store()
                .map_err(|error| plugin_upgrade_error(upgrade, owner.file_id(), error))?;
            let actor = factory
                .instantiate_actor()
                .await
                .map_err(|error| plugin_upgrade_error(upgrade, owner.file_id(), error))?;
            let mut store = PluginActorStore::new(actor, store_permit);
            let verified = preflight_rendered_v2_file(
                store.actor_mut(),
                WasmFileDescriptor {
                    path: Some(entry.path.clone()),
                    media_type: inferred_media_type_for_path(Some(&entry.path)).map(str::to_owned),
                    plugin: WasmPluginSelection {
                        plugin_key: upgrade.replacement.key().to_string(),
                        generation: upgrade.replacement.archive_blob_hash().to_string(),
                    },
                },
                entities,
                expected,
                limits,
            )
            .await;
            let retire_result = store.actor_mut().retire().await;
            if let Err(error) = verified.and(retire_result) {
                return Err(plugin_upgrade_error(upgrade, owner.file_id(), error));
            }
        }
    }
    Ok(())
}

async fn preflight_rendered_v2_file(
    actor: &mut dyn WasmComponentV2Actor,
    descriptor: WasmFileDescriptor,
    entities: Vec<WasmHostEntity>,
    expected: crate::Blob,
    limits: WasmTransitionLimits,
) -> Result<(), LixError> {
    let source = VecEntitySource::new(entities, limits)?;
    let transition = actor
        .open_entities(
            limits,
            WasmOpenEntitiesInput {
                descriptor,
                entities: Box::new(source),
                accepted: None,
            },
        )
        .await?;
    let validated =
        drain_entity_transition_edits(actor, transition, &[], Some(expected), None, limits).await?;
    actor.drop_document(validated.document).await
}

fn validate_owned_upgrade_schema_definitions(
    upgrade: &PluginGenerationUpgrade,
    file_id: &str,
    current_definitions: &BTreeMap<(String, String), JsonValue>,
    replacement_definitions: &BTreeMap<String, JsonValue>,
) -> Result<(), LixError> {
    for schema_key in upgrade.previous.schema_keys() {
        let current = current_definitions.get(&(upgrade.branch_id.clone(), schema_key.clone()));
        let replacement = replacement_definitions.get(schema_key);
        if current.is_none() || current != replacement {
            return Err(plugin_upgrade_error(
                upgrade,
                file_id,
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "schema definition '{schema_key}' differs from the authoritative owned generation"
                    ),
                ),
            ));
        }
    }
    Ok(())
}

fn plugin_upgrade_error(
    upgrade: &PluginGenerationUpgrade,
    file_id: &str,
    mut error: LixError,
) -> LixError {
    error.message = format!(
        "plugin '{}' generation upgrade rejected while preflighting owned file '{}' on branch '{}': {}",
        upgrade.replacement.key(),
        file_id,
        upgrade.branch_id,
        error.message
    );
    error
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginStateGroupKey {
    branch_id: String,
    plugin_key: String,
}

#[derive(Debug, Default)]
struct PluginStateGroup {
    file_ids: BTreeSet<String>,
    schema_keys: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy)]
struct PluginStateBatchRow {
    batch_index: u32,
    row_index: u32,
}

#[derive(Debug)]
struct PluginV2SemanticWriteGroup {
    plugin: PluginRegistryEntry,
    path: String,
    filename: String,
    owner_change_id: String,
    row_indices: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginBranchEntryKey {
    branch_id: String,
    plugin_key: String,
}

fn plugin_owner_needs_write(current: Option<&PluginFileOwner>, desired: &PluginFileOwner) -> bool {
    current != Some(desired)
}

fn duplicate_plugin_lifecycle_mutation() -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        "a write batch may mutate each plugin archive at most once",
    )
}

fn plugin_schema_collision_error(
    plugin_key: &str,
    entity_pk: &EntityPk,
    other_plugin: Option<&String>,
) -> LixError {
    let schema_key = entity_pk
        .as_single_string()
        .unwrap_or("<invalid schema identity>");
    let owner = other_plugin.map_or_else(
        || "an existing registered schema".to_string(),
        |other| format!("plugin '{other}'"),
    );
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "plugin '{plugin_key}' schema '{schema_key}' conflicts with {owner}; shared schema keys must have identical definitions"
        ),
    )
}

fn plugin_reconciliation_origin() -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: SharedStr::from_static("plugin_reconciliation"),
        operation: TransactionWriteOperation::Update,
        primary_key: None,
    }
}

#[cfg(test)]
fn mark_plugin_reconciliation_row(row: &mut TransactionWriteRow) {
    row.origin = Some(plugin_reconciliation_origin());
}

fn mark_plugin_reconciliation_batch(
    rows: &mut RawWriteBatch,
    source_index: usize,
) -> Result<(), LixError> {
    if source_index > rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin reconciliation row boundary exceeds the raw batch",
        ));
    }
    let origin = plugin_reconciliation_origin();
    for index in source_index..rows.len() {
        rows.set_origin(index, Some(origin.clone()));
    }
    Ok(())
}

fn plugin_registry_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string()],
    }
}

fn plugin_state_tombstone_batch(
    active_state: &[PluginStateBatchRow],
    state_batches: &[MaterializedLiveStateBatch],
    file_id: &str,
    context: &FilesystemRowContext,
) -> RawWriteBatch {
    let mut rows = RawWriteBatch::with_capacity(active_state.len());
    for selected in active_state {
        let row = state_batches[selected.batch_index as usize].row(selected.row_index as usize);
        rows.push_parts(
            Some(row.entity_pk().clone()),
            row.schema_key().into(),
            Some(file_id.into()),
            None,
            context.metadata.clone(),
            None,
            None,
            None,
            context.global,
            None,
            None,
            context.untracked,
            context.branch_id.as_str().into(),
        );
    }
    rows
}

fn transaction_write_targets_non_active_branch(
    write: &TransactionWrite,
    active_branch_id: &str,
) -> bool {
    let is_non_active_local = |branch_id: &str, global: bool| {
        !global && branch_id != GLOBAL_BRANCH_ID && branch_id != active_branch_id
    };
    match write {
        TransactionWrite::Rows { rows, .. } => rows
            .iter()
            .any(|row| is_non_active_local(&row.branch_id, row.global)),
        TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } => {
            rows.iter()
                .any(|row| is_non_active_local(&row.branch_id, row.global))
                || file_data
                    .iter()
                    .any(|write| is_non_active_local(&write.branch_id, write.global))
        }
    }
}

fn transaction_write_has_plugin_lifecycle_candidate(write: &TransactionWrite) -> bool {
    let (rows, file_data): (&RawWriteBatch, &[TransactionFileData]) = match write {
        TransactionWrite::Rows { rows, .. } => (rows, &[]),
        TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } => (rows, file_data),
    };
    file_data
        .iter()
        .any(|write| write.path.as_deref().is_some_and(is_plugin_storage_path))
        || rows.iter().any(|row| {
            row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY
                && row.snapshot.is_none()
                && row
                    .entity_pk
                    .and_then(|entity_pk| entity_pk.as_single_string_owned().ok())
                    .zip(
                        row.origin.and_then(|origin| {
                            plugin_key_from_archive_delete_origin(&origin.surface)
                        }),
                    )
                    .is_some_and(|(file_id, plugin_key)| {
                        plugin_archive_file_id_matches(&file_id, plugin_key)
                    })
        })
}

fn transaction_write_branch_ids(write: &TransactionWrite) -> BTreeSet<String> {
    match write {
        TransactionWrite::Rows { rows, .. } => {
            rows.iter().map(|row| row.branch_id.to_string()).collect()
        }
        TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } => rows
            .iter()
            .map(|row| row.branch_id.to_string())
            .chain(file_data.iter().map(|write| write.branch_id.clone()))
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

fn require_valid_reconciled_transaction_write_storage_scopes(
    write: &ReconciledTransactionWrite,
) -> Result<(), LixError> {
    match write {
        ReconciledTransactionWrite::Rows { rows, .. }
        | ReconciledTransactionWrite::RowsWithFileData { rows, .. } => {
            rows.require_valid_storage_scopes()
        }
    }
}

fn require_valid_transaction_write_row_storage_scopes(
    rows: &RawWriteBatch,
) -> Result<(), LixError> {
    for row in rows {
        require_valid_storage_scope(row.branch_id.as_str(), row.global)?;
    }
    Ok(())
}

fn require_valid_storage_scope(branch_id: &str, global: bool) -> Result<(), LixError> {
    if global != (branch_id == GLOBAL_BRANCH_ID) {
        return Err(LixError::new(
            LixError::CODE_INVALID_STORAGE_SCOPE,
            format!("invalid storage scope: branch_id='{branch_id}', global={global}"),
        ));
    }
    Ok(())
}

async fn resolve_active_branch_id(
    mode: &SessionMode,
    live_state: &LiveStateContext,
    branch_ctx: &BranchContext,
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { branch_id } => Ok(branch_id.clone()),
        SessionMode::Workspace => {
            load_workspace_branch_id_from_index(live_state, branch_ctx, read).await
        }
    }
}

fn validated_uuid_entity_pk(value: &str) -> Result<EntityPk, LixError> {
    EntityPk::uuid_from_canonical(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("validated identity is not a canonical UUID: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use serde_json::json;

    use super::*;
    use crate::Engine;
    use crate::GLOBAL_BRANCH_ID;
    use crate::NullableKeyFilter;
    use crate::branch::BranchContext;
    use crate::functions::FunctionProvider;
    use crate::storage_adapter::{Memory, StorageReadOptions};
    use crate::tracked_state::{
        TrackedStateDiffIdentity, TrackedStateKey, TrackedStateScanRequest,
    };
    use crate::transaction::types::{
        StagedCommitChangeBatchBuilder, StagedCommitChangeRefs, TransactionJson,
    };
    use crate::wasm::WasmEntity;

    fn raw_write_rows(rows: Vec<TransactionWriteRow>) -> RawWriteBatch {
        RawWriteBatch::from_test_rows(rows)
    }

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    #[test]
    fn prospective_overlay_keeps_ten_thousand_rows_in_one_dictionary_batch() {
        const ROW_COUNT: usize = 10_000;

        let timestamp =
            LixTimestamp::expect_parse("prospective overlay timestamp", "2026-01-01T00:00:00.000Z");
        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(ROW_COUNT);
        for ordinal in 0..ROW_COUNT {
            let entity_pk = EntityPk::single(format!("entity-{ordinal}"));
            rows.push_materialized_ref(
                &entity_pk,
                "example",
                Some("shared-file"),
                Some(SharedStr::from_static(r#"{"value":true}"#)),
                None,
                false,
                timestamp,
                timestamp,
                false,
                None,
                None,
                false,
                "main",
            );
        }
        let prospective = ProspectiveStagedRows {
            staged: Arc::new(TransactionWriteBuffer::new(FunctionProviderHandle::system()))
                .staging_overlay()
                .expect("empty staging overlay"),
            rows: rows.finish(),
        };

        let batch = prospective
            .staged_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["example".to_string()],
                    branch_ids: vec!["main".to_string()],
                    file_ids: vec![NullableKeyFilter::Value("shared-file".to_string())],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .expect("prospective batch scan");

        assert_eq!(batch.len(), ROW_COUNT);
        assert_eq!(batch.dictionary_entry_count(), 3);
        assert_eq!(batch.dictionary_arena_buffer_count(), 1);
    }

    #[test]
    fn reconciliation_rows_share_one_static_surface_across_mark_calls() {
        let mut first = key_value_stage_row("first", "value", false);
        let mut second = key_value_stage_row("second", "value", false);

        mark_plugin_reconciliation_row(&mut first);
        mark_plugin_reconciliation_row(&mut second);

        let first_surface = &first.origin.as_ref().expect("first origin").surface;
        let second_surface = &second.origin.as_ref().expect("second origin").surface;
        assert!(first_surface.shares_buffer_with(second_surface));
        assert_eq!(
            first_surface.as_bytes().as_ptr(),
            second_surface.as_bytes().as_ptr()
        );
    }

    #[test]
    fn format_only_plugin_rows_share_one_certified_metadata_buffer() {
        const ROW_COUNT: usize = 10_000;

        let metadata = v2_format_only_metadata();
        let mut rows = RawWriteBatch::with_capacity(ROW_COUNT);
        for _ in 0..ROW_COUNT {
            rows.push_parts(
                None,
                SharedStr::from_static("format_only_probe"),
                None,
                None,
                Some(metadata.clone()),
                None,
                None,
                None,
                false,
                None,
                None,
                false,
                SharedStr::from_static("main"),
            );
        }

        let first = rows
            .row(0)
            .metadata
            .expect("format-only row carries metadata");
        assert!(first.metadata_content_certified());
        assert_eq!(first.normalized(), V2_FORMAT_ONLY_METADATA_JSON);
        let first_pointer = first.normalized().as_ptr();
        assert!(rows.iter().all(|row| {
            let metadata = row.metadata.expect("format-only row carries metadata");
            metadata.metadata_content_certified()
                && metadata.normalized().as_ptr() == first_pointer
                && metadata.normalized() == V2_FORMAT_ONLY_METADATA_JSON
        }));
    }

    #[test]
    fn selected_blob_ref_change_requires_filesystem_index_rebuild() {
        let mut selected_changes = StagedCommitChangeRefs::default();
        let mut batch = StagedCommitChangeBatchBuilder::with_capacity(1);
        batch.push(
            TrackedStateDiffIdentity::from_key(TrackedStateKey {
                schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
                file_id: Some("file-a".to_string()),
                entity_pk: EntityPk::single("file-a"),
            }),
            CommitId::default(),
            ChangeId::default(),
            false,
            LixTimestamp::from_unix_millis_utc_lossy(0),
            LixTimestamp::from_unix_millis_utc_lossy(0),
        );
        selected_changes.add_selected_change_batch(batch.finish());
        let prepared_writes = PreparedWriteSet {
            state_rows: PreparedStateBatch::new(),
            insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::from([("main".to_string(), selected_changes)]),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_data_writes: Vec::new(),
        };

        assert!(prepared_writes_require_filesystem_index_rebuild(
            &prepared_writes
        ));
    }

    #[test]
    fn visible_materialization_requires_a_matching_blob_ref_identity() {
        let blob_hash = BlobHash::from_content(b"base");
        let row = |snapshot_id: &str| MaterializedLiveStateRow {
            entity_pk: EntityPk::single("01920000-0000-7000-8000-0000000000a2"),
            schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
            file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            snapshot_content: Some(
                serde_json::json!({
                    "id": snapshot_id,
                    "blob_hash": blob_hash.to_hex(),
                })
                .to_string()
                .into(),
            ),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            global: false,
            change_id: Some(ChangeId::default()),
            commit_id: None,
            untracked: false,
            branch_id: "main".into(),
        };

        let visible = decode_visible_v2_materialization(
            &row("01920000-0000-7000-8000-0000000000a2"),
            "01920000-0000-7000-8000-0000000000a2",
        )
        .expect("matching materialization should decode");
        assert!(matches!(
            visible.bytes,
            VisibleV2MaterializationBytes::Blob { hash } if hash == blob_hash
        ));
        let error = decode_visible_v2_materialization(
            &row("other-file"),
            "01920000-0000-7000-8000-0000000000a2",
        )
        .expect_err("mismatched blob-ref identity must not authorize a cached actor base");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    }

    #[test]
    fn semantic_renderer_splice_provenance_is_bound_to_its_visible_blob() {
        let base_blob_hash = BlobHash::from_content(b"abcdef");
        let rendered = semantic_rendered_file_data(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            "/document.md".to_string(),
            "document.md".to_string(),
            "main".to_string(),
            base_blob_hash,
            b"abXYef".as_slice().into(),
            Some(ValidatedSameLengthOutputSplice {
                offset: 2,
                length: 2,
            }),
        );

        assert_eq!(rendered.base_blob_hash(), Some(base_blob_hash));
        assert_eq!(
            rendered.same_length_blob_splice(),
            Some(crate::binary_cas::BlobSameLengthSplice::new(
                base_blob_hash,
                2,
                2,
            ))
        );

        let malformed = semantic_rendered_file_data(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            "/document.md".to_string(),
            "document.md".to_string(),
            "main".to_string(),
            base_blob_hash,
            b"abXYef".as_slice().into(),
            Some(ValidatedSameLengthOutputSplice {
                offset: 6,
                length: 1,
            }),
        );
        assert_eq!(malformed.base_blob_hash(), Some(base_blob_hash));
        assert_eq!(
            malformed.same_length_blob_splice(),
            None,
            "transaction-side bounds checks must force malformed metadata through the ordinary CAS path"
        );
    }

    #[test]
    fn format_only_equal_snapshots_are_semantic_noops() {
        let key = |id: &str| WasmEntityKey::from_owned_parts("plugin_note", vec![id.to_string()]);
        let live = |id: &str, snapshot_content: &str| MaterializedLiveStateRow {
            entity_pk: EntityPk::single(id),
            schema_key: "plugin_note".to_string(),
            file_id: Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            snapshot_content: Some(snapshot_content.to_string().into()),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
        };
        let upsert = |id: &str, snapshot: &[u8], effect| {
            let value = serde_json::from_slice::<JsonValue>(snapshot)
                .expect("test snapshot must contain valid JSON");
            let normalized_len = u32::try_from(snapshot.len()).expect("test snapshot fits u32");
            let canonical = crate::wasm::WasmCanonicalJson::from_batch_parts(
                vec![value],
                snapshot.to_vec(),
                vec![(0, normalized_len)],
                1,
                1,
            )
            .expect("test canonical batch must be valid")
            .pop()
            .expect("test canonical batch has one row");
            WasmEntityChange::Upsert {
                entity: WasmEntity {
                    key: key(id),
                    snapshot_content: WasmHostBytes::CanonicalJson(canonical),
                },
                effect,
            }
        };
        let changes = WasmHostEntityChanges {
            changes: vec![
                upsert(
                    "equal",
                    r#"{"id":"equal","text":"é"}"#.as_bytes(),
                    WasmChangeEffect::FormatOnly,
                ),
                upsert(
                    "changed",
                    br#"{"id":"changed","text":"new"}"#,
                    WasmChangeEffect::FormatOnly,
                ),
                upsert(
                    "content",
                    br#"{"id":"content","text":"same"}"#,
                    WasmChangeEffect::Content,
                ),
                WasmEntityChange::Delete(key("deleted")),
            ],
        };
        let accepted = [
            (
                key("equal"),
                Some(live("equal", r#"{"text":"\u00e9","id":"equal"}"#)),
            ),
            (
                key("changed"),
                Some(live("changed", r#"{"id":"changed","text":"old"}"#)),
            ),
            (
                key("content"),
                Some(live("content", r#"{"id":"content","text":"same"}"#)),
            ),
        ];
        let accepted_keys = accepted
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let accepted = MaterializedLiveStateExactBatch::from_rows(
            accepted.into_iter().map(|(_, row)| row).collect(),
        );

        let effective =
            suppress_v2_format_only_noops_against_batch(changes, &accepted_keys, &accepted)
                .expect("number-free normalized snapshots should compare");
        assert_eq!(effective.changes.len(), 3);
        assert_eq!(effective.changes[0].entity_key(), Some(&key("changed")));
        assert_eq!(effective.changes[1].entity_key(), Some(&key("content")));
        assert_eq!(effective.changes[2].entity_key(), Some(&key("deleted")));
    }

    #[test]
    fn plugin_owner_is_only_rewritten_when_its_durable_contract_changes() {
        let current = PluginFileOwner::new(
            "01920000-0000-7000-8000-0000000000a2",
            "plugin-a",
            vec!["schema-a".to_string()],
        )
        .expect("current owner should be valid");
        assert!(!plugin_owner_needs_write(Some(&current), &current));
        assert!(plugin_owner_needs_write(None, &current));

        for desired in [
            PluginFileOwner::new(
                "01920000-0000-7000-8000-0000000000a2",
                "plugin-b",
                vec!["schema-a".to_string()],
            )
            .expect("changed plugin owner should be valid"),
            PluginFileOwner::new(
                "01920000-0000-7000-8000-0000000000a2",
                "plugin-a",
                vec!["schema-b".to_string()],
            )
            .expect("changed schema owner should be valid"),
        ] {
            assert!(plugin_owner_needs_write(Some(&current), &desired));
        }
    }

    #[test]
    fn active_branch_write_gate_ignores_global_companion_rows_and_files() {
        let mut active_row = key_value_stage_row("active-row", "value", false);
        active_row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        active_row.global = false;
        let mut global_row = key_value_stage_row("global-row", "value", false);
        global_row.branch_id = GLOBAL_BRANCH_ID.into();
        global_row.global = true;
        let global_file = TransactionFileData::new(
            "global-file".to_string(),
            None,
            None,
            GLOBAL_BRANCH_ID.to_string(),
            true,
            false,
            Vec::new(),
        );

        assert!(
            !transaction_write_targets_non_active_branch(
                &TransactionWrite::RowsWithFileData {
                    mode: TransactionWriteMode::Replace,
                    rows: raw_write_rows(vec![active_row.clone(), global_row]),
                    file_data: vec![global_file],
                    count: 1,
                },
                "01920000-0000-7000-8000-0000000000a1",
            ),
            "normal local writes commonly carry global bookkeeping rows"
        );

        active_row.branch_id = "01920000-0000-7000-8000-0000000000b1".into();
        assert!(transaction_write_targets_non_active_branch(
            &TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: raw_write_rows(vec![active_row]),
            },
            "01920000-0000-7000-8000-0000000000a1",
        ));
    }

    enum UpgradePreflightBehavior {
        Render(Vec<u8>),
        Trap,
    }

    struct UpgradePreflightActor {
        behavior: UpgradePreflightBehavior,
        emitted: bool,
        discarded: bool,
    }

    impl UpgradePreflightActor {
        fn rendering(bytes: &[u8]) -> Self {
            Self {
                behavior: UpgradePreflightBehavior::Render(bytes.to_vec()),
                emitted: false,
                discarded: false,
            }
        }

        fn trapping() -> Self {
            Self {
                behavior: UpgradePreflightBehavior::Trap,
                emitted: false,
                discarded: false,
            }
        }
    }

    fn unused_upgrade_actor_method() -> LixError {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "unused upgrade preflight actor method",
        )
    }

    #[async_trait::async_trait]
    impl WasmComponentV2Actor for UpgradePreflightActor {
        async fn fork_document(
            &mut self,
            document: WasmDocumentHandle,
        ) -> Result<WasmDocumentHandle, LixError> {
            Ok(document)
        }

        async fn open_file(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenFileInput,
        ) -> Result<crate::wasm::WasmFileTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn open_entities(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenEntitiesInput,
        ) -> Result<crate::wasm::WasmEntityTransition, LixError> {
            match &self.behavior {
                UpgradePreflightBehavior::Render(_) => Ok(crate::wasm::WasmEntityTransition {
                    transition: crate::wasm::WasmTransitionHandle(1),
                    document: WasmDocumentHandle(2),
                    edits: crate::wasm::WasmEditCursorHandle(3),
                }),
                UpgradePreflightBehavior::Trap => Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "synthetic replacement trap",
                )),
            }
        }

        async fn file_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: WasmFileUpdate,
        ) -> Result<crate::wasm::WasmFileTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn entities_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: WasmEntityUpdate,
        ) -> Result<crate::wasm::WasmEntityTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn next_change_page(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
            _cursor: crate::wasm::WasmChangeCursorHandle,
            _max_bytes: u32,
        ) -> Result<Option<crate::wasm::WasmChangePage>, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn next_edit_page(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
            _cursor: crate::wasm::WasmEditCursorHandle,
            _max_edits: u32,
            _max_inline_bytes: u32,
        ) -> Result<Option<crate::wasm::WasmEditPage>, LixError> {
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let UpgradePreflightBehavior::Render(bytes) = &self.behavior else {
                return Err(unused_upgrade_actor_method());
            };
            Ok(Some(crate::wasm::WasmEditPage {
                edits: vec![crate::wasm::WasmOutputSplice {
                    offset: 0,
                    delete_len: 0,
                    insert: crate::wasm::WasmGuestBytes::Inline(bytes.clone().into()),
                }],
                outputs: None,
            }))
        }

        async fn output_len(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
            _outputs: crate::wasm::WasmByteOutputsHandle,
            _index: u32,
        ) -> Result<u64, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn read_output(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
            _outputs: crate::wasm::WasmByteOutputsHandle,
            _index: u32,
            _offset: u64,
            _length: u32,
        ) -> Result<Vec<u8>, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn finish_transition(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
        ) -> Result<crate::wasm::WasmTransitionCounters, LixError> {
            Ok(crate::wasm::WasmTransitionCounters::default())
        }

        async fn discard_transition(
            &mut self,
            _transition: crate::wasm::WasmTransitionHandle,
        ) -> Result<(), LixError> {
            self.discarded = true;
            Ok(())
        }

        fn is_retired(&self) -> bool {
            false
        }
    }

    fn upgrade_preflight_descriptor() -> WasmFileDescriptor {
        WasmFileDescriptor {
            path: Some("/owned.csv".to_string()),
            media_type: Some("text/csv".to_string()),
            plugin: WasmPluginSelection {
                plugin_key: "plugin_csv_v2".to_string(),
                generation: "replacement".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn owned_v2_upgrade_preflight_accepts_only_byte_stable_renderer() {
        let expected: crate::Blob = b"first,one\n".as_slice().into();
        let mut compatible = UpgradePreflightActor::rendering(expected.as_ref());
        preflight_rendered_v2_file(
            &mut compatible,
            upgrade_preflight_descriptor(),
            Vec::new(),
            expected.clone(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect("byte-stable replacement should pass preflight");

        let mut output_changing = UpgradePreflightActor::rendering(b"changed\n");
        let error = preflight_rendered_v2_file(
            &mut output_changing,
            upgrade_preflight_descriptor(),
            Vec::new(),
            expected.clone(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("output-changing replacement must fail preflight");
        assert!(error.message.contains("expected bytes"), "{error:?}");
        assert!(
            output_changing.discarded,
            "host rejection must discard the prospective transition"
        );

        let mut trapping = UpgradePreflightActor::trapping();
        let error = preflight_rendered_v2_file(
            &mut trapping,
            upgrade_preflight_descriptor(),
            Vec::new(),
            expected,
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("trapping replacement must fail preflight");
        assert!(error.message.contains("synthetic replacement trap"));
    }

    fn upgrade_test_entry(hash_byte: char) -> PluginRegistryEntry {
        let hash = std::iter::repeat_n(hash_byte, 64).collect::<String>();
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: "plugin_csv_v2".to_string(),
            runtime: crate::plugin::PluginRuntime::WasmComponentV2,
            api_version: "2.1.0".to_string(),
            path_glob: "*.csv".to_string(),
            content_type: Some(PluginContentType::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["csv_row".to_string()],
            create_schema_keys: vec!["csv_row".to_string()],
            manifest_json: r#"{"api_version":"2.1.0","entry":"plugin.wasm","key":"plugin_csv_v2","match":{"content_type":"text","path_glob":"*.csv"},"materialization":"blob","runtime":"wasm-component-v2","schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: crate::plugin::plugin_storage_archive_file_id("plugin_csv_v2"),
            archive_path: "/.lix/plugins/plugin_csv_v2.lixplugin".to_string(),
            archive_blob_hash: hash.clone(),
            wasm_blob_hash: hash,
        })
        .expect("upgrade test registry entry should be valid")
    }

    #[test]
    fn owned_v2_upgrade_rejects_schema_definition_change_before_authority_swap() {
        let previous = upgrade_test_entry('a');
        let upgrade = PluginGenerationUpgrade {
            branch_id: "main".to_string(),
            previous: previous.clone(),
            replacement: upgrade_test_entry('b'),
        };
        let definition = json!({
            "x-lix-key": "csv_row",
            "type": "object",
        });
        let current = BTreeMap::from([(
            ("main".to_string(), "csv_row".to_string()),
            definition.clone(),
        )]);
        validate_owned_upgrade_schema_definitions(
            &upgrade,
            "owned-file",
            &current,
            &BTreeMap::from([("csv_row".to_string(), definition)]),
        )
        .expect("identical schema definitions should be compatible");

        let error = validate_owned_upgrade_schema_definitions(
            &upgrade,
            "owned-file",
            &current,
            &BTreeMap::from([(
                "csv_row".to_string(),
                json!({
                    "x-lix-key": "csv_row",
                    "type": "object",
                    "properties": { "extra": { "type": "string" } },
                }),
            )]),
        )
        .expect_err("schema definition change must fail before the registry write is staged");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("schema definition 'csv_row'"));
        assert_eq!(
            previous.archive_blob_hash(),
            std::iter::repeat_n('a', 64).collect::<String>(),
            "the previously loaded authoritative entry remains untouched on rejection"
        );
    }

    #[tokio::test]
    #[ignore = "release-only transaction path-index benchmark probe"]
    #[allow(clippy::large_futures)] // Boxing would add allocation to the measured execute path.
    async fn transaction_path_index_benchmark_probe() {
        let file_count = std::env::var("LIX_PATH_INDEX_BENCH_FILES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000);
        let rounds = std::env::var("LIX_PATH_INDEX_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(24);
        let warmup_rounds = 4_usize;

        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open initialized storage");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let values = (0..file_count)
            .map(|index| format!("('/seed-{index:05}.md', X'01')"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_file (path, data) VALUES {values}"),
                &[],
            )
            .await
            .expect("fixture files should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/transaction-anchor.md', X'01')",
                &[],
            )
            .await
            .expect("transaction anchor descriptor should stage");

        reset_transaction_path_index_build_stats();
        let sql = "UPDATE lix_file SET data = X'02' WHERE path = '/seed-00000.md'";
        for _ in 0..warmup_rounds {
            transaction
                .execute(sql, &[])
                .await
                .expect("warm transaction path update should succeed");
        }

        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = Instant::now();
            transaction
                .execute(sql, &[])
                .await
                .expect("timed transaction path update should succeed");
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        let stats = transaction_path_index_build_stats();
        println!(
            "transaction_path_index_probe files={file_count} rounds={rounds} \
             builds={} descriptor_rows={} p50_us={} p95_us={}",
            stats.builds,
            stats.descriptor_rows,
            percentile(50, 100).as_micros(),
            percentile(95, 100).as_micros(),
        );
        assert_eq!(
            stats.builds, 1,
            "repeated data-only path updates should reuse one transaction-visible index"
        );
        assert_eq!(
            stats.descriptor_rows,
            file_count + 1,
            "the one cached build should include the staged anchor and committed files"
        );

        transaction
            .rollback()
            .await
            .expect("benchmark transaction should roll back");
    }

    #[tokio::test]
    #[ignore = "release-only committed filesystem index benchmark probe"]
    async fn committed_filesystem_path_index_benchmark_probe() {
        let file_count = std::env::var("LIX_PATH_INDEX_BENCH_FILES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000);
        let rounds = std::env::var("LIX_PATH_INDEX_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(24);
        let warmup_rounds = 4_usize;

        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open initialized storage");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let values = (0..file_count)
            .map(|index| format!("('file-{index:05}', '/seed-{index:05}.md', X'01')"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_file (id, path, data) VALUES {values}"),
                &[],
            )
            .await
            .expect("fixture files should commit");
        session
            .execute("SELECT id FROM lix_file WHERE path = '/seed-00000.md'", &[])
            .await
            .expect("fixture path index should warm");
        crate::filesystem::reset_full_rebuild_stats();

        let mut samples = Vec::with_capacity(rounds);
        for iteration in 0..warmup_rounds.saturating_add(rounds) {
            let path = if iteration % 2 == 0 {
                "/renamed-00000.md"
            } else {
                "/seed-00000.md"
            };
            session
                .execute(
                    &format!("UPDATE lix_file SET path = '{path}' WHERE id = 'file-00000'"),
                    &[],
                )
                .await
                .expect("descriptor invalidation fixture should commit");
            let started = Instant::now();
            session
                .execute(
                    &format!("UPDATE lix_file SET data = X'02' WHERE path = '{path}'"),
                    &[],
                )
                .await
                .expect("singleton write after descriptor commit should succeed");
            let elapsed = started.elapsed();
            if iteration >= warmup_rounds {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        println!(
            "committed_filesystem_path_index_probe files={file_count} rounds={rounds} \
             rebuilds={} descriptor_rows={} p50_us={} p95_us={}",
            crate::filesystem::full_rebuild_stats().0,
            crate::filesystem::full_rebuild_stats().1,
            percentile(50, 100).as_micros(),
            percentile(95, 100).as_micros(),
        );
        if incremental_filesystem_index_enabled() {
            assert_eq!(
                crate::filesystem::full_rebuild_stats(),
                (0, 0),
                "committed singleton updates must not rebuild or rescan descriptors"
            );
        }
    }

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let tracked_state = Arc::new(TrackedStateContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::clone(&tracked_state),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::wasm::UnsupportedWasmRuntime)),
            Arc::clone(&branch_ctx),
            Arc::clone(&catalog_context),
            Arc::new(SqlPlanningCache::default()),
            SessionFileViews::default(),
        )
        .await
        .expect("transaction should open");
        let mut transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        transaction
            .stage_rows(raw_write_rows(vec![
                key_value_stage_row("tracked-programmatic", "tracked", false),
                key_value_stage_row("untracked-programmatic", "untracked", true),
            ]))
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
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single("tracked-programmatic"),
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
        let packed_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("packed payload read should open");
        let inventory = crate::tracked_state::scan_commit_delta_inventory(&packed_read)
            .await
            .expect("packed authority should scan");
        assert!(
            inventory
                .commits
                .values()
                .any(|entry| entry
                    .members
                    .iter()
                    .any(|member| member.change.change_id == tracked_change_id
                        && member.change.entity_pk.as_single_string_owned().as_deref()
                            == Ok("tracked-programmatic"))),
            "tracked staged row should be authoritative in a packed commit delta"
        );

        let head_commit_id = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("branch ref should load")
            .expect("tracked commit should advance the global branch ref");

        let tracked_row = TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_batch_at_commit(
                &head_commit_id.to_string(),
                &[TrackedStateKey {
                    schema_key: "lix_key_value".to_string(),
                    entity_pk: EntityPk::single("tracked-programmatic"),
                    file_id: None,
                }],
            )
            .await
            .expect("tracked state should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("tracked row should be present in tracked state");
        assert_eq!(tracked_row.commit_id, head_commit_id);
        assert_eq!(
            tracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"tracked-programmatic","value":"tracked"}"#)
        );

        let live_untracked_row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single("untracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load")
            .expect("untracked row should be visible through live state");
        assert!(live_untracked_row.untracked);
        assert!(live_untracked_row.global);
        assert_eq!(live_untracked_row.branch_id.as_ref(), GLOBAL_BRANCH_ID);
        assert_eq!(
            live_untracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"untracked-programmatic","value":"untracked"}"#)
        );
        assert_eq!(
            live_untracked_row.change_id, None,
            "ordinary untracked rows must not enter the changelog"
        );
        assert_eq!(
            live_untracked_row.commit_id, None,
            "ordinary untracked rows must not enter the commit graph"
        );

        let tracked_rows = TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                &head_commit_id.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("tracked state should scan")
            .into_rows();
        assert!(
            tracked_rows
                .iter()
                .all(|row| row.entity_pk.as_single_string_owned().as_deref()
                    != Ok("untracked-programmatic")),
            "untracked staged rows should not be written into tracked state"
        );
    }

    #[tokio::test]
    async fn transaction_open_prewarms_tracked_and_sql_schema_catalogs() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, transaction) =
            open_test_transaction(&storage).await;

        assert!(
            transaction
                .schema_resolver
                .has_cached_catalog_for_test(&Domain::schema_catalog(GLOBAL_BRANCH_ID, true))
        );
        assert!(
            transaction
                .schema_resolver
                .has_cached_catalog_for_test(&Domain::schema_catalog(GLOBAL_BRANCH_ID, false))
        );
    }

    #[tokio::test]
    async fn stage_rows_accepts_lossy_iso_timestamps_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("lossy-timestamp", "value", true);
        row.created_at = Some("1969-12-31T23:59:59.999999Z".to_string());
        row.updated_at = Some("2026-04-23T00:00:00.123456Z".to_string());

        let outcome = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect("valid ISO timestamps should stage after lossy normalization");
        assert_eq!(outcome.count, 1);

        let rows = transaction
            .scan_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_pks: vec![EntityPk::single("lossy-timestamp")],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    untracked: Some(true),
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await
            .expect("staged row should scan through transaction live state");

        assert_eq!(rows.len(), 1);
        assert!(
            rows.row(0).change_id().is_some(),
            "prepared untracked rows must receive a real change id"
        );
        assert_eq!(
            rows.row(0).created_at().to_string(),
            "1970-01-01T00:00:00.000Z"
        );
        assert_eq!(
            rows.row(0).updated_at().to_string(),
            "2026-04-23T00:00:00.123Z"
        );
    }

    #[tokio::test]
    async fn stage_rows_validates_row_content_before_persistence() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::new(TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::wasm::UnsupportedWasmRuntime)),
            Arc::clone(&branch_ctx),
            Arc::clone(&catalog_context),
            Arc::new(SqlPlanningCache::default()),
            SessionFileViews::default(),
        )
        .await
        .expect("transaction should open");
        let mut transaction = opened.transaction;

        let mut invalid_row = key_value_stage_row("invalid-programmatic", "invalid", false);
        invalid_row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({"key": "invalid-programmatic"}),
        ));
        let error = transaction
            .stage_rows(raw_write_rows(vec![invalid_row]))
            .await
            .expect_err("row-local validation should reject while staging");
        assert!(
            error.message.contains("snapshot_content validation failed"),
            "validation error should explain the rejected schema data: {error:?}"
        );

        let head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("branch ref should load after failed commit");
        assert_eq!(
            head,
            Some(CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID)),
            "validation failure must not advance the branch ref"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_non_object_metadata_without_sql() {
        let storage = Memory::new();
        let (live_state, _binary_cas, branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let storage = StorageAdapter::new(storage);

        let mut row = key_value_stage_row("invalid-metadata", "value", false);
        row.metadata = Some(TransactionJson::from_value_for_test(json!("not-an-object")));
        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("non-object metadata should fail statement validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("metadata") && error.message.contains("JSON object"),
            "error should explain metadata object validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &live_state,
            &branch_ref,
            "invalid-metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("unknown-schema", "value", false);
        row.schema_key = "missing_schema".into();

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
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
    async fn stage_rows_rejects_missing_branch_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("ghost-branch-row", "value", false);
        row.branch_id = "ghost-branch".into();
        row.global = false;

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("missing branch should be rejected before staging");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert!(
            error
                .message
                .contains("branch 'ghost-branch' was not found"),
            "error should explain missing branch: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_invalid_storage_scope_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("invalid-storage-scope", "value", false);
        row.branch_id = GLOBAL_BRANCH_ID.into();
        row.global = false;

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("invalid storage scope should be rejected before staging");

        assert_eq!(error.code, LixError::CODE_INVALID_STORAGE_SCOPE);
        assert!(
            error
                .message
                .contains("branch_id='ffffffff-ffff-7fff-bfff-ffffffffffff', global=false"),
            "error should explain invalid storage scope: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_invalid_snapshot_json_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("invalid-json", "value", false);
        row.snapshot = Some(TransactionJson::from_value_for_test(json!("not-an-object")));

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("non-object snapshot should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("must be a JSON object"),
            "error should explain invalid snapshot shape: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_snapshot_that_violates_json_schema_without_sql() {
        let storage = Memory::new();
        let (live_state, _binary_cas, branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let storage = StorageAdapter::new(storage);

        let mut row = key_value_stage_row("schema-mismatch", "value", false);
        row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({"key": "schema-mismatch"}),
        ));
        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("JSON Schema mismatch should fail statement validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("snapshot_content validation failed"),
            "error should explain JSON Schema validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &live_state,
            &branch_ref,
            "schema-mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("malformed-registered-schema", "value", false);
        row.schema_key = "lix_registered_schema".into();
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
        row.entity_pk = None;

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("malformed registered schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("x-lix-primary-key"),
            "error should explain malformed registered schema: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_primary_key_entity_pk_mismatch_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("right-id", "value", false);
        row.entity_pk = Some(EntityPk::single("wrong-id"));

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("entity pk mismatch should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .message
                .contains("does not match x-lix-primary-key derived entity_pk"),
            "error should explain entity pk mismatch: {error:?}"
        );
    }

    #[derive(Clone)]
    struct CountingPreparationProvider {
        uuid_calls: Arc<AtomicUsize>,
        timestamp_calls: Arc<AtomicUsize>,
    }

    impl FunctionProvider for CountingPreparationProvider {
        fn uuid_v7(&mut self) -> uuid::Uuid {
            let call = self.uuid_calls.fetch_add(1, Ordering::SeqCst) + 1;
            counting_preparation_uuid(call)
        }

        fn timestamp(&mut self) -> LixTimestamp {
            self.timestamp_calls.fetch_add(1, Ordering::SeqCst);
            LixTimestamp::expect_parse("test timestamp", "2026-01-01T00:00:00.000Z")
        }
    }

    fn counting_preparation_uuid(call: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000_u128 + call as u128)
    }

    fn install_counting_preparation_provider(
        transaction: &mut Transaction,
    ) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let uuid_calls = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));
        transaction.functions =
            FunctionProviderHandle::shared(Box::new(CountingPreparationProvider {
                uuid_calls: Arc::clone(&uuid_calls),
                timestamp_calls: Arc::clone(&timestamp_calls),
            }));
        (uuid_calls, timestamp_calls)
    }

    fn account_stage_row(name: &str) -> TransactionWriteRow {
        TransactionWriteRow {
            entity_pk: None,
            schema_key: "lix_account".into(),
            file_id: None,
            snapshot: Some(TransactionJson::from_value_for_test(
                json!({ "name": name }),
            )),
            metadata: None,
            origin: None,
            created_at: Some("2026-01-01T00:00:00.000Z".to_string()),
            updated_at: Some("2026-01-01T00:00:00.000Z".to_string()),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: GLOBAL_BRANCH_ID.into(),
        }
    }

    #[tokio::test]
    async fn homogeneous_preparation_preserves_per_row_function_call_order() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let (uuid_calls, _timestamp_calls) =
            install_counting_preparation_provider(&mut transaction);

        let prepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![
                account_stage_row("Ada"),
                account_stage_row("Grace"),
            ]))
            .await
            .expect("accounts should prepare");

        assert_eq!(
            prepared.row(0).entity_pk,
            &EntityPk::uuid_from_canonical(&counting_preparation_uuid(1).to_string())
                .expect("UUID pk")
        );
        assert_eq!(
            prepared.row(0).change_id,
            Some(ChangeId::from(counting_preparation_uuid(2)))
        );
        assert_eq!(
            prepared.row(1).entity_pk,
            &EntityPk::uuid_from_canonical(&counting_preparation_uuid(3).to_string())
                .expect("UUID pk")
        );
        assert_eq!(
            prepared.row(1).change_id,
            Some(ChangeId::from(counting_preparation_uuid(4)))
        );
        assert_eq!(uuid_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn homogeneous_preparation_reports_earlier_scalar_error_before_later_schema_error() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let mut scalar_invalid = key_value_stage_row("scalar-invalid", "value", false);
        scalar_invalid.updated_at = Some("not-a-timestamp".to_string());
        let mut schema_invalid = key_value_stage_row("schema-invalid", "value", false);
        schema_invalid.snapshot =
            Some(TransactionJson::from_value_for_test(json!("not-an-object")));

        let error = transaction
            .prepare_transaction_rows(raw_write_rows(vec![scalar_invalid, schema_invalid]))
            .await
            .expect_err("the first row's scalar error should retain precedence");
        assert!(
            error.message.contains("invalid updated_at timestamp"),
            "unexpected preparation error: {error:?}"
        );
    }

    #[tokio::test]
    async fn mixed_reconciliation_normalizes_all_raw_rows_before_scalar_provider_calls() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let prepared_source = key_value_stage_row("prepared", "value", false);
        let prepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![prepared_source.clone()]))
            .await
            .expect("prepared semantic slot");
        let raw_valid = key_value_stage_row("raw-valid", "value", false);
        let mut raw_invalid = key_value_stage_row("raw-invalid", "value", false);
        raw_invalid.snapshot = Some(TransactionJson::from_value_for_test(json!("not-an-object")));
        let mut rows = ReconciledRowBatch::raw(raw_write_rows(vec![
            prepared_source,
            raw_valid,
            raw_invalid,
        ]));
        rows.take_raw_rows_at(&[0])
            .expect("semantic source should extract");
        rows.put_prepared_batch_at(&[0], prepared)
            .expect("prepared source should fill its slot");
        let (uuid_calls, timestamp_calls) = install_counting_preparation_provider(&mut transaction);

        let error = transaction
            .prepare_reconciled_rows(rows)
            .await
            .expect_err("later raw normalization should fail");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert_eq!(uuid_calls.load(Ordering::SeqCst), 0);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mixed_reconciliation_preserves_normalization_error_precedence_over_scalar_error() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let prepared_source = key_value_stage_row("prepared", "value", false);
        let prepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![prepared_source.clone()]))
            .await
            .expect("prepared semantic slot");
        let mut scalar_invalid = key_value_stage_row("raw-scalar-invalid", "value", false);
        scalar_invalid.updated_at = Some("not-a-timestamp".to_string());
        let mut schema_invalid = key_value_stage_row("raw-schema-invalid", "value", false);
        schema_invalid.snapshot =
            Some(TransactionJson::from_value_for_test(json!("not-an-object")));
        let mut rows = ReconciledRowBatch::raw(raw_write_rows(vec![
            prepared_source,
            scalar_invalid,
            schema_invalid,
        ]));
        rows.take_raw_rows_at(&[0])
            .expect("semantic source should extract");
        rows.put_prepared_batch_at(&[0], prepared)
            .expect("prepared source should fill its slot");

        let error = transaction
            .prepare_reconciled_rows(rows)
            .await
            .expect_err("normalization must finish before scalar planning");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            !error.message.contains("invalid updated_at timestamp"),
            "generic mixed preparation must preserve normalization precedence: {error:?}"
        );
    }

    async fn open_test_transaction(
        storage: &Memory,
    ) -> (
        Arc<LiveStateContext>,
        Arc<BinaryCasContext>,
        Arc<BranchContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageAdapter::new(storage.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            },
            storage,
            Arc::clone(&live_state),
            Arc::new(TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::wasm::UnsupportedWasmRuntime)),
            Arc::clone(&branch_ctx),
            catalog_context,
            Arc::new(SqlPlanningCache::default()),
            SessionFileViews::default(),
        )
        .await
        .expect("transaction should open");
        let transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        (
            live_state,
            binary_cas,
            branch_ctx,
            runtime_functions,
            transaction,
        )
    }

    async fn seed_visible_schema_rows(storage: StorageAdapter) {
        let rows = crate::schema::seed_schema_definitions()
            .into_iter()
            .map(|schema| {
                let key = crate::schema::schema_key_from_definition(schema)
                    .expect("seed schema key should derive");
                let snapshot_content = json!({ "value": schema }).to_string();
                crate::tracked_state::MaterializedTrackedStateRow {
                    entity_pk: crate::schema::registered_schema_entity_pk(&key.schema_key)
                        .expect("registered schema identity should derive"),
                    schema_key: "lix_registered_schema".to_string(),
                    file_id: None,
                    snapshot_content: Some(snapshot_content.into()),
                    metadata: None,
                    deleted: false,
                    created_at: "1970-01-01T00:00:00.000Z".to_string(),
                    updated_at: "1970-01-01T00:00:00.000Z".to_string(),
                    change_id: ChangeId::for_test_label(&format!(
                        "schema-fixture-{}",
                        key.schema_key
                    )),
                    commit_id: CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
                }
            })
            .collect::<Vec<_>>();
        crate::test_support::seed_branch_head_with_rows(
            storage,
            GLOBAL_BRANCH_ID,
            SCHEMA_FIXTURE_COMMIT_ID,
            &rows,
        )
        .await;
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageAdapter,
        live_state: &LiveStateContext,
        branch_ctx: &BranchContext,
        rejected_entity_pk: &str,
    ) {
        let head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("branch ref should load after failed commit");
        assert_eq!(
            head,
            Some(CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID)),
            "validation failure must not advance the branch ref"
        );
        let row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(rejected_entity_pk),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load after failed commit");
        assert_eq!(
            row, None,
            "validation failure must happen before live-state persistence"
        );
    }

    #[test]
    fn v2_create_contexts_are_retry_stable_and_file_incarnation_scoped() {
        let seed = uuid::Uuid::parse_str("01920000-0000-7000-8000-000000000007")
            .expect("fixture UUIDv7")
            .into_bytes();
        let key = PluginActorKey {
            branch_id: "main".to_string(),
            file_id: "01920000-0000-7000-8000-0000000000a2".to_string(),
            path: "/data.csv".to_string(),
            owner_change_id: "incarnation-a".to_string(),
            plugin_key: "plugin_csv_v2".to_string(),
            plugin_generation: "generation-a".to_string(),
        };
        assert_eq!(v2_create_context(seed, &key), v2_create_context(seed, &key));

        let mut other_file = key.clone();
        other_file.file_id = "01920000-0000-7000-8000-0000000000b2".to_string();
        assert_ne!(
            v2_create_context(seed, &key),
            v2_create_context(seed, &other_file)
        );

        let mut other_incarnation = key.clone();
        other_incarnation.owner_change_id = "incarnation-b".to_string();
        assert_ne!(
            v2_create_context(seed, &key),
            v2_create_context(seed, &other_incarnation)
        );
    }

    fn key_value_stage_row(key: &str, value: &str, untracked: bool) -> TransactionWriteRow {
        TransactionWriteRow {
            entity_pk: Some(EntityPk::single(key)),
            schema_key: "lix_key_value".into(),
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
            branch_id: GLOBAL_BRANCH_ID.into(),
        }
    }
    #[tokio::test]
    async fn reconciled_prepared_slot_freezes_nondeterministic_defaults_for_staging() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let source = TransactionWriteRow {
            entity_pk: None,
            schema_key: "lix_account".into(),
            file_id: None,
            snapshot: Some(TransactionJson::from_value_for_test(json!({
                "name": "Ada",
            }))),
            metadata: None,
            origin: None,
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: GLOBAL_BRANCH_ID.into(),
        };
        let rendered = transaction
            .prepare_transaction_rows(raw_write_rows(vec![source.clone()]))
            .await
            .expect("the semantic row should normalize once");
        let independently_reprepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![source.clone()]))
            .await
            .expect("a second normalization should also succeed");
        assert_ne!(
            rendered.row(0).entity_pk,
            independently_reprepared.row(0).entity_pk,
            "the fixture must prove its UUID default is nondeterministic"
        );

        let mut reconciled_rows = ReconciledRowBatch::raw(raw_write_rows(vec![source]));
        reconciled_rows
            .take_raw_rows_at(&[0])
            .expect("the semantic source should move out exactly once");
        reconciled_rows
            .put_prepared_batch_at(&[0], rendered.clone())
            .expect("the exact rendered row should fill its typed slot");
        let PreparedTransactionWrite::Rows { rows, .. } = transaction
            .prepare_transaction_write(ReconciledTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: reconciled_rows,
            })
            .await
            .expect("final preparation should reuse the prepared slot")
        else {
            panic!("row-only input should stay row-only");
        };
        assert_eq!(
            rows, rendered,
            "durable staging must receive the exact row supplied to entities_changed"
        );
    }

    #[tokio::test]
    async fn reconciled_prepared_slots_follow_raw_row_compaction_in_order() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let first_source = key_value_stage_row("semantic-first", "first", true);
        let second_source = key_value_stage_row("semantic-second", "second", true);
        let prepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![
                first_source.clone(),
                second_source.clone(),
            ]))
            .await
            .expect("semantic sources should prepare");
        let mut first_prepared = prepared.clone();
        first_prepared.select_rows(&[0]);
        let mut second_prepared = prepared;
        second_prepared.select_rows(&[1]);

        // Two independently prepared semantic groups remain in their original
        // relative order while stale raw rows before and between them are
        // compacted and a new engine row is appended.
        let mut rows = ReconciledRowBatch::raw(raw_write_rows(vec![
            key_value_stage_row("remove-before-semantic", "stale", true),
            first_source,
            key_value_stage_row("remove-between-semantic", "stale", true),
            second_source,
            key_value_stage_row("appended-reconciliation", "new", true),
        ]));
        rows.take_raw_rows_at(&[1])
            .expect("first semantic source should move once");
        rows.put_prepared_batch_at(&[1], first_prepared.clone())
            .expect("first prepared slot should retain its position");
        rows.take_raw_rows_at(&[3])
            .expect("second semantic source should move once");
        rows.put_prepared_batch_at(&[3], second_prepared.clone())
            .expect("second prepared slot should retain its position");
        rows.retain_raw(|row| {
            !matches!(
                row.entity_pk
                    .and_then(|entity_pk| entity_pk.as_single_string().ok()),
                Some("remove-before-semantic" | "remove-between-semantic")
            )
        })
        .expect("raw row compaction should preserve typed prepared slots");

        let prepared = transaction
            .prepare_reconciled_rows(rows)
            .await
            .expect("mixed reconciled rows should prepare once in batch order");
        assert_eq!(prepared.len(), 3);
        assert_eq!(prepared.row(0), first_prepared.row(0));
        assert_eq!(prepared.row(1), second_prepared.row(0));
        assert_eq!(
            prepared
                .row(2)
                .entity_pk
                .as_single_string()
                .expect("appended key should stay scalar"),
            "appended-reconciliation",
            "the raw appended row should follow both prepared semantic groups"
        );
    }

    #[test]
    fn reconciled_raw_ordinals_move_canonical_owners_without_row_materialization() {
        let mut values = Vec::new();
        let mut normalized = Vec::new();
        let mut offsets = Vec::new();
        for index in 0..3 {
            let value = json!({"key": format!("canonical-{index}"), "value": index});
            let encoded = serde_json::to_vec(&value).expect("canonical fixture");
            let start = u32::try_from(normalized.len()).expect("canonical offset");
            normalized.extend_from_slice(&encoded);
            let end = u32::try_from(normalized.len()).expect("canonical offset");
            values.push(value);
            offsets.push((start, end));
        }
        let canonical =
            crate::wasm::WasmCanonicalJson::from_batch_parts(values, normalized, offsets, 3, 3)
                .expect("canonical raw batch");
        let arena_probe = canonical[0].clone();
        let rows = canonical
            .into_iter()
            .enumerate()
            .map(|(index, snapshot)| {
                let mut row = key_value_stage_row(&format!("canonical-{index}"), "value", true);
                row.snapshot = Some(TransactionJson::from_canonical_batch(snapshot));
                row
            })
            .collect::<Vec<_>>();
        let mut reconciled = ReconciledRowBatch::raw(raw_write_rows(rows));

        let moved = reconciled
            .take_raw_rows_at(&[2, 0])
            .expect("noncontiguous raw ordinals should move in requested order");
        assert_eq!(moved.len(), 2);
        assert_eq!(
            moved
                .row(0)
                .entity_pk
                .expect("moved identity")
                .as_single_string()
                .expect("scalar identity"),
            "canonical-2"
        );
        let moved_first = moved
            .row(0)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("moved canonical owner");
        let moved_second = moved
            .row(1)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("moved canonical owner");
        assert!(moved_first.shares_batch_with(moved_second));
        assert!(moved_second.shares_batch_with(&arena_probe));

        let remaining = reconciled
            .take_raw_rows_at(&[1])
            .expect("unmoved raw ordinal must remain addressable");
        let remaining = remaining
            .row(0)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("remaining canonical owner");
        assert!(remaining.shares_batch_with(&arena_probe));
    }

    #[tokio::test]
    async fn direct_semantic_rendering_pages_oversized_snapshot_as_lazy_attachment() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let limits = WasmTransitionLimits::default();
        let large_value = "x".repeat(limits.max_record_bytes as usize + 32);
        let prepared = transaction
            .prepare_transaction_rows(raw_write_rows(vec![key_value_stage_row(
                "large-semantic-entity",
                &large_value,
                true,
            )]))
            .await
            .expect("large semantic row should normalize");
        let expected = prepared
            .row(0)
            .snapshot
            .expect("semantic upsert should carry a snapshot")
            .materialize_shared();

        // Semantic normalization is domain-aware. Normalize against the
        // seeded global catalog first, then shape the already-prepared row as
        // the branch-local, file-scoped plugin output this renderer consumes.
        // This preserves the original fixture's boundary without teaching
        // normalization that the synthetic, unseeded `main` branch is valid.
        let source = prepared.row(0);
        assert!(source.global);
        assert!(source.untracked);
        assert!(source.file_id.is_none());
        let mut semantic_rows = PreparedStateBatch::with_capacity(1);
        semantic_rows.push_parts(
            source.schema_plan_id,
            source.facts,
            source.entity_pk.clone(),
            source.schema_key.clone(),
            Some("large-file".into()),
            source.snapshot.cloned(),
            source.metadata.cloned(),
            source.origin.cloned(),
            source.origin_key,
            source.created_at,
            source.updated_at,
            false,
            source.change_id,
            source.commit_id,
            false,
            "main".into(),
        );

        let changes = v2_host_changes_from_prepared_rows(&semantic_rows, limits)
            .expect("direct semantic rendering should select a lazy snapshot source");
        let WasmEntityChange::Upsert { entity, .. } = &changes.changes[0] else {
            panic!("prepared semantic snapshot should become an upsert")
        };
        let WasmHostBytes::Source(slice) = &entity.snapshot_content else {
            panic!("an oversized direct semantic snapshot must not be packet-inline")
        };
        assert_eq!(slice.range.offset, 0);
        assert_eq!(slice.range.length, expected.len() as u64);
        assert_eq!(
            slice.source.read(0, 64).expect("lazy prefix should read"),
            expected.as_bytes()[..64]
        );

        let mut source = VecEntityChangeSource::new(changes, limits)
            .expect("lazy semantic change should fit the packet bounds");
        let page =
            crate::wasm::WasmEntityChangeSource::next_page(&mut source, limits.max_page_bytes)
                .expect("semantic renderer packet should page")
                .expect("one semantic change page should be emitted");
        assert_eq!(page.changes.len(), 1);
        assert!(matches!(
            page.changes[0],
            WasmEntityChange::Upsert {
                entity: WasmEntity {
                    snapshot_content: WasmHostBytes::Source(_),
                    ..
                },
                ..
            }
        ));
    }
}
