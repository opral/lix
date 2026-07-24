#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::sync::Mutex;

use async_trait::async_trait;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobDataReader, BlobHash};
use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchContext, BranchRefReader, branch_ref_stage_row};
use crate::catalog::{
    CatalogContext, CatalogFingerprint, CatalogSnapshot, load_catalog_revision,
    stage_catalog_revision,
};
use crate::changelog::{ChangeId, CommitId};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::common::LixTimestamp;
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    BlobRefRowInput, FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemPathKind, FilesystemRowContext, blob_ref_row,
    load_path_index_revision,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::{
    LiveStateContext, LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter,
    LiveStateProjection, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::live_state::{overlay_load_exact_rows, overlay_scan_rows};
use crate::plugin::{
    ArcByteSource, BoundIdNamespace, CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY,
    PluginActorCache, PluginActorColdInstall, PluginActorColdOpen, PluginActorKey,
    PluginActorLease, PluginActorStore, PluginArchiveInstallPlan, PluginContentType,
    PluginDetectedChange, PluginFileOwner, PluginObservation, PluginRegistry, PluginRegistryEntry,
    PluginRegistryEntryInput, PluginRuntimeHost, V2SchemaAllowlist, VecEntityChangeSource,
    VecEntitySource, build_file_update_splices, drain_entity_transition_edits,
    drain_file_transition_changes, host_entity_change_with_lazy_snapshot,
    host_entity_with_lazy_snapshot, inferred_media_type_for_path, is_plugin_storage_path,
    is_reservation_key, local_mutation_identity, plugin_install_plan_from_archive_path,
    plugin_key_from_archive_file_id, plugin_state_live_state_projection,
    require_existing_id_authorities, reservation_tombstone_row, reserve_namespace_row,
    transport_splice_preserves_utf8, validate_host_allocated_changes,
    validate_namespace_reservation,
};
use crate::session::{SessionMode, WORKSPACE_BRANCH_KEY};
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SessionFileViewKey, SessionFileViewMutation,
    SessionFileViews, SessionPluginFileView, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::sql2::{SqlPlanningCache, SqlWriteExecutionContext};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    Memory, StorageReadOptions, StorageWriteOptions, StorageWriteSetStats,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
};
use crate::tracked_state::{TrackedStateContext, TrackedStateStoreReader};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedTransactionWriteRow, REGISTERED_SCHEMA_KEY, normalize_transaction_write_row,
    remember_pending_registered_schema,
};
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{PreparedWriteSet, TransactionWriteBuffer};
use crate::transaction::types::{
    PreparedStateRow, PreparedTransactionWrite, StagedCommitChangeRef, TransactionFileData,
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOperation,
    TransactionWriteOrigin, TransactionWriteOutcome, TransactionWriteRow, stage_json_from_value,
};
use crate::transaction::validation::{TransactionValidationInput, validate_prepared_writes};
use crate::wasm::{
    WasmChangeEffect, WasmComponentV2Actor, WasmComponentV2Factory, WasmDocumentHandle,
    WasmEntityChange, WasmEntityUpdate, WasmFileDescriptor, WasmFileUpdate, WasmHostBytes,
    WasmHostEntity, WasmHostEntityChanges, WasmOpenEntitiesInput, WasmOpenFileInput,
    WasmPluginSelection, WasmTransitionLimits,
};
use crate::{LixError, NullableKeyFilter, SqlQueryResult, Value};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome {
    pub(crate) storage_stats: StorageWriteSetStats,
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
    commit_boundary: Option<TransactionCommitBoundary>,
    origin_key: Option<String>,
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
            let runtime_functions = {
                let runtime_live_state = live_state.reader(&read);
                FunctionContext::prepare(&runtime_live_state).await?
            };
            let functions = runtime_functions.provider();
            let schema_catalog = {
                let catalog_revision = load_catalog_revision(&read).await?;
                let visible_live_state = live_state.reader(&read);
                catalog_context
                    .compiled_catalog_for_transaction_open(
                        &visible_live_state,
                        &Domain::schema_catalog(active_branch_id.clone(), true),
                        catalog_revision.as_ref(),
                    )
                    .await?
            };
            Ok::<_, LixError>((
                active_branch_id,
                runtime_functions,
                functions,
                schema_catalog,
            ))
        }
        .await;
        let (active_branch_id, runtime_functions, functions, schema_catalog) = match setup_result {
            Ok(result) => result,
            Err(error) => {
                return Err(error);
            }
        };
        drop(read);
        let mut schema_resolver = TransactionSchemaResolver::new(Arc::clone(&catalog_context));
        schema_resolver.remember_compiled_catalog(
            &Domain::schema_catalog(active_branch_id.clone(), true),
            Arc::clone(&schema_catalog),
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
                sql_schema_snapshot: schema_catalog,
                sql_planning_cache,
                staged_writes,
                filesystem_path_index_cache: Arc::new(FilesystemPathIndexCache::default()),
                filesystem_path_index_epoch: Arc::new(AtomicUsize::new(0)),
                storage,
                functions,
                commit_boundary: None,
                origin_key: None,
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
        let catalog_revision_changed = prepared_writes_change_catalog(&prepared_writes);
        let _commit_guard = begin_commit_boundary(commit_boundary.as_ref());
        if let Err(error) = check_commit_boundary(commit_boundary.as_ref()) {
            transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return Err(error);
        }
        if let Err(error) = transaction
            .validate_prepared_writes_by_branch(&prepared_writes)
            .await
        {
            transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return Err(error);
        }
        let filesystem_delta_rows = if prepared_writes
            .state_rows
            .iter()
            .any(|row| row.schema_key == BRANCH_REF_SCHEMA_KEY)
        {
            Vec::new()
        } else {
            prepared_writes
                .state_rows
                .iter()
                .filter(|row| {
                    matches!(
                        row.schema_key.as_str(),
                        "lix_file_descriptor" | "lix_directory_descriptor"
                    )
                })
                .cloned()
                .map(MaterializedLiveStateRow::from)
                .collect::<Vec<_>>()
        };
        let commit_read_storage = transaction.storage.clone();
        let mut read = SharedStorageAdapterRead::new(
            commit_read_storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let previous_filesystem_revision = if filesystem_delta_rows.is_empty() {
            None
        } else {
            load_path_index_revision(&read).await.ok().flatten()
        };
        let mut writes = match commit::commit_prepared_writes(
            &transaction.binary_cas,
            transaction.branch_ctx.as_ref(),
            transaction.live_state.index(),
            Some(runtime_functions),
            &mut read,
            prepared_writes,
        )
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
        // Keep the prepared commit's storage borrow independent from the
        // transaction so deterministic preparation failures can still drain
        // prospective plugin actor documents before returning.
        let commit_storage = transaction.storage.clone();
        let prepared_commit = match commit_storage
            .prepare_write_set(writes, StorageWriteOptions::default())
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

    pub(crate) fn attach_commit_boundary(&mut self, boundary: TransactionCommitBoundary) {
        self.commit_boundary = Some(boundary);
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
    /// transaction still owns preparation from `TransactionWriteRow` into
    /// `PreparedStateRow`, so generated timestamps, change ids, commit ids, and
    /// commit change refs stay in one place.
    pub(crate) async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        if !transaction_write_has_plugin_lifecycle_candidate(&write) {
            // Acquire before normalization, plugin/state reads, or actor work.
            // The owned guard remains on this transaction through its durable
            // commit, so an upgrade cannot preflight across an in-flight
            // ordinary mutation and then swap authority ahead of it.
            self.ensure_plugin_generation_read_guard().await;
        }
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
        self.require_existing_transaction_write_branch_ids(&write)
            .await?;
        let (write, file_view_mutations, actor_publications, prepared_semantic_rows) =
            self.reconcile_plugin_write(write).await?;
        if let Err(error) = require_valid_transaction_write_storage_scopes(&write) {
            discard_plugin_actor_publications(actor_publications).await;
            return Err(error);
        }
        let write = match self
            .prepare_transaction_write(write, prepared_semantic_rows)
            .await
        {
            Ok(write) => write,
            Err(error) => {
                discard_plugin_actor_publications(actor_publications).await;
                return Err(error);
            }
        };
        if prepared_transaction_write_affects_filesystem_path_index(&write) {
            // TransactionWriteBuffer may retain an earlier row from this batch even
            // when a later row makes staging fail, so invalidate before staging.
            self.filesystem_path_index_epoch
                .fetch_add(1, Ordering::SeqCst);
        }
        let outcome = match self.staged_writes.stage_write(write) {
            Ok(outcome) => outcome,
            Err(error) => {
                discard_plugin_actor_publications(actor_publications).await;
                return Err(error);
            }
        };
        self.pending_file_view_mutations.extend(file_view_mutations);
        self.pending_plugin_actor_publications
            .extend(actor_publications);
        Ok(outcome)
    }

    async fn scan_visible_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read);
        overlay_scan_rows(&base, &staged, request).await
    }

    async fn visible_v2_materialization_root(
        &mut self,
        key: &PluginFileWriteKey,
    ) -> Result<Option<String>, LixError> {
        let rows = self
            .scan_visible_live_state(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    entity_pks: vec![EntityPk::single(key.file_id.clone())],
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
                    "v2 materialization root lookup returned duplicate rows for file '{}'",
                    key.file_id
                ),
            ));
        }
        rows.into_iter()
            .next()
            .map(|row| {
                row.change_id.map(|root| root.to_string()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "v2 materialization root for file '{}' is missing change_id",
                            key.file_id
                        ),
                    )
                })
            })
            .transpose()
    }

    async fn cold_open_v2_semantic_actor(
        &mut self,
        actor_key: &PluginActorKey,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentV2Factory>,
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
        let blob_rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    entity_pks: vec![EntityPk::single(actor_key.file_id.clone())],
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
        let [blob_row] = blob_rows.as_slice() else {
            return Err(LixError::new(
                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                format!(
                    "owned v2 plugin file '{}' must have exactly one visible materialization; found {}",
                    actor_key.file_id,
                    blob_rows.len()
                ),
            ));
        };
        let semantic_root = blob_row
            .change_id
            .map(|root| root.to_string())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{}' materialization is missing its semantic root",
                        actor_key.file_id
                    ),
                )
            })?;
        let mut cold_install: PluginActorColdInstall =
            match cache.prepare_cold_open(actor_key, &semantic_root).await? {
                PluginActorColdOpen::Ready(observation) => return Ok(observation),
                PluginActorColdOpen::Build(cold_install) => cold_install,
            };
        let store_permit = cache.admit_cold_store(&mut cold_install)?;
        let snapshot = blob_row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned v2 plugin file '{}' materialization is missing its blob reference",
                    actor_key.file_id
                ),
            )
        })?;
        let snapshot: PluginUpgradeBlobRefSnapshot =
            serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned v2 plugin file '{}' has an invalid blob reference: {error}",
                        actor_key.file_id
                    ),
                )
            })?;
        if snapshot.id != actor_key.file_id {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned v2 plugin file '{}' materialization identity does not match its file scope",
                    actor_key.file_id
                ),
            ));
        }
        let hash = BlobHash::from_hex(&snapshot.blob_hash)?;
        let base_blob_reader = self.binary_cas.reader(read);
        let materialized_bytes: crate::Blob =
            load_transaction_blob_bytes(&base_blob_reader, &self.staged_writes, &[hash])
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
                            actor_key.file_id, snapshot.blob_hash
                        ),
                    )
                })?
                .into();
        let rows = overlay_scan_rows(
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
        let rows = rows
            .into_iter()
            .filter(|row| {
                row.branch_id == file_key.branch_id
                    && row.file_id.as_deref() == Some(file_key.file_id.as_str())
                    && !row.global
                    && !row.untracked
                    && row.snapshot_content.is_some()
                    && plugin.schema_keys().binary_search(&row.schema_key).is_ok()
            })
            .collect::<Vec<_>>();
        let entity_count = rows.len();
        let limits = WasmTransitionLimits::default();
        let source = VecEntitySource::new(v2_host_entities_from_live_rows(rows, limits)?, limits)?;
        let mut actor = factory.instantiate_actor().await?;
        let transition = match actor
            .open_entities(
                limits,
                WasmOpenEntitiesInput {
                    descriptor,
                    entities: Box::new(source),
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
        let validated = match drain_entity_transition_edits(
            actor.as_mut(),
            transition,
            &[],
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
                validated.bytes_sha256,
                Arc::<str>::from(semantic_root),
            )
            .await
    }

    async fn load_visible_exact_live_state_rows(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.live_state.reader(read);
        overlay_load_exact_rows(&base, &staged, request).await
    }

    /// Drops `format-only` upserts that are semantically identical to the
    /// currently accepted durable entity. The exact-row lookup keeps this
    /// proportional to the sparse format-only output instead of hydrating the
    /// complete file graph.
    async fn suppress_v2_format_only_noops(
        &mut self,
        changes: WasmHostEntityChanges,
        file_key: &PluginFileWriteKey,
    ) -> Result<WasmHostEntityChanges, LixError> {
        let format_only_keys = changes
            .changes
            .iter()
            .filter_map(|change| match change {
                WasmEntityChange::Upsert {
                    entity,
                    effect: WasmChangeEffect::FormatOnly,
                } => Some(entity.key.clone()),
                WasmEntityChange::Upsert { .. } | WasmEntityChange::Delete(_) => None,
            })
            .collect::<Vec<_>>();
        if format_only_keys.is_empty() {
            return Ok(changes);
        }

        let requests = format_only_keys
            .iter()
            .map(|key| {
                Ok(LiveStateExactRowRequest {
                    schema_key: key.schema_key.clone(),
                    branch_id: file_key.branch_id.clone(),
                    entity_pk: EntityPk::from_parts(key.entity_pk.clone()).map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("v2 plugin emitted invalid entity_pk: {error}"),
                        )
                    })?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let current = self
            .load_visible_exact_live_state_rows(&LiveStateExactBatchRequest {
                rows: requests,
                projection: plugin_state_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let accepted = format_only_keys
            .into_iter()
            .zip(current)
            .collect::<BTreeMap<_, _>>();
        suppress_v2_format_only_noops_against_rows(changes, &accepted)
    }

    /// Validates host-allocated entity identities and returns the one durable
    /// namespace reservation row (if this transition is the first use of the
    /// namespace). Exact authority reads are proportional to sparse changed
    /// keys; a cold import whose IDs all use the supplied namespace performs
    /// only the single reservation lookup, independent of row count.
    async fn v2_id_namespace_rows(
        &mut self,
        plugin: &PluginRegistryEntry,
        changes: &WasmHostEntityChanges,
        bound: BoundIdNamespace,
        file_key: &PluginFileWriteKey,
        existing_reservation: Option<&MaterializedLiveStateRow>,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let validation = validate_host_allocated_changes(plugin, changes, bound)?;
        if !validation.requires_reservation && validation.existing_authorities.is_empty() {
            return Ok(Vec::new());
        }

        let exact_rows = validation
            .existing_authorities
            .iter()
            .map(|key| {
                Ok(LiveStateExactRowRequest {
                    schema_key: key.schema_key.clone(),
                    branch_id: file_key.branch_id.clone(),
                    entity_pk: EntityPk::from_parts(key.entity_pk.clone()).map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("v2 plugin emitted invalid host-allocated entity_pk: {error}"),
                        )
                    })?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let authority_count = validation.existing_authorities.len();
        let loaded = if exact_rows.is_empty() {
            Vec::new()
        } else {
            self.load_visible_exact_live_state_rows(&LiveStateExactBatchRequest {
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
            &loaded[..authority_count],
            &file_key.file_id,
            &file_key.branch_id,
        )?;

        let mut rows = Vec::new();
        if validation.requires_reservation {
            if let Some(row) = reserve_namespace_row(
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

    async fn preflight_v2_id_namespace(
        &mut self,
        bound: BoundIdNamespace,
        file_key: &PluginFileWriteKey,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        let reservation_key = bound.reservation_key();
        let mut loaded = self
            .load_visible_exact_live_state_rows(&LiveStateExactBatchRequest {
                rows: vec![LiveStateExactRowRequest {
                    schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                    branch_id: file_key.branch_id.clone(),
                    entity_pk: EntityPk::single(reservation_key),
                    file_id: Some(file_key.file_id.clone()),
                }],
                projection: plugin_state_live_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let existing = loaded.pop().flatten();
        validate_namespace_reservation(
            existing.as_ref(),
            bound,
            &file_key.file_id,
            &file_key.branch_id,
        )?;
        Ok(existing)
    }

    async fn v2_id_reservation_tombstones(
        &mut self,
        file_key: &PluginFileWriteKey,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let rows = self
            .scan_visible_live_state(&LiveStateScanRequest {
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
        rows.into_iter()
            .filter_map(|row| {
                let key = row.entity_pk.as_single_string().ok()?.to_string();
                is_reservation_key(&key).then_some(reservation_tombstone_row(
                    &key,
                    &file_key.file_id,
                    &file_key.branch_id,
                ))
            })
            .collect()
    }

    async fn reconcile_plugin_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<
        (
            TransactionWrite,
            BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
            Vec<PendingPluginActorPublication>,
            PreparedSemanticRows,
        ),
        LixError,
    > {
        match write {
            TransactionWrite::Rows { mode, mut rows } => {
                reject_external_plugin_registry_rows(&rows)?;
                let count = rows.len() as u64;
                let mut file_data = Vec::new();
                let mut reconciliation = self
                    .plugin_write_reconciliation(&rows, &mut file_data)
                    .await?;
                mark_plugin_reconciliation_rows(&mut reconciliation.rows);
                rows.extend(reconciliation.rows);
                if !file_data.is_empty() {
                    for (file_key, version) in &reconciliation.materialization_versions {
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
                        let mut materialized_row = blob_ref_row(BlobRefRowInput {
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
                        })?;
                        materialized_row.change_id = Some(version.clone());
                        mark_plugin_reconciliation_rows(std::slice::from_mut(
                            &mut materialized_row,
                        ));
                        rows.push(materialized_row);
                    }
                }
                let write = if file_data.is_empty() {
                    TransactionWrite::Rows { mode, rows }
                } else {
                    TransactionWrite::RowsWithFileData {
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
                    reconciliation.prepared_semantic_rows,
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
                let PluginWriteReconciliation {
                    file_keys,
                    materialized_file_keys,
                    materialization_versions,
                    rows: mut plugin_rows,
                    file_view_mutations,
                    actor_publications,
                    prepared_semantic_rows,
                } = self
                    .plugin_write_reconciliation(&rows, &mut file_data)
                    .await?;
                mark_plugin_reconciliation_rows(&mut plugin_rows);
                for (file_key, version) in &materialization_versions {
                    let matching_indexes = rows
                        .iter_mut()
                        .enumerate()
                        .filter_map(|(index, row)| {
                            file_key.matches_blob_ref_row(row).then_some(index)
                        })
                        .collect::<Vec<_>>();
                    if matching_indexes.len() > 1 {
                        discard_plugin_actor_publications(actor_publications).await;
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "v2 plugin materialization expected at most one blob-ref row for file '{}', found {}",
                                file_key.file_id,
                                matching_indexes.len()
                            ),
                        ));
                    }
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
                    let mut materialized_row = blob_ref_row(BlobRefRowInput {
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
                    })?;
                    materialized_row.change_id = Some(version.clone());
                    if let Some(index) = matching_indexes.into_iter().next() {
                        rows[index].snapshot = materialized_row.snapshot;
                        rows[index].change_id = materialized_row.change_id;
                    } else {
                        rows.push(materialized_row);
                    }
                }
                rows.retain(|row| !file_keys.iter().any(|key| key.matches_blob_ref_row(row)));
                rows.extend(plugin_rows);
                let file_data = file_data
                    .into_iter()
                    .filter(|write| {
                        let key = PluginFileWriteKey::from(write);
                        !file_keys.contains(&key)
                            && (!write.is_empty() || materialized_file_keys.contains(&key))
                    })
                    .collect();
                Ok((
                    TransactionWrite::RowsWithFileData {
                        mode,
                        rows,
                        file_data,
                        count,
                    },
                    file_view_mutations,
                    actor_publications,
                    prepared_semantic_rows,
                ))
            }
        }
    }

    fn acknowledged_session_plugin_observation(
        &self,
        key: &SessionFileViewKey,
        plugin: &PluginRegistryEntry,
        owner_change_id: &str,
    ) -> Option<PluginObservation> {
        if let Some(mutation) = self.pending_file_view_mutations.get(key) {
            return match mutation {
                SessionFileViewMutation::Set { view, .. }
                    if view.plugin_key == plugin.key()
                        && view.plugin_generation == plugin.archive_blob_hash()
                        && view.owner_change_id == owner_change_id =>
                {
                    view.observation.clone()
                }
                SessionFileViewMutation::Set { .. } | SessionFileViewMutation::Remove { .. } => {
                    None
                }
            };
        }
        self.session_file_views
            .plugin_file_view(
                key,
                plugin.key(),
                plugin.archive_blob_hash(),
                owner_change_id,
            )
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
        input_rows: &[TransactionWriteRow],
        file_data: &mut Vec<TransactionFileData>,
    ) -> Result<PluginWriteReconciliation, LixError> {
        let mut reconciliation = PluginWriteReconciliation::default();
        let mut lifecycle = BTreeMap::<PluginLifecycleKey, Option<PluginRegistryEntry>>::new();
        let mut lifecycle_schema_rows = Vec::<(PluginLifecycleKey, TransactionWriteRow)>::new();
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
                host_allocated_schema_keys: parsed.host_allocated_schema_keys.clone(),
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
            lifecycle_schema_rows.extend(
                schema_rows
                    .into_iter()
                    .map(|row| (lifecycle_key.clone(), row)),
            );
            branch_ids.insert(write.branch_id.clone());
        }

        // A canonical archive descriptor tombstone is the uninstall signal.
        // Other descriptor tombstones are ownership cleanup candidates.
        let mut deleted_file_keys = BTreeMap::<PluginFileWriteKey, Option<TransactionJson>>::new();
        for row in input_rows {
            if row.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || row.snapshot.is_some() {
                continue;
            }
            let Some(file_id) = row
                .entity_pk
                .as_ref()
                .and_then(|entity_pk| entity_pk.as_single_string().ok())
            else {
                continue;
            };
            if let Some(plugin_key) = plugin_key_from_archive_file_id(file_id) {
                if row.global || row.untracked || row.branch_id == GLOBAL_BRANCH_ID {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "plugin uninstall requires a tracked branch-local archive",
                    ));
                }
                let lifecycle_key = PluginLifecycleKey {
                    branch_id: row.branch_id.clone(),
                    plugin_key,
                };
                if lifecycle.insert(lifecycle_key, None).is_some() {
                    return Err(duplicate_plugin_lifecycle_mutation());
                }
                branch_ids.insert(row.branch_id.clone());
                continue;
            }
            if row.global || row.untracked {
                continue;
            }
            let key = PluginFileWriteKey {
                branch_id: row.branch_id.clone(),
                global: false,
                untracked: false,
                file_id: file_id.to_string(),
            };
            deleted_file_keys
                .entry(key)
                .or_insert_with(|| row.metadata.clone());
            branch_ids.insert(row.branch_id.clone());
        }

        // Registered-schema writes are rare lifecycle operations, but they
        // must still consult the registry even when no file data is present.
        // Otherwise a later public UPDATE/DELETE could invalidate an active
        // plugin's durable state contract behind the registry's back.
        for row in input_rows {
            if row.schema_key == REGISTERED_SCHEMA_KEY && !row.global && !row.untracked {
                branch_ids.insert(row.branch_id.clone());
            }
        }

        // Ordinary semantic DML carries no filesystem payload. A tracked,
        // file-scoped row may nevertheless belong to an active plugin and
        // therefore needs the small branch registry lookup before the host can
        // decide whether an entity-to-file transition is required.
        for row in input_rows {
            if !row.global && !row.untracked && row.file_id.is_some() {
                branch_ids.insert(row.branch_id.clone());
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
            for (lifecycle_key, row) in &lifecycle_schema_rows {
                let entity_pk = row.entity_pk.clone().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its entity identity",
                    )
                })?;
                let snapshot = row.snapshot.as_ref().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its definition",
                    )
                })?;
                let identity = (row.branch_id.clone(), entity_pk);
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

            let schema_rows = overlay_scan_rows(
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
            for row in schema_rows {
                let Some(snapshot) = row.snapshot_content.as_deref() else {
                    continue;
                };
                existing_schemas.insert(
                    (row.branch_id, row.entity_pk),
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
            for row in input_rows {
                if row.schema_key != REGISTERED_SCHEMA_KEY
                    || row.global
                    || row.untracked
                    || row.file_id.is_some()
                {
                    continue;
                }
                let Some(entity_pk) = row.entity_pk.clone() else {
                    continue;
                };
                let identity = (row.branch_id.clone(), entity_pk);
                if !desired_schemas.contains_key(&identity) {
                    continue;
                }
                match row.snapshot.as_ref() {
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
            reconciliation
                .rows
                .extend(lifecycle_schema_rows.into_iter().map(|(_, row)| row));
        }

        let registry_rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                    entity_pks: vec![EntityPk::single(PLUGIN_REGISTRY_KEY)],
                    branch_ids: branch_ids.iter().cloned().collect(),
                    file_ids: vec![NullableKeyFilter::Null],
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut registry_rows_by_branch = BTreeMap::<String, MaterializedLiveStateRow>::new();
        for row in registry_rows {
            if registry_rows_by_branch
                .insert(row.branch_id.clone(), row)
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "durable plugin registry lookup returned duplicate branch rows",
                ));
            }
        }

        let mut registries = BTreeMap::<String, PluginRegistry>::new();
        let mut changed_registry_branches = BTreeSet::<String>::new();
        let mut generation_upgrades = Vec::<PluginGenerationUpgrade>::new();
        for branch_id in &branch_ids {
            registries.insert(
                branch_id.clone(),
                PluginRegistry::from_optional_live_state_row(
                    registry_rows_by_branch.get(branch_id),
                    branch_id,
                )?,
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
                    registry.remove(&key.plugin_key)?;
                }
            }
            changed_registry_branches.insert(key.branch_id);
        }
        for row in input_rows {
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
            let Some(plugin) = registries.get(&row.branch_id).and_then(|registry| {
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
        for branch_id in changed_registry_branches {
            reconciliation.rows.push(
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
            return Ok(reconciliation);
        }

        let owner_branch_ids = active_branch_ids
            .iter()
            .cloned()
            .chain(deleted_file_keys.keys().map(|key| key.branch_id.clone()))
            .collect::<BTreeSet<_>>();
        let mut candidate_file_ids = BTreeSet::<String>::new();
        for write in file_data.iter() {
            if write.global
                || write.untracked
                || !active_branch_ids.contains(&write.branch_id)
                || write.path.as_deref().is_none_or(is_plugin_storage_path)
            {
                continue;
            }
            candidate_file_ids.insert(write.file_id.clone());
        }
        for key in deleted_file_keys.keys() {
            candidate_file_ids.insert(key.file_id.clone());
        }
        for row in input_rows {
            if row.global
                || row.untracked
                || !active_branch_ids.contains(&row.branch_id)
                || row.file_id.is_none()
            {
                continue;
            }
            candidate_file_ids.extend(row.file_id.iter().cloned());
        }
        if candidate_file_ids.is_empty() {
            return Ok(reconciliation);
        }

        let owner_rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                    entity_pks: vec![EntityPk::single(PLUGIN_OWNER_KEY)],
                    branch_ids: owner_branch_ids.into_iter().collect(),
                    file_ids: candidate_file_ids
                        .iter()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_live_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut owners = BTreeMap::<PluginFileWriteKey, PluginFileOwner>::new();
        let mut owner_change_ids = BTreeMap::<PluginFileWriteKey, String>::new();
        for row in owner_rows {
            let branch_id = row.branch_id.clone();
            let Some(owner) = PluginFileOwner::from_live_state_row(&row, &branch_id)? else {
                continue;
            };
            let owner_change_id = row.change_id.ok_or_else(|| {
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
        let mut unresolved_semantic_groups = BTreeMap::<
            PluginFileWriteKey,
            (PluginRegistryEntry, String, Vec<TransactionWriteRow>),
        >::new();
        for row in input_rows {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            if row.global || row.untracked || !active_branch_ids.contains(&row.branch_id) {
                continue;
            }
            let registry = registries
                .get(&row.branch_id)
                .expect("active semantic-write branch has a registry");
            let schema_is_plugin_owned = registry
                .plugins()
                .iter()
                .any(|plugin| plugin.schema_keys().binary_search(&row.schema_key).is_ok());
            if !schema_is_plugin_owned {
                continue;
            }
            let file_key = PluginFileWriteKey {
                branch_id: row.branch_id.clone(),
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
            if plugin.schema_keys().binary_search(&row.schema_key).is_err()
                || owner.schema_keys().binary_search(&row.schema_key).is_err()
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
            group.2.push(row.clone());
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
            for (file_key, (plugin, owner_change_id, rows)) in unresolved_semantic_groups {
                let entries = path_index
                    .exact_file_id_entries(&file_key.file_id)
                    .into_iter()
                    .filter(|entry| {
                        let live = entry.live_row();
                        entry.kind == FilesystemPathKind::File
                            && entry.id() == file_key.file_id
                            && live.branch_id == file_key.branch_id
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
                        rows,
                    },
                );
            }
        }

        let mut selected_plugins = BTreeMap::<PluginFileWriteKey, PluginRegistryEntry>::new();
        let mut full_content_classification_bytes = BTreeMap::<PluginFileWriteKey, u64>::new();
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
            // is unchanged. Text content constraints can be preserved by
            // validating the trusted splice's bounded UTF-8 window; all
            // inconclusive, binary, blind, cold, or path-reselected writes use
            // the ordinary full-payload classifier below.
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
                            observation
                                .bytes_sha256()
                                .matches_lower_hex(provenance.base_sha256())
                                && transport_splice_preserves_utf8(write.data(), provenance)
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
                full_content_classification_bytes.insert(file_key.clone(), classified_bytes);
            }
            let Some(plugin) = plugin else {
                continue;
            };
            selected_plugins.insert(file_key, plugin.clone());
        }

        let mut state_groups = BTreeMap::<PluginStateGroupKey, PluginStateGroup>::new();
        for (key, owner) in &owners {
            let selected = selected_plugins.get(key);
            // A same-owner v2 write is authorized by an exact document
            // observation and must not hydrate the complete durable graph.
            // Lifecycle removal/reselection still needs the old rows so it
            // can tombstone every schema owned by the previous plugin.
            if selected.is_some_and(|selected| selected.key() == owner.plugin_key()) {
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
            if let Some(selected) = selected
                && selected.key() == owner.plugin_key()
            {
                group
                    .schema_keys
                    .extend(selected.schema_keys().iter().cloned());
            }
        }
        let mut state_by_file =
            BTreeMap::<PluginStateFileKey, Vec<MaterializedLiveStateRow>>::new();
        for (group_key, group) in state_groups {
            let rows = overlay_scan_rows(
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
            for row in rows {
                let Some(file_id) = row.file_id.clone() else {
                    continue;
                };
                state_by_file
                    .entry(PluginStateFileKey {
                        branch_id: group_key.branch_id.clone(),
                        plugin_key: group_key.plugin_key.clone(),
                        file_id,
                    })
                    .or_default()
                    .push(row);
            }
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
            let factory = self.plugin_host.load_or_compile_v2_factory(&plugin).await?;
            component_v2_factories.insert(key, factory);
        }

        let mut reconciled_file_keys = BTreeSet::<PluginFileWriteKey>::new();
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
                    state_by_file.get(&PluginStateFileKey {
                        branch_id: write.branch_id.clone(),
                        plugin_key: owner.plugin_key().to_string(),
                        file_id: write.file_id.clone(),
                    })
                })
                .cloned()
                .unwrap_or_default();

            if owner.is_some_and(|owner| {
                selected.is_none_or(|selected| selected.key() != owner.plugin_key())
            }) {
                reconciliation.rows.extend(plugin_state_tombstone_rows(
                    &old_state,
                    &write.file_id,
                    &context,
                ));
                reconciliation
                    .rows
                    .extend(self.v2_id_reservation_tombstones(&file_key).await?);
            }

            let Some(selected) = selected else {
                reconciliation.remove_session_file_view(SessionFileViewKey::new(
                    &write.branch_id,
                    &write.file_id,
                ));
                if owner.is_some() {
                    reconciliation.rows.push(PluginFileOwner::delete_row(
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
                reconciliation.rows.push(owner_row);
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
            let schemas = V2SchemaAllowlist::from_slice(selected.schema_keys())?;
            let mutation_identity = write.mutation_identity().unwrap_or_else(|| {
                local_mutation_identity(self.functions.call_uuid_v7().into_bytes())
            });
            let bound_ids = BoundIdNamespace::bind(mutation_identity, &actor_key);
            let ids = bound_ids.ids();
            let existing_id_namespace_reservation =
                match self.preflight_v2_id_namespace(bound_ids, &file_key).await {
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

            let (changes, publication, materialized_bytes) = if same_plugin_owner {
                let observation = self
                    .acknowledged_session_plugin_observation(
                        &view.session_key,
                        selected,
                        current_owner_change_id
                            .as_deref()
                            .expect("same-owner v2 file should have an owner incarnation"),
                    )
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "warm v2 file writes require an exact acknowledged file read",
                        )
                        .with_hint("read the exact file bytes again before retrying the edit")
                    })?;
                if !v2_actor_key_is_descriptor_successor(observation.key(), &actor_key) {
                    return Err(LixError::new(
                        LixError::CODE_PLUGIN_OBSERVATION_STALE,
                        "the acknowledged v2 file identity no longer matches this write",
                    )
                    .with_hint("read the exact file bytes again before retrying the edit"));
                }
                let before_descriptor = v2_file_descriptor_from_actor_key(observation.key());
                let after_descriptor = descriptor.clone();
                let cache = self.plugin_host.actor_cache();
                // Acquire serialization first, then read the root again.
                // A second local session may have committed while this
                // request waited for the actor; reading before the lease
                // would mistake that valid serialization for an external
                // stale-cache race.
                let mut lease = cache.lease_for_transition(&observation).await?;
                let visible_root = self
                    .visible_v2_materialization_root(&file_key)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "the acknowledged v2 file no longer has a visible materialization root",
                        )
                        .with_hint("read the exact file bytes again before retrying the edit")
                    })?;
                lease.require_accepted_semantic_root(&visible_root)?;
                let observation_is_current = observation.semantic_root() == visible_root;
                let observed_bytes = lease.observed_bytes();
                let built_splices = build_file_update_splices(
                    &observed_bytes,
                    lease.observed_bytes_sha256(),
                    write.data(),
                    write.splice_provenance(),
                    limits,
                )?;
                let submitted_bytes_sha256 = built_splices.after_sha256;
                let host_full_diff_bytes_compared = built_splices.full_diff_bytes_compared;
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
                            ids,
                        },
                    )
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
                .await
                {
                    Ok(transition) => transition,
                    Err(error) => return Err(lease.handle_guest_call_error(error)),
                };
                if let Err(error) = lease.actor_mut().drop_document(detection_input).await {
                    return Err(lease.handle_guest_call_error(error));
                }

                let detection_document = detected_transition.document;
                let mut counters = detected_transition.counters;
                let changes = match self
                    .suppress_v2_format_only_noops(detected_transition.changes, &file_key)
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
                let (successor_document, materialized_bytes, materialized_bytes_sha256) =
                    if observation_is_current {
                        // The actor lease serializes this file and the durable
                        // root still equals the acknowledged observation. The
                        // validated file successor is therefore already the
                        // exact merge result; rendering the same sparse change
                        // onto the same base would only repeat guest work.
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
                counters.host_full_content_classification_bytes = full_content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.private_document_cache_hits = 1;
                counters.durable_semantic_changes =
                    u64::try_from(changes.entity_change_count()).unwrap_or(u64::MAX);
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
                )
            } else {
                let store_permit = self.plugin_host.actor_cache().admit_store()?;
                let mut actor = factory.instantiate_actor().await?;
                let source = ArcByteSource::new(submitted_bytes.clone());
                let transition = actor
                    .open_file(
                        limits,
                        WasmOpenFileInput {
                            descriptor,
                            file: Arc::new(source),
                            ids,
                        },
                    )
                    .await?;
                let validated =
                    drain_file_transition_changes(actor.as_mut(), transition, &schemas, limits)
                        .await?;
                let changes = validated.changes;
                let mut counters = validated.counters;
                counters.host_full_content_classification_bytes = full_content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes =
                    u64::try_from(changes.entity_change_count()).unwrap_or(u64::MAX);
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
                )
            };
            let namespace_rows = self
                .v2_id_namespace_rows(
                    selected,
                    &changes,
                    bound_ids,
                    &file_key,
                    existing_id_namespace_reservation.as_ref(),
                )
                .await;
            let namespace_rows = match namespace_rows {
                Ok(rows) => rows,
                Err(error) => {
                    publication.discard().await;
                    discard_plugin_actor_publications(std::mem::take(
                        &mut reconciliation.actor_publications,
                    ))
                    .await;
                    return Err(error);
                }
            };
            let change_rows = plugin_detected_changes_from_v2(&changes).and_then(|detected| {
                plugin_change_rows(
                    selected,
                    detected,
                    &write.file_id,
                    &context,
                    "plugin v2 file transition",
                )
            });
            let change_rows = match change_rows {
                Ok(rows) => rows,
                Err(error) => {
                    publication.discard().await;
                    discard_plugin_actor_publications(std::mem::take(
                        &mut reconciliation.actor_publications,
                    ))
                    .await;
                    return Err(error);
                }
            };
            reconciliation.rows.extend(namespace_rows);
            reconciliation.rows.extend(change_rows);
            if materialized_bytes.as_ref() != write.data() {
                write.replace_data(materialized_bytes);
            }
            reconciliation
                .materialized_file_keys
                .insert(file_key.clone());
            reconciliation
                .materialization_versions
                .insert(file_key.clone(), materialization_version);
            reconciliation.actor_publications.push(publication);
            reconciled_file_keys.insert(file_key);
            continue;
        }

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
            let prepared = self.prepare_transaction_rows(group.rows.clone()).await?;
            if prepared.iter().any(|row| {
                row.branch_id != file_key.branch_id
                    || row.file_id.as_deref() != Some(file_key.file_id.as_str())
                    || row.global
                    || row.untracked
                    || group
                        .plugin
                        .schema_keys()
                        .binary_search(&row.schema_key)
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
            let changes = v2_host_changes_from_prepared_rows(prepared.clone(), limits)?;
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
                    let visible_root = self
                        .visible_v2_materialization_root(&file_key)
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
                    let cold_open = cache.prepare_cold_open(&actor_key, &visible_root).await?;
                    let observation = match cold_open {
                        PluginActorColdOpen::Ready(observation) => observation,
                        PluginActorColdOpen::Build(cold_install) => {
                            drop(cold_install);
                            self.cold_open_v2_semantic_actor(
                                &actor_key,
                                &group.plugin,
                                descriptor.clone(),
                                factory,
                            )
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
                        cache.lease_for_transition(&observation).await?,
                        actor_key,
                        view,
                    )
                }
            };
            let visible_root = match self.visible_v2_materialization_root(&file_key).await {
                Ok(Some(root)) => root,
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
                &visible_root,
                &materialization_version,
                limits,
            )
            .await;
            let (publication, rendered_bytes, counters) = match transition {
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
            let rendered_file = TransactionFileData::new(
                file_key.file_id.clone(),
                Some(group.path),
                Some(group.filename),
                file_key.branch_id.clone(),
                false,
                false,
                rendered_bytes,
            )
            .with_had_blob_ref(true);
            file_data.push(rendered_file);
            reconciliation
                .materialized_file_keys
                .insert(file_key.clone());
            reconciliation
                .materialization_versions
                .insert(file_key.clone(), materialization_version);
            for (source, prepared) in group.rows.iter().zip(prepared) {
                reconciliation
                    .prepared_semantic_rows
                    .insert(source, prepared)?;
            }
            reconciliation.actor_publications.push(publication);
            reconciled_file_keys.insert(file_key);
        }

        for (file_key, metadata) in deleted_file_keys {
            if reconciled_file_keys.contains(&file_key) {
                continue;
            }
            reconciliation
                .rows
                .extend(self.v2_id_reservation_tombstones(&file_key).await?);
            let Some(owner) = owners.get(&file_key) else {
                reconciliation.remove_session_file_view(SessionFileViewKey::new(
                    &file_key.branch_id,
                    &file_key.file_id,
                ));
                continue;
            };
            reconciliation.remove_session_file_view(SessionFileViewKey::new(
                &file_key.branch_id,
                &file_key.file_id,
            ));
            let active_state = state_by_file
                .get(&PluginStateFileKey {
                    branch_id: file_key.branch_id.clone(),
                    plugin_key: owner.plugin_key().to_string(),
                    file_id: file_key.file_id.clone(),
                })
                .cloned()
                .unwrap_or_default();
            let context = FilesystemRowContext {
                branch_id: file_key.branch_id.clone(),
                global: false,
                untracked: false,
                file_id: None,
                metadata,
            };
            reconciliation.rows.extend(plugin_state_tombstone_rows(
                &active_state,
                &file_key.file_id,
                &context,
            ));
            reconciliation.rows.push(PluginFileOwner::delete_row(
                file_key.file_id,
                &file_key.branch_id,
            )?);
        }

        Ok(reconciliation)
    }

    async fn prepare_transaction_write(
        &mut self,
        write: TransactionWrite,
        mut prepared_semantic_rows: PreparedSemanticRows,
    ) -> Result<PreparedTransactionWrite, LixError> {
        let prepared = match write {
            TransactionWrite::Rows { mode, rows } => PreparedTransactionWrite::Rows {
                mode,
                rows: self
                    .prepare_transaction_rows_with_frozen(rows, &mut prepared_semantic_rows)
                    .await?,
            },
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
            } => PreparedTransactionWrite::RowsWithFileData {
                mode,
                rows: self
                    .prepare_transaction_rows_with_frozen(rows, &mut prepared_semantic_rows)
                    .await?,
                file_data,
                count,
            },
        };
        prepared_semantic_rows.require_consumed()?;
        Ok(prepared)
    }

    async fn prepare_transaction_rows(
        &mut self,
        rows: Vec<TransactionWriteRow>,
    ) -> Result<Vec<PreparedStateRow>, LixError> {
        self.prepare_transaction_rows_with_frozen(rows, &mut PreparedSemanticRows::default())
            .await
    }

    async fn prepare_transaction_rows_with_frozen(
        &mut self,
        rows: Vec<TransactionWriteRow>,
        prepared_semantic_rows: &mut PreparedSemanticRows,
    ) -> Result<Vec<PreparedStateRow>, LixError> {
        let row_count = rows.len();
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let live_state = self.live_state.reader(&read);
        let mut prepared_rows = Vec::with_capacity(row_count);
        prepared_rows.resize_with(row_count, || None);
        let mut rows_by_scope = BTreeMap::<Domain, Vec<(usize, TransactionWriteRow)>>::new();
        for (index, row) in rows.into_iter().enumerate() {
            if let Some(prepared) = prepared_semantic_rows.take_for(&row)? {
                prepared_rows[index] = Some(prepared);
                continue;
            }
            rows_by_scope
                .entry(Domain::schema_catalog(
                    row.schema_scope_branch_id().to_string(),
                    row.untracked,
                ))
                .or_default()
                .push((index, row));
        }

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
                    .with_hint("Schema definitions are scoped by branch and durability only; write them with null file_id."));
                }
                remember_pending_registered_schema(
                    row.snapshot.as_ref().map(TransactionJson::value),
                    Domain::schema_catalog(row.schema_scope_branch_id().to_string(), row.untracked),
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
                prepared_rows[index] =
                    Some(prepare_state_row(row, &functions, self.origin_key.clone())?);
            }
        }
        Ok(prepared_rows
            .into_iter()
            .map(|row| {
                row.expect("every row should be prepared exactly once by schema scope grouping")
            })
            .collect())
    }

    async fn validate_prepared_writes_by_branch(
        &mut self,
        prepared_writes: &PreparedWriteSet,
    ) -> Result<(), LixError> {
        let validation_index = prepared_writes.validation_index();
        for scope in validation_index.schema_scopes() {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let branch_prepared_writes = validation_index.validation_set_for_schema_scope(scope);
            let read = SharedStorageAdapterRead::new(
                self.storage
                    .begin_read(StorageReadOptions::default())
                    .await?,
            );
            let live_state = self.live_state.reader(&read);
            let schema_catalog = self
                .schema_resolver
                .catalog_for_validation(&live_state, scope)
                .await?;
            validate_prepared_writes(TransactionValidationInput::new(
                &branch_prepared_writes,
                schema_catalog,
                &live_state,
            ))
            .await?;
        }
        Ok(())
    }

    /// Convenience helper for programmatic APIs that only stage state rows.
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

    async fn require_existing_transaction_write_branch_ids(
        &mut self,
        write: &TransactionWrite,
    ) -> Result<(), LixError> {
        let branch_ids = transaction_write_branch_ids(write);
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let reader = self.branch_ctx.ref_reader(read);
        for branch_id in branch_ids {
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
        std::mem::replace(&mut self.origin_key, origin_key)
    }

    pub(crate) async fn execute_read_sql_statement(
        &mut self,
        sql: &str,
        statement: DataFusionStatement,
        params: &[Value],
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
                &read_ctx, self, sql, statement, params,
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
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: vec![branch_ref_stage_row(branch_id, &commit_id)],
        })
        .await?;
        Ok(())
    }

    pub(crate) fn stage_merge_commit(
        &self,
        branch_id: String,
        source_parent_commit_id: CommitId,
        selected_changes: impl IntoIterator<Item = StagedCommitChangeRef>,
    ) -> Result<String, LixError> {
        let commit_id = self
            .staged_writes
            .stage_selected_commit_change_refs(branch_id.clone(), selected_changes)?;
        self.staged_writes
            .add_commit_parent(branch_id, source_parent_commit_id)?;
        Ok(commit_id)
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
        &self,
    ) -> TrackedStateStoreReader<SharedStorageAdapterRead<StorageImpl::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open transaction read scope");
        self.tracked_state
            .reader(SharedStorageAdapterRead::new(read))
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
    staged: crate::transaction::staging::PreparedStateRowOverlay,
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

    fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore> {
        HistoryQuerySource {
            store: self.read_store.clone(),
            json_reader: crate::json_store::JsonStoreContext::new().reader(self.read_store.clone()),
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
    staged: crate::transaction::staging::PreparedStateRowOverlay,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
}

#[async_trait]
impl<R> crate::live_state::LiveStateReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
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
                filter: LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_pks: vec![request.entity_pk.clone()],
                    branch_ids: vec![request.branch_id.clone()],
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

    async fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        overlay_load_exact_rows(&self.base, &self.staged, request).await
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
            overlay_scan_rows(&self.base, &self.staged, &request.live_state_request()).await?;
        #[cfg(test)]
        record_transaction_path_index_build(rows.len());
        let index = Arc::new(FilesystemPathIndex::from_live_rows(rows)?);
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

fn prepare_state_row(
    normalized: NormalizedTransactionWriteRow,
    functions: &FunctionProviderHandle,
    origin_key: Option<String>,
) -> Result<PreparedStateRow, LixError> {
    let NormalizedTransactionWriteRow {
        row,
        snapshot,
        schema_plan_id,
        facts,
    } = normalized;
    let updated_at = match row.updated_at {
        Some(updated_at) => parse_prepared_timestamp("updated_at", &updated_at)?,
        None => functions.call_timestamp(),
    };
    let created_at = match row.created_at {
        Some(created_at) => parse_prepared_timestamp("created_at", &created_at)?,
        None => updated_at,
    };
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
        entity_pk: row.entity_pk.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "normalized transaction write row is missing entity_pk",
            )
        })?,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot,
        metadata,
        origin: row.origin,
        origin_key,
        created_at,
        updated_at,
        global: row.global,
        change_id: Some(match row.change_id {
            Some(change_id) => ChangeId::parse_lix(&change_id, "prepared row change_id")?,
            None => ChangeId::from(functions.call_uuid_v7()),
        }),
        commit_id: row
            .commit_id
            .as_deref()
            .map(|id| CommitId::parse_lix(id, "prepared row commit_id"))
            .transpose()?,
        untracked: row.untracked,
        branch_id: row.branch_id,
    })
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
        .flat_map(|change_refs| change_refs.selected_change_refs.iter())
        .any(|change_ref| change_ref.schema_key == REGISTERED_SCHEMA_KEY)
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

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
    }

    fn session_file_views(&self) -> Option<SessionFileViews> {
        Some(self.session_file_views.clone())
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

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_visible_live_state(request).await
    }

    async fn load_exact_live_state_rows(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        self.load_visible_exact_live_state_rows(request).await
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
        let rows = overlay_scan_rows(&base, &staged, &request.live_state_request()).await?;
        #[cfg(test)]
        record_transaction_path_index_build(rows.len());
        let index = Arc::new(FilesystemPathIndex::from_live_rows(rows)?);
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

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Self::stage_write(self, write).await
    }
}

fn prepared_transaction_write_affects_filesystem_path_index(
    write: &PreparedTransactionWrite,
) -> bool {
    let rows = match write {
        PreparedTransactionWrite::Rows { rows, .. }
        | PreparedTransactionWrite::RowsWithFileData { rows, .. } => rows,
    };
    rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            "lix_file_descriptor" | "lix_directory_descriptor" | BRANCH_REF_SCHEMA_KEY
        )
    })
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
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

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
fn v2_id_namespace(seed: [u8; 16], actor_key: &PluginActorKey) -> crate::wasm::WasmIdNamespace {
    BoundIdNamespace::bind(local_mutation_identity(seed), actor_key).ids()
}

fn suppress_v2_format_only_noops_against_rows(
    changes: WasmHostEntityChanges,
    accepted: &BTreeMap<crate::wasm::WasmEntityKey, Option<MaterializedLiveStateRow>>,
) -> Result<WasmHostEntityChanges, LixError> {
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
                let Some(base_snapshot) = base.snapshot_content.as_deref() else {
                    effective.push(change);
                    continue;
                };
                let candidate = match &entity.snapshot_content {
                    WasmHostBytes::Inline(bytes) => bytes,
                    WasmHostBytes::Source(_) => {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "validated v2 guest changes must own canonical inline snapshots",
                        ));
                    }
                };
                let candidate =
                    serde_json::from_slice::<JsonValue>(candidate).map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("validated v2 snapshot is invalid JSON: {error}"),
                        )
                    })?;
                let base = serde_json::from_str::<JsonValue>(base_snapshot).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("accepted v2 snapshot is invalid JSON: {error}"),
                    )
                })?;
                candidate == base
            }
            WasmEntityChange::Upsert { .. } | WasmEntityChange::Delete(_) => false,
        };
        if !is_noop {
            effective.push(change);
        }
    }
    Ok(WasmHostEntityChanges { changes: effective })
}

fn plugin_detected_changes_from_v2(
    changes: &WasmHostEntityChanges,
) -> Result<Vec<PluginDetectedChange>, LixError> {
    let mut detected = Vec::with_capacity(changes.entity_change_count());
    for change in &changes.changes {
        let (key, snapshot_content, effect) = match change {
            WasmEntityChange::Delete(key) => (key, None, WasmChangeEffect::Content),
            WasmEntityChange::Upsert { entity, effect } => {
                let bytes = match &entity.snapshot_content {
                    WasmHostBytes::Inline(bytes) => bytes,
                    WasmHostBytes::Source(_) => {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "validated v2 guest changes must own canonical inline snapshots",
                        ));
                    }
                };
                let snapshot = String::from_utf8(bytes.clone()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "validated v2 snapshot is not UTF-8",
                    )
                })?;
                (&entity.key, Some(snapshot), *effect)
            }
        };
        detected.push(PluginDetectedChange {
            entity_pk: EntityPk::from_parts(key.entity_pk.clone()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("v2 plugin emitted invalid entity_pk: {error}"),
                )
            })?,
            schema_key: key.schema_key.clone(),
            snapshot_content,
            metadata: (effect == WasmChangeEffect::FormatOnly)
                .then(|| r#"{"impact":"format"}"#.to_string()),
        });
    }
    Ok(detected)
}

fn v2_host_entities_from_live_rows(
    rows: Vec<MaterializedLiveStateRow>,
    limits: WasmTransitionLimits,
) -> Result<Vec<WasmHostEntity>, LixError> {
    let mut entities = rows
        .into_iter()
        .filter_map(|row| {
            row.snapshot_content.map(|snapshot_content| {
                host_entity_with_lazy_snapshot(
                    crate::wasm::WasmEntityKey {
                        schema_key: row.schema_key,
                        entity_pk: row.entity_pk.into_parts(),
                    },
                    snapshot_content.into_bytes(),
                    limits,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
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

fn v2_host_changes_from_prepared_rows(
    rows: Vec<PreparedStateRow>,
    limits: WasmTransitionLimits,
) -> Result<WasmHostEntityChanges, LixError> {
    let mut changes = rows
        .into_iter()
        .map(|row| {
            if row.global || row.untracked || row.file_id.is_none() {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "v2 semantic rendering requires tracked, branch-local, file-scoped rows",
                ));
            }
            let key = crate::wasm::WasmEntityKey {
                schema_key: row.schema_key,
                entity_pk: row.entity_pk.into_parts(),
            };
            match row.snapshot {
                Some(snapshot) => {
                    let format_only = row
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.value.get("impact"))
                        .and_then(JsonValue::as_str)
                        .is_some_and(|impact| impact == "format");
                    let effect = if format_only {
                        WasmChangeEffect::FormatOnly
                    } else {
                        WasmChangeEffect::Content
                    };
                    host_entity_change_with_lazy_snapshot(
                        key,
                        snapshot.materialize().into_bytes(),
                        effect,
                        limits,
                    )
                }
                None => Ok(WasmEntityChange::Delete(key)),
            }
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    changes.sort_by(|left, right| left.key().cmp(right.key()));
    for pair in changes.windows(2) {
        if pair[0].key() == pair[1].key() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "one v2 semantic write batch cannot contain the same entity key more than once",
            ));
        }
    }
    Ok(WasmHostEntityChanges { changes })
}

fn reject_external_plugin_registry_rows(rows: &[TransactionWriteRow]) -> Result<(), LixError> {
    for row in rows {
        if row.schema_key != KEY_VALUE_SCHEMA_KEY {
            continue;
        }
        let entity_key = row
            .entity_pk
            .as_ref()
            .and_then(|entity_pk| entity_pk.as_single_string().ok());
        let snapshot_key = row
            .snapshot
            .as_ref()
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
    fn matches_blob_ref_row(&self, row: &TransactionWriteRow) -> bool {
        row.schema_key == BLOB_REF_SCHEMA_KEY
            && row.branch_id == self.branch_id
            && row.global == self.global
            && row.untracked == self.untracked
            && row.file_id.as_deref() == Some(self.file_id.as_str())
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

#[derive(Default)]
struct PluginWriteReconciliation {
    file_keys: BTreeSet<PluginFileWriteKey>,
    materialized_file_keys: BTreeSet<PluginFileWriteKey>,
    materialization_versions: BTreeMap<PluginFileWriteKey, String>,
    rows: Vec<TransactionWriteRow>,
    file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    actor_publications: Vec<PendingPluginActorPublication>,
    prepared_semantic_rows: PreparedSemanticRows,
}

impl PluginWriteReconciliation {
    fn remove_session_file_view(&mut self, key: SessionFileViewKey) {
        self.file_view_mutations
            .insert(key.clone(), SessionFileViewMutation::Remove { key });
    }
}

/// Exact normalized rows already supplied to a v2 renderer.
///
/// The raw source row is the lookup key because reconciliation may append
/// engine-managed rows before final staging. Keeping the prepared row here
/// prevents volatile schema defaults, derived primary keys, timestamps, and
/// change IDs from being evaluated a second time after the guest rendered
/// them.
#[derive(Debug, Default)]
struct PreparedSemanticRows {
    by_source: BTreeMap<Vec<u8>, VecDeque<PreparedStateRow>>,
    remaining: usize,
}

impl PreparedSemanticRows {
    fn insert(
        &mut self,
        source: &TransactionWriteRow,
        prepared: PreparedStateRow,
    ) -> Result<(), LixError> {
        self.by_source
            .entry(transaction_write_row_fingerprint(source)?)
            .or_default()
            .push_back(prepared);
        self.remaining = self.remaining.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "prepared v2 semantic row count overflowed",
            )
        })?;
        Ok(())
    }

    fn take_for(
        &mut self,
        source: &TransactionWriteRow,
    ) -> Result<Option<PreparedStateRow>, LixError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let fingerprint = transaction_write_row_fingerprint(source)?;
        let Some(queue) = self.by_source.get_mut(&fingerprint) else {
            return Ok(None);
        };
        let prepared = queue.pop_front();
        if prepared.is_some() {
            self.remaining -= 1;
        }
        if queue.is_empty() {
            self.by_source.remove(&fingerprint);
        }
        Ok(prepared)
    }

    fn require_consumed(&self) -> Result<(), LixError> {
        if self.remaining == 0 {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "{} prepared v2 semantic row(s) disappeared during reconciliation",
                self.remaining
            ),
        ))
    }
}

fn transaction_write_row_fingerprint(row: &TransactionWriteRow) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(row).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to fingerprint a prepared v2 semantic row: {error}"),
        )
    })
}

struct PendingPluginActorView {
    session_key: SessionFileViewKey,
    plugin_key: String,
    plugin_generation: String,
    owner_change_id: String,
    semantic_chainable: bool,
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
}

impl PendingPluginActorPublication {
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
        }
    }

    async fn publish(self) -> Result<(SessionFileViewKey, SessionPluginFileView), LixError> {
        let (observation, view) = match self {
            Self::Existing {
                lease,
                successor_key,
                view,
            } => (lease.commit_successor_as(successor_key).await?, view),
            Self::New {
                cache,
                key,
                store,
                document,
                bytes,
                semantic_root,
                view,
            } => (
                cache.install(key, store, document, bytes, semantic_root),
                view,
            ),
        };
        Ok((
            view.session_key,
            SessionPluginFileView {
                plugin_key: view.plugin_key,
                plugin_generation: view.plugin_generation,
                owner_change_id: view.owner_change_id,
                observation: Some(observation),
            },
        ))
    }

    fn session_key(&self) -> &SessionFileViewKey {
        match self {
            Self::Existing { view, .. } | Self::New { view, .. } => &view.session_key,
        }
    }
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
    Ok((publication(lease), rendered_bytes, counters))
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

/// Proves that replacing a component generation cannot reinterpret any
/// currently owned file. The replacement factory is deliberately used only
/// as a disposable verifier: accepted actors are cold-opened later under the
/// new generation key, after the registry row commits.
async fn preflight_owned_v2_generation_upgrades(
    host: &PluginRuntimeHost,
    base: &dyn crate::live_state::LiveStateReader,
    staged: &impl crate::live_state::StagedLiveStateRows,
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
    let owner_rows = overlay_scan_rows(
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
    for row in owner_rows {
        let branch_id = row.branch_id.clone();
        let Some(owner) = PluginFileOwner::from_live_state_row(&row, &branch_id)? else {
            continue;
        };
        let Some(index) = upgrade_indexes
            .get(&(branch_id, owner.plugin_key().to_string()))
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

    let descriptor_rows = overlay_scan_rows(
        base,
        staged,
        &FilesystemPathIndexRequest::new(branch_ids).live_state_request(),
    )
    .await?;
    let path_index = FilesystemPathIndex::from_live_rows(descriptor_rows)?;

    let owned_schema_keys = upgrades
        .iter()
        .zip(&owners_by_upgrade)
        .filter(|(_, owners)| !owners.is_empty())
        .flat_map(|(upgrade, _)| upgrade.previous.schema_keys().iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(EntityPk::single)
        .collect::<Vec<_>>();
    let registered_schema_rows = overlay_scan_rows(
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
    for row in registered_schema_rows {
        let schema_key = row.entity_pk.as_single_string().map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema has an invalid identity: {error}"),
            )
        })?;
        let Some(snapshot) = row.snapshot_content.as_deref() else {
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
            .insert((row.branch_id, schema_key.to_string()), definition)
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
        let state_rows = overlay_scan_rows(
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
        let mut state_by_file = file_ids
            .iter()
            .cloned()
            .map(|file_id| (file_id, Vec::<MaterializedLiveStateRow>::new()))
            .collect::<BTreeMap<_, _>>();
        for row in state_rows {
            let Some(file_id) = row.file_id.clone() else {
                continue;
            };
            if row.branch_id == upgrade.branch_id
                && !row.global
                && !row.untracked
                && row.snapshot_content.is_some()
                && upgrade
                    .previous
                    .schema_keys()
                    .binary_search(&row.schema_key)
                    .is_ok()
                && let Some(rows) = state_by_file.get_mut(&file_id)
            {
                rows.push(row);
            }
        }

        let blob_rows = overlay_scan_rows(
            base,
            staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    entity_pks: file_ids.iter().cloned().map(EntityPk::single).collect(),
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
        for row in blob_rows {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            if row.branch_id != upgrade.branch_id || row.global || row.untracked {
                continue;
            }
            let Some(snapshot) = row.snapshot_content.as_deref() else {
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
                        && row.branch_id == upgrade.branch_id
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
            let entities = v2_host_entities_from_live_rows(
                state_by_file.remove(owner.file_id()).unwrap_or_default(),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginStateFileKey {
    branch_id: String,
    plugin_key: String,
    file_id: String,
}

#[derive(Debug)]
struct PluginV2SemanticWriteGroup {
    plugin: PluginRegistryEntry,
    path: String,
    filename: String,
    owner_change_id: String,
    rows: Vec<TransactionWriteRow>,
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

fn mark_plugin_reconciliation_rows(rows: &mut [TransactionWriteRow]) {
    for row in rows {
        row.origin = Some(TransactionWriteOrigin {
            surface: "plugin_reconciliation".to_string(),
            operation: TransactionWriteOperation::Update,
            primary_key: None,
        });
    }
}

fn plugin_registry_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string()],
    }
}

fn plugin_change_rows(
    plugin: &PluginRegistryEntry,
    changes: Vec<PluginDetectedChange>,
    file_id: &str,
    context: &FilesystemRowContext,
    json_context: &str,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let schema_keys = plugin.schema_keys().iter().collect::<BTreeSet<_>>();
    changes
        .into_iter()
        .map(|change| {
            if !schema_keys.contains(&change.schema_key) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin '{}' emitted schema key '{}' that is not declared in its manifest",
                        plugin.key(),
                        change.schema_key
                    ),
                ));
            }
            Ok(TransactionWriteRow {
                entity_pk: Some(change.entity_pk),
                schema_key: change.schema_key,
                file_id: Some(file_id.to_string()),
                snapshot: change
                    .snapshot_content
                    .map(|raw| plugin_transaction_json(&raw, json_context))
                    .transpose()?,
                metadata: change
                    .metadata
                    .map(|raw| plugin_transaction_json(&raw, json_context))
                    .transpose()?,
                origin: None,
                created_at: None,
                updated_at: None,
                global: context.global,
                change_id: None,
                commit_id: None,
                untracked: context.untracked,
                branch_id: context.branch_id.clone(),
            })
        })
        .collect()
}

fn plugin_state_tombstone_rows(
    active_state: &[MaterializedLiveStateRow],
    file_id: &str,
    context: &FilesystemRowContext,
) -> Vec<TransactionWriteRow> {
    active_state
        .iter()
        .map(|row| TransactionWriteRow {
            entity_pk: Some(row.entity_pk.clone()),
            schema_key: row.schema_key.clone(),
            file_id: Some(file_id.to_string()),
            snapshot: None,
            metadata: context.metadata.clone(),
            origin: None,
            created_at: None,
            updated_at: None,
            global: context.global,
            change_id: None,
            commit_id: None,
            untracked: context.untracked,
            branch_id: context.branch_id.clone(),
        })
        .collect()
}

fn plugin_transaction_json(raw: &str, context: &str) -> Result<TransactionJson, LixError> {
    let value = serde_json::from_str::<JsonValue>(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{context} emitted invalid JSON: {error}"),
        )
    })?;
    TransactionJson::from_value(value, context)
}

fn transaction_write_has_plugin_lifecycle_candidate(write: &TransactionWrite) -> bool {
    let (rows, file_data): (&[TransactionWriteRow], &[TransactionFileData]) = match write {
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
                    .as_ref()
                    .and_then(|entity_pk| entity_pk.as_single_string().ok())
                    .and_then(plugin_key_from_archive_file_id)
                    .is_some()
        })
}

fn transaction_write_branch_ids(write: &TransactionWrite) -> BTreeSet<String> {
    match write {
        TransactionWrite::Rows { rows, .. } => transaction_write_row_branch_ids(rows),
        TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } => transaction_write_row_branch_ids(rows)
            .into_iter()
            .chain(stage_file_data_branch_ids(file_data))
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

fn transaction_write_row_branch_ids(rows: &[TransactionWriteRow]) -> BTreeSet<String> {
    rows.iter().map(|row| row.branch_id.clone()).collect()
}

fn stage_file_data_branch_ids(file_data: &[TransactionFileData]) -> BTreeSet<String> {
    file_data
        .iter()
        .map(|write| write.branch_id.clone())
        .collect()
}

async fn resolve_active_branch_id(
    mode: &SessionMode,
    live_state: &LiveStateContext,
    branch_ctx: &BranchContext,
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { branch_id } => Ok(branch_id.clone()),
        SessionMode::Workspace => load_workspace_branch_id(live_state, branch_ctx, read).await,
    }
}

async fn load_workspace_branch_id(
    live_state: &LiveStateContext,
    branch_ctx: &BranchContext,
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<String, LixError> {
    let row = live_state
        .reader(read)
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(WORKSPACE_BRANCH_KEY),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace branch selector is missing lix_key_value:lix_workspace_branch_id",
            )
        })?;
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace branch selector is missing snapshot_content",
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace branch selector snapshot is invalid JSON: {error}"),
        )
    })?;
    let branch_id = snapshot
        .get("value")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace branch selector value must be a non-empty string",
            )
        })?
        .to_string();

    let head = branch_ctx
        .ref_reader(read)
        .load_head_commit_id(&branch_id)
        .await?;
    if head.is_none() {
        return Err(LixError::branch_not_found(
            branch_id,
            "load_workspace_branch_id",
            "workspace_selector",
        ));
    }

    Ok(branch_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use serde_json::json;

    use super::*;
    use crate::Engine;
    use crate::GLOBAL_BRANCH_ID;
    use crate::NullableKeyFilter;
    use crate::branch::BranchContext;
    use crate::changelog::ChangelogReader;
    use crate::storage_adapter::{Memory, StorageReadOptions};
    use crate::tracked_state::{TrackedStateKey, TrackedStateScanRequest};
    use crate::transaction::types::TransactionJson;
    use crate::wasm::WasmEntity;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            crate::live_state::LiveStateIndexContext::new(),
            CommitGraphContext::new(),
        )
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    #[test]
    fn format_only_equal_snapshots_are_semantic_noops() {
        let key = |id: &str| crate::wasm::WasmEntityKey {
            schema_key: "plugin_note".to_string(),
            entity_pk: vec![id.to_string()],
        };
        let live = |id: &str, snapshot_content: &str| MaterializedLiveStateRow {
            entity_pk: EntityPk::single(id),
            schema_key: "plugin_note".to_string(),
            file_id: Some("file-a".to_string()),
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            created_at: String::new(),
            updated_at: String::new(),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "branch-a".to_string(),
        };
        let upsert = |id: &str, snapshot: &[u8], effect| WasmEntityChange::Upsert {
            entity: WasmEntity {
                key: key(id),
                snapshot_content: WasmHostBytes::Inline(snapshot.to_vec()),
            },
            effect,
        };
        let changes = WasmHostEntityChanges {
            changes: vec![
                upsert(
                    "equal",
                    br#"{"id":"equal","text":"\u00e9"}"#,
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
        let accepted = BTreeMap::from([
            (
                key("equal"),
                Some(live("equal", r#"{"text":"é","id":"equal"}"#)),
            ),
            (
                key("changed"),
                Some(live("changed", r#"{"id":"changed","text":"old"}"#)),
            ),
            (
                key("content"),
                Some(live("content", r#"{"id":"content","text":"same"}"#)),
            ),
        ]);

        let effective = suppress_v2_format_only_noops_against_rows(changes, &accepted)
            .expect("number-free normalized snapshots should compare");
        assert_eq!(effective.changes.len(), 3);
        assert_eq!(effective.changes[0].key(), &key("changed"));
        assert_eq!(effective.changes[1].key(), &key("content"));
        assert_eq!(effective.changes[2].key(), &key("deleted"));
    }

    #[test]
    fn plugin_owner_is_only_rewritten_when_its_durable_contract_changes() {
        let current = PluginFileOwner::new("file-a", "plugin-a", vec!["schema-a".to_string()])
            .expect("current owner should be valid");
        assert!(!plugin_owner_needs_write(Some(&current), &current));
        assert!(plugin_owner_needs_write(None, &current));

        for desired in [
            PluginFileOwner::new("file-a", "plugin-b", vec!["schema-a".to_string()])
                .expect("changed plugin owner should be valid"),
            PluginFileOwner::new("file-a", "plugin-a", vec!["schema-b".to_string()])
                .expect("changed schema owner should be valid"),
        ] {
            assert!(plugin_owner_needs_write(Some(&current), &desired));
        }
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
                    insert: crate::wasm::WasmGuestBytes::Inline(bytes.clone()),
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
            api_version: "2.0.0".to_string(),
            path_glob: "*.csv".to_string(),
            content_type: Some(PluginContentType::Text),
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["csv_row".to_string()],
            host_allocated_schema_keys: vec!["csv_row".to_string()],
            manifest_json: r#"{"api_version":"2.0.0","entry":"plugin.wasm","key":"plugin_csv_v2","match":{"content_type":"text","path_glob":"*.csv"},"runtime":"wasm-component-v2","schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: "lix_plugin_archive::plugin_csv_v2".to_string(),
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
        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
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
                    if change.entity_pk.as_single_string_owned().as_deref()
                        == Ok("tracked-programmatic")
            ),
            "tracked staged row should be appended to changelog"
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
            .load_rows_at_commit(
                &head_commit_id.to_string(),
                &[TrackedStateKey {
                    schema_key: "lix_key_value".to_string(),
                    entity_pk: EntityPk::single("tracked-programmatic"),
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
        assert_eq!(live_untracked_row.branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(
            live_untracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"untracked-programmatic","value":"untracked"}"#)
        );
        let untracked_change_id = live_untracked_row
            .change_id
            .expect("untracked current row should have a real change id");
        let untracked_changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[untracked_change_id],
            })
            .await
            .expect("changelog should load untracked change");
        assert!(matches!(
            untracked_changes.entries.as_slice(),
            [Some(change)]
                if change.entity_pk.as_single_string_owned().as_deref()
                    == Ok("untracked-programmatic")
        ));

        let tracked_rows = TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                &head_commit_id.to_string(),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("tracked state should scan");
        assert!(
            tracked_rows
                .iter()
                .all(|row| row.entity_pk.as_single_string_owned().as_deref()
                    != Ok("untracked-programmatic")),
            "untracked staged rows should not be written into tracked state"
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
            .stage_rows(vec![row])
            .await
            .expect("valid ISO timestamps should stage after lossy normalization");
        assert_eq!(outcome.count, 1);

        let rows = transaction
            .scan_live_state(&LiveStateScanRequest {
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
            rows[0].change_id.is_some(),
            "prepared untracked rows must receive a real change id"
        );
        assert_eq!(rows[0].created_at, "1970-01-01T00:00:00.000Z");
        assert_eq!(rows[0].updated_at, "2026-04-23T00:00:00.123Z");
    }

    #[tokio::test]
    async fn prepared_semantic_rows_freeze_nondeterministic_defaults_for_staging() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let source = TransactionWriteRow {
            entity_pk: None,
            schema_key: "lix_account".to_string(),
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
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        };
        let rendered = transaction
            .prepare_transaction_rows(vec![source.clone()])
            .await
            .expect("the semantic row should normalize once")
            .pop()
            .expect("one semantic row should be prepared");
        let independently_reprepared = transaction
            .prepare_transaction_rows(vec![source.clone()])
            .await
            .expect("a second normalization should also succeed")
            .pop()
            .expect("one comparison row should be prepared");
        assert_ne!(
            rendered.entity_pk, independently_reprepared.entity_pk,
            "the fixture must prove its UUID default is nondeterministic"
        );

        let mut frozen = PreparedSemanticRows::default();
        frozen
            .insert(&source, rendered.clone())
            .expect("the exact rendered row should freeze");
        let PreparedTransactionWrite::Rows { rows, .. } = transaction
            .prepare_transaction_write(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: vec![source],
                },
                frozen,
            )
            .await
            .expect("final preparation should reuse the frozen row")
        else {
            panic!("row-only input should stay row-only");
        };
        assert_eq!(
            rows,
            vec![rendered],
            "durable staging must receive the exact row supplied to entities_changed"
        );
    }

    #[tokio::test]
    async fn direct_semantic_rendering_pages_oversized_snapshot_as_lazy_attachment() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let limits = WasmTransitionLimits::default();
        let large_value = "x".repeat(limits.max_record_bytes as usize + 32);
        let mut prepared = transaction
            .prepare_transaction_rows(vec![key_value_stage_row(
                "large-semantic-entity",
                &large_value,
                true,
            )])
            .await
            .expect("large semantic row should normalize")
            .pop()
            .expect("one prepared semantic row should exist");
        let expected = prepared
            .snapshot
            .as_ref()
            .expect("semantic upsert should carry a snapshot")
            .materialize();
        prepared.file_id = Some("large-file".to_string());
        prepared.global = false;
        prepared.untracked = false;
        prepared.branch_id = "main".to_string();

        let changes = v2_host_changes_from_prepared_rows(vec![prepared], limits)
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

    #[tokio::test]
    async fn commit_validates_staged_rows_before_persistence() {
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
    async fn commit_rejects_non_object_metadata_without_sql() {
        let storage = Memory::new();
        let (live_state, _binary_cas, branch_ref, runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let storage = StorageAdapter::new(storage);

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
    async fn stage_rows_rejects_missing_branch_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("ghost-branch-row", "value", false);
        row.branch_id = "ghost-branch".to_string();
        row.global = false;

        let error = transaction
            .stage_rows(vec![row])
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
        row.branch_id = GLOBAL_BRANCH_ID.to_string();
        row.global = false;

        let error = transaction
            .stage_rows(vec![row])
            .await
            .expect_err("invalid storage scope should be rejected before staging");

        assert_eq!(error.code, LixError::CODE_INVALID_STORAGE_SCOPE);
        assert!(
            error.message.contains("branch_id='global', global=false"),
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
        let storage = Memory::new();
        let (live_state, _binary_cas, branch_ref, runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let storage = StorageAdapter::new(storage);

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
        row.entity_pk = None;

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
    async fn stage_rows_rejects_primary_key_entity_pk_mismatch_without_sql() {
        let storage = Memory::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("right-id", "value", false);
        row.entity_pk = Some(EntityPk::single("wrong-id"));

        let error = transaction
            .stage_rows(vec![row])
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
                    snapshot_content: Some(snapshot_content),
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
    fn v2_id_namespaces_are_retry_stable_and_file_incarnation_scoped() {
        let seed = [7; 16];
        let key = PluginActorKey {
            branch_id: "main".to_string(),
            file_id: "file-a".to_string(),
            path: "/data.csv".to_string(),
            owner_change_id: "incarnation-a".to_string(),
            plugin_key: "plugin_csv_v2".to_string(),
            plugin_generation: "generation-a".to_string(),
        };
        assert_eq!(v2_id_namespace(seed, &key), v2_id_namespace(seed, &key));

        let mut other_file = key.clone();
        other_file.file_id = "file-b".to_string();
        assert_ne!(
            v2_id_namespace(seed, &key),
            v2_id_namespace(seed, &other_file)
        );

        let mut other_incarnation = key.clone();
        other_incarnation.owner_change_id = "incarnation-b".to_string();
        assert_ne!(
            v2_id_namespace(seed, &key),
            v2_id_namespace(seed, &other_incarnation)
        );
    }

    fn key_value_stage_row(key: &str, value: &str, untracked: bool) -> TransactionWriteRow {
        TransactionWriteRow {
            entity_pk: Some(EntityPk::single(key)),
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
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        }
    }
}
