#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet};
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
use crate::catalog::CatalogContext;
use crate::changelog::{ChangeId, CommitId};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::common::LixTimestamp;
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemRowContext, blob_ref_tombstone_row,
    load_path_index_revision,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::{
    LiveStateContext, LiveStateFileScanRequest, LiveStateFilter, LiveStateProjection,
    LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::live_state::{overlay_scan_file_rows, overlay_scan_rows};
use crate::plugin::{
    CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginArchiveInstallPlan,
    PluginDetectedChange, PluginFileOwner, PluginRegistry, PluginRegistryEntry,
    PluginRegistryEntryInput, PluginRuntimeHost, detect_changes_with_component_instance,
    is_plugin_storage_path, plugin_install_plan_from_archive_path, plugin_key_from_archive_file_id,
    plugin_state_live_state_projection, retain_plugin_state_rows_for_schema_keys,
};
use crate::session::{SessionMode, WORKSPACE_BRANCH_KEY};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
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
use crate::wasm::{WasmComponentInstance, WasmPluginFile};
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
    catalog_context: Arc<CatalogContext>,
    schema_resolver: TransactionSchemaResolver,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    storage: StorageAdapter<StorageImpl>,
    sql_schema_cache: SqlSchemaCache,
    functions: FunctionProviderHandle,
    commit_boundary: Option<TransactionCommitBoundary>,
    origin_key: Option<String>,
}

#[derive(Default)]
struct SqlSchemaCache {
    visible_schemas: Option<Vec<JsonValue>>,
}

impl SqlSchemaCache {
    fn is_prepared(&self) -> bool {
        self.visible_schemas.is_some()
    }

    fn prepare(&mut self, visible_schemas: Vec<JsonValue>) {
        self.visible_schemas = Some(visible_schemas);
    }

    fn visible_schemas(&self) -> Result<&[JsonValue], LixError> {
        self.visible_schemas.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "SQL visible schemas were requested before SQL transaction context preparation",
            )
        })
    }
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
                let visible_live_state = live_state.reader(&read);
                catalog_context
                    .compiled_catalog_for_domain(
                        &visible_live_state,
                        &Domain::schema_catalog(active_branch_id.clone(), true),
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
            schema_catalog,
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
                catalog_context,
                schema_resolver,
                staged_writes,
                filesystem_path_index_cache: Arc::new(FilesystemPathIndexCache::default()),
                filesystem_path_index_epoch: Arc::new(AtomicUsize::new(0)),
                storage,
                sql_schema_cache: SqlSchemaCache::default(),
                functions,
                commit_boundary: None,
                origin_key: None,
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
                return Err(error);
            }
        };
        let _commit_guard = begin_commit_boundary(commit_boundary.as_ref());
        check_commit_boundary(commit_boundary.as_ref())?;
        transaction
            .validate_prepared_writes_by_branch(&prepared_writes)
            .await?;
        let mut read = SharedStorageAdapterRead::new(
            transaction
                .storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let writes = match commit::commit_prepared_writes(
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
            Err(error) => return Err(error),
        };
        let prepared_commit = transaction
            .storage
            .prepare_write_set(writes, StorageWriteOptions::default())
            .await?;
        let storage_stats = commit_at_boundary(commit_boundary.as_ref(), || async move {
            let (_commit, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
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
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        Ok(())
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
        let write = self.reconcile_plugin_write(write).await?;
        require_valid_transaction_write_storage_scopes(&write)?;
        let write = self.prepare_transaction_write(write).await?;
        if prepared_transaction_write_affects_filesystem_path_index(&write) {
            // TransactionWriteBuffer may retain an earlier row from this batch even
            // when a later row makes staging fail, so invalidate before staging.
            self.filesystem_path_index_epoch
                .fetch_add(1, Ordering::SeqCst);
        }
        self.staged_writes.stage_write(write)
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

    async fn reconcile_plugin_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWrite, LixError> {
        match write {
            TransactionWrite::Rows { mode, mut rows } => {
                reject_external_plugin_registry_rows(&rows)?;
                let mut reconciliation = self
                    .plugin_write_reconciliation(&rows, &mut Vec::new())
                    .await?;
                mark_plugin_reconciliation_rows(&mut reconciliation.rows);
                rows.extend(reconciliation.rows);
                Ok(TransactionWrite::Rows { mode, rows })
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
                    rows: mut plugin_rows,
                } = self
                    .plugin_write_reconciliation(&rows, &mut file_data)
                    .await?;
                mark_plugin_reconciliation_rows(&mut plugin_rows);
                rows.retain(|row| !file_keys.iter().any(|key| key.matches_blob_ref_row(row)));
                rows.extend(plugin_rows);
                let file_data = file_data
                    .into_iter()
                    .filter(|write| {
                        !file_keys.contains(&PluginFileWriteKey::from(write)) && !write.is_empty()
                    })
                    .collect();
                Ok(TransactionWrite::RowsWithFileData {
                    mode,
                    rows,
                    file_data,
                    count,
                })
            }
        }
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
        file_data: &mut [TransactionFileData],
    ) -> Result<PluginWriteReconciliation, LixError> {
        let mut reconciliation = PluginWriteReconciliation::default();
        let mut lifecycle = BTreeMap::<PluginLifecycleKey, Option<PluginRegistryEntry>>::new();
        let mut lifecycle_schema_rows = Vec::<(PluginLifecycleKey, TransactionWriteRow)>::new();
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
                entry: parsed.manifest.entry.clone(),
                schema_keys: parsed.schema_keys.clone(),
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

        if branch_ids.is_empty() {
            return Ok(reconciliation);
        }

        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
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
                    registry.upsert(plugin)?;
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
        for row in owner_rows {
            let branch_id = row.branch_id.clone();
            let Some(owner) = PluginFileOwner::from_live_state_row(&row, &branch_id)? else {
                continue;
            };
            let key = PluginFileWriteKey {
                branch_id,
                global: false,
                untracked: false,
                file_id: owner.file_id().to_string(),
            };
            if owners.insert(key, owner).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "durable plugin owner lookup returned duplicate file rows",
                ));
            }
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

        let mut selected_plugins = BTreeMap::<PluginFileWriteKey, PluginRegistryEntry>::new();
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
            let Some(plugin) = catalogs
                .get(&write.branch_id)
                .and_then(|catalog| catalog.select(path))
            else {
                continue;
            };
            selected_plugins.insert(PluginFileWriteKey::from(write), plugin.clone());
        }

        let mut state_groups = BTreeMap::<PluginStateGroupKey, PluginStateGroup>::new();
        for (key, owner) in &owners {
            let group_key = PluginStateGroupKey {
                branch_id: key.branch_id.clone(),
                plugin_key: owner.plugin_key().to_string(),
            };
            let group = state_groups.entry(group_key).or_default();
            group.file_ids.insert(key.file_id.clone());
            group
                .schema_keys
                .extend(owner.schema_keys().iter().cloned());
            if let Some(selected) = selected_plugins.get(key)
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

        // Resolve warm components by their fixed content hash before asking
        // the CAS for bytes. Keep the returned Arc itself: another branch may
        // concurrently replace the key-only cache with a different hash.
        let mut component_instances =
            BTreeMap::<PluginBranchEntryKey, Arc<dyn WasmComponentInstance>>::new();
        let mut cold_entries = BTreeMap::<PluginBranchEntryKey, PluginRegistryEntry>::new();
        for (key, entry) in selected_entries {
            let hash = BlobHash::from_hex(entry.wasm_blob_hash())?;
            if let Some(instance) = self
                .plugin_host
                .cached_plugin_component(entry.key(), hash)?
            {
                component_instances.insert(key, instance);
            } else {
                cold_entries.insert(key, entry);
            }
        }

        let mut wasm_by_hash = current_install_wasm;
        let mut missing_hashes = Vec::<BlobHash>::new();
        for entry in cold_entries.values() {
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
        for (key, entry) in cold_entries {
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
            let instance =
                crate::plugin::load_or_init_plugin_component(&self.plugin_host, &plugin).await?;
            component_instances.insert(key, instance);
        }

        let mut reconciled_file_keys = BTreeSet::<PluginFileWriteKey>::new();
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
            }

            let Some(selected) = selected else {
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
            let component = component_instances
                .get(&installed_key)
                .expect("selected plugin should have a resolved component");
            let active_state = if owner.is_some_and(|owner| owner.plugin_key() == selected.key()) {
                let selected_schema_keys = selected.schema_keys().iter().collect::<BTreeSet<_>>();
                let obsolete_state = old_state
                    .iter()
                    .filter(|row| !selected_schema_keys.contains(&row.schema_key))
                    .cloned()
                    .collect::<Vec<_>>();
                reconciliation.rows.extend(plugin_state_tombstone_rows(
                    &obsolete_state,
                    &write.file_id,
                    &context,
                ));
                retain_plugin_state_rows_for_schema_keys(selected.schema_keys(), old_state)
            } else {
                Vec::new()
            };
            let changes = detect_changes_with_component_instance(
                component,
                &active_state,
                WasmPluginFile {
                    filename: write.filename.clone(),
                    data: write.data().to_vec(),
                },
            )
            .await?;
            if write.had_blob_ref {
                reconciliation.rows.push(blob_ref_tombstone_row(
                    write.file_id.clone(),
                    context.clone(),
                ));
            }
            reconciliation.rows.extend(plugin_change_rows(
                selected,
                changes,
                &write.file_id,
                &context,
                "plugin detect-changes",
            )?);
            reconciliation.rows.push(
                PluginFileOwner::from_registry_entry(write.file_id.clone(), selected)?
                    .write_row(&write.branch_id)?,
            );
            reconciliation.file_keys.insert(file_key.clone());
            reconciled_file_keys.insert(file_key);
        }

        for (file_key, metadata) in deleted_file_keys {
            if reconciled_file_keys.contains(&file_key) {
                continue;
            }
            let Some(owner) = owners.get(&file_key) else {
                continue;
            };
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
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let live_state = self.live_state.reader(&read);
        let mut rows_by_scope = BTreeMap::<Domain, Vec<(usize, TransactionWriteRow)>>::new();
        for (index, row) in rows.into_iter().enumerate() {
            rows_by_scope
                .entry(Domain::schema_catalog(
                    row.schema_scope_branch_id().to_string(),
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
        self.prepare_sql_visible_schemas().await?;
        let storage = self.storage.clone();
        let read = storage.begin_read(StorageReadOptions::default()).await?;
        let active_branch_id = self.active_branch_id.clone();
        let live_state = Arc::clone(&self.live_state);
        let binary_cas = Arc::clone(&self.binary_cas);
        let branch_ctx = Arc::clone(&self.branch_ctx);
        let visible_schemas = self.cached_visible_schemas()?.to_vec();
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

    pub(crate) async fn prepare_sql_visible_schemas(&mut self) -> Result<(), LixError> {
        if self.sql_schema_cache.is_prepared() {
            return Ok(());
        }
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let live_state = self.live_state.reader(&read);
        let visible_schemas = self
            .catalog_context
            .schema_jsons_for_sql_read_planning(&live_state, &self.active_branch_id)
            .await?;
        self.sql_schema_cache.prepare(visible_schemas);
        Ok(())
    }

    fn cached_visible_schemas(&self) -> Result<&[JsonValue], LixError> {
        self.sql_schema_cache.visible_schemas()
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

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
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

    async fn scan_file_rows(
        &self,
        request: &LiveStateFileScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        overlay_scan_file_rows(&self.base, &self.staged, request).await
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
        Ok(self.cached_visible_schemas()?.to_vec())
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
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
            .find(|key| matches!(*key, PLUGIN_REGISTRY_KEY | PLUGIN_OWNER_KEY));
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

#[derive(Debug, Default)]
struct PluginWriteReconciliation {
    file_keys: BTreeSet<PluginFileWriteKey>,
    rows: Vec<TransactionWriteRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginLifecycleKey {
    branch_id: String,
    plugin_key: String,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PluginBranchEntryKey {
    branch_id: String,
    plugin_key: String,
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

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            crate::live_state::LiveStateIndexContext::new(),
            CommitGraphContext::new(),
        )
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    #[tokio::test]
    #[ignore = "release-only transaction path-index benchmark probe"]
    async fn transaction_path_index_benchmark_probe() {
        let file_count = std::env::var("LIX_PATH_INDEX_BENCH_FILES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000);
        let rounds = std::env::var("LIX_PATH_INDEX_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(24);
        let warmup_rounds = 4;

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
