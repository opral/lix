#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobDataReader, BlobHash};
use crate::branch::{BranchContext, BranchRefReader};
use crate::catalog::CatalogContext;
use crate::changelog::{ChangeId, CommitId};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::common::LixTimestamp;
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    FilesystemIndex, FilesystemRowContext, blob_ref_tombstone_row, filesystem_schema_keys,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::{
    LiveStateContext, LiveStateFileScanRequest, LiveStateFilter, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::live_state::{overlay_scan_file_rows, overlay_scan_rows};
use crate::plugin::{
    InstalledPlugin, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH, PluginDetectedChange, PluginRuntimeHost,
    detect_changes_with_plugin, load_installed_plugins_from_filesystem,
    plugin_schema_rows_from_archive_path, plugin_state_live_state_projection,
    retain_plugin_state_rows, select_plugin_for_path,
};
use crate::session::{SessionMode, WORKSPACE_BRANCH_KEY};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource,
};
use crate::storage::{
    InMemoryStorageBackend, StorageBackend, StorageReadOptions, StorageWriteOptions,
    StorageWriteSetStats,
};
use crate::storage::{
    SharedStorageRead, StorageContext, StorageRead, StorageReadScope, StorageWriteSet,
};
use crate::tracked_state::{TrackedStateContext, TrackedStateStoreReader};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedTransactionWriteRow, REGISTERED_SCHEMA_KEY, normalize_transaction_write_row,
    remember_pending_registered_schema,
};
use crate::transaction::prepare_branch_ref_row;
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{PreparedWriteSet, TransactionWriteBuffer};
use crate::transaction::types::{
    PreparedStateRow, PreparedTransactionWrite, StagedCommitChangeRef, TransactionFileData,
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
    TransactionWriteRow, stage_json_from_value,
};
use crate::transaction::validation::{TransactionValidationInput, validate_prepared_writes};
use crate::{LixError, NullableKeyFilter, SqlQueryResult, Value};

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
    active_branch_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
    schema_resolver: TransactionSchemaResolver,
    staged_writes: Arc<TransactionWriteBuffer>,
    staged_storage_writes: StorageWriteSet,
    storage: StorageContext<B>,
    sql_schema_cache: SqlSchemaCache,
    functions: FunctionProviderHandle,
    commit_boundary: Option<TransactionCommitBoundary>,
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

    fn commit<T>(&self, commit: impl FnOnce() -> Result<T, LixError>) -> Result<T, LixError> {
        let _gate = self.state.lock_commit();
        self.check()?;
        commit()
    }
}

#[derive(Clone)]
pub(crate) struct CommitBoundaryState {
    active_count: Arc<AtomicUsize>,
    commit_gate: Arc<std::sync::Mutex<()>>,
    watch: tokio::sync::watch::Sender<usize>,
}

impl CommitBoundaryState {
    pub(crate) fn new() -> Self {
        let (watch, _) = tokio::sync::watch::channel(0);
        Self {
            active_count: Arc::new(AtomicUsize::new(0)),
            commit_gate: Arc::new(std::sync::Mutex::new(())),
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

    pub(crate) fn lock_commit(&self) -> std::sync::MutexGuard<'_, ()> {
        self.commit_gate
            .lock()
            .expect("commit boundary gate should not poison")
    }

    pub(crate) fn try_lock_commit(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        match self.commit_gate.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("commit boundary gate should not poison")
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
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Opens an execution-scoped staging area for SQL/provider hooks.
    async fn open(
        mode: &SessionMode,
        storage: StorageContext<B>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        plugin_host: PluginRuntimeHost,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
    ) -> Result<OpenTransaction<B>, LixError> {
        let read = SharedStorageRead::new(storage.begin_read(StorageReadOptions::default())?);
        let setup_result = async {
            let active_branch_id =
                resolve_active_branch_id(mode, live_state.as_ref(), branch_ctx.as_ref(), &read)
                    .await?;
            let runtime_functions = {
                let runtime_live_state = live_state.reader(&read);
                FunctionContext::prepare(&runtime_live_state).await?
            };
            let functions = runtime_functions.provider();
            let schema_facts = {
                let visible_live_state = live_state.reader(&read);
                catalog_context
                    .schema_facts_for_domain(
                        &visible_live_state,
                        &Domain::schema_catalog(active_branch_id.clone(), true),
                    )
                    .await?
            };
            Ok::<_, LixError>((active_branch_id, runtime_functions, functions, schema_facts))
        }
        .await;
        let (active_branch_id, runtime_functions, functions, schema_facts) = match setup_result {
            Ok(result) => result,
            Err(error) => {
                return Err(error);
            }
        };
        drop(read);
        let mut schema_resolver = TransactionSchemaResolver::new(Arc::clone(&catalog_context));
        schema_resolver.remember_schema_facts(
            &Domain::schema_catalog(active_branch_id.clone(), true),
            schema_facts,
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
                staged_storage_writes: StorageWriteSet::new(),
                storage,
                sql_schema_cache: SqlSchemaCache::default(),
                functions,
                commit_boundary: None,
            },
            runtime_functions,
        })
    }

    /// Commits prepared writes, runtime function state, and the backend transaction.
    ///
    /// Commit owns the execution boundary: prepared rows become changelog
    /// facts, branch-ref updates, and visible live_state rows before the
    /// backend transaction is committed.
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
        let mut read = SharedStorageRead::new(
            transaction
                .storage
                .begin_read(StorageReadOptions::default())?,
        );
        let mut writes = match commit::commit_prepared_writes(
            &transaction.binary_cas,
            transaction.branch_ctx.as_ref(),
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
        let prepared_commit = transaction
            .storage
            .prepare_write_set(writes, StorageWriteOptions::default())?;
        let storage_stats = commit_at_boundary(commit_boundary.as_ref(), || {
            let (_commit, stats) = prepared_commit.commit()?;
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
        self.staged_writes.stage_write(write)
    }

    async fn scan_visible_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let base = self.live_state.reader(read);
        overlay_scan_rows(&base, &staged, request).await
    }

    async fn scan_visible_live_state_file(
        &mut self,
        request: &LiveStateFileScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let base = self.live_state.reader(read);
        overlay_scan_file_rows(&base, &staged, request).await
    }

    async fn reconcile_plugin_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWrite, LixError> {
        match write {
            TransactionWrite::Rows { mode, mut rows } => {
                rows.extend(self.plugin_delete_tombstone_rows(&rows).await?);
                Ok(TransactionWrite::Rows { mode, rows })
            }
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
            } => {
                let mut rows = rows;
                let PluginFileDataReconciliation {
                    file_keys,
                    rows: plugin_rows,
                } = self.plugin_file_data_reconciliation(&file_data).await?;
                rows.retain(|row| !file_keys.iter().any(|key| key.matches_blob_ref_row(row)));
                rows.extend(plugin_rows);
                rows.extend(self.plugin_delete_tombstone_rows(&rows).await?);
                let file_data = file_data
                    .into_iter()
                    .filter(|write| {
                        !file_keys.contains(&PluginFileWriteKey::from(write))
                            && !write.data.is_empty()
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

    async fn plugin_file_data_reconciliation(
        &mut self,
        file_data: &[TransactionFileData],
    ) -> Result<PluginFileDataReconciliation, LixError> {
        let mut reconciliation = PluginFileDataReconciliation::default();
        for write in file_data {
            if let Some(rows) = plugin_archive_schema_rows_for_write(write)? {
                reconciliation.rows.extend(rows);
                continue;
            }
            if !is_plugin_reconciliation_candidate_path(&write.path) {
                continue;
            }
            let filesystem = self.filesystem_index_for_branch(&write.branch_id).await?;
            let installed_plugins = self.installed_plugins_for_filesystem(&filesystem).await?;
            let existing_file = filesystem.file_entries().find(|(_, file)| {
                file.id == write.file_id
                    && file.scope.global == write.global
                    && file.scope.untracked == write.untracked
            });
            let existing_plugin = existing_file
                .and_then(|(path, _)| select_plugin_for_path(&installed_plugins, path));
            let selected_plugin = select_plugin_for_path(&installed_plugins, &write.path);
            let context = FilesystemRowContext {
                branch_id: write.branch_id.clone(),
                global: write.global,
                untracked: write.untracked,
                file_id: None,
                metadata: None,
            };
            if let Some(existing_plugin) = existing_plugin
                && selected_plugin.is_none_or(|plugin| plugin.key != existing_plugin.key)
            {
                let existing_state = self
                    .active_plugin_state_rows(&write.branch_id, &write.file_id, existing_plugin)
                    .await?;
                reconciliation.rows.extend(plugin_state_tombstone_rows(
                    &existing_state,
                    &write.file_id,
                    &context,
                ));
            }
            let Some(plugin) = selected_plugin else {
                continue;
            };
            let active_state = self
                .active_plugin_state_rows(&write.branch_id, &write.file_id, plugin)
                .await?;
            let changes = detect_changes_with_plugin(
                &self.plugin_host,
                plugin,
                &active_state,
                write.data.clone(),
            )
            .await?;
            if existing_file.is_some_and(|(_, file)| file.blob_hash.is_some()) {
                reconciliation.rows.push(blob_ref_tombstone_row(
                    write.file_id.clone(),
                    context.clone(),
                ));
            }
            reconciliation.rows.extend(plugin_change_rows(
                plugin,
                changes,
                &write.file_id,
                &context,
                "plugin detect-changes",
            )?);
            reconciliation
                .file_keys
                .insert(PluginFileWriteKey::from(write));
        }
        Ok(reconciliation)
    }

    async fn plugin_delete_tombstone_rows(
        &mut self,
        rows: &[TransactionWriteRow],
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let mut tombstones = Vec::new();
        let mut seen = BTreeSet::new();
        for row in rows {
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
            let key = PluginFileWriteKey {
                branch_id: row.branch_id.clone(),
                global: row.global,
                untracked: row.untracked,
                file_id: file_id.to_string(),
            };
            if !seen.insert(key) {
                continue;
            }
            let filesystem = self.filesystem_index_for_branch(&row.branch_id).await?;
            let Some((path, _file)) = filesystem.file_entries().find(|(_, file)| {
                file.id == file_id
                    && file.scope.global == row.global
                    && file.scope.untracked == row.untracked
            }) else {
                continue;
            };
            if !is_plugin_reconciliation_candidate_path(path) {
                continue;
            }
            let installed_plugins = self.installed_plugins_for_filesystem(&filesystem).await?;
            let Some(plugin) = select_plugin_for_path(&installed_plugins, path) else {
                continue;
            };
            let active_state = self
                .active_plugin_state_rows(&row.branch_id, file_id, plugin)
                .await?;
            let context = FilesystemRowContext {
                branch_id: row.branch_id.clone(),
                global: row.global,
                untracked: row.untracked,
                file_id: None,
                metadata: row.metadata.clone(),
            };
            tombstones.extend(plugin_state_tombstone_rows(
                &active_state,
                file_id,
                &context,
            ));
        }
        Ok(tombstones)
    }

    async fn filesystem_index_for_branch(
        &mut self,
        branch_id: &str,
    ) -> Result<FilesystemIndex, LixError> {
        let rows = self
            .scan_visible_live_state(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: filesystem_schema_keys(),
                    branch_ids: vec![branch_id.to_string()],
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        FilesystemIndex::from_live_rows(rows)
    }

    async fn installed_plugins_for_filesystem(
        &self,
        filesystem: &FilesystemIndex,
    ) -> Result<Vec<InstalledPlugin>, LixError> {
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let blob_reader = self.binary_cas.reader(read);
        load_installed_plugins_from_filesystem(filesystem, &blob_reader).await
    }

    async fn active_plugin_state_rows(
        &mut self,
        branch_id: &str,
        file_id: &str,
        plugin: &InstalledPlugin,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let rows = self
            .scan_visible_live_state_file(&LiveStateFileScanRequest {
                branch_ids: vec![branch_id.to_string()],
                file_id: file_id.to_string(),
                schema_keys: plugin.schema_keys.clone(),
                projection: plugin_state_live_state_projection(),
                ..Default::default()
            })
            .await?;
        Ok(retain_plugin_state_rows(plugin, rows))
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
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
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

    async fn validate_prepared_writes_by_branch(
        &mut self,
        prepared_writes: &PreparedWriteSet,
    ) -> Result<(), LixError> {
        let validation_index = prepared_writes.validation_index();
        for scope in validation_index.schema_scopes() {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let branch_prepared_writes = validation_index.validation_set_for_schema_scope(scope);
            let read =
                SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
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
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
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

    pub(crate) async fn execute_read_sql_statement(
        &mut self,
        sql: &str,
        statement: DataFusionStatement,
        params: &[Value],
    ) -> Result<SqlQueryResult, LixError> {
        self.prepare_sql_visible_schemas().await?;
        let storage = self.storage.clone();
        let read = storage.begin_read(StorageReadOptions::default())?;
        let active_branch_id = self.active_branch_id.clone();
        let live_state = Arc::clone(&self.live_state);
        let binary_cas = Arc::clone(&self.binary_cas);
        let branch_ctx = Arc::clone(&self.branch_ctx);
        let visible_schemas = self.cached_visible_schemas()?.to_vec();
        let functions = self.functions.clone();
        let staged = self.staged_writes.staging_overlay()?;
        let staged_writes = Arc::clone(&self.staged_writes);
        let plugin_host = self.plugin_host.clone();

        with_static_transaction_sql_read::<B, _, _>(read, |read_store| async move {
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
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
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
        let timestamp = self.functions.call_timestamp().to_string();
        let canonical_row = prepare_branch_ref_row(branch_id, &commit_id, &timestamp)?;
        self.branch_ctx
            .stage_canonical_ref_rows(&mut self.staged_storage_writes, &[canonical_row.row])
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
    pub(crate) fn branch_ref_reader(&mut self) -> impl BranchRefReader + '_ {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        self.branch_ctx.ref_reader(SharedStorageRead::new(read))
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) fn tracked_state_reader(
        &mut self,
    ) -> TrackedStateStoreReader<SharedStorageRead<B::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        self.tracked_state.reader(SharedStorageRead::new(read))
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) fn commit_graph_reader(
        &mut self,
    ) -> CommitGraphStoreReader<SharedStorageRead<B::Read<'_>>> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("open transaction read scope");
        CommitGraphContext::new().reader(SharedStorageRead::new(read))
    }
}

pub(crate) struct TransactionSqlReadExecutionContext<R: crate::storage::StorageBackendRead> {
    active_branch_id: String,
    read_store: SharedStorageRead<R>,
    live_state: Arc<LiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    branch_ctx: Arc<BranchContext>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    staged: crate::transaction::staging::PreparedStateRowOverlay,
    staged_writes: Arc<TransactionWriteBuffer>,
    plugin_host: PluginRuntimeHost,
}

impl<R> SqlExecutionContext for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage::StorageBackendRead + Send + 'static,
{
    type ReadStore = SharedStorageRead<R>;

    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
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

struct TransactionReadLiveStateReader<R: crate::storage::StorageBackendRead> {
    base: crate::live_state::LiveStateStoreReader<SharedStorageRead<R>>,
    staged: crate::transaction::staging::PreparedStateRowOverlay,
}

#[async_trait]
impl<R> crate::live_state::LiveStateReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage::StorageBackendRead + Send + 'static,
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

/// Runs one transaction SQL read using a widened backend-read lifetime.
///
/// DataFusion requires provider state to be `'static`, but transaction reads
/// are scoped to the current backend snapshot. Keep this bridge private to
/// transaction SQL execution so no crate-level API can receive the widened
/// backend read handle.
async fn with_static_transaction_sql_read<B, F, Fut>(
    read: StorageReadScope<B::Read<'_>>,
    f: F,
) -> Result<SqlQueryResult, LixError>
where
    B: StorageBackend + 'static,
    F: FnOnce(SharedStorageRead<B::Read<'static>>) -> Fut,
    Fut: Future<Output = Result<SqlQueryResult, LixError>>,
{
    // SAFETY: the widened read is wrapped immediately in `SharedStorageRead`,
    // only passed into this private SQL execution closure, and explicitly
    // closed before returning. Escaped clones are detected by `close()`.
    let read = unsafe { assume_static_backend_read::<B>(read) };
    let read = SharedStorageRead::new(read);
    let close = read.clone();
    let result = f(read).await;
    let close_result = close.close().map_err(LixError::from);
    match (result, close_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (_, Err(close_error)) => Err(close_error),
    }
}

/// Erases the backend borrow lifetime for scoped transaction SQL execution.
///
/// # Safety
///
/// The returned read scope must not outlive the backend value that produced
/// `read`, and it must be dropped before the enclosing SQL execution returns.
unsafe fn assume_static_backend_read<B>(
    read: StorageReadScope<B::Read<'_>>,
) -> StorageReadScope<B::Read<'static>>
where
    B: StorageBackend + 'static,
{
    let read = std::mem::ManuallyDrop::new(read);
    unsafe {
        std::ptr::read(std::ptr::from_ref(&*read).cast::<StorageReadScope<B::Read<'static>>>())
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
        created_at,
        updated_at,
        global: row.global,
        change_id: if row.untracked {
            row.change_id
                .as_deref()
                .map(|id| ChangeId::parse_lix(id, "prepared untracked row change_id"))
                .transpose()?
        } else {
            Some(match row.change_id {
                Some(change_id) => {
                    ChangeId::parse_lix(&change_id, "prepared tracked row change_id")?
                }
                None => ChangeId::from(functions.call_uuid_v7()),
            })
        },
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
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
) -> Result<OpenTransaction<B>, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
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
impl<B> SqlWriteExecutionContext for Transaction<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
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
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);
        let base = self.binary_cas.reader(read);
        load_transaction_blob_bytes(&base, &self.staged_writes, hashes).await
    }

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_visible_live_state(request).await
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        let read = SharedStorageRead::new(self.storage.begin_read(StorageReadOptions::default())?);

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

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

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
struct PluginFileDataReconciliation {
    file_keys: BTreeSet<PluginFileWriteKey>,
    rows: Vec<TransactionWriteRow>,
}

fn plugin_archive_schema_rows_for_write(
    write: &TransactionFileData,
) -> Result<Option<Vec<TransactionWriteRow>>, LixError> {
    if !write.path.starts_with(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH) {
        return Ok(None);
    }
    Ok(Some(plugin_schema_rows_from_archive_path(
        &write.path,
        &write.data,
        &write.branch_id,
        write.global,
        write.untracked,
    )?))
}

fn is_plugin_reconciliation_candidate_path(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    !path.starts_with(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)
}

fn plugin_change_rows(
    plugin: &InstalledPlugin,
    changes: Vec<PluginDetectedChange>,
    file_id: &str,
    context: &FilesystemRowContext,
    json_context: &str,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let schema_keys = plugin.schema_keys.iter().collect::<BTreeSet<_>>();
    changes
        .into_iter()
        .map(|change| {
            if !schema_keys.contains(&change.schema_key) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin '{}' emitted schema key '{}' that is not declared in its manifest",
                        plugin.key, change.schema_key
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
    read: &(impl StorageRead + Send + Sync + ?Sized),
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { branch_id } => Ok(branch_id.clone()),
        SessionMode::Workspace => load_workspace_branch_id(live_state, branch_ctx, read).await,
    }
}

async fn load_workspace_branch_id(
    live_state: &LiveStateContext,
    branch_ctx: &BranchContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
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

    use serde_json::json;

    use super::*;
    use crate::GLOBAL_BRANCH_ID;
    use crate::NullableKeyFilter;
    use crate::branch::BranchContext;
    use crate::changelog::ChangelogReader;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions};
    use crate::tracked_state::{TrackedStateKey, TrackedStateScanRequest};
    use crate::transaction::types::TransactionJson;
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            UntrackedStateContext::new(),
            CommitGraphContext::new(),
        )
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let tracked_state = Arc::new(TrackedStateContext::new());
        let branch_ctx = Arc::new(BranchContext::new(Arc::new(UntrackedStateContext::new())));
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

        let untracked_row = UntrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single("untracked-programmatic"),
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

        let tracked_rows = TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
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
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

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
        assert_eq!(rows[0].created_at, "1970-01-01T00:00:00.000Z");
        assert_eq!(rows[0].updated_at, "2026-04-23T00:00:00.123Z");
    }

    #[tokio::test]
    async fn commit_validates_staged_rows_before_persistence() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new(Arc::new(UntrackedStateContext::new())));
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
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend.clone());
        let (live_state, _binary_cas, branch_ref, runtime_functions, mut transaction) =
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
            &branch_ref,
            "invalid-metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
    async fn stage_rows_rejects_missing_branch_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

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
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

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
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let (live_state, _binary_cas, branch_ref, runtime_functions, mut transaction) =
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
            &branch_ref,
            "schema-mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let backend = InMemoryStorageBackend::new();
        let (_live_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&backend).await;

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
        backend: &InMemoryStorageBackend,
    ) -> (
        Arc<LiveStateContext>,
        Arc<BinaryCasContext>,
        Arc<BranchContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageContext::new(backend.clone());
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new(Arc::new(UntrackedStateContext::new())));
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

    async fn seed_visible_schema_rows(storage: StorageContext) {
        let mut writes = StorageWriteSet::new();
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
        let branch_ref_row = prepare_branch_ref_row(
            GLOBAL_BRANCH_ID,
            &CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
            "1970-01-01T00:00:00.000Z",
        )
        .expect("schema fixture branch ref should stage");
        let mut read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("schema fixture read should open");
        crate::test_support::stage_tracked_root_from_materialized(
            &mut read,
            &mut writes,
            &TrackedStateContext::new(),
            SCHEMA_FIXTURE_COMMIT_ID,
            None,
            &rows,
        )
        .await
        .expect("schema fixture rows should stage");
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows([branch_ref_row.row.as_ref()])
            .expect("schema fixture branch ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("schema fixture transaction should commit");
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageContext,
        live_state: &LiveStateContext,
        branch_ctx: &BranchContext,
        rejected_entity_pk: &str,
    ) {
        let head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
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
