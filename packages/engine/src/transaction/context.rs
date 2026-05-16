use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobHash};
use crate::catalog::CatalogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::commit_store::CommitStoreContext;
use crate::domain::Domain;
use crate::entity_identity::EntityIdentity;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::{
    overlay_scan_rows, LiveStateContext, LiveStateRowRequest, LiveStateScanRequest,
    MaterializedLiveStateRow,
};
use crate::session::{SessionMode, WORKSPACE_VERSION_KEY};
use crate::sql2::SqlWriteExecutionContext;
use crate::storage::{StorageContext, StorageWriteSet, StorageWriteTransaction};
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
    stage_json_from_value, PreparedAdoptedStateRow, PreparedRowFacts, PreparedStateRow,
    PreparedTransactionWrite, TransactionAdoptedChange, TransactionFileData, TransactionJson,
    TransactionWrite, TransactionWriteMode, TransactionWriteOutcome, TransactionWriteRow,
};
use crate::transaction::validation::{validate_prepared_writes, TransactionValidationInput};
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome;

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
pub(crate) struct Transaction {
    active_version_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    commit_store: Arc<CommitStoreContext>,
    version_ctx: Arc<VersionContext>,
    schema_resolver: TransactionSchemaResolver,
    staged_writes: Arc<TransactionWriteBuffer>,
    storage_transaction: Box<dyn StorageWriteTransaction + Send + Sync + 'static>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
}

impl Transaction {
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL/provider hooks.
    async fn open(
        mode: &SessionMode,
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        commit_store: Arc<CommitStoreContext>,
        version_ctx: Arc<VersionContext>,
        catalog_context: Arc<CatalogContext>,
    ) -> Result<OpenTransaction, LixError> {
        let mut storage_transaction = storage.begin_write_transaction().await?;
        let setup_result = async {
            let active_version_id = resolve_active_version_id(
                mode,
                live_state.as_ref(),
                version_ctx.as_ref(),
                storage_transaction.as_mut(),
            )
            .await?;
            let runtime_functions = {
                let runtime_live_state = live_state.reader(storage_transaction.as_mut());
                FunctionContext::prepare(&runtime_live_state).await?
            };
            let functions = runtime_functions.provider();
            let visible_schemas = {
                let visible_live_state = live_state.reader(storage_transaction.as_mut());
                catalog_context
                    .schema_jsons_for_sql_read_planning(&visible_live_state, &active_version_id)
                    .await?
            };
            let schema_facts = {
                let visible_live_state = live_state.reader(storage_transaction.as_mut());
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
                    let _ = storage_transaction.rollback().await;
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
                commit_store,
                version_ctx,
                schema_resolver,
                staged_writes,
                storage_transaction,
                visible_schemas,
                functions,
            },
            runtime_functions,
        })
    }

    /// Commits prepared writes, runtime function state, and the backend transaction.
    ///
    /// Commit owns the execution boundary: prepared rows become commit-store
    /// facts, version-ref updates, and visible live_state rows before the
    /// backend transaction is committed.
    pub(crate) async fn commit(
        mut self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let prepared_writes = match self.staged_writes.drain() {
            Ok(prepared_writes) => prepared_writes,
            Err(error) => {
                let _ = self.storage_transaction.rollback().await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .validate_prepared_writes_by_version(&prepared_writes)
            .await
        {
            let _ = self.storage_transaction.rollback().await;
            return Err(error);
        }
        if let Err(error) = commit::commit_prepared_writes(
            &self.binary_cas,
            &self.commit_store,
            self.version_ctx.as_ref(),
            Some(runtime_functions),
            self.storage_transaction.as_mut(),
            prepared_writes,
        )
        .await
        {
            let _ = self.storage_transaction.rollback().await;
            return Err(error);
        }
        self.storage_transaction.commit().await?;
        Ok(TransactionCommitOutcome::default())
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        self.storage_transaction.rollback().await
    }

    /// Stages one decoded write batch into this transaction.
    ///
    /// This is the programmatic write entrypoint used by non-SQL APIs. The
    /// transaction still owns preparation from `TransactionWriteRow` into
    /// `PreparedStateRow`, so generated timestamps, change ids, commit ids, and
    /// commit membership stay in one place.
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
            TransactionWrite::AdoptedChanges { changes } => {
                PreparedTransactionWrite::AdoptedChanges {
                    rows: self.prepare_adopted_changes(changes).await?,
                }
            }
        })
    }

    async fn prepare_transaction_rows(
        &mut self,
        rows: Vec<TransactionWriteRow>,
    ) -> Result<Vec<PreparedStateRow>, LixError> {
        let row_count = rows.len();
        let staged = self.staged_writes.staging_overlay()?;
        let live_state = self.live_state.reader(self.storage_transaction.as_mut());
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

    async fn prepare_adopted_changes(
        &mut self,
        changes: Vec<TransactionAdoptedChange>,
    ) -> Result<Vec<PreparedAdoptedStateRow>, LixError> {
        let change_count = changes.len();
        let staged = self.staged_writes.staging_overlay()?;
        let live_state = self.live_state.reader(self.storage_transaction.as_mut());
        let mut changes_by_scope =
            BTreeMap::<Domain, Vec<(usize, TransactionAdoptedChange)>>::new();
        for (index, change) in changes.into_iter().enumerate() {
            let schema_scope_version_id = if change.version_id == GLOBAL_VERSION_ID {
                GLOBAL_VERSION_ID
            } else {
                change.version_id.as_str()
            };
            changes_by_scope
                .entry(Domain::schema_catalog(
                    schema_scope_version_id.to_string(),
                    false,
                ))
                .or_default()
                .push((index, change));
        }

        let mut prepared_rows = Vec::with_capacity(change_count);
        prepared_rows.resize_with(change_count, || None);
        for (domain, changes) in changes_by_scope {
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&live_state, &staged, &domain)
                .await?;
            for (_, change) in &changes {
                let row = &change.projected_row;
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
                remember_adopted_registered_schema(
                    Domain::schema_catalog(change.version_id.clone(), false),
                    row.snapshot_content.as_deref(),
                    catalog,
                )?;
            }
            let mut planned_changes = Vec::with_capacity(changes.len());
            for (index, change) in changes {
                let row = &change.projected_row;
                let Some((schema_plan_id, _)) = catalog.plan_for_key(&row.schema_key) else {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "schema '{}' is not visible to this transaction",
                            row.schema_key
                        ),
                    ));
                };
                if row.schema_key == REGISTERED_SCHEMA_KEY {
                    if row.file_id.is_some() {
                        return Err(LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            "lix_registered_schema rows must not be scoped to a file",
                        )
                        .with_hint("Schema definitions are scoped by version and durability only; write them with null file_id."));
                    }
                    remember_adopted_registered_schema(
                        Domain::schema_catalog(change.version_id.clone(), false),
                        row.snapshot_content.as_deref(),
                        catalog,
                    )?;
                }
                planned_changes.push((index, change, schema_plan_id));
            }
            for (index, change, schema_plan_id) in planned_changes {
                prepared_rows[index] = Some(prepare_adopted_state_row(change, schema_plan_id)?);
            }
        }
        Ok(prepared_rows
            .into_iter()
            .map(|row| row.expect("every adopted row should be prepared exactly once"))
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
            let live_state = self.live_state.reader(self.storage_transaction.as_mut());
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
        let reader = self
            .version_ctx
            .ref_reader(self.storage_transaction.as_mut());
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

    /// Adds an extra parent to the commit generated for `version_id`.
    ///
    /// Merge uses this to preserve source-branch ancestry. Ordinary writes do
    /// not call this because commit finalization already parents to the
    /// version's previous head.
    pub(crate) fn add_commit_parent(
        &self,
        version_id: String,
        parent_commit_id: String,
    ) -> Result<(), LixError> {
        self.staged_writes
            .add_commit_parent(version_id, parent_commit_id)
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
        let mut writes = StorageWriteSet::new();
        let canonical_row = prepare_version_ref_row(version_id, commit_id, &timestamp)?;
        self.version_ctx
            .stage_canonical_ref_rows(&mut writes, &[canonical_row.row])?;
        writes
            .apply(&mut self.storage_transaction.as_mut())
            .await
            .map(|_| ())
    }

    /// Returns the commit id currently staged for `version_id`, if tracked rows
    /// have been staged for that version.
    pub(crate) fn staged_commit_id(&self, version_id: &str) -> Result<Option<String>, LixError> {
        self.staged_writes.staged_commit_id(version_id)
    }

    /// Stages a commit for `version_id` even if no tracked rows changed.
    pub(crate) fn stage_empty_commit(&self, version_id: String) -> Result<String, LixError> {
        self.staged_writes.stage_empty_commit(version_id)
    }

    /// Creates a version-ref reader scoped to this write transaction.
    pub(crate) fn version_ref_reader(&mut self) -> impl VersionRefReader + '_ {
        self.version_ctx
            .ref_reader(self.storage_transaction.as_mut())
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) fn tracked_state_reader(
        &mut self,
    ) -> TrackedStateStoreReader<&mut dyn StorageWriteTransaction> {
        self.tracked_state.reader(self.storage_transaction.as_mut())
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) fn commit_graph_reader(
        &mut self,
    ) -> CommitGraphStoreReader<&mut dyn StorageWriteTransaction> {
        CommitGraphContext::new().reader(self.storage_transaction.as_mut())
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

fn remember_adopted_registered_schema(
    domain: Domain,
    snapshot_content: Option<&str>,
    catalog: &mut crate::catalog::CatalogSnapshot,
) -> Result<(), LixError> {
    let snapshot = snapshot_content
        .map(|value| {
            serde_json::from_str::<JsonValue>(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_UNKNOWN,
                    format!("adopted registered schema snapshot_content is invalid JSON: {error}"),
                )
            })
        })
        .transpose()?;
    remember_pending_registered_schema(snapshot.as_ref(), domain, catalog)
}

fn prepare_adopted_state_row(
    change: TransactionAdoptedChange,
    schema_plan_id: crate::catalog::SchemaPlanId,
) -> Result<PreparedAdoptedStateRow, LixError> {
    if change.change_id != change.projected_row.change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' does not match projected row change_id '{}'",
                change.change_id, change.projected_row.change_id
            ),
        ));
    }
    let row = change.projected_row;
    let snapshot = row
        .snapshot_content
        .as_deref()
        .map(|value| stage_materialized_json_text(value, "adopted row snapshot_content"))
        .transpose()?;
    let metadata = row
        .metadata
        .as_deref()
        .map(|value| stage_materialized_json_text(value, "adopted row metadata"))
        .transpose()?;
    Ok(PreparedAdoptedStateRow {
        schema_plan_id,
        facts: PreparedRowFacts::default(),
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot,
        metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: change.version_id == GLOBAL_VERSION_ID,
        change_id: change.change_id,
        commit_id: String::new(),
        version_id: change.version_id,
    })
}

fn stage_materialized_json_text(
    value: &str,
    context: &str,
) -> Result<crate::transaction::types::StageJson, LixError> {
    let parsed = serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!("{context} is invalid JSON: {error}"),
        )
    })?;
    let prepared = TransactionJson::from_value(parsed, context)?;
    stage_json_from_value(prepared, context)
}

pub(crate) struct OpenTransaction {
    pub(crate) transaction: Transaction,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction(
    mode: &SessionMode,
    storage: StorageContext,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    commit_store: Arc<CommitStoreContext>,
    version_ctx: Arc<VersionContext>,
    catalog_context: Arc<CatalogContext>,
) -> Result<OpenTransaction, LixError> {
    Transaction::open(
        mode,
        storage,
        live_state,
        tracked_state,
        binary_cas,
        commit_store,
        version_ctx,
        catalog_context,
    )
    .await
}

#[async_trait]
impl SqlWriteExecutionContext for Transaction {
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
        self.binary_cas
            .reader(self.storage_transaction.as_mut())
            .load_bytes_many(hashes)
            .await
    }

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let base = self.live_state.reader(self.storage_transaction.as_mut());
        overlay_scan_rows(&base, &staged, request).await
    }

    async fn load_version_head(&mut self, version_id: &str) -> Result<Option<String>, LixError> {
        self.version_ctx
            .ref_reader(self.storage_transaction.as_mut())
            .load_head_commit_id(version_id)
            .await
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
        TransactionWrite::AdoptedChanges { changes } => changes
            .iter()
            .map(|change| change.version_id.clone())
            .collect(),
    }
}

#[cfg(feature = "storage-benches")]
fn transaction_write_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.len(),
        TransactionWrite::RowsWithFileData { rows, .. } => rows.len(),
        TransactionWrite::AdoptedChanges { changes } => changes.len(),
    }
}

#[cfg(feature = "storage-benches")]
fn transaction_write_untracked_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.iter().filter(|row| row.untracked).count(),
        TransactionWrite::RowsWithFileData { rows, .. } => {
            rows.iter().filter(|row| row.untracked).count()
        }
        TransactionWrite::AdoptedChanges { .. } => 0,
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
        TransactionWrite::AdoptedChanges { .. } => Ok(()),
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
    transaction: &mut dyn StorageWriteTransaction,
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { version_id } => Ok(version_id.clone()),
        SessionMode::Workspace => {
            load_workspace_version_id(live_state, version_ctx, transaction).await
        }
    }
}

async fn load_workspace_version_id(
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    transaction: &mut dyn StorageWriteTransaction,
) -> Result<String, LixError> {
    let row = live_state
        .reader(&mut *transaction)
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
        .ref_reader(&mut *transaction)
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
    use crate::backend::testing::UnitTestBackend;
    use crate::commit_store::{ChangeScanRequest, CommitStoreContext};
    use crate::tracked_state::{TrackedStateRowRequest, TrackedStateScanRequest};
    use crate::transaction::types::TransactionJson;
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};
    use crate::version::VersionContext;
    use crate::Backend;
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(CommitStoreContext::new());
        let commit_store = Arc::new(CommitStoreContext::new());
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
            Arc::clone(&commit_store),
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

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangeScanRequest::default())
            .await
            .expect("changelog should scan");
        assert!(
            changes.iter().any(|change| change
                .record
                .entity_id
                .as_single_string_owned()
                .as_deref()
                == Ok("tracked-programmatic")),
            "tracked staged row should be appended to changelog"
        );
        assert!(
            !changes.iter().any(|change| change
                .record
                .entity_id
                .as_single_string_owned()
                .as_deref()
                == Ok("untracked-programmatic")),
            "untracked staged row must not be appended to changelog"
        );

        let head_commit_id = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load")
            .expect("tracked commit should advance the global version ref");

        let tracked_row = crate::tracked_state::TrackedStateContext::new()
            .reader(storage.clone())
            .load_rows_at_commit(
                &head_commit_id,
                &[TrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(
                        "tracked-programmatic",
                    ),
                    file_id: NullableKeyFilter::Null,
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
            .reader(storage.clone())
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
            .reader(storage.clone())
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
            .reader(storage.clone())
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(CommitStoreContext::new());
        let commit_store = Arc::new(CommitStoreContext::new());
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
            Arc::clone(&commit_store),
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

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangeScanRequest::default())
            .await
            .expect("changelog should scan after failed commit");
        assert!(
            changes.iter().all(|change| change
                .record
                .entity_id
                .as_single_string_owned()
                .as_deref()
                != Ok("invalid-programmatic")),
            "validation failure must happen before changelog persistence"
        );
        let head = version_ctx
            .ref_reader(storage.clone())
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let (live_state, _binary_cas, changelog, version_ref, runtime_functions, mut transaction) =
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
            &changelog,
            &version_ref,
            "invalid-metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let (live_state, _binary_cas, changelog, version_ref, runtime_functions, mut transaction) =
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
            &changelog,
            &version_ref,
            "schema-mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (
            _live_state,
            _binary_cas,
            _changelog,
            _version_ref,
            _runtime_functions,
            mut transaction,
        ) = open_test_transaction(&backend).await;

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
        backend: &Arc<dyn Backend + Send + Sync>,
    ) -> (
        Arc<LiveStateContext>,
        Arc<BinaryCasContext>,
        Arc<CommitStoreContext>,
        Arc<VersionContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageContext::new(Arc::clone(backend));
        let live_state = Arc::new(live_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(CommitStoreContext::new());
        let commit_store = Arc::new(CommitStoreContext::new());
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
            Arc::clone(&commit_store),
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
            changelog,
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
        let mut storage_transaction = storage
            .begin_write_transaction()
            .await
            .expect("schema fixture transaction should open");
        crate::test_support::stage_tracked_root_from_materialized(
            storage_transaction.as_mut(),
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
        writes
            .apply(&mut storage_transaction.as_mut())
            .await
            .expect("schema fixture rows should apply");
        storage_transaction
            .commit()
            .await
            .expect("schema fixture transaction should commit");
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageContext,
        live_state: &LiveStateContext,
        changelog: &CommitStoreContext,
        version_ctx: &VersionContext,
        rejected_entity_id: &str,
    ) {
        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangeScanRequest::default())
            .await
            .expect("changelog should scan after failed commit");
        assert!(
            changes.iter().all(|change| change
                .record
                .entity_id
                .as_single_string_owned()
                .as_deref()
                != Ok(rejected_entity_id)),
            "validation failure must happen before changelog persistence"
        );
        let head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load after failed commit");
        assert_eq!(
            head.as_deref(),
            Some(SCHEMA_FIXTURE_COMMIT_ID),
            "validation failure must not advance the version ref"
        );
        let row = live_state
            .reader(storage)
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
