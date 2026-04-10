use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use jsonschema::JSONSchema;

use crate::backend::TransactionBackendAdapter;
use crate::catalog::CatalogProjectionRegistry;
use crate::common::escape_sql_string;
use crate::contracts::parse_active_version_snapshot;
use crate::contracts::TrackedChangeView;
use crate::contracts::{
    ChangeBatch, CommitPreconditions, ExpectedHead, PendingPublicCommitSession,
    PreparedPublicReadArtifact, PreparedPublicWriteArtifact, PublicChange, SchemaKey, WriteLane,
};
use crate::contracts::{CompiledSchemaCache, PendingView};
use crate::contracts::{LixFunctionProvider, SharedFunctionProvider};
use crate::execution::write::buffered::TrackedTxnUnit;
use crate::execution::write::filesystem::runtime::{
    resolve_binary_blob_writes_in_transaction, BinaryBlobWrite,
};
use crate::execution::write::transaction::TransactionExecutionBackend;
use crate::execution::write::{TrackedCommitExecutionOutcome, WriteExecutionBindings};
use crate::live_state::RowIdentity;
use crate::session::collaborators::SessionCollaborators;
use crate::session::read_execution_bindings::CatalogProjectionRegistryReadExecutionBindings;
use crate::session::version_ops::commit::{
    append_tracked_with_pending_public_session, BufferedTrackedAppendArgs,
    CreateCommitAppliedOutput, CreateCommitDisposition, CreateCommitError, CreateCommitErrorKind,
    CreateCommitExpectedHead, CreateCommitIdempotencyKey, CreateCommitInvariantChecker,
    CreateCommitPreconditions, CreateCommitWriteLane, StagedChange,
};
use crate::session::write_validation::validate_commit_time_write;
use crate::{CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId};
use crate::{LixBackendTransaction, LixError, QueryResult, VersionId};

const ACTIVE_VERSION_SCHEMA_KEY: &str = "lix_active_version";
const ACTIVE_VERSION_FILE_ID: &str = "lix";
const GLOBAL_VERSION_ID: &str = "global";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";

struct WriteCompiledSchemaCache {
    inner: RwLock<HashMap<SchemaKey, Arc<JSONSchema>>>,
}

impl WriteCompiledSchemaCache {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl CompiledSchemaCache for WriteCompiledSchemaCache {
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>> {
        self.inner
            .read()
            .expect("write compiled schema cache lock poisoned")
            .get(key)
            .cloned()
    }

    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>) {
        self.inner
            .write()
            .expect("write compiled schema cache lock poisoned")
            .insert(key, schema);
    }
}

struct PublicCommitInvariantChecker<'a> {
    public_write: &'a PreparedPublicWriteArtifact,
    schema_cache: WriteCompiledSchemaCache,
}

impl<'a> PublicCommitInvariantChecker<'a> {
    fn new(public_write: &'a PreparedPublicWriteArtifact) -> Self {
        Self {
            public_write,
            schema_cache: WriteCompiledSchemaCache::new(),
        }
    }
}

#[async_trait(?Send)]
impl CreateCommitInvariantChecker for PublicCommitInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), CreateCommitError> {
        let backend = TransactionExecutionBackend::new(transaction);
        validate_commit_time_write(&backend, &self.schema_cache, self.public_write)
            .await
            .map_err(|error| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

#[async_trait(?Send)]
impl WriteExecutionBindings for SessionCollaborators {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError> {
        execute_prepared_public_read_with_registry(
            self.catalog_projection_registry(),
            transaction,
            pending_view,
            public_read,
        )
        .await
    }

    async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        writes: &[BinaryBlobWrite],
    ) -> Result<(), LixError> {
        persist_binary_blob_writes(transaction, writes).await
    }

    async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas(transaction).await
    }

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Result<(), LixError> {
        persist_runtime_sequence(transaction, functions).await
    }

    async fn execute_public_tracked_append_txn_with_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        unit: &TrackedTxnUnit,
        mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
    ) -> Result<TrackedCommitExecutionOutcome, LixError> {
        execute_public_tracked_append(transaction, unit, pending_commit_session.as_deref_mut())
            .await
    }
}

pub(crate) async fn execute_prepared_public_read_with_registry(
    projection_registry: &CatalogProjectionRegistry,
    transaction: &mut dyn LixBackendTransaction,
    pending_view: Option<&dyn PendingView>,
    public_read: &PreparedPublicReadArtifact,
) -> Result<QueryResult, LixError> {
    match public_read.contract.execution_mode() {
        crate::contracts::PublicReadExecutionMode::PendingView => {
            crate::session::pending_reads::execute_prepared_public_read_with_pending_view_in_transaction(
                transaction,
                pending_view,
                public_read,
            )
            .await
        }
        crate::contracts::PublicReadExecutionMode::Committed(_) => {
            let bindings = CatalogProjectionRegistryReadExecutionBindings::new(projection_registry);
            crate::execution::read::execute_prepared_public_read_artifact_in_transaction(
                transaction,
                &bindings,
                public_read,
            )
            .await
        }
    }
}

pub(crate) async fn persist_binary_blob_writes(
    transaction: &mut dyn LixBackendTransaction,
    writes: &[BinaryBlobWrite],
) -> Result<(), LixError> {
    let resolved = resolve_binary_blob_writes_in_transaction(transaction, writes)
        .await?
        .into_iter()
        .map(
            |write| crate::binary_cas::support::ResolvedBinaryBlobWrite {
                file_id: write.file_id,
                version_id: write.version_id,
                untracked: write.untracked,
                data: write.data,
            },
        )
        .collect::<Vec<_>>();
    crate::binary_cas::support::persist_resolved_binary_blob_writes_in_transaction(
        transaction,
        &resolved,
    )
    .await
}

pub(crate) async fn garbage_collect_unreachable_binary_cas(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    crate::binary_cas::support::garbage_collect_unreachable_binary_cas_in_transaction(transaction)
        .await
}

pub(crate) async fn persist_runtime_sequence(
    transaction: &mut dyn LixBackendTransaction,
    functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
) -> Result<(), LixError> {
    crate::runtime::deterministic_mode::persist_runtime_sequence_in_transaction(
        transaction,
        functions,
    )
    .await
}

pub(crate) async fn execute_public_tracked_append(
    transaction: &mut dyn LixBackendTransaction,
    unit: &TrackedTxnUnit,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<TrackedCommitExecutionOutcome, LixError> {
    let writer_key_updates = tracked_writer_key_updates_for_unit(unit);
    if unit
        .execution
        .change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !unit.has_compiler_only_filesystem_changes()
        && writer_key_updates.is_empty()
    {
        return Ok(TrackedCommitExecutionOutcome::default());
    }

    if unit.execution.change_batch.is_none()
        && !unit.has_compiler_only_filesystem_changes()
        && !writer_key_updates.is_empty()
    {
        let live_rows =
            tracked_live_rows_for_writer_key_updates(transaction, &writer_key_updates).await?;
        if !live_rows.is_empty() {
            crate::live_state::write_live_rows(transaction, &live_rows).await?;
        }
        return Ok(TrackedCommitExecutionOutcome::default());
    }

    let mut create_commit_functions = unit.runtime_state.functions().clone();
    let canonical_preconditions = canonical_create_commit_preconditions_for_tracked_unit(unit)?;

    if pending_commit_session
        .as_ref()
        .is_some_and(|slot| slot.as_ref().is_some())
        && !unit.has_compiler_only_filesystem_changes()
    {
        crate::runtime::deterministic_mode::ensure_runtime_sequence_initialized_in_transaction(
            transaction,
            &mut create_commit_functions,
        )
        .await?;
    }

    let mut invariant_checker = PublicCommitInvariantChecker::new(&unit.public_write);
    let invariant_checker = if unit.is_merged_transaction_plan() {
        None
    } else {
        Some(&mut invariant_checker as &mut dyn CreateCommitInvariantChecker)
    };

    let append_outcome = append_tracked_with_pending_public_session(
        transaction,
        BufferedTrackedAppendArgs {
            timestamp: None,
            changes: unit
                .execution
                .change_batch
                .as_ref()
                .map(|batch| public_changes_to_staged(&batch.changes))
                .transpose()?
                .unwrap_or_default(),
            filesystem_state: unit.filesystem_state.clone(),
            preconditions: canonical_preconditions,
            active_account_ids: unit.public_write.contract.active_account_ids.clone(),
            writer_key: unit.writer_key.clone(),
            should_emit_observe_tick: unit.should_emit_observe_tick(),
        },
        &mut create_commit_functions,
        invariant_checker,
        pending_commit_session.as_deref_mut(),
        !unit.has_compiler_only_filesystem_changes(),
    )
    .await?;

    if !writer_key_updates.is_empty() {
        let live_rows =
            tracked_live_rows_for_writer_key_updates(transaction, &writer_key_updates).await?;
        if !live_rows.is_empty() {
            crate::live_state::write_live_rows(transaction, &live_rows).await?;
        }
    }

    if append_outcome.merged_into_pending_session
        && create_commit_functions
            .deterministic_sequence_persist_highest_seen()
            .is_some()
    {
        crate::runtime::deterministic_mode::persist_runtime_sequence_in_transaction(
            transaction,
            &create_commit_functions,
        )
        .await?;
    }

    if let Some(applied_output) = append_outcome.applied_output.as_ref() {
        mirror_public_registered_schema_bootstrap_rows(transaction, applied_output).await?;
    }

    let applied_changes = public_changes_from_staged(&append_outcome.applied_changes);
    let plugin_changes_committed =
        matches!(append_outcome.disposition, CreateCommitDisposition::Applied);

    if plugin_changes_committed {
        crate::session::checkpoint_ops::cache::apply_public_version_last_checkpoint_side_effects(
            transaction,
            &unit.public_write,
            &ChangeBatch {
                changes: applied_changes.clone(),
                write_lane: unit
                    .execution
                    .change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &unit.execution.create_preconditions.write_lane {
                        WriteLane::SingleVersion(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        WriteLane::ActiveVersion => WriteLane::ActiveVersion,
                        WriteLane::GlobalAdmin => WriteLane::GlobalAdmin,
                    }),
                writer_key: unit
                    .execution
                    .change_batch
                    .as_ref()
                    .and_then(|batch| batch.writer_key.clone())
                    .or_else(|| unit.public_write.contract.writer_key.clone()),
                semantic_effects: Vec::new(),
            },
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public tracked write version checkpoint side effects failed: {}",
                error.description
            ),
        })?;
    }

    Ok(TrackedCommitExecutionOutcome {
        receipt: append_outcome.receipt,
        next_active_version_id: next_active_version_id_from_changes(&applied_changes)?,
        applied_changes,
        plugin_changes_committed,
    })
}

async fn mirror_public_registered_schema_bootstrap_rows(
    transaction: &mut dyn LixBackendTransaction,
    applied_output: &CreateCommitAppliedOutput,
) -> Result<(), LixError> {
    for row in &applied_output.canonical_changes {
        if row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }

        let snapshot_sql = row
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = row
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             is_tombstone = excluded.is_tombstone, \
             updated_at = excluded.updated_at",
            table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
            entity_id = escape_sql_string(&row.entity_id),
            schema_key = escape_sql_string(&row.schema_key),
            schema_version = escape_sql_string(&row.schema_version),
            file_id = escape_sql_string(&row.file_id),
            version_id = escape_sql_string(GLOBAL_VERSION_ID),
            plugin_key = escape_sql_string(&row.plugin_key),
            snapshot_content = snapshot_sql,
            change_id = escape_sql_string(&row.id),
            metadata = metadata_sql,
            is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 },
            created_at = escape_sql_string(&row.created_at),
            updated_at = escape_sql_string(&row.created_at),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok(())
}

fn canonical_create_commit_preconditions_for_tracked_unit(
    unit: &TrackedTxnUnit,
) -> Result<CreateCommitPreconditions, LixError> {
    canonical_create_commit_preconditions_from_public_write(
        &unit.execution.create_preconditions,
        unit.execution.change_batch.as_ref(),
        &unit.public_write,
    )
}

fn canonical_create_commit_preconditions_from_public_write(
    commit_preconditions: &CommitPreconditions,
    batch: Option<&ChangeBatch>,
    public_write: &PreparedPublicWriteArtifact,
) -> Result<CreateCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        WriteLane::SingleVersion(version_id) => CreateCommitWriteLane::Version(version_id.clone()),
        WriteLane::ActiveVersion => {
            let version_id = batch
                .into_iter()
                .flat_map(|batch| batch.changes.first())
                .map(|change| change.version_id.clone())
                .next()
                .or_else(|| public_write.contract.requested_version_id.clone())
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "public commit execution requires a concrete active version id",
                    )
                })?;
            CreateCommitWriteLane::Version(version_id)
        }
        WriteLane::GlobalAdmin => CreateCommitWriteLane::GlobalAdmin,
    };
    let expected_head = match &commit_preconditions.expected_head {
        ExpectedHead::CurrentHead => CreateCommitExpectedHead::CurrentHead,
    };

    Ok(CreateCommitPreconditions {
        write_lane,
        expected_head,
        idempotency_key: match &commit_preconditions.expected_head {
            ExpectedHead::CurrentHead => CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                commit_preconditions.idempotency_key.0.clone(),
            ),
        },
    })
}

fn public_changes_to_staged(changes: &[PublicChange]) -> Result<Vec<StagedChange>, LixError> {
    changes.iter().map(public_change_to_staged).collect()
}

fn tracked_writer_key_updates_for_unit(
    unit: &TrackedTxnUnit,
) -> BTreeMap<RowIdentity, Option<String>> {
    let mut updates = BTreeMap::new();
    for public_write in &unit.public_writes {
        let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
            continue;
        };
        for partition in &resolved.partitions {
            if partition.execution_mode != crate::contracts::WriteMode::Tracked {
                continue;
            }
            updates.extend(partition.writer_key_updates.iter().map(
                |(row_identity, writer_key)| {
                    (
                        RowIdentity {
                            schema_key: row_identity.schema_key.clone(),
                            version_id: row_identity.version_id.clone(),
                            entity_id: row_identity.entity_id.clone(),
                            file_id: row_identity.file_id.clone(),
                        },
                        writer_key.clone(),
                    )
                },
            ));
        }
    }
    updates
}

async fn tracked_live_rows_for_writer_key_updates(
    transaction: &mut dyn LixBackendTransaction,
    updates: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<Vec<crate::live_state::LiveRow>, LixError> {
    let backend = TransactionBackendAdapter::new(transaction);
    let mut rows = Vec::with_capacity(updates.len());
    for (row_identity, writer_key) in updates {
        let row = crate::live_state::load_exact_live_row(
            &backend,
            &crate::live_state::ExactLiveRowQuery {
                semantics: crate::live_state::LiveRowSemantics::Tracked,
                schema_key: row_identity.schema_key.clone(),
                version_id: row_identity.version_id.clone(),
                entity_id: row_identity.entity_id.clone(),
                file_id: Some(row_identity.file_id.clone()),
                schema_version: None,
                plugin_key: None,
                writer_key: None,
                global: None,
                untracked: Some(false),
                include_tombstones: true,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "writer_key-only live-state update requires tracked row '{}:{}'",
                    row_identity.schema_key, row_identity.entity_id
                ),
            )
        })?;
        rows.push(crate::live_state::LiveRow {
            writer_key: writer_key.clone(),
            ..row
        });
    }
    Ok(rows)
}

fn public_change_to_staged(change: &PublicChange) -> Result<StagedChange, LixError> {
    Ok(StagedChange {
        id: None,
        entity_id: EntityId::new(change.entity_id.clone())?,
        schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
        schema_version: change
            .schema_version
            .clone()
            .map(CanonicalSchemaVersion::new)
            .transpose()?,
        file_id: change.file_id.clone().map(FileId::new).transpose()?,
        plugin_key: change
            .plugin_key
            .clone()
            .map(CanonicalPluginKey::new)
            .transpose()?,
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: VersionId::new(change.version_id.clone())?,
        writer_key: change.writer_key.clone(),
        created_at: None,
    })
}

fn public_changes_from_staged(changes: &[StagedChange]) -> Vec<PublicChange> {
    changes
        .iter()
        .map(|change| PublicChange {
            entity_id: change.entity_id.to_string(),
            schema_key: change.schema_key.to_string(),
            schema_version: change.schema_version.as_ref().map(ToString::to_string),
            file_id: change.file_id.as_ref().map(ToString::to_string),
            plugin_key: change.plugin_key.as_ref().map(ToString::to_string),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            version_id: change.version_id.to_string(),
            writer_key: change.writer_key.clone(),
        })
        .collect()
}

fn next_active_version_id_from_changes<Change: TrackedChangeView>(
    changes: &[Change],
) -> Result<Option<String>, LixError> {
    for change in changes.iter().rev() {
        if change.schema_key() != ACTIVE_VERSION_SCHEMA_KEY
            || change.file_id() != Some(ACTIVE_VERSION_FILE_ID)
            || change.version_id() != GLOBAL_VERSION_ID
        {
            continue;
        }

        let Some(snapshot_content) = change.snapshot_content() else {
            continue;
        };
        return parse_active_version_snapshot(snapshot_content).map(Some);
    }

    Ok(None)
}
