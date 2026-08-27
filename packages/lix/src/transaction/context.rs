#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    // `WasmRowKey` contains a JSONB decode cache that cannot affect ordering.
    clippy::mutable_key_type,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::cell::Cell;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BinaryCasContext, BlobBytesBatch, BlobDataReader, BlobId};
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchHeadControlContext, BranchLifecycle,
    BranchOperation, BranchRefReader, BranchReferenceRole, branch_ref_stage_row,
};
use crate::catalog::{
    CatalogContext, CatalogFingerprint, CatalogRevision, CatalogSnapshot, SchemaPlanId,
    load_catalog_revision, stage_catalog_revision,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, ChangelogReader, CommitId, CommitLoadRequest,
    load_change_records, materialize_known_change_payloads,
};
use crate::checkpoint::{
    CHECKPOINT_SCHEMA_KEY, checkpoint_commit_id_at_head, checkpoint_stage_row,
};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::Domain;
use crate::filesystem::{
    BlobRefRowInput, FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemPathKind, FilesystemRowContext, load_path_index_revision,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::gc::{
    CheckpointGcState, CheckpointPublication, CheckpointRecoveryRef, load_checkpoint_gc_state,
    load_recovery_ref,
};
#[cfg(test)]
use crate::hot_state::HotStateRowRequest;
use crate::hot_state::{
    BranchHeadControlCache, CertifiedCurrentStatePredecessor, HotStateContext,
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateProjection,
    HotStateReader, HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateExactBatch,
    MaterializedHotStateRow, MaterializedHotStateRowRef, StagedHotStateRows, TrackedHeadContext,
    overlay_load_exact_batch, overlay_scan_batch,
};
use crate::plugin::runtime::{
    ArcByteSource, BoundCreateContext, CompiledPluginCatalog, ConflictRank, FileBytesSha256,
    LiveBatchRowSource, OuterRowJsonOperation, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY,
    PluginActorCache, PluginActorColdInstall, PluginActorColdOpen, PluginActorKey,
    PluginActorLease, PluginActorStagedCheckpoint, PluginActorStore, PluginActorStorePermit,
    PluginArchiveInstallPlan, PluginContentMatcher, PluginFileOwner, PluginObservation,
    PluginRegistry, PluginRegistryEntry, PluginRegistryEntryInput, PluginRowAuthorities,
    PluginRowAuthorityRange, PluginRuntimeHost, RowVersionRef, SchemaAllowlist,
    TypedColumnMergeResult as HostTypedColumnMergeResult, TypedRowVersionRef,
    ValidatedFileTransition, ValidatedSameLengthOutputSplice, VecRowChangeSource, VecRowSource,
    WasmCreateContext, build_file_update_splices, drain_file_transition_changes,
    drain_row_transition_edits, is_plugin_storage_path, is_reservation_key,
    load_plugin_registry_at_commit, local_mutation_identity, materialize_keyless_creates,
    plugin_archive_file_id_matches, plugin_install_plan_from_archive_path,
    plugin_key_from_archive_delete_origin, plugin_state_hot_state_projection, reconcile_row,
    reconcile_typed_row, require_existing_id_authorities, reservation_tombstone_row,
    reserve_create_row, transport_splice_preserves_prefix_exclusion,
    transport_splice_preserves_utf8, validate_create_changes, validate_create_reservation,
};
use crate::row_pk::RowPk;
use crate::session::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotency, ExecuteIdempotencyReceipt,
    SessionBranch, encode_receipt,
};
use crate::sql2::{
    ChangelogQuerySource, DiffCommand, DiffCommandSelection, HistoryQuerySource,
    SessionFileViewKey, SessionFileViewMutation, SessionFileViews, SessionPluginFileView,
    SqlChangelogQuerySource, SqlExecutionContext, SqlHistoryQuerySource,
};
use crate::sql2::{SqlPlanningCache, SqlWriteExecutionContext};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    Memory, StoragePrecondition, StorageReadOptions, StorageWriteOptions, StorageWriteSet,
    StorageWriteSetStats,
};
use crate::storage_adapter::{
    REVISION_KEY_CATALOG, REVISION_KEY_TRACKED_MUTATION, SharedStorageAdapterRead, StorageAdapter,
    StorageAdapterRead, StorageAdapterReadScope, load_revisions,
};
use crate::telemetry::{
    ActiveTelemetrySpan, Status, TRANSACTION_MATERIALIZE, TRANSACTION_STORAGE,
    instrument_lix_result,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffEntry, TrackedStateDiffKind, TrackedStateDiffRequest,
    TrackedStateFilter, TrackedStateKey, TrackedStateStoreReader,
};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedRowFacts, REGISTERED_SCHEMA_KEY, normalize_raw_write_row_in_place,
    remember_pending_registered_schema,
};
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staged_commit_changes::{
    StagedCommitChangeBatch, StagedCommitChangeBatchBuilder,
};
use crate::transaction::staging::{
    ImmutableMutationChunkStage, ImmutableMutationJournalChunk, MUTATION_JOURNAL_CHUNK_MAX_ROWS,
    PreparedStateRowOverlay, PreparedWriteSet, TransactionWriteBuffer,
    TransactionWriteBufferCheckpoint,
};
use crate::transaction::stale_commit::{
    StaleCommitPlan, StaleRowReconciliationPlan, classify_stale_commit,
};
use crate::transaction_types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch, PreparedRowFacts,
    PreparedStateBatch, PreparedTransactionWrite, RawWriteBatch, RawWriteRowRef, StagedIndexValues,
    TransactionFileContent, TransactionJson, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin, TransactionWriteOutcome,
    TransactionWriteRow, TypedMutationJournalBatch, canonicalize_transaction_json_batch,
    stage_json_from_value,
};

use crate::plugin::runtime::{
    WASM_COMPONENT_API_VERSION, WasmChangeEffect, WasmColdFileUpdate, WasmColumnMergeResult,
    WasmComponentActor, WasmComponentFactory, WasmDocumentCheckpoint, WasmDocumentHandle,
    WasmFileDescriptor, WasmFileUpdate, WasmHostBytes, WasmHostColumnMerge, WasmHostRow,
    WasmHostRowChanges, WasmOpenFileInput, WasmOpenRowsInput, WasmPluginSelection, WasmRow,
    WasmRowChange, WasmRowKey, WasmRowUpdate, WasmTransitionCounters, WasmTransitionLimits,
    WasmTypedRow,
};
use crate::telemetry::TelemetryAttribute;
use crate::transaction::validation::{
    TransactionValidationInput, fresh_plugin_file_import_certificate,
    prepared_tracked_rows_have_row_local_certificates, validate_certified_fresh_plugin_file_import,
    validate_certified_tracked_insert_identities, validate_prepared_writes,
};
use crate::{LixError, NullableKeyFilter, SqlQueryResult, Value};

mod cohort;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome {
    pub(crate) storage_stats: StorageWriteSetStats,
    pub(crate) commit_cohort_id: Option<String>,
}

fn typed_transaction_validation_counters(rows: &RawWriteBatch) -> WasmTransitionCounters {
    let mut counters = WasmTransitionCounters::default();
    for row in rows.iter() {
        let Some(typed) = row.decoded_snapshot() else {
            continue;
        };
        // Count every native row observed by transaction preparation. Rows
        // certified at the component or SQL construction boundary avoid
        // duplicate validation work, but they are still the dynamic proof
        // that this transaction never reconstructed an outer JSON snapshot.
        counters.typed_transaction_validation_calls = counters
            .typed_transaction_validation_calls
            .saturating_add(1);
        counters.typed_transaction_validation_bytes = counters
            .typed_transaction_validation_bytes
            .saturating_add(typed.estimated_size());
    }
    counters
}

#[cfg(test)]
#[test]
fn typed_transaction_counters_include_boundary_certified_rows() {
    let typed = Arc::new(WasmTypedRow {
        schema_fingerprint: [7; 32],
        row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
        row: lix_schema::Row::from([("id", lix_schema::Value::Text("row-1".to_owned()))]),
        native_payload: std::sync::OnceLock::new(),
        boundary_create_validation: std::sync::OnceLock::new(),
    });
    typed
        .certify_boundary_validation()
        .expect("test row should encode and certify");
    assert!(typed.boundary_validation_certified());

    let mut rows = RawWriteBatch::new();
    rows.push_typed_parts(
        Some(RowPk::single("row-1")),
        "typed_counter_probe".into(),
        None,
        Some(typed),
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        false,
        "main".into(),
    );

    let counters = typed_transaction_validation_counters(&rows);
    assert_eq!(counters.typed_transaction_validation_calls, 1);
    assert!(counters.typed_transaction_validation_bytes > 0);
}

/// Commits one coordinator-owned cohort in queue order.
///
/// The coordinator is the sole explicit-transaction persistence authority.
/// This initial execution seam deliberately lives beside `Transaction` so the
/// cohort planner can replace per-transaction execution without reintroducing
/// a type-erased closure queue.
pub(crate) async fn commit_transaction_cohort<StorageImpl>(
    cohort: Vec<(Transaction<StorageImpl>, FunctionContext)>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(cohort::commit_transaction_cohort(cohort)).await
}

pub(crate) fn transactions_can_share_cohort<StorageImpl>(
    a: &Transaction<StorageImpl>,
    b: &Transaction<StorageImpl>,
    eligible_a: bool,
    eligible_b: bool,
) -> bool
where
    StorageImpl: Storage + 'static,
{
    a.active_branch_id == b.active_branch_id
        && a.opening_active_branch_head == b.opening_active_branch_head
        && a.opening_global_branch_head == b.opening_global_branch_head
        && a.opening_tracked_mutation_revision == b.opening_tracked_mutation_revision
        && a.idempotency_receipt.is_none()
        && b.idempotency_receipt.is_none()
        && a.atomic_metadata_writes.is_none()
        && b.atomic_metadata_writes.is_none()
        && a.atomic_metadata_preconditions.is_empty()
        && b.atomic_metadata_preconditions.is_empty()
        && a.pending_branch_checkpoint_replacements.is_empty()
        && b.pending_branch_checkpoint_replacements.is_empty()
        && a.pending_restore_targets.is_empty()
        && b.pending_restore_targets.is_empty()
        && !a.await_durable_commit
        && !b.await_durable_commit
        && eligible_a
        && eligible_b
}

pub(crate) fn transaction_is_file_cohort_eligible<StorageImpl>(
    transaction: &Transaction<StorageImpl>,
) -> bool
where
    StorageImpl: Storage + 'static,
{
    transaction
        .staged_writes
        .is_file_cohort_eligible(&transaction.active_branch_id)
}

#[derive(Clone, PartialEq)]
struct StaleConflictPayload {
    snapshot: Option<SharedStr>,
    decoded_snapshot: Option<Arc<WasmTypedRow>>,
    metadata: Option<SharedStr>,
}

struct DecodedStalePayload {
    snapshot: JsonValue,
    metadata: Option<JsonValue>,
}

fn decode_stale_payload(
    payload: Option<&StaleConflictPayload>,
) -> Result<Option<DecodedStalePayload>, LixError> {
    payload
        .map(|payload| {
            let snapshot = match (&payload.decoded_snapshot, &payload.snapshot) {
                (Some(snapshot), _) => snapshot.to_json_value()?,
                (None, Some(snapshot)) => {
                    serde_json::from_str(snapshot.as_str()).map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("tracked row snapshot is invalid JSON: {error}"),
                        )
                    })?
                }
                (None, None) => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "generic stale row must have exactly one live payload",
                    ));
                }
            };
            Ok(DecodedStalePayload {
                snapshot,
                metadata: payload
                    .metadata
                    .as_ref()
                    .map(|metadata| serde_json::from_str(metadata.as_str()))
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("tracked row metadata is invalid JSON: {error}"),
                        )
                    })?,
            })
        })
        .transpose()
}

fn row_version_ref(payload: Option<&DecodedStalePayload>) -> Option<RowVersionRef<'_>> {
    payload.map(|payload| RowVersionRef {
        snapshot: &payload.snapshot,
        metadata: payload.metadata.as_ref(),
    })
}

fn typed_row_version_ref(
    payload: Option<&StaleConflictPayload>,
) -> Result<Option<TypedRowVersionRef<'_>>, LixError> {
    payload
        .map(|payload| {
            let snapshot = payload.decoded_snapshot.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin-owned stale row must have exactly one native typed snapshot",
                )
            })?;
            Ok(TypedRowVersionRef {
                snapshot,
                metadata: payload.metadata.as_ref(),
            })
        })
        .transpose()
}

struct StaleColumnMergeInput {
    key: TrackedStateKey,
    base: Option<StaleConflictPayload>,
    a: Option<StaleConflictPayload>,
    b: Option<StaleConflictPayload>,
    primary_key_columns: BTreeSet<String>,
    typed: bool,
    plugin: Option<PluginRegistryEntry>,
}

struct StaleColumnMergeGroup {
    plugin: PluginRegistryEntry,
    merges: Vec<WasmHostColumnMerge>,
    destinations: Vec<(usize, String)>,
}

fn common_registry_column_merger(
    schema_key: &str,
    opening: &PluginRegistry,
    current: &PluginRegistry,
) -> Result<Option<PluginRegistryEntry>, LixError> {
    fn merger<'a>(
        schema_key: &str,
        registry: &'a PluginRegistry,
    ) -> Option<&'a PluginRegistryEntry> {
        registry.plugins().iter().find(|plugin| {
            plugin.has_column_merger()
                && plugin
                    .schema_keys()
                    .binary_search_by(|key| key.as_str().cmp(schema_key))
                    .is_ok()
        })
    }
    match (merger(schema_key, opening), merger(schema_key, current)) {
        (None, None) => Ok(None),
        (Some(opening), Some(current)) if opening == current => Ok(Some(current.clone())),
        _ => Err(LixError::new(
            LixError::CODE_MERGE_CONFLICT,
            format!(
                "column merger generation for schema '{schema_key}' changed during the transaction"
            ),
        )
        .with_hint("commit the plugin generation change before retrying the row edit")),
    }
}

fn encoded_stale_payload(
    row: crate::plugin::runtime::ReconciledRow,
) -> Result<StaleConflictPayload, LixError> {
    Ok(StaleConflictPayload {
        snapshot: Some(
            serde_json::to_string(&row.snapshot)
                .map(SharedStr::from)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("failed to encode reconciled row: {error}"),
                    )
                })?,
        ),
        decoded_snapshot: None,
        metadata: row
            .metadata
            .map(|metadata| serde_json::to_string(&metadata).map(SharedStr::from))
            .transpose()
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to encode reconciled row metadata: {error}"),
                )
            })?,
    })
}

fn encoded_typed_stale_payload(
    row: crate::plugin::runtime::ReconciledTypedRow,
) -> StaleConflictPayload {
    StaleConflictPayload {
        snapshot: None,
        decoded_snapshot: Some(Arc::new(row.snapshot)),
        metadata: row.metadata,
    }
}

fn push_prepared_row_as_raw(
    rows: &mut RawWriteBatch,
    row: crate::transaction_types::PreparedStateRowRef<'_>,
) -> Result<(), LixError> {
    let metadata = row
        .metadata
        .map(|metadata| {
            TransactionJson::from_value(metadata.as_value().clone(), "prepared metadata")
        })
        .transpose()?;
    let common = (
        Some(row.row_pk.clone()),
        row.schema_key.clone(),
        row.file_id.cloned(),
        metadata,
        row.origin.cloned(),
        Some(row.created_at.to_string().into()),
        Some(row.updated_at.to_string().into()),
        row.global,
        row.change_id.map(|change_id| change_id.to_string().into()),
        None,
        row.untracked,
        row.branch_id.clone(),
    );
    if let Some(decoded_snapshot) = row.materialize_decoded_snapshot()? {
        rows.push_typed_parts(
            common.0,
            common.1,
            common.2,
            Some(decoded_snapshot),
            common.3,
            common.4,
            common.5,
            common.6,
            common.7,
            common.8,
            common.9,
            common.10,
            common.11,
        );
    } else {
        rows.push_parts(
            common.0, common.1, common.2, None, common.3, common.4, common.5, common.6, common.7,
            common.8, common.9, common.10, common.11,
        );
    }
    Ok(())
}

fn stale_payload_from_tracked(
    row: Option<crate::tracked_state::MaterializedTrackedStateRowRef<'_>>,
) -> Option<StaleConflictPayload> {
    row.filter(|row| !row.deleted()).and_then(|row| {
        if row.snapshot_content().is_none() && row.decoded_snapshot().is_none() {
            return None;
        }
        Some(StaleConflictPayload {
            snapshot: row.snapshot_content().cloned(),
            decoded_snapshot: row.decoded_snapshot().cloned(),
            metadata: row.metadata().cloned(),
        })
    })
}

/// The durable identity and blob reference of one plugin materialization.
#[derive(Debug, Clone)]
struct VisibleMaterialization {
    semantic_root: String,
    bytes: VisibleMaterializationBytes,
}

#[derive(Debug, Clone)]
enum VisibleMaterializationBytes {
    Blob { hash: BlobId },
}

#[cfg(test)]
fn decode_visible_materialization(
    row: &MaterializedHotStateRow,
    file_id: &str,
) -> Result<VisibleMaterialization, LixError> {
    decode_visible_materialization_parts(
        row.schema_key.as_str(),
        row.change_id,
        row.snapshot_content.as_deref(),
        file_id,
    )
}

fn decode_visible_materialization_ref(
    row: MaterializedHotStateRowRef<'_>,
    file_id: &str,
) -> Result<VisibleMaterialization, LixError> {
    decode_visible_materialization_parts(
        row.schema_key(),
        row.change_id(),
        row.snapshot_content().map(|content| content.as_str()),
        file_id,
    )
}

fn decode_visible_materialization_parts(
    schema_key: &str,
    change_id: Option<ChangeId>,
    snapshot_content: Option<&str>,
    file_id: &str,
) -> Result<VisibleMaterialization, LixError> {
    let semantic_root = change_id.map(|root| root.to_string()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("component materialization root for file '{file_id}' is missing change_id"),
        )
    })?;
    let snapshot = snapshot_content.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "owned component plugin file '{file_id}' materialization is missing its durable proof"
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
                            "owned component plugin file '{file_id}' has an invalid blob reference: {error}"
                        ),
                    )
                })?;
            if snapshot.id != file_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "owned component plugin file '{file_id}' materialization identity does not match its file scope"
                    ),
                ));
            }
            VisibleMaterializationBytes::Blob {
                hash: BlobId::from_hex(&snapshot.blob_hash)?,
            }
        }
        schema_key => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned component plugin file '{file_id}' materialization uses unsupported schema '{schema_key}'"
                ),
            ));
        }
    };
    Ok(VisibleMaterialization {
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
thread_local! {
    static TRANSACTION_PATH_INDEX_BUILD_STATS: Cell<TransactionPathIndexBuildStats> =
        const { Cell::new(TransactionPathIndexBuildStats {
            builds: 0,
            descriptor_rows: 0,
        }) };
}

#[cfg(test)]
fn reset_transaction_path_index_build_stats() {
    TRANSACTION_PATH_INDEX_BUILD_STATS.set(TransactionPathIndexBuildStats::default());
}

#[cfg(test)]
fn transaction_path_index_build_stats() -> TransactionPathIndexBuildStats {
    TRANSACTION_PATH_INDEX_BUILD_STATS.get()
}

#[cfg(test)]
fn record_transaction_path_index_build(descriptor_rows: usize) {
    let stats = TRANSACTION_PATH_INDEX_BUILD_STATS.get();
    TRANSACTION_PATH_INDEX_BUILD_STATS.set(TransactionPathIndexBuildStats {
        builds: stats.builds + 1,
        descriptor_rows: stats.descriptor_rows + descriptor_rows,
    });
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
/// Retires the write-context revocation token so any `SqlWriteContext` that
/// outlives this transaction fails deterministically instead of dereferencing
/// freed memory. See `sql2::WriteContextLiveness`.
impl<StorageImpl: Storage + 'static> Drop for Transaction<StorageImpl> {
    fn drop(&mut self) {
        self.write_context_liveness.retire();
    }
}

pub(crate) struct Transaction<StorageImpl: Storage + 'static = Memory> {
    write_context_liveness: crate::sql2::WriteContextLiveness,
    active_branch_id: String,
    active_account_id: String,
    hot_state: Arc<HotStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    schema_resolver: TransactionSchemaResolver,
    /// SQL binding is snapshot-isolated at transaction open. Schema writes
    /// staged later in this transaction affect validation but become visible
    /// to SQL planning only after commit opens a new transaction snapshot.
    sql_schema_snapshot: Arc<CatalogSnapshot>,
    /// Narrow catalog used to certify tracked writes without admitting schema
    /// definitions that exist only in the untracked durability lane.
    tracked_schema_snapshot: Arc<CatalogSnapshot>,
    /// Plugin ownership pinned with the transaction's opening branch head.
    opening_plugin_registry: PluginRegistry,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    prepared_mutation_program: Option<(
        Arc<str>,
        Arc<crate::sql2::PreparedPathValueReplacementProgram>,
    )>,
    prepared_mutation_membership: PreparedMutationMembership,
    /// Once proven, a homogeneous prepared generation can avoid locking the
    /// generic row overlay for every independent scalar mutation. Changing
    /// the prepared program invalidates this transaction-local proof.
    prepared_mutation_overlay_empty: bool,
    /// One logical timestamp for a homogeneous columnar mutation generation.
    /// Immutable replacement parts require their post-image rows to share the
    /// same lifecycle boundary, so sealing must not preserve per-call clocks.
    prepared_mutation_timestamp: Option<LixTimestamp>,
    mutation_journal: Option<TransactionMutationJournal>,
    mutation_journal_compressor: Option<crate::compression::ZstdLevel1Compressor>,
    mutation_journal_sealed_rows: usize,
    /// Eagerly sealed parts must remain one canonical prefix. Once a flush
    /// leaves an unsealed tail or exceeds the bounded prefix, later chunks
    /// must stay unsealed so commit can encode one contiguous suffix.
    mutation_journal_seal_prefix_open: bool,
    /// Sealing consumes the journal's owned column buffers. If any fallible
    /// validation or staging step rejects those buffers, this transaction can
    /// no longer provide statement atomicity and must remain rollback-only.
    mutation_journal_terminal_error: Option<LixError>,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    branch_head_control_cache: Arc<BranchHeadControlCache>,
    /// Coherent storage snapshot retained for explicit transaction reads.
    /// This field is declared before `storage` so it is dropped first.
    opening_read: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
    storage: Arc<StorageAdapter<StorageImpl>>,
    functions: FunctionProviderHandle,
    /// PostgreSQL `CURRENT_TIMESTAMP`, fixed at this implicit transaction's start.
    current_timestamp: Option<LixTimestamp>,
    /// Tracked-state revision observed by the coherent transaction-open read.
    /// Durable tracked publication must still be based on this revision;
    /// untracked current-state writes do not invalidate the tracked snapshot.
    opening_tracked_mutation_revision: Option<Bytes>,
    /// Branch roots captured by the same coherent opening read. They let an
    /// explicit transaction distinguish a disjoint concurrent commit from an
    /// overlapping semantic write without creating a temporary branch.
    opening_active_branch_head: Option<CommitId>,
    opening_global_branch_head: Option<CommitId>,
    commit_boundary: Option<TransactionCommitBoundary>,
    trust_filesystem_planner: bool,
    origin_key: Option<SharedStr>,
    idempotency_receipt: Option<(crate::storage_adapter::StorageKey, Vec<u8>)>,
    /// Storage-native metadata that must publish in the same backend commit as
    /// this transaction's file rows and history. Resumable media finalization
    /// uses this lane for its completed manifest and upload receipt.
    atomic_metadata_writes: Option<StorageWriteSet>,
    atomic_metadata_preconditions: Vec<StoragePrecondition>,
    suppress_ordinary_sync_event: bool,
    sync_role: crate::sync::SyncRole,
    await_durable_commit: bool,
    session_file_views: SessionFileViews,
    pending_file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    pending_plugin_actor_publications: Vec<PendingPluginActorPublication>,
    /// Explicit historical branch sources whose branchability may be owned by
    /// one still-pending authenticated checkpoint replacement. Resolution is
    /// delayed to the coherent commit-boundary read so its queue observation
    /// is fenced by the same branch publication batch.
    pending_branch_checkpoint_replacements: BTreeMap<String, CommitId>,
    /// Existing branches that are being restored to an ancestor. Restore is
    /// a ref move, but unlike a generic repoint it must carry branch-local
    /// untracked rows forward and reset the private working-diff epoch.
    pending_restore_targets: BTreeMap<String, PendingRestoreIntent>,
    plugin_generation_read_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    plugin_generation_upgrade_guard: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PendingRestoreIntent {
    pub(crate) expected_head_commit_id: CommitId,
    pub(crate) target_commit_id: CommitId,
}

struct TransactionMutationJournal {
    program: Arc<crate::sql2::PreparedPathValueReplacementProgram>,
    origin_key: Option<SharedStr>,
    identity_arena: Vec<u8>,
    identity_offsets: Vec<(usize, usize)>,
    snapshot_arena: Vec<u8>,
    snapshot_offsets: Vec<(usize, usize)>,
    timestamp: Option<LixTimestamp>,
}

const INITIAL_MUTATION_JOURNAL_ROWS: usize = 16;
const INITIAL_MUTATION_JOURNAL_ARENA_BYTES: usize = 1_024;
const MUTATION_JOURNAL_EAGER_SEAL_MAX_ROWS: usize = 16 * 1_024;

enum PreparedMutationMembership {
    Unprepared,
    Unavailable,
    Packed(crate::hot_state::PackedIdentityMembership),
}

impl TransactionMutationJournal {
    fn len(&self) -> usize {
        self.identity_offsets.len()
    }

    fn last_identity(&self) -> Option<&str> {
        let &(start, end) = self.identity_offsets.last()?;
        Some(
            std::str::from_utf8(&self.identity_arena[start..end])
                .expect("transaction journal identities are appended from str"),
        )
    }

    fn append_identity(&mut self, identity: &str) {
        let start = self.identity_arena.len();
        self.identity_arena.extend_from_slice(identity.as_bytes());
        self.identity_offsets
            .push((start, self.identity_arena.len()));
    }

    #[cfg(feature = "storage-benches")]
    fn record_row_ownership(&self, identity_bytes: usize, snapshot_bytes: usize) {
        crate::storage_bench::record_crud_ownership(
            crate::storage_bench::CRUD_OWNERSHIP_MUTATION_JOURNAL,
            1,
            identity_bytes,
            snapshot_bytes,
            2,
            0,
            0,
        );
        let total = identity_bytes.saturating_add(snapshot_bytes);
        crate::storage_bench::record_crud_ownership_transfer(
            crate::storage_bench::CRUD_OWNERSHIP_MUTATION_JOURNAL,
            total,
            0,
            total,
            0,
        );
    }
}

/// One already-resolved tracked-state transition. The expected side is
/// certified by the active branch head and its immutable historical root;
/// target payloads come from an exact read of the desired root.
struct TypedStateTransition {
    identity: TrackedStateKey,
    expected_change_id: Option<ChangeId>,
    target: Option<TypedStateTransitionTarget>,
}

struct TypedStateTransitionTarget {
    change_id: ChangeId,
    snapshot_content: Option<SharedStr>,
    decoded_snapshot: Option<Arc<WasmTypedRow>>,
    metadata: Option<SharedStr>,
}

/// State which must be restored when `RETURNING` evaluation fails after a
/// write has been staged in an explicit SQL transaction.
pub(crate) struct SqlStatementCheckpoint {
    staged_writes: TransactionWriteBufferCheckpoint,
    filesystem_path_index_epoch: usize,
    pending_file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    trust_filesystem_planner: bool,
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

fn start_materialize_span(
    transaction_count: usize,
    commit_cohort_id: &str,
) -> Option<ActiveTelemetrySpan> {
    ActiveTelemetrySpan::start_current(
        &TRANSACTION_MATERIALIZE,
        vec![
            TelemetryAttribute::i64(
                "lix.transaction.count",
                i64::try_from(transaction_count).unwrap_or(i64::MAX),
            ),
            TelemetryAttribute::string("lix.commit_cohort_id", commit_cohort_id),
        ],
    )
}

impl<StorageImpl> Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn stage_atomic_cas_publication(
        &mut self,
        writes: StorageWriteSet,
        preconditions: Vec<StoragePrecondition>,
        blob_id: BlobId,
    ) -> Result<(), LixError> {
        if self.atomic_metadata_writes.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "atomic transaction metadata was staged more than once",
            ));
        }
        if !writes.contains_put(
            crate::binary_cas::BINARY_CAS_MANIFEST_SPACE,
            blob_id.as_bytes(),
        ) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "atomic CAS publication is missing its prepared manifest",
            ));
        }
        self.atomic_metadata_writes = Some(writes);
        self.atomic_metadata_preconditions.extend(preconditions);
        self.await_durable_commit = true;
        Ok(())
    }

    fn opening_read(&self) -> SharedStorageAdapterRead<StorageImpl::Read<'static>> {
        self.opening_read.clone()
    }

    /// Stages an empty local commit only when this coherent transaction opened
    /// on a head whose pinned global base is stale. The caller uses this as a
    /// lazy auto-rebase before serving a live read, avoiding O(branches)
    /// fan-out on every global write.
    pub(crate) async fn stage_base_refresh_if_needed(&mut self) -> Result<bool, LixError> {
        if self.active_branch_id == GLOBAL_BRANCH_ID {
            return Ok(false);
        }
        let (Some(active_head), Some(global_head)) = (
            self.opening_active_branch_head,
            self.opening_global_branch_head,
        ) else {
            return Ok(false);
        };
        let node = CommitGraphContext::new()
            .reader(self.opening_read())
            .load_node(&active_head)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_COMMIT_NOT_FOUND,
                    format!("active branch head '{active_head}' does not exist"),
                )
            })?;
        if node.base_commit_id == Some(global_head) {
            return Ok(false);
        }
        self.staged_writes
            .stage_empty_commit(self.active_branch_id.clone())?;
        Ok(true)
    }

    async fn reconcile_stale_disjoint_writes<S>(
        &mut self,
        read: &S,
        prepared_writes: &mut PreparedWriteSet,
    ) -> Result<(), LixError>
    where
        S: StorageAdapterRead,
    {
        let current_revision =
            StorageAdapter::<StorageImpl>::load_tracked_mutation_revision_from_read(read).await?;
        if current_revision == self.opening_tracked_mutation_revision {
            return Ok(());
        }

        let conflict = |message: &'static str| {
            LixError::new(LixError::CODE_TRANSACTION_CONFLICT, message)
                .with_hint("Retry the transaction against the latest committed state.")
        };
        if prepared_writes.state_rows.iter().any(|row| {
            row.untracked || row.global || row.branch_id.as_str() != self.active_branch_id
        }) || !prepared_writes.extra_commit_parents_by_branch.is_empty()
            || !prepared_writes
                .first_commit_parent_override_by_branch
                .is_empty()
            || !prepared_writes.intermediate_commits.is_empty()
            || !prepared_writes.checkpoint_publications.is_empty()
            || prepared_writes
                .commit_change_refs_by_branch
                .keys()
                .any(|branch_id| branch_id != &self.active_branch_id)
        {
            return Err(conflict(
                "transaction snapshot is stale and contains writes outside the supported semantic reconciliation lane",
            ));
        }

        let branch_reader = self.branch_ctx.ref_reader(read);
        let current_global_head = branch_reader.load_head_commit_id(GLOBAL_BRANCH_ID).await?;
        let current_active_head = branch_reader
            .load_head_commit_id(&self.active_branch_id)
            .await?;
        if self.opening_active_branch_head.is_some() && current_active_head.is_none() {
            return Err(LixError::new(
                LixError::CODE_BRANCH_NOT_FOUND,
                format!("branch '{}' does not exist", self.active_branch_id),
            ));
        }
        if current_global_head != self.opening_global_branch_head {
            return Err(conflict(
                "transaction snapshot is stale because global schemas or plugin state changed after it opened",
            ));
        }
        if current_active_head == self.opening_active_branch_head {
            self.opening_tracked_mutation_revision = current_revision;
            return Ok(());
        }
        let Some(opening_head) = self.opening_active_branch_head else {
            return Err(conflict(
                "transaction snapshot is stale because its branch lifecycle changed after it opened",
            ));
        };
        let Some(current_head) = current_active_head else {
            return Err(LixError::new(
                LixError::CODE_BRANCH_NOT_FOUND,
                format!("branch '{}' does not exist", self.active_branch_id),
            ));
        };

        // A complete-set journal is certified against the coherent opening
        // snapshot. If this branch advanced, expose its identities—with exact
        // current predecessor lifecycle—to the established stale-write
        // classifier and generic reconciliation lane. Unchanged-head commits
        // retain the zero-reconstruction direct path.
        let journal_descriptors = prepared_writes.ordered_mutation_journal_descriptors();
        if !journal_descriptors.is_empty() {
            let base = self
                .hot_state
                .transaction_reader(read, Arc::clone(&self.branch_head_control_cache));
            let mut predecessors_by_commit = BTreeMap::new();
            for descriptor in journal_descriptors {
                let predecessors = load_immutable_mutation_predecessors(
                    &base,
                    &descriptor.schema_key,
                    &descriptor.branch_id,
                    &descriptor.row_pk_chunks,
                )
                .await?;
                predecessors_by_commit.insert(descriptor.commit_id, predecessors);
            }
            prepared_writes.hydrate_and_lower_ordered_mutation_journals(
                predecessors_by_commit,
                &self.functions,
            )?;
        }

        let mut tracked = self.tracked_state.reader(read);
        let opening_head_text = opening_head.to_string();
        let current_head_text = current_head.to_string();
        let generation_write_set = tracked
            .changed_identities_in_first_parent_interval(&opening_head_text, &current_head_text)
            .instrument(tracing::debug_span!(
                target: "lix_transaction",
                "lix.transaction.stale.generation_write_set"
            ))
            .await?;
        let (plan, concurrent_change_count, discovery) = match generation_write_set {
            Some(identities) => {
                let count = identities.len();
                let plan = {
                    let span = tracing::debug_span!(
                        target: "lix_transaction",
                        "lix.transaction.stale.classify",
                        prepared_rows = prepared_writes.state_rows.len(),
                        concurrent_changes = count,
                    );
                    let _entered = span.enter();
                    classify_stale_commit(
                        prepared_writes,
                        identities.iter().map(|identity| identity.as_key_ref()),
                    )
                };
                (plan, count, "generation_write_set")
            }
            None => {
                let concurrent = tracked
                    .diff_commits(
                        &opening_head_text,
                        &current_head_text,
                        &TrackedStateDiffRequest::default(),
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_transaction",
                        "lix.transaction.stale.general_diff"
                    ))
                    .await?;
                let count = concurrent.entries.len();
                let plan = {
                    let span = tracing::debug_span!(
                        target: "lix_transaction",
                        "lix.transaction.stale.classify",
                        prepared_rows = prepared_writes.state_rows.len(),
                        concurrent_changes = count,
                    );
                    let _entered = span.enter();
                    classify_stale_commit(
                        prepared_writes,
                        concurrent
                            .entries
                            .iter()
                            .map(|entry| entry.identity.as_key_ref()),
                    )
                };
                (plan, count, "general_diff")
            }
        };
        tracing::debug!(
            target: "lix_transaction",
            plan = plan.kind(),
            discovery,
            prepared_rows = prepared_writes.state_rows.len(),
            concurrent_changes = concurrent_change_count,
            "classified stale transaction commit"
        );
        match plan {
            StaleCommitPlan::Direct => {}
            StaleCommitPlan::ReconcileRows(plan) => {
                let file_count = plan.file_ids.len();
                let semantic_conflict_count = plan.semantic_conflict_indices.len();
                self.reconcile_stale_rows(read, prepared_writes, plan, opening_head, current_head)
                    .instrument(tracing::debug_span!(
                        target: "lix_transaction",
                        "lix.transaction.stale.reconcile",
                        file_count,
                        semantic_conflict_count,
                    ))
                    .await?;
            }
        }

        self.opening_active_branch_head = Some(current_head);
        self.opening_tracked_mutation_revision = current_revision;
        Ok(())
    }

    async fn reconcile_stale_rows<S>(
        &mut self,
        read: &S,
        prepared_writes: &mut PreparedWriteSet,
        plan: StaleRowReconciliationPlan,
        opening_head: CommitId,
        current_head: CommitId,
    ) -> Result<(), LixError>
    where
        S: StorageAdapterRead,
    {
        let StaleRowReconciliationPlan {
            semantic_conflict_indices: candidate_indices,
            file_ids,
        } = plan;
        let candidate_keys = candidate_indices
            .iter()
            .map(|&index| {
                let row = prepared_writes.state_rows.row(index);
                TrackedStateKey {
                    schema_key: row.schema_key.to_string(),
                    file_id: row.file_id.map(ToString::to_string),
                    row_pk: row.row_pk.clone(),
                }
            })
            .collect::<Vec<_>>();
        let mut tracked = self.tracked_state.reader(read);
        let base_rows = tracked
            .load_projected_batch_at_commit(
                &opening_head.to_string(),
                &candidate_keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let current_rows = tracked
            .load_projected_batch_at_commit(
                &current_head.to_string(),
                &candidate_keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let opening_registry =
            load_plugin_registry_at_commit(&mut tracked, &opening_head.to_string()).await?;
        let current_registry =
            load_plugin_registry_at_commit(&mut tracked, &current_head.to_string()).await?;
        let mut merge_inputs = Vec::with_capacity(candidate_indices.len());
        for (slot, &row_index) in candidate_indices.iter().enumerate() {
            let source = prepared_writes.state_rows.row(row_index);
            let target = current_rows.row(slot);
            let base_payload = stale_payload_from_tracked(base_rows.row(slot));
            let source_payload = source
                .snapshot
                .is_some()
                .then(|| {
                    Ok::<_, LixError>(StaleConflictPayload {
                        snapshot: None,
                        decoded_snapshot: source.materialize_decoded_snapshot()?,
                        metadata: source
                            .metadata
                            .map(|metadata| metadata.to_json_string().map(SharedStr::from))
                            .transpose()
                            .map_err(|error| LixError::unknown(error.to_string()))?,
                    })
                })
                .transpose()?;
            let target_payload = stale_payload_from_tracked(target);
            let source_change_id = source.change_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "staged tracked row is missing change_id during stale reconciliation",
                )
            })?;
            let source_rank = ConflictRank::new(source.updated_at, source_change_id);
            let target_rank =
                target.map(|row| ConflictRank::new(row.updated_at(), row.change_id()));
            let (a_payload, b_payload) = if target_rank.is_some_and(|rank| rank < source_rank) {
                (target_payload.as_ref(), source_payload.as_ref())
            } else {
                (source_payload.as_ref(), target_payload.as_ref())
            };

            let primary_key_columns = self
                .sql_schema_snapshot
                .plan(source.schema_plan_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "stale row reconciliation lost its schema plan",
                    )
                })
                .and_then(crate::plugin::runtime::primary_key_columns)?;
            merge_inputs.push(StaleColumnMergeInput {
                key: candidate_keys[slot].clone(),
                base: base_payload,
                a: a_payload.cloned(),
                b: b_payload.cloned(),
                primary_key_columns,
                typed: current_registry.owns_schema(source.schema_key.as_str()),
                plugin: common_registry_column_merger(
                    source.schema_key.as_str(),
                    &opening_registry,
                    &current_registry,
                )?,
            });
        }
        drop(tracked);

        // Superseded file actors hold Stores that the short-lived merger may
        // need. Retire them before component admission; row-only mergers have
        // no actor publication and pass through this unchanged.
        let (superseded, retained): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.pending_plugin_actor_publications)
                .into_iter()
                .partition(|publication| file_ids.contains(&publication.session_key().file_id));
        self.pending_plugin_actor_publications = retained;
        discard_plugin_actor_publications(superseded).await;
        let merged_payloads = self.merge_stale_column_inputs(&merge_inputs).await?;
        let mut reconciled = RawWriteBatch::with_capacity(candidate_indices.len());
        for (slot, merged_payload) in merged_payloads.iter().enumerate() {
            cohort::push_cohort_payload(
                &mut reconciled,
                &candidate_keys[slot],
                merged_payload.as_ref(),
                merged_payload
                    .as_ref()
                    .and_then(|payload| payload.decoded_snapshot.clone()),
                &self.active_branch_id,
            );
        }

        // File projection needs the complete staged delta for each affected
        // file, not just the overlapping rows. Preserve the other file rows
        // from this transaction in the replay batch.
        let conflict_indices = candidate_indices.iter().copied().collect::<BTreeSet<_>>();
        for (index, row) in prepared_writes.state_rows.iter().enumerate() {
            if conflict_indices.contains(&index)
                || !row
                    .file_id
                    .is_some_and(|file_id| file_ids.contains(file_id.as_str()))
                || !current_registry.owns_schema(row.schema_key.as_str())
            {
                continue;
            }
            push_prepared_row_as_raw(&mut reconciled, row)?;
        }

        self.pending_file_view_mutations
            .retain(|key, _| !file_ids.contains(&key.file_id));
        self.session_file_views
            .apply_mutations(
                file_ids
                    .iter()
                    .map(|file_id| SessionFileViewMutation::Remove {
                        key: SessionFileViewKey::new(&self.active_branch_id, file_id),
                    }),
            );

        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: reconciled,
        })
        .await?;
        let mut replacement = self.staged_writes.drain()?;
        let mut latest_file_content = BTreeMap::new();
        for write in replacement.file_content_writes.drain(..) {
            latest_file_content.insert((write.branch_id.clone(), write.file_id.clone()), write);
        }
        replacement
            .file_content_writes
            .extend(latest_file_content.into_values());
        let commit_id = prepared_writes
            .commit_change_refs_by_branch
            .get(&self.active_branch_id)
            .map(|change_refs| change_refs.commit_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "stale row reconciliation is missing its staged commit identity",
                )
            })?;
        replacement.state_rows.set_commit_id_all(commit_id);
        prepared_writes.replace_reconciled_writes(replacement, &file_ids);
        Ok(())
    }

    async fn resolve_pending_branch_checkpoint_replacements<S>(
        &mut self,
        read: &S,
        prepared_writes: &PreparedWriteSet,
    ) -> Result<BTreeMap<String, CheckpointRecoveryRef>, LixError>
    where
        S: StorageAdapterRead + Clone + Send + Sync,
    {
        let requests = std::mem::take(&mut self.pending_branch_checkpoint_replacements);
        let mut branch_checkpoint_bridges = BTreeMap::new();
        for (branch_id, source_commit_id) in requests {
            let Some(replacement) =
                crate::gc::resolve_pending_checkpoint_replacement(read, source_commit_id).await?
            else {
                continue;
            };
            let checkpoint_commit_id = replacement.checkpoint_commit_id;
            if checkpoint_commit_id == source_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint replacement cannot point to its recovered head",
                ));
            }

            // The queue authenticates the root/control transition. Reading
            // the complete semantic diff on this same snapshot additionally
            // proves both manifests/roots are present and that compaction did
            // not change any public tracked fact.
            let mut tracked = self.tracked_state.reader(read.clone());
            let diff = tracked
                .diff_commits(
                    &source_commit_id.to_string(),
                    &checkpoint_commit_id.to_string(),
                    &TrackedStateDiffRequest::default(),
                )
                .await?;
            if let Some(entry) = diff.entries.iter().find(|entry| {
                entry.identity.schema_key() != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
            }) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "pending checkpoint replacement changes public tracked identity '{}'",
                        entry.identity.schema_key()
                    ),
                ));
            }
            if replacement.checkpoint_branch_id.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "pending checkpoint replacement has no authenticated source branch",
                ));
            }
            if prepared_writes
                .checkpoint_publications
                .iter()
                .any(|publication| publication.recovery_ref.branch_id == branch_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("branch '{branch_id}' already staged checkpoint serving context"),
                ));
            }
            branch_checkpoint_bridges.insert(
                branch_id.clone(),
                CheckpointRecoveryRef {
                    branch_id,
                    recovered_head_commit_id: source_commit_id,
                    checkpoint_commit_id,
                    interval_has_commits: true,
                },
            );
        }
        Ok(branch_checkpoint_bridges)
    }

    async fn attach_checkpoint_branch_parents<S>(
        read: &S,
        prepared_writes: &mut PreparedWriteSet,
        commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    ) -> Result<(), LixError>
    where
        S: StorageAdapterRead + Clone + Send + Sync,
    {
        let branch_ids = prepared_writes
            .commit_change_refs_by_branch
            .keys()
            .filter(|branch_id| {
                !prepared_writes
                    .checkpoint_publications
                    .iter()
                    .any(|publication| publication.recovery_ref.branch_id == branch_id.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .load_many(&branch_ids)
            .await?;
        for (branch_id, control) in branch_ids.into_iter().zip(controls) {
            let Some(control) = control else {
                continue;
            };
            let Some(parent_head) = commit_parent_heads.get(&branch_id).copied().flatten() else {
                continue;
            };
            let Some(checkpoint_parent) = crate::gc::resolve_checkpoint_branch_parent(
                read,
                &branch_id,
                parent_head,
                control.working_diff_checkpoint_commit_id,
            )
            .await?
            else {
                continue;
            };
            let parents = prepared_writes
                .extra_commit_parents_by_branch
                .entry(branch_id)
                .or_default();
            if !parents.contains(&checkpoint_parent) {
                // The compacted checkpoint is the canonical ancestry bridge,
                // so it precedes any merge parent already staged by callers.
                parents.insert(0, checkpoint_parent);
            }
        }
        Ok(())
    }

    /// Opens an execution-scoped staging area for SQL/provider hooks.
    async fn open<T, F>(
        session_branch: &SessionBranch,
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        hot_state: Arc<HotStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        plugin_host: PluginRuntimeHost,
        branch_ctx: Arc<BranchContext>,
        catalog_context: Arc<CatalogContext>,
        sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
        session_file_views: SessionFileViews,
        runtime_boundary: F,
    ) -> Result<(OpenTransaction<StorageImpl>, T), LixError>
    where
        F: for<'runtime> AsyncFnOnce(&'runtime FunctionContext) -> Result<T, LixError>,
    {
        let storage = Arc::new(storage);
        let read = storage.begin_read(StorageReadOptions::default()).await?;
        // SAFETY: `storage` is retained in the transaction behind an `Arc` and
        // `opening_read` is declared before it, so the widened read is always
        // dropped before the storage value that produced it.
        let read = unsafe { assume_static_storage_read::<StorageImpl>(read) };
        let opening_read = SharedStorageAdapterRead::new(read);
        let read = opening_read.clone();
        let setup_result = async {
            let active_branch_id = session_branch.get()?;
            let runtime_functions =
                FunctionContext::prepare(&read, Some(hot_state.global_key_value_rows())).await?;
            let runtime_boundary_result = runtime_boundary(&runtime_functions).await?;
            let functions = runtime_functions.provider();
            // Transaction open needs the catalog revision and the tracked
            // mutation fence from the same pinned snapshot. Both live in the
            // one revision space, so one batched point read over two adjacent
            // keys replaces two independent lookups.
            let [catalog_revision, opening_tracked_mutation_revision] =
                load_revisions(&read, [REVISION_KEY_CATALOG, REVISION_KEY_TRACKED_MUTATION])
                    .await?;
            let catalog_revision = catalog_revision.map(CatalogRevision::from_storage_bytes);
            let (sql_schema_catalog, tracked_schema_catalog) = {
                let visible_hot_state = hot_state.reader(&read);
                let sql_schema_catalog = catalog_context
                    .compiled_catalog_for_transaction_open(
                        &visible_hot_state,
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
                        &visible_hot_state,
                        &Domain::schema_catalog(active_branch_id.clone(), false),
                        catalog_revision.as_ref(),
                    )
                    .await?;
                (sql_schema_catalog, tracked_schema_catalog)
            };
            let branch_reader = branch_ctx.ref_reader(&read);
            let opening_active_branch_head =
                branch_reader.load_head_commit_id(&active_branch_id).await?;
            let opening_global_branch_head = if active_branch_id == GLOBAL_BRANCH_ID {
                opening_active_branch_head
            } else {
                branch_reader.load_head_commit_id(GLOBAL_BRANCH_ID).await?
            };
            let opening_plugin_registry = if let Some(head) = opening_active_branch_head {
                let mut tracked = tracked_state.reader(&read);
                load_plugin_registry_at_commit(&mut tracked, &head.to_string()).await?
            } else {
                PluginRegistry::empty()
            };
            Ok::<_, LixError>((
                active_branch_id,
                runtime_functions,
                functions,
                sql_schema_catalog,
                tracked_schema_catalog,
                opening_tracked_mutation_revision,
                opening_active_branch_head,
                opening_global_branch_head,
                opening_plugin_registry,
                runtime_boundary_result,
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
            opening_active_branch_head,
            opening_global_branch_head,
            opening_plugin_registry,
            runtime_boundary_result,
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
            Arc::clone(&tracked_schema_catalog),
        );
        let staged_writes = Arc::new(TransactionWriteBuffer::new(functions.clone()));
        let transaction = Self {
                    write_context_liveness: crate::sql2::WriteContextLiveness::new(),
                    active_branch_id,
                    active_account_id,
                    hot_state,
                    tracked_state,
                    binary_cas,
                    plugin_host,
                    branch_ctx,
                    schema_resolver,
                    sql_schema_snapshot: sql_schema_catalog,
                    tracked_schema_snapshot: tracked_schema_catalog,
                    opening_plugin_registry,
                    sql_planning_cache,
                    prepared_mutation_program: None,
                    prepared_mutation_membership: PreparedMutationMembership::Unprepared,
                    prepared_mutation_overlay_empty: false,
                    prepared_mutation_timestamp: None,
                    mutation_journal: None,
                    mutation_journal_compressor: None,
                    mutation_journal_sealed_rows: 0,
                    mutation_journal_seal_prefix_open: true,
                    mutation_journal_terminal_error: None,
                    staged_writes,
                    filesystem_path_index_cache: Arc::new(FilesystemPathIndexCache::default()),
                    filesystem_path_index_epoch: Arc::new(AtomicUsize::new(0)),
                    branch_head_control_cache: Arc::new(BranchHeadControlCache::default()),
                    opening_read,
                    storage,
                    functions,
                    current_timestamp: None,
                    opening_tracked_mutation_revision,
                    opening_active_branch_head,
                    opening_global_branch_head,
                    commit_boundary: None,
                    trust_filesystem_planner: false,
                    origin_key: None,
                    idempotency_receipt: None,
                    atomic_metadata_writes: None,
                    atomic_metadata_preconditions: Vec::new(),
                    suppress_ordinary_sync_event: false,
                    sync_role: crate::sync::SyncRole::Disabled,
                    await_durable_commit: false,
                    session_file_views,
                    pending_file_view_mutations: BTreeMap::new(),
                    pending_plugin_actor_publications: Vec::new(),
                    pending_branch_checkpoint_replacements: BTreeMap::new(),
                    pending_restore_targets: BTreeMap::new(),
                    plugin_generation_read_guard: None,
                    plugin_generation_upgrade_guard: None,
                };
        Ok((
            OpenTransaction {
                transaction,
                runtime_functions,
            },
            runtime_boundary_result,
        ))
    }

    /// Commits prepared writes, runtime function state, and the storage transaction.
    ///
    /// Commit owns the execution boundary: prepared rows become changelog
    /// facts, branch-ref updates, and visible hot_state rows before the
    /// storage transaction is committed.
    pub(crate) async fn commit(
        self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let mut transaction = self;
        let commit_cohort_id = crate::telemetry::current_commit_cohort_id()
            .unwrap_or_else(crate::telemetry::next_commit_cohort_id);
        let materialize_span = start_materialize_span(1, &commit_cohort_id);
        let prepared_writes = match transaction.staged_writes.drain() {
            Ok(prepared_writes) => prepared_writes,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                if let Some(materialize_span) = materialize_span {
                    materialize_span.finish(
                        Status::error(error.code.clone()),
                        vec![TelemetryAttribute::string("error.type", error.code.clone())],
                    );
                }
                return Err(error);
            }
        };
        transaction
            .commit_prepared(
                runtime_functions,
                prepared_writes,
                1,
                commit_cohort_id,
                materialize_span,
            )
            .await
    }

    async fn commit_prepared(
        mut self,
        runtime_functions: &FunctionContext,
        mut prepared_writes: PreparedWriteSet,
        transaction_count: usize,
        commit_cohort_id: String,
        materialize_span: Option<ActiveTelemetrySpan>,
    ) -> Result<TransactionCommitOutcome, LixError> {
        #[cfg(feature = "storage-benches")]
        let _phase =
            crate::storage_bench::enter_crud_phase(crate::storage_bench::CRUD_PHASE_COMMIT);
        let transaction = &mut self;
        let commit_boundary = transaction.commit_boundary.clone();
        let _commit_guard = begin_commit_boundary(commit_boundary.as_ref());
        let (
            writes,
            write_options,
            filesystem_delta_rows,
            previous_filesystem_revision,
            next_catalog_revision,
        ) = instrument_lix_result(materialize_span, async {
            transaction
                .uncache_completed_plugin_actors_for_large_file_writes(&prepared_writes)
                .await;
            let tracked_state_changed = prepared_writes.state_rows.iter().any(|row| !row.untracked)
                || !prepared_writes.commit_change_refs_by_branch.is_empty()
                || !prepared_writes.extra_commit_parents_by_branch.is_empty();
            let has_untracked_state_writes =
                prepared_writes.state_rows.iter().any(|row| row.untracked);
            // Untracked rows are mutable current state, but their validation can read
            // tracked schemas, parents, uniqueness owners, or filesystem state.
            // Fence that snapshot without rotating the tracked revision: normal
            // tracked transactions remain independent of untracked-only commits.
            let requires_tracked_snapshot_fence =
                tracked_state_changed || has_untracked_state_writes;
            let catalog_revision_changed = prepared_writes_change_catalog(&prepared_writes);
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
            let commit_read = commit_read_storage
                .begin_read(StorageReadOptions::default())
                .await?;
            // SAFETY: `commit_read_storage` is an `Arc` retained through commit,
            // and the transaction drops this read before its storage field.
            let commit_read = unsafe { assume_static_storage_read::<StorageImpl>(commit_read) };
            let mut read = SharedStorageAdapterRead::new(commit_read);
            // Commit-time reconciliation and validation must all observe this
            // current coherent snapshot, while user statements above observed the
            // snapshot retained from transaction open.
            transaction.opening_read = read.clone();
            if let Err(error) = transaction
                .reconcile_stale_disjoint_writes(&read, &mut prepared_writes)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.transaction_reconcile_stale"
                ))
                .await
            {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
            let branch_checkpoint_bridges = match transaction
                .resolve_pending_branch_checkpoint_replacements(&read, &prepared_writes)
                .await
            {
                Ok(branch_checkpoint_bridges) => branch_checkpoint_bridges,
                Err(error) => {
                    transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                    return Err(error);
                }
            };
            let restore_targets = std::mem::take(&mut transaction.pending_restore_targets);
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
            if let Err(error) = Self::attach_checkpoint_branch_parents(
                &read,
                &mut prepared_writes,
                &commit_parent_heads,
            )
            .await
            {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
            if let Err(error) = transaction
                .validate_prepared_writes_by_branch(&read, &mut prepared_writes)
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
            // The delta itself is projected out of the commit below, once
            // addressable rows hold their final commit-delta change ids. Only its
            // *projectability* is decided here, because the revision the cached
            // views are keyed on has to be read before the commit publishes its
            // successor.
            let stages_projectable_filesystem_rows =
                prepared_writes_stage_filesystem_rows(&prepared_writes)
                    && !prepared_writes_require_filesystem_index_rebuild(&prepared_writes);
            // A failed revision read must not collapse into "no revision yet".
            // `None` is itself a live cache key — the state before the first
            // filesystem commit — so treating an error as `None` would rekey
            // entries built at an unknown revision onto this commit's successor and
            // make a stale index reachable. The outer `Option` is "the read
            // succeeded"; only that licenses a projection.
            let loaded_filesystem_revision = if stages_projectable_filesystem_rows {
                load_path_index_revision(&read).await.ok()
            } else {
                None
            };
            let filesystem_delta_projectable = loaded_filesystem_revision.is_some();
            let previous_filesystem_revision = loaded_filesystem_revision.flatten();
            let mut automatic_sync_writes = transaction.storage.new_write_set();
            let mut automatic_sync_preconditions = Vec::new();
            let capture_sync_commits = !transaction.suppress_ordinary_sync_event
                && transaction.sync_role == crate::sync::SyncRole::Authority;
            if !transaction.suppress_ordinary_sync_event
                && transaction.sync_role == crate::sync::SyncRole::Replica
            {
                // The immutable commit and ref are the durable outbox.
                // `build_sync_push` discovers unpublished local heads; no second
                // row-pack queue is maintained.
                transaction.await_durable_commit = true;
            }
            let materialized = match commit::commit_prepared_writes_with_parent_heads(
                &transaction.binary_cas,
                &transaction.tracked_state,
                Some(transaction.sql_schema_snapshot.as_ref()),
                Some(runtime_functions),
                &transaction.active_account_id,
                &commit_parent_heads,
                &mut read,
                &branch_checkpoint_bridges,
                capture_sync_commits,
                &restore_targets,
                prepared_writes,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_materialization"
            ))
            .await
            {
                Ok(commit) => commit,
                Err(error) => {
                    transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                    return Err(error);
                }
            };
            let staged_sync_event = if capture_sync_commits {
                // Consume the exact controls produced by materialization instead
                // of predicting checkpoint/restore semantics from prepared rows.
                // The event still joins the same atomic storage commit below.
                match crate::sync::stage_repository_transaction_event(
                    &read,
                    &mut automatic_sync_writes,
                    &mut automatic_sync_preconditions,
                    &materialized.sync_commits,
                    &materialized.published_branch_controls,
                )
                .await
                {
                    Ok(event) => event,
                    Err(error) => {
                        transaction
                            .discard_pending_plugin_actor_publications()
                            .await;
                        return Err(error);
                    }
                }
            } else {
                None
            };
            if staged_sync_event.is_some() {
                transaction.await_durable_commit = true;
            }
            if let Some(staged_sync_event) = &staged_sync_event
                && let Err(error) = crate::sync::validate_repository_transaction_event_transfer(
                    staged_sync_event,
                    &materialized.sync_commits,
                )
            {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
            let mut writes = materialized.writes;
            let materialization_preconditions = materialized.preconditions;
            let filesystem_delta_rows = if filesystem_delta_projectable {
                materialized.filesystem_delta_rows
            } else {
                Vec::new()
            };
            let next_catalog_revision =
                catalog_revision_changed.then(|| stage_catalog_revision(&mut writes));
            if tracked_state_changed {
                StorageAdapter::<StorageImpl>::stage_tracked_mutation_revision(&mut writes);
            }
            writes.extend(automatic_sync_writes);
            if let Some(metadata_writes) = transaction.atomic_metadata_writes.take() {
                writes.extend(metadata_writes);
            }
            let mut write_options = StorageWriteOptions::default();
            write_options.await_durable = transaction.await_durable_commit;
            write_options
                .preconditions
                .extend(materialization_preconditions);
            write_options
                .preconditions
                .append(&mut automatic_sync_preconditions);
            write_options
                .preconditions
                .append(&mut transaction.atomic_metadata_preconditions);
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
                write_options.await_durable = true;
                write_options.idempotency_key = Some(key.0.clone());
                write_options
                    .preconditions
                    .push(StoragePrecondition::KeyAbsent {
                        space: EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
                        key,
                    });
            }
            Ok((
                writes,
                write_options,
                filesystem_delta_rows,
                previous_filesystem_revision,
                next_catalog_revision,
            ))
        })
        .await?;
        // Keep the prepared commit's storage borrow independent from the
        // transaction so deterministic preparation failures can still drain
        // prospective plugin actor documents before returning.
        let commit_storage = transaction.storage.clone();
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_write_set_arena(&writes);
        let storage_span = ActiveTelemetrySpan::start_current(
            &TRANSACTION_STORAGE,
            vec![
                TelemetryAttribute::i64(
                    "lix.transaction.count",
                    i64::try_from(transaction_count).unwrap_or(i64::MAX),
                ),
                TelemetryAttribute::string("lix.commit_cohort_id", commit_cohort_id.clone()),
            ],
        );
        let storage_stats = instrument_lix_result(storage_span, async {
            let prepared_commit = match commit_storage
                .prepare_write_set(writes, write_options)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.transaction_storage_prepare"
                ))
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
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_ownership(
                    crate::storage_bench::CRUD_OWNERSHIP_ADAPTER,
                    stats.staged_puts.saturating_add(stats.staged_deletes) as usize,
                    0,
                    stats.written_bytes as usize,
                    stats.put_batches.saturating_add(stats.delete_batches) as usize,
                    stats.storage_calls as usize,
                    stats.touched_spaces as usize,
                );
                Ok(stats)
            })
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_storage_commit"
            ))
            .await?;
            let post_commit_read_storage = transaction.storage.clone();
            if let Some(next_catalog_revision) = next_catalog_revision.as_ref()
                && let Ok(next_read) = post_commit_read_storage
                    .begin_read(StorageReadOptions::default())
                    .await
            {
                let next_read = SharedStorageAdapterRead::new(next_read);
                // A concurrent schema commit may already have advanced this fresh
                // read beyond the revision we published. Never index that newer
                // catalog under our older revision key.
                if catalog_revision_is_current(
                    load_catalog_revision(&next_read)
                        .await
                        .ok()
                        .flatten()
                        .as_ref(),
                    next_catalog_revision,
                ) {
                    let visible_hot_state = transaction.hot_state.reader(&next_read);
                    // Cache warming is derived state. Once the durable commit
                    // succeeds, a warming failure must not turn success into an
                    // ambiguous error; the next transaction safely compiles on demand.
                    let _ = transaction
                        .schema_resolver
                        .warm_committed_catalogs(
                            &visible_hot_state,
                            &transaction.active_branch_id,
                            next_catalog_revision,
                        )
                        .await;
                }
            }
            if !filesystem_delta_rows.is_empty()
                && incremental_filesystem_index_enabled()
                && let Ok(next_read) = post_commit_read_storage
                    .begin_read(StorageReadOptions::default())
                    .await
            {
                let next_read = SharedStorageAdapterRead::new(next_read);
                if let Ok(next_revision) = load_path_index_revision(&next_read).await {
                    transaction.hot_state.advance_filesystem_path_indexes(
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
            Ok(storage_stats)
        })
        .await?;
        Ok(TransactionCommitOutcome {
            storage_stats,
            commit_cohort_id: Some(commit_cohort_id),
        })
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
            .file_content_writes
            .iter()
            .filter(|write| write.len() > MAX_RETAINED_IMPORT_BYTES as u64)
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

    pub(crate) fn set_sync_role(&mut self, role: crate::sync::SyncRole) {
        self.sync_role = role;
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

    /// Captures the mutable state an explicit SQL statement can change before
    /// its `RETURNING` projection is evaluated.
    ///
    /// Most write errors occur before staging. Post-image `RETURNING` paths
    /// intentionally stage first, though, so this checkpoint gives those paths
    /// normal statement atomicity without adding work to the automatic-write
    /// fast path.
    pub(crate) fn begin_sql_statement_checkpoint(
        &self,
    ) -> Result<SqlStatementCheckpoint, LixError> {
        Ok(SqlStatementCheckpoint {
            staged_writes: self.staged_writes.checkpoint()?,
            filesystem_path_index_epoch: self.filesystem_path_index_epoch.load(Ordering::SeqCst),
            pending_file_view_mutations: self.pending_file_view_mutations.clone(),
            trust_filesystem_planner: self.trust_filesystem_planner,
        })
    }

    /// Restores an explicit transaction after a statement failed during
    /// post-stage projection. Earlier successful statements remain staged.
    pub(crate) async fn rollback_sql_statement_checkpoint(
        &mut self,
        checkpoint: SqlStatementCheckpoint,
    ) -> Result<(), LixError> {
        let SqlStatementCheckpoint {
            staged_writes,
            filesystem_path_index_epoch,
            pending_file_view_mutations,
            trust_filesystem_planner,
        } = checkpoint;
        self.staged_writes.restore(staged_writes)?;
        self.filesystem_path_index_epoch
            .store(filesystem_path_index_epoch, Ordering::SeqCst);
        // The cache is derived from the discarded post-image. Evict it rather
        // than cloning potentially large indexes solely for this error path.
        self.filesystem_path_index_cache.clear();
        self.pending_file_view_mutations = pending_file_view_mutations;
        self.trust_filesystem_planner = trust_filesystem_planner;
        // A failed statement may have chained a predecessor actor successor.
        // Its prior document can no longer be restored byte-for-byte, so
        // conservatively discard all unpublished actor cache work. The staged
        // durable rows are restored above; a later read cold-opens from them.
        let publications = std::mem::take(&mut self.pending_plugin_actor_publications);
        let discarded_view_keys = publications
            .iter()
            .map(|publication| publication.session_key().clone())
            .collect::<Vec<_>>();
        discard_plugin_actor_publications(publications).await;
        for key in discarded_view_keys {
            self.pending_file_view_mutations
                .insert(key.clone(), SessionFileViewMutation::Remove { key });
        }
        // Registered-schema normalization uses copy-on-write catalogs. Rebuild
        // lazily from the restored staging overlay so failed registrations do
        // not survive in a cached catalog while earlier ones still do.
        self.schema_resolver.clear_cached_catalogs();
        Ok(())
    }

    async fn discard_pending_plugin_actor_publications(&mut self) {
        discard_plugin_actor_publications(std::mem::take(
            &mut self.pending_plugin_actor_publications,
        ))
        .await;
    }

    /// Releases private plugin actor leases after an explicit write statement.
    ///
    /// The durable semantic rows and checkpoints remain staged in this
    /// transaction. Keeping the live actor leased until commit would serialize
    /// another same-base transaction that edits the same file, so explicit
    /// transactions retain only the cold-open marker between statements.
    pub(crate) async fn release_pending_plugin_actor_leases(&mut self) {
        let publications = std::mem::take(&mut self.pending_plugin_actor_publications);
        let mut uncached = Vec::with_capacity(publications.len());
        for publication in publications {
            uncached.push(publication.into_uncached().await);
        }
        self.pending_plugin_actor_publications = uncached;
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
        let certified_fileless_typed_sql = matches!(
            &write,
            TransactionWrite::Rows { rows, .. }
                if rows
                    .certified_preparation()
                    .is_some_and(|certificate| certificate.fileless_typed_sql_rows)
        );
        if certified_fileless_typed_sql {
            let TransactionWrite::Rows { mode, rows } = write else {
                unreachable!("certified fileless SQL writes contain only rows")
            };
            return match mode {
                TransactionWriteMode::Insert => {
                    Box::pin(self.stage_certified_raw_parameter_batch_insert(rows)).await
                }
                TransactionWriteMode::Replace => {
                    Box::pin(self.stage_certified_parameter_batch_replace(rows)).await
                }
            };
        }
        Box::pin(self.stage_write_inner(write, None)).await
    }

    async fn stage_parameter_batch_insert(
        &mut self,
        write: TransactionWrite,
        statement_indices: Vec<u32>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Box::pin(self.stage_write_inner(write, Some(statement_indices))).await
    }

    /// Stages the fixed-shape public parameter INSERT proof without routing it
    /// through plugin reconciliation, storage-scope scans, or filesystem path
    /// preflight. The producer certificate excludes every system column those
    /// generic passes inspect; committed and transaction-local INSERT
    /// collision checks remain in the normal prepared journal.
    async fn stage_certified_raw_parameter_batch_insert(
        &mut self,
        mut rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        debug_assert!(rows.certified_preparation().is_some());
        self.ensure_plugin_generation_read_guard().await;

        let (branch_id, schema_key, global) = {
            let first = rows.row(0);
            (
                first.branch_id.clone(),
                first.schema_key.clone(),
                first.global,
            )
        };
        if !rows
            .certified_preparation()
            .is_some_and(|certificate| certificate.fileless_typed_sql_rows)
            && !global
            && self
                .visible_plugin_registry_owns_schema(&branch_id, &schema_key)
                .await?
        {
            rows.revoke_certified_preparation();
            let statement_indices = (0..row_count)
                .map(|index| {
                    u32::try_from(index).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "parameter batch row count exceeds u32",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Box::pin(self.stage_write_inner(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows,
                },
                Some(statement_indices),
            ))
            .await;
        }
        let staged = self.staged_writes.staging_overlay()?;
        if StagedHotStateRows::collection_replaced(
            &staged,
            branch_id.as_str(),
            schema_key.as_str(),
            None,
        )? {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!("collection '{schema_key}' was deleted earlier in this transaction"),
            )
            .with_hint(
                "Commit the collection deletion before recreating rows in its next generation.",
            ));
        }

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let prepared = self
            .prepare_transaction_rows(rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_prepare_rows"
            ))
            .await?;
        if prepared.len() != row_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified parameter INSERT preparation changed row cardinality",
            ));
        }
        tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage").in_scope(
            || {
                self.staged_writes.stage_certified_parameter_batch_insert(
                    PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: prepared,
                    },
                )
            },
        )
    }

    /// Stages a certified dense replacement without re-entering plugin,
    /// storage-scope, or filesystem-path preparation for every logical SQL
    /// statement. The producer only issues this certificate for existing,
    /// ordinary tracked, unfiled rows in one active-branch row collection.
    async fn stage_certified_parameter_batch_replace(
        &mut self,
        mut rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        debug_assert!(rows.certified_preparation().is_some());
        self.ensure_plugin_generation_read_guard().await;

        let (branch_id, schema_key, global) = {
            let first = rows.row(0);
            (
                first.branch_id.clone(),
                first.schema_key.clone(),
                first.global,
            )
        };
        if !rows
            .certified_preparation()
            .is_some_and(|certificate| certificate.fileless_typed_sql_rows)
            && !global
            && self
                .visible_plugin_registry_owns_schema(&branch_id, &schema_key)
                .await?
        {
            rows.revoke_certified_preparation();
            return Box::pin(self.stage_write_inner(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                },
                None,
            ))
            .await;
        }

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let prepared = self
            .prepare_transaction_rows(rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_prepare_rows"
            ))
            .await?;
        if prepared.len() != row_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified parameter replacement preparation changed row cardinality",
            ));
        }
        tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage").in_scope(
            || {
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: prepared,
                    })
            },
        )
    }

    async fn stage_write_inner(
        &mut self,
        write: TransactionWrite,
        statement_indices: Option<Vec<u32>>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        if let Some(statement_indices) = &statement_indices {
            debug_assert_eq!(statement_indices.len(), transaction_write_row_count(&write));
        }
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
        if let Some(statement_indices) = &statement_indices
            && statement_indices.len() != prepared_transaction_write_rows(&write).len()
        {
            discard_plugin_actor_publications(actor_publications).await;
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "parameter batch normalization changed row cardinality",
            ));
        }
        let (affects_filesystem_path_index, mut filesystem_delta_rows) =
            prepared_transaction_write_filesystem_index_impact(&write)?;
        let stage_result = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_buffer_stage"
        )
        .in_scope(|| match statement_indices {
            Some(indices) => self
                .staged_writes
                .stage_parameter_batch_insert(write, indices),
            None => self.staged_writes.stage_write(write),
        });
        let outcome = match stage_result {
            Ok(outcome) => outcome,
            Err(error) => {
                discard_plugin_actor_publications(actor_publications).await;
                return Err(error);
            }
        };
        if affects_filesystem_path_index {
            let tracked_branch_ids = filesystem_delta_rows
                .iter()
                .filter(|row| !row.untracked)
                .map(|row| row.branch_id.to_string())
                .collect::<BTreeSet<_>>();
            let mut commit_ids = BTreeMap::new();
            for branch_id in tracked_branch_ids {
                commit_ids.insert(
                    branch_id.clone(),
                    self.staged_writes.commit_id_for_branch(&branch_id)?,
                );
            }
            for row in &mut filesystem_delta_rows {
                row.commit_id = if row.untracked {
                    None
                } else {
                    commit_ids.get(row.branch_id.as_ref()).copied().flatten()
                };
            }
            let previous_epoch = self
                .filesystem_path_index_epoch
                .fetch_add(1, Ordering::SeqCst);
            let next_epoch = previous_epoch.wrapping_add(1);
            if incremental_filesystem_index_enabled() && !filesystem_delta_rows.is_empty() {
                self.filesystem_path_index_cache.advance_revisions(
                    &filesystem_delta_rows,
                    |revision| {
                        advance_transaction_path_index_cache_revision(
                            revision,
                            previous_epoch,
                            next_epoch,
                        )
                    },
                );
            }
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
            | TransactionWrite::RowsWithFileContent { rows, .. } => rows,
        };
        let staged = self.staged_writes.staging_overlay()?;
        let mut first_checked_scope = None;
        let mut additional_checked_scopes = None;
        for row in rows.iter() {
            if row.schema_key.as_str()
                == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            {
                continue;
            }
            let scope = (
                row.branch_id.as_str(),
                row.schema_key.as_str(),
                row.file_id.map(SharedStr::as_str),
            );
            let already_checked = match first_checked_scope {
                None => {
                    first_checked_scope = Some(scope);
                    false
                }
                Some(first) if first == scope => true,
                Some(first) => !additional_checked_scopes
                    .get_or_insert_with(|| std::collections::HashSet::from([first]))
                    .insert(scope),
            };
            if already_checked {
                continue;
            }
            if StagedHotStateRows::collection_replaced(
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
    pub(crate) async fn merge_plugin_columns(
        &mut self,
        plugin: &PluginRegistryEntry,
        merges: Vec<WasmHostColumnMerge>,
    ) -> Result<crate::plugin::runtime::ValidatedColumnMergeTransition, LixError> {
        self.ensure_plugin_generation_read_guard().await;
        let limits = conflict_resolution_limits(merges.len())?;
        let wasm_hash = BlobId::from_hex(plugin.wasm_blob_hash().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' has no column-merger component", plugin.key()),
            )
        })?)?;
        let wasm = if self
            .plugin_host
            .cached_plugin_factory(plugin.key(), wasm_hash)?
            .is_some()
        {
            None
        } else {
            let read = SharedStorageAdapterRead::new(
                self.storage
                    .begin_read(StorageReadOptions::default())
                    .await?,
            );
            let reader = self.binary_cas.reader(read);
            Some(
                load_transaction_blob_bytes(&reader, &self.staged_writes, &[wasm_hash])
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
                    })?,
            )
        };
        self.plugin_host
            .merge_columns(plugin, wasm, merges, limits)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_merge_columns"
            ))
            .await
    }

    pub(crate) fn merge_primary_key_columns(
        &self,
        schema_key: &str,
    ) -> Result<BTreeSet<String>, LixError> {
        let (_, plan) = self
            .sql_schema_snapshot
            .plan_for_key(schema_key)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("row merge references unknown schema '{schema_key}'"),
                )
            })?;
        crate::plugin::runtime::primary_key_columns(plan)
    }

    async fn merge_stale_column_inputs(
        &mut self,
        inputs: &[StaleColumnMergeInput],
    ) -> Result<Vec<Option<StaleConflictPayload>>, LixError> {
        let decoded = inputs
            .iter()
            .map(|input| {
                if input.typed {
                    Ok((None, None, None))
                } else {
                    Ok((
                        decode_stale_payload(input.base.as_ref())?,
                        decode_stale_payload(input.a.as_ref())?,
                        decode_stale_payload(input.b.as_ref())?,
                    ))
                }
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        for input in inputs.iter().filter(|input| input.typed) {
            let expected = self
                .sql_schema_snapshot
                .plan_for_key(input.key.schema_key.as_str())
                .map(|(_, plan)| plan.fingerprint().bytes())
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "typed row merge references unknown schema '{}'",
                            input.key.schema_key
                        ),
                    )
                })?;
            for (role, payload) in [
                ("base", input.base.as_ref()),
                ("successor A", input.a.as_ref()),
                ("successor B", input.b.as_ref()),
            ]
            .into_iter()
            .filter_map(|(role, payload)| payload.map(|payload| (role, payload)))
            {
                let row = typed_row_version_ref(Some(payload)).map_err(|error| {
                    LixError::new(
                        error.code,
                        format!(
                            "plugin-owned stale {role} row for schema '{}' must have exactly one native typed snapshot",
                            input.key.schema_key
                        ),
                    )
                })?
                .expect("a present typed merge payload has a row");
                if row.snapshot.schema_fingerprint != expected {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "plugin-owned stale row for schema '{}' has the wrong schema fingerprint",
                            input.key.schema_key
                        ),
                    ));
                }
            }
        }
        let mut groups = BTreeMap::<(String, String), StaleColumnMergeGroup>::new();
        for (row_index, input) in inputs.iter().enumerate() {
            if !input.typed {
                continue;
            }
            let Some(plugin) = input.plugin.as_ref() else {
                continue;
            };
            let (Some(base), Some(a), Some(b)) =
                (input.base.as_ref(), input.a.as_ref(), input.b.as_ref())
            else {
                continue;
            };
            let schema_plan = self
                .sql_schema_snapshot
                .plan_for_key(input.key.schema_key.as_str())
                .map(|(_, plan)| plan)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "column merge references unknown schema '{}'",
                            input.key.schema_key
                        ),
                    )
                })?;
            let schema_fingerprint = schema_plan.fingerprint().bytes();
            let base_row = base.decoded_snapshot.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin-owned base row is missing its native typed snapshot",
                )
            })?;
            let a_row = a.decoded_snapshot.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin-owned successor row is missing its native typed snapshot",
                )
            })?;
            let b_row = b.decoded_snapshot.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin-owned successor row is missing its native typed snapshot",
                )
            })?;
            if [base_row, a_row, b_row]
                .into_iter()
                .any(|row| row.schema_fingerprint != schema_fingerprint)
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin-owned stale row for schema '{}' has the wrong schema fingerprint",
                        input.key.schema_key
                    ),
                ));
            }
            let group = groups
                .entry((
                    plugin.key().to_owned(),
                    plugin.archive_blob_hash().to_owned(),
                ))
                .or_insert_with(|| StaleColumnMergeGroup {
                    plugin: plugin.clone(),
                    merges: Vec::new(),
                    destinations: Vec::new(),
                });
            reconcile_typed_row(
                typed_row_version_ref(Some(base))?,
                typed_row_version_ref(Some(a))?,
                typed_row_version_ref(Some(b))?,
                &input.primary_key_columns,
                |overlap| {
                    let ordinal = u32::try_from(group.merges.len()).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "column merge batch exceeds the u32 ordinal limit",
                        )
                    })?;
                    group.merges.push(WasmHostColumnMerge {
                        ordinal,
                        key: WasmRowKey::from_typed_parts(
                            input.key.schema_key.clone(),
                            schema_fingerprint,
                            base_row.row_pk.clone(),
                        )?,
                        file_id: input.key.file_id.clone(),
                        column: overlap.column.to_owned(),
                        schema_fingerprint,
                        base: base_row.row.get(overlap.column).cloned(),
                        a: overlap.a.cloned(),
                        b: overlap.b.cloned(),
                        base_row: base_row.clone(),
                        a_row: a_row.clone(),
                        b_row: b_row.clone(),
                    });
                    group
                        .destinations
                        .push((row_index, overlap.column.to_owned()));
                    Ok(Some(HostTypedColumnMergeResult::UseLww))
                },
            )?;
        }

        let mut replacements = BTreeMap::<(usize, String), HostTypedColumnMergeResult>::new();
        for (_, group) in groups {
            let resolved = self
                .merge_plugin_columns(&group.plugin, group.merges)
                .await?;
            if resolved.results.len() != group.destinations.len() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "validated column merge output lost input alignment",
                ));
            }
            for (destination, result) in group.destinations.into_iter().zip(resolved.results) {
                let result = match result {
                    WasmColumnMergeResult::UseLww => HostTypedColumnMergeResult::UseLww,
                    WasmColumnMergeResult::Replace(value) => {
                        HostTypedColumnMergeResult::Replace(value)
                    }
                };
                replacements.insert(destination, result);
            }
        }

        inputs
            .iter()
            .zip(&decoded)
            .enumerate()
            .map(|(row_index, (input, (base, a, b)))| {
                if input.typed {
                    Ok(reconcile_typed_row(
                        typed_row_version_ref(input.base.as_ref())?,
                        typed_row_version_ref(input.a.as_ref())?,
                        typed_row_version_ref(input.b.as_ref())?,
                        &input.primary_key_columns,
                        |overlap| Ok(replacements.remove(&(row_index, overlap.column.to_owned()))),
                    )?
                    .map(encoded_typed_stale_payload))
                } else {
                    reconcile_row(
                        row_version_ref(base.as_ref()),
                        row_version_ref(a.as_ref()),
                        row_version_ref(b.as_ref()),
                        &input.primary_key_columns,
                        |_| Ok(None),
                    )?
                    .map(encoded_stale_payload)
                    .transpose()
                }
            })
            .collect()
    }

    async fn scan_visible_hot_state_batch(
        &mut self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = self.opening_read();
        let base = self
            .hot_state
            .transaction_reader(read, Arc::clone(&self.branch_head_control_cache));
        overlay_scan_batch(&base, &staged, request).await
    }

    async fn visible_materialization(
        &mut self,
        key: &PluginFileWriteKey,
    ) -> Result<Option<VisibleMaterialization>, LixError> {
        let rows = self
            .scan_visible_hot_state_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    row_pks: vec![validated_uuid_row_pk(&key.file_id)?],
                    branch_ids: vec![key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(key.file_id.clone())],
                    untracked: Some(key.untracked),
                    ..Default::default()
                },
                projection: plugin_registry_hot_state_projection(),
                ..Default::default()
            })
            .await?;
        if rows.len() > 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "component materialization lookup returned duplicate rows for file '{}'",
                    key.file_id
                ),
            ));
        }
        rows.get(0)
            .map(|row| decode_visible_materialization_ref(row, &key.file_id))
            .transpose()
    }

    async fn cold_open_semantic_actor(
        &mut self,
        actor_key: &PluginActorKey,
        file_key: &PluginFileWriteKey,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentFactory>,
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
        let base = self.hot_state.reader(read.clone());
        let blob_rows = overlay_scan_batch(
            &base,
            &staged,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    row_pks: vec![validated_uuid_row_pk(&actor_key.file_id)?],
                    branch_ids: vec![actor_key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(actor_key.file_id.clone())],
                    untracked: Some(file_key.untracked),
                    ..Default::default()
                },
                projection: plugin_registry_hot_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        if blob_rows.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                format!(
                    "owned component plugin file '{}' must have exactly one visible materialization; found {}",
                    actor_key.file_id,
                    blob_rows.len()
                ),
            ));
        }
        let materialization =
            decode_visible_materialization_ref(blob_rows.row(0), &actor_key.file_id)?;
        if !matches!(
            &materialization.bytes,
            VisibleMaterializationBytes::Blob { .. }
        ) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "owned component plugin file '{}' materialization does not match plugin '{}' contract",
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
        let mut actor = factory.instantiate_actor().await?;
        let cold_open_hydrates_without_render = actor.cold_open_hydrates_without_render();
        let rows = if actor.cold_open_requires_rows() {
            overlay_scan_batch(
                &base,
                &staged,
                &HotStateScanRequest {
                    filter: HotStateFilter {
                        schema_keys: plugin.schema_keys().to_vec(),
                        branch_ids: vec![actor_key.branch_id.clone()],
                        file_ids: vec![NullableKeyFilter::Value(actor_key.file_id.clone())],
                        untracked: Some(file_key.untracked),
                        ..Default::default()
                    },
                    projection: plugin_state_hot_state_projection(),
                    ..Default::default()
                },
            )
            .await?
        } else {
            MaterializedHotStateBatch::default()
        };
        let row_ordinals = v2_host_row_ordinals_from_live_batch(
            &rows,
            file_key,
            plugin.schema_keys(),
            &self.sql_schema_snapshot,
        )?;
        let row_authorities = plugin_row_authorities_from_live_batch(&rows, &row_ordinals)?;
        let row_count = row_ordinals.len();
        let VisibleMaterializationBytes::Blob { hash } = materialization.bytes;
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
                    "owned component plugin file '{}' references missing materialized blob '{}'",
                    actor_key.file_id,
                    hash.to_hex()
                ),
            )
        })?
        .into();
        let source = LiveBatchRowSource::new_typed(rows, row_ordinals, limits)?;
        let transition = match actor
            .open_rows(
                limits,
                WasmOpenRowsInput {
                    descriptor,
                    rows: Box::new(source),
                    accepted: Some(Arc::new(ArcByteSource::new(materialized_bytes.clone()))),
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
        let validated = match drain_row_transition_edits(
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
        };
        let materialized_bytes = validated.bytes.clone();
        let materialized_bytes_sha256 = validated.bytes_sha256;
        let mut counters = validated.counters;
        counters.full_state_semantic_rows_materialized =
            u64::try_from(row_count).unwrap_or(u64::MAX);
        counters.full_document_reparses = 1;
        counters.full_renderer_invocations = u64::from(!cold_open_hydrates_without_render);
        self.plugin_host.record_transition_counters(counters);
        cache
            .install_cold_if_absent_with_authorities(
                cold_install,
                actor_key.clone(),
                PluginActorStore::new(actor, store_permit),
                validated.document,
                materialized_bytes,
                materialized_bytes_sha256,
                Arc::<str>::from(semantic_root),
                row_authorities,
            )
            .await
    }

    /// Leases an acknowledged actor, cold-opening only when cache eviction is
    /// the sole reason the observation no longer resolves.
    ///
    /// The durable semantic root must still exactly match the root delivered
    /// with the observation. A concurrent committed transition therefore
    /// remains stale and cannot be mistaken for benign working-set eviction.
    async fn lease_or_reopen_observed_actor(
        &mut self,
        observation: &PluginObservation,
        actor_key: &PluginActorKey,
        file_key: &PluginFileWriteKey,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentFactory>,
        current_publications: &mut Vec<PendingPluginActorPublication>,
    ) -> Result<PluginActorLease, LixError> {
        let cache = self.plugin_host.actor_cache();
        match cache.lease_for_transition(observation).await {
            Ok(lease) => return Ok(lease),
            Err(error) if error.code == LixError::CODE_PLUGIN_OBSERVATION_STALE => {
                let Some(visible_materialization) = self.visible_materialization(file_key).await?
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
            .cold_open_semantic_actor(
                actor_key,
                file_key,
                plugin,
                descriptor,
                factory,
                current_publications,
            )
            .await?;
        cache.lease_for_transition(&reopened).await
    }

    async fn load_visible_exact_hot_state_batch(
        &mut self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self
            .hot_state
            .transaction_reader(read, Arc::clone(&self.branch_head_control_cache));
        overlay_load_exact_batch(&base, &staged, request).await
    }

    async fn visible_plugin_registry_owns_schema(
        &mut self,
        branch_id: &str,
        schema_key: &str,
    ) -> Result<bool, LixError> {
        let registry_rows = self
            .load_visible_exact_hot_state_batch(&HotStateExactBatchRequest {
                rows: vec![HotStateExactRowRequest {
                    schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
                    branch_id: branch_id.to_owned(),
                    row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
                    file_id: None,
                }],
                projection: plugin_registry_hot_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        Ok(
            PluginRegistry::from_optional_hot_state_row(registry_rows.row(0), branch_id)?
                .owns_schema(schema_key),
        )
    }

    /// Drops `format-only` upserts that are semantically identical to the
    /// currently accepted durable row. The exact-row lookup keeps this
    /// proportional to the sparse format-only output instead of hydrating the
    /// complete file graph.
    async fn suppress_format_only_noops(
        &mut self,
        plugin: &PluginRegistryEntry,
        changes: WasmHostRowChanges,
        file_key: &PluginFileWriteKey,
    ) -> Result<(WasmHostRowChanges, BTreeSet<WasmRowKey>), LixError> {
        let format_only_keys = changes
            .changes
            .iter()
            .filter_map(|change| match change {
                WasmRowChange::Upsert {
                    row,
                    effect: WasmChangeEffect::FormatOnly,
                } => Some(row.key.clone()),
                WasmRowChange::Create { .. }
                | WasmRowChange::Upsert { .. }
                | WasmRowChange::Delete(_) => None,
            })
            .collect::<Vec<_>>();
        if format_only_keys.is_empty() {
            return Ok((changes, BTreeSet::new()));
        }

        let requests = format_only_keys
            .iter()
            .map(|key| {
                Ok(HotStateExactRowRequest {
                    schema_key: key.schema_key.to_string(),
                    branch_id: file_key.branch_id.clone(),
                    row_pk: plugin_row_pk(plugin, key)?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let current = self
            .load_visible_exact_hot_state_batch(&HotStateExactBatchRequest {
                rows: requests,
                projection: plugin_state_hot_state_projection(),
                untracked: Some(file_key.untracked),
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
            suppress_format_only_noops_against_batch(changes, &format_only_keys, &current)?,
            observed_existing,
        ))
    }

    /// Materializes keyless creates and returns the one durable mutation
    /// reservation row when this transition creates at least one row.
    /// Existing keyed updates are checked with exact sparse authority reads.
    async fn v2_create_rows(
        &mut self,
        plugin: &PluginRegistryEntry,
        changes: &mut WasmHostRowChanges,
        bound: BoundCreateContext,
        file_key: &PluginFileWriteKey,
        existing_reservation: Option<&MaterializedHotStateRow>,
        known_existing_authorities: Option<&PluginRowAuthorities>,
    ) -> Result<RawWriteBatch, LixError> {
        let mut validation = validate_create_changes(plugin, changes)?;
        if let Some(known) = known_existing_authorities {
            validation
                .existing_authorities
                .retain(|key| !known.contains(key));
        }
        materialize_keyless_creates(changes, bound.creates(), &self.sql_schema_snapshot)?;
        if !validation.requires_reservation && validation.existing_authorities.is_empty() {
            return Ok(RawWriteBatch::new());
        }

        let exact_rows = validation
            .existing_authorities
            .iter()
            .map(|key| {
                Ok(HotStateExactRowRequest {
                    schema_key: key.schema_key.to_string(),
                    branch_id: file_key.branch_id.clone(),
                    row_pk: plugin_row_pk(plugin, key)?,
                    file_id: Some(file_key.file_id.clone()),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let loaded = if exact_rows.is_empty() {
            MaterializedHotStateExactBatch::default()
        } else {
            self.load_visible_exact_hot_state_batch(&HotStateExactBatchRequest {
                rows: exact_rows,
                projection: plugin_state_hot_state_projection(),
                untracked: Some(file_key.untracked),
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
            file_key.untracked,
        )?;

        let mut rows = RawWriteBatch::with_capacity(usize::from(validation.requires_reservation));
        if validation.requires_reservation {
            if let Some(row) = reserve_create_row(
                existing_reservation,
                bound,
                &file_key.file_id,
                &file_key.branch_id,
                file_key.untracked,
            )? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn preflight_create(
        &mut self,
        bound: BoundCreateContext,
        file_key: &PluginFileWriteKey,
    ) -> Result<Option<MaterializedHotStateRow>, LixError> {
        self.preflight_creates(&[(bound, file_key.clone())])
            .await
            .map(|mut rows| rows.pop().expect("one preflight produces one result"))
    }

    async fn preflight_creates(
        &mut self,
        requests: &[(BoundCreateContext, PluginFileWriteKey)],
    ) -> Result<Vec<Option<MaterializedHotStateRow>>, LixError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        // Lane is a property of each requested file, but `HotStateExactBatchRequest`
        // carries one lane for the whole batch. Both lanes now reach plugin
        // reconciliation, so the batch is partitioned by lane and the results are
        // scattered back into caller order. Reading every reservation under a
        // single constant lane would miss an existing reservation in the other
        // lane and silently re-reserve over it.
        let mut existing_rows: Vec<Option<MaterializedHotStateRow>> =
            (0..requests.len()).map(|_| None).collect();
        for lane in [false, true] {
            let slots = requests
                .iter()
                .enumerate()
                .filter(|(_, (_, file_key))| file_key.untracked == lane)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if slots.is_empty() {
                continue;
            }
            let loaded = self
                .load_visible_exact_hot_state_batch(&HotStateExactBatchRequest {
                    rows: slots
                        .iter()
                        .map(|&index| {
                            let (bound, file_key) = &requests[index];
                            HotStateExactRowRequest {
                                schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                                branch_id: file_key.branch_id.clone(),
                                row_pk: RowPk::single(bound.reservation_key()),
                                file_id: Some(file_key.file_id.clone()),
                            }
                        })
                        .collect(),
                    projection: plugin_state_hot_state_projection(),
                    untracked: Some(lane),
                    include_tombstones: false,
                })
                .await?;
            for (slot, &index) in slots.iter().enumerate() {
                existing_rows[index] = loaded.row(slot).map(MaterializedHotStateRowRef::to_owned);
            }
        }
        for (index, (bound, file_key)) in requests.iter().enumerate() {
            validate_create_reservation(
                existing_rows[index].as_ref(),
                *bound,
                &file_key.file_id,
                &file_key.branch_id,
                file_key.untracked,
            )?;
        }
        Ok(existing_rows)
    }

    async fn v2_id_reservation_tombstones(
        &mut self,
        file_key: &PluginFileWriteKey,
    ) -> Result<RawWriteBatch, LixError> {
        let rows = self
            .scan_visible_hot_state_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                    branch_ids: vec![file_key.branch_id.clone()],
                    file_ids: vec![NullableKeyFilter::Value(file_key.file_id.clone())],
                    untracked: Some(file_key.untracked),
                    ..Default::default()
                },
                projection: plugin_registry_hot_state_projection(),
                ..Default::default()
            })
            .await?;
        let mut tombstones = RawWriteBatch::with_capacity(rows.len());
        for row in rows.iter() {
            let Ok(key) = row.row_pk().as_single_string() else {
                continue;
            };
            if is_reservation_key(key) {
                tombstones.push(reservation_tombstone_row(
                    key,
                    &file_key.file_id,
                    &file_key.branch_id,
                    file_key.untracked,
                )?);
            }
        }
        Ok(tombstones)
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
                let mut file_content = Vec::new();
                let mut reconciliation =
                    Box::pin(self.plugin_write_reconciliation(&mut rows, &mut file_content))
                        .await?;
                let mut rows = reconciliation.take_reconciled_rows(rows);
                for (file_key, version) in &reconciliation.materialization_versions {
                    let mut materialization_rows = RawWriteBatch::new();
                    let materialized_row_index = materialization_rows.len();
                    let payload = file_content
                        .iter()
                        .find(|write| PluginFileWriteKey::from(*write) == *file_key)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "component semantic materialization payload for file '{}' is missing",
                                    file_key.file_id
                                ),
                            )
                        })?;
                    BlobRefRowInput {
                        file_id: file_key.file_id.clone(),
                        blob_hash: payload.blob_hash().unwrap_or_else(|| {
                            BlobId::from_content(
                                payload
                                    .inline_data()
                                    .expect("plugin materializations require inline file content"),
                            )
                        }),
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
                    materialization_rows.set_change_id(
                        materialized_row_index,
                        Some(SharedStr::from(version.clone())),
                    );
                    mark_plugin_reconciliation_batch(&mut materialization_rows, 0)?;
                    rows.append_raw_batch(materialization_rows);
                }
                let write = if file_content.is_empty() {
                    ReconciledTransactionWrite::Rows { mode, rows }
                } else {
                    ReconciledTransactionWrite::RowsWithFileContent {
                        mode,
                        rows,
                        file_content,
                        count,
                    }
                };
                Ok((
                    write,
                    reconciliation.file_view_mutations,
                    reconciliation.actor_publications,
                ))
            }
            TransactionWrite::RowsWithFileContent {
                mode,
                rows,
                mut file_content,
                count,
            } => {
                let mut rows = rows;
                reject_external_plugin_registry_rows(&rows)?;
                let mut reconciliation = Box::pin(
                    self.plugin_write_reconciliation(&mut rows, &mut file_content)
                        .instrument(tracing::debug_span!(
                            target: "lix_perf",
                            "lix.perf.plugin_reconciliation"
                        )),
                )
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
                    let mut materialization_rows = RawWriteBatch::new();
                    let materialized_row_index = materialization_rows.len();
                    let payload = file_content
                        .iter()
                        .find(|write| PluginFileWriteKey::from(*write) == *file_key)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "component materialization payload for file '{}' is missing",
                                    file_key.file_id
                                ),
                            )
                        })?;
                    BlobRefRowInput {
                        file_id: file_key.file_id.clone(),
                        blob_hash: payload.blob_hash().unwrap_or_else(|| {
                            BlobId::from_content(
                                payload
                                    .inline_data()
                                    .expect("plugin materializations require inline file content"),
                            )
                        }),
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
                    materialization_rows.set_change_id(
                        materialized_row_index,
                        Some(SharedStr::from(version.clone())),
                    );
                    mark_plugin_reconciliation_batch(&mut materialization_rows, 0)?;
                    rows.append_raw_batch(materialization_rows);
                }
                let file_content = file_content
                    .into_iter()
                    .filter(|write| {
                        let key = PluginFileWriteKey::from(write);
                        !reconciliation.file_keys.contains(&key)
                            && (!write.is_empty()
                                || reconciliation.materialized_file_keys.contains(&key))
                    })
                    .collect();
                Ok((
                    ReconciledTransactionWrite::RowsWithFileContent {
                        mode,
                        rows,
                        file_content,
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
        file_content: &mut Vec<TransactionFileContent>,
    ) -> Result<PluginWriteReconciliation, LixError> {
        let input_row_count = rows.len();
        let mut reconciliation = PluginWriteReconciliation::default();
        let mut lifecycle = BTreeMap::<PluginLifecycleKey, Option<PluginRegistryEntry>>::new();
        let mut lifecycle_schema_keys = Vec::<PluginLifecycleKey>::new();
        let mut lifecycle_schema_rows = RawWriteBatch::new();
        let mut current_install_schema_definitions =
            BTreeMap::<PluginLifecycleKey, BTreeMap<String, JsonValue>>::new();
        let mut current_install_wasm = BTreeMap::<BlobId, Vec<u8>>::new();
        let mut branch_ids = BTreeSet::<String>::new();

        // Parse each archive exactly once. The original ZIP remains the file
        // payload; the extracted component is staged as a second CAS payload.
        for write in file_content.iter_mut() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            let Some(data) = write.inline_data() else {
                if is_plugin_storage_path(path) {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "prepared CAS content cannot install a plugin archive",
                    ));
                }
                if !write.global {
                    branch_ids.insert(write.branch_id.clone());
                }
                continue;
            };
            if !is_plugin_storage_path(path) {
                if !write.global {
                    branch_ids.insert(write.branch_id.clone());
                }
                continue;
            }
            let plan = plugin_install_plan_from_archive_path(
                path,
                data,
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
                runtime: crate::plugin::runtime::PluginRuntime::WasmComponent,
                api_version: WASM_COMPONENT_API_VERSION.to_owned(),
                capabilities: parsed.capabilities,
                path_glob: parsed
                    .manifest
                    .file_match
                    .as_ref()
                    .map(|matcher| matcher.path_glob.clone()),
                content: parsed
                    .manifest
                    .file_match
                    .as_ref()
                    .and_then(|matcher| matcher.content),
                entry: parsed.manifest.entry.clone(),
                schema_keys: parsed.schema_keys.clone(),
                create_schema_keys: parsed.create_schema_keys.clone(),
                manifest_json: parsed.normalized_manifest_json.clone(),
                archive_file_id,
                archive_path: path.to_string(),
                archive_blob_hash: archive_blob_hash.to_hex(),
                wasm_blob_hash: parsed.wasm_hash.map(BlobId::to_hex),
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
                        .row_pk
                        .as_ref()
                        .and_then(|row_pk| row_pk.as_single_string().ok())
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin schema row has an invalid identity",
                            )
                        })?
                        .to_string();
                    let snapshot = row.snapshot_json().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "plugin schema row is missing its snapshot",
                        )
                    })?;
                    let (snapshot_key, definition) =
                        crate::schema::schema_from_registered_snapshot(snapshot)?;
                    if snapshot_key.schema_key != schema_key {
                        return Err(LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            "plugin schema row identity does not match schema_key",
                        ));
                    }
                    Ok((schema_key, definition))
                })
                .collect::<Result<BTreeMap<_, _>, LixError>>()?;
            current_install_schema_definitions.insert(lifecycle_key.clone(), schema_definitions);
            if let (Some(wasm_hash), Some(wasm_bytes)) = (parsed.wasm_hash, parsed.wasm_bytes) {
                current_install_wasm
                    .entry(wasm_hash)
                    .or_insert_with(|| wasm_bytes.clone());
                write.add_auxiliary_payload(wasm_bytes);
            }
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
                .row_pk
                .as_ref()
                .and_then(|row_pk| row_pk.as_single_string_owned().ok())
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
            if row.global {
                continue;
            }
            // Deleting an untracked plugin-owned file must clean up its owner
            // and rows the same way a tracked deletion does.
            let key = PluginFileWriteKey {
                branch_id: row.branch_id.to_string(),
                global: false,
                untracked: row.untracked,
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

        // Ordinary semantic DML carries no filesystem payload. Every
        // branch-local row still needs the small registry lookup: file-backed
        // plugin rows may require a row-to-file transition, while fileless
        // plugin tables must enter the native typed payload lane before
        // transaction normalization. Lane is irrelevant because the registry
        // is branch-global.
        for row in rows.iter().take(input_row_count) {
            if !row.global {
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
        let base = self.hot_state.reader(read.clone());

        if !lifecycle_schema_rows.is_empty() {
            let mut desired_schemas = BTreeMap::<(String, RowPk), (String, JsonValue)>::new();
            for (lifecycle_key, row) in lifecycle_schema_keys
                .iter()
                .zip(lifecycle_schema_rows.iter())
            {
                let row_pk = row.row_pk.cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its row identity",
                    )
                })?;
                let snapshot = row.snapshot_json().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin schema row is missing its definition",
                    )
                })?;
                let identity = (row.branch_id.to_string(), row_pk);
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
                &HotStateScanRequest {
                    filter: HotStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        row_pks: desired_schemas
                            .keys()
                            .map(|(_, row_pk)| row_pk.clone())
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
                    projection: plugin_registry_hot_state_projection(),
                    ..Default::default()
                },
            )
            .await?;
            let mut existing_schemas = BTreeMap::<(String, RowPk), JsonValue>::new();
            for row in schema_rows.iter() {
                if row.deleted() {
                    continue;
                }
                let snapshot = row.snapshot_json_value()?.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "live registered schema row must have exactly one payload",
                    )
                })?;
                existing_schemas.insert(
                    (row.branch_id().to_string(), row.row_pk().clone()),
                    snapshot,
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
                let Some(row_pk) = row.row_pk.cloned() else {
                    continue;
                };
                let identity = (row.branch_id.to_string(), row_pk);
                if !desired_schemas.contains_key(&identity) {
                    continue;
                }
                match row.snapshot_json() {
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
            &HotStateExactBatchRequest {
                rows: branch_ids
                    .iter()
                    .map(|branch_id| HotStateExactRowRequest {
                        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                        branch_id: branch_id.clone(),
                        row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
                        file_id: None,
                    })
                    .collect(),
                projection: plugin_registry_hot_state_projection(),
                untracked: Some(false),
                include_tombstones: false,
            },
        )
        .await?;
        let mut registries = BTreeMap::<String, PluginRegistry>::new();
        let mut changed_registry_branches = BTreeSet::<String>::new();
        let mut generation_upgrades = Vec::<PluginGenerationUpgrade>::new();
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
            let row = registry_rows.row(slot);
            let change_id = row.and_then(MaterializedHotStateRowRef::change_id);
            let change_id_text = change_id.map(|change_id| change_id.to_string());
            let registry = match change_id
                .map(|change_id| {
                    self.plugin_host
                        .cached_plugin_registry(branch_id, &change_id.to_string())
                })
                .transpose()?
                .flatten()
            {
                Some(registry) => registry,
                None => {
                    let registry = PluginRegistry::from_optional_hot_state_row(row, branch_id)?;
                    if let Some(change_id) = change_id_text.as_deref() {
                        self.plugin_host
                            .cache_plugin_registry(branch_id, change_id, &registry)?;
                    }
                    registry
                }
            };
            registries.insert(branch_id.clone(), registry);
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
        let plugin_owned_row_indices = rows
            .iter()
            .take(input_row_count)
            .enumerate()
            .filter_map(|(index, row)| {
                (!row.global
                    && registries
                        .get(row.branch_id.as_str())
                        .is_some_and(|registry| registry.owns_schema(row.schema_key.as_str())))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        for index in plugin_owned_row_indices {
            rows.mark_plugin_owned(index);
        }
        for row in rows.iter().take(input_row_count) {
            if row.schema_key != REGISTERED_SCHEMA_KEY || row.global || row.untracked {
                continue;
            }
            let Some(schema_key) = row
                .row_pk
                .as_ref()
                .and_then(|row_pk| row_pk.as_single_string().ok())
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
            preflight_owned_generation_upgrades(
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
            for write in file_content.iter().filter(|write| {
                !write.global
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
        for write in file_content.iter() {
            if write.global
                || !active_branch_ids.contains(&write.branch_id)
                || write.path.as_deref().is_none_or(is_plugin_storage_path)
            {
                continue;
            }
            if write.inline_data().is_none() {
                continue;
            }
            candidate_file_keys.insert(PluginFileWriteKey::from(write));
        }
        for key in deleted_file_keys.keys() {
            candidate_file_keys.insert(key.clone());
        }
        for row in rows.iter().take(input_row_count) {
            if row.global
                || !active_branch_ids.contains(row.branch_id.as_str())
                || row.file_id.is_none()
            {
                continue;
            }
            candidate_file_keys.insert(PluginFileWriteKey {
                branch_id: row.branch_id.to_string(),
                global: false,
                untracked: row.untracked,
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

        // Owner rows are file-scoped, so each one lives in its own file's lane.
        // `HotStateExactBatchRequest` carries one lane per batch, so the
        // candidates are partitioned by lane and each partition is read under
        // its own lane. A single constant lane here would report an untracked
        // file as unowned and re-own it from scratch on every write.
        let mut owners = BTreeMap::<PluginFileWriteKey, PluginFileOwner>::new();
        let mut owner_change_ids = BTreeMap::<PluginFileWriteKey, String>::new();
        for lane in [false, true] {
            let lane_keys = candidate_file_keys
                .iter()
                .filter(|key| key.untracked == lane)
                .collect::<Vec<_>>();
            if lane_keys.is_empty() {
                continue;
            }
            let owner_rows = overlay_load_exact_batch(
                &base,
                &staged,
                &HotStateExactBatchRequest {
                    rows: lane_keys
                        .iter()
                        .map(|key| HotStateExactRowRequest {
                            schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                            branch_id: key.branch_id.clone(),
                            row_pk: RowPk::single(PLUGIN_OWNER_KEY),
                            file_id: Some(key.file_id.clone()),
                        })
                        .collect(),
                    projection: plugin_registry_hot_state_projection(),
                    untracked: Some(lane),
                    include_tombstones: false,
                },
            )
            .await?;
            for row in (0..owner_rows.len()).filter_map(|slot| owner_rows.row(slot)) {
                let branch_id = row.branch_id().to_string();
                let owner_row = row.to_owned();
                let Some(owner) =
                    PluginFileOwner::from_hot_state_row(&owner_row, &branch_id, lane)?
                else {
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
                    untracked: lane,
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

        let file_content_keys = file_content
            .iter()
            .map(PluginFileWriteKey::from)
            .collect::<BTreeSet<_>>();
        let mut unresolved_semantic_groups =
            BTreeMap::<PluginFileWriteKey, (PluginRegistryEntry, String, Vec<usize>)>::new();
        for (row_index, row) in rows.iter().take(input_row_count).enumerate() {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            if row.global || !active_branch_ids.contains(row.branch_id.as_str()) {
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
                untracked: row.untracked,
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
            if file_content_keys.contains(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot mutate both bytes and semantic rows for component plugin file '{file_id}'"
                    ),
                )
                .with_hint("submit either the byte mutation or the resolved row mutations"));
            }
            if deleted_file_keys.contains_key(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot delete component plugin file '{file_id}' and mutate its semantic rows"
                    ),
                ));
            }
            let owner_change_id = owner_change_ids.get(&file_key).cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "durable component plugin owner for file '{file_id}' is missing its incarnation"
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

        let mut semantic_groups = BTreeMap::<PluginFileWriteKey, PluginSemanticWriteGroup>::new();
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
                            // Path uniqueness is per lane, so the descriptor is
                            // matched in the same lane as its semantic rows.
                            && live.untracked == file_key.untracked
                    })
                    .collect::<Vec<_>>();
                let [entry] = entries.as_slice() else {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "owned component plugin file '{}' must resolve to exactly one path in its own lane; found {}",
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
                            "owned component plugin '{}' no longer matches file path '{}'",
                            plugin.key(),
                            entry.path
                        ),
                    ));
                }
                semantic_groups.insert(
                    file_key,
                    PluginSemanticWriteGroup {
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
        for write in file_content.iter() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            if write.global
                || is_plugin_storage_path(path)
                || !active_branch_ids.contains(&write.branch_id)
            {
                continue;
            }
            let Some(write_data) = write.inline_data() else {
                continue;
            };
            let file_key = PluginFileWriteKey::from(write);
            let catalog = catalogs
                .get(&write.branch_id)
                .expect("active plugin branch should have a compiled catalog");
            let registry = registries
                .get(&write.branch_id)
                .expect("active plugin branch should have a registry");

            // A warm component actor already carries an exact, generation-bound
            // selection. Reuse it only while every matcher-relevant identity
            // is unchanged. Content predicates have separate
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
                let content_still_matches = match plugin.content() {
                    None => true,
                    Some(PluginContentMatcher::Text) => {
                        write.splice_provenance().is_some_and(|provenance| {
                            observation.bytes_sha256().is_some_and(|digest| {
                                digest.matches_lower_hex(provenance.base_sha256())
                            }) && transport_splice_preserves_utf8(write_data, provenance)
                        })
                    }
                    Some(PluginContentMatcher::PrefixExcludes {
                        byte,
                        bytes: scan_bytes,
                    }) => write.splice_provenance().is_some_and(|provenance| {
                        observation.bytes_sha256().is_some_and(|digest| {
                            digest.matches_lower_hex(provenance.base_sha256())
                        }) && transport_splice_preserves_prefix_exclusion(
                            write_data, provenance, byte, scan_bytes,
                        )
                    }),
                    Some(PluginContentMatcher::Binary) => false,
                };
                content_still_matches.then_some(plugin)
            });

            let (plugin, classified_bytes) = warm_owned_plugin.map_or_else(
                || catalog.select_for_bytes_with_classification_work(path, write_data),
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
            // to persist one historical tombstone per semantic row.
            if deleted_file_keys.contains_key(key) {
                continue;
            }
            let selected = selected_plugins.get(key);
            let semantic = semantic_groups.get(key).map(|group| &group.plugin);
            // A same-owner component write is authorized by an exact document
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
                // Each scan below reads one lane, so groups are keyed by lane
                // too. Merging lanes into one group would read a file's rows
                // from the wrong lane and tombstone the wrong rows.
                untracked: key.untracked,
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
        let mut state_batches = Vec::<MaterializedHotStateBatch>::with_capacity(state_groups.len());
        let mut state_group_keys = Vec::<PluginStateGroupKey>::with_capacity(state_groups.len());
        for (group_key, group) in state_groups {
            let rows = overlay_scan_batch(
                &base,
                &staged,
                &HotStateScanRequest {
                    filter: HotStateFilter {
                        schema_keys: group.schema_keys.into_iter().collect(),
                        branch_ids: vec![group_key.branch_id.clone()],
                        file_ids: group
                            .file_ids
                            .iter()
                            .cloned()
                            .map(NullableKeyFilter::Value)
                            .collect(),
                        untracked: Some(group_key.untracked),
                        ..Default::default()
                    },
                    projection: plugin_state_hot_state_projection(),
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
            .map(MaterializedHotStateBatch::len)
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
        let mut component_factories =
            BTreeMap::<PluginBranchEntryKey, Arc<dyn WasmComponentFactory>>::new();
        let mut cold_entries = BTreeMap::<PluginBranchEntryKey, PluginRegistryEntry>::new();
        for (key, entry) in selected_entries {
            let hash = BlobId::from_hex(entry.wasm_blob_hash().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("plugin '{}' has no executable component", entry.key()),
                )
            })?)?;
            let cached_factory = self.plugin_host.cached_plugin_factory(entry.key(), hash)?;
            if let Some(factory) = cached_factory {
                component_factories.insert(key, factory);
            } else {
                cold_entries.insert(key, entry);
            }
        }

        let mut wasm_by_hash = current_install_wasm;
        let mut missing_hashes = Vec::<BlobId>::new();
        for entry in cold_entries.values() {
            let hash = BlobId::from_hex(entry.wasm_blob_hash().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("plugin '{}' has no executable component", entry.key()),
                )
            })?)?;
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
            let hash = BlobId::from_hex(entry.wasm_blob_hash().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("plugin '{}' has no executable component", entry.key()),
                )
            })?)?;
            let wasm = wasm_by_hash.get(&hash).cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "plugin registry references unavailable WASM blob '{}'",
                        hash.to_hex()
                    ),
                )
            })?;
            let plugin = entry.to_installed_plugin(Some(wasm))?;
            let factory = self
                .plugin_host
                .load_or_compile_factory(&plugin)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_factory_compile"
                ))
                .await?;
            component_factories.insert(key, factory);
        }

        let mut reconciled_file_keys = BTreeSet::<PluginFileWriteKey>::new();
        let fresh_file_indices = file_content
            .iter()
            .enumerate()
            .filter_map(|(index, write)| {
                let path = write.path.as_deref()?;
                if write.global
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
                let write = &file_content[file_index];
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
                let factory = component_factories
                    .get(&installed_key)
                    .expect("selected component plugin should have a compiled factory")
                    .clone();
                let desired_owner =
                    PluginFileOwner::from_registry_entry(write.file_id.clone(), &selected)?;
                let owner_change_id = self.functions.call_uuid_v7().to_string();
                let mut owner_row = desired_owner.write_row(&write.branch_id, write.untracked)?;
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
                    // Arena v3 retains only host-owned immutable roots after
                    // import; keeping the actor enables >8 MiB warm successors
                    // without retaining the transient guest parser graph.
                    retain_large_import_actor: retain_large_import_actor(&selected),
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
                            "one transaction cannot transition component plugin file '{}' more than once",
                            write.file_id
                        ),
                    )
                    .with_hint("combine the byte edits into one file update"));
                }

                let descriptor = v2_file_descriptor(write, &selected);
                let schemas = SchemaAllowlist::from_catalog(
                    selected.schema_keys(),
                    Arc::clone(&self.sql_schema_snapshot),
                )?;
                let mutation_identity = write.mutation_identity().unwrap_or_else(|| {
                    local_mutation_identity(self.functions.call_uuid_v7().into_bytes())
                });
                let create_context = BoundCreateContext::bind(mutation_identity, &actor_key)?;
                let materialization_version = self.functions.call_uuid_v7().to_string();
                let submitted_bytes = write
                    .inline_payload()
                    .expect("selected plugin writes require inline content")
                    .shared_bytes();
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
            let existing_rows = match self.preflight_creates(&preflight_requests).await {
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
                            creates,
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

            for (pending, mut actor, validated) in completed_opens {
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
                let row_authorities = plugin_row_authorities_from_transition(
                    &changes,
                    pending.create_context.creates(),
                );
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&pending.file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes =
                    u64::try_from(changes.row_change_count()).unwrap_or(u64::MAX);
                self.plugin_host.record_transition_counters(counters);

                rows.push(pending.owner_row);
                rows.append(create_rows);
                let write = &mut file_content[pending.file_index];
                let context = FilesystemRowContext {
                    branch_id: write.branch_id.clone(),
                    global: false,
                    untracked: write.untracked,
                    file_id: None,
                    metadata: None,
                };
                append_plugin_change_rows(
                    rows,
                    &pending.selected,
                    changes,
                    &write.file_id,
                    &context,
                )?;
                reconciliation
                    .materialized_file_keys
                    .insert(pending.file_key.clone());
                reconciliation.materialization_versions.insert(
                    pending.file_key.clone(),
                    pending.materialization_version.clone(),
                );
                reconciliation
                    .actor_publications
                    .push(PendingPluginActorPublication::New {
                        cache: self.plugin_host.actor_cache(),
                        key: pending.actor_key,
                        checkpoint: actor.checkpoint_document(validated.document).await?,
                        store: PluginActorStore::new(actor, pending.store_permit),
                        document: validated.document,
                        bytes: pending.submitted_bytes,
                        semantic_root: Arc::from(pending.materialization_version),
                        row_authorities,
                        view: pending.view,
                    });
                reconciled_file_keys.insert(pending.file_key);
            }
        }

        for write in file_content.iter_mut() {
            let Some(path) = write.path.as_deref() else {
                continue;
            };
            if write.global
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
                untracked: write.untracked,
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
                                .then_with(|| group.untracked.cmp(&write.untracked))
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
                        write.untracked,
                    )?);
                }
                reconciled_file_keys.insert(file_key);
                continue;
            };
            let installed_key = PluginBranchEntryKey {
                branch_id: write.branch_id.clone(),
                plugin_key: selected.key().to_string(),
            };
            let factory = component_factories
                .get(&installed_key)
                .expect("selected component plugin should have a compiled factory")
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
                let mut owner_row = desired_owner.write_row(&write.branch_id, write.untracked)?;
                owner_row.change_id = Some(owner_change_id.clone());
                rows.push(owner_row);
                owner_change_id
            } else {
                current_owner_change_id.clone().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "durable component plugin owner for file '{}' is missing its incarnation",
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
                retain_large_import_actor: retain_large_import_actor(selected),
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
                        "one transaction cannot transition component plugin file '{}' more than once",
                        write.file_id
                    ),
                )
                .with_hint("combine the byte edits into one file update"));
            }
            let descriptor = v2_file_descriptor(write, selected);
            let schemas = SchemaAllowlist::from_catalog(
                selected.schema_keys(),
                Arc::clone(&self.sql_schema_snapshot),
            )?;
            let mutation_identity = write.mutation_identity().unwrap_or_else(|| {
                local_mutation_identity(self.functions.call_uuid_v7().into_bytes())
            });
            let create_context = BoundCreateContext::bind(mutation_identity, &actor_key)?;
            let creates = create_context.creates();
            let existing_create_reservation =
                match self.preflight_create(create_context, &file_key).await {
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
            let submitted_bytes = write
                .inline_payload()
                .expect("selected plugin writes require inline content")
                .shared_bytes();
            let limits = WasmTransitionLimits::for_file_bytes(
                u64::try_from(submitted_bytes.len()).unwrap_or(u64::MAX),
            );
            let mut verified_same_length_blob_splice = None;
            let mut verified_blob_edit_splice = None;

            let (changes, publication, materialized_bytes, create_rows) = if same_plugin_owner {
                'same_owner: {
                    let acknowledged_view = self.acknowledged_session_plugin_view(
                        &view.session_key,
                        selected,
                        current_owner_change_id
                            .as_deref()
                            .expect("same-owner component file should have an owner incarnation"),
                    );
                    let cache = self.plugin_host.actor_cache().clone();
                    let acknowledged_observation = acknowledged_view
                        .as_ref()
                        .and_then(|view| view.observation.as_ref());
                    let cold_successor_candidate = match acknowledged_view.as_ref() {
                        Some(_) => acknowledged_observation
                            .is_none_or(|observation| !cache.contains_observation(observation)),
                        None => {
                            !self
                                .pending_file_view_mutations
                                .contains_key(&view.session_key)
                                && !self
                                    .session_file_views
                                    .has_plugin_file_at_path(&actor_key.branch_id, &actor_key.path)
                        }
                    };
                    if cold_successor_candidate {
                        let cold_limits = cold_successor_transition_limits(submitted_bytes.len());
                        let cold_before_descriptor = acknowledged_observation
                            .map(|observation| v2_file_descriptor_from_actor_key(observation.key()))
                            .unwrap_or_else(|| v2_file_descriptor_from_actor_key(&actor_key));
                        let cold_open_guard = cache.cold_open_guard().await;
                        let visible_materialization = self
                        .visible_materialization(&file_key)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                                "the component file no longer has a visible materialization root",
                            )
                        })?;
                        let observation_matches_visible_root =
                            acknowledged_observation.is_none_or(|observation| {
                                observation.semantic_root() == visible_materialization.semantic_root
                            });
                        let cold_open = cache
                            .prepare_cold_open(&actor_key, &visible_materialization.semantic_root)
                            .await?;
                        if let PluginActorColdOpen::Build(mut cold_install) = cold_open
                            && observation_matches_visible_root
                        {
                            let staged = self.staged_writes.staging_overlay()?;
                            let read = SharedStorageAdapterRead::new(
                                self.storage
                                    .begin_read(StorageReadOptions::default())
                                    .await?,
                            );
                            let base = self.hot_state.reader(read.clone());
                            let (
                                cold_before,
                                cold_edits,
                                host_full_diff_bytes_compared,
                                same_length_blob_splice,
                                blob_edit_splice,
                            ) = {
                                let VisibleMaterializationBytes::Blob { hash } =
                                    visible_materialization.bytes;
                                let before_bytes: crate::Blob =
                                        load_transaction_blob_bytes(
                                            &self.binary_cas.reader(read.clone()),
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
                                                    "owned component plugin file '{}' references missing materialized blob '{}'",
                                                    actor_key.file_id,
                                                    hash.to_hex()
                                                ),
                                            )
                                        })?
                                        .into();
                                let built_splices = tracing::debug_span!(
                                    target: "lix_perf",
                                    "lix.perf.plugin_splice_discovery"
                                )
                                .in_scope(|| {
                                    build_file_update_splices(
                                        &before_bytes,
                                        Some(FileBytesSha256::compute(&before_bytes)),
                                        write.inline_data().expect(
                                            "selected plugin writes require inline content",
                                        ),
                                        write.splice_provenance(),
                                        cold_limits,
                                    )
                                })?;
                                let same_length_blob_splice = built_splices
                                    .same_length_replacement()
                                    .map(|(offset, length)| (hash, offset, length));
                                let blob_edit_splice = built_splices.replacement().map(
                                    |(offset, delete_len, insert_len)| {
                                        (hash, offset, delete_len, insert_len)
                                    },
                                );
                                let before_source: Arc<dyn crate::plugin::runtime::WasmByteSource> =
                                    Arc::new(ArcByteSource::new(before_bytes.clone()));
                                (
                                    Some(before_source),
                                    built_splices.edits,
                                    built_splices.full_diff_bytes_compared,
                                    same_length_blob_splice,
                                    blob_edit_splice,
                                )
                            };
                            let decoded_checkpoint = cold_before.as_ref().and_then(|_| {
                                cache.checkpoint(&actor_key, &visible_materialization.semantic_root)
                            });
                            let store_permit = loop {
                                match cache.admit_cold_store(&mut cold_install) {
                                    Ok(permit) => break permit,
                                    Err(error)
                                        if error.code == LixError::CODE_PLUGIN_RESOURCE_LIMIT =>
                                    {
                                        if retire_oldest_completed_actor(
                                            &mut reconciliation.actor_publications,
                                        )
                                        .await
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
                            let mut actor = factory
                                .instantiate_actor()
                                .instrument(tracing::debug_span!(
                                    target: "lix_perf",
                                    "lix.perf.plugin_actor_instantiate"
                                ))
                                .await?;
                            let restored_checkpoint = decoded_checkpoint.is_some();
                            let rows = overlay_scan_batch(
                                &base,
                                &staged,
                                &HotStateScanRequest {
                                    filter: HotStateFilter {
                                        schema_keys: selected.schema_keys().to_vec(),
                                        branch_ids: vec![actor_key.branch_id.clone()],
                                        file_ids: vec![NullableKeyFilter::Value(
                                            actor_key.file_id.clone(),
                                        )],
                                        untracked: Some(file_key.untracked),
                                        ..Default::default()
                                    },
                                    projection: plugin_state_hot_state_projection(),
                                    ..Default::default()
                                },
                            )
                            .await?;
                            let row_ordinals = v2_host_row_ordinals_from_live_batch(
                                &rows,
                                &file_key,
                                selected.schema_keys(),
                                &self.sql_schema_snapshot,
                            )?;
                            let row_count = row_ordinals.len();
                            let cold_base_authorities =
                                plugin_row_authorities_from_live_batch(&rows, &row_ordinals)?;
                            let row_source: Box<dyn crate::plugin::runtime::WasmRowSource> =
                                Box::new(LiveBatchRowSource::new_typed(
                                    rows,
                                    row_ordinals,
                                    cold_limits,
                                )?);
                            let mut row_source = Some(row_source);
                            drop(base);
                            drop(read);
                            let transition_result = if let Some(checkpoint) = decoded_checkpoint {
                                let document = actor.restore_document(&checkpoint).await?;
                                actor
                                    .file_changed(
                                        document,
                                        cold_limits,
                                        WasmFileUpdate {
                                            before_descriptor: cold_before_descriptor,
                                            after_descriptor: descriptor.clone(),
                                            before: cold_before.expect(
                                                "decoded checkpoints are used only for blob materializations",
                                            ),
                                            edits: cold_edits,
                                            after: Arc::new(ArcByteSource::new(
                                                submitted_bytes.clone(),
                                            )),
                                            creates,
                                            rows: row_source.take(),
                                            prior_row_keys: None,
                                        },
                                    )
                                    .instrument(tracing::debug_span!(
                                        target: "lix_perf",
                                        "lix.perf.plugin_typed_cold_open_file_changed"
                                    ))
                                    .await
                                    .map(|transition| (transition, 0))
                            } else {
                                actor
                                    .cold_file_changed(
                                        cold_limits,
                                        WasmColdFileUpdate {
                                            before_descriptor: cold_before_descriptor,
                                            after_descriptor: descriptor.clone(),
                                            before: cold_before,
                                            edits: cold_edits,
                                            after: Arc::new(ArcByteSource::new(
                                                submitted_bytes.clone(),
                                            )),
                                            creates,
                                            rows: row_source.take().expect(
                                                "cold parse-changes row source is consumed once",
                                            ),
                                        },
                                    )
                                    .instrument(tracing::debug_span!(
                                        target: "lix_perf",
                                        "lix.perf.plugin_cold_file_changed"
                                    ))
                                    .await
                                    .map(|transition| (transition, row_count))
                            };
                            let (transition, row_count) = match transition_result {
                                Ok(transition) => transition,
                                Err(error) => {
                                    let _ = actor.retire().await;
                                    return Err(error);
                                }
                            };
                            let validated = match drain_file_transition_changes(
                                actor.as_mut(),
                                transition,
                                creates,
                                &schemas,
                                cold_limits,
                            )
                            .instrument(tracing::debug_span!(
                                target: "lix_perf",
                                "lix.perf.plugin_cold_drain_changes"
                            ))
                            .await
                            {
                                Ok(validated) => validated,
                                Err(error) => {
                                    let _ = actor.retire().await;
                                    return Err(error);
                                }
                            };
                            let mut changes = validated.changes;
                            let (filtered, _observed_existing_authorities) = self
                                .suppress_format_only_noops(selected, changes, &file_key)
                                .await?;
                            changes = filtered;
                            let create_rows = self
                                .v2_create_rows(
                                    selected,
                                    &mut changes,
                                    create_context,
                                    &file_key,
                                    existing_create_reservation.as_ref(),
                                    Some(&cold_base_authorities),
                                )
                                .await?;
                            let row_authorities = plugin_row_authorities_after_transition(
                                &cold_base_authorities,
                                &changes,
                                validated.replace_all_rows,
                            );
                            let mut counters = validated.counters;
                            counters.host_full_diff_bytes_compared = host_full_diff_bytes_compared;
                            counters.host_content_classification_bytes =
                                content_classification_bytes
                                    .get(&file_key)
                                    .copied()
                                    .unwrap_or(0);
                            counters.full_state_semantic_rows_materialized =
                                u64::try_from(row_count).unwrap_or(u64::MAX);
                            counters.private_document_cache_hits = u64::from(restored_checkpoint);
                            counters.full_document_reparses = u64::from(!restored_checkpoint);
                            counters.full_renderer_invocations = 0;
                            counters.durable_semantic_changes =
                                u64::try_from(changes.row_change_count()).unwrap_or(u64::MAX);
                            self.plugin_host.record_transition_counters(counters);
                            verified_same_length_blob_splice = same_length_blob_splice;
                            verified_blob_edit_splice = blob_edit_splice;
                            drop(cold_open_guard);
                            break 'same_owner (
                                changes,
                                PendingPluginActorPublication::New {
                                    cache,
                                    key: actor_key,
                                    checkpoint: actor
                                        .checkpoint_document(validated.document)
                                        .await?,
                                    store: PluginActorStore::new(actor, store_permit),
                                    document: validated.document,
                                    bytes: submitted_bytes.clone(),
                                    semantic_root: Arc::from(materialization_version.clone()),
                                    row_authorities,
                                    view,
                                },
                                submitted_bytes.clone(),
                                create_rows,
                            );
                        }
                        drop(cold_open_guard);
                    }
                    let observation = match acknowledged_view {
                        Some(view) => match view.observation {
                            Some(observation) => observation,
                            None => {
                                self.cold_open_semantic_actor(
                                    &actor_key,
                                    &file_key,
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
                            let remembered = self
                                .session_file_views
                                .unfiltered_plugin_file_view(&view.session_key);
                            return Err(LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "the session component file view no longer matches this write",
                        )
                        .with_details(serde_json::json!({
                            "branch_id": actor_key.branch_id,
                            "file_id": actor_key.file_id,
                            "path": actor_key.path,
                            "plugin_key": actor_key.plugin_key,
                            "plugin_generation": actor_key.plugin_generation,
                            "owner_change_id": actor_key.owner_change_id,
                            "remembered_view": remembered.as_ref().map(|remembered| serde_json::json!({
                                "path": remembered.path,
                                "plugin_key": remembered.plugin_key,
                                "plugin_generation": remembered.plugin_generation,
                                "owner_change_id": remembered.owner_change_id,
                                "has_observation": remembered.observation.is_some(),
                            })),
                            "pending_view_mutation": self
                                .pending_file_view_mutations
                                .contains_key(&view.session_key),
                        }))
                        .with_hint("read the exact file bytes again before retrying the edit"));
                        }
                        None => {
                            self.cold_open_semantic_actor(
                                &actor_key,
                                &file_key,
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
                            "the acknowledged component file descriptor is not a successor of this write",
                        )
                        .with_details(serde_json::json!({
                            "observed": {
                                "branch_id": observation.key().branch_id,
                                "file_id": observation.key().file_id,
                                "path": observation.key().path,
                                "plugin_key": observation.key().plugin_key,
                                "plugin_generation": observation.key().plugin_generation,
                                "owner_change_id": observation.key().owner_change_id,
                            },
                            "write": {
                                "branch_id": actor_key.branch_id,
                                "file_id": actor_key.file_id,
                                "path": actor_key.path,
                                "plugin_key": actor_key.plugin_key,
                                "plugin_generation": actor_key.plugin_generation,
                                "owner_change_id": actor_key.owner_change_id,
                            },
                        }))
                        .with_hint("read the exact file bytes again before retrying the edit"));
                    }
                    let before_descriptor = v2_file_descriptor_from_actor_key(observation.key());
                    let after_descriptor = descriptor.clone();
                    // Acquire serialization first, reopening a benignly evicted
                    // observation only while its exact durable root is unchanged.
                    // Then read the root again: a second local session may have
                    // committed while this request waited for the actor.
                    let mut lease = self
                        .lease_or_reopen_observed_actor(
                            &observation,
                            &actor_key,
                            &file_key,
                            selected,
                            descriptor.clone(),
                            Arc::clone(&factory),
                            &mut reconciliation.actor_publications,
                        )
                        .await?;
                    let visible_materialization = self
                    .visible_materialization(&file_key)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_PLUGIN_OBSERVATION_STALE,
                            "the acknowledged component file no longer has a visible materialization root",
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
                            write
                                .inline_data()
                                .expect("selected plugin writes require inline content"),
                            write.splice_provenance(),
                            limits,
                        )
                    })?;
                    let submitted_bytes_sha256 = built_splices.after_sha256;
                    let host_full_diff_bytes_compared = built_splices.full_diff_bytes_compared;
                    let same_length_blob_splice = built_splices.same_length_replacement();
                    let blob_edit_splice = built_splices.replacement();
                    let observed_source = ArcByteSource::new(observed_bytes.clone());
                    let submitted_source = ArcByteSource::new(submitted_bytes.clone());
                    let observed_document = lease.observed_document();
                    let prior_row_keys = lease.accepted_row_authorities().clone();
                    lease.begin_guest_call()?;
                    let detection_input =
                        match lease.actor_mut().fork_document(observed_document).await {
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
                                rows: None,
                                prior_row_keys: Some(Box::new(AuthorityRowKeySource(
                                    prior_row_keys,
                                ))),
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
                        creates,
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

                    let detection_document = detected_transition.document;
                    let mut counters = detected_transition.counters;
                    let accepted_row_authorities = lease.accepted_row_authorities().clone();
                    let (mut changes, _observed_existing_authorities) = match self
                        .suppress_format_only_noops(
                            selected,
                            detected_transition.changes,
                            &file_key,
                        )
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
                            Some(&accepted_row_authorities),
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
                    let successor_row_authorities = plugin_row_authorities_after_transition(
                        &accepted_row_authorities,
                        &changes,
                        detected_transition.replace_all_rows,
                    );
                    let (successor_document, materialized_bytes, materialized_bytes_sha256) =
                        if observation_is_current {
                            // The actor lease serializes this file and the durable
                            // root still equals the acknowledged observation. The
                            // validated file successor is therefore already the
                            // exact merge result; rendering the same sparse change
                            // onto the same base would only repeat guest work.
                            let VisibleMaterializationBytes::Blob { hash } =
                                &visible_materialization.bytes;
                            verified_same_length_blob_splice = same_length_blob_splice
                                .map(|(offset, length)| (*hash, offset, length));
                            verified_blob_edit_splice =
                                blob_edit_splice.map(|(offset, delete_len, insert_len)| {
                                    (*hash, offset, delete_len, insert_len)
                                });
                            (
                                detection_document,
                                submitted_bytes.clone(),
                                submitted_bytes_sha256,
                            )
                        } else {
                            // Detection happened against a historical session
                            // document. Apply its sparse merge-resolved delta to
                            // the actor's current accepted document so concurrent
                            // different-row edits compose and same-row edits
                            // obey transaction commit order.
                            if let Err(error) =
                                lease.actor_mut().drop_document(detection_document).await
                            {
                                return Err(lease.handle_guest_call_error(error));
                            }
                            let current_document = lease.accepted_document();
                            let current_bytes = lease.accepted_bytes();
                            let change_source =
                                match VecRowChangeSource::new(changes.clone(), limits) {
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
                                .rows_changed(
                                    renderer_input,
                                    limits,
                                    WasmRowUpdate {
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
                            let rendered_transition = match drain_row_transition_edits(
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
                            if let Err(error) =
                                lease.actor_mut().drop_document(renderer_input).await
                            {
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
                    counters.durable_semantic_changes =
                        u64::try_from(changes.row_change_count()).unwrap_or(u64::MAX);
                    self.plugin_host.record_transition_counters(counters);
                    let successor_checkpoint = match lease
                        .actor_mut()
                        .checkpoint_document(successor_document)
                        .await
                    {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => return Err(lease.handle_guest_call_error(error)),
                    };
                    lease.complete_guest_call(
                        successor_document,
                        successor_checkpoint,
                        materialized_bytes.clone(),
                        materialized_bytes_sha256,
                        materialization_version.clone(),
                    )?;
                    lease.set_successor_row_authorities(successor_row_authorities)?;
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
                }
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
                    creates,
                    &schemas,
                    cold_limits,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.plugin_open_file_drain"
                ))
                .await?;
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
                let row_authorities =
                    plugin_row_authorities_from_transition(&changes, create_context.creates());
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes =
                    u64::try_from(changes.row_change_count()).unwrap_or(u64::MAX);
                self.plugin_host.record_transition_counters(counters);
                (
                    changes,
                    PendingPluginActorPublication::New {
                        cache: self.plugin_host.actor_cache(),
                        key: actor_key,
                        checkpoint: actor.checkpoint_document(validated.document).await?,
                        store: PluginActorStore::new(actor, store_permit),
                        document: validated.document,
                        bytes: submitted_bytes.clone(),
                        semantic_root: Arc::from(materialization_version.clone()),
                        row_authorities,
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
                append_plugin_change_rows(rows, selected, changes, &write.file_id, &context)
            });
            if let Err(error) = change_rows {
                publication.discard().await;
                discard_plugin_actor_publications(std::mem::take(
                    &mut reconciliation.actor_publications,
                ))
                .await;
                return Err(error);
            }
            if materialized_bytes.as_ref()
                != write
                    .inline_data()
                    .expect("selected plugin writes require inline content")
            {
                write.replace_data(materialized_bytes);
            } else {
                if let Some((visible_base_blob_hash, offset, length)) =
                    verified_same_length_blob_splice
                {
                    write.set_verified_same_length_blob_splice(
                        visible_base_blob_hash,
                        offset,
                        length,
                    );
                }
                if let Some((visible_base_blob_hash, offset, delete_len, insert_len)) =
                    verified_blob_edit_splice
                {
                    write.set_verified_blob_edit_splice(
                        visible_base_blob_hash,
                        offset,
                        delete_len,
                        insert_len,
                    );
                }
            }
            reconciliation
                .materialized_file_keys
                .insert(file_key.clone());
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
                        "one write batch cannot transition component plugin file '{}' more than once",
                        file_key.file_id
                    ),
                ));
            }
            let installed_key = PluginBranchEntryKey {
                branch_id: file_key.branch_id.clone(),
                plugin_key: group.plugin.key().to_string(),
            };
            let factory = component_factories
                .get(&installed_key)
                .expect("semantic component plugin should have a compiled factory")
                .clone();
            let descriptor = WasmFileDescriptor {
                file_id: file_key.file_id.clone(),
                path: Some(group.path.clone()),
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
                    // Semantic rows stay in their file's lane, not the tracked
                    // lane: an untracked file's rows are untracked.
                    || row.untracked != file_key.untracked
                    || group
                        .plugin
                        .schema_keys()
                        .binary_search_by(|key| key.as_str().cmp(row.schema_key.as_str()))
                        .is_err()
            }) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "normalized semantic rows escaped component plugin file '{}' ownership",
                        file_key.file_id
                    ),
                ));
            }
            let limits = WasmTransitionLimits::default();
            let changes = v2_host_changes_from_prepared_rows(
                &prepared,
                file_key.untracked,
                Arc::clone(&self.sql_schema_snapshot),
            )?;
            if changes.row_change_count() == 0 {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "component semantic write batch must contain at least one row change",
                ));
            }
            let view = PendingPluginActorView {
                session_key: session_key.clone(),
                plugin_key: group.plugin.key().to_string(),
                plugin_generation: group.plugin.archive_blob_hash().to_string(),
                owner_change_id: group.owner_change_id.clone(),
                semantic_chainable: true,
                retain_large_import_actor: retain_large_import_actor(&group.plugin),
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
                            "semantic row writes cannot follow a byte or identity transition for component plugin file '{}' in the same transaction",
                            file_key.file_id
                        ),
                    )
                    .with_hint("commit the byte transition before editing semantic rows"));
                }
                None => {
                    let cache = self.plugin_host.actor_cache().clone();
                    let visible_materialization = self
                        .visible_materialization(&file_key)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_PLUGIN_OBSERVATION_STALE,
                                format!(
                                    "owned component plugin file '{}' has no visible materialization root",
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
                            self.cold_open_semantic_actor(
                                &actor_key,
                                &file_key,
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
                                "component semantic write actor identity no longer matches file '{}'",
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
            let visible_materialization = match self.visible_materialization(&file_key).await {
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
                            "owned component plugin file '{}' lost its materialization root",
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
            let transition = render_semantic_changes_with_lease(
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
            self.plugin_host.record_transition_counters(counters);
            let VisibleMaterializationBytes::Blob { hash } = visible_materialization.bytes;
            let rendered_file = semantic_rendered_file_content(
                file_key.file_id.clone(),
                group.path,
                group.filename,
                file_key.branch_id.clone(),
                file_key.untracked,
                hash,
                rendered_bytes,
                same_length_output_splice,
            );
            file_content.push(rendered_file);
            reconciliation
                .materialized_file_keys
                .insert(file_key.clone());
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
            let owner_tombstone = PluginFileOwner::delete_row(
                file_key.file_id,
                &file_key.branch_id,
                file_key.untracked,
            )?;
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
            ReconciledTransactionWrite::RowsWithFileContent {
                mode,
                rows,
                file_content,
                count,
            } => PreparedTransactionWrite::RowsWithFileContent {
                mode,
                rows: self.prepare_reconciled_rows(rows).await?,
                file_content,
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
        let opening_read = self.opening_read();
        match self
            .prepare_transaction_rows_with_homogeneous_inner(rows, allow_homogeneous)
            .await
        {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(Self::enrich_schema_visibility_error(opening_read, error).await),
        }
    }

    async fn prepare_transaction_rows_with_homogeneous_inner(
        &mut self,
        mut rows: RawWriteBatch,
        allow_homogeneous: bool,
    ) -> Result<PreparedStateBatch, LixError> {
        let row_count = rows.len();
        let typed_validation_counters = typed_transaction_validation_counters(&rows);
        // This check precedes every certified and scalar preparation path. A
        // plugin-owned JSON payload is both rejected and recorded at the one
        // remaining ingress choke point before normalization can inspect it.
        for index in 0..rows.len() {
            reject_plugin_owned_json_row(&rows, index, &self.plugin_host)?;
        }
        // SQL transaction time is stable across every prepared write batch.
        // `execute_batch` may lower one logical transaction into many small
        // batches, so seed this batch from the transaction-level sample and
        // publish a newly sampled value after successful preparation.
        let mut default_timestamp = self.current_timestamp;
        let certified_preparation = rows.certified_preparation();
        let certified_domain =
            certified_preparation.and_then(|_| homogeneous_row_normalization_domain(&rows));
        let certified_plan_is_current = allow_homogeneous
            && match certified_domain.as_ref() {
                Some(domain) => !self
                    .staged_writes
                    .has_staged_schema_catalog_change(domain)?,
                None => false,
            };
        if certified_preparation.is_some() && !certified_plan_is_current {
            rows.revoke_certified_preparation();
        }
        if allow_homogeneous
            && certified_plan_is_current
            && let Some(certificate) = certified_preparation
        {
            let timestamp =
                *default_timestamp.get_or_insert_with(|| self.functions.call_timestamp());
            let prepared = rows.into_certified_prepared(
                certificate,
                self.origin_key.as_ref(),
                timestamp,
                &self.functions,
            )?;
            self.plugin_host
                .record_transition_counters(typed_validation_counters);
            self.current_timestamp = default_timestamp;
            return Ok(prepared);
        }
        let staged = self.staged_writes.staging_overlay()?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let hot_state = self.hot_state.reader(&read);
        if allow_homogeneous && let Some(domain) = homogeneous_row_normalization_domain(&rows) {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&hot_state, &staged, &domain)
                .await?;
            let mut scalar_facts = PreparedScalarBatch::with_capacity(rows.len());
            for index in 0..rows.len() {
                let normalized = normalize_raw_write_row_in_place(
                    &mut rows,
                    index,
                    catalog,
                    functions.clone(),
                    &mut default_timestamp,
                    None,
                )?;
                scalar_facts.push(plan_prepared_row_scalars(
                    rows.row(index),
                    normalized,
                    &functions,
                    &mut default_timestamp,
                )?);
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
                .map(|row| usize::from(row.metadata.is_some()))
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
            self.plugin_host
                .record_transition_counters(typed_validation_counters);
            self.current_timestamp = default_timestamp;
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
                .catalog_for_row_normalization(&hot_state, &staged, &domain)
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
                // Rows that re-enter normalization from the typed engine
                // pipeline no longer carry the compatibility JSON slot.
                // Materialize JSON only for the catalog-definition boundary;
                // the staged/durable row remains typed-only.
                let decoded_snapshot = row
                    .decoded_snapshot()
                    .map(|typed| typed.to_json_value())
                    .transpose()?;
                remember_pending_registered_schema(
                    row.snapshot_json()
                        .map(TransactionJson::value)
                        .or(decoded_snapshot.as_ref()),
                    Domain::schema_catalog(row.schema_scope_branch_id().to_string(), row.untracked),
                    catalog,
                )?;
            }
            for &index in &row_indices {
                let normalized = normalize_raw_write_row_in_place(
                    &mut rows,
                    index,
                    catalog,
                    functions.clone(),
                    &mut default_timestamp,
                    None,
                )?;
                normalized_facts[index] = Some(normalized);
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
                    &mut default_timestamp,
                )?;
                scalar_ordinal_by_row[index] = scalar_facts.schema_plan_ids.len();
                scalar_facts.push(scalar);
            }
        }

        if rows.iter().any(|row| {
            row.metadata
                .is_some_and(TransactionJson::requires_batch_canonicalization)
        }) {
            canonicalize_transaction_json_batch(rows.metadata_slots_mut(), "prepared metadata")?;
        }

        let json_count = rows
            .iter()
            .map(|row| usize::from(row.metadata.is_some()))
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
        self.plugin_host
            .record_transition_counters(typed_validation_counters);
        self.current_timestamp = default_timestamp;
        Ok(prepared_rows)
    }

    async fn enrich_schema_visibility_error(
        opening_read: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
        mut error: LixError,
    ) -> LixError {
        if error.code != LixError::CODE_SCHEMA_DEFINITION {
            return error;
        }
        let Some(details) = error.details.as_mut().and_then(JsonValue::as_object_mut) else {
            return error;
        };
        let Some(schema_key) = details
            .get("schema_key")
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
        else {
            return error;
        };
        let scope = details
            .get("scope")
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| "this transaction's scope".to_string());
        let Some(branch_id) = scope.strip_prefix("branch:") else {
            return error;
        };
        let base_commit_id = match BranchHeadControlContext::new()
            .reader(opening_read.clone())
            .load(branch_id)
            .await
        {
            Ok(Some(control)) => Some(control.head_commit_id),
            Ok(None) | Err(_) => None,
        };
        if let Some(base_commit_id) = base_commit_id {
            details.insert(
                "base_commit_id".to_string(),
                JsonValue::String(base_commit_id.to_string()),
            );
        }
        let Some(entity_commit_id) = details
            .get("entity_commit_id")
            .and_then(JsonValue::as_str)
            .or_else(|| details.get("base_commit_id").and_then(JsonValue::as_str))
            .and_then(|value| CommitId::parse_lix(value, "schema visibility entity commit").ok())
        else {
            return error;
        };
        let mut tracked_state = TrackedStateContext::new().reader(opening_read.clone());
        let registrations = match tracked_state
            .load_projected_batch_at_commit(
                &entity_commit_id.to_string(),
                &[TrackedStateKey {
                    schema_key: REGISTERED_SCHEMA_KEY.to_string(),
                    file_id: None,
                    row_pk: RowPk::single(schema_key.as_str()),
                }],
                &ChangeRecordProjection::identity_only(),
            )
            .await
        {
            Ok(registrations) => registrations,
            Err(_) => return error,
        };
        let Some(registration_commit_id) = registrations.row(0).map(|row| row.commit_id()) else {
            return error;
        };
        details.insert(
            "commit_id".to_string(),
            JsonValue::String(registration_commit_id.to_string()),
        );
        let Some(base_commit_id) = base_commit_id else {
            error.hint = Some(format!(
                "Schema '{schema_key}' is registered at commit {registration_commit_id}, but it is not visible in {scope}. This usually indicates that the schema was registered in a different branch or durability scope."
            ));
            return error;
        };
        let mut graph = CommitGraphContext::new().reader(opening_read);
        const DIAGNOSTIC_ANCESTRY_LIMIT: usize = 1_024;
        let registration_is_ancestor = match graph
            .reachable_nodes_limited(&base_commit_id, DIAGNOSTIC_ANCESTRY_LIMIT)
            .await
        {
            Ok(nodes)
                if nodes
                    .iter()
                    .any(|node| node.commit.commit_id == registration_commit_id) =>
            {
                Some(true)
            }
            Ok(nodes) if nodes.len() < DIAGNOSTIC_ANCESTRY_LIMIT => Some(false),
            Ok(_) => None,
            Err(_) => None,
        };
        error.hint = Some(if registration_is_ancestor == Some(true) {
            format!(
                "Schema '{schema_key}' is registered at commit {registration_commit_id}, which is an ancestor of this transaction's base commit {base_commit_id}, but it is absent from {scope}'s transaction catalog. This can indicate a later schema removal, a durability-scope mismatch, or an internal catalog inconsistency."
            )
        } else if registration_is_ancestor == Some(false) {
            format!(
                "Schema '{schema_key}' is registered at commit {registration_commit_id}, but that commit is not an ancestor of this transaction's base commit {base_commit_id}. This usually indicates that an entity from another branch was staged without first merging or rebasing its schema registration into {scope}."
            )
        } else {
            format!(
                "Schema '{schema_key}' is registered at commit {registration_commit_id}, but its ancestry relative to this transaction's base commit {base_commit_id} could not be verified. Retry the transaction; if the error persists, inspect the commit graph and {scope}'s schema registration."
            )
        });
        error
    }

    /// Validates the drained write set and, on the way through, hands the
    /// batch its indexed-column extraction.
    ///
    /// Takes `&mut` only for that hand-off: validation itself reads. The two
    /// certificate early-returns below leave the extraction empty, which is
    /// the safe value — no entries and no witnesses means every read of those
    /// collections keeps scanning. Both are reachable only by rows that
    /// provably declare no indexed column or provably did not change one; see
    /// `declared_column_rows_never_bypass_extraction`.
    async fn validate_prepared_writes_by_branch(
        &mut self,
        read: &(impl StorageAdapterRead + ?Sized),
        prepared_writes: &mut PreparedWriteSet,
    ) -> Result<(), LixError> {
        if prepared_tracked_rows_have_row_local_certificates(&prepared_writes.state_rows) {
            // Row-local certificates avoid rebuilding the O(rows) validation
            // index, but they do not prove that a public INSERT identity is
            // absent from committed state.
            if !prepared_writes.insert_selection.is_empty() {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_transaction_validation_branch();
                let hot_state = self.hot_state.reader(read);
                validate_certified_tracked_insert_identities(&hot_state, prepared_writes)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.validation.insert_identities"
                    ))
                    .await?;
            }
            return Ok(());
        }
        if self.trust_filesystem_planner
            && let Some(certificate) = fresh_plugin_file_import_certificate(
                prepared_writes,
                Some(self.sql_schema_snapshot.as_ref()),
            )
        {
            // The certificate proves that every omitted file-scoped row has
            // completed row-local validation, has no transaction-wide schema
            // constraint, and is owned by this exact pending planner-created
            // descriptor. Keep public INSERT absence validation against this
            // coherent commit snapshot before skipping the O(rows) index.
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let hot_state = self.hot_state.reader(read);
            validate_certified_fresh_plugin_file_import(&hot_state, certificate).await?;
            return Ok(());
        }
        let staged = self.staged_writes.staging_overlay()?;
        let staged_commit_ids = prepared_writes
            .commit_change_refs_by_branch
            .values()
            .map(|refs| refs.commit_id)
            .chain(
                prepared_writes
                    .intermediate_commits
                    .iter()
                    .map(|commit| commit.change_refs.commit_id),
            )
            .collect::<BTreeSet<_>>();
        // `validation_index()` holds an immutable borrow of the write set for
        // the whole loop, so the extraction accumulates into a local and is
        // published once the borrow ends.
        let mut staged_index_values = StagedIndexValues::default();
        {
            let validation_index = prepared_writes.validation_index();
            for scope in validation_index.schema_scopes() {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_transaction_validation_branch();
                let branch_prepared_writes =
                    validation_index.validation_set_for_schema_scope(scope);
                let hot_state = self.hot_state.reader(read);
                let schema_catalog = self
                    .schema_resolver
                    .catalog_for_validation(&hot_state, &staged, scope)
                    .await?;
                let mut validation_input = TransactionValidationInput::new(
                    &branch_prepared_writes,
                    schema_catalog,
                    &hot_state,
                )
                .with_staged_commit_ids(staged_commit_ids.clone());
                if self.trust_filesystem_planner {
                    validation_input = validation_input.with_trusted_filesystem_planner();
                }
                staged_index_values.absorb(validate_prepared_writes(validation_input).await?);
            }
        }
        if !staged_index_values.is_empty() {
            prepared_writes
                .state_rows
                .set_staged_index_values(staged_index_values);
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

    #[cfg(test)]
    pub(crate) async fn stage_engine_test_rows(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.ensure_plugin_generation_read_guard().await;
        let prepared = self.prepare_transaction_rows(rows).await?;
        self.staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared,
            })
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

    /// Defers explicit historical-source branchability to the commit boundary.
    ///
    /// Ordinary reachable commits need no bridge. A compacted source is
    /// accepted only through one pending authenticated GC transition observed
    /// by the same read whose queue row fences branch publication.
    pub(crate) fn stage_branch_checkpoint_replacement_resolution(
        &mut self,
        branch_id: String,
        source_commit_id: CommitId,
    ) -> Result<(), LixError> {
        if self
            .pending_branch_checkpoint_replacements
            .insert(branch_id.clone(), source_commit_id)
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "branch '{branch_id}' staged more than one checkpoint replacement resolution"
                ),
            ));
        }
        Ok(())
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

    pub(crate) fn suppress_ordinary_sync_event(&mut self) {
        self.suppress_ordinary_sync_event = true;
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

    pub(crate) async fn try_execute_prepared_mutation(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<crate::sql2::SqlWriteResult>, LixError> {
        if let Some(error) = &self.mutation_journal_terminal_error {
            return Err(error.clone());
        }
        let Some((cached_sql, program)) = self.prepared_mutation_program.as_ref() else {
            return Ok(None);
        };
        if cached_sql.as_ref() != sql {
            return Ok(None);
        }
        let program = Arc::clone(program);
        let primary_key = program.primary_key(params)?;
        if self
            .mutation_journal
            .as_ref()
            .and_then(TransactionMutationJournal::last_identity)
            .is_some_and(|last| last >= primary_key)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "prepared mutation order barrier was not flushed before statement checkpoint",
            ));
        }
        if matches!(
            self.prepared_mutation_membership,
            PreparedMutationMembership::Unprepared
        ) {
            let read = self.opening_read();
            let base = self
                .hot_state
                .transaction_reader(read, Arc::clone(&self.branch_head_control_cache));
            self.prepared_mutation_membership = match base
                .prepare_packed_identity_membership(&self.active_branch_id, &program.schema_key)
                .await?
            {
                Some(membership) => PreparedMutationMembership::Packed(membership),
                None => PreparedMutationMembership::Unavailable,
            };
        }
        if !self.prepared_mutation_overlay_empty {
            let row_pk = RowPk::single(primary_key.to_owned());
            if self.staged_writes.staged_identity_may_affect(
                &self.active_branch_id,
                &program.schema_key,
                None,
                &row_pk,
            )? {
                // A transaction-local predecessor is part of the mutable
                // overlay, not durable history. Keep dependent statements on
                // the generic executor so INSERT/UPDATE/DELETE coalescing
                // retains lifecycle and constraint semantics.
                return Ok(None);
            }
            self.prepared_mutation_overlay_empty = !self.staged_writes.has_staged_state_rows()?;
        }
        let opening_read = self.opening_read();
        let cached_membership = match &mut self.prepared_mutation_membership {
            PreparedMutationMembership::Packed(membership) => {
                membership
                    .contains_single_string(&opening_read, primary_key)
                    .await?
            }
            PreparedMutationMembership::Unprepared | PreparedMutationMembership::Unavailable => {
                None
            }
        };
        let fallback_row = match cached_membership {
            Some(false) => return Ok(Some(crate::sql2::SqlWriteResult::affected(0))),
            Some(true) => None,
            None => {
                let Some(row) =
                    crate::sql2::prepare_path_value_replacement_row(self, &program, params).await?
                else {
                    return Ok(Some(crate::sql2::SqlWriteResult::affected(0)));
                };
                Some(row)
            }
        };
        debug_assert!(
            fallback_row
                .as_ref()
                .is_none_or(|row| row.row_pk.as_single_string().ok() == Some(primary_key))
        );
        let origin_key = self.origin_key.clone();
        let functions = self.functions.clone();
        let (journal_slot, timestamp_slot) = (
            &mut self.mutation_journal,
            &mut self.prepared_mutation_timestamp,
        );
        let journal = journal_slot.get_or_insert_with(|| TransactionMutationJournal {
            program: Arc::clone(&program),
            origin_key: origin_key.clone(),
            identity_arena: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ARENA_BYTES),
            identity_offsets: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ROWS),
            snapshot_arena: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ARENA_BYTES),
            snapshot_offsets: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ROWS),
            timestamp: None,
        });
        debug_assert!(Arc::ptr_eq(&journal.program, &program));
        debug_assert_eq!(journal.origin_key, origin_key);
        let snapshot_offset = match fallback_row {
            Some(row) => {
                let start = journal.snapshot_arena.len();
                journal
                    .snapshot_arena
                    .extend_from_slice(row.snapshot.normalized().as_bytes());
                (start, journal.snapshot_arena.len())
            }
            None => crate::sql2::append_path_value_replacement_snapshot(
                &program,
                primary_key,
                params,
                &mut journal.snapshot_arena,
            )?,
        };
        let timestamp = *timestamp_slot.get_or_insert_with(|| functions.call_timestamp());
        journal.append_identity(primary_key);
        journal.snapshot_offsets.push(snapshot_offset);
        #[cfg(feature = "storage-benches")]
        journal.record_row_ownership(
            primary_key.len(),
            snapshot_offset.1.saturating_sub(snapshot_offset.0),
        );
        let journal_timestamp = journal.timestamp.get_or_insert(timestamp);
        debug_assert_eq!(*journal_timestamp, timestamp);
        Ok(Some(crate::sql2::SqlWriteResult::affected(1)))
    }

    pub(crate) fn prepared_mutation_matches(&self, sql: &str) -> bool {
        self.prepared_mutation_program
            .as_ref()
            .is_some_and(|(cached_sql, _)| cached_sql.as_ref() == sql)
    }

    pub(crate) fn prepared_literal_mutation_shape(&self) -> Option<(&str, usize)> {
        let (shape, program) = self.prepared_mutation_program.as_ref()?;
        Some((shape, program.parameter_count()))
    }

    /// Appends one shape-certified literal UPDATE without constructing public
    /// `Value` DTOs. The explicit transaction has already admitted this plan;
    /// borrowed literal slices flow directly into identity and snapshot
    /// columns. Non-packed or dependent state returns to the generic path.
    pub(crate) async fn try_execute_cached_literal_prepared_mutation(
        &mut self,
        next_origin_key: Option<&str>,
        params: &[impl AsRef<str>],
    ) -> Result<Option<crate::sql2::SqlWriteResult>, LixError> {
        if let Some(error) = &self.mutation_journal_terminal_error {
            return Err(error.clone());
        }
        let (primary_key, replacement_value) = {
            let Some((_, program)) = self.prepared_mutation_program.as_ref() else {
                return Ok(None);
            };
            (
                program.primary_key_text(params)?,
                program.replacement_value_text(params)?,
            )
        };

        let same_origin = self
            .mutation_journal
            .as_ref()
            .is_none_or(|journal| journal.origin_key.as_deref() == next_origin_key);
        let ordered_append = same_origin
            && self
                .mutation_journal
                .as_ref()
                .and_then(TransactionMutationJournal::last_identity)
                .is_none_or(|last| last < primary_key);
        let chunk_has_capacity = self
            .mutation_journal
            .as_ref()
            .is_none_or(|journal| journal.len() < MUTATION_JOURNAL_CHUNK_MAX_ROWS);
        if !same_origin || !ordered_append || !chunk_has_capacity {
            self.flush_mutation_journal().await?;
        }
        if !same_origin || !ordered_append {
            self.lower_provisional_mutations_to_prepared().await?;
            self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
            self.prepared_mutation_overlay_empty = false;
        }

        if !matches!(
            self.prepared_mutation_membership,
            PreparedMutationMembership::Packed(_)
        ) {
            return Ok(None);
        }
        if !self.prepared_mutation_overlay_empty {
            let row_pk = RowPk::single(primary_key.to_owned());
            let schema_key = &self
                .prepared_mutation_program
                .as_ref()
                .expect("packed literal mutation retains its prepared program")
                .1
                .schema_key;
            if self.staged_writes.staged_identity_may_affect(
                &self.active_branch_id,
                schema_key,
                None,
                &row_pk,
            )? {
                return Ok(None);
            }
            self.prepared_mutation_overlay_empty = !self.staged_writes.has_staged_state_rows()?;
        }
        let opening_read = self.opening_read();
        let PreparedMutationMembership::Packed(membership) = &mut self.prepared_mutation_membership
        else {
            unreachable!("packed literal mutation membership was checked above")
        };
        match membership
            .contains_single_string(&opening_read, primary_key)
            .await?
        {
            Some(false) => return Ok(Some(crate::sql2::SqlWriteResult::affected(0))),
            Some(true) => {}
            None => return Ok(None),
        }

        let origin_key = next_origin_key.map(SharedStr::from);
        let journal_program = self.mutation_journal.is_none().then(|| {
            Arc::clone(
                &self
                    .prepared_mutation_program
                    .as_ref()
                    .expect("packed literal mutation retains its prepared program")
                    .1,
            )
        });
        let timestamp = match self.prepared_mutation_timestamp {
            Some(timestamp) => timestamp,
            None => {
                let timestamp = self.functions.call_timestamp();
                self.prepared_mutation_timestamp = Some(timestamp);
                timestamp
            }
        };
        let journal = self
            .mutation_journal
            .get_or_insert_with(|| TransactionMutationJournal {
                program: journal_program
                    .expect("new mutation journal retains its prepared program"),
                origin_key: origin_key.clone(),
                identity_arena: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ARENA_BYTES),
                identity_offsets: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ROWS),
                snapshot_arena: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ARENA_BYTES),
                snapshot_offsets: Vec::with_capacity(INITIAL_MUTATION_JOURNAL_ROWS),
                timestamp: None,
            });
        debug_assert_eq!(journal.origin_key, origin_key);
        let snapshot_offset = crate::sql2::append_path_value_replacement_snapshot_text(
            primary_key,
            Some(replacement_value),
            &mut journal.snapshot_arena,
        )?;
        journal.append_identity(primary_key);
        journal.snapshot_offsets.push(snapshot_offset);
        #[cfg(feature = "storage-benches")]
        journal.record_row_ownership(
            primary_key.len(),
            snapshot_offset.1.saturating_sub(snapshot_offset.0),
        );
        let journal_timestamp = journal.timestamp.get_or_insert(timestamp);
        debug_assert_eq!(*journal_timestamp, timestamp);
        Ok(Some(crate::sql2::SqlWriteResult::affected(1)))
    }

    pub(crate) fn remember_prepared_mutation(
        &mut self,
        sql: &str,
        plan: &crate::sql2::SqlLogicalPlan,
    ) -> Result<(), LixError> {
        let domain = Domain::schema_catalog(self.active_branch_id.clone(), false);
        if self
            .staged_writes
            .has_staged_schema_catalog_change(&domain)?
        {
            self.prepared_mutation_program = None;
            self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
            self.prepared_mutation_overlay_empty = false;
            self.prepared_mutation_timestamp = None;
            return Ok(());
        }
        self.prepared_mutation_program =
            crate::sql2::prepare_path_value_replacement_program(self, plan)
                .map(|program| (Arc::<str>::from(sql), Arc::new(program)));
        self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
        self.prepared_mutation_overlay_empty = false;
        self.prepared_mutation_timestamp = None;
        Ok(())
    }

    pub(crate) async fn flush_prepared_mutations(&mut self) -> Result<(), LixError> {
        if let Some(error) = &self.mutation_journal_terminal_error {
            return Err(error.clone());
        }
        let generation_seed = self.prepared_mutation_collection_generation_seed();
        let mut complete_generation = resolve_prepared_mutation_collection_generation(
            generation_seed,
            self.opening_read(),
            Arc::clone(&self.hot_state),
            Arc::clone(&self.branch_head_control_cache),
            self.active_branch_id.clone(),
        )
        .await?
        .map(|(schema_key, generation)| (schema_key, self.active_branch_id.clone(), generation));
        if let Some((schema_key, _, (live_count, ordered_identity_digest))) =
            complete_generation.as_ref()
        {
            let opening_read = self.opening_read();
            if opening_parent_complete_lifecycle_created_at(
                &opening_read,
                self.opening_active_branch_head,
                schema_key,
                *live_count,
                *ordered_identity_digest,
            )
            .await?
            .is_none()
            {
                complete_generation = None;
            }
        }
        if complete_generation.is_some() {
            self.flush_mutation_journal_final().await?;
        } else {
            // Without a certifiable base generation this journal cannot
            // become complete-set authority. Leave its final tail unsealed
            // for generic lowering.
            self.flush_mutation_journal().await?;
        }
        let replacement_certified =
            if let Some((schema_key, branch_id, (live_count, ordered_identity_digest))) =
                complete_generation
            {
                self.staged_writes.certify_complete_collection_replacement(
                    schema_key.as_str(),
                    &branch_id,
                    live_count,
                    ordered_identity_digest,
                )?
            } else {
                false
            };
        if !replacement_certified {
            self.lower_provisional_mutations_to_prepared().await?;
        }
        self.prepared_mutation_program = None;
        self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
        self.prepared_mutation_overlay_empty = false;
        self.prepared_mutation_timestamp = None;
        Ok(())
    }

    /// Flushes pending mutations into the transaction-visible row overlay.
    /// The immutable journal itself is a staged read source, so an intervening
    /// read seals only the current mutable chunk and does not reconstruct a
    /// `PreparedStateBatch`.
    pub(crate) async fn flush_prepared_mutations_for_read(&mut self) -> Result<(), LixError> {
        self.flush_mutation_journal().await?;
        let generation_seed = self.prepared_mutation_collection_generation_seed();
        let Some((schema_key, (live_count, ordered_identity_digest))) =
            resolve_prepared_mutation_collection_generation(
                generation_seed,
                self.opening_read(),
                Arc::clone(&self.hot_state),
                Arc::clone(&self.branch_head_control_cache),
                self.active_branch_id.clone(),
            )
            .await?
        else {
            self.hydrate_provisional_mutation_predecessors().await?;
            return Ok(());
        };
        let opening_read = self.opening_read();
        if let Some(created_at) = opening_parent_complete_lifecycle_created_at(
            &opening_read,
            self.opening_active_branch_head,
            &schema_key,
            live_count,
            ordered_identity_digest,
        )
        .await?
        {
            self.staged_writes.set_ordered_mutation_overlay_created_at(
                self.opening_active_branch_head
                    .expect("a certified opening parent lifecycle requires an opening branch head"),
                created_at,
            )?;
        } else {
            self.hydrate_provisional_mutation_predecessors().await?;
        }
        Ok(())
    }

    fn prepared_mutation_collection_generation_seed(
        &self,
    ) -> Option<(String, Option<(u64, [u8; 32])>)> {
        let Some((_, program)) = &self.prepared_mutation_program else {
            return None;
        };
        let generation = match &self.prepared_mutation_membership {
            PreparedMutationMembership::Packed(membership) => {
                Some(membership.complete_generation())
            }
            PreparedMutationMembership::Unprepared | PreparedMutationMembership::Unavailable => {
                None
            }
        };
        Some((program.schema_key.clone(), generation))
    }

    pub(crate) async fn flush_prepared_mutation_barrier(
        &mut self,
        next_sql: &str,
        next_origin_key: Option<&str>,
        params: &[Value],
    ) -> Result<(), LixError> {
        let same_program = self
            .prepared_mutation_program
            .as_ref()
            .is_some_and(|(sql, _)| sql.as_ref() == next_sql);
        let same_origin = self
            .mutation_journal
            .as_ref()
            .is_none_or(|journal| journal.origin_key.as_deref() == next_origin_key);
        let ordered_append = if same_program && same_origin {
            self.prepared_mutation_program
                .as_ref()
                .and_then(|(_, program)| program.primary_key(params).ok())
                .is_none_or(|row_pk| {
                    self.mutation_journal
                        .as_ref()
                        .and_then(TransactionMutationJournal::last_identity)
                        .is_none_or(|last| last < row_pk)
                })
        } else {
            false
        };
        let chunk_has_capacity = self
            .mutation_journal
            .as_ref()
            .is_none_or(|journal| journal.len() < MUTATION_JOURNAL_CHUNK_MAX_ROWS);
        if !same_program || !same_origin || !ordered_append || !chunk_has_capacity {
            self.flush_mutation_journal().await?;
        }
        if !same_program || !same_origin || !ordered_append {
            self.lower_provisional_mutations_to_prepared().await?;
            self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
            self.prepared_mutation_overlay_empty = false;
        }
        if !same_program {
            self.prepared_mutation_program = None;
            self.prepared_mutation_membership = PreparedMutationMembership::Unprepared;
            self.prepared_mutation_overlay_empty = false;
            self.prepared_mutation_timestamp = None;
        }
        Ok(())
    }

    async fn flush_mutation_journal(&mut self) -> Result<(), LixError> {
        self.flush_mutation_journal_with_tail(false).await
    }

    async fn flush_mutation_journal_final(&mut self) -> Result<(), LixError> {
        self.flush_mutation_journal_with_tail(true).await
    }

    async fn flush_mutation_journal_with_tail(
        &mut self,
        finalize_tail: bool,
    ) -> Result<(), LixError> {
        if let Some(error) = &self.mutation_journal_terminal_error {
            return Err(error.clone());
        }
        let result = self.flush_mutation_journal_inner(finalize_tail).await;
        if let Err(error) = &result {
            self.mutation_journal_terminal_error = Some(error.clone());
        }
        result
    }

    async fn flush_mutation_journal_inner(&mut self, finalize_tail: bool) -> Result<(), LixError> {
        let Some(journal) = self.mutation_journal.take() else {
            return Ok(());
        };
        if journal.identity_offsets.is_empty() {
            return Ok(());
        }
        self.ensure_plugin_generation_read_guard().await;
        #[cfg(feature = "storage-benches")]
        let identity_arena_bytes = journal.identity_arena.len();
        #[cfg(feature = "storage-benches")]
        let snapshot_arena_bytes = journal.snapshot_arena.len();
        #[cfg(feature = "storage-benches")]
        let journal_rows = journal.len();
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_crud_ownership(
                crate::storage_bench::CRUD_OWNERSHIP_IDENTITY_ENCODING,
                journal_rows,
                identity_arena_bytes,
                snapshot_arena_bytes,
                journal
                    .identity_offsets
                    .len()
                    .saturating_add(journal.snapshot_offsets.len()),
                0,
                0,
            );
            let total = identity_arena_bytes.saturating_add(snapshot_arena_bytes);
            crate::storage_bench::record_crud_ownership_transfer(
                crate::storage_bench::CRUD_OWNERSHIP_IDENTITY_ENCODING,
                0,
                0,
                total,
                0,
            );
        }
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(journal_rows);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let timestamp = journal.timestamp.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "non-empty transaction mutation journal has no lifecycle timestamp",
            )
        })?;
        let schema_plan = self
            .sql_schema_snapshot
            .plan(journal.program.schema_plan_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal lost its schema plan",
                )
            })?;
        let mut chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            journal.program.schema_plan_id,
            journal.program.schema_key.as_str().into(),
            self.active_branch_id.clone().into(),
            journal.origin_key,
            journal.identity_arena,
            journal.identity_offsets,
            journal.snapshot_arena,
            journal.snapshot_offsets,
            schema_plan,
            None,
            timestamp,
        )?;
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_crud_ownership(
                crate::storage_bench::CRUD_OWNERSHIP_NORMALIZATION,
                journal_rows,
                identity_arena_bytes,
                snapshot_arena_bytes,
                journal_rows.saturating_mul(2),
                0,
                0,
            );
            let total = identity_arena_bytes.saturating_add(snapshot_arena_bytes);
            crate::storage_bench::record_crud_ownership_transfer(
                crate::storage_bench::CRUD_OWNERSHIP_NORMALIZATION,
                0,
                0,
                total,
                0,
            );
        }
        let eager_collection_is_bounded = match &self.prepared_mutation_membership {
            PreparedMutationMembership::Packed(membership) => {
                usize::try_from(membership.complete_generation().0)
                    .is_ok_and(|rows| rows <= MUTATION_JOURNAL_EAGER_SEAL_MAX_ROWS)
            }
            PreparedMutationMembership::Unprepared | PreparedMutationMembership::Unavailable => {
                false
            }
        };
        if !eager_collection_is_bounded {
            self.mutation_journal_seal_prefix_open = false;
        }
        if self.mutation_journal_seal_prefix_open {
            let eager_row_count = self
                .mutation_journal_sealed_rows
                .checked_add(chunk.len())
                .filter(|&rows| rows <= MUTATION_JOURNAL_EAGER_SEAL_MAX_ROWS);
            if let Some(eager_row_count) = eager_row_count {
                chunk
                    .seal_replacement_parts(finalize_tail, &mut self.mutation_journal_compressor)?;
                if chunk.sealed_replacement_parts().is_some() {
                    self.mutation_journal_sealed_rows = eager_row_count;
                } else {
                    self.mutation_journal_seal_prefix_open = false;
                }
            } else {
                self.mutation_journal_seal_prefix_open = false;
            }
        }
        match self.staged_writes.stage_immutable_mutation_chunk(chunk)? {
            ImmutableMutationChunkStage::Staged => {}
            ImmutableMutationChunkStage::RequiresGeneric(chunk) => {
                self.lower_provisional_mutations_to_prepared().await?;
                let chunk = self.hydrate_immutable_mutation_chunk(chunk).await?;
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: chunk.into_prepared(false, &self.functions)?,
                    })?;
            }
        }
        Ok(())
    }

    async fn lower_provisional_mutations_to_prepared(&mut self) -> Result<(), LixError> {
        self.hydrate_provisional_mutation_predecessors().await?;
        self.staged_writes.lower_immutable_mutations_to_prepared()
    }

    async fn hydrate_provisional_mutation_predecessors(&mut self) -> Result<(), LixError> {
        let Some(descriptor) = self
            .staged_writes
            .provisional_mutation_journal_descriptor()?
        else {
            return Ok(());
        };
        if descriptor.predecessors_complete() {
            return Ok(());
        }
        let schema_key = descriptor.schema_key().to_owned();
        let branch_id = descriptor.branch_id().to_owned();
        let row_count = descriptor
            .row_pk_chunks()
            .iter()
            .map(|chunk| chunk.len())
            .sum();
        let mut predecessors = Vec::with_capacity(row_count);
        for row_pks in descriptor.row_pk_chunks() {
            let request = HotStateExactBatchRequest {
                rows: row_pks
                    .iter()
                    .cloned()
                    .map(|row_pk| HotStateExactRowRequest {
                        schema_key: schema_key.clone(),
                        branch_id: branch_id.clone(),
                        row_pk,
                        file_id: None,
                    })
                    .collect(),
                projection: HotStateProjection::default(),
                untracked: Some(false),
                include_tombstones: false,
            };
            let current = load_opening_exact_hot_state_batch(
                self.opening_read(),
                Arc::clone(&self.hot_state),
                Arc::clone(&self.branch_head_control_cache),
                &request,
            )
            .await?;
            for (slot, expected_row_pk) in row_pks.iter().enumerate() {
                let row = current.row(slot).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation lost its current-state predecessor",
                    )
                })?;
                if row.schema_key() != schema_key
                    || row.branch_id() != branch_id
                    || row.row_pk() != expected_row_pk
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation predecessor order changed",
                    ));
                }
                predecessors.push(row.durable_predecessor().cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation predecessor lacks durable evidence",
                    )
                })?);
            }
        }
        self.staged_writes
            .hydrate_immutable_mutation_predecessors(predecessors)?;
        Ok(())
    }

    async fn hydrate_immutable_mutation_chunk(
        &mut self,
        mut chunk: ImmutableMutationJournalChunk,
    ) -> Result<ImmutableMutationJournalChunk, LixError> {
        let row_pks = chunk.materialized_row_pks();
        let request = HotStateExactBatchRequest {
            rows: row_pks
                .iter()
                .cloned()
                .map(|row_pk| HotStateExactRowRequest {
                    schema_key: chunk.schema_key().to_owned(),
                    branch_id: chunk.branch_id().to_owned(),
                    row_pk,
                    file_id: None,
                })
                .collect(),
            projection: HotStateProjection::default(),
            untracked: Some(false),
            include_tombstones: false,
        };
        let current = load_opening_exact_hot_state_batch(
            self.opening_read(),
            Arc::clone(&self.hot_state),
            Arc::clone(&self.branch_head_control_cache),
            &request,
        )
        .await?;
        let mut predecessors = Vec::with_capacity(row_pks.len());
        for (slot, expected_row_pk) in row_pks.iter().enumerate() {
            let row = current.row(slot).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation lost its current-state predecessor",
                )
            })?;
            if row.schema_key() != chunk.schema_key()
                || row.branch_id() != chunk.branch_id()
                || row.row_pk() != expected_row_pk
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation predecessor order changed",
                ));
            }
            predecessors.push(row.durable_predecessor().cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation predecessor lacks durable evidence",
                )
            })?);
        }
        chunk.attach_durable_predecessors(predecessors)?;
        Ok(chunk)
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
        let read_store = self.opening_read();
        let active_branch_id = self.active_branch_id.clone();
        let hot_state = Arc::clone(&self.hot_state);
        let binary_cas = Arc::clone(&self.binary_cas);
        let branch_ctx = Arc::clone(&self.branch_ctx);
        let visible_schemas = self.sql_visible_schemas();
        let functions = self.functions.clone();
        let staged = self.staged_writes.staging_overlay()?;
        let staged_writes = Arc::clone(&self.staged_writes);
        let filesystem_path_index_cache = Arc::clone(&self.filesystem_path_index_cache);
        let filesystem_path_index_epoch = Arc::clone(&self.filesystem_path_index_epoch);
        let branch_head_control_cache = Arc::clone(&self.branch_head_control_cache);
        let plugin_host = self.plugin_host.clone();
        let sql_planning_cache = Arc::clone(&self.sql_planning_cache);
        let sql_catalog_fingerprint = self.sql_catalog_fingerprint().clone();

        let read_ctx = TransactionSqlReadExecutionContext {
            active_branch_id,
            active_account_id: self.active_account_id.clone(),
            read_store,
            hot_state,
            binary_cas,
            branch_ctx,
            visible_schemas,
            functions,
            staged,
            staged_writes,
            filesystem_path_index_cache,
            filesystem_path_index_epoch,
            branch_head_control_cache,
            plugin_host,
            sql_planning_cache,
            sql_catalog_fingerprint,
        };
        crate::sql2::execute_transaction_read_statement_from_parsed(
            &read_ctx, self, &sql, statement, &params,
        )
        .await
    }

    fn sql_visible_schemas(&self) -> Vec<JsonValue> {
        self.sql_schema_snapshot.schema_jsons()
    }

    pub(crate) fn visible_schema_keys(&self) -> Result<Vec<String>, LixError> {
        self.sql_visible_schemas()
            .iter()
            .map(crate::schema::schema_key_from_definition)
            .map(|result| result.map(|key| key.schema_key))
            .collect()
    }

    /// Returns the immutable Schema v1 plan used by plugin merge admission.
    /// Callers must use this plan rather than rebuilding a schema or guessing
    /// a fingerprint from row content.
    pub(crate) fn plugin_schema_plan(
        &self,
        schema_key: &str,
    ) -> Option<&crate::catalog::SchemaPlan> {
        self.sql_schema_snapshot
            .plan_for_key(schema_key)
            .map(|(_, plan)| plan)
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

    /// Stages an ancestor restore for commit-time lifecycle publication.
    pub(crate) async fn restore_branch_ref(
        &mut self,
        branch_id: &str,
        expected_head_commit_id: CommitId,
        target_commit_id: CommitId,
    ) -> Result<(), LixError> {
        if self.pending_restore_targets.contains_key(branch_id) {
            return Err(LixError::new(
                "LIX_INVALID_TRANSACTION_STATE",
                format!("branch '{branch_id}' staged more than one restore"),
            ));
        }
        self.advance_branch_ref(branch_id, target_commit_id).await?;
        self.pending_restore_targets.insert(
            branch_id.to_owned(),
            PendingRestoreIntent {
                expected_head_commit_id,
                target_commit_id,
            },
        );
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

    /// Stages the constant-work checkpoint boundary used by the SDK API.
    ///
    /// The metadata-only commit uses the prior checkpoint as its semantic first
    /// parent and records the captured head as its physical complete-state
    /// source. Interval compaction is not part of foreground publication.
    pub(crate) fn stage_checkpoint_boundary_commit(
        &self,
        branch_id: String,
        previous_checkpoint_commit_id: CommitId,
        recovered_head_commit_id: CommitId,
        interval_has_commits: bool,
        gc_state: CheckpointGcState,
    ) -> Result<String, LixError> {
        let commit_id = self.staged_writes.stage_selected_commit_change_refs(
            branch_id.clone(),
            StagedCommitChangeBatch::default(),
        )?;
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
                hot_working_diff_certified: false,
            })?;
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
        hot_working_diff_certified: bool,
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
                hot_working_diff_certified,
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
    pub(crate) async fn branch_ref_reader(
        &mut self,
    ) -> Result<impl BranchRefReader + '_, LixError> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        Ok(self
            .branch_ctx
            .ref_reader(SharedStorageAdapterRead::new(read)))
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) async fn tracked_state_reader(
        &mut self,
    ) -> Result<
        TrackedStateStoreReader<SharedStorageAdapterRead<StorageImpl::Read<'_>>>,
        LixError,
    > {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        Ok(self
            .tracked_state
            .reader(SharedStorageAdapterRead::new(read)))
    }

    /// Returns the private compaction cursor bound to this transaction's
    /// retained opening read and the caller-observed branch head.
    pub(crate) async fn checkpoint_commit_id_at_head(
        &mut self,
        branch_id: &str,
        head_commit_id: CommitId,
    ) -> Result<CommitId, LixError> {
        checkpoint_commit_id_at_head(self.opening_read(), branch_id, head_commit_id).await
    }

    pub(crate) async fn is_current_checkpoint_commit(
        &mut self,
        branch_id: &str,
        commit_id: CommitId,
    ) -> Result<bool, LixError> {
        let control = BranchHeadControlContext::new()
            .reader(self.opening_read())
            .load(branch_id)
            .await?
            .ok_or_else(|| {
                LixError::branch_not_found(branch_id, "resolve undo checkpoint boundary", "branch")
            })?;
        Ok(control.working_diff_checkpoint_commit_id == Some(commit_id))
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) async fn commit_graph_reader(
        &mut self,
    ) -> Result<
        CommitGraphStoreReader<SharedStorageAdapterRead<StorageImpl::Read<'_>>>,
        LixError,
    > {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        Ok(CommitGraphContext::new().reader(SharedStorageAdapterRead::new(read)))
    }

    /// Applies a tracked-state transition resolved from two immutable commits.
    ///
    /// This is the internal counterpart to the public diff command. The
    /// caller supplies typed tracked-state identities. The transaction's
    /// coherent opening head certifies the current
    /// side, so undo/redo does not need to reload visible live state after it
    /// has already read that exact historical root.
    pub(crate) async fn execute_tracked_state_transition(
        &mut self,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        keys: Vec<TrackedStateKey>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let branch_id = self.active_branch_id.clone();
        if self.opening_active_branch_head != Some(current_commit_id) {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "tracked-state transition source is no longer the active branch head",
            ));
        }
        if self
            .staged_writes
            .commit_id_for_branch(&branch_id)?
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed tracked-state transitions require a clean branch transaction",
            ));
        }
        if keys.is_empty() {
            return Err(empty_state_transition(current_commit_id, desired_commit_id));
        }
        let unique = keys.iter().collect::<BTreeSet<_>>();
        if unique.len() != keys.len() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "typed tracked-state transition contains more than one row for the same row",
            ));
        }

        let (current_rows, desired_rows) = {
            let mut tracked = self.tracked_state_reader().await?;
            let current_rows = tracked
                .load_projected_batch_at_commit(
                    &current_commit_id.to_string(),
                    &keys,
                    &ChangeRecordProjection::identity_only(),
                )
                .await?;
            let desired_rows = tracked
                .load_projected_batch_at_commit(
                    &desired_commit_id.to_string(),
                    &keys,
                    &ChangeRecordProjection::full(),
                )
                .await?;
            (current_rows, desired_rows)
        };
        let mut transitions = Vec::with_capacity(keys.len());
        for (index, identity) in keys.into_iter().enumerate() {
            let current = current_rows.row(index).filter(|row| !row.deleted());
            let desired = desired_rows.row(index).filter(|row| !row.deleted());
            for row in [current, desired].into_iter().flatten() {
                if row.schema_key() != identity.schema_key
                    || row.file_id() != identity.file_id.as_deref()
                    || row.row_pk() != &identity.row_pk
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "historical exact read returned a mismatched transition identity",
                    ));
                }
            }
            let expected_change_id = current.map(|row| row.change_id());
            let target = desired.map(|row| TypedStateTransitionTarget {
                change_id: row.change_id(),
                snapshot_content: row.snapshot_content().cloned(),
                decoded_snapshot: row.decoded_snapshot().cloned(),
                metadata: row.metadata().cloned(),
            });
            if expected_change_id != target.as_ref().map(|target| target.change_id) {
                transitions.push(TypedStateTransition {
                    identity,
                    expected_change_id,
                    target,
                });
            }
        }
        self.execute_typed_state_transitions(current_commit_id, desired_commit_id, transitions)
            .await
    }

    async fn execute_typed_state_transitions(
        &mut self,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        transitions: Vec<TypedStateTransition>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        if transitions.is_empty() {
            return Err(empty_state_transition(current_commit_id, desired_commit_id));
        }
        let branch_id = self.active_branch_id.clone();
        let rows_affected = transitions.len() as u64;
        let mut rows = RawWriteBatch::with_capacity(transitions.len());
        for transition in transitions {
            if transition.expected_change_id
                == transition.target.as_ref().map(|target| target.change_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "typed tracked-state transition contains an unchanged row",
                ));
            }
            let (snapshot, decoded_snapshot, metadata) = match transition.target {
                Some(target) => (
                    parse_materialized_diff_json(
                        target.snapshot_content,
                        "typed state transition target",
                    )?,
                    target.decoded_snapshot,
                    parse_materialized_diff_json(
                        target.metadata,
                        "typed state transition target metadata",
                    )?,
                ),
                None => (None, None, None),
            };
            let row_pk = Some(transition.identity.row_pk);
            let schema_key = transition.identity.schema_key.into();
            let file_id = transition.identity.file_id.map(Into::into);
            if let Some(decoded_snapshot) = decoded_snapshot {
                rows.push_typed_parts(
                    row_pk,
                    schema_key,
                    file_id,
                    Some(decoded_snapshot),
                    metadata,
                    None,
                    None,
                    None,
                    false,
                    None,
                    None,
                    false,
                    branch_id.clone().into(),
                );
            } else {
                rows.push(TransactionWriteRow {
                    row_pk,
                    schema_key,
                    file_id,
                    snapshot,
                    metadata,
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
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await?;
        Ok(crate::sql2::DiffCommandOutcome {
            rows_affected,
            commit_id: self
                .staged_writes
                .commit_id_for_branch(&branch_id)?
                .map(|commit_id| commit_id.to_string()),
        })
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
            records.values().cloned(),
            ChangeRecordProjection::full(),
        )?;
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
                    "selected relation row has no resolvable change",
                )
            })?);
            if let (Some(before), Some(after)) = (before, after)
                && diff_record_identity(before) != diff_record_identity(after)
            {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "selected relation row joins changes for different rows",
                ));
            }
            if !identities.insert(identity.clone()) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "diff command selection contains more than one row for the same row",
                ));
            }
            let (expected, target) = match command {
                DiffCommand::Revert => (sides.after, sides.before),
                DiffCommand::Apply => (sides.before, sides.after),
                DiffCommand::CreateCheckpoint => unreachable!(),
            };
            plans.push((diff_id, identity, expected, target));
        }
        let request = HotStateExactBatchRequest {
            rows: plans
                .iter()
                .map(
                    |(_, (schema_key, row_pk, file_id), _, _)| HotStateExactRowRequest {
                        schema_key: schema_key.clone(),
                        branch_id: branch_id.clone(),
                        row_pk: row_pk.clone(),
                        file_id: file_id.clone(),
                    },
                )
                .collect(),
            projection: HotStateProjection::default(),
            untracked: Some(false),
            include_tombstones: true,
        };
        let current = self
            .load_visible_exact_hot_state_batch(&request)
            .await?
            .into_rows();
        let mut target_change_ids = Vec::new();
        let mut rows = RawWriteBatch::with_capacity(plans.len());
        for ((diff_id, (schema_key, row_pk, file_id), expected, target), current) in
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
                    row_pk: Some(row_pk),
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
                let snapshot =
                    parse_materialized_diff_json(payload.snapshot_content, "diff target")?;
                let metadata =
                    parse_materialized_diff_json(payload.metadata, "diff target metadata")?;
                let row_pk = Some(identity.row_pk);
                let schema_key = identity.schema_key.into();
                let file_id = identity.file_id.map(Into::into);
                if let Some(decoded_snapshot) = payload.decoded_snapshot {
                    rows.push_typed_parts(
                        row_pk,
                        schema_key,
                        file_id,
                        Some(decoded_snapshot),
                        metadata,
                        None,
                        None,
                        None,
                        false,
                        None,
                        None,
                        false,
                        branch_id.clone().into(),
                    );
                } else {
                    rows.push(TransactionWriteRow {
                        row_pk,
                        schema_key,
                        file_id,
                        snapshot,
                        metadata,
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

    /// Resolves public relation identities against one coherent immutable diff.
    /// Encoded change pairs remain a private implementation detail of the
    /// existing selected/unselected machinery.
    async fn resolve_diff_command_selections(
        &mut self,
        command: DiffCommand,
        selections: &[DiffCommandSelection],
    ) -> Result<Vec<String>, LixError> {
        let unique = selections
            .iter()
            .map(|selection| (&selection.relation, &selection.row_pk))
            .collect::<BTreeSet<_>>();
        if unique.len() != selections.len() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "diff command selection contains duplicate (relation, row_pk) rows",
            ));
        }

        let catalog = self.sql_public_catalog()?;
        let mut schema_selections = BTreeMap::<&str, BTreeMap<RowPk, usize>>::new();
        let mut file_selections = BTreeMap::new();
        let mut file_descriptor_row_pks = Vec::new();
        for (index, selection) in selections.iter().enumerate() {
            match selection.relation.as_str() {
                "lix_file" => {
                    let file_id = selection.row_pk.as_single_string_owned().map_err(|_| {
                        LixError::new(
                            LixError::CODE_TYPE_MISMATCH,
                            "lix_file selections require a single file-id primary key",
                        )
                    })?;
                    let JsonValue::Array(parts) = selection.row_pk.as_json_array_value()? else {
                        unreachable!("RowPk always serializes as a JSON array")
                    };
                    let descriptor_row_pk =
                        RowPk::from_json_values(&parts, &[crate::row_pk::RowPkComponentType::Uuid])
                            .map_err(|error| {
                                LixError::new(
                                    LixError::CODE_TYPE_MISMATCH,
                                    format!("lix_file row_pk must contain a UUID file id: {error}"),
                                )
                            })?;
                    file_descriptor_row_pks.push(descriptor_row_pk);
                    file_selections.insert(file_id, index);
                }
                "lix_directory" => {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "lix_directory command selections require recursive foreign-key closure and are not supported yet",
                    ));
                }
                relation => {
                    let spec = catalog.schema_spec(relation).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_UNSUPPORTED_SQL,
                            format!("diff commands do not support relation '{relation}'"),
                        )
                    })?;
                    let JsonValue::Array(parts) = selection.row_pk.as_json_array_value()? else {
                        unreachable!("RowPk always serializes as a JSON array")
                    };
                    let typed_row_pk =
                        RowPk::from_json_values(&parts, &spec.primary_key_component_types)
                            .map_err(|error| {
                                LixError::new(
                                    LixError::CODE_TYPE_MISMATCH,
                                    format!("row_pk does not match relation '{relation}': {error}"),
                                )
                            })?;
                    schema_selections
                        .entry(relation)
                        .or_default()
                        .insert(typed_row_pk, index);
                }
            }
        }
        if !schema_selections.is_empty() && !file_selections.is_empty() {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "diff commands do not support mixing lix_file and schema relation selections",
            ));
        }

        let mut requests = Vec::new();
        if file_selections.is_empty() {
            let schema_keys = schema_selections
                .keys()
                .map(|relation| (*relation).to_string())
                .collect();
            let row_pks = schema_selections
                .values()
                .flat_map(|rows| rows.keys().cloned())
                .collect();
            requests.push(TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys,
                    row_pks,
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                retain_payloads: false,
            });
        } else {
            requests.push(TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    file_ids: file_selections
                        .keys()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                retain_payloads: false,
            });
            requests.push(TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![FILE_DESCRIPTOR_SCHEMA_KEY.to_string()],
                    row_pks: file_descriptor_row_pks,
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                retain_payloads: false,
            });
            if command == DiffCommand::CreateCheckpoint {
                requests.push(TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
                        include_tombstones: true,
                        ..TrackedStateFilter::default()
                    },
                    retain_payloads: false,
                });
            }
        }

        let source = selections
            .iter()
            .filter_map(|selection| selection.source_commits.as_ref())
            .collect::<BTreeSet<_>>();
        if source.len() > 1 {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "diff commands support one source commit pair per statement",
            ));
        }
        let mut entries = Vec::new();
        if command == DiffCommand::Apply {
            let (from_commit_id, to_commit_id) = source.into_iter().next().cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "lix_apply requires a selection from exactly one lix_diff(relation, from_commit_id, to_commit_id)",
                )
            })?;
            let mut tracked = self.tracked_state_reader().await?;
            for request in &requests {
                entries.extend(
                    tracked
                        .diff_commits(&from_commit_id, &to_commit_id, request)
                        .await?
                        .entries,
                );
            }
        } else {
            let branch_id = self.active_branch_id.clone();
            let control = BranchHeadControlContext::new()
                .reader(self.opening_read())
                .load(&branch_id)
                .await?
                .ok_or_else(|| {
                    LixError::branch_not_found(&branch_id, "resolve diff selection", "branch")
                })?;
            let checkpoint = control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("branch '{branch_id}' has no checkpoint cursor"),
                )
            })?;
            for request in &requests {
                let direct = TrackedHeadContext::new()
                    .reader(self.opening_read())
                    .working_diff_for_control(&branch_id, control, request)
                    .await?;
                if let Some(direct) = direct {
                    entries.extend(direct.diff.entries);
                } else {
                    let mut tracked = self.tracked_state_reader().await?;
                    entries.extend(
                        tracked
                            .diff_commits(
                                &checkpoint.to_string(),
                                &control.head_commit_id.to_string(),
                                request,
                            )
                            .await?
                            .entries,
                    );
                }
            }
        }

        let selected_directory_ids =
            if command == DiffCommand::CreateCheckpoint && !file_selections.is_empty() {
                self.selected_file_directory_closure(&entries, &file_selections)
                    .await?
            } else {
                BTreeSet::new()
            };
        let mut matched = BTreeSet::new();
        let mut resolved = Vec::new();
        for entry in entries {
            if entry.identity.schema_key() == CHECKPOINT_SCHEMA_KEY
                || entry.identity.schema_key() == crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
            {
                continue;
            }
            let mut selected = false;
            if let Some(index) = schema_selections
                .get(entry.identity.schema_key())
                .and_then(|rows| rows.get(entry.identity.row_pk()))
            {
                matched.insert(*index);
                selected = true;
            }
            if let Some(file_id) = entry.identity.file_id()
                && let Some(index) = file_selections.get(file_id)
            {
                matched.insert(*index);
                selected = true;
            }
            if entry.identity.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
                && let Ok(file_id) = entry.identity.row_pk().as_single_string_owned()
                && let Some(index) = file_selections.get(file_id.as_str())
            {
                matched.insert(*index);
                selected = true;
            }
            if entry.identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                && entry
                    .identity
                    .row_pk()
                    .as_single_string_owned()
                    .is_ok_and(|directory_id| selected_directory_ids.contains(&directory_id))
            {
                selected = true;
            }
            if selected {
                resolved.push(entry.diff_id()?);
            }
        }
        if matched.len() != selections.len() {
            return Err(unknown_diff_selection());
        }
        resolved.sort();
        resolved.dedup();
        Ok(resolved)
    }

    /// Includes only changed, live ancestors of selected file descriptors.
    /// Unrelated dirty directories remain in the unselected working interval.
    async fn selected_file_directory_closure(
        &mut self,
        entries: &[TrackedStateDiffEntry],
        file_selections: &BTreeMap<String, usize>,
    ) -> Result<BTreeSet<String>, LixError> {
        if !entries.iter().any(|entry| {
            entry.identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                && entry.kind != TrackedStateDiffKind::Removed
        }) {
            return Ok(BTreeSet::new());
        }
        let mut change_ids = BTreeSet::new();
        for entry in entries {
            let selected_file_descriptor = entry.identity.schema_key()
                == FILE_DESCRIPTOR_SCHEMA_KEY
                && entry
                    .identity
                    .row_pk()
                    .as_single_string_owned()
                    .is_ok_and(|file_id| file_selections.contains_key(&file_id));
            let live_directory = entry.identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                && entry.kind != TrackedStateDiffKind::Removed;
            if (selected_file_descriptor || live_directory)
                && let Some(after) = entry.after.as_ref().filter(|after| !after.deleted)
            {
                change_ids.insert(after.change_id);
            }
        }
        if change_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let mut records =
            futures_util::future::try_join_all(change_ids.iter().copied().map(|change_id| {
                let read = &read;
                async move {
                    crate::tracked_state::load_change_record_by_id(read, change_id)
                        .await
                        .map(|record| (change_id, record))
                }
            }))
            .await?
            .into_iter()
            .filter_map(|(change_id, record)| record.map(|record| (change_id, record)))
            .collect::<HashMap<_, _>>();
        let missing = change_ids
            .into_iter()
            .filter(|change_id| !records.contains_key(change_id))
            .collect::<Vec<_>>();
        records.extend(load_change_records(&read, missing.into_iter()).await?);
        let payloads = materialize_known_change_payloads(
            records.into_values(),
            ChangeRecordProjection {
                snapshot_content: true,
                metadata: false,
                snapshot: false,
                raw_snapshot: false,
            },
        )?;
        let mut file_parents = Vec::new();
        let mut directory_parents = BTreeMap::new();
        for payload in payloads.into_values() {
            let Some(identity) = payload.identity else {
                continue;
            };
            let Some(snapshot) = payload.snapshot_content else {
                continue;
            };
            let snapshot: JsonValue = serde_json::from_str(snapshot.as_ref()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("filesystem closure descriptor is invalid: {error}"),
                )
            })?;
            match identity.schema_key.as_str() {
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    if let Some(parent_id) =
                        snapshot.get("directory_id").and_then(JsonValue::as_str)
                    {
                        file_parents.push(parent_id.to_string());
                    }
                }
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let directory_id = identity.row_pk.as_single_string_owned()?;
                    let parent_id = snapshot
                        .get("parent_id")
                        .and_then(JsonValue::as_str)
                        .map(ToOwned::to_owned);
                    directory_parents.insert(directory_id, parent_id);
                }
                _ => {}
            }
        }
        let mut selected = BTreeSet::new();
        for mut directory_id in file_parents {
            while selected.insert(directory_id.clone()) {
                let Some(Some(parent_id)) = directory_parents.get(&directory_id) else {
                    break;
                };
                directory_id.clone_from(parent_id);
            }
        }
        Ok(selected)
    }

    pub(crate) async fn execute_full_checkpoint_command(
        &mut self,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let branch_id = self.active_branch_id.clone();
        let (previous_recovery, mut gc_state) =
            self.checkpoint_publication_state(&branch_id).await?;
        let head_commit_id = self
            .load_branch_head(&branch_id)
            .await?
            .ok_or_else(|| LixError::branch_not_found(&branch_id, "create checkpoint", "target"))?;
        let previous_checkpoint_commit_id = self
            .checkpoint_commit_id_at_head(&branch_id, head_commit_id)
            .await?;
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
        let commit_id = self.stage_checkpoint_boundary_commit(
            branch_id,
            previous_checkpoint_commit_id,
            head_commit_id,
            interval_has_commits,
            gc_state,
        )?;
        let checkpoint_commit_id = CommitId::parse_lix(&commit_id, "checkpoint commit id")?;
        let mut checkpoint_rows = RawWriteBatch::with_capacity(1);
        checkpoint_rows.push(checkpoint_stage_row(
            &checkpoint_commit_id,
            self.functions.call_uuid_v7().to_string(),
        ));
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: checkpoint_rows,
        })
        .await?;
        Ok(crate::sql2::DiffCommandOutcome {
            rows_affected: 1,
            commit_id: Some(commit_id),
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
        let control = BranchHeadControlContext::new()
            .reader(self.opening_read())
            .load(&branch_id)
            .await?
            .ok_or_else(|| LixError::branch_not_found(&branch_id, "create checkpoint", "target"))?;
        if control.head_commit_id != head_commit_id {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                format!("branch '{branch_id}' head changed while loading its checkpoint cursor"),
            ));
        }
        let previous_checkpoint_commit_id =
            control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("branch '{branch_id}' has no checkpoint cursor"),
                )
            })?;
        let direct_diff = TrackedHeadContext::new()
            .reader(self.opening_read())
            .working_diff_for_control(&branch_id, control, &TrackedStateDiffRequest::default())
            .await?;
        let hot_working_diff_certified = direct_diff.is_some();
        let diff = match direct_diff {
            Some(direct) => direct.diff,
            None => {
                let mut tracked = self.tracked_state_reader().await?;
                tracked
                    .diff_commits(
                        &previous_checkpoint_commit_id.to_string(),
                        &head_commit_id.to_string(),
                        &TrackedStateDiffRequest::default(),
                    )
                    .await?
            }
        };
        let requested = diff_ids.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != diff_ids.len() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "checkpoint selection contains duplicate relation-row changes",
            ));
        }
        let mut matched = BTreeSet::new();
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(diff.entries.len());
        let mut unselected = StagedCommitChangeBatchBuilder::with_capacity(diff.entries.len());
        let mut selected_source_membership_exact = true;
        let mut unselected_source_membership_exact = true;
        for entry in diff.entries.into_iter().filter(|entry| {
            entry.identity.schema_key() != CHECKPOINT_SCHEMA_KEY
                && entry.identity.schema_key() != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
        }) {
            let diff_id = entry.diff_id()?;
            let target = entry.after.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "selected working relation row has no target change",
                )
            })?;
            if requested.contains(&diff_id) {
                matched.insert(diff_id);
                selected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut selected, target, entry.kind);
            } else {
                unselected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut unselected, target, entry.kind);
            }
        }
        let selected = if selected_source_membership_exact {
            selected.finish_source_certified()
        } else {
            selected.finish()
        };
        let unselected = if unselected_source_membership_exact {
            unselected.finish_source_certified()
        } else {
            unselected.finish()
        };
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
            self.stage_checkpoint_commit(
                branch_id.clone(),
                previous_checkpoint_commit_id,
                head_commit_id,
                interval_has_commits,
                gc_state,
                selected,
                hot_working_diff_certified,
            )?
        } else {
            let checkpoint_commit_id = self.staged_writes.stage_intermediate_commit(
                branch_id.clone(),
                previous_checkpoint_commit_id,
                selected,
            )?;
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
                    hot_working_diff_certified,
                })?;
            checkpoint_commit_id.to_string()
        };
        let checkpoint_commit = CommitId::parse_lix(&checkpoint_commit_id, "checkpoint commit id")?;
        let mut checkpoint_rows = RawWriteBatch::with_capacity(1);
        checkpoint_rows.push(checkpoint_stage_row(
            &checkpoint_commit,
            self.functions.call_uuid_v7().to_string(),
        ));
        self.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: checkpoint_rows,
        })
        .await?;
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
        let source_commits = if command == DiffCommand::Apply {
            let source = diff_command_source_commits(&statement, &params)?;
            match source {
                Some((from, to)) => Some(self.resolve_diff_command_source_commits(from, to).await?),
                None => None,
            }
        } else {
            None
        };
        let result = self
            .execute_read_sql_statement(query_sql, statement, params)
            .await?;
        if result.columns.len() != 2 {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "diff command query must return relation and row_pk, got {} columns",
                    result.columns.len()
                ),
            ));
        }
        let mut selections = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let [Value::Text(relation), Value::Jsonb(row_pk)] = row.as_slice() else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "diff command query must return non-null relation text and JSONB row_pk values",
                ));
            };
            let row_pk = RowPk::from_json_array_text(&row_pk.to_string()).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("row_pk must be a JSON primary-key array: {error}"),
                )
            })?;
            selections.push(DiffCommandSelection {
                relation: relation.clone(),
                row_pk,
                source_commits: source_commits.clone(),
            });
        }
        if selections.is_empty() {
            return Ok(crate::sql2::DiffCommandOutcome {
                rows_affected: 0,
                commit_id: None,
            });
        }
        self.execute_diff_command(command, selections).await
    }

    async fn resolve_diff_command_source_commits(
        &mut self,
        from: DiffCommandSourceCommit,
        to: DiffCommandSourceCommit,
    ) -> Result<(String, String), LixError> {
        if let (DiffCommandSourceCommit::Literal(from), DiffCommandSourceCommit::Literal(to)) =
            (&from, &to)
        {
            return Ok((from.clone(), to.clone()));
        }

        let branch_id = self.active_branch_id.clone();
        let head = self.load_branch_head(&branch_id).await?.ok_or_else(|| {
            LixError::branch_not_found(&branch_id, "resolve diff source", "branch")
        })?;
        let uses_latest_checkpoint = from == DiffCommandSourceCommit::LatestCheckpoint
            || to == DiffCommandSourceCommit::LatestCheckpoint;
        let latest_checkpoint = if uses_latest_checkpoint {
            let opening_read = self.opening_read();
            let hot_state = self.hot_state.transaction_reader(
                opening_read.clone(),
                Arc::clone(&self.branch_head_control_cache),
            );
            crate::checkpoint::latest_checkpoint_commit_id_at_head(
                opening_read,
                &hot_state,
                &branch_id,
                head,
            )
            .await?
            .map(|commit_id| commit_id.to_string())
        } else {
            None
        };
        let needs_root = from == DiffCommandSourceCommit::Root
            || to == DiffCommandSourceCommit::Root
            || (uses_latest_checkpoint && latest_checkpoint.is_none());
        let root = if needs_root {
            let mut graph = CommitGraphContext::new().reader(self.opening_read());
            let mut current = head;
            loop {
                let node = graph.load_node(&current).await?.ok_or_else(|| {
                    crate::commit_graph::missing_commit_graph_error(&current)
                })?;
                let Some(first_parent) = node.parent_commit_ids.first().copied() else {
                    break Some(node.commit_id.to_string());
                };
                current = if node.first_parent_jump_span > 0 {
                    node.first_parent_jump_commit_id
                } else {
                    first_parent
                };
            }
        } else {
            None
        };
        let resolve = |source| match source {
            DiffCommandSourceCommit::Literal(value) => value,
            DiffCommandSourceCommit::ActiveHead => head.to_string(),
            DiffCommandSourceCommit::Root => root
                .clone()
                .expect("a repository root was resolved when either source requested it"),
            DiffCommandSourceCommit::LatestCheckpoint => latest_checkpoint
                .clone()
                .or_else(|| root.clone())
                .expect("a latest checkpoint or repository root was resolved when requested"),
        };
        Ok((resolve(from), resolve(to)))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DiffCommandSourceCommit {
    Literal(String),
    Root,
    ActiveHead,
    LatestCheckpoint,
}

fn diff_command_source_commits(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Result<Option<(DiffCommandSourceCommit, DiffCommandSourceCommit)>, LixError> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, TableFactor,
        Value as SqlValue, Visit, Visitor,
    };

    struct SourceVisitor<'a> {
        params: &'a [Value],
        sources: Vec<(DiffCommandSourceCommit, DiffCommandSourceCommit)>,
        error: Option<LixError>,
    }

    impl Visitor for SourceVisitor<'_> {
        type Break = ();

        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(arguments),
                ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            if !name.to_string().eq_ignore_ascii_case("lix_diff") {
                return ControlFlow::Continue(());
            }
            if arguments.args.len() != 3 {
                return ControlFlow::Continue(());
            }
            let resolve = |argument: &FunctionArg| -> Result<DiffCommandSourceCommit, LixError> {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument else {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "diff command source commits must be text literals, parameters, or root/checkpoint/head functions",
                    ));
                };
                match expression {
                    SqlExpr::Value(value) => match &value.value {
                        SqlValue::SingleQuotedString(value) => {
                            Ok(DiffCommandSourceCommit::Literal(value.clone()))
                        }
                        SqlValue::Placeholder(placeholder) => {
                            let index = placeholder
                                .strip_prefix('$')
                                .and_then(|index| index.parse::<usize>().ok())
                                .and_then(|index| index.checked_sub(1));
                            match index.and_then(|index| self.params.get(index)) {
                                Some(Value::Text(value)) => {
                                    Ok(DiffCommandSourceCommit::Literal(value.clone()))
                                }
                                _ => Err(LixError::new(
                                    LixError::CODE_TYPE_MISMATCH,
                                    "diff command source commit parameters must be non-null text",
                                )),
                            }
                        }
                        _ => Err(LixError::new(
                            LixError::CODE_TYPE_MISMATCH,
                            "diff command source commits must be non-null text",
                        )),
                    },
                    SqlExpr::Function(function) if matches!(&function.args, FunctionArguments::List(arguments) if arguments.args.is_empty()) => {
                        if function
                            .name
                            .to_string()
                            .eq_ignore_ascii_case("lix_root_commit_id")
                        {
                            Ok(DiffCommandSourceCommit::Root)
                        } else if function
                            .name
                            .to_string()
                            .eq_ignore_ascii_case("lix_active_branch_commit_id")
                        {
                            Ok(DiffCommandSourceCommit::ActiveHead)
                        } else if function
                            .name
                            .to_string()
                            .eq_ignore_ascii_case("lix_latest_checkpoint_commit_id")
                        {
                            Ok(DiffCommandSourceCommit::LatestCheckpoint)
                        } else {
                            Err(LixError::new(
                                LixError::CODE_UNSUPPORTED_SQL,
                                "diff command source commits only support lix_root_commit_id(), lix_latest_checkpoint_commit_id(), and lix_active_branch_commit_id() functions",
                            ))
                        }
                    }
                    _ => Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "diff command source commits must be text literals, parameters, or root/checkpoint/head functions",
                    )),
                }
            };
            match (resolve(&arguments.args[1]), resolve(&arguments.args[2])) {
                (Ok(from), Ok(to)) => self.sources.push((from, to)),
                (Err(error), _) | (_, Err(error)) => {
                    self.error = Some(error);
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = SourceVisitor {
        params,
        sources: Vec::new(),
        error: None,
    };
    if let DataFusionStatement::Statement(statement) = statement {
        let _ = statement.visit(&mut visitor);
    }
    if let Some(error) = visitor.error {
        return Err(error);
    }
    visitor.sources.sort();
    visitor.sources.dedup();
    if visitor.sources.len() > 1 {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "diff commands support exactly one source commit pair per statement",
        ));
    }
    Ok(visitor.sources.into_iter().next())
}

fn unknown_diff_selection() -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        "selected relation row is not present in the source diff",
    )
}

async fn load_immutable_mutation_predecessors<R>(
    reader: &R,
    schema_key: &str,
    branch_id: &str,
    row_pk_chunks: &[Arc<[RowPk]>],
) -> Result<Vec<CertifiedCurrentStatePredecessor>, LixError>
where
    R: HotStateReader + ?Sized,
{
    let row_count = row_pk_chunks.iter().map(|chunk| chunk.len()).sum();
    let mut predecessors = Vec::with_capacity(row_count);
    for row_pks in row_pk_chunks {
        let request = HotStateExactBatchRequest {
            rows: row_pks
                .iter()
                .cloned()
                .map(|row_pk| HotStateExactRowRequest {
                    schema_key: schema_key.to_owned(),
                    branch_id: branch_id.to_owned(),
                    row_pk,
                    file_id: None,
                })
                .collect(),
            projection: HotStateProjection::default(),
            untracked: Some(false),
            include_tombstones: false,
        };
        let current = reader.load_exact_batch(&request).await?;
        for (slot, expected_row_pk) in row_pks.iter().enumerate() {
            let row = current.row(slot).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation lost its current-state predecessor",
                )
            })?;
            if row.schema_key() != schema_key
                || row.branch_id() != branch_id
                || row.row_pk() != expected_row_pk
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation predecessor order changed",
                ));
            }
            predecessors.push(row.durable_predecessor().cloned().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation predecessor lacks durable evidence",
                )
            })?);
        }
    }
    Ok(predecessors)
}

fn conflict_resolution_limits(conflict_count: usize) -> Result<WasmTransitionLimits, LixError> {
    let conflict_count = u32::try_from(conflict_count).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "semantic conflict count exceeds the component protocol ordinal range",
        )
    })?;
    let mut limits = WasmTransitionLimits::default();
    // Each conflict can reference base, A, and B snapshots. These are
    // host-owned attachments from an already admitted finite batch, so bound
    // the source by its actual worst-case reference count instead of the
    // small-transition default.
    limits.max_attachment_refs = limits
        .max_attachment_refs
        .max(conflict_count.saturating_mul(3));
    // A resolver may emit one bounded replacement page per conflict. Keep the
    // independent aggregate byte and deadline limits unchanged.
    limits.max_pages = limits.max_pages.max(conflict_count.saturating_add(1));
    Ok(limits)
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
        "selected relation row changed; re-evaluate the source diff and retry",
    )
}

fn empty_state_transition(current: CommitId, desired: CommitId) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked-state transition from '{current}' to '{desired}' is empty"),
    )
}

async fn opening_parent_complete_lifecycle_created_at(
    read: &(impl StorageAdapterRead + ?Sized),
    parent_commit_id: Option<CommitId>,
    schema_key: &str,
    live_count: u64,
    ordered_identity_digest: [u8; 32],
) -> Result<Option<LixTimestamp>, LixError> {
    let Some(opening_parent_commit_id) = parent_commit_id else {
        return Ok(None);
    };
    let expected_scope = crate::tracked_state::CommitDeltaReplacementScope {
        schema_key: schema_key.to_owned(),
        file_id: None,
    };
    let Ok(expected_member_count) = u32::try_from(live_count) else {
        return Ok(None);
    };
    let mut current = Some(opening_parent_commit_id);
    let mut seen = BTreeSet::new();
    while let Some(commit_id) = current {
        if !seen.insert(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "opening parent lifecycle traversal contains a first-parent cycle",
            ));
        }
        let Some(metadata) =
            crate::tracked_state::load_commit_delta_replay_metadata(read, commit_id).await?
        else {
            return Ok(None);
        };
        if metadata.member_count == expected_member_count
            && metadata.single_partition.as_ref() == Some(&expected_scope)
            && let Some(summary) = metadata.lifecycle_summary.as_ref()
            && summary.scope == expected_scope
            && summary.ordered_identity_digest == ordered_identity_digest
        {
            return Ok(Some(summary.uniform_created_at));
        }
        // A certified commit touching only a different partition cannot alter
        // this collection's identity or created_at. Walk across such engine
        // bookkeeping roots (for example generation/catalog publication) to
        // the nearest lifecycle authority. A same-scope or mixed commit is a
        // hard boundary because it may contain delete/reinsert churn.
        if metadata.member_count != 0
            && metadata
                .single_partition
                .as_ref()
                .is_none_or(|scope| scope == &expected_scope)
        {
            return Ok(None);
        }
        let record = crate::changelog::ChangelogContext::new()
            .reader(read)
            .load_commits(CommitLoadRequest {
                commit_ids: &[commit_id],
            })
            .await?
            .into_iter()
            .next()
            .and_then(|(_, record)| record)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("opening lifecycle parent '{commit_id}' is missing"),
                )
            })?;
        current = record.parent_commit_ids.first().copied();
    }
    Ok(None)
}

async fn resolve_prepared_mutation_collection_generation(
    seed: Option<(String, Option<(u64, [u8; 32])>)>,
    read: impl StorageAdapterRead + Send,
    hot_state: Arc<HotStateContext>,
    branch_head_control_cache: Arc<BranchHeadControlCache>,
    branch_id: String,
) -> Result<Option<(String, (u64, [u8; 32]))>, LixError> {
    let Some((schema_key, generation)) = seed else {
        return Ok(None);
    };
    if let Some(generation) = generation {
        return Ok(Some((schema_key, generation)));
    }
    let base = hot_state.transaction_reader(read, branch_head_control_cache);
    let generation = base
        .collection_generation(
            &branch_id,
            crate::collection_generation::CollectionScopeRef {
                schema_key: &schema_key,
                file_id: None,
            },
        )
        .await?
        .and_then(|generation| {
            generation
                .ordered_identity_digest
                .map(|digest| (generation.live_count, digest))
        });
    Ok(generation.map(|generation| (schema_key, generation)))
}

async fn load_opening_exact_hot_state_batch(
    read: impl StorageAdapterRead + Send,
    hot_state: Arc<HotStateContext>,
    branch_head_control_cache: Arc<BranchHeadControlCache>,
    request: &HotStateExactBatchRequest,
) -> Result<MaterializedHotStateExactBatch, LixError> {
    let base = hot_state.transaction_reader(read, branch_head_control_cache);
    base.load_exact_batch(request).await
}

fn diff_record_identity(record: &ChangeRecord) -> (String, RowPk, Option<String>) {
    (
        record.schema_key.clone(),
        record.row_pk.clone(),
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
) -> bool {
    let created_at = match kind {
        TrackedStateDiffKind::Added => row.updated_at,
        TrackedStateDiffKind::Modified | TrackedStateDiffKind::Removed => row.created_at,
    };
    let source_membership_exact = created_at == row.created_at;
    selected.push(
        row.identity,
        row.commit_id,
        row.change_id,
        row.deleted,
        created_at,
        row.updated_at,
    );
    source_membership_exact
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
    active_account_id: String,
    read_store: SharedStorageAdapterRead<R>,
    hot_state: Arc<HotStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    branch_ctx: Arc<BranchContext>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    staged: PreparedStateRowOverlay,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    branch_head_control_cache: Arc<BranchHeadControlCache>,
    plugin_host: PluginRuntimeHost,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    sql_catalog_fingerprint: CatalogFingerprint,
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

    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        self.sql_planning_cache.datafusion_session()
    }

    fn datafusion_read_session(&self) -> crate::sql2::PooledReadSession {
        self.sql_planning_cache.datafusion_read_session()
    }

    async fn sql_planning_environment(
        &self,
    ) -> Result<
        Option<(
            Arc<SqlPlanningCache<CatalogFingerprint>>,
            CatalogFingerprint,
        )>,
        LixError,
    > {
        Ok(Some((
            Arc::clone(&self.sql_planning_cache),
            self.sql_catalog_fingerprint.clone(),
        )))
    }

    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn hot_state(&self) -> Arc<dyn HotStateReader> {
        Arc::new(TransactionReadHotStateReader {
            base: self.hot_state.transaction_reader(
                self.read_store.clone(),
                Arc::clone(&self.branch_head_control_cache),
            ),
            read_store: self.read_store.clone(),
            staged: self.staged.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
        })
    }

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(TransactionReadHotStateReader {
            base: self.hot_state.transaction_reader(
                self.read_store.clone(),
                Arc::clone(&self.branch_head_control_cache),
            ),
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
            default_as_of_commit_id,
        }
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            store: self.read_store.clone(),
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
    async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        load_transaction_blob_bytes(self.base.as_ref(), &self.staged_writes, hashes).await
    }
}

async fn load_transaction_blob_bytes(
    base: &dyn BlobDataReader,
    staged_writes: &TransactionWriteBuffer,
    hashes: &[BlobId],
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

struct TransactionReadHotStateReader<R: crate::storage_adapter::StorageRead> {
    base: crate::hot_state::HotStateContextReader<SharedStorageAdapterRead<R>>,
    read_store: SharedStorageAdapterRead<R>,
    staged: PreparedStateRowOverlay,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
}

#[async_trait]
impl<R> HotStateReader for TransactionReadHotStateReader<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    async fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        overlay_scan_batch(&self.base, &self.staged, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        overlay_load_exact_batch(&self.base, &self.staged, request).await
    }
}

#[async_trait]
impl<R> FilesystemPathIndexReader for TransactionReadHotStateReader<R>
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
            overlay_scan_batch(&self.base, &self.staged, &request.hot_state_request()).await?;
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

/// Erases the storage borrow lifetime for scoped transaction SQL execution.
///
/// # Safety
///
/// The returned read scope must not outlive the storage value that produced
/// `read`, and it must be dropped before that value.
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
    addressable_change_id: bool,
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
    addressable_change_ids: Vec<bool>,
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
            addressable_change_ids: Vec::with_capacity(capacity),
            commit_ids: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, row: PreparedScalarRow) {
        self.schema_plan_ids.push(row.schema_plan_id);
        self.facts.push(row.facts);
        self.created_at.push(row.created_at);
        self.updated_at.push(row.updated_at);
        self.change_ids.push(row.change_id);
        self.addressable_change_ids.push(row.addressable_change_id);
        self.commit_ids.push(row.commit_id);
    }

    fn row(&self, index: usize) -> PreparedScalarRow {
        PreparedScalarRow {
            schema_plan_id: self.schema_plan_ids[index],
            facts: self.facts[index],
            created_at: self.created_at[index],
            updated_at: self.updated_at[index],
            change_id: self.change_ids[index],
            addressable_change_id: self.addressable_change_ids[index],
            commit_id: self.commit_ids[index],
        }
    }
}

fn plan_prepared_row_scalars(
    row: RawWriteRowRef<'_>,
    normalized: NormalizedRowFacts,
    functions: &FunctionProviderHandle,
    default_timestamp: &mut Option<LixTimestamp>,
) -> Result<PreparedScalarRow, LixError> {
    let NormalizedRowFacts {
        schema_plan_id,
        facts,
    } = normalized;
    let updated_at = match row.updated_at {
        Some(updated_at) => parse_prepared_timestamp("updated_at", updated_at)?,
        None => *default_timestamp.get_or_insert_with(|| functions.call_timestamp()),
    };
    let created_at = match row.created_at {
        Some(created_at) => parse_prepared_timestamp("created_at", created_at)?,
        None => updated_at,
    };
    if row.row_pk.is_none() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "normalized transaction write row is missing row_pk",
        ));
    }
    let addressable_change_id = row.change_id.is_none();
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
        addressable_change_id,
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
    let durable_predecessor = rows.take_durable_predecessor(row_index);
    let row = rows.row(row_index);
    let decoded_snapshot = row.decoded_snapshot().cloned();
    let schema_key = row.schema_key.clone();
    let file_id = row.file_id.cloned();
    let origin = row.origin.cloned();
    let global = row.global;
    let untracked = row.untracked;
    let branch_id = row.branch_id.clone();
    let had_snapshot = rows.take_snapshot(row_index).is_some();
    let metadata = rows.take_metadata(row_index).map(stage_json_from_value);
    let row_pk = rows.take_row_pk(row_index).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "normalized transaction write row is missing row_pk",
        )
    })?;
    prepared.push_parts_with_change_addressability(
        scalar.schema_plan_id,
        scalar.facts,
        row_pk,
        schema_key,
        file_id,
        None,
        metadata,
        origin,
        origin_key,
        scalar.created_at,
        scalar.updated_at,
        global,
        scalar.change_id,
        scalar.addressable_change_id,
        scalar.commit_id,
        untracked,
        branch_id,
    );
    prepared.set_durable_predecessor(prepared.len() - 1, durable_predecessor);
    prepared.set_decoded_snapshot(prepared.len() - 1, decoded_snapshot);
    if had_snapshot && prepared.last().is_some_and(|row| row.snapshot.is_none()) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "schema-bound preparation did not produce a typed snapshot",
        ));
    }
    Ok(())
}

fn reject_plugin_owned_json_row(
    rows: &RawWriteBatch,
    row_index: usize,
    plugin_host: &PluginRuntimeHost,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    if !row.plugin_owned {
        return Ok(());
    }
    if row.decoded_snapshot().is_some() {
        return Ok(());
    }
    if row.snapshot.is_none() {
        return Ok(());
    }
    record_forbidden_plugin_json_ingress(row, plugin_host);
    Err(LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "plugin-owned SQL row '{}' used removed JSON snapshot ingress; emit a fingerprinted native typed row",
            row.schema_key
        ),
    ))
}

fn record_forbidden_plugin_json_ingress(row: RawWriteRowRef<'_>, plugin_host: &PluginRuntimeHost) {
    let mut counters = WasmTransitionCounters::default();
    counters.record_outer_row_json_operation(
        OuterRowJsonOperation::DomFallback,
        row.snapshot_json()
            .map_or(0, |snapshot| snapshot.normalized().len() as u64),
    );
    plugin_host.record_transition_counters(counters);
}

/// Returns the sole schema-catalog scope for a straightforward statement
/// batch. The SQL path normally reaches this with one row schema and one
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
        .flat_map(
            crate::transaction::staged_commit_changes::StagedCommitChangeRefs::selected_changes,
        )
        .any(|change_ref| change_ref.schema_key() == REGISTERED_SCHEMA_KEY)
}

/// Whether this commit's staged filesystem rows are an incomplete description
/// of how the visible filesystem changed, so cached path indexes must be
/// rebuilt from hot state rather than advanced by a delta.
///
/// Three shapes are incomplete, and all are about *which rows exist*, not about
/// when their identities are assigned:
///
/// * a **branch ref move** republishes the branch's entire visible filesystem;
///   the difference is between two commits, not between staged rows;
/// * a **change selected into this commit from another commit** (merge,
///   checkpoint, cherry-pick) enters the branch's visible filesystem without
///   appearing in `state_rows` at all;
/// * a **global or untracked filesystem row** changes visibility in every
///   branch at once, so a single-branch delta cannot describe it.
///
/// The last one is also the only shape whose staged rows can be *dropped*
/// between here and the capture point: `retain_untracked_rows_not_superseded_by_engine`
/// removes untracked rows that an engine row supersedes. Deciding it here, off
/// the complete pre-materialization row set, is what keeps the capture point
/// from silently projecting a delta that lost a row. `advance_committed`
/// evicts on the same condition, but it can only see the rows it is handed.
///
/// An ordinary create, rename, delete, or content write is fully described by
/// its staged rows and is therefore projectable. Those rows are read back out
/// of the commit once materialization has assigned their final change ids —
/// see [`commit::MaterializedCommit::filesystem_delta_rows`].
fn prepared_writes_require_filesystem_index_rebuild(prepared_writes: &PreparedWriteSet) -> bool {
    prepared_writes.state_rows.iter().any(|row| {
        row.schema_key == BRANCH_REF_SCHEMA_KEY
            || ((row.global || row.untracked)
                && matches!(
                    row.schema_key.as_str(),
                    "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
                ))
    }) || prepared_writes
        .commit_change_refs_by_branch
        .values()
        .flat_map(
            crate::transaction::staged_commit_changes::StagedCommitChangeRefs::selected_changes,
        )
        .any(|change_ref| {
            matches!(
                change_ref.schema_key(),
                "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
            )
        })
}

/// Whether this commit stages any row that the cached path index projects.
fn prepared_writes_stage_filesystem_rows(prepared_writes: &PreparedWriteSet) -> bool {
    prepared_writes.state_rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
        )
    })
}

pub(crate) struct OpenTransaction<StorageImpl: Storage + 'static = Memory> {
    pub(crate) transaction: Transaction<StorageImpl>,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction<StorageImpl>(
    session_branch: &SessionBranch,
    active_account_id: String,
    storage: StorageAdapter<StorageImpl>,
    hot_state: Arc<HotStateContext>,
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
    let (opened, ()) = Transaction::open(
        session_branch,
        active_account_id,
        storage,
        hot_state,
        tracked_state,
        binary_cas,
        plugin_host,
        branch_ctx,
        catalog_context,
        sql_planning_cache,
        session_file_views,
        async |_| Ok(()),
    )
    .await?;
    Ok(opened)
}

pub(crate) async fn open_transaction_with_runtime_boundary<StorageImpl, T, F>(
    session_branch: &SessionBranch,
    active_account_id: String,
    storage: StorageAdapter<StorageImpl>,
    hot_state: Arc<HotStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    session_file_views: SessionFileViews,
    runtime_boundary: F,
) -> Result<(OpenTransaction<StorageImpl>, T), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    F: for<'runtime> AsyncFnOnce(&'runtime FunctionContext) -> Result<T, LixError>,
{
    Transaction::open(
        session_branch,
        active_account_id,
        storage,
        hot_state,
        tracked_state,
        binary_cas,
        plugin_host,
        branch_ctx,
        catalog_context,
        sql_planning_cache,
        session_file_views,
        runtime_boundary,
    )
    .await
}

#[async_trait]
impl<StorageImpl> SqlWriteExecutionContext for Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn ensure_statement_allowed_after_restore(&self) -> Result<(), LixError> {
        if self.pending_restore_targets.is_empty() {
            return Ok(());
        }
        Err(LixError::new(
            "LIX_INVALID_TRANSACTION_STATE",
            "lix_restore must be the final statement before commit or rollback",
        ))
    }

    fn write_context_liveness(&self) -> crate::sql2::WriteContextLiveness {
        self.write_context_liveness.clone()
    }
    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    fn datafusion_session(&self) -> datafusion::prelude::SessionContext {
        self.sql_planning_cache.datafusion_session()
    }

    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn current_timestamp(&mut self) -> LixTimestamp {
        *self
            .current_timestamp
            .get_or_insert_with(|| self.functions.call_timestamp())
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

    fn tracked_schema_catalog_snapshot(&self) -> Option<Arc<CatalogSnapshot>> {
        Some(Arc::clone(&self.tracked_schema_snapshot))
    }

    fn plugin_owns_schema(&self, schema_key: &str) -> bool {
        self.opening_plugin_registry.owns_schema(schema_key)
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

    async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let base = self.binary_cas.reader(read);
        load_transaction_blob_bytes(&base, &self.staged_writes, hashes).await
    }

    async fn scan_hot_state_batch(
        &mut self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.scan_visible_hot_state_batch(request).await
    }

    async fn load_exact_hot_state_batch(
        &mut self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.load_visible_exact_hot_state_batch(request).await
    }

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let read = self.opening_read();
        let descriptor_epoch = self.filesystem_path_index_epoch.load(Ordering::SeqCst);
        if descriptor_epoch == 0 {
            return self
                .hot_state
                .snapshot_reader(read)
                .path_index(request)
                .await;
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
        let base = self.hot_state.snapshot_reader(read);
        let rows = overlay_scan_batch(&base, &staged, &request.hot_state_request()).await?;
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
        let read = self.opening_read();

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
            .collection_generation(branch_id, control.tracked_generation, scope)
            .await?;
        let staged = self.staged_writes.staging_overlay()?;
        if StagedHotStateRows::collection_replaced(
            &staged,
            branch_id,
            scope.schema_key,
            scope.file_id,
        )? {
            generation.live_count = 0;
        }
        Ok(Some(generation))
    }

    async fn load_exact_collection_live_count(
        &mut self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<u64>, LixError> {
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
        TrackedHeadContext::new()
            .reader(read)
            .exact_collection_live_count(branch_id, control.tracked_generation, scope)
            .await
            .map(Some)
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
            .staged_batch(&HotStateScanRequest {
                filter: HotStateFilter {
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

    async fn stage_parameter_batch_insert(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        if rows.certified_preparation().is_some() {
            return Box::pin(self.stage_certified_raw_parameter_batch_insert(rows)).await;
        }
        let statement_indices = (0..rows.len())
            .map(|index| {
                u32::try_from(index).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "parameter batch row count exceeds u32",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::stage_parameter_batch_insert(
            self,
            TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            },
            statement_indices,
        )
        .await
    }

    async fn stage_certified_parameter_batch_insert(
        &mut self,
        rows: CertifiedParameterInsertBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        self.ensure_plugin_generation_read_guard().await;

        let branch_id = rows.schema_scope_branch_id().to_owned();
        let schema_key = rows.schema_key().to_owned();
        if !rows.is_fileless_typed_sql_rows()
            && self
                .visible_plugin_registry_owns_schema(&branch_id, &schema_key)
                .await?
        {
            let mut raw = rows.into_raw()?;
            raw.revoke_certified_preparation();
            let statement_indices = (0..row_count)
                .map(|index| {
                    u32::try_from(index).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "parameter batch row count exceeds u32",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Box::pin(self.stage_write_inner(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: raw,
                },
                Some(statement_indices),
            ))
            .await;
        }
        let staged = self.staged_writes.staging_overlay()?;
        if StagedHotStateRows::collection_replaced(&staged, &branch_id, &schema_key, None)? {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!("collection '{schema_key}' was deleted earlier in this transaction"),
            )
            .with_hint(
                "Commit the collection deletion before recreating rows in its next generation.",
            ));
        }

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(
                row_count * usize::from(rows.untracked()),
            );
        }
        let domain = Domain::schema_catalog(branch_id, rows.untracked());
        let prepared = if self
            .staged_writes
            .has_staged_schema_catalog_change(&domain)?
        {
            self.prepare_transaction_rows(rows.into_raw()?)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.transaction_prepare_rows"
                ))
                .await?
        } else {
            rows.into_prepared(
                self.origin_key.as_ref(),
                self.functions.call_timestamp(),
                &self.functions,
            )?
        };
        if prepared.len() != row_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified parameter INSERT preparation changed row cardinality",
            ));
        }
        tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage").in_scope(
            || {
                self.staged_writes.stage_certified_parameter_batch_insert(
                    PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: prepared,
                    },
                )
            },
        )
    }

    async fn stage_parameter_batch_replace(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        if rows.certified_preparation().is_some() {
            return self.stage_certified_parameter_batch_replace(rows).await;
        }
        Self::stage_write(
            self,
            TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            },
        )
        .await
    }

    async fn stage_certified_parameter_batch_replace(
        &mut self,
        rows: CertifiedParameterReplacementBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        self.ensure_plugin_generation_read_guard().await;
        let branch_id = rows.schema_scope_branch_id().to_owned();
        let schema_key = rows.schema_key().to_owned();
        if self
            .visible_plugin_registry_owns_schema(&branch_id, &schema_key)
            .await?
        {
            let mut raw = rows.into_raw()?;
            raw.revoke_certified_preparation();
            return Box::pin(self.stage_write_inner(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows: raw,
                },
                None,
            ))
            .await;
        }
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(
                row_count * usize::from(rows.untracked()),
            );
        }
        let domain =
            Domain::schema_catalog(rows.schema_scope_branch_id().to_string(), rows.untracked());
        let prepared = if self
            .staged_writes
            .has_staged_schema_catalog_change(&domain)?
        {
            self.prepare_transaction_rows(rows.into_raw()?)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.transaction_prepare_rows"
                ))
                .await?
        } else {
            rows.into_prepared(
                self.origin_key.as_ref(),
                self.functions.call_timestamp(),
                &self.functions,
            )?
        };
        if prepared.len() != row_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified replacement preparation changed row cardinality",
            ));
        }
        tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage").in_scope(
            || {
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: prepared,
                    })
            },
        )
    }

    async fn stage_typed_mutation_journal_replace(
        &mut self,
        rows: TypedMutationJournalBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        self.ensure_plugin_generation_read_guard().await;
        let domain = Domain::schema_catalog(rows.branch_id.to_string(), false);
        if self.origin_key.is_some()
            || self
                .staged_writes
                .has_staged_schema_catalog_change(&domain)?
        {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "typed mutation journals require a stable schema and direct transaction origin",
            ));
        }
        let Some(parent_created_at) = opening_parent_complete_lifecycle_created_at(
            &self.opening_read(),
            self.opening_active_branch_head,
            rows.schema_key.as_str(),
            u64::try_from(row_count).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "typed mutation journal row count exceeds u64",
                )
            })?,
            rows.expected_ordered_identity_digest,
        )
        .await?
        else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed mutation journal lacks complete parent lifecycle authority",
            ));
        };

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let expected_ordered_identity_digest = rows.expected_ordered_identity_digest;
        let schema_key = rows.schema_key.clone();
        let branch_id = rows.branch_id.clone();
        let chunk = ImmutableMutationJournalChunk::try_new_typed_single_string_identities(
            rows.schema_plan_id,
            rows.schema_key,
            rows.branch_id,
            None,
            rows.identity_arena,
            rows.identity_offsets,
            rows.snapshot_arena,
            rows.snapshot_offsets,
            None,
            self.functions.call_timestamp(),
        )?;
        match self.staged_writes.stage_immutable_mutation_chunk(chunk)? {
            ImmutableMutationChunkStage::Staged => {
                if !self.staged_writes.certify_complete_collection_replacement(
                    schema_key.as_str(),
                    branch_id.as_str(),
                    u64::try_from(row_count).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "typed mutation journal row count exceeds u64",
                        )
                    })?,
                    expected_ordered_identity_digest,
                )? {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "typed mutation journal lost its complete replacement scope",
                    ));
                }
                self.staged_writes.set_ordered_mutation_overlay_created_at(
                    self.opening_active_branch_head.expect(
                        "a certified opening parent lifecycle requires an opening branch head",
                    ),
                    parent_created_at,
                )?;
            }
            ImmutableMutationChunkStage::RequiresGeneric(_) => {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "typed mutation journal overlaps an existing transaction mutation lane",
                ));
            }
        }
        Ok(TransactionWriteOutcome {
            count: u64::try_from(row_count).expect("typed mutation journal row count fits u64"),
        })
    }

    async fn can_stage_typed_mutation_journal_replace(
        &mut self,
        schema_key: &str,
        live_count: u64,
        ordered_identity_digest: [u8; 32],
    ) -> Result<bool, LixError> {
        let domain = Domain::schema_catalog(self.active_branch_id.clone(), false);
        if self.origin_key.is_some() {
            return Ok(false);
        }
        if self
            .staged_writes
            .has_staged_schema_catalog_change(&domain)?
        {
            return Ok(false);
        }
        opening_parent_complete_lifecycle_created_at(
            &self.opening_read(),
            self.opening_active_branch_head,
            schema_key,
            live_count,
            ordered_identity_digest,
        )
        .await
        .map(|created_at| created_at.is_some())
    }

    async fn execute_diff_command(
        &mut self,
        command: DiffCommand,
        selections: Vec<DiffCommandSelection>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let selected_rows = selections.len() as u64;
        let diff_ids = self
            .resolve_diff_command_selections(command, &selections)
            .await?;
        let mut outcome = match command {
            DiffCommand::Revert | DiffCommand::Apply => {
                self.execute_apply_or_revert(command, diff_ids).await
            }
            DiffCommand::CreateCheckpoint => self.execute_checkpoint_selection(diff_ids).await,
        }?;
        outcome.rows_affected = selected_rows;
        Ok(outcome)
    }

    async fn restore_active_branch(&mut self, commit_id: String) -> Result<(), LixError> {
        let branch_id = self.active_branch_id().to_string();
        self.ensure_statement_allowed_after_restore()?;
        if self.staged_writes.has_staged_state_rows()? {
            return Err(LixError::new(
                "LIX_INVALID_TRANSACTION_STATE",
                "lix_restore cannot follow another write in the same transaction",
            ));
        }
        let target_commit_id = BranchLifecycle::parse_commit_id(
            &commit_id,
            BranchOperation::Restore,
            BranchReferenceRole::Target,
        )?;
        let head_commit_id = {
            let reader = self.branch_ref_reader().await?;
            BranchLifecycle::new(&reader)
                .require_existing_commit_id(
                    &branch_id,
                    BranchOperation::Restore,
                    BranchReferenceRole::Source,
                )
                .await?
        };

        let mut commit_graph = self.commit_graph_reader().await?;
        BranchLifecycle::require_existing_commit(
            &mut commit_graph,
            target_commit_id,
            BranchOperation::Restore,
            BranchReferenceRole::Target,
        )
        .await?;

        if target_commit_id == head_commit_id {
            return Ok(());
        }
        let target_is_ancestor = commit_graph
            .reachable_nodes(&head_commit_id)
            .await?
            .iter()
            .any(|reachable| reachable.commit.commit_id == target_commit_id);
        if !target_is_ancestor {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!(
                    "restore target commit '{target_commit_id}' is not an ancestor of branch '{branch_id}' HEAD '{head_commit_id}'"
                ),
            ));
        }
        drop(commit_graph);

        self.restore_branch_ref(&branch_id, head_commit_id, target_commit_id)
            .await
    }

    fn staged_commit_id(&self, branch_id: &str) -> Result<Option<String>, LixError> {
        self.staged_writes
            .commit_id_for_branch(branch_id)
            .map(|commit_id| commit_id.map(|commit_id| commit_id.to_string()))
    }
}

fn prepared_transaction_write_filesystem_index_impact(
    write: &PreparedTransactionWrite,
) -> Result<(bool, Vec<MaterializedHotStateRow>), LixError> {
    let mut affects_index = false;
    let mut delta_rows = Vec::new();
    for row in prepared_transaction_write_rows(write).iter() {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY => {
                affects_index = true;
                delta_rows.push(
                    crate::transaction_types::materialized_hot_state_row_with_snapshot_projection(
                        row,
                    )?,
                );
            }
            BRANCH_REF_SCHEMA_KEY => affects_index = true,
            _ => {}
        }
    }
    Ok((affects_index, delta_rows))
}

fn prepared_transaction_write_rows(write: &PreparedTransactionWrite) -> &PreparedStateBatch {
    match write {
        PreparedTransactionWrite::Rows { rows, .. }
        | PreparedTransactionWrite::RowsWithFileContent { rows, .. } => rows,
    }
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

fn advance_transaction_path_index_cache_revision(
    revision: &[u8],
    previous_epoch: usize,
    next_epoch: usize,
) -> Option<Vec<u8>> {
    const PREFIX: &[u8] = b"transaction-path-index-v1";
    let epoch_end = PREFIX.len().checked_add(size_of::<usize>())?;
    if revision.len() < epoch_end
        || !revision.starts_with(PREFIX)
        || revision[PREFIX.len()..epoch_end] != previous_epoch.to_be_bytes()
    {
        return None;
    }
    let mut next_revision = revision.to_vec();
    next_revision[PREFIX.len()..epoch_end].copy_from_slice(&next_epoch.to_be_bytes());
    Some(next_revision)
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
    write: &TransactionFileContent,
    plugin: &PluginRegistryEntry,
) -> WasmFileDescriptor {
    WasmFileDescriptor {
        file_id: write.file_id.clone(),
        path: write.path.clone(),
        plugin: WasmPluginSelection {
            plugin_key: plugin.key().to_string(),
            generation: plugin.archive_blob_hash().to_string(),
        },
    }
}

fn v2_file_descriptor_from_actor_key(key: &PluginActorKey) -> WasmFileDescriptor {
    WasmFileDescriptor {
        file_id: key.file_id.clone(),
        path: Some(key.path.clone()),
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
fn v2_create_context(seed: [u8; 16], actor_key: &PluginActorKey) -> WasmCreateContext {
    BoundCreateContext::bind(local_mutation_identity(seed), actor_key)
        .expect("local mutation seeds are generated as UUIDv7")
        .creates()
}

fn suppress_format_only_noops_against_batch(
    changes: WasmHostRowChanges,
    keys: &[WasmRowKey],
    accepted: &MaterializedHotStateExactBatch,
) -> Result<WasmHostRowChanges, LixError> {
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
            WasmRowChange::Upsert {
                row,
                effect: WasmChangeEffect::FormatOnly,
            } => {
                let Some(Some(base)) = accepted.get(&row.key) else {
                    effective.push(change);
                    continue;
                };
                let WasmHostBytes::Typed(candidate) = &row.payload;
                let is_noop = base.decoded_snapshot().is_some_and(|base| {
                    base.schema_fingerprint == candidate.schema_fingerprint
                        && base.row_pk == candidate.row_pk
                        && base.row == candidate.row
                });
                is_noop
            }
            WasmRowChange::Create { .. }
            | WasmRowChange::Upsert { .. }
            | WasmRowChange::Delete(_) => false,
        };
        if !is_noop {
            effective.push(change);
        }
    }
    Ok(WasmHostRowChanges { changes: effective })
}

fn append_plugin_change_rows(
    rows: &mut RawWriteBatch,
    plugin: &PluginRegistryEntry,
    changes: WasmHostRowChanges,
    file_id: &str,
    context: &FilesystemRowContext,
) -> Result<(), LixError> {
    let allowed_schema_keys = plugin
        .schema_keys()
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    rows.reserve(changes.row_change_count());
    let mut interned_schema_keys = BTreeMap::<SharedStr, SharedStr>::new();
    let file_id = SharedStr::from(file_id);
    let branch_id = SharedStr::from(context.branch_id.as_str());
    // Format-only is a typed guest effect, so its exact engine metadata is
    // already known-valid. The plugin row payload itself remains typed all the
    // way into the staged write batch.
    let format_only_metadata = v2_format_only_metadata();
    for change in changes.changes {
        let (key, decoded_snapshot, effect) = match change {
            WasmRowChange::Create { .. } => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "keyless create was not materialized before transaction staging",
                ));
            }
            WasmRowChange::Delete(key) => (key, None, WasmChangeEffect::Content),
            WasmRowChange::Upsert { row, effect } => {
                let WasmHostBytes::Typed(typed) = row.payload;
                (row.key, Some(typed), effect)
            }
        };
        let row_pk = plugin_row_pk(plugin, &key)?;
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
        let metadata =
            (effect == WasmChangeEffect::FormatOnly).then(|| format_only_metadata.clone());
        if let Some(decoded_snapshot) = decoded_snapshot {
            rows.push_typed_parts(
                Some(row_pk),
                schema_key,
                Some(file_id.clone()),
                Some(decoded_snapshot),
                metadata,
                None,
                None,
                None,
                context.global,
                None,
                None,
                context.untracked,
                branch_id.clone(),
            );
        } else {
            rows.push_typed_parts(
                Some(row_pk),
                schema_key,
                Some(file_id.clone()),
                None,
                metadata,
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
    }
    Ok(())
}

fn plugin_row_pk(plugin: &PluginRegistryEntry, key: &WasmRowKey) -> Result<RowPk, LixError> {
    if plugin
        .create_schema_keys()
        .binary_search_by(|candidate| candidate.as_str().cmp(key.schema_key.as_str()))
        .is_ok()
    {
        let [lix_schema::Value::Uuid(_)] = key.row_pk.as_ref() else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "creatable schema '{}' requires one UUID primary-key component",
                    key.schema_key
                ),
            ));
        };
    }
    RowPk::from_schema_values(&key.row_pk).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "component plugin emitted invalid native row_pk for schema '{}' (components={:?}): {error}",
                key.schema_key,
                key.row_pk,
            ),
        )
    })
}

struct AuthorityRowKeySource(PluginRowAuthorities);

impl crate::plugin::runtime::WasmRowKeySource for AuthorityRowKeySource {
    fn into_keys(self: Box<Self>) -> Result<BTreeSet<WasmRowKey>, LixError> {
        Ok(self.0.materialize_keys())
    }
}

fn plugin_row_authorities_after_changes(
    base: &PluginRowAuthorities,
    changes: &WasmHostRowChanges,
) -> PluginRowAuthorities {
    let mut inserted = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for change in &changes.changes {
        match change {
            WasmRowChange::Upsert { row, .. } => {
                removed.remove(&row.key);
                inserted.insert(row.key.clone());
            }
            WasmRowChange::Delete(key) => {
                inserted.remove(key);
                removed.insert(key.clone());
            }
            WasmRowChange::Create { .. } => {}
        }
    }
    base.with_delta(inserted, removed)
}

fn plugin_row_authorities_from_transition(
    changes: &WasmHostRowChanges,
    creates: WasmCreateContext,
) -> PluginRowAuthorities {
    let namespace = creates
        .component_uuid_bytes(0)
        .expect("zero local ref always fits the create context");
    let mut generated = BTreeMap::<(SharedStr, [u8; 32]), Vec<u32>>::new();
    let mut inserted = BTreeSet::new();
    for change in &changes.changes {
        let WasmRowChange::Upsert { row, .. } = change else {
            continue;
        };
        let [lix_schema::Value::Uuid(id)] = row.key.row_pk.as_ref() else {
            inserted.insert(row.key.clone());
            continue;
        };
        let bytes = id.as_bytes();
        if bytes[..12] != namespace[..12] {
            inserted.insert(row.key.clone());
            continue;
        }
        generated
            .entry((row.key.schema_key.clone(), row.key.schema_fingerprint))
            .or_default()
            .push(u32::from_be_bytes(
                bytes[12..]
                    .try_into()
                    .expect("generated UUID local reference is four bytes"),
            ));
    }
    let mut ranges = Vec::new();
    for ((schema_key, fingerprint), mut local_refs) in generated {
        local_refs.sort_unstable();
        local_refs.dedup();
        let mut start = local_refs[0];
        let mut end = start;
        for local_ref in local_refs.into_iter().skip(1) {
            if end.checked_add(1) == Some(local_ref) {
                end = local_ref;
                continue;
            }
            ranges.push(PluginRowAuthorityRange::new(
                schema_key.to_string(),
                fingerprint,
                creates,
                start,
                end,
            ));
            start = local_ref;
            end = local_ref;
        }
        ranges.push(PluginRowAuthorityRange::new(
            schema_key.to_string(),
            fingerprint,
            creates,
            start,
            end,
        ));
    }
    PluginRowAuthorities::from_keys(inserted).with_ranges(ranges)
}

fn plugin_row_authorities_after_transition(
    prior: &PluginRowAuthorities,
    changes: &WasmHostRowChanges,
    replace_all_rows: bool,
) -> PluginRowAuthorities {
    let empty = PluginRowAuthorities::empty();
    let base = if replace_all_rows { &empty } else { prior };
    plugin_row_authorities_after_changes(base, changes)
}

fn plugin_row_authorities_from_live_batch(
    rows: &MaterializedHotStateBatch,
    ordinals: &[u32],
) -> Result<PluginRowAuthorities, LixError> {
    Ok(PluginRowAuthorities::from_keys(
        ordinals
            .iter()
            .map(|ordinal| -> Result<WasmRowKey, LixError> {
                let row = rows.row(*ordinal as usize);
                if let Some(typed) = row.materialize_decoded_snapshot()? {
                    return WasmRowKey::from_typed_parts(
                        row.schema_key().to_owned(),
                        typed.schema_fingerprint,
                        typed.row_pk.clone(),
                    );
                }
                Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin state row '{}' has no native typed payload",
                        row.schema_key()
                    ),
                ))
            })
            .collect::<Result<BTreeSet<_>, _>>()?,
    ))
}

fn v2_host_rows_from_live_batch_ordinals(
    rows: &MaterializedHotStateBatch,
    ordinals: &[u32],
) -> Result<Vec<WasmHostRow>, LixError> {
    let mut host_rows = Vec::with_capacity(ordinals.len());
    for ordinal in ordinals {
        let row = rows.get(*ordinal as usize).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin state selection references a row outside its batch owner",
            )
        })?;
        if let Some(typed) = row.materialize_decoded_snapshot()? {
            let key = WasmRowKey::from_typed_parts(
                row.schema_key().to_owned(),
                typed.schema_fingerprint,
                typed.row_pk.clone(),
            )?;
            host_rows.push(WasmRow {
                key,
                payload: WasmHostBytes::Typed(typed),
            });
        } else {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "plugin state row '{}' has no native typed payload",
                    row.schema_key()
                ),
            ));
        }
    }
    host_rows.sort_by(|left, right| left.key.cmp(&right.key));
    for pair in host_rows.windows(2) {
        if pair[0].key == pair[1].key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "durable component row hydration returned duplicate keys",
            ));
        }
    }
    Ok(host_rows)
}

fn v2_host_row_ordinals_from_live_batch(
    rows: &MaterializedHotStateBatch,
    file_key: &PluginFileWriteKey,
    schema_keys: &[String],
    catalog: &CatalogSnapshot,
) -> Result<Vec<u32>, LixError> {
    let mut ordinals = Vec::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let owned = row.branch_id() == file_key.branch_id
            && row.file_id() == Some(file_key.file_id.as_str())
            && !row.global()
            // A file's rows live in the file's own lane. Pinning this to
            // tracked would hydrate an untracked file as if it were empty.
            && row.untracked() == file_key.untracked
            && schema_keys
                .binary_search_by(|schema_key| schema_key.as_str().cmp(row.schema_key()))
                .is_ok();
        if !owned {
            continue;
        }
        if row.decoded_snapshot().is_none() && row.raw_snapshot().is_none() {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "plugin-owned durable row '{}.{}' must carry a native typed payload",
                    row.schema_key(),
                    row.row_pk().as_json_array_text()?
                ),
            ));
        }
        let typed = row
            .materialize_decoded_snapshot()?
            .expect("exclusive native payload was checked above");
        let plan = catalog
            .plan_for_key(row.schema_key())
            .map(|(_, plan)| plan)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin-owned durable row '{}' has no current schema plan",
                        row.schema_key()
                    ),
                )
            })?;
        typed.validate_resolved_schema_binding(
            row.schema_key(),
            &plan.key.schema_key,
            &plan.fingerprint().bytes(),
        )?;
        plan.compiled_schema
            .validate_complete_row(&typed.row)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "durable typed row validation failed for schema '{}': {error}",
                        row.schema_key()
                    ),
                )
            })?;
        let primary_key_columns = plan.compiled_schema.primary_key();
        if typed.row_pk.len() != primary_key_columns.len()
            || primary_key_columns
                .iter()
                .zip(typed.row_pk.iter())
                .any(|(column, value)| typed.row.get(column) != Some(value))
            || !row.row_pk().matches_schema_values(&typed.row_pk)
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "durable typed row identity does not match schema '{}'",
                    row.schema_key()
                ),
            ));
        }
        ordinals.push(u32::try_from(ordinal).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "durable plugin state exceeds u32 row ordinals",
            )
        })?);
    }
    ordinals.sort_unstable_by(|left, right| {
        let left = rows.row(*left as usize);
        let right = rows.row(*right as usize);
        left.schema_key()
            .cmp(right.schema_key())
            .then_with(|| left.row_pk().cmp(right.row_pk()))
    });
    for pair in ordinals.windows(2) {
        let left = rows.row(pair[0] as usize);
        let right = rows.row(pair[1] as usize);
        if left.schema_key() == right.schema_key() && left.row_pk() == right.row_pk() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "durable component row hydration returned duplicate keys",
            ));
        }
    }
    Ok(ordinals)
}

fn v2_host_changes_from_prepared_rows(
    rows: &PreparedStateBatch,
    untracked: bool,
    catalog: Arc<CatalogSnapshot>,
) -> Result<WasmHostRowChanges, LixError> {
    let mut changes = rows
        .iter()
        .map(|row| {
            // Rendering is lane-agnostic; the rows only have to agree with the
            // lane of the file being rendered.
            if row.global || row.untracked != untracked || row.file_id.is_none() {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "component semantic rendering requires branch-local, file-scoped rows in the file's own lane",
                ));
            }
            let plan = catalog
                .plan_for_key(&row.schema_key)
                .map(|(_, plan)| plan)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "typed plugin row '{}' has no current schema plan",
                            row.schema_key
                        ),
                    )
                })?;
            if let Some(typed) = row.materialize_decoded_snapshot()? {
                if typed.schema_fingerprint != plan.fingerprint().bytes() {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "typed plugin row '{}' fingerprint does not match the current schema",
                            row.schema_key
                        ),
                    ));
                }
                let (key, expected_pk) = typed_key_from_row_pk(plan, &row.schema_key, row.row_pk)?;
                if expected_pk.as_slice() != typed.row_pk.as_ref() {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "typed plugin row '{}' primary key disagrees with the staged row identity",
                            row.schema_key
                        ),
                    ));
                }
                let format_only = row
                    .metadata
                    .map(|metadata| metadata.to_json_string())
                    .transpose()
                    .map_err(|error| LixError::unknown(error.to_string()))?
                    .is_some_and(|metadata| metadata == V2_FORMAT_ONLY_METADATA_JSON);
                let effect = if format_only {
                    WasmChangeEffect::FormatOnly
                } else {
                    WasmChangeEffect::Content
                };
                return Ok(WasmRowChange::Upsert {
                    row: WasmRow {
                        key,
                        payload: WasmHostBytes::Typed(typed),
                    },
                    effect,
                });
            } else if row.snapshot.is_some() {
                Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin semantic row '{}' has no native typed payload",
                        row.schema_key
                    ),
                ))
            } else {
                let key = typed_key_from_row_pk(plan, &row.schema_key, &row.row_pk)?.0;
                Ok(WasmRowChange::Delete(key))
            }
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    changes.sort_by(|left, right| left.row_key().cmp(&right.row_key()));
    for pair in changes.windows(2) {
        if pair[0].row_key() == pair[1].row_key() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "one component semantic write batch cannot contain the same row key more than once",
            ));
        }
    }
    Ok(WasmHostRowChanges { changes })
}

fn typed_key_from_row_pk(
    plan: &crate::catalog::SchemaPlan,
    schema_key: &str,
    row_pk: &RowPk,
) -> Result<(WasmRowKey, Vec<lix_schema::Value>), LixError> {
    let types = plan.primary_key_component_types.as_deref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("schema '{schema_key}' has no typed primary-key definition"),
        )
    })?;
    let parts = row_pk.clone().into_parts();
    if parts.len() != types.len() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("schema '{schema_key}' row key has the wrong component count"),
        ));
    }
    let values = parts
        .into_iter()
        .zip(types.iter().copied())
        .map(|(part, data_type)| match data_type {
            crate::row_pk::RowPkComponentType::Uuid => uuid::Uuid::parse_str(&part)
                .map(lix_schema::Value::Uuid)
                .map_err(|error| {
                    LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string())
                }),
            crate::row_pk::RowPkComponentType::Integer => part
                .parse::<i64>()
                .map(lix_schema::Value::Int8)
                .map_err(|error| {
                    LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string())
                }),
            crate::row_pk::RowPkComponentType::String => Ok(lix_schema::Value::Text(part)),
            crate::row_pk::RowPkComponentType::Bytes => Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("schema '{schema_key}' uses an unsupported binary primary key"),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key = WasmRowKey::from_typed_parts(schema_key, plan.fingerprint().bytes(), values.clone())?;
    Ok((key, values))
}

fn reject_external_plugin_registry_rows(rows: &RawWriteBatch) -> Result<(), LixError> {
    for row in rows {
        if row.schema_key != KEY_VALUE_SCHEMA_KEY {
            continue;
        }
        let row_key = row.row_pk.and_then(|row_pk| row_pk.as_single_string().ok());
        let snapshot_key = row
            .snapshot_json()
            .and_then(|snapshot| snapshot.get("key"))
            .and_then(JsonValue::as_str);
        let reserved = [row_key, snapshot_key].into_iter().flatten().find(|key| {
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

    fn matches_materialization_row(&self, row: RawWriteRowRef<'_>) -> bool {
        self.matches_blob_ref_row(row)
    }
}

impl From<&TransactionFileContent> for PluginFileWriteKey {
    fn from(write: &TransactionFileContent) -> Self {
        Self {
            branch_id: write.branch_id.clone(),
            global: write.global,
            untracked: write.untracked,
            file_id: write.file_id.clone(),
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
    RowsWithFileContent {
        mode: TransactionWriteMode,
        rows: ReconciledRowBatch,
        file_content: Vec<TransactionFileContent>,
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
    existing_create_reservation: Option<MaterializedHotStateRow>,
    factory: Arc<dyn WasmComponentFactory>,
    descriptor: WasmFileDescriptor,
    schemas: SchemaAllowlist,
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
    existing_create_reservation: Option<MaterializedHotStateRow>,
    store_permit: PluginActorStorePermit,
    task: Option<
        tokio::task::JoinHandle<
            Result<(Box<dyn WasmComponentActor>, ValidatedFileTransition), LixError>,
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
        checkpoint: Option<WasmDocumentCheckpoint>,
        bytes: crate::Blob,
        semantic_root: Arc<str>,
        row_authorities: PluginRowAuthorities,
        view: PendingPluginActorView,
    },
    /// The plugin transition has already produced its durable rows, but the
    /// bounded working set does not keep its private Wasm Store alive.
    /// Keeping this marker preserves the normal one-transition-per-file
    /// validation and gives the session a non-authoritative file view after
    /// commit, forcing a cold open before any later edit.
    Uncached {
        key: PluginActorKey,
        view: PendingPluginActorView,
        checkpoint: Option<PluginActorStagedCheckpoint>,
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
                let checkpoint =
                    lease
                        .successor_checkpoint()
                        .and_then(|(cache, semantic_root, checkpoint)| {
                            cache.stage_checkpoint(successor_key.clone(), semantic_root, checkpoint)
                        });
                let _ = lease.discard_successor().await;
                Self::Uncached {
                    key: successor_key,
                    view,
                    checkpoint,
                }
            }
            Self::New {
                cache,
                mut store,
                key,
                document,
                checkpoint,
                semantic_root,
                row_authorities: _,
                view,
                ..
            } => {
                let staged_checkpoint =
                    cache.stage_checkpoint(key.clone(), Arc::clone(&semantic_root), checkpoint);
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
                Self::Uncached {
                    key,
                    view,
                    checkpoint: staged_checkpoint,
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
                checkpoint,
                bytes,
                semantic_root,
                row_authorities,
                view,
            } => {
                let path = key.path.clone();
                cache.remember_checkpoint(&key, &semantic_root, checkpoint);
                (
                    Some(cache.install_with_authorities(
                        key,
                        store,
                        document,
                        bytes,
                        semantic_root,
                        row_authorities,
                    )),
                    view,
                    path,
                )
            }
            Self::Uncached {
                key,
                view,
                checkpoint,
                ..
            } => {
                if let Some(checkpoint) = checkpoint {
                    checkpoint.publish();
                }
                (None, view, key.path)
            }
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

/// Builds the blob-backed file write for an accepted semantic renderer
/// transition.
fn semantic_rendered_file_content(
    file_id: String,
    path: String,
    filename: String,
    branch_id: String,
    untracked: bool,
    base_blob_hash: BlobId,
    rendered_bytes: crate::Blob,
    same_length_output_splice: Option<ValidatedSameLengthOutputSplice>,
) -> TransactionFileContent {
    // The re-rendered payload replaces the file in its own lane. Keying it as
    // tracked would leave an untracked file's materialization unmatched.
    let mut rendered_file = TransactionFileContent::new(
        file_id,
        Some(path),
        Some(filename),
        branch_id,
        false,
        untracked,
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

async fn render_semantic_changes_with_lease(
    mut lease: PluginActorLease,
    successor_key: PluginActorKey,
    view: PendingPluginActorView,
    descriptor: WasmFileDescriptor,
    changes: WasmHostRowChanges,
    visible_root: &str,
    materialization_version: &str,
    limits: WasmTransitionLimits,
) -> Result<
    (
        PendingPluginActorPublication,
        crate::Blob,
        Option<ValidatedSameLengthOutputSplice>,
        WasmTransitionCounters,
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
    let change_count = u64::try_from(changes.row_change_count()).unwrap_or(u64::MAX);
    let call = match lease.begin_pending_guest_call() {
        Ok(call) => call,
        Err(error) => return Err((error, publication(lease))),
    };
    if call.semantic_root() != visible_root {
        let error = LixError::new(
            LixError::CODE_PLUGIN_OBSERVATION_STALE,
            "component semantic write base no longer matches visible semantic state",
        );
        let error = lease.handle_pending_guest_call_error(call, error);
        return Err((error, publication(lease)));
    }
    let successor_row_authorities =
        plugin_row_authorities_after_changes(call.row_authorities(), &changes);
    let change_source = match VecRowChangeSource::new(changes, limits) {
        Ok(source) => source,
        Err(error) => {
            let error = lease.handle_pending_guest_call_error(call, error);
            return Err((error, publication(lease)));
        }
    };
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
        .rows_changed(
            renderer_input,
            limits,
            WasmRowUpdate {
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
    let rendered = match drain_row_transition_edits(
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
    let checkpoint = match lease
        .actor_mut()
        .checkpoint_document(rendered.document)
        .await
    {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            let error = lease.handle_pending_guest_call_error(call, error);
            return Err((error, publication(lease)));
        }
    };
    if let Err(error) = lease
        .complete_pending_guest_call(
            call,
            rendered.document,
            checkpoint,
            rendered.bytes,
            rendered.bytes_sha256,
            materialization_version.to_string(),
        )
        .await
    {
        return Err((error, publication(lease)));
    }
    if let Err(error) = lease.set_successor_row_authorities(successor_row_authorities) {
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

async fn preflight_owned_generation_upgrades(
    host: &PluginRuntimeHost,
    base: &dyn HotStateReader,
    staged: &impl StagedHotStateRows,
    base_blob_reader: &dyn BlobDataReader,
    staged_writes: &TransactionWriteBuffer,
    upgrades: &[PluginGenerationUpgrade],
    install_wasm: &BTreeMap<BlobId, Vec<u8>>,
    install_schema_definitions: &BTreeMap<PluginLifecycleKey, BTreeMap<String, JsonValue>>,
) -> Result<(), LixError> {
    // KNOWN LANE GAP, deliberate and scoped out of the unskip.
    //
    // Every scan below is pinned to the tracked lane, so a plugin generation
    // upgrade re-renders and validates only the tracked files that plugin owns.
    // An untracked owned file is not validated against the replacement plugin.
    //
    // This is a validation gap, not a loss: the preflight writes no rows. An
    // untracked owned file keeps its old-generation owner row, and the next
    // write to it takes the ordinary `plugin_owner_needs_write` path, which
    // rewrites the owner and re-reconciles under the new generation. The only
    // consequence is that a replacement plugin which cannot render that file
    // fails at the next write rather than at upgrade time.
    //
    // Closing it means lane-partitioning four scans and two path filters here
    // with their own coverage, which belongs with the enforcement work.
    let branch_ids = upgrades
        .iter()
        .map(|upgrade| upgrade.branch_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let owner_rows = overlay_scan_batch(
        base,
        staged,
        &HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                row_pks: vec![RowPk::single(PLUGIN_OWNER_KEY)],
                branch_ids: branch_ids.clone(),
                untracked: Some(false),
                ..Default::default()
            },
            projection: plugin_registry_hot_state_projection(),
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
        // This preflight is tracked-pinned; see the lane-gap note above.
        let Some(owner) = PluginFileOwner::from_hot_state_row(&owner_row, &branch_id, false)?
        else {
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
        &FilesystemPathIndexRequest::new(branch_ids).hot_state_request(),
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
        .map(RowPk::single)
        .collect::<Vec<_>>();
    let registered_schema_rows = overlay_scan_batch(
        base,
        staged,
        &HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                row_pks: owned_schema_keys,
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
            projection: plugin_registry_hot_state_projection(),
            ..Default::default()
        },
    )
    .await?;
    let mut registered_schema_definitions = BTreeMap::<(String, String), JsonValue>::new();
    for row in registered_schema_rows.iter() {
        let schema_key = row.row_pk().as_single_string().map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema has an invalid identity: {error}"),
            )
        })?;
        if row.deleted() {
            continue;
        }
        let snapshot = row.snapshot_json_value()?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "live active-plugin schema row must have exactly one payload",
            )
        })?;
        let (snapshot_key, definition) = crate::schema::schema_from_registered_snapshot(&snapshot)?;
        if snapshot_key.schema_key != schema_key {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("active plugin schema '{schema_key}' has mismatched snapshot identity"),
            ));
        }
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
            .validate_owned_upgrade_contract(&upgrade.replacement)?;
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
            &HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: upgrade.previous.schema_keys().to_vec(),
                    branch_ids: vec![upgrade.branch_id.clone()],
                    file_ids: file_id_filters.clone(),
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_state_hot_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut state_ordinals = Vec::<u32>::with_capacity(state_rows.len());
        for (ordinal, row) in state_rows.iter().enumerate() {
            let Some(file_id) = row.file_id() else {
                continue;
            };
            let owned = row.branch_id() == upgrade.branch_id
                && !row.global()
                && !row.untracked()
                && upgrade
                    .previous
                    .schema_keys()
                    .binary_search_by(|schema_key| schema_key.as_str().cmp(row.schema_key()))
                    .is_ok()
                && file_ids
                    .binary_search_by(|candidate| candidate.as_str().cmp(file_id))
                    .is_ok();
            if !owned {
                continue;
            }
            if row.decoded_snapshot().is_none() && row.raw_snapshot().is_none() {
                return Err(plugin_upgrade_error(
                    upgrade,
                    file_id,
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "plugin-owned durable row '{}' must carry a native typed payload",
                            row.schema_key()
                        ),
                    ),
                ));
            }
            state_ordinals.push(u32::try_from(ordinal).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin upgrade state batch exceeds u32 rows",
                )
            })?);
        }
        state_ordinals.sort_unstable_by(|left, right| {
            let left = state_rows.row(*left as usize);
            let right = state_rows.row(*right as usize);
            left.file_id()
                .cmp(&right.file_id())
                .then_with(|| left.schema_key().cmp(right.schema_key()))
                .then_with(|| left.row_pk().cmp(right.row_pk()))
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
            &HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
                    row_pks: file_ids
                        .iter()
                        .map(|file_id| validated_uuid_row_pk(file_id))
                        .collect::<Result<Vec<_>, _>>()?,
                    branch_ids: vec![upgrade.branch_id.clone()],
                    file_ids: file_id_filters,
                    untracked: Some(false),
                    ..Default::default()
                },
                projection: plugin_registry_hot_state_projection(),
                ..Default::default()
            },
        )
        .await?;
        let mut materialized_hash_by_file = BTreeMap::<String, BlobId>::new();
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
            let hash = BlobId::from_hex(&snapshot.blob_hash)
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
                                "owned component file is missing its materialized blob reference",
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

        let wasm_hash =
            BlobId::from_hex(upgrade.replacement.wasm_blob_hash().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "replacement file projection has no executable component",
                )
            })?)?;
        let wasm = install_wasm.get(&wasm_hash).cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "replacement plugin '{}' WASM payload is unavailable during upgrade preflight",
                    upgrade.replacement.key()
                ),
            )
        })?;
        let installed = upgrade.replacement.to_installed_plugin(Some(wasm))?;
        let factory = host.load_or_compile_factory(&installed).await?;
        let limits = WasmTransitionLimits::default();

        for (owner, expected) in owners.iter().zip(materialized_bytes) {
            let expected: crate::Blob = expected
                .ok_or_else(|| {
                    plugin_upgrade_error(
                        upgrade,
                        owner.file_id(),
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "owned component file references a missing materialized blob",
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
                            "owned component file must resolve to exactly one tracked descriptor, found {}",
                            matches.len()
                        ),
                    ),
                ));
            };
            let range = state_by_file.get(owner.file_id()).cloned().unwrap_or(0..0);
            let rows = v2_host_rows_from_live_batch_ordinals(&state_rows, &state_ordinals[range])?;
            let store_permit = host
                .actor_cache()
                .admit_store()
                .map_err(|error| plugin_upgrade_error(upgrade, owner.file_id(), error))?;
            let actor = factory
                .instantiate_actor()
                .await
                .map_err(|error| plugin_upgrade_error(upgrade, owner.file_id(), error))?;
            let mut store = PluginActorStore::new(actor, store_permit);
            let verified = preflight_rendered_file(
                store.actor_mut(),
                WasmFileDescriptor {
                    file_id: owner.file_id().to_owned(),
                    path: Some(entry.path.clone()),
                    plugin: WasmPluginSelection {
                        plugin_key: upgrade.replacement.key().to_string(),
                        generation: upgrade.replacement.archive_blob_hash().to_string(),
                    },
                },
                rows,
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

async fn preflight_rendered_file(
    actor: &mut dyn WasmComponentActor,
    descriptor: WasmFileDescriptor,
    rows: Vec<WasmHostRow>,
    expected: crate::Blob,
    limits: WasmTransitionLimits,
) -> Result<(), LixError> {
    let source = VecRowSource::new(rows, limits)?;
    let accepted = Arc::new(ArcByteSource::new(expected.clone()));
    let transition = actor
        .open_rows(
            limits,
            WasmOpenRowsInput {
                descriptor,
                rows: Box::new(source),
                accepted: Some(accepted),
            },
        )
        .await?;
    let validated = drain_row_transition_edits(
        actor,
        transition,
        expected.as_ref(),
        Some(expected.clone()),
        None,
        limits,
    )
    .await?;
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
    untracked: bool,
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
struct PluginSemanticWriteGroup {
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

fn retain_large_import_actor(plugin: &PluginRegistryEntry) -> bool {
    // component retains its guest document as before. Arena v3 retains only the
    // host-owned immutable root after import, so its large-file actor is also
    // the intended warm-successor cache entry.
    debug_assert!(!plugin.api_version().is_empty());
    true
}

fn cold_successor_transition_limits(file_bytes: usize) -> WasmTransitionLimits {
    WasmTransitionLimits::for_cold_file_bytes(u64::try_from(file_bytes).unwrap_or(u64::MAX))
}

fn duplicate_plugin_lifecycle_mutation() -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        "a write batch may mutate each plugin archive at most once",
    )
}

fn plugin_schema_collision_error(
    plugin_key: &str,
    row_pk: &RowPk,
    other_plugin: Option<&String>,
) -> LixError {
    let schema_key = row_pk
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

fn plugin_registry_hot_state_projection() -> HotStateProjection {
    HotStateProjection {
        columns: vec!["snapshot_content".to_string()],
    }
}

fn plugin_state_tombstone_batch(
    active_state: &[PluginStateBatchRow],
    state_batches: &[MaterializedHotStateBatch],
    file_id: &str,
    context: &FilesystemRowContext,
) -> RawWriteBatch {
    let mut rows = RawWriteBatch::with_capacity(active_state.len());
    for selected in active_state {
        let row = state_batches[selected.batch_index as usize].row(selected.row_index as usize);
        rows.push_parts(
            Some(row.row_pk().clone()),
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
        TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } => {
            rows.iter()
                .any(|row| is_non_active_local(&row.branch_id, row.global))
                || file_content
                    .iter()
                    .any(|write| is_non_active_local(&write.branch_id, write.global))
        }
    }
}

fn transaction_write_has_plugin_lifecycle_candidate(write: &TransactionWrite) -> bool {
    let (rows, file_content): (&RawWriteBatch, &[TransactionFileContent]) = match write {
        TransactionWrite::Rows { rows, .. } => (rows, &[]),
        TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } => (rows, file_content),
    };
    file_content
        .iter()
        .any(|write| write.path.as_deref().is_some_and(is_plugin_storage_path))
        || rows.iter().any(|row| {
            row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY
                && row.snapshot.is_none()
                && row.decoded_snapshot().is_none()
                && row
                    .row_pk
                    .and_then(|row_pk| row_pk.as_single_string_owned().ok())
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
        TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } => rows
            .iter()
            .map(|row| row.branch_id.to_string())
            .chain(file_content.iter().map(|write| write.branch_id.clone()))
            .collect(),
    }
}

fn transaction_write_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.len(),
        TransactionWrite::RowsWithFileContent { rows, .. } => rows.len(),
    }
}

#[cfg(feature = "storage-benches")]
fn transaction_write_untracked_row_count(write: &TransactionWrite) -> usize {
    match write {
        TransactionWrite::Rows { rows, .. } => rows.iter().filter(|row| row.untracked).count(),
        TransactionWrite::RowsWithFileContent { rows, .. } => {
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
        TransactionWrite::RowsWithFileContent { rows, .. } => {
            require_valid_transaction_write_row_storage_scopes(rows)
        }
    }
}

fn require_valid_reconciled_transaction_write_storage_scopes(
    write: &ReconciledTransactionWrite,
) -> Result<(), LixError> {
    match write {
        ReconciledTransactionWrite::Rows { rows, .. }
        | ReconciledTransactionWrite::RowsWithFileContent { rows, .. } => {
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

fn validated_uuid_row_pk(value: &str) -> Result<RowPk, LixError> {
    RowPk::uuid_from_canonical(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("validated identity is not a canonical UUID: {error}"),
        )
    })
}

fn catalog_revision_is_current(
    observed: Option<&CatalogRevision>,
    committed: &CatalogRevision,
) -> bool {
    observed == Some(committed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    use serde_json::json;

    use super::*;
    use crate::GLOBAL_BRANCH_ID;
    use crate::NullableKeyFilter;
    use crate::branch::BranchContext;
    use crate::engine::Engine;
    use crate::functions::{DeterministicFunctionProvider, FunctionProvider};
    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, Storage, StorageError, StorageReadOptions,
        StorageSessionToken, StorageWriteOptions,
    };
    use crate::tracked_state::{
        TrackedStateDiffIdentity, TrackedStateKey, TrackedStateScanRequest,
    };
    use crate::transaction::staged_commit_changes::{
        StagedCommitChangeBatchBuilder, StagedCommitChangeRefs,
    };
    use crate::transaction_types::TransactionJson;

    #[test]
    fn post_commit_catalog_warm_never_aliases_a_newer_revision() {
        let published = CatalogRevision::for_test(b"published");
        let concurrent = CatalogRevision::for_test(b"concurrent");

        assert!(catalog_revision_is_current(Some(&published), &published));
        assert!(!catalog_revision_is_current(Some(&concurrent), &published));
        assert!(!catalog_revision_is_current(None, &published));
    }

    fn raw_write_rows(rows: Vec<TransactionWriteRow>) -> RawWriteBatch {
        RawWriteBatch::from_test_rows(rows)
    }

    fn hot_state_context() -> HotStateContext {
        HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
    }

    #[derive(Clone)]
    struct OneShotExpiringStorage {
        inner: Memory,
        expire_next_read: Arc<AtomicBool>,
    }

    impl Storage for OneShotExpiringStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(
            &self,
            options: StorageReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            if self.expire_next_read.swap(false, Ordering::SeqCst) {
                return Err(StorageError::ReadExpired);
            }
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    #[tokio::test]
    async fn transaction_reader_open_expiry_is_returned_instead_of_panicking() {
        let inner = Memory::new();
        seed_visible_schema_rows(StorageAdapter::new(inner.clone())).await;
        let expire_next_read = Arc::new(AtomicBool::new(false));
        let storage = StorageAdapter::new(OneShotExpiringStorage {
            inner,
            expire_next_read: Arc::clone(&expire_next_read),
        });
        let opened = open_transaction(
            &SessionBranch::new(GLOBAL_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            storage,
            Arc::new(hot_state_context()),
            Arc::new(TrackedStateContext::new()),
            Arc::new(BinaryCasContext::new()),
            PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
            Arc::new(BranchContext::new()),
            Arc::new(CatalogContext::new()),
            Arc::new(SqlPlanningCache::default()),
            SessionFileViews::default(),
        )
        .await
        .expect("transaction should open");
        let mut transaction = opened.transaction;

        expire_next_read.store(true, Ordering::SeqCst);
        let error = match transaction.tracked_state_reader().await {
            Ok(_) => panic!("tracked-state reader should return the expired read"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);

        expire_next_read.store(true, Ordering::SeqCst);
        let error = match transaction.branch_ref_reader().await {
            Ok(_) => panic!("branch-ref reader should return the expired read"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);

        expire_next_read.store(true, Ordering::SeqCst);
        let error = match transaction.commit_graph_reader().await {
            Ok(_) => panic!("commit-graph reader should return the expired read"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
    }

    #[test]
    fn semantic_conflict_limits_scale_host_owned_records_but_not_bytes_or_deadline() {
        let defaults = WasmTransitionLimits::default();
        let limits = conflict_resolution_limits(5_000).expect("conflict count should fit");

        assert_eq!(limits.max_attachment_refs, 15_000);
        assert_eq!(limits.max_pages, 5_001);
        assert_eq!(limits.max_record_bytes, defaults.max_record_bytes);
        assert_eq!(limits.max_page_bytes, defaults.max_page_bytes);
        assert_eq!(limits.max_total_bytes, defaults.max_total_bytes);
        assert_eq!(
            limits.total_deadline_nanoseconds,
            defaults.total_deadline_nanoseconds
        );
    }

    #[test]
    fn cold_successor_deadline_scales_with_submitted_file_size() {
        let file_bytes = 10 * 1024 * 1024;
        let limits = cold_successor_transition_limits(file_bytes);

        assert_eq!(
            limits,
            WasmTransitionLimits::for_cold_file_bytes(file_bytes as u64)
        );
        assert!(
            limits.total_deadline_nanoseconds
                > WasmTransitionLimits::default().total_deadline_nanoseconds
        );
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
    fn plugin_json_ingress_choke_point_records_the_actual_rejected_payload() {
        let mut rows = raw_write_rows(vec![key_value_stage_row("forbidden", "json", false)]);
        rows.mark_plugin_owned(0);
        let host = PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime));

        let error = reject_plugin_owned_json_row(&rows, 0, &host)
            .expect_err("plugin-owned JSON ingress must be rejected");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        let counters = host.transition_counters();
        assert_eq!(counters.outer_row_json_dom_fallback_calls, 1);
        assert!(counters.outer_row_json_dom_fallback_bytes > 0);
        assert_eq!(counters.outer_row_json_parse_calls, 0);
        assert_eq!(counters.outer_row_json_serialize_calls, 0);
        assert_eq!(counters.outer_row_json_canonicalize_calls, 0);
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
                row_pk: RowPk::single("file-a"),
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
            file_content_writes: Vec::new(),
        };

        assert!(prepared_writes_require_filesystem_index_rebuild(
            &prepared_writes
        ));
    }

    /// An addressable filesystem row carries a provisional change id into
    /// materialization, which is why the delta is projected out of the commit
    /// rather than out of the caller's `PreparedWriteSet`. Addressability alone
    /// says nothing about whether the staged rows describe the whole change,
    /// so it must not force a rebuild — that is what made every file *create*
    /// discard the cached path index.
    #[test]
    fn addressable_filesystem_row_does_not_require_index_rebuild() {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        let mut state_rows = PreparedStateBatch::with_capacity(1);
        state_rows.push_parts_with_change_addressability(
            SchemaPlanId::for_test(0),
            PreparedRowFacts::default(),
            RowPk::single("file-a"),
            BLOB_REF_SCHEMA_KEY.into(),
            Some("file-a".into()),
            None,
            None,
            None,
            None,
            timestamp,
            timestamp,
            false,
            Some(ChangeId::for_test_label("provisional")),
            true,
            Some(CommitId::for_test_label("commit")),
            false,
            "main".into(),
        );
        let prepared_writes = PreparedWriteSet {
            state_rows,
            insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
        };

        assert!(prepared_writes_stage_filesystem_rows(&prepared_writes));
        assert!(!prepared_writes_require_filesystem_index_rebuild(
            &prepared_writes
        ));
    }

    /// A branch ref move republishes the branch's whole visible filesystem, so
    /// the staged rows cannot describe the difference.
    #[test]
    fn branch_ref_write_requires_filesystem_index_rebuild() {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        let mut state_rows = PreparedStateBatch::with_capacity(1);
        state_rows.push_parts_with_change_addressability(
            SchemaPlanId::for_test(0),
            PreparedRowFacts::default(),
            RowPk::single("main"),
            BRANCH_REF_SCHEMA_KEY.into(),
            None,
            None,
            None,
            None,
            None,
            timestamp,
            timestamp,
            true,
            Some(ChangeId::for_test_label("branch-ref")),
            false,
            Some(CommitId::for_test_label("commit")),
            false,
            "main".into(),
        );
        let prepared_writes = PreparedWriteSet {
            state_rows,
            insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
        };

        assert!(prepared_writes_require_filesystem_index_rebuild(
            &prepared_writes
        ));
    }

    /// A global or untracked filesystem row changes visibility in every branch,
    /// which a single-branch delta cannot describe. It is also the only staged
    /// filesystem row that can be dropped before the delta is captured, by
    /// `retain_untracked_rows_not_superseded_by_engine`, so the rebuild has to
    /// be decided here rather than left to `advance_committed`'s own eviction —
    /// that only sees the rows it is handed.
    #[test]
    fn global_or_untracked_filesystem_row_requires_index_rebuild() {
        for (global, untracked) in [(true, false), (false, true), (true, true)] {
            let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
            let mut state_rows = PreparedStateBatch::with_capacity(1);
            state_rows.push_parts_with_change_addressability(
                SchemaPlanId::for_test(0),
                PreparedRowFacts::default(),
                RowPk::single("file-a"),
                "lix_file_descriptor".into(),
                Some("file-a".into()),
                None,
                None,
                None,
                None,
                timestamp,
                timestamp,
                global,
                Some(ChangeId::for_test_label("provisional")),
                true,
                Some(CommitId::for_test_label("commit")),
                untracked,
                "main".into(),
            );
            let prepared_writes = PreparedWriteSet {
                state_rows,
                insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            };

            assert!(
                prepared_writes_require_filesystem_index_rebuild(&prepared_writes),
                "global={global} untracked={untracked} must force a rebuild"
            );
        }
    }

    #[test]
    fn visible_materialization_requires_a_matching_blob_ref_identity() {
        let blob_hash = BlobId::from_content(b"base");
        let row = |snapshot_id: &str| MaterializedHotStateRow {
            row_pk: RowPk::single("01920000-0000-7000-8000-0000000000a2"),
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

        let visible = decode_visible_materialization(
            &row("01920000-0000-7000-8000-0000000000a2"),
            "01920000-0000-7000-8000-0000000000a2",
        )
        .expect("matching materialization should decode");
        assert!(matches!(
            visible.bytes,
            VisibleMaterializationBytes::Blob { hash, .. } if hash == blob_hash
        ));
        let error = decode_visible_materialization(
            &row("other-file"),
            "01920000-0000-7000-8000-0000000000a2",
        )
        .expect_err("mismatched blob-ref identity must not authorize a cached actor base");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    }

    #[test]
    fn semantic_renderer_splice_provenance_is_bound_to_its_visible_blob() {
        let base_blob_hash = BlobId::from_content(b"abcdef");
        let rendered = semantic_rendered_file_content(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            "/document.md".to_string(),
            "document.md".to_string(),
            "main".to_string(),
            false,
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

        let malformed = semantic_rendered_file_content(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            "/document.md".to_string(),
            "document.md".to_string(),
            "main".to_string(),
            false,
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
        let global_file = TransactionFileContent::new(
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
                &TransactionWrite::RowsWithFileContent {
                    mode: TransactionWriteMode::Replace,
                    rows: raw_write_rows(vec![active_row.clone(), global_row]),
                    file_content: vec![global_file],
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
        accepted_len: u64,
    }

    impl UpgradePreflightActor {
        fn rendering(bytes: &[u8]) -> Self {
            Self {
                behavior: UpgradePreflightBehavior::Render(bytes.to_vec()),
                emitted: false,
                discarded: false,
                accepted_len: 0,
            }
        }

        fn trapping() -> Self {
            Self {
                behavior: UpgradePreflightBehavior::Trap,
                emitted: false,
                discarded: false,
                accepted_len: 0,
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
    impl WasmComponentActor for UpgradePreflightActor {
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
        ) -> Result<crate::plugin::runtime::WasmFileTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn open_rows(
            &mut self,
            _limits: WasmTransitionLimits,
            input: WasmOpenRowsInput,
        ) -> Result<crate::plugin::runtime::WasmRowTransition, LixError> {
            self.accepted_len = input.accepted.as_ref().map_or(0, |accepted| accepted.len());
            match &self.behavior {
                UpgradePreflightBehavior::Render(_) => {
                    Ok(crate::plugin::runtime::WasmRowTransition {
                        transition: crate::plugin::runtime::WasmTransitionHandle(1),
                        document: WasmDocumentHandle(2),
                        edits: crate::plugin::runtime::WasmEditCursorHandle(3),
                    })
                }
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
        ) -> Result<crate::plugin::runtime::WasmFileTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn rows_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: WasmRowUpdate,
        ) -> Result<crate::plugin::runtime::WasmRowTransition, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn next_change_page(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
            _cursor: crate::plugin::runtime::WasmChangeCursorHandle,
            _max_bytes: u32,
        ) -> Result<Option<crate::plugin::runtime::WasmChangePage>, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn next_edit_page(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
            _cursor: crate::plugin::runtime::WasmEditCursorHandle,
            _max_edits: u32,
            _max_inline_bytes: u32,
        ) -> Result<Option<crate::plugin::runtime::WasmEditPage>, LixError> {
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let UpgradePreflightBehavior::Render(bytes) = &self.behavior else {
                return Err(unused_upgrade_actor_method());
            };
            Ok(Some(crate::plugin::runtime::WasmEditPage {
                edits: vec![crate::plugin::runtime::WasmOutputSplice {
                    offset: 0,
                    delete_len: self.accepted_len,
                    insert: crate::plugin::runtime::WasmGuestBytes::Inline(bytes.clone().into()),
                }],
                outputs: None,
            }))
        }

        async fn output_len(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
            _outputs: crate::plugin::runtime::WasmByteOutputsHandle,
            _index: u32,
        ) -> Result<u64, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn read_output(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
            _outputs: crate::plugin::runtime::WasmByteOutputsHandle,
            _index: u32,
            _offset: u64,
            _length: u32,
        ) -> Result<Vec<u8>, LixError> {
            Err(unused_upgrade_actor_method())
        }

        async fn finish_transition(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
        ) -> Result<WasmTransitionCounters, LixError> {
            Ok(WasmTransitionCounters::default())
        }

        async fn discard_transition(
            &mut self,
            _transition: crate::plugin::runtime::WasmTransitionHandle,
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
            file_id: "owned-file".to_owned(),
            path: Some("/owned.csv".to_string()),
            plugin: WasmPluginSelection {
                plugin_key: "plugin_csv".to_string(),
                generation: "replacement".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn owned_component_upgrade_preflight_accepts_only_byte_stable_renderer() {
        let expected: crate::Blob = b"first,one\n".as_slice().into();
        let mut compatible = UpgradePreflightActor::rendering(expected.as_ref());
        preflight_rendered_file(
            &mut compatible,
            upgrade_preflight_descriptor(),
            Vec::new(),
            expected.clone(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect("byte-stable replacement should pass preflight");

        let mut output_changing = UpgradePreflightActor::rendering(b"changed\n");
        let error = preflight_rendered_file(
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
        let error = preflight_rendered_file(
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

    fn upgrade_test_entry(hash_byte: char, creatable: bool) -> PluginRegistryEntry {
        let hash = std::iter::repeat_n(hash_byte, 64).collect::<String>();
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: "plugin_csv".to_string(),
            runtime: crate::plugin::runtime::PluginRuntime::WasmComponent,
            api_version: "2.0.0".to_string(),
            capabilities: crate::plugin::runtime::PluginCapabilities {
                column_merger: true,
                file_projection: true,
            },
            path_glob: Some("*.csv".to_string()),
            content: Some(PluginContentMatcher::Text),
            entry: Some("plugin.wasm".to_string()),
            schema_keys: vec!["csv_row".to_string()],
            create_schema_keys: creatable.then(|| "csv_row".to_string()).into_iter().collect(),
            manifest_json: r#"{"entry":"plugin.wasm","file_match":{"content":"text","path_glob":"*.csv"},"key":"plugin_csv","schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: crate::plugin::runtime::plugin_storage_archive_file_id("plugin_csv"),
            archive_path: "/.lix/plugins/plugin_csv.lixplugin".to_string(),
            archive_blob_hash: hash.clone(),
            wasm_blob_hash: Some(hash),
        })
        .expect("upgrade test registry entry should be valid")
    }

    #[test]
    fn owned_component_upgrade_rejects_schema_definition_change_before_authority_swap() {
        let previous = upgrade_test_entry('a', true);
        let upgrade = PluginGenerationUpgrade {
            branch_id: "main".to_string(),
            previous: previous.clone(),
            replacement: upgrade_test_entry('b', true),
        };
        let definition = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "csv_row",
            "columns": [{ "name": "id", "type": "text", "nullable": false }],
            "primary_key": ["id"],
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
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "csv_row",
                    "columns": [
                        { "name": "extra", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["extra"],
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

    #[test]
    fn natural_primary_key_rows_remain_in_complete_replacement_authority() {
        let retained = WasmRowKey::from_typed_parts(
            "csv_row",
            [0; 32],
            vec![lix_schema::Value::Text("natural-primary-key".to_owned())],
        )
        .unwrap();
        let successor = plugin_row_authorities_after_transition(
            &PluginRowAuthorities::empty(),
            &WasmHostRowChanges {
                changes: vec![WasmRowChange::Upsert {
                    row: WasmHostRow {
                        key: retained.clone(),
                        payload: WasmHostBytes::Typed(Arc::new(WasmTypedRow {
                            schema_fingerprint: [0; 32],
                            row_pk: vec![lix_schema::Value::Text("natural-primary-key".to_owned())]
                                .into(),
                            row: lix_schema::Row::from([(
                                "value".to_owned(),
                                lix_schema::Value::Int8(1),
                            )]),
                            native_payload: std::sync::OnceLock::new(),
                            boundary_create_validation: std::sync::OnceLock::new(),
                        })),
                    },
                    effect: WasmChangeEffect::Content,
                }],
            },
            true,
        );

        assert!(successor.contains(&retained));
        assert_eq!(successor.materialize_keys(), BTreeSet::from([retained]));
    }

    /// Mechanism probe for the `lix_file` content-update scaling defect.
    ///
    /// A content-only update changes no descriptor -- same path, same name,
    /// same parent -- so the visible filesystem path index is unchanged by it.
    /// This measures how many times that index is nevertheless rebuilt from
    /// scratch, and how many descriptor rows each rebuild reads, so the cost
    /// can be attributed to repository size rather than inferred from timings.
    ///
    /// The counter lives inside `build_path_index`, the only full-rebuild site,
    /// which is a different layer from the timing instrument that first showed
    /// the slope.
    #[tokio::test]
    async fn content_updates_hit_the_path_index_cache_and_do_not_rebuild_it() {
        async fn measure(files: usize, updates: usize) -> (usize, usize, usize, usize) {
            let storage = Memory::new();
            Engine::initialize(storage.clone())
                .await
                .expect("storage should initialize");
            let engine = Engine::new(storage)
                .await
                .expect("engine should open initialized storage");
            let session = engine.open_session().await.expect("session should open");

            let values = (0..files)
                .map(|index| format!("('/seed-{index:05}.md', CAST('byte-01' AS BYTEA))"))
                .collect::<Vec<_>>()
                .join(", ");
            session
                .execute(
                    &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
                    &[],
                )
                .await
                .expect("fixture files should commit");

            let rows = session
                .execute("SELECT id FROM lix_file ORDER BY path", &[])
                .await
                .expect("file ids should read back");
            let ids = rows
                .rows()
                .iter()
                .map(|row| {
                    row.get::<String>("id")
                        .expect("file row should carry an id")
                })
                .collect::<Vec<_>>();
            assert!(ids.len() >= updates, "fixture must cover every update");

            crate::filesystem::reset_full_rebuild_stats();
            for id in ids.iter().take(updates) {
                session
                    .execute(
                        "UPDATE lix_file SET content = $1 WHERE id = $2",
                        &[
                            Value::Blob(b"byte-02".to_vec().into()),
                            Value::Text(id.clone()),
                        ],
                    )
                    .await
                    .expect("content update should commit");
            }
            let (builds, rows) = crate::filesystem::full_rebuild_stats();
            let (hits, misses) = crate::filesystem::path_index_cache_stats();
            (builds, rows, hits, misses)
        }

        let updates = 10;
        let (small_builds, small_rows, small_hits, small_misses) = measure(50, updates).await;
        let (large_builds, large_rows, large_hits, large_misses) = measure(500, updates).await;
        println!(
            "PATHINDEX files=50 builds={small_builds} rebuilt_rows={small_rows} hits={small_hits} misses={small_misses}"
        );
        println!(
            "PATHINDEX files=500 builds={large_builds} rebuilt_rows={large_rows} hits={large_hits} misses={large_misses}"
        );

        // Connectivity: the lane really is exercised, so a zero rebuild count
        // below is a positive result and not an instrument pointed elsewhere.
        assert!(
            small_hits >= updates && large_hits >= updates,
            "each content update must consult the path-index cache: {small_hits} / {large_hits}"
        );

        // The measured result: content updates HIT this cache. The visible
        // filesystem path index is NOT rebuilt per statement, so it is not the
        // source of the per-statement cost that scales with repository size.
        assert_eq!(
            small_builds, 0,
            "50-file repository rebuilt the path index {small_builds} times"
        );
        assert_eq!(
            large_builds, 0,
            "500-file repository rebuilt the path index {large_builds} times"
        );
        assert_eq!(small_rows, 0, "no rebuild should read descriptor rows");
        assert_eq!(large_rows, 0, "no rebuild should read descriptor rows");
    }

    /// Does the single-row blob-ref probe every `lix_file` content update
    /// issues push `row_pks` down to a seek, or scan the branch's whole
    /// blob-ref collection and filter in memory?
    ///
    /// `rows_scanned` is what the hot-state scan handed back before visibility
    /// resolution -- what storage actually read. `rows_returned` is what the
    /// update consumed. If the pushdown works these track each other; if not,
    /// `rows_scanned` tracks the repository size while `rows_returned` stays 1.
    #[tokio::test]
    async fn content_update_blob_ref_probe_scans_only_its_own_row() {
        async fn measure(files: usize, updates: usize) -> (usize, usize, usize) {
            let storage = Memory::new();
            Engine::initialize(storage.clone())
                .await
                .expect("storage should initialize");
            let engine = Engine::new(storage)
                .await
                .expect("engine should open initialized storage");
            let session = engine.open_session().await.expect("session should open");

            let values = (0..files)
                .map(|index| format!("('/seed-{index:05}.md', CAST('byte-01' AS BYTEA))"))
                .collect::<Vec<_>>()
                .join(", ");
            session
                .execute(
                    &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
                    &[],
                )
                .await
                .expect("fixture files should commit");

            let rows = session
                .execute("SELECT id FROM lix_file ORDER BY path", &[])
                .await
                .expect("file ids should read back");
            let ids = rows
                .rows()
                .iter()
                .map(|row| {
                    row.get::<String>("id")
                        .expect("file row should carry an id")
                })
                .collect::<Vec<_>>();
            assert!(ids.len() >= updates, "fixture must cover every update");

            crate::hot_state::reset_blob_ref_probe_stats();
            for id in ids.iter().take(updates) {
                session
                    .execute(
                        "UPDATE lix_file SET content = $1 WHERE id = $2",
                        &[
                            Value::Blob(b"byte-02".to_vec().into()),
                            Value::Text(id.clone()),
                        ],
                    )
                    .await
                    .expect("content update should commit");
            }
            crate::hot_state::blob_ref_probe_stats()
        }

        let updates = 10;
        let (small_calls, small_scanned, small_returned) = measure(50, updates).await;
        let (large_calls, large_scanned, large_returned) = measure(500, updates).await;
        println!(
            "BLOBPROBE files=50 calls={small_calls} scanned={small_scanned} returned={small_returned}"
        );
        println!(
            "BLOBPROBE files=500 calls={large_calls} scanned={large_scanned} returned={large_returned}"
        );

        // Connectivity: the probe really is issued once per content update, so
        // a low scanned count below means the pushdown works rather than that
        // this counter never ran.
        assert_eq!(
            small_calls, updates,
            "each content update should issue exactly one blob-ref probe"
        );
        assert_eq!(
            large_calls, updates,
            "each content update should issue exactly one blob-ref probe"
        );

        // Each probe consumes exactly its own row, at both repository sizes.
        assert_eq!(small_returned, updates, "one blob-ref row per update");
        assert_eq!(large_returned, updates, "one blob-ref row per update");

        // The claim under test: what storage READ must not grow with the
        // repository. A 10x larger repository must not make this probe read
        // 10x more rows.
        assert!(
            large_scanned < small_scanned * 5,
            "blob-ref probe reads scale with repository size: \
             50 files scanned {small_scanned}, 500 files scanned {large_scanned}"
        );
    }

    /// The by-id content update must reach its blob-ref row by a point read,
    /// never by walking the branch's blob-ref collection.
    ///
    /// This asserts on counters taken INSIDE `hot_scan_entries`, at the loop
    /// that decodes each storage key. The predecessor of this test counted
    /// `scan_batch`'s return value instead, which is already post
    /// `matches_filter` and therefore reads identically under a seek and under
    /// a full-prefix walk -- it reported "already a seek" for what was a walk
    /// over every file in the branch. `entries_decoded` is the number that can
    /// tell those apart, and `calls` makes a zero readable as "this arm did not
    /// run" rather than "the counter did not run".
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn content_update_blob_ref_probe_takes_the_point_route() {
        async fn measure(files: usize, updates: usize) -> (u64, u64, u64, u64, u64) {
            let storage = Memory::new();
            Engine::initialize(storage.clone())
                .await
                .expect("storage should initialize");
            let engine = Engine::new(storage)
                .await
                .expect("engine should open initialized storage");
            let session = engine.open_session().await.expect("session should open");

            let values = (0..files)
                .map(|index| format!("('/seed-{index:05}.md', CAST('byte-01' AS BYTEA))"))
                .collect::<Vec<_>>()
                .join(", ");
            session
                .execute(
                    &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
                    &[],
                )
                .await
                .expect("fixture files should commit");

            let rows = session
                .execute("SELECT id FROM lix_file ORDER BY path", &[])
                .await
                .expect("file ids should read back");
            let ids = rows
                .rows()
                .iter()
                .map(|row| {
                    row.get::<String>("id")
                        .expect("file row should carry an id")
                })
                .collect::<Vec<_>>();
            assert!(ids.len() >= updates, "fixture must cover every update");

            let _ = crate::storage_bench::take_hot_blob_ref_scan_accounting();
            for id in ids.iter().take(updates) {
                session
                    .execute(
                        "UPDATE lix_file SET content = $1 WHERE id = $2",
                        &[
                            Value::Blob(b"byte-02".to_vec().into()),
                            Value::Text(id.clone()),
                        ],
                    )
                    .await
                    .expect("content update should commit");
            }
            let (calls, point_batch, file_prefix, fallback, decoded, matched) =
                crate::storage_bench::take_hot_blob_ref_scan_accounting();

            // The update must actually have landed, or a cheap route would be
            // cheap for the wrong reason.
            for id in ids.iter().take(updates) {
                let read = session
                    .execute(
                        "SELECT content FROM lix_file WHERE id = $1",
                        &[Value::Text(id.clone())],
                    )
                    .await
                    .expect("content should read back");
                let row = read.rows().first().expect("updated file should be visible");
                let content = read
                    .get(row, "content")
                    .expect("content column should be present");
                assert!(
                    matches!(content, Value::Blob(bytes) if bytes.as_ref() == b"byte-02"),
                    "content update must be visible after the pinned probe"
                );
            }
            let _ = crate::storage_bench::take_hot_blob_ref_scan_accounting();
            let _ = (file_prefix, matched);
            (calls, point_batch, fallback, decoded, matched)
        }

        let updates = 10;
        let (small_calls, small_point, small_fallback, small_decoded, _) =
            measure(50, updates).await;
        let (large_calls, large_point, large_fallback, large_decoded, _) =
            measure(500, updates).await;
        println!(
            "BLOBROUTE files=50 calls={small_calls} point={small_point} \
fallback={small_fallback} decoded={small_decoded}"
        );
        println!(
            "BLOBROUTE files=500 calls={large_calls} point={large_point} \
fallback={large_fallback} decoded={large_decoded}"
        );

        // Connectivity: the probe really is issued, so the zeros below are
        // readable as "that arm did not run".
        assert!(
            small_calls >= updates as u64 && large_calls >= updates as u64,
            "every content update should issue a blob-ref probe: \
             50 files {small_calls} calls, 500 files {large_calls} calls"
        );

        // The claim under test: the point route runs and the walk does not.
        assert_eq!(small_fallback, 0, "50-file update must not walk the branch");
        assert_eq!(
            large_fallback, 0,
            "500-file update must not walk the branch"
        );
        assert!(
            small_point >= updates as u64 && large_point >= updates as u64,
            "the point route must serve every probe"
        );

        // The number that distinguishes a seek from a walk, at both sizes. A
        // 10x larger repository must not decode 10x more keys.
        assert_eq!(
            small_decoded, 0,
            "a pinned probe decodes no scanned keys at 50 files"
        );
        assert_eq!(
            large_decoded, 0,
            "a pinned probe decodes no scanned keys at 500 files"
        );
    }

    /// A blob-ref tombstone must not be mistaken for the live row, and must not
    /// hide it.
    ///
    /// `append_blob_ref_tombstone_row` (reached here by deleting the file)
    /// writes the tombstone with its `file_id` set from the same value as its
    /// row PK, so pinning the file id cannot change which rows are visible.
    /// A filter that missed the live row would look like a large speedup rather
    /// than a bug, so this is asserted rather than argued: the file is deleted,
    /// recreated under the SAME id so a tombstone and a live blob ref share one
    /// file id, and then updated through the pinned by-id route.
    #[tokio::test]
    async fn content_update_after_blob_ref_tombstone_still_resolves() {
        const ID: &str = "01920000-0000-7000-8000-0000000004a1";
        const OTHER: &str = "01920000-0000-7000-8000-0000000004a2";

        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open initialized storage");
        let session = engine.open_session().await.expect("session should open");

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) VALUES \
                     ('{ID}', '/a.md', CAST('one' AS BYTEA)), \
                     ('{OTHER}', '/b.md', CAST('keep' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("fixture should commit");

        // Deleting the file tombstones its blob ref.
        session
            .execute(&format!("DELETE FROM lix_file WHERE id = '{ID}'"), &[])
            .await
            .expect("delete should commit");

        // Recreate under the same id, so one file id now owns both a blob-ref
        // tombstone and a live blob ref.
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) VALUES \
                     ('{ID}', '/a.md', CAST('two' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("recreate should commit");

        let affected = session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE id = $2",
                &[
                    Value::Blob(b"three".to_vec().into()),
                    Value::Text(ID.to_string()),
                ],
            )
            .await
            .expect("content rewrite should commit");
        assert_eq!(
            affected.rows_affected(),
            1,
            "the pinned probe must still resolve a file whose id also owns a tombstone"
        );

        let read = session
            .execute(
                "SELECT content FROM lix_file WHERE id = $1",
                &[Value::Text(ID.to_string())],
            )
            .await
            .expect("content should read back");
        let row = read.rows().first().expect("file should be visible");
        let content = read
            .get(row, "content")
            .expect("content column should be present");
        assert!(
            matches!(content, Value::Blob(bytes) if bytes.as_ref() == b"three"),
            "a rewrite over a tombstoned blob ref must resolve through the pinned probe, got {content:?}"
        );

        // The untouched sibling must be unaffected by pinning another file id.
        let other = session
            .execute("SELECT content FROM lix_file WHERE path = '/b.md'", &[])
            .await
            .expect("sibling should read back");
        let other_row = other.rows().first().expect("sibling should be visible");
        let other_content = other
            .get(other_row, "content")
            .expect("sibling content should be present");
        assert!(
            matches!(other_content, Value::Blob(bytes) if bytes.as_ref() == b"keep"),
            "pinning one file id must not disturb another file"
        );

        // A file that is only tombstoned must match nothing, not a stale row.
        session
            .execute(&format!("DELETE FROM lix_file WHERE id = '{OTHER}'"), &[])
            .await
            .expect("second delete should commit");
        let missing = session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE id = $2",
                &[
                    Value::Blob(b"nope".to_vec().into()),
                    Value::Text(OTHER.to_string()),
                ],
            )
            .await
            .expect("update against a deleted file should not error");
        assert_eq!(
            missing.rows_affected(),
            0,
            "a deleted file must not be resurrected by the pinned probe"
        );
    }

    #[tokio::test]
    async fn staged_descriptor_batches_advance_transaction_path_index() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open initialized storage");
        let session = engine.open_session().await.expect("session should open");

        let values = (0..32)
            .map(|index| format!("('/seed-{index:02}.md', CAST('byte-01' AS BYTEA))"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
                &[],
            )
            .await
            .expect("fixture files should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        reset_transaction_path_index_build_stats();
        for index in 0..5 {
            transaction
                .execute(
                    &format!(
                        "INSERT INTO lix_file (path, content) VALUES ('/staged-{index}.md', CAST('byte-02' AS BYTEA))"
                    ),
                    &[],
                )
                .await
                .expect("descriptor batch should stage");
            transaction
                .execute(
                    "UPDATE lix_file SET content = CAST('byte-03' AS BYTEA) WHERE path = '/seed-00.md'",
                    &[],
                )
                .await
                .expect("path lookup after the staged descriptor should succeed");
            let rows = transaction
                .execute(
                    &format!(
                        "SELECT lixcol_commit_id FROM lix_file WHERE path = '/staged-{index}.md'"
                    ),
                    &[],
                )
                .await
                .expect("the advanced path-index row should remain queryable");
            rows.rows()[0]
                .get::<String>("lixcol_commit_id")
                .expect("the advanced descriptor must retain its staged commit id");
        }

        let stats = transaction_path_index_build_stats();
        assert_eq!(
            stats.builds, 2,
            "the data and metadata projections may each build once; later revisions must advance by delta"
        );
        assert!(
            stats.descriptor_rows > 0,
            "the regression must exercise a real transaction path-index build"
        );
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
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
        let session = engine.open_session().await.expect("session should open");

        let values = (0..file_count)
            .map(|index| format!("('/seed-{index:05}.md', CAST('byte-01' AS BYTEA))"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
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
                "INSERT INTO lix_file (path, content) VALUES ('/transaction-anchor.md', CAST('byte-01' AS BYTEA))",
                &[],
            )
            .await
            .expect("transaction anchor descriptor should stage");

        reset_transaction_path_index_build_stats();
        let sql =
            "UPDATE lix_file SET content = CAST('byte-02' AS BYTEA) WHERE path = '/seed-00000.md'";
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
        let session = engine.open_session().await.expect("session should open");

        let values = (0..file_count)
            .map(|index| {
                format!("('file-{index:05}', '/seed-{index:05}.md', CAST('byte-01' AS BYTEA))")
            })
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_file (id, path, content) VALUES {values}"),
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
                    &format!("UPDATE lix_file SET content = CAST('byte-02' AS BYTEA) WHERE path = '{path}'"),
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

    /// Creating a file must advance the cached path index, not discard it.
    ///
    /// This is the create analogue of the `(0, 0)` assertion in
    /// `committed_filesystem_path_index_benchmark_probe`, and it is a
    /// *deterministic* statement of the property: a rebuild count does not
    /// depend on machine, store shape, or fixture size, so it holds where a
    /// timing comparison would be contaminated by the fixture — seeding this
    /// workload runs through the very path the fix changes.
    ///
    /// Each insert also has to chain: the index advanced to revision N is what
    /// insert N+1 must find, otherwise only the first create would be cheap.
    #[tokio::test]
    async fn committed_file_creates_advance_the_path_index_without_rebuilding() {
        if !incremental_filesystem_index_enabled() {
            return;
        }
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open initialized storage");
        let session = engine.open_session().await.expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/seed.md', CAST('byte-01' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed file should commit");
        session
            .execute("SELECT id FROM lix_file WHERE path = '/seed.md'", &[])
            .await
            .expect("path index should warm");
        crate::filesystem::reset_full_rebuild_stats();

        // Reading after each create is what makes this discriminating. A create
        // that fails to advance the index leaves the cache keyed on the
        // superseded revision; nothing observes that until the next read
        // arrives at the new revision, misses, and rebuilds.
        for index in 0..8 {
            session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (path, content) \
                         VALUES ('/created-{index:02}.md', CAST('byte-01' AS BYTEA))"
                    ),
                    &[],
                )
                .await
                .expect("created file should commit");
            // The advanced index must be *correct*, not merely present: an
            // under-projected delta that lost a row shows up here as a missing
            // path rather than as a slow read.
            let result = session
                .execute(
                    &format!("SELECT id FROM lix_file WHERE path = '/created-{index:02}.md'"),
                    &[],
                )
                .await
                .expect("created path should resolve through the advanced index");
            assert_eq!(
                result.len(),
                1,
                "created path /created-{index:02}.md must resolve after the index advanced"
            );
        }

        assert_eq!(
            crate::filesystem::full_rebuild_stats(),
            (0, 0),
            "committed file creates must advance the cached path index, not rebuild it"
        );
    }

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage.clone());
        let hot_state = Arc::new(hot_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let tracked_state = Arc::new(TrackedStateContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionBranch::new(GLOBAL_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            storage.clone(),
            Arc::clone(&hot_state),
            Arc::clone(&tracked_state),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
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
        let staged = transaction
            .scan_hot_state_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    row_pks: vec![RowPk::single("tracked-programmatic")],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    untracked: Some(false),
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await
            .expect("staged tracked row should scan");
        assert_eq!(staged.len(), 1, "staged tracked row should be visible");
        let staged = staged.row(0);
        assert_eq!(
            staged.change_id(),
            None,
            "provisional engine-generated change IDs must not escape before commit"
        );
        transaction
            .commit(&runtime_functions)
            .await
            .expect("transaction should commit");

        let tracked_row = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                row_pk: RowPk::single("tracked-programmatic"),
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
                        && member.change.row_pk.as_single_string_owned().as_deref()
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
        assert_eq!(
            &tracked_change_id.as_uuid().as_bytes()[..12],
            &head_commit_id.as_uuid().as_bytes()[..12],
            "fresh tracked changes should carry their commit-delta address"
        );
        assert_eq!(
            &head_commit_id.as_uuid().as_bytes()[12..],
            &[0; 4],
            "fresh commit ids should reserve the packed change address suffix"
        );
        let addressed_change =
            crate::tracked_state::load_change_record_by_id(&packed_read, tracked_change_id)
                .await
                .expect("direct change address should load")
                .expect("direct change address should exist");
        assert_eq!(
            addressed_change.row_pk.as_single_string_owned().as_deref(),
            Ok("tracked-programmatic")
        );

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
                    row_pk: RowPk::single("tracked-programmatic"),
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

        let live_untracked_row = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                row_pk: RowPk::single("untracked-programmatic"),
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
        // A change id is identity, not changelog membership. This assertion
        // used to read `change_id == None` and stand in for "not in the
        // changelog", which only worked while untracked rows had no id at all.
        // Exclusion is asserted directly against tracked state below; here we
        // assert the identity the row is now required to carry. The id is a
        // freshly drawn v7 and therefore differs per run, so assert the
        // property rather than recording a literal.
        let untracked_change_id = live_untracked_row
            .change_id
            .expect("ordinary untracked rows must carry a change id");
        assert!(
            !untracked_change_id.as_uuid().is_nil(),
            "an untracked row's change id must be a real minted id, not a nil placeholder"
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
                .all(|row| row.row_pk.as_single_string_owned().as_deref()
                    != Ok("untracked-programmatic")),
            "untracked staged rows should not be written into tracked state"
        );
    }

    #[tokio::test]
    async fn transaction_open_prewarms_tracked_and_sql_schema_catalogs() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, transaction) =
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
            .scan_hot_state_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    row_pks: vec![RowPk::single("lossy-timestamp")],
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

    #[test]
    fn explicit_row_timestamps_do_not_advance_deterministic_sequence() {
        let mut row = key_value_stage_row("explicit-timestamps", "value", true);
        row.created_at = Some("2026-04-23T00:00:00.123Z".to_string());
        row.updated_at = Some("2026-04-24T00:00:00.456Z".to_string());
        row.change_id = Some(ChangeId::for_test_label("explicit-change").to_string());
        let rows = raw_write_rows(vec![row]);
        let functions =
            FunctionProviderHandle::shared(Box::new(DeterministicFunctionProvider::new(0, false)));
        let mut default_timestamp = None;

        let planned = plan_prepared_row_scalars(
            rows.row(0),
            NormalizedRowFacts {
                schema_plan_id: SchemaPlanId::for_test(0),
                facts: PreparedRowFacts::default(),
            },
            &functions,
            &mut default_timestamp,
        )
        .expect("explicit row timestamps should plan");

        assert_eq!(planned.created_at.to_string(), "2026-04-23T00:00:00.123Z");
        assert_eq!(planned.updated_at.to_string(), "2026-04-24T00:00:00.456Z");
        assert_eq!(default_timestamp, None);
        assert_eq!(
            functions.deterministic_sequence_persist_highest_seen(),
            None
        );
    }

    #[tokio::test]
    async fn stage_rows_validates_row_content_before_persistence() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage.clone());
        let hot_state = Arc::new(hot_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionBranch::new(GLOBAL_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            storage.clone(),
            Arc::clone(&hot_state),
            Arc::new(TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
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
            json!({"key": "invalid-programmatic", "extra": true}),
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
        let (hot_state, _binary_cas, branch_ref, _runtime_functions, mut transaction) =
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
            &hot_state,
            &branch_ref,
            "invalid-metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let details = error
            .details
            .as_ref()
            .expect("visibility error should be structured");
        assert_eq!(details["schema_key"], "missing_schema");
        assert_eq!(details["scope"], format!("branch:{GLOBAL_BRANCH_ID}"));
        assert_eq!(
            details["base_commit_id"],
            CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID).to_string()
        );
        assert!(details.get("commit_id").is_none());
        assert!(error.hint().is_some(), "visibility error should include a hint");
    }

    #[tokio::test]
    async fn schema_visibility_enrichment_resolves_registration_commit() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, transaction) =
            open_test_transaction(&storage).await;
        let fixture_commit_id = CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID);
        let error = LixError::schema_not_visible(
            "lix_file_descriptor",
            Some(fixture_commit_id.to_string()),
            None::<String>,
            GLOBAL_BRANCH_ID,
            false,
        );

        let error = Transaction::<Memory>::enrich_schema_visibility_error(
            transaction.opening_read(),
            error,
        )
        .await;

        let details = error
            .details
            .as_ref()
            .expect("enriched visibility error should be structured");
        assert_eq!(details["schema_key"], "lix_file_descriptor");
        assert_eq!(details["commit_id"], fixture_commit_id.to_string());
        assert_eq!(details["entity_commit_id"], fixture_commit_id.to_string());
        assert_eq!(details["base_commit_id"], fixture_commit_id.to_string());
        assert_eq!(details["scope"], format!("branch:{GLOBAL_BRANCH_ID}"));
        let hint = error.hint().expect("enriched error should include a hint");
        assert!(hint.contains("is registered at commit"), "{hint}");
        assert!(hint.contains(&fixture_commit_id.to_string()), "{hint}");
    }

    #[tokio::test]
    async fn stage_rows_reports_divergent_schema_registration_commit() {
        const BASE_BRANCH_ID: &str = "01920000-0000-7000-8000-000000000101";
        const SCHEMA_BRANCH_ID: &str = "01920000-0000-7000-8000-000000000102";
        const BASE_COMMIT_ID: &str = "01920000-0000-7000-8000-000000000103";
        const SCHEMA_REGISTRATION_COMMIT_ID: &str = "01920000-0000-7000-8000-000000000104";
        const ENTITY_COMMIT_ID: &str = "01920000-0000-7000-8000-000000000105";
        const SCHEMA_KEY: &str = "divergent_schema";

        let storage = Memory::new();
        let storage_adapter = StorageAdapter::new(storage.clone());
        seed_visible_schema_rows_for_branch(
            storage_adapter.clone(),
            BASE_BRANCH_ID,
            BASE_COMMIT_ID,
        )
        .await;
        let custom_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": SCHEMA_KEY,
            "columns": [{ "name": "id", "type": "text", "nullable": false }],
            "primary_key": ["id"]
        });
        let schema_registration = crate::tracked_state::MaterializedTrackedStateRow {
            row_pk: crate::schema::registered_schema_row_pk(SCHEMA_KEY)
                .expect("registered schema identity should derive"),
            schema_key: "lix_registered_schema".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({ "schema_key": SCHEMA_KEY, "value": custom_schema })
                    .to_string()
                    .into(),
            ),
            decoded_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "1970-01-01T00:00:00.000Z".to_string(),
            updated_at: "1970-01-01T00:00:00.000Z".to_string(),
            change_id: ChangeId::for_test_label("divergent-schema-registration"),
            commit_id: CommitId::for_test_label(SCHEMA_REGISTRATION_COMMIT_ID),
        };
        crate::test_support::seed_branch_head_with_rows(
            storage_adapter.clone(),
            SCHEMA_BRANCH_ID,
            SCHEMA_REGISTRATION_COMMIT_ID,
            std::slice::from_ref(&schema_registration),
        )
        .await;
        let mut entity_read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("entity commit read should open");
        let mut entity_writes = StorageWriteSet::new();
        crate::test_support::stage_tracked_root_from_materialized(
            &mut entity_read,
            &mut entity_writes,
            &TrackedStateContext::new(),
            ENTITY_COMMIT_ID,
            Some(SCHEMA_REGISTRATION_COMMIT_ID),
            &[],
        )
        .await
        .expect("entity commit should retain its parent schema registration");
        storage_adapter
            .commit_write_set(
                entity_writes,
                StorageWriteOptions::default(),
            )
            .await
            .expect("entity commit should persist");

        let opened = open_transaction(
            &SessionBranch::new(BASE_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            storage_adapter,
            Arc::new(hot_state_context()),
            Arc::new(TrackedStateContext::new()),
            Arc::new(BinaryCasContext::new()),
            PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
            Arc::new(BranchContext::new()),
            Arc::new(CatalogContext::new()),
            Arc::new(SqlPlanningCache::default()),
            SessionFileViews::default(),
        )
        .await
        .expect("base-branch transaction should open");
        let mut transaction = opened.transaction;
        let mut row = key_value_stage_row("divergent-row", "value", false);
        row.schema_key = SCHEMA_KEY.into();
        row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({ "id": "divergent-row" }),
        ));
        row.global = false;
        row.branch_id = BASE_BRANCH_ID.into();
        row.commit_id = Some(CommitId::for_test_label(ENTITY_COMMIT_ID).to_string());

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("a schema registered only on a divergent branch must not be visible");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        let details = error
            .details
            .as_ref()
            .expect("divergent visibility error should be structured");
        assert_eq!(details["schema_key"], SCHEMA_KEY);
        assert_eq!(details["scope"], format!("branch:{BASE_BRANCH_ID}"));
        assert_eq!(
            details["commit_id"],
            CommitId::for_test_label(SCHEMA_REGISTRATION_COMMIT_ID).to_string()
        );
        assert_eq!(
            details["entity_commit_id"],
            CommitId::for_test_label(ENTITY_COMMIT_ID).to_string()
        );
        assert_eq!(
            details["base_commit_id"],
            CommitId::for_test_label(BASE_COMMIT_ID).to_string()
        );
        let hint = error.hint().expect("divergent visibility error should have a hint");
        assert!(hint.contains("not an ancestor"), "{hint}");
        assert!(
            hint.contains(
                &CommitId::for_test_label(SCHEMA_REGISTRATION_COMMIT_ID).to_string()
            ),
            "{hint}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_missing_branch_without_sql() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
    async fn stage_rows_rejects_snapshot_with_unknown_schema_v1_column_without_sql() {
        let storage = Memory::new();
        let (hot_state, _binary_cas, branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let storage = StorageAdapter::new(storage);

        let mut row = key_value_stage_row("schema-mismatch", "value", false);
        row.snapshot = Some(TransactionJson::from_value_for_test(
            json!({"key": "schema-mismatch", "extra": true}),
        ));
        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("unknown Schema v1 column should fail statement validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.message.contains("snapshot_content validation failed"),
            "error should explain JSON Schema validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &hot_state,
            &branch_ref,
            "schema-mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("malformed-registered-schema", "value", false);
        row.schema_key = "lix_registered_schema".into();
        row.snapshot = Some(TransactionJson::from_value_for_test(json!({
            "value": {
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "malformed_registered_schema",
                "columns": [{ "name": "id", "type": "text", "nullable": false }],
                "primary_key": ["missing"]
            }
        })));
        row.row_pk = None;

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("malformed registered schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("unknown column 'missing'"),
            "error should explain malformed registered schema: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_primary_key_row_pk_mismatch_without_sql() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;

        let mut row = key_value_stage_row("right-id", "value", false);
        row.row_pk = Some(RowPk::single("wrong-id"));

        let error = transaction
            .stage_rows(raw_write_rows(vec![row]))
            .await
            .expect_err("row pk mismatch should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .message
                .contains("does not match primary_key-derived row_pk"),
            "error should explain row pk mismatch: {error:?}"
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
            row_pk: None,
            schema_key: "lix_account".into(),
            file_id: None,
            snapshot: Some(TransactionJson::from_value_for_test(
                json!({ "name": name, "kind": "human", "status": "active" }),
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
            prepared.row(0).row_pk,
            &RowPk::uuid_from_canonical(&counting_preparation_uuid(1).to_string())
                .expect("UUID pk")
        );
        assert_eq!(
            prepared.row(0).change_id,
            Some(ChangeId::from(counting_preparation_uuid(2)))
        );
        assert_eq!(
            prepared.row(1).row_pk,
            &RowPk::uuid_from_canonical(&counting_preparation_uuid(3).to_string())
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
        Arc<HotStateContext>,
        Arc<BinaryCasContext>,
        Arc<BranchContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageAdapter::new(storage.clone());
        let hot_state = Arc::new(hot_state_context());
        seed_visible_schema_rows(storage.clone()).await;
        let binary_cas = Arc::new(BinaryCasContext::new());
        let branch_ctx = Arc::new(BranchContext::new());
        let catalog_context = Arc::new(CatalogContext::new());
        let opened = open_transaction(
            &SessionBranch::new(GLOBAL_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            storage,
            Arc::clone(&hot_state),
            Arc::new(TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
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
            hot_state,
            binary_cas,
            branch_ctx,
            runtime_functions,
            transaction,
        )
    }

    fn visible_schema_fixture_rows(
        commit_id: &str,
    ) -> Vec<crate::tracked_state::MaterializedTrackedStateRow> {
        crate::schema::seed_schema_definitions()
            .into_iter()
            .map(|schema| {
                let key = crate::schema::schema_key_from_definition(schema)
                    .expect("seed schema key should derive");
                let snapshot_content = json!({
                    "schema_key": key.schema_key.clone(),
                    "value": schema,
                })
                .to_string();
                crate::tracked_state::MaterializedTrackedStateRow {
                    row_pk: crate::schema::registered_schema_row_pk(&key.schema_key)
                        .expect("registered schema identity should derive"),
                    schema_key: "lix_registered_schema".to_string(),
                    file_id: None,
                    snapshot_content: Some(snapshot_content.into()),
                    decoded_snapshot: None,
                    metadata: None,
                    deleted: false,
                    created_at: "1970-01-01T00:00:00.000Z".to_string(),
                    updated_at: "1970-01-01T00:00:00.000Z".to_string(),
                    change_id: ChangeId::for_test_label(&format!(
                        "schema-fixture-{}",
                        key.schema_key
                    )),
                    commit_id: CommitId::for_test_label(commit_id),
                }
            })
            .collect()
    }

    async fn seed_visible_schema_rows_for_branch(
        storage: StorageAdapter,
        branch_id: &str,
        commit_id: &str,
    ) {
        let rows = visible_schema_fixture_rows(commit_id);
        crate::test_support::seed_branch_head_with_rows(
            storage,
            branch_id,
            commit_id,
            &rows,
        )
        .await;
    }

    async fn seed_visible_schema_rows(storage: StorageAdapter) {
        seed_visible_schema_rows_for_branch(
            storage,
            GLOBAL_BRANCH_ID,
            SCHEMA_FIXTURE_COMMIT_ID,
        )
        .await;
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageAdapter,
        hot_state: &HotStateContext,
        branch_ctx: &BranchContext,
        rejected_row_pk: &str,
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
        let row = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                row_pk: RowPk::single(rejected_row_pk),
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
            plugin_key: "plugin_csv".to_string(),
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
            row_pk: Some(RowPk::single(key)),
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
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
            open_test_transaction(&storage).await;
        let source = TransactionWriteRow {
            row_pk: None,
            schema_key: "lix_account".into(),
            file_id: None,
            snapshot: Some(TransactionJson::from_value_for_test(json!({
                "name": "Ada",
                "kind": "human",
                "status": "active",
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
            rendered.row(0).row_pk,
            independently_reprepared.row(0).row_pk,
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
            "durable staging must receive the exact row supplied to rows_changed"
        );
    }

    #[tokio::test]
    async fn reconciled_prepared_slots_follow_raw_row_compaction_in_order() {
        let storage = Memory::new();
        let (_hot_state, _binary_cas, _branch_ref, _runtime_functions, mut transaction) =
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
                row.row_pk.and_then(|row_pk| row_pk.as_single_string().ok()),
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
                .row_pk
                .as_single_string()
                .expect("appended key should stay scalar"),
            "appended-reconciliation",
            "the raw appended row should follow both prepared semantic groups"
        );
    }
}
