#![allow(
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::cell::Cell;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BlobBytesBatch, BlobId, BlobLayout, BlobWriteReceipt};
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchLifecycle, BranchOperation, BranchRefReader,
    BranchReferenceRole,
};
use crate::catalog::{
    CatalogContext, CatalogFingerprint, CatalogSnapshot, SchemaPlanId, load_catalog_revision,
    stage_catalog_revision,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, CommitId, materialize_known_change_payloads,
};
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_marker_stage_row};
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader, ReachableCommitGraphNode};
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    BlobRefPluginCheckpoint, BlobRefRowInput, FileDescriptorWriteIntent, FilesystemPathIndex,
    FilesystemPathIndexCache, FilesystemPathIndexReader, FilesystemPathIndexRequest,
    FilesystemPathKind, FilesystemRowContext, append_blob_ref_tombstone_row,
    load_path_index_revision,
};
use crate::forktree::{
    AuthenticatedBlobReader, CanonicalUploadId, ForkTreeReadFacade, HistoricalStateRow,
    PreparedPublication, StateKey, UploadBindingRef, prepare_upload_part,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::gc::{
    CheckpointGcState, CheckpointPublication, CheckpointRecoveryRef,
    load_checkpoint_publication_state,
};
#[cfg(test)]
use crate::live_state::LiveStateRowRequest;
use crate::live_state::{
    CertifiedCurrentStatePredecessor, LiveStateContext, LiveStateExactBatchRequest,
    LiveStateExactRowRequest, LiveStateFilter, LiveStateProjection, LiveStateReader,
    LiveStateScanRequest, MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder,
    MaterializedLiveStateExactBatch, MaterializedLiveStateRow, MaterializedLiveStateRowRef,
    StagedLiveStateRows, is_derived_schema, overlay_load_exact_batch, overlay_scan_batch,
};
use crate::plugin::{
    ArcByteSource, BoundCreateContext, CompiledPluginCatalog, ConflictRank, FileBytesSha256,
    LiveBatchEntitySource, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginActorCache,
    PluginActorColdInstall, PluginActorColdOpen, PluginActorKey, PluginActorLease,
    PluginActorStagedCheckpoint, PluginActorStore, PluginActorStorePermit,
    PluginArchiveInstallPlan, PluginContentMatcher, PluginEntityAuthorities,
    PluginEntityAuthorityRange, PluginFileOwner, PluginObservation, PluginRegistry,
    PluginRegistryEntry, PluginRegistryEntryInput, PluginRuntimeHost, SchemaAllowlist,
    ValidatedConflictTransition, ValidatedFileTransition, ValidatedSameLengthOutputSplice,
    VecEntityChangeSource, VecEntityConflictSource, VecEntitySource, build_file_update_splices,
    canonicalize_snapshot, drain_conflict_transition_resolutions, drain_entity_transition_edits,
    drain_file_transition_changes, host_entity_change_with_lazy_snapshot,
    host_entity_with_lazy_snapshot, is_plugin_storage_path, is_reservation_key,
    local_mutation_identity, materialize_keyless_creates, plugin_archive_file_id_matches,
    plugin_install_plan_from_archive_path, plugin_key_from_archive_delete_origin,
    plugin_state_live_state_projection, plugin_storage_wasm_file_id,
    require_existing_id_authorities, reservation_tombstone_row, reserve_create_row,
    transport_splice_preserves_prefix_exclusion, transport_splice_preserves_utf8,
    validate_create_changes, validate_create_reservation,
};
use crate::session::{
    EXECUTE_IDEMPOTENCY_RECEIPT_SPACE, ExecuteIdempotency, ExecuteIdempotencyReceipt, SessionMode,
    encode_receipt,
};
use crate::sql2::{
    ChangelogQuerySource, DiffCommand, SessionFileViewKey, SessionFileViewMutation,
    SessionFileViews, SessionPluginFileView, SqlChangelogQuerySource, SqlExecutionContext,
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
    TrackedStateContext, TrackedStateDiffKind, TrackedStateKey, TrackedStateKeyRef,
};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedRowFacts, REGISTERED_SCHEMA_KEY, normalize_raw_write_row_in_place,
    remember_pending_registered_schema,
};
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{
    BranchRefPublicationIntent, ImmutableMutationChunkStage, ImmutableMutationJournalChunk,
    PreparedStateRowOverlay, PreparedWriteSet, TransactionWriteBuffer,
    TransactionWriteBufferCheckpoint,
};
use crate::transaction::stale_commit::{
    StaleCommitPlan, StalePluginReconciliationPlan, classify_stale_commit,
};
use crate::transaction::types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch,
    CertifiedRawWriteBatchPreparation, FileContent, PreparedRowFacts, PreparedStateBatch,
    PreparedTransactionWrite, RawWriteBatch, RawWriteRowRef, StagedCommitChangeBatch,
    StagedCommitChangeBatchBuilder, TransactionFileContent, TransactionJson, TransactionWrite,
    TransactionWriteMode, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteOutcome, TransactionWriteRow, TypedMutationJournalBatch,
    canonicalize_transaction_json_batch, stage_json_from_value,
};

use crate::transaction::validation::{
    TransactionValidationInput, fresh_plugin_file_import_certificate,
    prepared_tracked_rows_have_row_local_certificates, validate_certified_fresh_plugin_file_import,
    validate_certified_tracked_insert_identities, validate_prepared_writes,
};
use crate::wasm::{
    WASM_COMPONENT_API_VERSION, WasmCertifiedEntityBatch, WasmChangeEffect, WasmColdFileUpdate,
    WasmComponentActor, WasmComponentFactory, WasmConflictResolution, WasmConflictTake,
    WasmConflictUpdate, WasmDocumentCheckpoint, WasmDocumentHandle, WasmDurableDocumentCheckpoint,
    WasmEntityChange, WasmEntityConflict, WasmEntityKey, WasmEntityUpdate, WasmFileDescriptor,
    WasmFileUpdate, WasmHostBytes, WasmHostEntity, WasmHostEntityChanges, WasmOpenEntitiesInput,
    WasmOpenFileInput, WasmPluginSelection, WasmTransitionLimits,
};
use crate::{LixError, NullableKeyFilter, SqlQueryResult, Value};

mod cohort;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome {
    pub(crate) storage_stats: StorageWriteSetStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ForkTreeSelectorFence {
    global: Bytes,
    branch: Bytes,
}

async fn load_forktree_selector_fence<R>(
    read: &R,
    branch_id: &str,
) -> Result<ForkTreeSelectorFence, LixError>
where
    R: StorageAdapterRead + Clone,
{
    let facade = ForkTreeReadFacade::new(read.clone());
    let view = facade.branch(branch_id).await?;
    Ok(ForkTreeSelectorFence {
        global: view.raw_global_selector().clone(),
        branch: view.raw_branch_selector().clone(),
    })
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
        && a.opening_selector_fence == b.opening_selector_fence
        && a.idempotency_receipt.is_none()
        && b.idempotency_receipt.is_none()
        && a.pending_forktree_publication.is_none()
        && b.pending_forktree_publication.is_none()
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StaleConflictPayload {
    snapshot: SharedStr,
    metadata: Option<SharedStr>,
}

struct StaleSemanticConflict {
    key: TrackedStateKey,
    base: Option<StaleConflictPayload>,
    a: Option<StaleConflictPayload>,
    b: Option<StaleConflictPayload>,
}

struct StalePluginConflictGroup {
    plugin: PluginRegistryEntry,
    descriptor: WasmFileDescriptor,
    conflicts: Vec<StaleSemanticConflict>,
}

fn stale_payload_from_historical(row: Option<&HistoricalStateRow>) -> Option<StaleConflictPayload> {
    row.filter(|row| !row.deleted).and_then(|row| {
        Some(StaleConflictPayload {
            snapshot: row.snapshot_content.clone()?,
            metadata: row.metadata.clone(),
        })
    })
}

fn state_key_from_tracked(key: &TrackedStateKey) -> StateKey {
    StateKey {
        schema_key: key.schema_key.clone(),
        file_id: key.file_id.clone(),
        entity_pk: key.entity_pk.clone(),
    }
}

fn plugin_wasm_state_key(_branch_id: &str, plugin_key: &str) -> StateKey {
    let file_id = plugin_storage_wasm_file_id(plugin_key);
    StateKey {
        schema_key: BLOB_REF_SCHEMA_KEY.to_owned(),
        file_id: Some(file_id.clone()),
        entity_pk: EntityPk::uuid_from_canonical(&file_id)
            .expect("deterministic plugin WASM owner ID is a UUID"),
    }
}

fn plugin_materialization_state_key(file_id: &str) -> StateKey {
    StateKey {
        schema_key: BLOB_REF_SCHEMA_KEY.to_owned(),
        file_id: Some(file_id.to_owned()),
        entity_pk: EntityPk::uuid_from_canonical(file_id)
            .expect("plugin materialization owner ID is a UUID"),
    }
}

fn historical_row_matches_key(row: &HistoricalStateRow, key: &TrackedStateKey) -> bool {
    row.key.schema_key == key.schema_key
        && row.key.file_id == key.file_id
        && row.key.entity_pk == key.entity_pk
}

async fn load_historical_rows_at_commit<R>(
    facade: &ForkTreeReadFacade<R>,
    commit_id: CommitId,
    keys: &[TrackedStateKey],
) -> Result<Vec<Option<HistoricalStateRow>>, LixError>
where
    R: StorageAdapterRead,
{
    let state_keys = keys.iter().map(state_key_from_tracked).collect::<Vec<_>>();
    let rows = facade
        .load_state_rows_at_commit(&commit_id.to_string(), &state_keys)
        .await?;
    if rows.len() != keys.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "ForkTree historical state batch returned the wrong number of rows",
        ));
    }
    for (row, key) in rows.iter().zip(keys) {
        if let Some(row) = row
            && !historical_row_matches_key(row, key)
        {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree historical state row identity does not match its requested key",
            ));
        }
    }
    Ok(rows)
}

fn stale_conflict_bytes(payload: Option<&StaleConflictPayload>) -> Option<WasmHostBytes> {
    payload
        .map(|payload| WasmHostBytes::Inline(Bytes::copy_from_slice(payload.snapshot.as_bytes())))
}

fn push_stale_conflict_resolution(
    rows: &mut RawWriteBatch,
    conflict: &StaleSemanticConflict,
    resolution: WasmConflictResolution<WasmHostBytes>,
    branch_id: &str,
) -> Result<(), LixError> {
    let payload = stale_conflict_resolution_payload(conflict, resolution)?;
    cohort::push_cohort_payload(rows, &conflict.key, payload.as_ref(), branch_id);
    Ok(())
}

fn stale_conflict_resolution_payload(
    conflict: &StaleSemanticConflict,
    resolution: WasmConflictResolution<WasmHostBytes>,
) -> Result<Option<StaleConflictPayload>, LixError> {
    let payload = match resolution {
        WasmConflictResolution::Take(side) => {
            let selected = match side {
                WasmConflictTake::Base => conflict.base.as_ref(),
                WasmConflictTake::A => conflict.a.as_ref(),
                WasmConflictTake::B => conflict.b.as_ref(),
            }
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "plugin conflict resolver selected an absent value; use delete for a tombstone",
                )
            })?;
            Some(selected.clone())
        }
        WasmConflictResolution::Delete => None,
        WasmConflictResolution::Replace {
            snapshot_content,
            effect,
        } => {
            let WasmHostBytes::CanonicalJson(snapshot) = snapshot_content else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "validated conflict replacement is not canonical JSON",
                ));
            };
            let metadata = match effect {
                WasmChangeEffect::Content => None,
                WasmChangeEffect::FormatOnly => {
                    Some(SharedStr::from_static(V2_FORMAT_ONLY_METADATA_JSON))
                }
            };
            Some(StaleConflictPayload {
                snapshot: snapshot.normalized_shared(),
                metadata,
            })
        }
    };
    Ok(payload)
}

/// The durable identity and blob reference of one plugin materialization.
#[derive(Debug, Clone)]
struct VisibleMaterialization {
    semantic_root: String,
    bytes: VisibleMaterializationBytes,
    durable_checkpoint: Option<DecodedDurablePluginCheckpoint>,
}

#[derive(Debug, Clone)]
enum VisibleMaterializationBytes {
    Blob { hash: BlobId },
}

#[cfg(test)]
fn decode_visible_materialization(
    row: &MaterializedLiveStateRow,
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
    row: MaterializedLiveStateRowRef<'_>,
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
    let durable_checkpoint = if schema_key == BLOB_REF_SCHEMA_KEY {
        let snapshot: PluginUpgradeBlobRefSnapshot =
            serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("invalid authenticated blob reference: {error}"),
                )
            })?;
        snapshot
            .plugin_checkpoint
            .map(|checkpoint| {
                let runtime = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(checkpoint.runtime)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("invalid durable plugin checkpoint runtime: {error}"),
                        )
                    })?;
                let runtime = WasmDurableDocumentCheckpoint::decode(&runtime).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("invalid durable plugin checkpoint envelope: {error}"),
                    )
                })?;
                let authority = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(checkpoint.authority)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            format!("invalid durable plugin checkpoint authority: {error}"),
                        )
                    })?;
                Ok::<DecodedDurablePluginCheckpoint, LixError>(DecodedDurablePluginCheckpoint {
                    generation: checkpoint.generation,
                    semantic_root: checkpoint.semantic_root,
                    runtime: runtime.into(),
                    authorities: PluginEntityAuthorities::decode_checkpoint(&authority)?,
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(VisibleMaterialization {
        semantic_root,
        bytes,
        durable_checkpoint,
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
pub(crate) struct Transaction<StorageImpl: Storage + 'static = Memory> {
    active_branch_id: String,
    active_account_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    plugin_host: PluginRuntimeHost,
    branch_ctx: Arc<BranchContext>,
    schema_resolver: TransactionSchemaResolver,
    /// SQL binding is snapshot-isolated at transaction open. Schema writes
    /// staged later in this transaction affect validation but become visible
    /// to SQL planning only after commit opens a new transaction snapshot.
    sql_schema_snapshot: Arc<CatalogSnapshot>,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    prepared_mutation_program: Option<(
        Arc<str>,
        Arc<crate::sql2::PreparedPathValueReplacementProgram>,
    )>,
    prepared_mutation_timestamp: Option<LixTimestamp>,
    mutation_journal: Option<TransactionMutationJournal>,
    /// Sealing consumes the journal's owned column buffers. If any fallible
    /// validation or staging step rejects those buffers, this transaction can
    /// no longer provide statement atomicity and must remain rollback-only.
    mutation_journal_terminal_error: Option<LixError>,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    /// Coherent storage snapshot retained for explicit transaction reads.
    /// This field is declared before `storage` so it is dropped first.
    opening_read: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
    /// Branch-bound ForkTree owner derived once from the same opening read.
    /// Every transaction-visible ForkTree consumer clones this capability;
    /// none constructs a second unbound facade over the same snapshot.
    opening_forktree: ForkTreeReadFacade<SharedStorageAdapterRead<StorageImpl::Read<'static>>>,
    storage: Arc<StorageAdapter<StorageImpl>>,
    functions: FunctionProviderHandle,
    /// Raw authenticated ForkTree selector pair observed by the coherent
    /// transaction-open read. Publication rechecks this pair through the
    /// same global epoch/branch-generation CAS fence.
    opening_selector_fence: ForkTreeSelectorFence,
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
    pending_forktree_publication: Option<PreparedPublication>,
    await_durable_commit: bool,
    session_file_views: SessionFileViews,
    pending_file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    pending_plugin_wasm_by_owner: BTreeMap<(String, String), (BlobId, Vec<u8>)>,
    pending_plugin_actor_publications: Vec<PendingPluginActorPublication>,
    plugin_generation_read_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    plugin_generation_upgrade_guard: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
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
    metadata: Option<SharedStr>,
    global: bool,
    blob_manifest_object_ids: Vec<crate::forktree::ObjectId>,
}

/// State which must be restored when `RETURNING` evaluation fails after a
/// write has been staged in an explicit SQL transaction.
pub(crate) struct SqlStatementCheckpoint {
    staged_writes: TransactionWriteBufferCheckpoint,
    filesystem_path_index_epoch: usize,
    pending_file_view_mutations: BTreeMap<SessionFileViewKey, SessionFileViewMutation>,
    pending_plugin_wasm_by_owner: BTreeMap<(String, String), (BlobId, Vec<u8>)>,
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

impl<StorageImpl> Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn stage_forktree_upload_part(
        &mut self,
        upload_id: &str,
        binding: UploadBindingRef<'_>,
        part_number: u64,
        byte_offset: u64,
        content: &[u8],
    ) -> Result<crate::forktree::PreparedUploadPart, LixError> {
        let upload_id = CanonicalUploadId::new(upload_id.as_bytes())?;
        let facade = self.forktree_read_facade();
        let view = facade.branch(&self.active_branch_id).await?;
        let prepared =
            prepare_upload_part(&view, upload_id, binding, part_number, byte_offset, content)
                .await
                .map_err(|error| {
                    let error = LixError::from(error);
                    if error
                        .message
                        .contains("upload selector binding does not match this request")
                    {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "upload id is already bound to a different path or size",
                        )
                    } else if error
                        .message
                        .contains("upload part is outside the four-part completion window")
                    {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "upload part is outside the four-part completion window",
                        )
                    } else {
                        error
                    }
                })?;
        if !prepared.already_present {
            let mut publication = PreparedPublication::from_branch_view(&view)?;
            publication.publish_upload_part(prepared.clone())?;
            if let Some(mut pending) = self.pending_forktree_publication.take() {
                pending.merge_from(publication)?;
                self.pending_forktree_publication = Some(pending);
            } else {
                self.pending_forktree_publication = Some(publication);
            }
            self.await_durable_commit = true;
        }
        Ok(prepared)
    }

    /// Rejects a resumable upload that races with an already-published file.
    ///
    /// Completed upload selectors are deliberately retired with the receipt
    /// tree, so the visible BlobRef is the only durable idempotency witness
    /// after completion. An exact one-part replay may therefore return
    /// success without staging a second publication; a different payload (or
    /// a multipart continuation) fails closed as a stale target.
    pub(crate) async fn check_forktree_upload_target(
        &mut self,
        path: &str,
        total_size: u64,
        byte_offset: u64,
        content: &[u8],
    ) -> Result<bool, LixError> {
        let request = FilesystemPathIndexRequest::new(vec![self.active_branch_id.clone()])
            .with_blob_refs(true);
        let index = self.filesystem_path_index(&request).await?;
        let Some(entry) = index
            .exact_entries(path)
            .into_iter()
            .find(|entry| entry.kind == FilesystemPathKind::File)
        else {
            return Ok(false);
        };
        let row = entry.blob_ref_live_row().ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "visible upload target has no authenticated BlobRef",
            )
        })?;
        let snapshot = row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "visible upload target BlobRef has no snapshot",
            )
        })?;
        let snapshot = serde_json::from_str::<serde_json::Value>(snapshot).map_err(|_| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "visible upload target BlobRef snapshot is malformed",
            )
        })?;
        let declared_size = snapshot
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "visible upload target BlobRef size is absent",
                )
            })?;
        let blob_hash = snapshot
            .get("blob_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "visible upload target BlobRef identity is absent",
                )
            })?;
        let expected = BlobId::from_content(content).to_hex();
        if byte_offset == 0
            && total_size == content.len() as u64
            && declared_size == total_size
            && blob_hash == expected
        {
            return Ok(true);
        }
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "upload target is already bound to a different file identity",
        ))
    }

    fn opening_read(&self) -> SharedStorageAdapterRead<StorageImpl::Read<'static>> {
        self.opening_read.clone()
    }

    /// Clones the opaque ForkTree operation owner created from this
    /// transaction's already-retained opening read. This does not acquire or
    /// refresh a storage read and does not expose the underlying handle.
    pub(crate) fn forktree_read_facade(
        &self,
    ) -> ForkTreeReadFacade<SharedStorageAdapterRead<StorageImpl::Read<'static>>> {
        self.opening_forktree.clone()
    }

    async fn reconcile_stale_disjoint_writes<S>(
        &mut self,
        read: &S,
        prepared_writes: &mut PreparedWriteSet,
    ) -> Result<(), LixError>
    where
        S: StorageAdapterRead + Clone,
    {
        let current_selector_fence =
            load_forktree_selector_fence(read, &self.active_branch_id).await?;
        if current_selector_fence == self.opening_selector_fence {
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
            self.opening_selector_fence = current_selector_fence;
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
            // `opening_read` is rebound to the commit-boundary read before
            // reconciliation begins. Keep predecessor hydration on the same
            // operation-owned ForkTree facade as the stale classifier.
            let read = self.opening_read();
            let forktree =
                ForkTreeReadFacade::from_read_on_branch(read, &self.active_branch_id).await?;
            let mut predecessors_by_commit = BTreeMap::new();
            for descriptor in journal_descriptors {
                let predecessors = load_immutable_mutation_predecessors(
                    &forktree,
                    &descriptor.schema_key,
                    &descriptor.branch_id,
                    &descriptor.entity_pk_chunks,
                )
                .await?;
                predecessors_by_commit.insert(descriptor.commit_id, predecessors);
            }
            prepared_writes.hydrate_and_lower_ordered_mutation_journals(predecessors_by_commit)?;
        }

        let read = self.opening_read();
        let facade = ForkTreeReadFacade::from_read_on_branch(read, &self.active_branch_id).await?;
        let concurrent_payload = facade
            .diff_state_rows_between_commits(opening_head, current_head)
            .instrument(tracing::debug_span!(
                target: "lix_transaction",
                "lix.transaction.stale.forktree_diff"
            ))
            .await?;
        let mut concurrent_keys = concurrent_payload
            .into_iter()
            .filter_map(|entry| entry.after.or(entry.before))
            .map(|row| TrackedStateKey {
                schema_key: row.key.schema_key,
                file_id: row.key.file_id,
                entity_pk: row.key.entity_pk,
            })
            .collect::<Vec<_>>();
        let concurrent_identity = facade
            .touched_state_identities_between_commits(opening_head, current_head)
            .instrument(tracing::debug_span!(
                target: "lix_transaction",
                "lix.transaction.stale.forktree_identity"
            ))
            .await?;
        concurrent_keys.extend(
            concurrent_identity
                .into_iter()
                .map(|change| TrackedStateKey {
                    schema_key: change.key.schema_key,
                    file_id: change.key.file_id,
                    entity_pk: change.key.entity_pk,
                }),
        );
        concurrent_keys.sort();
        concurrent_keys.dedup();
        let concurrent_change_count = concurrent_keys.len();
        let plan = {
            let span = tracing::debug_span!(
                target: "lix_transaction",
                "lix.transaction.stale.classify",
                prepared_rows = prepared_writes.state_rows.len(),
                concurrent_changes = concurrent_change_count,
            );
            let _entered = span.enter();
            classify_stale_commit(
                prepared_writes,
                concurrent_keys.iter().map(|key| TrackedStateKeyRef {
                    schema_key: key.schema_key.as_str(),
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                }),
            )
        };
        let discovery = "forktree_diff";
        tracing::debug!(
            target: "lix_transaction",
            plan = plan.kind(),
            discovery,
            prepared_rows = prepared_writes.state_rows.len(),
            concurrent_changes = concurrent_change_count,
            "classified stale transaction commit"
        );
        match plan {
            StaleCommitPlan::Direct | StaleCommitPlan::RevalidateOrdinaryInsert => {}
            StaleCommitPlan::ReconcilePlugin(plan) => {
                let file_count = plan.file_ids.len();
                let semantic_conflict_count = plan.semantic_conflict_indices.len();
                self.reconcile_stale_plugin_writes(
                    &facade,
                    prepared_writes,
                    plan,
                    opening_head,
                    current_head,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_transaction",
                    "lix.transaction.stale.reconcile",
                    file_count,
                    semantic_conflict_count,
                ))
                .await?;
            }
            StaleCommitPlan::Unsafe => {
                return Err(conflict(
                    "concurrent transaction changed an overlapping entity outside a stable plugin-owned file",
                ));
            }
        }

        self.opening_active_branch_head = Some(current_head);
        self.opening_selector_fence = current_selector_fence;
        Ok(())
    }

    async fn reconcile_stale_plugin_writes<R>(
        &mut self,
        facade: &ForkTreeReadFacade<R>,
        prepared_writes: &mut PreparedWriteSet,
        plan: StalePluginReconciliationPlan,
        opening_head: CommitId,
        current_head: CommitId,
    ) -> Result<(), LixError>
    where
        R: StorageAdapterRead,
    {
        let conflict_error = || {
            LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "concurrent transaction changed an overlapping entity outside a stable plugin-owned file",
            )
            .with_hint("Retry the transaction against the latest committed state.")
        };
        let StalePluginReconciliationPlan {
            semantic_conflict_indices: candidate_indices,
            file_ids,
        } = plan;

        let owner_keys = file_ids
            .iter()
            .map(|file_id| TrackedStateKey {
                schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
                file_id: Some(file_id.clone()),
                entity_pk: EntityPk::single(PLUGIN_OWNER_KEY),
            })
            .collect::<Vec<_>>();
        let registry_key = TrackedStateKey {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
            file_id: None,
            entity_pk: EntityPk::single(PLUGIN_REGISTRY_KEY),
        };
        let base_owners = load_historical_rows_at_commit(facade, opening_head, &owner_keys).await?;
        let current_owners =
            load_historical_rows_at_commit(facade, current_head, &owner_keys).await?;
        let base_registry = load_historical_rows_at_commit(
            facade,
            opening_head,
            std::slice::from_ref(&registry_key),
        )
        .await?;
        let current_registry = load_historical_rows_at_commit(
            facade,
            current_head,
            std::slice::from_ref(&registry_key),
        )
        .await?;
        let base_registry_row = base_registry.first().and_then(Option::as_ref);
        let current_registry_row = current_registry.first().and_then(Option::as_ref);
        if base_registry_row.map(|row| row.change_id)
            != current_registry_row.map(|row| row.change_id)
            || base_registry_row.is_none()
            || current_registry_row.is_none()
        {
            return Err(conflict_error());
        }
        let registry_snapshot = current_registry_row
            .filter(|row| !row.deleted)
            .and_then(|row| row.snapshot_content.as_ref())
            .ok_or_else(conflict_error)
            .and_then(|snapshot| {
                serde_json::from_str(snapshot.as_str()).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("plugin registry snapshot is invalid JSON: {error}"),
                    )
                })
            })?;
        let registry =
            PluginRegistry::from_optional_snapshot(Some(&registry_snapshot)).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("plugin registry snapshot is invalid: {error}"),
                )
            })?;

        let path_index = self
            .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
                self.active_branch_id.clone(),
            ]))
            .await?;
        let mut groups = BTreeMap::<String, StalePluginConflictGroup>::new();
        for (owner_index, file_id) in file_ids.iter().enumerate() {
            let base_owner_row = base_owners[owner_index].as_ref();
            let current_owner_row = current_owners[owner_index].as_ref();
            if base_owner_row.map(|row| row.change_id) != current_owner_row.map(|row| row.change_id)
                || base_owner_row.is_none()
                || current_owner_row.is_none()
            {
                return Err(conflict_error());
            }
            let owner = PluginFileOwner::from_historical_state_row(
                current_owner_row.expect("checked above"),
            )?
            .ok_or_else(conflict_error)?;
            let plugin = registry
                .plugin(owner.plugin_key())
                .filter(|plugin| plugin.schema_keys() == owner.schema_keys())
                .cloned()
                .ok_or_else(conflict_error)?;
            let paths = path_index.exact_file_id_entries(file_id);
            let path = paths
                .iter()
                .find(|entry| entry.key.branch_id() == self.active_branch_id)
                .map(|entry| entry.path.clone())
                .ok_or_else(conflict_error)?;
            groups.insert(
                file_id.clone(),
                StalePluginConflictGroup {
                    descriptor: WasmFileDescriptor {
                        path: Some(path.clone()),
                        plugin: WasmPluginSelection {
                            plugin_key: plugin.key().to_owned(),
                            generation: plugin.archive_blob_hash().to_owned(),
                        },
                    },
                    plugin,
                    conflicts: Vec::new(),
                },
            );
        }

        let candidate_keys = candidate_indices
            .iter()
            .map(|&index| {
                let row = prepared_writes.state_rows.row(index);
                TrackedStateKey {
                    schema_key: row.schema_key.to_string(),
                    file_id: row.file_id.map(|file_id| file_id.to_string()),
                    entity_pk: row.entity_pk.clone(),
                }
            })
            .collect::<Vec<_>>();
        let base_rows =
            load_historical_rows_at_commit(facade, opening_head, &candidate_keys).await?;
        let current_rows =
            load_historical_rows_at_commit(facade, current_head, &candidate_keys).await?;
        for (slot, &row_index) in candidate_indices.iter().enumerate() {
            let source = prepared_writes.state_rows.row(row_index);
            let file_id = source.file_id.expect("candidate has file id").to_string();
            let group = groups.get_mut(&file_id).ok_or_else(conflict_error)?;
            if !group
                .plugin
                .schema_keys()
                .iter()
                .any(|schema_key| schema_key == source.schema_key.as_str())
            {
                return Err(conflict_error());
            }
            let target = current_rows[slot].as_ref();
            let source_payload = source.snapshot.map(|snapshot| StaleConflictPayload {
                snapshot: snapshot.materialize_shared(),
                metadata: source
                    .metadata
                    .map(|metadata| metadata.materialize_shared()),
            });
            let target_payload = stale_payload_from_historical(target);
            let source_change_id = source.change_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "staged tracked plugin row is missing change_id",
                )
            })?;
            let source_rank = ConflictRank::new(source.updated_at, source_change_id);
            let target_rank = target.map(|row| ConflictRank::new(row.updated_at, row.change_id));
            let (a, b) = if target_rank.is_some_and(|rank| rank < source_rank) {
                (target_payload, source_payload)
            } else {
                (source_payload, target_payload)
            };
            group.conflicts.push(StaleSemanticConflict {
                key: candidate_keys[slot].clone(),
                base: stale_payload_from_historical(base_rows[slot].as_ref()),
                a,
                b,
            });
        }

        // Staging the original stale semantic update retained a successor
        // actor. Retire it before admitting the short-lived static resolver;
        // otherwise two same-base transactions can exhaust the bounded Store
        // pool while one waits for capacity held by its own superseded work.
        let (superseded, retained): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.pending_plugin_actor_publications)
                .into_iter()
                .partition(|publication| file_ids.contains(&publication.session_key().file_id));
        self.pending_plugin_actor_publications = retained;
        discard_plugin_actor_publications(superseded).await;
        self.pending_file_view_mutations
            .retain(|key, _| !file_ids.contains(&key.file_id));
        self.session_file_views
            .apply_mutations(file_ids.iter().map(|file_id| {
                let key = SessionFileViewKey::new(&self.active_branch_id, file_id);
                SessionFileViewMutation::Remove { key }
            }));

        let mut reconciliation_batches = BTreeMap::<String, RawWriteBatch>::new();
        for (file_id, group) in &groups {
            if group.conflicts.is_empty() {
                continue;
            }
            let conflicts = group
                .conflicts
                .iter()
                .enumerate()
                .map(|(ordinal, conflict)| {
                    Ok(WasmEntityConflict {
                        ordinal: u32::try_from(ordinal).map_err(|_| {
                            LixError::new(
                                LixError::CODE_INVALID_PLUGIN,
                                "plugin conflict batch exceeds the u32 ordinal limit",
                            )
                        })?,
                        key: WasmEntityKey::from_owned_parts(
                            conflict.key.schema_key.clone(),
                            conflict.key.entity_pk.clone().into_parts(),
                        ),
                        base: stale_conflict_bytes(conflict.base.as_ref()),
                        a: stale_conflict_bytes(conflict.a.as_ref()),
                        b: stale_conflict_bytes(conflict.b.as_ref()),
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?;
            let resolutions = self
                .resolve_plugin_conflicts(&group.plugin, group.descriptor.clone(), conflicts)
                .instrument(tracing::debug_span!(
                    target: "lix_transaction",
                    "lix.transaction.stale.resolve_plugin",
                    plugin_key = group.plugin.key(),
                    conflict_count = group.conflicts.len(),
                ))
                .await?;
            let rows = reconciliation_batches
                .entry(file_id.clone())
                .or_insert_with(|| RawWriteBatch::with_capacity(group.conflicts.len()));
            for (conflict, resolution) in group.conflicts.iter().zip(resolutions.resolutions) {
                push_stale_conflict_resolution(rows, conflict, resolution, &self.active_branch_id)?;
            }
        }
        let conflict_row_indices = candidate_indices.iter().copied().collect::<BTreeSet<_>>();
        for (index, row) in prepared_writes.state_rows.iter().enumerate() {
            if conflict_row_indices.contains(&index) {
                continue;
            }
            let Some(file_id) = row.file_id.map(SharedStr::as_str) else {
                continue;
            };
            let Some(group) = groups.get(file_id) else {
                continue;
            };
            if !group
                .plugin
                .schema_keys()
                .iter()
                .any(|schema_key| schema_key == row.schema_key.as_str())
            {
                continue;
            }
            reconciliation_batches
                .entry(file_id.to_owned())
                .or_insert_with(RawWriteBatch::new)
                .push_parts(
                    Some(row.entity_pk.clone()),
                    row.schema_key.clone(),
                    row.file_id.cloned(),
                    row.snapshot.map(|snapshot| {
                        TransactionJson::from_unvalidated_shared_normalized_content(
                            snapshot.materialize_shared(),
                        )
                    }),
                    row.metadata.map(|metadata| {
                        TransactionJson::from_unvalidated_shared_normalized_content(
                            metadata.materialize_shared(),
                        )
                    }),
                    row.origin.cloned(),
                    Some(row.created_at.to_string().into()),
                    Some(row.updated_at.to_string().into()),
                    row.global,
                    row.change_id.map(|change_id| change_id.to_string().into()),
                    None,
                    row.untracked,
                    row.branch_id.clone(),
                );
        }
        // Conflict discovery and resolution already operate per file. Replay
        // the complete resolved write set through the same boundary so each
        // file performs one semantic transition and one render, independent
        // of how many conflicts and retained edits it contains.
        let replay_batch_count = reconciliation_batches.len();
        async {
            for rows in reconciliation_batches.into_values() {
                self.stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                })
                .await?;
            }
            Ok::<(), LixError>(())
        }
        .instrument(tracing::debug_span!(
            target: "lix_transaction",
            "lix.transaction.stale.replay",
            replay_batch_count,
        ))
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
                    "stale semantic transaction is missing its staged commit identity",
                )
            })?;
        for index in 0..replacement.state_rows.len() {
            replacement.state_rows.set_commit_id(index, Some(commit_id));
        }
        prepared_writes.replace_reconciled_file_writes(replacement, &file_ids);
        Ok(())
    }

    /// Opens an execution-scoped staging area for SQL/provider hooks.
    async fn open<T, F>(
        mode: &SessionMode,
        active_account_id: String,
        storage: StorageAdapter<StorageImpl>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
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
            let active_branch_id =
                resolve_active_branch_id(mode, live_state.as_ref(), branch_ctx.as_ref(), &read)
                    .await?;
            let runtime_functions = FunctionContext::prepare(&read).await?;
            let runtime_boundary_result = runtime_boundary(&runtime_functions).await?;
            let functions = runtime_functions.provider();
            let (sql_schema_catalog, tracked_schema_catalog) = {
                let catalog_revision = load_catalog_revision(&read).await?;
                let visible_live_state = ForkTreeReadFacade::new(read.clone());
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
            let opening_selector_fence =
                load_forktree_selector_fence(&read, &active_branch_id).await?;
            let branch_reader = branch_ctx.ref_reader(&read);
            let opening_active_branch_head =
                branch_reader.load_head_commit_id(&active_branch_id).await?;
            let opening_global_branch_head = if active_branch_id == GLOBAL_BRANCH_ID {
                opening_active_branch_head
            } else {
                branch_reader.load_head_commit_id(GLOBAL_BRANCH_ID).await?
            };
            Ok::<_, LixError>((
                active_branch_id,
                runtime_functions,
                functions,
                sql_schema_catalog,
                tracked_schema_catalog,
                opening_selector_fence,
                opening_active_branch_head,
                opening_global_branch_head,
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
            opening_selector_fence,
            opening_active_branch_head,
            opening_global_branch_head,
            runtime_boundary_result,
        ) = match setup_result {
            Ok(result) => result,
            Err(error) => {
                return Err(error);
            }
        };
        drop(read);
        let opening_forktree =
            ForkTreeReadFacade::from_read_on_branch(opening_read.clone(), &active_branch_id)
                .await?;
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
        Ok((
            OpenTransaction {
                transaction: Self {
                    active_branch_id,
                    active_account_id,
                    live_state,
                    tracked_state,
                    plugin_host,
                    branch_ctx,
                    schema_resolver,
                    sql_schema_snapshot: sql_schema_catalog,
                    sql_planning_cache,
                    prepared_mutation_program: None,
                    prepared_mutation_timestamp: None,
                    mutation_journal: None,
                    mutation_journal_terminal_error: None,
                    staged_writes,
                    filesystem_path_index_cache: Arc::new(FilesystemPathIndexCache::default()),
                    filesystem_path_index_epoch: Arc::new(AtomicUsize::new(0)),
                    opening_read,
                    opening_forktree,
                    storage,
                    functions,
                    opening_selector_fence,
                    opening_active_branch_head,
                    opening_global_branch_head,
                    commit_boundary: None,
                    trust_filesystem_planner: false,
                    origin_key: None,
                    idempotency_receipt: None,
                    pending_forktree_publication: None,
                    await_durable_commit: false,
                    session_file_views,
                    pending_file_view_mutations: BTreeMap::new(),
                    pending_plugin_wasm_by_owner: BTreeMap::new(),
                    pending_plugin_actor_publications: Vec::new(),
                    plugin_generation_read_guard: None,
                    plugin_generation_upgrade_guard: None,
                },
                runtime_functions,
            },
            runtime_boundary_result,
        ))
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
            .commit_prepared(runtime_functions, prepared_writes)
            .await
    }

    async fn commit_prepared(
        mut self,
        runtime_functions: &FunctionContext,
        mut prepared_writes: PreparedWriteSet,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let transaction = &mut self;
        let commit_boundary = transaction.commit_boundary.clone();
        transaction
            .uncache_completed_plugin_actors_for_large_file_writes(&prepared_writes)
            .await;
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
        let commit_read = commit_read_storage
            .begin_read(StorageReadOptions::default())
            .await?;
        // SAFETY: `commit_read_storage` is an `Arc` retained through commit,
        // and the transaction drops this read before its storage field.
        let commit_read = unsafe { assume_static_storage_read::<StorageImpl>(commit_read) };
        let read = SharedStorageAdapterRead::new(commit_read);
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
        let rebuild_filesystem_path_index =
            prepared_writes_require_filesystem_index_rebuild(&prepared_writes);
        let filesystem_delta_rows = if rebuild_filesystem_path_index {
            Vec::new()
        } else {
            prepared_writes
                .state_rows
                .iter()
                .filter(|row| {
                    matches!(
                        row.schema_key.as_str(),
                        "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
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
        let prepared_forktree_plan = match commit::prepare_forktree_publication_with_parent_heads(
            &transaction.active_account_id,
            &commit_parent_heads,
            runtime_functions.deterministic_sequence_checkpoint(),
            read.clone(),
            prepared_writes,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_forktree_publication"
        ))
        .await
        {
            Ok(publication) => publication,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
        };
        let prepared_forktree_plan = match (
            prepared_forktree_plan,
            transaction.pending_forktree_publication.take(),
        ) {
            (commit::PreparedForkTreePlan::Noop, None) => commit::PreparedForkTreePlan::Noop,
            (commit::PreparedForkTreePlan::Noop, Some(publication)) => {
                commit::PreparedForkTreePlan::Publication(publication)
            }
            (commit::PreparedForkTreePlan::Publication(mut publication), Some(upload)) => {
                publication.merge_from(upload)?;
                commit::PreparedForkTreePlan::Publication(publication)
            }
            (publication, None) => publication,
        };
        // ForkTree never commits independently. Its authenticated objects,
        // selectors, untracked rows, and exact CAS fences are lowered once
        // into the transaction-owned in-memory plan. Runtime metadata and the
        // idempotency receipt are appended below before the sole backend
        // prepare/commit boundary.
        let (mut writes, materialization_preconditions) =
            prepared_forktree_plan.into_storage_plan()?;
        if catalog_revision_changed {
            stage_catalog_revision(&mut writes);
        }
        let mut write_options = StorageWriteOptions::default();
        write_options.await_durable = transaction.await_durable_commit;
        write_options
            .preconditions
            .extend(materialization_preconditions);
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
                    space: EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
                    key,
                });
        }
        // Keep the prepared commit's storage borrow independent from the
        // transaction so deterministic preparation failures can still drain
        // prospective plugin actor documents before returning.
        let commit_storage = transaction.storage.clone();
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_write_set_arena(&writes);
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
        if rebuild_filesystem_path_index {
            transaction.live_state.clear_filesystem_path_indexes();
        } else if !filesystem_delta_rows.is_empty()
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
            pending_plugin_wasm_by_owner: self.pending_plugin_wasm_by_owner.clone(),
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
            pending_plugin_wasm_by_owner,
            trust_filesystem_planner,
        } = checkpoint;
        self.staged_writes.restore(staged_writes)?;
        self.filesystem_path_index_epoch
            .store(filesystem_path_index_epoch, Ordering::SeqCst);
        // The cache is derived from the discarded post-image. Evict it rather
        // than cloning potentially large indexes solely for this error path.
        self.filesystem_path_index_cache.clear();
        self.pending_file_view_mutations = pending_file_view_mutations;
        self.pending_plugin_wasm_by_owner = pending_plugin_wasm_by_owner;
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
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        debug_assert!(rows.certified_preparation().is_some());
        self.ensure_plugin_generation_read_guard().await;

        let first = rows.row(0);
        let branch_id = first.branch_id.clone();
        let schema_key = first.schema_key.clone();
        let staged = self.staged_writes.staging_overlay()?;
        if StagedLiveStateRows::collection_replaced(
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
        let tracked_certified_rows = rows.iter().all(|row| !row.untracked);
        let mut prepared = self
            .prepare_transaction_rows(rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_prepare_rows"
            ))
            .await?;
        if tracked_certified_rows {
            assign_certified_tracked_change_ids(&mut prepared, &self.functions);
        }
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
    /// ordinary tracked, unfiled rows in one active-branch entity collection.
    async fn stage_certified_parameter_batch_replace(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(TransactionWriteOutcome { count: 0 });
        }
        debug_assert!(rows.certified_preparation().is_some());
        self.ensure_plugin_generation_read_guard().await;

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let tracked_certified_rows = rows.iter().all(|row| !row.untracked);
        let mut prepared = self
            .prepare_transaction_rows(rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.transaction_prepare_rows"
            ))
            .await?;
        if tracked_certified_rows {
            assign_certified_tracked_change_ids(&mut prepared, &self.functions);
        }
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
            prepared_transaction_write_filesystem_index_impact(&write);
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

    /// Runs the stateless conflict resolver for one pinned component plugin
    /// generation. Unlike normal file mutation this creates no persistent
    /// document actor: a merge may need to resolve one row from a large file,
    /// and the resulting rows are rendered once by the ordinary staged-write
    /// reconciliation path.
    ///
    /// The caller supplies a registry entry loaded from the historical merge
    /// roots, not the mutable current registry. This keeps `b` selection
    /// and plugin code selection deterministic across merge direction and
    /// retries.
    pub(crate) async fn resolve_plugin_conflicts(
        &mut self,
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        conflicts: Vec<WasmEntityConflict<WasmHostBytes>>,
    ) -> Result<ValidatedConflictTransition, LixError> {
        self.ensure_plugin_generation_read_guard().await;
        let expected_count = conflicts.len();
        let limits = conflict_resolution_limits(expected_count)?;
        let source = VecEntityConflictSource::new(conflicts, limits)?;
        let wasm_hash = BlobId::from_hex(plugin.wasm_blob_hash())?;
        let factory = match self
            .plugin_host
            .cached_plugin_factory(plugin.key(), wasm_hash)?
        {
            Some(factory) => factory,
            None => {
                let read = self.opening_read();
                let wasm_key = plugin_wasm_state_key(&self.active_branch_id, plugin.key());
                let wasm = load_transaction_authenticated_plugin_bytes(
                    &read,
                    &self.active_branch_id,
                    &self.staged_writes,
                    &self.pending_plugin_wasm_by_owner,
                    &[(wasm_key, wasm_hash)],
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
                            "plugin registry references missing WASM blob '{}'",
                            wasm_hash.to_hex()
                        ),
                    )
                })?;
                let installed = plugin.to_installed_plugin(wasm)?;
                self.plugin_host
                    .load_or_compile_factory(&installed)
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
            .record_transition_counters(validated.counters);
        Ok(validated)
    }

    async fn scan_visible_live_state_batch(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let forktree = self.forktree_read_facade();
        let mut request = request.clone();
        if request.filter.branch_ids.is_empty() {
            // Validation and DML pre-images that omit a branch predicate are
            // scoped to this transaction's active branch before entering the
            // operation-owned ForkTree overlay.
            request.filter.branch_ids = vec![self.active_branch_id.clone()];
        }
        overlay_scan_batch(&forktree, &staged, &request).await
    }

    async fn visible_materialization(
        &mut self,
        key: &PluginFileWriteKey,
    ) -> Result<Option<VisibleMaterialization>, LixError> {
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
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
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentFactory>,
        current_publications: &mut Vec<PendingPluginActorPublication>,
    ) -> Result<PluginObservation, LixError> {
        let cache = self.plugin_host.actor_cache();
        let _cold_open_guard = cache.cold_open_guard().await;
        let staged = self.staged_writes.staging_overlay()?;
        let read = self.opening_read();
        let base = ForkTreeReadFacade::new(read.clone());
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
                    schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_string()],
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
        let rows = if actor.cold_open_requires_entities() {
            overlay_scan_batch(
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
            .await?
        } else {
            MaterializedLiveStateBatch::default()
        };
        let entity_ordinals =
            v2_host_entity_ordinals_from_live_batch(&rows, &file_key, plugin.schema_keys())?;
        let entity_authorities =
            plugin_entity_authorities_from_live_batch(plugin, &rows, &entity_ordinals);
        let entity_count = entity_ordinals.len();
        let VisibleMaterializationBytes::Blob { hash } = materialization.bytes;
        let materialized_bytes: crate::Blob = load_transaction_authenticated_plugin_bytes(
            &read,
            &actor_key.branch_id,
            &self.staged_writes,
            &self.pending_plugin_wasm_by_owner,
            &[(plugin_materialization_state_key(&actor_key.file_id), hash)],
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
        let source = LiveBatchEntitySource::new(rows, entity_ordinals, limits)?;
        let transition = match actor
            .open_entities(
                limits,
                WasmOpenEntitiesInput {
                    descriptor,
                    entities: Box::new(source),
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
        let validated = match drain_entity_transition_edits(
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
            u64::try_from(entity_count).unwrap_or(u64::MAX);
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
                entity_authorities,
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
        plugin: &PluginRegistryEntry,
        descriptor: WasmFileDescriptor,
        factory: Arc<dyn WasmComponentFactory>,
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
                let Some(visible_materialization) = self.visible_materialization(&file_key).await?
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
            .cold_open_semantic_actor(actor_key, plugin, descriptor, factory, current_publications)
            .await?;
        cache.lease_for_transition(&reopened).await
    }

    async fn load_visible_exact_live_state_batch(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let forktree = self.forktree_read_facade();
        overlay_load_exact_batch(&forktree, &staged, request).await
    }

    /// Drops `format-only` upserts that are semantically identical to the
    /// currently accepted durable entity. The exact-row lookup keeps this
    /// proportional to the sparse format-only output instead of hydrating the
    /// complete file graph.
    async fn suppress_format_only_noops(
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
            suppress_format_only_noops_against_batch(changes, &format_only_keys, &current)?,
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
        known_existing_authorities: Option<&PluginEntityAuthorities>,
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
                            format!("component plugin emitted invalid entity_pk: {error}"),
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

    async fn preflight_create(
        &mut self,
        bound: BoundCreateContext,
        file_key: &PluginFileWriteKey,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        self.preflight_creates(&[(bound, file_key.clone())])
            .await
            .map(|mut rows| rows.pop().expect("one preflight produces one result"))
    }

    async fn preflight_creates(
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
                let mut reconciliation = self
                    .plugin_write_reconciliation(&mut rows, &mut file_content)
                    .await?;
                reconciliation.attach_durable_checkpoints(&mut file_content)?;
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
                        plugin_checkpoint: payload.plugin_checkpoint().map(|checkpoint| {
                            BlobRefPluginCheckpoint {
                                generation: checkpoint.generation.clone(),
                                semantic_root: checkpoint.semantic_root.clone(),
                                runtime: checkpoint.runtime.as_ref().to_vec(),
                                authority: checkpoint.authority.as_ref().to_vec(),
                            }
                        }),
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
                let mut reconciliation = self
                    .plugin_write_reconciliation(&mut rows, &mut file_content)
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.plugin_reconciliation"
                    ))
                    .await?;
                reconciliation.attach_durable_checkpoints(&mut file_content)?;
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
                        plugin_checkpoint: payload.plugin_checkpoint().map(|checkpoint| {
                            BlobRefPluginCheckpoint {
                                generation: checkpoint.generation.clone(),
                                semantic_root: checkpoint.semantic_root.clone(),
                                runtime: checkpoint.runtime.as_ref().to_vec(),
                                authority: checkpoint.authority.as_ref().to_vec(),
                            }
                        }),
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
                    .filter_map(|mut write| {
                        let key = PluginFileWriteKey::from(&write);
                        let retain_payload = !reconciliation.file_keys.contains(&key)
                            && (!write.is_empty()
                                || reconciliation.materialized_file_keys.contains(&key));
                        if retain_payload {
                            return Some(write);
                        }
                        if write.certified_entity_batches().is_empty() {
                            return None;
                        }
                        write.retain_certified_batches_only();
                        Some(write)
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
        let mut current_install_plugin_wasm =
            BTreeMap::<(String, String), (BlobId, Vec<u8>)>::new();
        let mut wasm_owner_payloads = Vec::<TransactionFileContent>::new();
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
                if !write.global && !write.untracked {
                    branch_ids.insert(write.branch_id.clone());
                }
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
                runtime: crate::plugin::PluginRuntime::WasmComponent,
                api_version: WASM_COMPONENT_API_VERSION.to_owned(),
                path_glob: parsed.manifest.file_match.path_glob.clone(),
                content: parsed.manifest.file_match.content,
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
            let wasm_file_id = plugin_storage_wasm_file_id(&lifecycle_key.plugin_key);
            FileDescriptorWriteIntent {
                id: Some(wasm_file_id.clone()),
                directory_id: None,
                name: format!("{}.wasm", lifecycle_key.plugin_key),
                context: FilesystemRowContext {
                    branch_id: write.branch_id.clone(),
                    global: false,
                    untracked: false,
                    file_id: None,
                    metadata: None,
                },
            }
            .append_to(rows);
            BlobRefRowInput {
                file_id: wasm_file_id.clone(),
                blob_hash: parsed.wasm_hash,
                size_bytes: parsed.wasm_bytes.len() as u64,
                plugin_checkpoint: None,
                context: FilesystemRowContext {
                    branch_id: write.branch_id.clone(),
                    global: false,
                    untracked: false,
                    file_id: None,
                    metadata: None,
                },
            }
            .append_to(rows)?;
            // Keep the extracted component as an independently published
            // payload. The BlobRef row above is its sole durable owner; the
            // archive's auxiliary-payload side channel is intentionally not a
            // second, hash-only authority.
            wasm_owner_payloads.push(TransactionFileContent::new(
                wasm_file_id.clone(),
                None,
                None,
                write.branch_id.clone(),
                false,
                false,
                crate::transaction::types::FileContent::inline(parsed.wasm_bytes.clone()),
            ));
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
            current_install_plugin_wasm.insert(
                (write.branch_id.clone(), lifecycle_key.plugin_key.clone()),
                (parsed.wasm_hash, parsed.wasm_bytes.clone()),
            );
            self.pending_plugin_wasm_by_owner.insert(
                (write.branch_id.clone(), wasm_file_id),
                (parsed.wasm_hash, parsed.wasm_bytes.clone()),
            );
            lifecycle_schema_keys.extend(std::iter::repeat_n(
                lifecycle_key.clone(),
                schema_rows.len(),
            ));
            lifecycle_schema_rows.append(schema_rows);
            branch_ids.insert(write.branch_id.clone());
        }
        file_content.extend(wasm_owner_payloads);

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
        let read = self.opening_read();
        let base = ForkTreeReadFacade::new(read.clone());

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
                    registry.remove(&key.plugin_key)?;
                    let wasm_file_id = plugin_storage_wasm_file_id(&key.plugin_key);
                    append_blob_ref_tombstone_row(
                        rows,
                        wasm_file_id.clone(),
                        FilesystemRowContext {
                            branch_id: key.branch_id.clone(),
                            global: false,
                            untracked: false,
                            file_id: None,
                            metadata: None,
                        },
                    );
                    self.pending_plugin_wasm_by_owner
                        .remove(&(key.branch_id.clone(), wasm_file_id));
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
            preflight_owned_generation_upgrades(
                &self.plugin_host,
                &base,
                &staged,
                &read,
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
        for write in file_content.iter() {
            if write.global
                || write.untracked
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
            if file_content_keys.contains(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot mutate both bytes and semantic entities for component plugin file '{file_id}'"
                    ),
                )
                .with_hint("submit either the byte mutation or the resolved entity mutations"));
            }
            if deleted_file_keys.contains_key(&file_key) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "one write batch cannot delete component plugin file '{file_id}' and mutate its semantic entities"
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
                            && !live.untracked
                    })
                    .collect::<Vec<_>>();
                let [entry] = entries.as_slice() else {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        format!(
                            "owned component plugin file '{}' must resolve to exactly one tracked path; found {}",
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
                || write.untracked
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
            // to persist one historical tombstone per semantic entity.
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
        let mut component_factories =
            BTreeMap::<PluginBranchEntryKey, Arc<dyn WasmComponentFactory>>::new();
        let mut cold_entries = BTreeMap::<PluginBranchEntryKey, PluginRegistryEntry>::new();
        for (key, entry) in selected_entries {
            let hash = BlobId::from_hex(entry.wasm_blob_hash())?;
            let cached_factory = self.plugin_host.cached_plugin_factory(entry.key(), hash)?;
            if let Some(factory) = cached_factory {
                component_factories.insert(key, factory);
            } else {
                cold_entries.insert(key, entry);
            }
        }

        let mut wasm_by_hash = current_install_wasm;
        for (key, entry) in &cold_entries {
            let hash = BlobId::from_hex(entry.wasm_blob_hash())?;
            if let Some((installed_hash, bytes)) =
                current_install_plugin_wasm.get(&(key.branch_id.clone(), entry.key().to_owned()))
            {
                if *installed_hash != hash || BlobId::from_content(bytes) != hash {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "staged plugin '{}' WASM owner identity does not match its registry entry",
                            entry.key()
                        ),
                    ));
                }
                wasm_by_hash.insert(hash, bytes.clone());
                continue;
            }
            let wasm_key = plugin_wasm_state_key(&key.branch_id, entry.key());
            let bytes = load_transaction_authenticated_plugin_bytes(
                &read,
                &key.branch_id,
                &self.staged_writes,
                &self.pending_plugin_wasm_by_owner,
                &[(wasm_key, hash)],
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
                        "plugin registry references missing WASM blob '{}'",
                        hash.to_hex()
                    ),
                )
            })?;
            wasm_by_hash.insert(hash, bytes);
        }
        for (key, entry) in cold_entries {
            let hash = BlobId::from_hex(entry.wasm_blob_hash())?;
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
                        let mut validated = drain_file_transition_changes(
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
                        crate::plugin::certify_dense_fresh_file(&mut validated, creates, &schemas)?;
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
                let certified_row_count = validated
                    .certified_batches
                    .iter()
                    .map(|batch| batch.row_count)
                    .sum::<u64>();
                let mut changes = validated.changes;
                let schemas = SchemaAllowlist::from_catalog(
                    pending.selected.schema_keys(),
                    Arc::clone(&self.sql_schema_snapshot),
                )?;
                append_certified_entity_changes(
                    &mut changes,
                    &validated.certified_batches,
                    &schemas,
                )?;
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
                let entity_authorities = plugin_entity_authorities_from_transition(
                    &pending.selected,
                    &changes,
                    &validated.certified_batches,
                )?;
                file_content[pending.file_index]
                    .set_certified_entity_batches(validated.certified_batches);
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&pending.file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes = u64::try_from(changes.entity_change_count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(certified_row_count);
                self.plugin_host.record_transition_counters(counters);

                rows.push(pending.owner_row);
                rows.append(create_rows);
                let write = &mut file_content[pending.file_index];
                let context = FilesystemRowContext {
                    branch_id: write.branch_id.clone(),
                    global: false,
                    untracked: false,
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
                        entity_authorities,
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
                let mut owner_row = desired_owner.write_row(&write.branch_id)?;
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
                            let read = self.opening_read();
                            let base = ForkTreeReadFacade::new(read.clone());
                            let (
                                cold_before,
                                checkpoint_accepted_bytes,
                                checkpoint_blob_hash,
                                cold_edits,
                                host_full_diff_bytes_compared,
                                same_length_blob_splice,
                                blob_edit_splice,
                            ) = {
                                let VisibleMaterializationBytes::Blob { hash } =
                                    visible_materialization.bytes;
                                let before_bytes: crate::Blob =
                                        load_transaction_authenticated_plugin_bytes(
                                            &read,
                                            &actor_key.branch_id,
                                            &self.staged_writes,
                                            &self.pending_plugin_wasm_by_owner,
                                            &[(
                                                plugin_materialization_state_key(
                                                    &actor_key.file_id,
                                                ),
                                                hash,
                                            )],
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
                                let before_source: Arc<dyn crate::wasm::WasmByteSource> =
                                    Arc::new(ArcByteSource::new(before_bytes.clone()));
                                (
                                    Some(before_source),
                                    Some(before_bytes),
                                    Some(hash),
                                    built_splices.edits,
                                    built_splices.full_diff_bytes_compared,
                                    same_length_blob_splice,
                                    blob_edit_splice,
                                )
                            };
                            let decoded_checkpoint = cold_before.as_ref().and_then(|_| {
                                cache.checkpoint(&actor_key, &visible_materialization.semantic_root)
                            });
                            let _ = checkpoint_blob_hash;
                            let durable_checkpoint =
                                visible_materialization.durable_checkpoint.clone();
                            if let Some(checkpoint) = durable_checkpoint.as_ref() {
                                if checkpoint.semantic_root != visible_materialization.semantic_root
                                    || checkpoint.generation != actor_key.plugin_generation
                                {
                                    return Err(LixError::new(
                                        LixError::CODE_INVALID_PLUGIN,
                                        "durable plugin checkpoint identity does not match its authenticated file owner",
                                    ));
                                }
                            }
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
                            let durable_document = if decoded_checkpoint.is_none() {
                                if let Some(checkpoint) = durable_checkpoint.as_ref() {
                                    match actor
                                        .restore_durable_document(
                                            &checkpoint.runtime,
                                            checkpoint_accepted_bytes
                                                .as_deref()
                                                .expect("durable checkpoints are blob-backed"),
                                        )
                                        .await
                                    {
                                        Ok(document) => Some(document),
                                        Err(error) => {
                                            let _ = actor.retire().await;
                                            return Err(error);
                                        }
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            let restored_checkpoint =
                                decoded_checkpoint.is_some() || durable_document.is_some();
                            let mut cold_base_authorities: PluginEntityAuthorities =
                                PluginEntityAuthorities::empty();
                            let transition_result = if let Some(checkpoint) = decoded_checkpoint {
                                drop(base);
                                drop(read);
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
                                        },
                                    )
                                    .instrument(tracing::debug_span!(
                                        target: "lix_perf",
                                        "lix.perf.plugin_checkpoint_file_changed"
                                    ))
                                    .await
                                    .map(|transition| (transition, 0))
                            } else if let Some(document) = durable_document {
                                let checkpoint = durable_checkpoint
                                    .expect("a restored durable document retains its authority");
                                cold_base_authorities = checkpoint.authorities;
                                drop(base);
                                drop(read);
                                actor
                                    .file_changed(
                                        document,
                                        cold_limits,
                                        WasmFileUpdate {
                                            before_descriptor: cold_before_descriptor,
                                            after_descriptor: descriptor.clone(),
                                            before: cold_before.expect(
                                                "durable checkpoints are used only for blob materializations",
                                            ),
                                            edits: cold_edits,
                                            after: Arc::new(ArcByteSource::new(
                                                submitted_bytes.clone(),
                                            )),
                                            creates,
                                        },
                                    )
                                    .instrument(tracing::debug_span!(
                                        target: "lix_perf",
                                        "lix.perf.plugin_durable_checkpoint_file_changed"
                                    ))
                                    .await
                                    .map(|transition| (transition, 0))
                            } else {
                                let entity_rows = overlay_scan_batch(
                                    &base,
                                    &staged,
                                    &LiveStateScanRequest {
                                        filter: LiveStateFilter {
                                            schema_keys: selected.schema_keys().to_vec(),
                                            branch_ids: vec![actor_key.branch_id.clone()],
                                            file_ids: vec![NullableKeyFilter::Value(
                                                actor_key.file_id.clone(),
                                            )],
                                            untracked: Some(false),
                                            ..Default::default()
                                        },
                                        projection: plugin_state_live_state_projection(),
                                        ..Default::default()
                                    },
                                )
                                .await?;
                                let entity_ordinals = v2_host_entity_ordinals_from_live_batch(
                                    &entity_rows,
                                    &file_key,
                                    selected.schema_keys(),
                                )?;
                                let entity_count = entity_ordinals.len();
                                cold_base_authorities = plugin_entity_authorities_from_live_batch(
                                    selected,
                                    &entity_rows,
                                    &entity_ordinals,
                                );
                                let entity_source = LiveBatchEntitySource::new(
                                    entity_rows,
                                    entity_ordinals,
                                    cold_limits,
                                )?;
                                drop(base);
                                drop(read);
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
                                            entities: Box::new(entity_source),
                                        },
                                    )
                                    .instrument(tracing::debug_span!(
                                        target: "lix_perf",
                                        "lix.perf.plugin_cold_file_changed"
                                    ))
                                    .await
                                    .map(|transition| (transition, entity_count))
                            };
                            let (transition, entity_count) = match transition_result {
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
                            let certified_row_count = validated
                                .certified_batches
                                .iter()
                                .map(|batch| batch.row_count)
                                .sum::<u64>();
                            write.set_certified_entity_batches(validated.certified_batches);
                            let mut changes = validated.changes;
                            append_certified_entity_changes(
                                &mut changes,
                                write.certified_entity_batches(),
                                &schemas,
                            )?;
                            let (filtered, observed_existing_authorities) = self
                                .suppress_format_only_noops(selected, changes, &file_key)
                                .await?;
                            changes = filtered;
                            let observed_existing_authorities =
                                PluginEntityAuthorities::from_keys(observed_existing_authorities);
                            let create_rows = self
                                .v2_create_rows(
                                    selected,
                                    &mut changes,
                                    create_context,
                                    &file_key,
                                    existing_create_reservation.as_ref(),
                                    Some(&observed_existing_authorities),
                                )
                                .await?;
                            let entity_authorities = plugin_entity_authorities_after_transition(
                                selected,
                                &cold_base_authorities,
                                &changes,
                                write.certified_entity_batches(),
                            )?;
                            let mut counters = validated.counters;
                            counters.host_full_diff_bytes_compared = host_full_diff_bytes_compared;
                            counters.host_content_classification_bytes =
                                content_classification_bytes
                                    .get(&file_key)
                                    .copied()
                                    .unwrap_or(0);
                            counters.full_state_semantic_rows_materialized =
                                u64::try_from(entity_count).unwrap_or(u64::MAX);
                            counters.private_document_cache_hits = u64::from(restored_checkpoint);
                            counters.full_document_reparses = u64::from(!restored_checkpoint);
                            counters.full_renderer_invocations = 0;
                            counters.durable_semantic_changes =
                                u64::try_from(changes.entity_change_count())
                                    .unwrap_or(u64::MAX)
                                    .saturating_add(certified_row_count);
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
                                    entity_authorities,
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
                            "the acknowledged component file identity no longer matches this write",
                        )
                        .with_hint("read the exact file bytes again before retrying the edit"));
                        }
                        None => {
                            self.cold_open_semantic_actor(
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
                            "the acknowledged component file identity no longer matches this write",
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
                        .lease_or_reopen_observed_actor(
                            &observation,
                            &actor_key,
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

                    let certified_row_count = detected_transition
                        .certified_batches
                        .iter()
                        .map(|batch| batch.row_count)
                        .sum::<u64>();
                    write.set_certified_entity_batches(
                        detected_transition.certified_batches.clone(),
                    );
                    let detection_document = detected_transition.document;
                    let mut counters = detected_transition.counters;
                    let accepted_entity_authorities = lease.accepted_entity_authorities().clone();
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
                    append_certified_entity_changes(
                        &mut changes,
                        &detected_transition.certified_batches,
                        &schemas,
                    )?;
                    let create_rows = match self
                        .v2_create_rows(
                            selected,
                            &mut changes,
                            create_context,
                            &file_key,
                            existing_create_reservation.as_ref(),
                            Some(&accepted_entity_authorities),
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
                    let successor_entity_authorities = plugin_entity_authorities_after_transition(
                        selected,
                        &accepted_entity_authorities,
                        &changes,
                        &detected_transition.certified_batches,
                    )?;
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
                        u64::try_from(changes.entity_change_count())
                            .unwrap_or(u64::MAX)
                            .saturating_add(certified_row_count);
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
                    lease.set_successor_entity_authorities(successor_entity_authorities)?;
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
                let mut validated = drain_file_transition_changes(
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
                crate::plugin::certify_dense_fresh_file(&mut validated, creates, &schemas)?;
                let certified_row_count = validated
                    .certified_batches
                    .iter()
                    .map(|batch| batch.row_count)
                    .sum::<u64>();
                let mut changes = validated.changes;
                append_certified_entity_changes(
                    &mut changes,
                    &validated.certified_batches,
                    &schemas,
                )?;
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
                let entity_authorities = plugin_entity_authorities_from_transition(
                    selected,
                    &changes,
                    &validated.certified_batches,
                )?;
                write.set_certified_entity_batches(validated.certified_batches);
                let mut counters = validated.counters;
                counters.host_content_classification_bytes = content_classification_bytes
                    .get(&file_key)
                    .copied()
                    .unwrap_or(0);
                counters.full_document_reparses = 1;
                counters.durable_semantic_changes = u64::try_from(changes.entity_change_count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(certified_row_count);
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
                        entity_authorities,
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
                        "normalized semantic rows escaped component plugin file '{}' ownership",
                        file_key.file_id
                    ),
                ));
            }
            let limits = WasmTransitionLimits::default();
            let changes = v2_host_changes_from_prepared_rows(&prepared, limits)?;
            if changes.entity_change_count() == 0 {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "component semantic write batch must contain at least one entity change",
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
                            "semantic entity writes cannot follow a byte or identity transition for component plugin file '{}' in the same transaction",
                            file_key.file_id
                        ),
                    )
                    .with_hint("commit the byte transition before editing semantic entities"));
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
                &group.plugin,
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
        mut rows: RawWriteBatch,
        allow_homogeneous: bool,
    ) -> Result<PreparedStateBatch, LixError> {
        let row_count = rows.len();
        // SQL statement time is stable across every row in one write batch.
        // Besides matching mature database semantics, this avoids formatting
        // and sampling the wall clock once per row on bulk writes. Keep the
        // sample lazy so normalization errors retain precedence over scalar
        // provider calls.
        let mut default_timestamp = None;
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
            let timestamp = self.functions.call_timestamp();
            return rows.into_certified_prepared(certificate, self.origin_key.as_ref(), timestamp);
        }
        let staged = self.staged_writes.staging_overlay()?;
        let read = self.opening_read();
        let live_state = ForkTreeReadFacade::new(read.clone());
        if allow_homogeneous && let Some(domain) = homogeneous_row_normalization_domain(&rows) {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&live_state, &staged, &domain)
                .await?;
            let mut scalar_facts = PreparedScalarBatch::with_capacity(rows.len());
            for index in 0..rows.len() {
                let normalized =
                    normalize_raw_write_row_in_place(&mut rows, index, catalog, functions.clone())?;
                scalar_facts.push(plan_prepared_row_scalars(
                    rows.row(index),
                    normalized,
                    &functions,
                    &mut default_timestamp,
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
                    &mut default_timestamp,
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
        let validation_live_state = self.validation_live_state_reader();
        if prepared_tracked_rows_have_row_local_certificates(&prepared_writes.state_rows) {
            // Row-local certificates avoid rebuilding the O(rows) validation
            // index, but they do not prove that a public INSERT identity is
            // absent from committed state.
            if !prepared_writes.insert_selection.is_empty() {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_transaction_validation_branch();
                validate_certified_tracked_insert_identities(
                    &validation_live_state,
                    prepared_writes,
                )
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
            validate_certified_fresh_plugin_file_import(&validation_live_state, certificate)
                .await?;
            return Ok(());
        }
        let staged = self.staged_writes.staging_overlay()?;
        let validation_index = prepared_writes.validation_index();
        for scope in validation_index.schema_scopes() {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_validation_branch();
            let branch_prepared_writes = validation_index.validation_set_for_schema_scope(scope);
            let schema_catalog = self
                .schema_resolver
                .catalog_for_validation(&validation_live_state, &staged, scope)
                .await?;
            let mut validation_input = TransactionValidationInput::new(
                &branch_prepared_writes,
                schema_catalog,
                &validation_live_state,
            );
            if self.trust_filesystem_planner {
                validation_input = validation_input.with_trusted_filesystem_planner();
            }
            validate_prepared_writes(validation_input).await?;
        }
        Ok(())
    }

    fn validation_live_state_reader(
        &self,
    ) -> TransactionValidationLiveStateReader<SharedStorageAdapterRead<StorageImpl::Read<'static>>>
    {
        let forktree = self.forktree_read_facade();
        TransactionValidationLiveStateReader {
            forktree: forktree.clone(),
            graph: Arc::new(tokio::sync::Mutex::new(
                CommitGraphContext::new().reader(forktree),
            )),
        }
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
        let reader = self.branch_ref_reader_on_opening_read();
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

    /// Reports whether visible untracked state is owned by any requested file.
    pub(crate) async fn has_untracked_file_scoped_rows(
        &mut self,
        file_ids: &[String],
    ) -> Result<bool, LixError> {
        if file_ids.is_empty() {
            return Ok(false);
        }
        let branch_id = self.active_branch_id.clone();
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec![branch_id],
                    file_ids: file_ids
                        .iter()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    untracked: Some(true),
                    ..LiveStateFilter::default()
                },
                projection: LiveStateProjection::default(),
                limit: Some(1),
            })
            .await?;
        Ok(!rows.is_empty())
    }

    /// Reports whether the active branch has any visible untracked state.
    pub(crate) async fn has_untracked_rows(&mut self) -> Result<bool, LixError> {
        let branch_id = self.active_branch_id.clone();
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec![branch_id],
                    untracked: Some(true),
                    ..LiveStateFilter::default()
                },
                projection: LiveStateProjection::default(),
                limit: Some(1),
            })
            .await?;
        Ok(!rows.is_empty())
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
        let Some(row) =
            crate::sql2::prepare_path_value_replacement_row(self, &program, params).await?
        else {
            return Ok(Some(crate::sql2::SqlWriteResult::affected(0)));
        };
        let fallback_row = Some(row);
        debug_assert!(
            fallback_row
                .as_ref()
                .is_none_or(|row| row.entity_pk.as_single_string().ok() == Some(primary_key))
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
            self.prepared_mutation_timestamp = None;
            return Ok(());
        }
        self.prepared_mutation_program =
            crate::sql2::prepare_path_value_replacement_program(self, plan)
                .map(|program| (Arc::<str>::from(sql), Arc::new(program)));
        self.prepared_mutation_timestamp = None;
        Ok(())
    }

    pub(crate) async fn flush_prepared_mutations(&mut self) -> Result<(), LixError> {
        if let Some(error) = &self.mutation_journal_terminal_error {
            return Err(error.clone());
        }
        let generation_seed = None;
        let forktree = self.forktree_read_facade();
        let mut complete_generation = resolve_prepared_mutation_collection_generation(
            generation_seed,
            &forktree,
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
        self.prepared_mutation_timestamp = None;
        Ok(())
    }

    /// Flushes pending mutations into the transaction-visible row overlay.
    /// The immutable journal itself is a staged read source, so an intervening
    /// read seals only the current mutable chunk and does not reconstruct a
    /// `PreparedStateBatch`.
    pub(crate) async fn flush_prepared_mutations_for_read(&mut self) -> Result<(), LixError> {
        self.flush_mutation_journal().await?;
        let generation_seed = None;
        let forktree = self.forktree_read_facade();
        let Some((schema_key, (live_count, ordered_identity_digest))) =
            resolve_prepared_mutation_collection_generation(
                generation_seed,
                &forktree,
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
            self.staged_writes
                .set_ordered_mutation_overlay_created_at(created_at)?;
        } else {
            self.hydrate_provisional_mutation_predecessors().await?;
        }
        Ok(())
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
                .is_none_or(|entity_pk| {
                    self.mutation_journal
                        .as_ref()
                        .and_then(TransactionMutationJournal::last_identity)
                        .is_none_or(|last| last < entity_pk)
                })
        } else {
            false
        };
        let chunk_has_capacity = self
            .mutation_journal
            .as_ref()
            .is_none_or(|journal| journal.len() < 4_096);
        if !same_program || !same_origin || !ordered_append || !chunk_has_capacity {
            self.flush_mutation_journal().await?;
        }
        if !same_program || !same_origin || !ordered_append {
            self.lower_provisional_mutations_to_prepared().await?;
        }
        if !same_program {
            self.prepared_mutation_program = None;
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

    async fn flush_mutation_journal_inner(&mut self, _finalize_tail: bool) -> Result<(), LixError> {
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
        let chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            journal.program.schema_plan_id,
            journal.program.schema_key.as_str().into(),
            self.active_branch_id.clone().into(),
            journal.origin_key,
            journal.identity_arena,
            journal.identity_offsets,
            journal.snapshot_arena,
            journal.snapshot_offsets,
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
        match self.staged_writes.stage_immutable_mutation_chunk(chunk)? {
            ImmutableMutationChunkStage::Staged => {}
            ImmutableMutationChunkStage::RequiresGeneric(chunk) => {
                self.lower_provisional_mutations_to_prepared().await?;
                let chunk = self.hydrate_immutable_mutation_chunk(chunk).await?;
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: chunk.into_prepared(false)?,
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
            .entity_pk_chunks()
            .iter()
            .map(|chunk| chunk.len())
            .sum();
        let mut predecessors = Vec::with_capacity(row_count);
        let forktree = self.forktree_read_facade();
        for entity_pks in descriptor.entity_pk_chunks() {
            let request = LiveStateExactBatchRequest {
                rows: entity_pks
                    .iter()
                    .cloned()
                    .map(|entity_pk| LiveStateExactRowRequest {
                        schema_key: schema_key.clone(),
                        branch_id: branch_id.clone(),
                        entity_pk,
                        file_id: None,
                    })
                    .collect(),
                projection: LiveStateProjection::default(),
                untracked: Some(false),
                include_tombstones: false,
            };
            let current = load_opening_exact_live_state_batch(&forktree, &request).await?;
            for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
                let row = current.row(slot).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation lost its current-state predecessor",
                    )
                })?;
                if row.schema_key() != schema_key
                    || row.branch_id() != branch_id
                    || row.entity_pk() != expected_entity_pk
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
        let entity_pks = chunk.materialized_entity_pks();
        let request = LiveStateExactBatchRequest {
            rows: entity_pks
                .iter()
                .cloned()
                .map(|entity_pk| LiveStateExactRowRequest {
                    schema_key: chunk.schema_key().to_owned(),
                    branch_id: chunk.branch_id().to_owned(),
                    entity_pk,
                    file_id: None,
                })
                .collect(),
            projection: LiveStateProjection::default(),
            untracked: Some(false),
            include_tombstones: false,
        };
        let forktree = self.forktree_read_facade();
        let current = load_opening_exact_live_state_batch(&forktree, &request).await?;
        let mut predecessors = Vec::with_capacity(entity_pks.len());
        for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
            let row = current.row(slot).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation lost its current-state predecessor",
                )
            })?;
            if row.schema_key() != chunk.schema_key()
                || row.branch_id() != chunk.branch_id()
                || row.entity_pk() != expected_entity_pk
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
        let forktree = self.forktree_read_facade();
        let branch_ctx = Arc::clone(&self.branch_ctx);
        let visible_schemas = self.sql_visible_schemas();
        let functions = self.functions.clone();
        let staged = self.staged_writes.staging_overlay()?;
        let staged_writes = Arc::clone(&self.staged_writes);
        let filesystem_path_index_cache = Arc::clone(&self.filesystem_path_index_cache);
        let filesystem_path_index_epoch = Arc::clone(&self.filesystem_path_index_epoch);
        let plugin_host = self.plugin_host.clone();
        let sql_planning_cache = Arc::clone(&self.sql_planning_cache);
        let sql_catalog_fingerprint = self.sql_catalog_fingerprint().clone();

        let read_ctx = TransactionSqlReadExecutionContext {
            active_branch_id,
            active_account_id: self.active_account_id.clone(),
            read_store,
            forktree,
            branch_ctx,
            visible_schemas,
            functions,
            staged,
            staged_writes,
            filesystem_path_index_cache,
            filesystem_path_index_epoch,
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

    /// Stages a moving branch-head intent for the selector publication owner.
    /// No `lix_branch_ref` live-state row is created; the intent is consumed
    /// after the transaction's coherent read is retained and lowered into the
    /// same PreparedPublication and backend commit.
    pub(crate) fn stage_branch_ref_intent(
        &mut self,
        branch_id: &str,
        commit_id: Option<CommitId>,
        create: bool,
    ) -> Result<(), LixError> {
        self.staged_writes
            .stage_branch_ref_intent(BranchRefPublicationIntent {
                branch_id: branch_id.to_owned(),
                commit_id,
                create,
                change_id: ChangeId::from(self.functions.call_uuid_v7()),
                updated_at: self.functions.call_timestamp(),
            })
    }

    /// Advances a branch selector without staging tracked rows.
    pub(crate) async fn advance_branch_ref(
        &mut self,
        branch_id: &str,
        commit_id: CommitId,
    ) -> Result<(), LixError> {
        self.stage_branch_ref_intent(branch_id, Some(commit_id), false)
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
        let read = self.opening_read();
        load_checkpoint_publication_state(&read, branch_id).await
    }

    /// Creates a branch-ref reader over this transaction's retained opening
    /// read. Merge planning must not acquire a second snapshot just to resolve
    /// branch selectors.
    pub(crate) fn branch_ref_reader_on_opening_read(&self) -> impl BranchRefReader + '_ {
        self.branch_ctx.ref_reader(&self.opening_read)
    }

    /// Creates a commit-graph reader over the same immutable read that opened
    /// this transaction. The graph reader is bound to the same operation-owned
    /// ForkTree capability as SQL/live-state history and cannot refresh the
    /// transaction's view or acquire a second facade.
    pub(crate) fn commit_graph_reader_on_opening_read(
        &self,
    ) -> CommitGraphStoreReader<
        ForkTreeReadFacade<SharedStorageAdapterRead<StorageImpl::Read<'static>>>,
    > {
        CommitGraphContext::new().reader(self.opening_forktree.clone())
    }

    /// Applies a tracked-state transition resolved from two immutable commits.
    ///
    /// This is the internal counterpart to the public diff command. The
    /// caller supplies typed identities instead of user-facing `diff_id`
    /// strings. The transaction's coherent opening head certifies the current
    /// side, so undo/redo does not need to reload visible live state after it
    /// has already read that exact historical root.
    pub(crate) async fn execute_tracked_state_transition(
        &mut self,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        keys: Vec<TrackedStateKey>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let facade = self.opening_forktree.clone();
        self.execute_tracked_state_transition_with_facade(
            &facade,
            current_commit_id,
            desired_commit_id,
            keys,
        )
        .await
    }

    pub(crate) async fn execute_tracked_state_transition_with_facade<R>(
        &mut self,
        facade: &ForkTreeReadFacade<R>,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        keys: Vec<TrackedStateKey>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError>
    where
        R: StorageAdapterRead,
    {
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
                "typed tracked-state transition contains more than one row for the same entity",
            ));
        }

        let state_keys = keys
            .iter()
            .map(|identity| StateKey {
                schema_key: identity.schema_key.clone(),
                file_id: identity.file_id.clone(),
                entity_pk: identity.entity_pk.clone(),
            })
            .collect::<Vec<_>>();
        let current_rows = facade
            .load_state_rows_at_commit(&current_commit_id.to_string(), &state_keys)
            .await?;
        let desired_rows = facade
            .load_state_rows_at_commit(&desired_commit_id.to_string(), &state_keys)
            .await?;
        let mut transitions = Vec::with_capacity(keys.len());
        for (index, identity) in keys.into_iter().enumerate() {
            let current = current_rows[index].as_ref().filter(|row| !row.deleted);
            let desired = desired_rows[index].as_ref().filter(|row| !row.deleted);
            for row in [current, desired].into_iter().flatten() {
                if row.key.schema_key != identity.schema_key
                    || row.key.file_id != identity.file_id
                    || row.key.entity_pk != identity.entity_pk
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "historical exact read returned a mismatched transition identity",
                    ));
                }
            }
            let expected_change_id = current.map(|row| row.change_id);
            let target = desired.map(|row| TypedStateTransitionTarget {
                change_id: row.change_id,
                snapshot_content: row.snapshot_content.clone(),
                metadata: row.metadata.clone(),
                global: row.global,
                blob_manifest_object_ids: row.blob_manifest_object_ids.clone(),
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
        let mut file_content = Vec::new();
        for transition in transitions {
            if transition.expected_change_id
                == transition.target.as_ref().map(|target| target.change_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "typed tracked-state transition contains an unchanged row",
                ));
            }
            let (snapshot, metadata, target_global) = match transition.target.as_ref() {
                Some(target) => (
                    parse_materialized_diff_json(
                        target.snapshot_content.clone(),
                        "typed state transition target",
                    )?,
                    parse_materialized_diff_json(
                        target.metadata.clone(),
                        "typed state transition target metadata",
                    )?,
                    target.global,
                ),
                None => (None, None, false),
            };
            if let Some(target) = transition.target.as_ref() {
                if let Some(content) = Self::historical_blob_ref_file_content(
                    &transition.identity,
                    target,
                    &branch_id,
                )? {
                    file_content.push(content);
                }
            }
            let write_branch_id = if target_global {
                GLOBAL_BRANCH_ID.to_string()
            } else {
                branch_id.clone()
            };
            rows.push(TransactionWriteRow {
                entity_pk: Some(transition.identity.entity_pk),
                schema_key: transition.identity.schema_key.into(),
                file_id: transition.identity.file_id.map(Into::into),
                snapshot,
                metadata,
                origin: None,
                created_at: None,
                updated_at: None,
                global: target_global,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: write_branch_id.into(),
            });
        }
        let write = if file_content.is_empty() {
            TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            }
        } else {
            TransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows,
                file_content,
                count: rows_affected,
            }
        };
        self.stage_write(write).await?;
        Ok(crate::sql2::DiffCommandOutcome {
            rows_affected,
            commit_id: self
                .staged_writes
                .commit_id_for_branch(&branch_id)?
                .map(|commit_id| commit_id.to_string()),
        })
    }

    fn historical_blob_ref_file_content(
        identity: &TrackedStateKey,
        target: &TypedStateTransitionTarget,
        branch_id: &str,
    ) -> Result<Option<TransactionFileContent>, LixError> {
        if identity.schema_key != "lix_binary_blob_ref" {
            return Ok(None);
        }
        let file_id = identity.file_id.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical BlobRef transition has no file identity",
            )
        })?;
        let entity_pk = EntityPk::uuid_from_canonical(file_id).map_err(|_| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical BlobRef transition file identity is not a canonical UUID",
            )
        })?;
        if identity.entity_pk != entity_pk {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical BlobRef transition key identity is inconsistent",
            ));
        }
        let snapshot = target.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "live historical BlobRef transition has no snapshot",
            )
        })?;
        #[derive(serde::Deserialize)]
        struct HistoricalBlobRefSnapshot {
            id: String,
            blob_hash: String,
            size_bytes: u64,
        }
        let snapshot =
            serde_json::from_str::<HistoricalBlobRefSnapshot>(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("historical BlobRef transition snapshot is malformed: {error}"),
                )
            })?;
        if snapshot.id != file_id {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical BlobRef transition payload identity does not match its StateKey",
            ));
        }
        let blob_hash = BlobId::from_hex(&snapshot.blob_hash).map_err(|error| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("historical BlobRef transition has an invalid BlobId: {error}"),
            )
        })?;
        if target.blob_manifest_object_ids.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical live BlobRef transition must carry exactly one authenticated manifest",
            ));
        }
        let manifest_object_id = target.blob_manifest_object_ids[0];
        if manifest_object_id == crate::forktree::ObjectId::ZERO {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "historical BlobRef transition has a zero manifest identity",
            ));
        }
        let receipt = BlobWriteReceipt {
            hash: blob_hash,
            size_bytes: snapshot.size_bytes,
            // The durable manifest edge is the authority here; this receipt
            // carries no staged payload whose physical layout needs to be
            // described. Commit uses the authenticated manifest identity.
            layout: BlobLayout::Empty,
            manifest_object_id: *manifest_object_id.as_bytes(),
            manifest_was_existing: true,
        };
        Ok(Some(TransactionFileContent::new(
            file_id.to_owned(),
            None,
            None,
            if target.global {
                GLOBAL_BRANCH_ID.to_owned()
            } else {
                branch_id.to_owned()
            },
            target.global,
            false,
            FileContent::PreparedCas(receipt),
        )))
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
        let mut forktree_reader = self.forktree_read_facade();
        let records = if change_ids.is_empty() {
            HashMap::new()
        } else {
            let change_ids = change_ids.into_iter().collect::<Vec<_>>();
            forktree_reader
                .load_change_records(&change_ids)
                .await?
                .into_iter()
                .zip(change_ids)
                .filter_map(|(record, change_id)| record.map(|record| (change_id, record)))
                .collect::<HashMap<_, _>>()
        };
        let mut payloads = materialize_known_change_payloads(
            &mut forktree_reader,
            records.values().cloned(),
            ChangeRecordProjection::full(),
        )
        .await?;
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
        let historical = self.forktree_read_facade();
        let previous_checkpoint_commit_id = historical
            .checkpoint_history_from_head(head_commit_id, &branch_id)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("branch '{branch_id}' has no checkpoint baseline"),
                )
            })?
            .commit_id;
        let diff = historical
            .diff_branch_state_rows_between_commits(previous_checkpoint_commit_id, head_commit_id)
            .await?;
        let requested = diff_ids.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != diff_ids.len() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "checkpoint selection contains duplicate diff_id rows",
            ));
        }
        let mut matched = BTreeSet::new();
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(diff.len());
        let mut unselected = StagedCommitChangeBatchBuilder::with_capacity(diff.len());
        let mut selected_source_membership_exact = true;
        let mut unselected_source_membership_exact = true;
        for entry in diff.into_iter().filter(|entry| {
            entry
                .before
                .as_ref()
                .or(entry.after.as_ref())
                .is_some_and(|row| {
                    row.key.schema_key != CHECKPOINT_MARKER_SCHEMA_KEY
                        && row.key.schema_key != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                })
        }) {
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
            let kind = match entry.before.as_ref() {
                Some(before) if target.deleted && !before.deleted => TrackedStateDiffKind::Removed,
                Some(before) if before.deleted && !target.deleted => TrackedStateDiffKind::Added,
                Some(_) => TrackedStateDiffKind::Modified,
                None if target.deleted => TrackedStateDiffKind::Removed,
                None => TrackedStateDiffKind::Added,
            };
            let target = crate::tracked_state::TrackedStateDiffRow {
                identity: crate::tracked_state::TrackedStateDiffIdentity::from_key(
                    TrackedStateKey {
                        schema_key: target.key.schema_key.clone(),
                        file_id: target.key.file_id.clone(),
                        entity_pk: target.key.entity_pk.clone(),
                    },
                ),
                deleted: target.deleted,
                created_at: target.created_at,
                updated_at: target.updated_at,
                change_id: target.change_id,
                commit_id: target.commit_id,
            };
            if requested.contains(&diff_id) {
                matched.insert(diff_id);
                selected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut selected, target, kind);
            } else {
                unselected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut unselected, target, kind);
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

async fn load_immutable_mutation_predecessors(
    reader: &ForkTreeReadFacade<impl StorageAdapterRead + 'static>,
    schema_key: &str,
    branch_id: &str,
    entity_pk_chunks: &[Arc<[EntityPk]>],
) -> Result<Vec<CertifiedCurrentStatePredecessor>, LixError> {
    let row_count = entity_pk_chunks.iter().map(|chunk| chunk.len()).sum();
    let mut predecessors = Vec::with_capacity(row_count);
    for entity_pks in entity_pk_chunks {
        let request = LiveStateExactBatchRequest {
            rows: entity_pks
                .iter()
                .cloned()
                .map(|entity_pk| LiveStateExactRowRequest {
                    schema_key: schema_key.to_owned(),
                    branch_id: branch_id.to_owned(),
                    entity_pk,
                    file_id: None,
                })
                .collect(),
            projection: LiveStateProjection::default(),
            untracked: Some(false),
            include_tombstones: false,
        };
        let current = reader.load_exact_batch(&request).await?;
        for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
            let row = current.row(slot).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation lost its current-state predecessor",
                )
            })?;
            if row.schema_key() != schema_key
                || row.branch_id() != branch_id
                || row.entity_pk() != expected_entity_pk
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
        "stale or unknown diff_id; re-evaluate the source diff and retry",
    )
}

fn empty_state_transition(current: CommitId, desired: CommitId) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked-state transition from '{current}' to '{desired}' is empty"),
    )
}

async fn opening_parent_complete_lifecycle_created_at(
    _read: &(impl StorageAdapterRead + ?Sized),
    _parent_commit_id: Option<CommitId>,
    _schema_key: &str,
    _live_count: u64,
    _ordered_identity_digest: [u8; 32],
) -> Result<Option<LixTimestamp>, LixError> {
    Ok(None)
}

async fn resolve_prepared_mutation_collection_generation(
    seed: Option<(String, Option<(u64, [u8; 32])>)>,
    forktree: &ForkTreeReadFacade<impl StorageAdapterRead + 'static>,
    branch_id: String,
) -> Result<Option<(String, (u64, [u8; 32]))>, LixError> {
    let Some((schema_key, generation)) = seed else {
        return Ok(None);
    };
    if let Some(generation) = generation {
        return Ok(Some((schema_key, generation)));
    }
    let Some(generation) = forktree
        .collection_generation(
            &branch_id,
            crate::collection_generation::CollectionScopeRef {
                schema_key: &schema_key,
                file_id: None,
            },
        )
        .await?
    else {
        return Ok(None);
    };
    if generation.live_count == crate::collection_generation::DEFERRED_LIVE_COUNT
        || generation.live_count == 0
    {
        return Ok(None);
    }
    let ordered_identity_digest = generation.ordered_identity_digest.ok_or_else(|| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "authenticated collection generation is missing its ordered identity digest",
        )
    })?;
    Ok(Some((
        schema_key,
        (generation.live_count, ordered_identity_digest),
    )))
}

async fn load_opening_exact_live_state_batch(
    forktree: &ForkTreeReadFacade<impl StorageAdapterRead + 'static>,
    request: &LiveStateExactBatchRequest,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    forktree.load_exact_batch(request).await
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
    forktree: ForkTreeReadFacade<SharedStorageAdapterRead<R>>,
    branch_ctx: Arc<BranchContext>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    staged: PreparedStateRowOverlay,
    staged_writes: Arc<TransactionWriteBuffer>,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
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

    fn datafusion_read_session(&self) -> datafusion::prelude::SessionContext {
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

    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(TransactionReadLiveStateReader {
            forktree: self.forktree.clone(),
            read_store: self.read_store.clone(),
            staged: self.staged.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
        })
    }

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(TransactionReadLiveStateReader {
            forktree: self.forktree.clone(),
            read_store: self.read_store.clone(),
            staged: self.staged.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
        })
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            forktree_reader: self.forktree.clone(),
        }
    }

    fn commit_graph(&self) -> Box<dyn crate::commit_graph::CommitGraphReader> {
        Box::new(CommitGraphContext::new().reader(self.forktree.clone()))
    }

    fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
        Arc::new(self.branch_ctx.ref_reader(self.read_store.clone()))
    }

    fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        Ok(Arc::new(crate::forktree::blob_reader_on_read(
            self.read_store.clone(),
            &self.active_branch_id,
        )?))
    }

    async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }

    fn plugin_host(&self) -> PluginRuntimeHost {
        self.plugin_host.clone()
    }
}

/// Loads plugin WASM and materialized component payloads through their exact
/// authenticated BlobRef owners.  Staged bytes are still allowed before the
/// transaction is published, but their semantic BlobId is checked against
/// the caller-owned StateKey request and committed bytes are resolved only by
/// the retained ForkTree read.  The old hash-only BinaryCas reader is not a
/// valid plugin payload source.
async fn load_transaction_authenticated_plugin_bytes<R>(
    read: &SharedStorageAdapterRead<R>,
    branch_id: &str,
    staged_writes: &TransactionWriteBuffer,
    pending_plugin_wasm_by_owner: &BTreeMap<(String, String), (BlobId, Vec<u8>)>,
    requests: &[(StateKey, BlobId)],
) -> Result<BlobBytesBatch, LixError>
where
    R: crate::storage_adapter::StorageRead,
{
    if requests.is_empty() {
        return Ok(BlobBytesBatch::new(Vec::new()));
    }
    let mut entries = vec![None; requests.len()];
    let mut missing = Vec::new();
    for (index, (key, expected)) in requests.iter().enumerate() {
        let staged = key
            .file_id
            .as_deref()
            .map(|file_id| {
                staged_writes.load_staged_file_bytes_for_owner(branch_id, file_id, *expected)
            })
            .transpose()?
            .flatten();
        if let Some(bytes) = staged {
            if BlobId::from_content(&bytes) != *expected {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "staged plugin payload does not match its authenticated BlobRef identity",
                ));
            }
            entries[index] = Some(bytes);
        } else if let Some(file_id) = key.file_id.as_deref()
            && let Some((actual, bytes)) =
                pending_plugin_wasm_by_owner.get(&(branch_id.to_owned(), file_id.to_owned()))
        {
            if *actual != *expected || BlobId::from_content(bytes) != *expected {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "staged plugin payload owner does not match its authenticated BlobRef identity",
                ));
            }
            entries[index] = Some(bytes.clone());
        } else {
            missing.push((index, requests[index].0.clone(), *expected));
        }
    }
    if !missing.is_empty() {
        let reader = crate::forktree::blob_reader_on_read(read.clone(), branch_id)?;
        let keys = missing
            .iter()
            .map(|(_, key, _)| key.clone())
            .collect::<Vec<_>>();
        let fetched = reader.load_bytes_for_rows(&keys).await?.into_vec();
        if fetched.len() != missing.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "authenticated plugin payload batch length mismatch",
            ));
        }
        for ((index, _, expected), bytes) in missing.into_iter().zip(fetched) {
            let bytes = bytes.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "authenticated plugin BlobRef owner has no payload",
                )
            })?;
            if BlobId::from_content(&bytes) != expected {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "authenticated plugin BlobRef payload identity does not match the requested owner",
                ));
            }
            entries[index] = Some(bytes);
        }
    }
    Ok(BlobBytesBatch::new(entries))
}

struct TransactionValidationLiveStateReader<R: StorageAdapterRead + Clone> {
    forktree: ForkTreeReadFacade<R>,
    graph: Arc<tokio::sync::Mutex<CommitGraphStoreReader<ForkTreeReadFacade<R>>>>,
}

impl<R> TransactionValidationLiveStateReader<R>
where
    R: StorageAdapterRead + Clone + 'static,
{
    async fn scan_derived_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "validation request must select exactly one derived schema",
            ));
        };
        if !matches!(
            schema_key.as_str(),
            "lix_commit" | "lix_commit_edge" | BRANCH_REF_SCHEMA_KEY
        ) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "validation request contains an unsupported derived schema",
            ));
        }

        if schema_key == BRANCH_REF_SCHEMA_KEY {
            if !matches!(
                request.filter.rows,
                crate::live_state::LiveStateRowFilter::All
            ) || request.filter.untracked == Some(false)
                || (!request.filter.branch_ids.is_empty()
                    && !request
                        .filter
                        .branch_ids
                        .iter()
                        .any(|branch_id| branch_id == GLOBAL_BRANCH_ID))
            {
                return Ok(Vec::new());
            }
            let mut rows = Vec::new();
            for (branch_id, commit_id) in crate::forktree::scan_branch_heads(&self.forktree).await?
            {
                let entity_pk = EntityPk::uuid_from_canonical(&branch_id).map_err(|error| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("authenticated branch ID is not a UUID: {error}"),
                    )
                })?;
                let metadata =
                    crate::forktree::load_branch_ref_metadata(&self.forktree, &branch_id).await?;
                let snapshot = serde_json::json!({
                    "id": branch_id,
                    "commit_id": commit_id.to_string(),
                });
                rows.push(MaterializedLiveStateRow {
                    entity_pk,
                    schema_key: schema_key.clone(),
                    file_id: None,
                    snapshot_content: Some(
                        serde_json::to_string(&snapshot)
                            .map_err(|error| {
                                LixError::new(
                                    LixError::CODE_STORAGE_ERROR,
                                    format!("branch-ref snapshot serialization failed: {error}"),
                                )
                            })?
                            .into(),
                    ),
                    metadata: None,
                    deleted: false,
                    created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                    updated_at: metadata.updated_at,
                    global: true,
                    change_id: Some(metadata.change_id),
                    commit_id: Some(commit_id),
                    untracked: true,
                    branch_id: Arc::from(GLOBAL_BRANCH_ID),
                });
            }
            rows.retain(|row| {
                request.filter.entity_pks.is_empty()
                    || request.filter.entity_pks.contains(&row.entity_pk)
            });
            rows.retain(|_| {
                request
                    .filter
                    .file_ids
                    .iter()
                    .all(|file_id| file_id.matches(None))
            });
            rows.sort_by(|left, right| left.entity_pk.cmp(&right.entity_pk));
            if let Some(limit) = request.limit {
                rows.truncate(limit);
            }
            return Ok(rows);
        }

        if !matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::All
        ) {
            return Ok(Vec::new());
        }
        let branch_ids = if request.filter.branch_ids.is_empty() {
            crate::forktree::scan_branch_heads(&self.forktree)
                .await?
                .into_iter()
                .map(|(branch_id, _)| branch_id)
                .collect::<Vec<_>>()
        } else {
            request.filter.branch_ids.clone()
        };
        let mut heads = Vec::with_capacity(branch_ids.len());
        for branch_id in branch_ids {
            let head = crate::forktree::load_branch_head(&self.forktree, &branch_id)
                .await?
                .ok_or_else(|| {
                    LixError::branch_not_found(
                        branch_id.clone(),
                        "scan ForkTree derived commit surface",
                        "branch head",
                    )
                })?;
            heads.push((branch_id, head));
        }

        let mut rows = Vec::new();
        let mut graph = self.graph.lock().await;
        for (branch_id, head) in heads {
            let mut roots = BTreeSet::from([head]);
            if schema_key == "lix_commit" {
                for entity_pk in &request.filter.entity_pks {
                    let Ok(commit_text) = entity_pk.as_single_string_owned() else {
                        continue;
                    };
                    if let Ok(commit_id) = CommitId::parse_lix(&commit_text, "requested commit") {
                        roots.insert(commit_id);
                    }
                }
            }
            let mut reachable_by_id = BTreeMap::<CommitId, ReachableCommitGraphNode>::new();
            for root in roots {
                for reachable in graph.reachable_nodes(&root).await?.iter() {
                    reachable_by_id
                        .entry(reachable.commit.commit_id)
                        .or_insert_with(|| reachable.clone());
                }
            }
            for reachable in reachable_by_id.into_values() {
                let commit = reachable.commit;
                if schema_key == "lix_commit" {
                    let snapshot =
                        crate::changelog::commit_row_snapshot_json(&commit.commit_id.to_string())?;
                    let entity_pk = EntityPk::uuid_from_canonical(&commit.commit_id.to_string())
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                format!("authenticated commit ID is not a UUID: {error}"),
                            )
                        })?;
                    rows.push(MaterializedLiveStateRow {
                        entity_pk,
                        schema_key: schema_key.clone(),
                        file_id: None,
                        snapshot_content: Some(snapshot.into()),
                        metadata: None,
                        deleted: false,
                        created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                        updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                        global: branch_id == GLOBAL_BRANCH_ID,
                        change_id: None,
                        commit_id: Some(commit.commit_id),
                        untracked: false,
                        branch_id: Arc::from(branch_id.as_str()),
                    });
                } else {
                    for (parent_order, parent_id) in commit.parent_commit_ids.iter().enumerate() {
                        let parent_order = i64::try_from(parent_order).map_err(|_| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                "authenticated commit parent order exceeds SQL integer range",
                            )
                        })?;
                        let snapshot = serde_json::json!({
                            "parent_id": parent_id.to_string(),
                            "child_id": commit.commit_id.to_string(),
                            "parent_order": parent_order,
                        });
                        let entity_pk = EntityPk::from_json_values(
                            &[
                                serde_json::Value::String(commit.commit_id.to_string()),
                                serde_json::Value::Number(parent_order.into()),
                            ],
                            &[
                                crate::entity_pk::EntityPkComponentType::Uuid,
                                crate::entity_pk::EntityPkComponentType::Integer,
                            ],
                        )
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                format!("authenticated commit edge identity is invalid: {error}"),
                            )
                        })?;
                        rows.push(MaterializedLiveStateRow {
                            entity_pk,
                            schema_key: schema_key.clone(),
                            file_id: None,
                            snapshot_content: Some(
                                serde_json::to_string(&snapshot)
                                    .map_err(|error| {
                                        LixError::new(
                                            LixError::CODE_STORAGE_ERROR,
                                            format!(
                                                "commit edge snapshot serialization failed: {error}"
                                            ),
                                        )
                                    })?
                                    .into(),
                            ),
                            metadata: None,
                            deleted: false,
                            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                            updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                            global: branch_id == GLOBAL_BRANCH_ID,
                            change_id: None,
                            commit_id: Some(commit.commit_id),
                            untracked: false,
                            branch_id: Arc::from(branch_id.as_str()),
                        });
                    }
                }
            }
        }
        rows.retain(|row| {
            request.filter.entity_pks.is_empty()
                || request.filter.entity_pks.contains(&row.entity_pk)
        });
        rows.retain(|_| {
            request
                .filter
                .file_ids
                .iter()
                .all(|file_id| file_id.matches(None))
        });
        rows.sort_by(|left, right| {
            left.branch_id
                .cmp(&right.branch_id)
                .then_with(|| left.entity_pk.cmp(&right.entity_pk))
        });
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait]
impl<R> LiveStateReader for TransactionValidationLiveStateReader<R>
where
    R: StorageAdapterRead + Clone + 'static,
{
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let (current_schema_keys, derived_schema_keys) = validation_schema_partition(request);
        if derived_schema_keys.is_empty() {
            return crate::live_state::scan_forktree_facade(&self.forktree, request).await;
        }
        let mut rows = Vec::new();
        if !current_schema_keys.is_empty() {
            let mut current_request = request.clone();
            current_request.filter.schema_keys = current_schema_keys;
            current_request.limit = None;
            rows.extend(
                crate::live_state::scan_forktree_facade(&self.forktree, &current_request)
                    .await?
                    .into_rows(),
            );
        }
        for schema_key in derived_schema_keys {
            let mut derived_request = request.clone();
            derived_request.filter.schema_keys = vec![schema_key];
            derived_request.limit = None;
            rows.extend(self.scan_derived_rows(&derived_request).await?);
        }
        rows.sort_by(|left, right| {
            left.schema_key
                .cmp(&right.schema_key)
                .then_with(|| left.file_id.cmp(&right.file_id))
                .then_with(|| left.entity_pk.cmp(&right.entity_pk))
                .then_with(|| left.branch_id.cmp(&right.branch_id))
        });
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(MaterializedLiveStateBatch::from_rows(rows))
    }

    async fn scan_constraint_batch(
        &self,
        request: &LiveStateScanRequest,
        _tracked_only: bool,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_batch(request).await
    }

    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_batch(request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let mut output = vec![None; request.rows.len()];
        let mut current_positions = Vec::new();
        let mut derived_positions = BTreeMap::<String, Vec<usize>>::new();
        for (index, row) in request.rows.iter().enumerate() {
            if is_derived_schema(&row.schema_key) {
                derived_positions
                    .entry(row.schema_key.clone())
                    .or_default()
                    .push(index);
            } else {
                current_positions.push(index);
            }
        }

        if !current_positions.is_empty() {
            let current_request = LiveStateExactBatchRequest {
                rows: current_positions
                    .iter()
                    .map(|&index| request.rows[index].clone())
                    .collect(),
                projection: request.projection.clone(),
                untracked: request.untracked,
                include_tombstones: request.include_tombstones,
            };
            let current =
                crate::live_state::load_forktree_exact_facade(&self.forktree, &current_request)
                    .await?
                    .into_rows();
            for (index, row) in current_positions.into_iter().zip(current) {
                output[index] = row;
            }
        }

        for (schema_key, positions) in derived_positions {
            let mut derived_request = request.clone();
            derived_request.rows = positions
                .iter()
                .map(|&index| request.rows[index].clone())
                .collect();
            let scan_request = LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![schema_key.clone()],
                    entity_pks: derived_request
                        .rows
                        .iter()
                        .map(|row| row.entity_pk.clone())
                        .collect(),
                    branch_ids: derived_request
                        .rows
                        .iter()
                        .map(|row| row.branch_id.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    untracked: derived_request.untracked,
                    include_tombstones: derived_request.include_tombstones,
                    ..Default::default()
                },
                projection: derived_request.projection.clone(),
                limit: None,
            };
            let rows = self.scan_derived_rows(&scan_request).await?;
            for (position, requested) in positions.into_iter().zip(derived_request.rows) {
                output[position] = rows
                    .iter()
                    .find_map(|row| exact_row_matches(row, &requested).then(|| row.clone()));
            }
        }

        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(output.len());
        let mut slots = Vec::with_capacity(output.len());
        for row in output {
            let Some(row) = row else {
                slots.push(None);
                continue;
            };
            let ordinal = u32::try_from(builder.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "validation exact result exceeds u32 rows",
                )
            })?;
            builder.push_owned(row);
            slots.push(Some(ordinal));
        }
        MaterializedLiveStateExactBatch::new(builder.finish(), slots)
    }
}

fn validation_schema_partition(request: &LiveStateScanRequest) -> (Vec<String>, Vec<String>) {
    if request.filter.schema_keys.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut current = Vec::new();
    let mut derived = Vec::new();
    for schema_key in &request.filter.schema_keys {
        if is_derived_schema(schema_key) {
            if !derived.iter().any(|candidate| candidate == schema_key) {
                derived.push(schema_key.clone());
            }
        } else if !current.iter().any(|candidate| candidate == schema_key) {
            current.push(schema_key.clone());
        }
    }
    (current, derived)
}

fn exact_row_matches(row: &MaterializedLiveStateRow, requested: &LiveStateExactRowRequest) -> bool {
    row.schema_key == requested.schema_key
        && row.branch_id.as_ref() == requested.branch_id.as_str()
        && row.entity_pk == requested.entity_pk
        && row.file_id == requested.file_id
}

#[cfg(test)]
mod transaction_validation_reader_tests {
    use super::*;

    #[test]
    fn mixed_validation_requests_partition_current_and_authenticated_derived_schemas() {
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    "app.entity".to_owned(),
                    "lix_commit".to_owned(),
                    "app.entity".to_owned(),
                    BRANCH_REF_SCHEMA_KEY.to_owned(),
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            validation_schema_partition(&request),
            (
                vec!["app.entity".to_owned()],
                vec!["lix_commit".to_owned(), BRANCH_REF_SCHEMA_KEY.to_owned()]
            )
        );
    }

    #[test]
    fn empty_schema_validation_request_stays_current_state_only() {
        let request = LiveStateScanRequest::default();
        assert_eq!(
            validation_schema_partition(&request),
            (Vec::new(), Vec::new())
        );
        assert!(!is_derived_schema("app.entity"));
    }

    #[test]
    fn exact_validation_identity_requires_schema_branch_entity_and_file() {
        let row = MaterializedLiveStateRow {
            entity_pk: EntityPk::single("entity"),
            schema_key: "app.entity".to_owned(),
            file_id: Some("file".to_owned()),
            snapshot_content: None,
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::expect_parse("created", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated", "2026-01-01T00:00:00Z"),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "branch".into(),
        };
        let requested = LiveStateExactRowRequest {
            schema_key: "app.entity".to_owned(),
            branch_id: "branch".to_owned(),
            entity_pk: EntityPk::single("entity"),
            file_id: Some("file".to_owned()),
        };
        assert!(exact_row_matches(&row, &requested));
        assert!(!exact_row_matches(
            &row,
            &LiveStateExactRowRequest {
                file_id: Some("other-file".to_owned()),
                ..requested
            }
        ));
    }

    #[test]
    fn validation_reader_has_no_legacy_current_fallback_field() {
        let source = include_str!("context.rs");
        assert!(!source.contains(concat!("self.", "current.scan_batch")));
        assert!(!source.contains(concat!("self.", "current.load_exact_batch")));
    }

    #[test]
    fn validation_reader_uses_only_the_operation_forktree_capability() {
        let source = include_str!("context.rs");
        let start = source
            .find("struct TransactionValidationLiveStateReader")
            .expect("validation reader definition");
        let end = source[start..]
            .find("fn validation_schema_partition")
            .map(|offset| start + offset)
            .expect("validation reader end");
        let reader = &source[start..end];
        assert!(reader.contains("forktree: ForkTreeReadFacade"));
        assert!(reader.contains("CommitGraphStoreReader<ForkTreeReadFacade"));
        assert!(reader.contains("scan_derived_rows"));
        assert!(!reader.contains("CommitGraphLiveStateReader"));
        assert!(!reader.contains("derived_validation_reader"));
        assert!(!reader.contains("branch_ctx.ref_reader"));
        assert!(!reader.contains("LiveStateStoreReader"));
    }

    #[test]
    fn transaction_validation_reuses_the_opening_forktree_capability() {
        let source = include_str!("context.rs");
        let validation_start = source
            .find("fn validation_live_state_reader(")
            .expect("validation facade factory");
        let validation_end = source[validation_start..]
            .find("/// Convenience helper")
            .map(|offset| validation_start + offset)
            .expect("validation facade factory end");
        let validation = &source[validation_start..validation_end];
        assert!(validation.contains("let forktree = self.forktree_read_facade()"));
        assert!(validation.contains("forktree: forktree.clone()"));
        assert!(validation.contains("reader(forktree)"));
        assert!(!validation.contains("ForkTreeReadFacade::new"));

        let facade_start = source
            .find("pub(crate) fn forktree_read_facade(")
            .expect("transaction ForkTree facade accessor");
        let facade_end = source[facade_start..]
            .find("async fn reconcile_stale_disjoint_writes")
            .map(|offset| facade_start + offset)
            .expect("transaction ForkTree facade accessor end");
        let facade = &source[facade_start..facade_end];
        assert!(facade.contains("self.opening_forktree.clone()"));
        assert!(!facade.contains("ForkTreeReadFacade::new"));
    }

    #[test]
    fn transaction_sql_reader_uses_the_operation_forktree_owner() {
        let source = include_str!("context.rs");
        let start = source
            .rfind("struct TransactionReadLiveStateReader")
            .expect("transaction read reader definition");
        let end = source[start..]
            .find("/// Erases the storage borrow lifetime")
            .map(|offset| start + offset)
            .expect("transaction read reader end");
        let reader = &source[start..end];
        assert!(reader.contains("forktree: ForkTreeReadFacade"));
        assert!(!reader.contains("transaction_reader("));
        assert!(reader.contains("overlay_scan_batch(&self.forktree"));
        assert!(reader.contains("overlay_load_exact_batch(&self.forktree"));
    }

    #[test]
    fn transaction_predecessor_exact_reads_use_the_operation_forktree_owner() {
        let source = include_str!("context.rs");
        let helper_start = source
            .find("async fn load_opening_exact_live_state_batch")
            .expect("opening exact-batch helper");
        let helper = &source[helper_start..];
        let helper_end = helper
            .find("fn diff_record_identity")
            .expect("opening exact-batch helper end");
        let helper = &helper[..helper_end];
        assert!(helper.contains("ForkTreeReadFacade<impl StorageAdapterRead + 'static>"));
        assert!(helper.contains("forktree.load_exact_batch(request)"));
        assert!(!helper.contains("transaction_reader("));

        let predecessor_start = source
            .find("async fn load_immutable_mutation_predecessors")
            .expect("mutation predecessor helper");
        let predecessor_end = source
            .find("fn conflict_resolution_limits")
            .expect("mutation predecessor helper end");
        let predecessor = &source[predecessor_start..predecessor_end];
        assert!(predecessor.contains("ForkTreeReadFacade<impl StorageAdapterRead + 'static>"));
        assert!(predecessor.contains("reader.load_exact_batch(&request)"));
        assert!(!predecessor.contains("transaction_reader("));
    }

    #[test]
    fn staged_transaction_materialization_uses_the_same_forktree_owner() {
        let source = include_str!("context.rs");
        let scan_start = source
            .find("async fn scan_visible_live_state_batch")
            .expect("staged scan helper");
        let scan_end = source[scan_start..]
            .find("async fn visible_materialization")
            .map(|offset| scan_start + offset)
            .expect("staged scan helper end");
        let scan = &source[scan_start..scan_end];
        assert!(scan.contains("let forktree = self.forktree_read_facade()"));
        assert!(scan.contains("overlay_scan_batch(&forktree"));
        assert!(!scan.contains("transaction_reader("));

        let exact_start = source
            .find("async fn load_visible_exact_live_state_batch")
            .expect("staged exact helper");
        let exact_end = source[exact_start..]
            .find("/// Drops `format-only`")
            .map(|offset| exact_start + offset)
            .expect("staged exact helper end");
        let exact = &source[exact_start..exact_end];
        assert!(exact.contains("let forktree = self.forktree_read_facade()"));
        assert!(exact.contains("overlay_load_exact_batch(&forktree"));
        assert!(!exact.contains("transaction_reader("));
        assert!(!exact.contains("begin_read("));
    }

    #[test]
    fn collection_generation_uses_the_retained_forktree_owner() {
        let source = include_str!("context.rs");
        let resolver_start = source
            .find("async fn resolve_prepared_mutation_collection_generation")
            .expect("collection-generation resolver");
        let resolver_end = source[resolver_start..]
            .find("async fn load_opening_exact_live_state_batch")
            .map(|offset| resolver_start + offset)
            .expect("collection-generation resolver end");
        let resolver = &source[resolver_start..resolver_end];
        assert!(resolver.contains("ForkTreeReadFacade<impl StorageAdapterRead + 'static>"));
        assert!(resolver.contains("forktree\n        .collection_generation("));
        assert!(!resolver.contains("transaction_reader("));
        assert!(!resolver.contains("LiveStateContext"));

        let loader_start = source
            .rfind("async fn load_collection_generation(")
            .expect("transaction collection-generation loader");
        let loader_end = source[loader_start..]
            .find("async fn load_exact_collection_live_count")
            .map(|offset| loader_start + offset)
            .expect("transaction collection-generation loader end");
        let loader = &source[loader_start..loader_end];
        assert!(loader.contains("let forktree = self.forktree_read_facade()"));
        assert!(loader.contains("forktree.collection_generation("));
        assert!(!loader.contains(concat!("live_state.", "reader(")));

        let reader_source = include_str!("../live_state/forktree_reader.rs");
        let reader_start = reader_source
            .find("async fn collection_generation(")
            .expect("ForkTree collection-generation reader");
        let reader_end = reader_source[reader_start..]
            .find("async fn load_exact_view")
            .map(|offset| reader_start + offset)
            .expect("ForkTree collection-generation reader end");
        let reader = &reader_source[reader_start..reader_end];
        assert!(reader.contains("scan_facade("));
        assert!(reader.contains("authenticated_ordered_generation_digest("));
        assert!(reader.contains("row.commit_id() != Some(active_generation)"));
        assert!(reader.contains("include_tombstones: true"));
        assert!(!reader.contains(concat!("LiveState", "StoreReader")));
        assert!(resolver.contains("missing its ordered identity digest"));
        assert!(!resolver.contains(".and_then(|generation|"));
    }
}

struct TransactionReadLiveStateReader<R: crate::storage_adapter::StorageRead> {
    forktree: ForkTreeReadFacade<SharedStorageAdapterRead<R>>,
    read_store: SharedStorageAdapterRead<R>,
    staged: PreparedStateRowOverlay,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
}

#[async_trait]
impl<R> LiveStateReader for TransactionReadLiveStateReader<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        overlay_scan_batch(&self.forktree, &self.staged, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        overlay_load_exact_batch(&self.forktree, &self.staged, request).await
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
            let mut index = crate::filesystem::build_path_index(&self.forktree, request).await?;
            if request.cache_small_blob_data {
                index = Arc::new(
                    (*index)
                        .clone()
                        .hydrate_small_blob_data(&self.forktree)
                        .await?,
                );
            }
            return Ok(index);
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
            overlay_scan_batch(&self.forktree, &self.staged, &request.live_state_request()).await?;
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
    if row.entity_pk.is_none() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "normalized transaction write row is missing entity_pk",
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

fn assign_certified_tracked_change_ids(
    prepared: &mut PreparedStateBatch,
    functions: &FunctionProviderHandle,
) {
    for index in 0..prepared.len() {
        if prepared.row(index).change_id == Some(ChangeId::default()) {
            prepared.set_change_id(index, Some(ChangeId::from(functions.call_uuid_v7())));
        }
    }
}

#[cfg(test)]
mod certified_change_id_tests {
    use super::*;

    fn prepared_rows() -> PreparedStateBatch {
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(902),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        };
        CertifiedParameterReplacementBatch::new(
            vec![
                EntityPk::single("row-a"),
                EntityPk::single("row-b"),
                EntityPk::single("row-c"),
            ],
            ["a", "b", "c"]
                .into_iter()
                .map(|value| {
                    TransactionJson::from_certified_shared_normalized_row_content(
                        format!(r#"{{"value":"{value}"}}"#).into(),
                    )
                })
                .collect(),
            "catalog_test".into(),
            "main".into(),
            certificate,
        )
        .expect("certified rows should construct")
        .into_dense_prepared(
            None,
            LixTimestamp::expect_parse("timestamp", "2026-08-08T00:00:00.000Z"),
        )
        .expect("certified rows should prepare")
    }

    #[test]
    fn certified_tracked_rows_get_distinct_ids_once_in_input_order() {
        let functions = FunctionProviderHandle::system();
        let mut prepared = prepared_rows();
        assert!(
            prepared
                .iter()
                .all(|row| row.change_id == Some(ChangeId::default()))
        );

        assign_certified_tracked_change_ids(&mut prepared, &functions);
        let first_ids = prepared
            .iter()
            .map(|row| row.change_id.expect("assigned change id"))
            .collect::<Vec<_>>();
        assert!(
            first_ids
                .iter()
                .all(|change_id| *change_id != ChangeId::default())
        );
        assert_eq!(
            first_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            first_ids.len()
        );
        assert_eq!(
            prepared
                .iter()
                .map(|row| row.entity_pk.as_single_string().expect("single key"))
                .collect::<Vec<_>>(),
            vec!["row-a", "row-b", "row-c"]
        );

        // Retry/idempotency: the authoritative staging boundary assigns only
        // placeholders, so a repeated call cannot regenerate published IDs.
        assign_certified_tracked_change_ids(&mut prepared, &functions);
        assert_eq!(
            prepared
                .iter()
                .map(|row| row.change_id.expect("assigned change id"))
                .collect::<Vec<_>>(),
            first_ids
        );
    }

    #[test]
    fn certified_tracked_rows_preserve_supplied_ids_at_common_boundary() {
        let functions = FunctionProviderHandle::system();
        let supplied = ChangeId::for_test_label("supplied");
        let mut prepared = prepared_rows();
        prepared.set_change_id(1, Some(supplied));

        assign_certified_tracked_change_ids(&mut prepared, &functions);

        assert_eq!(prepared.row(1).change_id, Some(supplied));
        assert!(
            prepared
                .iter()
                .all(|row| row.change_id.is_some_and(|id| id != ChangeId::default()))
        );
        assert_eq!(
            prepared
                .iter()
                .map(|row| row.change_id.expect("assigned change id"))
                .collect::<BTreeSet<_>>()
                .len(),
            prepared.len()
        );
    }
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
    prepared.push_parts_with_change_addressability(
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
        scalar.addressable_change_id,
        scalar.commit_id,
        untracked,
        branch_id,
    );
    prepared.set_durable_predecessor(prepared.len() - 1, durable_predecessor);
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
        || !prepared_writes.branch_ref_intents.is_empty()
}

fn prepared_writes_require_filesystem_index_rebuild(prepared_writes: &PreparedWriteSet) -> bool {
    prepared_writes.state_rows.iter().any(|row| {
        row.schema_key == BRANCH_REF_SCHEMA_KEY
            || (row.addressable_change_id
                && matches!(
                    row.schema_key.as_str(),
                    "lix_file_descriptor" | "lix_directory_descriptor" | BLOB_REF_SCHEMA_KEY
                ))
    }) || prepared_writes
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

pub(crate) struct OpenTransaction<StorageImpl: Storage + 'static = Memory> {
    pub(crate) transaction: Transaction<StorageImpl>,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction<StorageImpl>(
    mode: &SessionMode,
    active_account_id: String,
    storage: StorageAdapter<StorageImpl>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
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
        mode,
        active_account_id,
        storage,
        live_state,
        tracked_state,
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
    mode: &SessionMode,
    active_account_id: String,
    storage: StorageAdapter<StorageImpl>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
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
        mode,
        active_account_id,
        storage,
        live_state,
        tracked_state,
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

    fn authenticated_blob_reader(
        &self,
    ) -> Result<Arc<dyn crate::forktree::AuthenticatedBlobReader>, LixError> {
        Ok(Arc::new(crate::forktree::blob_reader_on_read(
            self.opening_read(),
            &self.active_branch_id,
        )?))
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
        let read = self.opening_read();
        let descriptor_epoch = self.filesystem_path_index_epoch.load(Ordering::SeqCst);
        if descriptor_epoch == 0 {
            let staged = self.staged_writes.staging_overlay()?;
            let forktree = self.forktree_read_facade();
            let rows =
                overlay_scan_batch(&forktree, &staged, &request.live_state_request()).await?;
            let mut index = Arc::new(FilesystemPathIndex::from_live_batch(&rows)?);
            if request.cache_small_blob_data {
                index = Arc::new((*index).clone().hydrate_small_blob_data(&forktree).await?);
            }
            return Ok(index);
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
        let forktree = self.forktree_read_facade();
        let rows = overlay_scan_batch(&forktree, &staged, &request.live_state_request()).await?;
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
        let forktree = self.forktree_read_facade();
        forktree.collection_generation(branch_id, scope).await
    }

    async fn load_exact_collection_live_count(
        &mut self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<u64>, LixError> {
        if let Some(generation) = self.load_collection_generation(branch_id, scope).await?
            && generation.live_count != crate::collection_generation::DEFERRED_LIVE_COUNT
        {
            return Ok(Some(generation.live_count));
        }
        let file_ids = scope
            .file_id
            .map(|file_id| vec![NullableKeyFilter::Value(file_id.to_owned())])
            .unwrap_or_default();
        let rows = self
            .scan_visible_live_state_batch(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![scope.schema_key.to_owned()],
                    branch_ids: vec![branch_id.to_owned()],
                    file_ids,
                    include_tombstones: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        Ok(Some(rows.len() as u64))
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

    async fn stage_branch_ref_intent(
        &mut self,
        branch_id: &str,
        commit_id: Option<CommitId>,
        create: bool,
    ) -> Result<(), LixError> {
        self.stage_branch_ref_intent(branch_id, commit_id, create)
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
        let staged = self.staged_writes.staging_overlay()?;
        if StagedLiveStateRows::collection_replaced(&staged, &branch_id, &schema_key, None)? {
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
        let tracked_certified_rows = !rows.untracked();
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
            rows.into_dense_prepared(self.origin_key.as_ref(), self.functions.call_timestamp())?
        };
        let mut prepared = prepared;
        if tracked_certified_rows {
            assign_certified_tracked_change_ids(&mut prepared, &self.functions);
        }
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
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(
                row_count * usize::from(rows.untracked()),
            );
        }
        let tracked_certified_rows = !rows.untracked();
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
            rows.into_dense_prepared(self.origin_key.as_ref(), self.functions.call_timestamp())?
        };
        let mut prepared = prepared;
        if tracked_certified_rows {
            assign_certified_tracked_change_ids(&mut prepared, &self.functions);
        }
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
        if opening_parent_complete_lifecycle_created_at(
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
        .is_none()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed mutation journal lacks complete parent lifecycle authority",
            ));
        }

        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(row_count);
            crate::storage_bench::record_transaction_untracked_rows(0);
        }
        let expected_ordered_identity_digest = rows.expected_ordered_identity_digest;
        let schema_key = rows.schema_key.clone();
        let branch_id = rows.branch_id.clone();
        let chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
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
        if self.origin_key.is_some()
            || self
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

fn prepared_transaction_write_filesystem_index_impact(
    write: &PreparedTransactionWrite,
) -> (bool, Vec<MaterializedLiveStateRow>) {
    let mut affects_index = false;
    let mut delta_rows = Vec::new();
    for row in prepared_transaction_write_rows(write).iter() {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY => {
                affects_index = true;
                delta_rows.push(MaterializedLiveStateRow::from(row));
            }
            BRANCH_REF_SCHEMA_KEY => affects_index = true,
            _ => {}
        }
    }
    (affects_index, delta_rows)
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
        path: write.path.clone(),
        plugin: WasmPluginSelection {
            plugin_key: plugin.key().to_string(),
            generation: plugin.archive_blob_hash().to_string(),
        },
    }
}

fn v2_file_descriptor_from_actor_key(key: &PluginActorKey) -> WasmFileDescriptor {
    WasmFileDescriptor {
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
fn v2_create_context(seed: [u8; 16], actor_key: &PluginActorKey) -> crate::wasm::WasmCreateContext {
    BoundCreateContext::bind(local_mutation_identity(seed), actor_key)
        .expect("local mutation seeds are generated as UUIDv7")
        .creates()
}

fn suppress_format_only_noops_against_batch(
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
                            "validated component guest changes must own parsed canonical snapshots",
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
                        == canonicalize_snapshot(base_snapshot.as_bytes())?.as_slice()
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

fn append_plugin_change_rows(
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
                            "validated component guest changes must own parsed canonical snapshots",
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

fn append_certified_entity_changes(
    changes: &mut WasmHostEntityChanges,
    batches: &[WasmCertifiedEntityBatch],
    schemas: &crate::plugin::SchemaAllowlist,
) -> Result<(), LixError> {
    for batch in batches {
        changes
            .changes
            .extend(crate::plugin::materialize_certified_entity_batch(batch, schemas)?.changes);
    }
    changes.validate()
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
            format!("component plugin emitted invalid entity_pk: {error}"),
        )
    })
}

fn plugin_schema_is_creatable(plugin: &PluginRegistryEntry, schema_key: &str) -> bool {
    plugin
        .create_schema_keys()
        .binary_search_by(|candidate| candidate.as_str().cmp(schema_key))
        .is_ok()
}

fn plugin_entity_authorities_after_changes(
    plugin: &PluginRegistryEntry,
    base: &PluginEntityAuthorities,
    changes: &WasmHostEntityChanges,
) -> PluginEntityAuthorities {
    let mut inserted = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for change in &changes.changes {
        match change {
            WasmEntityChange::Upsert { entity, .. }
                if plugin_schema_is_creatable(plugin, &entity.key.schema_key) =>
            {
                removed.remove(&entity.key);
                inserted.insert(entity.key.clone());
            }
            WasmEntityChange::Delete(key)
                if plugin_schema_is_creatable(plugin, &key.schema_key) =>
            {
                inserted.remove(key);
                removed.insert(key.clone());
            }
            WasmEntityChange::Create { .. }
            | WasmEntityChange::Upsert { .. }
            | WasmEntityChange::Delete(_) => {}
        }
    }
    base.with_delta(inserted, removed)
}

fn plugin_entity_authorities_from_transition(
    plugin: &PluginRegistryEntry,
    changes: &WasmHostEntityChanges,
    certified_batches: &[WasmCertifiedEntityBatch],
) -> Result<PluginEntityAuthorities, LixError> {
    plugin_entity_authorities_after_transition(
        plugin,
        &PluginEntityAuthorities::empty(),
        changes,
        certified_batches,
    )
}

fn plugin_entity_authorities_after_transition(
    plugin: &PluginRegistryEntry,
    prior: &PluginEntityAuthorities,
    changes: &WasmHostEntityChanges,
    certified_batches: &[WasmCertifiedEntityBatch],
) -> Result<PluginEntityAuthorities, LixError> {
    let empty = PluginEntityAuthorities::empty();
    let base = if certified_batches
        .iter()
        .any(|batch| batch.complete_file_state)
    {
        &empty
    } else {
        prior
    };
    let base = plugin_entity_authorities_after_changes(plugin, base, changes);
    let mut ranges = Vec::new();
    for batch in certified_batches {
        for range in &batch.create_ranges {
            if !plugin_schema_is_creatable(plugin, &range.schema_key) {
                continue;
            }
            ranges.push(PluginEntityAuthorityRange::new(
                range.schema_key.clone(),
                batch.creates,
                range.first_local_ref,
                range.last_local_ref,
            ));
        }
    }
    Ok(base.with_ranges(ranges))
}

fn plugin_entity_authorities_from_live_batch(
    plugin: &PluginRegistryEntry,
    rows: &MaterializedLiveStateBatch,
    ordinals: &[u32],
) -> PluginEntityAuthorities {
    PluginEntityAuthorities::from_keys(
        ordinals
            .iter()
            .filter_map(|ordinal| {
                let row = rows.row(*ordinal as usize);
                plugin_schema_is_creatable(plugin, row.schema_key()).then(|| {
                    WasmEntityKey::from_owned_parts(
                        row.schema_key().to_owned(),
                        row.entity_pk().clone().into_parts(),
                    )
                })
            })
            .collect(),
    )
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
                "durable component entity hydration returned duplicate keys",
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
                "durable component entity hydration returned duplicate keys",
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
                    "component semantic rendering requires tracked, branch-local, file-scoped rows",
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
                "one component semantic write batch cannot contain the same entity key more than once",
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
    existing_create_reservation: Option<MaterializedLiveStateRow>,
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
    existing_create_reservation: Option<MaterializedLiveStateRow>,
    store_permit: PluginActorStorePermit,
    task: Option<
        tokio::task::JoinHandle<
            Result<(Box<dyn WasmComponentActor>, ValidatedFileTransition), LixError>,
        >,
    >,
}

#[derive(Clone)]
struct DecodedDurablePluginCheckpoint {
    generation: String,
    semantic_root: String,
    runtime: crate::Blob,
    authorities: PluginEntityAuthorities,
}

impl fmt::Debug for DecodedDurablePluginCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedDurablePluginCheckpoint")
            .field("generation", &self.generation)
            .field("semantic_root", &self.semantic_root)
            .field("runtime_bytes", &self.runtime.len())
            .finish_non_exhaustive()
    }
}

impl PluginWriteReconciliation {
    fn attach_durable_checkpoints(
        &self,
        file_content: &mut [TransactionFileContent],
    ) -> Result<(), LixError> {
        let mut checkpoints = BTreeMap::<
            (String, String),
            (WasmDurableDocumentCheckpoint, crate::Blob, String, String),
        >::new();
        for publication in &self.actor_publications {
            let Some((branch_id, file_id, generation, semantic_root, checkpoint, authorities)) =
                publication.durable_checkpoint()
            else {
                continue;
            };
            let Some(authority) = authorities
                .encode_checkpoint_bounded(WasmDurableDocumentCheckpoint::MAX_DECODED_BYTES)
            else {
                continue;
            };
            checkpoints.insert(
                (branch_id.to_owned(), file_id.to_owned()),
                (
                    checkpoint,
                    authority.into(),
                    generation.to_owned(),
                    semantic_root.to_string(),
                ),
            );
        }
        for ((branch_id, file_id), (checkpoint, authority, generation, semantic_root)) in
            checkpoints
        {
            let write = file_content
                .iter_mut()
                .find(|write| write.branch_id == branch_id && write.file_id == file_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "plugin checkpoint payload for file '{file_id}' has no materialized file owner"
                        ),
                    )
                })?;
            write.set_plugin_checkpoint(generation, semantic_root, checkpoint.bytes(), authority);
        }
        Ok(())
    }

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
        entity_authorities: PluginEntityAuthorities,
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
        semantic_root: Option<Arc<str>>,
        durable_checkpoint: Option<WasmDurableDocumentCheckpoint>,
        entity_authorities: PluginEntityAuthorities,
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
                let successor_checkpoint = lease.successor_checkpoint();
                let durable_checkpoint = successor_checkpoint
                    .as_ref()
                    .and_then(|(_, _, checkpoint)| checkpoint.as_ref())
                    .and_then(|checkpoint| checkpoint.durable_checkpoint());
                let semantic_root = successor_checkpoint
                    .as_ref()
                    .map(|(_, semantic_root, _)| Arc::clone(semantic_root));
                let entity_authorities = lease
                    .successor_entity_authorities()
                    .cloned()
                    .unwrap_or_else(PluginEntityAuthorities::empty);
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
                    semantic_root,
                    durable_checkpoint,
                    entity_authorities,
                }
            }
            Self::New {
                cache,
                mut store,
                key,
                document,
                checkpoint,
                semantic_root,
                entity_authorities,
                view,
                ..
            } => {
                let durable_checkpoint = checkpoint
                    .as_ref()
                    .and_then(WasmDocumentCheckpoint::durable_checkpoint);
                let staged_checkpoint =
                    cache.stage_checkpoint(key.clone(), Arc::clone(&semantic_root), checkpoint);
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
                Self::Uncached {
                    key,
                    view,
                    checkpoint: staged_checkpoint,
                    semantic_root: Some(semantic_root),
                    durable_checkpoint,
                    entity_authorities,
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
                entity_authorities,
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
                        entity_authorities,
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

    fn durable_checkpoint(
        &self,
    ) -> Option<(
        &str,
        &str,
        &str,
        Arc<str>,
        WasmDurableDocumentCheckpoint,
        &PluginEntityAuthorities,
    )> {
        match self {
            Self::Existing {
                lease,
                successor_key,
                ..
            } => {
                let (_, semantic_root, checkpoint) = lease.successor_checkpoint()?;
                checkpoint
                    .and_then(|checkpoint| checkpoint.durable_checkpoint())
                    .zip(lease.successor_entity_authorities())
                    .map(|(bytes, authorities)| {
                        (
                            successor_key.branch_id.as_str(),
                            successor_key.file_id.as_str(),
                            successor_key.plugin_generation.as_str(),
                            semantic_root,
                            bytes,
                            authorities,
                        )
                    })
            }
            Self::New {
                key,
                checkpoint,
                semantic_root,
                entity_authorities,
                ..
            } => checkpoint
                .as_ref()
                .and_then(WasmDocumentCheckpoint::durable_checkpoint)
                .map(|bytes| {
                    (
                        key.branch_id.as_str(),
                        key.file_id.as_str(),
                        key.plugin_generation.as_str(),
                        Arc::clone(semantic_root),
                        bytes,
                        entity_authorities,
                    )
                }),
            Self::Uncached {
                key,
                semantic_root,
                durable_checkpoint,
                entity_authorities,
                ..
            } => durable_checkpoint.clone().zip(semantic_root.clone()).map(
                |(bytes, semantic_root)| {
                    (
                        key.branch_id.as_str(),
                        key.file_id.as_str(),
                        key.plugin_generation.as_str(),
                        semantic_root,
                        bytes,
                        entity_authorities,
                    )
                },
            ),
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
    base_blob_hash: BlobId,
    rendered_bytes: crate::Blob,
    same_length_output_splice: Option<ValidatedSameLengthOutputSplice>,
) -> TransactionFileContent {
    let mut rendered_file = TransactionFileContent::new(
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

async fn render_semantic_changes_with_lease(
    mut lease: PluginActorLease,
    plugin: &PluginRegistryEntry,
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
    let successor_entity_authorities =
        plugin_entity_authorities_after_changes(plugin, call.entity_authorities(), &changes);
    let change_source = match VecEntityChangeSource::new(changes, limits) {
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
    if let Err(error) = lease.set_successor_entity_authorities(successor_entity_authorities) {
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
    #[serde(default)]
    plugin_checkpoint: Option<DurableBlobCheckpointSnapshot>,
}

#[derive(Debug, serde::Deserialize)]
struct DurableBlobCheckpointSnapshot {
    generation: String,
    semantic_root: String,
    runtime: String,
    authority: String,
}

async fn preflight_owned_generation_upgrades<R>(
    host: &PluginRuntimeHost,
    base: &dyn LiveStateReader,
    staged: &impl StagedLiveStateRows,
    read: &SharedStorageAdapterRead<R>,
    staged_writes: &TransactionWriteBuffer,
    upgrades: &[PluginGenerationUpgrade],
    install_wasm: &BTreeMap<BlobId, Vec<u8>>,
    install_schema_definitions: &BTreeMap<PluginLifecycleKey, BTreeMap<String, JsonValue>>,
) -> Result<(), LixError>
where
    R: crate::storage_adapter::StorageRead,
{
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

        let requests = owners
            .iter()
            .map(|owner| {
                materialized_hash_by_file
                    .get(owner.file_id())
                    .copied()
                    .map(|hash| (plugin_materialization_state_key(owner.file_id()), hash))
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
        let materialized_bytes = load_transaction_authenticated_plugin_bytes(
            read,
            &upgrade.branch_id,
            staged_writes,
            &BTreeMap::new(),
            &requests,
        )
        .await?
        .into_vec();
        if materialized_bytes.len() != owners.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin upgrade materialized blob batch length mismatch",
            ));
        }

        let wasm_hash = BlobId::from_hex(upgrade.replacement.wasm_blob_hash())?;
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
            let verified = preflight_rendered_file(
                store.actor_mut(),
                WasmFileDescriptor {
                    path: Some(entry.path.clone()),
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

async fn preflight_rendered_file(
    actor: &mut dyn WasmComponentActor,
    descriptor: WasmFileDescriptor,
    entities: Vec<WasmHostEntity>,
    expected: crate::Blob,
    limits: WasmTransitionLimits,
) -> Result<(), LixError> {
    let source = VecEntitySource::new(entities, limits)?;
    let accepted = Arc::new(ArcByteSource::new(expected.clone()));
    let transition = actor
        .open_entities(
            limits,
            WasmOpenEntitiesInput {
                descriptor,
                entities: Box::new(source),
                accepted: Some(accepted),
            },
        )
        .await?;
    let validated = drain_entity_transition_edits(
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

async fn resolve_active_branch_id(
    mode: &SessionMode,
    _live_state: &LiveStateContext,
    branch_ctx: &BranchContext,
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { branch_id } => branch_id
            .read()
            .map(|branch_id| branch_id.clone())
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "session branch selector is poisoned",
                )
            }),
        SessionMode::Workspace { branch_id } => {
            let branch_id = branch_id
                .read()
                .map(|branch_id| branch_id.clone())
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "workspace branch selector cache is poisoned",
                    )
                })?;
            let branch_ref = branch_ctx.ref_reader(read);
            BranchLifecycle::new(&branch_ref)
                .require_existing_ref(
                    &branch_id,
                    BranchOperation::LoadWorkspaceSelector,
                    BranchReferenceRole::WorkspaceSelector,
                )
                .await?;
            Ok(branch_id.clone())
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
