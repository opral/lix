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
use bytes::Bytes;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BlobBytesBatch, BlobId};
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchLifecycle, BranchOperation, BranchRefReader, BranchRefStoreReader,
    BranchReferenceRole,
};
use crate::catalog::{
    CatalogContext, CatalogFingerprint, CatalogSnapshot, SchemaPlanId, load_catalog_revision,
    stage_catalog_revision,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, CommitId, load_change_records,
    materialize_known_change_payloads,
};
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_marker_stage_row};
use crate::commit_graph::CommitGraphStoreReader;
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemPathKind,
};
use crate::forktree::{
    AuthenticatedBlobReader, CanonicalUploadId, ForkTreeReadFacade, HistoricalStateRow,
    PreparedPublication, StateCell, StateKey, StateKeyRef, UploadBindingRef, encode_state_key,
    prepare_upload_part,
};
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::gc::{CheckpointPublication, CheckpointRecoveryRef, load_checkpoint_publication_state};
use crate::plugin::{
    ArcByteSource, ConflictRank, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginActorCache,
    PluginActorKey, PluginActorLease, PluginActorStagedCheckpoint, PluginActorStore,
    PluginActorStorePermit, PluginEntityAuthorities, PluginEntityAuthorityRange, PluginFileOwner,
    PluginObservation, PluginRegistry, PluginRegistryEntry, PluginRuntimeHost,
    ValidatedConflictTransition, ValidatedSameLengthOutputSplice, VecEntityChangeSource,
    VecEntityConflictSource, drain_conflict_transition_resolutions, drain_entity_transition_edits,
    is_plugin_storage_path, is_reservation_key, plugin_archive_file_id_matches,
    plugin_key_from_archive_delete_origin, plugin_storage_wasm_file_id,
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
use crate::state::{CertifiedStatePredecessor, ForkTreeStateView, TransactionStateView};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    Memory, StoragePrecondition, StorageReadOptions, StorageWriteOptions, StorageWriteSetStats,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
};
use crate::transaction::commit;
use crate::transaction::normalization::{
    NormalizedRowFacts, REGISTERED_SCHEMA_KEY, normalize_raw_write_row_in_place,
    remember_pending_registered_schema,
};
use crate::transaction::schema_resolver::TransactionSchemaResolver;
use crate::transaction::staging::{
    BranchRefPublicationIntent, ImmutableMutationChunkStage, ImmutableMutationJournalChunk,
    PreparedWriteSet, TransactionWriteBuffer, TransactionWriteBufferCheckpoint,
};
use crate::transaction::stale_commit::{
    StaleCommitPlan, StalePluginReconciliationPlan, classify_stale_commit,
};
#[cfg(test)]
use crate::transaction::types::CertifiedRawWriteBatchPreparation;
use crate::transaction::types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch, PreparedRowFacts,
    PreparedStateBatch, PreparedTransactionWrite, RawWriteBatch, RawWriteRowRef,
    StagedCommitChangeBatch, StagedCommitChangeBatchBuilder, TransactionFileContent,
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
    TransactionWriteRow, TypedMutationJournalBatch, canonicalize_transaction_json_batch,
    stage_json_from_value,
};
use crate::transaction::validation::validate_prepared_writes_by_branch;

use crate::wasm::{
    WasmCertifiedEntityBatch, WasmChangeEffect, WasmConflictResolution, WasmConflictTake,
    WasmConflictUpdate, WasmDocumentCheckpoint, WasmDocumentHandle, WasmDurableDocumentCheckpoint,
    WasmEntityChange, WasmEntityConflict, WasmEntityKey, WasmEntityUpdate, WasmFileDescriptor,
    WasmHostBytes, WasmHostEntityChanges, WasmPluginSelection, WasmTransitionLimits,
};
use crate::{LixError, SqlQueryResult, Value};

/// # Safety
///
/// The transaction retains the storage value that created `read` and drops
/// the widened read before that storage value. This keeps the concrete native
/// retained-read boundary valid without introducing a second reader owner.
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
    key: StateKey,
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

fn state_key_from_tracked(key: &StateKey) -> StateKey {
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

fn historical_row_matches_key(row: &HistoricalStateRow, key: &StateKey) -> bool {
    row.key.schema_key == key.schema_key
        && row.key.file_id == key.file_id
        && row.key.entity_pk == key.entity_pk
}

async fn load_historical_rows_at_commit<R>(
    facade: &ForkTreeReadFacade<R>,
    commit_id: CommitId,
    keys: &[StateKey],
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
    plugin_host: PluginRuntimeHost,
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
    state_view: TransactionStateView<SharedStorageAdapterRead<StorageImpl::Read<'static>>>,
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
    identity: StateKey,
    expected_change_id: Option<ChangeId>,
    target: Option<TypedStateTransitionTarget>,
}

struct TypedStateTransitionTarget {
    change_id: ChangeId,
    snapshot_content: Option<SharedStr>,
    metadata: Option<SharedStr>,
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
        let row = entry.blob_ref_state_row().ok_or_else(|| {
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
        let expected = BlobId::from_canonical_content(content).to_hex();
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

    /// Creates the opaque ForkTree operation owner from this transaction's
    /// already-retained opening read. This does not acquire or refresh a
    /// storage read and does not expose the underlying handle.
    pub(crate) fn forktree_read_facade(
        &self,
    ) -> ForkTreeReadFacade<SharedStorageAdapterRead<StorageImpl::Read<'static>>> {
        ForkTreeReadFacade::new(self.opening_read())
    }

    /// Exposes the transaction's committed authenticated view without
    /// acquiring another storage read. Staged rows are added by the
    /// transaction overlay owner before a consumer asks for the combined
    /// `TransactionStateView`.
    pub(crate) async fn committed_state_view(
        &mut self,
    ) -> Result<ForkTreeStateView<SharedStorageAdapterRead<StorageImpl::Read<'static>>>, LixError>
    {
        let read = self.opening_read();
        let branch_id = self.active_branch_id.clone();
        ForkTreeStateView::from_facade(ForkTreeReadFacade::new(read), &branch_id).await
    }

    /// Refreshes the native transaction read overlay from the same live write
    /// buffer that commit will drain. The committed retained view is reused;
    /// this never opens a statement-local read or a second authority.
    fn refresh_state_view_from_staging(&mut self, bump_epoch: bool) -> Result<(), LixError> {
        let (staged, staged_untracked) = self
            .staged_writes
            .state_overlay_rows(&self.active_branch_id)?;
        self.state_view = self.state_view.with_staged_rows(staged, staged_untracked)?;
        if bump_epoch {
            self.filesystem_path_index_epoch
                .fetch_add(1, Ordering::SeqCst);
            self.filesystem_path_index_cache.clear();
        }
        Ok(())
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

        let branch_reader = BranchRefStoreReader::new(read);
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
            let forktree = self.forktree_read_facade();
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

        let facade = self.forktree_read_facade();
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
            .map(|row| StateKey {
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
        concurrent_keys.extend(concurrent_identity.into_iter().map(|change| StateKey {
            schema_key: change.key.schema_key,
            file_id: change.key.file_id,
            entity_pk: change.key.entity_pk,
        }));
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
                concurrent_keys.iter().map(|key| StateKeyRef {
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
            StaleCommitPlan::Direct => {}
            StaleCommitPlan::RevalidateOrdinaryInsert => {
                // The concurrent branch head is now the authoritative
                // validation snapshot.  Reuse the commit-boundary read that
                // produced the stale classification; do not reopen storage
                // or silently accept the INSERT race.
                let committed = self.committed_state_view().await?;
                let current_state = TransactionStateView::new(committed, Vec::new())?;
                validate_prepared_writes_by_branch(
                    &current_state,
                    &self.active_branch_id,
                    &self.sql_schema_snapshot,
                    prepared_writes,
                    self.trust_filesystem_planner,
                )
                .await?;
            }
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
            .map(|file_id| StateKey {
                schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
                file_id: Some(file_id.clone()),
                entity_pk: EntityPk::single(PLUGIN_OWNER_KEY),
            })
            .collect::<Vec<_>>();
        let registry_key = StateKey {
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
                StateKey {
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
        plugin_host: PluginRuntimeHost,
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
            let active_branch_id = resolve_active_branch_id(mode, &read).await?;
            let runtime_functions = FunctionContext::prepare(&read).await?;
            let runtime_boundary_result = runtime_boundary(&runtime_functions).await?;
            let functions = runtime_functions.provider();
            let opening_state_view = ForkTreeStateView::from_facade(
                ForkTreeReadFacade::new(read.clone()),
                &active_branch_id,
            )
            .await?;
            let opening_transaction_state_view =
                TransactionStateView::new(opening_state_view.clone(), Vec::new())?;
            let (sql_schema_catalog, tracked_schema_catalog) = {
                let catalog_revision = load_catalog_revision(&read).await?;
                let sql_schema_catalog = catalog_context
                    .compiled_catalog_for_transaction_open(
                        &opening_transaction_state_view,
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
                        &opening_transaction_state_view,
                        &Domain::schema_catalog(active_branch_id.clone(), false),
                        catalog_revision.as_ref(),
                    )
                    .await?;
                (sql_schema_catalog, tracked_schema_catalog)
            };
            let opening_selector_fence =
                load_forktree_selector_fence(&read, &active_branch_id).await?;
            let branch_reader = BranchRefStoreReader::new(&read);
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
                opening_transaction_state_view,
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
            opening_state_view,
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
            tracked_schema_catalog,
        );
        let staged_writes = Arc::new(TransactionWriteBuffer::new(functions.clone()));
        let state_view = opening_state_view;
        Ok((
            OpenTransaction {
                transaction: Self {
                    active_branch_id,
                    active_account_id,
                    plugin_host,
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
                    state_view,
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
    /// facts, branch-ref updates, and visible state rows before the
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
        let validation_state = match transaction.committed_state_view().await {
            Ok(committed) => match TransactionStateView::new(committed, Vec::new()) {
                Ok(state) => state,
                Err(error) => {
                    transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                    return Err(error);
                }
            },
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = validate_prepared_writes_by_branch(
            &validation_state,
            &transaction.active_branch_id,
            &transaction.sql_schema_snapshot,
            &prepared_writes,
            transaction.trust_filesystem_planner,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_native_validation"
        ))
        .await
        {
            transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return Err(error);
        }
        let commit_parent_heads =
            match commit::resolve_prepared_commit_parent_heads(&read, &prepared_writes, true).await
            {
                Ok(commit_parent_heads) => commit_parent_heads,
                Err(error) => {
                    transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                    return Err(error);
                }
            };
        let rebuild_filesystem_path_index =
            prepared_writes_require_filesystem_index_rebuild(&prepared_writes);
        let prepared_forktree_plan = match commit::prepare_forktree_publication_with_parent_heads(
            &transaction.active_account_id,
            &commit_parent_heads,
            runtime_functions.deterministic_sequence_checkpoint(),
            read.clone(),
            prepared_writes,
            transaction.pending_forktree_publication.take(),
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
        if rebuild_filesystem_path_index {
            transaction.filesystem_path_index_cache.clear();
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
        self.refresh_state_view_from_staging(false)?;
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
        let outcome = Box::pin(self.stage_write_inner(write, None)).await?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
    }

    async fn stage_parameter_batch_insert(
        &mut self,
        write: TransactionWrite,
        statement_indices: Vec<u32>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let outcome = Box::pin(self.stage_write_inner(write, Some(statement_indices))).await?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
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
        if self
            .staged_writes
            .collection_replaced(branch_id.as_str(), schema_key.as_str(), None)?
        {
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
        let outcome = tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage")
            .in_scope(|| {
                self.staged_writes.stage_certified_parameter_batch_insert(
                    PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: prepared,
                    },
                )
            })?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
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
        let outcome = tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage")
            .in_scope(|| {
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: prepared,
                    })
            })?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
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
        let file_view_mutations = BTreeMap::<SessionFileViewKey, SessionFileViewMutation>::new();
        let actor_publications = Vec::<PendingPluginActorPublication>::new();
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_transaction_rows_staged(transaction_write_row_count(
                &write,
            ));
            crate::storage_bench::record_transaction_untracked_rows(
                transaction_write_untracked_row_count(&write),
            );
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
            && statement_indices.len() != prepared_transaction_write_row_count(&write)
        {
            discard_plugin_actor_publications(actor_publications).await;
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "parameter batch normalization changed row cardinality",
            ));
        }
        let outcome = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.transaction_buffer_stage"
        )
        .in_scope(|| match statement_indices {
            Some(indices) => self
                .staged_writes
                .stage_parameter_batch_insert(write, indices),
            None => self.staged_writes.stage_write(write),
        })?;
        self.refresh_state_view_from_staging(true)?;
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
            if self.staged_writes.collection_replaced(
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
    async fn prepare_transaction_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<PreparedTransactionWrite, LixError> {
        Ok(match write {
            TransactionWrite::Rows { mode, rows } => PreparedTransactionWrite::Rows {
                mode,
                rows: self.prepare_transaction_rows(rows).await?,
            },
            TransactionWrite::RowsWithFileContent {
                mode,
                rows,
                file_content,
                count,
            } => PreparedTransactionWrite::RowsWithFileContent {
                mode,
                rows: self.prepare_transaction_rows(rows).await?,
                file_content,
                count,
            },
        })
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
        if allow_homogeneous && let Some(domain) = homogeneous_row_normalization_domain(&rows) {
            let functions = self.functions.clone();
            let catalog = self
                .schema_resolver
                .catalog_for_row_normalization(&self.state_view, &domain)
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
                .catalog_for_row_normalization(&self.state_view, &domain)
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

    async fn open_state_view(
        read: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
        branch_id: String,
    ) -> Result<ForkTreeStateView<SharedStorageAdapterRead<StorageImpl::Read<'static>>>, LixError>
    {
        ForkTreeStateView::from_facade(ForkTreeReadFacade::new(read), &branch_id).await
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
        let rows = Self::open_state_view(self.opening_read(), self.active_branch_id.clone())
            .await?
            .untracked_overlay_rows()
            .await?;
        Ok(rows.into_iter().any(|row| {
            uuid::Uuid::from_bytes(*row.owner.as_bytes()).to_string() == self.active_branch_id
                && row
                    .key
                    .file_id
                    .as_deref()
                    .is_some_and(|file_id| file_ids.iter().any(|candidate| candidate == file_id))
        }))
    }

    /// Reports whether the active branch has any visible untracked state.
    pub(crate) async fn has_untracked_rows(&mut self) -> Result<bool, LixError> {
        Ok(
            !Self::open_state_view(self.opening_read(), self.active_branch_id.clone())
                .await?
                .untracked_branch_range(None, None, Some(1))
                .await?
                .is_empty(),
        )
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
        self.refresh_state_view_from_staging(true)?;
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
            let current =
                load_opening_state_points(&forktree, &schema_key, &branch_id, entity_pks).await?;
            for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
                let row = current.get(slot).and_then(Option::as_ref).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation lost its current-state predecessor",
                    )
                })?;
                let decoded_key = crate::forktree::decode_state_key(&row.key)?;
                if decoded_key.schema_key != schema_key
                    || decoded_key.entity_pk != *expected_entity_pk
                    || matches!(row.value.cell, StateCell::Tombstone)
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "partial immutable mutation predecessor order changed",
                    ));
                }
                predecessors.push(CertifiedStatePredecessor::new(row.value.created_at));
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
        let forktree = self.forktree_read_facade();
        let current = load_opening_state_points(
            &forktree,
            chunk.schema_key(),
            chunk.branch_id(),
            &entity_pks,
        )
        .await?;
        let mut predecessors = Vec::with_capacity(entity_pks.len());
        for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
            let row = current.get(slot).and_then(Option::as_ref).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation lost its current-state predecessor",
                )
            })?;
            let decoded_key = crate::forktree::decode_state_key(&row.key)?;
            if decoded_key.schema_key != chunk.schema_key()
                || decoded_key.entity_pk != *expected_entity_pk
                || matches!(row.value.cell, StateCell::Tombstone)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "mixed immutable mutation predecessor order changed",
                ));
            }
            predecessors.push(CertifiedStatePredecessor::new(row.value.created_at));
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
        let forktree = ForkTreeReadFacade::new(read_store.clone());
        let active_branch_id = self.active_branch_id.clone();
        let state_view =
            ForkTreeStateView::from_facade(forktree.clone(), &active_branch_id).await?;
        let visible_schemas = self.sql_visible_schemas();
        let functions = self.functions.clone();
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
            state_view,
            visible_schemas,
            functions,
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
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<String, LixError> {
        let commit_id = self
            .staged_writes
            .stage_selected_commit_change_refs(branch_id.clone(), selected_changes)?;
        self.staged_writes
            .set_first_commit_parent(branch_id.clone(), previous_checkpoint_commit_id)?;
        self.staged_writes
            .add_checkpoint_publication(CheckpointPublication {
                recovery_ref: CheckpointRecoveryRef {
                    branch_id,
                    recovered_head_commit_id,
                    interval_has_commits,
                },
            })?;
        Ok(commit_id)
    }

    /// Loads the branch-local recovery root from one retained storage snapshot.
    pub(crate) async fn checkpoint_publication_state(
        &mut self,
        branch_id: &str,
    ) -> Result<Option<CheckpointRecoveryRef>, LixError> {
        let read = self.opening_read();
        load_checkpoint_publication_state(&read, branch_id).await
    }

    /// Creates a branch-ref reader over this transaction's retained opening
    /// read. Merge planning must not acquire a second snapshot just to resolve
    /// branch selectors.
    pub(crate) fn branch_ref_reader_on_opening_read(&self) -> impl BranchRefReader + '_ {
        BranchRefStoreReader::new(&self.opening_read)
    }

    /// Creates a commit-graph reader over the same immutable read that opened
    /// this transaction. The graph reader owns its positive topology cache and
    /// cannot refresh the transaction's view.
    pub(crate) fn commit_graph_reader_on_opening_read(
        &self,
    ) -> CommitGraphStoreReader<&SharedStorageAdapterRead<StorageImpl::Read<'static>>> {
        CommitGraphStoreReader::new(&self.opening_read)
    }

    /// Applies a tracked-state transition resolved from two immutable commits.
    ///
    /// This is the internal counterpart to the public diff command. The
    /// caller supplies typed identities instead of user-facing `diff_id`
    /// strings. The transaction's coherent opening head certifies the current
    /// side, so undo/redo does not need to reload visible live state after it
    /// has already read that exact historical root.
    #[cfg(test)]
    pub(crate) async fn execute_state_transition(
        &mut self,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        keys: Vec<StateKey>,
    ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
        let facade = self.forktree_read_facade();
        self.execute_state_transition_with_facade(
            &facade,
            current_commit_id,
            desired_commit_id,
            keys,
        )
        .await
    }

    pub(crate) async fn execute_state_transition_with_facade<R>(
        &mut self,
        facade: &ForkTreeReadFacade<R>,
        current_commit_id: CommitId,
        desired_commit_id: CommitId,
        keys: Vec<StateKey>,
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
            let (snapshot, metadata) = match transition.target {
                Some(target) => (
                    parse_materialized_diff_json(
                        target.snapshot_content,
                        "typed state transition target",
                    )?,
                    parse_materialized_diff_json(
                        target.metadata,
                        "typed state transition target metadata",
                    )?,
                ),
                None => (None, None),
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
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: branch_id.clone().into(),
            });
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
                crate::state::decode_diff_id(diff_id)
                    .map(|sides| (diff_id.as_str(), sides))
                    .map_err(|_| stale_or_unknown_diff_id())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let change_ids = selections
            .iter()
            .flat_map(|(_, sides)| [sides.before, sides.after])
            .flatten()
            .collect::<BTreeSet<_>>();
        let read = self.opening_read();
        let records = load_change_records(&read, change_ids.into_iter()).await?;
        let mut forktree_reader = ForkTreeReadFacade::new(read.clone());
        let mut payloads = materialize_known_change_payloads(
            &mut forktree_reader,
            records.values().cloned(),
            ChangeRecordProjection::full(),
        )
        .await?;
        drop(forktree_reader);
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
        let state_view =
            Self::open_state_view(self.opening_read(), self.active_branch_id.clone()).await?;
        let state_keys = plans
            .iter()
            .map(|(_, (schema_key, entity_pk, file_id), _, _)| {
                encode_state_key(StateKeyRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                    entity_pk,
                })
            })
            .collect::<Vec<_>>();
        let current = state_view
            .points(&state_keys, true)
            .await
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("diff command state lookup failed: {error}"),
                )
            })?;
        let mut target_change_ids = Vec::new();
        let mut rows = RawWriteBatch::with_capacity(plans.len());
        for ((diff_id, (schema_key, entity_pk, file_id), expected, target), current) in
            plans.into_iter().zip(current)
        {
            let current_matches = match expected {
                Some(expected) => current
                    .as_ref()
                    .map(|row| row.value.change_id == expected)
                    .unwrap_or(false),
                None => current
                    .as_ref()
                    .is_none_or(|row| row.value.cell == StateCell::Tombstone),
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
        let _previous_recovery = self.checkpoint_publication_state(&branch_id).await?;
        let head_commit_id = self
            .load_branch_head(&branch_id)
            .await?
            .ok_or_else(|| LixError::branch_not_found(&branch_id, "create checkpoint", "target"))?;
        let previous_checkpoint_commit_id = self
            .forktree_read_facade()
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
        let diff = self
            .forktree_read_facade()
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
            let diff_id = crate::state::encode_diff_id(
                entry.before.as_ref().map(|row| row.change_id),
                entry.after.as_ref().map(|row| row.change_id),
            )?;
            let target = entry.after.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("working diff '{diff_id}' has no target row"),
                )
            })?;
            let created_at =
                if entry.before.as_ref().is_none_or(|before| before.deleted) && !target.deleted {
                    target.updated_at
                } else {
                    target.created_at
                };
            if requested.contains(&diff_id) {
                matched.insert(diff_id);
                selected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut selected, target, created_at);
            } else {
                unselected_source_membership_exact &=
                    push_checkpoint_selected_change(&mut unselected, target, created_at);
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
                        interval_has_commits,
                    },
                })?;
            checkpoint_commit_id.to_string()
        };
        self.refresh_state_view_from_staging(true)?;
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
    forktree: &ForkTreeReadFacade<impl StorageAdapterRead + Clone + 'static>,
    schema_key: &str,
    branch_id: &str,
    entity_pk_chunks: &[Arc<[EntityPk]>],
) -> Result<Vec<CertifiedStatePredecessor>, LixError> {
    let row_count = entity_pk_chunks.iter().map(|chunk| chunk.len()).sum();
    let mut predecessors = Vec::with_capacity(row_count);
    for entity_pks in entity_pk_chunks {
        let current =
            load_opening_state_points(forktree, schema_key, branch_id, entity_pks).await?;
        for (slot, expected_entity_pk) in entity_pks.iter().enumerate() {
            let row = current.get(slot).and_then(Option::as_ref).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation lost its current-state predecessor",
                )
            })?;
            let decoded_key = crate::forktree::decode_state_key(&row.key)?;
            if decoded_key.schema_key != schema_key
                || decoded_key.entity_pk != *expected_entity_pk
                || matches!(row.value.cell, StateCell::Tombstone)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation predecessor order changed",
                ));
            }
            predecessors.push(CertifiedStatePredecessor::new(row.value.created_at));
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

async fn load_opening_state_points(
    forktree: &ForkTreeReadFacade<impl StorageAdapterRead + Clone + 'static>,
    schema_key: &str,
    branch_id: &str,
    entity_pks: &[EntityPk],
) -> Result<Vec<Option<crate::state::StateRow>>, LixError> {
    let view = ForkTreeStateView::from_facade((*forktree).clone(), branch_id).await?;
    let keys = entity_pks
        .iter()
        .map(|entity_pk| {
            encode_state_key(StateKeyRef {
                schema_key,
                file_id: None,
                entity_pk,
            })
        })
        .collect::<Vec<_>>();
    Ok(view.points(&keys, true).await?)
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
    row: HistoricalStateRow,
    created_at: LixTimestamp,
) -> bool {
    let source_membership_exact = created_at == row.created_at;
    selected.push(
        row.key,
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
    state_view: ForkTreeStateView<SharedStorageAdapterRead<R>>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
    filesystem_path_index_cache: Arc<FilesystemPathIndexCache>,
    filesystem_path_index_epoch: Arc<AtomicUsize>,
    plugin_host: PluginRuntimeHost,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    sql_catalog_fingerprint: CatalogFingerprint,
}

impl<R> TransactionSqlReadExecutionContext<R>
where
    R: crate::storage_adapter::StorageRead,
{
    pub(crate) fn state_view(&self) -> &ForkTreeStateView<SharedStorageAdapterRead<R>> {
        &self.state_view
    }
}

impl<R> Clone for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage_adapter::StorageRead,
{
    fn clone(&self) -> Self {
        Self {
            active_branch_id: self.active_branch_id.clone(),
            active_account_id: self.active_account_id.clone(),
            read_store: self.read_store.clone(),
            forktree: self.forktree.clone(),
            state_view: self.state_view.clone(),
            visible_schemas: self.visible_schemas.clone(),
            functions: self.functions.clone(),
            filesystem_path_index_cache: Arc::clone(&self.filesystem_path_index_cache),
            filesystem_path_index_epoch: Arc::clone(&self.filesystem_path_index_epoch),
            plugin_host: self.plugin_host.clone(),
            sql_planning_cache: Arc::clone(&self.sql_planning_cache),
            sql_catalog_fingerprint: self.sql_catalog_fingerprint.clone(),
        }
    }
}

#[async_trait]
impl<R> SqlExecutionContext for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage_adapter::StorageRead + 'static,
{
    type ReadStore = SharedStorageAdapterRead<R>;

    fn state_view(&self) -> &ForkTreeStateView<Self::ReadStore> {
        &self.state_view
    }

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

    fn filesystem_path_index(&self) -> Arc<dyn FilesystemPathIndexReader> {
        Arc::new(self.clone())
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
        ChangelogQuerySource {
            forktree_reader: ForkTreeReadFacade::new(self.read_store.clone()),
        }
    }

    fn commit_graph(&self) -> Box<dyn crate::commit_graph::CommitGraphReader> {
        Box::new(CommitGraphStoreReader::new(self.read_store.clone()))
    }

    fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
        Arc::new(BranchRefStoreReader::new(self.read_store.clone()))
    }

    fn authenticated_blob_reader(&self) -> Result<Arc<dyn AuthenticatedBlobReader>, LixError> {
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

#[async_trait]
impl<R> FilesystemPathIndexReader for TransactionSqlReadExecutionContext<R>
where
    R: crate::storage_adapter::StorageRead + Send + 'static,
{
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        crate::filesystem::build_path_index(&self.state_view, request).await
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
            if BlobId::from_canonical_content(&bytes) != *expected {
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
            if *actual != *expected || BlobId::from_canonical_content(bytes) != *expected {
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
            if BlobId::from_canonical_content(&bytes) != expected {
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

/// Runs validation-only derived projections through the caller-owned facade.
///
/// One request-local graph reader borrows this facade directly, so it cannot
/// acquire a storage read or become a second current-state owner. Ordinary
/// rows, commit rows, commit edges, and branch refs all remain bound to the
/// same opening read and its authenticated ForkTree identity.
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
        // Retiring a branch removes its descriptor and selector but cannot
        // change registered-schema visibility for any surviving branch.
        // Creation/repoint still rotates the revision: creation fences a
        // cached snapshot if the same branch identity is reused, while a
        // repoint can expose another commit's registered schemas.
        || prepared_writes
            .branch_ref_intents
            .iter()
            .any(|intent| intent.create || intent.commit_id.is_some())
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
    plugin_host: PluginRuntimeHost,
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
        plugin_host,
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
    plugin_host: PluginRuntimeHost,
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
        plugin_host,
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
    type ReadStore = SharedStorageAdapterRead<StorageImpl::Read<'static>>;

    fn state_view(&self) -> &TransactionStateView<Self::ReadStore> {
        &self.state_view
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

    fn authenticated_blob_reader(&self) -> Result<Arc<dyn AuthenticatedBlobReader>, LixError> {
        Ok(Arc::new(crate::forktree::blob_reader_on_read(
            self.opening_read(),
            &self.active_branch_id,
        )?))
    }

    fn load_staged_file_bytes_for_owner(
        &self,
        branch_id: &str,
        file_id: &str,
        expected: BlobId,
    ) -> Result<Option<Vec<u8>>, LixError> {
        self.staged_writes
            .load_staged_file_bytes_for_owner(branch_id, file_id, expected)
    }

    async fn filesystem_path_index(
        &mut self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let revision = self
            .filesystem_path_index_epoch
            .load(Ordering::SeqCst)
            .to_be_bytes();
        if let Some(index) = self
            .filesystem_path_index_cache
            .get(request, Some(&revision))
        {
            return Ok(index);
        }
        let index =
            crate::filesystem::path_index::build_path_index(&self.state_view, request).await?;
        Ok(self
            .filesystem_path_index_cache
            .insert(request, Some(&revision), index))
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        let read = self.opening_read();

        BranchRefStoreReader::new(read)
            .load_head_commit_id(branch_id)
            .await
    }

    async fn load_collection_generation(
        &mut self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        self.forktree_read_facade()
            .collection_generation(branch_id, scope)
            .await
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
        let view = ForkTreeStateView::from_facade(self.forktree_read_facade(), branch_id).await?;
        let rows = view.range(None, None, None, false).await.map_err(|error| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("collection state range failed: {error}"),
            )
        })?;
        let count = rows
            .into_iter()
            .filter_map(|row| crate::forktree::decode_state_key(&row.key).ok())
            .filter(|key| {
                key.schema_key == scope.schema_key
                    && scope
                        .file_id
                        .is_none_or(|file_id| key.file_id.as_deref() == Some(file_id))
            })
            .count();
        Ok(Some(u64::try_from(count).unwrap_or(u64::MAX)))
    }

    fn has_staged_collection_rows(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<bool, LixError> {
        self.staged_writes
            .has_staged_collection_rows(branch_id, scope)
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
        if self
            .staged_writes
            .collection_replaced(&branch_id, &schema_key, None)?
        {
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
        let outcome = tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage")
            .in_scope(|| {
                self.staged_writes.stage_certified_parameter_batch_insert(
                    PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: prepared,
                    },
                )
            })?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
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
        let outcome = tracing::debug_span!(target: "lix_perf", "lix.perf.transaction_buffer_stage")
            .in_scope(|| {
                self.staged_writes
                    .stage_write(PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: prepared,
                    })
            })?;
        self.refresh_state_view_from_staging(true)?;
        Ok(outcome)
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
        self.refresh_state_view_from_staging(true)?;
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

fn prepared_transaction_write_row_count(write: &PreparedTransactionWrite) -> usize {
    match write {
        PreparedTransactionWrite::Rows { rows, .. }
        | PreparedTransactionWrite::RowsWithFileContent { rows, .. } => rows.len(),
    }
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
            let branch_ref = BranchRefStoreReader::new(read);
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
