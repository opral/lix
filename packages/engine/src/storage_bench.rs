use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    ScanPlan, StorageAdapter, StorageAdapterRead, StorageCoreProjection, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageWriteOptions, StorageWriteSet,
    StorageWriteSetError,
};
use crate::{ReadOptions, WriteOptions};

fn stage_bench_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[crate::tracked_state::TrackedStateCommitDeltaRef<'_>],
) -> Result<Vec<crate::tracked_state::CommitDeltaChangeLocator>, crate::LixError> {
    let staged = crate::tracked_state::stage_commit_deltas_for_commit_state(writes, deltas)?;
    let commit_id = deltas
        .first()
        .map(|delta| delta.delta.commit_id)
        .unwrap_or_default();
    let mutations = staged.mutation_inventory().clone();
    crate::tracked_state::stage_commit_state_manifest(
        writes,
        &crate::tracked_state::CommitStateManifest {
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            commit_change_id: crate::changelog::ChangeId::for_test_label(&format!(
                "{commit_id}:bench-commit"
            )),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
            replay_debt: crate::tracked_state::CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            current_state_catalog: None,
            current_state_coverage_anchor: None,
            snapshot_root: None,
        },
    )?;
    Ok(staged.locators)
}

static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_BRANCHS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_COMPILES: AtomicU64 = AtomicU64::new(0);
static JSON_STORE_STAGE_BYTES: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_HITS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ROWS: AtomicU64 = AtomicU64::new(0);
static ENTITY_POINT_SNAPSHOT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static ENTITY_POINT_SNAPSHOT_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_PUTS: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_DELETES: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_WRITTEN_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_COMMIT_STATE_MANIFEST_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_DIRECTORY_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_DIRECTORY_NODES_LOADED: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_DIRECTORY_DESCRIPTORS_VISITED: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_DIRECTORY_NODES_ENCODED: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_CATALOG_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_DIRECTORY_RECOVERIES: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_CATALOG_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_CATALOG_HITS: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_CATALOG_ERRORS: AtomicU64 = AtomicU64::new(0);
static CRUD_SEALED_MANIFEST_LOADS: AtomicU64 = AtomicU64::new(0);
static CRUD_REPLAY_MANIFEST_LOADS: AtomicU64 = AtomicU64::new(0);
static CRUD_ORDERED_DELTA_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_MANIFEST_LEAF_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_SUMMARIZED_CHUNK_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_CHUNK_PAYLOAD_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
static IMMUTABLE_SEGMENT_IDENTITY_HASH_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MediaStructuralAccounting {
    pub temporary_manifest_leaf_rows: u64,
    pub legacy_equivalent_chunk_rows: u64,
    pub chunk_payload_hash_bytes: u64,
    pub segment_identity_hash_bytes: u64,
}

pub(crate) fn record_media_upload_manifest_leaf(chunk_count: usize) {
    MEDIA_UPLOAD_MANIFEST_LEAF_ROWS.fetch_add(1, Ordering::Relaxed);
    MEDIA_UPLOAD_SUMMARIZED_CHUNK_ROWS.fetch_add(chunk_count as u64, Ordering::Relaxed);
}

pub(crate) fn record_media_upload_chunk_payload_hash_bytes(payload_bytes: usize) {
    MEDIA_UPLOAD_CHUNK_PAYLOAD_HASH_BYTES.fetch_add(payload_bytes as u64, Ordering::Relaxed);
}

pub(crate) fn record_immutable_segment_identity_hash_bytes(bytes: usize) {
    IMMUTABLE_SEGMENT_IDENTITY_HASH_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn take_media_structural_accounting() -> MediaStructuralAccounting {
    MediaStructuralAccounting {
        temporary_manifest_leaf_rows: MEDIA_UPLOAD_MANIFEST_LEAF_ROWS.swap(0, Ordering::Relaxed),
        legacy_equivalent_chunk_rows: MEDIA_UPLOAD_SUMMARIZED_CHUNK_ROWS.swap(0, Ordering::Relaxed),
        chunk_payload_hash_bytes: MEDIA_UPLOAD_CHUNK_PAYLOAD_HASH_BYTES.swap(0, Ordering::Relaxed),
        segment_identity_hash_bytes: IMMUTABLE_SEGMENT_IDENTITY_HASH_BYTES
            .swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn record_certified_entity_insert_parameter_batch_execution() {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Returns and resets the number of certified parameter-batch INSERT routes.
///
/// Benchmark fixtures use this as a route certificate so a schema change
/// cannot silently turn the measured bulk INSERT back into sequential writes.
pub fn take_certified_entity_insert_parameter_batch_executions() -> u64 {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.swap(0, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudCertificateAccounting {
    pub attempts: u64,
    pub hits: u64,
    pub misses: u64,
    pub certified_rows: u64,
}

pub(crate) fn record_certified_entity_update_value_batch_attempt() {
    CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_certified_entity_update_value_batch_hit(row_count: usize) {
    CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_HITS.fetch_add(1, Ordering::Relaxed);
    CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ROWS.fetch_add(row_count as u64, Ordering::Relaxed);
}

/// Returns and resets generated UPDATE certificate hit/miss accounting.
pub fn take_certified_entity_update_value_batch_accounting() -> CrudCertificateAccounting {
    let attempts = CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ATTEMPTS.swap(0, Ordering::Relaxed);
    let hits = CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_HITS.swap(0, Ordering::Relaxed);
    let certified_rows = CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ROWS.swap(0, Ordering::Relaxed);
    CrudCertificateAccounting {
        attempts,
        hits,
        misses: attempts.saturating_sub(hits),
        certified_rows,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntityPointSnapshotCacheAccounting {
    pub hits: u64,
    pub misses: u64,
}

pub(crate) fn record_entity_point_snapshot_cache_hit() {
    ENTITY_POINT_SNAPSHOT_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_entity_point_snapshot_cache_miss() {
    ENTITY_POINT_SNAPSHOT_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

pub fn take_entity_point_snapshot_cache_accounting() -> EntityPointSnapshotCacheAccounting {
    EntityPointSnapshotCacheAccounting {
        hits: ENTITY_POINT_SNAPSHOT_CACHE_HITS.swap(0, Ordering::Relaxed),
        misses: ENTITY_POINT_SNAPSHOT_CACHE_MISSES.swap(0, Ordering::Relaxed),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudPhysicalWriteAccounting {
    pub puts: u64,
    pub deletes: u64,
    pub written_bytes: u64,
}

pub(crate) fn record_crud_physical_writes(stats: crate::storage_adapter::StorageWriteSetStats) {
    CRUD_PHYSICAL_PUTS.fetch_add(stats.staged_puts, Ordering::Relaxed);
    CRUD_PHYSICAL_DELETES.fetch_add(stats.staged_deletes, Ordering::Relaxed);
    CRUD_PHYSICAL_WRITTEN_BYTES.fetch_add(stats.written_bytes, Ordering::Relaxed);
}

pub fn take_crud_physical_write_accounting() -> CrudPhysicalWriteAccounting {
    CrudPhysicalWriteAccounting {
        puts: CRUD_PHYSICAL_PUTS.swap(0, Ordering::Relaxed),
        deletes: CRUD_PHYSICAL_DELETES.swap(0, Ordering::Relaxed),
        written_bytes: CRUD_PHYSICAL_WRITTEN_BYTES.swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn record_crud_commit_state_manifest_bytes(bytes: usize) {
    CRUD_COMMIT_STATE_MANIFEST_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn take_crud_commit_state_manifest_bytes() -> u64 {
    CRUD_COMMIT_STATE_MANIFEST_BYTES.swap(0, Ordering::Relaxed)
}

pub(crate) fn record_crud_current_state_directory_bytes(bytes: usize) {
    CRUD_CURRENT_STATE_DIRECTORY_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn take_crud_current_state_directory_bytes() -> u64 {
    CRUD_CURRENT_STATE_DIRECTORY_BYTES.swap(0, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudCurrentStateDirectoryAccounting {
    pub nodes_loaded: u64,
    pub descriptors_visited: u64,
    pub nodes_encoded: u64,
}

pub(crate) fn record_crud_current_state_directory_node_loaded() {
    CRUD_CURRENT_STATE_DIRECTORY_NODES_LOADED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_directory_descriptors_visited(count: usize) {
    CRUD_CURRENT_STATE_DIRECTORY_DESCRIPTORS_VISITED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_directory_node_encoded() {
    CRUD_CURRENT_STATE_DIRECTORY_NODES_ENCODED.fetch_add(1, Ordering::Relaxed);
}

pub fn take_crud_current_state_directory_accounting() -> CrudCurrentStateDirectoryAccounting {
    CrudCurrentStateDirectoryAccounting {
        nodes_loaded: CRUD_CURRENT_STATE_DIRECTORY_NODES_LOADED.swap(0, Ordering::Relaxed),
        descriptors_visited: CRUD_CURRENT_STATE_DIRECTORY_DESCRIPTORS_VISITED
            .swap(0, Ordering::Relaxed),
        nodes_encoded: CRUD_CURRENT_STATE_DIRECTORY_NODES_ENCODED.swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn record_crud_current_state_catalog_bytes(bytes: usize) {
    CRUD_CURRENT_STATE_CATALOG_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn take_crud_current_state_catalog_bytes() -> u64 {
    CRUD_CURRENT_STATE_CATALOG_BYTES.swap(0, Ordering::Relaxed)
}

pub(crate) fn record_crud_current_state_directory_recovery() {
    CRUD_CURRENT_STATE_DIRECTORY_RECOVERIES.fetch_add(1, Ordering::Relaxed);
}

pub fn take_crud_current_state_directory_recoveries() -> u64 {
    CRUD_CURRENT_STATE_DIRECTORY_RECOVERIES.swap(0, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudCurrentStateCatalogAccounting {
    pub attempts: u64,
    pub hits: u64,
    pub errors: u64,
    pub sealed_manifest_loads: u64,
    pub replay_manifest_loads: u64,
    pub ordered_delta_fallbacks: u64,
}

pub(crate) fn record_crud_current_state_catalog_attempt() {
    CRUD_CURRENT_STATE_CATALOG_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_catalog_hit() {
    CRUD_CURRENT_STATE_CATALOG_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_catalog_error() {
    CRUD_CURRENT_STATE_CATALOG_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_sealed_manifest_load() {
    CRUD_SEALED_MANIFEST_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_replay_manifest_load() {
    CRUD_REPLAY_MANIFEST_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_ordered_delta_fallback() {
    CRUD_ORDERED_DELTA_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

pub fn take_crud_current_state_catalog_accounting() -> CrudCurrentStateCatalogAccounting {
    CrudCurrentStateCatalogAccounting {
        attempts: CRUD_CURRENT_STATE_CATALOG_ATTEMPTS.swap(0, Ordering::Relaxed),
        hits: CRUD_CURRENT_STATE_CATALOG_HITS.swap(0, Ordering::Relaxed),
        errors: CRUD_CURRENT_STATE_CATALOG_ERRORS.swap(0, Ordering::Relaxed),
        sealed_manifest_loads: CRUD_SEALED_MANIFEST_LOADS.swap(0, Ordering::Relaxed),
        replay_manifest_loads: CRUD_REPLAY_MANIFEST_LOADS.swap(0, Ordering::Relaxed),
        ordered_delta_fallbacks: CRUD_ORDERED_DELTA_FALLBACKS.swap(0, Ordering::Relaxed),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinaryCasWriteAccounting {
    pub chunk_lookup_count: u64,
    pub chunk_lookup_batch_count: u64,
    pub chunk_lookup_hit_count: u64,
    pub chunk_lookup_miss_count: u64,
    pub chunk_lookup_elapsed_ns: u64,
    pub transaction_duplicate_chunk_count: u64,
}

/// Result of one benchmark-only historical tracked-state diff.
///
/// The durable-root flags prove which physical diff path the benchmark used:
/// the intended populated case is a checkpoint on the left and a rootless
/// ordinary commit on the right.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedHistoricalDiffBenchResult {
    pub entries: usize,
    pub left_has_durable_root: bool,
    pub right_has_durable_root: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointCommitScanBenchMode {
    Materialize,
    Stream,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointCommitScanBenchResult {
    pub commits: usize,
    pub pages: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitGraphBenchMode {
    AllNodes,
    LegacyAllNodes,
    ReachableNodes,
    LegacyReachableNodes,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitGraphBenchResult {
    pub nodes: usize,
    pub edges: usize,
    pub member_changes: usize,
}

/// Measures topology reads against the superseded eager commit shape.
///
/// Legacy modes deliberately reproduce the removed work: synthesized commit
/// changes for broad scans, plus commit-member payload hydration for graph
/// walks. The result counts keep that work observable to the optimizer.
#[inline(never)]
pub async fn read_commit_graph_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    head_commit_id: &str,
    mode: CommitGraphBenchMode,
) -> Result<CommitGraphBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut reader = crate::commit_graph::CommitGraphContext::new().reader(read);
    let head_commit_id =
        crate::changelog::CommitId::parse_lix(head_commit_id, "commit graph benchmark head")?;
    let nodes = match mode {
        CommitGraphBenchMode::AllNodes | CommitGraphBenchMode::LegacyAllNodes => {
            reader.all_nodes().await?
        }
        CommitGraphBenchMode::ReachableNodes | CommitGraphBenchMode::LegacyReachableNodes => reader
            .reachable_nodes(&head_commit_id)
            .await?
            .iter()
            .map(|reachable| reachable.commit.clone())
            .collect(),
    };
    let node_count = nodes.len();
    let edges = crate::commit_graph::commit_edges(&nodes).len();
    let mut member_changes = 0usize;
    if matches!(
        mode,
        CommitGraphBenchMode::LegacyAllNodes | CommitGraphBenchMode::LegacyReachableNodes
    ) {
        let mut legacy_shape = Vec::with_capacity(nodes.len());
        for node in nodes {
            let canonical = crate::commit_graph::canonical_commit_change(&node);
            let members = if mode == CommitGraphBenchMode::LegacyReachableNodes {
                let members = crate::tracked_state::load_commit_delta_members_with_payloads(
                    reader.store(),
                    node.commit_id,
                )
                .await?;
                member_changes = member_changes.saturating_add(members.len());
                members
            } else {
                Vec::new()
            };
            let mut member_change_ids = Vec::with_capacity(members.len());
            let member_payloads = members
                .into_iter()
                .map(|member| {
                    member_change_ids.push(member.change.change_id);
                    member.change
                })
                .collect::<Vec<_>>();
            legacy_shape.push((
                node,
                canonical.clone(),
                canonical,
                member_change_ids,
                member_payloads,
            ));
        }
        std::hint::black_box(&legacy_shape);
    }
    Ok(CommitGraphBenchResult {
        nodes: node_count,
        edges,
        member_changes,
    })
}

/// Adds one representative tracked payload to every commit in a graph fixture.
///
/// The topology benchmark uses this to preserve the old reachable-commit
/// model's payload-cache and retained-member costs instead of benchmarking an
/// unrealistically commit-only history.
pub async fn seed_commit_graph_members_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    commit_ids: &[String],
) -> Result<(), crate::LixError>
where
    StorageImpl: Storage,
{
    let mut writes = storage.new_write_set();
    let created_at = crate::common::LixTimestamp::expect_parse(
        "commit graph benchmark timestamp",
        "2026-05-20T00:00:00Z",
    );
    for (index, commit_id) in commit_ids.iter().enumerate() {
        let commit_id = crate::changelog::CommitId::parse_lix(
            commit_id,
            "commit graph benchmark member commit",
        )?;
        let entity_pk = crate::entity_pk::EntityPk::single(format!("bench-member-{index:08}"));
        let snapshot = crate::json_store::JsonSlot::from_json(&format!(
            "{{\"index\":{index},\"payload\":\"{}\"}}",
            "x".repeat(192)
        ));
        stage_bench_commit_deltas(
            &mut writes,
            &[crate::tracked_state::TrackedStateCommitDeltaRef {
                delta: crate::tracked_state::TrackedStateDeltaRef {
                    schema_key: "commit_graph_bench_member",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: crate::changelog::ChangeId::for_test_label(&format!(
                        "commit-graph-bench-member-{index}"
                    )),
                    commit_id,
                    deleted: false,
                    created_at,
                    updated_at: created_at,
                },
                snapshot: snapshot.as_ref_slot(),
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            }],
        )?;
    }
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepositoryGcBenchResult {
    pub live_commits: usize,
    pub swept_commits: usize,
    pub swept_standalone_changes: usize,
    pub swept_payloads: usize,
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub staged_written_bytes: u64,
    pub delete_descriptors: usize,
    pub delete_descriptor_capacity: usize,
    pub key_inline_bytes: usize,
    pub key_inline_capacity: usize,
    pub key_shared_buffers: usize,
    pub key_shared_bytes: usize,
    pub key_shared_capacity: usize,
    pub root_discovery_us: u64,
    pub changelog_us: u64,
    pub tracked_root_stage_us: u64,
    pub total_us: u64,
}

/// Plans production repository GC without committing its staged sweep.
///
/// The returned arena sizes expose the planner's retained mutation footprint;
/// dropping this function's local write set leaves the fixture unchanged for
/// repeatable measurements.
#[inline(never)]
pub async fn plan_repository_gc_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
) -> Result<RepositoryGcBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        storage.begin_read(ReadOptions::default()).await?,
    );
    let mut writes = storage.new_write_set();
    let plan = crate::gc::stage_repository_gc(read, &mut writes).await?;
    let stats = writes.stats();
    let arena = writes.arena_stats();
    Ok(RepositoryGcBenchResult {
        live_commits: plan.changelog.live.commits.len(),
        swept_commits: plan.changelog.sweep.commits.len(),
        swept_standalone_changes: plan.changelog.sweep.changes.len(),
        swept_payloads: plan.changelog.sweep.json_payloads.len(),
        staged_puts: stats.staged_puts,
        staged_deletes: stats.staged_deletes,
        staged_written_bytes: stats.written_bytes,
        delete_descriptors: arena.delete_descriptors,
        delete_descriptor_capacity: arena.delete_descriptor_capacity,
        key_inline_bytes: arena.key_inline_bytes,
        key_inline_capacity: arena.key_inline_capacity,
        key_shared_buffers: arena.key_shared_buffers,
        key_shared_bytes: arena.key_shared_bytes,
        key_shared_capacity: arena.key_shared_capacity,
        root_discovery_us: plan.profile.root_discovery_us,
        changelog_us: plan.profile.changelog_us,
        tracked_root_stage_us: plan.profile.tracked_root_stage_us,
        total_us: plan.profile.total_us,
    })
}

/// Scans public commit facts through the two checkpoint-history strategies.
///
/// `Materialize` is the current production path for unbounded checkpoint
/// history. `Stream` is a bounded-memory baseline over the same storage rows,
/// page size, codec, and read snapshot.
#[inline(never)]
pub async fn scan_checkpoint_commits_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    mode: CheckpointCommitScanBenchMode,
) -> Result<CheckpointCommitScanBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    const PAGE_SIZE: usize = 1_024;

    let read = storage.begin_read(ReadOptions::default()).await?;
    match mode {
        CheckpointCommitScanBenchMode::Materialize => {
            let records = crate::checkpoint::scan_checkpoint_commit_records(read).await?;
            Ok(CheckpointCommitScanBenchResult {
                commits: records.len(),
                pages: records.len().div_ceil(PAGE_SIZE),
            })
        }
        CheckpointCommitScanBenchMode::Stream => {
            let mut reader = crate::changelog::ChangelogContext::new().reader(read);
            let mut commits = 0usize;
            let mut pages = 0usize;
            let mut start_after = None::<String>;
            loop {
                let batch = crate::changelog::ChangelogReader::scan_commits(
                    &mut reader,
                    crate::changelog::CommitScanRequest {
                        start_after: start_after.as_deref(),
                        limit: Some(PAGE_SIZE),
                    },
                )
                .await?;
                commits = commits.checked_add(batch.entries.len()).ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "checkpoint benchmark commit count overflow",
                    )
                })?;
                pages = pages.checked_add(1).ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "checkpoint benchmark page count overflow",
                    )
                })?;
                let Some(next) = batch.next_start_after else {
                    break;
                };
                start_after = Some(next.to_string());
            }
            Ok(CheckpointCommitScanBenchResult { commits, pages })
        }
    }
}

/// Diffs two tracked commits through the production historical reader.
///
/// This is compiled only with `storage-benches`; it intentionally provides a
/// narrow measurement bridge without expanding the normal engine API.
#[inline(never)]
pub async fn diff_tracked_commits_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    left_commit_id: &str,
    right_commit_id: &str,
) -> Result<TrackedHistoricalDiffBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(read);
    let left_has_durable_root = reader.has_durable_commit_root(left_commit_id).await?;
    let right_has_durable_root = reader.has_durable_commit_root(right_commit_id).await?;
    let entries = reader
        .diff_commits(
            left_commit_id,
            right_commit_id,
            &crate::tracked_state::TrackedStateDiffRequest::default(),
        )
        .await?
        .entries
        .len();
    Ok(TrackedHistoricalDiffBenchResult {
        entries,
        left_has_durable_root,
        right_has_durable_root,
    })
}

pub fn reset_binary_cas_write_accounting() {
    crate::binary_cas::metrics::reset_binary_cas_write_metrics();
}

pub fn binary_cas_write_accounting() -> BinaryCasWriteAccounting {
    let metrics = crate::binary_cas::metrics::binary_cas_write_metrics_snapshot();
    BinaryCasWriteAccounting {
        chunk_lookup_count: metrics.chunk_lookup_count,
        chunk_lookup_batch_count: metrics.chunk_lookup_batch_count,
        chunk_lookup_hit_count: metrics.chunk_lookup_hit_count,
        chunk_lookup_miss_count: metrics.chunk_lookup_miss_count,
        chunk_lookup_elapsed_ns: metrics.chunk_lookup_elapsed_ns,
        transaction_duplicate_chunk_count: metrics.transaction_duplicate_chunk_count,
    }
}

/// Writes one payload through the production binary CAS and commits the
/// resulting canonical write set. This is intentionally available only to
/// storage benchmarks so they can isolate CAS layout costs from SQL planning,
/// validation, tracked state, and changelog work.
pub async fn write_binary_cas_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    bytes: &[u8],
) -> Result<String, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut writes = storage.new_write_set();
    let receipt = crate::binary_cas::BinaryCasContext::new()
        .writer_skipping_existing_chunks(&read, &mut writes)
        .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(bytes.to_vec()))
        .await?;
    storage
        .commit_write_set(writes, WriteOptions::default())
        .await?;
    Ok(receipt.hash.to_hex())
}

/// Reads one payload through the production binary CAS. See
/// [`write_binary_cas_for_bench`] for why this feature-gated helper exists.
pub async fn read_binary_cas_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    hash_hex: &str,
) -> Result<Option<Vec<u8>>, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let hash = crate::binary_cas::BlobId::from_hex(hash_hex)?;
    let mut entries = crate::binary_cas::BinaryCasContext::new()
        .reader(read)
        .load_bytes_many(&[hash])
        .await?
        .into_vec();
    Ok(entries.pop().flatten())
}

pub(crate) fn record_transaction_rows_staged(count: usize) {
    TRANSACTION_ROWS_STAGED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_untracked_rows(count: usize) {
    TRANSACTION_UNTRACKED_ROWS.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_validation_branch() {
    TRANSACTION_VALIDATION_BRANCHS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_load() {
    TRANSACTION_SCHEMA_CATALOG_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_compile() {
    TRANSACTION_SCHEMA_CATALOG_COMPILES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_json_store_stage_bytes(hash: [u8; 32]) {
    JSON_STORE_STAGE_BYTES.fetch_add(hash.len() as u64, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug)]
pub struct StorageLayoutAccounting {
    pub space_id: u32,
    pub space: &'static str,
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinaryManifestLayoutAccounting {
    pub manifests: u64,
    pub encoded_bytes: u64,
    pub empty_manifests: u64,
    pub single_chunk_manifests: u64,
    pub chunked_manifests: u64,
    pub delta_manifests: u64,
}

/// One fully reconstructed binary-CAS value for offline physical-layout
/// experiments. This stays behind `storage-benches`: production callers must
/// address CAS values by hash instead of enumerating the repository.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryCasPayloadInventoryEntry {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
    pub encoded_manifest_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CurrentImageCasOracleAccounting {
    pub current_file_images: u64,
    pub retained_manifests: u64,
    pub removed_manifests: u64,
    pub current_cas_row_bytes: u64,
    pub retained_cas_row_bytes: u64,
    pub reclaimable_cas_row_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BinaryCasOwnerLayoutAccounting {
    pub owner: String,
    pub references: u64,
    pub manifests: u64,
    pub logical_bytes: u64,
    pub encoded_manifest_bytes: u64,
    pub empty_manifests: u64,
    pub single_chunk_manifests: u64,
    pub chunked_manifests: u64,
    pub delta_manifests: u64,
    pub chunk_values: u64,
    pub encoded_chunk_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitDeltaLayoutAccounting {
    pub commit_id: String,
    pub physical_key_bytes: u64,
    pub physical_value_bytes: u64,
    pub segment_count: usize,
    pub members: usize,
    pub authored_members: usize,
    pub selected_members: usize,
    pub selected_tombstones: usize,
    pub selected_direct_addresses: usize,
    pub selected_source_commits: usize,
    pub dominant_selected_source_members: usize,
}

pub async fn commit_delta_layout_accounting<R>(
    read: &R,
) -> Result<Vec<CommitDeltaLayoutAccounting>, crate::LixError>
where
    R: StorageAdapterRead,
{
    let inventory = crate::tracked_state::scan_commit_delta_inventory(read).await?;
    let mut physical_bytes_by_commit =
        std::collections::BTreeMap::<crate::changelog::CommitId, (u64, u64)>::new();
    for space in [
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    ] {
        for entry in scan_layout_entries(read, space).await {
            let commit_id_bytes: [u8; 16] = entry
                .key
                .0
                .get(..16)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "benchmark commit-delta key has no commit UUID",
                    )
                })?;
            let commit_id =
                crate::changelog::CommitId::new(uuid::Uuid::from_bytes(commit_id_bytes));
            let physical = physical_bytes_by_commit.entry(commit_id).or_default();
            physical.0 += entry.key.0.len() as u64 + 4;
            physical.1 += match entry.value {
                StorageProjectedValue::KeyOnly => 0,
                StorageProjectedValue::FullValue(value) => value.len() as u64,
            };
        }
    }
    let locator_entries = scan_layout_entries(
        read,
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
    )
    .await;
    let mut locator_commit_by_change_id = std::collections::BTreeMap::new();
    for entry in locator_entries {
        let change_id_bytes: [u8; 16] = entry.key.0.as_ref().try_into().map_err(|_| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "benchmark change locator key is not one UUID",
            )
        })?;
        let change_id = crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(change_id_bytes));
        let StorageProjectedValue::FullValue(value) = entry.value else {
            unreachable!("change locator layout scan requests full values");
        };
        let locator = crate::tracked_state::decode_change_locator(change_id, &value)?;
        locator_commit_by_change_id.insert(change_id, locator.commit_id);
    }
    let authored_commit_by_change_id = inventory
        .commits
        .iter()
        .flat_map(|(commit_id, entry)| {
            entry
                .members
                .iter()
                .filter(|member| member.authored)
                .map(|member| (member.value.change_id, *commit_id))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    Ok(inventory
        .commits
        .into_iter()
        .map(|(commit_id, entry)| {
            let authored_members = entry
                .members
                .iter()
                .filter(|member| member.authored)
                .count();
            let selected_members = entry
                .members
                .iter()
                .filter(|member| member.is_selected_payload_ref())
                .count();
            let selected_tombstones = entry
                .members
                .len()
                .saturating_sub(authored_members)
                .saturating_sub(selected_members);
            let selected_direct_addresses = entry
                .members
                .iter()
                .filter(|member| {
                    member.is_selected_payload_ref()
                        && crate::tracked_state::direct_change_locator(member.value.change_id)
                            .is_some()
                })
                .count();
            let mut selected_members_by_source = std::collections::BTreeMap::<_, usize>::new();
            for member in entry
                .members
                .iter()
                .filter(|member| member.is_selected_payload_ref())
            {
                if let Some(source_commit_id) = authored_commit_by_change_id
                    .get(&member.value.change_id)
                    .copied()
                    .or_else(|| {
                        crate::tracked_state::direct_change_locator(member.value.change_id)
                            .map(|locator| locator.commit_id)
                    })
                    .or_else(|| {
                        locator_commit_by_change_id
                            .get(&member.value.change_id)
                            .copied()
                    })
                {
                    *selected_members_by_source
                        .entry(source_commit_id)
                        .or_default() += 1;
                }
            }
            CommitDeltaLayoutAccounting {
                commit_id: commit_id.to_string(),
                physical_key_bytes: physical_bytes_by_commit
                    .get(&commit_id)
                    .map_or(0, |bytes| bytes.0),
                physical_value_bytes: physical_bytes_by_commit
                    .get(&commit_id)
                    .map_or(0, |bytes| bytes.1),
                segment_count: entry.segment_count,
                members: entry.members.len(),
                authored_members,
                selected_members,
                selected_tombstones,
                selected_direct_addresses,
                selected_source_commits: selected_members_by_source.len(),
                dominant_selected_source_members: selected_members_by_source
                    .values()
                    .copied()
                    .max()
                    .unwrap_or_default(),
            }
        })
        .collect())
}

pub(crate) async fn commit_write_set_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    writes: StorageWriteSet,
) -> Result<crate::storage_adapter::StorageWriteSetStats, StorageWriteSetError>
where
    StorageImpl: Storage,
{
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await?;
    Ok(stats)
}

pub async fn layout_accounting<R>(read: &R) -> Vec<StorageLayoutAccounting>
where
    R: StorageAdapterRead,
{
    let mut accounting = Vec::with_capacity(native_storage_spaces().len());
    for space in native_storage_spaces() {
        accounting.push(scan_layout_space(read, *space).await);
    }
    accounting
}

pub async fn binary_manifest_layout_accounting<R>(
    read: &R,
) -> Result<BinaryManifestLayoutAccounting, crate::LixError>
where
    R: StorageAdapterRead,
{
    let entries = scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE).await;
    let mut accounting = BinaryManifestLayoutAccounting::default();
    for entry in entries {
        let StorageProjectedValue::FullValue(value) = entry.value else {
            unreachable!("binary manifest layout scan requests full values");
        };
        accounting.manifests += 1;
        accounting.encoded_bytes += value.len() as u64;
        match crate::binary_cas::decode_binary_cas_manifest(&value)? {
            crate::binary_cas::BinaryCasManifest::Empty { .. } => {
                accounting.empty_manifests += 1;
            }
            crate::binary_cas::BinaryCasManifest::SingleChunk { .. } => {
                accounting.single_chunk_manifests += 1;
            }
            crate::binary_cas::BinaryCasManifest::Chunked { .. } => {
                accounting.chunked_manifests += 1;
            }
            crate::binary_cas::BinaryCasManifest::Delta { .. } => {
                accounting.delta_manifests += 1;
            }
        }
    }
    Ok(accounting)
}

/// Reconstructs every unique binary-CAS payload through the production read
/// path. The bounded batches make the oracle usable for company-sized replay
/// fixtures without turning one inventory into an unbounded point-read plan.
pub async fn binary_cas_payload_inventory<R>(
    read: &R,
) -> Result<Vec<BinaryCasPayloadInventoryEntry>, crate::LixError>
where
    R: StorageAdapterRead,
{
    const BATCH_SIZE: usize = 256;

    let manifests =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE).await;
    let mut entries = Vec::with_capacity(manifests.len());
    for batch in manifests.chunks(BATCH_SIZE) {
        let hashes = batch
            .iter()
            .map(|entry| {
                let hash: [u8; 32] = entry.key.0.as_ref().try_into().map_err(|_| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "benchmark binary CAS manifest key is not one hash",
                    )
                })?;
                Ok(crate::binary_cas::BlobId::from_bytes(hash))
            })
            .collect::<Result<Vec<_>, crate::LixError>>()?;
        let payloads = crate::binary_cas::load_bytes_many(read, &hashes)
            .await?
            .into_vec();
        for ((manifest, hash), payload) in batch.iter().zip(hashes).zip(payloads) {
            let StorageProjectedValue::FullValue(encoded_manifest) = &manifest.value else {
                unreachable!("binary CAS payload inventory requests full manifests");
            };
            let bytes = payload.ok_or_else(|| {
                crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "benchmark binary CAS manifest '{}' has no payload",
                        hash.to_hex()
                    ),
                )
            })?;
            entries.push(BinaryCasPayloadInventoryEntry {
                hash: hash.into_bytes(),
                bytes,
                encoded_manifest_bytes: encoded_manifest.len() as u64,
            });
        }
    }
    Ok(entries)
}

/// Computes the exact logical CAS rows required by a current-image layout.
///
/// Current file images and binary/unclassified values remain ordinary CAS
/// payloads. Superseded payloads eligible for the catch-all WASM text plugin
/// are reconstructible from semantic history and may be removed. Dependency
/// traversal retains shared chunks, chunk manifests, delta bases, and presence
/// rows, so the result does not count unreachable manifest bytes as payload
/// savings.
pub async fn current_image_cas_oracle_accounting<R>(
    read: &R,
) -> Result<CurrentImageCasOracleAccounting, crate::LixError>
where
    R: StorageAdapterRead,
{
    use crate::live_state::LiveStateScanRequest;

    let live_state = crate::live_state::LiveStateContext::new(
        crate::tracked_state::TrackedStateContext::new(),
        crate::commit_graph::CommitGraphContext::new(),
    );
    let current_rows = live_state
        .reader(read)
        .scan_batch(&LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                schema_keys: vec!["lix_binary_blob_ref".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    let mut current_file_hashes = std::collections::BTreeSet::new();
    for row in current_rows.iter() {
        let Some(snapshot) = row.snapshot_content() else {
            continue;
        };
        let value: serde_json::Value =
            serde_json::from_str(snapshot.as_str()).map_err(|error| {
                crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    format!("current-image oracle found invalid blob reference: {error}"),
                )
            })?;
        let Some(hash) = value.get("blob_hash").and_then(serde_json::Value::as_str) else {
            continue;
        };
        current_file_hashes.insert(crate::binary_cas::BlobId::from_hex(hash)?);
    }

    let payloads = binary_cas_payload_inventory(read).await?;
    let mut retained_blobs = std::collections::BTreeSet::new();
    for payload in &payloads {
        let hash = crate::binary_cas::BlobId::from_bytes(payload.hash);
        let plugin_selectable = !payload.bytes.iter().take(8_000).any(|byte| *byte == 0);
        if current_file_hashes.contains(&hash) || !plugin_selectable {
            retained_blobs.insert(hash);
        }
    }

    let manifest_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE).await;
    let manifest_chunk_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE).await;
    let chunk_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE).await;
    let presence_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_CHUNK_PRESENCE_SPACE).await;

    let mut manifest_chunks = std::collections::BTreeMap::<
        crate::binary_cas::BlobId,
        Vec<(crate::binary_cas::BlobId, u64)>,
    >::new();
    for entry in &manifest_chunk_entries {
        let blob_hash = hash_from_key_prefix(&entry.key.0, "manifest chunk")?;
        let StorageProjectedValue::FullValue(value) = &entry.value else {
            unreachable!("current-image oracle requests full manifest chunks");
        };
        let (chunk_hash, _) = crate::binary_cas::decode_binary_cas_manifest_chunk(value)?;
        manifest_chunks.entry(blob_hash).or_default().push((
            crate::binary_cas::BlobId::from_bytes(chunk_hash),
            storage_entry_bytes(entry),
        ));
    }

    let mut retained_chunks = std::collections::BTreeSet::new();
    let mut retained_manifest_chunk_owners = std::collections::BTreeSet::new();
    let mut retained_manifest_bytes = 0u64;
    for entry in &manifest_entries {
        let blob_hash = hash_from_key_prefix(&entry.key.0, "manifest")?;
        if !retained_blobs.contains(&blob_hash) {
            continue;
        }
        retained_manifest_bytes += storage_entry_bytes(entry);
        let StorageProjectedValue::FullValue(value) = &entry.value else {
            unreachable!("current-image oracle requests full manifests");
        };
        match crate::binary_cas::decode_binary_cas_manifest(value)? {
            crate::binary_cas::BinaryCasManifest::Empty { .. } => {}
            crate::binary_cas::BinaryCasManifest::SingleChunk { chunk_hash, .. } => {
                retained_chunks.insert(crate::binary_cas::BlobId::from_bytes(chunk_hash));
            }
            crate::binary_cas::BinaryCasManifest::Chunked { .. } => {
                retained_manifest_chunk_owners.insert(blob_hash);
                retained_chunks.extend(
                    manifest_chunks
                        .get(&blob_hash)
                        .into_iter()
                        .flatten()
                        .map(|(hash, _)| *hash),
                );
            }
            crate::binary_cas::BinaryCasManifest::Delta {
                base_blob_hash,
                base_layout,
                ..
            } => match base_layout {
                crate::binary_cas::StorageBinaryCasDeltaBaseLayout::SingleChunk { chunk_hash } => {
                    retained_chunks.insert(crate::binary_cas::BlobId::from_bytes(chunk_hash));
                }
                crate::binary_cas::StorageBinaryCasDeltaBaseLayout::Chunked { .. } => {
                    let base_hash = crate::binary_cas::BlobId::from_bytes(base_blob_hash);
                    retained_manifest_chunk_owners.insert(base_hash);
                    retained_chunks.extend(
                        manifest_chunks
                            .get(&base_hash)
                            .into_iter()
                            .flatten()
                            .map(|(hash, _)| *hash),
                    );
                }
            },
        }
    }
    let retained_manifest_chunk_bytes = retained_manifest_chunk_owners
        .iter()
        .flat_map(|hash| manifest_chunks.get(hash).into_iter().flatten())
        .map(|(_, bytes)| *bytes)
        .sum::<u64>();
    let mut retained_chunk_bytes = 0u64;
    for entry in &chunk_entries {
        let hash = hash_from_key_prefix(&entry.key.0, "chunk")?;
        if retained_chunks.contains(&hash) {
            retained_chunk_bytes += storage_entry_bytes(entry);
        }
    }
    let mut retained_presence_bytes = 0u64;
    for entry in &presence_entries {
        let hash = hash_from_key_prefix(&entry.key.0, "chunk presence")?;
        if retained_chunks.contains(&hash) {
            retained_presence_bytes += storage_entry_bytes(entry);
        }
    }
    let current_cas_row_bytes = manifest_entries
        .iter()
        .chain(manifest_chunk_entries.iter())
        .chain(chunk_entries.iter())
        .chain(presence_entries.iter())
        .map(storage_entry_bytes)
        .sum::<u64>();
    let retained_cas_row_bytes = retained_manifest_bytes
        + retained_manifest_chunk_bytes
        + retained_chunk_bytes
        + retained_presence_bytes;
    Ok(CurrentImageCasOracleAccounting {
        current_file_images: current_file_hashes.len() as u64,
        retained_manifests: retained_blobs.len() as u64,
        removed_manifests: payloads.len().saturating_sub(retained_blobs.len()) as u64,
        current_cas_row_bytes,
        retained_cas_row_bytes,
        reclaimable_cas_row_bytes: current_cas_row_bytes.saturating_sub(retained_cas_row_bytes),
    })
}

fn hash_from_key_prefix(
    key: &Bytes,
    label: &str,
) -> Result<crate::binary_cas::BlobId, crate::LixError> {
    let hash: [u8; 32] = key
        .get(..32)
        .ok_or_else(|| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                format!("current-image oracle {label} key is shorter than one hash"),
            )
        })?
        .try_into()
        .expect("checked hash slice length");
    Ok(crate::binary_cas::BlobId::from_bytes(hash))
}

fn storage_entry_bytes(entry: &crate::storage_adapter::StorageReadEntry) -> u64 {
    4 + entry.key.0.len() as u64
        + match &entry.value {
            StorageProjectedValue::FullValue(value) => value.len() as u64,
            StorageProjectedValue::KeyOnly => 0,
        }
}

/// Attributes every binary-CAS manifest to the durable JSON field which owns it.
///
/// This benchmark-only inventory walks decoded commit deltas instead of raw
/// storage values, so adapter compression and packed history do not hide CAS
/// references. A final `unowned` row makes missing ownership explicit.
pub async fn binary_cas_owner_layout_accounting<R>(
    read: &R,
) -> Result<Vec<BinaryCasOwnerLayoutAccounting>, crate::LixError>
where
    R: StorageAdapterRead,
{
    use crate::json_store::JsonSlot;

    let inventory = crate::tracked_state::scan_commit_delta_inventory(read).await?;
    let mut seen_changes = std::collections::BTreeSet::new();
    let mut references = std::collections::BTreeMap::<String, u64>::new();
    let mut owners = std::collections::BTreeMap::<crate::binary_cas::BlobId, String>::new();
    let mut json_ref_hashes = std::collections::BTreeSet::new();
    for member in inventory
        .commits
        .values()
        .flat_map(|entry| entry.members.iter())
    {
        if !seen_changes.insert(member.change.change_id) {
            continue;
        }
        match &member.change.snapshot {
            JsonSlot::Inline(snapshot) => {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(snapshot) {
                    collect_binary_cas_json_owners(&value, &mut references, &mut owners);
                }
            }
            JsonSlot::Ref(json_ref) => {
                json_ref_hashes.insert(*json_ref.as_hash_array());
            }
            JsonSlot::None => {}
        }
    }
    let json_refs = json_ref_hashes
        .into_iter()
        .map(crate::json_store::JsonRef::from_hash_bytes)
        .collect::<Vec<_>>();
    let loaded = crate::json_store::JsonStoreContext::new()
        .load_bytes_many(
            read,
            crate::json_store::JsonLoadRequestRef {
                refs: &json_refs,
                scope: crate::json_store::JsonReadScopeRef::OutOfBand,
            },
        )
        .await?;
    for value in loaded.into_values().into_iter().flatten() {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&value) {
            collect_binary_cas_json_owners(&value, &mut references, &mut owners);
        }
    }

    let manifest_chunk_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE).await;
    let mut manifest_chunks = std::collections::BTreeMap::<
        crate::binary_cas::BlobId,
        Vec<crate::binary_cas::BlobId>,
    >::new();
    for entry in manifest_chunk_entries {
        let blob_hash: [u8; 32] = entry
            .key
            .0
            .get(..32)
            .ok_or_else(|| {
                crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    "benchmark binary CAS manifest-chunk key is too short",
                )
            })?
            .try_into()
            .expect("manifest-chunk hash slice is 32 bytes");
        let StorageProjectedValue::FullValue(value) = entry.value else {
            unreachable!("binary manifest-chunk owner scan requests full values");
        };
        let (chunk_hash, _) = crate::binary_cas::decode_binary_cas_manifest_chunk(&value)?;
        manifest_chunks
            .entry(crate::binary_cas::BlobId::from_bytes(blob_hash))
            .or_default()
            .push(crate::binary_cas::BlobId::from_bytes(chunk_hash));
    }

    let entries = scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE).await;
    let mut accounting =
        std::collections::BTreeMap::<String, BinaryCasOwnerLayoutAccounting>::new();
    let mut chunk_owners = std::collections::BTreeMap::<
        crate::binary_cas::BlobId,
        std::collections::BTreeSet<String>,
    >::new();
    let mut blob_chunks = std::collections::BTreeMap::<
        crate::binary_cas::BlobId,
        Vec<crate::binary_cas::BlobId>,
    >::new();
    let mut delta_bases = Vec::new();
    for entry in entries {
        let hash_bytes: [u8; 32] = entry.key.0.as_ref().try_into().map_err(|_| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "benchmark binary CAS manifest key is not one hash",
            )
        })?;
        let owner = owners
            .get(&crate::binary_cas::BlobId::from_bytes(hash_bytes))
            .cloned()
            .unwrap_or_else(|| "unowned".to_owned());
        let StorageProjectedValue::FullValue(value) = entry.value else {
            unreachable!("binary manifest owner scan requests full values");
        };
        let manifest = crate::binary_cas::decode_binary_cas_manifest(&value)?;
        let reference_count = references.get(&owner).copied().unwrap_or_default();
        let row =
            accounting
                .entry(owner.clone())
                .or_insert_with(|| BinaryCasOwnerLayoutAccounting {
                    owner: owner.clone(),
                    references: reference_count,
                    ..Default::default()
                });
        row.manifests += 1;
        row.logical_bytes += manifest.size_bytes();
        row.encoded_manifest_bytes += value.len() as u64;
        match manifest {
            crate::binary_cas::BinaryCasManifest::Empty { .. } => row.empty_manifests += 1,
            crate::binary_cas::BinaryCasManifest::SingleChunk { chunk_hash, .. } => {
                row.single_chunk_manifests += 1;
                let chunk_hash = crate::binary_cas::BlobId::from_bytes(chunk_hash);
                blob_chunks.insert(
                    crate::binary_cas::BlobId::from_bytes(hash_bytes),
                    vec![chunk_hash],
                );
                chunk_owners.entry(chunk_hash).or_default().insert(owner);
            }
            crate::binary_cas::BinaryCasManifest::Chunked { .. } => {
                row.chunked_manifests += 1;
                let blob_hash = crate::binary_cas::BlobId::from_bytes(hash_bytes);
                let chunks = manifest_chunks.get(&blob_hash).cloned().unwrap_or_default();
                for chunk_hash in &chunks {
                    chunk_owners
                        .entry(*chunk_hash)
                        .or_default()
                        .insert(owner.clone());
                }
                blob_chunks.insert(blob_hash, chunks);
            }
            crate::binary_cas::BinaryCasManifest::Delta { base_blob_hash, .. } => {
                row.delta_manifests += 1;
                delta_bases.push((owner, crate::binary_cas::BlobId::from_bytes(base_blob_hash)));
            }
        }
    }
    for (owner, base_blob_hash) in delta_bases {
        if let Some(chunks) = blob_chunks.get(&base_blob_hash) {
            for chunk_hash in chunks {
                chunk_owners
                    .entry(*chunk_hash)
                    .or_default()
                    .insert(owner.clone());
            }
        }
    }
    let chunk_entries =
        scan_layout_entries(read, crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE).await;
    for entry in chunk_entries {
        let chunk_hash: [u8; 32] = entry.key.0.as_ref().try_into().map_err(|_| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "benchmark binary CAS chunk key is not one hash",
            )
        })?;
        let StorageProjectedValue::FullValue(value) = entry.value else {
            unreachable!("binary chunk owner scan requests full values");
        };
        let owners = chunk_owners.get(&crate::binary_cas::BlobId::from_bytes(chunk_hash));
        let owner = match owners {
            None => "unowned_chunk".to_owned(),
            Some(owners) if owners.len() == 1 => owners.first().expect("one chunk owner").clone(),
            Some(owners) => format!(
                "shared_chunk:{}",
                owners.iter().cloned().collect::<Vec<_>>().join("+")
            ),
        };
        let row =
            accounting
                .entry(owner.clone())
                .or_insert_with(|| BinaryCasOwnerLayoutAccounting {
                    owner,
                    ..Default::default()
                });
        row.chunk_values += 1;
        row.encoded_chunk_bytes += value.len() as u64;
    }
    Ok(accounting.into_values().collect())
}

fn collect_binary_cas_json_owners(
    value: &serde_json::Value,
    references: &mut std::collections::BTreeMap<String, u64>,
    owners: &mut std::collections::BTreeMap<crate::binary_cas::BlobId, String>,
) {
    match value {
        serde_json::Value::Object(object) => {
            for (field, value) in object {
                if field.ends_with("hash") {
                    if let Some(value) = value.as_str() {
                        if let Ok(hash) = crate::binary_cas::BlobId::from_hex(value) {
                            let owner = match field.as_str() {
                                "blob_hash" => "file_blob",
                                "plugin_state_checkpoint_hash" => "plugin_runtime_checkpoint",
                                "plugin_authority_checkpoint_hash" => "plugin_authority_checkpoint",
                                "wasm_blob_hash" => "plugin_wasm",
                                _ => field,
                            }
                            .to_owned();
                            *references.entry(owner.clone()).or_default() += 1;
                            owners.entry(hash).or_insert(owner);
                        }
                    }
                }
                collect_binary_cas_json_owners(value, references, owners);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_binary_cas_json_owners(value, references, owners);
            }
        }
        _ => {}
    }
}

/// Per-row (key, value bytes) inventory of one space.
///
/// Equivalence tests compare these inventories byte-for-byte, so the scan
/// must be complete; the function asserts it observed every row.
pub async fn space_inventory<R>(read: &R, space_name: &str) -> Vec<(Vec<u8>, Vec<u8>)>
where
    R: StorageAdapterRead,
{
    let space = *native_storage_spaces()
        .iter()
        .find(|space| space.name == space_name)
        .expect("space name should exist");
    scan_layout_entries(read, space)
        .await
        .iter()
        .map(|entry| {
            (
                entry.key.0.to_vec(),
                match &entry.value {
                    StorageProjectedValue::KeyOnly => Vec::new(),
                    StorageProjectedValue::FullValue(value) => value.to_vec(),
                },
            )
        })
        .collect()
}

/// Physical storage-space IDs and names without scanning their contents.
///
/// Offline SST profiling uses this catalog to attribute blocks from databases
/// that may predate the current logical codecs.
pub fn layout_space_catalog() -> Vec<(u32, &'static str)> {
    native_storage_spaces()
        .iter()
        .map(|space| (space.id.0, space.name))
        .collect()
}

fn native_storage_spaces() -> &'static [crate::storage_adapter::StorageSpace] {
    &[
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        crate::live_state::HOT_ROW_SPACE,
        crate::live_state::HOT_FILE_SPACE,
        crate::live_state::HOT_DIFF_SPACE,
        crate::live_state::PACKED_CURRENT_BASE_CONTROL_SPACE,
        crate::live_state::PACKED_CURRENT_BASE_SPACE,
        crate::live_state::PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
        crate::live_state::ROOT_CURRENT_BASE_SPACE,
        crate::live_state::TRACKED_WORKING_DIFF_MARKER_SPACE,
        crate::live_state::CERTIFIED_ENTITY_BATCH_SPACE,
        crate::live_state::CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
        crate::live_state::CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
        crate::transaction::plugin_checkpoint::PLUGIN_CHECKPOINT_SPACE,
        crate::json_store::store::JSON_SPACE,
        crate::json_store::UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_PRESENCE_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::changelog::CHANGE_SPACE,
        crate::changelog::COMMIT_CHANGE_ID_SPACE,
    ]
}

async fn scan_layout_space<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> StorageLayoutAccounting
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut accounting = StorageLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: 0,
        key_bytes: 0,
        value_bytes: 0,
    };
    let mut resume_after = None;
    loop {
        let result = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after,
                    ..StorageScanOptions::default()
                },
            )
            .await
            .expect("scan complete storage bench layout space");
        let has_more = result.value.has_more;
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());
        for entry in result.value.entries {
            accounting.rows = accounting
                .rows
                .checked_add(1)
                .expect("storage layout row count should not overflow");
            accounting.key_bytes = accounting
                .key_bytes
                .checked_add(entry.key.0.len() as u64 + 4)
                .expect("storage layout key bytes should not overflow");
            accounting.value_bytes = accounting
                .value_bytes
                .checked_add(match entry.value {
                    StorageProjectedValue::KeyOnly => 0,
                    StorageProjectedValue::FullValue(value) => value.len() as u64,
                })
                .expect("storage layout value bytes should not overflow");
        }
        if !has_more {
            return accounting;
        }
        assert!(
            resume_after.is_some(),
            "storage scan reported more rows without a resume key"
        );
    }
}

async fn scan_layout_entries<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> Vec<crate::storage_adapter::StorageReadEntry>
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut entries = Vec::new();
    let mut resume_after = None;
    loop {
        let result = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after,
                    ..StorageScanOptions::default()
                },
            )
            .await
            .expect("scan complete storage bench layout space");
        let has_more = result.value.has_more;
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());
        entries.extend(result.value.entries);
        if !has_more {
            return entries;
        }
        assert!(
            resume_after.is_some(),
            "storage scan reported more rows without a resume key"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointCommitScanBenchMode, binary_manifest_layout_accounting,
        plan_repository_gc_for_bench, scan_checkpoint_commits_for_bench,
    };
    use crate::Engine;
    use crate::changelog::bench::{append_ordered_commits, stage_append_once};
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageKey, StorageValue, StorageWriteOptions,
    };

    #[tokio::test]
    async fn checkpoint_commit_scan_baseline_matches_materialized_records_across_pages() {
        let storage = Memory::new();
        let append = append_ordered_commits(0, 1_025).expect("build commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage commit fixture");
        let adapter = StorageAdapter::new(storage);

        let materialized =
            scan_checkpoint_commits_for_bench(&adapter, CheckpointCommitScanBenchMode::Materialize)
                .await
                .expect("materialize checkpoint commit records");
        let streamed =
            scan_checkpoint_commits_for_bench(&adapter, CheckpointCommitScanBenchMode::Stream)
                .await
                .expect("stream checkpoint commit records");

        assert_eq!(materialized.commits, 1_025);
        assert_eq!(materialized.pages, 2);
        assert_eq!(streamed, materialized);
    }

    #[tokio::test]
    async fn streamed_layout_accounting_matches_full_space_inventory() {
        let storage = Memory::new();
        let append = append_ordered_commits(0, 1_025).expect("build commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage commit fixture");
        let adapter = StorageAdapter::new(storage);
        let read = adapter
            .begin_read(crate::ReadOptions::default())
            .await
            .expect("begin layout accounting read");

        let inventory = super::space_inventory(&read, crate::changelog::COMMIT_SPACE.name).await;
        let accounting = super::layout_accounting(&read)
            .await
            .into_iter()
            .find(|space| space.space == crate::changelog::COMMIT_SPACE.name)
            .expect("commit space is accounted");
        assert_eq!(accounting.rows, inventory.len() as u64);
        assert_eq!(
            accounting.key_bytes,
            inventory
                .iter()
                .map(|(key, _)| key.len() as u64 + 4)
                .sum::<u64>()
        );
        assert_eq!(
            accounting.value_bytes,
            inventory
                .iter()
                .map(|(_, value)| value.len() as u64)
                .sum::<u64>()
        );
    }
    #[tokio::test]
    async fn binary_manifest_accounting_handles_out_of_line_layouts() {
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        for (key_byte, manifest) in [
            (
                1,
                crate::binary_cas::BinaryCasManifest::Empty { size_bytes: 0 },
            ),
            (
                2,
                crate::binary_cas::BinaryCasManifest::SingleChunk {
                    size_bytes: 7,
                    chunk_hash: [3; 32],
                },
            ),
            (
                3,
                crate::binary_cas::BinaryCasManifest::Chunked {
                    size_bytes: 42,
                    chunk_count: 2,
                },
            ),
        ] {
            writes.put(
                crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
                StorageKey(bytes::Bytes::from(vec![key_byte; 32])),
                StorageValue {
                    bytes: bytes::Bytes::from(crate::binary_cas::encode_binary_cas_manifest(
                        &manifest,
                    )),
                },
            );
        }
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("manifest fixtures should commit");
        let read = adapter
            .begin_read(crate::ReadOptions::default())
            .await
            .expect("begin manifest accounting read");
        let accounting = binary_manifest_layout_accounting(&read)
            .await
            .expect("manifest accounting should succeed");
        assert_eq!(accounting.manifests, 3);
        assert_eq!(accounting.empty_manifests, 1);
        assert_eq!(accounting.single_chunk_manifests, 1);
        assert_eq!(accounting.chunked_manifests, 1);
    }

    #[tokio::test]
    async fn repository_gc_benchmark_plans_unreachable_nodes_without_mutating() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("initialize engine");
        let append = append_ordered_commits(100, 10).expect("build unreachable commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage unreachable commit fixture");
        let adapter = StorageAdapter::new(storage);

        let first = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("plan repository gc");
        let second = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("repeat repository gc plan");

        assert_eq!(first.swept_commits, 10);
        // Each unreachable commit removes its changelog projection and the
        // unified commit-state authority. The hard cut retired the separate
        // tracked-root record.
        assert_eq!(first.staged_deletes, 20);
        assert_eq!(first.key_shared_buffers, 2);
        assert_eq!(first.key_shared_bytes, 20 * 16);
        assert_eq!(second.swept_commits, first.swept_commits);
        assert_eq!(second.staged_deletes, first.staged_deletes);
    }
}
