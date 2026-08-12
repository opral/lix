use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

use crate::storage::{ReadOptions, WriteOptions};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    StorageAdapter, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StoragePrefix, StorageProjectedValue, StorageWriteOptions, StorageWriteSet,
    StorageWriteSetError,
};

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
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: crate::tracked_state::CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
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
static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_CERTIFICATIONS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_HITS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ROWS: AtomicU64 = AtomicU64::new(0);
static ROOT_BASE_BATCH_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static ROOT_BASE_BATCH_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static ENTITY_POINT_SNAPSHOT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static ENTITY_POINT_SNAPSHOT_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_PUTS: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_DELETES: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_WRITTEN_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_COMMIT_STATE_MANIFEST_BYTES: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_SCOPED_RANGE_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_SCOPED_RANGE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_SCOPED_RANGE_HITS: AtomicU64 = AtomicU64::new(0);
static CRUD_CURRENT_STATE_SCOPED_RANGE_ERRORS: AtomicU64 = AtomicU64::new(0);
static CRUD_SEALED_MANIFEST_LOADS: AtomicU64 = AtomicU64::new(0);
static CRUD_REPLAY_MANIFEST_LOADS: AtomicU64 = AtomicU64::new(0);
static CRUD_ORDERED_DELTA_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static COMMIT_DELTA_DIRECT_SEGMENTS: AtomicU64 = AtomicU64::new(0);
static COMMIT_DELTA_DIRECT_ROWS: AtomicU64 = AtomicU64::new(0);
static COMMIT_DELTA_GENERIC_SEGMENTS: AtomicU64 = AtomicU64::new(0);
static COMMIT_DELTA_GENERIC_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_MANIFEST_LEAF_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_SUMMARIZED_CHUNK_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_CHUNK_PAYLOAD_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
static IMMUTABLE_SEGMENT_IDENTITY_HASH_BYTES: AtomicU64 = AtomicU64::new(0);

/// Matched transaction ownership counters used by the CRUD profile.  These
/// counters are deliberately disabled unless the profile enables them, so the
/// common instrumentation does not perturb normal engine execution.
pub const CRUD_OWNERSHIP_STAGE_COUNT: usize = 15;
pub const CRUD_OWNERSHIP_METRIC_COUNT: usize = 6;
pub const CRUD_OWNERSHIP_SQL_BOUND: usize = 0;
pub const CRUD_OWNERSHIP_RAW_BATCH: usize = 1;
pub const CRUD_OWNERSHIP_RAW_TRANSFER: usize = 2;
pub const CRUD_OWNERSHIP_PREPARED_BATCH: usize = 3;
pub const CRUD_OWNERSHIP_PREPARED_CLONE: usize = 4;
pub const CRUD_OWNERSHIP_REPLACEMENT_INPUT: usize = 5;
pub const CRUD_OWNERSHIP_REPLACEMENT_PART: usize = 6;
pub const CRUD_OWNERSHIP_AUTHORITY: usize = 7;
pub const CRUD_OWNERSHIP_ROOT_PUBLICATION: usize = 8;
pub const CRUD_OWNERSHIP_WRITE_SET: usize = 9;
pub const CRUD_OWNERSHIP_ADAPTER: usize = 10;
pub const CRUD_OWNERSHIP_MUTATION_JOURNAL: usize = 11;
pub const CRUD_OWNERSHIP_IDENTITY_ENCODING: usize = 12;
pub const CRUD_OWNERSHIP_NORMALIZATION: usize = 13;
pub const CRUD_OWNERSHIP_JOURNAL_SEAL: usize = 14;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudOwnershipMetric {
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub vec_entries: u64,
    pub string_entries: u64,
    pub map_entries: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudOwnershipAccounting {
    pub stages: [CrudOwnershipMetric; CRUD_OWNERSHIP_STAGE_COUNT],
    pub transfers: [CrudOwnershipTransferMetric; CRUD_OWNERSHIP_STAGE_COUNT],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudOwnershipTransferMetric {
    pub created_bytes: u64,
    pub cloned_bytes: u64,
    pub retained_bytes: u64,
    pub dropped_bytes: u64,
}

static CRUD_OWNERSHIP_ENABLED: AtomicBool = AtomicBool::new(false);
static CRUD_OWNERSHIP_COUNTERS: [AtomicU64;
    CRUD_OWNERSHIP_STAGE_COUNT * CRUD_OWNERSHIP_METRIC_COUNT] =
    [const { AtomicU64::new(0) }; CRUD_OWNERSHIP_STAGE_COUNT * CRUD_OWNERSHIP_METRIC_COUNT];
static CRUD_OWNERSHIP_TRANSFER_COUNTERS: [AtomicU64; CRUD_OWNERSHIP_STAGE_COUNT * 4] =
    [const { AtomicU64::new(0) }; CRUD_OWNERSHIP_STAGE_COUNT * 4];

/// Starts a matched operation-local ownership measurement and clears any
/// counters left by a prior profile operation.
pub fn begin_crud_ownership_accounting() {
    for counter in &CRUD_OWNERSHIP_COUNTERS {
        counter.store(0, Ordering::Relaxed);
    }
    for counter in &CRUD_OWNERSHIP_TRANSFER_COUNTERS {
        counter.store(0, Ordering::Relaxed);
    }
    CRUD_OWNERSHIP_ENABLED.store(true, Ordering::Relaxed);
}

pub(crate) fn record_crud_ownership_transfer(
    stage: usize,
    created_bytes: usize,
    cloned_bytes: usize,
    retained_bytes: usize,
    dropped_bytes: usize,
) {
    if !CRUD_OWNERSHIP_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    assert!(
        stage < CRUD_OWNERSHIP_STAGE_COUNT,
        "invalid ownership stage"
    );
    let values = [created_bytes, cloned_bytes, retained_bytes, dropped_bytes];
    let start = stage * 4;
    for (offset, value) in values.into_iter().enumerate() {
        CRUD_OWNERSHIP_TRANSFER_COUNTERS[start + offset].fetch_add(value as u64, Ordering::Relaxed);
    }
}

pub(crate) fn record_crud_ownership(
    stage: usize,
    rows: usize,
    key_bytes: usize,
    value_bytes: usize,
    vec_entries: usize,
    string_entries: usize,
    map_entries: usize,
) {
    if !CRUD_OWNERSHIP_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    assert!(
        stage < CRUD_OWNERSHIP_STAGE_COUNT,
        "invalid ownership stage"
    );
    let values = [
        rows,
        key_bytes,
        value_bytes,
        vec_entries,
        string_entries,
        map_entries,
    ];
    let start = stage * CRUD_OWNERSHIP_METRIC_COUNT;
    for (offset, value) in values.into_iter().enumerate() {
        CRUD_OWNERSHIP_COUNTERS[start + offset].fetch_add(value as u64, Ordering::Relaxed);
    }
}

pub fn take_crud_ownership_accounting() -> CrudOwnershipAccounting {
    CRUD_OWNERSHIP_ENABLED.store(false, Ordering::Relaxed);
    let mut stages = [CrudOwnershipMetric::default(); CRUD_OWNERSHIP_STAGE_COUNT];
    let mut transfers = [CrudOwnershipTransferMetric::default(); CRUD_OWNERSHIP_STAGE_COUNT];
    for (stage, metric) in stages.iter_mut().enumerate() {
        let start = stage * CRUD_OWNERSHIP_METRIC_COUNT;
        metric.rows = CRUD_OWNERSHIP_COUNTERS[start].swap(0, Ordering::Relaxed);
        metric.key_bytes = CRUD_OWNERSHIP_COUNTERS[start + 1].swap(0, Ordering::Relaxed);
        metric.value_bytes = CRUD_OWNERSHIP_COUNTERS[start + 2].swap(0, Ordering::Relaxed);
        metric.vec_entries = CRUD_OWNERSHIP_COUNTERS[start + 3].swap(0, Ordering::Relaxed);
        metric.string_entries = CRUD_OWNERSHIP_COUNTERS[start + 4].swap(0, Ordering::Relaxed);
        metric.map_entries = CRUD_OWNERSHIP_COUNTERS[start + 5].swap(0, Ordering::Relaxed);
        let transfer_start = stage * 4;
        transfers[stage].created_bytes =
            CRUD_OWNERSHIP_TRANSFER_COUNTERS[transfer_start].swap(0, Ordering::Relaxed);
        transfers[stage].cloned_bytes =
            CRUD_OWNERSHIP_TRANSFER_COUNTERS[transfer_start + 1].swap(0, Ordering::Relaxed);
        transfers[stage].retained_bytes =
            CRUD_OWNERSHIP_TRANSFER_COUNTERS[transfer_start + 2].swap(0, Ordering::Relaxed);
        transfers[stage].dropped_bytes =
            CRUD_OWNERSHIP_TRANSFER_COUNTERS[transfer_start + 3].swap(0, Ordering::Relaxed);
    }
    CrudOwnershipAccounting { stages, transfers }
}

pub(crate) fn record_crud_write_set_arena(writes: &StorageWriteSet) {
    let stats = writes.arena_stats();
    record_crud_ownership(
        CRUD_OWNERSHIP_WRITE_SET,
        stats
            .put_descriptors
            .saturating_add(stats.delete_descriptors),
        stats
            .key_inline_bytes
            .saturating_add(stats.key_shared_bytes),
        stats
            .value_inline_bytes
            .saturating_add(stats.value_shared_bytes),
        stats
            .put_descriptors
            .saturating_add(stats.delete_descriptors),
        stats
            .key_shared_buffers
            .saturating_add(stats.value_shared_buffers),
        stats.spaces,
    );
}

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

pub(crate) fn record_certified_entity_insert_parameter_batch_certification() {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_CERTIFICATIONS.fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CertifiedEntityInsertParameterBatchCounters {
    pub certifications: u64,
    pub executions: u64,
}

/// Reads the cumulative certified parameter-batch INSERT phase counters
/// without resetting them. Callers measuring one fixture/sample must subtract
/// a pre-operation snapshot from a post-operation snapshot.
pub fn certified_entity_insert_parameter_batch_counters()
-> CertifiedEntityInsertParameterBatchCounters {
    CertifiedEntityInsertParameterBatchCounters {
        certifications: CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_CERTIFICATIONS
            .load(Ordering::Relaxed),
        executions: CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_certified_entity_insert_parameter_batch_execution() {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Returns and resets the number of certified parameter-batch INSERT routes
/// that reached physical staging/execution.
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

pub(crate) fn record_root_base_batch_cache_hit() {
    ROOT_BASE_BATCH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_root_base_batch_cache_miss() {
    ROOT_BASE_BATCH_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

/// Hits and misses since the last call. A rotated generation that is scanned
/// repeatedly must show hits; zero hits means the serving cache is not
/// connected to the lane under test, which is not visible in a timing sweep.
pub fn take_root_base_batch_cache_accounting() -> (u64, u64) {
    (
        ROOT_BASE_BATCH_CACHE_HITS.swap(0, Ordering::Relaxed),
        ROOT_BASE_BATCH_CACHE_MISSES.swap(0, Ordering::Relaxed),
    )
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

pub(crate) fn record_crud_current_state_scoped_range_fallback() {
    CRUD_CURRENT_STATE_SCOPED_RANGE_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

pub fn take_crud_current_state_scoped_range_fallbacks() -> u64 {
    CRUD_CURRENT_STATE_SCOPED_RANGE_FALLBACKS.swap(0, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrudCurrentStateScopedRangeAccounting {
    pub attempts: u64,
    pub hits: u64,
    pub errors: u64,
    pub sealed_manifest_loads: u64,
    pub replay_manifest_loads: u64,
    pub ordered_delta_fallbacks: u64,
    pub commit_delta_direct_segments: u64,
    pub commit_delta_direct_rows: u64,
    pub commit_delta_generic_segments: u64,
    pub commit_delta_generic_rows: u64,
}

pub(crate) fn record_commit_delta_leaf_layout(rows: usize, direct: bool) {
    let (segments, encoded_rows) = if direct {
        (&COMMIT_DELTA_DIRECT_SEGMENTS, &COMMIT_DELTA_DIRECT_ROWS)
    } else {
        (&COMMIT_DELTA_GENERIC_SEGMENTS, &COMMIT_DELTA_GENERIC_ROWS)
    };
    segments.fetch_add(1, Ordering::Relaxed);
    encoded_rows.fetch_add(rows as u64, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_scoped_range_attempt() {
    CRUD_CURRENT_STATE_SCOPED_RANGE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_scoped_range_hit() {
    CRUD_CURRENT_STATE_SCOPED_RANGE_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_crud_current_state_scoped_range_error() {
    CRUD_CURRENT_STATE_SCOPED_RANGE_ERRORS.fetch_add(1, Ordering::Relaxed);
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

pub fn take_crud_current_state_scoped_range_accounting() -> CrudCurrentStateScopedRangeAccounting {
    CrudCurrentStateScopedRangeAccounting {
        attempts: CRUD_CURRENT_STATE_SCOPED_RANGE_ATTEMPTS.swap(0, Ordering::Relaxed),
        hits: CRUD_CURRENT_STATE_SCOPED_RANGE_HITS.swap(0, Ordering::Relaxed),
        errors: CRUD_CURRENT_STATE_SCOPED_RANGE_ERRORS.swap(0, Ordering::Relaxed),
        sealed_manifest_loads: CRUD_SEALED_MANIFEST_LOADS.swap(0, Ordering::Relaxed),
        replay_manifest_loads: CRUD_REPLAY_MANIFEST_LOADS.swap(0, Ordering::Relaxed),
        ordered_delta_fallbacks: CRUD_ORDERED_DELTA_FALLBACKS.swap(0, Ordering::Relaxed),
        commit_delta_direct_segments: COMMIT_DELTA_DIRECT_SEGMENTS.swap(0, Ordering::Relaxed),
        commit_delta_direct_rows: COMMIT_DELTA_DIRECT_ROWS.swap(0, Ordering::Relaxed),
        commit_delta_generic_segments: COMMIT_DELTA_GENERIC_SEGMENTS.swap(0, Ordering::Relaxed),
        commit_delta_generic_rows: COMMIT_DELTA_GENERIC_ROWS.swap(0, Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------
// Commit-root replay accounting (experiment AA).
//
// Answers "how many ancestor commits does one root-materialization boundary
// replay, and where does the per-replayed-commit cost go". The coarse counters
// tick once per boundary/plan and are always compiled under `storage-benches`.
// The per-node cost attribution is behind `root-replay-trace` so no timing A/B
// ever pays for an `Instant::now()` inside `hash_bytes`.
// ---------------------------------------------------------------------------

static ROOT_REPLAY_BOUNDARIES: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_PLANS_LOADED: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_PLANS_STAGED: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_AVAILABLE_ROOT_PROBES: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_AVAILABLE_ROOT_HITS: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_MAX_PLANS: AtomicU64 = AtomicU64::new(0);

static ROOT_REPLAY_PLAN_LOAD_NANOS: AtomicU64 = AtomicU64::new(0);
static ROOT_REPLAY_STAGE_NANOS: AtomicU64 = AtomicU64::new(0);

/// Per-boundary replay-set sizes, in boundary order.
static ROOT_REPLAY_PLAN_HISTOGRAM: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RootReplayAccounting {
    /// Distinct durable rootless parents that forced a replay.
    pub boundaries: u64,
    /// Total rebuild plans returned by the nearest-available-root walk.
    pub plans_loaded: u64,
    /// Plans actually replayed through the tracked-state root writer.
    pub plans_staged: u64,
    pub available_root_probes: u64,
    pub available_root_hits: u64,
    pub max_plans_in_one_boundary: u64,
    pub plan_load_nanos: u64,
    pub stage_nanos: u64,
    /// Replay-set size per boundary, in boundary order.
    pub plans_per_boundary: Vec<u64>,
}

pub(crate) fn record_root_replay_boundary(plans: usize) {
    ROOT_REPLAY_BOUNDARIES.fetch_add(1, Ordering::Relaxed);
    ROOT_REPLAY_PLANS_LOADED.fetch_add(plans as u64, Ordering::Relaxed);
    ROOT_REPLAY_MAX_PLANS.fetch_max(plans as u64, Ordering::Relaxed);
    if let Ok(mut histogram) = ROOT_REPLAY_PLAN_HISTOGRAM.lock() {
        histogram.push(plans as u64);
    }
}

pub(crate) fn record_root_replay_plan_staged() {
    ROOT_REPLAY_PLANS_STAGED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_root_replay_available_root_probe(hit: bool) {
    ROOT_REPLAY_AVAILABLE_ROOT_PROBES.fetch_add(1, Ordering::Relaxed);
    if hit {
        ROOT_REPLAY_AVAILABLE_ROOT_HITS.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_root_replay_plan_load_nanos(nanos: u64) {
    ROOT_REPLAY_PLAN_LOAD_NANOS.fetch_add(nanos, Ordering::Relaxed);
}

pub(crate) fn record_root_replay_stage_nanos(nanos: u64) {
    ROOT_REPLAY_STAGE_NANOS.fetch_add(nanos, Ordering::Relaxed);
}

pub fn take_root_replay_accounting() -> RootReplayAccounting {
    RootReplayAccounting {
        boundaries: ROOT_REPLAY_BOUNDARIES.swap(0, Ordering::Relaxed),
        plans_loaded: ROOT_REPLAY_PLANS_LOADED.swap(0, Ordering::Relaxed),
        plans_staged: ROOT_REPLAY_PLANS_STAGED.swap(0, Ordering::Relaxed),
        available_root_probes: ROOT_REPLAY_AVAILABLE_ROOT_PROBES.swap(0, Ordering::Relaxed),
        available_root_hits: ROOT_REPLAY_AVAILABLE_ROOT_HITS.swap(0, Ordering::Relaxed),
        max_plans_in_one_boundary: ROOT_REPLAY_MAX_PLANS.swap(0, Ordering::Relaxed),
        plan_load_nanos: ROOT_REPLAY_PLAN_LOAD_NANOS.swap(0, Ordering::Relaxed),
        stage_nanos: ROOT_REPLAY_STAGE_NANOS.swap(0, Ordering::Relaxed),
        plans_per_boundary: ROOT_REPLAY_PLAN_HISTOGRAM
            .lock()
            .map(|mut histogram| std::mem::take(&mut *histogram))
            .unwrap_or_default(),
    }
}

/// Per-node cost attribution for replayed commits.
///
/// Every bucket records `(in_replay, total)` so a run can say what fraction of
/// all tracked-state tree CPU is spent inside commit-root replay rather than on
/// the commit's own mutations.
#[cfg(feature = "root-replay-trace")]
mod replay_trace {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};

    macro_rules! bucket {
        ($replay_ns:ident, $total_ns:ident, $replay_bytes:ident, $total_bytes:ident,
         $replay_count:ident, $total_count:ident, $record:ident) => {
            static $replay_ns: AtomicU64 = AtomicU64::new(0);
            static $total_ns: AtomicU64 = AtomicU64::new(0);
            static $replay_bytes: AtomicU64 = AtomicU64::new(0);
            static $total_bytes: AtomicU64 = AtomicU64::new(0);
            static $replay_count: AtomicU64 = AtomicU64::new(0);
            static $total_count: AtomicU64 = AtomicU64::new(0);

            pub(crate) fn $record(nanos: u64, bytes: u64) {
                $total_ns.fetch_add(nanos, Ordering::Relaxed);
                $total_bytes.fetch_add(bytes, Ordering::Relaxed);
                $total_count.fetch_add(1, Ordering::Relaxed);
                if in_replay() {
                    $replay_ns.fetch_add(nanos, Ordering::Relaxed);
                    $replay_bytes.fetch_add(bytes, Ordering::Relaxed);
                    $replay_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        };
    }

    thread_local! {
        static REPLAY_DEPTH: Cell<u32> = const { Cell::new(0) };
    }

    pub(crate) fn in_replay() -> bool {
        REPLAY_DEPTH.with(|depth| depth.get() > 0)
    }

    pub(crate) fn enter() {
        REPLAY_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    }

    pub(crate) fn exit() {
        REPLAY_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }

    bucket!(
        READ_NS,
        READ_TOTAL_NS,
        READ_BYTES,
        READ_TOTAL_BYTES,
        READ_COUNT,
        READ_TOTAL_COUNT,
        record_chunk_read
    );
    bucket!(
        DECODE_NS,
        DECODE_TOTAL_NS,
        DECODE_BYTES,
        DECODE_TOTAL_BYTES,
        DECODE_COUNT,
        DECODE_TOTAL_COUNT,
        record_node_decode
    );
    bucket!(
        ENCODE_NS,
        ENCODE_TOTAL_NS,
        ENCODE_BYTES,
        ENCODE_TOTAL_BYTES,
        ENCODE_COUNT,
        ENCODE_TOTAL_COUNT,
        record_node_encode
    );
    bucket!(
        HASH_NS,
        HASH_TOTAL_NS,
        HASH_BYTES,
        HASH_TOTAL_BYTES,
        HASH_COUNT,
        HASH_TOTAL_COUNT,
        record_node_hash
    );

    pub(crate) fn take() -> super::RootReplayCostAttribution {
        let bucket = |ns: &AtomicU64,
                      total_ns: &AtomicU64,
                      bytes: &AtomicU64,
                      total_bytes: &AtomicU64,
                      count: &AtomicU64,
                      total_count: &AtomicU64| {
            super::RootReplayCostBucket {
                replay_nanos: ns.swap(0, Ordering::Relaxed),
                total_nanos: total_ns.swap(0, Ordering::Relaxed),
                replay_bytes: bytes.swap(0, Ordering::Relaxed),
                total_bytes: total_bytes.swap(0, Ordering::Relaxed),
                replay_count: count.swap(0, Ordering::Relaxed),
                total_count: total_count.swap(0, Ordering::Relaxed),
            }
        };
        super::RootReplayCostAttribution {
            storage_read: bucket(
                &READ_NS,
                &READ_TOTAL_NS,
                &READ_BYTES,
                &READ_TOTAL_BYTES,
                &READ_COUNT,
                &READ_TOTAL_COUNT,
            ),
            decode: bucket(
                &DECODE_NS,
                &DECODE_TOTAL_NS,
                &DECODE_BYTES,
                &DECODE_TOTAL_BYTES,
                &DECODE_COUNT,
                &DECODE_TOTAL_COUNT,
            ),
            encode: bucket(
                &ENCODE_NS,
                &ENCODE_TOTAL_NS,
                &ENCODE_BYTES,
                &ENCODE_TOTAL_BYTES,
                &ENCODE_COUNT,
                &ENCODE_TOTAL_COUNT,
            ),
            hash: bucket(
                &HASH_NS,
                &HASH_TOTAL_NS,
                &HASH_BYTES,
                &HASH_TOTAL_BYTES,
                &HASH_COUNT,
                &HASH_TOTAL_COUNT,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RootReplayCostBucket {
    pub replay_nanos: u64,
    pub total_nanos: u64,
    pub replay_bytes: u64,
    pub total_bytes: u64,
    pub replay_count: u64,
    pub total_count: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RootReplayCostAttribution {
    pub storage_read: RootReplayCostBucket,
    pub decode: RootReplayCostBucket,
    pub encode: RootReplayCostBucket,
    pub hash: RootReplayCostBucket,
}

/// True when this build carries the per-node replay cost attribution.
pub fn root_replay_trace_enabled() -> bool {
    cfg!(feature = "root-replay-trace")
}

pub fn take_root_replay_cost_attribution() -> RootReplayCostAttribution {
    #[cfg(feature = "root-replay-trace")]
    {
        replay_trace::take()
    }
    #[cfg(not(feature = "root-replay-trace"))]
    {
        RootReplayCostAttribution::default()
    }
}

/// Marks the dynamic extent of one commit-root replay for cost attribution.
pub(crate) struct RootReplayScope;

impl RootReplayScope {
    pub(crate) fn enter() -> Self {
        #[cfg(feature = "root-replay-trace")]
        replay_trace::enter();
        Self
    }
}

impl Drop for RootReplayScope {
    fn drop(&mut self) {
        #[cfg(feature = "root-replay-trace")]
        replay_trace::exit();
    }
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_replay_chunk_read(nanos: u64, bytes: u64) {
    replay_trace::record_chunk_read(nanos, bytes);
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_replay_node_decode(nanos: u64, bytes: u64) {
    replay_trace::record_node_decode(nanos, bytes);
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_replay_node_encode(nanos: u64, bytes: u64) {
    replay_trace::record_node_encode(nanos, bytes);
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_replay_node_hash(nanos: u64, bytes: u64) {
    replay_trace::record_node_hash(nanos, bytes);
}

// ---------------------------------------------------------------------------
// Plan-load phase attribution (experiment AB).
//
// Splits one commit-root rebuild plan load into named phases and, for every
// phase, separates the time spent inside the storage adapter's `get_many`
// boundary (I/O) from everything else (decode + allocation + setup). Also
// counts physical read batches and keys per phase, so "how many physical reads
// does one plan load issue" is answered by a counter rather than a guess.
//
// Gated behind `root-replay-trace` exactly like the AA attribution, so no A/B
// timing build pays for an `Instant::now()` inside `get_many`.
// ---------------------------------------------------------------------------

/// Named phases of one commit-root rebuild plan load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanLoadPhase {
    /// Everything outside a plan load.
    Other = 0,
    /// `load_available_root` — the bounded durable-root availability probe.
    AvailProbe = 1,
    /// `ChangelogReader::load_commits` for the one replayed commit.
    CommitRecord = 2,
    /// `load_point_replay_commit_state` — commit-state header + inventory.
    ReplayState = 3,
    /// Mutation-directory routing plus packed commit-delta segment reads and
    /// leaf decode.
    DeltaSegments = 4,
    /// Owned-key materialization of the decoded batch into plan deltas.
    Collect = 5,
    /// `commit_root_tree_is_readable` — the full tracked-state tree scan the
    /// availability probe runs to prove the addressed chunk closure is
    /// physically readable. Nested inside `AvailProbe`.
    AvailTreeScan = 6,
}

pub const PLAN_LOAD_PHASE_COUNT: usize = 7;

pub const PLAN_LOAD_PHASE_NAMES: [&str; PLAN_LOAD_PHASE_COUNT] = [
    "other",
    "avail_probe",
    "commit_record",
    "replay_state",
    "delta_segments",
    "collect",
    "avail_tree_scan",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanLoadPhaseMetric {
    /// Wall time inside the phase guard.
    pub wall_nanos: u64,
    /// Wall time inside `StorageAdapterRead::get_many` while in this phase.
    pub io_nanos: u64,
    /// `StorageAdapterRead::get_many` invocations issued while in this phase.
    pub read_calls: u64,
    /// Logical per-space requests inside those invocations.
    pub read_batches: u64,
    /// Keys requested across those batches.
    pub read_keys: u64,
    /// Keys that returned a value.
    pub read_hits: u64,
    /// Bytes returned by those batches.
    pub read_bytes: u64,
    /// Times the phase guard was entered.
    pub entries: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanLoadAttribution {
    pub phases: [PlanLoadPhaseMetric; PLAN_LOAD_PHASE_COUNT],
    /// Plan loads completed (one per replayed ancestor).
    pub plans: u64,
    /// Commit-delta members decoded across those plan loads.
    pub members_decoded: u64,
    /// Members kept in the returned plan.
    pub members_kept: u64,
    /// Bytes of packed commit-delta segment payload decoded.
    pub member_payload_bytes: u64,
}

/// True when this build carries the plan-load phase attribution.
pub fn plan_load_trace_enabled() -> bool {
    cfg!(feature = "root-replay-trace")
}

#[cfg(feature = "root-replay-trace")]
mod plan_load_trace {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{PLAN_LOAD_PHASE_COUNT, PlanLoadAttribution, PlanLoadPhase, PlanLoadPhaseMetric};

    const FIELDS: usize = 8;

    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);
    static COUNTERS: [AtomicU64; PLAN_LOAD_PHASE_COUNT * FIELDS] =
        [ZERO; PLAN_LOAD_PHASE_COUNT * FIELDS];
    static PLANS: AtomicU64 = AtomicU64::new(0);
    static MEMBERS_DECODED: AtomicU64 = AtomicU64::new(0);
    static MEMBERS_KEPT: AtomicU64 = AtomicU64::new(0);
    static MEMBER_PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);

    thread_local! {
        static PHASE: Cell<usize> = const { Cell::new(0) };
    }

    fn add(phase: usize, field: usize, value: u64) {
        COUNTERS[phase * FIELDS + field].fetch_add(value, Ordering::Relaxed);
    }

    pub(super) fn current_phase() -> usize {
        PHASE.with(Cell::get)
    }

    pub(super) fn set_phase(phase: usize) -> usize {
        PHASE.with(|slot| slot.replace(phase))
    }

    pub(super) fn record_phase_wall(phase: usize, nanos: u64) {
        add(phase, 0, nanos);
        add(phase, 6, 1);
    }

    pub(super) fn record_io(nanos: u64, batches: u64, keys: u64, hits: u64, bytes: u64) {
        let phase = current_phase();
        add(phase, 1, nanos);
        add(phase, 2, batches);
        add(phase, 3, keys);
        add(phase, 4, hits);
        add(phase, 5, bytes);
        add(phase, 7, 1);
    }

    pub(super) fn record_plan(members_decoded: u64, members_kept: u64, payload_bytes: u64) {
        PLANS.fetch_add(1, Ordering::Relaxed);
        MEMBERS_DECODED.fetch_add(members_decoded, Ordering::Relaxed);
        MEMBERS_KEPT.fetch_add(members_kept, Ordering::Relaxed);
        MEMBER_PAYLOAD_BYTES.fetch_add(payload_bytes, Ordering::Relaxed);
    }

    pub(super) fn take() -> PlanLoadAttribution {
        let mut phases = [PlanLoadPhaseMetric::default(); PLAN_LOAD_PHASE_COUNT];
        for (index, phase) in phases.iter_mut().enumerate() {
            let get = |field: usize| COUNTERS[index * FIELDS + field].swap(0, Ordering::Relaxed);
            *phase = PlanLoadPhaseMetric {
                wall_nanos: get(0),
                io_nanos: get(1),
                read_batches: get(2),
                read_keys: get(3),
                read_hits: get(4),
                read_bytes: get(5),
                entries: get(6),
                read_calls: get(7),
            };
        }
        PlanLoadAttribution {
            phases,
            plans: PLANS.swap(0, Ordering::Relaxed),
            members_decoded: MEMBERS_DECODED.swap(0, Ordering::Relaxed),
            members_kept: MEMBERS_KEPT.swap(0, Ordering::Relaxed),
            member_payload_bytes: MEMBER_PAYLOAD_BYTES.swap(0, Ordering::Relaxed),
        }
    }

    pub(super) fn phase_index(phase: PlanLoadPhase) -> usize {
        phase as usize
    }
}

pub fn take_plan_load_attribution() -> PlanLoadAttribution {
    #[cfg(feature = "root-replay-trace")]
    {
        plan_load_trace::take()
    }
    #[cfg(not(feature = "root-replay-trace"))]
    {
        PlanLoadAttribution::default()
    }
}

/// RAII guard marking the dynamic extent of one plan-load phase.
///
/// Phases nest: the guard restores the enclosing phase on drop, and wall time
/// is charged to the phase named by the guard (so an inner phase's time is
/// counted in both, exactly like a call-tree self/total split at one level).
pub(crate) struct PlanLoadPhaseScope {
    #[cfg(feature = "root-replay-trace")]
    phase: usize,
    #[cfg(feature = "root-replay-trace")]
    previous: usize,
    #[cfg(feature = "root-replay-trace")]
    start: std::time::Instant,
}

impl PlanLoadPhaseScope {
    #[allow(unused_variables)]
    pub(crate) fn enter(phase: PlanLoadPhase) -> Self {
        #[cfg(feature = "root-replay-trace")]
        {
            let phase = plan_load_trace::phase_index(phase);
            let previous = plan_load_trace::set_phase(phase);
            Self {
                phase,
                previous,
                start: std::time::Instant::now(),
            }
        }
        #[cfg(not(feature = "root-replay-trace"))]
        {
            Self {}
        }
    }
}

impl Drop for PlanLoadPhaseScope {
    fn drop(&mut self) {
        #[cfg(feature = "root-replay-trace")]
        {
            plan_load_trace::record_phase_wall(self.phase, self.start.elapsed().as_nanos() as u64);
            plan_load_trace::set_phase(self.previous);
        }
    }
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_plan_load_io(nanos: u64, batches: u64, keys: u64, hits: u64, bytes: u64) {
    plan_load_trace::record_io(nanos, batches, keys, hits, bytes);
}

#[cfg(feature = "root-replay-trace")]
pub(crate) fn record_plan_load_plan(members_decoded: u64, members_kept: u64, payload_bytes: u64) {
    plan_load_trace::record_plan(members_decoded, members_kept, payload_bytes);
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
    /// Whole reachable history for one member schema.
    HistoryFull,
    /// History restricted to the head commit (`lixcol_depth = 0`).
    HistoryDepth0,
    /// History for a bounded row demand (`LIMIT 10`).
    HistoryLimit10,
}

/// Row demand a bounded history benchmark mode asks for.
const COMMIT_GRAPH_BENCH_HISTORY_LIMIT: usize = 10;

/// Schema key that [`seed_commit_graph_members_for_bench`] writes per commit.
const COMMIT_GRAPH_BENCH_MEMBER_SCHEMA_KEY: &str = "commit_graph_bench_member";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitGraphBenchResult {
    pub nodes: usize,
    pub edges: usize,
    pub member_changes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeBaseBenchScenario {
    EqualHeads,
    AncestorDescendant,
    RecentFork,
    DeepFork,
    CrissCross,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeBaseBenchFixture {
    pub left_head: String,
    pub right_head: String,
    pub expected_base: Option<String>,
    pub commits: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MergePreparationBenchResult {
    pub base_commit_id: String,
    pub target_entries: usize,
    pub source_entries: usize,
}

/// Seeds an empty-state commit graph whose only varying dimension is ancestry.
///
/// The manifests deliberately contain no tracked mutations or durable roots so
/// merge preparation isolates topology discovery plus the equal-root diff fast
/// path. Commit generation and parent links remain the production authority.
pub async fn seed_merge_base_fixture_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    ancestry: usize,
    scenario: MergeBaseBenchScenario,
) -> Result<MergeBaseBenchFixture, crate::LixError>
where
    StorageImpl: Storage,
{
    if ancestry == 0 {
        return Err(crate::LixError::unknown(
            "merge-base benchmark ancestry must be positive",
        ));
    }
    let mut records = Vec::<crate::changelog::CommitRecord>::new();
    let mut generations = std::collections::HashMap::<crate::changelog::CommitId, u64>::new();
    let mut record_indices = std::collections::HashMap::<crate::changelog::CommitId, usize>::new();
    let scenario_name = match scenario {
        MergeBaseBenchScenario::EqualHeads => "equal",
        MergeBaseBenchScenario::AncestorDescendant => "ancestor",
        MergeBaseBenchScenario::RecentFork => "recent",
        MergeBaseBenchScenario::DeepFork => "deep",
        MergeBaseBenchScenario::CrissCross => "criss-cross",
    };
    let prefix = format!("merge-base-bench-{scenario_name}-{ancestry}");
    let mut append = |label: String,
                      parents: Vec<crate::changelog::CommitId>|
     -> Result<crate::changelog::CommitId, crate::LixError> {
        let generation = parents
            .iter()
            .map(|parent| {
                generations.get(parent).copied().ok_or_else(|| {
                    crate::LixError::unknown("merge-base benchmark parent is not seeded")
                })
            })
            .collect::<Result<Vec<u64>, _>>()?
            .into_iter()
            .max()
            .map_or(0, |generation| generation.saturating_add(1));
        let commit_id = crate::changelog::CommitId::for_test_label(&label);
        let parent = parents
            .as_slice()
            .first()
            .and_then(|parent_id| record_indices.get(parent_id))
            .map(|index| &records[*index]);
        let parent_jump = parent
            .and_then(|parent| record_indices.get(&parent.first_parent_jump_commit_id))
            .map(|index| &records[*index]);
        let (first_parent_jump_commit_id, first_parent_jump_span) =
            crate::changelog::next_first_parent_jump(commit_id, &parents, parent, parent_jump)?;
        records.push(crate::changelog::CommitRecord {
            format_version: 4,
            commit_id,
            generation,
            parent_commit_ids: parents,
            first_parent_jump_commit_id,
            first_parent_jump_span,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "merge-base benchmark timestamp",
                "2026-08-07T00:00:00Z",
            ),
        });
        generations.insert(commit_id, generation);
        record_indices.insert(commit_id, records.len() - 1);
        Ok(commit_id)
    };

    let root = append(format!("{prefix}-root"), Vec::new())?;
    let (left_head, right_head, expected_base) = match scenario {
        MergeBaseBenchScenario::EqualHeads => {
            let mut head = root;
            for index in 1..ancestry {
                head = append(format!("{prefix}-linear-{index}"), vec![head])?;
            }
            (head, head, Some(head))
        }
        MergeBaseBenchScenario::AncestorDescendant => {
            let mut head = root;
            for index in 0..ancestry {
                head = append(format!("{prefix}-descendant-{index}"), vec![head])?;
            }
            (root, head, Some(root))
        }
        MergeBaseBenchScenario::RecentFork => {
            let mut base = root;
            for index in 1..ancestry {
                base = append(format!("{prefix}-trunk-{index}"), vec![base])?;
            }
            let mut left = base;
            let mut right = base;
            for index in 0..8 {
                left = append(format!("{prefix}-left-{index}"), vec![left])?;
                right = append(format!("{prefix}-right-{index}"), vec![right])?;
            }
            (left, right, Some(base))
        }
        MergeBaseBenchScenario::DeepFork => {
            let mut left = root;
            let mut right = root;
            for index in 0..ancestry {
                left = append(format!("{prefix}-left-{index}"), vec![left])?;
                right = append(format!("{prefix}-right-{index}"), vec![right])?;
            }
            (left, right, Some(root))
        }
        MergeBaseBenchScenario::CrissCross => {
            let mut base = root;
            for index in 1..ancestry {
                base = append(format!("{prefix}-trunk-{index}"), vec![base])?;
            }
            let left = append(format!("{prefix}-left"), vec![base])?;
            let right = append(format!("{prefix}-right"), vec![base])?;
            let left_merge = append(format!("{prefix}-left-merge"), vec![left, right])?;
            let right_merge = append(format!("{prefix}-right-merge"), vec![right, left])?;
            (left_merge, right_merge, None)
        }
    };

    let mut read = storage.begin_read(ReadOptions::default()).await?;
    let mut writes = storage.new_write_set();
    crate::changelog::ChangelogWriter::stage_append(
        &mut crate::changelog::ChangelogContext::new().writer(&mut read, &mut writes),
        crate::changelog::ChangelogAppend {
            commits: records.clone(),
            changes: Vec::new(),
        },
    )
    .await?;
    for record in &records {
        crate::tracked_state::stage_commit_state_manifest(
            &mut writes,
            &crate::tracked_state::CommitStateManifest {
                commit_id: record.commit_id,
                change_account_id: record.account_id.clone(),
                replay_debt: crate::tracked_state::CommitStateReplayDebt {
                    depth: 1,
                    rows: 0,
                    bytes: 0,
                },
                mutations: crate::tracked_state::CommitStateMutationInventory::default(),
                touched_scope_filter: Default::default(),
                current_state_scoped_ranges: None,
                snapshot_root: None,
            },
        )?;
    }
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await?;
    Ok(MergeBaseBenchFixture {
        left_head: left_head.to_string(),
        right_head: right_head.to_string(),
        expected_base: expected_base.map(|commit_id| commit_id.to_string()),
        commits: records.len(),
    })
}

#[inline(never)]
pub async fn merge_base_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    left_commit_id: &str,
    right_commit_id: &str,
) -> Result<String, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut reader = crate::commit_graph::CommitGraphContext::new().reader(read);
    let left = crate::changelog::CommitId::parse_lix(left_commit_id, "merge benchmark left")?;
    let right = crate::changelog::CommitId::parse_lix(right_commit_id, "merge benchmark right")?;
    reader
        .merge_base(&left, &right)
        .await
        .map(|base| base.to_string())
}

#[inline(never)]
pub async fn prepare_merge_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    left_commit_id: &str,
    right_commit_id: &str,
) -> Result<MergePreparationBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let left = crate::changelog::CommitId::parse_lix(left_commit_id, "merge benchmark left")?;
    let right = crate::changelog::CommitId::parse_lix(right_commit_id, "merge benchmark right")?;
    let base = {
        let mut reader = crate::commit_graph::CommitGraphContext::new().reader(&read);
        reader.merge_base(&left, &right).await?
    };
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(&read);
    let analysis = crate::session::analyze_merge_for_bench(
        &mut reader,
        crate::session::MergeCommitsForBench {
            base_commit_id: base,
            target_commit_id: left,
            source_commit_id: right,
        },
    )
    .await?;
    Ok(MergePreparationBenchResult {
        base_commit_id: analysis.commits.base_commit_id.to_string(),
        target_entries: analysis.target_diff.entries.len(),
        source_entries: analysis.source_diff.entries.len(),
    })
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
    if let Some((max_depth, limit)) = match mode {
        CommitGraphBenchMode::HistoryFull => Some((None, None)),
        CommitGraphBenchMode::HistoryDepth0 => Some((Some(0), None)),
        CommitGraphBenchMode::HistoryLimit10 => {
            Some((None, Some(COMMIT_GRAPH_BENCH_HISTORY_LIMIT)))
        }
        _ => None,
    } {
        let history = reader
            .change_history_from_commit(
                &head_commit_id,
                &crate::commit_graph::CommitGraphChangeHistoryRequest {
                    entity_pks: Vec::new(),
                    schema_keys: vec![COMMIT_GRAPH_BENCH_MEMBER_SCHEMA_KEY.to_string()],
                    file_ids: Vec::new(),
                    min_depth: None,
                    max_depth,
                    include_tombstones: true,
                    limit,
                },
            )
            .await?;
        let entries = history.entries.len();
        std::hint::black_box(history);
        return Ok(CommitGraphBenchResult {
            nodes: 0,
            edges: 0,
            member_changes: entries,
        });
    }
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
        CommitGraphBenchMode::HistoryFull
        | CommitGraphBenchMode::HistoryDepth0
        | CommitGraphBenchMode::HistoryLimit10 => unreachable!("history modes returned above"),
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
                    schema_key: COMMIT_GRAPH_BENCH_MEMBER_SCHEMA_KEY,
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RepositoryGcBenchResult {
    pub live_commits: usize,
    pub swept_commits: usize,
    pub swept_standalone_changes: usize,
    pub standalone_swept_ids: Vec<String>,
    pub swept_payloads: usize,
    pub staged_puts: u64,
    pub staged_deletes: u64,
    /// Point-delete descriptors grouped by logical storage-space id.  This
    /// makes GC benchmark expectations resilient to additions of a new
    /// derived projection while still proving each authority lane explicitly.
    pub delete_counts_by_space: Vec<(u32, usize)>,
    pub deleted_commit_state_manifests: usize,
    pub deleted_mutation_inventories: usize,
    pub deleted_semantic_commit_projections: usize,
    pub deleted_semantic_change_rows: usize,
    pub staged_written_bytes: u64,
    pub delete_descriptors: usize,
    pub delete_descriptor_capacity: usize,
    pub key_inline_bytes: usize,
    pub key_inline_capacity: usize,
    pub key_shared_buffers: usize,
    pub key_shared_bytes: usize,
    pub key_shared_capacity: usize,
    pub reclaimed_generation_rows: u64,
    pub root_discovery_us: u64,
    pub changelog_us: u64,
    pub tracked_root_stage_us: u64,
    pub total_us: u64,
}

/// Isolates the branch-owned derived checkpoint retirement path.  This is a
/// benchmark-only bridge: the checkpoint rows are seeded directly into their
/// derived space, then the production branch-prefix retirement planner is
/// measured without SQL, commit-graph, or GC setup in the timed region.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BranchCheckpointDeleteBenchResult {
    pub matched_entries: usize,
    pub staged_deletes: u64,
    pub read_us: u64,
    pub total_us: u64,
    pub commit_us: u64,
    pub delete_descriptor_capacity: usize,
}

pub async fn seed_branch_plugin_checkpoints_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    branch_id: &str,
    file_count: usize,
    batch_size: usize,
) -> Result<(), crate::LixError>
where
    StorageImpl: Storage,
{
    let batch_size = batch_size.max(1);
    let branch = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INTERNAL_ERROR,
            format!("checkpoint benchmark branch id is not a UUID: {error}"),
        )
    })?;
    let generation = crate::binary_cas::BlobId::from_content(b"checkpoint-bench-generation");
    let blob_hash = crate::binary_cas::BlobId::from_content(b"checkpoint-bench-blob");
    let semantic_root = uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0004);
    let mut next = 0usize;
    while next < file_count {
        let end = next.saturating_add(batch_size).min(file_count);
        let mut writes = storage.new_write_set();
        for index in next..end {
            let file_id = uuid::Uuid::from_u128(
                0x0192_0000_0000_7000_8000_0000_1000_0000u128.saturating_add(index as u128),
            );
            crate::transaction::plugin_checkpoint::stage_current_plugin_checkpoint(
                &mut writes,
                &branch.to_string(),
                &file_id.to_string(),
                &generation.to_hex(),
                &semantic_root.to_string(),
                blob_hash,
                b"checkpoint-runtime",
                b"checkpoint-authority",
            )?;
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(crate::LixError::from)?;
        next = end;
    }
    Ok(())
}

/// Loads one checkpoint produced by [`seed_branch_plugin_checkpoints_for_bench`]
/// through the production decoder. This keeps corruption qualification on the
/// real persisted format without exposing checkpoint internals to applications.
pub async fn load_seeded_branch_plugin_checkpoint_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    branch_id: &str,
    file_id: &str,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, crate::LixError>
where
    StorageImpl: Storage,
{
    let generation = crate::binary_cas::BlobId::from_content(b"checkpoint-bench-generation");
    let blob_hash = crate::binary_cas::BlobId::from_content(b"checkpoint-bench-blob");
    let semantic_root = uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0004);
    let read = storage.begin_read(ReadOptions::default()).await?;
    crate::transaction::plugin_checkpoint::load_current_plugin_checkpoint(
        &read,
        branch_id,
        file_id,
        &generation.to_hex(),
        &semantic_root.to_string(),
        blob_hash,
    )
    .await
    .map(|checkpoint| {
        checkpoint.map(|checkpoint| {
            (
                checkpoint.runtime.as_ref().to_vec(),
                checkpoint.authority.as_ref().to_vec(),
            )
        })
    })
}

pub async fn delete_branch_plugin_checkpoints_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    branch_id: &str,
) -> Result<BranchCheckpointDeleteBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        storage.begin_read(ReadOptions::default()).await?,
    );
    let mut writes = storage.new_write_set();
    let started = std::time::Instant::now();
    crate::transaction::plugin_checkpoint::stage_delete_branch_plugin_checkpoints(
        &read,
        &mut writes,
        branch_id,
    )
    .await?;
    let total_us = started.elapsed().as_micros() as u64;
    let stats = writes.stats();
    let arena = writes.arena_stats();
    Ok(BranchCheckpointDeleteBenchResult {
        matched_entries: stats.staged_deletes as usize,
        staged_deletes: stats.staged_deletes,
        read_us: total_us,
        total_us,
        commit_us: 0,
        delete_descriptor_capacity: arena.delete_descriptor_capacity,
    })
}

pub async fn delete_and_commit_branch_plugin_checkpoints_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    branch_id: &str,
) -> Result<BranchCheckpointDeleteBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        storage.begin_read(ReadOptions::default()).await?,
    );
    let mut writes = storage.new_write_set();
    let started = std::time::Instant::now();
    crate::transaction::plugin_checkpoint::stage_delete_branch_plugin_checkpoints(
        &read,
        &mut writes,
        branch_id,
    )
    .await?;
    let read_us = started.elapsed().as_micros() as u64;
    let stats = writes.stats();
    let arena = writes.arena_stats();
    let commit_started = std::time::Instant::now();
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .map_err(crate::LixError::from)?;
    let commit_us = commit_started.elapsed().as_micros() as u64;
    Ok(BranchCheckpointDeleteBenchResult {
        matched_entries: stats.staged_deletes as usize,
        staged_deletes: stats.staged_deletes,
        read_us,
        total_us: read_us.saturating_add(commit_us),
        commit_us,
        delete_descriptor_capacity: arena.delete_descriptor_capacity,
    })
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
    let mut delete_counts_by_space: Vec<(u32, usize)> = writes
        .delete_counts_by_space()
        .into_iter()
        .map(|(space, count)| (space.id.0, count))
        .collect();
    delete_counts_by_space.sort_unstable_by_key(|(space_id, _)| *space_id);
    let delete_count = |space_id| {
        delete_counts_by_space
            .iter()
            .find_map(|(candidate, count)| (*candidate == space_id).then_some(*count))
            .unwrap_or_default()
    };
    let deleted_commit_state_manifests = delete_count(
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
            .id
            .0,
    );
    let deleted_mutation_inventories = delete_count(
        crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
            .id
            .0,
    );
    let deleted_semantic_commit_projections = delete_count(crate::changelog::COMMIT_SPACE.id.0);
    let deleted_semantic_change_rows = delete_count(crate::changelog::CHANGE_SPACE.id.0);
    Ok(RepositoryGcBenchResult {
        live_commits: plan.changelog.live.commits.len(),
        swept_commits: plan
            .changelog
            .sweep
            .commits
            .len()
            .saturating_add(plan.sweep.tracked_commit_roots.len()),
        swept_standalone_changes: plan
            .changelog
            .sweep
            .changes
            .len()
            .saturating_add(plan.sweep.standalone_changes.len()),
        standalone_swept_ids: plan
            .sweep
            .standalone_changes
            .iter()
            .map(ToString::to_string)
            .collect(),
        swept_payloads: plan.changelog.sweep.json_payloads.len(),
        staged_puts: stats.staged_puts,
        staged_deletes: stats.staged_deletes,
        delete_counts_by_space,
        deleted_commit_state_manifests,
        deleted_mutation_inventories,
        deleted_semantic_commit_projections,
        deleted_semantic_change_rows,
        staged_written_bytes: stats.written_bytes,
        delete_descriptors: arena.delete_descriptors,
        delete_descriptor_capacity: arena.delete_descriptor_capacity,
        key_inline_bytes: arena.key_inline_bytes,
        key_inline_capacity: arena.key_inline_capacity,
        key_shared_buffers: arena.key_shared_buffers,
        key_shared_bytes: arena.key_shared_bytes,
        key_shared_capacity: arena.key_shared_capacity,
        reclaimed_generation_rows: plan.sweep.reclaimed_generation_rows,
        root_discovery_us: plan.profile.root_discovery_us,
        changelog_us: plan.profile.changelog_us,
        tracked_root_stage_us: plan.profile.tracked_root_stage_us,
        total_us: plan.profile.total_us,
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepositoryGcCommitBenchResult {
    pub staged_deletes: u64,
    pub swept_commits: usize,
    pub reclaimed_generation_rows: u64,
    pub reclaimed_manifest_rows: usize,
    pub reclaimed_manifest_chunk_rows: usize,
    pub reclaimed_chunk_rows: usize,
    pub plan_us: u64,
    pub commit_us: u64,
}

/// Commits one production GC pass for the dual-adapter qualification lane.
/// The helper exposes only maintenance accounting; it does not add a second
/// reclamation implementation. Because benchmark callers operate below the
/// session write gate, a concurrently spawned checkpoint sweep can win the
/// authenticated publication fences. Retry that ordinary conflict from a new
/// snapshot; all failed planning and commit work remains visible in whole-cell
/// resource measurements.
pub async fn collect_repository_gc_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
) -> Result<RepositoryGcCommitBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    const MAX_CONFLICT_ATTEMPTS: usize = 8;
    for attempt in 0..MAX_CONFLICT_ATTEMPTS {
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage.begin_read(ReadOptions::default()).await?,
        );
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let started = std::time::Instant::now();
        let plan = crate::gc::stage_repository_gc_with_preconditions(
            read,
            &mut writes,
            &mut preconditions,
        )
        .await?;
        let plan_us = started.elapsed().as_micros() as u64;
        let stats = writes.stats();
        let binary_cas = plan.sweep.binary_cas.clone();
        let commit_started = std::time::Instant::now();
        match storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
        {
            Ok(_) => {
                return Ok(RepositoryGcCommitBenchResult {
                    staged_deletes: stats.staged_deletes,
                    reclaimed_generation_rows: plan.sweep.reclaimed_generation_rows,
                    swept_commits: plan
                        .changelog
                        .sweep
                        .commits
                        .len()
                        .saturating_add(plan.sweep.tracked_commit_roots.len()),
                    reclaimed_manifest_rows: binary_cas.reclaimed_manifest_rows,
                    reclaimed_manifest_chunk_rows: binary_cas.reclaimed_manifest_chunk_rows,
                    reclaimed_chunk_rows: binary_cas.reclaimed_chunk_rows,
                    plan_us,
                    commit_us: commit_started.elapsed().as_micros() as u64,
                });
            }
            Err(StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::WriteConflict
                | crate::storage_adapter::StorageError::PreconditionFailed(_),
            )) if attempt + 1 < MAX_CONFLICT_ATTEMPTS => tokio::task::yield_now().await,
            Err(error) => return Err(crate::LixError::from(error)),
        }
    }
    unreachable!("bounded repository GC conflict loop must return")
}

/// Audits standalone semantic facts for the GC benchmark without adding the
/// scan to the measured planner phase. The exact IDs and their authenticated
/// control reason make the old-vs-frontier sweep discrepancy explicit.
pub async fn audit_repository_gc_standalone_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
) -> Result<Vec<String>, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        storage.begin_read(ReadOptions::default()).await?,
    );
    crate::gc::audit_repository_gc_standalone_refs(&read).await
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

/// Reports whether one semantic commit currently owns an authenticated
/// durable tracked-state root. This keeps layout admission observable to
/// storage benchmarks without expanding the production engine API.
pub async fn has_durable_commit_root_for_bench<StorageImpl>(
    storage: StorageImpl,
    commit_id: &str,
) -> Result<bool, crate::LixError>
where
    StorageImpl: Storage,
{
    let adapter = StorageAdapter::new(storage);
    let read = adapter.begin_read(ReadOptions::default()).await?;
    let reader = crate::tracked_state::TrackedStateContext::new().reader(read);
    reader.has_durable_commit_root(commit_id).await
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
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

/// Exact value-level duplication for one storage space.
///
/// `duplicate_value_bytes` is what a perfect content-addressed store would not
/// have had to write: for every distinct value byte string that occurs `n`
/// times, `(n - 1) * len`. It is an upper bound on the win from content
/// addressing that plane, because it ignores whatever indirection a real
/// content-addressed layout would have to add.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageValueDuplication {
    pub space_id: u32,
    pub space: &'static str,
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    /// Distinct value byte strings in the space.
    pub distinct_values: u64,
    /// Rows whose value byte string also occurs on at least one other row.
    pub duplicate_rows: u64,
    pub duplicate_value_bytes: u64,
    /// Largest number of rows sharing one value byte string.
    pub max_occurrences: u64,
}

impl StorageValueDuplication {
    /// Share of the space's value bytes a perfect CAS would have elided.
    pub fn duplicate_fraction(&self) -> f64 {
        if self.value_bytes == 0 {
            0.0
        } else {
            self.duplicate_value_bytes as f64 / self.value_bytes as f64
        }
    }
}

/// Byte-exact duplication for every native storage space.
pub async fn space_value_duplication<R>(read: &R) -> Vec<StorageValueDuplication>
where
    R: StorageAdapterRead,
{
    let mut accounting = Vec::with_capacity(native_storage_spaces().len());
    for space in native_storage_spaces() {
        accounting.push(scan_space_value_duplication(read, *space).await);
    }
    accounting
}

async fn scan_space_value_duplication<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> StorageValueDuplication
where
    R: StorageAdapterRead,
{
    let mut accounting = StorageValueDuplication {
        space_id: space.id.0,
        space: space.name,
        ..StorageValueDuplication::default()
    };
    let mut occurrences = std::collections::HashMap::<[u8; 32], (u64, u64)>::new();
    for entry in scan_layout_entries(read, space).await {
        accounting.rows += 1;
        accounting.key_bytes += entry.key.0.len() as u64 + 4;
        let StorageProjectedValue::FullValue(value) = entry.value else {
            continue;
        };
        accounting.value_bytes += value.len() as u64;
        let digest = *blake3::hash(&value).as_bytes();
        let slot = occurrences.entry(digest).or_insert((0, value.len() as u64));
        slot.0 += 1;
    }
    accounting.distinct_values = occurrences.len() as u64;
    for (count, len) in occurrences.into_values() {
        accounting.max_occurrences = accounting.max_occurrences.max(count);
        if count > 1 {
            accounting.duplicate_rows += count - 1;
            accounting.duplicate_value_bytes += (count - 1) * len;
        }
    }
    accounting
}

/// Nearest-neighbour analysis of the commit-delta segment plane.
///
/// Byte-exact duplication answers "would a naive CAS dedup this". This answers
/// the follow-up: when two segments are *not* byte-identical, how far apart are
/// they? Two equal-length segments differing in a handful of bytes mean the
/// payload carries per-commit identity (commit id, timestamps) that a redesign
/// could hoist out; two segments differing in most of their bytes mean the
/// content genuinely differs and no format change would help.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitDeltaSegmentSimilarity {
    pub segments: u64,
    pub distinct_values: u64,
    /// Distinct values that share their byte length with another distinct value.
    pub same_length_distinct_values: u64,
    /// Compared pairs of equal-length distinct values.
    pub compared_pairs: u64,
    /// Pairs differing in at most 1% of their bytes.
    pub near_identical_pairs: u64,
    /// Smallest positive byte-difference count over all compared pairs.
    pub min_differing_bytes: u64,
    /// Byte length of the pair that produced `min_differing_bytes`.
    pub min_differing_pair_len: u64,
    /// Shared leading bytes of the pair that produced `min_differing_bytes`.
    pub min_differing_pair_common_prefix: u64,
    /// Shared trailing bytes of the same pair. A long shared suffix next to a
    /// short shared prefix is the signature of a small identity header in front
    /// of otherwise identical content.
    pub min_differing_pair_common_suffix: u64,
    /// Whether any pair was compared at all.
    pub compared_any: bool,
    /// Segments that a content-addressed plane could have elided *if* the
    /// format also hoisted per-commit identity out of the payload. Two segments
    /// count as content-equal when they share a byte length and their differing
    /// bytes all fall inside one window of at most
    /// `SEGMENT_IDENTITY_WINDOW_BYTES`.
    pub identity_normalized_duplicate_segments: u64,
    pub identity_normalized_duplicate_bytes: u64,
    /// Content-equivalence classes with more than one member.
    pub identity_normalized_shared_classes: u64,
}

/// Cap on distinct same-length values compared pairwise per length bucket.
const SEGMENT_SIMILARITY_BUCKET_CAP: usize = 128;
/// How much per-segment identity a redesigned payload is allowed to hoist out.
/// The LXCD16 direct leaf carries a 16-byte commit id, a 4-byte packed base and
/// a small timestamp-tail dictionary; 128 bytes is a generous allowance for all
/// of it, so this over-counts rather than under-counts the achievable win.
const SEGMENT_IDENTITY_WINDOW_BYTES: usize = 128;

pub async fn commit_delta_segment_similarity<R>(read: &R) -> CommitDeltaSegmentSimilarity
where
    R: StorageAdapterRead,
{
    let mut distinct = std::collections::HashMap::<[u8; 32], (Bytes, u64)>::new();
    let mut segments = 0u64;
    for entry in scan_layout_entries(
        read,
        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    )
    .await
    {
        segments += 1;
        let StorageProjectedValue::FullValue(value) = entry.value else {
            continue;
        };
        let slot = distinct
            .entry(*blake3::hash(&value).as_bytes())
            .or_insert((value, 0));
        slot.1 += 1;
    }

    let mut buckets = std::collections::BTreeMap::<usize, Vec<(Bytes, u64)>>::new();
    for (value, occurrences) in distinct.into_values() {
        buckets
            .entry(value.len())
            .or_default()
            .push((value, occurrences));
    }

    let mut similarity = CommitDeltaSegmentSimilarity {
        segments,
        min_differing_bytes: u64::MAX,
        ..CommitDeltaSegmentSimilarity::default()
    };
    for values in buckets.values() {
        similarity.distinct_values += values.len() as u64;
        if values.len() < 2 {
            continue;
        }
        similarity.same_length_distinct_values += values.len() as u64;
        let window = &values[..values.len().min(SEGMENT_SIMILARITY_BUCKET_CAP)];
        // Union-find over "content-equal once identity is hoisted out".
        let mut class = (0..window.len()).collect::<Vec<_>>();
        for (index, (left, _)) in window.iter().enumerate() {
            for (offset, (right, _)) in window[index + 1..].iter().enumerate() {
                let differing = left
                    .iter()
                    .zip(right.iter())
                    .filter(|(left, right)| left != right)
                    .count() as u64;
                similarity.compared_pairs += 1;
                similarity.compared_any = true;
                if differing * 100 <= left.len() as u64 {
                    similarity.near_identical_pairs += 1;
                }
                let common_prefix = left
                    .iter()
                    .zip(right.iter())
                    .take_while(|(left, right)| left == right)
                    .count();
                let common_suffix = left
                    .iter()
                    .rev()
                    .zip(right.iter().rev())
                    .take_while(|(left, right)| left == right)
                    .count();
                if differing > 0
                    && common_prefix + common_suffix + SEGMENT_IDENTITY_WINDOW_BYTES >= left.len()
                {
                    union(&mut class, index, index + 1 + offset);
                }
                if differing < similarity.min_differing_bytes {
                    similarity.min_differing_bytes = differing;
                    similarity.min_differing_pair_len = left.len() as u64;
                    similarity.min_differing_pair_common_prefix = common_prefix as u64;
                    similarity.min_differing_pair_common_suffix = common_suffix as u64;
                }
            }
        }
        let mut members = std::collections::BTreeMap::<usize, (u64, u64)>::new();
        for (index, (value, occurrences)) in window.iter().enumerate() {
            let root = find(&mut class, index);
            let slot = members.entry(root).or_insert((0, value.len() as u64));
            slot.0 += occurrences;
        }
        for (occurrences, len) in members.into_values() {
            if occurrences > 1 {
                similarity.identity_normalized_shared_classes += 1;
                similarity.identity_normalized_duplicate_segments += occurrences - 1;
                similarity.identity_normalized_duplicate_bytes += (occurrences - 1) * len;
            }
        }
    }
    if !similarity.compared_any {
        similarity.min_differing_bytes = 0;
    }
    similarity
}

fn find(class: &mut [usize], mut index: usize) -> usize {
    while class[index] != index {
        class[index] = class[class[index]];
        index = class[index];
    }
    index
}

fn union(class: &mut [usize], left: usize, right: usize) {
    let left = find(class, left);
    let right = find(class, right);
    if left != right {
        class[right] = left;
    }
}

pub async fn binary_manifest_layout_accounting<R>(
    read: &R,
) -> Result<BinaryManifestLayoutAccounting, crate::LixError>
where
    R: StorageAdapterRead,
{
    let entries = scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_SPACE).await;
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

    let manifests = scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_SPACE).await;
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
    use crate::hot_state::HotStateScanRequest;

    let hot_state = crate::hot_state::HotStateContext::new(
        crate::tracked_state::TrackedStateContext::new(),
        crate::commit_graph::CommitGraphContext::new(),
    );
    let current_rows = hot_state
        .reader(read)
        .scan_batch(&HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
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
        scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_SPACE).await;
    let manifest_chunk_entries =
        scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE).await;
    let chunk_entries = scan_layout_entries(read, crate::binary_cas::BINARY_CAS_CHUNK_SPACE).await;
    let presence_entries =
        scan_layout_entries(read, crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE).await;

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
        scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE).await;
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

    let entries = scan_layout_entries(read, crate::binary_cas::BINARY_CAS_MANIFEST_SPACE).await;
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
    let chunk_entries = scan_layout_entries(read, crate::binary_cas::BINARY_CAS_CHUNK_SPACE).await;
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

/// One registered storage space, looked up by its registry name.
///
/// Tools that issue their own point reads need the physical space without
/// re-declaring its id, which would be a second authority for the registry.
#[must_use]
pub fn storage_space_by_name(space_name: &str) -> crate::storage_adapter::StorageSpace {
    *native_storage_spaces()
        .iter()
        .find(|space| space.name == space_name)
        .expect("space name should exist")
}

/// The registered space for one physical space id, value semantics included.
///
/// Benchmarks and qualification harnesses that already hold an id must read
/// their space here rather than re-declaring it with
/// `StorageSpace::mutable`/`::immutable`. A space id has exactly one value
/// semantics, and both adapters place data by that declaration — RocksDB by
/// column family, SlateDB by LSM value versus object segment — so a harness
/// that guesses the semantics scans a different physical location than the
/// engine wrote. That is not hypothetical: `large_blob_updates` guessed
/// `immutable` for `binary_cas.chunk` and handed a raw payload to the
/// immutable-locator decoder, which reported it as data corruption.
#[must_use]
pub fn storage_space_by_id(space_id: u32) -> crate::storage_adapter::StorageSpace {
    *native_storage_spaces()
        .iter()
        .find(|space| space.id.0 == space_id)
        .unwrap_or_else(|| panic!("storage space id 0x{space_id:08x} is not registered"))
}

/// Per-row (key, value bytes) inventory of one space.
///
/// Equivalence tests compare these inventories byte-for-byte, so the scan
/// must be complete; the function asserts it observed every row.
pub async fn space_inventory<R>(read: &R, space_name: &str) -> Vec<(Vec<u8>, Vec<u8>)>
where
    R: StorageAdapterRead,
{
    let space = storage_space_by_name(space_name);
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

/// Every registered storage space, in physical key order.
///
/// Layout accounting derives from the one registry so a newly added space
/// appears in every layout report without a second list to maintain.
fn native_storage_spaces() -> &'static [crate::storage_adapter::StorageSpace] {
    crate::storage_spaces::ALL_STORAGE_SPACES
}

/// How a storage space derives its key from the bytes it stores.
///
/// Content addressing is what makes identical payloads cost one row instead
/// of many. The rule is stated once here so an audit can prove the invariant
/// rather than assume it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentAddressRule {
    /// The key carries an identity (commit id, entity path, ordinal) that is
    /// independent of the value bytes. Equal payloads under distinct keys are
    /// stored twice by construction.
    NotContentAddressed,
    /// `key == blake3(value)`.
    Blake3Value,
    /// `key == blake3::derive_key(context, value)`.
    Blake3KeyedValue(&'static str),
    /// `key == blake3(chunk payload)` after the stored chunk envelope is
    /// decoded.
    BinaryCasChunkPayload,
    /// `key == blake3(json text)` after the stored JSON envelope is decoded.
    JsonStorePayload,
    /// The key is a content address, but its payload lives in another space:
    /// this space stores keys only. Nothing here can be verified against the
    /// row's own (empty) value.
    ContentAddressedKeyOnlyMirror,
}

/// The content-address rule for one physical space id.
#[must_use]
pub fn content_address_rule(space_id: u32) -> ContentAddressRule {
    match space_id {
        // tracked_state.tree_chunk
        0x0004_0001 => ContentAddressRule::Blake3Value,
        // tracked_state.commit_mutation_directory_node.v1
        0x0004_002d => {
            ContentAddressRule::Blake3KeyedValue("lix commit mutation directory node v1")
        }
        // tracked_state.current_state_data_part.v1
        0x0004_002f => {
            ContentAddressRule::Blake3KeyedValue("lix native current-state data part v1")
        }
        // tracked_state.current_state_data_part_refs.v1
        0x0004_0030 => {
            ContentAddressRule::Blake3KeyedValue("lix native current-state data part refs v1")
        }
        // tracked_state.scoped_range.v3
        0x0004_0032 => {
            ContentAddressRule::Blake3KeyedValue("lix scoped current-state range node v3")
        }
        // binary_cas.chunk
        0x0005_0003 => ContentAddressRule::BinaryCasChunkPayload,
        // json_store.json
        0x0002_0001 => ContentAddressRule::JsonStorePayload,
        // binary_cas.chunk_presence
        0x0005_0004 => ContentAddressRule::ContentAddressedKeyOnlyMirror,
        _ => ContentAddressRule::NotContentAddressed,
    }
}

/// Recomputes the content address one row *should* have from the bytes it
/// stores.
///
/// `Ok(None)` means the space is not content-addressed (or stores keys only),
/// so there is nothing to check. `Ok(Some(digest))` must equal the row key.
pub fn recompute_content_address(
    space_id: u32,
    value: &[u8],
) -> Result<Option<[u8; 32]>, crate::LixError> {
    Ok(match content_address_rule(space_id) {
        ContentAddressRule::NotContentAddressed
        | ContentAddressRule::ContentAddressedKeyOnlyMirror => None,
        ContentAddressRule::Blake3Value => Some(*blake3::hash(value).as_bytes()),
        ContentAddressRule::Blake3KeyedValue(context) => Some(
            *blake3::Hasher::new_derive_key(context)
                .update(value)
                .finalize()
                .as_bytes(),
        ),
        ContentAddressRule::BinaryCasChunkPayload => {
            let (_codec, _len, payload) = crate::binary_cas::decode_binary_cas_chunk(value)?;
            Some(*blake3::hash(payload).as_bytes())
        }
        ContentAddressRule::JsonStorePayload => {
            let json = crate::json_store::store::decode_stored_json(value)?;
            Some(*blake3::hash(&json).as_bytes())
        }
    })
}

/// Decodes one `binary_cas.manifest_chunk` row into the chunk it references
/// and that chunk's logical size.
pub fn decode_binary_cas_chunk_reference(value: &[u8]) -> Result<([u8; 32], u64), crate::LixError> {
    crate::binary_cas::decode_binary_cas_manifest_chunk(value)
}

async fn scan_layout_space<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> StorageLayoutAccounting
where
    R: StorageAdapterRead,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()
    .expect("valid empty storage layout prefix");
    let mut accounting = StorageLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: 0,
        key_bytes: 0,
        value_bytes: 0,
    };
    let mut cursor = read
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await
        .expect("begin storage bench layout scan");
    loop {
        let (result, has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan complete storage bench layout space")
            .into_parts();
        for entry in result {
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
    }
}

async fn scan_layout_entries<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> Vec<crate::storage_adapter::StorageReadEntry>
where
    R: StorageAdapterRead,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()
    .expect("valid empty storage layout prefix");
    let mut entries = Vec::new();
    let mut cursor = read
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await
        .expect("begin storage bench layout scan");
    loop {
        let (result, has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan complete storage bench layout space")
            .into_parts();
        entries.extend(result);
        if !has_more {
            return entries;
        }
    }
}

// ---------------------------------------------------------------------------
// E42 probe — commit-delta decode census, attributed by write-path phase.
//
// Answers, deterministically and in one rep: does a single-row UPDATE reach
// through the packed commit-delta indirection to locate the row it is
// updating, or does it read the hot row? The phase is a process-global
// marker rather than a thread-local because the profile harness runs exactly
// one statement at a time under `block_on`; guards nest and restore.
// ---------------------------------------------------------------------------

pub const CRUD_PHASE_OTHER: usize = 0;
/// The write-side read: `scan_entity_candidates*` locating the target row.
pub const CRUD_PHASE_WRITE_READ: usize = 1;
/// `Transaction::commit_prepared` — publication of the new write set.
pub const CRUD_PHASE_COMMIT: usize = 2;
pub const CRUD_PHASE_COUNT: usize = 3;

static CRUD_PHASE: AtomicUsize = AtomicUsize::new(CRUD_PHASE_OTHER);
static DELTA_LEAF_DECODES: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_LEAF_DECODE_ROWS: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_LEAF_DECODE_BYTES: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_ZSTD_CALLS: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_ZSTD_IN_BYTES: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_ZSTD_OUT_BYTES: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_ORDERED_LOADS: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static DELTA_ENCODES: [AtomicU64; CRUD_PHASE_COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// Restores the enclosing phase when dropped.
#[derive(Debug)]
pub struct CrudPhaseGuard(usize);

impl Drop for CrudPhaseGuard {
    fn drop(&mut self) {
        CRUD_PHASE.store(self.0, Ordering::Relaxed);
    }
}

pub(crate) fn enter_crud_phase(phase: usize) -> CrudPhaseGuard {
    CrudPhaseGuard(CRUD_PHASE.swap(phase, Ordering::Relaxed))
}

fn crud_phase() -> usize {
    let phase = CRUD_PHASE.load(Ordering::Relaxed);
    if phase < CRUD_PHASE_COUNT {
        phase
    } else {
        CRUD_PHASE_OTHER
    }
}

pub(crate) fn record_commit_delta_leaf_decode(rows: usize, segment_bytes: usize) {
    let phase = crud_phase();
    DELTA_LEAF_DECODES[phase].fetch_add(1, Ordering::Relaxed);
    DELTA_LEAF_DECODE_ROWS[phase].fetch_add(rows as u64, Ordering::Relaxed);
    DELTA_LEAF_DECODE_BYTES[phase].fetch_add(segment_bytes as u64, Ordering::Relaxed);
}

pub(crate) fn record_commit_delta_sidecar_zstd(compressed: usize, uncompressed: usize) {
    let phase = crud_phase();
    DELTA_ZSTD_CALLS[phase].fetch_add(1, Ordering::Relaxed);
    DELTA_ZSTD_IN_BYTES[phase].fetch_add(compressed as u64, Ordering::Relaxed);
    DELTA_ZSTD_OUT_BYTES[phase].fetch_add(uncompressed as u64, Ordering::Relaxed);
}

pub(crate) fn record_commit_delta_ordered_load(keys: usize) {
    let phase = crud_phase();
    DELTA_ORDERED_LOADS[phase].fetch_add(keys.max(1) as u64, Ordering::Relaxed);
}

pub(crate) fn record_commit_delta_encode() {
    DELTA_ENCODES[crud_phase()].fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitDeltaPhaseCensus {
    pub leaf_decodes: u64,
    pub leaf_decode_rows: u64,
    pub leaf_decode_bytes: u64,
    pub zstd_calls: u64,
    pub zstd_in_bytes: u64,
    pub zstd_out_bytes: u64,
    pub ordered_load_keys: u64,
    pub encodes: u64,
}

/// Drains the census. Index with `CRUD_PHASE_*`.
pub fn take_commit_delta_phase_census() -> [CommitDeltaPhaseCensus; CRUD_PHASE_COUNT] {
    std::array::from_fn(|phase| CommitDeltaPhaseCensus {
        leaf_decodes: DELTA_LEAF_DECODES[phase].swap(0, Ordering::Relaxed),
        leaf_decode_rows: DELTA_LEAF_DECODE_ROWS[phase].swap(0, Ordering::Relaxed),
        leaf_decode_bytes: DELTA_LEAF_DECODE_BYTES[phase].swap(0, Ordering::Relaxed),
        zstd_calls: DELTA_ZSTD_CALLS[phase].swap(0, Ordering::Relaxed),
        zstd_in_bytes: DELTA_ZSTD_IN_BYTES[phase].swap(0, Ordering::Relaxed),
        zstd_out_bytes: DELTA_ZSTD_OUT_BYTES[phase].swap(0, Ordering::Relaxed),
        ordered_load_keys: DELTA_ORDERED_LOADS[phase].swap(0, Ordering::Relaxed),
        encodes: DELTA_ENCODES[phase].swap(0, Ordering::Relaxed),
    })
}

pub fn crud_phase_name(phase: usize) -> &'static str {
    match phase {
        CRUD_PHASE_WRITE_READ => "write_read",
        CRUD_PHASE_COMMIT => "commit_prepared",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointCommitScanBenchMode, binary_manifest_layout_accounting,
        plan_repository_gc_for_bench, scan_checkpoint_commits_for_bench,
    };
    use crate::changelog::bench::{append_ordered_commits, stage_append_once};
    use crate::engine::Engine;
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageKey, StorageValue, StorageWriteOptions,
    };
    use crate::{CreateBranchOptions, Value};

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
            .begin_read(crate::storage::ReadOptions::default())
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
                crate::binary_cas::BINARY_CAS_MANIFEST_SPACE,
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
            .begin_read(crate::storage::ReadOptions::default())
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
        let engine = Engine::new(storage.clone())
            .await
            .expect("open benchmark engine");
        let main = engine
            .open_session()
            .await
            .expect("open benchmark main session");
        let schema = serde_json::json!({
            "x-lix-key": "repository_gc_benchmark_fixture",
            "x-lix-primary-key": ["/path"],
            "type": "object",
            "required": ["path", "value"],
            "properties": {
                "path": { "type": "string" },
                "value": { "type": "integer" }
            },
            "additionalProperties": false
        });
        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register benchmark schema");
        let branch = main
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-000000000010".to_owned()),
                name: "repository-gc-benchmark-unreachable".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create benchmark branch");
        let branch_session = engine
            .open_session_at(branch.id.clone())
            .await
            .expect("open benchmark branch session");
        for commit_index in 0..10 {
            let mut transaction = branch_session
                .begin_transaction()
                .await
                .expect("begin benchmark transaction");
            for row_index in 0..10 {
                let row = commit_index * 10 + row_index;
                transaction
                    .execute(
                        "INSERT INTO repository_gc_benchmark_fixture (path, value) \
                         VALUES ($1, $2)",
                        &[
                            Value::Text(format!("/row/{row:08}")),
                            Value::Integer(row as i64),
                        ],
                    )
                    .await
                    .expect("stage benchmark row");
            }
            transaction
                .commit()
                .await
                .expect("publish benchmark commit");
        }
        main.execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch.id)],
        )
        .await
        .expect("delete benchmark branch");
        let adapter = StorageAdapter::new(storage);

        let before_layout = super::layout_accounting(
            &adapter
                .begin_read(crate::storage::ReadOptions::default())
                .await
                .expect("begin pre-GC inventory read"),
        )
        .await;

        let first = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("plan repository gc");
        let after_first_layout = super::layout_accounting(
            &adapter
                .begin_read(crate::storage::ReadOptions::default())
                .await
                .expect("begin post-first-plan inventory read"),
        )
        .await;
        let second = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("repeat repository gc plan");
        let after_second_layout = super::layout_accounting(
            &adapter
                .begin_read(crate::storage::ReadOptions::default())
                .await
                .expect("begin post-second-plan inventory read"),
        )
        .await;

        assert_eq!(first.swept_commits, 10);
        // Superseded branch-ref facts are no longer GC debt: each publication
        // deletes the ref change its own control supersedes, and the branch
        // deletion deletes the last one, all in the publishing write set.
        assert_eq!(first.swept_standalone_changes, 0);
        assert_eq!(first.deleted_commit_state_manifests, 10);
        assert_eq!(first.deleted_mutation_inventories, 10);
        // The ten branch-only commits are reclaimable, while the branch base
        // remains the active main head and therefore keeps its semantic
        // projection in the authenticated serving-dependency closure.
        assert_eq!(first.deleted_semantic_commit_projections, 10);
        // Each reclaimed projection owns one change fact and reverse-index row.
        assert_eq!(first.deleted_semantic_change_rows, 10);
        // The stranded serving generation is gone too, but the branch deletion
        // retired it rather than this sweep: a generation is reachable from
        // exactly one branch control, so the write set that removes the control
        // is the one that can prove nothing will read it again.
        assert_eq!(first.reclaimed_generation_rows, 0);
        assert_eq!(
            first.delete_counts_by_space,
            vec![
                (
                    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
                        .id
                        .0,
                    10,
                ), // commit-state manifest authority
                (
                    crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
                        .id
                        .0,
                    10,
                ), // mutation inventory authority
                (crate::changelog::COMMIT_SPACE.id.0, 10), // branch-only commit projections
                (crate::changelog::CHANGE_SPACE.id.0, 10), // their change facts
            ]
        );
        assert_eq!(
            first
                .delete_counts_by_space
                .iter()
                .map(|(_, count)| *count as u64)
                .sum::<u64>(),
            first.staged_deletes
        );
        assert_eq!(first.delete_descriptors, first.staged_deletes as usize);
        // GC also stages the mandatory binary-CAS reclamation key. Its put
        // shares the key arena with the UUID-keyed delete descriptors. Since the
        // revision singletons were consolidated into one space, the reclamation
        // token's *logical* key is a single byte (`b"b"`) and the 4-byte space
        // id is prepended at the physical layer, so derive the width from the
        // constant rather than restating it.
        const FENCE_KEY_BYTES: usize =
            crate::storage_adapter::REVISION_KEY_BINARY_CAS_RECLAMATION.len();
        assert_eq!(first.key_shared_buffers, first.staged_deletes as usize + 1);
        // Every canonical-record delete is UUID keyed, so each descriptor is
        // exactly 16 bytes.
        assert_eq!(
            first.key_shared_bytes,
            first.staged_deletes as usize * 16 + FENCE_KEY_BYTES
        );
        assert_eq!(second.swept_commits, first.swept_commits);
        assert_eq!(second.delete_counts_by_space, first.delete_counts_by_space);
        assert_eq!(second.staged_deletes, first.staged_deletes);
        assert_eq!(before_layout, after_first_layout);
        assert_eq!(after_first_layout, after_second_layout);
    }
}
