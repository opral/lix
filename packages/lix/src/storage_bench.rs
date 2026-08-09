use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::storage_adapter::StorageWriteSet;
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

/// Returns and resets the number of certified parameter-batch INSERT routes
/// selected by the planner, before any physical staging occurs.
pub fn take_certified_entity_insert_parameter_batch_certifications() -> u64 {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_CERTIFICATIONS.swap(0, Ordering::Relaxed)
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
    }
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
