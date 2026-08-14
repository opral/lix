use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::storage_adapter::StorageWriteSet;
static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_BRANCHS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_COMPILES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static TRANSACTION_VALIDATION_ROWS_VISITED: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static TRANSACTION_VALIDATION_ACCOUNTING_ENABLED: AtomicBool = AtomicBool::new(false);
static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_CERTIFICATIONS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_HITS: AtomicU64 = AtomicU64::new(0);
static CERTIFIED_ENTITY_UPDATE_VALUE_BATCH_ROWS: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_PUTS: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_DELETES: AtomicU64 = AtomicU64::new(0);
static CRUD_PHYSICAL_WRITTEN_BYTES: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_MANIFEST_LEAF_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_SUMMARIZED_CHUNK_ROWS: AtomicU64 = AtomicU64::new(0);
static MEDIA_UPLOAD_CHUNK_PAYLOAD_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
static IMMUTABLE_SEGMENT_IDENTITY_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static VERIFIED_INLINE_BLOB_SPLICE_CALLS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static VERIFIED_INLINE_BLOB_SPLICE_CHANGED_CHUNKS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "storage-benches")]
static VERIFIED_INLINE_BLOB_SPLICE_TOTAL_CHUNKS: AtomicU64 = AtomicU64::new(0);

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

/// Feature-gated publication accounting for the public SQL/file splice route.
/// It exists only for the benchmark qualification and never participates in
/// authority or publication decisions.
#[cfg(feature = "storage-benches")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VerifiedInlineBlobSpliceAccounting {
    pub calls: u64,
    pub changed_chunks: u64,
    pub total_chunks: u64,
}

#[cfg(feature = "storage-benches")]
pub fn begin_verified_inline_blob_splice_accounting() {
    VERIFIED_INLINE_BLOB_SPLICE_CALLS.store(0, Ordering::Relaxed);
    VERIFIED_INLINE_BLOB_SPLICE_CHANGED_CHUNKS.store(0, Ordering::Relaxed);
    VERIFIED_INLINE_BLOB_SPLICE_TOTAL_CHUNKS.store(0, Ordering::Relaxed);
}

#[cfg(feature = "storage-benches")]
pub(crate) fn record_verified_inline_blob_splice(changed_chunks: usize, total_chunks: usize) {
    VERIFIED_INLINE_BLOB_SPLICE_CALLS.fetch_add(1, Ordering::Relaxed);
    VERIFIED_INLINE_BLOB_SPLICE_CHANGED_CHUNKS.fetch_add(changed_chunks as u64, Ordering::Relaxed);
    VERIFIED_INLINE_BLOB_SPLICE_TOTAL_CHUNKS.fetch_add(total_chunks as u64, Ordering::Relaxed);
}

#[cfg(feature = "storage-benches")]
pub fn take_verified_inline_blob_splice_accounting() -> VerifiedInlineBlobSpliceAccounting {
    VerifiedInlineBlobSpliceAccounting {
        calls: VERIFIED_INLINE_BLOB_SPLICE_CALLS.swap(0, Ordering::Relaxed),
        changed_chunks: VERIFIED_INLINE_BLOB_SPLICE_CHANGED_CHUNKS.swap(0, Ordering::Relaxed),
        total_chunks: VERIFIED_INLINE_BLOB_SPLICE_TOTAL_CHUNKS.swap(0, Ordering::Relaxed),
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

pub(crate) fn record_transaction_rows_staged(count: usize) {
    TRANSACTION_ROWS_STAGED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_untracked_rows(count: usize) {
    TRANSACTION_UNTRACKED_ROWS.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_validation_branch() {
    TRANSACTION_VALIDATION_BRANCHS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "storage-benches")]
pub fn begin_transaction_validation_accounting() {
    TRANSACTION_VALIDATION_ROWS_VISITED.store(0, Ordering::Relaxed);
    TRANSACTION_VALIDATION_ACCOUNTING_ENABLED.store(true, Ordering::Relaxed);
}

#[cfg(feature = "storage-benches")]
pub(crate) fn record_transaction_validation_row_visited() {
    if TRANSACTION_VALIDATION_ACCOUNTING_ENABLED.load(Ordering::Relaxed) {
        TRANSACTION_VALIDATION_ROWS_VISITED.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "storage-benches")]
pub fn take_transaction_validation_rows_visited() -> u64 {
    TRANSACTION_VALIDATION_ACCOUNTING_ENABLED.store(false, Ordering::Relaxed);
    TRANSACTION_VALIDATION_ROWS_VISITED.swap(0, Ordering::Relaxed)
}

pub(crate) fn record_transaction_schema_catalog_load() {
    TRANSACTION_SCHEMA_CATALOG_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_compile() {
    TRANSACTION_SCHEMA_CATALOG_COMPILES.fetch_add(1, Ordering::Relaxed);
}
