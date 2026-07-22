use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    ScanPlan, StorageAdapterRead, StorageCoreProjection, StoragePrefix, StorageProjectedValue,
    StorageScanOptions, StorageWriteOptions, StorageWriteSet, StorageWriteSetError,
};

static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_BRANCHS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_COMPILES: AtomicU64 = AtomicU64::new(0);
static JSON_STORE_STAGE_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinaryCasWriteAccounting {
    pub chunk_lookup_count: u64,
    pub chunk_lookup_batch_count: u64,
    pub chunk_lookup_hit_count: u64,
    pub chunk_lookup_miss_count: u64,
    pub chunk_lookup_elapsed_ns: u64,
    pub transaction_duplicate_chunk_count: u64,
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

pub(crate) async fn commit_write_set_for_bench<StorageImpl>(
    storage: &crate::storage_adapter::StorageAdapter<StorageImpl>,
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

fn native_storage_spaces() -> &'static [crate::storage_adapter::StorageSpace] {
    &[
        crate::live_state::LIVE_STATE_INDEX_ROW_SPACE,
        crate::json_store::store::JSON_SPACE,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_ROOT_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_PRESENCE_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::changelog::CHANGE_SPACE,
        crate::changelog::COMMIT_CHANGE_REF_CHUNK_SPACE,
    ]
}

async fn scan_layout_space<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> StorageLayoutAccounting
where
    R: StorageAdapterRead,
{
    let entries = scan_layout_entries(read, space).await;

    StorageLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: entries.len() as u64,
        key_bytes: entries
            .iter()
            .map(|entry| entry.key.0.len() as u64 + 4)
            .sum(),
        value_bytes: entries
            .iter()
            .map(|entry| match &entry.value {
                StorageProjectedValue::KeyOnly => 0,
                StorageProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum(),
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
