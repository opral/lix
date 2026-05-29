use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::entity_pk::EntityPk;
use crate::storage::{
    ScanPlan, StorageCoreProjection, StorageKey, StoragePrefix, StorageProjectedValue, StorageRead,
    StorageScanOptions, StorageValue, StorageWriteOptions, StorageWriteSet, StorageWriteSetError,
};
use crate::untracked_state::UntrackedStateRowRef;

static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_BRANCHS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static JSON_STORE_STAGE_BYTES: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn commit_write_set_for_bench<B>(
    storage: &crate::storage::StorageContext<B>,
    writes: StorageWriteSet,
) -> Result<crate::storage::StorageWriteSetStats, StorageWriteSetError>
where
    B: crate::storage::StorageBackend,
{
    let (_commit, stats) = storage.commit_write_set(writes, StorageWriteOptions::default())?;
    Ok(stats)
}

pub fn layout_accounting<R>(read: &R) -> Vec<StorageLayoutAccounting>
where
    R: StorageRead,
{
    native_storage_spaces()
        .iter()
        .map(|space| scan_layout_space(read, *space))
        .collect()
}

fn native_storage_spaces() -> &'static [crate::storage::StorageSpace] {
    &[
        crate::untracked_state::storage::UNTRACKED_STATE_ROW_SPACE,
        crate::json_store::store::JSON_SPACE,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_ROOT_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::changelog::CHANGE_SPACE,
        crate::changelog::COMMIT_CHANGE_REF_CHUNK_SPACE,
    ]
}

fn scan_layout_space<R>(read: &R, space: crate::storage::StorageSpace) -> StorageLayoutAccounting
where
    R: StorageRead,
{
    let result = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    )
    .collect(
        read,
        StorageScanOptions {
            projection: StorageCoreProjection::FullValue,
            limit_rows: 1_000_000,
            ..StorageScanOptions::default()
        },
    )
    .expect("scan storage bench layout space");

    StorageLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: result.value.entries.len() as u64,
        key_bytes: result
            .value
            .entries
            .iter()
            .map(|entry| entry.key.0.len() as u64 + 4)
            .sum(),
        value_bytes: result
            .value
            .entries
            .iter()
            .map(|entry| match &entry.value {
                StorageProjectedValue::KeyOnly => 0,
                StorageProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum(),
    }
}

pub fn untracked_state_row_key_value(
    entity_pk: &str,
    snapshot_content: &str,
) -> (StorageKey, StorageValue) {
    untracked_state_row_key_value_with_payload(entity_pk, snapshot_content, false)
}

pub fn untracked_state_full_row_key_value(
    entity_pk: &str,
    snapshot_content: &str,
) -> (StorageKey, StorageValue) {
    untracked_state_row_key_value_with_payload(entity_pk, snapshot_content, true)
}

fn untracked_state_row_key_value_with_payload(
    entity_pk: &str,
    snapshot_content: &str,
    include_identity_in_value: bool,
) -> (StorageKey, StorageValue) {
    let entity_pk = EntityPk::single(entity_pk);
    let row = UntrackedStateRowRef {
        entity_pk: &entity_pk,
        schema_key: "json_pointer",
        file_id: Some(""),
        snapshot_content: Some(snapshot_content),
        metadata: None,
        created_at: crate::common::LixTimestamp::expect_parse(
            "created_at",
            "2026-01-01T00:00:00.000Z",
        ),
        updated_at: crate::common::LixTimestamp::expect_parse(
            "updated_at",
            "2026-01-01T00:00:00.000Z",
        ),
        global: false,
        branch_id: "bench-branch",
    };
    let value = if include_identity_in_value {
        crate::untracked_state::codec::encode_row_ref(row).expect("encode untracked bench row")
    } else {
        crate::untracked_state::codec::encode_payload_ref(row)
            .expect("encode untracked bench payload")
    };
    (
        StorageKey(Bytes::from(
            crate::untracked_state::storage::encode_untracked_state_row_key_ref(row.into())
                .expect("encode untracked bench key"),
        )),
        StorageValue {
            bytes: Bytes::from(value),
        },
    )
}
