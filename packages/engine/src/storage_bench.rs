use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::entity_identity::EntityIdentity;
use crate::storage::{StorageKey, StorageValue};
use crate::untracked_state::UntrackedStateRowRef;

static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_VERSIONS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static JSON_STORE_STAGE_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_transaction_rows_staged(count: usize) {
    TRANSACTION_ROWS_STAGED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_untracked_rows(count: usize) {
    TRANSACTION_UNTRACKED_ROWS.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_validation_version() {
    TRANSACTION_VALIDATION_VERSIONS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_load() {
    TRANSACTION_SCHEMA_CATALOG_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_json_store_stage_bytes(hash: [u8; 32]) {
    JSON_STORE_STAGE_BYTES.fetch_add(hash.len() as u64, Ordering::Relaxed);
}

pub fn untracked_state_row_key_value(
    entity_id: &str,
    snapshot_content: &str,
) -> (StorageKey, StorageValue) {
    untracked_state_row_key_value_with_payload(entity_id, snapshot_content, false)
}

pub fn untracked_state_full_row_key_value(
    entity_id: &str,
    snapshot_content: &str,
) -> (StorageKey, StorageValue) {
    untracked_state_row_key_value_with_payload(entity_id, snapshot_content, true)
}

fn untracked_state_row_key_value_with_payload(
    entity_id: &str,
    snapshot_content: &str,
    include_identity_in_value: bool,
) -> (StorageKey, StorageValue) {
    let entity_id = EntityIdentity::single(entity_id);
    let row = UntrackedStateRowRef {
        entity_id: &entity_id,
        schema_key: "json_pointer",
        file_id: Some(""),
        snapshot_content: Some(snapshot_content),
        metadata: None,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
        global: false,
        version_id: "bench-version",
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
