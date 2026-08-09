//! Test-only scalar counters for the prepared-CAS ownership qualification.
//!
//! This module is feature-gated and retains no payloads, object IDs, receipts,
//! roots, or reachability state. It is measurement-only; production builds do
//! not compile it.

#![cfg(feature = "prepared-cas-observability")]

use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparedCasCounters {
    pub current_transaction_resident_payload_bytes: u64,
    pub peak_transaction_resident_payload_bytes: u64,
    pub page_bytes_before_flush: u64,
    pub page_bytes_after_flush: u64,
    pub page_flushes: u64,
    pub prepared_receipt_count: u64,
    pub prepared_receipt_metadata_bytes: u64,
    pub final_transaction_payload_bytes: u64,
    pub unreferenced_object_ids_before_publish: u64,
    pub unreferenced_object_bytes_before_publish: u64,
    pub reachable_object_ids_after_publish: u64,
    pub reachable_object_bytes_after_publish: u64,
    pub orphan_object_ids_after_rollback: u64,
    pub orphan_object_bytes_after_rollback: u64,
    pub reclaimed_object_ids: u64,
    pub reclaimed_object_bytes: u64,
    pub corrupted_receipts_rejected: u64,
}

static COUNTERS: OnceLock<Mutex<PreparedCasCounters>> = OnceLock::new();

fn counters() -> &'static Mutex<PreparedCasCounters> {
    COUNTERS.get_or_init(|| Mutex::new(PreparedCasCounters::default()))
}

fn update(update: impl FnOnce(&mut PreparedCasCounters)) {
    let mut counters = counters()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    update(&mut counters);
}

pub fn reset() {
    update(|counters| *counters = PreparedCasCounters::default());
}

pub fn snapshot() -> PreparedCasCounters {
    *counters()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn record_resident_payload_bytes(current: usize) {
    update(|counters| {
        counters.current_transaction_resident_payload_bytes = current as u64;
        counters.peak_transaction_resident_payload_bytes = counters
            .peak_transaction_resident_payload_bytes
            .max(current as u64);
    });
}

pub(crate) fn record_page_flush(before: usize) {
    update(|counters| {
        counters.page_bytes_before_flush = counters
            .page_bytes_before_flush
            .saturating_add(before as u64);
        counters.page_bytes_after_flush = 0;
        counters.page_flushes = counters.page_flushes.saturating_add(1);
    });
}

pub(crate) fn record_receipt_metadata(bytes: usize) {
    update(|counters| {
        counters.prepared_receipt_count = counters.prepared_receipt_count.saturating_add(1);
        counters.prepared_receipt_metadata_bytes = counters
            .prepared_receipt_metadata_bytes
            .saturating_add(bytes as u64);
    });
}

pub(crate) fn record_final_transaction_payload(bytes: u64) {
    update(|counters| counters.final_transaction_payload_bytes = bytes);
}

pub(crate) fn record_unreferenced_objects(ids: usize, bytes: usize) {
    update(|counters| {
        counters.unreferenced_object_ids_before_publish = counters
            .unreferenced_object_ids_before_publish
            .saturating_add(ids as u64);
        counters.unreferenced_object_bytes_before_publish = counters
            .unreferenced_object_bytes_before_publish
            .saturating_add(bytes as u64);
    });
}

pub(crate) fn record_reachable_objects(ids: u64, bytes: u64) {
    update(|counters| {
        counters.reachable_object_ids_after_publish = counters
            .reachable_object_ids_after_publish
            .saturating_add(ids);
        counters.reachable_object_bytes_after_publish = counters
            .reachable_object_bytes_after_publish
            .saturating_add(bytes);
    });
}

pub(crate) fn record_orphans(ids: u64, bytes: u64) {
    update(|counters| {
        counters.orphan_object_ids_after_rollback = counters
            .orphan_object_ids_after_rollback
            .saturating_add(ids);
        counters.orphan_object_bytes_after_rollback = counters
            .orphan_object_bytes_after_rollback
            .saturating_add(bytes);
    });
}

pub(crate) fn record_reclaimed(ids: usize, bytes: usize) {
    update(|counters| {
        counters.reclaimed_object_ids = counters.reclaimed_object_ids.saturating_add(ids as u64);
        counters.reclaimed_object_bytes =
            counters.reclaimed_object_bytes.saturating_add(bytes as u64);
    });
}

pub(crate) fn record_corruption_rejection() {
    update(|counters| {
        counters.corrupted_receipts_rejected =
            counters.corrupted_receipts_rejected.saturating_add(1);
    });
}
