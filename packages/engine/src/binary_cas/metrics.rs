use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinaryCasWriteMetrics {
    pub chunk_lookup_count: u64,
    pub chunk_lookup_hit_count: u64,
    pub chunk_lookup_miss_count: u64,
    pub chunk_lookup_elapsed_ns: u64,
    pub transaction_duplicate_chunk_count: u64,
}

static CHUNK_LOOKUP_COUNT: AtomicU64 = AtomicU64::new(0);
static CHUNK_LOOKUP_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
static CHUNK_LOOKUP_MISS_COUNT: AtomicU64 = AtomicU64::new(0);
static CHUNK_LOOKUP_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_DUPLICATE_CHUNK_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn reset_binary_cas_write_metrics() {
    CHUNK_LOOKUP_COUNT.store(0, Ordering::Relaxed);
    CHUNK_LOOKUP_HIT_COUNT.store(0, Ordering::Relaxed);
    CHUNK_LOOKUP_MISS_COUNT.store(0, Ordering::Relaxed);
    CHUNK_LOOKUP_ELAPSED_NS.store(0, Ordering::Relaxed);
    TRANSACTION_DUPLICATE_CHUNK_COUNT.store(0, Ordering::Relaxed);
}

pub fn binary_cas_write_metrics_snapshot() -> BinaryCasWriteMetrics {
    BinaryCasWriteMetrics {
        chunk_lookup_count: CHUNK_LOOKUP_COUNT.load(Ordering::Relaxed),
        chunk_lookup_hit_count: CHUNK_LOOKUP_HIT_COUNT.load(Ordering::Relaxed),
        chunk_lookup_miss_count: CHUNK_LOOKUP_MISS_COUNT.load(Ordering::Relaxed),
        chunk_lookup_elapsed_ns: CHUNK_LOOKUP_ELAPSED_NS.load(Ordering::Relaxed),
        transaction_duplicate_chunk_count: TRANSACTION_DUPLICATE_CHUNK_COUNT
            .load(Ordering::Relaxed),
    }
}

pub(crate) fn record_binary_cas_chunk_lookup(exists: bool, elapsed: Duration) {
    CHUNK_LOOKUP_COUNT.fetch_add(1, Ordering::Relaxed);
    if exists {
        CHUNK_LOOKUP_HIT_COUNT.fetch_add(1, Ordering::Relaxed);
    } else {
        CHUNK_LOOKUP_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    CHUNK_LOOKUP_ELAPSED_NS.fetch_add(duration_ns(elapsed), Ordering::Relaxed);
}

pub(crate) fn record_binary_cas_transaction_duplicate_chunk() {
    TRANSACTION_DUPLICATE_CHUNK_COUNT.fetch_add(1, Ordering::Relaxed);
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
