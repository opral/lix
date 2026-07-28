//! Allocation and scorecard support for the large ignored E2E benchmarks.
//!
//! The allocator hooks intentionally perform atomic arithmetic only. In
//! particular, they must never format, lock, or allocate: doing so would
//! recursively enter the global allocator and invalidate both the benchmark
//! and the process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

pub const MACHINE_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_JSON=";
pub const LARGE_ALLOCATION_BYTES: u64 = 64 * 1024;

struct CountingSystemAllocator;

static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static LARGE_ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static SCOPE_ACTIVE: AtomicBool = AtomicBool::new(false);
static SCOPE_PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static SCOPE_LOCK: Mutex<()> = Mutex::new(());

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingSystemAllocator = CountingSystemAllocator;

unsafe impl GlobalAlloc for CountingSystemAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_reallocation(layout.size(), new_size);
        }
        new_pointer
    }
}

#[inline]
fn record_allocation(size: usize) {
    let size = size as u64;
    ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    if size >= LARGE_ALLOCATION_BYTES {
        LARGE_ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    record_scope_peak(live);
}

#[inline]
fn record_reallocation(old_size: usize, new_size: usize) {
    let old_size = old_size as u64;
    let new_size = new_size as u64;
    ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
    if new_size >= LARGE_ALLOCATION_BYTES {
        LARGE_ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    let live = if new_size >= old_size {
        LIVE_BYTES.fetch_add(new_size - old_size, Ordering::Relaxed) + (new_size - old_size)
    } else {
        LIVE_BYTES.fetch_sub(old_size - new_size, Ordering::Relaxed) - (old_size - new_size)
    };
    record_scope_peak(live);
}

#[inline]
fn record_scope_peak(live: u64) {
    if !SCOPE_ACTIVE.load(Ordering::Relaxed) {
        return;
    }
    let mut peak = SCOPE_PEAK_LIVE_BYTES.load(Ordering::Relaxed);
    while live > peak {
        match SCOPE_PEAK_LIVE_BYTES.compare_exchange_weak(
            peak,
            live,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => peak = actual,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationMetrics {
    pub allocation_count: u64,
    pub allocated_bytes: u64,
    pub live_bytes_delta: i64,
    pub peak_live_bytes_delta: u64,
    pub large_allocation_count: u64,
}

#[derive(Clone, Copy)]
struct AllocatorSnapshot {
    allocation_count: u64,
    allocated_bytes: u64,
    live_bytes: u64,
    large_allocation_count: u64,
}

impl AllocatorSnapshot {
    fn capture() -> Self {
        Self {
            allocation_count: ALLOCATION_COUNT.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
            large_allocation_count: LARGE_ALLOCATION_COUNT.load(Ordering::Relaxed),
        }
    }
}

/// One exclusive allocation-measurement window.
///
/// Run ignored allocation benchmarks with an exact test filter. The lock
/// prevents two instrumented windows in this test binary from overlapping,
/// while the global counters still include allocations made by any unrelated
/// test running concurrently.
pub struct AllocationScope {
    start: AllocatorSnapshot,
    _exclusive: MutexGuard<'static, ()>,
    active: bool,
}

impl AllocationScope {
    pub fn start() -> Self {
        let exclusive = SCOPE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = AllocatorSnapshot::capture();
        SCOPE_PEAK_LIVE_BYTES.store(start.live_bytes, Ordering::Relaxed);
        SCOPE_ACTIVE.store(true, Ordering::Release);
        Self {
            start,
            _exclusive: exclusive,
            active: true,
        }
    }

    pub fn finish(mut self) -> AllocationMetrics {
        self.stop()
    }

    fn stop(&mut self) -> AllocationMetrics {
        SCOPE_ACTIVE.store(false, Ordering::Release);
        self.active = false;
        let end = AllocatorSnapshot::capture();
        let peak_live_bytes = SCOPE_PEAK_LIVE_BYTES.load(Ordering::Relaxed);
        AllocationMetrics {
            allocation_count: end
                .allocation_count
                .saturating_sub(self.start.allocation_count),
            allocated_bytes: end
                .allocated_bytes
                .saturating_sub(self.start.allocated_bytes),
            live_bytes_delta: signed_delta(end.live_bytes, self.start.live_bytes),
            peak_live_bytes_delta: peak_live_bytes.saturating_sub(self.start.live_bytes),
            large_allocation_count: end
                .large_allocation_count
                .saturating_sub(self.start.large_allocation_count),
        }
    }
}

impl Drop for AllocationScope {
    fn drop(&mut self) {
        if self.active {
            SCOPE_ACTIVE.store(false, Ordering::Release);
        }
    }
}

fn signed_delta(end: u64, start: u64) -> i64 {
    if end >= start {
        end.saturating_sub(start).min(i64::MAX as u64) as i64
    } else {
        -(start.saturating_sub(end).min(i64::MAX as u64) as i64)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BenchmarkMeasurement {
    pub elapsed_ms: f64,
    pub allocations: AllocationMetrics,
}

impl BenchmarkMeasurement {
    pub fn new(elapsed: Duration, allocations: AllocationMetrics) -> Self {
        Self {
            elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
            allocations,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BenchmarkFixture {
    pub input_bytes: usize,
    pub logical_rows: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum BenchmarkGate {
    /// The user-requested batch-write acceptance ratios.
    BulkWrite,
    /// Sparse and direct operations may regress by no more than five percent.
    ElapsedRegression,
    /// Record a stable reference without imposing a performance ratio.
    Reference,
}

impl BenchmarkGate {
    fn json(self) -> serde_json::Value {
        match self {
            Self::BulkWrite => serde_json::json!({
                "comparison": "candidate_p50_over_baseline_p50",
                "max_candidate_over_baseline": {
                    "elapsed_ms": 0.70,
                    "allocation_count": 0.40,
                    "allocated_bytes": 0.50,
                    "peak_live_bytes_delta": 0.70
                }
            }),
            Self::ElapsedRegression => serde_json::json!({
                "comparison": "candidate_p50_over_baseline_p50",
                "max_candidate_over_baseline": {
                    "elapsed_ms": 1.05
                }
            }),
            Self::Reference => serde_json::json!({
                "comparison": "reference_only",
                "max_candidate_over_baseline": {}
            }),
        }
    }
}

pub fn emit_sample(
    benchmark: &'static str,
    lane: &'static str,
    sample: usize,
    fixture: BenchmarkFixture,
    gate: BenchmarkGate,
    measurement: BenchmarkMeasurement,
) {
    emit_json(serde_json::json!({
        "schema": "lix.shared-batch-benchmark.v1",
        "kind": "sample",
        "benchmark": benchmark,
        "lane": lane,
        "sample": sample,
        "fixture": {
            "input_bytes": fixture.input_bytes,
            "logical_rows": fixture.logical_rows
        },
        "allocator": {
            "implementation": "rust_global_system_allocator",
            "large_allocation_threshold_bytes": LARGE_ALLOCATION_BYTES
        },
        "metrics": {
            "elapsed_ms": measurement.elapsed_ms,
            "allocation_count": measurement.allocations.allocation_count,
            "allocated_bytes": measurement.allocations.allocated_bytes,
            "live_bytes_delta": measurement.allocations.live_bytes_delta,
            "peak_live_bytes_delta": measurement.allocations.peak_live_bytes_delta,
            "large_allocation_count": measurement.allocations.large_allocation_count
        },
        "gate": gate.json()
    }));
}

pub fn emit_summary(
    benchmark: &'static str,
    lane: &'static str,
    fixture: BenchmarkFixture,
    gate: BenchmarkGate,
    measurements: &[BenchmarkMeasurement],
) {
    assert!(
        !measurements.is_empty(),
        "a benchmark summary needs at least one sample"
    );
    let mut elapsed_ms = measurements
        .iter()
        .map(|measurement| measurement.elapsed_ms)
        .collect::<Vec<_>>();
    let mut allocation_count = measurements
        .iter()
        .map(|measurement| measurement.allocations.allocation_count)
        .collect::<Vec<_>>();
    let mut allocated_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.allocated_bytes)
        .collect::<Vec<_>>();
    let mut live_bytes_delta = measurements
        .iter()
        .map(|measurement| measurement.allocations.live_bytes_delta)
        .collect::<Vec<_>>();
    let mut peak_live_bytes_delta = measurements
        .iter()
        .map(|measurement| measurement.allocations.peak_live_bytes_delta)
        .collect::<Vec<_>>();
    let mut large_allocation_count = measurements
        .iter()
        .map(|measurement| measurement.allocations.large_allocation_count)
        .collect::<Vec<_>>();
    elapsed_ms.sort_by(f64::total_cmp);
    allocation_count.sort_unstable();
    allocated_bytes.sort_unstable();
    live_bytes_delta.sort_unstable();
    peak_live_bytes_delta.sort_unstable();
    large_allocation_count.sort_unstable();

    emit_json(serde_json::json!({
        "schema": "lix.shared-batch-benchmark.v1",
        "kind": "summary",
        "benchmark": benchmark,
        "lane": lane,
        "samples": measurements.len(),
        "fixture": {
            "input_bytes": fixture.input_bytes,
            "logical_rows": fixture.logical_rows
        },
        "allocator": {
            "implementation": "rust_global_system_allocator",
            "large_allocation_threshold_bytes": LARGE_ALLOCATION_BYTES
        },
        "metrics": {
            "elapsed_ms": {
                "p50": median(&elapsed_ms),
                "p95": percentile_95(&elapsed_ms)
            },
            "allocation_count": {"p50": median(&allocation_count)},
            "allocated_bytes": {"p50": median(&allocated_bytes)},
            "live_bytes_delta": {"p50": median(&live_bytes_delta)},
            "peak_live_bytes_delta": {"p50": median(&peak_live_bytes_delta)},
            "large_allocation_count": {"p50": median(&large_allocation_count)}
        },
        "gate": gate.json()
    }));
}

fn emit_json(value: serde_json::Value) {
    eprintln!(
        "{MACHINE_RECORD_PREFIX}{}",
        serde_json::to_string(&value).expect("benchmark record must serialize")
    );
}

fn median<T: Copy>(sorted: &[T]) -> T {
    sorted[sorted.len() / 2]
}

fn percentile_95<T: Copy>(sorted: &[T]) -> T {
    let index = ((sorted.len() * 95).div_ceil(100)).saturating_sub(1);
    sorted[index]
}
