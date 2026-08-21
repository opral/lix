//! Allocation and scorecard support for the large ignored E2E benchmarks.
//!
//! The allocator hooks intentionally perform atomic arithmetic only. In
//! particular, they must never format, lock, or allocate: doing so would
//! recursively enter the global allocator and invalidate both the benchmark
//! and the process.

#![recursion_limit = "512"]
#![allow(unused_attributes)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use lix::plugin::runtime::WasmTransitionCounters;

pub const MACHINE_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_JSON=";
pub const TRANSITION_RECORD_PREFIX: &str = "LIX_TRANSITION_PROFILE_JSON=";
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
    pub process_rss_start_bytes: u64,
    pub process_rss_end_bytes: u64,
    pub process_rss_delta_bytes: i64,
    pub physical_puts: u64,
    pub physical_deletes: u64,
    pub physical_written_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
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

/// Current Rust global-allocator live bytes.
///
/// Ignored profiling benchmarks use this only to annotate tracing phase
/// boundaries. Reading the relaxed counter does not allocate and therefore
/// does not perturb the measured scope.
pub fn current_live_bytes() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// One exclusive allocation-measurement window.
///
/// Run ignored allocation benchmarks with an exact test filter. The lock
/// prevents two instrumented windows in this test binary from overlapping,
/// while the global counters still include allocations made by any unrelated
/// test running concurrently.
#[derive(Debug)]
pub struct AllocationScope {
    start: AllocatorSnapshot,
    process_rss_start_bytes: u64,
    _exclusive: MutexGuard<'static, ()>,
    active: bool,
}

impl AllocationScope {
    pub fn start() -> Self {
        let exclusive = SCOPE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(feature = "storage-benches")]
        let _ = lix::storage_bench::take_crud_physical_write_accounting();
        // Read /proc before taking allocator counters. The filesystem helper
        // allocates, so doing this first keeps instrumentation out of the
        // measured allocation window.
        let process_rss_start_bytes = process_rss_current_bytes();
        let start = AllocatorSnapshot::capture();
        SCOPE_PEAK_LIVE_BYTES.store(start.live_bytes, Ordering::Relaxed);
        SCOPE_ACTIVE.store(true, Ordering::Release);
        Self {
            start,
            process_rss_start_bytes,
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
        // Likewise, stop allocation accounting before reading /proc. RSS is
        // reported as start/end/delta, never as a timed peak: Linux VmHWM is a
        // process-lifetime value polluted by setup and warmups.
        let process_rss_end_bytes = process_rss_current_bytes();
        #[cfg(feature = "storage-benches")]
        let physical = lix::storage_bench::take_crud_physical_write_accounting();
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
            process_rss_start_bytes: self.process_rss_start_bytes,
            process_rss_end_bytes,
            process_rss_delta_bytes: signed_delta(
                process_rss_end_bytes,
                self.process_rss_start_bytes,
            ),
            #[cfg(feature = "storage-benches")]
            physical_puts: physical.puts,
            #[cfg(not(feature = "storage-benches"))]
            physical_puts: 0,
            #[cfg(feature = "storage-benches")]
            physical_deletes: physical.deletes,
            #[cfg(not(feature = "storage-benches"))]
            physical_deletes: 0,
            #[cfg(feature = "storage-benches")]
            physical_written_bytes: physical.written_bytes,
            #[cfg(not(feature = "storage-benches"))]
            physical_written_bytes: 0,
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

/// Current Linux resident set size at a scope boundary. Unsupported runners
/// report zero; the qualification runner marks RSS evidence unavailable there.
fn process_rss_current_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:")?.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .map(|kilobytes| kilobytes.saturating_mul(1024))
        .unwrap_or(0)
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
    /// Instrumentation-only output; no checked-in baseline is available yet.
    InstrumentationOnly,
    /// The user-requested batch-write acceptance ratios.
    BulkWrite,
    /// Sparse and direct operations use the shared 10% p50 / 15% p95 envelope.
    ElapsedRegression,
}

/// Emits the non-timing evidence for one measured transition. Keeping this a
/// separate record lets the allocator window close before hashing or querying
/// correctness evidence, while preserving an exact sample join key.
pub fn emit_transition_profile(
    benchmark: &'static str,
    lane: &'static str,
    sample: usize,
    counters: WasmTransitionCounters,
    correctness: serde_json::Value,
) {
    let phases = correctness
        .get("phases_ms")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    eprintln!(
        "{TRANSITION_RECORD_PREFIX}{}",
        serde_json::to_string(&serde_json::json!({
            "schema": "lix.universal-plugin-transition-profile.v1",
            "contract": "typed-row-fixtures-v2",
            "benchmark": benchmark,
            "lane": lane,
            "sample": sample,
            "phases_ms": phases,
            "correctness": correctness,
            "counters": {
                "source_read_calls": counters.source_read_calls,
                "source_bytes_read": counters.source_bytes_read,
                "file_read_calls": counters.file_read_calls,
                "file_bytes_read": counters.file_bytes_read,
                "state_read_calls": counters.state_read_calls,
                "state_key_bytes": counters.state_key_bytes,
                "state_value_bytes_read": counters.state_value_bytes_read,
                "row_input_pages": counters.row_input_pages,
                "row_input_records": counters.row_input_records,
                "row_input_wire_bytes": counters.row_input_wire_bytes,
                "row_output_pages": counters.row_output_pages,
                "row_output_records": counters.row_output_records,
                "row_output_wire_bytes": counters.row_output_wire_bytes,
                "row_input_attachment_reads": counters.row_input_attachment_reads,
                "row_input_attachment_bytes": counters.row_input_attachment_bytes,
                "row_output_attachment_writes": counters.row_output_attachment_writes,
                "row_output_attachment_bytes": counters.row_output_attachment_bytes,
                "typed_row_decode_records": counters.typed_row_decode_records,
                "typed_row_decode_bytes": counters.typed_row_decode_bytes,
                "typed_row_decode_nanos": counters.typed_row_decode_nanos,
                "typed_row_encode_records": counters.typed_row_encode_records,
                "typed_row_encode_bytes": counters.typed_row_encode_bytes,
                "typed_row_schema_validation_calls": counters.typed_row_schema_validation_calls,
                "typed_row_schema_validation_bytes": counters.typed_row_schema_validation_bytes,
                "typed_row_schema_validation_nanos": counters.typed_row_schema_validation_nanos,
                "typed_transaction_validation_calls": counters.typed_transaction_validation_calls,
                "typed_transaction_validation_bytes": counters.typed_transaction_validation_bytes,
                "row_page_callback_calls": counters.row_page_callback_calls,
                "row_input_page_eof_callbacks": counters.row_input_page_eof_callbacks,
                "outer_row_json_parse_calls": counters.outer_row_json_parse_calls,
                "outer_row_json_parse_bytes": counters.outer_row_json_parse_bytes,
                "outer_row_json_serialize_calls": counters.outer_row_json_serialize_calls,
                "outer_row_json_serialize_bytes": counters.outer_row_json_serialize_bytes,
                "outer_row_json_canonicalize_calls": counters.outer_row_json_canonicalize_calls,
                "outer_row_json_canonicalize_bytes": counters.outer_row_json_canonicalize_bytes,
                "outer_row_json_dom_fallback_calls": counters.outer_row_json_dom_fallback_calls,
                "outer_row_json_dom_fallback_bytes": counters.outer_row_json_dom_fallback_bytes,
                "component_import_calls": counters.component_import_calls,
                "guest_export_calls": counters.guest_export_calls,
                "component_boundary_bytes": counters.component_boundary_bytes,
                "guest_linear_memory_high_water_bytes": counters.guest_linear_memory_high_water_bytes,
                "host_full_diff_bytes_compared": counters.host_full_diff_bytes_compared,
                "host_content_classification_bytes": counters.host_content_classification_bytes,
                "full_state_semantic_rows_materialized": counters.full_state_semantic_rows_materialized,
                "durable_semantic_changes": counters.durable_semantic_changes,
                "private_document_cache_hits": counters.private_document_cache_hits,
                "shared_renderer_cache_hits": counters.shared_renderer_cache_hits,
                "full_document_reparses": counters.full_document_reparses,
                "full_renderer_invocations": counters.full_renderer_invocations,
                "filesystem_sync_full_renders": counters.filesystem_sync_full_renders,
                "conflict_resolution_calls": counters.conflict_resolution_calls,
                "conflict_resolution_records": counters.conflict_resolution_records,
                "conflict_resolution_takes": counters.conflict_resolution_takes
            }
        }))
        .expect("transition profile record must serialize")
    );
}

impl BenchmarkGate {
    fn json(self) -> serde_json::Value {
        match self {
            Self::InstrumentationOnly => serde_json::json!({
                "comparison": "instrumentation_only",
                "correctness": "exact_hashes_and_cardinality",
                "baseline": "not_recorded"
            }),
            Self::BulkWrite => serde_json::json!({
                "comparison": "candidate_over_matched_baseline",
                "max_candidate_over_baseline": {
                    "elapsed_ms_p50": 1.10,
                    "elapsed_ms_p95": 1.15,
                    "allocated_bytes_p50": 1.10,
                    "peak_live_bytes_delta_p50": 1.10,
                    "guest_linear_memory_peak": 1.10,
                    "process_rss_delta_bytes_p50": 1.10
                },
                "allocation_count": {
                    "max_candidate_over_baseline": 1.20,
                    "only_when_allocated_bytes_improve": true
                },
                "correctness": "exact_hashes_and_cardinality"
            }),
            Self::ElapsedRegression => serde_json::json!({
                "comparison": "candidate_over_matched_baseline",
                "max_candidate_over_baseline": {
                    "elapsed_ms_p50": 1.10,
                    "elapsed_ms_p95": 1.15,
                    "allocated_bytes_p50": 1.10,
                    "peak_live_bytes_delta_p50": 1.10,
                    "guest_linear_memory_peak": 1.10,
                    "process_rss_delta_bytes_p50": 1.10
                },
                "correctness": "exact_hashes_and_cardinality"
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
        "contract": "typed-row-fixtures-v2",
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
            "large_allocation_count": measurement.allocations.large_allocation_count,
            "process_rss_start_bytes": measurement.allocations.process_rss_start_bytes,
            "process_rss_end_bytes": measurement.allocations.process_rss_end_bytes,
            "process_rss_delta_bytes": measurement.allocations.process_rss_delta_bytes,
            "physical_puts": measurement.allocations.physical_puts,
            "physical_deletes": measurement.allocations.physical_deletes,
            "physical_written_bytes": measurement.allocations.physical_written_bytes
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
    let mut process_rss_start_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.process_rss_start_bytes)
        .collect::<Vec<_>>();
    let mut process_rss_end_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.process_rss_end_bytes)
        .collect::<Vec<_>>();
    let mut process_rss_delta_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.process_rss_delta_bytes)
        .collect::<Vec<_>>();
    let mut physical_puts = measurements
        .iter()
        .map(|measurement| measurement.allocations.physical_puts)
        .collect::<Vec<_>>();
    let mut physical_deletes = measurements
        .iter()
        .map(|measurement| measurement.allocations.physical_deletes)
        .collect::<Vec<_>>();
    let mut physical_written_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.physical_written_bytes)
        .collect::<Vec<_>>();
    elapsed_ms.sort_by(f64::total_cmp);
    allocation_count.sort_unstable();
    allocated_bytes.sort_unstable();
    live_bytes_delta.sort_unstable();
    peak_live_bytes_delta.sort_unstable();
    large_allocation_count.sort_unstable();
    process_rss_start_bytes.sort_unstable();
    process_rss_end_bytes.sort_unstable();
    process_rss_delta_bytes.sort_unstable();
    physical_puts.sort_unstable();
    physical_deletes.sort_unstable();
    physical_written_bytes.sort_unstable();

    emit_json(serde_json::json!({
        "schema": "lix.shared-batch-benchmark.v1",
        "contract": "typed-row-fixtures-v2",
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
        "environment": benchmark_environment(),
        "metrics": {
            "elapsed_ms": {
                "p50": median(&elapsed_ms),
                "p95": percentile_95(&elapsed_ms)
            },
            "allocation_count": {"p50": median(&allocation_count)},
            "allocated_bytes": {"p50": median(&allocated_bytes)},
            "live_bytes_delta": {"p50": median(&live_bytes_delta)},
            "peak_live_bytes_delta": {"p50": median(&peak_live_bytes_delta)},
            "large_allocation_count": {"p50": median(&large_allocation_count)},
            "process_rss_start_bytes": {"p50": median(&process_rss_start_bytes)},
            "process_rss_end_bytes": {"p50": median(&process_rss_end_bytes)},
            "process_rss_delta_bytes": {"p50": median(&process_rss_delta_bytes)},
            "physical_puts": {
                "p50": median(&physical_puts),
                "p95": percentile_95(&physical_puts)
            },
            "physical_deletes": {
                "p50": median(&physical_deletes),
                "p95": percentile_95(&physical_deletes)
            },
            "physical_written_bytes": {
                "p50": median(&physical_written_bytes),
                "p95": percentile_95(&physical_written_bytes)
            }
        },
        "gate": gate.json()
    }));
}

fn benchmark_environment() -> serde_json::Value {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|value| {
            value.lines().find_map(|line| {
                line.strip_prefix("model name:")
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
            })
        });
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|value| value.trim().to_owned());
    serde_json::json!({
        "target_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "allocator": "rust_global_system_allocator",
        "cpu_model": cpu,
        "kernel": kernel,
        "storage_backend": "open_lix_default"
    })
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

#[cfg(test)]
mod tests {
    use super::BenchmarkGate;

    #[test]
    fn bulk_write_gate_covers_every_requested_resource_metric() {
        let gate = BenchmarkGate::BulkWrite.json();
        let thresholds = gate["max_candidate_over_baseline"]
            .as_object()
            .expect("bulk-write thresholds must be an object");
        assert_eq!(thresholds["elapsed_ms_p50"].as_f64(), Some(1.10));
        assert_eq!(thresholds["elapsed_ms_p95"].as_f64(), Some(1.15));
        assert_eq!(thresholds["allocated_bytes_p50"].as_f64(), Some(1.10));
        assert_eq!(thresholds["peak_live_bytes_delta_p50"].as_f64(), Some(1.10));
        assert_eq!(
            gate["allocation_count"]["max_candidate_over_baseline"].as_f64(),
            Some(1.20)
        );
        assert_eq!(
            gate["allocation_count"]["only_when_allocated_bytes_improve"].as_bool(),
            Some(true)
        );
    }
}
