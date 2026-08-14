#![allow(clippy::large_futures)]

use std::time::{Duration, Instant};
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
use std::{
    alloc::GlobalAlloc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main};

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
struct CountingAllocator;

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static ALLOCATION_ACCOUNTING_ENABLED: AtomicBool = AtomicBool::new(false);

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null() && new_size >= layout.size() {
            record_allocation(new_size - layout.size());
        }
        replacement
    }
}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn record_allocation(bytes: usize) {
    if ALLOCATION_ACCOUNTING_ENABLED.load(Ordering::Relaxed) {
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn reset_allocation_accounting() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_ACCOUNTING_ENABLED.store(
        std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_ALLOCATION_BYTES").is_some(),
        Ordering::Relaxed,
    );
}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn print_allocation_accounting(phase: &str) {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_ALLOCATION_BYTES").is_some() {
        ALLOCATION_ACCOUNTING_ENABLED.store(false, Ordering::Relaxed);
        println!(
            "tracked_state_crud allocation phase: {phase} allocated_bytes={} allocation_calls={}",
            ALLOCATED_BYTES.load(Ordering::Relaxed),
            ALLOCATION_CALLS.load(Ordering::Relaxed),
        );
    }
}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn finish_allocation_accounting() -> (u64, u64) {
    ALLOCATION_ACCOUNTING_ENABLED.store(false, Ordering::Relaxed);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn reset_allocation_accounting() {}
#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn print_allocation_accounting(_phase: &str) {}
#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn finish_allocation_accounting() -> (u64, u64) {
    (0, 0)
}

mod accounting;
mod io_stats;
mod kv_layout;
mod sql_session;
mod storage;
mod transaction_api;
mod workload;

use storage::{KV_STORAGE_PROFILES, STORAGE_PROFILES, StorageProfile};
use workload::{REAL_WORKLOAD_ROWS, SMOKE_ROWS, WorkloadRow, fixture_rows, row_label};

const READ_MANY_PK_COUNT: usize = 10;

fn tracked_state_crud_benches(c: &mut Criterion) {
    init_perf_tracing();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for tracked_state_crud benchmarks");
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE").is_some() {
        let row_count = profile_row_count();
        if std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_LAYER").as_deref()
            == Ok("sql_session_bound")
            && std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS").is_none()
        {
            assert_eq!(
                std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_OP").as_deref(),
                Ok("update_all"),
                "sql_session_bound only supports update_all"
            );
            profile_sql_session_bound_updates(
                &runtime,
                row_count,
                READ_MANY_PK_COUNT.min(row_count),
                profile_sample_count(),
                profile_sql_session_storage(),
            );
            return;
        }
        let rows = fixture_rows(row_count);
        profile_operation(&runtime, &rows);
        return;
    }
    let accounting_rows = accounting_row_count();
    let rows = fixture_rows(REAL_WORKLOAD_ROWS.max(accounting_rows));
    io_stats::maybe_print_io_report();
    accounting::maybe_print_accounting_report(&runtime, &rows[..accounting_rows]);

    for (label, row_count) in [("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)] {
        for &profile in KV_STORAGE_PROFILES {
            bench_kv_layout(c, &runtime, profile, &rows[..row_count], label);
        }
        for &profile in STORAGE_PROFILES {
            bench_transaction_api(c, &runtime, profile, &rows[..row_count], label);
            bench_sql_session(c, &runtime, profile, &rows[..row_count], label);
        }
    }
}

fn accounting_row_count() -> usize {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_ACCOUNTING").is_none() {
        return SMOKE_ROWS;
    }
    let Some(value) = std::env::var_os("LIX_TRACKED_STATE_CRUD_ACCOUNTING_ROW_COUNT") else {
        return SMOKE_ROWS;
    };
    let value = value.to_string_lossy();
    let row_count = value.parse::<usize>().unwrap_or_else(|_| {
        panic!(
            "LIX_TRACKED_STATE_CRUD_ACCOUNTING_ROW_COUNT must be a positive integer, got '{value}'"
        )
    });
    assert!(
        row_count > 0,
        "LIX_TRACKED_STATE_CRUD_ACCOUNTING_ROW_COUNT must be positive"
    );
    row_count
}

fn profile_sample_count() -> usize {
    std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or(15)
}

fn init_perf_tracing() {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_target(false)
            .try_init();
    }
}

/// Profile mode defaults to the representative 10k fixture and extends it with
/// deterministic ordered rows for explicit scaling studies without inflating
/// the steady-state Criterion benchmark definitions.
fn profile_row_count() -> usize {
    let Some(value) = std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_ROW_COUNT") else {
        return REAL_WORKLOAD_ROWS;
    };
    let value = value.to_string_lossy();
    let row_count = value.parse::<usize>().unwrap_or_else(|_| {
        panic!("LIX_TRACKED_STATE_CRUD_PROFILE_ROW_COUNT must be a positive integer, got '{value}'")
    });
    assert!(
        row_count > 0,
        "LIX_TRACKED_STATE_CRUD_PROFILE_ROW_COUNT must be positive"
    );
    row_count
}

fn profile_bound_update_row_count(max_rows: usize) -> usize {
    let Some(value) = std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_BOUND_UPDATE_ROW_COUNT")
    else {
        return max_rows;
    };
    let value = value.to_string_lossy();
    let row_count = value.parse::<usize>().unwrap_or_else(|_| {
        panic!(
            "LIX_TRACKED_STATE_CRUD_PROFILE_BOUND_UPDATE_ROW_COUNT must be an integer between 1 and {max_rows}, got '{value}'"
        )
    });
    assert!(
        (1..=max_rows).contains(&row_count),
        "LIX_TRACKED_STATE_CRUD_PROFILE_BOUND_UPDATE_ROW_COUNT must be between 1 and {max_rows}, got {row_count}"
    );
    row_count
}

fn profile_bound_update_spread() -> bool {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_BOUND_UPDATE_DISTRIBUTION").as_deref() {
        Ok("spread") => true,
        Ok("prefix") | Err(_) => false,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_BOUND_UPDATE_DISTRIBUTION '{other}'; expected prefix or spread"
        ),
    }
}

/// Reproducible, setup-excluded latency samples for one production transaction
/// operation. Criterion's CodSpeed-compatible harness intentionally delegates
/// timing to the runner, while this opt-in mode is useful for local profiling
/// and before/after investigation. Each sample starts from an independently
/// seeded fixture, matching the benchmark's measurement boundary. For
/// `read_many`, `LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_PK_COUNT` selects
/// the setup-excluded multi-point query shape.
fn profile_operation(runtime: &tokio::runtime::Runtime, rows: &[WorkloadRow]) {
    if let Ok(operation @ ("columnar_history" | "columnar_diff")) =
        std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_OP").as_deref()
    {
        profile_columnar_semantic_operation(
            runtime,
            rows,
            operation,
            profile_sample_count(),
            profile_sql_session_storage(),
        );
        return;
    }
    let operation = match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_OP").as_deref() {
        Ok("insert_all") => TransactionBenchOp::InsertAll,
        Ok("read_one") => TransactionBenchOp::ReadOneByPk,
        Ok("read_many") => TransactionBenchOp::ReadManyByPk,
        Ok("update_all") => TransactionBenchOp::UpdateAll,
        Ok("update_one") => TransactionBenchOp::UpdateOneByPk,
        Ok("delete_all") => TransactionBenchOp::DeleteAll,
        Ok("delete_one") => TransactionBenchOp::DeleteOneByPk,
        Ok("read_all") | Err(_) => TransactionBenchOp::ReadAll,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_OP '{other}'; expected insert_all, read_all, read_one, read_many, update_all, update_one, delete_all, or delete_one"
        ),
    };
    if sql_session_read_shape() != "full_result" {
        assert_eq!(
            std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_LAYER").as_deref(),
            Ok("sql_session"),
            "non-default LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE values require LIX_TRACKED_STATE_CRUD_PROFILE_LAYER=sql_session"
        );
    }
    let read_many_pk_count = profile_read_many_pk_count(operation, rows.len());
    let sample_count = profile_sample_count();
    let hot_repeats = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS")
        .ok()
        .map(|value| {
            let count = value.parse::<usize>().unwrap_or_else(|_| {
                panic!(
                    "LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must be a positive integer, got '{value}'"
                )
            });
            assert!(
                count > 0,
                "LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must be a positive integer"
            );
            count
        });
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_LAYER").as_deref() {
        Ok("kv_layout") => {
            assert!(
                hot_repeats.is_none(),
                "LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS is unavailable for the kv_layout layer"
            );
            profile_kv_layout_operation(runtime, rows, operation, read_many_pk_count, sample_count);
        }
        Ok("sql_session") => {
            let profile = profile_sql_session_storage();
            if let Some(repeats) = hot_repeats {
                profile_hot_sql_session_operations(
                    runtime,
                    rows,
                    operation,
                    read_many_pk_count,
                    repeats,
                    profile,
                );
            } else {
                profile_sql_session_operation(
                    runtime,
                    rows,
                    operation,
                    read_many_pk_count,
                    sample_count,
                    profile,
                );
            }
        }
        Ok("sql_session_bound") => {
            assert!(
                matches!(operation, TransactionBenchOp::UpdateAll),
                "sql_session_bound only supports update_all"
            );
            let profile = profile_sql_session_storage();
            if let Some(repeats) = hot_repeats {
                profile_hot_sql_session_bound_updates(
                    runtime,
                    rows,
                    read_many_pk_count,
                    repeats,
                    profile,
                );
            } else {
                profile_sql_session_bound_updates(
                    runtime,
                    rows.len(),
                    read_many_pk_count,
                    sample_count,
                    profile,
                );
            }
        }
        Ok("transaction") | Err(_) => {
            let profile = profile_transaction_storage();
            if let Some(repeats) = hot_repeats {
                profile_hot_transaction_operations(
                    runtime,
                    rows,
                    operation,
                    read_many_pk_count,
                    repeats,
                    profile,
                );
            } else {
                profile_transaction_operation(
                    runtime,
                    rows,
                    operation,
                    read_many_pk_count,
                    sample_count,
                    profile,
                );
            }
        }
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_LAYER '{other}'; expected transaction, sql_session, sql_session_bound, or kv_layout"
        ),
    }
}

fn profile_columnar_semantic_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: &str,
    sample_count: usize,
    profile: StorageProfile,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let fixture = runtime.block_on(sql_session::empty_fixture_with_read_many_pk_count(
            profile,
            rows,
            READ_MANY_PK_COUNT.min(rows.len()),
        ));
        let baseline = runtime.block_on(fixture.active_commit_id());
        runtime.block_on(fixture.insert_all());
        let head = runtime.block_on(fixture.active_commit_id());
        let start = Instant::now();
        let result = match operation {
            "columnar_history" => runtime.block_on(fixture.columnar_history_count(&head)),
            "columnar_diff" => runtime.block_on(fixture.columnar_diff_count(&baseline, &head)),
            _ => unreachable!("columnar semantic operation was validated"),
        };
        samples.push(start.elapsed());
        black_box(result);
    }
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    println!(
        "tracked_state_crud profile: sql_session/{}/{operation}/{} samples: median={:?} min={:?} max={:?}",
        profile.name(),
        sample_count,
        sorted[sorted.len() / 2],
        sorted[0],
        sorted[sorted.len() - 1],
    );
}

fn profile_read_many_pk_count(operation: TransactionBenchOp, row_count: usize) -> usize {
    let Ok(value) = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_PK_COUNT") else {
        return READ_MANY_PK_COUNT.min(row_count);
    };
    assert!(
        matches!(operation, TransactionBenchOp::ReadManyByPk),
        "LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_PK_COUNT only supports read_many"
    );
    let count = value.parse::<usize>().unwrap_or_else(|_| {
        panic!(
            "LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_PK_COUNT must be an integer between 1 and {row_count}, got '{value}'"
        )
    });
    assert!(
        (1..=row_count).contains(&count),
        "LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_PK_COUNT must be between 1 and {row_count}, got {count}"
    );
    count
}

fn profile_sql_session_storage() -> StorageProfile {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE").as_deref() {
        Ok("rocksdb") | Err(_) => StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb") => StorageProfile::SlateDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb_remote") => StorageProfile::SlateDBRemoteObjectStore,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE '{other}'; expected rocksdb, slatedb, or slatedb_remote"
        ),
    }
}

fn profile_transaction_storage() -> StorageProfile {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE").as_deref() {
        Ok("rocksdb") | Err(_) => StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb") => StorageProfile::SlateDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb_remote") => StorageProfile::SlateDBRemoteObjectStore,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE '{other}'; expected rocksdb, slatedb, or slatedb_remote"
        ),
    }
}

/// Keeps one seeded fixture alive for a repeatable profiling window. This
/// deliberately trades representative cache behavior for a trace dominated
/// by the operation itself rather than fixture construction.
fn profile_hot_transaction_operations(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    repeats: usize,
    profile: StorageProfile,
) {
    operation.assert_supports_hot_repeats();
    if matches!(operation, TransactionBenchOp::DeleteOneByPk) {
        assert!(
            repeats <= rows.len(),
            "delete_one hot repeats must not exceed the seeded row count"
        );
    }
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = runtime.block_on(transaction_api::seeded_fixture(profile, rows));
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(operation.run(&mut fixture, read_many_pk_count));
    }
    let elapsed = start.elapsed();
    let profile_detail = profile_read_many_detail(operation, read_many_pk_count);
    println!(
        "tracked_state_crud hot profile: transaction/{}/{}/{} repeats{profile_detail}: total={elapsed:?} per_operation={:?}",
        profile.name(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

fn sql_session_read_shape() -> &'static str {
    let shape = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE");
    if let Ok(label) = shape.as_deref()
        && let Some(shape) = sql_session::OlapReadShape::from_label(label)
    {
        return shape.label();
    }
    match shape.as_deref() {
        Ok("aggregate_count") => "aggregate_count",
        Ok("general_filter_sort") => "general_filter_sort",
        Ok("general_aggregate") => "general_aggregate",
        Ok("full_result") | Err(_) => "full_result",
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE '{other}'; expected full_result, aggregate_count, general_filter_sort, general_aggregate, olap_scan, olap_filter, olap_sort, olap_group, or olap_aggregate"
        ),
    }
}

fn profile_hot_sql_session_operations(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    repeats: usize,
    profile: StorageProfile,
) {
    operation.assert_supports_hot_repeats();
    assert!(
        !matches!(operation, TransactionBenchOp::DeleteOneByPk),
        "delete_one hot repeats are only available for the transaction layer"
    );
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let fixture = runtime.block_on(sql_session::seeded_fixture_with_read_many_pk_count(
        profile,
        rows,
        read_many_pk_count,
    ));
    let _ = lix::storage_bench::take_row_point_snapshot_cache_accounting();
    if sql_session::selected_olap_read_shape().is_some() {
        // Full semantic validation is intentionally outside the samples. This
        // keeps Rust-vs-JavaScript assertion work out of the engine ratio while
        // still validating the exact post-mutation public result once.
        runtime.block_on(fixture.validate_selected_olap());
        // Match the DuckDB control: one untimed warmup against one seeded and
        // optionally mutated fixture, followed by timed materializations.
        runtime.block_on(run_sql_session_operation(operation, &fixture));
        let _ = fixture.take_io_stats();
        let mut samples = Vec::with_capacity(repeats);
        let mut row_count = 0;
        for sample in 1..=repeats {
            reset_allocation_accounting();
            let cpu_before = process_cpu_micros();
            let rss_before = process_resident_bytes();
            let start = Instant::now();
            row_count += runtime.block_on(run_sql_session_operation(operation, &fixture));
            let elapsed = start.elapsed();
            let cpu_us = process_cpu_micros().saturating_sub(cpu_before);
            let rss_after = process_resident_bytes();
            let (allocated_bytes, allocation_calls) = finish_allocation_accounting();
            let io = fixture.take_io_stats();
            samples.push(elapsed);
            println!(
                "tracked_state_crud olap_sample: sample={sample} backend={} shape={} rows={} wall_us={:.3} cpu_us={cpu_us} alloc_bytes={allocated_bytes} alloc_calls={allocation_calls} rss_before_bytes={rss_before} rss_after_bytes={rss_after} get_calls={} get_keys={} get_values={} get_value_bytes={} scan_calls={} scan_entries={} scan_value_bytes={} write_batches={} write_bytes={} settled_bytes={}",
                profile.name(),
                sql_session_read_shape(),
                rows.len(),
                elapsed.as_secs_f64() * 1_000_000.0,
                io.get_calls,
                io.get_keys,
                io.get_values,
                io.get_value_bytes,
                io.scan_calls,
                io.scan_entries,
                io.scan_value_bytes,
                io.write_batches,
                io.write_bytes,
                fixture.settled_bytes(),
            );
        }
        let digest = runtime.block_on(fixture.selected_olap_digest());
        println!(
            "tracked_state_crud olap_digest: backend={} shape={} rows={} digest={digest:016x} settled_bytes={}",
            profile.name(),
            sql_session_read_shape(),
            rows.len(),
            fixture.settled_bytes(),
        );
        let (reopen_digest, settled_bytes) = runtime.block_on(fixture.cold_reopen_validate());
        assert_eq!(reopen_digest, digest, "cold-reopen OLAP digest changed");
        println!(
            "tracked_state_crud olap_reopen: backend={} shape={} rows={} digest={reopen_digest:016x} settled_bytes={settled_bytes} status=pass",
            profile.name(),
            sql_session_read_shape(),
            rows.len(),
        );
        print_profile_samples(
            &format!(
                "sql_session/{}/{}/{}",
                profile.name(),
                sql_session_read_shape(),
                sql_session::selected_olap_mutation_profile().label(),
            ),
            operation,
            read_many_pk_count,
            samples,
        );
        black_box(row_count);
        return;
    }
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(run_sql_session_operation(operation, &fixture));
    }
    let elapsed = start.elapsed();
    let profile_detail = format!(
        "{} read_shape={}",
        profile_read_many_detail(operation, read_many_pk_count),
        sql_session_read_shape()
    );
    println!(
        "tracked_state_crud hot profile: sql_session/{}/{}/{} repeats{profile_detail}: total={elapsed:?} per_operation={:?}",
        profile.name(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_READ_AFTER_HOT_UPDATES").is_some()
        && matches!(operation, TransactionBenchOp::UpdateOneByPk)
    {
        let _ = runtime.block_on(fixture.read_many_by_pk());
        let mut samples = Vec::with_capacity(profile_sample_count());
        for _ in 0..profile_sample_count() {
            let started = Instant::now();
            black_box(runtime.block_on(fixture.read_many_by_pk()));
            samples.push(started.elapsed());
        }
        print_profile_samples(
            &format!(
                "sql_session/{}/warm_after_{repeats}_updates",
                profile.name()
            ),
            TransactionBenchOp::ReadManyByPk,
            read_many_pk_count,
            samples,
        );
    }
    if matches!(operation, TransactionBenchOp::ReadOneByPk) {
        let cache = lix::storage_bench::take_row_point_snapshot_cache_accounting();
        println!(
            "tracked_state_crud point cache accounting: hits={} misses={}",
            cache.hits, cache.misses
        );
    }
    black_box(row_count);
}

fn profile_hot_sql_session_bound_updates(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    read_many_pk_count: usize,
    repeats: usize,
    profile: StorageProfile,
) {
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let bound_update_row_count = profile_bound_update_row_count(rows.len());
    let spread = profile_bound_update_spread();
    let fixture = runtime.block_on(sql_session::seeded_fixture_with_read_many_pk_count(
        profile,
        rows,
        read_many_pk_count,
    ));
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += if spread {
            runtime.block_on(fixture.update_spread_bound_rows(bound_update_row_count))
        } else if bound_update_row_count == rows.len() {
            runtime.block_on(fixture.update_all_bound())
        } else {
            runtime.block_on(fixture.update_bound_rows(bound_update_row_count))
        };
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: sql_session_bound/{}/update_all/{repeats} repeats: total={elapsed:?} per_operation={:?}",
        profile.name(),
        elapsed / repeats_u32,
    );
    // Keep the post-update serving cost outside the timed mutation window.
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_READ_AFTER_HOT_UPDATES").is_some() {
        let read_start = Instant::now();
        let read_rows = runtime.block_on(run_sql_session_operation(
            TransactionBenchOp::ReadAll,
            &fixture,
        ));
        println!(
            "tracked_state_crud hot profile: sql_session_bound/{}/read_all_after_{repeats}_updates: elapsed={:?}",
            profile.name(),
            read_start.elapsed(),
        );
        black_box(read_rows);
    }
    black_box(row_count);
}

#[cfg(target_os = "linux")]
fn maybe_print_profile_rss_phase(phase: &str) {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_RSS_PHASES").is_none() {
        return;
    }
    let status = std::fs::read_to_string("/proc/self/status").expect("read process status");
    let rss = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .unwrap_or("VmRSS: unavailable");
    let high_water = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .unwrap_or("VmHWM: unavailable");
    println!("tracked_state_crud rss phase: {phase} {rss} {high_water}");
}

#[cfg(not(target_os = "linux"))]
fn maybe_print_profile_rss_phase(_phase: &str) {}

fn profile_transaction_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    sample_count: usize,
    profile: StorageProfile,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = if operation.needs_seed() {
            runtime.block_on(transaction_api::seeded_fixture(profile, rows))
        } else {
            runtime.block_on(transaction_api::empty_fixture(profile, rows))
        };
        let start = Instant::now();
        let result = runtime.block_on(operation.run(&mut fixture, read_many_pk_count));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        &format!("transaction/{}", profile.name()),
        operation,
        read_many_pk_count,
        samples,
    );
}

fn profile_kv_layout_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    sample_count: usize,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = if operation.needs_seed() {
            runtime.block_on(kv_layout::seeded_fixture(StorageProfile::RocksDB, rows))
        } else {
            runtime.block_on(kv_layout::empty_fixture(StorageProfile::RocksDB, rows))
        };
        let start = Instant::now();
        let result = runtime.block_on(run_kv_layout_operation(
            operation,
            &mut fixture,
            read_many_pk_count,
        ));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        "kv_layout/lix_rocksdb",
        operation,
        read_many_pk_count,
        samples,
    );
}

async fn run_kv_layout_operation(
    operation: TransactionBenchOp,
    fixture: &mut kv_layout::KvFixture,
    read_many_pk_count: usize,
) -> usize {
    match operation {
        TransactionBenchOp::InsertAll => fixture.insert_all().await,
        TransactionBenchOp::ReadAll => fixture.read_all().await,
        TransactionBenchOp::ReadOneByPk => fixture.read_one_by_pk().await,
        TransactionBenchOp::ReadManyByPk => fixture.read_many_by_pk(read_many_pk_count).await,
        TransactionBenchOp::UpdateAll => fixture.update_all().await,
        TransactionBenchOp::UpdateOneByPk => fixture.update_one_by_pk().await,
        TransactionBenchOp::DeleteAll => fixture.delete_all().await,
        TransactionBenchOp::DeleteOneByPk => fixture.delete_one_by_pk().await,
    }
}

fn profile_sql_session_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    sample_count: usize,
    profile: StorageProfile,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        maybe_print_profile_rss_phase("before_seed");
        let fixture = if operation.needs_seed() {
            runtime.block_on(sql_session::seeded_fixture_with_read_many_pk_count(
                profile,
                rows,
                read_many_pk_count,
            ))
        } else {
            runtime.block_on(sql_session::empty_fixture_with_read_many_pk_count(
                profile,
                rows,
                read_many_pk_count,
            ))
        };
        maybe_print_profile_rss_phase("after_seed");
        reset_allocation_accounting();
        let ownership_accounting =
            std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_OWNERSHIP_ACCOUNTING").is_some();
        if ownership_accounting {
            lix::storage_bench::begin_crud_ownership_accounting();
        }
        let _ = lix::storage_bench::take_crud_current_state_scoped_range_accounting();
        if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_WRITE_ACCOUNTING").is_some() {
            let _ = lix::storage_bench::take_crud_physical_write_accounting();
            let _ = lix::storage_bench::take_crud_commit_state_manifest_bytes();
        }
        let start = Instant::now();
        let result = runtime.block_on(run_sql_session_operation(operation, &fixture));
        samples.push(start.elapsed());
        black_box(result);
        maybe_print_profile_rss_phase("after_operation");
        print_allocation_accounting("operation");
        if ownership_accounting {
            print_ownership_accounting(lix::storage_bench::take_crud_ownership_accounting());
        }
        if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_WRITE_ACCOUNTING").is_some() {
            let physical = lix::storage_bench::take_crud_physical_write_accounting();
            let manifest_bytes = lix::storage_bench::take_crud_commit_state_manifest_bytes();
            println!(
                "tracked_state_crud write accounting: staged_puts={} staged_deletes={} staged_value_bytes={} commit_state_manifest_value_bytes={}",
                physical.puts, physical.deletes, physical.written_bytes, manifest_bytes,
            );
        }
    }
    let profile_layer = if matches!(operation, TransactionBenchOp::ReadAll) {
        format!(
            "sql_session/{}/{}",
            profile.name(),
            sql_session_read_shape()
        )
    } else {
        format!("sql_session/{}", profile.name())
    };
    print_profile_samples(&profile_layer, operation, read_many_pk_count, samples);
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_ROUTE_ACCOUNTING").is_some() {
        let accounting = lix::storage_bench::take_crud_current_state_scoped_range_accounting();
        println!(
            "tracked_state_crud current-state scoped-range accounting: attempts={} hits={} errors={} sealed_manifest_loads={} replay_manifest_loads={} ordered_delta_fallbacks={} commit_delta_direct_segments={} commit_delta_direct_rows={} commit_delta_generic_segments={} commit_delta_generic_rows={}",
            accounting.attempts,
            accounting.hits,
            accounting.errors,
            accounting.sealed_manifest_loads,
            accounting.replay_manifest_loads,
            accounting.ordered_delta_fallbacks,
            accounting.commit_delta_direct_segments,
            accounting.commit_delta_direct_rows,
            accounting.commit_delta_generic_segments,
            accounting.commit_delta_generic_rows,
        );
    }
}

fn profile_sql_session_bound_updates(
    runtime: &tokio::runtime::Runtime,
    row_count: usize,
    read_many_pk_count: usize,
    sample_count: usize,
    profile: StorageProfile,
) {
    let bound_update_row_count = profile_bound_update_row_count(row_count);
    let spread = profile_bound_update_spread();
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        maybe_print_profile_rss_phase("before_seed");
        reset_allocation_accounting();
        let rows = fixture_rows(row_count);
        let fixture = runtime.block_on(
            sql_session::seeded_bound_update_fixture_with_read_many_pk_count(
                profile,
                rows,
                read_many_pk_count,
            ),
        );
        maybe_print_profile_rss_phase("after_seed");
        print_allocation_accounting("seed");
        reset_allocation_accounting();
        let ownership_accounting =
            std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_OWNERSHIP_ACCOUNTING").is_some();
        if ownership_accounting {
            lix::storage_bench::begin_crud_ownership_accounting();
        }
        let _ = lix::storage_bench::take_crud_physical_write_accounting();
        let _ = lix::storage_bench::take_crud_commit_state_manifest_bytes();
        let _ = lix::storage_bench::take_crud_current_state_scoped_range_fallbacks();
        let _ = lix::storage_bench::take_certified_row_update_value_batch_accounting();
        let start = Instant::now();
        let result = if spread {
            runtime.block_on(fixture.update_spread_bound_rows(bound_update_row_count))
        } else if bound_update_row_count == row_count {
            runtime.block_on(fixture.update_all_bound())
        } else {
            runtime.block_on(fixture.update_bound_rows(bound_update_row_count))
        };
        samples.push(start.elapsed());
        black_box(result);
        maybe_print_profile_rss_phase("after_update");
        print_allocation_accounting("update");
        if ownership_accounting {
            print_ownership_accounting(lix::storage_bench::take_crud_ownership_accounting());
        }
        let certificate = lix::storage_bench::take_certified_row_update_value_batch_accounting();
        let physical = lix::storage_bench::take_crud_physical_write_accounting();
        let manifest_bytes = lix::storage_bench::take_crud_commit_state_manifest_bytes();
        let scoped_range_fallbacks =
            lix::storage_bench::take_crud_current_state_scoped_range_fallbacks();
        println!(
            "tracked_state_crud generated update accounting: logical_rows={bound_update_row_count} certificate_attempts={} certificate_hits={} certificate_misses={} certified_rows={} staged_puts={} staged_deletes={} staged_value_bytes={} commit_state_manifest_value_bytes={manifest_bytes} current_state_scoped_range_fallbacks={scoped_range_fallbacks}",
            certificate.attempts,
            certificate.hits,
            certificate.misses,
            certificate.certified_rows,
            physical.puts,
            physical.deletes,
            physical.written_bytes
        );
    }
    print_profile_samples(
        &format!("sql_session_bound/{}", profile.name()),
        TransactionBenchOp::UpdateAll,
        read_many_pk_count,
        samples,
    );
}

fn print_ownership_accounting(accounting: lix::storage_bench::CrudOwnershipAccounting) {
    const NAMES: [&str; lix::storage_bench::CRUD_OWNERSHIP_STAGE_COUNT] = [
        "sql_bound",
        "raw_batch",
        "raw_transfer",
        "prepared_batch",
        "prepared_clone",
        "replacement_input",
        "replacement_part",
        "authority",
        "root_publication",
        "write_set",
        "adapter",
        "mutation_journal",
        "identity_encoding",
        "normalization",
        "journal_seal",
    ];
    for (stage, (name, metric)) in NAMES.into_iter().zip(accounting.stages).enumerate() {
        if metric.rows == 0
            && metric.key_bytes == 0
            && metric.value_bytes == 0
            && metric.vec_entries == 0
            && metric.string_entries == 0
            && metric.map_entries == 0
        {
            continue;
        }
        let transfer = accounting.transfers[stage];
        println!(
            "tracked_state_crud ownership: stage={name} rows={} key_bytes={} value_bytes={} vec_entries={} string_entries={} map_entries={} created_bytes={} cloned_bytes={} retained_bytes={} dropped_bytes={}",
            metric.rows,
            metric.key_bytes,
            metric.value_bytes,
            metric.vec_entries,
            metric.string_entries,
            metric.map_entries,
            transfer.created_bytes,
            transfer.cloned_bytes,
            transfer.retained_bytes,
            transfer.dropped_bytes,
        );
    }
}

async fn run_sql_session_operation(
    operation: TransactionBenchOp,
    fixture: &sql_session::SqlFixture,
) -> usize {
    match operation {
        TransactionBenchOp::InsertAll => fixture.insert_all().await,
        TransactionBenchOp::ReadAll => fixture.read_all().await,
        TransactionBenchOp::ReadOneByPk => fixture.read_one_by_pk().await,
        TransactionBenchOp::ReadManyByPk => fixture.read_many_by_pk().await,
        TransactionBenchOp::UpdateAll => fixture.update_all().await,
        TransactionBenchOp::UpdateOneByPk => fixture.update_one_by_pk().await,
        TransactionBenchOp::DeleteAll => fixture.delete_all().await,
        TransactionBenchOp::DeleteOneByPk => fixture.delete_one_by_pk().await,
    }
}

fn print_profile_samples(
    layer: &str,
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    mut samples: Vec<Duration>,
) {
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    let p95_index = ((samples.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95 = samples[p95_index];
    let profile_detail = profile_read_many_detail(operation, read_many_pk_count);
    println!(
        "tracked_state_crud profile: {layer}/{}/{} samples{profile_detail}: median={median:?} p95={p95:?} min={:?} max={:?}",
        profile_operation_name(operation),
        samples.len(),
        samples[0],
        samples[samples.len() - 1],
    );
}

fn process_resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1024))
}

fn process_cpu_micros() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    let user = u64::try_from(usage.ru_utime.tv_sec)
        .unwrap_or_default()
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(usage.ru_utime.tv_usec).unwrap_or_default());
    let system = u64::try_from(usage.ru_stime.tv_sec)
        .unwrap_or_default()
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(usage.ru_stime.tv_usec).unwrap_or_default());
    user.saturating_add(system)
}

fn profile_read_many_detail(operation: TransactionBenchOp, read_many_pk_count: usize) -> String {
    matches!(operation, TransactionBenchOp::ReadManyByPk)
        .then(|| format!(" read_many_pk_count={read_many_pk_count}"))
        .unwrap_or_default()
}

const fn profile_operation_name(operation: TransactionBenchOp) -> &'static str {
    match operation {
        TransactionBenchOp::InsertAll => "insert_all",
        TransactionBenchOp::ReadAll => "read_all",
        TransactionBenchOp::ReadOneByPk => "read_one",
        TransactionBenchOp::ReadManyByPk => "read_many",
        TransactionBenchOp::UpdateAll => "update_all",
        TransactionBenchOp::UpdateOneByPk => "update_one",
        TransactionBenchOp::DeleteAll => "delete_all",
        TransactionBenchOp::DeleteOneByPk => "delete_one",
    }
}

fn bench_kv_layout(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    profile: StorageProfile,
    rows: &[WorkloadRow],
    label: &str,
) {
    let mut group = c.benchmark_group(format!(
        "tracked_state_crud/kv_layout/{}/{label}",
        profile.name()
    ));
    configure_group(&mut group, rows.len());
    bench_async_ops(&mut group, runtime, profile, rows, "kv_layout", KvOps);
    group.finish();
}

fn bench_transaction_api(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    profile: StorageProfile,
    rows: &[WorkloadRow],
    label: &str,
) {
    let mut group = c.benchmark_group(format!(
        "tracked_state_crud/transaction/{}/{label}",
        profile.name()
    ));
    configure_group(&mut group, rows.len());
    let rows = rows.to_vec();

    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("insert_all_rows/{}", row_label(rows.len())),
        TransactionBenchOp::InsertAll,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("read_all_rows/{}", row_label(rows.len())),
        TransactionBenchOp::ReadAll,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("read_one_by_pk/{}", row_label(rows.len())),
        TransactionBenchOp::ReadOneByPk,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("read_many_by_pk/{READ_MANY_PK_COUNT}"),
        TransactionBenchOp::ReadManyByPk,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("update_all_rows/{}", row_label(rows.len())),
        TransactionBenchOp::UpdateAll,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("update_one_by_pk/{}", row_label(rows.len())),
        TransactionBenchOp::UpdateOneByPk,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("delete_all_rows/{}", row_label(rows.len())),
        TransactionBenchOp::DeleteAll,
    );
    bench_transaction_op(
        &mut group,
        runtime,
        profile,
        &rows,
        format!("delete_one_by_pk/{}", row_label(rows.len())),
        TransactionBenchOp::DeleteOneByPk,
    );
    group.finish();
}

#[derive(Clone, Copy)]
enum TransactionBenchOp {
    InsertAll,
    ReadAll,
    ReadOneByPk,
    ReadManyByPk,
    UpdateAll,
    UpdateOneByPk,
    DeleteAll,
    DeleteOneByPk,
}

impl TransactionBenchOp {
    fn needs_seed(self) -> bool {
        !matches!(self, Self::InsertAll)
    }

    fn assert_supports_hot_repeats(self) {
        assert!(
            matches!(
                self,
                Self::ReadAll
                    | Self::ReadOneByPk
                    | Self::ReadManyByPk
                    | Self::UpdateAll
                    | Self::UpdateOneByPk
                    | Self::DeleteOneByPk
            ),
            "LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS only supports read_all, read_one, read_many, update_all, update_one, or delete_one"
        );
    }

    async fn run(
        self,
        fixture: &mut transaction_api::TransactionFixture,
        read_many_pk_count: usize,
    ) -> usize {
        match self {
            Self::InsertAll => fixture.insert_all().await,
            Self::ReadAll => fixture.read_all().await,
            Self::ReadOneByPk => fixture.read_one_by_pk().await,
            Self::ReadManyByPk => fixture.read_many_by_pk(read_many_pk_count).await,
            Self::UpdateAll => fixture.update_all().await,
            Self::UpdateOneByPk => fixture.update_one_by_pk().await,
            Self::DeleteAll => fixture.delete_all().await,
            Self::DeleteOneByPk => fixture.delete_one_by_pk().await,
        }
    }
}

fn bench_transaction_op(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &tokio::runtime::Runtime,
    profile: StorageProfile,
    rows: &[WorkloadRow],
    name: String,
    op: TransactionBenchOp,
) {
    let rows = rows.to_vec();
    group.bench_function(name, |b| {
        b.iter_batched_ref(
            || {
                if op.needs_seed() {
                    runtime.block_on(transaction_api::seeded_fixture(profile, &rows))
                } else {
                    runtime.block_on(transaction_api::empty_fixture(profile, &rows))
                }
            },
            |fixture| black_box(runtime.block_on(op.run(fixture, READ_MANY_PK_COUNT))),
            BatchSize::LargeInput,
        );
    });
}

fn bench_sql_session(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    profile: StorageProfile,
    rows: &[WorkloadRow],
    label: &str,
) {
    let mut group = c.benchmark_group(format!(
        "tracked_state_crud/sql_session/{}/{label}",
        profile.name()
    ));
    configure_group(&mut group, rows.len());
    let rows = rows.to_vec();

    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::empty_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.insert_all())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.read_all())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(
        format!("read_all_rows_consumed/{}", row_label(rows.len())),
        |b| {
            b.iter_batched_ref(
                || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
                |fixture| black_box(runtime.block_on(fixture.read_all_rows_consumed())),
                BatchSize::LargeInput,
            );
        },
    );
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.read_one_by_pk())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_many_by_pk/{READ_MANY_PK_COUNT}"), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.read_many_by_pk())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.update_all())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.update_one_by_pk())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.delete_all())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(sql_session::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(fixture.delete_one_by_pk())),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

trait AsyncOps {
    type Fixture;

    async fn empty_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> Self::Fixture;
    async fn seeded_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> Self::Fixture;
    async fn insert_all(fixture: &mut Self::Fixture) -> usize;
    async fn read_all(fixture: &mut Self::Fixture) -> usize;
    async fn read_one_by_pk(fixture: &mut Self::Fixture) -> usize;
    async fn read_many_by_pk(fixture: &mut Self::Fixture, count: usize) -> usize;
    async fn update_all(fixture: &mut Self::Fixture) -> usize;
    async fn update_one_by_pk(fixture: &mut Self::Fixture) -> usize;
    async fn delete_all(fixture: &mut Self::Fixture) -> usize;
    async fn delete_one_by_pk(fixture: &mut Self::Fixture) -> usize;
}

struct KvOps;

impl AsyncOps for KvOps {
    type Fixture = kv_layout::KvFixture;

    async fn empty_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> Self::Fixture {
        kv_layout::empty_fixture(profile, rows).await
    }

    async fn seeded_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> Self::Fixture {
        kv_layout::seeded_fixture(profile, rows).await
    }

    async fn insert_all(fixture: &mut Self::Fixture) -> usize {
        fixture.insert_all().await
    }

    async fn read_all(fixture: &mut Self::Fixture) -> usize {
        fixture.read_all().await
    }

    async fn read_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.read_one_by_pk().await
    }

    async fn read_many_by_pk(fixture: &mut Self::Fixture, count: usize) -> usize {
        fixture.read_many_by_pk(count).await
    }

    async fn update_all(fixture: &mut Self::Fixture) -> usize {
        fixture.update_all().await
    }

    async fn update_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.update_one_by_pk().await
    }

    async fn delete_all(fixture: &mut Self::Fixture) -> usize {
        fixture.delete_all().await
    }

    async fn delete_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.delete_one_by_pk().await
    }
}

fn bench_async_ops<O: AsyncOps>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &tokio::runtime::Runtime,
    profile: StorageProfile,
    rows: &[WorkloadRow],
    _layer: &str,
    _ops: O,
) {
    let rows = rows.to_vec();
    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::empty_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::insert_all(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::read_all(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::read_one_by_pk(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_many_by_pk/{READ_MANY_PK_COUNT}"), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::read_many_by_pk(fixture, READ_MANY_PK_COUNT))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::update_all(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::update_one_by_pk(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::delete_all(fixture))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || runtime.block_on(O::seeded_fixture(profile, &rows)),
            |fixture| black_box(runtime.block_on(O::delete_one_by_pk(fixture))),
            BatchSize::LargeInput,
        );
    });
}

fn configure_group(group: &mut BenchmarkGroup<'_, WallTime>, row_count: usize) {
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(if row_count >= REAL_WORKLOAD_ROWS {
        Duration::from_secs(2)
    } else {
        Duration::from_secs(1)
    });
}

criterion_group!(benches, tracked_state_crud_benches);
criterion_main!(benches);
