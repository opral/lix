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
    }
}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn reset_allocation_accounting() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
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
            "tracked_state_crud allocation phase: {phase} allocated_bytes={}",
            ALLOCATED_BYTES.load(Ordering::Relaxed)
        );
    }
}

#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn reset_allocation_accounting() {}
#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn print_allocation_accounting(_phase: &str) {}

mod accounting;
mod io_stats;
mod kv_layout;
mod raw_sqlite;
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
    let rows = fixture_rows(REAL_WORKLOAD_ROWS);
    io_stats::maybe_print_io_report();
    accounting::maybe_print_accounting_report(&runtime, &rows[..SMOKE_ROWS]);

    for (label, row_count) in [("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)] {
        bench_raw_sqlite(c, &rows[..row_count], label);
        for &profile in KV_STORAGE_PROFILES {
            bench_kv_layout(c, &runtime, profile, &rows[..row_count], label);
        }
        for &profile in STORAGE_PROFILES {
            bench_transaction_api(c, &runtime, profile, &rows[..row_count], label);
            bench_sql_session(c, &runtime, profile, &rows[..row_count], label);
        }
    }
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
        Ok("raw_sqlite") => {
            let output = raw_sqlite_profile_output(operation);
            if let Some(repeats) = hot_repeats {
                profile_hot_raw_sqlite_operations(
                    rows,
                    operation,
                    read_many_pk_count,
                    repeats,
                    output,
                );
            } else {
                profile_raw_sqlite_operation(
                    rows,
                    operation,
                    read_many_pk_count,
                    sample_count,
                    output,
                );
            }
        }
        Ok("raw_sqlite_literal") => {
            assert!(
                matches!(
                    operation,
                    TransactionBenchOp::UpdateAll | TransactionBenchOp::ReadManyByPk
                ),
                "raw_sqlite_literal only supports update_all or read_many"
            );
            match operation {
                TransactionBenchOp::UpdateAll => {
                    if let Some(repeats) = hot_repeats {
                        profile_hot_raw_sqlite_literal_updates(rows, read_many_pk_count, repeats);
                    } else {
                        profile_raw_sqlite_literal_updates(rows, read_many_pk_count, sample_count);
                    }
                }
                TransactionBenchOp::ReadManyByPk => {
                    if let Some(repeats) = hot_repeats {
                        profile_hot_raw_sqlite_literal_read_many(rows, read_many_pk_count, repeats);
                    } else {
                        profile_raw_sqlite_literal_read_many(
                            rows,
                            read_many_pk_count,
                            sample_count,
                        );
                    }
                }
                _ => unreachable!("raw_sqlite_literal operation was validated above"),
            }
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
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_LAYER '{other}'; expected transaction, sql_session, sql_session_bound, kv_layout, raw_sqlite, or raw_sqlite_literal"
        ),
    }
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

#[derive(Clone, Copy)]
enum RawSqliteProfileOutput {
    Borrowed,
    PublicResult,
}

impl RawSqliteProfileOutput {
    const fn layer(self) -> &'static str {
        match self {
            Self::Borrowed => "raw_sqlite",
            Self::PublicResult => "raw_sqlite/public_result",
        }
    }
}

fn raw_sqlite_profile_output(operation: TransactionBenchOp) -> RawSqliteProfileOutput {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_OUTPUT").as_deref() {
        Ok("public_result") => {
            assert!(
                matches!(
                    operation,
                    TransactionBenchOp::ReadAll
                        | TransactionBenchOp::ReadOneByPk
                        | TransactionBenchOp::ReadManyByPk
                ),
                "LIX_TRACKED_STATE_CRUD_PROFILE_OUTPUT=public_result only supports read_all, read_one, or read_many"
            );
            RawSqliteProfileOutput::PublicResult
        }
        Ok("borrowed") | Err(_) => RawSqliteProfileOutput::Borrowed,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_OUTPUT '{other}'; expected borrowed or public_result"
        ),
    }
}

fn profile_sql_session_storage() -> StorageProfile {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE").as_deref() {
        Ok("sqlite") => StorageProfile::SQLite,
        Ok("rocksdb") | Err(_) => StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb") => StorageProfile::SlateDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb_remote") => StorageProfile::SlateDBRemoteObjectStore,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE '{other}'; expected rocksdb, sqlite, slatedb, or slatedb_remote"
        ),
    }
}

fn profile_transaction_storage() -> StorageProfile {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE").as_deref() {
        Ok("sqlite") => StorageProfile::SQLite,
        Ok("rocksdb") | Err(_) => StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb") => StorageProfile::SlateDB,
        #[cfg(feature = "slatedb")]
        Ok("slatedb_remote") => StorageProfile::SlateDBRemoteObjectStore,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE '{other}'; expected rocksdb, sqlite, slatedb, or slatedb_remote"
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
    let _ = lix_engine::storage_bench::take_entity_point_snapshot_cache_accounting();
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(run_sql_session_operation(operation, &fixture));
    }
    let elapsed = start.elapsed();
    let profile_detail = profile_read_many_detail(operation, read_many_pk_count);
    println!(
        "tracked_state_crud hot profile: sql_session/{}/{}/{} repeats{profile_detail}: total={elapsed:?} per_operation={:?}",
        profile.name(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    if matches!(operation, TransactionBenchOp::ReadOneByPk) {
        let cache = lix_engine::storage_bench::take_entity_point_snapshot_cache_accounting();
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

fn profile_hot_raw_sqlite_operations(
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    repeats: usize,
    output: RawSqliteProfileOutput,
) {
    operation.assert_supports_hot_repeats();
    assert!(
        !matches!(operation, TransactionBenchOp::DeleteOneByPk),
        "delete_one hot repeats are only available for the transaction layer"
    );
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count);
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += run_raw_sqlite_operation(operation, &mut fixture, read_many_pk_count, output);
    }
    let elapsed = start.elapsed();
    let profile_detail = profile_read_many_detail(operation, read_many_pk_count);
    println!(
        "tracked_state_crud hot profile: {}/{}/{} repeats{profile_detail}: total={elapsed:?} per_operation={:?}",
        output.layer(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

fn profile_hot_raw_sqlite_literal_updates(
    rows: &[WorkloadRow],
    read_many_pk_count: usize,
    repeats: usize,
) {
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count);
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += fixture.update_all_literal();
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: raw_sqlite/literal/update_all/{repeats} repeats: total={elapsed:?} per_operation={:?}",
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

/// Literal-SQL SQLite control for Lix's public `read_many` surface. It does
/// not reuse SQLite's prepared statement cache and builds an owned result, so
/// it isolates the remaining Lix/RocksDB cost without granting SQLite a
/// parser or result-ownership advantage.
fn profile_hot_raw_sqlite_literal_read_many(
    rows: &[WorkloadRow],
    read_many_pk_count: usize,
    repeats: usize,
) {
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let fixture = raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count);
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count +=
            black_box(fixture.read_many_by_pk_literal_public_result(read_many_pk_count)).len();
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: raw_sqlite/literal/public_result/read_many/{repeats} repeats{}: total={elapsed:?} per_operation={:?}",
        profile_read_many_detail(TransactionBenchOp::ReadManyByPk, read_many_pk_count),
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

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

fn profile_raw_sqlite_operation(
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    read_many_pk_count: usize,
    sample_count: usize,
    output: RawSqliteProfileOutput,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = if operation.needs_seed() {
            raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count)
        } else {
            raw_sqlite::empty_fixture_with_read_many_pk_count(rows, read_many_pk_count)
        };
        let start = Instant::now();
        let result = run_raw_sqlite_operation(operation, &mut fixture, read_many_pk_count, output);
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(output.layer(), operation, read_many_pk_count, samples);
}

fn profile_raw_sqlite_literal_updates(
    rows: &[WorkloadRow],
    read_many_pk_count: usize,
    sample_count: usize,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture =
            raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count);
        let start = Instant::now();
        let result = fixture.update_all_literal();
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        "raw_sqlite/literal",
        TransactionBenchOp::UpdateAll,
        read_many_pk_count,
        samples,
    );
}

fn profile_raw_sqlite_literal_read_many(
    rows: &[WorkloadRow],
    read_many_pk_count: usize,
    sample_count: usize,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let fixture = raw_sqlite::seeded_fixture_with_read_many_pk_count(rows, read_many_pk_count);
        let start = Instant::now();
        let result = black_box(fixture.read_many_by_pk_literal_public_result(read_many_pk_count));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        "raw_sqlite/literal/public_result",
        TransactionBenchOp::ReadManyByPk,
        read_many_pk_count,
        samples,
    );
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
        let start = Instant::now();
        let result = runtime.block_on(run_sql_session_operation(operation, &fixture));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        &format!("sql_session/{}", profile.name()),
        operation,
        read_many_pk_count,
        samples,
    );
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
        let _ = lix_engine::storage_bench::take_crud_physical_write_accounting();
        let _ = lix_engine::storage_bench::take_certified_entity_update_value_batch_accounting();
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
        let certificate =
            lix_engine::storage_bench::take_certified_entity_update_value_batch_accounting();
        let physical = lix_engine::storage_bench::take_crud_physical_write_accounting();
        println!(
            "tracked_state_crud generated update accounting: logical_rows={bound_update_row_count} certificate_attempts={} certificate_hits={} certificate_misses={} certified_rows={} physical_puts={} physical_deletes={} physical_written_bytes={}",
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

fn run_raw_sqlite_operation(
    operation: TransactionBenchOp,
    fixture: &mut raw_sqlite::RawSqliteFixture,
    read_many_pk_count: usize,
    output: RawSqliteProfileOutput,
) -> usize {
    match (operation, output) {
        (TransactionBenchOp::ReadAll, RawSqliteProfileOutput::PublicResult) => {
            black_box(fixture.read_all_public_result()).len()
        }
        (TransactionBenchOp::ReadOneByPk, RawSqliteProfileOutput::PublicResult) => {
            black_box(fixture.read_one_by_pk_public_result()).len()
        }
        (TransactionBenchOp::ReadManyByPk, RawSqliteProfileOutput::PublicResult) => {
            black_box(fixture.read_many_by_pk_public_result(read_many_pk_count)).len()
        }
        (TransactionBenchOp::InsertAll, _) => fixture.insert_all(),
        (TransactionBenchOp::ReadAll, _) => fixture.read_all(),
        (TransactionBenchOp::ReadOneByPk, _) => fixture.read_one_by_pk(),
        (TransactionBenchOp::ReadManyByPk, _) => fixture.read_many_by_pk(read_many_pk_count),
        (TransactionBenchOp::UpdateAll, _) => fixture.update_all(),
        (TransactionBenchOp::UpdateOneByPk, _) => fixture.update_one_by_pk(),
        (TransactionBenchOp::DeleteAll, _) => fixture.delete_all(),
        (TransactionBenchOp::DeleteOneByPk, _) => fixture.delete_one_by_pk(),
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
    let profile_detail = profile_read_many_detail(operation, read_many_pk_count);
    println!(
        "tracked_state_crud profile: {layer}/{}/{} samples{profile_detail}: median={median:?} min={:?} max={:?}",
        profile_operation_name(operation),
        samples.len(),
        samples[0],
        samples[samples.len() - 1],
    );
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

fn bench_raw_sqlite(c: &mut Criterion, rows: &[WorkloadRow], label: &str) {
    let mut group = c.benchmark_group(format!("tracked_state_crud/raw_sqlite/{label}"));
    configure_group(&mut group, rows.len());
    let rows = rows.to_vec();

    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::empty_fixture(&rows),
            |fixture| black_box(fixture.insert_all()),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.read_all()),
            BatchSize::LargeInput,
        );
    });
    // Lix SQL-session `read_all_rows` already returns an owned public
    // ExecuteResult. This companion control separates that common result
    // materialization from the deliberately borrowed raw SQLite lower bound.
    group.bench_function(
        format!("read_all_public_result_rows/{}", row_label(rows.len())),
        |b| {
            b.iter_batched_ref(
                || raw_sqlite::seeded_fixture(&rows),
                |fixture| black_box(fixture.read_all_public_result()),
                BatchSize::LargeInput,
            );
        },
    );
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.read_one_by_pk()),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(
        format!("read_one_by_pk_public_result/{}", row_label(rows.len())),
        |b| {
            b.iter_batched_ref(
                || raw_sqlite::seeded_fixture(&rows),
                |fixture| black_box(fixture.read_one_by_pk_public_result()),
                BatchSize::LargeInput,
            );
        },
    );
    group.bench_function(format!("read_many_by_pk/{READ_MANY_PK_COUNT}"), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.read_many_by_pk(READ_MANY_PK_COUNT)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(
        format!("read_many_by_pk_public_result/{READ_MANY_PK_COUNT}"),
        |b| {
            b.iter_batched_ref(
                || raw_sqlite::seeded_fixture(&rows),
                |fixture| black_box(fixture.read_many_by_pk_public_result(READ_MANY_PK_COUNT)),
                BatchSize::LargeInput,
            );
        },
    );
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.update_all()),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.update_one_by_pk()),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.delete_all()),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || raw_sqlite::seeded_fixture(&rows),
            |fixture| black_box(fixture.delete_one_by_pk()),
            BatchSize::LargeInput,
        );
    });
    group.finish();
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
