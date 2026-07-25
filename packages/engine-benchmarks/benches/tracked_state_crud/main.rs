use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main};

mod accounting;
mod io_stats;
mod kv_layout;
mod raw_sqlite;
mod sql_session;
mod storage;
mod transaction_api;
mod workload;

use storage::{STORAGE_PROFILES, StorageProfile};
use workload::{REAL_WORKLOAD_ROWS, SMOKE_ROWS, WorkloadRow, fixture_rows, row_label};

const READ_MANY_PK_COUNT: usize = 10;

fn tracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for tracked_state_crud benchmarks");
    let rows = fixture_rows();
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE").is_some() {
        profile_operation(&runtime, &rows[..REAL_WORKLOAD_ROWS]);
        return;
    }
    io_stats::maybe_print_io_report();
    accounting::maybe_print_accounting_report(&runtime, &rows[..SMOKE_ROWS]);

    for (label, row_count) in [("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)] {
        bench_raw_sqlite(c, &rows[..row_count], label);
        for profile in STORAGE_PROFILES {
            bench_kv_layout(c, &runtime, profile, &rows[..row_count], label);
            bench_transaction_api(c, &runtime, profile, &rows[..row_count], label);
            bench_sql_session(c, &runtime, profile, &rows[..row_count], label);
        }
    }
}

/// Reproducible, setup-excluded latency samples for one production transaction
/// operation. Criterion's CodSpeed-compatible harness intentionally delegates
/// timing to the runner, while this opt-in mode is useful for local profiling
/// and before/after investigation. Each sample starts from an independently
/// seeded fixture, matching the benchmark's measurement boundary.
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
    let sample_count = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or(15);
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
            profile_kv_layout_operation(runtime, rows, operation, sample_count);
        }
        Ok("raw_sqlite") => {
            let output = raw_sqlite_profile_output(operation);
            if let Some(repeats) = hot_repeats {
                profile_hot_raw_sqlite_operations(rows, operation, repeats, output);
            } else {
                profile_raw_sqlite_operation(rows, operation, sample_count, output);
            }
        }
        Ok("raw_sqlite_literal") => {
            assert!(
                matches!(operation, TransactionBenchOp::UpdateAll),
                "raw_sqlite_literal only supports update_all"
            );
            if let Some(repeats) = hot_repeats {
                profile_hot_raw_sqlite_literal_updates(rows, repeats);
            } else {
                profile_raw_sqlite_literal_updates(rows, sample_count);
            }
        }
        Ok("sql_session") => {
            let profile = profile_sql_session_storage();
            if let Some(repeats) = hot_repeats {
                profile_hot_sql_session_operations(runtime, rows, operation, repeats, profile);
            } else {
                profile_sql_session_operation(runtime, rows, operation, sample_count, profile);
            }
        }
        Ok("sql_session_bound") => {
            assert!(
                matches!(operation, TransactionBenchOp::UpdateAll),
                "sql_session_bound only supports update_all"
            );
            let profile = profile_sql_session_storage();
            if let Some(repeats) = hot_repeats {
                profile_hot_sql_session_bound_updates(runtime, rows, repeats, profile);
            } else {
                profile_sql_session_bound_updates(runtime, rows, sample_count, profile);
            }
        }
        Ok("transaction") | Err(_) => {
            if let Some(repeats) = hot_repeats {
                profile_hot_transaction_operations(runtime, rows, operation, repeats);
            } else {
                profile_transaction_operation(runtime, rows, operation, sample_count);
            }
        }
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_LAYER '{other}'; expected transaction, sql_session, sql_session_bound, kv_layout, raw_sqlite, or raw_sqlite_literal"
        ),
    }
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
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE '{other}'; expected rocksdb or sqlite"
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
    repeats: usize,
) {
    operation.assert_supports_hot_repeats();
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = runtime.block_on(transaction_api::seeded_fixture(
        StorageProfile::RocksDB,
        rows,
    ));
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(operation.run(&mut fixture));
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: transaction/lix_rocksdb/{}/{} repeats: total={elapsed:?} per_operation={:?}",
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
    repeats: usize,
    profile: StorageProfile,
) {
    operation.assert_supports_hot_repeats();
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let fixture = runtime.block_on(sql_session::seeded_fixture(profile, rows));
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(run_sql_session_operation(operation, &fixture));
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: sql_session/{}/{}/{} repeats: total={elapsed:?} per_operation={:?}",
        profile.name(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

fn profile_hot_sql_session_bound_updates(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    repeats: usize,
    profile: StorageProfile,
) {
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let fixture = runtime.block_on(sql_session::seeded_fixture(profile, rows));
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += runtime.block_on(fixture.update_all_bound());
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: sql_session_bound/{}/update_all/{repeats} repeats: total={elapsed:?} per_operation={:?}",
        profile.name(),
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

fn profile_hot_raw_sqlite_operations(
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    repeats: usize,
    output: RawSqliteProfileOutput,
) {
    operation.assert_supports_hot_repeats();
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = raw_sqlite::seeded_fixture(rows);
    let start = Instant::now();
    let mut row_count = 0;
    for _ in 0..repeats {
        row_count += run_raw_sqlite_operation(operation, &mut fixture, output);
    }
    let elapsed = start.elapsed();
    println!(
        "tracked_state_crud hot profile: {}/{}/{} repeats: total={elapsed:?} per_operation={:?}",
        output.layer(),
        profile_operation_name(operation),
        repeats,
        elapsed / repeats_u32,
    );
    black_box(row_count);
}

fn profile_hot_raw_sqlite_literal_updates(rows: &[WorkloadRow], repeats: usize) {
    let repeats_u32 =
        u32::try_from(repeats).expect("LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS must fit in u32");
    let mut fixture = raw_sqlite::seeded_fixture(rows);
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

fn profile_transaction_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    sample_count: usize,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = if operation.needs_seed() {
            runtime.block_on(transaction_api::seeded_fixture(
                StorageProfile::RocksDB,
                rows,
            ))
        } else {
            runtime.block_on(transaction_api::empty_fixture(
                StorageProfile::RocksDB,
                rows,
            ))
        };
        let start = Instant::now();
        let result = runtime.block_on(operation.run(&mut fixture));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples("transaction/lix_rocksdb", operation, samples);
}

fn profile_kv_layout_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
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
        let result = runtime.block_on(run_kv_layout_operation(operation, &mut fixture));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples("kv_layout/lix_rocksdb", operation, samples);
}

async fn run_kv_layout_operation(
    operation: TransactionBenchOp,
    fixture: &mut kv_layout::KvFixture,
) -> usize {
    match operation {
        TransactionBenchOp::InsertAll => fixture.insert_all().await,
        TransactionBenchOp::ReadAll => fixture.read_all().await,
        TransactionBenchOp::ReadOneByPk => fixture.read_one_by_pk().await,
        TransactionBenchOp::ReadManyByPk => fixture.read_many_by_pk(READ_MANY_PK_COUNT).await,
        TransactionBenchOp::UpdateAll => fixture.update_all().await,
        TransactionBenchOp::UpdateOneByPk => fixture.update_one_by_pk().await,
        TransactionBenchOp::DeleteAll => fixture.delete_all().await,
        TransactionBenchOp::DeleteOneByPk => fixture.delete_one_by_pk().await,
    }
}

fn profile_raw_sqlite_operation(
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    sample_count: usize,
    output: RawSqliteProfileOutput,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = if operation.needs_seed() {
            raw_sqlite::seeded_fixture(rows)
        } else {
            raw_sqlite::empty_fixture(rows)
        };
        let start = Instant::now();
        let result = run_raw_sqlite_operation(operation, &mut fixture, output);
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(output.layer(), operation, samples);
}

fn profile_raw_sqlite_literal_updates(rows: &[WorkloadRow], sample_count: usize) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let mut fixture = raw_sqlite::seeded_fixture(rows);
        let start = Instant::now();
        let result = fixture.update_all_literal();
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples("raw_sqlite/literal", TransactionBenchOp::UpdateAll, samples);
}

fn profile_sql_session_operation(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    operation: TransactionBenchOp,
    sample_count: usize,
    profile: StorageProfile,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let fixture = if operation.needs_seed() {
            runtime.block_on(sql_session::seeded_fixture(profile, rows))
        } else {
            runtime.block_on(sql_session::empty_fixture(profile, rows))
        };
        let start = Instant::now();
        let result = runtime.block_on(run_sql_session_operation(operation, &fixture));
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        &format!("sql_session/{}", profile.name()),
        operation,
        samples,
    );
}

fn profile_sql_session_bound_updates(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    sample_count: usize,
    profile: StorageProfile,
) {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let fixture = runtime.block_on(sql_session::seeded_fixture(profile, rows));
        let start = Instant::now();
        let result = runtime.block_on(fixture.update_all_bound());
        samples.push(start.elapsed());
        black_box(result);
    }
    print_profile_samples(
        &format!("sql_session_bound/{}", profile.name()),
        TransactionBenchOp::UpdateAll,
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
            black_box(fixture.read_many_by_pk_public_result(READ_MANY_PK_COUNT)).len()
        }
        (TransactionBenchOp::InsertAll, _) => fixture.insert_all(),
        (TransactionBenchOp::ReadAll, _) => fixture.read_all(),
        (TransactionBenchOp::ReadOneByPk, _) => fixture.read_one_by_pk(),
        (TransactionBenchOp::ReadManyByPk, _) => fixture.read_many_by_pk(READ_MANY_PK_COUNT),
        (TransactionBenchOp::UpdateAll, _) => fixture.update_all(),
        (TransactionBenchOp::UpdateOneByPk, _) => fixture.update_one_by_pk(),
        (TransactionBenchOp::DeleteAll, _) => fixture.delete_all(),
        (TransactionBenchOp::DeleteOneByPk, _) => fixture.delete_one_by_pk(),
    }
}

fn print_profile_samples(layer: &str, operation: TransactionBenchOp, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    println!(
        "tracked_state_crud profile: {layer}/{}/{} samples: median={median:?} min={:?} max={:?}",
        profile_operation_name(operation),
        samples.len(),
        samples[0],
        samples[samples.len() - 1],
    );
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
            ),
            "LIX_TRACKED_STATE_CRUD_PROFILE_HOT_REPEATS only supports read_all, read_one, read_many, update_all, or update_one"
        );
    }

    async fn run(self, fixture: &mut transaction_api::TransactionFixture) -> usize {
        match self {
            Self::InsertAll => fixture.insert_all().await,
            Self::ReadAll => fixture.read_all().await,
            Self::ReadOneByPk => fixture.read_one_by_pk().await,
            Self::ReadManyByPk => fixture.read_many_by_pk(READ_MANY_PK_COUNT).await,
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
            |fixture| black_box(runtime.block_on(op.run(fixture))),
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
