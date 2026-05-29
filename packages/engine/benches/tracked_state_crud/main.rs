use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main};

mod accounting;
mod backends;
mod io_stats;
mod kv_layout;
mod sql_session;
mod transaction_api;
mod workload;

use backends::{BACKEND_PROFILES, BackendProfile};
use workload::{REAL_WORKLOAD_ROWS, SMOKE_ROWS, WorkloadRow, fixture_rows, row_label};

const READ_MANY_PK_COUNT: usize = 10;

fn tracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for tracked_state_crud benchmarks");
    let rows = fixture_rows();
    io_stats::maybe_print_io_report();
    accounting::maybe_print_accounting_report(&runtime, &rows[..SMOKE_ROWS]);

    for (label, row_count) in [("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)] {
        for profile in BACKEND_PROFILES {
            bench_kv_layout(c, profile, &rows[..row_count], label);
            bench_transaction_api(c, &runtime, profile, &rows[..row_count], label);
            bench_sql_session(c, &runtime, profile, &rows[..row_count], label);
        }
    }
}

fn bench_kv_layout(c: &mut Criterion, profile: BackendProfile, rows: &[WorkloadRow], label: &str) {
    let mut group = c.benchmark_group(format!(
        "tracked_state_crud/kv_layout/{}/{label}",
        profile.name()
    ));
    configure_group(&mut group, rows.len());
    bench_sync_ops(&mut group, profile, rows, "kv_layout", KvOps);
    group.finish();
}

fn bench_transaction_api(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    profile: BackendProfile,
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

#[expect(clippy::cast_possible_truncation)]
fn bench_transaction_op(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &tokio::runtime::Runtime,
    profile: BackendProfile,
    rows: &[WorkloadRow],
    name: String,
    op: TransactionBenchOp,
) {
    let rows = rows.to_vec();
    group.bench_function(name, |b| {
        b.iter_custom(|iterations| {
            let mut fixtures = Vec::with_capacity(iterations as usize);
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let mut fixture = if op.needs_seed() {
                    runtime.block_on(transaction_api::seeded_fixture(profile, &rows))
                } else {
                    runtime.block_on(transaction_api::empty_fixture(profile, &rows))
                };
                let start = Instant::now();
                let rows = runtime.block_on(op.run(&mut fixture));
                elapsed += start.elapsed();
                black_box(rows);
                fixtures.push(fixture);
            }
            drop(fixtures);
            elapsed
        });
    });
}

fn bench_sql_session(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    profile: BackendProfile,
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
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_SQL_UPDATE").is_some() {
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
    }
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

trait SyncOps {
    type Fixture;

    fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> Self::Fixture;
    fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> Self::Fixture;
    fn insert_all(fixture: &mut Self::Fixture) -> usize;
    fn read_all(fixture: &mut Self::Fixture) -> usize;
    fn read_one_by_pk(fixture: &mut Self::Fixture) -> usize;
    fn read_many_by_pk(fixture: &mut Self::Fixture, count: usize) -> usize;
    fn update_all(fixture: &mut Self::Fixture) -> usize;
    fn update_one_by_pk(fixture: &mut Self::Fixture) -> usize;
    fn delete_all(fixture: &mut Self::Fixture) -> usize;
    fn delete_one_by_pk(fixture: &mut Self::Fixture) -> usize;
}

struct KvOps;

impl SyncOps for KvOps {
    type Fixture = kv_layout::KvFixture;

    fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> Self::Fixture {
        kv_layout::empty_fixture(profile, rows)
    }

    fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> Self::Fixture {
        kv_layout::seeded_fixture(profile, rows)
    }

    fn insert_all(fixture: &mut Self::Fixture) -> usize {
        fixture.insert_all()
    }

    fn read_all(fixture: &mut Self::Fixture) -> usize {
        fixture.read_all()
    }

    fn read_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.read_one_by_pk()
    }

    fn read_many_by_pk(fixture: &mut Self::Fixture, count: usize) -> usize {
        fixture.read_many_by_pk(count)
    }

    fn update_all(fixture: &mut Self::Fixture) -> usize {
        fixture.update_all()
    }

    fn update_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.update_one_by_pk()
    }

    fn delete_all(fixture: &mut Self::Fixture) -> usize {
        fixture.delete_all()
    }

    fn delete_one_by_pk(fixture: &mut Self::Fixture) -> usize {
        fixture.delete_one_by_pk()
    }
}

fn bench_sync_ops<O: SyncOps>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    profile: BackendProfile,
    rows: &[WorkloadRow],
    _layer: &str,
    _ops: O,
) {
    let rows = rows.to_vec();
    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::empty_fixture(profile, &rows),
            |fixture| black_box(O::insert_all(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::read_all(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::read_one_by_pk(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("read_many_by_pk/{READ_MANY_PK_COUNT}"), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::read_many_by_pk(fixture, READ_MANY_PK_COUNT)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::update_all(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::update_one_by_pk(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::delete_all(fixture)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched_ref(
            || O::seeded_fixture(profile, &rows),
            |fixture| black_box(O::delete_one_by_pk(fixture)),
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
