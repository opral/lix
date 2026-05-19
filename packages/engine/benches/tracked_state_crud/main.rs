use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

mod backends;
mod io_stats;
mod kv_layout;
mod physical_api;
mod sql_session;
mod workload;

use backends::{BackendProfile, BACKEND_PROFILES};
use workload::{fixture_rows, row_label, WorkloadRow, REAL_WORKLOAD_ROWS, SMOKE_ROWS};

fn tracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for tracked_state_crud benchmarks");
    let rows = fixture_rows();
    io_stats::maybe_print_io_report();

    for (label, row_count) in [("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)] {
        for profile in BACKEND_PROFILES {
            bench_kv_layout(c, profile, &rows[..row_count], label);
            bench_physical_api(c, profile, &rows[..row_count], label);
        }
        bench_sql_session(c, &runtime, &rows[..row_count], label);
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

fn bench_physical_api(
    c: &mut Criterion,
    profile: BackendProfile,
    rows: &[WorkloadRow],
    label: &str,
) {
    let mut group = c.benchmark_group(format!(
        "tracked_state_crud/physical_api/{}/{label}",
        profile.name()
    ));
    configure_group(&mut group, rows.len());
    bench_sync_ops(&mut group, profile, rows, "physical_api", PhysicalOps);
    group.finish();
}

fn bench_sql_session(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
    label: &str,
) {
    let mut group = c.benchmark_group(format!("tracked_state_crud/sql_session/in_memory/{label}"));
    configure_group(&mut group, rows.len());
    let rows = rows.to_vec();

    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::insert_all(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::read_all(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::read_one_by_pk(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_all_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::read_all_by_pk(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_SQL_UPDATE").is_some() {
        group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
            b.iter_batched(
                || rows.clone(),
                |rows| black_box(sql_session::update_all(runtime, &rows)),
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
            b.iter_batched(
                || rows.clone(),
                |rows| black_box(sql_session::update_one_by_pk(runtime, &rows)),
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::delete_all(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(sql_session::delete_one_by_pk(runtime, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

trait SyncOps {
    fn insert_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn read_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn read_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn read_all_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn update_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn update_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn delete_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
    fn delete_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize;
}

struct KvOps;
struct PhysicalOps;

impl SyncOps for KvOps {
    fn insert_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::insert_all(profile, rows)
    }
    fn read_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::read_all(profile, rows)
    }
    fn read_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::read_one_by_pk(profile, rows)
    }
    fn read_all_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::read_all_by_pk(profile, rows)
    }
    fn update_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::update_all(profile, rows)
    }
    fn update_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::update_one_by_pk(profile, rows)
    }
    fn delete_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::delete_all(profile, rows)
    }
    fn delete_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        kv_layout::delete_one_by_pk(profile, rows)
    }
}

impl SyncOps for PhysicalOps {
    fn insert_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::insert_all(profile, rows)
    }
    fn read_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::read_all(profile, rows)
    }
    fn read_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::read_one_by_pk(profile, rows)
    }
    fn read_all_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::read_all_by_pk(profile, rows)
    }
    fn update_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::update_all(profile, rows)
    }
    fn update_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::update_one_by_pk(profile, rows)
    }
    fn delete_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::delete_all(profile, rows)
    }
    fn delete_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
        physical_api::delete_one_by_pk(profile, rows)
    }
}

fn bench_sync_ops<O: SyncOps>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    profile: BackendProfile,
    rows: &[WorkloadRow],
    _layer: &str,
    _ops: O,
) {
    let rows = rows.to_vec();
    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::insert_all(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::read_all(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::read_one_by_pk(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("read_all_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::read_all_by_pk(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::update_all(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::update_one_by_pk(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::delete_all(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || rows.clone(),
            |rows| black_box(O::delete_one_by_pk(profile, &rows)),
            BatchSize::LargeInput,
        )
    });
}

fn configure_group(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    row_count: usize,
) {
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
