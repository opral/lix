use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{self, StorageBenchConfig};
use lix_engine::Backend;
use tokio::runtime::Runtime;

use crate::{Args, RocksDbBenchBackend, SqliteBenchBackend};

type BackendFactory = fn() -> Arc<dyn Backend + Send + Sync>;

#[derive(Clone, Copy)]
struct BackendProfile {
    name: &'static str,
    create: BackendFactory,
}

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    for profile in physical_backends() {
        bench_smoke(c, runtime, args, profile);
        bench_fast(c, runtime, args, profile);
        bench_full(c, runtime, args, profile);
    }
}

fn bench_smoke(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let smoke = args.config().with_rows(1_000);
    let mut group = c.benchmark_group(format!("physical_layout/changelog/smoke/{}", profile.name));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("append_changes/1k", |b| {
        b.iter_batched(
            || prepare_append(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog smoke append_changes succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_change_set/1k", |b| {
        b.iter_batched(
            || prepare_read(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_scan_change_set_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog smoke scan_change_set succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("load_changes_hit/1k", |b| {
        b.iter_batched(
            || prepare_read(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_load_changes_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog smoke load_changes_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_fast(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/changelog/fast/{}", profile.name));

    group.bench_function("append_changes/10k", |b| {
        b.iter_batched(
            || prepare_append(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog append_changes succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_change_set/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_scan_change_set_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog scan_change_set succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("load_changes_hit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_load_changes_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/changelog load_changes_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_full(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/changelog/full/{}", profile.name));

    for rows in [1_000usize, 10_000, 50_000] {
        let config = args.config().with_rows(rows);
        group.bench_function(format!("append_changes/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_append(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/changelog full append succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("scan_change_set/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_scan_change_set_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/changelog full scan_change_set succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("scan_all/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_scan_all_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/changelog full scan_all succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("load_changes_hit/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_load_changes_hit_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/changelog full load_changes_hit succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("load_changes_miss/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_load_changes_miss_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/changelog full load_changes_miss succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

fn prepare_append(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::ChangelogAppendFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_changelog_append_changes(config))
        .expect("prepare physical_layout/changelog append");
    (backend, fixture)
}

fn prepare_read(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::ChangelogReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_changelog_read(&backend, config))
        .expect("prepare physical_layout/changelog read");
    (backend, fixture)
}

fn physical_backends() -> [BackendProfile; 2] {
    [
        BackendProfile {
            name: "sqlite_tempfile",
            create: sqlite_tempfile_backend,
        },
        BackendProfile {
            name: "rocksdb_tempdir",
            create: rocksdb_backend,
        },
    ]
}

fn sqlite_tempfile_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(SqliteBenchBackend::tempfile().expect("create sqlite tempfile bench backend"))
}

fn rocksdb_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(RocksDbBenchBackend::new().expect("create rocksdb bench backend"))
}

fn label(rows: usize) -> &'static str {
    match rows {
        1_000 => "1k",
        10_000 => "10k",
        50_000 => "50k",
        _ => "rows",
    }
}
