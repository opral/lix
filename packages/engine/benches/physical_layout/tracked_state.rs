use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{self, StorageBenchConfig, StorageBenchSelectivity};
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
    let smoke = args
        .config()
        .with_rows(1_000)
        .with_state_payload_bytes(1024);
    let mut group = c.benchmark_group(format!(
        "physical_layout/tracked_state/smoke/{}",
        profile.name
    ));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("write_root_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_write_root(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_write_root_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state smoke write_root succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_headers_only_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_read(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_headers_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state smoke headers succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_full_rows_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_read(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state smoke full rows succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_file_header_selective_10pct_payload_1k/1k", |b| {
        b.iter_batched(
            || {
                prepare_read_file_selective(
                    runtime,
                    smoke.with_selectivity(StorageBenchSelectivity::Percent10),
                    profile,
                )
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(
                            storage_bench::tracked_state_scan_file_header_selective_prepared(
                                &backend, &fixture,
                            ),
                        )
                        .expect("physical_layout/tracked_state smoke file headers succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_update_1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_diff_update_rows(runtime, smoke, profile, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state smoke diff succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_fast(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!(
        "physical_layout/tracked_state/fast/{}",
        profile.name
    ));

    group.bench_function("write_root/10k", |b| {
        b.iter_batched(
            || prepare_write_root(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_write_root_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state write_root succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write_root_payload_1k/10k", |b| {
        b.iter_batched(
            || {
                prepare_write_root(
                    runtime,
                    args.config().with_state_payload_bytes(1024),
                    profile,
                )
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_write_root_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state write_root_payload_1k succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_existing_1pct/10k", |b| {
        b.iter_batched(
            || prepare_update_rows(runtime, args.config(), profile, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_update_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state update_existing_1pct succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_existing_10pct/10k", |b| {
        b.iter_batched(
            || prepare_update_rows(runtime, args.config(), profile, args.rows / 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_update_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state update_existing_10pct succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("tombstone_10pct/10k", |b| {
        b.iter_batched(
            || prepare_tombstone_rows(runtime, args.config(), profile, args.rows / 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_update_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state tombstone_10pct succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("read_point_hit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_read_point_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state read_point_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_headers_only/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_headers_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state scan_headers_only succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_full_rows/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state scan_full_rows succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_file_header_selective_10pct_payload_1k/10k", |b| {
        b.iter_batched(
            || {
                prepare_read_file_selective(
                    runtime,
                    args.config()
                        .with_state_payload_bytes(1024)
                        .with_selectivity(StorageBenchSelectivity::Percent10),
                    profile,
                )
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(
                            storage_bench::tracked_state_scan_file_header_selective_prepared(
                                &backend, &fixture,
                            ),
                        )
                        .expect("physical_layout/tracked_state file header scan succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_update_1pct/10k", |b| {
        b.iter_batched(
            || prepare_diff_update_rows(runtime, args.config(), profile, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/tracked_state diff_update_1pct succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_full(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!(
        "physical_layout/tracked_state/full/{}",
        profile.name
    ));

    for rows in [1_000usize, 10_000, 50_000] {
        let config = args.config().with_rows(rows);
        group.bench_function(format!("write_root/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_write_root(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full write_root succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("read_point_hit/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_read_point_hit_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full point_hit succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("scan_headers_only/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_headers_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full headers succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        group.bench_function(format!("scan_full_rows/{}", label(rows)), |b| {
            b.iter_batched(
                || prepare_read(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full full_rows succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, config) in [
        (
            "write_root_payload_1k/10k",
            args.config().with_state_payload_bytes(1024),
        ),
        (
            "write_root_payload_16k/1k",
            args.config()
                .with_rows(1_000)
                .with_state_payload_bytes(16 * 1024),
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_write_root(runtime, config, profile),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full payload write succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, changed_rows, tombstone) in [
        ("diff_equal/10k", 0usize, false),
        ("diff_update_1pct/10k", args.rows / 100, false),
        ("diff_update_10pct/10k", args.rows / 10, false),
        ("diff_tombstone_10pct/10k", args.rows / 10, true),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    if changed_rows == 0 {
                        prepare_diff_equal(runtime, args.config(), profile)
                    } else if tombstone {
                        prepare_diff_tombstone_rows(runtime, args.config(), profile, changed_rows)
                    } else {
                        prepare_diff_update_rows(runtime, args.config(), profile, changed_rows)
                    }
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_diff_commits_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/tracked_state full diff succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

fn prepare_write_root(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateWriteRootFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_write_root(config))
        .expect("prepare physical_layout/tracked_state write root");
    (backend, fixture)
}

fn prepare_read(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read(&backend, config))
        .expect("prepare physical_layout/tracked_state read");
    (backend, fixture)
}

fn prepare_read_file_selective(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read_file_selective(
            &backend, config,
        ))
        .expect("prepare physical_layout/tracked_state file-selective read");
    (backend, fixture)
}

fn prepare_update_rows(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateUpdateFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_update_rows(
            &backend, config, rows,
        ))
        .expect("prepare physical_layout/tracked_state update rows");
    (backend, fixture)
}

fn prepare_tombstone_rows(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateUpdateFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_tombstone_rows(
            &backend, config, rows,
        ))
        .expect("prepare physical_layout/tracked_state tombstone rows");
    (backend, fixture)
}

fn prepare_diff_equal(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateDiffFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_diff_equal(
            &backend, config,
        ))
        .expect("prepare physical_layout/tracked_state diff equal");
    (backend, fixture)
}

fn prepare_diff_update_rows(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateDiffFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_diff_update_rows(
            &backend, config, rows,
        ))
        .expect("prepare physical_layout/tracked_state diff update");
    (backend, fixture)
}

fn prepare_diff_tombstone_rows(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateDiffFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_diff_tombstone_rows(
            &backend, config, rows,
        ))
        .expect("prepare physical_layout/tracked_state diff tombstone");
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
