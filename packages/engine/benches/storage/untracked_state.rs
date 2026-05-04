use lix_engine::storage_bench::{
    self, StorageBenchConfig, StorageBenchKeyPattern, StorageBenchSelectivity,
    StorageBenchUpdateFraction,
};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("storage/untracked_state");
    group.bench_function("write_rows/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_untracked_state_write_rows(config(
                        &args,
                    )))
                    .expect("prepare untracked_state/write_rows");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_write_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/write_rows succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("read_point_hit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_read_point_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/read_point_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("read_point_miss/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_read_point_miss_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/read_point_miss succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_all/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_all_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_all succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_keys_only/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_keys_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_keys_only succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_headers_only/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_headers_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_headers_only succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_full_rows/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_full_rows succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for (label, bytes, rows, row_label) in
        [("1k", 1024, 10_000, "10k"), ("16k", 16 * 1024, 1_000, "1k")]
    {
        let config = config(&args)
            .with_state_payload_bytes(bytes)
            .with_rows(rows);
        let name = format!("scan_keys_only_payload_{label}/{row_label}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read_with(runtime, config),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_scan_keys_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/scan_keys_only payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        let name = format!("scan_headers_only_payload_{label}/{row_label}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read_with(runtime, config),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_scan_headers_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/scan_headers_only payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
        let name = format!("scan_full_rows_payload_{label}/{row_label}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read_with(runtime, config),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_scan_full_rows_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/scan_full_rows payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("scan_version/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_version_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_version succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_schema/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_scan_schema_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/scan_schema succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("overwrite_existing/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_untracked_state_overwrite(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare untracked_state/overwrite_existing");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_overwrite_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/overwrite_existing succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for rows in [1, 10, 100, 1_000] {
        let name = format!("write_rows/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_untracked_state_write_rows(
                            config(&args).with_rows(rows),
                        ))
                        .expect("prepare untracked_state/write_rows batch");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/write_rows batch succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, bytes, rows) in [
        ("small", 0, 10_000),
        ("1k", 1024, 10_000),
        ("16k", 16 * 1024, 1_000),
        ("128k", 128 * 1024, 100),
    ] {
        let name = format!("write_rows_payload_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_untracked_state_write_rows(
                            config(&args)
                                .with_state_payload_bytes(bytes)
                                .with_rows(rows),
                        ))
                        .expect("prepare untracked_state/write_rows payload");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/write_rows payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, key_pattern) in [
        ("sequential_keys", StorageBenchKeyPattern::Sequential),
        ("random_keys", StorageBenchKeyPattern::Random),
    ] {
        let name = format!("write_rows_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_untracked_state_write_rows(
                            config(&args).with_key_pattern(key_pattern),
                        ))
                        .expect("prepare untracked_state/write_rows key pattern");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/write_rows key pattern succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, selectivity) in [
        ("1pct", StorageBenchSelectivity::Percent1),
        ("10pct", StorageBenchSelectivity::Percent10),
        ("100pct", StorageBenchSelectivity::Percent100),
    ] {
        let name = format!("scan_schema_selectivity_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read_with(runtime, config(&args).with_selectivity(selectivity)),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::untracked_state_scan_schema_selective_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state/scan_schema selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for rows in [1_000, 10_000, 100_000] {
        let name = format!("read_point_hit_100_reads/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read_with(runtime, config(&args).with_rows(rows)),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::untracked_state_read_point_hit_constant_prepared(
                                    &backend, &fixture, 100,
                                ),
                            )
                            .expect("untracked_state/read_point_hit scaling succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, fraction) in [
        ("overwrite_10pct", StorageBenchUpdateFraction::Percent10),
        ("overwrite_all", StorageBenchUpdateFraction::Percent100),
    ] {
        let name = format!("{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_untracked_state_overwrite(
                            &backend,
                            config(&args).with_update_fraction(fraction),
                        ))
                        .expect("prepare untracked_state/overwrite shape");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_overwrite_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state/overwrite shape succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("insert_new_keys/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_untracked_state_insert_new_keys(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare untracked_state/insert_new_keys");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::untracked_state_write_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("untracked_state/insert_new_keys succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

fn prepare_read(
    runtime: &Runtime,
    args: Args,
) -> (
    std::sync::Arc<dyn lix_engine::Backend + Send + Sync>,
    lix_engine::storage_bench::UntrackedStateReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_untracked_state_read(
            &backend,
            config(&args),
        ))
        .expect("prepare untracked_state/read");
    (backend, fixture)
}

fn prepare_read_with(
    runtime: &Runtime,
    config: StorageBenchConfig,
) -> (
    std::sync::Arc<dyn lix_engine::Backend + Send + Sync>,
    lix_engine::storage_bench::UntrackedStateReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_untracked_state_read(
            &backend, config,
        ))
        .expect("prepare untracked_state/read variant");
    (backend, fixture)
}

fn config(args: &Args) -> StorageBenchConfig {
    args.config()
}
