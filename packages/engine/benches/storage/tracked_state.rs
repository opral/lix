use lix_engine::storage_bench::{
    self, StorageBenchConfig, StorageBenchKeyPattern, StorageBenchSelectivity,
    StorageBenchUpdateFraction,
};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("storage/tracked_state");
    group.bench_function("write_root/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_tracked_state_write_root(config(
                        &args,
                    )))
                    .expect("prepare tracked_state/write_root");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_write_root_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/write_root succeeds"),
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
                        .block_on(storage_bench::tracked_state_read_point_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/read_point_hit succeeds"),
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
                        .block_on(storage_bench::tracked_state_read_point_miss_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/read_point_miss succeeds"),
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
                        .block_on(storage_bench::tracked_state_scan_all_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/scan_all succeeds"),
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
                        .block_on(storage_bench::tracked_state_scan_schema_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/scan_schema succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_file/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_file_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/scan_file succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("update_existing/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_tracked_state_update(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare tracked_state/update_existing");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_update_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/update_existing succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for rows in [1, 10, 100, 1_000] {
        let name = format!("write_root/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_write_root(
                            config(&args).with_rows(rows),
                        ))
                        .expect("prepare tracked_state/write_root batch");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/write_root batch succeeds"),
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
        let name = format!("write_root_payload_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_write_root(
                            config(&args)
                                .with_state_payload_bytes(bytes)
                                .with_rows(rows),
                        ))
                        .expect("prepare tracked_state/write_root payload");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/write_root payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for threshold in [512, 1024, 2048, 4096, 8192] {
        let name = format!("write_root_payload_1k_max_inline_encoded_value_{threshold}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_tracked_state_write_root_with_max_inline_encoded_value_bytes(
                                config(&args).with_state_payload_bytes(1024),
                                threshold,
                            ),
                        )
                        .expect("prepare tracked_state/write_root max inline encoded value");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/write_root max inline encoded value succeeds"),
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
        let name = format!("write_root_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_write_root(
                            config(&args).with_key_pattern(key_pattern),
                        ))
                        .expect("prepare tracked_state/write_root key pattern");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_write_root_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/write_root key pattern succeeds"),
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
                || prepare_read_with(runtime, args, config(&args).with_selectivity(selectivity)),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_schema_selective_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/scan_schema selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, selectivity) in [
        ("1pct", StorageBenchSelectivity::Percent1),
        ("10pct", StorageBenchSelectivity::Percent10),
    ] {
        let name = format!("scan_file_selectivity_payload_1k_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read_file_selective_with(
                        runtime,
                        args,
                        config(&args)
                            .with_state_payload_bytes(1024)
                            .with_selectivity(selectivity),
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_file_selective_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/scan_file payload selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for threshold in [512, 1024, 2048, 4096, 8192] {
        let name = format!(
            "scan_file_selectivity_payload_1k_max_inline_encoded_value_{threshold}_10pct/10k"
        );
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read_file_selective_with_max_inline_encoded_value(
                        runtime,
                        args,
                        config(&args)
                            .with_state_payload_bytes(1024)
                            .with_selectivity(StorageBenchSelectivity::Percent10),
                        threshold,
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_file_selective_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/scan_file inline threshold succeeds"),
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
        let name = format!("scan_file_selectivity_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read_file_selective_with(
                        runtime,
                        args,
                        config(&args).with_selectivity(selectivity),
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_scan_file_selective_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/scan_file selectivity succeeds"),
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
        let name = format!("scan_file_header_selectivity_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read_file_selective_with(
                        runtime,
                        args,
                        config(&args).with_selectivity(selectivity),
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
                            .expect("tracked_state/scan_file header selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, selectivity) in [
        ("1pct", StorageBenchSelectivity::Percent1),
        ("10pct", StorageBenchSelectivity::Percent10),
    ] {
        let name = format!("scan_file_header_selectivity_payload_1k_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read_file_selective_with(
                        runtime,
                        args,
                        config(&args)
                            .with_state_payload_bytes(1024)
                            .with_selectivity(selectivity),
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
                            .expect("tracked_state/scan_file header payload selectivity succeeds"),
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
                || prepare_read_with(runtime, args, config(&args).with_rows(rows)),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::tracked_state_read_point_hit_constant_prepared(
                                    &backend, &fixture, 100,
                                ),
                            )
                            .expect("tracked_state/read_point_hit scaling succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, fraction) in [
        (
            "update_10pct_existing",
            StorageBenchUpdateFraction::Percent10,
        ),
        (
            "update_all_existing",
            StorageBenchUpdateFraction::Percent100,
        ),
    ] {
        let name = format!("{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_update(
                            &backend,
                            config(&args).with_update_fraction(fraction),
                        ))
                        .expect("prepare tracked_state/update shape");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_update_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/update shape succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for rows in [10_000, 100_000] {
        let name = format!("update_1_existing/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_update_rows(
                            &backend,
                            config(&args).with_rows(rows),
                            1,
                        ))
                        .expect("prepare tracked_state/update_1_existing");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_update_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/update_1_existing succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, rows, payload_bytes) in [
        ("partial_snapshot_update_1_payload_1k", 100_000, 1024),
        ("partial_snapshot_update_1_payload_16k", 10_000, 16 * 1024),
    ] {
        let name = format!("{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_tracked_state_partial_snapshot_update_rows(
                                &backend,
                                config(&args)
                                    .with_rows(rows)
                                    .with_state_payload_bytes(payload_bytes),
                                1,
                            ),
                        )
                        .expect("prepare tracked_state/partial_snapshot_update");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_update_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/partial_snapshot_update succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("append_new_child_commit/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_tracked_state_append_child(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare tracked_state/append_new_child_commit");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_update_existing_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/append_new_child_commit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for rows in [10_000, 100_000] {
        let name = format!("append_1_new_child_commit/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_append_child_rows(
                            &backend,
                            config(&args).with_rows(rows),
                            1,
                        ))
                        .expect("prepare tracked_state/append_1_new_child_commit");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_update_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/append_1_new_child_commit succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, rows) in [("delete_1", 1), ("delete_10pct", args.rows / 10)] {
        let name = format!("{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_tracked_state_tombstone_rows(
                            &backend,
                            config(&args),
                            rows,
                        ))
                        .expect("prepare tracked_state/delete tombstones");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_update_existing_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/delete tombstones succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("diff_equal/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_tracked_state_diff_equal(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare tracked_state/diff_equal");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("tracked_state/diff_equal succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for (label, changed_rows) in [
        ("diff_update_1", 1),
        ("diff_update_10pct", args.rows / 10),
        ("diff_delete_1", 1),
        ("diff_delete_10pct", args.rows / 10),
    ] {
        let name = format!("{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let config = config(&args);
                    let fixture = if label.starts_with("diff_delete") {
                        runtime
                            .block_on(storage_bench::prepare_tracked_state_diff_tombstone_rows(
                                &backend,
                                config,
                                changed_rows,
                            ))
                            .expect("prepare tracked_state/diff_delete")
                    } else {
                        runtime
                            .block_on(storage_bench::prepare_tracked_state_diff_update_rows(
                                &backend,
                                config,
                                changed_rows,
                            ))
                            .expect("prepare tracked_state/diff_update")
                    };
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::tracked_state_diff_commits_prepared(
                                &backend, &fixture,
                            ))
                            .expect("tracked_state/diff shape succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn prepare_read(
    runtime: &Runtime,
    args: Args,
) -> (
    BenchBackend,
    lix_engine::storage_bench::TrackedStateReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read(
            &backend,
            config(&args),
        ))
        .expect("prepare tracked_state/read");
    (backend, fixture)
}

fn prepare_read_with(
    runtime: &Runtime,
    args: Args,
    config: StorageBenchConfig,
) -> (
    BenchBackend,
    lix_engine::storage_bench::TrackedStateReadFixture,
) {
    let _ = args;
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read(&backend, config))
        .expect("prepare tracked_state/read variant");
    (backend, fixture)
}

fn prepare_read_file_selective_with(
    runtime: &Runtime,
    args: Args,
    config: StorageBenchConfig,
) -> (
    BenchBackend,
    lix_engine::storage_bench::TrackedStateReadFixture,
) {
    let _ = args;
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read_file_selective(
            &backend, config,
        ))
        .expect("prepare tracked_state/read file-selective variant");
    (backend, fixture)
}

fn prepare_read_file_selective_with_max_inline_encoded_value(
    runtime: &Runtime,
    args: Args,
    config: StorageBenchConfig,
    max_inline_encoded_value_bytes: usize,
) -> (
    BenchBackend,
    lix_engine::storage_bench::TrackedStateReadFixture,
) {
    let _ = args;
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(
            storage_bench::prepare_tracked_state_read_file_selective_with_max_inline_encoded_value_bytes(
                &backend,
                config,
                max_inline_encoded_value_bytes,
            ),
        )
        .expect("prepare tracked_state/read file-selective max inline encoded value variant");
    (backend, fixture)
}

fn config(args: &Args) -> StorageBenchConfig {
    args.config()
}
