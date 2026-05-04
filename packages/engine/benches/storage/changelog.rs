use lix_engine::storage_bench::{
    self, StorageBenchConfig, StorageBenchKeyPattern, StorageBenchSelectivity,
};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("storage/changelog");
    group.bench_function("encode_only/full_row/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(storage_bench::prepare_changelog_codec(config(&args)))
                    .expect("prepare changelog/encode_only")
            },
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_encode_only_prepared(&fixture))
                        .expect("changelog/encode_only succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("decode_only/full_row/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(storage_bench::prepare_changelog_codec(config(&args)))
                    .expect("prepare changelog/decode_only")
            },
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_decode_only_prepared(&fixture))
                        .expect("changelog/decode_only succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("append_changes/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_changelog_append_changes(config(
                        &args,
                    )))
                    .expect("prepare changelog/append_changes");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/append_changes succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("load_change_hit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_load_change_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/load_change_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("load_change_miss/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_load_change_miss_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/load_change_miss succeeds"),
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
                        .block_on(storage_bench::changelog_scan_all_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/scan_all succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_limit_100/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_scan_limit_100_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/scan_limit_100 succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for rows in [1, 10, 100, 1_000] {
        let name = format!("append_changes/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_append_changes(
                            config(&args).with_rows(rows),
                        ))
                        .expect("prepare changelog/append_changes batch");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("changelog/append_changes batch succeeds"),
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
        let name = format!("append_changes_payload_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_append_changes(
                            config(&args)
                                .with_state_payload_bytes(bytes)
                                .with_rows(rows),
                        ))
                        .expect("prepare changelog/append_changes payload");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("changelog/append_changes payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("append_changes_metadata_1k/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_changelog_append_metadata(
                        config(&args).with_state_payload_bytes(1024),
                    ))
                    .expect("prepare changelog/append metadata");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/append metadata succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for (label, bytes, rows) in [("1k", 1024, 10_000), ("16k", 16 * 1024, 1_000)] {
        let name = format!("append_changes_shared_payload_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_append_shared_payload(
                            config(&args)
                                .with_state_payload_bytes(bytes)
                                .with_rows(rows),
                        ))
                        .expect("prepare changelog/append shared payload");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("changelog/append shared payload succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, bytes, rows) in [("1k", 1024, 10_000), ("16k", 16 * 1024, 1_000)] {
        let name = format!("append_changes_shared_metadata_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_append_shared_metadata(
                            config(&args)
                                .with_state_payload_bytes(bytes)
                                .with_rows(rows),
                        ))
                        .expect("prepare changelog/append shared metadata");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("changelog/append shared metadata succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("append_changes_shared_payload_and_metadata_1k/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_changelog_append_shared_payload_and_metadata(
                            config(&args).with_state_payload_bytes(1024),
                        ),
                    )
                    .expect("prepare changelog/append shared payload and metadata");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/append shared payload and metadata succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("append_changes_tombstone/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_changelog_append_tombstones(config(
                        &args,
                    )))
                    .expect("prepare changelog/append tombstones");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/append tombstones succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("append_changes_composite_entity_id/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_changelog_append_composite_entity_ids(config(&args)),
                    )
                    .expect("prepare changelog/append composite entity ids");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_append_changes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/append composite entity ids succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for (label, selectivity) in [
        ("1pct", StorageBenchSelectivity::Percent1),
        ("10pct", StorageBenchSelectivity::Percent10),
        ("100pct", StorageBenchSelectivity::Percent100),
    ] {
        let name = format!("scan_schema_selectivity_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_read_with_selectivity(
                            &backend,
                            config(&args).with_selectivity(selectivity),
                        ))
                        .expect("prepare changelog/scan schema selectivity");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_scan_schema_prepared(
                                &backend,
                                &fixture,
                                selectivity,
                            ))
                            .expect("changelog/scan schema selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.bench_function("scan_entity_history/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_changelog_read_entity_history(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare changelog/scan entity history");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_scan_entity_history_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/scan entity history succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("scan_commit_facts/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_changelog_read_commit_facts(
                        &backend,
                        config(&args),
                    ))
                    .expect("prepare changelog/scan commit facts");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::changelog_scan_commit_facts_prepared(
                            &backend, &fixture,
                        ))
                        .expect("changelog/scan commit facts succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for (label, key_pattern) in [
        ("sequential_keys", StorageBenchKeyPattern::Sequential),
        ("random_keys", StorageBenchKeyPattern::Random),
    ] {
        let name = format!("append_changes_{label}/10k");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_changelog_append_changes(
                            config(&args).with_key_pattern(key_pattern),
                        ))
                        .expect("prepare changelog/append_changes key pattern");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::changelog_append_changes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("changelog/append_changes key pattern succeeds"),
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
    std::sync::Arc<dyn lix_engine::Backend + Send + Sync>,
    lix_engine::storage_bench::ChangelogReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_changelog_read(
            &backend,
            config(&args),
        ))
        .expect("prepare changelog/read");
    (backend, fixture)
}

fn config(args: &Args) -> StorageBenchConfig {
    args.config()
}
