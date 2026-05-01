use lix_engine::storage_bench::{self, StorageBenchConfig, StorageBenchKeyPattern};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("storage/changelog");
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
    BenchBackend,
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
