use std::sync::Arc;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{self, StorageApiFixture, StorageBenchSelectivity};
use lix_engine::Backend;
use tokio::runtime::Runtime;

use crate::{Args, BenchBackend, RocksDbBenchBackend, SqliteBenchBackend};

type BackendFactory = fn() -> Arc<dyn Backend + Send + Sync>;

#[derive(Clone, Copy)]
struct BackendProfile {
    name: &'static str,
    create: BackendFactory,
}

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    for profile in [
        BackendProfile {
            name: "in_memory",
            create: in_memory_backend,
        },
        BackendProfile {
            name: "sqlite_tempfile",
            create: sqlite_tempfile_backend,
        },
        BackendProfile {
            name: "rocksdb_tempdir",
            create: rocksdb_backend,
        },
    ] {
        bench_backend(c, runtime, args, profile);
    }
}

fn bench_backend(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("storage/api/{}", profile.name));

    for rows in [1usize, 10, 100, 1_000, args.rows] {
        group.bench_function(
            format!("write_kv_batch_put/{rows_label}", rows_label = label(rows)),
            |b| {
                b.iter_batched(
                    || (profile.create)(),
                    |backend| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_write_kv_batch_puts(
                                    backend, rows,
                                ))
                                .expect("storage/api write_kv_batch_put succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.bench_function("write_kv_batch_mixed_put_delete/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_mixed_put_delete(
                            backend, args.rows,
                        ))
                        .expect("storage/api write_kv_batch_mixed_put_delete succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write_kv_batch_multi_namespace/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_multi_namespace(
                            backend, args.rows,
                        ))
                        .expect("storage/api write_kv_batch_multi_namespace succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write_kv_batch_duplicate_keys/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_duplicate_keys(
                            backend, args.rows,
                        ))
                        .expect("storage/api write_kv_batch_duplicate_keys succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    for (label, rows, value_bytes) in [
        ("64b", args.rows, 64usize),
        ("1k", args.rows, 1_024),
        ("16k", 1_000, 16 * 1024),
        ("128k", 100, 128 * 1024),
    ] {
        group.bench_function(format!("write_kv_batch_value_size/{label}"), |b| {
            b.iter_batched(
                || (profile.create)(),
                |backend| {
                    black_box(
                        runtime
                            .block_on(storage_bench::storage_api_write_kv_batch_value_size(
                                backend,
                                rows,
                                value_bytes,
                            ))
                            .expect("storage/api write_kv_batch_value_size succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for rows in [1usize, 100, args.rows] {
        group.bench_function(
            format!(
                "transaction_write_and_commit/{rows_label}",
                rows_label = label(rows)
            ),
            |b| {
                b.iter_batched(
                    || (profile.create)(),
                    |backend| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_write_and_commit(
                                    backend, rows,
                                ))
                                .expect("storage/api transaction_write_and_commit succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.bench_function("transaction_rollback_after_write/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_rollback_after_write(
                            backend, args.rows,
                        ))
                        .expect("storage/api transaction_rollback_after_write succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    for reads in [100usize, 1_000, args.rows] {
        group.bench_function(
            format!("get_kv_many_hit/{reads_label}", reads_label = label(reads)),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_get_kv_many_hits_prepared(
                                    &fixture, reads,
                                ))
                                .expect("storage/api get_kv_many_hit succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!(
                "get_kv_many_exists/{reads_label}",
                reads_label = label(reads)
            ),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_get_kv_many_exists_prepared(
                                    &fixture, reads,
                                ))
                                .expect("storage/api get_kv_many_exists succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("get_kv_many_miss/{reads_label}", reads_label = label(reads)),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_get_kv_many_misses_prepared(
                                    &fixture, reads,
                                ))
                                .expect("storage/api get_kv_many_miss succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!(
                "get_kv_many_mixed_hit_miss/{reads_label}",
                reads_label = label(reads)
            ),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::storage_api_get_kv_many_mixed_hit_miss_prepared(
                                        &fixture, reads,
                                    ),
                                )
                                .expect("storage/api get_kv_many_mixed_hit_miss succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!(
                "get_kv_many_duplicate_keys/{reads_label}",
                reads_label = label(reads)
            ),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::storage_api_get_kv_many_duplicate_keys_prepared(
                                        &fixture, reads,
                                    ),
                                )
                                .expect("storage/api get_kv_many_duplicate_keys succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.bench_function("get_kv_many_multi_namespace/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_get_kv_many_multi_namespace(
                            backend, args.rows,
                        ))
                        .expect("storage/api get_kv_many_multi_namespace succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    for limit in [100usize, 1_000, args.rows] {
        group.bench_function(
            format!("scan_kv_prefix/{limit_label}", limit_label = label(limit)),
            |b| {
                b.iter_batched(
                    || prepare_read(runtime, args.rows, profile.create),
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(storage_bench::storage_api_scan_kv_prefix_prepared(
                                    &fixture, limit,
                                ))
                                .expect("storage/api scan_kv_prefix succeeds"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.bench_function("scan_kv_after_pages/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.rows, profile.create),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_scan_kv_after_pages_prepared(
                            &fixture, 100,
                        ))
                        .expect("storage/api scan_kv_after_pages succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_kv_small_limit_of_large_range/100_of_10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.rows, profile.create),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_scan_kv_prefix_prepared(
                            &fixture, 100,
                        ))
                        .expect("storage/api scan_kv_small_limit_of_large_range succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_kv_empty_range/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args.rows, profile.create),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_scan_kv_empty_range_prepared(
                            &fixture,
                        ))
                        .expect("storage/api scan_kv_empty_range succeeds"),
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
        group.bench_function(format!("scan_kv_prefix_selectivity_{label}/10k"), |b| {
            b.iter_batched(
                || prepare_selective_scan(runtime, args.rows, selectivity, profile.create),
                |fixture| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::storage_api_scan_kv_selective_prefix_prepared(
                                    &fixture,
                                    selectivity,
                                ),
                            )
                            .expect("storage/api scan_kv_prefix_selectivity succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.bench_function("transaction_commit_empty", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_transaction_commit_empty(backend))
                        .expect("storage/api transaction_commit_empty succeeds"),
                )
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn prepare_read(
    runtime: &Runtime,
    rows: usize,
    create_backend: BackendFactory,
) -> StorageApiFixture {
    let backend = create_backend();
    runtime
        .block_on(storage_bench::prepare_storage_api_read(backend, rows))
        .expect("prepare storage/api read fixture")
}

fn prepare_selective_scan(
    runtime: &Runtime,
    rows: usize,
    selectivity: StorageBenchSelectivity,
    create_backend: BackendFactory,
) -> StorageApiFixture {
    let backend = create_backend();
    runtime
        .block_on(storage_bench::prepare_storage_api_selective_scan(
            backend,
            rows,
            selectivity,
        ))
        .expect("prepare storage/api selective scan fixture")
}

fn in_memory_backend() -> Arc<dyn Backend + Send + Sync> {
    BenchBackend::new()
}

fn sqlite_tempfile_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(SqliteBenchBackend::tempfile().expect("create sqlite tempfile bench backend"))
}

fn rocksdb_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(RocksDbBenchBackend::new().expect("create rocksdb bench backend"))
}

fn label(rows: usize) -> String {
    if rows >= 1_000 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}
