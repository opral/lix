use std::sync::Arc;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{self, StorageBenchSelectivity};
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
        bench_fast(c, runtime, args, profile);
        bench_full(c, runtime, args, profile);
    }
}

fn bench_fast(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/backend_kv/fast/{}", profile.name));

    group.bench_function("write_batch_put/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_puts(
                            backend, args.rows,
                        ))
                        .expect("physical_layout/backend_kv write_batch_put succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("mixed_put_delete/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_mixed_put_delete(
                            backend, args.rows,
                        ))
                        .expect("physical_layout/backend_kv mixed_put_delete succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_values_hit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, profile, args.rows),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_get_values_hits_prepared(
                            &fixture, args.rows,
                        ))
                        .expect("physical_layout/backend_kv get_values_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_keys_prefix/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, profile, args.rows),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_scan_keys_prefix_prepared(
                            &fixture, args.rows,
                        ))
                        .expect("physical_layout/backend_kv scan_keys_prefix succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_full(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/backend_kv/full/{}", profile.name));

    for rows in [1_000usize, 10_000, 50_000] {
        group.bench_function(format!("write_batch_put/{}", label(rows)), |b| {
            b.iter_batched(
                || (profile.create)(),
                |backend| {
                    black_box(
                        runtime
                            .block_on(storage_bench::storage_api_write_kv_batch_puts(
                                backend, rows,
                            ))
                            .expect("physical_layout/backend_kv full write_batch_put succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.bench_function("write_batch_value_size_1k/10k", |b| {
        b.iter_batched(
            || (profile.create)(),
            |backend| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_write_kv_batch_value_size(
                            backend, args.rows, 1024,
                        ))
                        .expect("physical_layout/backend_kv value_size_1k succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_values_miss/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, profile, args.rows),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_get_values_misses_prepared(
                            &fixture, args.rows,
                        ))
                        .expect("physical_layout/backend_kv get_values_miss succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("get_values_mixed_hit_miss/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, profile, args.rows),
            |fixture| {
                black_box(
                    runtime
                        .block_on(
                            storage_bench::storage_api_get_values_mixed_hit_miss_prepared(
                                &fixture, args.rows,
                            ),
                        )
                        .expect("physical_layout/backend_kv get_values_mixed succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("scan_keys_after_pages/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, profile, args.rows),
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::storage_api_scan_keys_after_pages_prepared(
                            &fixture, 1024,
                        ))
                        .expect("physical_layout/backend_kv scan_keys_after_pages succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    for selectivity in [
        StorageBenchSelectivity::Percent1,
        StorageBenchSelectivity::Percent10,
    ] {
        let label = match selectivity {
            StorageBenchSelectivity::Percent1 => "1pct",
            StorageBenchSelectivity::Percent10 => "10pct",
            StorageBenchSelectivity::Percent100 => "100pct",
        };
        group.bench_function(format!("scan_keys_selective_prefix_{label}/10k"), |b| {
            b.iter_batched(
                || prepare_selective_scan(runtime, profile, args.rows, selectivity),
                |fixture| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::storage_api_scan_keys_selective_prefix_prepared(
                                    &fixture,
                                    selectivity,
                                ),
                            )
                            .expect("physical_layout/backend_kv selective scan succeeds"),
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
    profile: BackendProfile,
    rows: usize,
) -> storage_bench::StorageApiFixture {
    runtime
        .block_on(storage_bench::prepare_storage_api_read(
            (profile.create)(),
            rows,
        ))
        .expect("prepare physical_layout/backend_kv read")
}

fn prepare_selective_scan(
    runtime: &Runtime,
    profile: BackendProfile,
    rows: usize,
    selectivity: StorageBenchSelectivity,
) -> storage_bench::StorageApiFixture {
    runtime
        .block_on(storage_bench::prepare_storage_api_selective_scan(
            (profile.create)(),
            rows,
            selectivity,
        ))
        .expect("prepare physical_layout/backend_kv selective scan")
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
