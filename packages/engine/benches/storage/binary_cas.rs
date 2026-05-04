use lix_engine::storage_bench::{self, StorageBenchConfig};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("storage/binary_cas");
    group.bench_function("write_blobs_1k/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_binary_cas_write_blobs(config(&args)))
                    .expect("prepare binary_cas/write_blobs");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::binary_cas_write_blobs_prepared(
                            &backend, &fixture,
                        ))
                        .expect("binary_cas/write_blobs succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("read_blob_hit_1k/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::binary_cas_read_blob_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("binary_cas/read_blob_hit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("read_blob_miss_1k/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::binary_cas_read_blob_miss_prepared(
                            &backend, &fixture,
                        ))
                        .expect("binary_cas/read_blob_miss succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("write_duplicate_payload_1k/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_binary_cas_write_duplicate_payload(
                        config(&args),
                    ))
                    .expect("prepare binary_cas/write_duplicate_payload");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::binary_cas_write_blobs_prepared(
                            &backend, &fixture,
                        ))
                        .expect("binary_cas/write_duplicate_payload succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("write_half_duplicate_payload_1k/10k", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_binary_cas_write_half_duplicate_payload(config(
                            &args,
                        )),
                    )
                    .expect("prepare binary_cas/write_half_duplicate_payload");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::binary_cas_write_blobs_prepared(
                            &backend, &fixture,
                        ))
                        .expect("binary_cas/write_half_duplicate_payload succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    for rows in [1, 10, 100, 1_000] {
        let name = format!("write_blobs_1k/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_binary_cas_write_blobs(
                            config(&args).with_rows(rows),
                        ))
                        .expect("prepare binary_cas/write_blobs batch");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::binary_cas_write_blobs_prepared(
                                &backend, &fixture,
                            ))
                            .expect("binary_cas/write_blobs batch succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }
    for (label, bytes, rows) in [
        ("small", 16, 10_000),
        ("1k", 1024, 10_000),
        ("16k", 16 * 1024, 1_000),
        ("128k", 128 * 1024, 100),
    ] {
        let name = format!("write_blobs_payload_{label}/{rows}");
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_binary_cas_write_blobs(
                            config(&args).with_blob_bytes(bytes).with_rows(rows),
                        ))
                        .expect("prepare binary_cas/write_blobs payload");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::binary_cas_write_blobs_prepared(
                                &backend, &fixture,
                            ))
                            .expect("binary_cas/write_blobs payload succeeds"),
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
    lix_engine::storage_bench::BinaryCasReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_binary_cas_read(
            &backend,
            config(&args),
        ))
        .expect("prepare binary_cas/read");
    (backend, fixture)
}

fn config(args: &Args) -> StorageBenchConfig {
    args.config()
}
