use std::sync::Arc;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{
    self, JsonStorePayloadShape, JsonStoreProjectionShape, JsonStoreReadFixture,
};
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

fn bench_fast(c: &mut Criterion, runtime: &Runtime, _args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/json_store/fast/{}", profile.name));

    group.bench_function("write_unique_1k/10k", |b| {
        b.iter_batched(
            || prepare_write(runtime, JsonStorePayloadShape::SmallRaw1k, 10_000),
            |fixture| {
                let backend = (profile.create)();
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_write_prepared(&backend, &fixture))
                        .expect("physical_layout/json_store write_unique_1k succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write_same_1k/10k", |b| {
        b.iter_batched(
            || prepare_write_dedupe(runtime, JsonStorePayloadShape::SmallRaw1k, 10_000),
            |fixture| {
                let backend = (profile.create)();
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_write_prepared(&backend, &fixture))
                        .expect("physical_layout/json_store write_same_1k succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("read_bytes_1k/10k", |b| {
        b.iter_batched(
            || {
                prepare_read(
                    runtime,
                    profile,
                    JsonStorePayloadShape::SmallRaw1k,
                    10_000,
                    JsonStoreProjectionShape::TopLevelTarget,
                )
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_read_bytes_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/json_store read_bytes_1k succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_full(c: &mut Criterion, runtime: &Runtime, _args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/json_store/full/{}", profile.name));

    for (name, shape, rows, dedupe) in [
        (
            "write_unique_1k/10k",
            JsonStorePayloadShape::SmallRaw1k,
            10_000usize,
            false,
        ),
        (
            "write_same_1k/10k",
            JsonStorePayloadShape::SmallRaw1k,
            10_000,
            true,
        ),
        (
            "write_unique_16k/1k",
            JsonStorePayloadShape::MediumStructured16k,
            1_000,
            false,
        ),
        (
            "write_same_16k/1k",
            JsonStorePayloadShape::MediumStructured16k,
            1_000,
            true,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    if dedupe {
                        prepare_write_dedupe(runtime, shape, rows)
                    } else {
                        prepare_write(runtime, shape, rows)
                    }
                },
                |fixture| {
                    let backend = (profile.create)();
                    black_box(
                        runtime
                            .block_on(storage_bench::json_store_write_prepared(&backend, &fixture))
                            .expect("physical_layout/json_store full write succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, shape, rows) in [
        (
            "read_bytes_1k/10k",
            JsonStorePayloadShape::SmallRaw1k,
            10_000usize,
        ),
        (
            "read_bytes_16k/1k",
            JsonStorePayloadShape::MediumStructured16k,
            1_000,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read(
                        runtime,
                        profile,
                        shape,
                        rows,
                        JsonStoreProjectionShape::TopLevelTarget,
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::json_store_read_bytes_prepared(
                                &backend, &fixture,
                            ))
                            .expect("physical_layout/json_store full read_bytes succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.bench_function("read_projection_top_level_128k/50", |b| {
        b.iter_batched(
            || {
                prepare_read(
                    runtime,
                    profile,
                    JsonStorePayloadShape::LargeStructured128k,
                    50,
                    JsonStoreProjectionShape::TopLevelTarget,
                )
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_read_projection_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/json_store projection succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write_against_base_object_update_1_of_1000/50", |b| {
        b.iter_batched(
            || {
                let backend = (profile.create)();
                let fixture = runtime
                    .block_on(storage_bench::prepare_json_store_base_update_object(
                        &backend, 50,
                    ))
                    .expect("prepare physical_layout/json_store base update object");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(
                            storage_bench::json_store_write_against_base_object_prepared(
                                &backend, &fixture,
                            ),
                        )
                        .expect("physical_layout/json_store base update object succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn prepare_write(
    runtime: &Runtime,
    shape: JsonStorePayloadShape,
    rows: usize,
) -> storage_bench::JsonStoreWriteFixture {
    runtime
        .block_on(storage_bench::prepare_json_store_write(shape, rows))
        .expect("prepare physical_layout/json_store write")
}

fn prepare_write_dedupe(
    runtime: &Runtime,
    shape: JsonStorePayloadShape,
    rows: usize,
) -> storage_bench::JsonStoreWriteFixture {
    runtime
        .block_on(storage_bench::prepare_json_store_write_dedupe(shape, rows))
        .expect("prepare physical_layout/json_store write dedupe")
}

fn prepare_read(
    runtime: &Runtime,
    profile: BackendProfile,
    shape: JsonStorePayloadShape,
    rows: usize,
    projection: JsonStoreProjectionShape,
) -> (Arc<dyn Backend + Send + Sync>, JsonStoreReadFixture) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_json_store_projection_read(
            &backend, shape, rows, projection,
        ))
        .expect("prepare physical_layout/json_store read");
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
