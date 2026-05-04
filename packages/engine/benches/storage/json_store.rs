use lix_engine::storage_bench::{
    self, JsonStorePayloadShape, JsonStoreProjectionShape, JsonStoreReadFixture,
};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, _args: Args) {
    let mut group = c.benchmark_group("storage/json_store");

    for (name, shape, rows) in [
        (
            "write/small_raw_1k/1000",
            JsonStorePayloadShape::SmallRaw1k,
            1_000,
        ),
        (
            "write/medium_structured_16k/200",
            JsonStorePayloadShape::MediumStructured16k,
            200,
        ),
        (
            "write/large_structured_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            50,
        ),
        (
            "write/large_array_128k/50",
            JsonStorePayloadShape::LargeArray128k,
            50,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let backend = BenchBackend::new();
                    let fixture = runtime
                        .block_on(storage_bench::prepare_json_store_write(shape, rows))
                        .expect("prepare json_store/write");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::json_store_write_prepared(&backend, &fixture))
                            .expect("json_store/write succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.bench_function("write/dedupe_same_16k/1000", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_json_store_write_dedupe(
                        JsonStorePayloadShape::MediumStructured16k,
                        1_000,
                    ))
                    .expect("prepare json_store/write_dedupe");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_write_prepared(&backend, &fixture))
                        .expect("json_store/write_dedupe succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write/against_base_object_update_1_of_1000/50", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_json_store_base_update_object(
                        &backend, 50,
                    ))
                    .expect("prepare json_store/base_update_object");
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
                        .expect("json_store/base_update_object succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("write/against_base_array_update_1_of_1000/50", |b| {
        b.iter_batched(
            || {
                let backend = BenchBackend::new();
                let fixture = runtime
                    .block_on(storage_bench::prepare_json_store_base_update_array(
                        &backend, 50,
                    ))
                    .expect("prepare json_store/base_update_array");
                (backend, fixture)
            },
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::json_store_write_against_base_array_prepared(
                            &backend, &fixture,
                        ))
                        .expect("json_store/base_update_array succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    for (name, shape, rows) in [
        (
            "read_bytes/small_raw_1k/1000",
            JsonStorePayloadShape::SmallRaw1k,
            1_000,
        ),
        (
            "read_bytes/medium_structured_16k/200",
            JsonStorePayloadShape::MediumStructured16k,
            200,
        ),
        (
            "read_bytes/large_structured_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            50,
        ),
        (
            "read_bytes/large_array_128k/50",
            JsonStorePayloadShape::LargeArray128k,
            50,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read(
                        runtime,
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
                            .expect("json_store/read_bytes succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, shape, rows) in [
        (
            "read_value/small_raw_1k/1000",
            JsonStorePayloadShape::SmallRaw1k,
            1_000,
        ),
        (
            "read_value/large_structured_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            50,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    prepare_read(
                        runtime,
                        shape,
                        rows,
                        JsonStoreProjectionShape::TopLevelTarget,
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::json_store_read_value_prepared(
                                &backend, &fixture,
                            ))
                            .expect("json_store/read_value succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, shape, projection, rows) in [
        (
            "read_projection/top_level_1_prop_1k/1000",
            JsonStorePayloadShape::SmallRaw1k,
            JsonStoreProjectionShape::TopLevelTarget,
            1_000,
        ),
        (
            "read_projection/top_level_1_prop_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            JsonStoreProjectionShape::TopLevelTarget,
            50,
        ),
        (
            "read_projection/top_level_10_props_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            JsonStoreProjectionShape::TopLevelTenProps,
            50,
        ),
        (
            "read_projection/nested_prop_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            JsonStoreProjectionShape::NestedTarget,
            50,
        ),
        (
            "read_projection/array_item_1_of_1000/50",
            JsonStorePayloadShape::LargeArray128k,
            JsonStoreProjectionShape::ArrayItem999,
            50,
        ),
        (
            "read_projection/filter_prop_status_128k/50",
            JsonStorePayloadShape::LargeStructured128k,
            JsonStoreProjectionShape::Status,
            50,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_read(runtime, shape, rows, projection),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::json_store_read_projection_prepared(
                                &backend, &fixture,
                            ))
                            .expect("json_store/read_projection succeeds"),
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
    shape: JsonStorePayloadShape,
    rows: usize,
    projection: JsonStoreProjectionShape,
) -> (
    std::sync::Arc<dyn lix_engine::Backend + Send + Sync>,
    JsonStoreReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_json_store_projection_read(
            &backend, shape, rows, projection,
        ))
        .expect("prepare json_store/read");
    (backend, fixture)
}
