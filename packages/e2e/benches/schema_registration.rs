//! Measures one schema registration on a pre-initialized in-memory session.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lix::storage::Memory;
use lix::{Lix, Value, open_lix};
use serde_json::json;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime")
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Lix<Memory> {
    runtime.block_on(async {
        open_lix()
            .with_storage(Memory::new())
            .await
            .expect("open in-memory lix")
    })
}

fn schema_v1() -> Value {
    Value::Jsonb(
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bench_registration",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "name", "type": "text", "nullable": false },
                { "name": "metadata", "type": "jsonb", "nullable": false }
            ],
            "primary_key": ["id"]
        })
        .into(),
    )
}

fn schema_registration(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("schema_registration");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single", |b| {
        b.iter_batched_ref(
            || fixture(&runtime),
            |lix| {
                black_box(
                    runtime
                        .block_on(async {
                            lix.execute(
                                "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1 ->> 'key', $1)",
                                &[schema_v1()],
                            )
                            .await
                        })
                        .unwrap(),
                );
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, schema_registration);
criterion_main!(benches);
