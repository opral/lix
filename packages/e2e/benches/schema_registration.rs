//! Measures one schema registration on a pre-initialized in-memory session.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Memory;
use serde_json::json;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime")
}

fn fixture(runtime: &tokio::runtime::Runtime) -> SessionContext<Memory> {
    runtime.block_on(async {
        let storage = Memory::new();
        Engine::initialize(storage.clone()).await.unwrap();
        Engine::new(storage)
            .await
            .unwrap()
            .open_workspace_session()
            .await
            .unwrap()
    })
}

fn schema_v1() -> Value {
    Value::Json(
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bench_registration",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "name", "type": "text", "nullable": false },
                { "name": "metadata", "type": "jsonb", "nullable": true }
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
            |session| {
                black_box(
                    runtime
                        .block_on(session.execute(
                            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1 ->> 'key', $1)",
                            &[schema_v1()],
                        ))
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
