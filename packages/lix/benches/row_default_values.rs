//! Measures a one-row registered-schema insert using standard SQL
//! `INSERT ... DEFAULT VALUES`, excluding lix and schema-registration setup.

use std::{future::IntoFuture, hint::black_box};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lix::Value;
use lix::storage::Memory;
use lix::{Lix, open_lix};
use serde_json::json;

const DEFAULT_VALUES_SQL: &str = "INSERT INTO bench_default_values DEFAULT VALUES";
const EXPLICIT_VALUES_SQL: &str = "INSERT INTO bench_default_values (label) VALUES ('untitled')";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime")
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Lix<Memory> {
    runtime.block_on(async {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage)
            .await
            .expect("open benchmark lix");
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bench_default_values",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "label", "type": "text", "nullable": false, "default_value": "untitled" }
            ],
            "primary_key": ["id"]
        });
        let registered = session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1 ->> 'key', $1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("register benchmark schema");
        assert_eq!(registered.rows_affected(), 1);
        session
    })
}

fn execute(runtime: &tokio::runtime::Runtime, session: &Lix<Memory>, sql: &str) {
    black_box(
        runtime
            .block_on(session.execute(sql, &[]).into_future())
            .expect("execute benchmark SQL"),
    );
}

fn row_default_values(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("row_default_values");
    group.throughput(Throughput::Elements(1));
    // Each sample receives a fresh preconfigured session, so this captures
    // cold transaction behavior without charging lix boot or registration.
    group.bench_function("cold_standard_sql", |b| {
        b.iter_batched_ref(
            || fixture(&runtime),
            |session| execute(&runtime, session, DEFAULT_VALUES_SQL),
            BatchSize::LargeInput,
        );
    });
    group.bench_function("cold_explicit_values_control", |b| {
        b.iter_batched_ref(
            || fixture(&runtime),
            |session| execute(&runtime, session, EXPLICIT_VALUES_SQL),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, row_default_values);
criterion_main!(benches);
