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
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "x-lix-key": "bench_default_values",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "x-lix-default": "lix_uuid_v7()"
                },
                "label": { "type": "string", "default": "untitled" }
            },
            "required": ["id", "label"],
            "additionalProperties": false
        });
        let registered = session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Json(schema.into())],
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

fn entity_default_values(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("entity_default_values");
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

criterion_group!(benches, entity_default_values);
criterion_main!(benches);
