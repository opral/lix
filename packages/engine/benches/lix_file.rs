use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;
use support::test_json_plugin::{
    dummy_wasm_header, BenchJsonPluginRuntime, TEST_JSON_POINTER_SCHEMA_DEFINITION,
    TEST_PLUGIN_MANIFEST_JSON,
};

const JSON_LEAF_COUNT: usize = 120;

fn bench_lix_file_insert_no_plugin(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(false))
        .expect("failed to seed engine for no-plugin bench");

    run_file_insert_bench(c, &runtime, &engine, "lix_file_insert_no_plugin", false);
}

fn bench_lix_file_insert_plugin_json(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(true))
        .expect("failed to seed engine for plugin bench");

    run_file_insert_bench(c, &runtime, &engine, "lix_file_insert_plugin_json", true);
}

fn run_file_insert_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    bench_name: &str,
    plugin_active: bool,
) {
    let mut seq: u64 = 0;
    c.bench_function(bench_name, |b| {
        b.iter(|| {
            let file_id = format!("bench-file-{seq}");
            let ext = if plugin_active { "json" } else { "txt" };
            let path = format!("/bench/{file_id}.{ext}");
            let data = json_bytes(seq, JSON_LEAF_COUNT);
            seq += 1;

            let result = runtime
                .block_on(engine.execute(
                    "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                    &[Value::Text(file_id), Value::Text(path), Value::Blob(data)],
                    ExecuteOptions::default(),
                ))
                .expect("file insert should succeed");

            black_box(result.rows.len());
        });
    });
}

async fn seed_engine(with_plugin: bool) -> Result<lix_engine::Engine, LixError> {
    let backend = Box::new(BenchSqliteBackend::in_memory());
    let mut boot_args = BootArgs::new(backend);
    if with_plugin {
        boot_args.wasm_runtime = Some(Arc::new(BenchJsonPluginRuntime));
    }
    let engine = boot(boot_args);
    engine.init().await?;

    if with_plugin {
        engine
            .execute(
            "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES ('test_json_pointer~1', 'lix_stored_schema', 'lix', 'global', 'lix', ?, '1')",
                &[Value::Text(TEST_JSON_POINTER_SCHEMA_DEFINITION.to_string())],
                ExecuteOptions::default(),
            )
            .await?;

        engine
            .install_plugin(TEST_PLUGIN_MANIFEST_JSON, &dummy_wasm_header())
            .await?;

        let warmup_file_id = "bench-plugin-warmup";
        let warmup_path = format!("/bench/{warmup_file_id}.json");
        let warmup_data = json_bytes(0, JSON_LEAF_COUNT);
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[
                    Value::Text(warmup_file_id.to_string()),
                    Value::Text(warmup_path),
                    Value::Blob(warmup_data),
                ],
                ExecuteOptions::default(),
            )
            .await?;

        let detected = engine
            .execute(
                "SELECT COUNT(*) FROM lix_state WHERE file_id = ? AND plugin_key = 'test_json_plugin'",
                &[Value::Text(warmup_file_id.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        let count = scalar_count(&detected)?;
        if count <= 0 {
            return Err(LixError {
                message: "plugin benchmark warmup produced no detected rows".to_string(),
            });
        }
    }

    Ok(engine)
}

fn scalar_count(result: &lix_engine::QueryResult) -> Result<i64, LixError> {
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            message: "count query returned no rows".to_string(),
        })?;

    match value {
        Value::Integer(value) => Ok(*value),
        Value::Real(value) => Ok(*value as i64),
        other => Err(LixError {
            message: format!("count query returned unexpected value: {other:?}"),
        }),
    }
}

fn json_bytes(seed: u64, leaf_count: usize) -> Vec<u8> {
    let mut payload = serde_json::Map::with_capacity(leaf_count);
    for index in 0..leaf_count {
        payload.insert(
            format!("k_{index}"),
            json!(format!("{seed}_{index}_{}", "x".repeat((index % 9) + 1))),
        );
    }
    serde_json::to_vec(&serde_json::Value::Object(payload))
        .expect("json payload serialization should succeed")
}

criterion_group!(
    benches,
    bench_lix_file_insert_no_plugin,
    bench_lix_file_insert_plugin_json
);
criterion_main!(benches);
