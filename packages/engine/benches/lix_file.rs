use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, NoopWasmRuntime, Value};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;
use support::test_json_plugin::{
    build_test_plugin_archive, BenchJsonPluginRuntime, TEST_JSON_POINTER_SCHEMA_DEFINITION,
};

const JSON_LEAF_COUNT: usize = 8;
const FAST_ID_REPRO_FILE_ID: &str = "bench-fast-id-repro-file";
const FAST_ID_REPRO_SEED_UPDATES: usize = 4;
const READ_SCAN_FILE_COUNT_NO_PLUGIN: usize = 2;
const READ_POINT_FILE_COUNT_NO_PLUGIN: usize = 2;
const READ_SCAN_FILE_COUNT_PLUGIN: usize = 2;
const READ_POINT_FILE_COUNT_PLUGIN: usize = 2;
const READ_POINT_TARGET_INDEX: usize = 1;

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

fn bench_lix_file_exact_delete_missing_ids(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(false))
        .expect("failed to seed engine for exact-delete bench");

    c.bench_function("lix_file_exact_delete_missing_ids", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "DELETE FROM lix_file \
                     WHERE id IN ('bench-missing-delete-a', 'bench-missing-delete-b')",
                    &[],
                    ExecuteOptions::default(),
                ))
                .expect("delete should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_file_exact_update_missing_id(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(false))
        .expect("failed to seed engine for exact-update bench");

    c.bench_function("lix_file_exact_update_missing_id", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "UPDATE lix_file \
                     SET path = '/bench/missing-update.txt', data = x'01' \
                     WHERE id = 'bench-missing-update-id'",
                    &[],
                    ExecuteOptions::default(),
                ))
                .expect("update should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_file_update_fast_id_repro(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_fast_id_repro())
        .expect("failed to seed engine for fast-id repro bench");

    let mut seq: u64 = 0;
    c.bench_function("lix_file_update_fast_id_repro", |b| {
        b.iter(|| {
            let path = format!("/bench/fast-id-repro/{seq:06}.txt");
            let data = vec![
                (seq % 251) as u8,
                ((seq + 1) % 251) as u8,
                ((seq + 2) % 251) as u8,
            ];
            let sql = "UPDATE lix_file SET path = ?, data = ? WHERE id = ?";
            seq += 1;

            let result = runtime
                .block_on(engine.execute(
                    sql,
                    &[
                        Value::Text(path),
                        Value::Blob(data),
                        Value::Text(FAST_ID_REPRO_FILE_ID.to_string()),
                    ],
                    ExecuteOptions::default(),
                ))
                .expect("fast-id repro update should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_file_read_scan_no_plugin(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_read_dataset(
            false,
            READ_SCAN_FILE_COUNT_NO_PLUGIN,
        ))
        .expect("failed to seed engine for read-scan no-plugin bench");

    let warmup = runtime
        .block_on(engine.execute(
            "SELECT path, data FROM lix_file ORDER BY path",
            &[],
            ExecuteOptions::default(),
        ))
        .expect("warmup read scan should succeed");
    let expected_rows = warmup.rows.len();

    c.bench_function("lix_file_read_scan_path_data_no_plugin", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "SELECT path, data FROM lix_file ORDER BY path",
                    &[],
                    ExecuteOptions::default(),
                ))
                .expect("read scan should succeed");
            black_box(result.rows.len());
            debug_assert_eq!(result.rows.len(), expected_rows);
        });
    });
}

fn bench_lix_file_read_scan_plugin_json(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_read_dataset(
            true,
            READ_SCAN_FILE_COUNT_PLUGIN,
        ))
        .expect("failed to seed engine for read-scan plugin bench");

    let warmup = runtime
        .block_on(engine.execute(
            "SELECT path, data FROM lix_file ORDER BY path",
            &[],
            ExecuteOptions::default(),
        ))
        .expect("warmup read scan should succeed");
    let expected_rows = warmup.rows.len();

    c.bench_function("lix_file_read_scan_path_data_plugin_json", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "SELECT path, data FROM lix_file ORDER BY path",
                    &[],
                    ExecuteOptions::default(),
                ))
                .expect("read scan should succeed");
            black_box(result.rows.len());
            debug_assert_eq!(result.rows.len(), expected_rows);
        });
    });
}

fn bench_lix_file_read_point_path_plugin_json(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_read_dataset(
            true,
            READ_POINT_FILE_COUNT_PLUGIN,
        ))
        .expect("failed to seed engine for read-point plugin bench");

    let target_path = read_dataset_path(READ_POINT_TARGET_INDEX, true);
    let warmup = runtime
        .block_on(engine.execute(
            "SELECT path, data FROM lix_file WHERE path = ?",
            &[Value::Text(target_path.clone())],
            ExecuteOptions::default(),
        ))
        .expect("warmup point read should succeed");
    let expected_rows = warmup.rows.len();

    c.bench_function("lix_file_read_point_path_data_plugin_json", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "SELECT path, data FROM lix_file WHERE path = ?",
                    &[Value::Text(target_path.clone())],
                    ExecuteOptions::default(),
                ))
                .expect("point read should succeed");
            black_box(result.rows.len());
            debug_assert_eq!(result.rows.len(), expected_rows);
        });
    });
}

fn bench_lix_file_read_point_path_no_plugin(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_read_dataset(
            false,
            READ_POINT_FILE_COUNT_NO_PLUGIN,
        ))
        .expect("failed to seed engine for read-point no-plugin bench");

    let target_path = read_dataset_path(READ_POINT_TARGET_INDEX, false);
    let warmup = runtime
        .block_on(engine.execute(
            "SELECT path, data FROM lix_file WHERE path = ?",
            &[Value::Text(target_path.clone())],
            ExecuteOptions::default(),
        ))
        .expect("warmup point read should succeed");
    let expected_rows = warmup.rows.len();

    c.bench_function("lix_file_read_point_path_data_no_plugin", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(
                    "SELECT path, data FROM lix_file WHERE path = ?",
                    &[Value::Text(target_path.clone())],
                    ExecuteOptions::default(),
                ))
                .expect("point read should succeed");
            black_box(result.rows.len());
            debug_assert_eq!(result.rows.len(), expected_rows);
        });
    });
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
    let runtime: Arc<dyn lix_engine::WasmRuntime> = if with_plugin {
        Arc::new(BenchJsonPluginRuntime)
    } else {
        Arc::new(NoopWasmRuntime)
    };
    let engine = boot(BootArgs::new(backend, runtime));
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

        let plugin_archive = build_test_plugin_archive()?;
        engine.install_plugin(&plugin_archive).await?;

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

async fn seed_engine_with_fast_id_repro() -> Result<lix_engine::Engine, LixError> {
    let engine = seed_engine(false).await?;

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text(FAST_ID_REPRO_FILE_ID.to_string()),
                Value::Text("/bench/fast-id-repro/seed-000000.txt".to_string()),
                Value::Blob(vec![0]),
            ],
            ExecuteOptions::default(),
        )
        .await?;

    for seq in 0..FAST_ID_REPRO_SEED_UPDATES {
        let path = format!("/bench/fast-id-repro/seed-{seq:06}.txt");
        let data = vec![
            (seq % 251) as u8,
            ((seq + 1) % 251) as u8,
            ((seq + 2) % 251) as u8,
        ];
        engine
            .execute(
                "UPDATE lix_file SET path = ?, data = ? WHERE id = ?",
                &[
                    Value::Text(path),
                    Value::Blob(data),
                    Value::Text(FAST_ID_REPRO_FILE_ID.to_string()),
                ],
                ExecuteOptions::default(),
            )
            .await?;
    }

    Ok(engine)
}

async fn seed_engine_with_read_dataset(
    with_plugin: bool,
    file_count: usize,
) -> Result<lix_engine::Engine, LixError> {
    let engine = seed_engine(with_plugin).await?;

    for index in 0..file_count {
        let file_id = format!("bench-read-file-{index:05}");
        let path = read_dataset_path(index, with_plugin);
        let data = if with_plugin {
            json_bytes(index as u64, JSON_LEAF_COUNT)
        } else {
            vec![
                (index % 251) as u8,
                ((index / 3) % 251) as u8,
                ((index / 7) % 251) as u8,
            ]
        };

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[Value::Text(file_id), Value::Text(path), Value::Blob(data)],
                ExecuteOptions::default(),
            )
            .await?;
    }

    Ok(engine)
}

fn read_dataset_path(index: usize, plugin_active: bool) -> String {
    let extension = if plugin_active { "json" } else { "txt" };
    format!("/bench/read/{:02}/file-{index:05}.{extension}", index % 32)
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
    bench_lix_file_insert_plugin_json,
    bench_lix_file_exact_delete_missing_ids,
    bench_lix_file_exact_update_missing_id,
    bench_lix_file_update_fast_id_repro,
    bench_lix_file_read_scan_no_plugin,
    bench_lix_file_read_scan_plugin_json,
    bench_lix_file_read_point_path_no_plugin,
    bench_lix_file_read_point_path_plugin_json
);
criterion_main!(benches);
