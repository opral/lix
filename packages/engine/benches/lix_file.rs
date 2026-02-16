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
const HISTORY_FILE_ID: &str = "bench-history-file";
const HISTORY_SEED_UPDATES: usize = 1_500;
const READ_SCAN_FILE_COUNT_NO_PLUGIN: usize = 512;
const READ_POINT_FILE_COUNT_NO_PLUGIN: usize = 512;
const READ_SCAN_FILE_COUNT_PLUGIN: usize = 16;
const READ_POINT_FILE_COUNT_PLUGIN: usize = 16;
const READ_POINT_TARGET_INDEX: usize = 7;

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
        .block_on(seed_engine_with_history())
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
        .block_on(seed_engine_with_history())
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

fn bench_lix_file_read_scan_no_plugin(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine_with_read_dataset(false, READ_SCAN_FILE_COUNT_NO_PLUGIN))
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
        .block_on(seed_engine_with_read_dataset(true, READ_SCAN_FILE_COUNT_PLUGIN))
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

async fn seed_engine_with_history() -> Result<lix_engine::Engine, LixError> {
    let engine = seed_engine(false).await?;

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text(HISTORY_FILE_ID.to_string()),
                Value::Text("/bench/history-seed-0.txt".to_string()),
                Value::Blob(vec![0]),
            ],
            ExecuteOptions::default(),
        )
        .await?;

    for seq in 0..HISTORY_SEED_UPDATES {
        let path = format!("/bench/history-seed-{}.txt", seq % 32);
        let data = vec![(seq % 251) as u8, (seq % 127) as u8, (seq % 63) as u8];
        let data_hex = bytes_to_hex(&data);
        let sql = format!(
            "UPDATE lix_file SET path = '{}', data = x'{}' WHERE id = '{}'",
            escape_sql_literal(&path),
            data_hex,
            escape_sql_literal(HISTORY_FILE_ID),
        );
        engine.execute(&sql, &[], ExecuteOptions::default()).await?;
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
    format!(
        "/bench/read/{:02}/file-{index:05}.{extension}",
        index % 32
    )
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

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

criterion_group!(
    benches,
    bench_lix_file_insert_no_plugin,
    bench_lix_file_insert_plugin_json,
    bench_lix_file_exact_delete_missing_ids,
    bench_lix_file_exact_update_missing_id,
    bench_lix_file_read_scan_no_plugin,
    bench_lix_file_read_scan_plugin_json,
    bench_lix_file_read_point_path_no_plugin,
    bench_lix_file_read_point_path_plugin_json
);
criterion_main!(benches);
