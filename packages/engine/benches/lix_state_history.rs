use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, NoopWasmRuntime, Value};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;

const SCHEMA_KEY: &str = "bench_state_history_schema";
const FILE_ID: &str = "bench-state-history-file";
const ENTITY_COUNT: usize = 8;
const HISTORY_UPDATES: usize = 48;
const TARGET_ENTITY_INDEX: usize = 3;

fn bench_lix_state_history_count_by_root_commit(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
    ];

    c.bench_function("lix_state_history_count_by_root_commit", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    "SELECT COUNT(*) \
                     FROM lix_state_history \
                     WHERE schema_key = ? \
                       AND root_commit_id = ? \
                       AND snapshot_content IS NOT NULL",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("history count should succeed");
            let scalar = result
                .rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .expect("history count should return one scalar");
            black_box(scalar);
        });
    });
}

fn bench_lix_state_history_entity_timeline_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(seed.target_entity_id.clone()),
        Value::Text(seed.root_commit_id.clone()),
    ];

    c.bench_function("lix_state_history_entity_timeline_scan", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    "SELECT depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE schema_key = ? \
                       AND entity_id = ? \
                       AND root_commit_id = ? \
                     ORDER BY depth ASC",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("history timeline scan should succeed");
            black_box(result.rows.len());
        });
    });
}

struct HistorySeed {
    engine: lix_engine::Engine,
    root_commit_id: String,
    target_entity_id: String,
}

async fn seed_engine_with_history() -> Result<HistorySeed, LixError> {
    let backend = Box::new(BenchSqliteBackend::in_memory());
    let engine = boot(BootArgs::new(backend, Arc::new(NoopWasmRuntime)));
    engine.init().await?;

    insert_stored_schema(&engine).await?;
    insert_initial_rows(&engine).await?;
    apply_history_updates(&engine).await?;

    let root_commit_id = load_active_commit_id(&engine).await?;
    let target_entity_id = entity_id_at(TARGET_ENTITY_INDEX);

    Ok(HistorySeed {
        engine,
        root_commit_id,
        target_entity_id,
    })
}

async fn insert_stored_schema(engine: &lix_engine::Engine) -> Result<(), LixError> {
    let schema_snapshot = json!({
        "value": {
            "x-lix-key": SCHEMA_KEY,
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        }
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (?, 'lix_stored_schema', 'lix', 'global', 'lix', ?, '1')",
            &[
                Value::Text(format!("{SCHEMA_KEY}~1")),
                Value::Text(schema_snapshot),
            ],
            ExecuteOptions::default(),
        )
        .await?;
    Ok(())
}

async fn insert_initial_rows(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for index in 0..ENTITY_COUNT {
        let entity_id = entity_id_at(index);
        let snapshot = json!({ "value": format!("seed-{index:05}") }).to_string();
        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (?, ?, ?, 'lix', '1', ?)",
                &[
                    Value::Text(entity_id),
                    Value::Text(SCHEMA_KEY.to_string()),
                    Value::Text(FILE_ID.to_string()),
                    Value::Text(snapshot),
                ],
                ExecuteOptions::default(),
            )
            .await?;
    }
    Ok(())
}

async fn apply_history_updates(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for sequence in 0..HISTORY_UPDATES {
        let entity_index = sequence % ENTITY_COUNT;
        let entity_id = entity_id_at(entity_index);
        let snapshot = json!({
            "value": format!("update-{sequence:06}")
        })
        .to_string();

        let sql = format!(
            "UPDATE lix_state \
             SET snapshot_content = '{}' \
             WHERE entity_id = '{}' \
               AND schema_key = '{}' \
               AND file_id = '{}'",
            escape_sql_literal(&snapshot),
            escape_sql_literal(&entity_id),
            escape_sql_literal(SCHEMA_KEY),
            escape_sql_literal(FILE_ID),
        );
        engine.execute(&sql, &[], ExecuteOptions::default()).await?;
    }
    Ok(())
}

async fn load_active_commit_id(engine: &lix_engine::Engine) -> Result<String, LixError> {
    let result = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             LIMIT 1",
            &[],
            ExecuteOptions::default(),
        )
        .await?;
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            message: "active commit query returned no rows".to_string(),
        })?;
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!("active commit_id must be text, got {other:?}"),
        }),
    }
}

fn entity_id_at(index: usize) -> String {
    format!("bench-history-entity-{index:04}")
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

criterion_group!(
    benches,
    bench_lix_state_history_count_by_root_commit,
    bench_lix_state_history_entity_timeline_scan
);
criterion_main!(benches);
