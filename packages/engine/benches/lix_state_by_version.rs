use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde_json::json;
use std::hint::black_box;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;

const SCHEMA_KEY: &str = "bench_state_by_version_schema";
const FILE_ID: &str = "bench-state-by-version-file";
const VERSION_COUNT: usize = 24;
const ROWS_PER_VERSION: usize = 256;
const TARGET_VERSION_INDEX: usize = VERSION_COUNT - 1;

fn bench_lix_state_by_version_count_eq_version(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_versions())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(version_id_at(TARGET_VERSION_INDEX)),
    ];

    c.bench_function("lix_state_by_version_count_eq_version", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.execute(
                    "SELECT COUNT(*) \
                     FROM lix_state_by_version \
                     WHERE schema_key = ? \
                       AND version_id = ? \
                       AND snapshot_content IS NOT NULL",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("eq version count should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_state_by_version_count_in_version_list(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_versions())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(version_id_at(TARGET_VERSION_INDEX)),
        Value::Text(version_id_at(TARGET_VERSION_INDEX - 1)),
    ];

    c.bench_function("lix_state_by_version_count_in_version_list", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.execute(
                    "SELECT COUNT(*) \
                     FROM lix_state_by_version \
                     WHERE schema_key = ? \
                       AND version_id IN (?, ?) \
                       AND snapshot_content IS NOT NULL",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("IN list version count should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_state_by_version_count_active_scope_subquery(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_versions())
        .expect("failed to seed benchmark engine");
    let params = vec![Value::Text(SCHEMA_KEY.to_string())];

    c.bench_function("lix_state_by_version_count_active_scope_subquery", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.execute(
                    "SELECT COUNT(*) \
                     FROM lix_state_by_version \
                     WHERE schema_key = ? \
                       AND version_id IN (\
                           SELECT lix_json_text(snapshot_content, 'version_id') \
                           FROM lix_internal_state_untracked \
                           WHERE schema_key = 'lix_version_pointer' \
                             AND file_id = 'lix' \
                             AND version_id = 'global' \
                             AND snapshot_content IS NOT NULL\
                       ) \
                       AND snapshot_content IS NOT NULL",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("active-scope version count should succeed");
            black_box(result.rows.len());
        });
    });
}

async fn seed_engine_with_versions() -> Result<lix_engine::Engine, LixError> {
    let backend = Box::new(BenchSqliteBackend::in_memory());
    let engine = boot(BootArgs::new(backend));
    engine.init().await?;

    insert_stored_schema(&engine).await?;
    insert_versions(&engine).await?;
    insert_versioned_state_rows(&engine).await?;

    let target_version = version_id_at(TARGET_VERSION_INDEX);
    engine
        .execute(
            "UPDATE lix_active_version SET version_id = ?",
            &[Value::Text(target_version)],
            ExecuteOptions::default(),
        )
        .await?;

    Ok(engine)
}

async fn insert_stored_schema(engine: &lix_engine::Engine) -> Result<(), LixError> {
    let schema_snapshot = json!({
        "value": {
            "x-lix-key": SCHEMA_KEY,
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "integer" },
                "version": { "type": "string" }
            },
            "required": ["value", "version"],
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

async fn insert_versions(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for index in 0..VERSION_COUNT {
        let version_id = version_id_at(index);
        let parent = if index == 0 {
            "global".to_string()
        } else {
            version_id_at(index - 1)
        };

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (?, ?, ?, 0, ?, ?)",
                &[
                    Value::Text(version_id.clone()),
                    Value::Text(version_id.clone()),
                    Value::Text(parent),
                    Value::Text(format!("commit-{version_id}")),
                    Value::Text(format!("working-{version_id}")),
                ],
                ExecuteOptions::default(),
            )
            .await?;
    }
    Ok(())
}

async fn insert_versioned_state_rows(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for version_index in 0..VERSION_COUNT {
        let version_id = version_id_at(version_index);
        for row_index in 0..ROWS_PER_VERSION {
            let entity_id = format!("bench-entity-{row_index:05}");
            let snapshot = json!({
                "value": row_index as i64,
                "version": version_id,
            })
            .to_string();

            engine
                .execute(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (?, ?, ?, ?, 'lix', ?, '1')",
                    &[
                        Value::Text(entity_id),
                        Value::Text(SCHEMA_KEY.to_string()),
                        Value::Text(FILE_ID.to_string()),
                        Value::Text(version_id.clone()),
                        Value::Text(snapshot),
                    ],
                    ExecuteOptions::default(),
                )
                .await?;
        }
    }
    Ok(())
}

fn version_id_at(index: usize) -> String {
    format!("bench-v-{index:03}")
}

criterion_group!(
    benches,
    bench_lix_state_by_version_count_eq_version,
    bench_lix_state_by_version_count_in_version_list,
    bench_lix_state_by_version_count_active_scope_subquery
);
criterion_main!(benches);
