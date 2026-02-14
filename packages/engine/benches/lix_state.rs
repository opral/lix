use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde_json::json;
use std::hint::black_box;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;

const FILE_ID: &str = "bench-file";
const PLUGIN_KEY: &str = "plugin_json";
const SCHEMA_KEY: &str = "bench_state_schema";
const ROW_COUNT: usize = 2_000;

fn bench_lix_state_count_no_inheritance(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(ActiveVersionMode::Global))
        .expect("failed to seed benchmark engine");

    run_count_bench(
        c,
        &runtime,
        &engine,
        "lix_state_count_no_inheritance",
        "SELECT COUNT(*) FROM lix_state WHERE file_id = ? AND plugin_key = ?",
    );
}

fn bench_lix_state_count_inherited(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let engine = runtime
        .block_on(seed_engine(ActiveVersionMode::Child))
        .expect("failed to seed benchmark engine");

    run_count_bench(
        c,
        &runtime,
        &engine,
        "lix_state_count_inherited",
        "SELECT COUNT(*) FROM lix_state WHERE file_id = ? AND plugin_key = ?",
    );
}

fn run_count_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    bench_name: &str,
    sql: &str,
) {
    let params = vec![
        Value::Text(FILE_ID.to_string()),
        Value::Text(PLUGIN_KEY.to_string()),
    ];

    c.bench_function(bench_name, |b| {
        b.iter(|| {
            let result = runtime
                .block_on(engine.execute(sql, &params, ExecuteOptions::default()))
                .expect("count query should succeed");

            let value = result
                .rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .expect("count query should return one scalar");
            black_box(value);
        });
    });
}

#[derive(Clone, Copy)]
enum ActiveVersionMode {
    Global,
    Child,
}

async fn seed_engine(
    active_version_mode: ActiveVersionMode,
) -> Result<lix_engine::Engine, LixError> {
    let backend = Box::new(BenchSqliteBackend::in_memory());
    let engine = boot(BootArgs::new(backend));
    engine.init().await?;

    let schema_snapshot = json!({
        "value": {
            "x-lix-key": SCHEMA_KEY,
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "integer" }
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

    for idx in 0..ROW_COUNT {
        let snapshot = json!({ "value": idx as i64 }).to_string();
        let entity_id = format!("entity-{idx}");
        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (?, ?, ?, 'global', ?, ?, '1')",
                &[
                    Value::Text(entity_id),
                    Value::Text(SCHEMA_KEY.to_string()),
                    Value::Text(FILE_ID.to_string()),
                    Value::Text(PLUGIN_KEY.to_string()),
                    Value::Text(snapshot),
                ],
                ExecuteOptions::default(),
            )
            .await?;
    }

    if matches!(active_version_mode, ActiveVersionMode::Child) {
        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'bench-child', 'bench-child', 'global', 0, 'commit-bench-child', 'working-bench-child'\
                 )",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'bench-child'",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
    }

    Ok(engine)
}

criterion_group!(
    benches,
    bench_lix_state_count_no_inheritance,
    bench_lix_state_count_inherited
);
criterion_main!(benches);
