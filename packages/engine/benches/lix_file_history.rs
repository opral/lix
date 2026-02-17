use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use std::hint::black_box;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;

const FILE_COUNT: usize = 64;
const HISTORY_UPDATES: usize = 320;
const TARGET_FILE_INDEX: usize = 11;

fn bench_lix_file_history_count_by_root_commit(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_file_history())
        .expect("failed to seed benchmark engine");
    let params = vec![Value::Text(seed.root_commit_id.clone())];

    let warmup = runtime
        .block_on(seed.engine.execute(
            "SELECT COUNT(*) \
             FROM lix_file_history \
             WHERE lixcol_root_commit_id = ? \
               AND path IS NOT NULL",
            &params,
            ExecuteOptions::default(),
        ))
        .expect("file history count warmup should succeed");
    let expected_count =
        scalar_count(&warmup).expect("file history count warmup should return int");

    c.bench_function("lix_file_history_count_by_root_commit", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    "SELECT COUNT(*) \
                     FROM lix_file_history \
                     WHERE lixcol_root_commit_id = ? \
                       AND path IS NOT NULL",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("file history count should succeed");
            let count = scalar_count(&result).expect("file history count should return int");
            black_box(count);
            debug_assert_eq!(count, expected_count);
        });
    });
}

fn bench_lix_file_history_file_timeline_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_file_history())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(seed.target_file_id.clone()),
        Value::Text(seed.root_commit_id.clone()),
    ];

    let warmup = runtime
        .block_on(seed.engine.execute(
            "SELECT lixcol_depth, path, data \
             FROM lix_file_history \
             WHERE id = ? \
               AND lixcol_root_commit_id = ? \
             ORDER BY lixcol_depth ASC",
            &params,
            ExecuteOptions::default(),
        ))
        .expect("file history timeline warmup should succeed");
    let expected_rows = warmup.rows.len();

    c.bench_function("lix_file_history_file_timeline_scan", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    "SELECT lixcol_depth, path, data \
                     FROM lix_file_history \
                     WHERE id = ? \
                       AND lixcol_root_commit_id = ? \
                     ORDER BY lixcol_depth ASC",
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("file history timeline scan should succeed");
            black_box(result.rows.len());
            debug_assert_eq!(result.rows.len(), expected_rows);
        });
    });
}

struct HistorySeed {
    engine: lix_engine::Engine,
    root_commit_id: String,
    target_file_id: String,
}

async fn seed_engine_with_file_history() -> Result<HistorySeed, LixError> {
    let backend = Box::new(BenchSqliteBackend::in_memory());
    let engine = boot(BootArgs::new(backend));
    engine.init().await?;

    insert_seed_files(&engine).await?;
    apply_history_updates(&engine).await?;

    let root_commit_id = load_active_commit_id(&engine).await?;
    let target_file_id = file_id_at(TARGET_FILE_INDEX);

    Ok(HistorySeed {
        engine,
        root_commit_id,
        target_file_id,
    })
}

async fn insert_seed_files(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for index in 0..FILE_COUNT {
        let file_id = file_id_at(index);
        let path = file_path_at(index, 0);
        let data = file_data(index, 0);
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[Value::Text(file_id), Value::Text(path), Value::Blob(data)],
                ExecuteOptions::default(),
            )
            .await?;
    }
    Ok(())
}

async fn apply_history_updates(engine: &lix_engine::Engine) -> Result<(), LixError> {
    for sequence in 0..HISTORY_UPDATES {
        let file_index = sequence % FILE_COUNT;
        let version = sequence + 1;
        let file_id = file_id_at(file_index);
        let path = file_path_at(file_index, version);
        let data = file_data(file_index, version);

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = ?, data = ? \
                 WHERE id = ?",
                &[Value::Text(path), Value::Blob(data), Value::Text(file_id)],
                ExecuteOptions::default(),
            )
            .await?;
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

fn file_id_at(index: usize) -> String {
    format!("bench-history-file-{index:04}")
}

fn file_path_at(index: usize, version: usize) -> String {
    format!(
        "/bench/history/{:02}/file-{index:04}-v{version:05}.txt",
        (index + version) % 24
    )
}

fn file_data(index: usize, version: usize) -> Vec<u8> {
    vec![
        (index % 251) as u8,
        (version % 251) as u8,
        ((index + version) % 251) as u8,
        ((index * 3 + version * 5) % 251) as u8,
    ]
}

criterion_group!(
    benches,
    bench_lix_file_history_count_by_root_commit,
    bench_lix_file_history_file_timeline_scan
);
criterion_main!(benches);
