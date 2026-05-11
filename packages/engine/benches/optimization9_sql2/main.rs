use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{optimization9_sql2_bench, Engine, SessionContext, Value};
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;

#[path = "../storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const JSON_POINTER_SCHEMA_JSON: &str = include_str!("json_pointer.schema.json");
const PNPM_LOCK_JSON: &str = include_str!("pnpm-lock.fixture.json");
const ROW_COUNT: usize = 1_000;
const INSERT_ROWS: usize = 500;
const CHUNK_SIZE: usize = 500;

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
    updated_value_json: String,
}

#[derive(Clone, Copy)]
enum LixBackendProfile {
    Sqlite,
    RocksDb,
}

impl LixBackendProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
        }
    }
}

struct LixFixture {
    session: SessionContext,
}

fn optimization9_sql2_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for optimization9 sql2 benchmarks");
    let rows = fixture_rows();

    for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
        bench_smoke_crud(c, &runtime, profile, &rows);
        bench_planning_only(c, &runtime, profile, &rows);
        bench_execute_preplanned(c, &runtime, profile, &rows);
        bench_e2e_literal(c, &runtime, profile, &rows);
        bench_e2e_parameterized(c, &runtime, profile, &rows);
    }
}

fn bench_smoke_crud(
    c: &mut Criterion,
    runtime: &Runtime,
    profile: LixBackendProfile,
    all_rows: &[PointerRow],
) {
    let rows = all_rows[..ROW_COUNT].to_vec();
    let mut group = c.benchmark_group(format!("optimization9_sql2/smoke_crud/{}", profile.name()));
    configure_group(&mut group);

    group.bench_function("insert_all_rows/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_empty(profile)),
            |fixture| {
                insert_lix_rows_blocking(runtime, &fixture.session, &rows);
                black_box(rows.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_all_path_value/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let result = runtime
                    .block_on(
                        fixture
                            .session
                            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[]),
                    )
                    .expect("smoke select all");
                assert_eq!(result.len(), ROW_COUNT);
                black_box(result.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = select_one_literal_sql(pick_pk_row(&rows));
                let result = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("smoke select one");
                assert_eq!(result.len(), 1);
                black_box(result.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_all_values/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let affected = runtime
                    .block_on(fixture.session.execute(
                        r#"UPDATE json_pointer SET value = lix_json('{"updated":true}')"#,
                        &[],
                    ))
                    .expect("smoke update all")
                    .rows_affected();
                assert_eq!(affected as usize, ROW_COUNT);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = update_one_literal_sql(pick_pk_row(&rows));
                let affected = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("smoke update one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("delete_all_rows/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let affected = runtime
                    .block_on(fixture.session.execute("DELETE FROM json_pointer", &[]))
                    .expect("smoke delete all")
                    .rows_affected();
                assert_eq!(affected as usize, ROW_COUNT);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("delete_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = delete_one_literal_sql(pick_pk_row(&rows));
                let affected = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("smoke delete one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_planning_only(
    c: &mut Criterion,
    runtime: &Runtime,
    profile: LixBackendProfile,
    all_rows: &[PointerRow],
) {
    let rows = all_rows[..ROW_COUNT].to_vec();
    let mut group = c.benchmark_group(format!(
        "optimization9_sql2/planning_only/{}",
        profile.name()
    ));
    configure_group(&mut group);

    group.bench_function("select_all_path_value/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                black_box(runtime.block_on(optimization9_sql2_bench::plan_read_only(
                    &fixture.session,
                    "SELECT path, value FROM json_pointer ORDER BY path",
                )))
                .expect("plan select all")
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = select_one_literal_sql(pick_pk_row(&rows));
                black_box(runtime.block_on(optimization9_sql2_bench::plan_read_only(
                    &fixture.session,
                    &sql,
                )))
                .expect("plan select one")
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("insert_500_values/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_empty(profile)),
            |fixture| {
                let sql = insert_literal_sql(&rows[..INSERT_ROWS]);
                black_box(runtime.block_on(optimization9_sql2_bench::plan_write_only(
                    &fixture.session,
                    &sql,
                )))
                .expect("plan insert")
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_all_values/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                black_box(runtime.block_on(optimization9_sql2_bench::plan_write_only(
                    &fixture.session,
                    r#"UPDATE json_pointer SET value = lix_json('{"updated":true}')"#,
                )))
                .expect("plan update all")
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("delete_all_rows/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                black_box(runtime.block_on(optimization9_sql2_bench::plan_write_only(
                    &fixture.session,
                    "DELETE FROM json_pointer",
                )))
                .expect("plan delete all")
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_execute_preplanned(
    c: &mut Criterion,
    runtime: &Runtime,
    profile: LixBackendProfile,
    all_rows: &[PointerRow],
) {
    let rows = all_rows[..ROW_COUNT].to_vec();
    let mut group = c.benchmark_group(format!(
        "optimization9_sql2/execute_preplanned/{}",
        profile.name()
    ));
    configure_group(&mut group);

    group.bench_function("select_all_path_value/1k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime.block_on(prepare_lix_seeded(profile, &rows));
                runtime
                    .block_on(optimization9_sql2_bench::prepare_read_plan(
                        &fixture.session,
                        "SELECT path, value FROM json_pointer ORDER BY path",
                    ))
                    .expect("prepare select all plan")
            },
            |plan| {
                let result = runtime
                    .block_on(optimization9_sql2_bench::execute_read_plan(plan, &[]))
                    .expect("execute select all plan");
                assert_eq!(result.rows.len(), ROW_COUNT);
                black_box(result.rows.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_one_by_pk/1k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime.block_on(prepare_lix_seeded(profile, &rows));
                let sql = select_one_parameterized_sql();
                runtime
                    .block_on(optimization9_sql2_bench::prepare_read_plan(
                        &fixture.session,
                        sql,
                    ))
                    .expect("prepare select one plan")
            },
            |plan| {
                let params = vec![Value::Text(pick_pk_row(&rows).path.clone())];
                let result = runtime
                    .block_on(optimization9_sql2_bench::execute_read_plan(plan, &params))
                    .expect("execute select one plan");
                assert_eq!(result.rows.len(), 1);
                black_box(result.rows.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_e2e_literal(
    c: &mut Criterion,
    runtime: &Runtime,
    profile: LixBackendProfile,
    all_rows: &[PointerRow],
) {
    let rows = all_rows[..ROW_COUNT].to_vec();
    let mut group = c.benchmark_group(format!("optimization9_sql2/e2e_literal/{}", profile.name()));
    configure_group(&mut group);

    group.bench_function("select_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = select_one_literal_sql(pick_pk_row(&rows));
                let result = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("literal select one");
                assert_eq!(result.len(), 1);
                black_box(result.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = update_one_literal_sql(pick_pk_row(&rows));
                let affected = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("literal update one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("delete_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let sql = delete_one_literal_sql(pick_pk_row(&rows));
                let affected = runtime
                    .block_on(fixture.session.execute(&sql, &[]))
                    .expect("literal delete one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_e2e_parameterized(
    c: &mut Criterion,
    runtime: &Runtime,
    profile: LixBackendProfile,
    all_rows: &[PointerRow],
) {
    let rows = all_rows[..ROW_COUNT].to_vec();
    let mut group = c.benchmark_group(format!(
        "optimization9_sql2/e2e_parameterized/{}",
        profile.name()
    ));
    configure_group(&mut group);

    group.bench_function("select_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let row = pick_pk_row(&rows);
                let result = runtime
                    .block_on(fixture.session.execute(
                        select_one_parameterized_sql(),
                        &[Value::Text(row.path.clone())],
                    ))
                    .expect("parameterized select one");
                assert_eq!(result.len(), 1);
                black_box(result.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let row = pick_pk_row(&rows);
                let affected = runtime
                    .block_on(fixture.session.execute(
                        "UPDATE json_pointer SET value = lix_json($1) WHERE path = $2",
                        &[
                            Value::Text(row.updated_value_json.clone()),
                            Value::Text(row.path.clone()),
                        ],
                    ))
                    .expect("parameterized update one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("delete_one_by_pk/1k", |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, &rows)),
            |fixture| {
                let row = pick_pk_row(&rows);
                let affected = runtime
                    .block_on(fixture.session.execute(
                        "DELETE FROM json_pointer WHERE path = $1",
                        &[Value::Text(row.path.clone())],
                    ))
                    .expect("parameterized delete one")
                    .rows_affected();
                assert_eq!(affected, 1);
                black_box(affected)
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.sample_size(11);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
}

async fn prepare_lix_empty(profile: LixBackendProfile) -> LixFixture {
    let engine = match profile {
        LixBackendProfile::Sqlite => {
            let backend =
                SqliteBenchBackend::tempfile().expect("create sqlite optimization9 backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize sqlite optimization9 backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open sqlite optimization9 engine")
        }
        LixBackendProfile::RocksDb => {
            let backend = RocksDbBenchBackend::new().expect("create rocksdb optimization9 backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize rocksdb optimization9 backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open rocksdb optimization9 engine")
        }
    };
    let setup_session = engine
        .open_workspace_session()
        .await
        .expect("open optimization9 setup workspace session");
    register_json_pointer_schema(&setup_session).await;
    let session = engine
        .open_workspace_session()
        .await
        .expect("open optimization9 benchmark workspace session");
    LixFixture { session }
}

async fn prepare_lix_seeded(profile: LixBackendProfile, rows: &[PointerRow]) -> LixFixture {
    let fixture = prepare_lix_empty(profile).await;
    insert_lix_rows(&fixture.session, rows).await;
    fixture
}

async fn register_json_pointer_schema(session: &SessionContext) {
    let sql = format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
         VALUES (lix_json('{}'), false, false)",
        sql_string(JSON_POINTER_SCHEMA_JSON)
    );
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("register json_pointer schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn insert_lix_rows(session: &SessionContext, rows: &[PointerRow]) {
    for chunk in rows.chunks(CHUNK_SIZE) {
        let sql = insert_literal_sql(chunk);
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("insert json_pointer rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

fn insert_lix_rows_blocking(runtime: &Runtime, session: &SessionContext, rows: &[PointerRow]) {
    runtime.block_on(insert_lix_rows(session, rows));
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    assert!(
        rows.len() >= ROW_COUNT,
        "pnpm lock fixture should have at least {ROW_COUNT} pointer rows, got {}",
        rows.len()
    );
    rows
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    rows.push(PointerRow {
        path: path.to_string(),
        value_json: value.to_string(),
        updated_value_json: updated_value_for(path),
    });

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{path}/{}", index);
                flatten_json(&child_path, item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}/{}", escape_pointer_token(key));
                flatten_json(&child_path, child, rows);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn insert_literal_sql(rows: &[PointerRow]) -> String {
    let mut sql = String::from("INSERT INTO json_pointer (path, value) VALUES ");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "('{}', lix_json('{}'))",
            sql_string(row.path.as_str()),
            sql_string(row.value_json.as_str())
        ));
    }
    sql
}

fn select_one_literal_sql(row: &PointerRow) -> String {
    format!(
        "SELECT path, value FROM json_pointer WHERE path = '{}'",
        sql_string(row.path.as_str())
    )
}

fn select_one_parameterized_sql() -> &'static str {
    "SELECT path, value FROM json_pointer WHERE path = $1"
}

fn update_one_literal_sql(row: &PointerRow) -> String {
    format!(
        "UPDATE json_pointer SET value = lix_json('{}') WHERE path = '{}'",
        sql_string(row.updated_value_json.as_str()),
        sql_string(row.path.as_str())
    )
}

fn delete_one_literal_sql(row: &PointerRow) -> String {
    format!(
        "DELETE FROM json_pointer WHERE path = '{}'",
        sql_string(row.path.as_str())
    )
}

fn pick_pk_row(rows: &[PointerRow]) -> &PointerRow {
    &rows[rows.len() / 2]
}

fn updated_value_for(path: &str) -> String {
    serde_json::json!({
        "updated": true,
        "path": path,
    })
    .to_string()
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

criterion_group!(benches, optimization9_sql2_benches);
criterion_main!(benches);
