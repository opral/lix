use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{Engine, SessionContext};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "../storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../../plugin-json-v2/schema/json_pointer.json");
const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const SMOKE_ROWS: usize = 1_000;
const SCALE_ROWS: usize = 10_000;
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

struct RawSqliteFixture {
    conn: Connection,
    _dir: TempDir,
}

struct LixFixture {
    session: SessionContext,
}

fn json_pointer_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for json_pointer CRUD benchmarks");
    let rows = fixture_rows();

    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, SCALE_ROWS, "scale");
    bench_lix(c, &runtime, &rows, SCALE_ROWS, "scale");
}

fn bench_raw_sqlite(c: &mut Criterion, all_rows: &[PointerRow], row_count: usize, label: &str) {
    let rows = all_rows[..row_count].to_vec();
    let mut group = c.benchmark_group(format!("json_pointer_crud/raw_sqlite/{label}"));
    group.sample_size(if row_count <= SMOKE_ROWS { 20 } else { 11 });
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function(format!("insert_all_nodes/{}", row_label(row_count)), |b| {
        b.iter_batched(
            prepare_raw_sqlite_empty,
            |fixture| black_box(raw_sqlite_insert_all(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(
        format!("select_all_path_value/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_select_all(fixture, row_count)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(format!("select_by_pk_path/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_by_pk(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("update_all_values/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("update_by_pk_path/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_by_pk(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("delete_all_nodes/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("delete_by_pk_path/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_by_pk(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_lix(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
        let mut group = c.benchmark_group(format!("json_pointer_crud/{}/{label}", profile.name()));
        group.sample_size(if row_count <= SMOKE_ROWS { 11 } else { 11 });
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_secs(1));

        group.bench_function(format!("insert_all_nodes/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_empty(profile)),
                |fixture| black_box(runtime.block_on(lix_insert_all(fixture, &rows))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!("select_all_path_value/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                    |fixture| black_box(runtime.block_on(lix_select_all(fixture, row_count))),
                    BatchSize::LargeInput,
                )
            },
        );

        if row_count <= SMOKE_ROWS {
            group.bench_function(format!("select_by_pk_path/{}", row_label(row_count)), |b| {
                b.iter_batched(
                    || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                    |fixture| black_box(runtime.block_on(lix_select_by_pk(fixture, &rows))),
                    BatchSize::LargeInput,
                )
            });
        }

        group.bench_function(format!("update_all_values/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| black_box(runtime.block_on(lix_update_all(fixture, row_count))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("delete_all_nodes/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| black_box(runtime.block_on(lix_delete_all(fixture, row_count))),
                BatchSize::LargeInput,
            )
        });

        group.finish();
    }
}

fn prepare_raw_sqlite_empty() -> RawSqliteFixture {
    let dir = TempDir::new().expect("create raw sqlite tempdir");
    let conn = Connection::open(dir.path().join("json-pointer-crud.sqlite"))
        .expect("open raw sqlite json_pointer CRUD db");
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA foreign_keys = ON;
        CREATE TABLE json_pointer (
            path TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;
        ",
    )
    .expect("configure raw sqlite json_pointer CRUD db");
    RawSqliteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[PointerRow]) -> RawSqliteFixture {
    let fixture = prepare_raw_sqlite_empty();
    raw_sqlite_seed(&fixture.conn, rows);
    fixture
}

fn raw_sqlite_seed(conn: &Connection, rows: &[PointerRow]) {
    conn.execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite seed");
    {
        let mut statement = conn
            .prepare_cached(
                "INSERT INTO json_pointer (path, value) VALUES (?1, ?2)
                 ON CONFLICT(path) DO UPDATE SET value = excluded.value",
            )
            .expect("prepare raw sqlite seed insert");
        for row in rows {
            statement
                .execute(params![row.path.as_str(), row.value_json.as_str()])
                .expect("insert raw sqlite seed row");
        }
    }
    conn.execute_batch("COMMIT")
        .expect("commit raw sqlite seed");
}

fn raw_sqlite_insert_all(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    raw_sqlite_seed(&fixture.conn, rows);
    rows.len()
}

fn raw_sqlite_select_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT path, value FROM json_pointer ORDER BY path")
        .expect("prepare raw sqlite select all");
    let count = statement
        .query_map([], |_| Ok(()))
        .expect("raw sqlite select all")
        .count();
    assert_eq!(count, expected_rows);
    count
}

fn raw_sqlite_select_by_pk(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    fixture
        .conn
        .execute_batch("BEGIN DEFERRED")
        .expect("begin raw sqlite select by pk");
    let mut count = 0;
    {
        let mut statement = fixture
            .conn
            .prepare_cached("SELECT path, value FROM json_pointer WHERE path = ?1")
            .expect("prepare raw sqlite select by pk");
        for row in rows {
            if statement
                .query_row(params![row.path.as_str()], |_| Ok(()))
                .optional()
                .expect("raw sqlite select by pk")
                .is_some()
            {
                count += 1;
            }
        }
    }
    fixture
        .conn
        .execute_batch("COMMIT")
        .expect("commit raw sqlite select by pk");
    assert_eq!(count, rows.len());
    count
}

fn raw_sqlite_update_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute(
            "UPDATE json_pointer SET value = ?1",
            params![r#"{"updated":true}"#],
        )
        .expect("raw sqlite update all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_update_by_pk(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    fixture
        .conn
        .execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite update by pk");
    let mut affected = 0;
    {
        let mut statement = fixture
            .conn
            .prepare_cached("UPDATE json_pointer SET value = ?1 WHERE path = ?2")
            .expect("prepare raw sqlite update by pk");
        for row in rows {
            affected += statement
                .execute(params![row.updated_value_json.as_str(), row.path.as_str()])
                .expect("raw sqlite update by pk");
        }
    }
    fixture
        .conn
        .execute_batch("COMMIT")
        .expect("commit raw sqlite update by pk");
    assert_eq!(affected, rows.len());
    affected
}

fn raw_sqlite_delete_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute("DELETE FROM json_pointer", [])
        .expect("raw sqlite delete all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_delete_by_pk(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    fixture
        .conn
        .execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite delete by pk");
    let mut affected = 0;
    {
        let mut statement = fixture
            .conn
            .prepare_cached("DELETE FROM json_pointer WHERE path = ?1")
            .expect("prepare raw sqlite delete by pk");
        for row in rows {
            affected += statement
                .execute(params![row.path.as_str()])
                .expect("raw sqlite delete by pk");
        }
    }
    fixture
        .conn
        .execute_batch("COMMIT")
        .expect("commit raw sqlite delete by pk");
    assert_eq!(affected, rows.len());
    affected
}

async fn prepare_lix_empty(profile: LixBackendProfile) -> LixFixture {
    let engine = match profile {
        LixBackendProfile::Sqlite => {
            let backend =
                SqliteBenchBackend::tempfile().expect("create sqlite json_pointer CRUD backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize sqlite json_pointer CRUD Lix backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open sqlite json_pointer CRUD Lix engine")
        }
        LixBackendProfile::RocksDb => {
            let backend =
                RocksDbBenchBackend::new().expect("create rocksdb json_pointer CRUD backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize rocksdb json_pointer CRUD Lix backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open rocksdb json_pointer CRUD Lix engine")
        }
    };
    let setup_session = engine
        .open_workspace_session()
        .await
        .expect("open json_pointer CRUD Lix setup workspace session");
    register_json_pointer_schema(&setup_session).await;
    let session = engine
        .open_workspace_session()
        .await
        .expect("open json_pointer CRUD Lix benchmark workspace session");
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

async fn lix_insert_all(fixture: LixFixture, rows: &[PointerRow]) -> usize {
    insert_lix_rows(&fixture.session, rows).await;
    rows.len()
}

async fn insert_lix_rows(session: &SessionContext, rows: &[PointerRow]) {
    for chunk in rows.chunks(CHUNK_SIZE) {
        let mut sql = String::from("INSERT INTO json_pointer (path, value) VALUES ");
        for (index, row) in chunk.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "('{}', lix_json('{}'))",
                sql_string(row.path.as_str()),
                sql_string(row.value_json.as_str())
            ));
        }
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("insert json_pointer rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

async fn lix_select_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let result = fixture
        .session
        .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
        .await
        .expect("select all json_pointer rows");
    assert_eq!(result.len(), expected_rows);
    result.len()
}

async fn lix_select_by_pk(fixture: LixFixture, rows: &[PointerRow]) -> usize {
    let mut count = 0;
    for row in rows {
        let sql = format!(
            "SELECT path, value FROM json_pointer WHERE path = '{}'",
            sql_string(row.path.as_str())
        );
        let result = fixture
            .session
            .execute(&sql, &[])
            .await
            .expect("select json_pointer row by path");
        assert!(result.len() <= 1);
        count += result.len();
    }
    assert_eq!(count, rows.len());
    count
}

async fn lix_update_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .session
        .execute(
            r#"UPDATE json_pointer SET value = lix_json('{"updated":true}')"#,
            &[],
        )
        .await
        .expect("update all json_pointer rows")
        .rows_affected() as usize;
    assert_eq!(affected, expected_rows);
    affected
}

async fn lix_delete_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .session
        .execute("DELETE FROM json_pointer", &[])
        .await
        .expect("delete all json_pointer rows")
        .rows_affected() as usize;
    assert_eq!(affected, expected_rows);
    affected
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    assert!(
        rows.len() >= SCALE_ROWS,
        "pnpm lock fixture should have at least {SCALE_ROWS} pointer rows, got {}",
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

fn row_label(rows: usize) -> String {
    if rows >= 1_000 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}

criterion_group!(benches, json_pointer_crud_benches);
criterion_main!(benches);
