#![cfg(feature = "storage-benches")]

use std::fs;
use std::path::Path;

use lix_engine::{Engine, SessionContext};
use rusqlite::{params, Connection};
use serde_json::Value as JsonValue;
use tempfile::TempDir;

#[path = "../benches/storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../benches/storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../plugin-json-v2/schema/json_pointer.json");
const PNPM_LOCK_JSON: &str = include_str!("../benches/fixtures/pnpm-lock.fixture.json");
const ROW_COUNTS: [usize; 1] = [1_000];
const CHUNK_SIZE: usize = 500;

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
}

#[tokio::test]
#[ignore = "prints JSON pointer CRUD storage-size reference rows"]
async fn json_pointer_crud_storage_accounting() {
    let rows = fixture_rows();
    println!("| backend | rows | bytes on disk | bytes/row |");
    println!("| ------- | ---: | ------------: | --------: |");
    for row_count in ROW_COUNTS {
        let rows = &rows[..row_count];
        print_storage_row("raw SQLite", row_count, raw_sqlite_storage_bytes(rows));
        print_storage_row(
            "Lix SQLite",
            row_count,
            lix_sqlite_storage_bytes(rows).await,
        );
        print_storage_row(
            "Lix RocksDB",
            row_count,
            lix_rocksdb_storage_bytes(rows).await,
        );
    }
}

fn print_storage_row(backend: &str, rows: usize, bytes: u64) {
    println!(
        "| {backend} | {rows} | {bytes} | {:.1} |",
        bytes as f64 / rows as f64
    );
}

fn raw_sqlite_storage_bytes(rows: &[PointerRow]) -> u64 {
    let dir = TempDir::new().expect("create raw sqlite storage tempdir");
    let db_path = dir.path().join("json-pointer-crud.sqlite");
    let conn = Connection::open(&db_path).expect("open raw sqlite storage db");
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
    .expect("configure raw sqlite storage db");
    {
        let tx = conn
            .unchecked_transaction()
            .expect("begin raw sqlite storage transaction");
        {
            let mut statement = tx
                .prepare_cached("INSERT INTO json_pointer (path, value) VALUES (?1, ?2)")
                .expect("prepare raw sqlite storage insert");
            for row in rows {
                statement
                    .execute(params![row.path.as_str(), row.value_json.as_str()])
                    .expect("insert raw sqlite storage row");
            }
        }
        tx.commit().expect("commit raw sqlite storage transaction");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(FULL)")
        .expect("checkpoint raw sqlite storage db");
    directory_size(dir.path())
}

async fn lix_sqlite_storage_bytes(rows: &[PointerRow]) -> u64 {
    let backend = SqliteBenchBackend::tempfile().expect("create sqlite storage backend");
    let dir = backend
        .path()
        .and_then(Path::parent)
        .expect("sqlite backend should expose tempfile parent")
        .to_path_buf();
    let engine = initialize_engine(Box::new(backend.clone()), Box::new(backend)).await;
    let session = prepare_session(&engine).await;
    insert_lix_rows(&session, rows).await;
    directory_size(&dir)
}

async fn lix_rocksdb_storage_bytes(rows: &[PointerRow]) -> u64 {
    let backend = RocksDbBenchBackend::new().expect("create rocksdb storage backend");
    let dir = backend.path().to_path_buf();
    let engine = initialize_engine(Box::new(backend.clone()), Box::new(backend)).await;
    let session = prepare_session(&engine).await;
    insert_lix_rows(&session, rows).await;
    directory_size(&dir)
}

async fn initialize_engine(
    initializer_backend: Box<dyn lix_engine::Backend + Send + Sync>,
    engine_backend: Box<dyn lix_engine::Backend + Send + Sync>,
) -> Engine {
    Engine::initialize(initializer_backend)
        .await
        .expect("initialize storage benchmark engine");
    Engine::new(engine_backend)
        .await
        .expect("open storage benchmark engine")
}

async fn prepare_session(engine: &Engine) -> SessionContext {
    let session = engine
        .open_workspace_session()
        .await
        .expect("open json pointer storage workspace");
    register_json_pointer_schema(&session).await;
    session
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
        .expect("register json_pointer storage schema")
        .rows_affected();
    assert_eq!(affected, 1);
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
            .expect("insert json_pointer storage rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    assert!(rows.len() >= 10_000);
    rows
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    rows.push(PointerRow {
        path: path.to_string(),
        value_json: value.to_string(),
    });

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                flatten_json(&format!("{path}/{}", index), item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                flatten_json(
                    &format!("{path}/{}", key.replace('~', "~0").replace('/', "~1")),
                    child,
                    rows,
                );
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn directory_size(path: &Path) -> u64 {
    let metadata = fs::metadata(path).expect("read storage path metadata");
    if metadata.is_file() {
        return metadata.len();
    }

    let mut bytes = 0;
    for entry in fs::read_dir(path).expect("read storage directory") {
        let entry = entry.expect("read storage directory entry");
        bytes += directory_size(&entry.path());
    }
    bytes
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
