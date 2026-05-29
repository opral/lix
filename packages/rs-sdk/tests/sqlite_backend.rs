#![cfg(feature = "sqlite")]
use lix_engine::run_backend_conformance;
use lix_sdk::{
    SQLITE_FORMAT_VERSION, SqliteBackend, SqliteBackendFactory, Value, open_lix_with_backend,
};
use rusqlite::Connection;

#[test]
fn sqlite_backend_passes_backend_conformance() {
    let factory = SqliteBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn sqlite_backend_initializes_file_format_and_open_pragmas() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    let backend = SqliteBackend::open(&path).expect("sqlite backend opens");

    assert_eq!(
        backend
            .format_version()
            .expect("format version should read"),
        SQLITE_FORMAT_VERSION,
        "empty database should initialize to the current format version"
    );
    assert_eq!(
        sqlite_journal_mode(&path),
        "wal",
        "sqlite backend should use WAL journal mode"
    );
    assert_eq!(
        backend.busy_timeout_ms().expect("busy timeout should read"),
        5000,
        "sqlite backend should set a 5s busy timeout on opened connections"
    );

    drop(backend);
}

#[test]
fn sqlite_backend_refuses_future_file_format_version() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");
    let conn = Connection::open(&path).expect("sqlite file should create");
    conn.pragma_update(None, "user_version", 999)
        .expect("future user_version should write");
    drop(conn);

    let Err(error) = SqliteBackend::open(&path) else {
        panic!("future file format version should be refused");
    };

    assert!(
        error.to_string().contains("newer than supported version"),
        "error should explain future format version: {error}"
    );
}

#[tokio::test]
async fn sqlite_backend_persists_lix_data_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    {
        let lix = open_lix_with_backend(SqliteBackend::open(&path).expect("sqlite backend opens"))
            .await
            .expect("lix opens on sqlite backend");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('sqlite-key', 'sqlite-value')",
            &[],
        )
        .await
        .expect("write succeeds");
        lix.close().await.expect("lix closes");
    }

    let lix = open_lix_with_backend(SqliteBackend::open(&path).expect("sqlite backend reopens"))
        .await
        .expect("lix reopens on sqlite backend");
    let result = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'sqlite-key' AND value = lix_json('\"sqlite-value\"')",
            &[],
        )
        .await
        .expect("read succeeds");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("sqlite-key".to_string())]
    );
    lix.close().await.expect("lix closes");
}

fn sqlite_journal_mode(path: &std::path::Path) -> String {
    let conn = Connection::open(path).expect("sqlite file should open");
    conn.pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("journal_mode should read")
}
