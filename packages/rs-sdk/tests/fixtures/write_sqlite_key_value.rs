use std::path::PathBuf;

use lix_rs_sdk::{open_lix_with_backend, SqliteBackend, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let path = args
        .next()
        .map(PathBuf::from)
        .expect("usage: write_sqlite_key_value <path> <key> <value>");
    let key = args
        .next()
        .and_then(|value| value.into_string().ok())
        .expect("usage: write_sqlite_key_value <path> <key> <value>");
    let value = args
        .next()
        .and_then(|value| value.into_string().ok())
        .expect("usage: write_sqlite_key_value <path> <key> <value>");

    let backend = SqliteBackend::open(&path).expect("sqlite backend should open");
    let lix = open_lix_with_backend(backend.clone())
        .await
        .expect("lix should open on sqlite backend");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[Value::Text(key), Value::Text(value)],
    )
    .await
    .expect("key/value write should succeed");
    lix.close().await.expect("lix should close");
    backend
        .checkpoint()
        .expect("sqlite backend should checkpoint fixture");
}
