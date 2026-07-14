use std::path::PathBuf;

use lix_sdk::{SQLite, Value, open_lix_with_storage};

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

    let storage = SQLite::open(&path).expect("sqlite storage should open");
    let lix = open_lix_with_storage(storage.clone())
        .await
        .expect("lix should open on sqlite storage");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[Value::Text(key), Value::Text(value)],
    )
    .await
    .expect("key/value write should succeed");
    lix.close().await.expect("lix should close");
    storage
        .checkpoint()
        .expect("sqlite storage should checkpoint fixture");
}
