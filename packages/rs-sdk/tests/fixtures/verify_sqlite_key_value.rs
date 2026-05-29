use std::path::PathBuf;

use lix_sdk::{open_lix_with_backend, SqliteBackend, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let path = args
        .next()
        .map(PathBuf::from)
        .expect("usage: verify_sqlite_key_value <path> <key> <value>");
    let key = args
        .next()
        .and_then(|value| value.into_string().ok())
        .expect("usage: verify_sqlite_key_value <path> <key> <value>");
    let value = args
        .next()
        .and_then(|value| value.into_string().ok())
        .expect("usage: verify_sqlite_key_value <path> <key> <value>");

    let lix =
        open_lix_with_backend(SqliteBackend::open(&path).expect("sqlite backend should open"))
            .await
            .expect("lix should open on sqlite backend");
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key)],
        )
        .await
        .expect("key/value read should succeed");
    let actual = result
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .expect("expected one key/value row");
    match actual {
        Value::Json(json) if json.as_str() == Some(&value) => {}
        Value::Text(text) if text == &value => {}
        other => panic!("expected value {value:?}, got {other:?}"),
    }
    lix.close().await.expect("lix should close");
}
