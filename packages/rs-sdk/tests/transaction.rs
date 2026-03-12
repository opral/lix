use lix_rs_sdk::{Lix, LixConfig, SqliteBackend, Value, WasmtimeRuntime};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn run_async_with_large_stack<F, Fut>(build_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("lix-rs-sdk-tx-test".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build");
            runtime.block_on(build_future());
        })
        .expect("test thread should spawn")
        .join()
        .expect("test thread should join");
}

#[test]
fn execute_begin_commit_script_persists_changes() {
    run_async_with_large_stack(|| async {
        let path = temp_sqlite_path("tx-script");
        let lix = create_initialized_lix(&path).await;

        lix.execute(
            "BEGIN; \
             INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3); \
             COMMIT;",
            &[
                Value::Text("tx-script-commit".to_string()),
                Value::Text("/tx-script-commit.txt".to_string()),
                Value::Blob(vec![1, 2, 3]),
            ],
        )
        .await
        .expect("BEGIN/COMMIT script should succeed");

        let result = lix
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE id = ? AND path = ?",
                &[
                    Value::Text("tx-script-commit".to_string()),
                    Value::Text("/tx-script-commit.txt".to_string()),
                ],
            )
            .await
            .expect("verification query should succeed");

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(result.statements[0].rows[0][0], Value::Integer(1));

        cleanup_sqlite_path(&path);
    });
}

#[test]
fn execute_rejects_standalone_transaction_control() {
    run_async_with_large_stack(|| async {
        let path = temp_sqlite_path("tx-denied");
        let lix = create_initialized_lix(&path).await;

        let error = lix
            .execute("ROLLBACK;", &[])
            .await
            .expect_err("standalone transaction control should be rejected");

        assert_eq!(error.code, "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED");

        cleanup_sqlite_path(&path);
    });
}

async fn create_initialized_lix(path: &PathBuf) -> Lix {
    let init_result = Lix::init(LixConfig::new(
        Box::new(SqliteBackend::from_path(path).expect("sqlite backend should open")),
        Arc::new(WasmtimeRuntime::new().expect("wasmtime runtime should initialize")),
    ))
    .await
    .expect("Lix::init should succeed");
    assert!(init_result.initialized);

    Lix::open(LixConfig::new(
        Box::new(SqliteBackend::from_path(path).expect("sqlite backend should reopen")),
        Arc::new(WasmtimeRuntime::new().expect("wasmtime runtime should initialize")),
    ))
    .await
    .expect("Lix::open should succeed after Lix::init")
}

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-rs-sdk-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn cleanup_sqlite_path(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-wal", path.display())));
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", path.display())));
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-journal", path.display())));
}
