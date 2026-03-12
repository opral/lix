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
        .name("lix-rs-sdk-test".to_string())
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
fn select_works_after_explicit_init() {
    run_async_with_large_stack(|| async {
        let path = temp_sqlite_path("select-after-init");
        let lix = create_initialized_lix(&path).await;

        let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(result.statements[0].rows[0][0], Value::Integer(2));

        cleanup_sqlite_path(&path);
    });
}

#[test]
fn open_lix_requires_initialized_explicit_backend() {
    run_async_with_large_stack(|| async {
        let backend = SqliteBackend::in_memory().expect("sqlite backend should open");
        let result = Lix::open(LixConfig::new(
            Box::new(backend),
            Arc::new(WasmtimeRuntime::new().expect("wasmtime runtime should initialize")),
        ))
        .await;
        let error = match result {
            Ok(_) => panic!("Lix::open should reject an uninitialized explicit backend"),
            Err(error) => error,
        };

        assert_eq!(error.code, "LIX_ERROR_NOT_INITIALIZED");
    });
}

#[test]
fn init_lix_initializes_core_tables() {
    run_async_with_large_stack(|| async {
        let path = temp_sqlite_path("init-lix");
        let init_result = Lix::init(LixConfig::new(
            Box::new(SqliteBackend::from_path(&path).expect("sqlite backend should open")),
            Arc::new(WasmtimeRuntime::new().expect("wasmtime runtime should initialize")),
        ))
        .await
        .expect("Lix::init should succeed");
        assert!(init_result.initialized);

        let lix = Lix::open(LixConfig::new(
            Box::new(SqliteBackend::from_path(&path).expect("sqlite backend should reopen")),
            Arc::new(WasmtimeRuntime::new().expect("wasmtime runtime should initialize")),
        ))
        .await
        .expect("Lix::open should succeed after Lix::init");

        let result = lix
            .execute("SELECT COUNT(*) FROM lix_active_version", &[])
            .await
            .expect("Lix::init should create and expose active version");

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(result.statements[0].rows[0][0], Value::Integer(1));

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
