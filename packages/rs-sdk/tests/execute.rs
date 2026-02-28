use lix_rs_sdk::{open_lix, OpenLixConfig, Value};

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
fn select_works_with_default_in_memory_sqlite() {
    run_async_with_large_stack(|| async {
        let lix = open_lix(OpenLixConfig::default())
            .await
            .expect("open_lix should succeed");

        let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2));
    });
}

#[test]
fn open_lix_initializes_core_tables() {
    run_async_with_large_stack(|| async {
        let lix = open_lix(OpenLixConfig::default())
            .await
            .expect("open_lix should succeed");

        let result = lix
            .execute("SELECT COUNT(*) FROM lix_active_version", &[])
            .await
            .expect("init should create and expose active version");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    });
}
