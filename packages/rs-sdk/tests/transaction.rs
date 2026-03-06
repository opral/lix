use lix_rs_sdk::{open_lix, OpenLixConfig, Value};

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
fn begin_transaction_commit_persists_changes() {
    run_async_with_large_stack(|| async {
        let lix = open_lix(OpenLixConfig::default())
            .await
            .expect("open_lix should succeed");

        let mut tx = lix
            .begin_transaction()
            .await
            .expect("begin_transaction should succeed");
        tx.execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text("tx-commit".to_string()),
                Value::Text("/tx-commit.txt".to_string()),
                Value::Blob(vec![1, 2, 3]),
            ],
        )
        .await
        .expect("insert in transaction should succeed");
        tx.commit().await.expect("commit should succeed");

        let result = lix
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE id = ? AND path = ?",
                &[
                    Value::Text("tx-commit".to_string()),
                    Value::Text("/tx-commit.txt".to_string()),
                ],
            )
            .await
            .expect("verification query should succeed");

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(result.statements[0].rows[0][0], Value::Integer(1));
    });
}

#[test]
fn begin_transaction_rollback_discards_changes() {
    run_async_with_large_stack(|| async {
        let lix = open_lix(OpenLixConfig::default())
            .await
            .expect("open_lix should succeed");

        let mut tx = lix
            .begin_transaction()
            .await
            .expect("begin_transaction should succeed");
        tx.execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text("tx-rollback".to_string()),
                Value::Text("/tx-rollback.txt".to_string()),
                Value::Blob(vec![9, 9, 9]),
            ],
        )
        .await
        .expect("insert in transaction should succeed");
        tx.rollback().await.expect("rollback should succeed");

        let result = lix
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE id = ?",
                &[Value::Text("tx-rollback".to_string())],
            )
            .await
            .expect("verification query should succeed");

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(result.statements[0].rows[0][0], Value::Integer(0));
    });
}
