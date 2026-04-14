use crate::support;

use lix_engine::Value;

fn assert_text(value: &Value) -> String {
    match value {
        Value::Text(actual) => actual.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

async fn tracked_commit_state(engine: &support::simulation_test::SimulatedLix) -> (i64, String) {
    let commit_count = engine
        .execute("SELECT COUNT(*) FROM lix_commit", &[])
        .await
        .expect("commit count query should succeed");
    let head = engine
        .execute(
            "SELECT commit_id \
             FROM lix_version \
             WHERE id = lix_active_version_id() \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active head query should succeed");

    (
        match &commit_count.statements[0].rows[0][0] {
            Value::Integer(value) => *value,
            other => panic!("expected integer count, got {other:?}"),
        },
        assert_text(&head.statements[0].rows[0][0]),
    )
}

simulation_test!(same_value_state_update_is_noop, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('noop-key', 'same')",
            &[],
        )
        .await
        .expect("seed insert should succeed");

    let before = tracked_commit_state(&engine).await;

    engine
        .execute(
            "UPDATE lix_key_value SET value = 'same' WHERE key = 'noop-key'",
            &[],
        )
        .await
        .expect("noop update should succeed");

    let after = tracked_commit_state(&engine).await;
    assert_eq!(after, before);
});

simulation_test!(same_bytes_exact_file_update_is_noop, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('noop-file', '/noop.txt', lix_text_encode('same'))",
            &[],
        )
        .await
        .expect("seed file insert should succeed");

    let before = tracked_commit_state(&engine).await;

    engine
        .execute(
            "UPDATE lix_file SET data = $1 WHERE id = $2",
            &[
                Value::Blob(b"same".to_vec()),
                Value::Text("noop-file".to_string()),
            ],
        )
        .await
        .expect("noop file update should succeed");

    let after = tracked_commit_state(&engine).await;
    assert_eq!(after, before);

    let file_rows = engine
        .execute("SELECT data FROM lix_file WHERE id = 'noop-file'", &[])
        .await
        .expect("file read should succeed");
    assert_eq!(file_rows.statements[0].rows.len(), 1);
    match &file_rows.statements[0].rows[0][0] {
        Value::Blob(bytes) => assert_eq!(bytes, b"same"),
        other => panic!("expected blob data, got {other:?}"),
    }
});
