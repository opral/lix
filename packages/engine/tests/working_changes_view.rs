mod support;

use lix_engine::Value;
use std::sync::atomic::{AtomicU64, Ordering};

static UNIQUE_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_null(value: &Value) {
    assert_eq!(*value, Value::Null, "expected NULL, got {value:?}");
}

fn assert_non_empty_text(value: &Value) {
    match value {
        Value::Text(text) => assert!(!text.is_empty(), "expected non-empty text"),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_not_working_projection_change_id(value: &Value) {
    let text = as_text(value);
    assert!(
        !text.starts_with("working_projection:"),
        "expected non-projection change id, got {text}"
    );
}

fn unique_key(prefix: &str) -> String {
    let n = UNIQUE_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n}")
}

async fn rotate_working_commit(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('working-changes-view-seed', 'seed')",
            &[],
        )
        .await
        .expect("seed insert should succeed");
    engine
        .create_checkpoint()
        .await
        .expect("seed checkpoint should succeed");
}

async fn active_version_pointer(
    engine: &support::simulation_test::SimulationEngine,
) -> (String, String) {
    let result = engine
        .execute(
            "SELECT v.commit_id, v.working_commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version pointer query should succeed");
    assert_eq!(result.rows.len(), 1);
    (as_text(&result.rows[0][0]), as_text(&result.rows[0][1]))
}

simulation_test!(lix_working_changes_reports_added_rows, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");
    rotate_working_commit(&engine).await;
    let key = unique_key("wc-view-added");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
            &[Value::Text(key.clone())],
        )
        .await
        .expect("insert should succeed");

    let result = engine
        .execute(
            "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1",
            &[Value::Text(key.clone())],
        )
        .await
        .expect("working changes query should succeed");
    let (_tip_commit_id, working_commit_id) = active_version_pointer(&engine).await;

    assert_eq!(result.rows.len(), 1);
    assert_eq!(as_text(&result.rows[0][0]), "added");
    assert_null(&result.rows[0][1]);
    assert_non_empty_text(&result.rows[0][2]);
    assert_not_working_projection_change_id(&result.rows[0][2]);
    assert_null(&result.rows[0][3]);
    assert_eq!(as_text(&result.rows[0][4]), working_commit_id);
});

simulation_test!(
    lix_working_changes_update_reports_added_rows_against_commit_baseline,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-modified");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                "UPDATE lix_key_value SET value = 'v2' WHERE key = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("update should succeed");

        let result = engine
            .execute(
                "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
                &[Value::Text(key)],
            )
            .await
            .expect("working changes query should succeed");
        let (_tip_commit_id, working_commit_id) = active_version_pointer(&engine).await;

        assert_eq!(result.rows.len(), 1);
        assert_eq!(as_text(&result.rows[0][0]), "added");
        assert_null(&result.rows[0][1]);
        assert_non_empty_text(&result.rows[0][2]);
        assert_not_working_projection_change_id(&result.rows[0][2]);
        assert_null(&result.rows[0][3]);
        assert_eq!(as_text(&result.rows[0][4]), working_commit_id);
    }
);

simulation_test!(
    lix_working_changes_excludes_removed_rows_against_commit_baseline,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-removed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                "DELETE FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("delete should succeed");

        let result = engine
        .execute(
            "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
            &[Value::Text(key)],
        )
        .await
        .expect("working changes query should succeed");
        assert_eq!(result.rows.len(), 0);
    }
);

simulation_test!(
    lix_working_changes_excludes_unchanged_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-unchanged");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let result = engine
            .execute(
                "SELECT status, before_change_id, after_change_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
                &[Value::Text(key)],
            )
            .await
            .expect("working changes query should succeed");

        assert_eq!(result.rows.len(), 0);
    }
);
