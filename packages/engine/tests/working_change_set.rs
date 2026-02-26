mod support;

use lix_engine::Value;

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(value) => *value,
        other => panic!("expected integer value, got {other:?}"),
    }
}

async fn active_version_pointer(
    engine: &support::simulation_test::SimulationEngine,
) -> (String, String, String) {
    let result = engine
        .execute(
            "SELECT av.version_id, v.commit_id, v.working_commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(result.rows.len(), 1);
    (
        as_text(&result.rows[0][0]),
        as_text(&result.rows[0][1]),
        as_text(&result.rows[0][2]),
    )
}

async fn working_change_set_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let (_version_id, _tip_commit_id, working_commit_id) = active_version_pointer(engine).await;
    let result = engine
        .execute(
            "SELECT change_set_id \
             FROM lix_commit \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(working_commit_id)],
        )
        .await
        .expect("working commit query should succeed");
    assert_eq!(result.rows.len(), 1);
    as_text(&result.rows[0][0])
}

async fn ensure_rotated_working_commit(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('working-seed-key', 'seed')",
            &[],
        )
        .await
        .expect("seed insert should succeed");
    engine
        .create_checkpoint()
        .await
        .expect("seed checkpoint should succeed");
}

simulation_test!(
    working_insert_is_visible_in_current_working_change_set,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        ensure_rotated_working_commit(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-insert-key', 'v1')",
                &[],
            )
            .await
            .expect("working insert should succeed");

        let change_set_id = working_change_set_id(&engine).await;
        let result = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND entity_id = 'working-insert-key' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text(change_set_id)],
            )
            .await
            .expect("working change_set entity query should succeed");
        assert_eq!(as_i64(&result.rows[0][0]), 1);
    }
);

simulation_test!(
    working_delete_emits_tombstone_in_current_working_change_set,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-delete-key', 'v1')",
                &[],
            )
            .await
            .expect("seed key insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("seed checkpoint should succeed");

        engine
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'working-delete-key'",
                &[],
            )
            .await
            .expect("working delete should succeed");

        let change_set_id = working_change_set_id(&engine).await;
        let result = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element cse \
                 JOIN lix_change c ON c.id = cse.change_id \
                 WHERE cse.change_set_id = $1 \
                   AND cse.entity_id = 'working-delete-key' \
                   AND cse.schema_key = 'lix_key_value' \
                   AND cse.file_id = 'lix' \
                   AND c.snapshot_content IS NULL",
                &[Value::Text(change_set_id)],
            )
            .await
            .expect("working tombstone query should succeed");
        assert_eq!(as_i64(&result.rows[0][0]), 1);
    }
);

simulation_test!(
    working_change_set_updates_with_multiple_writes_before_checkpoint,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        ensure_rotated_working_commit(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-multi-key-1', 'a')",
                &[],
            )
            .await
            .expect("first working insert should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-multi-key-2', 'b')",
                &[],
            )
            .await
            .expect("second working insert should succeed");

        let change_set_id = working_change_set_id(&engine).await;
        let result = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id IN ('working-multi-key-1', 'working-multi-key-2')",
                &[Value::Text(change_set_id)],
            )
            .await
            .expect("working multi-write query should succeed");
        assert_eq!(as_i64(&result.rows[0][0]), 2);
    }
);

simulation_test!(
    working_change_set_is_reset_after_checkpoint_rotation,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        ensure_rotated_working_commit(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-reset-key', 'v1')",
                &[],
            )
            .await
            .expect("working insert should succeed");

        let before_change_set_id = working_change_set_id(&engine).await;
        let before_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND entity_id = 'working-reset-key' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text(before_change_set_id)],
            )
            .await
            .expect("pre-checkpoint working change_set query should succeed");
        assert_eq!(as_i64(&before_count.rows[0][0]), 1);

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let after_change_set_id = working_change_set_id(&engine).await;
        let after_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1",
                &[Value::Text(after_change_set_id)],
            )
            .await
            .expect("post-checkpoint working change_set query should succeed");
        assert_eq!(as_i64(&after_count.rows[0][0]), 0);

        let checkpoint_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND entity_id = 'working-reset-key' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text(checkpoint.change_set_id)],
            )
            .await
            .expect("checkpoint change_set query should succeed");
        assert_eq!(as_i64(&checkpoint_count.rows[0][0]), 1);
    }
);
