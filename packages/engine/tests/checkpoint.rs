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
) -> (String, String) {
    let result = engine
        .execute(
            "SELECT av.version_id, v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(result.rows.len(), 1);
    (as_text(&result.rows[0][0]), as_text(&result.rows[0][1]))
}

simulation_test!(
    checkpoint_create_succeeds_without_internal_access,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                access_to_internal: false,
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.expect("init should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-no-internal', 'v1')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed without internal table access");
        assert!(!checkpoint.id.is_empty());
        assert!(!checkpoint.change_set_id.is_empty());
    }
);

simulation_test!(
    checkpoint_noop_returns_tip_and_updates_last_checkpoint,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        let (version_id, before_commit_id) = active_version_pointer(&engine).await;
        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed");
        let (_version_id, after_commit_id) = active_version_pointer(&engine).await;

        assert_eq!(checkpoint.id, before_commit_id);
        assert_eq!(after_commit_id, before_commit_id);

        let baseline = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .expect("baseline query should succeed");
        assert_eq!(baseline.rows.len(), 1);
        assert_eq!(as_text(&baseline.rows[0][0]), before_commit_id);
    }
);

simulation_test!(checkpoint_labels_current_commit, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-label-key', 'v1')",
            &[],
        )
        .await
        .expect("tracked write should succeed");

    let (_version_id, commit_id) = active_version_pointer(&engine).await;
    engine
        .create_checkpoint()
        .await
        .expect("create_checkpoint should succeed");

    let rows = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_entity_label el \
             JOIN lix_label l ON l.id = el.label_id \
             WHERE el.entity_id = $1 \
               AND el.schema_key = 'lix_commit' \
               AND el.file_id = 'lix' \
               AND l.name = 'checkpoint'",
            &[Value::Text(commit_id)],
        )
        .await
        .expect("checkpoint label query should succeed");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(as_i64(&rows.rows[0][0]), 1);
});

simulation_test!(
    checkpoint_updates_last_checkpoint_after_tracked_write,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-baseline-key', 'v1')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let (version_id, tip_commit_id) = active_version_pointer(&engine).await;
        engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed");

        let baseline = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .expect("baseline query should succeed");
        assert_eq!(baseline.rows.len(), 1);
        assert_eq!(as_text(&baseline.rows[0][0]), tip_commit_id);
    }
);

simulation_test!(checkpoint_clears_working_changes, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_file (path, data, metadata) \
             VALUES ('/checkpoint-working.md', lix_text_encode('v1'), NULL)",
            &[],
        )
        .await
        .expect("tracked write should succeed");

    let before = engine
        .execute("SELECT COUNT(*) FROM lix_working_changes", &[])
        .await
        .expect("working changes query should succeed");
    assert!(as_i64(&before.rows[0][0]) > 0);

    engine
        .create_checkpoint()
        .await
        .expect("create_checkpoint should succeed");

    let after = engine
        .execute("SELECT COUNT(*) FROM lix_working_changes", &[])
        .await
        .expect("working changes query should succeed");
    assert_eq!(as_i64(&after.rows[0][0]), 0);
});

simulation_test!(
    checkpoint_does_not_create_commits_or_edges_on_noop,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        let commits_before = engine
            .execute("SELECT COUNT(*) FROM lix_commit", &[])
            .await
            .expect("commit count should succeed");
        let edges_before = engine
            .execute("SELECT COUNT(*) FROM lix_commit_edge", &[])
            .await
            .expect("edge count should succeed");
        let (_version_id, commit_before) = active_version_pointer(&engine).await;

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed");
        let (_version_id, commit_after) = active_version_pointer(&engine).await;

        let commits_after = engine
            .execute("SELECT COUNT(*) FROM lix_commit", &[])
            .await
            .expect("commit count should succeed");
        let edges_after = engine
            .execute("SELECT COUNT(*) FROM lix_commit_edge", &[])
            .await
            .expect("edge count should succeed");

        assert_eq!(checkpoint.id, commit_before);
        assert_eq!(commit_after, commit_before);
        assert_eq!(
            as_i64(&commits_after.rows[0][0]),
            as_i64(&commits_before.rows[0][0])
        );
        assert_eq!(
            as_i64(&edges_after.rows[0][0]),
            as_i64(&edges_before.rows[0][0])
        );
    }
);
