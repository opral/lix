mod support;

use lix_engine::{CreateVersionOptions, Value};

fn value_as_text(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        Value::Integer(value) => value.to_string(),
        other => panic!("expected text-like value, got {other:?}"),
    }
}

fn value_as_bool(value: &Value) -> bool {
    match value {
        Value::Boolean(value) => *value,
        Value::Integer(value) => *value != 0,
        Value::Text(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        other => panic!("expected boolean-compatible value, got {other:?}"),
    }
}

async fn active_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let active = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version commit query should succeed");
    assert_eq!(active.statements[0].rows.len(), 1);
    value_as_text(&active.statements[0].rows[0][0])
}

simulation_test!(create_version_defaults_to_active_parent, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    let active_before = engine
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
    let active_version_id = value_as_text(&active_before.statements[0].rows[0][0]);
    let active_commit_id = value_as_text(&active_before.statements[0].rows[0][1]);

    let created = engine
        .create_version(CreateVersionOptions::default())
        .await
        .expect("create_version should succeed");

    assert!(!created.id.is_empty());
    assert_eq!(created.name, created.id);

    let created_row = engine
        .execute(
            "SELECT id, name, hidden, commit_id \
             FROM lix_version \
             WHERE id = $1",
            &[Value::Text(created.id.clone())],
        )
        .await
        .expect("created version query should succeed");
    assert_eq!(created_row.statements[0].rows.len(), 1);
    let row = &created_row.statements[0].rows[0];
    assert_eq!(value_as_text(&row[0]), created.id);
    assert_eq!(value_as_text(&row[1]), created.name);
    assert!(!value_as_bool(&row[2]));
    assert_eq!(value_as_text(&row[3]), active_commit_id);
    let baseline_row = engine
        .execute(
            "SELECT checkpoint_commit_id \
             FROM lix_internal_last_checkpoint \
             WHERE version_id = $1",
            &[Value::Text(created.id.clone())],
        )
        .await
        .expect("baseline pointer query should succeed");
    assert_eq!(baseline_row.statements[0].rows.len(), 1);
    assert_eq!(
        value_as_text(&baseline_row.statements[0].rows[0][0]),
        active_commit_id
    );

    let active_after = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .expect("active version query after create should succeed");
    assert_eq!(
        value_as_text(&active_after.statements[0].rows[0][0]),
        active_version_id
    );
});

simulation_test!(
    tracked_write_moves_active_commit_id_off_global,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let active_before = engine
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
        assert_eq!(active_before.statements[0].rows.len(), 1);
        let active_version_id = value_as_text(&active_before.statements[0].rows[0][0]);
        assert_ne!(active_version_id, "global");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-pointer-regression', '1')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let active_after = engine
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
        assert_eq!(active_after.statements[0].rows.len(), 1);
        assert_eq!(
            value_as_text(&active_after.statements[0].rows[0][0]),
            active_version_id
        );
        assert_ne!(
            value_as_text(&active_after.statements[0].rows[0][1]),
            "global"
        );
    }
);

simulation_test!(
    content_only_update_moves_active_commit_pointer,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('version-api-content-only', '/version-api-content-only.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let before_commit = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('after') \
                 WHERE id = 'version-api-content-only'",
                &[],
            )
            .await
            .expect("content-only update should succeed");

        let after_commit = active_version_commit_id(&engine).await;
        assert_ne!(after_commit, before_commit);
    }
);

simulation_test!(
    create_version_with_options_and_switch_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let created = engine
            .create_version(CreateVersionOptions {
                id: Some("branch-alpha".to_string()),
                name: Some("Branch Alpha".to_string()),
                hidden: true,
            })
            .await
            .expect("create_version should succeed");
        assert_eq!(created.id, "branch-alpha");
        assert_eq!(created.name, "Branch Alpha");

        let created_row = engine
            .execute(
                "SELECT id, name, hidden \
                 FROM lix_version \
                 WHERE id = 'branch-alpha'",
                &[],
            )
            .await
            .expect("created version query should succeed");
        assert_eq!(created_row.statements[0].rows.len(), 1);
        let row = &created_row.statements[0].rows[0];
        assert_eq!(value_as_text(&row[0]), "branch-alpha");
        assert_eq!(value_as_text(&row[1]), "Branch Alpha");
        assert!(value_as_bool(&row[2]));

        engine
            .switch_version("branch-alpha".to_string())
            .await
            .expect("switch_version should succeed");
        let active = engine
            .execute(
                "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(
            value_as_text(&active.statements[0].rows[0][0]),
            "branch-alpha"
        );
    }
);

simulation_test!(switch_version_rejects_invalid_inputs, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    let empty = engine
        .switch_version("".to_string())
        .await
        .expect_err("empty version id should fail");
    assert!(empty.description.contains("non-empty"));

    let missing = engine
        .switch_version("missing-version-id".to_string())
        .await
        .expect_err("unknown version id should fail");
    assert!(missing.description.contains("does not exist"));
});

simulation_test!(create_version_switch_then_checkpoint, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    let created = engine
        .create_version(CreateVersionOptions::default())
        .await
        .expect("create_version should succeed");

    engine
        .switch_version(created.id)
        .await
        .expect("switch_version should succeed");

    engine
        .create_checkpoint()
        .await
        .expect("create_checkpoint should succeed after switching to created version");
});
