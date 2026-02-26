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

fn value_as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(value) => *value,
        Value::Text(value) => value
            .parse::<i64>()
            .unwrap_or_else(|error| panic!("expected i64-compatible text, got '{value}': {error}")),
        other => panic!("expected integer-compatible value, got {other:?}"),
    }
}

simulation_test!(create_version_defaults_to_active_parent, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

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
    let active_version_id = value_as_text(&active_before.rows[0][0]);
    let active_commit_id = value_as_text(&active_before.rows[0][1]);

    let created = engine
        .create_version(CreateVersionOptions::default())
        .await
        .expect("create_version should succeed");

    assert!(!created.id.is_empty());
    assert_eq!(created.name, created.id);
    assert_eq!(created.inherits_from_version_id, active_version_id);

    let created_row = engine
        .execute(
            "SELECT id, name, inherits_from_version_id, hidden, commit_id, working_commit_id \
             FROM lix_version \
             WHERE id = $1",
            &[Value::Text(created.id.clone())],
        )
        .await
        .expect("created version query should succeed");
    assert_eq!(created_row.rows.len(), 1);
    let row = &created_row.rows[0];
    assert_eq!(value_as_text(&row[0]), created.id);
    assert_eq!(value_as_text(&row[1]), created.name);
    assert_eq!(value_as_text(&row[2]), created.inherits_from_version_id);
    assert!(!value_as_bool(&row[3]));
    assert_eq!(value_as_text(&row[4]), active_commit_id);
    let working_commit_id = value_as_text(&row[5]);
    assert!(!working_commit_id.is_empty());

    let working_commit_rows = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_commit \
             WHERE id = $1",
            &[Value::Text(working_commit_id.clone())],
        )
        .await
        .expect("working commit existence query should succeed");
    assert_eq!(value_as_i64(&working_commit_rows.rows[0][0]), 1);

    let working_change_set_rows = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_change_set cs \
             JOIN lix_commit c ON c.change_set_id = cs.id \
             WHERE c.id = $1",
            &[Value::Text(working_commit_id)],
        )
        .await
        .expect("working change set existence query should succeed");
    assert_eq!(value_as_i64(&working_change_set_rows.rows[0][0]), 1);

    let active_after = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .expect("active version query after create should succeed");
    assert_eq!(value_as_text(&active_after.rows[0][0]), active_version_id);
});

simulation_test!(
    create_version_with_options_and_switch_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("init should succeed");

        let created = engine
            .create_version(CreateVersionOptions {
                id: Some("branch-alpha".to_string()),
                name: Some("Branch Alpha".to_string()),
                inherits_from_version_id: Some("global".to_string()),
                hidden: true,
            })
            .await
            .expect("create_version should succeed");
        assert_eq!(created.id, "branch-alpha");
        assert_eq!(created.name, "Branch Alpha");
        assert_eq!(created.inherits_from_version_id, "global");

        let created_row = engine
            .execute(
                "SELECT id, name, inherits_from_version_id, hidden \
                 FROM lix_version \
                 WHERE id = 'branch-alpha'",
                &[],
            )
            .await
            .expect("created version query should succeed");
        assert_eq!(created_row.rows.len(), 1);
        let row = &created_row.rows[0];
        assert_eq!(value_as_text(&row[0]), "branch-alpha");
        assert_eq!(value_as_text(&row[1]), "Branch Alpha");
        assert_eq!(value_as_text(&row[2]), "global");
        assert!(value_as_bool(&row[3]));

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
        assert_eq!(value_as_text(&active.rows[0][0]), "branch-alpha");
    }
);

simulation_test!(switch_version_rejects_invalid_inputs, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("init should succeed");

    let empty = engine
        .switch_version("".to_string())
        .await
        .expect_err("empty version id should fail");
    assert!(empty.message.contains("non-empty"));

    let missing = engine
        .switch_version("missing-version-id".to_string())
        .await
        .expect_err("unknown version id should fail");
    assert!(missing.message.contains("does not exist"));
});

simulation_test!(create_version_switch_then_checkpoint, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("init should succeed");

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
