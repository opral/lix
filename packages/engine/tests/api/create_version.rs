use crate::support;

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

async fn version_commit_id(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
) -> String {
    let result = engine
        .execute(
            "SELECT commit_id FROM lix_version WHERE id = $1 LIMIT 1",
            &[Value::Text(version_id.to_string())],
        )
        .await
        .expect("version commit query should succeed");
    assert_eq!(result.statements[0].rows.len(), 1);
    value_as_text(&result.statements[0].rows[0][0])
}

simulation_test!(create_version_defaults_to_active_parent, |sim| async move {
    let engine = sim
        .boot_simulated_lix_deterministic()
        .await
        .expect("boot_simulated_lix_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    let active_before = engine
        .execute(
            "SELECT lix_active_version_id(), commit_id \
             FROM lix_version \
             WHERE id = lix_active_version_id() \
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
    assert_eq!(created.parent_version_id, active_version_id);
    assert_eq!(created.parent_commit_id, active_commit_id);

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
        .execute("SELECT lix_active_version_id()", &[])
        .await
        .expect("active version query after create should succeed");
    assert_eq!(
        value_as_text(&active_after.statements[0].rows[0][0]),
        active_version_id
    );
});

simulation_test!(
    create_version_with_options_and_switch_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let created = engine
            .create_version(CreateVersionOptions {
                id: Some("branch-alpha".to_string()),
                name: Some("Branch Alpha".to_string()),
                source_version_id: None,
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
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version query should succeed");
        assert_eq!(
            value_as_text(&active.statements[0].rows[0][0]),
            "branch-alpha"
        );
    }
);

simulation_test!(
    create_version_can_target_explicit_source_without_switching_active,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let main_version_id = engine
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version query should succeed");
        let main_version_id = value_as_text(&main_version_id.statements[0].rows[0][0]);

        engine
            .create_version(CreateVersionOptions {
                id: Some("source-branch".to_string()),
                name: Some("Source Branch".to_string()),
                source_version_id: None,
                hidden: false,
            })
            .await
            .expect("source version should be created");
        engine
            .switch_version("source-branch".to_string())
            .await
            .expect("switch to source should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('source-only-key', 'source-only')",
                &[],
            )
            .await
            .expect("source branch write should succeed");

        let source_head = version_commit_id(&engine, "source-branch").await;

        engine
            .switch_version(main_version_id.clone())
            .await
            .expect("switch back to main should succeed");
        let active_before = engine
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version query should succeed");
        assert_eq!(
            value_as_text(&active_before.statements[0].rows[0][0]),
            main_version_id
        );

        let created = engine
            .create_version(CreateVersionOptions {
                id: Some("child-of-source".to_string()),
                name: Some("Child Of Source".to_string()),
                source_version_id: Some("source-branch".to_string()),
                hidden: false,
            })
            .await
            .expect("create_version with explicit source should succeed");

        assert_eq!(created.parent_version_id, "source-branch");
        assert_eq!(created.parent_commit_id, source_head);

        let active_after = engine
            .execute("SELECT lix_active_version_id()", &[])
            .await
            .expect("active version query should succeed");
        assert_eq!(
            value_as_text(&active_after.statements[0].rows[0][0]),
            main_version_id
        );

        engine
            .switch_version("child-of-source".to_string())
            .await
            .expect("switch to child should succeed");
        assert_eq!(
            version_commit_id(&engine, "child-of-source").await,
            source_head
        );
    }
);

simulation_test!(create_version_rejects_invalid_inputs, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.expect("init should succeed");

    let empty_id = engine
        .create_version(CreateVersionOptions {
            id: Some(String::new()),
            ..Default::default()
        })
        .await
        .expect_err("empty version id should fail");
    assert!(empty_id
        .description
        .contains("id must be a non-empty string"));

    let empty_name = engine
        .create_version(CreateVersionOptions {
            name: Some(" ".to_string()),
            ..Default::default()
        })
        .await
        .expect_err("empty version name should fail");
    assert!(empty_name
        .description
        .contains("name must be a non-empty string"));

    let reserved = engine
        .create_version(CreateVersionOptions {
            id: Some("global".to_string()),
            ..Default::default()
        })
        .await
        .expect_err("reserved global version id should fail");
    assert!(reserved.description.contains("reserved"));
});
