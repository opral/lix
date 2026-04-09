use lix_engine::Value;

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(actual) => actual.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

simulation_test!(
    lix_change_set_element_by_version_scopes_to_version_heads,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        engine
            .create_named_version("version-a")
            .await
            .expect("create_named_version should succeed");
        engine
            .create_named_version("version-b")
            .await
            .expect("create_named_version should succeed");

        let before_version_commit = engine
            .execute(
                "SELECT commit_id FROM lix_version WHERE id = 'version-a' LIMIT 1",
                &[],
            )
            .await
            .expect("version-a commit query should succeed");
        let before_commit_id = as_text(&before_version_commit.statements[0].rows[0][0]);
        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (key, value, lixcol_version_id) \
                 VALUES ('change-set-by-version-key-1', 'v1', 'version-a')",
                &[],
            )
            .await
            .expect("tracked by-version write should succeed");

        let after_version_commit = engine
            .execute(
                "SELECT commit_id FROM lix_version WHERE id = 'version-a' LIMIT 1",
                &[],
            )
            .await
            .expect("updated version-a commit query should succeed");
        let after_commit_id = as_text(&after_version_commit.statements[0].rows[0][0]);
        assert_ne!(
            before_commit_id, after_commit_id,
            "explicit by-version write should advance version-a head commit"
        );

        let default_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE entity_id = 'change-set-by-version-key-1' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[],
            )
            .await
            .expect("default change-set element query should succeed");
        match &default_count.statements[0].rows[0][0] {
            Value::Integer(count) => assert_eq!(*count, 1),
            other => panic!("expected default count as integer, got {other:?}"),
        }

        let active_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element_by_version \
                 WHERE lixcol_version_id = $1 \
                   AND entity_id = 'change-set-by-version-key-1' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text("version-a".to_string())],
            )
            .await
            .expect("active by-version membership query should succeed");
        assert_eq!(active_count.statements[0].rows.len(), 1);
        match &active_count.statements[0].rows[0][0] {
            Value::Integer(count) => assert_eq!(*count, 1),
            other => panic!("expected active count as integer, got {other:?}"),
        }

        let created_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element_by_version \
                 WHERE lixcol_version_id = $1 \
                   AND entity_id = 'change-set-by-version-key-1' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text("version-b".to_string())],
            )
            .await
            .expect("branch by-version membership query should succeed");
        assert_eq!(created_count.statements[0].rows.len(), 1);
        match &created_count.statements[0].rows[0][0] {
            Value::Integer(count) => assert_eq!(*count, 0),
            other => panic!("expected branch count as integer, got {other:?}"),
        }
    }
);

simulation_test!(
    lix_change_set_element_checkpoint_reads_do_not_require_commit_family_live_mirrors,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-cse-key-1', 'v1')",
                &[],
            )
            .await
            .expect("insert should succeed");

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        for table in [
            "lix_internal_live_v1_lix_commit",
            "lix_internal_live_v1_lix_change_set_element",
            "lix_internal_live_v1_lix_commit_edge",
        ] {
            engine
                .execute(&format!("DROP TABLE IF EXISTS {table}"), &[])
                .await
                .unwrap_or_else(|error| {
                    panic!("dropping '{table}' should succeed: {}", error.description)
                });
        }

        let checkpoint_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND entity_id = 'checkpoint-cse-key-1' \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text(checkpoint.change_set_id)],
            )
            .await
            .expect(
                "checkpoint change-set query should succeed without commit-family live mirrors",
            );

        match &checkpoint_count.statements[0].rows[0][0] {
            Value::Integer(count) => assert_eq!(*count, 1),
            other => panic!("expected checkpoint count as integer, got {other:?}"),
        }
    }
);
