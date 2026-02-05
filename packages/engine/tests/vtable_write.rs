mod support;

use lix_engine::Value;

simulation_test!(
    untracked_state_routes_to_untracked_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                 'lix_stored_schema',\
                 '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1.0.0\"}}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"untracked\"}', '1', 1\
             )",
                &[],
            )
            .await
            .unwrap();

        let initial = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        sim.expect_deterministic(initial.rows.clone());
        assert_eq!(initial.rows.len(), 1);
        assert_eq!(
            initial.rows[0][0],
            Value::Text("{\"key\":\"untracked\"}".to_string())
        );

        engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"key\":\"updated\"}' \
             WHERE entity_id = 'entity-1' AND untracked = 1",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            updated.rows[0][0],
            Value::Text("{\"key\":\"updated\"}".to_string())
        );

        engine
            .execute(
                "INSERT INTO lix_internal_state_materialized_v1_test_schema (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, change_id, created_at, updated_at\
                 ) VALUES (\
                 'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', 'change-1', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let read = engine
            .execute(
                "SELECT snapshot_content, untracked FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        sim.expect_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0][0],
            Value::Text("{\"key\":\"updated\"}".to_string())
        );
        assert_eq!(read.rows[0][1], Value::Integer(1));

        engine
            .execute(
                "DELETE FROM lix_internal_state_vtable WHERE entity_id = 'entity-1' AND untracked = 1",
                &[],
            )
            .await
            .unwrap();

        let remaining = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(remaining.rows[0][0], Value::Integer(0));
    }
);

simulation_test!(untracked_state_change_id_is_untracked, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                 'lix_stored_schema',\
                 '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1.0.0\"}}'\
                 )",
            &[],
        )
        .await
        .unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"untracked\"}', '1', 1\
             )",
                &[],
            )
            .await
            .unwrap();

    let vtable = engine
        .execute(
            "SELECT change_id FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(vtable.rows.len(), 1);
    assert_eq!(vtable.rows[0][0], Value::Text("untracked".to_string()));
});

simulation_test!(
    tracked_state_creates_change_and_materialized_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT snapshot_id FROM lix_internal_change WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.rows.len(), 1);

        let snapshot_id = match &changes.rows[0][0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected snapshot id"),
        };

        let snapshots = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(snapshots.rows.len(), 1);
        assert_eq!(
            snapshots.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );

        let materialized = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_materialized_v1_test_schema \
                 WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(materialized.rows.len(), 1);
        assert_eq!(
            materialized.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );
    }
);

simulation_test!(
    tracked_state_uses_no_content_snapshot_for_nulls,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-2', 'test_schema', 'file-1', 'version-1', 'lix', NULL, '1'\
             )",
                &[],
            )
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT snapshot_id FROM lix_internal_change WHERE entity_id = 'entity-2'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.rows.len(), 1);
        assert_eq!(changes.rows[0][0], Value::Text("no-content".to_string()));
    }
);

simulation_test!(tracked_state_change_id_matches_vtable, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

    let change = engine
        .execute(
            "SELECT id FROM lix_internal_change WHERE entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(change.rows.len(), 1);
    let change_id = match &change.rows[0][0] {
        Value::Text(value) => value.clone(),
        _ => panic!("expected change id"),
    };

    let vtable = engine
        .execute(
            "SELECT change_id FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(vtable.rows.len(), 1);
    assert_eq!(vtable.rows[0][0], Value::Text(change_id));
});
