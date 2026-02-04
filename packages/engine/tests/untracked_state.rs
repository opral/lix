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
             entity_id, schema_key, file_id, version_id, snapshot_content, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', '{\"key\":\"untracked\"}', 1\
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
