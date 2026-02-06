mod support;

use lix_engine::Value;

simulation_test!(
    vtable_read_prioritizes_untracked_over_tracked,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        // Register schema so materialized table exists.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        // Insert tracked row directly into materialized table.
        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'test_schema', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}'\
             )",
            &[],
        )
        .await
        .unwrap();

        // Insert untracked row via vtable (should take priority).
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

        let read = engine
            .execute(
                "SELECT snapshot_content, untracked FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0][0],
            Value::Text("{\"key\":\"untracked\"}".to_string())
        );
        assert_eq!(read.rows[0][1], Value::Integer(1));
    }
);

simulation_test!(
    vtable_read_returns_tracked_when_no_untracked_exists,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        // Register schema so materialized table exists.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        // Insert tracked row directly into materialized table.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'test_schema', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}'\
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

        sim.assert_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );
        assert_eq!(read.rows[0][1], Value::Integer(0));
    }
);

simulation_test!(
    vtable_read_schema_key_in_selects_multiple_materialized_tables,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        // Register schemas so materialized tables exist.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        // Insert tracked rows into both materialized tables.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let read = engine
            .execute(
                "SELECT entity_id, schema_key, file_id FROM lix_internal_state_vtable \
             WHERE schema_key IN ('schema_a', 'schema_b') \
             ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 3);
        assert_eq!(read.rows[0][0], Value::Text("entity-1".to_string()));
        assert_eq!(read.rows[0][1], Value::Text("schema_a".to_string()));
        assert_eq!(read.rows[0][2], Value::Text("file-1".to_string()));
        assert_eq!(read.rows[1][0], Value::Text("entity-2".to_string()));
        assert_eq!(read.rows[1][1], Value::Text("schema_b".to_string()));
        assert_eq!(read.rows[1][2], Value::Text("file-2".to_string()));
        assert_eq!(read.rows[2][0], Value::Text("entity-3".to_string()));
        assert_eq!(read.rows[2][1], Value::Text("schema_a".to_string()));
        assert_eq!(read.rows[2][2], Value::Text("file-3".to_string()));
    }
);

simulation_test!(
    vtable_read_schema_key_in_single_filters_out_other,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let schema_a_only = engine
            .execute(
                "SELECT entity_id, schema_key FROM lix_internal_state_vtable \
             WHERE schema_key IN ('schema_a') \
             ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(schema_a_only.rows.clone());
        assert_eq!(schema_a_only.rows.len(), 2);
        assert_eq!(
            schema_a_only.rows[0][0],
            Value::Text("entity-1".to_string())
        );
        assert_eq!(
            schema_a_only.rows[0][1],
            Value::Text("schema_a".to_string())
        );
        assert_eq!(
            schema_a_only.rows[1][0],
            Value::Text("entity-3".to_string())
        );
        assert_eq!(
            schema_a_only.rows[1][1],
            Value::Text("schema_a".to_string())
        );
    }
);

simulation_test!(
    vtable_read_schema_key_equals_selects_single_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let schema_a_eq = engine
            .execute(
                "SELECT entity_id, schema_key FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' \
             ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(schema_a_eq.rows.clone());
        assert_eq!(schema_a_eq.rows.len(), 2);
        assert_eq!(schema_a_eq.rows[0][0], Value::Text("entity-1".to_string()));
        assert_eq!(schema_a_eq.rows[0][1], Value::Text("schema_a".to_string()));
        assert_eq!(schema_a_eq.rows[1][0], Value::Text("entity-3".to_string()));
        assert_eq!(schema_a_eq.rows[1][1], Value::Text("schema_a".to_string()));
    }
);

simulation_test!(
    vtable_read_filters_by_entity_id_with_schema_key,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let entity_filter = engine
            .execute(
                "SELECT entity_id, schema_key, file_id FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_b' AND entity_id = 'entity-2'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(entity_filter.rows.clone());
        assert_eq!(entity_filter.rows.len(), 1);
        assert_eq!(
            entity_filter.rows[0][0],
            Value::Text("entity-2".to_string())
        );
        assert_eq!(
            entity_filter.rows[0][1],
            Value::Text("schema_b".to_string())
        );
        assert_eq!(entity_filter.rows[0][2], Value::Text("file-2".to_string()));
    }
);

simulation_test!(
    vtable_read_filters_by_file_id_with_schema_key,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let file_filter = engine
            .execute(
                "SELECT entity_id, schema_key, file_id FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' AND file_id = 'file-3'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(file_filter.rows.clone());
        assert_eq!(file_filter.rows.len(), 1);
        assert_eq!(file_filter.rows[0][0], Value::Text("entity-3".to_string()));
        assert_eq!(file_filter.rows[0][1], Value::Text("schema_a".to_string()));
        assert_eq!(file_filter.rows[0][2], Value::Text("file-3".to_string()));
    }
);

simulation_test!(
    vtable_read_filters_by_multiple_predicates,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'schema_a', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"a1\"}'\
             ), (\
             'entity-3', 'schema_a', '1', 'file-3', 'version-1', 'lix', '{\"key\":\"a2\"}'\
             )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-2', 'schema_b', '1', 'file-2', 'version-1', 'lix', '{\"key\":\"b1\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let multi_filter = engine
            .execute(
                "SELECT entity_id, schema_key, file_id FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' AND entity_id = 'entity-3' AND file_id = 'file-3'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(multi_filter.rows.clone());
        assert_eq!(multi_filter.rows.len(), 1);
        assert_eq!(multi_filter.rows[0][0], Value::Text("entity-3".to_string()));
        assert_eq!(multi_filter.rows[0][1], Value::Text("schema_a".to_string()));
        assert_eq!(multi_filter.rows[0][2], Value::Text("file-3".to_string()));
    }
);

simulation_test!(
    vtable_read_falls_back_to_tracked_after_untracked_deleted,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        // Register schema so materialized table exists.
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        // Insert tracked row directly into materialized table.
        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content\
             ) VALUES (\
             'entity-1', 'test_schema', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}'\
             )",
            &[],
        )
        .await
        .unwrap();

        // Insert untracked row via vtable.
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

        // Delete untracked row.
        engine
        .execute(
            "DELETE FROM lix_internal_state_vtable WHERE entity_id = 'entity-1' AND untracked = 1",
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

        sim.assert_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );
        assert_eq!(read.rows[0][1], Value::Integer(0));
    }
);
