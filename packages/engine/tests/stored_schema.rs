mod support;

use lix_engine::Value;

simulation_test!(
    stored_schema_registers_materialized_table,
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
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let stored = engine
        .execute(
            "SELECT entity_id, schema_key, schema_version, version_id, file_id, plugin_key, change_id, is_tombstone, created_at, updated_at, snapshot_content \
             FROM lix_internal_state_materialized_v1_lix_stored_schema \
             WHERE entity_id = 'test_schema~1'",
            &[],
        )
        .await
        .unwrap();

        sim.expect_deterministic(stored.rows.clone());
        assert_eq!(stored.rows.len(), 1);
        let row = &stored.rows[0];
        assert_eq!(row[0], Value::Text("test_schema~1".to_string()));
        assert_eq!(row[1], Value::Text("lix_stored_schema".to_string()));
        assert_eq!(row[2], Value::Text("1".to_string()));
        assert_eq!(row[3], Value::Text("global".to_string()));
        assert_eq!(row[4], Value::Text("lix".to_string()));
        assert_eq!(row[5], Value::Text("lix".to_string()));
        assert_eq!(row[6], Value::Text("schema".to_string()));
        assert_eq!(row[7], Value::Integer(0));
        assert_eq!(row[8], Value::Text("1970-01-01T00:00:00Z".to_string()));
        assert_eq!(row[9], Value::Text("1970-01-01T00:00:00Z".to_string()));
        assert_eq!(
            row[10],
            Value::Text(
                "{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}".to_string()
            )
        );

        let table_exists = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_state_materialized_v1_test_schema",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(table_exists.rows[0][0], Value::Integer(0));
    }
);

simulation_test!(
    stored_schema_insert_accepts_parameterized_snapshot,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ($1, $2)",
            &[
                Value::Text("lix_stored_schema".to_string()),
                Value::Text(
                    "{\"value\":{\"x-lix-key\":\"param_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}"
                        .to_string(),
                ),
            ],
        )
        .await
        .unwrap();

        let stored = engine
        .execute(
            "SELECT entity_id, schema_key, schema_version FROM lix_internal_state_materialized_v1_lix_stored_schema WHERE entity_id = 'param_schema~1'",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(stored.rows.len(), 1);
        let row = &stored.rows[0];
        assert_eq!(row[0], Value::Text("param_schema~1".to_string()));
        assert_eq!(row[1], Value::Text("lix_stored_schema".to_string()));
        assert_eq!(row[2], Value::Text("1".to_string()));
    }
);

simulation_test!(
    stored_schema_requires_foreign_key_targets_are_unique_or_primary,
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
             '{\"value\":{\"x-lix-key\":\"parent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-unique\":[[\"/slug\"]],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"slug\",\"name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let valid = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"child_schema\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"parent_id\":{\"type\":\"string\"}},\"required\":[\"parent_id\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await;

        assert!(valid.is_ok(), "{valid:?}");

        let invalid = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"bad_child\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_name\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/name\"]}}],\"type\":\"object\",\"properties\":{\"parent_name\":{\"type\":\"string\"}},\"required\":[\"parent_name\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await;

        let err = invalid.expect_err("expected foreign key validation error");
        assert!(
            err.to_string().contains("not a primary key or unique key"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    stored_schema_updates_validate_schema_definition,
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
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"value\":{\"x-lix-version\":\"1\"}}' \
             WHERE schema_key = 'lix_stored_schema' AND entity_id = 'test_schema~1' AND file_id = 'lix' AND version_id = 'global'",
                &[],
            )
            .await;

        let err = result.expect_err("expected stored schema validation error");
        assert!(
            err.to_string().contains("Invalid Lix schema definition"),
            "unexpected error: {err}"
        );
    }
);
