use lix_engine::Value;

simulation_test!(
    same_request_schema_insert_allows_snapshot_validation,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"same_request_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"lix\\\"\",\"lixcol_plugin_key\":\"\\\"lix\\\"\",\"lixcol_global\":\"true\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
             );\
             INSERT INTO same_request_schema (id, name) VALUES ('entity-1', 'Ada')", &[])
            .await;

        assert!(result.is_ok(), "{result:?}");

        let stored = engine
            .execute(
                "SELECT id, name FROM same_request_schema WHERE id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();
        let row = &stored.statements[0].rows[0];
        assert_eq!(row[0], Value::Text("entity-1".to_string()));
        assert_eq!(row[1], Value::Text("Ada".to_string()));
    }
);

simulation_test!(
    same_request_registered_schema_foreign_key_uses_pending_target,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"same_request_parent\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
             );\
             INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"same_request_child\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"same_request_parent\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"parent_id\":{\"type\":\"string\"}},\"required\":[\"parent_id\"],\"additionalProperties\":false}')\
             )", &[])
            .await;

        assert!(result.is_ok(), "{result:?}");

        let count = engine
            .execute(
                "SELECT COUNT(*) FROM lix_state_by_version \
             WHERE schema_key = 'lix_registered_schema' \
               AND entity_id IN ('same_request_parent~1', 'same_request_child~1')",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(count.statements[0].rows[0][0], Value::Integer(2));
    }
);

simulation_test!(
    same_request_schema_insert_applies_defaults,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let result = engine
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"same_request_default_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"lix\\\"\",\"lixcol_plugin_key\":\"\\\"lix\\\"\",\"lixcol_global\":\"true\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\",\"x-lix-default\":\"name + ''-slug''\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
             );\
             INSERT INTO same_request_default_schema (id, name) VALUES ('entity-1', 'Sample')", &[])
        .await;

        assert!(result.is_ok(), "{result:?}");

        let row = engine
            .execute(
                "SELECT id, name, slug FROM same_request_default_schema WHERE id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();
        let row = &row.statements[0].rows[0];
        assert_eq!(row[0], Value::Text("entity-1".to_string()));
        assert_eq!(row[1], Value::Text("Sample".to_string()));
        assert_eq!(row[2], Value::Text("Sample-slug".to_string()));
    }
);
