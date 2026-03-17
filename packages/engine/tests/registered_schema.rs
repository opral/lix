mod support;

use lix_engine::Value;
use serde_json::json;
use support::simulation_test::assert_boolean_like;

simulation_test!(
    registered_schema_registers_materialized_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )", &[])
            .await
            .unwrap();

        let stored = engine
        .execute(
            "SELECT entity_id, schema_key, schema_version, version_id, file_id, change_id, snapshot_content, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_registered_schema' \
               AND entity_id = 'test_schema~1'", &[])
        .await
        .unwrap();

        sim.assert_deterministic_normalized(stored.statements[0].rows.clone());
        assert_eq!(stored.statements[0].rows.len(), 1);
        let row = &stored.statements[0].rows[0];
        assert_eq!(row[0], Value::Text("test_schema~1".to_string()));
        assert_eq!(row[1], Value::Text("lix_registered_schema".to_string()));
        assert_eq!(row[2], Value::Text("1".to_string()));
        assert_eq!(row[3], Value::Text("global".to_string()));
        assert_eq!(row[4], Value::Text("lix".to_string()));
        assert_eq!(row[5], Value::Text("schema".to_string()));
        assert_boolean_like(&row[7], false);
        let expected_snapshot = serde_json::to_string(
            &serde_json::from_str::<serde_json::Value>(
                "{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}",
            )
            .expect("expected registered schema snapshot must be valid JSON"),
        )
        .expect("expected registered schema snapshot must serialize");
        assert_eq!(row[6], Value::Text(expected_snapshot));

        let table_exists = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(table_exists.statements[0].rows[0][0], Value::Integer(0));
    }
);

simulation_test!(
    registered_schema_insert_accepts_parameterized_snapshot,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ($1, $2)", &[
                Value::Text("lix_registered_schema".to_string()),
                Value::Text(
                    "{\"value\":{\"x-lix-key\":\"param_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}"
                        .to_string(),
                ),
            ])
        .await
        .unwrap();

        let stored = engine
            .execute(
                "SELECT entity_id, schema_key, schema_version \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_registered_schema' \
               AND entity_id = 'param_schema~1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(stored.statements[0].rows.len(), 1);
        let row = &stored.statements[0].rows[0];
        assert_eq!(row[0], Value::Text("param_schema~1".to_string()));
        assert_eq!(row[1], Value::Text("lix_registered_schema".to_string()));
        assert_eq!(row[2], Value::Text("1".to_string()));
    }
);

simulation_test!(
    registered_schema_refreshes_public_surface_dispatch_after_public_insert,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema_by_version (value, lixcol_version_id) VALUES (\
                 lix_json('{\"x-lix-key\":\"dispatch_refresh_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}'),\
                 'global'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute("SELECT COUNT(*) FROM dispatch_refresh_schema", &[])
            .await
            .expect("new public surface should dispatch through public lowering");

        assert_eq!(result.statements[0].rows, vec![vec![Value::Integer(0)]]);
    }
);

simulation_test!(
    register_schema_helper_inserts_globally_and_ensures_live_table,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .register_schema(&json!({
                "x-lix-key": "helper_registered_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }))
            .await
            .unwrap();

        let registered = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = 'helper_registered_schema~1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(registered.statements[0].rows, vec![vec![Value::Integer(1)]]);

        let live_table = engine
            .execute("SELECT COUNT(*) FROM helper_registered_schema", &[])
            .await
            .unwrap();
        assert_eq!(live_table.statements[0].rows, vec![vec![Value::Integer(0)]]);

        let internal_live_table = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_live_v1_helper_registered_schema",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            internal_live_table.statements[0].rows,
            vec![vec![Value::Integer(0)]]
        );
    }
);

simulation_test!(
    register_schema_helper_ignores_duplicate_registration_like_direct_insert,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let schema = json!({
            "x-lix-key": "helper_registered_schema_duplicate",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        });

        engine.register_schema(&schema).await.unwrap();

        engine
            .register_schema(&schema)
            .await
            .expect("duplicate helper registration should no-op like a direct insert");

        let registered = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = 'helper_registered_schema_duplicate~1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(registered.statements[0].rows, vec![vec![Value::Integer(1)]]);
    }
);

simulation_test!(
    registered_schema_public_surface_round_trips_inserted_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 lix_json('{\"x-lix-key\":\"qa_test\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}')\
                 )",
                &[],
            )
            .await
            .unwrap();

        let public_rows = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = 'qa_test~1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(
            public_rows.statements[0].rows,
            vec![vec![Value::Integer(1)]]
        );

        let state_rows = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_registered_schema' \
                   AND entity_id = 'qa_test~1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(
            state_rows.statements[0].rows,
            vec![vec![Value::Text("qa_test~1".to_string())]]
        );
    }
);

simulation_test!(
    registered_schema_rejects_removed_lixcol_version_override,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 lix_json('{\"x-lix-key\":\"qa_removed_override\",\"x-lix-version\":\"1\",\"x-lix-override-lixcols\":{\"lixcol_version_id\":\"\\\"global\\\"\"},\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}')\
                 )",
                &[],
            )
            .await
            .expect_err("removed lixcol_version_id override should be rejected");

        assert!(
            err.to_string().contains("lixcol_version_id")
                && err.to_string().contains("x-lix-override-lixcols"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    registered_schema_requires_foreign_key_targets_are_unique_or_primary,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"parent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-unique\":[[\"/slug\"]],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"slug\",\"name\"],\"additionalProperties\":false}}'\
             )", &[])
            .await
            .unwrap();

        let valid = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"child_schema\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"parent_id\":{\"type\":\"string\"}},\"required\":[\"parent_id\"],\"additionalProperties\":false}}'\
             )", &[])
            .await;

        assert!(valid.is_ok(), "{valid:?}");

        let invalid = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"bad_child\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_name\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/name\"]}}],\"type\":\"object\",\"properties\":{\"parent_name\":{\"type\":\"string\"}},\"required\":[\"parent_name\"],\"additionalProperties\":false}}'\
             )", &[])
            .await;

        let err = invalid.expect_err("expected foreign key validation error");
        assert!(
            err.to_string().contains("not a primary key or unique key"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    registered_schema_updates_validate_schema_definition,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )", &[])
            .await
            .unwrap();

        let result = engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"value\":{\"x-lix-version\":\"1\"}}' \
             WHERE schema_key = 'lix_registered_schema' AND entity_id = 'test_schema~1' AND file_id = 'lix' AND version_id = 'global'", &[])
            .await;

        let err = result.expect_err("expected registered schema validation error");
        assert!(
            err.to_string().contains("Invalid Lix schema definition"),
            "unexpected error: {err}"
        );
    }
);
