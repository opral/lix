use crate::support;

use lix_engine::{ExecuteOptions, LixError, Value};
use serde_json::json;
use support::simulation_test::assert_boolean_like;

simulation_test!(
    registered_schema_registers_materialized_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix_deterministic should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}')\
             )", &[])
            .await
            .unwrap();

        let active_version_id = engine.active_version_id().await.unwrap();
        let stored = engine
        .execute(
            "SELECT entity_id, schema_key, schema_version, version_id, file_id, change_id, snapshot_content, untracked \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_registered_schema' \
               AND entity_id = 'test_schema~1' \
               AND version_id = lix_active_version_id()",
            &[],
        )
        .await
        .unwrap();

        sim.assert_deterministic_normalized(stored.statements[0].rows.clone());
        assert_eq!(stored.statements[0].rows.len(), 1);
        let row = &stored.statements[0].rows[0];
        assert_eq!(row[0], Value::Text("test_schema~1".to_string()));
        assert_eq!(row[1], Value::Text("lix_registered_schema".to_string()));
        assert_eq!(row[2], Value::Text("1".to_string()));
        assert_eq!(row[3], Value::Text(active_version_id));
        assert_eq!(row[4], Value::Null);
        assert!(matches!(&row[5], Value::Text(change_id) if !change_id.is_empty()));
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
                "SELECT COUNT(*) FROM lix_state_by_version \
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))", &[
                Value::Text(
                    "{\"x-lix-key\":\"param_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}"
                        .to_string(),
                ),
            ])
        .await
        .unwrap();

        let stored = engine
            .execute(
                "SELECT entity_id, schema_key, schema_version \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_registered_schema' \
               AND entity_id = 'param_schema~1' \
               AND version_id = lix_active_version_id()",
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

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
    register_schema_helper_accepts_explicit_draft_2020_12_schema,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .register_schema(&json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "x-lix-key": "helper_registered_schema_2020_12",
                "x-lix-version": "1",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" }
                },
                "required": ["id", "name"],
                "additionalProperties": false
            }))
            .await
            .unwrap();

        let registered = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = 'helper_registered_schema_2020_12~1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(registered.statements[0].rows, vec![vec![Value::Integer(1)]]);

        let live_table = engine
            .execute("SELECT COUNT(*) FROM helper_registered_schema_2020_12", &[])
            .await
            .unwrap();
        assert_eq!(live_table.statements[0].rows, vec![vec![Value::Integer(0)]]);
    }
);

simulation_test!(
    register_schema_helper_rejects_missing_pointer_slash_with_targeted_hint,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin_transaction_with_options should succeed");

        let err = tx
            .register_schema(&json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "x-lix-key": "helper_registered_schema_bad_pointer",
                "x-lix-version": "1",
                "x-lix-primary-key": ["id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }))
            .await
            .expect_err("missing pointer slash should be rejected");
        tx.rollback().await.unwrap();

        assert_eq!(err.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            err.description.contains("must begin with '/'"),
            "unexpected description: {}",
            err.description
        );
        assert!(
            err.description
                .contains("x-lix-primary-key: \"id\" → \"/id\""),
            "unexpected description: {}",
            err.description
        );
        let hint = err
            .hint
            .as_deref()
            .expect("missing-slash error should carry a hint");
        assert!(hint.contains("/id"), "unexpected hint: {hint}");
        assert!(hint.contains("RFC 6901"), "unexpected hint: {hint}");
    }
);

simulation_test!(
    register_schema_helper_ignores_duplicate_registration_like_direct_insert,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

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
    register_schema_helper_rejects_invalid_cel_default_under_draft_2020_12,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin_transaction_with_options should succeed");

        let err = tx
            .register_schema(&json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "x-lix-key": "helper_registered_schema_bad_cel",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "x-lix-default": "lix_uuid_v7("
                    }
                },
                "additionalProperties": false
            }))
            .await
            .expect_err("invalid CEL default should be rejected");
        tx.rollback().await.unwrap();

        assert_eq!(err.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            err.description.contains("Invalid Lix schema definition"),
            "unexpected description: {}",
            err.description
        );
        assert!(
            err.description.contains("x-lix-default"),
            "unexpected description: {}",
            err.description
        );
        assert!(
            err.description.contains("cel"),
            "unexpected description: {}",
            err.description
        );
    }
);

simulation_test!(
    registered_schema_public_surface_round_trips_inserted_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

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
    registered_schema_requires_foreign_key_targets_are_unique_or_primary,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"parent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-unique\":[[\"/slug\"]],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"slug\",\"name\"],\"additionalProperties\":false}')\
             )", &[])
            .await
            .unwrap();

        let valid = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"child_schema\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"parent_id\":{\"type\":\"string\"}},\"required\":[\"parent_id\"],\"additionalProperties\":false}')\
             )", &[])
            .await;

        assert!(valid.is_ok(), "{valid:?}");

        let invalid = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"bad_child\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_name\"],\"references\":{\"schemaKey\":\"parent_schema\",\"properties\":[\"/name\"]}}],\"type\":\"object\",\"properties\":{\"parent_name\":{\"type\":\"string\"}},\"required\":[\"parent_name\"],\"additionalProperties\":false}')\
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}')\
             )", &[])
            .await
            .unwrap();

        let result = engine
            .execute(
                "UPDATE lix_state_by_version SET snapshot_content = '{\"value\":{\"x-lix-version\":\"1\"}}' \
             WHERE schema_key = 'lix_registered_schema' AND entity_id = 'test_schema~1' AND file_id IS NULL AND version_id = lix_active_version_id()", &[])
            .await;

        let err = result.expect_err("expected registered schema validation error");
        assert!(
            err.to_string()
                .contains("\"x-lix-key\" is a required property"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    registered_schema_raw_string_insert_hints_at_lix_json_wrapper,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ('{\"x-lix-key\":\"book\",\"x-lix-version\":\"1\",\"type\":\"object\",\"x-lix-primary-key\":[\"/id\"],\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')",
                &[],
            )
            .await;

        let err = result.expect_err("inserting a raw JSON string should fail");
        assert!(
            err.description
                .contains("could not extract primary-key field"),
            "expected primary-key extraction error, got: {}",
            err.description
        );
        let hint = err
            .hint
            .as_deref()
            .expect("raw-string insert should attach a hint");
        assert!(
            hint.contains("lix_json"),
            "hint should mention lix_json; got: {hint}"
        );
        assert!(
            !hint.contains("--params"),
            "engine hint must not reference CLI flags; got: {hint}"
        );
    }
);

simulation_test!(
    registered_schema_sqlite_json_hints_at_lix_json_wrapper,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (json('{\"x-lix-key\":\"book\",\"x-lix-version\":\"1\",\"type\":\"object\",\"x-lix-primary-key\":[\"/id\"],\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'))",
                &[],
            )
            .await;

        let err = result.expect_err("inserting via SQLite json() should fail");
        assert_eq!(
            err.code, "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION",
            "SQLite json() rejection should carry the categorized code"
        );
        assert!(
            !err.description.contains("day-1"),
            "error must not leak internal 'day-1' phrasing, got: {}",
            err.description
        );
        let hint = err
            .hint
            .as_deref()
            .expect("SQLite json() insert should attach a hint");
        assert!(
            hint.contains("lix_json"),
            "hint should mention lix_json; got: {hint}"
        );
        assert!(
            !hint.contains("--params"),
            "engine hint must not reference CLI flags; got: {hint}"
        );
    }
);

simulation_test!(
    boolean_field_integer_rejection_hints_at_true_false_literals,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json('{\"x-lix-key\":\"todo\",\"x-lix-version\":\"1\",\"type\":\"object\",\"x-lix-primary-key\":[\"/id\"],\"properties\":{\"id\":{\"type\":\"string\"},\"done\":{\"type\":\"boolean\"}},\"required\":[\"id\",\"done\"],\"additionalProperties\":false}'))",
                &[],
            )
            .await
            .expect("schema registration should succeed");

        let result = engine
            .execute("INSERT INTO todo (id, done) VALUES ('todo-1', 0)", &[])
            .await;

        let err = result.expect_err("inserting integer 0 for boolean field should fail");
        assert!(
            err.description.contains("is not of type") && err.description.contains("boolean"),
            "expected boolean-type validation error, got: {}",
            err.description
        );
        let hint = err
            .hint
            .as_deref()
            .expect("boolean-type mismatch should attach a hint");
        assert!(
            hint.contains("true") && hint.contains("false"),
            "hint should suggest true/false literals; got: {hint}"
        );
        assert!(
            !hint.contains("--params"),
            "engine hint must not reference CLI flags; got: {hint}"
        );
    }
);
