use lix_engine::CreateVersionOptions;
use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_registered_schema_insert_makes_schema_visible_to_lix_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let register_schema_result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_dummy_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");
        assert_eq!(register_schema_result, ExecuteResult::from_rows_affected(1));

        let registered_schema_row = session
            .execute(
                "SELECT lixcol_entity_id, value \
                 FROM lix_registered_schema",
                &[],
            )
            .await
            .expect("registered schema read should succeed");
        let registered_schema_rows = registered_schema_row;
        let registered_schema_entity_id = registered_schema_rows
            .rows()
            .iter()
            .find_map(|row| match row.values() {
                [Value::Text(entity_id), Value::Json(value)]
                    if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine2_dummy_schema") =>
                {
                    Some(entity_id)
                }
                [Value::Text(entity_id), Value::Text(value)] => {
                    let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                    (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine2_dummy_schema"))
                    .then_some(entity_id)
                }
                _ => None,
            })
            .expect("registered schema row should be visible");
        assert!(registered_schema_entity_id.starts_with("pk:v1:"));
        assert_ne!(registered_schema_entity_id, "engine2_dummy_schema~1");

        let insert_state_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'dummy-1', 'engine2_dummy_schema', NULL, lix_json('{\"id\":\"dummy-1\",\"name\":\"Dummy\"}'), '1', false, true\
             )",
            &[],
        )
        .await
        .expect("lix_state insert for registered schema should succeed");
        assert_eq!(insert_state_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT entity_id, schema_key, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'engine2_dummy_schema' AND entity_id = 'dummy-1'",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("dummy-1".to_string()),
                Value::Text("engine2_dummy_schema".to_string()),
                Value::Json(json!({"id": "dummy-1", "name": "Dummy"})),
            ]
        );
    }
);

simulation_test!(
    lix_registered_schema_insert_rejects_system_schema_key,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"lix_change\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect_err("system schema keys should not be user-registerable");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("system schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_registered_schema_insert_rejects_schema_version_above_one,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_future_schema\",\"x-lix-version\":\"2\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect_err("schema evolution should not be accepted yet");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .message
                .contains("schema evolution is not supported yet"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(lix_registered_schema_delete_is_rejected, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_delete_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("schema should register before delete attempt");

    let registered_schema_rows = session
        .execute(
            "SELECT lixcol_entity_id, value \
                 FROM lix_registered_schema",
            &[],
        )
        .await
        .expect("registered schema read should succeed");
    let delete_schema_entity_id = registered_schema_rows
        .rows()
        .iter()
        .find_map(|row| match row.values() {
            [Value::Text(entity_id), Value::Json(value)]
                if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine2_delete_schema") =>
            {
                Some(entity_id.clone())
            }
            [Value::Text(entity_id), Value::Text(value)] => {
                let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine2_delete_schema"))
                .then_some(entity_id.clone())
            }
            _ => None,
        })
        .expect("registered schema entity id should be discoverable");

    let error = session
        .execute(
            "DELETE FROM lix_registered_schema \
                 WHERE lixcol_entity_id = $1",
            &[Value::Text(delete_schema_entity_id)],
        )
        .await
        .expect_err("schema deletion is not supported yet");

    assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    assert!(
        error.message.contains("schema deletion is not supported"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(
    lix_registered_schema_insert_rejects_primary_key_without_json_pointer_slash,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_bad_pointer_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect_err("registered schema insert should reject JSON Pointers without leading slash");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("must begin with '/'"),
            "unexpected message: {}",
            error.message
        );
        assert!(
            error
                .message
                .contains("x-lix-primary-key: \"id\" → \"/id\""),
            "message should show the offending primary key pointer: {}",
            error.message
        );
        let hint = error.hint.as_deref().expect("error should include a hint");
        assert!(
            hint.contains("Did you mean [\"/id\"]?"),
            "hint should suggest the JSON Pointer form: {hint}"
        );
    }
);

simulation_test!(
    entity_by_version_insert_rejects_target_version_without_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_version(CreateVersionOptions {
            id: Some("schemaless-target".to_string()),
            name: "Schemaless Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target version should be created before schema registration");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_poison_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("schema should be visible on active main");

        let error = main
            .execute(
                "INSERT INTO engine2_poison_schema_by_version \
                 (id, name, lixcol_version_id, lixcol_untracked) \
                 VALUES ('poison-1', 'Poisoned', 'schemaless-target', true)",
                &[],
            )
            .await
            .expect_err("_by_version write must use the target version schema catalog");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine2_poison_schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    registered_schema_insert_rejects_divergent_same_key_version_shape,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_version(CreateVersionOptions {
            id: Some("divergent-target".to_string()),
            name: "Divergent Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target version should be created before schema divergence");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_divergent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("main schema should be registered");

        let target = sim.wrap_session(
            engine
                .open_session("divergent-target")
                .await
                .expect("target session should open"),
            &engine,
        );

        let error = target
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_divergent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect_err("same schema key/version must have one canonical shape");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .message
                .contains("already registered with a different definition"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    entity_by_version_insert_rejects_fk_graph_when_target_version_lacks_schemas,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_version(CreateVersionOptions {
            id: Some("fk-schemaless-target".to_string()),
            name: "FK Schemaless Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target version should be created before FK schemas");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_fk_parent_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("parent schema should register on active main");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_fk_child_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"engine2_fk_parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"parent_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"parent_id\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("child schema should register on active main");

        let parent_result = main
            .execute(
                "INSERT INTO engine2_fk_parent_schema_by_version \
                 (id, lixcol_version_id, lixcol_untracked) \
                 VALUES ('parent-1', 'fk-schemaless-target', true)",
                &[],
            )
            .await;

        if let Err(error) = parent_result {
            assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
            assert!(
                error.message.contains("engine2_fk_parent_schema"),
                "unexpected error: {error:?}"
            );
            return;
        }

        let error = main
            .execute(
                "INSERT INTO engine2_fk_child_schema_by_version \
                 (id, parent_id, lixcol_version_id, lixcol_untracked) \
                 VALUES ('child-1', 'parent-1', 'fk-schemaless-target', true)",
                &[],
            )
            .await
            .expect_err("FK-valid active graph must not be insertable into a schemaless target");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine2_fk_child_schema")
                || error.message.contains("engine2_fk_parent_schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    registered_entity_insert_applies_defaulted_primary_key,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_default_id_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO engine2_default_id_schema (name) VALUES ('Generated')",
                &[],
            )
            .await
            .expect("entity insert should apply defaulted primary key");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT lixcol_entity_id, id, name \
                 FROM engine2_default_id_schema \
                 WHERE name = 'Generated'",
                &[],
            )
            .await
            .expect("entity read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(entity_id), Value::Text(id), Value::Text(name)] = values else {
            panic!("expected generated id row, got {values:?}");
        };
        assert_eq!(entity_id, id);
        assert!(!id.is_empty(), "defaulted id should be non-empty");
        assert_eq!(name, "Generated");
    }
);

simulation_test!(
    registered_entity_insert_preserves_explicit_null_for_defaulted_column,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_nullable_default_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"status\":{\"type\":[\"string\",\"null\"],\"default\":\"computed\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine2_nullable_default_schema (id, status) \
                 VALUES ('explicit-null', NULL)",
                &[],
            )
            .await
            .expect("entity insert should preserve explicit null");

        session
            .execute(
                "INSERT INTO engine2_nullable_default_schema (id) \
                 VALUES ('omitted')",
                &[],
            )
            .await
            .expect("entity insert should apply default for omitted column");

        let result = session
            .execute(
                "SELECT id, status \
                 FROM engine2_nullable_default_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("entity read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![Value::Text("explicit-null".to_string()), Value::Null],
                vec![
                    Value::Text("omitted".to_string()),
                    Value::Text("computed".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(entity_by_version_expands_global_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let global_session = sim.wrap_session(
        engine
            .open_session("global")
            .await
            .expect("global session should open"),
        &engine,
    );
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    global_session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_overlay_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
             true\
             )",
            &[],
        )
        .await
        .expect("global registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_overlay_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO engine2_overlay_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('entity-global-overlay', 'Global Entity', true, false)",
            &[],
        )
        .await
        .expect("global entity insert should succeed");

    let result = session
        .execute(
            "SELECT id, name, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM engine2_overlay_schema_by_version \
                 WHERE lixcol_entity_id = 'entity-global-overlay' \
                 ORDER BY lixcol_version_id",
            &[],
        )
        .await
        .expect("entity by-version read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("entity-global-overlay".to_string()),
                Value::Text("Global Entity".to_string()),
                Value::Text(sim.main_version_id().to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
            vec![
                Value::Text("entity-global-overlay".to_string()),
                Value::Text("Global Entity".to_string()),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
        ],
    );
});

simulation_test!(
    global_entity_insert_rejects_active_only_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_global_poison_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("main-local schema registration should succeed");

        let error = session
            .execute(
                "INSERT INTO engine2_global_poison_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('global-poison-1', 'Wrong Scope', true, false)",
                &[],
            )
            .await
            .expect_err("global writes must validate through the global schema catalog");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine2_global_poison_schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    registered_typed_entity_surface_uses_primary_key_columns,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_typed_entity_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"},\"count\":{\"type\":\"number\"}},\"required\":[\"id\",\"name\",\"count\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO engine2_typed_entity_schema \
                 (id, name, count, lixcol_global, lixcol_untracked) \
                 VALUES ('typed-entity-1', 'Typed Entity', 7, false, false)",
                &[],
            )
            .await
            .expect("typed entity insert should succeed");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, name, count, lixcol_entity_id \
                 FROM engine2_typed_entity_schema \
                 WHERE id = 'typed-entity-1'",
                &[],
            )
            .await
            .expect("typed entity query by primary-key column should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("typed-entity-1".to_string()),
                Value::Text("Typed Entity".to_string()),
                Value::Real(7.0),
                Value::Text("typed-entity-1".to_string()),
            ]],
        );
    }
);

simulation_test!(
    typed_entity_number_update_accepts_integer_param_like_insert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_number_update_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"score\":{\"type\":\"number\"}},\"required\":[\"id\",\"score\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine2_number_update_schema \
                 (id, score, lixcol_global, lixcol_untracked) \
                 VALUES ('score-1', 1, false, false)",
                &[],
            )
            .await
            .expect("typed entity insert should accept integer literal for number column");

        session
            .execute(
                "UPDATE engine2_number_update_schema \
                 SET score = $1 \
                 WHERE id = 'score-1'",
                &[Value::Integer(52000)],
            )
            .await
            .expect("typed entity update should accept integer param for number column");

        let result = session
            .execute(
                "SELECT score \
                 FROM engine2_number_update_schema \
                 WHERE id = 'score-1'",
                &[],
            )
            .await
            .expect("typed entity query should succeed");
        assert_rows_eq(result, vec![vec![Value::Real(52000.0)]]);
    }
);

simulation_test!(
    typed_entity_update_preserves_absent_optional_non_nullable_fields,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine2_optional_update_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"rank\":{\"type\":\"integer\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine2_optional_update_schema \
                 (id, title, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', false, false)",
                &[],
            )
            .await
            .expect("insert should omit the optional rank field");

        session
            .execute(
                "UPDATE engine2_optional_update_schema \
                 SET title = 'after' \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("update should preserve absent optional fields");

        let result = session
            .execute(
                "SELECT title, rank, lixcol_snapshot_content \
                 FROM engine2_optional_update_schema \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("typed entity query should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("after".to_string()),
                Value::Null,
                Value::Json(json!({"id": "row-1", "title": "after"})),
            ]],
        );

        let error = session
            .execute(
                "UPDATE engine2_optional_update_schema \
                 SET rank = NULL \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect_err("explicit NULL should still be validated as JSON null");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .message
                .contains("/rank null is not of type \"integer\""),
            "expected rank validation error, got {error:?}"
        );
    }
);
