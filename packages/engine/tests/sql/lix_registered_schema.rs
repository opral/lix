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
             true,\
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
                 true,\
                 true\
                 )",
                &[],
            )
            .await
            .expect_err("registered schema insert should reject JSON Pointers without leading slash");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.description.contains("must begin with '/'"),
            "unexpected description: {}",
            error.description
        );
        assert!(
            error
                .description
                .contains("x-lix-primary-key: \"id\" → \"/id\""),
            "description should show the offending primary key pointer: {}",
            error.description
        );
        let hint = error.hint.as_deref().expect("error should include a hint");
        assert!(
            hint.contains("Did you mean [\"/id\"]?"),
            "hint should suggest the JSON Pointer form: {hint}"
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
                 true,\
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

simulation_test!(entity_by_version_expands_global_rows, |sim| async move {
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
             lix_json('{\"x-lix-key\":\"engine2_overlay_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
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
                 true,\
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
