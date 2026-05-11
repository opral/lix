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
             lix_json('{\"x-lix-key\":\"engine_dummy_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
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
                [Value::Json(entity_id), Value::Json(value)]
                    if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema") =>
                {
                    Some(entity_id)
                }
                [Value::Json(entity_id), Value::Text(value)] => {
                    let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                    (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema"))
                    .then_some(entity_id)
                }
                _ => None,
            })
            .expect("registered schema row should be visible");
        assert_eq!(registered_schema_entity_id, &json!(["engine_dummy_schema"]));

        let insert_state_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"dummy-1\"]'), 'engine_dummy_schema', NULL, lix_json('{\"id\":\"dummy-1\",\"name\":\"Dummy\"}'), false, true\
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
             WHERE schema_key = 'engine_dummy_schema' AND entity_id = lix_json('[\"dummy-1\"]')",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Json(json!(["dummy-1"])),
                Value::Text("engine_dummy_schema".to_string()),
                Value::Json(json!({"id": "dummy-1", "name": "Dummy"})),
            ]
        );
    }
);

simulation_test!(
    untracked_registered_schema_does_not_authorize_tracked_state_write,
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
                 lix_json('{\"x-lix-key\":\"engine_untracked_only_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("untracked schema registration should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"tracked-1\"]'), 'engine_untracked_only_schema', NULL, lix_json('{\"id\":\"tracked-1\",\"name\":\"Tracked\"}'), false, false\
                 )",
                &[],
            )
            .await
            .expect_err("tracked rows must not validate against committed untracked schemas");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
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
                 lix_json('{\"x-lix-key\":\"lix_change\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect_err("system schema keys should not be user-registerable");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error.message.contains("system schema"),
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
                 lix_json('{\"x-lix-key\":\"engine_delete_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
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
            [Value::Json(entity_id), Value::Json(value)]
                if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema") =>
            {
                Some(entity_id.clone())
            }
            [Value::Json(entity_id), Value::Text(value)] => {
                let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema"))
                .then_some(entity_id.clone())
            }
            _ => None,
        })
        .expect("registered schema entity id should be discoverable");

    let error = session
        .execute(
            "DELETE FROM lix_registered_schema \
                 WHERE lixcol_entity_id = $1",
            &[Value::Json(delete_schema_entity_id)],
        )
        .await
        .expect_err("schema deletion is not supported yet");

    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    assert!(
        error
            .message
            .contains("delete lix_registered_schema is not supported"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(
    tracked_registered_schema_update_allows_compatible_amendment_and_history,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let initial_schema = json!({
            "x-lix-key": "engine_schema_update_history",
                        "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });
        let amended_schema = json!({
            "x-lix-key": "engine_schema_update_history",
                        "x-lix-primary-key": ["/id"],
            "type": "object",
            "description": "Compatible tracked schema amendment",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" },
                "subtitle": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES ($1, false, false)",
                &[Value::Json(initial_schema.clone())],
            )
            .await
            .expect("tracked schema insert should succeed");
        let first_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");

        session
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_entity_id = lix_json('[\"engine_schema_update_history\"]')",
                &[Value::Json(amended_schema.clone())],
            )
            .await
            .expect("compatible tracked schema amendment should succeed");
        let second_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");
        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT value, lixcol_entity_id, lixcol_observed_commit_id, lixcol_start_commit_id, lixcol_depth \
                     FROM lix_registered_schema_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND lixcol_entity_id = lix_json('[\"engine_schema_update_history\"]') \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("tracked registered schema history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Json(amended_schema),
                    Value::Json(json!(["engine_schema_update_history"])),
                    Value::Text(second_commit_id.clone()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Json(initial_schema),
                    Value::Json(json!(["engine_schema_update_history"])),
                    Value::Text(first_commit_id),
                    Value::Text(second_commit_id),
                    Value::Integer(1),
                ],
            ],
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
                 lix_json('{\"x-lix-key\":\"engine_bad_pointer_schema\",\"x-lix-primary-key\":[\"id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
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
    lix_registered_schema_insert_rejects_unprojectable_entity_property,
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
                 lix_json('{\"x-lix-key\":\"engine_empty_property_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"kind\":{}},\"required\":[\"id\",\"kind\"],\"additionalProperties\":false}'),\
                 true,\
                 false\
                 )",
                &[],
            )
            .await
            .expect_err("registered schema insert should reject properties without a SQL projection type");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("property '/kind'"),
            "message should identify the unprojectable property: {}",
            error.message
        );
        assert!(
            error.message.contains("SQL-projectable JSON Schema type"),
            "message should explain the projection requirement: {}",
            error.message
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
             lix_json('{\"x-lix-key\":\"engine_poison_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("schema should be visible on active main");

        let error = main
            .execute(
                "INSERT INTO engine_poison_schema_by_version \
                 (id, name, lixcol_version_id, lixcol_untracked) \
                 VALUES ('poison-1', 'Poisoned', 'schemaless-target', true)",
                &[],
            )
            .await
            .expect_err("_by_version write must use the target version schema catalog");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine_poison_schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    registered_schema_identity_is_scoped_per_version,
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
             lix_json('{\"x-lix-key\":\"engine_divergent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
                &[],
            )
            .await
            .expect("main schema should be registered");

        let main_schema = json!({
            "x-lix-key": "engine_divergent_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        });
        let target_schema = json!({
            "x-lix-key": "engine_divergent_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });

        let target = sim.wrap_session(
            engine
                .open_session("divergent-target")
                .await
                .expect("target session should open"),
            &engine,
        );

        target
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_divergent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("same schema key may have independent version-local definitions");

        let main_result = main
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = lix_json('[\"engine_divergent_schema\"]')",
                &[],
            )
            .await
            .expect("main schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let target_result = target
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = lix_json('[\"engine_divergent_schema\"]')",
                &[],
            )
            .await
            .expect("target schema read should succeed");
        assert_rows_eq(target_result, vec![vec![Value::Json(target_schema)]]);
    }
);

simulation_test!(
    independent_schema_amendments_on_two_versions_are_allowed,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        let base_schema = json!({
            "x-lix-key": "engine_branch_schema_amendment",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });
        let main_schema = json!({
            "x-lix-key": "engine_branch_schema_amendment",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" },
                "main_note": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });
        let draft_schema = json!({
            "x-lix-key": "engine_branch_schema_amendment",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "title": { "type": "string" },
                "draft_note": { "type": "string" }
            },
            "required": ["id", "title"],
            "additionalProperties": false
        });

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES ($1, false, false)",
            &[Value::Json(base_schema)],
        )
        .await
        .expect("base schema should be registered");

        main.create_version(CreateVersionOptions {
            id: Some("schema-amendment-draft".to_string()),
            name: "Schema Amendment Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft version should be created from base schema");

        let draft = sim.wrap_session(
            engine
                .open_session("schema-amendment-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        let main_update = main
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_entity_id = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[Value::Json(main_schema.clone())],
            )
            .await
            .expect("main additive schema amendment should succeed");
        assert_eq!(main_update, ExecuteResult::from_rows_affected(1));

        let draft_update = draft
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_entity_id = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[Value::Json(draft_schema.clone())],
            )
            .await
            .expect("draft additive schema amendment should succeed");
        assert_eq!(draft_update, ExecuteResult::from_rows_affected(1));

        let main_result = main
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[],
            )
            .await
            .expect("main amended schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let draft_result = draft
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_id = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[],
            )
            .await
            .expect("draft amended schema read should succeed");
        assert_rows_eq(draft_result, vec![vec![Value::Json(draft_schema)]]);
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
             lix_json('{\"x-lix-key\":\"engine_fk_parent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("parent schema should register on active main");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_fk_child_schema\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"engine_fk_parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"parent_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"parent_id\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("child schema should register on active main");

        let parent_result = main
            .execute(
                "INSERT INTO engine_fk_parent_schema_by_version \
                 (id, lixcol_version_id, lixcol_untracked) \
                 VALUES ('parent-1', 'fk-schemaless-target', true)",
                &[],
            )
            .await;

        if let Err(error) = parent_result {
            assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
            assert!(
                error.message.contains("engine_fk_parent_schema"),
                "unexpected error: {error:?}"
            );
            return;
        }

        let error = main
            .execute(
                "INSERT INTO engine_fk_child_schema_by_version \
                 (id, parent_id, lixcol_version_id, lixcol_untracked) \
                 VALUES ('child-1', 'parent-1', 'fk-schemaless-target', true)",
                &[],
            )
            .await
            .expect_err("FK-valid active graph must not be insertable into a schemaless target");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine_fk_child_schema")
                || error.message.contains("engine_fk_parent_schema"),
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
                 lix_json('{\"x-lix-key\":\"engine_default_id_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO engine_default_id_schema (name) VALUES ('Generated')",
                &[],
            )
            .await
            .expect("entity insert should apply defaulted primary key");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT lixcol_entity_id, id, name \
                 FROM engine_default_id_schema \
                 WHERE name = 'Generated'",
                &[],
            )
            .await
            .expect("entity read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Json(entity_id), Value::Text(id), Value::Text(name)] = values else {
            panic!("expected generated id row, got {values:?}");
        };
        assert_eq!(entity_id, &json!([id]));
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
                 lix_json('{\"x-lix-key\":\"engine_nullable_default_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"status\":{\"type\":[\"string\",\"null\"],\"default\":\"computed\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_nullable_default_schema (id, status) \
                 VALUES ('explicit-null', NULL)",
                &[],
            )
            .await
            .expect("entity insert should preserve explicit null");

        session
            .execute(
                "INSERT INTO engine_nullable_default_schema (id) \
                 VALUES ('omitted')",
                &[],
            )
            .await
            .expect("entity insert should apply default for omitted column");

        let result = session
            .execute(
                "SELECT id, status \
                 FROM engine_nullable_default_schema \
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
             lix_json('{\"x-lix-key\":\"engine_overlay_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
             false\
             )",
            &[],
        )
        .await
        .expect("global registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_overlay_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO engine_overlay_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('entity-global-overlay', 'Global Entity', true, false)",
            &[],
        )
        .await
        .expect("global entity insert should succeed");

    let result = session
        .execute(
            "SELECT id, name, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM engine_overlay_schema_by_version \
                 WHERE lixcol_entity_id = lix_json('[\"entity-global-overlay\"]') \
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
                 lix_json('{\"x-lix-key\":\"engine_global_poison_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("main-local schema registration should succeed");

        let error = session
            .execute(
                "INSERT INTO engine_global_poison_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('global-poison-1', 'Wrong Scope', true, false)",
                &[],
            )
            .await
            .expect_err("global writes must validate through the global schema catalog");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine_global_poison_schema"),
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
                 lix_json('{\"x-lix-key\":\"engine_typed_entity_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"},\"count\":{\"type\":\"number\"}},\"required\":[\"id\",\"name\",\"count\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO engine_typed_entity_schema \
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
                 FROM engine_typed_entity_schema \
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
                Value::Json(json!(["typed-entity-1"])),
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
                 lix_json('{\"x-lix-key\":\"engine_number_update_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"score\":{\"type\":\"number\"}},\"required\":[\"id\",\"score\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_number_update_schema \
                 (id, score, lixcol_global, lixcol_untracked) \
                 VALUES ('score-1', 1, false, false)",
                &[],
            )
            .await
            .expect("typed entity insert should accept integer literal for number column");

        session
            .execute(
                "UPDATE engine_number_update_schema \
                 SET score = $1 \
                 WHERE id = 'score-1'",
                &[Value::Integer(52000)],
            )
            .await
            .expect("typed entity update should accept integer param for number column");

        let result = session
            .execute(
                "SELECT score \
                 FROM engine_number_update_schema \
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
                 lix_json('{\"x-lix-key\":\"engine_optional_update_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"rank\":{\"type\":\"integer\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_optional_update_schema \
                 (id, title, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', false, false)",
                &[],
            )
            .await
            .expect("insert should omit the optional rank field");

        session
            .execute(
                "UPDATE engine_optional_update_schema \
                 SET title = 'after' \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("update should preserve absent optional fields");

        let result = session
            .execute(
                "SELECT title, rank, lixcol_snapshot_content \
                 FROM engine_optional_update_schema \
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
                "UPDATE engine_optional_update_schema \
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
