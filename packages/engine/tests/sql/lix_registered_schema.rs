use lix_engine::CreateBranchOptions;
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
                "SELECT lixcol_entity_pk, value \
                 FROM lix_registered_schema",
                &[],
            )
            .await
            .expect("registered schema read should succeed");
        let registered_schema_rows = registered_schema_row;
        let registered_schema_entity_pk = registered_schema_rows
            .rows()
            .iter()
            .find_map(|row| match row.values() {
                [Value::Json(entity_pk), Value::Json(value)]
                    if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema") =>
                {
                    Some(entity_pk)
                }
                [Value::Json(entity_pk), Value::Text(value)] => {
                    let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                    (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema"))
                    .then_some(entity_pk)
                }
                _ => None,
            })
            .expect("registered schema row should be visible");
        assert_eq!(registered_schema_entity_pk, &json!(["engine_dummy_schema"]));

        let insert_state_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
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
                "SELECT entity_pk, schema_key, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'engine_dummy_schema' AND entity_pk = lix_json('[\"dummy-1\"]')",
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
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
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
            "SELECT lixcol_entity_pk, value \
                 FROM lix_registered_schema",
            &[],
        )
        .await
        .expect("registered schema read should succeed");
    let delete_schema_entity_pk = registered_schema_rows
        .rows()
        .iter()
        .find_map(|row| match row.values() {
            [Value::Json(entity_pk), Value::Json(value)]
                if value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema") =>
            {
                Some(entity_pk.clone())
            }
            [Value::Json(entity_pk), Value::Text(value)] => {
                let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                (value.get("x-lix-key").and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema"))
                .then_some(entity_pk.clone())
            }
            _ => None,
        })
        .expect("registered schema entity pk should be discoverable");

    let error = session
        .execute(
            "DELETE FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = $1",
            &[Value::Json(delete_schema_entity_pk)],
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");

        session
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_schema_update_history\"]')",
                &[Value::Json(amended_schema.clone())],
            )
            .await
            .expect("compatible tracked schema amendment should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");
        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT value, lixcol_entity_pk, lixcol_observed_commit_id, lixcol_start_commit_id, lixcol_depth \
                     FROM lix_registered_schema_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND lixcol_entity_pk = lix_json('[\"engine_schema_update_history\"]') \
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
    entity_by_branch_insert_rejects_target_branch_without_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_branch(CreateBranchOptions {
            id: Some("schemaless-target".to_string()),
            name: "Schemaless Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target branch should be created before schema registration");

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
                "INSERT INTO engine_poison_schema_by_branch \
                 (id, name, lixcol_branch_id, lixcol_untracked) \
                 VALUES ('poison-1', 'Poisoned', 'schemaless-target', true)",
                &[],
            )
            .await
            .expect_err("_by_branch write must use the target branch schema catalog");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("engine_poison_schema"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    registered_schema_identity_is_scoped_per_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_branch(CreateBranchOptions {
            id: Some("divergent-target".to_string()),
            name: "Divergent Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target branch should be created before schema divergence");

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
            .expect("same schema key may have independent branch-local definitions");

        let main_result = main
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_divergent_schema\"]')",
                &[],
            )
            .await
            .expect("main schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let target_result = target
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_divergent_schema\"]')",
                &[],
            )
            .await
            .expect("target schema read should succeed");
        assert_rows_eq(target_result, vec![vec![Value::Json(target_schema)]]);
    }
);

simulation_test!(
    independent_schema_amendments_on_two_branches_are_allowed,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
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

        main.create_branch(CreateBranchOptions {
            id: Some("schema-amendment-draft".to_string()),
            name: "Schema Amendment Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created from base schema");

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
                 WHERE lixcol_entity_pk = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[Value::Json(main_schema.clone())],
            )
            .await
            .expect("main additive schema amendment should succeed");
        assert_eq!(main_update, ExecuteResult::from_rows_affected(1));

        let draft_update = draft
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[Value::Json(draft_schema.clone())],
            )
            .await
            .expect("draft additive schema amendment should succeed");
        assert_eq!(draft_update, ExecuteResult::from_rows_affected(1));

        let main_result = main
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[],
            )
            .await
            .expect("main amended schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let draft_result = draft
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = lix_json('[\"engine_branch_schema_amendment\"]')",
                &[],
            )
            .await
            .expect("draft amended schema read should succeed");
        assert_rows_eq(draft_result, vec![vec![Value::Json(draft_schema)]]);
    }
);

simulation_test!(
    entity_by_branch_insert_rejects_fk_graph_when_target_branch_lacks_schemas,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_branch(CreateBranchOptions {
            id: Some("fk-schemaless-target".to_string()),
            name: "FK Schemaless Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target branch should be created before FK schemas");

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
                "INSERT INTO engine_fk_parent_schema_by_branch \
                 (id, lixcol_branch_id, lixcol_untracked) \
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
                "INSERT INTO engine_fk_child_schema_by_branch \
                 (id, parent_id, lixcol_branch_id, lixcol_untracked) \
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
                "SELECT lixcol_entity_pk, id, name \
                 FROM engine_default_id_schema \
                 WHERE name = 'Generated'",
                &[],
            )
            .await
            .expect("entity read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Json(entity_pk), Value::Text(id), Value::Text(name)] = values else {
            panic!("expected generated id row, got {values:?}");
        };
        assert_eq!(entity_pk, &json!([id]));
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

simulation_test!(entity_by_branch_expands_global_rows, |sim| async move {
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
            "SELECT id, name, lixcol_branch_id, lixcol_global, lixcol_untracked \
                 FROM engine_overlay_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"entity-global-overlay\"]') \
                 ORDER BY lixcol_branch_id",
            &[],
        )
        .await
        .expect("entity by-branch read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("entity-global-overlay".to_string()),
                Value::Text("Global Entity".to_string()),
                Value::Text(sim.main_branch_id().to_string()),
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
                "SELECT id, name, count, lixcol_entity_pk \
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
    typed_entity_update_accepts_file_id_predicate,
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
                 lix_json('{\"x-lix-key\":\"engine_file_scoped_entity_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES \
                 ('file-1', '/file-1.txt', X'31'), \
                 ('file-2', '/file-2.txt', X'32')",
                &[],
            )
            .await
            .expect("file inserts should succeed");

        session
            .execute(
                "INSERT INTO engine_file_scoped_entity_schema \
                 (id, name, lixcol_file_id, lixcol_global, lixcol_untracked) \
                 VALUES \
                 ('row-1', 'before-1', 'file-1', false, false), \
                 ('row-2', 'before-2', 'file-2', false, false)",
                &[],
            )
            .await
            .expect("typed entity inserts with file ids should succeed");

        let update = session
            .execute(
                "UPDATE engine_file_scoped_entity_schema \
                 SET name = 'after' \
                 WHERE lixcol_file_id = 'file-1'",
                &[],
            )
            .await
            .expect("file id should be accepted in an entity write predicate");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, name, lixcol_file_id \
                 FROM engine_file_scoped_entity_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("entity file id should be readable");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("row-1".to_string()),
                    Value::Text("after".to_string()),
                    Value::Text("file-1".to_string()),
                ],
                vec![
                    Value::Text("row-2".to_string()),
                    Value::Text("before-2".to_string()),
                    Value::Text("file-2".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    typed_entity_update_accepts_parseable_json_text_identity_predicate,
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
                 lix_json('{\"x-lix-key\":\"engine_identity_literal_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("schema registration should succeed");

        session
            .execute(
                "INSERT INTO engine_identity_literal_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', false, false)",
                &[],
            )
            .await
            .expect("typed entity insert should succeed");

        let update = session
            .execute(
                "UPDATE engine_identity_literal_schema \
                 SET name = 'after' \
                 WHERE lixcol_entity_pk = '[\"row-1\"]'",
                &[],
            )
            .await
            .expect("parseable JSON text identity predicate should be accepted");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT name FROM engine_identity_literal_schema WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("updated typed entity should read");
        assert_rows_eq(result, vec![vec![Value::Text("after".to_string())]]);
    }
);

simulation_test!(
    typed_entity_update_accepts_parseable_json_text_identity_in_predicate,
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
                 lix_json('{\"x-lix-key\":\"engine_identity_in_literal_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("schema registration should succeed");

        session
            .execute(
                "INSERT INTO engine_identity_in_literal_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', false, false)",
                &[],
            )
            .await
            .expect("typed entity insert should succeed");

        let update = session
            .execute(
                "UPDATE engine_identity_in_literal_schema \
                 SET name = 'after' \
                 WHERE lixcol_entity_pk IN ('[\"row-1\"]')",
                &[],
            )
            .await
            .expect("parseable JSON text identity IN predicate should be accepted");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT name FROM engine_identity_in_literal_schema WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("updated typed entity should read");
        assert_rows_eq(result, vec![vec![Value::Text("after".to_string())]]);
    }
);

simulation_test!(
    typed_entity_base_update_cannot_override_active_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_branch_filter_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("base-filter-draft".to_string()),
            name: "Base Filter Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let draft = sim.wrap_session(
            engine
                .open_session("base-filter-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        draft
            .execute(
                "INSERT INTO engine_base_branch_filter_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft', false, false)",
                &[],
            )
            .await
            .expect("draft entity insert should succeed");

        let error = main
            .execute(
                "UPDATE engine_base_branch_filter_schema \
                 SET name = 'main-updated-draft' \
                 WHERE lixcol_entity_pk = '[\"row-1\"]' \
                   AND lixcol_branch_id = 'base-filter-draft'",
                &[],
            )
            .await
            .expect_err("base entity table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = main
            .execute(
                "SELECT name \
                 FROM engine_base_branch_filter_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"row-1\"]') \
                   AND lixcol_branch_id = 'base-filter-draft'",
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(result, vec![vec![Value::Text("draft".to_string())]]);
    }
);

simulation_test!(
    typed_entity_base_insert_cannot_override_active_branch_scope,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_insert_branch_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("base-insert-draft".to_string()),
            name: "Base Insert Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let error = main
            .execute(
                "INSERT INTO engine_base_insert_branch_schema \
                 (id, name, lixcol_branch_id, lixcol_untracked) \
                 VALUES ('row-1', 'draft', 'base-insert-draft', false)",
                &[],
            )
            .await
            .expect_err("base entity table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = main
            .execute(
                "SELECT name \
                 FROM engine_base_insert_branch_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"row-1\"]') \
                   AND lixcol_branch_id = 'base-insert-draft'",
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(result, vec![]);
    }
);

simulation_test!(
    typed_entity_insert_rejects_unknown_column,
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
                 lix_json('{\"x-lix-key\":\"engine_unknown_insert_column_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let error = session
            .execute(
                "INSERT INTO engine_unknown_insert_column_schema \
                 (id, name, missing_column, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', 'ignored-before-fix', false, false)",
                &[],
            )
            .await
            .expect_err("typed entity insert should not ignore unknown columns");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = session
            .execute("SELECT id FROM engine_unknown_insert_column_schema", &[])
            .await
            .expect("select should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_entity_insert_rejects_duplicate_columns,
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
                 lix_json('{\"x-lix-key\":\"engine_duplicate_insert_column_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let error = session
            .execute(
                "INSERT INTO engine_duplicate_insert_column_schema \
                 (id, name, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', 'after', false, false)",
                &[],
            )
            .await
            .expect_err("typed entity insert should not accept duplicate columns");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let result = session
            .execute("SELECT id FROM engine_duplicate_insert_column_schema", &[])
            .await
            .expect("select should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_entity_insert_rejects_unresolved_qualified_table,
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
                 lix_json('{\"x-lix-key\":\"engine_qualified_insert_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO bogus.engine_qualified_insert_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'wrong', false, false)",
                &[],
            )
            .await
            .expect_err("qualified unresolved table should fall back to normal planning");

        let result = session
            .execute("SELECT id FROM engine_qualified_insert_schema", &[])
            .await
            .expect("select should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_entity_base_insert_cannot_override_active_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_branch_insert_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("base-insert-draft".to_string()),
            name: "Base Insert Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let error = main
            .execute(
                "INSERT INTO engine_base_branch_insert_schema \
                 (id, name, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft-via-main', 'base-insert-draft', false, false)",
                &[],
            )
            .await
            .expect_err("base entity table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = main
            .execute(
                "SELECT id \
                 FROM engine_base_branch_insert_schema_by_branch \
                 WHERE lixcol_branch_id = 'base-insert-draft'",
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_entity_by_branch_delete_requires_explicit_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_delete_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("by-branch-delete-draft".to_string()),
            name: "By-branch Delete Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        main.execute(
            "INSERT INTO engine_by_branch_delete_scope_schema \
             (id, name, lixcol_global, lixcol_untracked) \
             VALUES ('row-1', 'main', false, false)",
            &[],
        )
        .await
        .expect("main entity insert should succeed");

        let draft = sim.wrap_session(
            engine
                .open_session("by-branch-delete-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );
        draft
            .execute(
                "INSERT INTO engine_by_branch_delete_scope_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft', false, false)",
                &[],
            )
            .await
            .expect("draft entity insert should succeed");

        main.execute(
            "DELETE FROM engine_by_branch_delete_scope_schema_by_branch \
             WHERE lixcol_entity_pk = '[\"row-1\"]'",
            &[],
        )
        .await
        .expect_err("_by_branch delete should not delete all branches without a branch filter");

        let result = main
            .execute(
                &format!(
                    "SELECT name, lixcol_branch_id \
                 FROM engine_by_branch_delete_scope_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"row-1\"]') \
                   AND lixcol_branch_id IN ('{}', 'by-branch-delete-draft') \
                 ORDER BY name",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("draft".to_string()),
                    Value::Text("by-branch-delete-draft".to_string()),
                ],
                vec![
                    Value::Text("main".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    typed_entity_by_branch_update_requires_explicit_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_update_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("by-branch-update-draft".to_string()),
            name: "By-branch Update Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        main.execute(
            "INSERT INTO engine_by_branch_update_scope_schema \
             (id, name, lixcol_global, lixcol_untracked) \
             VALUES ('row-1', 'main', false, false)",
            &[],
        )
        .await
        .expect("main entity insert should succeed");

        let draft = sim.wrap_session(
            engine
                .open_session("by-branch-update-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );
        draft
            .execute(
                "INSERT INTO engine_by_branch_update_scope_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft', false, false)",
                &[],
            )
            .await
            .expect("draft entity insert should succeed");

        main.execute(
            "UPDATE engine_by_branch_update_scope_schema_by_branch \
             SET name = 'updated-all' \
             WHERE lixcol_entity_pk = '[\"row-1\"]'",
            &[],
        )
        .await
        .expect_err("_by_branch update should not update all branches without a branch filter");

        let result = main
            .execute(
                &format!(
                    "SELECT name, lixcol_branch_id \
                 FROM engine_by_branch_update_scope_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"row-1\"]') \
                   AND lixcol_branch_id IN ('{}', 'by-branch-update-draft') \
                 ORDER BY name",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("draft".to_string()),
                    Value::Text("by-branch-update-draft".to_string()),
                ],
                vec![
                    Value::Text("main".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    typed_entity_by_branch_dml_rejects_branch_id_alias,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_alias_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("by-branch-alias-draft".to_string()),
            name: "By-branch Alias Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        main.execute(
            "INSERT INTO engine_by_branch_alias_scope_schema \
             (id, name, lixcol_global, lixcol_untracked) \
             VALUES ('row-1', 'main', false, false)",
            &[],
        )
        .await
        .expect("main entity insert should succeed");

        let draft = sim.wrap_session(
            engine
                .open_session("by-branch-alias-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );
        draft
            .execute(
                "INSERT INTO engine_by_branch_alias_scope_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft', false, false)",
                &[],
            )
            .await
            .expect("draft entity insert should succeed");

        let update_error = main
            .execute(
                "UPDATE engine_by_branch_alias_scope_schema_by_branch \
                 SET name = 'updated-via-alias' \
                 WHERE lixcol_entity_pk = '[\"row-1\"]' \
                   AND branch_id = 'by-branch-alias-draft'",
                &[],
            )
            .await
            .expect_err("_by_branch update should not accept branch_id alias");
        assert_eq!(update_error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let delete_error = main
            .execute(
                "DELETE FROM engine_by_branch_alias_scope_schema_by_branch \
                 WHERE lixcol_entity_pk = '[\"row-1\"]' \
                   AND branch_id = 'by-branch-alias-draft'",
                &[],
            )
            .await
            .expect_err("_by_branch delete should not accept branch_id alias");
        assert_eq!(delete_error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = main
            .execute(
                &format!(
                    "SELECT name, lixcol_branch_id \
                 FROM engine_by_branch_alias_scope_schema_by_branch \
                 WHERE lixcol_entity_pk = lix_json('[\"row-1\"]') \
                   AND lixcol_branch_id IN ('{}', 'by-branch-alias-draft') \
                 ORDER BY name",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("by-branch query should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("draft".to_string()),
                    Value::Text("by-branch-alias-draft".to_string()),
                ],
                vec![
                    Value::Text("main".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    typed_entity_update_rejects_duplicate_assignments,
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
                 lix_json('{\"x-lix-key\":\"engine_duplicate_update_assignment_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_duplicate_update_assignment_schema \
                 (id, name, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'before', false, false)",
                &[],
            )
            .await
            .expect("entity insert should succeed");

        let error = session
            .execute(
                "UPDATE engine_duplicate_update_assignment_schema \
                 SET name = 'first', name = 'second' \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect_err("typed entity update should not accept duplicate assignments");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let result = session
            .execute(
                "SELECT name FROM engine_duplicate_update_assignment_schema WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("select should succeed");
        assert_rows_eq(result, vec![vec![Value::Text("before".to_string())]]);
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
