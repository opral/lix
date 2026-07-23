use lix_engine::CreateBranchOptions;
use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_schema_definition_insert_makes_typed_schema_surface_visible,
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_dummy_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");
        assert_eq!(register_schema_result, ExecuteResult::from_rows_affected(1));

        let registered_schema_row = session
            .execute(
                "SELECT key, definition \
                 FROM lix_schema_definition \
                 WHERE key = 'engine_dummy_schema'",
                &[],
            )
            .await
            .expect("registered schema read should succeed");
        assert_rows_eq(
            registered_schema_row,
            vec![vec![
                Value::Text("engine_dummy_schema".to_string()),
                Value::Json(json!({
                    "x-lix-key": "engine_dummy_schema",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "name": { "type": "string" }
                    },
                    "required": ["id", "name"],
                    "additionalProperties": false
                })),
            ]],
        );

        let insert_state_result = session
            .execute(
                "INSERT INTO engine_dummy_schema (id, name, lixcol_untracked) \
             VALUES ('dummy-1', 'Dummy', true)",
                &[],
            )
            .await
            .expect("typed insert for registered schema should succeed");
        assert_eq!(insert_state_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, name \
             FROM engine_dummy_schema \
             WHERE id = 'dummy-1'",
                &[],
            )
            .await
            .expect("typed read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("dummy-1".to_string()),
                Value::Text("Dummy".to_string()),
            ]
        );
    }
);

simulation_test!(
    lix_schema_definition_batches_foreign_key_dependencies,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let parent = json!({
            "x-lix-key": "batch_parent",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        let child = json!({
            "x-lix-key": "batch_child",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/parent_id"],
                "references": {
                    "schemaKey": "batch_parent",
                    "properties": ["/id"]
                }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "parent_id": { "type": "string" }
            },
            "required": ["id", "parent_id"],
            "additionalProperties": false
        });

        let reverse_order = session
            .execute(
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES ($1), ($2)",
                &[Value::Json(child), Value::Json(parent)],
            )
            .await
            .expect("one schema batch should resolve dependencies independent of row order");
        assert_eq!(reverse_order, ExecuteResult::from_rows_affected(2));

        let left = json!({
            "x-lix-key": "batch_mutual_left",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/right_id"],
                "references": {
                    "schemaKey": "batch_mutual_right",
                    "properties": ["/id"]
                }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "right_id": { "type": "string" }
            },
            "required": ["id", "right_id"],
            "additionalProperties": false
        });
        let right = json!({
            "x-lix-key": "batch_mutual_right",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/left_id"],
                "references": {
                    "schemaKey": "batch_mutual_left",
                    "properties": ["/id"]
                }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "left_id": { "type": "string" }
            },
            "required": ["id", "left_id"],
            "additionalProperties": false
        });

        let mutual = session
            .execute(
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES ($1), ($2)",
                &[Value::Json(left), Value::Json(right)],
            )
            .await
            .expect("one schema batch should compile mutually dependent definitions atomically");
        assert_eq!(mutual, ExecuteResult::from_rows_affected(2));

        let registered = session
            .execute(
                "SELECT key FROM lix_schema \
                 WHERE key LIKE 'batch_%' \
                 ORDER BY key",
                &[],
            )
            .await
            .expect("every schema in both dependency batches should be visible");
        assert_rows_eq(
            registered,
            vec![
                vec![Value::Text("batch_child".to_string())],
                vec![Value::Text("batch_mutual_left".to_string())],
                vec![Value::Text("batch_mutual_right".to_string())],
                vec![Value::Text("batch_parent".to_string())],
            ],
        );
    }
);

simulation_test!(
    lix_schema_exposes_one_semantic_contract_for_discovery_and_introspection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let definition = json!({
            "x-lix-key": "app_catalog_contract",
            "x-lix-primary-key": ["/tenant", "/id"],
            "type": "object",
            "properties": {
                "tenant": { "type": "string" },
                "id": {
                    "type": "string",
                    "x-lix-default": "lix_uuid_v7()"
                },
                "payload": { "type": ["object", "null"] },
                "score": { "type": "number" }
            },
            "required": ["tenant", "id", "score"],
            "additionalProperties": false
        });

        session
            .execute(
                "INSERT INTO lix_schema_definition (definition) VALUES ($1)",
                &[Value::Json(definition.clone())],
            )
            .await
            .expect("schema definition should register");

        let result = session
            .execute(
                "SELECT key, table_name, by_branch_table_name, history_table_name, \
                        primary_key, columns, surfaces, definition \
                 FROM lix_schema \
                 WHERE key = 'app_catalog_contract'",
                &[],
            )
            .await
            .expect("semantic schema catalog should be queryable");
        let [row] = result.rows() else {
            panic!("expected one schema catalog row, got {}", result.len());
        };
        let [
            Value::Text(key),
            Value::Text(table_name),
            Value::Text(by_branch_table_name),
            Value::Text(history_table_name),
            Value::Json(primary_key),
            Value::Json(columns),
            Value::Json(surfaces),
            Value::Json(returned_definition),
        ] = row.values()
        else {
            panic!("unexpected schema catalog row: {:?}", row.values());
        };
        assert_eq!(key, "app_catalog_contract");
        assert_eq!(table_name, "app_catalog_contract");
        assert_eq!(by_branch_table_name, "app_catalog_contract_by_branch");
        assert_eq!(history_table_name, "app_catalog_contract_history");
        assert_eq!(primary_key, &json!(["/tenant", "/id"]));
        assert_eq!(
            surfaces,
            &json!([
                "app_catalog_contract",
                "app_catalog_contract_by_branch",
                "app_catalog_contract_history"
            ])
        );
        assert_eq!(returned_definition, &definition);

        let columns = columns
            .as_array()
            .expect("column contracts should be a JSON array");
        let id = columns
            .iter()
            .find(|column| column["name"] == "id")
            .expect("id contract should exist");
        assert_eq!(id["data_type"], "TEXT");
        assert!(id["lix_value_kind"].is_null());
        assert_eq!(id["is_nullable"], false);
        assert_eq!(id["is_insertable"], true);
        assert_eq!(id["is_updatable"], false);
        assert_eq!(id["lix_insert_policy"], "DEFAULT");
        assert_eq!(id["column_default"], "lix_uuid_v7()");

        let payload = columns
            .iter()
            .find(|column| column["name"] == "payload")
            .expect("payload contract should exist");
        assert_eq!(payload["data_type"], "TEXT");
        assert_eq!(payload["lix_value_kind"], "JSON");
        assert_eq!(payload["is_nullable"], true);
        assert_eq!(payload["lix_insert_policy"], "OPTIONAL");

        let internal = session
            .execute(
                "SELECT table_name, by_branch_table_name, history_table_name, surfaces \
                 FROM lix_schema \
                 WHERE key = 'lix_registered_schema'",
                &[],
            )
            .await
            .expect("internal registered-schema definition remains discoverable");
        assert_rows_eq(
            internal,
            vec![vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Json(json!([])),
            ]],
        );
    }
);

simulation_test!(
    sql_catalog_templates_follow_committed_transaction_snapshots,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        transaction
            .execute(
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"sql_template_snapshot_note\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"text\":{\"type\":\"string\"}},\"required\":[\"id\",\"text\"],\"additionalProperties\":false}')\
             )",
                &[],
            )
            .await
            .expect("schema registration should stage");

        let insert_sql = "INSERT INTO sql_template_snapshot_note (id, text) VALUES ($1, $2)";
        transaction
            .execute(
                insert_sql,
                &[
                    Value::Text("note-1".to_string()),
                    Value::Text("after commit".to_string()),
                ],
            )
            .await
            .expect_err("SQL binding should keep the transaction-opening catalog snapshot");

        transaction
            .commit()
            .await
            .expect("schema transaction should commit");

        let inserted = session
            .execute(
                insert_sql,
                &[
                    Value::Text("note-1".to_string()),
                    Value::Text("after commit".to_string()),
                ],
            )
            .await
            .expect("the next transaction should bind against the committed catalog");
        assert_eq!(inserted.rows_affected(), 1);

        let selected = session
            .execute(
                "SELECT text FROM sql_template_snapshot_note WHERE id = $1",
                &[Value::Text("note-1".to_string())],
            )
            .await
            .expect("new entity surface should be readable after commit");
        assert_rows_eq(
            selected,
            vec![vec![Value::Text("after commit".to_string())]],
        );
    }
);

simulation_test!(
    lix_schema_definition_derives_read_only_key_and_rejects_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let null_error = session
            .execute(
                "INSERT INTO lix_schema_definition (definition) VALUES (NULL)",
                &[],
            )
            .await
            .expect_err("explicit NULL must be rejected");
        assert_eq!(null_error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(null_error.message.contains("does not allow explicit NULL"));

        let key_error = session
            .execute(
                "INSERT INTO lix_schema_definition (key, definition) \
                 VALUES ('wrong', lix_json('{\"x-lix-key\":\"derived\"}'))",
                &[],
            )
            .await
            .expect_err("derived key must not be writable");
        assert_eq!(key_error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(key_error.message.contains("key"));
    }
);

simulation_test!(
    lix_schema_definition_upsert_uses_derived_key_and_preserves_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let initial = json!({
            "x-lix-key": "app_upsert_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        let amended = json!({
            "x-lix-key": "app_upsert_schema",
            "x-lix-primary-key": ["/id"],
            "description": "amended",
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });

        for definition in [&initial, &amended] {
            let result = session
                .execute(
                    "INSERT INTO lix_schema_definition (definition) VALUES ($1) \
                     ON CONFLICT (key) DO UPDATE SET definition = excluded.definition",
                    &[Value::Json(definition.clone())],
                )
                .await
                .expect("derived-key upsert should succeed");
            assert_eq!(result, ExecuteResult::from_rows_affected(1));
        }

        let selected = session
            .execute(
                "SELECT definition FROM lix_schema_definition \
                 WHERE key = 'app_upsert_schema'",
                &[],
            )
            .await
            .expect("upserted definition should be readable");
        assert_rows_eq(selected, vec![vec![Value::Json(amended)]]);

        let changed_identity = json!({
            "x-lix-key": "app_different_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        let error = session
            .execute(
                "UPDATE lix_schema_definition SET definition = $1 \
                 WHERE key = 'app_upsert_schema'",
                &[Value::Json(changed_identity)],
            )
            .await
            .expect_err("UPDATE must not change the derived identity");
        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("cannot change derived key"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    schema_definition_relations_use_exact_branch_scope,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let global = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_definition = json!({
            "x-lix-key": "app_global_schema",
            "x-lix-primary-key": ["/id"],
            "description": "global",
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        let local_definition = json!({
            "x-lix-key": "app_global_schema",
            "x-lix-primary-key": ["/id"],
            "description": "local override",
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });

        global
            .execute(
                "INSERT INTO lix_schema_definition (definition) VALUES ($1)",
                &[Value::Json(global_definition.clone())],
            )
            .await
            .expect("global schema should register through the global session");

        let main_definition = main
            .execute(
                "SELECT key FROM lix_schema_definition \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("main definition catalog should remain queryable");
        assert_eq!(main_definition.len(), 0);
        let main_catalog = main
            .execute(
                "SELECT key FROM lix_schema \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("main semantic catalog should remain queryable");
        assert_eq!(main_catalog.len(), 0);

        let main_update = main
            .execute(
                "UPDATE lix_schema_definition SET definition = definition \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("UPDATE should use the same exact-scope row set as SELECT");
        assert_eq!(main_update, ExecuteResult::from_rows_affected(0));

        let global_update = global
            .execute(
                "UPDATE lix_schema_definition SET definition = definition \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("the global session should update its exact-scope definition");
        assert_eq!(global_update, ExecuteResult::from_rows_affected(1));

        main.execute(
            "INSERT INTO lix_schema_definition (definition) VALUES ($1) \
             ON CONFLICT (key) DO UPDATE SET definition = excluded.definition",
            &[Value::Json(local_definition.clone())],
        )
        .await
        .expect("main upsert should create a branch-local override");

        let main_row = main
            .execute(
                "SELECT definition FROM lix_schema_definition \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("main override should be visible");
        assert_rows_eq(main_row, vec![vec![Value::Json(local_definition)]]);

        let global_row = global
            .execute(
                "SELECT definition FROM lix_schema_definition \
                 WHERE key = 'app_global_schema'",
                &[],
            )
            .await
            .expect("global definition should remain visible");
        assert_rows_eq(global_row, vec![vec![Value::Json(global_definition)]]);
    }
);

simulation_test!(
    lix_schema_definition_reserves_lix_namespace_and_generated_names,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for schema_key in [
            "lix",
            "lix_file",
            "lix_key_value_history",
            "lix_state",
            "lix_state_history",
            "lix_file_descriptor",
            "lix_file_descriptor_history",
            "lix_plugin_note",
        ] {
            let schema = json!({
                "x-lix-key": schema_key,
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false,
            });
            let error = session
                .execute(
                    "INSERT INTO lix_schema_definition \
                     (definition) \
                     VALUES ($1)",
                    &[Value::Json(schema)],
                )
                .await
                .expect_err("every lix_* runtime schema key should be reserved");

            assert_eq!(error.code, LixError::CODE_RESERVED_SCHEMA_NAMESPACE);
            assert!(
                error.message.contains("reserved Lix schema namespace"),
                "{error:?}"
            );
            assert!(error.message.contains(schema_key), "{error:?}");
            assert!(
                error
                    .hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("acme_task")),
                "{error:?}"
            );
        }

        let noncolliding_schema = json!({
            "x-lix-key": "acme_plugin_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false,
        });
        session
            .execute(
                "INSERT INTO lix_schema_definition \
                 (definition) \
                 VALUES ($1)",
                &[Value::Json(noncolliding_schema)],
            )
            .await
            .expect("an application-owned schema namespace should remain registerable");

        let generated_name_collision = json!({
            "x-lix-key": "acme_plugin_note_history",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false,
        });
        let collision_error = session
            .execute(
                "INSERT INTO lix_schema_definition (definition) VALUES ($1)",
                &[Value::Json(generated_name_collision)],
            )
            .await
            .expect_err("a generated history table must reserve its name");
        assert_eq!(collision_error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            collision_error.message.contains("public SQL surface"),
            "{collision_error:?}"
        );
    }
);

simulation_test!(
    hidden_storage_schemas_remain_discoverable_without_public_sql_relations,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let registered = session
            .execute("SELECT key FROM lix_schema", &[])
            .await
            .expect("registered schemas should remain discoverable");
        let registered_keys = registered
            .rows()
            .iter()
            .filter_map(|row| match row.values() {
                [Value::Text(schema_key)] => Some(schema_key.as_str()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();

        for schema_key in [
            "lix_account",
            "lix_active_account",
            "lix_binary_blob_ref",
            "lix_branch_descriptor",
            "lix_branch_ref",
            "lix_change",
            "lix_change_author",
            "lix_commit",
            "lix_commit_edge",
            "lix_directory_descriptor",
            "lix_file_descriptor",
            "lix_key_value",
            "lix_label",
            "lix_label_assignment",
            "lix_registered_schema",
        ] {
            assert!(
                registered_keys.contains(schema_key),
                "{schema_key} should remain registered"
            );
        }

        let public_tables = session
            .execute("SELECT table_name FROM information_schema.tables", &[])
            .await
            .expect("public tables should be introspectable");
        let public_table_names = public_tables
            .rows()
            .iter()
            .filter_map(|row| match row.values() {
                [Value::Text(table_name)] => Some(table_name.as_str()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();

        for surface_name in [
            "lix_key_value",
            "lix_key_value_by_branch",
            "lix_key_value_history",
            "lix_schema",
            "lix_schema_definition",
        ] {
            assert!(
                public_table_names.contains(surface_name),
                "{surface_name} should remain public"
            );
        }
        for surface_name in [
            "lix_state",
            "lix_state_by_branch",
            "lix_state_history",
            "lix_registered_schema",
            "lix_registered_schema_by_branch",
            "lix_registered_schema_history",
            "lix_binary_blob_ref",
            "lix_binary_blob_ref_by_branch",
            "lix_binary_blob_ref_history",
            "lix_directory_descriptor",
            "lix_directory_descriptor_by_branch",
            "lix_directory_descriptor_history",
            "lix_file_descriptor",
            "lix_file_descriptor_by_branch",
            "lix_file_descriptor_history",
        ] {
            assert!(
                !public_table_names.contains(surface_name),
                "{surface_name} should not be public"
            );
        }
    }
);

simulation_test!(lix_schema_definition_delete_is_rejected, |sim| async move {
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_delete_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
             )",
                &[],
            )
            .await
            .expect("schema should register before delete attempt");

    let error = session
        .execute(
            "DELETE FROM lix_schema_definition WHERE key = 'engine_delete_schema'",
            &[],
        )
        .await
        .expect_err("schema deletion is not supported yet");

    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    assert!(
        error
            .message
            .contains("DELETE FROM lix_schema_definition is not supported"),
        "unexpected error: {error:?}"
    );

    let like_error = session
        .execute(
            "DELETE FROM lix_schema_definition \
             WHERE key LIKE 'engine_delete%'",
            &[],
        )
        .await
        .expect_err("schema deletion through LIKE is not supported either");
    assert_eq!(like_error.code, LixError::CODE_UNSUPPORTED_SQL);
    assert!(
        like_error
            .message
            .contains("DELETE FROM lix_schema_definition is not supported"),
        "unexpected error: {like_error:?}"
    );
});

simulation_test!(
    schema_definition_update_is_tracked_without_exposing_internal_storage_history,
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES ($1)",
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
                "UPDATE lix_schema_definition \
                 SET definition = $1 \
                 WHERE key = 'engine_schema_update_history'",
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

        let hidden_changes = session
            .execute(
                "SELECT id FROM lix_change \
                 WHERE schema_key = 'lix_registered_schema'",
                &[],
            )
            .await
            .expect("change activity should remain queryable");
        assert_eq!(hidden_changes.len(), 0);

        let current = session
            .execute(
                "SELECT definition FROM lix_schema_definition \
                 WHERE key = 'engine_schema_update_history'",
                &[],
            )
            .await
            .expect("amended schema definition should be readable");
        assert_rows_eq(current, vec![vec![Value::Json(amended_schema)]]);

        for retired_name in [
            "lix_state",
            "lix_state_by_branch",
            "lix_state_history",
            "lix_registered_schema",
            "lix_registered_schema_by_branch",
            "lix_registered_schema_history",
        ] {
            let error = session
                .execute(&format!("SELECT * FROM {retired_name}"), &[])
                .await
                .expect_err("retired public surfaces must not resolve");
            assert!(
                error.message.contains(retired_name),
                "unexpected error for {retired_name}: {error:?}"
            );
        }
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_bad_pointer_schema\",\"x-lix-primary-key\":[\"id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_empty_property_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"kind\":{}},\"required\":[\"id\",\"kind\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_poison_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_divergent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_divergent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}')\
             )",
                &[],
            )
            .await
            .expect("same schema key may have independent branch-local definitions");

        let main_result = main
            .execute(
                "SELECT definition \
                 FROM lix_schema_definition \
                 WHERE key = 'engine_divergent_schema'",
                &[],
            )
            .await
            .expect("main schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let target_result = target
            .execute(
                "SELECT definition \
                 FROM lix_schema_definition \
                 WHERE key = 'engine_divergent_schema'",
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES ($1)",
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
                "UPDATE lix_schema_definition \
                 SET definition = $1 \
                 WHERE key = 'engine_branch_schema_amendment'",
                &[Value::Json(main_schema.clone())],
            )
            .await
            .expect("main additive schema amendment should succeed");
        assert_eq!(main_update, ExecuteResult::from_rows_affected(1));

        let draft_update = draft
            .execute(
                "UPDATE lix_schema_definition \
                 SET definition = $1 \
                 WHERE key = 'engine_branch_schema_amendment'",
                &[Value::Json(draft_schema.clone())],
            )
            .await
            .expect("draft additive schema amendment should succeed");
        assert_eq!(draft_update, ExecuteResult::from_rows_affected(1));

        let main_result = main
            .execute(
                "SELECT definition \
                 FROM lix_schema_definition \
                 WHERE key = 'engine_branch_schema_amendment'",
                &[],
            )
            .await
            .expect("main amended schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Json(main_schema)]]);

        let draft_result = draft
            .execute(
                "SELECT definition \
                 FROM lix_schema_definition \
                 WHERE key = 'engine_branch_schema_amendment'",
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_fk_parent_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
             )",
            &[],
        )
        .await
        .expect("parent schema should register on active main");

        main.execute(
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_fk_child_schema\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"engine_fk_parent_schema\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"parent_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"parent_id\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_default_id_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_nullable_default_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"status\":{\"type\":[\"string\",\"null\"],\"default\":\"computed\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_overlay_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
             )",
            &[],
        )
        .await
        .expect("global registered schema insert should succeed");

    session
        .execute(
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_overlay_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_global_poison_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_typed_entity_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"},\"count\":{\"type\":\"number\"}},\"required\":[\"id\",\"name\",\"count\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_number_update_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"score\":{\"type\":\"number\"}},\"required\":[\"id\",\"score\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_file_scoped_entity_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_identity_literal_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_identity_in_literal_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_branch_filter_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_insert_branch_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_unknown_insert_column_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_duplicate_insert_column_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_qualified_insert_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_base_branch_insert_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_delete_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_update_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_by_branch_alias_scope_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_duplicate_update_assignment_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}')\
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_optional_update_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"rank\":{\"type\":\"integer\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}')\
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
