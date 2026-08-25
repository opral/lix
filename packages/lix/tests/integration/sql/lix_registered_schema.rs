use lix::CreateBranchOptions;
use lix::ExecuteResult;
use lix::LixError;
use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_registered_schema_insert_makes_typed_schema_surface_visible,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let register_schema_result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_dummy_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
                "SELECT lixcol_row_pk, value \
                 FROM lix_registered_schema",
                &[],
            )
            .await
            .expect("registered schema read should succeed");
        let registered_schema_rows = registered_schema_row;
        let registered_schema_row_pk = registered_schema_rows
            .rows()
            .iter()
            .find_map(|row| match row.values() {
                [Value::Jsonb(row_pk), Value::Jsonb(value)]
                    if value
                        .to_value()
                        .get("key")
                        .and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema") =>
                {
                    Some(row_pk)
                }
                [Value::Jsonb(row_pk), Value::Text(value)] => {
                    let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                    (value.get("key").and_then(serde_json::Value::as_str)
                        == Some("engine_dummy_schema"))
                    .then_some(row_pk)
                }
                _ => None,
            })
            .expect("registered schema row should be visible");
        assert_eq!(registered_schema_row_pk, &json!(["engine_dummy_schema"]));

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
    registered_schema_default_values_materializes_generated_and_literal_defaults,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "default_values_probe",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "label", "type": "text", "nullable": false, "default_value": "untitled" },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema registration should succeed");

        let inserted = session
            .execute("INSERT INTO default_values_probe DEFAULT VALUES", &[])
            .await
            .expect("DEFAULT VALUES should materialize registered-schema defaults");
        assert_eq!(inserted, ExecuteResult::from_rows_affected(1));

        let selected = session
            .execute("SELECT id, label FROM default_values_probe", &[])
            .await
            .expect("defaulted row should be readable");
        assert_eq!(selected.len(), 1);
        let [Value::Text(id), Value::Text(label)] = selected.rows()[0].values() else {
            panic!("generated and literal defaults should produce text columns");
        };
        let id = uuid::Uuid::parse_str(id).expect("generated default should be a UUID");
        assert_eq!(id.get_version_num(), 7);
        assert_eq!(label, "untitled");
    }
);

simulation_test!(
    registered_schema_default_values_still_validates_missing_required_properties,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "default_values_missing_required",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "label", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema registration should succeed");

        let error = session
            .execute(
                "INSERT INTO default_values_missing_required DEFAULT VALUES",
                &[],
            )
            .await
            .expect_err("missing required properties must remain a validation error");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }
);

simulation_test!(
    sql_catalog_templates_follow_committed_transaction_snapshots,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
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
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"sql_template_snapshot_note\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"text\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
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
            .expect("new schema surface should be readable after commit");
        assert_rows_eq(
            selected,
            vec![vec![Value::Text("after commit".to_string())]],
        );
    }
);

simulation_test!(
    untracked_registered_schema_does_not_authorize_tracked_typed_write,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_untracked_only_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 true\
                 )",
                &[],
            )
            .await
            .expect("untracked schema registration should succeed");

        let error = session
            .execute(
                "INSERT INTO engine_untracked_only_schema \
                 (id, name, lixcol_untracked) \
                 VALUES ('tracked-1', 'Tracked', false)",
                &[],
            )
            .await
            .expect_err("tracked rows must not validate against committed untracked schemas");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }
);

simulation_test!(
    lix_registered_schema_insert_rejects_reserved_lix_namespace,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for schema_key in [
            "lix",
            "lix_file",
            "lix_commit_edge",
            "lix_key_value_history",
            "lix_file_descriptor",
            "lix_file_descriptor_history",
            "lix_plugin_note",
        ] {
            let schema = json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": schema_key,
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            });
            let error = session
                .execute(
                    "INSERT INTO lix_registered_schema \
                     (value, lixcol_global, lixcol_untracked) \
                     VALUES ($1, false, false)",
                    &[Value::Jsonb(schema.into())],
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
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "acme_plugin_note",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema \
                 (value, lixcol_global, lixcol_untracked) \
                 VALUES ($1, false, false)",
                &[Value::Jsonb(noncolliding_schema.into())],
            )
            .await
            .expect("an application-owned schema namespace should remain registerable");
    }
);

simulation_test!(
    hidden_storage_schemas_remain_registered_without_public_sql_relations,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let registered = session
            .execute(
                "SELECT value ->> 'key' \
                 FROM lix_registered_schema",
                &[],
            )
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
            "lix_binary_blob_ref",
            "lix_branch_descriptor",
            "lix_branch_ref",
            "lix_change",
            "lix_checkpoint",
            "lix_commit",
            "lix_directory_descriptor",
            "lix_file_descriptor",
            "lix_key_value",
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

        for surface_name in ["lix_key_value", "lix_registered_schema", "lix_checkpoint"] {
            assert!(
                public_table_names.contains(surface_name),
                "{surface_name} should remain public"
            );
        }
        let table_functions = session
            .execute(
                "SELECT function_name, source_relation \
                 FROM information_schema.table_functions \
                 WHERE (function_name = 'lix_history' \
                        AND source_relation IN ('lix_checkpoint', 'lix_key_value', 'lix_registered_schema')) \
                    OR (function_name = 'lix_diff' \
                        AND source_relation IN ('lix_checkpoint', 'lix_key_value', 'lix_registered_schema')) \
                 GROUP BY function_name, source_relation \
                 ORDER BY function_name, source_relation",
                &[],
            )
            .await
            .expect("table function metadata should load");
        assert_rows_eq(
            table_functions,
            vec![
                vec![
                    Value::Text("lix_diff".to_string()),
                    Value::Text("lix_checkpoint".to_string()),
                ],
                vec![
                    Value::Text("lix_diff".to_string()),
                    Value::Text("lix_key_value".to_string()),
                ],
                vec![
                    Value::Text("lix_diff".to_string()),
                    Value::Text("lix_registered_schema".to_string()),
                ],
                vec![
                    Value::Text("lix_history".to_string()),
                    Value::Text("lix_checkpoint".to_string()),
                ],
                vec![
                    Value::Text("lix_history".to_string()),
                    Value::Text("lix_key_value".to_string()),
                ],
                vec![
                    Value::Text("lix_history".to_string()),
                    Value::Text("lix_registered_schema".to_string()),
                ],
            ],
        );
        for surface_name in [
            "lix_key_value_by_branch",
            "lix_registered_schema_by_branch",
            "lix_working_diff_by_branch",
            "lix_file_working_diff_by_branch",
            "lix_directory_working_diff_by_branch",
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

simulation_test!(lix_registered_schema_delete_is_rejected, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_delete_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("schema should register before delete attempt");

    let registered_schema_rows = session
        .execute(
            "SELECT lixcol_row_pk, value \
                 FROM lix_registered_schema",
            &[],
        )
        .await
        .expect("registered schema read should succeed");
    let delete_schema_row_pk = registered_schema_rows
        .rows()
        .iter()
        .find_map(|row| match row.values() {
            [Value::Jsonb(row_pk), Value::Jsonb(value)]
                if value
                    .to_value()
                    .get("key")
                    .and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema") =>
            {
                Some(row_pk.clone())
            }
            [Value::Jsonb(row_pk), Value::Text(value)] => {
                let value = serde_json::from_str::<serde_json::Value>(value).ok()?;
                (value.get("key").and_then(serde_json::Value::as_str)
                    == Some("engine_delete_schema"))
                .then_some(row_pk.clone())
            }
            _ => None,
        })
        .expect("registered schema row pk should be discoverable");

    let error = session
        .execute(
            "DELETE FROM lix_registered_schema \
                 WHERE lixcol_row_pk = $1",
            &[Value::Jsonb(delete_schema_row_pk)],
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

    let like_error = session
        .execute(
            "DELETE FROM lix_registered_schema \
             WHERE value ->> 'key' LIKE 'engine_delete%'",
            &[],
        )
        .await
        .expect_err("schema deletion through LIKE is not supported either");
    assert_eq!(like_error.code, LixError::CODE_UNSUPPORTED_SQL);
    assert!(
        like_error
            .message
            .contains("delete lix_registered_schema is not supported"),
        "unexpected error: {like_error:?}"
    );
});

simulation_test!(
    tracked_registered_schema_update_allows_compatible_amendment_and_history,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let initial_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_schema_update_history",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        let amended_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_schema_update_history",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
                { "name": "subtitle", "type": "text", "nullable": true },
            ],
            "primary_key": ["id"],
            "description": "Compatible tracked schema amendment",
        });

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES ($1, false, false)",
                &[Value::Jsonb(initial_schema.clone().into())],
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
                 WHERE lixcol_row_pk = CAST('[\"engine_schema_update_history\"]' AS JSONB)",
                &[Value::Jsonb(amended_schema.clone().into())],
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
                    "SELECT value, lixcol_row_pk, lixcol_observed_commit_id, lixcol_depth \
                     FROM lix_history('lix_registered_schema', '{second_commit_id}') \
                       WHERE lixcol_row_pk = CAST('[\"engine_schema_update_history\"]' AS JSONB) \
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
                    Value::Jsonb(amended_schema.into()),
                    Value::Jsonb(json!(["engine_schema_update_history"]).into()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Jsonb(initial_schema.into()),
                    Value::Jsonb(json!(["engine_schema_update_history"]).into()),
                    Value::Text(first_commit_id),
                    Value::Integer(1),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_registered_schema_insert_rejects_unknown_primary_key_column,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_bad_pointer_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"missing\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect_err("registered schema insert should reject unknown primary-key columns");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("unknown column 'missing'"),
            "unexpected message: {}",
            error.message
        );
    }
);

simulation_test!(
    lix_registered_schema_insert_rejects_unknown_column_type,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_empty_property_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"kind\",\"type\":\"object\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 true,\
                 false\
                 )",
                &[],
            )
            .await
            .expect_err("registered schema insert should reject unknown column types");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("unknown variant `object`"),
            "message should identify the unknown type: {}",
            error.message
        );
    }
);

simulation_test!(
    registered_schema_identity_is_scoped_per_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000012".to_string()),
            name: "Divergent Target".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("target branch should be created before schema divergence");

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_divergent_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
                &[],
            )
            .await
            .expect("main schema should be registered");

        let main_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_divergent_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "name", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        let target_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_divergent_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });

        let target = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000012")
                .await
                .expect("target session should open"),
            &engine,
        );

        target
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_divergent_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
                 WHERE lixcol_row_pk = CAST('[\"engine_divergent_schema\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("main schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Jsonb(main_schema.into())]]);

        let target_result = target
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_row_pk = CAST('[\"engine_divergent_schema\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("target schema read should succeed");
        assert_rows_eq(
            target_result,
            vec![vec![Value::Jsonb(target_schema.into())]],
        );
    }
);

simulation_test!(
    independent_schema_amendments_on_two_branches_are_allowed,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        let base_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_branch_schema_amendment",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        let main_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_branch_schema_amendment",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
                { "name": "main_note", "type": "text", "nullable": true },
            ],
            "primary_key": ["id"],
        });
        let draft_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "engine_branch_schema_amendment",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
                { "name": "draft_note", "type": "text", "nullable": true },
            ],
            "primary_key": ["id"],
        });

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES ($1, false, false)",
            &[Value::Jsonb(base_schema.into())],
        )
        .await
        .expect("base schema should be registered");

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000014".to_string()),
            name: "Schema Amendment Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created from base schema");

        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000014")
                .await
                .expect("draft session should open"),
            &engine,
        );

        let main_update = main
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_row_pk = CAST('[\"engine_branch_schema_amendment\"]' AS JSONB)",
                &[Value::Jsonb(main_schema.clone().into())],
            )
            .await
            .expect("main additive schema amendment should succeed");
        assert_eq!(main_update, ExecuteResult::from_rows_affected(1));

        let draft_update = draft
            .execute(
                "UPDATE lix_registered_schema \
                 SET value = $1 \
                 WHERE lixcol_row_pk = CAST('[\"engine_branch_schema_amendment\"]' AS JSONB)",
                &[Value::Jsonb(draft_schema.clone().into())],
            )
            .await
            .expect("draft additive schema amendment should succeed");
        assert_eq!(draft_update, ExecuteResult::from_rows_affected(1));

        let main_result = main
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_row_pk = CAST('[\"engine_branch_schema_amendment\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("main amended schema read should succeed");
        assert_rows_eq(main_result, vec![vec![Value::Jsonb(main_schema.into())]]);

        let draft_result = draft
            .execute(
                "SELECT value \
                 FROM lix_registered_schema \
                 WHERE lixcol_row_pk = CAST('[\"engine_branch_schema_amendment\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("draft amended schema read should succeed");
        assert_rows_eq(draft_result, vec![vec![Value::Jsonb(draft_schema.into())]]);
    }
);

simulation_test!(
    registered_row_insert_applies_defaulted_primary_key,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_default_id_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("row insert should apply defaulted primary key");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT lixcol_row_pk, id, name \
                 FROM engine_default_id_schema \
                 WHERE name = 'Generated'",
                &[],
            )
            .await
            .expect("row read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Jsonb(row_pk), Value::Text(id), Value::Text(name)] = values else {
            panic!("expected generated id row, got {values:?}");
        };
        assert_eq!(row_pk, &json!([id]));
        assert!(!id.is_empty(), "defaulted id should be non-empty");
        assert_eq!(name, "Generated");
    }
);

simulation_test!(
    registered_row_insert_preserves_explicit_null_for_defaulted_column,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_nullable_default_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"status\",\"type\":\"text\",\"nullable\":true,\"default_value\":\"computed\"}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("row insert should preserve explicit null");

        session
            .execute(
                "INSERT INTO engine_nullable_default_schema (id) \
                 VALUES ('omitted')",
                &[],
            )
            .await
            .expect("row insert should apply default for omitted column");

        let result = session
            .execute(
                "SELECT id, status \
                 FROM engine_nullable_default_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("row read should succeed");

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

simulation_test!(
    global_row_insert_rejects_active_only_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_global_poison_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
    registered_typed_schema_surface_uses_primary_key_columns,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_typed_row_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false},{\"name\":\"count\",\"type\":\"float8\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO engine_typed_row_schema \
                 (id, name, count, lixcol_global, lixcol_untracked) \
                 VALUES ('typed-row-1', 'Typed Row', 7, false, false)",
                &[],
            )
            .await
            .expect("typed row insert should succeed");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, name, count, lixcol_row_pk \
                 FROM engine_typed_row_schema \
                 WHERE id = 'typed-row-1'",
                &[],
            )
            .await
            .expect("typed row query by primary-key column should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("typed-row-1".to_string()),
                Value::Text("Typed Row".to_string()),
                Value::Real(7.0),
                Value::Jsonb(json!(["typed-row-1"]).into()),
            ]],
        );
    }
);

simulation_test!(
    typed_row_number_update_accepts_integer_param_like_insert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_number_update_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"score\",\"type\":\"float8\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("typed row insert should accept integer literal for number column");

        session
            .execute(
                "UPDATE engine_number_update_schema \
                 SET score = $1 \
                 WHERE id = 'score-1'",
                &[Value::Integer(52000)],
            )
            .await
            .expect("typed row update should accept integer param for number column");

        let result = session
            .execute(
                "SELECT score \
                 FROM engine_number_update_schema \
                 WHERE id = 'score-1'",
                &[],
            )
            .await
            .expect("typed row query should succeed");
        assert_rows_eq(result, vec![vec![Value::Real(52000.0)]]);
    }
);

simulation_test!(
    typed_row_update_accepts_file_id_predicate,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_file_scoped_row_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES \
                 ('66696c65-2d31-8000-8000-000000000000', '/66696c65-2d31-8000-8000-000000000000.txt', CAST('1' AS BYTEA)), \
                 ('66696c65-2d32-8000-8000-000000000000', '/66696c65-2d32-8000-8000-000000000000.txt', CAST('2' AS BYTEA))",
                &[],
            )
            .await
            .expect("file inserts should succeed");

        session
            .execute(
                "INSERT INTO engine_file_scoped_row_schema \
                 (id, name, lixcol_file_id, lixcol_global, lixcol_untracked) \
                 VALUES \
                 ('row-1', 'before-1', '66696c65-2d31-8000-8000-000000000000', false, false), \
                 ('row-2', 'before-2', '66696c65-2d32-8000-8000-000000000000', false, false)",
                &[],
            )
            .await
            .expect("typed row inserts with file ids should succeed");

        let update = session
            .execute(
                "UPDATE engine_file_scoped_row_schema \
                 SET name = 'after' \
                 WHERE lixcol_file_id = '66696c65-2d31-8000-8000-000000000000'",
                &[],
            )
            .await
            .expect("file id should be accepted in a row write predicate");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, name, lixcol_file_id \
                 FROM engine_file_scoped_row_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("row file id should be readable");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("row-1".to_string()),
                    Value::Text("after".to_string()),
                    Value::Text("66696c65-2d31-8000-8000-000000000000".to_string()),
                ],
                vec![
                    Value::Text("row-2".to_string()),
                    Value::Text("before-2".to_string()),
                    Value::Text("66696c65-2d32-8000-8000-000000000000".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    typed_row_update_accepts_parseable_json_text_identity_predicate,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_identity_literal_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("typed row insert should succeed");

        let update = session
            .execute(
                "UPDATE engine_identity_literal_schema \
                 SET name = 'after' \
                 WHERE lixcol_row_pk = '[\"row-1\"]'",
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
            .expect("updated typed row should read");
        assert_rows_eq(result, vec![vec![Value::Text("after".to_string())]]);
    }
);

simulation_test!(
    typed_row_update_accepts_parseable_json_text_identity_in_predicate,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_identity_in_literal_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("typed row insert should succeed");

        let update = session
            .execute(
                "UPDATE engine_identity_in_literal_schema \
                 SET name = 'after' \
                 WHERE lixcol_row_pk IN ('[\"row-1\"]')",
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
            .expect("updated typed row should read");
        assert_rows_eq(result, vec![vec![Value::Text("after".to_string())]]);
    }
);

simulation_test!(
    typed_row_base_update_cannot_override_active_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_base_branch_filter_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000d".to_string()),
            name: "Base Filter Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000d")
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
            .expect("draft row insert should succeed");

        let error = main
            .execute(
                "UPDATE engine_base_branch_filter_schema \
                 SET name = 'main-updated-draft' \
                 WHERE lixcol_row_pk = '[\"row-1\"]' \
                   AND lixcol_branch_id = '01930000-0000-7000-8000-00000000000d'",
                &[],
            )
            .await
            .expect_err("base row table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let result = draft
            .execute(
                "SELECT name FROM engine_base_branch_filter_schema \
                 WHERE lixcol_row_pk = CAST('[\"row-1\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("draft branch query should succeed");
        assert_rows_eq(result, vec![vec![Value::Text("draft".to_string())]]);
    }
);

simulation_test!(
    typed_row_base_insert_cannot_override_active_branch_scope,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_base_insert_branch_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000e".to_string()),
            name: "Base Insert Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let error = main
            .execute(
                "INSERT INTO engine_base_insert_branch_schema \
                 (id, name, lixcol_branch_id, lixcol_untracked) \
                 VALUES ('row-1', 'draft', '01930000-0000-7000-8000-00000000000e', false)",
                &[],
            )
            .await
            .expect_err("base row table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000e")
                .await
                .expect("draft session should open"),
            &engine,
        );
        let result = draft
            .execute(
                "SELECT name FROM engine_base_insert_branch_schema \
                 WHERE lixcol_row_pk = CAST('[\"row-1\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("draft branch query should succeed");
        assert_rows_eq(result, vec![]);
    }
);

simulation_test!(typed_row_insert_rejects_unknown_column, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_unknown_insert_column_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
        .expect_err("typed row insert should not ignore unknown columns");
    assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

    let result = session
        .execute("SELECT id FROM engine_unknown_insert_column_schema", &[])
        .await
        .expect("select should succeed");
    assert_rows_eq(result, Vec::<Vec<Value>>::new());
});

simulation_test!(
    typed_row_insert_rejects_duplicate_columns,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_duplicate_insert_column_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect_err("typed row insert should not accept duplicate columns");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let result = session
            .execute("SELECT id FROM engine_duplicate_insert_column_schema", &[])
            .await
            .expect("select should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_row_insert_rejects_unresolved_qualified_table,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_qualified_insert_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
    typed_row_base_insert_cannot_override_active_branch_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_base_branch_insert_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000e".to_string()),
            name: "Base Insert Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created after schema registration");

        let error = main
            .execute(
                "INSERT INTO engine_base_branch_insert_schema \
                 (id, name, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ('row-1', 'draft-via-main', '01930000-0000-7000-8000-00000000000e', false, false)",
                &[],
            )
            .await
            .expect_err("base row table should not expose lixcol_branch_id");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);

        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000e")
                .await
                .expect("draft session should open"),
            &engine,
        );
        let result = draft
            .execute("SELECT id FROM engine_base_branch_insert_schema", &[])
            .await
            .expect("draft branch query should succeed");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    typed_row_update_rejects_duplicate_assignments,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_duplicate_update_assignment_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
            .expect("row insert should succeed");

        let error = session
            .execute(
                "UPDATE engine_duplicate_update_assignment_schema \
                 SET name = 'first', name = 'second' \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect_err("typed row update should not accept duplicate assignments");
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
    primary_key_only_row_metadata_update_keeps_internal_snapshot_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_propertyless_update_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("primary-key-only schema should register");

        session
            .execute(
                "INSERT INTO engine_propertyless_update_schema \
                 (id, lixcol_row_pk, lixcol_metadata, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   'propertyless-row',\
                   CAST('[\"propertyless-row\"]' AS JSONB),\
                   CAST('{\"phase\":\"before\"}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("propertyless row should insert");

        session
            .execute(
                "UPDATE engine_propertyless_update_schema \
                 SET lixcol_metadata = CAST('{\"phase\":\"after\"}' AS JSONB) \
                 WHERE lixcol_row_pk = CAST('[\"propertyless-row\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("metadata-only update should retain its internal source snapshot");

        assert_rows_eq(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM engine_propertyless_update_schema \
                     WHERE lixcol_row_pk = CAST('[\"propertyless-row\"]' AS JSONB)",
                    &[],
                )
                .await
                .expect("updated propertyless row should remain readable"),
            vec![vec![Value::Jsonb(json!({"phase": "after"}).into())]],
        );
    }
);

simulation_test!(
    typed_row_update_preserves_absent_optional_non_nullable_fields,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_optional_update_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false},{\"name\":\"rank\",\"type\":\"int8\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
                "SELECT title, rank \
                 FROM engine_optional_update_schema \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("typed row query should succeed");
        assert_rows_eq(
            result,
            vec![vec![Value::Text("after".to_string()), Value::Null]],
        );

        session
            .execute(
                "UPDATE engine_optional_update_schema \
                 SET rank = NULL \
                 WHERE id = 'row-1'",
                &[],
            )
            .await
            .expect("explicit SQL NULL is valid for a nullable int8 column");
    }
);
