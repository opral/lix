use lix_engine::Value;

use super::select_rows;

simulation_test!(
    history_surfaces_are_introspected_as_views,
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
             lix_json('{\"x-lix-key\":\"engine_history_table_type\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        let rows = select_rows(
            &session,
            "SELECT table_name, table_type \
         FROM information_schema.tables \
         WHERE table_name IN (\
           'lix_state_history',\
           'lix_file_history',\
           'lix_directory_history',\
           'engine_history_table_type_history'\
         ) \
         ORDER BY table_name",
        )
        .await;

        let expected = [
            "engine_history_table_type_history",
            "lix_directory_history",
            "lix_file_history",
            "lix_state_history",
        ]
        .into_iter()
        .map(|table| {
            vec![
                Value::Text(table.to_string()),
                Value::Text("VIEW".to_string()),
            ]
        })
        .collect::<Vec<_>>();

        assert_eq!(rows, expected);
    }
);

simulation_test!(
    history_view_schemas_expose_tombstone_contract,
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
                 lix_json('{\"x-lix-key\":\"engine_history_contract_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"active\":{\"type\":\"boolean\"},\"meta\":{\"type\":\"object\"}},\"required\":[\"id\",\"count\",\"active\",\"meta\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let rows = select_rows(
            &session,
            "SELECT table_name, column_name, is_nullable \
             FROM information_schema.columns \
             WHERE table_name IN (\
               'lix_file_history',\
               'lix_directory_history',\
               'engine_history_contract_schema_history'\
             ) \
               AND (\
                 column_name IN ('path', 'directory_id', 'parent_id', 'name', 'data', 'id', 'count', 'active', 'meta') \
                 OR column_name = 'lixcol_snapshot_content'\
               ) \
             ORDER BY table_name, column_name",
        )
        .await;

        let expected = vec![
            ("engine_history_contract_schema_history", "active", "YES"),
            ("engine_history_contract_schema_history", "count", "YES"),
            ("engine_history_contract_schema_history", "id", "YES"),
            (
                "engine_history_contract_schema_history",
                "lixcol_snapshot_content",
                "YES",
            ),
            ("engine_history_contract_schema_history", "meta", "YES"),
            ("lix_directory_history", "id", "NO"),
            ("lix_directory_history", "lixcol_snapshot_content", "YES"),
            ("lix_directory_history", "name", "YES"),
            ("lix_directory_history", "parent_id", "YES"),
            ("lix_directory_history", "path", "YES"),
            ("lix_file_history", "data", "YES"),
            ("lix_file_history", "directory_id", "YES"),
            ("lix_file_history", "id", "NO"),
            ("lix_file_history", "lixcol_snapshot_content", "YES"),
            ("lix_file_history", "name", "YES"),
            ("lix_file_history", "path", "YES"),
        ]
        .into_iter()
        .map(|(table, column, nullable)| {
            vec![
                Value::Text(table.to_string()),
                Value::Text(column.to_string()),
                Value::Text(nullable.to_string()),
            ]
        })
        .collect::<Vec<_>>();

        assert_eq!(rows, expected);
    }
);

simulation_test!(
    typed_entity_history_exposes_tombstones_like_lix_state_history,
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
                 lix_json('{\"x-lix-key\":\"engine_history_conformance\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_history_conformance \
                 (lixcol_entity_pk, id, value, lixcol_untracked) \
                 VALUES (lix_json('[\"history-conformance-entity\"]'), 'history-conformance-entity', 'one', false)",
                &[],
            )
            .await
            .expect("entity insert should succeed");
        session
            .execute(
                "UPDATE engine_history_conformance \
                 SET value = 'two' \
                 WHERE lixcol_entity_pk = lix_json('[\"history-conformance-entity\"]')",
                &[],
            )
            .await
            .expect("entity update should succeed");
        session
            .execute(
                "DELETE FROM engine_history_conformance \
                 WHERE lixcol_entity_pk = lix_json('[\"history-conformance-entity\"]')",
                &[],
            )
            .await
            .expect("entity delete should succeed");

        let typed_rows = select_rows(
            &session,
            "SELECT id, value, lixcol_entity_pk, lixcol_snapshot_content, lixcol_depth \
             FROM engine_history_conformance_history \
             WHERE lixcol_start_commit_id = lix_active_branch_commit_id() \
               AND lixcol_entity_pk = lix_json('[\"history-conformance-entity\"]') \
             ORDER BY lixcol_depth",
        )
        .await;
        assert_eq!(typed_rows.len(), 3);
        assert_eq!(
            typed_rows[0],
            vec![
                Value::Null,
                Value::Null,
                Value::Json(serde_json::json!(["history-conformance-entity"])),
                Value::Null,
                Value::Integer(0),
            ]
        );

        let state_rows = select_rows(
            &session,
            "SELECT snapshot_content, depth \
             FROM lix_state_history \
             WHERE start_commit_id = lix_active_branch_commit_id() \
               AND schema_key = 'engine_history_conformance' \
               AND entity_pk = lix_json('[\"history-conformance-entity\"]') \
               AND snapshot_content IS NULL",
        )
        .await;
        assert_eq!(state_rows, vec![vec![Value::Null, Value::Integer(0)]]);
    }
);

simulation_test!(
    typed_entity_history_backfills_primary_key_columns_on_tombstones,
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
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('history-pk-backfill', 'one')",
                &[],
            )
            .await
            .expect("key value insert should succeed");
        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'history-pk-backfill'",
                &[],
            )
            .await
            .expect("key value delete should succeed");

        let rows = select_rows(
            &session,
            "SELECT key, value, lixcol_entity_pk, lixcol_snapshot_content, lixcol_depth \
             FROM lix_key_value_history \
             WHERE lixcol_start_commit_id = lix_active_branch_commit_id() \
               AND key = 'history-pk-backfill' \
             ORDER BY lixcol_depth",
        )
        .await;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("history-pk-backfill".to_string()),
                    Value::Null,
                    Value::Json(serde_json::json!(["history-pk-backfill"])),
                    Value::Null,
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-pk-backfill".to_string()),
                    lix_engine::Value::Json(serde_json::json!("one")),
                    Value::Json(serde_json::json!(["history-pk-backfill"])),
                    lix_engine::Value::Json(serde_json::json!({
                        "key": "history-pk-backfill",
                        "value": "one"
                    })),
                    Value::Integer(1),
                ],
            ]
        );
    }
);

simulation_test!(
    typed_entity_history_backfills_composite_primary_key_columns_on_tombstones,
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
                 lix_json('{\"x-lix-key\":\"engine_history_composite_pk\",\"x-lix-primary-key\":[\"/namespace\",\"/id\"],\"type\":\"object\",\"properties\":{\"namespace\":{\"type\":\"string\"},\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"namespace\",\"id\",\"value\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_history_composite_pk \
                 (namespace, id, value, lixcol_untracked) \
                 VALUES ('messages', '7', 'one', false)",
                &[],
            )
            .await
            .expect("composite entity insert should succeed");
        session
            .execute(
                "DELETE FROM engine_history_composite_pk \
                 WHERE namespace = 'messages' AND id = '7'",
                &[],
            )
            .await
            .expect("composite entity delete should succeed");

        let rows = select_rows(
            &session,
            "SELECT namespace, id, value, lixcol_snapshot_content, lixcol_depth \
             FROM engine_history_composite_pk_history \
             WHERE lixcol_start_commit_id = lix_active_branch_commit_id() \
               AND namespace = 'messages' \
               AND id = '7' \
             ORDER BY lixcol_depth",
        )
        .await;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("messages".to_string()),
                    Value::Text("7".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("messages".to_string()),
                    Value::Text("7".to_string()),
                    Value::Text("one".to_string()),
                    lix_engine::Value::Json(serde_json::json!({
                        "namespace": "messages",
                        "id": "7",
                        "value": "one"
                    })),
                    Value::Integer(1),
                ],
            ]
        );
    }
);

simulation_test!(
    lix_file_history_exposes_descriptor_tombstones_like_lix_state_history,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-conformance-file', '/docs/conformance.txt', X'6F6E65')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET data = X'74776F' WHERE id = 'history-conformance-file'",
                &[],
            )
            .await
            .expect("file update should succeed");
        session
            .execute(
                "DELETE FROM lix_file WHERE id = 'history-conformance-file'",
                &[],
            )
            .await
            .expect("file delete should succeed");

        let file_rows = select_rows(
            &session,
            "SELECT id, path, name, data, lixcol_entity_pk, lixcol_file_id, lixcol_snapshot_content, lixcol_depth \
             FROM lix_file_history \
             WHERE lixcol_start_commit_id = lix_active_branch_commit_id() \
               AND id = 'history-conformance-file' \
               AND lixcol_depth = 0",
        )
        .await;
        assert_eq!(
            file_rows,
            vec![vec![
                Value::Text("history-conformance-file".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Json(serde_json::json!(["history-conformance-file"])),
                Value::Text("history-conformance-file".to_string()),
                Value::Null,
                Value::Integer(0),
            ]]
        );

        let state_rows = select_rows(
            &session,
            "SELECT snapshot_content, depth \
             FROM lix_state_history \
             WHERE start_commit_id = lix_active_branch_commit_id() \
               AND schema_key = 'lix_file_descriptor' \
               AND entity_pk = lix_json('[\"history-conformance-file\"]') \
               AND snapshot_content IS NULL",
        )
        .await;
        assert_eq!(state_rows, vec![vec![Value::Null, Value::Integer(0)]]);
    }
);

simulation_test!(
    lix_directory_history_exposes_descriptor_tombstones_like_lix_state_history,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-conformance-dir', '/conformance/')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        session
            .execute(
                "UPDATE lix_directory SET name = 'conformance-updated' \
                 WHERE id = 'history-conformance-dir'",
                &[],
            )
            .await
            .expect("directory update should succeed");
        session
            .execute(
                "DELETE FROM lix_directory WHERE id = 'history-conformance-dir'",
                &[],
            )
            .await
            .expect("directory delete should succeed");

        let directory_rows = select_rows(
            &session,
            "SELECT id, path, parent_id, name, lixcol_entity_pk, lixcol_snapshot_content, lixcol_depth \
             FROM lix_directory_history \
             WHERE lixcol_start_commit_id = lix_active_branch_commit_id() \
               AND id = 'history-conformance-dir' \
               AND lixcol_depth = 0",
        )
        .await;
        assert_eq!(
            directory_rows,
            vec![vec![
                Value::Text("history-conformance-dir".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Json(serde_json::json!(["history-conformance-dir"])),
                Value::Null,
                Value::Integer(0),
            ]]
        );

        let state_rows = select_rows(
            &session,
            "SELECT snapshot_content, depth \
             FROM lix_state_history \
             WHERE start_commit_id = lix_active_branch_commit_id() \
               AND schema_key = 'lix_directory_descriptor' \
               AND entity_pk = lix_json('[\"history-conformance-dir\"]') \
               AND snapshot_content IS NULL",
        )
        .await;
        assert_eq!(state_rows, vec![vec![Value::Null, Value::Integer(0)]]);
    }
);
