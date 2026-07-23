use lix_engine::Value;
use serde_json::json;

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
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine_history_table_type\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}')\
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

        let provenance_columns = select_rows(
            &session,
            "SELECT table_name, column_name \
             FROM information_schema.columns \
             WHERE table_name IN ('lix_file_history', 'lix_directory_history') \
               AND column_name IN (\
                 'lixcol_source_changes',\
                 'lixcol_schema_key',\
                 'lixcol_file_id',\
                 'lixcol_snapshot_content',\
                 'lixcol_change_id',\
                 'lixcol_origin_key',\
                 'lixcol_metadata'\
               ) \
             ORDER BY table_name, column_name",
        )
        .await;
        assert_eq!(
            provenance_columns,
            vec![
                vec![
                    Value::Text("lix_directory_history".to_string()),
                    Value::Text("lixcol_source_changes".to_string()),
                ],
                vec![
                    Value::Text("lix_file_history".to_string()),
                    Value::Text("lixcol_source_changes".to_string()),
                ],
            ],
            "composed histories expose aggregate provenance without singular aliases"
        );
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_history_contract_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"active\":{\"type\":\"boolean\"},\"meta\":{\"type\":\"object\"}},\"required\":[\"id\",\"count\",\"active\",\"meta\"],\"additionalProperties\":false}')\
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
                 OR column_name IN ('lixcol_snapshot_content', 'lixcol_is_deleted', 'lixcol_source_changes')\
               ) \
             ORDER BY table_name, column_name",
        )
        .await;

        let expected = vec![
            ("engine_history_contract_schema_history", "active", "YES"),
            ("engine_history_contract_schema_history", "count", "YES"),
            ("engine_history_contract_schema_history", "id", "NO"),
            (
                "engine_history_contract_schema_history",
                "lixcol_is_deleted",
                "NO",
            ),
            (
                "engine_history_contract_schema_history",
                "lixcol_snapshot_content",
                "YES",
            ),
            ("engine_history_contract_schema_history", "meta", "YES"),
            ("lix_directory_history", "id", "NO"),
            ("lix_directory_history", "lixcol_is_deleted", "NO"),
            ("lix_directory_history", "lixcol_source_changes", "NO"),
            ("lix_directory_history", "name", "YES"),
            ("lix_directory_history", "parent_id", "YES"),
            ("lix_directory_history", "path", "YES"),
            ("lix_file_history", "data", "YES"),
            ("lix_file_history", "directory_id", "YES"),
            ("lix_file_history", "id", "NO"),
            ("lix_file_history", "lixcol_is_deleted", "NO"),
            ("lix_file_history", "lixcol_source_changes", "NO"),
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

simulation_test!(typed_entity_history_exposes_tombstones, |sim| async move {
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
                 lix_json('{\"x-lix-key\":\"engine_history_conformance\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false}')\
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
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
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
});

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
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
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
                    Value::Json(serde_json::json!("one")),
                    Value::Json(serde_json::json!(["history-pk-backfill"])),
                    Value::Json(serde_json::json!({
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
                "INSERT INTO lix_schema_definition (definition) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_history_composite_pk\",\"x-lix-primary-key\":[\"/namespace\",\"/id\"],\"type\":\"object\",\"properties\":{\"namespace\":{\"type\":\"string\"},\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"namespace\",\"id\",\"value\"],\"additionalProperties\":false}')\
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
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
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
                    Value::Json(serde_json::json!({
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
    typed_entity_history_reconstructs_nested_primary_key_roots_on_tombstones,
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
                 lix_json('{\"x-lix-key\":\"engine_history_nested_pk\",\"x-lix-primary-key\":[\"/identity/tenant\",\"/identity/id\"],\"type\":\"object\",\"properties\":{\"identity\":{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"string\"},\"id\":{\"type\":\"string\"}},\"required\":[\"tenant\",\"id\"],\"additionalProperties\":false},\"value\":{\"type\":\"string\"}},\"required\":[\"identity\",\"value\"],\"additionalProperties\":false}')\
             )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_history_nested_pk \
                 (identity, value, lixcol_untracked) \
                 VALUES (lix_json('{\"tenant\":\"acme\",\"id\":\"7\"}'), 'one', false)",
                &[],
            )
            .await
            .expect("nested-key entity insert should succeed");
        session
            .execute(
                "DELETE FROM engine_history_nested_pk \
                 WHERE lixcol_entity_pk = lix_json('[\"acme\",\"7\"]')",
                &[],
            )
            .await
            .expect("nested-key entity delete should succeed");

        let rows = select_rows(
            &session,
            "SELECT identity, value, lixcol_snapshot_content, lixcol_depth \
             FROM engine_history_nested_pk_history \
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
               AND lix_json_get_text(identity, 'tenant') = 'acme' \
               AND lix_json_get_text(identity, 'id') = '7' \
             ORDER BY lixcol_depth",
        )
        .await;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Json(serde_json::json!({
                        "tenant": "acme",
                        "id": "7"
                    })),
                    Value::Null,
                    Value::Null,
                    Value::Integer(0),
                ],
                vec![
                    Value::Json(serde_json::json!({
                        "tenant": "acme",
                        "id": "7"
                    })),
                    Value::Text("one".to_string()),
                    Value::Json(serde_json::json!({
                        "identity": {
                            "tenant": "acme",
                            "id": "7"
                        },
                        "value": "one"
                    })),
                    Value::Integer(1),
                ],
            ]
        );

        let nullability = select_rows(
            &session,
            "SELECT is_nullable \
             FROM information_schema.columns \
             WHERE table_name = 'engine_history_nested_pk_history' \
               AND column_name = 'identity'",
        )
        .await;
        assert_eq!(nullability, vec![vec![Value::Text("NO".to_string())]]);
    }
);

simulation_test!(
    lix_file_history_exposes_logical_tombstones,
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
            "SELECT id, path, name, data, lixcol_entity_pk, lixcol_is_deleted, lixcol_depth \
             FROM lix_file_history \
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
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
                Value::Boolean(true),
                Value::Integer(0),
            ]]
        );
    }
);

simulation_test!(
    lix_directory_history_exposes_logical_tombstones,
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
            "SELECT id, path, parent_id, name, lixcol_entity_pk, lixcol_is_deleted, lixcol_depth \
             FROM lix_directory_history \
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
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
                Value::Boolean(true),
                Value::Integer(0),
            ]]
        );
    }
);

simulation_test!(
    typed_history_routes_exact_anchor_from_join_predicate,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-join-anchor', 'one')",
                &[],
            )
            .await
            .expect("initial tracked write should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'history-join-anchor'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");

        let result = session
            .execute(
                &format!(
                    "SELECT h.lixcol_as_of_commit_id \
                     FROM lix_key_value_history AS h \
                     JOIN lix_key_value AS active \
                       ON h.key = active.key \
                      AND h.lixcol_as_of_commit_id = '{first_commit_id}' \
                     WHERE h.key = 'history-join-anchor'"
                ),
                &[],
            )
            .await
            .expect("an exact join anchor should route to the requested history root");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row
                    .get::<Value>("lixcol_as_of_commit_id")
                    .expect("lixcol_as_of_commit_id"))
                .collect::<Vec<_>>(),
            vec![Value::Text(first_commit_id.clone())]
        );

        let nullable_side = session
            .execute(
                &format!(
                    "SELECT h.lixcol_as_of_commit_id, h.lixcol_snapshot_content \
                     FROM lix_branch AS b \
                     LEFT JOIN lix_key_value_history AS h \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                      AND h.key = 'history-join-anchor' \
                     WHERE b.id = 'global'"
                ),
                &[],
            )
            .await
            .expect("an exact anchor on the nullable join side should route");
        assert_eq!(
            nullable_side
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Text(first_commit_id.clone()),
                Value::Json(json!({"key": "history-join-anchor", "value": "one"})),
            ]]
        );

        let right_nullable_side = session
            .execute(
                &format!(
                    "SELECT h.lixcol_as_of_commit_id, h.lixcol_snapshot_content \
                     FROM lix_key_value_history AS h \
                     RIGHT JOIN lix_branch AS b \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                      AND h.key = 'history-join-anchor' \
                     WHERE b.id = 'global'"
                ),
                &[],
            )
            .await
            .expect("an exact anchor on the nullable side of a right join should route");
        assert_eq!(
            right_nullable_side
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Text(first_commit_id.clone()),
                Value::Json(json!({"key": "history-join-anchor", "value": "one"})),
            ]]
        );

        let semi_join = session
            .execute(
                &format!(
                    "SELECT h.lixcol_as_of_commit_id, h.lixcol_snapshot_content \
                     FROM lix_key_value_history AS h \
                     LEFT SEMI JOIN lix_branch AS b \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                     WHERE h.key = 'history-join-anchor'"
                ),
                &[],
            )
            .await
            .expect("an exact semi-join anchor should route");
        assert_eq!(
            semi_join
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Text(first_commit_id.clone()),
                Value::Json(json!({"key": "history-join-anchor", "value": "one"})),
            ]]
        );

        let projected = session
            .execute(
                &format!(
                    "SELECT projected.anchor AS lixcol_as_of_commit_id \
                     FROM (\
                       SELECT key, lixcol_as_of_commit_id AS anchor \
                       FROM lix_key_value_history\
                     ) AS projected \
                     WHERE projected.anchor = '{first_commit_id}' \
                       AND projected.key = 'history-join-anchor'"
                ),
                &[],
            )
            .await
            .expect("an exact anchor should route through a direct projection alias");
        assert_eq!(
            projected
                .rows()
                .iter()
                .map(|row| row
                    .get::<Value>("lixcol_as_of_commit_id")
                    .expect("lixcol_as_of_commit_id"))
                .collect::<Vec<_>>(),
            vec![Value::Text(first_commit_id)]
        );
    }
);

simulation_test!(
    history_surfaces_reject_unrouteable_anchor_predicates,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for sql in [
            "SELECT key FROM lix_key_value_history WHERE lixcol_as_of_commit_id > 'cid_invalid'",
            "SELECT key FROM lix_key_value_history WHERE lixcol_as_of_commit_id NOT IN ('cid_invalid')",
            "SELECT key FROM lix_key_value_history WHERE lixcol_as_of_commit_id = 'cid_invalid' OR key = 'other'",
            "SELECT h.key FROM lix_key_value_history AS h JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = b.commit_id",
            "SELECT h.key FROM lix_key_value_history AS h JOIN lix_branch AS b ON h.lixcol_as_of_commit_id > b.commit_id",
            "SELECT h.key FROM lix_key_value_history AS h LEFT JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.key FROM lix_branch AS b RIGHT JOIN lix_key_value_history AS h ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.key FROM lix_key_value_history AS h FULL JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.key FROM lix_key_value_history AS h LEFT ANTI JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.key FROM lix_branch AS b RIGHT ANTI JOIN lix_key_value_history AS h ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT projected.key FROM (SELECT key, lixcol_as_of_commit_id AS anchor FROM lix_key_value_history) AS projected WHERE projected.anchor > 'cid_invalid'",
            "SELECT limited.key FROM (SELECT key, lixcol_as_of_commit_id AS anchor FROM lix_key_value_history LIMIT 1) AS limited WHERE limited.anchor = 'cid_invalid'",
            "SELECT id FROM lix_file_history WHERE lixcol_as_of_commit_id LIKE 'cid_%'",
            "SELECT id FROM lix_directory_history WHERE lixcol_as_of_commit_id IS NULL",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("unrouteable history anchors must not fall back to the active head");
            assert_eq!(error.code, lix_engine::LixError::CODE_UNSUPPORTED_SQL);
            assert!(
                error.to_string().contains("only supports exact equality"),
                "unexpected error: {error}"
            );
            assert!(
                error
                    .hint()
                    .is_some_and(|hint| hint.contains("pinned active branch head")),
                "unexpected hint: {error:?}"
            );
        }
    }
);

simulation_test!(
    unrelated_same_named_column_does_not_validate_as_history_anchor,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('collision', 'value')",
                &[],
            )
            .await
            .expect("history row should insert");

        let result = session
            .execute(
                "SELECT ordinary.lixcol_as_of_commit_id \
                 FROM (SELECT 'ordinary' AS lixcol_as_of_commit_id) AS ordinary \
                 CROSS JOIN lix_key_value_history AS history \
                 WHERE ordinary.lixcol_as_of_commit_id > 'a' \
                   AND history.key = 'collision' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("ordinary same-named predicate must not be treated as a history anchor");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row
                    .get::<Value>("lixcol_as_of_commit_id")
                    .expect("lixcol_as_of_commit_id"))
                .collect::<Vec<_>>(),
            vec![Value::Text("ordinary".to_string())]
        );
    }
);

simulation_test!(
    typed_history_supports_multiple_as_of_commit_filters,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-multi-start', 'one')",
                &[],
            )
            .await
            .expect("first write should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'history-multi-start'",
                &[],
            )
            .await
            .expect("second write should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");

        let in_rows = select_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id, lixcol_depth, lixcol_snapshot_content \
                 FROM lix_key_value_history \
                 WHERE lixcol_as_of_commit_id IN ('{first_commit_id}', '{second_commit_id}') \
                   AND key = 'history-multi-start' \
                   AND lixcol_depth = 0 \
                 ORDER BY lixcol_as_of_commit_id"
            ),
        )
        .await;
        assert_eq!(
            in_rows,
            vec![
                vec![
                    Value::Text(first_commit_id.clone()),
                    Value::Integer(0),
                    Value::Json(json!({"key": "history-multi-start", "value": "one"})),
                ],
                vec![
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                    Value::Json(json!({"key": "history-multi-start", "value": "two"})),
                ],
            ],
            "IN should allow multiple explicit history anchors"
        );

        let or_rows = select_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id \
                 FROM lix_key_value_history \
                 WHERE (lixcol_as_of_commit_id = '{first_commit_id}' \
                        OR lixcol_as_of_commit_id = '{second_commit_id}') \
                   AND key = 'history-multi-start' \
                   AND lixcol_depth = 0 \
                 ORDER BY lixcol_as_of_commit_id"
            ),
        )
        .await;
        assert_eq!(
            or_rows,
            vec![
                vec![Value::Text(first_commit_id)],
                vec![Value::Text(second_commit_id)],
            ],
            "OR should also allow multiple explicit history anchors"
        );
    }
);

simulation_test!(
    typed_history_intersects_conjunctive_value_filters,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-and-a', 'a')",
                &[],
            )
            .await
            .expect("first write should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('history-and-b', 'b')",
                &[],
            )
            .await
            .expect("second write should succeed");
        let head_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        let narrowed_rows = select_rows(
            &session,
            &format!(
                "SELECT key \
                 FROM lix_key_value_history \
                 WHERE lixcol_as_of_commit_id = '{head_commit_id}' \
                   AND key IN ('history-and-a', 'history-and-b') \
                   AND key = 'history-and-a'"
            ),
        )
        .await;
        assert_eq!(
            narrowed_rows,
            vec![vec![Value::Text("history-and-a".to_string())]],
            "AND filters on the same history column should intersect, not union"
        );

        let contradictory_rows = select_rows(
            &session,
            &format!(
                "SELECT key \
                 FROM lix_key_value_history \
                 WHERE lixcol_as_of_commit_id = '{head_commit_id}' \
                   AND key = 'history-and-a' \
                   AND key = 'history-and-b'"
            ),
        )
        .await;
        assert_eq!(
            contradictory_rows,
            Vec::<Vec<Value>>::new(),
            "contradictory AND filters on the same history column should return no rows"
        );
    }
);
