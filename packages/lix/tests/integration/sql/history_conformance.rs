use lix::Value;
use serde_json::json;

use super::select_rows;

simulation_test!(
    history_functions_are_not_exposed_as_static_tables,
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
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_table_type\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
           'lix_file_history',\
           'lix_directory_history',\
           'engine_history_table_type_history'\
         ) \
         ORDER BY table_name",
        )
        .await;

        assert!(rows.is_empty());
    }
);

simulation_test!(
    history_function_results_hide_the_anchor_column,
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
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_contract_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"count\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":false},{\"name\":\"meta\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        for sql in [
            "SELECT * FROM lix_file_history() LIMIT 0",
            "SELECT * FROM lix_directory_history() LIMIT 0",
            "SELECT * FROM engine_history_contract_schema_history() LIMIT 0",
        ] {
            let result = session.execute(sql, &[]).await.expect("history function");
            assert!(
                !result
                    .columns()
                    .iter()
                    .any(|column| column == "lixcol_as_of_commit_id")
            );
            assert!(
                result
                    .columns()
                    .iter()
                    .any(|column| column == "lixcol_depth")
            );
            assert!(
                result
                    .columns()
                    .iter()
                    .any(|column| column == "lixcol_is_deleted")
            );
        }
    }
);

simulation_test!(typed_entity_history_exposes_tombstones, |sim| async move {
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
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_conformance\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
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
                 VALUES (CAST('[\"history-conformance-entity\"]' AS JSONB), 'history-conformance-entity', 'one', false)",
                &[],
            )
            .await
            .expect("entity insert should succeed");
    session
        .execute(
            "UPDATE engine_history_conformance \
                 SET value = 'two' \
                 WHERE lixcol_entity_pk = CAST('[\"history-conformance-entity\"]' AS JSONB)",
            &[],
        )
        .await
        .expect("entity update should succeed");
    session
        .execute(
            "DELETE FROM engine_history_conformance \
                 WHERE lixcol_entity_pk = CAST('[\"history-conformance-entity\"]' AS JSONB)",
            &[],
        )
        .await
        .expect("entity delete should succeed");

    let typed_rows = select_rows(
        &session,
        "SELECT id, value, lixcol_entity_pk, lixcol_depth \
             FROM engine_history_conformance_history() \
               WHERE lixcol_entity_pk = CAST('[\"history-conformance-entity\"]' AS JSONB) \
             ORDER BY lixcol_depth",
    )
    .await;
    assert_eq!(typed_rows.len(), 3);
    assert_eq!(
        typed_rows[0],
        vec![
            Value::Text("history-conformance-entity".to_string()),
            Value::Null,
            Value::Json(serde_json::json!(["history-conformance-entity"]).into()),
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
                .open_session()
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
            "SELECT key, value, lixcol_entity_pk, lixcol_depth \
             FROM lix_key_value_history() \
               WHERE key = 'history-pk-backfill' \
             ORDER BY lixcol_depth",
        )
        .await;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("history-pk-backfill".to_string()),
                    Value::Null,
                    Value::Json(serde_json::json!(["history-pk-backfill"]).into()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-pk-backfill".to_string()),
                    Value::Json(serde_json::json!("one").into()),
                    Value::Json(serde_json::json!(["history-pk-backfill"]).into()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_composite_pk\",\"columns\":[{\"name\":\"namespace\",\"type\":\"text\",\"nullable\":false},{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"namespace\",\"id\"]}' AS JSONB),\
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
            "SELECT namespace, id, value, lixcol_depth \
             FROM engine_history_composite_pk_history() \
               WHERE namespace = 'messages' \
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
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("messages".to_string()),
                    Value::Text("7".to_string()),
                    Value::Text("one".to_string()),
                    Value::Integer(1),
                ],
            ]
        );
    }
);

simulation_test!(
    typed_entity_history_reconstructs_flat_primary_key_columns_on_tombstones,
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
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_history_nested_pk\",\"columns\":[{\"name\":\"tenant\",\"type\":\"text\",\"nullable\":false},{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"tenant\",\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_history_nested_pk \
                 (tenant, id, value, lixcol_untracked) \
                 VALUES ('acme', '7', 'one', false)",
                &[],
            )
            .await
            .expect("nested-key entity insert should succeed");
        session
            .execute(
                "DELETE FROM engine_history_nested_pk \
                 WHERE lixcol_entity_pk = CAST('[\"acme\",\"7\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("nested-key entity delete should succeed");

        let rows = select_rows(
            &session,
            "SELECT tenant, id, value, lixcol_depth \
             FROM engine_history_nested_pk_history() \
               WHERE tenant = 'acme' AND id = '7' \
             ORDER BY lixcol_depth",
        )
        .await;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("acme".to_string()),
                    Value::Text("7".to_string()),
                    Value::Null,
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("acme".to_string()),
                    Value::Text("7".to_string()),
                    Value::Text("one".to_string()),
                    Value::Integer(1),
                ],
            ]
        );

        let nullability = select_rows(
            &session,
            "SELECT is_nullable \
             FROM information_schema.table_functions \
             WHERE function_name = 'engine_history_nested_pk_history' \
               AND result_column = 'tenant'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('68697374-6f72-892d-836f-6e666f726d00', '/docs/conformance.txt', CAST('one' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET content = CAST('two' AS BYTEA) WHERE id = '68697374-6f72-892d-836f-6e666f726d00'",
                &[],
            )
            .await
            .expect("file update should succeed");
        session
            .execute(
                "DELETE FROM lix_file WHERE id = '68697374-6f72-892d-836f-6e666f726d00'",
                &[],
            )
            .await
            .expect("file delete should succeed");

        let file_rows = select_rows(
            &session,
            "SELECT id, path, name, content, lixcol_entity_pk, lixcol_is_deleted, lixcol_depth \
             FROM lix_file_history() \
               WHERE id = '68697374-6f72-892d-836f-6e666f726d00' \
               AND lixcol_depth = 0",
        )
        .await;
        assert_eq!(
            file_rows,
            vec![vec![
                Value::Text("68697374-6f72-892d-836f-6e666f726d00".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Json(serde_json::json!(["68697374-6f72-892d-836f-6e666f726d00"]).into()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('68697374-6f72-892d-836f-6e666f726d00', '/conformance')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        session
            .execute(
                "UPDATE lix_directory SET name = 'conformance-updated' \
                 WHERE id = '68697374-6f72-892d-836f-6e666f726d00'",
                &[],
            )
            .await
            .expect("directory update should succeed");
        session
            .execute(
                "DELETE FROM lix_directory WHERE id = '68697374-6f72-892d-836f-6e666f726d00'",
                &[],
            )
            .await
            .expect("directory delete should succeed");

        let directory_rows = select_rows(
            &session,
            "SELECT id, path, parent_id, name, lixcol_entity_pk, lixcol_is_deleted, lixcol_depth \
             FROM lix_directory_history() \
               WHERE id = '68697374-6f72-892d-836f-6e666f726d00' \
               AND lixcol_depth = 0",
        )
        .await;
        assert_eq!(
            directory_rows,
            vec![vec![
                Value::Text("68697374-6f72-892d-836f-6e666f726d00".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Json(serde_json::json!(["68697374-6f72-892d-836f-6e666f726d00"]).into()),
                Value::Boolean(true),
                Value::Integer(0),
            ]]
        );
    }
);

simulation_test!(
    typed_history_function_anchor_composes_with_joins,
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
                    "SELECT h.value \
                     FROM lix_key_value_history('{first_commit_id}') AS h \
                     JOIN lix_key_value AS active \
                       ON h.key = active.key \
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
                .map(|row| row.get::<Value>("value").expect("value"))
                .collect::<Vec<_>>(),
            vec![Value::Json(json!("one").into())]
        );

        let nullable_side = session
            .execute(
                &format!(
                    "SELECT h.value \
                     FROM lix_branch AS b \
                     LEFT JOIN lix_key_value_history('{first_commit_id}') AS h \
                       ON h.key = 'history-join-anchor' \
                     WHERE b.id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'"
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
            vec![vec![Value::Json(json!("one").into()),]]
        );

        let right_nullable_side = session
            .execute(
                &format!(
                    "SELECT h.value \
                     FROM lix_key_value_history('{first_commit_id}') AS h \
                     RIGHT JOIN lix_branch AS b \
                       ON h.key = 'history-join-anchor' \
                     WHERE b.id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'"
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
            vec![vec![Value::Json(json!("one").into()),]]
        );

        let semi_join = session
            .execute(
                &format!(
                    "SELECT h.value \
                     FROM lix_key_value_history('{first_commit_id}') AS h \
                     LEFT SEMI JOIN lix_branch AS b \
                       ON true \
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
            vec![vec![Value::Json(json!("one").into()),]]
        );

        let projected = session
            .execute(
                &format!(
                    "SELECT projected.snapshot \
                     FROM (\
                       SELECT key, value AS snapshot \
                       FROM lix_key_value_history('{first_commit_id}')\
                     ) AS projected \
                     WHERE projected.key = 'history-join-anchor'"
                ),
                &[],
            )
            .await
            .expect("an exact anchor should route through a direct projection alias");
        assert_eq!(
            projected
                .rows()
                .iter()
                .map(|row| row.get::<Value>("snapshot").expect("snapshot"))
                .collect::<Vec<_>>(),
            vec![Value::Json(json!("one").into())]
        );
    }
);

simulation_test!(
    history_surfaces_require_function_anchors,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for sql in [
            "SELECT key FROM lix_key_value_history",
            "SELECT lixcol_as_of_commit_id FROM lix_key_value_history()",
            "SELECT key FROM lix_key_value_history('one', 'two')",
            "SELECT key FROM lix_key_value_history(42)",
        ] {
            session
                .execute(sql, &[])
                .await
                .expect_err("invalid history function usage must fail");
        }
    }
);

simulation_test!(
    unrelated_same_named_column_does_not_validate_as_history_anchor,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('collision', 'value')",
                &[],
            )
            .await
            .expect("history row should insert");

        let result = session
            .execute(
                "SELECT ordinary.lixcol_as_of_commit_id \
                 FROM (SELECT 'ordinary' AS lixcol_as_of_commit_id) AS ordinary \
                 CROSS JOIN lix_key_value_history() AS history \
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
                .open_session()
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
                "SELECT '{first_commit_id}' AS anchor, lixcol_depth, value \
                 FROM lix_key_value_history('{first_commit_id}') \
                 WHERE key = 'history-multi-start' AND lixcol_depth = 0 \
                 UNION ALL \
                 SELECT '{second_commit_id}' AS anchor, lixcol_depth, value \
                 FROM lix_key_value_history('{second_commit_id}') \
                 WHERE key = 'history-multi-start' AND lixcol_depth = 0 \
                 ORDER BY anchor"
            ),
        )
        .await;
        assert_eq!(
            in_rows,
            vec![
                vec![
                    Value::Text(first_commit_id.clone()),
                    Value::Integer(0),
                    Value::Json(json!("one").into()),
                ],
                vec![
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                    Value::Json(json!("two").into()),
                ],
            ],
            "multiple history function calls can be unioned"
        );

        let or_rows = select_rows(
            &session,
            &format!(
                "SELECT '{first_commit_id}' AS anchor \
                 FROM lix_key_value_history('{first_commit_id}') \
                 WHERE key = 'history-multi-start' AND lixcol_depth = 0 \
                 UNION ALL \
                 SELECT '{second_commit_id}' AS anchor \
                 FROM lix_key_value_history('{second_commit_id}') \
                 WHERE key = 'history-multi-start' AND lixcol_depth = 0 \
                 ORDER BY anchor"
            ),
        )
        .await;
        assert_eq!(
            or_rows,
            vec![
                vec![Value::Text(first_commit_id)],
                vec![Value::Text(second_commit_id)],
            ],
            "union preserves both explicit anchors"
        );
    }
);

simulation_test!(
    typed_history_intersects_conjunctive_value_filters,
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
                 FROM lix_key_value_history('{head_commit_id}') \
                   WHERE key IN ('history-and-a', 'history-and-b') \
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
                 FROM lix_key_value_history('{head_commit_id}') \
                   WHERE key = 'history-and-a' \
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
