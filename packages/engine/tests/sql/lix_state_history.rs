use lix_engine::Value;
use serde_json::json;

simulation_test!(
    lix_state_history_defaults_to_pinned_active_head,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-start-required', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let result = session
            .execute(
                "SELECT lixcol_entity_pk, lixcol_as_of_commit_id \
                 FROM lix_state_history \
                 WHERE lixcol_entity_pk = lix_json('[\"history-start-required\"]')",
                &[],
            )
            .await
            .expect("anchor-free history should use the pinned active head");
        let active_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("active head should load")
            .expect("active branch should have a head");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| {
                    (
                        row.get::<Value>("lixcol_entity_pk")
                            .expect("lixcol_entity_pk"),
                        row.get::<Value>("lixcol_as_of_commit_id")
                            .expect("lixcol_as_of_commit_id"),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(
                Value::Json(json!(["history-start-required"])),
                Value::Text(active_head),
            )]
        );
    }
);

simulation_test!(
    lix_state_history_routes_exact_anchor_from_join_predicate,
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
                     FROM lix_state_history AS h \
                     JOIN lix_state AS active \
                       ON h.lixcol_entity_pk = active.entity_pk \
                      AND h.lixcol_schema_key = active.schema_key \
                      AND h.lixcol_as_of_commit_id = '{first_commit_id}' \
                     WHERE h.lixcol_entity_pk = lix_json('[\"history-join-anchor\"]')"
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
                     LEFT JOIN lix_state_history AS h \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                      AND h.lixcol_entity_pk = lix_json('[\"history-join-anchor\"]') \
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
                     FROM lix_state_history AS h \
                     RIGHT JOIN lix_branch AS b \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                      AND h.lixcol_entity_pk = lix_json('[\"history-join-anchor\"]') \
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
                     FROM lix_state_history AS h \
                     LEFT SEMI JOIN lix_branch AS b \
                       ON h.lixcol_as_of_commit_id = '{first_commit_id}' \
                     WHERE h.lixcol_entity_pk = lix_json('[\"history-join-anchor\"]')"
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
                       SELECT lixcol_entity_pk, lixcol_as_of_commit_id AS anchor \
                       FROM lix_state_history\
                     ) AS projected \
                     WHERE projected.anchor = '{first_commit_id}' \
                       AND projected.lixcol_entity_pk = lix_json('[\"history-join-anchor\"]')"
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
            "SELECT lixcol_entity_pk FROM lix_state_history WHERE lixcol_as_of_commit_id > 'cid_invalid'",
            "SELECT lixcol_entity_pk FROM lix_state_history WHERE lixcol_as_of_commit_id NOT IN ('cid_invalid')",
            "SELECT lixcol_entity_pk FROM lix_state_history WHERE lixcol_as_of_commit_id = 'cid_invalid' OR lixcol_schema_key = 'lix_key_value'",
            "SELECT h.lixcol_entity_pk FROM lix_state_history AS h JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = b.commit_id",
            "SELECT h.lixcol_entity_pk FROM lix_state_history AS h JOIN lix_branch AS b ON h.lixcol_as_of_commit_id > b.commit_id",
            "SELECT h.lixcol_entity_pk FROM lix_state_history AS h LEFT JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.lixcol_entity_pk FROM lix_branch AS b RIGHT JOIN lix_state_history AS h ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.lixcol_entity_pk FROM lix_state_history AS h FULL JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.lixcol_entity_pk FROM lix_state_history AS h LEFT ANTI JOIN lix_branch AS b ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT h.lixcol_entity_pk FROM lix_branch AS b RIGHT ANTI JOIN lix_state_history AS h ON h.lixcol_as_of_commit_id = 'cid_invalid'",
            "SELECT projected.lixcol_entity_pk FROM (SELECT lixcol_entity_pk, lixcol_as_of_commit_id AS anchor FROM lix_state_history) AS projected WHERE projected.anchor > 'cid_invalid'",
            "SELECT limited.lixcol_entity_pk FROM (SELECT lixcol_entity_pk, lixcol_as_of_commit_id AS anchor FROM lix_state_history LIMIT 1) AS limited WHERE limited.anchor = 'cid_invalid'",
            "SELECT key FROM lix_key_value_history WHERE lixcol_as_of_commit_id > 'cid_invalid'",
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
                 CROSS JOIN lix_state_history AS history \
                 WHERE ordinary.lixcol_as_of_commit_id > 'a' \
                   AND history.lixcol_entity_pk = lix_json('[\"collision\"]') \
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
    lix_state_history_accepts_active_branch_commit_id_filter,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-active-head', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let rows = select_history_rows(
            &session,
            "SELECT lixcol_entity_pk FROM lix_state_history WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id()",
        )
        .await;

        assert!(
            rows.iter()
                .any(|row| row.first() == Some(&Value::Json(json!(["history-active-head"])))),
            "expected active-head history row, got {rows:?}"
        );
    }
);

simulation_test!(
    lix_state_history_rejects_retired_anchor_names,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-prefixed-start', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        for retired in ["start_commit_id", "lixcol_start_commit_id"] {
            let error = session
                .execute(
                    &format!(
                        "SELECT lixcol_entity_pk \
                         FROM lix_state_history \
                         WHERE {retired} = lix_active_branch_commit_id()"
                    ),
                    &[],
                )
                .await
                .expect_err("retired history anchor must fail");

            assert_eq!(error.code, lix_engine::LixError::CODE_COLUMN_NOT_FOUND);
            assert!(
                error.to_string().contains(retired),
                "unexpected error: {error}"
            );
        }
    }
);

simulation_test!(
    lix_state_history_rejects_bare_system_column_names,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for retired in [
            "entity_pk",
            "schema_key",
            "observed_commit_id",
            "commit_created_at",
            "depth",
        ] {
            let error = session
                .execute(
                    &format!(
                        "SELECT {retired} \
                         FROM lix_state_history \
                         WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id()"
                    ),
                    &[],
                )
                .await
                .expect_err("bare history system columns must fail");
            assert_eq!(error.code, lix_engine::LixError::CODE_COLUMN_NOT_FOUND);
            assert!(
                error.to_string().contains(retired),
                "unexpected error: {error}"
            );
        }
    }
);

simulation_test!(
    lix_state_history_reads_from_explicit_historical_commit,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-explicit', 'one')",
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
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'history-explicit'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");

        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'history-explicit'",
                &[],
            )
            .await
            .expect("tombstone write should succeed");
        let third_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("third head should load")
            .expect("third head should exist");

        assert_ne!(first_commit_id, second_commit_id);
        assert_ne!(second_commit_id, third_commit_id);

        let first_history = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id, lixcol_depth, lixcol_snapshot_content, lixcol_change_id, lixcol_observed_commit_id, lixcol_commit_created_at, lixcol_change_created_at, lixcol_is_deleted \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{first_commit_id}' \
                   AND lixcol_entity_pk = lix_json('[\"history-explicit\"]') \
                 ORDER BY lixcol_depth"
            ),
        )
        .await;
        assert_eq!(
            &first_history[0][0..3],
            &[
                Value::Text(first_commit_id.clone()),
                Value::Integer(0),
                Value::Json(json!({"key": "history-explicit", "value": "one"})),
            ],
            "historical commit should be queryable after later commits"
        );
        let Value::Text(first_change_id) = &first_history[0][3] else {
            panic!("lixcol_change_id should be text");
        };
        let Value::Text(first_row_commit_id) = &first_history[0][4] else {
            panic!("lixcol_observed_commit_id should be text");
        };
        let Value::Text(first_commit_created_at) = &first_history[0][5] else {
            panic!("lixcol_commit_created_at should be text");
        };
        let Value::Text(first_change_created_at) = &first_history[0][6] else {
            panic!("lixcol_change_created_at should be text");
        };
        assert!(!first_change_id.is_empty());
        assert_eq!(first_row_commit_id, &first_commit_id);
        assert!(
            !first_commit_created_at.is_empty(),
            "lixcol_commit_created_at should be populated"
        );
        assert!(!first_change_created_at.is_empty());
        assert_eq!(first_history[0][7], Value::Boolean(false));

        let reversed_parameter = session
            .execute(
                "SELECT lixcol_as_of_commit_id, lixcol_snapshot_content \
                 FROM lix_state_history \
                 WHERE $1 = lixcol_as_of_commit_id \
                   AND lixcol_entity_pk = lix_json('[\"history-explicit\"]')",
                &[Value::Text(first_commit_id.clone())],
            )
            .await
            .expect("a reversed parameter equality should route the history anchor");
        assert_eq!(
            reversed_parameter
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Text(first_commit_id.clone()),
                Value::Json(json!({"key": "history-explicit", "value": "one"})),
            ]]
        );

        let second_history = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_depth, lixcol_snapshot_content, lixcol_is_deleted \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
                   AND lixcol_entity_pk = lix_json('[\"history-explicit\"]') \
                 ORDER BY lixcol_depth"
            ),
        )
        .await;
        assert_eq!(
            second_history,
            vec![
                vec![
                    Value::Integer(0),
                    Value::Json(json!({"key": "history-explicit", "value": "two"})),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Integer(1),
                    Value::Json(json!({"key": "history-explicit", "value": "one"})),
                    Value::Boolean(false),
                ],
            ],
            "lixcol_depth 0 is the as-of commit and parent changes appear at lixcol_depth > 0"
        );

        let tombstone_history = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_depth, lixcol_snapshot_content, lixcol_is_deleted \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{third_commit_id}' \
                   AND lixcol_entity_pk = lix_json('[\"history-explicit\"]') \
                   AND lixcol_depth = 0 \
                   AND lixcol_snapshot_content IS NULL"
            ),
        )
        .await;
        assert_eq!(
            tombstone_history,
            vec![vec![Value::Integer(0), Value::Null, Value::Boolean(true)]],
            "tombstone changes should be visible as NULL lixcol_snapshot_content"
        );
    }
);

simulation_test!(
    lix_state_history_routes_schema_entity_file_and_depth_filters,
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
                 VALUES ('history-file-a', '/history/a.txt', X'61')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first head should load")
            .expect("first head should exist");

        session
            .execute(
                "UPDATE lix_file SET data = X'62' WHERE id = 'history-file-a'",
                &[],
            )
            .await
            .expect("file update should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second head should load")
            .expect("second head should exist");

        let rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_entity_pk, lixcol_schema_key, lixcol_file_id, lixcol_depth \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
                   AND lixcol_schema_key = 'lix_binary_blob_ref' \
                   AND lixcol_entity_pk = lix_json('[\"history-file-a\"]') \
                   AND lixcol_file_id = 'history-file-a' \
                   AND lixcol_depth >= 0 \
                   AND lixcol_depth <= 1 \
                 ORDER BY lixcol_depth"
            ),
        )
        .await;
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Json(json!(["history-file-a"])),
                    Value::Text("lix_binary_blob_ref".to_string()),
                    Value::Text("history-file-a".to_string()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Json(json!(["history-file-a"])),
                    Value::Text("lix_binary_blob_ref".to_string()),
                    Value::Text("history-file-a".to_string()),
                    Value::Integer(1),
                ],
            ],
            "lixcol_schema_key, lixcol_entity_pk, lixcol_file_id, and lixcol_depth range filters should route through the provider"
        );

        let parent_only_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id, lixcol_depth \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
                   AND lixcol_schema_key = 'lix_binary_blob_ref' \
                   AND lixcol_entity_pk = lix_json('[\"history-file-a\"]') \
                   AND lixcol_file_id = 'history-file-a' \
                   AND lixcol_depth > 0 \
                   AND lixcol_depth < 2"
            ),
        )
        .await;
        assert_eq!(
            parent_only_rows,
            vec![vec![Value::Text(second_commit_id), Value::Integer(1)]],
            "strict lixcol_depth ranges should keep only matching parent rows"
        );

        let historical_start_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id, lixcol_depth \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{first_commit_id}' \
                   AND lixcol_schema_key = 'lix_binary_blob_ref' \
                   AND lixcol_entity_pk = lix_json('[\"history-file-a\"]') \
                   AND lixcol_file_id = 'history-file-a'"
            ),
        )
        .await;
        assert_eq!(
            historical_start_rows,
            vec![vec![Value::Text(first_commit_id), Value::Integer(0)]],
            "lixcol_file_id filtering should also work for historical non-head starts"
        );
    }
);

simulation_test!(
    lix_state_history_shows_tombstone_at_ancestor_depth,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('history-ancestor-tombstone', 'one')",
                &[],
            )
            .await
            .expect("initial tracked write should succeed");

        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'history-ancestor-tombstone'",
                &[],
            )
            .await
            .expect("delete should succeed");
        let delete_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("delete head should load")
            .expect("delete head should exist");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('history-unrelated-after-delete', 'later')",
                &[],
            )
            .await
            .expect("unrelated later write should succeed");
        let later_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("later head should load")
            .expect("later head should exist");
        assert_ne!(delete_commit_id, later_commit_id);

        let tombstone_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_observed_commit_id, lixcol_depth, lixcol_snapshot_content \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{later_commit_id}' \
                   AND lixcol_entity_pk = lix_json('[\"history-ancestor-tombstone\"]') \
                   AND lixcol_snapshot_content IS NULL \
                 ORDER BY lixcol_depth"
            ),
        )
        .await;
        assert_eq!(
            tombstone_rows,
            vec![vec![
                Value::Text(delete_commit_id),
                Value::Integer(1),
                Value::Null,
            ]],
            "a tombstone from the parent commit should appear at ancestor lixcol_depth"
        );
    }
);

simulation_test!(
    lix_state_history_supports_multiple_as_of_commit_filters,
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

        let in_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id, lixcol_depth, lixcol_snapshot_content \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id IN ('{first_commit_id}', '{second_commit_id}') \
                   AND lixcol_entity_pk = lix_json('[\"history-multi-start\"]') \
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

        let or_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_as_of_commit_id \
                 FROM lix_state_history \
                 WHERE (lixcol_as_of_commit_id = '{first_commit_id}' \
                        OR lixcol_as_of_commit_id = '{second_commit_id}') \
                   AND lixcol_entity_pk = lix_json('[\"history-multi-start\"]') \
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
    lix_state_history_intersects_conjunctive_value_filters,
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

        let narrowed_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_entity_pk \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{head_commit_id}' \
                   AND lixcol_entity_pk IN (lix_json('[\"history-and-a\"]'), lix_json('[\"history-and-b\"]')) \
                   AND lixcol_entity_pk = lix_json('[\"history-and-a\"]')"
            ),
        )
        .await;
        assert_eq!(
            narrowed_rows,
            vec![vec![Value::Json(json!(["history-and-a"]))]],
            "AND filters on the same history column should intersect, not union"
        );

        let contradictory_rows = select_history_rows(
            &session,
            &format!(
                "SELECT lixcol_entity_pk \
                 FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{head_commit_id}' \
                   AND lixcol_entity_pk = lix_json('[\"history-and-a\"]') \
                   AND lixcol_entity_pk = lix_json('[\"history-and-b\"]')"
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

async fn select_history_rows(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> Vec<Vec<Value>> {
    let result = session
        .execute(sql, &[])
        .await
        .expect("history SELECT should succeed");
    let row_set = result;
    row_set
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}
