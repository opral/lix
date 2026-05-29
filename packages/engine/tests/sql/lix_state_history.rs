use lix_engine::Value;
use serde_json::json;

simulation_test!(
    lix_state_history_requires_start_commit_id,
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

        let error = session
            .execute("SELECT entity_pk FROM lix_state_history", &[])
            .await
            .expect_err("history queries must provide start_commit_id");

        assert!(
            error
                .to_string()
                .contains("requires a start_commit_id filter")
        );
        assert_eq!(
            error.code,
            lix_engine::LixError::CODE_HISTORY_FILTER_REQUIRED
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("lix_active_branch_commit_id()")),
            "expected active-branch-head hint: {error}"
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
            "SELECT entity_pk FROM lix_state_history WHERE start_commit_id = lix_active_branch_commit_id()",
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
    lix_state_history_rejects_prefixed_start_commit_id_filter,
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

        let error = session
            .execute(
                "SELECT entity_pk \
                 FROM lix_state_history \
                 WHERE lixcol_start_commit_id = lix_active_branch_commit_id()",
                &[],
            )
            .await
            .expect_err("lix_state_history should only expose bare start_commit_id");

        assert_eq!(error.code, lix_engine::LixError::CODE_COLUMN_NOT_FOUND);
        assert!(
            error.to_string().contains("lixcol_start_commit_id"),
            "unexpected error: {error}"
        );
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
                "SELECT start_commit_id, depth, snapshot_content, change_id, observed_commit_id, commit_created_at \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{first_commit_id}' \
                   AND entity_pk = lix_json('[\"history-explicit\"]') \
                 ORDER BY depth"
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
            panic!("change_id should be text");
        };
        let Value::Text(first_row_commit_id) = &first_history[0][4] else {
            panic!("observed_commit_id should be text");
        };
        let Value::Text(first_commit_created_at) = &first_history[0][5] else {
            panic!("commit_created_at should be text");
        };
        assert!(!first_change_id.is_empty());
        assert_eq!(first_row_commit_id, &first_commit_id);
        assert!(
            !first_commit_created_at.is_empty(),
            "commit_created_at should be populated"
        );

        let second_history = select_history_rows(
            &session,
            &format!(
                "SELECT depth, snapshot_content \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{second_commit_id}' \
                   AND entity_pk = lix_json('[\"history-explicit\"]') \
                 ORDER BY depth"
            ),
        )
        .await;
        assert_eq!(
            second_history,
            vec![
                vec![
                    Value::Integer(0),
                    Value::Json(json!({"key": "history-explicit", "value": "two"})),
                ],
                vec![
                    Value::Integer(1),
                    Value::Json(json!({"key": "history-explicit", "value": "one"})),
                ],
            ],
            "depth 0 is the start commit and parent changes appear at depth > 0"
        );

        let tombstone_history = select_history_rows(
            &session,
            &format!(
                "SELECT depth, snapshot_content \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{third_commit_id}' \
                   AND entity_pk = lix_json('[\"history-explicit\"]') \
                   AND depth = 0 \
                   AND snapshot_content IS NULL"
            ),
        )
        .await;
        assert_eq!(
            tombstone_history,
            vec![vec![Value::Integer(0), Value::Null]],
            "tombstone changes should be visible as NULL snapshot_content"
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
                "SELECT entity_pk, schema_key, file_id, depth \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{second_commit_id}' \
                   AND schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"history-file-a\"]') \
                   AND file_id = 'history-file-a' \
                   AND depth >= 0 \
                   AND depth <= 1 \
                 ORDER BY depth"
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
            "schema_key, entity_pk, file_id, and depth range filters should route through the provider"
        );

        let parent_only_rows = select_history_rows(
            &session,
            &format!(
                "SELECT start_commit_id, depth \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{second_commit_id}' \
                   AND schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"history-file-a\"]') \
                   AND file_id = 'history-file-a' \
                   AND depth > 0 \
                   AND depth < 2"
            ),
        )
        .await;
        assert_eq!(
            parent_only_rows,
            vec![vec![Value::Text(second_commit_id), Value::Integer(1)]],
            "strict depth ranges should keep only matching parent rows"
        );

        let historical_start_rows = select_history_rows(
            &session,
            &format!(
                "SELECT start_commit_id, depth \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{first_commit_id}' \
                   AND schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"history-file-a\"]') \
                   AND file_id = 'history-file-a'"
            ),
        )
        .await;
        assert_eq!(
            historical_start_rows,
            vec![vec![Value::Text(first_commit_id), Value::Integer(0)]],
            "file_id filtering should also work for historical non-head starts"
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
                "SELECT observed_commit_id, depth, snapshot_content \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{later_commit_id}' \
                   AND entity_pk = lix_json('[\"history-ancestor-tombstone\"]') \
                   AND snapshot_content IS NULL \
                 ORDER BY depth"
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
            "a tombstone from the parent commit should appear at ancestor depth"
        );
    }
);

simulation_test!(
    lix_state_history_supports_multiple_start_commit_filters,
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
                "SELECT start_commit_id, depth, snapshot_content \
                 FROM lix_state_history \
                 WHERE start_commit_id IN ('{first_commit_id}', '{second_commit_id}') \
                   AND entity_pk = lix_json('[\"history-multi-start\"]') \
                   AND depth = 0 \
                 ORDER BY start_commit_id"
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
            "IN should allow multiple explicit history starts"
        );

        let or_rows = select_history_rows(
            &session,
            &format!(
                "SELECT start_commit_id \
                 FROM lix_state_history \
                 WHERE (start_commit_id = '{first_commit_id}' \
                        OR start_commit_id = '{second_commit_id}') \
                   AND entity_pk = lix_json('[\"history-multi-start\"]') \
                   AND depth = 0 \
                 ORDER BY start_commit_id"
            ),
        )
        .await;
        assert_eq!(
            or_rows,
            vec![
                vec![Value::Text(first_commit_id)],
                vec![Value::Text(second_commit_id)],
            ],
            "OR should also allow multiple explicit history starts"
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
                "SELECT entity_pk \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{head_commit_id}' \
                   AND entity_pk IN (lix_json('[\"history-and-a\"]'), lix_json('[\"history-and-b\"]')) \
                   AND entity_pk = lix_json('[\"history-and-a\"]')"
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
                "SELECT entity_pk \
                 FROM lix_state_history \
                 WHERE start_commit_id = '{head_commit_id}' \
                   AND entity_pk = lix_json('[\"history-and-a\"]') \
                   AND entity_pk = lix_json('[\"history-and-b\"]')"
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
