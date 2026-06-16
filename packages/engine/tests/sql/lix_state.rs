use lix_engine::ExecuteResult;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(lix_state_latest_update_wins, |sim| async move {
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
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-latest\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-latest\",\"value\":\"old\"}'), false, false\
             )",
            &[],
        )
        .await
        .expect("lix_state insert should succeed");
    session
        .execute(
            "UPDATE lix_state \
             SET snapshot_content = lix_json('{\"key\":\"state-latest\",\"value\":\"new\"}') \
             WHERE entity_pk = lix_json('[\"state-latest\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state update should succeed");

    let result = session
        .execute(
            "SELECT snapshot_content \
             FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-latest\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state read should succeed");
    assert_single_text(result, "{\"key\":\"state-latest\",\"value\":\"new\"}");
});

simulation_test!(lix_state_delete_hides_row, |sim| async move {
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
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-delete\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-delete\",\"value\":\"delete-me\"}'), false, false\
             )",
            &[],
        )
        .await
        .expect("lix_state insert should succeed");
    session
        .execute(
            "DELETE FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-delete\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state delete should succeed");

    let result = session
        .execute(
            "SELECT entity_pk \
             FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-delete\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state read should succeed");
    let rows = result;
    assert_eq!(rows.len(), 0);
});

simulation_test!(
    lix_state_update_intersects_repeated_identity_predicates,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 ('[\"state-repeat-a\"]', 'lix_key_value', NULL, '{\"key\":\"state-repeat-a\",\"value\":\"a\"}', false, false), \
                 ('[\"state-repeat-b\"]', 'lix_key_value', NULL, '{\"key\":\"state-repeat-b\",\"value\":\"b\"}', false, false)",
                &[],
            )
            .await
            .expect("lix_state insert should succeed");

        let result = session
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"key\":\"state-repeat-b\",\"value\":\"wrong\"}' \
                 WHERE entity_pk = '[\"state-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_pk = '[\"state-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory lix_state update should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = session
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-repeat-b\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        assert_single_text(result, "{\"key\":\"state-repeat-b\",\"value\":\"b\"}");
    }
);

simulation_test!(
    lix_state_update_accepts_parseable_json_text_identity_predicate,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('state-json-text', 'before')",
                &[],
            )
            .await
            .expect("key value insert should succeed");

        let update = session
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = lix_json('{\"key\":\"state-json-text\",\"value\":\"after\"}') \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = '[ \"state-json-text\" ]'",
                &[],
            )
            .await
            .expect("parseable JSON text identity predicate should update lix_state");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'state-json-text'",
                &[],
            )
            .await
            .expect("key value read should succeed");
        assert_rows_eq(result, vec![vec![Value::Json(json!("after"))]]);
    }
);

simulation_test!(
    lix_state_update_intersects_repeated_identity_predicates_in_transaction,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 ('[\"state-tx-repeat-a\"]', 'lix_key_value', NULL, '{\"key\":\"state-tx-repeat-a\",\"value\":\"a\"}', false, false), \
                 ('[\"state-tx-repeat-b\"]', 'lix_key_value', NULL, '{\"key\":\"state-tx-repeat-b\",\"value\":\"b\"}', false, false)",
                &[],
            )
            .await
            .expect("transactional lix_state insert should succeed");

        let result = transaction
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"key\":\"state-tx-repeat-b\",\"value\":\"wrong\"}' \
                 WHERE entity_pk = '[\"state-tx-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_pk = '[\"state-tx-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory transactional update should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = transaction
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-tx-repeat-b\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("transactional lix_state read should succeed");
        assert_single_text(result, "{\"key\":\"state-tx-repeat-b\",\"value\":\"b\"}");
    }
);

simulation_test!(
    lix_state_delete_intersects_repeated_identity_predicates,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 ('[\"state-delete-repeat-a\"]', 'lix_key_value', NULL, '{\"key\":\"state-delete-repeat-a\",\"value\":\"a\"}', false, false), \
                 ('[\"state-delete-repeat-b\"]', 'lix_key_value', NULL, '{\"key\":\"state-delete-repeat-b\",\"value\":\"b\"}', false, false)",
                &[],
            )
            .await
            .expect("lix_state insert should succeed");

        let result = session
            .execute(
                "DELETE FROM lix_state \
                 WHERE entity_pk = '[\"state-delete-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_pk = '[\"state-delete-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory lix_state delete should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = session
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-delete-repeat-b\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        assert_single_text(
            result,
            "{\"key\":\"state-delete-repeat-b\",\"value\":\"b\"}",
        );
    }
);

simulation_test!(
    lix_state_transaction_insert_global_row_is_visible_before_commit,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 '[\"state-tx-global\"]', 'lix_key_value', NULL, '{\"key\":\"state-tx-global\",\"value\":\"global\"}', true, false\
                 )",
                &[],
            )
            .await
            .expect("transactional global lix_state insert should succeed");

        let result = transaction
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-tx-global\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("transactional active read should succeed");
        assert_single_text(result, "{\"key\":\"state-tx-global\",\"value\":\"global\"}");

        let result = transaction
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"key\":\"state-tx-global\",\"value\":\"updated\"}' \
                 WHERE entity_pk = '[\"state-tx-global\"]' AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("transactional active update should see staged global row");
        assert_eq!(result.rows_affected(), 1);

        let result = transaction
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-tx-global\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("transactional active read after update should succeed");
        assert_single_text(
            result,
            "{\"key\":\"state-tx-global\",\"value\":\"updated\"}",
        );
    }
);

simulation_test!(
    lix_state_transaction_global_row_does_not_override_active_row,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 '[\"state-tx-global-shadowed\"]', 'lix_key_value', NULL, '{\"key\":\"state-tx-global-shadowed\",\"value\":\"active\"}', false, false\
                 )",
                &[],
            )
            .await
            .expect("active lix_state insert should succeed");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 '[\"state-tx-global-shadowed\"]', 'lix_key_value', NULL, '{\"key\":\"state-tx-global-shadowed\",\"value\":\"global\"}', true, false\
                 )",
                &[],
            )
            .await
            .expect("transactional global lix_state insert should succeed");

        let result = transaction
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-tx-global-shadowed\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("transactional active read should succeed");
        assert_single_text(
            result,
            "{\"key\":\"state-tx-global-shadowed\",\"value\":\"active\"}",
        );
    }
);

simulation_test!(
    lix_state_global_rows_are_visible_through_branch_overlay,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"state-global-overlay\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-global-overlay\",\"value\":\"global\"}'), true, false\
                 )",
                &[],
            )
            .await
            .expect("global lix_state insert should succeed");

        let active_result = session
            .execute(
                "SELECT entity_pk, global, untracked \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-global-overlay\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("active lix_state read should succeed");
        assert_rows_eq(
            active_result,
            vec![vec![
                Value::Json(json!(["state-global-overlay"])),
                Value::Boolean(true),
                Value::Boolean(false),
            ]],
        );

        let by_branch_result = session
            .execute(
                &format!(
                    "SELECT entity_pk, branch_id, global, untracked \
                 FROM lix_state_by_branch \
                 WHERE entity_pk = lix_json('[\"state-global-overlay\"]') AND schema_key = 'lix_key_value' \
                 AND branch_id IN ('{}', 'global') \
                 ORDER BY branch_id",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("by-branch lix_state read should succeed");
        assert_rows_eq(
            by_branch_result,
            vec![
                vec![
                    Value::Json(json!(["state-global-overlay"])),
                    Value::Text(sim.main_branch_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Json(json!(["state-global-overlay"])),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_state_branch_tombstone_hides_global_row_in_active_and_by_branch,
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
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"state-global-tombstone-overlay\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-global-tombstone-overlay\",\"value\":\"global\"}'), true, false\
                 )",
                &[],
            )
            .await
            .expect("global lix_state insert should succeed");
        session
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"state-global-tombstone-overlay\"]'), 'lix_key_value', NULL, NULL, false, false\
                 )",
                &[],
            )
            .await
            .expect("branch-local tombstone insert should succeed");

        let active_result = session
            .execute(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE entity_pk = lix_json('[\"state-global-tombstone-overlay\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("active lix_state read should succeed");
        assert_rows_eq(active_result, Vec::new());

        let by_branch_result = session
            .execute(
                &format!(
                    "SELECT entity_pk, branch_id, global, untracked \
                     FROM lix_state_by_branch \
                     WHERE entity_pk = lix_json('[\"state-global-tombstone-overlay\"]') AND schema_key = 'lix_key_value' \
                     AND branch_id IN ('{}', 'global') \
                     ORDER BY branch_id",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("by-branch lix_state read should succeed");
        assert_rows_eq(
            by_branch_result,
            vec![vec![
                Value::Json(json!(["state-global-tombstone-overlay"])),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]],
        );
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_update_uses_excluded,
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
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-upsert\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-upsert\",\"value\":\"old\"}'), false, false\
             )",
            &[],
        )
        .await
        .expect("seed insert should succeed");

        let result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-upsert\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-upsert\",\"value\":\"new\"}'), false, false\
             ) ON CONFLICT (entity_pk, schema_key, file_id) DO UPDATE SET snapshot_content = excluded.snapshot_content",
            &[],
        )
        .await
        .expect("upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT snapshot_content FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-upsert\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("read should succeed");
        assert_single_text(read, "{\"key\":\"state-upsert\",\"value\":\"new\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_nothing_keeps_existing,
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
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-nothing\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-nothing\",\"value\":\"keep\"}'), false, false\
             )",
            &[],
        )
        .await
        .expect("seed insert should succeed");

        let result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-nothing\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-nothing\",\"value\":\"ignored\"}'), false, false\
             ) ON CONFLICT (entity_pk, schema_key, file_id) DO NOTHING",
            &[],
        )
        .await
        .expect("upsert DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT snapshot_content FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-nothing\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("read should succeed");
        assert_single_text(read, "{\"key\":\"state-nothing\",\"value\":\"keep\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_inserts_when_absent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-fresh\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-fresh\",\"value\":\"created\"}'), false, false\
             ) ON CONFLICT (entity_pk, schema_key, file_id) DO UPDATE SET snapshot_content = excluded.snapshot_content",
            &[],
        )
        .await
        .expect("upsert on absent row should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT snapshot_content FROM lix_state \
             WHERE entity_pk = lix_json('[\"state-fresh\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("read should succeed");
        assert_single_text(read, "{\"key\":\"state-fresh\",\"value\":\"created\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_rejects_partial_target,
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
            "INSERT INTO lix_state (\
             entity_pk, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES (\
             lix_json('[\"state-partial-target\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-partial-target\",\"value\":\"new\"}'), false, false\
             ) ON CONFLICT (entity_pk, schema_key) DO NOTHING",
            &[],
        )
        .await
        .expect_err("partial conflict target should be rejected");

        assert!(
            error
                .message
                .contains("target must match conflict identity columns"),
            "expected conflict identity target error: {error}"
        );
    }
);

fn assert_single_text(result: ExecuteResult, expected: &str) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let expected_json = serde_json::from_str::<serde_json::Value>(expected)
        .expect("expected snapshot_content should be valid JSON");
    assert_eq!(row_set.rows()[0].values(), &[Value::Json(expected_json)]);
}
