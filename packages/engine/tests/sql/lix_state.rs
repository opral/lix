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
             entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
             WHERE entity_id = lix_json('[\"state-latest\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state update should succeed");

    let result = session
        .execute(
            "SELECT snapshot_content \
             FROM lix_state \
             WHERE entity_id = lix_json('[\"state-latest\"]') AND schema_key = 'lix_key_value'",
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
             entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
             WHERE entity_id = lix_json('[\"state-delete\"]') AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state delete should succeed");

    let result = session
        .execute(
            "SELECT entity_id \
             FROM lix_state \
             WHERE entity_id = lix_json('[\"state-delete\"]') AND schema_key = 'lix_key_value'",
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
                 WHERE entity_id = '[\"state-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_id = '[\"state-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory lix_state update should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = session
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_id = lix_json('[\"state-repeat-b\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("lix_state read should succeed");
        assert_single_text(result, "{\"key\":\"state-repeat-b\",\"value\":\"b\"}");
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
                 WHERE entity_id = '[\"state-tx-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_id = '[\"state-tx-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory transactional update should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = transaction
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_id = lix_json('[\"state-tx-repeat-b\"]') AND schema_key = 'lix_key_value'",
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
                 WHERE entity_id = '[\"state-delete-repeat-a\"]' \
                   AND schema_key = 'lix_key_value' \
                   AND entity_id = '[\"state-delete-repeat-b\"]'",
                &[],
            )
            .await
            .expect("contradictory lix_state delete should succeed with zero rows");
        assert_eq!(result.rows_affected(), 0);

        let result = session
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE entity_id = lix_json('[\"state-delete-repeat-b\"]') AND schema_key = 'lix_key_value'",
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
    lix_state_global_rows_are_visible_through_version_overlay,
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"state-global-overlay\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"state-global-overlay\",\"value\":\"global\"}'), true, false\
                 )",
                &[],
            )
            .await
            .expect("global lix_state insert should succeed");

        let active_result = session
            .execute(
                "SELECT entity_id, global, untracked \
                 FROM lix_state \
                 WHERE entity_id = lix_json('[\"state-global-overlay\"]') AND schema_key = 'lix_key_value'",
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

        let by_version_result = session
            .execute(
                &format!(
                    "SELECT entity_id, version_id, global, untracked \
                 FROM lix_state_by_version \
                 WHERE entity_id = lix_json('[\"state-global-overlay\"]') AND schema_key = 'lix_key_value' \
                 AND version_id IN ('{}', 'global') \
                 ORDER BY version_id",
                    sim.main_version_id()
                ),
                &[],
            )
            .await
            .expect("by-version lix_state read should succeed");
        assert_rows_eq(
            by_version_result,
            vec![
                vec![
                    Value::Json(json!(["state-global-overlay"])),
                    Value::Text(sim.main_version_id().to_string()),
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
    lix_state_version_tombstone_hides_global_row_in_active_and_by_version,
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (\
                 lix_json('[\"state-global-tombstone-overlay\"]'), 'lix_key_value', NULL, NULL, false, false\
                 )",
                &[],
            )
            .await
            .expect("version-local tombstone insert should succeed");

        let active_result = session
            .execute(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE entity_id = lix_json('[\"state-global-tombstone-overlay\"]') AND schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("active lix_state read should succeed");
        assert_rows_eq(active_result, Vec::new());

        let by_version_result = session
            .execute(
                &format!(
                    "SELECT entity_id, version_id, global, untracked \
                     FROM lix_state_by_version \
                     WHERE entity_id = lix_json('[\"state-global-tombstone-overlay\"]') AND schema_key = 'lix_key_value' \
                     AND version_id IN ('{}', 'global') \
                     ORDER BY version_id",
                    sim.main_version_id()
                ),
                &[],
            )
            .await
            .expect("by-version lix_state read should succeed");
        assert_rows_eq(
            by_version_result,
            vec![vec![
                Value::Json(json!(["state-global-tombstone-overlay"])),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]],
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
