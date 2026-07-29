use lix_engine::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    diff_commands_apply_revert_and_checkpoint_selections,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let baseline = sim.initial_commit_id().to_string();

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('a', 'one')",
                &[],
            )
            .await
            .expect("first tracked insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('b', 'two')",
                &[],
            )
            .await
            .expect("second tracked insert should succeed");
        let original_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        let working = select_rows(
            &session,
            "SELECT diff_id, entity_pk FROM lix_working_diff \
         WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
        )
        .await;
        assert_eq!(working.len(), 2);
        assert!(matches!(&working[0][0], Value::Text(id) if id.starts_with("d1.")));

        let values_error = session
            .execute(
                "INSERT INTO lix_revert (diff_id) VALUES ('d1.invalid')",
                &[],
            )
            .await
            .expect_err("command sinks must reject VALUES");
        assert_eq!(values_error.code, LixError::CODE_UNSUPPORTED_SQL);

        session
            .execute(
                "INSERT INTO lix_revert (diff_id) \
             SELECT diff_id FROM lix_working_diff \
             WHERE schema_key = 'lix_key_value' \
             ORDER BY entity_pk \
             LIMIT 1",
                &[],
            )
            .await
            .expect("selected revert should succeed");
        assert_eq!(
            select_rows(
                &session,
                "SELECT key, value FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key",
            )
            .await,
            vec![vec![
                Value::Text("b".to_string()),
                Value::Json(json!("two")),
            ]]
        );

        let historical_diff = format!(
            "SELECT diff_id, before_change_id, after_change_id \
         FROM lix_diff('{baseline}', '{original_head}') \
         WHERE schema_key = 'lix_key_value' \
           AND entity_pk = lix_json('[\"a\"]')"
        );
        let historical_rows = select_rows(&session, &historical_diff).await;
        assert_eq!(historical_rows.len(), 1);
        assert_eq!(historical_rows[0][1], Value::Null);
        assert!(matches!(historical_rows[0][2], Value::Text(_)));

        session
            .execute(
                "INSERT INTO lix_apply (diff_id) \
                 SELECT diff_id FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"a\"]')",
                &[
                    Value::Text(baseline.clone()),
                    Value::Text(original_head.to_string()),
                ],
            )
            .await
            .expect("selected historical apply should succeed");

        let stale = session
            .execute(
                "INSERT INTO lix_apply (diff_id) \
                 SELECT diff_id FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"a\"]')",
                &[
                    Value::Text(baseline),
                    Value::Text(original_head.to_string()),
                ],
            )
            .await
            .expect_err("strict apply must reject a moved head");
        assert_eq!(stale.code, LixError::CODE_CONSTRAINT_VIOLATION);

        session
            .execute(
                "INSERT INTO lix_create_checkpoint (diff_id) \
             SELECT diff_id FROM lix_working_diff \
             WHERE schema_key = $1 \
               AND entity_pk = lix_json($2)",
                &[
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("[\"a\"]".to_string()),
                ],
            )
            .await
            .expect("partial checkpoint should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT entity_pk FROM lix_working_diff \
             WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
            )
            .await,
            vec![vec![Value::Json(json!(["b"]))]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT key, value FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key",
            )
            .await,
            vec![
                vec![Value::Text("a".to_string()), Value::Json(json!("one"))],
                vec![Value::Text("b".to_string()), Value::Json(json!("two"))],
            ]
        );
    }
);
