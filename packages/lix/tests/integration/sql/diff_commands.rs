use lix::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    diff_commands_apply_revert_and_checkpoint_selections,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
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
            "SELECT diff_id, entity_pk, diff_type FROM lix_working_diff \
         WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
        )
        .await;
        assert_eq!(working.len(), 2);
        assert!(matches!(&working[0][0], Value::Text(id) if id.starts_with("d1.")));
        assert_eq!(working[0][2], Value::Text("added".to_string()));

        let values_error = session
            .execute(
                "INSERT INTO lix_revert (diff_id) VALUES ('d1.invalid')",
                &[],
            )
            .await
            .expect_err("command sinks must reject VALUES");
        assert_eq!(values_error.code, LixError::CODE_UNSUPPORTED_SQL);

        let selected_diff_id = working[0][0].clone();
        let reverted = session
            .execute(
                "INSERT INTO lix_revert (diff_id) \
                 SELECT diff_id \
                 FROM (VALUES ($1)) AS selected(diff_id) \
                 RETURNING commit_id",
                &[selected_diff_id],
            )
            .await
            .expect("selected revert should succeed");
        assert_eq!(reverted.rows_affected(), 1);
        assert_eq!(reverted.columns(), &["commit_id"]);
        assert_eq!(reverted.len(), 1);
        assert!(matches!(
            reverted.get(&reverted.rows()[0], "commit_id"),
            Some(Value::Text(commit_id)) if !commit_id.is_empty()
        ));
        assert_eq!(
            select_rows(
                &session,
                "SELECT key, value FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key",
            )
            .await,
            vec![vec![
                Value::Text("b".to_string()),
                Value::Json(json!("two").into()),
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

        let applied = session
            .execute(
                "INSERT INTO lix_apply (diff_id) \
                 SELECT diff_id FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"a\"]') \
                 RETURNING commit_id",
                &[
                    Value::Text(baseline.clone()),
                    Value::Text(original_head.to_string()),
                ],
            )
            .await
            .expect("selected historical apply should succeed");
        assert_eq!(applied.rows_affected(), 1);
        assert_eq!(applied.columns(), &["commit_id"]);
        assert_eq!(applied.len(), 1);

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
        assert_eq!(
            stale.message,
            "stale or unknown diff_id; re-evaluate the source diff and retry"
        );

        let checkpointed = session
            .execute(
                "INSERT INTO lix_create_checkpoint (diff_id) \
             SELECT diff_id FROM lix_working_diff \
             WHERE schema_key = $1 \
               AND entity_pk = lix_json($2) \
             RETURNING commit_id",
                &[
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("[\"a\"]".to_string()),
                ],
            )
            .await
            .expect("partial checkpoint should succeed");
        assert_eq!(checkpointed.rows_affected(), 1);
        assert_eq!(checkpointed.columns(), &["commit_id"]);
        assert_eq!(checkpointed.len(), 1);
        let checkpoint_commit_id = match checkpointed.get(&checkpointed.rows()[0], "commit_id") {
            Some(Value::Text(commit_id)) => commit_id.clone(),
            value => panic!("checkpoint RETURNING should contain a commit ID, got {value:?}"),
        };
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT commit_id, lixcol_global FROM lix_checkpoint \
                     WHERE commit_id = '{checkpoint_commit_id}'"
                ),
            )
            .await,
            vec![vec![
                Value::Text(checkpoint_commit_id.clone()),
                Value::Boolean(true),
            ]],
            "partial checkpoint publication must remain globally owned",
        );
        let child_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("partial checkpoint child head should load")
            .expect("partial checkpoint child head should exist");
        assert_ne!(child_head.to_string(), checkpoint_commit_id);

        assert_eq!(
            select_rows(
                &session,
                "SELECT entity_pk FROM lix_working_diff \
             WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
            )
            .await,
            vec![vec![Value::Json(json!(["b"]).into())]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT key, value FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key",
            )
            .await,
            vec![
                vec![Value::Text("a".to_string()), Value::Json(json!("one").into())],
                vec![Value::Text("b".to_string()), Value::Json(json!("two").into())],
            ]
        );

        let head_before_empty = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head before empty selection should load")
            .expect("head before empty selection should exist");
        let empty = session
            .execute(
                "INSERT INTO lix_revert (diff_id) \
                 SELECT diff_id FROM lix_working_diff WHERE 1 = 0 \
                 RETURNING commit_id",
                &[],
            )
            .await
            .expect("empty diff selection should be a successful no-op");
        assert_eq!(empty.rows_affected(), 0);
        assert_eq!(empty.columns(), &["commit_id"]);
        assert!(empty.is_empty());
        let head_after_empty = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head after empty selection should load")
            .expect("head after empty selection should exist");
        assert_eq!(head_after_empty, head_before_empty);
    }
);
