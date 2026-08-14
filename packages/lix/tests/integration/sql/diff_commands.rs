use lix::{CreateBranchOptions, LixError, MergeBranchOptions, MergeBranchPreviewOptions, Value};
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
                 FROM VALUES ($1) AS selected(diff_id) \
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
        let checkpoint_row = session
            .execute(
                "SELECT commit_id FROM lix_checkpoint WHERE commit_id = $1",
                &[Value::Text(checkpoint_commit_id.clone())],
            )
            .await
            .expect("returned checkpoint commit should be queryable");
        assert_eq!(checkpoint_row.len(), 1);
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

        let second_checkpoint = session
            .execute(
                "INSERT INTO lix_create_checkpoint (diff_id) \
                 SELECT diff_id FROM lix_working_diff \
                 WHERE schema_key = $1 \
                   AND entity_pk = lix_json($2) \
                 RETURNING commit_id",
                &[
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("[\"b\"]".to_string()),
                ],
            )
            .await
            .expect("second partial checkpoint should succeed");
        assert_eq!(second_checkpoint.rows_affected(), 1);
        let second_checkpoint_commit_id =
            match second_checkpoint.get(&second_checkpoint.rows()[0], "commit_id") {
                Some(Value::Text(commit_id)) => commit_id.clone(),
                value => {
                    panic!("second checkpoint RETURNING should contain a commit ID, got {value:?}")
                }
            };
        assert_ne!(second_checkpoint_commit_id, checkpoint_commit_id);
        assert!(
            select_rows(
                &session,
                "SELECT diff_id FROM lix_working_diff WHERE schema_key = 'lix_key_value'",
            )
            .await
            .is_empty(),
            "two consecutive partial checkpoints must advance the working-diff baseline"
        );
        let checkpoint_history = select_rows(
            &session,
            "SELECT commit_id, lixcol_depth FROM lix_checkpoint ORDER BY lixcol_depth",
        )
        .await;
        assert_eq!(
            checkpoint_history.first().map(|row| &row[0]),
            Some(&Value::Text(second_checkpoint_commit_id.clone())),
            "the second partial checkpoint must be the branch-bound latest checkpoint"
        );
        assert!(
            checkpoint_history
                .iter()
                .any(|row| { row.first() == Some(&Value::Text(checkpoint_commit_id.clone())) })
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

simulation_test!(
    historical_diff_exposes_distinct_change_ids_for_modified_row,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute(
                r#"INSERT INTO lix_registered_schema (value, lixcol_global)
                   VALUES (
                     lix_json('{"x-lix-key":"change_id_regression","x-lix-primary-key":["/id"],"type":"object","required":["id","value"],"properties":{"id":{"type":"string"},"value":{"type":"string"}},"additionalProperties":false}'),
                     false
                   )"#,
                &[],
            )
            .await
            .expect("regression schema should register");

        session
            .execute(
                "INSERT INTO change_id_regression (id, value) VALUES \
                 ('change-id-regression', 'before'), \
                 ('second-row', 'before'), \
                 ('stable-row', 'stable')",
                &[],
            )
            .await
            .expect("initial row should commit");
        let before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("pre-update head should load")
            .expect("pre-update head should exist");
        session
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000099".to_string()),
                name: "ChangeId regression branch".to_string(),
                from_commit_id: Some(before.to_string()),
            })
            .await
            .expect("branch snapshot should succeed");

        let mut update_transaction = session
            .begin_transaction()
            .await
            .expect("update transaction should begin");
        update_transaction
            .execute(
                "INSERT INTO change_id_regression (id, value) VALUES \
                 ('change-id-regression', 'after'), \
                 ('second-row', 'after') \
                 ON CONFLICT (id) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("updated rows should stage");
        update_transaction
            .commit()
            .await
            .expect("updated rows should commit");
        let after = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("post-update head should load")
            .expect("post-update head should exist");

        let rows = select_rows(
            &session,
            &format!(
                "SELECT diff_type, before_change_id, after_change_id \
                 FROM lix_diff('{before}', '{after}') \
                 WHERE schema_key = 'change_id_regression' \
                   AND entity_pk = lix_json('[\"change-id-regression\"]')"
            ),
        )
        .await;
        assert_eq!(rows.len(), 1, "one modified row should be visible");
        assert_eq!(rows[0][0], Value::Text("modified".to_string()));
        let (before_change_id, after_change_id) = match (&rows[0][1], &rows[0][2]) {
            (Value::Text(before), Value::Text(after)) => (before, after),
            values => panic!("modified diff must expose two change IDs, got {values:?}"),
        };
        assert!(!before_change_id.is_empty());
        assert!(!after_change_id.is_empty());
        assert_ne!(
            before_change_id, after_change_id,
            "an update must publish a new authenticated ChangeId"
        );
    }
);

simulation_test!(
    historical_diff_ignores_page_provenance_when_change_identity_is_unchanged,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (lix_json($1), false)",
            &[Value::Text(
                json!({
                    "x-lix-key": "change_id_branch_regression",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "required": ["id", "value"],
                    "properties": {
                        "id": {"type": "string"},
                        "value": {"type": "string"}
                    },
                    "additionalProperties": false
                })
                .to_string(),
            )],
        )
        .await
        .expect("register regression schema");
        main.execute(
            "INSERT INTO change_id_branch_regression (id, value) VALUES \
             ('row-0', 'base-0'), ('row-1', 'base-1'), ('row-2', 'base-2')",
            &[],
        )
        .await
        .expect("seed branch regression rows");
        let base = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("base head should load")
            .expect("base head should exist")
            .to_string();
        let branch = main
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000199".to_owned()),
                name: "ChangeId branch regression".to_owned(),
                from_commit_id: Some(base.clone()),
            })
            .await
            .expect("create source branch");
        let source = sim.wrap_session(
            engine
                .open_session(branch.id.clone())
                .await
                .expect("source session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO change_id_branch_regression (id, value) VALUES \
             ('row-0', 'target-0'), ('row-1', 'target-1') \
             ON CONFLICT (id) DO UPDATE SET value = excluded.value",
            &[],
        )
        .await
        .expect("target update should commit");
        source
            .execute(
                "INSERT INTO change_id_branch_regression (id, value) VALUES ('row-2', 'source-2') \
                 ON CONFLICT (id) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("source update should commit");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: branch.id.clone(),
            })
            .await
            .expect("merge preview should succeed");
        main.merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .expect("merge should succeed");
        let merged_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("post-merge head should load")
            .expect("post-merge head should exist")
            .to_string();

        let rows = select_rows(
            &main,
            &format!(
                "SELECT entity_pk, diff_type, before_change_id, after_change_id \
                 FROM lix_diff('{}', '{merged_head}') \
                 WHERE schema_key = 'change_id_branch_regression' ORDER BY entity_pk",
                preview.target_head_commit_id
            ),
        )
        .await;
        assert_eq!(
            rows.len(),
            1,
            "only the source-edited row belongs to the post-merge target diff"
        );
        assert_eq!(rows[0][0], Value::Json(json!(["row-2"])));
        assert_eq!(rows[0][1], Value::Text("modified".to_owned()));
        let (Value::Text(before), Value::Text(after)) = (&rows[0][2], &rows[0][3]) else {
            panic!("modified rows must expose two ChangeIds: {:?}", rows[0]);
        };
        assert_ne!(before, after, "modified rows must have distinct ChangeIds");
    }
);

simulation_test!(
    diff_commands_reject_staged_state_that_moved_the_selected_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let initial_head = sim.initial_commit_id().to_string();

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('apply-guard', 'published')",
                &[],
            )
            .await
            .expect("apply source should publish");
        let apply_source_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("apply source head should load")
            .expect("apply source head should exist")
            .to_string();
        session
            .execute("DELETE FROM lix_key_value WHERE key = 'apply-guard'", &[])
            .await
            .expect("apply target should return to absence");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('revert-guard', 'before')",
                &[],
            )
            .await
            .expect("revert predecessor should publish");
        let revert_before_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("revert predecessor head should load")
            .expect("revert predecessor head should exist")
            .to_string();
        session
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'revert-guard'",
                &[],
            )
            .await
            .expect("revert endpoint should publish");
        let revert_after_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("revert endpoint head should load")
            .expect("revert endpoint head should exist")
            .to_string();

        let mut apply_transaction = session
            .begin_transaction()
            .await
            .expect("apply transaction should begin");
        apply_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('apply-guard', 'staged')",
                &[],
            )
            .await
            .expect("apply transaction should stage its current value");
        let apply_error = apply_transaction
            .execute(
                "INSERT INTO lix_apply (diff_id) \
                 SELECT diff_id FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"apply-guard\"]')",
                &[Value::Text(initial_head), Value::Text(apply_source_head)],
            )
            .await
            .expect_err("apply must validate against the staged current value");
        assert_eq!(apply_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        let apply_visible = apply_transaction
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'apply-guard'",
                &[],
            )
            .await
            .expect("failed apply must retain the staged value");
        assert_eq!(
            apply_visible.rows()[0].get::<Value>("value").unwrap(),
            Value::Json(json!("staged"))
        );
        apply_transaction
            .rollback()
            .await
            .expect("apply transaction should roll back");

        let mut revert_transaction = session
            .begin_transaction()
            .await
            .expect("revert transaction should begin");
        revert_transaction
            .execute(
                "UPDATE lix_key_value SET value = 'staged' WHERE key = 'revert-guard'",
                &[],
            )
            .await
            .expect("revert transaction should stage its current value");
        let revert_error = revert_transaction
            .execute(
                "INSERT INTO lix_revert (diff_id) \
                 SELECT diff_id FROM lix_diff($1, $2) \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"revert-guard\"]')",
                &[
                    Value::Text(revert_before_head),
                    Value::Text(revert_after_head),
                ],
            )
            .await
            .expect_err("revert must validate against the staged current value");
        assert_eq!(revert_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        let revert_visible = revert_transaction
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'revert-guard'",
                &[],
            )
            .await
            .expect("failed revert must retain the staged value");
        assert_eq!(
            revert_visible.rows()[0].get::<Value>("value").unwrap(),
            Value::Json(json!("staged"))
        );
        revert_transaction
            .rollback()
            .await
            .expect("revert transaction should roll back");
    }
);
