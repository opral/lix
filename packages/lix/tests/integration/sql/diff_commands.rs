use lix::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    diff_commands_apply_revert_and_checkpoint_relation_selections,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = sim.initial_commit_id().to_string();

        for (key, value) in [("a", "one"), ("b", "two")] {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[Value::Text(key.to_string()), Value::Text(value.to_string())],
                )
                .await
                .expect("tracked insert should succeed");
        }
        let original_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist")
            .to_string();

        let working = select_rows(
            &session,
            &format!(
                "SELECT lixcol_row_pk, lixcol_diff_type FROM lix_diff('lix_key_value', '{baseline}', '{original_head}') ORDER BY lixcol_row_pk"
            ),
        )
        .await;
        assert_eq!(working.len(), 2);
        assert_eq!(working[0][1], Value::Text("added".to_string()));

        let legacy = session
            .execute("INSERT INTO lix_revert (diff_id) VALUES ('legacy')", &[])
            .await
            .expect_err("the retired diff_id command currency must not bind");
        assert_eq!(legacy.code, "LIX_COLUMN_NOT_FOUND");

        let reverted = session
            .execute(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff(\
                   'lix_key_value', lix_root_commit_id(), lix_active_branch_commit_id()\
                 ) \
                 WHERE lixcol_row_pk ->> 0 IN ('a', 'b') \
                 RETURNING commit_id",
                &[],
            )
            .await
            .expect("relation-row revert should accept root/head scalar source commits");
        assert_eq!(reverted.rows_affected(), 2);
        assert_eq!(reverted.columns(), &["commit_id"]);
        assert_eq!(reverted.rows().len(), 1);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key",
            )
            .await,
            Vec::<Vec<Value>>::new(),
        );

        let applied = session
            .execute(
                "INSERT INTO lix_apply (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff('lix_key_value', lix_root_commit_id(), $1) \
                 WHERE lixcol_row_pk ->> 0 IN ('a', 'b') \
                 RETURNING commit_id",
                &[Value::Text(original_head)],
            )
            .await
            .expect("historical relation-row apply should resolve the root scalar source commit");
        assert_eq!(applied.rows_affected(), 2);
        assert_eq!(applied.columns(), &["commit_id"]);
        assert_eq!(applied.rows().len(), 1);

        let checkpointed = session
            .execute(
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff(\
                   'lix_key_value', lix_latest_checkpoint_commit_id(), lix_active_branch_commit_id()\
                 ) \
                 WHERE lixcol_row_pk = CAST('[\"a\"]' AS JSONB) \
                 RETURNING commit_id",
                &[],
            )
            .await
            .expect("partial relation-row checkpoint should succeed");
        assert_eq!(checkpointed.rows_affected(), 1);
        let checkpoint_commit_id = match checkpointed.get(&checkpointed.rows()[0], "commit_id") {
            Some(Value::Text(commit_id)) => commit_id.clone(),
            value => panic!("checkpoint RETURNING should contain a commit ID, got {value:?}"),
        };
        let child_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("partial checkpoint child head should load")
            .expect("partial checkpoint child head should exist")
            .to_string();
        assert_ne!(child_head, checkpoint_commit_id);
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT lixcol_row_pk FROM lix_diff('lix_key_value', '{checkpoint_commit_id}', '{child_head}')"
                ),
            )
            .await,
            vec![vec![Value::Jsonb(json!(["b"]).into())]]
        );

        let empty = session
            .execute(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff('lix_key_value', $1, $2) WHERE 1 = 0 \
                 RETURNING commit_id",
                &[
                    Value::Text(checkpoint_commit_id.clone()),
                    Value::Text(child_head.clone()),
                ],
            )
            .await
            .expect("empty relation-row selection should be a successful no-op");
        assert_eq!(empty.rows_affected(), 0);
        assert!(empty.is_empty());
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("head after empty selection should load")
                .expect("head after empty selection should exist")
                .to_string(),
            child_head
        );

        let duplicate = session
            .execute(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT relation, CAST(row_pk AS JSONB) FROM \
                 (VALUES ('lix_key_value', '[\"b\"]'), ('lix_key_value', '[\"b\"]')) \
                 AS selected(relation, row_pk)",
                &[],
            )
            .await
            .expect_err("duplicate public relation-row selections must fail atomically");
        assert_eq!(duplicate.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    diff_commands_resolve_the_active_branch_latest_checkpoint,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-baseline', 'saved')",
                &[],
            )
            .await
            .expect("baseline value should commit");
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-working', 'draft')",
                &[],
            )
            .await
            .expect("working value should commit");
        let working_head = session
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("working head should read")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("working head should be text");

        let reverted = session
            .execute(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff(\
                   'lix_key_value', \
                   lix_latest_checkpoint_commit_id(), \
                   lix_active_branch_commit_id()\
                 ) \
                 RETURNING commit_id",
                &[],
            )
            .await
            .expect("revert should resolve the branch's checkpoint/head source commits");
        assert_eq!(reverted.rows_affected(), 1);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key FROM lix_key_value WHERE key LIKE 'checkpoint-%' ORDER BY key",
            )
            .await,
            vec![vec![Value::Text("checkpoint-baseline".to_string())]],
            "revert must preserve checkpointed values while removing working changes"
        );

        let applied = session
            .execute(
                "INSERT INTO lix_apply (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff(\
                   'lix_key_value', \
                   lix_latest_checkpoint_commit_id(), \
                   $1\
                 ) \
                 RETURNING commit_id",
                &[Value::Text(working_head)],
            )
            .await
            .expect("apply should resolve the active branch checkpoint source");
        assert_eq!(applied.rows_affected(), 1);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key FROM lix_key_value WHERE key LIKE 'checkpoint-%' ORDER BY key",
            )
            .await,
            vec![
                vec![Value::Text("checkpoint-baseline".to_string())],
                vec![Value::Text("checkpoint-working".to_string())],
            ],
            "apply must resolve the actual checkpoint baseline and restore the historical row"
        );
    }
);

simulation_test!(
    full_checkpoint_default_values_and_tombstoned_relation_revert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('recycled', 'one')",
                &[],
            )
            .await
            .expect("first insert should succeed");
        let first = session
            .execute(
                "INSERT INTO lix_create_checkpoint DEFAULT VALUES RETURNING commit_id",
                &[],
            )
            .await
            .expect("full metadata-only SQL checkpoint should succeed");
        assert_eq!(first.rows_affected(), 1);
        assert_eq!(first.columns(), &["commit_id"]);

        session
            .execute("DELETE FROM lix_key_value WHERE key = 'recycled'", &[])
            .await
            .expect("delete should succeed");
        let deleted_checkpoint = session
            .execute(
                "INSERT INTO lix_create_checkpoint DEFAULT VALUES RETURNING commit_id",
                &[],
            )
            .await
            .expect("checkpoint of the delete should succeed");
        let checkpoint_id = match deleted_checkpoint.get(&deleted_checkpoint.rows()[0], "commit_id")
        {
            Some(Value::Text(commit_id)) => commit_id.clone(),
            value => panic!("checkpoint must return commit id, got {value:?}"),
        };
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('recycled', 'two')",
                &[],
            )
            .await
            .expect("re-insert should succeed");
        let head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist")
            .to_string();
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT lixcol_diff_type FROM lix_diff('lix_key_value', '{checkpoint_id}', '{head}') \
                     WHERE lixcol_row_pk = CAST('[\"recycled\"]' AS JSONB)"
                ),
            )
            .await,
            vec![vec![Value::Text("added".to_string())]]
        );

        let reverted = session
            .execute(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT 'lix_key_value', CAST('[\"recycled\"]' AS JSONB) \
                 RETURNING commit_id",
                &[],
            )
            .await
            .expect("relation identity should revert a tombstone-backed add");
        assert_eq!(reverted.rows_affected(), 1);
        assert!(
            select_rows(
                &session,
                "SELECT key FROM lix_key_value WHERE key = 'recycled'",
            )
            .await
            .is_empty()
        );
    }
);

simulation_test!(
    checkpoint_returning_reports_one_command_result_for_multiple_selections,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = sim.initial_commit_id().to_string();

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES \
                 ('a', 'one'), ('b', 'two'), ('c', 'three')",
                &[],
            )
            .await
            .expect("tracked inserts should succeed");
        let head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist")
            .to_string();

        let checkpointed = session
            .execute(
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk \
                 FROM lix_diff('lix_key_value', $1, $2) \
                 WHERE lixcol_row_pk ->> 0 IN ('a', 'b', 'c') \
                 RETURNING commit_id",
                &[Value::Text(baseline), Value::Text(head)],
            )
            .await
            .expect("multi-selection checkpoint should succeed");

        assert_eq!(checkpointed.rows_affected(), 3);
        assert_eq!(checkpointed.columns(), &["commit_id"]);
        assert_eq!(
            checkpointed.rows().len(),
            1,
            "RETURNING describes the one command result, not its three inputs",
        );
        checkpointed.rows()[0]
            .get::<String>("commit_id")
            .expect("checkpoint command result should contain its commit ID");
    }
);

simulation_test!(
    partial_file_checkpoint_closes_changed_parent_directories,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = sim.initial_commit_id().to_string();
        session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/docs/nested/readme.txt', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("nested file insert should create its parent directories");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('unselected', 'keep-working')",
                &[],
            )
            .await
            .expect("unselected row should remain in the working interval");
        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/unrelated')",
                &[],
            )
            .await
            .expect("unrelated dirty directory should stay outside the file closure");
        let head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist")
            .to_string();
        let checkpoint = session
            .execute(
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_file', lixcol_row_pk FROM lix_diff('lix_file', $1, $2) \
                 WHERE to_path = '/docs/nested/readme.txt' RETURNING commit_id",
                &[Value::Text(baseline), Value::Text(head)],
            )
            .await
            .expect("file selection must include its changed parent-directory descriptors");
        assert_eq!(checkpoint.rows_affected(), 1);
        assert_eq!(
            select_rows(
                &session,
                "SELECT path FROM lix_file WHERE path = '/docs/nested/readme.txt'",
            )
            .await,
            vec![vec![Value::Text("/docs/nested/readme.txt".to_string())]]
        );
        let checkpoint_id = checkpoint.rows()[0]
            .get::<String>("commit_id")
            .expect("checkpoint id should decode");
        let head_after_checkpoint = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head after checkpoint should load")
            .expect("head after checkpoint should exist")
            .to_string();
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT to_path FROM lix_diff('lix_directory', '{checkpoint_id}', '{head_after_checkpoint}') \
                     WHERE to_path = '/unrelated'"
                ),
            )
            .await,
            vec![vec![Value::Text("/unrelated".to_string())]],
            "an unrelated changed directory must remain outside a selected file checkpoint",
        );
    }
);
