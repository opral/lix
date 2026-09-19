use lix::{LixError, Value};

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
                "SELECT key, diff_type FROM lix_diff('lix_key_value', '{baseline}', '{original_head}') ORDER BY key"
            ),
        )
        .await;
        assert_eq!(working.len(), 2);
        assert_eq!(working[0][1], Value::Text("added".to_string()));

        let _legacy = session
            .execute("SELECT lix_revert($1)", &[Value::Text(original_head.clone())])
            .await
            .expect_err("the scalar command shape must not bind");

        let reverted = session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') \
                         WHERE key IN ('a', 'b')))",
                &[],
            )
            .await
            .expect("relation-row restore should use the actual working baseline");
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
                "SELECT commit_id FROM lix_apply(\
                   lix_root_commit_id(), $1, \
                   ARRAY(SELECT row_ref \
                         FROM lix_diff('lix_key_value', lix_root_commit_id(), $1) \
                         WHERE key IN ('a', 'b')))",
                &[Value::Text(original_head)],
            )
            .await
            .expect("historical relation-row apply should resolve the root scalar source commit");
        assert_eq!(applied.columns(), &["commit_id"]);
        assert_eq!(applied.rows().len(), 1);

        let checkpointed = session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
                 SELECT row_ref \
                 FROM lix_diff('lix_key_value') \
                 WHERE key = 'a'))",
                &[],
            )
            .await
            .expect("partial relation-row checkpoint should succeed");
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
                    "SELECT key FROM lix_diff('lix_key_value', '{checkpoint_commit_id}', '{child_head}')"
                ),
            )
            .await,
            vec![vec![Value::Text("b".to_string())]]
        );

        let empty = session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE 1 = 0))",
                &[],
            )
            .await
            .expect("empty relation-row selection should be a successful no-op");
        assert_eq!(empty.columns(), &["commit_id"]);
        assert_eq!(empty.rows().len(), 1);
        assert!(matches!(empty.rows()[0].values(), [Value::Null]));
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
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT lix_row_ref('lix_key_value', 'b') \
                         UNION ALL \
                         SELECT lix_row_ref('lix_key_value', 'b')))",
                &[],
            )
            .await
            .expect_err("duplicate public relation-row selections must fail atomically");
        assert_eq!(duplicate.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    diff_commands_resolve_the_actual_working_baseline,
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
        let baseline = session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should commit")
            .commit_id;
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
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))",
                &[],
            )
            .await
            .expect("restore should resolve the branch's actual working baseline");
        assert_eq!(reverted.columns(), &["commit_id"]);
        assert_eq!(reverted.rows().len(), 1);
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
                "SELECT commit_id FROM lix_apply(\
                   $2, $1, \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', $2, $1)))",
                &[Value::Text(working_head), Value::Text(baseline)],
            )
            .await
            .expect("apply should resolve the active branch checkpoint source");
        assert_eq!(applied.columns(), &["commit_id"]);
        assert_eq!(applied.rows().len(), 1);
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
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("full metadata-only SQL checkpoint should succeed");
        assert_eq!(first.columns(), &["commit_id"]);

        session
            .execute("DELETE FROM lix_key_value WHERE key = 'recycled'", &[])
            .await
            .expect("delete should succeed");
        let deleted_checkpoint = session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
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
                    "SELECT diff_type FROM lix_diff('lix_key_value', '{checkpoint_id}', '{head}') \
                     WHERE key = 'recycled'"
                ),
            )
            .await,
            vec![vec![Value::Text("added".to_string())]]
        );

        let reverted = session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT lix_row_ref('lix_key_value', 'recycled')))",
                &[],
            )
            .await
            .expect("relation identity should restore a tombstone-backed row");
        assert_eq!(reverted.columns(), &["commit_id"]);
        assert_eq!(reverted.rows().len(), 1);
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
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
                 SELECT row_ref \
                 FROM lix_diff('lix_key_value', $1, $2) \
                 WHERE key IN ('a', 'b', 'c')))",
                &[Value::Text(baseline), Value::Text(head)],
            )
            .await
            .expect("multi-selection checkpoint should succeed");

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
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
                 SELECT row_ref FROM lix_diff('lix_file', $1, $2) \
                 WHERE to_path = '/docs/nested/readme.txt'))",
                &[Value::Text(baseline), Value::Text(head)],
            )
            .await
            .expect("file selection must include its changed parent-directory descriptors");
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

simulation_test!(
    scoped_file_revert_restores_removed_parent_directory,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let directory_id = "01950000-0000-7000-8000-000000000001";
        let file_id = "01950000-0000-7000-8000-000000000002";
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ($1, '/docs')",
                &[Value::Text(directory_id.to_owned())],
            )
            .await
            .expect("parent directory should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ($1, '/docs/a.md', CAST('hello' AS BYTEA))",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("child file should insert");
        session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("baseline checkpoint should succeed");

        session
            .execute(
                "DELETE FROM lix_directory WHERE id = $1",
                &[Value::Text(directory_id.to_owned())],
            )
            .await
            .expect("recursive directory delete should succeed");
        session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE id = $1))",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("file restore should close over its removed parent directory");

        assert_eq!(
            select_rows(
                &session,
                "SELECT path FROM lix_directory WHERE id = '01950000-0000-7000-8000-000000000001'",
            )
            .await,
            vec![vec![Value::Text("/docs".to_owned())]],
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path FROM lix_file WHERE id = '01950000-0000-7000-8000-000000000002'",
            )
            .await,
            vec![vec![Value::Text("/docs/a.md".to_owned())]],
        );
    }
);

simulation_test!(
    scoped_file_apply_creates_changed_parent_directory,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("baseline head should load")
            .expect("baseline head should exist")
            .to_string();
        let directory_id = "01950000-0000-7000-8000-000000000011";
        let file_id = "01950000-0000-7000-8000-000000000012";
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ($1, '/apply-docs')",
                &[Value::Text(directory_id.to_owned())],
            )
            .await
            .expect("parent directory should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ($1, '/apply-docs/a.md', CAST('hello' AS BYTEA))",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("child file should insert");
        let target = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("target head should load")
            .expect("target head should exist")
            .to_string();

        session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE id = $1))",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("precondition restore should remove the file");
        session
            .execute(
                "SELECT commit_id FROM lix_restore(\
                   (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_directory') WHERE id = $1))",
                &[Value::Text(directory_id.to_owned())],
            )
            .await
            .expect("precondition restore should remove the parent directory");
        assert!(
            select_rows(
                &session,
                "SELECT id FROM lix_directory WHERE id = '01950000-0000-7000-8000-000000000011'",
            )
            .await
            .is_empty(),
            "the apply dependency must actually be absent before the command",
        );
        session
            .execute(
                "SELECT commit_id FROM lix_apply(\
                   $1, $2, \
                   ARRAY(SELECT row_ref FROM lix_diff('lix_file', $1, $2) WHERE id = $3))",
                &[
                    Value::Text(baseline),
                    Value::Text(target),
                    Value::Text(file_id.to_owned()),
                ],
            )
            .await
            .expect("file apply should close over its changed parent directory");

        assert_eq!(
            select_rows(
                &session,
                "SELECT path FROM lix_directory WHERE id = '01950000-0000-7000-8000-000000000011'",
            )
            .await,
            vec![vec![Value::Text("/apply-docs".to_owned())]],
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path FROM lix_file WHERE id = '01950000-0000-7000-8000-000000000012'",
            )
            .await,
            vec![vec![Value::Text("/apply-docs/a.md".to_owned())]],
        );
    }
);


simulation_test!(
    diff_commands_apply_resolves_scalar_subquery_endpoints_once,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('subquery-apply', 'original')",
            &[],
        ).await.expect("insert should succeed");
        let source_head = engine.load_branch_head_commit_id(sim.main_branch_id())
            .await.expect("head loads").expect("head exists").to_string();
        session.execute(
            "SELECT commit_id FROM lix_restore(\
               (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()), \
               ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') \
                     WHERE key = 'subquery-apply'))",
            &[],
        ).await.expect("restore removes the selected value");

        let applied = session.execute(
            "SELECT commit_id FROM lix_apply(\
               lix_root_commit_id(), $1, \
               ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', \
                 lix_root_commit_id(), $1) WHERE key = 'subquery-apply'))",
            &[Value::Text(source_head.clone())],
        ).await.expect("apply uses explicit endpoints for the selected rows");
        assert_eq!(applied.columns(), &["commit_id"]);
        assert_eq!(applied.rows().len(), 1);
        assert_eq!(select_rows(&session,
            "SELECT value FROM lix_key_value WHERE key = 'subquery-apply'").await,
            vec![vec![Value::Jsonb(serde_json::json!("original").into())]]);

        let empty = session.execute(
            "SELECT commit_id FROM lix_apply(\
               lix_root_commit_id(), $1, \
               ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', \
                 lix_root_commit_id(), $1) WHERE key = 'absent-subquery-apply'))",
            &[Value::Text(source_head)],
        ).await.expect("empty selection with resolved endpoints remains a no-op");
        assert_eq!(empty.columns(), &["commit_id"]);
        assert_eq!(empty.rows().len(), 1);
        assert!(matches!(empty.rows()[0].values(), [Value::Null]));
    }
);

simulation_test!(
    inverse_apply_undoes_a_transaction_and_preserves_later_unrelated_changes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('modified', 'before'), ('removed', 'restore')",
            &[],
        ).await.unwrap();
        let before = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let mut tx = session.begin_transaction().await.unwrap();
        tx.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('added', 'remove')",
            &[],
        )
        .await
        .unwrap();
        tx.execute(
            "UPDATE lix_key_value SET value = 'after' WHERE key = 'modified'",
            &[],
        )
        .await
        .unwrap();
        tx.execute("DELETE FROM lix_key_value WHERE key = 'removed'", &[])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let after = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('later', 'keep')",
                &[],
            )
            .await
            .unwrap();
        let head_before_undo = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();

        let undo = session.execute(
            "SELECT commit_id FROM lix_apply(\
               $1, $2, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', $1, $2) \
                             WHERE key IN ('added', 'modified', 'removed'))) ",
            &[Value::Text(after.clone()), Value::Text(before.clone())],
        ).await.expect("a reversed commit pair should undo the selected transaction");
        assert_eq!(undo.columns(), &["commit_id"]);
        assert_eq!(undo.rows().len(), 1);
        let undo_commit = undo.rows()[0].get::<String>("commit_id").unwrap();
        assert_ne!(undo_commit, before);
        assert_ne!(undo_commit, after);
        assert_ne!(undo_commit, head_before_undo);
        assert_eq!(select_rows(&session,
            "SELECT key, value FROM lix_key_value WHERE key IN ('added', 'modified', 'removed', 'later') ORDER BY key"
        ).await, vec![
            vec![Value::Text("later".into()), Value::Jsonb(serde_json::json!("keep").into())],
            vec![Value::Text("modified".into()), Value::Jsonb(serde_json::json!("before").into())],
            vec![Value::Text("removed".into()), Value::Jsonb(serde_json::json!("restore").into())],
        ]);
        let original = session.execute(
            "SELECT key, diff_type FROM lix_diff('lix_key_value', $1, $2) WHERE key IN ('added', 'modified', 'removed') ORDER BY key",
            &[Value::Text(before), Value::Text(after)],
        ).await.unwrap();
        assert_eq!(
            original.rows().len(),
            3,
            "undo must preserve the original history"
        );
    }
);

simulation_test!(
    inverse_apply_rejects_stale_rows_without_partially_undoing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('a', 'before'), ('b', 'before')",
                &[],
            )
            .await
            .unwrap();
        let edit = session
            .execute(
                "UPDATE lix_key_value SET value = 'agent' WHERE key IN ('a', 'b')",
                &[],
            )
            .await
            .unwrap();
        let span = edit.commit().expect("write has a commit span");
        let before = span.before().to_owned();
        let after = span.after().to_owned();
        session
            .execute(
                "UPDATE lix_key_value SET value = 'human' WHERE key = 'b'",
                &[],
            )
            .await
            .unwrap();
        let head = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let error = session.execute(
            "SELECT commit_id FROM lix_apply(\
               $1, $2, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', $1, $2) \
                             WHERE key IN ('a', 'b'))) ",
            &[Value::Text(after), Value::Text(before)],
        ).await.expect_err("a later version of one selected row must reject the entire undo");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key, value FROM lix_key_value WHERE key IN ('a', 'b') ORDER BY key"
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("agent").into())
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("human").into())
                ],
            ]
        );
        assert_eq!(
            session
                .execute("SELECT lix_active_branch_commit_id() AS id", &[])
                .await
                .unwrap()
                .rows()[0]
                .get::<String>("id")
                .unwrap(),
            head,
            "rejected undo must not publish a commit"
        );
    }
);

simulation_test!(
    inverse_apply_rejects_a_later_edit_to_a_different_column,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "undo_clip",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "position", "type": "int8", "nullable": false},
                {"name": "title", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO undo_clip (id, position, title) VALUES ('clip', 1, 'original')",
                &[],
            )
            .await
            .unwrap();
        let edit = session
            .execute("UPDATE undo_clip SET position = 2 WHERE id = 'clip'", &[])
            .await
            .unwrap();
        let span = edit.commit().unwrap();
        session
            .execute(
                "UPDATE undo_clip SET title = 'human title' WHERE id = 'clip'",
                &[],
            )
            .await
            .unwrap();
        let error = session
            .execute(
                "SELECT commit_id FROM lix_apply(\
                   $1, $2, ARRAY(SELECT row_ref FROM lix_diff('undo_clip', $1, $2)))",
                &[
                    Value::Text(span.after().to_owned()),
                    Value::Text(span.before().to_owned()),
                ],
            )
            .await
            .expect_err("inverse apply checks whole-row versions, not individual columns");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(
            select_rows(
                &session,
                "SELECT position, title FROM undo_clip WHERE id = 'clip'"
            )
            .await,
            vec![vec![Value::Integer(2), Value::Text("human title".into())]]
        );
    }
);
