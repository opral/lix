use lix::{CreateBranchOptions, LixError, Value};
use serde_json::json;

use crate::support::simulation_test::engine::SimSession;

const RESTORE_SQL: &str = "SELECT commit_id FROM lix_restore($1)";

simulation_test!(
    restore_copies_an_ancestor_state_into_a_new_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = working_baseline(&session).await;

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/a.txt', CAST('a' AS BYTEA))",
                &[],
            )
            .await
            .expect("first file should commit");
        let source = head(&session).await;
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/b.txt', CAST('b' AS BYTEA))",
                &[],
            )
            .await
            .expect("second file should commit");
        let head_before_restore = head(&session).await;
        let commits_before_restore = count(&session, "lix_commit").await;

        let restored = restore(&session, &source)
            .await
            .expect("ancestor state should restore")
            .expect("a changed restore must return a commit ID");
        assert_ne!(restored, source, "restore publishes a new content commit");
        assert_ne!(restored, head_before_restore);
        assert_eq!(head(&session).await, restored);
        assert_eq!(
            count(&session, "lix_commit").await,
            commits_before_restore + 1
        );
        assert_eq!(working_baseline(&session).await, baseline);
        assert_eq!(file_count(&session, "/a.txt").await, 1);
        assert_eq!(file_count(&session, "/b.txt").await, 0);
    }
);

simulation_test!(
    restore_is_undoable_and_redoable_without_moving_working_baseline,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = working_baseline(&session).await;

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/a.txt', CAST('a' AS BYTEA))",
                &[],
            )
            .await
            .expect("first file should commit");
        let source = head(&session).await;
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/b.txt', CAST('b' AS BYTEA))",
                &[],
            )
            .await
            .expect("second file should commit");

        let restored = restore(&session, &source)
            .await
            .expect("restore should succeed")
            .expect("changed restore should return a commit ID");
        assert_eq!(file_count(&session, "/a.txt").await, 1);
        assert_eq!(file_count(&session, "/b.txt").await, 0);
        assert_eq!(working_baseline(&session).await, baseline);

        let undone = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("restore should be undoable");
        let undone_commit = undone.rows()[0]
            .get::<String>("commit_id")
            .expect("undo commit id");
        assert_ne!(undone_commit, restored);
        assert_eq!(head(&session).await, undone_commit);
        assert_eq!(file_count(&session, "/a.txt").await, 1);
        assert_eq!(file_count(&session, "/b.txt").await, 1);
        assert_eq!(working_baseline(&session).await, baseline);

        let redone = session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("restore should be redoable");
        let redone_commit = redone.rows()[0]
            .get::<String>("commit_id")
            .expect("redo commit id");
        assert_ne!(redone_commit, undone_commit);
        assert_eq!(head(&session).await, redone_commit);
        assert_eq!(file_count(&session, "/a.txt").await, 1);
        assert_eq!(file_count(&session, "/b.txt").await, 0);
        assert_eq!(working_baseline(&session).await, baseline);
    }
);

simulation_test!(
    restore_accepts_a_source_commit_from_a_sibling_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        let baseline = working_baseline(&main).await;
        let sibling_id = "01930000-0000-7000-8000-000000000051";
        main.create_branch(CreateBranchOptions {
            id: Some(sibling_id.to_string()),
            name: "Restore sibling".to_string(),
            from_commit_id: Some(sim.initial_commit_id().to_string()),
        })
        .await
        .expect("sibling branch should be created");
        let sibling = main.wrap_session(
            engine
                .open_session_at(sibling_id)
                .await
                .expect("sibling session should open"),
            &engine,
        );
        sibling
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('shared', 'sibling')",
                &[],
            )
            .await
            .expect("sibling source write should commit");
        let source = head(&sibling).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('shared', 'main')",
            &[],
        )
        .await
        .expect("main target write should commit");

        let restored = restore(&main, &source)
            .await
            .expect("sibling source should be accepted")
            .expect("changed restore should return a commit ID");
        assert_eq!(head(&main).await, restored);
        assert_eq!(value(&main, "shared").await, Some(json!("sibling")));
        assert_eq!(working_baseline(&main).await, baseline);
    }
);

simulation_test!(
    restore_noop_returns_a_null_receipt_and_missing_source_fails,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('main', 'one')",
                &[],
            )
            .await
            .expect("main change should commit");
        let current = head(&session).await;
        let commits_before_restore = count(&session, "lix_commit").await;

        assert_eq!(
            restore(&session, &current)
                .await
                .expect("restoring the current state should succeed"),
            None,
            "an unchanged restore has a NULL receipt commit_id"
        );
        assert_eq!(head(&session).await, current);
        assert_eq!(count(&session, "lix_commit").await, commits_before_restore);

        let missing = "01990000-0000-7000-8000-00000000dead";
        let error = restore(&session, missing)
            .await
            .expect_err("a missing source commit should fail");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
        assert_eq!(head(&session).await, current);
    }
);

simulation_test!(
    restore_selected_rows_preserves_unselected_later_work,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let baseline = working_baseline(&session).await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('selected', 'source'), ('unselected', 'source')",
                &[],
            )
            .await
            .expect("source rows should commit");
        let source = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key IN ('selected', 'unselected')",
                &[],
            )
            .await
            .expect("later rows should commit");

        let result = session
            .execute(
                "SELECT commit_id FROM lix_restore($1, ARRAY[lix_row_ref('lix_key_value', NULL, 'selected')])",
                &[Value::Text(source)],
            )
            .await
            .expect("selected restore should succeed");
        assert_eq!(result.columns(), &["commit_id"]);
        assert_eq!(result.rows().len(), 1);
        let restored = result.rows()[0]
            .get::<String>("commit_id")
            .expect("selected restore should publish a commit");
        assert_eq!(head(&session).await, restored);
        assert_eq!(working_baseline(&session).await, baseline);
        assert_eq!(value(&session, "selected").await, Some(json!("source")));
        assert_eq!(value(&session, "unselected").await, Some(json!("later")));
    }
);

simulation_test!(
    restore_preserves_branch_local_untracked_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('tracked', 'target')",
                &[],
            )
            .await
            .expect("target state should commit");
        let target = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'tracked'",
                &[],
            )
            .await
            .expect("later state should commit");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('local', 'kept', true)",
                &[],
            )
            .await
            .expect("branch-local untracked state should insert");

        let restored = restore(&session, &target)
            .await
            .expect("restore should retain untracked state")
            .expect("changed tracked state should publish a commit");
        assert_eq!(head(&session).await, restored);
        let values = session
            .execute(
                "SELECT key, value, lixcol_untracked FROM lix_key_value WHERE key IN ('local', 'tracked') ORDER BY key",
                &[],
            )
            .await
            .expect("restored state should read");
        assert_eq!(values.rows().len(), 2);
        assert_eq!(values.rows()[0].get::<String>("key").unwrap(), "local");
        assert_eq!(
            values.rows()[0].get::<serde_json::Value>("value").unwrap(),
            json!("kept")
        );
        assert!(values.rows()[0].get::<bool>("lixcol_untracked").unwrap());
        assert_eq!(values.rows()[1].get::<String>("key").unwrap(), "tracked");
        assert_eq!(
            values.rows()[1].get::<serde_json::Value>("value").unwrap(),
            json!("target")
        );
        assert!(!values.rows()[1].get::<bool>("lixcol_untracked").unwrap());
    }
);

simulation_test!(
    restore_requires_the_exact_receipt_select_shape,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('restore-sql', 'value')",
                &[],
            )
            .await
            .expect("seed write should commit");
        let source = head(&session).await;

        for sql in [
            "SELECT lix_restore($1)",
            "SELECT upper(lix_restore($1))",
            "SELECT commit_id FROM lix_restore($1), (SELECT 1)",
        ] {
            session
                .execute(sql, &[Value::Text(source.clone())])
                .await
                .expect_err("only the exact commit_id receipt select is supported");
        }

        for value in [Value::Null, Value::Integer(1)] {
            session
                .execute(RESTORE_SQL, &[value])
                .await
                .expect_err("non-text restore sources must fail");
        }
        session
            .execute(RESTORE_SQL, &[])
            .await
            .expect_err("missing restore source must fail");
    }
);

async fn head(session: &SimSession) -> String {
    let result = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("HEAD should read");
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("HEAD should be text")
}

async fn working_baseline(session: &SimSession) -> String {
    let result = session
        .execute(
            "SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()",
            &[],
        )
        .await
        .expect("working baseline should read");
    result.rows()[0]
        .get::<String>("working_base_commit_id")
        .expect("working baseline should be text")
}

async fn restore(session: &SimSession, commit_id: &str) -> Result<Option<String>, LixError> {
    let result = session
        .execute(RESTORE_SQL, &[Value::Text(commit_id.to_string())])
        .await?;
    assert_eq!(result.columns(), &["commit_id"]);
    assert_eq!(result.rows().len(), 1);
    if matches!(result.rows()[0].values(), [Value::Null]) {
        Ok(None)
    } else {
        Ok(Some(result.rows()[0].get::<String>("commit_id")?))
    }
}

async fn count(session: &SimSession, table: &str) -> i64 {
    let result = session
        .execute(&format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .await
        .expect("count should read");
    result.rows()[0]
        .get::<i64>("count")
        .expect("count should be integer")
}

async fn file_count(session: &SimSession, path: &str) -> i64 {
    let result = session
        .execute(
            "SELECT COUNT(*) AS count FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("file count should read");
    result.rows()[0]
        .get::<i64>("count")
        .expect("file count should be integer")
}

async fn value(session: &SimSession, key: &str) -> Option<serde_json::Value> {
    let result = session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("value should read");
    result
        .rows()
        .first()
        .and_then(|row| row.get::<serde_json::Value>("value").ok())
}
