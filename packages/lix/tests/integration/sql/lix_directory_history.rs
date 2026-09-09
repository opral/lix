use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    directory_history_records_rename_before_and_after_paths,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/before')", &[])
            .await
            .unwrap();
        session
            .execute(
                "UPDATE lix_directory SET path = '/after' WHERE path = '/before'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT from_path, to_path FROM lix_history('lix_directory') WHERE diff_type = 'modified'", &[]).await.unwrap(), vec![vec![Value::Text("/before".into()), Value::Text("/after".into())]]);
    }
);

simulation_test!(
    directory_history_reports_nested_ancestor_move,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/outer'), ('/outer/inner')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "UPDATE lix_directory SET path = '/moved' WHERE path = '/outer'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT from_path, to_path FROM lix_history('lix_directory') WHERE diff_type = 'modified' ORDER BY from_path", &[]).await.unwrap(), vec![vec![Value::Text("/outer".into()), Value::Text("/moved".into())],vec![Value::Text("/outer/inner".into()), Value::Text("/moved/inner".into())]]);
    }
);

simulation_test!(
    directory_history_recursive_delete_preserves_before_paths,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/outer'), ('/outer/inner')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute("DELETE FROM lix_directory WHERE path = '/outer'", &[])
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT from_path, to_path FROM lix_history('lix_directory') WHERE diff_type = 'removed' ORDER BY from_path", &[]).await.unwrap(), vec![vec![Value::Text("/outer".into()), Value::Null],vec![Value::Text("/outer/inner".into()), Value::Null]]);
    }
);

simulation_test!(
    directory_history_pinned_anchor_excludes_later_renames,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/before')", &[])
            .await
            .unwrap();
        let anchor = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .unwrap()
            .unwrap();
        session
            .execute(
                "UPDATE lix_directory SET path = '/after' WHERE path = '/before'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT diff_type, to_path FROM lix_history('lix_directory', $1) WHERE to_path = '/before'", &[Value::Text(anchor)]).await.unwrap(), vec![vec![Value::Text("added".into()), Value::Text("/before".into())]]);
    }
);
