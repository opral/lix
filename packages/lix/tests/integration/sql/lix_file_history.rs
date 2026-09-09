use super::assert_rows_eq;
use lix::{CreateBranchOptions, MergeBranchOptions, Value};

simulation_test!(
    file_history_reports_add_modify_remove_and_hydrates_exact_snapshots,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_file (id, path, content) VALUES ('01940000-0000-7000-8000-000000000001', '/before.txt', CAST('one' AS BYTEA))", &[]).await.unwrap();
        session.execute("UPDATE lix_file SET path = '/after.txt', content = CAST('two' AS BYTEA) WHERE id = '01940000-0000-7000-8000-000000000001'", &[]).await.unwrap();
        let event = session.execute("SELECT lixcol_from_commit_id, lixcol_to_commit_id FROM lix_history('lix_file') WHERE id = '01940000-0000-7000-8000-000000000001' AND diff_type = 'modified'", &[]).await.unwrap();
        for (column, expected) in [
            ("lixcol_from_commit_id", "one"),
            ("lixcol_to_commit_id", "two"),
        ] {
            let commit = event.rows()[0].get::<String>(column).unwrap();
            assert_rows_eq(session.execute("SELECT content FROM lix_as_of('lix_file', $1) WHERE id = '01940000-0000-7000-8000-000000000001'", &[Value::Text(commit)]).await.unwrap(), vec![vec![Value::Blob(expected.as_bytes().to_vec().into())]]);
        }
        session
            .execute(
                "DELETE FROM lix_file WHERE id = '01940000-0000-7000-8000-000000000001'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT diff_type, from_path, to_path FROM lix_history('lix_file') WHERE id = '01940000-0000-7000-8000-000000000001' ORDER BY lixcol_position", &[]).await.unwrap(), vec![
            vec![Value::Text("removed".into()), Value::Text("/after.txt".into()), Value::Null],
            vec![Value::Text("modified".into()), Value::Text("/before.txt".into()), Value::Text("/after.txt".into())],
            vec![Value::Text("added".into()), Value::Null, Value::Text("/before.txt".into())],
        ]);
    }
);

simulation_test!(
    file_history_path_only_file_has_empty_snapshot_content,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute("INSERT INTO lix_file (path) VALUES ('/empty')", &[])
            .await
            .unwrap();
        let history = session.execute("SELECT id, lixcol_to_commit_id FROM lix_history('lix_file') WHERE to_path = '/empty'", &[]).await.unwrap();
        let commit = history.rows()[0]
            .get::<String>("lixcol_to_commit_id")
            .unwrap();
        assert_rows_eq(
            session
                .execute(
                    "SELECT content FROM lix_as_of('lix_file', $1) WHERE path = '/empty'",
                    &[Value::Text(commit)],
                )
                .await
                .unwrap(),
            vec![vec![Value::Blob(Vec::new().into())]],
        );
    }
);

simulation_test!(
    file_history_propagates_nested_ancestor_rename,
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
        session.execute("INSERT INTO lix_file (path, content) VALUES ('/outer/inner/file', CAST('same' AS BYTEA))", &[]).await.unwrap();
        session
            .execute(
                "UPDATE lix_directory SET path = '/moved' WHERE path = '/outer'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT from_path, to_path, row_count FROM lix_history('lix_file') WHERE diff_type = 'modified'", &[]).await.unwrap(), vec![vec![Value::Text("/outer/inner/file".into()), Value::Text("/moved/inner/file".into()), Value::Integer(0)]]);
    }
);

simulation_test!(
    file_history_groups_same_commit_path_and_content_changes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/before', CAST('one' AS BYTEA))",
                &[],
            )
            .await
            .unwrap();
        session.execute("UPDATE lix_file SET path = '/after', content = CAST('two' AS BYTEA) WHERE path = '/before'", &[]).await.unwrap();
        assert_rows_eq(session.execute("SELECT diff_type, from_path, to_path FROM lix_history('lix_file') WHERE diff_type = 'modified'", &[]).await.unwrap(), vec![vec![Value::Text("modified".into()), Value::Text("/before".into()), Value::Text("/after".into())]]);
    }
);

simulation_test!(
    file_history_recursive_delete_preserves_file_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/folder')", &[])
            .await
            .unwrap();
        session.execute("INSERT INTO lix_file (id, path) VALUES ('01940000-0000-7000-8000-000000000001', '/folder/file')", &[]).await.unwrap();
        session
            .execute("DELETE FROM lix_directory WHERE path = '/folder'", &[])
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT id, from_path, to_path FROM lix_history('lix_file') WHERE diff_type = 'removed'", &[]).await.unwrap(), vec![vec![Value::Text("01940000-0000-7000-8000-000000000001".into()), Value::Text("/folder/file".into()), Value::Null]]);
    }
);

simulation_test!(
    file_history_convergent_merge_omits_source_sibling_and_empty_merge,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_file (id, path) VALUES ('01940000-0000-7000-8000-000000000001', '/before')", &[]).await.unwrap();
        let branch = "01930000-0000-7000-8000-00000000000c";
        session
            .create_branch(CreateBranchOptions {
                id: Some(branch.into()),
                name: "draft".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let draft = sim.wrap_session(engine.open_session_at(branch).await.unwrap(), &engine);
        session
            .execute(
                "UPDATE lix_file SET path = '/same' WHERE path = '/before'",
                &[],
            )
            .await
            .unwrap();
        let main_commit = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .unwrap()
            .unwrap();
        draft
            .execute(
                "UPDATE lix_file SET path = '/same' WHERE path = '/before'",
                &[],
            )
            .await
            .unwrap();
        let source_commit = engine
            .load_branch_head_commit_id(branch)
            .await
            .unwrap()
            .unwrap();
        let merge = session
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.into(),
            })
            .await
            .unwrap()
            .created_merge_commit_id
            .unwrap();
        assert_rows_eq(session.execute("SELECT lixcol_to_commit_id FROM lix_history('lix_file', $1) WHERE id = '01940000-0000-7000-8000-000000000001' AND diff_type = 'modified'", &[Value::Text(merge.clone())]).await.unwrap(), vec![vec![Value::Text(main_commit)]]);
        assert!(
            session
                .execute(
                    "SELECT commit_id FROM lix_log($1) WHERE commit_id = $2",
                    &[Value::Text(merge), Value::Text(source_commit)]
                )
                .await
                .unwrap()
                .is_empty()
        );
    }
);

simulation_test!(
    file_history_bound_identity_list_and_residual_path_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_file (id, path) VALUES ('01940000-0000-7000-8000-000000000001', '/z'), ('01940000-0000-7000-8000-000000000002', '/a')", &[]).await.unwrap();
        session
            .execute("UPDATE lix_file SET path = '/m' WHERE path = '/z'", &[])
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT to_path FROM lix_history('lix_file') WHERE id IN ($1, $2) AND to_path <> '/m' ORDER BY to_path LIMIT 1", &[Value::Text("01940000-0000-7000-8000-000000000001".into()), Value::Text("01940000-0000-7000-8000-000000000002".into())]).await.unwrap(), vec![vec![Value::Text("/a".into())]]);
    }
);

simulation_test!(
    file_history_path_filter_equals_filtered_unprojected_events,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/a'), ('/b'), ('/c')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute("UPDATE lix_file SET path = '/moved' WHERE path = '/b'", &[])
            .await
            .unwrap();
        let all = session.execute("SELECT id, diff_type, to_path FROM lix_history('lix_file') ORDER BY lixcol_position, id", &[]).await.unwrap();
        let selected = session.execute("SELECT id, diff_type, to_path FROM lix_history('lix_file') WHERE to_path = '/moved' ORDER BY lixcol_position, id", &[]).await.unwrap();
        let expected = all
            .rows()
            .iter()
            .filter(|row| row.get::<Value>("to_path").unwrap() == Value::Text("/moved".into()))
            .map(|row| row.values().to_vec())
            .collect();
        assert_rows_eq(selected, expected);
    }
);

simulation_test!(
    file_history_selected_destination_prunes_older_diffs,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_file (id, path, content) VALUES ('01940000-0000-7000-8000-000000000001', '/target', CAST('one' AS BYTEA))", &[]).await.unwrap();
        for index in 0..12 {
            session
                .execute(
                    "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE path = '/target'",
                    &[Value::Text(index.to_string())],
                )
                .await
                .unwrap();
        }
        let head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .unwrap()
            .unwrap();
        crate::sql2::take_mainline_work();
        let rows = session.execute("SELECT id FROM lix_history('lix_file', $1) WHERE id = $2 AND lixcol_to_commit_id = $1", &[Value::Text(head), Value::Text("01940000-0000-7000-8000-000000000001".into())]).await.unwrap();
        let work = crate::sql2::take_mainline_work();
        assert_eq!(rows.len(), 1);
        assert!(
            work.1 <= 1,
            "selected destination must compare only one pair: {work:?}"
        );
    }
);

simulation_test!(
    file_history_missing_identity_returns_no_events,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute("INSERT INTO lix_file (path) VALUES ('/existing')", &[])
            .await
            .unwrap();
        assert!(
            session
                .execute(
                    "SELECT id, diff_type FROM lix_history('lix_file') WHERE id = $1",
                    &[Value::Text("01940000-0000-7000-8000-000000000099".into())]
                )
                .await
                .unwrap()
                .is_empty()
        );
    }
);
