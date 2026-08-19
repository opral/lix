use lix::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    checkpoint_compacts_working_interval_and_projects_sql_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, commit_id, lixcol_global FROM lix_checkpoint",
            )
            .await,
            vec![vec![
                Value::Text(initial_commit_id.clone()),
                Value::Text(initial_commit_id.clone()),
                Value::Boolean(true),
            ]]
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-key', 'one')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'checkpoint-key'",
                &[],
            )
            .await
            .expect("tracked update should succeed");
        let old_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        assert_eq!(
            select_rows(&session, "SELECT commit_id FROM lix_checkpoint").await,
            vec![vec![Value::Text(initial_commit_id.clone())]],
            "ordinary branch commits do not mutate the global checkpoint row"
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT row_pk, schema_key, diff_type \
                 FROM lix_working_diff ORDER BY schema_key, row_pk",
            )
            .await,
            vec![vec![
                Value::Jsonb(json!(["checkpoint-key"]).into()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("added".to_string()),
            ]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT row_pk, schema_key, diff_type \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND row_pk = CAST('[\"checkpoint-key\"]' AS JSONB)",
            )
            .await,
            vec![vec![
                Value::Jsonb(json!(["checkpoint-key"]).into()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("added".to_string()),
            ]]
        );
        assert!(
            select_rows(
                &session,
                "SELECT row_pk \
                 FROM lix_working_diff \
                 WHERE schema_key = 'other_schema'",
            )
            .await
            .is_empty()
        );

        let receipt = session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_ne!(receipt.commit_id, old_head);
        assert_ne!(receipt.change_id, receipt.commit_id);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("head should load"),
            Some(receipt.commit_id.clone())
        );

        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_working_diff").await,
            vec![vec![Value::Integer(0)]]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT id, commit_id, lixcol_change_id, lixcol_global \
                     FROM lix_checkpoint WHERE id = '{}'",
                    receipt.commit_id
                ),
            )
            .await,
            vec![vec![
                Value::Text(receipt.commit_id.clone()),
                Value::Text(receipt.commit_id.clone()),
                Value::Text(receipt.change_id.clone()),
                Value::Boolean(true),
            ]]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT schema_key, row_pk FROM lix_change WHERE id = '{}'",
                    receipt.change_id
                ),
            )
            .await,
            vec![vec![
                Value::Text("lix_checkpoint".to_string()),
                Value::Jsonb(json!([receipt.commit_id.clone()]).into()),
            ]],
            "checkpoint publication must be a normal logical change"
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT id FROM lix_commit WHERE id = '{}'",
                    receipt.commit_id
                ),
            )
            .await,
            vec![vec![Value::Text(receipt.commit_id.clone())]],
            "checkpoint.commit_id foreign key must resolve to the captured branch commit"
        );
        let checkpoint_history = select_rows(
            &session,
            "SELECT commit_id, lixcol_change_id \
             FROM lix_checkpoint_history() \
             ORDER BY lixcol_depth",
        )
        .await;
        assert_eq!(checkpoint_history.len(), 2);
        assert_eq!(
            checkpoint_history[0],
            vec![
                Value::Text(receipt.commit_id.clone()),
                Value::Text(receipt.change_id.clone()),
            ],
            "latest checkpoint history row is the newly published logical change"
        );
        assert_eq!(
            checkpoint_history[1][0],
            Value::Text(initial_commit_id.clone())
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT parent_id FROM lix_commit_edge \
                     WHERE child_id = '{}'",
                    receipt.commit_id
                ),
            )
            .await,
            vec![vec![Value::Text(initial_commit_id)]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT value FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            vec![vec![Value::Jsonb(json!("two").into())]]
        );

        let timestamps_before_rebuild = select_rows(
            &session,
            "SELECT lixcol_created_at, lixcol_updated_at, lixcol_commit_id \
             FROM lix_key_value WHERE key = 'checkpoint-key'",
        )
        .await;
        assert_eq!(timestamps_before_rebuild.len(), 1);
        assert_eq!(
            timestamps_before_rebuild[0][0], timestamps_before_rebuild[0][1],
            "a newly added row must use the changelog's canonical timestamp"
        );
        assert_eq!(
            timestamps_before_rebuild[0][2],
            Value::Text(receipt.commit_id.clone()),
            "retained HOT rows must project the checkpoint as their live commit owner"
        );

        engine
            .rebuild_tracked_state_for_branch(sim.main_branch_id())
            .await
            .expect("checkpoint tracked state should rebuild");
        assert_eq!(
            select_rows(
                &session,
                "SELECT lixcol_created_at, lixcol_updated_at, lixcol_commit_id \
                 FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            timestamps_before_rebuild,
            "checkpoint timestamps must remain stable after tracked-state rebuild"
        );
    }
);

simulation_test!(
    checkpoint_surface_is_global_row_and_read_only,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        let rows = select_rows(&session, "SELECT id, commit_id FROM lix_checkpoint").await;
        assert_eq!(
            rows,
            vec![vec![
                Value::Text(sim.initial_commit_id().to_string()),
                Value::Text(sim.initial_commit_id().to_string()),
            ]]
        );

        let missing_by_branch = session
            .execute("SELECT * FROM lix_checkpoint_by_branch", &[])
            .await
            .expect_err("global checkpoint must not expose a branch-shaped surface");
        assert_eq!(missing_by_branch.code, LixError::CODE_TABLE_NOT_FOUND);

        for sql in [
            "INSERT INTO lix_checkpoint (id, commit_id) \
             VALUES ('01930000-0000-7000-8000-000000000001', 'fake')",
            "UPDATE lix_checkpoint SET commit_id = 'fake'",
            "DELETE FROM lix_working_diff",
            "DELETE FROM lix_file_working_diff",
            "UPDATE lix_directory_working_diff SET change_kind = 'fake'",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("checkpoint SQL surface should be read-only");
            assert_eq!(error.code, LixError::CODE_READ_ONLY);
        }

        for sql in [
            "SELECT * FROM lix_working_diff_by_branch",
            "SELECT * FROM lix_directory_working_diff_by_branch",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("retired by-branch surfaces must fail closed");
            assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
        }
    }
);

simulation_test!(
    working_diff_reports_net_tracked_adds_and_removals_after_a_revert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        for (key, value) in [("working-removed", "old"), ("working-reverted", "old")] {
            session
                .execute(
                    &format!("INSERT INTO lix_key_value (key, value) VALUES ('{key}', '{value}')"),
                    &[],
                )
                .await
                .expect("tracked baseline insert should succeed");
        }
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");

        for sql in [
            "UPDATE lix_key_value SET value = 'new' WHERE key = 'working-reverted'",
            "UPDATE lix_key_value SET value = 'old' WHERE key = 'working-reverted'",
            "DELETE FROM lix_key_value WHERE key = 'working-removed'",
            "INSERT INTO lix_key_value (key, value) VALUES ('working-added', 'new')",
        ] {
            session
                .execute(sql, &[])
                .await
                .expect("tracked working diff should succeed");
        }

        assert_eq!(
            select_rows(
                &session,
                "SELECT row_pk, diff_type \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY row_pk",
            )
            .await,
            vec![
                vec![
                    Value::Jsonb(json!(["working-added"]).into()),
                    Value::Text("added".to_string()),
                ],
                vec![
                    Value::Jsonb(json!(["working-removed"]).into()),
                    Value::Text("removed".to_string()),
                ],
            ],
            "the direct working-diff path must collapse a payload revert",
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT diff_type \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND row_pk = CAST('[\"working-removed\"]' AS JSONB)",
            )
            .await,
            vec![vec![Value::Text("removed".to_string())]],
            "an exact PK filter must preserve the same net diff",
        );
    }
);

simulation_test!(
    file_working_diff_reports_root_file_changes_without_directory_changes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let file_id = "01950000-0000-7000-8000-000000000099";
        let nested_file_id = "01950000-0000-7000-8000-000000000100";

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{file_id}', '/a.md', CAST('old' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("root file insert should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff",
            )
            .await,
            vec![vec![
                Value::Text(file_id.to_string()),
                Value::Text("/a.md".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]],
            "a root file add must not require an unrelated directory change",
        );

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{nested_file_id}', '/existing/b.md', CAST('old' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("nested baseline file insert should succeed");
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");
        for changed_file_id in [file_id, nested_file_id] {
            session
                .execute(
                    &format!(
                        "UPDATE lix_file SET content = CAST('new' AS BYTEA) WHERE id = '{changed_file_id}'"
                    ),
                    &[],
                )
                .await
                .expect("file data update should succeed");
        }

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![
                vec![
                    Value::Text(file_id.to_string()),
                    Value::Text("/a.md".to_string()),
                    Value::Text("/a.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
                vec![
                    Value::Text(nested_file_id.to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
            ],
            "data-only file changes must resolve targeted file and ancestor descriptors",
        );

        session
            .create_checkpoint()
            .await
            .expect("second baseline checkpoint should succeed");
        session
            .execute(&format!("DELETE FROM lix_file WHERE id = '{file_id}'"), &[])
            .await
            .expect("root file delete should succeed");
        session
            .execute(
                &format!(
                    "UPDATE lix_file SET path = '/existing/c.md' WHERE id = '{nested_file_id}'"
                ),
                &[],
            )
            .await
            .expect("nested file rename should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![
                vec![
                    Value::Text(file_id.to_string()),
                    Value::Null,
                    Value::Text("/a.md".to_string()),
                    Value::Text("removed".to_string()),
                ],
                vec![
                    Value::Text(nested_file_id.to_string()),
                    Value::Text("/existing/c.md".to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
            ],
            "descriptor-only removes and renames must use typed targeted keys",
        );
    }
);

simulation_test!(
    filesystem_working_diff_surfaces_compose_paths_and_directory_moves,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000001', '/docs/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000001".to_string()),
                Value::Text("/docs/readme.md".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path, previous_path, change_kind \
                 FROM lix_directory_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("/docs".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]]
        );

        session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_file_working_diff",).await,
            vec![vec![Value::Integer(0)]]
        );

        session
            .execute(
                "UPDATE lix_directory SET path = '/writing' WHERE path = '/docs'",
                &[],
            )
            .await
            .expect("directory move should succeed");
        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000001".to_string()),
                Value::Text("/writing/readme.md".to_string()),
                Value::Text("/docs/readme.md".to_string()),
                Value::Text("modified".to_string()),
            ]],
            "ancestor directory moves expand to descendant logical files"
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path, previous_path FROM lix_directory_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("/writing".to_string()),
                Value::Text("/docs".to_string()),
            ]]
        );
    }
);
