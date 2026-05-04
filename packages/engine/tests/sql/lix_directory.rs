use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;

use super::assert_rows_eq;

simulation_test!(lix_directory_insert_reads_nested_paths, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let insert_result = session
        .execute(
            "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
            &[],
        )
        .await
        .expect("directory insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let nested_insert_result = session
        .execute(
            "INSERT INTO lix_directory (id, path, hidden) \
             VALUES ('dir-nested', '/docs/nested/', false)",
            &[],
        )
        .await
        .expect("nested directory path insert should succeed");
    assert_eq!(nested_insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT id, path, parent_id, name, hidden \
             FROM lix_directory \
             WHERE id IN ('dir-docs', 'dir-nested') \
             ORDER BY path",
            &[],
        )
        .await
        .expect("directory read should succeed");
    let row_set = result;
    assert_eq!(row_set.len(), 2);
    assert_eq!(
        row_set.rows()[0].values(),
        &[
            Value::Text("dir-docs".to_string()),
            Value::Text("/docs/".to_string()),
            Value::Null,
            Value::Text("docs".to_string()),
            Value::Boolean(false),
        ]
    );
    assert_eq!(
        row_set.rows()[1].values(),
        &[
            Value::Text("dir-nested".to_string()),
            Value::Text("/docs/nested/".to_string()),
            Value::Text("dir-docs".to_string()),
            Value::Text("nested".to_string()),
            Value::Boolean(false),
        ]
    );
});

simulation_test!(
    lix_directory_insert_applies_defaulted_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let insert_result = session
            .execute(
                "INSERT INTO lix_directory (parent_id, name) \
             VALUES (NULL, 'docs')",
                &[],
            )
            .await
            .expect("directory insert should apply defaulted id and hidden flag");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, parent_id, name, hidden \
             FROM lix_directory \
             WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Null, Value::Text(name), Value::Boolean(hidden)] =
            values
        else {
            panic!("expected generated directory row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted directory id should be non-empty");
        assert_eq!(path, "/docs/");
        assert_eq!(name, "docs");
        assert!(!hidden);
    }
);

simulation_test!(
    lix_directory_path_insert_applies_defaulted_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let insert_result = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs/')", &[])
            .await
            .expect("directory path insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, parent_id, name, hidden \
             FROM lix_directory \
             WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Null, Value::Text(name), Value::Boolean(hidden)] =
            values
        else {
            panic!("expected generated directory path row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted directory id should be non-empty");
        assert_eq!(path, "/docs/");
        assert_eq!(name, "docs");
        assert!(!hidden);
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_duplicate_root_path,
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
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs/')", &[])
            .await
            .expect("first directory insert should succeed");
        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs/')", &[])
            .await
            .expect_err("duplicate directory path insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_dot_segments,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for path in ["/a/../b/", "/a/%2e%2e/b/", "/a/./b/"] {
            let error = session
                .execute(
                    "INSERT INTO lix_directory (path) VALUES ($1)",
                    &[Value::Text(path.to_string())],
                )
                .await
                .expect_err("directory path insert should reject dot segments");

            assert_eq!(error.code, "LIX_ERROR_PATH_DOT_SEGMENT");
        }

        let result = session
            .execute("SELECT path FROM lix_directory WHERE path = '/b/'", &[])
            .await
            .expect("directory read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_directory_update_rejects_parent_cycle,
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
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ('dir-parent', NULL, 'parent'), \
                 ('dir-child', 'dir-parent', 'child')",
                &[],
            )
            .await
            .expect("directory tree insert should succeed");

        let self_cycle = session
            .execute(
                "UPDATE lix_directory SET parent_id = id WHERE id = 'dir-parent'",
                &[],
            )
            .await
            .expect_err("self parent must be rejected");
        assert_eq!(self_cycle.code, LixError::CODE_CONSTRAINT_VIOLATION);

        let descendant_cycle = session
            .execute(
                "UPDATE lix_directory SET parent_id = 'dir-child' WHERE id = 'dir-parent'",
                &[],
            )
            .await
            .expect_err("parenting a directory under its descendant must be rejected");
        assert_eq!(descendant_cycle.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    lix_state_insert_rejects_directory_parent_cycle,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
                 ) VALUES \
                 ('dir-a', 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-a\",\"parent_id\":\"dir-b\",\"name\":\"a\",\"hidden\":false}'), '1', false, false), \
                 ('dir-b', 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-b\",\"parent_id\":\"dir-a\",\"name\":\"b\",\"hidden\":false}'), '1', false, false)",
                &[],
            )
            .await
            .expect_err("descriptor cycles staged through lix_state must be rejected");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    lix_directory_delete_recursively_deletes_tree,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let file_result = session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await
            .expect("file insert should succeed");
        assert_eq!(file_result, ExecuteResult::from_rows_affected(1));

        let directory_ids_result = session
            .execute(
                "SELECT id \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("directory id read before delete should succeed");
        let directory_id_rows = directory_ids_result;
        assert_eq!(directory_id_rows.len(), 2);
        let directory_ids = directory_id_rows
            .rows()
            .iter()
            .map(|row| {
                let Value::Text(id) = &row.values()[0] else {
                    panic!("directory id should be text");
                };
                id.clone()
            })
            .collect::<Vec<_>>();

        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs/'", &[])
            .await
            .expect("recursive directory delete should succeed");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(1));

        let directories_result = session
            .execute(
                "SELECT id, path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("directory read after delete should succeed");
        let directory_rows = directories_result;
        assert_eq!(
            directory_rows.len(),
            0,
            "recursive directory delete should delete the root and child directories"
        );

        let file_result = session
            .execute(
                "SELECT id, path \
             FROM lix_file \
             WHERE path = '/docs/guides/readme.md'",
                &[],
            )
            .await
            .expect("file read after delete should succeed");
        let file_rows = file_result;
        assert_eq!(
            file_rows.len(),
            0,
            "recursive directory delete should delete nested files"
        );

        let state_result = session
            .execute(
                &format!(
                    "SELECT entity_id, schema_key \
                 FROM lix_state \
                 WHERE entity_id IN ('{}', '{}', 'file-readme') \
                 ORDER BY schema_key, entity_id",
                    directory_ids[0], directory_ids[1]
                ),
                &[],
            )
            .await
            .expect("state read after delete should succeed");
        let state_rows = state_result;
        assert_eq!(
            state_rows.len(),
            0,
            "recursive directory delete should make descriptor/blob-ref state rows not visible"
        );
    }
);

simulation_test!(
    lix_directory_by_version_expands_global_rows,
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
                "INSERT INTO lix_directory (id, path, hidden, lixcol_global, lixcol_untracked) \
                 VALUES ('dir-global-overlay', '/shared/', false, true, false)",
                &[],
            )
            .await
            .expect("global directory insert should succeed");

        let result = session
            .execute(
                "SELECT id, path, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM lix_directory_by_version \
                 WHERE id = 'dir-global-overlay' \
                 ORDER BY lixcol_version_id",
                &[],
            )
            .await
            .expect("directory by-version read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-global-overlay".to_string()),
                    Value::Text("/shared/".to_string()),
                    Value::Text(sim.main_version_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("dir-global-overlay".to_string()),
                    Value::Text("/shared/".to_string()),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ],
        );
    }
);
