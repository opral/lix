use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_directory_path_insert_preserves_long_opaque_segments,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let long_segment = "a".repeat(256);
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-long-segment', $1)",
                &[Value::Text(format!("/{long_segment}/"))],
            )
            .await
            .expect("long opaque directory path segment should be accepted");

        let long_path = format!("/{}/", ["abcd"; 820].join("/"));
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-long-path', $1)",
                &[Value::Text(long_path.clone())],
            )
            .await
            .expect("long opaque directory path should be accepted");

        let result = session
            .execute(
                "SELECT id, path FROM lix_directory \
                 WHERE id IN ('dir-long-segment', 'dir-long-path') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-long-path".to_string()),
                    Value::Text(long_path),
                ],
                vec![
                    Value::Text("dir-long-segment".to_string()),
                    Value::Text(format!("/{long_segment}/")),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_directory_path_insert_preserves_percent_spelling,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path) in [
            ("dir-percent-a", "/docs/%61/"),
            ("dir-percent-nul", "/docs/%00evil/"),
            ("dir-percent-bidi", "/docs/%E2%80%AEevil/"),
        ] {
            session
                .execute(
                    &format!("INSERT INTO lix_directory (id, path) VALUES ('{id}', '{path}')"),
                    &[],
                )
                .await
                .expect("percent spelling should be stored literally");
        }

        let result = session
            .execute(
                "SELECT id, path, name FROM lix_directory \
                 WHERE id IN ('dir-percent-a', 'dir-percent-bidi', 'dir-percent-nul') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-percent-a".to_string()),
                    Value::Text("/docs/%61/".to_string()),
                    Value::Text("%61".to_string()),
                ],
                vec![
                    Value::Text("dir-percent-bidi".to_string()),
                    Value::Text("/docs/%E2%80%AEevil/".to_string()),
                    Value::Text("%E2%80%AEevil".to_string()),
                ],
                vec![
                    Value::Text("dir-percent-nul".to_string()),
                    Value::Text("/docs/%00evil/".to_string()),
                    Value::Text("%00evil".to_string()),
                ],
            ],
        );
    }
);

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
            "INSERT INTO lix_directory (id, parent_id, name) \
             VALUES ('dir-docs', NULL, 'docs')",
            &[],
        )
        .await
        .expect("directory insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let nested_insert_result = session
        .execute(
            "INSERT INTO lix_directory (id, path) \
             VALUES ('dir-nested', '/docs/nested/')",
            &[],
        )
        .await
        .expect("nested directory path insert should succeed");
    assert_eq!(nested_insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT id, path, parent_id, name \
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
        ]
    );
    assert_eq!(
        row_set.rows()[1].values(),
        &[
            Value::Text("dir-nested".to_string()),
            Value::Text("/docs/nested/".to_string()),
            Value::Text("dir-docs".to_string()),
            Value::Text("nested".to_string()),
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
            .expect("directory insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, parent_id, name \
             FROM lix_directory \
             WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [
            Value::Text(id),
            Value::Text(path),
            Value::Null,
            Value::Text(name),
        ] = values
        else {
            panic!("expected generated directory row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted directory id should be non-empty");
        assert_eq!(path, "/docs/");
        assert_eq!(name, "docs");
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
                "SELECT id, path, parent_id, name \
             FROM lix_directory \
             WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [
            Value::Text(id),
            Value::Text(path),
            Value::Null,
            Value::Text(name),
        ] = values
        else {
            panic!("expected generated directory path row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted directory id should be non-empty");
        assert_eq!(path, "/docs/");
        assert_eq!(name, "docs");
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
    lix_directory_insert_duplicate_id_reports_lix_directory,
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
                "INSERT INTO lix_directory (id, path) VALUES ('same-dir', '/a/')",
                &[],
            )
            .await
            .expect("first directory insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('same-dir', '/b/')",
                &[],
            )
            .await
            .expect_err("duplicate directory id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_directory'")
                && error.message.contains("id 'same-dir'")
                && !error.message.contains("lix_directory_descriptor"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_directory_by_branch_insert_duplicate_id_reports_lix_directory_by_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();

        session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_branch \
                     (id, path, lixcol_branch_id) \
                     VALUES ('same-dir', '/a/', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("first by-branch directory insert should succeed");

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_branch \
                     (id, path, lixcol_branch_id) \
                     VALUES ('same-dir', '/b/', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect_err("duplicate by-branch directory id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_directory_by_branch'")
                && error.message.contains("id 'same-dir'")
                && !error.message.contains("table 'lix_directory':")
                && !error.message.contains("lix_directory_descriptor"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_existing_file_entry,
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
            .execute("INSERT INTO lix_file (path) VALUES ('/foo')", &[])
            .await
            .expect("file insert should succeed");

        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo/')", &[])
            .await
            .expect_err("directory should conflict with file at same entry name");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_directory_descriptor_shape_insert_rejects_existing_file_entry,
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
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('file-foo', NULL, 'foo')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('dir-foo', NULL, 'foo')",
                &[],
            )
            .await
            .expect_err("descriptor-shaped directory insert should conflict with file");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_directory_update_rejects_existing_file_entry,
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
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('dir-bar', NULL, 'bar')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        session
            .execute("INSERT INTO lix_file (path) VALUES ('/foo')", &[])
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_directory SET name = 'foo' WHERE id = 'dir-bar'",
                &[],
            )
            .await
            .expect_err("directory rename should conflict with file");

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

        for path in ["/a/../b/", "/a/./b/"] {
            let error = session
                .execute(
                    "INSERT INTO lix_directory (path) VALUES ($1)",
                    &[Value::Text(path.to_string())],
                )
                .await
                .expect_err("directory path insert should reject dot segments");

            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        }

        let result = session
            .execute("SELECT path FROM lix_directory WHERE path = '/b/'", &[])
            .await
            .expect("directory read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_directory_descriptor_write_rejects_slash_in_name_at_schema_boundary,
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
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (lix_json('[\"dir-slash\"]'), 'lix_directory_descriptor', NULL, $1, false, false)",
                &[Value::Json(json!({
                    "id": "dir-slash",
                    "parent_id": null,
                    "name": "nested/name",
                }))],
            )
            .await
            .expect_err("directory descriptor name must keep '/' as structural separator");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.message.contains("lix_directory_descriptor"));
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
    lix_directory_descriptor_writes_preserve_opaque_names,
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
            .execute("INSERT INTO lix_directory (path) VALUES ('/Café/')", &[])
            .await
            .expect("directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (lix_json('[\"dir-cafe-decomposed\"]'), 'lix_directory_descriptor', NULL, $1, false, false)",
                &[Value::Json(json!({
                    "id": "dir-cafe-decomposed",
                    "parent_id": null,
                    "name": "Cafe\u{301}",
                                    }))],
            )
            .await
            .expect("decomposed descriptor name should remain distinct");

        session
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES (lix_json('[\"dir-zero-width\"]'), 'lix_directory_descriptor', NULL, $1, false, false)",
                &[Value::Json(json!({
                    "id": "dir-zero-width",
                    "parent_id": null,
                    "name": "zero\u{200D}width",
                                    }))],
            )
            .await
            .expect("zero-width descriptor name should be preserved");

        let result = session
            .execute(
                "SELECT id, path, name FROM lix_directory \
                 WHERE id IN ('dir-cafe-decomposed', 'dir-zero-width') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-cafe-decomposed".to_string()),
                    Value::Text("/Cafe\u{301}/".to_string()),
                    Value::Text("Cafe\u{301}".to_string()),
                ],
                vec![
                    Value::Text("dir-zero-width".to_string()),
                    Value::Text("/zero\u{200D}width/".to_string()),
                    Value::Text("zero\u{200D}width".to_string()),
                ],
            ],
        );
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
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 (lix_json('[\"dir-a\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-a\",\"parent_id\":\"dir-b\",\"name\":\"a\"}'), false, false), \
                 (lix_json('[\"dir-b\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-b\",\"parent_id\":\"dir-a\",\"name\":\"b\"}'), false, false)",
                &[],
            )
            .await
            .expect_err("descriptor cycles staged through lix_state must be rejected");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    lix_state_insert_rejects_directory_file_namespace_conflict,
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
            .execute("INSERT INTO lix_file (path) VALUES ('/foo')", &[])
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 (lix_json('[\"dir-foo\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-foo\",\"parent_id\":null,\"name\":\"foo\"}'), false, false)",
                &[],
            )
            .await
            .expect_err("lix_state directory descriptor must not bypass filesystem namespace");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("filesystem namespace conflict"),
            "expected namespace conflict error: {error}"
        );
    }
);

simulation_test!(
    lix_directory_allows_branch_local_entry_matching_global_file_entry,
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
                "INSERT INTO lix_file (id, path, lixcol_global) \
                 VALUES ('global-file-foo', '/foo', true)",
                &[],
            )
            .await
            .expect("global file insert should succeed");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('branch-dir-foo', '/foo/')",
                &[],
            )
            .await
            .expect("branch-local directory should be a distinct storage namespace");

        let global_file = session
            .execute(
                "SELECT id, path, lixcol_branch_id, lixcol_global \
                 FROM lix_file_by_branch \
                 WHERE id = 'global-file-foo' AND lixcol_branch_id = 'global'",
                &[],
            )
            .await
            .expect("global file should query");
        let branch_directory = session
            .execute(
                "SELECT id, path \
                 FROM lix_directory \
                 WHERE id = 'branch-dir-foo'",
                &[],
            )
            .await
            .expect("branch directory should query");

        assert_eq!(global_file.len(), 1);
        assert_eq!(branch_directory.len(), 1);
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
                "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F')",
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
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(3));

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
                    "SELECT entity_pk, schema_key \
                 FROM lix_state \
                 WHERE entity_pk IN (lix_json('[\"{}\"]'), lix_json('[\"{}\"]'), lix_json('[\"file-readme\"]')) \
                 ORDER BY schema_key, entity_pk",
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
    lix_directory_by_branch_expands_global_rows,
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
                "INSERT INTO lix_directory (id, path, lixcol_global, lixcol_untracked) \
                 VALUES ('dir-global-overlay', '/shared/', true, false)",
                &[],
            )
            .await
            .expect("global directory insert should succeed");

        let result = session
            .execute(
                "SELECT id, path, lixcol_branch_id, lixcol_global, lixcol_untracked \
                 FROM lix_directory_by_branch \
                 WHERE id = 'dir-global-overlay' \
                 ORDER BY lixcol_branch_id",
                &[],
            )
            .await
            .expect("directory by-branch read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-global-overlay".to_string()),
                    Value::Text("/shared/".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
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

simulation_test!(
    lix_directory_global_path_insert_reuses_existing_global_directory,
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
                "INSERT INTO lix_directory (id, path, lixcol_global) \
                 VALUES ('global-shared-dir-parent', '/shared/', true)",
                &[],
            )
            .await
            .expect("global parent directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_global) \
                 VALUES ('global-shared-dir-child', '/shared/child/', true)",
                &[],
            )
            .await
            .expect("global directory insert should reuse existing global parent directory");

        let result = session
            .execute(
                "SELECT path FROM lix_directory WHERE id = 'global-shared-dir-child'",
                &[],
            )
            .await
            .expect("global directory should read through active overlay");
        assert_rows_eq(
            result,
            vec![vec![Value::Text("/shared/child/".to_string())]],
        );
    }
);

simulation_test!(
    lix_directory_tracked_path_insert_promotes_untracked_directory,
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
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-docs', '/docs/', true)",
                &[],
            )
            .await
            .expect("untracked directory insert should succeed");
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs/')", &[])
            .await
            .expect("tracked directory insert should promote same path id");

        let result = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("dir-docs".to_string()),
                Value::Text("/docs/".to_string()),
                Value::Boolean(false),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_untracked_path_insert_reuses_tracked_parent_directory,
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
                "INSERT INTO lix_directory (id, path) VALUES ('dir-docs', '/docs/')",
                &[],
            )
            .await
            .expect("tracked parent insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-draft', '/docs/draft/', true)",
                &[],
            )
            .await
            .expect("untracked child insert should reuse tracked parent");

        let result = session
            .execute(
                "SELECT id, path, parent_id, lixcol_untracked \
                 FROM lix_directory \
                 WHERE id IN ('dir-docs', 'dir-draft') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-docs".to_string()),
                    Value::Text("/docs/".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("dir-draft".to_string()),
                    Value::Text("/docs/draft/".to_string()),
                    Value::Text("dir-docs".to_string()),
                    Value::Boolean(true),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_untracked_duplicate_with_different_id,
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
                "INSERT INTO lix_directory (id, path) VALUES ('dir-docs', '/docs/')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");
        let error = session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-docs-shadow', '/docs/', true)",
                &[],
            )
            .await
            .expect_err("untracked duplicate with a different id should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_directory_path_update_promotes_untracked_parents,
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
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-parent', '/archive/', true)",
                &[],
            )
            .await
            .expect("untracked parent insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-docs', '/docs/')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");

        session
            .execute(
                "UPDATE lix_directory SET path = '/archive/docs/' WHERE id = 'dir-docs'",
                &[],
            )
            .await
            .expect("directory path update should promote missing tracked parent");

        let result = session
            .execute(
                "SELECT id, path, parent_id, lixcol_untracked \
                 FROM lix_directory \
                 WHERE id IN ('dir-parent', 'dir-docs') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("dir-docs".to_string()),
                    Value::Text("/archive/docs/".to_string()),
                    Value::Text("dir-parent".to_string()),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("dir-parent".to_string()),
                    Value::Text("/archive/".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_do_update_uses_excluded,
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
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('dir-upsert', NULL, 'old')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('dir-upsert', NULL, 'new') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = 'dir-upsert'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("dir-upsert".to_string()),
                Value::Text("/new/".to_string()),
                Value::Null,
                Value::Text("new".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_do_nothing_keeps_existing,
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
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('dir-keep', NULL, 'keep')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('dir-keep', NULL, 'ignored') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = 'dir-keep'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("dir-keep".to_string()),
                Value::Text("/keep/".to_string()),
                Value::Null,
                Value::Text("keep".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_inserts_when_absent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('dir-fresh', NULL, 'fresh') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert on absent id should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = 'dir-fresh'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("dir-fresh".to_string()),
                Value::Text("/fresh/".to_string()),
                Value::Null,
                Value::Text("fresh".to_string()),
            ]],
        );
    }
);
