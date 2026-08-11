use lix::ExecuteResult;
use lix::LixError;
use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_directory_path_insert_preserves_long_opaque_segments,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let long_segment = "a".repeat(256);
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6469722d-6c6f-8e67-8d73-65676d656e00', $1)",
                &[Value::Text(format!("/{long_segment}"))],
            )
            .await
            .expect("long opaque directory path segment should be accepted");

        let long_path = format!("/{}", ["abcd"; 820].join("/"));
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6469722d-6c6f-8e67-8d70-617468000000', $1)",
                &[Value::Text(long_path.clone())],
            )
            .await
            .expect("long opaque directory path should be accepted");

        let result = session
            .execute(
                "SELECT id, path FROM lix_directory \
                 WHERE id IN ('6469722d-6c6f-8e67-8d73-65676d656e00', '6469722d-6c6f-8e67-8d70-617468000000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-6c6f-8e67-8d70-617468000000".to_string()),
                    Value::Text(long_path),
                ],
                vec![
                    Value::Text("6469722d-6c6f-8e67-8d73-65676d656e00".to_string()),
                    Value::Text(format!("/{long_segment}")),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path) in [
            ("6469722d-7065-8263-856e-742d61000000", "/docs/%61"),
            ("6469722d-7065-8263-856e-742d6e756c00", "/docs/%00evil"),
            (
                "6469722d-7065-8263-856e-742d62696400",
                "/docs/%E2%80%AEevil",
            ),
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
                 WHERE id IN ('6469722d-7065-8263-856e-742d61000000', '6469722d-7065-8263-856e-742d62696400', '6469722d-7065-8263-856e-742d6e756c00') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-7065-8263-856e-742d61000000".to_string()),
                    Value::Text("/docs/%61".to_string()),
                    Value::Text("%61".to_string()),
                ],
                vec![
                    Value::Text("6469722d-7065-8263-856e-742d62696400".to_string()),
                    Value::Text("/docs/%E2%80%AEevil".to_string()),
                    Value::Text("%E2%80%AEevil".to_string()),
                ],
                vec![
                    Value::Text("6469722d-7065-8263-856e-742d6e756c00".to_string()),
                    Value::Text("/docs/%00evil".to_string()),
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
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let insert_result = session
        .execute(
            "INSERT INTO lix_directory (id, parent_id, name) \
             VALUES ('6469722d-646f-8373-8000-000000000000', NULL, 'docs')",
            &[],
        )
        .await
        .expect("directory insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let nested_insert_result = session
        .execute(
            "INSERT INTO lix_directory (id, path) \
             VALUES ('6469722d-6e65-8374-8564-000000000000', '/docs/nested')",
            &[],
        )
        .await
        .expect("nested directory path insert should succeed");
    assert_eq!(nested_insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT id, path, parent_id, name \
             FROM lix_directory \
             WHERE id IN ('6469722d-646f-8373-8000-000000000000', '6469722d-6e65-8374-8564-000000000000') \
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
            Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
            Value::Text("/docs".to_string()),
            Value::Null,
            Value::Text("docs".to_string()),
        ]
    );
    assert_eq!(
        row_set.rows()[1].values(),
        &[
            Value::Text("6469722d-6e65-8374-8564-000000000000".to_string()),
            Value::Text("/docs/nested".to_string()),
            Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
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
                .open_session()
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
             WHERE path = '/docs'",
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
        assert_eq!(path, "/docs");
        assert_eq!(name, "docs");
    }
);

simulation_test!(
    lix_directory_path_insert_applies_defaulted_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let insert_result = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
            .await
            .expect("directory path insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, parent_id, name \
             FROM lix_directory \
             WHERE path = '/docs'",
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
        assert_eq!(path, "/docs");
        assert_eq!(name, "docs");
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_duplicate_root_path,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
            .await
            .expect("first directory insert should succeed");
        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('73616d65-2d64-8972-8000-000000000000', '/a')",
                &[],
            )
            .await
            .expect("first directory insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('73616d65-2d64-8972-8000-000000000000', '/b')",
                &[],
            )
            .await
            .expect_err("duplicate directory id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_directory'")
                && error
                    .message
                    .contains("id '73616d65-2d64-8972-8000-000000000000'")
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
                .open_session()
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
                     VALUES ('73616d65-2d64-8972-8000-000000000000', '/a', '{branch_id}')"
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
                     VALUES ('73616d65-2d64-8972-8000-000000000000', '/b', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect_err("duplicate by-branch directory id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_directory_by_branch'")
                && error
                    .message
                    .contains("id '73616d65-2d64-8972-8000-000000000000'")
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_file (path) VALUES ('/foo')", &[])
            .await
            .expect("file insert should succeed");

        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo')", &[])
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('66696c65-2d66-8f6f-8000-000000000000', NULL, 'foo')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('6469722d-666f-8f00-8000-000000000000', NULL, 'foo')",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('6469722d-6261-8200-8000-000000000000', NULL, 'bar')",
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
                "UPDATE lix_directory SET name = 'foo' WHERE id = '6469722d-6261-8200-8000-000000000000'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for path in ["/a/../b", "/a/./b"] {
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
            .execute("SELECT path FROM lix_directory WHERE path = '/b'", &[])
            .await
            .expect("directory read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_trailing_slash,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs/')", &[])
            .await
            .expect_err("non-root directory path must reject a trailing slash");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(error.message, "non-root path must not end with '/'");
    }
);

simulation_test!(
    lix_directory_write_rejects_invalid_name_segment_at_validator_boundary,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-736c-8173-8800-000000000000', NULL, 'nested/name')",
                &[],
            )
            .await
            .expect_err("directory name must keep '/' as structural separator");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error.message.contains("path segment must not contain '/'"),
            "{error}"
        );

        // The half of the removed `pattern` that motivated hardcoding it:
        // '.' and '..' must not be storable as directory names.
        let traversal = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-736c-8173-8800-000000000001', NULL, '..')",
                &[],
            )
            .await
            .expect_err("directory name must not be a traversal segment");

        assert!(
            traversal.message.contains("cannot be '.' or '..'"),
            "{traversal}"
        );
    }
);

simulation_test!(
    lix_directory_update_rejects_parent_cycle,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ('6469722d-7061-8265-8e74-000000000000', NULL, 'parent'), \
                 ('6469722d-6368-896c-8400-000000000000', '6469722d-7061-8265-8e74-000000000000', 'child')",
                &[],
            )
            .await
            .expect("directory tree insert should succeed");

        let self_cycle = session
            .execute(
                "UPDATE lix_directory SET parent_id = id WHERE id = '6469722d-7061-8265-8e74-000000000000'",
                &[],
            )
            .await
            .expect_err("self parent must be rejected");
        assert_eq!(self_cycle.code, LixError::CODE_CONSTRAINT_VIOLATION);

        let descendant_cycle = session
            .execute(
                "UPDATE lix_directory SET parent_id = '6469722d-6368-896c-8400-000000000000' WHERE id = '6469722d-7061-8265-8e74-000000000000'",
                &[],
            )
            .await
            .expect_err("parenting a directory under its descendant must be rejected");
        assert_eq!(descendant_cycle.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    lix_directory_writes_preserve_opaque_names,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/Café')", &[])
            .await
            .expect("directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-6361-8665-8d64-65636f6d7000', NULL, 'Cafe\u{301}')",
                &[],
            )
            .await
            .expect("decomposed directory name should remain distinct");

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-7a65-826f-8d77-696474680000', NULL, 'zero\u{200D}width')",
                &[],
            )
            .await
            .expect("zero-width directory name should be preserved");

        let result = session
            .execute(
                "SELECT id, path, name FROM lix_directory \
                 WHERE id IN ('6469722d-6361-8665-8d64-65636f6d7000', '6469722d-7a65-826f-8d77-696474680000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-6361-8665-8d64-65636f6d7000".to_string()),
                    Value::Text("/Cafe\u{301}".to_string()),
                    Value::Text("Cafe\u{301}".to_string()),
                ],
                vec![
                    Value::Text("6469722d-7a65-826f-8d77-696474680000".to_string()),
                    Value::Text("/zero\u{200D}width".to_string()),
                    Value::Text("zero\u{200D}width".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_directory_insert_rejects_directory_parent_cycle,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ('6469722d-6100-8000-8000-000000000000', '6469722d-6200-8000-8000-000000000000', 'a'), \
                 ('6469722d-6200-8000-8000-000000000000', '6469722d-6100-8000-8000-000000000000', 'b')",
                &[],
            )
            .await
            .expect_err("directory cycles must be rejected");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }
);

simulation_test!(
    lix_directory_insert_rejects_directory_file_namespace_conflict,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
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
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-666f-8f00-8000-000000000000', NULL, 'foo')",
                &[],
            )
            .await
            .expect_err("directory insert must not bypass filesystem namespace");

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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, lixcol_global) \
                 VALUES ('676c6f62-616c-8d66-896c-652d666f6f00', '/foo', true)",
                &[],
            )
            .await
            .expect("global file insert should succeed");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6272616e-6368-8d64-8972-2d666f6f0000', '/foo')",
                &[],
            )
            .await
            .expect("branch-local directory should be a distinct storage namespace");

        let global_file = session
            .execute(
                "SELECT id, path, lixcol_branch_id, lixcol_global \
                 FROM lix_file_by_branch \
                 WHERE id = '676c6f62-616c-8d66-896c-652d666f6f00' AND lixcol_branch_id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
                &[],
            )
            .await
            .expect("global file should query");
        let branch_directory = session
            .execute(
                "SELECT id, path \
                 FROM lix_directory \
                 WHERE id = '6272616e-6368-8d64-8972-2d666f6f0000'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let file_result = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
             VALUES ('66696c65-2d72-8561-846d-650000000000', '/docs/guides/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        assert_eq!(file_result, ExecuteResult::from_rows_affected(1));

        let directory_ids_result = session
            .execute(
                "SELECT id \
             FROM lix_directory \
             WHERE path IN ('/docs', '/docs/guides') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("directory id read before delete should succeed");
        let directory_id_rows = directory_ids_result;
        assert_eq!(directory_id_rows.len(), 2);
        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs'", &[])
            .await
            .expect("recursive directory delete should succeed");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(3));

        let directories_result = session
            .execute(
                "SELECT id, path \
             FROM lix_directory \
             WHERE path IN ('/docs', '/docs/guides') \
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
    }
);

simulation_test!(
    lix_directory_by_branch_expands_global_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_global, lixcol_untracked) \
                 VALUES ('6469722d-676c-8f62-816c-2d6f76657200', '/shared', true, false)",
                &[],
            )
            .await
            .expect("global directory insert should succeed");

        let result = session
            .execute(
                "SELECT id, path, lixcol_branch_id, lixcol_global, lixcol_untracked \
                 FROM lix_directory_by_branch \
                 WHERE id = '6469722d-676c-8f62-816c-2d6f76657200' \
                 ORDER BY lixcol_branch_id",
                &[],
            )
            .await
            .expect("directory by-branch read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-676c-8f62-816c-2d6f76657200".to_string()),
                    Value::Text("/shared".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("6469722d-676c-8f62-816c-2d6f76657200".to_string()),
                    Value::Text("/shared".to_string()),
                    Value::Text("ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_global) \
                 VALUES ('676c6f62-616c-8d73-8861-7265642d6401', '/shared', true)",
                &[],
            )
            .await
            .expect("global parent directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_global) \
                 VALUES ('676c6f62-616c-8d73-8861-7265642d6402', '/shared/child', true)",
                &[],
            )
            .await
            .expect("global directory insert should reuse existing global parent directory");

        let result = session
            .execute(
                "SELECT path FROM lix_directory WHERE id = '676c6f62-616c-8d73-8861-7265642d6402'",
                &[],
            )
            .await
            .expect("global directory should read through active overlay");
        assert_rows_eq(result, vec![vec![Value::Text("/shared/child".to_string())]]);
    }
);

simulation_test!(
    lix_directory_tracked_path_insert_rejects_untracked_directory,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('6469722d-646f-8373-8000-000000000000', '/docs', true)",
                &[],
            )
            .await
            .expect("untracked directory insert should succeed");
        let error = session
            .execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
            .await
            .expect_err("tracked directory insert must not replace an untracked directory");
        assert!(
            error
                .message
                .contains("a canonical untracked row already exists; delete it first"),
            "durability collision should have a targeted error: {error:?}"
        );

        let result = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/docs'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
                Value::Text("/docs".to_string()),
                Value::Boolean(true),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                &[],
            )
            .await
            .expect("tracked parent insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('6469722d-6472-8166-8400-000000000000', '/docs/draft', true)",
                &[],
            )
            .await
            .expect("untracked child insert should reuse tracked parent");

        let result = session
            .execute(
                "SELECT id, path, parent_id, lixcol_untracked \
                 FROM lix_directory \
                 WHERE id IN ('6469722d-646f-8373-8000-000000000000', '6469722d-6472-8166-8400-000000000000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
                    Value::Text("/docs".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("6469722d-6472-8166-8400-000000000000".to_string()),
                    Value::Text("/docs/draft".to_string()),
                    Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");
        let error = session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('6469722d-646f-8373-8d73-6861646f7700', '/docs', true)",
                &[],
            )
            .await
            .expect_err("untracked duplicate with a different id should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_directory_path_update_rejects_untracked_parent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('6469722d-7061-8265-8e74-000000000000', '/archive', true)",
                &[],
            )
            .await
            .expect("untracked parent insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_directory SET path = '/archive/docs' WHERE id = '6469722d-646f-8373-8000-000000000000'",
                &[],
            )
            .await
            .expect_err("directory path update must not promote an untracked parent");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let result = session
            .execute(
                "SELECT id, path, parent_id, lixcol_untracked \
                 FROM lix_directory \
                 WHERE id IN ('6469722d-7061-8265-8e74-000000000000', '6469722d-646f-8373-8000-000000000000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
                    Value::Text("/docs".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("6469722d-7061-8265-8e74-000000000000".to_string()),
                    Value::Text("/archive".to_string()),
                    Value::Null,
                    Value::Boolean(true),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-7570-8365-8274-000000000000', NULL, 'old')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-7570-8365-8274-000000000000', NULL, 'new') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = '6469722d-7570-8365-8274-000000000000'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-7570-8365-8274-000000000000".to_string()),
                Value::Text("/new".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-6b65-8570-8000-000000000000', NULL, 'keep')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-6b65-8570-8000-000000000000', NULL, 'ignored') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = '6469722d-6b65-8570-8000-000000000000'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-6b65-8570-8000-000000000000".to_string()),
                Value::Text("/keep".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) \
                 VALUES ('6469722d-6672-8573-8800-000000000000', NULL, 'fresh') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert on absent id should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, parent_id, name FROM lix_directory \
                 WHERE id = '6469722d-6672-8573-8800-000000000000'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-6672-8573-8800-000000000000".to_string()),
                Value::Text("/fresh".to_string()),
                Value::Null,
                Value::Text("fresh".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_path_do_nothing_is_idempotent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('6469722d-7061-8468-8d6b-656570000000', '/path-keep')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (path) \
                 VALUES ('/path-keep') \
                 ON CONFLICT (path) DO NOTHING",
                &[],
            )
            .await
            .expect("directory path DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id, path FROM lix_directory WHERE path = '/path-keep'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-7061-8468-8d6b-656570000000".to_string()),
                Value::Text("/path-keep".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_path_do_update_metadata,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_metadata) \
                 VALUES ('6469722d-7061-8468-8d6d-657461000000', '/path-meta', CAST('{\"version\":1}' AS JSONB))",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (path, lixcol_metadata) \
                 VALUES ('/path-meta', CAST('{\"version\":2}' AS JSONB)) \
                 ON CONFLICT (path) DO UPDATE SET lixcol_metadata = excluded.lixcol_metadata",
                &[],
            )
            .await
            .expect("directory path DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, lixcol_metadata FROM lix_directory WHERE path = '/path-meta'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-7061-8468-8d6d-657461000000".to_string()),
                Value::Jsonb(json!({"version": 2}).into()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_by_branch_insert_on_conflict_path_branch_do_nothing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
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
                     VALUES ('6469722d-6272-816e-8368-2d7061746800', '/branch-dir', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("seed by-branch directory insert should succeed");

        let result = session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_branch \
                     (path, lixcol_branch_id) \
                     VALUES ('/branch-dir', '{branch_id}') \
                     ON CONFLICT (path, lixcol_branch_id) DO NOTHING"
                ),
                &[],
            )
            .await
            .expect("by-branch directory path upsert should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/branch-dir'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![Value::Text(
                "6469722d-6272-816e-8368-2d7061746800".to_string(),
            )]],
        );
    }
);

simulation_test!(
    lix_directory_by_branch_insert_on_conflict_path_without_branch_target_rejects,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_branch \
                     (path, lixcol_branch_id) \
                     VALUES ('/dir-reject', '{branch_id}') \
                     ON CONFLICT (path) DO NOTHING"
                ),
                &[],
            )
            .await
            .expect_err("by-branch path-only target should be rejected");
        assert!(
            error
                .message
                .contains("path identity columns (path, lixcol_branch_id)")
        );
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_path_rejects_missing_path,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_directory (id) \
                 VALUES ('6469722d-6d69-8373-896e-672d70617400') \
                 ON CONFLICT (path) DO NOTHING",
                &[],
            )
            .await
            .expect_err("path upsert without path should be rejected");
        assert!(error.message.contains("requires non-null path"));
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_path_rejects_untracked_collision,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('6469722d-7472-8163-8b65-642d636f6c00', '/dir-collision')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_directory (path, lixcol_untracked) \
                 VALUES ('/dir-collision', true) \
                 ON CONFLICT (path) DO NOTHING",
                &[],
            )
            .await
            .expect_err("tracked/untracked path collision should be rejected");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("existing tracked directory"));
    }
);

simulation_test!(
    lix_directory_insert_on_conflict_path_updates_visible_global_directory,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_metadata, lixcol_global) \
                 VALUES ('6469722d-676c-8f62-816c-2d7061746800', '/global-dir', CAST('{\"version\":1}' AS JSONB), true)",
                &[],
            )
            .await
            .expect("global seed directory insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_directory (path, lixcol_metadata) \
                 VALUES ('/global-dir', CAST('{\"version\":2}' AS JSONB)) \
                 ON CONFLICT (path) DO UPDATE SET lixcol_metadata = excluded.lixcol_metadata",
                &[],
            )
            .await
            .expect("path upsert should update visible global directory");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, lixcol_metadata, lixcol_global, lixcol_branch_id \
                 FROM lix_directory_by_branch \
                 WHERE id = '6469722d-676c-8f62-816c-2d7061746800' AND lixcol_branch_id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
                &[],
            )
            .await
            .expect("global directory read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("6469722d-676c-8f62-816c-2d7061746800".to_string()),
                Value::Jsonb(json!({"version": 2}).into()),
                Value::Boolean(true),
                Value::Text("ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_transaction_path_index_rebuilds_for_subtree_move_and_delete,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('7472616e-7361-8374-896f-6e2d73756200', '/old/sub/readme.md', CAST('readme' AS BYTEA))",
                &[],
            )
            .await
            .expect("subtree fixture should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        let warm = transaction
            .execute(
                "SELECT id FROM lix_file WHERE path = '/old/sub/readme.md'",
                &[],
            )
            .await
            .expect("old subtree path should warm the transaction index");
        assert_rows_eq(
            warm,
            vec![vec![Value::Text(
                "7472616e-7361-8374-896f-6e2d73756200".to_string(),
            )]],
        );

        let moved = transaction
            .execute(
                "UPDATE lix_directory SET path = '/new' WHERE path = '/old'",
                &[],
            )
            .await
            .expect("transactional subtree root move should succeed");
        assert_eq!(moved.rows_affected(), 1);

        let old_path = transaction
            .execute(
                "SELECT id FROM lix_file WHERE path = '/old/sub/readme.md'",
                &[],
            )
            .await
            .expect("old subtree path lookup should succeed as a miss");
        assert_eq!(old_path.len(), 0);
        let moved_path = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/new/sub/readme.md'",
                &[],
            )
            .await
            .expect("moved descendant should use rebuilt transaction index");
        assert_rows_eq(
            moved_path,
            vec![vec![
                Value::Text("7472616e-7361-8374-896f-6e2d73756200".to_string()),
                Value::Text("/new/sub/readme.md".to_string()),
            ]],
        );

        let deleted = transaction
            .execute("DELETE FROM lix_directory WHERE path = '/new'", &[])
            .await
            .expect("transactional recursive subtree delete should succeed");
        assert_eq!(deleted.rows_affected(), 3);
        let after_delete = transaction
            .execute(
                "SELECT id FROM lix_file WHERE path = '/new/sub/readme.md'",
                &[],
            )
            .await
            .expect("deleted descendant path lookup should succeed as a miss");
        assert_eq!(after_delete.len(), 0);

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
        let restored = session
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/old/sub/readme.md'",
                &[],
            )
            .await
            .expect("rollback should restore the original subtree path");
        assert_rows_eq(
            restored,
            vec![vec![
                Value::Text("7472616e-7361-8374-896f-6e2d73756200".to_string()),
                Value::Text("/old/sub/readme.md".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_recursive_delete_removes_untracked_child_file,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_untracked) \
                 VALUES ('66696c65-2d64-8261-8674-000000000000', '/docs/draft.md', CAST('draft' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("untracked file insert should reuse the tracked parent directory");

        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs'", &[])
            .await
            .expect("recursive directory delete should succeed");
        assert_eq!(
            delete_result,
            ExecuteResult::from_rows_affected(2),
            "recursive delete of a tracked directory must also delete its untracked child file"
        );

        let files = session
            .execute("SELECT id, path FROM lix_file", &[])
            .await
            .expect("file read after recursive delete must not be poisoned by an orphan");
        assert_eq!(
            files.len(),
            0,
            "the untracked child file must not survive its parent directory"
        );

        let directories = session
            .execute("SELECT id, path FROM lix_directory", &[])
            .await
            .expect("directory read after recursive delete must not be poisoned by an orphan");
        assert_eq!(directories.len(), 0, "the directory must be deleted");
    }
);

simulation_test!(
    lix_directory_recursive_delete_removes_untracked_file_under_implicit_parent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        // `/docs` and `/docs/guides` are created implicitly by the tracked file
        // insert. Implicit intermediate directories are the common way a
        // tracked directory ends up above an untracked file.
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d74-8261-8b65-000000000000', '/docs/guides/tracked.md', CAST('tracked' AS BYTEA))",
                &[],
            )
            .await
            .expect("tracked nested file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_untracked) \
                 VALUES ('66696c65-2d75-8074-8261-000000000000', '/docs/guides/scratch.md', CAST('scratch' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("untracked nested file insert should succeed");

        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs'", &[])
            .await
            .expect("recursive directory delete should succeed");
        assert_eq!(
            delete_result,
            ExecuteResult::from_rows_affected(4),
            "delete must cover /docs, /docs/guides and both nested files"
        );

        let files = session
            .execute("SELECT id, path FROM lix_file", &[])
            .await
            .expect("file read after recursive delete must not be poisoned by an orphan");
        assert_eq!(files.len(), 0, "no nested file may survive its parent tree");

        let directories = session
            .execute("SELECT id, path FROM lix_directory", &[])
            .await
            .expect("directory read after recursive delete must not be poisoned by an orphan");
        assert_eq!(directories.len(), 0, "the whole tree must be deleted");
    }
);

simulation_test!(
    lix_directory_recursive_delete_removes_untracked_child_directory,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('6469722d-7375-8062-8000-000000000000', '/docs/sub', true)",
                &[],
            )
            .await
            .expect("untracked child directory insert should succeed");

        // Before the lane crossing this delete was rejected outright with a
        // LIX_ERROR_FOREIGN_KEY on the untracked child's /parent_id, leaving
        // the user no way to remove the tree.
        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs'", &[])
            .await
            .expect("recursive directory delete should reach the untracked child directory");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(2));

        let directories = session
            .execute("SELECT id, path FROM lix_directory", &[])
            .await
            .expect("directory read after recursive delete should succeed");
        assert_eq!(directories.len(), 0);
    }
);

simulation_test!(
    lix_directory_recursive_delete_survives_reopen_without_orphan,
    |sim| async move {
        let engine = sim.boot_engine().await;
        {
            let session = sim.wrap_session(
                engine
                    .open_session()
                    .await
                    .expect("main session should open"),
                &engine,
            );
            session
                .execute(
                    "INSERT INTO lix_directory (id, path) \
                     VALUES ('6469722d-646f-8373-8000-000000000000', '/docs')",
                    &[],
                )
                .await
                .expect("tracked directory insert should succeed");
            session
                .execute(
                    "INSERT INTO lix_file (id, path, content, lixcol_untracked) \
                     VALUES ('66696c65-2d64-8261-8674-000000000000', '/docs/draft.md', CAST('draft' AS BYTEA), true)",
                    &[],
                )
                .await
                .expect("untracked file insert should succeed");
            session
                .execute("DELETE FROM lix_directory WHERE path = '/docs'", &[])
                .await
                .expect("recursive directory delete should succeed");
        }

        // The orphan was durable: it survived a restart and left the branch
        // permanently unreadable. Assert the read surface is intact after one.
        let rebooted = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reboot from the current snapshot");
        let session = sim.wrap_session(
            rebooted
                .open_session()
                .await
                .expect("session should open after reboot"),
            &rebooted,
        );

        let files = session
            .execute("SELECT id, path FROM lix_file", &[])
            .await
            .expect("file read after reboot must not be poisoned by an orphan");
        assert_eq!(files.len(), 0);

        let directories = session
            .execute("SELECT id, path FROM lix_directory", &[])
            .await
            .expect("directory read after reboot must not be poisoned by an orphan");
        assert_eq!(directories.len(), 0);
    }
);
