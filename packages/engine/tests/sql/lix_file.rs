use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;
use lix_engine::{CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_file_update_can_compare_the_read_only_change_id,
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
                "INSERT INTO lix_file (id, path, data) VALUES ('guarded-file', '/guarded.txt', X'6265666F7265')",
                &[],
            )
            .await
            .expect("guarded file insert should succeed");
        let current = session
            .execute(
                "SELECT lixcol_change_id FROM lix_file WHERE id = 'guarded-file'",
                &[],
            )
            .await
            .expect("change id read should succeed");
        let change_id = match current.rows()[0]
            .value("lixcol_change_id")
            .expect("change id column")
        {
            Value::Text(value) => value.clone(),
            value => panic!("expected text change id, got {value:?}"),
        };

        let applied = session
            .execute(
                "UPDATE lix_file SET data = X'6166746572' WHERE path = '/guarded.txt' AND lixcol_change_id = $1",
                &[Value::Text(change_id)],
            )
            .await
            .expect("matching change id should be accepted in an update predicate");
        assert_eq!(applied.rows_affected(), 1);

        let stale = session
            .execute(
                "UPDATE lix_file SET data = X'7374616C65' WHERE path = '/guarded.txt' AND lixcol_change_id = 'stale'",
                &[],
            )
            .await
            .expect("stale change id should produce a zero-row update");
        assert_eq!(stale.rows_affected(), 0);

        let content = session
            .execute("SELECT data FROM lix_file WHERE id = 'guarded-file'", &[])
            .await
            .expect("guarded file read should succeed");
        assert_rows_eq(content, vec![vec![Value::Blob(b"after".to_vec().into())]]);
    }
);

simulation_test!(
    lix_file_read_allows_public_path_inside_scalar_function,
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
                "INSERT INTO lix_file (id, path) VALUES ('readme-file', '/Readme.md')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let result = session
            .execute(
                "SELECT id FROM lix_file WHERE lower(path) = '/readme.md'",
                &[],
            )
            .await
            .expect("path should behave as an opaque text column in predicates");

        assert_rows_eq(result, vec![vec![Value::Text("readme-file".to_string())]]);
    }
);

simulation_test!(
    lix_file_lower_path_like_keeps_the_blob_revision_for_guarded_updates,
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
                "INSERT INTO lix_file (id, path, data) VALUES ('guarded-search-file', '/Docs/Guarded-Readme.md', X'6265666F7265')",
                &[],
            )
            .await
            .expect("search fixture insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('other-search-file', '/Docs/other.md', X'6F74686572')",
                &[],
            )
            .await
            .expect("non-matching search fixture insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('unicode-search-file', '/Ä/Readme.md', X'756E69636F6465')",
                &[],
            )
            .await
            .expect("Unicode search fixture insert should succeed");

        let search = session
            .execute(
                "SELECT path, name, lixcol_metadata, lixcol_change_id, lixcol_updated_at \
                 FROM lix_file WHERE lower(path) LIKE $1 ORDER BY path",
                &[Value::Text("%guarded%".to_string())],
            )
            .await
            .expect("lower path search should succeed");
        assert_eq!(search.rows().len(), 1);
        assert_eq!(
            search.rows()[0].value("path").expect("path should exist"),
            &Value::Text("/Docs/Guarded-Readme.md".to_string())
        );
        assert_eq!(
            search.rows()[0].value("name").expect("name should exist"),
            &Value::Text("Guarded-Readme.md".to_string())
        );

        let mixed_case_pattern = session
            .execute(
                "SELECT path FROM lix_file WHERE lower(path) LIKE $1 ORDER BY path",
                &[Value::Text("%Guarded%".to_string())],
            )
            .await
            .expect("mixed-case lower path search should succeed");
        assert_eq!(
            mixed_case_pattern.len(),
            0,
            "LOWER(path) does not lower the LIKE pattern",
        );

        let change_id = match search.rows()[0]
            .value("lixcol_change_id")
            .expect("search result should include a revision")
        {
            Value::Text(value) => value.clone(),
            value => panic!("expected text change id, got {value:?}"),
        };

        let updated = session
            .execute(
                "UPDATE lix_file SET data = X'6166746572' WHERE path = '/Docs/Guarded-Readme.md' AND lixcol_change_id = $1",
                &[Value::Text(change_id)],
            )
            .await
            .expect("search revision should guard the data update");
        assert_eq!(updated.rows_affected(), 1);

        let content = session
            .execute(
                "SELECT data FROM lix_file WHERE id = 'guarded-search-file'",
                &[],
            )
            .await
            .expect("updated file should be readable");
        assert_rows_eq(content, vec![vec![Value::Blob(b"after".to_vec().into())]]);

        let unicode_search = session
            .execute(
                "SELECT path FROM lix_file WHERE lower(path) LIKE $1 ORDER BY path",
                &[Value::Text("%ä/readme%".to_string())],
            )
            .await
            .expect("non-ASCII lower path search should retain SQL semantics");
        assert_rows_eq(
            unicode_search,
            vec![vec![Value::Text("/Ä/Readme.md".to_string())]],
        );
    }
);

simulation_test!(
    lix_file_by_branch_read_rejects_dynamic_branch_id_operand,
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
                "SELECT id FROM lix_file_by_branch WHERE lixcol_branch_id = lower('main')",
                &[],
            )
            .await
            .expect_err("public branch id predicate should only accept literal/param operands");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("public column 'lixcol_branch_id'"));
    }
);

simulation_test!(
    lix_file_path_insert_preserves_long_opaque_segments,
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
                "INSERT INTO lix_file (id, path) VALUES ('file-long-segment', $1)",
                &[Value::Text(format!("/{long_segment}"))],
            )
            .await
            .expect("long opaque file path segment should be accepted");

        let long_path = format!("/{}", ["abcd"; 820].join("/"));
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-long-path', $1)",
                &[Value::Text(long_path.clone())],
            )
            .await
            .expect("long opaque file path should be accepted");

        let result = session
            .execute(
                "SELECT id, path FROM lix_file \
                 WHERE id IN ('file-long-segment', 'file-long-path') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("file-long-path".to_string()),
                    Value::Text(long_path),
                ],
                vec![
                    Value::Text("file-long-segment".to_string()),
                    Value::Text(format!("/{long_segment}")),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_writes_reject_plugin_storage_paths,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let insert_error = session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('plugin-poison', '/.lix/plugins/nested/plugin_sentinel.lixplugin', X'626164')",
                &[],
            )
            .await
            .expect_err("SQL insert should reject invalid plugin storage paths");
        assert_eq!(insert_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            insert_error
                .message
                .contains("reserved plugin storage path")
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('safe-file', '/safe.bin', X'6F6B')",
                &[],
            )
            .await
            .expect("safe file insert should succeed");

        let update_error = session
            .execute(
                "UPDATE lix_file \
                 SET path = '/.lix/plugins/plugin_sentinel.lixplugin' \
                 WHERE id = 'safe-file'",
                &[],
            )
            .await
            .expect_err("SQL update should reject plugin storage paths");
        assert_eq!(update_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            update_error.message.contains("plugin archive paths"),
            "unexpected error: {update_error:?}"
        );
    }
);

simulation_test!(
    lix_file_path_insert_preserves_percent_spelling,
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
            ("file-percent-a", "/docs/%61.txt"),
            ("file-percent-nul", "/docs/%00evil.txt"),
            ("file-percent-bidi", "/docs/%E2%80%AEevil.txt"),
        ] {
            session
                .execute(
                    &format!("INSERT INTO lix_file (id, path) VALUES ('{id}', '{path}')"),
                    &[],
                )
                .await
                .expect("percent spelling should be stored literally");
        }

        let result = session
            .execute(
                "SELECT id, path, name FROM lix_file \
                 WHERE id IN ('file-percent-a', 'file-percent-bidi', 'file-percent-nul') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("file-percent-a".to_string()),
                    Value::Text("/docs/%61.txt".to_string()),
                    Value::Text("%61.txt".to_string()),
                ],
                vec![
                    Value::Text("file-percent-bidi".to_string()),
                    Value::Text("/docs/%E2%80%AEevil.txt".to_string()),
                    Value::Text("%E2%80%AEevil.txt".to_string()),
                ],
                vec![
                    Value::Text("file-percent-nul".to_string()),
                    Value::Text("/docs/%00evil.txt".to_string()),
                    Value::Text("%00evil.txt".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_path_insert_preserves_opaque_file_name_segments,
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
            ("file-foo-dot", "/foo."),
            ("file-foo-dot-dot", "/foo.."),
            ("file-foo-dot-dot-dot", "/foo..."),
            ("file-archive", "/archive.tar.gz"),
            ("file-dotenv", "/.env"),
            ("file-hidden-in-docs", "/docs/.hidden"),
        ] {
            session
                .execute(
                    &format!("INSERT INTO lix_file (id, path) VALUES ('{id}', '{path}')"),
                    &[],
                )
                .await
                .expect("opaque file name insert should succeed");
        }

        let result = session
            .execute(
                "SELECT id, path, name \
                 FROM lix_file \
                 WHERE id IN (\
                   'file-foo-dot',\
                   'file-foo-dot-dot',\
                   'file-foo-dot-dot-dot',\
                   'file-archive',\
                   'file-dotenv',\
                   'file-hidden-in-docs'\
                 ) \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("file-archive".to_string()),
                    Value::Text("/archive.tar.gz".to_string()),
                    Value::Text("archive.tar.gz".to_string()),
                ],
                vec![
                    Value::Text("file-dotenv".to_string()),
                    Value::Text("/.env".to_string()),
                    Value::Text(".env".to_string()),
                ],
                vec![
                    Value::Text("file-foo-dot".to_string()),
                    Value::Text("/foo.".to_string()),
                    Value::Text("foo.".to_string()),
                ],
                vec![
                    Value::Text("file-foo-dot-dot".to_string()),
                    Value::Text("/foo..".to_string()),
                    Value::Text("foo..".to_string()),
                ],
                vec![
                    Value::Text("file-foo-dot-dot-dot".to_string()),
                    Value::Text("/foo...".to_string()),
                    Value::Text("foo...".to_string()),
                ],
                vec![
                    Value::Text("file-hidden-in-docs".to_string()),
                    Value::Text("/docs/.hidden".to_string()),
                    Value::Text(".hidden".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_descriptor_shape_insert_rejects_slash_in_name_at_renderer_boundary,
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
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('file-slash', NULL, 'nested/name')",
                &[],
            )
            .await
            .expect_err("file descriptor name must keep '/' as structural separator");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("path segment must not contain '/'"));
    }
);

simulation_test!(
    lix_file_descriptor_shape_insert_uses_name_as_full_basename,
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
                 VALUES ('file-descriptor-dot', NULL, 'foo.')",
                &[],
            )
            .await
            .expect("descriptor-shaped insert should accept full opaque basename");

        let result = session
            .execute(
                "SELECT id, path, name \
                 FROM lix_file \
                 WHERE id = 'file-descriptor-dot'",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("file-descriptor-dot".to_string()),
                Value::Text("/foo.".to_string()),
                Value::Text("foo.".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_extension_column_is_not_writable_identity,
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
                "INSERT INTO lix_file (id, directory_id, name, extension) \
                 VALUES ('file-extension-write', NULL, 'readme', 'md')",
                &[],
            )
            .await
            .expect_err("extension should not be accepted as writable file identity");
    }
);

simulation_test!(
    lix_file_namespace_treats_trailing_dot_names_as_distinct,
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
                "INSERT INTO lix_file (id, path) VALUES ('file-foo', '/foo')",
                &[],
            )
            .await
            .expect("plain file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-foo-dot', '/foo.')",
                &[],
            )
            .await
            .expect("trailing-dot file insert should be distinct from plain name");

        let result = session
            .execute(
                "SELECT id, path, name \
                 FROM lix_file \
                 WHERE id IN ('file-foo', 'file-foo-dot') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("file-foo".to_string()),
                    Value::Text("/foo".to_string()),
                    Value::Text("foo".to_string()),
                ],
                vec![
                    Value::Text("file-foo-dot".to_string()),
                    Value::Text("/foo.".to_string()),
                    Value::Text("foo.".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_insert_reads_path_data_and_parent_dirs,
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

        let result = session
            .execute(
                "SELECT id, path, data, lixcol_schema_key \
             FROM lix_file \
             WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("file-readme".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Text("lix_file_descriptor".to_string()),
            ]
        );

        let staged_state_result = session
            .execute(
                "SELECT entity_pk, schema_key \
             FROM lix_state \
             WHERE entity_pk = lix_json('[\"file-readme\"]') \
             ORDER BY schema_key, entity_pk",
                &[],
            )
            .await
            .expect("filesystem state read should succeed");
        let staged_state_rows = staged_state_result;
        assert_eq!(
            staged_state_rows.len(),
            2,
            "file path insert should stage one file descriptor and one blob ref for the file"
        );

        let directory_result = session
            .execute(
                "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("directory read after file insert should succeed");
        let directory_rows = directory_result;
        assert_eq!(
            directory_rows.len(),
            2,
            "file path insert should stage exactly the two missing parent directories"
        );
    }
);

simulation_test!(lix_file_insert_applies_defaulted_id, |sim| async move {
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
             VALUES ('dir-docs', NULL, 'docs')",
            &[],
        )
        .await
        .expect("directory insert should succeed");

    let insert_result = session
        .execute(
            "INSERT INTO lix_file (directory_id, name) \
             VALUES ('dir-docs', 'readme.md')",
            &[],
        )
        .await
        .expect("file insert should apply defaulted id");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT id, path, directory_id, name \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
            &[],
        )
        .await
        .expect("file read should succeed");
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let values = row_set.rows()[0].values();
    let [
        Value::Text(id),
        Value::Text(path),
        Value::Text(directory_id),
        Value::Text(name),
    ] = values
    else {
        panic!("expected generated file row, got {values:?}");
    };
    assert!(!id.is_empty(), "defaulted file id should be non-empty");
    assert_eq!(path, "/docs/readme.md");
    assert_eq!(directory_id, "dir-docs");
    assert_eq!(name, "readme.md");
});

simulation_test!(
    lix_file_path_insert_applies_defaulted_id_and_empty_data,
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
                "INSERT INTO lix_file (path) VALUES ('/docs/readme.md')",
                &[],
            )
            .await
            .expect("file path insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, name, data \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [
            Value::Text(id),
            Value::Text(path),
            Value::Text(name),
            Value::Blob(data),
        ] = values
        else {
            panic!("expected generated file path row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted file id should be non-empty");
        assert_eq!(path, "/docs/readme.md");
        assert_eq!(name, "readme.md");
        assert_eq!(data.as_ref(), b"");

        let null_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/docs/readme.md' AND data IS NULL",
                &[],
            )
            .await
            .expect("file null predicate should succeed");
        assert_eq!(null_result.len(), 0);
    }
);

simulation_test!(
    lix_file_path_data_insert_applies_defaulted_id,
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
                "INSERT INTO lix_file (path, data) VALUES ('/docs/readme.md', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file path data insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, data \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Blob(data)] = values else {
            panic!("expected generated file data row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted file id should be non-empty");
        assert_eq!(path, "/docs/readme.md");
        assert_eq!(data.as_ref(), b"hello");
    }
);

simulation_test!(lix_file_data_is_not_nullable, |sim| async move {
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
            "SELECT is_nullable \
             FROM information_schema.columns \
             WHERE table_name = 'lix_file' \
               AND column_name = 'data'",
            &[],
        )
        .await
        .expect("information schema read should succeed");

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Text("NO".to_string())]);
});

simulation_test!(lix_file_insert_rejects_null_data, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('null-data-file', '/null.bin', NULL)",
            &[],
        )
        .await
        .expect_err("explicit NULL data should be rejected");

    assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

    let parameter_error = session
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('null-param-data-file', '/null-param.bin', $1)",
            &[Value::Null],
        )
        .await
        .expect_err("parameterized NULL data should be rejected");

    assert_eq!(parameter_error.code, LixError::CODE_TYPE_MISMATCH);

    let result = session
        .execute(
            "SELECT id FROM lix_file \
             WHERE id IN ('null-data-file', 'null-param-data-file')",
            &[],
        )
        .await
        .expect("file read should succeed");
    assert_eq!(result.len(), 0);
});

simulation_test!(
    lix_file_insert_rejects_non_binary_data_literals,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, sql) in [
            (
                "text-data-file",
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('text-data-file', '/text.bin', 'hello')",
            ),
            (
                "int-data-file",
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('int-data-file', '/int.bin', 12345)",
            ),
            (
                "float-data-file",
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('float-data-file', '/float.bin', 1.5)",
            ),
            (
                "bool-data-file",
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('bool-data-file', '/bool.bin', true)",
            ),
            (
                "text-function-data-file",
                "INSERT INTO lix_file (id, path, data) \
                 VALUES (\
                   'text-function-data-file',\
                   '/text-function.bin',\
                   lix_json_get_text(lix_json('{\"value\":\"hello\"}'), 'value')\
                 )",
            ),
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("non-binary data literal should be rejected");

            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }

        let result = session
            .execute(
                "SELECT id FROM lix_file \
                 WHERE id IN (\
                   'text-data-file',\
                   'text-function-data-file',\
                   'int-data-file',\
                   'float-data-file',\
                   'bool-data-file'\
                 )",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_file_insert_rejects_non_binary_data_from_select,
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
                "INSERT INTO lix_file (id, path, data) \
                 SELECT 'select-text-data-file', '/select-text.bin', 'hello'",
                &[],
            )
            .await
            .expect_err("non-binary data from SELECT should be rejected");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let result = session
            .execute(
                "SELECT id FROM lix_file WHERE id = 'select-text-data-file'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_file_insert_rejects_non_binary_data_parameters,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, value) in [
            ("text-param-data-file", Value::Text("hello".to_string())),
            ("int-param-data-file", Value::Integer(12345)),
        ] {
            let error = session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('{id}', '/{id}.bin', $1)"
                    ),
                    &[value],
                )
                .await
                .expect_err("non-binary data parameter should be rejected");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }
    }
);

simulation_test!(
    lix_file_accepts_explicit_text_to_binary_casts,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('cast-binary-file', $1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/cast-binary.txt".to_string()),
                    Value::Text("inserted".to_string()),
                ],
            )
            .await
            .expect("explicit binary cast insert should succeed");

        session
            .execute(
                "UPDATE lix_file SET data = CAST($1 AS BYTEA) \
                 WHERE id = 'cast-binary-file'",
                &[Value::Text("updated".to_string())],
            )
            .await
            .expect("explicit binary cast update should succeed");

        let result = session
            .execute(
                "SELECT data FROM lix_file WHERE id = 'cast-binary-file'",
                &[],
            )
            .await
            .expect("cast file read should succeed");
        assert_rows_eq(result, vec![vec![Value::Blob(b"updated".to_vec().into())]]);
    }
);

simulation_test!(
    lix_file_insert_accepts_anonymous_path_and_data_parameters,
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
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[
                    Value::Text("anonymous-param-file".to_string()),
                    Value::Text("/anonymous-param.bin".to_string()),
                    Value::Blob(b"anonymous".to_vec().into()),
                ],
            )
            .await
            .expect("anonymous parameter insert should succeed");
        assert_eq!(insert_result.rows_affected(), 1);

        let result = session
            .execute(
                "SELECT path, data FROM lix_file WHERE id = ?",
                &[Value::Text("anonymous-param-file".to_string())],
            )
            .await
            .expect("anonymous parameter read should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/anonymous-param.bin".to_string()),
                Value::Blob(b"anonymous".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_anonymous_data_parameter_keeps_strict_blob_validation,
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
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[
                    Value::Text("anonymous-text-data-file".to_string()),
                    Value::Text("/anonymous-text-data.bin".to_string()),
                    Value::Text("not binary".to_string()),
                ],
            )
            .await
            .expect_err("anonymous non-binary data parameter should be rejected");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(lix_file_insert_accepts_empty_blob_data, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('empty-data-file', '/empty.bin', X'')",
            &[],
        )
        .await
        .expect("empty blob data should be accepted");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE id = 'empty-data-file'",
            &[],
        )
        .await
        .expect("file read should succeed");
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new().into())]);

    let blob_ref_result = session
        .execute(
            "SELECT entity_pk \
             FROM lix_state \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND entity_pk = lix_json('[\"empty-data-file\"]')",
            &[],
        )
        .await
        .expect("blob ref state read should succeed");
    assert_eq!(blob_ref_result.len(), 0);
});

simulation_test!(
    lix_file_path_insert_rejects_duplicate_root_path,
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
                "INSERT INTO lix_file (path, data) VALUES ('/x.bin', $1)",
                &[Value::Blob(vec![1].into())],
            )
            .await
            .expect("first file path insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/x.bin', $1)",
                &[Value::Blob(vec![2].into())],
            )
            .await
            .expect_err("duplicate file path insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_file_insert_duplicate_id_with_data_reports_lix_file,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('same-file', '/a.bin', X'01')",
                &[],
            )
            .await
            .expect("first file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('same-file', '/b.bin', X'02')",
                &[],
            )
            .await
            .expect_err("duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error.message.contains("id 'same-file'")
                && !error.message.contains("lix_binary_blob_ref"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_insert_duplicate_id_without_data_reports_lix_file,
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
                "INSERT INTO lix_file (id, path) VALUES ('same-file', '/a.bin')",
                &[],
            )
            .await
            .expect("first file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('same-file', '/b.bin')",
                &[],
            )
            .await
            .expect_err("duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error.message.contains("id 'same-file'")
                && !error.message.contains("lix_file_descriptor"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_insert_duplicate_id_in_same_batch_reports_lix_file,
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
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('same-file', '/a.bin', X'01'), \
                 ('same-file', '/b.bin', X'02')",
                &[],
            )
            .await
            .expect_err("same-batch duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error.message.contains("id 'same-file'")
                && !error.message.contains("lix_binary_blob_ref"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_by_branch_insert_duplicate_id_reports_lix_file_by_branch,
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
                    "INSERT INTO lix_file_by_branch \
                     (id, path, data, lixcol_branch_id) \
                     VALUES ('same-file', '/a.bin', X'01', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("first by-branch file insert should succeed");

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (id, path, data, lixcol_branch_id) \
                     VALUES ('same-file', '/b.bin', X'02', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect_err("duplicate by-branch file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file_by_branch'")
                && error.message.contains("id 'same-file'")
                && !error.message.contains("table 'lix_file':")
                && !error.message.contains("lix_binary_blob_ref"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_path_insert_rejects_existing_directory_entry,
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
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo/')", &[])
            .await
            .expect("directory insert should succeed");

        let error = session
            .execute("INSERT INTO lix_file (path) VALUES ('/foo')", &[])
            .await
            .expect_err("file should conflict with directory at same entry name");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("filesystem namespace conflict"),
            "expected namespace conflict error: {error}"
        );
    }
);

simulation_test!(
    lix_file_path_insert_allows_extension_distinct_from_directory,
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
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo/')", &[])
            .await
            .expect("directory insert should succeed");
        session
            .execute("INSERT INTO lix_file (path) VALUES ('/foo.txt')", &[])
            .await
            .expect("file basename foo.txt should not conflict with directory foo");

        let file_result = session
            .execute("SELECT path FROM lix_file WHERE path = '/foo.txt'", &[])
            .await
            .expect("file path should query");
        let directory_result = session
            .execute("SELECT path FROM lix_directory WHERE path = '/foo/'", &[])
            .await
            .expect("directory path should query");

        assert_eq!(file_result.len(), 1);
        assert_eq!(directory_result.len(), 1);
    }
);

simulation_test!(
    lix_file_path_insert_rejects_file_as_implicit_ancestor,
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
            .execute("INSERT INTO lix_file (path) VALUES ('/foo/bar.txt')", &[])
            .await
            .expect_err("implicit ancestor directory should conflict with existing file");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_file_descriptor_shape_insert_rejects_existing_directory_entry,
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
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('dir-foo', NULL, 'foo')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('file-foo', NULL, 'foo')",
                &[],
            )
            .await
            .expect_err("descriptor-shaped file insert should conflict with directory");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_file_update_rejects_existing_directory_entry,
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
                "INSERT INTO lix_file (id, path) VALUES ('file-foo', '/foo')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/bar/')", &[])
            .await
            .expect("directory insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET path = '/bar' WHERE id = 'file-foo'",
                &[],
            )
            .await
            .expect_err("file path update should conflict with directory");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_file_insert_rejects_missing_directory_id,
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
                "INSERT INTO lix_file (directory_id, name) \
                 VALUES ('missing-dir', 'readme.md')",
                &[],
            )
            .await
            .expect_err("file insert should reject missing directory_id");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }
);

simulation_test!(
    lix_file_update_rejects_missing_directory_id_and_preserves_path,
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
            .expect("directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('file-readme', 'dir-docs', 'readme.md')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET directory_id = 'missing-dir' WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect_err("file update should reject missing directory_id");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);

        let result = session
            .execute(
                "SELECT path, directory_id FROM lix_file WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Text("/docs/readme.md".to_string()),
                Value::Text("dir-docs".to_string())
            ]
        );
    }
);

simulation_test!(
    lix_file_path_insert_rejects_dot_segments,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for path in ["/a/../b/c.txt", "/a/./b/c.txt"] {
            let error = session
                .execute(
                    "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                    &[
                        Value::Text(path.to_string()),
                        Value::Blob(Vec::new().into()),
                    ],
                )
                .await
                .expect_err("file path insert should reject dot segments");

            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
            assert!(error.message.contains("path segment cannot be '.' or '..'"));
        }

        let result = session
            .execute("SELECT path FROM lix_file WHERE path = '/b/c.txt'", &[])
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_file_data_insert_applies_defaulted_id,
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
             VALUES ('dir-docs', NULL, 'docs')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO lix_file (directory_id, name, data) \
             VALUES ('dir-docs', 'readme.md', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file data insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, data \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Blob(data)] = values else {
            panic!("expected generated file data row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted file id should be non-empty");
        assert_eq!(path, "/docs/readme.md");
        assert_eq!(data.as_ref(), b"hello");
    }
);

simulation_test!(lix_file_path_update_preserves_data, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F')",
            &[],
        )
        .await
        .expect("file insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let update_result = session
        .execute(
            "UPDATE lix_file \
             SET path = '/docs/readme-renamed.md' \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("file path update should succeed");
    assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

    let file_result = session
        .execute(
            "SELECT id, path, data \
             FROM lix_file \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("file read after path update should succeed");
    let file_rows = file_result;
    assert_eq!(file_rows.len(), 1);
    assert_eq!(
        file_rows.rows()[0].values(),
        &[
            Value::Text("file-readme".to_string()),
            Value::Text("/docs/readme-renamed.md".to_string()),
            Value::Blob(b"hello".to_vec().into()),
        ]
    );

    let state_result = session
        .execute(
            "SELECT entity_pk, schema_key \
             FROM lix_state \
             WHERE entity_pk = lix_json('[\"file-readme\"]') \
             ORDER BY schema_key, entity_pk",
            &[],
        )
        .await
        .expect("filesystem state read after path update should succeed");
    let state_rows = state_result;
    assert_eq!(
        state_rows.len(),
        2,
        "path update should update one file descriptor and preserve one blob ref"
    );

    let directory_result = session
        .execute(
            "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
            &[],
        )
        .await
        .expect("directory read after path update should succeed");
    let directory_rows = directory_result;
    assert_eq!(
        directory_rows.len(),
        2,
        "path update should not stage an extra directory descriptor"
    );
});

simulation_test!(
    lix_file_path_update_by_path_uses_fresh_index,
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
                "INSERT INTO lix_file (id, path, data) VALUES \
             ('file-target', '/docs/target.md', X'746172676574'), \
             ('file-other', '/docs/other.md', X'6F74686572')",
                &[],
            )
            .await
            .expect("file fixtures should insert");

        let warm = session
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("exact path lookup should warm the filesystem index");
        assert_rows_eq(
            warm,
            vec![vec![
                Value::Text("file-target".to_string()),
                Value::Text("/docs/target.md".to_string()),
            ]],
        );

        let update = session
            .execute(
                "UPDATE lix_file \
             SET path = '/archive/renamed.md' \
             WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("path-filtered file rename should succeed");
        assert_eq!(update, ExecuteResult::from_rows_affected(1));

        let old = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("old path lookup should succeed as a miss");
        assert_eq!(old.len(), 0);

        let renamed = session
            .execute(
                "SELECT id, path, data FROM lix_file WHERE path = '/archive/renamed.md'",
                &[],
            )
            .await
            .expect("renamed path should be visible through a fresh index");
        assert_rows_eq(
            renamed,
            vec![vec![
                Value::Text("file-target".to_string()),
                Value::Text("/archive/renamed.md".to_string()),
                Value::Blob(b"target".to_vec().into()),
            ]],
        );

        let unrelated = session
            .execute(
                "SELECT path, data FROM lix_file WHERE id = 'file-other'",
                &[],
            )
            .await
            .expect("unrelated file should remain unchanged");
        assert_rows_eq(
            unrelated,
            vec![vec![
                Value::Text("/docs/other.md".to_string()),
                Value::Blob(b"other".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_path_index_invalidates_when_branch_head_moves,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('branch-head-file', '/branch-head.txt', X'68656164')",
                &[],
            )
            .await
            .expect("file fixture should insert");
        let current_head = session
            .execute(
                &format!(
                    "SELECT commit_id FROM lix_branch WHERE id = '{}'",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("current branch head should query")
            .rows()[0]
            .values()[0]
            .clone();

        let warm = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/branch-head.txt'",
                &[],
            )
            .await
            .expect("exact path lookup should warm the filesystem index");
        assert_rows_eq(
            warm,
            vec![vec![Value::Text("branch-head-file".to_string())]],
        );

        let reset = session
            .execute(
                &format!(
                    "UPDATE lix_branch SET commit_id = '{}' WHERE id = '{}'",
                    sim.initial_commit_id(),
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("branch head should reset to the initial commit");
        assert_eq!(reset, ExecuteResult::from_rows_affected(1));

        let old_head_lookup = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/branch-head.txt'",
                &[],
            )
            .await
            .expect("path lookup after branch reset should succeed");
        assert_eq!(old_head_lookup.len(), 0);

        let current_head = match current_head {
            Value::Text(commit_id) => commit_id,
            other => panic!("expected text commit id, got {other:?}"),
        };
        let restore = session
            .execute(
                &format!(
                    "UPDATE lix_branch SET commit_id = '{current_head}' WHERE id = '{}'",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect("branch head should restore to the file commit");
        assert_eq!(restore, ExecuteResult::from_rows_affected(1));

        let restored_lookup = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/branch-head.txt'",
                &[],
            )
            .await
            .expect("path lookup after branch restore should succeed");
        assert_rows_eq(
            restored_lookup,
            vec![vec![Value::Text("branch-head-file".to_string())]],
        );
    }
);

simulation_test!(
    atelier_current_path_range_and_order_workloads,
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
                "INSERT INTO lix_file (id, path, data) VALUES \
             ('extension-a', '/.lix/app_data/atelier/extensions/demo/a.js', X'61'), \
             ('extension-b', '/.lix/app_data/atelier/extensions/demo/b.js', X'62'), \
             ('extension-upper', '/.lix/app_data/atelier/extensions0/out.js', X'78'), \
             ('unrelated', '/docs/readme.md', X'72')",
                &[],
            )
            .await
            .expect("Atelier path fixtures should insert");

        let range = session
            .execute(
                "SELECT path, data FROM lix_file \
             WHERE path >= '/.lix/app_data/atelier/extensions/' \
               AND path < '/.lix/app_data/atelier/extensions0' \
             ORDER BY path",
                &[],
            )
            .await
            .expect("Atelier extension prefix range should query");
        assert_rows_eq(
            range,
            vec![
                vec![
                    Value::Text("/.lix/app_data/atelier/extensions/demo/a.js".to_string()),
                    Value::Blob(b"a".to_vec().into()),
                ],
                vec![
                    Value::Text("/.lix/app_data/atelier/extensions/demo/b.js".to_string()),
                    Value::Blob(b"b".to_vec().into()),
                ],
            ],
        );

        let listing = session
            .execute(
                "SELECT path, kind FROM (\
               SELECT path, 'directory' AS kind FROM lix_directory \
               UNION ALL \
               SELECT path, 'file' AS kind FROM lix_file\
             ) ORDER BY path",
                &[],
            )
            .await
            .expect("Atelier ordered filesystem listing should query");
        let paths = listing
            .rows()
            .iter()
            .map(|row| match &row.values()[0] {
                Value::Text(path) => path.clone(),
                other => panic!("expected text path, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(paths.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(paths.contains(&"/docs/readme.md".to_string()));
        assert!(paths.contains(&"/.lix/app_data/atelier/extensions0/out.js".to_string()));

        let explain = session
            .execute(
                "EXPLAIN SELECT path, kind FROM (\
                   SELECT path, 'directory' AS kind FROM lix_directory \
                   UNION ALL \
                   SELECT path, 'file' AS kind FROM lix_file\
                 ) ORDER BY path",
                &[],
            )
            .await
            .expect("Atelier ordered listing should explain");
        let plan = explain
            .rows()
            .iter()
            .flat_map(|row| row.values().iter())
            .map(|value| match value {
                Value::Text(value) => value.clone(),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !plan.contains("SortExec"),
            "path-ordered providers should avoid a physical sort:\n{plan}"
        );
        assert!(
            plan.contains("SortPreservingMergeExec"),
            "the file/directory union should merge ordered inputs:\n{plan}"
        );
    }
);

simulation_test!(
    lix_file_update_rejects_null_data_and_preserves_existing_data,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('update-null-file', '/update-null.bin', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET data = NULL WHERE id = 'update-null-file'",
                &[],
            )
            .await
            .expect_err("explicit NULL data update should be rejected");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let parameter_error = session
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = 'update-null-file'",
                &[Value::Null],
            )
            .await
            .expect_err("parameterized NULL data update should be rejected");

        assert_eq!(parameter_error.code, LixError::CODE_TYPE_MISMATCH);

        let result = session
            .execute(
                "SELECT data FROM lix_file WHERE id = 'update-null-file'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Blob(b"hello".to_vec().into())]
        );
    }
);

simulation_test!(
    lix_file_update_rejects_non_binary_data_literals_and_preserves_existing_data,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, assignment) in [
            ("update-text-file", "'hello'"),
            (
                "update-text-function-file",
                "lix_json_get_text(lix_json('{\"value\":\"hello\"}'), 'value')",
            ),
            ("update-int-file", "12345"),
            ("update-float-file", "1.5"),
            ("update-bool-file", "true"),
        ] {
            session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('{id}', '/{id}.bin', X'68656C6C6F')"
                    ),
                    &[],
                )
                .await
                .expect("file insert should succeed");

            let error = session
                .execute(
                    &format!("UPDATE lix_file SET data = {assignment} WHERE id = '{id}'"),
                    &[],
                )
                .await
                .expect_err("non-binary data literal update should be rejected");

            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }

        let result = session
            .execute(
                "SELECT id, data FROM lix_file \
                 WHERE id IN (\
                   'update-text-file',\
                   'update-text-function-file',\
                   'update-int-file',\
                   'update-float-file',\
                   'update-bool-file'\
                 ) \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("update-bool-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("update-float-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("update-int-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("update-text-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("update-text-function-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_update_rejects_non_binary_data_parameters_and_preserves_existing_data,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, value) in [
            ("update-text-param-file", Value::Text("hello".to_string())),
            ("update-int-param-file", Value::Integer(12345)),
        ] {
            session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, data) \
                         VALUES ('{id}', '/{id}.bin', X'68656C6C6F')"
                    ),
                    &[],
                )
                .await
                .expect("file insert should succeed");

            let error = session
                .execute(
                    &format!("UPDATE lix_file SET data = $1 WHERE id = '{id}'"),
                    &[value],
                )
                .await
                .expect_err("non-binary data parameter update should be rejected");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }

        let result = session
            .execute(
                "SELECT id, data FROM lix_file \
                 WHERE id IN ('update-text-param-file', 'update-int-param-file') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("update-int-param-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("update-text-param-file".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
            ],
        );
    }
);

simulation_test!(lix_file_update_accepts_empty_blob_data, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('empty-update-file', '/empty-update.bin', X'68656C6C6F')",
            &[],
        )
        .await
        .expect("file insert should succeed");

    let update_result = session
        .execute(
            "UPDATE lix_file SET data = X'' WHERE id = 'empty-update-file'",
            &[],
        )
        .await
        .expect("empty blob data update should be accepted");
    assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE id = 'empty-update-file'",
            &[],
        )
        .await
        .expect("file read should succeed");
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new().into())]);

    let blob_ref_result = session
        .execute(
            "SELECT entity_pk \
             FROM lix_state \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND entity_pk = lix_json('[\"empty-update-file\"]')",
            &[],
        )
        .await
        .expect("blob ref state read should succeed");
    assert_eq!(blob_ref_result.len(), 0);
});

simulation_test!(
    lix_file_equal_normalized_metadata_skips_descriptor_history,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let upsert = "INSERT INTO lix_file (path, data, lixcol_metadata) \
                      VALUES ($1, $2, $3) \
                      ON CONFLICT (path) DO UPDATE SET \
                        data = excluded.data, \
                        lixcol_metadata = excluded.lixcol_metadata";
        let metadata = json!({"a": 1, "z": 2});
        session
            .execute(
                upsert,
                &[
                    Value::Text("/equal-metadata.bin".to_string()),
                    Value::Blob(b"before".to_vec().into()),
                    Value::Json(metadata.clone()),
                ],
            )
            .await
            .expect("initial file upsert should succeed");
        let file = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/equal-metadata.bin'",
                &[],
            )
            .await
            .expect("file id should load");
        let [Value::Text(file_id)] = file.rows()[0].values() else {
            panic!("expected file id");
        };

        session
            .execute(
                upsert,
                &[
                    Value::Text("/equal-metadata.bin".to_string()),
                    Value::Blob(b"after".to_vec().into()),
                    Value::Json(metadata.clone()),
                ],
            )
            .await
            .expect("equal-metadata overwrite should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        let current = session
            .execute(
                "SELECT data, lixcol_metadata \
                 FROM lix_file WHERE path = '/equal-metadata.bin'",
                &[],
            )
            .await
            .expect("updated file should load");
        assert_eq!(
            current.rows()[0].values(),
            &[Value::Blob(b"after".to_vec().into()), Value::Json(metadata),]
        );
        assert_eq!(
            file_descriptor_event_count(&session, &commit_id, file_id).await,
            0
        );
    }
);

simulation_test!(
    lix_file_changed_and_null_metadata_keep_descriptor_history_exact,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let upsert = "INSERT INTO lix_file (path, data, lixcol_metadata) \
                      VALUES ($1, $2, $3) \
                      ON CONFLICT (path) DO UPDATE SET \
                        data = excluded.data, \
                        lixcol_metadata = excluded.lixcol_metadata";
        session
            .execute(
                upsert,
                &[
                    Value::Text("/changed-metadata.bin".to_string()),
                    Value::Blob(b"one".to_vec().into()),
                    Value::Json(json!({"version": 1})),
                ],
            )
            .await
            .expect("initial file upsert should succeed");
        let file = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/changed-metadata.bin'",
                &[],
            )
            .await
            .expect("file id should load");
        let [Value::Text(file_id)] = file.rows()[0].values() else {
            panic!("expected file id");
        };

        session
            .execute(
                upsert,
                &[
                    Value::Text("/changed-metadata.bin".to_string()),
                    Value::Blob(b"two".to_vec().into()),
                    Value::Json(json!({"version": 2})),
                ],
            )
            .await
            .expect("changed-metadata overwrite should succeed");
        let changed_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");
        assert_eq!(
            file_descriptor_event_count(&session, &changed_commit_id, file_id).await,
            1
        );

        session
            .execute(
                upsert,
                &[
                    Value::Text("/changed-metadata.bin".to_string()),
                    Value::Blob(b"three".to_vec().into()),
                    Value::Null,
                ],
            )
            .await
            .expect("metadata removal should succeed");
        let removed_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");
        assert_eq!(
            file_descriptor_event_count(&session, &removed_commit_id, file_id).await,
            1
        );

        session
            .execute(
                upsert,
                &[
                    Value::Text("/changed-metadata.bin".to_string()),
                    Value::Blob(b"four".to_vec().into()),
                    Value::Null,
                ],
            )
            .await
            .expect("equal null metadata overwrite should succeed");
        let equal_null_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");
        assert_eq!(
            file_descriptor_event_count(&session, &equal_null_commit_id, file_id).await,
            0
        );
        let current = session
            .execute(
                "SELECT data, lixcol_metadata \
                 FROM lix_file WHERE path = '/changed-metadata.bin'",
                &[],
            )
            .await
            .expect("null-metadata file should load");
        assert_eq!(
            current.rows()[0].values(),
            &[Value::Blob(b"four".to_vec().into()), Value::Null]
        );
    }
);

async fn file_descriptor_event_count(
    session: &crate::support::simulation_test::engine::SimSession,
    commit_id: &str,
    file_id: &str,
) -> usize {
    session
        .execute(
            &format!(
                "SELECT lixcol_entity_pk FROM lix_state_history \
                 WHERE lixcol_as_of_commit_id = '{commit_id}' \
                   AND lixcol_depth = 0 \
                   AND lixcol_schema_key = 'lix_file_descriptor' \
                   AND lixcol_entity_pk = lix_json('[\"{file_id}\"]')"
            ),
            &[],
        )
        .await
        .expect("descriptor history should load")
        .len()
}

simulation_test!(
    lix_file_update_empty_data_on_empty_file_does_not_stage_blob_ref_tombstone,
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
                "INSERT INTO lix_file (id, path) \
                 VALUES ('already-empty-file', '/already-empty.bin')",
                &[],
            )
            .await
            .expect("path-only file insert should succeed");

        session
            .execute(
                "UPDATE lix_file SET data = X'' WHERE id = 'already-empty-file'",
                &[],
            )
            .await
            .expect("empty data update should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        let blob_ref_history = session
            .execute(
                &format!(
                    "SELECT lixcol_entity_pk \
                     FROM lix_state_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND lixcol_schema_key = 'lix_binary_blob_ref' \
                       AND lixcol_entity_pk = lix_json('[\"already-empty-file\"]')"
                ),
                &[],
            )
            .await
            .expect("blob ref history read should succeed");
        assert_eq!(blob_ref_history.len(), 0);
    }
);

simulation_test!(lix_file_by_branch_expands_global_rows, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data, lixcol_global, lixcol_untracked) \
             VALUES ('file-global-overlay', '/global.txt', X'67', true, false)",
            &[],
        )
        .await
        .expect("global file insert should succeed");

    let result = session
        .execute(
            "SELECT id, path, lixcol_branch_id, lixcol_global, lixcol_untracked \
             FROM lix_file_by_branch \
             WHERE id = 'file-global-overlay' \
             ORDER BY lixcol_branch_id",
            &[],
        )
        .await
        .expect("file by-branch read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("file-global-overlay".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text(sim.main_branch_id().to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
            vec![
                Value::Text("file-global-overlay".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
        ],
    );
});

simulation_test!(
    lix_file_global_path_insert_reuses_existing_global_directory,
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
                 VALUES ('global-shared-dir-for-file', '/shared/', true)",
                &[],
            )
            .await
            .expect("global directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_global) \
                 VALUES ('global-shared-file', '/shared/a.txt', CAST('a' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("global file insert should reuse existing global parent directory");

        let result = session
            .execute(
                "SELECT path FROM lix_file WHERE id = 'global-shared-file'",
                &[],
            )
            .await
            .expect("global file should read through active overlay");
        assert_rows_eq(result, vec![vec![Value::Text("/shared/a.txt".to_string())]]);
    }
);

simulation_test!(
    lix_file_tracked_path_insert_promotes_untracked_parent_directory,
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
                 VALUES ('dir-scratch', '/scratch/', true)",
                &[],
            )
            .await
            .expect("untracked parent insert should succeed");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-readme', '/scratch/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("tracked file insert should promote untracked parent");

        let directories = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/scratch/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            directories,
            vec![vec![
                Value::Text("dir-scratch".to_string()),
                Value::Text("/scratch/".to_string()),
                Value::Boolean(false),
            ]],
        );

        let files = session
            .execute(
                "SELECT id, path, directory_id, data \
                 FROM lix_file \
                 WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            files,
            vec![vec![
                Value::Text("file-readme".to_string()),
                Value::Text("/scratch/readme.md".to_string()),
                Value::Text("dir-scratch".to_string()),
                Value::Blob(b"hello".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_untracked_path_insert_reuses_tracked_parent_directory,
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
                "INSERT INTO lix_file (id, path, data, lixcol_untracked) \
                 VALUES ('file-draft', '/docs/draft.md', CAST('draft' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("untracked file insert should reuse tracked parent");

        let directories = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            directories,
            vec![vec![
                Value::Text("dir-docs".to_string()),
                Value::Text("/docs/".to_string()),
                Value::Boolean(false),
            ]],
        );

        let files = session
            .execute(
                "SELECT id, path, directory_id, lixcol_untracked \
                 FROM lix_file \
                 WHERE id = 'file-draft'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            files,
            vec![vec![
                Value::Text("file-draft".to_string()),
                Value::Text("/docs/draft.md".to_string()),
                Value::Text("dir-docs".to_string()),
                Value::Boolean(true),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_do_update_replaces_data,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-upsert', '/docs/upsert.md', X'6F6C64')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-upsert', '/docs/upsert.md', X'6E6577') \
                 ON CONFLICT (id) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, data FROM lix_file WHERE id = 'file-upsert'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-upsert".to_string()),
                Value::Text("/docs/upsert.md".to_string()),
                Value::Blob(b"new".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_do_nothing_keeps_existing,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-nothing', '/docs/nothing.md', X'6B656570')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-nothing', '/docs/nothing.md', X'6967') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id, path, data FROM lix_file WHERE id = 'file-nothing'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-nothing".to_string()),
                Value::Text("/docs/nothing.md".to_string()),
                Value::Blob(b"keep".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_inserts_when_absent,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-fresh', '/docs/fresh.md', X'6E6577') \
                 ON CONFLICT (id) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("upsert on absent id should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, data FROM lix_file WHERE id = 'file-fresh'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-fresh".to_string()),
                Value::Text("/docs/fresh.md".to_string()),
                Value::Blob(b"new".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_path_inserts_when_absent,
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
                "INSERT INTO lix_file (path, data) \
                 VALUES ('/docs/path-fresh.md', X'6E6577') \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("path upsert on absent file should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT path, data FROM lix_file WHERE path = '/docs/path-fresh.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("/docs/path-fresh.md".to_string()),
                Value::Blob(b"new".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_path_updates_existing_data_and_preserves_id,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-path-upsert', '/docs/path-upsert.md', X'6F6C64')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (path, data) \
                 VALUES ('/docs/path-upsert.md', X'6E6577') \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("path upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, data FROM lix_file WHERE path = '/docs/path-upsert.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-path-upsert".to_string()),
                Value::Text("/docs/path-upsert.md".to_string()),
                Value::Blob(b"new".to_vec().into()),
            ]],
        );

        let files = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/docs/path-upsert.md'",
                &[],
            )
            .await
            .expect("file count read should succeed");
        assert_eq!(files.len(), 1);

        let blob_refs = session
            .execute(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"file-path-upsert\"]')",
                &[],
            )
            .await
            .expect("blob ref read should succeed");
        assert_eq!(blob_refs.len(), 1);
    }
);

simulation_test!(
    lix_file_by_branch_insert_on_conflict_path_branch_updates_existing,
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
                    "INSERT INTO lix_file_by_branch \
                     (id, path, data, lixcol_branch_id) \
                     VALUES ('file-branch-path-upsert', '/docs/branch.md', X'6F6C64', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("seed by-branch insert should succeed");

        let result = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (path, data, lixcol_branch_id) \
                     VALUES ('/docs/branch.md', X'6E6577', '{branch_id}') \
                     ON CONFLICT (path, lixcol_branch_id) DO UPDATE SET data = excluded.data"
                ),
                &[],
            )
            .await
            .expect("by-branch path upsert should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/docs/branch.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-branch-path-upsert".to_string()),
                Value::Blob(b"new".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_by_branch_insert_on_conflict_path_without_branch_target_rejects,
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

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (path, data, lixcol_branch_id) \
                     VALUES ('/docs/reject.md', X'00', '{branch_id}') \
                     ON CONFLICT (path) DO UPDATE SET data = excluded.data"
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
    lix_file_insert_on_conflict_path_rejects_missing_path,
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
                "INSERT INTO lix_file (id, data) \
                 VALUES ('file-missing-path-upsert', X'00') \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect_err("path upsert without path should be rejected");
        assert!(error.message.contains("requires non-null path"));
    }
);

simulation_test!(
    lix_file_insert_on_conflict_path_rejects_untracked_collision,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-tracked-collision', '/docs/collision.md', X'00')",
                &[],
            )
            .await
            .expect("tracked file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (path, data, lixcol_untracked) \
                 VALUES ('/docs/collision.md', X'01', true) \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect_err("tracked/untracked path collision should be rejected");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("existing tracked file"));
    }
);

simulation_test!(
    lix_file_insert_on_conflict_path_updates_visible_global_file,
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
                "INSERT INTO lix_file (id, path, data, lixcol_global) \
                 VALUES ('file-global-path-upsert', '/docs/global.md', X'6F6C64', true)",
                &[],
            )
            .await
            .expect("global seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (path, data) \
                 VALUES ('/docs/global.md', X'6E6577') \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("path upsert should update visible global file");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, data, lixcol_global, lixcol_branch_id \
                 FROM lix_file_by_branch \
                 WHERE id = 'file-global-path-upsert' AND lixcol_branch_id = 'global'",
                &[],
            )
            .await
            .expect("global file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("file-global-path-upsert".to_string()),
                Value::Blob(b"new".to_vec().into()),
                Value::Boolean(true),
                Value::Text("global".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_rejects_duplicate_target_columns,
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
                "INSERT INTO lix_file (path, data) \
                 VALUES ('/docs/duplicate-target.md', X'00') \
                 ON CONFLICT (path, path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect_err("duplicate conflict target columns should be rejected");
        assert!(
            error
                .message
                .contains("duplicate write target column 'path'"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_preserves_reads_after_failure_and_rollback,
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
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('transaction-target', '/docs/target.md', X'6265666F7265')",
                &[],
            )
            .await
            .expect("transaction fixture file should insert");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('transaction-conflict-directory', '/conflict/')",
                &[],
            )
            .await
            .expect("transaction conflict directory should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('transaction-anchor', '/transaction-anchor.md', X'01')",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");

        let update = transaction
            .execute(
                "UPDATE lix_file SET data = X'6166746572' \
                 WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("first path-filtered transaction update should succeed");
        assert_eq!(update.rows_affected(), 1);

        let first_read = transaction
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("transaction read-your-writes lookup should succeed");
        assert_rows_eq(
            first_read,
            vec![vec![
                Value::Text("transaction-target".to_string()),
                Value::Blob(b"after".to_vec().into()),
            ]],
        );

        transaction
            .execute(
                "UPDATE lix_file SET data = X'616761696E' \
                 WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("repeated path-filtered transaction update should succeed");
        let repeated_read = transaction
            .execute(
                "SELECT data FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("repeated transaction path lookup should succeed");
        assert_rows_eq(
            repeated_read,
            vec![vec![Value::Blob(b"again".to_vec().into())]],
        );

        let error = transaction
            .execute(
                "UPDATE lix_file SET path = '/conflict' WHERE id = 'transaction-target'",
                &[],
            )
            .await
            .expect_err("conflicting transaction path update should fail");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let after_failure = transaction
            .execute(
                "SELECT id, path, data FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("cached path lookup should remain usable after a failed write");
        assert_rows_eq(
            after_failure,
            vec![vec![
                Value::Text("transaction-target".to_string()),
                Value::Text("/docs/target.md".to_string()),
                Value::Blob(b"again".to_vec().into()),
            ]],
        );

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");

        let rolled_back = session
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("rolled-back transaction path lookup should succeed");
        assert_rows_eq(
            rolled_back,
            vec![vec![
                Value::Text("transaction-target".to_string()),
                Value::Blob(b"before".to_vec().into()),
            ]],
        );
        let anchor = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/transaction-anchor.md'",
                &[],
            )
            .await
            .expect("rolled-back anchor lookup should succeed");
        assert_eq!(anchor.len(), 0);
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_rebuilds_for_branch_local_tombstones,
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
                "INSERT INTO lix_file_by_branch \
                 (id, path, data, lixcol_global, lixcol_branch_id) \
                 VALUES ('lane-file', '/global.md', X'01', true, 'global')",
                &[],
            )
            .await
            .expect("global lane file should insert");
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (id, path, data, lixcol_branch_id) \
                     VALUES ('lane-file', '/branch.md', X'02', '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("branch-local lane file should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('lane-anchor', '/lane-anchor.md', X'03')",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");

        let local = transaction
            .execute("SELECT id, path FROM lix_file WHERE id = 'lane-file'", &[])
            .await
            .expect("branch-local lane file should be visible");
        assert_rows_eq(
            local,
            vec![vec![
                Value::Text("lane-file".to_string()),
                Value::Text("/branch.md".to_string()),
            ]],
        );

        let deleted = transaction
            .execute(
                &format!(
                    "DELETE FROM lix_file_by_branch \
                     WHERE id = 'lane-file' AND lixcol_branch_id = '{branch_id}'"
                ),
                &[],
            )
            .await
            .expect("branch-local lane tombstone should stage");
        assert_eq!(deleted.rows_affected(), 1);

        let hidden_by_tombstone = transaction
            .execute("SELECT id, path FROM lix_file WHERE id = 'lane-file'", &[])
            .await
            .expect("lane lookup should succeed after the local tombstone");
        assert_eq!(
            hidden_by_tombstone.len(),
            0,
            "the branch-local tombstone must suppress its lower-priority global lane"
        );

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
        let restored = session
            .execute("SELECT id, path FROM lix_file WHERE id = 'lane-file'", &[])
            .await
            .expect("branch-local lane should be restored after rollback");
        assert_rows_eq(
            restored,
            vec![vec![
                Value::Text("lane-file".to_string()),
                Value::Text("/branch.md".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_observes_other_session_commits,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("transaction session should open"),
            &engine,
        );
        let other_session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("other session should open"),
            &engine,
        );

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('revision-anchor', '/revision-anchor.md', X'01')",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");

        let missing = transaction
            .execute(
                "SELECT id FROM lix_file WHERE path = '/other-session.md'",
                &[],
            )
            .await
            .expect("initial transaction lookup should succeed");
        assert_eq!(missing.len(), 0);

        other_session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('other-session-file', '/other-session.md', X'02')",
                &[],
            )
            .await
            .expect("other session file should commit");

        let visible_after_commit = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/other-session.md'",
                &[],
            )
            .await
            .expect("transaction lookup should observe the newer committed path revision");
        assert_rows_eq(
            visible_after_commit,
            vec![vec![
                Value::Text("other-session-file".to_string()),
                Value::Text("/other-session.md".to_string()),
            ]],
        );

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_observes_merge_reachability_changes,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("path-index-draft".to_string()),
            name: "Path index draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should create");
        let draft = main.wrap_session(
            engine
                .open_session("path-index-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('merge-main-file', '/main.md', X'01')",
            &[],
        )
        .await
        .expect("main divergence file should insert");
        draft
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('merge-draft-file', '/merged.md', X'02')",
                &[],
            )
            .await
            .expect("draft merge file should insert");

        let transaction_session = main.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("transaction session should open"),
            &engine,
        );
        let mut transaction = transaction_session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('merge-revision-anchor', '/merge-revision-anchor.md', X'03')",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");
        let missing_before_merge = transaction
            .execute("SELECT id FROM lix_file WHERE path = '/merged.md'", &[])
            .await
            .expect("initial merge path lookup should succeed");
        assert_eq!(missing_before_merge.len(), 0);

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "path-index-draft".to_string(),
            })
            .await
            .expect("merge should succeed");
        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);

        let visible_after_merge = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/merged.md'",
                &[],
            )
            .await
            .expect("transaction lookup should observe merged reachable descriptors");
        assert_rows_eq(
            visible_after_merge,
            vec![vec![
                Value::Text("merge-draft-file".to_string()),
                Value::Text("/merged.md".to_string()),
            ]],
        );

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }
);
