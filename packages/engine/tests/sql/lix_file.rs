use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;

use super::assert_rows_eq;

simulation_test!(
    lix_file_read_rejects_public_path_inside_scalar_function,
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
                "SELECT id FROM lix_file WHERE lower(path) = '/readme.md'",
                &[],
            )
            .await
            .expect_err("public path column should not be hidden inside scalar functions");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("public column 'path'"));
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
    lix_file_path_insert_rejects_overlong_paths_and_segments,
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
        let segment_error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-long-segment', $1)",
                &[Value::Text(format!("/{long_segment}"))],
            )
            .await
            .expect_err("overlong file path segment should be rejected");
        assert_eq!(segment_error.code, LixError::CODE_INVALID_PARAM);
        assert!(segment_error.message.contains("path segment is too long"));

        let long_path = format!("/{}", ["abcd"; 820].join("/"));
        let path_error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-long-path', $1)",
                &[Value::Text(long_path)],
            )
            .await
            .expect_err("overlong file path should be rejected");
        assert_eq!(path_error.code, LixError::CODE_INVALID_PARAM);
        assert!(path_error.message.contains("path is too long"));

        let encoded_segment_at_limit = "%61".repeat(255);
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-encoded-limit', $1)",
                &[Value::Text(format!("/{encoded_segment_at_limit}"))],
            )
            .await
            .expect("percent-encoded segment should be measured after canonicalization");

        let encoded_segment_over_limit = "%61".repeat(256);
        let encoded_segment_error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-encoded-over-limit', $1)",
                &[Value::Text(format!("/{encoded_segment_over_limit}"))],
            )
            .await
            .expect_err("overlong canonical segment should be rejected");
        assert_eq!(encoded_segment_error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            encoded_segment_error
                .message
                .contains("path segment is too long")
        );

        let huge_path = format!("/{}", "a".repeat(1024 * 1024));
        let huge_error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('file-huge-path', $1)",
                &[Value::Text(huge_path)],
            )
            .await
            .expect_err("huge path input should be rejected without runtime internals");
        assert_eq!(huge_error.code, LixError::CODE_INVALID_PARAM);
        assert!(huge_error.message.contains("path input is too long"));
    }
);

simulation_test!(
    lix_file_path_insert_rejects_percent_encoded_forbidden_code_points,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path, expected_reason) in [
            (
                "file-percent-nul",
                "/docs/%00evil.txt",
                "path must not contain a NUL byte",
            ),
            (
                "file-percent-bidi",
                "/docs/%E2%80%AEevil.txt",
                "path segment contains a character that is not allowed",
            ),
        ] {
            let error = session
                .execute(
                    &format!("INSERT INTO lix_file (id, path) VALUES ('{id}', '{path}')"),
                    &[],
                )
                .await
                .expect_err("percent-encoded forbidden path code point should be rejected");
            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
            assert!(error.message.contains(expected_reason), "{error:?}");
        }
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
            ("file-percent-dot", "/docs/%2Ehidden"),
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
                   'file-percent-dot'\
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
                    Value::Text("file-percent-dot".to_string()),
                    Value::Text("/docs/.hidden".to_string()),
                    Value::Text(".hidden".to_string()),
                ],
            ],
        );
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
                Value::Blob(b"hello".to_vec()),
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
    lix_file_path_insert_applies_defaulted_id,
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
                "SELECT id, path, name \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Text(name)] = values else {
            panic!("expected generated file path row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted file id should be non-empty");
        assert_eq!(path, "/docs/readme.md");
        assert_eq!(name, "readme.md");
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
        assert_eq!(data, b"hello");
    }
);

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
                    Value::Blob(b"anonymous".to_vec()),
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
                Value::Blob(b"anonymous".to_vec()),
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
    assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new())]);
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
                &[Value::Blob(vec![1])],
            )
            .await
            .expect("first file path insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/x.bin', $1)",
                &[Value::Blob(vec![2])],
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

        for path in ["/a/../b/c.txt", "/a/%2e%2e/b/c.txt", "/a/./b/c.txt"] {
            let error = session
                .execute(
                    "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                    &[Value::Text(path.to_string()), Value::Blob(Vec::new())],
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
        assert_eq!(data, b"hello");
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
            Value::Blob(b"hello".to_vec()),
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
        assert_eq!(result.rows()[0].values(), &[Value::Blob(b"hello".to_vec())]);
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
                    Value::Blob(b"hello".to_vec()),
                ],
                vec![
                    Value::Text("update-float-file".to_string()),
                    Value::Blob(b"hello".to_vec()),
                ],
                vec![
                    Value::Text("update-int-file".to_string()),
                    Value::Blob(b"hello".to_vec()),
                ],
                vec![
                    Value::Text("update-text-file".to_string()),
                    Value::Blob(b"hello".to_vec()),
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
                    Value::Blob(b"hello".to_vec()),
                ],
                vec![
                    Value::Text("update-text-param-file".to_string()),
                    Value::Blob(b"hello".to_vec()),
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
    assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new())]);
});

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
                 VALUES ('global-shared-file', '/shared/a.txt', lix_text_encode('a'), true)",
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
