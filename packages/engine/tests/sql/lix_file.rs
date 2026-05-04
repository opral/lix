use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;

use super::assert_rows_eq;

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
                "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await
            .expect("file insert should succeed");
        assert_eq!(file_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, data, hidden, lixcol_schema_key \
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
                Value::Boolean(false),
                Value::Text("lix_file_descriptor".to_string()),
            ]
        );

        let staged_state_result = session
            .execute(
                "SELECT entity_id, schema_key \
             FROM lix_state \
             WHERE entity_id = 'file-readme' \
             ORDER BY schema_key, entity_id",
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
            "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
            &[],
        )
        .await
        .expect("directory insert should succeed");

    let insert_result = session
        .execute(
            "INSERT INTO lix_file (directory_id, name, extension) \
             VALUES ('dir-docs', 'readme', 'md')",
            &[],
        )
        .await
        .expect("file insert should apply defaulted id and hidden flag");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let result = session
        .execute(
            "SELECT id, path, directory_id, name, extension, hidden \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
            &[],
        )
        .await
        .expect("file read should succeed");
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let values = row_set.rows()[0].values();
    let [Value::Text(id), Value::Text(path), Value::Text(directory_id), Value::Text(name), Value::Text(extension), Value::Boolean(hidden)] =
        values
    else {
        panic!("expected generated file row, got {values:?}");
    };
    assert!(!id.is_empty(), "defaulted file id should be non-empty");
    assert_eq!(path, "/docs/readme.md");
    assert_eq!(directory_id, "dir-docs");
    assert_eq!(name, "readme");
    assert_eq!(extension, "md");
    assert!(!hidden);
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
                "SELECT id, path, name, extension, hidden \
             FROM lix_file \
             WHERE path = '/docs/readme.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        let values = row_set.rows()[0].values();
        let [Value::Text(id), Value::Text(path), Value::Text(name), Value::Text(extension), Value::Boolean(hidden)] =
            values
        else {
            panic!("expected generated file path row, got {values:?}");
        };
        assert!(!id.is_empty(), "defaulted file id should be non-empty");
        assert_eq!(path, "/docs/readme.md");
        assert_eq!(name, "readme");
        assert_eq!(extension, "md");
        assert!(!hidden);
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

    assert!(
        error.message.contains("requires binary data")
            && error.message.contains("use X'' for an empty file"),
        "unexpected error: {error}"
    );

    let parameter_error = session
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('null-param-data-file', '/null-param.bin', $1)",
            &[Value::Null],
        )
        .await
        .expect_err("parameterized NULL data should be rejected");

    assert!(
        parameter_error.message.contains("requires binary data")
            && parameter_error
                .message
                .contains("use X'' for an empty file"),
        "unexpected error: {parameter_error}"
    );

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
                "INSERT INTO lix_file (id, directory_id, name, extension) \
                 VALUES ('file-foo', NULL, 'foo', NULL)",
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
                "INSERT INTO lix_file (directory_id, name, extension) \
                 VALUES ('missing-dir', 'readme', 'md')",
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
                "INSERT INTO lix_file (id, directory_id, name, extension) \
                 VALUES ('file-readme', 'dir-docs', 'readme', 'md')",
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

            assert_eq!(error.code, "LIX_ERROR_PATH_DOT_SEGMENT");
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
                "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO lix_file (directory_id, name, extension, data) \
             VALUES ('dir-docs', 'readme', 'md', X'68656C6C6F')",
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
            "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
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
            "SELECT entity_id, schema_key \
             FROM lix_state \
             WHERE entity_id = 'file-readme' \
             ORDER BY schema_key, entity_id",
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

        assert!(
            error.message.contains("requires binary data")
                && error.message.contains("use X'' for an empty file"),
            "unexpected error: {error}"
        );

        let parameter_error = session
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = 'update-null-file'",
                &[Value::Null],
            )
            .await
            .expect_err("parameterized NULL data update should be rejected");

        assert!(
            parameter_error.message.contains("requires binary data")
                && parameter_error
                    .message
                    .contains("use X'' for an empty file"),
            "unexpected error: {parameter_error}"
        );

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

simulation_test!(lix_file_by_version_expands_global_rows, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data, hidden, lixcol_global, lixcol_untracked) \
             VALUES ('file-global-overlay', '/global.txt', X'67', false, true, false)",
            &[],
        )
        .await
        .expect("global file insert should succeed");

    let result = session
        .execute(
            "SELECT id, path, lixcol_version_id, lixcol_global, lixcol_untracked \
             FROM lix_file_by_version \
             WHERE id = 'file-global-overlay' \
             ORDER BY lixcol_version_id",
            &[],
        )
        .await
        .expect("file by-version read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("file-global-overlay".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text(sim.main_version_id().to_string()),
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
