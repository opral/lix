use lix::ExecuteResult;
use lix::LixError;
use lix::Value;
use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_file_public_timestamps_cover_descriptor_and_content_revisions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let file_id = "74696d65-7374-816d-8073-000000000001";
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{file_id}', '/timestamps.txt', CAST('one' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("file insert should succeed");

        let initial = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at, lixcol_change_id \
                     FROM lix_file WHERE id = '{file_id}'"
                ),
                &[],
            )
            .await
            .expect("inserted file should be readable");
        let initial_created_at = initial.rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created_at should be text");
        let initial_updated_at = initial.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("updated_at should be text");
        let initial_change_id = initial.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("change_id should be text");

        session
            .execute(
                &format!(
                    "UPDATE lix_file SET content = CAST('two' AS BYTEA) WHERE id = '{file_id}'"
                ),
                &[],
            )
            .await
            .expect("content update should succeed");
        let content_update = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at, lixcol_change_id \
                     FROM lix_file WHERE id = '{file_id}'"
                ),
                &[],
            )
            .await
            .expect("content-updated file should be readable");
        let content_created_at = content_update.rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created_at should be text");
        let content_updated_at = content_update.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("updated_at should be text");
        let content_change_id = content_update.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("change_id should be text");
        assert_eq!(content_created_at, initial_created_at);
        assert_ne!(content_updated_at, initial_updated_at);
        assert_ne!(content_change_id, initial_change_id);

        let renamed_file_id = "74696d65-7374-816d-8073-000000000002";
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{renamed_file_id}', '/before-rename.txt', CAST('one' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("rename fixture insert should succeed");
        let before_rename = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at \
                     FROM lix_file WHERE id = '{renamed_file_id}'"
                ),
                &[],
            )
            .await
            .expect("rename fixture should be readable");
        let rename_created_at = before_rename.rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created_at should be text");
        let rename_updated_at = before_rename.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("updated_at should be text");

        session
            .execute(
                &format!(
                    "UPDATE lix_file SET path = '/renamed-timestamps.txt' \
                     WHERE id = '{renamed_file_id}'"
                ),
                &[],
            )
            .await
            .expect("rename should succeed");
        let renamed = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at \
                     FROM lix_file \
                     WHERE path = '/renamed-timestamps.txt' AND id = '{renamed_file_id}'"
                ),
                &[],
            )
            .await
            .expect("renamed file should be readable");
        assert_eq!(
            renamed.rows()[0]
                .get::<String>("lixcol_created_at")
                .expect("created_at should be text"),
            rename_created_at,
        );
        assert_eq!(
            renamed.rows()[0]
                .get::<String>("lixcol_updated_at")
                .expect("updated_at should be text"),
            rename_updated_at,
        );
    }
);

simulation_test!(
    file_descriptor_changes_always_carry_their_file_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let file_id = "66696c65-2d69-8464-8000-000000000001";
        let directory_id = "64697265-6374-846f-8000-000000000001";

        session
            .execute(
                &format!("INSERT INTO lix_directory (id, path) VALUES ('{directory_id}', '/docs')"),
                &[],
            )
            .await
            .expect("directory creation should succeed");
        session
            .execute(
                &format!("UPDATE lix_directory SET path = '/notes' WHERE id = '{directory_id}'"),
                &[],
            )
            .await
            .expect("directory rename should succeed");
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{file_id}', '/readme.md', CAST('hello' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("file creation should succeed");

        assert_eq!(
            super::select_rows(
                &session,
                &format!(
                    "SELECT DISTINCT schema_key, file_id \
                     FROM lix_change \
                     WHERE file_id = '{file_id}' \
                       AND schema_key IN ('lix_file_descriptor', 'lix_binary_blob_ref') \
                     ORDER BY schema_key"
                ),
            )
            .await,
            vec![
                vec![
                    Value::Text("lix_binary_blob_ref".to_string()),
                    Value::Text(file_id.to_string()),
                ],
                vec![
                    Value::Text("lix_file_descriptor".to_string()),
                    Value::Text(file_id.to_string()),
                ],
            ],
            "a natural file_id filter must return content and descriptor changes",
        );
        assert_eq!(
            super::select_rows(
                &session,
                &format!(
                    "SELECT file_id \
                     FROM lix_change \
                     WHERE schema_key = 'lix_directory_descriptor' \
                       AND entity_pk = lix_json('[\"{directory_id}\"]') \
                     ORDER BY created_at"
                ),
            )
            .await,
            vec![vec![Value::Null], vec![Value::Null]],
            "directory creation and rename remain namespace-level",
        );

        let baseline = session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed")
            .commit_id;
        session
            .execute(
                &format!("UPDATE lix_file SET path = '/notes/readme.md' WHERE id = '{file_id}'"),
                &[],
            )
            .await
            .expect("moving a file under an existing directory should succeed");
        let renamed_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("renamed head should load")
            .expect("renamed head should exist")
            .to_string();

        assert_eq!(
            super::select_rows(
                &session,
                &format!(
                    "SELECT schema_key, file_id \
                     FROM lix_working_diff \
                     WHERE file_id = '{file_id}' \
                       AND schema_key = 'lix_file_descriptor'"
                ),
            )
            .await,
            vec![vec![
                Value::Text("lix_file_descriptor".to_string()),
                Value::Text(file_id.to_string()),
            ]],
            "a pending rename must be visible through a natural working-diff file filter",
        );
        assert_eq!(
            super::select_rows(
                &session,
                &format!(
                    "SELECT schema_key, file_id \
                     FROM lix_diff('{baseline}', '{renamed_head}') \
                     WHERE file_id = '{file_id}' \
                       AND schema_key = 'lix_file_descriptor'"
                ),
            )
            .await,
            vec![vec![
                Value::Text("lix_file_descriptor".to_string()),
                Value::Text(file_id.to_string()),
            ]],
            "a pending rename must be visible through a natural historical diff file filter",
        );

        session
            .execute(&format!("DELETE FROM lix_file WHERE id = '{file_id}'"), &[])
            .await
            .expect("file deletion should succeed");
        let descriptor_changes = super::select_rows(
            &session,
            &format!(
                "SELECT file_id, snapshot_content \
                 FROM lix_change \
                 WHERE file_id = '{file_id}' \
                   AND schema_key = 'lix_file_descriptor' \
                 ORDER BY created_at"
            ),
        )
        .await;
        assert_eq!(descriptor_changes.len(), 3);
        assert!(
            descriptor_changes
                .iter()
                .all(|row| row[0] == Value::Text(file_id.to_string()))
        );
        assert_eq!(
            descriptor_changes
                .iter()
                .filter(|row| row[1] == Value::Null)
                .count(),
            1,
            "exactly the deletion descriptor change is a tombstone",
        );
    }
);

simulation_test!(
    lix_file_update_can_compare_the_read_only_change_id,
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
                "INSERT INTO lix_file (id, path, content) VALUES ('67756172-6465-842d-8669-6c6500000000', '/guarded.txt', CAST('before' AS BYTEA))",
                &[],
            )
            .await
            .expect("guarded file insert should succeed");
        let current = session
            .execute(
                "SELECT lixcol_change_id FROM lix_file WHERE id = '67756172-6465-842d-8669-6c6500000000'",
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
                "UPDATE lix_file SET content = CAST('after' AS BYTEA) WHERE path = '/guarded.txt' AND lixcol_change_id = $1",
                &[Value::Text(change_id)],
            )
            .await
            .expect("matching change id should be accepted in an update predicate");
        assert_eq!(applied.rows_affected(), 1);

        let stale = session
            .execute(
                "UPDATE lix_file SET content = CAST('stale' AS BYTEA) WHERE path = '/guarded.txt' AND lixcol_change_id = 'stale'",
                &[],
            )
            .await
            .expect("stale change id should produce a zero-row update");
        assert_eq!(stale.rows_affected(), 0);

        let content = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '67756172-6465-842d-8669-6c6500000000'",
                &[],
            )
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('72656164-6d65-8d66-896c-650000000000', '/Readme.md')",
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

        assert_rows_eq(
            result,
            vec![vec![Value::Text(
                "72656164-6d65-8d66-896c-650000000000".to_string(),
            )]],
        );
    }
);

simulation_test!(
    lix_file_exact_content_batch_rejects_a_missing_pinned_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let missing = sim.wrap_session(
            engine
                .open_session_at("missing-file-branch")
                .await
                .expect("a pinned session may open before its branch is resolved"),
            &engine,
        );

        let error = missing
            .execute(
                "SELECT content FROM lix_file WHERE path IN ('/missing-a.txt', '/missing-b.txt')",
                &[],
            )
            .await
            .expect_err("the exact data batch must validate its pinned branch");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }
);

simulation_test!(
    lix_file_unknown_metadata_column_suggests_public_name,
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
            .execute("SELECT metadata FROM lix_file", &[])
            .await
            .expect_err("metadata should not be a public column");
        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert_eq!(error.hint.as_deref(), Some("Did you mean lixcol_metadata?"));
    }
);

simulation_test!(
    lix_file_legacy_payload_column_is_not_supported,
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
            .execute("SELECT data FROM lix_file", &[])
            .await
            .expect_err("the retired data column must not remain available");

        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
    }
);

simulation_test!(
    lix_file_lower_path_like_keeps_the_blob_revision_for_guarded_updates,
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
                "INSERT INTO lix_file (id, path, content) VALUES ('67756172-6465-842d-8365-617263682d00', '/Docs/Guarded-Readme.md', CAST('before' AS BYTEA))",
                &[],
            )
            .await
            .expect("search fixture insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ('6f746865-722d-8365-8172-63682d666900', '/Docs/other.md', CAST('other' AS BYTEA))",
                &[],
            )
            .await
            .expect("non-matching search fixture insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ('756e6963-6f64-852d-8365-617263682d00', '/Ä/Readme.md', CAST('unicode' AS BYTEA))",
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
                "UPDATE lix_file SET content = CAST('after' AS BYTEA) WHERE path = '/Docs/Guarded-Readme.md' AND lixcol_change_id = $1",
                &[Value::Text(change_id)],
            )
            .await
            .expect("search revision should guard the data update");
        assert_eq!(updated.rows_affected(), 1);

        let content = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '67756172-6465-842d-8365-617263682d00'",
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
                .open_session()
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let long_segment = "a".repeat(256);
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('66696c65-2d6c-8f6e-872d-7365676d6500', $1)",
                &[Value::Text(format!("/{long_segment}"))],
            )
            .await
            .expect("long opaque file path segment should be accepted");

        let long_path = format!("/{}", ["abcd"; 820].join("/"));
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('66696c65-2d6c-8f6e-872d-706174680000', $1)",
                &[Value::Text(long_path.clone())],
            )
            .await
            .expect("long opaque file path should be accepted");

        let result = session
            .execute(
                "SELECT id, path FROM lix_file \
                 WHERE id IN ('66696c65-2d6c-8f6e-872d-7365676d6500', '66696c65-2d6c-8f6e-872d-706174680000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("66696c65-2d6c-8f6e-872d-706174680000".to_string()),
                    Value::Text(long_path),
                ],
                vec![
                    Value::Text("66696c65-2d6c-8f6e-872d-7365676d6500".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let insert_error = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('plugin-poison', '/.lix/plugins/nested/plugin_sentinel.lixplugin', CAST('bad' AS BYTEA))",
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('73616665-2d66-896c-8500-000000000000', '/safe.bin', CAST('ok' AS BYTEA))",
                &[],
            )
            .await
            .expect("safe file insert should succeed");

        let update_error = session
            .execute(
                "UPDATE lix_file \
                 SET path = '/.lix/plugins/plugin_sentinel.lixplugin' \
                 WHERE id = '73616665-2d66-896c-8500-000000000000'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path) in [
            ("66696c65-2d70-8572-8365-6e742d610000", "/docs/%61.txt"),
            ("66696c65-2d70-8572-8365-6e742d6e7500", "/docs/%00evil.txt"),
            (
                "66696c65-2d70-8572-8365-6e742d626900",
                "/docs/%E2%80%AEevil.txt",
            ),
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
                 WHERE id IN ('66696c65-2d70-8572-8365-6e742d610000', '66696c65-2d70-8572-8365-6e742d626900', '66696c65-2d70-8572-8365-6e742d6e7500') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("66696c65-2d70-8572-8365-6e742d610000".to_string()),
                    Value::Text("/docs/%61.txt".to_string()),
                    Value::Text("%61.txt".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d70-8572-8365-6e742d626900".to_string()),
                    Value::Text("/docs/%E2%80%AEevil.txt".to_string()),
                    Value::Text("%E2%80%AEevil.txt".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d70-8572-8365-6e742d6e7500".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path) in [
            ("66696c65-2d66-8f6f-8d64-6f7400000000", "/foo."),
            ("66696c65-2d66-8f6f-8d64-6f742d646f01", "/foo.."),
            ("66696c65-2d66-8f6f-8d64-6f742d646f02", "/foo..."),
            ("66696c65-2d61-8263-8869-766500000000", "/archive.tar.gz"),
            ("66696c65-2d64-8f74-856e-760000000000", "/.env"),
            ("66696c65-2d68-8964-8465-6e2d696e2d00", "/docs/.hidden"),
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
                   '66696c65-2d66-8f6f-8d64-6f7400000000',\
                   '66696c65-2d66-8f6f-8d64-6f742d646f01',\
                   '66696c65-2d66-8f6f-8d64-6f742d646f02',\
                   '66696c65-2d61-8263-8869-766500000000',\
                   '66696c65-2d64-8f74-856e-760000000000',\
                   '66696c65-2d68-8964-8465-6e2d696e2d00'\
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
                    Value::Text("66696c65-2d61-8263-8869-766500000000".to_string()),
                    Value::Text("/archive.tar.gz".to_string()),
                    Value::Text("archive.tar.gz".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d64-8f74-856e-760000000000".to_string()),
                    Value::Text("/.env".to_string()),
                    Value::Text(".env".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d66-8f6f-8d64-6f7400000000".to_string()),
                    Value::Text("/foo.".to_string()),
                    Value::Text("foo.".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d66-8f6f-8d64-6f742d646f01".to_string()),
                    Value::Text("/foo..".to_string()),
                    Value::Text("foo..".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d66-8f6f-8d64-6f742d646f02".to_string()),
                    Value::Text("/foo...".to_string()),
                    Value::Text("foo...".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d68-8964-8465-6e2d696e2d00".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('66696c65-2d73-8c61-8368-000000000000', NULL, 'nested/name')",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('66696c65-2d64-8573-8372-6970746f7200', NULL, 'foo.')",
                &[],
            )
            .await
            .expect("descriptor-shaped insert should accept full opaque basename");

        let result = session
            .execute(
                "SELECT id, path, name \
                 FROM lix_file \
                 WHERE id = '66696c65-2d64-8573-8372-6970746f7200'",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("66696c65-2d64-8573-8372-6970746f7200".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name, extension) \
                 VALUES ('66696c65-2d65-8874-856e-73696f6e2d00', NULL, 'readme', 'md')",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('66696c65-2d66-8f6f-8000-000000000000', '/foo')",
                &[],
            )
            .await
            .expect("plain file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('66696c65-2d66-8f6f-8d64-6f7400000000', '/foo.')",
                &[],
            )
            .await
            .expect("trailing-dot file insert should be distinct from plain name");

        let result = session
            .execute(
                "SELECT id, path, name \
                 FROM lix_file \
                 WHERE id IN ('66696c65-2d66-8f6f-8000-000000000000', '66696c65-2d66-8f6f-8d64-6f7400000000') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("66696c65-2d66-8f6f-8000-000000000000".to_string()),
                    Value::Text("/foo".to_string()),
                    Value::Text("foo".to_string()),
                ],
                vec![
                    Value::Text("66696c65-2d66-8f6f-8d64-6f7400000000".to_string()),
                    Value::Text("/foo.".to_string()),
                    Value::Text("foo.".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_insert_reads_path_content_and_parent_dirs,
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

        let result = session
            .execute(
                "SELECT id, path, content, lixcol_schema_key \
             FROM lix_file \
             WHERE id = '66696c65-2d72-8561-846d-650000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("66696c65-2d72-8561-846d-650000000000".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Text("lix_file_descriptor".to_string()),
            ]
        );

        let component_changes = session
            .execute(
                "SELECT schema_key \
             FROM lix_change \
             WHERE entity_pk = lix_json('[\"66696c65-2d72-8561-846d-650000000000\"]') \
               AND schema_key IN ('lix_file_descriptor', 'lix_binary_blob_ref') \
             ORDER BY schema_key",
                &[],
            )
            .await
            .expect("filesystem component changes should remain inspectable");
        assert_eq!(
            component_changes.len(),
            2,
            "file path insert should emit one file descriptor and one blob ref change"
        );

        let directory_result = session
            .execute(
                "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs', '/docs/guides') \
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
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_directory (id, parent_id, name) \
             VALUES ('6469722d-646f-8373-8000-000000000000', NULL, 'docs')",
            &[],
        )
        .await
        .expect("directory insert should succeed");

    let insert_result = session
        .execute(
            "INSERT INTO lix_file (directory_id, name) \
             VALUES ('6469722d-646f-8373-8000-000000000000', 'readme.md')",
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
    assert_eq!(directory_id, "6469722d-646f-8373-8000-000000000000");
    assert_eq!(name, "readme.md");
});

simulation_test!(
    lix_file_path_insert_applies_defaulted_id_and_empty_content,
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
                "INSERT INTO lix_file (path) VALUES ('/docs/readme.md')",
                &[],
            )
            .await
            .expect("file path insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, name, content \
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
                "SELECT id FROM lix_file WHERE path = '/docs/readme.md' AND content IS NULL",
                &[],
            )
            .await
            .expect("file null predicate should succeed");
        assert_eq!(null_result.len(), 0);
    }
);

simulation_test!(
    lix_file_path_content_insert_applies_defaulted_id,
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
                "INSERT INTO lix_file (path, content) VALUES ('/docs/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file path data insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, content \
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

simulation_test!(lix_file_content_is_not_nullable, |sim| async move {
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
            "SELECT is_nullable \
             FROM information_schema.columns \
             WHERE table_name = 'lix_file' \
               AND column_name = 'content'",
            &[],
        )
        .await
        .expect("information schema read should succeed");

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Text("NO".to_string())]);
});

simulation_test!(lix_file_insert_rejects_null_content, |sim| async move {
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
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('6e756c6c-2d64-8174-812d-66696c650000', '/null.bin', NULL)",
            &[],
        )
        .await
        .expect_err("explicit NULL data should be rejected");

    assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

    let parameter_error = session
        .execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('6e756c6c-2d70-8172-816d-2d6461746100', '/null-param.bin', $1)",
            &[Value::Null],
        )
        .await
        .expect_err("parameterized NULL data should be rejected");

    assert_eq!(parameter_error.code, LixError::CODE_TYPE_MISMATCH);

    let result = session
        .execute(
            "SELECT id FROM lix_file \
             WHERE id IN ('6e756c6c-2d64-8174-812d-66696c650000', '6e756c6c-2d70-8172-816d-2d6461746100')",
            &[],
        )
        .await
        .expect("file read should succeed");
    assert_eq!(result.len(), 0);
});

simulation_test!(
    lix_file_insert_rejects_non_binary_content_literals,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, sql) in [
            (
                "74657874-2d64-8174-812d-66696c650000",
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('74657874-2d64-8174-812d-66696c650000', '/text.bin', 'hello')",
            ),
            (
                "696e742d-6461-8461-8d66-696c65000000",
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('696e742d-6461-8461-8d66-696c65000000', '/int.bin', 12345)",
            ),
            (
                "666c6f61-742d-8461-8461-2d66696c6500",
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('666c6f61-742d-8461-8461-2d66696c6500', '/float.bin', 1.5)",
            ),
            (
                "626f6f6c-2d64-8174-812d-66696c650000",
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('626f6f6c-2d64-8174-812d-66696c650000', '/bool.bin', true)",
            ),
            (
                "74657874-2d66-856e-8374-696f6e2d6400",
                "INSERT INTO lix_file (id, path, content) \
                 VALUES (\
                   '74657874-2d66-856e-8374-696f6e2d6400',\
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
            assert_eq!(
                error.hint(),
                Some("Use CAST($1 AS BYTEA) with a text parameter for file contents."),
                "{id}"
            );
        }

        let result = session
            .execute(
                "SELECT id FROM lix_file \
                 WHERE id IN (\
                   '74657874-2d64-8174-812d-66696c650000',\
                   '74657874-2d66-856e-8374-696f6e2d6400',\
                   '696e742d-6461-8461-8d66-696c65000000',\
                   '666c6f61-742d-8461-8461-2d66696c6500',\
                   '626f6f6c-2d64-8174-812d-66696c650000'\
                 )",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_file_insert_rejects_non_binary_content_from_select,
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
                "INSERT INTO lix_file (id, path, content) \
                 SELECT '73656c65-6374-8d74-8578-742d64617400', '/select-text.bin', 'hello'",
                &[],
            )
            .await
            .expect_err("non-binary content from SELECT should be rejected");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert_eq!(
            error.hint(),
            Some("Use CAST($1 AS BYTEA) with a text parameter for file contents.")
        );

        let result = session
            .execute(
                "SELECT id FROM lix_file WHERE id = '73656c65-6374-8d74-8578-742d64617400'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 0);
    }
);

simulation_test!(
    lix_file_insert_rejects_non_binary_content_parameters,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, value) in [
            (
                "74657874-2d70-8172-816d-2d6461746100",
                Value::Text("hello".to_string()),
            ),
            (
                "696e742d-7061-8261-8d2d-646174612d00",
                Value::Integer(12345),
            ),
        ] {
            let error = session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, content) \
                         VALUES ('{id}', '/{id}.bin', $1)"
                    ),
                    &[value],
                )
                .await
                .expect_err("non-binary data parameter should be rejected");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
            assert_eq!(
                error.hint(),
                Some("Use CAST($1 AS BYTEA) with a text parameter for file contents."),
                "{id}"
            );
        }
    }
);

simulation_test!(
    lix_file_accepts_explicit_text_to_binary_casts,
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
                 VALUES ('63617374-2d62-896e-8172-792d66696c00', $1, CAST($2 AS BYTEA))",
                &[
                    Value::Text("/cast-binary.txt".to_string()),
                    Value::Text("inserted".to_string()),
                ],
            )
            .await
            .expect("explicit binary cast insert should succeed");

        session
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) \
                 WHERE id = '63617374-2d62-896e-8172-792d66696c00'",
                &[Value::Text("updated".to_string())],
            )
            .await
            .expect("explicit binary cast update should succeed");

        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '63617374-2d62-896e-8172-792d66696c00'",
                &[],
            )
            .await
            .expect("cast file read should succeed");
        assert_rows_eq(result, vec![vec![Value::Blob(b"updated".to_vec().into())]]);

        let unicode_content = "äöü, —, →, €, ↔";
        session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/cast-unicode.txt', CAST($1 AS BYTEA))",
                &[Value::Text(unicode_content.to_string())],
            )
            .await
            .expect("UTF-8 text cast insert should succeed");

        let result = session
            .execute(
                "SELECT CAST(content AS TEXT) FROM lix_file WHERE path = '/cast-unicode.txt'",
                &[],
            )
            .await
            .expect("UTF-8 cast file read should succeed");
        assert_rows_eq(result, vec![vec![Value::Text(unicode_content.to_string())]]);

        let lengths = session
            .execute(
                "SELECT length(content), OCTET_LENGTH(content) \
                 FROM lix_file WHERE path = '/cast-unicode.txt'",
                &[],
            )
            .await
            .expect("file length query should succeed");
        assert_rows_eq(lengths, vec![vec![Value::Integer(15), Value::Integer(26)]]);

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/raw-bytes.bin', $1)",
                &[Value::Blob(vec![0xff, 0x00, 0x61].into())],
            )
            .await
            .expect("raw binary file insert should succeed");
        let raw_length = session
            .execute(
                "SELECT OCTET_LENGTH(content) FROM lix_file WHERE path = '/raw-bytes.bin'",
                &[],
            )
            .await
            .expect("raw binary length query should succeed");
        assert_rows_eq(raw_length, vec![vec![Value::Integer(3)]]);
    }
);

simulation_test!(
    lix_file_insert_accepts_numbered_path_and_content_parameters,
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
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text("616e6f6e-796d-8f75-832d-706172616d00".to_string()),
                    Value::Text("/numbered-param.bin".to_string()),
                    Value::Blob(b"numbered".to_vec().into()),
                ],
            )
            .await
            .expect("numbered parameter insert should succeed");
        assert_eq!(insert_result.rows_affected(), 1);

        let result = session
            .execute(
                "SELECT path, content FROM lix_file WHERE id = $1",
                &[Value::Text(
                    "616e6f6e-796d-8f75-832d-706172616d00".to_string(),
                )],
            )
            .await
            .expect("numbered parameter read should succeed");
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/numbered-param.bin".to_string()),
                Value::Blob(b"numbered".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_numbered_content_parameter_keeps_strict_blob_validation,
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
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text("616e6f6e-796d-8f75-832d-746578742d00".to_string()),
                    Value::Text("/numbered-text-data.bin".to_string()),
                    Value::Text("not binary".to_string()),
                ],
            )
            .await
            .expect_err("numbered non-binary data parameter should be rejected");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    lix_file_insert_accepts_empty_blob_content,
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
                "INSERT INTO lix_file (id, path, content) \
             VALUES ('656d7074-792d-8461-8461-2d66696c6500', '/empty.bin', CAST('' AS BYTEA))",
                &[],
            )
            .await
            .expect("empty blob data should be accepted");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '656d7074-792d-8461-8461-2d66696c6500'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new().into())]);

        let blob_ref_changes = session
            .execute(
                "SELECT id \
             FROM lix_change \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND entity_pk = lix_json('[\"656d7074-792d-8461-8461-2d66696c6500\"]')",
                &[],
            )
            .await
            .expect("blob ref changes should read");
        assert_eq!(blob_ref_changes.len(), 0);
    }
);

simulation_test!(
    lix_file_path_insert_rejects_duplicate_root_path,
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
                "INSERT INTO lix_file (path, content) VALUES ('/x.bin', $1)",
                &[Value::Blob(vec![1].into())],
            )
            .await
            .expect("first file path insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/x.bin', $1)",
                &[Value::Blob(vec![2].into())],
            )
            .await
            .expect_err("duplicate file path insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_file_insert_duplicate_id_with_content_reports_lix_file,
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
                 VALUES ('73616d65-2d66-896c-8500-000000000000', '/a.bin', CAST('byte-01' AS BYTEA))",
                &[],
            )
            .await
            .expect("first file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('73616d65-2d66-896c-8500-000000000000', '/b.bin', CAST('byte-02' AS BYTEA))",
                &[],
            )
            .await
            .expect_err("duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error
                    .message
                    .contains("id '73616d65-2d66-896c-8500-000000000000'")
                && !error.message.contains("lix_binary_blob_ref"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(
    lix_file_insert_duplicate_id_without_content_reports_lix_file,
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
                "INSERT INTO lix_file (id, path) VALUES ('73616d65-2d66-896c-8500-000000000000', '/a.bin')",
                &[],
            )
            .await
            .expect("first file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('73616d65-2d66-896c-8500-000000000000', '/b.bin')",
                &[],
            )
            .await
            .expect_err("duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error
                    .message
                    .contains("id '73616d65-2d66-896c-8500-000000000000'")
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ('73616d65-2d66-896c-8500-000000000000', '/a.bin', CAST('byte-01' AS BYTEA)), \
                 ('73616d65-2d66-896c-8500-000000000000', '/b.bin', CAST('byte-02' AS BYTEA))",
                &[],
            )
            .await
            .expect_err("same-batch duplicate file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file'")
                && error
                    .message
                    .contains("id '73616d65-2d66-896c-8500-000000000000'")
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (id, path, content, lixcol_branch_id) \
                     VALUES ('73616d65-2d66-896c-8500-000000000000', '/a.bin', CAST('byte-01' AS BYTEA), '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("first by-branch file insert should succeed");

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (id, path, content, lixcol_branch_id) \
                     VALUES ('73616d65-2d66-896c-8500-000000000000', '/b.bin', CAST('byte-02' AS BYTEA), '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect_err("duplicate by-branch file id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_file_by_branch'")
                && error
                    .message
                    .contains("id '73616d65-2d66-896c-8500-000000000000'")
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo')", &[])
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/foo')", &[])
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
            .execute("SELECT path FROM lix_directory WHERE path = '/foo'", &[])
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ('6469722d-666f-8f00-8000-000000000000', NULL, 'foo')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('66696c65-2d66-8f6f-8000-000000000000', NULL, 'foo')",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ('66696c65-2d66-8f6f-8000-000000000000', '/foo')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute("INSERT INTO lix_directory (path) VALUES ('/bar')", &[])
            .await
            .expect("directory insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET path = '/bar' WHERE id = '66696c65-2d66-8f6f-8000-000000000000'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_file (directory_id, name) \
                 VALUES ('6d697373-696e-872d-8469-720000000000', 'readme.md')",
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
            .expect("directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, directory_id, name) \
                 VALUES ('66696c65-2d72-8561-846d-650000000000', '6469722d-646f-8373-8000-000000000000', 'readme.md')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET directory_id = '6d697373-696e-872d-8469-720000000000' WHERE id = '66696c65-2d72-8561-846d-650000000000'",
                &[],
            )
            .await
            .expect_err("file update should reject missing directory_id");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);

        let result = session
            .execute(
                "SELECT path, directory_id FROM lix_file WHERE id = '66696c65-2d72-8561-846d-650000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Text("/docs/readme.md".to_string()),
                Value::Text("6469722d-646f-8373-8000-000000000000".to_string())
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for path in ["/a/../b/c.txt", "/a/./b/c.txt"] {
            let error = session
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
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
    lix_file_content_insert_applies_defaulted_id,
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
             VALUES ('6469722d-646f-8373-8000-000000000000', NULL, 'docs')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        let insert_result = session
            .execute(
                "INSERT INTO lix_file (directory_id, name, content) \
             VALUES ('6469722d-646f-8373-8000-000000000000', 'readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file data insert should apply defaulted id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT id, path, content \
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

simulation_test!(lix_file_path_update_preserves_content, |sim| async move {
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
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('66696c65-2d72-8561-846d-650000000000', '/docs/guides/readme.md', CAST('hello' AS BYTEA))",
            &[],
        )
        .await
        .expect("file insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let update_result = session
        .execute(
            "UPDATE lix_file \
             SET path = '/docs/readme-renamed.md' \
             WHERE id = '66696c65-2d72-8561-846d-650000000000'",
            &[],
        )
        .await
        .expect("file path update should succeed");
    assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

    let file_result = session
        .execute(
            "SELECT id, path, content \
             FROM lix_file \
             WHERE id = '66696c65-2d72-8561-846d-650000000000'",
            &[],
        )
        .await
        .expect("file read after path update should succeed");
    let file_rows = file_result;
    assert_eq!(file_rows.len(), 1);
    assert_eq!(
        file_rows.rows()[0].values(),
        &[
            Value::Text("66696c65-2d72-8561-846d-650000000000".to_string()),
            Value::Text("/docs/readme-renamed.md".to_string()),
            Value::Blob(b"hello".to_vec().into()),
        ]
    );

    let directory_result = session
        .execute(
            "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs', '/docs/guides') \
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
             ('66696c65-2d74-8172-8765-740000000000', '/docs/target.md', CAST('target' AS BYTEA)), \
             ('66696c65-2d6f-8468-8572-000000000000', '/docs/other.md', CAST('other' AS BYTEA))",
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
                Value::Text("66696c65-2d74-8172-8765-740000000000".to_string()),
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
                "SELECT id, path, content FROM lix_file WHERE path = '/archive/renamed.md'",
                &[],
            )
            .await
            .expect("renamed path should be visible through a fresh index");
        assert_rows_eq(
            renamed,
            vec![vec![
                Value::Text("66696c65-2d74-8172-8765-740000000000".to_string()),
                Value::Text("/archive/renamed.md".to_string()),
                Value::Blob(b"target".to_vec().into()),
            ]],
        );

        let unrelated = session
            .execute(
                "SELECT path, content FROM lix_file WHERE id = '66696c65-2d6f-8468-8572-000000000000'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('6272616e-6368-8d68-8561-642d66696c00', '/branch-head.txt', CAST('head' AS BYTEA))",
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
            vec![vec![Value::Text(
                "6272616e-6368-8d68-8561-642d66696c00".to_string(),
            )]],
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
            vec![vec![Value::Text(
                "6272616e-6368-8d68-8561-642d66696c00".to_string(),
            )]],
        );
    }
);

simulation_test!(
    atelier_current_path_range_and_order_workloads,
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
                "INSERT INTO lix_file (id, path, content) VALUES \
             ('01950000-0000-7000-8000-000000000008', '/.lix/app_data/atelier/extensions/demo/a.js', CAST('a' AS BYTEA)), \
             ('01950000-0000-7000-8000-000000000009', '/.lix/app_data/atelier/extensions/demo/b.js', CAST('b' AS BYTEA)), \
             ('01950000-0000-7000-8000-00000000000a', '/.lix/app_data/atelier/extensions0/out.js', CAST('x' AS BYTEA)), \
             ('01940000-0000-7000-8000-000000000003', '/docs/readme.md', CAST('r' AS BYTEA))",
                &[],
            )
            .await
            .expect("Atelier path fixtures should insert");

        let range = session
            .execute(
                "SELECT path, content FROM lix_file \
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
    lix_file_update_rejects_null_content_and_preserves_existing_content,
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
                 VALUES ('75706461-7465-8d6e-856c-6c2d66696c00', '/update-null.bin', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_file SET content = NULL WHERE id = '75706461-7465-8d6e-856c-6c2d66696c00'",
                &[],
            )
            .await
            .expect_err("explicit NULL data update should be rejected");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let parameter_error = session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE id = '75706461-7465-8d6e-856c-6c2d66696c00'",
                &[Value::Null],
            )
            .await
            .expect_err("parameterized NULL data update should be rejected");

        assert_eq!(parameter_error.code, LixError::CODE_TYPE_MISMATCH);

        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '75706461-7465-8d6e-856c-6c2d66696c00'",
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
    lix_file_update_rejects_non_binary_content_literals_and_preserves_existing_content,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, assignment) in [
            ("75706461-7465-8d74-8578-742d66696c00", "'hello'"),
            (
                "75706461-7465-8d74-8578-742d66756e00",
                "lix_json_get_text(lix_json('{\"value\":\"hello\"}'), 'value')",
            ),
            ("75706461-7465-8d69-8e74-2d66696c6500", "12345"),
            ("75706461-7465-8d66-8c6f-61742d666900", "1.5"),
            ("75706461-7465-8d62-8f6f-6c2d66696c00", "true"),
        ] {
            session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, content) \
                         VALUES ('{id}', '/{id}.bin', CAST('hello' AS BYTEA))"
                    ),
                    &[],
                )
                .await
                .expect("file insert should succeed");

            let error = session
                .execute(
                    &format!("UPDATE lix_file SET content = {assignment} WHERE id = '{id}'"),
                    &[],
                )
                .await
                .expect_err("non-binary data literal update should be rejected");

            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }

        let result = session
            .execute(
                "SELECT id, content FROM lix_file \
                 WHERE id IN (\
                   '75706461-7465-8d74-8578-742d66696c00',\
                   '75706461-7465-8d74-8578-742d66756e00',\
                   '75706461-7465-8d69-8e74-2d66696c6500',\
                   '75706461-7465-8d66-8c6f-61742d666900',\
                   '75706461-7465-8d62-8f6f-6c2d66696c00'\
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
                    Value::Text("75706461-7465-8d62-8f6f-6c2d66696c00".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("75706461-7465-8d66-8c6f-61742d666900".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("75706461-7465-8d69-8e74-2d66696c6500".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("75706461-7465-8d74-8578-742d66696c00".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("75706461-7465-8d74-8578-742d66756e00".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_update_rejects_non_binary_content_parameters_and_preserves_existing_content,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, value) in [
            (
                "75706461-7465-8d74-8578-742d70617200",
                Value::Text("hello".to_string()),
            ),
            (
                "75706461-7465-8d69-8e74-2d7061726100",
                Value::Integer(12345),
            ),
        ] {
            session
                .execute(
                    &format!(
                        "INSERT INTO lix_file (id, path, content) \
                         VALUES ('{id}', '/{id}.bin', CAST('hello' AS BYTEA))"
                    ),
                    &[],
                )
                .await
                .expect("file insert should succeed");

            let error = session
                .execute(
                    &format!("UPDATE lix_file SET content = $1 WHERE id = '{id}'"),
                    &[value],
                )
                .await
                .expect_err("non-binary data parameter update should be rejected");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{id}");
        }

        let result = session
            .execute(
                "SELECT id, content FROM lix_file \
                 WHERE id IN ('75706461-7465-8d74-8578-742d70617200', '75706461-7465-8d69-8e74-2d7061726100') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("75706461-7465-8d69-8e74-2d7061726100".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
                vec![
                    Value::Text("75706461-7465-8d74-8578-742d70617200".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                ],
            ],
        );
    }
);

simulation_test!(
    lix_file_update_accepts_empty_blob_content,
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
             VALUES ('656d7074-792d-8570-8461-74652d666900', '/empty-update.bin', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let update_result = session
        .execute(
            "UPDATE lix_file SET content = CAST('' AS BYTEA) WHERE id = '656d7074-792d-8570-8461-74652d666900'",
            &[],
        )
        .await
        .expect("empty blob data update should be accepted");
        assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT content FROM lix_file WHERE id = '656d7074-792d-8570-8461-74652d666900'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Blob(Vec::new().into())]);
    }
);

simulation_test!(
    lix_file_equal_normalized_metadata_skips_descriptor_history,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let upsert = "INSERT INTO lix_file (path, content, lixcol_metadata) \
                      VALUES ($1, $2, $3) \
                      ON CONFLICT (path) DO UPDATE SET \
                        content = excluded.content, \
                        lixcol_metadata = excluded.lixcol_metadata";
        let metadata = json!({"a": 1, "z": 2});
        session
            .execute(
                upsert,
                &[
                    Value::Text("/equal-metadata.bin".to_string()),
                    Value::Blob(b"before".to_vec().into()),
                    Value::Json(metadata.clone().into()),
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
                    Value::Json(metadata.clone().into()),
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
                "SELECT content, lixcol_metadata \
                 FROM lix_file WHERE path = '/equal-metadata.bin'",
                &[],
            )
            .await
            .expect("updated file should load");
        assert_eq!(
            current.rows()[0].values(),
            &[Value::Blob(b"after".to_vec().into()), Value::Json(metadata.into()),]
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let upsert = "INSERT INTO lix_file (path, content, lixcol_metadata) \
                      VALUES ($1, $2, $3) \
                      ON CONFLICT (path) DO UPDATE SET \
                        content = excluded.content, \
                        lixcol_metadata = excluded.lixcol_metadata";
        session
            .execute(
                upsert,
                &[
                    Value::Text("/changed-metadata.bin".to_string()),
                    Value::Blob(b"one".to_vec().into()),
                    Value::Json(json!({"version": 1}).into()),
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
                    Value::Json(json!({"version": 2}).into()),
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
                "SELECT content, lixcol_metadata \
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
    let result = session
        .execute(
            &format!(
                "SELECT lixcol_source_changes FROM lix_file_history('{commit_id}') \
                   WHERE lixcol_depth = 0 \
                   AND id = '{file_id}'"
            ),
            &[],
        )
        .await
        .expect("file history should load");
    let Some(row) = result.rows().first() else {
        return 0;
    };
    let Value::Json(source_changes) = row
        .get::<Value>("lixcol_source_changes")
        .expect("file history source changes should decode")
    else {
        panic!("file history source changes should be JSON");
    };
    let source_changes = source_changes.to_value();
    source_changes
        .as_array()
        .expect("file history source changes should be an array")
        .iter()
        .filter(|source| {
            source["schema_key"] == json!("lix_file_descriptor")
                && source["entity_pk"] == json!([file_id])
        })
        .count()
}

simulation_test!(
    lix_file_update_empty_content_on_empty_file_does_not_stage_blob_ref_tombstone,
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
                "INSERT INTO lix_file (id, path) \
                 VALUES ('616c7265-6164-892d-856d-7074792d6600', '/already-empty.bin')",
                &[],
            )
            .await
            .expect("path-only file insert should succeed");

        session
            .execute(
                "UPDATE lix_file SET content = CAST('' AS BYTEA) WHERE id = '616c7265-6164-892d-856d-7074792d6600'",
                &[],
            )
            .await
            .expect("empty data update should succeed");
        let blob_ref_changes = session
            .execute(
                "SELECT id \
                 FROM lix_change \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"616c7265-6164-892d-856d-7074792d6600\"]')",
                &[],
            )
            .await
            .expect("blob ref changes should read");
        assert_eq!(blob_ref_changes.len(), 0);
    }
);

simulation_test!(lix_file_by_branch_expands_global_rows, |sim| async move {
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
            "INSERT INTO lix_file (id, path, content, lixcol_global, lixcol_untracked) \
             VALUES ('66696c65-2d67-8c6f-8261-6c2d6f766500', '/global.txt', CAST('g' AS BYTEA), true, false)",
            &[],
        )
        .await
        .expect("global file insert should succeed");

    let result = session
        .execute(
            "SELECT id, path, lixcol_branch_id, lixcol_global, lixcol_untracked \
             FROM lix_file_by_branch \
             WHERE id = '66696c65-2d67-8c6f-8261-6c2d6f766500' \
             ORDER BY lixcol_branch_id",
            &[],
        )
        .await
        .expect("file by-branch read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("66696c65-2d67-8c6f-8261-6c2d6f766500".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text(sim.main_branch_id().to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
            vec![
                Value::Text("66696c65-2d67-8c6f-8261-6c2d6f766500".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text("ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()),
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
            .expect("global directory insert should succeed");

        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_global) \
                 VALUES ('676c6f62-616c-8d73-8861-7265642d6600', '/shared/a.txt', CAST('a' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("global file insert should reuse existing global parent directory");

        let result = session
            .execute(
                "SELECT path FROM lix_file WHERE id = '676c6f62-616c-8d73-8861-7265642d6600'",
                &[],
            )
            .await
            .expect("global file should read through active overlay");
        assert_rows_eq(result, vec![vec![Value::Text("/shared/a.txt".to_string())]]);
    }
);

simulation_test!(
    lix_file_tracked_path_insert_rejects_untracked_parent_directory,
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
                 VALUES ('6469722d-7363-8261-8463-680000000000', '/scratch', true)",
                &[],
            )
            .await
            .expect("untracked parent insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d72-8561-846d-650000000000', '/scratch/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect_err("tracked file insert must not promote an untracked parent");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let directories = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/scratch'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            directories,
            vec![vec![
                Value::Text("6469722d-7363-8261-8463-680000000000".to_string()),
                Value::Text("/scratch".to_string()),
                Value::Boolean(true),
            ]],
        );

        let files = session
            .execute(
                "SELECT id, path, directory_id, content \
                 FROM lix_file \
                 WHERE id = '66696c65-2d72-8561-846d-650000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert!(files.is_empty(), "failed insert must not create a file");
    }
);

simulation_test!(
    lix_file_untracked_path_insert_reuses_tracked_parent_directory,
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
                "INSERT INTO lix_file (id, path, content, lixcol_untracked) \
                 VALUES ('66696c65-2d64-8261-8674-000000000000', '/docs/draft.md', CAST('draft' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("untracked file insert should reuse tracked parent");

        let directories = session
            .execute(
                "SELECT id, path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path = '/docs'",
                &[],
            )
            .await
            .expect("directory read should succeed");
        assert_rows_eq(
            directories,
            vec![vec![
                Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
                Value::Text("/docs".to_string()),
                Value::Boolean(false),
            ]],
        );

        let files = session
            .execute(
                "SELECT id, path, directory_id, lixcol_untracked \
                 FROM lix_file \
                 WHERE id = '66696c65-2d64-8261-8674-000000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            files,
            vec![vec![
                Value::Text("66696c65-2d64-8261-8674-000000000000".to_string()),
                Value::Text("/docs/draft.md".to_string()),
                Value::Text("6469722d-646f-8373-8000-000000000000".to_string()),
                Value::Boolean(true),
            ]],
        );
    }
);

simulation_test!(
    lix_file_insert_on_conflict_do_update_replaces_content,
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
                 VALUES ('66696c65-2d75-8073-8572-740000000000', '/docs/upsert.md', CAST('old' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d75-8073-8572-740000000000', '/docs/upsert.md', CAST('new' AS BYTEA)) \
                 ON CONFLICT (id) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, content FROM lix_file WHERE id = '66696c65-2d75-8073-8572-740000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d75-8073-8572-740000000000".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d6e-8f74-8869-6e6700000000', '/docs/nothing.md', CAST('keep' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d6e-8f74-8869-6e6700000000', '/docs/nothing.md', CAST('ig' AS BYTEA)) \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert DO NOTHING should succeed");
        assert_eq!(result.rows_affected(), 0);

        let read = session
            .execute(
                "SELECT id, path, content FROM lix_file WHERE id = '66696c65-2d6e-8f74-8869-6e6700000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d6e-8f74-8869-6e6700000000".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d66-8265-8368-000000000000', '/docs/fresh.md', CAST('new' AS BYTEA)) \
                 ON CONFLICT (id) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("upsert on absent id should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, content FROM lix_file WHERE id = '66696c65-2d66-8265-8368-000000000000'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d66-8265-8368-000000000000".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/docs/path-fresh.md', CAST('new' AS BYTEA)) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("path upsert on absent file should insert");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT path, content FROM lix_file WHERE path = '/docs/path-fresh.md'",
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
    lix_file_insert_on_conflict_path_updates_existing_content_and_preserves_id,
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
                 VALUES ('66696c65-2d70-8174-882d-757073657200', '/docs/path-upsert.md', CAST('old' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/docs/path-upsert.md', CAST('new' AS BYTEA)) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("path upsert DO UPDATE should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, path, content FROM lix_file WHERE path = '/docs/path-upsert.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d70-8174-882d-757073657200".to_string()),
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
    }
);

simulation_test!(
    lix_file_by_branch_insert_on_conflict_path_branch_updates_existing,
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
                    "INSERT INTO lix_file_by_branch \
                     (id, path, content, lixcol_branch_id) \
                     VALUES ('66696c65-2d62-8261-8e63-682d70617400', '/docs/branch.md', CAST('old' AS BYTEA), '{branch_id}')"
                ),
                &[],
            )
            .await
            .expect("seed by-branch insert should succeed");

        let result = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (path, content, lixcol_branch_id) \
                     VALUES ('/docs/branch.md', CAST('new' AS BYTEA), '{branch_id}') \
                     ON CONFLICT (path, lixcol_branch_id) DO UPDATE SET content = excluded.content"
                ),
                &[],
            )
            .await
            .expect("by-branch path upsert should succeed");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, content FROM lix_file WHERE path = '/docs/branch.md'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d62-8261-8e63-682d70617400".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (path, content, lixcol_branch_id) \
                     VALUES ('/docs/reject.md', CAST('byte-00' AS BYTEA), '{branch_id}') \
                     ON CONFLICT (path) DO UPDATE SET content = excluded.content"
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_file (id, content) \
                 VALUES ('66696c65-2d6d-8973-8369-6e672d706100', CAST('byte-00' AS BYTEA)) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('66696c65-2d74-8261-836b-65642d636f00', '/docs/collision.md', CAST('byte-00' AS BYTEA))",
                &[],
            )
            .await
            .expect("tracked file insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_file (path, content, lixcol_untracked) \
                 VALUES ('/docs/collision.md', CAST('byte-01' AS BYTEA), true) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content, lixcol_global) \
                 VALUES ('66696c65-2d67-8c6f-8261-6c2d70617400', '/docs/global.md', CAST('old' AS BYTEA), true)",
                &[],
            )
            .await
            .expect("global seed insert should succeed");

        let result = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/docs/global.md', CAST('new' AS BYTEA)) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("path upsert should update visible global file");
        assert_eq!(result.rows_affected(), 1);

        let read = session
            .execute(
                "SELECT id, content, lixcol_global, lixcol_branch_id \
                 FROM lix_file_by_branch \
                 WHERE id = '66696c65-2d67-8c6f-8261-6c2d70617400' AND lixcol_branch_id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
                &[],
            )
            .await
            .expect("global file read should succeed");
        assert_rows_eq(
            read,
            vec![vec![
                Value::Text("66696c65-2d67-8c6f-8261-6c2d70617400".to_string()),
                Value::Blob(b"new".to_vec().into()),
                Value::Boolean(true),
                Value::Text("ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()),
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/docs/duplicate-target.md', CAST('byte-00' AS BYTEA)) \
                 ON CONFLICT (path, path) DO UPDATE SET content = excluded.content",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000002', '/docs/target.md', CAST('before' AS BYTEA))",
                &[],
            )
            .await
            .expect("transaction fixture file should insert");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('7472616e-7361-8374-896f-6e2d636f6e00', '/conflict')",
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-00000000000b', '/01950000-0000-7000-8000-00000000000b.md', CAST('byte-01' AS BYTEA))",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");

        let update = transaction
            .execute(
                "UPDATE lix_file SET content = CAST('after' AS BYTEA) \
                 WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("first path-filtered transaction update should succeed");
        assert_eq!(update.rows_affected(), 1);

        let first_read = transaction
            .execute(
                "SELECT id, content FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("transaction read-your-writes lookup should succeed");
        assert_rows_eq(
            first_read,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000002".to_string()),
                Value::Blob(b"after".to_vec().into()),
            ]],
        );

        transaction
            .execute(
                "UPDATE lix_file SET content = CAST('again' AS BYTEA) \
                 WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("repeated path-filtered transaction update should succeed");
        let repeated_read = transaction
            .execute(
                "SELECT content FROM lix_file WHERE path = '/docs/target.md'",
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
                "UPDATE lix_file SET path = '/conflict' WHERE id = '01950000-0000-7000-8000-000000000002'",
                &[],
            )
            .await
            .expect_err("conflicting transaction path update should fail");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let after_failure = transaction
            .execute(
                "SELECT id, path, content FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("cached path lookup should remain usable after a failed write");
        assert_rows_eq(
            after_failure,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000002".to_string()),
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
                "SELECT id, content FROM lix_file WHERE path = '/docs/target.md'",
                &[],
            )
            .await
            .expect("rolled-back transaction path lookup should succeed");
        assert_rows_eq(
            rolled_back,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000002".to_string()),
                Value::Blob(b"before".to_vec().into()),
            ]],
        );
        let anchor = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/01950000-0000-7000-8000-00000000000b.md'",
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();

        session
            .execute(
                "INSERT INTO lix_file_by_branch \
                 (id, path, content, lixcol_global, lixcol_branch_id) \
                 VALUES ('6c616e65-2d66-896c-8500-000000000000', '/global.md', CAST('byte-01' AS BYTEA), true, 'ffffffff-ffff-7fff-bfff-ffffffffffff')",
                &[],
            )
            .await
            .expect("global lane file should insert");
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_branch \
                     (id, path, content, lixcol_branch_id) \
                     VALUES ('6c616e65-2d66-896c-8500-000000000000', '/branch.md', CAST('byte-02' AS BYTEA), '{branch_id}')"
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000003', '/01950000-0000-7000-8000-000000000003.md', CAST('byte-03' AS BYTEA))",
                &[],
            )
            .await
            .expect("transaction descriptor anchor should stage");

        let local = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE id = '6c616e65-2d66-896c-8500-000000000000'",
                &[],
            )
            .await
            .expect("branch-local lane file should be visible");
        assert_rows_eq(
            local,
            vec![vec![
                Value::Text("6c616e65-2d66-896c-8500-000000000000".to_string()),
                Value::Text("/branch.md".to_string()),
            ]],
        );

        let deleted = transaction
            .execute(
                &format!(
                    "DELETE FROM lix_file_by_branch \
                     WHERE id = '6c616e65-2d66-896c-8500-000000000000' AND lixcol_branch_id = '{branch_id}'"
                ),
                &[],
            )
            .await
            .expect("branch-local lane tombstone should stage");
        assert_eq!(deleted.rows_affected(), 1);

        let hidden_by_tombstone = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE id = '6c616e65-2d66-896c-8500-000000000000'",
                &[],
            )
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
            .execute(
                "SELECT id, path FROM lix_file WHERE id = '6c616e65-2d66-896c-8500-000000000000'",
                &[],
            )
            .await
            .expect("branch-local lane should be restored after rollback");
        assert_rows_eq(
            restored,
            vec![vec![
                Value::Text("6c616e65-2d66-896c-8500-000000000000".to_string()),
                Value::Text("/branch.md".to_string()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_retains_other_session_snapshot,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("transaction session should open"),
            &engine,
        );
        let other_session = sim.wrap_session(
            engine
                .open_session()
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000004', '/01950000-0000-7000-8000-000000000004.md', CAST('byte-01' AS BYTEA))",
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('6f746865-722d-8365-8373-696f6e2d6600', '/other-session.md', CAST('byte-02' AS BYTEA))",
                &[],
            )
            .await
            .expect("other session file should commit");

        let still_missing = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/other-session.md'",
                &[],
            )
            .await
            .expect("transaction lookup should retain its opening snapshot");
        assert_eq!(still_missing.len(), 0);

        transaction
            .rollback()
            .await
            .expect("stale transaction rollback should succeed");

        let mut retry = session
            .begin_transaction()
            .await
            .expect("retry transaction should begin");
        let visible_after_commit = retry
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/other-session.md'",
                &[],
            )
            .await
            .expect("retry should observe the newer committed path revision");
        assert_rows_eq(
            visible_after_commit,
            vec![vec![
                Value::Text("6f746865-722d-8365-8373-696f6e2d6600".to_string()),
                Value::Text("/other-session.md".to_string()),
            ]],
        );

        retry
            .rollback()
            .await
            .expect("retry transaction rollback should succeed");
    }
);

simulation_test!(
    lix_file_transaction_path_index_cache_retains_pre_merge_snapshot,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000a".to_string()),
            name: "Path index draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should create");
        let draft = main.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000a")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('6d657267-652d-8d61-896e-2d66696c6500', '/main.md', CAST('byte-01' AS BYTEA))",
            &[],
        )
        .await
        .expect("main divergence file should insert");
        draft
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('6d657267-652d-8472-8166-742d66696c00', '/merged.md', CAST('byte-02' AS BYTEA))",
                &[],
            )
            .await
            .expect("draft merge file should insert");

        let transaction_session = main.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
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
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000005', '/01950000-0000-7000-8000-000000000005.md', CAST('byte-03' AS BYTEA))",
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
                source_branch_id: "01930000-0000-7000-8000-00000000000a".to_string(),
            })
            .await
            .expect("merge should succeed");
        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);

        let still_missing = transaction
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/merged.md'",
                &[],
            )
            .await
            .expect("transaction lookup should retain its pre-merge snapshot");
        assert_eq!(still_missing.len(), 0);

        transaction
            .rollback()
            .await
            .expect("stale transaction rollback should succeed");

        let mut retry = transaction_session
            .begin_transaction()
            .await
            .expect("retry transaction should begin");
        let visible_after_merge = retry
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/merged.md'",
                &[],
            )
            .await
            .expect("retry should observe merged reachable descriptors");
        assert_rows_eq(
            visible_after_merge,
            vec![vec![
                Value::Text("6d657267-652d-8472-8166-742d66696c00".to_string()),
                Value::Text("/merged.md".to_string()),
            ]],
        );

        retry
            .rollback()
            .await
            .expect("retry transaction rollback should succeed");
    }
);
