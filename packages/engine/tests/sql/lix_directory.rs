use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_directory_path_insert_rejects_overlong_paths_and_segments,
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
                "INSERT INTO lix_directory (id, path) VALUES ('dir-long-segment', $1)",
                &[Value::Text(format!("/{long_segment}/"))],
            )
            .await
            .expect_err("overlong directory path segment should be rejected");
        assert_eq!(segment_error.code, "LIX_ERROR_PATH_SEGMENT_TOO_LONG");

        let long_path = format!("/{}/", ["abcd"; 820].join("/"));
        let path_error = session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-long-path', $1)",
                &[Value::Text(long_path)],
            )
            .await
            .expect_err("overlong directory path should be rejected");
        assert_eq!(path_error.code, "LIX_ERROR_PATH_TOO_LONG");

        let encoded_segment_at_limit = "%61".repeat(255);
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-encoded-limit', $1)",
                &[Value::Text(format!("/{encoded_segment_at_limit}/"))],
            )
            .await
            .expect("percent-encoded segment should be measured after canonicalization");

        let encoded_segment_over_limit = "%61".repeat(256);
        let encoded_segment_error = session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-encoded-over-limit', $1)",
                &[Value::Text(format!("/{encoded_segment_over_limit}/"))],
            )
            .await
            .expect_err("overlong canonical segment should be rejected");
        assert_eq!(
            encoded_segment_error.code,
            "LIX_ERROR_PATH_SEGMENT_TOO_LONG"
        );

        let huge_path = format!("/{}/", "a".repeat(1024 * 1024));
        let huge_error = session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-huge-path', $1)",
                &[Value::Text(huge_path)],
            )
            .await
            .expect_err("huge path input should be rejected without runtime internals");
        assert_eq!(huge_error.code, "LIX_ERROR_PATH_INPUT_TOO_LONG");
    }
);

simulation_test!(
    lix_directory_path_insert_rejects_percent_encoded_forbidden_code_points,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (id, path, expected_code) in [
            (
                "dir-percent-nul",
                "/docs/%00evil/",
                "LIX_ERROR_PATH_NUL_BYTE",
            ),
            (
                "dir-percent-bidi",
                "/docs/%E2%80%AEevil/",
                "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT",
            ),
        ] {
            let error = session
                .execute(
                    &format!("INSERT INTO lix_directory (id, path) VALUES ('{id}', '{path}')"),
                    &[],
                )
                .await
                .expect_err("percent-encoded forbidden path code point should be rejected");
            assert_eq!(error.code, expected_code);
        }
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
    lix_directory_by_version_insert_duplicate_id_reports_lix_directory_by_version,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let version_id = sim.main_version_id();

        session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version \
                     (id, path, lixcol_version_id) \
                     VALUES ('same-dir', '/a/', '{version_id}')"
                ),
                &[],
            )
            .await
            .expect("first by-version directory insert should succeed");

        let error = session
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version \
                     (id, path, lixcol_version_id) \
                     VALUES ('same-dir', '/b/', '{version_id}')"
                ),
                &[],
            )
            .await
            .expect_err("duplicate by-version directory id insert should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("table 'lix_directory_by_version'")
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
    lix_directory_descriptor_writes_use_canonical_path_segment_validation,
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
            .expect("canonical directory insert should succeed");

        let nfc_collision = session
			.execute(
				"INSERT INTO lix_state (\
	             entity_id, schema_key, file_id, snapshot_content, global, untracked\
	             ) VALUES (lix_json('[\"dir-cafe-decomposed\"]'), 'lix_directory_descriptor', NULL, $1, false, false)",
				&[Value::Json(json!({
					"id": "dir-cafe-decomposed",
					"parent_id": null,
                    "name": "Cafe\u{301}",
                    "hidden": false,
                }))],
            )
            .await
            .expect_err("decomposed descriptor name should normalize before uniqueness");
        assert_eq!(nfc_collision.code, LixError::CODE_UNIQUE);

        let zero_width = session
			.execute(
				"INSERT INTO lix_state (\
	             entity_id, schema_key, file_id, snapshot_content, global, untracked\
	             ) VALUES (lix_json('[\"dir-zero-width\"]'), 'lix_directory_descriptor', NULL, $1, false, false)",
				&[Value::Json(json!({
					"id": "dir-zero-width",
					"parent_id": null,
                    "name": "zero\u{200D}width",
                    "hidden": false,
                }))],
            )
            .await
            .expect_err("descriptor name should reject zero-width characters");
        assert_eq!(zero_width.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 (lix_json('[\"dir-a\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-a\",\"parent_id\":\"dir-b\",\"name\":\"a\",\"hidden\":false}'), false, false), \
                 (lix_json('[\"dir-b\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-b\",\"parent_id\":\"dir-a\",\"name\":\"b\",\"hidden\":false}'), false, false)",
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
                 entity_id, schema_key, file_id, snapshot_content, global, untracked\
                 ) VALUES \
                 (lix_json('[\"dir-foo\"]'), 'lix_directory_descriptor', NULL, lix_json('{\"id\":\"dir-foo\",\"parent_id\":null,\"name\":\"foo\",\"hidden\":false}'), false, false)",
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
    lix_directory_allows_version_local_entry_matching_global_file_entry,
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
                "INSERT INTO lix_directory (id, path) VALUES ('version-dir-foo', '/foo/')",
                &[],
            )
            .await
            .expect("version-local directory should be a distinct storage namespace");

        let global_file = session
            .execute(
                "SELECT id, path, lixcol_version_id, lixcol_global \
                 FROM lix_file_by_version \
                 WHERE id = 'global-file-foo' AND lixcol_version_id = 'global'",
                &[],
            )
            .await
            .expect("global file should query");
        let version_directory = session
            .execute(
                "SELECT id, path \
                 FROM lix_directory \
                 WHERE id = 'version-dir-foo'",
                &[],
            )
            .await
            .expect("version directory should query");

        assert_eq!(global_file.len(), 1);
        assert_eq!(version_directory.len(), 1);
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
                    "SELECT entity_id, schema_key \
                 FROM lix_state \
                 WHERE entity_id IN (lix_json('[\"{}\"]'), lix_json('[\"{}\"]'), lix_json('[\"file-readme\"]')) \
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
