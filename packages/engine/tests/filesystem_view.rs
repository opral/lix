mod support;

use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_blob_text(value: &Value, expected: &str) {
    match value {
        Value::Blob(actual) => assert_eq!(actual.as_slice(), expected.as_bytes()),
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected blob value, got {other:?}"),
    }
}

fn assert_non_empty_text(value: &Value) {
    match value {
        Value::Text(actual) => assert!(
            !actual.is_empty(),
            "expected non-empty text value, got empty string"
        ),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_integer(value: &Value, expected: i64) {
    match value {
        Value::Integer(actual) => assert_eq!(*actual, expected),
        other => panic!("expected integer value {expected}, got {other:?}"),
    }
}

fn assert_boolean_like(value: &Value, expected: bool) {
    match value {
        Value::Integer(actual) => assert_eq!(*actual != 0, expected),
        Value::Text(actual) => {
            let normalized = actual.trim().to_ascii_lowercase();
            let parsed = match normalized.as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                _ => panic!("expected boolean-like text, got '{actual}'"),
            };
            assert_eq!(parsed, expected);
        }
        other => panic!("expected boolean-like value, got {other:?}"),
    }
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id as text, got {other:?}"),
    }
}

async fn active_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_version v \
             JOIN lix_active_version av ON av.version_id = v.id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version commit id as text, got {other:?}"),
    }
}

async fn insert_version(
    engine: &support::simulation_test::SimulationEngine,
    version_id: &str,
    parent_version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', '{parent_version_id}', 0, 'commit-{version_id}', 'working-{version_id}'\
         )",
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    file_view_insert_reads_inserted_blob_data,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-1', '/src/index.ts', 'ignored')",
            &[],
        )
        .await
        .unwrap();

        let result = engine
            .execute(
                "SELECT id, path, data, lixcol_schema_key FROM lix_file WHERE id = 'file-1'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.rows.clone());
        assert_eq!(result.rows.len(), 1);
        assert_text(&result.rows[0][0], "file-1");
        assert_text(&result.rows[0][1], "/src/index.ts");
        assert_blob_text(&result.rows[0][2], "ignored");
        assert_text(&result.rows[0][3], "lix_file_descriptor");
    }
);

simulation_test!(
    file_insert_autocreates_first_level_directory,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-autodir-1', '/docs/readme.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT path, name, parent_id, hidden \
                 FROM lix_directory \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(directories.rows.len(), 1);
        assert_text(&directories.rows[0][0], "/docs/");
        assert_text(&directories.rows[0][1], "docs");
        assert!(matches!(directories.rows[0][2], Value::Null));
        assert_boolean_like(&directories.rows[0][3], false);
    }
);

simulation_test!(
    file_insert_autocreates_all_ancestor_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-autodir-2', '/docs/guides/intro.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT id, path, name, parent_id, hidden \
                 FROM lix_directory \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directories.rows.len(), 2);

        assert_text(&directories.rows[0][1], "/docs/");
        assert_text(&directories.rows[0][2], "docs");
        assert!(matches!(directories.rows[0][3], Value::Null));
        assert_boolean_like(&directories.rows[0][4], false);

        assert_text(&directories.rows[1][1], "/docs/guides/");
        assert_text(&directories.rows[1][2], "guides");
        let parent_id = match &directories.rows[1][3] {
            Value::Text(value) => value.clone(),
            other => panic!("expected guides parent_id as text, got {other:?}"),
        };
        let docs_id = match &directories.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs id as text, got {other:?}"),
        };
        assert_eq!(parent_id, docs_id);
        assert_boolean_like(&directories.rows[1][4], false);
    }
);

simulation_test!(file_view_update_data_updates_file_cache, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-2', '/src/readme.md', 'ignored')",
            &[],
        )
        .await
        .unwrap();

    let before = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-2' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    let version_id = active_version_id(&engine).await;

    engine
        .execute(
            "UPDATE lix_file SET data = 'ignored-again' WHERE id = 'file-2'",
            &[],
        )
        .await
        .unwrap();

    let after = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-2' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(after.rows.clone());
    assert_eq!(before.rows, after.rows);

    let file_row = engine
        .execute("SELECT data FROM lix_file WHERE id = 'file-2'", &[])
        .await
        .unwrap();
    assert_eq!(file_row.rows.len(), 1);
    assert_blob_text(&file_row.rows[0][0], "ignored-again");

    let cache_row = engine
        .execute(
            &format!(
                "SELECT data FROM lix_internal_file_data_cache \
                 WHERE file_id = 'file-2' \
                   AND version_id = '{}'",
                version_id.replace('\'', "''")
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(cache_row.rows.len(), 1);
    assert_blob_text(&cache_row.rows[0][0], "ignored-again");
});

simulation_test!(
    file_view_update_data_expression_fails_fast,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-2-expr', '/src/readme.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "UPDATE lix_file SET data = data WHERE id = 'file-2-expr'",
                &[],
            )
            .await
            .expect_err("data expression updates should fail fast");
        assert!(
            err.message
                .contains("unsupported file data update expression"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    directory_insert_by_path_autocreates_missing_ancestors,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/guides/api/')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute("SELECT path FROM lix_directory ORDER BY path", &[])
            .await
            .unwrap();

        sim.assert_deterministic(directories.rows.clone());
        assert_eq!(directories.rows.len(), 2);
        assert_text(&directories.rows[0][0], "/guides/");
        assert_text(&directories.rows[1][0], "/guides/api/");
    }
);

simulation_test!(
    directory_delete_cascades_nested_directories_and_files,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-docs', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-guides', '/docs/guides/', 'dir-docs', 'guides')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-cascade-1', '/docs/guides/intro.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute("DELETE FROM lix_directory WHERE id = 'dir-docs'", &[])
            .await
            .unwrap();

        let directories = engine
            .execute("SELECT id FROM lix_directory ORDER BY id", &[])
            .await
            .unwrap();
        let files = engine
            .execute("SELECT id FROM lix_file ORDER BY id", &[])
            .await
            .unwrap();

        sim.assert_deterministic(directories.rows.clone());
        sim.assert_deterministic(files.rows.clone());
        assert!(directories.rows.is_empty());
        assert!(files.rows.is_empty());
    }
);

simulation_test!(
    directory_delete_with_parameterized_path_cascades_descendants,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-docs-param', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-guides-param', '/docs/guides/', 'dir-docs-param', 'guides')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-cascade-param', '/docs/guides/intro.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_directory WHERE path = $1",
                &[Value::Text("/docs/".to_string())],
            )
            .await
            .unwrap();

        let directories = engine
            .execute("SELECT id FROM lix_directory ORDER BY id", &[])
            .await
            .unwrap();
        let files = engine
            .execute("SELECT id FROM lix_file ORDER BY id", &[])
            .await
            .unwrap();

        sim.assert_deterministic(directories.rows.clone());
        sim.assert_deterministic(files.rows.clone());
        assert!(directories.rows.is_empty());
        assert!(files.rows.is_empty());
    }
);

simulation_test!(
    directory_view_crud_rewrites_to_descriptor,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('dir-1', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .unwrap();

        let inserted = engine
            .execute(
                "SELECT id, path, name, lixcol_schema_key FROM lix_directory WHERE id = 'dir-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(inserted.rows.len(), 1);
        assert_text(&inserted.rows[0][0], "dir-1");
        assert_text(&inserted.rows[0][1], "/docs/");
        assert_text(&inserted.rows[0][2], "docs");
        assert_text(&inserted.rows[0][3], "lix_directory_descriptor");

        engine
            .execute(
                "UPDATE lix_directory SET name = 'guides' WHERE id = 'dir-1'",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute("SELECT name FROM lix_directory WHERE id = 'dir-1'", &[])
            .await
            .unwrap();
        sim.assert_deterministic(updated.rows.clone());
        assert_eq!(updated.rows.len(), 1);
        assert_text(&updated.rows[0][0], "guides");

        engine
            .execute("DELETE FROM lix_directory WHERE id = 'dir-1'", &[])
            .await
            .unwrap();

        let deleted = engine
            .execute("SELECT id FROM lix_directory WHERE id = 'dir-1'", &[])
            .await
            .unwrap();
        assert!(deleted.rows.is_empty());
    }
);

simulation_test!(filesystem_file_view_rejects_id_updates, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-id-immutable', '/immutable.json', 'ignored')",
            &[],
        )
        .await
        .unwrap();

    let file_update_err = engine
        .execute(
            "UPDATE lix_file SET id = 'file-id-new' WHERE id = 'file-id-immutable'",
            &[],
        )
        .await
        .expect_err("lix_file id update should fail");
    assert!(
        file_update_err.message.contains("id is immutable"),
        "unexpected error: {}",
        file_update_err.message
    );

    let version_id = active_version_id(&engine).await;
    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('file-id-immutable-by-version', '/immutable-by-version.json', 'ignored', $1)",
            &[Value::Text(version_id.clone())],
        )
        .await
        .unwrap();

    let by_version_update_err = engine
        .execute(
            "UPDATE lix_file_by_version \
             SET id = 'file-id-new-by-version' \
             WHERE id = 'file-id-immutable-by-version' AND lixcol_version_id = $1",
            &[Value::Text(version_id)],
        )
        .await
        .expect_err("lix_file_by_version id update should fail");
    assert!(
        by_version_update_err.message.contains("id is immutable"),
        "unexpected error: {}",
        by_version_update_err.message
    );
});

simulation_test!(
    filesystem_directory_view_rejects_id_updates,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('dir-id-immutable', '/immutable-dir/', NULL, 'immutable-dir')",
                &[],
            )
            .await
            .unwrap();

        let directory_update_err = engine
            .execute(
                "UPDATE lix_directory SET id = 'dir-id-new' WHERE id = 'dir-id-immutable'",
                &[],
            )
            .await
            .expect_err("lix_directory id update should fail");
        assert!(
            directory_update_err.message.contains("id is immutable"),
            "unexpected error: {}",
            directory_update_err.message
        );

        let version_id = active_version_id(&engine).await;
        engine
        .execute(
            "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
             VALUES ('dir-id-immutable-by-version', '/immutable-dir-by-version/', NULL, 'immutable-dir-by-version', $1)",
            &[Value::Text(version_id.clone())],
        )
        .await
        .unwrap();

        let by_version_update_err = engine
            .execute(
                "UPDATE lix_directory_by_version \
             SET id = 'dir-id-new-by-version' \
             WHERE id = 'dir-id-immutable-by-version' AND lixcol_version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .expect_err("lix_directory_by_version id update should fail");
        assert!(
            by_version_update_err.message.contains("id is immutable"),
            "unexpected error: {}",
            by_version_update_err.message
        );
    }
);

simulation_test!(filesystem_history_views_reject_writes, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let file_history_err = engine
        .execute(
            "INSERT INTO lix_file_history (id, path) VALUES ('f-history', '/history.txt')",
            &[],
        )
        .await
        .expect_err("lix_file_history insert should fail");
    assert!(
        file_history_err
            .message
            .contains("lix_file_history does not support INSERT"),
        "unexpected error: {}",
        file_history_err.message
    );

    let file_history_update_err = engine
        .execute(
            "UPDATE lix_file_history SET path = '/history-renamed.txt' WHERE id = 'f-history'",
            &[],
        )
        .await
        .expect_err("lix_file_history update should fail");
    assert!(
        file_history_update_err
            .message
            .contains("lix_file_history does not support UPDATE"),
        "unexpected error: {}",
        file_history_update_err.message
    );

    let file_history_delete_err = engine
        .execute("DELETE FROM lix_file_history WHERE id = 'f-history'", &[])
        .await
        .expect_err("lix_file_history delete should fail");
    assert!(
        file_history_delete_err
            .message
            .contains("lix_file_history does not support DELETE"),
        "unexpected error: {}",
        file_history_delete_err.message
    );

    let directory_history_err = engine
        .execute(
            "DELETE FROM lix_directory_history WHERE id = 'd-history'",
            &[],
        )
        .await
        .expect_err("lix_directory_history delete should fail");
    assert!(
        directory_history_err
            .message
            .contains("lix_directory_history does not support DELETE"),
        "unexpected error: {}",
        directory_history_err.message
    );

    let directory_history_insert_err = engine
        .execute(
            "INSERT INTO lix_directory_history (id, path) VALUES ('d-history', '/history/')",
            &[],
        )
        .await
        .expect_err("lix_directory_history insert should fail");
    assert!(
        directory_history_insert_err
            .message
            .contains("lix_directory_history does not support INSERT"),
        "unexpected error: {}",
        directory_history_insert_err.message
    );

    let directory_history_update_err = engine
        .execute(
            "UPDATE lix_directory_history SET path = '/history-renamed/' WHERE id = 'd-history'",
            &[],
        )
        .await
        .expect_err("lix_directory_history update should fail");
    assert!(
        directory_history_update_err
            .message
            .contains("lix_directory_history does not support UPDATE"),
        "unexpected error: {}",
        directory_history_update_err.message
    );
});

simulation_test!(file_by_version_crud_is_version_scoped, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let version_a = active_version_id(&engine).await;
    let version_b = "fs-version-b";
    let version_a_sql = version_a.replace('\'', "''");
    let version_b_sql = version_b.replace('\'', "''");
    insert_version(&engine, version_b, &version_a).await;

    engine
        .execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config.json', 'ignored', '{version_a}')",
                version_a = version_a_sql
            ),
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config.json', 'ignored', '{version_b}')",
                version_b = version_b_sql
            ),
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            &format!(
                "UPDATE lix_file_by_version \
                 SET path = '/shared/config-renamed.json', data = 'ignored-again' \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_b}'",
                version_b = version_b_sql
            ),
            &[],
        )
        .await
        .unwrap();

    let row_a = engine
        .execute(
            &format!(
                "SELECT path, data FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_a}'",
                version_a = version_a_sql
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row_a.rows.len(), 1);
    assert_text(&row_a.rows[0][0], "/shared/config.json");
    assert_blob_text(&row_a.rows[0][1], "ignored");

    let row_b = engine
        .execute(
            &format!(
                "SELECT path, data FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_b}'",
                version_b = version_b_sql
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row_b.rows.len(), 1);
    assert_text(&row_b.rows[0][0], "/shared/config-renamed.json");
    assert_blob_text(&row_b.rows[0][1], "ignored-again");

    engine
        .execute(
            &format!(
                "DELETE FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_b}'",
                version_b = version_b_sql
            ),
            &[],
        )
        .await
        .unwrap();

    let after_delete_a = engine
        .execute(
            &format!(
                "SELECT id FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_a}'",
                version_a = version_a_sql
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(after_delete_a.rows.len(), 1);

    let after_delete_b = engine
        .execute(
            &format!(
                "SELECT id FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version_b}'",
                version_b = version_b_sql
            ),
            &[],
        )
        .await
        .unwrap();
    assert!(after_delete_b.rows.is_empty());
});

simulation_test!(file_by_version_requires_version_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();
    let version_id = active_version_id(&engine).await;

    let insert_err = engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data) \
             VALUES ('missing-version', '/missing.json', 'ignored')",
            &[],
        )
        .await
        .expect_err("insert without version should fail");
    assert!(
        insert_err.message.contains("requires lixcol_version_id")
            || insert_err.message.contains("requires version_id"),
        "unexpected error: {}",
        insert_err.message
    );

    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('needs-version-predicate', '/needs-version.json', 'ignored', $1)",
            &[Value::Text(version_id.clone())],
        )
        .await
        .unwrap();

    let update_err = engine
        .execute(
            "UPDATE lix_file_by_version \
             SET path = '/changed.json' \
             WHERE id = 'needs-version-predicate'",
            &[],
        )
        .await
        .expect_err("update without version predicate should fail");
    assert!(
        update_err
            .message
            .contains("requires a version_id predicate")
            || update_err
                .message
                .contains("requires explicit lixcol_version_id")
            || update_err.message.contains("requires version_id"),
        "unexpected error: {}",
        update_err.message
    );

    let delete_err = engine
        .execute(
            "DELETE FROM lix_file_by_version WHERE id = 'needs-version-predicate'",
            &[],
        )
        .await
        .expect_err("delete without version predicate should fail");
    assert!(
        delete_err
            .message
            .contains("requires a version_id predicate")
            || delete_err
                .message
                .contains("requires explicit lixcol_version_id")
            || delete_err.message.contains("requires version_id"),
        "unexpected error: {}",
        delete_err.message
    );

    engine
        .execute(
            "UPDATE lix_file_by_version \
             SET path = '/changed.json' \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized version predicate update should succeed");

    let after_update = engine
        .execute(
            "SELECT path FROM lix_file_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized version predicate select should succeed");
    assert_eq!(after_update.rows.len(), 1);
    assert_text(&after_update.rows[0][0], "/changed.json");

    engine
        .execute(
            "DELETE FROM lix_file_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized version predicate delete should succeed");

    let after_delete = engine
        .execute(
            "SELECT id FROM lix_file_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("post-delete parameterized select should succeed");
    assert!(after_delete.rows.is_empty());
});

simulation_test!(
    directory_by_version_crud_is_version_scoped,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let version_a = active_version_id(&engine).await;
        let version_b = "dir-version-b";
        let version_a_sql = version_a.replace('\'', "''");
        let version_b_sql = version_b.replace('\'', "''");
        insert_version(&engine, version_b, &version_a).await;

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('dir-shared', '/docs/', NULL, 'docs', '{version_a}')",
                    version_a = version_a_sql
                ),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('dir-shared', '/docs/', NULL, 'docs', '{version_b}')",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                &format!(
                    "UPDATE lix_directory_by_version \
                     SET path = '/guides/', name = 'guides' \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_b}'",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();

        let row_a = engine
            .execute(
                &format!(
                    "SELECT path, name FROM lix_directory_by_version \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_a}'",
                    version_a = version_a_sql
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row_a.rows.len(), 1);
        assert_text(&row_a.rows[0][0], "/docs/");
        assert_text(&row_a.rows[0][1], "docs");

        let row_b = engine
            .execute(
                &format!(
                    "SELECT path, name FROM lix_directory_by_version \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_b}'",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row_b.rows.len(), 1);
        assert_text(&row_b.rows[0][0], "/guides/");
        assert_text(&row_b.rows[0][1], "guides");

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_directory_by_version \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_b}'",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();

        let after_delete_a = engine
            .execute(
                &format!(
                    "SELECT id FROM lix_directory_by_version \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_a}'",
                    version_a = version_a_sql
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_delete_a.rows.len(), 1);

        let after_delete_b = engine
            .execute(
                &format!(
                    "SELECT id FROM lix_directory_by_version \
                     WHERE id = 'dir-shared' AND lixcol_version_id = '{version_b}'",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();
        assert!(after_delete_b.rows.is_empty());
    }
);

simulation_test!(directory_by_version_requires_version_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();
    let version_id = active_version_id(&engine).await;

    let insert_err = engine
        .execute(
            "INSERT INTO lix_directory_by_version (id, path, parent_id, name) \
             VALUES ('missing-version', '/missing/', NULL, 'missing')",
            &[],
        )
        .await
        .expect_err("insert without version should fail");
    assert!(
        insert_err.message.contains("requires lixcol_version_id")
            || insert_err.message.contains("requires version_id"),
        "unexpected error: {}",
        insert_err.message
    );

    engine
        .execute(
            "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
             VALUES ('needs-version-predicate', '/needs-version/', NULL, 'needs-version', $1)",
            &[Value::Text(version_id.clone())],
        )
        .await
        .unwrap();

    let update_err = engine
        .execute(
            "UPDATE lix_directory_by_version \
             SET name = 'changed' \
             WHERE id = 'needs-version-predicate'",
            &[],
        )
        .await
        .expect_err("update without version predicate should fail");
    assert!(
        update_err
            .message
            .contains("requires a version_id predicate")
            || update_err
                .message
                .contains("requires explicit lixcol_version_id")
            || update_err.message.contains("requires version_id"),
        "unexpected error: {}",
        update_err.message
    );

    let delete_err = engine
        .execute(
            "DELETE FROM lix_directory_by_version WHERE id = 'needs-version-predicate'",
            &[],
        )
        .await
        .expect_err("delete without version predicate should fail");
    assert!(
        delete_err
            .message
            .contains("requires a version_id predicate")
            || delete_err
                .message
                .contains("requires explicit lixcol_version_id")
            || delete_err.message.contains("requires version_id"),
        "unexpected error: {}",
        delete_err.message
    );

    engine
        .execute(
            "UPDATE lix_directory_by_version \
             SET path = '/changed/', name = 'changed' \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized directory version predicate update should succeed");

    let after_update = engine
        .execute(
            "SELECT path, name FROM lix_directory_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized directory select should succeed");
    assert_eq!(after_update.rows.len(), 1);
    assert_text(&after_update.rows[0][0], "/changed/");
    assert_text(&after_update.rows[0][1], "changed");

    engine
        .execute(
            "DELETE FROM lix_directory_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("parameterized directory version predicate delete should succeed");

    let after_delete = engine
        .execute(
            "SELECT id FROM lix_directory_by_version \
             WHERE id = 'needs-version-predicate' AND lixcol_version_id = $1",
            &[Value::Text(version_id.clone())],
        )
        .await
        .expect("post-delete parameterized directory select should succeed");
    assert!(after_delete.rows.is_empty());
});

simulation_test!(
    file_update_data_plus_metadata_updates_descriptor,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-mixed', '/mixed.json', 'ignored')",
            &[],
        )
        .await
        .unwrap();

        let before = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_change \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-mixed'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before.rows.len(), 1);

        engine
            .execute(
                "UPDATE lix_file \
             SET data = 'ignored-again', metadata = '{\"owner\":\"sam\"}' \
             WHERE id = 'file-mixed'",
                &[],
            )
            .await
            .unwrap();

        let after = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_change \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-mixed'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after.rows.len(), 1);
        assert_integer(&after.rows[0][0], 2);

        let file_row = engine
            .execute(
                "SELECT data, metadata FROM lix_file WHERE id = 'file-mixed'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_row.rows.len(), 1);
        assert_blob_text(&file_row.rows[0][0], "ignored-again");
        assert!(
            matches!(&file_row.rows[0][1], Value::Text(metadata) if metadata.contains("\"owner\":\"sam\"")),
            "expected metadata containing owner key, got {:?}",
            file_row.rows[0][1]
        );
    }
);

simulation_test!(
    file_insert_with_only_data_column_is_rejected,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let err = engine
            .execute("INSERT INTO lix_file (data) VALUES ('ignored')", &[])
            .await
            .expect_err("insert with only data should fail");
        assert!(
            err.message
                .contains("file insert requires at least one non-data column"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(filesystem_views_generate_default_ids, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/auto-id.txt', 'ignored')",
            &[],
        )
        .await
        .unwrap();
    let file = engine
        .execute("SELECT id FROM lix_file WHERE path = '/auto-id.txt'", &[])
        .await
        .unwrap();
    assert_eq!(file.rows.len(), 1);
    assert_non_empty_text(&file.rows[0][0]);

    engine
        .execute(
            "INSERT INTO lix_directory (path, parent_id, name) VALUES ('/auto-dir/', NULL, 'auto-dir')",
            &[],
        )
        .await
        .unwrap();
    let directory = engine
        .execute(
            "SELECT id FROM lix_directory WHERE path = '/auto-dir/'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(directory.rows.len(), 1);
    assert_non_empty_text(&directory.rows[0][0]);
});

simulation_test!(
    filesystem_hidden_defaults_and_explicit_true_writes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('hidden-file-default', '/hidden-default.json', 'ignored')",
            &[],
        )
        .await
        .unwrap();
        let file_default = engine
            .execute(
                "SELECT hidden FROM lix_file WHERE id = 'hidden-file-default'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_default.rows.len(), 1);
        assert_boolean_like(&file_default.rows[0][0], false);

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
                 VALUES ('hidden-file-true', '/hidden-true.json', 'ignored', true)",
                &[],
            )
            .await
            .unwrap();
        let file_true = engine
            .execute(
                "SELECT hidden FROM lix_file WHERE id = 'hidden-file-true'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_true.rows.len(), 1);
        assert_boolean_like(&file_true.rows[0][0], true);

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('hidden-dir-default', '/hidden-dir-default/', NULL, 'hidden-dir-default')",
                &[],
            )
            .await
            .unwrap();
        let dir_default = engine
            .execute(
                "SELECT hidden FROM lix_directory WHERE id = 'hidden-dir-default'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(dir_default.rows.len(), 1);
        assert_boolean_like(&dir_default.rows[0][0], false);

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name, hidden) \
                 VALUES ('hidden-dir-true', '/hidden-dir-true/', NULL, 'hidden-dir-true', true)",
                &[],
            )
            .await
            .unwrap();
        let dir_true = engine
            .execute(
                "SELECT hidden FROM lix_directory WHERE id = 'hidden-dir-true'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(dir_true.rows.len(), 1);
        assert_boolean_like(&dir_true.rows[0][0], true);
    }
);

simulation_test!(directory_duplicate_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('dir-dup-a', '/docs/', NULL, 'docs')",
            &[],
        )
        .await
        .unwrap();

    let err = engine
        .execute(
            "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('dir-dup-b', '/docs/', NULL, 'docs')",
            &[],
        )
        .await
        .expect_err("duplicate directory path should fail");
    assert!(
        err.message.contains("Unique constraint violation")
            || err.message.contains("already exists"),
        "unexpected error: {}",
        err.message
    );
});

simulation_test!(
    directory_duplicate_inherited_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "directory-inheritance-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-parent-docs', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .expect("parent version directory insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'",
                    child_version_id
                ),
                &[],
            )
            .await
            .expect("active version switch should succeed");

        let err = engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-child-docs', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .expect_err("duplicate inherited directory path should fail");
        assert!(
            err.message.contains("Unique constraint violation")
                || err.message.contains("already exists"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(file_duplicate_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-dup-a', '/docs/readme.md', 'ignored')",
            &[],
        )
        .await
        .unwrap();

    let err = engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-dup-b', '/docs/readme.md', 'ignored')",
            &[],
        )
        .await
        .expect_err("duplicate file path should fail");
    assert!(
        err.message.contains("Unique constraint violation")
            || err.message.contains("already exists"),
        "unexpected error: {}",
        err.message
    );
});

simulation_test!(
    file_duplicate_inherited_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-inheritance-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-parent-readme', '/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect("parent version file insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'",
                    child_version_id
                ),
                &[],
            )
            .await
            .expect("active version switch should succeed");

        let err = engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-readme', '/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect_err("duplicate inherited file path should fail");
        assert!(
            err.message.contains("Unique constraint violation")
                || err.message.contains("already exists"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    file_duplicate_inherited_nested_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-inheritance-nested-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-parent-docs-readme', '/docs/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect("parent version nested file insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'",
                    child_version_id
                ),
                &[],
            )
            .await
            .expect("active version switch should succeed");

        let err = engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-docs-readme', '/docs/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect_err("duplicate inherited nested file path should fail");
        assert!(
            err.message.contains("Unique constraint violation")
                || err.message.contains("already exists"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    file_reinsert_path_after_child_tombstone_of_inherited_file_succeeds,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-inheritance-tombstone-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-parent-readme-tombstone', '/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect("parent version file insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'",
                    child_version_id
                ),
                &[],
            )
            .await
            .expect("active version switch should succeed");

        engine
            .execute(
                "DELETE FROM lix_file WHERE id = 'file-parent-readme-tombstone'",
                &[],
            )
            .await
            .expect("child tombstone delete should succeed");

        let deleted_rows = engine
            .execute(
                "SELECT id FROM lix_file WHERE path = '/readme.md'",
                &[],
            )
            .await
            .expect("post-delete query should succeed");
        assert!(
            deleted_rows.rows.is_empty(),
            "deleted inherited file should not be visible in child version",
        );

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-readme-tombstone', '/readme.md', 'ignored')",
                &[],
            )
            .await
            .expect("re-insert after child tombstone should succeed");

        let rows = engine
            .execute(
                "SELECT id, path FROM lix_file WHERE path = '/readme.md'",
                &[],
            )
            .await
            .expect("query should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "file-child-readme-tombstone");
        assert_text(&rows.rows[0][1], "/readme.md");
    }
);

simulation_test!(
    file_path_update_to_inherited_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-inheritance-update-collision-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-parent-a', '/docs/a.md', 'ignored')",
                &[],
            )
            .await
            .expect("parent version file insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'",
                    child_version_id
                ),
                &[],
            )
            .await
            .expect("active version switch should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-b', '/docs/b.md', 'ignored')",
                &[],
            )
            .await
            .expect("child version unique file insert should succeed");

        let err = engine
            .execute(
                "UPDATE lix_file SET path = '/docs/a.md' WHERE id = 'file-child-b'",
                &[],
            )
            .await
            .expect_err("updating to inherited path should fail");
        assert!(
            err.message.contains("Unique constraint violation")
                || err.message.contains("already exists"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    file_insert_nested_path_with_missing_parent_does_not_conflict_with_same_root_filename,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('root-readme', '/readme.md', 'root')",
                &[],
            )
            .await
            .expect("root file insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('nested-readme', '/docs/readme.md', 'nested')",
                &[],
            )
            .await
            .expect(
                "nested file insert should succeed even when parent directory is auto-created and root filename matches",
            );

        let files = engine
            .execute("SELECT id, path FROM lix_file ORDER BY path", &[])
            .await
            .expect("file query should succeed");
        assert_eq!(files.rows.len(), 2);
        assert_text(&files.rows[0][0], "nested-readme");
        assert_text(&files.rows[0][1], "/docs/readme.md");
        assert_text(&files.rows[1][0], "root-readme");
        assert_text(&files.rows[1][1], "/readme.md");
    }
);

simulation_test!(file_path_update_collision_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-path-a', '/docs/a.md', 'ignored')",
            &[],
        )
        .await
        .unwrap();
    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-path-b', '/docs/b.md', 'ignored')",
            &[],
        )
        .await
        .unwrap();

    let err = engine
        .execute(
            "UPDATE lix_file SET path = '/docs/a.md' WHERE id = 'file-path-b'",
            &[],
        )
        .await
        .expect_err("path update collision should fail");
    assert!(
        err.message.contains("Unique constraint violation")
            || err.message.contains("already exists"),
        "unexpected error: {}",
        err.message
    );
});

simulation_test!(
    file_path_update_auto_creates_missing_parent_directories_in_same_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-path-auto-dir', '/a.md', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_file SET path = '/docs/guides/a.md' WHERE id = 'file-path-auto-dir'",
                &[],
            )
            .await
            .expect("path update should auto-create parent directories");

        let file_row = engine
            .execute(
                "SELECT path, lixcol_commit_id \
                 FROM lix_file \
                 WHERE id = 'file-path-auto-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_row.rows.len(), 1);
        assert_text(&file_row.rows[0][0], "/docs/guides/a.md");
        let file_commit_id = match &file_row.rows[0][1] {
            Value::Text(value) => value.clone(),
            other => panic!("expected file commit_id as text, got {other:?}"),
        };
        let version_id = active_version_id(&engine).await.replace('\'', "''");
        let file_descriptor_row = engine
            .execute(
                &format!(
                    "SELECT directory_id \
                     FROM lix_file_descriptor_by_version \
                     WHERE id = 'file-path-auto-dir' \
                       AND lixcol_version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_descriptor_row.rows.len(), 1);
        let file_directory_id = match &file_descriptor_row.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected file directory_id as text, got {other:?}"),
        };

        let directory_rows = engine
            .execute(
                "SELECT id, path, parent_id, lixcol_commit_id \
                 FROM lix_directory \
                 WHERE path IN ('/docs/', '/docs/guides/') \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directory_rows.rows.len(), 2);

        assert_text(&directory_rows.rows[0][1], "/docs/");
        let docs_directory_id = match &directory_rows.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs directory id as text, got {other:?}"),
        };
        match &directory_rows.rows[0][2] {
            Value::Null => {}
            other => panic!("expected /docs/ parent_id to be NULL, got {other:?}"),
        }
        assert_text(&directory_rows.rows[0][3], &file_commit_id);

        assert_text(&directory_rows.rows[1][1], "/docs/guides/");
        assert_text(&directory_rows.rows[1][2], &docs_directory_id);
        assert_text(&directory_rows.rows[1][3], &file_commit_id);
        let guides_directory_id = match &directory_rows.rows[1][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected guides directory id as text, got {other:?}"),
        };
        assert_eq!(file_directory_id, guides_directory_id);
    }
);

simulation_test!(
    file_path_update_with_untracked_predicate_persists_missing_parent_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_id_sql = version_id.replace('\'', "''");
        let snapshot_content = serde_json::json!({
            "id": "file-path-untracked",
            "directory_id": null,
            "name": "a",
            "extension": "md",
            "hidden": false
        })
        .to_string()
        .replace('\'', "''");
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                        entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                     ) VALUES (\
                        'file-path-untracked', 'lix_file_descriptor', 'lix', '{version_id}', 'lix', '{snapshot_content}', '1', 1\
                     )",
                    version_id = version_id_sql,
                    snapshot_content = snapshot_content
                ),
                &[],
            )
            .await
            .expect("seed untracked file descriptor should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/guides/a.md' \
                 WHERE id = 'file-path-untracked' AND lixcol_untracked = 1",
                &[],
            )
            .await
            .expect("untracked path update should succeed");

        let file_row = engine
            .execute(
                "SELECT path \
                 FROM lix_file \
                 WHERE id = 'file-path-untracked' AND lixcol_untracked = 1",
                &[],
            )
            .await
            .expect("updated untracked file row should be readable");
        assert_eq!(file_row.rows.len(), 1);
        assert_text(&file_row.rows[0][0], "/docs/guides/a.md");

        let directory_rows = engine
            .execute(
                "SELECT path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path IN ('/docs/', '/docs/guides/') \
                 ORDER BY path",
                &[],
            )
            .await
            .expect("auto-created parent directories should be readable");
        assert_eq!(directory_rows.rows.len(), 2);
        assert_text(&directory_rows.rows[0][0], "/docs/");
        assert_text(&directory_rows.rows[1][0], "/docs/guides/");
        assert_boolean_like(&directory_rows.rows[0][1], true);
        assert_boolean_like(&directory_rows.rows[1][1], true);
    }
);

simulation_test!(
    file_path_update_noop_does_not_create_parent_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/noop.json' \
                 WHERE id = 'missing-file'",
                &[],
            )
            .await
            .expect("no-op file path update should succeed");

        let directories = engine
            .execute("SELECT id FROM lix_directory WHERE path = '/docs/'", &[])
            .await
            .expect("directory lookup should succeed");
        assert!(directories.rows.is_empty());
    }
);

simulation_test!(
    file_view_exposes_active_version_commit_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-commit-id', '/commit-id.json', 'ignored')",
                &[],
            )
            .await
            .unwrap();

        let expected_commit_id = active_version_commit_id(&engine).await;
        assert!(!expected_commit_id.is_empty());

        let rows = engine
            .execute(
                "SELECT lixcol_commit_id FROM lix_file WHERE id = 'file-commit-id'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        let actual_commit_id = match &rows.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text lixcol_commit_id, got {other:?}"),
        };
        assert_eq!(actual_commit_id, expected_commit_id);
    }
);

simulation_test!(
    filesystem_current_views_follow_active_version_switch,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let version_a = active_version_id(&engine).await;
        let version_b = "filesystem-switch-version-b";
        let version_a_sql = version_a.replace('\'', "''");
        let version_b_sql = version_b.replace('\'', "''");
        insert_version(&engine, version_b, &version_a).await;

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('switch-file', '/switch-a.json', 'ignored', '{version_a}')",
                    version_a = version_a_sql,
                ),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('switch-file', '/switch-b.json', 'ignored', '{version_b}')",
                    version_b = version_b_sql,
                ),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('switch-dir', '/a/', NULL, 'a', '{version_a}')",
                    version_a = version_a_sql,
                ),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('switch-dir', '/b/', NULL, 'b', '{version_b}')",
                    version_b = version_b_sql,
                ),
                &[],
            )
            .await
            .unwrap();

        let before_file = engine
            .execute("SELECT path FROM lix_file WHERE id = 'switch-file'", &[])
            .await
            .unwrap();
        assert_eq!(before_file.rows.len(), 1);
        assert_text(&before_file.rows[0][0], "/switch-a.json");

        let before_dir = engine
            .execute(
                "SELECT path FROM lix_directory WHERE id = 'switch-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_dir.rows.len(), 1);
        assert_text(&before_dir.rows[0][0], "/a/");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{version_b}'",
                    version_b = version_b_sql
                ),
                &[],
            )
            .await
            .unwrap();

        let after_file = engine
            .execute("SELECT path FROM lix_file WHERE id = 'switch-file'", &[])
            .await
            .unwrap();
        assert_eq!(after_file.rows.len(), 1);
        assert_text(&after_file.rows[0][0], "/switch-b.json");

        let after_dir = engine
            .execute(
                "SELECT path FROM lix_directory WHERE id = 'switch-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_dir.rows.len(), 1);
        assert_text(&after_dir.rows[0][0], "/b/");
    }
);

simulation_test!(invalid_filesystem_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let file_err = engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('invalid-file', 'invalid-path', 'ignored')",
            &[],
        )
        .await
        .expect_err("invalid file path should fail");
    assert!(
        file_err.message.contains("lix_file_descriptor")
            || file_err.message.contains("does not match schema"),
        "unexpected error: {}",
        file_err.message
    );

    let directory_err = engine
        .execute(
            "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('invalid-dir', '/missing-trailing-slash', NULL, 'invalid-dir')",
            &[],
        )
        .await
        .expect_err("invalid directory path should fail");
    assert!(
        directory_err.message.contains("lix_directory_descriptor")
            || directory_err.message.contains("does not match schema"),
        "unexpected error: {}",
        directory_err.message
    );
});

simulation_test!(
    filesystem_views_expose_expected_lixcol_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, metadata) \
             VALUES ('lixcol-file', '/lixcol.json', 'ignored', '{\"tag\":\"file\"}')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('lixcol-dir', '/lixcol/', NULL, 'lixcol')",
                &[],
            )
            .await
            .unwrap();

        let file_rows = engine
            .execute(
                "SELECT \
                lixcol_entity_id, lixcol_schema_key, lixcol_file_id, lixcol_plugin_key, \
                lixcol_schema_version, lixcol_inherited_from_version_id, lixcol_change_id, \
                lixcol_created_at, lixcol_updated_at, lixcol_writer_key, lixcol_untracked, lixcol_metadata \
             FROM lix_file WHERE id = 'lixcol-file'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_rows.rows.len(), 1);
        assert_text(&file_rows.rows[0][1], "lix_file_descriptor");
        assert_text(&file_rows.rows[0][3], "lix");
        match &file_rows.rows[0][9] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected lixcol_writer_key as text/null, got {other:?}"),
        }

        let file_shape_rows = engine
            .execute(
                "SELECT directory_id, name, extension \
                 FROM lix_file \
                 WHERE id = 'lixcol-file'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_shape_rows.rows.len(), 1);
        match &file_shape_rows.rows[0][0] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected directory_id as text/null, got {other:?}"),
        }
        assert_text(&file_shape_rows.rows[0][1], "lixcol");
        assert_text(&file_shape_rows.rows[0][2], "json");

        let active_version = active_version_id(&engine).await.replace('\'', "''");
        let file_by_version_shape_rows = engine
            .execute(
                &format!(
                    "SELECT directory_id, name, extension, lixcol_writer_key \
                     FROM lix_file_by_version \
                     WHERE id = 'lixcol-file' \
                       AND lixcol_version_id = '{active_version}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_by_version_shape_rows.rows.len(), 1);
        match &file_by_version_shape_rows.rows[0][0] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected file_by_version directory_id as text/null, got {other:?}"),
        }
        assert_text(&file_by_version_shape_rows.rows[0][1], "lixcol");
        assert_text(&file_by_version_shape_rows.rows[0][2], "json");
        match &file_by_version_shape_rows.rows[0][3] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected file_by_version writer key as text/null, got {other:?}"),
        }

        let directory_rows = engine
            .execute(
                "SELECT \
                lixcol_entity_id, lixcol_schema_key, lixcol_schema_version, lixcol_inherited_from_version_id, \
                lixcol_change_id, lixcol_created_at, lixcol_updated_at, lixcol_commit_id, \
                lixcol_untracked, lixcol_metadata \
             FROM lix_directory WHERE id = 'lixcol-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directory_rows.rows.len(), 1);
        assert_text(&directory_rows.rows[0][1], "lix_directory_descriptor");
        match &directory_rows.rows[0][2] {
            Value::Text(value) => assert!(!value.is_empty(), "expected non-empty schema version"),
            other => panic!("expected lixcol_schema_version as text, got {other:?}"),
        }
        match &directory_rows.rows[0][9] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected lixcol_metadata as text/null, got {other:?}"),
        }

        let directory_by_version_rows = engine
            .execute(
                &format!(
                    "SELECT \
                    lixcol_schema_version, lixcol_metadata \
                 FROM lix_directory_by_version \
                 WHERE id = 'lixcol-dir' \
                   AND lixcol_version_id = '{active_version}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directory_by_version_rows.rows.len(), 1);
        match &directory_by_version_rows.rows[0][0] {
            Value::Text(value) => assert!(!value.is_empty(), "expected non-empty schema version"),
            other => panic!("expected by-version schema version as text, got {other:?}"),
        }
        match &directory_by_version_rows.rows[0][1] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected by-version metadata as text/null, got {other:?}"),
        }
    }
);

simulation_test!(
    file_and_file_by_version_expose_change_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('change-id-file', '/change-id.json', 'ignored')",
            &[],
        )
        .await
        .unwrap();

        let version_id = active_version_id(&engine).await;
        let file_row = engine
            .execute(
                "SELECT lixcol_change_id FROM lix_file WHERE id = 'change-id-file'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_row.rows.len(), 1);
        let file_change_id = match &file_row.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text lixcol_change_id, got {other:?}"),
        };
        assert!(!file_change_id.is_empty());

        let by_version_row = engine
            .execute(
                "SELECT lixcol_change_id FROM lix_file_by_version \
             WHERE id = 'change-id-file' AND lixcol_version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .unwrap();
        assert_eq!(by_version_row.rows.len(), 1);
        let by_version_change_id = match &by_version_row.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text lixcol_change_id, got {other:?}"),
        };
        assert_eq!(by_version_change_id, file_change_id);
    }
);

simulation_test!(file_metadata_update_changes_change_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data, metadata) \
             VALUES ('metadata-change-file', '/metadata-change.json', 'ignored', '{\"owner\":\"a\"}')",
            &[],
        )
        .await
        .unwrap();

    let before = engine
        .execute(
            "SELECT lixcol_change_id FROM lix_file WHERE id = 'metadata-change-file'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    let before_change_id = match &before.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text change id, got {other:?}"),
    };

    engine
        .execute(
            "UPDATE lix_file \
             SET metadata = '{\"owner\":\"b\"}' \
             WHERE id = 'metadata-change-file'",
            &[],
        )
        .await
        .unwrap();

    let after = engine
        .execute(
            "SELECT lixcol_change_id, metadata FROM lix_file WHERE id = 'metadata-change-file'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(after.rows.len(), 1);
    let after_change_id = match &after.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text change id, got {other:?}"),
    };
    assert_ne!(before_change_id, after_change_id);
    assert!(
        matches!(&after.rows[0][1], Value::Text(metadata) if metadata.contains("\"owner\":\"b\"")),
        "expected updated metadata payload, got {:?}",
        after.rows[0][1]
    );
});

simulation_test!(
    filesystem_history_views_project_commit_and_depth,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
             VALUES ('history-file', '/history.json', 'ignored')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_file SET path = '/history-updated.json' WHERE id = 'history-file'",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
             VALUES ('history-dir', '/history-dir/', NULL, 'history-dir')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_directory SET name = 'history-renamed' WHERE id = 'history-dir'",
                &[],
            )
            .await
            .unwrap();

        let file_history = engine
            .execute(
                "SELECT id, path, lixcol_commit_id, lixcol_depth \
             FROM lix_file_history \
             WHERE id = 'history-file' \
             ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();
        assert!(!file_history.rows.is_empty());
        assert_text(&file_history.rows[0][0], "history-file");
        assert!(matches!(file_history.rows[0][1], Value::Text(_)));
        assert!(matches!(file_history.rows[0][2], Value::Text(_)));
        assert!(matches!(file_history.rows[0][3], Value::Integer(_)));

        let directory_history = engine
            .execute(
                "SELECT id, path, lixcol_commit_id, lixcol_depth \
             FROM lix_directory_history \
             WHERE id = 'history-dir' \
             ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();
        assert!(!directory_history.rows.is_empty());
        assert_text(&directory_history.rows[0][0], "history-dir");
        assert!(matches!(directory_history.rows[0][1], Value::Text(_)));
        assert!(matches!(directory_history.rows[0][2], Value::Text(_)));
        assert!(matches!(directory_history.rows[0][3], Value::Integer(_)));
    }
);

simulation_test!(
    non_prefixed_filesystem_views_are_not_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let file_err = engine
            .execute("SELECT id FROM file", &[])
            .await
            .expect_err("non-prefixed file should not be supported");
        assert!(
            file_err.message.contains("file")
                && (file_err.message.contains("no such table")
                    || file_err.message.contains("does not exist")),
            "unexpected error: {}",
            file_err.message
        );

        let directory_err = engine
            .execute("SELECT id FROM \"directory\"", &[])
            .await
            .expect_err("non-prefixed directory should not be supported");
        assert!(
            directory_err.message.contains("directory")
                && (directory_err.message.contains("no such table")
                    || directory_err.message.contains("does not exist")),
            "unexpected error: {}",
            directory_err.message
        );
    }
);
