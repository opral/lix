mod support;

use lix_engine::Value;
use support::simulation_test::assert_boolean_like;

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

fn parse_available_columns_from_unknown_column_error(description: &str) -> Vec<String> {
    let marker = "Available columns: ";
    let start = description
        .find(marker)
        .unwrap_or_else(|| panic!("missing available columns marker in error: {description}"));
    let tail = &description[start + marker.len()..];
    let end = tail
        .find('.')
        .unwrap_or_else(|| panic!("missing available columns terminator in error: {description}"));
    let raw = tail[..end].trim();
    if raw == "(unknown)" {
        return Vec::new();
    }
    raw.split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string)
        .collect()
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute("SELECT lix_active_version_id()", &[])
        .await
        .unwrap();
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id as text, got {other:?}"),
    }
}

async fn active_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT commit_id \
             FROM lix_version \
             WHERE id = lix_active_version_id() \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version commit id as text, got {other:?}"),
    }
}

async fn binary_blob_hash_for_file_version(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    version_id: &str,
) -> String {
    let rows = engine
        .execute(
            &format!(
                "SELECT lix_json_extract(snapshot_content, 'blob_hash') \
                 FROM lix_state_by_version \
                 WHERE file_id = '{}' \
                   AND version_id = '{}' \
                   AND schema_key = 'lix_binary_blob_ref' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                file_id, version_id
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected blob hash as text, got {other:?}"),
    }
}

async fn insert_version(
    engine: &support::simulation_test::SimulationEngine,
    version_id: &str,
    _parent_version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, hidden, commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', false, 'commit-{version_id}'\
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
        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-1', '/src/index.ts', X'69676E6F726564')", &[])
        .await
        .unwrap();

        let result = engine
            .execute(
                "SELECT id, path, data, lixcol_schema_key FROM lix_file WHERE id = 'file-1'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(result.statements[0].rows.len(), 1);
        assert_text(&result.statements[0].rows[0][0], "file-1");
        assert_text(&result.statements[0].rows[0][1], "/src/index.ts");
        assert_blob_text(&result.statements[0].rows[0][2], "ignored");
        assert_text(&result.statements[0].rows[0][3], "lix_file_descriptor");
    }
);

simulation_test!(
    file_insert_autocreates_first_level_directory,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-autodir-1', '/docs/readme.md', X'69676E6F726564')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT path, name, parent_id, hidden \
                 FROM lix_directory \
                 WHERE path = '/docs/' \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(directories.statements[0].rows.len(), 1);
        assert_text(&directories.statements[0].rows[0][0], "/docs/");
        assert_text(&directories.statements[0].rows[0][1], "docs");
        assert!(matches!(directories.statements[0].rows[0][2], Value::Null));
        assert_boolean_like(&directories.statements[0].rows[0][3], false);
    }
);

simulation_test!(
    file_insert_autocreates_all_ancestor_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-autodir-2', '/docs/guides/intro.md', X'69676E6F726564')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT id, path, name, parent_id, hidden \
                 FROM lix_directory \
                 WHERE path IN ('/docs/', '/docs/guides/') \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directories.statements[0].rows.len(), 2);

        assert_text(&directories.statements[0].rows[0][1], "/docs/");
        assert_text(&directories.statements[0].rows[0][2], "docs");
        assert!(matches!(directories.statements[0].rows[0][3], Value::Null));
        assert_boolean_like(&directories.statements[0].rows[0][4], false);

        assert_text(&directories.statements[0].rows[1][1], "/docs/guides/");
        assert_text(&directories.statements[0].rows[1][2], "guides");
        let parent_id = match &directories.statements[0].rows[1][3] {
            Value::Text(value) => value.clone(),
            other => panic!("expected guides parent_id as text, got {other:?}"),
        };
        let docs_id = match &directories.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs id as text, got {other:?}"),
        };
        assert_eq!(parent_id, docs_id);
        assert_boolean_like(&directories.statements[0].rows[1][4], false);
    }
);

simulation_test!(
    file_view_update_data_updates_binary_blob_ref_without_touching_descriptor_state,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-2', '/src/readme.md', X'69676E6F726564')", &[])
        .await
        .unwrap();

        let before = engine
            .execute(
                "SELECT COUNT(*) FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-2' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before.statements[0].rows.len(), 1);
        let version_id = active_version_id(&engine).await;
        let before_blob_hash =
            binary_blob_hash_for_file_version(&engine, "file-2", &version_id).await;

        engine
            .execute(
                "UPDATE lix_file SET data = X'69676E6F7265642D616761696E' WHERE id = 'file-2'",
                &[],
            )
            .await
            .unwrap();

        let after = engine
            .execute(
                "SELECT COUNT(*) FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-2' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(after.statements[0].rows.clone());
        assert_eq!(before.statements[0].rows, after.statements[0].rows);

        let file_row = engine
            .execute("SELECT data FROM lix_file WHERE id = 'file-2'", &[])
            .await
            .unwrap();
        assert_eq!(file_row.statements[0].rows.len(), 1);
        assert_blob_text(&file_row.statements[0].rows[0][0], "ignored-again");
        let after_blob_hash =
            binary_blob_hash_for_file_version(&engine, "file-2", &version_id).await;
        assert_ne!(before_blob_hash, after_blob_hash);
    }
);

simulation_test!(
    lix_file_update_data_with_qmark_params_persists_bytes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/update-repro.md', lix_text_encode('before'))", &[])
        .await
        .expect("seed insert should succeed");

        let seeded = engine
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/update-repro.md' LIMIT 1",
                &[],
            )
            .await
            .expect("seed read should succeed");
        assert_eq!(seeded.statements[0].rows.len(), 1);
        let file_id = match &seeded.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text id, got {other:?}"),
        };
        assert_blob_text(&seeded.statements[0].rows[0][1], "before");

        engine
            .execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[Value::Blob(b"after".to_vec()), Value::Text(file_id.clone())],
            )
            .await
            .expect("file update should succeed");

        let verify = engine
            .execute(
                &format!("SELECT data FROM lix_file WHERE id = '{}' LIMIT 1", file_id),
                &[],
            )
            .await
            .expect("verify read should succeed");
        assert_eq!(verify.statements[0].rows.len(), 1);
        assert_blob_text(&verify.statements[0].rows[0][0], "after");
    }
);

simulation_test!(
    lix_file_read_before_update_then_read_returns_updated_bytes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/update-read-consistency.md', lix_text_encode('before'))", &[])
            .await
            .expect("seed insert should succeed");

        let seeded = engine
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/update-read-consistency.md' LIMIT 1",
                &[],
            )
            .await
            .expect("seed read should succeed");
        assert_eq!(seeded.statements[0].rows.len(), 1);
        let file_id = match &seeded.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text id, got {other:?}"),
        };
        assert_blob_text(&seeded.statements[0].rows[0][1], "before");

        engine
            .execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[Value::Blob(b"after".to_vec()), Value::Text(file_id.clone())],
            )
            .await
            .expect("update should succeed");

        let read_after = engine
            .execute(
                &format!("SELECT data FROM lix_file WHERE id = '{file_id}' LIMIT 1"),
                &[],
            )
            .await
            .expect("first read after update should succeed");
        assert_eq!(read_after.statements[0].rows.len(), 1);
        assert_blob_text(&read_after.statements[0].rows[0][0], "after");

        let read_again = engine
            .execute(
                &format!("SELECT data FROM lix_file WHERE id = '{file_id}' LIMIT 1"),
                &[],
            )
            .await
            .expect("second read after update should succeed");
        assert_eq!(read_again.statements[0].rows.len(), 1);
        assert_blob_text(&read_again.statements[0].rows[0][0], "after");
    }
);

simulation_test!(
    lix_file_update_data_matched_row_is_not_silent_noop,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/update-change-id.md', lix_text_encode('before'))", &[])
        .await
        .expect("seed insert should succeed");

        let seeded = engine
            .execute(
                "SELECT id FROM lix_file WHERE path = '/update-change-id.md' LIMIT 1",
                &[],
            )
            .await
            .expect("seed read should succeed");
        assert_eq!(seeded.statements[0].rows.len(), 1);
        let file_id = match &seeded.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text id, got {other:?}"),
        };

        let before = engine
            .execute(
                &format!("SELECT data FROM lix_file WHERE id = '{file_id}' LIMIT 1"),
                &[],
            )
            .await
            .expect("before read should succeed");
        assert_eq!(before.statements[0].rows.len(), 1);
        assert_blob_text(&before.statements[0].rows[0][0], "before");

        engine
            .execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[Value::Blob(b"after".to_vec()), Value::Text(file_id.clone())],
            )
            .await
            .expect("update should succeed");

        let after = engine
            .execute(
                &format!("SELECT data FROM lix_file WHERE id = '{file_id}' LIMIT 1"),
                &[],
            )
            .await
            .expect("post-update read should succeed");
        assert_eq!(after.statements[0].rows.len(), 1);
        assert_blob_text(&after.statements[0].rows[0][0], "after");
    }
);

simulation_test!(
    file_view_update_data_expression_fails_fast,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-2-expr', '/src/readme.md', lix_text_encode('ignored'))", &[])
            .await
            .expect("seed insert should succeed");

        let err = engine
            .execute(
                "UPDATE lix_file SET data = data WHERE id = 'file-2-expr'",
                &[],
            )
            .await
            .expect_err("data expression updates should fail fast");
        assert_eq!(err.code, "LIX_ERROR_FILE_DATA_EXPECTS_BYTES");
    }
);

simulation_test!(
    file_data_insert_decode_never_null,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('qa-insert-decode', '/qa-insert-decode.md', lix_text_encode('insert-value'))",
                &[],
            )
            .await
            .expect("insert should succeed");

        let rows = engine
            .execute(
                "SELECT lix_text_decode(data) FROM lix_file WHERE id = 'qa-insert-decode' LIMIT 1",
                &[],
            )
            .await
            .expect("decode read should succeed");

        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "insert-value");
    }
);

simulation_test!(
    file_data_update_decode_never_null,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('qa-update-decode', '/qa-update-decode.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .expect("insert should succeed");
        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('after') \
                 WHERE id = 'qa-update-decode'",
                &[],
            )
            .await
            .expect("update should succeed");

        let rows = engine
            .execute(
                "SELECT lix_text_decode(data) FROM lix_file WHERE id = 'qa-update-decode' LIMIT 1",
                &[],
            )
            .await
            .expect("decode read should succeed");

        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "after");
    }
);

simulation_test!(
    file_data_cache_miss_auto_materializes_from_cas,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('qa-cache-miss', '/qa-cache-miss.md', lix_text_encode('cache-miss'))",
                &[],
            )
            .await
            .expect("insert should succeed");

        let version_id = active_version_id(&engine).await;
        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'qa-cache-miss' AND version_id = '{}'",
                    version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");

        let rows = engine
            .execute(
                "SELECT lix_text_decode(data) FROM lix_file WHERE id = 'qa-cache-miss' LIMIT 1",
                &[],
            )
            .await
            .expect("decode read should succeed");

        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "cache-miss");
    }
);

simulation_test!(
    file_data_returns_null_when_blob_store_row_missing,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('qa-manifest-only', '/qa-manifest-only.md', lix_text_encode('manifest-only'))",
                &[],
            )
            .await
            .expect("insert should succeed");

        let version_id = active_version_id(&engine).await;
        let blob_hash =
            binary_blob_hash_for_file_version(&engine, "qa-manifest-only", &version_id).await;

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'qa-manifest-only' AND version_id = '{}'",
                    version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");
        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_binary_blob_store WHERE blob_hash = '{}'",
                    blob_hash
                ),
                &[],
            )
            .await
            .expect("blob store delete should succeed");

        let rows = engine
            .execute(
                "SELECT lix_text_decode(data) FROM lix_file WHERE id = 'qa-manifest-only' LIMIT 1",
                &[],
            )
            .await
            .expect("decode read should succeed");

        assert_eq!(rows.statements[0].rows.len(), 1);
        assert!(matches!(rows.statements[0].rows[0][0], Value::Null));
    }
);

simulation_test!(
    file_data_returns_null_when_payload_is_missing,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('qa-unrecoverable', '/qa-unrecoverable.md', lix_text_encode('missing'))",
                &[],
            )
            .await
            .expect("insert should succeed");

        let version_id = active_version_id(&engine).await;
        let blob_hash =
            binary_blob_hash_for_file_version(&engine, "qa-unrecoverable", &version_id).await;

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'qa-unrecoverable' AND version_id = '{}'",
                    version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");
        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_binary_blob_store WHERE blob_hash = '{}'",
                    blob_hash
                ),
                &[],
            )
            .await
            .expect("blob store delete should succeed");
        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_binary_blob_manifest_chunk WHERE blob_hash = '{}'",
                    blob_hash
                ),
                &[],
            )
            .await
            .expect("manifest chunk delete should succeed");
        let rows = engine
            .execute(
                "SELECT lix_text_decode(data) FROM lix_file WHERE id = 'qa-unrecoverable' LIMIT 1",
                &[],
            )
            .await
            .expect("unrecoverable read should succeed with null payload");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert!(matches!(rows.statements[0].rows[0][0], Value::Null));
    }
);

simulation_test!(
    directory_insert_by_path_autocreates_missing_ancestors,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/guides/api/')",
                &[],
            )
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT path FROM lix_directory \
                 WHERE path IN ('/guides/', '/guides/api/') \
                 ORDER BY path",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(directories.statements[0].rows.clone());
        assert_eq!(directories.statements[0].rows.len(), 2);
        assert_text(&directories.statements[0].rows[0][0], "/guides/");
        assert_text(&directories.statements[0].rows[1][0], "/guides/api/");
    }
);

simulation_test!(
    directory_delete_cascades_nested_directories_and_files,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
                 VALUES ('file-cascade-1', '/docs/guides/intro.md', lix_text_encode('ignored'))",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute("DELETE FROM lix_directory WHERE id = 'dir-docs'", &[])
            .await
            .unwrap();

        let directories = engine
            .execute(
                "SELECT id FROM lix_directory \
                 WHERE path = '/docs/' OR path LIKE '/docs/%' \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        let files = engine
            .execute(
                "SELECT id FROM lix_file \
                 WHERE path LIKE '/docs/%' \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(directories.statements[0].rows.clone());
        sim.assert_deterministic(files.statements[0].rows.clone());
        assert!(directories.statements[0].rows.is_empty());
        assert!(files.statements[0].rows.is_empty());
    }
);

simulation_test!(
    directory_delete_with_parameterized_path_cascades_descendants,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
                 VALUES ('file-cascade-param', '/docs/guides/intro.md', lix_text_encode('ignored'))", &[])
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
            .execute(
                "SELECT id FROM lix_directory \
                 WHERE path = '/docs/' OR path LIKE '/docs/%' \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        let files = engine
            .execute(
                "SELECT id FROM lix_file \
                 WHERE path LIKE '/docs/%' \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(directories.statements[0].rows.clone());
        sim.assert_deterministic(files.statements[0].rows.clone());
        assert!(directories.statements[0].rows.is_empty());
        assert!(files.statements[0].rows.is_empty());
    }
);

simulation_test!(
    directory_view_crud_rewrites_to_descriptor,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
        assert_eq!(inserted.statements[0].rows.len(), 1);
        assert_text(&inserted.statements[0].rows[0][0], "dir-1");
        assert_text(&inserted.statements[0].rows[0][1], "/docs/");
        assert_text(&inserted.statements[0].rows[0][2], "docs");
        assert_text(
            &inserted.statements[0].rows[0][3],
            "lix_directory_descriptor",
        );

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
        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_text(&updated.statements[0].rows[0][0], "guides");

        engine
            .execute("DELETE FROM lix_directory WHERE id = 'dir-1'", &[])
            .await
            .unwrap();

        let deleted = engine
            .execute("SELECT id FROM lix_directory WHERE id = 'dir-1'", &[])
            .await
            .unwrap();
        assert!(deleted.statements[0].rows.is_empty());
    }
);

simulation_test!(filesystem_file_view_rejects_id_updates, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-id-immutable', '/immutable.json', lix_text_encode('ignored'))", &[])
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
        file_update_err.description.contains("id is immutable"),
        "unexpected error: {}",
        file_update_err.description
    );

    let version_id = active_version_id(&engine).await;
    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('file-id-immutable-by-version', '/immutable-by-version.json', lix_text_encode('ignored'), $1)", &[Value::Text(version_id.clone())])
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
        by_version_update_err
            .description
            .contains("id is immutable"),
        "unexpected error: {}",
        by_version_update_err.description
    );
});

simulation_test!(
    filesystem_directory_view_rejects_id_updates,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
            directory_update_err.description.contains("id is immutable"),
            "unexpected error: {}",
            directory_update_err.description
        );

        let version_id = active_version_id(&engine).await;
        engine
        .execute(
            "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
             VALUES ('dir-id-immutable-by-version', '/immutable-dir-by-version/', NULL, 'immutable-dir-by-version', $1)", &[Value::Text(version_id.clone())])
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
            by_version_update_err
                .description
                .contains("id is immutable"),
            "unexpected error: {}",
            by_version_update_err.description
        );
    }
);

simulation_test!(filesystem_history_views_reject_writes, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    let file_history_err = engine
        .execute(
            "INSERT INTO lix_file_history (id, path) VALUES ('f-history', '/history.txt')",
            &[],
        )
        .await
        .expect_err("lix_file_history insert should fail");
    assert_eq!(
        file_history_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );

    let file_history_update_err = engine
        .execute(
            "UPDATE lix_file_history SET path = '/history-renamed.txt' WHERE id = 'f-history'",
            &[],
        )
        .await
        .expect_err("lix_file_history update should fail");
    assert_eq!(
        file_history_update_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );

    let file_history_delete_err = engine
        .execute("DELETE FROM lix_file_history WHERE id = 'f-history'", &[])
        .await
        .expect_err("lix_file_history delete should fail");
    assert_eq!(
        file_history_delete_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );

    let directory_history_err = engine
        .execute(
            "DELETE FROM lix_directory_history WHERE id = 'd-history'",
            &[],
        )
        .await
        .expect_err("lix_directory_history delete should fail");
    assert_eq!(
        directory_history_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );

    let directory_history_insert_err = engine
        .execute(
            "INSERT INTO lix_directory_history (id, path) VALUES ('d-history', '/history/')",
            &[],
        )
        .await
        .expect_err("lix_directory_history insert should fail");
    assert_eq!(
        directory_history_insert_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );

    let directory_history_update_err = engine
        .execute(
            "UPDATE lix_directory_history SET path = '/history-renamed/' WHERE id = 'd-history'",
            &[],
        )
        .await
        .expect_err("lix_directory_history update should fail");
    assert_eq!(
        directory_history_update_err.code,
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
    );
});

simulation_test!(file_by_version_crud_is_version_scoped, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    let version_a = active_version_id(&engine).await;
    let version_b = "fs-version-b";
    let version_a_sql = version_a.replace('\'', "''");
    let version_b_sql = version_b.replace('\'', "''");
    insert_version(&engine, version_b, &version_a).await;

    engine
        .execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config.json', lix_text_encode('ignored'), '{version_a}')",
                version_a = version_a_sql
            ), &[])
        .await
        .unwrap();

    engine
        .execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config.json', lix_text_encode('ignored'), '{version_b}')",
                version_b = version_b_sql
            ), &[])
        .await
        .unwrap();

    engine
        .execute(
            &format!(
                "UPDATE lix_file_by_version \
                 SET path = '/shared/config-renamed.json', data = lix_text_encode('ignored-again') \
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
    assert_eq!(row_a.statements[0].rows.len(), 1);
    assert_text(&row_a.statements[0].rows[0][0], "/shared/config.json");
    assert_blob_text(&row_a.statements[0].rows[0][1], "ignored");

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
    assert_eq!(row_b.statements[0].rows.len(), 1);
    assert_text(
        &row_b.statements[0].rows[0][0],
        "/shared/config-renamed.json",
    );
    assert_blob_text(&row_b.statements[0].rows[0][1], "ignored-again");

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
    assert_eq!(after_delete_a.statements[0].rows.len(), 1);

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
    assert!(after_delete_b.statements[0].rows.is_empty());
});

simulation_test!(
    file_by_version_insert_records_append_idempotency,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-idem-insert', '/idem-insert.json', X'6869', '{version_sql}')"
                ),
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT write_lane, commit_id \
                 FROM lix_internal_commit_idempotency \
                 ORDER BY write_lane, idempotency_key",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(
            &rows.statements[0].rows[0][0],
            &format!("version:{version_id}"),
        );
        match &rows.statements[0].rows[0][1] {
            Value::Text(value) => assert!(!value.is_empty(), "commit_id should not be empty"),
            other => panic!("expected text commit_id, got {other:?}"),
        }
    }
);

simulation_test!(
    file_by_version_update_records_append_idempotency,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-idem-update', '/idem-update.json', X'6869', '{version_sql}')"
                ),
                &[],
            )
            .await
            .unwrap();

        let before_update = engine
            .execute("SELECT COUNT(*) FROM lix_internal_commit_idempotency", &[])
            .await
            .unwrap();
        assert_integer(&before_update.statements[0].rows[0][0], 1);

        engine
            .execute(
                &format!(
                    "UPDATE lix_file_by_version \
                     SET data = X'68692d75706461746564' \
                     WHERE id = 'file-idem-update' AND lixcol_version_id = '{version_sql}'"
                ),
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT write_lane, commit_id \
                 FROM lix_internal_commit_idempotency \
                 ORDER BY write_lane, idempotency_key",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(
            &rows.statements[0].rows[1][0],
            &format!("version:{version_id}"),
        );
        match &rows.statements[0].rows[1][1] {
            Value::Text(value) => assert!(!value.is_empty(), "commit_id should not be empty"),
            other => panic!("expected text commit_id, got {other:?}"),
        }
    }
);

simulation_test!(
    file_by_version_delete_records_append_idempotency,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-idem-delete', '/idem-delete.json', X'6869', '{version_sql}')"
                ),
                &[],
            )
            .await
            .unwrap();

        let before_delete = engine
            .execute("SELECT COUNT(*) FROM lix_internal_commit_idempotency", &[])
            .await
            .unwrap();
        let before_delete_count = match &before_delete.statements[0].rows[0][0] {
            Value::Integer(value) => *value,
            other => panic!("expected integer count, got {other:?}"),
        };

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_file_by_version \
                     WHERE id = 'file-idem-delete' AND lixcol_version_id = '{version_sql}'"
                ),
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT write_lane, commit_id \
                 FROM lix_internal_commit_idempotency \
                 ORDER BY write_lane, idempotency_key",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(
            rows.statements[0].rows.len(),
            usize::try_from(before_delete_count + 1).unwrap()
        );
        assert_text(
            &rows.statements[0].rows.last().unwrap()[0],
            &format!("version:{version_id}"),
        );
        match &rows.statements[0].rows.last().unwrap()[1] {
            Value::Text(value) => assert!(!value.is_empty(), "commit_id should not be empty"),
            other => panic!("expected text commit_id, got {other:?}"),
        }
    }
);

simulation_test!(
    directory_by_version_delete_records_append_idempotency,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('dir-idem-delete', '/idem-delete/', NULL, 'idem-delete', '{version_sql}')"
                ),
                &[],
            )
            .await
            .unwrap();

        let before_delete = engine
            .execute("SELECT COUNT(*) FROM lix_internal_commit_idempotency", &[])
            .await
            .unwrap();
        let before_delete_count = match &before_delete.statements[0].rows[0][0] {
            Value::Integer(value) => *value,
            other => panic!("expected integer count, got {other:?}"),
        };

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_directory_by_version \
                     WHERE id = 'dir-idem-delete' AND lixcol_version_id = '{version_sql}'"
                ),
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT write_lane, commit_id \
                 FROM lix_internal_commit_idempotency \
                 ORDER BY write_lane, idempotency_key",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(
            rows.statements[0].rows.len(),
            usize::try_from(before_delete_count + 1).unwrap()
        );
        assert_text(
            &rows.statements[0].rows.last().unwrap()[0],
            &format!("version:{version_id}"),
        );
        match &rows.statements[0].rows.last().unwrap()[1] {
            Value::Text(value) => assert!(!value.is_empty(), "commit_id should not be empty"),
            other => panic!("expected text commit_id, got {other:?}"),
        }
    }
);

simulation_test!(file_by_version_requires_version_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();
    let version_id = active_version_id(&engine).await;

    let insert_err = engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data) \
             VALUES ('missing-version', '/missing.json', lix_text_encode('ignored'))",
            &[],
        )
        .await
        .expect_err("insert without version should fail");
    assert!(
        insert_err
            .description
            .contains("requires lixcol_version_id")
            || insert_err.description.contains("requires version_id"),
        "unexpected error: {}",
        insert_err.description
    );

    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('needs-version-predicate', '/needs-version.json', lix_text_encode('ignored'), $1)", &[Value::Text(version_id.clone())])
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
            .description
            .contains("requires a version_id predicate")
            || update_err
                .description
                .contains("requires explicit lixcol_version_id")
            || update_err.description.contains("requires version_id"),
        "unexpected error: {}",
        update_err.description
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
            .description
            .contains("requires a version_id predicate")
            || delete_err
                .description
                .contains("requires explicit lixcol_version_id")
            || delete_err.description.contains("requires version_id"),
        "unexpected error: {}",
        delete_err.description
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
    assert_eq!(after_update.statements[0].rows.len(), 1);
    assert_text(&after_update.statements[0].rows[0][0], "/changed.json");

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
    assert!(after_delete.statements[0].rows.is_empty());
});

simulation_test!(
    directory_by_version_crud_is_version_scoped,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
                ), &[])
            .await
            .unwrap();

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('dir-shared', '/docs/', NULL, 'docs', '{version_b}')",
                    version_b = version_b_sql
                ), &[])
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
        assert_eq!(row_a.statements[0].rows.len(), 1);
        assert_text(&row_a.statements[0].rows[0][0], "/docs/");
        assert_text(&row_a.statements[0].rows[0][1], "docs");

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
        assert_eq!(row_b.statements[0].rows.len(), 1);
        assert_text(&row_b.statements[0].rows[0][0], "/guides/");
        assert_text(&row_b.statements[0].rows[0][1], "guides");

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
        assert_eq!(after_delete_a.statements[0].rows.len(), 1);

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
        assert!(after_delete_b.statements[0].rows.is_empty());
    }
);

simulation_test!(directory_by_version_requires_version_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();
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
        insert_err
            .description
            .contains("requires lixcol_version_id")
            || insert_err.description.contains("requires version_id"),
        "unexpected error: {}",
        insert_err.description
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
            .description
            .contains("requires a version_id predicate")
            || update_err
                .description
                .contains("requires explicit lixcol_version_id")
            || update_err.description.contains("requires version_id"),
        "unexpected error: {}",
        update_err.description
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
            .description
            .contains("requires a version_id predicate")
            || delete_err
                .description
                .contains("requires explicit lixcol_version_id")
            || delete_err.description.contains("requires version_id"),
        "unexpected error: {}",
        delete_err.description
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
    assert_eq!(after_update.statements[0].rows.len(), 1);
    assert_text(&after_update.statements[0].rows[0][0], "/changed/");
    assert_text(&after_update.statements[0].rows[0][1], "changed");

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
    assert!(after_delete.statements[0].rows.is_empty());
});

simulation_test!(
    file_update_data_plus_metadata_updates_descriptor,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('file-mixed', '/mixed.json', lix_text_encode('ignored'))", &[])
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
        assert_eq!(before.statements[0].rows.len(), 1);

        engine
            .execute(
                "UPDATE lix_file \
             SET data = lix_text_encode('ignored-again'), metadata = '{\"owner\":\"sam\"}' \
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
        assert_eq!(after.statements[0].rows.len(), 1);
        assert_integer(&after.statements[0].rows[0][0], 2);

        let file_row = engine
            .execute(
                "SELECT data, metadata FROM lix_file WHERE id = 'file-mixed'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_row.statements[0].rows.len(), 1);
        assert_blob_text(&file_row.statements[0].rows[0][0], "ignored-again");
        assert!(
            matches!(&file_row.statements[0].rows[0][1], Value::Text(metadata) if metadata.contains("\"owner\":\"sam\"")),
            "expected metadata containing owner key, got {:?}",
            file_row.statements[0].rows[0][1]
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
        engine.initialize().await.unwrap();

        let err = engine
            .execute("INSERT INTO lix_file (data) VALUES ('ignored')", &[])
            .await
            .expect_err("insert with only data should fail");
        assert!(
            err.description
                .contains("file insert requires at least one non-data column"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(file_insert_with_text_data_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    let err = engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('bytes-text-insert', '/bytes-text-insert.bin', 'HELLO WORLD')", &[])
            .await
            .expect_err("text data insert should fail");
    assert_eq!(err.code, "LIX_ERROR_FILE_DATA_EXPECTS_BYTES");
});

simulation_test!(file_insert_with_blob_hex_data_succeeds, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('bytes-hex-insert', '/bytes-hex-insert.bin', X'48454C4C4F20574F524C44')", &[])
            .await
            .expect("hex data insert should succeed");

    let row = engine
        .execute(
            "SELECT data FROM lix_file WHERE id = 'bytes-hex-insert' LIMIT 1",
            &[],
        )
        .await
        .expect("read inserted hex data should succeed");
    assert_eq!(row.statements[0].rows.len(), 1);
    assert_blob_text(&row.statements[0].rows[0][0], "HELLO WORLD");
});

simulation_test!(file_update_with_text_data_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('bytes-text-update', '/bytes-text-update.bin', X'00')", &[])
            .await
            .expect("seed insert should succeed");

    let err = engine
        .execute(
            "UPDATE lix_file SET data = 'HELLO WORLD' WHERE id = 'bytes-text-update'",
            &[],
        )
        .await
        .expect_err("text data update should fail");
    assert_eq!(err.code, "LIX_ERROR_FILE_DATA_EXPECTS_BYTES");
});

simulation_test!(file_update_with_blob_hex_data_succeeds, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('bytes-hex-update', '/bytes-hex-update.bin', X'00')", &[])
            .await
            .expect("seed insert should succeed");

    engine
        .execute(
            "UPDATE lix_file SET data = X'48454C4C4F20574F524C44' WHERE id = 'bytes-hex-update'",
            &[],
        )
        .await
        .expect("hex data update should succeed");

    let row = engine
        .execute(
            "SELECT data FROM lix_file WHERE id = 'bytes-hex-update' LIMIT 1",
            &[],
        )
        .await
        .expect("read updated hex data should succeed");
    assert_eq!(row.statements[0].rows.len(), 1);
    assert_blob_text(&row.statements[0].rows[0][0], "HELLO WORLD");
});

simulation_test!(filesystem_views_generate_default_ids, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/auto-id.txt', X'69676E6F726564')",
            &[],
        )
        .await
        .unwrap();
    let file = engine
        .execute("SELECT id FROM lix_file WHERE path = '/auto-id.txt'", &[])
        .await
        .unwrap();
    assert_eq!(file.statements[0].rows.len(), 1);
    assert_non_empty_text(&file.statements[0].rows[0][0]);

    engine
        .execute(
            "INSERT INTO lix_directory (path, parent_id, name) VALUES ('/auto-dir/', NULL, 'auto-dir')", &[])
        .await
        .unwrap();
    let directory = engine
        .execute(
            "SELECT id FROM lix_directory WHERE path = '/auto-dir/'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(directory.statements[0].rows.len(), 1);
    assert_non_empty_text(&directory.statements[0].rows[0][0]);
});

simulation_test!(
    filesystem_multi_row_insert_reuses_shared_ancestor_directory,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('multi-row-a', '/docs/a.md', X'41'), \
                 ('multi-row-b', '/docs/b.md', X'42')",
                &[],
            )
            .await
            .expect("multi-row file insert should succeed");

        let directory_rows = engine
            .execute(
                "SELECT id, path FROM lix_directory WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("directory verification query should succeed");
        assert_eq!(directory_rows.statements[0].rows.len(), 1);
        assert_text(&directory_rows.statements[0].rows[0][1], "/docs/");

        let file_rows = engine
            .execute(
                "SELECT id, path FROM lix_file \
                 WHERE id IN ('multi-row-a', 'multi-row-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("file verification query should succeed");
        assert_eq!(file_rows.statements[0].rows.len(), 2);
        assert_text(&file_rows.statements[0].rows[0][0], "multi-row-a");
        assert_text(&file_rows.statements[0].rows[1][0], "multi-row-b");
    }
);

simulation_test!(
    filesystem_multi_row_directory_insert_promotes_explicit_ancestor_row,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name, hidden) VALUES \
                 ('child-guides', '/docs/guides/', NULL, 'guides', false), \
                 ('parent-docs', '/docs/', NULL, 'docs', true)",
                &[],
            )
            .await
            .expect("multi-row directory insert should succeed");

        let parent_rows = engine
            .execute(
                "SELECT id, hidden FROM lix_directory WHERE path = '/docs/'",
                &[],
            )
            .await
            .expect("parent verification query should succeed");
        assert_eq!(parent_rows.statements[0].rows.len(), 1);
        assert_text(&parent_rows.statements[0].rows[0][0], "parent-docs");
        assert_boolean_like(&parent_rows.statements[0].rows[0][1], true);

        let child_rows = engine
            .execute(
                "SELECT id, parent_id FROM lix_directory WHERE path = '/docs/guides/'",
                &[],
            )
            .await
            .expect("child verification query should succeed");
        assert_eq!(child_rows.statements[0].rows.len(), 1);
        assert_text(&child_rows.statements[0].rows[0][0], "child-guides");
        assert_text(&child_rows.statements[0].rows[0][1], "parent-docs");
    }
);

simulation_test!(
    filesystem_file_auto_id_insert_persists_data,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/auto-id-data.txt', X'48454C4C4F20574F524C44')", &[])
            .await
            .expect("insert with auto id should succeed");

        let row = engine
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/auto-id-data.txt' LIMIT 1",
                &[],
            )
            .await
            .expect("read auto-id file should succeed");
        assert_eq!(row.statements[0].rows.len(), 1);
        assert_non_empty_text(&row.statements[0].rows[0][0]);
        assert_blob_text(&row.statements[0].rows[0][1], "HELLO WORLD");
    }
);

simulation_test!(
    filesystem_hidden_defaults_and_explicit_true_writes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('hidden-file-default', '/hidden-default.json', X'69676E6F726564')", &[])
        .await
        .unwrap();
        let file_default = engine
            .execute(
                "SELECT hidden FROM lix_file WHERE id = 'hidden-file-default'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(file_default.statements[0].rows.len(), 1);
        assert_boolean_like(&file_default.statements[0].rows[0][0], false);

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
                 VALUES ('hidden-file-true', '/hidden-true.json', X'69676E6F726564', true)",
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
        assert_eq!(file_true.statements[0].rows.len(), 1);
        assert_boolean_like(&file_true.statements[0].rows[0][0], true);

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
        assert_eq!(dir_default.statements[0].rows.len(), 1);
        assert_boolean_like(&dir_default.statements[0].rows[0][0], false);

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
        assert_eq!(dir_true.statements[0].rows.len(), 1);
        assert_boolean_like(&dir_true.statements[0].rows[0][0], true);
    }
);

simulation_test!(directory_duplicate_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

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
        err.description.contains("Unique constraint violation")
            || err.description.contains("already exists"),
        "unexpected error: {}",
        err.description
    );
});

simulation_test!(
    directory_update_hidden_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES \
                 ('dir-or-update-a', '/dir-or-update-a/', NULL, 'dir-or-update-a'), \
                 ('dir-or-update-b', '/dir-or-update-b/', NULL, 'dir-or-update-b')",
                &[],
            )
            .await
            .expect("multi-row seed directory insert should succeed");

        engine
            .execute(
                "UPDATE lix_directory \
                 SET hidden = true \
                 WHERE id = 'dir-or-update-a' OR path = '/dir-or-update-b/'",
                &[],
            )
            .await
            .expect("OR-selector directory update should succeed");

        let rows = engine
            .execute(
                "SELECT id, hidden \
                 FROM lix_directory \
                 WHERE id IN ('dir-or-update-a', 'dir-or-update-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("verification query should succeed");

        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "dir-or-update-a");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
        assert_text(&rows.statements[0].rows[1][0], "dir-or-update-b");
        assert_boolean_like(&rows.statements[0].rows[1][1], true);
    }
);

simulation_test!(
    directory_delete_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES \
                 ('dir-or-delete-a', '/dir-or-delete-a/', NULL, 'dir-or-delete-a'), \
                 ('dir-or-delete-b', '/dir-or-delete-b/', NULL, 'dir-or-delete-b')",
                &[],
            )
            .await
            .expect("multi-row seed directory insert should succeed");
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-or-delete-a-child', '/dir-or-delete-a/note.md', X'41'), \
                 ('file-or-delete-b-child', '/dir-or-delete-b/note.md', X'42')",
                &[],
            )
            .await
            .expect("multi-row seed child file insert should succeed");

        engine
            .execute(
                "DELETE FROM lix_directory \
                 WHERE id = 'dir-or-delete-a' OR path = '/dir-or-delete-b/'",
                &[],
            )
            .await
            .expect("OR-selector directory delete should succeed");

        let directory_rows = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_directory \
                 WHERE id IN ('dir-or-delete-a', 'dir-or-delete-b')",
                &[],
            )
            .await
            .expect("directory count query should succeed");
        assert_eq!(directory_rows.statements[0].rows.len(), 1);
        assert_integer(&directory_rows.statements[0].rows[0][0], 0);

        let file_rows = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file \
                 WHERE id IN ('file-or-delete-a-child', 'file-or-delete-b-child')",
                &[],
            )
            .await
            .expect("file count query should succeed");
        assert_eq!(file_rows.statements[0].rows.len(), 1);
        assert_integer(&file_rows.statements[0].rows[0][0], 0);
    }
);

simulation_test!(
    directory_batch_rename_updates_descendant_paths_from_renamed_parents,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES \
                 ('dir-batch-parent', '/docs/', NULL, 'docs'), \
                 ('dir-batch-child', '/docs/guides/', 'dir-batch-parent', 'guides')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");

        engine
            .execute(
                "UPDATE lix_directory \
                 SET name = 'renamed' \
                 WHERE id = 'dir-batch-parent' OR id = 'dir-batch-child'",
                &[],
            )
            .await
            .expect("batch directory rename should succeed");

        let rows = engine
            .execute(
                "SELECT id, path, parent_id \
                 FROM lix_directory \
                 WHERE id IN ('dir-batch-parent', 'dir-batch-child') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("verification query should succeed");
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "dir-batch-child");
        assert_text(&rows.statements[0].rows[0][1], "/renamed/renamed/");
        assert_text(&rows.statements[0].rows[0][2], "dir-batch-parent");
        assert_text(&rows.statements[0].rows[1][0], "dir-batch-parent");
        assert_text(&rows.statements[0].rows[1][1], "/renamed/");
        match &rows.statements[0].rows[1][2] {
            Value::Null => {}
            other => panic!("expected root parent_id to remain NULL, got {other:?}"),
        }
    }
);

simulation_test!(
    directory_parent_rename_does_not_rewrite_descendant_descriptors,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES \
                 ('dir-stable-parent', '/docs/', NULL, 'docs'), \
                 ('dir-stable-child', '/docs/guides/', 'dir-stable-parent', 'guides')",
                &[],
            )
            .await
            .expect("seed directory insert should succeed");
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-stable-child', '/docs/guides/note.md', X'41')",
                &[],
            )
            .await
            .expect("seed child file insert should succeed");

        let before = engine
            .execute(
                "SELECT \
                    (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent'), \
                    (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-child'), \
                    (SELECT lixcol_change_id FROM lix_file WHERE id = 'file-stable-child')",
                &[],
            )
            .await
            .expect("before query should succeed");
        assert_eq!(before.statements[0].rows.len(), 1);
        let before_parent_change_id = match &before.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected parent change id as text, got {other:?}"),
        };
        let before_child_dir_change_id = match &before.statements[0].rows[0][1] {
            Value::Text(value) => value.clone(),
            other => panic!("expected child directory change id as text, got {other:?}"),
        };
        let before_child_file_change_id = match &before.statements[0].rows[0][2] {
            Value::Text(value) => value.clone(),
            other => panic!("expected child file change id as text, got {other:?}"),
        };

        engine
            .execute(
                "UPDATE lix_directory SET name = 'renamed' WHERE id = 'dir-stable-parent'",
                &[],
            )
            .await
            .expect("parent rename should succeed");

        let after = engine
            .execute(
                "SELECT \
                    (SELECT path FROM lix_directory WHERE id = 'dir-stable-parent'), \
                    (SELECT path FROM lix_directory WHERE id = 'dir-stable-child'), \
                    (SELECT path FROM lix_file WHERE id = 'file-stable-child'), \
                    (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent'), \
                    (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-child'), \
                    (SELECT lixcol_change_id FROM lix_file WHERE id = 'file-stable-child')",
                &[],
            )
            .await
            .expect("after query should succeed");
        assert_eq!(after.statements[0].rows.len(), 1);

        assert_text(&after.statements[0].rows[0][0], "/renamed/");
        assert_text(&after.statements[0].rows[0][1], "/renamed/guides/");
        assert_text(&after.statements[0].rows[0][2], "/renamed/guides/note.md");

        let after_parent_change_id = match &after.statements[0].rows[0][3] {
            Value::Text(value) => value.clone(),
            other => panic!("expected parent change id as text, got {other:?}"),
        };
        let after_child_dir_change_id = match &after.statements[0].rows[0][4] {
            Value::Text(value) => value.clone(),
            other => panic!("expected child directory change id as text, got {other:?}"),
        };
        let after_child_file_change_id = match &after.statements[0].rows[0][5] {
            Value::Text(value) => value.clone(),
            other => panic!("expected child file change id as text, got {other:?}"),
        };

        assert_ne!(before_parent_change_id, after_parent_change_id);
        assert_eq!(before_child_dir_change_id, after_child_dir_change_id);
        assert_eq!(before_child_file_change_id, after_child_file_change_id);
    }
);

simulation_test!(
    directory_duplicate_global_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "directory-global-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                 VALUES ('dir-global-docs', '/docs/', NULL, 'docs', 'global')",
                &[],
            )
            .await
            .expect("global directory insert should succeed");

        engine
            .switch_version(child_version_id.to_string())
            .await
            .expect("active version switch should succeed");

        let err = engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('dir-child-docs', '/docs/', NULL, 'docs')",
                &[],
            )
            .await
            .expect_err("duplicate global directory path should fail");
        assert!(
            err.description.contains("Unique constraint violation")
                || err.description.contains("already exists"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(file_duplicate_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-dup-a', '/docs/readme.md', lix_text_encode('ignored'))",
            &[],
        )
        .await
        .unwrap();

    let err = engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-dup-b', '/docs/readme.md', lix_text_encode('ignored'))",
            &[],
        )
        .await
        .expect_err("duplicate file path should fail");
    assert!(
        err.description.contains("Unique constraint violation")
            || err.description.contains("already exists"),
        "unexpected error: {}",
        err.description
    );
});

simulation_test!(
    file_duplicate_global_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-global-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-global-readme', '/readme.md', lix_text_encode('ignored'), 'global')",
                &[],
            )
            .await
            .expect("global file insert should succeed");

        engine
            .switch_version(child_version_id.to_string())
            .await
            .expect("active version switch should succeed");

        let err = engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-readme', '/readme.md', lix_text_encode('ignored'))",
                &[],
            )
            .await
            .expect_err("duplicate global file path should fail");
        assert!(
            err.description.contains("Unique constraint violation")
                || err.description.contains("already exists"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    file_reinsert_path_after_child_tombstone_of_global_file_succeeds,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-global-tombstone-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-global-readme-tombstone', '/readme.md', lix_text_encode('ignored'), 'global')",
                &[],
            )
            .await
            .expect("global file insert should succeed");

        engine
            .switch_version(child_version_id.to_string())
            .await
            .expect("active version switch should succeed");

        let visible_rows = engine
            .execute(
                "SELECT id, path, lixcol_global FROM lix_file WHERE path = '/readme.md'",
                &[],
            )
            .await
            .expect("pre-delete query should succeed");
        assert_eq!(visible_rows.statements[0].rows.len(), 1);
        assert_text(
            &visible_rows.statements[0].rows[0][0],
            "file-global-readme-tombstone",
        );
        assert_text(&visible_rows.statements[0].rows[0][1], "/readme.md");
        assert_boolean_like(&visible_rows.statements[0].rows[0][2], true);

        engine
            .execute(
                "DELETE FROM lix_file WHERE id = 'file-global-readme-tombstone'",
                &[],
            )
            .await
            .expect("local tombstone against global file should succeed");

        let deleted_rows = engine
            .execute("SELECT id FROM lix_file WHERE path = '/readme.md'", &[])
            .await
            .expect("post-delete query should succeed");
        assert!(
            deleted_rows.statements[0].rows.is_empty(),
            "deleted global file should not be visible in child version",
        );

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-readme-tombstone', '/readme.md', lix_text_encode('ignored'))",
                &[],
            )
            .await
            .expect("re-insert after child tombstone should succeed");

        let rows = engine
            .execute(
                "SELECT id, path, lixcol_global FROM lix_file WHERE path = '/readme.md'",
                &[],
            )
            .await
            .expect("query should succeed");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(
            &rows.statements[0].rows[0][0],
            "file-child-readme-tombstone",
        );
        assert_text(&rows.statements[0].rows[0][1], "/readme.md");
        assert_boolean_like(&rows.statements[0].rows[0][2], false);
    }
);

simulation_test!(
    file_delete_by_path_clause_succeeds,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-delete-by-path-repro', '/delete-by-path-repro.md', X'41')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        engine
            .execute(
                "DELETE FROM lix_file WHERE path = '/delete-by-path-repro.md'",
                &[],
            )
            .await
            .expect("delete by path should succeed");

        let rows = engine
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE id = 'file-delete-by-path-repro'",
                &[],
            )
            .await
            .expect("count query should succeed");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_integer(&rows.statements[0].rows[0][0], 0);
    }
);

simulation_test!(
    file_by_version_delete_with_explicit_version_predicate_succeeds_when_no_rows_match,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "DELETE FROM lix_file_by_version \
                 WHERE lixcol_version_id = 'global' \
                   AND id = 'does-not-exist'",
                &[],
            )
            .await
            .expect(
                "delete with explicit version predicate should succeed even when no rows match",
            );
    }
);

simulation_test!(
    file_update_by_path_clause_succeeds,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-update-by-path-repro', '/update-by-path-old.md', X'41')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/update-by-path-new.md' \
                 WHERE path = '/update-by-path-old.md'",
                &[],
            )
            .await
            .expect("update by path should succeed");

        let old_rows = engine
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE path = '/update-by-path-old.md'",
                &[],
            )
            .await
            .expect("old path count query should succeed");
        assert_eq!(old_rows.statements[0].rows.len(), 1);
        assert_integer(&old_rows.statements[0].rows[0][0], 0);

        let new_rows = engine
            .execute(
                "SELECT COUNT(*) FROM lix_file WHERE path = '/update-by-path-new.md'",
                &[],
            )
            .await
            .expect("new path count query should succeed");
        assert_eq!(new_rows.statements[0].rows.len(), 1);
        assert_integer(&new_rows.statements[0].rows[0][0], 1);
    }
);

simulation_test!(
    file_update_hidden_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-or-update-a', '/or-update-a.md', X'41'), \
                 ('file-or-update-b', '/or-update-b.md', X'42')",
                &[],
            )
            .await
            .expect("multi-row seed insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET hidden = true \
                 WHERE id = 'file-or-update-a' OR path = '/or-update-b.md'",
                &[],
            )
            .await
            .expect("OR-selector update should succeed");

        let rows = engine
            .execute(
                "SELECT id, hidden \
                 FROM lix_file \
                 WHERE id IN ('file-or-update-a', 'file-or-update-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .expect("verification query should succeed");

        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "file-or-update-a");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
        assert_text(&rows.statements[0].rows[1][0], "file-or-update-b");
        assert_boolean_like(&rows.statements[0].rows[1][1], true);
    }
);

simulation_test!(
    file_delete_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-or-delete-a', '/or-delete-a.md', X'41'), \
                 ('file-or-delete-b', '/or-delete-b.md', X'42')",
                &[],
            )
            .await
            .expect("multi-row seed insert should succeed");

        engine
            .execute(
                "DELETE FROM lix_file \
                 WHERE id = 'file-or-delete-a' OR path = '/or-delete-b.md'",
                &[],
            )
            .await
            .expect("OR-selector delete should succeed");

        let rows = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_file \
                 WHERE id IN ('file-or-delete-a', 'file-or-delete-b')",
                &[],
            )
            .await
            .expect("count query should succeed");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_integer(&rows.statements[0].rows[0][0], 0);
    }
);

simulation_test!(
    file_multi_row_path_update_reports_unique_constraint,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-batch-path-a', '/batch-path-a.md', X'41'), \
                 ('file-batch-path-b', '/batch-path-b.md', X'42')",
                &[],
            )
            .await
            .expect("seed file insert should succeed");

        let error = engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/same-batch-path.md' \
                 WHERE id = 'file-batch-path-a' OR id = 'file-batch-path-b'",
                &[],
            )
            .await
            .expect_err("multi-row file path update should fail");
        assert!(
            error.description.contains("Unique constraint violation")
                && !error.description.contains("does not yet support"),
            "unexpected error: {}",
            error.description
        );
    }
);

simulation_test!(
    file_update_rejects_unknown_assignment_column,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute(
                "UPDATE lix_file SET bogus = 'x' WHERE path = '/no-file.md'",
                &[],
            )
            .await
            .expect_err("update with unknown assignment should fail");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    file_delete_rejects_unknown_where_column,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute("DELETE FROM lix_file WHERE bogus = 'x'", &[])
            .await
            .expect_err("delete with unknown predicate should fail");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    file_path_update_to_global_path_is_rejected_in_child_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let parent_version_id = active_version_id(&engine).await;
        let child_version_id = "file-global-update-collision-child";
        insert_version(&engine, child_version_id, &parent_version_id).await;

        engine
            .execute(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-global-a', '/docs/a.md', lix_text_encode('ignored'), 'global')",
                &[],
            )
            .await
            .expect("global file insert should succeed");

        engine
            .switch_version(child_version_id.to_string())
            .await
            .expect("active version switch should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-child-b', '/docs/b.md', lix_text_encode('ignored'))",
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
            .expect_err("updating to global path should fail");
        assert!(
            err.description.contains("Unique constraint violation")
                || err.description.contains("already exists"),
            "unexpected error: {}",
            err.description
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
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('root-readme', '/readme.md', lix_text_encode('root'))",
                &[],
            )
            .await
            .expect("root file insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('nested-readme', '/docs/readme.md', lix_text_encode('nested'))", &[])
            .await
            .expect(
                "nested file insert should succeed even when parent directory is auto-created and root filename matches",
            );

        let files = engine
            .execute("SELECT id, path FROM lix_file ORDER BY path", &[])
            .await
            .expect("file query should succeed");
        assert_eq!(files.statements[0].rows.len(), 2);
        assert_text(&files.statements[0].rows[0][0], "nested-readme");
        assert_text(&files.statements[0].rows[0][1], "/docs/readme.md");
        assert_text(&files.statements[0].rows[1][0], "root-readme");
        assert_text(&files.statements[0].rows[1][1], "/readme.md");
    }
);

simulation_test!(file_path_update_collision_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-path-a', '/docs/a.md', lix_text_encode('ignored'))",
            &[],
        )
        .await
        .unwrap();
    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-path-b', '/docs/b.md', lix_text_encode('ignored'))",
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
        err.description.contains("Unique constraint violation")
            || err.description.contains("already exists"),
        "unexpected error: {}",
        err.description
    );
});

simulation_test!(
    file_path_update_auto_creates_missing_parent_directories_in_same_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-path-auto-dir', '/a.md', lix_text_encode('ignored'))",
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
        assert_eq!(file_row.statements[0].rows.len(), 1);
        assert_text(&file_row.statements[0].rows[0][0], "/docs/guides/a.md");
        let file_commit_id = match &file_row.statements[0].rows[0][1] {
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
        assert_eq!(file_descriptor_row.statements[0].rows.len(), 1);
        let file_directory_id = match &file_descriptor_row.statements[0].rows[0][0] {
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
        assert_eq!(directory_rows.statements[0].rows.len(), 2);

        assert_text(&directory_rows.statements[0].rows[0][1], "/docs/");
        let docs_directory_id = match &directory_rows.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs directory id as text, got {other:?}"),
        };
        match &directory_rows.statements[0].rows[0][2] {
            Value::Null => {}
            other => panic!("expected /docs/ parent_id to be NULL, got {other:?}"),
        }
        assert_text(&directory_rows.statements[0].rows[0][3], &file_commit_id);

        assert_text(&directory_rows.statements[0].rows[1][1], "/docs/guides/");
        assert_text(&directory_rows.statements[0].rows[1][2], &docs_directory_id);
        assert_text(&directory_rows.statements[0].rows[1][3], &file_commit_id);
        let guides_directory_id = match &directory_rows.statements[0].rows[1][0] {
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
        engine.initialize().await.unwrap();

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
                    "INSERT INTO lix_state_by_version (\
                        entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                     ) VALUES (\
                        'file-path-untracked', 'lix_file_descriptor', 'lix', '{version_id}', 'lix', '{snapshot_content}', '1', true\
                     )",
                    version_id = version_id_sql,
                    snapshot_content = snapshot_content
                ), &[])
            .await
            .expect("seed untracked file descriptor should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/guides/a.md' \
                 WHERE id = 'file-path-untracked' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("untracked path update should succeed");

        let file_row = engine
            .execute(
                "SELECT path \
                 FROM lix_file \
                 WHERE id = 'file-path-untracked' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("updated untracked file row should be readable");
        assert_eq!(file_row.statements[0].rows.len(), 1);
        assert_text(&file_row.statements[0].rows[0][0], "/docs/guides/a.md");

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
        assert_eq!(directory_rows.statements[0].rows.len(), 2);
        assert_text(&directory_rows.statements[0].rows[0][0], "/docs/");
        assert_text(&directory_rows.statements[0].rows[1][0], "/docs/guides/");
        assert_boolean_like(&directory_rows.statements[0].rows[0][1], true);
        assert_boolean_like(&directory_rows.statements[0].rows[1][1], true);
    }
);

simulation_test!(
    untracked_file_write_roundtrip_persists_data,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_untracked) \
                 VALUES ('file-untracked-live', '/untracked-live.md', lix_text_encode('before'), true)",
                &[],
            )
            .await
            .expect("untracked file insert should succeed");

        let inserted = engine
            .execute(
                "SELECT lix_text_decode(data), lixcol_untracked \
                 FROM lix_file \
                 WHERE id = 'file-untracked-live' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("inserted untracked file should be readable");
        assert_eq!(inserted.statements[0].rows.len(), 1);
        assert_text(&inserted.statements[0].rows[0][0], "before");
        assert_boolean_like(&inserted.statements[0].rows[0][1], true);

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = lix_text_encode('after') \
                 WHERE id = 'file-untracked-live' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("untracked file data update should succeed");

        let updated = engine
            .execute(
                "SELECT lix_text_decode(data) \
                 FROM lix_file \
                 WHERE id = 'file-untracked-live' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("updated untracked file should be readable");
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_text(&updated.statements[0].rows[0][0], "after");

        engine
            .execute(
                "DELETE FROM lix_file \
                 WHERE id = 'file-untracked-live' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("untracked file delete should succeed");

        let deleted = engine
            .execute(
                "SELECT id \
                 FROM lix_file \
                 WHERE id = 'file-untracked-live' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("deleted untracked file should be queryable");
        assert!(deleted.statements[0].rows.is_empty());
    }
);

simulation_test!(
    file_by_version_insert_with_untracked_persists_data,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_id_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id, lixcol_untracked) \
                     VALUES ('file-untracked-by-version', '/untracked-by-version.md', lix_text_encode('versioned'), '{version_id_sql}', true)"
                ),
                &[],
            )
            .await
            .expect("untracked file_by_version insert should succeed");

        let rows = engine
            .execute(
                &format!(
                    "SELECT lix_text_decode(data), lixcol_untracked \
                     FROM lix_file_by_version \
                     WHERE id = 'file-untracked-by-version' \
                       AND lixcol_version_id = '{version_id_sql}'"
                ),
                &[],
            )
            .await
            .expect("untracked file_by_version row should be readable");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "versioned");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
    }
);

simulation_test!(
    untracked_directory_insert_autocreates_missing_parent_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-untracked-live', '/untracked/docs/guides/', true)",
                &[],
            )
            .await
            .expect("untracked directory insert should succeed");

        let rows = engine
            .execute(
                "SELECT path, lixcol_untracked \
                 FROM lix_directory \
                 WHERE path IN ('/untracked/', '/untracked/docs/', '/untracked/docs/guides/') \
                 ORDER BY path",
                &[],
            )
            .await
            .expect("untracked directory rows should be readable");
        assert_eq!(rows.statements[0].rows.len(), 3);
        assert_text(&rows.statements[0].rows[0][0], "/untracked/");
        assert_text(&rows.statements[0].rows[1][0], "/untracked/docs/");
        assert_text(&rows.statements[0].rows[2][0], "/untracked/docs/guides/");
        for row in &rows.statements[0].rows {
            assert_boolean_like(&row[1], true);
        }
    }
);

simulation_test!(
    directory_by_version_insert_with_untracked_is_visible,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let version_id_sql = version_id.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, lixcol_version_id, lixcol_untracked) \
                     VALUES ('dir-untracked-by-version', '/untracked-by-version/', '{version_id_sql}', true)"
                ),
                &[],
            )
            .await
            .expect("untracked directory_by_version insert should succeed");

        let rows = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_untracked \
                     FROM lix_directory_by_version \
                     WHERE id = 'dir-untracked-by-version' \
                       AND lixcol_version_id = '{version_id_sql}'"
                ),
                &[],
            )
            .await
            .expect("untracked directory_by_version row should be readable");
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "/untracked-by-version/");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
    }
);

simulation_test!(
    untracked_directory_delete_cascades_only_untracked_descendants,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_untracked) \
                 VALUES ('dir-untracked-delete', '/delete-untracked/root/', true)",
                &[],
            )
            .await
            .expect("seed untracked directory should succeed");
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_untracked) \
                 VALUES ('file-untracked-delete', '/delete-untracked/root/note.md', lix_text_encode('gone'), true)",
                &[],
            )
            .await
            .expect("seed untracked child file should succeed");

        engine
            .execute(
                "DELETE FROM lix_directory \
                 WHERE id = 'dir-untracked-delete' AND lixcol_untracked = true",
                &[],
            )
            .await
            .expect("untracked directory delete should succeed");

        let directory_rows = engine
            .execute(
                "SELECT path FROM lix_directory WHERE path = '/delete-untracked/root/'",
                &[],
            )
            .await
            .expect("deleted directory cascade should be queryable");
        assert!(directory_rows.statements[0].rows.is_empty());

        let file_rows = engine
            .execute(
                "SELECT id FROM lix_file WHERE id = 'file-untracked-delete'",
                &[],
            )
            .await
            .expect("deleted untracked file should be queryable");
        assert!(file_rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    tracked_directory_delete_partitions_tracked_and_untracked_descendants,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-tracked-mixed', '/tracked-mixed/')",
                &[],
            )
            .await
            .expect("tracked directory insert should succeed");
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_untracked) \
                 VALUES ('file-untracked-mixed', '/tracked-mixed/note.md', lix_text_encode('mixed'), true)",
                &[],
            )
            .await
            .expect("untracked child file insert should succeed");

        engine
            .execute(
                "DELETE FROM lix_directory WHERE id = 'dir-tracked-mixed'",
                &[],
            )
            .await
            .expect("tracked delete should partition mixed tracked/untracked cascade");

        let directory_rows = engine
            .execute(
                "SELECT id FROM lix_directory WHERE id = 'dir-tracked-mixed'",
                &[],
            )
            .await
            .expect("tracked root directory should be queryable after delete");
        assert!(directory_rows.statements[0].rows.is_empty());

        let file_rows = engine
            .execute(
                "SELECT id FROM lix_file WHERE id = 'file-untracked-mixed'",
                &[],
            )
            .await
            .expect("untracked descendant file should be queryable after delete");
        assert!(file_rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    file_path_update_noop_does_not_create_parent_directories,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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
        assert!(directories.statements[0].rows.is_empty());
    }
);

simulation_test!(
    file_path_update_ignores_materialized_row_when_untracked_tombstone_exists,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-tombstone-fast-path', '/src/tombstone.md', lix_text_encode('seed'))",
                &[],
            )
            .await
            .expect("seed file should succeed");
        let version_id = active_version_id(&engine).await.replace('\'', "''");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                        entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                     ) VALUES (\
                        'file-tombstone-fast-path', 'lix_file_descriptor', 'lix', '{version_id}', 'lix', NULL, '1', true\
                     )"
                ), &[])
            .await
            .expect("seed untracked tombstone should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/should-not-exist.md' \
                 WHERE id = 'file-tombstone-fast-path'",
                &[],
            )
            .await
            .expect("update should behave as no-op");

        let file_rows = engine
            .execute(
                "SELECT id FROM lix_file WHERE id = 'file-tombstone-fast-path'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert!(
            file_rows.statements[0].rows.is_empty(),
            "tombstoned file must stay hidden after no-op update"
        );

        let directory_rows = engine
            .execute("SELECT id FROM lix_directory WHERE path = '/docs/'", &[])
            .await
            .expect("directory read should succeed");
        assert!(
            directory_rows.statements[0].rows.is_empty(),
            "no-op update should not create parent directories for tombstoned rows"
        );
    }
);

simulation_test!(
    file_view_exposes_active_version_commit_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-commit-id', '/commit-id.json', lix_text_encode('ignored'))",
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
        assert_eq!(rows.statements[0].rows.len(), 1);
        let actual_commit_id = match &rows.statements[0].rows[0][0] {
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
        engine.initialize().await.unwrap();

        let version_a = active_version_id(&engine).await;
        let version_b = "filesystem-switch-version-b";
        let version_a_sql = version_a.replace('\'', "''");
        let version_b_sql = version_b.replace('\'', "''");
        insert_version(&engine, version_b, &version_a).await;

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('switch-file', '/switch-a.json', lix_text_encode('ignored'), '{version_a}')",
                    version_a = version_a_sql,
                ), &[])
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('switch-file', '/switch-b.json', lix_text_encode('ignored'), '{version_b}')",
                    version_b = version_b_sql,
                ), &[])
            .await
            .unwrap();

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('switch-dir', '/a/', NULL, 'a', '{version_a}')",
                    version_a = version_a_sql,
                ), &[])
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_directory_by_version (id, path, parent_id, name, lixcol_version_id) \
                     VALUES ('switch-dir', '/b/', NULL, 'b', '{version_b}')",
                    version_b = version_b_sql,
                ), &[])
            .await
            .unwrap();

        let before_file = engine
            .execute("SELECT path FROM lix_file WHERE id = 'switch-file'", &[])
            .await
            .unwrap();
        assert_eq!(before_file.statements[0].rows.len(), 1);
        assert_text(&before_file.statements[0].rows[0][0], "/switch-a.json");

        let before_dir = engine
            .execute(
                "SELECT path FROM lix_directory WHERE id = 'switch-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_dir.statements[0].rows.len(), 1);
        assert_text(&before_dir.statements[0].rows[0][0], "/a/");

        engine
            .switch_version(version_b.to_string())
            .await
            .unwrap();

        let after_file = engine
            .execute("SELECT path FROM lix_file WHERE id = 'switch-file'", &[])
            .await
            .unwrap();
        assert_eq!(after_file.statements[0].rows.len(), 1);
        assert_text(&after_file.statements[0].rows[0][0], "/switch-b.json");

        let after_dir = engine
            .execute(
                "SELECT path FROM lix_directory WHERE id = 'switch-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_dir.statements[0].rows.len(), 1);
        assert_text(&after_dir.statements[0].rows[0][0], "/b/");
    }
);

simulation_test!(invalid_filesystem_paths_are_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    let file_err = engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('invalid-file', 'invalid-path', lix_text_encode('ignored'))", &[])
        .await
        .expect_err("invalid file path should fail");
    assert!(
        file_err.description.contains("lix_file_descriptor")
            || file_err.description.contains("does not match schema")
            || file_err
                .description
                .contains("file paths must start with '/'"),
        "unexpected error: {}",
        file_err.description
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
        directory_err
            .description
            .contains("lix_directory_descriptor")
            || directory_err.description.contains("does not match schema"),
        "unexpected error: {}",
        directory_err.description
    );
});

simulation_test!(
    filesystem_views_expose_expected_lixcol_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, metadata) \
             VALUES ('lixcol-file', '/lixcol.json', lix_text_encode('ignored'), '{\"tag\":\"file\"}')", &[])
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
                lixcol_schema_version, lixcol_global, lixcol_change_id, \
                lixcol_created_at, lixcol_updated_at, lixcol_writer_key, lixcol_untracked, lixcol_metadata \
             FROM lix_file WHERE id = 'lixcol-file'", &[])
            .await
            .unwrap();
        assert_eq!(file_rows.statements[0].rows.len(), 1);
        assert_text(&file_rows.statements[0].rows[0][1], "lix_file_descriptor");
        assert_text(&file_rows.statements[0].rows[0][3], "lix");
        assert_boolean_like(&file_rows.statements[0].rows[0][5], false);
        match &file_rows.statements[0].rows[0][9] {
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
        assert_eq!(file_shape_rows.statements[0].rows.len(), 1);
        match &file_shape_rows.statements[0].rows[0][0] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected directory_id as text/null, got {other:?}"),
        }
        assert_text(&file_shape_rows.statements[0].rows[0][1], "lixcol");
        assert_text(&file_shape_rows.statements[0].rows[0][2], "json");

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
        assert_eq!(file_by_version_shape_rows.statements[0].rows.len(), 1);
        match &file_by_version_shape_rows.statements[0].rows[0][0] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected file_by_version directory_id as text/null, got {other:?}"),
        }
        assert_text(
            &file_by_version_shape_rows.statements[0].rows[0][1],
            "lixcol",
        );
        assert_text(&file_by_version_shape_rows.statements[0].rows[0][2], "json");
        match &file_by_version_shape_rows.statements[0].rows[0][3] {
            Value::Text(_) | Value::Null => {}
            other => panic!("expected file_by_version writer key as text/null, got {other:?}"),
        }

        let directory_rows = engine
            .execute(
                "SELECT \
                lixcol_entity_id, lixcol_schema_key, lixcol_schema_version, lixcol_global, \
                lixcol_change_id, lixcol_created_at, lixcol_updated_at, lixcol_commit_id, \
                lixcol_untracked, lixcol_metadata \
             FROM lix_directory WHERE id = 'lixcol-dir'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(directory_rows.statements[0].rows.len(), 1);
        assert_text(
            &directory_rows.statements[0].rows[0][1],
            "lix_directory_descriptor",
        );
        match &directory_rows.statements[0].rows[0][2] {
            Value::Text(value) => assert!(!value.is_empty(), "expected non-empty schema version"),
            other => panic!("expected lixcol_schema_version as text, got {other:?}"),
        }
        assert_boolean_like(&directory_rows.statements[0].rows[0][3], false);
        match &directory_rows.statements[0].rows[0][9] {
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
        assert_eq!(directory_by_version_rows.statements[0].rows.len(), 1);
        match &directory_by_version_rows.statements[0].rows[0][0] {
            Value::Text(value) => assert!(!value.is_empty(), "expected non-empty schema version"),
            other => panic!("expected by-version schema version as text, got {other:?}"),
        }
        match &directory_by_version_rows.statements[0].rows[0][1] {
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
        engine.initialize().await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ('change-id-file', '/change-id.json', lix_text_encode('ignored'))", &[])
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
        assert_eq!(file_row.statements[0].rows.len(), 1);
        let file_change_id = match &file_row.statements[0].rows[0][0] {
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
        assert_eq!(by_version_row.statements[0].rows.len(), 1);
        let by_version_change_id = match &by_version_row.statements[0].rows[0][0] {
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
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data, metadata) \
             VALUES ('metadata-change-file', '/metadata-change.json', lix_text_encode('ignored'), '{\"owner\":\"a\"}')", &[])
        .await
        .unwrap();

    let before = engine
        .execute(
            "SELECT lixcol_change_id FROM lix_file WHERE id = 'metadata-change-file'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.statements[0].rows.len(), 1);
    let before_change_id = match &before.statements[0].rows[0][0] {
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
    assert_eq!(after.statements[0].rows.len(), 1);
    let after_change_id = match &after.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text change id, got {other:?}"),
    };
    assert_ne!(before_change_id, after_change_id);
    assert!(
        matches!(&after.statements[0].rows[0][1], Value::Text(metadata) if metadata.contains("\"owner\":\"b\"")),
        "expected updated metadata payload, got {:?}",
        after.statements[0].rows[0][1]
    );
});

simulation_test!(
    filesystem_history_views_project_commit_and_depth,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
             VALUES ('history-file', '/history.json', lix_text_encode('ignored'))",
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
                "SELECT id, path, lixcol_commit_id, lixcol_commit_created_at, lixcol_depth \
             FROM lix_file_history \
             WHERE id = 'history-file' \
             ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();
        assert!(!file_history.statements[0].rows.is_empty());
        assert_text(&file_history.statements[0].rows[0][0], "history-file");
        assert!(matches!(
            file_history.statements[0].rows[0][1],
            Value::Text(_)
        ));
        assert!(matches!(
            file_history.statements[0].rows[0][2],
            Value::Text(_)
        ));
        assert!(matches!(
            file_history.statements[0].rows[0][3],
            Value::Text(_)
        ));
        assert!(matches!(
            file_history.statements[0].rows[0][4],
            Value::Integer(_)
        ));

        let directory_history = engine
            .execute(
                "SELECT id, path, lixcol_commit_id, lixcol_commit_created_at, lixcol_depth \
             FROM lix_directory_history \
             WHERE id = 'history-dir' \
             ORDER BY lixcol_depth ASC",
                &[],
            )
            .await
            .unwrap();
        assert!(!directory_history.statements[0].rows.is_empty());
        assert_text(&directory_history.statements[0].rows[0][0], "history-dir");
        assert!(matches!(
            directory_history.statements[0].rows[0][1],
            Value::Text(_)
        ));
        assert!(matches!(
            directory_history.statements[0].rows[0][2],
            Value::Text(_)
        ));
        assert!(matches!(
            directory_history.statements[0].rows[0][3],
            Value::Text(_)
        ));
        assert!(matches!(
            directory_history.statements[0].rows[0][4],
            Value::Integer(_)
        ));
    }
);

simulation_test!(
    directory_history_unknown_column_diagnostic_matches_select_star_columns,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) \
                 VALUES ('history-dir-columns', '/history-dir-columns/', NULL, 'history-dir-columns')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_directory SET name = 'history-dir-columns-renamed' WHERE id = 'history-dir-columns'",
                &[],
            )
            .await
            .unwrap();

        let root_commit_id = active_version_commit_id(&engine).await;
        let select_star = engine
            .execute(
                &format!(
                    "SELECT * \
                     FROM lix_directory_history \
                     WHERE id = 'history-dir-columns' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    root_commit_id
                ),
                &[],
            )
            .await
            .expect("SELECT * on lix_directory_history should succeed");
        assert_eq!(select_star.statements[0].rows.len(), 1);
        assert!(
            select_star.statements[0]
                .columns
                .contains(&"lixcol_commit_created_at".to_string()),
            "directory history select-star columns should expose lixcol_commit_created_at"
        );

        let error = engine
            .execute(
                &format!(
                    "SELECT bogus \
                     FROM lix_directory_history \
                     WHERE id = 'history-dir-columns' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    root_commit_id
                ),
                &[],
            )
            .await
            .expect_err("unknown directory history column read should fail");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");

        let available_columns =
            parse_available_columns_from_unknown_column_error(&error.description);
        assert_eq!(
            available_columns, select_star.statements[0].columns,
            "unknown-column diagnostics should list the same columns as `SELECT *` on lix_directory_history. error: {}",
            error.description
        );
    }
);

simulation_test!(
    non_prefixed_filesystem_views_are_not_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let file_err = engine
            .execute("SELECT id FROM file", &[])
            .await
            .expect_err("non-prefixed file should not be supported");
        assert_eq!(file_err.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");

        let directory_err = engine
            .execute("SELECT id FROM \"directory\"", &[])
            .await
            .expect_err("non-prefixed directory should not be supported");
        assert_eq!(directory_err.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
    }
);
