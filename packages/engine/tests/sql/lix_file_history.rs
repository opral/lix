use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_file_history_reads_path_and_data_from_commit_graph,
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
                 VALUES ('history-file', '/docs/guides/readme.md', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first file commit head should load")
            .expect("first file commit head should exist");

        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/readme-renamed.md' \
                 WHERE id = 'history-file'",
                &[],
            )
            .await
            .expect("file path update should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second file commit head should load")
            .expect("second file commit head should exist");

        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, name, data, lixcol_start_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id = 'history-file' \
                       AND path LIKE '/docs/%' \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/readme-renamed.md".to_string()),
                    Value::Text("readme-renamed.md".to_string()),
                    Value::Blob(b"hello".to_vec()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/guides/readme.md".to_string()),
                    Value::Text("readme.md".to_string()),
                    Value::Blob(b"hello".to_vec()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(1),
                ],
            ],
        );

        let snapshot_result = session
            .execute(
                &format!(
                    "SELECT lixcol_snapshot_content \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id = 'history-file' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file history descriptor snapshot should be selectable");
        let snapshot = snapshot_result.rows()[0]
            .get::<Value>("lixcol_snapshot_content")
            .expect("snapshot_content should be present");
        let Value::Json(snapshot) = snapshot else {
            panic!("snapshot_content should be semantic JSON, got {snapshot:?}");
        };
        assert_eq!(snapshot["name"], json!("readme-renamed.md"));

        let result = session
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{first_commit_id}' \
                       AND path LIKE '/missing/%'"
                ),
                &[],
            )
            .await
            .expect("file history should route start commit and leave path LIKE as residual");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    lix_file_history_requires_start_commit_id,
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
            .execute("SELECT id FROM lix_file_history", &[])
            .await
            .expect_err("file history queries must provide start commit");

        assert!(
            error
                .to_string()
                .contains("requires a lixcol_start_commit_id filter"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("WHERE lixcol_start_commit_id")),
            "unexpected error: {error}"
        );
    }
);

simulation_test!(
    lix_file_history_exposes_file_descriptor_schema_key,
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
                 VALUES ('history-file-blob-filter', '/docs/blob-filter.txt', X'626C6F62')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET data = X'626C6F6232' \
                 WHERE id = 'history-file-blob-filter'",
                &[],
            )
            .await
            .expect("file data update should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, data, lixcol_schema_key \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND lixcol_schema_key = 'lix_file_descriptor' \
                       AND id = 'history-file-blob-filter' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file-descriptor-filtered file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("history-file-blob-filter".to_string()),
                Value::Text("/docs/blob-filter.txt".to_string()),
                Value::Blob(b"blob2".to_vec()),
                Value::Text("lix_file_descriptor".to_string()),
            ]],
        );

        let blob_schema_result = session
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND lixcol_schema_key = 'lix_binary_blob_ref' \
                       AND id = 'history-file-blob-filter'"
                ),
                &[],
            )
            .await
            .expect("blob-ref-filtered file history read should succeed");
        assert_rows_eq(blob_schema_result, Vec::<Vec<Value>>::new());
    }
);
