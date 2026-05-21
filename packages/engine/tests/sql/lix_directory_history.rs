use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_directory_history_reads_paths_from_commit_graph,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-dir-docs', '/docs/')",
                &[],
            )
            .await
            .expect("root directory insert should succeed");
        let first_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("first directory commit head should load")
            .expect("first directory commit head should exist");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-dir-guides', '/docs/guides/')",
                &[],
            )
            .await
            .expect("nested directory insert should succeed");
        let second_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("second directory commit head should load")
            .expect("second directory commit head should exist");

        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, parent_id, name, lixcol_start_commit_id, lixcol_depth \
                     FROM lix_directory_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id IN ('history-dir-docs', 'history-dir-guides') \
                     ORDER BY lixcol_depth, id"
                ),
                &[],
            )
            .await
            .expect("directory history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-dir-guides".to_string()),
                    Value::Text("/docs/guides/".to_string()),
                    Value::Text("history-dir-docs".to_string()),
                    Value::Text("guides".to_string()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-dir-docs".to_string()),
                    Value::Text("/docs/".to_string()),
                    Value::Null,
                    Value::Text("docs".to_string()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(1),
                ],
            ],
        );

        let snapshot_result = session
            .execute(
                &format!(
                    "SELECT lixcol_snapshot_content \
                     FROM lix_directory_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id = 'history-dir-guides' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("directory history descriptor snapshot should be selectable");
        let snapshot = snapshot_result.rows()[0]
            .get::<Value>("lixcol_snapshot_content")
            .expect("snapshot_content should be present");
        let Value::Json(snapshot) = snapshot else {
            panic!("snapshot_content should be semantic JSON, got {snapshot:?}");
        };
        assert_eq!(snapshot["parent_id"], json!("history-dir-docs"));
        assert_eq!(snapshot["name"], json!("guides"));
    }
);

simulation_test!(
    lix_directory_history_requires_start_commit_id,
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
            .execute("SELECT id FROM lix_directory_history", &[])
            .await
            .expect_err("directory history queries must provide start commit");

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
    lix_directory_history_records_recursive_delete,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-delete-docs', '/docs/')",
                &[],
            )
            .await
            .expect("root directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-delete-guides', '/docs/guides/')",
                &[],
            )
            .await
            .expect("nested directory insert should succeed");

        session
            .execute(
                "DELETE FROM lix_directory WHERE id = 'history-delete-docs'",
                &[],
            )
            .await
            .expect("recursive directory delete should succeed");
        let delete_commit_id = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("delete commit head should load")
            .expect("delete commit head should exist");

        let result = session
            .execute(
                &format!(
					"SELECT id, path, name, lixcol_snapshot_content, lixcol_schema_key, lixcol_start_commit_id, lixcol_depth \
	                 FROM lix_directory_history \
	                 WHERE lixcol_start_commit_id = '{delete_commit_id}' \
	                   AND lixcol_entity_pk IN (lix_json('[\"history-delete-docs\"]'), lix_json('[\"history-delete-guides\"]')) \
	                   AND lixcol_depth = 0 \
	                 ORDER BY lixcol_entity_pk"
				),
                &[],
            )
            .await
            .expect("directory delete history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-delete-docs".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("lix_directory_descriptor".to_string()),
                    Value::Text(delete_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-delete-guides".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("lix_directory_descriptor".to_string()),
                    Value::Text(delete_commit_id),
                    Value::Integer(0),
                ],
            ],
        );
    }
);
