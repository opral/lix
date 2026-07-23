use std::collections::BTreeSet;

use lix_engine::{CreateBranchOptions, MergeBranchOptions, Value};
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
            .load_branch_head_commit_id(sim.main_branch_id())
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second directory commit head should load")
            .expect("second directory commit head should exist");

        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, parent_id, name, lixcol_as_of_commit_id, lixcol_depth \
                     FROM lix_directory_history \
                     WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
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

        let source_changes_result = session
            .execute(
                &format!(
                    "SELECT lixcol_source_changes \
                     FROM lix_directory_history \
                     WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
                       AND id = 'history-dir-guides' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("directory history source changes should be selectable");
        let source_changes = source_changes_result.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .expect("source_changes should be present");
        let Value::Json(source_changes) = source_changes else {
            panic!("source_changes should be semantic JSON, got {source_changes:?}");
        };
        assert_eq!(source_changes.as_array().map(Vec::len), Some(1));
        assert_eq!(
            source_changes[0]["schema_key"],
            json!("lix_directory_descriptor")
        );
        assert_eq!(
            source_changes[0]["snapshot_content"]["parent_id"],
            json!("history-dir-docs")
        );
        assert_eq!(
            source_changes[0]["snapshot_content"]["name"],
            json!("guides")
        );
        assert_eq!(
            source_changes[0]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "created_at",
                "entity_pk",
                "file_id",
                "id",
                "metadata",
                "origin_key",
                "schema_key",
                "snapshot_content",
            ]
        );
    }
);

simulation_test!(
    lix_directory_history_requires_as_of_commit_id,
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
            .expect_err("directory history queries must provide an as-of commit");

        assert!(
            error
                .to_string()
                .contains("requires a lixcol_as_of_commit_id filter"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("WHERE lixcol_as_of_commit_id")),
            "unexpected error: {error}"
        );
    }
);

simulation_test!(
    lix_directory_history_preserves_equal_depth_siblings_in_a_diamond,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_directory (id, path) VALUES ('diamond-dir', '/before/')",
            &[],
        )
        .await
        .expect("base directory should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("diamond-dir-draft".to_string()),
            name: "Diamond directory draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session("diamond-dir-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_directory SET name = 'same' WHERE id = 'diamond-dir'",
            &[],
        )
        .await
        .expect("main rename should succeed");
        let main_sibling = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main sibling should load")
            .expect("main sibling should exist");
        draft
            .execute(
                "UPDATE lix_directory SET name = 'same' WHERE id = 'diamond-dir'",
                &[],
            )
            .await
            .expect("draft rename should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("diamond-dir-draft")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "diamond-dir-draft".to_string(),
            })
            .await
            .expect("convergent sibling renames should merge");
        let merge_commit_id = receipt
            .created_merge_commit_id
            .expect("convergent sibling renames should create an empty merge commit");

        let rows = main
            .execute(
                &format!(
                    "SELECT path, lixcol_observed_commit_id, lixcol_depth \
                     FROM lix_directory_history \
                     WHERE lixcol_as_of_commit_id = '{merge_commit_id}' \
                       AND id = 'diamond-dir' \
                       AND lixcol_depth = 1 \
                     ORDER BY lixcol_observed_commit_id"
                ),
                &[],
            )
            .await
            .expect("diamond directory history should load");

        assert_eq!(rows.len(), 2, "both equal-depth sibling revisions survive");
        let mut observed = rows
            .rows()
            .iter()
            .map(|row| {
                assert_eq!(
                    row.get::<Value>("path").expect("path should decode"),
                    Value::Text("/same/".to_string())
                );
                assert_eq!(
                    row.get::<Value>("lixcol_depth")
                        .expect("history depth should decode"),
                    Value::Integer(1)
                );
                match row
                    .get::<Value>("lixcol_observed_commit_id")
                    .expect("observed commit should exist")
                {
                    Value::Text(commit_id) => commit_id,
                    value => panic!("observed commit should be text, got {value:?}"),
                }
            })
            .collect::<Vec<_>>();
        observed.sort();
        let mut expected = vec![main_sibling, draft_sibling];
        expected.sort();
        assert_eq!(observed, expected);
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("delete commit head should load")
            .expect("delete commit head should exist");

        let result = session
            .execute(
                &format!(
					"SELECT id, path, name, lixcol_is_deleted, lixcol_source_changes, lixcol_as_of_commit_id, lixcol_depth \
	                 FROM lix_directory_history \
	                 WHERE lixcol_as_of_commit_id = '{delete_commit_id}' \
	                   AND lixcol_entity_pk IN (lix_json('[\"history-delete-docs\"]'), lix_json('[\"history-delete-guides\"]')) \
	                   AND lixcol_depth = 0 \
	                 ORDER BY lixcol_entity_pk"
				),
                &[],
            )
            .await
            .expect("directory delete history read should succeed");

        assert_eq!(result.len(), 2);
        for (row, expected_id) in result
            .rows()
            .iter()
            .zip(["history-delete-docs", "history-delete-guides"])
        {
            assert_eq!(
                &row.values()[..4],
                &[
                    Value::Text(expected_id.to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Boolean(true),
                ]
            );
            let Value::Json(source_changes) = &row.values()[4] else {
                panic!("delete source changes should be JSON");
            };
            let source_changes = source_changes
                .as_array()
                .expect("delete source changes should be an array");
            let expected_source_ids = if expected_id == "history-delete-docs" {
                BTreeSet::from(["history-delete-docs"])
            } else {
                BTreeSet::from(["history-delete-docs", "history-delete-guides"])
            };
            let actual_source_ids = source_changes
                .iter()
                .map(|source| {
                    assert_eq!(source["schema_key"], json!("lix_directory_descriptor"));
                    assert_eq!(source["snapshot_content"], serde_json::Value::Null);
                    source["entity_pk"][0]
                        .as_str()
                        .expect("directory source identity should be text")
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(actual_source_ids, expected_source_ids);
            assert_eq!(row.values()[5], Value::Text(delete_commit_id.clone()));
            assert_eq!(row.values()[6], Value::Integer(0));
        }
    }
);
