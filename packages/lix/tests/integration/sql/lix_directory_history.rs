use std::collections::BTreeSet;

use lix::{CreateBranchOptions, MergeBranchOptions, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_directory_history_reads_paths_from_commit_graph,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('68697374-6f72-892d-8469-722d646f6300', '/docs')",
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
                 VALUES ('68697374-6f72-892d-8469-722d67756900', '/docs/guides')",
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
                    "SELECT id, path, parent_id, name, lixcol_depth \
                     FROM lix_history('lix_directory', '{second_commit_id}') \
                       WHERE id IN ('68697374-6f72-892d-8469-722d646f6300', '68697374-6f72-892d-8469-722d67756900') \
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
                    Value::Text("68697374-6f72-892d-8469-722d67756900".to_string()),
                    Value::Text("/docs/guides".to_string()),
                    Value::Text("68697374-6f72-892d-8469-722d646f6300".to_string()),
                    Value::Text("guides".to_string()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("68697374-6f72-892d-8469-722d646f6300".to_string()),
                    Value::Text("/docs".to_string()),
                    Value::Null,
                    Value::Text("docs".to_string()),
                    Value::Integer(1),
                ],
            ],
        );

        let source_changes_result = session
            .execute(
                &format!(
                    "SELECT lixcol_source_changes \
                     FROM lix_history('lix_directory', '{second_commit_id}') \
                       WHERE id = '68697374-6f72-892d-8469-722d67756900' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("directory history source changes should be selectable");
        let source_changes = source_changes_result.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .expect("source_changes should be present");
        let Value::Jsonb(source_changes) = source_changes else {
            panic!("source_changes should be semantic JSON, got {source_changes:?}");
        };
        let source_changes = source_changes.to_value();
        assert_eq!(source_changes.as_array().map(Vec::len), Some(1));
        assert_eq!(
            source_changes[0]["schema_key"],
            json!("lix_directory_descriptor")
        );
        assert_eq!(
            source_changes[0]["snapshot_content"]["parent_id"],
            json!("68697374-6f72-892d-8469-722d646f6300")
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
                "file_id",
                "id",
                "metadata",
                "origin_key",
                "row_ref",
                "snapshot_content",
            ]
        );
    }
);

simulation_test!(
    lix_directory_history_defaults_to_active_head,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('68697374-6f72-892d-8465-6661756c7401', '/history-default')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        let result = session
            .execute(
                "SELECT id, lixcol_depth \
                 FROM lix_history('lix_directory') \
                 WHERE id = '68697374-6f72-892d-8465-6661756c7401'",
                &[],
            )
            .await
            .expect("directory history should default to the active head");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("68697374-6f72-892d-8465-6661756c7401".to_string()),
                Value::Integer(0),
            ]],
        );
    }
);

simulation_test!(
    lix_directory_history_preserves_equal_depth_siblings_in_a_diamond,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_directory (id, path) VALUES ('6469616d-6f6e-842d-8469-720000000000', '/before')",
            &[],
        )
        .await
        .expect("base directory should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000009".to_string()),
            name: "Diamond directory draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000009")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_directory SET name = 'same' WHERE id = '6469616d-6f6e-842d-8469-720000000000'",
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
                "UPDATE lix_directory SET name = 'same' WHERE id = '6469616d-6f6e-842d-8469-720000000000'",
                &[],
            )
            .await
            .expect("draft rename should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000009")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000009".to_string(),
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
                     FROM lix_history('lix_directory', '{merge_commit_id}') \
                       WHERE id = '6469616d-6f6e-842d-8469-720000000000' \
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
                    Value::Text("/same".to_string())
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('01940000-0000-7000-8000-000000000001', '/docs')",
                &[],
            )
            .await
            .expect("root directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('01940000-0000-7000-8000-000000000002', '/docs/guides')",
                &[],
            )
            .await
            .expect("nested directory insert should succeed");

        session
            .execute(
                "DELETE FROM lix_directory WHERE id = '01940000-0000-7000-8000-000000000001'",
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
					"SELECT id, path, name, lixcol_is_deleted, lixcol_source_changes, lixcol_depth \
	                 FROM lix_history('lix_directory', '{delete_commit_id}') \
	                   WHERE lixcol_row_ref IN (lix_row_ref('lix_directory', '01940000-0000-7000-8000-000000000001'), lix_row_ref('lix_directory', '01940000-0000-7000-8000-000000000002')) \
	                   AND lixcol_depth = 0 \
	                 ORDER BY id"
				),
                &[],
            )
            .await
            .expect("directory delete history read should succeed");

        assert_eq!(result.len(), 2);
        for (row, expected_id) in result.rows().iter().zip([
            "01940000-0000-7000-8000-000000000001",
            "01940000-0000-7000-8000-000000000002",
        ]) {
            assert_eq!(
                &row.values()[..4],
                &[
                    Value::Text(expected_id.to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Boolean(true),
                ]
            );
            let Value::Jsonb(source_changes) = &row.values()[4] else {
                panic!("delete source changes should be JSON");
            };
            let source_changes = source_changes.to_value();
            let source_changes = source_changes
                .as_array()
                .expect("delete source changes should be an array");
            let expected_source_count = if expected_id
                == "01940000-0000-7000-8000-000000000001"
            {
                1
            } else {
                2
            };
            let actual_source_refs = source_changes
                .iter()
                .map(|source| {
                    assert_eq!(source["snapshot_content"], serde_json::Value::Null);
                    source["row_ref"]
                        .as_str()
                        .expect("directory source row_ref should be text")
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(actual_source_refs.len(), expected_source_count);
            assert_eq!(row.values()[5], Value::Integer(0));
        }
    }
);
