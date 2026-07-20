use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    delete_returning_uses_predelete_rows_across_builtin_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, file_id, snapshot_content, global, untracked) \
                 VALUES (lix_json('[\"returning-state\"]'), 'lix_key_value', NULL, \
                 lix_json('{\"key\":\"returning-state\",\"value\":\"before\"}'), false, false)",
                &[],
            )
            .await
            .expect("state fixture insert should succeed");
        let deleted_state = session
            .execute(
                "DELETE FROM lix_state \
                 WHERE entity_pk = lix_json('[\"returning-state\"]') \
                   AND schema_key = 'lix_key_value' \
                 RETURNING schema_key, snapshot_content AS before_snapshot",
                &[],
            )
            .await
            .expect("state DELETE RETURNING should succeed");
        assert_eq!(deleted_state.rows_affected(), 1);
        assert_eq!(deleted_state.columns(), ["schema_key", "before_snapshot"]);
        assert_rows_eq(
            deleted_state,
            vec![vec![
                Value::Text("lix_key_value".to_string()),
                Value::Json(json!({"key": "returning-state", "value": "before"})),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('returning-file', '/returning-file.txt', X'6265666F7265')",
                &[],
            )
            .await
            .expect("file fixture insert should succeed");
        let deleted_file = session
            .execute(
                "DELETE FROM lix_file WHERE path LIKE '/returning-%' RETURNING id, data",
                &[],
            )
            .await
            .expect("file DELETE LIKE RETURNING should succeed");
        assert_eq!(deleted_file.rows_affected(), 1);
        assert_rows_eq(
            deleted_file,
            vec![vec![
                Value::Text("returning-file".to_string()),
                Value::Blob(b"before".to_vec()),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('returning-directory', '/returning-directory/')",
                &[],
            )
            .await
            .expect("directory fixture insert should succeed");
        let deleted_directory = session
            .execute(
                "DELETE FROM lix_directory \
                 WHERE path LIKE '/returning-directory/%' \
                 RETURNING id, path",
                &[],
            )
            .await
            .expect("directory DELETE LIKE RETURNING should succeed");
        assert_eq!(deleted_directory.rows_affected(), 1);
        assert_rows_eq(
            deleted_directory,
            vec![vec![
                Value::Text("returning-directory".to_string()),
                Value::Text("/returning-directory/".to_string()),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) VALUES ('returning-branch', 'Returning branch')",
                &[],
            )
            .await
            .expect("branch fixture insert should succeed");
        let deleted_branch = session
            .execute(
                "DELETE FROM lix_branch WHERE id = 'returning-branch' RETURNING id, name",
                &[],
            )
            .await
            .expect("branch DELETE RETURNING should succeed");
        assert_eq!(deleted_branch.rows_affected(), 1);
        assert_rows_eq(
            deleted_branch,
            vec![vec![
                Value::Text("returning-branch".to_string()),
                Value::Text("Returning branch".to_string()),
            ]],
        );
    }
);

simulation_test!(
    delete_returning_supports_direct_and_like_filtered_entity_deletes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('returning-direct', 'before')",
                &[],
            )
            .await
            .expect("entity fixture insert should succeed");
        let direct = session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'returning-direct' \
                 RETURNING key, value AS before_value",
                &[],
            )
            .await
            .expect("direct entity DELETE RETURNING should succeed");
        assert_eq!(direct.rows_affected(), 1);
        assert_eq!(direct.columns(), ["key", "before_value"]);
        assert_rows_eq(
            direct,
            vec![vec![
                Value::Text("returning-direct".to_string()),
                Value::Json(json!("before")),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES \
                 ('returning-like-a', 'A'), ('returning-like-b', 'B')",
                &[],
            )
            .await
            .expect("LIKE entity fixtures should insert");
        let matching = session
            .execute(
                "DELETE FROM lix_key_value WHERE key LIKE 'returning-like-%' \
                 RETURNING key, value",
                &[],
            )
            .await
            .expect("entity DELETE LIKE RETURNING should succeed");
        assert_eq!(matching.rows_affected(), 2);
        let mut rows = matching
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            let Value::Text(left) = &left[0] else {
                panic!("entity key should be returned as text")
            };
            let Value::Text(right) = &right[0] else {
                panic!("entity key should be returned as text")
            };
            left.cmp(right)
        });
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("returning-like-a".to_string()),
                    Value::Json(json!("A")),
                ],
                vec![
                    Value::Text("returning-like-b".to_string()),
                    Value::Json(json!("B")),
                ],
            ]
        );
    }
);

simulation_test!(
    delete_returning_keeps_columns_for_known_zero_matches,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let result = session
            .execute(
                "DELETE FROM lix_file WHERE false RETURNING id, path AS deleted_path",
                &[],
            )
            .await
            .expect("known-empty DELETE RETURNING should succeed");
        assert_eq!(result.rows_affected(), 0);
        assert_eq!(result.columns(), ["id", "deleted_path"]);
        assert!(result.rows().is_empty());
    }
);
