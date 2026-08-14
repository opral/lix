use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    delete_returning_uses_predelete_rows_across_filesystem_and_branch_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('72657475-726e-896e-872d-66696c650000', '/returning-file.txt', CAST('before' AS BYTEA))",
                &[],
            )
            .await
            .expect("file fixture insert should succeed");
        let deleted_file = session
            .execute(
                "DELETE FROM lix_file WHERE path LIKE '/returning-%' RETURNING id, content",
                &[],
            )
            .await
            .expect("file DELETE LIKE RETURNING should succeed");
        assert_eq!(deleted_file.rows_affected(), 1);
        assert_rows_eq(
            deleted_file,
            vec![vec![
                Value::Text("72657475-726e-896e-872d-66696c650000".to_string()),
                Value::Blob(b"before".to_vec().into()),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('72657475-726e-896e-872d-646972656300', '/72657475-726e-896e-872d-646972656300')",
                &[],
            )
            .await
            .expect("directory fixture insert should succeed");
        let deleted_directory = session
            .execute(
                "DELETE FROM lix_directory \
                 WHERE path = '/72657475-726e-896e-872d-646972656300' \
                 RETURNING id, path",
                &[],
            )
            .await
            .expect("directory DELETE LIKE RETURNING should succeed");
        assert_eq!(deleted_directory.rows_affected(), 1);
        assert_rows_eq(
            deleted_directory,
            vec![vec![
                Value::Text("72657475-726e-896e-872d-646972656300".to_string()),
                Value::Text("/72657475-726e-896e-872d-646972656300".to_string()),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) VALUES ('72657475-726e-896e-872d-6272616e6300', 'Returning branch')",
                &[],
            )
            .await
            .expect("branch fixture insert should succeed");
        let deleted_branch = session
            .execute(
                "DELETE FROM lix_branch WHERE id = '72657475-726e-896e-872d-6272616e6300' RETURNING id, name",
                &[],
            )
            .await
            .expect("branch DELETE RETURNING should succeed");
        assert_eq!(deleted_branch.rows_affected(), 1);
        assert_rows_eq(
            deleted_branch,
            vec![vec![
                Value::Text("72657475-726e-896e-872d-6272616e6300".to_string()),
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
            engine.open_session().await.expect("session should open"),
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
                Value::Json(json!("before").into()),
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
                    Value::Json(json!("A").into()),
                ],
                vec![
                    Value::Text("returning-like-b".to_string()),
                    Value::Json(json!("B").into()),
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
            engine.open_session().await.expect("session should open"),
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
