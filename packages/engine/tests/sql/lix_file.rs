use lix_engine::ExecuteResult;
use lix_engine::Value;

use super::assert_rows_eq;

simulation_test!(
    lix_file_insert_reads_path_data_and_parent_dirs,
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

        let result = session
            .execute(
                "SELECT id, path, data, hidden, lixcol_schema_key \
             FROM lix_file \
             WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("file read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("file-readme".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Blob(b"hello".to_vec()),
                Value::Boolean(false),
                Value::Text("lix_file_descriptor".to_string()),
            ]
        );

        let staged_state_result = session
            .execute(
                "SELECT entity_id, schema_key \
             FROM lix_state \
             WHERE entity_id = 'file-readme' \
             ORDER BY schema_key, entity_id",
                &[],
            )
            .await
            .expect("filesystem state read should succeed");
        let staged_state_rows = staged_state_result;
        assert_eq!(
            staged_state_rows.len(),
            2,
            "file path insert should stage one file descriptor and one blob ref for the file"
        );

        let directory_result = session
            .execute(
                "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
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

simulation_test!(lix_file_path_update_preserves_data, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
            &[],
        )
        .await
        .expect("file insert should succeed");
    assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

    let update_result = session
        .execute(
            "UPDATE lix_file \
             SET path = '/docs/readme-renamed.md' \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("file path update should succeed");
    assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

    let file_result = session
        .execute(
            "SELECT id, path, data \
             FROM lix_file \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("file read after path update should succeed");
    let file_rows = file_result;
    assert_eq!(file_rows.len(), 1);
    assert_eq!(
        file_rows.rows()[0].values(),
        &[
            Value::Text("file-readme".to_string()),
            Value::Text("/docs/readme-renamed.md".to_string()),
            Value::Blob(b"hello".to_vec()),
        ]
    );

    let state_result = session
        .execute(
            "SELECT entity_id, schema_key \
             FROM lix_state \
             WHERE entity_id = 'file-readme' \
             ORDER BY schema_key, entity_id",
            &[],
        )
        .await
        .expect("filesystem state read after path update should succeed");
    let state_rows = state_result;
    assert_eq!(
        state_rows.len(),
        2,
        "path update should update one file descriptor and preserve one blob ref"
    );

    let directory_result = session
        .execute(
            "SELECT path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
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

simulation_test!(lix_file_by_version_expands_global_rows, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data, hidden, lixcol_global, lixcol_untracked) \
             VALUES ('file-global-overlay', '/global.txt', X'67', false, true, false)",
            &[],
        )
        .await
        .expect("global file insert should succeed");

    let result = session
        .execute(
            "SELECT id, path, lixcol_version_id, lixcol_global, lixcol_untracked \
             FROM lix_file_by_version \
             WHERE id = 'file-global-overlay' \
             ORDER BY lixcol_version_id",
            &[],
        )
        .await
        .expect("file by-version read should succeed");
    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("file-global-overlay".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text(sim.main_version_id().to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
            vec![
                Value::Text("file-global-overlay".to_string()),
                Value::Text("/global.txt".to_string()),
                Value::Text("global".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ],
        ],
    );
});
