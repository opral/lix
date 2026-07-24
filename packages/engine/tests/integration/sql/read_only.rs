use lix_engine::{LixError, Value};

simulation_test!(
    read_only_branch_components_reject_direct_entity_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        assert_read_only_error(
            session
                .execute(
                    "INSERT INTO lix_branch_descriptor (id, name, hidden) \
                     VALUES ('orphan-descriptor', 'Orphan', false)",
                    &[],
                )
                .await
                .expect_err("descriptor insert should be read-only"),
            "lix_branch_descriptor",
            "lix_branch",
        );

        assert_read_only_error(
            session
                .execute(
                    "UPDATE lix_branch_descriptor SET name = 'Renamed' \
                     WHERE id = 'main'",
                    &[],
                )
                .await
                .expect_err("descriptor update should be read-only"),
            "lix_branch_descriptor",
            "lix_branch",
        );

        assert_read_only_error(
            session
                .execute("DELETE FROM lix_branch_ref WHERE id = 'main'", &[])
                .await
                .expect_err("ref delete should be read-only"),
            "lix_branch_ref",
            "lix_branch",
        );
    }
);

simulation_test!(
    state_and_filesystem_storage_relations_are_not_public,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        for surface_name in [
            "lix_state",
            "lix_state_by_branch",
            "lix_state_history",
            "lix_binary_blob_ref",
            "lix_binary_blob_ref_by_branch",
            "lix_binary_blob_ref_history",
            "lix_directory_descriptor",
            "lix_directory_descriptor_by_branch",
            "lix_directory_descriptor_history",
            "lix_file_descriptor",
            "lix_file_descriptor_by_branch",
            "lix_file_descriptor_history",
        ] {
            let error = session
                .execute(&format!("SELECT * FROM {surface_name}"), &[])
                .await
                .expect_err("private storage relation should not resolve");
            assert_eq!(
                error.code,
                LixError::CODE_TABLE_NOT_FOUND,
                "{surface_name}: {error:?}"
            );
        }
    }
);

simulation_test!(read_only_history_views_reject_dml, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    assert_read_only_error(
        session
            .execute(
                "INSERT INTO lix_file_history (id, path) VALUES ('history-file', '/x.txt')",
                &[],
            )
            .await
            .expect_err("history insert should be read-only"),
        "lix_file_history",
        "History views are query-only",
    );

    assert_read_only_error(
        session
            .execute("UPDATE lix_directory_history SET name = 'renamed'", &[])
            .await
            .expect_err("history update should be read-only"),
        "lix_directory_history",
        "History views are query-only",
    );
});

simulation_test!(read_only_typed_history_views_reject_dml, |sim| async move {
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
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"read_only_history_entity\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}'),\
             false,\
             true\
             )",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

    assert_read_only_error(
        session
            .execute(
                "INSERT INTO read_only_history_entity_history (id) VALUES ('entity-a')",
                &[],
            )
            .await
            .expect_err("typed history insert should be read-only"),
        "read_only_history_entity_history",
        "History views are query-only",
    );
});

simulation_test!(
    cached_read_syntax_uses_a_fresh_storage_snapshot,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let reader = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("reader session should open"),
            &engine,
        );
        let writer = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("writer session should open"),
            &engine,
        );
        let select_sql = "SELECT data FROM lix_file WHERE id = $1";
        let params = [Value::Text("fresh-snapshot-file".to_string())];

        let before = reader
            .execute(select_sql, &params)
            .await
            .expect("initial cached-syntax read should succeed");
        assert_eq!(before.len(), 0);

        writer
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
                &[
                    Value::Text("fresh-snapshot-file".to_string()),
                    Value::Text("/fresh-snapshot.txt".to_string()),
                    Value::Blob(b"fresh".to_vec().into()),
                ],
            )
            .await
            .expect("intervening write should commit");

        let after = reader
            .execute(select_sql, &params)
            .await
            .expect("repeated syntax should use a fresh provider snapshot");
        assert_eq!(
            after.rows()[0].values(),
            &[Value::Blob(b"fresh".to_vec().into())]
        );
    }
);

fn assert_read_only_error(error: LixError, schema_key: &str, hint_fragment: &str) {
    assert_eq!(error.code, LixError::CODE_READ_ONLY);
    assert!(
        error.message.contains(schema_key),
        "read-only error should name {schema_key}: {error:?}"
    );
    assert!(
        error
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains(hint_fragment)),
        "read-only error should guide callers toward {hint_fragment}: {error:?}"
    );
}
