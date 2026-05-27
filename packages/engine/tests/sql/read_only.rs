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
    read_only_branch_components_reject_lix_state_writes,
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
                    "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                     VALUES (lix_json('[\"orphan-descriptor\"]'), 'lix_branch_descriptor', \
                       lix_json('{\"id\":\"orphan-descriptor\",\"name\":\"Orphan\"}'))",
                    &[],
                )
                .await
                .expect_err("descriptor insert via lix_state should be read-only"),
            "lix_branch_descriptor",
            "lix_branch",
        );

        let descriptor_count = session
            .execute(
                "SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = 'orphan-descriptor'",
                &[],
            )
            .await
            .expect("descriptor count should query");
        assert_eq!(
            descriptor_count.rows()[0].values(),
            &[Value::Integer(0)],
            "read-only rejection should prevent orphan descriptor persistence"
        );
    }
);

simulation_test!(read_only_file_descriptor_rejects_writes, |sim| async move {
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
                "INSERT INTO lix_file_descriptor (id, directory_id, name) \
                 VALUES ('file-direct', NULL, 'direct.txt')",
                &[],
            )
            .await
            .expect_err("file descriptor insert should be read-only"),
        "lix_file_descriptor",
        "lix_file",
    );
});

simulation_test!(read_only_binary_blob_ref_rejects_writes, |sim| async move {
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
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('file-with-data', '/file.bin', X'4142')",
            &[],
        )
        .await
        .expect("file insert should create managed blob ref");

    assert_read_only_error(
        session
            .execute(
                "INSERT INTO lix_binary_blob_ref (id, blob_hash, size_bytes) \
                 VALUES ('file-direct', 'fake-hash', 2)",
                &[],
            )
            .await
            .expect_err("blob ref insert should be read-only"),
        "lix_binary_blob_ref",
        "lix_file data column",
    );

    assert_read_only_error(
        session
            .execute(
                "UPDATE lix_binary_blob_ref \
                 SET blob_hash = 'other-hash' \
                 WHERE id = 'file-with-data'",
                &[],
            )
            .await
            .expect_err("blob ref update should be read-only"),
        "lix_binary_blob_ref",
        "lix_file data column",
    );

    assert_read_only_error(
        session
            .execute(
                "DELETE FROM lix_binary_blob_ref WHERE id = 'file-with-data'",
                &[],
            )
            .await
            .expect_err("blob ref delete should be read-only"),
        "lix_binary_blob_ref",
        "lix_file data column",
    );

    assert_read_only_error(
        session
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"file-with-data\"]')",
                &[],
            )
            .await
            .expect_err("blob ref delete via lix_state should be read-only"),
        "lix_binary_blob_ref",
        "lix_file data column",
    );

    let data = session
        .execute("SELECT data FROM lix_file WHERE id = 'file-with-data'", &[])
        .await
        .expect("file data should still be readable");
    assert_eq!(data.rows()[0].values(), &[Value::Blob(vec![0x41, 0x42])]);
});

simulation_test!(
    read_only_directory_descriptor_rejects_writes,
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
                    "INSERT INTO lix_directory_descriptor (id, parent_id, name) \
                     VALUES ('dir-direct', NULL, 'direct')",
                    &[],
                )
                .await
                .expect_err("directory descriptor insert should be read-only"),
            "lix_directory_descriptor",
            "lix_directory",
        );
    }
);

simulation_test!(
    read_only_internal_state_rejects_lix_state_writes,
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
                    "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content, global) \
                     VALUES (lix_json('[\"fake-change\"]'), 'lix_change', \
                       lix_json('{\"id\":\"fake-change\",\"entity_pk\":\"x\",\"schema_key\":\"lix_key_value\"}'), true)",
                    &[],
                )
                .await
                .expect_err("lix_change insert via lix_state should be read-only"),
            "lix_change",
            "transactions commit",
        );
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

    assert_read_only_error(
        session
            .execute("DELETE FROM lix_state_history", &[])
            .await
            .expect_err("history delete should be read-only"),
        "lix_state_history",
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
