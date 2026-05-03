use lix_engine::{LixError, Value};

simulation_test!(
    read_only_version_components_reject_direct_entity_writes,
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
                    "INSERT INTO lix_version_descriptor (id, name, hidden) \
                     VALUES ('orphan-descriptor', 'Orphan', false)",
                    &[],
                )
                .await
                .expect_err("descriptor insert should be read-only"),
            "lix_version_descriptor",
            "lix_version",
        );

        assert_read_only_error(
            session
                .execute(
                    "UPDATE lix_version_descriptor SET name = 'Renamed' \
                     WHERE id = 'main'",
                    &[],
                )
                .await
                .expect_err("descriptor update should be read-only"),
            "lix_version_descriptor",
            "lix_version",
        );

        assert_read_only_error(
            session
                .execute("DELETE FROM lix_version_ref WHERE id = 'main'", &[])
                .await
                .expect_err("ref delete should be read-only"),
            "lix_version_ref",
            "lix_version",
        );
    }
);

simulation_test!(
    read_only_version_components_reject_lix_state_writes,
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
                    "INSERT INTO lix_state (entity_id, schema_key, snapshot_content, schema_version) \
                     VALUES ('orphan-descriptor', 'lix_version_descriptor', \
                       lix_json('{\"id\":\"orphan-descriptor\",\"name\":\"Orphan\",\"hidden\":false}'), '1')",
                    &[],
                )
                .await
                .expect_err("descriptor insert via lix_state should be read-only"),
            "lix_version_descriptor",
            "lix_version",
        );

        let descriptor_count = session
            .execute(
                "SELECT COUNT(*) FROM lix_version_descriptor WHERE id = 'orphan-descriptor'",
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
                "INSERT INTO lix_file_descriptor (id, directory_id, name, extension) \
                 VALUES ('file-direct', NULL, 'direct', 'txt')",
                &[],
            )
            .await
            .expect_err("file descriptor insert should be read-only"),
        "lix_file_descriptor",
        "lix_file",
    );
});

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
                    "INSERT INTO lix_state (entity_id, schema_key, snapshot_content, schema_version, global) \
                     VALUES ('fake-change', 'lix_change', \
                       lix_json('{\"id\":\"fake-change\",\"entity_id\":\"x\",\"schema_key\":\"lix_key_value\",\"schema_version\":\"1\"}'), '1', true)",
                    &[],
                )
                .await
                .expect_err("lix_change insert via lix_state should be read-only"),
            "lix_change",
            "transactions commit",
        );

        assert_read_only_error(
            session
                .execute(
                    "INSERT INTO lix_state (entity_id, schema_key, snapshot_content, schema_version, global) \
                     VALUES ('workspace', 'lix_active_version', \
                       lix_json('{\"id\":\"workspace\",\"version_id\":\"main\"}'), '1', true)",
                    &[],
                )
                .await
                .expect_err("lix_active_version insert via lix_state should be read-only"),
            "lix_active_version",
            "switchVersion",
        );
    }
);

fn assert_read_only_error(error: LixError, schema_key: &str, hint_fragment: &str) {
    assert_eq!(error.code, LixError::CODE_READ_ONLY);
    assert!(
        error.description.contains(schema_key),
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
